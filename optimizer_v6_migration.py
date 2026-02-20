"""
Optimizer API v6 — Class-Free Migration
=========================================
This module replaces the hardcoded AGENCY_CONFIG + class forks with
config-driven routing from REF_ADVERTISER_CONFIG.

ROUTING KEY: IMPRESSION_JOIN_STRATEGY (already populated in REF_ADVERTISER_CONFIG)
  - 'ADM_PREFIX'  → row-level queries on PARAMOUNT_IMPRESSIONS_REPORT_90_DAYS
                     Uses COUNT DISTINCT (Paramount/CTV impression log)
  - 'PCM_4KEY'    → pre-aggregated queries on CAMPAIGN_PERFORMANCE_REPORT_WEEKLY_STATS
                     Uses SUM (Xandr/standard DSP weekly rollup)
  - 'DIRECT_AG'   → web pixel direct, no impression join (web-only agencies)

EXPOSURE_SOURCE (existing taxonomy, NOT modified):
  - 'IMPRESSION'  → standard impression-based attribution
  - 'WEB'         → web pixel attribution only
  - 'OOH'         → out-of-home measurement

MIGRATION STRATEGY:
  1. Replace AGENCY_CONFIG dict with dynamic lookup from Snowflake
  2. Route based on IMPRESSION_JOIN_STRATEGY (replaces 'if agency_id == 1480'):
     - 'ADM_PREFIX' → Paramount row-level impression path
     - 'PCM_4KEY'   → Class B pre-aggregated path
  3. Store/web visit data comes from V5_ALL_VISITS for ALL agencies
  4. Impression routing is config-driven — any agency can use either path

INTEGRATION:
  from optimizer_v6_migration import get_impression_strategy, get_agency_config
  # Then in each endpoint, replace:
  #   if agency_id == 1480:
  # with:
  #   if get_impression_strategy(agency_id, conn) == 'ADM_PREFIX':
"""
import threading
import time


# =============================================================================
# DYNAMIC AGENCY CONFIG (replaces hardcoded AGENCY_CONFIG dict)
# =============================================================================
_agency_config_cache = {}
_agency_config_lock = threading.Lock()
_agency_config_ts = 0
AGENCY_CONFIG_TTL = 300  # Refresh every 5 minutes

# Routing constants
STRATEGY_ADM_PREFIX = 'ADM_PREFIX'   # Row-level impression log (Paramount)
STRATEGY_PCM_4KEY = 'PCM_4KEY'       # Pre-aggregated weekly stats (Class B)
STRATEGY_DIRECT_AG = 'DIRECT_AG'     # Web pixel direct (no impression join)


def load_agency_config(conn):
    """
    Load agency config from REF_ADVERTISER_CONFIG + AGENCY_ADVERTISER.
    Returns dict keyed by AGENCY_ID with capabilities and routing info.

    Replaces:
        AGENCY_CONFIG = {
            1480: {'name': 'Paramount', 'class': 'PARAMOUNT'},
            1813: {'name': 'Causal iQ', 'class': 'B'},
            ...
        }
    """
    cursor = conn.cursor()
    cursor.execute("""
        SELECT
            c.AGENCY_ID,
            MAX(aa.AGENCY_NAME) as AGENCY_NAME,
            MAX(c.EXPOSURE_SOURCE) as EXPOSURE_SOURCE,
            MAX(c.IMPRESSION_JOIN_STRATEGY) as IMPRESSION_JOIN_STRATEGY,
            MAX(c.MATCH_STRATEGY) as MATCH_STRATEGY,
            BOOLOR_AGG(c.HAS_STORE_VISIT_ATTRIBUTION) as HAS_STORE_VISITS,
            BOOLOR_AGG(c.HAS_WEB_VISIT_ATTRIBUTION) as HAS_WEB_VISITS,
            BOOLOR_AGG(c.HAS_IMPRESSION_TRACKING) as HAS_IMPRESSIONS,
            COUNT(DISTINCT c.ADVERTISER_ID) as ADVERTISER_COUNT,
            LISTAGG(DISTINCT c.PLATFORM_TYPE_IDS, ',') as ALL_PLATFORMS
        FROM QUORUMDB.BASE_TABLES.REF_ADVERTISER_CONFIG c
        LEFT JOIN QUORUMDB.SEGMENT_DATA.AGENCY_ADVERTISER aa
            ON c.AGENCY_ID = aa.ADVERTISER_ID
        WHERE c.CONFIG_STATUS = 'ACTIVE'
          AND c.HAS_IMPRESSION_TRACKING = TRUE
        GROUP BY c.AGENCY_ID
        HAVING COUNT(DISTINCT c.ADVERTISER_ID) > 0
    """)

    config = {}
    for row in cursor.fetchall():
        agency_id = row[0]
        config[agency_id] = {
            'name': row[1] or f'Agency {agency_id}',
            'exposure_source': row[2] or 'IMPRESSION',
            'impression_join_strategy': row[3] or STRATEGY_PCM_4KEY,
            'match_strategy': row[4] or 'IP_MAID',
            'has_store_visits': row[5] or False,
            'has_web_visits': row[6] or False,
            'has_impressions': row[7] or False,
            'advertiser_count': row[8] or 0,
            'platforms': row[9] or '',
        }

    cursor.close()
    return config


def get_agency_config(conn=None):
    """
    Returns cached agency config, refreshing if stale.
    Thread-safe with 5-minute TTL.
    """
    global _agency_config_cache, _agency_config_ts

    with _agency_config_lock:
        if time.time() - _agency_config_ts < AGENCY_CONFIG_TTL and _agency_config_cache:
            return _agency_config_cache

    # Need to refresh — requires a connection
    if conn is None:
        return _agency_config_cache  # Return stale if no connection available

    config = load_agency_config(conn)

    with _agency_config_lock:
        _agency_config_cache = config
        _agency_config_ts = time.time()

    return config


def get_impression_strategy(agency_id, conn=None):
    """
    Get the impression join strategy for an agency.
    THIS IS THE PRIMARY ROUTING KEY — replaces `if agency_id == 1480:`.

    Returns:
        'ADM_PREFIX'  → row-level COUNT DISTINCT on PARAMOUNT_IMPRESSIONS_REPORT_90_DAYS
        'PCM_4KEY'    → pre-aggregated SUM on CAMPAIGN_PERFORMANCE_REPORT_WEEKLY_STATS
        'DIRECT_AG'   → web pixel direct, no impression join
    """
    config = get_agency_config(conn)
    agency = config.get(int(agency_id), {})
    return agency.get('impression_join_strategy', STRATEGY_PCM_4KEY)


def is_row_level_agency(agency_id, conn=None):
    """
    Convenience: True if agency uses row-level impression data (ADM_PREFIX).
    Direct replacement for `if agency_id == 1480:` in old code.
    """
    return get_impression_strategy(agency_id, conn) == STRATEGY_ADM_PREFIX


def get_agency_name(agency_id, conn=None):
    """Replaces the old get_agency_name() that read from hardcoded dict."""
    config = get_agency_config(conn)
    agency = config.get(int(agency_id), {})
    return agency.get('name', f'Agency {agency_id}')


def get_agency_capabilities(agency_id, conn=None):
    """
    Returns capability flags for an agency.
    Used by endpoints to decide what metrics to show.
    """
    config = get_agency_config(conn)
    return config.get(int(agency_id), {
        'has_store_visits': False,
        'has_web_visits': False,
        'has_impressions': False,
    })


# =============================================================================
# DATA AVAILABILITY — Per-Agency Dimension Availability Check
# =============================================================================
# Different DSPs populate different columns in AD_IMPRESSION_LOG_V2.
# This check tells the frontend which tabs to show vs grey-out.
#
# Data availability matrix (from Feb 2026 audit):
#   SITE_DOMAIN:       Only Paramount (opaque IDs), Causal iQ (~15% real), Magnite (4097 real)
#   USER_POSTAL_CODE:  Only Causal iQ (289K distinct) and Magnite (5036 distinct)
#   CREATIVE_ID:       Most agencies have it, but names vary
#   WEBPIXEL_EVENTS:   Only Paramount (49M), LotLinx (71M), NPRP (1.2M), ByRider (85K)
# =============================================================================
_dimension_avail_cache = {}
_dimension_avail_lock = threading.Lock()
_dimension_avail_ts = 0
DIMENSION_AVAIL_TTL = 600  # 10 minutes — these change slowly


def check_data_availability(agency_id, conn):
    """
    Check what dimension data is available for an agency.
    Returns a dict with boolean availability flags and reasons.

    Checks AD_IMPRESSION_LOG_V2 for non-zero/non-null dimension columns,
    and WEBPIXEL_EVENTS for web pixel data. Results are cached per-agency.

    Returns:
        {
            'has_publisher_data': bool,
            'has_geo_data': bool,
            'has_creative_data': bool,
            'has_creative_names': bool,
            'has_web_pixel': bool,
            'publisher_count': int,
            'geo_zip_count': int,
            'creative_count': int,
            'web_pixel_events': int,
            'reasons': {
                'publisher': str,  # human-readable reason if unavailable
                'geographic': str,
                'creative': str,
                'traffic_sources': str,
            }
        }
    """
    global _dimension_avail_cache, _dimension_avail_ts

    agency_id = int(agency_id)

    # Check cache first
    with _dimension_avail_lock:
        if (time.time() - _dimension_avail_ts < DIMENSION_AVAIL_TTL
                and agency_id in _dimension_avail_cache):
            return _dimension_avail_cache[agency_id]

    result = _query_dimension_availability(agency_id, conn)

    with _dimension_avail_lock:
        _dimension_avail_cache[agency_id] = result
        _dimension_avail_ts = time.time()

    return result


def _query_dimension_availability(agency_id, conn):
    """Run the actual Snowflake queries to check dimension availability."""
    cursor = conn.cursor()
    result = {
        'has_publisher_data': False,
        'has_geo_data': False,
        'has_creative_data': False,
        'has_creative_names': False,
        'has_web_pixel': False,
        'publisher_count': 0,
        'geo_zip_count': 0,
        'creative_count': 0,
        'web_pixel_events': 0,
        'reasons': {},
    }

    try:
        # Single efficient query: check all dimension columns at once
        # Uses APPROX_COUNT_DISTINCT for speed on billion-row table
        # Last 30 days only — no need to scan full history
        cursor.execute("""
            SELECT
                APPROX_COUNT_DISTINCT(
                    CASE WHEN SITE_DOMAIN IS NOT NULL AND SITE_DOMAIN != '0' AND SITE_DOMAIN != ''
                         THEN SITE_DOMAIN END
                ) as PUBLISHER_COUNT,
                APPROX_COUNT_DISTINCT(
                    CASE WHEN USER_POSTAL_CODE IS NOT NULL AND USER_POSTAL_CODE != '0' AND USER_POSTAL_CODE != ''
                         THEN USER_POSTAL_CODE END
                ) as ZIP_COUNT,
                APPROX_COUNT_DISTINCT(
                    CASE WHEN CREATIVE_ID IS NOT NULL AND CREATIVE_ID != '0' AND CREATIVE_ID != ''
                         THEN CREATIVE_ID END
                ) as CREATIVE_COUNT,
                APPROX_COUNT_DISTINCT(
                    CASE WHEN CREATIVE_NAME IS NOT NULL AND CREATIVE_NAME != '' AND CREATIVE_NAME != '0'
                         THEN CREATIVE_NAME END
                ) as CREATIVE_NAME_COUNT
            FROM QUORUMDB.BASE_TABLES.AD_IMPRESSION_LOG_V2
            WHERE AGENCY_ID = %(agency_id)s
              AND AUCTION_TIMESTAMP >= DATEADD(day, -30, CURRENT_DATE)
        """, {'agency_id': agency_id})

        row = cursor.fetchone()
        if row:
            pub_count = int(row[0]) if row[0] else 0
            zip_count = int(row[1]) if row[1] else 0
            creative_count = int(row[2]) if row[2] else 0
            creative_name_count = int(row[3]) if row[3] else 0

            result['publisher_count'] = pub_count
            result['geo_zip_count'] = zip_count
            result['creative_count'] = creative_count

            # Publisher: need at least 2 distinct non-zero publishers
            result['has_publisher_data'] = pub_count >= 2
            if not result['has_publisher_data']:
                result['reasons']['publisher'] = (
                    'Publisher (SITE_DOMAIN) data is not available for this agency\'s DSP. '
                    'This is a data source limitation, not a bug.'
                )

            # Geographic: need at least 10 distinct non-zero zip codes
            result['has_geo_data'] = zip_count >= 10
            if not result['has_geo_data']:
                result['reasons']['geographic'] = (
                    'Geographic (postal code) data is not available for this agency\'s DSP. '
                    'Currently only Causal iQ and Magnite provide zip-level data.'
                )

            # Creative: need at least 1 creative ID
            result['has_creative_data'] = creative_count >= 1
            result['has_creative_names'] = creative_name_count >= 1
            if not result['has_creative_data']:
                result['reasons']['creative'] = (
                    'Creative ID data is not populated for this agency\'s DSP.'
                )

    except Exception:
        # If the impression query fails, default to False for all
        result['reasons']['publisher'] = 'Could not verify publisher data availability.'
        result['reasons']['geographic'] = 'Could not verify geographic data availability.'
        result['reasons']['creative'] = 'Could not verify creative data availability.'

    # Check web pixel availability separately (different table)
    try:
        cursor.execute("""
            SELECT COUNT(*) as EVENT_COUNT
            FROM QUORUMDB.DERIVED_TABLES.WEBPIXEL_EVENTS
            WHERE AGENCY_ID = %(agency_id)s
              AND EVENT_TIMESTAMP >= DATEADD(day, -30, CURRENT_DATE)
            LIMIT 1
        """, {'agency_id': agency_id})
        row = cursor.fetchone()
        event_count = int(row[0]) if row and row[0] else 0
        result['web_pixel_events'] = event_count
        result['has_web_pixel'] = event_count > 0
        if not result['has_web_pixel']:
            result['reasons']['traffic_sources'] = (
                'This agency does not have a web pixel deployed. '
                'Traffic source analysis requires WEBPIXEL_EVENTS data.'
            )
    except Exception:
        result['reasons']['traffic_sources'] = 'Could not verify web pixel data availability.'

    cursor.close()
    return result


def get_tab_availability(agency_id, conn):
    """
    Returns a frontend-friendly dict of tab availability.
    Each tab maps to {available: bool, reason: str}.
    """
    avail = check_data_availability(agency_id, conn)
    strategy = get_impression_strategy(agency_id, conn)

    # Campaign, Line Item, Timeseries are always available if agency has impressions
    caps = get_agency_capabilities(agency_id, conn)
    has_impressions = caps.get('has_impressions', False)

    return {
        'campaigns':      {'available': has_impressions, 'reason': '' if has_impressions else 'No impression data for this agency.'},
        'lineitems':      {'available': has_impressions, 'reason': '' if has_impressions else 'No impression data for this agency.'},
        'creatives':      {'available': avail['has_creative_data'], 'reason': avail['reasons'].get('creative', '')},
        'publishers':     {'available': avail['has_publisher_data'], 'reason': avail['reasons'].get('publisher', '')},
        'geographic':     {'available': avail['has_geo_data'], 'reason': avail['reasons'].get('geographic', '')},
        'traffic_sources': {'available': avail['has_web_pixel'], 'reason': avail['reasons'].get('traffic_sources', '')},
        'lift':           {'available': has_impressions, 'reason': '' if has_impressions else 'No impression data for this agency.'},
        'timeseries':     {'available': has_impressions, 'reason': '' if has_impressions else 'No impression data for this agency.'},
        'strategy': strategy,
        'has_creative_names': avail['has_creative_names'],
        'publisher_count': avail['publisher_count'],
        'geo_zip_count': avail['geo_zip_count'],
        'creative_count': avail['creative_count'],
        'web_pixel_events': avail['web_pixel_events'],
    }


# =============================================================================
# MIGRATED ENDPOINT EXAMPLES
# =============================================================================
# Below are the key patterns showing how each endpoint type migrates.
# The full v6 API applies these patterns to all 16 data endpoints.


def migrated_agencies_endpoint(start_date, end_date, conn):
    """
    BEFORE (v5):
        query_class_b = "... FROM CAMPAIGN_PERFORMANCE_REPORT_WEEKLY_STATS ..."
        query_paramount = "... FROM PARAMOUNT_IMPRESSIONS_REPORT_90_DAYS ..."
        # Hardcoded: Paramount separate, Class B separate, merged in Python

    AFTER (v6):
        Route by IMPRESSION_JOIN_STRATEGY from config. Same two query paths,
        but the routing is dynamic — any agency can use either path.
    """
    cursor = conn.cursor()
    config = get_agency_config(conn)
    all_results = []

    # Group agencies by impression join strategy
    row_level_agencies = [aid for aid, c in config.items()
                          if c['impression_join_strategy'] == STRATEGY_ADM_PREFIX]
    pre_agg_agencies = [aid for aid, c in config.items()
                        if c['impression_join_strategy'] == STRATEGY_PCM_4KEY]

    # Pre-aggregated path (PCM_4KEY — formerly "Class B")
    if pre_agg_agencies:
        cursor.execute("""
            SELECT
                AGENCY_ID,
                SUM(IMPRESSIONS) as IMPRESSIONS,
                SUM(VISITORS) as STORE_VISITS,
                0 as WEB_VISITS,
                COUNT(DISTINCT ADVERTISER_ID) as ADVERTISER_COUNT
            FROM QUORUMDB.SEGMENT_DATA.CAMPAIGN_PERFORMANCE_REPORT_WEEKLY_STATS
            WHERE LOG_DATE BETWEEN %(start_date)s AND %(end_date)s
            GROUP BY AGENCY_ID
            HAVING SUM(IMPRESSIONS) > 0 OR SUM(VISITORS) > 0
        """, {'start_date': start_date, 'end_date': end_date})

        for row in cursor.fetchall():
            agency_id = row[0]
            if agency_id in config:  # Only include configured agencies
                all_results.append({
                    'AGENCY_ID': agency_id,
                    'AGENCY_NAME': config[agency_id]['name'],
                    'IMPRESSIONS': row[1] or 0,
                    'STORE_VISITS': row[2] or 0,
                    'WEB_VISITS': row[3] or 0,
                    'ADVERTISER_COUNT': row[4] or 0,
                    'IMPRESSION_STRATEGY': STRATEGY_PCM_4KEY
                })

    # Row-level path (ADM_PREFIX — formerly "Paramount")
    for agency_id in row_level_agencies:
        cursor.execute("""
            SELECT
                %(agency_id)s as AGENCY_ID,
                APPROX_COUNT_DISTINCT(CACHE_BUSTER) as IMPRESSIONS,
                APPROX_COUNT_DISTINCT(CASE WHEN IS_STORE_VISIT = 'TRUE' THEN IMP_MAID END) as STORE_VISITS,
                APPROX_COUNT_DISTINCT(CASE WHEN IS_SITE_VISIT = 'TRUE' THEN IP END) as WEB_VISITS,
                APPROX_COUNT_DISTINCT(QUORUM_ADVERTISER_ID) as ADVERTISER_COUNT
            FROM QUORUMDB.SEGMENT_DATA.PARAMOUNT_IMPRESSIONS_REPORT_90_DAYS
            WHERE IMP_DATE BETWEEN %(start_date)s AND %(end_date)s
              AND AGENCY_ID = %(agency_id)s
        """, {'agency_id': agency_id, 'start_date': start_date, 'end_date': end_date})

        row = cursor.fetchone()
        if row and (row[1] or row[2] or row[3]):
            all_results.append({
                'AGENCY_ID': agency_id,
                'AGENCY_NAME': config[agency_id]['name'],
                'IMPRESSIONS': row[1] or 0,
                'STORE_VISITS': row[2] or 0,
                'WEB_VISITS': row[3] or 0,
                'ADVERTISER_COUNT': row[4] or 0,
                'IMPRESSION_STRATEGY': STRATEGY_ADM_PREFIX
            })

    # Enrich with V5 web visit counts for agencies that have web attribution
    for result in all_results:
        aid = result['AGENCY_ID']
        caps = config.get(aid, {})
        if caps.get('has_web_visits') and result['IMPRESSION_STRATEGY'] == STRATEGY_PCM_4KEY:
            cursor.execute("""
                SELECT COUNT(DISTINCT DEVICE_ID) as WEB_VISITS
                FROM QUORUMDB.SEGMENT_DATA.V5_ALL_VISITS
                WHERE AGENCY_ID = %(agency_id)s
                  AND VISIT_TYPE = 'WEB'
                  AND VISIT_DATE BETWEEN %(start_date)s AND %(end_date)s
            """, {'agency_id': aid, 'start_date': start_date, 'end_date': end_date})
            wv_row = cursor.fetchone()
            if wv_row and wv_row[0]:
                result['WEB_VISITS'] = wv_row[0]

    all_results.sort(key=lambda x: x.get('IMPRESSIONS', 0) or 0, reverse=True)
    return all_results


def migrated_campaign_performance(agency_id, advertiser_id, start_date, end_date, conn):
    """
    BEFORE (v5):
        if agency_id == 1480:
            query = "... FROM PARAMOUNT_IMPRESSIONS_REPORT_90_DAYS ..."
        else:
            query = "... FROM CAMPAIGN_PERFORMANCE_REPORT_WEEKLY_STATS ..."

    AFTER (v6):
        strategy = get_impression_strategy(agency_id, conn)
        if strategy == 'ADM_PREFIX':
            ... (row-level COUNT DISTINCT)
        else:
            ... (pre-aggregated SUM)
    """
    cursor = conn.cursor()
    strategy = get_impression_strategy(agency_id, conn)

    if strategy == STRATEGY_ADM_PREFIX:
        cursor.execute("""
            SELECT
                IO_ID,
                MAX(IO_NAME) as IO_NAME,
                COUNT(DISTINCT CACHE_BUSTER) as IMPRESSIONS,
                COUNT(DISTINCT CASE WHEN IS_STORE_VISIT = 'TRUE' THEN IMP_MAID END) as STORE_VISITS,
                COUNT(DISTINCT CASE WHEN IS_SITE_VISIT = 'TRUE' THEN IP END) as WEB_VISITS
            FROM QUORUMDB.SEGMENT_DATA.PARAMOUNT_IMPRESSIONS_REPORT_90_DAYS
            WHERE QUORUM_ADVERTISER_ID = %(advertiser_id)s
              AND IMP_DATE BETWEEN %(start_date)s AND %(end_date)s
            GROUP BY IO_ID
            HAVING COUNT(DISTINCT CACHE_BUSTER) >= 100
            ORDER BY 3 DESC
        """, {
            'advertiser_id': advertiser_id,
            'start_date': start_date,
            'end_date': end_date
        })
    else:
        # Pre-aggregated path — works for any PCM_4KEY or DIRECT_AG agency
        cursor.execute("""
            SELECT
                CAST(IO_ID AS NUMBER) as IO_ID,
                MAX(IO_NAME) as IO_NAME,
                SUM(IMPRESSIONS) as IMPRESSIONS,
                SUM(VISITORS) as STORE_VISITS,
                0 as WEB_VISITS
            FROM QUORUMDB.SEGMENT_DATA.CAMPAIGN_PERFORMANCE_REPORT_WEEKLY_STATS
            WHERE AGENCY_ID = %(agency_id)s
              AND ADVERTISER_ID = %(advertiser_id)s
              AND LOG_DATE BETWEEN %(start_date)s AND %(end_date)s
            GROUP BY IO_ID
            HAVING SUM(IMPRESSIONS) >= 100 OR SUM(VISITORS) >= 10
            ORDER BY 3 DESC
        """, {
            'agency_id': agency_id,
            'advertiser_id': advertiser_id,
            'start_date': start_date,
            'end_date': end_date
        })

    columns = [desc[0] for desc in cursor.description]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]


# =============================================================================
# V6 STORE VISITS — UNIFIED FOR ALL AGENCIES
# =============================================================================
def get_store_visits_v6(agency_id, advertiser_id, start_date, end_date, conn):
    """
    NEW in v6: Unified store visit query for ANY agency.
    Uses V5_ALL_VISITS instead of forked Paramount/Class B paths.

    This replaces:
      - PARAMOUNT_STORE_VISIT_RAW_90_DAYS (Paramount path)
      - Web visitors logic (Class B had no web visits)
    """
    cursor = conn.cursor()

    cursor.execute("""
        SELECT
            VISIT_TYPE,
            COUNT(DISTINCT DEVICE_ID) as UNIQUE_VISITORS,
            COUNT(*) as TOTAL_VISITS,
            MIN(VISIT_DATE) as FIRST_VISIT,
            MAX(VISIT_DATE) as LAST_VISIT
        FROM QUORUMDB.SEGMENT_DATA.V5_ALL_VISITS
        WHERE AGENCY_ID = %(agency_id)s
          AND ADVERTISER_ID = %(advertiser_id)s
          AND VISIT_DATE BETWEEN %(start_date)s AND %(end_date)s
        GROUP BY VISIT_TYPE
    """, {
        'agency_id': agency_id,
        'advertiser_id': advertiser_id,
        'start_date': start_date,
        'end_date': end_date
    })

    columns = [desc[0] for desc in cursor.description]
    results = [dict(zip(columns, row)) for row in cursor.fetchall()]

    # Reshape: {STORE: {visitors, visits}, WEB: {visitors, visits}}
    visits = {}
    for r in results:
        visits[r['VISIT_TYPE']] = {
            'unique_visitors': r['UNIQUE_VISITORS'],
            'total_visits': r['TOTAL_VISITS'],
            'first_visit': str(r['FIRST_VISIT']) if r['FIRST_VISIT'] else None,
            'last_visit': str(r['LAST_VISIT']) if r['LAST_VISIT'] else None,
        }

    return visits


# =============================================================================
# MIGRATION CHECKLIST
# =============================================================================
"""
ENDPOINT MIGRATION STATUS:

For each endpoint, the migration is:
  1. Replace `if agency_id == 1480:` with:
       if get_impression_strategy(agency_id, conn) == 'ADM_PREFIX':
     or use the convenience function:
       if is_row_level_agency(agency_id, conn):
  2. Replace `get_agency_name(agency_id)` with `get_agency_name(agency_id, conn)`
  3. Where WEB_VISITS is hardcoded to 0, add V5_ALL_VISITS lookup if agency has_web_visits
  4. Add IMPRESSION_STRATEGY to response for transparency

ROUTING FIELD: IMPRESSION_JOIN_STRATEGY (already populated in REF_ADVERTISER_CONFIG)
  - ADM_PREFIX  = row-level (Paramount today, any future CTV agency)
  - PCM_4KEY    = pre-aggregated (all standard DSP agencies)
  - DIRECT_AG   = web pixel direct (web-only measurement)

DO NOT MODIFY: EXPOSURE_SOURCE (existing taxonomy describing data type)
  - IMPRESSION  = impression-based attribution
  - WEB         = web pixel attribution
  - OOH         = out-of-home measurement

ENDPOINTS TO MIGRATE:
  [x] /api/v5/agencies          → migrated_agencies_endpoint (pattern shown above)
  [x] /api/v5/campaign-perf     → migrated_campaign_performance (pattern shown above)
  [ ] /api/v5/advertisers       → same pattern as agencies, per-agency routing
  [ ] /api/v5/lineitem-perf     → same pattern as campaign-perf
  [ ] /api/v5/creative-perf     → same pattern, add creative columns
  [ ] /api/v5/publisher-perf    → same pattern
  [ ] /api/v5/zip-performance   → same pattern, different geo join tables
  [ ] /api/v5/dma-performance   → same pattern
  [ ] /api/v5/summary           → same pattern, add V5 visit counts
  [ ] /api/v5/timeseries        → same pattern, group by date
  [ ] /api/v5/lift-analysis     → complex: keep both paths, route by config
  [ ] /api/v5/traffic-sources   → Paramount-specific, gate by has_web_visits
  [ ] /api/v5/optimize          → same pattern
  [ ] /api/v5/optimize-geo      → same pattern, geo join
  [ ] /api/v5/agency-timeseries → same pattern
  [ ] /api/v5/adv-timeseries    → same pattern

NEW ENDPOINTS (v6):
  [x] /api/v5/store-visits      → get_store_visits_v6 (unified for all agencies)
  [ ] /api/v5/web-visits        → V5_WEB_VISITS_PARAMOUNT + future Class B web

ROLE CHANGES (in bootstrap SQL):
  - OPTIMIZER_READONLY_ROLE needs SELECT on:
    * BASE_TABLES.REF_ADVERTISER_CONFIG (new)
    * SEGMENT_DATA.V5_ALL_VISITS (new)
    * SEGMENT_DATA.V5_STORE_VISITS_ENRICHED (new)
    * SEGMENT_DATA.V5_STORE_VISITS_PARAMOUNT (new)
    * SEGMENT_DATA.V5_WEB_VISITS_PARAMOUNT (new)
    * SEGMENT_DATA.V5_STORE_VISITS_WITH_HOUSEHOLD (new)
  - Config API needs CONFIG_ADMIN_ROLE with INSERT/UPDATE on:
    * REF_DATA.PIXEL_CAMPAIGN_MAPPING_V2
    * DERIVED_TABLES.ADVERTISER_DOMAIN_MAPPING
    * SEGMENT_DATA.SEGMENT_MD5_MAPPING
    * BASE_TABLES.REF_ADVERTISER_CONFIG

NO BOOTSTRAP UPDATE NEEDED FOR EXPOSURE_SOURCE:
  EXPOSURE_SOURCE and IMPRESSION_JOIN_STRATEGY are already correctly populated.
  The bootstrap SQL only needs GRANTs and CONFIG_ADMIN_ROLE creation.
"""
