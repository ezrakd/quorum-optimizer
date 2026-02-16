"""
Quorum Optimizer API v6 — Config-Driven Routing
=================================================
Migrated from v5: replaces all `if agency_id == 1480:` hardcoded forks
with config-driven routing via IMPRESSION_JOIN_STRATEGY from REF_ADVERTISER_CONFIG.

ROUTING KEY: IMPRESSION_JOIN_STRATEGY
  - ADM_PREFIX  → row-level COUNT DISTINCT on PARAMOUNT_IMPRESSIONS_REPORT_90_DAYS
  - PCM_4KEY    → pre-aggregated SUM on CAMPAIGN_PERFORMANCE_REPORT_WEEKLY_STATS
  - DIRECT_AG   → web pixel direct (no impression join)

All SQL queries preserved exactly from v5 — only the routing logic changed.
"""
from flask import Flask, jsonify, request
from flask_cors import CORS
import snowflake.connector
import os
from datetime import datetime, timedelta, date
import re
import time
import threading

from optimizer_v6_migration import (
    get_agency_config,
    get_impression_strategy,
    is_row_level_agency,
    get_agency_name,
    get_agency_capabilities,
    STRATEGY_ADM_PREFIX,
    STRATEGY_PCM_4KEY,
    STRATEGY_DIRECT_AG,
)

app = Flask(__name__, static_folder=os.path.dirname(os.path.abspath(__file__)), static_url_path='')
CORS(app)


# =============================================================================
# IN-MEMORY CACHE for slow endpoints (traffic-sources scans 310M row table)
# =============================================================================
_cache = {}
_cache_lock = threading.Lock()
CACHE_TTL = 600  # 10 minutes

def cache_get(key):
    with _cache_lock:
        entry = _cache.get(key)
        if entry and time.time() - entry['ts'] < CACHE_TTL:
            return entry['data']
        elif entry:
            del _cache[key]
    return None

def cache_set(key, data):
    with _cache_lock:
        _cache[key] = {'data': data, 'ts': time.time()}
        if len(_cache) > 200:
            cutoff = time.time() - CACHE_TTL
            expired = [k for k, v in _cache.items() if v['ts'] < cutoff]
            for k in expired:
                del _cache[k]


def get_snowflake_connection(retries=2):
    last_err = None
    for attempt in range(retries + 1):
        try:
            return snowflake.connector.connect(
                user=os.environ.get('SNOWFLAKE_USER'),
                password=os.environ.get('SNOWFLAKE_PASSWORD'),
                account=os.environ.get('SNOWFLAKE_ACCOUNT'),
                warehouse=os.environ.get('SNOWFLAKE_WAREHOUSE', 'COMPUTE_WH'),
                database=os.environ.get('SNOWFLAKE_DATABASE', 'QUORUMDB'),
                schema=os.environ.get('SNOWFLAKE_SCHEMA', 'SEGMENT_DATA'),
                role=os.environ.get('SNOWFLAKE_ROLE', 'OPTIMIZER_READONLY_ROLE'),
                insecure_mode=True
            )
        except Exception as e:
            last_err = e
            if attempt < retries and ('certificate' in str(e).lower() or '254007' in str(e)):
                time.sleep(1)
                continue
            raise


def get_date_range():
    end_date = request.args.get('end_date', datetime.now().strftime('%Y-%m-%d'))
    start_date = request.args.get('start_date',
        (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d'))
    return start_date, end_date


# =============================================================================
# HEALTH CHECK
# =============================================================================
@app.route('/health', methods=['GET'])
def health_check():
    return jsonify({
        'status': 'healthy',
        'version': '6.0-config-driven',
        'description': 'Config-driven routing via IMPRESSION_JOIN_STRATEGY. No hardcoded agency forks.',
        'routing': {
            'ADM_PREFIX': 'Row-level COUNT DISTINCT on PARAMOUNT_IMPRESSIONS_REPORT_90_DAYS',
            'PCM_4KEY': 'Pre-aggregated SUM on CAMPAIGN_PERFORMANCE_REPORT_WEEKLY_STATS',
            'DIRECT_AG': 'Web pixel direct (no impression join)',
        },
        'endpoints': [
            '/api/v6/agencies', '/api/v6/advertisers', '/api/v6/campaign-performance',
            '/api/v6/lineitem-performance', '/api/v6/creative-performance',
            '/api/v6/publisher-performance', '/api/v6/zip-performance',
            '/api/v6/dma-performance', '/api/v6/summary', '/api/v6/timeseries',
            '/api/v6/lift-analysis', '/api/v6/traffic-sources',
            '/api/v6/optimize', '/api/v6/optimize-geo',
            '/api/v6/agency-timeseries', '/api/v6/advertiser-timeseries',
            '/api/v6/pipeline-health', '/api/v6/table-access',
        ]
    })


# =============================================================================
# WEB VISIT ENRICHMENT (from Ali's pre-matched AD_TO_WEB_VISIT_ATTRIBUTION)
# =============================================================================
def enrich_web_visits_agency(cursor, start_date, end_date):
    """Return {agency_id: web_visit_count} from the pre-matched attribution table."""
    try:
        cursor.execute("""
            SELECT AD_IMPRESSION_AGENCY_ID, COUNT(DISTINCT MAID || '|' || WEB_VISIT_DATE) as WEB_VISITS
            FROM QUORUMDB.DERIVED_TABLES.AD_TO_WEB_VISIT_ATTRIBUTION
            WHERE WEB_VISIT_DATE BETWEEN %(start_date)s AND %(end_date)s
            GROUP BY AD_IMPRESSION_AGENCY_ID
        """, {'start_date': start_date, 'end_date': end_date})
        return {int(r[0]): int(r[1]) for r in cursor.fetchall()}
    except Exception:
        return {}


def enrich_web_visits_advertiser(cursor, agency_id, advertiser_id, start_date, end_date):
    """Return total web visits for a specific advertiser from the pre-matched table."""
    try:
        cursor.execute("""
            SELECT COUNT(DISTINCT MAID || '|' || WEB_VISIT_DATE) as WEB_VISITS
            FROM QUORUMDB.DERIVED_TABLES.AD_TO_WEB_VISIT_ATTRIBUTION
            WHERE AD_IMPRESSION_AGENCY_ID = %(agency_id)s
              AND AD_IMPRESSION_ADVERTISER_ID = %(advertiser_id)s
              AND WEB_VISIT_DATE BETWEEN %(start_date)s AND %(end_date)s
        """, {'agency_id': agency_id, 'advertiser_id': advertiser_id,
              'start_date': start_date, 'end_date': end_date})
        row = cursor.fetchone()
        return int(row[0]) if row else 0
    except Exception:
        return 0


def enrich_web_visits_timeseries(cursor, agency_id, advertiser_id, start_date, end_date):
    """Return {date_str: web_visit_count} for timeseries enrichment."""
    try:
        cursor.execute("""
            SELECT WEB_VISIT_DATE, COUNT(DISTINCT MAID) as WEB_VISITS
            FROM QUORUMDB.DERIVED_TABLES.AD_TO_WEB_VISIT_ATTRIBUTION
            WHERE AD_IMPRESSION_AGENCY_ID = %(agency_id)s
              AND AD_IMPRESSION_ADVERTISER_ID = %(advertiser_id)s
              AND WEB_VISIT_DATE BETWEEN %(start_date)s AND %(end_date)s
            GROUP BY WEB_VISIT_DATE
        """, {'agency_id': agency_id, 'advertiser_id': advertiser_id,
              'start_date': start_date, 'end_date': end_date})
        return {str(r[0]): int(r[1]) for r in cursor.fetchall()}
    except Exception:
        return {}


# =============================================================================
# STORE VISIT ENRICHMENT (from Ali's WEB_TO_STORE_VISIT_ATTRIBUTION)
# Used for ADM_PREFIX agencies whose impression query doesn't include store visits.
# PCM_4KEY agencies already get store visits from CAMPAIGN_PERFORMANCE_REPORT_WEEKLY_STATS.
# =============================================================================
def enrich_store_visits_agency(cursor, start_date, end_date):
    """Return {agency_id: unique_store_visitor_count} from the pre-matched store attribution table."""
    try:
        cursor.execute("""
            SELECT AGENCY_ID, COUNT(DISTINCT MAID) as STORE_VISITS
            FROM QUORUMDB.DERIVED_TABLES.WEB_TO_STORE_VISIT_ATTRIBUTION
            WHERE STORE_VISIT_DATE BETWEEN %(start_date)s AND %(end_date)s
            GROUP BY AGENCY_ID
        """, {'start_date': start_date, 'end_date': end_date})
        return {int(r[0]): int(r[1]) for r in cursor.fetchall()}
    except Exception:
        return {}


def enrich_store_visits_advertiser(cursor, agency_id, advertiser_id, start_date, end_date):
    """Return unique store visitor count for a specific advertiser from the pre-matched table."""
    try:
        cursor.execute("""
            SELECT COUNT(DISTINCT MAID) as STORE_VISITS
            FROM QUORUMDB.DERIVED_TABLES.WEB_TO_STORE_VISIT_ATTRIBUTION
            WHERE AGENCY_ID = %(agency_id)s
              AND ADVERTISER_ID = %(advertiser_id)s
              AND STORE_VISIT_DATE BETWEEN %(start_date)s AND %(end_date)s
        """, {'agency_id': agency_id, 'advertiser_id': advertiser_id,
              'start_date': start_date, 'end_date': end_date})
        row = cursor.fetchone()
        return int(row[0]) if row else 0
    except Exception:
        return 0


def enrich_store_visits_timeseries(cursor, agency_id, advertiser_id, start_date, end_date):
    """Return {date_str: unique_store_visitor_count} for timeseries enrichment."""
    try:
        cursor.execute("""
            SELECT STORE_VISIT_DATE, COUNT(DISTINCT MAID) as STORE_VISITS
            FROM QUORUMDB.DERIVED_TABLES.WEB_TO_STORE_VISIT_ATTRIBUTION
            WHERE AGENCY_ID = %(agency_id)s
              AND ADVERTISER_ID = %(advertiser_id)s
              AND STORE_VISIT_DATE BETWEEN %(start_date)s AND %(end_date)s
            GROUP BY STORE_VISIT_DATE
        """, {'agency_id': agency_id, 'advertiser_id': advertiser_id,
              'start_date': start_date, 'end_date': end_date})
        return {str(r[0]): int(r[1]) for r in cursor.fetchall()}
    except Exception:
        return {}


# =============================================================================
# AGENCY OVERVIEW
# =============================================================================
@app.route('/api/v6/agencies', methods=['GET'])
def get_agencies():
    try:
        start_date, end_date = get_date_range()
        conn = get_snowflake_connection()
        cursor = conn.cursor()
        config = get_agency_config(conn)

        # --- PCM_4KEY agencies: pre-aggregated path ---
        cursor.execute("""
            SELECT AGENCY_ID, SUM(IMPRESSIONS) as IMPRESSIONS,
                SUM(VISITORS) as STORE_VISITS, 0 as WEB_VISITS,
                COUNT(DISTINCT ADVERTISER_ID) as ADVERTISER_COUNT,
                MIN(LOG_DATE) as MIN_DATE, MAX(LOG_DATE) as MAX_DATE
            FROM QUORUMDB.SEGMENT_DATA.CAMPAIGN_PERFORMANCE_REPORT_WEEKLY_STATS
            WHERE LOG_DATE BETWEEN %(start_date)s AND %(end_date)s
            GROUP BY AGENCY_ID
            HAVING SUM(IMPRESSIONS) > 0 OR SUM(VISITORS) > 0
        """, {'start_date': start_date, 'end_date': end_date})

        columns = [desc[0] for desc in cursor.description]
        all_results = []

        for row in cursor.fetchall():
            d = dict(zip(columns, row))
            agency_id = d['AGENCY_ID']
            agency_cfg = config.get(agency_id, {})
            strategy = agency_cfg.get('impression_join_strategy', STRATEGY_PCM_4KEY)
            if strategy == STRATEGY_PCM_4KEY:
                d['AGENCY_NAME'] = agency_cfg.get('name', f'Agency {agency_id}')
                d['STORE_VISIT_RATE'] = round(
                    (d['STORE_VISITS'] or 0) * 100.0 / d['IMPRESSIONS'], 4
                ) if d.get('IMPRESSIONS') else 0
                d['IMPRESSION_STRATEGY'] = STRATEGY_PCM_4KEY
                all_results.append(d)

        # --- ADM_PREFIX agencies: row-level via V2+PCM join ---
        row_level_agencies = [aid for aid, c in config.items()
                              if c.get('impression_join_strategy') == STRATEGY_ADM_PREFIX]

        if row_level_agencies:
            rl_ids = ','.join(str(int(a)) for a in row_level_agencies)
            cursor.execute(f"""
                SELECT m.AGENCY_ID,
                       COUNT(*) as IMPRESSIONS,
                       0 as STORE_VISITS,
                       0 as WEB_VISITS,
                       COUNT(DISTINCT m.QUORUM_ADVERTISER_ID) as ADVERTISER_COUNT,
                       MIN(v.AUCTION_TIMESTAMP::DATE) as MIN_DATE,
                       MAX(v.AUCTION_TIMESTAMP::DATE) as MAX_DATE
                FROM QUORUMDB.BASE_TABLES.AD_IMPRESSION_LOG_V2 v
                JOIN (
                    SELECT DSP_ADVERTISER_ID, AGENCY_ID,
                           MAX(QUORUM_ADVERTISER_ID) as QUORUM_ADVERTISER_ID
                    FROM QUORUMDB.REF_DATA.PIXEL_CAMPAIGN_MAPPING_V2
                    WHERE AGENCY_ID IN ({rl_ids})
                      AND QUORUM_ADVERTISER_ID IS NOT NULL AND QUORUM_ADVERTISER_ID != 0
                    GROUP BY DSP_ADVERTISER_ID, AGENCY_ID
                ) m ON v.DSP_ADVERTISER_ID = m.DSP_ADVERTISER_ID
                   AND v.AGENCY_ID = m.AGENCY_ID
                WHERE v.AGENCY_ID IN ({rl_ids})
                  AND v.AUCTION_TIMESTAMP::DATE BETWEEN %(start_date)s AND %(end_date)s
                GROUP BY m.AGENCY_ID
            """, {'start_date': start_date, 'end_date': end_date})

            for row in cursor.fetchall():
                agency_id_val = row[0]
                imps = row[1] or 0
                store = row[2] or 0
                web = row[3] or 0
                all_results.append({
                    'AGENCY_ID': agency_id_val,
                    'AGENCY_NAME': config.get(agency_id_val, {}).get('name', f'Agency {agency_id_val}'),
                    'IMPRESSIONS': imps,
                    'STORE_VISITS': store,
                    'WEB_VISITS': web,
                    'ADVERTISER_COUNT': row[4] or 0,
                    'STORE_VISIT_RATE': round(store * 100.0 / imps, 4) if imps else 0,
                    'WEB_VISIT_RATE': round(web * 100.0 / imps, 4) if imps else 0,
                    'MIN_DATE': str(row[5]) if row[5] else None,
                    'MAX_DATE': str(row[6]) if row[6] else None,
                    'IMPRESSION_STRATEGY': STRATEGY_ADM_PREFIX,
                })

        # Enrich with web visits from pre-matched attribution table
        web_by_agency = enrich_web_visits_agency(cursor, start_date, end_date)
        for r in all_results:
            aid = r.get('AGENCY_ID')
            wv = web_by_agency.get(aid, 0)
            r['WEB_VISITS'] = wv
            imps = r.get('IMPRESSIONS') or 0
            r['WEB_VISIT_RATE'] = round(wv * 100.0 / imps, 4) if imps > 0 else 0

        # Enrich ADM_PREFIX agencies with store visits from WEB_TO_STORE_VISIT_ATTRIBUTION
        # (PCM_4KEY agencies already have store visits baked into the weekly stats)
        store_by_agency = enrich_store_visits_agency(cursor, start_date, end_date)
        for r in all_results:
            if r.get('IMPRESSION_STRATEGY') == STRATEGY_ADM_PREFIX:
                aid = r.get('AGENCY_ID')
                sv = store_by_agency.get(aid, 0)
                r['STORE_VISITS'] = sv
                imps = r.get('IMPRESSIONS') or 0
                r['STORE_VISIT_RATE'] = round(sv * 100.0 / imps, 4) if imps > 0 else 0

        all_results.sort(key=lambda x: x.get('IMPRESSIONS', 0) or 0, reverse=True)
        cursor.close()
        conn.close()
        return jsonify({'success': True, 'data': all_results})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


# =============================================================================
# ADVERTISERS
# =============================================================================
@app.route('/api/v6/advertisers', methods=['GET'])
def get_advertisers():
    try:
        agency_id = request.args.get('agency_id')
        if not agency_id:
            return jsonify({'success': False, 'error': 'agency_id required'}), 400

        agency_id = int(agency_id)
        start_date, end_date = get_date_range()
        conn = get_snowflake_connection()
        cursor = conn.cursor()
        strategy = get_impression_strategy(agency_id, conn)

        if strategy == STRATEGY_ADM_PREFIX:
            # Row-level: join V2 impressions to PCM campaign mapping at query time
            cursor.execute("""
                SELECT
                    m.QUORUM_ADVERTISER_ID as ADVERTISER_ID,
                    COALESCE(MAX(aa.COMP_NAME), 'Advertiser ' || m.QUORUM_ADVERTISER_ID) as ADVERTISER_NAME,
                    COUNT(*) as IMPRESSIONS,
                    0 as STORE_VISITS,
                    0 as WEB_VISITS,
                    MIN(v.AUCTION_TIMESTAMP::DATE) as MIN_DATE,
                    MAX(v.AUCTION_TIMESTAMP::DATE) as MAX_DATE
                FROM QUORUMDB.BASE_TABLES.AD_IMPRESSION_LOG_V2 v
                JOIN (
                    SELECT DSP_ADVERTISER_ID, AGENCY_ID,
                           MAX(QUORUM_ADVERTISER_ID) as QUORUM_ADVERTISER_ID
                    FROM QUORUMDB.REF_DATA.PIXEL_CAMPAIGN_MAPPING_V2
                    WHERE AGENCY_ID = %(agency_id)s
                      AND QUORUM_ADVERTISER_ID IS NOT NULL AND QUORUM_ADVERTISER_ID != 0
                    GROUP BY DSP_ADVERTISER_ID, AGENCY_ID
                ) m ON v.DSP_ADVERTISER_ID = m.DSP_ADVERTISER_ID
                   AND v.AGENCY_ID = m.AGENCY_ID
                LEFT JOIN QUORUMDB.SEGMENT_DATA.AGENCY_ADVERTISER aa
                    ON m.QUORUM_ADVERTISER_ID = aa.ID
                WHERE v.AGENCY_ID = %(agency_id)s
                  AND v.AUCTION_TIMESTAMP::DATE BETWEEN %(start_date)s AND %(end_date)s
                GROUP BY m.QUORUM_ADVERTISER_ID
                HAVING COUNT(*) > 0
                ORDER BY 3 DESC
            """, {'agency_id': agency_id, 'start_date': start_date, 'end_date': end_date})
        else:
            # Pre-aggregated path
            cursor.execute("""
                SELECT
                    w.ADVERTISER_ID,
                    COALESCE(MAX(aa.COMP_NAME), 'Advertiser ' || w.ADVERTISER_ID) as ADVERTISER_NAME,
                    SUM(w.IMPRESSIONS) as IMPRESSIONS,
                    SUM(w.VISITORS) as STORE_VISITS,
                    0 as WEB_VISITS,
                    MIN(w.LOG_DATE) as MIN_DATE, MAX(w.LOG_DATE) as MAX_DATE
                FROM QUORUMDB.SEGMENT_DATA.CAMPAIGN_PERFORMANCE_REPORT_WEEKLY_STATS w
                LEFT JOIN QUORUMDB.SEGMENT_DATA.AGENCY_ADVERTISER aa ON w.ADVERTISER_ID = aa.ID
                WHERE w.AGENCY_ID = %(agency_id)s
                  AND w.LOG_DATE BETWEEN %(start_date)s AND %(end_date)s
                GROUP BY w.ADVERTISER_ID
                HAVING SUM(w.IMPRESSIONS) > 0 OR SUM(w.VISITORS) > 0
                ORDER BY SUM(w.IMPRESSIONS) DESC
            """, {'agency_id': agency_id, 'start_date': start_date, 'end_date': end_date})

        columns = [desc[0] for desc in cursor.description]
        results = []
        for row in cursor.fetchall():
            d = dict(zip(columns, row))
            if d.get('ADVERTISER_NAME') and strategy == STRATEGY_ADM_PREFIX:
                d['ADVERTISER_NAME'] = re.sub(r'^[0-9A-Za-z]+ - ', '', d['ADVERTISER_NAME'])
            d['IMPRESSION_STRATEGY'] = strategy
            results.append(d)

        # Enrich with web visits from pre-matched attribution table
        try:
            cursor.execute("""
                SELECT AD_IMPRESSION_ADVERTISER_ID, COUNT(DISTINCT MAID || '|' || WEB_VISIT_DATE) as WEB_VISITS
                FROM QUORUMDB.DERIVED_TABLES.AD_TO_WEB_VISIT_ATTRIBUTION
                WHERE AD_IMPRESSION_AGENCY_ID = %(agency_id)s
                  AND WEB_VISIT_DATE BETWEEN %(start_date)s AND %(end_date)s
                GROUP BY AD_IMPRESSION_ADVERTISER_ID
            """, {'agency_id': agency_id, 'start_date': start_date, 'end_date': end_date})
            web_by_adv = {int(r[0]): int(r[1]) for r in cursor.fetchall()}
        except Exception:
            web_by_adv = {}

        # Enrich ADM_PREFIX advertisers with store visits from WEB_TO_STORE_VISIT_ATTRIBUTION
        store_by_adv = {}
        if strategy == STRATEGY_ADM_PREFIX:
            try:
                cursor.execute("""
                    SELECT ADVERTISER_ID, COUNT(DISTINCT MAID) as STORE_VISITS
                    FROM QUORUMDB.DERIVED_TABLES.WEB_TO_STORE_VISIT_ATTRIBUTION
                    WHERE AGENCY_ID = %(agency_id)s
                      AND STORE_VISIT_DATE BETWEEN %(start_date)s AND %(end_date)s
                    GROUP BY ADVERTISER_ID
                """, {'agency_id': agency_id, 'start_date': start_date, 'end_date': end_date})
                store_by_adv = {int(r[0]): int(r[1]) for r in cursor.fetchall()}
            except Exception:
                store_by_adv = {}

        for d in results:
            imps = d.get('IMPRESSIONS') or 0
            store = d.get('STORE_VISITS') or 0
            # Override store visits for ADM_PREFIX from attribution table
            if strategy == STRATEGY_ADM_PREFIX:
                store = store_by_adv.get(d.get('ADVERTISER_ID'), 0)
                d['STORE_VISITS'] = store
            web = web_by_adv.get(d.get('ADVERTISER_ID'), 0)
            d['WEB_VISITS'] = web
            d['STORE_VISIT_RATE'] = round(store * 100.0 / imps, 4) if imps > 0 else 0
            d['WEB_VISIT_RATE'] = round(web * 100.0 / imps, 4) if imps > 0 else 0

        cursor.close()
        conn.close()
        return jsonify({'success': True, 'data': results})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


# =============================================================================
# CAMPAIGN PERFORMANCE
# =============================================================================
@app.route('/api/v6/campaign-performance', methods=['GET'])
def get_campaign_performance():
    try:
        agency_id = request.args.get('agency_id')
        advertiser_id = request.args.get('advertiser_id')

        if not agency_id or not advertiser_id:
            return jsonify({'success': False, 'error': 'agency_id and advertiser_id required'}), 400

        agency_id = int(agency_id)
        start_date, end_date = get_date_range()
        conn = get_snowflake_connection()
        cursor = conn.cursor()
        strategy = get_impression_strategy(agency_id, conn)

        if strategy == STRATEGY_ADM_PREFIX:
            cursor.execute("""
                SELECT
                    v.INSERTION_ORDER_ID, MAX(v.INSERTION_ORDER_NAME) as IO_NAME,
                    COUNT(*) as IMPRESSIONS,
                    0 as STORE_VISITS,
                    0 as WEB_VISITS
                FROM QUORUMDB.BASE_TABLES.AD_IMPRESSION_LOG_V2 v
                JOIN (
                    SELECT DISTINCT DSP_ADVERTISER_ID
                    FROM QUORUMDB.REF_DATA.PIXEL_CAMPAIGN_MAPPING_V2
                    WHERE QUORUM_ADVERTISER_ID = %(advertiser_id)s
                      AND AGENCY_ID = %(agency_id)s
                ) pcm ON v.DSP_ADVERTISER_ID = pcm.DSP_ADVERTISER_ID
                WHERE v.AGENCY_ID = %(agency_id)s
                  AND v.AUCTION_TIMESTAMP::DATE BETWEEN %(start_date)s AND %(end_date)s
                GROUP BY v.INSERTION_ORDER_ID
                HAVING COUNT(*) >= 100
                ORDER BY 3 DESC
            """, {'advertiser_id': advertiser_id, 'agency_id': agency_id, 'start_date': start_date, 'end_date': end_date})
        else:
            cursor.execute("""
                SELECT
                    CAST(IO_ID AS NUMBER) as IO_ID, MAX(IO_NAME) as IO_NAME,
                    SUM(IMPRESSIONS) as IMPRESSIONS,
                    SUM(VISITORS) as STORE_VISITS, 0 as WEB_VISITS
                FROM QUORUMDB.SEGMENT_DATA.CAMPAIGN_PERFORMANCE_REPORT_WEEKLY_STATS
                WHERE AGENCY_ID = %(agency_id)s AND ADVERTISER_ID = %(advertiser_id)s
                  AND LOG_DATE BETWEEN %(start_date)s AND %(end_date)s
                GROUP BY IO_ID
                HAVING SUM(IMPRESSIONS) >= 100 OR SUM(VISITORS) >= 10
                ORDER BY 3 DESC
            """, {'agency_id': agency_id, 'advertiser_id': advertiser_id,
                  'start_date': start_date, 'end_date': end_date})

        columns = [desc[0] for desc in cursor.description]
        results = [dict(zip(columns, row)) for row in cursor.fetchall()]
        cursor.close()
        conn.close()
        return jsonify({'success': True, 'data': results, 'strategy': strategy})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


# =============================================================================
# LINEITEM PERFORMANCE
# =============================================================================
@app.route('/api/v6/lineitem-performance', methods=['GET'])
def get_lineitem_performance():
    try:
        agency_id = request.args.get('agency_id')
        advertiser_id = request.args.get('advertiser_id')
        campaign_id = request.args.get('campaign_id')

        if not agency_id or not advertiser_id:
            return jsonify({'success': False, 'error': 'agency_id and advertiser_id required'}), 400

        agency_id = int(agency_id)
        start_date, end_date = get_date_range()
        conn = get_snowflake_connection()
        cursor = conn.cursor()
        strategy = get_impression_strategy(agency_id, conn)

        if strategy == STRATEGY_ADM_PREFIX:
            filters = ""
            if campaign_id: filters += f" AND v.INSERTION_ORDER_ID = '{campaign_id}'"

            query = f"""
                SELECT
                    v.LINE_ITEM_ID, MAX(v.LINE_ITEM_NAME) as LINEITEM_NAME,
                    v.INSERTION_ORDER_ID, MAX(v.INSERTION_ORDER_NAME) as IO_NAME,
                    MAX(v.DSP_PLATFORM_TYPE) as PLATFORM_TYPE,
                    COUNT(*) as IMPRESSIONS,
                    0 as STORE_VISITS,
                    0 as WEB_VISITS
                FROM QUORUMDB.BASE_TABLES.AD_IMPRESSION_LOG_V2 v
                JOIN (
                    SELECT DISTINCT DSP_ADVERTISER_ID
                    FROM QUORUMDB.REF_DATA.PIXEL_CAMPAIGN_MAPPING_V2
                    WHERE QUORUM_ADVERTISER_ID = %(advertiser_id)s
                      AND AGENCY_ID = %(agency_id)s
                ) pcm ON v.DSP_ADVERTISER_ID = pcm.DSP_ADVERTISER_ID
                WHERE v.AGENCY_ID = %(agency_id)s
                  AND v.AUCTION_TIMESTAMP::DATE BETWEEN %(start_date)s AND %(end_date)s {filters}
                GROUP BY v.LINE_ITEM_ID, v.INSERTION_ORDER_ID
                HAVING COUNT(*) >= 50
                ORDER BY 6 DESC
            """
            cursor.execute(query, {'advertiser_id': advertiser_id, 'agency_id': agency_id,
                                   'start_date': start_date, 'end_date': end_date})
        else:
            filters = ""
            if campaign_id: filters += f" AND IO_ID = '{campaign_id}'"

            query = f"""
                SELECT
                    LI_ID as LINEITEM_ID, MAX(LI_NAME) as LINEITEM_NAME,
                    IO_ID, MAX(IO_NAME) as IO_NAME,
                    MAX(x.PT) as PLATFORM_TYPE,
                    SUM(w.IMPRESSIONS) as IMPRESSIONS,
                    SUM(w.VISITORS) as STORE_VISITS, 0 as WEB_VISITS
                FROM QUORUMDB.SEGMENT_DATA.CAMPAIGN_PERFORMANCE_REPORT_WEEKLY_STATS w
                LEFT JOIN (
                    SELECT DISTINCT ADVERTISER_ID, LINE_ITEM_ID, PT
                    FROM QUORUMDB.SEGMENT_DATA.XANDR_IMPRESSION_LOG
                    WHERE LOG_DATE >= DATEADD(day, -7, CURRENT_DATE)
                ) x ON w.ADVERTISER_ID = x.ADVERTISER_ID AND w.LI_ID = x.LINE_ITEM_ID
                WHERE w.AGENCY_ID = %(agency_id)s AND w.ADVERTISER_ID = %(advertiser_id)s
                  AND w.LOG_DATE BETWEEN %(start_date)s AND %(end_date)s {filters}
                GROUP BY LI_ID, IO_ID
                HAVING SUM(w.IMPRESSIONS) >= 50 OR SUM(w.VISITORS) >= 5
                ORDER BY 6 DESC
            """
            cursor.execute(query, {'agency_id': agency_id, 'advertiser_id': advertiser_id,
                                   'start_date': start_date, 'end_date': end_date})

        columns = [desc[0] for desc in cursor.description]
        results = [dict(zip(columns, row)) for row in cursor.fetchall()]
        cursor.close()
        conn.close()
        return jsonify({'success': True, 'data': results, 'strategy': strategy})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


# =============================================================================
# CREATIVE PERFORMANCE
# =============================================================================
@app.route('/api/v6/creative-performance', methods=['GET'])
def get_creative_performance():
    try:
        agency_id = request.args.get('agency_id')
        advertiser_id = request.args.get('advertiser_id')
        campaign_id = request.args.get('campaign_id')
        lineitem_id = request.args.get('lineitem_id')

        if not agency_id or not advertiser_id:
            return jsonify({'success': False, 'error': 'agency_id and advertiser_id required'}), 400

        agency_id = int(agency_id)
        start_date, end_date = get_date_range()
        conn = get_snowflake_connection()
        cursor = conn.cursor()
        strategy = get_impression_strategy(agency_id, conn)

        if strategy == STRATEGY_ADM_PREFIX:
            filters = ""
            if campaign_id: filters += f" AND v.INSERTION_ORDER_ID = '{campaign_id}'"
            if lineitem_id: filters += f" AND v.LINE_ITEM_ID = '{lineitem_id}'"

            query = f"""
                WITH bounce_data AS (
                    SELECT
                        CREATIVE_NAME, IP,
                        COUNT(DISTINCT WEB_IMPRESSION_ID) as page_views
                    FROM QUORUMDB.SEGMENT_DATA.WEB_VISITORS_TO_LOG
                    WHERE QUORUM_ADVERTISER_ID = %(advertiser_id_str)s
                      AND IS_SITE_VISIT = 'TRUE'
                      AND SITE_VISIT_TIMESTAMP BETWEEN %(start_date)s AND %(end_date)s
                      AND MAID IS NOT NULL AND CREATIVE_NAME IS NOT NULL
                    GROUP BY CREATIVE_NAME, IP
                )
                SELECT
                    v.CREATIVE_NAME, NULL as CREATIVE_SIZE,
                    COUNT(*) as IMPRESSIONS,
                    0 as STORE_VISITS,
                    0 as WEB_VISITS,
                    MAX(b_stats.bounce_rate) as BOUNCE_RATE,
                    MAX(b_stats.avg_pages) as AVG_PAGES
                FROM QUORUMDB.BASE_TABLES.AD_IMPRESSION_LOG_V2 v
                JOIN (
                    SELECT DISTINCT DSP_ADVERTISER_ID
                    FROM QUORUMDB.REF_DATA.PIXEL_CAMPAIGN_MAPPING_V2
                    WHERE QUORUM_ADVERTISER_ID = %(advertiser_id)s
                      AND AGENCY_ID = %(agency_id)s
                ) pcm ON v.DSP_ADVERTISER_ID = pcm.DSP_ADVERTISER_ID
                LEFT JOIN (
                    SELECT CREATIVE_NAME,
                        ROUND(SUM(CASE WHEN page_views = 1 THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0), 1) as bounce_rate,
                        ROUND(AVG(page_views), 2) as avg_pages
                    FROM bounce_data GROUP BY CREATIVE_NAME
                ) b_stats ON v.CREATIVE_NAME = b_stats.CREATIVE_NAME
                WHERE v.AGENCY_ID = %(agency_id)s
                  AND v.AUCTION_TIMESTAMP::DATE BETWEEN %(start_date)s AND %(end_date)s
                  AND v.CREATIVE_NAME IS NOT NULL {filters}
                GROUP BY v.CREATIVE_NAME
                HAVING COUNT(*) >= 100
                ORDER BY 3 DESC
            """
            cursor.execute(query, {
                'advertiser_id': int(advertiser_id),
                'agency_id': agency_id,
                'advertiser_id_str': str(advertiser_id),
                'start_date': start_date, 'end_date': end_date
            })
        else:
            filters = ""
            if campaign_id: filters += f" AND w.IO_ID = '{campaign_id}'"
            if lineitem_id: filters += f" AND w.LI_ID = '{lineitem_id}'"

            query = f"""
                WITH creative_map AS (
                    SELECT DISTINCT ADVERTISER_ID, CREATIVE_ID, CREATIVE_NAME, CREATIVE_SIZE
                    FROM QUORUMDB.SEGMENT_DATA.XANDR_IMPRESSION_LOG
                    WHERE LOG_DATE >= DATEADD(day, -7, CURRENT_DATE)
                      AND CREATIVE_NAME IS NOT NULL
                )
                SELECT
                    COALESCE(cm.CREATIVE_NAME, 'Creative ' || w.CREATIVE_ID) as CREATIVE_NAME,
                    MAX(cm.CREATIVE_SIZE) as CREATIVE_SIZE,
                    SUM(w.IMPRESSIONS) as IMPRESSIONS,
                    SUM(w.VISITORS) as STORE_VISITS, 0 as WEB_VISITS,
                    NULL as BOUNCE_RATE, NULL as AVG_PAGES
                FROM QUORUMDB.SEGMENT_DATA.CAMPAIGN_PERFORMANCE_REPORT_WEEKLY_STATS w
                LEFT JOIN creative_map cm ON w.ADVERTISER_ID = cm.ADVERTISER_ID AND w.CREATIVE_ID = cm.CREATIVE_ID
                WHERE w.AGENCY_ID = %(agency_id)s AND w.ADVERTISER_ID = %(advertiser_id)s
                  AND w.LOG_DATE BETWEEN %(start_date)s AND %(end_date)s {filters}
                GROUP BY COALESCE(cm.CREATIVE_NAME, 'Creative ' || w.CREATIVE_ID)
                HAVING SUM(w.IMPRESSIONS) >= 100
                ORDER BY 3 DESC
            """
            cursor.execute(query, {'agency_id': agency_id, 'advertiser_id': advertiser_id,
                                   'start_date': start_date, 'end_date': end_date})

        columns = [desc[0] for desc in cursor.description]
        results = []
        note = None
        for row in cursor.fetchall():
            d = dict(zip(columns, row))
            imps = d.get('IMPRESSIONS') or 0
            store = d.get('STORE_VISITS') or 0
            web = d.get('WEB_VISITS') or 0
            d['STORE_VISIT_RATE'] = round(store * 100.0 / imps, 4) if imps > 0 else 0
            d['WEB_VISIT_RATE'] = round(web * 100.0 / imps, 4) if imps > 0 else 0
            results.append(d)

        if strategy != STRATEGY_ADM_PREFIX:
            note = 'Bounce rate and avg pages not available for pre-aggregated agencies'

        cursor.close()
        conn.close()
        return jsonify({'success': True, 'data': results, 'strategy': strategy, 'note': note})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


# =============================================================================
# PUBLISHER PERFORMANCE
# =============================================================================
@app.route('/api/v6/publisher-performance', methods=['GET'])
def get_publisher_performance():
    try:
        agency_id = request.args.get('agency_id')
        advertiser_id = request.args.get('advertiser_id')
        campaign_id = request.args.get('campaign_id')
        lineitem_id = request.args.get('lineitem_id')

        if not agency_id or not advertiser_id:
            return jsonify({'success': False, 'error': 'agency_id and advertiser_id required'}), 400

        agency_id = int(agency_id)
        start_date, end_date = get_date_range()
        conn = get_snowflake_connection()
        cursor = conn.cursor()
        strategy = get_impression_strategy(agency_id, conn)

        if strategy == STRATEGY_ADM_PREFIX:
            filters = ""
            if campaign_id: filters += f" AND v.INSERTION_ORDER_ID = '{campaign_id}'"
            if lineitem_id: filters += f" AND v.LINE_ITEM_ID = '{lineitem_id}'"

            query = f"""
                SELECT
                    v.SITE_DOMAIN as PUBLISHER,
                    COUNT(*) as IMPRESSIONS,
                    0 as STORE_VISITS,
                    0 as WEB_VISITS
                FROM QUORUMDB.BASE_TABLES.AD_IMPRESSION_LOG_V2 v
                JOIN (
                    SELECT DISTINCT DSP_ADVERTISER_ID
                    FROM QUORUMDB.REF_DATA.PIXEL_CAMPAIGN_MAPPING_V2
                    WHERE QUORUM_ADVERTISER_ID = %(advertiser_id)s
                      AND AGENCY_ID = %(agency_id)s
                ) pcm ON v.DSP_ADVERTISER_ID = pcm.DSP_ADVERTISER_ID
                WHERE v.AGENCY_ID = %(agency_id)s
                  AND v.AUCTION_TIMESTAMP::DATE BETWEEN %(start_date)s AND %(end_date)s
                  AND v.SITE_DOMAIN IS NOT NULL {filters}
                GROUP BY v.SITE_DOMAIN
                HAVING COUNT(*) >= 100
                ORDER BY 2 DESC LIMIT 50
            """
            cursor.execute(query, {'advertiser_id': advertiser_id, 'agency_id': agency_id,
                                   'start_date': start_date, 'end_date': end_date})
        else:
            filters = ""
            if campaign_id: filters += f" AND IO_ID = '{campaign_id}'"
            if lineitem_id: filters += f" AND LI_ID = '{lineitem_id}'"

            query = f"""
                SELECT
                    PUBLISHER,
                    SUM(IMPRESSIONS) as IMPRESSIONS,
                    SUM(VISITORS) as STORE_VISITS, 0 as WEB_VISITS
                FROM QUORUMDB.SEGMENT_DATA.CAMPAIGN_PERFORMANCE_REPORT_WEEKLY_STATS
                WHERE AGENCY_ID = %(agency_id)s AND ADVERTISER_ID = %(advertiser_id)s
                  AND LOG_DATE BETWEEN %(start_date)s AND %(end_date)s
                  AND PUBLISHER IS NOT NULL {filters}
                GROUP BY PUBLISHER
                HAVING SUM(IMPRESSIONS) >= 100
                ORDER BY 2 DESC LIMIT 50
            """
            cursor.execute(query, {'agency_id': agency_id, 'advertiser_id': advertiser_id,
                                   'start_date': start_date, 'end_date': end_date})

        columns = [desc[0] for desc in cursor.description]
        results = [dict(zip(columns, row)) for row in cursor.fetchall()]
        cursor.close()
        conn.close()
        return jsonify({'success': True, 'data': results, 'strategy': strategy})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


# =============================================================================
# ZIP PERFORMANCE
# =============================================================================
@app.route('/api/v6/zip-performance', methods=['GET'])
def get_zip_performance():
    try:
        agency_id = request.args.get('agency_id')
        advertiser_id = request.args.get('advertiser_id')
        campaign_id = request.args.get('campaign_id')
        lineitem_id = request.args.get('lineitem_id')

        if not agency_id or not advertiser_id:
            return jsonify({'success': False, 'error': 'agency_id and advertiser_id required'}), 400

        agency_id = int(agency_id)
        start_date, end_date = get_date_range()
        conn = get_snowflake_connection()
        cursor = conn.cursor()
        strategy = get_impression_strategy(agency_id, conn)

        note = None

        if strategy == STRATEGY_ADM_PREFIX:
            filters = ""
            if campaign_id: filters += f" AND v.INSERTION_ORDER_ID = '{campaign_id}'"
            if lineitem_id: filters += f" AND v.LINE_ITEM_ID = '{lineitem_id}'"

            query = f"""
                SELECT v.USER_POSTAL_CODE as ZIP,
                    MAX(d.CITY) as CITY, MAX(d.STATE_PROV) as STATE,
                    COUNT(*) as IMPRESSIONS,
                    0 as STORE_VISITS,
                    0 as WEB_VISITS
                FROM QUORUMDB.BASE_TABLES.AD_IMPRESSION_LOG_V2 v
                JOIN (
                    SELECT DISTINCT DSP_ADVERTISER_ID
                    FROM QUORUMDB.REF_DATA.PIXEL_CAMPAIGN_MAPPING_V2
                    WHERE QUORUM_ADVERTISER_ID = %(advertiser_id)s
                ) pcm ON v.DSP_ADVERTISER_ID = pcm.DSP_ADVERTISER_ID
                LEFT JOIN QUORUMDB.SEGMENT_DATA.DBIP_LOOKUP_US d ON v.USER_POSTAL_CODE = d.ZIPCODE
                WHERE v.AUCTION_TIMESTAMP::DATE BETWEEN %(start_date)s AND %(end_date)s {filters}
                GROUP BY v.USER_POSTAL_CODE
                HAVING COUNT(*) >= 50
                ORDER BY 4 DESC LIMIT 100
            """
            cursor.execute(query, {'advertiser_id': advertiser_id,
                                   'start_date': start_date, 'end_date': end_date})
        else:
            filters = ""
            if campaign_id: filters += f" AND IO_ID = '{campaign_id}'"
            if lineitem_id: filters += f" AND LI_ID = '{lineitem_id}'"

            # Try CAMPAIGN_POSTAL_REPORTING first (has ZIP), fallback to weekly stats
            try:
                query = f"""
                    SELECT p.ZIP,
                        MAX(d.CITY) as CITY, MAX(d.STATE_PROV) as STATE,
                        SUM(p.IMPRESSIONS) as IMPRESSIONS,
                        SUM(p.VISITORS) as STORE_VISITS, 0 as WEB_VISITS
                    FROM QUORUMDB.SEGMENT_DATA.CAMPAIGN_POSTAL_REPORTING p
                    LEFT JOIN QUORUMDB.SEGMENT_DATA.DBIP_LOOKUP_US d ON p.ZIP = d.ZIPCODE
                    WHERE p.AGENCY_ID = %(agency_id)s AND p.ADVERTISER_ID = %(advertiser_id)s
                      AND p.LOG_DATE BETWEEN %(start_date)s AND %(end_date)s {filters}
                    GROUP BY p.ZIP
                    HAVING SUM(p.IMPRESSIONS) >= 50
                    ORDER BY 4 DESC LIMIT 100
                """
                cursor.execute(query, {'agency_id': agency_id, 'advertiser_id': advertiser_id,
                                       'start_date': start_date, 'end_date': end_date})
            except Exception:
                # Fallback: weekly stats has ZIP column too
                query = f"""
                    SELECT ZIP,
                        MAX(DMA) as DMA,
                        SUM(IMPRESSIONS) as IMPRESSIONS,
                        SUM(VISITORS) as STORE_VISITS, 0 as WEB_VISITS
                    FROM QUORUMDB.SEGMENT_DATA.CAMPAIGN_PERFORMANCE_REPORT_WEEKLY_STATS
                    WHERE AGENCY_ID = %(agency_id)s AND ADVERTISER_ID = %(advertiser_id)s
                      AND LOG_DATE BETWEEN %(start_date)s AND %(end_date)s
                      AND ZIP IS NOT NULL AND ZIP != '' {filters}
                    GROUP BY ZIP
                    HAVING SUM(IMPRESSIONS) >= 50
                    ORDER BY 3 DESC LIMIT 100
                """
                cursor.execute(query, {'agency_id': agency_id, 'advertiser_id': advertiser_id,
                                       'start_date': start_date, 'end_date': end_date})
                note = 'Using weekly stats fallback for ZIP data'

        columns = [desc[0] for desc in cursor.description]
        results = [dict(zip(columns, row)) for row in cursor.fetchall()]
        cursor.close()
        conn.close()
        return jsonify({'success': True, 'data': results, 'strategy': strategy, 'note': note})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


# =============================================================================
# DMA PERFORMANCE
# =============================================================================
@app.route('/api/v6/dma-performance', methods=['GET'])
def get_dma_performance():
    try:
        agency_id = request.args.get('agency_id')
        advertiser_id = request.args.get('advertiser_id')
        campaign_id = request.args.get('campaign_id')
        lineitem_id = request.args.get('lineitem_id')

        if not agency_id or not advertiser_id:
            return jsonify({'success': False, 'error': 'agency_id and advertiser_id required'}), 400

        agency_id = int(agency_id)
        start_date, end_date = get_date_range()
        conn = get_snowflake_connection()
        cursor = conn.cursor()
        strategy = get_impression_strategy(agency_id, conn)

        if strategy == STRATEGY_ADM_PREFIX:
            filters = ""
            if campaign_id: filters += f" AND v.INSERTION_ORDER_ID = '{campaign_id}'"
            if lineitem_id: filters += f" AND v.LINE_ITEM_ID = '{lineitem_id}'"

            query = f"""
                WITH zip_dma AS (
                    SELECT ZIPCODE, MAX(DMA_NAME) as DMA_NAME
                    FROM QUORUMDB.SEGMENT_DATA.DBIP_LOOKUP_US
                    WHERE DMA_NAME IS NOT NULL AND DMA_NAME != ''
                    GROUP BY ZIPCODE
                )
                SELECT d.DMA_NAME as DMA, COUNT(*) as IMPRESSIONS,
                    0 as STORE_VISITS,
                    0 as WEB_VISITS
                FROM QUORUMDB.BASE_TABLES.AD_IMPRESSION_LOG_V2 v
                JOIN (
                    SELECT DISTINCT DSP_ADVERTISER_ID
                    FROM QUORUMDB.REF_DATA.PIXEL_CAMPAIGN_MAPPING_V2
                    WHERE QUORUM_ADVERTISER_ID = %(advertiser_id)s
                ) pcm ON v.DSP_ADVERTISER_ID = pcm.DSP_ADVERTISER_ID
                JOIN zip_dma d ON v.USER_POSTAL_CODE = d.ZIPCODE
                WHERE v.AUCTION_TIMESTAMP::DATE BETWEEN %(start_date)s AND %(end_date)s {filters}
                GROUP BY d.DMA_NAME HAVING COUNT(*) >= 100 ORDER BY 2 DESC LIMIT 50
            """
            cursor.execute(query, {'advertiser_id': advertiser_id,
                                   'start_date': start_date, 'end_date': end_date})
        else:
            filters = ""
            if campaign_id: filters += f" AND IO_ID = '{campaign_id}'"
            if lineitem_id: filters += f" AND LI_ID = '{lineitem_id}'"

            query = f"""
                SELECT DMA, SUM(IMPRESSIONS) as IMPRESSIONS,
                    SUM(VISITORS) as STORE_VISITS, 0 as WEB_VISITS
                FROM QUORUMDB.SEGMENT_DATA.CAMPAIGN_PERFORMANCE_REPORT_WEEKLY_STATS
                WHERE AGENCY_ID = %(agency_id)s AND ADVERTISER_ID = %(advertiser_id)s
                  AND LOG_DATE BETWEEN %(start_date)s AND %(end_date)s
                  AND DMA IS NOT NULL AND DMA != '' {filters}
                GROUP BY DMA HAVING SUM(IMPRESSIONS) >= 100 ORDER BY 2 DESC LIMIT 50
            """
            cursor.execute(query, {'agency_id': agency_id, 'advertiser_id': advertiser_id,
                                   'start_date': start_date, 'end_date': end_date})

        columns = [desc[0] for desc in cursor.description]
        results = [dict(zip(columns, row)) for row in cursor.fetchall()]
        cursor.close()
        conn.close()
        return jsonify({'success': True, 'data': results, 'strategy': strategy})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


# =============================================================================
# SUMMARY
# =============================================================================
@app.route('/api/v6/summary', methods=['GET'])
def get_summary():
    try:
        agency_id = request.args.get('agency_id')
        advertiser_id = request.args.get('advertiser_id')

        if not agency_id or not advertiser_id:
            return jsonify({'success': False, 'error': 'agency_id and advertiser_id required'}), 400

        agency_id = int(agency_id)
        start_date, end_date = get_date_range()
        conn = get_snowflake_connection()
        cursor = conn.cursor()
        strategy = get_impression_strategy(agency_id, conn)

        if strategy == STRATEGY_ADM_PREFIX:
            query = """
                SELECT
                    COUNT(*) as IMPRESSIONS,
                    0 as STORE_VISITS,
                    0 as WEB_VISITS,
                    MIN(v.AUCTION_TIMESTAMP::DATE) as MIN_DATE,
                    MAX(v.AUCTION_TIMESTAMP::DATE) as MAX_DATE
                FROM QUORUMDB.BASE_TABLES.AD_IMPRESSION_LOG_V2 v
                JOIN (
                    SELECT DISTINCT DSP_ADVERTISER_ID
                    FROM QUORUMDB.REF_DATA.PIXEL_CAMPAIGN_MAPPING_V2
                    WHERE QUORUM_ADVERTISER_ID = %(advertiser_id)s
                ) pcm ON v.DSP_ADVERTISER_ID = pcm.DSP_ADVERTISER_ID
                WHERE v.AUCTION_TIMESTAMP::DATE BETWEEN %(start_date)s AND %(end_date)s
            """
        else:
            query = """
                SELECT SUM(IMPRESSIONS) as IMPRESSIONS, SUM(VISITORS) as STORE_VISITS,
                    0 as WEB_VISITS, MIN(LOG_DATE) as MIN_DATE, MAX(LOG_DATE) as MAX_DATE,
                    COUNT(DISTINCT IO_ID) as CAMPAIGN_COUNT, COUNT(DISTINCT LI_ID) as LINEITEM_COUNT
                FROM QUORUMDB.SEGMENT_DATA.CAMPAIGN_PERFORMANCE_REPORT_WEEKLY_STATS
                WHERE AGENCY_ID = %(agency_id)s AND ADVERTISER_ID = %(advertiser_id)s
                  AND LOG_DATE BETWEEN %(start_date)s AND %(end_date)s
            """

        cursor.execute(query, {'agency_id': agency_id, 'advertiser_id': advertiser_id,
                               'start_date': start_date, 'end_date': end_date})
        columns = [desc[0] for desc in cursor.description]
        row = cursor.fetchone()
        result = dict(zip(columns, row)) if row else {}

        # Enrich with web visits from pre-matched attribution table
        web = enrich_web_visits_advertiser(cursor, agency_id, advertiser_id, start_date, end_date)

        # Enrich ADM_PREFIX with store visits from WEB_TO_STORE_VISIT_ATTRIBUTION
        imps = result.get('IMPRESSIONS') or 0
        store = result.get('STORE_VISITS') or 0
        if strategy == STRATEGY_ADM_PREFIX:
            store = enrich_store_visits_advertiser(cursor, agency_id, advertiser_id, start_date, end_date)
            result['STORE_VISITS'] = store
        result['WEB_VISITS'] = web
        result['STORE_VISIT_RATE'] = round(store * 100.0 / imps, 4) if imps > 0 else 0
        result['WEB_VISIT_RATE'] = round(web * 100.0 / imps, 4) if imps > 0 else 0
        result['TOTAL_VISITS'] = store + web
        result['IMPRESSION_STRATEGY'] = strategy

        cursor.close()
        conn.close()
        return jsonify({'success': True, 'data': result})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


# =============================================================================
# TIMESERIES
# =============================================================================
@app.route('/api/v6/timeseries', methods=['GET'])
def get_timeseries():
    try:
        agency_id = request.args.get('agency_id')
        advertiser_id = request.args.get('advertiser_id')

        if not agency_id or not advertiser_id:
            return jsonify({'success': False, 'error': 'agency_id and advertiser_id required'}), 400

        agency_id = int(agency_id)
        start_date, end_date = get_date_range()
        conn = get_snowflake_connection()
        cursor = conn.cursor()
        strategy = get_impression_strategy(agency_id, conn)

        if strategy == STRATEGY_ADM_PREFIX:
            query = """
                SELECT
                    v.AUCTION_TIMESTAMP::DATE as LOG_DATE,
                    COUNT(*) as IMPRESSIONS,
                    0 as STORE_VISITS,
                    0 as WEB_VISITS
                FROM QUORUMDB.BASE_TABLES.AD_IMPRESSION_LOG_V2 v
                JOIN (
                    SELECT DISTINCT DSP_ADVERTISER_ID
                    FROM QUORUMDB.REF_DATA.PIXEL_CAMPAIGN_MAPPING_V2
                    WHERE QUORUM_ADVERTISER_ID = %(advertiser_id)s
                ) pcm ON v.DSP_ADVERTISER_ID = pcm.DSP_ADVERTISER_ID
                WHERE v.AUCTION_TIMESTAMP::DATE BETWEEN %(start_date)s AND %(end_date)s
                GROUP BY v.AUCTION_TIMESTAMP::DATE
                ORDER BY v.AUCTION_TIMESTAMP::DATE
            """
        else:
            query = """
                SELECT LOG_DATE, SUM(IMPRESSIONS) as IMPRESSIONS, SUM(VISITORS) as STORE_VISITS, 0 as WEB_VISITS
                FROM QUORUMDB.SEGMENT_DATA.CAMPAIGN_PERFORMANCE_REPORT_WEEKLY_STATS
                WHERE AGENCY_ID = %(agency_id)s AND ADVERTISER_ID = %(advertiser_id)s
                  AND LOG_DATE BETWEEN %(start_date)s AND %(end_date)s
                GROUP BY LOG_DATE ORDER BY LOG_DATE
            """

        cursor.execute(query, {'agency_id': agency_id, 'advertiser_id': advertiser_id,
                               'start_date': start_date, 'end_date': end_date})
        columns = [desc[0] for desc in cursor.description]
        results = []
        for row in cursor.fetchall():
            d = dict(zip(columns, row))
            if d.get('LOG_DATE'): d['LOG_DATE'] = str(d['LOG_DATE'])
            results.append(d)

        # Enrich timeseries with web visits from pre-matched attribution table
        web_ts = enrich_web_visits_timeseries(cursor, agency_id, advertiser_id, start_date, end_date)
        for d in results:
            d['WEB_VISITS'] = web_ts.get(d.get('LOG_DATE'), 0)

        # Enrich ADM_PREFIX timeseries with store visits from WEB_TO_STORE_VISIT_ATTRIBUTION
        if strategy == STRATEGY_ADM_PREFIX:
            store_ts = enrich_store_visits_timeseries(cursor, agency_id, advertiser_id, start_date, end_date)
            for d in results:
                d['STORE_VISITS'] = store_ts.get(d.get('LOG_DATE'), 0)

        cursor.close()
        conn.close()
        return jsonify({'success': True, 'data': results, 'strategy': strategy})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


# =============================================================================
# LIFT ANALYSIS
# =============================================================================
@app.route('/api/v6/lift-analysis', methods=['GET'])
def get_lift_analysis():
    try:
        agency_id = request.args.get('agency_id')
        advertiser_id = request.args.get('advertiser_id')
        group_by = request.args.get('group_by', 'campaign')

        if not agency_id or not advertiser_id:
            return jsonify({'success': False, 'error': 'agency_id and advertiser_id required'}), 400

        agency_id = int(agency_id)
        start_date, end_date = get_date_range()
        conn = get_snowflake_connection()
        cursor = conn.cursor()
        strategy = get_impression_strategy(agency_id, conn)

        if strategy == STRATEGY_ADM_PREFIX:
            if group_by == 'lineitem':
                group_cols = "v.INSERTION_ORDER_ID, v.LINE_ITEM_ID"
                name_cols = """
                    COALESCE(MAX(v.LINE_ITEM_NAME), 'LI-' || v.LINE_ITEM_ID::VARCHAR) as NAME,
                    COALESCE(MAX(v.INSERTION_ORDER_NAME), 'IO-' || v.INSERTION_ORDER_ID::VARCHAR) as PARENT_NAME,
                    v.LINE_ITEM_ID as ID, v.INSERTION_ORDER_ID as PARENT_ID,
                """
            else:
                group_cols = "v.INSERTION_ORDER_ID"
                name_cols = """
                    COALESCE(MAX(v.INSERTION_ORDER_NAME), 'IO-' || v.INSERTION_ORDER_ID::VARCHAR) as NAME,
                    NULL as PARENT_NAME, v.INSERTION_ORDER_ID as ID, NULL as PARENT_ID,
                """

            query = f"""
                WITH
                exposed_devices AS (
                    SELECT DISTINCT LOWER(REPLACE(v.DEVICE_ID_RAW,'-','')) AS device_id
                    FROM QUORUMDB.BASE_TABLES.AD_IMPRESSION_LOG_V2 v
                    JOIN (
                        SELECT DISTINCT DSP_ADVERTISER_ID
                        FROM QUORUMDB.REF_DATA.PIXEL_CAMPAIGN_MAPPING_V2
                        WHERE QUORUM_ADVERTISER_ID = %(advertiser_id)s
                    ) pcm ON v.DSP_ADVERTISER_ID = pcm.DSP_ADVERTISER_ID
                    WHERE v.AUCTION_TIMESTAMP::DATE BETWEEN %(start_date)s AND %(end_date)s AND v.DEVICE_ID_RAW IS NOT NULL
                ),
                control_devices AS (
                    SELECT DISTINCT LOWER(REPLACE(v.DEVICE_ID_RAW,'-','')) AS device_id
                    FROM QUORUMDB.BASE_TABLES.AD_IMPRESSION_LOG_V2 v
                    WHERE v.AUCTION_TIMESTAMP::DATE BETWEEN %(start_date)s AND %(end_date)s AND v.DEVICE_ID_RAW IS NOT NULL
                      AND LOWER(REPLACE(v.DEVICE_ID_RAW,'-','')) NOT IN (SELECT device_id FROM exposed_devices)
                ),
                adv_web_visit_days AS (
                    SELECT LOWER(REPLACE(MAID,'-','')) AS device_id, DATE(SITE_VISIT_TIMESTAMP) AS event_date
                    FROM QUORUMDB.SEGMENT_DATA.PARAMOUNT_SITEVISITS
                    WHERE QUORUM_ADVERTISER_ID = %(advertiser_id_str)s AND MAID IS NOT NULL
                    GROUP BY 1, 2
                ),
                adv_store_visit_days AS (
                    SELECT LOWER(REPLACE(MAID,'-','')) AS device_id, DATE(DRIVE_BY_TIME) AS event_date
                    FROM QUORUMDB.SEGMENT_DATA.PARAMOUNT_STORE_VISIT_RAW_90_DAYS
                    WHERE ADVERTISER_ID = %(advertiser_id)s AND MAID IS NOT NULL
                    GROUP BY 1, 2
                ),
                web_network_control AS (
                    SELECT COUNT(DISTINCT c.device_id) AS control_n,
                        COUNT(DISTINCT CASE WHEN v.device_id IS NOT NULL THEN c.device_id END) AS control_visitors,
                        COUNT(DISTINCT CASE WHEN v.device_id IS NOT NULL THEN c.device_id END)::FLOAT
                            / NULLIF(COUNT(DISTINCT c.device_id), 0) * 100 AS control_rate
                    FROM control_devices c LEFT JOIN adv_web_visit_days v ON v.device_id = c.device_id
                ),
                store_network_control AS (
                    SELECT COUNT(DISTINCT c.device_id) AS control_n,
                        COUNT(DISTINCT CASE WHEN v.device_id IS NOT NULL THEN c.device_id END) AS control_visitors,
                        COUNT(DISTINCT CASE WHEN v.device_id IS NOT NULL THEN c.device_id END)::FLOAT
                            / NULLIF(COUNT(DISTINCT c.device_id), 0) * 100 AS control_rate
                    FROM control_devices c LEFT JOIN adv_store_visit_days v ON v.device_id = c.device_id
                ),
                exposed_store_visitors AS (
                    SELECT COUNT(DISTINCT CASE WHEN sv.device_id IS NOT NULL THEN e.device_id END) AS store_visitors
                    FROM exposed_devices e LEFT JOIN adv_store_visit_days sv ON sv.device_id = e.device_id
                ),
                campaign_metrics AS (
                    SELECT {group_cols}, {name_cols}
                        COUNT(*) as IMPRESSIONS, COUNT(DISTINCT v.DEVICE_ID_RAW) as REACH,
                        0 as WEB_VISITORS
                    FROM QUORUMDB.BASE_TABLES.AD_IMPRESSION_LOG_V2 v
                    JOIN (
                        SELECT DISTINCT DSP_ADVERTISER_ID, INSERTION_ORDER_ID, LINE_ITEM_ID
                        FROM QUORUMDB.REF_DATA.PIXEL_CAMPAIGN_MAPPING_V2
                        WHERE QUORUM_ADVERTISER_ID = %(advertiser_id)s
                    ) pcm ON v.DSP_ADVERTISER_ID = pcm.DSP_ADVERTISER_ID
                    WHERE v.AUCTION_TIMESTAMP::DATE BETWEEN %(start_date)s AND %(end_date)s
                    GROUP BY {group_cols} HAVING COUNT(*) >= 1000
                ),
                adv_baselines AS (
                    SELECT SUM(WEB_VISITORS)::FLOAT / NULLIF(SUM(REACH), 0) * 100 as WEB_BASELINE,
                        SUM(WEB_VISITORS) as TOTAL_WEB
                    FROM campaign_metrics
                )
                SELECT c.NAME, c.PARENT_NAME, c.ID, c.PARENT_ID, c.IMPRESSIONS, c.REACH as PANEL_REACH,
                    c.WEB_VISITORS, ROUND(c.WEB_VISITORS::FLOAT / NULLIF(c.REACH, 0) * 100, 4) as WEB_VISIT_RATE,
                    ROUND(b.WEB_BASELINE, 4) as WEB_ADV_BASELINE,
                    CASE WHEN b.WEB_BASELINE > 0 THEN ROUND(c.WEB_VISITORS::FLOAT / NULLIF(c.REACH, 0) * 100 / b.WEB_BASELINE * 100, 1) END as WEB_INDEX,
                    ROUND(wnc.control_rate, 4) as WEB_NETWORK_BASELINE, wnc.control_n as WEB_CONTROL_N, wnc.control_visitors as WEB_CONTROL_VISITORS,
                    CASE WHEN wnc.control_rate > 0 THEN ROUND((c.WEB_VISITORS::FLOAT / NULLIF(c.REACH, 0) * 100 - wnc.control_rate) / wnc.control_rate * 100, 1) END as WEB_LIFT_VS_NETWORK,
                    CASE WHEN wnc.control_rate > 0 AND c.REACH > 0 AND wnc.control_n > 0 THEN ROUND(
                        (c.WEB_VISITORS::FLOAT / c.REACH - wnc.control_visitors::FLOAT / wnc.control_n) /
                        NULLIF(SQRT(((c.WEB_VISITORS + wnc.control_visitors)::FLOAT / (c.REACH + wnc.control_n)) *
                        (1 - (c.WEB_VISITORS + wnc.control_visitors)::FLOAT / (c.REACH + wnc.control_n)) *
                        (1.0/c.REACH + 1.0/wnc.control_n)), 0), 2) END as WEB_Z_SCORE,
                    esv.store_visitors as STORE_VISITORS,
                    ROUND(esv.store_visitors::FLOAT / NULLIF(c.REACH, 0) * 100, 4) as STORE_VISIT_RATE,
                    ROUND(snc.control_rate, 4) as STORE_NETWORK_BASELINE, snc.control_n as STORE_CONTROL_N, snc.control_visitors as STORE_CONTROL_VISITORS,
                    CASE WHEN snc.control_rate > 0 THEN ROUND((esv.store_visitors::FLOAT / NULLIF(c.REACH, 0) * 100 - snc.control_rate) / snc.control_rate * 100, 1) END as STORE_LIFT_VS_NETWORK,
                    CASE WHEN snc.control_rate > 0 AND c.REACH > 0 AND snc.control_n > 0 THEN ROUND(
                        (esv.store_visitors::FLOAT / c.REACH - snc.control_visitors::FLOAT / snc.control_n) /
                        NULLIF(SQRT(((esv.store_visitors + snc.control_visitors)::FLOAT / (c.REACH + snc.control_n)) *
                        (1 - (esv.store_visitors + snc.control_visitors)::FLOAT / (c.REACH + snc.control_n)) *
                        (1.0/c.REACH + 1.0/snc.control_n)), 0), 2) END as STORE_Z_SCORE,
                    b.TOTAL_WEB, esv.store_visitors as TOTAL_STORE
                FROM campaign_metrics c
                CROSS JOIN adv_baselines b CROSS JOIN web_network_control wnc
                CROSS JOIN store_network_control snc CROSS JOIN exposed_store_visitors esv
                WHERE c.REACH >= 100 ORDER BY c.IMPRESSIONS DESC LIMIT 100
            """

            cursor.execute(query, {
                'advertiser_id': int(advertiser_id), 'advertiser_id_str': str(advertiser_id),
                'start_date': start_date, 'end_date': end_date
            })

            columns = [desc[0] for desc in cursor.description]
            rows = cursor.fetchall()
            cursor.close()
            conn.close()

            if not rows:
                return jsonify({
                    'success': True, 'web_data': [], 'store_data': [],
                    'web_adv_baseline': None, 'web_network_baseline': None, 'store_network_baseline': None,
                    'web_control_n': 0, 'store_control_n': 0,
                    'total_web_visitors': 0, 'total_store_visitors': 0,
                    'strategy': strategy,
                    'message': 'No lift data available - requires minimum 1,000 impressions per campaign'
                })

            web_data, store_data = [], []
            web_adv_baseline = web_network_baseline = store_network_baseline = None
            web_control_n = store_control_n = total_web = total_store = 0

            for row in rows:
                d = dict(zip(columns, row))
                if total_web == 0:
                    total_web = int(d.get('TOTAL_WEB') or 0)
                    total_store = int(d.get('TOTAL_STORE') or 0)
                    web_adv_baseline = float(d.get('WEB_ADV_BASELINE') or 0)
                    web_network_baseline = float(d.get('WEB_NETWORK_BASELINE') or 0)
                    store_network_baseline = float(d.get('STORE_NETWORK_BASELINE') or 0)
                    web_control_n = int(d.get('WEB_CONTROL_N') or 0)
                    store_control_n = int(d.get('STORE_CONTROL_N') or 0)

                def confidence_from_z(z):
                    if z is None: return None
                    z = abs(float(z))
                    if z >= 2.576: return '99%'
                    if z >= 1.96: return '95%'
                    if z >= 1.645: return '90%'
                    return 'NS'

                web_data.append({
                    'NAME': d['NAME'], 'PARENT_NAME': d.get('PARENT_NAME'),
                    'ID': d.get('ID'), 'PARENT_ID': d.get('PARENT_ID'),
                    'IMPRESSIONS': int(d['IMPRESSIONS'] or 0), 'PANEL_REACH': int(d['PANEL_REACH'] or 0),
                    'VISITORS': int(d['WEB_VISITORS'] or 0),
                    'VISIT_RATE': float(d['WEB_VISIT_RATE'] or 0),
                    'ADV_BASELINE_VR': float(d['WEB_ADV_BASELINE'] or 0),
                    'INDEX_VS_AVG': float(d['WEB_INDEX']) if d['WEB_INDEX'] else None,
                    'NETWORK_BASELINE_VR': float(d['WEB_NETWORK_BASELINE'] or 0),
                    'LIFT_VS_NETWORK': float(d['WEB_LIFT_VS_NETWORK']) if d['WEB_LIFT_VS_NETWORK'] else None,
                    'Z_SCORE': float(d['WEB_Z_SCORE']) if d['WEB_Z_SCORE'] else None,
                    'CONFIDENCE': confidence_from_z(d.get('WEB_Z_SCORE')),
                })
                store_data.append({
                    'NAME': d['NAME'], 'PARENT_NAME': d.get('PARENT_NAME'),
                    'ID': d.get('ID'), 'PARENT_ID': d.get('PARENT_ID'),
                    'IMPRESSIONS': int(d['IMPRESSIONS'] or 0), 'PANEL_REACH': int(d['PANEL_REACH'] or 0),
                    'VISITORS': int(d['STORE_VISITORS'] or 0),
                    'VISIT_RATE': float(d['STORE_VISIT_RATE'] or 0),
                    'NETWORK_BASELINE_VR': float(d['STORE_NETWORK_BASELINE'] or 0),
                    'LIFT_VS_NETWORK': float(d['STORE_LIFT_VS_NETWORK']) if d['STORE_LIFT_VS_NETWORK'] else None,
                    'Z_SCORE': float(d['STORE_Z_SCORE']) if d['STORE_Z_SCORE'] else None,
                    'CONFIDENCE': confidence_from_z(d.get('STORE_Z_SCORE')),
                })

            return jsonify({
                'success': True, 'web_data': web_data, 'store_data': store_data,
                'web_adv_baseline': web_adv_baseline, 'web_network_baseline': web_network_baseline,
                'store_network_baseline': store_network_baseline,
                'web_control_n': web_control_n, 'store_control_n': store_control_n,
                'total_web_visitors': total_web, 'total_store_visitors': total_store,
                'strategy': strategy,
                'methodology': {
                    'index': 'Campaign visit rate vs advertiser average',
                    'lift_vs_network': 'Campaign visit rate vs network control',
                    'web_source': 'PARAMOUNT_SITEVISITS',
                    'store_source': 'PARAMOUNT_STORE_VISIT_RAW_90_DAYS'
                }
            })

        else:
            # PCM_4KEY lift: simpler model (store visits only, index-based)
            if group_by == 'lineitem':
                group_cols = "IO_ID, IO_NAME, LI_ID, LI_NAME"
                name_cols = "LI_NAME as NAME, IO_NAME as PARENT_NAME, LI_ID as ID, IO_ID as PARENT_ID,"
            else:
                group_cols = "IO_ID, IO_NAME"
                name_cols = "IO_NAME as NAME, NULL as PARENT_NAME, IO_ID as ID, NULL as PARENT_ID,"

            query = f"""
                WITH campaign_metrics AS (
                    SELECT {group_cols}, {name_cols}
                        SUM(IMPRESSIONS) as IMPRESSIONS, SUM(REACH) as REACH,
                        SUM(PANEL_REACH) as PANEL_REACH, SUM(VISITORS) as VISITORS
                    FROM QUORUMDB.SEGMENT_DATA.CAMPAIGN_PERFORMANCE_REPORT_WEEKLY_STATS
                    WHERE ADVERTISER_ID = %(advertiser_id)s AND LOG_DATE BETWEEN %(start_date)s AND %(end_date)s
                    GROUP BY {group_cols} HAVING SUM(IMPRESSIONS) >= 1000
                ),
                baseline AS (
                    SELECT SUM(VISITORS)::FLOAT / NULLIF(SUM(PANEL_REACH), 0) * 100 as BASELINE_VR
                    FROM campaign_metrics
                )
                SELECT c.NAME, c.PARENT_NAME, c.ID, c.PARENT_ID, c.IMPRESSIONS, c.REACH, c.PANEL_REACH, c.VISITORS,
                    ROUND(c.VISITORS::FLOAT / NULLIF(c.PANEL_REACH, 0) * 100, 4) as VISIT_RATE,
                    ROUND(b.BASELINE_VR, 4) as BASELINE_VR,
                    CASE WHEN b.BASELINE_VR > 0 THEN ROUND(c.VISITORS::FLOAT / NULLIF(c.PANEL_REACH, 0) * 100 / b.BASELINE_VR * 100, 1) END as INDEX_VS_AVG,
                    CASE WHEN b.BASELINE_VR > 0 THEN ROUND((c.VISITORS::FLOAT / NULLIF(c.PANEL_REACH, 0) * 100 - b.BASELINE_VR) / b.BASELINE_VR * 100, 1) END as LIFT_PCT
                FROM campaign_metrics c CROSS JOIN baseline b
                WHERE c.PANEL_REACH >= 1000 ORDER BY c.IMPRESSIONS DESC LIMIT 100
            """
            cursor.execute(query, {'advertiser_id': int(advertiser_id),
                                   'start_date': start_date, 'end_date': end_date})
            visit_type = 'store'

        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()

        if not rows:
            cursor.close()
            conn.close()
            return jsonify({'success': True, 'data': [], 'baseline': None, 'visit_type': visit_type,
                'strategy': strategy,
                'message': 'No lift data available - requires minimum 1,000 panel reach per campaign'})

        baseline = None
        results = []
        for row in rows:
            d = dict(zip(columns, row))
            if baseline is None and d.get('BASELINE_VR'):
                baseline = float(d['BASELINE_VR'])
            for k, v in d.items():
                if hasattr(v, 'is_integer'):
                    d[k] = float(v) if v else 0
            results.append(d)

        cursor.close()
        conn.close()
        return jsonify({'success': True, 'data': results, 'baseline': baseline,
                        'visit_type': visit_type, 'strategy': strategy})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


# =============================================================================
# TRAFFIC SOURCES (row-level agencies only — requires web visitor log)
# =============================================================================
@app.route('/api/v6/traffic-sources', methods=['GET'])
def get_traffic_sources():
    advertiser_id = request.args.get('advertiser_id')
    agency_id = request.args.get('agency_id')

    if not advertiser_id:
        return jsonify({'success': False, 'error': 'advertiser_id parameter required'}), 400

    try:
        conn = get_snowflake_connection()
        strategy = get_impression_strategy(agency_id, conn) if agency_id else STRATEGY_PCM_4KEY

        if strategy != STRATEGY_ADM_PREFIX:
            conn.close()
            return jsonify({
                'success': True, 'data': [],
                'message': 'Traffic sources analysis requires row-level impression data (ADM_PREFIX strategy only)',
                'strategy': strategy
            })

        start_date, end_date = get_date_range()

        cache_key = f"traffic-sources:{advertiser_id}:{start_date}:{end_date}"
        cached = cache_get(cache_key)
        if cached is not None:
            return jsonify({'success': True, 'data': cached, 'cached': True, 'strategy': strategy})

        cursor = conn.cursor()

        query = """
            WITH
            visitor_uuids AS (
                SELECT WEB_IMPRESSION_ID AS UUID, MAID,
                       SITE_VISIT_IP AS IP, SITE_VISIT_TIMESTAMP::DATE AS visit_date
                FROM QUORUMDB.SEGMENT_DATA.WEB_VISITORS_TO_LOG
                WHERE QUORUM_ADVERTISER_ID = %(advertiser_id)s
                  AND IS_SITE_VISIT = 'TRUE'
                  AND SITE_VISIT_TIMESTAMP BETWEEN %(start_date)s AND %(end_date)s
                  AND MAID IS NOT NULL AND MAID != ''
                  AND SITE_VISIT_IP IS NOT NULL AND SITE_VISIT_IP != ''
                LIMIT 50000
            ),
            uuid_classified AS (
                SELECT vu.UUID, vu.MAID, vu.IP, vu.visit_date,
                    CASE
                        WHEN p.VALUE ILIKE '%%doubleclick%%' OR p.VALUE ILIKE '%%syndicatedsearch%%' OR p.VALUE ILIKE '%%gclid%%' OR p.VALUE ILIKE '%%googleadservices%%' THEN 'Google Ads'
                        WHEN p.VALUE ILIKE '%%google%%' THEN 'Google Organic'
                        WHEN p.VALUE ILIKE '%%facebook%%' OR p.VALUE ILIKE '%%fbapp%%' OR p.VALUE ILIKE '%%fb.com%%' OR p.VALUE ILIKE '%%fbclid%%' THEN 'Meta/Facebook'
                        WHEN p.VALUE ILIKE '%%youtube%%' THEN 'YouTube'
                        WHEN p.VALUE ILIKE '%%instagram%%' THEN 'Instagram'
                        WHEN p.VALUE ILIKE '%%taboola%%' THEN 'Taboola'
                        WHEN p.VALUE ILIKE '%%outbrain%%' THEN 'Outbrain'
                        WHEN p.VALUE ILIKE '%%tiktok%%' THEN 'TikTok'
                        WHEN p.VALUE ILIKE '%%bing%%' THEN 'Bing'
                        WHEN p.VALUE ILIKE '%%yahoo%%' THEN 'Yahoo'
                        WHEN p.VALUE ILIKE '%%t.co%%' OR p.VALUE ILIKE '%%twitter%%' THEN 'Twitter/X'
                        WHEN p.VALUE ILIKE '%%linkedin%%' THEN 'LinkedIn'
                        WHEN p.VALUE ILIKE '%%pinterest%%' THEN 'Pinterest'
                        WHEN p.VALUE ILIKE '%%snapchat%%' THEN 'Snapchat'
                        WHEN p.VALUE ILIKE '%%reddit%%' THEN 'Reddit'
                        WHEN p.VALUE ILIKE '%%_ef_transaction%%' THEN 'Affiliate'
                        WHEN p.VALUE IS NULL OR p.VALUE = '-' OR p.VALUE = '' THEN 'Direct'
                        WHEN p.VALUE ILIKE '%%localhost%%' OR p.VALUE ILIKE '%%127.0.0.1%%' THEN 'SKIP'
                        ELSE 'Other Referral'
                    END AS traffic_source
                FROM visitor_uuids vu
                LEFT JOIN QUORUMDB.SEGMENT_DATA.PARAMOUNT_WEB_IMPRESSION_DATA p
                    ON vu.UUID = p.UUID AND p.KEY = 'referrer'
            ),
            ip_day AS (
                SELECT IP, visit_date,
                       COUNT(DISTINCT UUID) AS pageviews,
                       MODE(traffic_source) AS dominant_source
                FROM uuid_classified
                WHERE traffic_source != 'SKIP'
                GROUP BY 1, 2
            ),
            ctv_ips AS (
                SELECT DISTINCT uc.IP
                FROM uuid_classified uc
                WHERE EXISTS (
                    SELECT 1 FROM QUORUMDB.BASE_TABLES.AD_IMPRESSION_LOG_V2 v
                    JOIN (
                        SELECT DISTINCT DSP_ADVERTISER_ID
                        FROM QUORUMDB.REF_DATA.PIXEL_CAMPAIGN_MAPPING_V2
                        WHERE QUORUM_ADVERTISER_ID = %(advertiser_id_int)s
                    ) pcm ON v.DSP_ADVERTISER_ID = pcm.DSP_ADVERTISER_ID
                    WHERE v.DEVICE_ID_RAW = uc.MAID
                      AND v.AUCTION_TIMESTAMP::DATE BETWEEN %(start_date)s AND %(end_date)s
                )
            ),
            classified AS (
                SELECT id.dominant_source AS traffic_source,
                       id.pageviews, id.IP,
                       CASE WHEN ci.IP IS NOT NULL THEN 1 ELSE 0 END AS is_ctv
                FROM ip_day id
                LEFT JOIN ctv_ips ci ON id.IP = ci.IP
            )
            SELECT traffic_source, 'click' AS SOURCE_TYPE,
                COUNT(*) AS VISITOR_DAYS,
                COUNT(DISTINCT IP) AS UNIQUE_VISITORS,
                ROUND(AVG(pageviews), 2) AS AVG_PAGEVIEWS,
                APPROX_PERCENTILE(pageviews, 0.50) AS P50_PAGES,
                APPROX_PERCENTILE(pageviews, 0.90) AS P90_PAGES,
                ROUND(SUM(CASE WHEN pageviews = 1 THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0), 1) AS BOUNCE_RATE,
                SUM(is_ctv) AS CTV_OVERLAP,
                ROUND(SUM(is_ctv)::FLOAT / NULLIF(COUNT(*), 0) * 100, 1) AS CTV_OVERLAP_PCT,
                ROUND(AVG(CASE WHEN is_ctv = 1 THEN pageviews END), 2) AS AVG_PAGES_CTV,
                ROUND(AVG(CASE WHEN is_ctv = 0 THEN pageviews END), 2) AS AVG_PAGES_NON_CTV
            FROM classified
            GROUP BY 1 HAVING COUNT(*) >= 5
            UNION ALL
            SELECT 'Paramount CTV' AS traffic_source, 'ctv' AS SOURCE_TYPE,
                COUNT(*) AS VISITOR_DAYS,
                COUNT(DISTINCT IP) AS UNIQUE_VISITORS,
                ROUND(AVG(pageviews), 2) AS AVG_PAGEVIEWS,
                APPROX_PERCENTILE(pageviews, 0.50) AS P50_PAGES,
                APPROX_PERCENTILE(pageviews, 0.90) AS P90_PAGES,
                ROUND(SUM(CASE WHEN pageviews = 1 THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0), 1) AS BOUNCE_RATE,
                0 AS CTV_OVERLAP, 0 AS CTV_OVERLAP_PCT,
                NULL AS AVG_PAGES_CTV, NULL AS AVG_PAGES_NON_CTV
            FROM classified WHERE is_ctv = 1
            ORDER BY VISITOR_DAYS DESC
        """

        cursor.execute(query, {
            'advertiser_id': str(advertiser_id),
            'advertiser_id_int': int(advertiser_id),
            'start_date': start_date,
            'end_date': end_date
        })

        columns = [desc[0] for desc in cursor.description]
        results = []
        for row in cursor.fetchall():
            d = dict(zip(columns, row))
            for k, v in d.items():
                if hasattr(v, 'is_integer'):
                    d[k] = int(v) if v == int(v) else float(v)
                elif v is None:
                    d[k] = None
            results.append(d)

        cursor.close()
        conn.close()
        cache_set(cache_key, results)

        return jsonify({'success': True, 'data': results, 'strategy': strategy})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


# =============================================================================
# AGENCY TIMESERIES
# =============================================================================
@app.route('/api/v6/agency-timeseries', methods=['GET'])
def get_agency_timeseries():
    try:
        start_date, end_date = get_date_range()
        conn = get_snowflake_connection()
        cursor = conn.cursor()
        config = get_agency_config(conn)

        # Pre-aggregated agencies
        cursor.execute("""
            SELECT LOG_DATE::DATE as DT, AGENCY_ID, SUM(IMPRESSIONS) as IMPRESSIONS
            FROM QUORUMDB.SEGMENT_DATA.CAMPAIGN_PERFORMANCE_REPORT_WEEKLY_STATS
            WHERE LOG_DATE BETWEEN %(start_date)s AND %(end_date)s
            GROUP BY LOG_DATE::DATE, AGENCY_ID HAVING SUM(IMPRESSIONS) > 0
        """, {'start_date': start_date, 'end_date': end_date})
        rows_b = cursor.fetchall()

        week_dates = sorted(set(str(r[0]) for r in rows_b))

        # Row-level agencies: use summary stats (daily) and bucket into weeks
        row_level_agencies = [aid for aid, c in config.items()
                              if c.get('impression_join_strategy') == STRATEGY_ADM_PREFIX]

        rows_p_daily = []
        if row_level_agencies:
            rl_ids = ','.join(str(int(a)) for a in row_level_agencies)
            cursor.execute(f"""
                SELECT v.AUCTION_TIMESTAMP::DATE as DT, v.AGENCY_ID, COUNT(*) as TOTAL_IMPRESSIONS
                FROM QUORUMDB.BASE_TABLES.AD_IMPRESSION_LOG_V2 v
                JOIN (
                    SELECT DISTINCT DSP_ADVERTISER_ID, AGENCY_ID
                    FROM QUORUMDB.REF_DATA.PIXEL_CAMPAIGN_MAPPING_V2
                    WHERE AGENCY_ID IN ({rl_ids})
                      AND QUORUM_ADVERTISER_ID IS NOT NULL AND QUORUM_ADVERTISER_ID != 0
                ) m ON v.DSP_ADVERTISER_ID = m.DSP_ADVERTISER_ID
                   AND v.AGENCY_ID = m.AGENCY_ID
                WHERE v.AGENCY_ID IN ({rl_ids})
                  AND v.AUCTION_TIMESTAMP::DATE BETWEEN %(start_date)s AND %(end_date)s
                GROUP BY v.AUCTION_TIMESTAMP::DATE, v.AGENCY_ID HAVING COUNT(*) > 0
            """, {'start_date': start_date, 'end_date': end_date})
            rows_p_daily = cursor.fetchall()

        cursor.close()
        conn.close()

        from datetime import datetime as dt_cls

        def find_week_bucket(dt_str, anchors):
            if not anchors:
                return dt_str
            dt_val = dt_cls.strptime(dt_str, '%Y-%m-%d').date() if isinstance(dt_str, str) else dt_str
            anchor_dates = sorted([dt_cls.strptime(a, '%Y-%m-%d').date() if isinstance(a, str) else a for a in anchors])
            for anchor in anchor_dates:
                if dt_val <= anchor:
                    return str(anchor)
            return str(anchor_dates[-1])

        rows_p = []
        if week_dates:
            para_weekly = {}
            for dt_val, aid, imps in rows_p_daily:
                bucket = find_week_bucket(str(dt_val), week_dates)
                key = (bucket, int(aid))
                para_weekly[key] = para_weekly.get(key, 0) + int(imps)
            for (bucket, aid), imps in para_weekly.items():
                rows_p.append((bucket, aid, imps))
        else:
            para_weekly = {}
            for dt_val, aid, imps in rows_p_daily:
                d = dt_val if isinstance(dt_val, date) else dt_cls.strptime(str(dt_val), '%Y-%m-%d').date()
                monday = d - timedelta(days=d.weekday())
                key = (str(monday), int(aid))
                para_weekly[key] = para_weekly.get(key, 0) + int(imps)
            for (bucket, aid), imps in para_weekly.items():
                rows_p.append((bucket, aid, imps))

        data = {}
        for dt_val, agency_id, imps in rows_b + rows_p:
            dt_str = str(dt_val)
            if dt_str not in data: data[dt_str] = {}
            aid = int(agency_id)
            data[dt_str][aid] = int(imps) + data[dt_str].get(aid, 0)

        agencies = {}
        for agency_map in data.values():
            for aid in agency_map:
                if aid not in agencies:
                    agencies[aid] = get_agency_name(aid)

        return jsonify({'success': True, 'data': data, 'agencies': agencies})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


# =============================================================================
# ADVERTISER TIMESERIES
# =============================================================================
@app.route('/api/v6/advertiser-timeseries', methods=['GET'])
def get_advertiser_timeseries():
    try:
        agency_id = request.args.get('agency_id')
        if not agency_id:
            return jsonify({'success': False, 'error': 'agency_id required'}), 400

        agency_id = int(agency_id)
        start_date, end_date = get_date_range()
        conn = get_snowflake_connection()
        cursor = conn.cursor()
        strategy = get_impression_strategy(agency_id, conn)

        if strategy == STRATEGY_ADM_PREFIX:
            cursor.execute("""
                WITH daily AS (
                    SELECT v.AUCTION_TIMESTAMP::DATE as DT,
                           m.QUORUM_ADVERTISER_ID as AID,
                           COALESCE(MAX(aa.COMP_NAME), 'Advertiser ' || m.QUORUM_ADVERTISER_ID) as ANAME,
                           COUNT(*) as IMPS
                    FROM QUORUMDB.BASE_TABLES.AD_IMPRESSION_LOG_V2 v
                    JOIN (
                        SELECT DSP_ADVERTISER_ID, AGENCY_ID,
                               MAX(QUORUM_ADVERTISER_ID) as QUORUM_ADVERTISER_ID
                        FROM QUORUMDB.REF_DATA.PIXEL_CAMPAIGN_MAPPING_V2
                        WHERE AGENCY_ID = %(agency_id)s
                          AND QUORUM_ADVERTISER_ID IS NOT NULL AND QUORUM_ADVERTISER_ID != 0
                        GROUP BY DSP_ADVERTISER_ID, AGENCY_ID
                    ) m ON v.DSP_ADVERTISER_ID = m.DSP_ADVERTISER_ID
                       AND v.AGENCY_ID = m.AGENCY_ID
                    LEFT JOIN QUORUMDB.SEGMENT_DATA.AGENCY_ADVERTISER aa
                        ON m.QUORUM_ADVERTISER_ID = aa.ID
                    WHERE v.AGENCY_ID = %(agency_id)s
                      AND v.AUCTION_TIMESTAMP::DATE BETWEEN %(start_date)s AND %(end_date)s
                    GROUP BY v.AUCTION_TIMESTAMP::DATE, m.QUORUM_ADVERTISER_ID HAVING COUNT(*) > 0
                ),
                ranked AS (
                    SELECT AID, SUM(IMPS) as TOTAL_IMPS
                    FROM daily GROUP BY AID
                    ORDER BY TOTAL_IMPS DESC
                    LIMIT 15
                )
                SELECT d.DT,
                       CASE WHEN r.AID IS NOT NULL THEN d.AID ELSE -1 END as ADVERTISER_ID,
                       CASE WHEN r.AID IS NOT NULL THEN d.ANAME ELSE 'Other' END as ADVERTISER_NAME,
                       SUM(d.IMPS) as IMPRESSIONS
                FROM daily d
                LEFT JOIN ranked r ON d.AID = r.AID
                GROUP BY d.DT,
                         CASE WHEN r.AID IS NOT NULL THEN d.AID ELSE -1 END,
                         CASE WHEN r.AID IS NOT NULL THEN d.ANAME ELSE 'Other' END
            """, {'agency_id': agency_id, 'start_date': start_date, 'end_date': end_date})
        else:
            cursor.execute("""
                SELECT LOG_DATE::DATE as DT, w.ADVERTISER_ID,
                       COALESCE(MAX(aa.COMP_NAME), 'Advertiser ' || w.ADVERTISER_ID) as ADVERTISER_NAME,
                       SUM(w.IMPRESSIONS) as IMPRESSIONS
                FROM QUORUMDB.SEGMENT_DATA.CAMPAIGN_PERFORMANCE_REPORT_WEEKLY_STATS w
                LEFT JOIN QUORUMDB.SEGMENT_DATA.AGENCY_ADVERTISER aa ON w.ADVERTISER_ID = aa.ID
                WHERE w.AGENCY_ID = %(agency_id)s AND w.LOG_DATE BETWEEN %(start_date)s AND %(end_date)s
                GROUP BY LOG_DATE::DATE, w.ADVERTISER_ID HAVING SUM(w.IMPRESSIONS) > 0
            """, {'agency_id': agency_id, 'start_date': start_date, 'end_date': end_date})

        rows = cursor.fetchall()
        cursor.close()
        conn.close()

        data = {}
        advertisers = {}
        for dt_val, adv_id, adv_name, imps in rows:
            dt_str = str(dt_val)
            adv_id = int(adv_id)
            if dt_str not in data: data[dt_str] = {}
            data[dt_str][adv_id] = int(imps) + data[dt_str].get(adv_id, 0)
            if adv_id not in advertisers:
                name = adv_name or f'Advertiser {adv_id}'
                if strategy == STRATEGY_ADM_PREFIX:
                    name = re.sub(r'^[0-9A-Za-z]+ - ', '', name)
                advertisers[adv_id] = name

        return jsonify({'success': True, 'data': data, 'advertisers': advertisers, 'strategy': strategy})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


# =============================================================================
# OPTIMIZE RECOMMENDATIONS
# =============================================================================
@app.route('/api/v6/optimize', methods=['GET'])
def get_optimize():
    advertiser_id = request.args.get('advertiser_id')
    agency_id = request.args.get('agency_id')

    if not advertiser_id:
        return jsonify({'success': False, 'error': 'advertiser_id parameter required'}), 400

    try:
        conn = get_snowflake_connection()
        cursor = conn.cursor()
        strategy = get_impression_strategy(agency_id, conn) if agency_id else STRATEGY_PCM_4KEY

        if strategy == STRATEGY_ADM_PREFIX:
            date_filter = "v.AUCTION_TIMESTAMP::DATE BETWEEN DATEADD(day, -35, CURRENT_DATE) AND DATEADD(day, -5, CURRENT_DATE)"
            adv_filter = "pcm.QUORUM_ADVERTISER_ID = %(adv_id)s"
            imps_expr = "COUNT(*)"
            web_expr = "0"
            store_expr = "0"
            web_vr = f"0"
            store_vr = f"0"

            q1 = f"""
                WITH adv_dsp AS (
                    SELECT DISTINCT DSP_ADVERTISER_ID
                    FROM QUORUMDB.REF_DATA.PIXEL_CAMPAIGN_MAPPING_V2
                    WHERE QUORUM_ADVERTISER_ID = %(adv_id)s
                )
                SELECT 'baseline' as DIM_TYPE, 'overall' as DIM_KEY, NULL as DIM_NAME,
                    {imps_expr} as IMPS, {web_expr} as WEB_VISITS, {store_expr} as STORE_VISITS,
                    {web_vr} as WEB_VR, {store_vr} as STORE_VR
                FROM QUORUMDB.BASE_TABLES.AD_IMPRESSION_LOG_V2 v
                WHERE v.DSP_ADVERTISER_ID IN (SELECT DSP_ADVERTISER_ID FROM adv_dsp) AND {date_filter}
                UNION ALL
                SELECT 'campaign', v.INSERTION_ORDER_ID::VARCHAR, MAX(v.INSERTION_ORDER_NAME), {imps_expr}, {web_expr}, {store_expr}, {web_vr}, {store_vr}
                FROM QUORUMDB.BASE_TABLES.AD_IMPRESSION_LOG_V2 v
                WHERE v.DSP_ADVERTISER_ID IN (SELECT DSP_ADVERTISER_ID FROM adv_dsp) AND {date_filter} GROUP BY v.INSERTION_ORDER_ID
                UNION ALL
                SELECT 'lineitem', v.LINE_ITEM_ID::VARCHAR, MAX(v.LINE_ITEM_NAME), {imps_expr}, {web_expr}, {store_expr}, {web_vr}, {store_vr}
                FROM QUORUMDB.BASE_TABLES.AD_IMPRESSION_LOG_V2 v
                WHERE v.DSP_ADVERTISER_ID IN (SELECT DSP_ADVERTISER_ID FROM adv_dsp) AND {date_filter} GROUP BY v.LINE_ITEM_ID
                UNION ALL
                SELECT 'creative', v.CREATIVE_NAME, NULL, {imps_expr}, {web_expr}, {store_expr}, {web_vr}, {store_vr}
                FROM QUORUMDB.BASE_TABLES.AD_IMPRESSION_LOG_V2 v
                WHERE v.DSP_ADVERTISER_ID IN (SELECT DSP_ADVERTISER_ID FROM adv_dsp) AND {date_filter} GROUP BY v.CREATIVE_NAME
                UNION ALL
                SELECT 'dow', DAYOFWEEK(v.AUCTION_TIMESTAMP)::VARCHAR, NULL, {imps_expr}, {web_expr}, {store_expr}, {web_vr}, {store_vr}
                FROM QUORUMDB.BASE_TABLES.AD_IMPRESSION_LOG_V2 v
                WHERE v.DSP_ADVERTISER_ID IN (SELECT DSP_ADVERTISER_ID FROM adv_dsp) AND {date_filter} GROUP BY DAYOFWEEK(v.AUCTION_TIMESTAMP)
                UNION ALL
                SELECT 'site', v.SITE_DOMAIN, NULL, {imps_expr}, {web_expr}, {store_expr}, {web_vr}, {store_vr}
                FROM QUORUMDB.BASE_TABLES.AD_IMPRESSION_LOG_V2 v
                WHERE v.DSP_ADVERTISER_ID IN (SELECT DSP_ADVERTISER_ID FROM adv_dsp) AND {date_filter} GROUP BY v.SITE_DOMAIN HAVING COUNT(*) >= 500
                ORDER BY 1, 4 DESC
            """
            cursor.execute(q1, {'adv_id': int(advertiser_id)})
        else:
            vr_expr = "ROUND(SUM(VISITORS)*100.0/NULLIF(SUM(IMPRESSIONS),0), 4)"
            q1 = """
                SELECT 'baseline' as DIM_TYPE, 'overall' as DIM_KEY, NULL as DIM_NAME,
                    SUM(IMPRESSIONS) as IMPS, 0 as WEB_VISITS, SUM(VISITORS) as STORE_VISITS,
                    0 as WEB_VR, {vr} as STORE_VR
                FROM QUORUMDB.SEGMENT_DATA.CAMPAIGN_PERFORMANCE_REPORT_WEEKLY_STATS
                WHERE AGENCY_ID = %(agency_id)s AND ADVERTISER_ID = %(adv_id)s
                  AND LOG_DATE BETWEEN DATEADD(day, -35, CURRENT_DATE) AND DATEADD(day, -5, CURRENT_DATE)
                UNION ALL
                SELECT 'campaign', IO_ID::VARCHAR, MAX(IO_NAME),
                    SUM(IMPRESSIONS), 0, SUM(VISITORS), 0, {vr}
                FROM QUORUMDB.SEGMENT_DATA.CAMPAIGN_PERFORMANCE_REPORT_WEEKLY_STATS
                WHERE AGENCY_ID = %(agency_id)s AND ADVERTISER_ID = %(adv_id)s
                  AND LOG_DATE BETWEEN DATEADD(day, -35, CURRENT_DATE) AND DATEADD(day, -5, CURRENT_DATE)
                GROUP BY IO_ID
                UNION ALL
                SELECT 'lineitem', LI_ID::VARCHAR, MAX(LI_NAME),
                    SUM(IMPRESSIONS), 0, SUM(VISITORS), 0, {vr}
                FROM QUORUMDB.SEGMENT_DATA.CAMPAIGN_PERFORMANCE_REPORT_WEEKLY_STATS
                WHERE AGENCY_ID = %(agency_id)s AND ADVERTISER_ID = %(adv_id)s
                  AND LOG_DATE BETWEEN DATEADD(day, -35, CURRENT_DATE) AND DATEADD(day, -5, CURRENT_DATE)
                GROUP BY LI_ID
                UNION ALL
                SELECT 'site', PUBLISHER, NULL,
                    SUM(IMPRESSIONS), 0, SUM(VISITORS), 0, {vr}
                FROM QUORUMDB.SEGMENT_DATA.CAMPAIGN_PERFORMANCE_REPORT_WEEKLY_STATS
                WHERE AGENCY_ID = %(agency_id)s AND ADVERTISER_ID = %(adv_id)s
                  AND LOG_DATE BETWEEN DATEADD(day, -35, CURRENT_DATE) AND DATEADD(day, -5, CURRENT_DATE)
                  AND PUBLISHER IS NOT NULL AND PUBLISHER != ''
                GROUP BY PUBLISHER HAVING SUM(IMPRESSIONS) >= 500
                UNION ALL
                SELECT 'dow', DAYOFWEEK(LOG_DATE)::VARCHAR, NULL,
                    SUM(IMPRESSIONS), 0, SUM(VISITORS), 0, {vr}
                FROM QUORUMDB.SEGMENT_DATA.CAMPAIGN_PERFORMANCE_REPORT_WEEKLY_STATS
                WHERE AGENCY_ID = %(agency_id)s AND ADVERTISER_ID = %(adv_id)s
                  AND LOG_DATE BETWEEN DATEADD(day, -35, CURRENT_DATE) AND DATEADD(day, -5, CURRENT_DATE)
                GROUP BY DAYOFWEEK(LOG_DATE)
                ORDER BY 1, 4 DESC
            """.format(vr=vr_expr)
            cursor.execute(q1, {'agency_id': int(agency_id), 'adv_id': int(advertiser_id)})

        columns = [desc[0] for desc in cursor.description]
        results = []
        for row in cursor.fetchall():
            d = dict(zip(columns, row))
            for k, v in d.items():
                if hasattr(v, 'is_integer'):
                    d[k] = float(v) if v else 0
            results.append(d)

        cursor.close()
        conn.close()
        return jsonify({'success': True, 'data': results, 'strategy': strategy})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


# =============================================================================
# OPTIMIZE GEO
# =============================================================================
@app.route('/api/v6/optimize-geo', methods=['GET'])
def get_optimize_geo():
    advertiser_id = request.args.get('advertiser_id')
    agency_id = request.args.get('agency_id')

    if not advertiser_id:
        return jsonify({'success': False, 'error': 'advertiser_id parameter required'}), 400

    try:
        conn = get_snowflake_connection()
        cursor = conn.cursor()
        strategy = get_impression_strategy(agency_id, conn) if agency_id else STRATEGY_PCM_4KEY

        if strategy == STRATEGY_ADM_PREFIX:
            date_filter = "i.AUCTION_TIMESTAMP::DATE BETWEEN DATEADD(day, -35, CURRENT_DATE) AND DATEADD(day, -5, CURRENT_DATE)"
            imps_expr = "COUNT(*)"
            web_expr = "0"
            store_expr = "0"
            web_vr = f"0"
            store_vr = f"0"

            q2 = f"""
                WITH adv_dsp AS (
                    SELECT DISTINCT DSP_ADVERTISER_ID
                    FROM QUORUMDB.REF_DATA.PIXEL_CAMPAIGN_MAPPING_V2
                    WHERE QUORUM_ADVERTISER_ID = %(adv_id)s
                )
                SELECT 'dma' as DIM_TYPE, z.DMA_CODE as DIM_KEY, MAX(z.DMA_NAME) as DIM_NAME,
                    {imps_expr} as IMPS, {web_expr} as WEB_VISITS, {store_expr} as STORE_VISITS,
                    {web_vr} as WEB_VR, {store_vr} as STORE_VR
                FROM QUORUMDB.BASE_TABLES.AD_IMPRESSION_LOG_V2 i
                JOIN QUORUMDB.SEGMENT_DATA.ZIP_DMA_MAPPING z ON i.USER_POSTAL_CODE = z.ZIP_CODE
                WHERE i.DSP_ADVERTISER_ID IN (SELECT DSP_ADVERTISER_ID FROM adv_dsp) AND {date_filter}
                GROUP BY z.DMA_CODE HAVING COUNT(*) >= 500
                UNION ALL
                SELECT 'zip', i.USER_POSTAL_CODE, MAX(z.DMA_NAME), {imps_expr}, {web_expr}, {store_expr}, {web_vr}, {store_vr}
                FROM QUORUMDB.BASE_TABLES.AD_IMPRESSION_LOG_V2 i
                JOIN QUORUMDB.SEGMENT_DATA.ZIP_DMA_MAPPING z ON i.USER_POSTAL_CODE = z.ZIP_CODE
                WHERE i.DSP_ADVERTISER_ID IN (SELECT DSP_ADVERTISER_ID FROM adv_dsp) AND {date_filter}
                GROUP BY i.USER_POSTAL_CODE HAVING COUNT(*) >= 50
                ORDER BY 1, 4 DESC
            """
            cursor.execute(q2, {'adv_id': int(advertiser_id)})
        else:
            vr_expr = "ROUND(SUM(VISITORS)*100.0/NULLIF(SUM(IMPRESSIONS),0), 4)"
            q2 = """
                SELECT 'dma' as DIM_TYPE, DMA as DIM_KEY, DMA as DIM_NAME,
                    SUM(IMPRESSIONS) as IMPS, 0 as WEB_VISITS, SUM(VISITORS) as STORE_VISITS,
                    0 as WEB_VR, {vr} as STORE_VR
                FROM QUORUMDB.SEGMENT_DATA.CAMPAIGN_PERFORMANCE_REPORT_WEEKLY_STATS
                WHERE AGENCY_ID = %(agency_id)s AND ADVERTISER_ID = %(adv_id)s
                  AND LOG_DATE BETWEEN DATEADD(day, -35, CURRENT_DATE) AND DATEADD(day, -5, CURRENT_DATE)
                  AND DMA IS NOT NULL AND DMA != ''
                GROUP BY DMA HAVING SUM(IMPRESSIONS) >= 500
                UNION ALL
                SELECT 'zip', ZIP, MAX(DMA),
                    SUM(IMPRESSIONS), 0, SUM(VISITORS), 0, {vr}
                FROM QUORUMDB.SEGMENT_DATA.CAMPAIGN_PERFORMANCE_REPORT_WEEKLY_STATS
                WHERE AGENCY_ID = %(agency_id)s AND ADVERTISER_ID = %(adv_id)s
                  AND LOG_DATE BETWEEN DATEADD(day, -35, CURRENT_DATE) AND DATEADD(day, -5, CURRENT_DATE)
                  AND ZIP IS NOT NULL AND ZIP != ''
                GROUP BY ZIP HAVING SUM(IMPRESSIONS) >= 50
                ORDER BY 1, 4 DESC
            """.format(vr=vr_expr)
            cursor.execute(q2, {'agency_id': int(agency_id), 'adv_id': int(advertiser_id)})

        columns = [desc[0] for desc in cursor.description]
        results = []
        for row in cursor.fetchall():
            d = dict(zip(columns, row))
            for k, v in d.items():
                if hasattr(v, 'is_integer'):
                    d[k] = float(v) if v else 0
            results.append(d)

        cursor.close()
        conn.close()
        return jsonify({'success': True, 'data': results, 'strategy': strategy})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


# =============================================================================
# OPS CONSOLE — PIPELINE HEALTH DASHBOARD (no routing fork — universal)
# =============================================================================
@app.route('/api/v6/pipeline-health', methods=['GET'])
def pipeline_health():
    """Ops console: table freshness, volume trends, scheduled tasks, anomalies."""
    try:
        conn = get_snowflake_connection()
        cursor = conn.cursor()

        # 1. TABLE METADATA
        table_meta = {}
        for schema in ['BASE_TABLES', 'DERIVED_TABLES']:
            try:
                cursor.execute(f"SHOW TABLES IN SCHEMA QUORUMDB.{schema}")
                for row in cursor.fetchall():
                    full_name = f"QUORUMDB.{schema}.{row[1]}"
                    table_meta[full_name] = {
                        'rows': int(row[7]) if row[7] else 0,
                        'bytes': int(row[8]) if row[8] else 0
                    }
            except:
                pass

        # 2. FRESHNESS
        table_health = []
        volume_trends = {}

        freshness_queries = [
            ('impressions', "SELECT MAX(AUCTION_TIMESTAMP)::DATE FROM QUORUMDB.BASE_TABLES.AD_IMPRESSION_LOG_V2 WHERE AUCTION_TIMESTAMP >= DATEADD('day', -3, CURRENT_DATE()) AND AUCTION_TIMESTAMP <= CURRENT_TIMESTAMP()"),
            ('store_visits', "SELECT MAX(STORE_VISIT_DATE) FROM QUORUMDB.BASE_TABLES.STORE_VISITS WHERE STORE_VISIT_DATE >= DATEADD('day', -60, CURRENT_DATE())"),
            ('web_pixel_raw', "SELECT MAX(SYS_TIMESTAMP)::DATE FROM QUORUMDB.BASE_TABLES.WEBPIXEL_IMPRESSION_LOG WHERE SYS_TIMESTAMP >= DATEADD('day', -3, CURRENT_DATE()) AND SYS_TIMESTAMP <= CURRENT_TIMESTAMP()"),
            ('web_pixel_events', "SELECT MAX(STAGING_SYS_TIMESTAMP)::DATE FROM QUORUMDB.DERIVED_TABLES.WEBPIXEL_EVENTS WHERE STAGING_SYS_TIMESTAMP >= DATEADD('day', -30, CURRENT_DATE())"),
        ]

        for pname, query in freshness_queries:
            try:
                cursor.execute(query)
                row = cursor.fetchone()
                last_data_str = str(row[0]) if row and row[0] else None
                ds = (date.today() - datetime.strptime(last_data_str, '%Y-%m-%d').date()).days if last_data_str else 999
                volume_trends[pname] = {'last_data': last_data_str, 'days_stale': ds}
            except:
                volume_trends[pname] = {'last_data': None, 'days_stale': 999}

        pipeline_config = [
            ('Ad Impressions (V2)', 'QUORUMDB.BASE_TABLES.AD_IMPRESSION_LOG_V2', 'impressions', 'Primary ad impression log from all DSPs'),
            ('Store Visits', 'QUORUMDB.BASE_TABLES.STORE_VISITS', 'store_visits', 'Foot traffic attribution data'),
            ('Web Pixel Raw', 'QUORUMDB.BASE_TABLES.WEBPIXEL_IMPRESSION_LOG', 'web_pixel_raw', 'Raw web pixel fire events'),
            ('Web Pixel Events', 'QUORUMDB.DERIVED_TABLES.WEBPIXEL_EVENTS', 'web_pixel_events', 'Transformed web pixel analytical layer'),
        ]
        for label, tbl, key, desc in pipeline_config:
            meta = table_meta.get(tbl, {})
            vt = volume_trends.get(key, {})
            ds = vt.get('days_stale', 999)
            status = 'green' if ds <= 1 else ('yellow' if ds <= 3 else 'red')
            table_health.append({
                'label': label, 'table': tbl, 'description': desc,
                'last_data': vt.get('last_data'), 'days_stale': ds,
                'total_rows': meta.get('rows', 0), 'total_bytes': meta.get('bytes', 0),
                'status': status
            })

        # 3. ALERTS
        alerts = []
        for t in table_health:
            if t['days_stale'] > 7:
                alerts.append({
                    'severity': 'critical', 'pipeline': t['label'],
                    'message': f"{t['label']} — no data in {t['days_stale']} days (last: {t['last_data'] or 'never'})"
                })
            elif t['days_stale'] > 3:
                alerts.append({
                    'severity': 'warning', 'pipeline': t['label'],
                    'message': f"{t['label']} — {t['days_stale']} days stale (last: {t['last_data'] or 'never'})"
                })

        # 4. SCHEDULED TASKS
        tasks = []
        try:
            cursor.execute("SHOW TASKS IN DATABASE QUORUMDB")
            for row in cursor.fetchall():
                task_name = row[1]
                schema = row[4]
                schedule = row[8]
                state = row[10]
                definition = row[11][:200] if row[11] else ''
                last_committed = str(row[15]) if row[15] else None
                last_suspended = str(row[16]) if row[16] else None
                suspend_reason = row[20] if len(row) > 20 else None

                task_status = 'green' if state == 'started' else 'red'
                tasks.append({
                    'name': task_name, 'schema': schema, 'schedule': schedule,
                    'state': state, 'status': task_status,
                    'definition_preview': definition,
                    'last_committed': last_committed, 'last_suspended': last_suspended,
                    'suspend_reason': suspend_reason
                })
                if state == 'suspended':
                    alerts.append({
                        'severity': 'critical', 'pipeline': task_name,
                        'message': f"Scheduled task {task_name} is SUSPENDED" + (f" — {suspend_reason}" if suspend_reason else "")
                    })
        except Exception as te:
            tasks.append({'name': 'TASKS_UNAVAILABLE', 'error': str(te)[:200], 'status': 'unknown'})

        # 5. TRANSFORM LOG
        transform_log = []
        try:
            cursor.execute("""
                SELECT BATCH_ID, STATUS, STARTED_AT, COMPLETED_AT,
                       EVENTS_INSERTED, ERROR_MESSAGE, HOUR_START, HOUR_END
                FROM QUORUMDB.DERIVED_TABLES.WEBPIXEL_TRANSFORM_LOG
                ORDER BY STARTED_AT DESC LIMIT 20
            """)
            for row in cursor.fetchall():
                transform_log.append({
                    'BATCH_ID': row[0], 'STATUS': row[1],
                    'STARTED_AT': str(row[2]) if row[2] else None,
                    'COMPLETED_AT': str(row[3]) if row[3] else None,
                    'EVENTS_INSERTED': int(row[4]) if row[4] else 0,
                    'ERROR_MESSAGE': row[5],
                    'HOUR_START': str(row[6]) if row[6] else None,
                    'HOUR_END': str(row[7]) if row[7] else None,
                })
        except:
            pass

        # 6. STORED PROCEDURES
        procedures = []
        try:
            cursor.execute("SHOW USER PROCEDURES IN DATABASE QUORUMDB")
            for row in cursor.fetchall():
                proc_name = row[1]
                schema = row[2]
                desc = row[9] if len(row) > 9 else ''
                if proc_name.startswith('SYSTEM$'):
                    continue
                procedures.append({
                    'name': proc_name, 'schema': schema,
                    'description': desc[:200] if desc else ''
                })
        except:
            pass

        cursor.close()
        conn.close()

        severity_order = {'critical': 0, 'warning': 1, 'info': 2}
        alerts.sort(key=lambda a: severity_order.get(a.get('severity', 'info'), 3))

        has_critical = any(a['severity'] == 'critical' for a in alerts)
        has_warning = any(a['severity'] == 'warning' for a in alerts)
        overall_status = 'red' if has_critical else ('yellow' if has_warning else 'green')

        pipeline_status = {}
        for t in table_health:
            pipeline_status[t['label']] = t['status']
        for task in tasks:
            if task.get('state') == 'suspended':
                pipeline_status[task['name']] = 'red'

        return jsonify({
            'success': True, 'overall_status': overall_status,
            'tables': table_health, 'volume_trends': volume_trends,
            'alerts': alerts, 'tasks': tasks,
            'transform_log': transform_log, 'procedures': procedures,
            'pipeline_status': pipeline_status,
            'summary': {
                'total_tables_monitored': len(table_health),
                'tables_healthy': sum(1 for t in table_health if t['status'] == 'green'),
                'tables_warning': sum(1 for t in table_health if t['status'] == 'yellow'),
                'tables_critical': sum(1 for t in table_health if t['status'] == 'red'),
                'total_alerts': len(alerts),
                'critical_alerts': sum(1 for a in alerts if a['severity'] == 'critical'),
                'tasks_running': sum(1 for t in tasks if t.get('state') == 'started'),
                'tasks_suspended': sum(1 for t in tasks if t.get('state') == 'suspended'),
            }
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


# =============================================================================
# TABLE ACCESS TRACKING
# =============================================================================
@app.route('/api/v6/table-access', methods=['GET'])
def get_table_access():
    """Table access tracking: who's querying what, anomaly detection."""
    try:
        conn = get_snowflake_connection()
        cursor = conn.cursor()

        tracked = [
            ('AD_IMPRESSION_LOG_V2', 'Ad Impressions'),
            ('STORE_VISITS', 'Store Visits'),
            ('WEBPIXEL_IMPRESSION_LOG', 'Web Pixel Raw'),
            ('WEBPIXEL_EVENTS', 'Web Pixel Events'),
            ('SEGMENT_DEVICES_CAPTURED', 'Segment Devices'),
            ('XANDR_IMPRESSION_LOG', 'Xandr Impressions'),
            ('CAMPAIGN_PERFORMANCE_REPORT_WEEKLY_STATS', 'Campaign Weekly Stats'),
            ('PARAMOUNT_IMPRESSIONS_REPORT_90_DAYS', 'Paramount 90-Day'),
            ('AGENCY_ADVERTISER', 'Agency Advertiser'),
            ('LIVERAMP_IMPRESSION_LOG', 'LiveRamp Impressions'),
        ]
        case_cols = ",\n".join(
            f"SUM(CASE WHEN UPPER(QUERY_TEXT) LIKE '%{t[0]}%' THEN 1 ELSE 0 END) AS \"{t[0]}\""
            for t in tracked
        )
        cursor.execute(f"""
            SELECT
                START_TIME::DATE as query_date,
                COUNT(DISTINCT USER_NAME) as daily_users,
                {case_cols}
            FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
            WHERE START_TIME >= DATEADD('day', -7, CURRENT_TIMESTAMP())
              AND DATABASE_NAME = 'QUORUMDB'
              AND QUERY_TYPE IN ('SELECT','INSERT','CREATE_TABLE_AS_SELECT','MERGE')
            GROUP BY query_date
            ORDER BY query_date
        """)
        columns = [desc[0] for desc in cursor.description]
        daily_rows = [dict(zip(columns, row)) for row in cursor.fetchall()]

        today_str = str(date.today())
        yesterday_str = str(date.today() - timedelta(days=1))

        table_access = []
        access_alerts = []

        for tbl_name, label in tracked:
            col = tbl_name
            daily_counts = [(r['QUERY_DATE'], int(r.get(col) or 0)) for r in daily_rows if int(r.get(col) or 0) > 0]
            total_7d = sum(c for _, c in daily_counts)
            if total_7d == 0:
                continue

            avg_daily = round(total_7d / max(len(daily_counts), 1))
            today_q = next((int(r.get(col) or 0) for r in daily_rows if str(r['QUERY_DATE']) == today_str), 0)
            yesterday_q = next((int(r.get(col) or 0) for r in daily_rows if str(r['QUERY_DATE']) == yesterday_str), 0)
            max_users = max((int(r.get('DAILY_USERS') or 0) for r in daily_rows if int(r.get(col) or 0) > 0), default=0)
            last_accessed = str(max(d for d, c in daily_counts)) if daily_counts else None

            if today_q == 0 and avg_daily > 10:
                anomaly = 'silent'
            elif avg_daily > 10 and today_q < avg_daily * 0.3:
                anomaly = 'drop'
            elif avg_daily > 10 and today_q > avg_daily * 3:
                anomaly = 'spike'
            else:
                anomaly = 'normal'

            entry = {
                'table_name': tbl_name, 'label': label,
                'total_queries_7d': total_7d, 'max_daily_users': max_users,
                'last_accessed': last_accessed, 'avg_daily_queries': avg_daily,
                'today_queries': today_q, 'yesterday_queries': yesterday_q,
                'anomaly': anomaly
            }
            table_access.append(entry)

            if anomaly == 'silent':
                access_alerts.append({'severity': 'warning', 'pipeline': label,
                    'message': f"{label} — zero queries today (avg {avg_daily}/day). Table may have gone dark."})
            elif anomaly == 'drop':
                access_alerts.append({'severity': 'warning', 'pipeline': label,
                    'message': f"{label} — query volume dropped to {today_q} (avg {avg_daily}/day)"})

        table_access.sort(key=lambda x: x['total_queries_7d'], reverse=True)

        cursor.close()
        conn.close()
        return jsonify({'success': True, 'data': table_access, 'alerts': access_alerts})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)[:200],
            'grant_needed': 'GRANT IMPORTED PRIVILEGES ON DATABASE SNOWFLAKE TO ROLE OPTIMIZER_READONLY_ROLE;'}), 500


# =============================================================================
# MAIN
# =============================================================================
if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8080))
    app.run(host='0.0.0.0', port=port, debug=False)
