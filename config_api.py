"""
Quorum Config API — Admin Screen Backend
==========================================
Endpoints for the 3-panel admin config screen:
  Panel 1: Unmapped Activity (impressions + web pixels)
  Panel 2: POI Location Assignment
  Panel 3: Campaign Mapping CRUD

Write targets:
  - REF_AGENCY_ADVERTISER (BASE_TABLES) — canonical advertiser registry
  - REF_ADVERTISER_CONFIG (BASE_TABLES) — advertiser config flags/counts
  - SEGMENT_MD5_MAPPING (BASE_TABLES) — POI segment assignments
  - PIXEL_CAMPAIGN_MAPPING_V2 (REF_DATA) — DSP campaign mappings
  - ADVERTISER_DOMAIN_MAPPING (DERIVED_TABLES) — web pixel URL mappings

Read sources:
  - REF_AGENCY_ADVERTISER (BASE_TABLES) — agency/advertiser lookup
  - REF_ADVERTISER_CONFIG (BASE_TABLES) — config status, report types
  - REF_REPORT_TYPE (BASE_TABLES) — report type taxonomy
  - SEGMENT_METADATA (BASE_TABLES) — POI brand/DMA/zip details
  - V_UNMAPPED_AD_IMPRESSIONS (BASE_TABLES) — unmapped impression discovery
  - V_POI_BRAND_SEARCH (BASE_TABLES) — POI brand search
  - AD_IMPRESSION_LOG_V2 (BASE_TABLES) — impression verification

Designed to run alongside optimizer_api_v6 (same Flask app) or standalone.
"""
import os
from flask import Blueprint, jsonify, request, g
from datetime import datetime

config_bp = Blueprint('config', __name__, url_prefix='/api/config')


# =============================================================================
# HELPERS
# =============================================================================
def get_config_connection():
    """
    Uses the same connection pattern as the optimizer.
    When integrated into the main app, replace with shared connection factory.
    For writes, needs a role with INSERT/UPDATE grants — not OPTIMIZER_READONLY_ROLE.
    """
    import snowflake.connector
    import os
    return snowflake.connector.connect(
        user=os.environ.get('SNOWFLAKE_USER'),
        password=os.environ.get('SNOWFLAKE_PASSWORD'),
        account=os.environ.get('SNOWFLAKE_ACCOUNT'),
        warehouse=os.environ.get('SNOWFLAKE_WAREHOUSE', 'COMPUTE_WH'),
        database='QUORUMDB',
        schema='SEGMENT_DATA',
        role=os.environ.get('SNOWFLAKE_ADMIN_ROLE',
             os.environ.get('SNOWFLAKE_ROLE', 'OPTIMIZER_READONLY_ROLE')),
        insecure_mode=True
    )


def rows_to_dicts(cursor):
    columns = [desc[0] for desc in cursor.description]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]


# =============================================================================
# PANEL 1a: UNMAPPED AD IMPRESSIONS
# =============================================================================
@config_bp.route('/unmapped-impressions', methods=['GET'])
def get_unmapped_impressions():
    """
    Returns DSP campaigns with impressions firing but no Quorum advertiser mapping.
    Replaces: manual SQL against impression logs + PIXEL_CAMPAIGN_MAPPING_V2
    Query params: agency_id (optional filter), min_impressions (default 100)
    """
    try:
        agency_id = request.args.get('agency_id')
        min_impressions = int(request.args.get('min_impressions', 100))

        conn = get_config_connection()
        cursor = conn.cursor()

        query = """
            SELECT
                AGENCY_ID, AGENCY_NAME, PLATFORM_TYPE_ID, PLATFORM_NAME,
                DSP_ADVERTISER_ID, DSP_ADVERTISER_NAME,
                IMPRESSIONS_30D, ACTIVE_DAYS,
                FIRST_SEEN, LAST_SEEN, ACTION_REQUIRED
            FROM QUORUMDB.BASE_TABLES.V_UNMAPPED_AD_IMPRESSIONS
            WHERE IMPRESSIONS_30D >= %(min_impressions)s
        """
        params = {'min_impressions': min_impressions}

        if agency_id:
            query += " AND AGENCY_ID = %(agency_id)s"
            params['agency_id'] = int(agency_id)

        query += " ORDER BY IMPRESSIONS_30D DESC"

        cursor.execute(query, params)
        results = rows_to_dicts(cursor)

        cursor.close()
        conn.close()

        return jsonify({
            'success': True,
            'count': len(results),
            'data': results
        })

    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


# =============================================================================
# PANEL 1b: UNMAPPED WEB PIXELS
# =============================================================================
@config_bp.route('/unmapped-webpixels', methods=['GET'])
def get_unmapped_webpixels():
    """
    Returns web pixel base domains firing in the last 7 days.
    Two-step approach: fast domain aggregation, then Python-side filtering
    against mapped domains to avoid expensive SQL NOT EXISTS.
    """
    try:
        agency_id = request.args.get('agency_id')
        if not agency_id:
            return jsonify({'success': False, 'error': 'agency_id is required'}), 400

        agency_id = int(agency_id)
        conn = get_config_connection()
        cursor = conn.cursor()

        # Step 1: Get already-mapped domains for this agency (fast, small result set)
        cursor.execute("""
            SELECT DISTINCT LOWER(
                SPLIT_PART(
                    SPLIT_PART(
                        REPLACE(REPLACE("URL", 'https://', ''), 'http://', ''),
                    '/', 1),
                '?', 1)
            ) as MAPPED_DOMAIN
            FROM QUORUMDB.DERIVED_TABLES.ADVERTISER_DOMAIN_MAPPING
            WHERE "Agency_Id" = %s AND "URL" IS NOT NULL
        """, (agency_id,))
        mapped_domains = set(row[0] for row in cursor.fetchall())

        # Step 2: Get active pixel domains for this agency with conversion metrics
        # Dropped COUNT(DISTINCT CLIENT_IP) for speed — it's the slowest aggregation
        cursor.execute("""
            SELECT
                LOWER(SPLIT_PART(SPLIT_PART(PAGE_URL, '://', 2), '/', 1)) as BASE_DOMAIN,
                COUNT(*) as EVENT_COUNT_7D,
                0 as UNIQUE_DEVICES_7D,
                COUNT_IF(EVENT_TYPE = 'purchase') as PURCHASES_7D,
                COUNT_IF(EVENT_TYPE = 'lead') as LEADS_7D,
                COUNT_IF(EVENT_TYPE = 'site_visit') as SITE_VISITS_7D,
                SUM(COALESCE(TRY_CAST(CONVERSION_VALUE AS FLOAT), 0)) as REVENUE_7D,
                MIN(EVENT_TIMESTAMP) as FIRST_SEEN,
                MAX(EVENT_TIMESTAMP) as LAST_SEEN
            FROM QUORUMDB.DERIVED_TABLES.WEBPIXEL_EVENTS
            WHERE EVENT_TIMESTAMP >= DATEADD(day, -7, CURRENT_TIMESTAMP())
              AND PAGE_URL IS NOT NULL
              AND AGENCY_ID = %s
            GROUP BY BASE_DOMAIN
            HAVING COUNT(*) >= 10
            ORDER BY EVENT_COUNT_7D DESC
            LIMIT 200
        """, (agency_id,))

        columns = [desc[0] for desc in cursor.description]
        all_domains = [dict(zip(columns, row)) for row in cursor.fetchall()]

        cursor.close()
        conn.close()

        # Step 3: Filter out mapped domains in Python (fast)
        # Extract root domains from mapped set for better matching
        # e.g. "www.scholastic.com" -> "scholastic.com" so it matches "shop.scholastic.com"
        def get_root_domain(d):
            """Strip www. prefix and get the registrable domain (last 2+ parts)."""
            d = d.lower().strip('.')
            if d.startswith('www.'):
                d = d[4:]
            return d

        mapped_roots = set()
        for md in mapped_domains:
            root = get_root_domain(md)
            mapped_roots.add(root)
            # Also add the raw domain for exact matching
            mapped_roots.add(md)

        platform_detect = {
            'myshopify.com': 'SHOPIFY', 'shopify': 'SHOPIFY', 'shopifypreview.com': 'SHOPIFY',
            'ticketmaster.com': 'TICKETMASTER', 'livenation.com': 'TICKETMASTER',
            'square.site': 'SQUARE', 'squareup.com': 'SQUARE', 'squarespace': 'SQUARESPACE',
            'wix.com': 'WIX', 'wixsite.com': 'WIX', 'wixstudio': 'WIX',
            'gtm-': 'GTM', 'googletagmanager': 'GTM',
            'woocommerce': 'WOOCOMMERCE',
            'bigcommerce': 'BIGCOMMERCE',
        }

        results = []
        for d in all_domains:
            domain = d.get('BASE_DOMAIN', '')
            if not domain:
                continue

            # Check if domain is already mapped using root domain matching
            # "shop.scholastic.com" matches if "scholastic.com" is in mapped_roots
            is_mapped = False
            domain_root = get_root_domain(domain)
            for mr in mapped_roots:
                # Check: exact match, root contains mapped, mapped contains root
                if domain_root == mr or mr.endswith('.' + domain_root) or domain_root.endswith('.' + mr):
                    is_mapped = True
                    break
                # Also check if they share the same registrable domain
                # e.g. "shop.scholastic.com" root="shop.scholastic.com"
                #       mapped root="scholastic.com"
                # domain_root ends with ".scholastic.com"? No, but "scholastic.com" is in domain_root
                if mr in domain_root or domain_root in mr:
                    is_mapped = True
                    break
            if is_mapped:
                continue

            # Detect platform
            detected = 'STANDARD'
            for key, platform in platform_detect.items():
                if key in domain:
                    detected = platform
                    break

            d['AGENCY_ID'] = agency_id
            d['DETECTED_PLATFORM'] = detected
            d['IS_MAPPED'] = False
            results.append(d)

            if len(results) >= 100:
                break

        return jsonify({'success': True, 'count': len(results), 'data': results})

    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


# =============================================================================
# WEB PIXEL PREVIEW (for configure modal)
# =============================================================================
@config_bp.route('/pixel-preview', methods=['GET'])
def get_pixel_preview():
    """
    Returns sample events and event type breakdown for a domain,
    plus detected tag type. Used in the configure modal.
    """
    try:
        agency_id = request.args.get('agency_id')
        domain = request.args.get('domain', '').lower().strip()

        if not agency_id or not domain:
            return jsonify({'success': False, 'error': 'agency_id and domain required'}), 400

        agency_id = int(agency_id)
        conn = get_config_connection()
        cursor = conn.cursor()

        # Event type breakdown
        cursor.execute("""
            SELECT
                COALESCE(EVENT_TYPE, 'unknown') as EVENT_TYPE,
                COUNT(*) as CNT,
                COUNT(DISTINCT CLIENT_IP) as UNIQUE_IPS
            FROM QUORUMDB.DERIVED_TABLES.WEBPIXEL_EVENTS
            WHERE AGENCY_ID = %s
              AND EVENT_TIMESTAMP >= DATEADD(day, -7, CURRENT_TIMESTAMP())
              AND LOWER(SPLIT_PART(SPLIT_PART(PAGE_URL, '://', 2), '/', 1)) = %s
            GROUP BY EVENT_TYPE
            ORDER BY CNT DESC
        """, (agency_id, domain))
        event_types = [{'event_type': r[0], 'count': r[1], 'unique_ips': r[2]}
                       for r in cursor.fetchall()]

        # Sample recent events (5 rows)
        cursor.execute("""
            SELECT
                EVENT_TIMESTAMP,
                PAGE_URL,
                EVENT_TYPE,
                UTM_SOURCE,
                UTM_MEDIUM,
                UTM_CAMPAIGN,
                TRAFFIC_SOURCE,
                CONVERSION_VALUE,
                ALL_PARAMETERS::STRING as PARAMS
            FROM QUORUMDB.DERIVED_TABLES.WEBPIXEL_EVENTS
            WHERE AGENCY_ID = %s
              AND EVENT_TIMESTAMP >= DATEADD(day, -3, CURRENT_TIMESTAMP())
              AND LOWER(SPLIT_PART(SPLIT_PART(PAGE_URL, '://', 2), '/', 1)) = %s
            ORDER BY EVENT_TIMESTAMP DESC
            LIMIT 5
        """, (agency_id, domain))

        columns = [desc[0] for desc in cursor.description]
        samples = [dict(zip(columns, row)) for row in cursor.fetchall()]

        # Detect tag type from domain name first, then fall back to params
        detected_tag = 'STANDARD'
        domain_detect = {
            'myshopify.com': 'SHOPIFY', 'shopify': 'SHOPIFY',
            'ticketmaster.com': 'TICKETMASTER', 'livenation.com': 'TICKETMASTER',
            'square.site': 'SQUARE', 'squareup.com': 'SQUARE',
            'squarespace.com': 'SQUARESPACE', 'squarespace': 'SQUARESPACE',
            'wixsite.com': 'WIX', 'wix.com': 'WIX', 'wixstudio': 'WIX',
            'gtm-': 'GTM', 'googletagmanager': 'GTM',
            'bigcommerce': 'BIGCOMMERCE',
        }
        for key, platform in domain_detect.items():
            if key in domain:
                detected_tag = platform
                break
        # If domain didn't match, check params
        if detected_tag == 'STANDARD':
            for s in samples:
                params = (s.get('PARAMS') or '').lower()
                page = (s.get('PAGE_URL') or '').lower()
                if 'shopify' in params or 'checkout_completed' in params:
                    detected_tag = 'SHOPIFY'; break
                elif 'demandware' in params or 'demandware' in page:
                    detected_tag = 'SFCC'; break
                elif 'woocommerce' in params or 'wc-' in params:
                    detected_tag = 'WOOCOMMERCE'; break
                elif 'squarespace' in params:
                    detected_tag = 'SQUARESPACE'; break
                elif 'ticketmaster' in params:
                    detected_tag = 'TICKETMASTER'; break
                elif 'wix' in params:
                    detected_tag = 'WIX'; break
                elif 'gtm' in params or 'google_tag' in params:
                    detected_tag = 'GTM'; break

        # Clean up sample params to keep payload small
        for s in samples:
            p = s.get('PARAMS', '')
            if p and len(p) > 200:
                s['PARAMS'] = p[:200] + '...'

        cursor.close()
        conn.close()

        return jsonify({
            'success': True,
            'domain': domain,
            'event_types': event_types,
            'samples': samples,
            'detected_tag': detected_tag,
            'total_events_7d': sum(e['count'] for e in event_types)
        })

    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


# =============================================================================
# PANEL 2: POI BRAND SEARCH
# =============================================================================
@config_bp.route('/poi-brands', methods=['GET'])
def get_poi_brands():
    """
    Search the brand library for store locations to assign to an advertiser.
    Replaces: manual SQL against REF_LOCATION_DEDUPLICATED + SEGMENT_METADATA
    Query params: search (brand name), dma (optional), limit (default 50)
    """
    try:
        search = request.args.get('search', '')
        dma = request.args.get('dma', '')
        limit = min(int(request.args.get('limit', 50)), 200)

        if not search and not dma:
            return jsonify({
                'success': False,
                'error': 'Provide search (brand name) or dma filter'
            }), 400

        conn = get_config_connection()
        cursor = conn.cursor()

        query = """
            SELECT *
            FROM QUORUMDB.BASE_TABLES.V_POI_BRAND_SEARCH
            WHERE 1=1
        """
        params = {}

        if search:
            query += " AND BRAND ILIKE %(search)s"
            params['search'] = f'%{search}%'

        if dma:
            query += " AND DMA_NAME ILIKE %(dma)s"
            params['dma'] = f'%{dma}%'

        query += f" LIMIT {limit}"

        cursor.execute(query, params)
        results = rows_to_dicts(cursor)

        cursor.close()
        conn.close()

        return jsonify({'success': True, 'count': len(results), 'data': results})

    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


# =============================================================================
# PANEL 3: CAMPAIGN MAPPING — LIST EXISTING
# =============================================================================
@config_bp.route('/campaign-mappings', methods=['GET'])
def get_campaign_mappings():
    """
    List current campaign mappings, optionally filtered by agency.
    Replaces: SELECT * FROM REF_DATA.PIXEL_CAMPAIGN_MAPPING_V2 WHERE ...
    """
    try:
        agency_id = request.args.get('agency_id')
        unmapped_only = request.args.get('unmapped_only', 'false').lower() == 'true'

        conn = get_config_connection()
        cursor = conn.cursor()

        query = """
            SELECT
                pcm.MAPPING_ID, pcm.DSP_ADVERTISER_ID, pcm.INSERTION_ORDER_ID,
                pcm.LINE_ITEM_ID, pcm.QUORUM_ADVERTISER_ID, pcm.AGENCY_ID,
                pcm.DSP_PLATFORM_TYPE,
                COALESCE(dsp.PLATFORM_NAME, pcm.DATA_SOURCE, 'Unknown') as PLATFORM_NAME,
                pcm.DATA_SOURCE,
                pcm.ADVERTISER_NAME_FROM_DSP, pcm.INSERTION_ORDER_NAME_FROM_DSP,
                pcm.LINE_ITEM_NAME_FROM_DSP,
                pcm.CAMPAIGN_NAME_MANUAL,
                COALESCE(aa.ADVERTISER_NAME, '') as QUORUM_ADVERTISER_NAME,
                pcm.IMPRESSIONS_14DAY_ROLLING, pcm.REACH_14DAY_ROLLING,
                pcm.IMPRESSION_COUNT, pcm.FIRST_IMPRESSION_TIMESTAMP, pcm.LAST_IMPRESSION_TIMESTAMP,
                pcm.CAMPAIGN_START_DATE, pcm.CAMPAIGN_END_DATE,
                pcm.CREATED_AT, pcm.MODIFIED_AT
            FROM QUORUMDB.REF_DATA.PIXEL_CAMPAIGN_MAPPING_V2 pcm
            LEFT JOIN QUORUMDB.BASE_TABLES.REF_DSP_PLATFORM dsp
                ON TRY_CAST(pcm.DSP_PLATFORM_TYPE AS INTEGER) = dsp.PLATFORM_TYPE_ID
            LEFT JOIN QUORUMDB.BASE_TABLES.REF_AGENCY_ADVERTISER aa
                ON pcm.QUORUM_ADVERTISER_ID = aa.ADVERTISER_ID AND pcm.AGENCY_ID = aa.AGENCY_ID
            WHERE 1=1
        """
        params = {}

        if agency_id:
            query += " AND pcm.AGENCY_ID = %(agency_id)s"
            params['agency_id'] = int(agency_id)

        if unmapped_only:
            query += " AND pcm.QUORUM_ADVERTISER_ID IS NULL"

        query += " ORDER BY pcm.LAST_IMPRESSION_TIMESTAMP DESC NULLS LAST LIMIT 500"

        cursor.execute(query, params)
        results = rows_to_dicts(cursor)

        cursor.close()
        conn.close()

        return jsonify({'success': True, 'count': len(results), 'data': results})

    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


# =============================================================================
# WRITE: MAP A DSP CAMPAIGN TO QUORUM ADVERTISER
# =============================================================================
@config_bp.route('/map-campaign', methods=['POST'])
def map_campaign():
    """
    Map a DSP campaign (PT + DSP_ADVERTISER_ID) to a Quorum advertiser.
    Replaces: manual INSERT INTO PIXEL_CAMPAIGN_MAPPING_V2

    POST body:
    {
        "dsp_advertiser_id": 1443673,
        "platform_type": 21,
        "quorum_advertiser_id": 12345,
        "agency_id": 1480,
        "insertion_order_id": null,      // optional
        "line_item_id": null,            // optional
        "campaign_name": "Lilikoi Agency", // optional override
        "data_source": "ADMIN_CONFIG"    // auto-set
    }
    """
    try:
        data = request.get_json()
        if not data:
            return jsonify({'success': False, 'error': 'JSON body required'}), 400

        required = ['dsp_advertiser_id', 'platform_type', 'quorum_advertiser_id', 'agency_id']
        missing = [f for f in required if f not in data]
        if missing:
            return jsonify({'success': False, 'error': f'Missing fields: {missing}'}), 400

        conn = get_config_connection()
        cursor = conn.cursor()

        # Insert the mapping (MAPPING_ID is NOT NULL, no auto-increment — generate via MAX+1)
        cursor.execute("""
            INSERT INTO QUORUMDB.REF_DATA.PIXEL_CAMPAIGN_MAPPING_V2 (
                MAPPING_ID,
                DSP_ADVERTISER_ID, INSERTION_ORDER_ID, LINE_ITEM_ID,
                QUORUM_ADVERTISER_ID, AGENCY_ID, DSP_PLATFORM_TYPE,
                DATA_SOURCE, CAMPAIGN_NAME_MANUAL,
                ADVERTISER_NAME_FROM_DSP,
                CREATED_AT, MODIFIED_AT
            )
            SELECT
                COALESCE(MAX(MAPPING_ID), 0) + 1,
                %(dsp_advertiser_id)s,
                %(insertion_order_id)s,
                %(line_item_id)s,
                %(quorum_advertiser_id)s,
                %(agency_id)s,
                %(platform_type)s,
                'ADMIN_CONFIG',
                %(campaign_name)s,
                %(dsp_advertiser_name)s,
                CURRENT_TIMESTAMP(),
                CURRENT_TIMESTAMP()
            FROM QUORUMDB.REF_DATA.PIXEL_CAMPAIGN_MAPPING_V2
        """, {
            'dsp_advertiser_id': data['dsp_advertiser_id'],
            'insertion_order_id': data.get('insertion_order_id'),
            'line_item_id': data.get('line_item_id'),
            'quorum_advertiser_id': data['quorum_advertiser_id'],
            'agency_id': data['agency_id'],
            'platform_type': str(data['platform_type']),
            'campaign_name': data.get('campaign_name'),
            'dsp_advertiser_name': data.get('dsp_advertiser_name'),
        })

        # Update REF_ADVERTISER_CONFIG campaign count
        cursor.execute("""
            UPDATE QUORUMDB.BASE_TABLES.REF_ADVERTISER_CONFIG
            SET
                CAMPAIGN_MAPPING_COUNT = CAMPAIGN_MAPPING_COUNT + 1,
                HAS_IMPRESSION_TRACKING = TRUE,
                UPDATED_AT = CURRENT_TIMESTAMP()
            WHERE ADVERTISER_ID = %(advertiser_id)s
              AND AGENCY_ID = %(agency_id)s
        """, {
            'advertiser_id': data['quorum_advertiser_id'],
            'agency_id': data['agency_id']
        })

        conn.commit()
        cursor.close()
        conn.close()

        return jsonify({
            'success': True,
            'message': f"Mapped DSP advertiser {data['dsp_advertiser_id']} "
                       f"(PT={data['platform_type']}) → Quorum advertiser {data['quorum_advertiser_id']}"
        })

    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


# =============================================================================
# READ: MAPPED URLS FOR AN ADVERTISER
# =============================================================================
@config_bp.route('/mapped-urls', methods=['GET'])
def get_mapped_urls():
    """Return the web pixel domains mapped to a specific advertiser."""
    try:
        agency_id = request.args.get('agency_id')
        advertiser_id = request.args.get('advertiser_id')
        if not agency_id or not advertiser_id:
            return jsonify({'success': False, 'error': 'agency_id and advertiser_id required'}), 400

        conn = get_config_connection()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT "URL", "IS_POI", "CREATED_AT"
            FROM QUORUMDB.DERIVED_TABLES.ADVERTISER_DOMAIN_MAPPING
            WHERE "Agency_Id" = %s AND "Advertiser_id" = %s
            ORDER BY "CREATED_AT" DESC
        """, (int(agency_id), int(advertiser_id)))
        urls = [{'url': r[0], 'is_poi': r[1], 'created_at': str(r[2]) if r[2] else None}
                for r in cursor.fetchall()]
        cursor.close()
        conn.close()
        return jsonify({'success': True, 'urls': urls})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


# =============================================================================
# WRITE: CONFIGURE A WEB PIXEL DOMAIN
# =============================================================================
@config_bp.route('/configure-domain', methods=['POST'])
def configure_domain():
    """
    Map a web pixel domain to a Quorum advertiser.
    Replaces: manual INSERT INTO ADVERTISER_DOMAIN_MAPPING

    POST body:
    {
        "agency_id": 2234,
        "advertiser_id": 12345,
        "url": "mountainwestbankchecking.com",
        "advertiser_name": "Mountain West Bank",
        "is_poi": false,
        "web_visit_source": "QUORUM_ADV_WEB_VISITS"  // optional, auto-detected from agency_id
    }
    """
    try:
        data = request.get_json()
        if not data:
            return jsonify({'success': False, 'error': 'JSON body required'}), 400

        required = ['agency_id', 'advertiser_id', 'url']
        missing = [f for f in required if f not in data]
        if missing:
            return jsonify({'success': False, 'error': f'Missing fields: {missing}'}), 400

        conn = get_config_connection()
        cursor = conn.cursor()

        # Check for existing mapping
        cursor.execute("""
            SELECT COUNT(*) FROM QUORUMDB.DERIVED_TABLES.ADVERTISER_DOMAIN_MAPPING
            WHERE "URL" = %(url)s AND "Advertiser_id" = %(advertiser_id)s
        """, {
            'url': data['url'],
            'advertiser_id': data['advertiser_id']
        })
        if cursor.fetchone()[0] > 0:
            cursor.close()
            conn.close()
            return jsonify({
                'success': False,
                'error': f"Domain {data['url']} already mapped to advertiser {data['advertiser_id']}"
            }), 409

        # Insert domain mapping
        cursor.execute("""
            INSERT INTO QUORUMDB.DERIVED_TABLES.ADVERTISER_DOMAIN_MAPPING (
                "Agency_Id", "Advertiser_id", "URL",
                "ADVERTISER_NAME", "IS_POI", "CREATED_AT"
            ) VALUES (
                %(agency_id)s, %(advertiser_id)s, %(url)s,
                %(advertiser_name)s, %(is_poi)s, CURRENT_TIMESTAMP()
            )
        """, {
            'agency_id': data['agency_id'],
            'advertiser_id': data['advertiser_id'],
            'url': data['url'],
            'advertiser_name': data.get('advertiser_name', ''),
            'is_poi': data.get('is_poi', False)
        })

        # Determine WEB_VISIT_SOURCE — all agencies use base table architecture
        web_visit_source = data.get('web_visit_source', 'QUORUM_ADV_WEB_VISITS')

        # Update REF_ADVERTISER_CONFIG
        cursor.execute("""
            UPDATE QUORUMDB.BASE_TABLES.REF_ADVERTISER_CONFIG
            SET
                HAS_WEB_VISIT_ATTRIBUTION = TRUE,
                WEB_PIXEL_URL_COUNT = WEB_PIXEL_URL_COUNT + 1,
                WEB_VISIT_SOURCE = %(web_visit_source)s,
                UPDATED_AT = CURRENT_TIMESTAMP()
            WHERE ADVERTISER_ID = %(advertiser_id)s
              AND AGENCY_ID = %(agency_id)s
        """, {
            'advertiser_id': data['advertiser_id'],
            'agency_id': data['agency_id'],
            'web_visit_source': web_visit_source
        })

        conn.commit()
        cursor.close()
        conn.close()

        return jsonify({
            'success': True,
            'message': f"Domain '{data['url']}' mapped to advertiser {data['advertiser_id']}. "
                       "Web visit attribution enabled."
        })

    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


# =============================================================================
# WRITE: ASSIGN POI SEGMENTS TO ADVERTISER
# =============================================================================
@config_bp.route('/assign-poi', methods=['POST'])
def assign_poi():
    """
    Assign POI (store location) segments from the brand library to an advertiser.
    Replaces: manual INSERT INTO SEGMENT_MD5_MAPPING + SEGMENT_METADATA updates

    POST body:
    {
        "advertiser_id": 5102,
        "agency_id": 1813,
        "segment_md5s": ["abc123...", "def456..."],   // from V_POI_BRAND_SEARCH
        "brand_name": "Starbucks",                    // for audit trail
        "dma": "New York"                             // for audit trail
    }
    """
    try:
        data = request.get_json()
        if not data:
            return jsonify({'success': False, 'error': 'JSON body required'}), 400

        required = ['advertiser_id', 'agency_id', 'segment_md5s']
        missing = [f for f in required if f not in data]
        if missing:
            return jsonify({'success': False, 'error': f'Missing fields: {missing}'}), 400

        md5s = data['segment_md5s']
        if not md5s or not isinstance(md5s, list):
            return jsonify({'success': False, 'error': 'segment_md5s must be a non-empty list'}), 400

        conn = get_config_connection()
        cursor = conn.cursor()

        # Find which MD5s are already assigned to this advertiser
        placeholders = ','.join([f"'{m}'" for m in md5s])
        cursor.execute(f"""
            SELECT SEGMENT_MD5
            FROM QUORUMDB.BASE_TABLES.SEGMENT_MD5_MAPPING
            WHERE ADVERTISER_ID = %(advertiser_id)s
              AND SEGMENT_MD5 IN ({placeholders})
        """, {'advertiser_id': data['advertiser_id']})

        existing = set(row[0] for row in cursor.fetchall())
        new_md5s = [m for m in md5s if m not in existing]

        if not new_md5s:
            cursor.close()
            conn.close()
            return jsonify({
                'success': True,
                'message': f"All {len(md5s)} segments already assigned to advertiser {data['advertiser_id']}",
                'inserted': 0,
                'skipped': len(md5s)
            })

        # Insert new segment assignments
        # Generate SEGMENT_UNIQUE_ID from advertiser_id + md5 for deterministic IDs
        insert_values = []
        for md5 in new_md5s:
            seg_uid = f"ADV_{data['advertiser_id']}_{md5[:12]}"
            insert_values.append(
                f"({data['advertiser_id']}, '{md5}', '{seg_uid}')"
            )

        cursor.execute(f"""
            INSERT INTO QUORUMDB.BASE_TABLES.SEGMENT_MD5_MAPPING
                (ADVERTISER_ID, SEGMENT_MD5, SEGMENT_UNIQUE_ID)
            VALUES {','.join(insert_values)}
        """)

        # Update REF_ADVERTISER_CONFIG
        cursor.execute("""
            UPDATE QUORUMDB.BASE_TABLES.REF_ADVERTISER_CONFIG
            SET
                HAS_STORE_VISIT_ATTRIBUTION = TRUE,
                SEGMENT_COUNT = SEGMENT_COUNT + %(new_count)s,
                UPDATED_AT = CURRENT_TIMESTAMP()
            WHERE ADVERTISER_ID = %(advertiser_id)s
              AND AGENCY_ID = %(agency_id)s
        """, {
            'new_count': len(new_md5s),
            'advertiser_id': data['advertiser_id'],
            'agency_id': data['agency_id']
        })

        conn.commit()
        cursor.close()
        conn.close()

        return jsonify({
            'success': True,
            'message': f"Assigned {len(new_md5s)} new POI segments to advertiser {data['advertiser_id']}. "
                       f"Store visit attribution enabled.",
            'inserted': len(new_md5s),
            'skipped': len(existing),
            'brand': data.get('brand_name', ''),
            'dma': data.get('dma', '')
        })

    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


# =============================================================================
# READ: ADVERTISER CONFIG STATUS
# =============================================================================
@config_bp.route('/advertiser-config', methods=['GET'])
def get_advertiser_config():
    """
    Get config status for an advertiser or list all active configs.
    Replaces: manual SELECT from REF_ADVERTISER_CONFIG

    Query params:
      - advertiser_id: specific advertiser
      - agency_id: filter by agency
      - active_only: true/false (default true)
    """
    try:
        advertiser_id = request.args.get('advertiser_id')
        agency_id = request.args.get('agency_id')
        active_only = request.args.get('active_only', 'true').lower() == 'true'

        conn = get_config_connection()
        cursor = conn.cursor()

        query = """
            SELECT
                c.ADVERTISER_ID, c.AGENCY_ID,
                c.HAS_STORE_VISIT_ATTRIBUTION, c.HAS_WEB_VISIT_ATTRIBUTION,
                c.HAS_IMPRESSION_TRACKING,
                c.SEGMENT_COUNT, c.POI_URL_COUNT, c.WEB_PIXEL_URL_COUNT,
                c.CAMPAIGN_MAPPING_COUNT, c.PLATFORM_TYPE_IDS, c.PLATFORM_COUNT,
                c.ATTRIBUTION_WINDOW_DAYS, c.MATCH_STRATEGY,
                c.IMPRESSION_JOIN_STRATEGY, c.EXPOSURE_SOURCE,
                c.WEB_PIXEL_INTEGRATION_TYPE,
                c.WEB_VISIT_SOURCE,
                c.CONFIG_STATUS,
                c.LAST_IMPRESSION_AT, c.LAST_STORE_VISIT_AT, c.LAST_WEB_VISIT_AT,
                c.CREATED_AT, c.UPDATED_AT,
                a.ADVERTISER_NAME,
                ag.AGENCY_NAME
            FROM QUORUMDB.BASE_TABLES.REF_ADVERTISER_CONFIG c
            LEFT JOIN QUORUMDB.BASE_TABLES.REF_AGENCY_ADVERTISER a
                ON c.ADVERTISER_ID = a.ADVERTISER_ID AND c.AGENCY_ID = a.AGENCY_ID
            LEFT JOIN (
                SELECT DISTINCT AGENCY_ID as AG_ID, AGENCY_NAME
                FROM QUORUMDB.BASE_TABLES.REF_AGENCY_ADVERTISER
                WHERE AGENCY_NAME IS NOT NULL
            ) ag ON c.AGENCY_ID = ag.AG_ID
            WHERE 1=1
        """
        params = {}

        if advertiser_id:
            query += " AND c.ADVERTISER_ID = %(advertiser_id)s"
            params['advertiser_id'] = int(advertiser_id)

        if agency_id:
            query += " AND c.AGENCY_ID = %(agency_id)s"
            params['agency_id'] = int(agency_id)

        if active_only:
            query += " AND c.CONFIG_STATUS = 'ACTIVE'"

        query += " ORDER BY c.LAST_IMPRESSION_AT DESC NULLS LAST LIMIT 500"

        cursor.execute(query, params)
        results = rows_to_dicts(cursor)

        cursor.close()
        conn.close()

        return jsonify({'success': True, 'count': len(results), 'data': results})

    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


# =============================================================================
# READ: ADVERTISER SEARCH (autocomplete for mapping workflows)
# =============================================================================
@config_bp.route('/search-advertisers', methods=['GET'])
def search_advertisers():
    """
    Search advertisers by name for the mapping autocomplete.
    Replaces: manual lookup in AGENCY_ADVERTISER

    Query params: q (search term), agency_id (optional filter)
    """
    try:
        q = request.args.get('q', '')
        agency_id = request.args.get('agency_id')
        reporting_ready = request.args.get('reporting_ready', 'false').lower() == 'true'

        if len(q) < 2:
            return jsonify({'success': False, 'error': 'Search term must be at least 2 chars'}), 400

        conn = get_config_connection()
        cursor = conn.cursor()

        query = """
            SELECT DISTINCT
                aa.ADVERTISER_ID,
                aa.AGENCY_ID,
                aa.ADVERTISER_NAME,
                aa.AGENCY_NAME,
                c.CONFIG_STATUS,
                c.SEGMENT_COUNT,
                c.WEB_PIXEL_URL_COUNT,
                c.CAMPAIGN_MAPPING_COUNT,
                c.HAS_STORE_VISIT_ATTRIBUTION,
                c.HAS_WEB_VISIT_ATTRIBUTION,
                c.HAS_IMPRESSION_TRACKING,
                c.WEB_VISIT_SOURCE
            FROM QUORUMDB.BASE_TABLES.REF_AGENCY_ADVERTISER aa
            LEFT JOIN QUORUMDB.BASE_TABLES.REF_ADVERTISER_CONFIG c
                ON aa.ADVERTISER_ID = c.ADVERTISER_ID AND aa.AGENCY_ID = c.AGENCY_ID
            WHERE aa.ADVERTISER_NAME ILIKE %(search)s
        """
        params = {'search': f'%{q}%'}

        if agency_id:
            query += " AND aa.AGENCY_ID = %(agency_id)s"
            params['agency_id'] = int(agency_id)

        if reporting_ready:
            query += " AND c.CONFIG_STATUS = 'ACTIVE' AND (COALESCE(c.SEGMENT_COUNT, 0) > 0 OR COALESCE(c.WEB_PIXEL_URL_COUNT, 0) > 0)"

        query += " ORDER BY aa.ADVERTISER_NAME LIMIT 25"

        cursor.execute(query, params)
        results = rows_to_dicts(cursor)

        cursor.close()
        conn.close()

        return jsonify({'success': True, 'count': len(results), 'data': results})

    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


# =============================================================================
# SCORECARD: CONFIGURATION COMPLETENESS
# =============================================================================
@config_bp.route('/scorecard', methods=['GET'])
def get_scorecard():
    """
    Configuration completeness metrics for the dashboard header.
    Shows: how many advertisers have each capability enabled vs total.
    """
    try:
        conn = get_config_connection()
        cursor = conn.cursor()

        cursor.execute("""
            SELECT
                COUNT(*) as TOTAL_ADVERTISERS,
                SUM(CASE WHEN HAS_STORE_VISIT_ATTRIBUTION AND SEGMENT_COUNT > 0 AND CAMPAIGN_MAPPING_COUNT > 0 THEN 1 ELSE 0 END) as STORE_VISIT_CONFIGURED,
                SUM(CASE WHEN HAS_WEB_VISIT_ATTRIBUTION AND WEB_PIXEL_URL_COUNT > 0 THEN 1 ELSE 0 END) as WEB_VISIT_CONFIGURED,
                SUM(CASE WHEN HAS_IMPRESSION_TRACKING AND CAMPAIGN_MAPPING_COUNT > 0 THEN 1 ELSE 0 END) as IMPRESSION_CONFIGURED,
                SUM(CASE WHEN CONFIG_STATUS = 'ACTIVE'
                     AND (CAMPAIGN_MAPPING_COUNT > 0 OR WEB_PIXEL_URL_COUNT > 0)
                     THEN 1 ELSE 0 END) as ACTIVE_CONFIGS,
                COUNT(DISTINCT CASE WHEN CONFIG_STATUS = 'ACTIVE'
                     AND (CAMPAIGN_MAPPING_COUNT > 0 OR WEB_PIXEL_URL_COUNT > 0)
                     THEN AGENCY_ID END) as AGENCY_COUNT,
                MAX(UPDATED_AT) as LAST_CONFIG_UPDATE,
                SUM(CASE WHEN SEGMENT_COUNT > 0 THEN 1 ELSE 0 END) as POI_ASSIGNED
            FROM QUORUMDB.BASE_TABLES.REF_ADVERTISER_CONFIG
            WHERE CONFIG_STATUS = 'ACTIVE'
        """)
        row = cursor.fetchone()
        columns = [desc[0] for desc in cursor.description]
        totals = dict(zip(columns, row))

        # Unmapped counts
        cursor.execute("""
            SELECT COUNT(*) FROM QUORUMDB.BASE_TABLES.V_UNMAPPED_AD_IMPRESSIONS
            WHERE IMPRESSIONS_30D >= 100
        """)
        totals['UNMAPPED_IMPRESSIONS'] = cursor.fetchone()[0]

        cursor.close()
        conn.close()

        return jsonify({'success': True, 'data': totals})

    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


# =============================================================================
# WRITE: UPDATE ADVERTISER CONFIG FLAGS
# =============================================================================
@config_bp.route('/update-config', methods=['POST'])
def update_config():
    """
    Update config flags for an advertiser (attribution window, match strategy, etc).
    Replaces: manual UPDATE on REF_ADVERTISER_CONFIG

    POST body:
    {
        "advertiser_id": 12345,
        "agency_id": 1813,
        "attribution_window_days": 14,
        "match_strategy": "HOUSEHOLD",
        "web_visit_source": "PARAMOUNT_SITEVISITS",  // or "QUORUM_ADV_WEB_VISITS"
        "config_status": "ACTIVE"
    }
    """
    try:
        data = request.get_json()
        if not data:
            return jsonify({'success': False, 'error': 'JSON body required'}), 400

        if 'advertiser_id' not in data or 'agency_id' not in data:
            return jsonify({'success': False, 'error': 'advertiser_id and agency_id required'}), 400

        # Whitelist of updatable fields
        allowed_fields = {
            'attribution_window_days': 'ATTRIBUTION_WINDOW_DAYS',
            'match_strategy': 'MATCH_STRATEGY',
            'impression_join_strategy': 'IMPRESSION_JOIN_STRATEGY',
            'config_status': 'CONFIG_STATUS',
            'has_store_visit_attribution': 'HAS_STORE_VISIT_ATTRIBUTION',
            'has_web_visit_attribution': 'HAS_WEB_VISIT_ATTRIBUTION',
            'has_impression_tracking': 'HAS_IMPRESSION_TRACKING',
            'web_pixel_integration_type': 'WEB_PIXEL_INTEGRATION_TYPE',
            'exposure_source': 'EXPOSURE_SOURCE',
            'web_visit_source': 'WEB_VISIT_SOURCE',
        }

        set_clauses = ["UPDATED_AT = CURRENT_TIMESTAMP()"]
        params = {
            'advertiser_id': data['advertiser_id'],
            'agency_id': data['agency_id']
        }

        for json_key, col_name in allowed_fields.items():
            if json_key in data:
                param_key = f'val_{json_key}'
                set_clauses.append(f"{col_name} = %({param_key})s")
                params[param_key] = data[json_key]

        if len(set_clauses) == 1:
            return jsonify({'success': False, 'error': 'No updatable fields provided'}), 400

        conn = get_config_connection()
        cursor = conn.cursor()

        cursor.execute(f"""
            UPDATE QUORUMDB.BASE_TABLES.REF_ADVERTISER_CONFIG
            SET {', '.join(set_clauses)}
            WHERE ADVERTISER_ID = %(advertiser_id)s
              AND AGENCY_ID = %(agency_id)s
        """, params)

        rows_updated = cursor.rowcount
        conn.commit()
        cursor.close()
        conn.close()

        if rows_updated == 0:
            return jsonify({
                'success': False,
                'error': f"No config found for advertiser {data['advertiser_id']} / agency {data['agency_id']}"
            }), 404

        return jsonify({
            'success': True,
            'message': f"Updated config for advertiser {data['advertiser_id']}",
            'fields_updated': [k for k in allowed_fields if k in data]
        })

    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


# =============================================================================
# ADVERTISER HUB: List advertisers with inline domains, POIs, campaigns
# =============================================================================
@config_bp.route('/advertiser-hub', methods=['GET'])
def get_advertiser_hub():
    """
    Returns advertiser list with inline domains, POI counts, campaign counts,
    and setup status indicators for the Advertiser Hub view.
    Supports agency filtering for non-admin users.
    Query params: agency_id (optional), search (optional name filter), limit
    """
    try:
        agency_id = request.args.get('agency_id')
        search = request.args.get('search', '').strip()
        limit = min(int(request.args.get('limit', 100)), 500)

        conn = get_config_connection()
        cursor = conn.cursor()

        query = """
            WITH adv_domains AS (
                SELECT
                    "Advertiser_id" as ADV_ID,
                    "Agency_Id" as AG_ID,
                    LISTAGG(DISTINCT "URL", ', ') WITHIN GROUP (ORDER BY "URL") as DOMAINS,
                    COUNT(DISTINCT "URL") as DOMAIN_COUNT
                FROM QUORUMDB.DERIVED_TABLES.ADVERTISER_DOMAIN_MAPPING
                WHERE "URL" IS NOT NULL
                GROUP BY "Advertiser_id", "Agency_Id"
            ),
            adv_pois AS (
                SELECT
                    ADVERTISER_ID as ADV_ID,
                    COUNT(DISTINCT SEGMENT_MD5) as POI_COUNT
                FROM QUORUMDB.BASE_TABLES.SEGMENT_MD5_MAPPING
                GROUP BY ADVERTISER_ID
            ),
            adv_campaigns AS (
                SELECT
                    QUORUM_ADVERTISER_ID as ADV_ID,
                    AGENCY_ID as AG_ID,
                    COUNT(*) as CAMPAIGN_COUNT,
                    SUM(COALESCE(IMPRESSIONS_14DAY_ROLLING, 0)) as TOTAL_IMPRESSIONS_14D,
                    MAX(LAST_IMPRESSION_TIMESTAMP) as LAST_IMPRESSION
                FROM QUORUMDB.REF_DATA.PIXEL_CAMPAIGN_MAPPING_V2
                WHERE QUORUM_ADVERTISER_ID IS NOT NULL
                GROUP BY QUORUM_ADVERTISER_ID, AGENCY_ID
            )
            SELECT
                aa.ADVERTISER_ID,
                aa.AGENCY_ID,
                aa.ADVERTISER_NAME,
                aa.AGENCY_NAME,
                COALESCE(c.CONFIG_STATUS, 'UNCONFIGURED') as CONFIG_STATUS,
                COALESCE(c.ATTRIBUTION_WINDOW_DAYS, 30) as ATTRIBUTION_WINDOW_DAYS,
                COALESCE(c.MATCH_STRATEGY, 'HOUSEHOLD') as MATCH_STRATEGY,
                COALESCE(c.IMPRESSION_JOIN_STRATEGY, 'PCM_4KEY') as IMPRESSION_JOIN_STRATEGY,
                c.HAS_STORE_VISIT_ATTRIBUTION,
                c.HAS_WEB_VISIT_ATTRIBUTION,
                c.HAS_IMPRESSION_TRACKING,
                c.WEB_VISIT_SOURCE,
                COALESCE(d.DOMAINS, '') as DOMAINS,
                COALESCE(d.DOMAIN_COUNT, 0) as DOMAIN_COUNT,
                COALESCE(p.POI_COUNT, 0) as POI_COUNT,
                COALESCE(cm.CAMPAIGN_COUNT, 0) as CAMPAIGN_COUNT,
                COALESCE(cm.TOTAL_IMPRESSIONS_14D, 0) as TOTAL_IMPRESSIONS_14D,
                cm.LAST_IMPRESSION,
                c.SEGMENT_COUNT,
                c.WEB_PIXEL_URL_COUNT,
                c.CAMPAIGN_MAPPING_COUNT,
                c.UPDATED_AT
            FROM QUORUMDB.BASE_TABLES.REF_AGENCY_ADVERTISER aa
            LEFT JOIN QUORUMDB.BASE_TABLES.REF_ADVERTISER_CONFIG c
                ON aa.ADVERTISER_ID = c.ADVERTISER_ID AND aa.AGENCY_ID = c.AGENCY_ID
            LEFT JOIN adv_domains d
                ON aa.ADVERTISER_ID = d.ADV_ID AND aa.AGENCY_ID = d.AG_ID
            LEFT JOIN adv_pois p
                ON aa.ADVERTISER_ID = p.ADV_ID
            LEFT JOIN adv_campaigns cm
                ON aa.ADVERTISER_ID = cm.ADV_ID AND aa.AGENCY_ID = cm.AG_ID
            WHERE 1=1
        """
        params = {}

        if agency_id:
            query += " AND aa.AGENCY_ID = %(agency_id)s"
            params['agency_id'] = int(agency_id)

        if search:
            query += " AND aa.ADVERTISER_NAME ILIKE %(search)s"
            params['search'] = f'%{search}%'

        # Only show advertisers with some activity or config
        query += """ AND (
            c.CONFIG_STATUS IS NOT NULL
            OR d.DOMAIN_COUNT > 0
            OR p.POI_COUNT > 0
            OR cm.CAMPAIGN_COUNT > 0
        )"""

        query += f" ORDER BY COALESCE(cm.TOTAL_IMPRESSIONS_14D, 0) DESC, aa.ADVERTISER_NAME LIMIT {limit}"

        cursor.execute(query, params)
        results = rows_to_dicts(cursor)

        # Add setup completeness score
        for r in results:
            score = 0
            steps_needed = []
            if r.get('CAMPAIGN_COUNT', 0) > 0 or r.get('CAMPAIGN_MAPPING_COUNT', 0) > 0:
                score += 1
            else:
                steps_needed.append('Map campaigns')
            if r.get('DOMAIN_COUNT', 0) > 0 or r.get('WEB_PIXEL_URL_COUNT', 0) > 0:
                score += 1
            else:
                steps_needed.append('Add web pixel URLs')
            if r.get('POI_COUNT', 0) > 0 or r.get('SEGMENT_COUNT', 0) > 0:
                score += 1
            else:
                steps_needed.append('Assign POI locations')
            r['SETUP_SCORE'] = score
            r['SETUP_MAX'] = 3
            r['STEPS_NEEDED'] = steps_needed

        cursor.close()
        conn.close()

        return jsonify({'success': True, 'count': len(results), 'data': results})

    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


# =============================================================================
# ADVERTISER DETAIL: Full detail for one advertiser
# =============================================================================
@config_bp.route('/advertiser-detail', methods=['GET'])
def get_advertiser_detail():
    """
    Returns full detail for a single advertiser: config, domains, campaigns, POIs.
    Query params: advertiser_id (required), agency_id (optional)
    """
    try:
        advertiser_id = request.args.get('advertiser_id')
        agency_id = request.args.get('agency_id')
        if not advertiser_id:
            return jsonify({'success': False, 'error': 'advertiser_id required'}), 400
        advertiser_id = int(advertiser_id)

        conn = get_config_connection()
        cursor = conn.cursor()

        # 1. Basic info + config
        query = """
            SELECT
                aa.ADVERTISER_ID, aa.AGENCY_ID,
                aa.ADVERTISER_NAME, aa.AGENCY_NAME,
                c.CONFIG_STATUS, c.ATTRIBUTION_WINDOW_DAYS, c.MATCH_STRATEGY,
                c.IMPRESSION_JOIN_STRATEGY, c.EXPOSURE_SOURCE,
                c.HAS_STORE_VISIT_ATTRIBUTION, c.HAS_WEB_VISIT_ATTRIBUTION,
                c.HAS_IMPRESSION_TRACKING, c.WEB_PIXEL_INTEGRATION_TYPE,
                c.WEB_VISIT_SOURCE,
                c.SEGMENT_COUNT, c.WEB_PIXEL_URL_COUNT, c.CAMPAIGN_MAPPING_COUNT,
                c.PLATFORM_TYPE_IDS, c.CREATED_AT, c.UPDATED_AT
            FROM QUORUMDB.BASE_TABLES.REF_AGENCY_ADVERTISER aa
            LEFT JOIN QUORUMDB.BASE_TABLES.REF_ADVERTISER_CONFIG c
                ON aa.ADVERTISER_ID = c.ADVERTISER_ID AND aa.AGENCY_ID = c.AGENCY_ID
            WHERE aa.ADVERTISER_ID = %s
        """
        q_params = [advertiser_id]
        if agency_id:
            query += " AND aa.AGENCY_ID = %s"
            q_params.append(int(agency_id))
        query += " LIMIT 1"
        cursor.execute(query, q_params)
        config_rows = rows_to_dicts(cursor)
        config = config_rows[0] if config_rows else None

        if not config:
            cursor.close()
            conn.close()
            return jsonify({'success': False, 'error': 'Advertiser not found'}), 404

        effective_agency = agency_id or config.get('AGENCY_ID')

        # 2. Domains
        cursor.execute("""
            SELECT "URL", "IS_POI", "CREATED_AT"
            FROM QUORUMDB.DERIVED_TABLES.ADVERTISER_DOMAIN_MAPPING
            WHERE "Advertiser_id" = %s AND "Agency_Id" = %s
            ORDER BY "CREATED_AT" DESC
        """, (advertiser_id, int(effective_agency)))
        domains = [{'url': r[0], 'is_poi': r[1], 'created_at': str(r[2]) if r[2] else None}
                   for r in cursor.fetchall()]

        # 3. Campaigns
        cursor.execute("""
            SELECT
                MAPPING_ID, DSP_ADVERTISER_ID, INSERTION_ORDER_ID, LINE_ITEM_ID,
                DSP_PLATFORM_TYPE, DATA_SOURCE,
                ADVERTISER_NAME_FROM_DSP, INSERTION_ORDER_NAME_FROM_DSP,
                LINE_ITEM_NAME_FROM_DSP, CAMPAIGN_NAME_MANUAL,
                IMPRESSIONS_14DAY_ROLLING, REACH_14DAY_ROLLING,
                IMPRESSION_COUNT, FIRST_IMPRESSION_TIMESTAMP, LAST_IMPRESSION_TIMESTAMP,
                CAMPAIGN_START_DATE, CAMPAIGN_END_DATE
            FROM QUORUMDB.REF_DATA.PIXEL_CAMPAIGN_MAPPING_V2
            WHERE QUORUM_ADVERTISER_ID = %s AND AGENCY_ID = %s
            ORDER BY LAST_IMPRESSION_TIMESTAMP DESC NULLS LAST
            LIMIT 100
        """, (advertiser_id, int(effective_agency)))
        campaigns = rows_to_dicts(cursor)

        # 4. POI summary — join through SEGMENT_METADATA (V_POI_BRAND_SEARCH is brand-aggregate, no SEGMENT_MD5)
        cursor.execute("""
            SELECT
                sm.SEGMENT_MD5,
                sm.SEGMENT_UNIQUE_ID,
                COALESCE(smd.BRAND, 'Unknown') as BRAND,
                COALESCE(smd.CATEGORY, '') as CATEGORY,
                COALESCE(smd.DMA_NAME, '') as DMA_NAME,
                1 as LOCATIONS,
                COALESCE(smd.ZIP_CODE, '') as ZIP_CODE
            FROM QUORUMDB.BASE_TABLES.SEGMENT_MD5_MAPPING sm
            LEFT JOIN QUORUMDB.BASE_TABLES.SEGMENT_METADATA smd
                ON sm.SEGMENT_UNIQUE_ID = smd.SEGMENT_UNIQUE_ID
            WHERE sm.ADVERTISER_ID = %s
            ORDER BY smd.BRAND, smd.DMA_NAME
            LIMIT 500
        """, (advertiser_id,))
        pois = rows_to_dicts(cursor)

        # POI summary stats
        poi_brands = set()
        poi_dmas = set()
        poi_zips = set()
        for p in pois:
            if p.get('BRAND') and p['BRAND'] != 'Unknown':
                poi_brands.add(p['BRAND'])
            if p.get('DMA_NAME'):
                poi_dmas.add(p['DMA_NAME'])
            if p.get('ZIP_CODE'):
                poi_zips.add(p['ZIP_CODE'])

        cursor.close()
        conn.close()

        return jsonify({
            'success': True,
            'config': config,
            'domains': domains,
            'campaigns': campaigns,
            'pois': pois,
            'poi_summary': {
                'total_segments': len(pois),
                'unique_brands': len(poi_brands),
                'brands': sorted(list(poi_brands)),
                'unique_dmas': len(poi_dmas),
                'unique_zips': len(poi_zips)
            }
        })

    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


# =============================================================================
# WRITE: REMOVE POI FROM ADVERTISER
# =============================================================================
@config_bp.route('/remove-poi', methods=['POST'])
def remove_poi():
    """
    Remove POI segment(s) from an advertiser.
    POST body: { "advertiser_id": 123, "agency_id": 1480, "segment_md5s": ["abc..."] }
    """
    try:
        data = request.get_json()
        if not data:
            return jsonify({'success': False, 'error': 'JSON body required'}), 400

        advertiser_id = data.get('advertiser_id')
        agency_id = data.get('agency_id')
        md5s = data.get('segment_md5s', [])
        if not advertiser_id or not md5s:
            return jsonify({'success': False, 'error': 'advertiser_id and segment_md5s required'}), 400

        conn = get_config_connection()
        cursor = conn.cursor()

        placeholders = ','.join([f"'{m}'" for m in md5s])
        cursor.execute(f"""
            DELETE FROM QUORUMDB.BASE_TABLES.SEGMENT_MD5_MAPPING
            WHERE ADVERTISER_ID = %s AND SEGMENT_MD5 IN ({placeholders})
        """, (int(advertiser_id),))
        deleted = cursor.rowcount

        # Update config counts
        if deleted > 0 and agency_id:
            cursor.execute("""
                UPDATE QUORUMDB.BASE_TABLES.REF_ADVERTISER_CONFIG
                SET SEGMENT_COUNT = GREATEST(SEGMENT_COUNT - %s, 0),
                    UPDATED_AT = CURRENT_TIMESTAMP()
                WHERE ADVERTISER_ID = %s AND AGENCY_ID = %s
            """, (deleted, int(advertiser_id), int(agency_id)))

            # Check if any POIs remain
            cursor.execute("""
                SELECT COUNT(*) FROM QUORUMDB.BASE_TABLES.SEGMENT_MD5_MAPPING
                WHERE ADVERTISER_ID = %s
            """, (int(advertiser_id),))
            remaining = cursor.fetchone()[0]
            if remaining == 0:
                cursor.execute("""
                    UPDATE QUORUMDB.BASE_TABLES.REF_ADVERTISER_CONFIG
                    SET HAS_STORE_VISIT_ATTRIBUTION = FALSE, UPDATED_AT = CURRENT_TIMESTAMP()
                    WHERE ADVERTISER_ID = %s AND AGENCY_ID = %s
                """, (int(advertiser_id), int(agency_id)))

        conn.commit()
        cursor.close()
        conn.close()

        return jsonify({'success': True, 'deleted': deleted})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


# =============================================================================
# WRITE: CREATE NEW ADVERTISER
# =============================================================================
@config_bp.route('/create-advertiser', methods=['POST'])
def create_advertiser():
    """
    Create a new advertiser in AGENCY_ADVERTISER and REF_ADVERTISER_CONFIG.
    POST body: { "advertiser_name": "Acme Corp", "agency_id": 1480 }
    """
    try:
        data = request.get_json()
        if not data:
            return jsonify({'success': False, 'error': 'JSON body required'}), 400

        name = data.get('advertiser_name', '').strip()
        agency_id = data.get('agency_id')
        if not name or not agency_id:
            return jsonify({'success': False, 'error': 'advertiser_name and agency_id required'}), 400

        agency_id = int(agency_id)
        conn = get_config_connection()
        cursor = conn.cursor()

        # Check for duplicates
        cursor.execute("""
            SELECT ADVERTISER_ID FROM QUORUMDB.BASE_TABLES.REF_AGENCY_ADVERTISER
            WHERE ADVERTISER_NAME ILIKE %s AND AGENCY_ID = %s
            LIMIT 1
        """, (name, agency_id))
        existing = cursor.fetchone()
        if existing:
            cursor.close()
            conn.close()
            return jsonify({
                'success': False,
                'error': f'Advertiser "{name}" already exists for this agency (ID: {existing[0]})'
            }), 409

        # Get next ID
        cursor.execute("SELECT COALESCE(MAX(ADVERTISER_ID), 0) + 1 FROM QUORUMDB.BASE_TABLES.REF_AGENCY_ADVERTISER")
        new_id = cursor.fetchone()[0]

        # Get agency name
        cursor.execute("""
            SELECT DISTINCT AGENCY_NAME FROM QUORUMDB.BASE_TABLES.REF_AGENCY_ADVERTISER
            WHERE AGENCY_ID = %s AND AGENCY_NAME IS NOT NULL LIMIT 1
        """, (agency_id,))
        agency_name_row = cursor.fetchone()
        agency_name = agency_name_row[0] if agency_name_row else ''

        # Insert advertiser
        cursor.execute("""
            INSERT INTO QUORUMDB.BASE_TABLES.REF_AGENCY_ADVERTISER
                (AGENCY_ID, ADVERTISER_ID, ADVERTISER_NAME, AGENCY_NAME, CREATED_AT, UPDATED_AT)
            VALUES (%s, %s, %s, %s, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP())
        """, (agency_id, new_id, name, agency_name))

        # All agencies use base table architecture
        default_web_visit_source = 'QUORUM_ADV_WEB_VISITS'

        # Create initial config
        cursor.execute("""
            INSERT INTO QUORUMDB.BASE_TABLES.REF_ADVERTISER_CONFIG (
                ADVERTISER_ID, AGENCY_ID, CONFIG_STATUS,
                ATTRIBUTION_WINDOW_DAYS, MATCH_STRATEGY, IMPRESSION_JOIN_STRATEGY,
                HAS_STORE_VISIT_ATTRIBUTION, HAS_WEB_VISIT_ATTRIBUTION, HAS_IMPRESSION_TRACKING,
                SEGMENT_COUNT, POI_URL_COUNT, WEB_PIXEL_URL_COUNT, CAMPAIGN_MAPPING_COUNT,
                WEB_VISIT_SOURCE,
                CREATED_AT, UPDATED_AT
            ) VALUES (
                %s, %s, 'ACTIVE', 30, 'HOUSEHOLD', 'PCM_4KEY',
                FALSE, FALSE, FALSE, 0, 0, 0, 0,
                %s,
                CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()
            )
        """, (new_id, agency_id, default_web_visit_source))

        conn.commit()
        cursor.close()
        conn.close()

        return jsonify({
            'success': True,
            'advertiser_id': new_id,
            'message': f'Created advertiser "{name}" (ID: {new_id}) for agency {agency_id}'
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


# =============================================================================
# IMPRESSION LOG SEARCH — discover new advertisers
# =============================================================================
@config_bp.route('/impression-search', methods=['GET'])
def impression_search():
    """
    Search ad impression logs by agency to discover new unmapped advertisers.
    Returns DSP advertisers grouped by name with impression counts.
    Query params: agency_id (required), days (default 30), min_impressions (default 50)
    """
    try:
        agency_id = request.args.get('agency_id')
        if not agency_id:
            return jsonify({'success': False, 'error': 'agency_id required'}), 400

        days = min(int(request.args.get('days', 30)), 90)
        min_impressions = int(request.args.get('min_impressions', 50))

        conn = get_config_connection()
        cursor = conn.cursor()

        cursor.execute("""
            SELECT
                imp.AGENCY_ID,
                imp.DSP_ADVERTISER_ID,
                imp.DSP_ADVERTISER_NAME,
                imp.PLATFORM_TYPE_ID,
                COALESCE(dsp.PLATFORM_NAME, 'Unknown') as PLATFORM_NAME,
                COUNT(*) as IMPRESSIONS,
                COUNT(DISTINCT CAST(imp.EVENT_DATE AS DATE)) as ACTIVE_DAYS,
                MIN(imp.EVENT_DATE) as FIRST_SEEN,
                MAX(imp.EVENT_DATE) as LAST_SEEN,
                CASE WHEN pcm.QUORUM_ADVERTISER_ID IS NOT NULL THEN 'MAPPED'
                     ELSE 'UNMAPPED' END as STATUS,
                pcm.QUORUM_ADVERTISER_ID,
                COALESCE(aa.ADVERTISER_NAME, '') as QUORUM_ADVERTISER_NAME
            FROM QUORUMDB.BASE_TABLES.AD_IMPRESSION_LOG_V2 imp
            LEFT JOIN QUORUMDB.REF_DATA.PIXEL_CAMPAIGN_MAPPING_V2 pcm
                ON imp.DSP_ADVERTISER_ID = pcm.DSP_ADVERTISER_ID
                AND imp.AGENCY_ID = pcm.AGENCY_ID
                AND imp.PLATFORM_TYPE_ID = TRY_CAST(pcm.DSP_PLATFORM_TYPE AS INTEGER)
            LEFT JOIN QUORUMDB.BASE_TABLES.REF_DSP_PLATFORM dsp
                ON imp.PLATFORM_TYPE_ID = dsp.PLATFORM_TYPE_ID
            LEFT JOIN QUORUMDB.BASE_TABLES.REF_AGENCY_ADVERTISER aa
                ON pcm.QUORUM_ADVERTISER_ID = aa.ADVERTISER_ID AND pcm.AGENCY_ID = aa.AGENCY_ID
            WHERE imp.AGENCY_ID = %(agency_id)s
              AND imp.EVENT_DATE >= DATEADD(day, -%(days)s, CURRENT_DATE())
            GROUP BY imp.AGENCY_ID, imp.DSP_ADVERTISER_ID, imp.DSP_ADVERTISER_NAME,
                     imp.PLATFORM_TYPE_ID, dsp.PLATFORM_NAME,
                     pcm.QUORUM_ADVERTISER_ID, aa.ADVERTISER_NAME
            HAVING COUNT(*) >= %(min_impressions)s
            ORDER BY STATUS DESC, IMPRESSIONS DESC
            LIMIT 200
        """, {'agency_id': int(agency_id), 'days': days, 'min_impressions': min_impressions})

        results = rows_to_dicts(cursor)
        cursor.close()
        conn.close()

        mapped = [r for r in results if r.get('STATUS') == 'MAPPED']
        unmapped = [r for r in results if r.get('STATUS') == 'UNMAPPED']

        return jsonify({
            'success': True,
            'count': len(results),
            'mapped_count': len(mapped),
            'unmapped_count': len(unmapped),
            'data': results
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


# =============================================================================
# STORE LIST UPLOAD + EMAIL TRIGGER
# =============================================================================
@config_bp.route('/upload-storelist', methods=['POST'])
def upload_storelist():
    """
    Upload a store list CSV for POI processing.
    Saves the file and sends notification email to cs@quorum.inc.
    POST: multipart form with 'file', 'advertiser_id', 'agency_id', 'advertiser_name'
    """
    try:
        import smtplib
        from email.mime.multipart import MIMEMultipart
        from email.mime.text import MIMEText
        from email.mime.base import MIMEBase
        from email import encoders
        import tempfile

        advertiser_id = request.form.get('advertiser_id')
        agency_id = request.form.get('agency_id')
        advertiser_name = request.form.get('advertiser_name', '')
        uploaded_by = ''
        if hasattr(g, 'user'):
            uploaded_by = g.user.get('email', g.user.get('user_id', 'unknown'))

        if 'file' not in request.files:
            return jsonify({'success': False, 'error': 'No file uploaded'}), 400

        file = request.files['file']
        if not file.filename:
            return jsonify({'success': False, 'error': 'Empty filename'}), 400

        # Save to temp
        temp_dir = tempfile.mkdtemp()
        filepath = os.path.join(temp_dir, file.filename)
        file.save(filepath)

        # Read file content for preview
        with open(filepath, 'r', errors='ignore') as f:
            preview_lines = f.readlines()[:10]
        preview = ''.join(preview_lines)
        total_lines = sum(1 for _ in open(filepath, 'r', errors='ignore'))

        # Try to send email notification
        email_sent = False
        email_error = None
        try:
            import os as _os
            smtp_host = _os.environ.get('SMTP_HOST', 'smtp.gmail.com')
            smtp_port = int(_os.environ.get('SMTP_PORT', 587))
            smtp_user = _os.environ.get('SMTP_USER', '')
            smtp_pass = _os.environ.get('SMTP_PASSWORD', '')

            if smtp_user and smtp_pass:
                msg = MIMEMultipart()
                msg['From'] = smtp_user
                msg['To'] = 'cs@quorum.inc'
                msg['Subject'] = f'Store List Upload — {advertiser_name} (Adv {advertiser_id}, Agency {agency_id})'

                body = f"""New store list uploaded for POI processing.

Advertiser: {advertiser_name} (ID: {advertiser_id})
Agency ID: {agency_id}
Uploaded by: {uploaded_by}
File: {file.filename}
Rows: ~{total_lines}

Preview (first 10 lines):
{preview}

Please process this store list via PolyPak and assign the resulting segments to advertiser {advertiser_id}.
"""
                msg.attach(MIMEText(body, 'plain'))

                # Attach file
                with open(filepath, 'rb') as attachment:
                    part = MIMEBase('application', 'octet-stream')
                    part.set_payload(attachment.read())
                    encoders.encode_base64(part)
                    part.add_header('Content-Disposition', f'attachment; filename={file.filename}')
                    msg.attach(part)

                with smtplib.SMTP(smtp_host, smtp_port) as server:
                    server.starttls()
                    server.login(smtp_user, smtp_pass)
                    server.send_message(msg)
                email_sent = True
        except Exception as mail_err:
            email_error = str(mail_err)

        # Clean up temp file
        import shutil
        shutil.rmtree(temp_dir, ignore_errors=True)

        return jsonify({
            'success': True,
            'message': f'Store list "{file.filename}" received ({total_lines} rows).',
            'email_sent': email_sent,
            'email_error': email_error,
            'rows': total_lines,
            'filename': file.filename
        })

    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


# =============================================================================
# REMOVE DOMAIN FROM ADVERTISER
# =============================================================================
@config_bp.route('/remove-domain', methods=['POST'])
def remove_domain():
    """
    Remove a web pixel domain from an advertiser.
    POST body: { "advertiser_id": 123, "agency_id": 1480, "url": "example.com" }
    """
    try:
        data = request.get_json()
        if not data:
            return jsonify({'success': False, 'error': 'JSON body required'}), 400

        advertiser_id = data.get('advertiser_id')
        agency_id = data.get('agency_id')
        url = data.get('url', '').strip()
        if not advertiser_id or not agency_id or not url:
            return jsonify({'success': False, 'error': 'advertiser_id, agency_id, and url required'}), 400

        conn = get_config_connection()
        cursor = conn.cursor()

        cursor.execute("""
            DELETE FROM QUORUMDB.DERIVED_TABLES.ADVERTISER_DOMAIN_MAPPING
            WHERE "Advertiser_id" = %s AND "Agency_Id" = %s AND "URL" = %s
        """, (int(advertiser_id), int(agency_id), url))
        deleted = cursor.rowcount

        if deleted > 0:
            cursor.execute("""
                UPDATE QUORUMDB.BASE_TABLES.REF_ADVERTISER_CONFIG
                SET WEB_PIXEL_URL_COUNT = GREATEST(WEB_PIXEL_URL_COUNT - %s, 0),
                    UPDATED_AT = CURRENT_TIMESTAMP()
                WHERE ADVERTISER_ID = %s AND AGENCY_ID = %s
            """, (deleted, int(advertiser_id), int(agency_id)))

        conn.commit()
        cursor.close()
        conn.close()

        return jsonify({'success': True, 'deleted': deleted})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


# =============================================================================
# AGENCIES LIST — for dropdowns
# =============================================================================
@config_bp.route('/agencies', methods=['GET'])
def get_agencies():
    """
    Return list of agencies that have at least one active advertiser —
    i.e. advertisers with linked POIs, web pixel URLs, campaign mappings,
    or defined report types. Mirrors the optimizer's agency scope.
    """
    try:
        conn = get_config_connection()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT
                c.AGENCY_ID,
                COALESCE(ag.AGENCY_NAME, '') as AGENCY_NAME,
                COUNT(DISTINCT c.ADVERTISER_ID) as ADVERTISER_COUNT,
                SUM(CASE WHEN c.SEGMENT_COUNT > 0 THEN 1 ELSE 0 END) as WITH_POI,
                SUM(CASE WHEN c.WEB_PIXEL_URL_COUNT > 0 THEN 1 ELSE 0 END) as WITH_WEB_PIXEL,
                SUM(CASE WHEN c.CAMPAIGN_MAPPING_COUNT > 0 THEN 1 ELSE 0 END) as WITH_CAMPAIGNS,
                SUM(CASE WHEN c.REPORT_TYPE_IDS IS NOT NULL AND c.REPORT_TYPE_IDS != '' THEN 1 ELSE 0 END) as WITH_REPORT_TYPE
            FROM QUORUMDB.BASE_TABLES.REF_ADVERTISER_CONFIG c
            LEFT JOIN (
                SELECT DISTINCT AGENCY_ID as AG_ID, AGENCY_NAME
                FROM QUORUMDB.BASE_TABLES.REF_AGENCY_ADVERTISER
                WHERE AGENCY_NAME IS NOT NULL AND AGENCY_NAME != ''
            ) ag ON c.AGENCY_ID = ag.AG_ID
            WHERE c.CONFIG_STATUS = 'ACTIVE'
              AND (
                  c.SEGMENT_COUNT > 0
                  OR c.WEB_PIXEL_URL_COUNT > 0
                  OR c.CAMPAIGN_MAPPING_COUNT > 0
                  OR (c.REPORT_TYPE_IDS IS NOT NULL AND c.REPORT_TYPE_IDS != '')
              )
            GROUP BY c.AGENCY_ID, ag.AGENCY_NAME
            ORDER BY AGENCY_NAME
            LIMIT 100
        """)
        results = rows_to_dicts(cursor)
        cursor.close()
        conn.close()
        return jsonify({'success': True, 'data': results})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


# =============================================================================
# INTEGRATION: Register with main Flask app
# =============================================================================
def register_config_api(app):
    """
    Call from main optimizer app:
        from config_api import register_config_api
        register_config_api(app)

    Adds all /api/config/* endpoints to the Flask app.
    """
    app.register_blueprint(config_bp)


# =============================================================================
# STANDALONE MODE (for testing)
# =============================================================================
if __name__ == '__main__':
    from flask import Flask
    from flask_cors import CORS

    app = Flask(__name__)
    CORS(app)
    register_config_api(app)

    @app.route('/health')
    def health():
        return jsonify({
            'status': 'healthy',
            'service': 'config-api',
            'endpoints': [rule.rule for rule in app.url_map.iter_rules()
                         if rule.rule.startswith('/api/config')]
        })

    app.run(host='0.0.0.0', port=5001, debug=True)
