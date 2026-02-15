"""
Quorum Config API — Admin Screen Backend
==========================================
Endpoints for the 3-panel admin config screen:
  Panel 1: Unmapped Activity (impressions + web pixels)
  Panel 2: POI Location Assignment
  Panel 3: Campaign Mapping CRUD

Write targets:
  - PIXEL_CAMPAIGN_MAPPING_V2 (REF_DATA)
  - ADVERTISER_DOMAIN_MAPPING (DERIVED_TABLES)
  - SEGMENT_MD5_MAPPING (SEGMENT_DATA)
  - REF_ADVERTISER_CONFIG (BASE_TABLES)

Read sources:
  - V_UNMAPPED_AD_IMPRESSIONS (BASE_TABLES)
  - V_UNMAPPED_WEB_PIXELS (BASE_TABLES)
  - V_POI_BRAND_SEARCH (BASE_TABLES)
  - REF_ADVERTISER_CONFIG (BASE_TABLES)
  - REF_AGENCY_ADVERTISER (BASE_TABLES)

Designed to run alongside optimizer_api_v5 (same Flask app) or standalone.
"""
from flask import Blueprint, jsonify, request
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
    Returns web pixel domains firing in the last 7 days that are not yet mapped
    in ADVERTISER_DOMAIN_MAPPING. Uses direct query against WEBPIXEL_EVENTS
    instead of the slow V_UNMAPPED_WEB_PIXELS view.
    """
    try:
        agency_id = request.args.get('agency_id')

        conn = get_config_connection()
        cursor = conn.cursor()

        query = """
            WITH recent_pixels AS (
                SELECT
                    we.AGENCY_ID,
                    we.PAGE_URL_DOMAIN as DOMAIN,
                    COUNT(*) as EVENT_COUNT_7D,
                    COUNT(DISTINCT we.DEVICE_ID) as UNIQUE_DEVICES_7D,
                    MIN(we.EVENT_TIMESTAMP) as FIRST_SEEN,
                    MAX(we.EVENT_TIMESTAMP) as LAST_SEEN
                FROM QUORUMDB.DERIVED_TABLES.WEBPIXEL_EVENTS we
                WHERE we.EVENT_TIMESTAMP >= DATEADD(day, -7, CURRENT_TIMESTAMP())
                  AND we.PAGE_URL_DOMAIN IS NOT NULL
                  AND we.PAGE_URL_DOMAIN != ''
        """
        params = {}

        if agency_id:
            query += " AND we.AGENCY_ID = %(agency_id)s"
            params['agency_id'] = int(agency_id)

        query += """
                GROUP BY we.AGENCY_ID, we.PAGE_URL_DOMAIN
                HAVING COUNT(*) >= 10
            )
            SELECT
                rp.AGENCY_ID,
                COALESCE(MAX(aa.AGENCY_NAME), 'Agency ' || rp.AGENCY_ID) as AGENCY_NAME,
                rp.DOMAIN,
                rp.EVENT_COUNT_7D,
                rp.UNIQUE_DEVICES_7D,
                rp.FIRST_SEEN,
                rp.LAST_SEEN
            FROM recent_pixels rp
            LEFT JOIN QUORUMDB.SEGMENT_DATA.AGENCY_ADVERTISER aa
                ON rp.AGENCY_ID = aa.ADVERTISER_ID
            LEFT JOIN QUORUMDB.REF_DATA.ADVERTISER_DOMAIN_MAPPING adm
                ON rp.AGENCY_ID = adm.AGENCY_ID
                AND rp.DOMAIN LIKE '%' || adm.DOMAIN || '%'
            WHERE adm.MAPPING_ID IS NULL
            GROUP BY rp.AGENCY_ID, rp.DOMAIN, rp.EVENT_COUNT_7D, rp.UNIQUE_DEVICES_7D, rp.FIRST_SEEN, rp.LAST_SEEN
            ORDER BY rp.EVENT_COUNT_7D DESC
            LIMIT 100
        """

        cursor.execute(query, params)
        results = rows_to_dicts(cursor)

        cursor.close()
        conn.close()

        return jsonify({'success': True, 'count': len(results), 'data': results})

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
                COALESCE(aa.COMP_NAME, '') as QUORUM_ADVERTISER_NAME,
                pcm.IMPRESSIONS_14DAY_ROLLING, pcm.REACH_14DAY_ROLLING,
                pcm.IMPRESSION_COUNT, pcm.FIRST_IMPRESSION_TIMESTAMP, pcm.LAST_IMPRESSION_TIMESTAMP,
                pcm.CAMPAIGN_START_DATE, pcm.CAMPAIGN_END_DATE,
                pcm.CREATED_AT, pcm.MODIFIED_AT
            FROM QUORUMDB.REF_DATA.PIXEL_CAMPAIGN_MAPPING_V2 pcm
            LEFT JOIN QUORUMDB.BASE_TABLES.REF_DSP_PLATFORM dsp
                ON TRY_CAST(pcm.DSP_PLATFORM_TYPE AS INTEGER) = dsp.PLATFORM_TYPE_ID
            LEFT JOIN QUORUMDB.SEGMENT_DATA.AGENCY_ADVERTISER aa
                ON pcm.QUORUM_ADVERTISER_ID = aa.ID AND pcm.AGENCY_ID = aa.ADVERTISER_ID
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

        # Insert the mapping
        cursor.execute("""
            INSERT INTO QUORUMDB.REF_DATA.PIXEL_CAMPAIGN_MAPPING_V2 (
                DSP_ADVERTISER_ID, INSERTION_ORDER_ID, LINE_ITEM_ID,
                QUORUM_ADVERTISER_ID, AGENCY_ID, DSP_PLATFORM_TYPE,
                DATA_SOURCE, CAMPAIGN_NAME_MANUAL,
                ADVERTISER_NAME_FROM_DSP,
                CREATED_AT, MODIFIED_AT
            ) VALUES (
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
            )
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
        "is_poi": false
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

        # Update REF_ADVERTISER_CONFIG
        cursor.execute("""
            UPDATE QUORUMDB.BASE_TABLES.REF_ADVERTISER_CONFIG
            SET
                HAS_WEB_VISIT_ATTRIBUTION = TRUE,
                WEB_PIXEL_URL_COUNT = WEB_PIXEL_URL_COUNT + 1,
                UPDATED_AT = CURRENT_TIMESTAMP()
            WHERE ADVERTISER_ID = %(advertiser_id)s
              AND AGENCY_ID = %(agency_id)s
        """, {
            'advertiser_id': data['advertiser_id'],
            'agency_id': data['agency_id']
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
            FROM QUORUMDB.SEGMENT_DATA.SEGMENT_MD5_MAPPING
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
            INSERT INTO QUORUMDB.SEGMENT_DATA.SEGMENT_MD5_MAPPING
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
                c.CONFIG_STATUS,
                c.LAST_IMPRESSION_AT, c.LAST_STORE_VISIT_AT, c.LAST_WEB_VISIT_AT,
                c.CREATED_AT, c.UPDATED_AT,
                a.COMP_NAME as ADVERTISER_NAME,
                ag.AGENCY_NAME
            FROM QUORUMDB.BASE_TABLES.REF_ADVERTISER_CONFIG c
            LEFT JOIN QUORUMDB.SEGMENT_DATA.AGENCY_ADVERTISER a
                ON c.ADVERTISER_ID = a.ID AND c.AGENCY_ID = a.ADVERTISER_ID
            LEFT JOIN (
                SELECT DISTINCT ADVERTISER_ID as AG_ID, AGENCY_NAME
                FROM QUORUMDB.SEGMENT_DATA.AGENCY_ADVERTISER
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
                aa.ID as ADVERTISER_ID,
                aa.ADVERTISER_ID as AGENCY_ID,
                aa.COMP_NAME as ADVERTISER_NAME,
                aa.AGENCY_NAME,
                c.CONFIG_STATUS,
                c.SEGMENT_COUNT,
                c.WEB_PIXEL_URL_COUNT,
                c.CAMPAIGN_MAPPING_COUNT,
                c.HAS_STORE_VISIT_ATTRIBUTION,
                c.HAS_WEB_VISIT_ATTRIBUTION,
                c.HAS_IMPRESSION_TRACKING
            FROM QUORUMDB.SEGMENT_DATA.AGENCY_ADVERTISER aa
            LEFT JOIN QUORUMDB.BASE_TABLES.REF_ADVERTISER_CONFIG c
                ON aa.ID = c.ADVERTISER_ID AND aa.ADVERTISER_ID = c.AGENCY_ID
            WHERE aa.COMP_NAME ILIKE %(search)s
        """
        params = {'search': f'%{q}%'}

        if agency_id:
            query += " AND aa.ADVERTISER_ID = %(agency_id)s"
            params['agency_id'] = int(agency_id)

        if reporting_ready:
            query += " AND c.CONFIG_STATUS = 'ACTIVE' AND (COALESCE(c.SEGMENT_COUNT, 0) > 0 OR COALESCE(c.WEB_PIXEL_URL_COUNT, 0) > 0)"

        query += " ORDER BY aa.COMP_NAME LIMIT 25"

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
