"""
Quorum Optimizer API v2

Attribution Logic:
- Last-touch attribution: Only the most recent impression before a conversion gets credit
- S_VISITS: Physical store visits (from QUORUM_ADV_STORE_VISITS)
- W_VISITS: Website visits (from WEB_VISITORS_TO_LOG)
- W_LEADS: Lead form submissions
- W_PURCHASES: Purchase conversions
- W_AMOUNT: Total purchase dollar amount

Platform (PT) Configuration:
- Each platform stores publisher info in different columns
- Config determines which column to use and how to resolve names
- Hierarchy: Advertiser Override > Agency Override > PT Default
"""

from flask import Flask, jsonify, request
from flask_cors import CORS
import snowflake.connector
import os
from urllib.parse import unquote

app = Flask(__name__)
CORS(app)

SNOWFLAKE_CONFIG = {
    'account': os.environ.get('SNOWFLAKE_ACCOUNT', 'FZB05958.us-east-1'),
    'user': os.environ.get('SNOWFLAKE_USER', 'OPTIMIZER_SERVICE_USER'),
    'password': os.environ.get('SNOWFLAKE_PASSWORD', ''),
    'warehouse': os.environ.get('SNOWFLAKE_WAREHOUSE', 'COMPUTE_WH'),
    'database': 'QUORUMDB',
    'schema': 'SEGMENT_DATA'
}

# ============================================================================
# PLATFORM (PT) CONFIGURATION - Based on actual data population analysis
# ============================================================================
PT_CONFIG = {
    "6": {
        "name": "Trade Desk",
        "publisher_column": "SITE",  # 100% coverage
        "fallback_column": "PUBLISHER_ID",  # 71% coverage
        "name_lookup_table": "PUBLISHERS_ID_NAME_MAPPING",  # For PUBLISHER_CODE lookups
        "url_decode": False,
        "coverage_pct": 100.0
    },
    "8": {
        "name": "DV 360",
        "publisher_column": "PUBLISHER_ID",  # 99% coverage
        "fallback_column": None,
        "name_lookup_table": None,
        "url_decode": False,
        "coverage_pct": 99.0
    },
    "9": {
        "name": "DCM/GAM",
        "publisher_column": "PUBLISHER_ID",  # 99.9% coverage
        "fallback_column": None,
        "name_lookup_table": None,
        "url_decode": False,
        "coverage_pct": 99.9
    },
    "11": {
        "name": "Xandr",
        "publisher_column": "PUBLISHER_ID",  # 100% coverage
        "fallback_column": "PUBLISHER_CODE",  # 25% coverage
        "name_lookup_table": None,
        "url_decode": False,
        "coverage_pct": 100.0
    },
    "12": {
        "name": "Simpli.fi",
        "publisher_column": "PUBLISHER_ID",
        "fallback_column": None,
        "name_lookup_table": None,
        "url_decode": False,
        "coverage_pct": None
    },
    "13": {
        "name": "Adelphic",
        "publisher_column": "PUBLISHER_ID",  # 92% coverage
        "fallback_column": None,
        "name_lookup_table": None,
        "url_decode": False,
        "coverage_pct": 91.9
    },
    "14": {
        "name": "Beeswax",
        "publisher_column": "PUBLISHER_CODE",
        "fallback_column": None,
        "name_lookup_table": None,
        "url_decode": False,
        "coverage_pct": None
    },
    "15": {
        "name": "Xtreme Reach",
        "publisher_column": "PUBLISHER_CODE",
        "fallback_column": None,
        "name_lookup_table": None,
        "url_decode": False,
        "coverage_pct": None
    },
    "16": {
        "name": "StackAdapt",
        "publisher_column": "SITE",
        "fallback_column": None,
        "name_lookup_table": None,
        "url_decode": False,
        "coverage_pct": None
    },
    "17": {
        "name": "Unknown (17)",
        "publisher_column": "PUBLISHER_CODE",  # 0% coverage - no data
        "fallback_column": None,
        "name_lookup_table": None,
        "url_decode": False,
        "coverage_pct": 0.0
    },
    "20": {
        "name": "SpringServe",
        "publisher_column": "PUBLISHER_CODE",  # 97% coverage
        "fallback_column": "SITE",  # 22% coverage
        "name_lookup_table": None,
        "url_decode": False,
        "coverage_pct": 96.9
    },
    "21": {
        "name": "FreeWheel",
        "publisher_column": "SITE",  # 94.5% coverage
        "fallback_column": None,
        "name_lookup_table": None,
        "url_decode": False,
        "coverage_pct": 94.5
    },
    "22": {
        "name": "MNTN",
        "publisher_column": "PUBLISHER_CODE",  # 91.5% coverage
        "fallback_column": None,
        "name_lookup_table": None,
        "url_decode": True,  # Values are URL encoded (e.g., "Paramount%2520Streaming")
        "coverage_pct": 91.5
    },
    "23": {
        "name": "Yahoo",
        "publisher_column": "PUBLISHER_ID",  # 100% coverage
        "fallback_column": None,
        "name_lookup_table": None,
        "url_decode": False,
        "coverage_pct": 100.0
    },
    "25": {
        "name": "Amazon DSP",
        "publisher_column": "PUBLISHER_CODE",
        "fallback_column": None,
        "name_lookup_table": None,
        "url_decode": False,
        "coverage_pct": None
    },
    "27": {
        "name": "Unknown (27)",
        "publisher_column": "SITE",  # 100% coverage
        "fallback_column": None,
        "name_lookup_table": None,
        "url_decode": False,
        "coverage_pct": 100.0
    }
}

# Default config for unknown PTs
DEFAULT_PT_CONFIG = {
    "name": "Unknown",
    "publisher_column": "PUBLISHER_CODE",
    "fallback_column": "PUBLISHER_ID",
    "name_lookup_table": None,
    "url_decode": False,
    "coverage_pct": None
}

# ============================================================================
# AGENCY-LEVEL OVERRIDES
# For agencies that need different settings than PT defaults
# ============================================================================
AGENCY_CONFIG = {
    "1813": {  # Causal iQ - uses multiple PTs per advertiser
        "name": "Causal iQ",
        "pt_assignment": "BY_ADVERTISER",  # Flag: PT varies by advertiser, not agency-wide
        "notes": "Uses PT 6, 8, 9, 11, 23 depending on advertiser"
    },
    "2514": {  # MNTN
        "name": "MNTN",
        "pt_assignment": "BY_AGENCY",
        "default_pt": "22",
        "notes": "Primarily PT 22"
    },
    "1956": {  # Dealer Spike
        "name": "Dealer Spike", 
        "pt_assignment": "BY_AGENCY",
        "default_pt": "13",
        "notes": "Uses Adelphic (PT 13)"
    },
    "2234": {  # Magnite
        "name": "Magnite",
        "pt_assignment": "BY_ADVERTISER",
        "notes": "Uses PT 0, 11, 20, 23, 25, 33"
    }
}

# ============================================================================
# ADVERTISER-LEVEL OVERRIDES
# For specific advertisers that need different settings
# ============================================================================
ADVERTISER_CONFIG = {
    # Example structure - populate as needed
    # "advertiser_id": {
    #     "publisher_column": "SITE",  # Override the PT default
    #     "url_decode": True,
    #     "notes": "Special handling for this advertiser"
    # }
}


def get_snowflake_connection():
    return snowflake.connector.connect(**SNOWFLAKE_CONFIG)

def execute_query(query, params=None):
    conn = get_snowflake_connection()
    try:
        cursor = conn.cursor()
        if params:
            cursor.execute(query, params)
        else:
            cursor.execute(query)
        columns = [desc[0] for desc in cursor.description]
        results = []
        for row in cursor.fetchall():
            results.append(dict(zip(columns, row)))
        return results
    finally:
        conn.close()

def decode_publisher_name(name, url_decode=False):
    """Decode URL-encoded publisher names if needed."""
    if name and url_decode:
        try:
            # Double decode for values like "Paramount%2520Streaming"
            return unquote(unquote(str(name)))
        except:
            return name
    return name

def get_publisher_config(pt, agency_id=None, advertiser_id=None):
    """
    Get publisher column configuration with override hierarchy:
    1. Advertiser override (if exists)
    2. Agency override (if exists) 
    3. PT default
    4. Global default
    """
    config = DEFAULT_PT_CONFIG.copy()
    
    # Start with PT default
    pt_str = str(pt) if pt else None
    if pt_str and pt_str in PT_CONFIG:
        config.update(PT_CONFIG[pt_str])
    
    # Check agency override
    agency_str = str(agency_id) if agency_id else None
    if agency_str and agency_str in AGENCY_CONFIG:
        agency_cfg = AGENCY_CONFIG[agency_str]
        # Agency config can override specific fields
        for key in ['publisher_column', 'fallback_column', 'url_decode', 'name_lookup_table']:
            if key in agency_cfg:
                config[key] = agency_cfg[key]
        config['agency_override'] = True
        config['agency_name'] = agency_cfg.get('name')
    
    # Check advertiser override (highest priority)
    adv_str = str(advertiser_id) if advertiser_id else None
    if adv_str and adv_str in ADVERTISER_CONFIG:
        adv_cfg = ADVERTISER_CONFIG[adv_str]
        for key in ['publisher_column', 'fallback_column', 'url_decode', 'name_lookup_table']:
            if key in adv_cfg:
                config[key] = adv_cfg[key]
        config['advertiser_override'] = True
    
    return config


@app.route('/api/health', methods=['GET'])
def health_check():
    try:
        conn = get_snowflake_connection()
        conn.close()
        return jsonify({'status': 'healthy', 'snowflake': 'connected'})
    except Exception as e:
        return jsonify({'status': 'unhealthy', 'error': str(e)}), 500


# ============================================================================
# CONFIGURATION ENDPOINTS
# ============================================================================

@app.route('/api/config/platforms', methods=['GET'])
def get_platform_config():
    """Get all platform configurations."""
    return jsonify({
        'success': True, 
        'data': PT_CONFIG,
        'default': DEFAULT_PT_CONFIG
    })

@app.route('/api/config/agencies', methods=['GET'])
def get_agency_config():
    """Get all agency-level configurations."""
    return jsonify({
        'success': True,
        'data': AGENCY_CONFIG
    })

@app.route('/api/config/advertisers', methods=['GET'])
def get_advertiser_config():
    """Get all advertiser-level configurations."""
    return jsonify({
        'success': True,
        'data': ADVERTISER_CONFIG
    })

@app.route('/api/config/resolve', methods=['GET'])
def resolve_config():
    """Resolve the effective configuration for a given PT/Agency/Advertiser combination."""
    pt = request.args.get('pt')
    agency_id = request.args.get('agency_id')
    advertiser_id = request.args.get('advertiser_id')
    
    config = get_publisher_config(pt, agency_id, advertiser_id)
    
    return jsonify({
        'success': True,
        'input': {'pt': pt, 'agency_id': agency_id, 'advertiser_id': advertiser_id},
        'resolved_config': config
    })


# ============================================================================
# AGENCY / ADVERTISER ENDPOINTS
# ============================================================================

@app.route('/api/agencies', methods=['GET'])
def get_agencies():
    """Get list of all agencies with store visit data."""
    try:
        query = """
            SELECT 
                sv.AGENCY_ID,
                MAX(aa.AGENCY_NAME) as AGENCY_NAME,
                COUNT(DISTINCT sv.QUORUM_ADVERTISER_ID) as ADVERTISER_COUNT,
                COUNT(*) as TOTAL_IMPRESSIONS,
                SUM(CASE WHEN sv.IS_STORE_VISIT THEN 1 ELSE 0 END) as TOTAL_S_VISITS
            FROM QUORUMDB.SEGMENT_DATA.QUORUM_ADV_STORE_VISITS sv
            LEFT JOIN QUORUMDB.SEGMENT_DATA.AGENCY_ADVERTISER aa 
                ON sv.QUORUM_ADVERTISER_ID = aa.ID
            WHERE sv.AGENCY_ID IS NOT NULL
            GROUP BY sv.AGENCY_ID
            HAVING COUNT(*) > 1000
            ORDER BY TOTAL_IMPRESSIONS DESC
        """
        results = execute_query(query)
        
        # Add config info for each agency
        for row in results:
            agency_id = str(row.get('AGENCY_ID', ''))
            if agency_id in AGENCY_CONFIG:
                row['CONFIG'] = AGENCY_CONFIG[agency_id]
        
        return jsonify({'success': True, 'data': results})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/advertisers', methods=['GET'])
def get_advertisers():
    """Get advertisers for a specific agency."""
    agency_id = request.args.get('agency_id')
    if not agency_id:
        return jsonify({'success': False, 'error': 'agency_id parameter required'}), 400
    
    try:
        query = """
            SELECT 
                sv.QUORUM_ADVERTISER_ID as ADVERTISER_ID,
                MAX(aa.COMP_NAME) as ADVERTISER_NAME,
                MAX(sv.PT) as PT,
                COUNT(*) as TOTAL_IMPRESSIONS,
                SUM(CASE WHEN sv.IS_STORE_VISIT THEN 1 ELSE 0 END) as TOTAL_S_VISITS
            FROM QUORUMDB.SEGMENT_DATA.QUORUM_ADV_STORE_VISITS sv
            LEFT JOIN QUORUMDB.SEGMENT_DATA.AGENCY_ADVERTISER aa 
                ON sv.QUORUM_ADVERTISER_ID = aa.ID
            WHERE sv.AGENCY_ID = %s
            GROUP BY sv.QUORUM_ADVERTISER_ID
            HAVING COUNT(*) > 0
            ORDER BY TOTAL_IMPRESSIONS DESC
            LIMIT 50
        """
        results = execute_query(query, (agency_id,))
        
        # Add PT config info
        for row in results:
            pt = str(row.get('PT', ''))
            row['PT_NAME'] = PT_CONFIG.get(pt, DEFAULT_PT_CONFIG).get('name', 'Unknown')
        
        return jsonify({'success': True, 'data': results})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/advertiser-summary', methods=['GET'])
def get_advertiser_summary():
    """Get summary metrics for a specific advertiser (store visits)."""
    advertiser_id = request.args.get('advertiser_id')
    start_date = request.args.get('start_date', '2020-01-01')
    end_date = request.args.get('end_date', '2030-12-31')
    
    if not advertiser_id:
        return jsonify({'success': False, 'error': 'advertiser_id parameter required'}), 400
    
    try:
        query = """
            WITH last_touch AS (
                SELECT *,
                    ROW_NUMBER() OVER (
                        PARTITION BY DEVICE_ID_QU, DRIVEBYDATE 
                        ORDER BY IMP_TIMESTAMP DESC
                    ) as rn
                FROM QUORUMDB.SEGMENT_DATA.QUORUM_ADV_STORE_VISITS
                WHERE QUORUM_ADVERTISER_ID = %s
                  AND IS_STORE_VISIT = TRUE
                  AND IMP_TIMESTAMP >= %s
                  AND IMP_TIMESTAMP < %s
            )
            SELECT 
                %s as ADVERTISER_ID,
                MAX(aa.COMP_NAME) as ADVERTISER_NAME,
                MAX(lt.AGENCY_ID) as AGENCY_ID,
                MAX(lt.PT) as PT,
                (SELECT COUNT(*) FROM QUORUMDB.SEGMENT_DATA.QUORUM_ADV_STORE_VISITS 
                 WHERE QUORUM_ADVERTISER_ID = %s AND IMP_TIMESTAMP >= %s AND IMP_TIMESTAMP < %s) as TOTAL_IMPRESSIONS,
                COUNT(*) as S_VISITS,
                COUNT(DISTINCT IO_ID) as CAMPAIGN_COUNT,
                COUNT(DISTINCT LINEITEM_ID) as LINEITEM_COUNT
            FROM last_touch lt
            LEFT JOIN QUORUMDB.SEGMENT_DATA.AGENCY_ADVERTISER aa 
                ON lt.QUORUM_ADVERTISER_ID = aa.ID
            WHERE lt.rn = 1
        """
        results = execute_query(query, (advertiser_id, start_date, end_date, advertiser_id, advertiser_id, start_date, end_date))
        if results and results[0]:
            row = results[0]
            pt = str(row.get('PT', ''))
            agency_id = str(row.get('AGENCY_ID', ''))
            row['PT_NAME'] = PT_CONFIG.get(pt, DEFAULT_PT_CONFIG).get('name', 'Unknown')
            row['PUBLISHER_CONFIG'] = get_publisher_config(pt, agency_id, advertiser_id)
            return jsonify({'success': True, 'data': row})
        else:
            return jsonify({'success': False, 'error': 'Advertiser not found'}), 404
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


# ============================================================================
# ZIP PERFORMANCE (Last-Touch Attribution)
# ============================================================================

@app.route('/api/zip-performance', methods=['GET'])
def get_zip_performance():
    """Get ZIP-level performance with last-touch attribution."""
    advertiser_id = request.args.get('advertiser_id')
    start_date = request.args.get('start_date', '2020-01-01')
    end_date = request.args.get('end_date', '2030-12-31')
    min_impressions = request.args.get('min_impressions', '100')
    
    if not advertiser_id:
        return jsonify({'success': False, 'error': 'advertiser_id parameter required'}), 400
    
    try:
        query = """
            WITH last_touch_visits AS (
                SELECT sv.*,
                    ROW_NUMBER() OVER (
                        PARTITION BY sv.DEVICE_ID_QU, sv.DRIVEBYDATE 
                        ORDER BY sv.IMP_TIMESTAMP DESC
                    ) as rn
                FROM QUORUMDB.SEGMENT_DATA.QUORUM_ADV_STORE_VISITS sv
                WHERE sv.QUORUM_ADVERTISER_ID = %s
                  AND sv.IS_STORE_VISIT = TRUE
                  AND sv.IMP_TIMESTAMP >= %s
                  AND sv.IMP_TIMESTAMP < %s
            ),
            impressions_by_zip AS (
                SELECT 
                    mca.ZIP_CODE,
                    COUNT(*) as IMPRESSIONS
                FROM QUORUMDB.SEGMENT_DATA.QUORUM_ADV_STORE_VISITS sv
                JOIN QUORUM_CROSS_CLOUD.ATTAIN_FEED.MAID_CENTROID_ASSOCIATION mca 
                    ON LOWER(sv.IMP_MAID) = LOWER(mca.DEVICE_ID)
                WHERE sv.QUORUM_ADVERTISER_ID = %s
                  AND mca.ZIP_CODE IS NOT NULL AND mca.ZIP_CODE != ''
                  AND sv.IMP_TIMESTAMP >= %s
                  AND sv.IMP_TIMESTAMP < %s
                GROUP BY mca.ZIP_CODE
            ),
            visits_by_zip AS (
                SELECT 
                    mca.ZIP_CODE,
                    COUNT(*) as S_VISITS
                FROM last_touch_visits ltv
                JOIN QUORUM_CROSS_CLOUD.ATTAIN_FEED.MAID_CENTROID_ASSOCIATION mca 
                    ON LOWER(ltv.IMP_MAID) = LOWER(mca.DEVICE_ID)
                WHERE ltv.rn = 1
                  AND mca.ZIP_CODE IS NOT NULL AND mca.ZIP_CODE != ''
                GROUP BY mca.ZIP_CODE
            )
            SELECT 
                i.ZIP_CODE,
                zdm.DMA_CODE,
                zdm.DMA_NAME,
                zpd.POPULATION,
                i.IMPRESSIONS,
                COALESCE(v.S_VISITS, 0) as S_VISITS
            FROM impressions_by_zip i
            LEFT JOIN visits_by_zip v ON i.ZIP_CODE = v.ZIP_CODE
            LEFT JOIN QUORUMDB.SEGMENT_DATA.ZIP_DMA_MAPPING zdm ON i.ZIP_CODE = zdm.ZIP_CODE
            LEFT JOIN QUORUMDB.SEGMENT_DATA.ZIP_POPULATION_DATA zpd ON i.ZIP_CODE = zpd.ZIP_CODE
            WHERE i.IMPRESSIONS >= %s
            ORDER BY i.IMPRESSIONS DESC
            LIMIT 1000
        """
        results = execute_query(query, (advertiser_id, start_date, end_date, advertiser_id, start_date, end_date, min_impressions))
        return jsonify({'success': True, 'data': results})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


# ============================================================================
# CAMPAIGN PERFORMANCE (Last-Touch Attribution)
# ============================================================================

@app.route('/api/campaign-performance', methods=['GET'])
def get_campaign_performance():
    """Get campaign (IO) level performance with last-touch attribution."""
    advertiser_id = request.args.get('advertiser_id')
    start_date = request.args.get('start_date', '2020-01-01')
    end_date = request.args.get('end_date', '2030-12-31')
    
    if not advertiser_id:
        return jsonify({'success': False, 'error': 'advertiser_id parameter required'}), 400
    
    try:
        query = """
            WITH last_touch_visits AS (
                SELECT *,
                    ROW_NUMBER() OVER (
                        PARTITION BY DEVICE_ID_QU, DRIVEBYDATE 
                        ORDER BY IMP_TIMESTAMP DESC
                    ) as rn
                FROM QUORUMDB.SEGMENT_DATA.QUORUM_ADV_STORE_VISITS
                WHERE QUORUM_ADVERTISER_ID = %s
                  AND IS_STORE_VISIT = TRUE
                  AND IMP_TIMESTAMP >= %s
                  AND IMP_TIMESTAMP < %s
            ),
            impressions_by_campaign AS (
                SELECT 
                    IO_ID as CAMPAIGN_ID,
                    MAX(IO_NAME) as CAMPAIGN_NAME,
                    MAX(PT) as PT,
                    COUNT(*) as IMPRESSIONS
                FROM QUORUMDB.SEGMENT_DATA.QUORUM_ADV_STORE_VISITS
                WHERE QUORUM_ADVERTISER_ID = %s
                  AND IO_ID IS NOT NULL
                  AND IMP_TIMESTAMP >= %s
                  AND IMP_TIMESTAMP < %s
                GROUP BY IO_ID
            ),
            visits_by_campaign AS (
                SELECT 
                    IO_ID as CAMPAIGN_ID,
                    COUNT(*) as S_VISITS
                FROM last_touch_visits
                WHERE rn = 1
                GROUP BY IO_ID
            )
            SELECT 
                i.PT,
                i.CAMPAIGN_ID,
                i.CAMPAIGN_NAME,
                i.IMPRESSIONS,
                COALESCE(v.S_VISITS, 0) as S_VISITS
            FROM impressions_by_campaign i
            LEFT JOIN visits_by_campaign v ON i.CAMPAIGN_ID = v.CAMPAIGN_ID
            ORDER BY i.IMPRESSIONS DESC
            LIMIT 50
        """
        results = execute_query(query, (advertiser_id, start_date, end_date, advertiser_id, start_date, end_date))
        
        # Add PT names
        for row in results:
            pt = str(row.get('PT', ''))
            row['PT_NAME'] = PT_CONFIG.get(pt, DEFAULT_PT_CONFIG).get('name', 'Unknown')
            # Decode campaign name if needed
            if PT_CONFIG.get(pt, {}).get('url_decode'):
                row['CAMPAIGN_NAME'] = decode_publisher_name(row.get('CAMPAIGN_NAME'), True)
        
        return jsonify({'success': True, 'data': results})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


# ============================================================================
# LINE ITEM PERFORMANCE (Last-Touch Attribution)
# ============================================================================

@app.route('/api/lineitem-performance', methods=['GET'])
def get_lineitem_performance():
    """Get line item level performance with last-touch attribution."""
    advertiser_id = request.args.get('advertiser_id')
    campaign_id = request.args.get('campaign_id')
    start_date = request.args.get('start_date', '2020-01-01')
    end_date = request.args.get('end_date', '2030-12-31')
    
    if not advertiser_id:
        return jsonify({'success': False, 'error': 'advertiser_id parameter required'}), 400
    
    try:
        campaign_filter = "AND IO_ID = %s" if campaign_id else ""
        params = [advertiser_id, start_date, end_date]
        if campaign_id:
            params.append(campaign_id)
        params.extend([advertiser_id, start_date, end_date])
        if campaign_id:
            params.append(campaign_id)
        
        query = f"""
            WITH last_touch_visits AS (
                SELECT *,
                    ROW_NUMBER() OVER (
                        PARTITION BY DEVICE_ID_QU, DRIVEBYDATE 
                        ORDER BY IMP_TIMESTAMP DESC
                    ) as rn
                FROM QUORUMDB.SEGMENT_DATA.QUORUM_ADV_STORE_VISITS
                WHERE QUORUM_ADVERTISER_ID = %s
                  AND IS_STORE_VISIT = TRUE
                  AND IMP_TIMESTAMP >= %s
                  AND IMP_TIMESTAMP < %s
                  {campaign_filter}
            ),
            impressions_by_lineitem AS (
                SELECT 
                    IO_ID as CAMPAIGN_ID,
                    MAX(IO_NAME) as CAMPAIGN_NAME,
                    LINEITEM_ID,
                    MAX(LINEITEM_NAME) as LINEITEM_NAME,
                    MAX(PT) as PT,
                    COUNT(*) as IMPRESSIONS
                FROM QUORUMDB.SEGMENT_DATA.QUORUM_ADV_STORE_VISITS
                WHERE QUORUM_ADVERTISER_ID = %s
                  AND LINEITEM_ID IS NOT NULL
                  AND IMP_TIMESTAMP >= %s
                  AND IMP_TIMESTAMP < %s
                  {campaign_filter}
                GROUP BY IO_ID, LINEITEM_ID
            ),
            visits_by_lineitem AS (
                SELECT 
                    LINEITEM_ID,
                    COUNT(*) as S_VISITS
                FROM last_touch_visits
                WHERE rn = 1
                GROUP BY LINEITEM_ID
            )
            SELECT 
                i.PT,
                i.CAMPAIGN_ID,
                i.CAMPAIGN_NAME,
                i.LINEITEM_ID,
                i.LINEITEM_NAME,
                i.IMPRESSIONS,
                COALESCE(v.S_VISITS, 0) as S_VISITS
            FROM impressions_by_lineitem i
            LEFT JOIN visits_by_lineitem v ON i.LINEITEM_ID = v.LINEITEM_ID
            ORDER BY i.IMPRESSIONS DESC
            LIMIT 100
        """
        results = execute_query(query, tuple(params))
        
        # Add PT names
        for row in results:
            pt = str(row.get('PT', ''))
            row['PT_NAME'] = PT_CONFIG.get(pt, DEFAULT_PT_CONFIG).get('name', 'Unknown')
        
        return jsonify({'success': True, 'data': results})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


# ============================================================================
# PUBLISHER PERFORMANCE (Last-Touch Attribution with PT Config)
# ============================================================================

@app.route('/api/publisher-performance', methods=['GET'])
def get_publisher_performance():
    """Get publisher-level performance with last-touch attribution.
    
    Uses PT configuration to determine which column to use for publisher grouping.
    Respects agency/advertiser overrides.
    """
    advertiser_id = request.args.get('advertiser_id')
    start_date = request.args.get('start_date', '2020-01-01')
    end_date = request.args.get('end_date', '2030-12-31')
    
    if not advertiser_id:
        return jsonify({'success': False, 'error': 'advertiser_id parameter required'}), 400
    
    try:
        # Query gets all three publisher columns - we resolve which to use in Python
        query = """
            WITH last_touch_visits AS (
                SELECT *,
                    ROW_NUMBER() OVER (
                        PARTITION BY DEVICE_ID_QU, DRIVEBYDATE 
                        ORDER BY IMP_TIMESTAMP DESC
                    ) as rn
                FROM QUORUMDB.SEGMENT_DATA.QUORUM_ADV_STORE_VISITS
                WHERE QUORUM_ADVERTISER_ID = %s
                  AND IS_STORE_VISIT = TRUE
                  AND IMP_TIMESTAMP >= %s
                  AND IMP_TIMESTAMP < %s
            ),
            impressions_by_publisher AS (
                SELECT 
                    PT,
                    AGENCY_ID,
                    PUBLISHER_ID,
                    PUBLISHER_CODE,
                    SITE,
                    COUNT(*) as IMPRESSIONS
                FROM QUORUMDB.SEGMENT_DATA.QUORUM_ADV_STORE_VISITS
                WHERE QUORUM_ADVERTISER_ID = %s
                  AND IMP_TIMESTAMP >= %s
                  AND IMP_TIMESTAMP < %s
                GROUP BY PT, AGENCY_ID, PUBLISHER_ID, PUBLISHER_CODE, SITE
            ),
            visits_by_publisher AS (
                SELECT 
                    PT,
                    PUBLISHER_ID,
                    PUBLISHER_CODE,
                    SITE,
                    COUNT(*) as S_VISITS
                FROM last_touch_visits
                WHERE rn = 1
                GROUP BY PT, PUBLISHER_ID, PUBLISHER_CODE, SITE
            )
            SELECT 
                i.PT,
                i.AGENCY_ID,
                i.PUBLISHER_ID,
                i.PUBLISHER_CODE,
                i.SITE,
                i.IMPRESSIONS,
                COALESCE(v.S_VISITS, 0) as S_VISITS
            FROM impressions_by_publisher i
            LEFT JOIN visits_by_publisher v 
                ON COALESCE(i.PT, '') = COALESCE(v.PT, '')
                AND COALESCE(i.PUBLISHER_ID, -1) = COALESCE(v.PUBLISHER_ID, -1)
                AND COALESCE(i.PUBLISHER_CODE, '') = COALESCE(v.PUBLISHER_CODE, '')
                AND COALESCE(i.SITE, '') = COALESCE(v.SITE, '')
            WHERE i.IMPRESSIONS >= 100
            ORDER BY i.IMPRESSIONS DESC
            LIMIT 200
        """
        results = execute_query(query, (advertiser_id, start_date, end_date, advertiser_id, start_date, end_date))
        
        # Post-process results to resolve publisher names based on config hierarchy
        for row in results:
            pt = str(row.get('PT', ''))
            agency_id = str(row.get('AGENCY_ID', ''))
            
            # Get resolved config for this PT/Agency/Advertiser
            config = get_publisher_config(pt, agency_id, advertiser_id)
            
            # Determine the primary publisher value based on config
            pub_col = config['publisher_column']
            if pub_col == 'PUBLISHER_CODE':
                raw_value = row.get('PUBLISHER_CODE')
            elif pub_col == 'PUBLISHER_ID':
                raw_value = row.get('PUBLISHER_ID')
            elif pub_col == 'SITE':
                raw_value = row.get('SITE')
            else:
                raw_value = row.get('PUBLISHER_CODE') or row.get('PUBLISHER_ID') or row.get('SITE')
            
            # Decode if needed
            publisher_name = decode_publisher_name(raw_value, config.get('url_decode', False))
            
            # Add resolved fields
            row['PT_NAME'] = config['name']
            row['PUBLISHER_NAME'] = publisher_name
            row['PUBLISHER_COLUMN_USED'] = pub_col
        
        return jsonify({'success': True, 'data': results})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


# ============================================================================
# WEB EVENTS MODULE (Last-Touch Attribution)
# ============================================================================

@app.route('/api/web/advertisers', methods=['GET'])
def get_web_advertisers():
    """Get advertisers with web event data."""
    try:
        query = """
            WITH last_touch AS (
                SELECT *,
                    ROW_NUMBER() OVER (
                        PARTITION BY MAID, DATE(SITE_VISIT_TIMESTAMP)
                        ORDER BY SITE_VISIT_TIMESTAMP DESC
                    ) as rn
                FROM QUORUMDB.SEGMENT_DATA.WEB_VISITORS_TO_LOG
            )
            SELECT 
                lt.QUORUM_ADVERTISER_ID as ADVERTISER_ID,
                MAX(aa.COMP_NAME) as ADVERTISER_NAME,
                MAX(aa.AGENCY_NAME) as AGENCY_NAME,
                COUNT(*) as TOTAL_IMPRESSIONS,
                SUM(CASE WHEN lt.IS_SITE_VISIT = 'TRUE' AND lt.rn = 1 THEN 1 ELSE 0 END) as W_VISITS,
                SUM(CASE WHEN lt.IS_LEAD = 'TRUE' AND lt.rn = 1 THEN 1 ELSE 0 END) as W_LEADS,
                SUM(CASE WHEN lt.IS_PURCHASE = 'TRUE' AND lt.rn = 1 THEN 1 ELSE 0 END) as W_PURCHASES,
                SUM(CASE WHEN lt.IS_PURCHASE = 'TRUE' AND lt.rn = 1 AND lt.PURCHASE_VALUE IS NOT NULL AND lt.PURCHASE_VALUE != '' 
                    THEN TRY_CAST(lt.PURCHASE_VALUE AS FLOAT) ELSE 0 END) as W_AMOUNT
            FROM last_touch lt
            LEFT JOIN QUORUMDB.SEGMENT_DATA.AGENCY_ADVERTISER aa 
                ON lt.QUORUM_ADVERTISER_ID = aa.ID
            GROUP BY lt.QUORUM_ADVERTISER_ID
            HAVING COUNT(*) > 1000
            ORDER BY TOTAL_IMPRESSIONS DESC
            LIMIT 100
        """
        results = execute_query(query)
        return jsonify({'success': True, 'data': results})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/web/summary', methods=['GET'])
def get_web_summary():
    """Get web event summary for a specific advertiser."""
    advertiser_id = request.args.get('advertiser_id')
    start_date = request.args.get('start_date', '2020-01-01')
    end_date = request.args.get('end_date', '2030-12-31')
    
    if not advertiser_id:
        return jsonify({'success': False, 'error': 'advertiser_id parameter required'}), 400
    
    try:
        query = """
            WITH last_touch AS (
                SELECT *,
                    ROW_NUMBER() OVER (
                        PARTITION BY MAID, DATE(SITE_VISIT_TIMESTAMP)
                        ORDER BY SITE_VISIT_TIMESTAMP DESC
                    ) as rn
                FROM QUORUMDB.SEGMENT_DATA.WEB_VISITORS_TO_LOG
                WHERE QUORUM_ADVERTISER_ID = %s
                  AND SITE_VISIT_TIMESTAMP >= %s
                  AND SITE_VISIT_TIMESTAMP < %s
            )
            SELECT 
                %s as ADVERTISER_ID,
                MAX(aa.COMP_NAME) as ADVERTISER_NAME,
                COUNT(*) as TOTAL_IMPRESSIONS,
                SUM(CASE WHEN lt.IS_SITE_VISIT = 'TRUE' AND lt.rn = 1 THEN 1 ELSE 0 END) as W_VISITS,
                SUM(CASE WHEN lt.IS_LEAD = 'TRUE' AND lt.rn = 1 THEN 1 ELSE 0 END) as W_LEADS,
                SUM(CASE WHEN lt.IS_PURCHASE = 'TRUE' AND lt.rn = 1 THEN 1 ELSE 0 END) as W_PURCHASES,
                SUM(CASE WHEN lt.IS_PURCHASE = 'TRUE' AND lt.rn = 1 AND lt.PURCHASE_VALUE IS NOT NULL AND lt.PURCHASE_VALUE != '' 
                    THEN TRY_CAST(lt.PURCHASE_VALUE AS FLOAT) ELSE 0 END) as W_AMOUNT
            FROM last_touch lt
            LEFT JOIN QUORUMDB.SEGMENT_DATA.AGENCY_ADVERTISER aa 
                ON lt.QUORUM_ADVERTISER_ID = aa.ID
        """
        results = execute_query(query, (advertiser_id, start_date, end_date, advertiser_id))
        if results:
            return jsonify({'success': True, 'data': results[0]})
        else:
            return jsonify({'success': False, 'error': 'Advertiser not found'}), 404
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/web/zip-performance', methods=['GET'])
def get_web_zip_performance():
    """Get ZIP-level web event data with last-touch attribution."""
    advertiser_id = request.args.get('advertiser_id')
    start_date = request.args.get('start_date', '2020-01-01')
    end_date = request.args.get('end_date', '2030-12-31')
    min_impressions = request.args.get('min_impressions', '100')
    
    if not advertiser_id:
        return jsonify({'success': False, 'error': 'advertiser_id parameter required'}), 400
    
    try:
        query = """
            WITH last_touch AS (
                SELECT wv.*,
                    ROW_NUMBER() OVER (
                        PARTITION BY wv.MAID, DATE(wv.SITE_VISIT_TIMESTAMP)
                        ORDER BY wv.SITE_VISIT_TIMESTAMP DESC
                    ) as rn
                FROM QUORUMDB.SEGMENT_DATA.WEB_VISITORS_TO_LOG wv
                WHERE wv.QUORUM_ADVERTISER_ID = %s
                  AND wv.SITE_VISIT_TIMESTAMP >= %s
                  AND wv.SITE_VISIT_TIMESTAMP < %s
            ),
            impressions_by_zip AS (
                SELECT 
                    mca.ZIP_CODE,
                    COUNT(*) as IMPRESSIONS
                FROM last_touch lt
                JOIN QUORUM_CROSS_CLOUD.ATTAIN_FEED.MAID_CENTROID_ASSOCIATION mca 
                    ON LOWER(lt.MAID) = LOWER(mca.DEVICE_ID)
                WHERE mca.ZIP_CODE IS NOT NULL AND mca.ZIP_CODE != ''
                GROUP BY mca.ZIP_CODE
            ),
            conversions_by_zip AS (
                SELECT 
                    mca.ZIP_CODE,
                    SUM(CASE WHEN lt.IS_SITE_VISIT = 'TRUE' THEN 1 ELSE 0 END) as W_VISITS,
                    SUM(CASE WHEN lt.IS_LEAD = 'TRUE' THEN 1 ELSE 0 END) as W_LEADS,
                    SUM(CASE WHEN lt.IS_PURCHASE = 'TRUE' THEN 1 ELSE 0 END) as W_PURCHASES,
                    SUM(CASE WHEN lt.IS_PURCHASE = 'TRUE' AND lt.PURCHASE_VALUE IS NOT NULL AND lt.PURCHASE_VALUE != '' 
                        THEN TRY_CAST(lt.PURCHASE_VALUE AS FLOAT) ELSE 0 END) as W_AMOUNT
                FROM last_touch lt
                JOIN QUORUM_CROSS_CLOUD.ATTAIN_FEED.MAID_CENTROID_ASSOCIATION mca 
                    ON LOWER(lt.MAID) = LOWER(mca.DEVICE_ID)
                WHERE lt.rn = 1
                  AND mca.ZIP_CODE IS NOT NULL AND mca.ZIP_CODE != ''
                GROUP BY mca.ZIP_CODE
            )
            SELECT 
                i.ZIP_CODE,
                zdm.DMA_CODE,
                zdm.DMA_NAME,
                zpd.POPULATION,
                i.IMPRESSIONS,
                COALESCE(c.W_VISITS, 0) as W_VISITS,
                COALESCE(c.W_LEADS, 0) as W_LEADS,
                COALESCE(c.W_PURCHASES, 0) as W_PURCHASES,
                COALESCE(c.W_AMOUNT, 0) as W_AMOUNT
            FROM impressions_by_zip i
            LEFT JOIN conversions_by_zip c ON i.ZIP_CODE = c.ZIP_CODE
            LEFT JOIN QUORUMDB.SEGMENT_DATA.ZIP_DMA_MAPPING zdm ON i.ZIP_CODE = zdm.ZIP_CODE
            LEFT JOIN QUORUMDB.SEGMENT_DATA.ZIP_POPULATION_DATA zpd ON i.ZIP_CODE = zpd.ZIP_CODE
            WHERE i.IMPRESSIONS >= %s
            ORDER BY i.IMPRESSIONS DESC
            LIMIT 1000
        """
        results = execute_query(query, (advertiser_id, start_date, end_date, min_impressions))
        return jsonify({'success': True, 'data': results})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500
# ============================================================================
# STORE ZIP OPTIMIZATION MODULE - CORRECTED VERSION
# Replace your existing endpoints with this version
# ============================================================================

@app.route('/api/store/agencies-postal', methods=['GET'])
def get_agencies_postal():
    """Get agencies from CAMPAIGN_POSTAL_REPORTING for ZIP optimization."""
    try:
        query = """
            SELECT DISTINCT 
                AGENCY_ID as id,
                AGENCY_NAME as name,
                COUNT(DISTINCT ADVERTISER_ID) as advertiserCount
            FROM QUORUMDB.SEGMENT_DATA.CAMPAIGN_POSTAL_REPORTING
            WHERE AGENCY_ID IS NOT NULL 
                AND AGENCY_NAME IS NOT NULL
            GROUP BY AGENCY_ID, AGENCY_NAME
            ORDER BY AGENCY_NAME
        """
        results = execute_query(query)
        return jsonify({'success': True, 'data': results})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/store/advertisers-postal', methods=['GET'])
def get_advertisers_postal():
    """Get advertisers from CAMPAIGN_POSTAL_REPORTING for ZIP optimization."""
    try:
        agency_id = request.args.get('agency_id')
        
        if not agency_id:
            return jsonify({'success': False, 'error': 'agency_id is required'}), 400
        
        query = """
            SELECT DISTINCT 
                ADVERTISER_ID as id,
                ADVERTISER_NAME as name
            FROM QUORUMDB.SEGMENT_DATA.CAMPAIGN_POSTAL_REPORTING
            WHERE AGENCY_ID = %s
                AND ADVERTISER_NAME IS NOT NULL
            ORDER BY ADVERTISER_NAME
            LIMIT 100
        """
        results = execute_query(query, (agency_id,))
        return jsonify({'success': True, 'data': results})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/store/campaign-performance-postal', methods=['GET'])
def get_campaign_performance_postal():
    """Get campaign performance from CAMPAIGN_POSTAL_REPORTING."""
    try:
        advertiser_id = request.args.get('advertiser_id')
        start_date = request.args.get('start_date', '')
        end_date = request.args.get('end_date', '')
        
        if not advertiser_id:
            return jsonify({'success': False, 'error': 'advertiser_id is required'}), 400
        
        date_filter = ""
        params = [advertiser_id]
        
        if start_date:
            date_filter += " AND DATE >= %s"
            params.append(start_date)
        if end_date:
            date_filter += " AND DATE <= %s"
            params.append(end_date)
        
        query = f"""
            SELECT 
                CAMPAIGN_ID,
                CAMPAIGN_NAME,
                SUM(IMPRESSIONS) as IMPRESSIONS,
                SUM(STORE_VISITS) as VISITS,
                CASE 
                    WHEN SUM(IMPRESSIONS) > 0 THEN SUM(STORE_VISITS)::FLOAT / SUM(IMPRESSIONS)::FLOAT
                    ELSE 0 
                END as visitRate
            FROM QUORUMDB.SEGMENT_DATA.CAMPAIGN_POSTAL_REPORTING
            WHERE ADVERTISER_ID = %s
                {date_filter}
            GROUP BY CAMPAIGN_ID, CAMPAIGN_NAME
            HAVING SUM(IMPRESSIONS) > 0
            ORDER BY IMPRESSIONS DESC
            LIMIT 50
        """
        
        campaigns = execute_query(query, tuple(params))
        
        return jsonify({
            'success': True,
            'data': {'campaigns': campaigns}
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/store/zip-analysis-postal', methods=['GET'])
def get_zip_analysis_postal():
    """Get ZIP code analysis from CAMPAIGN_POSTAL_REPORTING for reallocation."""
    try:
        advertiser_id = request.args.get('advertiser_id')
        start_date = request.args.get('start_date', '')
        end_date = request.args.get('end_date', '')
        
        if not advertiser_id:
            return jsonify({'success': False, 'error': 'advertiser_id is required'}), 400
        
        # Build params lists
        params_baseline = [advertiser_id]
        params_zip = [advertiser_id]
        params_dma = [advertiser_id]
        
        # Build date filters
        date_filter = ""
        if start_date:
            date_filter += " AND DATE >= %s"
            params_baseline.append(start_date)
            params_zip.append(start_date)
            params_dma.append(start_date)
        if end_date:
            date_filter += " AND DATE <= %s"
            params_baseline.append(end_date)
            params_zip.append(end_date)
            params_dma.append(end_date)
        
        # Get baseline metrics
        baseline_query = f"""
            SELECT 
                SUM(IMPRESSIONS) as totalImpressions,
                SUM(STORE_VISITS) as totalVisits,
                CASE 
                    WHEN SUM(IMPRESSIONS) > 0 THEN SUM(STORE_VISITS)::FLOAT / SUM(IMPRESSIONS)::FLOAT
                    ELSE 0 
                END as overallVisitRate,
                SUM(USER_HOME_POSTAL_CODE_POPULATION) as totalPopulation
            FROM QUORUMDB.SEGMENT_DATA.CAMPAIGN_POSTAL_REPORTING
            WHERE ADVERTISER_ID = %s
                {date_filter}
        """
        
        baseline_result = execute_query(baseline_query, tuple(params_baseline))
        baseline = baseline_result[0] if baseline_result else {
            'totalImpressions': 0,
            'totalVisits': 0,
            'overallVisitRate': 0,
            'totalPopulation': 0
        }
        
        # Get ZIP code data (only ZIPs with 3500+ impressions)
        zip_query = f"""
            SELECT 
                zdm.DMA_NAME as dma,
                cpr.USER_HOME_POSTAL_CODE as zip,
                MAX(cpr.USER_HOME_POSTAL_CODE_POPULATION) as population,
                SUM(cpr.IMPRESSIONS) as impressions,
                SUM(cpr.STORE_VISITS) as visits
            FROM QUORUMDB.SEGMENT_DATA.CAMPAIGN_POSTAL_REPORTING cpr
            LEFT JOIN QUORUMDB.SEGMENT_DATA.ZIP_DMA_MAPPING zdm 
                ON cpr.USER_HOME_POSTAL_CODE = zdm.ZIP_CODE
            WHERE cpr.ADVERTISER_ID = %s
                {date_filter}
                AND cpr.USER_HOME_POSTAL_CODE IS NOT NULL
                AND cpr.USER_HOME_POSTAL_CODE != 'UNKNOWN'
            GROUP BY zdm.DMA_NAME, cpr.USER_HOME_POSTAL_CODE
            HAVING SUM(cpr.IMPRESSIONS) >= 3500
            ORDER BY impressions DESC
            LIMIT 500
        """
        
        zip_codes = execute_query(zip_query, tuple(params_zip))
        
        # Get DMA ZIP counts
        dma_counts_query = f"""
            SELECT 
                zdm.DMA_NAME,
                COUNT(DISTINCT cpr.USER_HOME_POSTAL_CODE) as ZIP_COUNT
            FROM QUORUMDB.SEGMENT_DATA.CAMPAIGN_POSTAL_REPORTING cpr
            LEFT JOIN QUORUMDB.SEGMENT_DATA.ZIP_DMA_MAPPING zdm 
                ON cpr.USER_HOME_POSTAL_CODE = zdm.ZIP_CODE
            WHERE cpr.ADVERTISER_ID = %s
                {date_filter}
                AND cpr.USER_HOME_POSTAL_CODE IS NOT NULL
                AND cpr.USER_HOME_POSTAL_CODE != 'UNKNOWN'
            GROUP BY zdm.DMA_NAME
            HAVING COUNT(DISTINCT cpr.USER_HOME_POSTAL_CODE) > 0
        """
        
        dma_counts = execute_query(dma_counts_query, tuple(params_dma))
        
        # Build dma_zip_counts dict - handle any case variation
        dma_zip_counts = {}
        for row in dma_counts:
            dma_name = row.get('DMA_NAME') or row.get('dma_name') or row.get('dma')
            zip_count = row.get('ZIP_COUNT') or row.get('zip_count') or row.get('zipCount') or row.get('ZIPCOUNT')
            if dma_name:
                dma_zip_counts[dma_name] = zip_count
        
        return jsonify({
            'success': True,
            'data': {
                'baseline': baseline,
                'zipCodes': zip_codes,
                'dmaZipCounts': dma_zip_counts
            }
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500
        """
        
        baseline_result = execute_query(baseline_query, tuple(params_baseline))
        baseline = baseline_result[0] if baseline_result else {
            'totalImpressions': 0,
            'totalVisits': 0,
            'overallVisitRate': 0,
            'totalPopulation': 0
        }
        
        # Get ZIP code data (only ZIPs with 3500+ impressions)
        zip_query = f"""
            SELECT 
                zdm.DMA_NAME as dma,
                cpr.USER_HOME_POSTAL_CODE as zip,
                MAX(cpr.USER_HOME_POSTAL_CODE_POPULATION) as population,
                SUM(cpr.IMPRESSIONS) as impressions,
                SUM(cpr.STORE_VISITS) as visits
            FROM QUORUMDB.SEGMENT_DATA.CAMPAIGN_POSTAL_REPORTING cpr
            LEFT JOIN QUORUMDB.SEGMENT_DATA.ZIP_DMA_MAPPING zdm 
                ON cpr.USER_HOME_POSTAL_CODE = zdm.ZIP_CODE
            WHERE cpr.ADVERTISER_ID = %s
                {date_filter}
                AND cpr.USER_HOME_POSTAL_CODE IS NOT NULL
                AND cpr.USER_HOME_POSTAL_CODE != 'UNKNOWN'
            GROUP BY zdm.DMA_NAME, cpr.USER_HOME_POSTAL_CODE
            HAVING SUM(cpr.IMPRESSIONS) >= 3500
            ORDER BY impressions DESC
            LIMIT 500
        """
        
        zip_codes = execute_query(zip_query, tuple(params_zip))
        
        # Get DMA ZIP counts
        dma_counts_query = f"""
            SELECT 
                zdm.DMA_NAME as dma,
                COUNT(DISTINCT cpr.USER_HOME_POSTAL_CODE) as zipCount
            FROM QUORUMDB.SEGMENT_DATA.CAMPAIGN_POSTAL_REPORTING cpr
            LEFT JOIN QUORUMDB.SEGMENT_DATA.ZIP_DMA_MAPPING zdm 
                ON cpr.USER_HOME_POSTAL_CODE = zdm.ZIP_CODE
            WHERE cpr.ADVERTISER_ID = %s
                {date_filter}
                AND cpr.USER_HOME_POSTAL_CODE IS NOT NULL
                AND cpr.USER_HOME_POSTAL_CODE != 'UNKNOWN'
            GROUP BY zdm.DMA_NAME
            HAVING COUNT(DISTINCT cpr.USER_HOME_POSTAL_CODE) > 0
        """
        
        dma_counts = execute_query(dma_counts_query, tuple(params_dma))
        dma_zip_counts = {row.get('DMA') or row.get('dma'): row.get('ZIPCOUNT') or row.get('zipCount') for row in dma_counts}
        
        return jsonify({
            'success': True,
            'data': {
                'baseline': baseline,
                'zipCodes': zip_codes,
                'dmaZipCounts': dma_zip_counts
            }
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


# ============================================================================
# WEB CAMPAIGN PERFORMANCE (Last-Touch Attribution)
# Uses PARAMOUNT_AD_FULL_ATTRIBUTION_V2 which has campaign/lineitem/publisher data
# ============================================================================

@app.route('/api/web/campaign-performance', methods=['GET'])
def get_web_campaign_performance():
    """Get campaign (IO) level web performance with last-touch attribution.
    
    Uses PARAMOUNT_AD_FULL_ATTRIBUTION_V2 which has full campaign/lineitem data.
    Last-touch is determined by MAX(IMP_DATE) per device per visit.
    """
    advertiser_id = request.args.get('advertiser_id')
    start_date = request.args.get('start_date', '2020-01-01')
    end_date = request.args.get('end_date', '2030-12-31')
    
    if not advertiser_id:
        return jsonify({'success': False, 'error': 'advertiser_id parameter required'}), 400
    
    try:
        query = """
            WITH last_touch AS (
                SELECT *,
                    ROW_NUMBER() OVER (
                        PARTITION BY IMP_MAID, SITE_VISIT_ID 
                        ORDER BY IMP_DATE DESC
                    ) as rn
                FROM QUORUMDB.SEGMENT_DATA.PARAMOUNT_AD_FULL_ATTRIBUTION_V2
                WHERE QUORUM_ADVERTISER_ID = %s
                  AND IS_SITE_VISIT = 'TRUE'
                  AND SITE_VISIT_TIMESTAMP >= %s
                  AND SITE_VISIT_TIMESTAMP < %s
            ),
            impressions_by_campaign AS (
                SELECT 
                    IO_ID as CAMPAIGN_ID,
                    MAX(IO_NAME) as CAMPAIGN_NAME,
                    MAX(PT) as PT,
                    COUNT(*) as IMPRESSIONS
                FROM QUORUMDB.SEGMENT_DATA.PARAMOUNT_AD_FULL_ATTRIBUTION_V2
                WHERE QUORUM_ADVERTISER_ID = %s
                  AND SITE_VISIT_TIMESTAMP >= %s
                  AND SITE_VISIT_TIMESTAMP < %s
                GROUP BY IO_ID
            ),
            visits_by_campaign AS (
                SELECT 
                    IO_ID as CAMPAIGN_ID,
                    COUNT(*) as W_VISITS
                FROM last_touch
                WHERE rn = 1
                GROUP BY IO_ID
            )
            SELECT 
                i.PT,
                i.CAMPAIGN_ID,
                i.CAMPAIGN_NAME,
                i.IMPRESSIONS,
                COALESCE(v.W_VISITS, 0) as W_VISITS
            FROM impressions_by_campaign i
            LEFT JOIN visits_by_campaign v ON i.CAMPAIGN_ID = v.CAMPAIGN_ID
            ORDER BY i.IMPRESSIONS DESC
            LIMIT 50
        """
        results = execute_query(query, (advertiser_id, start_date, end_date, advertiser_id, start_date, end_date))
        
        # Add PT names
        for row in results:
            pt = str(row.get('PT', ''))
            row['PT_NAME'] = PT_CONFIG.get(pt, DEFAULT_PT_CONFIG).get('name', 'Unknown')
        
        return jsonify({'success': True, 'data': results})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/web/lineitem-performance', methods=['GET'])
def get_web_lineitem_performance():
    """Get line item level web performance with last-touch attribution."""
    advertiser_id = request.args.get('advertiser_id')
    campaign_id = request.args.get('campaign_id')
    start_date = request.args.get('start_date', '2020-01-01')
    end_date = request.args.get('end_date', '2030-12-31')
    
    if not advertiser_id:
        return jsonify({'success': False, 'error': 'advertiser_id parameter required'}), 400
    
    try:
        campaign_filter = "AND IO_ID = %s" if campaign_id else ""
        params = [advertiser_id, start_date, end_date]
        if campaign_id:
            params.append(campaign_id)
        params.extend([advertiser_id, start_date, end_date])
        if campaign_id:
            params.append(campaign_id)
        
        query = f"""
            WITH last_touch AS (
                SELECT *,
                    ROW_NUMBER() OVER (
                        PARTITION BY IMP_MAID, SITE_VISIT_ID 
                        ORDER BY IMP_DATE DESC
                    ) as rn
                FROM QUORUMDB.SEGMENT_DATA.PARAMOUNT_AD_FULL_ATTRIBUTION_V2
                WHERE QUORUM_ADVERTISER_ID = %s
                  AND IS_SITE_VISIT = 'TRUE'
                  AND SITE_VISIT_TIMESTAMP >= %s
                  AND SITE_VISIT_TIMESTAMP < %s
                  {campaign_filter}
            ),
            impressions_by_lineitem AS (
                SELECT 
                    IO_ID as CAMPAIGN_ID,
                    MAX(IO_NAME) as CAMPAIGN_NAME,
                    LINEITEM_ID,
                    MAX(LINEITEM_NAME) as LINEITEM_NAME,
                    MAX(PT) as PT,
                    COUNT(*) as IMPRESSIONS
                FROM QUORUMDB.SEGMENT_DATA.PARAMOUNT_AD_FULL_ATTRIBUTION_V2
                WHERE QUORUM_ADVERTISER_ID = %s
                  AND LINEITEM_ID IS NOT NULL
                  AND SITE_VISIT_TIMESTAMP >= %s
                  AND SITE_VISIT_TIMESTAMP < %s
                  {campaign_filter}
                GROUP BY IO_ID, LINEITEM_ID
            ),
            visits_by_lineitem AS (
                SELECT 
                    LINEITEM_ID,
                    COUNT(*) as W_VISITS
                FROM last_touch
                WHERE rn = 1
                GROUP BY LINEITEM_ID
            )
            SELECT 
                i.PT,
                i.CAMPAIGN_ID,
                i.CAMPAIGN_NAME,
                i.LINEITEM_ID,
                i.LINEITEM_NAME,
                i.IMPRESSIONS,
                COALESCE(v.W_VISITS, 0) as W_VISITS
            FROM impressions_by_lineitem i
            LEFT JOIN visits_by_lineitem v ON i.LINEITEM_ID = v.LINEITEM_ID
            ORDER BY i.IMPRESSIONS DESC
            LIMIT 100
        """
        results = execute_query(query, tuple(params))
        
        # Add PT names
        for row in results:
            pt = str(row.get('PT', ''))
            row['PT_NAME'] = PT_CONFIG.get(pt, DEFAULT_PT_CONFIG).get('name', 'Unknown')
        
        return jsonify({'success': True, 'data': results})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/web/publisher-performance', methods=['GET'])
def get_web_publisher_performance():
    """Get publisher-level web performance with last-touch attribution.
    
    Uses SITE column from PARAMOUNT_AD_FULL_ATTRIBUTION_V2.
    """
    advertiser_id = request.args.get('advertiser_id')
    start_date = request.args.get('start_date', '2020-01-01')
    end_date = request.args.get('end_date', '2030-12-31')
    
    if not advertiser_id:
        return jsonify({'success': False, 'error': 'advertiser_id parameter required'}), 400
    
    try:
        query = """
            WITH last_touch AS (
                SELECT *,
                    ROW_NUMBER() OVER (
                        PARTITION BY IMP_MAID, SITE_VISIT_ID 
                        ORDER BY IMP_DATE DESC
                    ) as rn
                FROM QUORUMDB.SEGMENT_DATA.PARAMOUNT_AD_FULL_ATTRIBUTION_V2
                WHERE QUORUM_ADVERTISER_ID = %s
                  AND IS_SITE_VISIT = 'TRUE'
                  AND SITE_VISIT_TIMESTAMP >= %s
                  AND SITE_VISIT_TIMESTAMP < %s
            ),
            impressions_by_publisher AS (
                SELECT 
                    PT,
                    SITE as PUBLISHER_NAME,
                    COUNT(*) as IMPRESSIONS
                FROM QUORUMDB.SEGMENT_DATA.PARAMOUNT_AD_FULL_ATTRIBUTION_V2
                WHERE QUORUM_ADVERTISER_ID = %s
                  AND SITE_VISIT_TIMESTAMP >= %s
                  AND SITE_VISIT_TIMESTAMP < %s
                GROUP BY PT, SITE
            ),
            visits_by_publisher AS (
                SELECT 
                    PT,
                    SITE as PUBLISHER_NAME,
                    COUNT(*) as W_VISITS
                FROM last_touch
                WHERE rn = 1
                GROUP BY PT, SITE
            )
            SELECT 
                i.PT,
                i.PUBLISHER_NAME,
                i.IMPRESSIONS,
                COALESCE(v.W_VISITS, 0) as W_VISITS
            FROM impressions_by_publisher i
            LEFT JOIN visits_by_publisher v 
                ON COALESCE(i.PT, '') = COALESCE(v.PT, '')
                AND COALESCE(i.PUBLISHER_NAME, '') = COALESCE(v.PUBLISHER_NAME, '')
            WHERE i.IMPRESSIONS >= 100
            ORDER BY i.IMPRESSIONS DESC
            LIMIT 200
        """
        results = execute_query(query, (advertiser_id, start_date, end_date, advertiser_id, start_date, end_date))
        
        # Add PT names
        for row in results:
            pt = str(row.get('PT', ''))
            row['PT_NAME'] = PT_CONFIG.get(pt, DEFAULT_PT_CONFIG).get('name', 'Unknown')
        
        return jsonify({'success': True, 'data': results})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


# ============================================================================
# WEB ADVERTISERS (Updated to use PARAMOUNT table for better data)
# ============================================================================

@app.route('/api/web/advertisers-v2', methods=['GET'])
def get_web_advertisers_v2():
    """Get advertisers with web event data from PARAMOUNT attribution table.
    
    This provides more complete data including campaign/lineitem counts.
    """
    try:
        query = """
            WITH last_touch AS (
                SELECT *,
                    ROW_NUMBER() OVER (
                        PARTITION BY IMP_MAID, SITE_VISIT_ID 
                        ORDER BY IMP_DATE DESC
                    ) as rn
                FROM QUORUMDB.SEGMENT_DATA.PARAMOUNT_AD_FULL_ATTRIBUTION_V2
                WHERE IS_SITE_VISIT = 'TRUE'
            )
            SELECT 
                QUORUM_ADVERTISER_ID as ADVERTISER_ID,
                MAX(ADVERTISER_NAME) as ADVERTISER_NAME,
                MAX(PT) as PT,
                COUNT(*) as TOTAL_IMPRESSIONS,
                SUM(CASE WHEN IS_SITE_VISIT = 'TRUE' THEN 1 ELSE 0 END) as ATTRIBUTED_IMPRESSIONS,
                SUM(CASE WHEN rn = 1 THEN 1 ELSE 0 END) as W_VISITS,
                COUNT(DISTINCT IO_ID) as CAMPAIGN_COUNT,
                COUNT(DISTINCT LINEITEM_ID) as LINEITEM_COUNT
            FROM last_touch
            GROUP BY QUORUM_ADVERTISER_ID
            HAVING SUM(CASE WHEN rn = 1 THEN 1 ELSE 0 END) > 0
            ORDER BY W_VISITS DESC
            LIMIT 100
        """
        results = execute_query(query)
        
        # Add PT names
        for row in results:
            pt = str(row.get('PT', ''))
            row['PT_NAME'] = PT_CONFIG.get(pt, DEFAULT_PT_CONFIG).get('name', 'Unknown')
        
        return jsonify({'success': True, 'data': results})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500
    
if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=True)
