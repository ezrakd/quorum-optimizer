"""
Quorum Optimizer API v3 - Unified Architecture
Supports 14 agencies via dual-path query system:
- Class A (7 agencies): QRM_ALL_VISITS_V3 (fast, pre-deduplicated)
- Class B (7 agencies): CAMPAIGN_PERFORMANCE_STORE_VISITS_RAW (slower, requires ROW_NUMBER)
"""

import os
from flask import Flask, request, jsonify
from flask_cors import CORS
import snowflake.connector
from datetime import datetime

app = Flask(__name__)
CORS(app)

# Snowflake connection
def get_snowflake_connection():
    return snowflake.connector.connect(
        user=os.environ.get('SNOWFLAKE_USER', 'OPTIMIZER_SERVICE_USER'),
        password=os.environ.get('SNOWFLAKE_PASSWORD'),
        account=os.environ.get('SNOWFLAKE_ACCOUNT', 'FZB05958.us-east-1'),
        warehouse=os.environ.get('SNOWFLAKE_WAREHOUSE', 'COMPUTE_WH'),
        database='QUORUMDB',
        schema='SEGMENT_DATA'
    )

# Agency Classification
CLASS_A_AGENCIES = [1480, 1956, 2298, 1955, 2514, 1950, 2086]  # QRM_ALL_VISITS_V3
CLASS_B_AGENCIES = [1813, 2234, 1972, 2379, 1445, 1880, 2744]  # CAMPAIGN_PERFORMANCE_STORE_VISITS_RAW

def get_agency_class(agency_id):
    """Determine which query pattern to use"""
    if agency_id in CLASS_A_AGENCIES:
        return 'A'
    elif agency_id in CLASS_B_AGENCIES:
        return 'B'
    else:
        return None

# ============================================================================
# AGENCIES ENDPOINT - Universal (combines both classes)
# ============================================================================

@app.route('/api/v3/agencies', methods=['GET'])
def get_agencies_v3():
    """Get all agencies with visit counts (Class A + Class B)"""
    try:
        conn = get_snowflake_connection()
        cursor = conn.cursor()
        
        # Class A agencies from QRM_ALL_VISITS_V3
        query_a = """
            SELECT 
                v.AGENCY_ID,
                aa.AGENCY_NAME,
                'A' as AGENCY_CLASS,
                COUNT(*) as TOTAL_VISITS,
                COUNT(DISTINCT v.QUORUM_ADVERTISER_ID) as ADVERTISER_COUNT
            FROM QUORUMDB.SEGMENT_DATA.QRM_ALL_VISITS_V3 v
            JOIN QUORUMDB.SEGMENT_DATA.AGENCY_ADVERTISER aa 
                ON v.AGENCY_ID = aa.ADVERTISER_ID
            WHERE v.AGENCY_ID IN (1480, 1956, 2298, 1955, 2514, 1950, 2086)
              AND v.VISIT_TYPE = 'STORE'
            GROUP BY v.AGENCY_ID, aa.AGENCY_NAME
        """
        
        # Class B agencies from CAMPAIGN_PERFORMANCE_STORE_VISITS_RAW
        query_b = """
            WITH deduplicated AS (
                SELECT 
                    cp.AGENCY_ID,
                    cp.ADVERTISER_ID,
                    cp.DEVICE_ID,
                    cp.DRIVE_BY_DATE,
                    cp.POI_MD5,
                    ROW_NUMBER() OVER (
                        PARTITION BY cp.DEVICE_ID, cp.DRIVE_BY_DATE, cp.POI_MD5
                        ORDER BY cp.IMP_ID DESC
                    ) as rn
                FROM QUORUMDB.SEGMENT_DATA.CAMPAIGN_PERFORMANCE_STORE_VISITS_RAW cp
                WHERE cp.AGENCY_ID IN (1813, 2234, 1972, 2379, 1445, 1880, 2744)
            )
            SELECT 
                d.AGENCY_ID,
                aa.AGENCY_NAME,
                'B' as AGENCY_CLASS,
                COUNT(*) as TOTAL_VISITS,
                COUNT(DISTINCT d.ADVERTISER_ID) as ADVERTISER_COUNT
            FROM deduplicated d
            JOIN QUORUMDB.SEGMENT_DATA.AGENCY_ADVERTISER aa 
                ON d.AGENCY_ID = aa.ADVERTISER_ID
            WHERE d.rn = 1
            GROUP BY d.AGENCY_ID, aa.AGENCY_NAME
        """
        
        # Execute both and combine
        cursor.execute(query_a)
        results_a = cursor.fetchall()
        
        cursor.execute(query_b)
        results_b = cursor.fetchall()
        
        # Combine results
        agencies = []
        for row in results_a + results_b:
            agencies.append({
                'AGENCY_ID': row[0],
                'AGENCY_NAME': row[1],
                'AGENCY_CLASS': row[2],
                'TOTAL_VISITS': row[3],
                'ADVERTISER_COUNT': row[4]
            })
        
        cursor.close()
        conn.close()
        
        return jsonify({'success': True, 'data': agencies})
        
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

# ============================================================================
# ADVERTISERS ENDPOINT - Dual-path
# ============================================================================

@app.route('/api/v3/advertisers', methods=['GET'])
def get_advertisers_v3():
    """Get advertisers for an agency (auto-detects Class A vs B)"""
    try:
        agency_id = int(request.args.get('agency_id'))
        agency_class = get_agency_class(agency_id)
        
        if not agency_class:
            return jsonify({'success': False, 'error': 'Agency not supported'}), 400
        
        conn = get_snowflake_connection()
        cursor = conn.cursor()
        
        if agency_class == 'A':
            # Class A: QRM_ALL_VISITS_V3
            query = """
                SELECT 
                    v.QUORUM_ADVERTISER_ID as ADVERTISER_ID,
                    aa.COMP_NAME as ADVERTISER_NAME,
                    COUNT(*) as TOTAL_VISITS
                FROM QUORUMDB.SEGMENT_DATA.QRM_ALL_VISITS_V3 v
                JOIN QUORUMDB.SEGMENT_DATA.AGENCY_ADVERTISER aa 
                    ON v.QUORUM_ADVERTISER_ID = aa.ID::TEXT
                WHERE v.AGENCY_ID = %s
                  AND v.VISIT_TYPE = 'STORE'
                GROUP BY v.QUORUM_ADVERTISER_ID, aa.COMP_NAME
                ORDER BY TOTAL_VISITS DESC
            """
        else:
            # Class B: CAMPAIGN_PERFORMANCE_STORE_VISITS_RAW
            query = """
                WITH deduplicated AS (
                    SELECT 
                        cp.ADVERTISER_ID,
                        cp.DEVICE_ID,
                        cp.DRIVE_BY_DATE,
                        cp.POI_MD5,
                        ROW_NUMBER() OVER (
                            PARTITION BY cp.DEVICE_ID, cp.DRIVE_BY_DATE, cp.POI_MD5
                            ORDER BY cp.IMP_ID DESC
                        ) as rn
                    FROM QUORUMDB.SEGMENT_DATA.CAMPAIGN_PERFORMANCE_STORE_VISITS_RAW cp
                    WHERE cp.AGENCY_ID = %s
                )
                SELECT 
                    d.ADVERTISER_ID,
                    aa.COMP_NAME as ADVERTISER_NAME,
                    COUNT(*) as TOTAL_VISITS
                FROM deduplicated d
                JOIN QUORUMDB.SEGMENT_DATA.AGENCY_ADVERTISER aa 
                    ON d.ADVERTISER_ID = aa.ID
                WHERE d.rn = 1
                GROUP BY d.ADVERTISER_ID, aa.COMP_NAME
                ORDER BY TOTAL_VISITS DESC
            """
        
        cursor.execute(query, (agency_id,))
        results = cursor.fetchall()
        
        advertisers = []
        for row in results:
            advertisers.append({
                'ADVERTISER_ID': str(row[0]),
                'ADVERTISER_NAME': row[1],
                'TOTAL_VISITS': row[2]
            })
        
        cursor.close()
        conn.close()
        
        return jsonify({'success': True, 'data': advertisers})
        
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

# ============================================================================
# SUMMARY ENDPOINT - Dual-path
# ============================================================================

@app.route('/api/v3/advertiser-summary', methods=['GET'])
def get_advertiser_summary_v3():
    """Get summary metrics for an advertiser (auto-detects Class A vs B)"""
    try:
        advertiser_id = request.args.get('advertiser_id')
        agency_id = int(request.args.get('agency_id'))
        start_date = request.args.get('start_date', '2020-01-01')
        end_date = request.args.get('end_date', '2030-12-31')
        
        agency_class = get_agency_class(agency_id)
        if not agency_class:
            return jsonify({'success': False, 'error': 'Agency not supported'}), 400
        
        conn = get_snowflake_connection()
        cursor = conn.cursor()
        
        if agency_class == 'A':
            # Class A: QRM_ALL_VISITS_V3
            query = """
                SELECT 
                    COUNT(*) as TOTAL_VISITS,
                    COUNT(DISTINCT MAID) as UNIQUE_DEVICES,
                    MIN(CONVERSION_DATE) as EARLIEST_DATE,
                    MAX(CONVERSION_DATE) as LATEST_DATE
                FROM QUORUMDB.SEGMENT_DATA.QRM_ALL_VISITS_V3
                WHERE QUORUM_ADVERTISER_ID = %s
                  AND AGENCY_ID = %s
                  AND VISIT_TYPE = 'STORE'
                  AND CONVERSION_DATE >= %s
                  AND CONVERSION_DATE < %s
            """
        else:
            # Class B: CAMPAIGN_PERFORMANCE_STORE_VISITS_RAW
            query = """
                WITH deduplicated AS (
                    SELECT 
                        cp.DEVICE_ID,
                        cp.DRIVE_BY_DATE,
                        cp.POI_MD5,
                        ROW_NUMBER() OVER (
                            PARTITION BY cp.DEVICE_ID, cp.DRIVE_BY_DATE, cp.POI_MD5
                            ORDER BY cp.IMP_ID DESC
                        ) as rn
                    FROM QUORUMDB.SEGMENT_DATA.CAMPAIGN_PERFORMANCE_STORE_VISITS_RAW cp
                    WHERE cp.ADVERTISER_ID = %s
                      AND cp.AGENCY_ID = %s
                      AND cp.DRIVE_BY_DATE >= %s
                      AND cp.DRIVE_BY_DATE < %s
                )
                SELECT 
                    COUNT(*) as TOTAL_VISITS,
                    COUNT(DISTINCT DEVICE_ID) as UNIQUE_DEVICES,
                    MIN(DRIVE_BY_DATE) as EARLIEST_DATE,
                    MAX(DRIVE_BY_DATE) as LATEST_DATE
                FROM deduplicated
                WHERE rn = 1
            """
        
        cursor.execute(query, (advertiser_id, agency_id, start_date, end_date))
        row = cursor.fetchone()
        
        # Get impression count from XANDR
        imp_query = """
            SELECT COUNT(*) as IMPRESSIONS
            FROM QUORUMDB.SEGMENT_DATA.XANDR_IMPRESSION_LOG x
            JOIN QUORUMDB.SEGMENT_DATA."QuorumAdvImpMapping" m 
                ON x.ADVERTISER_ID = m.ADVERTISER_ID
            WHERE m.QUORUM_ADVERTISER_ID = %s
              AND x.TIMESTAMP >= %s
              AND x.TIMESTAMP < %s
        """
        cursor.execute(imp_query, (advertiser_id, start_date, end_date))
        imp_row = cursor.fetchone()
        
        cursor.close()
        conn.close()
        
        total_visits = row[0] if row[0] else 0
        impressions = imp_row[0] if imp_row and imp_row[0] else 1
        
        return jsonify({
            'success': True,
            'data': {
                'TOTAL_VISITS': total_visits,
                'TOTAL_IMPRESSIONS': impressions,
                'VISIT_RATE': round(total_visits / impressions * 100, 3) if impressions > 0 else 0,
                'UNIQUE_DEVICES': row[1] if row[1] else 0,
                'DATE_RANGE_START': str(row[2]) if row[2] else start_date,
                'DATE_RANGE_END': str(row[3]) if row[3] else end_date
            }
        })
        
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

# ============================================================================
# ZIP PERFORMANCE ENDPOINT - Dual-path
# ============================================================================

@app.route('/api/v3/zip-performance', methods=['GET'])
def get_zip_performance_v3():
    """Get ZIP code performance (auto-detects Class A vs B)"""
    try:
        advertiser_id = request.args.get('advertiser_id')
        agency_id = int(request.args.get('agency_id'))
        start_date = request.args.get('start_date', '2020-01-01')
        end_date = request.args.get('end_date', '2030-12-31')
        min_visits = int(request.args.get('min_visits', '10'))
        
        agency_class = get_agency_class(agency_id)
        if not agency_class:
            return jsonify({'success': False, 'error': 'Agency not supported'}), 400
        
        conn = get_snowflake_connection()
        cursor = conn.cursor()
        
        if agency_class == 'A':
            # Class A: QRM_ALL_VISITS_V3
            query = """
                WITH visits_by_zip AS (
                    SELECT 
                        mca.ZIP_CODE,
                        COUNT(*) as S_VISITS,
                        COUNT(DISTINCT v.MAID) as UNIQUE_DEVICES
                    FROM QUORUMDB.SEGMENT_DATA.QRM_ALL_VISITS_V3 v
                    JOIN QUORUM_CROSS_CLOUD.ATTAIN_FEED.MAID_CENTROID_ASSOCIATION mca 
                        ON LOWER(v.MAID) = LOWER(mca.DEVICE_ID)
                    WHERE v.QUORUM_ADVERTISER_ID = %s
                      AND v.AGENCY_ID = %s
                      AND v.VISIT_TYPE = 'STORE'
                      AND v.CONVERSION_DATE >= %s
                      AND v.CONVERSION_DATE < %s
                    GROUP BY mca.ZIP_CODE
                    HAVING COUNT(*) >= %s
                )
                SELECT 
                    v.ZIP_CODE,
                    COALESCE(zdm.DMA_NAME, 'UNKNOWN') as DMA_NAME,
                    COALESCE(zpd.POPULATION, 0) as POPULATION,
                    v.S_VISITS,
                    v.UNIQUE_DEVICES
                FROM visits_by_zip v
                LEFT JOIN QUORUMDB.SEGMENT_DATA.ZIP_DMA_MAPPING zdm 
                    ON v.ZIP_CODE = zdm.ZIP_CODE
                LEFT JOIN QUORUMDB.SEGMENT_DATA.ZIP_POPULATION_DATA zpd 
                    ON v.ZIP_CODE = zpd.ZIP_CODE
                ORDER BY v.S_VISITS DESC
                LIMIT 500
            """
        else:
            # Class B: CAMPAIGN_PERFORMANCE_STORE_VISITS_RAW
            query = """
                WITH deduplicated AS (
                    SELECT 
                        cp.DEVICE_ID,
                        cp.DRIVE_BY_DATE,
                        cp.POI_MD5,
                        ROW_NUMBER() OVER (
                            PARTITION BY cp.DEVICE_ID, cp.DRIVE_BY_DATE, cp.POI_MD5
                            ORDER BY cp.IMP_ID DESC
                        ) as rn
                    FROM QUORUMDB.SEGMENT_DATA.CAMPAIGN_PERFORMANCE_STORE_VISITS_RAW cp
                    WHERE cp.ADVERTISER_ID = %s
                      AND cp.AGENCY_ID = %s
                      AND cp.DRIVE_BY_DATE >= %s
                      AND cp.DRIVE_BY_DATE < %s
                ),
                visits_by_zip AS (
                    SELECT 
                        mca.ZIP_CODE,
                        COUNT(*) as S_VISITS,
                        COUNT(DISTINCT d.DEVICE_ID) as UNIQUE_DEVICES
                    FROM deduplicated d
                    JOIN QUORUM_CROSS_CLOUD.ATTAIN_FEED.MAID_CENTROID_ASSOCIATION mca 
                        ON LOWER(d.DEVICE_ID) = LOWER(mca.DEVICE_ID)
                    WHERE d.rn = 1
                    GROUP BY mca.ZIP_CODE
                    HAVING COUNT(*) >= %s
                )
                SELECT 
                    v.ZIP_CODE,
                    COALESCE(zdm.DMA_NAME, 'UNKNOWN') as DMA_NAME,
                    COALESCE(zpd.POPULATION, 0) as POPULATION,
                    v.S_VISITS,
                    v.UNIQUE_DEVICES
                FROM visits_by_zip v
                LEFT JOIN QUORUMDB.SEGMENT_DATA.ZIP_DMA_MAPPING zdm 
                    ON v.ZIP_CODE = zdm.ZIP_CODE
                LEFT JOIN QUORUMDB.SEGMENT_DATA.ZIP_POPULATION_DATA zpd 
                    ON v.ZIP_CODE = zpd.ZIP_CODE
                ORDER BY v.S_VISITS DESC
                LIMIT 500
            """
        
        cursor.execute(query, (advertiser_id, agency_id, start_date, end_date, min_visits))
        results = cursor.fetchall()
        
        zips = []
        for row in results:
            zips.append({
                'ZIP_CODE': row[0],
                'DMA_NAME': row[1],
                'POPULATION': row[2],
                'S_VISITS': row[3],
                'UNIQUE_DEVICES': row[4]
            })
        
        cursor.close()
        conn.close()
        
        return jsonify({'success': True, 'data': zips})
        
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

# ============================================================================
# CAMPAIGN PERFORMANCE ENDPOINT - Dual-path
# ============================================================================

@app.route('/api/v3/campaign-performance', methods=['GET'])
def get_campaign_performance_v3():
    """Get campaign performance (auto-detects Class A vs B)"""
    try:
        advertiser_id = request.args.get('advertiser_id')
        agency_id = int(request.args.get('agency_id'))
        start_date = request.args.get('start_date', '2020-01-01')
        end_date = request.args.get('end_date', '2030-12-31')
        
        agency_class = get_agency_class(agency_id)
        if not agency_class:
            return jsonify({'success': False, 'error': 'Agency not supported'}), 400
        
        conn = get_snowflake_connection()
        cursor = conn.cursor()
        
        if agency_class == 'A':
            # Class A: QRM_ALL_VISITS_V3 (has campaign data built-in)
            query = """
                SELECT 
                    COALESCE(CAMPAIGN_ID, 0) as CAMPAIGN_ID,
                    COALESCE(CAMPAIGN_NAME, 'Unknown') as CAMPAIGN_NAME,
                    COUNT(*) as S_VISITS,
                    COUNT(DISTINCT MAID) as UNIQUE_DEVICES
                FROM QUORUMDB.SEGMENT_DATA.QRM_ALL_VISITS_V3
                WHERE QUORUM_ADVERTISER_ID = %s
                  AND AGENCY_ID = %s
                  AND VISIT_TYPE = 'STORE'
                  AND CONVERSION_DATE >= %s
                  AND CONVERSION_DATE < %s
                GROUP BY CAMPAIGN_ID, CAMPAIGN_NAME
                ORDER BY S_VISITS DESC
            """
        else:
            # Class B: Join to XANDR via IMP_ID
            query = """
                WITH deduplicated AS (
                    SELECT 
                        cp.IMP_ID,
                        cp.DEVICE_ID,
                        cp.DRIVE_BY_DATE,
                        cp.POI_MD5,
                        ROW_NUMBER() OVER (
                            PARTITION BY cp.DEVICE_ID, cp.DRIVE_BY_DATE, cp.POI_MD5
                            ORDER BY cp.IMP_ID DESC
                        ) as rn
                    FROM QUORUMDB.SEGMENT_DATA.CAMPAIGN_PERFORMANCE_STORE_VISITS_RAW cp
                    WHERE cp.ADVERTISER_ID = %s
                      AND cp.AGENCY_ID = %s
                      AND cp.DRIVE_BY_DATE >= %s
                      AND cp.DRIVE_BY_DATE < %s
                )
                SELECT 
                    COALESCE(x.IO_ID, 0) as CAMPAIGN_ID,
                    COALESCE(x.IO_NAME, 'Unknown') as CAMPAIGN_NAME,
                    COUNT(*) as S_VISITS,
                    COUNT(DISTINCT d.DEVICE_ID) as UNIQUE_DEVICES
                FROM deduplicated d
                LEFT JOIN QUORUMDB.SEGMENT_DATA.XANDR_IMPRESSION_LOG x 
                    ON d.IMP_ID = x.ID
                WHERE d.rn = 1
                GROUP BY x.IO_ID, x.IO_NAME
                ORDER BY S_VISITS DESC
            """
        
        cursor.execute(query, (advertiser_id, agency_id, start_date, end_date))
        results = cursor.fetchall()
        
        campaigns = []
        for row in results:
            campaigns.append({
                'CAMPAIGN_ID': row[0],
                'CAMPAIGN_NAME': row[1],
                'S_VISITS': row[2],
                'UNIQUE_DEVICES': row[3]
            })
        
        cursor.close()
        conn.close()
        
        return jsonify({'success': True, 'data': campaigns})
        
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

# ============================================================================
# PUBLISHER PERFORMANCE ENDPOINT - Dual-path
# ============================================================================

@app.route('/api/v3/publisher-performance', methods=['GET'])
def get_publisher_performance_v3():
    """Get publisher performance (auto-detects Class A vs B)"""
    try:
        advertiser_id = request.args.get('advertiser_id')
        agency_id = int(request.args.get('agency_id'))
        start_date = request.args.get('start_date', '2020-01-01')
        end_date = request.args.get('end_date', '2030-12-31')
        
        agency_class = get_agency_class(agency_id)
        if not agency_class:
            return jsonify({'success': False, 'error': 'Agency not supported'}), 400
        
        conn = get_snowflake_connection()
        cursor = conn.cursor()
        
        if agency_class == 'A':
            # Class A: QRM_ALL_VISITS_V3 (has publisher data built-in)
            query = """
                SELECT 
                    COALESCE(PUBLISHER, 'Unknown') as PUBLISHER,
                    COUNT(*) as S_VISITS,
                    COUNT(DISTINCT MAID) as UNIQUE_DEVICES
                FROM QUORUMDB.SEGMENT_DATA.QRM_ALL_VISITS_V3
                WHERE QUORUM_ADVERTISER_ID = %s
                  AND AGENCY_ID = %s
                  AND VISIT_TYPE = 'STORE'
                  AND CONVERSION_DATE >= %s
                  AND CONVERSION_DATE < %s
                GROUP BY PUBLISHER
                ORDER BY S_VISITS DESC
            """
        else:
            # Class B: Join to XANDR via IMP_ID
            query = """
                WITH deduplicated AS (
                    SELECT 
                        cp.IMP_ID,
                        cp.DEVICE_ID,
                        cp.DRIVE_BY_DATE,
                        cp.POI_MD5,
                        ROW_NUMBER() OVER (
                            PARTITION BY cp.DEVICE_ID, cp.DRIVE_BY_DATE, cp.POI_MD5
                            ORDER BY cp.IMP_ID DESC
                        ) as rn
                    FROM QUORUMDB.SEGMENT_DATA.CAMPAIGN_PERFORMANCE_STORE_VISITS_RAW cp
                    WHERE cp.ADVERTISER_ID = %s
                      AND cp.AGENCY_ID = %s
                      AND cp.DRIVE_BY_DATE >= %s
                      AND cp.DRIVE_BY_DATE < %s
                )
                SELECT 
                    COALESCE(x.PUBLISHER_CODE, x.SITE, CAST(x.PUBLISHER_ID AS TEXT), 'Unknown') as PUBLISHER,
                    COUNT(*) as S_VISITS,
                    COUNT(DISTINCT d.DEVICE_ID) as UNIQUE_DEVICES
                FROM deduplicated d
                LEFT JOIN QUORUMDB.SEGMENT_DATA.XANDR_IMPRESSION_LOG x 
                    ON d.IMP_ID = x.ID
                WHERE d.rn = 1
                GROUP BY PUBLISHER
                ORDER BY S_VISITS DESC
            """
        
        cursor.execute(query, (advertiser_id, agency_id, start_date, end_date))
        results = cursor.fetchall()
        
        publishers = []
        for row in results:
            publishers.append({
                'PUBLISHER': row[0],
                'S_VISITS': row[1],
                'UNIQUE_DEVICES': row[2]
            })
        
        cursor.close()
        conn.close()
        
        return jsonify({'success': True, 'data': publishers})
        
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

# ============================================================================
# HEALTH CHECK
# ============================================================================

@app.route('/api/v3/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    return jsonify({
        'success': True,
        'version': '3.0',
        'architecture': 'dual-path',
        'class_a_agencies': CLASS_A_AGENCIES,
        'class_b_agencies': CLASS_B_AGENCIES
    })

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=False)
"""
Quorum Optimizer API v3

Changes from v2:
- Added ViacomCBS WhoSay (1480) web visit support
- /api/agencies now unions store visits + web visits
- /api/advertisers detects visit type per agency
- Added VISIT_TYPE indicator (STORE, WEB, BOTH)

Attribution Logic:
- Last-touch attribution: Only the most recent impression before a conversion gets credit
- S_VISITS: Physical store visits (from QUORUM_ADV_STORE_VISITS)
- W_VISITS: Website visits (from WEB_VISITORS_TO_LOG)
- W_LEADS: Lead form submissions
- W_PURCHASES: Purchase conversions
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
# AGENCY CONFIGURATION - Which agencies use which visit type
# ============================================================================
WEB_VISIT_AGENCIES = ['1480']  # ViacomCBS WhoSay - uses WEB_VISITORS_TO_LOG
STORE_VISIT_AGENCIES = ['2514', '1950', '2234', '1813']  # MNTN, ByRider, Magnite, Causal iQ

# ============================================================================
# PLATFORM (PT) CONFIGURATION
# ============================================================================
PT_CONFIG = {
    "6": {"name": "Trade Desk", "publisher_column": "SITE", "url_decode": False},
    "8": {"name": "DV 360", "publisher_column": "PUBLISHER_ID", "url_decode": False},
    "9": {"name": "DCM/GAM", "publisher_column": "PUBLISHER_ID", "url_decode": False},
    "11": {"name": "Xandr", "publisher_column": "PUBLISHER_ID", "url_decode": False},
    "20": {"name": "Magnite/SpringServe", "publisher_column": "PUBLISHER_CODE", "url_decode": False},
    "22": {"name": "MNTN", "publisher_column": "PUBLISHER_CODE", "url_decode": True},
}

DEFAULT_PT_CONFIG = {"name": "Unknown", "publisher_column": "PUBLISHER_CODE", "url_decode": False}


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


# ============================================================================
# ENDPOINTS
# ============================================================================

@app.route('/api/agencies', methods=['GET'])
def get_agencies():
    """Get list of all agencies with store visit OR web visit data."""
    try:
        query = """
            -- Store visit agencies
            SELECT 
                sv.AGENCY_ID,
                MAX(aa.AGENCY_NAME) as AGENCY_NAME,
                COUNT(DISTINCT sv.QUORUM_ADVERTISER_ID) as ADVERTISER_COUNT,
                COUNT(*) as TOTAL_IMPRESSIONS,
                SUM(CASE WHEN sv.IS_STORE_VISIT THEN 1 ELSE 0 END) as TOTAL_VISITS,
                'STORE' as VISIT_TYPE
            FROM QUORUMDB.SEGMENT_DATA.QUORUM_ADV_STORE_VISITS sv
            LEFT JOIN QUORUMDB.SEGMENT_DATA.AGENCY_ADVERTISER aa 
                ON sv.QUORUM_ADVERTISER_ID = aa.ID
            WHERE sv.AGENCY_ID IS NOT NULL
            GROUP BY sv.AGENCY_ID
            HAVING COUNT(*) > 1000
            
            UNION ALL
            
            -- Web visit agencies (ViacomCBS WhoSay)
            SELECT 
                1480 as AGENCY_ID,
                'ViacomCBS WhoSay' as AGENCY_NAME,
                COUNT(DISTINCT wv.QUORUM_ADVERTISER_ID) as ADVERTISER_COUNT,
                COUNT(*) as TOTAL_IMPRESSIONS,
                COUNT(*) as TOTAL_VISITS,
                'WEB' as VISIT_TYPE
            FROM QUORUMDB.SEGMENT_DATA.WEB_VISITORS_TO_LOG wv
            JOIN QUORUMDB.SEGMENT_DATA.AGENCY_ADVERTISER aa 
                ON TRY_CAST(wv.QUORUM_ADVERTISER_ID AS NUMBER) = aa.ID
            WHERE aa.AGENCY_NAME = 'ViacomCBS WhoSay'
              AND wv.SITE_VISIT_TIMESTAMP >= DATEADD(day, -90, CURRENT_DATE())
            
            ORDER BY TOTAL_IMPRESSIONS DESC
        """
        results = execute_query(query)
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
        # ViacomCBS WhoSay - use WEB_VISITORS_TO_LOG
        if agency_id == '1480':
            query = """
                SELECT 
                    wv.QUORUM_ADVERTISER_ID as ADVERTISER_ID,
                    MAX(aa.COMP_NAME) as ADVERTISER_NAME,
                    COUNT(*) as TOTAL_IMPRESSIONS,
                    COUNT(*) as TOTAL_VISITS,
                    SUM(CASE WHEN wv.IS_LEAD = 'TRUE' THEN 1 ELSE 0 END) as TOTAL_LEADS,
                    SUM(CASE WHEN wv.IS_PURCHASE = 'TRUE' THEN 1 ELSE 0 END) as TOTAL_PURCHASES,
                    'WEB' as VISIT_TYPE
                FROM QUORUMDB.SEGMENT_DATA.WEB_VISITORS_TO_LOG wv
                LEFT JOIN QUORUMDB.SEGMENT_DATA.AGENCY_ADVERTISER aa 
                    ON TRY_CAST(wv.QUORUM_ADVERTISER_ID AS NUMBER) = aa.ID
                WHERE aa.AGENCY_NAME = 'ViacomCBS WhoSay'
                  AND wv.SITE_VISIT_TIMESTAMP >= DATEADD(day, -90, CURRENT_DATE())
                GROUP BY wv.QUORUM_ADVERTISER_ID
                HAVING COUNT(*) > 100
                ORDER BY TOTAL_IMPRESSIONS DESC
                LIMIT 200
            """
        else:
            # Store visit agencies
            query = f"""
                SELECT 
                    sv.QUORUM_ADVERTISER_ID as ADVERTISER_ID,
                    MAX(aa.COMP_NAME) as ADVERTISER_NAME,
                    MAX(sv.PT) as PT,
                    COUNT(*) as TOTAL_IMPRESSIONS,
                    SUM(CASE WHEN sv.IS_STORE_VISIT THEN 1 ELSE 0 END) as TOTAL_VISITS,
                    0 as TOTAL_LEADS,
                    0 as TOTAL_PURCHASES,
                    'STORE' as VISIT_TYPE
                FROM QUORUMDB.SEGMENT_DATA.QUORUM_ADV_STORE_VISITS sv
                LEFT JOIN QUORUMDB.SEGMENT_DATA.AGENCY_ADVERTISER aa 
                    ON sv.QUORUM_ADVERTISER_ID = aa.ID
                WHERE sv.AGENCY_ID = {agency_id}
                GROUP BY sv.QUORUM_ADVERTISER_ID
                HAVING COUNT(*) > 0
                ORDER BY TOTAL_IMPRESSIONS DESC
                LIMIT 100
            """
        
        results = execute_query(query)
        
        for row in results:
            pt = str(row.get('PT', ''))
            row['PT_NAME'] = PT_CONFIG.get(pt, DEFAULT_PT_CONFIG).get('name', 'Unknown')
        
        return jsonify({'success': True, 'data': results})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/advertiser-summary', methods=['GET'])
def get_advertiser_summary():
    """Get summary metrics for a specific advertiser."""
    advertiser_id = request.args.get('advertiser_id')
    agency_id = request.args.get('agency_id')
    start_date = request.args.get('start_date', '2020-01-01')
    end_date = request.args.get('end_date', '2030-12-31')
    
    if not advertiser_id:
        return jsonify({'success': False, 'error': 'advertiser_id parameter required'}), 400
    
    try:
        # ViacomCBS WhoSay - web visits
        if agency_id == '1480':
            query = f"""
                SELECT 
                    '{advertiser_id}' as QUORUM_ADVERTISER_ID,
                    MAX(aa.COMP_NAME) as ADVERTISER_NAME,
                    COUNT(*) as TOTAL_IMPRESSIONS,
                    COUNT(*) as S_VISITS,
                    SUM(CASE WHEN wv.IS_LEAD = 'TRUE' THEN 1 ELSE 0 END) as W_LEADS,
                    SUM(CASE WHEN wv.IS_PURCHASE = 'TRUE' THEN 1 ELSE 0 END) as W_PURCHASES,
                    COUNT(DISTINCT DATE_TRUNC('day', wv.SITE_VISIT_TIMESTAMP)) as ACTIVE_DAYS,
                    'WEB' as VISIT_TYPE
                FROM QUORUMDB.SEGMENT_DATA.WEB_VISITORS_TO_LOG wv
                LEFT JOIN QUORUMDB.SEGMENT_DATA.AGENCY_ADVERTISER aa 
                    ON TRY_CAST(wv.QUORUM_ADVERTISER_ID AS NUMBER) = aa.ID
                WHERE wv.QUORUM_ADVERTISER_ID = '{advertiser_id}'
                  AND wv.SITE_VISIT_TIMESTAMP >= '{start_date}'
                  AND wv.SITE_VISIT_TIMESTAMP < '{end_date}'
            """
        else:
            # Store visits
            query = f"""
                WITH last_touch AS (
                    SELECT 
                        QUORUM_ADVERTISER_ID,
                        IMP_ID,
                        IMP_TIMESTAMP,
                        IS_STORE_VISIT,
                        PT,
                        ROW_NUMBER() OVER (
                            PARTITION BY DEVICE_ID, 
                            CASE WHEN IS_STORE_VISIT THEN STORE_VISIT_TIMESTAMP ELSE IMP_TIMESTAMP END
                            ORDER BY IMP_TIMESTAMP DESC
                        ) as rn
                    FROM QUORUMDB.SEGMENT_DATA.QUORUM_ADV_STORE_VISITS
                    WHERE QUORUM_ADVERTISER_ID = {advertiser_id}
                      AND CAST(IMP_TIMESTAMP AS DATE) >= '{start_date}'
                      AND CAST(IMP_TIMESTAMP AS DATE) <= '{end_date}'
                )
                SELECT 
                    lt.QUORUM_ADVERTISER_ID,
                    MAX(aa.COMP_NAME) as ADVERTISER_NAME,
                    MAX(lt.PT) as PT,
                    COUNT(*) as TOTAL_IMPRESSIONS,
                    SUM(CASE WHEN lt.IS_STORE_VISIT AND lt.rn = 1 THEN 1 ELSE 0 END) as S_VISITS,
                    0 as W_LEADS,
                    0 as W_PURCHASES,
                    COUNT(DISTINCT CAST(lt.IMP_TIMESTAMP AS DATE)) as ACTIVE_DAYS,
                    'STORE' as VISIT_TYPE
                FROM last_touch lt
                LEFT JOIN QUORUMDB.SEGMENT_DATA.AGENCY_ADVERTISER aa 
                    ON lt.QUORUM_ADVERTISER_ID = aa.ID
                GROUP BY lt.QUORUM_ADVERTISER_ID
            """
        
        results = execute_query(query)
        
        if results:
            row = results[0]
            pt = str(row.get('PT', ''))
            row['PT_NAME'] = PT_CONFIG.get(pt, DEFAULT_PT_CONFIG).get('name', 'Unknown')
            return jsonify({'success': True, 'data': row})
        else:
            return jsonify({'success': False, 'error': 'No data found'}), 404
            
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/campaign-performance', methods=['GET'])
def get_campaign_performance():
    """Get campaign-level performance metrics."""
    advertiser_id = request.args.get('advertiser_id')
    agency_id = request.args.get('agency_id')
    start_date = request.args.get('start_date', '2020-01-01')
    end_date = request.args.get('end_date', '2030-12-31')
    
    if not advertiser_id:
        return jsonify({'success': False, 'error': 'advertiser_id parameter required'}), 400
    
    try:
        # ViacomCBS - web visits don't have campaign data in WEB_VISITORS_TO_LOG
        # Would need to join back to XANDR_IMPRESSION_LOG for campaign info
        if agency_id == '1480':
            # For now, return daily aggregates as "campaigns"
            query = f"""
                SELECT 
                    DATE_TRUNC('day', wv.SITE_VISIT_TIMESTAMP)::VARCHAR as CAMPAIGN_ID,
                    TO_CHAR(wv.SITE_VISIT_TIMESTAMP, 'YYYY-MM-DD') as CAMPAIGN_NAME,
                    COUNT(*) as IMPRESSIONS,
                    COUNT(*) as S_VISITS,
                    SUM(CASE WHEN wv.IS_LEAD = 'TRUE' THEN 1 ELSE 0 END) as W_LEADS
                FROM QUORUMDB.SEGMENT_DATA.WEB_VISITORS_TO_LOG wv
                WHERE wv.QUORUM_ADVERTISER_ID = '{advertiser_id}'
                  AND wv.SITE_VISIT_TIMESTAMP >= '{start_date}'
                  AND wv.SITE_VISIT_TIMESTAMP < '{end_date}'
                GROUP BY DATE_TRUNC('day', wv.SITE_VISIT_TIMESTAMP)
                ORDER BY CAMPAIGN_NAME DESC
                LIMIT 30
            """
        else:
            # Store visits with campaign data
            query = f"""
                WITH last_touch AS (
                    SELECT 
                        CAMPAIGN_ID,
                        CAMPAIGN_NAME,
                        IMP_ID,
                        IS_STORE_VISIT,
                        ROW_NUMBER() OVER (
                            PARTITION BY DEVICE_ID, 
                            CASE WHEN IS_STORE_VISIT THEN STORE_VISIT_TIMESTAMP ELSE IMP_TIMESTAMP END
                            ORDER BY IMP_TIMESTAMP DESC
                        ) as rn
                    FROM QUORUMDB.SEGMENT_DATA.QUORUM_ADV_STORE_VISITS
                    WHERE QUORUM_ADVERTISER_ID = {advertiser_id}
                      AND CAST(IMP_TIMESTAMP AS DATE) >= '{start_date}'
                      AND CAST(IMP_TIMESTAMP AS DATE) <= '{end_date}'
                )
                SELECT 
                    CAMPAIGN_ID,
                    MAX(CAMPAIGN_NAME) as CAMPAIGN_NAME,
                    COUNT(*) as IMPRESSIONS,
                    SUM(CASE WHEN IS_STORE_VISIT AND rn = 1 THEN 1 ELSE 0 END) as S_VISITS
                FROM last_touch
                GROUP BY CAMPAIGN_ID
                ORDER BY IMPRESSIONS DESC
                LIMIT 50
            """
        
        results = execute_query(query)
        return jsonify({'success': True, 'data': results})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/publisher-performance', methods=['GET'])
def get_publisher_performance():
    """Get publisher-level performance (store visits only for now)."""
    advertiser_id = request.args.get('advertiser_id')
    agency_id = request.args.get('agency_id')
    start_date = request.args.get('start_date', '2020-01-01')
    end_date = request.args.get('end_date', '2030-12-31')
    
    if not advertiser_id:
        return jsonify({'success': False, 'error': 'advertiser_id parameter required'}), 400
    
    try:
        # ViacomCBS - web visits
        if agency_id == '1480':
            # Traffic source from WEBPIXEL_IMPRESSION_LOG.REFERER
            query = f"""
                WITH sources AS (
                    SELECT 
                        wv.WEB_IMPRESSION_ID,
                        wv.IS_LEAD,
                        wv.IS_PURCHASE,
                        REGEXP_REPLACE(
                            REGEXP_REPLACE(pil.REFERER, '^https?://(www\\\\.)?', ''),
                            '/.*$', ''
                        ) as TRAFFIC_SOURCE
                    FROM QUORUMDB.SEGMENT_DATA.WEB_VISITORS_TO_LOG wv
                    LEFT JOIN QUORUMDB.SEGMENT_DATA.WEBPIXEL_IMPRESSION_LOG pil 
                        ON wv.WEB_IMPRESSION_ID = pil.UUID
                    WHERE wv.QUORUM_ADVERTISER_ID = '{advertiser_id}'
                      AND wv.SITE_VISIT_TIMESTAMP >= '{start_date}'
                      AND wv.SITE_VISIT_TIMESTAMP < '{end_date}'
                )
                SELECT 
                    TRAFFIC_SOURCE as PUBLISHER_CODE,
                    TRAFFIC_SOURCE as PUBLISHER_NAME,
                    COUNT(*) as IMPRESSIONS,
                    COUNT(*) as S_VISITS,
                    SUM(CASE WHEN IS_LEAD = 'TRUE' THEN 1 ELSE 0 END) as W_LEADS
                FROM sources
                WHERE TRAFFIC_SOURCE IS NOT NULL 
                  AND TRAFFIC_SOURCE != ''
                  AND LEN(TRAFFIC_SOURCE) > 3
                GROUP BY TRAFFIC_SOURCE
                HAVING COUNT(*) >= 100
                ORDER BY IMPRESSIONS DESC
                LIMIT 100
            """
        else:
            # Store visits with publisher data
            query = f"""
                WITH last_touch AS (
                    SELECT 
                        COALESCE(SITE, PUBLISHER_CODE, PUBLISHER_ID::VARCHAR) as PUBLISHER_CODE,
                        IMP_ID,
                        IS_STORE_VISIT,
                        ROW_NUMBER() OVER (
                            PARTITION BY DEVICE_ID, 
                            CASE WHEN IS_STORE_VISIT THEN STORE_VISIT_TIMESTAMP ELSE IMP_TIMESTAMP END
                            ORDER BY IMP_TIMESTAMP DESC
                        ) as rn
                    FROM QUORUMDB.SEGMENT_DATA.QUORUM_ADV_STORE_VISITS
                    WHERE QUORUM_ADVERTISER_ID = {advertiser_id}
                      AND CAST(IMP_TIMESTAMP AS DATE) >= '{start_date}'
                      AND CAST(IMP_TIMESTAMP AS DATE) <= '{end_date}'
                )
                SELECT 
                    PUBLISHER_CODE,
                    PUBLISHER_CODE as PUBLISHER_NAME,
                    COUNT(*) as IMPRESSIONS,
                    SUM(CASE WHEN IS_STORE_VISIT AND rn = 1 THEN 1 ELSE 0 END) as S_VISITS
                FROM last_touch
                WHERE PUBLISHER_CODE IS NOT NULL
                GROUP BY PUBLISHER_CODE
                HAVING COUNT(*) >= 100
                ORDER BY IMPRESSIONS DESC
                LIMIT 100
            """
        
        results = execute_query(query)
        return jsonify({'success': True, 'data': results})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/zip-performance', methods=['GET'])
def get_zip_performance():
    """Get ZIP code level performance (store visits only for now)."""
    advertiser_id = request.args.get('advertiser_id')
    agency_id = request.args.get('agency_id')
    start_date = request.args.get('start_date', '2020-01-01')
    end_date = request.args.get('end_date', '2030-12-31')
    
    if not advertiser_id:
        return jsonify({'success': False, 'error': 'advertiser_id parameter required'}), 400
    
    try:
        # ViacomCBS - web visits don't have meaningful ZIP data
        if agency_id == '1480':
            return jsonify({'success': True, 'data': [], 'message': 'ZIP data not available for web visits'})
        
        # Store visits
        query = f"""
            WITH last_touch AS (
                SELECT 
                    sv.POSTAL_CODE as ZIP_CODE,
                    sv.DMA,
                    sv.IMP_ID,
                    sv.IS_STORE_VISIT,
                    ROW_NUMBER() OVER (
                        PARTITION BY sv.DEVICE_ID, 
                        CASE WHEN sv.IS_STORE_VISIT THEN sv.STORE_VISIT_TIMESTAMP ELSE sv.IMP_TIMESTAMP END
                        ORDER BY sv.IMP_TIMESTAMP DESC
                    ) as rn
                FROM QUORUMDB.SEGMENT_DATA.QUORUM_ADV_STORE_VISITS sv
                WHERE sv.QUORUM_ADVERTISER_ID = {advertiser_id}
                  AND CAST(sv.IMP_TIMESTAMP AS DATE) >= '{start_date}'
                  AND CAST(sv.IMP_TIMESTAMP AS DATE) <= '{end_date}'
            )
            SELECT 
                lt.ZIP_CODE,
                lt.DMA as DMA_CODE,
                MAX(zdm.DMA_NAME) as DMA_NAME,
                MAX(zdm.POPULATION) as ZIP_POPULATION,
                COUNT(*) as IMPRESSIONS,
                SUM(CASE WHEN lt.IS_STORE_VISIT AND lt.rn = 1 THEN 1 ELSE 0 END) as S_VISITS
            FROM last_touch lt
            LEFT JOIN QUORUMDB.SEGMENT_DATA.ZIP_DMA_MAPPING zdm 
                ON lt.ZIP_CODE = zdm.ZIP_CODE
            WHERE lt.ZIP_CODE IS NOT NULL
              AND lt.ZIP_CODE != ''
            GROUP BY lt.ZIP_CODE, lt.DMA
            HAVING COUNT(*) >= 100
            ORDER BY IMPRESSIONS DESC
            LIMIT 500
        """
        
        results = execute_query(query)
        return jsonify({'success': True, 'data': results})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/health', methods=['GET'])
def health_check():
    return jsonify({'status': 'healthy', 'version': 'v3-web-visits'})


if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=True)
