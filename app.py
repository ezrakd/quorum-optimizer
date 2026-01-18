"""
Quorum Optimizer API
A simple Flask backend that connects to Snowflake and serves data to the frontend.

Data Sources:
- AGENCY_ADVERTISER: Agency/advertiser names and metadata
- QUORUM_ADV_STORE_VISITS: Pre-calculated impression-to-visit attribution (gold table)
- MAID_CENTROID_ASSOCIATION: User home ZIP from device ID
- ZIP_DMA_MAPPING: ZIP to DMA lookup
- ZIP_POPULATION_DATA: ZIP population data
- WEB_VISITORS_TO_LOG: Web event attribution (site visits, leads, purchases)
- WEBPIXEL_IMPRESSION_LOG: Web pixel impressions
"""

from flask import Flask, jsonify, request
from flask_cors import CORS
import snowflake.connector
import os

app = Flask(__name__)
CORS(app)  # Allow requests from any origin (for MVP)

# Snowflake connection config - uses environment variables in production
SNOWFLAKE_CONFIG = {
    'account': os.environ.get('SNOWFLAKE_ACCOUNT', 'FZB05958.us-east-1'),
    'user': os.environ.get('SNOWFLAKE_USER', 'OPTIMIZER_SERVICE_USER'),
    'password': os.environ.get('SNOWFLAKE_PASSWORD', ''),
    'warehouse': os.environ.get('SNOWFLAKE_WAREHOUSE', 'COMPUTE_WH'),
    'database': 'QUORUMDB',
    'schema': 'SEGMENT_DATA'
}

def get_snowflake_connection():
    """Create and return a Snowflake connection."""
    return snowflake.connector.connect(**SNOWFLAKE_CONFIG)

def execute_query(query, params=None):
    """Execute a query and return results as list of dicts."""
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


@app.route('/api/health', methods=['GET'])
def health_check():
    """Health check endpoint - also verifies Snowflake connection."""
    try:
        conn = get_snowflake_connection()
        conn.close()
        return jsonify({'status': 'healthy', 'snowflake': 'connected'})
    except Exception as e:
        return jsonify({'status': 'unhealthy', 'error': str(e)}), 500


# ============================================================================
# STORE VISITS MODULE (Location Visits)
# ============================================================================

@app.route('/api/agencies', methods=['GET'])
def get_agencies():
    """Get list of all agencies with their advertiser counts and total impressions."""
    try:
        query = """
            SELECT 
                sv.AGENCY_ID,
                MAX(aa.AGENCY_NAME) as AGENCY_NAME,
                COUNT(DISTINCT sv.QUORUM_ADVERTISER_ID) as ADVERTISER_COUNT,
                COUNT(*) as TOTAL_IMPRESSIONS,
                SUM(CASE WHEN sv.IS_STORE_VISIT THEN 1 ELSE 0 END) as TOTAL_VISITS
            FROM QUORUMDB.SEGMENT_DATA.QUORUM_ADV_STORE_VISITS sv
            LEFT JOIN QUORUMDB.SEGMENT_DATA.AGENCY_ADVERTISER aa 
                ON sv.QUORUM_ADVERTISER_ID = aa.ID
            WHERE sv.AGENCY_ID IS NOT NULL
            GROUP BY sv.AGENCY_ID
            HAVING COUNT(*) > 1000
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
        query = """
            SELECT 
                sv.QUORUM_ADVERTISER_ID as ADVERTISER_ID,
                MAX(aa.COMP_NAME) as ADVERTISER_NAME,
                COUNT(*) as TOTAL_IMPRESSIONS,
                SUM(CASE WHEN sv.IS_STORE_VISIT THEN 1 ELSE 0 END) as TOTAL_VISITS
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
        return jsonify({'success': True, 'data': results})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/advertiser-summary', methods=['GET'])
def get_advertiser_summary():
    """Get summary metrics for a specific advertiser.
    
    Optional params: start_date, end_date (YYYY-MM-DD format)
    """
    advertiser_id = request.args.get('advertiser_id')
    start_date = request.args.get('start_date', '2020-01-01')
    end_date = request.args.get('end_date', '2030-12-31')
    
    if not advertiser_id:
        return jsonify({'success': False, 'error': 'advertiser_id parameter required'}), 400
    
    try:
        query = """
            SELECT 
                sv.QUORUM_ADVERTISER_ID as ADVERTISER_ID,
                MAX(aa.COMP_NAME) as ADVERTISER_NAME,
                COUNT(*) as TOTAL_IMPRESSIONS,
                SUM(CASE WHEN sv.IS_STORE_VISIT THEN 1 ELSE 0 END) as TOTAL_VISITS,
                COUNT(DISTINCT sv.IO_ID) as CAMPAIGN_COUNT
            FROM QUORUMDB.SEGMENT_DATA.QUORUM_ADV_STORE_VISITS sv
            LEFT JOIN QUORUMDB.SEGMENT_DATA.AGENCY_ADVERTISER aa 
                ON sv.QUORUM_ADVERTISER_ID = aa.ID
            WHERE sv.QUORUM_ADVERTISER_ID = %s
              AND sv.IMP_TIMESTAMP >= %s
              AND sv.IMP_TIMESTAMP < %s
            GROUP BY sv.QUORUM_ADVERTISER_ID
        """
        results = execute_query(query, (advertiser_id, start_date, end_date))
        if results:
            return jsonify({'success': True, 'data': results[0]})
        else:
            return jsonify({'success': False, 'error': 'Advertiser not found'}), 404
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/zip-performance', methods=['GET'])
def get_zip_performance():
    """Get ZIP-level impression and visit data for an advertiser.
    
    Includes DMA and population data for reallocation analysis.
    
    Optional params: start_date, end_date (YYYY-MM-DD format), min_impressions (default 100)
    """
    advertiser_id = request.args.get('advertiser_id')
    start_date = request.args.get('start_date', '2020-01-01')
    end_date = request.args.get('end_date', '2030-12-31')
    min_impressions = request.args.get('min_impressions', '100')
    
    if not advertiser_id:
        return jsonify({'success': False, 'error': 'advertiser_id parameter required'}), 400
    
    try:
        query = """
            SELECT 
                mca.ZIP_CODE,
                zdm.DMA_CODE,
                zdm.DMA_NAME,
                zpd.POPULATION,
                COUNT(*) as IMPRESSIONS,
                SUM(CASE WHEN sv.IS_STORE_VISIT THEN 1 ELSE 0 END) as VISITS
            FROM QUORUMDB.SEGMENT_DATA.QUORUM_ADV_STORE_VISITS sv
            JOIN QUORUM_CROSS_CLOUD.ATTAIN_FEED.MAID_CENTROID_ASSOCIATION mca 
                ON LOWER(sv.IMP_MAID) = LOWER(mca.DEVICE_ID)
            LEFT JOIN QUORUMDB.SEGMENT_DATA.ZIP_DMA_MAPPING zdm
                ON mca.ZIP_CODE = zdm.ZIP_CODE
            LEFT JOIN QUORUMDB.SEGMENT_DATA.ZIP_POPULATION_DATA zpd
                ON mca.ZIP_CODE = zpd.ZIP_CODE
            WHERE sv.QUORUM_ADVERTISER_ID = %s
              AND mca.ZIP_CODE IS NOT NULL
              AND mca.ZIP_CODE != ''
              AND sv.IMP_TIMESTAMP >= %s
              AND sv.IMP_TIMESTAMP < %s
            GROUP BY mca.ZIP_CODE, zdm.DMA_CODE, zdm.DMA_NAME, zpd.POPULATION
            HAVING COUNT(*) >= %s
            ORDER BY IMPRESSIONS DESC
            LIMIT 1000
        """
        results = execute_query(query, (advertiser_id, start_date, end_date, min_impressions))
        return jsonify({'success': True, 'data': results})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/campaign-performance', methods=['GET'])
def get_campaign_performance():
    """Get campaign-level performance for an advertiser.
    
    Optional params: start_date, end_date (YYYY-MM-DD format)
    """
    advertiser_id = request.args.get('advertiser_id')
    start_date = request.args.get('start_date', '2020-01-01')
    end_date = request.args.get('end_date', '2030-12-31')
    
    if not advertiser_id:
        return jsonify({'success': False, 'error': 'advertiser_id parameter required'}), 400
    
    try:
        query = """
            SELECT 
                sv.IO_ID as CAMPAIGN_ID,
                MAX(sv.IO_NAME) as CAMPAIGN_NAME,
                COUNT(*) as IMPRESSIONS,
                SUM(CASE WHEN sv.IS_STORE_VISIT THEN 1 ELSE 0 END) as VISITS
            FROM QUORUMDB.SEGMENT_DATA.QUORUM_ADV_STORE_VISITS sv
            WHERE sv.QUORUM_ADVERTISER_ID = %s
              AND sv.IO_ID IS NOT NULL
              AND sv.IMP_TIMESTAMP >= %s
              AND sv.IMP_TIMESTAMP < %s
            GROUP BY sv.IO_ID
            HAVING COUNT(*) > 0
            ORDER BY IMPRESSIONS DESC
            LIMIT 50
        """
        results = execute_query(query, (advertiser_id, start_date, end_date))
        return jsonify({'success': True, 'data': results})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/publisher-performance', methods=['GET'])
def get_publisher_performance():
    """Get publisher-level performance for an advertiser.
    
    Optional params: start_date, end_date (YYYY-MM-DD format)
    """
    advertiser_id = request.args.get('advertiser_id')
    start_date = request.args.get('start_date', '2020-01-01')
    end_date = request.args.get('end_date', '2030-12-31')
    
    if not advertiser_id:
        return jsonify({'success': False, 'error': 'advertiser_id parameter required'}), 400
    
    try:
        query = """
            SELECT 
                sv.PUBLISHER_ID,
                sv.PUBLISHER_CODE,
                COUNT(*) as IMPRESSIONS,
                SUM(CASE WHEN sv.IS_STORE_VISIT THEN 1 ELSE 0 END) as VISITS
            FROM QUORUMDB.SEGMENT_DATA.QUORUM_ADV_STORE_VISITS sv
            WHERE sv.QUORUM_ADVERTISER_ID = %s
              AND sv.IMP_TIMESTAMP >= %s
              AND sv.IMP_TIMESTAMP < %s
            GROUP BY sv.PUBLISHER_ID, sv.PUBLISHER_CODE
            HAVING COUNT(*) >= 100
            ORDER BY IMPRESSIONS DESC
            LIMIT 100
        """
        results = execute_query(query, (advertiser_id, start_date, end_date))
        return jsonify({'success': True, 'data': results})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


# ============================================================================
# WEB EVENTS MODULE
# ============================================================================

@app.route('/api/web/advertisers', methods=['GET'])
def get_web_advertisers():
    """Get advertisers with web event data.
    
    Note: ViacomCBS WhoSay (Agency 1480) represents ~95% of web pixel advertisers.
    """
    try:
        query = """
            SELECT 
                wv.QUORUM_ADVERTISER_ID as ADVERTISER_ID,
                MAX(aa.COMP_NAME) as ADVERTISER_NAME,
                MAX(aa.AGENCY_NAME) as AGENCY_NAME,
                COUNT(*) as TOTAL_IMPRESSIONS,
                SUM(CASE WHEN wv.IS_SITE_VISIT = 'TRUE' THEN 1 ELSE 0 END) as SITE_VISITS,
                SUM(CASE WHEN wv.IS_LEAD = 'TRUE' THEN 1 ELSE 0 END) as LEADS,
                SUM(CASE WHEN wv.IS_PURCHASE = 'TRUE' THEN 1 ELSE 0 END) as PURCHASES
            FROM QUORUMDB.SEGMENT_DATA.WEB_VISITORS_TO_LOG wv
            LEFT JOIN QUORUMDB.SEGMENT_DATA.AGENCY_ADVERTISER aa 
                ON wv.QUORUM_ADVERTISER_ID = aa.ID
            GROUP BY wv.QUORUM_ADVERTISER_ID
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
    """Get web event summary for a specific advertiser.
    
    Optional params: start_date, end_date (YYYY-MM-DD format)
    """
    advertiser_id = request.args.get('advertiser_id')
    start_date = request.args.get('start_date', '2020-01-01')
    end_date = request.args.get('end_date', '2030-12-31')
    
    if not advertiser_id:
        return jsonify({'success': False, 'error': 'advertiser_id parameter required'}), 400
    
    try:
        query = """
            SELECT 
                wv.QUORUM_ADVERTISER_ID as ADVERTISER_ID,
                MAX(aa.COMP_NAME) as ADVERTISER_NAME,
                COUNT(*) as TOTAL_IMPRESSIONS,
                SUM(CASE WHEN wv.IS_SITE_VISIT = 'TRUE' THEN 1 ELSE 0 END) as SITE_VISITS,
                SUM(CASE WHEN wv.IS_LEAD = 'TRUE' THEN 1 ELSE 0 END) as LEADS,
                SUM(CASE WHEN wv.IS_PURCHASE = 'TRUE' THEN 1 ELSE 0 END) as PURCHASES,
                SUM(CASE WHEN wv.PURCHASE_VALUE IS NOT NULL AND wv.PURCHASE_VALUE != '' 
                    THEN TRY_CAST(wv.PURCHASE_VALUE AS FLOAT) ELSE 0 END) as TOTAL_PURCHASE_VALUE
            FROM QUORUMDB.SEGMENT_DATA.WEB_VISITORS_TO_LOG wv
            LEFT JOIN QUORUMDB.SEGMENT_DATA.AGENCY_ADVERTISER aa 
                ON wv.QUORUM_ADVERTISER_ID = aa.ID
            WHERE wv.QUORUM_ADVERTISER_ID = %s
              AND wv.SITE_VISIT_TIMESTAMP >= %s
              AND wv.SITE_VISIT_TIMESTAMP < %s
            GROUP BY wv.QUORUM_ADVERTISER_ID
        """
        results = execute_query(query, (advertiser_id, start_date, end_date))
        if results:
            return jsonify({'success': True, 'data': results[0]})
        else:
            return jsonify({'success': False, 'error': 'Advertiser not found'}), 404
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/web/zip-performance', methods=['GET'])
def get_web_zip_performance():
    """Get ZIP-level web event data for an advertiser.
    
    Optional params: start_date, end_date (YYYY-MM-DD format), min_impressions (default 100)
    """
    advertiser_id = request.args.get('advertiser_id')
    start_date = request.args.get('start_date', '2020-01-01')
    end_date = request.args.get('end_date', '2030-12-31')
    min_impressions = request.args.get('min_impressions', '100')
    
    if not advertiser_id:
        return jsonify({'success': False, 'error': 'advertiser_id parameter required'}), 400
    
    try:
        query = """
            SELECT 
                mca.ZIP_CODE,
                zdm.DMA_CODE,
                zdm.DMA_NAME,
                zpd.POPULATION,
                COUNT(*) as IMPRESSIONS,
                SUM(CASE WHEN wv.IS_SITE_VISIT = 'TRUE' THEN 1 ELSE 0 END) as SITE_VISITS,
                SUM(CASE WHEN wv.IS_LEAD = 'TRUE' THEN 1 ELSE 0 END) as LEADS,
                SUM(CASE WHEN wv.IS_PURCHASE = 'TRUE' THEN 1 ELSE 0 END) as PURCHASES
            FROM QUORUMDB.SEGMENT_DATA.WEB_VISITORS_TO_LOG wv
            JOIN QUORUM_CROSS_CLOUD.ATTAIN_FEED.MAID_CENTROID_ASSOCIATION mca 
                ON LOWER(wv.MAID) = LOWER(mca.DEVICE_ID)
            LEFT JOIN QUORUMDB.SEGMENT_DATA.ZIP_DMA_MAPPING zdm
                ON mca.ZIP_CODE = zdm.ZIP_CODE
            LEFT JOIN QUORUMDB.SEGMENT_DATA.ZIP_POPULATION_DATA zpd
                ON mca.ZIP_CODE = zpd.ZIP_CODE
            WHERE wv.QUORUM_ADVERTISER_ID = %s
              AND mca.ZIP_CODE IS NOT NULL
              AND mca.ZIP_CODE != ''
              AND wv.SITE_VISIT_TIMESTAMP >= %s
              AND wv.SITE_VISIT_TIMESTAMP < %s
            GROUP BY mca.ZIP_CODE, zdm.DMA_CODE, zdm.DMA_NAME, zpd.POPULATION
            HAVING COUNT(*) >= %s
            ORDER BY IMPRESSIONS DESC
            LIMIT 1000
        """
        results = execute_query(query, (advertiser_id, start_date, end_date, min_impressions))
        return jsonify({'success': True, 'data': results})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=True)
