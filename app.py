"""
Quorum Optimizer API
A simple Flask backend that connects to Snowflake and serves data to the frontend.
"""

from flask import Flask, jsonify, request
from flask_cors import CORS
import snowflake.connector
import os

app = Flask(__name__)
CORS(app)  # Allow requests from any origin (for MVP)

# Snowflake connection config - uses environment variables in production
SNOWFLAKE_CONFIG = {
    'account': os.environ.get('SNOWFLAKE_ACCOUNT', 'FZB05958'),
    'user': os.environ.get('SNOWFLAKE_USER', 'OPTIMIZER_SERVICE_USER'),
    'password': os.environ.get('SNOWFLAKE_PASSWORD', 'G3tpaid$2026!'),
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


@app.route('/api/agencies', methods=['GET'])
def get_agencies():
    """Get list of all agencies with their advertiser counts and total impressions."""
    try:
        query = """
            SELECT 
                AGENCY_ID,
                AGENCY_NAME,
                COUNT(DISTINCT ADVERTISER_ID) as ADVERTISER_COUNT,
                SUM(IMPRESSIONS) as TOTAL_IMPRESSIONS
            FROM CAMPAIGN_POSTAL_REPORTING
            WHERE AGENCY_NAME IS NOT NULL AND AGENCY_NAME != ''
            GROUP BY AGENCY_ID, AGENCY_NAME
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
                ADVERTISER_ID,
                ADVERTISER_NAME,
                SUM(IMPRESSIONS) as TOTAL_IMPRESSIONS,
                SUM(STORE_VISITS) as TOTAL_VISITS
            FROM CAMPAIGN_POSTAL_REPORTING
            WHERE AGENCY_ID = %s
            GROUP BY ADVERTISER_ID, ADVERTISER_NAME
            HAVING SUM(IMPRESSIONS) > 0
            ORDER BY TOTAL_IMPRESSIONS DESC
            LIMIT 50
        """
        results = execute_query(query, (agency_id,))
        return jsonify({'success': True, 'data': results})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/advertiser-summary', methods=['GET'])
def get_advertiser_summary():
    """Get summary metrics for a specific advertiser."""
    advertiser_id = request.args.get('advertiser_id')
    if not advertiser_id:
        return jsonify({'success': False, 'error': 'advertiser_id parameter required'}), 400
    
    try:
        query = """
            SELECT 
                ADVERTISER_ID,
                ADVERTISER_NAME,
                SUM(IMPRESSIONS) as TOTAL_IMPRESSIONS,
                SUM(STORE_VISITS) as TOTAL_VISITS,
                COUNT(DISTINCT USER_HOME_POSTAL_CODE) as ZIP_COUNT,
                COUNT(DISTINCT CAMPAIGN_ID) as CAMPAIGN_COUNT
            FROM CAMPAIGN_POSTAL_REPORTING
            WHERE ADVERTISER_ID = %s
            GROUP BY ADVERTISER_ID, ADVERTISER_NAME
        """
        results = execute_query(query, (advertiser_id,))
        if results:
            return jsonify({'success': True, 'data': results[0]})
        else:
            return jsonify({'success': False, 'error': 'Advertiser not found'}), 404
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=True)
