"""
Quorum Optimizer API v4 - Unified Single-Source Architecture
Uses QRM_ALL_VISITS_V4 (93.9M rows) and QRM_DIMENSION_STATS (pre-aggregated)
No more dual-path queries - all agency classes unified
"""

import os
from flask import Flask, request, jsonify
from flask_cors import CORS
import snowflake.connector
from datetime import datetime

app = Flask(__name__)
CORS(app)

# ============================================================================
# SNOWFLAKE CONNECTION
# ============================================================================

def get_snowflake_connection():
    return snowflake.connector.connect(
        user=os.environ.get('SNOWFLAKE_USER', 'OPTIMIZER_SERVICE_USER'),
        password=os.environ.get('SNOWFLAKE_PASSWORD'),
        account=os.environ.get('SNOWFLAKE_ACCOUNT', 'FZB05958.us-east-1'),
        warehouse=os.environ.get('SNOWFLAKE_WAREHOUSE', 'COMPUTE_WH'),
        database='QUORUMDB',
        schema='SEGMENT_DATA'
    )

def execute_query(query, params=None):
    """Execute query and return results as list of dicts"""
    conn = get_snowflake_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(query, params or {})
        columns = [col[0] for col in cursor.description]
        results = []
        for row in cursor.fetchall():
            results.append(dict(zip(columns, row)))
        return results
    finally:
        conn.close()

def success_response(data):
    return jsonify({'success': True, 'data': data})

def error_response(message, status=400):
    return jsonify({'success': False, 'error': message}), status

# ============================================================================
# AGENCIES & ADVERTISERS
# ============================================================================

@app.route('/api/v4/agencies', methods=['GET'])
def get_agencies():
    """Get all agencies with visit counts from V4"""
    query = """
        SELECT 
            v.AGENCY_ID,
            aa.AGENCY_NAME,
            v.DATA_CLASS as AGENCY_CLASS,
            COUNT(*) as TOTAL_VISITS,
            COUNT(DISTINCT v.QUORUM_ADVERTISER_ID) as ADVERTISER_COUNT
        FROM QUORUMDB.SEGMENT_DATA.QRM_ALL_VISITS_V4 v
        JOIN QUORUMDB.SEGMENT_DATA.AGENCY_ADVERTISER aa 
            ON v.AGENCY_ID = aa.ADVERTISER_ID
        WHERE v.AGENCY_ID IS NOT NULL
        GROUP BY v.AGENCY_ID, aa.AGENCY_NAME, v.DATA_CLASS
        ORDER BY TOTAL_VISITS DESC
    """
    try:
        results = execute_query(query)
        return success_response(results)
    except Exception as e:
        return error_response(str(e))

@app.route('/api/v4/advertisers', methods=['GET'])
def get_advertisers():
    """Get advertisers for an agency"""
    agency_id = request.args.get('agency_id')
    if not agency_id:
        return error_response('agency_id required')
    
    query = """
        SELECT 
            v.QUORUM_ADVERTISER_ID as ADVERTISER_ID,
            aa.AGENCY_NAME as ADVERTISER_NAME,
            COUNT(*) as TOTAL_VISITS,
            SUM(CASE WHEN v.VISIT_TYPE = 'STORE' THEN 1 ELSE 0 END) as STORE_VISITS,
            SUM(CASE WHEN v.VISIT_TYPE = 'WEB' THEN 1 ELSE 0 END) as WEB_VISITS
        FROM QUORUMDB.SEGMENT_DATA.QRM_ALL_VISITS_V4 v
        LEFT JOIN QUORUMDB.SEGMENT_DATA.AGENCY_ADVERTISER aa 
            ON v.QUORUM_ADVERTISER_ID = CAST(aa.ADVERTISER_ID AS VARCHAR)
        WHERE v.AGENCY_ID = %(agency_id)s
        GROUP BY v.QUORUM_ADVERTISER_ID, aa.AGENCY_NAME
        ORDER BY TOTAL_VISITS DESC
    """
    try:
        results = execute_query(query, {'agency_id': int(agency_id)})
        return success_response(results)
    except Exception as e:
        return error_response(str(e))

# ============================================================================
# DROPDOWN LISTS (for filter dropdowns)
# ============================================================================

@app.route('/api/v4/campaigns-list', methods=['GET'])
def get_campaigns_list():
    """Get campaign list for dropdown"""
    advertiser_id = request.args.get('advertiser_id')
    if not advertiser_id:
        return error_response('advertiser_id required')
    
    query = """
        SELECT DISTINCT
            CAMPAIGN_ID,
            CAMPAIGN_NAME,
            COUNT(*) as VISITS
        FROM QUORUMDB.SEGMENT_DATA.QRM_ALL_VISITS_V4
        WHERE QUORUM_ADVERTISER_ID = %(advertiser_id)s
          AND CAMPAIGN_ID IS NOT NULL
        GROUP BY CAMPAIGN_ID, CAMPAIGN_NAME
        ORDER BY VISITS DESC
    """
    try:
        results = execute_query(query, {'advertiser_id': advertiser_id})
        return success_response(results)
    except Exception as e:
        return error_response(str(e))

@app.route('/api/v4/lineitems-list', methods=['GET'])
def get_lineitems_list():
    """Get line item list for dropdown"""
    advertiser_id = request.args.get('advertiser_id')
    if not advertiser_id:
        return error_response('advertiser_id required')
    
    query = """
        SELECT DISTINCT
            LINEITEM_ID,
            LINEITEM_NAME,
            CAMPAIGN_ID,
            CAMPAIGN_NAME,
            COUNT(*) as VISITS
        FROM QUORUMDB.SEGMENT_DATA.QRM_ALL_VISITS_V4
        WHERE QUORUM_ADVERTISER_ID = %(advertiser_id)s
          AND LINEITEM_ID IS NOT NULL
        GROUP BY LINEITEM_ID, LINEITEM_NAME, CAMPAIGN_ID, CAMPAIGN_NAME
        ORDER BY VISITS DESC
    """
    try:
        results = execute_query(query, {'advertiser_id': advertiser_id})
        return success_response(results)
    except Exception as e:
        return error_response(str(e))

# ============================================================================
# ADVERTISER SUMMARY (date-filtered)
# ============================================================================

@app.route('/api/v4/advertiser-summary', methods=['GET'])
def get_advertiser_summary():
    """Get summary stats for an advertiser"""
    advertiser_id = request.args.get('advertiser_id')
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    
    if not advertiser_id:
        return error_response('advertiser_id required')
    
    date_filter = ""
    if start_date and end_date:
        date_filter = "AND CONVERSION_DATE BETWEEN %(start_date)s AND %(end_date)s"
    
    query = f"""
        SELECT 
            COUNT(*) as TOTAL_VISITS,
            SUM(CASE WHEN VISIT_TYPE = 'STORE' THEN 1 ELSE 0 END) as STORE_VISITS,
            SUM(CASE WHEN VISIT_TYPE = 'WEB' THEN 1 ELSE 0 END) as WEB_VISITS,
            SUM(CASE WHEN IS_LEAD = 1 THEN 1 ELSE 0 END) as LEADS,
            SUM(CASE WHEN IS_PURCHASE = 1 THEN 1 ELSE 0 END) as PURCHASES,
            COUNT(DISTINCT MAID) as UNIQUE_DEVICES,
            COUNT(DISTINCT USER_HOME_ZIP) as UNIQUE_ZIPS,
            COUNT(DISTINCT USER_HOME_DMA) as UNIQUE_DMAS
        FROM QUORUMDB.SEGMENT_DATA.QRM_ALL_VISITS_V4
        WHERE QUORUM_ADVERTISER_ID = %(advertiser_id)s
        {date_filter}
    """
    try:
        results = execute_query(query, {
            'advertiser_id': advertiser_id,
            'start_date': start_date,
            'end_date': end_date
        })
        return success_response(results[0] if results else {})
    except Exception as e:
        return error_response(str(e))

# ============================================================================
# CAMPAIGN PERFORMANCE (date-filtered)
# ============================================================================

@app.route('/api/v4/campaign-performance', methods=['GET'])
def get_campaign_performance():
    """Get campaign performance with date filter"""
    advertiser_id = request.args.get('advertiser_id')
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    
    if not advertiser_id:
        return error_response('advertiser_id required')
    
    date_filter = ""
    if start_date and end_date:
        date_filter = "AND CONVERSION_DATE BETWEEN %(start_date)s AND %(end_date)s"
    
    query = f"""
        SELECT 
            CAMPAIGN_ID,
            CAMPAIGN_NAME,
            COUNT(*) as TOTAL_VISITS,
            SUM(CASE WHEN VISIT_TYPE = 'STORE' THEN 1 ELSE 0 END) as STORE_VISITS,
            SUM(CASE WHEN VISIT_TYPE = 'WEB' THEN 1 ELSE 0 END) as WEB_VISITS,
            SUM(CASE WHEN IS_LEAD = 1 THEN 1 ELSE 0 END) as LEADS,
            SUM(CASE WHEN IS_PURCHASE = 1 THEN 1 ELSE 0 END) as PURCHASES,
            COUNT(DISTINCT MAID) as UNIQUE_DEVICES
        FROM QUORUMDB.SEGMENT_DATA.QRM_ALL_VISITS_V4
        WHERE QUORUM_ADVERTISER_ID = %(advertiser_id)s
          AND CAMPAIGN_ID IS NOT NULL
        {date_filter}
        GROUP BY CAMPAIGN_ID, CAMPAIGN_NAME
        ORDER BY TOTAL_VISITS DESC
    """
    try:
        results = execute_query(query, {
            'advertiser_id': advertiser_id,
            'start_date': start_date,
            'end_date': end_date
        })
        return success_response(results)
    except Exception as e:
        return error_response(str(e))

# ============================================================================
# LINE ITEM PERFORMANCE (date-filtered, campaign-filterable)
# ============================================================================

@app.route('/api/v4/lineitem-performance', methods=['GET'])
def get_lineitem_performance():
    """Get line item performance"""
    advertiser_id = request.args.get('advertiser_id')
    campaign_id = request.args.get('campaign_id')
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    
    if not advertiser_id:
        return error_response('advertiser_id required')
    
    filters = ["QUORUM_ADVERTISER_ID = %(advertiser_id)s", "LINEITEM_ID IS NOT NULL"]
    if campaign_id:
        filters.append("CAMPAIGN_ID = %(campaign_id)s")
    if start_date and end_date:
        filters.append("CONVERSION_DATE BETWEEN %(start_date)s AND %(end_date)s")
    
    where_clause = " AND ".join(filters)
    
    query = f"""
        SELECT 
            LINEITEM_ID,
            LINEITEM_NAME,
            CAMPAIGN_ID,
            CAMPAIGN_NAME,
            COUNT(*) as TOTAL_VISITS,
            SUM(CASE WHEN VISIT_TYPE = 'STORE' THEN 1 ELSE 0 END) as STORE_VISITS,
            SUM(CASE WHEN VISIT_TYPE = 'WEB' THEN 1 ELSE 0 END) as WEB_VISITS,
            SUM(CASE WHEN IS_LEAD = 1 THEN 1 ELSE 0 END) as LEADS,
            SUM(CASE WHEN IS_PURCHASE = 1 THEN 1 ELSE 0 END) as PURCHASES,
            COUNT(DISTINCT MAID) as UNIQUE_DEVICES
        FROM QUORUMDB.SEGMENT_DATA.QRM_ALL_VISITS_V4
        WHERE {where_clause}
        GROUP BY LINEITEM_ID, LINEITEM_NAME, CAMPAIGN_ID, CAMPAIGN_NAME
        ORDER BY TOTAL_VISITS DESC
    """
    try:
        results = execute_query(query, {
            'advertiser_id': advertiser_id,
            'campaign_id': campaign_id,
            'start_date': start_date,
            'end_date': end_date
        })
        return success_response(results)
    except Exception as e:
        return error_response(str(e))

# ============================================================================
# DMA PERFORMANCE (date-filtered)
# ============================================================================

@app.route('/api/v4/dma-performance', methods=['GET'])
def get_dma_performance():
    """Get DMA performance with date filter"""
    advertiser_id = request.args.get('advertiser_id')
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    
    if not advertiser_id:
        return error_response('advertiser_id required')
    
    date_filter = ""
    if start_date and end_date:
        date_filter = "AND CONVERSION_DATE BETWEEN %(start_date)s AND %(end_date)s"
    
    query = f"""
        SELECT 
            USER_HOME_DMA as DMA,
            COUNT(*) as TOTAL_VISITS,
            SUM(CASE WHEN VISIT_TYPE = 'STORE' THEN 1 ELSE 0 END) as STORE_VISITS,
            SUM(CASE WHEN VISIT_TYPE = 'WEB' THEN 1 ELSE 0 END) as WEB_VISITS,
            COUNT(DISTINCT MAID) as UNIQUE_DEVICES,
            COUNT(DISTINCT USER_HOME_ZIP) as UNIQUE_ZIPS
        FROM QUORUMDB.SEGMENT_DATA.QRM_ALL_VISITS_V4
        WHERE QUORUM_ADVERTISER_ID = %(advertiser_id)s
          AND USER_HOME_DMA IS NOT NULL
        {date_filter}
        GROUP BY USER_HOME_DMA
        ORDER BY TOTAL_VISITS DESC
    """
    try:
        results = execute_query(query, {
            'advertiser_id': advertiser_id,
            'start_date': start_date,
            'end_date': end_date
        })
        return success_response(results)
    except Exception as e:
        return error_response(str(e))

# ============================================================================
# DIMENSION STATS ENDPOINTS (pre-aggregated, all-time, with filters)
# ============================================================================

def get_dimension_data(advertiser_id, dimension_type, campaign_id=None, lineitem_id=None):
    """Generic function to get dimension stats with optional filters"""
    
    # Determine which dimension type to use based on filters
    if lineitem_id:
        dim_type = f'LINEITEM_{dimension_type}'
        filter_col = 'LINEITEM_ID'
        filter_val = lineitem_id
    elif campaign_id:
        dim_type = f'CAMPAIGN_{dimension_type}'
        filter_col = 'CAMPAIGN_ID'
        filter_val = campaign_id
    else:
        dim_type = dimension_type
        filter_col = None
        filter_val = None
    
    filters = [
        "QUORUM_ADVERTISER_ID = %(advertiser_id)s",
        "DIMENSION_TYPE = %(dimension_type)s"
    ]
    params = {'advertiser_id': advertiser_id, 'dimension_type': dim_type}
    
    if filter_col and filter_val:
        filters.append(f"{filter_col} = %(filter_val)s")
        params['filter_val'] = filter_val
    
    where_clause = " AND ".join(filters)
    
    query = f"""
        SELECT 
            DIMENSION_VALUE,
            DIMENSION_VALUE_2,
            DMA,
            VISITS as TOTAL_VISITS,
            STORE_VISITS,
            WEB_VISITS,
            LEADS,
            PURCHASES,
            UNIQUE_DEVICES,
            CAMPAIGN_ID,
            CAMPAIGN_NAME,
            LINEITEM_ID,
            LINEITEM_NAME
        FROM QUORUMDB.SEGMENT_DATA.QRM_DIMENSION_STATS
        WHERE {where_clause}
        ORDER BY TOTAL_VISITS DESC
    """
    return execute_query(query, params)

@app.route('/api/v4/zip-performance', methods=['GET'])
def get_zip_performance():
    """Get ZIP performance (pre-aggregated, all-time, 1K+ threshold)"""
    advertiser_id = request.args.get('advertiser_id')
    campaign_id = request.args.get('campaign_id')
    lineitem_id = request.args.get('lineitem_id')
    
    if not advertiser_id:
        return error_response('advertiser_id required')
    
    try:
        results = get_dimension_data(advertiser_id, 'ZIP', campaign_id, lineitem_id)
        # Rename DIMENSION_VALUE to ZIP_CODE, DMA to DMA_NAME
        for r in results:
            r['ZIP_CODE'] = r.pop('DIMENSION_VALUE', None)
            r['DMA_NAME'] = r.get('DMA')
        return success_response(results)
    except Exception as e:
        return error_response(str(e))

@app.route('/api/v4/publisher-performance', methods=['GET'])
def get_publisher_performance():
    """Get publisher performance (pre-aggregated, all-time, 1K+ threshold)"""
    advertiser_id = request.args.get('advertiser_id')
    campaign_id = request.args.get('campaign_id')
    lineitem_id = request.args.get('lineitem_id')
    
    if not advertiser_id:
        return error_response('advertiser_id required')
    
    try:
        results = get_dimension_data(advertiser_id, 'PUBLISHER', campaign_id, lineitem_id)
        for r in results:
            r['PUBLISHER'] = r.pop('DIMENSION_VALUE', None)
        return success_response(results)
    except Exception as e:
        return error_response(str(e))

@app.route('/api/v4/creative-performance', methods=['GET'])
def get_creative_performance():
    """Get creative performance (pre-aggregated, all-time, 1K+ threshold)"""
    advertiser_id = request.args.get('advertiser_id')
    campaign_id = request.args.get('campaign_id')
    lineitem_id = request.args.get('lineitem_id')
    
    if not advertiser_id:
        return error_response('advertiser_id required')
    
    try:
        results = get_dimension_data(advertiser_id, 'CREATIVE', campaign_id, lineitem_id)
        for r in results:
            r['CREATIVE'] = r.pop('DIMENSION_VALUE', None)
        return success_response(results)
    except Exception as e:
        return error_response(str(e))

# ============================================================================
# COMBO DIMENSIONS (pre-aggregated, all-time)
# ============================================================================

@app.route('/api/v4/dma-publisher', methods=['GET'])
def get_dma_publisher():
    """Get DMA × Publisher combo performance"""
    advertiser_id = request.args.get('advertiser_id')
    
    if not advertiser_id:
        return error_response('advertiser_id required')
    
    query = """
        SELECT 
            DIMENSION_VALUE as DMA,
            DIMENSION_VALUE_2 as PUBLISHER,
            VISITS as TOTAL_VISITS,
            STORE_VISITS,
            WEB_VISITS,
            UNIQUE_DEVICES
        FROM QUORUMDB.SEGMENT_DATA.QRM_DIMENSION_STATS
        WHERE QUORUM_ADVERTISER_ID = %(advertiser_id)s
          AND DIMENSION_TYPE = 'DMA_PUBLISHER'
        ORDER BY TOTAL_VISITS DESC
    """
    try:
        results = execute_query(query, {'advertiser_id': advertiser_id})
        return success_response(results)
    except Exception as e:
        return error_response(str(e))

@app.route('/api/v4/publisher-creative', methods=['GET'])
def get_publisher_creative():
    """Get Publisher × Creative combo performance"""
    advertiser_id = request.args.get('advertiser_id')
    
    if not advertiser_id:
        return error_response('advertiser_id required')
    
    query = """
        SELECT 
            DIMENSION_VALUE as PUBLISHER,
            DIMENSION_VALUE_2 as CREATIVE,
            VISITS as TOTAL_VISITS,
            STORE_VISITS,
            WEB_VISITS,
            UNIQUE_DEVICES
        FROM QUORUMDB.SEGMENT_DATA.QRM_DIMENSION_STATS
        WHERE QUORUM_ADVERTISER_ID = %(advertiser_id)s
          AND DIMENSION_TYPE = 'PUBLISHER_CREATIVE'
        ORDER BY TOTAL_VISITS DESC
    """
    try:
        results = execute_query(query, {'advertiser_id': advertiser_id})
        return success_response(results)
    except Exception as e:
        return error_response(str(e))

# ============================================================================
# DAILY TREND (date-filtered)
# ============================================================================

@app.route('/api/v4/daily-trend', methods=['GET'])
def get_daily_trend():
    """Get daily visit trend"""
    advertiser_id = request.args.get('advertiser_id')
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    
    if not advertiser_id:
        return error_response('advertiser_id required')
    
    date_filter = ""
    if start_date and end_date:
        date_filter = "AND CONVERSION_DATE BETWEEN %(start_date)s AND %(end_date)s"
    
    query = f"""
        SELECT 
            CONVERSION_DATE as DATE,
            COUNT(*) as TOTAL_VISITS,
            SUM(CASE WHEN VISIT_TYPE = 'STORE' THEN 1 ELSE 0 END) as STORE_VISITS,
            SUM(CASE WHEN VISIT_TYPE = 'WEB' THEN 1 ELSE 0 END) as WEB_VISITS,
            COUNT(DISTINCT MAID) as UNIQUE_DEVICES
        FROM QUORUMDB.SEGMENT_DATA.QRM_ALL_VISITS_V4
        WHERE QUORUM_ADVERTISER_ID = %(advertiser_id)s
        {date_filter}
        GROUP BY CONVERSION_DATE
        ORDER BY CONVERSION_DATE
    """
    try:
        results = execute_query(query, {
            'advertiser_id': advertiser_id,
            'start_date': start_date,
            'end_date': end_date
        })
        # Convert dates to strings
        for r in results:
            if r.get('DATE'):
                r['DATE'] = str(r['DATE'])
        return success_response(results)
    except Exception as e:
        return error_response(str(e))

# ============================================================================
# BACKWARD COMPATIBILITY - V3 ROUTES
# ============================================================================

@app.route('/api/v3/agencies', methods=['GET'])
def get_agencies_v3():
    return get_agencies()

@app.route('/api/v3/advertisers', methods=['GET'])
def get_advertisers_v3():
    return get_advertisers()

@app.route('/api/v3/advertiser-summary', methods=['GET'])
def get_summary_v3():
    return get_advertiser_summary()

@app.route('/api/v3/campaign-performance', methods=['GET'])
def get_campaigns_v3():
    return get_campaign_performance()

@app.route('/api/v3/zip-performance', methods=['GET'])
def get_zip_v3():
    return get_zip_performance()

@app.route('/api/v3/publisher-performance', methods=['GET'])
def get_publisher_v3():
    return get_publisher_performance()

# ============================================================================
# HEALTH CHECK
# ============================================================================

@app.route('/health', methods=['GET'])
def health_check():
    return jsonify({
        'status': 'healthy',
        'version': 'v4',
        'tables': ['QRM_ALL_VISITS_V4', 'QRM_DIMENSION_STATS']
    })

@app.route('/', methods=['GET'])
def root():
    return jsonify({
        'service': 'Quorum Optimizer API',
        'version': 'v4',
        'endpoints': {
            'agencies': '/api/v4/agencies',
            'advertisers': '/api/v4/advertisers?agency_id=',
            'summary': '/api/v4/advertiser-summary?advertiser_id=&start_date=&end_date=',
            'campaigns': '/api/v4/campaign-performance?advertiser_id=',
            'lineitems': '/api/v4/lineitem-performance?advertiser_id=',
            'zip': '/api/v4/zip-performance?advertiser_id=',
            'publisher': '/api/v4/publisher-performance?advertiser_id=',
            'creative': '/api/v4/creative-performance?advertiser_id=',
            'dma': '/api/v4/dma-performance?advertiser_id=',
            'dma_publisher': '/api/v4/dma-publisher?advertiser_id=',
            'publisher_creative': '/api/v4/publisher-creative?advertiser_id=',
            'daily_trend': '/api/v4/daily-trend?advertiser_id='
        }
    })

# ============================================================================
# MAIN
# ============================================================================

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=False)
