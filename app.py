"""
Quorum Optimizer API v4 - Unified Architecture
Single source of truth using QRM_ALL_VISITS_V4 + QRM_DIMENSION_STATS

Changes from v3:
- No more dual-path (Class A/B) - all data in QRM_ALL_VISITS_V4
- ZIP/Publisher/Creative use pre-aggregated QRM_DIMENSION_STATS
- User home ZIP enrichment built into V4
- Campaign/Line Item dropdown filters for dimension reports
"""

import os
from flask import Flask, request, jsonify
from flask_cors import CORS
import snowflake.connector
from datetime import datetime

app = Flask(__name__)
CORS(app)

# =============================================================================
# DATABASE CONNECTION
# =============================================================================

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
    """Execute a query and return results as list of dicts"""
    conn = get_snowflake_connection()
    cursor = conn.cursor()
    try:
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
        cursor.close()
        conn.close()


# =============================================================================
# HEALTH CHECK
# =============================================================================

@app.route('/api/v4/health', methods=['GET'])
@app.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    try:
        # Quick query to verify V4 tables exist
        results = execute_query("""
            SELECT 
                'QRM_ALL_VISITS_V4' as tbl,
                COUNT(*) as rows,
                MAX(CONVERSION_DATE) as latest_date
            FROM QUORUMDB.SEGMENT_DATA.QRM_ALL_VISITS_V4
            UNION ALL
            SELECT 
                'QRM_DIMENSION_STATS' as tbl,
                COUNT(*) as rows,
                NULL as latest_date
            FROM QUORUMDB.SEGMENT_DATA.QRM_DIMENSION_STATS
        """)
        return jsonify({
            'success': True,
            'status': 'healthy',
            'version': 'v4',
            'tables': results
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'status': 'error',
            'error': str(e)
        }), 500


# =============================================================================
# AGENCIES ENDPOINT
# =============================================================================

@app.route('/api/v4/agencies', methods=['GET'])
@app.route('/api/agencies', methods=['GET'])
def get_agencies():
    """Get all agencies with visit counts"""
    try:
        query = """
            SELECT 
                v.AGENCY_ID,
                aa.AGENCY_NAME,
                v.DATA_CLASS as AGENCY_CLASS,
                COUNT(*) as TOTAL_VISITS,
                COUNT(DISTINCT v.QUORUM_ADVERTISER_ID) as ADVERTISER_COUNT,
                SUM(CASE WHEN v.VISIT_TYPE = 'STORE' THEN 1 ELSE 0 END) as STORE_VISITS,
                SUM(CASE WHEN v.VISIT_TYPE = 'WEB' THEN 1 ELSE 0 END) as WEB_VISITS
            FROM QUORUMDB.SEGMENT_DATA.QRM_ALL_VISITS_V4 v
            LEFT JOIN QUORUMDB.SEGMENT_DATA.AGENCY_ADVERTISER aa 
                ON v.AGENCY_ID = aa.ADVERTISER_ID
            WHERE v.AGENCY_ID IS NOT NULL
            GROUP BY v.AGENCY_ID, aa.AGENCY_NAME, v.DATA_CLASS
            ORDER BY TOTAL_VISITS DESC
        """
        results = execute_query(query)
        return jsonify({'success': True, 'data': results})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


# =============================================================================
# ADVERTISERS ENDPOINT
# =============================================================================

@app.route('/api/v4/advertisers', methods=['GET'])
@app.route('/api/advertisers', methods=['GET'])
def get_advertisers():
    """Get advertisers for an agency"""
    try:
        agency_id = request.args.get('agency_id')
        if not agency_id:
            return jsonify({'success': False, 'error': 'agency_id required'}), 400
        
        query = """
            SELECT 
                v.QUORUM_ADVERTISER_ID as ADVERTISER_ID,
                aa.COMP_NAME as ADVERTISER_NAME,
                COUNT(*) as TOTAL_VISITS,
                SUM(CASE WHEN v.VISIT_TYPE = 'STORE' THEN 1 ELSE 0 END) as STORE_VISITS,
                SUM(CASE WHEN v.VISIT_TYPE = 'WEB' THEN 1 ELSE 0 END) as WEB_VISITS,
                SUM(CASE WHEN v.IS_LEAD = 1 THEN 1 ELSE 0 END) as LEADS,
                SUM(CASE WHEN v.IS_PURCHASE = 1 THEN 1 ELSE 0 END) as PURCHASES
            FROM QUORUMDB.SEGMENT_DATA.QRM_ALL_VISITS_V4 v
            LEFT JOIN QUORUMDB.SEGMENT_DATA.AGENCY_ADVERTISER aa 
                ON v.QUORUM_ADVERTISER_ID = CAST(aa.ID AS VARCHAR)
            WHERE v.AGENCY_ID = %s
            GROUP BY v.QUORUM_ADVERTISER_ID, aa.COMP_NAME
            ORDER BY TOTAL_VISITS DESC
        """
        results = execute_query(query, (agency_id,))
        return jsonify({'success': True, 'data': results})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


# =============================================================================
# ADVERTISER SUMMARY ENDPOINT
# =============================================================================

@app.route('/api/v4/advertiser-summary', methods=['GET'])
@app.route('/api/advertiser-summary', methods=['GET'])
def get_advertiser_summary():
    """Get summary metrics for an advertiser with date filtering"""
    try:
        advertiser_id = request.args.get('advertiser_id')
        start_date = request.args.get('start_date', '2020-01-01')
        end_date = request.args.get('end_date', '2030-12-31')
        
        if not advertiser_id:
            return jsonify({'success': False, 'error': 'advertiser_id required'}), 400
        
        query = """
            SELECT 
                COUNT(*) as TOTAL_VISITS,
                COUNT(DISTINCT MAID) as UNIQUE_DEVICES,
                SUM(CASE WHEN VISIT_TYPE = 'STORE' THEN 1 ELSE 0 END) as STORE_VISITS,
                SUM(CASE WHEN VISIT_TYPE = 'WEB' THEN 1 ELSE 0 END) as WEB_VISITS,
                SUM(CASE WHEN IS_LEAD = 1 THEN 1 ELSE 0 END) as LEADS,
                SUM(CASE WHEN IS_PURCHASE = 1 THEN 1 ELSE 0 END) as PURCHASES,
                COUNT(DISTINCT USER_HOME_ZIP) as UNIQUE_ZIPS,
                COUNT(DISTINCT USER_HOME_DMA) as UNIQUE_DMAS,
                MIN(CONVERSION_DATE) as EARLIEST_DATE,
                MAX(CONVERSION_DATE) as LATEST_DATE
            FROM QUORUMDB.SEGMENT_DATA.QRM_ALL_VISITS_V4
            WHERE QUORUM_ADVERTISER_ID = %s
              AND CONVERSION_DATE >= %s
              AND CONVERSION_DATE <= %s
        """
        results = execute_query(query, (advertiser_id, start_date, end_date))
        
        if results and results[0]:
            data = results[0]
            # Convert dates to strings
            data['EARLIEST_DATE'] = str(data['EARLIEST_DATE']) if data['EARLIEST_DATE'] else start_date
            data['LATEST_DATE'] = str(data['LATEST_DATE']) if data['LATEST_DATE'] else end_date
            return jsonify({'success': True, 'data': data})
        else:
            return jsonify({'success': True, 'data': {}})
            
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


# =============================================================================
# CAMPAIGN PERFORMANCE ENDPOINT (Date-filterable)
# =============================================================================

@app.route('/api/v4/campaign-performance', methods=['GET'])
@app.route('/api/campaign-performance', methods=['GET'])
def get_campaign_performance():
    """Get campaign performance with date filtering"""
    try:
        advertiser_id = request.args.get('advertiser_id')
        start_date = request.args.get('start_date', '2020-01-01')
        end_date = request.args.get('end_date', '2030-12-31')
        
        if not advertiser_id:
            return jsonify({'success': False, 'error': 'advertiser_id required'}), 400
        
        query = """
            SELECT 
                COALESCE(CAMPAIGN_ID, 0) as CAMPAIGN_ID,
                COALESCE(CAMPAIGN_NAME, 'Unknown') as CAMPAIGN_NAME,
                COUNT(*) as TOTAL_VISITS,
                SUM(CASE WHEN VISIT_TYPE = 'STORE' THEN 1 ELSE 0 END) as STORE_VISITS,
                SUM(CASE WHEN VISIT_TYPE = 'WEB' THEN 1 ELSE 0 END) as WEB_VISITS,
                COUNT(DISTINCT MAID) as UNIQUE_DEVICES,
                SUM(CASE WHEN IS_LEAD = 1 THEN 1 ELSE 0 END) as LEADS,
                SUM(CASE WHEN IS_PURCHASE = 1 THEN 1 ELSE 0 END) as PURCHASES,
                COUNT(DISTINCT USER_HOME_ZIP) as UNIQUE_ZIPS
            FROM QUORUMDB.SEGMENT_DATA.QRM_ALL_VISITS_V4
            WHERE QUORUM_ADVERTISER_ID = %s
              AND CONVERSION_DATE >= %s
              AND CONVERSION_DATE <= %s
            GROUP BY CAMPAIGN_ID, CAMPAIGN_NAME
            ORDER BY TOTAL_VISITS DESC
        """
        results = execute_query(query, (advertiser_id, start_date, end_date))
        return jsonify({'success': True, 'data': results})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


# =============================================================================
# LINE ITEM PERFORMANCE ENDPOINT (Date-filterable)
# =============================================================================

@app.route('/api/v4/lineitem-performance', methods=['GET'])
@app.route('/api/lineitem-performance', methods=['GET'])
def get_lineitem_performance():
    """Get line item performance with date filtering"""
    try:
        advertiser_id = request.args.get('advertiser_id')
        campaign_id = request.args.get('campaign_id')  # Optional filter
        start_date = request.args.get('start_date', '2020-01-01')
        end_date = request.args.get('end_date', '2030-12-31')
        
        if not advertiser_id:
            return jsonify({'success': False, 'error': 'advertiser_id required'}), 400
        
        # Base query
        query = """
            SELECT 
                COALESCE(CAMPAIGN_ID, 0) as CAMPAIGN_ID,
                COALESCE(CAMPAIGN_NAME, 'Unknown') as CAMPAIGN_NAME,
                COALESCE(LINEITEM_ID, '0') as LINEITEM_ID,
                COALESCE(LINEITEM_NAME, 'Unknown') as LINEITEM_NAME,
                COUNT(*) as TOTAL_VISITS,
                SUM(CASE WHEN VISIT_TYPE = 'STORE' THEN 1 ELSE 0 END) as STORE_VISITS,
                SUM(CASE WHEN VISIT_TYPE = 'WEB' THEN 1 ELSE 0 END) as WEB_VISITS,
                COUNT(DISTINCT MAID) as UNIQUE_DEVICES,
                SUM(CASE WHEN IS_LEAD = 1 THEN 1 ELSE 0 END) as LEADS,
                SUM(CASE WHEN IS_PURCHASE = 1 THEN 1 ELSE 0 END) as PURCHASES
            FROM QUORUMDB.SEGMENT_DATA.QRM_ALL_VISITS_V4
            WHERE QUORUM_ADVERTISER_ID = %s
              AND CONVERSION_DATE >= %s
              AND CONVERSION_DATE <= %s
        """
        params = [advertiser_id, start_date, end_date]
        
        # Optional campaign filter
        if campaign_id:
            query += " AND CAMPAIGN_ID = %s"
            params.append(campaign_id)
        
        query += """
            GROUP BY CAMPAIGN_ID, CAMPAIGN_NAME, LINEITEM_ID, LINEITEM_NAME
            ORDER BY TOTAL_VISITS DESC
        """
        
        results = execute_query(query, tuple(params))
        return jsonify({'success': True, 'data': results})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


# =============================================================================
# ZIP PERFORMANCE ENDPOINT (Pre-aggregated, All-time with dropdown filters)
# =============================================================================

@app.route('/api/v4/zip-performance', methods=['GET'])
@app.route('/api/zip-performance', methods=['GET'])
def get_zip_performance():
    """
    Get ZIP code performance from pre-aggregated stats.
    Supports filtering by campaign_id or lineitem_id via dropdown.
    """
    try:
        advertiser_id = request.args.get('advertiser_id')
        campaign_id = request.args.get('campaign_id')  # Optional dropdown
        lineitem_id = request.args.get('lineitem_id')  # Optional dropdown
        limit = int(request.args.get('limit', 500))
        
        if not advertiser_id:
            return jsonify({'success': False, 'error': 'advertiser_id required'}), 400
        
        # Determine dimension type based on filters
        if lineitem_id:
            dimension_type = 'LINEITEM_ZIP'
            filter_col = 'LINEITEM_ID'
            filter_val = lineitem_id
        elif campaign_id:
            dimension_type = 'CAMPAIGN_ZIP'
            filter_col = 'CAMPAIGN_ID'
            filter_val = campaign_id
        else:
            dimension_type = 'ZIP'
            filter_col = None
            filter_val = None
        
        query = """
            SELECT 
                DIMENSION_VALUE as ZIP_CODE,
                DMA as DMA_NAME,
                CENSUS_BLOCK,
                VISITS as TOTAL_VISITS,
                STORE_VISITS,
                WEB_VISITS,
                LEADS,
                PURCHASES,
                UNIQUE_DEVICES
            FROM QUORUMDB.SEGMENT_DATA.QRM_DIMENSION_STATS
            WHERE QUORUM_ADVERTISER_ID = %s
              AND DIMENSION_TYPE = %s
        """
        params = [advertiser_id, dimension_type]
        
        if filter_col and filter_val:
            query += f" AND {filter_col} = %s"
            params.append(filter_val)
        
        query += f" ORDER BY TOTAL_VISITS DESC LIMIT {limit}"
        
        results = execute_query(query, tuple(params))
        
        return jsonify({
            'success': True, 
            'data': results,
            'dimension_type': dimension_type,
            'note': 'All-time aggregated data (1K+ visits threshold)'
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


# =============================================================================
# PUBLISHER PERFORMANCE ENDPOINT (Pre-aggregated, All-time with dropdown filters)
# =============================================================================

@app.route('/api/v4/publisher-performance', methods=['GET'])
@app.route('/api/publisher-performance', methods=['GET'])
def get_publisher_performance():
    """
    Get publisher performance from pre-aggregated stats.
    Supports filtering by campaign_id or lineitem_id via dropdown.
    """
    try:
        advertiser_id = request.args.get('advertiser_id')
        campaign_id = request.args.get('campaign_id')  # Optional dropdown
        lineitem_id = request.args.get('lineitem_id')  # Optional dropdown
        limit = int(request.args.get('limit', 500))
        
        if not advertiser_id:
            return jsonify({'success': False, 'error': 'advertiser_id required'}), 400
        
        # Determine dimension type based on filters
        if lineitem_id:
            dimension_type = 'LINEITEM_PUBLISHER'
            filter_col = 'LINEITEM_ID'
            filter_val = lineitem_id
        elif campaign_id:
            dimension_type = 'CAMPAIGN_PUBLISHER'
            filter_col = 'CAMPAIGN_ID'
            filter_val = campaign_id
        else:
            dimension_type = 'PUBLISHER'
            filter_col = None
            filter_val = None
        
        query = """
            SELECT 
                DIMENSION_VALUE as PUBLISHER,
                VISITS as TOTAL_VISITS,
                STORE_VISITS,
                WEB_VISITS,
                LEADS,
                PURCHASES,
                UNIQUE_DEVICES
            FROM QUORUMDB.SEGMENT_DATA.QRM_DIMENSION_STATS
            WHERE QUORUM_ADVERTISER_ID = %s
              AND DIMENSION_TYPE = %s
        """
        params = [advertiser_id, dimension_type]
        
        if filter_col and filter_val:
            query += f" AND {filter_col} = %s"
            params.append(filter_val)
        
        query += f" ORDER BY TOTAL_VISITS DESC LIMIT {limit}"
        
        results = execute_query(query, tuple(params))
        
        return jsonify({
            'success': True, 
            'data': results,
            'dimension_type': dimension_type,
            'note': 'All-time aggregated data (1K+ visits threshold)'
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


# =============================================================================
# CREATIVE PERFORMANCE ENDPOINT (Pre-aggregated, All-time with dropdown filters)
# =============================================================================

@app.route('/api/v4/creative-performance', methods=['GET'])
@app.route('/api/creative-performance', methods=['GET'])
def get_creative_performance():
    """
    Get creative performance from pre-aggregated stats.
    Supports filtering by campaign_id or lineitem_id via dropdown.
    """
    try:
        advertiser_id = request.args.get('advertiser_id')
        campaign_id = request.args.get('campaign_id')  # Optional dropdown
        lineitem_id = request.args.get('lineitem_id')  # Optional dropdown
        limit = int(request.args.get('limit', 500))
        
        if not advertiser_id:
            return jsonify({'success': False, 'error': 'advertiser_id required'}), 400
        
        # Determine dimension type based on filters
        if lineitem_id:
            dimension_type = 'LINEITEM_CREATIVE'
            filter_col = 'LINEITEM_ID'
            filter_val = lineitem_id
        elif campaign_id:
            dimension_type = 'CAMPAIGN_CREATIVE'
            filter_col = 'CAMPAIGN_ID'
            filter_val = campaign_id
        else:
            dimension_type = 'CREATIVE'
            filter_col = None
            filter_val = None
        
        query = """
            SELECT 
                DIMENSION_VALUE as CREATIVE,
                VISITS as TOTAL_VISITS,
                STORE_VISITS,
                WEB_VISITS,
                LEADS,
                PURCHASES,
                UNIQUE_DEVICES
            FROM QUORUMDB.SEGMENT_DATA.QRM_DIMENSION_STATS
            WHERE QUORUM_ADVERTISER_ID = %s
              AND DIMENSION_TYPE = %s
        """
        params = [advertiser_id, dimension_type]
        
        if filter_col and filter_val:
            query += f" AND {filter_col} = %s"
            params.append(filter_val)
        
        query += f" ORDER BY TOTAL_VISITS DESC LIMIT {limit}"
        
        results = execute_query(query, tuple(params))
        
        return jsonify({
            'success': True, 
            'data': results,
            'dimension_type': dimension_type,
            'note': 'All-time aggregated data (1K+ visits threshold)'
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


# =============================================================================
# DMA × PUBLISHER COMBO ENDPOINT
# =============================================================================

@app.route('/api/v4/dma-publisher', methods=['GET'])
def get_dma_publisher():
    """Get DMA × Publisher combo analysis"""
    try:
        advertiser_id = request.args.get('advertiser_id')
        limit = int(request.args.get('limit', 500))
        
        if not advertiser_id:
            return jsonify({'success': False, 'error': 'advertiser_id required'}), 400
        
        query = """
            SELECT 
                DIMENSION_VALUE as DMA,
                DIMENSION_VALUE_2 as PUBLISHER,
                VISITS as TOTAL_VISITS,
                STORE_VISITS,
                WEB_VISITS,
                UNIQUE_DEVICES
            FROM QUORUMDB.SEGMENT_DATA.QRM_DIMENSION_STATS
            WHERE QUORUM_ADVERTISER_ID = %s
              AND DIMENSION_TYPE = 'DMA_PUBLISHER'
            ORDER BY TOTAL_VISITS DESC
            LIMIT %s
        """
        results = execute_query(query, (advertiser_id, limit))
        return jsonify({'success': True, 'data': results})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


# =============================================================================
# PUBLISHER × CREATIVE COMBO ENDPOINT
# =============================================================================

@app.route('/api/v4/publisher-creative', methods=['GET'])
def get_publisher_creative():
    """Get Publisher × Creative combo analysis"""
    try:
        advertiser_id = request.args.get('advertiser_id')
        limit = int(request.args.get('limit', 500))
        
        if not advertiser_id:
            return jsonify({'success': False, 'error': 'advertiser_id required'}), 400
        
        query = """
            SELECT 
                DIMENSION_VALUE as PUBLISHER,
                DIMENSION_VALUE_2 as CREATIVE,
                VISITS as TOTAL_VISITS,
                STORE_VISITS,
                WEB_VISITS,
                UNIQUE_DEVICES
            FROM QUORUMDB.SEGMENT_DATA.QRM_DIMENSION_STATS
            WHERE QUORUM_ADVERTISER_ID = %s
              AND DIMENSION_TYPE = 'PUBLISHER_CREATIVE'
            ORDER BY TOTAL_VISITS DESC
            LIMIT %s
        """
        results = execute_query(query, (advertiser_id, limit))
        return jsonify({'success': True, 'data': results})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


# =============================================================================
# DROPDOWN OPTIONS ENDPOINTS (For populating Campaign/Line Item dropdowns)
# =============================================================================

@app.route('/api/v4/campaigns-list', methods=['GET'])
def get_campaigns_list():
    """Get list of campaigns for dropdown (from V4 table)"""
    try:
        advertiser_id = request.args.get('advertiser_id')
        if not advertiser_id:
            return jsonify({'success': False, 'error': 'advertiser_id required'}), 400
        
        query = """
            SELECT DISTINCT
                CAMPAIGN_ID,
                CAMPAIGN_NAME
            FROM QUORUMDB.SEGMENT_DATA.QRM_ALL_VISITS_V4
            WHERE QUORUM_ADVERTISER_ID = %s
              AND CAMPAIGN_ID IS NOT NULL
            ORDER BY CAMPAIGN_NAME
        """
        results = execute_query(query, (advertiser_id,))
        return jsonify({'success': True, 'data': results})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/v4/lineitems-list', methods=['GET'])
def get_lineitems_list():
    """Get list of line items for dropdown (from V4 table)"""
    try:
        advertiser_id = request.args.get('advertiser_id')
        campaign_id = request.args.get('campaign_id')  # Optional filter
        
        if not advertiser_id:
            return jsonify({'success': False, 'error': 'advertiser_id required'}), 400
        
        query = """
            SELECT DISTINCT
                CAMPAIGN_ID,
                CAMPAIGN_NAME,
                LINEITEM_ID,
                LINEITEM_NAME
            FROM QUORUMDB.SEGMENT_DATA.QRM_ALL_VISITS_V4
            WHERE QUORUM_ADVERTISER_ID = %s
              AND LINEITEM_ID IS NOT NULL
        """
        params = [advertiser_id]
        
        if campaign_id:
            query += " AND CAMPAIGN_ID = %s"
            params.append(campaign_id)
        
        query += " ORDER BY CAMPAIGN_NAME, LINEITEM_NAME"
        
        results = execute_query(query, tuple(params))
        return jsonify({'success': True, 'data': results})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


# =============================================================================
# DAILY TREND ENDPOINT (Date-filterable from V4)
# =============================================================================

@app.route('/api/v4/daily-trend', methods=['GET'])
def get_daily_trend():
    """Get daily visit trend with date filtering"""
    try:
        advertiser_id = request.args.get('advertiser_id')
        campaign_id = request.args.get('campaign_id')  # Optional
        start_date = request.args.get('start_date', '2020-01-01')
        end_date = request.args.get('end_date', '2030-12-31')
        
        if not advertiser_id:
            return jsonify({'success': False, 'error': 'advertiser_id required'}), 400
        
        query = """
            SELECT 
                CONVERSION_DATE as DATE,
                COUNT(*) as TOTAL_VISITS,
                SUM(CASE WHEN VISIT_TYPE = 'STORE' THEN 1 ELSE 0 END) as STORE_VISITS,
                SUM(CASE WHEN VISIT_TYPE = 'WEB' THEN 1 ELSE 0 END) as WEB_VISITS,
                COUNT(DISTINCT MAID) as UNIQUE_DEVICES
            FROM QUORUMDB.SEGMENT_DATA.QRM_ALL_VISITS_V4
            WHERE QUORUM_ADVERTISER_ID = %s
              AND CONVERSION_DATE >= %s
              AND CONVERSION_DATE <= %s
        """
        params = [advertiser_id, start_date, end_date]
        
        if campaign_id:
            query += " AND CAMPAIGN_ID = %s"
            params.append(campaign_id)
        
        query += " GROUP BY CONVERSION_DATE ORDER BY CONVERSION_DATE"
        
        results = execute_query(query, tuple(params))
        
        # Convert dates to strings
        for row in results:
            row['DATE'] = str(row['DATE'])
        
        return jsonify({'success': True, 'data': results})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


# =============================================================================
# DMA PERFORMANCE ENDPOINT (Aggregated from V4)
# =============================================================================

@app.route('/api/v4/dma-performance', methods=['GET'])
def get_dma_performance():
    """Get DMA-level performance from user home DMA"""
    try:
        advertiser_id = request.args.get('advertiser_id')
        start_date = request.args.get('start_date', '2020-01-01')
        end_date = request.args.get('end_date', '2030-12-31')
        
        if not advertiser_id:
            return jsonify({'success': False, 'error': 'advertiser_id required'}), 400
        
        query = """
            SELECT 
                COALESCE(USER_HOME_DMA, 'Unknown') as DMA,
                COUNT(*) as TOTAL_VISITS,
                SUM(CASE WHEN VISIT_TYPE = 'STORE' THEN 1 ELSE 0 END) as STORE_VISITS,
                SUM(CASE WHEN VISIT_TYPE = 'WEB' THEN 1 ELSE 0 END) as WEB_VISITS,
                COUNT(DISTINCT MAID) as UNIQUE_DEVICES,
                COUNT(DISTINCT USER_HOME_ZIP) as UNIQUE_ZIPS
            FROM QUORUMDB.SEGMENT_DATA.QRM_ALL_VISITS_V4
            WHERE QUORUM_ADVERTISER_ID = %s
              AND CONVERSION_DATE >= %s
              AND CONVERSION_DATE <= %s
            GROUP BY USER_HOME_DMA
            ORDER BY TOTAL_VISITS DESC
        """
        results = execute_query(query, (advertiser_id, start_date, end_date))
        return jsonify({'success': True, 'data': results})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


# =============================================================================
# BACKWARD COMPATIBILITY - V3 ROUTES (Redirect to V4 handlers)
# =============================================================================

@app.route('/api/v3/agencies', methods=['GET'])
def get_agencies_v3():
    return get_agencies()

@app.route('/api/v3/advertisers', methods=['GET'])
def get_advertisers_v3():
    return get_advertisers()

@app.route('/api/v3/advertiser-summary', methods=['GET'])
def get_advertiser_summary_v3():
    return get_advertiser_summary()

@app.route('/api/v3/campaign-performance', methods=['GET'])
def get_campaign_performance_v3():
    return get_campaign_performance()

@app.route('/api/v3/publisher-performance', methods=['GET'])
def get_publisher_performance_v3():
    return get_publisher_performance()

@app.route('/api/v3/zip-performance', methods=['GET'])
def get_zip_performance_v3():
    return get_zip_performance()

@app.route('/api/v3/health', methods=['GET'])
def health_check_v3():
    return health_check()


# =============================================================================
# MAIN
# =============================================================================

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=True)
