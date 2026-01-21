"""
Quorum Optimizer API v3.1
==========================

Version: 3.1 (Production Ready)
Updated: January 20, 2026

Architecture: Two-table system
- QRM_ALL_VISITS_V3: Conversions (numerator)
- XANDR_IMPRESSION_LOG: Impressions (denominator)

Deployment: Railway (github.com/ezrakd/quorum-optimizer)
"""

import os
import json
from urllib.parse import unquote
from flask import Flask, request, jsonify
from flask_cors import CORS
import snowflake.connector
from datetime import datetime, timedelta

# =============================================================================
# FLASK APP INITIALIZATION
# =============================================================================

app = Flask(__name__)
CORS(app)


# =============================================================================
# DATABASE HELPERS
# =============================================================================

def get_snowflake_connection():
    """Create Snowflake connection from environment variables"""
    return snowflake.connector.connect(
        account=os.environ.get('SNOWFLAKE_ACCOUNT', 'FZB05958.us-east-1'),
        user=os.environ.get('SNOWFLAKE_USER', 'OPTIMIZER_SERVICE_USER'),
        password=os.environ.get('SNOWFLAKE_PASSWORD'),
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
            row_dict = {}
            for i, val in enumerate(row):
                # Convert to JSON-serializable types
                if hasattr(val, 'isoformat'):
                    row_dict[columns[i]] = val.isoformat()
                elif val is None:
                    row_dict[columns[i]] = None
                else:
                    row_dict[columns[i]] = val
            results.append(row_dict)
        return results
    finally:
        conn.close()


def get_date_params(request):
    """Extract and validate date parameters (default: last 30 days)"""
    default_end = datetime.now().strftime('%Y-%m-%d')
    default_start = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
    
    start_date = request.args.get('start_date', default_start)
    end_date = request.args.get('end_date', default_end)
    
    return start_date, end_date


# =============================================================================
# HEALTH CHECK
# =============================================================================

@app.route('/api/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    try:
        conn = get_snowflake_connection()
        conn.close()
        return jsonify({
            'success': True, 
            'status': 'healthy', 
            'version': '3.1',
            'architecture': 'QRM_ALL_VISITS_V3 + XANDR_IMPRESSION_LOG'
        })
    except Exception as e:
        return jsonify({'success': False, 'status': 'unhealthy', 'error': str(e)}), 500


# =============================================================================
# V2 ENDPOINTS (Backward Compatible - Store Visits Only)
# =============================================================================

@app.route('/api/agencies', methods=['GET'])
def get_agencies():
    """List agencies with store visit counts"""
    start_date, end_date = get_date_params(request)
    
    query = """
    SELECT 
        v.AGENCY_ID,
        MAX(aa.AGENCY_NAME) as AGENCY_NAME,
        COUNT(*) as S_VISITS,
        COUNT(DISTINCT v.QUORUM_ADVERTISER_ID) as ADVERTISER_COUNT
    FROM QUORUMDB.SEGMENT_DATA.QRM_ALL_VISITS_V3 v
    LEFT JOIN QUORUMDB.SEGMENT_DATA.AGENCY_ADVERTISER aa 
        ON TRY_CAST(v.QUORUM_ADVERTISER_ID AS NUMBER) = aa.ID
    WHERE v.CONVERSION_DATE >= %(start_date)s
      AND v.CONVERSION_DATE <= %(end_date)s
      AND v.VISIT_TYPE = 'STORE'
      AND v.AGENCY_ID IS NOT NULL
    GROUP BY v.AGENCY_ID
    HAVING COUNT(*) > 0
    ORDER BY COUNT(*) DESC
    """
    
    try:
        results = execute_query(query, {'start_date': start_date, 'end_date': end_date})
        return jsonify({'success': True, 'data': results})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/advertisers', methods=['GET'])
def get_advertisers():
    """List advertisers for an agency (store visits)"""
    agency_id = request.args.get('agency_id')
    start_date, end_date = get_date_params(request)
    
    if not agency_id:
        return jsonify({'success': False, 'error': 'agency_id required'}), 400
    
    query = """
    SELECT 
        v.QUORUM_ADVERTISER_ID as ADVERTISER_ID,
        MAX(aa.COMP_NAME) as ADVERTISER_NAME,
        COUNT(*) as S_VISITS,
        COUNT(DISTINCT v.MAID) as UNIQUE_VISITORS
    FROM QUORUMDB.SEGMENT_DATA.QRM_ALL_VISITS_V3 v
    LEFT JOIN QUORUMDB.SEGMENT_DATA.AGENCY_ADVERTISER aa 
        ON TRY_CAST(v.QUORUM_ADVERTISER_ID AS NUMBER) = aa.ID
    WHERE v.AGENCY_ID = %(agency_id)s
      AND v.CONVERSION_DATE >= %(start_date)s
      AND v.CONVERSION_DATE <= %(end_date)s
      AND v.VISIT_TYPE = 'STORE'
    GROUP BY v.QUORUM_ADVERTISER_ID
    ORDER BY COUNT(*) DESC
    """
    
    try:
        results = execute_query(query, {
            'agency_id': int(agency_id),
            'start_date': start_date,
            'end_date': end_date
        })
        return jsonify({'success': True, 'data': results})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/advertiser-summary', methods=['GET'])
def get_advertiser_summary():
    """Get store visit summary WITH IMPRESSIONS (campaign-level join)"""
    advertiser_id = request.args.get('advertiser_id')
    start_date, end_date = get_date_params(request)
    
    if not advertiser_id:
        return jsonify({'success': False, 'error': 'advertiser_id required'}), 400
    
    query = """
    WITH visit_campaigns AS (
        SELECT 
            AGENCY_ID,
            CAMPAIGN_ID,
            COUNT(*) as S_VISITS,
            COUNT(DISTINCT MAID) as UNIQUE_VISITORS
        FROM QUORUMDB.SEGMENT_DATA.QRM_ALL_VISITS_V3
        WHERE QUORUM_ADVERTISER_ID = %(advertiser_id)s
          AND CONVERSION_DATE >= %(start_date)s
          AND CONVERSION_DATE <= %(end_date)s
          AND VISIT_TYPE = 'STORE'
          AND CAMPAIGN_ID IS NOT NULL
        GROUP BY AGENCY_ID, CAMPAIGN_ID
    ),
    campaign_impressions AS (
        SELECT 
            v.AGENCY_ID,
            x.IO_ID as CAMPAIGN_ID,
            COUNT(*) as IMPRESSIONS
        FROM visit_campaigns v
        INNER JOIN QUORUMDB.SEGMENT_DATA.XANDR_IMPRESSION_LOG x 
            ON x.IO_ID = v.CAMPAIGN_ID AND x.AGENCY_ID = v.AGENCY_ID
        WHERE CAST(x.TIMESTAMP AS DATE) >= %(start_date)s
          AND CAST(x.TIMESTAMP AS DATE) <= %(end_date)s
        GROUP BY v.AGENCY_ID, x.IO_ID
    )
    SELECT 
        COALESCE(SUM(i.IMPRESSIONS), 0) as IMPRESSIONS,
        SUM(v.S_VISITS) as S_VISITS,
        COUNT(DISTINCT v.UNIQUE_VISITORS) as UNIQUE_VISITORS,
        COUNT(DISTINCT v.CAMPAIGN_ID) as UNIQUE_CAMPAIGNS,
        SUM(v.S_VISITS) as WITH_CAMPAIGN,
        CASE WHEN COALESCE(SUM(i.IMPRESSIONS), 0) > 0 
             THEN ROUND((SUM(v.S_VISITS)::FLOAT / SUM(i.IMPRESSIONS)) * 100, 4)
             ELSE 0 
        END as VISIT_RATE
    FROM visit_campaigns v
    LEFT JOIN campaign_impressions i ON v.CAMPAIGN_ID = i.CAMPAIGN_ID AND v.AGENCY_ID = i.AGENCY_ID
    """
    
    try:
        results = execute_query(query, {
            'advertiser_id': advertiser_id,
            'start_date': start_date,
            'end_date': end_date
        })
        return jsonify({'success': True, 'data': results[0] if results else {}})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/campaign-performance', methods=['GET'])
def get_campaign_performance():
    """Get campaign breakdown WITH IMPRESSIONS (direct campaign join)"""
    advertiser_id = request.args.get('advertiser_id')
    start_date, end_date = get_date_params(request)
    
    if not advertiser_id:
        return jsonify({'success': False, 'error': 'advertiser_id required'}), 400
    
    query = """
    WITH visit_data AS (
        SELECT 
            AGENCY_ID,
            CAMPAIGN_ID,
            MAX(CAMPAIGN_NAME) as CAMPAIGN_NAME,
            COUNT(*) as S_VISITS,
            COUNT(DISTINCT MAID) as UNIQUE_VISITORS
        FROM QUORUMDB.SEGMENT_DATA.QRM_ALL_VISITS_V3
        WHERE QUORUM_ADVERTISER_ID = %(advertiser_id)s
          AND CONVERSION_DATE >= %(start_date)s
          AND CONVERSION_DATE <= %(end_date)s
          AND VISIT_TYPE = 'STORE'
          AND CAMPAIGN_ID IS NOT NULL
        GROUP BY AGENCY_ID, CAMPAIGN_ID
    ),
    impressions AS (
        SELECT 
            v.CAMPAIGN_ID,
            MAX(x.IO_NAME) as CAMPAIGN_NAME_XANDR,
            COUNT(*) as IMPRESSIONS
        FROM visit_data v
        INNER JOIN QUORUMDB.SEGMENT_DATA.XANDR_IMPRESSION_LOG x 
            ON x.IO_ID = v.CAMPAIGN_ID 
            AND x.AGENCY_ID = v.AGENCY_ID
        WHERE CAST(x.TIMESTAMP AS DATE) >= %(start_date)s
          AND CAST(x.TIMESTAMP AS DATE) <= %(end_date)s
        GROUP BY v.CAMPAIGN_ID
    )
    SELECT 
        v.CAMPAIGN_ID,
        COALESCE(v.CAMPAIGN_NAME, i.CAMPAIGN_NAME_XANDR) as CAMPAIGN_NAME,
        COALESCE(i.IMPRESSIONS, 0) as IMPRESSIONS,
        v.S_VISITS,
        v.UNIQUE_VISITORS,
        CASE WHEN COALESCE(i.IMPRESSIONS, 0) > 0 
             THEN ROUND((v.S_VISITS::FLOAT / i.IMPRESSIONS) * 100, 4)
             ELSE 0 
        END as VISIT_RATE
    FROM visit_data v
    LEFT JOIN impressions i ON v.CAMPAIGN_ID = i.CAMPAIGN_ID
    ORDER BY IMPRESSIONS DESC NULLS LAST
    LIMIT 100
    """
    
    try:
        results = execute_query(query, {
            'advertiser_id': advertiser_id,
            'start_date': start_date,
            'end_date': end_date
        })
        return jsonify({'success': True, 'data': results})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/publisher-performance', methods=['GET'])
def get_publisher_performance():
    """Get store visit publisher breakdown WITH IMPRESSIONS"""
    advertiser_id = request.args.get('advertiser_id')
    agency_id = request.args.get('agency_id')  # Required for impression join
    min_visits = int(request.args.get('min_visits', 10))
    start_date, end_date = get_date_params(request)
    
    if not advertiser_id:
        return jsonify({'success': False, 'error': 'advertiser_id required'}), 400
    if not agency_id:
        return jsonify({'success': False, 'error': 'agency_id required'}), 400
    
    query = """
    WITH visit_publishers AS (
        SELECT 
            v.PUBLISHER,
            MAX(v.PLATFORM_TYPE) as PT,
            COUNT(*) as S_VISITS,
            COUNT(DISTINCT v.MAID) as UNIQUE_VISITORS
        FROM QUORUMDB.SEGMENT_DATA.QRM_ALL_VISITS_V3 v
        WHERE v.QUORUM_ADVERTISER_ID = %(advertiser_id)s
          AND v.CONVERSION_DATE >= %(start_date)s
          AND v.CONVERSION_DATE <= %(end_date)s
          AND v.VISIT_TYPE = 'STORE'
          AND v.PUBLISHER IS NOT NULL
        GROUP BY v.PUBLISHER
        HAVING COUNT(*) >= %(min_visits)s
    ),
    publisher_impressions AS (
        SELECT 
            vp.PUBLISHER,
            COUNT(*) as IMPRESSIONS
        FROM visit_publishers vp
        INNER JOIN QUORUMDB.SEGMENT_DATA.XANDR_IMPRESSION_LOG x 
            ON x.AGENCY_ID = %(agency_id)s
            AND (
                x.PUBLISHER_CODE = vp.PUBLISHER 
                OR x.PUBLISHER_CODE = REPLACE(REPLACE(vp.PUBLISHER, '%%2520', ' '), '%%20', ' ')
            )
        WHERE CAST(x.TIMESTAMP AS DATE) >= %(start_date)s
          AND CAST(x.TIMESTAMP AS DATE) <= %(end_date)s
        GROUP BY vp.PUBLISHER
    )
    SELECT 
        REPLACE(REPLACE(vp.PUBLISHER, '%%2520', ' '), '%%20', ' ') as PUBLISHER,
        vp.PT,
        COALESCE(pi.IMPRESSIONS, 0) as IMPRESSIONS,
        vp.S_VISITS,
        vp.UNIQUE_VISITORS,
        CASE WHEN COALESCE(pi.IMPRESSIONS, 0) > 0 
             THEN ROUND((vp.S_VISITS::FLOAT / pi.IMPRESSIONS) * 100, 4)
             ELSE 0 
        END as VISIT_RATE
    FROM visit_publishers vp
    LEFT JOIN publisher_impressions pi ON vp.PUBLISHER = pi.PUBLISHER
    ORDER BY COALESCE(pi.IMPRESSIONS, 0) DESC, vp.S_VISITS DESC
    """
    
    try:
        results = execute_query(query, {
            'advertiser_id': advertiser_id,
            'agency_id': int(agency_id),
            'start_date': start_date,
            'end_date': end_date,
            'min_visits': min_visits
        })
        return jsonify({'success': True, 'data': results})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/zip-performance', methods=['GET'])
def get_zip_performance():
    """Get store visit ZIP breakdown WITH IMPRESSIONS"""
    advertiser_id = request.args.get('advertiser_id')
    agency_id = request.args.get('agency_id')  # Required for impression join
    min_visits = int(request.args.get('min_visits', 10))
    start_date, end_date = get_date_params(request)
    
    if not advertiser_id:
        return jsonify({'success': False, 'error': 'advertiser_id required'}), 400
    if not agency_id:
        return jsonify({'success': False, 'error': 'agency_id required'}), 400
    
    query = """
    WITH visit_zips AS (
        SELECT 
            mca.ZIP_CODE,
            COUNT(*) as S_VISITS,
            COUNT(DISTINCT v.MAID) as UNIQUE_VISITORS
        FROM QUORUMDB.SEGMENT_DATA.QRM_ALL_VISITS_V3 v
        JOIN QUORUM_CROSS_CLOUD.ATTAIN_FEED.MAID_CENTROID_ASSOCIATION mca 
            ON UPPER(REPLACE(v.MAID, '-', '')) = UPPER(REPLACE(mca.DEVICE_ID, '-', ''))
        WHERE v.QUORUM_ADVERTISER_ID = %(advertiser_id)s
          AND v.CONVERSION_DATE >= %(start_date)s
          AND v.CONVERSION_DATE <= %(end_date)s
          AND v.VISIT_TYPE = 'STORE'
        GROUP BY mca.ZIP_CODE
        HAVING COUNT(*) >= %(min_visits)s
    ),
    zip_impressions AS (
        SELECT 
            x.POSTAL_CODE as ZIP_CODE,
            COUNT(*) as IMPRESSIONS
        FROM QUORUMDB.SEGMENT_DATA.XANDR_IMPRESSION_LOG x
        WHERE x.AGENCY_ID = %(agency_id)s
          AND CAST(x.TIMESTAMP AS DATE) >= %(start_date)s
          AND CAST(x.TIMESTAMP AS DATE) <= %(end_date)s
          AND x.POSTAL_CODE IS NOT NULL
        GROUP BY x.POSTAL_CODE
    )
    SELECT 
        vz.ZIP_CODE,
        zdm.DMA_NAME,
        zpd.POPULATION as ZIP_POPULATION,
        COALESCE(zi.IMPRESSIONS, 0) as IMPRESSIONS,
        vz.S_VISITS,
        vz.UNIQUE_VISITORS,
        CASE WHEN COALESCE(zi.IMPRESSIONS, 0) > 0 
             THEN ROUND((vz.S_VISITS::FLOAT / zi.IMPRESSIONS) * 100, 4)
             ELSE 0 
        END as VISIT_RATE
    FROM visit_zips vz
    LEFT JOIN zip_impressions zi ON vz.ZIP_CODE = zi.ZIP_CODE
    LEFT JOIN QUORUMDB.SEGMENT_DATA.ZIP_DMA_MAPPING zdm ON vz.ZIP_CODE = zdm.ZIP_CODE
    LEFT JOIN QUORUMDB.SEGMENT_DATA.ZIP_POPULATION_DATA zpd ON vz.ZIP_CODE = zpd.ZIP_CODE
    ORDER BY vz.S_VISITS DESC
    """
    
    try:
        results = execute_query(query, {
            'advertiser_id': advertiser_id,
            'agency_id': int(agency_id),
            'start_date': start_date,
            'end_date': end_date,
            'min_visits': min_visits
        })
        return jsonify({'success': True, 'data': results})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


# =============================================================================
# WEB ENDPOINTS (Backward Compatible)
# =============================================================================

@app.route('/api/web/advertisers', methods=['GET'])
def get_web_advertisers():
    """List web advertisers"""
    start_date, end_date = get_date_params(request)
    
    query = """
    SELECT 
        v.QUORUM_ADVERTISER_ID as ADVERTISER_ID,
        MAX(aa.COMP_NAME) as ADVERTISER_NAME,
        1480 as AGENCY_ID,
        'ViacomCBS WhoSay' as AGENCY_NAME,
        COUNT(*) as W_VISITS,
        SUM(v.IS_LEAD) as W_LEADS,
        SUM(v.IS_PURCHASE) as W_PURCHASES,
        SUM(v.PURCHASE_VALUE) as W_AMOUNT
    FROM QUORUMDB.SEGMENT_DATA.QRM_ALL_VISITS_V3 v
    LEFT JOIN QUORUMDB.SEGMENT_DATA.AGENCY_ADVERTISER aa 
        ON TRY_CAST(v.QUORUM_ADVERTISER_ID AS NUMBER) = aa.ID
    WHERE v.CONVERSION_DATE >= %(start_date)s
      AND v.CONVERSION_DATE <= %(end_date)s
      AND v.VISIT_TYPE = 'WEB'
    GROUP BY v.QUORUM_ADVERTISER_ID
    HAVING COUNT(*) > 0
    ORDER BY COUNT(*) DESC
    """
    
    try:
        results = execute_query(query, {'start_date': start_date, 'end_date': end_date})
        return jsonify({'success': True, 'data': results})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/web/summary', methods=['GET'])
def get_web_summary():
    """Get web visit summary"""
    advertiser_id = request.args.get('advertiser_id')
    start_date, end_date = get_date_params(request)
    
    if not advertiser_id:
        return jsonify({'success': False, 'error': 'advertiser_id required'}), 400
    
    query = """
    SELECT 
        COUNT(*) as W_VISITS,
        COUNT(DISTINCT MAID) as UNIQUE_VISITORS,
        SUM(PAGE_VIEWS) as W_PAGE_VIEWS,
        SUM(IS_LEAD) as W_LEADS,
        SUM(IS_PURCHASE) as W_PURCHASES,
        SUM(PURCHASE_VALUE) as W_AMOUNT,
        SUM(CASE WHEN CAMPAIGN_ID IS NOT NULL THEN 1 ELSE 0 END) as WITH_CAMPAIGN
    FROM QUORUMDB.SEGMENT_DATA.QRM_ALL_VISITS_V3
    WHERE QUORUM_ADVERTISER_ID = %(advertiser_id)s
      AND CONVERSION_DATE >= %(start_date)s
      AND CONVERSION_DATE <= %(end_date)s
      AND VISIT_TYPE = 'WEB'
    """
    
    try:
        results = execute_query(query, {
            'advertiser_id': advertiser_id,
            'start_date': start_date,
            'end_date': end_date
        })
        return jsonify({'success': True, 'data': results[0] if results else {}})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/web/campaign-performance', methods=['GET'])
def get_web_campaign_performance():
    """Get web visit campaign breakdown"""
    advertiser_id = request.args.get('advertiser_id')
    start_date, end_date = get_date_params(request)
    
    if not advertiser_id:
        return jsonify({'success': False, 'error': 'advertiser_id required'}), 400
    
    query = """
    SELECT 
        CAMPAIGN_ID,
        MAX(CAMPAIGN_NAME) as CAMPAIGN_NAME,
        COUNT(*) as W_VISITS,
        SUM(IS_LEAD) as W_LEADS,
        SUM(IS_PURCHASE) as W_PURCHASES,
        SUM(PURCHASE_VALUE) as W_AMOUNT,
        COUNT(DISTINCT MAID) as UNIQUE_VISITORS
    FROM QUORUMDB.SEGMENT_DATA.QRM_ALL_VISITS_V3
    WHERE QUORUM_ADVERTISER_ID = %(advertiser_id)s
      AND CONVERSION_DATE >= %(start_date)s
      AND CONVERSION_DATE <= %(end_date)s
      AND VISIT_TYPE = 'WEB'
      AND CAMPAIGN_ID IS NOT NULL
    GROUP BY CAMPAIGN_ID
    ORDER BY W_VISITS DESC
    """
    
    try:
        results = execute_query(query, {
            'advertiser_id': advertiser_id,
            'start_date': start_date,
            'end_date': end_date
        })
        return jsonify({'success': True, 'data': results})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/web/publisher-performance', methods=['GET'])
def get_web_publisher_performance():
    """Get web visit publisher breakdown"""
    advertiser_id = request.args.get('advertiser_id')
    min_visits = int(request.args.get('min_visits', 10))
    start_date, end_date = get_date_params(request)
    
    if not advertiser_id:
        return jsonify({'success': False, 'error': 'advertiser_id required'}), 400
    
    query = """
    SELECT 
        PUBLISHER,
        MAX(PLATFORM_TYPE) as PT,
        COUNT(*) as W_VISITS,
        SUM(IS_LEAD) as W_LEADS,
        SUM(IS_PURCHASE) as W_PURCHASES,
        SUM(PURCHASE_VALUE) as W_AMOUNT,
        COUNT(DISTINCT MAID) as UNIQUE_VISITORS
    FROM QUORUMDB.SEGMENT_DATA.QRM_ALL_VISITS_V3
    WHERE QUORUM_ADVERTISER_ID = %(advertiser_id)s
      AND CONVERSION_DATE >= %(start_date)s
      AND CONVERSION_DATE <= %(end_date)s
      AND VISIT_TYPE = 'WEB'
      AND PUBLISHER IS NOT NULL
    GROUP BY PUBLISHER
    HAVING W_VISITS >= %(min_visits)s
    ORDER BY W_VISITS DESC
    """
    
    try:
        results = execute_query(query, {
            'advertiser_id': advertiser_id,
            'start_date': start_date,
            'end_date': end_date,
            'min_visits': min_visits
        })
        return jsonify({'success': True, 'data': results})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/web/zip-performance', methods=['GET'])
def get_web_zip_performance():
    """Get web visit ZIP breakdown"""
    advertiser_id = request.args.get('advertiser_id')
    min_visits = int(request.args.get('min_visits', 10))
    start_date, end_date = get_date_params(request)
    
    if not advertiser_id:
        return jsonify({'success': False, 'error': 'advertiser_id required'}), 400
    
    query = """
    WITH visit_zips AS (
        SELECT 
            mca.ZIP_CODE,
            COUNT(*) as W_VISITS,
            SUM(v.IS_LEAD) as W_LEADS,
            SUM(v.IS_PURCHASE) as W_PURCHASES,
            SUM(v.PURCHASE_VALUE) as W_AMOUNT,
            COUNT(DISTINCT v.MAID) as UNIQUE_VISITORS
        FROM QUORUMDB.SEGMENT_DATA.QRM_ALL_VISITS_V3 v
        JOIN QUORUM_CROSS_CLOUD.ATTAIN_FEED.MAID_CENTROID_ASSOCIATION mca 
            ON UPPER(REPLACE(v.MAID, '-', '')) = UPPER(REPLACE(mca.DEVICE_ID, '-', ''))
        WHERE v.QUORUM_ADVERTISER_ID = %(advertiser_id)s
          AND v.CONVERSION_DATE >= %(start_date)s
          AND v.CONVERSION_DATE <= %(end_date)s
          AND v.VISIT_TYPE = 'WEB'
        GROUP BY mca.ZIP_CODE
    )
    SELECT 
        vz.ZIP_CODE,
        zdm.DMA_NAME,
        zpd.POPULATION as ZIP_POPULATION,
        vz.W_VISITS,
        vz.W_LEADS,
        vz.W_PURCHASES,
        vz.W_AMOUNT,
        vz.UNIQUE_VISITORS
    FROM visit_zips vz
    LEFT JOIN QUORUMDB.SEGMENT_DATA.ZIP_DMA_MAPPING zdm ON vz.ZIP_CODE = zdm.ZIP_CODE
    LEFT JOIN QUORUMDB.SEGMENT_DATA.ZIP_POPULATION_DATA zpd ON vz.ZIP_CODE = zpd.ZIP_CODE
    WHERE vz.W_VISITS >= %(min_visits)s
    ORDER BY vz.W_VISITS DESC
    """
    
    try:
        results = execute_query(query, {
            'advertiser_id': advertiser_id,
            'start_date': start_date,
            'end_date': end_date,
            'min_visits': min_visits
        })
        return jsonify({'success': True, 'data': results})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


# =============================================================================
# V3 UNIFIED ENDPOINTS (New - Handles both Store + Web)
# =============================================================================

@app.route('/api/v3/agencies', methods=['GET'])
def get_agencies_v3():
    """List all agencies with both store and web metrics
    
    FIX: Web visits have NULL v.AGENCY_ID, so get from AGENCY_ADVERTISER join
    """
    start_date, end_date = get_date_params(request)
    
    query = """
    SELECT 
        aa.AGENCY_ID,
        MAX(aa.AGENCY_NAME) as AGENCY_NAME,
        SUM(CASE WHEN v.VISIT_TYPE = 'STORE' THEN 1 ELSE 0 END) as S_VISITS,
        SUM(CASE WHEN v.VISIT_TYPE = 'WEB' THEN 1 ELSE 0 END) as W_VISITS,
        SUM(v.IS_LEAD) as W_LEADS,
        SUM(v.IS_PURCHASE) as W_PURCHASES,
        COUNT(DISTINCT v.QUORUM_ADVERTISER_ID) as ADVERTISER_COUNT,
        COUNT(DISTINCT v.MAID) as UNIQUE_VISITORS
    FROM QUORUMDB.SEGMENT_DATA.QRM_ALL_VISITS_V3 v
    LEFT JOIN QUORUMDB.SEGMENT_DATA.AGENCY_ADVERTISER aa 
        ON TRY_CAST(v.QUORUM_ADVERTISER_ID AS NUMBER) = aa.ID
    WHERE v.CONVERSION_DATE >= %(start_date)s
      AND v.CONVERSION_DATE <= %(end_date)s
      AND aa.AGENCY_ID IS NOT NULL
    GROUP BY aa.AGENCY_ID
    HAVING SUM(CASE WHEN v.VISIT_TYPE = 'STORE' THEN 1 ELSE 0 END) > 0 
        OR SUM(CASE WHEN v.VISIT_TYPE = 'WEB' THEN 1 ELSE 0 END) > 0
    ORDER BY (SUM(CASE WHEN v.VISIT_TYPE = 'STORE' THEN 1 ELSE 0 END) + 
              SUM(CASE WHEN v.VISIT_TYPE = 'WEB' THEN 1 ELSE 0 END)) DESC
    """
    
    try:
        results = execute_query(query, {'start_date': start_date, 'end_date': end_date})
        return jsonify({'success': True, 'data': results})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/v3/advertiser-summary', methods=['GET'])
def get_advertiser_summary_v3():
    """Get unified summary WITH IMPRESSIONS (store and web)"""
    advertiser_id = request.args.get('advertiser_id')
    start_date, end_date = get_date_params(request)
    
    if not advertiser_id:
        return jsonify({'success': False, 'error': 'advertiser_id required'}), 400
    
    query = """
    WITH visits AS (
        SELECT 
            VISIT_TYPE,
            COUNT(*) as TOTAL_VISITS,
            COUNT(DISTINCT MAID) as UNIQUE_VISITORS,
            SUM(PAGE_VIEWS) as TOTAL_PAGE_VIEWS,
            SUM(IS_LEAD) as TOTAL_LEADS,
            SUM(IS_PURCHASE) as TOTAL_PURCHASES,
            SUM(PURCHASE_VALUE) as TOTAL_REVENUE,
            SUM(CASE WHEN CAMPAIGN_ID IS NOT NULL THEN 1 ELSE 0 END) as WITH_CAMPAIGN,
            COUNT(DISTINCT CAMPAIGN_ID) as UNIQUE_CAMPAIGNS
        FROM QUORUMDB.SEGMENT_DATA.QRM_ALL_VISITS_V3
        WHERE QUORUM_ADVERTISER_ID = %(advertiser_id)s
          AND CONVERSION_DATE >= %(start_date)s
          AND CONVERSION_DATE <= %(end_date)s
        GROUP BY VISIT_TYPE
    ),
    impressions AS (
        SELECT COALESCE(COUNT(*), 0) as IMPRESSIONS
        FROM QUORUMDB.SEGMENT_DATA.XANDR_IMPRESSION_LOG
        WHERE CAST(QUORUM_ADVERTISER_ID AS VARCHAR) = %(advertiser_id)s
          AND CAST(TIMESTAMP AS DATE) >= %(start_date)s
          AND CAST(TIMESTAMP AS DATE) <= %(end_date)s
    )
    SELECT 
        v.VISIT_TYPE,
        v.TOTAL_VISITS,
        v.UNIQUE_VISITORS,
        v.TOTAL_PAGE_VIEWS,
        v.TOTAL_LEADS,
        v.TOTAL_PURCHASES,
        v.TOTAL_REVENUE,
        v.WITH_CAMPAIGN,
        v.UNIQUE_CAMPAIGNS,
        i.IMPRESSIONS,
        CASE WHEN i.IMPRESSIONS > 0 AND v.VISIT_TYPE = 'STORE'
             THEN ROUND((v.TOTAL_VISITS::FLOAT / i.IMPRESSIONS) * 100, 4)
             ELSE 0 
        END as VISIT_RATE
    FROM visits v
    CROSS JOIN impressions i
    """
    
    try:
        results = execute_query(query, {
            'advertiser_id': advertiser_id,
            'start_date': start_date,
            'end_date': end_date
        })
        
        # Restructure response
        response = {
            'advertiser_id': advertiser_id,
            'date_range': {'start': start_date, 'end': end_date},
            'store': None,
            'web': None
        }
        
        for row in results:
            if row['VISIT_TYPE'] == 'STORE':
                response['store'] = {
                    'impressions': row['IMPRESSIONS'],
                    'visits': row['TOTAL_VISITS'],
                    'visit_rate': row['VISIT_RATE'],
                    'unique_visitors': row['UNIQUE_VISITORS'],
                    'with_campaign': row['WITH_CAMPAIGN'],
                    'campaigns': row['UNIQUE_CAMPAIGNS']
                }
            elif row['VISIT_TYPE'] == 'WEB':
                response['web'] = {
                    'visits': row['TOTAL_VISITS'],
                    'unique_visitors': row['UNIQUE_VISITORS'],
                    'page_views': row['TOTAL_PAGE_VIEWS'],
                    'leads': row['TOTAL_LEADS'],
                    'purchases': row['TOTAL_PURCHASES'],
                    'revenue': row['TOTAL_REVENUE'],
                    'with_campaign': row['WITH_CAMPAIGN'],
                    'campaigns': row['UNIQUE_CAMPAIGNS']
                }
        
        return jsonify({'success': True, 'data': response})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


# =============================================================================
# MAIN
# =============================================================================

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=False)
