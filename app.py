"""
Quorum Optimizer API v5 - Gold Table Architecture
FIXED: Class B nested aggregate SQL errors
Uses QUORUM_ADV_STORE_VISITS for Class A store advertisers
Uses CAMPAIGN_PERFORMANCE_REPORT_WEEKLY_STATS for Class B
Uses PARAMOUNT_IMPRESSIONS_REPORT_90_DAYS for Class W (Paramount)
"""

import os
from flask import Flask, request, jsonify
from flask_cors import CORS
import snowflake.connector
from datetime import datetime, timedelta

app = Flask(__name__)
CORS(app)

# Agency Classification
CLASS_A_AGENCIES = {2514, 1956, 2298, 1955, 2086, 1950}  # MNTN, Dealer Spike, InteractRV, ARI, Level5, ByRider
CLASS_B_AGENCIES = {1813, 2234, 1972, 2379, 1445, 1880, 2744}  # Causal iQ, Magnite, Hearst, The Shipyard, Publicis, TeamSnap, Parallel Path
CLASS_W_AGENCIES = {1480}  # ViacomCBS / Paramount

def get_snowflake_connection():
    return snowflake.connector.connect(
        user=os.environ.get('SNOWFLAKE_USER', 'OPTIMIZER_SERVICE_USER'),
        password=os.environ.get('SNOWFLAKE_PASSWORD'),
        account=os.environ.get('SNOWFLAKE_ACCOUNT', 'FZB05958.us-east-1'),
        warehouse=os.environ.get('SNOWFLAKE_WAREHOUSE', 'COMPUTE_WH'),
        database='QUORUMDB',
        schema='SEGMENT_DATA'
    )

def get_agency_class(agency_id):
    """Determine which query pattern to use"""
    agency_id = int(agency_id) if agency_id else 0
    if agency_id in CLASS_A_AGENCIES:
        return 'A'
    elif agency_id in CLASS_B_AGENCIES:
        return 'B'
    elif agency_id in CLASS_W_AGENCIES:
        return 'W'
    return None

# ============================================================================
# HEALTH CHECK
# ============================================================================

@app.route('/health', methods=['GET'])
def health():
    return jsonify({
        'status': 'healthy',
        'version': 'v5-fixed',
        'tables': {
            'class_a': ['QUORUM_ADV_STORE_VISITS'],
            'class_b': ['CAMPAIGN_PERFORMANCE_REPORT_WEEKLY_STATS'],
            'class_w': ['PARAMOUNT_IMPRESSIONS_REPORT_90_DAYS']
        },
        'class_a_agencies': list(CLASS_A_AGENCIES),
        'class_b_agencies': list(CLASS_B_AGENCIES),
        'class_w_agencies': list(CLASS_W_AGENCIES)
    })

# ============================================================================
# AGENCY OVERVIEW - All agencies with totals
# ============================================================================

@app.route('/api/v5/agency-overview', methods=['GET'])
def get_agency_overview_v5():
    """Get all agencies with impression and visit counts"""
    try:
        start_date = request.args.get('start_date', (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d'))
        end_date = request.args.get('end_date', datetime.now().strftime('%Y-%m-%d'))
        
        conn = get_snowflake_connection()
        cursor = conn.cursor()
        
        all_results = []
        
        # Class A agencies from QUORUM_ADV_STORE_VISITS
        query_class_a = """
            WITH store_stats AS (
                SELECT 
                    s.AGENCY_ID,
                    COUNT(DISTINCT s.AD_IMP_ID) as IMPRESSIONS,
                    0 as WEB_VISITS,
                    COUNT(DISTINCT CASE WHEN s.IS_STORE_VISIT = TRUE THEN s.DEVICE_ID_QU END) as LOCATION_VISITS,
                    COUNT(DISTINCT s.QUORUM_ADVERTISER_ID) as ADVERTISER_COUNT
                FROM QUORUMDB.SEGMENT_DATA.QUORUM_ADV_STORE_VISITS s
                WHERE s.IMP_TIMESTAMP >= %s AND s.IMP_TIMESTAMP < %s
                  AND s.AGENCY_ID IN (2514, 1956, 2298, 1955, 2086, 1950)
                GROUP BY s.AGENCY_ID
            )
            SELECT 
                s.AGENCY_ID,
                COALESCE(aa.AGENCY_NAME, 'Agency ' || s.AGENCY_ID) as AGENCY_NAME,
                s.IMPRESSIONS,
                s.WEB_VISITS,
                s.LOCATION_VISITS,
                s.ADVERTISER_COUNT,
                'A' as AGENCY_CLASS
            FROM store_stats s
            LEFT JOIN (
                SELECT DISTINCT ADVERTISER_ID as AGENCY_ID, AGENCY_NAME 
                FROM QUORUMDB.SEGMENT_DATA.AGENCY_ADVERTISER
                WHERE AGENCY_NAME IS NOT NULL
            ) aa ON s.AGENCY_ID = aa.AGENCY_ID
        """
        cursor.execute(query_class_a, (start_date, end_date))
        columns = [desc[0] for desc in cursor.description]
        for row in cursor.fetchall():
            all_results.append(dict(zip(columns, row)))
        
        # Class B agencies from CAMPAIGN_PERFORMANCE_REPORT_WEEKLY_STATS
        query_class_b = """
            WITH weekly_stats AS (
                SELECT 
                    w.AGENCY_ID,
                    SUM(w.IMPRESSIONS) as IMPRESSIONS,
                    0 as WEB_VISITS,
                    SUM(w.VISITORS) as LOCATION_VISITS,
                    COUNT(DISTINCT w.ADVERTISER_ID) as ADVERTISER_COUNT
                FROM QUORUMDB.SEGMENT_DATA.CAMPAIGN_PERFORMANCE_REPORT_WEEKLY_STATS w
                WHERE w.LOG_DATE >= %s AND w.LOG_DATE < %s
                  AND w.AGENCY_ID IN (1813, 2234, 1972, 2379, 1445, 1880, 2744)
                GROUP BY w.AGENCY_ID
            )
            SELECT 
                s.AGENCY_ID,
                COALESCE(aa.AGENCY_NAME, 'Agency ' || s.AGENCY_ID) as AGENCY_NAME,
                s.IMPRESSIONS,
                s.WEB_VISITS,
                s.LOCATION_VISITS,
                s.ADVERTISER_COUNT,
                'B' as AGENCY_CLASS
            FROM weekly_stats s
            LEFT JOIN (
                SELECT DISTINCT ADVERTISER_ID as AGENCY_ID, AGENCY_NAME 
                FROM QUORUMDB.SEGMENT_DATA.AGENCY_ADVERTISER
                WHERE AGENCY_NAME IS NOT NULL
            ) aa ON s.AGENCY_ID = aa.AGENCY_ID
        """
        cursor.execute(query_class_b, (start_date, end_date))
        columns = [desc[0] for desc in cursor.description]
        for row in cursor.fetchall():
            all_results.append(dict(zip(columns, row)))
        
        # Class W (Paramount) from PARAMOUNT_IMPRESSIONS_REPORT_90_DAYS
        query_class_w = """
            SELECT 
                1480 as AGENCY_ID,
                'ViacomCBS / Paramount' as AGENCY_NAME,
                COUNT(*) as IMPRESSIONS,
                SUM(CASE WHEN IS_SITE_VISIT = TRUE THEN 1 ELSE 0 END) as WEB_VISITS,
                SUM(CASE WHEN IS_STORE_VISIT = TRUE THEN 1 ELSE 0 END) as LOCATION_VISITS,
                COUNT(DISTINCT QUORUM_ADVERTISER_ID) as ADVERTISER_COUNT,
                'W' as AGENCY_CLASS
            FROM QUORUMDB.SEGMENT_DATA.PARAMOUNT_IMPRESSIONS_REPORT_90_DAYS
        """
        cursor.execute(query_class_w)
        columns = [desc[0] for desc in cursor.description]
        for row in cursor.fetchall():
            all_results.append(dict(zip(columns, row)))
        
        # Add visit rates
        for row in all_results:
            impressions = row.get('IMPRESSIONS', 0) or 0
            web_visits = row.get('WEB_VISITS', 0) or 0
            loc_visits = row.get('LOCATION_VISITS', 0) or 0
            row['W_VISIT_RATE'] = round((web_visits / impressions * 100), 4) if impressions > 0 else 0
            row['L_VISIT_RATE'] = round((loc_visits / impressions * 100), 4) if impressions > 0 else 0
        
        # Sort by impressions
        all_results.sort(key=lambda x: x.get('IMPRESSIONS', 0) or 0, reverse=True)
        
        cursor.close()
        conn.close()
        
        return jsonify({'success': True, 'data': all_results})
        
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})

# ============================================================================
# ADVERTISER OVERVIEW - All advertisers for an agency
# ============================================================================

@app.route('/api/v5/advertiser-overview', methods=['GET'])
def get_advertiser_overview_v5():
    """Get all advertisers for an agency with impression and visit counts"""
    try:
        agency_id = request.args.get('agency_id')
        start_date = request.args.get('start_date', (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d'))
        end_date = request.args.get('end_date', datetime.now().strftime('%Y-%m-%d'))
        
        if not agency_id:
            return jsonify({'success': False, 'error': 'agency_id required'})
        
        agency_class = get_agency_class(agency_id)
        conn = get_snowflake_connection()
        cursor = conn.cursor()
        
        if agency_class == 'A':
            query = """
                SELECT 
                    s.QUORUM_ADVERTISER_ID as ADVERTISER_ID,
                    COALESCE(aa.COMP_NAME, 'Advertiser ' || s.QUORUM_ADVERTISER_ID) as ADVERTISER_NAME,
                    COUNT(DISTINCT s.AD_IMP_ID) as IMPRESSIONS,
                    0 as WEB_VISITS,
                    COUNT(DISTINCT CASE WHEN s.IS_STORE_VISIT = TRUE THEN s.DEVICE_ID_QU END) as LOCATION_VISITS
                FROM QUORUMDB.SEGMENT_DATA.QUORUM_ADV_STORE_VISITS s
                LEFT JOIN QUORUMDB.SEGMENT_DATA.AGENCY_ADVERTISER aa ON s.QUORUM_ADVERTISER_ID = aa.ID
                WHERE s.AGENCY_ID = %s
                  AND s.IMP_TIMESTAMP >= %s AND s.IMP_TIMESTAMP < %s
                GROUP BY s.QUORUM_ADVERTISER_ID, aa.COMP_NAME
                HAVING COUNT(DISTINCT s.AD_IMP_ID) >= 10000
                ORDER BY IMPRESSIONS DESC
            """
            cursor.execute(query, (agency_id, start_date, end_date))
            
        elif agency_class == 'B':
            query = """
                SELECT 
                    w.ADVERTISER_ID,
                    COALESCE(aa.COMP_NAME, 'Advertiser ' || w.ADVERTISER_ID) as ADVERTISER_NAME,
                    SUM(w.IMPRESSIONS) as IMPRESSIONS,
                    0 as WEB_VISITS,
                    SUM(w.VISITORS) as LOCATION_VISITS
                FROM QUORUMDB.SEGMENT_DATA.CAMPAIGN_PERFORMANCE_REPORT_WEEKLY_STATS w
                LEFT JOIN QUORUMDB.SEGMENT_DATA.AGENCY_ADVERTISER aa ON w.ADVERTISER_ID = aa.ID
                WHERE w.AGENCY_ID = %s
                  AND w.LOG_DATE >= %s AND w.LOG_DATE < %s
                GROUP BY w.ADVERTISER_ID, aa.COMP_NAME
                HAVING SUM(w.IMPRESSIONS) >= 10000
                ORDER BY IMPRESSIONS DESC
            """
            cursor.execute(query, (agency_id, start_date, end_date))
            
        elif agency_class == 'W':
            query = """
                SELECT 
                    p.QUORUM_ADVERTISER_ID as ADVERTISER_ID,
                    COALESCE(aa.COMP_NAME, p.QUORUM_ADVERTISER_ID::VARCHAR) as ADVERTISER_NAME,
                    COUNT(*) as IMPRESSIONS,
                    SUM(CASE WHEN p.IS_SITE_VISIT = TRUE THEN 1 ELSE 0 END) as WEB_VISITS,
                    SUM(CASE WHEN p.IS_STORE_VISIT = TRUE THEN 1 ELSE 0 END) as LOCATION_VISITS
                FROM QUORUMDB.SEGMENT_DATA.PARAMOUNT_IMPRESSIONS_REPORT_90_DAYS p
                LEFT JOIN QUORUMDB.SEGMENT_DATA.AGENCY_ADVERTISER aa ON p.QUORUM_ADVERTISER_ID = aa.ID
                GROUP BY p.QUORUM_ADVERTISER_ID, aa.COMP_NAME
                HAVING COUNT(*) >= 10000
                ORDER BY IMPRESSIONS DESC
            """
            cursor.execute(query)
        else:
            cursor.close()
            conn.close()
            return jsonify({'success': False, 'error': f'Unknown agency class for agency_id {agency_id}'})
        
        columns = [desc[0] for desc in cursor.description]
        results = [dict(zip(columns, row)) for row in cursor.fetchall()]
        
        # Add visit rates
        for row in results:
            impressions = row.get('IMPRESSIONS', 0) or 0
            web_visits = row.get('WEB_VISITS', 0) or 0
            loc_visits = row.get('LOCATION_VISITS', 0) or 0
            row['W_VISIT_RATE'] = round((web_visits / impressions * 100), 4) if impressions > 0 else 0
            row['L_VISIT_RATE'] = round((loc_visits / impressions * 100), 4) if impressions > 0 else 0
        
        cursor.close()
        conn.close()
        
        return jsonify({'success': True, 'data': results})
        
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})

# ============================================================================
# ADVERTISER SUMMARY - Summary cards for a single advertiser
# ============================================================================

@app.route('/api/v5/advertiser-summary', methods=['GET'])
def get_advertiser_summary_v5():
    """Get summary metrics for a single advertiser"""
    try:
        advertiser_id = request.args.get('advertiser_id')
        agency_id = request.args.get('agency_id')
        start_date = request.args.get('start_date', (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d'))
        end_date = request.args.get('end_date', datetime.now().strftime('%Y-%m-%d'))
        
        if not advertiser_id or not agency_id:
            return jsonify({'success': False, 'error': 'advertiser_id and agency_id required'})
        
        agency_class = get_agency_class(agency_id)
        conn = get_snowflake_connection()
        cursor = conn.cursor()
        
        if agency_class == 'A':
            query = """
                SELECT 
                    COUNT(DISTINCT s.AD_IMP_ID) as IMPRESSIONS,
                    0 as WEB_VISITS,
                    COUNT(DISTINCT CASE WHEN s.IS_STORE_VISIT = TRUE THEN s.DEVICE_ID_QU END) as LOCATION_VISITS,
                    COUNT(DISTINCT s.IO_ID) as CAMPAIGN_COUNT,
                    COUNT(DISTINCT s.PUBLISHER_CODE) as PUBLISHER_COUNT,
                    COUNT(DISTINCT s.CREATIVE_ID) as CREATIVE_COUNT
                FROM QUORUMDB.SEGMENT_DATA.QUORUM_ADV_STORE_VISITS s
                WHERE s.QUORUM_ADVERTISER_ID = %s
                  AND s.AGENCY_ID = %s
                  AND s.IMP_TIMESTAMP >= %s AND s.IMP_TIMESTAMP < %s
            """
            cursor.execute(query, (advertiser_id, agency_id, start_date, end_date))
            
        elif agency_class == 'B':
            query = """
                SELECT 
                    SUM(w.IMPRESSIONS) as IMPRESSIONS,
                    0 as WEB_VISITS,
                    SUM(w.VISITORS) as LOCATION_VISITS,
                    COUNT(DISTINCT w.IO_ID) as CAMPAIGN_COUNT,
                    COUNT(DISTINCT w.PUBLISHER) as PUBLISHER_COUNT,
                    0 as CREATIVE_COUNT
                FROM QUORUMDB.SEGMENT_DATA.CAMPAIGN_PERFORMANCE_REPORT_WEEKLY_STATS w
                WHERE w.ADVERTISER_ID = %s
                  AND w.AGENCY_ID = %s
                  AND w.LOG_DATE >= %s AND w.LOG_DATE < %s
            """
            cursor.execute(query, (advertiser_id, agency_id, start_date, end_date))
            
        elif agency_class == 'W':
            query = """
                SELECT 
                    COUNT(*) as IMPRESSIONS,
                    SUM(CASE WHEN IS_SITE_VISIT = TRUE THEN 1 ELSE 0 END) as WEB_VISITS,
                    SUM(CASE WHEN IS_STORE_VISIT = TRUE THEN 1 ELSE 0 END) as LOCATION_VISITS,
                    COUNT(DISTINCT IO_ID) as CAMPAIGN_COUNT,
                    COUNT(DISTINCT PUBLISHER_CODE) as PUBLISHER_COUNT,
                    COUNT(DISTINCT CREATIVE_ID) as CREATIVE_COUNT
                FROM QUORUMDB.SEGMENT_DATA.PARAMOUNT_IMPRESSIONS_REPORT_90_DAYS
                WHERE QUORUM_ADVERTISER_ID = %s
            """
            cursor.execute(query, (advertiser_id,))
        else:
            cursor.close()
            conn.close()
            return jsonify({'success': False, 'error': f'Unknown agency class'})
        
        columns = [desc[0] for desc in cursor.description]
        row = cursor.fetchone()
        result = dict(zip(columns, row)) if row else {}
        
        # Add visit rates
        impressions = result.get('IMPRESSIONS', 0) or 0
        web_visits = result.get('WEB_VISITS', 0) or 0
        loc_visits = result.get('LOCATION_VISITS', 0) or 0
        result['W_VISIT_RATE'] = round((web_visits / impressions * 100), 4) if impressions > 0 else 0
        result['L_VISIT_RATE'] = round((loc_visits / impressions * 100), 4) if impressions > 0 else 0
        
        cursor.close()
        conn.close()
        
        return jsonify({'success': True, 'data': result})
        
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})

# ============================================================================
# CAMPAIGN PERFORMANCE - All campaigns for an advertiser
# ============================================================================

@app.route('/api/v5/campaign-performance', methods=['GET'])
def get_campaign_performance_v5():
    """Get campaign-level performance for an advertiser - FIXED FOR CLASS B"""
    try:
        advertiser_id = request.args.get('advertiser_id')
        agency_id = request.args.get('agency_id')
        start_date = request.args.get('start_date', (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d'))
        end_date = request.args.get('end_date', datetime.now().strftime('%Y-%m-%d'))
        
        if not advertiser_id or not agency_id:
            return jsonify({'success': False, 'error': 'advertiser_id and agency_id required'})
        
        agency_class = get_agency_class(agency_id)
        conn = get_snowflake_connection()
        cursor = conn.cursor()
        
        if agency_class == 'A':
            query = """
                SELECT 
                    s.IO_ID,
                    COALESCE(c.IO_NAME, 'Campaign ' || s.IO_ID) as CAMPAIGN_NAME,
                    COUNT(DISTINCT s.AD_IMP_ID) as IMPRESSIONS,
                    0 as WEB_VISITS,
                    COUNT(DISTINCT CASE WHEN s.IS_STORE_VISIT = TRUE THEN s.DEVICE_ID_QU END) as LOCATION_VISITS
                FROM QUORUMDB.SEGMENT_DATA.QUORUM_ADV_STORE_VISITS s
                LEFT JOIN QUORUMDB.SEGMENT_DATA.CAMPAIGNS c ON s.IO_ID = c.IO_ID
                WHERE s.QUORUM_ADVERTISER_ID = %s
                  AND s.AGENCY_ID = %s
                  AND s.IMP_TIMESTAMP >= %s AND s.IMP_TIMESTAMP < %s
                GROUP BY s.IO_ID, c.IO_NAME
                HAVING COUNT(DISTINCT s.AD_IMP_ID) >= 1000
                ORDER BY IMPRESSIONS DESC
            """
            cursor.execute(query, (advertiser_id, agency_id, start_date, end_date))
            
        elif agency_class == 'B':
            # FIXED: Removed nested aggregate - just use direct SUM
            query = """
                SELECT 
                    w.IO_ID,
                    COALESCE(MAX(c.IO_NAME), 'Campaign ' || w.IO_ID) as CAMPAIGN_NAME,
                    SUM(w.IMPRESSIONS) as IMPRESSIONS,
                    0 as WEB_VISITS,
                    SUM(w.VISITORS) as LOCATION_VISITS
                FROM QUORUMDB.SEGMENT_DATA.CAMPAIGN_PERFORMANCE_REPORT_WEEKLY_STATS w
                LEFT JOIN QUORUMDB.SEGMENT_DATA.CAMPAIGNS c ON w.IO_ID = c.IO_ID
                WHERE w.ADVERTISER_ID = %s
                  AND w.AGENCY_ID = %s
                  AND w.LOG_DATE >= %s AND w.LOG_DATE < %s
                GROUP BY w.IO_ID
                HAVING SUM(w.IMPRESSIONS) >= 1000
                ORDER BY IMPRESSIONS DESC
            """
            cursor.execute(query, (advertiser_id, agency_id, start_date, end_date))
            
        elif agency_class == 'W':
            query = """
                SELECT 
                    p.IO_ID,
                    COALESCE(c.IO_NAME, 'Campaign ' || p.IO_ID) as CAMPAIGN_NAME,
                    COUNT(*) as IMPRESSIONS,
                    SUM(CASE WHEN p.IS_SITE_VISIT = TRUE THEN 1 ELSE 0 END) as WEB_VISITS,
                    SUM(CASE WHEN p.IS_STORE_VISIT = TRUE THEN 1 ELSE 0 END) as LOCATION_VISITS
                FROM QUORUMDB.SEGMENT_DATA.PARAMOUNT_IMPRESSIONS_REPORT_90_DAYS p
                LEFT JOIN QUORUMDB.SEGMENT_DATA.CAMPAIGNS c ON p.IO_ID = c.IO_ID
                WHERE p.QUORUM_ADVERTISER_ID = %s
                GROUP BY p.IO_ID, c.IO_NAME
                HAVING COUNT(*) >= 1000
                ORDER BY IMPRESSIONS DESC
            """
            cursor.execute(query, (advertiser_id,))
        else:
            cursor.close()
            conn.close()
            return jsonify({'success': False, 'error': f'Unknown agency class'})
        
        columns = [desc[0] for desc in cursor.description]
        results = [dict(zip(columns, row)) for row in cursor.fetchall()]
        
        # Add visit rates
        for row in results:
            impressions = row.get('IMPRESSIONS', 0) or 0
            web_visits = row.get('WEB_VISITS', 0) or 0
            loc_visits = row.get('LOCATION_VISITS', 0) or 0
            row['W_VISIT_RATE'] = round((web_visits / impressions * 100), 4) if impressions > 0 else 0
            row['L_VISIT_RATE'] = round((loc_visits / impressions * 100), 4) if impressions > 0 else 0
        
        cursor.close()
        conn.close()
        
        return jsonify({'success': True, 'data': results})
        
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})

# ============================================================================
# LINE ITEM PERFORMANCE - Line items for a campaign
# ============================================================================

@app.route('/api/v5/lineitem-performance', methods=['GET'])
def get_lineitem_performance_v5():
    """Get line item performance for a campaign"""
    try:
        campaign_id = request.args.get('campaign_id')
        advertiser_id = request.args.get('advertiser_id')
        agency_id = request.args.get('agency_id')
        start_date = request.args.get('start_date', (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d'))
        end_date = request.args.get('end_date', datetime.now().strftime('%Y-%m-%d'))
        
        if not campaign_id or not agency_id:
            return jsonify({'success': False, 'error': 'campaign_id and agency_id required'})
        
        agency_class = get_agency_class(agency_id)
        conn = get_snowflake_connection()
        cursor = conn.cursor()
        
        if agency_class == 'A':
            query = """
                SELECT 
                    s.LINE_ITEM_ID,
                    COALESCE(li.LINE_ITEM_NAME, 'Line Item ' || s.LINE_ITEM_ID) as LINE_ITEM_NAME,
                    COUNT(DISTINCT s.AD_IMP_ID) as IMPRESSIONS,
                    0 as WEB_VISITS,
                    COUNT(DISTINCT CASE WHEN s.IS_STORE_VISIT = TRUE THEN s.DEVICE_ID_QU END) as LOCATION_VISITS
                FROM QUORUMDB.SEGMENT_DATA.QUORUM_ADV_STORE_VISITS s
                LEFT JOIN QUORUMDB.SEGMENT_DATA.LINE_ITEMS li ON s.LINE_ITEM_ID = li.LINE_ITEM_ID
                WHERE s.IO_ID = %s
                  AND s.AGENCY_ID = %s
                  AND s.IMP_TIMESTAMP >= %s AND s.IMP_TIMESTAMP < %s
                GROUP BY s.LINE_ITEM_ID, li.LINE_ITEM_NAME
                HAVING COUNT(DISTINCT s.AD_IMP_ID) >= 100
                ORDER BY IMPRESSIONS DESC
            """
            cursor.execute(query, (campaign_id, agency_id, start_date, end_date))
            
        elif agency_class == 'B':
            query = """
                SELECT 
                    w.LINE_ITEM_ID,
                    COALESCE(MAX(li.LINE_ITEM_NAME), 'Line Item ' || w.LINE_ITEM_ID) as LINE_ITEM_NAME,
                    SUM(w.IMPRESSIONS) as IMPRESSIONS,
                    0 as WEB_VISITS,
                    SUM(w.VISITORS) as LOCATION_VISITS
                FROM QUORUMDB.SEGMENT_DATA.CAMPAIGN_PERFORMANCE_REPORT_WEEKLY_STATS w
                LEFT JOIN QUORUMDB.SEGMENT_DATA.LINE_ITEMS li ON w.LINE_ITEM_ID = li.LINE_ITEM_ID
                WHERE w.IO_ID = %s
                  AND w.AGENCY_ID = %s
                  AND w.LOG_DATE >= %s AND w.LOG_DATE < %s
                GROUP BY w.LINE_ITEM_ID
                HAVING SUM(w.IMPRESSIONS) >= 100
                ORDER BY IMPRESSIONS DESC
            """
            cursor.execute(query, (campaign_id, agency_id, start_date, end_date))
            
        elif agency_class == 'W':
            query = """
                SELECT 
                    p.LINE_ITEM_ID,
                    COALESCE(li.LINE_ITEM_NAME, 'Line Item ' || p.LINE_ITEM_ID) as LINE_ITEM_NAME,
                    COUNT(*) as IMPRESSIONS,
                    SUM(CASE WHEN p.IS_SITE_VISIT = TRUE THEN 1 ELSE 0 END) as WEB_VISITS,
                    SUM(CASE WHEN p.IS_STORE_VISIT = TRUE THEN 1 ELSE 0 END) as LOCATION_VISITS
                FROM QUORUMDB.SEGMENT_DATA.PARAMOUNT_IMPRESSIONS_REPORT_90_DAYS p
                LEFT JOIN QUORUMDB.SEGMENT_DATA.LINE_ITEMS li ON p.LINE_ITEM_ID = li.LINE_ITEM_ID
                WHERE p.IO_ID = %s
                GROUP BY p.LINE_ITEM_ID, li.LINE_ITEM_NAME
                HAVING COUNT(*) >= 100
                ORDER BY IMPRESSIONS DESC
            """
            cursor.execute(query, (campaign_id,))
        else:
            cursor.close()
            conn.close()
            return jsonify({'success': False, 'error': f'Unknown agency class'})
        
        columns = [desc[0] for desc in cursor.description]
        results = [dict(zip(columns, row)) for row in cursor.fetchall()]
        
        # Add visit rates
        for row in results:
            impressions = row.get('IMPRESSIONS', 0) or 0
            web_visits = row.get('WEB_VISITS', 0) or 0
            loc_visits = row.get('LOCATION_VISITS', 0) or 0
            row['W_VISIT_RATE'] = round((web_visits / impressions * 100), 4) if impressions > 0 else 0
            row['L_VISIT_RATE'] = round((loc_visits / impressions * 100), 4) if impressions > 0 else 0
        
        cursor.close()
        conn.close()
        
        return jsonify({'success': True, 'data': results})
        
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})

# ============================================================================
# PUBLISHER PERFORMANCE - Publisher breakdown
# ============================================================================

@app.route('/api/v5/publisher-performance', methods=['GET'])
def get_publisher_performance_v5():
    """Get publisher-level performance - FIXED FOR CLASS B"""
    try:
        advertiser_id = request.args.get('advertiser_id')
        agency_id = request.args.get('agency_id')
        campaign_id = request.args.get('campaign_id')
        start_date = request.args.get('start_date', (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d'))
        end_date = request.args.get('end_date', datetime.now().strftime('%Y-%m-%d'))
        
        if not advertiser_id or not agency_id:
            return jsonify({'success': False, 'error': 'advertiser_id and agency_id required'})
        
        agency_class = get_agency_class(agency_id)
        conn = get_snowflake_connection()
        cursor = conn.cursor()
        
        if agency_class == 'A':
            query = """
                SELECT 
                    s.PUBLISHER_CODE,
                    COUNT(DISTINCT s.AD_IMP_ID) as IMPRESSIONS,
                    0 as WEB_VISITS,
                    COUNT(DISTINCT CASE WHEN s.IS_STORE_VISIT = TRUE THEN s.DEVICE_ID_QU END) as LOCATION_VISITS
                FROM QUORUMDB.SEGMENT_DATA.QUORUM_ADV_STORE_VISITS s
                WHERE s.QUORUM_ADVERTISER_ID = %s
                  AND s.AGENCY_ID = %s
                  AND s.IMP_TIMESTAMP >= %s AND s.IMP_TIMESTAMP < %s
                  {campaign_filter}
                GROUP BY s.PUBLISHER_CODE
                HAVING COUNT(DISTINCT s.AD_IMP_ID) >= 1000
                ORDER BY IMPRESSIONS DESC
                LIMIT 100
            """
            campaign_filter = f"AND s.IO_ID = '{campaign_id}'" if campaign_id else ""
            query = query.format(campaign_filter=campaign_filter)
            cursor.execute(query, (advertiser_id, agency_id, start_date, end_date))
            
        elif agency_class == 'B':
            # FIXED: Removed nested aggregate
            query = """
                SELECT 
                    w.PUBLISHER as PUBLISHER_CODE,
                    SUM(w.IMPRESSIONS) as IMPRESSIONS,
                    0 as WEB_VISITS,
                    SUM(w.VISITORS) as LOCATION_VISITS
                FROM QUORUMDB.SEGMENT_DATA.CAMPAIGN_PERFORMANCE_REPORT_WEEKLY_STATS w
                WHERE w.ADVERTISER_ID = %s
                  AND w.AGENCY_ID = %s
                  AND w.LOG_DATE >= %s AND w.LOG_DATE < %s
                  {campaign_filter}
                GROUP BY w.PUBLISHER
                HAVING SUM(w.IMPRESSIONS) >= 1000
                ORDER BY IMPRESSIONS DESC
                LIMIT 100
            """
            campaign_filter = f"AND w.IO_ID = '{campaign_id}'" if campaign_id else ""
            query = query.format(campaign_filter=campaign_filter)
            cursor.execute(query, (advertiser_id, agency_id, start_date, end_date))
            
        elif agency_class == 'W':
            query = """
                SELECT 
                    p.PUBLISHER_CODE,
                    COUNT(*) as IMPRESSIONS,
                    SUM(CASE WHEN p.IS_SITE_VISIT = TRUE THEN 1 ELSE 0 END) as WEB_VISITS,
                    SUM(CASE WHEN p.IS_STORE_VISIT = TRUE THEN 1 ELSE 0 END) as LOCATION_VISITS
                FROM QUORUMDB.SEGMENT_DATA.PARAMOUNT_IMPRESSIONS_REPORT_90_DAYS p
                WHERE p.QUORUM_ADVERTISER_ID = %s
                  {campaign_filter}
                GROUP BY p.PUBLISHER_CODE
                HAVING COUNT(*) >= 1000
                ORDER BY IMPRESSIONS DESC
                LIMIT 100
            """
            campaign_filter = f"AND p.IO_ID = '{campaign_id}'" if campaign_id else ""
            query = query.format(campaign_filter=campaign_filter)
            cursor.execute(query, (advertiser_id,))
        else:
            cursor.close()
            conn.close()
            return jsonify({'success': False, 'error': f'Unknown agency class'})
        
        columns = [desc[0] for desc in cursor.description]
        results = [dict(zip(columns, row)) for row in cursor.fetchall()]
        
        # Add visit rates and clean publisher names
        for row in results:
            impressions = row.get('IMPRESSIONS', 0) or 0
            web_visits = row.get('WEB_VISITS', 0) or 0
            loc_visits = row.get('LOCATION_VISITS', 0) or 0
            row['W_VISIT_RATE'] = round((web_visits / impressions * 100), 4) if impressions > 0 else 0
            row['L_VISIT_RATE'] = round((loc_visits / impressions * 100), 4) if impressions > 0 else 0
            # Clean URL-encoded publisher names
            if row.get('PUBLISHER_CODE'):
                try:
                    from urllib.parse import unquote
                    row['PUBLISHER_CODE'] = unquote(unquote(str(row['PUBLISHER_CODE'])))
                except:
                    pass
        
        cursor.close()
        conn.close()
        
        return jsonify({'success': True, 'data': results})
        
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})

# ============================================================================
# CREATIVE PERFORMANCE - Creative breakdown
# ============================================================================

@app.route('/api/v5/creative-performance', methods=['GET'])
def get_creative_performance_v5():
    """Get creative-level performance - FIXED FOR CLASS B"""
    try:
        advertiser_id = request.args.get('advertiser_id')
        agency_id = request.args.get('agency_id')
        campaign_id = request.args.get('campaign_id')
        start_date = request.args.get('start_date', (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d'))
        end_date = request.args.get('end_date', datetime.now().strftime('%Y-%m-%d'))
        
        if not advertiser_id or not agency_id:
            return jsonify({'success': False, 'error': 'advertiser_id and agency_id required'})
        
        agency_class = get_agency_class(agency_id)
        conn = get_snowflake_connection()
        cursor = conn.cursor()
        
        if agency_class == 'A':
            query = """
                SELECT 
                    s.CREATIVE_ID,
                    COALESCE(cr.CREATIVE_NAME, 'Creative ' || s.CREATIVE_ID) as CREATIVE_NAME,
                    COUNT(DISTINCT s.AD_IMP_ID) as IMPRESSIONS,
                    0 as WEB_VISITS,
                    COUNT(DISTINCT CASE WHEN s.IS_STORE_VISIT = TRUE THEN s.DEVICE_ID_QU END) as LOCATION_VISITS
                FROM QUORUMDB.SEGMENT_DATA.QUORUM_ADV_STORE_VISITS s
                LEFT JOIN QUORUMDB.SEGMENT_DATA.CREATIVES cr ON s.CREATIVE_ID = cr.CREATIVE_ID
                WHERE s.QUORUM_ADVERTISER_ID = %s
                  AND s.AGENCY_ID = %s
                  AND s.IMP_TIMESTAMP >= %s AND s.IMP_TIMESTAMP < %s
                  {campaign_filter}
                GROUP BY s.CREATIVE_ID, cr.CREATIVE_NAME
                HAVING COUNT(DISTINCT s.AD_IMP_ID) >= 1000
                ORDER BY IMPRESSIONS DESC
                LIMIT 100
            """
            campaign_filter = f"AND s.IO_ID = '{campaign_id}'" if campaign_id else ""
            query = query.format(campaign_filter=campaign_filter)
            cursor.execute(query, (advertiser_id, agency_id, start_date, end_date))
            
        elif agency_class == 'B':
            # Class B doesn't have creative data in CAMPAIGN_PERFORMANCE_REPORT_WEEKLY_STATS
            # Return empty result with message
            cursor.close()
            conn.close()
            return jsonify({
                'success': True, 
                'data': [],
                'message': 'Creative data not available for this agency type'
            })
            
        elif agency_class == 'W':
            query = """
                SELECT 
                    p.CREATIVE_ID,
                    COALESCE(cr.CREATIVE_NAME, 'Creative ' || p.CREATIVE_ID) as CREATIVE_NAME,
                    COUNT(*) as IMPRESSIONS,
                    SUM(CASE WHEN p.IS_SITE_VISIT = TRUE THEN 1 ELSE 0 END) as WEB_VISITS,
                    SUM(CASE WHEN p.IS_STORE_VISIT = TRUE THEN 1 ELSE 0 END) as LOCATION_VISITS
                FROM QUORUMDB.SEGMENT_DATA.PARAMOUNT_IMPRESSIONS_REPORT_90_DAYS p
                LEFT JOIN QUORUMDB.SEGMENT_DATA.CREATIVES cr ON p.CREATIVE_ID = cr.CREATIVE_ID
                WHERE p.QUORUM_ADVERTISER_ID = %s
                  {campaign_filter}
                GROUP BY p.CREATIVE_ID, cr.CREATIVE_NAME
                HAVING COUNT(*) >= 1000
                ORDER BY IMPRESSIONS DESC
                LIMIT 100
            """
            campaign_filter = f"AND p.IO_ID = '{campaign_id}'" if campaign_id else ""
            query = query.format(campaign_filter=campaign_filter)
            cursor.execute(query, (advertiser_id,))
        else:
            cursor.close()
            conn.close()
            return jsonify({'success': False, 'error': f'Unknown agency class'})
        
        columns = [desc[0] for desc in cursor.description]
        results = [dict(zip(columns, row)) for row in cursor.fetchall()]
        
        # Add visit rates
        for row in results:
            impressions = row.get('IMPRESSIONS', 0) or 0
            web_visits = row.get('WEB_VISITS', 0) or 0
            loc_visits = row.get('LOCATION_VISITS', 0) or 0
            row['W_VISIT_RATE'] = round((web_visits / impressions * 100), 4) if impressions > 0 else 0
            row['L_VISIT_RATE'] = round((loc_visits / impressions * 100), 4) if impressions > 0 else 0
        
        cursor.close()
        conn.close()
        
        return jsonify({'success': True, 'data': results})
        
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})

# ============================================================================
# ZIP PERFORMANCE - Geographic breakdown by ZIP
# ============================================================================

@app.route('/api/v5/zip-performance', methods=['GET'])
def get_zip_performance_v5():
    """Get ZIP code performance - optimized with LIMIT"""
    try:
        advertiser_id = request.args.get('advertiser_id')
        agency_id = request.args.get('agency_id')
        campaign_id = request.args.get('campaign_id')
        start_date = request.args.get('start_date', (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d'))
        end_date = request.args.get('end_date', datetime.now().strftime('%Y-%m-%d'))
        
        if not advertiser_id or not agency_id:
            return jsonify({'success': False, 'error': 'advertiser_id and agency_id required'})
        
        agency_class = get_agency_class(agency_id)
        conn = get_snowflake_connection()
        cursor = conn.cursor()
        
        if agency_class == 'A':
            # Use centroid first, fallback to impression ZIP
            query = """
                SELECT 
                    COALESCE(s.CENTROID_ZIP, s.IMP_ZIP) as ZIP_CODE,
                    COUNT(DISTINCT s.AD_IMP_ID) as IMPRESSIONS,
                    0 as WEB_VISITS,
                    COUNT(DISTINCT CASE WHEN s.IS_STORE_VISIT = TRUE THEN s.DEVICE_ID_QU END) as LOCATION_VISITS
                FROM QUORUMDB.SEGMENT_DATA.QUORUM_ADV_STORE_VISITS s
                WHERE s.QUORUM_ADVERTISER_ID = %s
                  AND s.AGENCY_ID = %s
                  AND s.IMP_TIMESTAMP >= %s AND s.IMP_TIMESTAMP < %s
                  {campaign_filter}
                GROUP BY COALESCE(s.CENTROID_ZIP, s.IMP_ZIP)
                HAVING COUNT(DISTINCT s.AD_IMP_ID) >= 1000
                ORDER BY IMPRESSIONS DESC
                LIMIT 50
            """
            campaign_filter = f"AND s.IO_ID = '{campaign_id}'" if campaign_id else ""
            query = query.format(campaign_filter=campaign_filter)
            cursor.execute(query, (advertiser_id, agency_id, start_date, end_date))
            
        elif agency_class == 'B':
            # Class B uses XANDR log for ZIP data
            query = """
                SELECT 
                    x.GEO_POSTAL_CODE as ZIP_CODE,
                    COUNT(*) as IMPRESSIONS,
                    0 as WEB_VISITS,
                    0 as LOCATION_VISITS
                FROM QUORUMDB.SEGMENT_DATA.XANDR_IMPRESSION_LOG x
                WHERE x.QUORUM_ADVERTISER_ID = %s
                  AND x.LOG_DATE >= %s AND x.LOG_DATE < %s
                  {campaign_filter}
                GROUP BY x.GEO_POSTAL_CODE
                HAVING COUNT(*) >= 1000
                ORDER BY IMPRESSIONS DESC
                LIMIT 50
            """
            campaign_filter = f"AND x.IO_ID = '{campaign_id}'" if campaign_id else ""
            query = query.format(campaign_filter=campaign_filter)
            cursor.execute(query, (advertiser_id, start_date, end_date))
            
        elif agency_class == 'W':
            query = """
                SELECT 
                    COALESCE(p.CENTROID_ZIP, p.IMP_ZIP) as ZIP_CODE,
                    COUNT(*) as IMPRESSIONS,
                    SUM(CASE WHEN p.IS_SITE_VISIT = TRUE THEN 1 ELSE 0 END) as WEB_VISITS,
                    SUM(CASE WHEN p.IS_STORE_VISIT = TRUE THEN 1 ELSE 0 END) as LOCATION_VISITS
                FROM QUORUMDB.SEGMENT_DATA.PARAMOUNT_IMPRESSIONS_REPORT_90_DAYS p
                WHERE p.QUORUM_ADVERTISER_ID = %s
                  {campaign_filter}
                GROUP BY COALESCE(p.CENTROID_ZIP, p.IMP_ZIP)
                HAVING COUNT(*) >= 1000
                ORDER BY IMPRESSIONS DESC
                LIMIT 50
            """
            campaign_filter = f"AND p.IO_ID = '{campaign_id}'" if campaign_id else ""
            query = query.format(campaign_filter=campaign_filter)
            cursor.execute(query, (advertiser_id,))
        else:
            cursor.close()
            conn.close()
            return jsonify({'success': False, 'error': f'Unknown agency class'})
        
        columns = [desc[0] for desc in cursor.description]
        results = [dict(zip(columns, row)) for row in cursor.fetchall()]
        
        # Add visit rates
        for row in results:
            impressions = row.get('IMPRESSIONS', 0) or 0
            web_visits = row.get('WEB_VISITS', 0) or 0
            loc_visits = row.get('LOCATION_VISITS', 0) or 0
            row['W_VISIT_RATE'] = round((web_visits / impressions * 100), 4) if impressions > 0 else 0
            row['L_VISIT_RATE'] = round((loc_visits / impressions * 100), 4) if impressions > 0 else 0
        
        cursor.close()
        conn.close()
        
        return jsonify({'success': True, 'data': results})
        
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})

# ============================================================================
# DMA PERFORMANCE - Geographic breakdown by DMA
# ============================================================================

@app.route('/api/v5/dma-performance', methods=['GET'])
def get_dma_performance_v5():
    """Get DMA performance - optimized with LIMIT"""
    try:
        advertiser_id = request.args.get('advertiser_id')
        agency_id = request.args.get('agency_id')
        campaign_id = request.args.get('campaign_id')
        start_date = request.args.get('start_date', (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d'))
        end_date = request.args.get('end_date', datetime.now().strftime('%Y-%m-%d'))
        
        if not advertiser_id or not agency_id:
            return jsonify({'success': False, 'error': 'advertiser_id and agency_id required'})
        
        agency_class = get_agency_class(agency_id)
        conn = get_snowflake_connection()
        cursor = conn.cursor()
        
        if agency_class == 'A':
            query = """
                SELECT 
                    COALESCE(s.CENTROID_DMA, s.IMP_DMA) as DMA_CODE,
                    COUNT(DISTINCT s.AD_IMP_ID) as IMPRESSIONS,
                    0 as WEB_VISITS,
                    COUNT(DISTINCT CASE WHEN s.IS_STORE_VISIT = TRUE THEN s.DEVICE_ID_QU END) as LOCATION_VISITS
                FROM QUORUMDB.SEGMENT_DATA.QUORUM_ADV_STORE_VISITS s
                WHERE s.QUORUM_ADVERTISER_ID = %s
                  AND s.AGENCY_ID = %s
                  AND s.IMP_TIMESTAMP >= %s AND s.IMP_TIMESTAMP < %s
                  {campaign_filter}
                GROUP BY COALESCE(s.CENTROID_DMA, s.IMP_DMA)
                HAVING COUNT(DISTINCT s.AD_IMP_ID) >= 1000
                ORDER BY IMPRESSIONS DESC
                LIMIT 50
            """
            campaign_filter = f"AND s.IO_ID = '{campaign_id}'" if campaign_id else ""
            query = query.format(campaign_filter=campaign_filter)
            cursor.execute(query, (advertiser_id, agency_id, start_date, end_date))
            
        elif agency_class == 'B':
            query = """
                SELECT 
                    x.GEO_DMA as DMA_CODE,
                    COUNT(*) as IMPRESSIONS,
                    0 as WEB_VISITS,
                    0 as LOCATION_VISITS
                FROM QUORUMDB.SEGMENT_DATA.XANDR_IMPRESSION_LOG x
                WHERE x.QUORUM_ADVERTISER_ID = %s
                  AND x.LOG_DATE >= %s AND x.LOG_DATE < %s
                  {campaign_filter}
                GROUP BY x.GEO_DMA
                HAVING COUNT(*) >= 1000
                ORDER BY IMPRESSIONS DESC
                LIMIT 50
            """
            campaign_filter = f"AND x.IO_ID = '{campaign_id}'" if campaign_id else ""
            query = query.format(campaign_filter=campaign_filter)
            cursor.execute(query, (advertiser_id, start_date, end_date))
            
        elif agency_class == 'W':
            query = """
                SELECT 
                    COALESCE(p.CENTROID_DMA, p.IMP_DMA) as DMA_CODE,
                    COUNT(*) as IMPRESSIONS,
                    SUM(CASE WHEN p.IS_SITE_VISIT = TRUE THEN 1 ELSE 0 END) as WEB_VISITS,
                    SUM(CASE WHEN p.IS_STORE_VISIT = TRUE THEN 1 ELSE 0 END) as LOCATION_VISITS
                FROM QUORUMDB.SEGMENT_DATA.PARAMOUNT_IMPRESSIONS_REPORT_90_DAYS p
                WHERE p.QUORUM_ADVERTISER_ID = %s
                  {campaign_filter}
                GROUP BY COALESCE(p.CENTROID_DMA, p.IMP_DMA)
                HAVING COUNT(*) >= 1000
                ORDER BY IMPRESSIONS DESC
                LIMIT 50
            """
            campaign_filter = f"AND p.IO_ID = '{campaign_id}'" if campaign_id else ""
            query = query.format(campaign_filter=campaign_filter)
            cursor.execute(query, (advertiser_id,))
        else:
            cursor.close()
            conn.close()
            return jsonify({'success': False, 'error': f'Unknown agency class'})
        
        columns = [desc[0] for desc in cursor.description]
        results = [dict(zip(columns, row)) for row in cursor.fetchall()]
        
        # Add visit rates
        for row in results:
            impressions = row.get('IMPRESSIONS', 0) or 0
            web_visits = row.get('WEB_VISITS', 0) or 0
            loc_visits = row.get('LOCATION_VISITS', 0) or 0
            row['W_VISIT_RATE'] = round((web_visits / impressions * 100), 4) if impressions > 0 else 0
            row['L_VISIT_RATE'] = round((loc_visits / impressions * 100), 4) if impressions > 0 else 0
        
        cursor.close()
        conn.close()
        
        return jsonify({'success': True, 'data': results})
        
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})

# ============================================================================
# IMPRESSIONS TIMESERIES - For charts
# ============================================================================

@app.route('/api/v5/impressions-timeseries', methods=['GET'])
def get_impressions_timeseries_v5():
    """Get daily impressions for charts"""
    try:
        agency_id = request.args.get('agency_id')
        advertiser_id = request.args.get('advertiser_id')
        start_date = request.args.get('start_date', (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d'))
        end_date = request.args.get('end_date', datetime.now().strftime('%Y-%m-%d'))
        
        conn = get_snowflake_connection()
        cursor = conn.cursor()
        
        # Agency-level timeseries (top agencies by day)
        if not agency_id and not advertiser_id:
            query = """
                WITH daily_data AS (
                    SELECT 
                        DATE(IMP_TIMESTAMP) as LOG_DATE,
                        AGENCY_ID,
                        COUNT(DISTINCT AD_IMP_ID) as IMPRESSIONS
                    FROM QUORUMDB.SEGMENT_DATA.QUORUM_ADV_STORE_VISITS
                    WHERE IMP_TIMESTAMP >= %s AND IMP_TIMESTAMP < %s
                    GROUP BY DATE(IMP_TIMESTAMP), AGENCY_ID
                    
                    UNION ALL
                    
                    SELECT 
                        LOG_DATE,
                        AGENCY_ID,
                        SUM(IMPRESSIONS) as IMPRESSIONS
                    FROM QUORUMDB.SEGMENT_DATA.CAMPAIGN_PERFORMANCE_REPORT_WEEKLY_STATS
                    WHERE LOG_DATE >= %s AND LOG_DATE < %s
                    GROUP BY LOG_DATE, AGENCY_ID
                ),
                ranked AS (
                    SELECT 
                        d.LOG_DATE,
                        d.AGENCY_ID,
                        COALESCE(aa.AGENCY_NAME, 'Agency ' || d.AGENCY_ID) as ENTITY_NAME,
                        d.IMPRESSIONS,
                        ROW_NUMBER() OVER (PARTITION BY d.LOG_DATE ORDER BY d.IMPRESSIONS DESC) as rn
                    FROM daily_data d
                    LEFT JOIN (
                        SELECT DISTINCT ADVERTISER_ID as AGENCY_ID, AGENCY_NAME 
                        FROM QUORUMDB.SEGMENT_DATA.AGENCY_ADVERTISER
                    ) aa ON d.AGENCY_ID = aa.AGENCY_ID
                )
                SELECT 
                    LOG_DATE,
                    CASE WHEN rn <= 10 THEN ENTITY_NAME ELSE 'Others' END as ENTITY_NAME,
                    SUM(IMPRESSIONS) as IMPRESSIONS
                FROM ranked
                GROUP BY LOG_DATE, CASE WHEN rn <= 10 THEN ENTITY_NAME ELSE 'Others' END
                ORDER BY LOG_DATE, IMPRESSIONS DESC
            """
            cursor.execute(query, (start_date, end_date, start_date, end_date))
            
        # Advertiser-level timeseries for a specific agency
        elif agency_id and not advertiser_id:
            agency_class = get_agency_class(agency_id)
            
            if agency_class == 'A':
                query = """
                    WITH daily_data AS (
                        SELECT 
                            DATE(s.IMP_TIMESTAMP) as LOG_DATE,
                            s.QUORUM_ADVERTISER_ID as ADVERTISER_ID,
                            COALESCE(aa.COMP_NAME, 'Advertiser ' || s.QUORUM_ADVERTISER_ID) as ADVERTISER_NAME,
                            COUNT(DISTINCT s.AD_IMP_ID) as IMPRESSIONS
                        FROM QUORUMDB.SEGMENT_DATA.QUORUM_ADV_STORE_VISITS s
                        LEFT JOIN QUORUMDB.SEGMENT_DATA.AGENCY_ADVERTISER aa ON s.QUORUM_ADVERTISER_ID = aa.ID
                        WHERE s.AGENCY_ID = %s
                          AND s.IMP_TIMESTAMP >= %s AND s.IMP_TIMESTAMP < %s
                        GROUP BY DATE(s.IMP_TIMESTAMP), s.QUORUM_ADVERTISER_ID, aa.COMP_NAME
                    ),
                    ranked AS (
                        SELECT *, ROW_NUMBER() OVER (PARTITION BY LOG_DATE ORDER BY IMPRESSIONS DESC) as rn
                        FROM daily_data
                    )
                    SELECT 
                        LOG_DATE,
                        CASE WHEN rn <= 10 THEN ADVERTISER_NAME ELSE 'Others' END as ENTITY_NAME,
                        SUM(IMPRESSIONS) as IMPRESSIONS
                    FROM ranked
                    GROUP BY LOG_DATE, CASE WHEN rn <= 10 THEN ADVERTISER_NAME ELSE 'Others' END
                    ORDER BY LOG_DATE, IMPRESSIONS DESC
                """
                cursor.execute(query, (agency_id, start_date, end_date))
                
            elif agency_class == 'B':
                query = """
                    WITH daily_data AS (
                        SELECT 
                            w.LOG_DATE,
                            w.ADVERTISER_ID,
                            COALESCE(aa.COMP_NAME, 'Advertiser ' || w.ADVERTISER_ID) as ADVERTISER_NAME,
                            SUM(w.IMPRESSIONS) as IMPRESSIONS
                        FROM QUORUMDB.SEGMENT_DATA.CAMPAIGN_PERFORMANCE_REPORT_WEEKLY_STATS w
                        LEFT JOIN QUORUMDB.SEGMENT_DATA.AGENCY_ADVERTISER aa ON w.ADVERTISER_ID = aa.ID
                        WHERE w.AGENCY_ID = %s
                          AND w.LOG_DATE >= %s AND w.LOG_DATE < %s
                        GROUP BY w.LOG_DATE, w.ADVERTISER_ID, aa.COMP_NAME
                    ),
                    ranked AS (
                        SELECT *, ROW_NUMBER() OVER (PARTITION BY LOG_DATE ORDER BY IMPRESSIONS DESC) as rn
                        FROM daily_data
                    )
                    SELECT 
                        LOG_DATE,
                        CASE WHEN rn <= 10 THEN ADVERTISER_NAME ELSE 'Others' END as ENTITY_NAME,
                        SUM(IMPRESSIONS) as IMPRESSIONS
                    FROM ranked
                    GROUP BY LOG_DATE, CASE WHEN rn <= 10 THEN ADVERTISER_NAME ELSE 'Others' END
                    ORDER BY LOG_DATE, IMPRESSIONS DESC
                """
                cursor.execute(query, (agency_id, start_date, end_date))
                
            else:
                cursor.close()
                conn.close()
                return jsonify({'success': True, 'data': []})
        else:
            cursor.close()
            conn.close()
            return jsonify({'success': True, 'data': []})
        
        columns = [desc[0] for desc in cursor.description]
        results = [dict(zip(columns, row)) for row in cursor.fetchall()]
        
        # Convert dates to strings
        for row in results:
            if row.get('LOG_DATE'):
                row['LOG_DATE'] = str(row['LOG_DATE'])
        
        cursor.close()
        conn.close()
        
        return jsonify({'success': True, 'data': results})
        
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})

# ============================================================================
# BACKWARD COMPATIBILITY ROUTES
# ============================================================================

@app.route('/api/v3/agencies', methods=['GET'])
def get_agencies_v3():
    return get_agency_overview_v5()

@app.route('/api/v3/agency-overview', methods=['GET'])
def get_agency_overview_v3():
    return get_agency_overview_v5()

@app.route('/api/v3/advertisers', methods=['GET'])
def get_advertisers_v3():
    return get_advertiser_overview_v5()

@app.route('/api/v3/advertiser-overview', methods=['GET'])
def get_advertiser_overview_v3():
    return get_advertiser_overview_v5()

@app.route('/api/v3/summary', methods=['GET'])
def get_summary_v3():
    return get_advertiser_summary_v5()

@app.route('/api/v3/campaign-performance', methods=['GET'])
def get_campaign_performance_v3():
    return get_campaign_performance_v5()

@app.route('/api/v3/publisher-performance', methods=['GET'])
def get_publisher_performance_v3():
    return get_publisher_performance_v5()

@app.route('/api/v3/creative-performance', methods=['GET'])
def get_creative_performance_v3():
    return get_creative_performance_v5()

# ============================================================================
# RUN
# ============================================================================

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=False)
