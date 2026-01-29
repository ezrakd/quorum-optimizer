"""
Quorum Optimizer API v5 - Simplified Architecture

Data Sources:
- QIR (QUORUM_IMPRESSIONS_REPORT): Row-level data for MNTN, Dealer Spike, InteractRV, ARI, Level5, ByRider
- CPRS (CAMPAIGN_PERFORMANCE_REPORT_WEEKLY_STATS): Pre-aggregated for Causal iQ, Hearst, Magnite, Parallel Path, Publicis, The Shipyard
- PARAMOUNT: Custom tables for ViacomCBS/Paramount

Agency -> Source mapping is determined by checking which table has data for that agency.
"""

from flask import Flask, jsonify, request
from flask_cors import CORS
import snowflake.connector
import os
from datetime import datetime, timedelta

app = Flask(__name__)
CORS(app)

# Snowflake connection
def get_snowflake_connection():
    return snowflake.connector.connect(
        user=os.environ.get('SNOWFLAKE_USER'),
        password=os.environ.get('SNOWFLAKE_PASSWORD'),
        account=os.environ.get('SNOWFLAKE_ACCOUNT'),
        warehouse=os.environ.get('SNOWFLAKE_WAREHOUSE', 'COMPUTE_WH'),
        database='QUORUMDB',
        schema='SEGMENT_DATA'
    )

# ============================================================================
# AGENCY SOURCE DETECTION
# ============================================================================

# Static mapping based on which tables have data (from analysis)
# QIR = QUORUM_IMPRESSIONS_REPORT (row-level, has IS_STORE_VISIT, IS_SITE_VISIT)
# CPRS = CAMPAIGN_PERFORMANCE_REPORT_WEEKLY_STATS (pre-aggregated weekly)
# PARAMOUNT = Custom Paramount tables

QIR_AGENCIES = {2514, 1956, 2298, 2086, 1955, 1950}  # MNTN, Dealer Spike, InteractRV, Level5, ARI, ByRider
CPRS_AGENCIES = {1813, 1972, 2234, 2744, 1445, 2379, 2691, 1880}  # Causal iQ, Hearst, Magnite, Parallel Path, Publicis, The Shipyard, TravelSpike, TeamSnap
PARAMOUNT_AGENCY = 1480

def get_agency_source(agency_id):
    """Determine which data source to use for an agency"""
    agency_id = int(agency_id)
    if agency_id == PARAMOUNT_AGENCY:
        return 'PARAMOUNT'
    elif agency_id in QIR_AGENCIES:
        return 'QIR'
    elif agency_id in CPRS_AGENCIES:
        return 'CPRS'
    else:
        # Default to CPRS for unknown agencies
        return 'CPRS'

# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def parse_dates(request):
    """Parse start_date and end_date from request, default to last 30 days"""
    end_date = request.args.get('end_date', datetime.now().strftime('%Y-%m-%d'))
    start_date = request.args.get('start_date', (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d'))
    return start_date, end_date

def safe_divide(numerator, denominator, multiplier=100, decimals=4):
    """Safely divide and return percentage"""
    if denominator and denominator > 0:
        return round((numerator or 0) / denominator * multiplier, decimals)
    return 0

# ============================================================================
# V5 ENDPOINTS
# ============================================================================

@app.route('/api/v5/agencies', methods=['GET'])
def get_agencies():
    """Get list of all active agencies with summary stats"""
    try:
        conn = get_snowflake_connection()
        cursor = conn.cursor()
        
        # Get agencies from QIR
        query_qir = """
            SELECT 
                q.AGENCY_ID,
                MAX(aa.AGENCY_NAME) as AGENCY_NAME,
                COUNT(DISTINCT q.QUORUM_ADVERTISER_ID) as ADVERTISER_COUNT,
                SUM(1) as IMPRESSIONS
            FROM QUORUMDB.SEGMENT_DATA.QUORUM_IMPRESSIONS_REPORT q
            LEFT JOIN QUORUMDB.SEGMENT_DATA.AGENCY_ADVERTISER aa ON q.AGENCY_ID = aa.ADVERTISER_ID
            WHERE q.LOG_DATE >= DATEADD(day, -30, CURRENT_DATE())
            GROUP BY q.AGENCY_ID
        """
        cursor.execute(query_qir)
        qir_results = cursor.fetchall()
        
        # Get agencies from CPRS
        query_cprs = """
            SELECT 
                w.AGENCY_ID,
                MAX(aa.AGENCY_NAME) as AGENCY_NAME,
                COUNT(DISTINCT w.ADVERTISER_ID) as ADVERTISER_COUNT,
                SUM(w.IMPRESSIONS) as IMPRESSIONS
            FROM QUORUMDB.SEGMENT_DATA.CAMPAIGN_PERFORMANCE_REPORT_WEEKLY_STATS w
            LEFT JOIN QUORUMDB.SEGMENT_DATA.AGENCY_ADVERTISER aa ON w.AGENCY_ID = aa.ADVERTISER_ID
            WHERE w.LOG_DATE >= DATEADD(day, -30, CURRENT_DATE())
              AND w.AGENCY_ID NOT IN (SELECT DISTINCT AGENCY_ID FROM QUORUMDB.SEGMENT_DATA.QUORUM_IMPRESSIONS_REPORT WHERE LOG_DATE >= DATEADD(day, -30, CURRENT_DATE()))
            GROUP BY w.AGENCY_ID
        """
        cursor.execute(query_cprs)
        cprs_results = cursor.fetchall()
        
        # Get Paramount
        query_paramount = """
            SELECT 
                1480 as AGENCY_ID,
                'ViacomCBS / Paramount' as AGENCY_NAME,
                COUNT(DISTINCT QUORUM_ADVERTISER_ID) as ADVERTISER_COUNT,
                COUNT(*) as IMPRESSIONS
            FROM QUORUMDB.SEGMENT_DATA.PARAMOUNT_IMPRESSIONS_REPORT_90_DAYS
        """
        cursor.execute(query_paramount)
        paramount_results = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        # Combine results
        agencies = []
        for row in qir_results + cprs_results + paramount_results:
            agencies.append({
                'AGENCY_ID': row[0],
                'AGENCY_NAME': row[1] or f'Agency {row[0]}',
                'ADVERTISER_COUNT': row[2],
                'IMPRESSIONS': row[3]
            })
        
        # Sort by impressions descending
        agencies.sort(key=lambda x: x['IMPRESSIONS'] or 0, reverse=True)
        
        return jsonify({'success': True, 'data': agencies})
        
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/v5/advertiser-overview', methods=['GET'])
def get_advertiser_overview():
    """Get all advertisers for an agency with summary metrics"""
    try:
        agency_id = request.args.get('agency_id')
        if not agency_id:
            return jsonify({'success': False, 'error': 'agency_id required'}), 400
            
        start_date, end_date = parse_dates(request)
        source = get_agency_source(agency_id)
        
        conn = get_snowflake_connection()
        cursor = conn.cursor()
        
        if source == 'QIR':
            query = """
                SELECT 
                    q.QUORUM_ADVERTISER_ID as ADVERTISER_ID,
                    MAX(aa.COMP_NAME) as ADVERTISER_NAME,
                    COUNT(*) as IMPRESSIONS,
                    SUM(CASE WHEN q.IS_STORE_VISIT = TRUE THEN 1 ELSE 0 END) as LOCATION_VISITS,
                    SUM(CASE WHEN q.IS_SITE_VISIT = TRUE THEN 1 ELSE 0 END) as WEB_VISITS
                FROM QUORUMDB.SEGMENT_DATA.QUORUM_IMPRESSIONS_REPORT q
                LEFT JOIN QUORUMDB.SEGMENT_DATA.AGENCY_ADVERTISER aa ON q.QUORUM_ADVERTISER_ID = aa.ID
                WHERE q.AGENCY_ID = %s
                  AND q.LOG_DATE >= %s AND q.LOG_DATE < %s
                GROUP BY q.QUORUM_ADVERTISER_ID
                HAVING COUNT(*) >= 1000
                ORDER BY IMPRESSIONS DESC
            """
            cursor.execute(query, (agency_id, start_date, end_date))
            
        elif source == 'CPRS':
            query = """
                SELECT 
                    w.ADVERTISER_ID,
                    MAX(aa.COMP_NAME) as ADVERTISER_NAME,
                    SUM(w.IMPRESSIONS) as IMPRESSIONS,
                    SUM(w.VISITORS) as LOCATION_VISITS,
                    0 as WEB_VISITS
                FROM QUORUMDB.SEGMENT_DATA.CAMPAIGN_PERFORMANCE_REPORT_WEEKLY_STATS w
                LEFT JOIN QUORUMDB.SEGMENT_DATA.AGENCY_ADVERTISER aa ON w.ADVERTISER_ID = aa.ID
                WHERE w.AGENCY_ID = %s
                  AND w.LOG_DATE >= %s AND w.LOG_DATE < %s
                GROUP BY w.ADVERTISER_ID
                HAVING SUM(w.IMPRESSIONS) >= 1000
                ORDER BY IMPRESSIONS DESC
            """
            cursor.execute(query, (agency_id, start_date, end_date))
            
        elif source == 'PARAMOUNT':
            # Get impressions from PARAMOUNT table
            query_imps = """
                SELECT 
                    p.QUORUM_ADVERTISER_ID as ADVERTISER_ID,
                    MAX(aa.COMP_NAME) as ADVERTISER_NAME,
                    COUNT(*) as IMPRESSIONS
                FROM QUORUMDB.SEGMENT_DATA.PARAMOUNT_IMPRESSIONS_REPORT_90_DAYS p
                LEFT JOIN QUORUMDB.SEGMENT_DATA.AGENCY_ADVERTISER aa ON p.QUORUM_ADVERTISER_ID = aa.ID
                GROUP BY p.QUORUM_ADVERTISER_ID
                HAVING COUNT(*) >= 10000
            """
            cursor.execute(query_imps)
            imp_rows = {str(int(row[0])): {'ADVERTISER_ID': row[0], 'ADVERTISER_NAME': row[1], 'IMPRESSIONS': row[2]} for row in cursor.fetchall()}
            
            # Get web visits from WEB_VISITORS_TO_LOG
            query_visits = """
                SELECT 
                    QUORUM_ADVERTISER_ID,
                    COUNT(DISTINCT WEB_IMPRESSION_ID) as WEB_VISITS
                FROM QUORUMDB.SEGMENT_DATA.WEB_VISITORS_TO_LOG
                WHERE SITE_VISIT_TIMESTAMP >= %s AND SITE_VISIT_TIMESTAMP < %s
                GROUP BY QUORUM_ADVERTISER_ID
            """
            cursor.execute(query_visits, (start_date, end_date))
            
            for row in cursor.fetchall():
                adv_id = str(row[0])
                if adv_id in imp_rows:
                    imp_rows[adv_id]['WEB_VISITS'] = row[1]
            
            cursor.close()
            conn.close()
            
            results = []
            for adv_id, data in imp_rows.items():
                impressions = data.get('IMPRESSIONS', 0) or 0
                web_visits = data.get('WEB_VISITS', 0) or 0
                results.append({
                    'ADVERTISER_ID': data['ADVERTISER_ID'],
                    'ADVERTISER_NAME': data['ADVERTISER_NAME'],
                    'IMPRESSIONS': impressions,
                    'LOCATION_VISITS': 0,
                    'WEB_VISITS': web_visits,
                    'L_VISIT_RATE': 0,
                    'W_VISIT_RATE': safe_divide(web_visits, impressions)
                })
            
            results.sort(key=lambda x: x['IMPRESSIONS'], reverse=True)
            return jsonify({'success': True, 'data': results})
        
        # Process QIR/CPRS results
        results = []
        for row in cursor.fetchall():
            impressions = row[2] or 0
            location_visits = row[3] or 0
            web_visits = row[4] or 0
            results.append({
                'ADVERTISER_ID': row[0],
                'ADVERTISER_NAME': row[1],
                'IMPRESSIONS': impressions,
                'LOCATION_VISITS': location_visits,
                'WEB_VISITS': web_visits,
                'L_VISIT_RATE': safe_divide(location_visits, impressions),
                'W_VISIT_RATE': safe_divide(web_visits, impressions)
            })
        
        cursor.close()
        conn.close()
        return jsonify({'success': True, 'data': results})
        
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/v5/advertiser-summary', methods=['GET'])
def get_advertiser_summary():
    """Get summary metrics for a specific advertiser"""
    try:
        agency_id = request.args.get('agency_id')
        advertiser_id = request.args.get('advertiser_id')
        if not agency_id or not advertiser_id:
            return jsonify({'success': False, 'error': 'agency_id and advertiser_id required'}), 400
            
        start_date, end_date = parse_dates(request)
        source = get_agency_source(agency_id)
        
        conn = get_snowflake_connection()
        cursor = conn.cursor()
        
        if source == 'QIR':
            query = """
                SELECT 
                    COUNT(*) as IMPRESSIONS,
                    SUM(CASE WHEN IS_STORE_VISIT = TRUE THEN 1 ELSE 0 END) as LOCATION_VISITS,
                    SUM(CASE WHEN IS_SITE_VISIT = TRUE THEN 1 ELSE 0 END) as WEB_VISITS,
                    COUNT(DISTINCT IO_ID) as CAMPAIGN_COUNT,
                    COUNT(DISTINCT PUBLISHER_CODE) as PUBLISHER_COUNT,
                    COUNT(DISTINCT CREATIVE_ID) as CREATIVE_COUNT
                FROM QUORUMDB.SEGMENT_DATA.QUORUM_IMPRESSIONS_REPORT
                WHERE QUORUM_ADVERTISER_ID = %s
                  AND AGENCY_ID = %s
                  AND LOG_DATE >= %s AND LOG_DATE < %s
            """
            cursor.execute(query, (advertiser_id, agency_id, start_date, end_date))
            
        elif source == 'CPRS':
            query = """
                SELECT 
                    SUM(IMPRESSIONS) as IMPRESSIONS,
                    SUM(VISITORS) as LOCATION_VISITS,
                    0 as WEB_VISITS,
                    COUNT(DISTINCT IO_ID) as CAMPAIGN_COUNT,
                    COUNT(DISTINCT PUBLISHER) as PUBLISHER_COUNT,
                    0 as CREATIVE_COUNT
                FROM QUORUMDB.SEGMENT_DATA.CAMPAIGN_PERFORMANCE_REPORT_WEEKLY_STATS
                WHERE ADVERTISER_ID = %s
                  AND AGENCY_ID = %s
                  AND LOG_DATE >= %s AND LOG_DATE < %s
            """
            cursor.execute(query, (advertiser_id, agency_id, start_date, end_date))
            
        elif source == 'PARAMOUNT':
            # Get impression stats
            query_imps = """
                SELECT 
                    COUNT(*) as IMPRESSIONS,
                    COUNT(DISTINCT IO_ID) as CAMPAIGN_COUNT,
                    COUNT(DISTINCT PUBLISHER_CODE) as PUBLISHER_COUNT,
                    COUNT(DISTINCT CREATIVE_ID) as CREATIVE_COUNT
                FROM QUORUMDB.SEGMENT_DATA.PARAMOUNT_IMPRESSIONS_REPORT_90_DAYS
                WHERE QUORUM_ADVERTISER_ID = %s
            """
            cursor.execute(query_imps, (advertiser_id,))
            imp_row = cursor.fetchone()
            
            # Get web visits
            query_visits = """
                SELECT COUNT(DISTINCT WEB_IMPRESSION_ID) as WEB_VISITS
                FROM QUORUMDB.SEGMENT_DATA.WEB_VISITORS_TO_LOG
                WHERE QUORUM_ADVERTISER_ID = %s
                  AND SITE_VISIT_TIMESTAMP >= %s AND SITE_VISIT_TIMESTAMP < %s
            """
            cursor.execute(query_visits, (advertiser_id, start_date, end_date))
            visit_row = cursor.fetchone()
            
            cursor.close()
            conn.close()
            
            impressions = imp_row[0] if imp_row else 0
            web_visits = visit_row[0] if visit_row else 0
            
            return jsonify({'success': True, 'data': {
                'IMPRESSIONS': impressions,
                'LOCATION_VISITS': 0,
                'WEB_VISITS': web_visits,
                'CAMPAIGN_COUNT': imp_row[1] if imp_row else 0,
                'PUBLISHER_COUNT': imp_row[2] if imp_row else 0,
                'CREATIVE_COUNT': imp_row[3] if imp_row else 0,
                'L_VISIT_RATE': 0,
                'W_VISIT_RATE': safe_divide(web_visits, impressions)
            }})
        
        row = cursor.fetchone()
        cursor.close()
        conn.close()
        
        if not row:
            return jsonify({'success': True, 'data': {}})
        
        impressions = row[0] or 0
        location_visits = row[1] or 0
        web_visits = row[2] or 0
        
        return jsonify({'success': True, 'data': {
            'IMPRESSIONS': impressions,
            'LOCATION_VISITS': location_visits,
            'WEB_VISITS': web_visits,
            'CAMPAIGN_COUNT': row[3] or 0,
            'PUBLISHER_COUNT': row[4] or 0,
            'CREATIVE_COUNT': row[5] or 0,
            'L_VISIT_RATE': safe_divide(location_visits, impressions),
            'W_VISIT_RATE': safe_divide(web_visits, impressions)
        }})
        
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/v5/campaign-performance', methods=['GET'])
def get_campaign_performance():
    """Get campaign-level performance for an advertiser"""
    try:
        agency_id = request.args.get('agency_id')
        advertiser_id = request.args.get('advertiser_id')
        if not agency_id or not advertiser_id:
            return jsonify({'success': False, 'error': 'agency_id and advertiser_id required'}), 400
            
        start_date, end_date = parse_dates(request)
        source = get_agency_source(agency_id)
        
        conn = get_snowflake_connection()
        cursor = conn.cursor()
        
        if source == 'QIR':
            query = """
                SELECT 
                    IO_ID,
                    MAX(IO_NAME) as CAMPAIGN_NAME,
                    COUNT(*) as IMPRESSIONS,
                    SUM(CASE WHEN IS_STORE_VISIT = TRUE THEN 1 ELSE 0 END) as LOCATION_VISITS,
                    SUM(CASE WHEN IS_SITE_VISIT = TRUE THEN 1 ELSE 0 END) as WEB_VISITS
                FROM QUORUMDB.SEGMENT_DATA.QUORUM_IMPRESSIONS_REPORT
                WHERE QUORUM_ADVERTISER_ID = %s
                  AND AGENCY_ID = %s
                  AND LOG_DATE >= %s AND LOG_DATE < %s
                GROUP BY IO_ID
                HAVING COUNT(*) >= 100
                ORDER BY IMPRESSIONS DESC
            """
            cursor.execute(query, (advertiser_id, agency_id, start_date, end_date))
            
        elif source == 'CPRS':
            query = """
                SELECT 
                    IO_ID,
                    MAX(IO_NAME) as CAMPAIGN_NAME,
                    SUM(IMPRESSIONS) as IMPRESSIONS,
                    SUM(VISITORS) as LOCATION_VISITS,
                    0 as WEB_VISITS
                FROM QUORUMDB.SEGMENT_DATA.CAMPAIGN_PERFORMANCE_REPORT_WEEKLY_STATS
                WHERE ADVERTISER_ID = %s
                  AND AGENCY_ID = %s
                  AND LOG_DATE >= %s AND LOG_DATE < %s
                GROUP BY IO_ID
                HAVING SUM(IMPRESSIONS) >= 100
                ORDER BY IMPRESSIONS DESC
            """
            cursor.execute(query, (advertiser_id, agency_id, start_date, end_date))
            
        elif source == 'PARAMOUNT':
            query = """
                SELECT 
                    IO_ID,
                    MAX(IO_NAME) as CAMPAIGN_NAME,
                    COUNT(*) as IMPRESSIONS,
                    0 as LOCATION_VISITS,
                    0 as WEB_VISITS
                FROM QUORUMDB.SEGMENT_DATA.PARAMOUNT_IMPRESSIONS_REPORT_90_DAYS
                WHERE QUORUM_ADVERTISER_ID = %s
                GROUP BY IO_ID
                HAVING COUNT(*) >= 100
                ORDER BY IMPRESSIONS DESC
            """
            cursor.execute(query, (advertiser_id,))
        
        results = []
        for row in cursor.fetchall():
            impressions = row[2] or 0
            location_visits = row[3] or 0
            web_visits = row[4] or 0
            results.append({
                'CAMPAIGN_ID': row[0],
                'CAMPAIGN_NAME': row[1],
                'IMPRESSIONS': impressions,
                'LOCATION_VISITS': location_visits,
                'WEB_VISITS': web_visits,
                'L_VISIT_RATE': safe_divide(location_visits, impressions),
                'W_VISIT_RATE': safe_divide(web_visits, impressions)
            })
        
        cursor.close()
        conn.close()
        return jsonify({'success': True, 'data': results})
        
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/v5/line-item-performance', methods=['GET'])
def get_line_item_performance():
    """Get line item performance for a campaign"""
    try:
        agency_id = request.args.get('agency_id')
        campaign_id = request.args.get('campaign_id')
        if not agency_id or not campaign_id:
            return jsonify({'success': False, 'error': 'agency_id and campaign_id required'}), 400
            
        start_date, end_date = parse_dates(request)
        source = get_agency_source(agency_id)
        
        conn = get_snowflake_connection()
        cursor = conn.cursor()
        
        if source == 'QIR':
            query = """
                SELECT 
                    LINEITEM_ID as LINE_ITEM_ID,
                    MAX(LINEITEM_NAME) as LINE_ITEM_NAME,
                    COUNT(*) as IMPRESSIONS,
                    SUM(CASE WHEN IS_STORE_VISIT = TRUE THEN 1 ELSE 0 END) as LOCATION_VISITS,
                    SUM(CASE WHEN IS_SITE_VISIT = TRUE THEN 1 ELSE 0 END) as WEB_VISITS
                FROM QUORUMDB.SEGMENT_DATA.QUORUM_IMPRESSIONS_REPORT
                WHERE IO_ID = %s
                  AND AGENCY_ID = %s
                  AND LOG_DATE >= %s AND LOG_DATE < %s
                GROUP BY LINEITEM_ID
                HAVING COUNT(*) >= 10
                ORDER BY IMPRESSIONS DESC
            """
            cursor.execute(query, (campaign_id, agency_id, start_date, end_date))
            
        elif source == 'CPRS':
            query = """
                SELECT 
                    LI_ID as LINE_ITEM_ID,
                    MAX(LI_NAME) as LINE_ITEM_NAME,
                    SUM(IMPRESSIONS) as IMPRESSIONS,
                    SUM(VISITORS) as LOCATION_VISITS,
                    0 as WEB_VISITS
                FROM QUORUMDB.SEGMENT_DATA.CAMPAIGN_PERFORMANCE_REPORT_WEEKLY_STATS
                WHERE IO_ID = %s
                  AND AGENCY_ID = %s
                  AND LOG_DATE >= %s AND LOG_DATE < %s
                GROUP BY LI_ID
                HAVING SUM(IMPRESSIONS) >= 10
                ORDER BY IMPRESSIONS DESC
            """
            cursor.execute(query, (campaign_id, agency_id, start_date, end_date))
            
        elif source == 'PARAMOUNT':
            query = """
                SELECT 
                    LINEITEM_ID as LINE_ITEM_ID,
                    MAX(LINEITEM_NAME) as LINE_ITEM_NAME,
                    COUNT(*) as IMPRESSIONS,
                    0 as LOCATION_VISITS,
                    0 as WEB_VISITS
                FROM QUORUMDB.SEGMENT_DATA.PARAMOUNT_IMPRESSIONS_REPORT_90_DAYS
                WHERE IO_ID = %s
                GROUP BY LINEITEM_ID
                HAVING COUNT(*) >= 10
                ORDER BY IMPRESSIONS DESC
            """
            cursor.execute(query, (campaign_id,))
        
        results = []
        for row in cursor.fetchall():
            impressions = row[2] or 0
            location_visits = row[3] or 0
            web_visits = row[4] or 0
            results.append({
                'LINE_ITEM_ID': row[0],
                'LINE_ITEM_NAME': row[1],
                'IMPRESSIONS': impressions,
                'LOCATION_VISITS': location_visits,
                'WEB_VISITS': web_visits,
                'L_VISIT_RATE': safe_divide(location_visits, impressions),
                'W_VISIT_RATE': safe_divide(web_visits, impressions)
            })
        
        cursor.close()
        conn.close()
        return jsonify({'success': True, 'data': results})
        
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/v5/creative-performance', methods=['GET'])
def get_creative_performance():
    """Get creative performance for an advertiser, optionally filtered by campaign"""
    try:
        agency_id = request.args.get('agency_id')
        advertiser_id = request.args.get('advertiser_id')
        campaign_id = request.args.get('campaign_id')
        if not agency_id or not advertiser_id:
            return jsonify({'success': False, 'error': 'agency_id and advertiser_id required'}), 400
            
        start_date, end_date = parse_dates(request)
        source = get_agency_source(agency_id)
        
        conn = get_snowflake_connection()
        cursor = conn.cursor()
        
        if source == 'QIR':
            campaign_filter = "AND IO_ID = %s" if campaign_id else ""
            query = f"""
                SELECT 
                    CREATIVE_ID,
                    MAX(CREATIVE_NAME) as CREATIVE_NAME,
                    COUNT(*) as IMPRESSIONS,
                    SUM(CASE WHEN IS_STORE_VISIT = TRUE THEN 1 ELSE 0 END) as LOCATION_VISITS,
                    SUM(CASE WHEN IS_SITE_VISIT = TRUE THEN 1 ELSE 0 END) as WEB_VISITS
                FROM QUORUMDB.SEGMENT_DATA.QUORUM_IMPRESSIONS_REPORT
                WHERE QUORUM_ADVERTISER_ID = %s
                  AND AGENCY_ID = %s
                  AND LOG_DATE >= %s AND LOG_DATE < %s
                  {campaign_filter}
                GROUP BY CREATIVE_ID
                HAVING COUNT(*) >= 100
                ORDER BY IMPRESSIONS DESC
            """
            params = (advertiser_id, agency_id, start_date, end_date, campaign_id) if campaign_id else (advertiser_id, agency_id, start_date, end_date)
            cursor.execute(query, params)
            
        elif source == 'CPRS':
            # CPRS doesn't have creative-level data
            cursor.close()
            conn.close()
            return jsonify({'success': True, 'data': [], 'message': 'Creative data not available for this agency'})
            
        elif source == 'PARAMOUNT':
            campaign_filter = "AND IO_ID = %s" if campaign_id else ""
            query = f"""
                SELECT 
                    CREATIVE_ID,
                    MAX(CREATIVE_NAME) as CREATIVE_NAME,
                    COUNT(*) as IMPRESSIONS,
                    0 as LOCATION_VISITS,
                    0 as WEB_VISITS
                FROM QUORUMDB.SEGMENT_DATA.PARAMOUNT_IMPRESSIONS_REPORT_90_DAYS
                WHERE QUORUM_ADVERTISER_ID = %s
                  {campaign_filter}
                GROUP BY CREATIVE_ID
                HAVING COUNT(*) >= 100
                ORDER BY IMPRESSIONS DESC
            """
            params = (advertiser_id, campaign_id) if campaign_id else (advertiser_id,)
            cursor.execute(query, params)
        
        results = []
        for row in cursor.fetchall():
            impressions = row[2] or 0
            location_visits = row[3] or 0
            web_visits = row[4] or 0
            results.append({
                'CREATIVE_ID': row[0],
                'CREATIVE_NAME': row[1],
                'IMPRESSIONS': impressions,
                'LOCATION_VISITS': location_visits,
                'WEB_VISITS': web_visits,
                'L_VISIT_RATE': safe_divide(location_visits, impressions),
                'W_VISIT_RATE': safe_divide(web_visits, impressions)
            })
        
        cursor.close()
        conn.close()
        return jsonify({'success': True, 'data': results})
        
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/v5/publisher-performance', methods=['GET'])
def get_publisher_performance():
    """Get publisher/contextual performance for an advertiser"""
    try:
        agency_id = request.args.get('agency_id')
        advertiser_id = request.args.get('advertiser_id')
        campaign_id = request.args.get('campaign_id')
        if not agency_id or not advertiser_id:
            return jsonify({'success': False, 'error': 'agency_id and advertiser_id required'}), 400
            
        start_date, end_date = parse_dates(request)
        source = get_agency_source(agency_id)
        
        conn = get_snowflake_connection()
        cursor = conn.cursor()
        
        if source == 'QIR':
            campaign_filter = "AND IO_ID = %s" if campaign_id else ""
            query = f"""
                SELECT 
                    PUBLISHER_CODE as PUBLISHER,
                    COUNT(*) as IMPRESSIONS,
                    SUM(CASE WHEN IS_STORE_VISIT = TRUE THEN 1 ELSE 0 END) as LOCATION_VISITS,
                    SUM(CASE WHEN IS_SITE_VISIT = TRUE THEN 1 ELSE 0 END) as WEB_VISITS
                FROM QUORUMDB.SEGMENT_DATA.QUORUM_IMPRESSIONS_REPORT
                WHERE QUORUM_ADVERTISER_ID = %s
                  AND AGENCY_ID = %s
                  AND LOG_DATE >= %s AND LOG_DATE < %s
                  {campaign_filter}
                GROUP BY PUBLISHER_CODE
                HAVING COUNT(*) >= 100
                ORDER BY IMPRESSIONS DESC
                LIMIT 100
            """
            params = (advertiser_id, agency_id, start_date, end_date, campaign_id) if campaign_id else (advertiser_id, agency_id, start_date, end_date)
            cursor.execute(query, params)
            
        elif source == 'CPRS':
            campaign_filter = "AND IO_ID = %s" if campaign_id else ""
            query = f"""
                SELECT 
                    PUBLISHER,
                    SUM(IMPRESSIONS) as IMPRESSIONS,
                    SUM(VISITORS) as LOCATION_VISITS,
                    0 as WEB_VISITS
                FROM QUORUMDB.SEGMENT_DATA.CAMPAIGN_PERFORMANCE_REPORT_WEEKLY_STATS
                WHERE ADVERTISER_ID = %s
                  AND AGENCY_ID = %s
                  AND LOG_DATE >= %s AND LOG_DATE < %s
                  {campaign_filter}
                GROUP BY PUBLISHER
                HAVING SUM(IMPRESSIONS) >= 100
                ORDER BY IMPRESSIONS DESC
                LIMIT 100
            """
            params = (advertiser_id, agency_id, start_date, end_date, campaign_id) if campaign_id else (advertiser_id, agency_id, start_date, end_date)
            cursor.execute(query, params)
            
        elif source == 'PARAMOUNT':
            campaign_filter = "AND IO_ID = %s" if campaign_id else ""
            query = f"""
                SELECT 
                    PUBLISHER_CODE as PUBLISHER,
                    COUNT(*) as IMPRESSIONS,
                    0 as LOCATION_VISITS,
                    0 as WEB_VISITS
                FROM QUORUMDB.SEGMENT_DATA.PARAMOUNT_IMPRESSIONS_REPORT_90_DAYS
                WHERE QUORUM_ADVERTISER_ID = %s
                  {campaign_filter}
                GROUP BY PUBLISHER_CODE
                HAVING COUNT(*) >= 100
                ORDER BY IMPRESSIONS DESC
                LIMIT 100
            """
            params = (advertiser_id, campaign_id) if campaign_id else (advertiser_id,)
            cursor.execute(query, params)
        
        results = []
        for row in cursor.fetchall():
            impressions = row[1] or 0
            location_visits = row[2] or 0
            web_visits = row[3] or 0
            results.append({
                'PUBLISHER': row[0],
                'IMPRESSIONS': impressions,
                'LOCATION_VISITS': location_visits,
                'WEB_VISITS': web_visits,
                'L_VISIT_RATE': safe_divide(location_visits, impressions),
                'W_VISIT_RATE': safe_divide(web_visits, impressions)
            })
        
        cursor.close()
        conn.close()
        return jsonify({'success': True, 'data': results})
        
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/v5/zip-performance', methods=['GET'])
def get_zip_performance():
    """Get ZIP code performance for an advertiser"""
    try:
        agency_id = request.args.get('agency_id')
        advertiser_id = request.args.get('advertiser_id')
        campaign_id = request.args.get('campaign_id')
        if not agency_id or not advertiser_id:
            return jsonify({'success': False, 'error': 'agency_id and advertiser_id required'}), 400
            
        start_date, end_date = parse_dates(request)
        source = get_agency_source(agency_id)
        
        conn = get_snowflake_connection()
        cursor = conn.cursor()
        
        if source == 'QIR':
            campaign_filter = "AND q.IO_ID = %s" if campaign_id else ""
            query = f"""
                SELECT 
                    q.ZIP_CODE,
                    COUNT(*) as IMPRESSIONS,
                    SUM(CASE WHEN q.IS_STORE_VISIT = TRUE THEN 1 ELSE 0 END) as LOCATION_VISITS,
                    SUM(CASE WHEN q.IS_SITE_VISIT = TRUE THEN 1 ELSE 0 END) as WEB_VISITS,
                    MAX(z.CITY_NAME) as CITY,
                    MAX(z.STATE_ABBREVIATION) as STATE
                FROM QUORUMDB.SEGMENT_DATA.QUORUM_IMPRESSIONS_REPORT q
                LEFT JOIN QUORUMDB.REF_DATA.ZIP_POPULATION_DATA z ON q.ZIP_CODE = z.ZIP_CODE
                WHERE q.QUORUM_ADVERTISER_ID = %s
                  AND q.AGENCY_ID = %s
                  AND q.LOG_DATE >= %s AND q.LOG_DATE < %s
                  AND q.ZIP_CODE IS NOT NULL AND q.ZIP_CODE != ''
                  {campaign_filter}
                GROUP BY q.ZIP_CODE
                HAVING COUNT(*) >= 100
                ORDER BY IMPRESSIONS DESC
                LIMIT 50
            """
            params = (advertiser_id, agency_id, start_date, end_date, campaign_id) if campaign_id else (advertiser_id, agency_id, start_date, end_date)
            cursor.execute(query, params)
            
        elif source == 'CPRS':
            campaign_filter = "AND w.IO_ID = %s" if campaign_id else ""
            query = f"""
                SELECT 
                    w.ZIP as ZIP_CODE,
                    SUM(w.IMPRESSIONS) as IMPRESSIONS,
                    SUM(w.VISITORS) as LOCATION_VISITS,
                    0 as WEB_VISITS,
                    MAX(z.CITY_NAME) as CITY,
                    MAX(z.STATE_ABBREVIATION) as STATE
                FROM QUORUMDB.SEGMENT_DATA.CAMPAIGN_PERFORMANCE_REPORT_WEEKLY_STATS w
                LEFT JOIN QUORUMDB.REF_DATA.ZIP_POPULATION_DATA z ON w.ZIP = z.ZIP_CODE
                WHERE w.ADVERTISER_ID = %s
                  AND w.AGENCY_ID = %s
                  AND w.LOG_DATE >= %s AND w.LOG_DATE < %s
                  AND w.ZIP IS NOT NULL AND w.ZIP != ''
                  {campaign_filter}
                GROUP BY w.ZIP
                HAVING SUM(w.IMPRESSIONS) >= 100
                ORDER BY IMPRESSIONS DESC
                LIMIT 50
            """
            params = (advertiser_id, agency_id, start_date, end_date, campaign_id) if campaign_id else (advertiser_id, agency_id, start_date, end_date)
            cursor.execute(query, params)
            
        elif source == 'PARAMOUNT':
            # Paramount doesn't have ZIP in the main table
            cursor.close()
            conn.close()
            return jsonify({'success': True, 'data': [], 'message': 'ZIP data not available for Paramount'})
        
        results = []
        for row in cursor.fetchall():
            impressions = row[1] or 0
            location_visits = row[2] or 0
            web_visits = row[3] or 0
            results.append({
                'ZIP_CODE': row[0],
                'IMPRESSIONS': impressions,
                'LOCATION_VISITS': location_visits,
                'WEB_VISITS': web_visits,
                'CITY': row[4],
                'STATE': row[5],
                'L_VISIT_RATE': safe_divide(location_visits, impressions),
                'W_VISIT_RATE': safe_divide(web_visits, impressions)
            })
        
        cursor.close()
        conn.close()
        return jsonify({'success': True, 'data': results})
        
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/v5/dma-performance', methods=['GET'])
def get_dma_performance():
    """Get DMA performance for an advertiser"""
    try:
        agency_id = request.args.get('agency_id')
        advertiser_id = request.args.get('advertiser_id')
        campaign_id = request.args.get('campaign_id')
        if not agency_id or not advertiser_id:
            return jsonify({'success': False, 'error': 'agency_id and advertiser_id required'}), 400
            
        start_date, end_date = parse_dates(request)
        source = get_agency_source(agency_id)
        
        conn = get_snowflake_connection()
        cursor = conn.cursor()
        
        if source == 'QIR':
            # QIR may not have DMA - check if column exists
            campaign_filter = "AND IO_ID = %s" if campaign_id else ""
            query = f"""
                SELECT 
                    DMA,
                    COUNT(*) as IMPRESSIONS,
                    SUM(CASE WHEN IS_STORE_VISIT = TRUE THEN 1 ELSE 0 END) as LOCATION_VISITS,
                    SUM(CASE WHEN IS_SITE_VISIT = TRUE THEN 1 ELSE 0 END) as WEB_VISITS
                FROM QUORUMDB.SEGMENT_DATA.QUORUM_IMPRESSIONS_REPORT
                WHERE QUORUM_ADVERTISER_ID = %s
                  AND AGENCY_ID = %s
                  AND LOG_DATE >= %s AND LOG_DATE < %s
                  AND DMA IS NOT NULL AND DMA != ''
                  {campaign_filter}
                GROUP BY DMA
                HAVING COUNT(*) >= 100
                ORDER BY IMPRESSIONS DESC
                LIMIT 50
            """
            params = (advertiser_id, agency_id, start_date, end_date, campaign_id) if campaign_id else (advertiser_id, agency_id, start_date, end_date)
            cursor.execute(query, params)
            
        elif source == 'CPRS':
            campaign_filter = "AND IO_ID = %s" if campaign_id else ""
            query = f"""
                SELECT 
                    DMA,
                    SUM(IMPRESSIONS) as IMPRESSIONS,
                    SUM(VISITORS) as LOCATION_VISITS,
                    0 as WEB_VISITS
                FROM QUORUMDB.SEGMENT_DATA.CAMPAIGN_PERFORMANCE_REPORT_WEEKLY_STATS
                WHERE ADVERTISER_ID = %s
                  AND AGENCY_ID = %s
                  AND LOG_DATE >= %s AND LOG_DATE < %s
                  AND DMA IS NOT NULL AND DMA != ''
                  {campaign_filter}
                GROUP BY DMA
                HAVING SUM(IMPRESSIONS) >= 100
                ORDER BY IMPRESSIONS DESC
                LIMIT 50
            """
            params = (advertiser_id, agency_id, start_date, end_date, campaign_id) if campaign_id else (advertiser_id, agency_id, start_date, end_date)
            cursor.execute(query, params)
            
        elif source == 'PARAMOUNT':
            cursor.close()
            conn.close()
            return jsonify({'success': True, 'data': [], 'message': 'DMA data not available for Paramount'})
        
        results = []
        for row in cursor.fetchall():
            impressions = row[1] or 0
            location_visits = row[2] or 0
            web_visits = row[3] or 0
            results.append({
                'DMA': row[0],
                'IMPRESSIONS': impressions,
                'LOCATION_VISITS': location_visits,
                'WEB_VISITS': web_visits,
                'L_VISIT_RATE': safe_divide(location_visits, impressions),
                'W_VISIT_RATE': safe_divide(web_visits, impressions)
            })
        
        cursor.close()
        conn.close()
        return jsonify({'success': True, 'data': results})
        
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


# ============================================================================
# HEALTH CHECK
# ============================================================================

@app.route('/health', methods=['GET'])
def health_check():
    return jsonify({'status': 'healthy', 'version': 'v5-clean'})


@app.route('/', methods=['GET'])
def root():
    return jsonify({
        'name': 'Quorum Optimizer API',
        'version': 'v5-clean',
        'endpoints': [
            '/api/v5/agencies',
            '/api/v5/advertiser-overview',
            '/api/v5/advertiser-summary',
            '/api/v5/campaign-performance',
            '/api/v5/line-item-performance',
            '/api/v5/creative-performance',
            '/api/v5/publisher-performance',
            '/api/v5/zip-performance',
            '/api/v5/dma-performance'
        ]
    })


if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=False)
