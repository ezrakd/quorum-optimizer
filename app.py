"""
Quorum Optimizer API - Complete v5
Supports both v3 endpoints (for overview/navigation) and non-versioned endpoints (for detail views)

Data Sources:
- QIR (QUORUM_IMPRESSIONS_REPORT): Row-level for MNTN, Dealer Spike, InteractRV, ARI, Level5, ByRider
- CPRS (CAMPAIGN_PERFORMANCE_REPORT_WEEKLY_STATS): Pre-aggregated for Causal iQ, Hearst, Magnite, etc.
- PARAMOUNT: Custom tables for ViacomCBS/Paramount
"""

from flask import Flask, jsonify, request
from flask_cors import CORS
import snowflake.connector
import os
from datetime import datetime, timedelta

app = Flask(__name__)
CORS(app)

# ============================================================================
# SNOWFLAKE CONNECTION
# ============================================================================

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

QIR_AGENCIES = {2514, 1956, 2298, 2086, 1955, 1950}  # MNTN, Dealer Spike, InteractRV, Level5, ARI, ByRider
CPRS_AGENCIES = {1813, 1972, 2234, 2744, 1445, 2379, 2691, 1880}  # Causal iQ, Hearst, Magnite, etc.
PARAMOUNT_AGENCY = 1480

def get_agency_source(agency_id):
    """Determine which data source to use for an agency"""
    agency_id = int(agency_id) if agency_id else 0
    if agency_id == PARAMOUNT_AGENCY:
        return 'PARAMOUNT'
    elif agency_id in QIR_AGENCIES:
        return 'QIR'
    elif agency_id in CPRS_AGENCIES:
        return 'CPRS'
    else:
        return 'CPRS'

# ============================================================================
# HELPERS
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

def execute_query(query, params=None):
    """Execute a query and return results"""
    conn = get_snowflake_connection()
    cursor = conn.cursor()
    try:
        if params:
            cursor.execute(query, params)
        else:
            cursor.execute(query)
        results = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description] if cursor.description else []
        return results, columns
    finally:
        cursor.close()
        conn.close()

# ============================================================================
# V3 ENDPOINTS - Overview/Navigation
# ============================================================================

@app.route('/api/v3/agencies', methods=['GET'])
def get_agencies_v3():
    """Get list of all active agencies"""
    try:
        # Get agencies from QIR
        query_qir = """
            SELECT DISTINCT q.AGENCY_ID, MAX(aa.AGENCY_NAME) as AGENCY_NAME,
                   COUNT(DISTINCT q.QUORUM_ADVERTISER_ID) as ADVERTISER_COUNT
            FROM QUORUMDB.SEGMENT_DATA.QUORUM_IMPRESSIONS_REPORT q
            LEFT JOIN QUORUMDB.SEGMENT_DATA.AGENCY_ADVERTISER aa ON q.AGENCY_ID = aa.ADVERTISER_ID
            WHERE q.LOG_DATE >= DATEADD(day, -30, CURRENT_DATE())
            GROUP BY q.AGENCY_ID
        """
        qir_results, _ = execute_query(query_qir)
        
        # Get agencies from CPRS (excluding those already in QIR)
        query_cprs = """
            SELECT DISTINCT w.AGENCY_ID, MAX(aa.AGENCY_NAME) as AGENCY_NAME,
                   COUNT(DISTINCT w.ADVERTISER_ID) as ADVERTISER_COUNT
            FROM QUORUMDB.SEGMENT_DATA.CAMPAIGN_PERFORMANCE_REPORT_WEEKLY_STATS w
            LEFT JOIN QUORUMDB.SEGMENT_DATA.AGENCY_ADVERTISER aa ON w.AGENCY_ID = aa.ADVERTISER_ID
            WHERE w.LOG_DATE >= DATEADD(day, -30, CURRENT_DATE())
              AND w.AGENCY_ID NOT IN (SELECT DISTINCT AGENCY_ID FROM QUORUMDB.SEGMENT_DATA.QUORUM_IMPRESSIONS_REPORT WHERE LOG_DATE >= DATEADD(day, -30, CURRENT_DATE()))
            GROUP BY w.AGENCY_ID
        """
        cprs_results, _ = execute_query(query_cprs)
        
        # Get Paramount
        query_paramount = """
            SELECT 1480 as AGENCY_ID, 'ViacomCBS / Paramount' as AGENCY_NAME,
                   COUNT(DISTINCT QUORUM_ADVERTISER_ID) as ADVERTISER_COUNT
            FROM QUORUMDB.SEGMENT_DATA.PARAMOUNT_IMPRESSIONS_REPORT_90_DAYS
        """
        paramount_results, _ = execute_query(query_paramount)
        
        agencies = []
        for row in qir_results + cprs_results + paramount_results:
            agencies.append({
                'AGENCY_ID': str(row[0]),
                'AGENCY_NAME': row[1] or f'Agency {row[0]}',
                'ADVERTISER_COUNT': row[2] or 0
            })
        
        agencies.sort(key=lambda x: x['AGENCY_NAME'])
        return jsonify({'success': True, 'data': agencies})
        
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/v3/agency-overview', methods=['GET'])
def get_agency_overview_v3():
    """Get all agencies with full metrics"""
    try:
        start_date, end_date = parse_dates(request)
        
        # QIR agencies
        query_qir = """
            SELECT 
                q.AGENCY_ID, MAX(aa.AGENCY_NAME) as AGENCY_NAME,
                COUNT(DISTINCT q.QUORUM_ADVERTISER_ID) as ADVERTISER_COUNT,
                COUNT(*) as IMPRESSIONS,
                SUM(CASE WHEN q.IS_STORE_VISIT = TRUE THEN 1 ELSE 0 END) as LOCATION_VISITS,
                SUM(CASE WHEN q.IS_SITE_VISIT = TRUE THEN 1 ELSE 0 END) as WEB_VISITS
            FROM QUORUMDB.SEGMENT_DATA.QUORUM_IMPRESSIONS_REPORT q
            LEFT JOIN QUORUMDB.SEGMENT_DATA.AGENCY_ADVERTISER aa ON q.AGENCY_ID = aa.ADVERTISER_ID
            WHERE q.LOG_DATE >= %s AND q.LOG_DATE < %s
            GROUP BY q.AGENCY_ID
        """
        qir_results, _ = execute_query(query_qir, (start_date, end_date))
        
        # CPRS agencies
        query_cprs = """
            SELECT 
                w.AGENCY_ID, MAX(aa.AGENCY_NAME) as AGENCY_NAME,
                COUNT(DISTINCT w.ADVERTISER_ID) as ADVERTISER_COUNT,
                SUM(w.IMPRESSIONS) as IMPRESSIONS,
                SUM(w.VISITORS) as LOCATION_VISITS,
                0 as WEB_VISITS
            FROM QUORUMDB.SEGMENT_DATA.CAMPAIGN_PERFORMANCE_REPORT_WEEKLY_STATS w
            LEFT JOIN QUORUMDB.SEGMENT_DATA.AGENCY_ADVERTISER aa ON w.AGENCY_ID = aa.ADVERTISER_ID
            WHERE w.LOG_DATE >= %s AND w.LOG_DATE < %s
              AND w.AGENCY_ID NOT IN (SELECT DISTINCT AGENCY_ID FROM QUORUMDB.SEGMENT_DATA.QUORUM_IMPRESSIONS_REPORT WHERE LOG_DATE >= %s AND LOG_DATE < %s)
            GROUP BY w.AGENCY_ID
        """
        cprs_results, _ = execute_query(query_cprs, (start_date, end_date, start_date, end_date))
        
        # Paramount - impressions + web visits from separate table
        query_paramount = """
            SELECT 1480, 'ViacomCBS / Paramount', COUNT(DISTINCT QUORUM_ADVERTISER_ID), COUNT(*), 0, 0
            FROM QUORUMDB.SEGMENT_DATA.PARAMOUNT_IMPRESSIONS_REPORT_90_DAYS
        """
        paramount_results, _ = execute_query(query_paramount)
        
        # Get Paramount web visits separately
        query_paramount_web = """
            SELECT COUNT(DISTINCT WEB_IMPRESSION_ID)
            FROM QUORUMDB.SEGMENT_DATA.WEB_VISITORS_TO_LOG
            WHERE SITE_VISIT_TIMESTAMP >= %s AND SITE_VISIT_TIMESTAMP < %s
        """
        paramount_web, _ = execute_query(query_paramount_web, (start_date, end_date))
        paramount_web_visits = paramount_web[0][0] if paramount_web else 0
        
        agencies = []
        for row in qir_results + cprs_results:
            impressions = row[3] or 0
            location_visits = row[4] or 0
            web_visits = row[5] or 0
            agencies.append({
                'AGENCY_ID': str(row[0]),
                'AGENCY_NAME': row[1] or f'Agency {row[0]}',
                'ADVERTISER_COUNT': row[2] or 0,
                'IMPRESSIONS': impressions,
                'LOCATION_VISITS': location_visits,
                'WEB_VISITS': web_visits,
                'LOCATION_VR': safe_divide(location_visits, impressions),
                'VISIT_RATE': safe_divide(web_visits, impressions)
            })
        
        # Add Paramount
        if paramount_results:
            p = paramount_results[0]
            impressions = p[3] or 0
            agencies.append({
                'AGENCY_ID': '1480',
                'AGENCY_NAME': 'ViacomCBS / Paramount',
                'ADVERTISER_COUNT': p[2] or 0,
                'IMPRESSIONS': impressions,
                'LOCATION_VISITS': 0,
                'WEB_VISITS': paramount_web_visits,
                'LOCATION_VR': 0,
                'VISIT_RATE': safe_divide(paramount_web_visits, impressions)
            })
        
        agencies.sort(key=lambda x: x['IMPRESSIONS'] or 0, reverse=True)
        return jsonify({'success': True, 'data': agencies})
        
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/v3/advertiser-overview', methods=['GET'])
def get_advertiser_overview_v3():
    """Get advertisers for an agency with metrics"""
    try:
        agency_id = request.args.get('agency_id')
        if not agency_id:
            return jsonify({'success': False, 'error': 'agency_id required'}), 400
            
        start_date, end_date = parse_dates(request)
        source = get_agency_source(agency_id)
        
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
                WHERE q.AGENCY_ID = %s AND q.LOG_DATE >= %s AND q.LOG_DATE < %s
                GROUP BY q.QUORUM_ADVERTISER_ID
                HAVING COUNT(*) >= 1000
                ORDER BY IMPRESSIONS DESC
            """
            results, _ = execute_query(query, (agency_id, start_date, end_date))
            
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
                WHERE w.AGENCY_ID = %s AND w.LOG_DATE >= %s AND w.LOG_DATE < %s
                GROUP BY w.ADVERTISER_ID
                HAVING SUM(w.IMPRESSIONS) >= 1000
                ORDER BY IMPRESSIONS DESC
            """
            results, _ = execute_query(query, (agency_id, start_date, end_date))
            
        elif source == 'PARAMOUNT':
            # Get impressions
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
            imp_results, _ = execute_query(query_imps)
            imp_dict = {str(int(r[0])): {'ADVERTISER_ID': str(int(r[0])), 'ADVERTISER_NAME': r[1], 'IMPRESSIONS': r[2]} for r in imp_results}
            
            # Get web visits
            query_visits = """
                SELECT QUORUM_ADVERTISER_ID, COUNT(DISTINCT WEB_IMPRESSION_ID) as WEB_VISITS
                FROM QUORUMDB.SEGMENT_DATA.WEB_VISITORS_TO_LOG
                WHERE SITE_VISIT_TIMESTAMP >= %s AND SITE_VISIT_TIMESTAMP < %s
                GROUP BY QUORUM_ADVERTISER_ID
            """
            visit_results, _ = execute_query(query_visits, (start_date, end_date))
            
            for row in visit_results:
                adv_id = str(row[0])
                if adv_id in imp_dict:
                    imp_dict[adv_id]['WEB_VISITS'] = row[1]
            
            advertisers = []
            for adv_id, data in imp_dict.items():
                impressions = data.get('IMPRESSIONS', 0) or 0
                web_visits = data.get('WEB_VISITS', 0) or 0
                advertisers.append({
                    'ADVERTISER_ID': adv_id,
                    'ADVERTISER_NAME': data['ADVERTISER_NAME'],
                    'IMPRESSIONS': impressions,
                    'LOCATION_VISITS': 0,
                    'WEB_VISITS': web_visits,
                    'LOCATION_VR': 0,
                    'VISIT_RATE': safe_divide(web_visits, impressions)
                })
            advertisers.sort(key=lambda x: x['IMPRESSIONS'], reverse=True)
            return jsonify({'success': True, 'data': advertisers})
        
        # Process QIR/CPRS results
        advertisers = []
        for row in results:
            impressions = row[2] or 0
            location_visits = row[3] or 0
            web_visits = row[4] or 0
            advertisers.append({
                'ADVERTISER_ID': str(row[0]),
                'ADVERTISER_NAME': row[1],
                'IMPRESSIONS': impressions,
                'LOCATION_VISITS': location_visits,
                'WEB_VISITS': web_visits,
                'LOCATION_VR': safe_divide(location_visits, impressions),
                'VISIT_RATE': safe_divide(web_visits, impressions)
            })
        
        return jsonify({'success': True, 'data': advertisers})
        
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/v3/advertisers', methods=['GET'])
def get_advertisers_v3():
    """Get simple advertiser list for sidebar"""
    try:
        agency_id = request.args.get('agency_id')
        if not agency_id:
            return jsonify({'success': False, 'error': 'agency_id required'}), 400
            
        source = get_agency_source(agency_id)
        
        if source == 'QIR':
            query = """
                SELECT DISTINCT q.QUORUM_ADVERTISER_ID as ADVERTISER_ID,
                       MAX(aa.COMP_NAME) as ADVERTISER_NAME
                FROM QUORUMDB.SEGMENT_DATA.QUORUM_IMPRESSIONS_REPORT q
                LEFT JOIN QUORUMDB.SEGMENT_DATA.AGENCY_ADVERTISER aa ON q.QUORUM_ADVERTISER_ID = aa.ID
                WHERE q.AGENCY_ID = %s AND q.LOG_DATE >= DATEADD(day, -90, CURRENT_DATE())
                GROUP BY q.QUORUM_ADVERTISER_ID
                HAVING COUNT(*) >= 1000
                ORDER BY ADVERTISER_NAME
            """
            results, _ = execute_query(query, (agency_id,))
            
        elif source == 'CPRS':
            query = """
                SELECT DISTINCT w.ADVERTISER_ID,
                       MAX(aa.COMP_NAME) as ADVERTISER_NAME
                FROM QUORUMDB.SEGMENT_DATA.CAMPAIGN_PERFORMANCE_REPORT_WEEKLY_STATS w
                LEFT JOIN QUORUMDB.SEGMENT_DATA.AGENCY_ADVERTISER aa ON w.ADVERTISER_ID = aa.ID
                WHERE w.AGENCY_ID = %s AND w.LOG_DATE >= DATEADD(day, -90, CURRENT_DATE())
                GROUP BY w.ADVERTISER_ID
                HAVING SUM(w.IMPRESSIONS) >= 1000
                ORDER BY ADVERTISER_NAME
            """
            results, _ = execute_query(query, (agency_id,))
            
        elif source == 'PARAMOUNT':
            query = """
                SELECT DISTINCT p.QUORUM_ADVERTISER_ID as ADVERTISER_ID,
                       MAX(aa.COMP_NAME) as ADVERTISER_NAME
                FROM QUORUMDB.SEGMENT_DATA.PARAMOUNT_IMPRESSIONS_REPORT_90_DAYS p
                LEFT JOIN QUORUMDB.SEGMENT_DATA.AGENCY_ADVERTISER aa ON p.QUORUM_ADVERTISER_ID = aa.ID
                GROUP BY p.QUORUM_ADVERTISER_ID
                HAVING COUNT(*) >= 10000
                ORDER BY ADVERTISER_NAME
            """
            results, _ = execute_query(query)
        
        advertisers = [{'ADVERTISER_ID': str(row[0]), 'ADVERTISER_NAME': row[1] or f'Advertiser {row[0]}'} for row in results]
        return jsonify({'success': True, 'data': advertisers})
        
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/v3/global-advertiser-overview', methods=['GET'])
def get_global_advertiser_overview_v3():
    """Get all advertisers across all agencies"""
    try:
        start_date, end_date = parse_dates(request)
        
        # Get top advertisers from QIR
        query_qir = """
            SELECT 
                q.QUORUM_ADVERTISER_ID as ADVERTISER_ID,
                MAX(aa.COMP_NAME) as ADVERTISER_NAME,
                MAX(ag.AGENCY_NAME) as AGENCY_NAME,
                COUNT(*) as IMPRESSIONS,
                SUM(CASE WHEN q.IS_STORE_VISIT = TRUE THEN 1 ELSE 0 END) as LOCATION_VISITS,
                SUM(CASE WHEN q.IS_SITE_VISIT = TRUE THEN 1 ELSE 0 END) as WEB_VISITS
            FROM QUORUMDB.SEGMENT_DATA.QUORUM_IMPRESSIONS_REPORT q
            LEFT JOIN QUORUMDB.SEGMENT_DATA.AGENCY_ADVERTISER aa ON q.QUORUM_ADVERTISER_ID = aa.ID
            LEFT JOIN (SELECT DISTINCT ADVERTISER_ID, AGENCY_NAME FROM QUORUMDB.SEGMENT_DATA.AGENCY_ADVERTISER) ag ON q.AGENCY_ID = ag.ADVERTISER_ID
            WHERE q.LOG_DATE >= %s AND q.LOG_DATE < %s
            GROUP BY q.QUORUM_ADVERTISER_ID
            HAVING COUNT(*) >= 10000
            ORDER BY IMPRESSIONS DESC
            LIMIT 50
        """
        qir_results, _ = execute_query(query_qir, (start_date, end_date))
        
        advertisers = []
        for row in qir_results:
            impressions = row[3] or 0
            location_visits = row[4] or 0
            web_visits = row[5] or 0
            advertisers.append({
                'ADVERTISER_ID': str(row[0]),
                'ADVERTISER_NAME': row[1],
                'AGENCY_NAME': row[2],
                'IMPRESSIONS': impressions,
                'LOCATION_VISITS': location_visits,
                'WEB_VISITS': web_visits,
                'LOCATION_VR': safe_divide(location_visits, impressions),
                'VISIT_RATE': safe_divide(web_visits, impressions)
            })
        
        return jsonify({'success': True, 'data': advertisers})
        
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/v3/impressions-timeseries', methods=['GET'])
def get_impressions_timeseries_v3():
    """Get daily impressions by agency for chart"""
    try:
        start_date, end_date = parse_dates(request)
        
        query = """
            SELECT 
                TO_CHAR(q.LOG_DATE, 'YYYY-MM-DD') as DATE,
                MAX(aa.AGENCY_NAME) as AGENCY_NAME,
                COUNT(*) as IMPRESSIONS
            FROM QUORUMDB.SEGMENT_DATA.QUORUM_IMPRESSIONS_REPORT q
            LEFT JOIN (SELECT DISTINCT ADVERTISER_ID, AGENCY_NAME FROM QUORUMDB.SEGMENT_DATA.AGENCY_ADVERTISER) aa ON q.AGENCY_ID = aa.ADVERTISER_ID
            WHERE q.LOG_DATE >= %s AND q.LOG_DATE < %s
            GROUP BY q.LOG_DATE, q.AGENCY_ID
            ORDER BY DATE, AGENCY_NAME
        """
        results, _ = execute_query(query, (start_date, end_date))
        
        # Pivot to {date: {agency: impressions}}
        date_data = {}
        for row in results:
            date_str = row[0]
            agency = row[1] or 'Unknown'
            impressions = row[2] or 0
            if date_str not in date_data:
                date_data[date_str] = {'date': date_str}
            date_data[date_str][agency] = impressions
        
        timeseries = list(date_data.values())
        timeseries.sort(key=lambda x: x['date'])
        
        return jsonify({'success': True, 'data': timeseries})
        
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/v3/advertiser-timeseries', methods=['GET'])
def get_advertiser_timeseries_v3():
    """Get daily impressions by advertiser for an agency"""
    try:
        agency_id = request.args.get('agency_id')
        if not agency_id:
            return jsonify({'success': False, 'error': 'agency_id required'}), 400
            
        start_date, end_date = parse_dates(request)
        source = get_agency_source(agency_id)
        
        if source == 'QIR':
            query = """
                SELECT 
                    TO_CHAR(q.LOG_DATE, 'YYYY-MM-DD') as DATE,
                    MAX(aa.COMP_NAME) as ADVERTISER_NAME,
                    COUNT(*) as IMPRESSIONS
                FROM QUORUMDB.SEGMENT_DATA.QUORUM_IMPRESSIONS_REPORT q
                LEFT JOIN QUORUMDB.SEGMENT_DATA.AGENCY_ADVERTISER aa ON q.QUORUM_ADVERTISER_ID = aa.ID
                WHERE q.AGENCY_ID = %s AND q.LOG_DATE >= %s AND q.LOG_DATE < %s
                GROUP BY q.LOG_DATE, q.QUORUM_ADVERTISER_ID
                ORDER BY DATE, ADVERTISER_NAME
            """
            results, _ = execute_query(query, (agency_id, start_date, end_date))
        else:
            # For CPRS/Paramount, return empty - they don't have daily granularity
            return jsonify({'success': True, 'data': []})
        
        # Pivot data
        date_data = {}
        for row in results:
            date_str = row[0]
            advertiser = row[1] or 'Unknown'
            impressions = row[2] or 0
            if date_str not in date_data:
                date_data[date_str] = {'date': date_str}
            date_data[date_str][advertiser] = date_data[date_str].get(advertiser, 0) + impressions
        
        timeseries = list(date_data.values())
        timeseries.sort(key=lambda x: x['date'])
        
        return jsonify({'success': True, 'data': timeseries})
        
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


# ============================================================================
# NON-VERSIONED ENDPOINTS - Advertiser Detail
# ============================================================================

@app.route('/api/advertiser-summary', methods=['GET'])
def get_advertiser_summary():
    """Get summary metrics for a specific advertiser"""
    try:
        agency_id = request.args.get('agency_id')
        advertiser_id = request.args.get('advertiser_id')
        if not agency_id or not advertiser_id:
            return jsonify({'success': False, 'error': 'agency_id and advertiser_id required'}), 400
            
        start_date, end_date = parse_dates(request)
        source = get_agency_source(agency_id)
        
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
                WHERE QUORUM_ADVERTISER_ID = %s AND AGENCY_ID = %s
                  AND LOG_DATE >= %s AND LOG_DATE < %s
            """
            results, _ = execute_query(query, (advertiser_id, agency_id, start_date, end_date))
            
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
                WHERE ADVERTISER_ID = %s AND AGENCY_ID = %s
                  AND LOG_DATE >= %s AND LOG_DATE < %s
            """
            results, _ = execute_query(query, (advertiser_id, agency_id, start_date, end_date))
            
        elif source == 'PARAMOUNT':
            query_imps = """
                SELECT COUNT(*), COUNT(DISTINCT IO_ID), COUNT(DISTINCT PUBLISHER_CODE), COUNT(DISTINCT CREATIVE_ID)
                FROM QUORUMDB.SEGMENT_DATA.PARAMOUNT_IMPRESSIONS_REPORT_90_DAYS
                WHERE QUORUM_ADVERTISER_ID = %s
            """
            imp_results, _ = execute_query(query_imps, (advertiser_id,))
            
            query_visits = """
                SELECT COUNT(DISTINCT WEB_IMPRESSION_ID)
                FROM QUORUMDB.SEGMENT_DATA.WEB_VISITORS_TO_LOG
                WHERE QUORUM_ADVERTISER_ID = %s
                  AND SITE_VISIT_TIMESTAMP >= %s AND SITE_VISIT_TIMESTAMP < %s
            """
            visit_results, _ = execute_query(query_visits, (advertiser_id, start_date, end_date))
            
            impressions = imp_results[0][0] if imp_results else 0
            web_visits = visit_results[0][0] if visit_results else 0
            
            return jsonify({'success': True, 'data': {
                'IMPRESSIONS': impressions,
                'LOCATION_VISITS': 0,
                'WEB_VISITS': web_visits,
                'CAMPAIGN_COUNT': imp_results[0][1] if imp_results else 0,
                'PUBLISHER_COUNT': imp_results[0][2] if imp_results else 0,
                'CREATIVE_COUNT': imp_results[0][3] if imp_results else 0,
                'LOCATION_VR': 0,
                'VISIT_RATE': safe_divide(web_visits, impressions)
            }})
        
        if not results or not results[0]:
            return jsonify({'success': True, 'data': {}})
        
        row = results[0]
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
            'LOCATION_VR': safe_divide(location_visits, impressions),
            'VISIT_RATE': safe_divide(web_visits, impressions)
        }})
        
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/campaign-performance', methods=['GET'])
def get_campaign_performance():
    """Get campaign-level performance for an advertiser"""
    try:
        agency_id = request.args.get('agency_id')
        advertiser_id = request.args.get('advertiser_id')
        if not agency_id or not advertiser_id:
            return jsonify({'success': False, 'error': 'agency_id and advertiser_id required'}), 400
            
        start_date, end_date = parse_dates(request)
        source = get_agency_source(agency_id)
        
        if source == 'QIR':
            query = """
                SELECT IO_ID, MAX(IO_NAME) as CAMPAIGN_NAME, COUNT(*) as IMPRESSIONS,
                       SUM(CASE WHEN IS_STORE_VISIT = TRUE THEN 1 ELSE 0 END) as LOCATION_VISITS,
                       SUM(CASE WHEN IS_SITE_VISIT = TRUE THEN 1 ELSE 0 END) as WEB_VISITS
                FROM QUORUMDB.SEGMENT_DATA.QUORUM_IMPRESSIONS_REPORT
                WHERE QUORUM_ADVERTISER_ID = %s AND AGENCY_ID = %s
                  AND LOG_DATE >= %s AND LOG_DATE < %s
                GROUP BY IO_ID HAVING COUNT(*) >= 100
                ORDER BY IMPRESSIONS DESC
            """
            results, _ = execute_query(query, (advertiser_id, agency_id, start_date, end_date))
            
        elif source == 'CPRS':
            query = """
                SELECT IO_ID, MAX(IO_NAME) as CAMPAIGN_NAME, SUM(IMPRESSIONS) as IMPRESSIONS,
                       SUM(VISITORS) as LOCATION_VISITS, 0 as WEB_VISITS
                FROM QUORUMDB.SEGMENT_DATA.CAMPAIGN_PERFORMANCE_REPORT_WEEKLY_STATS
                WHERE ADVERTISER_ID = %s AND AGENCY_ID = %s
                  AND LOG_DATE >= %s AND LOG_DATE < %s
                GROUP BY IO_ID HAVING SUM(IMPRESSIONS) >= 100
                ORDER BY IMPRESSIONS DESC
            """
            results, _ = execute_query(query, (advertiser_id, agency_id, start_date, end_date))
            
        elif source == 'PARAMOUNT':
            query = """
                SELECT IO_ID, MAX(IO_NAME) as CAMPAIGN_NAME, COUNT(*) as IMPRESSIONS,
                       0 as LOCATION_VISITS, 0 as WEB_VISITS
                FROM QUORUMDB.SEGMENT_DATA.PARAMOUNT_IMPRESSIONS_REPORT_90_DAYS
                WHERE QUORUM_ADVERTISER_ID = %s
                GROUP BY IO_ID HAVING COUNT(*) >= 100
                ORDER BY IMPRESSIONS DESC
            """
            results, _ = execute_query(query, (advertiser_id,))
        
        campaigns = []
        for row in results:
            impressions = row[2] or 0
            location_visits = row[3] or 0
            web_visits = row[4] or 0
            campaigns.append({
                'CAMPAIGN_ID': row[0],
                'CAMPAIGN_NAME': row[1],
                'IMPRESSIONS': impressions,
                'LOCATION_VISITS': location_visits,
                'WEB_VISITS': web_visits,
                'LOCATION_VR': safe_divide(location_visits, impressions),
                'VISIT_RATE': safe_divide(web_visits, impressions)
            })
        
        return jsonify({'success': True, 'data': campaigns})
        
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/publisher-performance', methods=['GET'])
def get_publisher_performance():
    """Get publisher/contextual performance for an advertiser"""
    try:
        agency_id = request.args.get('agency_id')
        advertiser_id = request.args.get('advertiser_id')
        if not agency_id or not advertiser_id:
            return jsonify({'success': False, 'error': 'agency_id and advertiser_id required'}), 400
            
        start_date, end_date = parse_dates(request)
        source = get_agency_source(agency_id)
        
        if source == 'QIR':
            query = """
                SELECT PUBLISHER_CODE, COUNT(*) as IMPRESSIONS,
                       SUM(CASE WHEN IS_STORE_VISIT = TRUE THEN 1 ELSE 0 END) as LOCATION_VISITS,
                       SUM(CASE WHEN IS_SITE_VISIT = TRUE THEN 1 ELSE 0 END) as WEB_VISITS
                FROM QUORUMDB.SEGMENT_DATA.QUORUM_IMPRESSIONS_REPORT
                WHERE QUORUM_ADVERTISER_ID = %s AND AGENCY_ID = %s
                  AND LOG_DATE >= %s AND LOG_DATE < %s
                GROUP BY PUBLISHER_CODE HAVING COUNT(*) >= 100
                ORDER BY IMPRESSIONS DESC LIMIT 100
            """
            results, _ = execute_query(query, (advertiser_id, agency_id, start_date, end_date))
            
        elif source == 'CPRS':
            query = """
                SELECT PUBLISHER, SUM(IMPRESSIONS) as IMPRESSIONS,
                       SUM(VISITORS) as LOCATION_VISITS, 0 as WEB_VISITS
                FROM QUORUMDB.SEGMENT_DATA.CAMPAIGN_PERFORMANCE_REPORT_WEEKLY_STATS
                WHERE ADVERTISER_ID = %s AND AGENCY_ID = %s
                  AND LOG_DATE >= %s AND LOG_DATE < %s
                GROUP BY PUBLISHER HAVING SUM(IMPRESSIONS) >= 100
                ORDER BY IMPRESSIONS DESC LIMIT 100
            """
            results, _ = execute_query(query, (advertiser_id, agency_id, start_date, end_date))
            
        elif source == 'PARAMOUNT':
            query = """
                SELECT PUBLISHER_CODE, COUNT(*) as IMPRESSIONS, 0 as LOCATION_VISITS, 0 as WEB_VISITS
                FROM QUORUMDB.SEGMENT_DATA.PARAMOUNT_IMPRESSIONS_REPORT_90_DAYS
                WHERE QUORUM_ADVERTISER_ID = %s
                GROUP BY PUBLISHER_CODE HAVING COUNT(*) >= 100
                ORDER BY IMPRESSIONS DESC LIMIT 100
            """
            results, _ = execute_query(query, (advertiser_id,))
        
        publishers = []
        for row in results:
            impressions = row[1] or 0
            location_visits = row[2] or 0
            web_visits = row[3] or 0
            publishers.append({
                'PUBLISHER': row[0],
                'PUBLISHER_CODE': row[0],
                'IMPRESSIONS': impressions,
                'LOCATION_VISITS': location_visits,
                'WEB_VISITS': web_visits,
                'LOCATION_VR': safe_divide(location_visits, impressions),
                'VISIT_RATE': safe_divide(web_visits, impressions)
            })
        
        return jsonify({'success': True, 'data': publishers})
        
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/zip-performance', methods=['GET'])
def get_zip_performance():
    """Get ZIP code performance for an advertiser"""
    try:
        agency_id = request.args.get('agency_id')
        advertiser_id = request.args.get('advertiser_id')
        if not agency_id or not advertiser_id:
            return jsonify({'success': False, 'error': 'agency_id and advertiser_id required'}), 400
            
        start_date, end_date = parse_dates(request)
        source = get_agency_source(agency_id)
        
        if source == 'QIR':
            query = """
                SELECT q.ZIP_CODE, COUNT(*) as IMPRESSIONS,
                       SUM(CASE WHEN q.IS_STORE_VISIT = TRUE THEN 1 ELSE 0 END) as LOCATION_VISITS,
                       SUM(CASE WHEN q.IS_SITE_VISIT = TRUE THEN 1 ELSE 0 END) as WEB_VISITS,
                       MAX(z.CITY_NAME) as CITY, MAX(z.STATE_ABBREVIATION) as STATE
                FROM QUORUMDB.SEGMENT_DATA.QUORUM_IMPRESSIONS_REPORT q
                LEFT JOIN QUORUMDB.REF_DATA.ZIP_POPULATION_DATA z ON q.ZIP_CODE = z.ZIP_CODE
                WHERE q.QUORUM_ADVERTISER_ID = %s AND q.AGENCY_ID = %s
                  AND q.LOG_DATE >= %s AND q.LOG_DATE < %s
                  AND q.ZIP_CODE IS NOT NULL AND q.ZIP_CODE != ''
                GROUP BY q.ZIP_CODE HAVING COUNT(*) >= 100
                ORDER BY IMPRESSIONS DESC LIMIT 50
            """
            results, _ = execute_query(query, (advertiser_id, agency_id, start_date, end_date))
            
        elif source == 'CPRS':
            query = """
                SELECT w.ZIP, SUM(w.IMPRESSIONS) as IMPRESSIONS,
                       SUM(w.VISITORS) as LOCATION_VISITS, 0 as WEB_VISITS,
                       MAX(z.CITY_NAME) as CITY, MAX(z.STATE_ABBREVIATION) as STATE
                FROM QUORUMDB.SEGMENT_DATA.CAMPAIGN_PERFORMANCE_REPORT_WEEKLY_STATS w
                LEFT JOIN QUORUMDB.REF_DATA.ZIP_POPULATION_DATA z ON w.ZIP = z.ZIP_CODE
                WHERE w.ADVERTISER_ID = %s AND w.AGENCY_ID = %s
                  AND w.LOG_DATE >= %s AND w.LOG_DATE < %s
                  AND w.ZIP IS NOT NULL AND w.ZIP != ''
                GROUP BY w.ZIP HAVING SUM(w.IMPRESSIONS) >= 100
                ORDER BY IMPRESSIONS DESC LIMIT 50
            """
            results, _ = execute_query(query, (advertiser_id, agency_id, start_date, end_date))
            
        elif source == 'PARAMOUNT':
            return jsonify({'success': True, 'data': [], 'message': 'ZIP data not available for Paramount'})
        
        zips = []
        for row in results:
            impressions = row[1] or 0
            location_visits = row[2] or 0
            web_visits = row[3] or 0
            zips.append({
                'ZIP_CODE': row[0],
                'IMPRESSIONS': impressions,
                'LOCATION_VISITS': location_visits,
                'WEB_VISITS': web_visits,
                'CITY': row[4],
                'STATE': row[5],
                'LOCATION_VR': safe_divide(location_visits, impressions),
                'VISIT_RATE': safe_divide(web_visits, impressions)
            })
        
        return jsonify({'success': True, 'data': zips})
        
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


# ============================================================================
# WEB-SPECIFIC ENDPOINTS (for Paramount and web-only agencies)
# ============================================================================

@app.route('/api/web/summary', methods=['GET'])
def get_web_summary():
    """Get web-focused summary for Paramount advertisers"""
    try:
        advertiser_id = request.args.get('advertiser_id')
        if not advertiser_id:
            return jsonify({'success': False, 'error': 'advertiser_id required'}), 400
            
        start_date, end_date = parse_dates(request)
        
        # Get impressions
        query_imps = """
            SELECT COUNT(*), COUNT(DISTINCT IO_ID), COUNT(DISTINCT PUBLISHER_CODE)
            FROM QUORUMDB.SEGMENT_DATA.PARAMOUNT_IMPRESSIONS_REPORT_90_DAYS
            WHERE QUORUM_ADVERTISER_ID = %s
        """
        imp_results, _ = execute_query(query_imps, (advertiser_id,))
        
        # Get web visits
        query_visits = """
            SELECT COUNT(DISTINCT WEB_IMPRESSION_ID)
            FROM QUORUMDB.SEGMENT_DATA.WEB_VISITORS_TO_LOG
            WHERE QUORUM_ADVERTISER_ID = %s
              AND SITE_VISIT_TIMESTAMP >= %s AND SITE_VISIT_TIMESTAMP < %s
        """
        visit_results, _ = execute_query(query_visits, (advertiser_id, start_date, end_date))
        
        impressions = imp_results[0][0] if imp_results else 0
        web_visits = visit_results[0][0] if visit_results else 0
        
        return jsonify({'success': True, 'data': {
            'IMPRESSIONS': impressions,
            'WEB_VISITS': web_visits,
            'CAMPAIGN_COUNT': imp_results[0][1] if imp_results else 0,
            'PUBLISHER_COUNT': imp_results[0][2] if imp_results else 0,
            'VISIT_RATE': safe_divide(web_visits, impressions)
        }})
        
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/web/campaign-performance', methods=['GET'])
def get_web_campaign_performance():
    """Get campaign performance for Paramount advertisers"""
    try:
        advertiser_id = request.args.get('advertiser_id')
        if not advertiser_id:
            return jsonify({'success': False, 'error': 'advertiser_id required'}), 400
        
        query = """
            SELECT IO_ID, MAX(IO_NAME) as CAMPAIGN_NAME, COUNT(*) as IMPRESSIONS
            FROM QUORUMDB.SEGMENT_DATA.PARAMOUNT_IMPRESSIONS_REPORT_90_DAYS
            WHERE QUORUM_ADVERTISER_ID = %s
            GROUP BY IO_ID HAVING COUNT(*) >= 100
            ORDER BY IMPRESSIONS DESC
        """
        results, _ = execute_query(query, (advertiser_id,))
        
        campaigns = [{'CAMPAIGN_ID': r[0], 'CAMPAIGN_NAME': r[1], 'IMPRESSIONS': r[2], 'WEB_VISITS': 0} for r in results]
        return jsonify({'success': True, 'data': campaigns})
        
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/web/publisher-performance', methods=['GET'])
def get_web_publisher_performance():
    """Get publisher performance for Paramount advertisers"""
    try:
        advertiser_id = request.args.get('advertiser_id')
        if not advertiser_id:
            return jsonify({'success': False, 'error': 'advertiser_id required'}), 400
        
        query = """
            SELECT PUBLISHER_CODE, COUNT(*) as IMPRESSIONS
            FROM QUORUMDB.SEGMENT_DATA.PARAMOUNT_IMPRESSIONS_REPORT_90_DAYS
            WHERE QUORUM_ADVERTISER_ID = %s
            GROUP BY PUBLISHER_CODE HAVING COUNT(*) >= 100
            ORDER BY IMPRESSIONS DESC LIMIT 100
        """
        results, _ = execute_query(query, (advertiser_id,))
        
        publishers = [{'PUBLISHER': r[0], 'PUBLISHER_CODE': r[0], 'IMPRESSIONS': r[1], 'WEB_VISITS': 0} for r in results]
        return jsonify({'success': True, 'data': publishers})
        
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/web/zip-performance', methods=['GET'])
def get_web_zip_performance():
    """Get ZIP performance for Paramount - not available"""
    return jsonify({'success': True, 'data': [], 'message': 'ZIP data not available for web-only view'})


# ============================================================================
# HEALTH CHECK
# ============================================================================

@app.route('/health', methods=['GET'])
def health_check():
    return jsonify({'status': 'healthy', 'version': 'v5-complete'})


@app.route('/', methods=['GET'])
def root():
    return jsonify({
        'name': 'Quorum Optimizer API',
        'version': 'v5-complete',
        'endpoints': {
            'v3': ['/api/v3/agencies', '/api/v3/agency-overview', '/api/v3/advertiser-overview', 
                   '/api/v3/advertisers', '/api/v3/global-advertiser-overview',
                   '/api/v3/impressions-timeseries', '/api/v3/advertiser-timeseries'],
            'detail': ['/api/advertiser-summary', '/api/campaign-performance', 
                      '/api/publisher-performance', '/api/zip-performance'],
            'web': ['/api/web/summary', '/api/web/campaign-performance',
                   '/api/web/publisher-performance', '/api/web/zip-performance']
        }
    })


if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=False)
