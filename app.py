"""
Quorum Optimizer API v5 - Gold Table Architecture
Uses QUORUM_ADV_STORE_VISITS for Class A store advertisers (includes impressions!)
Uses CAMPAIGN_PERFORMANCE_REPORT_WEEKLY_STATS + CAMPAIGN_PERFORMANCE_STORE_VISITS_RAW for Class B
Uses QUORUM_ADV_WEB_VISITS + PARAMOUNT_IMPRESSIONS_REPORT for web advertisers
"""

import os
from flask import Flask, request, jsonify
from flask_cors import CORS
import snowflake.connector
from datetime import datetime, timedelta

app = Flask(__name__)
CORS(app)

# Class A agencies use QUORUM_ADV_STORE_VISITS (gold table with impressions)
CLASS_A_AGENCIES = {2514, 1956, 2298, 1955, 2086, 1950}  # MNTN, Dealer Spike, InteractRV, ARI, Level5, ByRider

# Class B agencies use CAMPAIGN_PERFORMANCE_REPORT_WEEKLY_STATS + CAMPAIGN_PERFORMANCE_STORE_VISITS_RAW
CLASS_B_AGENCIES = {1813, 2234, 1972, 2379, 1445, 1880, 2744}  # Causal iQ, Magnite, Hearst, The Shipyard, Publicis, TeamSnap, Parallel Path

# Class W (Web) agencies use PARAMOUNT_MAPPED_IMPRESSIONS + PARAMOUNT_IMPRESSIONS_REPORT_90_DAYS
CLASS_W_AGENCIES = {1480}  # ViacomCBS / Paramount - web pixel advertisers

# PT to publisher column mapping (per PT_CONFIGURATION.md)
PT_PUBLISHER_CONFIG = {
    '6': 'SITE',        # TTD
    '8': 'SITE',        # Amobee
    '9': 'SITE',        # DV360
    '11': 'SITE',       # SimpliFi
    '13': 'PUBLISHER_CODE',  # Adelphic
    '16': 'SITE',       # FreeWheel
    '20': 'SITE',       # Magnite
    '22': 'PUBLISHER_CODE',  # MNTN (URL-encoded)
    '23': 'SITE',       # StackAdapt
    '25': 'PUBLISHER_CODE',  # Platform25
    '28': 'SITE',       # Yahoo
    '33': 'PUBLISHER_CODE',  # Platform33
}

def get_snowflake_connection():
    return snowflake.connector.connect(
        user=os.environ.get('SNOWFLAKE_USER', 'OPTIMIZER_SERVICE_USER'),
        password=os.environ.get('SNOWFLAKE_PASSWORD'),
        account=os.environ.get('SNOWFLAKE_ACCOUNT', 'FZB05958.us-east-1'),
        warehouse=os.environ.get('SNOWFLAKE_WAREHOUSE', 'COMPUTE_WH'),
        database='QUORUMDB',
        schema='SEGMENT_DATA'
    )

def parse_date(date_str):
    """Parse date string in multiple formats to YYYY-MM-DD"""
    if not date_str:
        return None
    
    from datetime import datetime
    
    # Try different date formats
    for fmt in ['%Y-%m-%d', '%m/%d/%Y', '%Y/%m/%d', '%m-%d-%Y']:
        try:
            dt = datetime.strptime(date_str, fmt)
            return dt.strftime('%Y-%m-%d')
        except ValueError:
            continue
    
    # If all fail, return as-is and let SQL handle it
    return date_str

# ============================================================================
# HEALTH CHECK
# ============================================================================

@app.route('/health', methods=['GET'])
def health():
    return jsonify({
        'status': 'healthy',
        'version': 'v5',
        'tables': {
            'class_a': ['QUORUM_ADV_STORE_VISITS'],
            'class_b': ['CAMPAIGN_PERFORMANCE_REPORT_WEEKLY_STATS', 'CAMPAIGN_PERFORMANCE_STORE_VISITS_RAW', 'XANDR_IMPRESSION_LOG'],
            'class_w': ['PARAMOUNT_MAPPED_IMPRESSIONS', 'PARAMOUNT_IMPRESSIONS_REPORT_90_DAYS']
        },
        'class_a_agencies': list(CLASS_A_AGENCIES),
        'class_b_agencies': list(CLASS_B_AGENCIES),
        'class_w_agencies': list(CLASS_W_AGENCIES)
    })

# ============================================================================
# AGENCIES ENDPOINT
# ============================================================================

@app.route('/api/v5/agencies', methods=['GET'])
def get_agencies_v5():
    """Get all agencies with impression and visit counts from appropriate tables"""
    try:
        conn = get_snowflake_connection()
        cursor = conn.cursor()
        
        all_results = []
        errors = []
        
        # Query Class A agencies from gold table
        query_class_a = """
            WITH store_stats AS (
                SELECT 
                    AGENCY_ID,
                    COUNT(DISTINCT AD_IMP_ID) as IMPRESSIONS,
                    COUNT(DISTINCT CASE WHEN IS_STORE_VISIT = TRUE THEN DEVICE_ID_QU END) as STORE_VISITS,
                    COUNT(DISTINCT QUORUM_ADVERTISER_ID) as ADVERTISER_COUNT
                FROM QUORUMDB.SEGMENT_DATA.QUORUM_ADV_STORE_VISITS
                WHERE IMP_TIMESTAMP >= DATEADD(day, -90, CURRENT_DATE())
                GROUP BY AGENCY_ID
            ),
            agency_names AS (
                SELECT DISTINCT ADVERTISER_ID as AGENCY_ID, AGENCY_NAME 
                FROM QUORUMDB.SEGMENT_DATA.AGENCY_ADVERTISER
                WHERE AGENCY_NAME IS NOT NULL
            )
            SELECT 
                s.AGENCY_ID,
                COALESCE(an.AGENCY_NAME, 'Agency ' || s.AGENCY_ID) as AGENCY_NAME,
                s.IMPRESSIONS,
                0 as WEB_VISITS,
                s.STORE_VISITS as LOCATION_VISITS,
                s.ADVERTISER_COUNT,
                'A' as AGENCY_CLASS
            FROM store_stats s
            LEFT JOIN agency_names an ON s.AGENCY_ID = an.AGENCY_ID
            WHERE s.IMPRESSIONS >= 100000
        """
        
        # Query Class B agencies from weekly stats + visits raw
        query_class_b = """
            WITH impressions AS (
                SELECT AGENCY_ID, ADVERTISER_ID, SUM(IMPRESSIONS) as TOTAL_IMPRESSIONS
                FROM QUORUMDB.SEGMENT_DATA.CAMPAIGN_PERFORMANCE_REPORT_WEEKLY_STATS
                WHERE AGENCY_ID IN (1813, 2234, 1972, 2379, 1445, 1880, 2744)
                  AND LOG_DATE >= DATEADD(day, -90, CURRENT_DATE())
                GROUP BY AGENCY_ID, ADVERTISER_ID
            ),
            visits AS (
                SELECT AGENCY_ID, ADVERTISER_ID, COUNT(DISTINCT CONCAT(DEVICE_ID, DRIVE_BY_DATE, POI_MD5)) as STORE_VISITS
                FROM QUORUMDB.SEGMENT_DATA.CAMPAIGN_PERFORMANCE_STORE_VISITS_RAW
                WHERE AGENCY_ID IN (1813, 2234, 1972, 2379, 1445, 1880, 2744)
                  AND DRIVE_BY_DATE >= DATEADD(day, -90, CURRENT_DATE())
                GROUP BY AGENCY_ID, ADVERTISER_ID
            ),
            combined AS (
                SELECT 
                    COALESCE(i.AGENCY_ID, v.AGENCY_ID) as AGENCY_ID,
                    COALESCE(i.TOTAL_IMPRESSIONS, 0) as IMPRESSIONS,
                    COALESCE(v.STORE_VISITS, 0) as STORE_VISITS
                FROM impressions i
                FULL OUTER JOIN visits v ON i.AGENCY_ID = v.AGENCY_ID AND i.ADVERTISER_ID = v.ADVERTISER_ID
            ),
            agency_totals AS (
                SELECT 
                    AGENCY_ID,
                    SUM(IMPRESSIONS) as IMPRESSIONS,
                    SUM(STORE_VISITS) as STORE_VISITS,
                    COUNT(*) as ADVERTISER_COUNT
                FROM combined
                GROUP BY AGENCY_ID
                HAVING SUM(IMPRESSIONS) >= 10000 OR SUM(STORE_VISITS) >= 100
            ),
            agency_names AS (
                SELECT DISTINCT ADVERTISER_ID as AGENCY_ID, AGENCY_NAME 
                FROM QUORUMDB.SEGMENT_DATA.AGENCY_ADVERTISER
                WHERE AGENCY_NAME IS NOT NULL
            )
            SELECT 
                t.AGENCY_ID,
                COALESCE(an.AGENCY_NAME, 'Agency ' || t.AGENCY_ID) as AGENCY_NAME,
                t.IMPRESSIONS,
                0 as WEB_VISITS,
                t.STORE_VISITS as LOCATION_VISITS,
                t.ADVERTISER_COUNT,
                'B' as AGENCY_CLASS
            FROM agency_totals t
            LEFT JOIN agency_names an ON t.AGENCY_ID = an.AGENCY_ID
        """
        
        # Query Class W (ViacomCBS/Paramount) from Paramount tables - BOTH web and location visits
        query_class_w = """
            WITH impressions AS (
                SELECT 
                    COUNT(*) as IMPRESSIONS,
                    COUNT(DISTINCT IMP_MAID) as UNIQUE_VISITORS,
                    COUNT(DISTINCT QUORUM_ADVERTISER_ID) as ADVERTISER_COUNT
                FROM QUORUMDB.SEGMENT_DATA.PARAMOUNT_MAPPED_IMPRESSIONS
                WHERE IMP_DATE >= DATEADD(day, -90, CURRENT_DATE())
            ),
            conversions AS (
                SELECT 
                    COUNT(DISTINCT CASE WHEN IS_SITE_VISIT = 'TRUE' THEN IMP_MAID END) as SITE_VISITORS,
                    COUNT(DISTINCT CASE WHEN IS_STORE_VISIT = 'TRUE' THEN IMP_MAID END) as STORE_VISITORS
                FROM QUORUMDB.SEGMENT_DATA.PARAMOUNT_IMPRESSIONS_REPORT_90_DAYS
                WHERE IMP_DATE >= DATEADD(day, -90, CURRENT_DATE())
            )
            SELECT 
                1480 as AGENCY_ID,
                'ViacomCBS / Paramount' as AGENCY_NAME,
                i.IMPRESSIONS,
                c.SITE_VISITORS as WEB_VISITS,
                c.STORE_VISITORS as LOCATION_VISITS,
                i.ADVERTISER_COUNT,
                'W' as AGENCY_CLASS
            FROM impressions i
            CROSS JOIN conversions c
        """
        
        # Execute each query with error handling
        try:
            cursor.execute(query_class_a)
            results_a = [dict(zip([desc[0] for desc in cursor.description], row)) for row in cursor.fetchall()]
            all_results.extend(results_a)
        except Exception as e:
            errors.append(f"Class A: {str(e)[:50]}")
        
        try:
            cursor.execute(query_class_b)
            results_b = [dict(zip([desc[0] for desc in cursor.description], row)) for row in cursor.fetchall()]
            all_results.extend(results_b)
        except Exception as e:
            errors.append(f"Class B: {str(e)[:50]}")
        
        try:
            cursor.execute(query_class_w)
            results_w = [dict(zip([desc[0] for desc in cursor.description], row)) for row in cursor.fetchall()]
            all_results.extend(results_w)
        except Exception as e:
            errors.append(f"Class W: {str(e)[:50]}")
        
        # Sort by impressions
        all_results.sort(key=lambda x: x.get('IMPRESSIONS', 0) or 0, reverse=True)
        
        cursor.close()
        conn.close()
        
        response = {'success': True, 'data': all_results}
        if errors:
            response['warnings'] = errors
        
        return jsonify(response)
        
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

# ============================================================================
# ADVERTISERS ENDPOINT
# ============================================================================

@app.route('/api/v5/advertisers', methods=['GET'])
def get_advertisers_v5():
    """Get advertisers for an agency with impression counts"""
    agency_id = request.args.get('agency_id', type=int)
    if not agency_id:
        return jsonify({'success': False, 'error': 'agency_id required'}), 400
    
    try:
        conn = get_snowflake_connection()
        cursor = conn.cursor()
        
        # Check agency class
        if agency_id in CLASS_A_AGENCIES:
            query = """
                WITH adv_stats AS (
                    SELECT 
                        QUORUM_ADVERTISER_ID as ADVERTISER_ID,
                        COUNT(DISTINCT AD_IMP_ID) as IMPRESSIONS,
                        COUNT(DISTINCT CASE WHEN IS_STORE_VISIT = TRUE THEN DEVICE_ID_QU END) as STORE_VISITS,
                        COUNT(DISTINCT IO_ID) as CAMPAIGN_COUNT
                    FROM QUORUMDB.SEGMENT_DATA.QUORUM_ADV_STORE_VISITS
                    WHERE AGENCY_ID = %(agency_id)s
                      AND IMP_TIMESTAMP >= DATEADD(day, -90, CURRENT_DATE())
                    GROUP BY QUORUM_ADVERTISER_ID
                    HAVING COUNT(DISTINCT AD_IMP_ID) >= 1000
                )
                SELECT 
                    a.ADVERTISER_ID,
                    COALESCE(aa.COMP_NAME, 'Advertiser ' || a.ADVERTISER_ID) as ADVERTISER_NAME,
                    a.IMPRESSIONS,
                    a.STORE_VISITS,
                    a.CAMPAIGN_COUNT,
                    'A' as AGENCY_CLASS
                FROM adv_stats a
                LEFT JOIN QUORUMDB.SEGMENT_DATA.AGENCY_ADVERTISER aa 
                    ON a.ADVERTISER_ID = aa.ID
                ORDER BY a.IMPRESSIONS DESC
            """
        elif agency_id in CLASS_W_AGENCIES:
            # Class W (ViacomCBS/Paramount) query - both web and store visits
            query = """
                WITH impressions AS (
                    SELECT 
                        CAST(QUORUM_ADVERTISER_ID AS NUMBER) as ADVERTISER_ID,
                        MAX(ADVERTISER_NAME) as ADVERTISER_NAME,
                        COUNT(*) as IMPRESSIONS,
                        COUNT(DISTINCT IO_ID) as CAMPAIGN_COUNT
                    FROM QUORUMDB.SEGMENT_DATA.PARAMOUNT_MAPPED_IMPRESSIONS
                    WHERE IMP_DATE >= DATEADD(day, -90, CURRENT_DATE())
                    GROUP BY QUORUM_ADVERTISER_ID
                ),
                conversions AS (
                    SELECT 
                        QUORUM_ADVERTISER_ID as ADVERTISER_ID,
                        COUNT(DISTINCT CASE WHEN IS_SITE_VISIT = 'TRUE' THEN IMP_MAID END) as SITE_VISITORS,
                        COUNT(DISTINCT CASE WHEN IS_STORE_VISIT = 'TRUE' THEN IMP_MAID END) as STORE_VISITORS,
                        COUNT(DISTINCT CASE WHEN IS_LEAD = 'TRUE' THEN IMP_MAID END) as LEAD_VISITORS,
                        COUNT(DISTINCT CASE WHEN IS_PURCHASE = 'TRUE' THEN IMP_MAID END) as PURCHASERS
                    FROM QUORUMDB.SEGMENT_DATA.PARAMOUNT_IMPRESSIONS_REPORT_90_DAYS
                    WHERE IMP_DATE >= DATEADD(day, -90, CURRENT_DATE())
                    GROUP BY QUORUM_ADVERTISER_ID
                )
                SELECT 
                    i.ADVERTISER_ID,
                    i.ADVERTISER_NAME,
                    i.IMPRESSIONS,
                    COALESCE(c.SITE_VISITORS, 0) + COALESCE(c.STORE_VISITORS, 0) as STORE_VISITS,
                    i.CAMPAIGN_COUNT,
                    'W' as AGENCY_CLASS
                FROM impressions i
                LEFT JOIN conversions c ON i.ADVERTISER_ID = c.ADVERTISER_ID
                WHERE i.IMPRESSIONS >= 10000
                ORDER BY i.IMPRESSIONS DESC
            """
        else:
            # Class B query using weekly stats - VISITORS column directly
            query = """
                SELECT 
                    ADVERTISER_ID,
                    COALESCE(aa.COMP_NAME, 'Advertiser ' || w.ADVERTISER_ID) as ADVERTISER_NAME,
                    SUM(IMPRESSIONS) as IMPRESSIONS,
                    SUM(VISITORS) as STORE_VISITS,
                    COUNT(DISTINCT IO_ID) as CAMPAIGN_COUNT,
                    'B' as AGENCY_CLASS
                FROM QUORUMDB.SEGMENT_DATA.CAMPAIGN_PERFORMANCE_REPORT_WEEKLY_STATS w
                LEFT JOIN QUORUMDB.SEGMENT_DATA.AGENCY_ADVERTISER aa ON w.ADVERTISER_ID = aa.ID
                WHERE AGENCY_ID = %(agency_id)s
                  AND LOG_DATE >= DATEADD(day, -90, CURRENT_DATE())
                GROUP BY ADVERTISER_ID, aa.COMP_NAME
                HAVING SUM(IMPRESSIONS) >= 1000 OR SUM(VISITORS) >= 10
                ORDER BY SUM(IMPRESSIONS) DESC
            """
        
        cursor.execute(query, {'agency_id': agency_id})
        columns = [desc[0] for desc in cursor.description]
        results = [dict(zip(columns, row)) for row in cursor.fetchall()]
        
        cursor.close()
        conn.close()
        
        return jsonify({'success': True, 'data': results})
        
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

# ============================================================================
# ADVERTISER SUMMARY
# ============================================================================

@app.route('/api/v5/advertiser-summary', methods=['GET'])
def get_advertiser_summary_v5():
    """Get summary metrics for an advertiser - includes impressions!"""
    advertiser_id = request.args.get('advertiser_id', type=int)
    agency_id = request.args.get('agency_id', type=int)
    if not advertiser_id:
        return jsonify({'success': False, 'error': 'advertiser_id required'}), 400
    
    start_date = parse_date(request.args.get('start_date'))
    end_date = parse_date(request.args.get('end_date'))
    
    try:
        conn = get_snowflake_connection()
        cursor = conn.cursor()
        
        # Determine agency class
        if agency_id in CLASS_A_AGENCIES:
            agency_class = 'A'
        elif agency_id in CLASS_W_AGENCIES:
            agency_class = 'W'
        elif agency_id in CLASS_B_AGENCIES:
            agency_class = 'B'
        else:
            agency_class = 'A'  # Default to Class A
        
        if agency_class == 'A':
            date_filter = ""
            if start_date and end_date:
                date_filter = f"AND CAST(IMP_TIMESTAMP AS DATE) BETWEEN '{start_date}' AND '{end_date}'"
            else:
                date_filter = "AND IMP_TIMESTAMP >= DATEADD(day, -28, CURRENT_DATE())"
            
            query = f"""
                SELECT 
                    COUNT(DISTINCT AD_IMP_ID) as IMPRESSIONS,
                    COUNT(DISTINCT CASE WHEN IS_STORE_VISIT = TRUE THEN DEVICE_ID_QU END) as STORE_VISITS,
                    COUNT(DISTINCT IO_ID) as CAMPAIGN_COUNT,
                    COUNT(DISTINCT PUBLISHER_CODE) as PUBLISHER_COUNT,
                    COUNT(DISTINCT ZIP_CODE) as ZIP_COUNT,
                    COUNT(DISTINCT DEVICE_ID_QU) as UNIQUE_VISITORS
                FROM QUORUMDB.SEGMENT_DATA.QUORUM_ADV_STORE_VISITS
                WHERE QUORUM_ADVERTISER_ID = %(advertiser_id)s
                  {date_filter}
            """
            visit_type = 'store'
            
        elif agency_class == 'W':
            # Class W (ViacomCBS/Paramount) query - both web and store visits
            date_filter = ""
            if start_date and end_date:
                date_filter = f"AND IMP_DATE BETWEEN '{start_date}' AND '{end_date}'"
            else:
                date_filter = "AND IMP_DATE >= DATEADD(day, -28, CURRENT_DATE())"
            
            query = f"""
                WITH impressions AS (
                    SELECT 
                        COUNT(*) as IMPRESSIONS,
                        COUNT(DISTINCT IO_ID) as CAMPAIGN_COUNT,
                        COUNT(DISTINCT SITE) as PUBLISHER_COUNT,
                        COUNT(DISTINCT IMP_MAID) as UNIQUE_VISITORS
                    FROM QUORUMDB.SEGMENT_DATA.PARAMOUNT_MAPPED_IMPRESSIONS
                    WHERE QUORUM_ADVERTISER_ID = '{advertiser_id}'
                      {date_filter}
                ),
                conversions AS (
                    SELECT 
                        COUNT(DISTINCT CASE WHEN IS_SITE_VISIT = 'TRUE' THEN IMP_MAID END) as SITE_VISITORS,
                        COUNT(DISTINCT CASE WHEN IS_STORE_VISIT = 'TRUE' THEN IMP_MAID END) as STORE_VISITORS,
                        COUNT(DISTINCT CASE WHEN IS_LEAD = 'TRUE' THEN IMP_MAID END) as LEAD_VISITORS,
                        COUNT(DISTINCT CASE WHEN IS_PURCHASE = 'TRUE' THEN IMP_MAID END) as PURCHASERS,
                        COUNT(DISTINCT ZIP_CODE) as ZIP_COUNT
                    FROM QUORUMDB.SEGMENT_DATA.PARAMOUNT_IMPRESSIONS_REPORT_90_DAYS
                    WHERE QUORUM_ADVERTISER_ID = {advertiser_id}
                      {date_filter}
                )
                SELECT 
                    i.IMPRESSIONS,
                    COALESCE(c.STORE_VISITORS, 0) as STORE_VISITS,
                    COALESCE(c.SITE_VISITORS, 0) as WEB_VISITS,
                    i.CAMPAIGN_COUNT,
                    i.PUBLISHER_COUNT,
                    COALESCE(c.ZIP_COUNT, 0) as ZIP_COUNT,
                    i.UNIQUE_VISITORS,
                    COALESCE(c.LEAD_VISITORS, 0) as LEAD_VISITORS,
                    COALESCE(c.PURCHASERS, 0) as PURCHASERS
                FROM impressions i
                CROSS JOIN conversions c
            """
            visit_type = 'both'
            
        else:
            # Class B query
            date_filter_imps = ""
            date_filter_visits = ""
            if start_date and end_date:
                date_filter_imps = f"AND CAST(LOG_DATE AS DATE) BETWEEN '{start_date}' AND '{end_date}'"
                date_filter_visits = f"AND CAST(DRIVE_BY_DATE AS DATE) BETWEEN '{start_date}' AND '{end_date}'"
            else:
                date_filter_imps = "AND LOG_DATE >= DATEADD(day, -28, CURRENT_DATE())"
                date_filter_visits = "AND DRIVE_BY_DATE >= DATEADD(day, -28, CURRENT_DATE())"
            
            query = f"""
                WITH impressions AS (
                    SELECT 
                        SUM(IMPRESSIONS) as IMPRESSIONS,
                        COUNT(DISTINCT IO_ID) as CAMPAIGN_COUNT,
                        COUNT(DISTINCT PUBLISHER) as PUBLISHER_COUNT,
                        COUNT(DISTINCT ZIP) as ZIP_COUNT
                    FROM QUORUMDB.SEGMENT_DATA.CAMPAIGN_PERFORMANCE_REPORT_WEEKLY_STATS
                    WHERE ADVERTISER_ID = %(advertiser_id)s
                      {date_filter_imps}
                ),
                visits AS (
                    SELECT 
                        COUNT(DISTINCT CONCAT(DEVICE_ID, DRIVE_BY_DATE, POI_MD5)) as STORE_VISITS,
                        COUNT(DISTINCT DEVICE_ID) as UNIQUE_VISITORS
                    FROM QUORUMDB.SEGMENT_DATA.CAMPAIGN_PERFORMANCE_STORE_VISITS_RAW
                    WHERE ADVERTISER_ID = %(advertiser_id)s
                      {date_filter_visits}
                )
                SELECT 
                    COALESCE(i.IMPRESSIONS, 0) as IMPRESSIONS,
                    COALESCE(v.STORE_VISITS, 0) as STORE_VISITS,
                    COALESCE(i.CAMPAIGN_COUNT, 0) as CAMPAIGN_COUNT,
                    COALESCE(i.PUBLISHER_COUNT, 0) as PUBLISHER_COUNT,
                    COALESCE(i.ZIP_COUNT, 0) as ZIP_COUNT,
                    COALESCE(v.UNIQUE_VISITORS, 0) as UNIQUE_VISITORS
                FROM impressions i
                CROSS JOIN visits v
            """
            visit_type = 'store'
        
        cursor.execute(query, {'advertiser_id': advertiser_id})
        row = cursor.fetchone()
        columns = [desc[0] for desc in cursor.description]
        
        result = dict(zip(columns, row)) if row else {}
        
        # Calculate visit rate - for Paramount, include both web and store visits
        imps = result.get('IMPRESSIONS', 0) or 1
        store_visits = result.get('STORE_VISITS', 0) or 0
        web_visits = result.get('WEB_VISITS', 0) or 0
        total_visits = store_visits + web_visits
        result['VISIT_RATE'] = round(total_visits / imps * 100, 4)
        result['VISIT_TYPE'] = visit_type
        result['AGENCY_CLASS'] = agency_class
        
        cursor.close()
        conn.close()
        
        return jsonify({'success': True, 'data': result})
        
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

# ============================================================================
# CAMPAIGN PERFORMANCE - WITH IMPRESSIONS!
# ============================================================================

@app.route('/api/v5/campaign-performance', methods=['GET'])
def get_campaign_performance_v5():
    """Get campaign-level metrics with impressions and visit rates"""
    advertiser_id = request.args.get('advertiser_id', type=int)
    agency_id = request.args.get('agency_id', type=int)
    
    if not advertiser_id:
        return jsonify({'success': False, 'error': 'advertiser_id required'}), 400
    
    start_date = parse_date(request.args.get('start_date'))
    end_date = parse_date(request.args.get('end_date'))
    
    try:
        conn = get_snowflake_connection()
        cursor = conn.cursor()
        
        # Determine agency class
        if agency_id in CLASS_A_AGENCIES:
            agency_class = 'A'
        elif agency_id in CLASS_W_AGENCIES:
            agency_class = 'W'
        else:
            agency_class = 'B'
        
        if agency_class == 'A':
            date_filter = ""
            if start_date and end_date:
                date_filter = f"AND CAST(IMP_TIMESTAMP AS DATE) BETWEEN '{start_date}' AND '{end_date}'"
            else:
                date_filter = "AND IMP_TIMESTAMP >= DATEADD(day, -28, CURRENT_DATE())"
            
            query = f"""
                SELECT 
                    IO_ID as CAMPAIGN_ID,
                    IO_NAME as CAMPAIGN_NAME,
                    COUNT(DISTINCT AD_IMP_ID) as IMPRESSIONS,
                    0 as W_VISITS,
                    COUNT(DISTINCT CASE WHEN IS_STORE_VISIT = TRUE THEN DEVICE_ID_QU END) as S_VISITS
                FROM QUORUMDB.SEGMENT_DATA.QUORUM_ADV_STORE_VISITS
                WHERE QUORUM_ADVERTISER_ID = %(advertiser_id)s
                  {date_filter}
                GROUP BY IO_ID, IO_NAME
                HAVING COUNT(DISTINCT AD_IMP_ID) >= 1000
                ORDER BY COUNT(DISTINCT AD_IMP_ID) DESC
            """
            
        elif agency_class == 'W':
            # Class W (ViacomCBS/Paramount) query - both web and store visits
            date_filter = ""
            if start_date and end_date:
                date_filter = f"AND IMP_DATE BETWEEN '{start_date}' AND '{end_date}'"
            else:
                date_filter = "AND IMP_DATE >= DATEADD(day, -28, CURRENT_DATE())"
            
            query = f"""
                WITH impressions AS (
                    SELECT 
                        IO_ID,
                        MAX(IO_NAME) as IO_NAME,
                        COUNT(*) as IMPRESSIONS
                    FROM QUORUMDB.SEGMENT_DATA.PARAMOUNT_MAPPED_IMPRESSIONS
                    WHERE QUORUM_ADVERTISER_ID = '{advertiser_id}'
                      {date_filter}
                    GROUP BY IO_ID
                ),
                conversions AS (
                    SELECT 
                        IO_ID,
                        COUNT(DISTINCT CASE WHEN IS_SITE_VISIT = 'TRUE' THEN IMP_MAID END) as SITE_VISITORS,
                        COUNT(DISTINCT CASE WHEN IS_STORE_VISIT = 'TRUE' THEN IMP_MAID END) as STORE_VISITORS
                    FROM QUORUMDB.SEGMENT_DATA.PARAMOUNT_IMPRESSIONS_REPORT_90_DAYS
                    WHERE QUORUM_ADVERTISER_ID = {advertiser_id}
                      {date_filter}
                    GROUP BY IO_ID
                )
                SELECT 
                    i.IO_ID as CAMPAIGN_ID,
                    i.IO_NAME as CAMPAIGN_NAME,
                    i.IMPRESSIONS,
                    COALESCE(c.SITE_VISITORS, 0) as W_VISITS,
                    COALESCE(c.STORE_VISITORS, 0) as S_VISITS
                FROM impressions i
                LEFT JOIN conversions c ON i.IO_ID = c.IO_ID
                WHERE i.IMPRESSIONS >= 1000
                ORDER BY i.IMPRESSIONS DESC
            """
            
        else:
            # Class B query - use VISITORS column directly from WEEKLY_STATS
            date_filter = ""
            if start_date and end_date:
                date_filter = f"AND CAST(LOG_DATE AS DATE) BETWEEN '{start_date}' AND '{end_date}'"
            else:
                date_filter = "AND LOG_DATE >= DATEADD(day, -28, CURRENT_DATE())"
            
            query = f"""
                SELECT 
                    CAST(IO_ID AS NUMBER) as CAMPAIGN_ID,
                    MAX(IO_NAME) as CAMPAIGN_NAME,
                    SUM(IMPRESSIONS) as IMPRESSIONS,
                    0 as W_VISITS,
                    SUM(VISITORS) as S_VISITS
                FROM QUORUMDB.SEGMENT_DATA.CAMPAIGN_PERFORMANCE_REPORT_WEEKLY_STATS
                WHERE ADVERTISER_ID = %(advertiser_id)s
                  {date_filter}
                GROUP BY IO_ID
                HAVING SUM(IMPRESSIONS) >= 1000 OR SUM(VISITORS) >= 10
                ORDER BY SUM(IMPRESSIONS) DESC
            """
        
        cursor.execute(query, {'advertiser_id': advertiser_id})
        columns = [desc[0] for desc in cursor.description]
        results = [dict(zip(columns, row)) for row in cursor.fetchall()]
        
        cursor.close()
        conn.close()
        
        return jsonify({'success': True, 'data': results})
        
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

# ============================================================================
# PUBLISHER PERFORMANCE
# ============================================================================

@app.route('/api/v5/publisher-performance', methods=['GET'])
def get_publisher_performance_v5():
    """Get publisher-level metrics with impressions"""
    advertiser_id = request.args.get('advertiser_id', type=int)
    agency_id = request.args.get('agency_id', type=int)
    campaign_id = request.args.get('campaign_id')
    
    if not advertiser_id:
        return jsonify({'success': False, 'error': 'advertiser_id required'}), 400
    
    start_date = parse_date(request.args.get('start_date'))
    end_date = parse_date(request.args.get('end_date'))
    
    try:
        conn = get_snowflake_connection()
        cursor = conn.cursor()
        
        # Determine agency class
        if agency_id in CLASS_A_AGENCIES:
            agency_class = 'A'
        elif agency_id in CLASS_W_AGENCIES:
            agency_class = 'W'
        else:
            agency_class = 'B'
        
        if agency_class == 'A':
            date_filter = ""
            if start_date and end_date:
                date_filter = f"AND CAST(IMP_TIMESTAMP AS DATE) BETWEEN '{start_date}' AND '{end_date}'"
            else:
                date_filter = "AND IMP_TIMESTAMP >= DATEADD(day, -28, CURRENT_DATE())"
            
            campaign_filter = ""
            if campaign_id:
                campaign_filter = f"AND IO_ID = '{campaign_id}'"
            
            query = f"""
                SELECT 
                    PUBLISHER_CODE,
                    COUNT(DISTINCT AD_IMP_ID) as IMPRESSIONS,
                    COUNT(DISTINCT CASE WHEN IS_STORE_VISIT = TRUE THEN DEVICE_ID_QU END) as S_VISITS
                FROM QUORUMDB.SEGMENT_DATA.QUORUM_ADV_STORE_VISITS
                WHERE QUORUM_ADVERTISER_ID = %(advertiser_id)s
                  {date_filter}
                  {campaign_filter}
                GROUP BY PUBLISHER_CODE
                HAVING COUNT(DISTINCT AD_IMP_ID) >= 100
                ORDER BY COUNT(DISTINCT AD_IMP_ID) DESC
                LIMIT 50
            """
            
        elif agency_class == 'W':
            # Class W (ViacomCBS/Paramount) query - both web and store visits
            date_filter = ""
            if start_date and end_date:
                date_filter = f"AND IMP_DATE BETWEEN '{start_date}' AND '{end_date}'"
            else:
                date_filter = "AND IMP_DATE >= DATEADD(day, -28, CURRENT_DATE())"
            
            campaign_filter_imps = ""
            campaign_filter_conv = ""
            if campaign_id:
                campaign_filter_imps = f"AND IO_ID = {campaign_id}"
                campaign_filter_conv = f"AND IO_ID = {campaign_id}"
            
            query = f"""
                WITH impressions AS (
                    SELECT 
                        SITE as PUBLISHER_CODE,
                        COUNT(*) as IMPRESSIONS
                    FROM QUORUMDB.SEGMENT_DATA.PARAMOUNT_MAPPED_IMPRESSIONS
                    WHERE QUORUM_ADVERTISER_ID = '{advertiser_id}'
                      {date_filter}
                      {campaign_filter_imps}
                    GROUP BY SITE
                ),
                conversions AS (
                    SELECT 
                        SITE as PUBLISHER_CODE,
                        COUNT(DISTINCT CASE WHEN IS_SITE_VISIT = 'TRUE' THEN IMP_MAID END) as SITE_VISITORS,
                        COUNT(DISTINCT CASE WHEN IS_STORE_VISIT = 'TRUE' THEN IMP_MAID END) as STORE_VISITORS
                    FROM QUORUMDB.SEGMENT_DATA.PARAMOUNT_IMPRESSIONS_REPORT_90_DAYS
                    WHERE QUORUM_ADVERTISER_ID = {advertiser_id}
                      {date_filter}
                      {campaign_filter_conv}
                    GROUP BY SITE
                )
                SELECT 
                    COALESCE(i.PUBLISHER_CODE, c.PUBLISHER_CODE) as PUBLISHER_CODE,
                    COALESCE(i.IMPRESSIONS, 0) as IMPRESSIONS,
                    COALESCE(c.SITE_VISITORS, 0) + COALESCE(c.STORE_VISITORS, 0) as S_VISITS
                FROM impressions i
                FULL OUTER JOIN conversions c ON i.PUBLISHER_CODE = c.PUBLISHER_CODE
                WHERE COALESCE(i.IMPRESSIONS, 0) >= 100
                ORDER BY COALESCE(i.IMPRESSIONS, 0) DESC
                LIMIT 50
            """
            
        else:
            # Class B query - use VISITORS column directly from WEEKLY_STATS
            date_filter = ""
            if start_date and end_date:
                date_filter = f"AND CAST(LOG_DATE AS DATE) BETWEEN '{start_date}' AND '{end_date}'"
            else:
                date_filter = "AND LOG_DATE >= DATEADD(day, -28, CURRENT_DATE())"
            
            campaign_filter = ""
            if campaign_id:
                campaign_filter = f"AND IO_ID = '{campaign_id}'"
            
            query = f"""
                SELECT 
                    PUBLISHER as PUBLISHER_CODE,
                    SUM(IMPRESSIONS) as IMPRESSIONS,
                    SUM(VISITORS) as S_VISITS
                FROM QUORUMDB.SEGMENT_DATA.CAMPAIGN_PERFORMANCE_REPORT_WEEKLY_STATS
                WHERE ADVERTISER_ID = %(advertiser_id)s
                  {date_filter}
                  {campaign_filter}
                GROUP BY PUBLISHER
                HAVING SUM(IMPRESSIONS) >= 100
                ORDER BY SUM(IMPRESSIONS) DESC
                LIMIT 50
            """
        
        cursor.execute(query, {'advertiser_id': advertiser_id})
        columns = [desc[0] for desc in cursor.description]
        results = [dict(zip(columns, row)) for row in cursor.fetchall()]
        
        cursor.close()
        conn.close()
        
        return jsonify({'success': True, 'data': results})
        
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

# ============================================================================
# ZIP PERFORMANCE (GEOGRAPHIC)
# ============================================================================

@app.route('/api/v5/zip-performance', methods=['GET'])
def get_zip_performance_v5():
    """Get zip-level metrics with impressions"""
    advertiser_id = request.args.get('advertiser_id', type=int)
    agency_id = request.args.get('agency_id', type=int)
    campaign_id = request.args.get('campaign_id')
    
    if not advertiser_id:
        return jsonify({'success': False, 'error': 'advertiser_id required'}), 400
    
    start_date = parse_date(request.args.get('start_date'))
    end_date = parse_date(request.args.get('end_date'))
    
    try:
        conn = get_snowflake_connection()
        cursor = conn.cursor()
        
        # Determine agency class
        if agency_id in CLASS_A_AGENCIES:
            agency_class = 'A'
        elif agency_id in CLASS_W_AGENCIES:
            agency_class = 'W'
        else:
            agency_class = 'B'
        
        if agency_class == 'A':
            date_filter = ""
            if start_date and end_date:
                date_filter = f"AND CAST(IMP_TIMESTAMP AS DATE) BETWEEN '{start_date}' AND '{end_date}'"
            else:
                date_filter = "AND IMP_TIMESTAMP >= DATEADD(day, -28, CURRENT_DATE())"
            
            campaign_filter = ""
            if campaign_id:
                campaign_filter = f"AND IO_ID = '{campaign_id}'"
            
            query = f"""
                WITH zip_stats AS (
                    SELECT 
                        ZIP_CODE,
                        COUNT(DISTINCT AD_IMP_ID) as IMPRESSIONS,
                        COUNT(DISTINCT CASE WHEN IS_STORE_VISIT = TRUE THEN DEVICE_ID_QU END) as S_VISITS
                    FROM QUORUMDB.SEGMENT_DATA.QUORUM_ADV_STORE_VISITS
                    WHERE QUORUM_ADVERTISER_ID = %(advertiser_id)s
                      AND ZIP_CODE IS NOT NULL
                      AND ZIP_CODE != ''
                      {date_filter}
                      {campaign_filter}
                    GROUP BY ZIP_CODE
                    HAVING COUNT(DISTINCT AD_IMP_ID) >= 100
                ),
                ranked AS (
                    SELECT *,
                        ROW_NUMBER() OVER (ORDER BY IMPRESSIONS DESC) as imp_rank,
                        ROW_NUMBER() OVER (ORDER BY CASE WHEN IMPRESSIONS > 0 THEN S_VISITS * 1.0 / IMPRESSIONS ELSE 0 END ASC) as low_rate_rank
                    FROM zip_stats
                )
                SELECT r.ZIP_CODE, r.IMPRESSIONS, r.S_VISITS, d.DMA_NAME
                FROM ranked r
                LEFT JOIN QUORUMDB.SEGMENT_DATA.ZIP_DMA_MAPPING d ON r.ZIP_CODE = d.ZIP_CODE
                WHERE imp_rank <= 50 OR low_rate_rank <= 50
                ORDER BY r.IMPRESSIONS DESC
            """
            
        elif agency_class == 'W':
            # Class W (ViacomCBS/Paramount) - ZIP only available in conversions table
            date_filter = ""
            if start_date and end_date:
                date_filter = f"AND IMP_DATE BETWEEN '{start_date}' AND '{end_date}'"
            else:
                date_filter = "AND IMP_DATE >= DATEADD(day, -28, CURRENT_DATE())"
            
            campaign_filter = ""
            if campaign_id:
                campaign_filter = f"AND IO_ID = {campaign_id}"
            
            # Note: PARAMOUNT_MAPPED_IMPRESSIONS doesn't have ZIP, only conversions table does
            query = f"""
                WITH conversions AS (
                    SELECT 
                        ZIP_CODE,
                        COUNT(*) as CONVERSION_IMPS,
                        COUNT(DISTINCT CASE WHEN IS_SITE_VISIT = 'TRUE' THEN IMP_MAID END) as SITE_VISITORS
                    FROM QUORUMDB.SEGMENT_DATA.PARAMOUNT_IMPRESSIONS_REPORT_90_DAYS
                    WHERE QUORUM_ADVERTISER_ID = {advertiser_id}
                      AND ZIP_CODE IS NOT NULL AND ZIP_CODE != '' AND ZIP_CODE != '0'
                      {date_filter}
                      {campaign_filter}
                    GROUP BY ZIP_CODE
                    HAVING COUNT(*) >= 100
                ),
                ranked AS (
                    SELECT 
                        ZIP_CODE,
                        CONVERSION_IMPS as IMPRESSIONS,
                        SITE_VISITORS as S_VISITS,
                        ROW_NUMBER() OVER (ORDER BY CONVERSION_IMPS DESC) as imp_rank,
                        ROW_NUMBER() OVER (ORDER BY CASE WHEN CONVERSION_IMPS > 0 THEN SITE_VISITORS * 1.0 / CONVERSION_IMPS ELSE 0 END ASC) as low_rate_rank
                    FROM conversions
                )
                SELECT r.ZIP_CODE, r.IMPRESSIONS, r.S_VISITS, d.DMA_NAME
                FROM ranked r
                LEFT JOIN QUORUMDB.SEGMENT_DATA.ZIP_DMA_MAPPING d ON r.ZIP_CODE = d.ZIP_CODE
                WHERE imp_rank <= 50 OR low_rate_rank <= 50
                ORDER BY r.IMPRESSIONS DESC
            """
            
        else:
            # Class B query - use VISITORS column directly from WEEKLY_STATS
            date_filter = ""
            if start_date and end_date:
                date_filter = f"AND CAST(LOG_DATE AS DATE) BETWEEN '{start_date}' AND '{end_date}'"
            else:
                date_filter = "AND LOG_DATE >= DATEADD(day, -28, CURRENT_DATE())"
            
            campaign_filter = ""
            if campaign_id:
                campaign_filter = f"AND IO_ID = '{campaign_id}'"
            
            query = f"""
                WITH combined AS (
                    SELECT 
                        ZIP as ZIP_CODE,
                        SUM(IMPRESSIONS) as IMPRESSIONS,
                        SUM(VISITORS) as S_VISITS
                    FROM QUORUMDB.SEGMENT_DATA.CAMPAIGN_PERFORMANCE_REPORT_WEEKLY_STATS
                    WHERE ADVERTISER_ID = %(advertiser_id)s
                      AND ZIP IS NOT NULL AND ZIP != '' AND ZIP != '0'
                      {date_filter}
                      {campaign_filter}
                    GROUP BY ZIP
                    HAVING SUM(IMPRESSIONS) >= 100
                ),
                ranked AS (
                    SELECT *,
                        ROW_NUMBER() OVER (ORDER BY IMPRESSIONS DESC) as imp_rank,
                        ROW_NUMBER() OVER (ORDER BY CASE WHEN IMPRESSIONS > 0 THEN S_VISITS * 1.0 / IMPRESSIONS ELSE 0 END ASC) as low_rate_rank
                    FROM combined
                )
                SELECT r.ZIP_CODE, r.IMPRESSIONS, r.S_VISITS, d.DMA_NAME
                FROM ranked r
                LEFT JOIN QUORUMDB.SEGMENT_DATA.ZIP_DMA_MAPPING d ON r.ZIP_CODE = d.ZIP_CODE
                WHERE imp_rank <= 50 OR low_rate_rank <= 50
                ORDER BY r.IMPRESSIONS DESC
            """
        
        cursor.execute(query, {'advertiser_id': advertiser_id})
        columns = [desc[0] for desc in cursor.description]
        results = [dict(zip(columns, row)) for row in cursor.fetchall()]
        
        cursor.close()
        conn.close()
        
        return jsonify({'success': True, 'data': results})
        
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

# ============================================================================
# CREATIVE PERFORMANCE
# ============================================================================

@app.route('/api/v5/creative-performance', methods=['GET'])
def get_creative_performance_v5():
    """Get creative-level metrics"""
    advertiser_id = request.args.get('advertiser_id', type=int)
    campaign_id = request.args.get('campaign_id')
    
    if not advertiser_id:
        return jsonify({'success': False, 'error': 'advertiser_id required'}), 400
    
    start_date = parse_date(request.args.get('start_date'))
    end_date = parse_date(request.args.get('end_date'))
    
    try:
        conn = get_snowflake_connection()
        cursor = conn.cursor()
        
        date_filter = ""
        if start_date and end_date:
            date_filter = f"AND CAST(IMP_TIMESTAMP AS DATE) BETWEEN '{start_date}' AND '{end_date}'"
        else:
            date_filter = "AND IMP_TIMESTAMP >= DATEADD(day, -28, CURRENT_DATE())"
        
        campaign_filter = ""
        if campaign_id:
            campaign_filter = f"AND IO_ID = '{campaign_id}'"
        
        query = f"""
            SELECT 
                CREATIVE_ID,
                CREATIVE_NAME,
                COUNT(DISTINCT AD_IMP_ID) as IMPRESSIONS,
                COUNT(DISTINCT CASE WHEN IS_STORE_VISIT = TRUE THEN DEVICE_ID_QU END) as S_VISITS
            FROM QUORUMDB.SEGMENT_DATA.QUORUM_ADV_STORE_VISITS
            WHERE QUORUM_ADVERTISER_ID = %(advertiser_id)s
              AND CREATIVE_ID IS NOT NULL
              {date_filter}
              {campaign_filter}
            GROUP BY CREATIVE_ID, CREATIVE_NAME
            HAVING COUNT(DISTINCT AD_IMP_ID) >= 100
            ORDER BY COUNT(DISTINCT AD_IMP_ID) DESC
            LIMIT 30
        """
        
        cursor.execute(query, {'advertiser_id': advertiser_id})
        columns = [desc[0] for desc in cursor.description]
        results = [dict(zip(columns, row)) for row in cursor.fetchall()]
        
        cursor.close()
        conn.close()
        
        return jsonify({'success': True, 'data': results})
        
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

# ============================================================================
# LINEITEM PERFORMANCE
# ============================================================================

@app.route('/api/v5/lineitem-performance', methods=['GET'])
def get_lineitem_performance_v5():
    """Get line item level metrics"""
    advertiser_id = request.args.get('advertiser_id', type=int)
    campaign_id = request.args.get('campaign_id')
    
    if not advertiser_id:
        return jsonify({'success': False, 'error': 'advertiser_id required'}), 400
    
    start_date = parse_date(request.args.get('start_date'))
    end_date = parse_date(request.args.get('end_date'))
    
    try:
        conn = get_snowflake_connection()
        cursor = conn.cursor()
        
        date_filter = ""
        if start_date and end_date:
            date_filter = f"AND CAST(IMP_TIMESTAMP AS DATE) BETWEEN '{start_date}' AND '{end_date}'"
        else:
            date_filter = "AND IMP_TIMESTAMP >= DATEADD(day, -28, CURRENT_DATE())"
        
        campaign_filter = ""
        if campaign_id:
            campaign_filter = f"AND IO_ID = '{campaign_id}'"
        
        query = f"""
            SELECT 
                LINEITEM_ID,
                LINEITEM_NAME,
                IO_ID as CAMPAIGN_ID,
                IO_NAME as CAMPAIGN_NAME,
                COUNT(DISTINCT AD_IMP_ID) as IMPRESSIONS,
                COUNT(DISTINCT CASE WHEN IS_STORE_VISIT = TRUE THEN DEVICE_ID_QU END) as S_VISITS
            FROM QUORUMDB.SEGMENT_DATA.QUORUM_ADV_STORE_VISITS
            WHERE QUORUM_ADVERTISER_ID = %(advertiser_id)s
              AND LINEITEM_ID IS NOT NULL
              {date_filter}
              {campaign_filter}
            GROUP BY LINEITEM_ID, LINEITEM_NAME, IO_ID, IO_NAME
            HAVING COUNT(DISTINCT AD_IMP_ID) >= 100
            ORDER BY COUNT(DISTINCT AD_IMP_ID) DESC
            LIMIT 50
        """
        
        cursor.execute(query, {'advertiser_id': advertiser_id})
        columns = [desc[0] for desc in cursor.description]
        results = [dict(zip(columns, row)) for row in cursor.fetchall()]
        
        cursor.close()
        conn.close()
        
        return jsonify({'success': True, 'data': results})
        
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

# ============================================================================
# DMA PERFORMANCE
# ============================================================================

@app.route('/api/v5/dma-performance', methods=['GET'])
def get_dma_performance_v5():
    """Get DMA-level metrics"""
    advertiser_id = request.args.get('advertiser_id', type=int)
    campaign_id = request.args.get('campaign_id')
    
    if not advertiser_id:
        return jsonify({'success': False, 'error': 'advertiser_id required'}), 400
    
    start_date = parse_date(request.args.get('start_date'))
    end_date = parse_date(request.args.get('end_date'))
    
    try:
        conn = get_snowflake_connection()
        cursor = conn.cursor()
        
        date_filter = ""
        if start_date and end_date:
            date_filter = f"AND CAST(IMP_TIMESTAMP AS DATE) BETWEEN '{start_date}' AND '{end_date}'"
        else:
            date_filter = "AND IMP_TIMESTAMP >= DATEADD(day, -28, CURRENT_DATE())"
        
        campaign_filter = ""
        if campaign_id:
            campaign_filter = f"AND IO_ID = '{campaign_id}'"
        
        # Join with DMA lookup for names
        query = f"""
            WITH dma_stats AS (
                SELECT 
                    SUBSTRING(CENSUS_BLOCK_ID, 1, 5) as DMA_CODE,
                    COUNT(DISTINCT AD_IMP_ID) as IMPRESSIONS,
                    COUNT(DISTINCT CASE WHEN IS_STORE_VISIT = TRUE THEN DEVICE_ID_QU END) as S_VISITS
                FROM QUORUMDB.SEGMENT_DATA.QUORUM_ADV_STORE_VISITS
                WHERE QUORUM_ADVERTISER_ID = %(advertiser_id)s
                  AND CENSUS_BLOCK_ID IS NOT NULL
                  {date_filter}
                  {campaign_filter}
                GROUP BY SUBSTRING(CENSUS_BLOCK_ID, 1, 5)
                HAVING COUNT(DISTINCT AD_IMP_ID) >= 1000
            )
            SELECT 
                s.DMA_CODE,
                COALESCE(d.DMA_NAME, 'DMA ' || s.DMA_CODE) as DMA_NAME,
                s.IMPRESSIONS,
                s.S_VISITS
            FROM dma_stats s
            LEFT JOIN QUORUMDB.SEGMENT_DATA.DMA_LOOKUP d ON s.DMA_CODE = d.DMA_CODE
            ORDER BY s.IMPRESSIONS DESC
            LIMIT 30
        """
        
        cursor.execute(query, {'advertiser_id': advertiser_id})
        columns = [desc[0] for desc in cursor.description]
        results = [dict(zip(columns, row)) for row in cursor.fetchall()]
        
        cursor.close()
        conn.close()
        
        return jsonify({'success': True, 'data': results})
        
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

# ============================================================================
# BACKWARD COMPATIBILITY - V3/V4 ROUTES
# ============================================================================

# Redirect v3/v4 endpoints to v5
@app.route('/api/v3/agencies', methods=['GET'])
@app.route('/api/v4/agencies', methods=['GET'])
def agencies_compat():
    return get_agencies_v5()

@app.route('/api/v3/advertisers', methods=['GET'])
@app.route('/api/v4/advertisers', methods=['GET'])
@app.route('/api/advertisers', methods=['GET'])
def advertisers_compat():
    return get_advertisers_v5()

@app.route('/api/v3/advertiser-summary', methods=['GET'])
@app.route('/api/v4/advertiser-summary', methods=['GET'])
@app.route('/api/advertiser-summary', methods=['GET'])
def summary_compat():
    return get_advertiser_summary_v5()

@app.route('/api/v3/campaign-performance', methods=['GET'])
@app.route('/api/v4/campaign-performance', methods=['GET'])
@app.route('/api/campaign-performance', methods=['GET'])
def campaign_compat():
    return get_campaign_performance_v5()

@app.route('/api/v3/publisher-performance', methods=['GET'])
@app.route('/api/v4/publisher-performance', methods=['GET'])
@app.route('/api/publisher-performance', methods=['GET'])
def publisher_compat():
    return get_publisher_performance_v5()

@app.route('/api/v3/zip-performance', methods=['GET'])
@app.route('/api/v4/zip-performance', methods=['GET'])
@app.route('/api/zip-performance', methods=['GET'])
def zip_compat():
    return get_zip_performance_v5()

@app.route('/api/v3/creative-performance', methods=['GET'])
@app.route('/api/v4/creative-performance', methods=['GET'])
@app.route('/api/creative-performance', methods=['GET'])
def creative_compat():
    return get_creative_performance_v5()

# Overview endpoint compatibility
@app.route('/api/v3/agency-overview', methods=['GET'])
@app.route('/api/v4/agency-overview', methods=['GET'])
def agency_overview_compat():
    return get_agency_overview_v5()

@app.route('/api/v3/advertiser-overview', methods=['GET'])
@app.route('/api/v4/advertiser-overview', methods=['GET'])
def advertiser_overview_compat():
    return get_advertiser_overview_v5()

@app.route('/api/v3/impressions-timeseries', methods=['GET'])
@app.route('/api/v4/impressions-timeseries', methods=['GET'])
def impressions_timeseries_compat():
    return get_impressions_timeseries_v5()

@app.route('/api/v3/advertiser-timeseries', methods=['GET'])
@app.route('/api/v4/advertiser-timeseries', methods=['GET'])
def advertiser_timeseries_compat():
    return get_impressions_timeseries_v5()

@app.route('/api/v3/global-advertiser-overview', methods=['GET'])
@app.route('/api/v4/global-advertiser-overview', methods=['GET'])
def global_advertiser_overview_compat():
    """Get all advertisers across all agencies"""
    try:
        conn = get_snowflake_connection()
        cursor = conn.cursor()
        
        query = """
            SELECT 
                AGENCY_ID,
                AGENCY_NAME,
                ADVERTISER_ID,
                ADVERTISER_NAME,
                SUM(IMPRESSIONS) as IMPRESSIONS,
                SUM(COALESCE(SITE_VISITS, 0)) as WEB_VISITS,
                SUM(COALESCE(TEST_VISITORS, 0)) as LOCATION_VISITS
            FROM QUORUMDB.SEGMENT_DATA.DAILY_ADVERTISER_REPORTING
            WHERE LOG_DATE >= DATEADD(day, -90, CURRENT_DATE())
            GROUP BY AGENCY_ID, AGENCY_NAME, ADVERTISER_ID, ADVERTISER_NAME
            ORDER BY IMPRESSIONS DESC
            LIMIT 200
        """
        
        cursor.execute(query)
        columns = [desc[0] for desc in cursor.description]
        results = [dict(zip(columns, row)) for row in cursor.fetchall()]
        
        cursor.close()
        conn.close()
        
        return jsonify({'success': True, 'data': results})
        
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

# ============================================================================
# OVERVIEW ENDPOINTS (for dashboard screens) - FIXED TO SHOW MNTN & VIACOM
# ============================================================================

@app.route('/api/v5/agency-overview', methods=['GET'])
def get_agency_overview_v5():
    """Get all agencies for overview with date filtering"""
    try:
        # Accept date parameters from query string
        start_date = parse_date(request.args.get('start_date'))
        end_date = parse_date(request.args.get('end_date'))
        
        # Calculate default date range if not provided (last 30 days)
        if not start_date or not end_date:
            from datetime import datetime, timedelta
            end = datetime.now()
            start = end - timedelta(days=30)
            start_date = start.strftime('%Y-%m-%d')
            end_date = end.strftime('%Y-%m-%d')
        
        # Query agencies directly with date filtering
        conn = get_snowflake_connection()
        cursor = conn.cursor()
        
        all_results = []
        errors = []
        
        # Log the dates being used for debugging
        print(f"[OVERVIEW DEBUG] Date range: {start_date} to {end_date}")
        
        # Class A - with date filter and lower threshold
        query_a = f"""
            WITH store_stats AS (
                SELECT 
                    AGENCY_ID,
                    COUNT(DISTINCT AD_IMP_ID) as IMPRESSIONS,
                    COUNT(DISTINCT CASE WHEN IS_STORE_VISIT = TRUE THEN DEVICE_ID_QU END) as STORE_VISITS,
                    COUNT(DISTINCT QUORUM_ADVERTISER_ID) as ADVERTISER_COUNT
                FROM QUORUMDB.SEGMENT_DATA.QUORUM_ADV_STORE_VISITS
                WHERE IMP_TIMESTAMP >= TO_DATE('{start_date}', 'YYYY-MM-DD')
                  AND IMP_TIMESTAMP < DATEADD(day, 1, TO_DATE('{end_date}', 'YYYY-MM-DD'))
                GROUP BY AGENCY_ID
            ),
            agency_names AS (
                SELECT DISTINCT ADVERTISER_ID as AGENCY_ID, AGENCY_NAME 
                FROM QUORUMDB.SEGMENT_DATA.AGENCY_ADVERTISER
                WHERE AGENCY_NAME IS NOT NULL
            )
            SELECT 
                s.AGENCY_ID,
                COALESCE(an.AGENCY_NAME, 'Agency ' || s.AGENCY_ID) as AGENCY_NAME,
                s.IMPRESSIONS,
                0 as WEB_VISITS,
                s.STORE_VISITS as LOCATION_VISITS,
                s.ADVERTISER_COUNT
            FROM store_stats s
            LEFT JOIN agency_names an ON s.AGENCY_ID = an.AGENCY_ID
            WHERE s.IMPRESSIONS >= 1000
        """
        
        try:
            cursor.execute(query_a)
            results = [dict(zip([d[0] for d in cursor.description], row)) for row in cursor.fetchall()]
            all_results.extend(results)
            print(f"[CLASS A] Returned {len(results)} agencies")
            if not results:
                errors.append(f"Class A: Query executed but returned 0 results (Date range: {start_date} to {end_date})")
        except Exception as e:
            print(f"[CLASS A ERROR] {str(e)}")
            errors.append(f"Class A ERROR: {str(e)}")
        
        # Class B - with date filter and lower threshold
        query_b = f"""
            WITH impressions AS (
                SELECT AGENCY_ID, ADVERTISER_ID, SUM(IMPRESSIONS) as TOTAL_IMPRESSIONS
                FROM QUORUMDB.SEGMENT_DATA.CAMPAIGN_PERFORMANCE_REPORT_WEEKLY_STATS
                WHERE AGENCY_ID IN (1813, 2234, 1972, 2379, 1445, 1880, 2744)
                  AND LOG_DATE >= TO_DATE('{start_date}', 'YYYY-MM-DD')
                  AND LOG_DATE <= TO_DATE('{end_date}', 'YYYY-MM-DD')
                GROUP BY AGENCY_ID, ADVERTISER_ID
            ),
            visits AS (
                SELECT AGENCY_ID, ADVERTISER_ID, COUNT(DISTINCT CONCAT(DEVICE_ID, DRIVE_BY_DATE, POI_MD5)) as STORE_VISITS
                FROM QUORUMDB.SEGMENT_DATA.CAMPAIGN_PERFORMANCE_STORE_VISITS_RAW
                WHERE AGENCY_ID IN (1813, 2234, 1972, 2379, 1445, 1880, 2744)
                  AND DRIVE_BY_DATE >= TO_DATE('{start_date}', 'YYYY-MM-DD')
                  AND DRIVE_BY_DATE <= TO_DATE('{end_date}', 'YYYY-MM-DD')
                GROUP BY AGENCY_ID, ADVERTISER_ID
            ),
            combined AS (
                SELECT 
                    COALESCE(i.AGENCY_ID, v.AGENCY_ID) as AGENCY_ID,
                    COALESCE(i.TOTAL_IMPRESSIONS, 0) as IMPRESSIONS,
                    COALESCE(v.STORE_VISITS, 0) as STORE_VISITS
                FROM impressions i
                FULL OUTER JOIN visits v ON i.AGENCY_ID = v.AGENCY_ID AND i.ADVERTISER_ID = v.ADVERTISER_ID
            ),
            agency_totals AS (
                SELECT 
                    AGENCY_ID,
                    SUM(IMPRESSIONS) as IMPRESSIONS,
                    SUM(STORE_VISITS) as STORE_VISITS,
                    COUNT(*) as ADVERTISER_COUNT
                FROM combined
                GROUP BY AGENCY_ID
            ),
            agency_names AS (
                SELECT DISTINCT ADVERTISER_ID as AGENCY_ID, AGENCY_NAME 
                FROM QUORUMDB.SEGMENT_DATA.AGENCY_ADVERTISER
                WHERE AGENCY_NAME IS NOT NULL
            )
            SELECT 
                t.AGENCY_ID,
                COALESCE(an.AGENCY_NAME, 'Agency ' || t.AGENCY_ID) as AGENCY_NAME,
                t.IMPRESSIONS,
                0 as WEB_VISITS,
                t.STORE_VISITS as LOCATION_VISITS,
                t.ADVERTISER_COUNT
            FROM agency_totals t
            LEFT JOIN agency_names an ON t.AGENCY_ID = an.AGENCY_ID
            WHERE t.IMPRESSIONS >= 1000 OR t.STORE_VISITS >= 10
        """
        
        try:
            cursor.execute(query_b)
            results = [dict(zip([d[0] for d in cursor.description], row)) for row in cursor.fetchall()]
            all_results.extend(results)
            if not results:
                errors.append(f"Class B: Query executed but returned 0 results")
        except Exception as e:
            errors.append(f"Class B ERROR: {str(e)}")
        
        # Class W (Paramount) - with date filter
        query_w = f"""
            WITH impressions AS (
                SELECT 
                    COUNT(*) as IMPRESSIONS,
                    COUNT(DISTINCT QUORUM_ADVERTISER_ID) as ADVERTISER_COUNT
                FROM QUORUMDB.SEGMENT_DATA.PARAMOUNT_MAPPED_IMPRESSIONS
                WHERE CAST(IMP_DATE AS DATE) BETWEEN '{start_date}' AND '{end_date}'
            ),
            conversions AS (
                SELECT 
                    COUNT(DISTINCT CASE WHEN IS_SITE_VISIT = 'TRUE' THEN IMP_MAID END) as SITE_VISITORS,
                    COUNT(DISTINCT CASE WHEN IS_STORE_VISIT = 'TRUE' THEN IMP_MAID END) as STORE_VISITORS
                FROM QUORUMDB.SEGMENT_DATA.PARAMOUNT_IMPRESSIONS_REPORT_90_DAYS
                WHERE CAST(IMP_DATE AS DATE) BETWEEN '{start_date}' AND '{end_date}'
            )
            SELECT 
                1480 as AGENCY_ID,
                'ViacomCBS / Paramount' as AGENCY_NAME,
                i.IMPRESSIONS,
                c.SITE_VISITORS as WEB_VISITS,
                c.STORE_VISITORS as LOCATION_VISITS,
                i.ADVERTISER_COUNT
            FROM impressions i
            CROSS JOIN conversions c
            WHERE i.IMPRESSIONS >= 1000
        """
        
        try:
            cursor.execute(query_w)
            results = [dict(zip([d[0] for d in cursor.description], row)) for row in cursor.fetchall()]
            all_results.extend(results)
            print(f"[CLASS W] Returned {len(results)} agencies")
            if not results:
                errors.append(f"Class W: Query executed but returned 0 results (Date range: {start_date} to {end_date})")
        except Exception as e:
            print(f"[CLASS W ERROR] {str(e)}")
            errors.append(f"Class W ERROR: {str(e)}")
        
        cursor.close()
        conn.close()
        
        # Sort by impressions
        all_results.sort(key=lambda x: x.get('IMPRESSIONS', 0) or 0, reverse=True)
        
        response = {'success': True, 'data': all_results}
        if errors:
            response['warnings'] = errors
        
        return jsonify(response)
        
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/v5/advertiser-overview', methods=['GET'])
def get_advertiser_overview_v5():
    """Get advertisers for an agency - delegates to /advertisers endpoint"""
    agency_id = request.args.get('agency_id', type=int)
    
    if not agency_id:
        # Global view not supported, return empty
        return jsonify({'success': True, 'data': []})
    
    try:
        # Reuse the advertisers endpoint which has the correct logic
        advertisers_response = get_advertisers_v5()
        advertisers_data = advertisers_response.get_json()
        
        if not advertisers_data.get('success'):
            return advertisers_response
        
        # Transform to overview format (STORE_VISITS becomes WEB_VISITS or LOCATION_VISITS)
        overview_data = []
        for adv in advertisers_data.get('data', []):
            agency_class = adv.get('AGENCY_CLASS', 'A')
            store_visits = adv.get('STORE_VISITS', 0)
            
            overview_data.append({
                'ADVERTISER_ID': adv['ADVERTISER_ID'],
                'ADVERTISER_NAME': adv['ADVERTISER_NAME'],
                'IMPRESSIONS': adv.get('IMPRESSIONS', 0),
                # Class W = web visits, others = location visits
                'WEB_VISITS': store_visits if agency_class == 'W' else 0,
                'LOCATION_VISITS': store_visits if agency_class != 'W' else 0,
            })
        
        return jsonify({'success': True, 'data': overview_data})
        
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/v5/impressions-timeseries', methods=['GET'])
def get_impressions_timeseries_v5():
    """Get daily impressions by agency/advertiser - queries correct source tables"""
    agency_id = request.args.get('agency_id', type=int)
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    
    try:
        conn = get_snowflake_connection()
        cursor = conn.cursor()
        
        # Calculate default date range if not provided (last 30 days)
        if not start_date or not end_date:
            from datetime import datetime, timedelta
            end = datetime.now()
            start = end - timedelta(days=30)
            start_date = start.strftime('%Y-%m-%d')
            end_date = end.strftime('%Y-%m-%d')
        
        if agency_id:
            # Determine agency class
            if agency_id in CLASS_A_AGENCIES:
                # Class A - Query QUORUM_ADV_STORE_VISITS
                query = f"""
                    WITH ranked AS (
                        SELECT 
                            QUORUM_ADVERTISER_ID,
                            MAX(aa.COMP_NAME) as ADVERTISER_NAME,
                            COUNT(DISTINCT AD_IMP_ID) as total_imps,
                            ROW_NUMBER() OVER (ORDER BY COUNT(DISTINCT AD_IMP_ID) DESC) as rn
                        FROM QUORUMDB.SEGMENT_DATA.QUORUM_ADV_STORE_VISITS v
                        LEFT JOIN QUORUMDB.SEGMENT_DATA.AGENCY_ADVERTISER aa ON v.QUORUM_ADVERTISER_ID = aa.ID
                        WHERE AGENCY_ID = %(agency_id)s
                          AND IMP_TIMESTAMP >= TO_DATE('{start_date}', 'YYYY-MM-DD')
                          AND IMP_TIMESTAMP < DATEADD(day, 1, TO_DATE('{end_date}', 'YYYY-MM-DD'))
                        GROUP BY QUORUM_ADVERTISER_ID
                    )
                    SELECT 
                        CAST(v.IMP_TIMESTAMP AS DATE) as LOG_DATE,
                        CASE WHEN r.rn <= 10 THEN r.ADVERTISER_NAME ELSE 'Others' END as ENTITY_NAME,
                        COUNT(DISTINCT v.AD_IMP_ID) as IMPRESSIONS
                    FROM QUORUMDB.SEGMENT_DATA.QUORUM_ADV_STORE_VISITS v
                    JOIN ranked r ON v.QUORUM_ADVERTISER_ID = r.QUORUM_ADVERTISER_ID
                    WHERE v.AGENCY_ID = %(agency_id)s
                      AND v.IMP_TIMESTAMP >= TO_DATE('{start_date}', 'YYYY-MM-DD')
                      AND v.IMP_TIMESTAMP < DATEADD(day, 1, TO_DATE('{end_date}', 'YYYY-MM-DD'))
                    GROUP BY CAST(v.IMP_TIMESTAMP AS DATE), CASE WHEN r.rn <= 10 THEN r.ADVERTISER_NAME ELSE 'Others' END
                    ORDER BY LOG_DATE, IMPRESSIONS DESC
                """
                cursor.execute(query, {'agency_id': agency_id})
                
            elif agency_id in CLASS_W_AGENCIES:
                # Class W (Paramount) - Query PARAMOUNT_MAPPED_IMPRESSIONS
                query = f"""
                    WITH ranked AS (
                        SELECT 
                            QUORUM_ADVERTISER_ID,
                            MAX(ADVERTISER_NAME) as ADVERTISER_NAME,
                            COUNT(*) as total_imps,
                            ROW_NUMBER() OVER (ORDER BY COUNT(*) DESC) as rn
                        FROM QUORUMDB.SEGMENT_DATA.PARAMOUNT_MAPPED_IMPRESSIONS
                        WHERE IMP_DATE >= TO_DATE('{start_date}', 'YYYY-MM-DD')
                          AND IMP_DATE <= TO_DATE('{end_date}', 'YYYY-MM-DD')
                        GROUP BY QUORUM_ADVERTISER_ID
                    )
                    SELECT 
                        CAST(p.IMP_DATE AS DATE) as LOG_DATE,
                        CASE WHEN r.rn <= 10 THEN r.ADVERTISER_NAME ELSE 'Others' END as ENTITY_NAME,
                        COUNT(*) as IMPRESSIONS
                    FROM QUORUMDB.SEGMENT_DATA.PARAMOUNT_MAPPED_IMPRESSIONS p
                    JOIN ranked r ON p.QUORUM_ADVERTISER_ID = r.QUORUM_ADVERTISER_ID
                    WHERE p.IMP_DATE >= TO_DATE('{start_date}', 'YYYY-MM-DD')
                      AND p.IMP_DATE <= TO_DATE('{end_date}', 'YYYY-MM-DD')
                    GROUP BY CAST(p.IMP_DATE AS DATE), CASE WHEN r.rn <= 10 THEN r.ADVERTISER_NAME ELSE 'Others' END
                    ORDER BY LOG_DATE, IMPRESSIONS DESC
                """
                cursor.execute(query)
                
            else:
                # Class B - Query DAILY_ADVERTISER_REPORTING (works for them)
                query = f"""
                    WITH ranked AS (
                        SELECT 
                            ADVERTISER_ID,
                            ADVERTISER_NAME,
                            SUM(IMPRESSIONS) as total_imps,
                            ROW_NUMBER() OVER (ORDER BY SUM(IMPRESSIONS) DESC) as rn
                        FROM QUORUMDB.SEGMENT_DATA.DAILY_ADVERTISER_REPORTING
                        WHERE AGENCY_ID = %(agency_id)s
                          AND CAST(LOG_DATE AS DATE) BETWEEN '{start_date}' AND '{end_date}'
                        GROUP BY ADVERTISER_ID, ADVERTISER_NAME
                    )
                    SELECT 
                        d.LOG_DATE,
                        CASE WHEN r.rn <= 10 THEN d.ADVERTISER_NAME ELSE 'Others' END as ENTITY_NAME,
                        SUM(d.IMPRESSIONS) as IMPRESSIONS
                    FROM QUORUMDB.SEGMENT_DATA.DAILY_ADVERTISER_REPORTING d
                    JOIN ranked r ON d.ADVERTISER_ID = r.ADVERTISER_ID
                    WHERE d.AGENCY_ID = %(agency_id)s
                      AND CAST(d.LOG_DATE AS DATE) BETWEEN '{start_date}' AND '{end_date}'
                    GROUP BY d.LOG_DATE, CASE WHEN r.rn <= 10 THEN d.ADVERTISER_NAME ELSE 'Others' END
                    ORDER BY d.LOG_DATE, SUM(d.IMPRESSIONS) DESC
                """
                cursor.execute(query, {'agency_id': agency_id})
        else:
            # Global view - use DAILY_ADVERTISER_REPORTING (best we can do for overview)
            query = f"""
                SELECT 
                    LOG_DATE,
                    AGENCY_NAME as ENTITY_NAME,
                    SUM(IMPRESSIONS) as IMPRESSIONS
                FROM QUORUMDB.SEGMENT_DATA.DAILY_ADVERTISER_REPORTING
                WHERE CAST(LOG_DATE AS DATE) BETWEEN '{start_date}' AND '{end_date}'
                GROUP BY LOG_DATE, AGENCY_NAME
                ORDER BY LOG_DATE, SUM(IMPRESSIONS) DESC
            """
            cursor.execute(query)
        
        columns = [desc[0] for desc in cursor.description]
        results = [dict(zip(columns, row)) for row in cursor.fetchall()]
        
        cursor.close()
        conn.close()
        
        # Return empty array if no data (instead of error)
        return jsonify({
            'success': True, 
            'data': results if results else []
        })
        
    except Exception as e:
        # Return empty data on error instead of failing
        return jsonify({
            'success': True, 
            'data': [], 
            'warning': str(e)
        })
        
        columns = [desc[0] for desc in cursor.description]
        results = [dict(zip(columns, row)) for row in cursor.fetchall()]
        
        cursor.close()
        conn.close()
        
        # Return empty array if no data (instead of error)
        return jsonify({'success': True, 'data': results if results else []})
        
    except Exception as e:
        # Return empty data on error instead of failing
        return jsonify({'success': True, 'data': [], 'warning': str(e)})

# ============================================================================
# RUN
# ============================================================================

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8080))
    app.run(host='0.0.0.0', port=port, debug=False)
