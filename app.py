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
                s.STORE_VISITS,
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
                t.STORE_VISITS,
                t.ADVERTISER_COUNT,
                'B' as AGENCY_CLASS
            FROM agency_totals t
            LEFT JOIN agency_names an ON t.AGENCY_ID = an.AGENCY_ID
        """
        
        # Query Class W (ViacomCBS/Paramount) from Paramount tables
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
                    COUNT(DISTINCT CASE WHEN IS_SITE_VISIT = 'TRUE' THEN IMP_MAID END) as SITE_VISITORS
                FROM QUORUMDB.SEGMENT_DATA.PARAMOUNT_IMPRESSIONS_REPORT_90_DAYS
                WHERE IMP_DATE >= DATEADD(day, -90, CURRENT_DATE())
            )
            SELECT 
                1480 as AGENCY_ID,
                'ViacomCBS / Paramount' as AGENCY_NAME,
                i.IMPRESSIONS,
                c.SITE_VISITORS as STORE_VISITS,
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
                    ON a.ADVERTISER_ID = aa.ADVERTISER_ID
                ORDER BY a.IMPRESSIONS DESC
            """
        elif agency_id in CLASS_W_AGENCIES:
            # Class W (ViacomCBS/Paramount) query
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
                    COALESCE(c.SITE_VISITORS, 0) as STORE_VISITS,
                    i.CAMPAIGN_COUNT,
                    'W' as AGENCY_CLASS
                FROM impressions i
                LEFT JOIN conversions c ON i.ADVERTISER_ID = c.ADVERTISER_ID
                WHERE i.IMPRESSIONS >= 10000
                ORDER BY i.IMPRESSIONS DESC
            """
        else:
            # Class B query using weekly stats + visits raw
            query = """
                WITH impressions AS (
                    SELECT 
                        ADVERTISER_ID,
                        SUM(IMPRESSIONS) as IMPRESSIONS,
                        COUNT(DISTINCT IO_ID) as CAMPAIGN_COUNT
                    FROM QUORUMDB.SEGMENT_DATA.CAMPAIGN_PERFORMANCE_REPORT_WEEKLY_STATS
                    WHERE AGENCY_ID = %(agency_id)s
                      AND LOG_DATE >= DATEADD(day, -90, CURRENT_DATE())
                    GROUP BY ADVERTISER_ID
                ),
                visits AS (
                    SELECT 
                        ADVERTISER_ID,
                        COUNT(DISTINCT CONCAT(DEVICE_ID, DRIVE_BY_DATE, POI_MD5)) as STORE_VISITS
                    FROM QUORUMDB.SEGMENT_DATA.CAMPAIGN_PERFORMANCE_STORE_VISITS_RAW
                    WHERE AGENCY_ID = %(agency_id)s
                      AND DRIVE_BY_DATE >= DATEADD(day, -90, CURRENT_DATE())
                    GROUP BY ADVERTISER_ID
                )
                SELECT 
                    COALESCE(i.ADVERTISER_ID, v.ADVERTISER_ID) as ADVERTISER_ID,
                    COALESCE(aa.COMP_NAME, 'Advertiser ' || COALESCE(i.ADVERTISER_ID, v.ADVERTISER_ID)) as ADVERTISER_NAME,
                    COALESCE(i.IMPRESSIONS, 0) as IMPRESSIONS,
                    COALESCE(v.STORE_VISITS, 0) as STORE_VISITS,
                    COALESCE(i.CAMPAIGN_COUNT, 0) as CAMPAIGN_COUNT,
                    'B' as AGENCY_CLASS
                FROM impressions i
                FULL OUTER JOIN visits v ON i.ADVERTISER_ID = v.ADVERTISER_ID
                LEFT JOIN QUORUMDB.SEGMENT_DATA.AGENCY_ADVERTISER aa 
                    ON COALESCE(i.ADVERTISER_ID, v.ADVERTISER_ID) = aa.ADVERTISER_ID
                WHERE COALESCE(i.IMPRESSIONS, 0) >= 1000 OR COALESCE(v.STORE_VISITS, 0) >= 10
                ORDER BY COALESCE(i.IMPRESSIONS, 0) DESC
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
    
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    
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
            # Class W (ViacomCBS/Paramount) query
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
                        COUNT(DISTINCT CASE WHEN IS_LEAD = 'TRUE' THEN IMP_MAID END) as LEAD_VISITORS,
                        COUNT(DISTINCT CASE WHEN IS_PURCHASE = 'TRUE' THEN IMP_MAID END) as PURCHASERS,
                        COUNT(DISTINCT CASE WHEN IS_SITE_VISIT = 'TRUE' THEN ZIP_CODE END) as ZIP_COUNT
                    FROM QUORUMDB.SEGMENT_DATA.PARAMOUNT_IMPRESSIONS_REPORT_90_DAYS
                    WHERE QUORUM_ADVERTISER_ID = {advertiser_id}
                      {date_filter}
                )
                SELECT 
                    i.IMPRESSIONS,
                    COALESCE(c.SITE_VISITORS, 0) as STORE_VISITS,
                    i.CAMPAIGN_COUNT,
                    i.PUBLISHER_COUNT,
                    COALESCE(c.ZIP_COUNT, 0) as ZIP_COUNT,
                    i.UNIQUE_VISITORS,
                    COALESCE(c.LEAD_VISITORS, 0) as LEAD_VISITORS,
                    COALESCE(c.PURCHASERS, 0) as PURCHASERS
                FROM impressions i
                CROSS JOIN conversions c
            """
            visit_type = 'web'
            
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
        
        # Calculate visit rate
        imps = result.get('IMPRESSIONS', 0) or 1
        visits = result.get('STORE_VISITS', 0) or 0
        result['VISIT_RATE'] = round(visits / imps * 100, 4)
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
    
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    
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
                    COUNT(DISTINCT CASE WHEN IS_STORE_VISIT = TRUE THEN DEVICE_ID_QU END) as S_VISITS
                FROM QUORUMDB.SEGMENT_DATA.QUORUM_ADV_STORE_VISITS
                WHERE QUORUM_ADVERTISER_ID = %(advertiser_id)s
                  {date_filter}
                GROUP BY IO_ID, IO_NAME
                HAVING COUNT(DISTINCT AD_IMP_ID) >= 1000
                ORDER BY COUNT(DISTINCT AD_IMP_ID) DESC
            """
            
        elif agency_class == 'W':
            # Class W (ViacomCBS/Paramount) query
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
                        COUNT(DISTINCT CASE WHEN IS_SITE_VISIT = 'TRUE' THEN IMP_MAID END) as SITE_VISITORS
                    FROM QUORUMDB.SEGMENT_DATA.PARAMOUNT_IMPRESSIONS_REPORT_90_DAYS
                    WHERE QUORUM_ADVERTISER_ID = {advertiser_id}
                      {date_filter}
                    GROUP BY IO_ID
                )
                SELECT 
                    i.IO_ID as CAMPAIGN_ID,
                    i.IO_NAME as CAMPAIGN_NAME,
                    i.IMPRESSIONS,
                    COALESCE(c.SITE_VISITORS, 0) as S_VISITS
                FROM impressions i
                LEFT JOIN conversions c ON i.IO_ID = c.IO_ID
                WHERE i.IMPRESSIONS >= 1000
                ORDER BY i.IMPRESSIONS DESC
            """
            
        else:
            # Class B query - join visits to XANDR for campaign context
            date_filter_imps = ""
            date_filter_visits = ""
            if start_date and end_date:
                date_filter_imps = f"AND CAST(LOG_DATE AS DATE) BETWEEN '{start_date}' AND '{end_date}'"
                date_filter_visits = f"AND CAST(v.DRIVE_BY_DATE AS DATE) BETWEEN '{start_date}' AND '{end_date}'"
            else:
                date_filter_imps = "AND LOG_DATE >= DATEADD(day, -28, CURRENT_DATE())"
                date_filter_visits = "AND v.DRIVE_BY_DATE >= DATEADD(day, -28, CURRENT_DATE())"
            
            query = f"""
                WITH imps AS (
                    SELECT 
                        CAST(IO_ID AS NUMBER) as IO_ID,
                        MAX(IO_NAME) as IO_NAME,
                        SUM(IMPRESSIONS) as IMPRESSIONS
                    FROM QUORUMDB.SEGMENT_DATA.CAMPAIGN_PERFORMANCE_REPORT_WEEKLY_STATS
                    WHERE ADVERTISER_ID = %(advertiser_id)s
                      {date_filter_imps}
                    GROUP BY IO_ID
                ),
                visits AS (
                    SELECT 
                        x.IO_ID,
                        COUNT(DISTINCT CONCAT(v.DEVICE_ID, v.DRIVE_BY_DATE, v.POI_MD5)) as STORE_VISITS
                    FROM QUORUMDB.SEGMENT_DATA.CAMPAIGN_PERFORMANCE_STORE_VISITS_RAW v
                    JOIN QUORUMDB.SEGMENT_DATA.XANDR_IMPRESSION_LOG x ON v.IMP_ID = x.ID
                    WHERE v.ADVERTISER_ID = %(advertiser_id)s
                      {date_filter_visits}
                    GROUP BY x.IO_ID
                )
                SELECT 
                    COALESCE(i.IO_ID, v.IO_ID) as CAMPAIGN_ID,
                    i.IO_NAME as CAMPAIGN_NAME,
                    COALESCE(i.IMPRESSIONS, 0) as IMPRESSIONS,
                    COALESCE(v.STORE_VISITS, 0) as S_VISITS
                FROM imps i
                FULL OUTER JOIN visits v ON i.IO_ID = v.IO_ID
                WHERE COALESCE(i.IMPRESSIONS, 0) >= 1000 OR COALESCE(v.STORE_VISITS, 0) >= 10
                ORDER BY COALESCE(i.IMPRESSIONS, 0) DESC
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
    
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    
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
            # Class W (ViacomCBS/Paramount) query
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
                        COUNT(DISTINCT CASE WHEN IS_SITE_VISIT = 'TRUE' THEN IMP_MAID END) as SITE_VISITORS
                    FROM QUORUMDB.SEGMENT_DATA.PARAMOUNT_IMPRESSIONS_REPORT_90_DAYS
                    WHERE QUORUM_ADVERTISER_ID = {advertiser_id}
                      {date_filter}
                      {campaign_filter_conv}
                    GROUP BY SITE
                )
                SELECT 
                    COALESCE(i.PUBLISHER_CODE, c.PUBLISHER_CODE) as PUBLISHER_CODE,
                    COALESCE(i.IMPRESSIONS, 0) as IMPRESSIONS,
                    COALESCE(c.SITE_VISITORS, 0) as S_VISITS
                FROM impressions i
                FULL OUTER JOIN conversions c ON i.PUBLISHER_CODE = c.PUBLISHER_CODE
                WHERE COALESCE(i.IMPRESSIONS, 0) >= 100
                ORDER BY COALESCE(i.IMPRESSIONS, 0) DESC
                LIMIT 50
            """
            
        else:
            # Class B query - join visits to XANDR for publisher context
            date_filter_imps = ""
            date_filter_visits = ""
            if start_date and end_date:
                date_filter_imps = f"AND CAST(LOG_DATE AS DATE) BETWEEN '{start_date}' AND '{end_date}'"
                date_filter_visits = f"AND CAST(v.DRIVE_BY_DATE AS DATE) BETWEEN '{start_date}' AND '{end_date}'"
            else:
                date_filter_imps = "AND LOG_DATE >= DATEADD(day, -28, CURRENT_DATE())"
                date_filter_visits = "AND v.DRIVE_BY_DATE >= DATEADD(day, -28, CURRENT_DATE())"
            
            campaign_filter_imps = ""
            campaign_filter_visits = ""
            if campaign_id:
                campaign_filter_imps = f"AND IO_ID = '{campaign_id}'"
                campaign_filter_visits = f"AND x.IO_ID = {campaign_id}"
            
            query = f"""
                WITH imps AS (
                    SELECT 
                        PUBLISHER as PUBLISHER_CODE,
                        SUM(IMPRESSIONS) as IMPRESSIONS
                    FROM QUORUMDB.SEGMENT_DATA.CAMPAIGN_PERFORMANCE_REPORT_WEEKLY_STATS
                    WHERE ADVERTISER_ID = %(advertiser_id)s
                      {date_filter_imps}
                      {campaign_filter_imps}
                    GROUP BY PUBLISHER
                ),
                visits AS (
                    SELECT 
                        CASE 
                            WHEN x.PT IN ('6', '8', '9', '11', '16', '20', '23', '28') THEN x.SITE
                            ELSE x.PUBLISHER_CODE
                        END as PUBLISHER_CODE,
                        COUNT(DISTINCT CONCAT(v.DEVICE_ID, v.DRIVE_BY_DATE, v.POI_MD5)) as STORE_VISITS
                    FROM QUORUMDB.SEGMENT_DATA.CAMPAIGN_PERFORMANCE_STORE_VISITS_RAW v
                    JOIN QUORUMDB.SEGMENT_DATA.XANDR_IMPRESSION_LOG x ON v.IMP_ID = x.ID
                    WHERE v.ADVERTISER_ID = %(advertiser_id)s
                      {date_filter_visits}
                      {campaign_filter_visits}
                    GROUP BY CASE 
                        WHEN x.PT IN ('6', '8', '9', '11', '16', '20', '23', '28') THEN x.SITE
                        ELSE x.PUBLISHER_CODE
                    END
                )
                SELECT 
                    COALESCE(i.PUBLISHER_CODE, v.PUBLISHER_CODE) as PUBLISHER_CODE,
                    COALESCE(i.IMPRESSIONS, 0) as IMPRESSIONS,
                    COALESCE(v.STORE_VISITS, 0) as S_VISITS
                FROM imps i
                FULL OUTER JOIN visits v ON i.PUBLISHER_CODE = v.PUBLISHER_CODE
                WHERE COALESCE(i.IMPRESSIONS, 0) >= 100 OR COALESCE(v.STORE_VISITS, 0) >= 10
                ORDER BY COALESCE(i.IMPRESSIONS, 0) DESC
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
    
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    
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
                SELECT ZIP_CODE, IMPRESSIONS, S_VISITS
                FROM ranked
                WHERE imp_rank <= 50 OR low_rate_rank <= 50
                ORDER BY IMPRESSIONS DESC
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
                SELECT ZIP_CODE, IMPRESSIONS, S_VISITS
                FROM ranked
                WHERE imp_rank <= 50 OR low_rate_rank <= 50
                ORDER BY IMPRESSIONS DESC
            """
            
        else:
            # Class B query - join visits to XANDR for ZIP context
            date_filter_imps = ""
            date_filter_visits = ""
            if start_date and end_date:
                date_filter_imps = f"AND CAST(LOG_DATE AS DATE) BETWEEN '{start_date}' AND '{end_date}'"
                date_filter_visits = f"AND CAST(v.DRIVE_BY_DATE AS DATE) BETWEEN '{start_date}' AND '{end_date}'"
            else:
                date_filter_imps = "AND LOG_DATE >= DATEADD(day, -28, CURRENT_DATE())"
                date_filter_visits = "AND v.DRIVE_BY_DATE >= DATEADD(day, -28, CURRENT_DATE())"
            
            campaign_filter_imps = ""
            campaign_filter_visits = ""
            if campaign_id:
                campaign_filter_imps = f"AND IO_ID = '{campaign_id}'"
                campaign_filter_visits = f"AND x.IO_ID = {campaign_id}"
            
            query = f"""
                WITH imps AS (
                    SELECT 
                        ZIP as ZIP_CODE,
                        SUM(IMPRESSIONS) as IMPRESSIONS
                    FROM QUORUMDB.SEGMENT_DATA.CAMPAIGN_PERFORMANCE_REPORT_WEEKLY_STATS
                    WHERE ADVERTISER_ID = %(advertiser_id)s
                      AND ZIP IS NOT NULL AND ZIP != '' AND ZIP != '0'
                      {date_filter_imps}
                      {campaign_filter_imps}
                    GROUP BY ZIP
                ),
                visits AS (
                    SELECT 
                        x.POSTAL_CODE as ZIP_CODE,
                        COUNT(DISTINCT CONCAT(v.DEVICE_ID, v.DRIVE_BY_DATE, v.POI_MD5)) as STORE_VISITS
                    FROM QUORUMDB.SEGMENT_DATA.CAMPAIGN_PERFORMANCE_STORE_VISITS_RAW v
                    JOIN QUORUMDB.SEGMENT_DATA.XANDR_IMPRESSION_LOG x ON v.IMP_ID = x.ID
                    WHERE v.ADVERTISER_ID = %(advertiser_id)s
                      AND x.POSTAL_CODE IS NOT NULL AND x.POSTAL_CODE != '' AND x.POSTAL_CODE != '0'
                      {date_filter_visits}
                      {campaign_filter_visits}
                    GROUP BY x.POSTAL_CODE
                ),
                combined AS (
                    SELECT 
                        COALESCE(i.ZIP_CODE, v.ZIP_CODE) as ZIP_CODE,
                        COALESCE(i.IMPRESSIONS, 0) as IMPRESSIONS,
                        COALESCE(v.STORE_VISITS, 0) as S_VISITS
                    FROM imps i
                    FULL OUTER JOIN visits v ON i.ZIP_CODE = v.ZIP_CODE
                    WHERE COALESCE(i.IMPRESSIONS, 0) >= 100 OR COALESCE(v.STORE_VISITS, 0) >= 5
                ),
                ranked AS (
                    SELECT *,
                        ROW_NUMBER() OVER (ORDER BY IMPRESSIONS DESC) as imp_rank,
                        ROW_NUMBER() OVER (ORDER BY CASE WHEN IMPRESSIONS > 0 THEN S_VISITS * 1.0 / IMPRESSIONS ELSE 0 END ASC) as low_rate_rank
                    FROM combined
                )
                SELECT ZIP_CODE, IMPRESSIONS, S_VISITS
                FROM ranked
                WHERE imp_rank <= 50 OR low_rate_rank <= 50
                ORDER BY IMPRESSIONS DESC
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
    
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    
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
    
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    
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
    
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    
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


# ============================================================================
# LIFT ANALYSIS - POI Visit Lift Calculation
# ============================================================================

import math

# Agency configuration for lift analysis
LIFT_AGENCY_CONFIGS = {
    # PT 22 - MNTN (Direct MAID)
    2514: {'name': 'MNTN', 'pt': '22', 'method': 'direct', 'table': 'QUORUM_ADV_STORE_VISITS', 'device_field': 'IMP_MAID'},
    # PT 13 - Adelphic agencies (Direct MAID)
    1950: {'name': 'Byrider', 'pt': '13', 'method': 'direct', 'table': 'QUORUM_ADV_STORE_VISITS', 'device_field': 'IMP_MAID'},
    1955: {'name': 'Marine/Boat', 'pt': '13', 'method': 'direct', 'table': 'QUORUM_ADV_STORE_VISITS', 'device_field': 'IMP_MAID'},
    1956: {'name': 'Dealer Spike', 'pt': '13', 'method': 'direct', 'table': 'QUORUM_ADV_STORE_VISITS', 'device_field': 'IMP_MAID'},
    2086: {'name': 'Level5', 'pt': '13', 'method': 'direct', 'table': 'QUORUM_ADV_STORE_VISITS', 'device_field': 'IMP_MAID'},
    2298: {'name': 'InteractRV', 'pt': '13', 'method': 'direct', 'table': 'QUORUM_ADV_STORE_VISITS', 'device_field': 'IMP_MAID'},
    # Variable PT - IP-to-MAID agencies
    1813: {'name': 'Causal iQ', 'pt': 'variable', 'method': 'ip_to_maid', 'table': 'XANDR_IMPRESSION_LOG', 'device_field': 'CLIENT_IP'},
    1972: {'name': 'Hearst', 'pt': 'variable', 'method': 'ip_to_maid', 'table': 'XANDR_IMPRESSION_LOG', 'device_field': 'CLIENT_IP'},
    2234: {'name': 'Magnite', 'pt': 'variable', 'method': 'ip_to_maid', 'table': 'XANDR_IMPRESSION_LOG', 'device_field': 'CLIENT_IP'},
    # PT 28 - Paramount CTV (IP-to-MAID)
    1480: {'name': 'Paramount', 'pt': '28', 'method': 'ip_to_maid', 'table': 'PARAMOUNT_IMP_STORE_VISITS', 'device_field': 'IP', 'agency_filter': True},
}

def calculate_lift_stats(n_e, x_e, n_c, x_c):
    """Calculate z-score, p-value, and confidence intervals for two-proportion test."""
    if n_e == 0 or n_c == 0:
        return {'z': 0.0, 'p': 1.0, 'ci_lower': 0.0, 'ci_upper': 0.0}
    
    p_e = x_e / n_e
    p_c = x_c / n_c
    p_pooled = (x_e + x_c) / (n_e + n_c)
    
    if p_pooled == 0 or p_pooled == 1:
        return {'z': 0.0, 'p': 1.0, 'ci_lower': 0.0, 'ci_upper': 0.0}
    
    se = math.sqrt(p_pooled * (1 - p_pooled) * (1/n_e + 1/n_c))
    if se == 0:
        return {'z': 0.0, 'p': 1.0, 'ci_lower': 0.0, 'ci_upper': 0.0}
    
    z = (p_e - p_c) / se
    p_value = 2 * (1 - 0.5 * (1 + math.erf(abs(z) / math.sqrt(2))))
    
    # Calculate 95% CI for relative lift
    if p_c > 0:
        relative_lift = ((p_e / p_c) - 1) * 100
        se_relative = math.sqrt((p_e * (1-p_e) / n_e + p_c * (1-p_c) / n_c)) / p_c * 100
        ci_lower = relative_lift - 1.96 * se_relative
        ci_upper = relative_lift + 1.96 * se_relative
    else:
        relative_lift = 0
        ci_lower = 0
        ci_upper = 0
    
    return {
        'z': round(z, 2),
        'p': round(p_value, 4),
        'ci_lower': round(ci_lower, 2),
        'ci_upper': round(ci_upper, 2)
    }

@app.route('/api/v5/lift-analysis', methods=['GET'])
def calculate_lift():
    """
    Calculate store visit lift for an advertiser.
    Returns data in format expected by frontend.
    """
    agency_id = request.args.get('agency_id', type=int)
    advertiser_id = request.args.get('advertiser_id', type=int)
    
    if not agency_id or not advertiser_id:
        return jsonify({'success': False, 'error': 'agency_id and advertiser_id required'}), 400
    
    config = LIFT_AGENCY_CONFIGS.get(agency_id)
    if not config:
        return jsonify({'success': False, 'error': f'Agency {agency_id} not configured for lift analysis'}), 400
    
    try:
        conn = get_snowflake_connection()
        cursor = conn.cursor()
        
        # Build exposed devices query based on method
        if config['method'] == 'direct':
            exposed_query = f"""
                SELECT DISTINCT {config['device_field']} as DEVICE_ID
                FROM QUORUMDB.SEGMENT_DATA.{config['table']}
                WHERE QUORUM_ADVERTISER_ID = '{advertiser_id}'
                  AND {config['device_field']} IS NOT NULL
                  AND LENGTH({config['device_field']}) = 36
            """
        else:  # ip_to_maid
            exposed_query = f"""
                WITH impression_ips AS (
                    SELECT DISTINCT {config['device_field']} as IP
                    FROM QUORUMDB.SEGMENT_DATA.{config['table']}
                    WHERE QUORUM_ADVERTISER_ID = '{advertiser_id}'
                      AND {config['device_field']} IS NOT NULL
                      AND {config['device_field']} != ''
                      AND {config['device_field']} NOT LIKE '2%'
                )
                SELECT DISTINCT ipm.MAID as DEVICE_ID
                FROM impression_ips ii
                INNER JOIN QUORUMDB.SEGMENT_DATA.IP_MAID_MAPPING ipm ON ii.IP = ipm.IP
                WHERE ipm.MAID IS NOT NULL AND LENGTH(ipm.MAID) = 36
            """
        
        panel_table = f"QUORUMDB.DNA_CORE.PANEL_MAIDS_UNDER_50_MILES_{advertiser_id}"
        
        # Full lift calculation query
        lift_query = f"""
            WITH 
            panel_visits AS (
                SELECT DEVICE_ID, DRIVE_BY_DATE as VISIT_DATE 
                FROM {panel_table}
            ),
            exposed_devices AS (
                {exposed_query}
            ),
            exposed_in_panel AS (
                SELECT DISTINCT p.DEVICE_ID 
                FROM panel_visits p 
                INNER JOIN exposed_devices e ON UPPER(p.DEVICE_ID) = UPPER(e.DEVICE_ID)
            ),
            control_in_panel AS (
                SELECT DISTINCT p.DEVICE_ID 
                FROM panel_visits p 
                LEFT JOIN exposed_devices e ON UPPER(p.DEVICE_ID) = UPPER(e.DEVICE_ID) 
                WHERE e.DEVICE_ID IS NULL
            ),
            visitor_devices AS (
                SELECT DEVICE_ID 
                FROM panel_visits 
                GROUP BY DEVICE_ID 
                HAVING COUNT(DISTINCT VISIT_DATE) >= 2
            )
            SELECT
                (SELECT COUNT(*) FROM exposed_in_panel) as n_e,
                (SELECT COUNT(*) FROM exposed_in_panel e INNER JOIN visitor_devices v ON UPPER(e.DEVICE_ID) = UPPER(v.DEVICE_ID)) as x_e,
                (SELECT COUNT(*) FROM control_in_panel) as n_c,
                (SELECT COUNT(*) FROM control_in_panel c INNER JOIN visitor_devices v ON UPPER(c.DEVICE_ID) = UPPER(v.DEVICE_ID)) as x_c
        """
        
        cursor.execute(lift_query)
        row = cursor.fetchone()
        
        if not row:
            cursor.close()
            conn.close()
            return jsonify({'success': False, 'error': 'No data returned'}), 500
        
        n_e, x_e, n_c, x_c = row
        n_e = n_e or 0
        x_e = x_e or 0
        n_c = n_c or 0
        x_c = x_c or 0
        
        # Calculate rates and lift
        exposed_rate = (x_e / n_e * 100) if n_e > 0 else 0
        control_rate = (x_c / n_c * 100) if n_c > 0 else 0
        relative_lift = ((exposed_rate / control_rate) - 1) * 100 if control_rate > 0 else 0
        absolute_lift = exposed_rate - control_rate
        
        stats = calculate_lift_stats(n_e, x_e, n_c, x_c)
        
        # Determine significance
        if n_e < 30:
            is_significant = False
            significance_level = 'Insufficient sample size'
        elif abs(stats['z']) >= 1.96:
            is_significant = True
            significance_level = 'p < 0.05 (95% confidence)'
        elif abs(stats['z']) >= 1.65:
            is_significant = False
            significance_level = 'p < 0.10 (marginal)'
        else:
            is_significant = False
            significance_level = 'Not statistically significant'
        
        # Build interpretation
        if is_significant and relative_lift > 0:
            interpretation = f"Ad-exposed users show {relative_lift:.1f}% higher store visit rate vs unexposed panel. This lift is statistically significant at 95% confidence."
        elif relative_lift > 0:
            interpretation = f"Ad-exposed users show {relative_lift:.1f}% higher store visit rate, but the result is not statistically significant. Consider running longer or with larger samples."
        elif relative_lift < 0:
            interpretation = f"Control group shows higher visit rate than exposed group. This may indicate targeting or measurement issues."
        else:
            interpretation = "No measurable lift detected between exposed and control groups."
        
        cursor.close()
        conn.close()
        
        # Return in format frontend expects
        return jsonify({
            'success': True,
            'data': {
                'filter_level': 'advertiser',
                'methodology': f'{config["method"].replace("_", " ").title()} device matching via {config["table"]}',
                'exposed_group': {
                    'n': n_e,
                    'visitors': x_e,
                    'rate': round(exposed_rate, 2)
                },
                'control_group': {
                    'n': n_c,
                    'visitors': x_c,
                    'rate': round(control_rate, 2)
                },
                'lift': {
                    'relative': round(relative_lift, 2),
                    'absolute': round(absolute_lift, 2),
                    'ci_95_lower': stats['ci_lower'],
                    'ci_95_upper': stats['ci_upper']
                },
                'significance': {
                    'is_significant': is_significant,
                    'significance_level': significance_level,
                    'z_score': stats['z'],
                    'p_value': stats['p']
                },
                'interpretation': {
                    'summary': interpretation
                }
            }
        })
        
    except Exception as e:
        error_msg = str(e)
        if 'does not exist' in error_msg.lower() or 'invalid identifier' in error_msg.lower():
            return jsonify({'success': False, 'error': f'Panel table not found for advertiser {advertiser_id}'})
        return jsonify({'success': False, 'error': error_msg}), 500

@app.route('/api/v5/lift/panel/<int:advertiser_id>/status', methods=['GET'])
def check_panel_status(advertiser_id):
    """Check if panel exists for an advertiser."""
    try:
        conn = get_snowflake_connection()
        cursor = conn.cursor()
        
        query = f"""
            SELECT COUNT(DISTINCT DEVICE_ID) as panel_size,
                   MIN(DRIVE_BY_DATE) as min_date,
                   MAX(DRIVE_BY_DATE) as max_date
            FROM QUORUMDB.DNA_CORE.PANEL_MAIDS_UNDER_50_MILES_{advertiser_id}
        """
        
        cursor.execute(query)
        row = cursor.fetchone()
        
        cursor.close()
        conn.close()
        
        if row and row[0] > 0:
            return jsonify({
                'success': True,
                'exists': True,
                'data': {
                    'advertiser_id': advertiser_id,
                    'panel_size': row[0],
                    'min_date': str(row[1]),
                    'max_date': str(row[2])
                }
            })
        else:
            return jsonify({'success': True, 'exists': False})
            
    except Exception as e:
        return jsonify({'success': True, 'exists': False, 'error': str(e)})

@app.route('/api/v5/lift/panel/<int:advertiser_id>', methods=['POST'])
def create_panel(advertiser_id):
    """Create panel table for an advertiser."""
    data = request.get_json() or {}
    agency_id = data.get('agency_id')
    lookback_days = data.get('lookback_days', 90)
    
    agency_filter = ""
    if agency_id:
        agency_filter = f"AND sm.AGENCY_ID = {agency_id}"
    
    try:
        conn = get_snowflake_connection()
        cursor = conn.cursor()
        
        query = f"""
            CREATE OR REPLACE TABLE QUORUMDB.DNA_CORE.PANEL_MAIDS_UNDER_50_MILES_{advertiser_id} AS
            WITH advertiser_segments AS (
                SELECT DISTINCT SEGMENT_MD5 
                FROM QUORUMDB.SEGMENT_DATA.SEGMENT_MD5_MAPPING sm
                WHERE sm.ADVERTISER_ID = {advertiser_id}
                {agency_filter}
            )
            SELECT DISTINCT sdc.DEVICE_ID, sdc.DRIVE_BY_DATE
            FROM QUORUMDB.SEGMENT_DATA.SEGMENT_DEVICES_CAPTURED sdc
            INNER JOIN advertiser_segments s ON sdc.SEGMENT_MD5 = s.SEGMENT_MD5
            WHERE sdc.DRIVE_BY_DATE >= DATEADD(day, -{lookback_days}, CURRENT_DATE())
              AND sdc.DEVICE_ID IS NOT NULL
              AND LENGTH(sdc.DEVICE_ID) = 36
        """
        
        cursor.execute(query)
        
        # Get panel stats
        stats_query = f"""
            SELECT COUNT(DISTINCT DEVICE_ID), MIN(DRIVE_BY_DATE), MAX(DRIVE_BY_DATE)
            FROM QUORUMDB.DNA_CORE.PANEL_MAIDS_UNDER_50_MILES_{advertiser_id}
        """
        cursor.execute(stats_query)
        row = cursor.fetchone()
        
        cursor.close()
        conn.close()
        
        return jsonify({
            'success': True,
            'data': {
                'advertiser_id': advertiser_id,
                'panel_size': row[0] if row else 0,
                'min_date': str(row[1]) if row else None,
                'max_date': str(row[2]) if row else None
            }
        })
        
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

# ============================================================================
# RUN
# ============================================================================

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8080))
    app.run(host='0.0.0.0', port=port, debug=False)
