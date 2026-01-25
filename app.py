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
        schema='SEGMENT_DATA',
        role=os.environ.get('SNOWFLAKE_ROLE', 'OPTIMIZER_READONLY_ROLE')
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
                c.SITE_VISITORS as SITE_VISITS,
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
            # Extract CLIENT_IDENTIFIER from ADVERTISER_NAME (e.g., "924962_selfservice_Lean Rx" -> 924962)
            query = """
                WITH advertiser_mapping AS (
                    SELECT DISTINCT
                        QUORUM_ADVERTISER_ID,
                        ADVERTISER_NAME,
                        TRY_CAST(SUBSTRING(ADVERTISER_NAME, 1, POSITION('_' IN ADVERTISER_NAME)-1) AS NUMBER) as CLIENT_ID
                    FROM QUORUMDB.SEGMENT_DATA.PARAMOUNT_MAPPED_IMPRESSIONS
                    WHERE IMP_DATE >= DATEADD(day, -90, CURRENT_DATE())
                      AND ADVERTISER_NAME LIKE '%%_%%'
                ),
                impressions AS (
                    SELECT 
                        QUORUM_ADVERTISER_ID,
                        COUNT(*) as IMPRESSIONS,
                        COUNT(DISTINCT IO_ID) as CAMPAIGN_COUNT
                    FROM QUORUMDB.SEGMENT_DATA.PARAMOUNT_MAPPED_IMPRESSIONS
                    WHERE IMP_DATE >= DATEADD(day, -90, CURRENT_DATE())
                    GROUP BY QUORUM_ADVERTISER_ID
                ),
                conversions AS (
                    SELECT 
                        QUORUM_ADVERTISER_ID,
                        COUNT(DISTINCT CASE WHEN IS_SITE_VISIT = 'TRUE' THEN IMP_MAID END) as SITE_VISITORS,
                        COUNT(DISTINCT CASE WHEN IS_LEAD = 'TRUE' THEN IMP_MAID END) as LEAD_VISITORS,
                        COUNT(DISTINCT CASE WHEN IS_PURCHASE = 'TRUE' THEN IMP_MAID END) as PURCHASERS
                    FROM QUORUMDB.SEGMENT_DATA.PARAMOUNT_IMPRESSIONS_REPORT_90_DAYS
                    WHERE IMP_DATE >= DATEADD(day, -90, CURRENT_DATE())
                    GROUP BY QUORUM_ADVERTISER_ID
                )
                SELECT 
                    am.CLIENT_ID as ADVERTISER_ID,
                    am.ADVERTISER_NAME,
                    COALESCE(i.IMPRESSIONS, 0) as IMPRESSIONS,
                    COALESCE(c.SITE_VISITORS, 0) as SITE_VISITS,
                    COALESCE(i.CAMPAIGN_COUNT, 0) as CAMPAIGN_COUNT,
                    'W' as AGENCY_CLASS
                FROM advertiser_mapping am
                LEFT JOIN impressions i ON am.QUORUM_ADVERTISER_ID = i.QUORUM_ADVERTISER_ID
                LEFT JOIN conversions c ON am.QUORUM_ADVERTISER_ID = c.QUORUM_ADVERTISER_ID
                WHERE am.CLIENT_ID IS NOT NULL
                  AND COALESCE(i.IMPRESSIONS, 0) >= 10000
                ORDER BY COALESCE(i.IMPRESSIONS, 0) DESC
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
            # First, map CLIENT_IDENTIFIER to QUORUM_ADVERTISER_ID
            lookup_query = f"""
                SELECT DISTINCT QUORUM_ADVERTISER_ID
                FROM QUORUMDB.SEGMENT_DATA.PARAMOUNT_MAPPED_IMPRESSIONS
                WHERE ADVERTISER_NAME LIKE '{advertiser_id}_%'
                LIMIT 1
            """
            cursor.execute(lookup_query)
            result = cursor.fetchone()
            quorum_adv_id = result[0] if result else str(advertiser_id)
            
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
                    WHERE QUORUM_ADVERTISER_ID = '{quorum_adv_id}'
                      {date_filter}
                ),
                conversions AS (
                    SELECT 
                        COUNT(DISTINCT CASE WHEN IS_SITE_VISIT = 'TRUE' THEN IMP_MAID END) as SITE_VISITORS,
                        COUNT(DISTINCT CASE WHEN IS_LEAD = 'TRUE' THEN IMP_MAID END) as LEAD_VISITORS,
                        COUNT(DISTINCT CASE WHEN IS_PURCHASE = 'TRUE' THEN IMP_MAID END) as PURCHASERS,
                        COUNT(DISTINCT CASE WHEN IS_SITE_VISIT = 'TRUE' THEN ZIP_CODE END) as ZIP_COUNT
                    FROM QUORUMDB.SEGMENT_DATA.PARAMOUNT_IMPRESSIONS_REPORT_90_DAYS
                    WHERE QUORUM_ADVERTISER_ID = {quorum_adv_id}
                      {date_filter}
                )
                SELECT 
                    i.IMPRESSIONS,
                    COALESCE(c.SITE_VISITORS, 0) as SITE_VISITS,
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
        
        # Calculate visit rate (handle both SITE_VISITS for web and STORE_VISITS for physical locations)
        imps = result.get('IMPRESSIONS', 0) or 1
        visits = result.get('SITE_VISITS', 0) or result.get('STORE_VISITS', 0) or 0
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
            # First, map CLIENT_IDENTIFIER to QUORUM_ADVERTISER_ID
            lookup_query = f"""
                SELECT DISTINCT QUORUM_ADVERTISER_ID
                FROM QUORUMDB.SEGMENT_DATA.PARAMOUNT_MAPPED_IMPRESSIONS
                WHERE ADVERTISER_NAME LIKE '{advertiser_id}_%'
                LIMIT 1
            """
            cursor.execute(lookup_query)
            result = cursor.fetchone()
            quorum_adv_id = result[0] if result else str(advertiser_id)
            
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
                    WHERE QUORUM_ADVERTISER_ID = '{quorum_adv_id}'
                      {date_filter}
                    GROUP BY IO_ID
                ),
                conversions AS (
                    SELECT 
                        IO_ID,
                        COUNT(DISTINCT CASE WHEN IS_SITE_VISIT = 'TRUE' THEN IMP_MAID END) as SITE_VISITORS
                    FROM QUORUMDB.SEGMENT_DATA.PARAMOUNT_IMPRESSIONS_REPORT_90_DAYS
                    WHERE QUORUM_ADVERTISER_ID = {quorum_adv_id}
                      {date_filter}
                    GROUP BY IO_ID
                )
                SELECT 
                    i.IO_ID as CAMPAIGN_ID,
                    i.IO_NAME as CAMPAIGN_NAME,
                    i.IMPRESSIONS,
                    COALESCE(c.SITE_VISITORS, 0) as SITE_VISITS
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
            # First, map CLIENT_IDENTIFIER to QUORUM_ADVERTISER_ID
            lookup_query = f"""
                SELECT DISTINCT QUORUM_ADVERTISER_ID
                FROM QUORUMDB.SEGMENT_DATA.PARAMOUNT_MAPPED_IMPRESSIONS
                WHERE ADVERTISER_NAME LIKE '{advertiser_id}_%'
                LIMIT 1
            """
            cursor.execute(lookup_query)
            result = cursor.fetchone()
            quorum_adv_id = result[0] if result else str(advertiser_id)
            
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
                    WHERE QUORUM_ADVERTISER_ID = '{quorum_adv_id}'
                      {date_filter}
                      {campaign_filter_imps}
                    GROUP BY SITE
                ),
                conversions AS (
                    SELECT 
                        SITE as PUBLISHER_CODE,
                        COUNT(DISTINCT CASE WHEN IS_SITE_VISIT = 'TRUE' THEN IMP_MAID END) as SITE_VISITORS
                    FROM QUORUMDB.SEGMENT_DATA.PARAMOUNT_IMPRESSIONS_REPORT_90_DAYS
                    WHERE QUORUM_ADVERTISER_ID = {quorum_adv_id}
                      {date_filter}
                      {campaign_filter_conv}
                    GROUP BY SITE
                )
                SELECT 
                    COALESCE(i.PUBLISHER_CODE, c.PUBLISHER_CODE) as PUBLISHER_CODE,
                    COALESCE(i.IMPRESSIONS, 0) as IMPRESSIONS,
                    COALESCE(c.SITE_VISITORS, 0) as SITE_VISITS
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
            # First, map CLIENT_IDENTIFIER to QUORUM_ADVERTISER_ID
            lookup_query = f"""
                SELECT DISTINCT QUORUM_ADVERTISER_ID
                FROM QUORUMDB.SEGMENT_DATA.PARAMOUNT_MAPPED_IMPRESSIONS
                WHERE ADVERTISER_NAME LIKE '{advertiser_id}_%'
                LIMIT 1
            """
            cursor.execute(lookup_query)
            result = cursor.fetchone()
            quorum_adv_id = result[0] if result else str(advertiser_id)
            
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
                    WHERE QUORUM_ADVERTISER_ID = {quorum_adv_id}
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
                        SITE_VISITORS as SITE_VISITS,
                        ROW_NUMBER() OVER (ORDER BY CONVERSION_IMPS DESC) as imp_rank,
                        ROW_NUMBER() OVER (ORDER BY CASE WHEN CONVERSION_IMPS > 0 THEN SITE_VISITORS * 1.0 / CONVERSION_IMPS ELSE 0 END ASC) as low_rate_rank
                    FROM conversions
                )
                SELECT ZIP_CODE, IMPRESSIONS, SITE_VISITS
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
        
        if agency_class == 'W':
            # Class W doesn't have DMA data - return empty result
            return jsonify({'success': True, 'data': [], 'message': 'DMA data not available for web pixel advertisers'})
        
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
# LIFT ANALYSIS ENDPOINT - Panel-Based
# ============================================================================

def _calculate_aggregate_lift(cursor, conn, panel_table, advertiser_id, agency_id,
                              campaign_id, lineitem_id, exposure_start_date, 
                              exposure_end_plus_1, lookback_days):
    """Calculate aggregate lift (single result)"""
    try:
        query = f"""
WITH
params AS (
  SELECT
    {advertiser_id}::NUMBER AS advertiser_id,
    {agency_id}::NUMBER AS agency_id,
    '{exposure_start_date}'::DATE AS exp_start,
    '{exposure_end_plus_1}'::DATE AS exp_end_plus_1,
    {lookback_days}::NUMBER AS lookback_days
),

-- Filter panel to relevant date range
panel_devices AS (
  SELECT DISTINCT DEVICE_ID AS device_id
  FROM {panel_table}
  WHERE DEVICE_ID IS NOT NULL
    AND DRIVE_BY_DATE >= DATEADD(day, -{lookback_days}, '{exposure_start_date}'::DATE)
    AND DRIVE_BY_DATE <= DATEADD(day, {lookback_days}, '{exposure_end_plus_1}'::DATE)
),

panel_coverage AS (
  SELECT
    MIN(DRIVE_BY_DATE) AS visit_min,
    MAX(DRIVE_BY_DATE) AS visit_max
  FROM {panel_table}
),

-- Filter impressions with INNER JOIN to panel
exp_days AS (
  SELECT DISTINCT
    i.DEVICE_UNIQUE_ID AS device_id,
    CAST(i.SYS_TIMESTAMP AS DATE) AS exposure_date
  FROM QUORUMDB.SEGMENT_DATA.XANDR_IMPRESSION_LOG i
  INNER JOIN panel_devices p ON p.device_id = i.DEVICE_UNIQUE_ID
  CROSS JOIN params k
  WHERE i.SYS_TIMESTAMP >= k.exp_start::TIMESTAMP_NTZ
    AND i.SYS_TIMESTAMP < k.exp_end_plus_1::TIMESTAMP_NTZ
    AND TRY_TO_NUMBER(REGEXP_SUBSTR(TO_VARCHAR(i.QUORUM_ADVERTISER_ID), '[0-9]+')) = k.advertiser_id
    AND i.AGENCY_ID = k.agency_id
    AND i.DEVICE_UNIQUE_ID IS NOT NULL
    {'AND i.IO_ID = ' + campaign_id if campaign_id else ''}
    {'AND i.LI_ID = ' + lineitem_id if lineitem_id else ''}
),

exp_devices AS (
  SELECT DISTINCT device_id
  FROM exp_days
),

-- Control = panel devices that did NOT see ads
control_pool AS (
  SELECT p.device_id
  FROM panel_devices p
  WHERE NOT EXISTS (
    SELECT 1 FROM exp_devices e WHERE e.device_id = p.device_id
  )
),

exp_ranked AS (
  SELECT device_id, ROW_NUMBER() OVER (ORDER BY device_id) AS rn
  FROM exp_devices
),

-- Hash-based matching (deterministic, faster than RANDOM)
ctrl_ranked AS (
  SELECT device_id, ROW_NUMBER() OVER (ORDER BY ABS(HASH(device_id))) AS rn
  FROM control_pool
),

matched AS (
  SELECT
    e.device_id AS exp_device_id,
    c.device_id AS ctrl_device_id
  FROM exp_ranked e
  INNER JOIN ctrl_ranked c ON c.rn = e.rn
),

ctrl_days AS (
  SELECT
    m.ctrl_device_id AS device_id,
    d.exposure_date
  FROM matched m
  INNER JOIN exp_days d ON d.device_id = m.exp_device_id
),

-- Filter visits to date range
visit_days AS (
  SELECT DISTINCT
    DEVICE_ID AS device_id,
    DRIVE_BY_DATE AS visit_date
  FROM {panel_table}
  WHERE DEVICE_ID IS NOT NULL
    AND DRIVE_BY_DATE >= DATEADD(day, -{lookback_days}, '{exposure_start_date}'::DATE)
    AND DRIVE_BY_DATE <= DATEADD(day, {lookback_days}, '{exposure_end_plus_1}'::DATE)
),

-- Exposed group: visits within 28 days AFTER first exposure
exp_attrib_days AS (
  SELECT DISTINCT
    e.device_id,
    e.exposure_date
  FROM exp_days e
  INNER JOIN visit_days v ON v.device_id = e.device_id
  CROSS JOIN params k
  WHERE v.visit_date > e.exposure_date
    AND v.visit_date <= DATEADD(day, k.lookback_days, e.exposure_date)
),

-- Control group: visits within same time windows
ctrl_attrib_days AS (
  SELECT DISTINCT
    c.device_id,
    c.exposure_date
  FROM ctrl_days c
  INNER JOIN visit_days v ON v.device_id = c.device_id
  CROSS JOIN params k
  WHERE v.visit_date > c.exposure_date
    AND v.visit_date <= DATEADD(day, k.lookback_days, c.exposure_date)
),

counts AS (
  SELECT
    (SELECT COUNT(*) FROM exp_devices) AS n_e,
    (SELECT COUNT(DISTINCT device_id) FROM exp_attrib_days) AS x_e,
    (SELECT COUNT(DISTINCT device_id) FROM (SELECT DISTINCT device_id FROM ctrl_days)) AS n_c,
    (SELECT COUNT(DISTINCT device_id) FROM ctrl_attrib_days) AS x_c
),

stats AS (
  SELECT
    n_e, x_e, x_e::FLOAT/n_e AS p_e,
    n_c, x_c, x_c::FLOAT/n_c AS p_c,
    (x_e::FLOAT/n_e) - (x_c::FLOAT/n_c) AS abs_lift,
    ((x_e::FLOAT/n_e) / NULLIF((x_c::FLOAT/n_c),0)) - 1 AS rel_lift,
    SQRT((p_e*(1-p_e))/n_e + (p_c*(1-p_c))/n_c) AS se_diff,
    (
      ((p_e - p_c) /
        SQRT(
          (((x_e + x_c)::FLOAT / (n_e + n_c))
          * (1 - ((x_e + x_c)::FLOAT / (n_e + n_c))))
          * (1/n_e + 1/n_c)
        )
      )
    ) AS z_score
  FROM (
    SELECT
      n_e, x_e, x_e::FLOAT/n_e AS p_e,
      n_c, x_c, x_c::FLOAT/n_c AS p_c
    FROM counts
  )
),

pval AS (
  SELECT
    *,
    (1 / (1 + 0.2316419 * ABS(z_score))) AS t,
    EXP(-0.5 * z_score * z_score) / SQRT(2 * 3.141592653589793) AS phi
  FROM stats
),

pval2 AS (
  SELECT
    *,
    (0.319381530*t
     + (-0.356563782)*t*t
     + (1.781477937)*t*t*t
     + (-1.821255978)*t*t*t*t
     + (1.330274429)*t*t*t*t*t) AS poly
  FROM pval
)

SELECT
  k.advertiser_id,
  k.agency_id,
  k.exp_start,
  DATEADD(day, -1, k.exp_end_plus_1) AS exp_end,
  pc.visit_min,
  pc.visit_max,
  n_e AS device_exposed_n,
  n_c AS device_control_n,
  p_e AS exposed_rate,
  p_c AS control_rate,
  abs_lift AS device_abs_lift,
  abs_lift - 1.96*se_diff AS device_abs_lift_ci_95_lo,
  abs_lift + 1.96*se_diff AS device_abs_lift_ci_95_hi,
  rel_lift AS device_rel_lift,
  z_score AS device_z,
  2 * (phi * poly) AS device_p_value_approx
FROM pval2
JOIN params k ON 1=1
CROSS JOIN panel_coverage pc
        """
        
        cursor.execute(query)
        columns = [desc[0] for desc in cursor.description]
        row = dict(zip(columns, cursor.fetchone()))
        cursor.close()
        conn.close()
        
        if not row:
            return jsonify({
                'success': False,
                'error': 'No lift data returned. Panel may have insufficient data for this time period.'
            }), 404
        
        # Build response
        response_data = {
            'advertiser_id': int(row.get('ADVERTISER_ID', advertiser_id)),
            'agency_id': int(row.get('AGENCY_ID', agency_id)),
            'exposure_start_date': str(row.get('EXP_START')),
            'exposure_end_date': str(row.get('EXP_END')),
            'visit_coverage_start': str(row.get('VISIT_MIN')),
            'visit_coverage_end': str(row.get('VISIT_MAX')),
        }
        
        # Add campaign/lineitem info if filtered
        if campaign_id:
            response_data['campaign_id'] = int(campaign_id)
            response_data['filter_level'] = 'campaign'
        elif lineitem_id:
            response_data['lineitem_id'] = int(lineitem_id)
            response_data['filter_level'] = 'lineitem'
        else:
            response_data['filter_level'] = 'advertiser'
        
        response_data.update({
            'exposed_group': {
                'n': int(row.get('DEVICE_EXPOSED_N', 0)),
                'conversions': int(row.get('DEVICE_EXPOSED_N', 0) * row.get('EXPOSED_RATE', 0)),
                'rate': round(float(row.get('EXPOSED_RATE', 0)) * 100, 4)
            },
            'control_group': {
                'n': int(row.get('DEVICE_CONTROL_N', 0)),
                'conversions': int(row.get('DEVICE_CONTROL_N', 0) * row.get('CONTROL_RATE', 0)),
                'rate': round(float(row.get('CONTROL_RATE', 0)) * 100, 4)
            },
            'lift': {
                'absolute': round(float(row.get('DEVICE_ABS_LIFT', 0)) * 100, 4),
                'relative': round(float(row.get('DEVICE_REL_LIFT', 0)) * 100, 2),
                'ci_95_lower': round(float(row.get('DEVICE_ABS_LIFT_CI_95_LO', 0)) * 100, 4),
                'ci_95_upper': round(float(row.get('DEVICE_ABS_LIFT_CI_95_HI', 0)) * 100, 4)
            },
            'significance': {
                'z_score': round(float(row.get('DEVICE_Z', 0)), 2),
                'p_value': round(float(row.get('DEVICE_P_VALUE_APPROX', 1)), 6),
                'is_significant': float(row.get('DEVICE_P_VALUE_APPROX', 1)) < 0.05,
                'significance_level': 'p < 0.001' if float(row.get('DEVICE_P_VALUE_APPROX', 1)) < 0.001 else f"p = {float(row.get('DEVICE_P_VALUE_APPROX', 1)):.4f}"
            },
            'methodology': 'Panel-constrained matched controls with 28-day rolling attribution',
            'interpretation': {
                'summary': f"Campaign drove {round(float(row.get('DEVICE_REL_LIFT', 0)) * 100, 1)}% relative lift in store visits",
                'exposed_rate_display': f"{round(float(row.get('EXPOSED_RATE', 0)) * 100, 2)}% of exposed users visited",
                'control_rate_display': f"{round(float(row.get('CONTROL_RATE', 0)) * 100, 2)}% of control users visited",
                'lift_display': f"+{round(float(row.get('DEVICE_ABS_LIFT', 0)) * 100, 2)}% absolute lift"
            }
        })
        
        return jsonify({
            'success': True,
            'data': response_data
        })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

def _calculate_segmented_lift(cursor, conn, panel_table, advertiser_id, agency_id,
                              campaign_id, lineitem_id, segment_by,
                              exposure_start_date, exposure_end_plus_1, lookback_days):
    """Calculate lift broken down by segment (returns array of results)"""
    
    # Map segment_by to SQL column names
    segment_mapping = {
        'zip': 'i.POSTAL_CODE',
        'dma': 'i.DMA', 
        'publisher': f"""
            CASE 
                WHEN i.PT = '6' THEN i.SITE
                WHEN i.PT = '8' THEN i.SITE
                WHEN i.PT = '9' THEN i.SITE
                WHEN i.PT = '11' THEN i.SITE
                WHEN i.PT = '13' THEN i.PUBLISHER_CODE
                WHEN i.PT = '16' THEN i.SITE
                WHEN i.PT = '20' THEN i.SITE
                WHEN i.PT = '22' THEN i.PUBLISHER_CODE
                WHEN i.PT = '23' THEN i.SITE
                WHEN i.PT = '25' THEN i.PUBLISHER_CODE
                WHEN i.PT = '28' THEN i.SITE
                WHEN i.PT = '33' THEN i.PUBLISHER_CODE
                ELSE i.SITE
            END
        """
    }
    
    segment_column = segment_mapping[segment_by]
    segment_alias = segment_by.upper()
    
    try:
        query = f"""
WITH
params AS (
  SELECT
    {advertiser_id}::NUMBER AS advertiser_id,
    {agency_id}::NUMBER AS agency_id,
    '{exposure_start_date}'::DATE AS exp_start,
    '{exposure_end_plus_1}'::DATE AS exp_end_plus_1,
    {lookback_days}::NUMBER AS lookback_days
),

-- Filter panel to relevant date range
panel_devices AS (
  SELECT DISTINCT DEVICE_ID AS device_id
  FROM {panel_table}
  WHERE DEVICE_ID IS NOT NULL
    AND DRIVE_BY_DATE >= DATEADD(day, -{lookback_days}, '{exposure_start_date}'::DATE)
    AND DRIVE_BY_DATE <= DATEADD(day, {lookback_days}, '{exposure_end_plus_1}'::DATE)
),

-- Filter impressions WITH segment field
exp_days AS (
  SELECT DISTINCT
    i.DEVICE_UNIQUE_ID AS device_id,
    CAST(i.SYS_TIMESTAMP AS DATE) AS exposure_date,
    {segment_column} AS segment
  FROM QUORUMDB.SEGMENT_DATA.XANDR_IMPRESSION_LOG i
  INNER JOIN panel_devices p ON p.device_id = i.DEVICE_UNIQUE_ID
  CROSS JOIN params k
  WHERE i.SYS_TIMESTAMP >= k.exp_start::TIMESTAMP_NTZ
    AND i.SYS_TIMESTAMP < k.exp_end_plus_1::TIMESTAMP_NTZ
    AND TRY_TO_NUMBER(REGEXP_SUBSTR(TO_VARCHAR(i.QUORUM_ADVERTISER_ID), '[0-9]+')) = k.advertiser_id
    AND i.AGENCY_ID = k.agency_id
    AND i.DEVICE_UNIQUE_ID IS NOT NULL
    {'AND i.IO_ID = ' + campaign_id if campaign_id else ''}
    {'AND i.LI_ID = ' + lineitem_id if lineitem_id else ''}
    AND {segment_column} IS NOT NULL
    AND {segment_column} != ''
    AND {segment_column} != '0'
),

-- Group exposed devices by segment
exp_devices_by_segment AS (
  SELECT segment, device_id
  FROM exp_days
),

-- Get all segments
segments AS (
  SELECT DISTINCT segment
  FROM exp_days
),

-- For each segment, get exposed devices
exp_by_seg AS (
  SELECT 
    s.segment,
    e.device_id,
    ROW_NUMBER() OVER (PARTITION BY s.segment ORDER BY e.device_id) AS rn
  FROM segments s
  LEFT JOIN exp_devices_by_segment e ON s.segment = e.segment
),

-- Control pool by segment (panel devices not in exposed for that segment)
ctrl_pool_by_seg AS (
  SELECT 
    s.segment,
    p.device_id,
    ROW_NUMBER() OVER (PARTITION BY s.segment ORDER BY ABS(HASH(p.device_id))) AS rn
  FROM segments s
  CROSS JOIN panel_devices p
  WHERE NOT EXISTS (
    SELECT 1 FROM exp_devices_by_segment e 
    WHERE e.device_id = p.device_id AND e.segment = s.segment
  )
),

-- Match 1:1 within each segment
matched_by_seg AS (
  SELECT
    e.segment,
    e.device_id AS exp_device_id,
    c.device_id AS ctrl_device_id
  FROM exp_by_seg e
  INNER JOIN ctrl_pool_by_seg c ON c.segment = e.segment AND c.rn = e.rn
  WHERE e.device_id IS NOT NULL
),

-- Control exposure dates (matched from exposed)
ctrl_days AS (
  SELECT
    m.segment,
    m.ctrl_device_id AS device_id,
    d.exposure_date
  FROM matched_by_seg m
  INNER JOIN exp_days d ON d.device_id = m.exp_device_id AND d.segment = m.segment
),

-- Filter visits to date range
visit_days AS (
  SELECT DISTINCT
    DEVICE_ID AS device_id,
    DRIVE_BY_DATE AS visit_date
  FROM {panel_table}
  WHERE DEVICE_ID IS NOT NULL
    AND DRIVE_BY_DATE >= DATEADD(day, -{lookback_days}, '{exposure_start_date}'::DATE)
    AND DRIVE_BY_DATE <= DATEADD(day, {lookback_days}, '{exposure_end_plus_1}'::DATE)
),

-- Exposed conversions by segment
exp_attrib_by_seg AS (
  SELECT DISTINCT
    e.segment,
    e.device_id
  FROM exp_days e
  INNER JOIN visit_days v ON v.device_id = e.device_id
  CROSS JOIN params k
  WHERE v.visit_date > e.exposure_date
    AND v.visit_date <= DATEADD(day, k.lookback_days, e.exposure_date)
),

-- Control conversions by segment
ctrl_attrib_by_seg AS (
  SELECT DISTINCT
    c.segment,
    c.device_id
  FROM ctrl_days c
  INNER JOIN visit_days v ON v.device_id = c.device_id
  CROSS JOIN params k
  WHERE v.visit_date > c.exposure_date
    AND v.visit_date <= DATEADD(day, k.lookback_days, c.exposure_date)
),

-- Aggregate counts by segment
counts_by_seg AS (
  SELECT
    s.segment,
    COUNT(DISTINCT e.device_id) AS n_e,
    COUNT(DISTINCT ea.device_id) AS x_e,
    COUNT(DISTINCT c.ctrl_device_id) AS n_c,
    COUNT(DISTINCT ca.device_id) AS x_c
  FROM segments s
  LEFT JOIN exp_by_seg e ON s.segment = e.segment
  LEFT JOIN exp_attrib_by_seg ea ON s.segment = ea.segment
  LEFT JOIN matched_by_seg c ON s.segment = c.segment
  LEFT JOIN ctrl_attrib_by_seg ca ON s.segment = ca.segment
  GROUP BY s.segment
  HAVING n_e >= 100 AND n_c >= 100  -- Minimum sample size
),

-- Calculate lift statistics by segment
stats_by_seg AS (
  SELECT
    segment,
    n_e, x_e, x_e::FLOAT/NULLIF(n_e, 0) AS p_e,
    n_c, x_c, x_c::FLOAT/NULLIF(n_c, 0) AS p_c,
    (x_e::FLOAT/NULLIF(n_e, 0)) - (x_c::FLOAT/NULLIF(n_c, 0)) AS abs_lift,
    ((x_e::FLOAT/NULLIF(n_e, 0)) / NULLIF((x_c::FLOAT/NULLIF(n_c, 0)),0)) - 1 AS rel_lift,
    SQRT(
      ((x_e::FLOAT/NULLIF(n_e, 0))*(1-(x_e::FLOAT/NULLIF(n_e, 0))))/NULLIF(n_e, 0) + 
      ((x_c::FLOAT/NULLIF(n_c, 0))*(1-(x_c::FLOAT/NULLIF(n_c, 0))))/NULLIF(n_c, 0)
    ) AS se_diff,
    (
      ((x_e::FLOAT/NULLIF(n_e, 0)) - (x_c::FLOAT/NULLIF(n_c, 0))) /
        SQRT(
          (((x_e + x_c)::FLOAT / NULLIF((n_e + n_c), 0))
          * (1 - ((x_e + x_c)::FLOAT / NULLIF((n_e + n_c), 0))))
          * (1/NULLIF(n_e, 0) + 1/NULLIF(n_c, 0))
        )
    ) AS z_score
  FROM counts_by_seg
  WHERE n_e > 0 AND n_c > 0
),

-- Calculate p-value approximation
pval_by_seg AS (
  SELECT
    *,
    (1 / (1 + 0.2316419 * ABS(z_score))) AS t,
    EXP(-0.5 * z_score * z_score) / SQRT(2 * 3.141592653589793) AS phi
  FROM stats_by_seg
),

pval2_by_seg AS (
  SELECT
    *,
    (0.319381530*t
     + (-0.356563782)*t*t
     + (1.781477937)*t*t*t
     + (-1.821255978)*t*t*t*t
     + (1.330274429)*t*t*t*t*t) AS poly
  FROM pval_by_seg
)

SELECT
  segment AS {segment_alias},
  n_e AS device_exposed_n,
  n_c AS device_control_n,
  p_e AS exposed_rate,
  p_c AS control_rate,
  abs_lift AS device_abs_lift,
  abs_lift - 1.96*se_diff AS device_abs_lift_ci_95_lo,
  abs_lift + 1.96*se_diff AS device_abs_lift_ci_95_hi,
  rel_lift AS device_rel_lift,
  z_score AS device_z,
  2 * (phi * poly) AS device_p_value_approx
FROM pval2_by_seg
ORDER BY device_rel_lift DESC
        """
        
        cursor.execute(query)
        columns = [desc[0] for desc in cursor.description]
        rows = [dict(zip(columns, row)) for row in cursor.fetchall()]
        cursor.close()
        conn.close()
        
        if not rows:
            return jsonify({
                'success': False,
                'error': f'No segmented lift data returned. May not have sufficient data per {segment_by} segment.'
            }), 404
        
        # Build array of segment results
        segments_data = []
        for row in rows:
            segment_result = {
                segment_by: str(row.get(segment_alias)),
                'exposed_group': {
                    'n': int(row.get('DEVICE_EXPOSED_N', 0)),
                    'conversions': int(row.get('DEVICE_EXPOSED_N', 0) * row.get('EXPOSED_RATE', 0)),
                    'rate': round(float(row.get('EXPOSED_RATE', 0)) * 100, 4)
                },
                'control_group': {
                    'n': int(row.get('DEVICE_CONTROL_N', 0)),
                    'conversions': int(row.get('DEVICE_CONTROL_N', 0) * row.get('CONTROL_RATE', 0)),
                    'rate': round(float(row.get('CONTROL_RATE', 0)) * 100, 4)
                },
                'lift': {
                    'absolute': round(float(row.get('DEVICE_ABS_LIFT', 0)) * 100, 4),
                    'relative': round(float(row.get('DEVICE_REL_LIFT', 0)) * 100, 2),
                    'ci_95_lower': round(float(row.get('DEVICE_ABS_LIFT_CI_95_LO', 0)) * 100, 4),
                    'ci_95_upper': round(float(row.get('DEVICE_ABS_LIFT_CI_95_HI', 0)) * 100, 4)
                },
                'significance': {
                    'z_score': round(float(row.get('DEVICE_Z', 0)), 2),
                    'p_value': round(float(row.get('DEVICE_P_VALUE_APPROX', 1)), 6),
                    'is_significant': float(row.get('DEVICE_P_VALUE_APPROX', 1)) < 0.05
                }
            }
            segments_data.append(segment_result)
        
        response_data = {
            'advertiser_id': int(advertiser_id),
            'agency_id': int(agency_id),
            'segment_by': segment_by,
            'total_segments': len(segments_data),
            'segments': segments_data,
            'methodology': f'Panel-constrained lift calculated separately for each {segment_by}',
            'note': f'Results sorted by relative lift (descending). Minimum 100 exposed + 100 control per segment.'
        }
        
        # Add campaign/lineitem info if filtered
        if campaign_id:
            response_data['campaign_id'] = int(campaign_id)
        if lineitem_id:
            response_data['lineitem_id'] = int(lineitem_id)
        
        return jsonify({
            'success': True,
            'data': response_data
        })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/api/v5/lift-analysis', methods=['GET'])
def get_lift_analysis_v5():
    """
    Calculate incremental visit lift using panel-constrained matched controls.
    
    Methodology:
    - Panel Universe: PANEL_MAIDS_UNDER_50_MILES_<ADVERTISER_ID>
    - Exposed: Panel devices that saw ads (from XANDR_IMPRESSION_LOG)
    - Control: Panel devices that did NOT see ads (matched 1:1 via hash)
    - Attribution: Rolling 28-day window after exposure
    
    Query Parameters:
    - advertiser_id (required): Target advertiser ID
    - agency_id (required): Agency ID
    - campaign_id (optional): Filter to specific campaign/IO
    - lineitem_id (optional): Filter to specific line item
    - segment_by (optional): Break down by 'zip', 'dma', or 'publisher'
    - exposure_start_date (optional): Campaign start (default: 90 days ago)
    - exposure_end_date (optional): Campaign end (default: today)
    - lookback_days (optional): Attribution window (default: 28)
    
    Granularity Levels:
    - No filters: ADVERTISER-LEVEL lift (all campaigns)
    - With campaign_id: CAMPAIGN-LEVEL lift (single campaign)
    - With lineitem_id: LINEITEM-LEVEL lift (single line item)
    - With segment_by: Array of lifts for each ZIP/DMA/Publisher
    
    Examples:
    - Overall: /api/v5/lift-analysis?advertiser_id=8123&agency_id=1813
    - Campaign: /api/v5/lift-analysis?advertiser_id=8123&agency_id=1813&campaign_id=11026146
    - By ZIP: /api/v5/lift-analysis?advertiser_id=8123&agency_id=1813&segment_by=zip
    - Campaign by Publisher: /api/v5/lift-analysis?advertiser_id=8123&agency_id=1813&campaign_id=11026146&segment_by=publisher
    
    Returns lift metrics with statistical significance testing
    """
    try:
        # Get parameters
        advertiser_id = request.args.get('advertiser_id')
        agency_id = request.args.get('agency_id')
        campaign_id = request.args.get('campaign_id')  # Optional - filter to specific campaign
        lineitem_id = request.args.get('lineitem_id')  # Optional - filter to specific line item
        segment_by = request.args.get('segment_by')  # Optional - break down by zip/dma/publisher
        
        # Validate segment_by parameter
        valid_segments = ['zip', 'dma', 'publisher', None]
        if segment_by and segment_by not in valid_segments:
            return jsonify({
                'success': False,
                'error': f'segment_by must be one of: {", ".join([s for s in valid_segments if s])}'
            }), 400
        
        if not advertiser_id or not agency_id:
            return jsonify({
                'success': False,
                'error': 'advertiser_id and agency_id parameters required'
            }), 400
        
        # Date parameters with defaults
        default_end = datetime.now().date()
        default_start = default_end - timedelta(days=90)
        
        exposure_start_date = request.args.get('exposure_start_date', str(default_start))
        exposure_end_date = request.args.get('exposure_end_date', str(default_end))
        lookback_days = int(request.args.get('lookback_days', 28))
        
        # Calculate exposure_end_plus_1 for SQL < bound
        end_date_obj = datetime.strptime(exposure_end_date, '%Y-%m-%d')
        exposure_end_plus_1 = (end_date_obj + timedelta(days=1)).strftime('%Y-%m-%d')
        
        # Construct panel table name
        panel_table = f"QUORUMDB.SEGMENT_DATA.PANEL_MAIDS_UNDER_50_MILES_{advertiser_id}"
        
        # Check if panel table exists
        conn = get_snowflake_connection()
        cursor = conn.cursor()
        
        check_query = f"""
            SELECT COUNT(*) as cnt
            FROM QUORUMDB.INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = 'SEGMENT_DATA'
              AND TABLE_NAME = 'PANEL_MAIDS_UNDER_50_MILES_{advertiser_id}'
        """
        cursor.execute(check_query)
        panel_exists = cursor.fetchone()[0] > 0
        
        if not panel_exists:
            return jsonify({
                'success': False,
                'error': f'Panel table does not exist for advertiser {advertiser_id}. Panel-based lift requires pre-built panel tables.',
                'recommendation': 'Use agency-based lift methodology instead'
            }), 404
        
        # Route to appropriate query based on segment_by parameter
        if segment_by:
            # Segmented lift query (returns array of results)
            return _calculate_segmented_lift(
                cursor, conn, panel_table, advertiser_id, agency_id, 
                campaign_id, lineitem_id, segment_by,
                exposure_start_date, exposure_end_plus_1, lookback_days
            )
        else:
            # Aggregate lift query (returns single result)
            return _calculate_aggregate_lift(
                cursor, conn, panel_table, advertiser_id, agency_id,
                campaign_id, lineitem_id,
                exposure_start_date, exposure_end_plus_1, lookback_days
            )
        
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

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
# RUN
# ============================================================================

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8080))
    app.run(host='0.0.0.0', port=port, debug=False)
