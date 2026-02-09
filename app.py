"""
Quorum Optimizer API v5 - Hybrid Architecture
==============================================
Uses pre-aggregated tables for fast queries:
- CAMPAIGN_PERFORMANCE_REPORT_WEEKLY_STATS (Class B agencies)
- PARAMOUNT_DASHBOARD_SUMMARY_STATS (Paramount advertiser-level)
- PARAMOUNT_MAPPED_IMPRESSIONS + PARAMOUNT_IMPRESSIONS_REPORT_90_DAYS (Paramount campaign-level)
- CAMPAIGN_POSTAL_REPORTING (Geographic - all agencies, full history)

Web visits included where available (Paramount only for now).
Geographic endpoint uses full history with no date filter.
"""

from flask import Flask, jsonify, request
from flask_cors import CORS
import snowflake.connector
import os
from datetime import datetime, timedelta
import re

app = Flask(__name__)
CORS(app)

# =============================================================================
# CONFIGURATION
# =============================================================================

# Agency classification and names
AGENCY_CONFIG = {
    # Paramount - uses PARAMOUNT_* tables
    1480: {'name': 'Paramount', 'class': 'PARAMOUNT'},
    
    # Class B - uses CAMPAIGN_PERFORMANCE_REPORT_WEEKLY_STATS
    1813: {'name': 'Causal iQ', 'class': 'B'},
    2514: {'name': 'MNTN', 'class': 'B'},
    1972: {'name': 'Hearst', 'class': 'B'},
    2234: {'name': 'Magnite', 'class': 'B'},
    2379: {'name': 'The Shipyard', 'class': 'B'},
    1445: {'name': 'Publicis', 'class': 'B'},
    1880: {'name': 'TeamSnap', 'class': 'B'},
    2744: {'name': 'Parallel Path', 'class': 'B'},
    2691: {'name': 'TravelSpike', 'class': 'B'},
    2393: {'name': 'AIOPW', 'class': 'B'},
}

CLASS_B_AGENCIES = [k for k, v in AGENCY_CONFIG.items() if v['class'] == 'B']

def get_agency_name(agency_id):
    """Get agency name from config"""
    config = AGENCY_CONFIG.get(int(agency_id))
    return config['name'] if config else f"Agency {agency_id}"

def get_agency_class(agency_id):
    """Get agency class (PARAMOUNT or B)"""
    config = AGENCY_CONFIG.get(int(agency_id))
    return config['class'] if config else 'B'

def get_snowflake_connection(retries=2):
    """Get Snowflake connection with retry for transient SSL/cert errors."""
    last_err = None
    for attempt in range(retries + 1):
        try:
            return snowflake.connector.connect(
                user=os.environ.get('SNOWFLAKE_USER'),
                password=os.environ.get('SNOWFLAKE_PASSWORD'),
                account=os.environ.get('SNOWFLAKE_ACCOUNT'),
                warehouse=os.environ.get('SNOWFLAKE_WAREHOUSE', 'COMPUTE_WH'),
                database=os.environ.get('SNOWFLAKE_DATABASE', 'QUORUMDB'),
                schema=os.environ.get('SNOWFLAKE_SCHEMA', 'SEGMENT_DATA'),
                role=os.environ.get('SNOWFLAKE_ROLE', 'OPTIMIZER_READONLY_ROLE'),
                insecure_mode=True  # Bypass OCSP check — workaround for S3 stage cert issue
            )
        except Exception as e:
            last_err = e
            if attempt < retries and ('certificate' in str(e).lower() or '254007' in str(e)):
                import time
                time.sleep(1)
                continue
            raise

def get_date_range():
    """Get date range from request params with defaults"""
    end_date = request.args.get('end_date', datetime.now().strftime('%Y-%m-%d'))
    start_date = request.args.get('start_date', 
        (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d'))
    return start_date, end_date

# =============================================================================
# HEALTH CHECK
# =============================================================================

@app.route('/health', methods=['GET'])
def health_check():
    return jsonify({
        'status': 'healthy',
        'version': '5.5-charts-v6',
        'description': 'Traffic sources 50K sample for Railway timeout compat',
        'endpoints': [
            '/api/v5/agencies',
            '/api/v5/advertisers',
            '/api/v5/campaigns',
            '/api/v5/lineitems',
            '/api/v5/publishers',
            '/api/v5/zip-performance',
            '/api/v5/dma-performance',
            '/api/v5/summary',
            '/api/v5/timeseries',
            '/api/v5/lift-analysis',
            '/api/v5/traffic-sources',
            '/api/v5/optimize',
            '/api/v5/agency-timeseries',
            '/api/v5/advertiser-timeseries'
        ]
    })

# =============================================================================
# AGENCY OVERVIEW
# =============================================================================

@app.route('/api/v5/agencies', methods=['GET'])
def get_agencies():
    try:
        start_date, end_date = get_date_range()
        conn = get_snowflake_connection()
        cursor = conn.cursor()
        
        all_results = []
        
        query_class_b = """
            SELECT 
                AGENCY_ID,
                SUM(IMPRESSIONS) as IMPRESSIONS,
                SUM(VISITORS) as STORE_VISITS,
                0 as WEB_VISITS,
                COUNT(DISTINCT ADVERTISER_ID) as ADVERTISER_COUNT
            FROM QUORUMDB.SEGMENT_DATA.CAMPAIGN_PERFORMANCE_REPORT_WEEKLY_STATS
            WHERE LOG_DATE BETWEEN %(start_date)s AND %(end_date)s
            GROUP BY AGENCY_ID
            HAVING SUM(IMPRESSIONS) > 0 OR SUM(VISITORS) > 0
        """
        cursor.execute(query_class_b, {'start_date': start_date, 'end_date': end_date})
        for row in cursor.fetchall():
            agency_id = row[0]
            all_results.append({
                'AGENCY_ID': agency_id,
                'AGENCY_NAME': get_agency_name(agency_id),
                'IMPRESSIONS': row[1] or 0,
                'STORE_VISITS': row[2] or 0,
                'WEB_VISITS': row[3] or 0,
                'ADVERTISER_COUNT': row[4] or 0
            })
        
        query_paramount = """
            WITH deduped AS (
                SELECT DISTINCT DATE, QUORUM_ADVERTISER_ID, ADVERTISER_NAME, 
                       IMPRESSIONS, STORE_VISITS, SITE_VISITS
                FROM QUORUMDB.SEGMENT_DATA.PARAMOUNT_DASHBOARD_SUMMARY_STATS
                WHERE DATE BETWEEN %(start_date)s AND %(end_date)s
            )
            SELECT 
                1480 as AGENCY_ID,
                SUM(IMPRESSIONS) as IMPRESSIONS,
                SUM(STORE_VISITS) as STORE_VISITS,
                SUM(SITE_VISITS) as WEB_VISITS,
                COUNT(DISTINCT QUORUM_ADVERTISER_ID) as ADVERTISER_COUNT
            FROM deduped
        """
        cursor.execute(query_paramount, {'start_date': start_date, 'end_date': end_date})
        row = cursor.fetchone()
        if row and (row[1] or row[2] or row[3]):
            all_results.append({
                'AGENCY_ID': 1480,
                'AGENCY_NAME': 'Paramount',
                'IMPRESSIONS': row[1] or 0,
                'STORE_VISITS': row[2] or 0,
                'WEB_VISITS': row[3] or 0,
                'ADVERTISER_COUNT': row[4] or 0
            })
        
        all_results.sort(key=lambda x: x.get('IMPRESSIONS', 0) or 0, reverse=True)
        
        cursor.close()
        conn.close()
        
        return jsonify({'success': True, 'data': all_results})
        
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

# =============================================================================
# ADVERTISER OVERVIEW
# =============================================================================

@app.route('/api/v5/advertisers', methods=['GET'])
def get_advertisers():
    try:
        agency_id = request.args.get('agency_id')
        if not agency_id:
            return jsonify({'success': False, 'error': 'agency_id required'}), 400
        
        agency_id = int(agency_id)
        start_date, end_date = get_date_range()
        conn = get_snowflake_connection()
        cursor = conn.cursor()
        
        if agency_id == 1480:
            query = """
                WITH deduped AS (
                    SELECT DISTINCT DATE, QUORUM_ADVERTISER_ID, ADVERTISER_NAME, 
                           IMPRESSIONS, STORE_VISITS, SITE_VISITS
                    FROM QUORUMDB.SEGMENT_DATA.PARAMOUNT_DASHBOARD_SUMMARY_STATS
                    WHERE DATE BETWEEN %(start_date)s AND %(end_date)s
                )
                SELECT 
                    QUORUM_ADVERTISER_ID as ADVERTISER_ID,
                    MAX(ADVERTISER_NAME) as ADVERTISER_NAME,
                    SUM(IMPRESSIONS) as IMPRESSIONS,
                    SUM(STORE_VISITS) as STORE_VISITS,
                    SUM(SITE_VISITS) as WEB_VISITS
                FROM deduped
                GROUP BY QUORUM_ADVERTISER_ID
                HAVING SUM(IMPRESSIONS) > 0 OR SUM(STORE_VISITS) > 0 OR SUM(SITE_VISITS) > 0
                ORDER BY 3 DESC
            """
            cursor.execute(query, {'start_date': start_date, 'end_date': end_date})
        else:
            query = """
                SELECT 
                    w.ADVERTISER_ID,
                    COALESCE(MAX(aa.COMP_NAME), 'Advertiser ' || w.ADVERTISER_ID) as ADVERTISER_NAME,
                    SUM(w.IMPRESSIONS) as IMPRESSIONS,
                    SUM(w.VISITORS) as STORE_VISITS,
                    0 as WEB_VISITS
                FROM QUORUMDB.SEGMENT_DATA.CAMPAIGN_PERFORMANCE_REPORT_WEEKLY_STATS w
                LEFT JOIN QUORUMDB.SEGMENT_DATA.AGENCY_ADVERTISER aa 
                    ON w.ADVERTISER_ID = aa.ID
                WHERE w.AGENCY_ID = %(agency_id)s
                  AND w.LOG_DATE BETWEEN %(start_date)s AND %(end_date)s
                GROUP BY w.ADVERTISER_ID
                HAVING SUM(w.IMPRESSIONS) > 0 OR SUM(w.VISITORS) > 0
                ORDER BY 3 DESC
            """
            cursor.execute(query, {
                'agency_id': agency_id,
                'start_date': start_date,
                'end_date': end_date
            })
        
        columns = [desc[0] for desc in cursor.description]
        results = [dict(zip(columns, row)) for row in cursor.fetchall()]
        
        if agency_id == 1480:
            for r in results:
                if r.get('ADVERTISER_NAME'):
                    r['ADVERTISER_NAME'] = re.sub(r'^[0-9A-Za-z]+ - ', '', r['ADVERTISER_NAME'])
        
        cursor.close()
        conn.close()
        
        return jsonify({'success': True, 'data': results})
        
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

# =============================================================================
# CAMPAIGN PERFORMANCE
# =============================================================================

@app.route('/api/v5/campaign-performance', methods=['GET'])
def get_campaign_performance():
    try:
        agency_id = request.args.get('agency_id')
        advertiser_id = request.args.get('advertiser_id')
        
        if not agency_id or not advertiser_id:
            return jsonify({'success': False, 'error': 'agency_id and advertiser_id required'}), 400
        
        agency_id = int(agency_id)
        start_date, end_date = get_date_range()
        conn = get_snowflake_connection()
        cursor = conn.cursor()
        
        if agency_id == 1480:
            query = """
                SELECT 
                    IO_ID,
                    MAX(IO_NAME) as IO_NAME,
                    COUNT(*) as IMPRESSIONS,
                    COUNT(DISTINCT CASE WHEN IS_STORE_VISIT = 'TRUE' THEN IMP_MAID END) as STORE_VISITS,
                    COUNT(DISTINCT CASE WHEN IS_SITE_VISIT = 'TRUE' THEN IMP_MAID END) as WEB_VISITS
                FROM QUORUMDB.SEGMENT_DATA.PARAMOUNT_IMPRESSIONS_REPORT_90_DAYS
                WHERE QUORUM_ADVERTISER_ID = %(advertiser_id)s
                  AND IMP_DATE BETWEEN %(start_date)s AND %(end_date)s
                GROUP BY IO_ID
                HAVING COUNT(*) >= 100
                ORDER BY 3 DESC
            """
        else:
            query = """
                SELECT 
                    CAST(IO_ID AS NUMBER) as IO_ID,
                    MAX(IO_NAME) as IO_NAME,
                    SUM(IMPRESSIONS) as IMPRESSIONS,
                    SUM(VISITORS) as STORE_VISITS,
                    0 as WEB_VISITS
                FROM QUORUMDB.SEGMENT_DATA.CAMPAIGN_PERFORMANCE_REPORT_WEEKLY_STATS
                WHERE AGENCY_ID = %(agency_id)s
                  AND ADVERTISER_ID = %(advertiser_id)s
                  AND LOG_DATE BETWEEN %(start_date)s AND %(end_date)s
                GROUP BY IO_ID
                HAVING SUM(IMPRESSIONS) >= 100 OR SUM(VISITORS) >= 10
                ORDER BY 3 DESC
            """
        
        cursor.execute(query, {
            'agency_id': agency_id,
            'advertiser_id': advertiser_id,
            'start_date': start_date,
            'end_date': end_date
        })
        
        columns = [desc[0] for desc in cursor.description]
        results = [dict(zip(columns, row)) for row in cursor.fetchall()]
        
        cursor.close()
        conn.close()
        
        return jsonify({'success': True, 'data': results})
        
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

# =============================================================================
# LINE ITEM PERFORMANCE
# =============================================================================

@app.route('/api/v5/lineitem-performance', methods=['GET'])
def get_lineitem_performance():
    try:
        agency_id = request.args.get('agency_id')
        advertiser_id = request.args.get('advertiser_id')
        campaign_id = request.args.get('campaign_id')
        
        if not agency_id or not advertiser_id:
            return jsonify({'success': False, 'error': 'agency_id and advertiser_id required'}), 400
        
        agency_id = int(agency_id)
        start_date, end_date = get_date_range()
        conn = get_snowflake_connection()
        cursor = conn.cursor()
        
        campaign_filter = ""
        if campaign_id:
            campaign_filter = f"AND IO_ID = '{campaign_id}'"
        
        if agency_id == 1480:
            query = f"""
                SELECT 
                    LINEITEM_ID as LI_ID,
                    MAX(LINEITEM_NAME) as LI_NAME,
                    MAX(IO_ID) as IO_ID,
                    MAX(IO_NAME) as IO_NAME,
                    COUNT(*) as IMPRESSIONS,
                    COUNT(DISTINCT CASE WHEN IS_STORE_VISIT = 'TRUE' THEN IMP_MAID END) as STORE_VISITS,
                    COUNT(DISTINCT CASE WHEN IS_SITE_VISIT = 'TRUE' THEN IMP_MAID END) as WEB_VISITS,
                    'Paramount' as PLATFORM
                FROM QUORUMDB.SEGMENT_DATA.PARAMOUNT_IMPRESSIONS_REPORT_90_DAYS
                WHERE QUORUM_ADVERTISER_ID = %(advertiser_id)s
                  AND IMP_DATE BETWEEN %(start_date)s AND %(end_date)s
                  {campaign_filter}
                GROUP BY LINEITEM_ID
                HAVING COUNT(*) >= 100
                ORDER BY COUNT(*) DESC
                LIMIT 100
            """
        else:
            query = f"""
                WITH lineitem_stats AS (
                    SELECT 
                        LI_ID, MAX(LI_NAME) as LI_NAME, MAX(IO_ID) as IO_ID, MAX(IO_NAME) as IO_NAME,
                        SUM(IMPRESSIONS) as IMPRESSIONS, SUM(VISITORS) as STORE_VISITS, 0 as WEB_VISITS
                    FROM QUORUMDB.SEGMENT_DATA.CAMPAIGN_PERFORMANCE_REPORT_WEEKLY_STATS
                    WHERE AGENCY_ID = %(agency_id)s AND ADVERTISER_ID = %(advertiser_id)s
                      AND LOG_DATE BETWEEN %(start_date)s AND %(end_date)s {campaign_filter}
                    GROUP BY LI_ID
                    HAVING SUM(IMPRESSIONS) >= 100 OR SUM(VISITORS) >= 10
                ),
                lineitem_pt AS (
                    SELECT LINEITEM_ID, PT, COUNT(*) as cnt,
                        ROW_NUMBER() OVER (PARTITION BY LINEITEM_ID ORDER BY COUNT(*) DESC) as rn
                    FROM QUORUMDB.SEGMENT_DATA.XANDR_IMPRESSION_LOG
                    WHERE AGENCY_ID = %(agency_id)s AND TIMESTAMP::DATE BETWEEN %(start_date)s AND %(end_date)s
                    GROUP BY LINEITEM_ID, PT
                )
                SELECT ls.LI_ID, ls.LI_NAME, ls.IO_ID, ls.IO_NAME, ls.IMPRESSIONS, ls.STORE_VISITS, ls.WEB_VISITS,
                    COALESCE(p.PLATFORM, 'PT=' || COALESCE(lp.PT::VARCHAR, '?')) as PLATFORM
                FROM lineitem_stats ls
                LEFT JOIN lineitem_pt lp ON ls.LI_ID = lp.LINEITEM_ID AND lp.rn = 1
                LEFT JOIN QUORUMDB.SEGMENT_DATA.PT_TO_PLATFORM p ON lp.PT = p.PT
                ORDER BY ls.IMPRESSIONS DESC LIMIT 100
            """
        
        cursor.execute(query, {
            'agency_id': agency_id, 'advertiser_id': advertiser_id,
            'start_date': start_date, 'end_date': end_date
        })
        
        columns = [desc[0] for desc in cursor.description]
        results = [dict(zip(columns, row)) for row in cursor.fetchall()]
        
        cursor.close()
        conn.close()
        return jsonify({'success': True, 'data': results})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

# =============================================================================
# PUBLISHER PERFORMANCE
# =============================================================================

@app.route('/api/v5/publisher-performance', methods=['GET'])
def get_publisher_performance():
    try:
        agency_id = request.args.get('agency_id')
        advertiser_id = request.args.get('advertiser_id')
        campaign_id = request.args.get('campaign_id')
        lineitem_id = request.args.get('lineitem_id')
        
        if not agency_id or not advertiser_id:
            return jsonify({'success': False, 'error': 'agency_id and advertiser_id required'}), 400
        
        agency_id = int(agency_id)
        start_date, end_date = get_date_range()
        conn = get_snowflake_connection()
        cursor = conn.cursor()
        
        paramount_filters = ""
        if campaign_id: paramount_filters += f" AND IO_ID = '{campaign_id}'"
        if lineitem_id: paramount_filters += f" AND LINEITEM_ID = '{lineitem_id}'"
        
        classb_filters = ""
        if campaign_id: classb_filters += f" AND IO_ID = '{campaign_id}'"
        if lineitem_id: classb_filters += f" AND LI_ID = '{lineitem_id}'"
        
        if agency_id == 1480:
            query = f"""
                SELECT SITE as PUBLISHER, COUNT(*) as IMPRESSIONS,
                    COUNT(DISTINCT CASE WHEN IS_STORE_VISIT = 'TRUE' THEN IMP_MAID END) as STORE_VISITS,
                    COUNT(DISTINCT CASE WHEN IS_SITE_VISIT = 'TRUE' THEN IMP_MAID END) as WEB_VISITS
                FROM QUORUMDB.SEGMENT_DATA.PARAMOUNT_IMPRESSIONS_REPORT_90_DAYS
                WHERE QUORUM_ADVERTISER_ID = %(advertiser_id)s
                  AND IMP_DATE BETWEEN %(start_date)s AND %(end_date)s {paramount_filters}
                GROUP BY SITE HAVING COUNT(*) >= 100 ORDER BY 2 DESC LIMIT 50
            """
        else:
            query = f"""
                SELECT PUBLISHER, SUM(IMPRESSIONS) as IMPRESSIONS,
                    SUM(VISITORS) as STORE_VISITS, 0 as WEB_VISITS
                FROM QUORUMDB.SEGMENT_DATA.CAMPAIGN_PERFORMANCE_REPORT_WEEKLY_STATS
                WHERE AGENCY_ID = %(agency_id)s AND ADVERTISER_ID = %(advertiser_id)s
                  AND LOG_DATE BETWEEN %(start_date)s AND %(end_date)s {classb_filters}
                GROUP BY PUBLISHER HAVING SUM(IMPRESSIONS) >= 100 ORDER BY 2 DESC LIMIT 50
            """
        
        cursor.execute(query, {
            'agency_id': agency_id, 'advertiser_id': advertiser_id,
            'start_date': start_date, 'end_date': end_date
        })
        
        columns = [desc[0] for desc in cursor.description]
        results = [dict(zip(columns, row)) for row in cursor.fetchall()]
        cursor.close()
        conn.close()
        return jsonify({'success': True, 'data': results})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

# =============================================================================
# GEOGRAPHIC / ZIP PERFORMANCE
# =============================================================================

@app.route('/api/v5/zip-performance', methods=['GET'])
def get_zip_performance():
    try:
        agency_id = request.args.get('agency_id')
        advertiser_id = request.args.get('advertiser_id')
        campaign_id = request.args.get('campaign_id')
        lineitem_id = request.args.get('lineitem_id')
        
        if not agency_id or not advertiser_id:
            return jsonify({'success': False, 'error': 'agency_id and advertiser_id required'}), 400
        
        agency_id = int(agency_id)
        conn = get_snowflake_connection()
        cursor = conn.cursor()
        
        if agency_id == 1480:
            start_date, end_date = get_date_range()
            filters = ""
            if campaign_id: filters += f" AND IO_ID = '{campaign_id}'"
            if lineitem_id: filters += f" AND LINEITEM_ID = '{lineitem_id}'"
            
            query = f"""
                SELECT ZIP_CODE, COUNT(*) as IMPRESSIONS,
                    COUNT(DISTINCT CASE WHEN IS_STORE_VISIT = 'TRUE' THEN IMP_MAID END) as STORE_VISITS,
                    COUNT(DISTINCT CASE WHEN IS_SITE_VISIT = 'TRUE' THEN IMP_MAID END) as WEB_VISITS
                FROM QUORUMDB.SEGMENT_DATA.PARAMOUNT_IMPRESSIONS_REPORT_90_DAYS
                WHERE QUORUM_ADVERTISER_ID = %(advertiser_id)s
                  AND IMP_DATE BETWEEN %(start_date)s AND %(end_date)s
                  AND ZIP_CODE IS NOT NULL AND ZIP_CODE != '' AND ZIP_CODE != 'null' AND ZIP_CODE != 'UNKNOWN'
                  {filters}
                GROUP BY ZIP_CODE HAVING COUNT(*) >= 100
                ORDER BY 3 DESC, 2 DESC LIMIT 100
            """
            cursor.execute(query, {'advertiser_id': advertiser_id, 'start_date': start_date, 'end_date': end_date})
            note = 'Date filtered (matches date selector)'
        else:
            filters = ""
            if campaign_id: filters += f" AND CAMPAIGN_ID = {campaign_id}"
            if lineitem_id: filters += f" AND LINEITEM_ID = '{lineitem_id}'"
            
            query = f"""
                SELECT USER_HOME_POSTAL_CODE as ZIP_CODE, SUM(IMPRESSIONS) as IMPRESSIONS,
                    SUM(STORE_VISITS) as STORE_VISITS, 0 as WEB_VISITS
                FROM QUORUMDB.SEGMENT_DATA.CAMPAIGN_POSTAL_REPORTING
                WHERE AGENCY_ID = %(agency_id)s AND ADVERTISER_ID = %(advertiser_id)s
                  AND USER_HOME_POSTAL_CODE IS NOT NULL AND USER_HOME_POSTAL_CODE != ''
                  AND USER_HOME_POSTAL_CODE != 'null' AND USER_HOME_POSTAL_CODE != 'UNKNOWN'
                  {filters}
                GROUP BY USER_HOME_POSTAL_CODE
                HAVING SUM(IMPRESSIONS) >= 100 OR SUM(STORE_VISITS) >= 1
                ORDER BY 3 DESC, 2 DESC LIMIT 100
            """
            cursor.execute(query, {'agency_id': agency_id, 'advertiser_id': advertiser_id})
            note = 'Full history (all-time data)'
        
        columns = [desc[0] for desc in cursor.description]
        results = [dict(zip(columns, row)) for row in cursor.fetchall()]
        cursor.close()
        conn.close()
        return jsonify({'success': True, 'data': results, 'note': note})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

# =============================================================================
# DMA PERFORMANCE
# =============================================================================

@app.route('/api/v5/dma-performance', methods=['GET'])
def get_dma_performance():
    try:
        agency_id = request.args.get('agency_id')
        advertiser_id = request.args.get('advertiser_id')
        campaign_id = request.args.get('campaign_id')
        lineitem_id = request.args.get('lineitem_id')
        
        if not agency_id or not advertiser_id:
            return jsonify({'success': False, 'error': 'agency_id and advertiser_id required'}), 400
        
        agency_id = int(agency_id)
        start_date, end_date = get_date_range()
        conn = get_snowflake_connection()
        cursor = conn.cursor()
        
        if agency_id == 1480:
            filters = ""
            if campaign_id: filters += f" AND p.IO_ID = '{campaign_id}'"
            if lineitem_id: filters += f" AND p.LINEITEM_ID = '{lineitem_id}'"
            
            query = f"""
                WITH zip_dma AS (
                    SELECT ZIPCODE, MAX(DMA_NAME) as DMA_NAME 
                    FROM QUORUMDB.SEGMENT_DATA.DBIP_LOOKUP_US 
                    WHERE DMA_NAME IS NOT NULL AND DMA_NAME != ''
                    GROUP BY ZIPCODE
                )
                SELECT d.DMA_NAME as DMA, COUNT(*) as IMPRESSIONS,
                    COUNT(DISTINCT CASE WHEN p.IS_STORE_VISIT = 'TRUE' THEN p.IMP_MAID END) as STORE_VISITS,
                    COUNT(DISTINCT CASE WHEN p.IS_SITE_VISIT = 'TRUE' THEN p.IMP_MAID END) as WEB_VISITS
                FROM QUORUMDB.SEGMENT_DATA.PARAMOUNT_IMPRESSIONS_REPORT_90_DAYS p
                JOIN zip_dma d ON p.ZIP_CODE = d.ZIPCODE
                WHERE p.QUORUM_ADVERTISER_ID = %(advertiser_id)s
                  AND p.IMP_DATE BETWEEN %(start_date)s AND %(end_date)s {filters}
                GROUP BY d.DMA_NAME HAVING COUNT(*) >= 100 ORDER BY 2 DESC LIMIT 50
            """
            cursor.execute(query, {'advertiser_id': advertiser_id, 'start_date': start_date, 'end_date': end_date})
        else:
            filters = ""
            if campaign_id: filters += f" AND IO_ID = '{campaign_id}'"
            if lineitem_id: filters += f" AND LI_ID = '{lineitem_id}'"
            
            query = f"""
                SELECT DMA, SUM(IMPRESSIONS) as IMPRESSIONS,
                    SUM(VISITORS) as STORE_VISITS, 0 as WEB_VISITS
                FROM QUORUMDB.SEGMENT_DATA.CAMPAIGN_PERFORMANCE_REPORT_WEEKLY_STATS
                WHERE AGENCY_ID = %(agency_id)s AND ADVERTISER_ID = %(advertiser_id)s
                  AND LOG_DATE BETWEEN %(start_date)s AND %(end_date)s
                  AND DMA IS NOT NULL AND DMA != '' {filters}
                GROUP BY DMA HAVING SUM(IMPRESSIONS) >= 100 ORDER BY 2 DESC LIMIT 50
            """
            cursor.execute(query, {'agency_id': agency_id, 'advertiser_id': advertiser_id, 'start_date': start_date, 'end_date': end_date})
        
        columns = [desc[0] for desc in cursor.description]
        results = [dict(zip(columns, row)) for row in cursor.fetchall()]
        cursor.close()
        conn.close()
        return jsonify({'success': True, 'data': results})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

# =============================================================================
# SUMMARY ENDPOINT
# =============================================================================

@app.route('/api/v5/summary', methods=['GET'])
def get_summary():
    try:
        agency_id = request.args.get('agency_id')
        advertiser_id = request.args.get('advertiser_id')
        
        if not agency_id or not advertiser_id:
            return jsonify({'success': False, 'error': 'agency_id and advertiser_id required'}), 400
        
        agency_id = int(agency_id)
        start_date, end_date = get_date_range()
        conn = get_snowflake_connection()
        cursor = conn.cursor()
        
        if agency_id == 1480:
            query = """
                SELECT SUM(IMPRESSIONS) as IMPRESSIONS, SUM(STORE_VISITS) as STORE_VISITS,
                    SUM(SITE_VISITS) as WEB_VISITS, MIN(DATE) as MIN_DATE, MAX(DATE) as MAX_DATE
                FROM QUORUMDB.SEGMENT_DATA.PARAMOUNT_DASHBOARD_SUMMARY_STATS
                WHERE QUORUM_ADVERTISER_ID = %(advertiser_id)s AND DATE BETWEEN %(start_date)s AND %(end_date)s
            """
        else:
            query = """
                SELECT SUM(IMPRESSIONS) as IMPRESSIONS, SUM(VISITORS) as STORE_VISITS,
                    0 as WEB_VISITS, MIN(LOG_DATE) as MIN_DATE, MAX(LOG_DATE) as MAX_DATE,
                    COUNT(DISTINCT IO_ID) as CAMPAIGN_COUNT, COUNT(DISTINCT LI_ID) as LINEITEM_COUNT
                FROM QUORUMDB.SEGMENT_DATA.CAMPAIGN_PERFORMANCE_REPORT_WEEKLY_STATS
                WHERE AGENCY_ID = %(agency_id)s AND ADVERTISER_ID = %(advertiser_id)s
                  AND LOG_DATE BETWEEN %(start_date)s AND %(end_date)s
            """
        
        cursor.execute(query, {'agency_id': agency_id, 'advertiser_id': advertiser_id, 'start_date': start_date, 'end_date': end_date})
        columns = [desc[0] for desc in cursor.description]
        row = cursor.fetchone()
        result = dict(zip(columns, row)) if row else {}
        
        imps = result.get('IMPRESSIONS') or 0
        store = result.get('STORE_VISITS') or 0
        web = result.get('WEB_VISITS') or 0
        result['STORE_VISIT_RATE'] = round(store * 100.0 / imps, 4) if imps > 0 else 0
        result['WEB_VISIT_RATE'] = round(web * 100.0 / imps, 4) if imps > 0 else 0
        result['TOTAL_VISITS'] = store + web
        
        cursor.close()
        conn.close()
        return jsonify({'success': True, 'data': result})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

# =============================================================================
# TIMESERIES ENDPOINT
# =============================================================================

@app.route('/api/v5/timeseries', methods=['GET'])
def get_timeseries():
    try:
        agency_id = request.args.get('agency_id')
        advertiser_id = request.args.get('advertiser_id')
        
        if not agency_id or not advertiser_id:
            return jsonify({'success': False, 'error': 'agency_id and advertiser_id required'}), 400
        
        agency_id = int(agency_id)
        start_date, end_date = get_date_range()
        conn = get_snowflake_connection()
        cursor = conn.cursor()
        
        if agency_id == 1480:
            query = """
                WITH deduped AS (
                    SELECT DISTINCT DATE, QUORUM_ADVERTISER_ID, IMPRESSIONS, STORE_VISITS, SITE_VISITS
                    FROM QUORUMDB.SEGMENT_DATA.PARAMOUNT_DASHBOARD_SUMMARY_STATS
                    WHERE QUORUM_ADVERTISER_ID = %(advertiser_id)s AND DATE BETWEEN %(start_date)s AND %(end_date)s
                )
                SELECT DATE as LOG_DATE, SUM(IMPRESSIONS) as IMPRESSIONS, 
                       SUM(STORE_VISITS) as STORE_VISITS, SUM(SITE_VISITS) as WEB_VISITS
                FROM deduped GROUP BY DATE ORDER BY DATE
            """
        else:
            query = """
                SELECT LOG_DATE, SUM(IMPRESSIONS) as IMPRESSIONS, SUM(VISITORS) as STORE_VISITS, 0 as WEB_VISITS
                FROM QUORUMDB.SEGMENT_DATA.CAMPAIGN_PERFORMANCE_REPORT_WEEKLY_STATS
                WHERE AGENCY_ID = %(agency_id)s AND ADVERTISER_ID = %(advertiser_id)s
                  AND LOG_DATE BETWEEN %(start_date)s AND %(end_date)s
                GROUP BY LOG_DATE ORDER BY LOG_DATE
            """
        
        cursor.execute(query, {'agency_id': agency_id, 'advertiser_id': advertiser_id, 'start_date': start_date, 'end_date': end_date})
        columns = [desc[0] for desc in cursor.description]
        results = []
        for row in cursor.fetchall():
            d = dict(zip(columns, row))
            if d.get('LOG_DATE'): d['LOG_DATE'] = str(d['LOG_DATE'])
            results.append(d)
        
        cursor.close()
        conn.close()
        return jsonify({'success': True, 'data': results})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

# =============================================================================
# LIFT ANALYSIS
# =============================================================================

@app.route('/api/v5/lift-analysis', methods=['GET'])
def get_lift_analysis():
    try:
        agency_id = request.args.get('agency_id')
        advertiser_id = request.args.get('advertiser_id')
        group_by = request.args.get('group_by', 'campaign')
        
        if not agency_id or not advertiser_id:
            return jsonify({'success': False, 'error': 'agency_id and advertiser_id required'}), 400
        
        agency_id = int(agency_id)
        start_date, end_date = get_date_range()
        conn = get_snowflake_connection()
        cursor = conn.cursor()
        
        if agency_id == 1480:
            if group_by == 'lineitem':
                group_cols = "IO_ID, LINEITEM_ID"
                name_cols = """
                    COALESCE(MAX(LINEITEM_NAME), 'LI-' || LINEITEM_ID::VARCHAR) as NAME,
                    COALESCE(MAX(IO_NAME), 'IO-' || IO_ID::VARCHAR) as PARENT_NAME,
                    LINEITEM_ID as ID, IO_ID as PARENT_ID,
                """
            else:
                group_cols = "IO_ID"
                name_cols = """
                    COALESCE(MAX(IO_NAME), 'IO-' || IO_ID::VARCHAR) as NAME,
                    NULL as PARENT_NAME, IO_ID as ID, NULL as PARENT_ID,
                """
            
            query = f"""
                WITH 
                exposed_devices AS (
                    SELECT DISTINCT LOWER(REPLACE(IMP_MAID,'-','')) AS device_id
                    FROM QUORUMDB.SEGMENT_DATA.PARAMOUNT_IMPRESSIONS_REPORT_90_DAYS
                    WHERE QUORUM_ADVERTISER_ID::INT = %(advertiser_id)s
                      AND IMP_DATE BETWEEN %(start_date)s AND %(end_date)s AND IMP_MAID IS NOT NULL
                ),
                control_devices AS (
                    SELECT DISTINCT LOWER(REPLACE(IMP_MAID,'-','')) AS device_id
                    FROM QUORUMDB.SEGMENT_DATA.PARAMOUNT_IMPRESSIONS_REPORT_90_DAYS
                    WHERE QUORUM_ADVERTISER_ID::INT != %(advertiser_id)s
                      AND IMP_DATE BETWEEN %(start_date)s AND %(end_date)s AND IMP_MAID IS NOT NULL
                      AND LOWER(REPLACE(IMP_MAID,'-','')) NOT IN (SELECT device_id FROM exposed_devices)
                ),
                adv_web_visit_days AS (
                    SELECT LOWER(REPLACE(MAID,'-','')) AS device_id, DATE(SITE_VISIT_TIMESTAMP) AS event_date
                    FROM QUORUMDB.SEGMENT_DATA.PARAMOUNT_SITEVISITS
                    WHERE QUORUM_ADVERTISER_ID = %(advertiser_id_str)s AND MAID IS NOT NULL
                    GROUP BY 1, 2
                ),
                adv_store_visit_days AS (
                    SELECT LOWER(REPLACE(MAID,'-','')) AS device_id, DATE(DRIVE_BY_TIME) AS event_date
                    FROM QUORUMDB.SEGMENT_DATA.PARAMOUNT_STORE_VISIT_RAW_90_DAYS
                    WHERE ADVERTISER_ID = %(advertiser_id)s AND MAID IS NOT NULL
                    GROUP BY 1, 2
                ),
                web_network_control AS (
                    SELECT COUNT(DISTINCT c.device_id) AS control_n,
                        COUNT(DISTINCT CASE WHEN v.device_id IS NOT NULL THEN c.device_id END) AS control_visitors,
                        COUNT(DISTINCT CASE WHEN v.device_id IS NOT NULL THEN c.device_id END)::FLOAT 
                            / NULLIF(COUNT(DISTINCT c.device_id), 0) * 100 AS control_rate
                    FROM control_devices c LEFT JOIN adv_web_visit_days v ON v.device_id = c.device_id
                ),
                store_network_control AS (
                    SELECT COUNT(DISTINCT c.device_id) AS control_n,
                        COUNT(DISTINCT CASE WHEN v.device_id IS NOT NULL THEN c.device_id END) AS control_visitors,
                        COUNT(DISTINCT CASE WHEN v.device_id IS NOT NULL THEN c.device_id END)::FLOAT 
                            / NULLIF(COUNT(DISTINCT c.device_id), 0) * 100 AS control_rate
                    FROM control_devices c LEFT JOIN adv_store_visit_days v ON v.device_id = c.device_id
                ),
                exposed_store_visitors AS (
                    SELECT COUNT(DISTINCT CASE WHEN sv.device_id IS NOT NULL THEN e.device_id END) AS store_visitors
                    FROM exposed_devices e LEFT JOIN adv_store_visit_days sv ON sv.device_id = e.device_id
                ),
                campaign_metrics AS (
                    SELECT {group_cols}, {name_cols}
                        COUNT(*) as IMPRESSIONS, COUNT(DISTINCT IMP_MAID) as REACH,
                        COUNT(DISTINCT CASE WHEN IS_SITE_VISIT = 'TRUE' THEN IMP_MAID END) as WEB_VISITORS
                    FROM QUORUMDB.SEGMENT_DATA.PARAMOUNT_IMPRESSIONS_REPORT_90_DAYS
                    WHERE QUORUM_ADVERTISER_ID::INT = %(advertiser_id)s
                      AND IMP_DATE BETWEEN %(start_date)s AND %(end_date)s
                    GROUP BY {group_cols} HAVING COUNT(*) >= 1000
                ),
                adv_baselines AS (
                    SELECT SUM(WEB_VISITORS)::FLOAT / NULLIF(SUM(REACH), 0) * 100 as WEB_BASELINE,
                        SUM(WEB_VISITORS) as TOTAL_WEB
                    FROM campaign_metrics
                )
                SELECT c.NAME, c.PARENT_NAME, c.ID, c.PARENT_ID, c.IMPRESSIONS, c.REACH as PANEL_REACH,
                    c.WEB_VISITORS, ROUND(c.WEB_VISITORS::FLOAT / NULLIF(c.REACH, 0) * 100, 4) as WEB_VISIT_RATE,
                    ROUND(b.WEB_BASELINE, 4) as WEB_ADV_BASELINE,
                    CASE WHEN b.WEB_BASELINE > 0 THEN ROUND(c.WEB_VISITORS::FLOAT / NULLIF(c.REACH, 0) * 100 / b.WEB_BASELINE * 100, 1) END as WEB_INDEX,
                    ROUND(wnc.control_rate, 4) as WEB_NETWORK_BASELINE, wnc.control_n as WEB_CONTROL_N, wnc.control_visitors as WEB_CONTROL_VISITORS,
                    CASE WHEN wnc.control_rate > 0 THEN ROUND((c.WEB_VISITORS::FLOAT / NULLIF(c.REACH, 0) * 100 - wnc.control_rate) / wnc.control_rate * 100, 1) END as WEB_LIFT_VS_NETWORK,
                    CASE WHEN wnc.control_rate > 0 AND c.REACH > 0 AND wnc.control_n > 0 THEN ROUND(
                        (c.WEB_VISITORS::FLOAT / c.REACH - wnc.control_visitors::FLOAT / wnc.control_n) /
                        NULLIF(SQRT(((c.WEB_VISITORS + wnc.control_visitors)::FLOAT / (c.REACH + wnc.control_n)) *
                        (1 - (c.WEB_VISITORS + wnc.control_visitors)::FLOAT / (c.REACH + wnc.control_n)) *
                        (1.0/c.REACH + 1.0/wnc.control_n)), 0), 2) END as WEB_Z_SCORE,
                    esv.store_visitors as STORE_VISITORS,
                    ROUND(esv.store_visitors::FLOAT / NULLIF(c.REACH, 0) * 100, 4) as STORE_VISIT_RATE,
                    ROUND(snc.control_rate, 4) as STORE_NETWORK_BASELINE, snc.control_n as STORE_CONTROL_N, snc.control_visitors as STORE_CONTROL_VISITORS,
                    CASE WHEN snc.control_rate > 0 THEN ROUND((esv.store_visitors::FLOAT / NULLIF(c.REACH, 0) * 100 - snc.control_rate) / snc.control_rate * 100, 1) END as STORE_LIFT_VS_NETWORK,
                    CASE WHEN snc.control_rate > 0 AND c.REACH > 0 AND snc.control_n > 0 THEN ROUND(
                        (esv.store_visitors::FLOAT / c.REACH - snc.control_visitors::FLOAT / snc.control_n) /
                        NULLIF(SQRT(((esv.store_visitors + snc.control_visitors)::FLOAT / (c.REACH + snc.control_n)) *
                        (1 - (esv.store_visitors + snc.control_visitors)::FLOAT / (c.REACH + snc.control_n)) *
                        (1.0/c.REACH + 1.0/snc.control_n)), 0), 2) END as STORE_Z_SCORE,
                    b.TOTAL_WEB, esv.store_visitors as TOTAL_STORE
                FROM campaign_metrics c
                CROSS JOIN adv_baselines b CROSS JOIN web_network_control wnc
                CROSS JOIN store_network_control snc CROSS JOIN exposed_store_visitors esv
                WHERE c.REACH >= 100 ORDER BY c.IMPRESSIONS DESC LIMIT 100
            """
            
            cursor.execute(query, {
                'advertiser_id': int(advertiser_id), 'advertiser_id_str': str(advertiser_id),
                'start_date': start_date, 'end_date': end_date
            })
            
            columns = [desc[0] for desc in cursor.description]
            rows = cursor.fetchall()
            cursor.close()
            conn.close()
            
            if not rows:
                return jsonify({
                    'success': True, 'web_data': [], 'store_data': [],
                    'web_adv_baseline': None, 'web_network_baseline': None, 'store_network_baseline': None,
                    'web_control_n': 0, 'store_control_n': 0,
                    'total_web_visitors': 0, 'total_store_visitors': 0,
                    'message': 'No lift data available - requires minimum 1,000 impressions per campaign'
                })
            
            web_data, store_data = [], []
            web_adv_baseline = web_network_baseline = store_network_baseline = None
            web_control_n = store_control_n = total_web = total_store = 0
            
            for row in rows:
                d = dict(zip(columns, row))
                if total_web == 0:
                    total_web = int(d.get('TOTAL_WEB') or 0)
                    total_store = int(d.get('TOTAL_STORE') or 0)
                    web_adv_baseline = float(d.get('WEB_ADV_BASELINE') or 0)
                    web_network_baseline = float(d.get('WEB_NETWORK_BASELINE') or 0)
                    store_network_baseline = float(d.get('STORE_NETWORK_BASELINE') or 0)
                    web_control_n = int(d.get('WEB_CONTROL_N') or 0)
                    store_control_n = int(d.get('STORE_CONTROL_N') or 0)
                
                def confidence_from_z(z):
                    if z is None: return None
                    z = abs(float(z))
                    if z >= 2.576: return '99%'
                    if z >= 1.96: return '95%'
                    if z >= 1.645: return '90%'
                    return 'NS'
                
                web_data.append({
                    'NAME': d['NAME'], 'PARENT_NAME': d.get('PARENT_NAME'),
                    'ID': d.get('ID'), 'PARENT_ID': d.get('PARENT_ID'),
                    'IMPRESSIONS': int(d['IMPRESSIONS'] or 0), 'PANEL_REACH': int(d['PANEL_REACH'] or 0),
                    'VISITORS': int(d['WEB_VISITORS'] or 0),
                    'VISIT_RATE': float(d['WEB_VISIT_RATE'] or 0),
                    'ADV_BASELINE_VR': float(d['WEB_ADV_BASELINE'] or 0),
                    'INDEX_VS_AVG': float(d['WEB_INDEX']) if d['WEB_INDEX'] else None,
                    'NETWORK_BASELINE_VR': float(d['WEB_NETWORK_BASELINE'] or 0),
                    'LIFT_VS_NETWORK': float(d['WEB_LIFT_VS_NETWORK']) if d['WEB_LIFT_VS_NETWORK'] else None,
                    'Z_SCORE': float(d['WEB_Z_SCORE']) if d['WEB_Z_SCORE'] else None,
                    'CONFIDENCE': confidence_from_z(d.get('WEB_Z_SCORE')),
                })
                store_data.append({
                    'NAME': d['NAME'], 'PARENT_NAME': d.get('PARENT_NAME'),
                    'ID': d.get('ID'), 'PARENT_ID': d.get('PARENT_ID'),
                    'IMPRESSIONS': int(d['IMPRESSIONS'] or 0), 'PANEL_REACH': int(d['PANEL_REACH'] or 0),
                    'VISITORS': int(d['STORE_VISITORS'] or 0),
                    'VISIT_RATE': float(d['STORE_VISIT_RATE'] or 0),
                    'NETWORK_BASELINE_VR': float(d['STORE_NETWORK_BASELINE'] or 0),
                    'LIFT_VS_NETWORK': float(d['STORE_LIFT_VS_NETWORK']) if d['STORE_LIFT_VS_NETWORK'] else None,
                    'Z_SCORE': float(d['STORE_Z_SCORE']) if d['STORE_Z_SCORE'] else None,
                    'CONFIDENCE': confidence_from_z(d.get('STORE_Z_SCORE')),
                })
            
            return jsonify({
                'success': True, 'web_data': web_data, 'store_data': store_data,
                'web_adv_baseline': web_adv_baseline, 'web_network_baseline': web_network_baseline,
                'store_network_baseline': store_network_baseline,
                'web_control_n': web_control_n, 'store_control_n': store_control_n,
                'total_web_visitors': total_web, 'total_store_visitors': total_store,
                'methodology': {
                    'index': 'Campaign visit rate vs advertiser average',
                    'lift_vs_network': 'Campaign visit rate vs network control',
                    'web_source': 'PARAMOUNT_SITEVISITS',
                    'store_source': 'PARAMOUNT_STORE_VISIT_RAW_90_DAYS'
                }
            })
            
        else:
            if group_by == 'lineitem':
                group_cols = "IO_ID, IO_NAME, LI_ID, LI_NAME"
                name_cols = "LI_NAME as NAME, IO_NAME as PARENT_NAME, LI_ID as ID, IO_ID as PARENT_ID,"
            else:
                group_cols = "IO_ID, IO_NAME"
                name_cols = "IO_NAME as NAME, NULL as PARENT_NAME, IO_ID as ID, NULL as PARENT_ID,"
            
            query = f"""
                WITH campaign_metrics AS (
                    SELECT {group_cols}, {name_cols}
                        SUM(IMPRESSIONS) as IMPRESSIONS, SUM(REACH) as REACH,
                        SUM(PANEL_REACH) as PANEL_REACH, SUM(VISITORS) as VISITORS
                    FROM QUORUMDB.SEGMENT_DATA.CAMPAIGN_PERFORMANCE_REPORT_WEEKLY_STATS
                    WHERE ADVERTISER_ID = %(advertiser_id)s AND LOG_DATE BETWEEN %(start_date)s AND %(end_date)s
                    GROUP BY {group_cols} HAVING SUM(IMPRESSIONS) >= 1000
                ),
                baseline AS (
                    SELECT SUM(VISITORS)::FLOAT / NULLIF(SUM(PANEL_REACH), 0) * 100 as BASELINE_VR
                    FROM campaign_metrics
                )
                SELECT c.NAME, c.PARENT_NAME, c.ID, c.PARENT_ID, c.IMPRESSIONS, c.REACH, c.PANEL_REACH, c.VISITORS,
                    ROUND(c.VISITORS::FLOAT / NULLIF(c.PANEL_REACH, 0) * 100, 4) as VISIT_RATE,
                    ROUND(b.BASELINE_VR, 4) as BASELINE_VR,
                    CASE WHEN b.BASELINE_VR > 0 THEN ROUND(c.VISITORS::FLOAT / NULLIF(c.PANEL_REACH, 0) * 100 / b.BASELINE_VR * 100, 1) END as INDEX_VS_AVG,
                    CASE WHEN b.BASELINE_VR > 0 THEN ROUND((c.VISITORS::FLOAT / NULLIF(c.PANEL_REACH, 0) * 100 - b.BASELINE_VR) / b.BASELINE_VR * 100, 1) END as LIFT_PCT
                FROM campaign_metrics c CROSS JOIN baseline b
                WHERE c.PANEL_REACH >= 1000 ORDER BY c.IMPRESSIONS DESC LIMIT 100
            """
            cursor.execute(query, {'advertiser_id': int(advertiser_id), 'start_date': start_date, 'end_date': end_date})
            visit_type = 'store'
        
        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        
        if not rows:
            cursor.close()
            conn.close()
            return jsonify({'success': True, 'data': [], 'baseline': None, 'visit_type': visit_type,
                'message': 'No lift data available - requires minimum 1,000 panel reach per campaign'})
        
        baseline = None
        results = []
        for row in rows:
            d = dict(zip(columns, row))
            if baseline is None and d.get('BASELINE_VR'):
                baseline = float(d['BASELINE_VR'])
            for k, v in d.items():
                if hasattr(v, 'is_integer'):
                    d[k] = float(v) if v else 0
            results.append(d)
        
        cursor.close()
        conn.close()
        return jsonify({'success': True, 'data': results, 'baseline': baseline, 'visit_type': visit_type})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

# =============================================================================
# TRAFFIC SOURCES (Paramount only)
# =============================================================================

@app.route('/api/v5/traffic-sources', methods=['GET'])
def get_traffic_sources():
    """Compare engagement (pageviews/visit) between click-based traffic sources
    and CTV view-through visitors. Shows overlap % and CTV-exposed vs non-exposed engagement."""
    advertiser_id = request.args.get('advertiser_id')
    
    if not advertiser_id:
        return jsonify({'success': False, 'error': 'advertiser_id parameter required'}), 400
    
    try:
        start_date, end_date = get_date_range()
        conn = get_snowflake_connection()
        cursor = conn.cursor()
        
        query = """
            WITH 
            -- All web visitor UUIDs for this advertiser in date range (sampled for performance)
            visitor_uuids AS (
                SELECT WEB_IMPRESSION_ID AS UUID,
                       LOWER(REPLACE(MAID, '-', '')) AS clean_maid,
                       SITE_VISIT_TIMESTAMP::DATE AS visit_date
                FROM QUORUMDB.SEGMENT_DATA.WEB_VISITORS_TO_LOG
                WHERE QUORUM_ADVERTISER_ID = %(advertiser_id)s
                  AND IS_SITE_VISIT = 'TRUE'
                  AND SITE_VISIT_TIMESTAMP BETWEEN %(start_date)s AND %(end_date)s
                  AND MAID IS NOT NULL AND MAID != ''
                LIMIT 50000
            ),
            
            -- Classify each UUID's traffic source via direct JOIN (faster than IN subquery)
            uuid_classified AS (
                SELECT vu.UUID, vu.clean_maid, vu.visit_date,
                    CASE 
                        WHEN p.VALUE ILIKE '%%doubleclick%%' OR p.VALUE ILIKE '%%syndicatedsearch%%' OR p.VALUE ILIKE '%%gclid%%' OR p.VALUE ILIKE '%%googleadservices%%' THEN 'Google Ads'
                        WHEN p.VALUE ILIKE '%%google%%' THEN 'Google Organic'
                        WHEN p.VALUE ILIKE '%%facebook%%' OR p.VALUE ILIKE '%%fbapp%%' OR p.VALUE ILIKE '%%fb.com%%' OR p.VALUE ILIKE '%%fbclid%%' THEN 'Meta/Facebook'
                        WHEN p.VALUE ILIKE '%%youtube%%' THEN 'YouTube'
                        WHEN p.VALUE ILIKE '%%instagram%%' THEN 'Instagram'
                        WHEN p.VALUE ILIKE '%%taboola%%' THEN 'Taboola'
                        WHEN p.VALUE ILIKE '%%outbrain%%' THEN 'Outbrain'
                        WHEN p.VALUE ILIKE '%%tiktok%%' THEN 'TikTok'
                        WHEN p.VALUE ILIKE '%%bing%%' THEN 'Bing'
                        WHEN p.VALUE ILIKE '%%yahoo%%' THEN 'Yahoo'
                        WHEN p.VALUE ILIKE '%%t.co%%' OR p.VALUE ILIKE '%%twitter%%' THEN 'Twitter/X'
                        WHEN p.VALUE ILIKE '%%linkedin%%' THEN 'LinkedIn'
                        WHEN p.VALUE ILIKE '%%pinterest%%' THEN 'Pinterest'
                        WHEN p.VALUE ILIKE '%%snapchat%%' THEN 'Snapchat'
                        WHEN p.VALUE ILIKE '%%reddit%%' THEN 'Reddit'
                        WHEN p.VALUE ILIKE '%%_ef_transaction%%' THEN 'Affiliate'
                        WHEN p.VALUE IS NULL OR p.VALUE = '-' OR p.VALUE = '' THEN 'Direct'
                        WHEN p.VALUE ILIKE '%%localhost%%' OR p.VALUE ILIKE '%%127.0.0.1%%' THEN 'SKIP'
                        ELSE 'Other Referral'
                    END AS traffic_source
                FROM visitor_uuids vu
                LEFT JOIN QUORUMDB.SEGMENT_DATA.PARAMOUNT_WEB_IMPRESSION_DATA p 
                    ON vu.UUID = p.UUID AND p.KEY = 'referrer'
            ),
            
            -- Aggregate at MAID-day level (pageviews = distinct UUIDs per MAID per day)
            maid_day AS (
                SELECT clean_maid, visit_date,
                       COUNT(*) AS pageviews,
                       MODE(traffic_source) AS dominant_source
                FROM uuid_classified
                WHERE traffic_source != 'SKIP'
                GROUP BY 1, 2
            ),
            
            -- CTV-exposed MAIDs for this advertiser
            ctv_maids AS (
                SELECT DISTINCT LOWER(REPLACE(IMP_MAID, '-', '')) AS clean_maid
                FROM QUORUMDB.SEGMENT_DATA.PARAMOUNT_IMPRESSIONS_REPORT_90_DAYS
                WHERE QUORUM_ADVERTISER_ID = %(advertiser_id_int)s
            ),
            
            -- Join CTV flag
            classified AS (
                SELECT md.dominant_source AS traffic_source,
                       md.pageviews,
                       md.clean_maid,
                       CASE WHEN cm.clean_maid IS NOT NULL THEN 1 ELSE 0 END AS is_ctv
                FROM maid_day md
                LEFT JOIN ctv_maids cm ON md.clean_maid = cm.clean_maid
            )
            
            -- Click-based sources with CTV split engagement
            SELECT traffic_source, 'click' AS SOURCE_TYPE,
                COUNT(*) AS VISITOR_DAYS,
                COUNT(DISTINCT clean_maid) AS UNIQUE_VISITORS,
                ROUND(AVG(pageviews), 2) AS AVG_PAGEVIEWS,
                APPROX_PERCENTILE(pageviews, 0.50) AS P50_PAGES,
                APPROX_PERCENTILE(pageviews, 0.90) AS P90_PAGES,
                SUM(is_ctv) AS CTV_OVERLAP,
                ROUND(SUM(is_ctv)::FLOAT / NULLIF(COUNT(*), 0) * 100, 1) AS CTV_OVERLAP_PCT,
                ROUND(AVG(CASE WHEN is_ctv = 1 THEN pageviews END), 2) AS AVG_PAGES_CTV,
                ROUND(AVG(CASE WHEN is_ctv = 0 THEN pageviews END), 2) AS AVG_PAGES_NON_CTV
            FROM classified
            GROUP BY 1 HAVING COUNT(*) >= 10
            
            UNION ALL
            
            -- CTV View-Through row
            SELECT 'Paramount CTV' AS traffic_source, 'ctv' AS SOURCE_TYPE,
                COUNT(*) AS VISITOR_DAYS,
                COUNT(DISTINCT clean_maid) AS UNIQUE_VISITORS,
                ROUND(AVG(pageviews), 2) AS AVG_PAGEVIEWS,
                APPROX_PERCENTILE(pageviews, 0.50) AS P50_PAGES,
                APPROX_PERCENTILE(pageviews, 0.90) AS P90_PAGES,
                0 AS CTV_OVERLAP, 0 AS CTV_OVERLAP_PCT,
                NULL AS AVG_PAGES_CTV, NULL AS AVG_PAGES_NON_CTV
            FROM classified WHERE is_ctv = 1
            
            ORDER BY VISITOR_DAYS DESC
        """
        
        cursor.execute(query, {
            'advertiser_id': str(advertiser_id),
            'advertiser_id_int': int(advertiser_id),
            'start_date': start_date,
            'end_date': end_date
        })
        
        columns = [desc[0] for desc in cursor.description]
        results = []
        for row in cursor.fetchall():
            d = dict(zip(columns, row))
            # Convert Decimal types to float/int for JSON serialization
            for k, v in d.items():
                if hasattr(v, 'is_integer'):
                    d[k] = int(v) if v == int(v) else float(v)
                elif v is None:
                    d[k] = None
            results.append(d)
        
        cursor.close()
        conn.close()
        return jsonify({'success': True, 'data': results})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

# =============================================================================
# AGENCY TIMESERIES (for overview chart - stacked bar by agency)
# =============================================================================

@app.route('/api/v5/agency-timeseries', methods=['GET'])
def get_agency_timeseries():
    """Weekly impression timeseries broken out by agency for stacked bar chart."""
    try:
        start_date, end_date = get_date_range()
        conn = get_snowflake_connection()
        cursor = conn.cursor()
        
        # Class B: already weekly, use LOG_DATE as week anchor
        cursor.execute("""
            SELECT LOG_DATE::DATE as DT, AGENCY_ID, SUM(IMPRESSIONS) as IMPRESSIONS
            FROM QUORUMDB.SEGMENT_DATA.CAMPAIGN_PERFORMANCE_REPORT_WEEKLY_STATS
            WHERE LOG_DATE BETWEEN %(start_date)s AND %(end_date)s
            GROUP BY LOG_DATE::DATE, AGENCY_ID HAVING SUM(IMPRESSIONS) > 0
        """, {'start_date': start_date, 'end_date': end_date})
        rows_b = cursor.fetchall()
        
        # Paramount: daily data, bucket into same weekly anchors as Class B
        # Find the Class B week dates to align
        week_dates = sorted(set(str(r[0]) for r in rows_b))
        
        cursor.execute("""
            SELECT DATE::DATE as DT, 1480 as AGENCY_ID, SUM(IMPRESSIONS) as IMPRESSIONS
            FROM QUORUMDB.SEGMENT_DATA.PARAMOUNT_DASHBOARD_SUMMARY_STATS
            WHERE DATE BETWEEN %(start_date)s AND %(end_date)s
            GROUP BY DATE::DATE HAVING SUM(IMPRESSIONS) > 0
        """, {'start_date': start_date, 'end_date': end_date})
        rows_p_daily = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        # Bucket Paramount daily data into the same weeks as Class B
        # Each week bucket = the Class B LOG_DATE that covers that period
        from datetime import datetime, timedelta
        
        def find_week_bucket(dt_str, anchors):
            """Assign a daily date to its nearest weekly anchor (closest future anchor)."""
            if not anchors:
                return dt_str
            dt = datetime.strptime(dt_str, '%Y-%m-%d').date() if isinstance(dt_str, str) else dt_str
            anchor_dates = sorted([datetime.strptime(a, '%Y-%m-%d').date() if isinstance(a, str) else a for a in anchors])
            # Find the anchor that this date falls before (or the last one)
            for anchor in anchor_dates:
                if dt <= anchor:
                    return str(anchor)
            return str(anchor_dates[-1])
        
        # Build Paramount weekly rows
        rows_p = []
        if week_dates:
            para_weekly = {}
            for dt, aid, imps in rows_p_daily:
                bucket = find_week_bucket(str(dt), week_dates)
                key = (bucket, int(aid))
                para_weekly[key] = para_weekly.get(key, 0) + int(imps)
            for (bucket, aid), imps in para_weekly.items():
                rows_p.append((bucket, aid, imps))
        else:
            # No Class B data, just use Paramount weekly buckets
            para_weekly = {}
            for dt, aid, imps in rows_p_daily:
                from datetime import date as date_type
                d = dt if isinstance(dt, date_type) else datetime.strptime(str(dt), '%Y-%m-%d').date()
                # Use Monday as week start
                monday = d - timedelta(days=d.weekday())
                key = (str(monday), int(aid))
                para_weekly[key] = para_weekly.get(key, 0) + int(imps)
            for (bucket, aid), imps in para_weekly.items():
                rows_p.append((bucket, aid, imps))
        
        data = {}
        for dt, agency_id, imps in rows_b + rows_p:
            dt_str = str(dt)
            if dt_str not in data: data[dt_str] = {}
            aid = int(agency_id)
            data[dt_str][aid] = int(imps) + data[dt_str].get(aid, 0)
        
        agencies = {}
        for agency_map in data.values():
            for aid in agency_map:
                if aid not in agencies:
                    agencies[aid] = get_agency_name(aid)
        
        return jsonify({'success': True, 'data': data, 'agencies': agencies})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

# =============================================================================
# ADVERTISER TIMESERIES (for per-agency chart - stacked bar by advertiser)
# =============================================================================

@app.route('/api/v5/advertiser-timeseries', methods=['GET'])
def get_advertiser_timeseries():
    """Daily impression timeseries for an agency, broken out by advertiser."""
    try:
        agency_id = request.args.get('agency_id')
        if not agency_id:
            return jsonify({'success': False, 'error': 'agency_id required'}), 400
        
        agency_id = int(agency_id)
        start_date, end_date = get_date_range()
        conn = get_snowflake_connection()
        cursor = conn.cursor()
        
        if agency_id == 1480:
            cursor.execute("""
                WITH daily AS (
                    SELECT DATE::DATE as DT, QUORUM_ADVERTISER_ID as AID,
                           MAX(ADVERTISER_NAME) as ANAME, SUM(IMPRESSIONS) as IMPS
                    FROM QUORUMDB.SEGMENT_DATA.PARAMOUNT_DASHBOARD_SUMMARY_STATS
                    WHERE DATE BETWEEN %(start_date)s AND %(end_date)s
                    GROUP BY DATE::DATE, QUORUM_ADVERTISER_ID HAVING SUM(IMPRESSIONS) > 0
                ),
                ranked AS (
                    SELECT AID, SUM(IMPS) as TOTAL_IMPS
                    FROM daily GROUP BY AID
                    ORDER BY TOTAL_IMPS DESC
                    LIMIT 15
                )
                SELECT d.DT, 
                       CASE WHEN r.AID IS NOT NULL THEN d.AID ELSE -1 END as ADVERTISER_ID,
                       CASE WHEN r.AID IS NOT NULL THEN d.ANAME ELSE 'Other' END as ADVERTISER_NAME,
                       SUM(d.IMPS) as IMPRESSIONS
                FROM daily d
                LEFT JOIN ranked r ON d.AID = r.AID
                GROUP BY d.DT, 
                         CASE WHEN r.AID IS NOT NULL THEN d.AID ELSE -1 END,
                         CASE WHEN r.AID IS NOT NULL THEN d.ANAME ELSE 'Other' END
            """, {'start_date': start_date, 'end_date': end_date})
        else:
            cursor.execute("""
                SELECT LOG_DATE::DATE as DT, w.ADVERTISER_ID,
                       COALESCE(MAX(aa.COMP_NAME), 'Advertiser ' || w.ADVERTISER_ID) as ADVERTISER_NAME,
                       SUM(w.IMPRESSIONS) as IMPRESSIONS
                FROM QUORUMDB.SEGMENT_DATA.CAMPAIGN_PERFORMANCE_REPORT_WEEKLY_STATS w
                LEFT JOIN QUORUMDB.SEGMENT_DATA.AGENCY_ADVERTISER aa ON w.ADVERTISER_ID = aa.ID
                WHERE w.AGENCY_ID = %(agency_id)s AND w.LOG_DATE BETWEEN %(start_date)s AND %(end_date)s
                GROUP BY LOG_DATE::DATE, w.ADVERTISER_ID HAVING SUM(w.IMPRESSIONS) > 0
            """, {'agency_id': agency_id, 'start_date': start_date, 'end_date': end_date})
        
        rows = cursor.fetchall()
        cursor.close()
        conn.close()
        
        data = {}
        advertisers = {}
        for dt, adv_id, adv_name, imps in rows:
            dt_str = str(dt)
            adv_id = int(adv_id)
            if dt_str not in data: data[dt_str] = {}
            data[dt_str][adv_id] = int(imps) + data[dt_str].get(adv_id, 0)
            if adv_id not in advertisers:
                name = adv_name or f'Advertiser {adv_id}'
                if agency_id == 1480:
                    name = re.sub(r'^[0-9A-Za-z]+ - ', '', name)
                advertisers[adv_id] = name
        
        return jsonify({'success': True, 'data': data, 'advertisers': advertisers})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

# =============================================================================
# MAIN
# =============================================================================

# =============================================================================
# OPTIMIZE RECOMMENDATIONS (Paramount only)
# =============================================================================

@app.route('/api/v5/optimize', methods=['GET'])
def get_optimize():
    """Pull non-geo performance data for optimization (no JOIN - fast).
    Baseline window: 30 days ending 5 days back."""
    advertiser_id = request.args.get('advertiser_id')
    
    if not advertiser_id:
        return jsonify({'success': False, 'error': 'advertiser_id parameter required'}), 400
    
    try:
        conn = get_snowflake_connection()
        cursor = conn.cursor()
        
        date_filter = "IMP_DATE BETWEEN DATEADD(day, -35, CURRENT_DATE) AND DATEADD(day, -5, CURRENT_DATE)"
        adv_filter = "QUORUM_ADVERTISER_ID = %(adv_id)s"
        web_expr = "SUM(CASE WHEN IS_SITE_VISIT = 'TRUE' THEN 1 ELSE 0 END)"
        store_expr = "SUM(CASE WHEN IS_STORE_VISIT = 'TRUE' THEN 1 ELSE 0 END)"
        web_vr = f"ROUND({web_expr}*100.0/NULLIF(COUNT(*),0), 4)"
        store_vr = f"ROUND({store_expr}*100.0/NULLIF(COUNT(*),0), 4)"
        
        q1 = f"""
            SELECT 'baseline' as DIM_TYPE, 'overall' as DIM_KEY, NULL as DIM_NAME,
                COUNT(*) as IMPS, {web_expr} as WEB_VISITS, {store_expr} as STORE_VISITS,
                {web_vr} as WEB_VR, {store_vr} as STORE_VR
            FROM QUORUMDB.SEGMENT_DATA.PARAMOUNT_IMPRESSIONS_REPORT_90_DAYS
            WHERE {adv_filter} AND {date_filter}

            UNION ALL
            SELECT 'campaign', IO_ID::VARCHAR, MAX(IO_NAME), COUNT(*), {web_expr}, {store_expr}, {web_vr}, {store_vr}
            FROM QUORUMDB.SEGMENT_DATA.PARAMOUNT_IMPRESSIONS_REPORT_90_DAYS
            WHERE {adv_filter} AND {date_filter} GROUP BY IO_ID

            UNION ALL
            SELECT 'lineitem', LINEITEM_ID::VARCHAR, MAX(LINEITEM_NAME), COUNT(*), {web_expr}, {store_expr}, {web_vr}, {store_vr}
            FROM QUORUMDB.SEGMENT_DATA.PARAMOUNT_IMPRESSIONS_REPORT_90_DAYS
            WHERE {adv_filter} AND {date_filter} GROUP BY LINEITEM_ID

            UNION ALL
            SELECT 'creative', CREATIVE_NAME, NULL, COUNT(*), {web_expr}, {store_expr}, {web_vr}, {store_vr}
            FROM QUORUMDB.SEGMENT_DATA.PARAMOUNT_IMPRESSIONS_REPORT_90_DAYS
            WHERE {adv_filter} AND {date_filter} GROUP BY CREATIVE_NAME

            UNION ALL
            SELECT 'dow', DAYOFWEEK(IMP_DATE)::VARCHAR, NULL, COUNT(*), {web_expr}, {store_expr}, {web_vr}, {store_vr}
            FROM QUORUMDB.SEGMENT_DATA.PARAMOUNT_IMPRESSIONS_REPORT_90_DAYS
            WHERE {adv_filter} AND {date_filter} GROUP BY DAYOFWEEK(IMP_DATE)

            UNION ALL
            SELECT 'site', SITE, NULL, COUNT(*), {web_expr}, {store_expr}, {web_vr}, {store_vr}
            FROM QUORUMDB.SEGMENT_DATA.PARAMOUNT_IMPRESSIONS_REPORT_90_DAYS
            WHERE {adv_filter} AND {date_filter} GROUP BY SITE HAVING COUNT(*) >= 2000

            ORDER BY 1, 4 DESC
        """
        
        cursor.execute(q1, {'adv_id': int(advertiser_id)})
        columns = [desc[0] for desc in cursor.description]
        results = []
        for row in cursor.fetchall():
            d = dict(zip(columns, row))
            for k, v in d.items():
                if hasattr(v, 'is_integer'):
                    d[k] = float(v) if v else 0
            results.append(d)
        
        cursor.close()
        conn.close()
        return jsonify({'success': True, 'data': results})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/v5/optimize-geo', methods=['GET'])
def get_optimize_geo():
    """Pull geo dimensions (DMA + ZIP) separately — requires JOIN so slower."""
    advertiser_id = request.args.get('advertiser_id')
    
    if not advertiser_id:
        return jsonify({'success': False, 'error': 'advertiser_id parameter required'}), 400
    
    try:
        conn = get_snowflake_connection()
        cursor = conn.cursor()
        
        date_filter = "IMP_DATE BETWEEN DATEADD(day, -35, CURRENT_DATE) AND DATEADD(day, -5, CURRENT_DATE)"
        adv_filter = "QUORUM_ADVERTISER_ID = %(adv_id)s"
        web_expr = "SUM(CASE WHEN i.IS_SITE_VISIT = 'TRUE' THEN 1 ELSE 0 END)"
        store_expr = "SUM(CASE WHEN i.IS_STORE_VISIT = 'TRUE' THEN 1 ELSE 0 END)"
        web_vr = f"ROUND({web_expr}*100.0/NULLIF(COUNT(*),0), 4)"
        store_vr = f"ROUND({store_expr}*100.0/NULLIF(COUNT(*),0), 4)"
        
        q2 = f"""
            SELECT 'dma' as DIM_TYPE, z.DMA_CODE as DIM_KEY, MAX(z.DMA_NAME) as DIM_NAME,
                COUNT(*) as IMPS, {web_expr} as WEB_VISITS, {store_expr} as STORE_VISITS,
                {web_vr} as WEB_VR, {store_vr} as STORE_VR
            FROM QUORUMDB.SEGMENT_DATA.PARAMOUNT_IMPRESSIONS_REPORT_90_DAYS i
            JOIN QUORUMDB.SEGMENT_DATA.ZIP_DMA_MAPPING z ON i.ZIP_CODE = z.ZIP_CODE
            WHERE i.{adv_filter} AND i.{date_filter}
            GROUP BY z.DMA_CODE HAVING COUNT(*) >= 2000

            UNION ALL
            SELECT 'zip', i.ZIP_CODE, MAX(z.DMA_NAME), COUNT(*), {web_expr}, {store_expr}, {web_vr}, {store_vr}
            FROM QUORUMDB.SEGMENT_DATA.PARAMOUNT_IMPRESSIONS_REPORT_90_DAYS i
            JOIN QUORUMDB.SEGMENT_DATA.ZIP_DMA_MAPPING z ON i.ZIP_CODE = z.ZIP_CODE
            WHERE i.{adv_filter} AND i.{date_filter}
            GROUP BY i.ZIP_CODE HAVING COUNT(*) >= 200

            ORDER BY 1, 4 DESC
        """
        
        cursor.execute(q2, {'adv_id': int(advertiser_id)})
        columns = [desc[0] for desc in cursor.description]
        results = []
        for row in cursor.fetchall():
            d = dict(zip(columns, row))
            for k, v in d.items():
                if hasattr(v, 'is_integer'):
                    d[k] = float(v) if v else 0
            results.append(d)
        
        cursor.close()
        conn.close()
        return jsonify({'success': True, 'data': results})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8080))
    app.run(host='0.0.0.0', port=port, debug=False)
