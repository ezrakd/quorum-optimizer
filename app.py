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

def get_snowflake_connection():
    return snowflake.connector.connect(
        user=os.environ.get('SNOWFLAKE_USER'),
        password=os.environ.get('SNOWFLAKE_PASSWORD'),
        account=os.environ.get('SNOWFLAKE_ACCOUNT'),
        warehouse=os.environ.get('SNOWFLAKE_WAREHOUSE', 'COMPUTE_WH'),
        database=os.environ.get('SNOWFLAKE_DATABASE', 'QUORUMDB'),
        schema=os.environ.get('SNOWFLAKE_SCHEMA', 'SEGMENT_DATA'),
        role=os.environ.get('SNOWFLAKE_ROLE', 'OPTIMIZER_READONLY_ROLE')
    )

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
        'version': '5.2-lift',
        'description': 'Pre-aggregated tables with unified lift analysis',
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
            '/api/v5/traffic-sources (Paramount only)'
        ]
    })

# =============================================================================
# AGENCY OVERVIEW
# =============================================================================

@app.route('/api/v5/agencies', methods=['GET'])
def get_agencies():
    """
    Get all agencies with aggregated metrics.
    Uses CAMPAIGN_PERFORMANCE_REPORT_WEEKLY_STATS for Class B,
    PARAMOUNT_DASHBOARD_SUMMARY_STATS for Paramount.
    """
    try:
        start_date, end_date = get_date_range()
        conn = get_snowflake_connection()
        cursor = conn.cursor()
        
        all_results = []
        
        # Query 1: Class B agencies from CAMPAIGN_PERFORMANCE_REPORT_WEEKLY_STATS
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
        
        # Query 2: Paramount from PARAMOUNT_DASHBOARD_SUMMARY_STATS (deduplicated)
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
        
        # Sort by impressions descending
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
    """
    Get advertisers for a specific agency.
    """
    try:
        agency_id = request.args.get('agency_id')
        if not agency_id:
            return jsonify({'success': False, 'error': 'agency_id required'}), 400
        
        agency_id = int(agency_id)
        start_date, end_date = get_date_range()
        conn = get_snowflake_connection()
        cursor = conn.cursor()
        
        if agency_id == 1480:
            # Paramount - use PARAMOUNT_DASHBOARD_SUMMARY_STATS (deduplicated)
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
            # Class B - use CAMPAIGN_PERFORMANCE_REPORT_WEEKLY_STATS
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
        
        # Clean up advertiser names for Paramount (remove prefix IDs)
        if agency_id == 1480:
            import re
            for r in results:
                if r.get('ADVERTISER_NAME'):
                    # Remove patterns like "949515 - " or "001Kb00001Jy9wJIAR - "
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
    """
    Get campaign/IO level performance for an advertiser.
    """
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
            # Paramount - use PARAMOUNT_IMPRESSIONS_REPORT_90_DAYS for everything
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
            # Class B - use CAMPAIGN_PERFORMANCE_REPORT_WEEKLY_STATS
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
    """
    Get line item level performance for an advertiser.
    """
    try:
        agency_id = request.args.get('agency_id')
        advertiser_id = request.args.get('advertiser_id')
        campaign_id = request.args.get('campaign_id')  # Optional IO filter
        
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
            # Paramount - aggregate from PARAMOUNT_IMPRESSIONS_REPORT_90_DAYS
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
            # Class B - use CAMPAIGN_PERFORMANCE_REPORT_WEEKLY_STATS with PT lookup
            query = f"""
                WITH lineitem_stats AS (
                    SELECT 
                        LI_ID,
                        MAX(LI_NAME) as LI_NAME,
                        MAX(IO_ID) as IO_ID,
                        MAX(IO_NAME) as IO_NAME,
                        SUM(IMPRESSIONS) as IMPRESSIONS,
                        SUM(VISITORS) as STORE_VISITS,
                        0 as WEB_VISITS
                    FROM QUORUMDB.SEGMENT_DATA.CAMPAIGN_PERFORMANCE_REPORT_WEEKLY_STATS
                    WHERE AGENCY_ID = %(agency_id)s
                      AND ADVERTISER_ID = %(advertiser_id)s
                      AND LOG_DATE BETWEEN %(start_date)s AND %(end_date)s
                      {campaign_filter}
                    GROUP BY LI_ID
                    HAVING SUM(IMPRESSIONS) >= 100 OR SUM(VISITORS) >= 10
                ),
                lineitem_pt AS (
                    SELECT 
                        LINEITEM_ID,
                        PT,
                        COUNT(*) as cnt,
                        ROW_NUMBER() OVER (PARTITION BY LINEITEM_ID ORDER BY COUNT(*) DESC) as rn
                    FROM QUORUMDB.SEGMENT_DATA.XANDR_IMPRESSION_LOG
                    WHERE AGENCY_ID = %(agency_id)s
                      AND TIMESTAMP::DATE BETWEEN %(start_date)s AND %(end_date)s
                    GROUP BY LINEITEM_ID, PT
                )
                SELECT 
                    ls.LI_ID,
                    ls.LI_NAME,
                    ls.IO_ID,
                    ls.IO_NAME,
                    ls.IMPRESSIONS,
                    ls.STORE_VISITS,
                    ls.WEB_VISITS,
                    COALESCE(p.PLATFORM, 'PT=' || COALESCE(lp.PT::VARCHAR, '?')) as PLATFORM
                FROM lineitem_stats ls
                LEFT JOIN lineitem_pt lp ON ls.LI_ID = lp.LINEITEM_ID AND lp.rn = 1
                LEFT JOIN QUORUMDB.SEGMENT_DATA.PT_TO_PLATFORM p ON lp.PT = p.PT
                ORDER BY ls.IMPRESSIONS DESC
                LIMIT 100
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
# PUBLISHER PERFORMANCE
# =============================================================================

@app.route('/api/v5/publisher-performance', methods=['GET'])
def get_publisher_performance():
    """
    Get publisher level performance for an advertiser.
    """
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
        
        # Build filters for Paramount
        paramount_filters = ""
        if campaign_id:
            paramount_filters += f" AND IO_ID = '{campaign_id}'"
        if lineitem_id:
            paramount_filters += f" AND LINEITEM_ID = '{lineitem_id}'"
        
        # Build filters for Class B
        classb_filters = ""
        if campaign_id:
            classb_filters += f" AND IO_ID = '{campaign_id}'"
        if lineitem_id:
            classb_filters += f" AND LI_ID = '{lineitem_id}'"
        
        if agency_id == 1480:
            # Paramount - use PARAMOUNT_IMPRESSIONS_REPORT_90_DAYS
            query = f"""
                SELECT 
                    SITE as PUBLISHER,
                    COUNT(*) as IMPRESSIONS,
                    COUNT(DISTINCT CASE WHEN IS_STORE_VISIT = 'TRUE' THEN IMP_MAID END) as STORE_VISITS,
                    COUNT(DISTINCT CASE WHEN IS_SITE_VISIT = 'TRUE' THEN IMP_MAID END) as WEB_VISITS
                FROM QUORUMDB.SEGMENT_DATA.PARAMOUNT_IMPRESSIONS_REPORT_90_DAYS
                WHERE QUORUM_ADVERTISER_ID = %(advertiser_id)s
                  AND IMP_DATE BETWEEN %(start_date)s AND %(end_date)s
                  {paramount_filters}
                GROUP BY SITE
                HAVING COUNT(*) >= 100
                ORDER BY 2 DESC
                LIMIT 50
            """
        else:
            # Class B - use PUBLISHER column from WEEKLY_STATS
            query = f"""
                SELECT 
                    PUBLISHER,
                    SUM(IMPRESSIONS) as IMPRESSIONS,
                    SUM(VISITORS) as STORE_VISITS,
                    0 as WEB_VISITS
                FROM QUORUMDB.SEGMENT_DATA.CAMPAIGN_PERFORMANCE_REPORT_WEEKLY_STATS
                WHERE AGENCY_ID = %(agency_id)s
                  AND ADVERTISER_ID = %(advertiser_id)s
                  AND LOG_DATE BETWEEN %(start_date)s AND %(end_date)s
                  {classb_filters}
                GROUP BY PUBLISHER
                HAVING SUM(IMPRESSIONS) >= 100
                ORDER BY 2 DESC
                LIMIT 50
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
# GEOGRAPHIC / ZIP PERFORMANCE
# Uses CAMPAIGN_POSTAL_REPORTING - full history, no date filter
# =============================================================================

@app.route('/api/v5/zip-performance', methods=['GET'])
def get_zip_performance():
    """
    Get geographic/ZIP code performance for an advertiser.
    Paramount: uses PARAMOUNT_IMPRESSIONS_REPORT_90_DAYS (date filtered)
    Class B: uses CAMPAIGN_POSTAL_REPORTING (full history, no date filter)
    """
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
            # Paramount - use PARAMOUNT_IMPRESSIONS_REPORT_90_DAYS with ZIP_CODE
            start_date, end_date = get_date_range()
            
            filters = ""
            if campaign_id:
                filters += f" AND IO_ID = '{campaign_id}'"
            if lineitem_id:
                filters += f" AND LINEITEM_ID = '{lineitem_id}'"
            
            query = f"""
                SELECT 
                    ZIP_CODE,
                    COUNT(*) as IMPRESSIONS,
                    COUNT(DISTINCT CASE WHEN IS_STORE_VISIT = 'TRUE' THEN IMP_MAID END) as STORE_VISITS,
                    COUNT(DISTINCT CASE WHEN IS_SITE_VISIT = 'TRUE' THEN IMP_MAID END) as WEB_VISITS
                FROM QUORUMDB.SEGMENT_DATA.PARAMOUNT_IMPRESSIONS_REPORT_90_DAYS
                WHERE QUORUM_ADVERTISER_ID = %(advertiser_id)s
                  AND IMP_DATE BETWEEN %(start_date)s AND %(end_date)s
                  AND ZIP_CODE IS NOT NULL 
                  AND ZIP_CODE != ''
                  AND ZIP_CODE != 'null'
                  AND ZIP_CODE != 'UNKNOWN'
                  {filters}
                GROUP BY ZIP_CODE
                HAVING COUNT(*) >= 100
                ORDER BY 3 DESC, 2 DESC
                LIMIT 100
            """
            
            cursor.execute(query, {
                'advertiser_id': advertiser_id,
                'start_date': start_date,
                'end_date': end_date
            })
            note = 'Date filtered (matches date selector)'
        else:
            # Class B - use CAMPAIGN_POSTAL_REPORTING (full history)
            filters = ""
            if campaign_id:
                filters += f" AND CAMPAIGN_ID = {campaign_id}"
            if lineitem_id:
                filters += f" AND LINEITEM_ID = '{lineitem_id}'"
            
            query = f"""
                SELECT 
                    USER_HOME_POSTAL_CODE as ZIP_CODE,
                    SUM(IMPRESSIONS) as IMPRESSIONS,
                    SUM(STORE_VISITS) as STORE_VISITS,
                    0 as WEB_VISITS
                FROM QUORUMDB.SEGMENT_DATA.CAMPAIGN_POSTAL_REPORTING
                WHERE AGENCY_ID = %(agency_id)s
                  AND ADVERTISER_ID = %(advertiser_id)s
                  AND USER_HOME_POSTAL_CODE IS NOT NULL 
                  AND USER_HOME_POSTAL_CODE != ''
                  AND USER_HOME_POSTAL_CODE != 'null'
                  AND USER_HOME_POSTAL_CODE != 'UNKNOWN'
                  {filters}
                GROUP BY USER_HOME_POSTAL_CODE
                HAVING SUM(IMPRESSIONS) >= 100 OR SUM(STORE_VISITS) >= 1
                ORDER BY 3 DESC, 2 DESC
                LIMIT 100
            """
            
            cursor.execute(query, {
                'agency_id': agency_id,
                'advertiser_id': advertiser_id
            })
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
# Uses CAMPAIGN_PERFORMANCE_REPORT_WEEKLY_STATS (has DMA column)
# =============================================================================

@app.route('/api/v5/dma-performance', methods=['GET'])
def get_dma_performance():
    """
    Get DMA level performance for an advertiser.
    Paramount: derives DMA from ZIP_CODE using DBIP_LOOKUP_US reference
    Class B: uses DMA column from CAMPAIGN_PERFORMANCE_REPORT_WEEKLY_STATS
    """
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
            # Paramount - derive DMA from ZIP using DBIP_LOOKUP_US reference table
            filters = ""
            if campaign_id:
                filters += f" AND p.IO_ID = '{campaign_id}'"
            if lineitem_id:
                filters += f" AND p.LINEITEM_ID = '{lineitem_id}'"
            
            query = f"""
                WITH zip_dma AS (
                    SELECT ZIPCODE, MAX(DMA_NAME) as DMA_NAME 
                    FROM QUORUMDB.SEGMENT_DATA.DBIP_LOOKUP_US 
                    WHERE DMA_NAME IS NOT NULL AND DMA_NAME != ''
                    GROUP BY ZIPCODE
                )
                SELECT 
                    d.DMA_NAME as DMA,
                    COUNT(*) as IMPRESSIONS,
                    COUNT(DISTINCT CASE WHEN p.IS_STORE_VISIT = 'TRUE' THEN p.IMP_MAID END) as STORE_VISITS,
                    COUNT(DISTINCT CASE WHEN p.IS_SITE_VISIT = 'TRUE' THEN p.IMP_MAID END) as WEB_VISITS
                FROM QUORUMDB.SEGMENT_DATA.PARAMOUNT_IMPRESSIONS_REPORT_90_DAYS p
                JOIN zip_dma d ON p.ZIP_CODE = d.ZIPCODE
                WHERE p.QUORUM_ADVERTISER_ID = %(advertiser_id)s
                  AND p.IMP_DATE BETWEEN %(start_date)s AND %(end_date)s
                  {filters}
                GROUP BY d.DMA_NAME
                HAVING COUNT(*) >= 100
                ORDER BY 2 DESC
                LIMIT 50
            """
            
            cursor.execute(query, {
                'advertiser_id': advertiser_id,
                'start_date': start_date,
                'end_date': end_date
            })
        else:
            # Class B - use DMA column from WEEKLY_STATS
            filters = ""
            if campaign_id:
                filters += f" AND IO_ID = '{campaign_id}'"
            if lineitem_id:
                filters += f" AND LI_ID = '{lineitem_id}'"
            
            query = f"""
                SELECT 
                    DMA,
                    SUM(IMPRESSIONS) as IMPRESSIONS,
                    SUM(VISITORS) as STORE_VISITS,
                    0 as WEB_VISITS
                FROM QUORUMDB.SEGMENT_DATA.CAMPAIGN_PERFORMANCE_REPORT_WEEKLY_STATS
                WHERE AGENCY_ID = %(agency_id)s
                  AND ADVERTISER_ID = %(advertiser_id)s
                  AND LOG_DATE BETWEEN %(start_date)s AND %(end_date)s
                  AND DMA IS NOT NULL AND DMA != ''
                  {filters}
                GROUP BY DMA
                HAVING SUM(IMPRESSIONS) >= 100
                ORDER BY 2 DESC
                LIMIT 50
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
# SUMMARY ENDPOINT
# =============================================================================

@app.route('/api/v5/summary', methods=['GET'])
def get_summary():
    """
    Get summary metrics for an advertiser.
    """
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
            # Paramount
            query = """
                SELECT 
                    SUM(IMPRESSIONS) as IMPRESSIONS,
                    SUM(STORE_VISITS) as STORE_VISITS,
                    SUM(SITE_VISITS) as WEB_VISITS,
                    MIN(DATE) as MIN_DATE,
                    MAX(DATE) as MAX_DATE
                FROM QUORUMDB.SEGMENT_DATA.PARAMOUNT_DASHBOARD_SUMMARY_STATS
                WHERE QUORUM_ADVERTISER_ID = %(advertiser_id)s
                  AND DATE BETWEEN %(start_date)s AND %(end_date)s
            """
        else:
            # Class B
            query = """
                SELECT 
                    SUM(IMPRESSIONS) as IMPRESSIONS,
                    SUM(VISITORS) as STORE_VISITS,
                    0 as WEB_VISITS,
                    MIN(LOG_DATE) as MIN_DATE,
                    MAX(LOG_DATE) as MAX_DATE,
                    COUNT(DISTINCT IO_ID) as CAMPAIGN_COUNT,
                    COUNT(DISTINCT LI_ID) as LINEITEM_COUNT
                FROM QUORUMDB.SEGMENT_DATA.CAMPAIGN_PERFORMANCE_REPORT_WEEKLY_STATS
                WHERE AGENCY_ID = %(agency_id)s
                  AND ADVERTISER_ID = %(advertiser_id)s
                  AND LOG_DATE BETWEEN %(start_date)s AND %(end_date)s
            """
        
        cursor.execute(query, {
            'agency_id': agency_id,
            'advertiser_id': advertiser_id,
            'start_date': start_date,
            'end_date': end_date
        })
        
        columns = [desc[0] for desc in cursor.description]
        row = cursor.fetchone()
        result = dict(zip(columns, row)) if row else {}
        
        # Calculate visit rate
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
    """
    Get daily timeseries data for charts.
    """
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
            # Paramount
            query = """
                SELECT 
                    DATE as LOG_DATE,
                    IMPRESSIONS,
                    STORE_VISITS,
                    SITE_VISITS as WEB_VISITS
                FROM QUORUMDB.SEGMENT_DATA.PARAMOUNT_DASHBOARD_SUMMARY_STATS
                WHERE QUORUM_ADVERTISER_ID = %(advertiser_id)s
                  AND DATE BETWEEN %(start_date)s AND %(end_date)s
                ORDER BY DATE
            """
        else:
            # Class B - aggregate by day
            query = """
                SELECT 
                    LOG_DATE,
                    SUM(IMPRESSIONS) as IMPRESSIONS,
                    SUM(VISITORS) as STORE_VISITS,
                    0 as WEB_VISITS
                FROM QUORUMDB.SEGMENT_DATA.CAMPAIGN_PERFORMANCE_REPORT_WEEKLY_STATS
                WHERE AGENCY_ID = %(agency_id)s
                  AND ADVERTISER_ID = %(advertiser_id)s
                  AND LOG_DATE BETWEEN %(start_date)s AND %(end_date)s
                GROUP BY LOG_DATE
                ORDER BY LOG_DATE
            """
        
        cursor.execute(query, {
            'agency_id': agency_id,
            'advertiser_id': advertiser_id,
            'start_date': start_date,
            'end_date': end_date
        })
        
        columns = [desc[0] for desc in cursor.description]
        results = []
        for row in cursor.fetchall():
            d = dict(zip(columns, row))
            # Convert date to string for JSON
            if d.get('LOG_DATE'):
                d['LOG_DATE'] = str(d['LOG_DATE'])
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
    """
    Unified lift analysis with auto-detection of visit type.
    
    Lift methodology:
    - Visit Rate = VISITORS / REACH × 100
    - Baseline = Overall average visit rate for the advertiser
    - Lift % = (Visit Rate - Baseline) / Baseline × 100
    - Index = Visit Rate / Baseline × 100 (100 = average performance)
    
    Data Sources:
    - Paramount (1480): PARAMOUNT_IMPRESSIONS_REPORT_90_DAYS (web visits via IS_SITE_VISIT)
    - All Others: CAMPAIGN_PERFORMANCE_REPORT_WEEKLY_STATS (store visits)
    
    Parameters:
    - agency_id: Required
    - advertiser_id: Required
    - group_by: 'campaign' (default) or 'lineitem'
    """
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
            # =================================================================
            # PARAMOUNT: Web-based lift from PARAMOUNT_IMPRESSIONS_REPORT_90_DAYS
            # =================================================================
            if group_by == 'lineitem':
                group_cols = "IO_ID, LINEITEM_ID"
                name_cols = """
                    COALESCE(MAX(LINEITEM_NAME), 'LI-' || LINEITEM_ID::VARCHAR) as NAME,
                    COALESCE(MAX(IO_NAME), 'IO-' || IO_ID::VARCHAR) as PARENT_NAME,
                    LINEITEM_ID as ID,
                    IO_ID as PARENT_ID,
                """
            else:  # campaign
                group_cols = "IO_ID"
                name_cols = """
                    COALESCE(MAX(IO_NAME), 'IO-' || IO_ID::VARCHAR) as NAME,
                    NULL as PARENT_NAME,
                    IO_ID as ID,
                    NULL as PARENT_ID,
                """
            
            query = f"""
                WITH campaign_metrics AS (
                    SELECT 
                        {group_cols},
                        {name_cols}
                        COUNT(*) as IMPRESSIONS,
                        COUNT(DISTINCT IMP_MAID) as REACH,
                        COUNT(DISTINCT CASE WHEN IS_SITE_VISIT = 'TRUE' THEN IMP_MAID END) as VISITORS
                    FROM QUORUMDB.SEGMENT_DATA.PARAMOUNT_IMPRESSIONS_REPORT_90_DAYS
                    WHERE QUORUM_ADVERTISER_ID::INT = %(advertiser_id)s
                      AND IMP_DATE BETWEEN %(start_date)s AND %(end_date)s
                    GROUP BY {group_cols}
                    HAVING COUNT(*) >= 1000
                ),
                baseline AS (
                    SELECT SUM(VISITORS)::FLOAT / NULLIF(SUM(REACH), 0) * 100 as BASELINE_VR
                    FROM campaign_metrics
                )
                SELECT 
                    c.NAME,
                    c.PARENT_NAME,
                    c.ID,
                    c.PARENT_ID,
                    c.IMPRESSIONS,
                    c.REACH,
                    c.REACH as PANEL_REACH,
                    c.VISITORS,
                    ROUND(c.VISITORS::FLOAT / NULLIF(c.REACH, 0) * 100, 4) as VISIT_RATE,
                    ROUND(b.BASELINE_VR, 4) as BASELINE_VR,
                    CASE 
                        WHEN b.BASELINE_VR > 0 
                        THEN ROUND(c.VISITORS::FLOAT / NULLIF(c.REACH, 0) * 100 / b.BASELINE_VR * 100, 1)
                        ELSE NULL 
                    END as INDEX_VS_AVG,
                    CASE 
                        WHEN b.BASELINE_VR > 0 
                        THEN ROUND((c.VISITORS::FLOAT / NULLIF(c.REACH, 0) * 100 - b.BASELINE_VR) / b.BASELINE_VR * 100, 1)
                        ELSE NULL 
                    END as LIFT_PCT
                FROM campaign_metrics c
                CROSS JOIN baseline b
                WHERE c.REACH >= 100
                ORDER BY c.IMPRESSIONS DESC
                LIMIT 100
            """
            
            cursor.execute(query, {
                'advertiser_id': int(advertiser_id),
                'start_date': start_date,
                'end_date': end_date
            })
            visit_type = 'web'
            
        else:
            # =================================================================
            # ALL OTHER AGENCIES: Use pre-aggregated CAMPAIGN_PERFORMANCE_REPORT_WEEKLY_STATS
            # =================================================================
            if group_by == 'lineitem':
                group_cols = "IO_ID, IO_NAME, LI_ID, LI_NAME"
                name_cols = """
                    LI_NAME as NAME,
                    IO_NAME as PARENT_NAME,
                    LI_ID as ID,
                    IO_ID as PARENT_ID,
                """
            else:  # campaign
                group_cols = "IO_ID, IO_NAME"
                name_cols = """
                    IO_NAME as NAME,
                    NULL as PARENT_NAME,
                    IO_ID as ID,
                    NULL as PARENT_ID,
                """
            
            query = f"""
                WITH campaign_metrics AS (
                    SELECT 
                        {group_cols},
                        {name_cols}
                        SUM(IMPRESSIONS) as IMPRESSIONS,
                        SUM(REACH) as REACH,
                        SUM(PANEL_REACH) as PANEL_REACH,
                        SUM(VISITORS) as VISITORS
                    FROM QUORUMDB.SEGMENT_DATA.CAMPAIGN_PERFORMANCE_REPORT_WEEKLY_STATS
                    WHERE ADVERTISER_ID = %(advertiser_id)s
                      AND LOG_DATE BETWEEN %(start_date)s AND %(end_date)s
                    GROUP BY {group_cols}
                    HAVING SUM(IMPRESSIONS) >= 1000
                ),
                baseline AS (
                    SELECT SUM(VISITORS)::FLOAT / NULLIF(SUM(PANEL_REACH), 0) * 100 as BASELINE_VR
                    FROM campaign_metrics
                )
                SELECT 
                    c.NAME,
                    c.PARENT_NAME,
                    c.ID,
                    c.PARENT_ID,
                    c.IMPRESSIONS,
                    c.REACH,
                    c.PANEL_REACH,
                    c.VISITORS,
                    ROUND(c.VISITORS::FLOAT / NULLIF(c.PANEL_REACH, 0) * 100, 4) as VISIT_RATE,
                    ROUND(b.BASELINE_VR, 4) as BASELINE_VR,
                    CASE 
                        WHEN b.BASELINE_VR > 0 
                        THEN ROUND(c.VISITORS::FLOAT / NULLIF(c.PANEL_REACH, 0) * 100 / b.BASELINE_VR * 100, 1)
                        ELSE NULL 
                    END as INDEX_VS_AVG,
                    CASE 
                        WHEN b.BASELINE_VR > 0 
                        THEN ROUND((c.VISITORS::FLOAT / NULLIF(c.PANEL_REACH, 0) * 100 - b.BASELINE_VR) / b.BASELINE_VR * 100, 1)
                        ELSE NULL 
                    END as LIFT_PCT
                FROM campaign_metrics c
                CROSS JOIN baseline b
                WHERE c.PANEL_REACH >= 1000
                ORDER BY c.IMPRESSIONS DESC
                LIMIT 100
            """
            
            cursor.execute(query, {
                'advertiser_id': int(advertiser_id),
                'start_date': start_date,
                'end_date': end_date
            })
            visit_type = 'store'
        
        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        
        if not rows:
            cursor.close()
            conn.close()
            return jsonify({
                'success': True,
                'data': [],
                'baseline': None,
                'visit_type': visit_type,
                'message': 'No lift data available - requires minimum 1,000 panel reach per campaign'
            })
        
        # Extract baseline from first row
        baseline = None
        results = []
        for row in rows:
            d = dict(zip(columns, row))
            if baseline is None and d.get('BASELINE_VR'):
                baseline = float(d['BASELINE_VR'])
            # Convert Decimal types for JSON
            for k, v in d.items():
                if hasattr(v, 'is_integer'):
                    d[k] = float(v) if v else 0
            results.append(d)
        
        cursor.close()
        conn.close()
        
        return jsonify({
            'success': True,
            'data': results,
            'baseline': baseline,
            'visit_type': visit_type
        })
        
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

# =============================================================================
# TRAFFIC SOURCES (Paramount only)
# =============================================================================

@app.route('/api/v5/traffic-sources', methods=['GET'])
def get_traffic_sources():
    """
    Get traffic source analysis for CTV-attributed web visits.
    
    Shows which traffic sources drove visitors who ALSO saw CTV ads.
    Only works for Paramount advertisers with web pixel.
    
    Query Parameters:
    - advertiser_id (required): Quorum Advertiser ID
    - min_visits (optional): Minimum visits to include source (default: 100)
    """
    advertiser_id = request.args.get('advertiser_id')
    min_visits = int(request.args.get('min_visits', '100'))
    
    if not advertiser_id:
        return jsonify({
            'success': False,
            'error': 'advertiser_id parameter required'
        }), 400
    
    try:
        conn = get_snowflake_connection()
        cursor = conn.cursor()
        
        # Use string formatting for the query since ILIKE wildcards conflict with %(param)s style
        query = f"""
            WITH 
            -- Get CTV-attributed web visits with their referrer sources
            ctv_visits_with_source AS (
                SELECT 
                    p.WEB_IMPRESSION_ID,
                    CASE 
                        WHEN r.VALUE ILIKE '%doubleclick%' OR r.VALUE ILIKE '%googleadservices%' THEN 'Google Ads'
                        WHEN r.VALUE ILIKE '%google%' THEN 'Google Organic'
                        WHEN r.VALUE ILIKE '%fbapp%' OR r.VALUE ILIKE '%facebook%' OR r.VALUE ILIKE '%fb.com%' THEN 'Meta/Facebook'
                        WHEN r.VALUE ILIKE '%instagram%' THEN 'Instagram'
                        WHEN r.VALUE ILIKE '%youtube%' THEN 'YouTube'
                        WHEN r.VALUE ILIKE '%tiktok%' THEN 'TikTok'
                        WHEN r.VALUE ILIKE '%taboola%' THEN 'Taboola'
                        WHEN r.VALUE ILIKE '%outbrain%' THEN 'Outbrain'
                        WHEN r.VALUE ILIKE '%bing%' THEN 'Bing'
                        WHEN r.VALUE ILIKE '%yahoo%' THEN 'Yahoo'
                        WHEN r.VALUE ILIKE '%t.co%' OR r.VALUE ILIKE '%twitter%' THEN 'Twitter/X'
                        WHEN r.VALUE IS NULL OR r.VALUE = '-' OR r.VALUE = '' THEN 'Direct'
                        ELSE 'Other'
                    END as traffic_source
                FROM QUORUMDB.SEGMENT_DATA.PARAMOUNT_IMPRESSIONS_REPORT_90_DAYS p
                LEFT JOIN QUORUMDB.SEGMENT_DATA.PARAMOUNT_WEB_IMPRESSION_DATA r
                    ON p.WEB_IMPRESSION_ID = r.UUID AND r.KEY = 'referrer'
                WHERE p.QUORUM_ADVERTISER_ID = {int(advertiser_id)}
                  AND p.IS_SITE_VISIT = 'TRUE'
                  AND p.WEB_IMPRESSION_ID IS NOT NULL
            ),
            
            -- Total CTV impressions for this advertiser
            ctv_impressions AS (
                SELECT COUNT(*) as imp_count
                FROM QUORUMDB.SEGMENT_DATA.PARAMOUNT_IMPRESSIONS_REPORT_90_DAYS
                WHERE QUORUM_ADVERTISER_ID = {int(advertiser_id)}
            ),
            
            -- Count page views per visit (events per UUID)
            visit_page_views AS (
                SELECT 
                    c.WEB_IMPRESSION_ID,
                    c.traffic_source,
                    COUNT(e.UUID) as page_views
                FROM ctv_visits_with_source c
                LEFT JOIN QUORUMDB.SEGMENT_DATA.PARAMOUNT_WEB_IMPRESSION_DATA e
                    ON c.WEB_IMPRESSION_ID = e.UUID AND e.KEY = 'event'
                GROUP BY c.WEB_IMPRESSION_ID, c.traffic_source
            ),
            
            -- Aggregate by traffic source
            source_metrics AS (
                SELECT 
                    traffic_source as source,
                    COUNT(*) as visits,
                    ROUND(AVG(page_views), 2) as avg_page_views_per_visit
                FROM visit_page_views
                WHERE traffic_source NOT IN ('Other')
                GROUP BY traffic_source
                HAVING COUNT(*) >= {min_visits}
            ),
            
            -- Total CTV visits for percentage calculation
            total_ctv_visits AS (
                SELECT COUNT(*) as total FROM ctv_visits_with_source
            )
            
            -- Final output with CTV overlap (all are 100% since these are CTV-attributed)
            SELECT 
                source,
                0 as impressions,
                visits,
                avg_page_views_per_visit,
                ROUND(visits::FLOAT / NULLIF((SELECT total FROM total_ctv_visits), 0) * 100, 2) as pct_of_ctv_traffic
            FROM source_metrics

            UNION ALL

            -- CTV View Through summary row
            SELECT 
                'CTV View Through' as source,
                (SELECT imp_count FROM ctv_impressions) as impressions,
                (SELECT total FROM total_ctv_visits) as visits,
                ROUND((SELECT AVG(page_views) FROM visit_page_views), 2) as avg_page_views_per_visit,
                100.0 as pct_of_ctv_traffic

            ORDER BY 
                CASE WHEN source = 'CTV View Through' THEN 0 ELSE 1 END,
                visits DESC
        """
        
        cursor.execute(query)
        
        columns = [desc[0].lower() for desc in cursor.description]
        results = []
        for row in cursor.fetchall():
            d = dict(zip(columns, row))
            # Convert Decimal to float for JSON serialization
            for k, v in d.items():
                if hasattr(v, 'is_integer'):  # Decimal type
                    d[k] = float(v) if v else 0
            # Rename pct_of_ctv_traffic to ctv_overlap_pct for frontend compatibility
            if 'pct_of_ctv_traffic' in d:
                d['ctv_overlap_pct'] = d.pop('pct_of_ctv_traffic')
            results.append(d)
        
        cursor.close()
        conn.close()
        
        return jsonify({'success': True, 'data': results})
        
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

# =============================================================================
# MAIN
# =============================================================================

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8080))
    app.run(host='0.0.0.0', port=port, debug=False)
