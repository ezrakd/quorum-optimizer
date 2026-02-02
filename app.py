"""
Quorum Optimizer API v5.3 - Unified Platform Architecture with Lift
====================================================================
All queries now use BASE_TABLES.AD_IMPRESSION_LOG as the single source of truth.

Key changes in v5.3:
- Unified Lift Analysis for ALL agencies (no more class delineation)
- Paramount: Web-based lift from PARAMOUNT_IMPRESSIONS_REPORT_90_DAYS
- Others: Store-based lift from AD_IMPRESSION_LOG + CAMPAIGN_PERFORMANCE_STORE_VISITS_RAW
- IO-based advertiser mapping (uses CPSV_RAW.ADVERTISER_ID to identify campaigns)

Key changes in v5.2:
- Direct QUORUM_ADVERTISER_ID filtering (no ADVERTISER_PIXEL_STATS join needed)
- Device-home geo fallback: COALESCE(MAID_CENTROID_DATA.ZIP_CODE, POSTAL_CODE)
- Correct join key: AD_IMPRESSION_LOG.ID = CAMPAIGN_PERFORMANCE_STORE_VISITS_RAW.IMP_ID

Tables:
- BASE_TABLES.AD_IMPRESSION_LOG: All impressions with QUORUM_ADVERTISER_ID, PT, DMA, POSTAL_CODE
- BASE_TABLES.MAID_CENTROID_DATA: Device home ZIP (0.04% coverage, will improve)
- SEGMENT_DATA.CAMPAIGN_PERFORMANCE_STORE_VISITS_RAW: Store visit attribution (+ advertiser mapping)
- SEGMENT_DATA.PT_TO_PLATFORM: Platform code lookup (PT → Platform name)
- SEGMENT_DATA.PARAMOUNT_IMPRESSIONS_REPORT_90_DAYS: Paramount impressions with IS_SITE_VISIT

Lift Methodology:
- Visit Rate = VISITORS / REACH × 100
- Baseline = Average visit rate for the advertiser
- Lift % = (Visit Rate - Baseline) / Baseline × 100
- Index = Visit Rate / Baseline × 100 (100 = average)
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

AGENCY_CONFIG = {
    1480: {'name': 'Paramount'},
    1813: {'name': 'Causal iQ'},
    2514: {'name': 'MNTN'},
    1972: {'name': 'Hearst'},
    2234: {'name': 'Magnite'},
    2379: {'name': 'The Shipyard'},
    1445: {'name': 'Publicis'},
    1880: {'name': 'TeamSnap'},
    2744: {'name': 'Parallel Path'},
    2691: {'name': 'TravelSpike'},
    2393: {'name': 'AIOPW'},
    1956: {'name': 'Dealer Spike'},
    2298: {'name': 'InteractRV'},
    1955: {'name': 'ARI Network'},
    2086: {'name': 'Level5'},
    1950: {'name': 'ByRider'},
    1202: {'name': 'LotLinx'},
    1565: {'name': 'NPRP Media'},
    1697: {'name': '313 Presents'},
}

def get_agency_name(agency_id):
    """Get agency name from config"""
    config = AGENCY_CONFIG.get(int(agency_id))
    return config['name'] if config else f"Agency {agency_id}"

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

def clean_advertiser_name(name):
    """Remove prefix IDs from advertiser names"""
    if name:
        return re.sub(r'^[0-9A-Za-z]+ - ', '', str(name))
    return name

# =============================================================================
# HEALTH CHECK
# =============================================================================

@app.route('/health', methods=['GET'])
def health_check():
    return jsonify({
        'status': 'healthy',
        'version': '5.3-unified-lift',
        'description': 'Unified AD_IMPRESSION_LOG with device-geo fallback and unified lift analysis'
    })

# =============================================================================
# AGENCY OVERVIEW
# =============================================================================

@app.route('/api/v5/agencies', methods=['GET'])
def get_agencies():
    """
    Get all agencies with aggregated metrics from unified AD_IMPRESSION_LOG.
    Paramount store/web visits come from PARAMOUNT_DASHBOARD_SUMMARY_STATS.
    """
    try:
        start_date, end_date = get_date_range()
        conn = get_snowflake_connection()
        cursor = conn.cursor()
        
        all_results = []
        
        # Query 1: All agencies (except Paramount) from AD_IMPRESSION_LOG
        query_unified = """
            SELECT 
                i.AGENCY_ID,
                COUNT(DISTINCT i.ID) as IMPRESSIONS,
                COUNT(DISTINCT sv.DEVICE_ID) as STORE_VISITS,
                COUNT(DISTINCT i.QUORUM_ADVERTISER_ID) as ADVERTISER_COUNT
            FROM QUORUMDB.BASE_TABLES.AD_IMPRESSION_LOG i
            LEFT JOIN QUORUMDB.SEGMENT_DATA.CAMPAIGN_PERFORMANCE_STORE_VISITS_RAW sv 
                ON sv.IMP_ID = i.ID 
                AND sv.AGENCY_ID = i.AGENCY_ID
            WHERE i.TIMESTAMP::DATE BETWEEN %(start_date)s AND %(end_date)s
              AND i.AGENCY_ID != 1480
            GROUP BY i.AGENCY_ID
            HAVING COUNT(DISTINCT i.ID) > 0
        """
        cursor.execute(query_unified, {'start_date': start_date, 'end_date': end_date})
        for row in cursor.fetchall():
            agency_id = row[0]
            all_results.append({
                'AGENCY_ID': agency_id,
                'AGENCY_NAME': get_agency_name(agency_id),
                'IMPRESSIONS': row[1] or 0,
                'STORE_VISITS': row[2] or 0,
                'WEB_VISITS': 0,
                'ADVERTISER_COUNT': row[3] or 0
            })
        
        # Query 2: Paramount from PARAMOUNT_DASHBOARD_SUMMARY_STATS (has store + web visits)
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
    """
    Get advertisers for a specific agency.
    Uses QUORUM_ADVERTISER_ID directly from AD_IMPRESSION_LOG.
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
            # Paramount - use PARAMOUNT_DASHBOARD_SUMMARY_STATS
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
            # Other agencies - unified AD_IMPRESSION_LOG query
            query = """
                SELECT 
                    i.QUORUM_ADVERTISER_ID as ADVERTISER_ID,
                    COALESCE(MAX(aa.COMP_NAME), MAX(i.ADV_NAME), 'Advertiser ' || i.QUORUM_ADVERTISER_ID) as ADVERTISER_NAME,
                    COUNT(DISTINCT i.ID) as IMPRESSIONS,
                    COUNT(DISTINCT sv.DEVICE_ID) as STORE_VISITS,
                    0 as WEB_VISITS
                FROM QUORUMDB.BASE_TABLES.AD_IMPRESSION_LOG i
                LEFT JOIN QUORUMDB.SEGMENT_DATA.AGENCY_ADVERTISER aa 
                    ON i.QUORUM_ADVERTISER_ID = aa.ID::VARCHAR
                LEFT JOIN QUORUMDB.SEGMENT_DATA.CAMPAIGN_PERFORMANCE_STORE_VISITS_RAW sv 
                    ON sv.IMP_ID = i.ID 
                    AND sv.AGENCY_ID = i.AGENCY_ID
                WHERE i.AGENCY_ID = %(agency_id)s
                  AND i.TIMESTAMP::DATE BETWEEN %(start_date)s AND %(end_date)s
                  AND i.QUORUM_ADVERTISER_ID IS NOT NULL
                  AND i.QUORUM_ADVERTISER_ID != '0'
                GROUP BY i.QUORUM_ADVERTISER_ID
                HAVING COUNT(DISTINCT i.ID) > 0
                ORDER BY COUNT(DISTINCT i.ID) DESC
            """
            cursor.execute(query, {
                'agency_id': agency_id,
                'start_date': start_date,
                'end_date': end_date
            })
        
        columns = [desc[0] for desc in cursor.description]
        results = [dict(zip(columns, row)) for row in cursor.fetchall()]
        
        for r in results:
            r['ADVERTISER_NAME'] = clean_advertiser_name(r.get('ADVERTISER_NAME'))
        
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
            # Paramount
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
            cursor.execute(query, {
                'advertiser_id': advertiser_id,
                'start_date': start_date,
                'end_date': end_date
            })
        else:
            # Other agencies - unified query
            query = """
                SELECT 
                    i.IO_ID,
                    MAX(i.IO_NAME) as IO_NAME,
                    COUNT(DISTINCT i.ID) as IMPRESSIONS,
                    COUNT(DISTINCT sv.DEVICE_ID) as STORE_VISITS,
                    0 as WEB_VISITS
                FROM QUORUMDB.BASE_TABLES.AD_IMPRESSION_LOG i
                LEFT JOIN QUORUMDB.SEGMENT_DATA.CAMPAIGN_PERFORMANCE_STORE_VISITS_RAW sv 
                    ON sv.IMP_ID = i.ID 
                    AND sv.AGENCY_ID = i.AGENCY_ID
                WHERE i.AGENCY_ID = %(agency_id)s
                  AND i.QUORUM_ADVERTISER_ID = %(advertiser_id)s
                  AND i.TIMESTAMP::DATE BETWEEN %(start_date)s AND %(end_date)s
                GROUP BY i.IO_ID
                HAVING COUNT(DISTINCT i.ID) >= 100
                ORDER BY COUNT(DISTINCT i.ID) DESC
            """
            cursor.execute(query, {
                'agency_id': agency_id,
                'advertiser_id': str(advertiser_id),
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
    Get line item level performance with platform information.
    """
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
        
        if agency_id == 1480:
            # Paramount
            filters = ""
            if campaign_id:
                filters = f"AND IO_ID = '{campaign_id}'"
            
            query = f"""
                SELECT 
                    LINEITEM_ID as LI_ID,
                    MAX(LINEITEM_NAME) as LI_NAME,
                    MAX(IO_ID) as IO_ID,
                    MAX(IO_NAME) as IO_NAME,
                    COUNT(*) as IMPRESSIONS,
                    COUNT(DISTINCT CASE WHEN IS_STORE_VISIT = 'TRUE' THEN IMP_MAID END) as STORE_VISITS,
                    COUNT(DISTINCT CASE WHEN IS_SITE_VISIT = 'TRUE' THEN IMP_MAID END) as WEB_VISITS,
                    'Free Wheel' as PLATFORM
                FROM QUORUMDB.SEGMENT_DATA.PARAMOUNT_IMPRESSIONS_REPORT_90_DAYS
                WHERE QUORUM_ADVERTISER_ID = %(advertiser_id)s
                  AND IMP_DATE BETWEEN %(start_date)s AND %(end_date)s
                  {filters}
                GROUP BY LINEITEM_ID
                HAVING COUNT(*) >= 100
                ORDER BY COUNT(*) DESC
                LIMIT 100
            """
            cursor.execute(query, {
                'advertiser_id': advertiser_id,
                'start_date': start_date,
                'end_date': end_date
            })
        else:
            # Other agencies - unified query with PT lookup
            filters = ""
            if campaign_id:
                filters = f"AND i.IO_ID = {campaign_id}"
            
            query = f"""
                SELECT 
                    i.LINEITEM_ID as LI_ID,
                    MAX(i.LI_NAME) as LI_NAME,
                    MAX(i.IO_ID) as IO_ID,
                    MAX(i.IO_NAME) as IO_NAME,
                    COUNT(DISTINCT i.ID) as IMPRESSIONS,
                    COUNT(DISTINCT sv.DEVICE_ID) as STORE_VISITS,
                    0 as WEB_VISITS,
                    COALESCE(MAX(p.PLATFORM), 'PT=' || COALESCE(MAX(i.PT)::VARCHAR, '?')) as PLATFORM
                FROM QUORUMDB.BASE_TABLES.AD_IMPRESSION_LOG i
                LEFT JOIN QUORUMDB.SEGMENT_DATA.PT_TO_PLATFORM p ON i.PT = p.PT
                LEFT JOIN QUORUMDB.SEGMENT_DATA.CAMPAIGN_PERFORMANCE_STORE_VISITS_RAW sv 
                    ON sv.IMP_ID = i.ID 
                    AND sv.AGENCY_ID = i.AGENCY_ID
                WHERE i.AGENCY_ID = %(agency_id)s
                  AND i.QUORUM_ADVERTISER_ID = %(advertiser_id)s
                  AND i.TIMESTAMP::DATE BETWEEN %(start_date)s AND %(end_date)s
                  {filters}
                GROUP BY i.LINEITEM_ID
                HAVING COUNT(DISTINCT i.ID) >= 100
                ORDER BY COUNT(DISTINCT i.ID) DESC
                LIMIT 100
            """
            cursor.execute(query, {
                'agency_id': agency_id,
                'advertiser_id': str(advertiser_id),
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
    Get publisher level performance. Uses SITE column from AD_IMPRESSION_LOG.
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
            # Paramount
            filters = ""
            if campaign_id:
                filters += f" AND IO_ID = '{campaign_id}'"
            if lineitem_id:
                filters += f" AND LINEITEM_ID = '{lineitem_id}'"
            
            query = f"""
                SELECT 
                    SITE as PUBLISHER,
                    COUNT(*) as IMPRESSIONS,
                    COUNT(DISTINCT CASE WHEN IS_STORE_VISIT = 'TRUE' THEN IMP_MAID END) as STORE_VISITS,
                    COUNT(DISTINCT CASE WHEN IS_SITE_VISIT = 'TRUE' THEN IMP_MAID END) as WEB_VISITS
                FROM QUORUMDB.SEGMENT_DATA.PARAMOUNT_IMPRESSIONS_REPORT_90_DAYS
                WHERE QUORUM_ADVERTISER_ID = %(advertiser_id)s
                  AND IMP_DATE BETWEEN %(start_date)s AND %(end_date)s
                  {filters}
                GROUP BY SITE
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
            # Other agencies - unified query
            filters = ""
            if campaign_id:
                filters += f" AND i.IO_ID = {campaign_id}"
            if lineitem_id:
                filters += f" AND i.LINEITEM_ID = '{lineitem_id}'"
            
            query = f"""
                SELECT 
                    COALESCE(i.SITE, i.PUBLISHER_CODE, 'Unknown') as PUBLISHER,
                    COUNT(DISTINCT i.ID) as IMPRESSIONS,
                    COUNT(DISTINCT sv.DEVICE_ID) as STORE_VISITS,
                    0 as WEB_VISITS
                FROM QUORUMDB.BASE_TABLES.AD_IMPRESSION_LOG i
                LEFT JOIN QUORUMDB.SEGMENT_DATA.CAMPAIGN_PERFORMANCE_STORE_VISITS_RAW sv 
                    ON sv.IMP_ID = i.ID 
                    AND sv.AGENCY_ID = i.AGENCY_ID
                WHERE i.AGENCY_ID = %(agency_id)s
                  AND i.QUORUM_ADVERTISER_ID = %(advertiser_id)s
                  AND i.TIMESTAMP::DATE BETWEEN %(start_date)s AND %(end_date)s
                  {filters}
                GROUP BY COALESCE(i.SITE, i.PUBLISHER_CODE, 'Unknown')
                HAVING COUNT(DISTINCT i.ID) >= 100
                ORDER BY COUNT(DISTINCT i.ID) DESC
                LIMIT 50
            """
            cursor.execute(query, {
                'agency_id': agency_id,
                'advertiser_id': str(advertiser_id),
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
# ZIP PERFORMANCE (with device-geo fallback)
# =============================================================================

@app.route('/api/v5/zip-performance', methods=['GET'])
def get_zip_performance():
    """
    Get ZIP code performance with device-home geo fallback.
    Priority: MAID_CENTROID_DATA.ZIP_CODE > AD_IMPRESSION_LOG.POSTAL_CODE
    
    Device-home geo coverage is currently ~0.04% but will improve as
    more mobile IDs flow through the system.
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
            # Paramount
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
        else:
            # Other agencies - unified query with device-geo fallback
            filters = ""
            if campaign_id:
                filters += f" AND i.IO_ID = {campaign_id}"
            if lineitem_id:
                filters += f" AND i.LINEITEM_ID = '{lineitem_id}'"
            
            # Device-geo fallback: prefer home ZIP from mobility data, fall back to impression ZIP
            query = f"""
                WITH device_geo AS (
                    SELECT MAID, ZIP_CODE as HOME_ZIP
                    FROM QUORUMDB.BASE_TABLES.MAID_CENTROID_DATA
                    WHERE ZIP_CODE IS NOT NULL AND ZIP_CODE != ''
                )
                SELECT 
                    COALESCE(dg.HOME_ZIP, i.POSTAL_CODE) as ZIP_CODE,
                    COUNT(DISTINCT i.ID) as IMPRESSIONS,
                    COUNT(DISTINCT sv.DEVICE_ID) as STORE_VISITS,
                    0 as WEB_VISITS
                FROM QUORUMDB.BASE_TABLES.AD_IMPRESSION_LOG i
                LEFT JOIN device_geo dg ON REPLACE(i.DEVICE_UNIQUE_ID, 'SYS-', '') = dg.MAID
                LEFT JOIN QUORUMDB.SEGMENT_DATA.CAMPAIGN_PERFORMANCE_STORE_VISITS_RAW sv 
                    ON sv.IMP_ID = i.ID 
                    AND sv.AGENCY_ID = i.AGENCY_ID
                WHERE i.AGENCY_ID = %(agency_id)s
                  AND i.QUORUM_ADVERTISER_ID = %(advertiser_id)s
                  AND i.TIMESTAMP::DATE BETWEEN %(start_date)s AND %(end_date)s
                  AND COALESCE(dg.HOME_ZIP, i.POSTAL_CODE) IS NOT NULL 
                  AND COALESCE(dg.HOME_ZIP, i.POSTAL_CODE) != ''
                  AND COALESCE(dg.HOME_ZIP, i.POSTAL_CODE) != 'null'
                  {filters}
                GROUP BY COALESCE(dg.HOME_ZIP, i.POSTAL_CODE)
                HAVING COUNT(DISTINCT i.ID) >= 100
                ORDER BY COUNT(DISTINCT sv.DEVICE_ID) DESC, COUNT(DISTINCT i.ID) DESC
                LIMIT 100
            """
            cursor.execute(query, {
                'agency_id': agency_id,
                'advertiser_id': str(advertiser_id),
                'start_date': start_date,
                'end_date': end_date
            })
        
        columns = [desc[0] for desc in cursor.description]
        results = [dict(zip(columns, row)) for row in cursor.fetchall()]
        
        cursor.close()
        conn.close()
        
        return jsonify({
            'success': True, 
            'data': results, 
            'note': 'ZIP priority: device-home (mobility) → impression (IP-based)'
        })
        
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

# =============================================================================
# DMA PERFORMANCE (with device-geo fallback)
# =============================================================================

@app.route('/api/v5/dma-performance', methods=['GET'])
def get_dma_performance():
    """
    Get DMA level performance with device-home geo fallback.
    Derives DMA from ZIP using DBIP_LOOKUP_US reference table.
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
            # Paramount - derive DMA from ZIP
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
            # Other agencies - unified query with device-geo fallback
            filters = ""
            if campaign_id:
                filters += f" AND i.IO_ID = {campaign_id}"
            if lineitem_id:
                filters += f" AND i.LINEITEM_ID = '{lineitem_id}'"
            
            # Device-geo fallback: prefer home ZIP from mobility data, fall back to impression ZIP
            # Then derive DMA from the resolved ZIP
            query = f"""
                WITH device_geo AS (
                    SELECT MAID, ZIP_CODE as HOME_ZIP
                    FROM QUORUMDB.BASE_TABLES.MAID_CENTROID_DATA
                    WHERE ZIP_CODE IS NOT NULL AND ZIP_CODE != ''
                ),
                zip_dma AS (
                    SELECT ZIPCODE, MAX(DMA_NAME) as DMA_NAME 
                    FROM QUORUMDB.SEGMENT_DATA.DBIP_LOOKUP_US 
                    WHERE DMA_NAME IS NOT NULL AND DMA_NAME != ''
                    GROUP BY ZIPCODE
                )
                SELECT 
                    d.DMA_NAME as DMA,
                    COUNT(DISTINCT i.ID) as IMPRESSIONS,
                    COUNT(DISTINCT sv.DEVICE_ID) as STORE_VISITS,
                    0 as WEB_VISITS
                FROM QUORUMDB.BASE_TABLES.AD_IMPRESSION_LOG i
                LEFT JOIN device_geo dg ON REPLACE(i.DEVICE_UNIQUE_ID, 'SYS-', '') = dg.MAID
                JOIN zip_dma d ON COALESCE(dg.HOME_ZIP, i.POSTAL_CODE) = d.ZIPCODE
                LEFT JOIN QUORUMDB.SEGMENT_DATA.CAMPAIGN_PERFORMANCE_STORE_VISITS_RAW sv 
                    ON sv.IMP_ID = i.ID 
                    AND sv.AGENCY_ID = i.AGENCY_ID
                WHERE i.AGENCY_ID = %(agency_id)s
                  AND i.QUORUM_ADVERTISER_ID = %(advertiser_id)s
                  AND i.TIMESTAMP::DATE BETWEEN %(start_date)s AND %(end_date)s
                  {filters}
                GROUP BY d.DMA_NAME
                HAVING COUNT(DISTINCT i.ID) >= 100
                ORDER BY COUNT(DISTINCT i.ID) DESC
                LIMIT 50
            """
            cursor.execute(query, {
                'agency_id': agency_id,
                'advertiser_id': str(advertiser_id),
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
            cursor.execute(query, {
                'advertiser_id': advertiser_id,
                'start_date': start_date,
                'end_date': end_date
            })
        else:
            # Other agencies - unified query
            query = """
                SELECT 
                    COUNT(DISTINCT i.ID) as IMPRESSIONS,
                    COUNT(DISTINCT sv.DEVICE_ID) as STORE_VISITS,
                    0 as WEB_VISITS,
                    MIN(i.TIMESTAMP::DATE) as MIN_DATE,
                    MAX(i.TIMESTAMP::DATE) as MAX_DATE,
                    COUNT(DISTINCT i.IO_ID) as CAMPAIGN_COUNT,
                    COUNT(DISTINCT i.LINEITEM_ID) as LINEITEM_COUNT
                FROM QUORUMDB.BASE_TABLES.AD_IMPRESSION_LOG i
                LEFT JOIN QUORUMDB.SEGMENT_DATA.CAMPAIGN_PERFORMANCE_STORE_VISITS_RAW sv 
                    ON sv.IMP_ID = i.ID 
                    AND sv.AGENCY_ID = i.AGENCY_ID
                WHERE i.AGENCY_ID = %(agency_id)s
                  AND i.QUORUM_ADVERTISER_ID = %(advertiser_id)s
                  AND i.TIMESTAMP::DATE BETWEEN %(start_date)s AND %(end_date)s
            """
            cursor.execute(query, {
                'agency_id': agency_id,
                'advertiser_id': str(advertiser_id),
                'start_date': start_date,
                'end_date': end_date
            })
        
        columns = [desc[0] for desc in cursor.description]
        row = cursor.fetchone()
        result = dict(zip(columns, row)) if row else {}
        
        if result.get('MIN_DATE'):
            result['MIN_DATE'] = str(result['MIN_DATE'])
        if result.get('MAX_DATE'):
            result['MAX_DATE'] = str(result['MAX_DATE'])
        
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
            cursor.execute(query, {
                'advertiser_id': advertiser_id,
                'start_date': start_date,
                'end_date': end_date
            })
        else:
            # Other agencies - unified query
            query = """
                SELECT 
                    i.TIMESTAMP::DATE as LOG_DATE,
                    COUNT(DISTINCT i.ID) as IMPRESSIONS,
                    COUNT(DISTINCT sv.DEVICE_ID) as STORE_VISITS,
                    0 as WEB_VISITS
                FROM QUORUMDB.BASE_TABLES.AD_IMPRESSION_LOG i
                LEFT JOIN QUORUMDB.SEGMENT_DATA.CAMPAIGN_PERFORMANCE_STORE_VISITS_RAW sv 
                    ON sv.IMP_ID = i.ID 
                    AND sv.AGENCY_ID = i.AGENCY_ID
                WHERE i.AGENCY_ID = %(agency_id)s
                  AND i.QUORUM_ADVERTISER_ID = %(advertiser_id)s
                  AND i.TIMESTAMP::DATE BETWEEN %(start_date)s AND %(end_date)s
                GROUP BY i.TIMESTAMP::DATE
                ORDER BY i.TIMESTAMP::DATE
            """
            cursor.execute(query, {
                'agency_id': agency_id,
                'advertiser_id': str(advertiser_id),
                'start_date': start_date,
                'end_date': end_date
            })
        
        columns = [desc[0] for desc in cursor.description]
        results = []
        for row in cursor.fetchall():
            d = dict(zip(columns, row))
            if d.get('LOG_DATE'):
                d['LOG_DATE'] = str(d['LOG_DATE'])
            results.append(d)
        
        cursor.close()
        conn.close()
        
        return jsonify({'success': True, 'data': results})
        
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

# =============================================================================
# LIFT ANALYSIS ENDPOINT - UNIFIED
# =============================================================================

@app.route('/api/v5/lift-analysis', methods=['GET'])
def get_lift_analysis():
    """
    Unified lift analysis using AD_IMPRESSION_LOG + CAMPAIGN_PERFORMANCE_STORE_VISITS_RAW.
    
    Lift methodology:
    - Visit Rate = VISITORS / REACH × 100
    - Baseline = Overall average visit rate for the advertiser
    - Lift % = (Visit Rate - Baseline) / Baseline × 100
    - Index = Visit Rate / Baseline × 100 (100 = average performance)
    
    Data Sources:
    - Paramount (1480): PARAMOUNT_IMPRESSIONS_REPORT_90_DAYS (web visits via IS_SITE_VISIT)
    - All Others: AD_IMPRESSION_LOG + CAMPAIGN_PERFORMANCE_STORE_VISITS_RAW (store visits)
    
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
                    COALESCE(MAX(LI_NAME), 'LI-' || LINEITEM_ID::VARCHAR) as NAME,
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
                    WHERE QUORUM_ADVERTISER_ID = %(advertiser_id)s
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
            # ALL OTHER AGENCIES: Store-based lift from unified tables
            # Uses IO-based advertiser mapping from CAMPAIGN_PERFORMANCE_STORE_VISITS_RAW
            # =================================================================
            if group_by == 'lineitem':
                group_cols = "aio.IO_ID, i.LINEITEM_ID"
                name_cols = """
                    COALESCE(MAX(i.LI_NAME), 'LI-' || i.LINEITEM_ID::VARCHAR) as NAME,
                    COALESCE(MAX(i.IO_NAME), 'IO-' || aio.IO_ID::VARCHAR) as PARENT_NAME,
                    i.LINEITEM_ID as ID,
                    aio.IO_ID as PARENT_ID,
                """
            else:  # campaign
                group_cols = "aio.IO_ID"
                name_cols = """
                    COALESCE(MAX(i.IO_NAME), 'IO-' || aio.IO_ID::VARCHAR) as NAME,
                    NULL as PARENT_NAME,
                    aio.IO_ID as ID,
                    NULL as PARENT_ID,
                """
            
            query = f"""
                WITH advertiser_ios AS (
                    -- Get advertiser -> IO mapping from CPSV_RAW (which has correct ADVERTISER_ID)
                    SELECT DISTINCT 
                        sv.AGENCY_ID, 
                        sv.ADVERTISER_ID, 
                        imp.IO_ID
                    FROM QUORUMDB.SEGMENT_DATA.CAMPAIGN_PERFORMANCE_STORE_VISITS_RAW sv
                    INNER JOIN QUORUMDB.BASE_TABLES.AD_IMPRESSION_LOG imp ON sv.IMP_ID = imp.ID
                    WHERE sv.AGENCY_ID = %(agency_id)s
                      AND sv.ADVERTISER_ID = %(advertiser_id)s
                      AND sv.DRIVE_BY_DATE >= DATEADD(day, -90, CURRENT_DATE())
                ),
                campaign_metrics AS (
                    SELECT 
                        {group_cols},
                        {name_cols}
                        COUNT(DISTINCT i.ID) as IMPRESSIONS,
                        COUNT(DISTINCT i.DEVICE_UNIQUE_ID) as REACH,
                        COUNT(DISTINCT sv.DEVICE_ID) as VISITORS
                    FROM advertiser_ios aio
                    INNER JOIN QUORUMDB.BASE_TABLES.AD_IMPRESSION_LOG i 
                        ON i.IO_ID = aio.IO_ID AND i.AGENCY_ID = aio.AGENCY_ID
                    LEFT JOIN QUORUMDB.SEGMENT_DATA.CAMPAIGN_PERFORMANCE_STORE_VISITS_RAW sv 
                        ON sv.IMP_ID = i.ID AND sv.AGENCY_ID = i.AGENCY_ID
                    WHERE i.TIMESTAMP BETWEEN %(start_date)s AND %(end_date)s
                    GROUP BY {group_cols}
                    HAVING COUNT(DISTINCT i.ID) >= 1000
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
                'agency_id': agency_id,
                'advertiser_id': int(advertiser_id),
                'start_date': start_date,
                'end_date': end_date
            })
            visit_type = 'store'
        
        columns = [desc[0] for desc in cursor.description]
        results = []
        baseline = None
        
        for row in cursor.fetchall():
            d = dict(zip(columns, row))
            if baseline is None and d.get('BASELINE_VR') is not None:
                baseline = round(float(d['BASELINE_VR']), 4) if d['BASELINE_VR'] else None
            # Clean up the result
            d['NAME'] = d.get('NAME') or d.get('ID') or 'Unknown'
            results.append(d)
        
        cursor.close()
        conn.close()
        
        return jsonify({
            'success': True, 
            'data': results,
            'baseline': baseline,
            'group_by': group_by,
            'visit_type': visit_type,
            'note': f'Lift calculated vs advertiser average ({visit_type} visits). Index 100 = average performance.'
        })
        
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

# =============================================================================
# MAIN
# =============================================================================

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8080))
    app.run(host='0.0.0.0', port=port, debug=False)
