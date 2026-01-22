"""
Quorum Optimizer API v3.6 - VIACOM STORE VISITS
===============================================
Version: 3.6
Updated: January 22, 2026

Changes from v3.5:
- ViacomCBS now supports STORE VISITS from PARAMOUNT_IMP_STORE_VISITS
- Summary shows both W_VISITS and S_VISITS for ViacomCBS advertisers
- Publishers and ZIPs combine web + store data
- Requires SELECT grant on PARAMOUNT_IMP_STORE_VISITS to OPTIMIZER_READONLY_ROLE

Architecture:
- Class A (MNTN, Dealer Spike, InteractRV, ARI, ByRider, Level5): QRM_ALL_VISITS_V3
- Class B (Causal iQ, Hearst, Magnite, TeamSnap, Shipyard, Parallel Path): CAMPAIGN_PERFORMANCE_STORE_VISITS_RAW
- ViacomCBS: QRM_ALL_VISITS_V3 (web) + PARAMOUNT_IMP_STORE_VISITS (store) + XANDR (impressions)
- Impressions: XANDR_IMPRESSION_LOG (all agencies)

Deployment: Railway (github.com/ezrakd/quorum-optimizer)
"""

import os
from flask import Flask, request, jsonify
from flask_cors import CORS
import snowflake.connector
from datetime import datetime, timedelta

app = Flask(__name__)
CORS(app)

# =============================================================================
# AGENCY CLASSIFICATION
# =============================================================================

CLASS_A_AGENCIES = [2514, 1956, 2298, 1955, 1950, 2086]  # MNTN, Dealer Spike, InteractRV, ARI, ByRider, Level5
CLASS_B_AGENCIES = [1813, 1972, 2234, 2379, 1880, 2744]  # Causal iQ, Hearst, Magnite, TeamSnap, Shipyard, Parallel Path
VIACOM_AGENCY = 1480

AGENCY_NAMES = {
    2514: 'MNTN', 1956: 'Dealer Spike', 2298: 'InteractRV', 
    1955: 'ARI Network Services', 1950: 'ByRider', 2086: 'Level5',
    1813: 'Causal iQ', 1972: 'Hearst', 2234: 'Magnite',
    2379: 'The Shipyard', 1880: 'TeamSnap', 2744: 'Parallel Path',
    1480: 'ViacomCBS WhoSay'
}

def get_agency_class(agency_id):
    """Determine which data source to use for an agency"""
    agency_id = int(agency_id)
    if agency_id in CLASS_A_AGENCIES:
        return 'A'
    elif agency_id in CLASS_B_AGENCIES:
        return 'B'
    elif agency_id == VIACOM_AGENCY:
        return 'VIACOM'
    return None

# =============================================================================
# DATABASE HELPERS
# =============================================================================

def get_snowflake_connection():
    return snowflake.connector.connect(
        account=os.environ.get('SNOWFLAKE_ACCOUNT', 'FZB05958.us-east-1'),
        user=os.environ.get('SNOWFLAKE_USER', 'OPTIMIZER_SERVICE_USER'),
        password=os.environ.get('SNOWFLAKE_PASSWORD'),
        warehouse=os.environ.get('SNOWFLAKE_WAREHOUSE', 'COMPUTE_WH'),
        database='QUORUMDB',
        schema='SEGMENT_DATA'
    )

def execute_query(query, params=None):
    conn = get_snowflake_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(query, params or {})
        columns = [col[0] for col in cursor.description]
        results = []
        for row in cursor.fetchall():
            row_dict = {}
            for i, val in enumerate(row):
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
    default_end = datetime.now().strftime('%Y-%m-%d')
    default_start = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
    return request.args.get('start_date', default_start), request.args.get('end_date', default_end)

# =============================================================================
# HEALTH CHECK
# =============================================================================

@app.route('/api/health', methods=['GET'])
def health_check():
    try:
        conn = get_snowflake_connection()
        conn.close()
        return jsonify({
            'success': True, 
            'status': 'healthy', 
            'version': '3.6-VIACOM-STORE',
            'agencies_supported': 13,
            'class_a': CLASS_A_AGENCIES,
            'class_b': CLASS_B_AGENCIES,
            'viacom': VIACOM_AGENCY,
            'note': 'ViacomCBS now includes web + store visits'
        })
    except Exception as e:
        return jsonify({'success': False, 'status': 'unhealthy', 'error': str(e)}), 500

# =============================================================================
# UNIFIED AGENCIES ENDPOINT - ALL 13 AGENCIES (FIXED)
# =============================================================================

@app.route('/api/v3/agencies', methods=['GET'])
def get_all_agencies():
    """List ALL 13 agencies with S_VISITS and W_VISITS
    
    NOTE: ViacomCBS store visits are omitted due to PARAMOUNT_IMP_STORE_VISITS
    table access issues. Web visits still work fine (62M+ visits).
    """
    start_date, end_date = get_date_params(request)
    
    # FIXED: Removed PARAMOUNT_IMP_STORE_VISITS reference that was causing 500 error
    query = """
    SELECT AGENCY_ID, SUM(SV) as S_VISITS, SUM(WV) as W_VISITS, MAX(ADV_CNT) as ADVERTISER_COUNT
    FROM (
        -- Class A: QRM_ALL_VISITS_V3 store visits
        SELECT v.AGENCY_ID, COUNT(*) as SV, 0 as WV, COUNT(DISTINCT v.QUORUM_ADVERTISER_ID) as ADV_CNT
        FROM QUORUMDB.SEGMENT_DATA.QRM_ALL_VISITS_V3 v
        WHERE v.CONVERSION_DATE >= %(start_date)s AND v.CONVERSION_DATE <= %(end_date)s
          AND v.VISIT_TYPE = 'STORE' AND v.AGENCY_ID IN (2514, 1956, 2298, 1955, 1950, 2086)
        GROUP BY v.AGENCY_ID
        
        UNION ALL
        
        -- Class B: CAMPAIGN_PERFORMANCE_STORE_VISITS_RAW
        SELECT cp.AGENCY_ID, COUNT(DISTINCT CONCAT(cp.DEVICE_ID, cp.DRIVE_BY_DATE, cp.POI_MD5)), 0, COUNT(DISTINCT cp.ADVERTISER_ID)
        FROM QUORUMDB.SEGMENT_DATA.CAMPAIGN_PERFORMANCE_STORE_VISITS_RAW cp
        WHERE cp.DRIVE_BY_DATE >= %(start_date)s AND cp.DRIVE_BY_DATE <= %(end_date)s
          AND cp.AGENCY_ID IN (1813, 1972, 2234, 2379, 1880, 2744)
        GROUP BY cp.AGENCY_ID
        
        UNION ALL
        
        -- ViacomCBS Web ONLY (store visits omitted - PARAMOUNT_IMP_STORE_VISITS not accessible)
        SELECT 1480, 0, COUNT(*), COUNT(DISTINCT v.QUORUM_ADVERTISER_ID)
        FROM QUORUMDB.SEGMENT_DATA.QRM_ALL_VISITS_V3 v
        JOIN QUORUMDB.SEGMENT_DATA.AGENCY_ADVERTISER aa ON TRY_CAST(v.QUORUM_ADVERTISER_ID AS NUMBER) = aa.ID
        WHERE v.CONVERSION_DATE >= %(start_date)s AND v.CONVERSION_DATE <= %(end_date)s
          AND v.VISIT_TYPE = 'WEB' AND aa.ADVERTISER_ID = 1480
    ) combined
    GROUP BY AGENCY_ID
    ORDER BY (SUM(SV) + SUM(WV)) DESC
    """
    
    try:
        results = execute_query(query, {'start_date': start_date, 'end_date': end_date})
        # Add agency names
        for r in results:
            r['AGENCY_NAME'] = AGENCY_NAMES.get(r['AGENCY_ID'], 'Unknown')
            r['DATA_CLASS'] = get_agency_class(r['AGENCY_ID'])
        return jsonify({'success': True, 'data': results, 'total_agencies': len(results)})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

# =============================================================================
# UNIFIED ADVERTISERS ENDPOINT (FIXED)
# =============================================================================

@app.route('/api/v3/advertisers', methods=['GET'])
def get_advertisers_unified():
    """List advertisers for any agency - routes to correct data source
    
    FIXED: ViacomCBS now uses web visits only (removed PARAMOUNT_IMP_STORE_VISITS)
    """
    agency_id = request.args.get('agency_id')
    start_date, end_date = get_date_params(request)
    
    if not agency_id:
        return jsonify({'success': False, 'error': 'agency_id required'}), 400
    
    agency_class = get_agency_class(agency_id)
    
    if agency_class == 'A':
        query = """
        SELECT 
            v.QUORUM_ADVERTISER_ID as ADVERTISER_ID,
            MAX(aa.COMP_NAME) as ADVERTISER_NAME,
            COUNT(*) as S_VISITS,
            0 as W_VISITS,
            COUNT(DISTINCT v.MAID) as UNIQUE_VISITORS
        FROM QUORUMDB.SEGMENT_DATA.QRM_ALL_VISITS_V3 v
        LEFT JOIN QUORUMDB.SEGMENT_DATA.AGENCY_ADVERTISER aa 
            ON TRY_CAST(v.QUORUM_ADVERTISER_ID AS NUMBER) = aa.ID
        WHERE v.AGENCY_ID = %(agency_id)s
          AND v.CONVERSION_DATE >= %(start_date)s
          AND v.CONVERSION_DATE <= %(end_date)s
          AND v.VISIT_TYPE = 'STORE'
        GROUP BY v.QUORUM_ADVERTISER_ID
        ORDER BY S_VISITS DESC
        """
    elif agency_class == 'B':
        query = """
        SELECT 
            cp.ADVERTISER_ID::TEXT as ADVERTISER_ID,
            MAX(aa.COMP_NAME) as ADVERTISER_NAME,
            COUNT(DISTINCT CONCAT(cp.DEVICE_ID, cp.DRIVE_BY_DATE, cp.POI_MD5)) as S_VISITS,
            0 as W_VISITS,
            COUNT(DISTINCT cp.DEVICE_ID) as UNIQUE_VISITORS
        FROM QUORUMDB.SEGMENT_DATA.CAMPAIGN_PERFORMANCE_STORE_VISITS_RAW cp
        JOIN QUORUMDB.SEGMENT_DATA.AGENCY_ADVERTISER aa ON cp.ADVERTISER_ID = aa.ID
        WHERE cp.AGENCY_ID = %(agency_id)s
          AND cp.DRIVE_BY_DATE >= %(start_date)s
          AND cp.DRIVE_BY_DATE <= %(end_date)s
        GROUP BY cp.ADVERTISER_ID
        ORDER BY S_VISITS DESC
        """
    elif agency_class == 'VIACOM':
        # FIXED: ViacomCBS web visits only (removed PARAMOUNT_IMP_STORE_VISITS union)
        query = """
        SELECT 
            v.QUORUM_ADVERTISER_ID as ADVERTISER_ID, 
            MAX(aa.COMP_NAME) as ADVERTISER_NAME,
            0 as S_VISITS, 
            COUNT(*) as W_VISITS, 
            COUNT(DISTINCT v.MAID) as UNIQUE_VISITORS
        FROM QUORUMDB.SEGMENT_DATA.QRM_ALL_VISITS_V3 v
        JOIN QUORUMDB.SEGMENT_DATA.AGENCY_ADVERTISER aa ON TRY_CAST(v.QUORUM_ADVERTISER_ID AS NUMBER) = aa.ID
        WHERE v.CONVERSION_DATE >= %(start_date)s AND v.CONVERSION_DATE <= %(end_date)s
          AND v.VISIT_TYPE = 'WEB' AND aa.ADVERTISER_ID = 1480
        GROUP BY v.QUORUM_ADVERTISER_ID
        ORDER BY W_VISITS DESC
        """
    else:
        return jsonify({'success': False, 'error': f'Unknown agency_id: {agency_id}'}), 400
    
    try:
        results = execute_query(query, {
            'agency_id': int(agency_id),
            'start_date': start_date,
            'end_date': end_date
        })
        return jsonify({
            'success': True, 
            'data': results, 
            'agency_id': int(agency_id),
            'agency_name': AGENCY_NAMES.get(int(agency_id), 'Unknown'),
            'data_class': agency_class
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

# =============================================================================
# UNIFIED ADVERTISER SUMMARY WITH IMPRESSIONS
# =============================================================================

@app.route('/api/v3/advertiser-summary', methods=['GET'])
def get_advertiser_summary_unified():
    """Get advertiser summary with impressions - routes to correct join logic"""
    advertiser_id = request.args.get('advertiser_id')
    agency_id = request.args.get('agency_id')
    start_date, end_date = get_date_params(request)
    
    if not advertiser_id:
        return jsonify({'success': False, 'error': 'advertiser_id required'}), 400
    if not agency_id:
        return jsonify({'success': False, 'error': 'agency_id required'}), 400
    
    agency_class = get_agency_class(agency_id)
    
    if agency_class == 'A':
        # Class A: Campaign-based impression join
        query = """
        WITH visit_campaigns AS (
            SELECT CAMPAIGN_ID, COUNT(*) as S_VISITS, COUNT(DISTINCT MAID) as UNIQUE_VISITORS
            FROM QUORUMDB.SEGMENT_DATA.QRM_ALL_VISITS_V3
            WHERE QUORUM_ADVERTISER_ID = %(advertiser_id)s
              AND CONVERSION_DATE >= %(start_date)s AND CONVERSION_DATE <= %(end_date)s
              AND VISIT_TYPE = 'STORE' AND CAMPAIGN_ID IS NOT NULL
            GROUP BY CAMPAIGN_ID
        ),
        impressions AS (
            SELECT v.CAMPAIGN_ID, COUNT(*) as IMPRESSIONS
            FROM visit_campaigns v
            JOIN QUORUMDB.SEGMENT_DATA.XANDR_IMPRESSION_LOG x ON x.IO_ID = v.CAMPAIGN_ID
            WHERE x.AGENCY_ID = %(agency_id)s
              AND CAST(x.TIMESTAMP AS DATE) >= %(start_date)s 
              AND CAST(x.TIMESTAMP AS DATE) <= %(end_date)s
            GROUP BY v.CAMPAIGN_ID
        )
        SELECT 
            COALESCE(SUM(i.IMPRESSIONS), 0) as IMPRESSIONS,
            SUM(v.S_VISITS) as S_VISITS,
            0 as W_VISITS,
            SUM(v.UNIQUE_VISITORS) as UNIQUE_VISITORS,
            CASE WHEN COALESCE(SUM(i.IMPRESSIONS), 0) > 0 
                 THEN ROUND((SUM(v.S_VISITS)::FLOAT / SUM(i.IMPRESSIONS)) * 100, 4) ELSE 0 END as VISIT_RATE,
            'FULL' as DATA_TIER,
            'STORE' as VISIT_TYPE
        FROM visit_campaigns v
        LEFT JOIN impressions i ON v.CAMPAIGN_ID = i.CAMPAIGN_ID
        """
    elif agency_class == 'B':
        # Class B: QuorumAdvImpMapping-based impression join
        query = """
        WITH visits AS (
            SELECT 
                COUNT(DISTINCT CONCAT(cp.DEVICE_ID, cp.DRIVE_BY_DATE, cp.POI_MD5)) as S_VISITS,
                COUNT(DISTINCT cp.DEVICE_ID) as UNIQUE_VISITORS
            FROM QUORUMDB.SEGMENT_DATA.CAMPAIGN_PERFORMANCE_STORE_VISITS_RAW cp
            WHERE cp.ADVERTISER_ID = %(advertiser_id)s
              AND cp.DRIVE_BY_DATE >= %(start_date)s AND cp.DRIVE_BY_DATE <= %(end_date)s
        ),
        io_mapping AS (
            SELECT DISTINCT IO_ID
            FROM QUORUMDB.SEGMENT_DATA."QuorumAdvImpMapping"
            WHERE QUORUM_ADVERTISER_ID = %(advertiser_id)s AND IO_ID IS NOT NULL AND IO_ID != 0
        ),
        impressions AS (
            SELECT COUNT(*) as IMPRESSIONS
            FROM io_mapping m
            JOIN QUORUMDB.SEGMENT_DATA.XANDR_IMPRESSION_LOG x ON m.IO_ID = x.IO_ID
            WHERE x.AGENCY_ID = %(agency_id)s
              AND CAST(x.TIMESTAMP AS DATE) >= %(start_date)s 
              AND CAST(x.TIMESTAMP AS DATE) <= %(end_date)s
        )
        SELECT 
            COALESCE(i.IMPRESSIONS, 0) as IMPRESSIONS,
            v.S_VISITS,
            0 as W_VISITS,
            v.UNIQUE_VISITORS,
            CASE WHEN COALESCE(i.IMPRESSIONS, 0) > 0 
                 THEN ROUND((v.S_VISITS::FLOAT / i.IMPRESSIONS) * 100, 4) ELSE 0 END as VISIT_RATE,
            CASE WHEN i.IMPRESSIONS > 0 THEN 'MAPPED' ELSE 'UNMAPPED' END as DATA_TIER,
            'STORE' as VISIT_TYPE
        FROM visits v
        CROSS JOIN impressions i
        """
    elif agency_class == 'VIACOM':
        # ViacomCBS: Web visits from QRM + Store visits from PARAMOUNT_IMP_STORE_VISITS
        query = """
        WITH web_campaigns AS (
            SELECT CAMPAIGN_ID, COUNT(*) as W_VISITS, COUNT(DISTINCT MAID) as UNIQUE_VISITORS,
                   SUM(IS_LEAD) as W_LEADS, SUM(IS_PURCHASE) as W_PURCHASES, SUM(PURCHASE_VALUE) as W_AMOUNT
            FROM QUORUMDB.SEGMENT_DATA.QRM_ALL_VISITS_V3
            WHERE QUORUM_ADVERTISER_ID = %(advertiser_id)s
              AND CONVERSION_DATE >= %(start_date)s AND CONVERSION_DATE <= %(end_date)s
              AND VISIT_TYPE = 'WEB' AND CAMPAIGN_ID IS NOT NULL
            GROUP BY CAMPAIGN_ID
        ),
        store_visits AS (
            SELECT COUNT(*) as S_VISITS, COUNT(DISTINCT IMP_MAID) as S_UNIQUE_VISITORS
            FROM QUORUMDB.SEGMENT_DATA.PARAMOUNT_IMP_STORE_VISITS
            WHERE QUORUM_ADVERTISER_ID = %(advertiser_id)s
              AND DRIVEBYDATE >= %(start_date)s AND DRIVEBYDATE <= %(end_date)s
        ),
        all_campaigns AS (
            SELECT CAMPAIGN_ID FROM web_campaigns
            UNION
            SELECT DISTINCT IO_ID FROM QUORUMDB.SEGMENT_DATA.PARAMOUNT_IMP_STORE_VISITS
            WHERE QUORUM_ADVERTISER_ID = %(advertiser_id)s
              AND DRIVEBYDATE >= %(start_date)s AND DRIVEBYDATE <= %(end_date)s
              AND IO_ID IS NOT NULL
        ),
        impressions AS (
            SELECT COUNT(*) as IMPRESSIONS
            FROM QUORUMDB.SEGMENT_DATA.XANDR_IMPRESSION_LOG
            WHERE AGENCY_ID = 1480
              AND IO_ID IN (SELECT CAMPAIGN_ID FROM all_campaigns)
              AND CAST(TIMESTAMP AS DATE) >= %(start_date)s 
              AND CAST(TIMESTAMP AS DATE) <= %(end_date)s
        )
        SELECT 
            COALESCE(i.IMPRESSIONS, 0) as IMPRESSIONS,
            COALESCE(SUM(w.W_VISITS), 0) as W_VISITS,
            COALESCE(s.S_VISITS, 0) as S_VISITS,
            COALESCE(SUM(w.UNIQUE_VISITORS), 0) + COALESCE(s.S_UNIQUE_VISITORS, 0) as UNIQUE_VISITORS,
            COALESCE(SUM(w.W_LEADS), 0) as W_LEADS,
            COALESCE(SUM(w.W_PURCHASES), 0) as W_PURCHASES,
            COALESCE(SUM(w.W_AMOUNT), 0) as W_AMOUNT,
            CASE WHEN COALESCE(i.IMPRESSIONS, 0) > 0 
                 THEN ROUND(((COALESCE(SUM(w.W_VISITS), 0) + COALESCE(s.S_VISITS, 0))::FLOAT / i.IMPRESSIONS) * 100, 4) 
                 ELSE 0 END as VISIT_RATE,
            CASE WHEN COALESCE(s.S_VISITS, 0) > 0 AND COALESCE(SUM(w.W_VISITS), 0) > 0 THEN 'WEB_AND_STORE'
                 WHEN COALESCE(s.S_VISITS, 0) > 0 THEN 'STORE_ONLY'
                 ELSE 'WEB_ONLY' END as DATA_TIER,
            CASE WHEN COALESCE(s.S_VISITS, 0) > 0 THEN 'STORE' ELSE 'WEB' END as VISIT_TYPE
        FROM web_campaigns w
        CROSS JOIN store_visits s
        CROSS JOIN impressions i
        GROUP BY i.IMPRESSIONS, s.S_VISITS, s.S_UNIQUE_VISITORS
        """
    else:
        return jsonify({'success': False, 'error': f'Unknown agency_id: {agency_id}'}), 400
    
    try:
        results = execute_query(query, {
            'advertiser_id': advertiser_id,
            'agency_id': int(agency_id),
            'start_date': start_date,
            'end_date': end_date
        })
        return jsonify({
            'success': True, 
            'data': results[0] if results else {},
            'data_class': agency_class
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

# =============================================================================
# CAMPAIGN PERFORMANCE
# =============================================================================

@app.route('/api/v3/campaign-performance', methods=['GET'])
def get_campaign_performance_unified():
    """Campaign breakdown - only available for Class A and ViacomCBS"""
    advertiser_id = request.args.get('advertiser_id')
    agency_id = request.args.get('agency_id')
    start_date, end_date = get_date_params(request)
    
    if not advertiser_id or not agency_id:
        return jsonify({'success': False, 'error': 'advertiser_id and agency_id required'}), 400
    
    agency_class = get_agency_class(agency_id)
    
    if agency_class == 'B':
        # Class B: Use aggregate stats table
        query = """
        SELECT 
            IO_ID::NUMBER as CAMPAIGN_ID,
            MAX(IO_NAME) as CAMPAIGN_NAME,
            SUM(IMPRESSIONS) as IMPRESSIONS,
            SUM(VISITORS) as S_VISITS,
            CASE WHEN SUM(IMPRESSIONS) > 0 
                 THEN ROUND((SUM(VISITORS)::FLOAT / SUM(IMPRESSIONS)) * 100, 4) ELSE 0 END as VISIT_RATE
        FROM QUORUMDB.SEGMENT_DATA.CAMPAIGN_PERFORMANCE_REPORT_DAILY_STATS
        WHERE ADVERTISER_ID = %(advertiser_id)s
          AND AGENCY_ID = %(agency_id)s
          AND LOG_DATE >= %(start_date)s AND LOG_DATE <= %(end_date)s
          AND IO_ID IS NOT NULL
        GROUP BY IO_ID
        ORDER BY S_VISITS DESC
        LIMIT 50
        """
    elif agency_class == 'A':
        # Class A: Direct campaign data with impressions
        query = """
        WITH visit_data AS (
            SELECT CAMPAIGN_ID, MAX(CAMPAIGN_NAME) as CAMPAIGN_NAME,
                   COUNT(*) as S_VISITS, COUNT(DISTINCT MAID) as UNIQUE_VISITORS
            FROM QUORUMDB.SEGMENT_DATA.QRM_ALL_VISITS_V3
            WHERE QUORUM_ADVERTISER_ID = %(advertiser_id)s
              AND CONVERSION_DATE >= %(start_date)s AND CONVERSION_DATE <= %(end_date)s
              AND VISIT_TYPE = 'STORE' AND CAMPAIGN_ID IS NOT NULL
            GROUP BY CAMPAIGN_ID
        ),
        impressions AS (
            SELECT v.CAMPAIGN_ID, COUNT(*) as IMPRESSIONS
            FROM visit_data v
            JOIN QUORUMDB.SEGMENT_DATA.XANDR_IMPRESSION_LOG x 
                ON x.IO_ID = v.CAMPAIGN_ID AND x.AGENCY_ID = %(agency_id)s
            WHERE CAST(x.TIMESTAMP AS DATE) >= %(start_date)s 
              AND CAST(x.TIMESTAMP AS DATE) <= %(end_date)s
            GROUP BY v.CAMPAIGN_ID
        )
        SELECT v.CAMPAIGN_ID, v.CAMPAIGN_NAME, COALESCE(i.IMPRESSIONS, 0) as IMPRESSIONS,
               v.S_VISITS, v.UNIQUE_VISITORS,
               CASE WHEN COALESCE(i.IMPRESSIONS, 0) > 0 
                    THEN ROUND((v.S_VISITS::FLOAT / i.IMPRESSIONS) * 100, 4) ELSE 0 END as VISIT_RATE
        FROM visit_data v
        LEFT JOIN impressions i ON v.CAMPAIGN_ID = i.CAMPAIGN_ID
        ORDER BY IMPRESSIONS DESC NULLS LAST
        LIMIT 50
        """
    elif agency_class == 'VIACOM':
        # ViacomCBS: Combine web + store campaign data
        query = """
        WITH web_campaigns AS (
            SELECT CAMPAIGN_ID, MAX(CAMPAIGN_NAME) as CAMPAIGN_NAME,
                   COUNT(*) as W_VISITS, 
                   SUM(IS_LEAD) as W_LEADS, SUM(IS_PURCHASE) as W_PURCHASES,
                   COUNT(DISTINCT MAID) as UNIQUE_VISITORS
            FROM QUORUMDB.SEGMENT_DATA.QRM_ALL_VISITS_V3
            WHERE QUORUM_ADVERTISER_ID = %(advertiser_id)s
              AND CONVERSION_DATE >= %(start_date)s AND CONVERSION_DATE <= %(end_date)s
              AND VISIT_TYPE = 'WEB' AND CAMPAIGN_ID IS NOT NULL
            GROUP BY CAMPAIGN_ID
        ),
        store_campaigns AS (
            SELECT IO_ID as CAMPAIGN_ID, MAX(IO_NAME) as CAMPAIGN_NAME,
                   COUNT(*) as S_VISITS,
                   COUNT(DISTINCT IMP_MAID) as UNIQUE_VISITORS
            FROM QUORUMDB.SEGMENT_DATA.PARAMOUNT_IMP_STORE_VISITS
            WHERE QUORUM_ADVERTISER_ID = %(advertiser_id)s
              AND DRIVEBYDATE >= %(start_date)s AND DRIVEBYDATE <= %(end_date)s
              AND IO_ID IS NOT NULL
            GROUP BY IO_ID
        ),
        combined AS (
            SELECT 
                COALESCE(w.CAMPAIGN_ID, s.CAMPAIGN_ID) as CAMPAIGN_ID,
                COALESCE(w.CAMPAIGN_NAME, s.CAMPAIGN_NAME) as CAMPAIGN_NAME,
                COALESCE(w.W_VISITS, 0) as W_VISITS,
                COALESCE(s.S_VISITS, 0) as S_VISITS,
                COALESCE(w.W_LEADS, 0) as W_LEADS,
                COALESCE(w.W_PURCHASES, 0) as W_PURCHASES,
                COALESCE(w.UNIQUE_VISITORS, 0) + COALESCE(s.UNIQUE_VISITORS, 0) as UNIQUE_VISITORS
            FROM web_campaigns w
            FULL OUTER JOIN store_campaigns s ON w.CAMPAIGN_ID = s.CAMPAIGN_ID
        ),
        impressions AS (
            SELECT IO_ID, COUNT(*) as IMPRESSIONS
            FROM QUORUMDB.SEGMENT_DATA.XANDR_IMPRESSION_LOG
            WHERE AGENCY_ID = 1480
              AND CAST(TIMESTAMP AS DATE) >= %(start_date)s 
              AND CAST(TIMESTAMP AS DATE) <= %(end_date)s
            GROUP BY IO_ID
        )
        SELECT 
            c.CAMPAIGN_ID, c.CAMPAIGN_NAME,
            c.W_VISITS, c.S_VISITS,
            c.W_VISITS + c.S_VISITS as TOTAL_VISITS,
            c.W_LEADS, c.W_PURCHASES, c.UNIQUE_VISITORS,
            COALESCE(i.IMPRESSIONS, 0) as IMPRESSIONS
        FROM combined c
        LEFT JOIN impressions i ON c.CAMPAIGN_ID = i.IO_ID
        ORDER BY TOTAL_VISITS DESC
        LIMIT 50
        """
    else:
        return jsonify({'success': False, 'error': f'Unknown agency_id: {agency_id}'}), 400
    
    try:
        results = execute_query(query, {
            'advertiser_id': advertiser_id,
            'agency_id': int(agency_id),
            'start_date': start_date,
            'end_date': end_date
        })
        return jsonify({'success': True, 'data': results, 'data_class': agency_class})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

# =============================================================================
# PUBLISHER PERFORMANCE (Class A and ViacomCBS only)
# =============================================================================

@app.route('/api/v3/publisher-performance', methods=['GET'])
def get_publisher_performance_unified():
    """Publisher breakdown - Class A has impressions, ViacomCBS web only"""
    advertiser_id = request.args.get('advertiser_id')
    agency_id = request.args.get('agency_id')
    min_visits = int(request.args.get('min_visits', 10))
    start_date, end_date = get_date_params(request)
    
    if not advertiser_id or not agency_id:
        return jsonify({'success': False, 'error': 'advertiser_id and agency_id required'}), 400
    
    agency_class = get_agency_class(agency_id)
    
    if agency_class == 'B':
        return jsonify({
            'success': True, 
            'data': [], 
            'message': 'Publisher breakdown not available for Class B agencies (Causal iQ, Magnite, etc.)',
            'data_class': agency_class
        })
    
    if agency_class == 'A':
        query = """
        WITH visit_publishers AS (
            SELECT PUBLISHER, MAX(PLATFORM_TYPE) as PT, COUNT(*) as S_VISITS, 
                   COUNT(DISTINCT MAID) as UNIQUE_VISITORS
            FROM QUORUMDB.SEGMENT_DATA.QRM_ALL_VISITS_V3
            WHERE QUORUM_ADVERTISER_ID = %(advertiser_id)s
              AND CONVERSION_DATE >= %(start_date)s AND CONVERSION_DATE <= %(end_date)s
              AND VISIT_TYPE = 'STORE' AND PUBLISHER IS NOT NULL
            GROUP BY PUBLISHER
            HAVING COUNT(*) >= %(min_visits)s
        ),
        publisher_impressions AS (
            SELECT vp.PUBLISHER, COUNT(*) as IMPRESSIONS
            FROM visit_publishers vp
            JOIN QUORUMDB.SEGMENT_DATA.XANDR_IMPRESSION_LOG x 
                ON x.AGENCY_ID = %(agency_id)s
                AND (x.PUBLISHER_CODE = vp.PUBLISHER 
                     OR x.PUBLISHER_CODE = REPLACE(REPLACE(vp.PUBLISHER, '%%2520', ' '), '%%20', ' '))
            WHERE CAST(x.TIMESTAMP AS DATE) >= %(start_date)s 
              AND CAST(x.TIMESTAMP AS DATE) <= %(end_date)s
            GROUP BY vp.PUBLISHER
        )
        SELECT REPLACE(REPLACE(vp.PUBLISHER, '%%2520', ' '), '%%20', ' ') as PUBLISHER,
               vp.PT, COALESCE(pi.IMPRESSIONS, 0) as IMPRESSIONS, vp.S_VISITS, vp.UNIQUE_VISITORS,
               CASE WHEN COALESCE(pi.IMPRESSIONS, 0) > 0 
                    THEN ROUND((vp.S_VISITS::FLOAT / pi.IMPRESSIONS) * 100, 4) ELSE 0 END as VISIT_RATE
        FROM visit_publishers vp
        LEFT JOIN publisher_impressions pi ON vp.PUBLISHER = pi.PUBLISHER
        ORDER BY IMPRESSIONS DESC NULLS LAST
        LIMIT 100
        """
    else:  # VIACOM
        # ViacomCBS: Combine web visits from QRM + store visits from PARAMOUNT
        query = """
        WITH adv_campaigns AS (
            SELECT DISTINCT CAMPAIGN_ID FROM QUORUMDB.SEGMENT_DATA.QRM_ALL_VISITS_V3
            WHERE QUORUM_ADVERTISER_ID = %(advertiser_id)s
              AND CONVERSION_DATE >= %(start_date)s AND CONVERSION_DATE <= %(end_date)s
              AND VISIT_TYPE = 'WEB' AND CAMPAIGN_ID IS NOT NULL
            UNION
            SELECT DISTINCT IO_ID FROM QUORUMDB.SEGMENT_DATA.PARAMOUNT_IMP_STORE_VISITS
            WHERE QUORUM_ADVERTISER_ID = %(advertiser_id)s
              AND DRIVEBYDATE >= %(start_date)s AND DRIVEBYDATE <= %(end_date)s
              AND IO_ID IS NOT NULL
        ),
        web_visits AS (
            SELECT PUBLISHER, MAX(PLATFORM_TYPE) as PT,
                   COUNT(*) as W_VISITS,
                   COUNT(DISTINCT MAID) as W_UNIQUE
            FROM QUORUMDB.SEGMENT_DATA.QRM_ALL_VISITS_V3
            WHERE QUORUM_ADVERTISER_ID = %(advertiser_id)s
              AND CONVERSION_DATE >= %(start_date)s AND CONVERSION_DATE <= %(end_date)s
              AND VISIT_TYPE = 'WEB' AND PUBLISHER IS NOT NULL
            GROUP BY PUBLISHER
        ),
        store_visits AS (
            SELECT SITE as PUBLISHER, MAX(PT) as PT,
                   COUNT(*) as S_VISITS,
                   COUNT(DISTINCT IMP_MAID) as S_UNIQUE
            FROM QUORUMDB.SEGMENT_DATA.PARAMOUNT_IMP_STORE_VISITS
            WHERE QUORUM_ADVERTISER_ID = %(advertiser_id)s
              AND DRIVEBYDATE >= %(start_date)s AND DRIVEBYDATE <= %(end_date)s
              AND SITE IS NOT NULL
            GROUP BY SITE
        ),
        impressions AS (
            SELECT SITE, COUNT(*) as IMPRESSIONS
            FROM QUORUMDB.SEGMENT_DATA.XANDR_IMPRESSION_LOG
            WHERE AGENCY_ID = 1480
              AND IO_ID IN (SELECT CAMPAIGN_ID FROM adv_campaigns)
              AND CAST(TIMESTAMP AS DATE) >= %(start_date)s 
              AND CAST(TIMESTAMP AS DATE) <= %(end_date)s
            GROUP BY SITE
        ),
        combined AS (
            SELECT 
                COALESCE(w.PUBLISHER, s.PUBLISHER) as PUBLISHER,
                COALESCE(w.PT, s.PT) as PT,
                COALESCE(w.W_VISITS, 0) as W_VISITS,
                COALESCE(s.S_VISITS, 0) as S_VISITS,
                COALESCE(w.W_UNIQUE, 0) + COALESCE(s.S_UNIQUE, 0) as UNIQUE_VISITORS
            FROM web_visits w
            FULL OUTER JOIN store_visits s ON w.PUBLISHER = s.PUBLISHER
        )
        SELECT 
            c.PUBLISHER, c.PT,
            c.W_VISITS, c.S_VISITS,
            c.W_VISITS + c.S_VISITS as TOTAL_VISITS,
            c.UNIQUE_VISITORS,
            COALESCE(i.IMPRESSIONS, 0) as IMPRESSIONS
        FROM combined c
        LEFT JOIN impressions i ON c.PUBLISHER = i.SITE
        WHERE c.W_VISITS + c.S_VISITS >= %(min_visits)s
        ORDER BY TOTAL_VISITS DESC
        LIMIT 100
        """
    
    try:
        results = execute_query(query, {
            'advertiser_id': advertiser_id,
            'agency_id': int(agency_id),
            'start_date': start_date,
            'end_date': end_date,
            'min_visits': min_visits
        })
        return jsonify({'success': True, 'data': results, 'data_class': agency_class})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

# =============================================================================
# ZIP PERFORMANCE
# =============================================================================

@app.route('/api/v3/zip-performance', methods=['GET'])
def get_zip_performance_unified():
    """ZIP breakdown - available for all agency classes"""
    advertiser_id = request.args.get('advertiser_id')
    agency_id = request.args.get('agency_id')
    min_visits = int(request.args.get('min_visits', 5))
    start_date, end_date = get_date_params(request)
    
    if not advertiser_id or not agency_id:
        return jsonify({'success': False, 'error': 'advertiser_id and agency_id required'}), 400
    
    agency_class = get_agency_class(agency_id)
    
    if agency_class == 'A':
        query = """
        SELECT 
            mca.ZIP_CODE, 
            zdm.DMA_NAME,
            COUNT(*) as S_VISITS, 
            COUNT(DISTINCT v.MAID) as UNIQUE_VISITORS,
            MAX(mca.ZIP_POPULATION) as ZIP_POPULATION,
            0 as IMPRESSIONS
        FROM QUORUMDB.SEGMENT_DATA.QRM_ALL_VISITS_V3 v
        JOIN QUORUM_CROSS_CLOUD.ATTAIN_FEED.MAID_CENTROID_ASSOCIATION mca 
            ON LOWER(v.MAID) = LOWER(mca.DEVICE_ID)
        LEFT JOIN QUORUMDB.SEGMENT_DATA.ZIP_DMA_MAPPING zdm ON mca.ZIP_CODE = zdm.ZIP_CODE
        WHERE v.QUORUM_ADVERTISER_ID = %(advertiser_id)s
          AND v.CONVERSION_DATE >= %(start_date)s AND v.CONVERSION_DATE <= %(end_date)s
          AND v.VISIT_TYPE = 'STORE'
        GROUP BY mca.ZIP_CODE, zdm.DMA_NAME
        HAVING COUNT(*) >= %(min_visits)s
        ORDER BY S_VISITS DESC
        LIMIT 500
        """
    elif agency_class == 'B':
        query = """
        SELECT 
            mca.ZIP_CODE, 
            zdm.DMA_NAME,
            COUNT(DISTINCT CONCAT(cp.DEVICE_ID, cp.DRIVE_BY_DATE, cp.POI_MD5)) as S_VISITS,
            COUNT(DISTINCT cp.DEVICE_ID) as UNIQUE_VISITORS,
            MAX(mca.ZIP_POPULATION) as ZIP_POPULATION,
            0 as IMPRESSIONS
        FROM QUORUMDB.SEGMENT_DATA.CAMPAIGN_PERFORMANCE_STORE_VISITS_RAW cp
        JOIN QUORUM_CROSS_CLOUD.ATTAIN_FEED.MAID_CENTROID_ASSOCIATION mca 
            ON LOWER(cp.DEVICE_ID) = LOWER(mca.DEVICE_ID)
        LEFT JOIN QUORUMDB.SEGMENT_DATA.ZIP_DMA_MAPPING zdm ON mca.ZIP_CODE = zdm.ZIP_CODE
        WHERE cp.ADVERTISER_ID = %(advertiser_id)s
          AND cp.DRIVE_BY_DATE >= %(start_date)s AND cp.DRIVE_BY_DATE <= %(end_date)s
        GROUP BY mca.ZIP_CODE, zdm.DMA_NAME
        HAVING S_VISITS >= %(min_visits)s
        ORDER BY S_VISITS DESC
        LIMIT 500
        """
    else:  # VIACOM - combining web + store visits
        query = """
        WITH web_visits AS (
            SELECT 
                mca.ZIP_CODE,
                COUNT(*) as W_VISITS,
                COUNT(DISTINCT v.MAID) as W_UNIQUE
            FROM QUORUMDB.SEGMENT_DATA.QRM_ALL_VISITS_V3 v
            JOIN QUORUM_CROSS_CLOUD.ATTAIN_FEED.MAID_CENTROID_ASSOCIATION mca 
                ON LOWER(v.MAID) = LOWER(mca.DEVICE_ID)
            WHERE v.QUORUM_ADVERTISER_ID = %(advertiser_id)s
              AND v.CONVERSION_DATE >= %(start_date)s AND v.CONVERSION_DATE <= %(end_date)s
              AND v.VISIT_TYPE = 'WEB'
            GROUP BY mca.ZIP_CODE
        ),
        store_visits AS (
            SELECT 
                ZIP_CODE,
                COUNT(*) as S_VISITS,
                COUNT(DISTINCT IMP_MAID) as S_UNIQUE
            FROM QUORUMDB.SEGMENT_DATA.PARAMOUNT_IMP_STORE_VISITS
            WHERE QUORUM_ADVERTISER_ID = %(advertiser_id)s
              AND DRIVEBYDATE >= %(start_date)s AND DRIVEBYDATE <= %(end_date)s
              AND ZIP_CODE IS NOT NULL
            GROUP BY ZIP_CODE
        ),
        combined AS (
            SELECT 
                COALESCE(w.ZIP_CODE, s.ZIP_CODE) as ZIP_CODE,
                COALESCE(w.W_VISITS, 0) as W_VISITS,
                COALESCE(s.S_VISITS, 0) as S_VISITS,
                COALESCE(w.W_UNIQUE, 0) + COALESCE(s.S_UNIQUE, 0) as UNIQUE_VISITORS
            FROM web_visits w
            FULL OUTER JOIN store_visits s ON w.ZIP_CODE = s.ZIP_CODE
        )
        SELECT 
            zdm.DMA_NAME,
            c.ZIP_CODE,
            c.W_VISITS, c.S_VISITS,
            c.W_VISITS + c.S_VISITS as TOTAL_VISITS,
            c.UNIQUE_VISITORS,
            0 as ZIP_POPULATION,
            0 as IMPRESSIONS
        FROM combined c
        LEFT JOIN QUORUMDB.SEGMENT_DATA.ZIP_DMA_MAPPING zdm ON c.ZIP_CODE = zdm.ZIP_CODE
        WHERE c.W_VISITS + c.S_VISITS >= %(min_visits)s
        ORDER BY TOTAL_VISITS DESC
        LIMIT 500
        """
    
    try:
        results = execute_query(query, {
            'advertiser_id': advertiser_id,
            'agency_id': int(agency_id),
            'start_date': start_date,
            'end_date': end_date,
            'min_visits': min_visits
        })
        return jsonify({'success': True, 'data': results, 'data_class': agency_class})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

# =============================================================================
# DIAGNOSTIC ENDPOINTS
# =============================================================================

@app.route('/api/v3/unmapped-advertisers', methods=['GET'])
def get_unmapped_advertisers():
    """List Class B advertisers missing IO_ID mapping"""
    start_date, end_date = get_date_params(request)
    
    query = """
    WITH visits AS (
        SELECT cp.AGENCY_ID, cp.ADVERTISER_ID, MAX(aa.COMP_NAME) as ADVERTISER_NAME,
               COUNT(DISTINCT CONCAT(cp.DEVICE_ID, cp.DRIVE_BY_DATE, cp.POI_MD5)) as S_VISITS
        FROM QUORUMDB.SEGMENT_DATA.CAMPAIGN_PERFORMANCE_STORE_VISITS_RAW cp
        JOIN QUORUMDB.SEGMENT_DATA.AGENCY_ADVERTISER aa ON cp.ADVERTISER_ID = aa.ID
        WHERE cp.DRIVE_BY_DATE >= %(start_date)s AND cp.DRIVE_BY_DATE <= %(end_date)s
          AND cp.AGENCY_ID IN (1813, 1972, 2234, 2379, 1880, 2744)
        GROUP BY cp.AGENCY_ID, cp.ADVERTISER_ID
    ),
    mapping_exists AS (
        SELECT DISTINCT QUORUM_ADVERTISER_ID::NUMBER as ADVERTISER_ID
        FROM QUORUMDB.SEGMENT_DATA."QuorumAdvImpMapping"
        WHERE IO_ID IS NOT NULL AND IO_ID != 0
    )
    SELECT v.AGENCY_ID, v.ADVERTISER_ID, v.ADVERTISER_NAME, v.S_VISITS,
           CASE WHEN m.ADVERTISER_ID IS NOT NULL THEN 'MAPPED' ELSE 'UNMAPPED' END as MAPPING_STATUS
    FROM visits v
    LEFT JOIN mapping_exists m ON v.ADVERTISER_ID = m.ADVERTISER_ID
    WHERE m.ADVERTISER_ID IS NULL
    ORDER BY v.S_VISITS DESC
    """
    
    try:
        results = execute_query(query, {'start_date': start_date, 'end_date': end_date})
        for r in results:
            r['AGENCY_NAME'] = AGENCY_NAMES.get(r['AGENCY_ID'], 'Unknown')
        return jsonify({'success': True, 'data': results, 'count': len(results)})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/v3/mapping-coverage', methods=['GET'])
def get_mapping_coverage():
    """Summary of QuorumAdvImpMapping coverage by agency"""
    start_date, end_date = get_date_params(request)
    
    query = """
    WITH visits AS (
        SELECT cp.AGENCY_ID, cp.ADVERTISER_ID,
               COUNT(DISTINCT CONCAT(cp.DEVICE_ID, cp.DRIVE_BY_DATE, cp.POI_MD5)) as S_VISITS
        FROM QUORUMDB.SEGMENT_DATA.CAMPAIGN_PERFORMANCE_STORE_VISITS_RAW cp
        WHERE cp.DRIVE_BY_DATE >= %(start_date)s AND cp.DRIVE_BY_DATE <= %(end_date)s
          AND cp.AGENCY_ID IN (1813, 1972, 2234, 2379, 1880, 2744)
        GROUP BY cp.AGENCY_ID, cp.ADVERTISER_ID
    ),
    mapping_exists AS (
        SELECT DISTINCT QUORUM_ADVERTISER_ID::NUMBER as ADVERTISER_ID
        FROM QUORUMDB.SEGMENT_DATA."QuorumAdvImpMapping"
        WHERE IO_ID IS NOT NULL AND IO_ID != 0
    )
    SELECT 
        v.AGENCY_ID,
        COUNT(*) as TOTAL_ADVERTISERS,
        SUM(CASE WHEN m.ADVERTISER_ID IS NOT NULL THEN 1 ELSE 0 END) as MAPPED,
        SUM(CASE WHEN m.ADVERTISER_ID IS NULL THEN 1 ELSE 0 END) as UNMAPPED,
        SUM(v.S_VISITS) as TOTAL_VISITS,
        SUM(CASE WHEN m.ADVERTISER_ID IS NOT NULL THEN v.S_VISITS ELSE 0 END) as MAPPED_VISITS,
        ROUND(SUM(CASE WHEN m.ADVERTISER_ID IS NOT NULL THEN v.S_VISITS ELSE 0 END)::FLOAT / 
              NULLIF(SUM(v.S_VISITS), 0) * 100, 1) as MAPPED_VISIT_PCT
    FROM visits v
    LEFT JOIN mapping_exists m ON v.ADVERTISER_ID = m.ADVERTISER_ID
    GROUP BY v.AGENCY_ID
    ORDER BY TOTAL_VISITS DESC
    """
    
    try:
        results = execute_query(query, {'start_date': start_date, 'end_date': end_date})
        for r in results:
            r['AGENCY_NAME'] = AGENCY_NAMES.get(r['AGENCY_ID'], 'Unknown')
        return jsonify({'success': True, 'data': results})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

# =============================================================================
# BACKWARD COMPATIBLE ENDPOINTS (v2 style - store visits only)
# =============================================================================

@app.route('/api/agencies', methods=['GET'])
def get_agencies_v2():
    """v2 compatible - redirects to v3"""
    return get_all_agencies()

@app.route('/api/advertisers', methods=['GET'])
def get_advertisers_v2():
    """v2 compatible - Class A only"""
    return get_advertisers_unified()

@app.route('/api/advertiser-summary', methods=['GET'])
def get_advertiser_summary_v2():
    """v2 compatible - redirects to v3"""
    return get_advertiser_summary_unified()

@app.route('/api/campaign-performance', methods=['GET'])
def get_campaign_performance_v2():
    """v2 compatible - redirects to v3"""
    return get_campaign_performance_unified()

@app.route('/api/publisher-performance', methods=['GET'])
def get_publisher_performance_v2():
    """v2 compatible - redirects to v3"""
    return get_publisher_performance_unified()

@app.route('/api/zip-performance', methods=['GET'])
def get_zip_performance_v2():
    """v2 compatible - redirects to v3"""
    return get_zip_performance_unified()

# =============================================================================
# MAIN
# =============================================================================

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=False)
