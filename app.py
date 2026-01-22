"""
Quorum Optimizer API v3.7 - OVERVIEW ENDPOINTS
================================================
Version: 3.7
Updated: January 22, 2026

New Endpoints:
- /api/v3/agency-overview - Cross-agency view with impressions
- /api/v3/advertiser-overview - Cross-advertiser view for specific agency

Architecture:
- Class A (MNTN, Dealer Spike, InteractRV, ARI, ByRider, Level5): QRM_ALL_VISITS_V3
- Class B (Causal iQ, Hearst, Magnite, TeamSnap, Shipyard, Parallel Path): CAMPAIGN_PERFORMANCE_REPORT_DAILY_STATS
- ViacomCBS: QRM_ALL_VISITS_V3 (web) + XANDR_IMPRESSION_LOG (impressions)

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

CLASS_A_AGENCIES = [2514, 1956, 2298, 1955, 1950, 2086]
CLASS_B_AGENCIES = [1813, 1972, 2234, 2379, 1880, 2744]
VIACOM_AGENCY = 1480

AGENCY_NAMES = {
    2514: 'MNTN', 1956: 'Dealer Spike', 2298: 'InteractRV', 
    1955: 'ARI Network Services', 1950: 'ByRider', 2086: 'Level5',
    1813: 'Causal iQ', 1972: 'Hearst', 2234: 'Magnite',
    2379: 'The Shipyard', 1880: 'TeamSnap', 2744: 'Parallel Path',
    1480: 'ViacomCBS WhoSay'
}

def get_agency_class(agency_id):
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
    """Light health check - doesn't test DB to avoid timeout"""
    return jsonify({
        'success': True, 
        'status': 'healthy', 
        'version': '3.7',
        'agencies_supported': 13,
        'new_endpoints': [
            '/api/v3/agency-overview',
            '/api/v3/advertiser-overview',
            '/api/v3/impressions-timeseries',
            '/api/v3/global-advertiser-overview'
        ]
    })

# =============================================================================
# NEW: AGENCY OVERVIEW (Cross-Agency View) - Uses DAILY_ADVERTISER_REPORTING
# =============================================================================

@app.route('/api/v3/agency-overview', methods=['GET'])
def get_agency_overview():
    """Cross-agency overview using pre-aggregated DAILY_ADVERTISER_REPORTING"""
    start_date, end_date = get_date_params(request)
    
    query = """
    SELECT 
        AGENCY_ID,
        AGENCY_NAME,
        COUNT(DISTINCT ADVERTISER_ID) as ADVERTISER_COUNT,
        SUM(IMPRESSIONS) as IMPRESSIONS,
        SUM(TEST_VISITORS) as LOCATION_VISITS,
        SUM(COALESCE(SITE_VISITS, 0)) as WEB_VISITS,
        0 as WEB_TRAFFIC,
        CASE WHEN SUM(IMPRESSIONS) > 0 
             THEN SUM(TEST_VISITORS)::FLOAT / SUM(IMPRESSIONS) ELSE 0 END as LOCATION_VR,
        CASE WHEN SUM(IMPRESSIONS) > 0 
             THEN (SUM(TEST_VISITORS) + SUM(COALESCE(SITE_VISITS, 0)))::FLOAT / SUM(IMPRESSIONS) ELSE 0 END as VISIT_RATE,
        0 as WEB_CONTRIBUTION
    FROM QUORUMDB.SEGMENT_DATA.DAILY_ADVERTISER_REPORTING
    WHERE LOG_DATE >= %(start_date)s AND LOG_DATE <= %(end_date)s
    GROUP BY AGENCY_ID, AGENCY_NAME
    ORDER BY IMPRESSIONS DESC
    """
    
    try:
        results = execute_query(query, {'start_date': start_date, 'end_date': end_date})
        for r in results:
            r['DATA_CLASS'] = get_agency_class(r['AGENCY_ID'])
        return jsonify({'success': True, 'data': results, 'total_agencies': len(results), 'date_range': {'start': start_date, 'end': end_date}})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

# =============================================================================
# NEW: ADVERTISER OVERVIEW (Cross-Advertiser View) - Uses DAILY_ADVERTISER_REPORTING
# =============================================================================

@app.route('/api/v3/advertiser-overview', methods=['GET'])
def get_advertiser_overview():
    """Cross-advertiser overview for a specific agency using pre-aggregated data, only configured advertisers with visits"""
    agency_id = request.args.get('agency_id')
    start_date, end_date = get_date_params(request)
    
    if not agency_id:
        return jsonify({'success': False, 'error': 'agency_id required'}), 400
    
    # For ViacomCBS (1480), filter to only configured advertisers in PARAMOUNT_URL_MAPPING
    if int(agency_id) == 1480:
        query = """
        SELECT 
            d.ADVERTISER_ID,
            d.ADVERTISER_NAME,
            SUM(d.IMPRESSIONS) as IMPRESSIONS,
            SUM(d.TEST_VISITORS) as LOCATION_VISITS,
            SUM(COALESCE(d.SITE_VISITS, 0)) as WEB_VISITS,
            0 as WEB_TRAFFIC,
            CASE WHEN SUM(d.IMPRESSIONS) > 0 
                 THEN SUM(d.TEST_VISITORS)::FLOAT / SUM(d.IMPRESSIONS) ELSE 0 END as LOCATION_VR,
            CASE WHEN SUM(d.IMPRESSIONS) > 0 
                 THEN (SUM(d.TEST_VISITORS) + SUM(COALESCE(d.SITE_VISITS, 0)))::FLOAT / SUM(d.IMPRESSIONS) ELSE 0 END as VISIT_RATE,
            CASE WHEN SUM(d.IMPRESSIONS) > 0 AND SUM(COALESCE(d.SITE_VISITS, 0)) > 0
                 THEN (SUM(COALESCE(d.SITE_VISITS, 0))::FLOAT / SUM(d.IMPRESSIONS)) * 100 ELSE 0 END as WEB_CONTRIBUTION
        FROM QUORUMDB.SEGMENT_DATA.DAILY_ADVERTISER_REPORTING d
        WHERE d.AGENCY_ID = %(agency_id)s
          AND d.LOG_DATE >= %(start_date)s AND d.LOG_DATE <= %(end_date)s
          AND d.ADVERTISER_ID IN (
              SELECT ADVERTISER_ID FROM QUORUMDB.SEGMENT_DATA.PARAMOUNT_URL_MAPPING
          )
        GROUP BY d.ADVERTISER_ID, d.ADVERTISER_NAME
        HAVING SUM(d.TEST_VISITORS) > 0 OR SUM(COALESCE(d.SITE_VISITS, 0)) > 0
        ORDER BY IMPRESSIONS DESC
        """
    else:
        # All other agencies: just filter to those with visits
        query = """
        SELECT 
            ADVERTISER_ID,
            ADVERTISER_NAME,
            SUM(IMPRESSIONS) as IMPRESSIONS,
            SUM(TEST_VISITORS) as LOCATION_VISITS,
            SUM(COALESCE(SITE_VISITS, 0)) as WEB_VISITS,
            0 as WEB_TRAFFIC,
            CASE WHEN SUM(IMPRESSIONS) > 0 
                 THEN SUM(TEST_VISITORS)::FLOAT / SUM(IMPRESSIONS) ELSE 0 END as LOCATION_VR,
            CASE WHEN SUM(IMPRESSIONS) > 0 
                 THEN (SUM(TEST_VISITORS) + SUM(COALESCE(SITE_VISITS, 0)))::FLOAT / SUM(IMPRESSIONS) ELSE 0 END as VISIT_RATE,
            CASE WHEN SUM(IMPRESSIONS) > 0 AND SUM(COALESCE(SITE_VISITS, 0)) > 0
                 THEN (SUM(COALESCE(SITE_VISITS, 0))::FLOAT / SUM(IMPRESSIONS)) * 100 ELSE 0 END as WEB_CONTRIBUTION
        FROM QUORUMDB.SEGMENT_DATA.DAILY_ADVERTISER_REPORTING
        WHERE AGENCY_ID = %(agency_id)s
          AND LOG_DATE >= %(start_date)s AND LOG_DATE <= %(end_date)s
        GROUP BY ADVERTISER_ID, ADVERTISER_NAME
        HAVING SUM(TEST_VISITORS) > 0 OR SUM(COALESCE(SITE_VISITS, 0)) > 0
        ORDER BY IMPRESSIONS DESC
        """
    
    try:
        results = execute_query(query, {'agency_id': int(agency_id), 'start_date': start_date, 'end_date': end_date})
        return jsonify({
            'success': True, 
            'data': results, 
            'agency_id': int(agency_id), 
            'agency_name': AGENCY_NAMES.get(int(agency_id), 'Unknown'), 
            'total_advertisers': len(results), 
            'date_range': {'start': start_date, 'end': end_date}
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

# =============================================================================
# NEW: IMPRESSIONS TIME SERIES (For Chart) - Uses DAILY_ADVERTISER_REPORTING
# =============================================================================

@app.route('/api/v3/impressions-timeseries', methods=['GET'])
def get_impressions_timeseries():
    """Daily impressions by agency for time series chart using pre-aggregated data"""
    start_date, end_date = get_date_params(request)
    
    query = """
    SELECT 
        LOG_DATE,
        AGENCY_ID,
        AGENCY_NAME,
        SUM(IMPRESSIONS) as IMPRESSIONS
    FROM QUORUMDB.SEGMENT_DATA.DAILY_ADVERTISER_REPORTING
    WHERE LOG_DATE >= %(start_date)s AND LOG_DATE <= %(end_date)s
    GROUP BY LOG_DATE, AGENCY_ID, AGENCY_NAME
    ORDER BY LOG_DATE, AGENCY_ID
    """
    
    try:
        results = execute_query(query, {'start_date': start_date, 'end_date': end_date})
        return jsonify({
            'success': True, 
            'data': results,
            'date_range': {'start': start_date, 'end': end_date}
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

# =============================================================================
# NEW: ADVERTISER TIME SERIES (For Agency Overview Chart)
# =============================================================================

@app.route('/api/v3/advertiser-timeseries', methods=['GET'])
def get_advertiser_timeseries():
    """Daily impressions by advertiser for a specific agency - top 20 + Others"""
    agency_id = request.args.get('agency_id')
    start_date, end_date = get_date_params(request)
    
    if not agency_id:
        return jsonify({'success': False, 'error': 'agency_id required'}), 400
    
    query = """
    WITH daily_adv AS (
        SELECT 
            LOG_DATE,
            ADVERTISER_ID,
            ADVERTISER_NAME,
            SUM(IMPRESSIONS) as IMPRESSIONS
        FROM QUORUMDB.SEGMENT_DATA.DAILY_ADVERTISER_REPORTING
        WHERE AGENCY_ID = %(agency_id)s
          AND LOG_DATE >= %(start_date)s AND LOG_DATE <= %(end_date)s
        GROUP BY LOG_DATE, ADVERTISER_ID, ADVERTISER_NAME
    ),
    ranked_advertisers AS (
        SELECT ADVERTISER_ID, ADVERTISER_NAME, SUM(IMPRESSIONS) as TOTAL_IMPRESSIONS,
               ROW_NUMBER() OVER (ORDER BY SUM(IMPRESSIONS) DESC) as rank
        FROM daily_adv
        GROUP BY ADVERTISER_ID, ADVERTISER_NAME
    )
    SELECT 
        d.LOG_DATE,
        CASE WHEN r.rank <= 20 THEN d.ADVERTISER_NAME ELSE 'Others' END as ADVERTISER_NAME,
        SUM(d.IMPRESSIONS) as IMPRESSIONS
    FROM daily_adv d
    JOIN ranked_advertisers r ON d.ADVERTISER_ID = r.ADVERTISER_ID
    GROUP BY d.LOG_DATE, CASE WHEN r.rank <= 20 THEN d.ADVERTISER_NAME ELSE 'Others' END
    ORDER BY d.LOG_DATE, IMPRESSIONS DESC
    """
    
    try:
        results = execute_query(query, {'agency_id': int(agency_id), 'start_date': start_date, 'end_date': end_date})
        return jsonify({
            'success': True, 
            'data': results,
            'agency_id': int(agency_id),
            'date_range': {'start': start_date, 'end': end_date}
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

# =============================================================================
# NEW: GLOBAL ADVERTISER OVERVIEW (All Agencies) - Uses DAILY_ADVERTISER_REPORTING
# =============================================================================

@app.route('/api/v3/global-advertiser-overview', methods=['GET'])
def get_global_advertiser_overview():
    """Cross-advertiser overview across ALL agencies using pre-aggregated data"""
    start_date, end_date = get_date_params(request)
    
    query = """
    SELECT 
        AGENCY_ID,
        AGENCY_NAME,
        ADVERTISER_ID,
        ADVERTISER_NAME,
        SUM(IMPRESSIONS) as IMPRESSIONS,
        SUM(TEST_VISITORS) as LOCATION_VISITS,
        SUM(COALESCE(SITE_VISITS, 0)) as WEB_VISITS,
        0 as WEB_TRAFFIC,
        CASE WHEN SUM(IMPRESSIONS) > 0 
             THEN SUM(TEST_VISITORS)::FLOAT / SUM(IMPRESSIONS) ELSE 0 END as LOCATION_VR,
        CASE WHEN SUM(IMPRESSIONS) > 0 
             THEN (SUM(TEST_VISITORS) + SUM(COALESCE(SITE_VISITS, 0)))::FLOAT / SUM(IMPRESSIONS) ELSE 0 END as VISIT_RATE,
        CASE WHEN SUM(IMPRESSIONS) > 0 AND SUM(COALESCE(SITE_VISITS, 0)) > 0
             THEN (SUM(COALESCE(SITE_VISITS, 0))::FLOAT / SUM(IMPRESSIONS)) * 100 ELSE 0 END as WEB_CONTRIBUTION
    FROM QUORUMDB.SEGMENT_DATA.DAILY_ADVERTISER_REPORTING
    WHERE LOG_DATE >= %(start_date)s AND LOG_DATE <= %(end_date)s
    GROUP BY AGENCY_ID, AGENCY_NAME, ADVERTISER_ID, ADVERTISER_NAME
    ORDER BY IMPRESSIONS DESC
    LIMIT 200
    """
    
    try:
        results = execute_query(query, {'start_date': start_date, 'end_date': end_date})
        return jsonify({
            'success': True, 
            'data': results,
            'total_advertisers': len(results),
            'date_range': {'start': start_date, 'end': end_date}
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

# =============================================================================
# SIDEBAR/LISTING ENDPOINTS - Now using DAILY_ADVERTISER_REPORTING for speed
# =============================================================================

@app.route('/api/v3/agencies', methods=['GET'])
def get_all_agencies():
    """Get agencies for sidebar - uses pre-aggregated data for speed, counts only configured advertisers with visits"""
    start_date, end_date = get_date_params(request)
    
    # For ViacomCBS (1480), we need to filter to only configured advertisers
    # Other agencies: filter to those with actual visits
    query = """
    WITH configured_advertisers AS (
        -- ViacomCBS: Only advertisers in PARAMOUNT_URL_MAPPING (configured for reporting)
        SELECT DISTINCT ADVERTISER_ID 
        FROM QUORUMDB.SEGMENT_DATA.PARAMOUNT_URL_MAPPING
    ),
    advertiser_visits AS (
        SELECT 
            d.AGENCY_ID,
            d.AGENCY_NAME,
            d.ADVERTISER_ID,
            SUM(d.TEST_VISITORS) as S_VISITS,
            SUM(COALESCE(d.SITE_VISITS, 0)) as W_VISITS
        FROM QUORUMDB.SEGMENT_DATA.DAILY_ADVERTISER_REPORTING d
        WHERE d.LOG_DATE >= %(start_date)s AND d.LOG_DATE <= %(end_date)s
        GROUP BY d.AGENCY_ID, d.AGENCY_NAME, d.ADVERTISER_ID
        HAVING SUM(d.TEST_VISITORS) > 0 OR SUM(COALESCE(d.SITE_VISITS, 0)) > 0
    ),
    filtered_advertisers AS (
        SELECT 
            av.AGENCY_ID,
            av.AGENCY_NAME,
            av.ADVERTISER_ID,
            av.S_VISITS,
            av.W_VISITS
        FROM advertiser_visits av
        WHERE 
            -- For ViacomCBS (1480): only include configured advertisers
            (av.AGENCY_ID = 1480 AND av.ADVERTISER_ID IN (SELECT ADVERTISER_ID FROM configured_advertisers))
            -- For all other agencies: include all with visits
            OR av.AGENCY_ID != 1480
    )
    SELECT 
        AGENCY_ID,
        AGENCY_NAME,
        COUNT(DISTINCT ADVERTISER_ID) as ADVERTISER_COUNT,
        SUM(S_VISITS) as S_VISITS,
        SUM(W_VISITS) as W_VISITS
    FROM filtered_advertisers
    GROUP BY AGENCY_ID, AGENCY_NAME
    ORDER BY (SUM(S_VISITS) + SUM(W_VISITS)) DESC
    """
    try:
        results = execute_query(query, {'start_date': start_date, 'end_date': end_date})
        for r in results:
            r['DATA_CLASS'] = get_agency_class(r['AGENCY_ID'])
        return jsonify({'success': True, 'data': results, 'total_agencies': len(results)})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/v3/advertisers', methods=['GET'])
def get_advertisers_unified():
    """Get advertisers for sidebar - uses pre-aggregated data for speed, only configured advertisers with visits"""
    agency_id = request.args.get('agency_id')
    start_date, end_date = get_date_params(request)
    if not agency_id:
        return jsonify({'success': False, 'error': 'agency_id required'}), 400
    
    # For ViacomCBS (1480), filter to only configured advertisers in PARAMOUNT_URL_MAPPING
    if int(agency_id) == 1480:
        query = """
        SELECT 
            d.ADVERTISER_ID,
            d.ADVERTISER_NAME,
            SUM(d.TEST_VISITORS) as S_VISITS,
            SUM(COALESCE(d.SITE_VISITS, 0)) as W_VISITS
        FROM QUORUMDB.SEGMENT_DATA.DAILY_ADVERTISER_REPORTING d
        WHERE d.AGENCY_ID = %(agency_id)s
          AND d.LOG_DATE >= %(start_date)s AND d.LOG_DATE <= %(end_date)s
          AND d.ADVERTISER_ID IN (
              SELECT ADVERTISER_ID FROM QUORUMDB.SEGMENT_DATA.PARAMOUNT_URL_MAPPING
          )
        GROUP BY d.ADVERTISER_ID, d.ADVERTISER_NAME
        HAVING SUM(d.TEST_VISITORS) > 0 OR SUM(COALESCE(d.SITE_VISITS, 0)) > 0
        ORDER BY (SUM(d.TEST_VISITORS) + SUM(COALESCE(d.SITE_VISITS, 0))) DESC
        """
    else:
        # All other agencies: just filter to those with visits
        query = """
        SELECT 
            ADVERTISER_ID,
            ADVERTISER_NAME,
            SUM(TEST_VISITORS) as S_VISITS,
            SUM(COALESCE(SITE_VISITS, 0)) as W_VISITS
        FROM QUORUMDB.SEGMENT_DATA.DAILY_ADVERTISER_REPORTING
        WHERE AGENCY_ID = %(agency_id)s
          AND LOG_DATE >= %(start_date)s AND LOG_DATE <= %(end_date)s
        GROUP BY ADVERTISER_ID, ADVERTISER_NAME
        HAVING SUM(TEST_VISITORS) > 0 OR SUM(COALESCE(SITE_VISITS, 0)) > 0
        ORDER BY (SUM(TEST_VISITORS) + SUM(COALESCE(SITE_VISITS, 0))) DESC
        """
    
    try:
        results = execute_query(query, {'agency_id': int(agency_id), 'start_date': start_date, 'end_date': end_date})
        return jsonify({'success': True, 'data': results, 'agency_id': int(agency_id), 'total_advertisers': len(results)})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

# =============================================================================
# ADVERTISER DETAIL ENDPOINTS (granular - unchanged, uses raw tables)
# =============================================================================

@app.route('/api/v3/advertiser-summary', methods=['GET'])
def get_advertiser_summary_unified():
    advertiser_id = request.args.get('advertiser_id')
    agency_id = request.args.get('agency_id')
    start_date, end_date = get_date_params(request)
    if not advertiser_id or not agency_id:
        return jsonify({'success': False, 'error': 'advertiser_id and agency_id required'}), 400
    agency_class = get_agency_class(agency_id)
    
    if agency_class == 'A':
        query = """
        WITH visit_campaigns AS (
            SELECT CAMPAIGN_ID, COUNT(*) as S_VISITS FROM QUORUMDB.SEGMENT_DATA.QRM_ALL_VISITS_V3
            WHERE QUORUM_ADVERTISER_ID = %(advertiser_id)s AND CONVERSION_DATE >= %(start_date)s AND CONVERSION_DATE <= %(end_date)s AND VISIT_TYPE = 'STORE' AND CAMPAIGN_ID IS NOT NULL
            GROUP BY CAMPAIGN_ID
        ),
        impressions AS (
            SELECT v.CAMPAIGN_ID, COUNT(*) as IMPRESSIONS FROM visit_campaigns v
            JOIN QUORUMDB.SEGMENT_DATA.XANDR_IMPRESSION_LOG x ON x.IO_ID = v.CAMPAIGN_ID
            WHERE x.AGENCY_ID = %(agency_id)s AND CAST(x.TIMESTAMP AS DATE) >= %(start_date)s AND CAST(x.TIMESTAMP AS DATE) <= %(end_date)s
            GROUP BY v.CAMPAIGN_ID
        )
        SELECT COALESCE(SUM(i.IMPRESSIONS), 0) as IMPRESSIONS, SUM(v.S_VISITS) as S_VISITS, 0 as W_VISITS,
               CASE WHEN COALESCE(SUM(i.IMPRESSIONS), 0) > 0 THEN ROUND((SUM(v.S_VISITS)::FLOAT / SUM(i.IMPRESSIONS)) * 100, 4) ELSE 0 END as VISIT_RATE, 'STORE' as VISIT_TYPE
        FROM visit_campaigns v LEFT JOIN impressions i ON v.CAMPAIGN_ID = i.CAMPAIGN_ID
        """
    elif agency_class == 'B':
        query = """
        SELECT COALESCE(SUM(IMPRESSIONS), 0) as IMPRESSIONS, COALESCE(SUM(VISITORS), 0) as S_VISITS, 0 as W_VISITS,
               CASE WHEN COALESCE(SUM(IMPRESSIONS), 0) > 0 THEN ROUND((SUM(VISITORS)::FLOAT / SUM(IMPRESSIONS)) * 100, 4) ELSE 0 END as VISIT_RATE, 'STORE' as VISIT_TYPE
        FROM QUORUMDB.SEGMENT_DATA.CAMPAIGN_PERFORMANCE_REPORT_DAILY_STATS
        WHERE ADVERTISER_ID = %(advertiser_id)s AND LOG_DATE >= %(start_date)s AND LOG_DATE <= %(end_date)s
        """
    elif agency_class == 'VIACOM':
        query = """
        WITH visit_campaigns AS (
            SELECT CAMPAIGN_ID, COUNT(*) as W_VISITS FROM QUORUMDB.SEGMENT_DATA.QRM_ALL_VISITS_V3
            WHERE QUORUM_ADVERTISER_ID = %(advertiser_id)s AND CONVERSION_DATE >= %(start_date)s AND CONVERSION_DATE <= %(end_date)s AND VISIT_TYPE = 'WEB' AND CAMPAIGN_ID IS NOT NULL
            GROUP BY CAMPAIGN_ID
        ),
        impressions AS (
            SELECT IO_ID, COUNT(*) as IMPRESSIONS FROM QUORUMDB.SEGMENT_DATA.XANDR_IMPRESSION_LOG
            WHERE AGENCY_ID = 1480 AND IO_ID IN (SELECT CAMPAIGN_ID FROM visit_campaigns) AND CAST(TIMESTAMP AS DATE) >= %(start_date)s AND CAST(TIMESTAMP AS DATE) <= %(end_date)s
            GROUP BY IO_ID
        )
        SELECT COALESCE(SUM(i.IMPRESSIONS), 0) as IMPRESSIONS, 0 as S_VISITS, SUM(v.W_VISITS) as W_VISITS,
               CASE WHEN COALESCE(SUM(i.IMPRESSIONS), 0) > 0 THEN ROUND((SUM(v.W_VISITS)::FLOAT / SUM(i.IMPRESSIONS)) * 100, 4) ELSE 0 END as VISIT_RATE, 'WEB' as VISIT_TYPE
        FROM visit_campaigns v LEFT JOIN impressions i ON v.CAMPAIGN_ID = i.IO_ID
        """
    else:
        return jsonify({'success': False, 'error': f'Unknown agency_id: {agency_id}'}), 400
    
    try:
        results = execute_query(query, {'advertiser_id': str(advertiser_id), 'agency_id': int(agency_id), 'start_date': start_date, 'end_date': end_date})
        return jsonify({'success': True, 'data': results[0] if results else {}, 'data_class': agency_class})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/v3/campaign-performance', methods=['GET'])
def get_campaign_performance_unified():
    advertiser_id = request.args.get('advertiser_id')
    agency_id = request.args.get('agency_id')
    start_date, end_date = get_date_params(request)
    if not advertiser_id or not agency_id:
        return jsonify({'success': False, 'error': 'advertiser_id and agency_id required'}), 400
    agency_class = get_agency_class(agency_id)
    
    if agency_class == 'A':
        query = """
        WITH visit_campaigns AS (
            SELECT CAMPAIGN_ID, CAMPAIGN_NAME, COUNT(*) as S_VISITS, 0 as W_VISITS FROM QUORUMDB.SEGMENT_DATA.QRM_ALL_VISITS_V3
            WHERE QUORUM_ADVERTISER_ID = %(advertiser_id)s AND CONVERSION_DATE >= %(start_date)s AND CONVERSION_DATE <= %(end_date)s AND VISIT_TYPE = 'STORE' AND CAMPAIGN_ID IS NOT NULL
            GROUP BY CAMPAIGN_ID, CAMPAIGN_NAME
        ),
        campaign_impressions AS (
            SELECT IO_ID as CAMPAIGN_ID, COUNT(*) as IMPRESSIONS FROM QUORUMDB.SEGMENT_DATA.XANDR_IMPRESSION_LOG
            WHERE AGENCY_ID = %(agency_id)s AND CAST(TIMESTAMP AS DATE) >= %(start_date)s AND CAST(TIMESTAMP AS DATE) <= %(end_date)s
            GROUP BY IO_ID
        )
        SELECT v.CAMPAIGN_ID, v.CAMPAIGN_NAME, v.S_VISITS, v.W_VISITS, COALESCE(i.IMPRESSIONS, 0) as IMPRESSIONS
        FROM visit_campaigns v LEFT JOIN campaign_impressions i ON v.CAMPAIGN_ID = i.CAMPAIGN_ID ORDER BY IMPRESSIONS DESC
        """
    elif agency_class == 'B':
        query = """
        SELECT IO_ID::TEXT as CAMPAIGN_ID, IO_NAME as CAMPAIGN_NAME, SUM(VISITORS) as S_VISITS, 0 as W_VISITS, SUM(IMPRESSIONS) as IMPRESSIONS
        FROM QUORUMDB.SEGMENT_DATA.CAMPAIGN_PERFORMANCE_REPORT_DAILY_STATS
        WHERE ADVERTISER_ID = %(advertiser_id)s AND LOG_DATE >= %(start_date)s AND LOG_DATE <= %(end_date)s
        GROUP BY IO_ID, IO_NAME ORDER BY IMPRESSIONS DESC
        """
    elif agency_class == 'VIACOM':
        query = """
        WITH visit_campaigns AS (
            SELECT CAMPAIGN_ID, CAMPAIGN_NAME, 0 as S_VISITS, COUNT(*) as W_VISITS FROM QUORUMDB.SEGMENT_DATA.QRM_ALL_VISITS_V3
            WHERE QUORUM_ADVERTISER_ID = %(advertiser_id)s AND CONVERSION_DATE >= %(start_date)s AND CONVERSION_DATE <= %(end_date)s AND VISIT_TYPE = 'WEB' AND CAMPAIGN_ID IS NOT NULL
            GROUP BY CAMPAIGN_ID, CAMPAIGN_NAME
        ),
        campaign_impressions AS (
            SELECT IO_ID, COUNT(*) as IMPRESSIONS FROM QUORUMDB.SEGMENT_DATA.XANDR_IMPRESSION_LOG
            WHERE AGENCY_ID = 1480 AND CAST(TIMESTAMP AS DATE) >= %(start_date)s AND CAST(TIMESTAMP AS DATE) <= %(end_date)s
            GROUP BY IO_ID
        )
        SELECT v.CAMPAIGN_ID, v.CAMPAIGN_NAME, v.S_VISITS, v.W_VISITS, COALESCE(i.IMPRESSIONS, 0) as IMPRESSIONS
        FROM visit_campaigns v LEFT JOIN campaign_impressions i ON v.CAMPAIGN_ID = i.IO_ID ORDER BY W_VISITS DESC
        """
    else:
        return jsonify({'success': False, 'error': f'Unknown agency_id: {agency_id}'}), 400
    
    try:
        results = execute_query(query, {'advertiser_id': str(advertiser_id), 'agency_id': int(agency_id), 'start_date': start_date, 'end_date': end_date})
        return jsonify({'success': True, 'data': results})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/v3/publisher-performance', methods=['GET'])
def get_publisher_performance_unified():
    advertiser_id = request.args.get('advertiser_id')
    agency_id = request.args.get('agency_id')
    start_date, end_date = get_date_params(request)
    if not advertiser_id or not agency_id:
        return jsonify({'success': False, 'error': 'advertiser_id and agency_id required'}), 400
    agency_class = get_agency_class(agency_id)
    
    if agency_class in ['A', 'VIACOM']:
        visit_type = 'WEB' if agency_class == 'VIACOM' else 'STORE'
        query = f"""
        WITH visit_publishers AS (
            SELECT PUBLISHER as PUBLISHER_CODE, COUNT(*) as VISITS FROM QUORUMDB.SEGMENT_DATA.QRM_ALL_VISITS_V3
            WHERE QUORUM_ADVERTISER_ID = %(advertiser_id)s AND CONVERSION_DATE >= %(start_date)s AND CONVERSION_DATE <= %(end_date)s AND VISIT_TYPE = '{visit_type}' AND PUBLISHER IS NOT NULL
            GROUP BY PUBLISHER
        ),
        publisher_impressions AS (
            SELECT SITE_DOMAIN as PUBLISHER_CODE, COUNT(*) as IMPRESSIONS FROM QUORUMDB.SEGMENT_DATA.XANDR_IMPRESSION_LOG
            WHERE AGENCY_ID = %(agency_id)s AND CAST(TIMESTAMP AS DATE) >= %(start_date)s AND CAST(TIMESTAMP AS DATE) <= %(end_date)s AND SITE_DOMAIN IS NOT NULL
            GROUP BY SITE_DOMAIN
        )
        SELECT COALESCE(v.PUBLISHER_CODE, i.PUBLISHER_CODE) as PUBLISHER_CODE,
               COALESCE(v.VISITS, 0) as {'W_VISITS' if visit_type == 'WEB' else 'S_VISITS'},
               0 as {'S_VISITS' if visit_type == 'WEB' else 'W_VISITS'},
               COALESCE(i.IMPRESSIONS, 0) as IMPRESSIONS
        FROM visit_publishers v FULL OUTER JOIN publisher_impressions i ON v.PUBLISHER_CODE = i.PUBLISHER_CODE
        WHERE COALESCE(i.IMPRESSIONS, 0) > 0 OR COALESCE(v.VISITS, 0) > 0 ORDER BY IMPRESSIONS DESC
        """
    elif agency_class == 'B':
        query = """
        SELECT SITE_NAME as PUBLISHER_CODE, SUM(VISITORS) as S_VISITS, 0 as W_VISITS, SUM(IMPRESSIONS) as IMPRESSIONS
        FROM QUORUMDB.SEGMENT_DATA.CAMPAIGN_PERFORMANCE_REPORT_DAILY_STATS
        WHERE ADVERTISER_ID = %(advertiser_id)s AND LOG_DATE >= %(start_date)s AND LOG_DATE <= %(end_date)s AND SITE_NAME IS NOT NULL
        GROUP BY SITE_NAME ORDER BY IMPRESSIONS DESC
        """
    else:
        return jsonify({'success': False, 'error': f'Unknown agency_id: {agency_id}'}), 400
    
    try:
        results = execute_query(query, {'advertiser_id': str(advertiser_id), 'agency_id': int(agency_id), 'start_date': start_date, 'end_date': end_date})
        return jsonify({'success': True, 'data': results})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/v3/zip-performance', methods=['GET'])
def get_zip_performance_unified():
    advertiser_id = request.args.get('advertiser_id')
    agency_id = request.args.get('agency_id')
    start_date, end_date = get_date_params(request)
    if not advertiser_id or not agency_id:
        return jsonify({'success': False, 'error': 'advertiser_id and agency_id required'}), 400
    agency_class = get_agency_class(agency_id)
    
    if agency_class == 'A':
        query = """
        WITH zip_visits AS (
            SELECT mca.POSTAL_CODE as ZIP_CODE, dma.DMA_NAME, COUNT(*) as S_VISITS, 0 as W_VISITS
            FROM QUORUMDB.SEGMENT_DATA.QRM_ALL_VISITS_V3 v
            JOIN QUORUMDB.SEGMENT_DATA.MAID_CENTROID_ASSOCIATION mca ON UPPER(REPLACE(v.MAID, '-', '')) = UPPER(REPLACE(mca.DEVICE_ID, '-', ''))
            LEFT JOIN QUORUMDB.SEGMENT_DATA.ZIP_TO_DMA dma ON mca.POSTAL_CODE = dma.ZIP_CODE
            WHERE v.QUORUM_ADVERTISER_ID = %(advertiser_id)s AND v.AGENCY_ID = %(agency_id)s AND v.CONVERSION_DATE >= %(start_date)s AND v.CONVERSION_DATE <= %(end_date)s AND v.VISIT_TYPE = 'STORE'
            GROUP BY mca.POSTAL_CODE, dma.DMA_NAME
        )
        SELECT ZIP_CODE, DMA_NAME, S_VISITS, W_VISITS, 0 as IMPRESSIONS FROM zip_visits ORDER BY S_VISITS DESC
        """
    elif agency_class == 'B':
        query = """
        SELECT cp.ZIP_CODE, zdma.DMA_NAME, COUNT(DISTINCT CONCAT(cp.DEVICE_ID, cp.DRIVE_BY_DATE, cp.POI_MD5)) as S_VISITS, 0 as W_VISITS, 0 as IMPRESSIONS
        FROM QUORUMDB.SEGMENT_DATA.CAMPAIGN_PERFORMANCE_STORE_VISITS_RAW cp
        LEFT JOIN QUORUMDB.SEGMENT_DATA.ZIP_TO_DMA zdma ON cp.ZIP_CODE = zdma.ZIP_CODE
        WHERE cp.ADVERTISER_ID = %(advertiser_id)s AND cp.DRIVE_BY_DATE >= %(start_date)s AND cp.DRIVE_BY_DATE <= %(end_date)s
        GROUP BY cp.ZIP_CODE, zdma.DMA_NAME ORDER BY S_VISITS DESC
        """
    elif agency_class == 'VIACOM':
        query = """
        WITH zip_visits AS (
            SELECT mca.POSTAL_CODE as ZIP_CODE, dma.DMA_NAME, 0 as S_VISITS, COUNT(*) as W_VISITS
            FROM QUORUMDB.SEGMENT_DATA.QRM_ALL_VISITS_V3 v
            JOIN QUORUMDB.SEGMENT_DATA.MAID_CENTROID_ASSOCIATION mca ON UPPER(REPLACE(v.MAID, '-', '')) = UPPER(REPLACE(mca.DEVICE_ID, '-', ''))
            LEFT JOIN QUORUMDB.SEGMENT_DATA.ZIP_TO_DMA dma ON mca.POSTAL_CODE = dma.ZIP_CODE
            WHERE v.QUORUM_ADVERTISER_ID = %(advertiser_id)s AND v.CONVERSION_DATE >= %(start_date)s AND v.CONVERSION_DATE <= %(end_date)s AND v.VISIT_TYPE = 'WEB'
            GROUP BY mca.POSTAL_CODE, dma.DMA_NAME
        )
        SELECT ZIP_CODE, DMA_NAME, S_VISITS, W_VISITS, 0 as IMPRESSIONS FROM zip_visits ORDER BY W_VISITS DESC
        """
    else:
        return jsonify({'success': False, 'error': f'Unknown agency_id: {agency_id}'}), 400
    
    try:
        results = execute_query(query, {'advertiser_id': str(advertiser_id), 'agency_id': int(agency_id), 'start_date': start_date, 'end_date': end_date})
        return jsonify({'success': True, 'data': results})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

# Backward compatible endpoints
@app.route('/api/agencies', methods=['GET'])
def get_agencies_v2(): return get_all_agencies()

@app.route('/api/advertisers', methods=['GET'])
def get_advertisers_v2(): return get_advertisers_unified()

@app.route('/api/advertiser-summary', methods=['GET'])
def get_advertiser_summary_v2(): return get_advertiser_summary_unified()

@app.route('/api/campaign-performance', methods=['GET'])
def get_campaign_performance_v2(): return get_campaign_performance_unified()

@app.route('/api/publisher-performance', methods=['GET'])
def get_publisher_performance_v2(): return get_publisher_performance_unified()

@app.route('/api/zip-performance', methods=['GET'])
def get_zip_performance_v2(): return get_zip_performance_unified()

@app.route('/api/web/advertisers', methods=['GET'])
def get_web_advertisers(): return get_advertisers_unified()

@app.route('/api/web/summary', methods=['GET'])
def get_web_summary(): return get_advertiser_summary_unified()

@app.route('/api/web/campaign-performance', methods=['GET'])
def get_web_campaign_performance(): return get_campaign_performance_unified()

@app.route('/api/web/publisher-performance', methods=['GET'])
def get_web_publisher_performance(): return get_publisher_performance_unified()

@app.route('/api/web/zip-performance', methods=['GET'])
def get_web_zip_performance(): return get_zip_performance_unified()

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=False)
