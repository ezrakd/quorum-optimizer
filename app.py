"""
Quorum Optimizer API v5 - WEB VISIT FIX v3 (Performance + Accuracy)
===================================================================
FIX v3: Uses APPROX_COUNT_DISTINCT on PARAMOUNT_IMPRESSIONS_REPORT_90_DAYS
for web/store visit counts. Handles 100M rows in seconds with <2% error.
No pre-aggregated tables needed.

Root cause: PARAMOUNT_DASHBOARD_SUMMARY_STATS.SITE_VISITS was inflated
(counting total site visitors, not CTV-attributed web visits).
Lean RX: was 13.5% web VR (broken), now ~1.4% (correct, matches drill-down).

Changed endpoints (APPROX_COUNT_DISTINCT on impression report):
  - /api/v5/agencies (Paramount section) — all-advertiser scan, uses APPROX
  - /api/v5/advertisers (Paramount branch) — all-advertiser scan, uses APPROX
  - /api/v5/summary (Paramount branch) — single-advertiser, uses exact COUNT
  - /api/v5/timeseries (Paramount branch) — single-advertiser, uses exact COUNT

New endpoints:
  - /api/v5/creative-performance — creative breakdown with bounce rate + avg pages

Unchanged endpoints (already used impression report correctly):
  - /api/v5/campaign-performance
  - /api/v5/lineitem-performance
  - /api/v5/publisher-performance
  - /api/v5/zip-performance
  - /api/v5/dma-performance
  - /api/v5/lift-analysis
  - /api/v5/traffic-sources
  - /api/v5/optimize / optimize-geo
"""
from flask import Flask, jsonify, request
from flask_cors import CORS
import snowflake.connector
import os
from datetime import datetime, timedelta, date
import re
import time
import threading

app = Flask(__name__)
CORS(app)

# =============================================================================
# CONFIGURATION
# =============================================================================
AGENCY_CONFIG = {
    1480: {'name': 'Paramount', 'class': 'PARAMOUNT'},
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

# =============================================================================
# IN-MEMORY CACHE for slow endpoints (traffic-sources scans 310M row table)
# =============================================================================
_cache = {}
_cache_lock = threading.Lock()
CACHE_TTL = 600  # 10 minutes

def cache_get(key):
    with _cache_lock:
        entry = _cache.get(key)
        if entry and time.time() - entry['ts'] < CACHE_TTL:
            return entry['data']
        elif entry:
            del _cache[key]
    return None

def cache_set(key, data):
    with _cache_lock:
        _cache[key] = {'data': data, 'ts': time.time()}
        # Evict old entries if cache gets too large
        if len(_cache) > 200:
            cutoff = time.time() - CACHE_TTL
            expired = [k for k, v in _cache.items() if v['ts'] < cutoff]
            for k in expired:
                del _cache[k]

def get_agency_name(agency_id):
    config = AGENCY_CONFIG.get(int(agency_id))
    return config['name'] if config else f"Agency {agency_id}"

def get_agency_class(agency_id):
    config = AGENCY_CONFIG.get(int(agency_id))
    return config['class'] if config else 'B'

def get_snowflake_connection(retries=2):
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
                insecure_mode=True
            )
        except Exception as e:
            last_err = e
            if attempt < retries and ('certificate' in str(e).lower() or '254007' in str(e)):
                import time
                time.sleep(1)
                continue
            raise

def get_date_range():
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
        'version': '5.13-pipeline-health',
        'description': 'Added pipeline health dashboard: cross-client monitoring for impressions, store visits, web pixels',
        'endpoints': [
            '/api/v5/agencies', '/api/v5/advertisers', '/api/v5/campaigns',
            '/api/v5/lineitems', '/api/v5/creative-performance', '/api/v5/publishers',
            '/api/v5/zip-performance', '/api/v5/dma-performance', '/api/v5/summary',
            '/api/v5/timeseries', '/api/v5/lift-analysis', '/api/v5/traffic-sources',
            '/api/v5/optimize', '/api/v5/agency-timeseries', '/api/v5/advertiser-timeseries',
            '/api/v5/pipeline-health'
        ]
    })

# =============================================================================
# AGENCY OVERVIEW  [FIXED: Paramount web visits]
# =============================================================================
@app.route('/api/v5/agencies', methods=['GET'])
def get_agencies():
    try:
        start_date, end_date = get_date_range()
        conn = get_snowflake_connection()
        cursor = conn.cursor()

        all_results = []

        # Class B agencies — unchanged (web visits already hardcoded to 0)
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

        # FIXED v4: APPROX_COUNT_DISTINCT(CACHE_BUSTER) for correct impression count
        query_paramount = """
            SELECT
                1480 as AGENCY_ID,
                APPROX_COUNT_DISTINCT(CACHE_BUSTER) as IMPRESSIONS,
                APPROX_COUNT_DISTINCT(CASE WHEN IS_STORE_VISIT = 'TRUE' THEN IMP_MAID END) as STORE_VISITS,
                APPROX_COUNT_DISTINCT(CASE WHEN IS_SITE_VISIT = 'TRUE' THEN IP END) as WEB_VISITS,
                APPROX_COUNT_DISTINCT(QUORUM_ADVERTISER_ID) as ADVERTISER_COUNT
            FROM QUORUMDB.SEGMENT_DATA.PARAMOUNT_IMPRESSIONS_REPORT_90_DAYS
            WHERE IMP_DATE BETWEEN %(start_date)s AND %(end_date)s
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
# ADVERTISER OVERVIEW  [FIXED: Paramount web visits]
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
            # FIXED v4: APPROX_COUNT_DISTINCT(CACHE_BUSTER) for correct impression count
            query = """
                WITH imp AS (
                    SELECT
                        CAST(QUORUM_ADVERTISER_ID AS INTEGER) as ADVERTISER_ID,
                        APPROX_COUNT_DISTINCT(CACHE_BUSTER) as IMPRESSIONS,
                        APPROX_COUNT_DISTINCT(CASE WHEN IS_STORE_VISIT = 'TRUE' THEN IMP_MAID END) as STORE_VISITS,
                        APPROX_COUNT_DISTINCT(CASE WHEN IS_SITE_VISIT = 'TRUE' THEN IP END) as WEB_VISITS
                    FROM QUORUMDB.SEGMENT_DATA.PARAMOUNT_IMPRESSIONS_REPORT_90_DAYS
                    WHERE IMP_DATE BETWEEN %(start_date)s AND %(end_date)s
                    GROUP BY QUORUM_ADVERTISER_ID
                    HAVING COUNT(*) > 0
                ),
                names AS (
                    SELECT QUORUM_ADVERTISER_ID, MAX(ADVERTISER_NAME) as ADVERTISER_NAME
                    FROM QUORUMDB.SEGMENT_DATA.PARAMOUNT_DASHBOARD_SUMMARY_STATS
                    WHERE DATE BETWEEN %(start_date)s AND %(end_date)s
                    GROUP BY QUORUM_ADVERTISER_ID
                )
                SELECT
                    i.ADVERTISER_ID,
                    COALESCE(n.ADVERTISER_NAME, 'Advertiser ' || i.ADVERTISER_ID) as ADVERTISER_NAME,
                    i.IMPRESSIONS,
                    i.STORE_VISITS,
                    i.WEB_VISITS
                FROM imp i
                LEFT JOIN names n ON i.ADVERTISER_ID = n.QUORUM_ADVERTISER_ID
                ORDER BY i.IMPRESSIONS DESC
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
# CAMPAIGN PERFORMANCE (unchanged — already used impression report correctly)
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
                    COUNT(DISTINCT CACHE_BUSTER) as IMPRESSIONS,
                    COUNT(DISTINCT CASE WHEN IS_STORE_VISIT = 'TRUE' THEN IMP_MAID END) as STORE_VISITS,
                    COUNT(DISTINCT CASE WHEN IS_SITE_VISIT = 'TRUE' THEN IP END) as WEB_VISITS
                FROM QUORUMDB.SEGMENT_DATA.PARAMOUNT_IMPRESSIONS_REPORT_90_DAYS
                WHERE QUORUM_ADVERTISER_ID = %(advertiser_id)s
                  AND IMP_DATE BETWEEN %(start_date)s AND %(end_date)s
                GROUP BY IO_ID
                HAVING COUNT(DISTINCT CACHE_BUSTER) >= 100
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
# LINE ITEM PERFORMANCE (unchanged)
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
                    COUNT(DISTINCT CACHE_BUSTER) as IMPRESSIONS,
                    COUNT(DISTINCT CASE WHEN IS_STORE_VISIT = 'TRUE' THEN IMP_MAID END) as STORE_VISITS,
                    COUNT(DISTINCT CASE WHEN IS_SITE_VISIT = 'TRUE' THEN IP END) as WEB_VISITS,
                    'Paramount' as PLATFORM
                FROM QUORUMDB.SEGMENT_DATA.PARAMOUNT_IMPRESSIONS_REPORT_90_DAYS
                WHERE QUORUM_ADVERTISER_ID = %(advertiser_id)s
                  AND IMP_DATE BETWEEN %(start_date)s AND %(end_date)s
                  {campaign_filter}
                GROUP BY LINEITEM_ID
                HAVING COUNT(DISTINCT CACHE_BUSTER) >= 100
                ORDER BY COUNT(DISTINCT CACHE_BUSTER) DESC
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
# CREATIVE PERFORMANCE (NEW — between Line Items and Publishers)
# =============================================================================
@app.route('/api/v5/creative-performance', methods=['GET'])
def get_creative_performance():
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
            paramount_filters = ""
            if campaign_id: paramount_filters += f" AND IO_ID = '{campaign_id}'"
            if lineitem_id: paramount_filters += f" AND LINEITEM_ID = '{lineitem_id}'"

            query = f"""
                WITH creative_base AS (
                    SELECT
                        CREATIVE_ID,
                        MAX(CREATIVE_NAME) as CREATIVE_NAME,
                        COUNT(DISTINCT CACHE_BUSTER) as IMPRESSIONS,
                        COUNT(DISTINCT CASE WHEN IS_STORE_VISIT = 'TRUE' THEN IMP_MAID END) as STORE_VISITS,
                        COUNT(DISTINCT CASE WHEN IS_SITE_VISIT = 'TRUE' THEN IP END) as WEB_VISITS
                    FROM QUORUMDB.SEGMENT_DATA.PARAMOUNT_IMPRESSIONS_REPORT_90_DAYS
                    WHERE QUORUM_ADVERTISER_ID = %(advertiser_id)s
                      AND IMP_DATE BETWEEN %(start_date)s AND %(end_date)s
                      {paramount_filters}
                    GROUP BY CREATIVE_ID
                    HAVING COUNT(DISTINCT CACHE_BUSTER) >= 100
                ),
                bounce_data AS (
                    SELECT
                        CREATIVE_ID,
                        IMP_MAID,
                        COUNT(DISTINCT WEB_IMPRESSION_ID) as page_views
                    FROM QUORUMDB.SEGMENT_DATA.PARAMOUNT_IMPRESSIONS_REPORT_90_DAYS
                    WHERE QUORUM_ADVERTISER_ID = %(advertiser_id)s
                      AND IMP_DATE BETWEEN %(start_date)s AND %(end_date)s
                      AND IS_SITE_VISIT = 'TRUE'
                      AND WEB_IMPRESSION_ID IS NOT NULL AND WEB_IMPRESSION_ID != ''
                      {paramount_filters}
                    GROUP BY CREATIVE_ID, IMP_MAID
                ),
                bounce_agg AS (
                    SELECT
                        CREATIVE_ID,
                        COUNT(*) as TOTAL_WEB_VISITORS,
                        SUM(CASE WHEN page_views = 1 THEN 1 ELSE 0 END) as BOUNCED_VISITORS,
                        ROUND(AVG(page_views), 2) as AVG_PAGES_PER_VISITOR
                    FROM bounce_data
                    GROUP BY CREATIVE_ID
                )
                SELECT
                    cb.CREATIVE_ID,
                    cb.CREATIVE_NAME,
                    cb.IMPRESSIONS,
                    cb.STORE_VISITS,
                    cb.WEB_VISITS,
                    COALESCE(ba.BOUNCED_VISITORS, 0) as BOUNCED_VISITORS,
                    COALESCE(ba.AVG_PAGES_PER_VISITOR, 0) as AVG_PAGES_PER_VISITOR,
                    ROUND(COALESCE(ba.BOUNCED_VISITORS, 0) * 100.0 / NULLIF(COALESCE(ba.TOTAL_WEB_VISITORS, 0), 0), 1) as BOUNCE_RATE
                FROM creative_base cb
                LEFT JOIN bounce_agg ba ON cb.CREATIVE_ID = ba.CREATIVE_ID
                ORDER BY cb.IMPRESSIONS DESC
                LIMIT 100
            """
        else:
            # Class B: creative data from Xandr impression log (impressions only)
            classb_filters = ""
            if campaign_id: classb_filters += f" AND IO_ID = {campaign_id}"
            if lineitem_id: classb_filters += f" AND LINEITEM_ID = '{lineitem_id}'"

            query = f"""
                SELECT
                    CREATIVE_ID,
                    MAX(CREATIVE_NAME) as CREATIVE_NAME,
                    COUNT(*) as IMPRESSIONS,
                    0 as STORE_VISITS,
                    0 as WEB_VISITS,
                    0 as BOUNCED_VISITORS,
                    0 as AVG_PAGES_PER_VISITOR,
                    NULL as BOUNCE_RATE
                FROM QUORUMDB.SEGMENT_DATA.XANDR_IMPRESSION_LOG
                WHERE AGENCY_ID = %(agency_id)s
                  AND TIMESTAMP::DATE BETWEEN %(start_date)s AND %(end_date)s
                  {classb_filters}
                GROUP BY CREATIVE_ID
                HAVING COUNT(*) >= 100
                ORDER BY IMPRESSIONS DESC
                LIMIT 100
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
# PUBLISHER PERFORMANCE (unchanged)
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
                SELECT SITE as PUBLISHER, COUNT(DISTINCT CACHE_BUSTER) as IMPRESSIONS,
                    COUNT(DISTINCT CASE WHEN IS_STORE_VISIT = 'TRUE' THEN IMP_MAID END) as STORE_VISITS,
                    COUNT(DISTINCT CASE WHEN IS_SITE_VISIT = 'TRUE' THEN IP END) as WEB_VISITS
                FROM QUORUMDB.SEGMENT_DATA.PARAMOUNT_IMPRESSIONS_REPORT_90_DAYS
                WHERE QUORUM_ADVERTISER_ID = %(advertiser_id)s
                  AND IMP_DATE BETWEEN %(start_date)s AND %(end_date)s {paramount_filters}
                GROUP BY SITE HAVING COUNT(DISTINCT CACHE_BUSTER) >= 100 ORDER BY 2 DESC LIMIT 50
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
# GEOGRAPHIC / ZIP PERFORMANCE (unchanged)
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
                WITH zip_dma AS (
                    SELECT ZIPCODE, MAX(DMA_NAME) as DMA_NAME
                    FROM QUORUMDB.SEGMENT_DATA.DBIP_LOOKUP_US
                    WHERE DMA_NAME IS NOT NULL AND DMA_NAME != ''
                    GROUP BY ZIPCODE
                )
                SELECT p.ZIP_CODE, COALESCE(d.DMA_NAME, 'Unknown') as DMA_NAME, COUNT(DISTINCT p.CACHE_BUSTER) as IMPRESSIONS,
                    COUNT(DISTINCT CASE WHEN p.IS_STORE_VISIT = 'TRUE' THEN p.IMP_MAID END) as STORE_VISITS,
                    COUNT(DISTINCT CASE WHEN p.IS_SITE_VISIT = 'TRUE' THEN p.IP END) as WEB_VISITS
                FROM QUORUMDB.SEGMENT_DATA.PARAMOUNT_IMPRESSIONS_REPORT_90_DAYS p
                LEFT JOIN zip_dma d ON p.ZIP_CODE = d.ZIPCODE
                WHERE p.QUORUM_ADVERTISER_ID = %(advertiser_id)s
                  AND p.IMP_DATE BETWEEN %(start_date)s AND %(end_date)s
                  AND p.ZIP_CODE IS NOT NULL AND p.ZIP_CODE != '' AND p.ZIP_CODE != 'null' AND p.ZIP_CODE != 'UNKNOWN'
                  {filters}
                GROUP BY p.ZIP_CODE, d.DMA_NAME HAVING COUNT(DISTINCT p.CACHE_BUSTER) >= 100
                ORDER BY 4 DESC, 3 DESC LIMIT 200
            """
            cursor.execute(query, {'advertiser_id': advertiser_id, 'start_date': start_date, 'end_date': end_date})
            note = 'Date filtered (matches date selector)'
        else:
            filters = ""
            if campaign_id: filters += f" AND CAMPAIGN_ID = {campaign_id}"
            if lineitem_id: filters += f" AND LINEITEM_ID = '{lineitem_id}'"

            query = f"""
                WITH zip_dma AS (
                    SELECT ZIPCODE, MAX(DMA_NAME) as DMA_NAME
                    FROM QUORUMDB.SEGMENT_DATA.DBIP_LOOKUP_US
                    WHERE DMA_NAME IS NOT NULL AND DMA_NAME != ''
                    GROUP BY ZIPCODE
                )
                SELECT cp.USER_HOME_POSTAL_CODE as ZIP_CODE, COALESCE(d.DMA_NAME, 'Unknown') as DMA_NAME,
                    SUM(cp.IMPRESSIONS) as IMPRESSIONS,
                    SUM(cp.STORE_VISITS) as STORE_VISITS, 0 as WEB_VISITS
                FROM QUORUMDB.SEGMENT_DATA.CAMPAIGN_POSTAL_REPORTING cp
                LEFT JOIN zip_dma d ON cp.USER_HOME_POSTAL_CODE = d.ZIPCODE
                WHERE cp.AGENCY_ID = %(agency_id)s AND cp.ADVERTISER_ID = %(advertiser_id)s
                  AND cp.USER_HOME_POSTAL_CODE IS NOT NULL AND cp.USER_HOME_POSTAL_CODE != ''
                  AND cp.USER_HOME_POSTAL_CODE != 'null' AND cp.USER_HOME_POSTAL_CODE != 'UNKNOWN'
                  {filters}
                GROUP BY cp.USER_HOME_POSTAL_CODE, d.DMA_NAME
                HAVING SUM(cp.IMPRESSIONS) >= 100 OR SUM(cp.STORE_VISITS) >= 1
                ORDER BY 4 DESC, 3 DESC LIMIT 200
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
# DMA PERFORMANCE (unchanged)
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
                SELECT d.DMA_NAME as DMA, COUNT(DISTINCT p.CACHE_BUSTER) as IMPRESSIONS,
                    COUNT(DISTINCT CASE WHEN p.IS_STORE_VISIT = 'TRUE' THEN p.IMP_MAID END) as STORE_VISITS,
                    COUNT(DISTINCT CASE WHEN p.IS_SITE_VISIT = 'TRUE' THEN p.IP END) as WEB_VISITS
                FROM QUORUMDB.SEGMENT_DATA.PARAMOUNT_IMPRESSIONS_REPORT_90_DAYS p
                JOIN zip_dma d ON p.ZIP_CODE = d.ZIPCODE
                WHERE p.QUORUM_ADVERTISER_ID = %(advertiser_id)s
                  AND p.IMP_DATE BETWEEN %(start_date)s AND %(end_date)s {filters}
                GROUP BY d.DMA_NAME HAVING COUNT(DISTINCT p.CACHE_BUSTER) >= 100 ORDER BY 2 DESC LIMIT 50
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
# SUMMARY ENDPOINT  [FIXED: Paramount web visits]
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
            # FIXED v4: COUNT(DISTINCT CACHE_BUSTER) for correct impression count
            query = """
                SELECT
                    COUNT(DISTINCT CACHE_BUSTER) as IMPRESSIONS,
                    COUNT(DISTINCT CASE WHEN IS_STORE_VISIT = 'TRUE' THEN IMP_MAID END) as STORE_VISITS,
                    COUNT(DISTINCT CASE WHEN IS_SITE_VISIT = 'TRUE' THEN IP END) as WEB_VISITS,
                    MIN(IMP_DATE) as MIN_DATE,
                    MAX(IMP_DATE) as MAX_DATE
                FROM QUORUMDB.SEGMENT_DATA.PARAMOUNT_IMPRESSIONS_REPORT_90_DAYS
                WHERE QUORUM_ADVERTISER_ID = %(advertiser_id)s
                  AND IMP_DATE BETWEEN %(start_date)s AND %(end_date)s
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
# TIMESERIES ENDPOINT  [FIXED: Paramount web visits]
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
            # FIXED v4: COUNT(DISTINCT CACHE_BUSTER) for correct impression count
            query = """
                SELECT
                    IMP_DATE as LOG_DATE,
                    COUNT(DISTINCT CACHE_BUSTER) as IMPRESSIONS,
                    COUNT(DISTINCT CASE WHEN IS_STORE_VISIT = 'TRUE' THEN IMP_MAID END) as STORE_VISITS,
                    COUNT(DISTINCT CASE WHEN IS_SITE_VISIT = 'TRUE' THEN IP END) as WEB_VISITS
                FROM QUORUMDB.SEGMENT_DATA.PARAMOUNT_IMPRESSIONS_REPORT_90_DAYS
                WHERE QUORUM_ADVERTISER_ID = %(advertiser_id)s
                  AND IMP_DATE BETWEEN %(start_date)s AND %(end_date)s
                GROUP BY IMP_DATE
                ORDER BY IMP_DATE
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
# LIFT ANALYSIS (unchanged — already used impression report)
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
                        COUNT(DISTINCT CACHE_BUSTER) as IMPRESSIONS, COUNT(DISTINCT IMP_MAID) as REACH,
                        COUNT(DISTINCT CASE WHEN IS_SITE_VISIT = 'TRUE' THEN IP END) as WEB_VISITORS
                    FROM QUORUMDB.SEGMENT_DATA.PARAMOUNT_IMPRESSIONS_REPORT_90_DAYS
                    WHERE QUORUM_ADVERTISER_ID::INT = %(advertiser_id)s
                      AND IMP_DATE BETWEEN %(start_date)s AND %(end_date)s
                    GROUP BY {group_cols} HAVING COUNT(DISTINCT CACHE_BUSTER) >= 1000
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
# TRAFFIC SOURCES (unchanged — Paramount only)
# =============================================================================
@app.route('/api/v5/traffic-sources', methods=['GET'])
def get_traffic_sources():
    advertiser_id = request.args.get('advertiser_id')

    if not advertiser_id:
        return jsonify({'success': False, 'error': 'advertiser_id parameter required'}), 400

    try:
        start_date, end_date = get_date_range()

        # Check cache first (this query scans 310M unclustered rows — ~15s cold)
        cache_key = f"traffic-sources:{advertiser_id}:{start_date}:{end_date}"
        cached = cache_get(cache_key)
        if cached is not None:
            return jsonify({'success': True, 'data': cached, 'cached': True})
        conn = get_snowflake_connection()
        cursor = conn.cursor()

        # IP-level (household) grouping for accurate pageviews per visitor.
        # WEB_VISITORS_TO_LOG has device-graph fan-out (~25 MAIDs per UUID),
        # so MAID-level grouping undercounts engagement. IP consolidates correctly.
        # CTV overlap checked via MAID linkage (any MAID at that IP in impression report).
        query = """
            WITH
            visitor_uuids AS (
                SELECT WEB_IMPRESSION_ID AS UUID,
                       MAID,
                       SITE_VISIT_IP AS IP,
                       SITE_VISIT_TIMESTAMP::DATE AS visit_date
                FROM QUORUMDB.SEGMENT_DATA.WEB_VISITORS_TO_LOG
                WHERE QUORUM_ADVERTISER_ID = %(advertiser_id)s
                  AND IS_SITE_VISIT = 'TRUE'
                  AND SITE_VISIT_TIMESTAMP BETWEEN %(start_date)s AND %(end_date)s
                  AND MAID IS NOT NULL AND MAID != ''
                  AND SITE_VISIT_IP IS NOT NULL AND SITE_VISIT_IP != ''
                LIMIT 50000
            ),
            uuid_classified AS (
                SELECT vu.UUID, vu.MAID, vu.IP, vu.visit_date,
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
            ip_day AS (
                SELECT IP, visit_date,
                       COUNT(DISTINCT UUID) AS pageviews,
                       MODE(traffic_source) AS dominant_source
                FROM uuid_classified
                WHERE traffic_source != 'SKIP'
                GROUP BY 1, 2
            ),
            ctv_ips AS (
                SELECT DISTINCT uc.IP
                FROM uuid_classified uc
                WHERE EXISTS (
                    SELECT 1 FROM QUORUMDB.SEGMENT_DATA.PARAMOUNT_IMPRESSIONS_REPORT_90_DAYS r
                    WHERE r.IMP_MAID = uc.MAID
                      AND r.QUORUM_ADVERTISER_ID = %(advertiser_id_int)s
                      AND r.IMP_DATE BETWEEN %(start_date)s AND %(end_date)s
                )
            ),
            classified AS (
                SELECT id.dominant_source AS traffic_source,
                       id.pageviews,
                       id.IP,
                       CASE WHEN ci.IP IS NOT NULL THEN 1 ELSE 0 END AS is_ctv
                FROM ip_day id
                LEFT JOIN ctv_ips ci ON id.IP = ci.IP
            )
            SELECT traffic_source, 'click' AS SOURCE_TYPE,
                COUNT(*) AS VISITOR_DAYS,
                COUNT(DISTINCT IP) AS UNIQUE_VISITORS,
                ROUND(AVG(pageviews), 2) AS AVG_PAGEVIEWS,
                APPROX_PERCENTILE(pageviews, 0.50) AS P50_PAGES,
                APPROX_PERCENTILE(pageviews, 0.90) AS P90_PAGES,
                ROUND(SUM(CASE WHEN pageviews = 1 THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0), 1) AS BOUNCE_RATE,
                SUM(is_ctv) AS CTV_OVERLAP,
                ROUND(SUM(is_ctv)::FLOAT / NULLIF(COUNT(*), 0) * 100, 1) AS CTV_OVERLAP_PCT,
                ROUND(AVG(CASE WHEN is_ctv = 1 THEN pageviews END), 2) AS AVG_PAGES_CTV,
                ROUND(AVG(CASE WHEN is_ctv = 0 THEN pageviews END), 2) AS AVG_PAGES_NON_CTV
            FROM classified
            GROUP BY 1 HAVING COUNT(*) >= 5
            UNION ALL
            SELECT 'Paramount CTV' AS traffic_source, 'ctv' AS SOURCE_TYPE,
                COUNT(*) AS VISITOR_DAYS,
                COUNT(DISTINCT IP) AS UNIQUE_VISITORS,
                ROUND(AVG(pageviews), 2) AS AVG_PAGEVIEWS,
                APPROX_PERCENTILE(pageviews, 0.50) AS P50_PAGES,
                APPROX_PERCENTILE(pageviews, 0.90) AS P90_PAGES,
                ROUND(SUM(CASE WHEN pageviews = 1 THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0), 1) AS BOUNCE_RATE,
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
            for k, v in d.items():
                if hasattr(v, 'is_integer'):
                    d[k] = int(v) if v == int(v) else float(v)
                elif v is None:
                    d[k] = None
            results.append(d)

        cursor.close()
        conn.close()

        # Cache results for 10 min (310M row scan is expensive)
        cache_set(cache_key, results)

        return jsonify({'success': True, 'data': results})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

# =============================================================================
# AGENCY TIMESERIES (unchanged)
# =============================================================================
@app.route('/api/v5/agency-timeseries', methods=['GET'])
def get_agency_timeseries():
    try:
        start_date, end_date = get_date_range()
        conn = get_snowflake_connection()
        cursor = conn.cursor()

        cursor.execute("""
            SELECT LOG_DATE::DATE as DT, AGENCY_ID, SUM(IMPRESSIONS) as IMPRESSIONS
            FROM QUORUMDB.SEGMENT_DATA.CAMPAIGN_PERFORMANCE_REPORT_WEEKLY_STATS
            WHERE LOG_DATE BETWEEN %(start_date)s AND %(end_date)s
            GROUP BY LOG_DATE::DATE, AGENCY_ID HAVING SUM(IMPRESSIONS) > 0
        """, {'start_date': start_date, 'end_date': end_date})
        rows_b = cursor.fetchall()

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

        from datetime import datetime, timedelta

        def find_week_bucket(dt_str, anchors):
            if not anchors:
                return dt_str
            dt = datetime.strptime(dt_str, '%Y-%m-%d').date() if isinstance(dt_str, str) else dt_str
            anchor_dates = sorted([datetime.strptime(a, '%Y-%m-%d').date() if isinstance(a, str) else a for a in anchors])
            for anchor in anchor_dates:
                if dt <= anchor:
                    return str(anchor)
            return str(anchor_dates[-1])

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
            para_weekly = {}
            for dt, aid, imps in rows_p_daily:
                from datetime import date as date_type
                d = dt if isinstance(dt, date_type) else datetime.strptime(str(dt), '%Y-%m-%d').date()
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
# ADVERTISER TIMESERIES (unchanged)
# =============================================================================
@app.route('/api/v5/advertiser-timeseries', methods=['GET'])
def get_advertiser_timeseries():
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
# OPTIMIZE RECOMMENDATIONS (unchanged — Paramount only)
# =============================================================================
@app.route('/api/v5/optimize', methods=['GET'])
def get_optimize():
    advertiser_id = request.args.get('advertiser_id')
    agency_id = request.args.get('agency_id')

    if not advertiser_id:
        return jsonify({'success': False, 'error': 'advertiser_id parameter required'}), 400

    try:
        conn = get_snowflake_connection()
        cursor = conn.cursor()

        is_paramount = agency_id and int(agency_id) == 1480

        if is_paramount:
            # --- PARAMOUNT PATH: row-level data with web + store visits ---
            date_filter = "IMP_DATE BETWEEN DATEADD(day, -35, CURRENT_DATE) AND DATEADD(day, -5, CURRENT_DATE)"
            adv_filter = "QUORUM_ADVERTISER_ID = %(adv_id)s"
            imps_expr = "COUNT(DISTINCT CACHE_BUSTER)"
            web_expr = "COUNT(DISTINCT CASE WHEN IS_SITE_VISIT = 'TRUE' THEN IP END)"
            store_expr = "COUNT(DISTINCT CASE WHEN IS_STORE_VISIT = 'TRUE' THEN IMP_MAID END)"
            web_vr = f"ROUND({web_expr}*100.0/NULLIF({imps_expr},0), 4)"
            store_vr = f"ROUND({store_expr}*100.0/NULLIF({imps_expr},0), 4)"

            q1 = f"""
                SELECT 'baseline' as DIM_TYPE, 'overall' as DIM_KEY, NULL as DIM_NAME,
                    {imps_expr} as IMPS, {web_expr} as WEB_VISITS, {store_expr} as STORE_VISITS,
                    {web_vr} as WEB_VR, {store_vr} as STORE_VR
                FROM QUORUMDB.SEGMENT_DATA.PARAMOUNT_IMPRESSIONS_REPORT_90_DAYS
                WHERE {adv_filter} AND {date_filter}
                UNION ALL
                SELECT 'campaign', IO_ID::VARCHAR, MAX(IO_NAME), {imps_expr}, {web_expr}, {store_expr}, {web_vr}, {store_vr}
                FROM QUORUMDB.SEGMENT_DATA.PARAMOUNT_IMPRESSIONS_REPORT_90_DAYS
                WHERE {adv_filter} AND {date_filter} GROUP BY IO_ID
                UNION ALL
                SELECT 'lineitem', LINEITEM_ID::VARCHAR, MAX(LINEITEM_NAME), {imps_expr}, {web_expr}, {store_expr}, {web_vr}, {store_vr}
                FROM QUORUMDB.SEGMENT_DATA.PARAMOUNT_IMPRESSIONS_REPORT_90_DAYS
                WHERE {adv_filter} AND {date_filter} GROUP BY LINEITEM_ID
                UNION ALL
                SELECT 'creative', CREATIVE_NAME, NULL, {imps_expr}, {web_expr}, {store_expr}, {web_vr}, {store_vr}
                FROM QUORUMDB.SEGMENT_DATA.PARAMOUNT_IMPRESSIONS_REPORT_90_DAYS
                WHERE {adv_filter} AND {date_filter} GROUP BY CREATIVE_NAME
                UNION ALL
                SELECT 'dow', DAYOFWEEK(IMP_DATE)::VARCHAR, NULL, {imps_expr}, {web_expr}, {store_expr}, {web_vr}, {store_vr}
                FROM QUORUMDB.SEGMENT_DATA.PARAMOUNT_IMPRESSIONS_REPORT_90_DAYS
                WHERE {adv_filter} AND {date_filter} GROUP BY DAYOFWEEK(IMP_DATE)
                UNION ALL
                SELECT 'site', SITE, NULL, {imps_expr}, {web_expr}, {store_expr}, {web_vr}, {store_vr}
                FROM QUORUMDB.SEGMENT_DATA.PARAMOUNT_IMPRESSIONS_REPORT_90_DAYS
                WHERE {adv_filter} AND {date_filter} GROUP BY SITE HAVING COUNT(DISTINCT CACHE_BUSTER) >= 500
                ORDER BY 1, 4 DESC
            """
            cursor.execute(q1, {'adv_id': int(advertiser_id)})
        else:
            # --- CLASS B PATH: weekly stats, store visits only (no web pixel) ---
            vr_expr = "ROUND(SUM(VISITORS)*100.0/NULLIF(SUM(IMPRESSIONS),0), 4)"
            q1 = """
                SELECT 'baseline' as DIM_TYPE, 'overall' as DIM_KEY, NULL as DIM_NAME,
                    SUM(IMPRESSIONS) as IMPS, 0 as WEB_VISITS, SUM(VISITORS) as STORE_VISITS,
                    0 as WEB_VR, {vr} as STORE_VR
                FROM QUORUMDB.SEGMENT_DATA.CAMPAIGN_PERFORMANCE_REPORT_WEEKLY_STATS
                WHERE AGENCY_ID = %(agency_id)s AND ADVERTISER_ID = %(adv_id)s
                  AND LOG_DATE BETWEEN DATEADD(day, -35, CURRENT_DATE) AND DATEADD(day, -5, CURRENT_DATE)
                UNION ALL
                SELECT 'campaign', IO_ID::VARCHAR, MAX(IO_NAME),
                    SUM(IMPRESSIONS), 0, SUM(VISITORS), 0, {vr}
                FROM QUORUMDB.SEGMENT_DATA.CAMPAIGN_PERFORMANCE_REPORT_WEEKLY_STATS
                WHERE AGENCY_ID = %(agency_id)s AND ADVERTISER_ID = %(adv_id)s
                  AND LOG_DATE BETWEEN DATEADD(day, -35, CURRENT_DATE) AND DATEADD(day, -5, CURRENT_DATE)
                GROUP BY IO_ID
                UNION ALL
                SELECT 'lineitem', LI_ID::VARCHAR, MAX(LI_NAME),
                    SUM(IMPRESSIONS), 0, SUM(VISITORS), 0, {vr}
                FROM QUORUMDB.SEGMENT_DATA.CAMPAIGN_PERFORMANCE_REPORT_WEEKLY_STATS
                WHERE AGENCY_ID = %(agency_id)s AND ADVERTISER_ID = %(adv_id)s
                  AND LOG_DATE BETWEEN DATEADD(day, -35, CURRENT_DATE) AND DATEADD(day, -5, CURRENT_DATE)
                GROUP BY LI_ID
                UNION ALL
                SELECT 'site', PUBLISHER, NULL,
                    SUM(IMPRESSIONS), 0, SUM(VISITORS), 0, {vr}
                FROM QUORUMDB.SEGMENT_DATA.CAMPAIGN_PERFORMANCE_REPORT_WEEKLY_STATS
                WHERE AGENCY_ID = %(agency_id)s AND ADVERTISER_ID = %(adv_id)s
                  AND LOG_DATE BETWEEN DATEADD(day, -35, CURRENT_DATE) AND DATEADD(day, -5, CURRENT_DATE)
                  AND PUBLISHER IS NOT NULL AND PUBLISHER != ''
                GROUP BY PUBLISHER HAVING SUM(IMPRESSIONS) >= 500
                UNION ALL
                SELECT 'dow', DAYOFWEEK(LOG_DATE)::VARCHAR, NULL,
                    SUM(IMPRESSIONS), 0, SUM(VISITORS), 0, {vr}
                FROM QUORUMDB.SEGMENT_DATA.CAMPAIGN_PERFORMANCE_REPORT_WEEKLY_STATS
                WHERE AGENCY_ID = %(agency_id)s AND ADVERTISER_ID = %(adv_id)s
                  AND LOG_DATE BETWEEN DATEADD(day, -35, CURRENT_DATE) AND DATEADD(day, -5, CURRENT_DATE)
                GROUP BY DAYOFWEEK(LOG_DATE)
                ORDER BY 1, 4 DESC
            """.format(vr=vr_expr)
            cursor.execute(q1, {'agency_id': int(agency_id), 'adv_id': int(advertiser_id)})

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
    advertiser_id = request.args.get('advertiser_id')
    agency_id = request.args.get('agency_id')

    if not advertiser_id:
        return jsonify({'success': False, 'error': 'advertiser_id parameter required'}), 400

    try:
        conn = get_snowflake_connection()
        cursor = conn.cursor()

        is_paramount = agency_id and int(agency_id) == 1480

        if is_paramount:
            # --- PARAMOUNT PATH: row-level geo with web + store ---
            date_filter = "IMP_DATE BETWEEN DATEADD(day, -35, CURRENT_DATE) AND DATEADD(day, -5, CURRENT_DATE)"
            adv_filter = "QUORUM_ADVERTISER_ID = %(adv_id)s"
            imps_expr = "COUNT(DISTINCT i.CACHE_BUSTER)"
            web_expr = "COUNT(DISTINCT CASE WHEN i.IS_SITE_VISIT = 'TRUE' THEN i.IP END)"
            store_expr = "COUNT(DISTINCT CASE WHEN i.IS_STORE_VISIT = 'TRUE' THEN i.IMP_MAID END)"
            web_vr = f"ROUND({web_expr}*100.0/NULLIF({imps_expr},0), 4)"
            store_vr = f"ROUND({store_expr}*100.0/NULLIF({imps_expr},0), 4)"

            q2 = f"""
                SELECT 'dma' as DIM_TYPE, z.DMA_CODE as DIM_KEY, MAX(z.DMA_NAME) as DIM_NAME,
                    {imps_expr} as IMPS, {web_expr} as WEB_VISITS, {store_expr} as STORE_VISITS,
                    {web_vr} as WEB_VR, {store_vr} as STORE_VR
                FROM QUORUMDB.SEGMENT_DATA.PARAMOUNT_IMPRESSIONS_REPORT_90_DAYS i
                JOIN QUORUMDB.SEGMENT_DATA.ZIP_DMA_MAPPING z ON i.ZIP_CODE = z.ZIP_CODE
                WHERE i.{adv_filter} AND i.{date_filter}
                GROUP BY z.DMA_CODE HAVING COUNT(DISTINCT i.CACHE_BUSTER) >= 500
                UNION ALL
                SELECT 'zip', i.ZIP_CODE, MAX(z.DMA_NAME), {imps_expr}, {web_expr}, {store_expr}, {web_vr}, {store_vr}
                FROM QUORUMDB.SEGMENT_DATA.PARAMOUNT_IMPRESSIONS_REPORT_90_DAYS i
                JOIN QUORUMDB.SEGMENT_DATA.ZIP_DMA_MAPPING z ON i.ZIP_CODE = z.ZIP_CODE
                WHERE i.{adv_filter} AND i.{date_filter}
                GROUP BY i.ZIP_CODE HAVING COUNT(DISTINCT i.CACHE_BUSTER) >= 50
                ORDER BY 1, 4 DESC
            """
            cursor.execute(q2, {'adv_id': int(advertiser_id)})
        else:
            # --- CLASS B PATH: weekly stats geo, store visits only ---
            vr_expr = "ROUND(SUM(VISITORS)*100.0/NULLIF(SUM(IMPRESSIONS),0), 4)"
            q2 = """
                SELECT 'dma' as DIM_TYPE, DMA as DIM_KEY, DMA as DIM_NAME,
                    SUM(IMPRESSIONS) as IMPS, 0 as WEB_VISITS, SUM(VISITORS) as STORE_VISITS,
                    0 as WEB_VR, {vr} as STORE_VR
                FROM QUORUMDB.SEGMENT_DATA.CAMPAIGN_PERFORMANCE_REPORT_WEEKLY_STATS
                WHERE AGENCY_ID = %(agency_id)s AND ADVERTISER_ID = %(adv_id)s
                  AND LOG_DATE BETWEEN DATEADD(day, -35, CURRENT_DATE) AND DATEADD(day, -5, CURRENT_DATE)
                  AND DMA IS NOT NULL AND DMA != ''
                GROUP BY DMA HAVING SUM(IMPRESSIONS) >= 500
                UNION ALL
                SELECT 'zip', ZIP, MAX(DMA),
                    SUM(IMPRESSIONS), 0, SUM(VISITORS), 0, {vr}
                FROM QUORUMDB.SEGMENT_DATA.CAMPAIGN_PERFORMANCE_REPORT_WEEKLY_STATS
                WHERE AGENCY_ID = %(agency_id)s AND ADVERTISER_ID = %(adv_id)s
                  AND LOG_DATE BETWEEN DATEADD(day, -35, CURRENT_DATE) AND DATEADD(day, -5, CURRENT_DATE)
                  AND ZIP IS NOT NULL AND ZIP != ''
                GROUP BY ZIP HAVING SUM(IMPRESSIONS) >= 50
                ORDER BY 1, 4 DESC
            """.format(vr=vr_expr)
            cursor.execute(q2, {'agency_id': int(agency_id), 'adv_id': int(advertiser_id)})

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

# =============================================================================
# OPS CONSOLE — PIPELINE HEALTH DASHBOARD
# =============================================================================
@app.route('/api/v5/pipeline-health', methods=['GET'])
def pipeline_health():
    """Ops console: table freshness, volume trends, scheduled tasks, anomalies."""
    try:
        conn = get_snowflake_connection()
        cursor = conn.cursor()

        # =====================================================================
        # 1. TABLE METADATA — batch SHOW TABLES (metadata only, instant)
        # =====================================================================
        table_meta = {}
        for schema in ['BASE_TABLES', 'DERIVED_TABLES']:
            try:
                cursor.execute(f"SHOW TABLES IN SCHEMA QUORUMDB.{schema}")
                for row in cursor.fetchall():
                    full_name = f"QUORUMDB.{schema}.{row[1]}"
                    table_meta[full_name] = {
                        'rows': int(row[7]) if row[7] else 0,
                        'bytes': int(row[8]) if row[8] else 0
                    }
            except:
                pass

        # =====================================================================
        # 2. FRESHNESS — MAX only (no COUNT, uses micropartition metadata)
        #    Row counts come from SHOW TABLES metadata above (zero scan)
        # =====================================================================
        table_health = []
        volume_trends = {}

        # MAX queries are fast: Snowflake resolves from partition stats
        freshness_queries = [
            ('impressions', "SELECT MAX(AUCTION_TIMESTAMP)::DATE FROM QUORUMDB.BASE_TABLES.AD_IMPRESSION_LOG_V2 WHERE AUCTION_TIMESTAMP >= DATEADD('day', -3, CURRENT_DATE()) AND AUCTION_TIMESTAMP <= CURRENT_TIMESTAMP()"),
            ('store_visits', "SELECT MAX(STORE_VISIT_DATE) FROM QUORUMDB.BASE_TABLES.STORE_VISITS WHERE STORE_VISIT_DATE >= DATEADD('day', -60, CURRENT_DATE())"),
            ('web_pixel_raw', "SELECT MAX(SYS_TIMESTAMP)::DATE FROM QUORUMDB.BASE_TABLES.WEBPIXEL_IMPRESSION_LOG WHERE SYS_TIMESTAMP >= DATEADD('day', -3, CURRENT_DATE()) AND SYS_TIMESTAMP <= CURRENT_TIMESTAMP()"),
            ('web_pixel_events', "SELECT MAX(STAGING_SYS_TIMESTAMP)::DATE FROM QUORUMDB.DERIVED_TABLES.WEBPIXEL_EVENTS WHERE STAGING_SYS_TIMESTAMP >= DATEADD('day', -30, CURRENT_DATE())"),
        ]

        for pname, query in freshness_queries:
            try:
                cursor.execute(query)
                row = cursor.fetchone()
                last_data_str = str(row[0]) if row and row[0] else None
                ds = (date.today() - datetime.strptime(last_data_str, '%Y-%m-%d').date()).days if last_data_str else 999
                volume_trends[pname] = {'last_data': last_data_str, 'days_stale': ds}
            except:
                volume_trends[pname] = {'last_data': None, 'days_stale': 999}

        # Build table_health from freshness + metadata (zero data scanning)
        pipeline_config = [
            ('Ad Impressions (V2)', 'QUORUMDB.BASE_TABLES.AD_IMPRESSION_LOG_V2', 'impressions', 'Primary ad impression log from all DSPs'),
            ('Store Visits', 'QUORUMDB.BASE_TABLES.STORE_VISITS', 'store_visits', 'Foot traffic attribution data'),
            ('Web Pixel Raw', 'QUORUMDB.BASE_TABLES.WEBPIXEL_IMPRESSION_LOG', 'web_pixel_raw', 'Raw web pixel fire events'),
            ('Web Pixel Events', 'QUORUMDB.DERIVED_TABLES.WEBPIXEL_EVENTS', 'web_pixel_events', 'Transformed web pixel analytical layer'),
        ]
        for label, tbl, key, desc in pipeline_config:
            meta = table_meta.get(tbl, {})
            vt = volume_trends.get(key, {})
            ds = vt.get('days_stale', 999)
            status = 'green' if ds <= 1 else ('yellow' if ds <= 3 else 'red')
            table_health.append({
                'label': label, 'table': tbl, 'description': desc,
                'last_data': vt.get('last_data'), 'days_stale': ds,
                'total_rows': meta.get('rows', 0), 'total_bytes': meta.get('bytes', 0),
                'status': status
            })

        # =====================================================================
        # 3. ALERTS — staleness + wow drops
        # =====================================================================
        alerts = []

        for t in table_health:
            if t['days_stale'] > 7:
                alerts.append({
                    'severity': 'critical',
                    'pipeline': t['label'],
                    'message': f"{t['label']} — no data in {t['days_stale']} days (last: {t['last_data'] or 'never'})"
                })
            elif t['days_stale'] > 3:
                alerts.append({
                    'severity': 'warning',
                    'pipeline': t['label'],
                    'message': f"{t['label']} — {t['days_stale']} days stale (last: {t['last_data'] or 'never'})"
                })

        # =====================================================================
        # 4. SCHEDULED TASKS — check what's running/suspended
        # =====================================================================
        tasks = []
        try:
            cursor.execute("SHOW TASKS IN DATABASE QUORUMDB")
            for row in cursor.fetchall():
                task_name = row[1]
                schema = row[4]
                schedule = row[8]
                state = row[10]
                definition = row[11][:200] if row[11] else ''
                last_committed = str(row[15]) if row[15] else None
                last_suspended = str(row[16]) if row[16] else None
                suspend_reason = row[20] if len(row) > 20 else None

                task_status = 'green' if state == 'started' else 'red'
                tasks.append({
                    'name': task_name,
                    'schema': schema,
                    'schedule': schedule,
                    'state': state,
                    'status': task_status,
                    'definition_preview': definition,
                    'last_committed': last_committed,
                    'last_suspended': last_suspended,
                    'suspend_reason': suspend_reason
                })

                # Alert on suspended tasks
                if state == 'suspended':
                    alerts.append({
                        'severity': 'critical',
                        'pipeline': task_name,
                        'message': f"Scheduled task {task_name} is SUSPENDED" + (f" — {suspend_reason}" if suspend_reason else "")
                    })
        except Exception as te:
            tasks.append({'name': 'TASKS_UNAVAILABLE', 'error': str(te)[:200], 'status': 'unknown'})

        # =====================================================================
        # 5. TRANSFORM LOG — recent batch runs
        # =====================================================================
        transform_log = []
        try:
            cursor.execute("""
                SELECT BATCH_ID, STATUS, STARTED_AT, COMPLETED_AT,
                       EVENTS_INSERTED, ERROR_MESSAGE, HOUR_START, HOUR_END
                FROM QUORUMDB.DERIVED_TABLES.WEBPIXEL_TRANSFORM_LOG
                ORDER BY STARTED_AT DESC
                LIMIT 20
            """)
            for row in cursor.fetchall():
                transform_log.append({
                    'BATCH_ID': row[0], 'STATUS': row[1],
                    'STARTED_AT': str(row[2]) if row[2] else None,
                    'COMPLETED_AT': str(row[3]) if row[3] else None,
                    'EVENTS_INSERTED': int(row[4]) if row[4] else 0,
                    'ERROR_MESSAGE': row[5],
                    'HOUR_START': str(row[6]) if row[6] else None,
                    'HOUR_END': str(row[7]) if row[7] else None,
                })
        except:
            pass  # DERIVED_TABLES may not be accessible

        # =====================================================================
        # 6. STORED PROCEDURES — inventory
        # =====================================================================
        procedures = []
        try:
            cursor.execute("SHOW USER PROCEDURES IN DATABASE QUORUMDB")
            for row in cursor.fetchall():
                proc_name = row[1]
                schema = row[2]
                desc = row[9] if len(row) > 9 else ''
                if proc_name.startswith('SYSTEM$'):
                    continue
                procedures.append({
                    'name': proc_name,
                    'schema': schema,
                    'description': desc[:200] if desc else ''
                })
        except:
            pass

        cursor.close()
        conn.close()

        # =====================================================================
        # 7. BUILD RESPONSE
        # =====================================================================
        # Sort alerts: critical first, then warning
        severity_order = {'critical': 0, 'warning': 1, 'info': 2}
        alerts.sort(key=lambda a: severity_order.get(a.get('severity', 'info'), 3))

        # Overall system status
        has_critical = any(a['severity'] == 'critical' for a in alerts)
        has_warning = any(a['severity'] == 'warning' for a in alerts)
        overall_status = 'red' if has_critical else ('yellow' if has_warning else 'green')

        # Pipeline-level status summary
        pipeline_status = {}
        for t in table_health:
            pipeline_status[t['label']] = t['status']
        for task in tasks:
            if task.get('state') == 'suspended':
                pipeline_status[task['name']] = 'red'

        return jsonify({
            'success': True,
            'overall_status': overall_status,
            'tables': table_health,
            'volume_trends': volume_trends,
            'alerts': alerts,
            'tasks': tasks,
            'transform_log': transform_log,
            'procedures': procedures,
            'pipeline_status': pipeline_status,
            'summary': {
                'total_tables_monitored': len(table_health),
                'tables_healthy': sum(1 for t in table_health if t['status'] == 'green'),
                'tables_warning': sum(1 for t in table_health if t['status'] == 'yellow'),
                'tables_critical': sum(1 for t in table_health if t['status'] == 'red'),
                'total_alerts': len(alerts),
                'critical_alerts': sum(1 for a in alerts if a['severity'] == 'critical'),
                'tasks_running': sum(1 for t in tasks if t.get('state') == 'started'),
                'tasks_suspended': sum(1 for t in tasks if t.get('state') == 'suspended'),
            }
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

# =============================================================================
# MAIN
# =============================================================================
if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8080))
    app.run(host='0.0.0.0', port=port, debug=False)
