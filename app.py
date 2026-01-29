"""
Quorum Optimizer API v5 - Fast startup version
Uses hardcoded agency list to avoid slow queries
"""

from flask import Flask, jsonify, request
from flask_cors import CORS
import snowflake.connector
import os
from datetime import datetime, timedelta
import logging
from urllib.parse import unquote

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)
CORS(app)

def decode_publisher(name):
    """URL-decode publisher names (handles %2520 double-encoding)"""
    if not name:
        return name
    # Double-decode to handle %2520 -> %20 -> space
    try:
        decoded = unquote(unquote(str(name)))
        return decoded
    except:
        return name

# ============================================================================
# HARDCODED AGENCY LIST - Avoids slow startup query
# ============================================================================

AGENCIES = [
    {'AGENCY_ID': '2514', 'AGENCY_NAME': 'MNTN', 'SOURCE': 'QIR', 'ADVERTISER_COUNT': 30},
    {'AGENCY_ID': '1956', 'AGENCY_NAME': 'Dealer Spike', 'SOURCE': 'QIR', 'ADVERTISER_COUNT': 25},
    {'AGENCY_ID': '2298', 'AGENCY_NAME': 'InteractRV', 'SOURCE': 'QIR', 'ADVERTISER_COUNT': 15},
    {'AGENCY_ID': '2086', 'AGENCY_NAME': 'Level5', 'SOURCE': 'QIR', 'ADVERTISER_COUNT': 10},
    {'AGENCY_ID': '1955', 'AGENCY_NAME': 'ARI', 'SOURCE': 'QIR', 'ADVERTISER_COUNT': 20},
    {'AGENCY_ID': '1950', 'AGENCY_NAME': 'ByRider', 'SOURCE': 'QIR', 'ADVERTISER_COUNT': 8},
    {'AGENCY_ID': '1813', 'AGENCY_NAME': 'Causal iQ', 'SOURCE': 'CPRS', 'ADVERTISER_COUNT': 45},
    {'AGENCY_ID': '1972', 'AGENCY_NAME': 'Hearst', 'SOURCE': 'CPRS', 'ADVERTISER_COUNT': 20},
    {'AGENCY_ID': '2234', 'AGENCY_NAME': 'Magnite', 'SOURCE': 'CPRS', 'ADVERTISER_COUNT': 12},
    {'AGENCY_ID': '2744', 'AGENCY_NAME': 'Parallel Path', 'SOURCE': 'CPRS', 'ADVERTISER_COUNT': 18},
    {'AGENCY_ID': '1445', 'AGENCY_NAME': 'Publicis', 'SOURCE': 'CPRS', 'ADVERTISER_COUNT': 15},
    {'AGENCY_ID': '2379', 'AGENCY_NAME': 'The Shipyard', 'SOURCE': 'CPRS', 'ADVERTISER_COUNT': 10},
    {'AGENCY_ID': '2691', 'AGENCY_NAME': 'TeamSnap', 'SOURCE': 'CPRS', 'ADVERTISER_COUNT': 5},
    {'AGENCY_ID': '1880', 'AGENCY_NAME': 'TravelSpike', 'SOURCE': 'CPRS', 'ADVERTISER_COUNT': 8},
    {'AGENCY_ID': '1480', 'AGENCY_NAME': 'ViacomCBS / Paramount', 'SOURCE': 'PARAMOUNT', 'ADVERTISER_COUNT': 95},
]

AGENCY_SOURCE = {a['AGENCY_ID']: a['SOURCE'] for a in AGENCIES}

def get_source(agency_id):
    return AGENCY_SOURCE.get(str(agency_id), 'CPRS')

# ============================================================================
# SNOWFLAKE
# ============================================================================

def get_conn():
    return snowflake.connector.connect(
        user=os.environ.get('SNOWFLAKE_USER'),
        password=os.environ.get('SNOWFLAKE_PASSWORD'),
        account=os.environ.get('SNOWFLAKE_ACCOUNT'),
        warehouse=os.environ.get('SNOWFLAKE_WAREHOUSE', 'COMPUTE_WH'),
        database='QUORUMDB',
        schema='SEGMENT_DATA',
        login_timeout=30,
        network_timeout=120
    )

def query(sql, params=None):
    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute(sql, params or [])
        return cur.fetchall()
    finally:
        cur.close()
        conn.close()

def dates(req):
    end = req.args.get('end_date', datetime.now().strftime('%Y-%m-%d'))
    start = req.args.get('start_date', (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d'))
    return start, end

def pct(num, denom):
    return round(num / denom * 100, 4) if denom else 0

# ============================================================================
# HEALTH / ROOT
# ============================================================================

@app.route('/health')
def health():
    return jsonify({'status': 'ok', 'version': 'v5-fast'})

@app.route('/')
def root():
    return jsonify({'name': 'Quorum Optimizer API', 'version': 'v5-fast', 'status': 'running'})

# ============================================================================
# V3 ENDPOINTS
# ============================================================================

@app.route('/api/v3/agencies')
def get_agencies():
    """Return hardcoded agency list - instant response"""
    return jsonify({'success': True, 'data': AGENCIES})


@app.route('/api/v3/agency-overview')
def get_agency_overview():
    """Get agency metrics - uses simpler/faster queries"""
    try:
        start, end = dates(request)
        results = []
        
        # QIR agencies - single fast query
        qir_sql = """
            SELECT AGENCY_ID, COUNT(*) as IMPS,
                   SUM(CASE WHEN IS_STORE_VISIT THEN 1 ELSE 0 END) as LV,
                   SUM(CASE WHEN IS_SITE_VISIT THEN 1 ELSE 0 END) as WV
            FROM QUORUMDB.SEGMENT_DATA.QUORUM_IMPRESSIONS_REPORT
            WHERE LOG_DATE >= %s AND LOG_DATE < %s
            GROUP BY AGENCY_ID
        """
        qir_data = {str(r[0]): r for r in query(qir_sql, [start, end])}
        
        # CPRS agencies - single fast query  
        cprs_sql = """
            SELECT AGENCY_ID, SUM(IMPRESSIONS) as IMPS, SUM(VISITORS) as LV
            FROM QUORUMDB.SEGMENT_DATA.CAMPAIGN_PERFORMANCE_REPORT_WEEKLY_STATS
            WHERE LOG_DATE >= %s AND LOG_DATE < %s
            GROUP BY AGENCY_ID
        """
        cprs_data = {str(r[0]): r for r in query(cprs_sql, [start, end])}
        
        # Paramount - count DISTINCT impressions and visits
        try:
            paramount_sql = """
                SELECT COUNT(DISTINCT CACHE_BUSTER),
                       COUNT(DISTINCT CASE WHEN IS_STORE_VISIT = 'TRUE' THEN CACHE_BUSTER END),
                       COUNT(DISTINCT CASE WHEN IS_SITE_VISIT = 'TRUE' THEN WEB_IMPRESSION_ID END)
                FROM QUORUMDB.SEGMENT_DATA.PARAMOUNT_IMPRESSIONS_REPORT_90_DAYS
                WHERE IMP_DATE >= %s AND IMP_DATE < %s
            """
            p_row = query(paramount_sql, [start, end])[0]
            paramount_imps = p_row[0] or 0
            paramount_lv = p_row[1] or 0
            paramount_wv = p_row[2] or 0
        except:
            paramount_imps, paramount_lv, paramount_wv = 0, 0, 0
        
        for a in AGENCIES:
            aid = a['AGENCY_ID']
            src = a['SOURCE']
            
            if src == 'QIR' and aid in qir_data:
                r = qir_data[aid]
                imps, lv, wv = r[1] or 0, r[2] or 0, r[3] or 0
            elif src == 'CPRS' and aid in cprs_data:
                r = cprs_data[aid]
                imps, lv, wv = r[1] or 0, r[2] or 0, 0
            elif src == 'PARAMOUNT':
                imps, lv, wv = paramount_imps, paramount_lv, paramount_wv
            else:
                imps, lv, wv = 0, 0, 0
            
            results.append({
                'AGENCY_ID': aid,
                'AGENCY_NAME': a['AGENCY_NAME'],
                'IMPRESSIONS': imps,
                'LOCATION_VISITS': lv,
                'WEB_VISITS': wv,
                'LOCATION_VR': pct(lv, imps),
                'VISIT_RATE': pct(wv, imps)
            })
        
        results.sort(key=lambda x: x['IMPRESSIONS'], reverse=True)
        return jsonify({'success': True, 'data': results})
    except Exception as e:
        logger.error(f"agency-overview error: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/v3/advertiser-overview')
def get_advertiser_overview():
    """Get advertisers for an agency"""
    try:
        agency_id = request.args.get('agency_id')
        if not agency_id:
            return jsonify({'success': False, 'error': 'agency_id required'}), 400
        
        start, end = dates(request)
        src = get_source(agency_id)
        
        if src == 'QIR':
            sql = """
                SELECT q.QUORUM_ADVERTISER_ID, MAX(a.COMP_NAME), COUNT(*),
                       SUM(CASE WHEN q.IS_STORE_VISIT THEN 1 ELSE 0 END),
                       SUM(CASE WHEN q.IS_SITE_VISIT THEN 1 ELSE 0 END)
                FROM QUORUMDB.SEGMENT_DATA.QUORUM_IMPRESSIONS_REPORT q
                LEFT JOIN QUORUMDB.SEGMENT_DATA.AGENCY_ADVERTISER a ON q.QUORUM_ADVERTISER_ID = a.ID
                WHERE q.AGENCY_ID = %s AND q.LOG_DATE >= %s AND q.LOG_DATE < %s
                GROUP BY q.QUORUM_ADVERTISER_ID
                HAVING COUNT(*) >= 1000
                ORDER BY 3 DESC LIMIT 100
            """
            rows = query(sql, [agency_id, start, end])
        elif src == 'CPRS':
            sql = """
                SELECT w.ADVERTISER_ID, MAX(a.COMP_NAME), SUM(w.IMPRESSIONS), SUM(w.VISITORS), 0
                FROM QUORUMDB.SEGMENT_DATA.CAMPAIGN_PERFORMANCE_REPORT_WEEKLY_STATS w
                LEFT JOIN QUORUMDB.SEGMENT_DATA.AGENCY_ADVERTISER a ON w.ADVERTISER_ID = a.ID
                WHERE w.AGENCY_ID = %s AND w.LOG_DATE >= %s AND w.LOG_DATE < %s
                GROUP BY w.ADVERTISER_ID
                HAVING SUM(w.IMPRESSIONS) >= 1000
                ORDER BY 3 DESC LIMIT 100
            """
            rows = query(sql, [agency_id, start, end])
        elif src == 'PARAMOUNT':
            sql = """
                SELECT CAST(p.QUORUM_ADVERTISER_ID AS INTEGER), MAX(a.COMP_NAME), 
                       COUNT(DISTINCT p.CACHE_BUSTER),
                       COUNT(DISTINCT CASE WHEN p.IS_STORE_VISIT = 'TRUE' THEN p.CACHE_BUSTER END),
                       COUNT(DISTINCT CASE WHEN p.IS_SITE_VISIT = 'TRUE' THEN p.WEB_IMPRESSION_ID END)
                FROM QUORUMDB.SEGMENT_DATA.PARAMOUNT_IMPRESSIONS_REPORT_90_DAYS p
                LEFT JOIN QUORUMDB.SEGMENT_DATA.AGENCY_ADVERTISER a ON CAST(p.QUORUM_ADVERTISER_ID AS INTEGER) = a.ID
                WHERE p.IMP_DATE >= %s AND p.IMP_DATE < %s
                GROUP BY p.QUORUM_ADVERTISER_ID
                HAVING COUNT(DISTINCT p.CACHE_BUSTER) >= 10000
                ORDER BY 3 DESC LIMIT 100
            """
            rows = query(sql, [start, end])
        else:
            rows = []
        
        data = []
        for r in rows:
            imps, lv, wv = r[2] or 0, r[3] or 0, r[4] or 0
            data.append({
                'ADVERTISER_ID': str(r[0]),
                'ADVERTISER_NAME': r[1] or f'Advertiser {r[0]}',
                'IMPRESSIONS': imps,
                'LOCATION_VISITS': lv,
                'WEB_VISITS': wv,
                'LOCATION_VR': pct(lv, imps),
                'VISIT_RATE': pct(wv, imps)
            })
        
        return jsonify({'success': True, 'data': data})
    except Exception as e:
        logger.error(f"advertiser-overview error: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/v3/advertisers')
def get_advertisers():
    """Simple advertiser list for sidebar"""
    try:
        agency_id = request.args.get('agency_id')
        if not agency_id:
            return jsonify({'success': False, 'error': 'agency_id required'}), 400
        
        src = get_source(agency_id)
        
        if src == 'QIR':
            sql = """
                SELECT DISTINCT q.QUORUM_ADVERTISER_ID, MAX(a.COMP_NAME)
                FROM QUORUMDB.SEGMENT_DATA.QUORUM_IMPRESSIONS_REPORT q
                LEFT JOIN QUORUMDB.SEGMENT_DATA.AGENCY_ADVERTISER a ON q.QUORUM_ADVERTISER_ID = a.ID
                WHERE q.AGENCY_ID = %s AND q.LOG_DATE >= DATEADD(day, -90, CURRENT_DATE())
                GROUP BY q.QUORUM_ADVERTISER_ID HAVING COUNT(*) >= 1000
            """
            rows = query(sql, [agency_id])
        elif src == 'CPRS':
            sql = """
                SELECT DISTINCT w.ADVERTISER_ID, MAX(a.COMP_NAME)
                FROM QUORUMDB.SEGMENT_DATA.CAMPAIGN_PERFORMANCE_REPORT_WEEKLY_STATS w
                LEFT JOIN QUORUMDB.SEGMENT_DATA.AGENCY_ADVERTISER a ON w.ADVERTISER_ID = a.ID
                WHERE w.AGENCY_ID = %s AND w.LOG_DATE >= DATEADD(day, -90, CURRENT_DATE())
                GROUP BY w.ADVERTISER_ID HAVING SUM(w.IMPRESSIONS) >= 1000
            """
            rows = query(sql, [agency_id])
        else:
            rows = []
        
        data = [{'ADVERTISER_ID': str(r[0]), 'ADVERTISER_NAME': r[1] or f'Advertiser {r[0]}'} for r in rows]
        return jsonify({'success': True, 'data': data})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


# ============================================================================
# DETAIL ENDPOINTS
# ============================================================================

@app.route('/api/advertiser-summary')
def get_advertiser_summary():
    try:
        agency_id = request.args.get('agency_id')
        advertiser_id = request.args.get('advertiser_id')
        if not agency_id or not advertiser_id:
            return jsonify({'success': False, 'error': 'agency_id and advertiser_id required'}), 400
        
        start, end = dates(request)
        src = get_source(agency_id)
        
        if src == 'QIR':
            sql = """
                SELECT COUNT(*), SUM(CASE WHEN IS_STORE_VISIT THEN 1 ELSE 0 END),
                       SUM(CASE WHEN IS_SITE_VISIT THEN 1 ELSE 0 END),
                       COUNT(DISTINCT IO_ID), COUNT(DISTINCT PUBLISHER_CODE)
                FROM QUORUMDB.SEGMENT_DATA.QUORUM_IMPRESSIONS_REPORT
                WHERE QUORUM_ADVERTISER_ID = %s AND AGENCY_ID = %s AND LOG_DATE >= %s AND LOG_DATE < %s
            """
            rows = query(sql, [advertiser_id, agency_id, start, end])
        elif src == 'CPRS':
            sql = """
                SELECT SUM(IMPRESSIONS), SUM(VISITORS), 0, COUNT(DISTINCT IO_ID), COUNT(DISTINCT PUBLISHER)
                FROM QUORUMDB.SEGMENT_DATA.CAMPAIGN_PERFORMANCE_REPORT_WEEKLY_STATS
                WHERE ADVERTISER_ID = %s AND AGENCY_ID = %s AND LOG_DATE >= %s AND LOG_DATE < %s
            """
            rows = query(sql, [advertiser_id, agency_id, start, end])
        elif src == 'PARAMOUNT':
            sql = """
                SELECT COUNT(DISTINCT CACHE_BUSTER), 
                       COUNT(DISTINCT CASE WHEN IS_STORE_VISIT = 'TRUE' THEN CACHE_BUSTER END),
                       COUNT(DISTINCT CASE WHEN IS_SITE_VISIT = 'TRUE' THEN WEB_IMPRESSION_ID END),
                       COUNT(DISTINCT IO_ID), COUNT(DISTINCT SITE)
                FROM QUORUMDB.SEGMENT_DATA.PARAMOUNT_IMPRESSIONS_REPORT_90_DAYS
                WHERE CAST(QUORUM_ADVERTISER_ID AS INTEGER) = %s
                  AND IMP_DATE >= %s AND IMP_DATE < %s
            """
            rows = query(sql, [int(advertiser_id), start, end])
        else:
            rows = [(0, 0, 0, 0, 0)]
        
        r = rows[0] if rows else (0, 0, 0, 0, 0)
        imps, lv, wv = r[0] or 0, r[1] or 0, r[2] or 0
        
        return jsonify({'success': True, 'data': {
            'IMPRESSIONS': imps,
            'LOCATION_VISITS': lv,
            'WEB_VISITS': wv,
            'CAMPAIGN_COUNT': r[3] or 0,
            'PUBLISHER_COUNT': r[4] or 0,
            'LOCATION_VR': pct(lv, imps),
            'VISIT_RATE': pct(wv, imps)
        }})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/campaign-performance')
def get_campaign_performance():
    try:
        agency_id = request.args.get('agency_id')
        advertiser_id = request.args.get('advertiser_id')
        if not agency_id or not advertiser_id:
            return jsonify({'success': False, 'error': 'agency_id and advertiser_id required'}), 400
        
        start, end = dates(request)
        src = get_source(agency_id)
        
        if src == 'QIR':
            sql = """
                SELECT IO_ID, MAX(IO_NAME), COUNT(*),
                       SUM(CASE WHEN IS_STORE_VISIT THEN 1 ELSE 0 END),
                       SUM(CASE WHEN IS_SITE_VISIT THEN 1 ELSE 0 END)
                FROM QUORUMDB.SEGMENT_DATA.QUORUM_IMPRESSIONS_REPORT
                WHERE QUORUM_ADVERTISER_ID = %s AND AGENCY_ID = %s AND LOG_DATE >= %s AND LOG_DATE < %s
                GROUP BY IO_ID HAVING COUNT(*) >= 100 ORDER BY 3 DESC LIMIT 50
            """
            rows = query(sql, [advertiser_id, agency_id, start, end])
        elif src == 'CPRS':
            sql = """
                SELECT IO_ID, MAX(IO_NAME), SUM(IMPRESSIONS), SUM(VISITORS), 0
                FROM QUORUMDB.SEGMENT_DATA.CAMPAIGN_PERFORMANCE_REPORT_WEEKLY_STATS
                WHERE ADVERTISER_ID = %s AND AGENCY_ID = %s AND LOG_DATE >= %s AND LOG_DATE < %s
                GROUP BY IO_ID HAVING SUM(IMPRESSIONS) >= 100 ORDER BY 3 DESC LIMIT 50
            """
            rows = query(sql, [advertiser_id, agency_id, start, end])
        elif src == 'PARAMOUNT':
            sql = """
                SELECT IO_ID, MAX(IO_NAME), COUNT(DISTINCT CACHE_BUSTER),
                       COUNT(DISTINCT CASE WHEN IS_STORE_VISIT = 'TRUE' THEN CACHE_BUSTER END),
                       COUNT(DISTINCT CASE WHEN IS_SITE_VISIT = 'TRUE' THEN WEB_IMPRESSION_ID END)
                FROM QUORUMDB.SEGMENT_DATA.PARAMOUNT_IMPRESSIONS_REPORT_90_DAYS
                WHERE CAST(QUORUM_ADVERTISER_ID AS INTEGER) = %s
                  AND IMP_DATE >= %s AND IMP_DATE < %s
                GROUP BY IO_ID HAVING COUNT(DISTINCT CACHE_BUSTER) >= 100 ORDER BY 3 DESC LIMIT 50
            """
            rows = query(sql, [int(advertiser_id), start, end])
        else:
            rows = []
        
        data = []
        for r in rows:
            imps, lv, wv = r[2] or 0, r[3] or 0, r[4] or 0
            data.append({
                'CAMPAIGN_ID': r[0],
                'CAMPAIGN_NAME': r[1] or str(r[0]),
                'IMPRESSIONS': imps,
                'LOCATION_VISITS': lv,
                'WEB_VISITS': wv,
                'LOCATION_VR': pct(lv, imps)
            })
        
        return jsonify({'success': True, 'data': data})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/publisher-performance')
def get_publisher_performance():
    try:
        agency_id = request.args.get('agency_id')
        advertiser_id = request.args.get('advertiser_id')
        if not agency_id or not advertiser_id:
            return jsonify({'success': False, 'error': 'agency_id and advertiser_id required'}), 400
        
        start, end = dates(request)
        src = get_source(agency_id)
        
        if src == 'QIR':
            sql = """
                SELECT PUBLISHER_CODE, COUNT(*),
                       SUM(CASE WHEN IS_STORE_VISIT THEN 1 ELSE 0 END),
                       SUM(CASE WHEN IS_SITE_VISIT THEN 1 ELSE 0 END)
                FROM QUORUMDB.SEGMENT_DATA.QUORUM_IMPRESSIONS_REPORT
                WHERE QUORUM_ADVERTISER_ID = %s AND AGENCY_ID = %s AND LOG_DATE >= %s AND LOG_DATE < %s
                GROUP BY PUBLISHER_CODE HAVING COUNT(*) >= 100 ORDER BY 2 DESC LIMIT 50
            """
            rows = query(sql, [advertiser_id, agency_id, start, end])
        elif src == 'CPRS':
            sql = """
                SELECT PUBLISHER, SUM(IMPRESSIONS), SUM(VISITORS), 0
                FROM QUORUMDB.SEGMENT_DATA.CAMPAIGN_PERFORMANCE_REPORT_WEEKLY_STATS
                WHERE ADVERTISER_ID = %s AND AGENCY_ID = %s AND LOG_DATE >= %s AND LOG_DATE < %s
                GROUP BY PUBLISHER HAVING SUM(IMPRESSIONS) >= 100 ORDER BY 2 DESC LIMIT 50
            """
            rows = query(sql, [advertiser_id, agency_id, start, end])
        elif src == 'PARAMOUNT':
            sql = """
                SELECT SITE, COUNT(DISTINCT CACHE_BUSTER),
                       COUNT(DISTINCT CASE WHEN IS_STORE_VISIT = 'TRUE' THEN CACHE_BUSTER END),
                       COUNT(DISTINCT CASE WHEN IS_SITE_VISIT = 'TRUE' THEN WEB_IMPRESSION_ID END)
                FROM QUORUMDB.SEGMENT_DATA.PARAMOUNT_IMPRESSIONS_REPORT_90_DAYS
                WHERE CAST(QUORUM_ADVERTISER_ID AS INTEGER) = %s
                  AND IMP_DATE >= %s AND IMP_DATE < %s
                GROUP BY SITE HAVING COUNT(DISTINCT CACHE_BUSTER) >= 100 ORDER BY 2 DESC LIMIT 50
            """
            rows = query(sql, [int(advertiser_id), start, end])
        else:
            rows = []
        
        data = []
        for r in rows:
            imps, lv, wv = r[1] or 0, r[2] or 0, r[3] or 0
            pub_name = decode_publisher(r[0])
            data.append({
                'PUBLISHER': pub_name,
                'PUBLISHER_CODE': pub_name,
                'IMPRESSIONS': imps,
                'LOCATION_VISITS': lv,
                'WEB_VISITS': wv,
                'LOCATION_VR': pct(lv, imps),
                'WEB_VR': pct(wv, imps)
            })
        
        return jsonify({'success': True, 'data': data})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/zip-performance')
def get_zip_performance():
    try:
        agency_id = request.args.get('agency_id')
        advertiser_id = request.args.get('advertiser_id')
        if not agency_id or not advertiser_id:
            return jsonify({'success': False, 'error': 'agency_id and advertiser_id required'}), 400
        
        start, end = dates(request)
        src = get_source(agency_id)
        logger.info(f"zip-performance: agency={agency_id}, advertiser={advertiser_id}, src={src}, dates={start} to {end}")
        
        if src == 'QIR':
            sql = """
                SELECT q.ZIP_CODE, COUNT(*),
                       SUM(CASE WHEN q.IS_STORE_VISIT THEN 1 ELSE 0 END),
                       MAX(z.CITY_NAME), MAX(z.STATE_ABBREVIATION)
                FROM QUORUMDB.SEGMENT_DATA.QUORUM_IMPRESSIONS_REPORT q
                LEFT JOIN QUORUMDB.REF_DATA.ZIP_POPULATION_DATA z ON q.ZIP_CODE = z.ZIP_CODE
                WHERE q.QUORUM_ADVERTISER_ID = %s AND q.AGENCY_ID = %s 
                  AND q.LOG_DATE >= %s AND q.LOG_DATE < %s
                  AND q.ZIP_CODE IS NOT NULL AND q.ZIP_CODE != ''
                GROUP BY q.ZIP_CODE HAVING COUNT(*) >= 100 ORDER BY 2 DESC LIMIT 50
            """
            rows = query(sql, [advertiser_id, agency_id, start, end])
        elif src == 'CPRS':
            sql = """
                SELECT w.ZIP, SUM(w.IMPRESSIONS), SUM(w.VISITORS),
                       MAX(z.CITY_NAME), MAX(z.STATE_ABBREVIATION)
                FROM QUORUMDB.SEGMENT_DATA.CAMPAIGN_PERFORMANCE_REPORT_WEEKLY_STATS w
                LEFT JOIN QUORUMDB.REF_DATA.ZIP_POPULATION_DATA z ON w.ZIP = z.ZIP_CODE
                WHERE w.ADVERTISER_ID = %s AND w.AGENCY_ID = %s 
                  AND w.LOG_DATE >= %s AND w.LOG_DATE < %s
                  AND w.ZIP IS NOT NULL AND w.ZIP != ''
                GROUP BY w.ZIP HAVING SUM(w.IMPRESSIONS) >= 100 ORDER BY 2 DESC LIMIT 50
            """
            rows = query(sql, [advertiser_id, agency_id, start, end])
        elif src == 'PARAMOUNT':
            sql = """
                SELECT p.ZIP_CODE, COUNT(DISTINCT p.CACHE_BUSTER),
                       COUNT(DISTINCT CASE WHEN p.IS_STORE_VISIT = 'TRUE' THEN p.CACHE_BUSTER END),
                       MAX(z.CITY_NAME), MAX(z.STATE_ABBREVIATION)
                FROM QUORUMDB.SEGMENT_DATA.PARAMOUNT_IMPRESSIONS_REPORT_90_DAYS p
                LEFT JOIN QUORUMDB.REF_DATA.ZIP_POPULATION_DATA z ON p.ZIP_CODE = z.ZIP_CODE
                WHERE CAST(p.QUORUM_ADVERTISER_ID AS INTEGER) = %s 
                  AND p.IMP_DATE >= %s AND p.IMP_DATE < %s
                  AND p.ZIP_CODE IS NOT NULL AND p.ZIP_CODE != ''
                GROUP BY p.ZIP_CODE HAVING COUNT(DISTINCT p.CACHE_BUSTER) >= 100 ORDER BY 2 DESC LIMIT 50
            """
            rows = query(sql, [int(advertiser_id), start, end])
        else:
            rows = []
        
        data = []
        for r in rows:
            imps, lv = r[1] or 0, r[2] or 0
            data.append({
                'ZIP_CODE': r[0],
                'IMPRESSIONS': imps,
                'LOCATION_VISITS': lv,
                'CITY': r[3],
                'STATE': r[4],
                'LOCATION_VR': pct(lv, imps)
            })
        
        return jsonify({'success': True, 'data': data})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=False)
