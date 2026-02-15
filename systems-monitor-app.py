"""
Quorum Systems Monitor API
===========================
Live Snowflake-backed data transfer health monitoring.
Provides endpoints for client pipeline health, gap analysis,
and table access tracking.

Clients monitored:
  - Attain (Snowflake Share)
  - LotLinx (S3 + Snowflake)
  - Paramount (S3 + Snowflake)
  - Causal IQ (Weekly Stats)
  - MNTN (XANDR + Weekly Stats + S3)
"""
from flask import Flask, jsonify, request, send_file
from flask_cors import CORS
import snowflake.connector
import os
from datetime import datetime, timedelta, date
import time
import threading
import json

app = Flask(__name__)
CORS(app)

# =============================================================================
# CONFIGURATION
# =============================================================================
CLIENT_CONFIG = {
    'attain': {
        'name': 'Attain',
        'mechanism': 'Snowflake Share',
        'mechanismDetail': 'Outbound share SHARE_TO_ATTAIN → account UK76987',
        'badges': ['snowflake'],
        'revenue': '$6,000/mo (pilot)',
        'tables': [
            {
                'name': 'SEGMENT_MAID_WITH_IP',
                'schema': 'QUORUM_CROSS_CLOUD.ATTAIN_FEED',
                'dateColumn': 'CREATED_AT',
                'cadence': 'daily',
                'countQuery': "SELECT COUNT(*) FROM QUORUM_CROSS_CLOUD.ATTAIN_FEED.SEGMENT_MAID_WITH_IP",
                'freshnessQuery': "SELECT MAX(CREATED_AT)::DATE FROM QUORUM_CROSS_CLOUD.ATTAIN_FEED.SEGMENT_MAID_WITH_IP",
                'recentQuery': "SELECT COUNT(*) FROM QUORUM_CROSS_CLOUD.ATTAIN_FEED.SEGMENT_MAID_WITH_IP WHERE CREATED_AT >= CURRENT_DATE()",
                'volumeQuery': """
                    SELECT CREATED_AT::DATE as dt, COUNT(*) as vol
                    FROM QUORUM_CROSS_CLOUD.ATTAIN_FEED.SEGMENT_MAID_WITH_IP
                    WHERE CREATED_AT >= DATEADD('day', -14, CURRENT_DATE())
                    GROUP BY dt ORDER BY dt
                """,
            },
            {
                'name': 'MAID_CENTROID_ASSOCIATION',
                'schema': 'QUORUM_CROSS_CLOUD.ATTAIN_FEED',
                'dateColumn': None,
                'cadence': 'refresh',
                'countQuery': "SELECT COUNT(*) FROM QUORUM_CROSS_CLOUD.ATTAIN_FEED.MAID_CENTROID_ASSOCIATION",
                'freshnessQuery': None,
                'note': 'Reference table — match rate check recommended'
            },
            {
                'name': 'HOUSE_HOLD_MAPPING',
                'schema': 'QUORUM_CROSS_CLOUD.ATTAIN_FEED',
                'dateColumn': None,
                'cadence': 'refresh',
                'countQuery': "SELECT COUNT(*) FROM QUORUM_CROSS_CLOUD.ATTAIN_FEED.HOUSE_HOLD_MAPPING",
                'freshnessQuery': None,
                'note': 'Household data — manual refresh process, stale since Nov 2025'
            },
            {
                'name': 'SEGMENT_META_DATA',
                'schema': 'QUORUM_CROSS_CLOUD.ATTAIN_FEED',
                'dateColumn': None,
                'cadence': 'refresh',
                'countQuery': "SELECT COUNT(*) FROM QUORUM_CROSS_CLOUD.ATTAIN_FEED.SEGMENT_META_DATA",
                'freshnessQuery': None,
            },
        ]
    },
    'lotlinx': {
        'name': 'LotLinx',
        'mechanism': 'S3 + Snowflake',
        'mechanismDetail': 'S3 bucket: lotlinx-webpixeldata-filedrop → Snowflake tables',
        'badges': ['s3', 'snowflake'],
        'revenue': '$24,000/mo',
        'tables': [
            {
                'name': 'SEGMENT_DEVICES_CAPTURED_LOTLINX_BACKUP',
                'schema': 'QUORUMDB.SEGMENT_DATA',
                'dateColumn': 'DRIVE_BY_DATE',
                'cadence': 'daily',
                'freshnessQuery': "SELECT MAX(DRIVE_BY_DATE)::DATE FROM QUORUMDB.SEGMENT_DATA.SEGMENT_DEVICES_CAPTURED_LOTLINX_BACKUP",
                'volumeQuery': """
                    SELECT DRIVE_BY_DATE::DATE as dt, COUNT(*) as vol
                    FROM QUORUMDB.SEGMENT_DATA.SEGMENT_DEVICES_CAPTURED_LOTLINX_BACKUP
                    WHERE DRIVE_BY_DATE >= DATEADD('day', -14, CURRENT_DATE())
                    GROUP BY dt ORDER BY dt
                """,
                'note': 'Backfills in ~3 batch loads over 4-6 days. Cross-check LOTLINX_QUORUMIMP_DATA for true volumes.'
            }
        ]
    },
    'paramount': {
        'name': 'Paramount',
        'mechanism': 'S3 + Snowflake',
        'mechanismDetail': 'S3 buckets: paramount-retargeting-files, quorum-paramount-data',
        'badges': ['s3', 'snowflake'],
        'revenue': '$43,000–$55,000/mo',
        'tables': [
            {
                'name': 'PARAMOUNT_IMPRESSIONS_REPORT_90_DAYS',
                'schema': 'QUORUMDB.SEGMENT_DATA',
                'dateColumn': 'IMP_DATE',
                'cadence': 'daily',
                'freshnessQuery': "SELECT MAX(IMP_DATE)::DATE FROM QUORUMDB.SEGMENT_DATA.PARAMOUNT_IMPRESSIONS_REPORT_90_DAYS",
                'volumeQuery': """
                    SELECT IMP_DATE::DATE as dt, COUNT(*) as vol
                    FROM QUORUMDB.SEGMENT_DATA.PARAMOUNT_IMPRESSIONS_REPORT_90_DAYS
                    WHERE IMP_DATE >= DATEADD('day', -14, CURRENT_DATE())
                    GROUP BY dt ORDER BY dt
                """,
            },
            {
                'name': 'PARAMOUNT_MAPPED_IMPRESSIONS',
                'schema': 'QUORUMDB.SEGMENT_DATA',
                'dateColumn': 'IMP_DATE',
                'cadence': 'daily',
                'freshnessQuery': "SELECT MAX(IMP_DATE)::DATE FROM QUORUMDB.SEGMENT_DATA.PARAMOUNT_MAPPED_IMPRESSIONS",
                'countQuery': "SELECT COUNT(*) FROM QUORUMDB.SEGMENT_DATA.PARAMOUNT_MAPPED_IMPRESSIONS",
            },
            {
                'name': 'PARAMOUNT_SITEVISITS',
                'schema': 'QUORUMDB.SEGMENT_DATA',
                'dateColumn': 'SITE_VISIT_TIMESTAMP',
                'cadence': 'daily',
                'freshnessQuery': "SELECT MAX(SITE_VISIT_TIMESTAMP)::DATE FROM QUORUMDB.SEGMENT_DATA.PARAMOUNT_SITEVISITS",
            },
            {
                'name': 'PARAMOUNT_LEADS',
                'schema': 'QUORUMDB.SEGMENT_DATA',
                'dateColumn': 'LEAD_TIMESTAMP',
                'cadence': 'daily',
                'freshnessQuery': "SELECT MAX(LEAD_TIMESTAMP)::DATE FROM QUORUMDB.SEGMENT_DATA.PARAMOUNT_LEADS",
            },
            {
                'name': 'PARAMOUNT_CUSTOM_AUDIENCE_DELIVERY',
                'schema': 'QUORUMDB.SEGMENT_DATA',
                'dateColumn': 'LAST_IMPRESSION_PIXEL_FIRED',
                'cadence': 'daily',
                'freshnessQuery': "SELECT MAX(LAST_IMPRESSION_PIXEL_FIRED)::DATE FROM QUORUMDB.SEGMENT_DATA.PARAMOUNT_CUSTOM_AUDIENCE_DELIVERY",
                'countQuery': "SELECT COUNT(*) FROM QUORUMDB.SEGMENT_DATA.PARAMOUNT_CUSTOM_AUDIENCE_DELIVERY",
            },
        ]
    },
    'causal': {
        'name': 'Causal IQ',
        'mechanism': 'Weekly Stats',
        'mechanismDetail': 'Agency 1813 → CAMPAIGN_PERFORMANCE_REPORT_WEEKLY_STATS',
        'badges': ['weekly', 'snowflake'],
        'revenue': '$18,000–$30,000/mo',
        'tables': [
            {
                'name': 'CAMPAIGN_PERFORMANCE_REPORT_WEEKLY_STATS',
                'schema': 'QUORUMDB.SEGMENT_DATA',
                'dateColumn': 'LOG_DATE',
                'cadence': 'weekly',
                'freshnessQuery': "SELECT MAX(LOG_DATE)::DATE FROM QUORUMDB.SEGMENT_DATA.CAMPAIGN_PERFORMANCE_REPORT_WEEKLY_STATS WHERE AGENCY_ID = 1813",
                'countQuery': "SELECT COUNT(*) FROM QUORUMDB.SEGMENT_DATA.CAMPAIGN_PERFORMANCE_REPORT_WEEKLY_STATS WHERE AGENCY_ID = 1813",
                'volumeQuery': """
                    SELECT LOG_DATE::DATE as dt, COUNT(*) as vol
                    FROM QUORUMDB.SEGMENT_DATA.CAMPAIGN_PERFORMANCE_REPORT_WEEKLY_STATS
                    WHERE AGENCY_ID = 1813 AND LOG_DATE >= DATEADD('day', -60, CURRENT_DATE())
                    GROUP BY dt ORDER BY dt
                """,
                'extraQuery': "SELECT COUNT(DISTINCT ADVERTISER_ID) FROM QUORUMDB.SEGMENT_DATA.CAMPAIGN_PERFORMANCE_REPORT_WEEKLY_STATS WHERE AGENCY_ID = 1813",
                'note': 'Agency 1813 — weekly cadence (Sun–Sat), LOG_DATE = week-end date'
            }
        ]
    },
    'mntn': {
        'name': 'MNTN',
        'mechanism': 'XANDR + Weekly Stats + S3',
        'mechanismDetail': 'PT=22 in XANDR_IMPRESSION_LOG + Agency 2514 weekly stats + S3 mntn-data-drop',
        'badges': ['xandr', 'weekly', 's3'],
        'revenue': '$3,000–$18,000/mo',
        'tables': [
            {
                'name': 'XANDR_IMPRESSION_LOG (PT=22)',
                'schema': 'QUORUMDB.SEGMENT_DATA',
                'dateColumn': 'TIMESTAMP',
                'cadence': 'daily',
                'freshnessQuery': "SELECT MAX(CAST(TIMESTAMP AS DATE)) FROM QUORUMDB.SEGMENT_DATA.XANDR_IMPRESSION_LOG WHERE PT = 22 AND CAST(TIMESTAMP AS DATE) <= CURRENT_DATE()",
                'volumeQuery': """
                    SELECT CAST(TIMESTAMP AS DATE) as dt, COUNT(*) as vol
                    FROM QUORUMDB.SEGMENT_DATA.XANDR_IMPRESSION_LOG
                    WHERE PT = 22 AND CAST(TIMESTAMP AS DATE) BETWEEN DATEADD('day', -14, CURRENT_DATE()) AND CURRENT_DATE()
                    GROUP BY dt ORDER BY dt
                """,
                'note': '100% of PT=22 rows have null/0 PUBLISHER_ID — MNTN platform-level data gap'
            },
            {
                'name': 'CAMPAIGN_PERFORMANCE_REPORT_WEEKLY_STATS',
                'schema': 'QUORUMDB.SEGMENT_DATA',
                'dateColumn': 'LOG_DATE',
                'cadence': 'weekly',
                'freshnessQuery': "SELECT MAX(LOG_DATE)::DATE FROM QUORUMDB.SEGMENT_DATA.CAMPAIGN_PERFORMANCE_REPORT_WEEKLY_STATS WHERE AGENCY_ID = 2514",
                'countQuery': "SELECT COUNT(*) FROM QUORUMDB.SEGMENT_DATA.CAMPAIGN_PERFORMANCE_REPORT_WEEKLY_STATS WHERE AGENCY_ID = 2514",
                'volumeQuery': """
                    SELECT LOG_DATE::DATE as dt, COUNT(*) as vol
                    FROM QUORUMDB.SEGMENT_DATA.CAMPAIGN_PERFORMANCE_REPORT_WEEKLY_STATS
                    WHERE AGENCY_ID = 2514 AND LOG_DATE >= DATEADD('day', -60, CURRENT_DATE())
                    GROUP BY dt ORDER BY dt
                """,
                'extraQuery': "SELECT COUNT(DISTINCT ADVERTISER_ID) FROM QUORUMDB.SEGMENT_DATA.CAMPAIGN_PERFORMANCE_REPORT_WEEKLY_STATS WHERE AGENCY_ID = 2514",
                'note': 'Agency 2514 — weekly cadence'
            }
        ]
    }
}

# =============================================================================
# SNOWFLAKE CONNECTION
# =============================================================================
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
                time.sleep(1)
                continue
            raise

def run_query(conn, sql, params=None):
    """Execute query and return results."""
    cur = conn.cursor()
    try:
        if params:
            cur.execute(sql, params)
        else:
            cur.execute(sql)
        cols = [d[0] for d in cur.description] if cur.description else []
        rows = cur.fetchall()
        return cols, rows
    finally:
        cur.close()

# =============================================================================
# IN-MEMORY CACHE (health data is expensive to gather)
# =============================================================================
_cache = {}
_cache_lock = threading.Lock()
CACHE_TTL = 300  # 5 minutes

def cache_get(key):
    with _cache_lock:
        entry = _cache.get(key)
        if entry and time.time() - entry['ts'] < CACHE_TTL:
            return entry['data']
    return None

def cache_set(key, data):
    with _cache_lock:
        _cache[key] = {'data': data, 'ts': time.time()}

# =============================================================================
# ENDPOINTS
# =============================================================================

@app.route('/')
def index():
    """Serve the monitoring dashboard."""
    return send_file('data-transfer-health.html')

@app.route('/api/health')
def health_check():
    return jsonify({'status': 'ok', 'service': 'quorum-systems-monitor', 'timestamp': datetime.utcnow().isoformat()})

@app.route('/api/client-health')
def client_health():
    """
    Fetch live health data for all clients by querying Snowflake.
    Returns freshness, row counts, and daily volumes per table per client.
    """
    client_filter = request.args.get('client', None)
    cache_key = f"client_health_{client_filter or 'all'}"
    cached = cache_get(cache_key)
    if cached:
        return jsonify({'success': True, 'data': cached, 'cached': True})

    try:
        conn = get_snowflake_connection()
    except Exception as e:
        return jsonify({'success': False, 'error': f'Snowflake connection failed: {str(e)}'}), 500

    try:
        results = {}
        clients_to_check = {client_filter: CLIENT_CONFIG[client_filter]} if client_filter and client_filter in CLIENT_CONFIG else CLIENT_CONFIG

        for client_key, client_cfg in clients_to_check.items():
            client_result = {
                'name': client_cfg['name'],
                'mechanism': client_cfg['mechanism'],
                'mechanismDetail': client_cfg['mechanismDetail'],
                'badges': client_cfg['badges'],
                'revenue': client_cfg['revenue'],
                'tables': []
            }

            for table_cfg in client_cfg['tables']:
                table_result = {
                    'name': table_cfg['name'],
                    'schema': table_cfg.get('schema', ''),
                    'dateColumn': table_cfg.get('dateColumn'),
                    'cadence': table_cfg.get('cadence', 'daily'),
                    'note': table_cfg.get('note', ''),
                    'latestDate': None,
                    'totalRows': None,
                    'recentRows': None,
                    'dailyVolumes': None,
                    'advertisers': None,
                    'status': 'healthy',
                }

                # Freshness
                if table_cfg.get('freshnessQuery'):
                    try:
                        _, rows = run_query(conn, table_cfg['freshnessQuery'])
                        if rows and rows[0][0]:
                            val = rows[0][0]
                            table_result['latestDate'] = str(val)[:10] if hasattr(val, 'isoformat') else str(val)[:10]
                    except Exception as e:
                        table_result['note'] = f"Freshness query failed: {str(e)[:100]}"

                # Row count
                if table_cfg.get('countQuery'):
                    try:
                        _, rows = run_query(conn, table_cfg['countQuery'])
                        if rows and rows[0][0] is not None:
                            table_result['totalRows'] = rows[0][0]
                    except:
                        pass

                # Recent rows
                if table_cfg.get('recentQuery'):
                    try:
                        _, rows = run_query(conn, table_cfg['recentQuery'])
                        if rows and rows[0][0] is not None:
                            table_result['recentRows'] = rows[0][0]
                    except:
                        pass

                # Daily volumes
                if table_cfg.get('volumeQuery'):
                    try:
                        _, rows = run_query(conn, table_cfg['volumeQuery'])
                        if rows:
                            table_result['dailyVolumes'] = [
                                {'date': str(r[0])[:10], 'count': r[1]}
                                for r in rows
                            ]
                    except:
                        pass

                # Advertiser count (for weekly stats)
                if table_cfg.get('extraQuery'):
                    try:
                        _, rows = run_query(conn, table_cfg['extraQuery'])
                        if rows and rows[0][0] is not None:
                            table_result['advertisers'] = rows[0][0]
                    except:
                        pass

                # Compute status from freshness
                if table_result['latestDate']:
                    try:
                        latest = datetime.strptime(table_result['latestDate'], '%Y-%m-%d').date()
                        days_stale = (date.today() - latest).days
                        if table_result['cadence'] == 'weekly':
                            table_result['status'] = 'healthy' if days_stale <= 8 else ('warning' if days_stale <= 14 else 'critical')
                        else:
                            table_result['status'] = 'healthy' if days_stale <= 1 else ('warning' if days_stale <= 3 else 'critical')
                    except:
                        pass
                elif table_result['cadence'] == 'refresh':
                    table_result['status'] = 'healthy'

                client_result['tables'].append(table_result)

            # Overall client status
            statuses = [t['status'] for t in client_result['tables']]
            if 'critical' in statuses:
                client_result['overallStatus'] = 'critical'
            elif 'warning' in statuses:
                client_result['overallStatus'] = 'warning'
            else:
                client_result['overallStatus'] = 'healthy'

            results[client_key] = client_result

        conn.close()
        cache_set(cache_key, results)
        return jsonify({
            'success': True,
            'data': results,
            'timestamp': datetime.utcnow().isoformat(),
            'cached': False
        })

    except Exception as e:
        conn.close()
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/snowflake-tasks')
def snowflake_tasks():
    """Get status of all Snowflake tasks."""
    cached = cache_get('snowflake_tasks')
    if cached:
        return jsonify({'success': True, 'data': cached, 'cached': True})

    try:
        conn = get_snowflake_connection()
        cur = conn.cursor()
        cur.execute("SHOW TASKS IN SCHEMA QUORUMDB.SEGMENT_DATA")
        cols = [d[0] for d in cur.description]
        rows = cur.fetchall()
        tasks = []
        for row in rows:
            task = dict(zip(cols, row))
            tasks.append({
                'name': task.get('name', ''),
                'state': task.get('state', ''),
                'schedule': task.get('schedule', ''),
                'warehouse': task.get('warehouse', ''),
                'definition': task.get('definition', '')[:200],
            })
        cur.close()
        conn.close()
        cache_set('snowflake_tasks', tasks)
        return jsonify({'success': True, 'data': tasks})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/table-access')
def table_access():
    """
    Track which tables are being queried and by whom.
    Uses SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY with CASE WHEN pattern matching.
    """
    cached = cache_get('table_access')
    if cached:
        return jsonify({'success': True, 'data': cached, 'cached': True})

    tracked_tables = [
        ('AD_IMPRESSION_LOG_V2', 'Ad Impressions'),
        ('STORE_VISITS', 'Store Visits'),
        ('WEBPIXEL_IMPRESSION_LOG', 'Web Pixel Raw'),
        ('WEBPIXEL_EVENTS', 'Web Pixel Events'),
        ('SEGMENT_DEVICES_CAPTURED', 'Segment Devices'),
        ('XANDR_IMPRESSION_LOG', 'Xandr Impressions'),
        ('CAMPAIGN_PERFORMANCE_REPORT_WEEKLY_STATS', 'Campaign Weekly Stats'),
        ('PARAMOUNT_IMPRESSIONS_REPORT_90_DAYS', 'Paramount 90-Day'),
        ('AGENCY_ADVERTISER', 'Agency Advertiser'),
        ('LIVERAMP_IMPRESSION_LOG', 'LiveRamp Impressions'),
        ('SEGMENT_MAID_WITH_IP', 'Attain Feed'),
        ('PARAMOUNT_SITEVISITS', 'Paramount Site Visits'),
        ('PARAMOUNT_LEADS', 'Paramount Leads'),
        ('LOTLINX_QUORUMIMP_DATA', 'LotLinx Raw'),
    ]

    case_cols = ",\n".join(
        f"SUM(CASE WHEN UPPER(QUERY_TEXT) LIKE '%{t[0]}%' THEN 1 ELSE 0 END) AS \"{t[0]}\""
        for t in tracked_tables
    )

    sql = f"""
        SELECT
            TO_DATE(START_TIME) AS query_date,
            {case_cols}
        FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
        WHERE START_TIME >= DATEADD('day', -7, CURRENT_TIMESTAMP())
          AND QUERY_TYPE = 'SELECT'
          AND EXECUTION_STATUS = 'SUCCESS'
          AND DATABASE_NAME = 'QUORUMDB'
        GROUP BY query_date
        ORDER BY query_date
    """

    try:
        conn = get_snowflake_connection()
        cols, rows = run_query(conn, sql)

        # Pivot into per-table results with anomaly detection
        table_results = []
        for t_name, t_label in tracked_tables:
            col_idx = cols.index(t_name) if t_name in cols else None
            if col_idx is None:
                continue

            daily_counts = []
            for row in rows:
                daily_counts.append({
                    'date': str(row[0]),
                    'queries': row[col_idx] or 0
                })

            # Anomaly detection
            counts = [d['queries'] for d in daily_counts]
            avg_count = sum(counts[:-1]) / max(len(counts) - 1, 1) if len(counts) > 1 else (counts[0] if counts else 0)
            today_count = counts[-1] if counts else 0

            if avg_count > 10 and today_count == 0:
                signal = 'silent'
            elif avg_count > 10 and today_count < avg_count * 0.3:
                signal = 'drop'
            elif avg_count > 0 and today_count > avg_count * 3:
                signal = 'spike'
            else:
                signal = 'normal'

            table_results.append({
                'table': t_name,
                'label': t_label,
                'daily': daily_counts,
                'avgQueries': round(avg_count, 1),
                'todayQueries': today_count,
                'signal': signal,
            })

        conn.close()
        cache_set('table_access', table_results)
        return jsonify({'success': True, 'data': table_results})
    except Exception as e:
        error_msg = str(e)
        if 'not authorized' in error_msg.lower() or 'does not exist' in error_msg.lower():
            return jsonify({
                'success': False,
                'error': 'IMPORTED PRIVILEGES on SNOWFLAKE database needed',
                'grant_needed': True
            })
        return jsonify({'success': False, 'error': error_msg}), 500


@app.route('/api/table-access-users')
def table_access_users():
    """Who is querying each table? Breakdown by user."""
    cached = cache_get('table_access_users')
    if cached:
        return jsonify({'success': True, 'data': cached, 'cached': True})

    sql = """
        SELECT
            USER_NAME,
            COUNT(*) as query_count,
            MAX(START_TIME) as last_query
        FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
        WHERE START_TIME >= DATEADD('day', -7, CURRENT_TIMESTAMP())
          AND QUERY_TYPE = 'SELECT'
          AND EXECUTION_STATUS = 'SUCCESS'
          AND DATABASE_NAME = 'QUORUMDB'
        GROUP BY USER_NAME
        ORDER BY query_count DESC
    """
    try:
        conn = get_snowflake_connection()
        cols, rows = run_query(conn, sql)
        users = []
        for row in rows:
            users.append({
                'user': row[0],
                'queryCount': row[1],
                'lastQuery': str(row[2])[:19] if row[2] else None
            })
        conn.close()
        cache_set('table_access_users', users)
        return jsonify({'success': True, 'data': users})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/lotlinx-crosscheck')
def lotlinx_crosscheck():
    """Cross-validate LotLinx BACKUP vs QUORUMIMP_DATA to detect real issues vs backfill lag."""
    cached = cache_get('lotlinx_crosscheck')
    if cached:
        return jsonify({'success': True, 'data': cached, 'cached': True})

    sql = """
        SELECT 'BACKUP' AS source, DATE(DRIVE_BY_DATE) AS dt, COUNT(*) AS vol
        FROM QUORUMDB.SEGMENT_DATA.SEGMENT_DEVICES_CAPTURED_LOTLINX_BACKUP
        WHERE DRIVE_BY_DATE >= DATEADD('day', -7, CURRENT_DATE())
        GROUP BY dt
        UNION ALL
        SELECT 'QUORUMIMP' AS source, DATE(IMPRESSION_TIMESTAMP) AS dt, COUNT(*) AS vol
        FROM QUORUMDB.LOTLINX.LOTLINX_QUORUMIMP_DATA
        WHERE IMPRESSION_TIMESTAMP >= DATEADD('day', -7, CURRENT_DATE())
        GROUP BY dt
        ORDER BY dt, source
    """
    try:
        conn = get_snowflake_connection()
        _, rows = run_query(conn, sql)
        data = [{'source': r[0], 'date': str(r[1])[:10], 'volume': r[2]} for r in rows]
        conn.close()
        cache_set('lotlinx_crosscheck', data)
        return jsonify({'success': True, 'data': data})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/mntn-publisher-null-rate')
def mntn_publisher_null_rate():
    """Check MNTN publisher null rate — known 100% null issue."""
    cached = cache_get('mntn_null_rate')
    if cached:
        return jsonify({'success': True, 'data': cached, 'cached': True})

    sql = """
        SELECT CAST(TIMESTAMP AS DATE) as dt, COUNT(*) as total,
            SUM(CASE WHEN PUBLISHER_ID IS NULL OR PUBLISHER_ID = 0 THEN 1 ELSE 0 END) as null_pub
        FROM QUORUMDB.SEGMENT_DATA.XANDR_IMPRESSION_LOG
        WHERE PT = 22 AND CAST(TIMESTAMP AS DATE) BETWEEN DATEADD('day', -3, CURRENT_DATE()) AND CURRENT_DATE()
        GROUP BY dt ORDER BY dt
    """
    try:
        conn = get_snowflake_connection()
        _, rows = run_query(conn, sql)
        data = [{'date': str(r[0])[:10], 'total': r[1], 'nullPublisher': r[2], 'nullPct': round(r[2]*100.0/max(r[1],1), 1)} for r in rows]
        conn.close()
        cache_set('mntn_null_rate', data)
        return jsonify({'success': True, 'data': data})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


# =============================================================================
# MAIN
# =============================================================================
if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8080))
    app.run(host='0.0.0.0', port=port, debug=False)
