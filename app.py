"""
Quorum Optimizer API v5 - Unified View Architecture
====================================================
Uses V5 views for simplified, consistent data access:
- V5_ALL_VISITS: Combined store + web visits (all agencies)
- V5_STORE_VISITS_ENRICHED: Store visits with campaign metadata
- V5_STORE_VISITS_WITH_HOUSEHOLD: Store visits + household attribution
- V5_WEB_VISITS_PARAMOUNT: Web visits for Paramount (agency 1480)

Data Sources by Agency:
- Class A (MNTN 2514): QUORUM_ADV_STORE_VISITS via V5_STORE_VISITS_ENRICHED
- Class B (Causal iQ, Magnite, etc.): CPSV_RAW via V5_STORE_VISITS_ENRICHED  
- Paramount (1480): PARAMOUNT_IMP_STORE_VISITS + WEBPIXEL via V5_ALL_VISITS
"""

import os
from flask import Flask, request, jsonify
from flask_cors import CORS
import snowflake.connector
from datetime import datetime, timedelta

app = Flask(__name__)
CORS(app)

# =============================================================================
# CONFIGURATION
# =============================================================================

# Agency name mapping (hardcoded since no proper agency table exists)
AGENCY_NAMES = {
    1480: "Paramount",
    1813: "Causal iQ", 
    2514: "MNTN",
    2234: "Magnite",
    1972: "Hearst",
    2379: "The Shipyard",
    1445: "Publicis",
    2744: "Parallel Path",
    2691: "Agency 2691",
    1956: "Dealer Spike",
    2298: "InteractRV",
    1955: "ARI",
    2086: "Level5",
    1950: "ByRider",
    1880: "TeamSnap"
}

def get_agency_name(agency_id):
    """Get agency name from mapping"""
    return AGENCY_NAMES.get(int(agency_id), f"Agency {agency_id}")

def get_snowflake_connection():
    return snowflake.connector.connect(
        user=os.environ.get('SNOWFLAKE_USER', 'OPTIMIZER_SERVICE_USER'),
        password=os.environ.get('SNOWFLAKE_PASSWORD'),
        account=os.environ.get('SNOWFLAKE_ACCOUNT', 'FZB05958.us-east-1'),
        warehouse=os.environ.get('SNOWFLAKE_WAREHOUSE', 'COMPUTE_WH'),
        database='QUORUMDB',
        schema='SEGMENT_DATA'
    )

def parse_date(date_str):
    """Validate and parse date string to prevent SQL injection"""
    if not date_str:
        return None
    try:
        datetime.strptime(date_str, '%Y-%m-%d')
        return date_str
    except (ValueError, AttributeError):
        return None

def get_default_dates():
    """Return default 30-day date range"""
    end_date = datetime.now().strftime('%Y-%m-%d')
    start_date = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
    return start_date, end_date

# =============================================================================
# HEALTH CHECK
# =============================================================================

@app.route('/health', methods=['GET'])
def health():
    return jsonify({
        'status': 'healthy',
        'version': 'v5-unified',
        'views': [
            'V5_ALL_VISITS',
            'V5_STORE_VISITS_ENRICHED', 
            'V5_STORE_VISITS_WITH_HOUSEHOLD',
            'V5_WEB_VISITS_PARAMOUNT',
            'V5_STORE_VISITS_PARAMOUNT'
        ],
        'data_sources': {
            'class_a': 'QUORUM_ADV_STORE_VISITS (MNTN)',
            'class_b': 'CAMPAIGN_PERFORMANCE_STORE_VISITS_RAW (Causal iQ, Magnite, etc.)',
            'paramount': 'PARAMOUNT_IMP_STORE_VISITS + WEBPIXEL_IMPRESSION_LOG'
        }
    })

# =============================================================================
# AGENCIES ENDPOINT
# =============================================================================

@app.route('/api/v5/agencies', methods=['GET'])
def get_agencies():
    """Get all agencies with visit counts"""
    try:
        conn = get_snowflake_connection()
        cursor = conn.cursor()
        
        start_date, end_date = get_default_dates()
        start_date = parse_date(request.args.get('start_date')) or start_date
        end_date = parse_date(request.args.get('end_date')) or end_date
        
        query = """
            SELECT 
                AGENCY_ID,
                COUNT(DISTINCT ADVERTISER_ID) as ADVERTISER_COUNT,
                SUM(CASE WHEN VISIT_TYPE = 'STORE' THEN 1 ELSE 0 END) as STORE_VISITS,
                SUM(CASE WHEN VISIT_TYPE = 'WEB' THEN 1 ELSE 0 END) as WEB_VISITS,
                COUNT(DISTINCT DEVICE_ID) as UNIQUE_VISITORS
            FROM QUORUMDB.SEGMENT_DATA.V5_ALL_VISITS
            WHERE VISIT_DATE BETWEEN %(start_date)s AND %(end_date)s
            GROUP BY AGENCY_ID
            ORDER BY STORE_VISITS + WEB_VISITS DESC
        """
        
        cursor.execute(query, {'start_date': start_date, 'end_date': end_date})
        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        
        # Add agency names from mapping
        results = []
        for row in rows:
            data = dict(zip(columns, row))
            data['AGENCY_NAME'] = get_agency_name(data['AGENCY_ID'])
            results.append(data)
        
        cursor.close()
        conn.close()
        
        return jsonify({'success': True, 'data': results})
        
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

# =============================================================================
# ADVERTISERS ENDPOINT
# =============================================================================

@app.route('/api/v5/advertisers', methods=['GET'])
def get_advertisers():
    """Get advertisers for an agency with visit counts"""
    try:
        agency_id = request.args.get('agency_id', type=int)
        if not agency_id:
            return jsonify({'success': False, 'error': 'agency_id required'}), 400
        
        conn = get_snowflake_connection()
        cursor = conn.cursor()
        
        start_date, end_date = get_default_dates()
        start_date = parse_date(request.args.get('start_date')) or start_date
        end_date = parse_date(request.args.get('end_date')) or end_date
        
        # Get advertiser names based on agency
        adv_names = {}
        if agency_id == 1480:  # Paramount - use URL mapping
            name_query = """
                SELECT DISTINCT ADVERTISER_ID, 
                    REGEXP_REPLACE(ADVERTISER_NAME, '^[0-9A-Za-z]+ - ', '') as ADVERTISER_NAME
                FROM QUORUMDB.SEGMENT_DATA.PARAMOUNT_URL_MAPPING
                WHERE ADVERTISER_ID IS NOT NULL
            """
            cursor.execute(name_query)
            for row in cursor.fetchall():
                adv_names[row[0]] = row[1]
        else:  # Other agencies - use ADVERTISER_PIXEL_STATS
            name_query = """
                SELECT DISTINCT QUORUM_ADVERTISER_ID, IMP_ADV_NAME
                FROM QUORUMDB.SEGMENT_DATA.ADVERTISER_PIXEL_STATS
                WHERE AGENCY_ID = %(agency_id)s AND IMP_ADV_NAME IS NOT NULL
            """
            cursor.execute(name_query, {'agency_id': agency_id})
            for row in cursor.fetchall():
                if row[0] and row[1]:
                    adv_names[int(row[0])] = row[1]
        
        query = """
            SELECT 
                ADVERTISER_ID,
                SUM(CASE WHEN VISIT_TYPE = 'STORE' THEN 1 ELSE 0 END) as STORE_VISITS,
                SUM(CASE WHEN VISIT_TYPE = 'WEB' THEN 1 ELSE 0 END) as WEB_VISITS,
                COUNT(DISTINCT DEVICE_ID) as UNIQUE_VISITORS
            FROM QUORUMDB.SEGMENT_DATA.V5_ALL_VISITS
            WHERE AGENCY_ID = %(agency_id)s
              AND VISIT_DATE BETWEEN %(start_date)s AND %(end_date)s
            GROUP BY ADVERTISER_ID
            ORDER BY STORE_VISITS + WEB_VISITS DESC
        """
        
        cursor.execute(query, {
            'agency_id': agency_id,
            'start_date': start_date,
            'end_date': end_date
        })
        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        
        # Add advertiser names
        results = []
        for row in rows:
            data = dict(zip(columns, row))
            adv_id = data['ADVERTISER_ID']
            data['ADVERTISER_NAME'] = adv_names.get(adv_id, f"Advertiser {adv_id}")
            results.append(data)
        
        cursor.close()
        conn.close()
        
        return jsonify({'success': True, 'data': results})
        
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

# =============================================================================
# CAMPAIGN PERFORMANCE ENDPOINT
# =============================================================================

@app.route('/api/v5/campaign-performance', methods=['GET'])
def get_campaign_performance():
    """Get campaign (IO) level performance for an advertiser"""
    try:
        agency_id = request.args.get('agency_id', type=int)
        advertiser_id = request.args.get('advertiser_id', type=int)
        
        if not agency_id or not advertiser_id:
            return jsonify({'success': False, 'error': 'agency_id and advertiser_id required'}), 400
        
        conn = get_snowflake_connection()
        cursor = conn.cursor()
        
        start_date, end_date = get_default_dates()
        start_date = parse_date(request.args.get('start_date')) or start_date
        end_date = parse_date(request.args.get('end_date')) or end_date
        
        query = """
            SELECT 
                IO_ID,
                IO_NAME,
                VISIT_TYPE,
                COUNT(*) as VISITS,
                COUNT(DISTINCT DEVICE_ID) as UNIQUE_VISITORS
            FROM QUORUMDB.SEGMENT_DATA.V5_ALL_VISITS
            WHERE AGENCY_ID = %(agency_id)s
              AND ADVERTISER_ID = %(advertiser_id)s
              AND VISIT_DATE BETWEEN %(start_date)s AND %(end_date)s
              AND IO_ID IS NOT NULL
            GROUP BY IO_ID, IO_NAME, VISIT_TYPE
            ORDER BY VISITS DESC
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
# LINEITEM PERFORMANCE ENDPOINT
# =============================================================================

@app.route('/api/v5/lineitem-performance', methods=['GET'])
def get_lineitem_performance():
    """Get line item level performance for an advertiser"""
    try:
        agency_id = request.args.get('agency_id', type=int)
        advertiser_id = request.args.get('advertiser_id', type=int)
        io_id = request.args.get('io_id', type=int)  # Optional filter
        
        if not agency_id or not advertiser_id:
            return jsonify({'success': False, 'error': 'agency_id and advertiser_id required'}), 400
        
        conn = get_snowflake_connection()
        cursor = conn.cursor()
        
        start_date, end_date = get_default_dates()
        start_date = parse_date(request.args.get('start_date')) or start_date
        end_date = parse_date(request.args.get('end_date')) or end_date
        
        io_filter = "AND IO_ID = %(io_id)s" if io_id else ""
        
        query = f"""
            SELECT 
                IO_ID,
                IO_NAME,
                LINEITEM_ID,
                LINEITEM_NAME,
                VISIT_TYPE,
                COUNT(*) as VISITS,
                COUNT(DISTINCT DEVICE_ID) as UNIQUE_VISITORS
            FROM QUORUMDB.SEGMENT_DATA.V5_ALL_VISITS
            WHERE AGENCY_ID = %(agency_id)s
              AND ADVERTISER_ID = %(advertiser_id)s
              AND VISIT_DATE BETWEEN %(start_date)s AND %(end_date)s
              AND LINEITEM_ID IS NOT NULL
              {io_filter}
            GROUP BY IO_ID, IO_NAME, LINEITEM_ID, LINEITEM_NAME, VISIT_TYPE
            ORDER BY VISITS DESC
        """
        
        params = {
            'agency_id': agency_id,
            'advertiser_id': advertiser_id,
            'start_date': start_date,
            'end_date': end_date
        }
        if io_id:
            params['io_id'] = io_id
        
        cursor.execute(query, params)
        columns = [desc[0] for desc in cursor.description]
        results = [dict(zip(columns, row)) for row in cursor.fetchall()]
        
        cursor.close()
        conn.close()
        
        return jsonify({'success': True, 'data': results})
        
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

# =============================================================================
# CREATIVE PERFORMANCE ENDPOINT
# =============================================================================

@app.route('/api/v5/creative-performance', methods=['GET'])
def get_creative_performance():
    """Get creative level performance for an advertiser"""
    try:
        agency_id = request.args.get('agency_id', type=int)
        advertiser_id = request.args.get('advertiser_id', type=int)
        
        if not agency_id or not advertiser_id:
            return jsonify({'success': False, 'error': 'agency_id and advertiser_id required'}), 400
        
        conn = get_snowflake_connection()
        cursor = conn.cursor()
        
        start_date, end_date = get_default_dates()
        start_date = parse_date(request.args.get('start_date')) or start_date
        end_date = parse_date(request.args.get('end_date')) or end_date
        
        query = """
            SELECT 
                CREATIVE_ID,
                MAX(CREATIVE_NAME) as CREATIVE_NAME,
                VISIT_TYPE,
                COUNT(*) as VISITS,
                COUNT(DISTINCT DEVICE_ID) as UNIQUE_VISITORS
            FROM QUORUMDB.SEGMENT_DATA.V5_ALL_VISITS
            WHERE AGENCY_ID = %(agency_id)s
              AND ADVERTISER_ID = %(advertiser_id)s
              AND VISIT_DATE BETWEEN %(start_date)s AND %(end_date)s
              AND CREATIVE_ID IS NOT NULL
            GROUP BY CREATIVE_ID, VISIT_TYPE
            ORDER BY VISITS DESC
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
# GEOGRAPHIC PERFORMANCE ENDPOINT (ZIP)
# =============================================================================

@app.route('/api/v5/zip-performance', methods=['GET'])
def get_zip_performance():
    """Get ZIP code level performance for an advertiser"""
    try:
        agency_id = request.args.get('agency_id', type=int)
        advertiser_id = request.args.get('advertiser_id', type=int)
        
        if not agency_id or not advertiser_id:
            return jsonify({'success': False, 'error': 'agency_id and advertiser_id required'}), 400
        
        conn = get_snowflake_connection()
        cursor = conn.cursor()
        
        start_date, end_date = get_default_dates()
        start_date = parse_date(request.args.get('start_date')) or start_date
        end_date = parse_date(request.args.get('end_date')) or end_date
        
        query = """
            SELECT 
                ZIP_CODE,
                DMA,
                VISIT_TYPE,
                COUNT(*) as VISITS,
                COUNT(DISTINCT DEVICE_ID) as UNIQUE_VISITORS
            FROM QUORUMDB.SEGMENT_DATA.V5_ALL_VISITS
            WHERE AGENCY_ID = %(agency_id)s
              AND ADVERTISER_ID = %(advertiser_id)s
              AND VISIT_DATE BETWEEN %(start_date)s AND %(end_date)s
              AND ZIP_CODE IS NOT NULL
              AND ZIP_CODE != '0'
              AND ZIP_CODE != ''
            GROUP BY ZIP_CODE, DMA, VISIT_TYPE
            ORDER BY VISITS DESC
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
# DMA PERFORMANCE ENDPOINT
# =============================================================================

@app.route('/api/v5/dma-performance', methods=['GET'])
def get_dma_performance():
    """Get DMA level performance for an advertiser"""
    try:
        agency_id = request.args.get('agency_id', type=int)
        advertiser_id = request.args.get('advertiser_id', type=int)
        
        if not agency_id or not advertiser_id:
            return jsonify({'success': False, 'error': 'agency_id and advertiser_id required'}), 400
        
        conn = get_snowflake_connection()
        cursor = conn.cursor()
        
        start_date, end_date = get_default_dates()
        start_date = parse_date(request.args.get('start_date')) or start_date
        end_date = parse_date(request.args.get('end_date')) or end_date
        
        query = """
            SELECT 
                DMA,
                VISIT_TYPE,
                COUNT(*) as VISITS,
                COUNT(DISTINCT DEVICE_ID) as UNIQUE_VISITORS,
                COUNT(DISTINCT ZIP_CODE) as ZIP_COUNT
            FROM QUORUMDB.SEGMENT_DATA.V5_ALL_VISITS
            WHERE AGENCY_ID = %(agency_id)s
              AND ADVERTISER_ID = %(advertiser_id)s
              AND VISIT_DATE BETWEEN %(start_date)s AND %(end_date)s
              AND DMA IS NOT NULL
            GROUP BY DMA, VISIT_TYPE
            ORDER BY VISITS DESC
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
# TIMESERIES ENDPOINT (for charts)
# =============================================================================

@app.route('/api/v5/timeseries', methods=['GET'])
def get_timeseries():
    """Get daily visit counts for charts"""
    try:
        agency_id = request.args.get('agency_id', type=int)
        advertiser_id = request.args.get('advertiser_id', type=int)
        
        conn = get_snowflake_connection()
        cursor = conn.cursor()
        
        start_date, end_date = get_default_dates()
        start_date = parse_date(request.args.get('start_date')) or start_date
        end_date = parse_date(request.args.get('end_date')) or end_date
        
        # Build WHERE clause based on filters
        where_parts = ["VISIT_DATE BETWEEN %(start_date)s AND %(end_date)s"]
        params = {'start_date': start_date, 'end_date': end_date}
        
        if agency_id:
            where_parts.append("AGENCY_ID = %(agency_id)s")
            params['agency_id'] = agency_id
        if advertiser_id:
            where_parts.append("ADVERTISER_ID = %(advertiser_id)s")
            params['advertiser_id'] = advertiser_id
        
        where_clause = " AND ".join(where_parts)
        
        query = f"""
            SELECT 
                VISIT_DATE,
                SUM(CASE WHEN VISIT_TYPE = 'STORE' THEN 1 ELSE 0 END) as STORE_VISITS,
                SUM(CASE WHEN VISIT_TYPE = 'WEB' THEN 1 ELSE 0 END) as WEB_VISITS,
                COUNT(DISTINCT DEVICE_ID) as UNIQUE_VISITORS
            FROM QUORUMDB.SEGMENT_DATA.V5_ALL_VISITS
            WHERE {where_clause}
            GROUP BY VISIT_DATE
            ORDER BY VISIT_DATE
        """
        
        cursor.execute(query, params)
        columns = [desc[0] for desc in cursor.description]
        results = [dict(zip(columns, row)) for row in cursor.fetchall()]
        
        # Convert dates to strings for JSON
        for row in results:
            if row.get('VISIT_DATE'):
                row['VISIT_DATE'] = str(row['VISIT_DATE'])
        
        cursor.close()
        conn.close()
        
        return jsonify({'success': True, 'data': results})
        
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

# =============================================================================
# SUMMARY ENDPOINT (combined metrics)
# =============================================================================

@app.route('/api/v5/summary', methods=['GET'])
def get_summary():
    """Get summary metrics for an advertiser"""
    try:
        agency_id = request.args.get('agency_id', type=int)
        advertiser_id = request.args.get('advertiser_id', type=int)
        
        if not agency_id or not advertiser_id:
            return jsonify({'success': False, 'error': 'agency_id and advertiser_id required'}), 400
        
        conn = get_snowflake_connection()
        cursor = conn.cursor()
        
        start_date, end_date = get_default_dates()
        start_date = parse_date(request.args.get('start_date')) or start_date
        end_date = parse_date(request.args.get('end_date')) or end_date
        
        query = """
            SELECT 
                COUNT(*) as TOTAL_VISITS,
                SUM(CASE WHEN VISIT_TYPE = 'STORE' THEN 1 ELSE 0 END) as STORE_VISITS,
                SUM(CASE WHEN VISIT_TYPE = 'WEB' THEN 1 ELSE 0 END) as WEB_VISITS,
                COUNT(DISTINCT DEVICE_ID) as UNIQUE_VISITORS,
                COUNT(DISTINCT IO_ID) as CAMPAIGN_COUNT,
                COUNT(DISTINCT LINEITEM_ID) as LINEITEM_COUNT,
                MIN(VISIT_DATE) as MIN_DATE,
                MAX(VISIT_DATE) as MAX_DATE
            FROM QUORUMDB.SEGMENT_DATA.V5_ALL_VISITS
            WHERE AGENCY_ID = %(agency_id)s
              AND ADVERTISER_ID = %(advertiser_id)s
              AND VISIT_DATE BETWEEN %(start_date)s AND %(end_date)s
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
        
        # Convert dates to strings
        if result.get('MIN_DATE'):
            result['MIN_DATE'] = str(result['MIN_DATE'])
        if result.get('MAX_DATE'):
            result['MAX_DATE'] = str(result['MAX_DATE'])
        
        cursor.close()
        conn.close()
        
        return jsonify({'success': True, 'data': result})
        
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

# =============================================================================
# HOUSEHOLD ATTRIBUTION ENDPOINT
# =============================================================================

@app.route('/api/v5/household-summary', methods=['GET'])
def get_household_summary():
    """Get household attribution summary for an advertiser"""
    try:
        agency_id = request.args.get('agency_id', type=int)
        advertiser_id = request.args.get('advertiser_id', type=int)
        
        if not agency_id or not advertiser_id:
            return jsonify({'success': False, 'error': 'agency_id and advertiser_id required'}), 400
        
        conn = get_snowflake_connection()
        cursor = conn.cursor()
        
        start_date, end_date = get_default_dates()
        start_date = parse_date(request.args.get('start_date')) or start_date
        end_date = parse_date(request.args.get('end_date')) or end_date
        
        query = """
            SELECT 
                IO_NAME,
                COUNT(*) as STORE_VISITS,
                COUNT(DISTINCT DEVICE_ID) as UNIQUE_DEVICES,
                COUNT(DISTINCT HOUSEHOLD_ID) as UNIQUE_HOUSEHOLDS,
                ROUND(AVG(HOUSEHOLD_CONFIDENCE), 2) as AVG_HH_CONFIDENCE,
                SUM(HAS_HOUSEHOLD) as WITH_HOUSEHOLD_MATCH
            FROM QUORUMDB.SEGMENT_DATA.V5_STORE_VISITS_WITH_HOUSEHOLD
            WHERE AGENCY_ID = %(agency_id)s
              AND ADVERTISER_ID = %(advertiser_id)s
              AND DRIVE_BY_DATE BETWEEN %(start_date)s AND %(end_date)s
            GROUP BY IO_NAME
            ORDER BY STORE_VISITS DESC
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
# ADVERTISER OVERVIEW (combined endpoint for detail page)
# =============================================================================

@app.route('/api/v5/advertiser-overview', methods=['GET'])
def get_advertiser_overview():
    """Get complete overview for an advertiser (summary + top campaigns)"""
    try:
        agency_id = request.args.get('agency_id', type=int)
        advertiser_id = request.args.get('advertiser_id', type=int)
        
        if not agency_id or not advertiser_id:
            return jsonify({'success': False, 'error': 'agency_id and advertiser_id required'}), 400
        
        conn = get_snowflake_connection()
        cursor = conn.cursor()
        
        start_date, end_date = get_default_dates()
        start_date = parse_date(request.args.get('start_date')) or start_date
        end_date = parse_date(request.args.get('end_date')) or end_date
        
        params = {
            'agency_id': agency_id,
            'advertiser_id': advertiser_id,
            'start_date': start_date,
            'end_date': end_date
        }
        
        # Summary query
        summary_query = """
            SELECT 
                COUNT(*) as TOTAL_VISITS,
                SUM(CASE WHEN VISIT_TYPE = 'STORE' THEN 1 ELSE 0 END) as STORE_VISITS,
                SUM(CASE WHEN VISIT_TYPE = 'WEB' THEN 1 ELSE 0 END) as WEB_VISITS,
                COUNT(DISTINCT DEVICE_ID) as UNIQUE_VISITORS,
                COUNT(DISTINCT IO_ID) as CAMPAIGN_COUNT
            FROM QUORUMDB.SEGMENT_DATA.V5_ALL_VISITS
            WHERE AGENCY_ID = %(agency_id)s
              AND ADVERTISER_ID = %(advertiser_id)s
              AND VISIT_DATE BETWEEN %(start_date)s AND %(end_date)s
        """
        
        cursor.execute(summary_query, params)
        summary_columns = [desc[0] for desc in cursor.description]
        summary_row = cursor.fetchone()
        summary = dict(zip(summary_columns, summary_row)) if summary_row else {}
        
        # Top campaigns query
        campaigns_query = """
            SELECT 
                IO_ID,
                IO_NAME,
                SUM(CASE WHEN VISIT_TYPE = 'STORE' THEN 1 ELSE 0 END) as STORE_VISITS,
                SUM(CASE WHEN VISIT_TYPE = 'WEB' THEN 1 ELSE 0 END) as WEB_VISITS,
                COUNT(DISTINCT DEVICE_ID) as UNIQUE_VISITORS
            FROM QUORUMDB.SEGMENT_DATA.V5_ALL_VISITS
            WHERE AGENCY_ID = %(agency_id)s
              AND ADVERTISER_ID = %(advertiser_id)s
              AND VISIT_DATE BETWEEN %(start_date)s AND %(end_date)s
              AND IO_ID IS NOT NULL
            GROUP BY IO_ID, IO_NAME
            ORDER BY STORE_VISITS + WEB_VISITS DESC
            LIMIT 10
        """
        
        cursor.execute(campaigns_query, params)
        campaigns_columns = [desc[0] for desc in cursor.description]
        campaigns = [dict(zip(campaigns_columns, row)) for row in cursor.fetchall()]
        
        cursor.close()
        conn.close()
        
        return jsonify({
            'success': True,
            'data': {
                'summary': summary,
                'top_campaigns': campaigns
            }
        })
        
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

# =============================================================================
# MAIN
# =============================================================================

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=True)
