"""
TRAFFIC SOURCE PERFORMANCE ENDPOINT - FINAL VERSION
Add this to app.py

Features:
- Page views per visit (pixel fires per user per day)
- CTV View Through as separate row (all visits from CTV-exposed users)
- CTV overlap % for click-based sources (28-day lookback)
- CTV timing: Before/Same Day/After
- NO visit_rate column (removed per request)
- Filters out Internal traffic
"""

@app.route('/api/web/traffic-source-performance', methods=['GET'])
def get_web_traffic_source_performance():
    """
    Get traffic source performance with CTV overlap analysis.
    
    Only works for advertisers with Quorum web pixel installed.
    Shows how click-based sources (Google, Meta, etc.) overlap with CTV exposure.
    
    Query Parameters:
    - advertiser_id (required): Advertiser ID in WEB_VISITORS_TO_LOG
    - xandr_advertiser_id (required): Advertiser ID in XANDR_IMPRESSION_LOG (for CTV data)
    - agency_id (required): Agency ID
    - start_date (optional): Start date (default: 2020-01-01)
    - end_date (optional): End date (default: 2030-12-31)
    - min_visits (optional): Minimum visits to include (default: 10)
    
    Returns:
    {
      "success": true,
      "data": [
        {
          "source": "Google Ads",
          "impressions": 26116,
          "visits": 13860,
          "leads": 19,
          "purchases": 1,
          "avg_page_views_per_visit": 4.49,
          "ctv_overlap_pct": 0.014,
          "ctv_before": 2,
          "ctv_same_day": 0,
          "ctv_after": 0
        },
        {
          "source": "CTV View Through",
          "impressions": 3031860,
          "visits": 3757,
          "leads": 45,
          "purchases": 2,
          "avg_page_views_per_visit": 3.21,
          "ctv_overlap_pct": 100.0,
          "ctv_before": 3757,
          "ctv_same_day": 0,
          "ctv_after": 0
        }
      ]
    }
    """
    advertiser_id = request.args.get('advertiser_id')
    xandr_advertiser_id = request.args.get('xandr_advertiser_id')
    agency_id = request.args.get('agency_id')
    start_date = request.args.get('start_date', '2020-01-01')
    end_date = request.args.get('end_date', '2030-12-31')
    min_visits = request.args.get('min_visits', '10')
    
    if not advertiser_id or not agency_id or not xandr_advertiser_id:
        return jsonify({
            'success': False,
            'error': 'advertiser_id, xandr_advertiser_id, and agency_id parameters required'
        }), 400
    
    try:
        query = """
            WITH 
            -- Get CTV impressions
            ctv_impressions AS (
              SELECT 
                UPPER(REPLACE(DEVICE_UNIQUE_ID, '-', '')) as maid,
                TIMESTAMP as ctv_timestamp
              FROM QUORUMDB.SEGMENT_DATA.XANDR_IMPRESSION_LOG
              WHERE AGENCY_ID = %s
                AND ADVERTISER_ID = %s
                AND DEVICE_UNIQUE_ID IS NOT NULL
                AND DEVICE_UNIQUE_ID NOT LIKE 'SYS-%%'
                AND TIMESTAMP >= %s
                AND TIMESTAMP < %s
            ),
            
            -- Get pixel impressions with traffic source
            pixel_impressions AS (
              SELECT 
                UUID,
                CLIENT_IP,
                REFERER,
                CAST(TIMESTAMP AS DATE) as visit_date,
                TIMESTAMP as pixel_timestamp,
                CASE 
                  WHEN REFERER ILIKE '%%doubleclick%%' OR REFERER ILIKE '%%syndicatedsearch.goog%%' 
                    THEN 'Google Ads'
                  WHEN REFERER ILIKE '%%fbapp%%' OR REFERER ILIKE '%%facebook%%' 
                    THEN 'Meta/Facebook'
                  WHEN REFERER ILIKE '%%_ef_transaction_id%%' 
                    THEN 'Affiliate Network'
                  WHEN REFERER ILIKE '%%localhost%%' OR REFERER ILIKE '%%127.0.0.1%%' 
                    THEN 'Internal'
                  WHEN REFERER IS NULL OR REFERER = '-' 
                    THEN 'Direct'
                  ELSE 'Other'
                END as traffic_source
              FROM QUORUMDB.SEGMENT_DATA.WEBPIXEL_IMPRESSION_LOG
              WHERE AG_ID = %s
                AND TIMESTAMP >= %s
                AND TIMESTAMP < %s
            ),
            
            -- Get web conversions
            conversions AS (
              SELECT 
                UPPER(REPLACE(MAID, '-', '')) as maid,
                WEB_IMPRESSION_ID,
                CAST(SITE_VISIT_TIMESTAMP AS DATE) as visit_date,
                SITE_VISIT_TIMESTAMP as conv_timestamp,
                IS_LEAD,
                IS_PURCHASE
              FROM QUORUMDB.SEGMENT_DATA.WEB_VISITORS_TO_LOG
              WHERE QUORUM_ADVERTISER_ID = %s
                AND SITE_VISIT_TIMESTAMP >= %s
                AND SITE_VISIT_TIMESTAMP < %s
            ),
            
            -- Join to get traffic source
            conversions_with_source AS (
              SELECT 
                c.*,
                p.traffic_source,
                p.CLIENT_IP
              FROM conversions c
              LEFT JOIN pixel_impressions p ON c.WEB_IMPRESSION_ID = p.UUID
            ),
            
            -- Count page views per user per day
            page_views_per_user_day AS (
              SELECT 
                p.CLIENT_IP,
                p.visit_date,
                p.traffic_source,
                COUNT(*) as page_views
              FROM pixel_impressions p
              WHERE traffic_source NOT IN ('Other', 'Internal')
              GROUP BY p.CLIENT_IP, p.visit_date, p.traffic_source
            ),
            
            -- Join conversions to page views
            conversions_with_pageviews AS (
              SELECT 
                c.*,
                COALESCE(pv.page_views, 0) as page_views
              FROM conversions_with_source c
              LEFT JOIN page_views_per_user_day pv 
                ON c.CLIENT_IP = pv.CLIENT_IP 
                AND c.visit_date = pv.visit_date
                AND c.traffic_source = pv.traffic_source
            ),
            
            -- Check CTV overlap with 28-day lookback
            conversions_with_ctv AS (
              SELECT 
                c.*,
                CASE WHEN ctv.maid IS NOT NULL THEN 1 ELSE 0 END as had_ctv,
                CASE 
                  WHEN MAX(CASE WHEN ctv.ctv_timestamp < c.conv_timestamp 
                               AND ctv.ctv_timestamp >= DATEADD(day, -28, c.conv_timestamp)
                          THEN 1 ELSE 0 END) = 1 THEN 1 ELSE 0 
                END as had_ctv_before,
                CASE 
                  WHEN MAX(CASE WHEN CAST(ctv.ctv_timestamp AS DATE) = c.visit_date
                          THEN 1 ELSE 0 END) = 1 THEN 1 ELSE 0 
                END as had_ctv_same_day,
                CASE 
                  WHEN MAX(CASE WHEN ctv.ctv_timestamp > c.conv_timestamp
                               AND ctv.ctv_timestamp <= DATEADD(day, 28, c.conv_timestamp)
                          THEN 1 ELSE 0 END) = 1 THEN 1 ELSE 0 
                END as had_ctv_after
              FROM conversions_with_pageviews c
              LEFT JOIN ctv_impressions ctv ON c.maid = ctv.maid
              GROUP BY c.maid, c.WEB_IMPRESSION_ID, c.visit_date, c.conv_timestamp, 
                       c.IS_LEAD, c.IS_PURCHASE, c.traffic_source, c.CLIENT_IP, c.page_views
            ),
            
            -- Aggregate click-based sources
            click_sources AS (
              SELECT 
                traffic_source as source,
                COUNT(*) as visits,
                SUM(CASE WHEN IS_LEAD = 'TRUE' THEN 1 ELSE 0 END) as leads,
                SUM(CASE WHEN IS_PURCHASE = 'TRUE' THEN 1 ELSE 0 END) as purchases,
                AVG(page_views) as avg_page_views_per_visit,
                (SUM(had_ctv_before)::FLOAT / COUNT(*)) * 100 as ctv_overlap_pct,
                SUM(had_ctv_before) as ctv_before,
                SUM(had_ctv_same_day) as ctv_same_day,
                SUM(had_ctv_after) as ctv_after
              FROM conversions_with_ctv
              WHERE traffic_source NOT IN ('Other', 'Internal')
              GROUP BY traffic_source
              HAVING COUNT(*) >= %s
            ),
            
            -- CTV View Through = ALL visits from CTV-exposed users
            ctv_view_through AS (
              SELECT 
                COUNT(*) as visits,
                SUM(CASE WHEN IS_LEAD = 'TRUE' THEN 1 ELSE 0 END) as leads,
                SUM(CASE WHEN IS_PURCHASE = 'TRUE' THEN 1 ELSE 0 END) as purchases,
                AVG(page_views) as avg_page_views_per_visit,
                COUNT(*) as ctv_before
              FROM conversions_with_ctv
              WHERE had_ctv = 1
            )
            
            -- Combine results
            SELECT 
              source,
              0 as impressions,
              visits,
              leads,
              purchases,
              avg_page_views_per_visit,
              ctv_overlap_pct,
              ctv_before,
              ctv_same_day,
              ctv_after
            FROM click_sources
            
            UNION ALL
            
            SELECT 
              'CTV View Through' as source,
              (SELECT COUNT(*) FROM ctv_impressions) as impressions,
              (SELECT visits FROM ctv_view_through) as visits,
              (SELECT leads FROM ctv_view_through) as leads,
              (SELECT purchases FROM ctv_view_through) as purchases,
              (SELECT avg_page_views_per_visit FROM ctv_view_through) as avg_page_views_per_visit,
              100.0 as ctv_overlap_pct,
              (SELECT ctv_before FROM ctv_view_through) as ctv_before,
              0 as ctv_same_day,
              0 as ctv_after
            
            ORDER BY visits DESC
        """
        
        params = (
            agency_id, xandr_advertiser_id, start_date, end_date,  # CTV impressions
            agency_id, start_date, end_date,  # Pixel impressions
            advertiser_id, start_date, end_date,  # Conversions
            min_visits  # Filter
        )
        
        results = execute_query(query, params)
        
        return jsonify({'success': True, 'data': results})
        
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500
