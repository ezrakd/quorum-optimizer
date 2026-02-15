-- =============================================================================
-- QUORUM OPTIMIZER - EXAMPLE QUERIES
-- =============================================================================
-- These queries are used to populate the optimizer with real data from Snowflake
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 1. GET ADVERTISER SUMMARY (Impressions, Visits, Rate)
-- -----------------------------------------------------------------------------
SELECT 
    ADVERTISER_ID,
    ADVERTISER_NAME,
    SUM(IMPRESSIONS) as total_impressions,
    SUM(STORE_VISITS) as total_visits,
    ROUND(SUM(STORE_VISITS) / NULLIF(SUM(IMPRESSIONS), 0) * 100, 3) as visit_rate_pct
FROM QUORUMDB.SEGMENT_DATA.CAMPAIGN_POSTAL_REPORTING
WHERE AGENCY_ID = 2514  -- MNTN
  AND IMPRESSIONS > 0
GROUP BY ADVERTISER_ID, ADVERTISER_NAME
ORDER BY total_visits DESC
LIMIT 20;


-- -----------------------------------------------------------------------------
-- 2. GET PUBLISHER DATA FOR MNTN/MAGNITE (PT=22, PUBLISHER_CODE)
-- -----------------------------------------------------------------------------
SELECT 
    x.PUBLISHER_CODE,
    COUNT(DISTINCT x.ID) as attributed_impressions,
    COUNT(*) as visits
FROM QUORUMDB.SEGMENT_DATA.CAMPAIGN_PERFORMANCE_STORE_VISITS_RAW sv
JOIN QUORUMDB.SEGMENT_DATA.XANDR_IMPRESSION_LOG x ON sv.IMP_ID = x.ID
WHERE sv.AGENCY_ID = 2514
  AND sv.ADVERTISER_ID = 45143  -- Noodles & Company
  AND x.PUBLISHER_CODE IS NOT NULL
  AND x.PUBLISHER_CODE != ''
  AND x.PUBLISHER_CODE != '0'
GROUP BY x.PUBLISHER_CODE
ORDER BY visits DESC
LIMIT 15;


-- -----------------------------------------------------------------------------
-- 3. GET CONTEXT DATA FOR CAUSAL IQ TTD (PT=6, SITE field)
-- -----------------------------------------------------------------------------
SELECT 
    x.SITE as context,
    COUNT(DISTINCT x.ID) as attributed_impressions,
    COUNT(*) as visits
FROM QUORUMDB.SEGMENT_DATA.CAMPAIGN_PERFORMANCE_STORE_VISITS_RAW sv
JOIN QUORUMDB.SEGMENT_DATA.XANDR_IMPRESSION_LOG x ON sv.IMP_ID = x.ID
WHERE sv.AGENCY_ID = 1813
  AND sv.ADVERTISER_ID = 45141  -- Lowes TTD
  AND x.PT = '6'  -- Trade Desk only
  AND x.SITE IS NOT NULL
  AND x.SITE != ''
  AND x.SITE != '0'
GROUP BY x.SITE
ORDER BY visits DESC
LIMIT 15;


-- -----------------------------------------------------------------------------
-- 4. GET ZIP CODE DATA WITH DMA
-- -----------------------------------------------------------------------------
SELECT 
    c.USER_HOME_POSTAL_CODE as zip,
    COALESCE(z.DMA_NAME, 'UNKNOWN') as dma,
    SUM(c.IMPRESSIONS) as impressions,
    SUM(c.STORE_VISITS) as visits
FROM QUORUMDB.SEGMENT_DATA.CAMPAIGN_POSTAL_REPORTING c
LEFT JOIN QUORUMDB.SEGMENT_DATA.ZIP_DMA_MAPPING z 
    ON c.USER_HOME_POSTAL_CODE = z.ZIP_CODE
WHERE c.AGENCY_ID = 2514
  AND c.ADVERTISER_ID = 45143
  AND c.USER_HOME_POSTAL_CODE != 'UNKNOWN'
  AND c.STORE_VISITS > 0
GROUP BY c.USER_HOME_POSTAL_CODE, z.DMA_NAME
ORDER BY impressions DESC
LIMIT 20;


-- -----------------------------------------------------------------------------
-- 5. GET CAMPAIGN PERFORMANCE
-- -----------------------------------------------------------------------------
SELECT 
    CAMPAIGN_ID,
    CAMPAIGN_NAME,
    SUM(IMPRESSIONS) as impressions,
    SUM(STORE_VISITS) as visits,
    ROUND(SUM(STORE_VISITS) / NULLIF(SUM(IMPRESSIONS), 0) * 100, 4) as visit_rate_pct
FROM QUORUMDB.SEGMENT_DATA.CAMPAIGN_POSTAL_REPORTING
WHERE AGENCY_ID = 2514
  AND ADVERTISER_ID = 45143
GROUP BY CAMPAIGN_ID, CAMPAIGN_NAME
ORDER BY impressions DESC
LIMIT 15;


-- -----------------------------------------------------------------------------
-- 6. CHECK PLATFORM TYPE (PT) FOR AN ADVERTISER
-- -----------------------------------------------------------------------------
SELECT 
    x.PT,
    COUNT(*) as impression_count,
    COUNT(DISTINCT sv.ID) as attributed_visits
FROM QUORUMDB.SEGMENT_DATA.CAMPAIGN_PERFORMANCE_STORE_VISITS_RAW sv
JOIN QUORUMDB.SEGMENT_DATA.XANDR_IMPRESSION_LOG x ON sv.IMP_ID = x.ID
WHERE sv.AGENCY_ID = 1813
  AND sv.ADVERTISER_ID = 7389  -- Space Coast
GROUP BY x.PT
ORDER BY impression_count DESC;


-- -----------------------------------------------------------------------------
-- 7. GET DMA ZIP COUNTS (for guardrails)
-- -----------------------------------------------------------------------------
SELECT 
    z.DMA_NAME,
    COUNT(DISTINCT c.USER_HOME_POSTAL_CODE) as zip_count
FROM QUORUMDB.SEGMENT_DATA.CAMPAIGN_POSTAL_REPORTING c
JOIN QUORUMDB.SEGMENT_DATA.ZIP_DMA_MAPPING z 
    ON c.USER_HOME_POSTAL_CODE = z.ZIP_CODE
WHERE c.AGENCY_ID = 2514
  AND c.ADVERTISER_ID = 45143
  AND c.IMPRESSIONS >= 3500
GROUP BY z.DMA_NAME
ORDER BY zip_count DESC;


-- -----------------------------------------------------------------------------
-- 8. LIST ALL AGENCIES WITH STORE VISIT DATA
-- -----------------------------------------------------------------------------
SELECT 
    AGENCY_ID,
    AGENCY_NAME,
    COUNT(DISTINCT ADVERTISER_ID) as advertiser_count,
    SUM(IMPRESSIONS) as total_impressions,
    SUM(STORE_VISITS) as total_visits
FROM QUORUMDB.SEGMENT_DATA.CAMPAIGN_POSTAL_REPORTING
WHERE STORE_VISITS > 0
GROUP BY AGENCY_ID, AGENCY_NAME
ORDER BY total_visits DESC;


-- -----------------------------------------------------------------------------
-- PT (Platform Type) Reference
-- -----------------------------------------------------------------------------
-- PT=6  : Trade Desk (TTD) - use SITE field for context
-- PT=8  : DV 360 - no context data available
-- PT=11 : Xandr - use SITE field for context
-- PT=22 : MNTN - use PUBLISHER_CODE for publishers
