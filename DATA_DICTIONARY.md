# Quorum Optimizer - Data Dictionary

**Last Updated:** January 18, 2026

---

## Database Overview

- **Account**: FZB05958.us-east-1 (Snowflake)
- **Primary Database**: QUORUMDB
- **Primary Schema**: SEGMENT_DATA
- **Secondary Database**: QUORUM_CROSS_CLOUD (for device mapping)

---

## Primary Attribution Tables

### QUORUM_ADV_STORE_VISITS

**Purpose**: Gold table for store visit attribution. Contains ad impressions linked to physical store visits.

**Row Count**: ~84 million  
**Key Usage**: Store visits module

| Column | Type | Description | Notes |
|--------|------|-------------|-------|
| DEVICE_ID_QU | VARCHAR | Quorum device identifier | Used for last-touch partitioning |
| IMP_MAID | VARCHAR | Mobile Advertising ID | Join to MAID_CENTROID_ASSOCIATION |
| DRIVEBYDATE | DATE | Store visit date | Used for last-touch partitioning |
| IMP_TIMESTAMP | TIMESTAMP | Impression timestamp | ORDER BY DESC for last-touch |
| IS_STORE_VISIT | BOOLEAN | Visit attribution flag | ⚠️ TRUE for ALL preceding impressions, not just last-touch |
| QUORUM_ADVERTISER_ID | NUMBER | Advertiser ID | Join to AGENCY_ADVERTISER |
| AGENCY_ID | NUMBER | Agency ID | |
| PT | VARCHAR | Platform Type ID | See PT Configuration |
| IO_ID | NUMBER | Insertion Order (Campaign) ID | |
| IO_NAME | VARCHAR | Campaign name | |
| LINEITEM_ID | VARCHAR | Line Item ID | Often "0" for MNTN |
| LINEITEM_NAME | VARCHAR | Line Item name | |
| PUBLISHER_ID | NUMBER | Publisher numeric ID | Used by Adelphic, DV360, etc. |
| PUBLISHER_CODE | VARCHAR | Publisher alphanumeric code | Used by MNTN, SpringServe |
| SITE | VARCHAR | Site/domain | Used by Trade Desk, FreeWheel |

**Critical Note**: The `IS_STORE_VISIT=TRUE` flag marks ALL impressions that preceded a store visit for that device, not just the last one. You MUST apply last-touch logic:

```sql
ROW_NUMBER() OVER (
    PARTITION BY DEVICE_ID_QU, DRIVEBYDATE 
    ORDER BY IMP_TIMESTAMP DESC
) as rn
-- Then filter WHERE rn = 1
```

---

### PARAMOUNT_AD_FULL_ATTRIBUTION_V2

**Purpose**: Web event attribution with full campaign/lineitem/publisher data.

**Row Count**: ~320 million  
**Key Usage**: Web events campaign/lineitem/publisher breakdowns

| Column | Type | Description | Notes |
|--------|------|-------------|-------|
| IMP_MAID | VARCHAR | Mobile Advertising ID | Used for last-touch partitioning |
| SITE_VISIT_ID | VARCHAR | Unique site visit identifier | Used for last-touch partitioning |
| IMP_DATE | DATE | Impression date | ORDER BY DESC for last-touch |
| SITE_VISIT_TIMESTAMP | TIMESTAMP | Web visit timestamp | |
| IS_SITE_VISIT | VARCHAR | Site visit flag ('TRUE'/'FALSE') | String, not boolean! |
| IS_LEAD | VARCHAR | Lead flag | |
| IS_PURCHASE | VARCHAR | Purchase flag | |
| PURCHASE_VALUE | VARCHAR | Purchase amount | |
| QUORUM_ADVERTISER_ID | VARCHAR | Advertiser ID | String type |
| ADVERTISER_NAME | VARCHAR | Advertiser name | |
| PT | VARCHAR | Platform Type ID | Typically "21" (FreeWheel) |
| IO_ID | NUMBER | Campaign ID | |
| IO_NAME | VARCHAR | Campaign name | |
| LINEITEM_ID | VARCHAR | Line Item ID | |
| LINEITEM_NAME | VARCHAR | Line Item name | |
| SITE | VARCHAR | Publisher/site name | Primary publisher column |
| CREATIVE_ID | NUMBER | Creative ID | |
| CREATIVE_NAME | VARCHAR | Creative name | |
| ZIP_CODE | VARCHAR | ZIP from impression | |

**Note**: This table primarily contains FreeWheel (PT=21) data. Same last-touch logic applies:

```sql
ROW_NUMBER() OVER (
    PARTITION BY IMP_MAID, SITE_VISIT_ID 
    ORDER BY IMP_DATE DESC
) as rn
```

---

### WEB_VISITORS_TO_LOG

**Purpose**: Basic web event data without campaign/publisher details.

**Key Usage**: Web summary and ZIP-level metrics

| Column | Type | Description |
|--------|------|-------------|
| WEB_IMPRESSION_ID | VARCHAR | Links to WEBPIXEL_IMPRESSION_LOG.UUID |
| QUORUM_ADVERTISER_ID | VARCHAR | Advertiser ID |
| MAID | VARCHAR | Mobile Advertising ID |
| SITE_VISIT_TIMESTAMP | TIMESTAMP | Visit timestamp |
| IS_SITE_VISIT | VARCHAR | 'TRUE'/'FALSE' |
| LEAD_TIMESTAMP | TIMESTAMP | Lead event time |
| IS_LEAD | VARCHAR | 'TRUE'/'FALSE' |
| PURCHASE_TIMESTAMP | TIMESTAMP | Purchase event time |
| IS_PURCHASE | VARCHAR | 'TRUE'/'FALSE' |
| PURCHASE_VALUE | VARCHAR | Purchase amount |

---

## Reference Tables

### AGENCY_ADVERTISER

**Purpose**: Advertiser and agency metadata.

| Column | Type | Description |
|--------|------|-------------|
| ID | NUMBER | Advertiser ID (PK) |
| COMP_NAME | VARCHAR | Company/advertiser name |
| AGENCY_NAME | VARCHAR | Agency name |
| IS_POI | BOOLEAN | Has physical locations |
| STATUS | NUMBER | Active status |

---

### MAID_CENTROID_ASSOCIATION

**Database**: QUORUM_CROSS_CLOUD.ATTAIN_FEED  
**Purpose**: Device to ZIP code mapping.

**Row Count**: ~497 million

| Column | Type | Description |
|--------|------|-------------|
| DEVICE_ID | VARCHAR | Mobile Advertising ID |
| CENSUS_BLOCK_ID | VARCHAR | Census block identifier |
| ZIP_CODE | VARCHAR | ZIP code |
| ZIP_POPULATION | NUMBER | ZIP population |

**Critical**: Join on `DEVICE_ID`, NOT `CENSUS_BLOCK_ID`. Census block join causes ~72x row multiplication because multiple devices share the same census block.

```sql
-- CORRECT
JOIN MAID_CENTROID_ASSOCIATION mca 
    ON LOWER(sv.IMP_MAID) = LOWER(mca.DEVICE_ID)

-- WRONG - causes row explosion
JOIN MAID_CENTROID_ASSOCIATION mca 
    ON sv.CENSUS_BLOCK_ID = mca.CENSUS_BLOCK_ID
```

---

### ZIP_DMA_MAPPING

**Purpose**: ZIP to DMA (Designated Market Area) lookup.

| Column | Type | Description |
|--------|------|-------------|
| ZIP_CODE | VARCHAR | ZIP code (PK) |
| DMA_CODE | VARCHAR | DMA numeric code |
| DMA_NAME | VARCHAR | DMA name (e.g., "DENVER") |

---

### ZIP_POPULATION_DATA

**Purpose**: ZIP code population data.

| Column | Type | Description |
|--------|------|-------------|
| ZIP_CODE | VARCHAR | ZIP code (PK) |
| POPULATION | NUMBER | Population count |

---

### PUBLISHERS_ID_NAME_MAPPING

**Purpose**: Publisher ID to name mapping for Trade Desk.

| Column | Type | Description |
|--------|------|-------------|
| ID | VARCHAR | Publisher code |
| PUBLISHER_NAME | VARCHAR | Human-readable name |
| CAMPAIGN_TYPE | VARCHAR | Campaign type (e.g., "TTD") |

**Note**: Only useful for Trade Desk (PT=6). Join: `PUBLISHER_CODE = ID`

---

## Impression Log Tables

### XANDR_IMPRESSION_LOG

**Purpose**: Raw ad impressions from multiple platforms.

**Key Columns**:
- PT - Platform Type ID
- QUORUM_ADVERTISER_ID - Advertiser
- AGENCY_ID - Agency
- IO_ID, IO_NAME - Campaign
- LINEITEM_ID, LINEITEM_NAME - Line Item
- PUBLISHER_ID, PUBLISHER_CODE, SITE - Publisher info
- CLIENT_IP, USER_IP - For attribution
- DEVICE_UNIQUE_ID - Device ID (ignore "SYS-" prefix = synthetic)
- TIMESTAMP, SYS_TIMESTAMP - Timing
- POSTAL_CODE, DMA - Geography

**Note**: This is the raw impression log. For attribution, use the gold tables (QUORUM_ADV_STORE_VISITS, PARAMOUNT_AD_FULL_ATTRIBUTION_V2) which have pre-calculated attribution flags.

---

### WEBPIXEL_IMPRESSION_LOG

**Purpose**: Website pixel tracking events.

| Column | Type | Description |
|--------|------|-------------|
| ID | NUMBER | Row ID |
| UUID | VARCHAR | Unique event ID |
| TIMESTAMP | TIMESTAMP | Event time |
| AG_ID | NUMBER | Agency ID |
| REFERER | VARCHAR | Website URL visited |
| CLIENT_IP | VARCHAR | Client IP address |

---

## Platform Type (PT) Reference

| PT | Platform | Primary Publisher Column |
|----|----------|--------------------------|
| 6 | Trade Desk | SITE |
| 8 | DV 360 | PUBLISHER_ID |
| 9 | DCM/GAM | PUBLISHER_ID |
| 11 | Xandr | PUBLISHER_ID |
| 13 | Adelphic | PUBLISHER_ID |
| 20 | SpringServe | PUBLISHER_CODE |
| 21 | FreeWheel | SITE |
| 22 | MNTN | PUBLISHER_CODE (URL encoded) |
| 23 | Yahoo | PUBLISHER_ID |

---

## Common Query Patterns

### Last-Touch Store Visits by ZIP

```sql
WITH last_touch AS (
    SELECT sv.*,
        ROW_NUMBER() OVER (
            PARTITION BY sv.DEVICE_ID_QU, sv.DRIVEBYDATE 
            ORDER BY sv.IMP_TIMESTAMP DESC
        ) as rn
    FROM QUORUMDB.SEGMENT_DATA.QUORUM_ADV_STORE_VISITS sv
    WHERE sv.QUORUM_ADVERTISER_ID = ?
      AND sv.IS_STORE_VISIT = TRUE
),
visits_by_zip AS (
    SELECT 
        mca.ZIP_CODE,
        COUNT(*) as S_VISITS
    FROM last_touch lt
    JOIN QUORUM_CROSS_CLOUD.ATTAIN_FEED.MAID_CENTROID_ASSOCIATION mca 
        ON LOWER(lt.IMP_MAID) = LOWER(mca.DEVICE_ID)
    WHERE lt.rn = 1
    GROUP BY mca.ZIP_CODE
)
SELECT * FROM visits_by_zip;
```

### Last-Touch Web Events by Campaign

```sql
WITH last_touch AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY IMP_MAID, SITE_VISIT_ID 
            ORDER BY IMP_DATE DESC
        ) as rn
    FROM QUORUMDB.SEGMENT_DATA.PARAMOUNT_AD_FULL_ATTRIBUTION_V2
    WHERE QUORUM_ADVERTISER_ID = ?
      AND IS_SITE_VISIT = 'TRUE'
)
SELECT 
    IO_ID as CAMPAIGN_ID,
    MAX(IO_NAME) as CAMPAIGN_NAME,
    COUNT(*) as W_VISITS
FROM last_touch
WHERE rn = 1
GROUP BY IO_ID;
```

---

## Data Quality Notes

1. **MNTN Line Items**: Often "0" - platform doesn't pass this data
2. **IS_SITE_VISIT is STRING**: Compare to 'TRUE' not TRUE
3. **QUORUM_ADVERTISER_ID type varies**: NUMBER in some tables, VARCHAR in others
4. **URL encoding**: MNTN (PT=22) values need double URL decoding
5. **Case sensitivity**: MAID comparisons should use LOWER() for safety

---

## Table Row Counts (Approximate)

| Table | Rows |
|-------|------|
| QUORUM_ADV_STORE_VISITS | 84M |
| PARAMOUNT_AD_FULL_ATTRIBUTION_V2 | 320M |
| XANDR_IMPRESSION_LOG | Billions |
| MAID_CENTROID_ASSOCIATION | 497M |
| AGENCY_ADVERTISER | ~50K |

