# Quorum Data Dictionary

## Overview
This document contains institutional knowledge about Quorum's Snowflake data platform to help Claude (and team members) understand the data structure, column mappings, and nuances.

---

## Snowflake Connection Details
- **Account:** GBGSJYL-FIB09623 (legacy locator: FZB05958)
- **Region:** us-east-1
- **MCP Server:** QUORUMDB.SEGMENT_DATA.CLAUDESERVER
- **MCP Server URL:** `https://GBGSJYL-FIB09623.snowflakecomputing.com/api/v2/databases/QUORUMDB/schemas/SEGMENT_DATA/mcp-servers/CLAUDESERVER`

---

## Primary Databases

### QUORUMDB (Primary Production)
- **Created:** 2023-12-14
- **Schemas:** 19
- **Key Schema:** SEGMENT_DATA (436,770+ tables)

### QUORUM_CROSS_CLOUD
- **Purpose:** GCP replication & identifier crosswalks
- **Created:** 2025-11-13
- **Use Case:** Crosswalking between tables, identity resolution

---

## Key Schemas in QUORUMDB

### SEGMENT_DATA (Core Production)
Primary schema containing billions of rows of location intelligence data.

| Table | Description | Row Scale |
|-------|-------------|-----------|
| SEGMENT_DEVICES_CAPTURED | Core fact table - device captures in segments | Billions |
| segment_poi_overlap | Attribution overlaps | Multi-billion |
| VERASET_RAW_DATA | Vendor raw location data (Vendor ID: 18) | 3.26T rows, 142TB |
| UNACAST_RAW_DATA | Vendor raw location data (Vendor ID: 16) | 89-day retention |
| XANDR_IMPRESSION_LOG | Ad impressions (multi-platform) | Updated hourly |
| WEBPIXEL_IMPRESSION_LOG | Website pixel tracking | Updated hourly |
| IP_MAID_MAPPING | IP to Mobile Ad ID crosswalk | Deterministic enrichment |
| segment_md5_mapping | Hashed segment identifiers | - |
| poi_md5_mapping | Hashed POI identifiers | - |
| segment_inventory | Polygons + H3 cells | - |
| CAMPAIGN_PERFORMANCE_STORE_VISITS_RAW | Attribution output | - |

---

## Core Concept: Segments

**Segments** are physical locations that Quorum has geofenced to track MAID (Mobile Advertising ID) activity.

| Identifier | Description |
|------------|-------------|
| Segment ID | Application database primary key |
| Segment MD5 | Hashed version used in segment processing |

---

## Key Tables for Brand Data

### SEGMENT_DETAIL
- **Purpose:** Metadata for all segments, including BRAND definitions
- **Key Columns:**
  - `SEGMENT_UNIQUE_ID` - e.g., "S-10005595"
  - `SEGMENT_ID` - numeric ID
  - `BRAND` - e.g., "Taco Bell", "Lowes", "Ace Hardware"
  - `CATEGORY` - e.g., "Restaurants - QSR", "Retail - Home Improvement Big Box"
  - `AGENCY_ID` - **IMPORTANT:** Use 1949 for Brand Library queries
  - `ADVERTISER_ID` - links to advertiser/campaign
  - `DMA_NAME`, `ZIP_CODE`, `REGION` - geography
  - `SEGMENT_TYPE` - e.g., 5 = Geo Fence
  - `STATUS` - 1 = active
  - `PROCESSING_ALGO` - e.g., "geobased&viewshed"

### SEGMENT_DAILY_VISITOR_BY_MD5
- **Purpose:** Devices with at least one visit to a location (by MD5) on a given date
- **Columns:** SEGMENT_MD5, DRIVE_BY_DATE, UNIQUE_VISITORS

### SEGMENT_MD5_MAPPING
- **Purpose:** Crosswalk between Segment IDs and MD5 hashes
- **Columns:** SEGMENT_MD5, SEGMENT_UNIQUE_ID

### SEGMENT_POI
- **Purpose:** Associations between locations and advertiser campaigns

---

## Notes & Tribal Knowledge

### âš ï¸ CRITICAL: Agency ID Filtering
**Always filter to `AGENCY_ID = 1949` for Brand Library queries.**
- Agency 1949 = Brand Library (clean brand/location data)
- Other agencies = segments deployed for advertiser attribution

### âš ï¸ CRITICAL: Segment Type Filtering
**For physical location analysis, filter to:**
- `SEGMENT_TYPE = 5` (Geo Fence)
- `STATUS = 1` (Active)

### MD5 is the key
Most processing tables use SEGMENT_MD5, not Segment ID. Always join through SEGMENT_MD5_MAPPING.

### Brand lookup path
```
SEGMENT_DETAIL (brand name, AGENCY_ID=1949, STATUS=1, SEGMENT_TYPE=5)
    â†’ SEGMENT_MD5_MAPPING (get MD5)
    â†’ visitor/device tables
```

---

## Example Queries

### Count visitors at Taco Bell on a specific date
```sql
SELECT 
    sd.BRAND,
    sdv.DRIVE_BY_DATE,
    COUNT(DISTINCT sdv.SEGMENT_MD5) as location_count,
    SUM(sdv.UNIQUE_VISITORS) as total_visitors
FROM QUORUMDB.SEGMENT_DATA.SEGMENT_DAILY_VISITOR_BY_MD5 sdv
JOIN QUORUMDB.SEGMENT_DATA.SEGMENT_MD5_MAPPING smm 
    ON sdv.SEGMENT_MD5 = smm.SEGMENT_MD5
JOIN QUORUMDB.SEGMENT_DATA.SEGMENT_DETAIL sd 
    ON smm.SEGMENT_UNIQUE_ID = sd.SEGMENT_UNIQUE_ID
WHERE sd.BRAND = 'Taco Bell'
  AND sd.AGENCY_ID = 1949
  AND sd.SEGMENT_TYPE = 5
  AND sd.STATUS = 1
  AND sdv.DRIVE_BY_DATE = '2024-01-01'
GROUP BY sd.BRAND, sdv.DRIVE_BY_DATE;
```

### List all Taco Bell locations
```sql
SELECT SEGMENT_UNIQUE_ID, SEGMENT_NAME, DMA_NAME, ZIP_CODE
FROM QUORUMDB.SEGMENT_DATA.SEGMENT_DETAIL
WHERE BRAND = 'Taco Bell'
  AND AGENCY_ID = 1949
  AND SEGMENT_TYPE = 5
  AND STATUS = 1;
```

---

## Platform Reference (PT_TO_PLATFORM)

**Note:** "XANDR_IMPRESSION_LOG" contains impressions from multiple platforms, identified by PT code.

| PT | Platform |
|----|----------|
| 6 | Trade Desk |
| 8 | DV 360 |
| 9 | DCM/GAM |
| 11 | Xandr |
| 12 | Simpli fi |
| 13 | Adelphic |
| 14 | Beeswax |
| 15 | Xtreme Reach |
| 16 | Stack Adapt |
| 20 | Spring Serve |
| 21 | Free Wheel |
| 22 | MNTN |
| 23 | Yahoo |
| 25 | Amazon DSP |

### Trade Desk Publisher Lookup (PT = 6 only)
- **Table:** TTD_PUBLISHER_NAMES
- **Columns:** ID, PUBLISHER_NAME, CAMPAIGN_TYPE
- **âš ï¸ Join:** `PUBLISHER_CODE = ID` (not PUBLISHER_ID!)
- **Only applies to PT=6**

```sql
SELECT x.*, pub.PUBLISHER_NAME
FROM QUORUMDB.SEGMENT_DATA.XANDR_IMPRESSION_LOG x
LEFT JOIN QUORUMDB.SEGMENT_DATA.TTD_PUBLISHER_NAMES pub 
    ON x.PUBLISHER_CODE = pub.ID
WHERE x.PT = 6;
```

---

## Agency & Advertiser Reference

### AGENCY_ADVERTISER
- **Purpose:** Maps agency/advertiser IDs to names
- **Key Columns:** ID, AGENCY_ID, AGENCY_NAME, COMP_NAME, PIXEL_TAG, PIXEL_ID, STATUS

| AG_ID | Agency Name | Notes |
|-------|-------------|-------|
| 1949 | Brand Library | **Use for brand queries** |
| 1202 | (Automotive) | Web pixel - dealer sites |
| 1088 | BroadSum Advertising | |
| 1127 | Herb Chambers Companies | |
| 1134 | Lumber Liquidators | |

---

## QUORUM_CROSS_CLOUD Database (ATTAIN_FEED)

### HOUSE_HOLD_MAPPING
- **Columns:** DEVICE_ID, HOUSEHOLD_ID, CONFIDENCE_SCORE
- **Rows:** 497M

### MAID_CENTROID_ASSOCIATION
- **Columns:** DEVICE_ID, CENSUS_BLOCK_ID, ZIP_CODE, ZIP_POPULATION
- **Rows:** 497M
- **Use:** Where visitors live

### SEGMENT_MAID_WITH_IP
- **Columns:** SEGMENT_MD5, DEVICE_ID, IP_ADDRESS, PING_TIMESTAMP, CREATED_AT
- **Rows:** 25B
- **Use:** Device-level attribution

### SEGMENT_META_DATA
- **Columns:** SEGMENT_MD5, SEGMENT_UNIQUE_ID, BRAND, CATEGORY, DMA_NAME, ZIP_CODE
- **Rows:** 739K
- **Use:** Lightweight brand lookup

---

## Impression Logs

### XANDR_IMPRESSION_LOG
Multi-platform ad impressions (see PT codes above).

**Key Columns:**
- `ID`, `IMP_ID`, `AUCTION_ID` - Identifiers
- `TIMESTAMP`, `SYS_TIMESTAMP` - Timing
- `ADVERTISER_ID`, `AGENCY_ID`, `QUORUM_ADVERTISER_ID` - Advertiser links
- `CAMPAIGN_ID`, `IO_ID`, `LINEITEM_ID` - Campaign hierarchy
- `CLIENT_IP`, `USER_IP` - For attribution
- `DEVICE_UNIQUE_ID` - Device ID ("SYS-" prefix = synthetic)
- `LATITUDE`, `LONGITUDE`, `POSTAL_CODE`, `DMA` - Geography
- `REFERER_URL`, `PUBLISHER_ID`, `PUBLISHER_CODE` - Publisher
- `PT` - Platform code (see table above)
- `SUPPLY_TYPE` - 0=web, 1=app

### WEBPIXEL_IMPRESSION_LOG
Website pixel tracking.

**Key Columns:**
- `ID`, `UUID` - Identifiers
- `TIMESTAMP`, `SYS_TIMESTAMP` - Timing
- `AG_ID` - Agency ID
- `REFERER` - Website URL visited
- `CLIENT_IP` - For attribution

---

## Attribution Flow
```
Ad Impression (XANDR) or Website Visit (WEBPIXEL)
    â†“ CLIENT_IP / USER_IP
IP_MAID_MAPPING (IP â†’ DEVICE_ID)
    â†“ DEVICE_ID
SEGMENT_MAID_WITH_IP or SEGMENT_DEVICES_CAPTURED
    â†“ SEGMENT_MD5
SEGMENT_MD5_MAPPING â†’ SEGMENT_DETAIL (brand/location)
    â†“
CAMPAIGN_PERFORMANCE_STORE_VISITS_RAW
```

---

## Vendor IDs
| Vendor | ID |
|--------|-----|
| Unacast | 16 |
| Veraset | 18 |

---

## Changelog
- **2026-01-17:** Initial creation during Snowflake MCP setup
