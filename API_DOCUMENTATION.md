# Quorum Optimizer API v2 - Technical Documentation

**Last Updated:** January 18, 2026  
**Version:** 2.0  
**Status:** Production Ready  

---

## Table of Contents

1. [Overview](#overview)
2. [Architecture](#architecture)
3. [Attribution Logic](#attribution-logic)
4. [Platform (PT) Configuration](#platform-pt-configuration)
5. [API Endpoints](#api-endpoints)
6. [Data Sources](#data-sources)
7. [Testing](#testing)
8. [Deployment](#deployment)

---

## Overview

The Quorum Optimizer API provides campaign analytics and attribution data for advertising optimization. It supports both **store visit attribution** (physical location visits) and **web event attribution** (website visits, leads, purchases).

### Key Features

- **Last-touch attribution**: Only the most recent impression before a conversion gets credit
- **Multi-platform support**: Handles 14+ ad platforms with different data structures
- **Configurable publisher resolution**: PT-level defaults with agency/advertiser overrides
- **Unified metrics**: Consistent naming (S_VISITS, W_VISITS, W_LEADS, W_PURCHASES, W_AMOUNT)

### Tech Stack

- **Backend**: Python Flask
- **Database**: Snowflake (QUORUMDB.SEGMENT_DATA)
- **Hosting**: Railway
- **Repository**: github.com/ezrakd/quorum-optimizer

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        Client Applications                       │
│                    (React Frontend, Reports)                     │
└─────────────────────────────────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────┐
│                      Flask API (Railway)                         │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────────┐  │
│  │ Config      │  │ Store Visit │  │ Web Events              │  │
│  │ Endpoints   │  │ Endpoints   │  │ Endpoints               │  │
│  └─────────────┘  └─────────────┘  └─────────────────────────┘  │
│                                                                  │
│  ┌─────────────────────────────────────────────────────────────┐│
│  │              PT Configuration System                        ││
│  │  (Platform defaults → Agency overrides → Advertiser overrides)│
│  └─────────────────────────────────────────────────────────────┘│
└─────────────────────────────────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────┐
│                     Snowflake (QUORUMDB)                         │
│  ┌─────────────────────┐  ┌─────────────────────────────────┐   │
│  │ QUORUM_ADV_STORE_   │  │ PARAMOUNT_AD_FULL_ATTRIBUTION_  │   │
│  │ VISITS (84M rows)   │  │ V2 (320M rows)                  │   │
│  └─────────────────────┘  └─────────────────────────────────┘   │
│  ┌─────────────────────┐  ┌─────────────────────────────────┐   │
│  │ WEB_VISITORS_TO_LOG │  │ MAID_CENTROID_ASSOCIATION       │   │
│  └─────────────────────┘  └─────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────┘
```

---

## Attribution Logic

### Last-Touch Attribution

All endpoints use **last-touch attribution** - only the most recent impression before a conversion receives credit. This prevents double-counting when a device sees multiple ads before converting.

#### Store Visits Attribution

```sql
ROW_NUMBER() OVER (
    PARTITION BY DEVICE_ID_QU, DRIVEBYDATE 
    ORDER BY IMP_TIMESTAMP DESC
) as rn
-- Keep only rn = 1
```

- **Partition**: Device + Visit Date
- **Order**: Most recent impression timestamp wins
- **Source**: QUORUM_ADV_STORE_VISITS

#### Web Events Attribution

```sql
ROW_NUMBER() OVER (
    PARTITION BY IMP_MAID, SITE_VISIT_ID 
    ORDER BY IMP_DATE DESC
) as rn
-- Keep only rn = 1
```

- **Partition**: Device + Site Visit ID
- **Order**: Most recent impression date wins
- **Source**: PARAMOUNT_AD_FULL_ATTRIBUTION_V2

### Why Last-Touch?

The gold tables (QUORUM_ADV_STORE_VISITS, PARAMOUNT_AD_FULL_ATTRIBUTION_V2) mark ALL impressions that preceded a conversion as attributed. Example: A device with 20 impressions across 5 publishers, then visits a store - all 20 rows have `IS_STORE_VISIT=TRUE`. Without last-touch logic, we'd count 20 visits instead of 1.

---

## Platform (PT) Configuration

### The Problem

Each ad platform (Trade Desk, MNTN, Adelphic, etc.) stores publisher information in different columns:
- Some use `PUBLISHER_ID` (numeric)
- Some use `PUBLISHER_CODE` (alphanumeric)
- Some use `SITE` (domain/URL)
- Some URL-encode values

### The Solution

A hierarchical configuration system:

```
1. Advertiser Override (highest priority)
2. Agency Override
3. PT Default
4. Global Default (lowest priority)
```

### PT Configuration (Data-Driven)

Based on actual column population analysis:

| PT | Platform | Primary Column | Coverage | URL Decode |
|----|----------|---------------|----------|------------|
| 6 | Trade Desk | SITE | 100% | No |
| 8 | DV 360 | PUBLISHER_ID | 99% | No |
| 9 | DCM/GAM | PUBLISHER_ID | 99.9% | No |
| 11 | Xandr | PUBLISHER_ID | 100% | No |
| 13 | Adelphic | PUBLISHER_ID | 92% | No |
| 20 | SpringServe | PUBLISHER_CODE | 97% | No |
| 21 | FreeWheel | SITE | 94.5% | No |
| 22 | MNTN | PUBLISHER_CODE | 91.5% | **Yes** |
| 23 | Yahoo | PUBLISHER_ID | 100% | No |

### Agency Configuration

Some agencies use different PTs per advertiser:

| Agency ID | Agency | PT Assignment | Notes |
|-----------|--------|---------------|-------|
| 1813 | Causal iQ | BY_ADVERTISER | Uses PT 6, 8, 9, 11, 23 |
| 2514 | MNTN | BY_AGENCY | Primarily PT 22 |
| 2234 | Magnite | BY_ADVERTISER | Uses PT 0, 11, 20, 23, 25, 33 |
| 1956 | Dealer Spike | BY_AGENCY | Uses Adelphic (PT 13) |

### Config Resolution API

```
GET /api/config/resolve?pt=22&agency_id=2514&advertiser_id=45143
```

Returns the effective configuration after applying all overrides.

---

## API Endpoints

### Configuration Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/health` | GET | Health check |
| `/api/config/platforms` | GET | All PT configurations |
| `/api/config/agencies` | GET | Agency-level overrides |
| `/api/config/advertisers` | GET | Advertiser-level overrides |
| `/api/config/resolve` | GET | Resolve effective config |

### Store Visits Endpoints

| Endpoint | Method | Parameters | Returns |
|----------|--------|------------|---------|
| `/api/agencies` | GET | - | Agency list with S_VISITS |
| `/api/advertisers` | GET | `agency_id` | Advertiser list with PT |
| `/api/advertiser-summary` | GET | `advertiser_id`, `start_date`, `end_date` | Summary metrics |
| `/api/zip-performance` | GET | `advertiser_id`, `start_date`, `end_date`, `min_impressions` | ZIP + DMA + Population + S_VISITS |
| `/api/campaign-performance` | GET | `advertiser_id`, `start_date`, `end_date` | Campaign breakdown |
| `/api/lineitem-performance` | GET | `advertiser_id`, `campaign_id`, `start_date`, `end_date` | Line item breakdown |
| `/api/publisher-performance` | GET | `advertiser_id`, `start_date`, `end_date` | Publisher breakdown |

### Web Events Endpoints

| Endpoint | Method | Parameters | Returns |
|----------|--------|------------|---------|
| `/api/web/advertisers` | GET | - | Web advertisers (basic) |
| `/api/web/advertisers-v2` | GET | - | Web advertisers with campaign/lineitem counts |
| `/api/web/summary` | GET | `advertiser_id`, `start_date`, `end_date` | W_VISITS, W_LEADS, W_PURCHASES, W_AMOUNT |
| `/api/web/zip-performance` | GET | `advertiser_id`, `start_date`, `end_date`, `min_impressions` | ZIP + all web metrics |
| `/api/web/campaign-performance` | GET | `advertiser_id`, `start_date`, `end_date` | Campaign breakdown |
| `/api/web/lineitem-performance` | GET | `advertiser_id`, `campaign_id`, `start_date`, `end_date` | Line item breakdown |
| `/api/web/publisher-performance` | GET | `advertiser_id`, `start_date`, `end_date` | Publisher breakdown |

### Standard Parameters

| Parameter | Format | Default | Description |
|-----------|--------|---------|-------------|
| `start_date` | YYYY-MM-DD | 2020-01-01 | Filter start |
| `end_date` | YYYY-MM-DD | 2030-12-31 | Filter end |
| `min_impressions` | Integer | 100 | Minimum impressions threshold |

### Response Format

All endpoints return:

```json
{
  "success": true,
  "data": [ ... ]
}
```

Or on error:

```json
{
  "success": false,
  "error": "Error message"
}
```

---

## Data Sources

### Primary Tables

| Table | Purpose | Rows | Key Columns |
|-------|---------|------|-------------|
| `QUORUM_ADV_STORE_VISITS` | Store visit attribution | 84M | DEVICE_ID_QU, DRIVEBYDATE, IMP_TIMESTAMP, IS_STORE_VISIT, PT, IO_ID, LINEITEM_ID, PUBLISHER_ID/CODE, IMP_MAID |
| `PARAMOUNT_AD_FULL_ATTRIBUTION_V2` | Web event attribution | 320M | IMP_MAID, SITE_VISIT_ID, IMP_DATE, IS_SITE_VISIT, PT, IO_ID, LINEITEM_ID, SITE |
| `WEB_VISITORS_TO_LOG` | Web events (basic) | - | MAID, SITE_VISIT_TIMESTAMP, IS_SITE_VISIT, IS_LEAD, IS_PURCHASE, PURCHASE_VALUE |

### Reference Tables

| Table | Purpose |
|-------|---------|
| `AGENCY_ADVERTISER` | Agency/advertiser names and metadata |
| `MAID_CENTROID_ASSOCIATION` | Device ID → ZIP code mapping |
| `ZIP_DMA_MAPPING` | ZIP → DMA lookup |
| `ZIP_POPULATION_DATA` | ZIP → Population |
| `PUBLISHERS_ID_NAME_MAPPING` | Publisher ID → Name (Trade Desk) |

### Important Notes

1. **Join on Device ID, not Census Block**: ZIP queries must join MAID_CENTROID_ASSOCIATION on `DEVICE_ID`, not `CENSUS_BLOCK_ID` (which causes row multiplication)

2. **IS_STORE_VISIT marks ALL preceding impressions**: Not just last-touch. Must apply ROW_NUMBER() logic.

3. **PARAMOUNT table is PT=21 (FreeWheel) only**: For web events with campaign/lineitem data

4. **URL encoding**: MNTN (PT=22) values are double URL-encoded (e.g., `Paramount%2520Streaming`)

---

## Testing

### Store Visits Test Data

| Advertiser | ID | Agency | Impressions | S_VISITS |
|------------|-----|--------|-------------|----------|
| Noodles & Company | 45143 | MNTN (2514) | 14.7M | ~24K |

```bash
curl "https://quorum-optimizer-production.up.railway.app/api/advertiser-summary?advertiser_id=45143"
```

### Web Events Test Data

| Advertiser | ID | W_VISITS | Campaigns | Publishers |
|------------|-----|----------|-----------|------------|
| Montana Knife Company | 37838 | 4,720 | 4 | 41 |
| Herschel Supply Co | 36209 | 4,431 | 1 | 46 |
| Nittany Digital LLC | 29574 | 3,371 | 10 | 91 |

```bash
curl "https://quorum-optimizer-production.up.railway.app/api/web/campaign-performance?advertiser_id=37838"
```

---

## Deployment

### Environment Variables

```bash
SNOWFLAKE_ACCOUNT=FZB05958.us-east-1
SNOWFLAKE_USER=OPTIMIZER_SERVICE_USER
SNOWFLAKE_PASSWORD=<secret>
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
PORT=5000
```

### Railway Deployment

```bash
cd quorum-optimizer-api
git add .
git commit -m "Update API"
git push  # Auto-deploys to Railway
```

### Local Development

```bash
pip install flask flask-cors snowflake-connector-python
export SNOWFLAKE_PASSWORD="..."
python app.py
```

---

## Metrics Glossary

| Metric | Source | Description |
|--------|--------|-------------|
| `S_VISITS` | QUORUM_ADV_STORE_VISITS | Physical store visits (last-touch) |
| `W_VISITS` | WEB_VISITORS_TO_LOG / PARAMOUNT | Website visits (last-touch) |
| `W_LEADS` | WEB_VISITORS_TO_LOG | Lead form submissions (last-touch) |
| `W_PURCHASES` | WEB_VISITORS_TO_LOG | Purchase conversions (last-touch) |
| `W_AMOUNT` | WEB_VISITORS_TO_LOG | Total purchase dollar value (last-touch) |
| `IMPRESSIONS` | Various | Total ad impressions (all, not just converting) |
| `PT` | Various | Platform Type ID |
| `PT_NAME` | Config | Human-readable platform name |

---

## Future Development

### Planned Features

1. **Audience Explorer**: SEGMENT_POI_OVERLAP analysis for cross-visitation
2. **Generate Audience**: Export to DSPs (TTD, LiveRamp)
3. **Frequency Analysis**: 5-bucket frequency breakdown
4. **Lift Calculation**: Exposed vs control with statistical significance
5. **Admin UI**: Config management interface for PT/Agency/Advertiser settings

### Known Limitations

1. **MNTN line items**: Often populated as "0" - platform doesn't pass this data
2. **PARAMOUNT table scope**: Only covers FreeWheel (PT=21) advertisers for web events
3. **Date filtering**: Uses impression timestamp, not conversion timestamp

---

## Changelog

### v2.0 (January 18, 2026)
- Added last-touch attribution across all endpoints
- Added PT configuration system with override hierarchy
- Added web events campaign/lineitem/publisher endpoints
- Added config resolution API
- Fixed ZIP join (device-level instead of census block)
- Unified metrics naming (S_VISITS, W_VISITS, etc.)

### v1.0 (January 17, 2026)
- Initial API with store visits endpoints
- Basic agency/advertiser listing
- ZIP and campaign performance

