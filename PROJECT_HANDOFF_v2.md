# Quorum Optimizer - Project Handoff v2

**Date**: January 18, 2026  
**Status**: Backend API Complete, Frontend Integration Pending  
**API URL**: https://quorum-optimizer-production.up.railway.app  

---

## 🎯 PROJECT OVERVIEW

Campaign optimization tool analyzing store visit and web event performance to provide reallocation recommendations at ZIP, campaign, line item, and publisher levels.

### What's Built

1. **Python Flask API** (1,256 lines) deployed on Railway
2. **PT Configuration System** with agency/advertiser override hierarchy
3. **Last-touch Attribution** across all endpoints
4. **Dual Module Support**: Store Visits + Web Events
5. **Complete Documentation**

---

## ✅ WHAT'S COMPLETE

### Backend API (100%)

#### Store Visits Module
- ✅ `/api/agencies` - List agencies with S_VISITS
- ✅ `/api/advertisers` - Advertisers by agency with PT
- ✅ `/api/advertiser-summary` - Summary with config info
- ✅ `/api/zip-performance` - ZIP + DMA + Population + S_VISITS
- ✅ `/api/campaign-performance` - Campaign breakdown
- ✅ `/api/lineitem-performance` - Line item breakdown
- ✅ `/api/publisher-performance` - Publisher with PT-aware name resolution

#### Web Events Module
- ✅ `/api/web/advertisers` - Basic web advertiser list
- ✅ `/api/web/advertisers-v2` - With campaign/lineitem counts
- ✅ `/api/web/summary` - W_VISITS, W_LEADS, W_PURCHASES, W_AMOUNT
- ✅ `/api/web/zip-performance` - ZIP + all web metrics
- ✅ `/api/web/campaign-performance` - Campaign breakdown
- ✅ `/api/web/lineitem-performance` - Line item breakdown
- ✅ `/api/web/publisher-performance` - Publisher breakdown

#### Configuration System
- ✅ `/api/config/platforms` - All PT configurations
- ✅ `/api/config/agencies` - Agency overrides
- ✅ `/api/config/advertisers` - Advertiser overrides
- ✅ `/api/config/resolve` - Config resolution

### Attribution Logic (100%)
- ✅ Last-touch attribution for store visits
- ✅ Last-touch attribution for web events
- ✅ Proper device-level ZIP joins (fixed census block issue)
- ✅ URL decoding for MNTN publisher names

### Documentation (100%)
- ✅ API_DOCUMENTATION.md - Complete technical reference
- ✅ PROJECT_HANDOFF_v2.md - This file
- ✅ PT_CONFIGURATION.md - Platform config details
- ✅ DATA_DICTIONARY.md - Table and column reference

---

## ❌ WHAT'S NOT COMPLETE

### Frontend (0%)
- React artifact exists but uses hardcoded data
- Need to connect to live API endpoints
- Need to add web events tab

### Additional Features (0%)
- Audience Explorer (SEGMENT_POI_OVERLAP)
- Generate Audience (DSP export)
- Frequency breakdown (5 buckets)
- Lift calculation (exposed vs control)
- Admin UI for config management

---

## 📊 DATA ARCHITECTURE

### Attribution Tables

| Module | Table | Rows | Last-Touch Key |
|--------|-------|------|----------------|
| Store Visits | QUORUM_ADV_STORE_VISITS | 84M | DEVICE_ID_QU + DRIVEBYDATE |
| Web Events | PARAMOUNT_AD_FULL_ATTRIBUTION_V2 | 320M | IMP_MAID + SITE_VISIT_ID |
| Web Events (basic) | WEB_VISITORS_TO_LOG | - | MAID + DATE(SITE_VISIT_TIMESTAMP) |

### Reference Tables

| Table | Purpose |
|-------|---------|
| AGENCY_ADVERTISER | Names and metadata |
| MAID_CENTROID_ASSOCIATION | Device → ZIP |
| ZIP_DMA_MAPPING | ZIP → DMA |
| ZIP_POPULATION_DATA | ZIP → Population |

### Critical Data Notes

1. **Gold tables mark ALL impressions as attributed** - Not just last-touch. Must apply ROW_NUMBER() logic.

2. **ZIP join must use DEVICE_ID** - Joining on CENSUS_BLOCK_ID causes 72x row multiplication.

3. **PARAMOUNT table = FreeWheel (PT=21)** - Web campaign/lineitem data only available for these advertisers.

4. **MNTN values are double URL-encoded** - e.g., `Paramount%2520Streaming` → `Paramount Streaming`

---

## 🔧 PT CONFIGURATION

### Platform Defaults (Based on Data Analysis)

| PT | Platform | Column | Coverage |
|----|----------|--------|----------|
| 6 | Trade Desk | SITE | 100% |
| 8 | DV 360 | PUBLISHER_ID | 99% |
| 9 | DCM/GAM | PUBLISHER_ID | 99.9% |
| 11 | Xandr | PUBLISHER_ID | 100% |
| 13 | Adelphic | PUBLISHER_ID | 92% |
| 20 | SpringServe | PUBLISHER_CODE | 97% |
| 21 | FreeWheel | SITE | 94.5% |
| 22 | MNTN | PUBLISHER_CODE | 91.5% |
| 23 | Yahoo | PUBLISHER_ID | 100% |

### Override Hierarchy

```
Advertiser Override > Agency Override > PT Default > Global Default
```

### Agencies with Variable PT

| Agency | Behavior | Notes |
|--------|----------|-------|
| Causal iQ (1813) | BY_ADVERTISER | Uses PT 6, 8, 9, 11, 23 |
| Magnite (2234) | BY_ADVERTISER | Uses PT 0, 11, 20, 23, 25, 33 |
| MNTN (2514) | BY_AGENCY | Consistently PT 22 |

---

## 🧪 TEST DATA

### Store Visits

```bash
# Noodles & Company - MNTN
curl "https://quorum-optimizer-production.up.railway.app/api/advertiser-summary?advertiser_id=45143"
curl "https://quorum-optimizer-production.up.railway.app/api/publisher-performance?advertiser_id=45143"
```

### Web Events

```bash
# Montana Knife Company - 4,720 web visits
curl "https://quorum-optimizer-production.up.railway.app/api/web/campaign-performance?advertiser_id=37838"

# Nittany Digital - 10 campaigns, 91 publishers
curl "https://quorum-optimizer-production.up.railway.app/api/web/publisher-performance?advertiser_id=29574"
```

---

## 📁 FILE STRUCTURE

```
quorum-optimizer-api/
├── app.py                    # Main API (1,256 lines)
├── requirements.txt          # Dependencies
├── Procfile                  # Railway config
├── API_DOCUMENTATION.md      # Technical reference
├── PROJECT_HANDOFF_v2.md     # This file
├── PT_CONFIGURATION.md       # Platform config details
└── DATA_DICTIONARY.md        # Table reference
```

---

## 🚀 DEPLOYMENT

### Railway (Current)

```bash
cd quorum-optimizer-api
git add .
git commit -m "Update"
git push  # Auto-deploys
```

### Environment Variables

```
SNOWFLAKE_ACCOUNT=FZB05958.us-east-1
SNOWFLAKE_USER=OPTIMIZER_SERVICE_USER
SNOWFLAKE_PASSWORD=<secret>
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
```

---

## 📈 METRICS REFERENCE

| Metric | Meaning |
|--------|---------|
| `S_VISITS` | Store visits (last-touch) |
| `W_VISITS` | Web visits (last-touch) |
| `W_LEADS` | Lead submissions (last-touch) |
| `W_PURCHASES` | Purchases (last-touch) |
| `W_AMOUNT` | Purchase $ value (last-touch) |
| `IMPRESSIONS` | Total impressions |
| `PT` | Platform Type ID |
| `PT_NAME` | Platform name |

---

## 🎯 NEXT STEPS

### Immediate (Frontend)
1. Connect React artifact to live API
2. Add web events tab
3. Implement reallocation logic UI
4. Add export functionality

### Medium-term (Features)
1. Audience Explorer module
2. Generate Audience (DSP export)
3. Admin UI for config management
4. Frequency analysis

### Long-term (Analytics)
1. Lift calculation with significance testing
2. Automated recommendations
3. Scheduled reports

---

## 📞 SUPPORT

### Snowflake
- Account: FZB05958.us-east-1
- User: OPTIMIZER_SERVICE_USER
- MCP Server: QUORUMDB.SEGMENT_DATA.CLAUDESERVER

### Repository
- GitHub: github.com/ezrakd/quorum-optimizer

### Live API
- URL: https://quorum-optimizer-production.up.railway.app
- Health: /api/health

