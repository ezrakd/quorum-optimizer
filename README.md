# Quorum Optimizer — Location Intelligence Platform

**Live**: https://quorum-optimizer-production.up.railway.app
**Admin**: https://quorum-optimizer-production.up.railway.app/admin
**Repo**: github.com/ezrakd/quorum-optimizer
**Deploy**: Railway (auto-deploy from `main`)
**Snowflake User**: `OPTIMIZER_SERVICE_USER`

---

## Architecture

```
Railway (gunicorn)
  └── server.py                     ← Entry point (Procfile: gunicorn server:app)
        ├── optimizer_api_v6.py      ← Reporting API (/api/v6/*)  [READ-ONLY]
        │     └── optimizer_v6_migration.py  ← Config loader (get_agency_config, get_impression_strategy)
        ├── config_api.py            ← Admin API (/api/config/*)   [READ + WRITE]
        ├── optimizer_v6.html        ← Dashboard frontend (served at /)
        └── config_admin.html        ← Admin frontend (served at /admin)
```

All files live in the repo root. Flask serves them as static files via `static_folder=__file__`.

## Files in Production

| File | Lines | Purpose | Status |
|------|-------|---------|--------|
| `server.py` | 52 | Registers blueprints, serves HTML routes | **ACTIVE** |
| `optimizer_api_v6.py` | 2,119 | Config-driven reporting API (v6) | **ACTIVE** |
| `optimizer_v6_migration.py` | 451 | Snowflake config loader, strategy routing | **ACTIVE** (imported by v6) |
| `config_api.py` | 1,103 | Admin config API — mapping, POI, domains | **ACTIVE** |
| `optimizer_v6.html` | 2,206 | Optimizer dashboard UI | **ACTIVE** |
| `config_admin.html` | 1,260 | Config admin 3-panel UI | **ACTIVE** |
| `requirements.txt` | 4 | flask, flask-cors, snowflake-connector, gunicorn | **ACTIVE** |
| `Procfile` | 1 | `gunicorn server:app` | **ACTIVE** |

## Files NOT in Production

| File | Lines | Purpose | Status |
|------|-------|---------|--------|
| `app.py` | 1,954 | Old v5 API (hardcoded agency classes) | **DEAD** — identical to v5_fixed, superseded by v6 |
| `optimizer_api_v5_fixed.py` | 1,954 | v5 backup | **DEAD** — keep for reference only |
| `optimizer_v5_24.html` | 2,201 | Old v5 dashboard | **DEAD** — superseded by v6 html |
| `systems-monitor-app.py` | 655 | Standalone data transfer health monitor | **STANDALONE** — not registered in server.py |
| `data-transfer-health.html` | 2,020 | Systems monitor frontend | **STANDALONE** — paired with systems-monitor-app |

## Dead Documentation (superseded by this file)

These `.md` files were generated during development cycles. They contain useful historical context but are not maintained:

`PROJECT_HANDOFF_v2.md`, `API_DOCUMENTATION.md`, `00_PROJECT_AQUARIUS_FINAL_DELIVERY_SUMMARY.md`, `QUERY_MIGRATION_ANALYSIS.md`, `IMPRESSION_DATA_SOURCES_ASSESSMENT.md`, `OPTIMIZER_MIGRATION_GUIDE.md`, `BASE_ARCHITECTURE_READINESS_STATUS.md`, `NEXT_STEPS_OPTIMIZER_MIGRATION.md`, `DEPLOYMENT_CHECKLIST.md`, `AD_IMPRESSION_LOG_V2_ANALYSIS.md`, `AD_IMPRESSION_LOG_V2_DATE_COVERAGE.md`, `WEB_AND_STORE_VISITS_STATUS.md`, `STORE_VISITS_CORRECTED_STATUS.md`, `PIPELINE_DIAGNOSIS_COMPLETE.md`, `ADMIN_CONFIG_SCREEN_DESIGN.md`, `LOCATION_DEDUP_STRATEGY.md`, `HOUSEHOLD_CORE_Findings.md`, `Entity_Blindspot_Summary.md`, `AUDIENCE_DELIVERY_STATUS.md`, `ali_webpixel_fix_notes.md`, `README_Session_20260213.md`, `README_Reference_Tables.md`, `CHANGELOG.md`, `DATA_DICTIONARY.md`, `PT_CONFIGURATION.md`

---

## Environment Variables (Railway)

| Variable | Value | Purpose |
|----------|-------|---------|
| `SNOWFLAKE_ACCOUNT` | `FZB05958.us-east-1` | Snowflake account locator |
| `SNOWFLAKE_USER` | `OPTIMIZER_SERVICE_USER` | Service account |
| `SNOWFLAKE_PASSWORD` | (secret) | Service account password |
| `SNOWFLAKE_WAREHOUSE` | `COMPUTE_WH` | Compute warehouse |
| `SNOWFLAKE_DATABASE` | `QUORUMDB` | Database |
| `SNOWFLAKE_SCHEMA` | `SEGMENT_DATA` | Default schema |
| `SNOWFLAKE_ROLE` | `OPTIMIZER_READONLY_ROLE` | Read-only role (reporting API) |
| `SNOWFLAKE_ADMIN_ROLE` | `CONFIG_ADMIN_ROLE` | Write role (config API) |
| `PORT` | (set by Railway) | HTTP port |

Role fallback chain in `config_api.py`: `SNOWFLAKE_ADMIN_ROLE` → `SNOWFLAKE_ROLE` → `OPTIMIZER_READONLY_ROLE`

---

## Impression Routing

The v6 API routes queries based on `IMPRESSION_JOIN_STRATEGY` in `REF_ADVERTISER_CONFIG`:

| Strategy | How It Works | Tables Used |
|----------|-------------|-------------|
| `ADM_PREFIX` | Row-level JOIN of `AD_IMPRESSION_LOG_V2` to `PIXEL_CAMPAIGN_MAPPING_V2` at query time | V2 impression log + PCM mapping |
| `PCM_4KEY` | Pre-aggregated rollup from `CAMPAIGN_PERFORMANCE_REPORT_WEEKLY_STATS` | Weekly stats table (has store visits built in) |

ViacomCBS (1480) uses `ADM_PREFIX`. Most other agencies use `PCM_4KEY`.

Web visits are enriched from `AD_TO_WEB_VISIT_ATTRIBUTION` (Ali's pre-matched table) as a separate query merged in Python — not embedded in the impression SQL.

---

## Snowflake Tables — Complete Catalog

### READ Tables (optimizer_api_v6.py — reporting)

| Table | Schema | Used By | Purpose |
|-------|--------|---------|---------|
| `AD_IMPRESSION_LOG_V2` | BASE_TABLES | agencies, advertisers, summary, timeseries, campaign/lineitem/creative/publisher/zip/dma perf, lift, traffic-sources, optimize, optimize-geo | Row-level impression log (ADM_PREFIX path) |
| `PIXEL_CAMPAIGN_MAPPING_V2` | REF_DATA | All ADM_PREFIX queries | Maps DSP_ADVERTISER_ID → QUORUM_ADVERTISER_ID |
| `CAMPAIGN_PERFORMANCE_REPORT_WEEKLY_STATS` | SEGMENT_DATA | agencies, advertisers, summary, timeseries, campaign/lineitem/creative/publisher/zip/dma perf, lift, optimize, optimize-geo, agency-timeseries, advertiser-timeseries | Pre-aggregated weekly stats (PCM_4KEY path) |
| `AD_TO_WEB_VISIT_ATTRIBUTION` | DERIVED_TABLES | agencies, advertisers, summary, timeseries | Pre-matched ad impression → web visit attribution |
| `AGENCY_ADVERTISER` | SEGMENT_DATA | advertisers, advertiser-timeseries, config search | Advertiser names and metadata |
| `CAMPAIGN_POSTAL_REPORTING` | SEGMENT_DATA | zip-performance | ZIP-level pre-aggregated stats |
| `DBIP_LOOKUP_US` | SEGMENT_DATA | zip-performance, dma-performance | ZIP → city/state lookup |
| `ZIP_DMA_MAPPING` | SEGMENT_DATA | optimize-geo | ZIP → DMA mapping |
| `WEB_VISITORS_TO_LOG` | SEGMENT_DATA | creative-performance, traffic-sources | Web visitor bounce data, traffic classification |
| `PARAMOUNT_SITEVISITS` | SEGMENT_DATA | lift-analysis | Web visit events for lift calculation |
| `PARAMOUNT_STORE_VISIT_RAW_90_DAYS` | SEGMENT_DATA | lift-analysis | Store visit events for lift calculation |
| `PARAMOUNT_WEB_IMPRESSION_DATA` | SEGMENT_DATA | traffic-sources | Web impression referrer data |
| `XANDR_IMPRESSION_LOG` | SEGMENT_DATA | lineitem-performance, creative-performance | Creative ID/name/size lookup |
| `REF_DSP_PLATFORM` | BASE_TABLES | campaign-mappings | DSP platform name lookup |
| `STORE_VISITS` | BASE_TABLES | pipeline-health | Freshness check |
| `WEBPIXEL_IMPRESSION_LOG` | BASE_TABLES | pipeline-health | Freshness check |
| `WEBPIXEL_EVENTS` | DERIVED_TABLES | pipeline-health | Freshness check |
| `WEBPIXEL_TRANSFORM_LOG` | DERIVED_TABLES | pipeline-health | Transform batch log |
| `SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY` | (system) | table-access | Query usage anomaly detection |

### READ Tables (config_api.py — admin)

| Table | Schema | Used By | Purpose |
|-------|--------|---------|---------|
| `V_UNMAPPED_AD_IMPRESSIONS` | BASE_TABLES | unmapped-impressions, scorecard | DSP campaigns with no Quorum mapping |
| `WEBPIXEL_EVENTS` | DERIVED_TABLES | unmapped-webpixels, pixel-preview | Active pixel domains + conversion metrics |
| `V_POI_BRAND_SEARCH` | BASE_TABLES | poi-brands | Brand/location search for POI assignment |
| `ADVERTISER_DOMAIN_MAPPING` | DERIVED_TABLES | unmapped-webpixels, mapped-urls | Already-mapped web pixel domains |
| `PIXEL_CAMPAIGN_MAPPING_V2` | REF_DATA | campaign-mappings | Existing campaign mappings |
| `REF_ADVERTISER_CONFIG` | BASE_TABLES | advertiser-config, scorecard, search | Config status flags |
| `AGENCY_ADVERTISER` | SEGMENT_DATA | campaign-mappings, advertiser-config, search | Advertiser/agency names |
| `REF_DSP_PLATFORM` | BASE_TABLES | campaign-mappings | Platform name lookup |

### WRITE Tables (config_api.py only)

| Table | Schema | Operation | Endpoint | Purpose |
|-------|--------|-----------|----------|---------|
| `PIXEL_CAMPAIGN_MAPPING_V2` | REF_DATA | INSERT | map-campaign | Map DSP campaign → Quorum advertiser |
| `ADVERTISER_DOMAIN_MAPPING` | DERIVED_TABLES | INSERT | configure-domain | Map web pixel domain → advertiser |
| `SEGMENT_MD5_MAPPING` | SEGMENT_DATA | INSERT | assign-poi | Assign POI segments to advertiser |
| `REF_ADVERTISER_CONFIG` | BASE_TABLES | UPDATE | map-campaign, configure-domain, assign-poi, update-config | Update config flags and counts |

### Tables NOT Used by App (but referenced in old docs)

These appeared in v5 or earlier documentation but are NOT queried by any active code:

| Table | Why It's Not Used |
|-------|-------------------|
| `QUORUM_ADV_STORE_VISITS` | v5 store visit attribution — replaced by CAMPAIGN_PERFORMANCE_REPORT_WEEKLY_STATS |
| `PARAMOUNT_AD_FULL_ATTRIBUTION_V2` | v5 web attribution — replaced by AD_TO_WEB_VISIT_ATTRIBUTION |
| `PARAMOUNT_DASHBOARD_SUMMARY_STATS` | Inflated site visit counts — removed in v5 fix |
| `MAID_CENTROID_ASSOCIATION` | Device → ZIP mapping — no longer used (ZIP comes from impression log) |
| `ZIP_POPULATION_DATA` | ZIP population — removed, not needed for current reporting |

---

## API Endpoints

### Reporting API (`/api/v6/*`) — optimizer_api_v6.py

All endpoints are **read-only SELECT**. Routing determined by `IMPRESSION_JOIN_STRATEGY`.

| Endpoint | Params | Returns |
|----------|--------|---------|
| `GET /api/v6/agencies` | start_date, end_date | Agency list with impressions, store visits, web visits |
| `GET /api/v6/advertisers` | agency_id | Advertiser list for agency |
| `GET /api/v6/summary` | agency_id, advertiser_id | Advertiser totals |
| `GET /api/v6/timeseries` | agency_id, advertiser_id | Daily impression/visit chart data |
| `GET /api/v6/campaign-performance` | agency_id, advertiser_id | By insertion order |
| `GET /api/v6/lineitem-performance` | agency_id, advertiser_id | By line item |
| `GET /api/v6/creative-performance` | agency_id, advertiser_id | By creative with bounce rate |
| `GET /api/v6/publisher-performance` | agency_id, advertiser_id | By publisher/site |
| `GET /api/v6/zip-performance` | agency_id, advertiser_id | By ZIP code |
| `GET /api/v6/dma-performance` | agency_id, advertiser_id | By DMA |
| `GET /api/v6/lift-analysis` | agency_id, advertiser_id, group_by | Exposed vs control lift |
| `GET /api/v6/traffic-sources` | agency_id, advertiser_id | Web traffic source classification |
| `GET /api/v6/optimize` | agency_id, advertiser_id, dimension | Reallocation recommendations |
| `GET /api/v6/optimize-geo` | agency_id, advertiser_id | Geo optimization (DMA/ZIP) |
| `GET /api/v6/agency-timeseries` | agency_id | Agency-level daily chart |
| `GET /api/v6/advertiser-timeseries` | agency_id | All advertisers daily chart |
| `GET /api/v6/pipeline-health` | — | Data freshness and pipeline status |
| `GET /api/v6/table-access` | — | Query usage anomaly detection |
| `GET /health` | — | App health check |

### Config API (`/api/config/*`) — config_api.py

Write operations require `CONFIG_ADMIN_ROLE`.

| Endpoint | Method | Purpose |
|----------|--------|---------|
| `/api/config/unmapped-impressions` | GET | DSP campaigns needing mapping |
| `/api/config/unmapped-webpixels` | GET | Web pixel domains needing mapping |
| `/api/config/pixel-preview` | GET | Event breakdown for a pixel domain |
| `/api/config/poi-brands` | GET | Search POI brand library |
| `/api/config/campaign-mappings` | GET | List existing campaign mappings |
| `/api/config/map-campaign` | POST | Map DSP campaign → Quorum advertiser |
| `/api/config/configure-domain` | POST | Map web pixel domain → advertiser |
| `/api/config/assign-poi` | POST | Assign POI segments to advertiser |
| `/api/config/mapped-urls` | GET | Show mapped web pixel URLs for advertiser |
| `/api/config/advertiser-config` | GET | Full config status for advertiser |
| `/api/config/search-advertisers` | GET | Autocomplete advertiser search |
| `/api/config/scorecard` | GET | Config completeness metrics |
| `/api/config/update-config` | POST | Update config flags |

---

## Snowflake Roles & Permissions

| Role | Grants | Used By |
|------|--------|---------|
| `OPTIMIZER_READONLY_ROLE` | SELECT on all reporting tables | Reporting API (default) |
| `CONFIG_ADMIN_ROLE` | SELECT + INSERT + UPDATE on config tables | Config API (writes) |

Granted to: `OPTIMIZER_SERVICE_USER` (Railway service account)

```sql
-- If config writes fail with "Insufficient privileges":
GRANT ROLE CONFIG_ADMIN_ROLE TO USER OPTIMIZER_SERVICE_USER;
```

---

## Deployment

```bash
# From the Aquarius repo root:
git add <files> && git commit -m "description" && git push origin main
# Railway auto-deploys from main. Takes ~60 seconds.
```

If git lock files block commits:
```bash
rm -f .git/index.lock .git/HEAD.lock
```

---

## Pipeline Health: What's Automated vs. Manual

### Automated (SFSERVICESUSER — healthy, self-sustaining)

| Table | Writes/7d | Status |
|-------|-----------|--------|
| `XANDR_IMPRESSION_LOG` | 4,803 | Healthy |
| `WEBPIXEL_IMPRESSION_LOG` | 2,442 | Healthy |
| `CAMPAIGN_PERFORMANCE_REPORT_WEEKLY_STATS` | 895 | Healthy (monitor for weekly lag) |
| `CAMPAIGN_PERFORMANCE_STORE_VISITS_RAW` | 481 | Healthy |
| `STORE_VISITS` | 4,407/30d | Healthy |
| `IP_MAID_MAPPING` | 24 | Healthy (1.5B rows) |
| `WEB_VISITORS_TO_LOG` | 334/14d | Healthy |
| `CAMPAIGN_POSTAL_REPORTING` | 135 | Healthy |

### Manual (Ali / ALIDEO — CRITICAL: no automation, no monitoring)

| Table | Writes/7d | Used By | Risk |
|-------|-----------|---------|------|
| `AD_IMPRESSION_LOG_V2` | 7 (manual) | ADM_PREFIX path (ViacomCBS) | **If Ali is unavailable, ViacomCBS goes stale** |
| `AD_TO_WEB_VISIT_ATTRIBUTION` | 13 (manual) | Web visit counts on all dashboards | **Web visits stop updating** |
| `WEB_TO_STORE_VISIT_ATTRIBUTION` | 36 (manual) | Store visit attribution | **Attribution goes stale** |
| `WEBPIXEL_EVENTS` | 13 (manual) | Config admin web pixel screen | **Config admin shows stale data** |

**These four tables must be automated as the top operational priority.**

### Orphaned / Broken

| Process | Issue |
|---------|-------|
| `PARAMOUNT_SITEVISITS` | Zero writes in 90 days. Used by lift-analysis endpoint. Data frozen. |
| `PARAMOUNT_STORE_VISIT_RAW_90_DAYS` | Zero writes in 90 days. Used by lift-analysis endpoint. Data frozen. |
| `DAILY_QRM_V4_REFRESH` (task) | SUSPENDED_DUE_TO_ERRORS. Calls REFRESH_QRM_V4() which can't be read. |
| `PARAMOUNT_DASHBOARD_SUMMARY_STATS` | SFSERVICESUSER still writing (8/7d) but no v6 consumer. Straggler. |
| `AD_IMPRESSION_LOG` (old, not V2) | SFSERVICESUSER still writing (4,563/7d) but no v6 consumer. Straggler. |

### Known Data Anomaly

`AD_IMPRESSION_LOG_V2` contains rows with `AUCTION_TIMESTAMP` as far out as 2286-11-20 (260 years in the future). Corrupt/test data to be cleaned.

---

## Priority Stack (as of Feb 15, 2026)

### P0 — Daily Job Automation + Health Monitoring (NEXT BIG PROJECT)

The biggest action item is to get daily jobs tight, automated, and trackable in the health monitoring dashboard. This means:

1. **Automate the four manual tables** (AD_IMPRESSION_LOG_V2, AD_TO_WEB_VISIT_ATTRIBUTION, WEB_TO_STORE_VISIT_ATTRIBUTION, WEBPIXEL_EVENTS) as Snowflake tasks with CRON schedules.
2. **Build the health monitoring dashboard** to track all daily/weekly jobs with staleness alerts. This is useless until #1 is done.
3. **Deprecate straggler writes** to old AD_IMPRESSION_LOG and PARAMOUNT_DASHBOARD_SUMMARY_STATS.
4. **Decide on lift-analysis tables**: Fix PARAMOUNT_SITEVISITS writer or deprecate the endpoint.

Owner: Aziz + Ali. Ezra wants them to implement so they fully understand what they're managing.

### P1 — Household ETL (final core infrastructure piece)

The household-level ETL is the final core infrastructure piece. HOUSEHOLD_CORE schema exists with substantial data:

| Table | Rows | Purpose |
|-------|------|---------|
| `HOUSEHOLD_UNIVERSE` | 8.3M | Household master records |
| `HOUSEHOLD_DEVICES` | 8.4B | Device-to-household mapping (clustered) |
| `HOUSEHOLD_IDENTIFIERS` | 2.9B | Cross-device identity resolution |
| `HOUSEHOLD_SEGMENTS` | 459K | Segment membership |
| `TARGET_SEGMENTS` | 1,752 | Segment definitions |

None of these are wired to the app yet. This converts device-level attribution into household-level reporting.

### P1 — Automated DNA Population

DNA_CORE schema contains DNA_GRAPH (411M rows) and 30+ PANEL_MAIDS tables. Population needs to be automated. Owner: Ali.

### P2 — Clerk Authentication

User authentication via Clerk. Lower priority — the app currently runs without auth. Described as simple relative to other work.

### P2 — Report Definitions and New Features

| Feature | Description |
|---------|-------------|
| **Audience Creation** | DSP export from segment tables |
| **OOH-to-Web** | Out-of-home to web conversion reporting |
| **Tourism** | Tourism-focused reporting module |
| **Context Affinity** | Content/context-based audience affinity analysis |

---

## Automation Handoff (for Aziz and Ali)

Operating philosophy: "I need to be able to sail this 85' boat by myself with one deckhand." The system must be manageable by Ezra and Aziz, with Ali handling DBA-level Snowflake work.

1. **Know what's live**: Only `server.py`, `optimizer_api_v6.py`, `optimizer_v6_migration.py`, `config_api.py`, and the two HTML files are in production. Everything else is reference material.

2. **Know the deploy target**: This runs on **Railway** (not Heroku, not AWS, not anything else). Env vars are set in the Railway dashboard. Auto-deploys from GitHub `main`.

3. **Know the Snowflake user**: The service account is `OPTIMIZER_SERVICE_USER` (not `QUORUM_OPTIMIZER`, not `EZRADOTY`). Check this file or Railway env vars if unsure.

4. **Know the role chain**: Reporting uses `OPTIMIZER_READONLY_ROLE`. Config writes use `CONFIG_ADMIN_ROLE`. If a new table needs write access, grant it to `CONFIG_ADMIN_ROLE`.

5. **Don't touch tables you don't own**: The app reads from many tables but only writes to 4 (see WRITE Tables above). Never modify the reporting tables — those are populated by upstream ETL pipelines.

6. **Test before pushing**: Railway auto-deploys. A broken push = broken production. Run `python server.py` locally first if possible.

7. **When using Claude for changes**: Always tell it the current state of the system (Railway, not Heroku; OPTIMIZER_SERVICE_USER, not QUORUM_OPTIMIZER; v6, not v5). Claude can hallucinate service names and usernames from training data. Pin it to this README.

8. **S3 Stages**: 14 external S3 stages exist (PARAMOUNT_DATA_STAGE, COMMONS_DATA_STAGE, UNACAST_DATA_STAGE, VERASET_DATA_STAGE, ALI_DATA_DROP_S3_STAGE, etc.). No Snowpipes — all ingestion is batch COPY INTO from external schedulers.

9. **Snowflake Tasks**: Only 2 exist. `TASK_ATTAIN_SEGMENT_MAID_UPSERT` runs Mon/Wed/Fri (healthy). `DAILY_QRM_V4_REFRESH` is broken (SUSPENDED_DUE_TO_ERRORS).

---

## Quick Reference: Common Maintenance Queries

### Check if an agency has web visit data
```sql
SELECT AD_IMPRESSION_AGENCY_ID, COUNT(*) as WEB_VISITS
FROM QUORUMDB.DERIVED_TABLES.AD_TO_WEB_VISIT_ATTRIBUTION
WHERE WEB_VISIT_DATE >= DATEADD(day, -30, CURRENT_DATE())
GROUP BY AD_IMPRESSION_AGENCY_ID
ORDER BY WEB_VISITS DESC;
```

### Check unmapped impressions for an agency
```sql
SELECT DSP_ADVERTISER_ID, DSP_ADVERTISER_NAME, IMPRESSIONS_30D
FROM QUORUMDB.BASE_TABLES.V_UNMAPPED_AD_IMPRESSIONS
WHERE AGENCY_ID = 1480
ORDER BY IMPRESSIONS_30D DESC;
```

### Check campaign mappings for an agency
```sql
SELECT MAPPING_ID, DSP_ADVERTISER_ID, QUORUM_ADVERTISER_ID, ADVERTISER_NAME_FROM_DSP
FROM QUORUMDB.REF_DATA.PIXEL_CAMPAIGN_MAPPING_V2
WHERE AGENCY_ID = 1480 AND QUORUM_ADVERTISER_ID IS NOT NULL
ORDER BY MAPPING_ID DESC
LIMIT 20;
```

### Check web pixel domain mappings
```sql
SELECT "Agency_Id", "Advertiser_id", "URL", "IS_POI", "CREATED_AT"
FROM QUORUMDB.DERIVED_TABLES.ADVERTISER_DOMAIN_MAPPING
ORDER BY "CREATED_AT" DESC
LIMIT 20;
```

### Check advertiser config status
```sql
SELECT AGENCY_ID, ADVERTISER_ID, ADVERTISER_NAME,
       IMPRESSION_JOIN_STRATEGY, CAMPAIGN_MAPPING_COUNT,
       WEB_PIXEL_URL_COUNT, HAS_WEB_VISIT_ATTRIBUTION, HAS_STORE_VISIT_ATTRIBUTION
FROM QUORUMDB.BASE_TABLES.REF_ADVERTISER_CONFIG
WHERE AGENCY_ID = 1480
ORDER BY CAMPAIGN_MAPPING_COUNT DESC;
```

### Verify impression counts for an advertiser (ADM_PREFIX)
```sql
SELECT COUNT(*) as IMPRESSIONS
FROM QUORUMDB.BASE_TABLES.AD_IMPRESSION_LOG_V2 v
JOIN (
    SELECT DISTINCT DSP_ADVERTISER_ID
    FROM QUORUMDB.REF_DATA.PIXEL_CAMPAIGN_MAPPING_V2
    WHERE QUORUM_ADVERTISER_ID = 29228  -- e.g., Vrai
) pcm ON v.DSP_ADVERTISER_ID = pcm.DSP_ADVERTISER_ID
WHERE v.AUCTION_TIMESTAMP::DATE >= DATEADD(day, -60, CURRENT_DATE());
```

### Check data freshness
```sql
SELECT 'impressions' as source, MAX(AUCTION_TIMESTAMP)::DATE as latest
FROM QUORUMDB.BASE_TABLES.AD_IMPRESSION_LOG_V2
WHERE AUCTION_TIMESTAMP >= DATEADD(day, -3, CURRENT_DATE())
UNION ALL
SELECT 'web_pixels', MAX(STAGING_SYS_TIMESTAMP)::DATE
FROM QUORUMDB.DERIVED_TABLES.WEBPIXEL_EVENTS
WHERE STAGING_SYS_TIMESTAMP >= DATEADD(day, -30, CURRENT_DATE())
UNION ALL
SELECT 'web_attribution', MAX(INSERTED_AT)::DATE
FROM QUORUMDB.DERIVED_TABLES.AD_TO_WEB_VISIT_ATTRIBUTION;
```

### Grant config write access (if needed)
```sql
-- Run as ACCOUNTADMIN:
GRANT ROLE CONFIG_ADMIN_ROLE TO USER OPTIMIZER_SERVICE_USER;
```
