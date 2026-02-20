# Quorum Optimizer Migration — Project Status

**Last updated:** Feb 19, 2026
**Branch:** main
**Latest commit:** aec8ebf (Fix store visits for Causal iQ)

---

## System Overview

The Quorum Location Intelligence Platform optimizer reads from Snowflake and serves lift analysis, campaign performance, and attribution metrics via a Flask API (`optimizer_api_v6.py`) with a single-page frontend (`optimizer_v6.html`).

### Data Flow

```
AD_IMPRESSION_LOG_V2 (real device UUIDs + IPs)
        │
        ├── PIXEL_CAMPAIGN_MAPPING_V2 (Quorum ADV ↔ DSP ADV bridge)
        │
        ├── IP_HOUSEHOLD_LOOKUP (IP → Household, 68.6M IPs → 31.1M HH)
        │
        ├── AD_TO_WEB_VISIT_ATTRIBUTION (web visits, 6 agencies with URL mappings)
        │        └── MAID is 99.5% synthetic (IP_ENRICHED), HOUSEHOLD_ID backfilled
        │
        ├── WEB_TO_STORE_VISIT_ATTRIBUTION (derived store visits, 7 agencies)
        │        └── Web→Store pipeline, HOUSEHOLD_ID present
        │
        └── STORE_VISITS (base store visits, 11 agencies)
                 └── Ad→Store pipeline, MAID is real UUID (V5 source)
```

### Identity Resolution

**Critical architectural fact:** Attribution MAIDs (AD_TO_WEB_VISIT_ATTRIBUTION) are 99.5% synthetic IPs (MAID_SOURCE = 'IP_ENRICHED'), NOT real mobile ad IDs. They will NEVER match impression log DEVICE_ID_RAW. The only viable join path is:

```
Impression IP → IP_HOUSEHOLD_LOOKUP → HOUSEHOLD_ID ← Attribution HOUSEHOLD_ID
```

Resolution rate: 8-17% depending on agency.

---

## What's Working

### Deployed Fixes (all committed to main)

| Commit | Fix | Impact |
|--------|-----|--------|
| 91715b4 | Lift analysis real web visitor counts, traffic sources agency_id, login token refresh | Web visitors no longer hardcoded to 0; traffic sources works for all agencies; no more stale-token logouts |
| 84f006d | HH matching for campaign/lineitem web visits | Web VR now varies by campaign (was identical across all campaigns for an advertiser) |
| 0cda153 | Agency nav filter + HH matching consolidated | Only active config agencies shown in nav; PCM_4KEY agencies without config excluded |
| aec8ebf | UNION both store visit tables | 15/20 agencies now have store visits (was 7/20). Causal iQ went from 0 to 18.6M rows |

### Snowflake Config Changes

- **6 agencies flipped to ADM_PREFIX:** 1956, 2298, 1955, 2086, 1950, 1565 (weekly stats table was empty for 2026)
- **HOUSEHOLD_ID backfill:** Web visits 93.4% coverage, Store visits 82.5% for Paramount

### Signal Coverage (20 Active Agencies)

| Signal | Coverage | Notes |
|--------|----------|-------|
| Impressions (AD_IMPRESSION_LOG_V2) | 17/20 (85%) | 3 agencies have zero impressions (1221, 1448, 2691 has minimal) |
| Campaign mapping (PIXEL_CAMPAIGN_MAPPING_V2) | 20/20 (100%) | All agencies have mappings |
| Web visit attribution | 6/20 (30%) | **By design** — only agencies with web pixel URL mappings. Not a bug. |
| Store visits (combined) | 15/20 (75%) | UNION of derived (7 agencies) + base (11 agencies), minimal overlap |
| IP→HH resolution | Global | 68.6M IPs → 31.1M households, 8-17% resolution rate |

### Config Alignment

Web visit attribution is opt-in:
- `WEB_PIXEL_URL_COUNT > 0` → only Paramount (1480) has web pixel URLs
- `HAS_WEB_VISIT_ATTRIBUTION = true` → 6 agencies (1480, 1950, 1955, 1956, 2086, 2298)
- The 14 agencies without web visits are **store-visit-focused** — this is correct behavior

Store visit sources are split across two non-overlapping tables:
- `WEB_TO_STORE_VISIT_ATTRIBUTION` (derived): 2298, 1956, 1869, 2086, 1955, 1480, 1680, 1950, 1203, 1565
- `STORE_VISITS` (base): 1813, 1880, 2379, 2234, 1445, 1972, 2393, 2514, 2691, 2744, 1480
- Only agency 1480 appears in both (5 overlapping visitor IDs — negligible)

---

## What's Broken

### P0 — Lift Analysis Uses Wrong Join Strategy

**File:** `optimizer_api_v6.py`, lift analysis query (~line 1404)

**Problem:** The lift analysis joins exposed devices to web/store visitors at the MAID/device level:
```sql
exposed_devices e LEFT JOIN adv_web_visit_days wv ON wv.device_id = e.device_id
```
Where `e.device_id` = DEVICE_ID_RAW (real UUID) and `wv.device_id` = MAID (99.5% synthetic). These NEVER match. Tested across 3 agencies — **zero overlap** in all cases.

**Fix:** Rewrite to household-level matching (same pattern as campaign web visits in commit 0cda153):
1. Resolve exposed impression IPs → households via IP_HOUSEHOLD_LOOKUP
2. Resolve attribution visitor households (already in HOUSEHOLD_ID column)
3. Match at household level
4. Count exposed households that also appear as visitor households

**Impact:** Without this fix, lift analysis shows ~0 web/store visitors in the exposed group, making all lift percentages and z-scores meaningless.

---

## Pending Work

### For This Sprint

- [ ] **P0: Rewrite lift analysis to household matching** (see above)
- [ ] **P1: Add STORE_VISIT_SOURCE config field** — explicitly declare per-agency whether to read from DERIVED, BASE, or BOTH

### For Ali (Data Engineering)

- [ ] Populate HOUSEHOLD_ID at INSERT time in web/store visit BUILDs
- [ ] Daily Snowflake Task for ongoing HOUSEHOLD_ID backfill
- [ ] Populate INSERTION_ORDER_ID in attribution tables

### Stretch Goals

- [ ] Aziz: Temporal IP→HH resolution (improve 8-17% resolution rate)
- [ ] Automated daily signal health monitoring
- [ ] Zip/DMA/publisher/creative tabs: campaign-level HH matching (currently proportional)

---

## Key Tables Reference

| Table | Schema | Purpose | Key Columns |
|-------|--------|---------|-------------|
| AD_IMPRESSION_LOG_V2 | BASE_TABLES | Raw impressions | DEVICE_ID_RAW, USER_IP_FROM_HTTP_REQUEST, DSP_ADVERTISER_ID, INSERTION_ORDER_ID, LINE_ITEM_ID |
| AD_TO_WEB_VISIT_ATTRIBUTION | DERIVED_TABLES | Web visit attribution | MAID (99.5% synthetic), HOUSEHOLD_ID, AD_IMPRESSION_AGENCY_ID, AD_IMPRESSION_ADVERTISER_ID |
| WEB_TO_STORE_VISIT_ATTRIBUTION | DERIVED_TABLES | Store visits (web→store) | AGENCY_ID, ADVERTISER_ID, MAID, HOUSEHOLD_ID, STORE_VISIT_DATE |
| STORE_VISITS | BASE_TABLES | Store visits (ad→store) | AGENCY_ID, QUORUM_ADVERTISER_ID, MAID (real UUID), INSERTION_ORDER_ID, LINE_ITEM_ID |
| IP_HOUSEHOLD_LOOKUP | HOUSEHOLD_CORE | IP→Household resolution | IP_ADDRESS → HOUSEHOLD_ID (68.6M rows) |
| PIXEL_CAMPAIGN_MAPPING_V2 | REF_DATA | Advertiser ID bridge | QUORUM_ADVERTISER_ID ↔ DSP_ADVERTISER_ID, AGENCY_ID |
| REF_ADVERTISER_CONFIG | BASE_TABLES | Agency/advertiser config | IMPRESSION_JOIN_STRATEGY, PLATFORM_TYPE_IDS, HAS_STORE/WEB_VISIT_ATTRIBUTION, WEB_PIXEL_URL_COUNT, POI_URL_COUNT |

---

## Files

| File | Purpose |
|------|---------|
| optimizer_api_v6.py | Flask API — all endpoints, enrichment functions, lift analysis |
| optimizer_v6.html | Frontend SPA — all tabs, charts, tables |
| optimizer_v6_migration.py | Config loading, strategy routing (ADM_PREFIX vs PCM_4KEY) |
| signal_diagnostic_dashboard.html | Signal coverage audit dashboard (generated Feb 19, 2026) |
