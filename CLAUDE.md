# Project Aquarius — Session Memory

## What This Project Is
Quorum Location Intelligence Platform optimizer migration. Flask API (optimizer_api_v6.py) + HTML frontend (optimizer_v6.html) backed by Snowflake. The optimizer shows advertisers how their ad impressions correlate with web visits and store visits, broken down by publisher, geography, creative, campaign, and other dimensions.

## Critical Rules — DO NOT VIOLATE
1. **NEVER replace the Quorum logo.** The real logo is a base64-encoded JPEG. It lives in optimizer_v6.html, login.html, and config_admin.html. Do NOT generate SVG alternatives.
2. **NEVER use PARAMOUNT_IMPRESSIONS_REPORT.** It was eliminated from all active query paths in commit 30bc933. Use the canonical HH join path (Block 1) instead.
3. **Read reference_archive/docs/DECISIONS_DO_NOT_REVERSE.md** at session start. These 10 decisions were made after extensive debugging.
4. **Read reference_archive/docs/GOTCHAS.md** at session start. These are known data traps.
5. **Two store visit pipelines**: WEB_TO_STORE_VISIT_ATTRIBUTION (derived) and STORE_VISITS (base). Both must be UNIONed.
6. **MAID identity mismatch**: Attribution MAIDs are 99.5% synthetic (IP_ENRICHED). Impression DEVICE_ID_RAW is real UUID. They NEVER match. Use household-level matching.
7. **Always audit data availability before building a new tab or feature.** Different DSPs populate different columns. See "DSP Data Sparsity" below.
8. **Build vertically, not horizontally.** Reusable building blocks, not one-off tab-specific logic.
9. **Geographic tab requires 3K impression minimum** per zip to display.

## What Each Tab Is Supposed To Show

Understanding what each tab MEANS is critical. Previous sessions made mistakes by confusing concepts.

- **Agency Overview**: High-level metrics for all advertisers under an agency. Impressions, web visits, store visits, visit rates.
- **Advertiser List**: Same metrics broken down per advertiser within the agency.
- **Campaign / IO**: Per-insertion-order and per-campaign metrics. Requires PIXEL_CAMPAIGN_MAPPING_V2 to link DSP campaigns to Quorum advertisers.
- **Line Item**: Same pattern, one level deeper.
- **Publisher**: Which ad publishers (SITE_DOMAIN from AD_IMPRESSION_LOG_V2) drove impressions, and what % of exposed households visited. NOTE: Many agencies have SITE_DOMAIN = "0" (blank). Only show this tab when publisher data exists.
- **Creative**: Which ad creatives performed best. Uses CREATIVE_ID from AD_IMPRESSION_LOG_V2. Some agencies have IDs but no names. Evaluate WEB_VISITORS_TO_LOG for bounce rate data.
- **Geographic**: Per-zip-code performance. Uses USER_POSTAL_CODE from AD_IMPRESSION_LOG_V2. Most agencies have this = "0". Only Causal iQ and Magnite have real zip data currently. 3K impression minimum per zip to display.
- **Lift Analysis**: Exposed vs control household cohorts. Measures whether ad exposure actually increases visit rates above baseline. Uses Block 1 HH join at household level.
- **Traffic Sources**: HOW users arrive at the advertiser's website — google.com, facebook.com, direct, etc. This is REFERRAL TRAFFIC from **WEBPIXEL_EVENTS** (UTM_SOURCE, CLIENT_REFERRER_URL), NOT ad impression publishers. Only agencies with deployed web pixels have this data. This is conceptually different from the Publisher tab.
- **Timeseries**: Web and store visits over time. Uses AD_TO_WEB_VISIT_ATTRIBUTION + STORE_VISITS grouped by date.

## DSP Data Sparsity — The Core Inconsistency Problem

**Different DSPs populate different columns in AD_IMPRESSION_LOG_V2.** The code must check what's available, not assume everything is populated.

### Data Availability Matrix (as of Feb 2026, last 30 days):

| Agency | AGENCY_ID | Strategy | SITE_DOMAIN | USER_POSTAL_CODE | CREATIVE_ID | CREATIVE_NAME | Web Pixel |
|--------|-----------|----------|-------------|-----------------|-------------|---------------|-----------|
| Paramount | 1480 | ADM_PREFIX | Opaque IDs (g1094458) | ALL "0" | Yes (525) | Yes (1,109) | Yes (49M events) |
| Dealer Spike | 1956 | ADM_PREFIX | ALL "0" | ALL "0" | Yes (1,199) | No | No |
| InteractRV | 2298 | ADM_PREFIX | ALL "0" | ALL "0" | Yes (280) | No | No |
| Level5 | 2086 | ADM_PREFIX | ALL "0" | ALL "0" | Yes (38) | No | No |
| ARI | 1955 | ADM_PREFIX | ALL "0" | ALL "0" | Yes (149) | No | No |
| ByRider | 1950 | ADM_PREFIX | ALL "0" | ALL "0" | Yes (48) | No | Yes (85K events) |
| NPRP Media | 1565 | ADM_PREFIX | ALL "0" | ALL "0" | Yes (68) | No | Yes (1.2M events) |
| Causal iQ | 1813 | PCM_4KEY | Real domains (85% "0") | Real zips (289K) | Yes (1,035, some "0") | Yes (126) | No |
| MNTN | 2514 | PCM_4KEY | ALL "0" | ALL "0" | Yes (552, 75% "0") | Yes (937) | No |
| Magnite | 2234 | PCM_4KEY | Real domains (4,097) | Real zips (5,036) | Only 3 total | Yes (51) | Yes (6K events) |

### What this means for each tab:
- **Publisher tab**: Only works for Paramount (opaque IDs), Causal iQ, and Magnite. Blank for everyone else.
- **Geographic tab**: Only works for Causal iQ and Magnite. Blank for everyone else including Paramount.
- **Creative tab**: Works for most agencies (they have CREATIVE_ID). But names are missing for Dealer Spike, InteractRV, Level5, ARI, ByRider.
- **Traffic Sources**: Only works for agencies with web pixels: Paramount (49M), LotLinx (71M), NPRP (1.2M), ByRider (85K). Data source is WEBPIXEL_EVENTS, NOT AD_IMPRESSION_LOG_V2.

### Paramount publisher IDs are opaque
SITE_DOMAIN values like "g1094458" are Xandr/GAM publisher IDs, not human-readable domain names. May need a mapping table to resolve to real names. This is a known gap.

## Architecture Quick Ref
- **API**: optimizer_api_v6.py (Flask, Snowflake)
- **Frontend**: optimizer_v6.html (single-file HTML/JS/CSS)
- **Login**: login.html (Clerk auth)
- **Admin**: config_admin.html (advertiser config)
- **Locked Reference**: optimizer_v5_24.html (DO NOT DELETE — reference for original behavior)

## Canonical Table Tiers (see TABLE_MAP.md for full detail)
- **Tier 1 (Base)**: AD_IMPRESSION_LOG_V2, STORE_VISITS, REF_ADVERTISER_CONFIG, REF_LOCATION
- **Tier 2 (Reference)**: PIXEL_CAMPAIGN_MAPPING_V2
- **Tier 3 (Derived)**: AD_TO_WEB_VISIT_ATTRIBUTION, WEB_TO_STORE_VISIT_ATTRIBUTION, VW_WEB_VISITS_UNIFIED
- **Tier 4 (Household)**: IP_HOUSEHOLD_LOOKUP, MAID_HOUSEHOLD_LOOKUP
- **Web Pixel**: WEBPIXEL_EVENTS (in DERIVED_TABLES) — source for Traffic Sources tab
- **Legacy (kept for PCM_4KEY fallback only)**: CAMPAIGN_PERFORMANCE_REPORT_WEEKLY_STATS

## Block Architecture (reusable building blocks)
- **Block 1**: `enrich_visits_by_hh_join()` — foundational function. Takes a GROUP BY column, returns per-dimension web + store visit counts via HH-level join. Used by all per-dimension tabs.
- **Block 2**: Lift Analysis — HH-level exposed vs control cohorts. Built on Block 1 identity resolution.
- **Block 3**: Publisher tab — AD_IMPRESSION_LOG_V2 by SITE_DOMAIN + Block 1. PARAMOUNT_IMPRESSIONS_REPORT eliminated.
- **Block 4** (TODO): Geographic tab — same Block 1 with GROUP BY USER_POSTAL_CODE. 3K impression minimum.
- **Block 5** (TODO): Creative tab — same Block 1 with GROUP BY CREATIVE_ID. Evaluate WEB_VISITORS_TO_LOG for bounce rate.
- **Traffic Sources**: DONE — AD_TO_WEB_VISIT_ATTRIBUTION → WEBPIXEL_EVENTS UUID join. Shows referral traffic (UTM, referrer, direct), not ad publishers.

## Deployed Commits (chronological)
1. 8fd4698 — Remove MAID_HOUSEHOLD_LOOKUP JOIN, use backfilled HOUSEHOLD_ID
2. 91715b4 — Fix lift analysis, traffic sources, login glitch
3. 84f006d — Use household matching for campaign/lineitem web visits
4. 0cda153 — Agency nav filter + HH matching for campaign web visits
5. aec8ebf — Store visit UNION fix (Causal iQ + all base table agencies)
6. 53c7656 — Status doc + signal diagnostic dashboard
7. 30bc933 — Block 1-3 HH rewrite + logo fix (Publisher, Lift, Traffic Sources)

## Known Issues (Current)
- ~~**P0**: Traffic Sources tab wired to wrong data source~~ — FIXED. Now uses AD_TO_WEB_VISIT_ATTRIBUTION joined to WEBPIXEL_EVENTS via WEB_VISIT_UUID for real referral traffic (UTM_SOURCE, CLIENT_REFERRER_URL).
- ~~**P0**: No data availability routing~~ — FIXED. Tabs show "not available" with reason when DSP doesn't populate a column.
- **P1**: Geographic tab (Block 4) not yet built. Only Causal iQ and Magnite have zip data.
- **P1**: Creative tab (Block 5) not yet built. Most agencies have CREATIVE_ID but names vary.
- **P1**: Paramount publisher IDs are opaque (g1094458), need mapping to human-readable names.
- **P1**: Add STORE_VISIT_SOURCE config field per agency (DERIVED/BASE/BOTH)
- **P2**: Causal iQ and Magnite are PCM_4KEY but have row-level impression data — could benefit from Block 1 treatment.
- **P2**: Centroid data (MAID_CENTROID_DATA) not integrated — potential 3rd identity resolution path.

## Corrections From Ezra (Do Not Repeat These Mistakes)
- **Logo**: Never "fix" the logo without visually confirming it. A previous session replaced the real logo with an SVG.
- **PARAMOUNT_IMPRESSIONS_REPORT**: Ezra explicitly said to stop using SEGMENT_DATA tables. Build from canonical tiers only.
- **Traffic Sources ≠ Publishers**: Traffic Sources shows how users arrive at the advertiser's website (google, facebook, direct from WEBPIXEL_EVENTS). Publisher shows which ad exchanges served impressions. These are completely different concepts.
- **Test across agencies, not just Paramount**: Paramount has the richest data. Testing only against Paramount hides problems with other agencies that have sparse DSP data.
- **Audit data before writing code**: Always check what columns actually contain data for each agency before building a feature.

## Team Assignments
- **Ali**: Populate HOUSEHOLD_ID at INSERT time, daily Snowflake Task, populate INSERTION_ORDER_ID
- **Aziz**: Temporal IP→HH resolution (improve beyond current 31% IPv4 rate)

## Snowflake Connection
- Database: QUORUMDB
- Schemas: BASE_TABLES, DERIVED_TABLES, REF_DATA, HOUSEHOLD_CORE, SEGMENT_DATA (legacy)
- MCP SQL tool is SELECT/INSERT/UPDATE only. DDL (CREATE TABLE, ALTER, DROP) must be run by user in Snowsight/SnowSQL.
