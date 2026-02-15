# Audience Delivery Pipeline — Status & Architecture Notes

**Last Updated:** 2026-02-15
**Project:** Aquarius (Config-Driven Optimizer Migration)

---

## Current State

The optimizer migration (Track A) has established config-driven routing for impression tracking, web pixel assignment, POI/store visit attribution, and campaign mapping. The audience delivery pipeline (Track B) is **not yet implemented** in the new architecture.

### What Exists Today (Legacy)

The legacy system manages segment delivery through these Snowflake tables:

- **APP_DB.DELIVERY_OPTION** — 831K LiveRamp entries (TYPE_ID 52), 1.2M Facebook (TYPE_ID 8), 423K Xandr (TYPE_ID 23). Each row maps a segment to a delivery destination with parameters like time window (30/60/90 days, never-expired) and push mode (merged, split).
- **APP_DB.DELIVERY_OPTION_ALIAS** — Friendly names for delivery groups.
- **SEGMENT_DATA.SEGMENT_DELIVERY_DETAILS** — Historical delivery log with timestamps and status.
- **SEGMENT_DATA.LIVERAMP_IMPRESSION_LOG** — Device-level impression data (MAIDs, IPs, segment IDs) keyed to LiveRamp's ingestion format.
- **SEGMENT_DATA.PARAMOUNT_CUSTOM_AUDIENCE_DELIVERY** — Paramount-specific audience file generation.

The legacy delivery flow is managed by the Java/Node backend and is **not** config-driven — each agency's delivery is hardcoded.

### What's in REF_ADVERTISER_CONFIG (Ready for Track B)

The config table already has fields relevant to delivery:

- `EXPOSURE_SOURCE` — IMPRESSION, WEB, OOH (determines which device IDs are available)
- `IMPRESSION_JOIN_STRATEGY` — PCM_4KEY, ADM_PREFIX, DIRECT_AG (determines how impressions map to advertisers)
- `HAS_STORE_VISIT_ATTRIBUTION` / `HAS_WEB_VISIT_ATTRIBUTION` — flags for which attribution types are active
- `PLATFORM_TYPE_IDS` — which DSP platforms are in use
- `SEGMENT_COUNT` — number of POI segments assigned
- `WEB_PIXEL_URL_COUNT` — number of web pixel domains configured

These fields can drive delivery logic: which advertisers are delivery-ready, what device ID types are available, and what attribution windows apply.

---

## Track B: Deliverable Architecture

### Key Principle

**We do not need to replicate the legacy segment-pushing structure.** The new pipeline can be simpler:

- Push **lists of device IDs** (MAIDs, IDFA/GAID, or UUIDv2s if available) rather than complex segment objects.
- A **weekly refresh job** re-queries the underlying data and re-pushes the updated list on the same logic.
- The delivery format is a flat file (PSV/CSV) staged to S3, then SFTP-pushed to the destination (LiveRamp, TTD, etc.).

### Device ID Sources

| Source | Table | ID Type | Coverage |
|--------|-------|---------|----------|
| Ad Impressions | AD_IMPRESSION_LOG_V2 | MAID (IDFA/GAID), IP | 137M rows for agency 1480, back to July 2025 |
| LiveRamp Impressions | LIVERAMP_IMPRESSION_LOG | MAID, RampID (GROUPING_INDICATOR), IP | Historical, device-level with segment IDs |
| Web Pixel Events | WEBPIXEL_EVENTS | IP, Client IP | 7-day rolling, per-agency |
| Store Visits | CAMPAIGN_PERFORMANCE_REPORT_WEEKLY_STATS | Aggregated (no individual device IDs) | Weekly rollup |

### Proposed Flow

```
REF_ADVERTISER_CONFIG (who gets delivery)
    ↓
Query device IDs from source tables (impression log, web pixels, etc.)
    ↓
Format as flat file (LiveRamp PSV or TTD CSV)
    ↓
Stage to S3 bucket
    ↓
SFTP push to destination (LiveRamp, TTD, etc.)
    ↓
Log delivery to SEGMENT_DELIVERY_DETAILS
    ↓
Weekly cron re-runs the same pipeline
```

### LiveRamp File Format (PSV)

Based on the existing eCST (Enhanced Client-Side Tag) integration, LiveRamp expects:

- Pipe-separated values (`.psv`)
- Fields: Device ID, Device Type (IDFA/GAID/IP), Segment ID, Timestamp
- Delivered via SFTP to LiveRamp's endpoint
- Files keyed to RampIDs are processed by LiveRamp for audience creation and retargeting

### Trade Desk Direct (Planned)

Per internal discussion (Feb 2025), direct TTD delivery is planned:
- API-only access (no SFTP)
- ~2,861 segments, 90-day expiry, $1.15 price
- Using LiveRamp format as reference for field mapping

### Platform-Specific Web Pixel Setup

The web pixel configuration documentation covers setup for these platforms, which affects how device IDs are captured:

- **Shopify** — Customer Events API (`analytics.subscribe`)
- **WordPress** — WPCode plugin or header/footer injection
- **Google Tag Manager** — Custom HTML tag
- **Squarespace** — Code Injection (two methods)
- **Ticketmaster** — Custom parameters with event binding
- **WooCommerce** — PHP hook (`woocommerce_thankyou`)
- **Wix** — Settings → Custom Code

The config admin auto-detects platform type from domain patterns when assigning unmapped web pixels.

---

## Dependencies Not Yet Added

```
# requirements.txt additions needed for Track B:
boto3           # S3 upload
paramiko        # SFTP push
schedule        # Weekly job scheduling (or use Railway cron)
```

---

## What's Blocking Track B

1. **Web pixel assignment flow** — Config admin can now discover unmapped pixels and map them to advertisers. This needs to be fully tested and hardened before delivery makes sense.
2. **Impression V2 data gap** — PARAMOUNT_DASHBOARD_SUMMARY_STATS only starts Jan 15, 2026. AD_IMPRESSION_LOG_V2 goes back to July 2025 but needs a materialized daily summary for efficient querying. Currently the optimizer timeseries for ViacomCBS only shows ~1 month of data.
3. **S3 bucket + SFTP credentials** — Need to be provisioned and added as environment variables.
4. **Delivery config in REF_ADVERTISER_CONFIG** — Need new columns: `DELIVERY_ENABLED`, `DELIVERY_DESTINATIONS` (JSON array), `DELIVERY_CADENCE`, `DELIVERY_LOOKBACK_DAYS`.

---

## Files Reference

| File | Role |
|------|------|
| `config_api.py` | Config admin API — advertiser search, web pixel discovery, POI assignment, domain mapping |
| `config_admin.html` | Config admin UI — all panels for managing advertiser configurations |
| `optimizer_api_v6.py` | Main optimizer API — impression/store visit/web visit reporting |
| `optimizer_v6_migration.py` | Agency config loader from REF_ADVERTISER_CONFIG |
| `AUDIENCE_DELIVERY_STATUS.md` | This file |
