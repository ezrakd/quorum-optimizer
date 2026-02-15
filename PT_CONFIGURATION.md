# Platform (PT) Configuration Guide

**Last Updated:** January 18, 2026

---

## Overview

Each advertising platform (DSP) sends impression data with publisher information in different columns:
- `PUBLISHER_ID` - Numeric identifier
- `PUBLISHER_CODE` - Alphanumeric code
- `SITE` - Domain or URL

The PT Configuration system determines which column to use for each platform, with support for agency and advertiser-level overrides.

---

## PT ID Reference

| PT | Platform | Status |
|----|----------|--------|
| 6 | Trade Desk | ✅ Configured |
| 8 | DV 360 | ✅ Configured |
| 9 | DCM/GAM | ✅ Configured |
| 11 | Xandr | ✅ Configured |
| 12 | Simpli.fi | ✅ Configured |
| 13 | Adelphic | ✅ Configured |
| 14 | Beeswax | ✅ Configured |
| 15 | Xtreme Reach | ✅ Configured |
| 16 | StackAdapt | ✅ Configured |
| 17 | Unknown | ⚠️ No data |
| 20 | SpringServe | ✅ Configured |
| 21 | FreeWheel | ✅ Configured |
| 22 | MNTN | ✅ Configured |
| 23 | Yahoo | ✅ Configured |
| 25 | Amazon DSP | ✅ Configured |
| 27 | Unknown | ✅ Configured |

---

## Data-Driven Configuration

Based on actual column population analysis from XANDR_IMPRESSION_LOG:

### High Coverage Platforms

| PT | Platform | Column | Coverage | Notes |
|----|----------|--------|----------|-------|
| 6 | Trade Desk | SITE | 100% | Best coverage |
| 11 | Xandr | PUBLISHER_ID | 100% | Also has 25% PUBLISHER_CODE |
| 23 | Yahoo | PUBLISHER_ID | 100% | |
| 27 | Unknown | SITE | 100% | |
| 9 | DCM/GAM | PUBLISHER_ID | 99.9% | |
| 8 | DV 360 | PUBLISHER_ID | 99% | |

### Medium Coverage Platforms

| PT | Platform | Column | Coverage | Notes |
|----|----------|--------|----------|-------|
| 20 | SpringServe | PUBLISHER_CODE | 97% | Also has 22% SITE |
| 21 | FreeWheel | SITE | 94.5% | |
| 13 | Adelphic | PUBLISHER_ID | 92% | |
| 22 | MNTN | PUBLISHER_CODE | 91.5% | **URL encoded** |

### Special Handling

#### MNTN (PT=22)
- Uses `PUBLISHER_CODE`
- Values are **double URL-encoded**
- Example: `Paramount%2520Streaming` → decode twice → `Paramount Streaming`
- Config: `"url_decode": true`

#### Trade Desk (PT=6)
- Uses `SITE` for domain names
- Publisher names available in `PUBLISHERS_ID_NAME_MAPPING` table
- Join on `PUBLISHER_CODE = ID` for name lookup

---

## Configuration Structure

### PT_CONFIG (Default)

```python
PT_CONFIG = {
    "22": {
        "name": "MNTN",
        "publisher_column": "PUBLISHER_CODE",
        "fallback_column": None,
        "name_lookup_table": None,
        "url_decode": True,
        "coverage_pct": 91.5
    },
    ...
}
```

### AGENCY_CONFIG (Override)

```python
AGENCY_CONFIG = {
    "1813": {
        "name": "Causal iQ",
        "pt_assignment": "BY_ADVERTISER",
        "notes": "Uses PT 6, 8, 9, 11, 23 depending on advertiser"
    },
    ...
}
```

### ADVERTISER_CONFIG (Override)

```python
ADVERTISER_CONFIG = {
    "12345": {
        "publisher_column": "SITE",
        "url_decode": False,
        "notes": "Special handling for this advertiser"
    }
}
```

---

## Override Hierarchy

Resolution order (first match wins):

1. **Advertiser Override** - Specific advertiser settings
2. **Agency Override** - Agency-level settings
3. **PT Default** - Platform-specific defaults
4. **Global Default** - Fallback for unknown PTs

### Example Resolution

```
Advertiser: 45143 (Noodles & Company)
Agency: 2514 (MNTN)
PT: 22

1. Check ADVERTISER_CONFIG["45143"] → Not found
2. Check AGENCY_CONFIG["2514"] → Found, but no publisher_column override
3. Check PT_CONFIG["22"] → Found: publisher_column = "PUBLISHER_CODE", url_decode = True
4. Result: Use PUBLISHER_CODE, decode URLs
```

---

## API Endpoints

### View All Configurations

```bash
# All PT configs
curl /api/config/platforms

# Agency overrides
curl /api/config/agencies

# Advertiser overrides
curl /api/config/advertisers
```

### Resolve Effective Config

```bash
curl "/api/config/resolve?pt=22&agency_id=2514&advertiser_id=45143"
```

Response:
```json
{
  "success": true,
  "input": {
    "pt": "22",
    "agency_id": "2514",
    "advertiser_id": "45143"
  },
  "resolved_config": {
    "name": "MNTN",
    "publisher_column": "PUBLISHER_CODE",
    "url_decode": true,
    "coverage_pct": 91.5
  }
}
```

---

## Agency PT Patterns

### Consistent PT (BY_AGENCY)

| Agency | ID | PT | Platform |
|--------|-----|-----|----------|
| MNTN | 2514 | 22 | MNTN |
| Dealer Spike | 1956 | 13 | Adelphic |
| InteractRV | 2298 | 13 | Adelphic |
| Level5 | 2086 | 13 | Adelphic |

### Variable PT (BY_ADVERTISER)

| Agency | ID | PTs Used |
|--------|-----|----------|
| Causal iQ | 1813 | 6, 8, 9, 11, 23 |
| Magnite | 2234 | 0, 11, 20, 23, 25, 33 |
| Hearst | 1972 | 0, 6, 9, 16, 28 |

---

## Adding New Configurations

### New PT

Add to `PT_CONFIG` in `app.py`:

```python
"99": {
    "name": "New Platform",
    "publisher_column": "SITE",  # or PUBLISHER_ID, PUBLISHER_CODE
    "fallback_column": None,
    "name_lookup_table": None,
    "url_decode": False,
    "coverage_pct": None  # Update after data analysis
}
```

### New Agency Override

Add to `AGENCY_CONFIG`:

```python
"9999": {
    "name": "New Agency",
    "pt_assignment": "BY_AGENCY",  # or BY_ADVERTISER
    "default_pt": "22",
    "publisher_column": "SITE",  # Optional override
    "notes": "Reason for override"
}
```

### New Advertiser Override

Add to `ADVERTISER_CONFIG`:

```python
"99999": {
    "publisher_column": "PUBLISHER_ID",
    "url_decode": False,
    "notes": "Special handling reason"
}
```

---

## Pixel Mapping Reference

From `Quorum_Pixel_Mapping___DSP_Web_Pixel_Master_Template.xlsx`:

| Parameter | Our Column | DSPs |
|-----------|------------|------|
| `pbid` | PUBLISHER_ID | XANDR, DV360, DCM/GAM, TTD, Adelphic, SimpliFi, SpringServe, Yahoo |
| `site` | SITE | Trade Desk, StackAdapt, SpringServe, FreeWheel |
| `sid` | SITE_ID | XANDR, Adelphic |

This mapping shows which platforms natively support which publisher identifiers.

---

## Troubleshooting

### Publisher Names Missing

1. Check PT config has correct `publisher_column`
2. Verify data exists in that column for the PT
3. Check if URL decoding is needed

### Wrong Publisher Data

1. Use `/api/config/resolve` to see effective config
2. Check for agency/advertiser overrides
3. Verify PT is correct in source data

### Adding Override

If a specific advertiser needs different handling:
1. Add to `ADVERTISER_CONFIG`
2. Restart API
3. Test with `/api/config/resolve`

