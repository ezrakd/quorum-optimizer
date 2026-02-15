# Quorum Optimizer - Module Reference Guide

**Version:** 1.2  
**Last Updated:** January 18, 2026  
**Status:** LOCKED

---

## 📦 MODULE INVENTORY

### Data Modules (Core Analytics)
| Module ID | Module Name | Purpose | Status |
|-----------|-------------|---------|--------|
| `MOD-LIFT` | Lift vs Control | Measure campaign effectiveness vs baseline | 🟡 In Progress |
| `MOD-CAMP` | Campaign Performance | Performance by campaign | 🟢 Complete |
| `MOD-LI` | Line Item Performance | Performance by line item | 🔴 Not Started |
| `MOD-CRE` | Creative Performance | Performance by creative | 🔴 Not Started |
| `MOD-CTX` | Context Optimization | Optimize by show/content/publisher | 🟡 Partial |
| `MOD-GEO` | Geographic Optimization | Optimize by postal code/DMA | 🟢 Complete |
| `MOD-TRF` | Traffic Source Optimization | Optimize by platform/source | 🟡 Partial *(requires web pixel)* |

### UX Modules (Interface)
| Module ID | Module Name | Purpose | Status |
|-----------|-------------|---------|--------|
| `UX-LAYOUT` | Layout Template | Sidebar, tabs, main content structure | 🟢 Complete |
| `UX-COMP` | Reusable Components | DataTable, KPI Cards, Tooltips | 🟢 Complete |
| `UX-STYLE` | Style Guide | Colors, fonts, spacing, glassmorphism | 🟢 Complete |

---

## 📐 STANDARD PAGE TEMPLATE

All advertiser pages should follow this module order:

```
┌─────────────────────────────────────────────────────────────┐
│ 1. MOD-LIFT: KPI Summary Cards                              │
│    [Visit Rate] [Lift vs Control (?)] [Impressions] [Visits]│
├─────────────────────────────────────────────────────────────┤
│ 2. MOD-CAMP: Campaign Performance Table                     │
│    Campaign | Impressions | Visits | Visit Rate | Index     │
├─────────────────────────────────────────────────────────────┤
│ 3. MOD-CTX: Context/Publisher Optimization                  │
│    [Reallocation Cards] + [Data Table] + [Export]           │
├─────────────────────────────────────────────────────────────┤
│ 4. MOD-GEO: Geographic Optimization                         │
│    [Reallocation Cards] + [Data Table] + [Export]           │
├─────────────────────────────────────────────────────────────┤
│ 5. MOD-TRF: Traffic Source Optimization                     │
│    [Data Table] + [Export]                                  │
│    ⚠️  ONLY if advertiser has Quorum web pixel installed    │
└─────────────────────────────────────────────────────────────┘
```

---

## 📊 ADVERTISER STATUS SUMMARY

### Store Visit Advertisers (Location-Based)

| Agency | Agency ID | Advertiser | Adv ID | Impressions | Visits | Rate | Status |
|--------|-----------|------------|--------|-------------|--------|------|--------|
| **MNTN** | 2514 | Noodles & Company | 45143 | 7.6M | 30K | 0.40% | 🟢 Active (full data) |
| **MNTN** | 2514 | First Watch | 27828 | 53.7M | 122K | 0.23% | 📋 Available |
| **MNTN** | 2514 | Sky Zone | 37704 | 30.7M | 89K | 0.29% | 📋 Available |
| **Magnite** | 2234 | Mountain West Bank | 40143 | 3.4M | 11K | 0.33% | 🟢 Active (full data) |
| **Magnite** | 2234 | Visit Pensacola | 39323 | 1.5M | 10.5K | 0.71% | 🟢 Active (full data) |
| **Causal iQ** | 1813 | Lowes TTD | 45141 | 7.0M | 404K | 5.78% | 🟢 Active (PT=6 TTD) |
| **Causal iQ** | 1813 | AutoZone | 8123 | 25.4M | 870K | 3.43% | 🟢 Active (PT=6 TTD) |
| **Causal iQ** | 1813 | Golden Nugget Casino | 27588 | 11.8M | 633K | 5.37% | 🟢 Active (PT=6 TTD) |
| **Causal iQ** | 1813 | Space Coast Tourism | 7389 | 147M | 5.7M | 3.88% | 🟡 GEO only (PT=8 DV360)* |
| **AIOPW** | 2393 | CCT Expanded | 29959 | 72.4M | 18K | 0.03% | 📋 Available |

*Causal iQ PT=6 (TTD): MOD-CTX available via SITE field. PT=8 (DV360): GEO only, no context data.

### Web Visit Advertisers

| Agency | Agency ID | Advertiser | Adv ID | Impressions | Visits | Rate | Status |
|--------|-----------|------------|--------|-------------|--------|------|--------|
| **ViacomCBS WhoSay** | 1480 | Lean RX | 40514 | 1.4M | 24K | 1.67% | 🟢 Active |

---

## 🔢 STANDARD CALCULATIONS (Universal)

These calculations are **identical across all agencies and PTs**. Do not modify per-agency.

### Visit Rate
```
visit_rate = visits / impressions
display: (visit_rate * 100).toFixed(2) + '%'
```

### Performance Index (MOD-CAMP, MOD-LI, MOD-CRE)
```
perf_index = (item_visit_rate / baseline_visit_rate) * 100
// 100 = baseline, >100 = outperforming, <100 = underperforming
```

### Reallocation Priority (MOD-CTX, MOD-GEO, MOD-TRF)
```
imp_share = item_impressions / total_impressions
perf_ratio = item_visit_rate / baseline_visit_rate
priority = (imp_share * 1000) - (perf_ratio * 100)

// Color coding:
// > 100: #ff4444 (red) - URGENT reallocate
// > 50:  #ff8844 (orange)
// > 0:   #ffbb44 (yellow)  
// > -50: #88dd88 (light green)
// else:  #44cc44 (green) - KEEP
```

### Lift vs Control (MOD-LIFT)
```
lift_pct = ((exposed_rate - control_rate) / control_rate) * 100
significance: z-test, p < 0.05
```

### Projected Improvement
```
exc_imps = sum(excluded_items.impressions)
exc_visits = sum(excluded_items.visits)
new_rate = (total_visits - exc_visits) / (total_imps - exc_imps)
improvement = ((new_rate / baseline_rate) - 1) * 100
```

---

## 🗃️ AGENCY CONFIGURATION MATRIX

### Attribution Type by Agency
| Agency ID | Agency Name | Attribution Type | Primary PT | MOD-CTX Status | Notes |
|-----------|-------------|------------------|------------|----------------|-------|
| 2514 | MNTN | `store_visits` | 22 | 🟢 PUBLISHER_CODE | CTV platform, gold table |
| 2234 | Magnite | `store_visits` | 20 | 🟢 PUBLISHER_CODE | SpringServe, XANDR only |
| 1813 | Causal iQ | `store_visits` | 6/8 | 🟢 PT=6: SITE column | TTD has SITE, DV360 no context |
| 1480 | ViacomCBS WhoSay | `web_visits` | 21 | 🟢 SITE column | FreeWheel g#### codes |
| 2393 | AIOPW | `store_visits` | TBD | 🔴 TBD | OOH/Clear Channel |

#### ⚠️ Causal iQ Data Notes
Causal iQ advertisers (Agency 1813) have visit data via CAMPAIGN_POSTAL_REPORTING:
- **PT=6 (TTD)**: MOD-CTX available via SITE field joined to store visits via IMP_ID
  - Lowes TTD (45141), AutoZone (8123), Golden Nugget (27588)
- **PT=8 (DV360)**: SITE field empty, GEO only
  - Space Coast Tourism (7389)
- Campaign names are empty (only "" or "0" values) for all - MOD-CAMP not available

### PT (Platform) Reference
| PT Code | Platform Name | Context Column | Publisher Column | Lookup Table |
|---------|---------------|----------------|------------------|--------------|
| 6 | Trade Desk | `SITE` (domains) | `PUBLISHER_ID` | `PUBLISHERS_ID_NAME_MAPPING` (no match) |
| 8 | DV 360 | N/A (empty) | N/A | None |
| 11 | Xandr | N/A | `PUBLISHER_ID` | None |
| 20 | SpringServe | N/A | `PUBLISHER_CODE` | None (names in field) |
| 21 | FreeWheel | `SITE` (g#### codes) | N/A | None |
| 22 | MNTN | N/A | `PUBLISHER_CODE` | None (names in field) |

---

## 📈 MODULE CONFIGURATIONS BY AGENCY

### MOD-LIFT: Lift vs Control

#### Standard Interface (All Agencies)
| Element | Display |
|---------|---------|
| Visit Rate | Large %, 2 decimals |
| Lift vs Control | +X.X% with (?) tooltip |
| Total Impressions | X.XXM format |
| Total Visits | Number with commas |

#### Agency Configurations
| Agency | Exposed Source | Control Methodology | Status |
|--------|----------------|---------------------|--------|
| 1480 | XANDR + WEBPIXEL join | TBD | 🟡 |
| 2514 | QUORUM_ADV_STORE_VISITS | DMA baseline | 🟡 |
| 1813 | CAMPAIGN_POSTAL_REPORTING | TBD | 🔴 |

---

### MOD-CAMP: Campaign Performance

#### Standard Interface (All Agencies)
| Column | Label | Format |
|--------|-------|--------|
| name | Campaign | text |
| impressions | Impressions | number with commas |
| visits | Visits | number, green |
| visitRate | Visit Rate | 2 decimals + % |
| index | Index | color-coded (100=baseline) |

#### Agency Configurations
| Agency | Source Table | Campaign Column | Status |
|--------|--------------|-----------------|--------|
| **2514** (MNTN) | CAMPAIGN_POSTAL_REPORTING | CAMPAIGN_NAME | 🟢 **LOCKED** |
| **2234** (Magnite) | CAMPAIGN_POSTAL_REPORTING | CAMPAIGN_NAME | 🟢 **LOCKED** |
| **1813** (Causal iQ) | CAMPAIGN_POSTAL_REPORTING | CAMPAIGN_NAME | ⚠️ **EMPTY** |
| **1480** (ViacomCBS) | XANDR_IMPRESSION_LOG | IO_NAME or LI_NAME | 🟡 Needs work |

**Note:** Causal iQ campaign names are empty ("" or "0") in CAMPAIGN_POSTAL_REPORTING. MOD-CAMP not available.

---

### MOD-LI: Line Item Performance

#### Standard Interface (All Agencies)
| Column | Label | Format |
|--------|-------|--------|
| name | Line Item | text |
| impressions | Impressions | number with commas |
| visits | Visits | number, green |
| visitRate | Visit Rate | 2 decimals + % |
| index | Index | color-coded |

#### Agency Configurations
| Agency | Source Table | Line Item Column | Status |
|--------|--------------|------------------|--------|
| 2514 | QUORUM_ADV_STORE_VISITS | LINEITEM_NAME | 🔴 Not Started |
| 1813 | TBD | TBD | 🔴 Not Started |
| 1480 | XANDR_IMPRESSION_LOG | LI_NAME | 🔴 Not Started |

---

### MOD-CRE: Creative Performance

#### Standard Interface (All Agencies)
| Column | Label | Format |
|--------|-------|--------|
| name | Creative | text |
| impressions | Impressions | number with commas |
| visits | Visits | number, green |
| visitRate | Visit Rate | 2 decimals + % |
| index | Index | color-coded |

#### Agency Configurations
| Agency | Source Table | Creative Column | Status |
|--------|--------------|-----------------|--------|
| 2514 | QUORUM_ADV_STORE_VISITS | CREATIVE_NAME | 🔴 Not Started |
| 1813 | TBD | TBD | 🔴 Not Started |
| 1480 | XANDR_IMPRESSION_LOG | CREATIVE_NAME | 🔴 Not Started |

---

### MOD-CTX: Context Optimization

#### Standard Interface (All Agencies)
| Column | Label | Format |
|--------|-------|--------|
| code/id | Context/Publisher | monospace, bold, blue |
| impressions | Impressions | number with commas |
| visits | Visits | number, green |
| visitRate | Visit Rate | 2 decimals + % |
| reallocationPriority | Priority | color-coded |

#### Agency Configurations

| Agency | Source Table | Context Column | Context Label | Status |
|--------|--------------|----------------|---------------|--------|
| **2514** (MNTN) | `QUORUM_ADV_STORE_VISITS` | `PUBLISHER_CODE` | Publisher | 🟢 **LOCKED** |
| **2234** (Magnite) | `XANDR_IMPRESSION_LOG` | `PUBLISHER_CODE` | Publisher | 🟢 **LOCKED** |
| **1480** (ViacomCBS) | `XANDR_IMPRESSION_LOG` | `SITE` | Context Code | 🟢 **LOCKED** |
| **1813** (Causal iQ PT=6) | `XANDR_IMPRESSION_LOG` | `SITE` | Context Code | 🟢 **LOCKED** |
| **1813** (Causal iQ PT=8) | N/A | N/A | N/A | ⚠️ **NOT AVAILABLE** |

**Notes:**
- MNTN: Publisher names directly in PUBLISHER_CODE (e.g., "HBO Max", "NBC", "Paramount Streaming - Comedy")
- Magnite: Publisher names directly in PUBLISHER_CODE (e.g., "Samsung Ads", "DIRECTV", "Sling TV")
- ViacomCBS: Context codes in SITE column (e.g., "g1080220", "g1080212") - filter `LIKE 'g%'`
- Causal iQ PT=6 (TTD): Domain names in SITE column (e.g., "mail.yahoo.com", "poki.com", "solitaired.com")
- Causal iQ PT=8 (DV360): SITE field empty, no context attribution available

---

### MOD-GEO: Geographic Optimization

#### Standard Interface (All Agencies)
| Column | Label | Format |
|--------|-------|--------|
| zip | ZIP Code | monospace, bold, blue |
| dma | DMA | text, gray (if available) |
| impressions | Impressions | number with commas |
| visits | Visits | number, green |
| visitRate | Visit Rate | 2 decimals + % |
| reallocationPriority | Priority | color-coded |

#### Agency Configurations

| Agency | ZIP Source | Has DMA | Pop Weighting | Guardrails | Status |
|--------|------------|---------|---------------|------------|--------|
| **2514** (MNTN) | `USER_HOME_POSTAL_CODE` | Yes | Yes | 3.5K min, 35% max, 5/DMA | 🟢 **LOCKED** |
| **2234** (Magnite) | `USER_HOME_POSTAL_CODE` | Yes | Yes | 5K min, 35% max, 5/DMA | 🟢 **LOCKED** |
| **1813** (Causal iQ) | `USER_HOME_POSTAL_CODE` | Yes | Yes | 50K min, 35% max | 🟢 **LOCKED** |
| **1480** (ViacomCBS) | IP→MAID→ZIP | No | No | 1K min, 35% max | 🟢 **LOCKED** |

**Notes:**
- All store visit agencies use `CAMPAIGN_POSTAL_REPORTING.USER_HOME_POSTAL_CODE`
- DMA lookup via `ZIP_DMA_MAPPING` table
- Causal iQ: Higher minimum threshold (50K) due to massive impression volumes

---

### MOD-TRF: Traffic Source Optimization

#### ⚠️ Prerequisite
**MOD-TRF requires Quorum web pixel on advertiser's website.** Without the pixel, we cannot see which traffic sources drove web visits. This module is only available for advertisers where we can join XANDR impressions to WEBPIXEL conversions.

#### Standard Interface (All Agencies)
| Column | Label | Format |
|--------|-------|--------|
| source | Traffic Source | text, bold |
| impressions | Impressions | number with commas |
| visits | Visits | number, green |
| visitRate | Visit Rate | 2 decimals + % |
| reallocationPriority | Priority | color-coded |

#### Agency Configurations

| Agency | Source Column | Transform | Status |
|--------|---------------|-----------|--------|
| **1480** (ViacomCBS) | `REFERER_URL` | CASE grouping | 🟢 **LOCKED** |
| **2514** (MNTN) | N/A | N/A | ⛔ Not applicable (no web pixel) |
| **2234** (Magnite) | N/A | N/A | ⛔ Not applicable (no web pixel) |
| **1813** (Causal iQ) | N/A | N/A | ⛔ Not applicable (no web pixel) |

**Note:** MOD-TRF is only available for advertisers with Quorum web pixel installed.

---

## 🎨 UX MODULES (LOCKED)

### UX-STYLE (Style Guide)
```javascript
const STYLE_GUIDE = {
  colors: {
    background: '#0b1220',
    primary: '#7aa2ff',
    success: '#3ddc97',
    warning: '#ffbb44',
    danger: '#ff4444',
    textPrimary: '#e7eefc',
    textSecondary: '#a8b6d8',
    textMuted: '#606875'
  },
  // Visit Rate: 2 decimals + %
  // Numbers: toLocaleString() for commas
  // Priority: color-coded background
};
```

---

## 📋 SESSION PROTOCOL

### Starting a Session
Please state:
1. **Module:** Which module? (MOD-LIFT, MOD-CAMP, MOD-LI, MOD-CRE, MOD-CTX, MOD-GEO, MOD-TRF, UX-*)
2. **Agency/PT:** Which agency ID or PT code? (or "universal")
3. **Goal:** What specific change or exploration?

Example:
> "Let's work on **MOD-CTX** for **MNTN (2514)**. I want to explore what column we should use for context grouping."

### My Commitments
- ✅ Only modify the specified module
- ✅ Reference this document for existing configurations
- ✅ Flag if a change would affect other modules
- ✅ Update this reference when we lock new configurations
- ❌ Will NOT hallucinate or change locked modules

---

## 🔒 CHANGE LOG

| Date | Module | Agency/PT | Change | Status |
|------|--------|-----------|--------|--------|
| 2026-01-18 | MOD-CTX | 1480 | SITE column with g% filter | 🟢 LOCKED |
| 2026-01-18 | MOD-GEO | 1480 | IP→MAID→ZIP derivation | 🟢 LOCKED |
| 2026-01-18 | MOD-TRF | 1480 | REFERER_URL grouping | 🟢 LOCKED |
| 2026-01-18 | MOD-GEO | 2514 | ZIP_CODE + DMA + pop weighting | 🟢 LOCKED |
| 2026-01-18 | MOD-CAMP | 2514 | CAMPAIGN_NAME from postal table | 🟢 LOCKED |
| 2026-01-18 | UX-* | All | Layout, Style, Components | 🟢 LOCKED |

---

## 🚧 TODO / NEEDS EXPLORATION

| Priority | Module | Agency/PT | Question |
|----------|--------|-----------|----------|
| 🔴 High | MOD-CTX | 2514 (MNTN) | What column for context? PUBLISHER_CODE? |
| 🟡 Med | MOD-LIFT | All | Define control group methodology |
| 🟡 Med | MOD-LI | All | Line item performance implementation |
| 🟡 Med | MOD-CRE | All | Creative performance implementation |
| 🟢 Low | MOD-GEO | 1480 | Add DMA via ZIP_DMA_MAPPING |
| 🟢 Low | MOD-CTX | 1813 | Explore Causal iQ data structure |

---

**This document is the source of truth. Update it when configurations are locked.**
