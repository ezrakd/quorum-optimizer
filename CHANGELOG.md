# Changelog

All notable changes to the Quorum Optimizer will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

## [v8] - 2026-01-18

### Added
- **MNTN Advertisers**: First Watch (27828), Sky Zone (37704), Northern Tool (45346), Visit PA (32623)
- **Causal iQ TTD Advertisers**: Lowes TTD (45141), Golden Nugget Casino (27588)
- Real Snowflake data for all new advertisers (publishers and ZIP codes)

### Changed
- AutoZone (8123) now uses `contexts` array instead of empty `publishers` (PT=6 TTD SITE field)
- Updated MODULE_REFERENCE.md to v1.2 with Causal iQ TTD documentation
- Updated module-config.json to v1.2 with PT=6 context sources

### Fixed
- Northern Tool had fabricated placeholder data → replaced with real Snowflake query results
- First Watch, Sky Zone, Visit PA all now have real publisher and ZIP data

### Data Sources Verified
- Northern Tool: 8 publishers (NBC, HBO Max, Peacock, etc.), 8 ZIPs
- First Watch: 8 publishers (Samsung TV+ dominant), 8 ZIPs  
- Sky Zone: 8 publishers (Tubi Entertainment leading), 8 ZIPs
- Visit PA: 8 publishers (web domains: easybrain.com, dailymotion.com), 8 ZIPs

---

## [v7] - 2026-01-17

### Added
- Sidebar collapse/expand toggle button
- Sidebar drag-to-resize functionality (240px-450px range)
- MOD-CTX (Context/Publisher optimization) module structure
- ViacomCBS Lean RX with `contexts` array (SITE field data)

### Changed
- Separated `publishers` (MNTN/Magnite PT=22) from `contexts` (ViacomCBS PT=11, Causal iQ PT=6)
- Added `noPublisherData` flag for advertisers without context data

### Technical
- Lines 17-46: Sidebar state management and resize handlers
- Lines 607-681: Sidebar UI with collapse button and resize handle

---

## [v6] - 2026-01-17

### Added
- Magnite advertisers: Mountain West Bank (40143), Visit Pensacola (39323)
- Publisher optimization module for MNTN advertisers
- Real publisher data from PUBLISHER_CODE field

---

## [v5] - 2026-01-17

### Added
- Causal iQ advertisers: Space Coast Tourism (7389), AutoZone (8123)
- Geographic (ZIP) optimization working for all agencies

### Known Issues
- Causal iQ MOD-CTX not available (QUORUM_ADVERTISER_ID not populated)

---

## [v4] - 2026-01-16

### Added
- Full MOD-GEO implementation with reallocation recommendations
- Conservative and Aggressive reallocation tiers
- CSV export and clipboard copy functions
- Color-coded reallocation priority scores

---

## [v3] - 2026-01-16

### Added
- Campaign performance table (MOD-CAMP)
- Performance index calculation (visit rate vs baseline)

---

## [v2] - 2026-01-16

### Added
- KPI summary cards (MOD-LIFT)
- Date range picker
- Agency/Advertiser sidebar navigation

---

## [v1] - 2026-01-16

### Added
- Initial React component structure
- Dark theme with glassmorphism design
- Noodles & Company demo data
- Basic tab navigation

---

## Version Naming

- **v1-v6**: Development iterations
- **v7**: First "locked" version with sidebar features
- **v8+**: Production-ready versions with real data
