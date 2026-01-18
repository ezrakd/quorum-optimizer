# Quorum Optimizer

Campaign optimization tool for Quorum that analyzes store visit and web event performance, providing ZIP code and publisher/context reallocation recommendations.

## 🎯 Overview

The Quorum Optimizer helps media buyers identify underperforming geographic and contextual placements, then provides actionable reallocation recommendations to improve campaign visit rates.

**Live Demo**: Runs as a React artifact in Claude.ai  
**Target Production URL**: https://www.quorum.inc/optimizer/  
**Data Source**: Snowflake (QUORUMDB.SEGMENT_DATA)

## 📊 Features

### Modules

| Module | Description | Status |
|--------|-------------|--------|
| MOD-LIFT | KPI summary cards (visit rate, lift, impressions, visits) | ✅ Active |
| MOD-CAMP | Campaign performance table with index scoring | ✅ Active |
| MOD-CTX | Context/Publisher optimization with reallocation | ✅ Active |
| MOD-GEO | ZIP code optimization with reallocation | ✅ Active |

### Supported Agencies

| Agency | ID | Platform | Context Source |
|--------|-----|----------|----------------|
| MNTN | 2514 | PT=22 | PUBLISHER_CODE (CTV) |
| Magnite | 2234 | PT=22 | PUBLISHER_CODE (CTV) |
| Causal iQ | 1813 | PT=6/8 | SITE (TTD) or GEO-only (DV360) |
| ViacomCBS WhoSay | 1480 | PT=11 | SITE (Xandr) |

## 🚀 Quick Start

### Running in Claude.ai
1. Create a new Claude.ai project
2. Connect the QUORUM_SNOWFLAKE MCP connector
3. Upload `src/quorum-optimizer.jsx` to the conversation
4. Ask Claude to render the artifact

### Local Development
```bash
# Clone the repo
git clone https://github.com/[your-org]/quorum-optimizer.git
cd quorum-optimizer

# The JSX file is a self-contained React component
# To run locally, you'll need a React environment
npx create-react-app quorum-test
cp src/quorum-optimizer.jsx quorum-test/src/App.jsx
cd quorum-test && npm start
```

## 📁 Project Structure

```
quorum-optimizer/
├── src/
│   └── quorum-optimizer.jsx    # Main React component (latest version)
├── docs/
│   ├── MODULE_REFERENCE.md     # Technical documentation
│   ├── module-config.json      # Machine-readable config
│   ├── DATA_DICTIONARY.md      # Snowflake schema reference
│   └── DEPLOYMENT_GUIDE.md     # Production deployment steps
├── queries/
│   └── example-queries.sql     # Useful Snowflake queries
├── CHANGELOG.md                # Version history
└── README.md                   # This file
```

## 📈 Data Flow

```
Ad Impression (XANDR_IMPRESSION_LOG)
    ↓ IMP_ID join
Store Visit (CAMPAIGN_PERFORMANCE_STORE_VISITS_RAW)
    ↓ Aggregate by
ZIP Code (CAMPAIGN_POSTAL_REPORTING) + Publisher/Context
    ↓ Calculate
Performance Index, Pop Weighted Delivery Index, Reallocation Priority
    ↓ Output
Optimization Recommendations (CSV/Clipboard)
```

## 🔧 Key Calculations

```javascript
// Performance Index (centered at 100)
performanceIndex = (zipVisitRate / baselineVisitRate) * 100

// Population Weighted Delivery Index (centered at 100)
popWeightedDeliveryIndex = (zipImpShare / zipPopShare) * 100

// Reallocation Priority
// Positive = over-delivering to underperformer (REALLOCATE)
// Negative = efficient delivery (KEEP)
reallocationPriority = popWeightedDeliveryIndex - performanceIndex
```

## 🛡️ Guardrails

- **Min impressions**: 3,500 per ZIP/context
- **Max reallocation**: 35% of total impressions
- **Min coverage**: 5 ZIPs per DMA
- **Unknown handling**: Keep UNKNOWN/null ZIPs in rotation

## 📝 Version History

See [CHANGELOG.md](CHANGELOG.md) for detailed version history.

| Version | Date | Highlights |
|---------|------|------------|
| v8 | 2026-01-18 | +4 MNTN advertisers, +3 Causal iQ TTD, real Snowflake data |
| v7 | 2026-01-17 | Sidebar collapse/resize, MOD-CTX structure |
| v1-v6 | 2026-01-16 | Initial development, basic features |

## 🤝 Contributing

1. Create a feature branch: `git checkout -b feature/my-feature`
2. Make changes to `src/quorum-optimizer.jsx`
3. Test in Claude.ai artifact renderer
4. Commit with descriptive message: `git commit -m "Add: new feature description"`
5. Push and create PR

## 📄 License

Proprietary - Quorum Inc.
