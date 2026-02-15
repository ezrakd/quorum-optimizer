# Quorum Optimizer API

Campaign analytics and attribution API for advertising optimization.

## Quick Start

```bash
# Install dependencies
pip install flask flask-cors snowflake-connector-python

# Set environment variables
export SNOWFLAKE_ACCOUNT="FZB05958.us-east-1"
export SNOWFLAKE_USER="OPTIMIZER_SERVICE_USER"
export SNOWFLAKE_PASSWORD="your-password"
export SNOWFLAKE_WAREHOUSE="COMPUTE_WH"

# Run locally
python app.py
```

## Live API

**URL**: https://quorum-optimizer-production.up.railway.app

**Health Check**: `GET /api/health`

## Documentation

| Document | Description |
|----------|-------------|
| [API_DOCUMENTATION.md](API_DOCUMENTATION.md) | Complete API reference |
| [PROJECT_HANDOFF_v2.md](PROJECT_HANDOFF_v2.md) | Project status and next steps |
| [PT_CONFIGURATION.md](PT_CONFIGURATION.md) | Platform configuration guide |
| [DATA_DICTIONARY.md](DATA_DICTIONARY.md) | Database tables and columns |

## Key Endpoints

### Store Visits
- `GET /api/agencies` - List agencies
- `GET /api/advertisers?agency_id=X` - List advertisers
- `GET /api/advertiser-summary?advertiser_id=X` - Summary metrics
- `GET /api/zip-performance?advertiser_id=X` - ZIP-level data
- `GET /api/campaign-performance?advertiser_id=X` - Campaign breakdown
- `GET /api/publisher-performance?advertiser_id=X` - Publisher breakdown

### Web Events
- `GET /api/web/advertisers-v2` - List web advertisers
- `GET /api/web/summary?advertiser_id=X` - Web summary
- `GET /api/web/campaign-performance?advertiser_id=X` - Campaign breakdown
- `GET /api/web/publisher-performance?advertiser_id=X` - Publisher breakdown

### Configuration
- `GET /api/config/platforms` - PT configurations
- `GET /api/config/resolve?pt=X&agency_id=Y` - Resolve config

## Test Data

**Store Visits**: Advertiser 45143 (Noodles & Company)  
**Web Events**: Advertiser 37838 (Montana Knife Company)

## Attribution

All endpoints use **last-touch attribution** - only the most recent impression before a conversion receives credit.

## Deployment

Deployed on Railway with auto-deploy from GitHub:
```bash
git add . && git commit -m "Update" && git push
```

## Repository

GitHub: github.com/ezrakd/quorum-optimizer
