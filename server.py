"""
Quorum Optimizer Server — Unified Entry Point
===============================================
Registers both the v6 optimizer API and the config API Blueprint.

Usage:
    python server.py

Environment variables:
    SNOWFLAKE_USER, SNOWFLAKE_PASSWORD, SNOWFLAKE_ACCOUNT
    SNOWFLAKE_WAREHOUSE (default: COMPUTE_WH)
    SNOWFLAKE_DATABASE (default: QUORUMDB)
    SNOWFLAKE_SCHEMA (default: SEGMENT_DATA)
    SNOWFLAKE_ROLE (default: OPTIMIZER_READONLY_ROLE)
    SNOWFLAKE_ADMIN_ROLE (default: ACCOUNTADMIN) — for config writes
    PORT (default: 8080)
"""
import os

# Import the v6 optimizer Flask app
from optimizer_api_v6 import app

# Import the config API Blueprint
from config_api import config_bp

# Register config Blueprint onto the main app
app.register_blueprint(config_bp)

# Serve the v6 frontend at root
@app.route('/')
def index():
    return app.send_static_file('optimizer_v6.html')

# Also serve at /optimizer for backward compat
@app.route('/optimizer')
def optimizer_page():
    return app.send_static_file('optimizer_v6.html')


if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8080))
    print(f"Starting Quorum Optimizer v6 on port {port}")
    print(f"  Optimizer API: /api/v6/*")
    print(f"  Config API:    /api/config/*")
    print(f"  Frontend:      /")
    app.run(host='0.0.0.0', port=port, debug=False)
