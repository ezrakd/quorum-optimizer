"""
Quorum Optimizer Server — Unified Entry Point
===============================================
Registers both the v6 optimizer API and the config API Blueprint.
Adds Clerk authentication for all routes via before_request hook.

Usage:
    python server.py

Environment variables:
    SNOWFLAKE_USER, SNOWFLAKE_PASSWORD, SNOWFLAKE_ACCOUNT
    SNOWFLAKE_WAREHOUSE (default: COMPUTE_WH)
    SNOWFLAKE_DATABASE (default: QUORUMDB)
    SNOWFLAKE_SCHEMA (default: SEGMENT_DATA)
    SNOWFLAKE_ROLE (default: OPTIMIZER_READONLY_ROLE)
    SNOWFLAKE_ADMIN_ROLE (default: ACCOUNTADMIN) — for config writes
    CLERK_PUBLISHABLE_KEY — Clerk frontend key
    CLERK_SECRET_KEY — Clerk backend key (enables auth when set)
    PORT (default: 8080)
"""
import os
from flask import request, jsonify, g

# Import the v6 optimizer Flask app
from optimizer_api_v6 import app

# Import the config API Blueprint
from config_api import config_bp

# Import auth
from auth import _check_auth, _is_auth_enabled

# Register config Blueprint onto the main app
app.register_blueprint(config_bp)


# ---------------------------------------------------------------------------
# Global before_request hook — enforces auth on all API routes
# ---------------------------------------------------------------------------
# Routes that do NOT require authentication
PUBLIC_PATHS = {'/login', '/health', '/api/auth/config'}


@app.before_request
def enforce_auth():
    """
    Check authentication before every request.
    - Public paths are exempt (login page, health check, auth config)
    - Static files are exempt (served by Flask's static handler)
    - /api/config/* requires admin role
    - /api/v6/* requires any authenticated user
    - Page routes (/, /optimizer, /admin) serve HTML (auth checked client-side)
    """
    path = request.path

    # Public paths — no auth required
    if path in PUBLIC_PATHS:
        return None

    # Static file requests — no auth (CSS, JS, images, HTML files)
    # These are served by Flask's static file handler
    if '.' in path.split('/')[-1] and not path.startswith('/api/'):
        return None

    # Page routes (/, /optimizer, /admin) — serve HTML, auth handled client-side
    if path in ('/', '/optimizer', '/admin'):
        return None

    # API routes — require authentication
    if path.startswith('/api/'):
        err = _check_auth()
        if err:
            return err

        # Config API requires admin role
        if path.startswith('/api/config/'):
            if g.user.get('role') != 'admin':
                return jsonify({'error': 'Admin access required'}), 403

    return None


# ---------------------------------------------------------------------------
# Login page (unauthenticated)
# ---------------------------------------------------------------------------
@app.route('/login')
def login_page():
    """Serve login page with Clerk publishable key injected."""
    clerk_pk = os.environ.get('CLERK_PUBLISHABLE_KEY', '')
    login_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'login.html')
    with open(login_path, 'r') as f:
        html = f.read()
    # Inject the publishable key as a JS variable
    html = html.replace(
        "|| '';",
        f"|| '{clerk_pk}';",
        1
    )
    return html


# ---------------------------------------------------------------------------
# API endpoint to provide Clerk config to the frontend
# ---------------------------------------------------------------------------
@app.route('/api/auth/config')
def auth_config():
    """Return Clerk publishable key and auth status for frontend init."""
    clerk_pk = os.environ.get('CLERK_PUBLISHABLE_KEY', '')
    return jsonify({
        'publishable_key': clerk_pk,
        'auth_enabled': bool(os.environ.get('CLERK_SECRET_KEY')),
    })


# ---------------------------------------------------------------------------
# API endpoint to return current user info (for frontend display)
# ---------------------------------------------------------------------------
@app.route('/api/auth/me')
def auth_me():
    """Return current user info if authenticated."""
    err = _check_auth()
    if err:
        return err
    return jsonify({
        'user_id': g.user.get('user_id'),
        'email': g.user.get('email'),
        'role': g.user.get('role'),
        'agency_id': g.user.get('agency_id'),
    })


# ---------------------------------------------------------------------------
# Page routes
# ---------------------------------------------------------------------------
@app.route('/')
def index():
    return app.send_static_file('optimizer_v6.html')


@app.route('/optimizer')
def optimizer_page():
    return app.send_static_file('optimizer_v6.html')


@app.route('/admin')
def admin_page():
    return app.send_static_file('config_admin.html')


if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8080))
    auth_status = "ENABLED" if os.environ.get('CLERK_SECRET_KEY') else "DISABLED (no CLERK_SECRET_KEY)"
    print(f"Starting Quorum Optimizer v6 on port {port}")
    print(f"  Auth:          {auth_status}")
    print(f"  Optimizer API: /api/v6/*")
    print(f"  Config API:    /api/config/*")
    print(f"  Frontend:      /")
    print(f"  Config Admin:  /admin")
    print(f"  Login:         /login")
    app.run(host='0.0.0.0', port=port, debug=False)
