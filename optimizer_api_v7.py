"""
Quorum Optimizer API v7
=======================
Clean rewrite targeting PERF_BY_* dimensional fact tables.

Key changes from v6:
- Unified query strategy (no more ADM_PREFIX vs PCM_4KEY routing)
- All performance data from PERF_BY_* tables (DERIVED_TABLES)
- Store visits from HH_STORE_VISIT_ATTRIBUTION (already HH-resolved)
- Web visits from HH_WEB_VISIT_ATTRIBUTION (already HH-resolved)
- Discovery from BASE_TABLES.AGENCY_ADVERTISER_PROFILE (canonical names)
- Visit rate = (visitors * COALESCE(multiplier, 1)) / NULLIF(impressions, 0)
- Lift analysis uses HH_STORE_VISIT_ATTRIBUTION (single source, no UNION)

Table architecture (run39 + run40-43):
  PERF_BY_PUBLISHER  — publisher/supply-side performance
  PERF_BY_GEO        — geographic (zip + DMA)
  PERF_BY_TRAFFIC    — traffic source classification
  PERF_BY_CREATIVE   — creative-level performance
  PERF_BY_HOUSEHOLD  — household-level (top 1000 HH per advertiser)

Enrichment (SP_ENRICH_DIMENSIONAL_FACTS):
  VISITORS, WEB_VISITORS, VISIT_RATE are pre-computed on PERF_BY_* tables.
  For endpoints needing HH-level detail (lift analysis), query HH_* tables directly.

VISIT COUNTING METHODOLOGY (CRITICAL — READ BEFORE MODIFYING)
==============================================================
All visit metrics use LAST-TOUCH attribution with strict deduplication.
The canonical source tables are HH_WEB_VISIT_ATTRIBUTION and
HH_STORE_VISIT_ATTRIBUTION — these are pre-filtered to IS_LAST_TOUCH=TRUE
and resolved to HOUSEHOLD_ID. Do NOT use the raw multi-touch tables
(AD_TO_WEB_VISIT_ATTRIBUTION, WEB_TO_STORE_VISIT_ATTRIBUTION) for visit
counts — those contain one row per impression-per-visit and will inflate
counts by 25x+ due to impression fan-out.

  WEB VISITS:
    - Unit: 1 per unique web session (WEB_VISIT_UUID) per day
    - Attribution: last impression before visit only (IS_LAST_TOUCH=TRUE)
    - Source: HH_WEB_VISIT_ATTRIBUTION
    - Count: COUNT(DISTINCT WEB_VISIT_UUID)
    - NOT page views (one session = one visit regardless of pages viewed)
    - NOT per-impression (one session matched to many impressions = still 1 visit)

  STORE VISITS:
    - Unit: 1 per household per day per location
    - Attribution: last impression before visit only (IS_LAST_TOUCH=TRUE)
    - Target source: HH_STORE_VISIT_ATTRIBUTION (run49)
    - Target count: COUNT(DISTINCT HOUSEHOLD_ID || '|' || STORE_VISIT_DATE
                         || '|' || STORE_VISIT_SEGMENT_MD5)
    - NOT per-ping (device seen 5 times at same location same day = 1 visit)
    - NOT per-impression (one visit matched to many impressions = still 1 visit)

  VISIT RATE:
    - Formula: unique_visits / impressions * 100
    - With coverage multiplier: (unique_visits * multiplier) / impressions * 100
    - Coverage multiplier compensates for HH resolution rate (<100% match)
"""

import os
import json
import logging
from datetime import datetime, timedelta
from functools import wraps

from flask import Flask, Blueprint, request, jsonify, g, current_app
import snowflake.connector
from snowflake.connector import DictCursor

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

# Blueprint for v7 API routes — registered by server.py onto the main app
v7_bp = Blueprint("v7", __name__)

# Default config values (applied when register_v7_config is called)
V7_DEFAULTS = {
    "SNOWFLAKE_ACCOUNT": os.environ.get("SNOWFLAKE_ACCOUNT", "quorum_inc.us-east-1"),
    "SNOWFLAKE_USER": os.environ.get("SNOWFLAKE_USER", "OPTIMIZER_SVC"),
    "SNOWFLAKE_PASSWORD": os.environ.get("SNOWFLAKE_PASSWORD", ""),
    "SNOWFLAKE_WAREHOUSE": os.environ.get("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
    "SNOWFLAKE_DATABASE": os.environ.get("SNOWFLAKE_DATABASE", "QUORUMDB"),
    "SNOWFLAKE_ROLE": os.environ.get("SNOWFLAKE_ROLE", "OPTIMIZER_READONLY_ROLE"),
    "API_TOKEN": os.environ.get("OPTIMIZER_API_TOKEN", ""),
    "DEFAULT_DATE_RANGE_DAYS": 30,
    "MAX_DATE_RANGE_DAYS": 365,
    "LOG_LEVEL": os.environ.get("LOG_LEVEL", "INFO"),
}


def register_v7_config(app):
    """Apply v7 defaults to an existing Flask app (only sets missing keys)."""
    for k, v in V7_DEFAULTS.items():
        app.config.setdefault(k, v)


logging.basicConfig(
    level=getattr(logging, V7_DEFAULTS["LOG_LEVEL"]),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("optimizer_v7")

# ---------------------------------------------------------------------------
# Schema constants — single source of truth for table references
# ---------------------------------------------------------------------------

T = {
    # Dimensional fact tables (DERIVED_TABLES)
    "PERF_PUB":       "QUORUMDB.DERIVED_TABLES.PERF_BY_PUBLISHER",
    "PERF_GEO":       "QUORUMDB.DERIVED_TABLES.PERF_BY_GEO",
    "PERF_TRAFFIC":   "QUORUMDB.DERIVED_TABLES.PERF_BY_TRAFFIC",
    "PERF_CREATIVE":  "QUORUMDB.DERIVED_TABLES.PERF_BY_CREATIVE",
    "PERF_HH":        "QUORUMDB.DERIVED_TABLES.PERF_BY_HOUSEHOLD",
    "COVERAGE":       "QUORUMDB.DERIVED_TABLES.COVERAGE_MULTIPLIER_STATS",

    # Attribution tables (DERIVED_TABLES)
    "HH_STORE":       "QUORUMDB.DERIVED_TABLES.HH_STORE_VISIT_ATTRIBUTION",
    "HH_WEB":         "QUORUMDB.DERIVED_TABLES.HH_WEB_VISIT_ATTRIBUTION",
    "WEBPIXEL":       "QUORUMDB.DERIVED_TABLES.WEBPIXEL_EVENTS",

    # App/config tables
    "AGENCY_ADV":     "QUORUMDB.DERIVED_TABLES.AGENCY_ADVERTISER_ACTIVE",
    "AAP":            "QUORUMDB.BASE_TABLES.AGENCY_ADVERTISER_PROFILE",
    "REPORT_LAYOUT":  "QUORUMDB.APP_DB.REPORT_LAYOUT_SETTING",
    "CAMPAIGN":       "QUORUMDB.APP_DB.CAMPAIGN",
    "LINE_ITEM":      "QUORUMDB.APP_DB.LINE_ITEM",
    "DELIVERY_OPT":   "QUORUMDB.APP_DB.DELIVERY_OPTION",
    "ADV_PIXEL":      "QUORUMDB.APP_DB.ADVERTISER_PIXEL_STATS",
    "UNIVERSAL_PX":   "QUORUMDB.APP_DB.UNIVERSAL_PIXEL",

    # Reference tables
    "REF_ADV_CFG":    "QUORUMDB.BASE_TABLES.REF_ADVERTISER_CONFIG",
    "REF_DSP":        "QUORUMDB.BASE_TABLES.REF_DSP_PLATFORMS",
    "REF_COL_DEF":    "QUORUMDB.BASE_TABLES.REF_COLUMN_DEFINITIONS",
    "PCM":            "QUORUMDB.REF_DATA.PIXEL_CAMPAIGN_MAPPING_V2",
    "ZIP_DMA":        "QUORUMDB.REF_DATA.ZIP_DMA_MAPPING",

    # Household core
    "IP_HH":          "QUORUMDB.HOUSEHOLD_CORE.IP_HOUSEHOLD_LOOKUP",
    "MAID_HH":        "QUORUMDB.HOUSEHOLD_CORE.MAID_HOUSEHOLD_LOOKUP",

    # Impression log (for lift analysis only)
    "IMP_LOG":        "QUORUMDB.BASE_TABLES.AD_IMPRESSION_LOG_V2",
}

# ---------------------------------------------------------------------------
# Agency name resolution
# Uses AGENCY_ADVERTISER_PROFILE (BASE_TABLES) for dynamic name lookup.
# COMP_NAME in AGENCY_ADVERTISER shows dealership names, NOT agency names.
# AAP.AGENCY_NAME is the canonical source for agency display names.
# ---------------------------------------------------------------------------

# Per-request cache for agency name map (populated on first call per request)
def get_agency_names():
    """Fetch agency name map from AGENCY_ADVERTISER_PROFILE.

    Returns dict of {agency_id: agency_name}.
    Cached on Flask g object for the duration of the request.
    """
    cached = getattr(g, "_agency_names", None)
    if cached is not None:
        return cached

    rows = execute_query(
        f"""
        SELECT DISTINCT AGENCY_ID, AGENCY_NAME
        FROM {T['AAP']}
        WHERE AGENCY_NAME IS NOT NULL
        """,
    )
    name_map = {safe_int(r["AGENCY_ID"]): r["AGENCY_NAME"] for r in rows}
    g._agency_names = name_map
    return name_map


def resolve_agency_name(agency_id):
    """Resolve an agency ID to its display name."""
    names = get_agency_names()
    return names.get(agency_id, f"Agency {agency_id}")


# ---------------------------------------------------------------------------
# Snowflake Connection
# ---------------------------------------------------------------------------

def get_snowflake_conn():
    """Get or reuse a Snowflake connection for the current request.

    Uses v7-specific key 'sf_conn_v7' to avoid colliding with v6's connection.
    Reads config from current_app (the main Flask app registered by server.py).
    Includes retry logic for SSL certificate errors (common on Heroku/Railway).
    """
    if "sf_conn_v7" not in g:
        cfg = current_app.config
        retries = 3
        last_err = None
        for attempt in range(retries):
            try:
                g.sf_conn_v7 = snowflake.connector.connect(
                    account=cfg.get("SNOWFLAKE_ACCOUNT", V7_DEFAULTS["SNOWFLAKE_ACCOUNT"]),
                    user=cfg.get("SNOWFLAKE_USER", V7_DEFAULTS["SNOWFLAKE_USER"]),
                    password=cfg.get("SNOWFLAKE_PASSWORD", V7_DEFAULTS["SNOWFLAKE_PASSWORD"]),
                    warehouse=cfg.get("SNOWFLAKE_WAREHOUSE", V7_DEFAULTS["SNOWFLAKE_WAREHOUSE"]),
                    database=cfg.get("SNOWFLAKE_DATABASE", V7_DEFAULTS["SNOWFLAKE_DATABASE"]),
                    role=cfg.get("SNOWFLAKE_ROLE", V7_DEFAULTS["SNOWFLAKE_ROLE"]),
                    insecure_mode=True,
                    session_parameters={"QUERY_TAG": "optimizer_v7"},
                )
                break
            except Exception as e:
                last_err = e
                if attempt < retries - 1 and ('certificate' in str(e).lower() or '254007' in str(e)):
                    logger.warning(f"Snowflake connection attempt {attempt + 1} failed (cert), retrying: {e}")
                    continue
                raise
    return g.sf_conn_v7


@v7_bp.teardown_app_request
def close_snowflake_conn_v7(exception):
    conn = g.pop("sf_conn_v7", None)
    if conn is not None:
        try:
            conn.close()
        except Exception:
            pass


def execute_query(sql, params=None, fetch="all"):
    """Execute a Snowflake query and return results as list of dicts.

    Args:
        sql: SQL string with %(name)s-style parameters
        params: dict of parameter values
        fetch: "all" returns list[dict], "one" returns dict|None, "none" returns rowcount
    """
    conn = get_snowflake_conn()
    cur = conn.cursor(DictCursor)
    try:
        cur.execute(sql, params or {})
        if fetch == "all":
            return cur.fetchall()
        elif fetch == "one":
            return cur.fetchone()
        else:
            return cur.rowcount
    finally:
        cur.close()


# ---------------------------------------------------------------------------
# Auth Middleware
# ---------------------------------------------------------------------------

def require_auth(f):
    """Validate Bearer token or API key."""
    @wraps(f)
    def decorated(*args, **kwargs):
        token = current_app.config.get("API_TOKEN", "")
        if not token:
            # No token configured = auth disabled (dev mode)
            return f(*args, **kwargs)

        auth_header = request.headers.get("Authorization", "")
        api_key = request.args.get("api_key", "")

        if auth_header.startswith("Bearer "):
            provided = auth_header[7:]
        elif api_key:
            provided = api_key
        else:
            return jsonify({"error": "Missing authentication"}), 401

        if provided != token:
            return jsonify({"error": "Invalid authentication"}), 403

        return f(*args, **kwargs)
    return decorated


# ---------------------------------------------------------------------------
# Utility Functions
# ---------------------------------------------------------------------------

def parse_date_range():
    """Extract start_date and end_date from request args.

    Accepts: start_date, end_date (YYYY-MM-DD)
    Defaults: last 30 days
    """
    end_str = request.args.get("end_date")
    start_str = request.args.get("start_date")

    if end_str:
        try:
            end_date = datetime.strptime(end_str, "%Y-%m-%d").date()
        except ValueError:
            end_date = datetime.utcnow().date()
    else:
        end_date = datetime.utcnow().date()

    if start_str:
        try:
            start_date = datetime.strptime(start_str, "%Y-%m-%d").date()
        except ValueError:
            start_date = end_date - timedelta(days=current_app.config.get("DEFAULT_DATE_RANGE_DAYS", 30))
    else:
        start_date = end_date - timedelta(days=current_app.config.get("DEFAULT_DATE_RANGE_DAYS", 30))

    # Clamp to max range
    max_days = current_app.config.get("MAX_DATE_RANGE_DAYS", 365)
    if (end_date - start_date).days > max_days:
        start_date = end_date - timedelta(days=max_days)

    return start_date, end_date


def get_agency_id():
    """Extract agency_id from request args. Required for most endpoints."""
    val = request.args.get("agency_id")
    if val is None:
        return None
    try:
        return int(val)
    except (ValueError, TypeError):
        return None


def get_advertiser_id():
    """Extract advertiser_id from request args."""
    val = request.args.get("advertiser_id")
    if val is None:
        return None
    try:
        return int(val)
    except (ValueError, TypeError):
        return None


_coverage_cache = {}


def get_coverage_multiplier(advertiser_id, grain=None, grain_id=None):
    """Get coverage multiplier with Wilson score 95% CI significance.

    Queries COVERAGE_MULTIPLIER_STATS at specified grain level with
    automatic fallback: LINE_ITEM → CAMPAIGN → ADVERTISER → 1.0.

    Args:
        advertiser_id: Advertiser ID.
        grain: 'LINE_ITEM', 'CAMPAIGN', or 'ADVERTISER' (default: ADVERTISER).
        grain_id: LI_ID or IO_ID depending on grain.

    Returns:
        dict with: multiplier, is_significant, ci_lower, ci_upper,
                   sample_size, grain, hh_resolution_rate
    """
    cache_key = f"{advertiser_id}_{grain}_{grain_id}"
    if cache_key in _coverage_cache:
        return _coverage_cache[cache_key]

    default = {
        "multiplier": 1.0,
        "is_significant": False,
        "ci_lower": 1.0,
        "ci_upper": 1.0,
        "sample_size": 0,
        "grain": "DEFAULT",
        "hh_resolution_rate": 0.0,
    }

    # Build ordered query attempts based on grain
    attempts = []
    if grain == "LINE_ITEM" and grain_id:
        attempts.append(("LINE_ITEM", f"AND GRAIN = 'LINE_ITEM' AND LI_ID = %(gid)s"))
    if grain in ("LINE_ITEM", "CAMPAIGN") and grain_id:
        attempts.append(("CAMPAIGN", f"AND GRAIN = 'CAMPAIGN' AND IO_ID = %(gid)s"))
    attempts.append(("ADVERTISER", "AND GRAIN = 'ADVERTISER'"))

    try:
        for attempt_grain, clause in attempts:
            row = execute_query(
                f"""
                SELECT COVERAGE_MULTIPLIER, IS_SIGNIFICANT,
                       MULTIPLIER_CI_LOWER, MULTIPLIER_CI_UPPER,
                       SAMPLE_SIZE, GRAIN, HH_RESOLUTION_RATE
                FROM {T['COVERAGE']}
                WHERE ADVERTISER_ID = %(adv_id)s {clause}
                LIMIT 1
                """,
                {"adv_id": advertiser_id, "gid": grain_id},
                fetch="one",
            )
            if row:
                result = {
                    "multiplier": safe_float(row.get("COVERAGE_MULTIPLIER"), 1.0),
                    "is_significant": bool(row.get("IS_SIGNIFICANT")),
                    "ci_lower": safe_float(row.get("MULTIPLIER_CI_LOWER"), 1.0),
                    "ci_upper": safe_float(row.get("MULTIPLIER_CI_UPPER"), 1.0),
                    "sample_size": safe_int(row.get("SAMPLE_SIZE")) or 0,
                    "grain": row.get("GRAIN", attempt_grain),
                    "hh_resolution_rate": safe_float(row.get("HH_RESOLUTION_RATE"), 0.0),
                }
                _coverage_cache[cache_key] = result
                return result
    except Exception as e:
        logger.warning("Coverage multiplier lookup failed for %s: %s", advertiser_id, e)

    _coverage_cache[cache_key] = default
    return default


def safe_visit_rate(visitors, impressions, multiplier=1.0):
    """Canonical visit rate formula.

    visit_rate = (visitors * multiplier) / impressions
    """
    if not impressions or impressions == 0:
        return 0.0
    return round((visitors * (multiplier or 1.0)) / impressions, 8)


def safe_float(val, default=0.0):
    """Safely convert a value to float."""
    if val is None:
        return default
    try:
        return float(val)
    except (ValueError, TypeError):
        return default


def safe_int(val, default=0):
    """Safely convert a value to int."""
    if val is None:
        return default
    try:
        return int(val)
    except (ValueError, TypeError):
        return default


def api_error(message, status_code=400):
    """Return a standardized error response."""
    return jsonify({"success": False, "error": message}), status_code


@v7_bp.errorhandler(Exception)
def handle_v7_exception(e):
    """Catch unhandled exceptions in v7 endpoints and return JSON."""
    logger.error(f"Unhandled v7 error: {type(e).__name__}: {e}")
    return jsonify({
        "success": False,
        "error": f"{type(e).__name__}: {str(e)}"
    }), 500


@v7_bp.errorhandler(500)
def handle_v7_500(e):
    """Catch 500 errors in v7 endpoints and return JSON."""
    logger.error(f"v7 500 error: {e}")
    return jsonify({
        "success": False,
        "error": f"Internal server error: {str(e)}"
    }), 500


def v6_response(data):
    """Wrap response in v6-compatible {success, data} envelope.

    The v6 HTML frontend expects:
    - All responses wrapped in {"success": true, "data": <payload>}
    - UPPERCASE field names (Snowflake column convention)
    - List endpoints: data is a flat array
    - Object endpoints: data is a flat object
    """
    return jsonify({"success": True, "data": data})


# ---------------------------------------------------------------------------
# Advertiser Config (from REF_ADVERTISER_CONFIG)
# ---------------------------------------------------------------------------
# Config flags drive which attribution types are queried per advertiser.
# This avoids expensive HH_STORE / HH_WEB queries for advertisers that
# don't have the corresponding attribution set up.
# ---------------------------------------------------------------------------

def get_advertiser_config(advertiser_id):
    """Fetch advertiser config flags from REF_ADVERTISER_CONFIG.

    Returns a dict with normalized boolean/string fields.
    Falls back to safe defaults (all False) if no config row exists.
    Results are cached on Flask's g object for the duration of the request.
    """
    cache_key = f"_adv_config_{advertiser_id}"
    cached = getattr(g, cache_key, None)
    if cached is not None:
        return cached

    row = execute_query(
        f"""
        SELECT
            HAS_STORE_VISIT_ATTRIBUTION,
            HAS_WEB_VISIT_ATTRIBUTION,
            HAS_IMPRESSION_TRACKING,
            ATTRIBUTION_WINDOW_DAYS,
            MATCH_STRATEGY,
            CONFIG_STATUS,
            WEB_VISIT_SOURCE,
            ADVERTISER_DISPLAY_NAME,
            POI_URL_COUNT,
            WEB_PIXEL_URL_COUNT
        FROM {T['REF_ADV_CFG']}
        WHERE ADVERTISER_ID = %(adv_id)s
        """,
        {"adv_id": advertiser_id},
        fetch="one",
    )

    if row:
        config = {
            "has_store": row.get("HAS_STORE_VISIT_ATTRIBUTION") is True
                         or str(row.get("HAS_STORE_VISIT_ATTRIBUTION", "")).lower() == "true",
            "has_web": row.get("HAS_WEB_VISIT_ATTRIBUTION") is True
                       or str(row.get("HAS_WEB_VISIT_ATTRIBUTION", "")).lower() == "true",
            "has_impressions": row.get("HAS_IMPRESSION_TRACKING") is True
                               or str(row.get("HAS_IMPRESSION_TRACKING", "")).lower() == "true",
            "attribution_window": safe_int(row.get("ATTRIBUTION_WINDOW_DAYS"), default=14),
            "match_strategy": row.get("MATCH_STRATEGY") or "MAID_PRIORITY",
            "config_status": row.get("CONFIG_STATUS") or "UNKNOWN",
            "web_visit_source": row.get("WEB_VISIT_SOURCE") or "",
            "display_name": row.get("ADVERTISER_DISPLAY_NAME") or "",
            "poi_count": safe_int(row.get("POI_URL_COUNT")),
            "web_pixel_count": safe_int(row.get("WEB_PIXEL_URL_COUNT")),
            "config_found": True,
        }
    else:
        config = {
            "has_store": False,
            "has_web": False,
            "has_impressions": False,
            "attribution_window": 14,
            "match_strategy": "MAID_PRIORITY",
            "config_status": "NOT_CONFIGURED",
            "web_visit_source": "",
            "display_name": "",
            "poi_count": 0,
            "web_pixel_count": 0,
            "config_found": False,
        }

    setattr(g, cache_key, config)
    return config


# ---------------------------------------------------------------------------
# Store Visit Enrichment (from HH_STORE_VISIT_ATTRIBUTION)
# ---------------------------------------------------------------------------
# These functions query the HH-resolved attribution table directly.
# Used when PERF_BY_* pre-computed VISITORS aren't sufficient
# (e.g., summary totals, per-brand breakdown, lift analysis).
# ---------------------------------------------------------------------------

def get_store_visits_total(advertiser_id, start_date, end_date):
    """Total store visit count for an advertiser (last-touch attribution)."""
    row = execute_query(
        f"""
        SELECT COUNT(*) AS total_visits,
               COUNT(DISTINCT HOUSEHOLD_ID) AS unique_hh
        FROM {T['HH_STORE']}
        WHERE AD_IMPRESSION_ADVERTISER_ID = %(adv_id)s
          AND IS_LAST_TOUCH = TRUE
          AND STORE_VISIT_DATE BETWEEN %(start)s AND %(end)s
        """,
        {"adv_id": advertiser_id, "start": str(start_date), "end": str(end_date)},
        fetch="one",
    )
    return {
        "total_visits": safe_int(row.get("TOTAL_VISITS")) if row else 0,
        "unique_households": safe_int(row.get("UNIQUE_HH")) if row else 0,
    }


def get_store_visits_by_campaign(advertiser_id, start_date, end_date):
    """Store visits grouped by insertion order (campaign)."""
    rows = execute_query(
        f"""
        SELECT INSERTION_ORDER_ID AS io_id,
               CAMPAIGN_NAME AS io_name,
               COUNT(*) AS visits,
               COUNT(DISTINCT HOUSEHOLD_ID) AS unique_hh
        FROM {T['HH_STORE']}
        WHERE AD_IMPRESSION_ADVERTISER_ID = %(adv_id)s
          AND IS_LAST_TOUCH = TRUE
          AND STORE_VISIT_DATE BETWEEN %(start)s AND %(end)s
        GROUP BY 1, 2
        """,
        {"adv_id": advertiser_id, "start": str(start_date), "end": str(end_date)},
    )
    return {str(r["IO_ID"]): {"visits": r["VISITS"], "unique_hh": r["UNIQUE_HH"],
                               "name": r["IO_NAME"]} for r in rows}


def get_store_visits_by_lineitem(advertiser_id, start_date, end_date):
    """Store visits grouped by line item."""
    rows = execute_query(
        f"""
        SELECT LINE_ITEM_ID AS li_id,
               LINE_ITEM_NAME AS li_name,
               COUNT(*) AS visits,
               COUNT(DISTINCT HOUSEHOLD_ID) AS unique_hh
        FROM {T['HH_STORE']}
        WHERE AD_IMPRESSION_ADVERTISER_ID = %(adv_id)s
          AND IS_LAST_TOUCH = TRUE
          AND STORE_VISIT_DATE BETWEEN %(start)s AND %(end)s
        GROUP BY 1, 2
        """,
        {"adv_id": advertiser_id, "start": str(start_date), "end": str(end_date)},
    )
    return {str(r["LI_ID"]): {"visits": r["VISITS"], "unique_hh": r["UNIQUE_HH"],
                               "name": r["LI_NAME"]} for r in rows}


def get_store_visits_by_creative(advertiser_id, start_date, end_date):
    """Store visits grouped by creative + IO (to avoid duplication when same creative spans IOs)."""
    rows = execute_query(
        f"""
        SELECT CREATIVE_ID,
               CREATIVE_NAME,
               INSERTION_ORDER_ID AS IO_ID,
               COUNT(*) AS visits,
               COUNT(DISTINCT HOUSEHOLD_ID) AS unique_hh
        FROM {T['HH_STORE']}
        WHERE AD_IMPRESSION_ADVERTISER_ID = %(adv_id)s
          AND IS_LAST_TOUCH = TRUE
          AND STORE_VISIT_DATE BETWEEN %(start)s AND %(end)s
        GROUP BY 1, 2, 3
        """,
        {"adv_id": advertiser_id, "start": str(start_date), "end": str(end_date)},
    )
    # Keyed by (creative_id, io_id) tuple for precise lookup
    result = {}
    for r in rows:
        key = (str(r["CREATIVE_ID"]), str(r.get("IO_ID", "")))
        result[key] = {"visits": r["VISITS"], "unique_hh": r["UNIQUE_HH"],
                        "name": r["CREATIVE_NAME"]}
    return result


def get_store_visits_by_date(advertiser_id, start_date, end_date):
    """Store visits grouped by date (for timeseries)."""
    rows = execute_query(
        f"""
        SELECT STORE_VISIT_DATE AS visit_date,
               COUNT(*) AS visits,
               COUNT(DISTINCT HOUSEHOLD_ID) AS unique_hh
        FROM {T['HH_STORE']}
        WHERE AD_IMPRESSION_ADVERTISER_ID = %(adv_id)s
          AND IS_LAST_TOUCH = TRUE
          AND STORE_VISIT_DATE BETWEEN %(start)s AND %(end)s
        GROUP BY 1
        ORDER BY 1
        """,
        {"adv_id": advertiser_id, "start": str(start_date), "end": str(end_date)},
    )
    return {str(r["VISIT_DATE"]): {"visits": r["VISITS"], "unique_hh": r["UNIQUE_HH"]}
            for r in rows}


def get_store_visits_by_brand(advertiser_id, start_date, end_date):
    """Store visits grouped by brand/category (for store visit detail view)."""
    rows = execute_query(
        f"""
        SELECT STORE_VISIT_BRAND AS brand,
               STORE_VISIT_CATEGORY AS category,
               COUNT(*) AS visits,
               COUNT(DISTINCT HOUSEHOLD_ID) AS unique_hh,
               AVG(DAYS_TO_VISIT) AS avg_days_to_visit
        FROM {T['HH_STORE']}
        WHERE AD_IMPRESSION_ADVERTISER_ID = %(adv_id)s
          AND IS_LAST_TOUCH = TRUE
          AND STORE_VISIT_DATE BETWEEN %(start)s AND %(end)s
        GROUP BY 1, 2
        ORDER BY visits DESC
        """,
        {"adv_id": advertiser_id, "start": str(start_date), "end": str(end_date)},
    )
    return [{"brand": r["BRAND"], "category": r["CATEGORY"],
             "visits": r["VISITS"], "unique_hh": r["UNIQUE_HH"],
             "avg_days_to_visit": safe_float(r.get("AVG_DAYS_TO_VISIT"))}
            for r in rows]


# ---------------------------------------------------------------------------
# Web Visit Enrichment (from HH_WEB_VISIT_ATTRIBUTION)
# ---------------------------------------------------------------------------

def get_web_visits_total(advertiser_id, start_date, end_date):
    """Total web visit count for an advertiser.
    Last-touch only, 1 per unique web session (WEB_VISIT_UUID)."""
    row = execute_query(
        f"""
        SELECT COUNT(DISTINCT WEB_VISIT_UUID) AS total_visits,
               COUNT(DISTINCT HOUSEHOLD_ID) AS unique_hh
        FROM {T['HH_WEB']}
        WHERE AD_IMPRESSION_ADVERTISER_ID = %(adv_id)s
          AND WEB_VISIT_DATE BETWEEN %(start)s AND %(end)s
        """,
        {"adv_id": advertiser_id, "start": str(start_date), "end": str(end_date)},
        fetch="one",
    )
    return {
        "total_visits": safe_int(row.get("TOTAL_VISITS")) if row else 0,
        "unique_households": safe_int(row.get("UNIQUE_HH")) if row else 0,
    }


def get_web_visits_by_campaign(advertiser_id, start_date, end_date):
    """Web visits grouped by insertion order (campaign)."""
    rows = execute_query(
        f"""
        SELECT INSERTION_ORDER_ID AS io_id,
               CAMPAIGN_NAME AS io_name,
               COUNT(*) AS visits,
               COUNT(DISTINCT HOUSEHOLD_ID) AS unique_hh
        FROM {T['HH_WEB']}
        WHERE AD_IMPRESSION_ADVERTISER_ID = %(adv_id)s
          AND IS_LAST_TOUCH = TRUE
          AND WEB_VISIT_DATE BETWEEN %(start)s AND %(end)s
        GROUP BY 1, 2
        """,
        {"adv_id": advertiser_id, "start": str(start_date), "end": str(end_date)},
    )
    return {str(r["IO_ID"]): {"visits": r["VISITS"], "unique_hh": r["UNIQUE_HH"],
                               "name": r["IO_NAME"]} for r in rows}


def get_web_visits_by_lineitem(advertiser_id, start_date, end_date):
    """Web visits grouped by line item."""
    rows = execute_query(
        f"""
        SELECT LINE_ITEM_ID AS li_id,
               LINE_ITEM_NAME AS li_name,
               COUNT(*) AS visits,
               COUNT(DISTINCT HOUSEHOLD_ID) AS unique_hh
        FROM {T['HH_WEB']}
        WHERE AD_IMPRESSION_ADVERTISER_ID = %(adv_id)s
          AND IS_LAST_TOUCH = TRUE
          AND WEB_VISIT_DATE BETWEEN %(start)s AND %(end)s
        GROUP BY 1, 2
        """,
        {"adv_id": advertiser_id, "start": str(start_date), "end": str(end_date)},
    )
    return {str(r["LI_ID"]): {"visits": r["VISITS"], "unique_hh": r["UNIQUE_HH"],
                               "name": r["LI_NAME"]} for r in rows}


def get_web_visits_by_creative(advertiser_id, start_date, end_date):
    """Web visits grouped by creative + IO (to avoid duplication when same creative spans IOs)."""
    rows = execute_query(
        f"""
        SELECT CREATIVE_ID,
               CREATIVE_NAME,
               INSERTION_ORDER_ID AS IO_ID,
               COUNT(*) AS visits,
               COUNT(DISTINCT HOUSEHOLD_ID) AS unique_hh
        FROM {T['HH_WEB']}
        WHERE AD_IMPRESSION_ADVERTISER_ID = %(adv_id)s
          AND IS_LAST_TOUCH = TRUE
          AND WEB_VISIT_DATE BETWEEN %(start)s AND %(end)s
        GROUP BY 1, 2, 3
        """,
        {"adv_id": advertiser_id, "start": str(start_date), "end": str(end_date)},
    )
    result = {}
    for r in rows:
        key = (str(r["CREATIVE_ID"]), str(r.get("IO_ID", "")))
        result[key] = {"visits": r["VISITS"], "unique_hh": r["UNIQUE_HH"],
                        "name": r["CREATIVE_NAME"]}
    return result


def get_web_visits_by_date(advertiser_id, start_date, end_date):
    """Web visits grouped by date (for timeseries)."""
    rows = execute_query(
        f"""
        SELECT WEB_VISIT_DATE AS visit_date,
               COUNT(*) AS visits,
               COUNT(DISTINCT HOUSEHOLD_ID) AS unique_hh
        FROM {T['HH_WEB']}
        WHERE AD_IMPRESSION_ADVERTISER_ID = %(adv_id)s
          AND IS_LAST_TOUCH = TRUE
          AND WEB_VISIT_DATE BETWEEN %(start)s AND %(end)s
        GROUP BY 1
        ORDER BY 1
        """,
        {"adv_id": advertiser_id, "start": str(start_date), "end": str(end_date)},
    )
    return {str(r["VISIT_DATE"]): {"visits": r["VISITS"], "unique_hh": r["UNIQUE_HH"]}
            for r in rows}


# =========================================================================
#  ENDPOINTS
# =========================================================================

# ---------------------------------------------------------------------------
# Visit Enrichment — LAST-TOUCH, DEDUPLICATED
# See VISIT COUNTING METHODOLOGY in module docstring for rules.
# Source tables: HH_WEB_VISIT_ATTRIBUTION + HH_STORE_VISIT_ATTRIBUTION
# These tables are pre-filtered to IS_LAST_TOUCH=TRUE.
# DO NOT use AD_TO_WEB_VISIT_ATTRIBUTION or WEB_TO_STORE_VISIT_ATTRIBUTION
# for visit counts — those are raw multi-touch and inflate 25x+.
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# Health Check
# ---------------------------------------------------------------------------

@v7_bp.route("/api/v7/health", methods=["GET"])
def health():
    """Basic health check — verifies Snowflake connectivity."""
    try:
        row = execute_query("SELECT CURRENT_TIMESTAMP() AS ts", fetch="one")
        return jsonify({
            "status": "healthy",
            "version": "v7",
            "snowflake_ts": str(row["TS"]) if row else None,
        })
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        return jsonify({"status": "unhealthy", "error": str(e)}), 503


# ---------------------------------------------------------------------------
# Discovery Endpoints
# ---------------------------------------------------------------------------

@v7_bp.route("/api/v7/agencies", methods=["GET"])
@require_auth
def agencies():
    """List all agencies with active advertisers.

    AGENCY_ID comes from DSP impression data (not AGENCY_ADVERTISER.ID).
    We get the definitive list from PERF_BY_PUBLISHER and resolve names
    dynamically from AGENCY_ADVERTISER_PROFILE (BASE_TABLES).
    """
    start_date, end_date = parse_date_range()

    rows = execute_query(
        f"""
        SELECT
            AGENCY_ID,
            COUNT(DISTINCT ADVERTISER_ID) AS advertiser_count,
            SUM(IMPRESSIONS) AS impressions,
            SUM(COALESCE(VISITORS, 0)) AS store_visits,
            SUM(COALESCE(WEB_VISITORS, 0)) AS web_visits,
            MIN(LOG_DATE) AS min_date,
            MAX(LOG_DATE) AS max_date
        FROM {T['PERF_PUB']}
        WHERE LOG_DATE BETWEEN %(start)s AND %(end)s
        GROUP BY AGENCY_ID
        ORDER BY impressions DESC
        """,
        {"start": str(start_date), "end": str(end_date)},
    )

    result = []
    for r in rows:
        aid = safe_int(r.get("AGENCY_ID"))
        imps = safe_int(r.get("IMPRESSIONS"))
        sv = safe_int(r.get("STORE_VISITS"))
        wv = safe_int(r.get("WEB_VISITS"))
        sv_rate = round(sv / imps * 100, 4) if imps > 0 else 0
        wv_rate = round(wv / imps * 100, 4) if imps > 0 else 0
        result.append({
            "AGENCY_ID": aid,
            "AGENCY_NAME": resolve_agency_name(aid),
            "ADVERTISER_COUNT": safe_int(r.get("ADVERTISER_COUNT")),
            "IMPRESSIONS": imps,
            "STORE_VISITS": sv,
            "STORE_VISIT_RATE": sv_rate,
            "WEB_VISITS": wv,
            "WEB_VISIT_RATE": wv_rate,
            "MIN_DATE": str(r.get("MIN_DATE", "")),
            "MAX_DATE": str(r.get("MAX_DATE", "")),
        })

    return v6_response(result)


@v7_bp.route("/api/v7/advertisers", methods=["GET"])
@require_auth
def advertisers():
    """List advertisers for an agency, enriched with impression counts.

    Query params: agency_id (required), start_date, end_date
    """
    agency_id = get_agency_id()
    if agency_id is None:
        return api_error("agency_id is required")

    start_date, end_date = parse_date_range()

    # Get advertisers from PERF_BY_PUBLISHER (definitive source for active advertisers)
    # then LEFT JOIN to AGENCY_ADVERTISER for config details
    rows = execute_query(
        f"""
        WITH perf AS (
            SELECT
                ADVERTISER_ID,
                SUM(IMPRESSIONS) AS impressions,
                SUM(COALESCE(VISITORS, 0)) AS store_visits,
                SUM(COALESCE(WEB_VISITORS, 0)) AS web_visits,
                MIN(LOG_DATE) AS min_date,
                MAX(LOG_DATE) AS max_date
            FROM {T['PERF_PUB']}
            WHERE AGENCY_ID = %(agency_id)s
              AND LOG_DATE BETWEEN %(start)s AND %(end)s
            GROUP BY ADVERTISER_ID
        )
        SELECT
            p.ADVERTISER_ID,
            REGEXP_REPLACE(aap.ADVERTISER_NAME, '^[^ ]+ - ', '') AS ADVERTISER_NAME,
            aa.REPORT_STATUS,
            aa.PIXEL_ID,
            aa.STORE_VISIT_ATTR_WINDOW,
            aa.ACCOUNT_MANAGER_NAME,
            p.impressions,
            p.store_visits,
            p.web_visits,
            p.min_date,
            p.max_date,
            cfg.HAS_STORE_VISIT_ATTRIBUTION,
            cfg.HAS_WEB_VISIT_ATTRIBUTION,
            cfg.CONFIG_STATUS,
            cfg.ADVERTISER_DISPLAY_NAME
        FROM perf p
        LEFT JOIN {T['AAP']} aap ON p.ADVERTISER_ID = aap.ADVERTISER_ID
        LEFT JOIN {T['AGENCY_ADV']} aa ON p.ADVERTISER_ID = aa.ID
        LEFT JOIN {T['REF_ADV_CFG']} cfg ON p.ADVERTISER_ID = cfg.ADVERTISER_ID
        ORDER BY p.impressions DESC
        """,
        {"agency_id": agency_id, "start": str(start_date), "end": str(end_date)},
    )

    result = []
    for r in rows:
        adv_id = safe_int(r.get("ADVERTISER_ID"))
        imps = safe_int(r.get("IMPRESSIONS"))
        sv = safe_int(r.get("STORE_VISITS"))
        wv = safe_int(r.get("WEB_VISITS"))
        sv_rate = round(sv / imps * 100, 4) if imps > 0 else 0
        wv_rate = round(wv / imps * 100, 4) if imps > 0 else 0

        # Config flags from REF_ADVERTISER_CONFIG
        has_store = (r.get("HAS_STORE_VISIT_ATTRIBUTION") is True
                     or str(r.get("HAS_STORE_VISIT_ATTRIBUTION", "")).lower() == "true")
        has_web = (r.get("HAS_WEB_VISIT_ATTRIBUTION") is True
                   or str(r.get("HAS_WEB_VISIT_ATTRIBUTION", "")).lower() == "true")

        # Use config display name if available, fall back to AAP.ADVERTISER_NAME
        display_name = r.get("ADVERTISER_DISPLAY_NAME") or r.get("ADVERTISER_NAME") or f"Advertiser {adv_id}"

        result.append({
            "ADVERTISER_ID": adv_id,
            "ADVERTISER_NAME": display_name,
            "REPORT_STATUS": r.get("REPORT_STATUS"),
            "PIXEL_ID": safe_int(r.get("PIXEL_ID")),
            "STORE_VISIT_ATTR_WINDOW": safe_int(r.get("STORE_VISIT_ATTR_WINDOW"), default=14),
            "ACCOUNT_MANAGER": r.get("ACCOUNT_MANAGER_NAME"),
            "IMPRESSIONS": imps,
            "STORE_VISITS": sv,
            "STORE_VISIT_RATE": sv_rate,
            "WEB_VISITS": wv,
            "WEB_VISIT_RATE": wv_rate,
            "MIN_DATE": str(r.get("MIN_DATE", "")),
            "MAX_DATE": str(r.get("MAX_DATE", "")),
            "HAS_STORE_ATTRIBUTION": has_store,
            "HAS_WEB_ATTRIBUTION": has_web,
            "CONFIG_STATUS": r.get("CONFIG_STATUS") or "NOT_CONFIGURED",
        })

    return v6_response(result)


@v7_bp.route("/api/v7/tab-availability", methods=["GET"])
@require_auth
def tab_availability():
    """Check which report tabs have data for a given advertiser.

    Returns boolean flags for: impressions, store_visits, web_visits,
    publisher, geo, creative, traffic, household, lift.
    """
    advertiser_id = get_advertiser_id()
    if advertiser_id is None:
        return api_error("advertiser_id is required")

    start_date, end_date = parse_date_range()
    params = {"adv_id": advertiser_id, "start": str(start_date), "end": str(end_date)}

    # Config flags determine store/web attribution capability
    cfg = get_advertiser_config(advertiser_id)

    # Check PERF_BY_* dimension availability (still query-based — these are fast)
    checks = execute_query(
        f"""
        SELECT
            (SELECT COUNT(*) FROM {T['PERF_PUB']}
             WHERE ADVERTISER_ID = %(adv_id)s AND LOG_DATE BETWEEN %(start)s AND %(end)s
             LIMIT 1) AS has_impressions,
            (SELECT COUNT(*) FROM {T['PERF_PUB']}
             WHERE ADVERTISER_ID = %(adv_id)s AND LOG_DATE BETWEEN %(start)s AND %(end)s
               AND PUBLISHER != ''
             LIMIT 1) AS has_publisher,
            (SELECT COUNT(*) FROM {T['PERF_GEO']}
             WHERE ADVERTISER_ID = %(adv_id)s AND LOG_DATE BETWEEN %(start)s AND %(end)s
             LIMIT 1) AS has_geo,
            (SELECT COUNT(*) FROM {T['PERF_CREATIVE']}
             WHERE ADVERTISER_ID = %(adv_id)s AND LOG_DATE BETWEEN %(start)s AND %(end)s
             LIMIT 1) AS has_creative,
            (SELECT COUNT(*) FROM {T['PERF_TRAFFIC']}
             WHERE ADVERTISER_ID = %(adv_id)s AND LOG_DATE BETWEEN %(start)s AND %(end)s
             LIMIT 1) AS has_traffic,
            (SELECT COUNT(*) FROM {T['PERF_HH']}
             WHERE ADVERTISER_ID = %(adv_id)s AND LOG_DATE BETWEEN %(start)s AND %(end)s
             LIMIT 1) AS has_household
        """,
        params,
        fetch="one",
    )

    if not checks:
        checks = {}

    has_imps = safe_int(checks.get("HAS_IMPRESSIONS")) > 0
    has_sv = cfg["has_store"]
    has_wv = cfg["has_web"]

    return v6_response({
        "ADVERTISER_ID": advertiser_id,
        "IMPRESSIONS": has_imps,
        "STORE_VISITS": has_sv,
        "WEB_VISITS": has_wv,
        "PUBLISHER": safe_int(checks.get("HAS_PUBLISHER")) > 0,
        "GEO": safe_int(checks.get("HAS_GEO")) > 0,
        "CREATIVE": safe_int(checks.get("HAS_CREATIVE")) > 0,
        "TRAFFIC": safe_int(checks.get("HAS_TRAFFIC")) > 0,
        "HOUSEHOLD": safe_int(checks.get("HAS_HOUSEHOLD")) > 0,
        "LIFT": has_sv and has_imps,
        "CONFIG_STATUS": cfg["config_status"],
    })


# ---------------------------------------------------------------------------
# Summary Endpoint
# ---------------------------------------------------------------------------

@v7_bp.route("/api/v7/summary", methods=["GET"])
@require_auth
def summary():
    """Headline metrics for an advertiser.

    Returns: impressions, device_reach, household_reach, store_visits,
    web_visits, visit_rate, date range.
    """
    advertiser_id = get_advertiser_id()
    if advertiser_id is None:
        return api_error("advertiser_id is required")

    start_date, end_date = parse_date_range()
    params = {"adv_id": advertiser_id, "start": str(start_date), "end": str(end_date)}

    # Impression totals + campaign/lineitem counts from PERF_BY_PUBLISHER
    imp_row = execute_query(
        f"""
        SELECT
            SUM(IMPRESSIONS) AS total_impressions,
            SUM(DEVICE_REACH) AS total_device_reach,
            SUM(HOUSEHOLD_REACH) AS total_hh_reach,
            COUNT(DISTINCT PLATFORM_NAME) AS platform_count,
            COUNT(DISTINCT IO_ID) AS campaign_count,
            COUNT(DISTINCT LI_ID) AS lineitem_count,
            MIN(LOG_DATE) AS first_date,
            MAX(LOG_DATE) AS last_date
        FROM {T['PERF_PUB']}
        WHERE ADVERTISER_ID = %(adv_id)s
          AND LOG_DATE BETWEEN %(start)s AND %(end)s
        """,
        params,
        fetch="one",
    )

    impressions = safe_int(imp_row.get("TOTAL_IMPRESSIONS")) if imp_row else 0

    # Config-driven: only query attribution tables when configured
    cfg = get_advertiser_config(advertiser_id)

    sv_zero = {"total_visits": 0, "unique_households": 0}
    wv_zero = {"total_visits": 0, "unique_households": 0}

    sv = get_store_visits_total(advertiser_id, start_date, end_date) if cfg["has_store"] else sv_zero
    wv = get_web_visits_total(advertiser_id, start_date, end_date) if cfg["has_web"] else wv_zero

    # Coverage multiplier with statistical significance
    cov = get_coverage_multiplier(advertiser_id, grain="ADVERTISER")
    multiplier = cov["multiplier"]

    # Store visits = panel_reach × multiplier (projected total)
    # Both store and web visits are HH-matched counts — multiplier scales
    # them up to estimate total visits across all impressions (not just
    # those we could resolve to households).
    store_panel = sv["total_visits"]
    web_panel = wv["total_visits"]
    store_visits = int(store_panel * multiplier)
    web_visits = int(web_panel * multiplier)

    # Visit rate = visits / impressions
    store_vr = safe_visit_rate(store_visits, impressions)
    web_vr = safe_visit_rate(web_visits, impressions)

    return v6_response({
        "ADVERTISER_ID": advertiser_id,
        "IMPRESSIONS": impressions,
        "DEVICE_REACH": safe_int(imp_row.get("TOTAL_DEVICE_REACH")) if imp_row else 0,
        "HOUSEHOLD_REACH": safe_int(imp_row.get("TOTAL_HH_REACH")) if imp_row else 0,
        "UNIQUE_HOUSEHOLDS": sv["unique_households"],
        "STORE_PANEL_REACH": store_panel,
        "STORE_VISITS": store_visits,
        "STORE_VISIT_RATE": store_vr,
        "STORE_SIGNIFICANT": cov["is_significant"],
        "WEB_PANEL_REACH": web_panel,
        "WEB_VISITS": web_visits,
        "WEB_VISIT_RATE": web_vr,
        "WEB_SIGNIFICANT": cov["is_significant"],
        "VISIT_RATE": store_vr,
        "MULTIPLIER": multiplier,
        "MULTIPLIER_CI_LOWER": cov["ci_lower"],
        "MULTIPLIER_CI_UPPER": cov["ci_upper"],
        "MULTIPLIER_SIGNIFICANT": cov["is_significant"],
        "MULTIPLIER_GRAIN": cov["grain"],
        "MULTIPLIER_SAMPLE_SIZE": cov["sample_size"],
        "HH_RESOLUTION_RATE": cov["hh_resolution_rate"],
        "PLATFORM_COUNT": safe_int(imp_row.get("PLATFORM_COUNT")) if imp_row else 0,
        "CAMPAIGN_COUNT": safe_int(imp_row.get("CAMPAIGN_COUNT")) if imp_row else 0,
        "LINEITEM_COUNT": safe_int(imp_row.get("LINEITEM_COUNT")) if imp_row else 0,
        "START_DATE": str(start_date),
        "END_DATE": str(end_date),
        "DATA_START": str(imp_row.get("FIRST_DATE", "")) if imp_row else "",
        "DATA_END": str(imp_row.get("LAST_DATE", "")) if imp_row else "",
        "HAS_STORE_ATTRIBUTION": cfg["has_store"],
        "HAS_WEB_ATTRIBUTION": cfg["has_web"],
        "CONFIG_STATUS": cfg["config_status"],
    })


# ---------------------------------------------------------------------------
# Timeseries Endpoint
# ---------------------------------------------------------------------------

@v7_bp.route("/api/v7/timeseries", methods=["GET"])
@require_auth
def timeseries():
    """Daily timeseries for an advertiser.

    Returns arrays of daily impression, reach, visit, and visit rate metrics.
    """
    advertiser_id = get_advertiser_id()
    if advertiser_id is None:
        return api_error("advertiser_id is required")

    start_date, end_date = parse_date_range()
    params = {"adv_id": advertiser_id, "start": str(start_date), "end": str(end_date)}

    # Daily impressions from PERF_BY_PUBLISHER
    imp_rows = execute_query(
        f"""
        SELECT
            LOG_DATE,
            SUM(IMPRESSIONS) AS impressions,
            SUM(DEVICE_REACH) AS device_reach,
            SUM(HOUSEHOLD_REACH) AS hh_reach
        FROM {T['PERF_PUB']}
        WHERE ADVERTISER_ID = %(adv_id)s
          AND LOG_DATE BETWEEN %(start)s AND %(end)s
        GROUP BY LOG_DATE
        ORDER BY LOG_DATE
        """,
        params,
    )

    # Config-driven: only query attribution tables when configured
    cfg = get_advertiser_config(advertiser_id)

    sv_by_date = get_store_visits_by_date(advertiser_id, start_date, end_date) if cfg["has_store"] else {}
    wv_by_date = get_web_visits_by_date(advertiser_id, start_date, end_date) if cfg["has_web"] else {}

    cov = get_coverage_multiplier(advertiser_id)
    multiplier = cov["multiplier"]

    series = []
    for r in imp_rows:
        d = str(r["LOG_DATE"])
        imps = safe_int(r.get("IMPRESSIONS"))
        sv_day = sv_by_date.get(d, {})
        wv_day = wv_by_date.get(d, {})
        visitors = safe_int(sv_day.get("visits"))
        web_v = safe_int(wv_day.get("visits"))
        # Apply coverage multiplier to visit counts (same as campaign-performance)
        store_visits = int(visitors * multiplier) if visitors > 0 else 0
        web_visits = int(web_v * multiplier) if web_v > 0 else 0
        svr = safe_visit_rate(visitors, imps, multiplier)
        wvr = safe_visit_rate(web_v, imps, multiplier)

        series.append({
            "LOG_DATE": d,
            "IMPRESSIONS": imps,
            "DEVICE_REACH": safe_int(r.get("DEVICE_REACH")),
            "HOUSEHOLD_REACH": safe_int(r.get("HH_REACH")),
            "STORE_VISITS": store_visits,
            "STORE_VISIT_RATE": svr,
            "WEB_VISITS": web_visits,
            "WEB_VISIT_RATE": wvr,
            "UNIQUE_HOUSEHOLDS": safe_int(sv_day.get("unique_hh")),
            "VISIT_RATE": svr,
        })

    return v6_response(series)


# ---------------------------------------------------------------------------
# Core Performance Endpoints
# ---------------------------------------------------------------------------

@v7_bp.route("/api/v7/campaign-performance", methods=["GET"])
@require_auth
def campaign_performance():
    """Performance metrics grouped by insertion order (campaign).

    Aggregates from PERF_BY_PUBLISHER grouped by IO_ID.
    Enriches with store/web visit counts from HH attribution tables.
    """
    advertiser_id = get_advertiser_id()
    if advertiser_id is None:
        return api_error("advertiser_id is required")

    start_date, end_date = parse_date_range()
    params = {"adv_id": advertiser_id, "start": str(start_date), "end": str(end_date)}

    # Aggregate impressions by IO from PERF_BY_PUBLISHER
    # GROUP BY IO_ID only — IO_NAME has encoding variants that create duplicate rows
    imp_rows = execute_query(
        f"""
        SELECT
            IO_ID,
            MAX(IO_NAME) AS IO_NAME,
            MAX(PLATFORM_NAME) AS PLATFORM_NAME,
            SUM(IMPRESSIONS) AS impressions,
            SUM(DEVICE_REACH) AS device_reach,
            SUM(HOUSEHOLD_REACH) AS hh_reach,
            SUM(VISITORS) AS visitors,
            SUM(WEB_VISITORS) AS web_visitors,
            MIN(LOG_DATE) AS first_date,
            MAX(LOG_DATE) AS last_date
        FROM {T['PERF_PUB']}
        WHERE ADVERTISER_ID = %(adv_id)s
          AND LOG_DATE BETWEEN %(start)s AND %(end)s
        GROUP BY IO_ID
        ORDER BY impressions DESC
        """,
        params,
    )

    # Config-driven: only query HH attribution fallback when configured
    cfg = get_advertiser_config(advertiser_id)
    sv_by_io = get_store_visits_by_campaign(advertiser_id, start_date, end_date) if cfg["has_store"] else {}
    wv_by_io = get_web_visits_by_campaign(advertiser_id, start_date, end_date) if cfg["has_web"] else {}

    campaigns = []
    for r in imp_rows:
        io_id = str(r.get("IO_ID", ""))
        imps = safe_int(r.get("IMPRESSIONS"))

        # PERF_BY pre-enriched values (Layer 2) — use when available
        perf_store = safe_int(r.get("VISITORS"))
        perf_web = safe_int(r.get("WEB_VISITORS"))

        # HH attribution fallback for IOs where PERF_BY enrichment = 0
        sv = sv_by_io.get(io_id, {})
        wv = wv_by_io.get(io_id, {})

        # Campaign-grain multiplier with fallback to advertiser
        cov = get_coverage_multiplier(advertiser_id, grain="CAMPAIGN", grain_id=io_id)
        multiplier = cov["multiplier"]

        # Store visits: prefer PERF_BY, fallback to HH attribution
        if perf_store > 0:
            store_panel = perf_store
            store_visits = int(perf_store * multiplier)
        else:
            store_panel = safe_int(sv.get("visits"))
            store_visits = int(store_panel * multiplier)

        # Web visits: prefer PERF_BY, fallback to HH attribution
        # Multiplier applied same as store visits (HH resolution scaling)
        if perf_web > 0:
            web_panel = perf_web
            web_visits = int(perf_web * multiplier)
        else:
            web_panel = safe_int(wv.get("visits"))
            web_visits = int(web_panel * multiplier)

        svr = safe_visit_rate(store_visits, imps)
        wvr = safe_visit_rate(web_visits, imps)

        campaigns.append({
            "IO_ID": io_id,
            "IO_NAME": r.get("IO_NAME") or f"IO {io_id}",
            "PLATFORM": r.get("PLATFORM_NAME"),
            "IMPRESSIONS": imps,
            "DEVICE_REACH": safe_int(r.get("DEVICE_REACH")),
            "HOUSEHOLD_REACH": safe_int(r.get("HH_REACH")),
            "STORE_PANEL_REACH": store_panel,
            "STORE_VISITS": store_visits,
            "STORE_VISIT_RATE": svr,
            "STORE_VR": svr,
            "WEB_PANEL_REACH": web_panel,
            "WEB_VISITS": web_visits,
            "WEB_VISIT_RATE": wvr,
            "WEB_VR": wvr,
            "UNIQUE_HOUSEHOLDS": safe_int(sv.get("unique_hh")),
            "VISIT_RATE": svr,
            "MULTIPLIER": multiplier,
            "MULTIPLIER_SIGNIFICANT": cov["is_significant"],
            "MULTIPLIER_GRAIN": cov["grain"],
            "FIRST_DATE": str(r.get("FIRST_DATE", "")),
            "LAST_DATE": str(r.get("LAST_DATE", "")),
        })

    return v6_response(campaigns)


@v7_bp.route("/api/v7/lineitem-performance", methods=["GET"])
@require_auth
def lineitem_performance():
    """Performance metrics grouped by line item.

    Aggregates from PERF_BY_PUBLISHER grouped by LI_ID.
    Optionally filter by io_id to show line items within a campaign.
    """
    advertiser_id = get_advertiser_id()
    if advertiser_id is None:
        return api_error("advertiser_id is required")

    io_id_filter = request.args.get("io_id")
    start_date, end_date = parse_date_range()
    params = {"adv_id": advertiser_id, "start": str(start_date), "end": str(end_date)}

    io_clause = ""
    if io_id_filter:
        io_clause = "AND IO_ID = %(io_id)s"
        params["io_id"] = io_id_filter

    # GROUP BY IO_ID + LI_ID only — name fields have encoding variants
    imp_rows = execute_query(
        f"""
        SELECT
            IO_ID, MAX(IO_NAME) AS IO_NAME,
            LI_ID, MAX(LI_NAME) AS LI_NAME,
            MAX(PLATFORM_NAME) AS PLATFORM_NAME,
            SUM(IMPRESSIONS) AS impressions,
            SUM(DEVICE_REACH) AS device_reach,
            SUM(HOUSEHOLD_REACH) AS hh_reach,
            SUM(VISITORS) AS visitors,
            SUM(WEB_VISITORS) AS web_visitors,
            MIN(LOG_DATE) AS first_date,
            MAX(LOG_DATE) AS last_date
        FROM {T['PERF_PUB']}
        WHERE ADVERTISER_ID = %(adv_id)s
          AND LOG_DATE BETWEEN %(start)s AND %(end)s
          {io_clause}
        GROUP BY IO_ID, LI_ID
        ORDER BY impressions DESC
        """,
        params,
    )

    cfg = get_advertiser_config(advertiser_id)
    sv_by_li = get_store_visits_by_lineitem(advertiser_id, start_date, end_date) if cfg["has_store"] else {}
    wv_by_li = get_web_visits_by_lineitem(advertiser_id, start_date, end_date) if cfg["has_web"] else {}

    lineitems = []
    for r in imp_rows:
        li_id = str(r.get("LI_ID", ""))
        imps = safe_int(r.get("IMPRESSIONS"))

        # PERF_BY pre-enriched values (Layer 2) — use when available
        perf_store = safe_int(r.get("VISITORS"))
        perf_web = safe_int(r.get("WEB_VISITORS"))

        # HH attribution fallback
        sv = sv_by_li.get(li_id, {})
        wv = wv_by_li.get(li_id, {})

        # Line-item grain multiplier with fallback to campaign → advertiser
        cov = get_coverage_multiplier(advertiser_id, grain="LINE_ITEM", grain_id=li_id)
        multiplier = cov["multiplier"]

        if perf_store > 0:
            store_panel = perf_store
            store_visits = int(perf_store * multiplier)
        else:
            store_panel = safe_int(sv.get("visits"))
            store_visits = int(store_panel * multiplier)

        # Multiplier applied same as store visits (HH resolution scaling)
        if perf_web > 0:
            web_panel = perf_web
            web_visits = int(perf_web * multiplier)
        else:
            web_panel = safe_int(wv.get("visits"))
            web_visits = int(web_panel * multiplier)

        svr = safe_visit_rate(store_visits, imps)
        wvr = safe_visit_rate(web_visits, imps)

        io_name = r.get("IO_NAME") or ""
        li_name = r.get("LI_NAME") or f"LI {li_id}"
        lineitems.append({
            "IO_ID": str(r.get("IO_ID", "")),
            "IO_NAME": io_name,
            "LI_ID": li_id,
            "LI_NAME": li_name,
            "ID": li_id,
            "NAME": li_name,
            "PARENT_NAME": io_name,
            "PLATFORM": r.get("PLATFORM_NAME"),
            "IMPRESSIONS": imps,
            "DEVICE_REACH": safe_int(r.get("DEVICE_REACH")),
            "HOUSEHOLD_REACH": safe_int(r.get("HH_REACH")),
            "STORE_VISITS": store_visits,
            "STORE_VISIT_RATE": svr,
            "STORE_VR": svr,
            "WEB_VISITS": web_visits,
            "WEB_VISIT_RATE": wvr,
            "WEB_VR": wvr,
            "UNIQUE_HOUSEHOLDS": safe_int(sv.get("unique_hh")),
            "VISIT_RATE": svr,
            "MULTIPLIER": multiplier,
            "MULTIPLIER_SIGNIFICANT": cov["is_significant"],
            "MULTIPLIER_GRAIN": cov["grain"],
            "FIRST_DATE": str(r.get("FIRST_DATE", "")),
            "LAST_DATE": str(r.get("LAST_DATE", "")),
        })

    return v6_response(lineitems)


@v7_bp.route("/api/v7/creative-performance", methods=["GET"])
@require_auth
def creative_performance():
    """Performance metrics grouped by creative.

    Reads directly from PERF_BY_CREATIVE (already at creative grain).
    """
    advertiser_id = get_advertiser_id()
    if advertiser_id is None:
        return api_error("advertiser_id is required")

    start_date, end_date = parse_date_range()
    params = {"adv_id": advertiser_id, "start": str(start_date), "end": str(end_date)}

    # GROUP BY ID fields only — name fields have encoding variants
    rows = execute_query(
        f"""
        SELECT
            CREATIVE_ID, MAX(CREATIVE_NAME) AS CREATIVE_NAME,
            MAX(CREATIVE_SIZE) AS CREATIVE_SIZE,
            IO_ID, MAX(IO_NAME) AS IO_NAME,
            LI_ID, MAX(LI_NAME) AS LI_NAME,
            MAX(PLATFORM_NAME) AS PLATFORM_NAME,
            SUM(IMPRESSIONS) AS impressions,
            SUM(DEVICE_REACH) AS device_reach,
            SUM(HOUSEHOLD_REACH) AS hh_reach,
            SUM(VISITORS) AS visitors,
            SUM(WEB_VISITORS) AS web_visitors
        FROM {T['PERF_CREATIVE']}
        WHERE ADVERTISER_ID = %(adv_id)s
          AND LOG_DATE BETWEEN %(start)s AND %(end)s
        GROUP BY CREATIVE_ID, IO_ID, LI_ID
        ORDER BY impressions DESC
        """,
        params,
    )

    cfg = get_advertiser_config(advertiser_id)
    sv_by_cr = get_store_visits_by_creative(advertiser_id, start_date, end_date) if cfg["has_store"] else {}
    wv_by_cr = get_web_visits_by_creative(advertiser_id, start_date, end_date) if cfg["has_web"] else {}
    cov = get_coverage_multiplier(advertiser_id)
    multiplier = cov["multiplier"]

    creatives = []
    for r in rows:
        cr_id = str(r.get("CREATIVE_ID", ""))
        io_id = str(r.get("IO_ID", ""))
        imps = safe_int(r.get("IMPRESSIONS"))

        # PERF_BY pre-enriched values — use when available
        perf_store = safe_int(r.get("VISITORS"))
        perf_web = safe_int(r.get("WEB_VISITORS"))

        # HH attribution fallback
        sv = sv_by_cr.get((cr_id, io_id), {})
        wv = wv_by_cr.get((cr_id, io_id), {})

        if perf_store > 0:
            store_panel = perf_store
            store_visits = int(perf_store * multiplier)
        else:
            store_panel = safe_int(sv.get("visits"))
            store_visits = int(store_panel * multiplier)

        # Multiplier applied same as store visits (HH resolution scaling)
        if perf_web > 0:
            web_panel = perf_web
            web_visits = int(perf_web * multiplier)
        else:
            web_panel = safe_int(wv.get("visits"))
            web_visits = int(web_panel * multiplier)

        svr = safe_visit_rate(store_visits, imps)
        wvr = safe_visit_rate(web_visits, imps)

        hh_reach = safe_int(r.get("HH_REACH"))
        creatives.append({
            "CREATIVE_ID": cr_id,
            "CREATIVE_NAME": r.get("CREATIVE_NAME") or f"Creative {cr_id}",
            "CREATIVE_SIZE": r.get("CREATIVE_SIZE"),
            "IO_ID": io_id,
            "IO_NAME": r.get("IO_NAME") or "",
            "LI_ID": str(r.get("LI_ID", "")),
            "LI_NAME": r.get("LI_NAME") or "",
            "PLATFORM": r.get("PLATFORM_NAME"),
            "IMPRESSIONS": imps,
            "DEVICE_REACH": safe_int(r.get("DEVICE_REACH")),
            "HOUSEHOLD_REACH": hh_reach,
            "STORE_VISITS": store_visits,
            "STORE_VISIT_RATE": svr,
            "STORE_VR": svr,
            "WEB_VISITS": web_visits,
            "WEB_VISIT_RATE": wvr,
            "WEB_VR": wvr,
            "VISIT_RATE": svr,
            "MULTIPLIER": multiplier,
        })

    return v6_response(creatives)


@v7_bp.route("/api/v7/publisher-performance", methods=["GET"])
@require_auth
def publisher_performance():
    """Performance metrics grouped by publisher/supply vendor.

    Reads directly from PERF_BY_PUBLISHER.
    Supports optional grouping: publisher (default), supply_vendor, site_id.
    """
    advertiser_id = get_advertiser_id()
    if advertiser_id is None:
        return api_error("advertiser_id is required")

    group_by = request.args.get("group_by", "publisher")  # publisher|supply_vendor|site_id
    start_date, end_date = parse_date_range()
    params = {"adv_id": advertiser_id, "start": str(start_date), "end": str(end_date)}

    group_col = {
        "publisher": "PUBLISHER",
        "supply_vendor": "SUPPLY_VENDOR",
        "site_id": "SITE_ID",
    }.get(group_by, "PUBLISHER")

    rows = execute_query(
        f"""
        SELECT
            {group_col} AS group_value,
            SUM(IMPRESSIONS) AS impressions,
            SUM(DEVICE_REACH) AS device_reach,
            SUM(HOUSEHOLD_REACH) AS hh_reach,
            SUM(VISITORS) AS visitors,
            SUM(WEB_VISITORS) AS web_visitors
        FROM {T['PERF_PUB']}
        WHERE ADVERTISER_ID = %(adv_id)s
          AND LOG_DATE BETWEEN %(start)s AND %(end)s
        GROUP BY {group_col}
        ORDER BY impressions DESC
        LIMIT 500
        """,
        params,
    )

    cov = get_coverage_multiplier(advertiser_id)
    multiplier = cov["multiplier"]

    publishers = []
    for r in rows:
        imps = safe_int(r.get("IMPRESSIONS"))
        visitors = safe_int(r.get("VISITORS"))
        web_v = safe_int(r.get("WEB_VISITORS"))
        # Apply coverage multiplier to visit counts (same as campaign-performance)
        store_visits = int(visitors * multiplier) if visitors > 0 else 0
        web_visits = int(web_v * multiplier) if web_v > 0 else 0
        svr = safe_visit_rate(visitors, imps, multiplier)
        publishers.append({
            "PUBLISHER": r.get("GROUP_VALUE") or "(unknown)",
            "NAME": r.get("GROUP_VALUE") or "(unknown)",
            "IMPRESSIONS": imps,
            "DEVICE_REACH": safe_int(r.get("DEVICE_REACH")),
            "HOUSEHOLD_REACH": safe_int(r.get("HH_REACH")),
            "STORE_VISITS": store_visits,
            "STORE_VISIT_RATE": svr,
            "WEB_VISITS": web_visits,
            "VISIT_RATE": svr,
        })

    return v6_response(publishers)


@v7_bp.route("/api/v7/zip-performance", methods=["GET"])
@require_auth
def zip_performance():
    """Performance metrics grouped by ZIP code.

    Reads from PERF_BY_GEO filtered to non-empty ZIP.
    """
    advertiser_id = get_advertiser_id()
    if advertiser_id is None:
        return api_error("advertiser_id is required")

    start_date, end_date = parse_date_range()
    limit = min(int(request.args.get("limit", 500)), 2000)
    params = {"adv_id": advertiser_id, "start": str(start_date), "end": str(end_date)}

    rows = execute_query(
        f"""
        SELECT
            ZIP,
            DMA,
            SUM(IMPRESSIONS) AS impressions,
            SUM(DEVICE_REACH) AS device_reach,
            SUM(HOUSEHOLD_REACH) AS hh_reach,
            SUM(VISITORS) AS visitors,
            SUM(WEB_VISITORS) AS web_visitors
        FROM {T['PERF_GEO']}
        WHERE ADVERTISER_ID = %(adv_id)s
          AND LOG_DATE BETWEEN %(start)s AND %(end)s
          AND ZIP != ''
        GROUP BY ZIP, DMA
        ORDER BY impressions DESC
        LIMIT {limit}
        """,
        params,
    )

    cov = get_coverage_multiplier(advertiser_id)
    multiplier = cov["multiplier"]

    zips = []
    for r in rows:
        imps = safe_int(r.get("IMPRESSIONS"))
        visitors = safe_int(r.get("VISITORS"))
        web_v = safe_int(r.get("WEB_VISITORS"))
        # Apply coverage multiplier to visit counts (same as campaign-performance)
        store_visits = int(visitors * multiplier) if visitors > 0 else 0
        web_visits = int(web_v * multiplier) if web_v > 0 else 0
        svr = safe_visit_rate(visitors, imps, multiplier)
        dma_val = r.get("DMA") or ""
        zips.append({
            "ZIP_CODE": r.get("ZIP"),
            "DMA": dma_val,
            "DMA_NAME": dma_val,
            "IMPRESSIONS": imps,
            "DEVICE_REACH": safe_int(r.get("DEVICE_REACH")),
            "HOUSEHOLD_REACH": safe_int(r.get("HH_REACH")),
            "STORE_VISITS": store_visits,
            "STORE_VISIT_RATE": svr,
            "WEB_VISITS": web_visits,
            "VISIT_RATE": svr,
        })

    return v6_response(zips)


@v7_bp.route("/api/v7/dma-performance", methods=["GET"])
@require_auth
def dma_performance():
    """Performance metrics grouped by DMA (Designated Market Area).

    Reads from PERF_BY_GEO aggregated to DMA level.
    """
    advertiser_id = get_advertiser_id()
    if advertiser_id is None:
        return api_error("advertiser_id is required")

    start_date, end_date = parse_date_range()
    params = {"adv_id": advertiser_id, "start": str(start_date), "end": str(end_date)}

    rows = execute_query(
        f"""
        SELECT
            DMA,
            SUM(IMPRESSIONS) AS impressions,
            SUM(DEVICE_REACH) AS device_reach,
            SUM(HOUSEHOLD_REACH) AS hh_reach,
            SUM(VISITORS) AS visitors,
            SUM(WEB_VISITORS) AS web_visitors
        FROM {T['PERF_GEO']}
        WHERE ADVERTISER_ID = %(adv_id)s
          AND LOG_DATE BETWEEN %(start)s AND %(end)s
          AND DMA != ''
          AND DMA != '0'
        GROUP BY DMA
        ORDER BY impressions DESC
        """,
        params,
    )

    cov = get_coverage_multiplier(advertiser_id)
    multiplier = cov["multiplier"]

    dmas = []
    for r in rows:
        imps = safe_int(r.get("IMPRESSIONS"))
        visitors = safe_int(r.get("VISITORS"))
        web_v = safe_int(r.get("WEB_VISITORS"))
        # Apply coverage multiplier to visit counts (same as campaign-performance)
        store_visits = int(visitors * multiplier) if visitors > 0 else 0
        web_visits = int(web_v * multiplier) if web_v > 0 else 0
        svr = safe_visit_rate(visitors, imps, multiplier)
        dmas.append({
            "DMA": r.get("DMA"),
            "DMA_NAME": r.get("DMA") or "",
            "IMPRESSIONS": imps,
            "DEVICE_REACH": safe_int(r.get("DEVICE_REACH")),
            "HOUSEHOLD_REACH": safe_int(r.get("HH_REACH")),
            "STORE_VISITS": store_visits,
            "STORE_VISIT_RATE": svr,
            "WEB_VISITS": web_visits,
            "VISIT_RATE": svr,
        })

    return v6_response(dmas)


# ---------------------------------------------------------------------------
# Analytics Endpoints
# ---------------------------------------------------------------------------

@v7_bp.route("/api/v7/lift-analysis", methods=["GET"])
@require_auth
def lift_analysis():
    """Exposed vs control visit rate comparison (store visit lift).

    This endpoint requires row-level impression data — it cannot use
    pre-aggregated PERF_BY_* tables. It:
    1. Identifies exposed households (saw an ad for this advertiser)
    2. Identifies store-visiting households (from HH_STORE_VISIT_ATTRIBUTION)
    3. Computes visit rates for exposed vs control groups
    4. Returns lift = exposed_rate / control_rate

    Uses HH_STORE_VISIT_ATTRIBUTION (single source, no UNION of legacy tables).
    """
    advertiser_id = get_advertiser_id()
    if advertiser_id is None:
        return api_error("advertiser_id is required")

    start_date, end_date = parse_date_range()
    params = {"adv_id": advertiser_id, "start": str(start_date), "end": str(end_date)}

    rows = execute_query(
        f"""
        WITH exposed_hh AS (
            -- Households that were exposed to ads for this advertiser
            SELECT DISTINCT HOUSEHOLD_ID_CORE AS hh_id
            FROM {T['IMP_LOG']}
            WHERE ADVERTISER_ID = %(adv_id)s
              AND LOG_DATE BETWEEN %(start)s AND %(end)s
              AND HOUSEHOLD_ID_CORE IS NOT NULL
              AND HOUSEHOLD_ID_CORE > 0
        ),
        store_visit_hh AS (
            -- Households that visited stores (HH-resolved, last-touch)
            SELECT DISTINCT HOUSEHOLD_ID AS hh_id
            FROM {T['HH_STORE']}
            WHERE AD_IMPRESSION_ADVERTISER_ID = %(adv_id)s
              AND STORE_VISIT_DATE BETWEEN %(start)s AND %(end)s
              AND IS_LAST_TOUCH = TRUE
              AND HOUSEHOLD_ID IS NOT NULL
              AND HOUSEHOLD_ID > 0
        ),
        web_visit_hh AS (
            -- Households that had web visits (HH-resolved, last-touch)
            SELECT DISTINCT HOUSEHOLD_ID AS hh_id
            FROM {T['HH_WEB']}
            WHERE AD_IMPRESSION_ADVERTISER_ID = %(adv_id)s
              AND WEB_VISIT_DATE BETWEEN %(start)s AND %(end)s
              AND IS_LAST_TOUCH = TRUE
              AND HOUSEHOLD_ID IS NOT NULL
              AND HOUSEHOLD_ID > 0
        ),
        metrics AS (
            SELECT
                COUNT(DISTINCT e.hh_id) AS exposed_hh_count,
                COUNT(DISTINCT CASE WHEN sv.hh_id IS NOT NULL THEN e.hh_id END) AS exposed_store_visitors,
                COUNT(DISTINCT CASE WHEN wv.hh_id IS NOT NULL THEN e.hh_id END) AS exposed_web_visitors,
                COUNT(DISTINCT sv.hh_id) AS total_store_visit_hh,
                COUNT(DISTINCT wv.hh_id) AS total_web_visit_hh
            FROM exposed_hh e
            LEFT JOIN store_visit_hh sv ON e.hh_id = sv.hh_id
            LEFT JOIN web_visit_hh wv ON e.hh_id = wv.hh_id
        )
        SELECT * FROM metrics
        """,
        params,
        fetch="one",
    )

    if not rows:
        return v6_response({
            "ADVERTISER_ID": advertiser_id,
            "LIFT_PCT": 0,
            "CONFIDENCE": 0,
            "PANEL_REACH": 0,
            "UNIQUE_HOUSEHOLDS": 0,
            "VISITORS": 0,
            "EXPOSED_VISIT_RATE": 0,
            "CONTROL_VISIT_RATE": 0,
        })

    exposed_hh = safe_int(rows.get("EXPOSED_HH_COUNT"))
    exposed_sv = safe_int(rows.get("EXPOSED_STORE_VISITORS"))
    exposed_wv = safe_int(rows.get("EXPOSED_WEB_VISITORS"))
    total_sv_hh = safe_int(rows.get("TOTAL_STORE_VISIT_HH"))
    total_wv_hh = safe_int(rows.get("TOTAL_WEB_VISIT_HH"))

    # Exposed visit rate
    exposed_sv_rate = (exposed_sv / exposed_hh) if exposed_hh > 0 else 0
    exposed_wv_rate = (exposed_wv / exposed_hh) if exposed_hh > 0 else 0

    # Control = store visitors NOT in exposed group
    control_sv = max(total_sv_hh - exposed_sv, 0)
    # For control rate, estimate control population as a multiple of exposed (conservative)
    control_pop = max(exposed_hh, 1)  # Approximation
    control_sv_rate = (control_sv / control_pop) if control_pop > 0 else 0

    # Lift = (exposed_rate - control_rate) / control_rate * 100
    if control_sv_rate > 0:
        lift_pct = round(((exposed_sv_rate - control_sv_rate) / control_sv_rate) * 100, 1)
    elif exposed_sv_rate > 0:
        lift_pct = 100.0  # Infinite lift → cap at 100%
    else:
        lift_pct = 0.0

    # Simple confidence heuristic (based on sample size)
    confidence = min(round((exposed_hh / 1000) * 10, 1), 99.0) if exposed_hh > 0 else 0

    return v6_response({
        "ADVERTISER_ID": advertiser_id,
        "LIFT_PCT": lift_pct,
        "CONFIDENCE": confidence,
        "PANEL_REACH": exposed_hh,
        "UNIQUE_HOUSEHOLDS": exposed_hh,
        "VISITORS": exposed_sv,
        "EXPOSED_HH": exposed_hh,
        "EXPOSED_VISITORS": exposed_sv,
        "EXPOSED_VISIT_RATE": round(exposed_sv_rate, 6),
        "CONTROL_VISITORS": control_sv,
        "CONTROL_VISIT_RATE": round(control_sv_rate, 6),
        "EXPOSED_WEB_VISITORS": exposed_wv,
        "EXPOSED_WEB_VISIT_RATE": round(exposed_wv_rate, 6),
        "TOTAL_STORE_VISIT_HH": total_sv_hh,
        "TOTAL_WEB_VISIT_HH": total_wv_hh,
        "LIFT_VS_NETWORK": lift_pct,
    })


@v7_bp.route("/api/v7/traffic-sources", methods=["GET"])
@require_auth
def traffic_sources():
    """Traffic source classification from web pixel events.

    Reads from PERF_BY_TRAFFIC which classifies URLs by type.
    Also enriches with web visit attribution data.

    NOTE: This endpoint was the subject of a Feb 2025 regression spiral.
    Changes are minimal — only the data source references are updated.
    The classification logic is preserved from v6.
    """
    advertiser_id = get_advertiser_id()
    if advertiser_id is None:
        return api_error("advertiser_id is required")

    start_date, end_date = parse_date_range()
    params = {"adv_id": advertiser_id, "start": str(start_date), "end": str(end_date)}

    rows = execute_query(
        f"""
        SELECT
            URL_TYPE,
            SUM(IMPRESSIONS) AS impressions,
            SUM(DEVICE_REACH) AS device_reach,
            SUM(HOUSEHOLD_REACH) AS hh_reach,
            SUM(VISITORS) AS visitors,
            SUM(WEB_VISITORS) AS web_visitors,
            COUNT(DISTINCT URL) AS unique_urls
        FROM {T['PERF_TRAFFIC']}
        WHERE ADVERTISER_ID = %(adv_id)s
          AND LOG_DATE BETWEEN %(start)s AND %(end)s
        GROUP BY URL_TYPE
        ORDER BY impressions DESC
        """,
        params,
    )

    cov = get_coverage_multiplier(advertiser_id)
    multiplier = cov["multiplier"]

    # Compute total unique HH across all rows for VISIT_SHARE calculation
    total_unique_hh = sum(safe_int(r.get("HH_REACH")) for r in rows)

    # Compute date span for DAILY_VISITORS
    date_span = max((end_date - start_date).days, 1)

    sources = []
    for r in rows:
        imps = safe_int(r.get("IMPRESSIONS"))
        visitors = safe_int(r.get("VISITORS"))
        web_v = safe_int(r.get("WEB_VISITORS"))
        hh_reach = safe_int(r.get("HH_REACH"))
        svr = safe_visit_rate(visitors, imps, multiplier)
        url_type = r.get("URL_TYPE") or "other"

        # Derived fields the HTML expects
        # Apply coverage multiplier to visit counts (same as campaign-performance)
        store_visits = int(visitors * multiplier) if visitors > 0 else 0
        web_visits = int(web_v * multiplier) if web_v > 0 else 0
        unique_visits = store_visits  # total visit events (multiplied)
        unique_hh = hh_reach     # unique households
        daily_visitors = round(unique_hh / date_span) if date_span > 0 else 0
        visits_per_hh = round(unique_visits / unique_hh, 1) if unique_hh > 0 else 0
        visit_share = round(unique_hh * 100.0 / total_unique_hh, 1) if total_unique_hh > 0 else 0
        avg_days_to_visit = "—"  # placeholder — needs HH-level timestamp data

        sources.append({
            "SOURCE_TYPE": url_type,
            "TRAFFIC_SOURCE": url_type,
            "TRAFFIC_MEDIUM": url_type,
            "NAME": url_type,
            "IMPRESSIONS": imps,
            "DEVICE_REACH": safe_int(r.get("DEVICE_REACH")),
            "HOUSEHOLD_REACH": hh_reach,
            "UNIQUE_HOUSEHOLDS": unique_hh,
            "UNIQUE_VISITS": unique_visits,
            "DAILY_VISITORS": daily_visitors,
            "VISITS_PER_HH": visits_per_hh,
            "VISIT_SHARE": visit_share,
            "AVG_DAYS_TO_VISIT": avg_days_to_visit,
            "STORE_VISITS": store_visits,
            "STORE_VISIT_RATE": svr,
            "WEB_VISITS": web_visits,
            "UNIQUE_URLS": safe_int(r.get("UNIQUE_URLS")),
            "VISIT_RATE": svr,
            "CONVERSIONS": 0,
            "CONVERSION_VALUE": 0,
            "CONVERSION_RATE": 0,
        })

    return v6_response(sources)


# ---------------------------------------------------------------------------
# Management Endpoints
# ---------------------------------------------------------------------------

@v7_bp.route("/api/v7/agency-timeseries", methods=["GET"])
@require_auth
def agency_timeseries():
    """Daily timeseries aggregated by agency.

    When agency_id is provided, returns daily metrics for that agency.
    When omitted, returns daily metrics per agency (for overview chart).
    """
    agency_id = get_agency_id()  # Returns None if not provided
    start_date, end_date = parse_date_range()

    if agency_id is not None:
        # Single agency: aggregate daily
        params = {"agency_id": agency_id, "start": str(start_date), "end": str(end_date)}
        rows = execute_query(
            f"""
            SELECT
                LOG_DATE,
                SUM(IMPRESSIONS) AS impressions,
                SUM(COALESCE(DEVICE_REACH, 0)) AS device_reach,
                SUM(COALESCE(HOUSEHOLD_REACH, 0)) AS hh_reach,
                SUM(COALESCE(VISITORS, 0)) AS visitors,
                SUM(COALESCE(WEB_VISITORS, 0)) AS web_visitors,
                COUNT(DISTINCT ADVERTISER_ID) AS active_advertisers
            FROM {T['PERF_PUB']}
            WHERE AGENCY_ID = %(agency_id)s
              AND LOG_DATE BETWEEN %(start)s AND %(end)s
            GROUP BY LOG_DATE
            ORDER BY LOG_DATE
            """,
            params,
        )
    else:
        # All agencies: group by agency + date (for overview chart)
        params = {"start": str(start_date), "end": str(end_date)}
        rows = execute_query(
            f"""
            SELECT
                LOG_DATE,
                AGENCY_ID,
                SUM(IMPRESSIONS) AS impressions,
                SUM(COALESCE(DEVICE_REACH, 0)) AS device_reach,
                SUM(COALESCE(HOUSEHOLD_REACH, 0)) AS hh_reach,
                SUM(COALESCE(VISITORS, 0)) AS visitors,
                SUM(COALESCE(WEB_VISITORS, 0)) AS web_visitors,
                COUNT(DISTINCT ADVERTISER_ID) AS active_advertisers
            FROM {T['PERF_PUB']}
            WHERE LOG_DATE BETWEEN %(start)s AND %(end)s
            GROUP BY LOG_DATE, AGENCY_ID
            ORDER BY LOG_DATE, AGENCY_ID
            """,
            params,
        )

    if agency_id is not None:
        # Single-agency mode: return flat array (used by advertiser-detail chart)
        series = []
        for r in rows:
            imps = safe_int(r.get("IMPRESSIONS"))
            visitors = safe_int(r.get("VISITORS"))
            svr = safe_visit_rate(visitors, imps)
            series.append({
                "LOG_DATE": str(r["LOG_DATE"]),
                "IMPRESSIONS": imps,
                "DEVICE_REACH": safe_int(r.get("DEVICE_REACH")),
                "HOUSEHOLD_REACH": safe_int(r.get("HH_REACH")),
                "STORE_VISITS": visitors,
                "STORE_VISIT_RATE": svr,
                "WEB_VISITS": safe_int(r.get("WEB_VISITORS")),
                "ACTIVE_ADVERTISERS": safe_int(r.get("ACTIVE_ADVERTISERS")),
                "VISIT_RATE": svr,
            })
        return v6_response(series)
    else:
        # All-agencies mode: return nested dict for stacked bar chart
        # Shape: { "data": { "2026-02-01": { "123": 500000, ... }, ... }, "agencies": { "123": "Causal", ... } }
        data = {}
        for r in rows:
            dt_str = str(r["LOG_DATE"])
            aid = safe_int(r.get("AGENCY_ID"))
            imps = safe_int(r.get("IMPRESSIONS"))
            if dt_str not in data:
                data[dt_str] = {}
            data[dt_str][aid] = imps + data[dt_str].get(aid, 0)

        agencies = {}
        for date_map in data.values():
            for aid in date_map:
                if aid not in agencies:
                    agencies[aid] = resolve_agency_name(aid)

        return jsonify({"success": True, "data": data, "agencies": agencies})


@v7_bp.route("/api/v7/advertiser-timeseries", methods=["GET"])
@require_auth
def advertiser_timeseries():
    """Daily timeseries per advertiser within an agency.

    Returns per-advertiser daily breakdown (for agency-level dashboard).
    """
    agency_id = get_agency_id()
    if agency_id is None:
        return api_error("agency_id is required")

    start_date, end_date = parse_date_range()
    params = {"agency_id": agency_id, "start": str(start_date), "end": str(end_date)}

    rows = execute_query(
        f"""
        SELECT
            p.ADVERTISER_ID,
            COALESCE(REGEXP_REPLACE(aap.ADVERTISER_NAME, '^[^ ]+ - ', ''), 'Advertiser ' || p.ADVERTISER_ID) AS adv_name,
            p.LOG_DATE,
            SUM(p.IMPRESSIONS) AS impressions,
            SUM(p.VISITORS) AS visitors,
            SUM(p.WEB_VISITORS) AS web_visitors
        FROM {T['PERF_PUB']} p
        LEFT JOIN {T['AAP']} aap
            ON p.ADVERTISER_ID = aap.ADVERTISER_ID
        WHERE p.AGENCY_ID = %(agency_id)s
          AND p.LOG_DATE BETWEEN %(start)s AND %(end)s
        GROUP BY p.ADVERTISER_ID, adv_name, p.LOG_DATE
        ORDER BY p.ADVERTISER_ID, p.LOG_DATE
        """,
        params,
    )

    # Build nested dict for stacked bar chart
    # Shape: { "data": { "2026-02-01": { "47648": 500000, ... } }, "advertisers": { "47648": "Advertiser 47648", ... } }
    data = {}
    advertisers = {}
    for r in rows:
        dt_str = str(r["LOG_DATE"])
        adv_id = safe_int(r.get("ADVERTISER_ID"))
        imps = safe_int(r.get("IMPRESSIONS"))
        adv_name = r.get("ADV_NAME") or f"Advertiser {adv_id}"
        if dt_str not in data:
            data[dt_str] = {}
        data[dt_str][adv_id] = imps + data[dt_str].get(adv_id, 0)
        if adv_id not in advertisers:
            advertisers[adv_id] = adv_name

    return jsonify({"success": True, "data": data, "advertisers": advertisers})


@v7_bp.route("/api/v7/optimize", methods=["GET"])
@require_auth
def optimize():
    """Optimization data — DIM_TYPE UNION ALL format for frontend rendering.

    Returns rows with DIM_TYPE, DIM_KEY, DIM_NAME, IMPS, WEB_VISITS,
    STORE_VISITS, WEB_VR, STORE_VR for: baseline, campaign, lineitem,
    creative, dow, site.

    The HTML merges this with optimize-geo (dma, zip) and filters by
    DIM_TYPE to render each section of the Optimize tab.
    """
    advertiser_id = get_advertiser_id()
    if advertiser_id is None:
        return api_error("advertiser_id is required")

    start_date, end_date = parse_date_range()
    params = {"adv_id": advertiser_id, "start": str(start_date), "end": str(end_date)}

    # ---------- Visit rate expression ----------
    vr_store = "ROUND(SUM(VISITORS) * 100.0 / NULLIF(SUM(IMPRESSIONS), 0), 4)"
    vr_web   = "ROUND(SUM(WEB_VISITORS) * 100.0 / NULLIF(SUM(IMPRESSIONS), 0), 4)"

    # ---------- UNION ALL: baseline + campaign + lineitem + dow + site ----------
    # Source: PERF_BY_PUBLISHER (has IO_ID, LI_ID, PUBLISHER, LOG_DATE)
    pub_filter = f"""
        WHERE ADVERTISER_ID = %(adv_id)s
          AND LOG_DATE BETWEEN %(start)s AND %(end)s
    """

    rows = execute_query(
        f"""
        SELECT 'baseline' AS DIM_TYPE, 'overall' AS DIM_KEY, NULL AS DIM_NAME,
            SUM(IMPRESSIONS) AS IMPS, SUM(WEB_VISITORS) AS WEB_VISITS,
            SUM(VISITORS) AS STORE_VISITS,
            {vr_web} AS WEB_VR, {vr_store} AS STORE_VR
        FROM {T['PERF_PUB']}
        {pub_filter}

        UNION ALL

        SELECT 'campaign', IO_ID::VARCHAR, MAX(IO_NAME),
            SUM(IMPRESSIONS), SUM(WEB_VISITORS), SUM(VISITORS),
            {vr_web}, {vr_store}
        FROM {T['PERF_PUB']}
        {pub_filter}
          AND IO_ID IS NOT NULL
        GROUP BY IO_ID

        UNION ALL

        SELECT 'lineitem', LI_ID::VARCHAR, MAX(LI_NAME),
            SUM(IMPRESSIONS), SUM(WEB_VISITORS), SUM(VISITORS),
            {vr_web}, {vr_store}
        FROM {T['PERF_PUB']}
        {pub_filter}
          AND LI_ID IS NOT NULL
        GROUP BY LI_ID

        UNION ALL

        SELECT 'creative', CREATIVE_NAME, NULL,
            SUM(IMPRESSIONS), SUM(WEB_VISITORS), SUM(VISITORS),
            {vr_web}, {vr_store}
        FROM {T['PERF_CREATIVE']}
        {pub_filter}
          AND CREATIVE_NAME IS NOT NULL AND CREATIVE_NAME != ''
        GROUP BY CREATIVE_NAME

        UNION ALL

        SELECT 'dow', DAYOFWEEK(LOG_DATE)::VARCHAR, NULL,
            SUM(IMPRESSIONS), SUM(WEB_VISITORS), SUM(VISITORS),
            {vr_web}, {vr_store}
        FROM {T['PERF_PUB']}
        {pub_filter}
        GROUP BY DAYOFWEEK(LOG_DATE)

        UNION ALL

        SELECT 'site', PUBLISHER, NULL,
            SUM(IMPRESSIONS), SUM(WEB_VISITORS), SUM(VISITORS),
            {vr_web}, {vr_store}
        FROM {T['PERF_PUB']}
        {pub_filter}
          AND PUBLISHER IS NOT NULL AND PUBLISHER != ''
        GROUP BY PUBLISHER
        HAVING SUM(IMPRESSIONS) >= 500

        ORDER BY 1, 4 DESC
        """,
        params,
    )

    results = []
    for r in rows:
        results.append({
            "DIM_TYPE": r.get("DIM_TYPE"),
            "DIM_KEY": r.get("DIM_KEY"),
            "DIM_NAME": r.get("DIM_NAME"),
            "IMPS": safe_int(r.get("IMPS")),
            "WEB_VISITS": safe_int(r.get("WEB_VISITS")),
            "STORE_VISITS": safe_int(r.get("STORE_VISITS")),
            "WEB_VR": safe_float(r.get("WEB_VR")),
            "STORE_VR": safe_float(r.get("STORE_VR")),
        })

    return v6_response(results)


@v7_bp.route("/api/v7/optimize-geo", methods=["GET"])
@require_auth
def optimize_geo():
    """Geographic optimization — DIM_TYPE UNION ALL format (dma + zip).

    Returns rows with DIM_TYPE='dma' or 'zip', plus DIM_KEY, DIM_NAME,
    IMPS, WEB_VISITS, STORE_VISITS, WEB_VR, STORE_VR.

    The HTML merges these into the optimize tab data alongside the
    baseline/campaign/lineitem/creative/dow/site rows from /optimize.
    """
    advertiser_id = get_advertiser_id()
    if advertiser_id is None:
        return api_error("advertiser_id is required")

    start_date, end_date = parse_date_range()
    params = {"adv_id": advertiser_id, "start": str(start_date), "end": str(end_date)}

    vr_store = "ROUND(SUM(VISITORS) * 100.0 / NULLIF(SUM(IMPRESSIONS), 0), 4)"
    vr_web   = "ROUND(SUM(WEB_VISITORS) * 100.0 / NULLIF(SUM(IMPRESSIONS), 0), 4)"

    geo_filter = f"""
        WHERE ADVERTISER_ID = %(adv_id)s
          AND LOG_DATE BETWEEN %(start)s AND %(end)s
    """

    rows = execute_query(
        f"""
        SELECT 'dma' AS DIM_TYPE, DMA AS DIM_KEY, DMA AS DIM_NAME,
            SUM(IMPRESSIONS) AS IMPS, SUM(WEB_VISITORS) AS WEB_VISITS,
            SUM(VISITORS) AS STORE_VISITS,
            {vr_web} AS WEB_VR, {vr_store} AS STORE_VR
        FROM {T['PERF_GEO']}
        {geo_filter}
          AND DMA IS NOT NULL AND DMA != '' AND DMA != '0'
        GROUP BY DMA
        HAVING SUM(IMPRESSIONS) >= 500

        UNION ALL

        SELECT 'zip', ZIP, MAX(DMA),
            SUM(IMPRESSIONS), SUM(WEB_VISITORS), SUM(VISITORS),
            {vr_web}, {vr_store}
        FROM {T['PERF_GEO']}
        {geo_filter}
          AND ZIP IS NOT NULL AND ZIP != ''
        GROUP BY ZIP
        HAVING SUM(IMPRESSIONS) >= 50

        ORDER BY 1, 4 DESC
        """,
        params,
    )

    results = []
    for r in rows:
        results.append({
            "DIM_TYPE": r.get("DIM_TYPE"),
            "DIM_KEY": r.get("DIM_KEY"),
            "DIM_NAME": r.get("DIM_NAME"),
            "IMPS": safe_int(r.get("IMPS")),
            "WEB_VISITS": safe_int(r.get("WEB_VISITS")),
            "STORE_VISITS": safe_int(r.get("STORE_VISITS")),
            "WEB_VR": safe_float(r.get("WEB_VR")),
            "STORE_VR": safe_float(r.get("STORE_VR")),
        })

    return v6_response(results)


@v7_bp.route("/api/v7/pipeline-health", methods=["GET"])
@require_auth
def pipeline_health():
    """Check freshness and row counts for all critical tables.

    Returns status per table: last update timestamp, row count,
    and staleness flag (>24h = stale).
    """
    # Per-table column map: (display_name, fqn, freshness_col, range_col)
    # freshness_col = column to check for "when was this table last updated"
    # range_col = column for min/max date range (None if not applicable)
    tables_to_check = [
        ("PERF_BY_PUBLISHER",          T["PERF_PUB"],     "CREATED_AT",          "LOG_DATE"),
        ("PERF_BY_GEO",                T["PERF_GEO"],     "CREATED_AT",          "LOG_DATE"),
        ("PERF_BY_TRAFFIC",            T["PERF_TRAFFIC"],  "CREATED_AT",          "LOG_DATE"),
        ("PERF_BY_CREATIVE",           T["PERF_CREATIVE"], "CREATED_AT",          "LOG_DATE"),
        ("PERF_BY_HOUSEHOLD",          T["PERF_HH"],      "CREATED_AT",          "LOG_DATE"),
        ("HH_STORE_VISIT_ATTRIBUTION", T["HH_STORE"],     "INSERTED_AT",         "STORE_VISIT_DATE"),
        ("HH_WEB_VISIT_ATTRIBUTION",   T["HH_WEB"],       "INSERTED_AT",         "WEB_VISIT_DATE"),
        ("WEBPIXEL_EVENTS",            T["WEBPIXEL"],      "EVENT_TIMESTAMP",     "EVENT_TIMESTAMP"),
        ("AD_IMPRESSION_LOG_V2",       T["IMP_LOG"],       "INGESTION_TIMESTAMP", "INGESTION_TIMESTAMP"),
        ("PIXEL_CAMPAIGN_MAPPING_V2",  T["PCM"],           "CREATED_AT",          "CREATED_AT"),
        ("COVERAGE_MULTIPLIER_STATS",  T["COVERAGE"],      "COMPUTED_AT",         "COMPUTED_AT"),
    ]

    results = []
    for name, fqn, freshness_col, range_col in tables_to_check:
        try:
            # Build query using the correct columns for each table
            range_select = ""
            if range_col and range_col != freshness_col:
                range_select = f", MAX({range_col}) AS max_date, MIN({range_col}) AS min_date"
            elif range_col:
                range_select = f", MAX({range_col}) AS max_date, MIN({range_col}) AS min_date"

            row = execute_query(
                f"""
                SELECT
                    COUNT(*) AS row_count,
                    MAX({freshness_col}) AS last_created
                    {range_select}
                FROM {fqn}
                """,
                fetch="one",
            )
            if row:
                date_range = {}
                if range_col:
                    date_range = {
                        "min": str(row.get("MIN_DATE", "")),
                        "max": str(row.get("MAX_DATE", "")),
                    }
                results.append({
                    "table": name,
                    "row_count": safe_int(row.get("ROW_COUNT")),
                    "last_created": str(row.get("LAST_CREATED", "")),
                    "date_range": date_range,
                    "status": "ok",
                })
            else:
                results.append({"table": name, "status": "empty"})
        except Exception as e:
            results.append({"table": name, "status": "error", "error": str(e)})

    return v6_response({"TABLES": results, "CHECKED_AT": str(datetime.utcnow())})


@v7_bp.route("/api/v7/table-access", methods=["GET"])
@require_auth
def table_access():
    """List all canonical tables available to the optimizer.

    Returns the table registry with descriptions.
    """
    tables = [
        {"schema": "DERIVED_TABLES", "table": "PERF_BY_PUBLISHER", "type": "dimensional_fact",
         "description": "Publisher/supply-side performance, pre-enriched with visitors"},
        {"schema": "DERIVED_TABLES", "table": "PERF_BY_GEO", "type": "dimensional_fact",
         "description": "Geographic performance by ZIP and DMA"},
        {"schema": "DERIVED_TABLES", "table": "PERF_BY_TRAFFIC", "type": "dimensional_fact",
         "description": "Traffic source classification performance"},
        {"schema": "DERIVED_TABLES", "table": "PERF_BY_CREATIVE", "type": "dimensional_fact",
         "description": "Creative-level performance"},
        {"schema": "DERIVED_TABLES", "table": "PERF_BY_HOUSEHOLD", "type": "dimensional_fact",
         "description": "Household-level performance (top 1000 HH per advertiser)"},
        {"schema": "DERIVED_TABLES", "table": "HH_STORE_VISIT_ATTRIBUTION", "type": "attribution",
         "description": "Store visit attribution — HH-resolved, multi-touch"},
        {"schema": "DERIVED_TABLES", "table": "HH_WEB_VISIT_ATTRIBUTION", "type": "attribution",
         "description": "Web visit attribution — HH-resolved, multi-touch"},
        {"schema": "DERIVED_TABLES", "table": "WEBPIXEL_EVENTS", "type": "event",
         "description": "Raw web pixel events (page views, conversions)"},
        {"schema": "BASE_TABLES", "table": "AD_IMPRESSION_LOG_V2", "type": "raw",
         "description": "Unified impression log (all DSPs) — used for lift analysis"},
        {"schema": "REF_DATA", "table": "PIXEL_CAMPAIGN_MAPPING_V2", "type": "reference",
         "description": "Pixel→campaign name resolution"},
        {"schema": "APP_DB", "table": "AGENCY_ADVERTISER", "type": "config",
         "description": "Agency/advertiser registry"},
        {"schema": "APP_DB", "table": "REPORT_LAYOUT_SETTING", "type": "config",
         "description": "Per-advertiser report layout configuration"},
        {"schema": "BASE_TABLES", "table": "REF_ADVERTISER_CONFIG", "type": "config",
         "description": "Per-advertiser settings (multiplier, thresholds)"},
        {"schema": "HOUSEHOLD_CORE", "table": "IP_HOUSEHOLD_LOOKUP", "type": "identity",
         "description": "IP→Household resolution (used in lift analysis)"},
        {"schema": "HOUSEHOLD_CORE", "table": "MAID_HOUSEHOLD_LOOKUP", "type": "identity",
         "description": "MAID→Household resolution (used in lift analysis)"},
    ]

    return v6_response(tables)


# ---------------------------------------------------------------------------
# Store Visit Detail (new in v7 — exposes brand-level breakdown)
# ---------------------------------------------------------------------------

@v7_bp.route("/api/v7/store-visit-detail", methods=["GET"])
@require_auth
def store_visit_detail():
    """Detailed store visit breakdown by brand and category.

    New in v7 — leverages HH_STORE_VISIT_ATTRIBUTION's BRAND/CATEGORY fields.
    """
    advertiser_id = get_advertiser_id()
    if advertiser_id is None:
        return api_error("advertiser_id is required")

    start_date, end_date = parse_date_range()

    # Config-driven: skip if store visit attribution not configured
    cfg = get_advertiser_config(advertiser_id)
    if not cfg["has_store"]:
        return v6_response({
            "ADVERTISER_ID": advertiser_id,
            "TOTAL_VISITS": 0,
            "UNIQUE_HOUSEHOLDS": 0,
            "BRANDS": [],
            "HAS_STORE_ATTRIBUTION": False,
        })

    brands = get_store_visits_by_brand(advertiser_id, start_date, end_date)
    totals = get_store_visits_total(advertiser_id, start_date, end_date)

    # Convert brand records to UPPERCASE
    uc_brands = []
    for b in brands:
        uc_brands.append({
            "BRAND": b.get("brand"),
            "CATEGORY": b.get("category"),
            "VISITS": b.get("visits", 0),
            "UNIQUE_HH": b.get("unique_hh", 0),
            "AVG_DAYS_TO_VISIT": b.get("avg_days_to_visit", 0),
        })

    return v6_response({
        "ADVERTISER_ID": advertiser_id,
        "TOTAL_VISITS": totals["total_visits"],
        "UNIQUE_HOUSEHOLDS": totals["unique_households"],
        "BRANDS": uc_brands,
    })


# ---------------------------------------------------------------------------
# Main Entry Point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    # Standalone testing mode — creates its own Flask app
    app = Flask(__name__)
    register_v7_config(app)
    app.register_blueprint(v7_bp)
    from flask_cors import CORS
    CORS(app)
    port = int(os.environ.get("PORT", 5001))
    debug = os.environ.get("FLASK_DEBUG", "0") == "1"
    logger.info(f"Starting Optimizer API v7 (standalone) on port {port}")
    app.run(host="0.0.0.0", port=port, debug=debug)
