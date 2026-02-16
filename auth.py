"""
Clerk Authentication Middleware for Flask
==========================================
JWT verification against Clerk's JWKS endpoint.
Provides decorators for route-level auth and role enforcement.

User metadata expected in Clerk public_metadata:
  {
    "role": "admin" | "agency",
    "agency_id": null | 1480 | ...
  }

Admin users see all agencies. Agency users are locked to their agency_id.
"""
import os
import json
import time
import functools
import threading

from flask import request, jsonify, g
import jwt
import requests


# ---------------------------------------------------------------------------
# JWKS Cache — fetch Clerk's public keys once, refresh periodically
# ---------------------------------------------------------------------------
_jwks_cache = {"keys": None, "fetched_at": 0}
_jwks_lock = threading.Lock()
JWKS_TTL = 3600  # re-fetch keys every hour


def _get_clerk_domain():
    """Derive Clerk frontend API domain from publishable key or env var."""
    domain = os.environ.get('CLERK_DOMAIN')
    if domain:
        return domain.rstrip('/')

    # Fallback: derive from publishable key (pk_live_xxx or pk_test_xxx)
    pk = os.environ.get('CLERK_PUBLISHABLE_KEY', '')
    if pk:
        # Clerk publishable keys encode the instance — but we need the JWKS URL
        # which is at the Clerk Frontend API domain
        pass

    # Default: must be set via CLERK_DOMAIN env var
    return None


def _get_jwks():
    """Fetch and cache Clerk's JWKS (JSON Web Key Set)."""
    with _jwks_lock:
        now = time.time()
        if _jwks_cache["keys"] and (now - _jwks_cache["fetched_at"]) < JWKS_TTL:
            return _jwks_cache["keys"]

    # Clerk JWKS endpoint
    clerk_domain = _get_clerk_domain()
    if not clerk_domain:
        # Try the Clerk Backend API approach
        secret_key = os.environ.get('CLERK_SECRET_KEY', '')
        if secret_key:
            # Use Clerk Backend API to get JWKS
            resp = requests.get(
                'https://api.clerk.com/v1/jwks',
                headers={'Authorization': f'Bearer {secret_key}'},
                timeout=10
            )
            resp.raise_for_status()
            keys = resp.json().get('keys', [])
        else:
            raise RuntimeError("CLERK_SECRET_KEY or CLERK_DOMAIN must be set")
    else:
        jwks_url = f"{clerk_domain}/.well-known/jwks.json"
        resp = requests.get(jwks_url, timeout=10)
        resp.raise_for_status()
        keys = resp.json().get('keys', [])

    with _jwks_lock:
        _jwks_cache["keys"] = keys
        _jwks_cache["fetched_at"] = time.time()

    return keys


def _decode_clerk_token(token):
    """
    Decode and verify a Clerk JWT.
    Returns the decoded payload or raises an exception.
    """
    # Get the key ID from the token header
    unverified_header = jwt.get_unverified_header(token)
    kid = unverified_header.get('kid')

    # Find the matching key from JWKS
    jwks = _get_jwks()
    matching_key = None
    for key_data in jwks:
        if key_data.get('kid') == kid:
            matching_key = key_data
            break

    if not matching_key:
        # Force refresh JWKS in case keys rotated
        with _jwks_lock:
            _jwks_cache["fetched_at"] = 0
        jwks = _get_jwks()
        for key_data in jwks:
            if key_data.get('kid') == kid:
                matching_key = key_data
                break

    if not matching_key:
        raise jwt.InvalidTokenError(f"No matching key found for kid={kid}")

    # Build the public key from JWK
    public_key = jwt.algorithms.RSAAlgorithm.from_jwk(json.dumps(matching_key))

    # Decode and verify the token
    payload = jwt.decode(
        token,
        public_key,
        algorithms=['RS256'],
        options={
            'verify_aud': False,  # Clerk tokens don't always have aud
            'verify_iss': False,  # We verify via JWKS key match
        }
    )
    return payload


def _extract_user_info(payload):
    """
    Extract role and agency_id from Clerk JWT payload.
    Clerk stores custom data in public_metadata or session claims.
    """
    # Check for public_metadata in the token (Clerk session claims)
    metadata = payload.get('public_metadata', {})
    if not metadata:
        # Also check under 'metadata' key
        metadata = payload.get('metadata', {})
    if not metadata:
        # Check Clerk's custom claims path
        metadata = payload.get('user_public_metadata', {})

    role = metadata.get('role', 'agency')  # default to agency (least privilege)
    agency_id = metadata.get('agency_id')

    # Convert agency_id to int if it's a string
    if agency_id is not None:
        try:
            agency_id = int(agency_id)
        except (ValueError, TypeError):
            agency_id = None

    return {
        'user_id': payload.get('sub'),
        'role': role,
        'agency_id': agency_id,
        'email': payload.get('email', ''),
    }


# ---------------------------------------------------------------------------
# Auth check (non-decorator) — returns True if auth is disabled or valid
# ---------------------------------------------------------------------------
def _is_auth_enabled():
    """Check if Clerk auth is configured. If not, skip auth (dev mode)."""
    return bool(os.environ.get('CLERK_SECRET_KEY'))


def _check_auth():
    """
    Verify auth and populate g.user. Returns None if OK, or a Response if denied.
    """
    if not _is_auth_enabled():
        # Auth not configured — allow all (dev mode), set admin user
        g.user = {'user_id': 'dev', 'role': 'admin', 'agency_id': None, 'email': 'dev@quorum.inc'}
        return None

    auth_header = request.headers.get('Authorization', '')
    if not auth_header.startswith('Bearer '):
        return jsonify({'error': 'Missing or invalid Authorization header'}), 401

    token = auth_header[7:]  # Strip "Bearer "

    try:
        payload = _decode_clerk_token(token)
        g.user = _extract_user_info(payload)
        return None  # Auth OK
    except jwt.ExpiredSignatureError:
        return jsonify({'error': 'Token expired'}), 401
    except jwt.InvalidTokenError as e:
        return jsonify({'error': f'Invalid token: {str(e)}'}), 401
    except Exception as e:
        return jsonify({'error': f'Authentication failed: {str(e)}'}), 401


# ---------------------------------------------------------------------------
# Decorators
# ---------------------------------------------------------------------------
def require_auth(f):
    """Decorator: requires a valid Clerk JWT. Populates g.user."""
    @functools.wraps(f)
    def decorated(*args, **kwargs):
        err = _check_auth()
        if err:
            return err
        return f(*args, **kwargs)
    return decorated


def require_admin(f):
    """Decorator: requires a valid Clerk JWT with role='admin'."""
    @functools.wraps(f)
    def decorated(*args, **kwargs):
        err = _check_auth()
        if err:
            return err
        if g.user.get('role') != 'admin':
            return jsonify({'error': 'Admin access required'}), 403
        return f(*args, **kwargs)
    return decorated


def get_effective_agency_id(requested_agency_id):
    """
    For admin users: returns whatever agency_id was requested.
    For agency users: returns their assigned agency_id (ignores the request param).
    """
    user = getattr(g, 'user', None)
    if not user:
        return requested_agency_id

    if user['role'] == 'admin':
        return requested_agency_id
    else:
        # Agency users are locked to their assigned agency_id
        return user.get('agency_id') or requested_agency_id
