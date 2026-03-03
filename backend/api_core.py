#!/usr/bin/env python3
# Audit date: 2026-03-03 — fixes applied for CRITICAL, HIGH, and safe MEDIUM issues.
# See backend_audit_report.md for full details.
"""
GoHireHumans API - CGI Backend
Handles all API routes for the AI-to-Human task marketplace.
Routes via PATH_INFO: /cgi-bin/api.py/<route>
"""

import json
import os
import sys
import sqlite3
import hashlib
import hmac
import secrets
import time
import re
import threading
import urllib.parse
import urllib.request
from datetime import datetime, timezone, timedelta


SERVICE_FEE_RATE = 0.01  # 1% platform service fee
# ─── Database Setup ───────────────────────────────────────────────────────────

DB_PATH = os.environ.get("DATABASE_PATH", "agentwork.db")

def get_db():
    db = sqlite3.connect(DB_PATH)
    db.row_factory = sqlite3.Row
    db.execute("PRAGMA journal_mode=WAL")
    db.execute("PRAGMA foreign_keys=ON")
    return db

def init_db():
    db = get_db()
    db.executescript("""
    CREATE TABLE IF NOT EXISTS users (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        email TEXT UNIQUE NOT NULL,
        password_hash TEXT NOT NULL,
        role TEXT NOT NULL CHECK(role IN ('worker','ai_client','admin')),
        name TEXT,
        handle TEXT,
        avatar_url TEXT,
        created_at TEXT DEFAULT (datetime('now')),
        updated_at TEXT DEFAULT (datetime('now')),
        is_active INTEGER DEFAULT 1,
        is_suspended INTEGER DEFAULT 0,
        is_banned INTEGER DEFAULT 0
    );

    CREATE TABLE IF NOT EXISTS worker_profiles (
        user_id INTEGER PRIMARY KEY REFERENCES users(id),
        skills TEXT DEFAULT '[]',
        hourly_rate_min REAL,
        hourly_rate_max REAL,
        per_task_rate_min REAL,
        per_task_rate_max REAL,
        geography TEXT,
        timezone TEXT,
        payout_method TEXT DEFAULT 'pending_setup',
        bio TEXT,
        avg_rating REAL DEFAULT 0,
        total_ratings INTEGER DEFAULT 0,
        total_tasks_completed INTEGER DEFAULT 0,
        estimated_earnings REAL DEFAULT 0,
        withdrawable_balance REAL DEFAULT 0,
        favorite_categories TEXT DEFAULT '[]',
        notification_email INTEGER DEFAULT 1,
        payout_account_id TEXT,
        payout_method_details TEXT,
        is_verified INTEGER DEFAULT 0,
        verification_level TEXT DEFAULT 'basic',
        total_flags INTEGER DEFAULT 0
    );

    CREATE TABLE IF NOT EXISTS ai_client_profiles (
        user_id INTEGER PRIMARY KEY REFERENCES users(id),
        app_name TEXT,
        description TEXT,
        callback_urls TEXT DEFAULT '[]',
        webhook_url TEXT,
        webhook_secret TEXT,
        billing_email TEXT,
        credit_balance REAL DEFAULT 0,
        total_spent REAL DEFAULT 0,
        avg_rating REAL DEFAULT 0,
        total_ratings INTEGER DEFAULT 0
    );

    CREATE TABLE IF NOT EXISTS api_keys (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER NOT NULL REFERENCES users(id),
        key_prefix TEXT NOT NULL,
        key_hash TEXT NOT NULL,
        name TEXT,
        created_at TEXT DEFAULT (datetime('now')),
        last_used_at TEXT,
        is_active INTEGER DEFAULT 1,
        usage_count INTEGER DEFAULT 0
    );

    CREATE TABLE IF NOT EXISTS tasks (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        client_id INTEGER NOT NULL REFERENCES users(id),
        worker_id INTEGER REFERENCES users(id),
        title TEXT NOT NULL,
        description TEXT NOT NULL,
        category TEXT NOT NULL,
        location_type TEXT NOT NULL CHECK(location_type IN ('remote','specific_address','region')),
        location_detail TEXT,
        budget_type TEXT NOT NULL CHECK(budget_type IN ('flat_fee','hourly')),
        budget_amount REAL NOT NULL,
        time_cap_hours REAL,
        due_by TEXT,
        required_skills TEXT DEFAULT '[]',
        attachments TEXT DEFAULT '[]',
        status TEXT NOT NULL DEFAULT 'draft' CHECK(status IN ('draft','open','reserved','in_progress','submitted','completed','disputed','canceled')),
        reserved_until TEXT,
        deliverables_text TEXT,
        deliverables_files TEXT DEFAULT '[]',
        deliverables_data TEXT,
        idempotency_key TEXT UNIQUE,
        created_at TEXT DEFAULT (datetime('now')),
        updated_at TEXT DEFAULT (datetime('now')),
        completed_at TEXT,
        canceled_at TEXT
    );

    CREATE TABLE IF NOT EXISTS ratings (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        task_id INTEGER NOT NULL REFERENCES tasks(id),
        from_user_id INTEGER NOT NULL REFERENCES users(id),
        to_user_id INTEGER NOT NULL REFERENCES users(id),
        rating INTEGER NOT NULL CHECK(rating BETWEEN 1 AND 5),
        review TEXT,
        created_at TEXT DEFAULT (datetime('now'))
    );

    CREATE TABLE IF NOT EXISTS flags (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        flagged_by INTEGER NOT NULL REFERENCES users(id),
        flagged_entity_type TEXT NOT NULL CHECK(flagged_entity_type IN ('task','worker','client')),
        flagged_entity_id INTEGER NOT NULL,
        reason TEXT NOT NULL,
        status TEXT DEFAULT 'pending' CHECK(status IN ('pending','reviewed','resolved','dismissed')),
        admin_notes TEXT,
        created_at TEXT DEFAULT (datetime('now')),
        resolved_at TEXT
    );

    CREATE TABLE IF NOT EXISTS ledger (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER NOT NULL REFERENCES users(id),
        task_id INTEGER REFERENCES tasks(id),
        entry_type TEXT NOT NULL CHECK(entry_type IN ('credit','debit','fee','payout_request','fund_add')),
        amount REAL NOT NULL,
        balance_after REAL,
        description TEXT,
        stripe_payment_id TEXT,
        created_at TEXT DEFAULT (datetime('now'))
    );

    CREATE TABLE IF NOT EXISTS payout_requests (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER NOT NULL REFERENCES users(id),
        amount REAL NOT NULL,
        status TEXT DEFAULT 'pending' CHECK(status IN ('pending','processing','completed','rejected')),
        created_at TEXT DEFAULT (datetime('now')),
        processed_at TEXT
    );

    CREATE TABLE IF NOT EXISTS webhook_events (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        client_id INTEGER NOT NULL REFERENCES users(id),
        task_id INTEGER NOT NULL,
        event_type TEXT NOT NULL,
        payload TEXT NOT NULL,
        delivery_status TEXT DEFAULT 'pending' CHECK(delivery_status IN ('pending','delivered','failed','retrying')),
        attempts INTEGER DEFAULT 0,
        last_attempt_at TEXT,
        created_at TEXT DEFAULT (datetime('now'))
    );

    CREATE TABLE IF NOT EXISTS audit_log (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER,
        action TEXT NOT NULL,
        entity_type TEXT,
        entity_id INTEGER,
        details TEXT,
        ip_address TEXT,
        created_at TEXT DEFAULT (datetime('now'))
    );

    CREATE TABLE IF NOT EXISTS file_uploads (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER NOT NULL REFERENCES users(id),
        task_id INTEGER REFERENCES tasks(id),
        filename TEXT NOT NULL,
        content_type TEXT,
        storage_key TEXT NOT NULL,
        file_size INTEGER,
        status TEXT DEFAULT 'pending' CHECK(status IN ('pending','completed','failed')),
        upload_url TEXT,
        created_at TEXT DEFAULT (datetime('now'))
    );

    CREATE TABLE IF NOT EXISTS worker_verifications (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER NOT NULL REFERENCES users(id),
        verification_type TEXT NOT NULL CHECK(verification_type IN ('identity','background','license','notary','medical')),
        status TEXT DEFAULT 'pending' CHECK(status IN ('pending','approved','rejected','expired')),
        document_reference TEXT,
        verified_at TEXT,
        expires_at TEXT,
        admin_notes TEXT,
        created_at TEXT DEFAULT (datetime('now'))
    );

    CREATE TABLE IF NOT EXISTS notifications (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER NOT NULL REFERENCES users(id),
        type TEXT NOT NULL,
        title TEXT NOT NULL,
        message TEXT,
        link TEXT,
        is_read INTEGER DEFAULT 0,
        created_at TEXT DEFAULT (datetime('now'))
    );

    CREATE TABLE IF NOT EXISTS quality_reviews (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        task_id INTEGER NOT NULL REFERENCES tasks(id),
        reviewer_type TEXT DEFAULT 'ai' CHECK(reviewer_type IN ('ai','human','auto')),
        score REAL,
        flags TEXT DEFAULT '[]',
        recommendation TEXT CHECK(recommendation IN ('approve','flag','reject')),
        details TEXT,
        created_at TEXT DEFAULT (datetime('now'))
    );

    CREATE TABLE IF NOT EXISTS task_templates (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL,
        description TEXT,
        category TEXT NOT NULL,
        default_title TEXT,
        default_description TEXT,
        default_location_type TEXT DEFAULT 'remote',
        default_budget_type TEXT DEFAULT 'flat_fee',
        default_budget_amount REAL,
        default_required_skills TEXT DEFAULT '[]',
        is_public INTEGER DEFAULT 1,
        created_by INTEGER REFERENCES users(id),
        usage_count INTEGER DEFAULT 0,
        created_at TEXT DEFAULT (datetime('now'))
    );

    CREATE INDEX IF NOT EXISTS idx_tasks_status ON tasks(status);
    CREATE INDEX IF NOT EXISTS idx_tasks_client ON tasks(client_id);
    CREATE INDEX IF NOT EXISTS idx_tasks_worker ON tasks(worker_id);
    CREATE INDEX IF NOT EXISTS idx_tasks_category ON tasks(category);
    CREATE INDEX IF NOT EXISTS idx_api_keys_hash ON api_keys(key_hash);
    CREATE INDEX IF NOT EXISTS idx_ledger_user ON ledger(user_id);
    CREATE INDEX IF NOT EXISTS idx_audit_log_user ON audit_log(user_id);
    CREATE INDEX IF NOT EXISTS idx_flags_status ON flags(status);
    CREATE INDEX IF NOT EXISTS idx_notifications_user ON notifications(user_id);

    CREATE TABLE IF NOT EXISTS sessions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER NOT NULL REFERENCES users(id),
        token TEXT UNIQUE NOT NULL,
        expires_at TEXT NOT NULL,
        created_at TEXT DEFAULT (datetime('now'))
    );
    """)
    db.commit()
    db.close()

# ─── Rate Limiter ─────────────────────────────────────────────────────────────

_rate_limit_store = {}  # {ip: [timestamps]}
_rate_limit_lock = threading.Lock()  # HIGH-09: thread-safe rate limiter

def check_rate_limit() -> bool:
    """Returns True if request is allowed, False if rate limit exceeded."""
    # HIGH-09: use a lock to protect the shared dict under threaded gunicorn
    ip = os.environ.get("REMOTE_ADDR", "unknown")
    now = time.time()
    window = 60  # seconds
    limit = 60   # requests per window

    with _rate_limit_lock:
        # Clean up old entries
        if ip in _rate_limit_store:
            _rate_limit_store[ip] = [t for t in _rate_limit_store[ip] if now - t < window]
        else:
            _rate_limit_store[ip] = []

        if len(_rate_limit_store[ip]) >= limit:
            return False
        _rate_limit_store[ip].append(now)
        return True

# ─── Helpers ──────────────────────────────────────────────────────────────────

# ─── Content Safety System ─────────────────────────────────────────────────────
# GoHireHumans enforces strict safety standards. We are the trusted, professional
# marketplace — not the dark web. Every task is screened.

BLOCKED_KEYWORDS = [
    # Violence & weapons
    'illegal', 'weapon', 'gun', 'firearm', 'knife', 'ammunition', 'explosive',
    'bomb', 'arson', 'assault', 'attack', 'murder', 'kill', 'violent',
    # Drugs & substances
    'drug', 'narcotic', 'cocaine', 'heroin', 'meth', 'fentanyl', 'marijuana',
    'cannabis', 'weed', 'psychedelic', 'controlled substance',
    # Self-harm
    'self-harm', 'suicide', 'self harm', 'cut myself', 'end my life',
    # Hate & discrimination
    'hate speech', 'racial slur', 'racist', 'sexist', 'homophobic', 'nazi',
    'white supremac', 'hate group',
    # Sexual & adult content
    'explicit', 'adult content', 'pornograph', 'sexual', 'escort', 'companionship',
    'girlfriend experience', 'boyfriend experience', 'sugar daddy', 'sugar baby',
    'sugar mama', 'intimacy', 'intimate', 'massage with happy', 'happy ending',
    'adult entertainment', 'strip', 'cam girl', 'cam boy', 'onlyfans',
    'hookup', 'hook up', 'dating service', 'romantic', 'cuddle service',
    'body rub', 'sensual', 'erotic', 'fetish', 'dominat', 'submissive',
    'bdsm', 'lingerie model', 'nude', 'naked', 'nsfw', 'xxx',
    'sex work', 'prostitut', 'call girl', 'rentboy',
    # Terrorism & extremism
    'terroris', 'extremis', 'radicali', 'jihad', 'manifesto',
    # Cybercrime & hacking
    'hack', 'exploit', 'phishing', 'malware', 'ransomware', 'ddos',
    'social engineer', 'identity theft', 'credit card fraud', 'scam',
    'money laundering', 'counterfeit',
    # Stalking & harassment
    'stalk', 'follow someone', 'track someone', 'spy on', 'surveillance of person',
    'harass', 'intimidat', 'threaten', 'blackmail', 'extort',
    # Fraud & deception
    'fake identity', 'forge', 'counterfeit', 'impersonat', 'catfish',
    'pyramid scheme', 'ponzi',
    # Gambling (unlicensed)
    'illegal gambling', 'underground casino', 'unlicensed betting',
    # Human trafficking
    'traffic', 'smuggl', 'forced labor', 'indentured',
]

# Additional phrase patterns that require context (partial matches blocked)
BLOCKED_PHRASES = [
    'rent my body', 'rent your body', 'needs your body',
    'physical affection', 'personal company', 'keep me company',
    'be my date', 'pretend to be my', 'fake girlfriend', 'fake boyfriend',
    'no questions asked', 'off the books', 'under the table',
    'cash only', 'untraceable', 'anonymous task',
]

# Curated, professional service categories only
VALID_CATEGORIES = [
    'phone_call', 'in_person_errand', 'document_signing', 'media_capture',
    'expert_review', 'data_entry', 'research', 'writing', 'translation',
    'customer_support', 'testing', 'inspection', 'delivery',
    'virtual_assistant', 'bookkeeping', 'event_support', 'notary',
    'property_check', 'mystery_shopping', 'transcription', 'tutoring',
    'it_support', 'graphic_design', 'social_media', 'other'
]

def hash_password(password):
    salt = secrets.token_hex(16)
    h = hashlib.pbkdf2_hmac('sha256', password.encode(), salt.encode(), 100000)
    return f"{salt}:{h.hex()}"

def verify_password(password, stored):
    # MED-05: guard against malformed hash (no colon) instead of raising ValueError
    parts = stored.split(':', 1)
    if len(parts) != 2:
        return False  # Malformed hash, never matches
    salt, h = parts
    computed = hashlib.pbkdf2_hmac('sha256', password.encode(), salt.encode(), 100000)
    return hmac.compare_digest(computed.hex(), h)

def generate_api_key():
    key = f"aw_{secrets.token_hex(32)}"
    prefix = key[:10]
    key_hash = hashlib.sha256(key.encode()).hexdigest()
    return key, prefix, key_hash

def generate_session_token():
    return secrets.token_hex(32)

def json_response(data, status=200):
    print(f"Status: {status}")
    print("Content-Type: application/json")
    print()
    print(json.dumps(data, default=str))

def error_response(message, status=400):
    json_response({"error": message}, status)

def get_body():
    # Issue 8: cache stdin so it's read only once (stdin cannot be re-read).
    # Returns {} on no body, None on malformed JSON, or the parsed dict.
    if not hasattr(get_body, '_cache'):
        try:
            length = int(os.environ.get("CONTENT_LENGTH", 0) or 0)
            if length > 0:
                raw = sys.stdin.read(length)
                parsed = json.loads(raw)
                if not isinstance(parsed, dict):
                    get_body._cache = None  # non-dict body treated as bad JSON
                else:
                    get_body._cache = parsed
            else:
                get_body._cache = {}
        except json.JSONDecodeError:
            get_body._cache = None  # Callers should check: if body is None: return error_response("Invalid JSON", 400)
        except (ValueError, OSError):
            get_body._cache = None
    return get_body._cache

def get_query_params():
    qs = os.environ.get("QUERY_STRING", "")
    return dict(urllib.parse.parse_qsl(qs))

def row_to_dict(row):
    if row is None:
        return None
    return dict(row)

def authenticate_session(db):
    """Authenticate via session token in Authorization: Bearer header (preferred) or query param (legacy)."""
    # HIGH-07: prefer Authorization header; fall back to query params for backward compatibility
    token = None
    auth_header = os.environ.get("HTTP_AUTHORIZATION", "")
    if auth_header.startswith("Bearer "):
        token = auth_header[7:].strip()

    if not token:
        qs = os.environ.get("QUERY_STRING", "")
        params = dict(urllib.parse.parse_qsl(qs))
        token = params.get("token") or params.get("auth")

    if not token:
        return None

    # Session tokens stored in a simple sessions table
    row = db.execute(
        "SELECT user_id FROM sessions WHERE token = ? AND expires_at > datetime('now')",
        [token]
    ).fetchone()
    if row:
        user = db.execute("SELECT * FROM users WHERE id = ?", [row['user_id']]).fetchone()
        if user and user['is_active'] and not user['is_banned']:
            return row_to_dict(user)
    return None

def authenticate_api_key(db):
    """Authenticate via API key in X-API-Key header (preferred) or query param (legacy)."""
    # HIGH-06: prefer X-API-Key header; fall back to query param for backward compatibility
    api_key = os.environ.get("HTTP_X_API_KEY", "").strip() or None
    if not api_key:
        params = get_query_params()
        api_key = params.get("api_key")
    if not api_key:
        return None

    key_hash = hashlib.sha256(api_key.encode()).hexdigest()
    row = db.execute(
        "SELECT ak.*, u.* FROM api_keys ak JOIN users u ON ak.user_id = u.id WHERE ak.key_hash = ? AND ak.is_active = 1",
        [key_hash]
    ).fetchone()
    if row:
        db.execute("UPDATE api_keys SET last_used_at = datetime('now'), usage_count = usage_count + 1 WHERE key_hash = ?", [key_hash])
        db.commit()
        user = db.execute("SELECT * FROM users WHERE id = ?", [row['user_id']]).fetchone()
        if user and user['is_active'] and not user['is_banned']:
            return row_to_dict(user)
    return None

def authenticate(db):
    """Try session first, then API key."""
    user = authenticate_session(db)
    if not user:
        user = authenticate_api_key(db)
    return user

def audit(db, user_id, action, entity_type=None, entity_id=None, details=None):
    """Insert an audit log entry. Caller MUST call db.commit() afterward —
    this function does not commit on its own so that audit + business logic
    can be committed atomically."""
    db.execute(
        "INSERT INTO audit_log (user_id, action, entity_type, entity_id, details) VALUES (?,?,?,?,?)",
        [user_id, action, entity_type, entity_id, json.dumps(details) if details else None]
    )

def check_content_safety(text):
    """Comprehensive content safety screening.
    GoHireHumans maintains the highest trust and safety standards.
    All task content is screened before posting."""
    lower = text.lower()

    # Check single keywords
    for kw in BLOCKED_KEYWORDS:
        if kw in lower:
            return False, f"This task was not approved. Our safety review flagged prohibited content. GoHireHumans is a professional services marketplace — please review our Acceptable Use Policy."

    # Check blocked phrases
    for phrase in BLOCKED_PHRASES:
        if phrase in lower:
            return False, f"This task was not approved. Our safety review flagged prohibited content. GoHireHumans is a professional services marketplace — please review our Acceptable Use Policy."

    return True, None

def queue_webhook(db, client_id, task_id, event_type, payload):
    """Queue a webhook event for delivery."""
    profile = db.execute("SELECT webhook_url, webhook_secret FROM ai_client_profiles WHERE user_id = ?", [client_id]).fetchone()
    if profile and profile['webhook_url']:
        db.execute(
            "INSERT INTO webhook_events (client_id, task_id, event_type, payload) VALUES (?,?,?,?)",
            [client_id, task_id, event_type, json.dumps(payload)]
        )

def push_notification(db, user_id, notif_type, title, message=None, link=None):
    """Insert a notification for a user."""
    db.execute(
        "INSERT INTO notifications (user_id, type, title, message, link) VALUES (?,?,?,?,?)",
        [user_id, notif_type, title, message, link]
    )

def fake_stripe_payment_id():
    """STUB: Replace with real Stripe integration for production payments."""
    if os.environ.get("STRIPE_SECRET_KEY"):
        raise NotImplementedError(
            "Real Stripe integration not yet implemented. "
            "Remove fake_stripe_payment_id() and integrate stripe.PaymentIntent."
        )
    return f"pi_sim_{secrets.token_hex(12)}"

# ─── Route Handler ────────────────────────────────────────────────────────────

def handle_request():
    init_db()

    # Rate limiting
    if not check_rate_limit():
        print("Status: 429")
        print("Content-Type: application/json")
        print()
        print(json.dumps({"error": "Rate limit exceeded", "retry_after": 60}))
        return

    # HIGH-03: sessions table is now created in init_db(), not here per-request
    # HIGH-12: use try/finally to ensure db.close() on every code path
    db = get_db()
    try:
        _handle_routes(db)
    finally:
        db.close()


def _handle_routes(db):
    method = os.environ.get("REQUEST_METHOD", "GET")
    path = os.environ.get("PATH_INFO", "").rstrip("/")
    params = get_query_params()

    # Issue 8: centralized guard — reject malformed JSON bodies for all mutating methods
    if method in ("POST", "PUT", "PATCH") and get_body() is None:
        return error_response("Invalid JSON in request body", 400)

    # ── Auth Routes ────────────────────────────────────────────────────────

    if path == "/auth/register" and method == "POST":
        body = get_body()
        email = body.get("email", "").strip().lower()
        password = body.get("password", "")
        role = body.get("role", "worker")
        name = body.get("name", "")

        if not email or not password:
            return error_response("Email and password required")
        if len(password) < 10:  # HIGH-05: increased from 6 to 10 characters
            return error_response("Password must be at least 10 characters")
        if role not in ('worker', 'ai_client', 'admin'):
            return error_response("Invalid role")

        existing = db.execute("SELECT id FROM users WHERE email = ?", [email]).fetchone()
        if existing:
            return error_response("Email already registered", 409)

        pw_hash = hash_password(password)
        cursor = db.execute(
            "INSERT INTO users (email, password_hash, role, name) VALUES (?,?,?,?)",
            [email, pw_hash, role, name]
        )
        user_id = cursor.lastrowid

        if role == 'worker':
            skills = body.get("skills", [])
            db.execute(
                "INSERT INTO worker_profiles (user_id, skills, bio, geography, timezone) VALUES (?,?,?,?,?)",
                [user_id, json.dumps(skills), body.get("bio", ""), body.get("geography", ""), body.get("timezone", "")]
            )
        elif role == 'ai_client':
            db.execute(
                "INSERT INTO ai_client_profiles (user_id, app_name, description, billing_email) VALUES (?,?,?,?)",
                [user_id, body.get("app_name", name), body.get("description", ""), email]
            )

        token = generate_session_token()
        expires = (datetime.now(timezone.utc) + timedelta(days=7)).isoformat()
        db.execute("INSERT INTO sessions (user_id, token, expires_at) VALUES (?,?,?)", [user_id, token, expires])
        audit(db, user_id, "register", "user", user_id)
        db.commit()

        return json_response({"id": user_id, "email": email, "role": role, "name": name, "token": token}, 201)

    elif path == "/auth/login" and method == "POST":
        body = get_body()
        email = body.get("email", "").strip().lower()
        password = body.get("password", "")

        user = db.execute("SELECT * FROM users WHERE email = ?", [email]).fetchone()
        if not user or not verify_password(password, user['password_hash']):
            return error_response("Invalid credentials", 401)
        if user['is_banned']:
            return error_response("Account banned", 403)
        if user['is_suspended']:
            return error_response("Account suspended", 403)

        token = generate_session_token()
        expires = (datetime.now(timezone.utc) + timedelta(days=7)).isoformat()
        db.execute("INSERT INTO sessions (user_id, token, expires_at) VALUES (?,?,?)", [user['id'], token, expires])
        audit(db, user['id'], "login", "user", user['id'])
        db.commit()

        user_data = row_to_dict(user)
        del user_data['password_hash']
        user_data['token'] = token

        # Attach profile data
        if user['role'] == 'worker':
            profile = db.execute("SELECT * FROM worker_profiles WHERE user_id = ?", [user['id']]).fetchone()
            if profile:
                user_data['profile'] = row_to_dict(profile)
        elif user['role'] == 'ai_client':
            profile = db.execute("SELECT * FROM ai_client_profiles WHERE user_id = ?", [user['id']]).fetchone()
            if profile:
                user_data['profile'] = row_to_dict(profile)
        # Admin has no separate profile
        if user['role'] == 'admin':
            user_data['profile'] = {}

        return json_response(user_data)

    elif path == "/auth/me" and method == "GET":
        user = authenticate(db)
        if not user:
            return error_response("Unauthorized", 401)
        del user['password_hash']

        if user['role'] == 'worker':
            profile = db.execute("SELECT * FROM worker_profiles WHERE user_id = ?", [user['id']]).fetchone()
            if profile:
                user['profile'] = row_to_dict(profile)
        elif user['role'] == 'ai_client':
            profile = db.execute("SELECT * FROM ai_client_profiles WHERE user_id = ?", [user['id']]).fetchone()
            if profile:
                user['profile'] = row_to_dict(profile)
        elif user['role'] == 'admin':
            user['profile'] = {}

        return json_response(user)

    elif path == "/auth/logout" and method == "POST":
        auth_header = os.environ.get("HTTP_AUTHORIZATION", "")
        token = auth_header[7:].strip() if auth_header.startswith("Bearer ") else None
        if not token:
            token = params.get("token") or params.get("auth")  # legacy fallback
        if token:
            db.execute("DELETE FROM sessions WHERE token = ?", [token])
            db.commit()
        return json_response({"ok": True})

    # ── Google OAuth Stub ──────────────────────────────────────────────────
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # STUB: Google OAuth — NOT PRODUCTION READY
    # This is a simulated OAuth flow for development/demo purposes.
    # TODO before production:
    #   1. Register a real Google OAuth client_id and client_secret
    #   2. Exchange the auth code for tokens via Google's token endpoint
    #   3. Fetch the user's real email from the userinfo endpoint
    #   4. Store and verify the state parameter to prevent CSRF
    #   5. Use GET redirect for callback (not POST)
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    elif path == "/auth/oauth/google" and method == "GET":
        script_name = os.environ.get("SCRIPT_NAME", "/cgi-bin/api.py")
        base = os.environ.get("HTTP_HOST", "localhost")
        redirect_uri = f"https://{base}{script_name}/auth/oauth/google/callback"
        auth_url = (
            f"https://accounts.google.com/o/oauth2/v2/auth"
            f"?client_id=GOHIREHUMANS_DEMO"
            f"&redirect_uri={urllib.parse.quote(redirect_uri)}"
            f"&response_type=code"
            f"&scope=email+profile"
            f"&state={secrets.token_hex(16)}"
        )
        return json_response({"auth_url": auth_url})

    elif path == "/auth/oauth/google/callback" and method == "POST":
        body = get_body()
        code = body.get("code", secrets.token_hex(8))
        # Simulate OAuth: create or find user
        email = f"oauth_user_{code}@gmail.com"
        existing = db.execute("SELECT * FROM users WHERE email = ?", [email]).fetchone()
        if existing:
            user_row = existing
        else:
            cursor = db.execute(
                "INSERT INTO users (email, password_hash, role, name) VALUES (?,?,?,?)",
                [email, hash_password(secrets.token_hex(16)), 'worker', f"OAuth User {code[:6]}"]
            )
            uid = cursor.lastrowid
            db.execute(
                "INSERT INTO worker_profiles (user_id, skills, bio, geography, timezone) VALUES (?,?,?,?,?)",
                [uid, '[]', 'OAuth user', '', '']
            )
            user_row = db.execute("SELECT * FROM users WHERE id = ?", [uid]).fetchone()

        token = generate_session_token()
        expires = (datetime.now(timezone.utc) + timedelta(days=7)).isoformat()
        db.execute("INSERT INTO sessions (user_id, token, expires_at) VALUES (?,?,?)", [user_row['id'], token, expires])
        audit(db, user_row['id'], "oauth_login", "user", user_row['id'])
        db.commit()

        user_data = row_to_dict(user_row)
        del user_data['password_hash']
        user_data['token'] = token
        profile = db.execute("SELECT * FROM worker_profiles WHERE user_id = ?", [user_row['id']]).fetchone()
        if profile:
            user_data['profile'] = row_to_dict(profile)
        return json_response(user_data)

    # ── Profile Routes ─────────────────────────────────────────────────────

    elif path == "/profile" and method == "PUT":
        user = authenticate(db)
        if not user:
            return error_response("Unauthorized", 401)
        body = get_body()

        if body.get("name"):
            db.execute("UPDATE users SET name = ?, updated_at = datetime('now') WHERE id = ?", [body['name'], user['id']])

        if user['role'] == 'worker':
            # HIGH-04: screen bio for prohibited content before saving
            if body.get('bio'):
                safe, msg = check_content_safety(body['bio'])
                if not safe:
                    return error_response(f"Bio rejected: {msg}", 422)
            updates = []
            vals = []
            for field in ['skills', 'hourly_rate_min', 'hourly_rate_max', 'per_task_rate_min', 'per_task_rate_max', 'geography', 'timezone', 'bio', 'favorite_categories', 'notification_email']:
                if field in body:
                    val = body[field]
                    if field in ('skills', 'favorite_categories'):
                        val = json.dumps(val) if isinstance(val, list) else val
                    updates.append(f"{field} = ?")
                    vals.append(val)
            if updates:
                vals.append(user['id'])
                db.execute(f"UPDATE worker_profiles SET {', '.join(updates)} WHERE user_id = ?", vals)

        elif user['role'] == 'ai_client':
            updates = []
            vals = []
            for field in ['app_name', 'description', 'callback_urls', 'webhook_url', 'webhook_secret', 'billing_email']:
                if field in body:
                    val = body[field]
                    if field == 'callback_urls':
                        val = json.dumps(val) if isinstance(val, list) else val
                    updates.append(f"{field} = ?")
                    vals.append(val)
            if updates:
                vals.append(user['id'])
                db.execute(f"UPDATE ai_client_profiles SET {', '.join(updates)} WHERE user_id = ?", vals)

        audit(db, user['id'], "update_profile", "user", user['id'])
        db.commit()
        return json_response({"ok": True})

    # ── API Key Management ─────────────────────────────────────────────────

    elif path == "/api/v1/clients/api-keys" and method == "POST":
        user = authenticate(db)
        if not user:
            return error_response("Unauthorized", 401)
        if user['role'] != 'ai_client':
            return error_response("Only AI clients can create API keys", 403)

        body = get_body()
        key, prefix, key_hash = generate_api_key()
        db.execute(
            "INSERT INTO api_keys (user_id, key_prefix, key_hash, name) VALUES (?,?,?,?)",
            [user['id'], prefix, key_hash, body.get("name", "Default")]
        )
        audit(db, user['id'], "create_api_key", "api_key", None)
        db.commit()

        return json_response({"api_key": key, "prefix": prefix, "name": body.get("name", "Default")}, 201)

    elif path == "/api/v1/clients/api-keys" and method == "GET":
        user = authenticate(db)
        if not user:
            return error_response("Unauthorized", 401)
        if user['role'] != 'ai_client':
            return error_response("Only AI clients can view API keys", 403)

        keys = db.execute(
            "SELECT id, key_prefix, name, created_at, last_used_at, is_active, usage_count FROM api_keys WHERE user_id = ?",
            [user['id']]
        ).fetchall()

        return json_response([row_to_dict(k) for k in keys])

    elif re.match(r"^/api/v1/clients/api-keys/\d+/revoke$", path) and method == "POST":
        user = authenticate(db)
        if not user:
            return error_response("Unauthorized", 401)
        key_id = int(path.split("/")[5])
        db.execute("UPDATE api_keys SET is_active = 0 WHERE id = ? AND user_id = ?", [key_id, user['id']])
        audit(db, user['id'], "revoke_api_key", "api_key", key_id)
        db.commit()
        return json_response({"ok": True})

    # ── Task CRUD ──────────────────────────────────────────────────────────

    elif path == "/api/v1/tasks" and method == "POST":
        user = authenticate(db)
        if not user:
            return error_response("Unauthorized", 401)
        if user['role'] not in ('ai_client', 'admin'):
            return error_response("Only AI clients can create tasks", 403)

        body = get_body()

        # Validate required fields
        for field in ['title', 'description', 'category', 'location_type', 'budget_type', 'budget_amount']:
            if not body.get(field):
                return error_response(f"Missing required field: {field}")

        if body['category'] not in VALID_CATEGORIES:
            return error_response(f"Invalid category. Must be one of: {', '.join(VALID_CATEGORIES)}")

        if body['location_type'] not in ('remote', 'specific_address', 'region'):
            return error_response("Invalid location_type")

        if body['budget_type'] not in ('flat_fee', 'hourly'):
            return error_response("Invalid budget_type")

        # Validate budget amount (MED-09: add max cap)
        budget = float(body['budget_amount'])
        if budget <= 0:
            return error_response("budget_amount must be positive")
        if budget > 100000:
            return error_response("budget_amount exceeds maximum allowed value ($100,000)")

        # Content safety
        safe, msg = check_content_safety(body['title'] + " " + body['description'])
        if not safe:
            return error_response(f"Task rejected: {msg}", 422)

        # Idempotency check
        idempotency_key = body.get("idempotency_key")
        if idempotency_key:
            existing = db.execute("SELECT id FROM tasks WHERE idempotency_key = ?", [idempotency_key]).fetchone()
            if existing:
                task = db.execute("SELECT * FROM tasks WHERE id = ?", [existing['id']]).fetchone()
                return json_response(row_to_dict(task))

        status = body.get("status", "open")
        if status not in ('draft', 'open'):
            status = 'open'

        cursor = db.execute(
            """INSERT INTO tasks (client_id, title, description, category, location_type, location_detail,
            budget_type, budget_amount, time_cap_hours, due_by, required_skills, attachments, status, idempotency_key)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            [user['id'], body['title'], body['description'], body['category'],
             body['location_type'], body.get('location_detail', ''),
             body['budget_type'], budget,
             body.get('time_cap_hours'), body.get('due_by'),
             json.dumps(body.get('required_skills', [])),
             json.dumps(body.get('attachments', [])),
             status, idempotency_key]
        )
        task_id = cursor.lastrowid

        # CRIT-01: Atomic balance deduction — use UPDATE...WHERE >= to prevent race conditions / negative balance
        rows_updated = db.execute(
            "UPDATE ai_client_profiles SET credit_balance = credit_balance - ? "
            "WHERE user_id = ? AND credit_balance >= ?",
            [budget, user['id'], budget]
        ).rowcount
        if rows_updated == 0:
            db.rollback()
            return error_response("Insufficient credit balance", 402)
        new_balance_row = db.execute("SELECT credit_balance FROM ai_client_profiles WHERE user_id = ?", [user['id']]).fetchone()
        new_balance = new_balance_row['credit_balance'] if new_balance_row else 0
        db.execute(
            "INSERT INTO ledger (user_id, task_id, entry_type, amount, balance_after, description) VALUES (?,?,?,?,?,?)",
            [user['id'], task_id, 'debit', budget, new_balance, f"Task #{task_id}: {body['title']}"]
        )

        # Queue webhook
        queue_webhook(db, user['id'], task_id, "task.created", {
            "event": "task.created", "task_id": task_id, "title": body['title'],
            "status": status, "created_at": datetime.now(timezone.utc).isoformat()
        })

        audit(db, user['id'], "create_task", "task", task_id)
        db.commit()

        task = db.execute("SELECT * FROM tasks WHERE id = ?", [task_id]).fetchone()
        return json_response(row_to_dict(task), 201)

    elif path == "/api/v1/tasks" and method == "GET":
        user = authenticate(db)
        if not user:
            return error_response("Unauthorized", 401)

        status = params.get("status")
        category = params.get("category")
        location_type = params.get("location_type")
        skills = params.get("skills")
        search = params.get("search")
        near = params.get("near")
        page = int(params.get("page", 1))
        per_page = min(int(params.get("per_page", 20)), 100)
        offset = (page - 1) * per_page

        conditions = []
        values = []

        if user['role'] == 'ai_client':
            conditions.append("t.client_id = ?")
            values.append(user['id'])
        elif user['role'] == 'worker':
            # Workers see open tasks or their own tasks
            conditions.append("(t.status = 'open' OR t.worker_id = ?)")
            values.append(user['id'])

        if status:
            conditions.append("t.status = ?")
            values.append(status)
        if category:
            conditions.append("t.category = ?")
            values.append(category)
        if location_type:
            conditions.append("t.location_type = ?")
            values.append(location_type)
        if search:
            conditions.append("(t.title LIKE ? OR t.description LIKE ?)")
            values.extend([f"%{search}%", f"%{search}%"])

        where = " AND ".join(conditions) if conditions else "1=1"

        count = db.execute(f"SELECT COUNT(*) as c FROM tasks t WHERE {where}", values).fetchone()['c']
        tasks = db.execute(
            f"""SELECT t.*, u.name as client_name, u.email as client_email
            FROM tasks t JOIN users u ON t.client_id = u.id
            WHERE {where} ORDER BY t.created_at DESC LIMIT ? OFFSET ?""",
            values + [per_page, offset]
        ).fetchall()

        task_list = []
        for t in tasks:
            td = row_to_dict(t)
            # Geographic proximity matching
            if near:
                loc_detail = (td.get('location_detail') or '').lower()
                td['proximity_match'] = near.lower() in loc_detail
            task_list.append(td)

        return json_response({
            "tasks": task_list,
            "total": count,
            "page": page,
            "per_page": per_page,
            "total_pages": (count + per_page - 1) // per_page
        })

    elif re.match(r"^/api/v1/tasks/\d+$", path) and method == "GET":
        user = authenticate(db)
        if not user:
            return error_response("Unauthorized", 401)

        task_id = int(path.split("/")[-1])
        task = db.execute(
            """SELECT t.*, u.name as client_name, u.email as client_email
            FROM tasks t JOIN users u ON t.client_id = u.id WHERE t.id = ?""",
            [task_id]
        ).fetchone()

        if not task:
            return error_response("Task not found", 404)

        # Access control: client owns it, worker assigned to it, or admin, or task is open and user is worker
        if user['role'] == 'ai_client' and task['client_id'] != user['id']:
            return error_response("Forbidden", 403)
        if user['role'] == 'worker' and task['status'] not in ('open',) and task['worker_id'] != user['id']:
            return error_response("Forbidden", 403)

        result = row_to_dict(task)

        # Include ratings
        ratings = db.execute("SELECT * FROM ratings WHERE task_id = ?", [task_id]).fetchall()
        result['ratings'] = [row_to_dict(r) for r in ratings]

        # If worker assigned, include worker info
        if task['worker_id']:
            worker = db.execute("SELECT id, name, email FROM users WHERE id = ?", [task['worker_id']]).fetchone()
            if worker:
                result['worker'] = row_to_dict(worker)

        return json_response(result)

    elif re.match(r"^/api/v1/tasks/\d+/accept$", path) and method == "POST":
        user = authenticate(db)
        if not user:
            return error_response("Unauthorized", 401)
        if user['role'] != 'worker':
            return error_response("Only workers can accept tasks", 403)

        task_id = int(path.split("/")[4])
        task = db.execute("SELECT * FROM tasks WHERE id = ?", [task_id]).fetchone()
        if not task:
            return error_response("Task not found", 404)

        # HIGH-11: use atomic UPDATE...WHERE status='open' to prevent double-accept race condition
        reserved_until = (datetime.now(timezone.utc) + timedelta(minutes=30)).isoformat()
        rows_updated = db.execute(
            "UPDATE tasks SET worker_id = ?, status = 'in_progress', reserved_until = ?, updated_at = datetime('now') "
            "WHERE id = ? AND status = 'open'",
            [user['id'], reserved_until, task_id]
        ).rowcount
        if rows_updated == 0:
            db.rollback()
            return error_response("Task is no longer available (already accepted by another worker)", 409)
        # Re-fetch task to get client_id for notifications
        task = db.execute("SELECT * FROM tasks WHERE id = ?", [task_id]).fetchone()

        queue_webhook(db, task['client_id'], task_id, "task.accepted", {
            "event": "task.accepted", "task_id": task_id,
            "worker_id": user['id'], "worker_name": user['name']
        })

        # Notify client
        push_notification(db, task['client_id'], "task_accepted",
            f"Task accepted: {task['title']}",
            f"Worker {user['name']} accepted your task.",
            f"/api/v1/tasks/{task_id}")

        audit(db, user['id'], "accept_task", "task", task_id)
        db.commit()
        return json_response({"ok": True, "status": "in_progress"})

    elif re.match(r"^/api/v1/tasks/\d+/submit$", path) and method == "POST":
        user = authenticate(db)
        if not user:
            return error_response("Unauthorized", 401)
        if user['role'] != 'worker':
            return error_response("Only workers can submit deliverables", 403)

        task_id = int(path.split("/")[4])
        task = db.execute("SELECT * FROM tasks WHERE id = ? AND worker_id = ?", [task_id, user['id']]).fetchone()
        if not task:
            return error_response("Task not found or not assigned to you", 404)
        if task['status'] not in ('in_progress', 'reserved'):
            return error_response("Task cannot be submitted in current state", 409)

        body = get_body()
        db.execute(
            """UPDATE tasks SET status = 'submitted', deliverables_text = ?, deliverables_files = ?,
            deliverables_data = ?, updated_at = datetime('now') WHERE id = ?""",
            [body.get('text', ''), json.dumps(body.get('files', [])),
             json.dumps(body.get('data', {})), task_id]
        )

        queue_webhook(db, task['client_id'], task_id, "task.submitted", {
            "event": "task.submitted", "task_id": task_id, "worker_id": user['id']
        })

        # Notify client
        push_notification(db, task['client_id'], "task_submitted",
            f"Deliverables submitted: {task['title']}",
            f"Worker {user['name']} submitted deliverables for review.",
            f"/api/v1/tasks/{task_id}")

        audit(db, user['id'], "submit_task", "task", task_id)
        db.commit()
        return json_response({"ok": True, "status": "submitted"})

    elif re.match(r"^/api/v1/tasks/\d+/complete$", path) and method == "POST":
        user = authenticate(db)
        if not user:
            return error_response("Unauthorized", 401)

        task_id = int(path.split("/")[4])
        task = db.execute("SELECT * FROM tasks WHERE id = ?", [task_id]).fetchone()
        if not task:
            return error_response("Task not found", 404)
        if task['client_id'] != user['id'] and user['role'] != 'admin':
            return error_response("Only the task owner can mark complete", 403)
        if task['status'] != 'submitted':
            return error_response("Task must be in submitted state", 409)

        db.execute(
            "UPDATE tasks SET status = 'completed', completed_at = datetime('now'), updated_at = datetime('now') WHERE id = ?",
            [task_id]
        )

        # Credit worker earnings
        if task['worker_id']:
            wp = db.execute("SELECT * FROM worker_profiles WHERE user_id = ?", [task['worker_id']]).fetchone()
            # TODO: Track cumulative platform fees in a dedicated table/column for revenue reporting.
            # Currently, service_fee is only recorded in individual ledger entries.
            service_fee = round(task['budget_amount'] * SERVICE_FEE_RATE, 2)
            worker_payout = task['budget_amount'] - service_fee
            if wp:
                new_earnings = (wp['estimated_earnings'] or 0) + worker_payout
                new_withdrawable = (wp['withdrawable_balance'] or 0) + worker_payout
                new_completed = (wp['total_tasks_completed'] or 0) + 1
                db.execute(
                    "UPDATE worker_profiles SET estimated_earnings = ?, withdrawable_balance = ?, total_tasks_completed = ? WHERE user_id = ?",
                    [new_earnings, new_withdrawable, new_completed, task['worker_id']]
                )
                db.execute(
                    "INSERT INTO ledger (user_id, task_id, entry_type, amount, balance_after, description) VALUES (?,?,?,?,?,?)",
                    [task['worker_id'], task_id, 'credit', worker_payout, new_withdrawable, f"Payment for task #{task_id} (1% fee: ${service_fee:.2f})"]
                )
                db.execute(
                    "INSERT INTO ledger (user_id, task_id, entry_type, amount, balance_after, description) VALUES (?,?,?,?,?,?)",
                    [task['client_id'], task_id, 'fee', service_fee, None, f"Platform fee (1%) for task #{task_id}"]
                )
            # CRIT-03: notify worker with correct worker_payout amount (not full budget_amount)
            push_notification(db, task['worker_id'], "task_completed",
                "Task completed & payment released",
                f"Task #{task_id} approved. ${worker_payout:.2f} added to your balance (after 1% platform fee).",
                f"/api/v1/tasks/{task_id}")

        # Update client spent
        db.execute(
            "UPDATE ai_client_profiles SET total_spent = total_spent + ? WHERE user_id = ?",
            [task['budget_amount'], task['client_id']]
        )

        queue_webhook(db, task['client_id'], task_id, "task.completed", {
            "event": "task.completed", "task_id": task_id, "worker_id": task['worker_id']
        })

        audit(db, user['id'], "complete_task", "task", task_id)
        db.commit()
        return json_response({"ok": True, "status": "completed"})

    elif re.match(r"^/api/v1/tasks/\d+/cancel$", path) and method == "POST":
        user = authenticate(db)
        if not user:
            return error_response("Unauthorized", 401)

        task_id = int(path.split("/")[4])
        task = db.execute("SELECT * FROM tasks WHERE id = ?", [task_id]).fetchone()
        if not task:
            return error_response("Task not found", 404)
        if task['client_id'] != user['id'] and user['role'] != 'admin':
            return error_response("Only the task owner can cancel", 403)
        if task['status'] in ('completed', 'canceled'):
            return error_response("Task cannot be canceled in current state", 409)

        db.execute(
            "UPDATE tasks SET status = 'canceled', canceled_at = datetime('now'), updated_at = datetime('now') WHERE id = ?",
            [task_id]
        )

        # Refund client
        db.execute(
            "UPDATE ai_client_profiles SET credit_balance = credit_balance + ? WHERE user_id = ?",
            [task['budget_amount'], task['client_id']]
        )
        new_balance_row = db.execute(
            "SELECT credit_balance FROM ai_client_profiles WHERE user_id = ?",
            [task['client_id']]
        ).fetchone()
        new_balance = new_balance_row['credit_balance'] if new_balance_row else 0
        db.execute(
            "INSERT INTO ledger (user_id, task_id, entry_type, amount, balance_after, description) VALUES (?,?,?,?,?,?)",
            [task['client_id'], task_id, 'credit', task['budget_amount'], new_balance, f"Refund for canceled task #{task_id}"]
        )

        queue_webhook(db, task['client_id'], task_id, "task.canceled", {
            "event": "task.canceled", "task_id": task_id
        })

        # Notify worker if assigned
        if task['worker_id']:
            push_notification(db, task['worker_id'], "task_canceled",
                f"Task canceled: {task['title']}",
                "A task you accepted was canceled by the client.",
                f"/api/v1/tasks/{task_id}")

        audit(db, user['id'], "cancel_task", "task", task_id)
        db.commit()
        return json_response({"ok": True, "status": "canceled"})

    elif re.match(r"^/api/v1/tasks/\d+/dispute$", path) and method == "POST":
        user = authenticate(db)
        if not user:
            return error_response("Unauthorized", 401)

        task_id = int(path.split("/")[4])
        task = db.execute("SELECT * FROM tasks WHERE id = ?", [task_id]).fetchone()
        if not task:
            return error_response("Task not found", 404)

        # CRIT-06: only task participants (client, worker) or admins can open a dispute
        if user['id'] not in (task['client_id'], task['worker_id']) and user['role'] != 'admin':
            return error_response("Only task participants can open a dispute", 403)
        if task['status'] not in ('in_progress', 'submitted', 'reserved'):
            return error_response("Can only dispute active tasks", 409)

        body = get_body()
        db.execute(
            "UPDATE tasks SET status = 'disputed', updated_at = datetime('now') WHERE id = ?",
            [task_id]
        )

        queue_webhook(db, task['client_id'], task_id, "task.disputed", {
            "event": "task.disputed", "task_id": task_id, "reason": body.get("reason", "")
        })

        # Notify both parties
        if task['worker_id'] and task['worker_id'] != user['id']:
            push_notification(db, task['worker_id'], "task_disputed",
                f"Dispute opened: {task['title']}",
                f"A dispute was opened on task #{task_id}.",
                f"/api/v1/tasks/{task_id}")
        if task['client_id'] != user['id']:
            push_notification(db, task['client_id'], "task_disputed",
                f"Dispute opened: {task['title']}",
                f"A dispute was opened on task #{task_id}.",
                f"/api/v1/tasks/{task_id}")

        audit(db, user['id'], "dispute_task", "task", task_id, {"reason": body.get("reason", "")})
        db.commit()
        return json_response({"ok": True, "status": "disputed"})

    # ── AI Task Matching / Recommendations ────────────────────────────────

    elif path == "/api/v1/tasks/recommended" and method == "GET":
        user = authenticate(db)
        if not user:
            return error_response("Unauthorized", 401)
        if user['role'] != 'worker':
            return error_response("Only workers can get recommendations", 403)

        wp = db.execute("SELECT skills, geography FROM worker_profiles WHERE user_id = ?", [user['id']]).fetchone()
        if not wp:
            return json_response({"tasks": [], "total": 0})

        try:
            worker_skills = set(json.loads(wp['skills'] or '[]'))
        except (json.JSONDecodeError, ValueError):  # HIGH-02: catch only specific exceptions
            worker_skills = set()
        worker_geo = (wp['geography'] or '').lower()

        open_tasks = db.execute(
            """SELECT t.*, u.name as client_name FROM tasks t
            JOIN users u ON t.client_id = u.id
            WHERE t.status = 'open'
            ORDER BY t.created_at DESC LIMIT 200"""
        ).fetchall()

        scored = []
        for t in open_tasks:
            td = row_to_dict(t)
            try:
                req_skills = set(json.loads(t['required_skills'] or '[]'))
            except (json.JSONDecodeError, ValueError):  # HIGH-02: catch only specific exceptions
                req_skills = set()

            overlap = len(worker_skills & req_skills)
            skill_score = overlap * 2

            geo_bonus = 0
            match_reasons = []
            loc_detail = (t['location_detail'] or '').lower()

            if overlap > 0:
                match_reasons.append(f"Skill match: {', '.join(worker_skills & req_skills)}")

            if worker_geo and worker_geo != 'remote':
                city = worker_geo.split(',')[0].strip().lower()
                if city and (city in loc_detail or city in (t['location_type'] or '').lower()):
                    geo_bonus = 1
                    match_reasons.append(f"Location match: {worker_geo}")

            if t['location_type'] == 'remote':
                geo_bonus = 0.5
                match_reasons.append("Remote task")

            match_score = round(skill_score + geo_bonus * 1.5, 2)
            if match_score > 0 or not req_skills:
                td['match_score'] = match_score
                td['match_reasons'] = match_reasons
                scored.append(td)

        scored.sort(key=lambda x: x['match_score'], reverse=True)
        top = scored[:20]
        return json_response({"tasks": top, "total": len(top)})

    # ── Batch Task Creation ────────────────────────────────────────────────

    elif path == "/api/v1/tasks/batch" and method == "POST":
        user = authenticate(db)
        if not user:
            return error_response("Unauthorized", 401)
        if user['role'] not in ('ai_client', 'admin'):
            return error_response("Only AI clients can create tasks", 403)

        body = get_body()
        tasks_input = body.get("tasks", [])
        template_id = body.get("template_id")

        if not tasks_input or not isinstance(tasks_input, list):
            return error_response("tasks array required")
        if len(tasks_input) > 50:
            return error_response("Maximum 50 tasks per batch")

        # Optionally load template defaults
        template_defaults = {}
        if template_id:
            tmpl = db.execute("SELECT * FROM task_templates WHERE id = ?", [template_id]).fetchone()
            if tmpl:
                template_defaults = {
                    'category': tmpl['category'],
                    'location_type': tmpl['default_location_type'],
                    'budget_type': tmpl['default_budget_type'],
                    'budget_amount': tmpl['default_budget_amount'],
                    'required_skills': json.loads(tmpl['default_required_skills'] or '[]'),
                }

        created_ids = []
        errors = []

        for idx, task_input in enumerate(tasks_input):
            try:
                db.execute("SAVEPOINT batch_task")
                merged = {**template_defaults, **task_input}
                for field in ['title', 'description', 'category', 'location_type', 'budget_type', 'budget_amount']:
                    if not merged.get(field):
                        raise ValueError(f"Missing required field: {field}")
                if merged['category'] not in VALID_CATEGORIES:
                    raise ValueError(f"Invalid category: {merged['category']}")
                if merged['location_type'] not in ('remote', 'specific_address', 'region'):
                    raise ValueError("Invalid location_type")
                if merged['budget_type'] not in ('flat_fee', 'hourly'):
                    raise ValueError("Invalid budget_type")
                safe, msg = check_content_safety(merged['title'] + " " + merged['description'])
                if not safe:
                    raise ValueError(f"Content rejected: {msg}")

                status = merged.get("status", "open")
                if status not in ('draft', 'open'):
                    status = 'open'

                cursor = db.execute(
                    """INSERT INTO tasks (client_id, title, description, category, location_type, location_detail,
                    budget_type, budget_amount, time_cap_hours, due_by, required_skills, attachments, status)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                    [user['id'], merged['title'], merged['description'], merged['category'],
                     merged['location_type'], merged.get('location_detail', ''),
                     merged['budget_type'], float(merged['budget_amount']),
                     merged.get('time_cap_hours'), merged.get('due_by'),
                     json.dumps(merged.get('required_skills', [])),
                     json.dumps(merged.get('attachments', [])),
                     status]
                )
                task_id = cursor.lastrowid

                # CRIT-01: atomic balance deduction — prevent race condition / negative balance in batch
                batch_budget = float(merged['budget_amount'])
                batch_rows_updated = db.execute(
                    "UPDATE ai_client_profiles SET credit_balance = credit_balance - ? "
                    "WHERE user_id = ? AND credit_balance >= ?",
                    [batch_budget, user['id'], batch_budget]
                ).rowcount
                if batch_rows_updated == 0:
                    raise ValueError("Insufficient credit balance for this task")
                batch_balance_row = db.execute("SELECT credit_balance FROM ai_client_profiles WHERE user_id = ?", [user['id']]).fetchone()
                batch_new_balance = batch_balance_row['credit_balance'] if batch_balance_row else 0
                db.execute(
                    "INSERT INTO ledger (user_id, task_id, entry_type, amount, balance_after, description) VALUES (?,?,?,?,?,?)",
                    [user['id'], task_id, 'debit', batch_budget, batch_new_balance, f"Batch task #{task_id}"]
                )

                if template_id:
                    db.execute("UPDATE task_templates SET usage_count = usage_count + 1 WHERE id = ?", [template_id])

                db.execute("RELEASE SAVEPOINT batch_task")
                created_ids.append(task_id)
            except Exception as e:
                db.execute("ROLLBACK TO SAVEPOINT batch_task")
                db.execute("RELEASE SAVEPOINT batch_task")
                errors.append({"index": idx, "error": str(e)})

        audit(db, user['id'], "batch_create_tasks", "task", None, {"count": len(created_ids)})
        db.commit()
        return json_response({
            "created": created_ids,
            "errors": errors,
            "total_created": len(created_ids),
            "total_failed": len(errors)
        }, 201)

    # ── Task Templates ─────────────────────────────────────────────────────

    elif path == "/api/v1/templates" and method == "GET":
        user = authenticate(db)
        if not user:
            return error_response("Unauthorized", 401)

        templates = db.execute(
            """SELECT * FROM task_templates
            WHERE is_public = 1 OR created_by = ?
            ORDER BY usage_count DESC""",
            [user['id']]
        ).fetchall()
        return json_response([row_to_dict(t) for t in templates])

    elif re.match(r"^/api/v1/templates/\d+$", path) and method == "GET":
        user = authenticate(db)
        if not user:
            return error_response("Unauthorized", 401)

        tmpl_id = int(path.split("/")[-1])
        tmpl = db.execute("SELECT * FROM task_templates WHERE id = ?", [tmpl_id]).fetchone()
        if not tmpl:
            return error_response("Template not found", 404)
        if not tmpl['is_public'] and tmpl['created_by'] != user['id'] and user['role'] != 'admin':
            return error_response("Forbidden", 403)
        return json_response(row_to_dict(tmpl))

    elif path == "/api/v1/templates" and method == "POST":
        user = authenticate(db)
        if not user:
            return error_response("Unauthorized", 401)
        if user['role'] not in ('ai_client', 'admin'):
            return error_response("Only AI clients and admins can create templates", 403)

        body = get_body()
        for field in ['name', 'category']:
            if not body.get(field):
                return error_response(f"Missing required field: {field}")
        if body['category'] not in VALID_CATEGORIES:
            return error_response(f"Invalid category")

        cursor = db.execute(
            """INSERT INTO task_templates (name, description, category, default_title, default_description,
            default_location_type, default_budget_type, default_budget_amount, default_required_skills,
            is_public, created_by)
            VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
            [body['name'], body.get('description', ''), body['category'],
             body.get('default_title', ''), body.get('default_description', ''),
             body.get('default_location_type', 'remote'),
             body.get('default_budget_type', 'flat_fee'),
             body.get('default_budget_amount'),
             json.dumps(body.get('default_required_skills', [])),
             1 if body.get('is_public', True) else 0,
             user['id']]
        )
        tmpl_id = cursor.lastrowid
        db.commit()
        tmpl = db.execute("SELECT * FROM task_templates WHERE id = ?", [tmpl_id]).fetchone()
        return json_response(row_to_dict(tmpl), 201)

    elif re.match(r"^/api/v1/tasks/from-template/\d+$", path) and method == "POST":
        user = authenticate(db)
        if not user:
            return error_response("Unauthorized", 401)
        if user['role'] not in ('ai_client', 'admin'):
            return error_response("Only AI clients can create tasks", 403)

        template_id = int(path.split("/")[-1])
        tmpl = db.execute("SELECT * FROM task_templates WHERE id = ?", [template_id]).fetchone()
        if not tmpl:
            return error_response("Template not found", 404)

        body = get_body()
        # Merge template defaults with overrides
        task_data = {
            'title': body.get('title') or tmpl['default_title'] or tmpl['name'],
            'description': body.get('description') or tmpl['default_description'] or '',
            'category': body.get('category') or tmpl['category'],
            'location_type': body.get('location_type') or tmpl['default_location_type'],
            'location_detail': body.get('location_detail', ''),
            'budget_type': body.get('budget_type') or tmpl['default_budget_type'],
            'budget_amount': body.get('budget_amount') or tmpl['default_budget_amount'] or 0,
            'required_skills': body.get('required_skills') or json.loads(tmpl['default_required_skills'] or '[]'),
            'status': body.get('status', 'open'),
        }

        for field in ['title', 'description']:
            if not task_data.get(field):
                return error_response(f"Missing required field: {field}")

        status = task_data['status']
        if status not in ('draft', 'open'):
            status = 'open'

        safe, msg = check_content_safety(task_data['title'] + " " + task_data['description'])
        if not safe:
            return error_response(f"Task rejected: {msg}", 422)

        cursor = db.execute(
            """INSERT INTO tasks (client_id, title, description, category, location_type, location_detail,
            budget_type, budget_amount, required_skills, status)
            VALUES (?,?,?,?,?,?,?,?,?,?)""",
            [user['id'], task_data['title'], task_data['description'],
             task_data['category'], task_data['location_type'], task_data['location_detail'],
             task_data['budget_type'], float(task_data['budget_amount']),
             json.dumps(task_data['required_skills']), status]
        )
        task_id = cursor.lastrowid

        # CRIT-01: atomic balance deduction for from-template task creation
        tmpl_budget = float(task_data['budget_amount'])
        tmpl_rows_updated = db.execute(
            "UPDATE ai_client_profiles SET credit_balance = credit_balance - ? "
            "WHERE user_id = ? AND credit_balance >= ?",
            [tmpl_budget, user['id'], tmpl_budget]
        ).rowcount
        if tmpl_rows_updated == 0:
            db.rollback()
            return error_response("Insufficient credit balance", 402)
        tmpl_balance_row = db.execute("SELECT credit_balance FROM ai_client_profiles WHERE user_id = ?", [user['id']]).fetchone()
        tmpl_new_balance = tmpl_balance_row['credit_balance'] if tmpl_balance_row else 0
        db.execute(
            "INSERT INTO ledger (user_id, task_id, entry_type, amount, balance_after, description) VALUES (?,?,?,?,?,?)",
            [user['id'], task_id, 'debit', tmpl_budget, tmpl_new_balance, f"Task from template #{template_id}"]
        )

        db.execute("UPDATE task_templates SET usage_count = usage_count + 1 WHERE id = ?", [template_id])
        audit(db, user['id'], "create_task_from_template", "task", task_id, {"template_id": template_id})
        db.commit()

        task = db.execute("SELECT * FROM tasks WHERE id = ?", [task_id]).fetchone()
        return json_response(row_to_dict(task), 201)

    # ── Quality Review ─────────────────────────────────────────────────────

    elif re.match(r"^/api/v1/tasks/\d+/quality-review$", path) and method == "POST":
        user = authenticate(db)
        if not user:
            return error_response("Unauthorized", 401)
        if user['role'] != 'admin':
            return error_response("Admin access required", 403)

        task_id = int(path.split("/")[4])
        task = db.execute("SELECT * FROM tasks WHERE id = ?", [task_id]).fetchone()
        if not task:
            return error_response("Task not found", 404)

        score = 0
        flags = []
        details_parts = []

        # Check deliverables_text length > 50
        dl_text = task['deliverables_text'] or ''
        if len(dl_text) > 50:
            score += 30
            details_parts.append("Deliverables text is substantive (+30)")
        else:
            flags.append("deliverables_text_too_short")
            details_parts.append("Deliverables text too short or missing (+0)")

        # Check deliverables_files has entries
        try:
            dl_files = json.loads(task['deliverables_files'] or '[]')
        except (json.JSONDecodeError, ValueError):  # HIGH-02
            dl_files = []
        if dl_files:
            score += 20
            details_parts.append("Files attached (+20)")
        else:
            flags.append("no_deliverable_files")

        # Check deliverables_data is not empty
        try:
            dl_data = json.loads(task['deliverables_data'] or '{}')
        except (json.JSONDecodeError, ValueError):  # HIGH-02
            dl_data = {}
        if dl_data:
            score += 20
            details_parts.append("Structured data provided (+20)")
        else:
            flags.append("no_deliverable_data")

        # Check completed within time cap
        if task['completed_at'] and task['created_at'] and task['time_cap_hours']:
            try:
                created = datetime.fromisoformat(task['created_at'].replace('Z', '+00:00'))
                completed = datetime.fromisoformat(task['completed_at'].replace('Z', '+00:00'))
                elapsed_hours = (completed - created).total_seconds() / 3600
                if elapsed_hours <= task['time_cap_hours']:
                    score += 30
                    details_parts.append(f"Completed within time cap ({elapsed_hours:.1f}h <= {task['time_cap_hours']}h) (+30)")
                else:
                    flags.append("exceeded_time_cap")
                    details_parts.append(f"Exceeded time cap (+0)")
            except (ValueError, TypeError):  # HIGH-02
                score += 15
                details_parts.append("Time check inconclusive (+15)")
        else:
            score += 15
            details_parts.append("No time cap set; partial credit (+15)")

        if score >= 70:
            recommendation = "approve"
        elif score >= 40:
            recommendation = "flag"
        else:
            recommendation = "reject"

        # Remove existing review for this task if any
        db.execute("DELETE FROM quality_reviews WHERE task_id = ?", [task_id])

        cursor = db.execute(
            """INSERT INTO quality_reviews (task_id, reviewer_type, score, flags, recommendation, details)
            VALUES (?,?,?,?,?,?)""",
            [task_id, 'ai', score, json.dumps(flags), recommendation, "; ".join(details_parts)]
        )
        review_id = cursor.lastrowid
        db.commit()

        review = db.execute("SELECT * FROM quality_reviews WHERE id = ?", [review_id]).fetchone()
        return json_response(row_to_dict(review), 201)

    elif re.match(r"^/api/v1/tasks/\d+/quality-review$", path) and method == "GET":
        user = authenticate(db)
        if not user:
            return error_response("Unauthorized", 401)

        task_id = int(path.split("/")[4])
        review = db.execute("SELECT * FROM quality_reviews WHERE task_id = ?", [task_id]).fetchone()
        if not review:
            return error_response("No quality review found for this task", 404)
        return json_response(row_to_dict(review))

    # ── Ratings ────────────────────────────────────────────────────────────

    elif re.match(r"^/api/v1/tasks/\d+/rate$", path) and method == "POST":
        user = authenticate(db)
        if not user:
            return error_response("Unauthorized", 401)

        task_id = int(path.split("/")[4])
        task = db.execute("SELECT * FROM tasks WHERE id = ?", [task_id]).fetchone()
        if not task:
            return error_response("Task not found", 404)
        if task['status'] != 'completed':
            return error_response("Can only rate completed tasks", 409)

        body = get_body()
        rating = body.get("rating")
        if not rating or rating < 1 or rating > 5:
            return error_response("Rating must be 1-5")

        # Determine who is being rated
        if user['id'] == task['client_id']:
            to_user_id = task['worker_id']
        elif user['id'] == task['worker_id']:
            to_user_id = task['client_id']
        else:
            return error_response("Only task participants can rate", 403)

        # Check no duplicate
        existing = db.execute(
            "SELECT id FROM ratings WHERE task_id = ? AND from_user_id = ?",
            [task_id, user['id']]
        ).fetchone()
        if existing:
            return error_response("Already rated for this task", 409)

        db.execute(
            "INSERT INTO ratings (task_id, from_user_id, to_user_id, rating, review) VALUES (?,?,?,?,?)",
            [task_id, user['id'], to_user_id, rating, body.get("review", "")]
        )

        # Update average rating
        rated_user = db.execute("SELECT role FROM users WHERE id = ?", [to_user_id]).fetchone()
        if rated_user:
            avg = db.execute("SELECT AVG(rating) as avg, COUNT(*) as cnt FROM ratings WHERE to_user_id = ?", [to_user_id]).fetchone()
            if rated_user['role'] == 'worker':
                db.execute(
                    "UPDATE worker_profiles SET avg_rating = ?, total_ratings = ? WHERE user_id = ?",
                    [avg['avg'], avg['cnt'], to_user_id]
                )
            elif rated_user['role'] == 'ai_client':
                db.execute(
                    "UPDATE ai_client_profiles SET avg_rating = ?, total_ratings = ? WHERE user_id = ?",
                    [avg['avg'], avg['cnt'], to_user_id]
                )

        audit(db, user['id'], "rate", "rating", task_id)
        db.commit()
        return json_response({"ok": True}, 201)

    # ── Flags (Fraud/Abuse) ────────────────────────────────────────────────

    elif path == "/api/v1/flags" and method == "POST":
        user = authenticate(db)
        if not user:
            return error_response("Unauthorized", 401)

        body = get_body()
        if not body.get("entity_type") or not body.get("entity_id") or not body.get("reason"):
            return error_response("entity_type, entity_id, and reason required")

        # CRIT-07: validate entity_type before INSERT to prevent arbitrary values
        valid_entity_types = ('task', 'worker', 'client')
        if body['entity_type'] not in valid_entity_types:
            return error_response(f"entity_type must be one of: {', '.join(valid_entity_types)}")
        if not isinstance(body['entity_id'], int):
            return error_response("entity_id must be an integer")
        if len(str(body['reason'])) > 2000:
            return error_response("reason must be 2000 characters or fewer")

        # MED-12: per-user rate limit on flag submissions to prevent flooding
        recent_flags = db.execute(
            "SELECT COUNT(*) as c FROM flags WHERE flagged_by = ? AND created_at >= datetime('now', '-1 hour')",
            [user['id']]
        ).fetchone()['c']
        if recent_flags >= 10:
            return error_response("Too many flags submitted. Please wait before flagging again.", 429)

        db.execute(
            "INSERT INTO flags (flagged_by, flagged_entity_type, flagged_entity_id, reason) VALUES (?,?,?,?)",
            [user['id'], body['entity_type'], body['entity_id'], body['reason']]
        )
        audit(db, user['id'], "flag", body['entity_type'], body['entity_id'])
        db.commit()
        return json_response({"ok": True}, 201)

    elif path == "/api/v1/flags" and method == "GET":
        user = authenticate(db)
        if not user or user['role'] != 'admin':
            return error_response("Admin access required", 403)

        status = params.get("status", "pending")
        flags = db.execute(
            """SELECT f.*, u.name as flagged_by_name, u.email as flagged_by_email
            FROM flags f JOIN users u ON f.flagged_by = u.id
            WHERE f.status = ? ORDER BY f.created_at DESC""",
            [status]
        ).fetchall()
        return json_response([row_to_dict(f) for f in flags])

    elif re.match(r"^/api/v1/flags/\d+$", path) and method == "PUT":
        user = authenticate(db)
        if not user or user['role'] != 'admin':
            return error_response("Admin access required", 403)

        flag_id = int(path.split("/")[-1])
        body = get_body()
        db.execute(
            "UPDATE flags SET status = ?, admin_notes = ?, resolved_at = datetime('now') WHERE id = ?",
            [body.get('status', 'resolved'), body.get('admin_notes', ''), flag_id]
        )

        # Handle ban/suspend actions
        flag = db.execute("SELECT * FROM flags WHERE id = ?", [flag_id]).fetchone()
        if flag and body.get("action") in ('ban', 'suspend', 'warn'):
            entity_user_id = flag['flagged_entity_id']
            if body['action'] == 'ban':
                db.execute("UPDATE users SET is_banned = 1 WHERE id = ?", [entity_user_id])
            elif body['action'] == 'suspend':
                db.execute("UPDATE users SET is_suspended = 1 WHERE id = ?", [entity_user_id])
            audit(db, user['id'], body['action'], "user", entity_user_id)

        db.commit()
        return json_response({"ok": True})

    # ── Payments / Ledger ──────────────────────────────────────────────────

    elif path == "/api/v1/payments/add-funds" and method == "POST":
        user = authenticate(db)
        if not user or user['role'] != 'ai_client':
            return error_response("Only AI clients can add funds", 403)

        body = get_body()
        amount = float(body.get("amount", 0))
        if amount <= 0:
            return error_response("Amount must be positive")

        stripe_payment_id = fake_stripe_payment_id()
        profile = db.execute("SELECT credit_balance FROM ai_client_profiles WHERE user_id = ?", [user['id']]).fetchone()
        new_balance = (profile['credit_balance'] or 0) + amount
        db.execute("UPDATE ai_client_profiles SET credit_balance = ? WHERE user_id = ?", [new_balance, user['id']])
        db.execute(
            "INSERT INTO ledger (user_id, task_id, entry_type, amount, balance_after, description, stripe_payment_id) VALUES (?,?,?,?,?,?,?)",
            [user['id'], None, 'fund_add', amount, new_balance, f"Added ${amount:.2f} (test funds)", stripe_payment_id]
        )
        audit(db, user['id'], "add_funds", "payment", None, {"amount": amount, "stripe_payment_id": stripe_payment_id})
        db.commit()
        return json_response({"ok": True, "new_balance": new_balance, "stripe_payment_id": stripe_payment_id})

    elif path == "/api/v1/payments/request-payout" and method == "POST":
        user = authenticate(db)
        if not user or user['role'] != 'worker':
            return error_response("Only workers can request payouts", 403)

        body = get_body()
        amount = float(body.get("amount", 0))

        # CRIT-02: atomic payout deduction — UPDATE...WHERE >= prevents double-withdrawal race condition
        rows_updated = db.execute(
            "UPDATE worker_profiles SET withdrawable_balance = withdrawable_balance - ? "
            "WHERE user_id = ? AND withdrawable_balance >= ? AND ? > 0",
            [amount, user['id'], amount, amount]
        ).rowcount
        if rows_updated == 0:
            db.rollback()
            return error_response("Insufficient withdrawable balance, invalid amount, or concurrent request conflict", 409)
        new_balance_row = db.execute("SELECT withdrawable_balance FROM worker_profiles WHERE user_id = ?", [user['id']]).fetchone()
        new_balance = new_balance_row['withdrawable_balance'] if new_balance_row else 0

        db.execute(
            "INSERT INTO payout_requests (user_id, amount) VALUES (?,?)",
            [user['id'], amount]
        )
        db.execute(
            "INSERT INTO ledger (user_id, task_id, entry_type, amount, balance_after, description) VALUES (?,?,?,?,?,?)",
            [user['id'], None, 'payout_request', amount, new_balance, f"Payout request: ${amount:.2f}"]
        )
        audit(db, user['id'], "request_payout", "payment", None, {"amount": amount})
        db.commit()
        return json_response({"ok": True, "new_balance": new_balance})

    elif path == "/api/v1/payments/ledger" and method == "GET":
        user = authenticate(db)
        if not user:
            return error_response("Unauthorized", 401)

        entries = db.execute(
            "SELECT * FROM ledger WHERE user_id = ? ORDER BY created_at DESC LIMIT 50",
            [user['id']]
        ).fetchall()
        return json_response([row_to_dict(e) for e in entries])

    elif path == "/api/v1/payments/create-checkout" and method == "POST":
        user = authenticate(db)
        if not user:
            return error_response("Unauthorized", 401)

        body = get_body()
        amount = float(body.get("amount", 0))
        currency = body.get("currency", "usd")
        if amount <= 0:
            return error_response("Amount must be positive")

        session_id = f"cs_sim_{secrets.token_hex(16)}"
        payment_url = f"https://checkout.stripe.com/pay/{session_id}#simulated"
        stripe_payment_id = fake_stripe_payment_id()

        # Record in ledger as pending (not yet applied to balance)
        db.execute(
            "INSERT INTO ledger (user_id, task_id, entry_type, amount, balance_after, description, stripe_payment_id) VALUES (?,?,?,?,?,?,?)",
            [user['id'], None, 'fund_add', amount, None, f"Checkout session {session_id} ({currency.upper()})", stripe_payment_id]
        )
        audit(db, user['id'], "create_checkout", "payment", None, {"amount": amount, "currency": currency, "session_id": session_id})
        db.commit()
        return json_response({"session_id": session_id, "payment_url": payment_url, "stripe_payment_id": stripe_payment_id, "amount": amount, "currency": currency}, 201)

    elif path == "/api/v1/payments/connect-account" and method == "POST":
        user = authenticate(db)
        if not user or user['role'] != 'worker':
            return error_response("Only workers can set up payout accounts", 403)

        body = get_body()
        bank_name = body.get("bank_name", "")
        account_last4 = body.get("account_last4", "")
        if not bank_name or not account_last4:
            return error_response("bank_name and account_last4 required")

        payout_account_id = f"acct_sim_{secrets.token_hex(10)}"
        payout_method_details = json.dumps({"bank_name": bank_name, "account_last4": account_last4})

        db.execute(
            "UPDATE worker_profiles SET payout_account_id = ?, payout_method_details = ?, payout_method = 'stripe' WHERE user_id = ?",
            [payout_account_id, payout_method_details, user['id']]
        )
        audit(db, user['id'], "connect_payout_account", "worker_profile", user['id'])
        db.commit()
        return json_response({"ok": True, "payout_account_id": payout_account_id, "bank_name": bank_name, "account_last4": account_last4})

    elif path == "/api/v1/payments/payout-history" and method == "GET":
        user = authenticate(db)
        if not user or user['role'] != 'worker':
            return error_response("Only workers can view payout history", 403)

        payouts = db.execute(
            "SELECT * FROM payout_requests WHERE user_id = ? ORDER BY created_at DESC",
            [user['id']]
        ).fetchall()
        return json_response([row_to_dict(p) for p in payouts])

    # ── File Uploads ───────────────────────────────────────────────────────

    elif path == "/api/v1/uploads/presign" and method == "POST":
        user = authenticate(db)
        if not user:
            return error_response("Unauthorized", 401)

        body = get_body()
        filename = body.get("filename", "")
        content_type = body.get("content_type", "application/octet-stream")
        task_id = body.get("task_id")

        if not filename:
            return error_response("filename required")

        storage_key = f"uploads/{user['id']}/{secrets.token_hex(16)}/{filename}"
        script_name = os.environ.get("SCRIPT_NAME", "/cgi-bin/api.py")

        cursor = db.execute(
            """INSERT INTO file_uploads (user_id, task_id, filename, content_type, storage_key, status)
            VALUES (?,?,?,?,?,?)""",
            [user['id'], task_id, filename, content_type, storage_key, 'pending']
        )
        upload_id = cursor.lastrowid
        upload_url = f"{script_name}/api/v1/uploads/{upload_id}/complete"
        db.execute("UPDATE file_uploads SET upload_url = ? WHERE id = ?", [upload_url, upload_id])
        db.commit()

        return json_response({"upload_id": upload_id, "upload_url": upload_url, "storage_key": storage_key}, 201)

    elif path == "/api/v1/uploads" and method == "GET":
        user = authenticate(db)
        if not user:
            return error_response("Unauthorized", 401)

        task_id = params.get("task_id")
        if task_id:
            uploads = db.execute(
                "SELECT * FROM file_uploads WHERE task_id = ? AND user_id = ? ORDER BY created_at DESC",
                [task_id, user['id']]
            ).fetchall()
        else:
            uploads = db.execute(
                "SELECT * FROM file_uploads WHERE user_id = ? ORDER BY created_at DESC LIMIT 50",
                [user['id']]
            ).fetchall()
        return json_response([row_to_dict(u) for u in uploads])

    elif re.match(r"^/api/v1/uploads/\d+/complete$", path) and method == "POST":
        user = authenticate(db)
        if not user:
            return error_response("Unauthorized", 401)

        upload_id = int(path.split("/")[4])
        upload = db.execute("SELECT * FROM file_uploads WHERE id = ? AND user_id = ?", [upload_id, user['id']]).fetchone()
        if not upload:
            return error_response("Upload not found", 404)

        body = get_body()
        file_size = body.get("file_size")
        db.execute(
            "UPDATE file_uploads SET status = 'completed', file_size = ? WHERE id = ?",
            [file_size, upload_id]
        )
        db.commit()

        upload = db.execute("SELECT * FROM file_uploads WHERE id = ?", [upload_id]).fetchone()
        return json_response(row_to_dict(upload))

    # ── Webhook Delivery ───────────────────────────────────────────────────

    elif path == "/api/v1/webhooks" and method == "GET":
        user = authenticate(db)
        if not user or user['role'] != 'ai_client':
            return error_response("Only AI clients can view webhooks", 403)

        events = db.execute(
            "SELECT * FROM webhook_events WHERE client_id = ? ORDER BY created_at DESC LIMIT 50",
            [user['id']]
        ).fetchall()
        return json_response([row_to_dict(e) for e in events])

    elif path == "/api/v1/webhooks/deliver" and method == "POST":
        user = authenticate(db)
        if not user or user['role'] != 'admin':
            return error_response("Admin access required", 403)

        pending = db.execute(
            "SELECT we.*, acp.webhook_url, acp.webhook_secret FROM webhook_events we "
            "JOIN ai_client_profiles acp ON we.client_id = acp.user_id "
            "WHERE we.delivery_status = 'pending'"
        ).fetchall()

        delivered = 0
        failed = 0
        now = datetime.now(timezone.utc).isoformat()

        for event in pending:
            if not event['webhook_url']:
                db.execute(
                    "UPDATE webhook_events SET delivery_status = 'failed', attempts = attempts + 1, last_attempt_at = ? WHERE id = ?",
                    [now, event['id']]
                )
                failed += 1
            else:
                # CRIT-05: actually attempt HTTP delivery with HMAC-SHA256 signature
                secret = event['webhook_secret'] or ''
                payload_bytes = event['payload'].encode('utf-8')
                sig = hmac.new(secret.encode('utf-8'), payload_bytes, hashlib.sha256).hexdigest()
                req = urllib.request.Request(
                    event['webhook_url'],
                    data=payload_bytes,
                    headers={
                        'Content-Type': 'application/json',
                        'X-GoHireHumans-Signature': f'sha256={sig}',
                    },
                    method='POST'
                )
                try:
                    with urllib.request.urlopen(req, timeout=5):
                        delivery_status = 'delivered'
                        delivered += 1
                except Exception:
                    delivery_status = 'failed'
                    failed += 1
                db.execute(
                    "UPDATE webhook_events SET delivery_status = ?, attempts = attempts + 1, last_attempt_at = ? WHERE id = ?",
                    [delivery_status, now, event['id']]
                )

        db.commit()
        return json_response({"delivered": delivered, "failed": failed})

    elif re.match(r"^/api/v1/webhooks/retry/\d+$", path) and method == "POST":
        user = authenticate(db)
        if not user or user['role'] != 'admin':
            return error_response("Admin access required", 403)

        webhook_id = int(path.split("/")[-1])
        event = db.execute("SELECT * FROM webhook_events WHERE id = ?", [webhook_id]).fetchone()
        if not event:
            return error_response("Webhook event not found", 404)

        db.execute(
            "UPDATE webhook_events SET delivery_status = 'pending' WHERE id = ?",
            [webhook_id]
        )
        db.commit()
        return json_response({"ok": True, "id": webhook_id})

    # ── Worker Verification / KYC ──────────────────────────────────────────

    elif path == "/api/v1/workers/verify" and method == "POST":
        user = authenticate(db)
        if not user or user['role'] != 'worker':
            return error_response("Only workers can submit verifications", 403)

        body = get_body()
        verification_type = body.get("verification_type", "")
        document_reference = body.get("document_reference", "")

        valid_types = ('identity', 'background', 'license', 'notary', 'medical')
        if verification_type not in valid_types:
            return error_response(f"verification_type must be one of: {', '.join(valid_types)}")

        cursor = db.execute(
            "INSERT INTO worker_verifications (user_id, verification_type, document_reference) VALUES (?,?,?)",
            [user['id'], verification_type, document_reference]
        )
        verif_id = cursor.lastrowid
        audit(db, user['id'], "submit_verification", "worker_verification", verif_id)
        db.commit()

        verif = db.execute("SELECT * FROM worker_verifications WHERE id = ?", [verif_id]).fetchone()
        return json_response(row_to_dict(verif), 201)

    elif path == "/api/v1/workers/verifications" and method == "GET":
        user = authenticate(db)
        if not user:
            return error_response("Unauthorized", 401)
        if user['role'] != 'worker':
            return error_response("Only workers can view their verifications", 403)

        verifs = db.execute(
            "SELECT * FROM worker_verifications WHERE user_id = ? ORDER BY created_at DESC",
            [user['id']]
        ).fetchall()
        return json_response([row_to_dict(v) for v in verifs])

    elif path == "/api/v1/workers/ranked" and method == "GET":
        user = authenticate(db)
        if not user:
            return error_response("Unauthorized", 401)

        workers = db.execute(
            """SELECT u.id, u.name, u.email, wp.avg_rating, wp.total_tasks_completed,
            wp.is_verified, wp.total_flags, wp.skills, wp.geography, wp.bio, wp.verification_level
            FROM users u
            JOIN worker_profiles wp ON u.id = wp.user_id
            WHERE u.role = 'worker' AND u.is_active = 1 AND u.is_banned = 0
            LIMIT 200"""
        ).fetchall()

        ranked = []
        for w in workers:
            wd = row_to_dict(w)
            avg_rating = w['avg_rating'] or 0
            total_completed = w['total_tasks_completed'] or 0
            is_verified = w['is_verified'] or 0
            total_flags = w['total_flags'] or 0

            reputation_score = (
                avg_rating * 20
                + total_completed * 2
                + is_verified * 15
                + total_flags * (-10)
            )
            wd['reputation_score'] = round(reputation_score, 2)
            ranked.append(wd)

        ranked.sort(key=lambda x: x['reputation_score'], reverse=True)
        return json_response({"workers": ranked[:50], "total": len(ranked)})

    elif path == "/api/v1/admin/verifications" and method == "GET":
        user = authenticate(db)
        if not user or user['role'] != 'admin':
            return error_response("Admin access required", 403)

        status_filter = params.get("status", "pending")
        verifs = db.execute(
            """SELECT wv.*, u.name as worker_name, u.email as worker_email
            FROM worker_verifications wv
            JOIN users u ON wv.user_id = u.id
            WHERE wv.status = ?
            ORDER BY wv.created_at DESC""",
            [status_filter]
        ).fetchall()
        return json_response([row_to_dict(v) for v in verifs])

    elif re.match(r"^/api/v1/admin/verifications/\d+$", path) and method == "PUT":
        user = authenticate(db)
        if not user or user['role'] != 'admin':
            return error_response("Admin access required", 403)

        verif_id = int(path.split("/")[-1])
        body = get_body()
        new_status = body.get("status", "approved")
        admin_notes = body.get("admin_notes", "")

        if new_status not in ('approved', 'rejected', 'expired'):
            return error_response("status must be approved, rejected, or expired")

        verif = db.execute("SELECT * FROM worker_verifications WHERE id = ?", [verif_id]).fetchone()
        if not verif:
            return error_response("Verification not found", 404)

        verified_at = datetime.now(timezone.utc).isoformat() if new_status == 'approved' else None
        db.execute(
            "UPDATE worker_verifications SET status = ?, admin_notes = ?, verified_at = ? WHERE id = ?",
            [new_status, admin_notes, verified_at, verif_id]
        )

        if new_status == 'approved':
            # Determine verification level by type
            level_map = {
                'identity': 'verified',
                'background': 'background_checked',
                'license': 'licensed',
                'notary': 'notary',
                'medical': 'medical_certified',
            }
            level = level_map.get(verif['verification_type'], 'verified')
            db.execute(
                "UPDATE worker_profiles SET is_verified = 1, verification_level = ? WHERE user_id = ?",
                [level, verif['user_id']]
            )
            push_notification(db, verif['user_id'], "verification_approved",
                "Verification approved!",
                f"Your {verif['verification_type']} verification has been approved.",
                "/api/v1/workers/verifications")
        elif new_status == 'rejected':
            push_notification(db, verif['user_id'], "verification_rejected",
                "Verification rejected",
                f"Your {verif['verification_type']} verification was rejected. {admin_notes}",
                "/api/v1/workers/verify")

        audit(db, user['id'], f"verification_{new_status}", "worker_verification", verif_id)
        db.commit()
        return json_response({"ok": True})

    # ── Notifications ──────────────────────────────────────────────────────

    elif path == "/api/v1/notifications/stream" and method == "GET":
        user = authenticate(db)
        if not user:
            print("Status: 401")
            print("Content-Type: text/event-stream")
            print()
            print("event: error\ndata: {\"error\": \"Unauthorized\"}\n\n")
            return

        notifications = db.execute(
            "SELECT * FROM notifications WHERE user_id = ? ORDER BY created_at DESC LIMIT 20",
            [user['id']]
        ).fetchall()

        print("Status: 200")
        print("Content-Type: text/event-stream")
        print("Cache-Control: no-cache")
        print()

        for notif in reversed(notifications):
            nd = row_to_dict(notif)
            print(f"id: {nd['id']}")
            print(f"event: {nd['type']}")
            print(f"data: {json.dumps(nd, default=str)}")
            print()

        # Close stream
        print("event: close")
        print("data: {\"message\": \"Stream closed\"}")
        print()
        return

    elif path == "/api/v1/notifications" and method == "GET":
        user = authenticate(db)
        if not user:
            return error_response("Unauthorized", 401)

        unread_only = params.get("unread_only", "").lower() in ('true', '1')
        limit = min(int(params.get("limit", 50)), 100)

        query = "SELECT * FROM notifications WHERE user_id = ?"
        qvals = [user['id']]
        if unread_only:
            query += " AND is_read = 0"
        query += " ORDER BY created_at DESC LIMIT ?"
        qvals.append(limit)

        notifs = db.execute(query, qvals).fetchall()
        return json_response([row_to_dict(n) for n in notifs])

    elif path == "/api/v1/notifications/unread-count" and method == "GET":
        user = authenticate(db)
        if not user:
            return error_response("Unauthorized", 401)
        count = db.execute(
            "SELECT COUNT(*) as c FROM notifications WHERE user_id = ? AND is_read = 0",
            [user['id']]
        ).fetchone()['c']
        return json_response({"count": count})

    elif re.match(r"^/api/v1/notifications/\d+/read$", path) and method == "POST":
        user = authenticate(db)
        if not user:
            return error_response("Unauthorized", 401)

        notif_id = int(path.split("/")[4])
        db.execute(
            "UPDATE notifications SET is_read = 1 WHERE id = ? AND user_id = ?",
            [notif_id, user['id']]
        )
        db.commit()
        return json_response({"ok": True})

    elif path == "/api/v1/notifications/read-all" and method == "POST":
        user = authenticate(db)
        if not user:
            return error_response("Unauthorized", 401)

        db.execute("UPDATE notifications SET is_read = 1 WHERE user_id = ?", [user['id']])
        db.commit()
        return json_response({"ok": True})

    # ── Dynamic Pricing Suggestions ────────────────────────────────────────

    elif path == "/api/v1/pricing/suggest" and method == "GET":
        user = authenticate(db)
        if not user:
            return error_response("Unauthorized", 401)

        category = params.get("category")
        location_type = params.get("location_type")
        required_skills_str = params.get("required_skills", "")
        # MED-14: cap at 20 skills to prevent DoS via large iteration
        required_skills = [s.strip() for s in required_skills_str.split(",") if s.strip()][:20] if required_skills_str else []

        conditions = ["status = 'completed'"]
        values = []
        factors = []

        if category:
            conditions.append("category = ?")
            values.append(category)
            factors.append(f"Category: {category}")
        if location_type:
            conditions.append("location_type = ?")
            values.append(location_type)
            factors.append(f"Location type: {location_type}")

        where = " AND ".join(conditions)
        rows = db.execute(f"SELECT budget_amount FROM tasks WHERE {where}", values).fetchall()
        amounts = [r['budget_amount'] for r in rows if r['budget_amount'] is not None]

        if not amounts:
            # Fallback defaults
            suggested = 50.0
            return json_response({
                "suggested_price": suggested,
                "range": {"min": suggested * 0.5, "max": suggested * 2.0},
                "avg": suggested,
                "median": suggested,
                "sample_size": 0,
                "factors": factors + ["Insufficient data — using defaults"]
            })

        avg = sum(amounts) / len(amounts)
        min_amount = min(amounts)
        max_amount = max(amounts)
        sorted_amounts = sorted(amounts)
        n = len(sorted_amounts)
        if n % 2 == 0:
            median = (sorted_amounts[n // 2 - 1] + sorted_amounts[n // 2]) / 2
        else:
            median = sorted_amounts[n // 2]

        # Skill complexity bonus
        skill_bonus = len(required_skills) * 5.0
        if required_skills:
            factors.append(f"Skill complexity bonus: +${skill_bonus:.2f} for {len(required_skills)} skills")

        suggested = round(avg + skill_bonus, 2)
        factors.append(f"Based on {n} completed tasks")

        return json_response({
            "suggested_price": suggested,
            "range": {"min": round(min_amount, 2), "max": round(max_amount, 2)},
            "avg": round(avg, 2),
            "median": round(median, 2),
            "sample_size": n,
            "factors": factors
        })

    # ── Admin Routes ───────────────────────────────────────────────────────

    elif path == "/api/v1/admin/stats" and method == "GET":
        user = authenticate(db)
        if not user or user['role'] != 'admin':
            return error_response("Admin access required", 403)

        stats = {
            "total_users": db.execute("SELECT COUNT(*) as c FROM users").fetchone()['c'],
            "total_workers": db.execute("SELECT COUNT(*) as c FROM users WHERE role = 'worker'").fetchone()['c'],
            "total_clients": db.execute("SELECT COUNT(*) as c FROM users WHERE role = 'ai_client'").fetchone()['c'],
            "total_tasks": db.execute("SELECT COUNT(*) as c FROM tasks").fetchone()['c'],
            "tasks_by_status": {},
            "tasks_today": db.execute("SELECT COUNT(*) as c FROM tasks WHERE date(created_at) = date('now')").fetchone()['c'],
            "total_payout_value": db.execute("SELECT COALESCE(SUM(budget_amount),0) as s FROM tasks WHERE status = 'completed'").fetchone()['s'],
            "avg_task_value": db.execute("SELECT COALESCE(AVG(budget_amount),0) as a FROM tasks").fetchone()['a'],
            "pending_flags": db.execute("SELECT COUNT(*) as c FROM flags WHERE status = 'pending'").fetchone()['c'],
            "completion_rate": 0,
            "active_workers": db.execute("SELECT COUNT(DISTINCT worker_id) as c FROM tasks WHERE worker_id IS NOT NULL").fetchone()['c'],
            "active_clients": db.execute("SELECT COUNT(DISTINCT client_id) as c FROM tasks").fetchone()['c'],
        }

        for status_val in ['draft','open','reserved','in_progress','submitted','completed','disputed','canceled']:
            stats['tasks_by_status'][status_val] = db.execute(
                "SELECT COUNT(*) as c FROM tasks WHERE status = ?", [status_val]
            ).fetchone()['c']

        total = stats['total_tasks']
        completed = stats['tasks_by_status'].get('completed', 0)
        stats['completion_rate'] = round((completed / total * 100), 1) if total > 0 else 0

        return json_response(stats)

    elif path == "/api/v1/admin/users" and method == "GET":
        user = authenticate(db)
        if not user or user['role'] != 'admin':
            return error_response("Admin access required", 403)

        users = db.execute(
            """SELECT u.id, u.email, u.role, u.name, u.created_at,
                      u.is_active, u.is_suspended, u.is_banned,
                      COALESCE(wp.is_verified, 0) as is_verified
               FROM users u
               LEFT JOIN worker_profiles wp ON u.id = wp.user_id
               ORDER BY u.created_at DESC"""
        ).fetchall()
        return json_response([row_to_dict(u) for u in users])

    elif re.match(r"^/api/v1/admin/users/\d+$", path) and method == "PUT":
        user = authenticate(db)
        if not user or user['role'] != 'admin':
            return error_response("Admin access required", 403)

        target_id = int(path.split("/")[-1])
        body = get_body()
        for field in ['is_active', 'is_suspended', 'is_banned']:
            if field in body:
                db.execute(f"UPDATE users SET {field} = ? WHERE id = ?", [body[field], target_id])
        audit(db, user['id'], "admin_update_user", "user", target_id, body)
        db.commit()
        return json_response({"ok": True})

    elif path == "/api/v1/admin/audit-log" and method == "GET":
        user = authenticate(db)
        if not user or user['role'] != 'admin':
            return error_response("Admin access required", 403)

        logs = db.execute(
            """SELECT al.*, u.name, u.email FROM audit_log al
            LEFT JOIN users u ON al.user_id = u.id
            ORDER BY al.created_at DESC LIMIT 100"""
        ).fetchall()
        return json_response([row_to_dict(l) for l in logs])

    elif path == "/api/v1/admin/tasks" and method == "GET":
        user = authenticate(db)
        if not user or user['role'] != 'admin':
            return error_response("Admin access required", 403)

        tasks = db.execute(
            """SELECT t.*, u.name as client_name, w.name as worker_name
            FROM tasks t
            JOIN users u ON t.client_id = u.id
            LEFT JOIN users w ON t.worker_id = w.id
            ORDER BY t.created_at DESC LIMIT 100"""
        ).fetchall()
        return json_response([row_to_dict(t) for t in tasks])

    elif path == "/api/v1/admin/send-test-email" and method == "POST":
        user = authenticate(db)
        if not user or user['role'] != 'admin':
            return error_response("Admin access required", 403)

        body = get_body()
        to = body.get("to", "")
        subject = body.get("subject", "")
        email_body = body.get("body", "")

        if not to or not subject:
            return error_response("to and subject are required")

        audit(db, user['id'], "email_stub", "email", None, {"to": to, "subject": subject, "body": email_body})
        db.commit()
        return json_response({"status": "queued", "message": "Email delivery is stubbed in MVP"})

    # ── Analytics Dashboard ────────────────────────────────────────────────

    elif path == "/api/v1/admin/analytics/trends" and method == "GET":
        user = authenticate(db)
        if not user or user['role'] != 'admin':
            return error_response("Admin access required", 403)

        rows = db.execute("""
            SELECT
                date(created_at) as day,
                COUNT(*) as new_tasks,
                SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) as completed_tasks,
                COALESCE(SUM(CASE WHEN status = 'completed' THEN budget_amount ELSE 0 END), 0) as revenue
            FROM tasks
            WHERE date(created_at) >= date('now', '-30 days')
            GROUP BY date(created_at)
            ORDER BY day ASC
        """).fetchall()
        return json_response([row_to_dict(r) for r in rows])

    elif path == "/api/v1/admin/analytics/cohorts" and method == "GET":
        user = authenticate(db)
        if not user or user['role'] != 'admin':
            return error_response("Admin access required", 403)

        cohort_rows = db.execute("""
            SELECT
                strftime('%Y-%m', u.created_at) as cohort_month,
                COUNT(u.id) as total_users,
                COUNT(DISTINCT CASE
                    WHEN t.created_at >= datetime('now', '-30 days')
                         OR t.completed_at >= datetime('now', '-30 days')
                    THEN u.id END) as active_users
            FROM users u
            LEFT JOIN tasks t ON (t.client_id = u.id OR t.worker_id = u.id)
            GROUP BY strftime('%Y-%m', u.created_at)
            ORDER BY cohort_month ASC
        """).fetchall()

        result = []
        for r in cohort_rows:
            rd = row_to_dict(r)
            total = rd['total_users'] or 0
            active = rd['active_users'] or 0
            rd['retention_rate'] = round(active / total * 100, 1) if total > 0 else 0
            result.append(rd)
        return json_response(result)

    elif path == "/api/v1/admin/analytics/categories" and method == "GET":
        user = authenticate(db)
        if not user or user['role'] != 'admin':
            return error_response("Admin access required", 403)

        rows = db.execute("""
            SELECT
                category,
                COUNT(*) as task_count,
                COALESCE(AVG(budget_amount), 0) as avg_budget,
                ROUND(100.0 * SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) / COUNT(*), 1) as completion_rate,
                COALESCE(AVG(CASE WHEN r.rating IS NOT NULL THEN r.rating END), 0) as avg_rating
            FROM tasks t
            LEFT JOIN ratings r ON t.id = r.task_id AND r.from_user_id = t.client_id
            GROUP BY category
            ORDER BY task_count DESC
        """).fetchall()
        return json_response([row_to_dict(r) for r in rows])

    elif path == "/api/v1/admin/analytics/forecasting" and method == "GET":
        user = authenticate(db)
        if not user or user['role'] != 'admin':
            return error_response("Admin access required", 403)

        # Get last 30 days of daily task counts
        rows = db.execute("""
            SELECT date(created_at) as day, COUNT(*) as count
            FROM tasks
            WHERE date(created_at) >= date('now', '-30 days')
            GROUP BY date(created_at)
            ORDER BY day ASC
        """).fetchall()

        counts = [r['count'] for r in rows]
        n = len(counts)

        if n < 2:
            avg = counts[0] if counts else 0
            forecast = [{"day": str((datetime.now(timezone.utc) + timedelta(days=i+1)).date()), "predicted_tasks": round(avg)} for i in range(7)]
            return json_response({"forecast": forecast, "model": "insufficient_data", "sample_days": n})

        # Simple linear regression
        x_mean = (n - 1) / 2.0
        y_mean = sum(counts) / n
        numerator = sum((i - x_mean) * (counts[i] - y_mean) for i in range(n))
        denominator = sum((i - x_mean) ** 2 for i in range(n))
        slope = numerator / denominator if denominator != 0 else 0
        intercept = y_mean - slope * x_mean

        forecast = []
        for i in range(1, 8):
            x = n - 1 + i
            predicted = max(0, round(intercept + slope * x))
            day = (datetime.now(timezone.utc) + timedelta(days=i)).date()
            forecast.append({"day": str(day), "predicted_tasks": predicted})

        return json_response({
            "forecast": forecast,
            "model": "linear_regression",
            "slope": round(slope, 4),
            "intercept": round(intercept, 4),
            "sample_days": n
        })

    # ── Seed Route (for demo setup) ────────────────────────────────────────

    elif path == "/seed" and method == "POST":
        # CRIT-04: require SEED_SECRET env var to prevent unauthenticated database resets
        seed_secret = os.environ.get("SEED_SECRET")
        if not seed_secret:
            return error_response("Seed endpoint disabled", 404)
        seed_body = get_body()
        provided_secret = (seed_body or {}).get("secret", "")
        if not hmac.compare_digest(seed_secret, provided_secret):
            return error_response("Forbidden", 403)

        # Check if already seeded
        existing = db.execute("SELECT COUNT(*) as c FROM users").fetchone()['c']
        if existing > 0:
            return json_response({"message": "Already seeded", "users": existing})

        # Create 2 AI clients
        clients_data = [
            {"email": "ai@automatedinbox.com", "name": "AutomatedInbox AI", "app_name": "AutomatedInbox", "description": "AI assistant that handles email triage and response drafting, needs humans for calls and signatures."},
            {"email": "ai@fieldops.io", "name": "FieldOps Agent", "app_name": "FieldOps", "description": "AI operations platform that coordinates field inspections, deliveries, and in-person verification tasks."},
        ]

        client_ids = []
        client_keys = []
        for c in clients_data:
            cursor = db.execute(
                "INSERT INTO users (email, password_hash, role, name) VALUES (?,?,?,?)",
                [c['email'], hash_password("demo1234"), 'ai_client', c['name']]
            )
            uid = cursor.lastrowid
            client_ids.append(uid)

            key, prefix, key_hash = generate_api_key()
            client_keys.append(key)
            db.execute(
                "INSERT INTO api_keys (user_id, key_prefix, key_hash, name) VALUES (?,?,?,?)",
                [uid, prefix, key_hash, "Default Key"]
            )

            webhook_secret = secrets.token_hex(16)
            db.execute(
                "INSERT INTO ai_client_profiles (user_id, app_name, description, billing_email, credit_balance, webhook_url, webhook_secret) VALUES (?,?,?,?,?,?,?)",
                [uid, c['app_name'], c['description'], c['email'], 500.00, f"https://example.com/webhooks/{c['app_name'].lower()}", webhook_secret]
            )

        # Create 5 workers
        workers_data = [
            {"email": "sarah@example.com", "name": "Sarah Chen", "skills": ["phone_call","customer_support","writing"], "geo": "San Francisco, CA", "tz": "America/Los_Angeles", "bio": "Former customer success manager, excellent phone presence and writing skills.", "rate_min": 20, "rate_max": 45},
            {"email": "marcus@example.com", "name": "Marcus Johnson", "skills": ["in_person_errand","delivery","inspection","media_capture"], "geo": "New York, NY", "tz": "America/New_York", "bio": "Reliable field worker with a car. Available for errands, inspections, and photo/video capture in the NYC metro area.", "rate_min": 25, "rate_max": 50},
            {"email": "elena@example.com", "name": "Elena Rodriguez", "skills": ["translation","writing","research","data_entry"], "geo": "Remote", "tz": "America/Denver", "bio": "Bilingual (English/Spanish) researcher and writer. Detail-oriented with strong data skills.", "rate_min": 18, "rate_max": 40},
            {"email": "james@example.com", "name": "James Park", "skills": ["expert_review","document_signing","research"], "geo": "Chicago, IL", "tz": "America/Chicago", "bio": "Former paralegal with expertise in document review and legal research. Notary public.", "rate_min": 30, "rate_max": 60},
            {"email": "aisha@example.com", "name": "Aisha Patel", "skills": ["testing","media_capture","customer_support","data_entry"], "geo": "Austin, TX", "tz": "America/Chicago", "bio": "QA background with strong attention to detail. Available for user testing, data tasks, and media capture.", "rate_min": 22, "rate_max": 42},
        ]

        worker_ids = []
        for w in workers_data:
            cursor = db.execute(
                "INSERT INTO users (email, password_hash, role, name) VALUES (?,?,?,?)",
                [w['email'], hash_password("demo1234"), 'worker', w['name']]
            )
            uid = cursor.lastrowid
            worker_ids.append(uid)
            db.execute(
                """INSERT INTO worker_profiles (user_id, skills, geography, timezone, bio,
                hourly_rate_min, hourly_rate_max, per_task_rate_min, per_task_rate_max,
                favorite_categories, estimated_earnings, withdrawable_balance, total_tasks_completed, avg_rating, total_ratings)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                [uid, json.dumps(w['skills']), w['geo'], w['tz'], w['bio'],
                 w['rate_min'], w['rate_max'], w['rate_min']*1.5, w['rate_max']*2,
                 json.dumps(w['skills'][:2]),
                 round(w['rate_min'] * 8.5, 2), round(w['rate_min'] * 4.2, 2),
                 3, round(3.5 + secrets.randbelow(15)/10, 1), secrets.randbelow(8)+2]
            )

        # Create admin
        cursor = db.execute(
            "INSERT INTO users (email, password_hash, role, name) VALUES (?,?,?,?)",
            ["admin@gohirehumans.com", hash_password("admin1234"), 'admin', 'GoHireHumans Admin']
        )
        admin_id = cursor.lastrowid

        # Create 10 tasks in different states
        tasks_data = [
            {"title": "Make a follow-up phone call to vendor", "desc": "Call ABC Supplies at (555) 123-4567 to confirm our order #4521 has shipped. Ask for tracking number and estimated delivery date. Record the answers in a structured format.", "cat": "phone_call", "loc": "remote", "budget": 15.00, "bt": "flat_fee", "status": "open", "skills": ["phone_call"]},
            {"title": "Photograph apartment for listing", "desc": "Visit 425 Oak Street, Apt 3B, Brooklyn NY 11201. Take 15-20 high-quality photos of each room, including kitchen, bathrooms, living areas, and any outdoor space. Upload photos organized by room.", "cat": "media_capture", "loc": "specific_address", "loc_detail": "425 Oak St, Apt 3B, Brooklyn NY 11201", "budget": 75.00, "bt": "flat_fee", "status": "open", "skills": ["media_capture"]},
            {"title": "Translate product description to Spanish", "desc": "Translate the attached 800-word product description from English to Spanish. Must be native-quality, not machine translation. Marketing tone, consumer electronics product.", "cat": "translation", "loc": "remote", "budget": 35.00, "bt": "flat_fee", "status": "open", "skills": ["translation","writing"]},
            {"title": "Review and sign NDA on behalf of company", "desc": "Review the attached NDA from TechPartner Inc. Check for standard terms, flag any unusual clauses, and sign on behalf of FieldOps (authorized signer details will be provided upon acceptance).", "cat": "document_signing", "loc": "remote", "budget": 50.00, "bt": "flat_fee", "status": "in_progress", "skills": ["document_signing","expert_review"], "worker_idx": 3},
            {"title": "User test a mobile checkout flow", "desc": "Complete a structured usability test of our mobile checkout flow. Follow the test script (attached), record your screen, and provide written feedback on each step. Takes approximately 45 minutes.", "cat": "testing", "loc": "remote", "budget": 40.00, "bt": "hourly", "time_cap": 1.0, "status": "in_progress", "skills": ["testing"], "worker_idx": 4},
            {"title": "Inspect construction site progress", "desc": "Visit the construction site at 1200 Industrial Blvd, Chicago IL. Document current progress with photos, check completion of Phase 2 milestones against the attached checklist, and note any safety concerns.", "cat": "inspection", "loc": "specific_address", "loc_detail": "1200 Industrial Blvd, Chicago, IL", "budget": 90.00, "bt": "flat_fee", "status": "submitted", "skills": ["inspection","media_capture"], "worker_idx": 1},
            {"title": "Research competitor pricing", "desc": "Research and compile pricing information for the top 5 competitors in the smart home security camera market. Include pricing tiers, features per tier, and any promotional offers. Deliver as a structured spreadsheet.", "cat": "research", "loc": "remote", "budget": 45.00, "bt": "flat_fee", "status": "completed", "skills": ["research","data_entry"], "worker_idx": 2},
            {"title": "Pick up signed documents from law office", "desc": "Pick up a sealed envelope from Chen & Associates at 500 Market St, Suite 300, San Francisco CA 94105. Deliver it to our office at 750 Battery St, 4th Floor. Confirm delivery with reception.", "cat": "in_person_errand", "loc": "specific_address", "loc_detail": "500 Market St, Suite 300, San Francisco, CA", "budget": 30.00, "bt": "flat_fee", "status": "completed", "skills": ["in_person_errand","delivery"], "worker_idx": 0},
            {"title": "Data entry from scanned receipts", "desc": "Enter data from 50 scanned receipts into the provided spreadsheet template. Fields: date, vendor, amount, category, payment method. Must be 99%+ accurate.", "cat": "data_entry", "loc": "remote", "budget": 60.00, "bt": "hourly", "time_cap": 3.0, "status": "open", "skills": ["data_entry"]},
            {"title": "Customer interview via video call", "desc": "Conduct a 30-minute structured interview with a customer (contact details provided upon acceptance). Follow the interview script, take notes, and provide a written summary of key insights. Must have professional demeanor and reliable internet.", "cat": "customer_support", "loc": "remote", "budget": 55.00, "bt": "flat_fee", "status": "draft", "skills": ["customer_support","phone_call"]},
        ]

        for i, t in enumerate(tasks_data):
            client_id = client_ids[i % 2]
            worker_id = worker_ids[t['worker_idx']] if 'worker_idx' in t else None

            cursor = db.execute(
                """INSERT INTO tasks (client_id, worker_id, title, description, category, location_type,
                location_detail, budget_type, budget_amount, time_cap_hours, required_skills, status,
                completed_at, created_at, updated_at)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,datetime('now', ?),datetime('now', ?))""",
                [client_id, worker_id, t['title'], t['desc'], t['cat'], t['loc'],
                 t.get('loc_detail', ''), t['bt'], t['budget'], t.get('time_cap'),
                 json.dumps(t.get('skills', [])), t['status'],
                 datetime.now(timezone.utc).isoformat() if t['status'] == 'completed' else None,
                 f"-{(10-i)} hours", f"-{(10-i)} hours"]
            )
            task_id = cursor.lastrowid

            # Add ratings for completed tasks
            if t['status'] == 'completed' and worker_id:
                rating_val = 4 + secrets.randbelow(2)
                db.execute(
                    "INSERT INTO ratings (task_id, from_user_id, to_user_id, rating, review) VALUES (?,?,?,?,?)",
                    [task_id, client_id, worker_id, rating_val, "Great work, delivered on time!"]
                )
                db.execute(
                    "INSERT INTO ratings (task_id, from_user_id, to_user_id, rating, review) VALUES (?,?,?,?,?)",
                    [task_id, worker_id, client_id, rating_val, "Clear instructions and responsive."]
                )

        # Seed 5 built-in task templates
        template_seeds = [
            {
                "name": "Phone Call Follow-Up",
                "description": "Make a follow-up phone call on behalf of the client",
                "category": "phone_call",
                "default_title": "Follow-Up Phone Call",
                "default_description": "Call the specified contact to follow up on [topic]. Record the key outcomes in a structured format.",
                "default_location_type": "remote",
                "default_budget_type": "flat_fee",
                "default_budget_amount": 15.0,
                "default_required_skills": json.dumps(["phone_call"]),
            },
            {
                "name": "Photo Documentation",
                "description": "Capture photos of a specific location or item",
                "category": "media_capture",
                "default_title": "Photo Documentation",
                "default_description": "Visit the specified address and take comprehensive photos as described. Organize by area/section.",
                "default_location_type": "specific_address",
                "default_budget_type": "flat_fee",
                "default_budget_amount": 75.0,
                "default_required_skills": json.dumps(["media_capture"]),
            },
            {
                "name": "Document Review & Signing",
                "description": "Review a document for standard terms and sign on behalf of the client",
                "category": "document_signing",
                "default_title": "Document Review & Signing",
                "default_description": "Review the attached document, flag any non-standard clauses, and sign on behalf of the client as authorized.",
                "default_location_type": "remote",
                "default_budget_type": "flat_fee",
                "default_budget_amount": 50.0,
                "default_required_skills": json.dumps(["document_signing", "expert_review"]),
            },
            {
                "name": "Field Inspection",
                "description": "Inspect a physical location and document findings",
                "category": "inspection",
                "default_title": "Field Inspection",
                "default_description": "Visit the specified site and conduct an inspection according to the provided checklist. Document with photos and a written report.",
                "default_location_type": "specific_address",
                "default_budget_type": "flat_fee",
                "default_budget_amount": 90.0,
                "default_required_skills": json.dumps(["inspection"]),
            },
            {
                "name": "Data Entry Batch",
                "description": "Enter structured data from provided source materials",
                "category": "data_entry",
                "default_title": "Data Entry Batch",
                "default_description": "Enter data from the provided source materials into the specified template. Accuracy of 99%+ required.",
                "default_location_type": "remote",
                "default_budget_type": "flat_fee",
                "default_budget_amount": 60.0,
                "default_required_skills": json.dumps(["data_entry"]),
            },
        ]

        for tmpl in template_seeds:
            db.execute(
                """INSERT INTO task_templates (name, description, category, default_title, default_description,
                default_location_type, default_budget_type, default_budget_amount, default_required_skills,
                is_public, created_by)
                VALUES (?,?,?,?,?,?,?,?,?,1,?)""",
                [tmpl['name'], tmpl['description'], tmpl['category'],
                 tmpl['default_title'], tmpl['default_description'],
                 tmpl['default_location_type'], tmpl['default_budget_type'],
                 tmpl['default_budget_amount'], tmpl['default_required_skills'],
                 admin_id]
            )

        db.commit()
        # Note: db.close() is handled by the try/finally in handle_request()

        # CRIT-04: do NOT return plaintext passwords in the response
        return json_response({
            "message": "Seed data created successfully",
            "ai_clients": [
                {"email": clients_data[0]['email'], "api_key": client_keys[0]},
                {"email": clients_data[1]['email'], "api_key": client_keys[1]},
            ],
            "workers": [{"email": w['email']} for w in workers_data],
            "admin": {"email": "admin@gohirehumans.com"},
            "tasks_created": 10,
            "templates_created": 5
        }, 201)

    else:
        return error_response(f"Route not found: {method} {path}", 404)

    # Note: db.close() is handled by the try/finally wrapper in handle_request()
