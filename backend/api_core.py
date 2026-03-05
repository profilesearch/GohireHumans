#!/usr/bin/env python3
"""
GoHireHumans API - CGI Backend (Redesign)
Two-sided marketplace: workers post services, employers post jobs.
Routes via PATH_INFO. Called by server.py handle_request().
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

try:
    import stripe
    STRIPE_AVAILABLE = True
except ImportError:
    STRIPE_AVAILABLE = False


# ─── Config ───────────────────────────────────────────────────────────────────

SERVICE_FEE_RATE = 0.01  # 1% platform fee charged to employer on top of amount

DB_PATH = os.environ.get("DATABASE_PATH", "gohirehumans.db")
STRIPE_SECRET_KEY = os.environ.get("STRIPE_SECRET_KEY", "").strip()
STRIPE_PUBLISHABLE_KEY = os.environ.get("STRIPE_PUBLISHABLE_KEY", "").strip()
STRIPE_WEBHOOK_SECRET = os.environ.get("STRIPE_WEBHOOK_SECRET", "").strip()
FRONTEND_URL = os.environ.get("FRONTEND_URL", "https://gohirehumans.com")
SEED_SECRET = os.environ.get("SEED_SECRET", "ghh_seed_2026_temp_kx9m4")


def stripe_configured():
    return STRIPE_AVAILABLE and bool(STRIPE_SECRET_KEY)


if stripe_configured():
    stripe.api_key = STRIPE_SECRET_KEY


# ─── Database ─────────────────────────────────────────────────────────────────

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
        name TEXT NOT NULL DEFAULT '',
        avatar_url TEXT,
        is_admin INTEGER DEFAULT 0,
        is_active INTEGER DEFAULT 1,
        is_suspended INTEGER DEFAULT 0,
        is_banned INTEGER DEFAULT 0,
        created_at TEXT DEFAULT (datetime('now')),
        updated_at TEXT DEFAULT (datetime('now'))
    );

    CREATE TABLE IF NOT EXISTS sessions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER NOT NULL REFERENCES users(id),
        token TEXT UNIQUE NOT NULL,
        expires_at TEXT NOT NULL,
        created_at TEXT DEFAULT (datetime('now'))
    );

    CREATE TABLE IF NOT EXISTS worker_profiles (
        user_id INTEGER PRIMARY KEY REFERENCES users(id),
        bio TEXT DEFAULT '',
        skills TEXT DEFAULT '[]',
        hourly_rate REAL,
        payout_method TEXT DEFAULT 'pending_setup',
        payout_account_id TEXT,
        payout_method_details TEXT,
        avg_rating REAL DEFAULT 0,
        total_reviews INTEGER DEFAULT 0,
        total_orders_completed INTEGER DEFAULT 0,
        is_verified INTEGER DEFAULT 0,
        timezone TEXT DEFAULT '',
        location TEXT DEFAULT '',
        portfolio_url TEXT DEFAULT '',
        created_at TEXT DEFAULT (datetime('now'))
    );

    CREATE TABLE IF NOT EXISTS employer_profiles (
        user_id INTEGER PRIMARY KEY REFERENCES users(id),
        company_name TEXT DEFAULT '',
        description TEXT DEFAULT '',
        website TEXT DEFAULT '',
        payment_method_id TEXT,
        stripe_customer_id TEXT,
        avg_rating REAL DEFAULT 0,
        total_reviews INTEGER DEFAULT 0,
        total_orders INTEGER DEFAULT 0,
        created_at TEXT DEFAULT (datetime('now'))
    );

    CREATE TABLE IF NOT EXISTS services (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        worker_id INTEGER NOT NULL REFERENCES users(id),
        title TEXT NOT NULL,
        description TEXT NOT NULL,
        category TEXT NOT NULL,
        pricing_type TEXT NOT NULL DEFAULT 'fixed' CHECK(pricing_type IN ('fixed','hourly','custom')),
        price REAL,
        hourly_rate REAL,
        delivery_time_days INTEGER,
        includes TEXT DEFAULT '',
        tags TEXT DEFAULT '[]',
        images TEXT DEFAULT '[]',
        status TEXT NOT NULL DEFAULT 'active' CHECK(status IN ('active','paused','removed')),
        avg_rating REAL DEFAULT 0,
        total_reviews INTEGER DEFAULT 0,
        created_at TEXT DEFAULT (datetime('now')),
        updated_at TEXT DEFAULT (datetime('now'))
    );

    CREATE TABLE IF NOT EXISTS jobs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        employer_id INTEGER NOT NULL REFERENCES users(id),
        title TEXT NOT NULL,
        description TEXT NOT NULL,
        category TEXT NOT NULL,
        location_type TEXT NOT NULL DEFAULT 'remote' CHECK(location_type IN ('remote','on_site','hybrid')),
        location_detail TEXT DEFAULT '',
        budget_type TEXT NOT NULL DEFAULT 'fixed' CHECK(budget_type IN ('fixed','hourly')),
        budget_amount REAL NOT NULL,
        estimated_hours REAL,
        required_skills TEXT DEFAULT '[]',
        due_by TEXT,
        status TEXT NOT NULL DEFAULT 'open' CHECK(status IN ('open','reviewing','hired','in_progress','completed','canceled')),
        created_at TEXT DEFAULT (datetime('now')),
        updated_at TEXT DEFAULT (datetime('now'))
    );

    CREATE TABLE IF NOT EXISTS applications (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        job_id INTEGER NOT NULL REFERENCES jobs(id),
        worker_id INTEGER NOT NULL REFERENCES users(id),
        cover_message TEXT DEFAULT '',
        portfolio_url TEXT DEFAULT '',
        status TEXT NOT NULL DEFAULT 'pending' CHECK(status IN ('pending','shortlisted','accepted','rejected')),
        created_at TEXT DEFAULT (datetime('now')),
        UNIQUE(job_id, worker_id)
    );

    CREATE TABLE IF NOT EXISTS orders (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        type TEXT NOT NULL CHECK(type IN ('service_order','job_hire')),
        service_id INTEGER REFERENCES services(id),
        job_id INTEGER REFERENCES jobs(id),
        worker_id INTEGER NOT NULL REFERENCES users(id),
        employer_id INTEGER NOT NULL REFERENCES users(id),
        status TEXT NOT NULL DEFAULT 'pending' CHECK(status IN ('pending','in_progress','submitted','revision_requested','completed','canceled','disputed')),
        total_amount REAL NOT NULL,
        worker_notes TEXT DEFAULT '',
        employer_notes TEXT DEFAULT '',
        created_at TEXT DEFAULT (datetime('now')),
        updated_at TEXT DEFAULT (datetime('now')),
        completed_at TEXT
    );

    CREATE TABLE IF NOT EXISTS milestones (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        order_id INTEGER NOT NULL REFERENCES orders(id),
        title TEXT NOT NULL,
        description TEXT DEFAULT '',
        amount REAL NOT NULL,
        sequence INTEGER NOT NULL DEFAULT 1,
        status TEXT NOT NULL DEFAULT 'pending' CHECK(status IN ('pending','funded','in_progress','submitted','approved','disputed')),
        escrow_payment_id TEXT,
        funded_at TEXT,
        released_at TEXT
    );

    CREATE TABLE IF NOT EXISTS hourly_contracts (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        order_id INTEGER NOT NULL UNIQUE REFERENCES orders(id),
        hourly_rate REAL NOT NULL,
        weekly_hour_cap REAL NOT NULL DEFAULT 40,
        current_week_escrow_amount REAL DEFAULT 0,
        current_week_escrow_payment_id TEXT,
        status TEXT NOT NULL DEFAULT 'active' CHECK(status IN ('active','paused','ended')),
        week_start_date TEXT,
        created_at TEXT DEFAULT (datetime('now'))
    );

    CREATE TABLE IF NOT EXISTS time_entries (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        contract_id INTEGER NOT NULL REFERENCES hourly_contracts(id),
        date TEXT NOT NULL,
        hours REAL NOT NULL,
        description TEXT DEFAULT '',
        status TEXT NOT NULL DEFAULT 'pending' CHECK(status IN ('pending','approved','disputed')),
        week_of TEXT NOT NULL,
        created_at TEXT DEFAULT (datetime('now'))
    );

    CREATE TABLE IF NOT EXISTS reviews (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        order_id INTEGER NOT NULL REFERENCES orders(id),
        from_user_id INTEGER NOT NULL REFERENCES users(id),
        to_user_id INTEGER NOT NULL REFERENCES users(id),
        rating INTEGER NOT NULL CHECK(rating BETWEEN 1 AND 5),
        text TEXT DEFAULT '',
        is_visible INTEGER DEFAULT 0,
        created_at TEXT DEFAULT (datetime('now')),
        UNIQUE(order_id, from_user_id)
    );

    CREATE TABLE IF NOT EXISTS escrow_holds (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        order_id INTEGER NOT NULL REFERENCES orders(id),
        milestone_id INTEGER REFERENCES milestones(id),
        amount REAL NOT NULL,
        status TEXT NOT NULL DEFAULT 'held' CHECK(status IN ('held','released','refunded','partial')),
        stripe_payment_intent_id TEXT,
        created_at TEXT DEFAULT (datetime('now')),
        released_at TEXT
    );

    CREATE TABLE IF NOT EXISTS notifications (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER NOT NULL REFERENCES users(id),
        type TEXT NOT NULL,
        title TEXT NOT NULL,
        message TEXT DEFAULT '',
        link TEXT DEFAULT '',
        is_read INTEGER DEFAULT 0,
        created_at TEXT DEFAULT (datetime('now'))
    );

    CREATE TABLE IF NOT EXISTS audit_log (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER,
        action TEXT NOT NULL,
        entity_type TEXT,
        entity_id INTEGER,
        details TEXT,
        created_at TEXT DEFAULT (datetime('now'))
    );

    CREATE TABLE IF NOT EXISTS platform_revenue (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        order_id INTEGER REFERENCES orders(id),
        fee_amount REAL NOT NULL,
        fee_type TEXT DEFAULT 'service_fee',
        created_at TEXT DEFAULT (datetime('now'))
    );

    CREATE INDEX IF NOT EXISTS idx_services_worker ON services(worker_id);
    CREATE INDEX IF NOT EXISTS idx_services_category ON services(category);
    CREATE INDEX IF NOT EXISTS idx_services_status ON services(status);
    CREATE INDEX IF NOT EXISTS idx_jobs_employer ON jobs(employer_id);
    CREATE INDEX IF NOT EXISTS idx_jobs_category ON jobs(category);
    CREATE INDEX IF NOT EXISTS idx_jobs_status ON jobs(status);
    CREATE INDEX IF NOT EXISTS idx_applications_job ON applications(job_id);
    CREATE INDEX IF NOT EXISTS idx_applications_worker ON applications(worker_id);
    CREATE INDEX IF NOT EXISTS idx_orders_worker ON orders(worker_id);
    CREATE INDEX IF NOT EXISTS idx_orders_employer ON orders(employer_id);
    CREATE INDEX IF NOT EXISTS idx_milestones_order ON milestones(order_id);
    CREATE INDEX IF NOT EXISTS idx_time_entries_contract ON time_entries(contract_id);
    CREATE INDEX IF NOT EXISTS idx_reviews_order ON reviews(order_id);
    CREATE INDEX IF NOT EXISTS idx_notifications_user ON notifications(user_id);
    CREATE INDEX IF NOT EXISTS idx_audit_log_user ON audit_log(user_id);
    CREATE INDEX IF NOT EXISTS idx_sessions_token ON sessions(token);
    """)
    db.commit()
    db.close()


# ─── Rate Limiter ──────────────────────────────────────────────────────────────

_rate_limit_store = {}
_rate_limit_lock = threading.Lock()


def check_rate_limit() -> bool:
    ip = os.environ.get("REMOTE_ADDR", "unknown")
    now = time.time()
    window = 60
    limit = 120

    with _rate_limit_lock:
        if ip in _rate_limit_store:
            _rate_limit_store[ip] = [t for t in _rate_limit_store[ip] if now - t < window]
        else:
            _rate_limit_store[ip] = []

        if len(_rate_limit_store[ip]) >= limit:
            return False
        _rate_limit_store[ip].append(now)
        return True


# ─── Content Safety ────────────────────────────────────────────────────────────

BLOCKED_KEYWORDS = [
    'illegal', 'weapon', 'gun', 'firearm', 'knife', 'ammunition', 'explosive',
    'bomb', 'arson', 'assault', 'attack', 'murder', 'kill', 'violent',
    'drug', 'narcotic', 'cocaine', 'heroin', 'meth', 'fentanyl',
    'controlled substance',
    'self-harm', 'suicide', 'self harm', 'end my life',
    'hate speech', 'racial slur', 'racist', 'sexist', 'homophobic', 'nazi',
    'white supremac', 'hate group',
    'explicit', 'adult content', 'pornograph', 'sexual', 'escort', 'companionship',
    'girlfriend experience', 'boyfriend experience', 'sugar daddy', 'sugar baby',
    'intimacy service', 'massage with happy', 'happy ending',
    'adult entertainment', 'cam girl', 'cam boy', 'onlyfans',
    'hookup', 'hook up', 'dating service',
    'body rub', 'sensual', 'erotic', 'fetish', 'dominat', 'submissive',
    'bdsm', 'nude', 'nsfw', 'xxx',
    'sex work', 'prostitut', 'call girl',
    'terroris', 'extremis', 'radicali', 'jihad',
    'hack', 'exploit', 'phishing', 'malware', 'ransomware', 'ddos',
    'identity theft', 'credit card fraud', 'scam',
    'money laundering', 'counterfeit',
    'stalk', 'spy on', 'harass', 'intimidat', 'blackmail', 'extort',
    'fake identity', 'forge', 'impersonat', 'catfish',
    'pyramid scheme', 'ponzi',
    'forced labor', 'indentured',
]

BLOCKED_PHRASES = [
    'rent my body', 'rent your body',
    'physical affection', 'personal company',
    'be my date', 'pretend to be my', 'fake girlfriend', 'fake boyfriend',
    'no questions asked', 'off the books', 'under the table',
    'untraceable', 'anonymous task',
]

VALID_CATEGORIES = [
    'web_development', 'mobile_development', 'software_development',
    'graphic_design', 'ui_ux_design', 'video_editing', 'photography',
    'writing', 'copywriting', 'translation', 'proofreading',
    'digital_marketing', 'social_media', 'seo', 'content_creation',
    'data_entry', 'virtual_assistant', 'customer_support',
    'accounting', 'bookkeeping', 'legal', 'consulting',
    'research', 'data_analysis', 'machine_learning',
    'audio_production', 'voice_over', 'music',
    'tutoring', 'coaching', 'it_support',
    'phone_call', 'in_person_errand', 'document_signing', 'media_capture',
    'expert_review', 'inspection', 'delivery', 'event_support',
    'notary', 'property_check', 'mystery_shopping', 'transcription',
    'testing', 'other'
]


def check_content_safety(text):
    lower = text.lower()
    for kw in BLOCKED_KEYWORDS:
        if kw in lower:
            return False, "Content was not approved. GoHireHumans is a professional marketplace — please review our Acceptable Use Policy."
    for phrase in BLOCKED_PHRASES:
        if phrase in lower:
            return False, "Content was not approved. GoHireHumans is a professional marketplace — please review our Acceptable Use Policy."
    return True, None


# ─── Helpers ───────────────────────────────────────────────────────────────────

def hash_password(password):
    salt = secrets.token_hex(16)
    h = hashlib.pbkdf2_hmac('sha256', password.encode(), salt.encode(), 100000)
    return f"{salt}:{h.hex()}"


def verify_password(password, stored):
    parts = stored.split(':', 1)
    if len(parts) != 2:
        return False
    salt, h = parts
    computed = hashlib.pbkdf2_hmac('sha256', password.encode(), salt.encode(), 100000)
    return hmac.compare_digest(computed.hex(), h)


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
    if not hasattr(get_body, '_cache'):
        try:
            length = int(os.environ.get("CONTENT_LENGTH", 0) or 0)
            if length > 0:
                if hasattr(get_body_raw, '_raw_cache'):
                    raw = get_body_raw._raw_cache
                else:
                    raw = sys.stdin.read(length)
                    get_body_raw._raw_cache = raw
                parsed = json.loads(raw)
                if not isinstance(parsed, dict):
                    get_body._cache = None
                else:
                    get_body._cache = parsed
            else:
                get_body._cache = {}
        except json.JSONDecodeError:
            get_body._cache = None
        except (ValueError, OSError):
            get_body._cache = None
    return get_body._cache


def get_body_raw():
    if not hasattr(get_body_raw, '_raw_cache'):
        content_length = int(os.environ.get("CONTENT_LENGTH", 0) or 0)
        if content_length > 0:
            get_body_raw._raw_cache = sys.stdin.read(content_length)
        else:
            get_body_raw._raw_cache = sys.stdin.read()
    return get_body_raw._raw_cache


def get_query_params():
    qs = os.environ.get("QUERY_STRING", "")
    return dict(urllib.parse.parse_qsl(qs))


def row_to_dict(row):
    if row is None:
        return None
    return dict(row)


def authenticate_session(db):
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

    row = db.execute(
        "SELECT user_id FROM sessions WHERE token = ? AND expires_at > datetime('now')",
        [token]
    ).fetchone()
    if row:
        user = db.execute("SELECT * FROM users WHERE id = ?", [row['user_id']]).fetchone()
        if user and user['is_active'] and not user['is_banned']:
            return row_to_dict(user)
    return None


def authenticate(db):
    return authenticate_session(db)


def audit(db, user_id, action, entity_type=None, entity_id=None, details=None):
    db.execute(
        "INSERT INTO audit_log (user_id, action, entity_type, entity_id, details) VALUES (?,?,?,?,?)",
        [user_id, action, entity_type, entity_id, json.dumps(details) if details else None]
    )


def push_notification(db, user_id, notif_type, title, message=None, link=None):
    db.execute(
        "INSERT INTO notifications (user_id, type, title, message, link) VALUES (?,?,?,?,?)",
        [user_id, notif_type, title, message or "", link or ""]
    )


def fake_payment_intent_id():
    return f"pi_sim_{secrets.token_hex(12)}"


def user_has_worker_profile(db, user_id):
    return db.execute("SELECT user_id FROM worker_profiles WHERE user_id = ?", [user_id]).fetchone() is not None


def user_has_employer_profile(db, user_id):
    return db.execute("SELECT user_id FROM employer_profiles WHERE user_id = ?", [user_id]).fetchone() is not None


def ensure_worker_profile(db, user_id):
    """Create a minimal worker profile if not exists."""
    if not user_has_worker_profile(db, user_id):
        db.execute(
            "INSERT INTO worker_profiles (user_id) VALUES (?)",
            [user_id]
        )


def ensure_employer_profile(db, user_id):
    """Create a minimal employer profile if not exists."""
    if not user_has_employer_profile(db, user_id):
        db.execute(
            "INSERT INTO employer_profiles (user_id) VALUES (?)",
            [user_id]
        )


def worker_has_payout_setup(db, user_id):
    wp = db.execute("SELECT payout_account_id, payout_method FROM worker_profiles WHERE user_id = ?", [user_id]).fetchone()
    if not wp:
        return False
    return bool(wp['payout_account_id']) and wp['payout_method'] not in ('pending_setup', None, '')


def employer_has_payment_setup(db, user_id):
    ep = db.execute("SELECT payment_method_id, stripe_customer_id FROM employer_profiles WHERE user_id = ?", [user_id]).fetchone()
    if not ep:
        return False
    return bool(ep['payment_method_id']) or bool(ep['stripe_customer_id'])


def release_escrow_to_worker(db, order_id, milestone_id, amount, worker_id):
    """Release escrow hold, transfer to worker via Stripe or simulation."""
    # Mark escrow released
    db.execute(
        "UPDATE escrow_holds SET status='released', released_at=datetime('now') WHERE order_id=? AND (milestone_id=? OR milestone_id IS NULL) AND status='held'",
        [order_id, milestone_id]
    )
    # Platform fee
    fee = round(amount * SERVICE_FEE_RATE, 2)
    worker_payout = round(amount - fee, 2)

    db.execute(
        "INSERT INTO platform_revenue (order_id, fee_amount, fee_type) VALUES (?,?,?)",
        [order_id, fee, 'service_fee']
    )

    # Attempt Stripe transfer if configured
    if stripe_configured():
        wp = db.execute("SELECT payout_account_id FROM worker_profiles WHERE user_id = ?", [worker_id]).fetchone()
        if wp and wp['payout_account_id'] and not wp['payout_account_id'].startswith('acct_sim_'):
            try:
                stripe.Transfer.create(
                    amount=int(worker_payout * 100),
                    currency="usd",
                    destination=wp['payout_account_id'],
                    metadata={"order_id": str(order_id), "milestone_id": str(milestone_id or "")},
                    description=f"GoHireHumans escrow release order #{order_id}"
                )
            except stripe.error.StripeError:
                pass  # Log but don't fail the flow; admin can retry

    return worker_payout, fee


def fund_escrow_stripe(db, employer_id, amount, order_id, milestone_id=None, description="Escrow hold"):
    """
    Fund escrow. Returns (payment_intent_id, mode).
    - With Stripe: create PaymentIntent with capture_method=manual, then capture it.
      Platform charges employer's saved payment method.
    - Without Stripe: simulate.
    """
    ep = db.execute("SELECT stripe_customer_id, payment_method_id FROM employer_profiles WHERE user_id = ?", [employer_id]).fetchone()

    if stripe_configured() and ep and ep['stripe_customer_id'] and ep['payment_method_id']:
        try:
            total_charge = int((amount * (1 + SERVICE_FEE_RATE)) * 100)  # employer pays amount + 1% fee
            pi = stripe.PaymentIntent.create(
                amount=total_charge,
                currency="usd",
                customer=ep['stripe_customer_id'],
                payment_method=ep['payment_method_id'],
                confirm=True,
                off_session=True,
                capture_method="automatic",
                description=description,
                metadata={
                    "order_id": str(order_id),
                    "milestone_id": str(milestone_id or ""),
                    "employer_id": str(employer_id),
                }
            )
            pi_id = pi.id
            mode = "live"
        except stripe.error.StripeError as e:
            raise ValueError(f"Payment failed: {str(e)}")
    else:
        pi_id = fake_payment_intent_id()
        mode = "simulated"

    # Record escrow hold
    db.execute(
        "INSERT INTO escrow_holds (order_id, milestone_id, amount, status, stripe_payment_intent_id) VALUES (?,?,?,'held',?)",
        [order_id, milestone_id, amount, pi_id]
    )
    return pi_id, mode


# ─── Route Handler ─────────────────────────────────────────────────────────────

def handle_request():
    # Clear per-request caches
    if hasattr(get_body, '_cache'):
        del get_body._cache
    if hasattr(get_body_raw, '_raw_cache'):
        del get_body_raw._raw_cache

    init_db()

    if not check_rate_limit():
        print("Status: 429")
        print("Content-Type: application/json")
        print()
        print(json.dumps({"error": "Rate limit exceeded", "retry_after": 60}))
        return

    db = get_db()
    try:
        _handle_routes(db)
    finally:
        db.close()


def _handle_routes(db):
    method = os.environ.get("REQUEST_METHOD", "GET")
    path = os.environ.get("PATH_INFO", "").rstrip("/")
    params = get_query_params()

    # Centralized JSON body guard for mutating methods
    if method in ("POST", "PUT", "PATCH") and path != "/webhooks/stripe":
        if get_body() is None:
            return error_response("Invalid JSON in request body", 400)

    # ═══════════════════════════════════════════════════════════════════════════
    # AUTH ROUTES
    # ═══════════════════════════════════════════════════════════════════════════

    if path == "/auth/register" and method == "POST":
        body = get_body()
        email = body.get("email", "").strip().lower()
        password = body.get("password", "")
        name = body.get("name", "").strip()

        if not email or not password:
            return error_response("Email and password required")
        if len(password) < 8:
            return error_response("Password must be at least 8 characters")
        if not name:
            return error_response("Name required")
        if not re.match(r'^[^@]+@[^@]+\.[^@]+$', email):
            return error_response("Invalid email address")

        existing = db.execute("SELECT id FROM users WHERE email = ?", [email]).fetchone()
        if existing:
            return error_response("Email already registered", 409)

        pw_hash = hash_password(password)
        cursor = db.execute(
            "INSERT INTO users (email, password_hash, name) VALUES (?,?,?)",
            [email, pw_hash, name]
        )
        user_id = cursor.lastrowid

        token = generate_session_token()
        expires = (datetime.now(timezone.utc) + timedelta(days=30)).isoformat()
        db.execute("INSERT INTO sessions (user_id, token, expires_at) VALUES (?,?,?)", [user_id, token, expires])
        audit(db, user_id, "register", "user", user_id)
        db.commit()

        return json_response({
            "id": user_id,
            "email": email,
            "name": name,
            "is_admin": 0,
            "token": token,
            "worker_profile": None,
            "employer_profile": None
        }, 201)

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
        expires = (datetime.now(timezone.utc) + timedelta(days=30)).isoformat()
        db.execute("INSERT INTO sessions (user_id, token, expires_at) VALUES (?,?,?)", [user['id'], token, expires])
        audit(db, user['id'], "login", "user", user['id'])
        db.commit()

        user_data = row_to_dict(user)
        del user_data['password_hash']
        user_data['token'] = token

        wp = db.execute("SELECT * FROM worker_profiles WHERE user_id = ?", [user['id']]).fetchone()
        ep = db.execute("SELECT * FROM employer_profiles WHERE user_id = ?", [user['id']]).fetchone()
        user_data['worker_profile'] = row_to_dict(wp)
        user_data['employer_profile'] = row_to_dict(ep)

        return json_response(user_data)

    elif path == "/auth/logout" and method == "POST":
        auth_header = os.environ.get("HTTP_AUTHORIZATION", "")
        token = auth_header[7:].strip() if auth_header.startswith("Bearer ") else None
        if not token:
            token = params.get("token")
        if token:
            db.execute("DELETE FROM sessions WHERE token = ?", [token])
            db.commit()
        return json_response({"ok": True})

    # ═══════════════════════════════════════════════════════════════════════════
    # PROFILE ROUTES
    # ═══════════════════════════════════════════════════════════════════════════

    elif path == "/profile" and method == "GET":
        user = authenticate(db)
        if not user:
            return error_response("Unauthorized", 401)
        ud = dict(user)
        del ud['password_hash']
        wp = db.execute("SELECT * FROM worker_profiles WHERE user_id = ?", [user['id']]).fetchone()
        ep = db.execute("SELECT * FROM employer_profiles WHERE user_id = ?", [user['id']]).fetchone()
        ud['worker_profile'] = row_to_dict(wp)
        ud['employer_profile'] = row_to_dict(ep)
        return json_response(ud)

    elif path == "/profile" and method == "PUT":
        user = authenticate(db)
        if not user:
            return error_response("Unauthorized", 401)
        body = get_body()

        updates = []
        vals = []
        for field in ['name', 'avatar_url']:
            if field in body:
                updates.append(f"{field} = ?")
                vals.append(body[field])
        if updates:
            vals.append(user['id'])
            db.execute(f"UPDATE users SET {', '.join(updates)}, updated_at=datetime('now') WHERE id = ?", vals)

        audit(db, user['id'], "update_profile", "user", user['id'])
        db.commit()
        return json_response({"ok": True})

    elif path == "/profile/worker" and method == "GET":
        user = authenticate(db)
        if not user:
            return error_response("Unauthorized", 401)
        wp = db.execute("SELECT * FROM worker_profiles WHERE user_id = ?", [user['id']]).fetchone()
        return json_response(row_to_dict(wp))

    elif path == "/profile/worker" and method == "PUT":
        user = authenticate(db)
        if not user:
            return error_response("Unauthorized", 401)
        body = get_body()

        # Content screen bio
        if body.get('bio'):
            safe, msg = check_content_safety(body['bio'])
            if not safe:
                return error_response(f"Bio rejected: {msg}", 422)

        if not user_has_worker_profile(db, user['id']):
            db.execute("INSERT INTO worker_profiles (user_id) VALUES (?)", [user['id']])

        updates = []
        vals = []
        for field in ['bio', 'hourly_rate', 'payout_method', 'timezone', 'location', 'portfolio_url']:
            if field in body:
                updates.append(f"{field} = ?")
                vals.append(body[field])
        if 'skills' in body:
            updates.append("skills = ?")
            vals.append(json.dumps(body['skills']) if isinstance(body['skills'], list) else body['skills'])
        if updates:
            vals.append(user['id'])
            db.execute(f"UPDATE worker_profiles SET {', '.join(updates)} WHERE user_id = ?", vals)

        audit(db, user['id'], "update_worker_profile", "worker_profile", user['id'])
        db.commit()
        wp = db.execute("SELECT * FROM worker_profiles WHERE user_id = ?", [user['id']]).fetchone()
        return json_response(row_to_dict(wp))

    elif path == "/profile/employer" and method == "GET":
        user = authenticate(db)
        if not user:
            return error_response("Unauthorized", 401)
        ep = db.execute("SELECT * FROM employer_profiles WHERE user_id = ?", [user['id']]).fetchone()
        return json_response(row_to_dict(ep))

    elif path == "/profile/employer" and method == "PUT":
        user = authenticate(db)
        if not user:
            return error_response("Unauthorized", 401)
        body = get_body()

        if not user_has_employer_profile(db, user['id']):
            db.execute("INSERT INTO employer_profiles (user_id) VALUES (?)", [user['id']])

        updates = []
        vals = []
        for field in ['company_name', 'description', 'website']:
            if field in body:
                updates.append(f"{field} = ?")
                vals.append(body[field])
        if updates:
            vals.append(user['id'])
            db.execute(f"UPDATE employer_profiles SET {', '.join(updates)} WHERE user_id = ?", vals)

        audit(db, user['id'], "update_employer_profile", "employer_profile", user['id'])
        db.commit()
        ep = db.execute("SELECT * FROM employer_profiles WHERE user_id = ?", [user['id']]).fetchone()
        return json_response(row_to_dict(ep))

    # ═══════════════════════════════════════════════════════════════════════════
    # CATEGORIES
    # ═══════════════════════════════════════════════════════════════════════════

    elif path == "/categories" and method == "GET":
        return json_response({"categories": VALID_CATEGORIES})

    # ═══════════════════════════════════════════════════════════════════════════
    # SERVICES (Public browse, auth for mutations)
    # ═══════════════════════════════════════════════════════════════════════════

    elif path == "/services" and method == "GET":
        page = max(1, int(params.get("page", 1)))
        per_page = min(int(params.get("per_page", 20)), 100)
        offset = (page - 1) * per_page
        category = params.get("category")
        search = params.get("search", "").strip()
        min_price = params.get("min_price")
        max_price = params.get("max_price")
        pricing_type = params.get("pricing_type")

        conditions = ["s.status = 'active'"]
        values = []

        if category:
            conditions.append("s.category = ?")
            values.append(category)
        if pricing_type:
            conditions.append("s.pricing_type = ?")
            values.append(pricing_type)
        if min_price:
            conditions.append("(s.price >= ? OR s.hourly_rate >= ?)")
            values.extend([float(min_price), float(min_price)])
        if max_price:
            conditions.append("(s.price <= ? OR s.hourly_rate <= ?)")
            values.extend([float(max_price), float(max_price)])
        if search:
            conditions.append("(s.title LIKE ? OR s.description LIKE ? OR s.tags LIKE ?)")
            pct = f"%{search}%"
            values.extend([pct, pct, pct])

        where = " AND ".join(conditions)
        count = db.execute(f"SELECT COUNT(*) as c FROM services s WHERE {where}", values).fetchone()['c']
        rows = db.execute(
            f"""SELECT s.*, u.name as worker_name, u.avatar_url as worker_avatar,
                wp.avg_rating as worker_rating, wp.total_reviews as worker_review_count,
                wp.is_verified as worker_is_verified
                FROM services s
                JOIN users u ON s.worker_id = u.id
                LEFT JOIN worker_profiles wp ON s.worker_id = wp.user_id
                WHERE {where}
                ORDER BY s.avg_rating DESC, s.total_reviews DESC, s.created_at DESC
                LIMIT ? OFFSET ?""",
            values + [per_page, offset]
        ).fetchall()

        return json_response({
            "services": [row_to_dict(r) for r in rows],
            "total": count,
            "page": page,
            "per_page": per_page,
            "total_pages": (count + per_page - 1) // per_page
        })

    elif re.match(r"^/services/(\d+)$", path) and method == "GET":
        service_id = int(re.match(r"^/services/(\d+)$", path).group(1))
        row = db.execute(
            """SELECT s.*, u.name as worker_name, u.avatar_url as worker_avatar,
               wp.bio as worker_bio, wp.avg_rating as worker_rating,
               wp.total_reviews as worker_review_count, wp.is_verified as worker_is_verified,
               wp.skills as worker_skills
               FROM services s
               JOIN users u ON s.worker_id = u.id
               LEFT JOIN worker_profiles wp ON s.worker_id = wp.user_id
               WHERE s.id = ? AND s.status != 'removed'""",
            [service_id]
        ).fetchone()
        if not row:
            return error_response("Service not found", 404)
        return json_response(row_to_dict(row))

    elif path == "/services" and method == "POST":
        user = authenticate(db)
        if not user:
            return error_response("Unauthorized", 401)
        body = get_body()

        for field in ['title', 'description', 'category']:
            if not body.get(field):
                return error_response(f"Missing required field: {field}")

        if body['category'] not in VALID_CATEGORIES:
            return error_response(f"Invalid category. Must be one of: {', '.join(VALID_CATEGORIES)}")

        safe, msg = check_content_safety(body['title'] + " " + body['description'])
        if not safe:
            return error_response(f"Service rejected: {msg}", 422)

        # Must have worker profile with payout setup
        ensure_worker_profile(db, user['id'])
        if not worker_has_payout_setup(db, user['id']):
            return error_response("You must set up a payout method before posting services. Use /payments/setup-worker.", 402)

        pricing_type = body.get("pricing_type", "fixed")
        if pricing_type not in ('fixed', 'hourly', 'custom'):
            return error_response("pricing_type must be fixed, hourly, or custom")

        price = body.get("price")
        hourly_rate = body.get("hourly_rate")

        if pricing_type == 'fixed' and not price:
            return error_response("price required for fixed pricing")
        if pricing_type == 'hourly' and not hourly_rate:
            return error_response("hourly_rate required for hourly pricing")

        tags = body.get("tags", [])
        images = body.get("images", [])

        cursor = db.execute(
            """INSERT INTO services
               (worker_id, title, description, category, pricing_type, price, hourly_rate,
                delivery_time_days, includes, tags, images, status)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,'active')""",
            [user['id'], body['title'], body['description'], body['category'],
             pricing_type, price, hourly_rate,
             body.get("delivery_time_days"),
             body.get("includes", ""),
             json.dumps(tags) if isinstance(tags, list) else tags,
             json.dumps(images) if isinstance(images, list) else images]
        )
        service_id = cursor.lastrowid
        audit(db, user['id'], "create_service", "service", service_id)
        db.commit()
        svc = db.execute("SELECT * FROM services WHERE id = ?", [service_id]).fetchone()
        return json_response(row_to_dict(svc), 201)

    elif re.match(r"^/services/(\d+)$", path) and method == "PUT":
        user = authenticate(db)
        if not user:
            return error_response("Unauthorized", 401)
        service_id = int(re.match(r"^/services/(\d+)$", path).group(1))
        svc = db.execute("SELECT * FROM services WHERE id = ?", [service_id]).fetchone()
        if not svc:
            return error_response("Service not found", 404)
        if svc['worker_id'] != user['id'] and not user['is_admin']:
            return error_response("Forbidden", 403)

        body = get_body()
        if body.get('title') or body.get('description'):
            txt = (body.get('title') or svc['title']) + " " + (body.get('description') or svc['description'])
            safe, msg = check_content_safety(txt)
            if not safe:
                return error_response(f"Service update rejected: {msg}", 422)

        updates = []
        vals = []
        for field in ['title', 'description', 'category', 'pricing_type', 'price', 'hourly_rate',
                      'delivery_time_days', 'includes', 'status']:
            if field in body:
                if field == 'category' and body[field] not in VALID_CATEGORIES:
                    return error_response("Invalid category")
                if field == 'status' and body[field] not in ('active', 'paused', 'removed'):
                    return error_response("Invalid status")
                updates.append(f"{field} = ?")
                vals.append(body[field])
        for field in ['tags', 'images']:
            if field in body:
                updates.append(f"{field} = ?")
                vals.append(json.dumps(body[field]) if isinstance(body[field], list) else body[field])
        if updates:
            updates.append("updated_at = datetime('now')")
            vals.append(service_id)
            db.execute(f"UPDATE services SET {', '.join(updates)} WHERE id = ?", vals)
        audit(db, user['id'], "update_service", "service", service_id)
        db.commit()
        svc = db.execute("SELECT * FROM services WHERE id = ?", [service_id]).fetchone()
        return json_response(row_to_dict(svc))

    elif re.match(r"^/services/(\d+)$", path) and method == "DELETE":
        user = authenticate(db)
        if not user:
            return error_response("Unauthorized", 401)
        service_id = int(re.match(r"^/services/(\d+)$", path).group(1))
        svc = db.execute("SELECT * FROM services WHERE id = ?", [service_id]).fetchone()
        if not svc:
            return error_response("Service not found", 404)
        if svc['worker_id'] != user['id'] and not user['is_admin']:
            return error_response("Forbidden", 403)
        db.execute("UPDATE services SET status='removed', updated_at=datetime('now') WHERE id = ?", [service_id])
        audit(db, user['id'], "delete_service", "service", service_id)
        db.commit()
        return json_response({"ok": True})

    # ═══════════════════════════════════════════════════════════════════════════
    # JOBS (Public browse, auth for mutations)
    # ═══════════════════════════════════════════════════════════════════════════

    elif path == "/jobs" and method == "GET":
        page = max(1, int(params.get("page", 1)))
        per_page = min(int(params.get("per_page", 20)), 100)
        offset = (page - 1) * per_page
        category = params.get("category")
        search = params.get("search", "").strip()
        location_type = params.get("location_type")
        budget_type = params.get("budget_type")
        min_budget = params.get("min_budget")
        max_budget = params.get("max_budget")
        status_filter = params.get("status", "open")

        conditions = ["j.status = ?"]
        values = [status_filter]

        if category:
            conditions.append("j.category = ?")
            values.append(category)
        if location_type:
            conditions.append("j.location_type = ?")
            values.append(location_type)
        if budget_type:
            conditions.append("j.budget_type = ?")
            values.append(budget_type)
        if min_budget:
            conditions.append("j.budget_amount >= ?")
            values.append(float(min_budget))
        if max_budget:
            conditions.append("j.budget_amount <= ?")
            values.append(float(max_budget))
        if search:
            conditions.append("(j.title LIKE ? OR j.description LIKE ?)")
            pct = f"%{search}%"
            values.extend([pct, pct])

        where = " AND ".join(conditions)
        count = db.execute(f"SELECT COUNT(*) as c FROM jobs j WHERE {where}", values).fetchone()['c']
        rows = db.execute(
            f"""SELECT j.*, u.name as employer_name, u.avatar_url as employer_avatar,
                ep.company_name, ep.avg_rating as employer_rating
                FROM jobs j
                JOIN users u ON j.employer_id = u.id
                LEFT JOIN employer_profiles ep ON j.employer_id = ep.user_id
                WHERE {where}
                ORDER BY j.created_at DESC
                LIMIT ? OFFSET ?""",
            values + [per_page, offset]
        ).fetchall()

        return json_response({
            "jobs": [row_to_dict(r) for r in rows],
            "total": count,
            "page": page,
            "per_page": per_page,
            "total_pages": (count + per_page - 1) // per_page
        })

    elif re.match(r"^/jobs/(\d+)$", path) and method == "GET":
        job_id = int(re.match(r"^/jobs/(\d+)$", path).group(1))
        row = db.execute(
            """SELECT j.*, u.name as employer_name, u.avatar_url as employer_avatar,
               ep.company_name, ep.avg_rating as employer_rating,
               ep.description as employer_description
               FROM jobs j
               JOIN users u ON j.employer_id = u.id
               LEFT JOIN employer_profiles ep ON j.employer_id = ep.user_id
               WHERE j.id = ?""",
            [job_id]
        ).fetchone()
        if not row:
            return error_response("Job not found", 404)
        result = row_to_dict(row)
        # Count applications (not listing them)
        result['application_count'] = db.execute(
            "SELECT COUNT(*) as c FROM applications WHERE job_id = ?", [job_id]
        ).fetchone()['c']
        return json_response(result)

    elif path == "/jobs" and method == "POST":
        user = authenticate(db)
        if not user:
            return error_response("Unauthorized", 401)
        body = get_body()

        for field in ['title', 'description', 'category', 'budget_type', 'budget_amount']:
            if not body.get(field) and body.get(field) != 0:
                return error_response(f"Missing required field: {field}")

        if body['category'] not in VALID_CATEGORIES:
            return error_response(f"Invalid category")

        if body['budget_type'] not in ('fixed', 'hourly'):
            return error_response("budget_type must be fixed or hourly")

        budget = float(body['budget_amount'])
        if budget <= 0 or budget > 1000000:
            return error_response("budget_amount must be positive and <= 1,000,000")

        safe, msg = check_content_safety(body['title'] + " " + body['description'])
        if not safe:
            return error_response(f"Job rejected: {msg}", 422)

        location_type = body.get("location_type", "remote")
        if location_type not in ('remote', 'on_site', 'hybrid'):
            return error_response("location_type must be remote, on_site, or hybrid")

        # Auto-create employer profile if needed
        ensure_employer_profile(db, user['id'])

        required_skills = body.get("required_skills", [])
        cursor = db.execute(
            """INSERT INTO jobs
               (employer_id, title, description, category, location_type, location_detail,
                budget_type, budget_amount, estimated_hours, required_skills, due_by, status)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,'open')""",
            [user['id'], body['title'], body['description'], body['category'],
             location_type, body.get("location_detail", ""),
             body['budget_type'], budget,
             body.get("estimated_hours"),
             json.dumps(required_skills) if isinstance(required_skills, list) else required_skills,
             body.get("due_by")]
        )
        job_id = cursor.lastrowid
        audit(db, user['id'], "create_job", "job", job_id)
        db.commit()
        job = db.execute("SELECT * FROM jobs WHERE id = ?", [job_id]).fetchone()
        return json_response(row_to_dict(job), 201)

    elif re.match(r"^/jobs/(\d+)$", path) and method == "PUT":
        user = authenticate(db)
        if not user:
            return error_response("Unauthorized", 401)
        job_id = int(re.match(r"^/jobs/(\d+)$", path).group(1))
        job = db.execute("SELECT * FROM jobs WHERE id = ?", [job_id]).fetchone()
        if not job:
            return error_response("Job not found", 404)
        if job['employer_id'] != user['id'] and not user['is_admin']:
            return error_response("Forbidden", 403)
        if job['status'] not in ('open', 'reviewing'):
            return error_response("Can only edit open or reviewing jobs", 409)

        body = get_body()
        if body.get('title') or body.get('description'):
            txt = (body.get('title') or job['title']) + " " + (body.get('description') or job['description'])
            safe, msg = check_content_safety(txt)
            if not safe:
                return error_response(f"Job update rejected: {msg}", 422)

        updates = []
        vals = []
        for field in ['title', 'description', 'category', 'location_type', 'location_detail',
                      'budget_type', 'estimated_hours', 'due_by', 'status']:
            if field in body:
                updates.append(f"{field} = ?")
                vals.append(body[field])
        if 'budget_amount' in body:
            updates.append("budget_amount = ?")
            vals.append(float(body['budget_amount']))
        if 'required_skills' in body:
            updates.append("required_skills = ?")
            vals.append(json.dumps(body['required_skills']) if isinstance(body['required_skills'], list) else body['required_skills'])
        if updates:
            updates.append("updated_at = datetime('now')")
            vals.append(job_id)
            db.execute(f"UPDATE jobs SET {', '.join(updates)} WHERE id = ?", vals)
        audit(db, user['id'], "update_job", "job", job_id)
        db.commit()
        job = db.execute("SELECT * FROM jobs WHERE id = ?", [job_id]).fetchone()
        return json_response(row_to_dict(job))

    elif re.match(r"^/jobs/(\d+)$", path) and method == "DELETE":
        user = authenticate(db)
        if not user:
            return error_response("Unauthorized", 401)
        job_id = int(re.match(r"^/jobs/(\d+)$", path).group(1))
        job = db.execute("SELECT * FROM jobs WHERE id = ?", [job_id]).fetchone()
        if not job:
            return error_response("Job not found", 404)
        if job['employer_id'] != user['id'] and not user['is_admin']:
            return error_response("Forbidden", 403)
        if job['status'] in ('in_progress', 'completed'):
            return error_response("Cannot cancel a job that is in progress or completed", 409)
        db.execute("UPDATE jobs SET status='canceled', updated_at=datetime('now') WHERE id = ?", [job_id])
        audit(db, user['id'], "cancel_job", "job", job_id)
        db.commit()
        return json_response({"ok": True})

    # ═══════════════════════════════════════════════════════════════════════════
    # JOB APPLICATIONS
    # ═══════════════════════════════════════════════════════════════════════════

    elif re.match(r"^/jobs/(\d+)/applications$", path) and method == "GET":
        user = authenticate(db)
        if not user:
            return error_response("Unauthorized", 401)
        job_id = int(re.match(r"^/jobs/(\d+)/applications$", path).group(1))
        job = db.execute("SELECT * FROM jobs WHERE id = ?", [job_id]).fetchone()
        if not job:
            return error_response("Job not found", 404)
        if job['employer_id'] != user['id'] and not user['is_admin']:
            return error_response("Forbidden — only the job owner can view applicants", 403)

        apps = db.execute(
            """SELECT a.*, u.name as worker_name, u.avatar_url as worker_avatar,
               wp.bio as worker_bio, wp.avg_rating as worker_rating,
               wp.total_reviews as worker_review_count, wp.skills as worker_skills,
               wp.is_verified as worker_is_verified
               FROM applications a
               JOIN users u ON a.worker_id = u.id
               LEFT JOIN worker_profiles wp ON a.worker_id = wp.user_id
               WHERE a.job_id = ?
               ORDER BY a.created_at DESC""",
            [job_id]
        ).fetchall()
        return json_response([row_to_dict(a) for a in apps])

    elif re.match(r"^/jobs/(\d+)/apply$", path) and method == "POST":
        user = authenticate(db)
        if not user:
            return error_response("Unauthorized", 401)
        job_id = int(re.match(r"^/jobs/(\d+)/apply$", path).group(1))
        job = db.execute("SELECT * FROM jobs WHERE id = ?", [job_id]).fetchone()
        if not job:
            return error_response("Job not found", 404)
        if job['status'] not in ('open', 'reviewing'):
            return error_response("This job is not accepting applications", 409)
        if job['employer_id'] == user['id']:
            return error_response("You cannot apply to your own job", 403)

        # Ensure worker profile exists
        ensure_worker_profile(db, user['id'])

        body = get_body()
        existing = db.execute(
            "SELECT id FROM applications WHERE job_id = ? AND worker_id = ?",
            [job_id, user['id']]
        ).fetchone()
        if existing:
            return error_response("You have already applied to this job", 409)

        cursor = db.execute(
            "INSERT INTO applications (job_id, worker_id, cover_message, portfolio_url) VALUES (?,?,?,?)",
            [job_id, user['id'], body.get("cover_message", ""), body.get("portfolio_url", "")]
        )
        app_id = cursor.lastrowid

        # Update job status to reviewing if it's open
        db.execute(
            "UPDATE jobs SET status='reviewing', updated_at=datetime('now') WHERE id = ? AND status='open'",
            [job_id]
        )

        # Notify employer
        push_notification(db, job['employer_id'], "new_application",
            f"New application: {job['title']}",
            f"{user['name']} applied to your job.",
            f"/jobs/{job_id}/applications")

        audit(db, user['id'], "apply_job", "application", app_id)
        db.commit()
        app = db.execute("SELECT * FROM applications WHERE id = ?", [app_id]).fetchone()
        return json_response(row_to_dict(app), 201)

    # ═══════════════════════════════════════════════════════════════════════════
    # HIRING FLOW
    # ═══════════════════════════════════════════════════════════════════════════

    elif re.match(r"^/jobs/(\d+)/hire$", path) and method == "POST":
        user = authenticate(db)
        if not user:
            return error_response("Unauthorized", 401)
        job_id = int(re.match(r"^/jobs/(\d+)/hire$", path).group(1))
        job = db.execute("SELECT * FROM jobs WHERE id = ?", [job_id]).fetchone()
        if not job:
            return error_response("Job not found", 404)
        if job['employer_id'] != user['id']:
            return error_response("Forbidden", 403)
        if job['status'] not in ('open', 'reviewing'):
            return error_response("Job must be open or reviewing to hire", 409)

        body = get_body()
        applicant_id = body.get("applicant_id") or body.get("worker_id")
        if not applicant_id:
            return error_response("applicant_id required")

        # Verify application exists
        app = db.execute(
            "SELECT * FROM applications WHERE job_id = ? AND worker_id = ?",
            [job_id, int(applicant_id)]
        ).fetchone()
        if not app:
            return error_response("Application not found for this worker", 404)

        # Check employer has payment setup
        ensure_employer_profile(db, user['id'])
        if not employer_has_payment_setup(db, user['id']):
            return error_response("You must set up a payment method before hiring. Use /payments/setup-employer.", 402)

        worker_id = int(applicant_id)
        total_amount = float(job['budget_amount'])

        # Create order
        cursor = db.execute(
            """INSERT INTO orders (type, job_id, worker_id, employer_id, status, total_amount)
               VALUES ('job_hire', ?, ?, ?, 'in_progress', ?)""",
            [job_id, worker_id, user['id'], total_amount]
        )
        order_id = cursor.lastrowid

        if job['budget_type'] == 'fixed':
            # Fixed-price: set up milestones
            milestones_input = body.get("milestones", [])
            if not milestones_input:
                # Default: 1 milestone = full amount
                milestones_input = [{"title": "Project completion", "description": "Full project deliverable", "amount": total_amount}]

            # Validate milestone amounts sum to total
            ms_total = sum(float(m.get("amount", 0)) for m in milestones_input)
            if abs(ms_total - total_amount) > 0.01:
                db.rollback()
                return error_response(f"Milestone amounts ({ms_total}) must sum to job budget ({total_amount})", 400)

            milestone_ids = []
            for seq, m in enumerate(milestones_input, 1):
                mc = db.execute(
                    "INSERT INTO milestones (order_id, title, description, amount, sequence, status) VALUES (?,?,?,?,?,'pending')",
                    [order_id, m.get("title", f"Milestone {seq}"), m.get("description", ""), float(m['amount']), seq]
                )
                milestone_ids.append(mc.lastrowid)

            # Fund first milestone escrow immediately
            first_ms_id = milestone_ids[0]
            first_ms_amount = float(milestones_input[0]['amount'])
            try:
                pi_id, mode = fund_escrow_stripe(
                    db, user['id'], first_ms_amount, order_id, first_ms_id,
                    f"Escrow for job #{job_id} milestone 1"
                )
            except ValueError as e:
                db.rollback()
                return error_response(str(e), 402)

            # Mark first milestone as funded/in_progress
            db.execute(
                "UPDATE milestones SET status='in_progress', escrow_payment_id=?, funded_at=datetime('now') WHERE id=?",
                [pi_id, first_ms_id]
            )

        elif job['budget_type'] == 'hourly':
            # Hourly: create hourly contract, fund 1 week's max escrow
            hourly_rate = float(body.get("hourly_rate") or job['budget_amount'])
            weekly_cap = float(body.get("weekly_hour_cap", 40))
            week_escrow = round(hourly_rate * weekly_cap, 2)

            week_start = datetime.now(timezone.utc).strftime("%Y-%m-%d")
            hc = db.execute(
                """INSERT INTO hourly_contracts
                   (order_id, hourly_rate, weekly_hour_cap, current_week_escrow_amount, week_start_date, status)
                   VALUES (?,?,?,?,?,'active')""",
                [order_id, hourly_rate, weekly_cap, week_escrow, week_start]
            )
            contract_id = hc.lastrowid

            # Fund first week escrow
            try:
                pi_id, mode = fund_escrow_stripe(
                    db, user['id'], week_escrow, order_id, None,
                    f"Hourly contract #{contract_id} first week escrow"
                )
            except ValueError as e:
                db.rollback()
                return error_response(str(e), 402)

            db.execute(
                "UPDATE hourly_contracts SET current_week_escrow_payment_id=? WHERE id=?",
                [pi_id, contract_id]
            )

        # Update job status
        db.execute("UPDATE jobs SET status='hired', updated_at=datetime('now') WHERE id = ?", [job_id])
        # Accept this application, reject others
        db.execute(
            "UPDATE applications SET status='accepted' WHERE job_id=? AND worker_id=?",
            [job_id, worker_id]
        )
        db.execute(
            "UPDATE applications SET status='rejected' WHERE job_id=? AND worker_id!=?",
            [job_id, worker_id]
        )

        # Notify worker
        push_notification(db, worker_id, "job_hired",
            f"You've been hired!",
            f"You've been hired for: {job['title']}",
            f"/orders/{order_id}")

        audit(db, user['id'], "hire_worker", "order", order_id, {"job_id": job_id, "worker_id": worker_id})
        db.commit()

        order = db.execute("SELECT * FROM orders WHERE id = ?", [order_id]).fetchone()
        result = row_to_dict(order)
        if job['budget_type'] == 'fixed':
            mss = db.execute("SELECT * FROM milestones WHERE order_id = ? ORDER BY sequence", [order_id]).fetchall()
            result['milestones'] = [row_to_dict(m) for m in mss]
        else:
            hc_row = db.execute("SELECT * FROM hourly_contracts WHERE order_id = ?", [order_id]).fetchone()
            result['hourly_contract'] = row_to_dict(hc_row)
        return json_response(result, 201)

    # ═══════════════════════════════════════════════════════════════════════════
    # SERVICE ORDERS (Purchase a service)
    # ═══════════════════════════════════════════════════════════════════════════

    elif re.match(r"^/services/(\d+)/order$", path) and method == "POST":
        user = authenticate(db)
        if not user:
            return error_response("Unauthorized", 401)
        service_id = int(re.match(r"^/services/(\d+)/order$", path).group(1))
        svc = db.execute("SELECT * FROM services WHERE id = ? AND status = 'active'", [service_id]).fetchone()
        if not svc:
            return error_response("Service not found or unavailable", 404)
        if svc['worker_id'] == user['id']:
            return error_response("You cannot order your own service", 403)

        ensure_employer_profile(db, user['id'])
        if not employer_has_payment_setup(db, user['id']):
            return error_response("You must set up a payment method before ordering. Use /payments/setup-employer.", 402)

        body = get_body()
        pricing_type = svc['pricing_type']

        if pricing_type == 'fixed':
            total_amount = float(svc['price'] or 0)
        elif pricing_type == 'hourly':
            hours = float(body.get("hours", 1))
            total_amount = round(float(svc['hourly_rate'] or 0) * hours, 2)
        else:
            # custom pricing: employer provides amount
            total_amount = float(body.get("amount", 0))
            if total_amount <= 0:
                return error_response("amount required for custom pricing")

        if total_amount <= 0:
            return error_response("Service price must be positive")

        # Create order
        cursor = db.execute(
            """INSERT INTO orders (type, service_id, worker_id, employer_id, status, total_amount)
               VALUES ('service_order', ?, ?, ?, 'in_progress', ?)""",
            [service_id, svc['worker_id'], user['id'], total_amount]
        )
        order_id = cursor.lastrowid

        # Create single milestone for the full amount
        mc = db.execute(
            "INSERT INTO milestones (order_id, title, description, amount, sequence, status) VALUES (?,?,?,?,1,'pending')",
            [order_id, "Service delivery", body.get("notes", ""), total_amount]
        )
        milestone_id = mc.lastrowid

        # Fund escrow
        try:
            pi_id, mode = fund_escrow_stripe(
                db, user['id'], total_amount, order_id, milestone_id,
                f"Escrow for service order #{order_id}"
            )
        except ValueError as e:
            db.rollback()
            return error_response(str(e), 402)

        db.execute(
            "UPDATE milestones SET status='in_progress', escrow_payment_id=?, funded_at=datetime('now') WHERE id=?",
            [pi_id, milestone_id]
        )

        # Notify worker
        push_notification(db, svc['worker_id'], "new_order",
            f"New service order!",
            f"Someone ordered your service: {svc['title']}",
            f"/orders/{order_id}")

        audit(db, user['id'], "order_service", "order", order_id)
        db.commit()

        order = db.execute("SELECT * FROM orders WHERE id = ?", [order_id]).fetchone()
        result = row_to_dict(order)
        ms = db.execute("SELECT * FROM milestones WHERE order_id = ?", [order_id]).fetchall()
        result['milestones'] = [row_to_dict(m) for m in ms]
        return json_response(result, 201)

    # ═══════════════════════════════════════════════════════════════════════════
    # ORDERS
    # ═══════════════════════════════════════════════════════════════════════════

    elif path == "/orders" and method == "GET":
        user = authenticate(db)
        if not user:
            return error_response("Unauthorized", 401)

        role_filter = params.get("role")  # "worker" or "employer"
        status_filter = params.get("status")
        page = max(1, int(params.get("page", 1)))
        per_page = min(int(params.get("per_page", 20)), 100)
        offset = (page - 1) * per_page

        conditions = ["(o.worker_id = ? OR o.employer_id = ?)"]
        values = [user['id'], user['id']]

        if role_filter == 'worker':
            conditions = ["o.worker_id = ?"]
            values = [user['id']]
        elif role_filter == 'employer':
            conditions = ["o.employer_id = ?"]
            values = [user['id']]

        if status_filter:
            conditions.append("o.status = ?")
            values.append(status_filter)

        where = " AND ".join(conditions)
        count = db.execute(f"SELECT COUNT(*) as c FROM orders o WHERE {where}", values).fetchone()['c']
        rows = db.execute(
            f"""SELECT o.*,
                wu.name as worker_name, wu.avatar_url as worker_avatar,
                eu.name as employer_name, eu.avatar_url as employer_avatar,
                s.title as service_title,
                j.title as job_title
                FROM orders o
                JOIN users wu ON o.worker_id = wu.id
                JOIN users eu ON o.employer_id = eu.id
                LEFT JOIN services s ON o.service_id = s.id
                LEFT JOIN jobs j ON o.job_id = j.id
                WHERE {where}
                ORDER BY o.updated_at DESC
                LIMIT ? OFFSET ?""",
            values + [per_page, offset]
        ).fetchall()

        return json_response({
            "orders": [row_to_dict(r) for r in rows],
            "total": count,
            "page": page,
            "per_page": per_page,
            "total_pages": (count + per_page - 1) // per_page
        })

    elif re.match(r"^/orders/(\d+)$", path) and method == "GET":
        user = authenticate(db)
        if not user:
            return error_response("Unauthorized", 401)
        order_id = int(re.match(r"^/orders/(\d+)$", path).group(1))
        order = db.execute(
            """SELECT o.*,
               wu.name as worker_name, wu.avatar_url as worker_avatar,
               eu.name as employer_name, eu.avatar_url as employer_avatar,
               s.title as service_title,
               j.title as job_title
               FROM orders o
               JOIN users wu ON o.worker_id = wu.id
               JOIN users eu ON o.employer_id = eu.id
               LEFT JOIN services s ON o.service_id = s.id
               LEFT JOIN jobs j ON o.job_id = j.id
               WHERE o.id = ?""",
            [order_id]
        ).fetchone()
        if not order:
            return error_response("Order not found", 404)
        if order['worker_id'] != user['id'] and order['employer_id'] != user['id'] and not user['is_admin']:
            return error_response("Forbidden", 403)

        result = row_to_dict(order)
        ms = db.execute("SELECT * FROM milestones WHERE order_id = ? ORDER BY sequence", [order_id]).fetchall()
        result['milestones'] = [row_to_dict(m) for m in ms]
        hc = db.execute("SELECT * FROM hourly_contracts WHERE order_id = ?", [order_id]).fetchone()
        result['hourly_contract'] = row_to_dict(hc)
        if hc:
            entries = db.execute(
                "SELECT * FROM time_entries WHERE contract_id = ? ORDER BY date DESC LIMIT 50",
                [hc['id']]
            ).fetchall()
            result['time_entries'] = [row_to_dict(e) for e in entries]
        escrow = db.execute("SELECT * FROM escrow_holds WHERE order_id = ? ORDER BY created_at DESC", [order_id]).fetchall()
        result['escrow_holds'] = [row_to_dict(e) for e in escrow]
        reviews = db.execute("SELECT * FROM reviews WHERE order_id = ?", [order_id]).fetchall()
        result['reviews'] = [row_to_dict(r) for r in reviews]
        return json_response(result)

    elif re.match(r"^/orders/(\d+)/submit$", path) and method == "POST":
        user = authenticate(db)
        if not user:
            return error_response("Unauthorized", 401)
        order_id = int(re.match(r"^/orders/(\d+)/submit$", path).group(1))
        order = db.execute("SELECT * FROM orders WHERE id = ?", [order_id]).fetchone()
        if not order:
            return error_response("Order not found", 404)
        if order['worker_id'] != user['id']:
            return error_response("Only the worker can submit deliverables", 403)
        if order['status'] not in ('in_progress', 'revision_requested'):
            return error_response("Order must be in_progress or revision_requested to submit", 409)

        body = get_body()
        notes = body.get("notes", "")

        db.execute(
            "UPDATE orders SET status='submitted', worker_notes=?, updated_at=datetime('now') WHERE id=?",
            [notes, order_id]
        )

        # Update current milestone to submitted
        db.execute(
            """UPDATE milestones SET status='submitted' WHERE order_id=? AND status='in_progress'
               AND id = (SELECT id FROM milestones WHERE order_id=? AND status='in_progress' ORDER BY sequence LIMIT 1)""",
            [order_id, order_id]
        )

        push_notification(db, order['employer_id'], "order_submitted",
            "Deliverables submitted",
            f"Work has been submitted for review on order #{order_id}.",
            f"/orders/{order_id}")

        audit(db, user['id'], "submit_order", "order", order_id)
        db.commit()
        return json_response({"ok": True, "status": "submitted"})

    elif re.match(r"^/orders/(\d+)/approve$", path) and method == "POST":
        user = authenticate(db)
        if not user:
            return error_response("Unauthorized", 401)
        order_id = int(re.match(r"^/orders/(\d+)/approve$", path).group(1))
        order = db.execute("SELECT * FROM orders WHERE id = ?", [order_id]).fetchone()
        if not order:
            return error_response("Order not found", 404)
        if order['employer_id'] != user['id']:
            return error_response("Only the employer can approve", 403)
        if order['status'] != 'submitted':
            return error_response("Order must be in submitted state to approve", 409)

        body = get_body()

        # Find current submitted milestone
        current_ms = db.execute(
            "SELECT * FROM milestones WHERE order_id=? AND status='submitted' ORDER BY sequence LIMIT 1",
            [order_id]
        ).fetchone()
        if not current_ms:
            return error_response("No submitted milestone found", 409)

        ms_id = current_ms['id']
        ms_amount = float(current_ms['amount'])

        # Release escrow for this milestone
        worker_payout, fee = release_escrow_to_worker(db, order_id, ms_id, ms_amount, order['worker_id'])

        db.execute(
            "UPDATE milestones SET status='approved', released_at=datetime('now') WHERE id=?",
            [ms_id]
        )

        # Check if there are more milestones to fund
        next_ms = db.execute(
            "SELECT * FROM milestones WHERE order_id=? AND status='pending' ORDER BY sequence LIMIT 1",
            [order_id]
        ).fetchone()

        if next_ms:
            # Fund next milestone
            try:
                pi_id, mode = fund_escrow_stripe(
                    db, user['id'], float(next_ms['amount']), order_id, next_ms['id'],
                    f"Escrow for order #{order_id} milestone {next_ms['sequence']}"
                )
                db.execute(
                    "UPDATE milestones SET status='in_progress', escrow_payment_id=?, funded_at=datetime('now') WHERE id=?",
                    [pi_id, next_ms['id']]
                )
                db.execute("UPDATE orders SET status='in_progress', updated_at=datetime('now') WHERE id=?", [order_id])
                push_notification(db, order['worker_id'], "milestone_funded",
                    f"Next milestone funded",
                    f"Milestone {next_ms['sequence']} has been funded. Continue working!",
                    f"/orders/{order_id}")
            except ValueError as e:
                # Can't fund next milestone — mark order as disputed
                db.execute("UPDATE orders SET status='disputed', updated_at=datetime('now') WHERE id=?", [order_id])
                push_notification(db, order['worker_id'], "payment_issue",
                    "Payment issue on next milestone",
                    f"Could not fund next milestone: {str(e)}",
                    f"/orders/{order_id}")
        else:
            # All milestones done — complete order
            db.execute(
                "UPDATE orders SET status='completed', completed_at=datetime('now'), updated_at=datetime('now') WHERE id=?",
                [order_id]
            )
            # Update worker stats
            db.execute(
                "UPDATE worker_profiles SET total_orders_completed = total_orders_completed + 1 WHERE user_id=?",
                [order['worker_id']]
            )
            db.execute(
                "UPDATE employer_profiles SET total_orders = total_orders + 1 WHERE user_id=?",
                [order['employer_id']]
            )
            # Update service stats if applicable
            if order['service_id']:
                db.execute(
                    "UPDATE services SET total_reviews = total_reviews + 1 WHERE id=?",
                    [order['service_id']]
                )
            push_notification(db, order['worker_id'], "order_completed",
                "Order completed — payment released!",
                f"Order #{order_id} is complete. ${worker_payout:.2f} earned (after 1% fee).",
                f"/orders/{order_id}")
            push_notification(db, order['employer_id'], "order_completed",
                "Order completed",
                f"Order #{order_id} has been completed successfully.",
                f"/orders/{order_id}")

        audit(db, user['id'], "approve_order", "order", order_id)
        db.commit()
        return json_response({"ok": True, "worker_payout": worker_payout, "platform_fee": fee})

    elif re.match(r"^/orders/(\d+)/request-revision$", path) and method == "POST":
        user = authenticate(db)
        if not user:
            return error_response("Unauthorized", 401)
        order_id = int(re.match(r"^/orders/(\d+)/request-revision$", path).group(1))
        order = db.execute("SELECT * FROM orders WHERE id = ?", [order_id]).fetchone()
        if not order:
            return error_response("Order not found", 404)
        if order['employer_id'] != user['id']:
            return error_response("Only the employer can request revisions", 403)
        if order['status'] != 'submitted':
            return error_response("Order must be submitted to request revision", 409)

        body = get_body()
        notes = body.get("notes", "")

        db.execute(
            "UPDATE orders SET status='revision_requested', employer_notes=?, updated_at=datetime('now') WHERE id=?",
            [notes, order_id]
        )
        # Revert milestone to in_progress
        db.execute(
            """UPDATE milestones SET status='in_progress' WHERE order_id=? AND status='submitted'
               AND id=(SELECT id FROM milestones WHERE order_id=? AND status='submitted' ORDER BY sequence LIMIT 1)""",
            [order_id, order_id]
        )

        push_notification(db, order['worker_id'], "revision_requested",
            "Revision requested",
            f"The employer has requested a revision on order #{order_id}. Notes: {notes}",
            f"/orders/{order_id}")

        audit(db, user['id'], "request_revision", "order", order_id)
        db.commit()
        return json_response({"ok": True, "status": "revision_requested"})

    elif re.match(r"^/orders/(\d+)/dispute$", path) and method == "POST":
        user = authenticate(db)
        if not user:
            return error_response("Unauthorized", 401)
        order_id = int(re.match(r"^/orders/(\d+)/dispute$", path).group(1))
        order = db.execute("SELECT * FROM orders WHERE id = ?", [order_id]).fetchone()
        if not order:
            return error_response("Order not found", 404)
        if order['worker_id'] != user['id'] and order['employer_id'] != user['id'] and not user['is_admin']:
            return error_response("Only order participants can open a dispute", 403)
        if order['status'] in ('completed', 'canceled', 'disputed'):
            return error_response("Cannot dispute an order in this state", 409)

        body = get_body()
        db.execute(
            "UPDATE orders SET status='disputed', updated_at=datetime('now') WHERE id=?",
            [order_id]
        )

        # Notify both parties
        other_id = order['employer_id'] if user['id'] == order['worker_id'] else order['worker_id']
        push_notification(db, other_id, "order_disputed",
            f"Dispute opened on order #{order_id}",
            f"A dispute has been raised. Reason: {body.get('reason', '')}",
            f"/orders/{order_id}")
        push_notification(db, 1, "admin_dispute",  # Admin user_id=1 (or we'd fetch admin IDs)
            f"Dispute on order #{order_id}",
            f"Order #{order_id} has been disputed.",
            f"/admin/orders")

        audit(db, user['id'], "dispute_order", "order", order_id, {"reason": body.get("reason", "")})
        db.commit()
        return json_response({"ok": True, "status": "disputed"})

    elif re.match(r"^/orders/(\d+)/complete$", path) and method == "POST":
        user = authenticate(db)
        if not user:
            return error_response("Unauthorized", 401)
        order_id = int(re.match(r"^/orders/(\d+)/complete$", path).group(1))
        order = db.execute("SELECT * FROM orders WHERE id = ?", [order_id]).fetchone()
        if not order:
            return error_response("Order not found", 404)
        if order['employer_id'] != user['id'] and not user['is_admin']:
            return error_response("Only the employer or admin can complete an order", 403)
        if order['status'] not in ('submitted', 'in_progress'):
            return error_response("Order must be submitted or in_progress to complete", 409)

        db.execute(
            "UPDATE orders SET status='completed', completed_at=datetime('now'), updated_at=datetime('now') WHERE id=?",
            [order_id]
        )
        db.execute(
            "UPDATE worker_profiles SET total_orders_completed = total_orders_completed + 1 WHERE user_id=?",
            [order['worker_id']]
        )
        db.execute(
            "UPDATE employer_profiles SET total_orders = total_orders + 1 WHERE user_id=?",
            [order['employer_id']]
        )

        push_notification(db, order['worker_id'], "order_completed",
            "Order marked complete",
            f"Order #{order_id} has been marked complete.",
            f"/orders/{order_id}")

        audit(db, user['id'], "complete_order", "order", order_id)
        db.commit()
        return json_response({"ok": True, "status": "completed"})

    # ═══════════════════════════════════════════════════════════════════════════
    # HOURLY CONTRACT
    # ═══════════════════════════════════════════════════════════════════════════

    elif re.match(r"^/orders/(\d+)/log-hours$", path) and method == "POST":
        user = authenticate(db)
        if not user:
            return error_response("Unauthorized", 401)
        order_id = int(re.match(r"^/orders/(\d+)/log-hours$", path).group(1))
        order = db.execute("SELECT * FROM orders WHERE id = ?", [order_id]).fetchone()
        if not order:
            return error_response("Order not found", 404)
        if order['worker_id'] != user['id']:
            return error_response("Only the worker can log hours", 403)
        if order['status'] != 'in_progress':
            return error_response("Order must be in_progress to log hours", 409)

        hc = db.execute("SELECT * FROM hourly_contracts WHERE order_id = ?", [order_id]).fetchone()
        if not hc or hc['status'] != 'active':
            return error_response("No active hourly contract found for this order", 404)

        body = get_body()
        date_str = body.get("date", datetime.now(timezone.utc).strftime("%Y-%m-%d"))
        hours = float(body.get("hours", 0))
        description = body.get("description", "")

        if hours <= 0 or hours > 24:
            return error_response("hours must be between 0 and 24")

        # Determine week_of (Monday of the week)
        try:
            entry_date = datetime.strptime(date_str, "%Y-%m-%d")
            week_start = entry_date - timedelta(days=entry_date.weekday())
            week_of = week_start.strftime("%Y-%m-%d")
        except ValueError:
            return error_response("Invalid date format, use YYYY-MM-DD")

        # Check weekly cap
        week_total = db.execute(
            "SELECT COALESCE(SUM(hours),0) as total FROM time_entries WHERE contract_id=? AND week_of=? AND status!='disputed'",
            [hc['id'], week_of]
        ).fetchone()['total']

        if week_total + hours > hc['weekly_hour_cap']:
            return error_response(f"Adding {hours} hours would exceed weekly cap of {hc['weekly_hour_cap']} hours (already have {week_total})", 409)

        cursor = db.execute(
            "INSERT INTO time_entries (contract_id, date, hours, description, week_of) VALUES (?,?,?,?,?)",
            [hc['id'], date_str, hours, description, week_of]
        )
        entry_id = cursor.lastrowid

        push_notification(db, order['employer_id'], "hours_logged",
            f"Hours logged on order #{order_id}",
            f"Worker logged {hours}h on {date_str}: {description}",
            f"/orders/{order_id}")

        audit(db, user['id'], "log_hours", "time_entry", entry_id)
        db.commit()
        entry = db.execute("SELECT * FROM time_entries WHERE id = ?", [entry_id]).fetchone()
        return json_response(row_to_dict(entry), 201)

    elif re.match(r"^/orders/(\d+)/approve-hours$", path) and method == "POST":
        user = authenticate(db)
        if not user:
            return error_response("Unauthorized", 401)
        order_id = int(re.match(r"^/orders/(\d+)/approve-hours$", path).group(1))
        order = db.execute("SELECT * FROM orders WHERE id = ?", [order_id]).fetchone()
        if not order:
            return error_response("Order not found", 404)
        if order['employer_id'] != user['id']:
            return error_response("Only the employer can approve hours", 403)

        hc = db.execute("SELECT * FROM hourly_contracts WHERE order_id = ?", [order_id]).fetchone()
        if not hc:
            return error_response("No hourly contract for this order", 404)

        body = get_body()
        week_of = body.get("week_of")
        if not week_of:
            return error_response("week_of required (YYYY-MM-DD format, Monday of the week)")

        # Get pending entries for this week
        entries = db.execute(
            "SELECT * FROM time_entries WHERE contract_id=? AND week_of=? AND status='pending'",
            [hc['id'], week_of]
        ).fetchall()
        if not entries:
            return error_response("No pending time entries for this week", 404)

        total_hours = sum(float(e['hours']) for e in entries)
        total_pay = round(total_hours * float(hc['hourly_rate']), 2)
        fee = round(total_pay * SERVICE_FEE_RATE, 2)
        worker_pay = round(total_pay - fee, 2)

        # Mark entries approved
        db.execute(
            "UPDATE time_entries SET status='approved' WHERE contract_id=? AND week_of=? AND status='pending'",
            [hc['id'], week_of]
        )

        # Release escrow for these hours
        db.execute(
            "UPDATE escrow_holds SET status='released', released_at=datetime('now') WHERE order_id=? AND status='held'",
            [order_id]
        )
        db.execute(
            "INSERT INTO platform_revenue (order_id, fee_amount, fee_type) VALUES (?,?,'hourly_service_fee')",
            [order_id, fee]
        )

        # Transfer to worker if Stripe configured
        if stripe_configured():
            wp = db.execute("SELECT payout_account_id FROM worker_profiles WHERE user_id=?", [order['worker_id']]).fetchone()
            if wp and wp['payout_account_id'] and not wp['payout_account_id'].startswith('acct_sim_'):
                try:
                    stripe.Transfer.create(
                        amount=int(worker_pay * 100),
                        currency="usd",
                        destination=wp['payout_account_id'],
                        metadata={"order_id": str(order_id), "week_of": week_of}
                    )
                except stripe.error.StripeError:
                    pass

        # Refund unused escrow and fund next week
        escrow_held = float(hc['current_week_escrow_amount'] or 0)
        unused = max(0, round(escrow_held - total_pay, 2))

        # Fund next week's escrow
        if hc['status'] == 'active':
            next_week_escrow = round(float(hc['hourly_rate']) * float(hc['weekly_hour_cap']), 2)
            try:
                pi_id, mode = fund_escrow_stripe(
                    db, order['employer_id'], next_week_escrow, order_id, None,
                    f"Hourly contract next week escrow"
                )
                db.execute(
                    "UPDATE hourly_contracts SET current_week_escrow_amount=?, current_week_escrow_payment_id=? WHERE id=?",
                    [next_week_escrow, pi_id, hc['id']]
                )
            except ValueError:
                pass  # If can't fund next week, contract continues but without new escrow

        push_notification(db, order['worker_id'], "hours_approved",
            f"Hours approved — payment released!",
            f"{total_hours}h approved for week of {week_of}. ${worker_pay:.2f} released.",
            f"/orders/{order_id}")

        audit(db, user['id'], "approve_hours", "hourly_contract", hc['id'], {"week_of": week_of, "hours": total_hours})
        db.commit()
        return json_response({
            "ok": True,
            "hours_approved": total_hours,
            "worker_pay": worker_pay,
            "platform_fee": fee,
            "unused_escrow_refunded": unused
        })

    elif re.match(r"^/orders/(\d+)/end-contract$", path) and method == "POST":
        user = authenticate(db)
        if not user:
            return error_response("Unauthorized", 401)
        order_id = int(re.match(r"^/orders/(\d+)/end-contract$", path).group(1))
        order = db.execute("SELECT * FROM orders WHERE id = ?", [order_id]).fetchone()
        if not order:
            return error_response("Order not found", 404)
        if order['worker_id'] != user['id'] and order['employer_id'] != user['id']:
            return error_response("Only order participants can end the contract", 403)

        hc = db.execute("SELECT * FROM hourly_contracts WHERE order_id = ?", [order_id]).fetchone()
        if not hc:
            return error_response("No hourly contract for this order", 404)
        if hc['status'] == 'ended':
            return error_response("Contract already ended", 409)

        body = get_body()

        db.execute("UPDATE hourly_contracts SET status='ended' WHERE id=?", [hc['id']])
        db.execute(
            "UPDATE orders SET status='completed', completed_at=datetime('now'), updated_at=datetime('now') WHERE id=?",
            [order_id]
        )

        # Refund remaining escrow
        db.execute(
            "UPDATE escrow_holds SET status='refunded', released_at=datetime('now') WHERE order_id=? AND status='held'",
            [order_id]
        )

        other_id = order['employer_id'] if user['id'] == order['worker_id'] else order['worker_id']
        push_notification(db, other_id, "contract_ended",
            f"Hourly contract ended",
            f"The hourly contract on order #{order_id} has been ended.",
            f"/orders/{order_id}")

        audit(db, user['id'], "end_contract", "hourly_contract", hc['id'], {"reason": body.get("reason", "")})
        db.commit()
        return json_response({"ok": True, "status": "ended"})

    # ═══════════════════════════════════════════════════════════════════════════
    # REVIEWS
    # ═══════════════════════════════════════════════════════════════════════════

    elif re.match(r"^/orders/(\d+)/review$", path) and method == "POST":
        user = authenticate(db)
        if not user:
            return error_response("Unauthorized", 401)
        order_id = int(re.match(r"^/orders/(\d+)/review$", path).group(1))
        order = db.execute("SELECT * FROM orders WHERE id = ?", [order_id]).fetchone()
        if not order:
            return error_response("Order not found", 404)
        if order['status'] != 'completed':
            return error_response("Can only review completed orders", 409)
        if order['worker_id'] != user['id'] and order['employer_id'] != user['id']:
            return error_response("Only order participants can leave reviews", 403)

        # Determine who they're reviewing
        if user['id'] == order['employer_id']:
            to_user_id = order['worker_id']
        else:
            to_user_id = order['employer_id']

        body = get_body()
        rating = body.get("rating")
        if not rating or not isinstance(rating, int) or rating < 1 or rating > 5:
            return error_response("rating must be an integer 1-5")

        # Check no duplicate
        existing = db.execute(
            "SELECT id FROM reviews WHERE order_id=? AND from_user_id=?",
            [order_id, user['id']]
        ).fetchone()
        if existing:
            return error_response("You have already reviewed this order", 409)

        cursor = db.execute(
            "INSERT INTO reviews (order_id, from_user_id, to_user_id, rating, text, is_visible) VALUES (?,?,?,?,?,0)",
            [order_id, user['id'], to_user_id, rating, body.get("text", "")]
        )
        review_id = cursor.lastrowid

        # Check if both parties have reviewed — or if 14 days have passed
        review_count = db.execute(
            "SELECT COUNT(*) as c FROM reviews WHERE order_id=?",
            [order_id]
        ).fetchone()['c']

        make_visible = False
        if review_count >= 2:
            make_visible = True
        else:
            # Check if order was completed > 14 days ago
            if order['completed_at']:
                try:
                    completed_dt = datetime.fromisoformat(order['completed_at'].replace('Z', '+00:00'))
                    if completed_dt.tzinfo is None:
                        completed_dt = completed_dt.replace(tzinfo=timezone.utc)
                    if (datetime.now(timezone.utc) - completed_dt).days >= 14:
                        make_visible = True
                except (ValueError, AttributeError):
                    pass

        if make_visible:
            db.execute("UPDATE reviews SET is_visible=1 WHERE order_id=?", [order_id])

        # Update average rating for the recipient
        avg_row = db.execute(
            "SELECT AVG(rating) as avg, COUNT(*) as cnt FROM reviews WHERE to_user_id=? AND is_visible=1",
            [to_user_id]
        ).fetchone()
        # Update worker or employer profile
        if db.execute("SELECT user_id FROM worker_profiles WHERE user_id=?", [to_user_id]).fetchone():
            db.execute(
                "UPDATE worker_profiles SET avg_rating=?, total_reviews=? WHERE user_id=?",
                [avg_row['avg'] or 0, avg_row['cnt'] or 0, to_user_id]
            )
        if db.execute("SELECT user_id FROM employer_profiles WHERE user_id=?", [to_user_id]).fetchone():
            db.execute(
                "UPDATE employer_profiles SET avg_rating=?, total_reviews=? WHERE user_id=?",
                [avg_row['avg'] or 0, avg_row['cnt'] or 0, to_user_id]
            )

        audit(db, user['id'], "submit_review", "review", review_id)
        db.commit()
        review = db.execute("SELECT * FROM reviews WHERE id=?", [review_id]).fetchone()
        return json_response(row_to_dict(review), 201)

    elif re.match(r"^/users/(\d+)/reviews$", path) and method == "GET":
        target_id = int(re.match(r"^/users/(\d+)/reviews$", path).group(1))
        page = max(1, int(params.get("page", 1)))
        per_page = min(int(params.get("per_page", 20)), 100)
        offset = (page - 1) * per_page

        count = db.execute(
            "SELECT COUNT(*) as c FROM reviews WHERE to_user_id=? AND is_visible=1",
            [target_id]
        ).fetchone()['c']
        rows = db.execute(
            """SELECT r.*, u.name as reviewer_name, u.avatar_url as reviewer_avatar
               FROM reviews r
               JOIN users u ON r.from_user_id = u.id
               WHERE r.to_user_id=? AND r.is_visible=1
               ORDER BY r.created_at DESC
               LIMIT ? OFFSET ?""",
            [target_id, per_page, offset]
        ).fetchall()
        return json_response({
            "reviews": [row_to_dict(r) for r in rows],
            "total": count,
            "page": page,
            "per_page": per_page
        })

    # ═══════════════════════════════════════════════════════════════════════════
    # PAYMENTS
    # ═══════════════════════════════════════════════════════════════════════════

    elif path == "/payments/setup-employer" and method == "POST":
        user = authenticate(db)
        if not user:
            return error_response("Unauthorized", 401)

        ensure_employer_profile(db, user['id'])
        body = get_body()

        if stripe_configured():
            try:
                ep = db.execute("SELECT stripe_customer_id FROM employer_profiles WHERE user_id=?", [user['id']]).fetchone()
                if ep and ep['stripe_customer_id']:
                    customer_id = ep['stripe_customer_id']
                else:
                    customer = stripe.Customer.create(
                        email=user['email'],
                        name=user['name'],
                        metadata={"user_id": str(user['id'])}
                    )
                    customer_id = customer.id
                    db.execute(
                        "UPDATE employer_profiles SET stripe_customer_id=? WHERE user_id=?",
                        [customer_id, user['id']]
                    )
                    db.commit()

                # Create SetupIntent for saving payment method
                setup_intent = stripe.SetupIntent.create(
                    customer=customer_id,
                    payment_method_types=["card"],
                    metadata={"user_id": str(user['id'])}
                )
                db.commit()
                return json_response({
                    "client_secret": setup_intent.client_secret,
                    "customer_id": customer_id,
                    "publishable_key": STRIPE_PUBLISHABLE_KEY,
                    "mode": "live"
                })
            except stripe.error.StripeError as e:
                return error_response(f"Stripe error: {str(e)}", 502)
        else:
            # Simulation mode
            sim_customer_id = f"cus_sim_{secrets.token_hex(10)}"
            sim_payment_method = f"pm_sim_{secrets.token_hex(10)}"
            db.execute(
                "UPDATE employer_profiles SET stripe_customer_id=?, payment_method_id=? WHERE user_id=?",
                [sim_customer_id, sim_payment_method, user['id']]
            )
            audit(db, user['id'], "setup_employer_payment_sim", "employer_profile", user['id'])
            db.commit()
            return json_response({
                "customer_id": sim_customer_id,
                "payment_method_id": sim_payment_method,
                "mode": "simulated",
                "message": "Simulated payment method set up successfully"
            })

    elif path == "/payments/confirm-setup-employer" and method == "POST":
        """Called after frontend confirms SetupIntent — save the payment method."""
        user = authenticate(db)
        if not user:
            return error_response("Unauthorized", 401)

        body = get_body()
        payment_method_id = body.get("payment_method_id")
        if not payment_method_id:
            return error_response("payment_method_id required")

        if stripe_configured():
            try:
                ep = db.execute("SELECT stripe_customer_id FROM employer_profiles WHERE user_id=?", [user['id']]).fetchone()
                if ep and ep['stripe_customer_id']:
                    stripe.PaymentMethod.attach(payment_method_id, customer=ep['stripe_customer_id'])
                    stripe.Customer.modify(
                        ep['stripe_customer_id'],
                        invoice_settings={"default_payment_method": payment_method_id}
                    )
            except stripe.error.StripeError as e:
                return error_response(f"Stripe error: {str(e)}", 502)

        db.execute(
            "UPDATE employer_profiles SET payment_method_id=? WHERE user_id=?",
            [payment_method_id, user['id']]
        )
        audit(db, user['id'], "confirm_employer_payment", "employer_profile", user['id'])
        db.commit()
        return json_response({"ok": True, "payment_method_id": payment_method_id})

    elif path == "/payments/setup-worker" and method == "POST":
        user = authenticate(db)
        if not user:
            return error_response("Unauthorized", 401)

        ensure_worker_profile(db, user['id'])

        if stripe_configured():
            try:
                wp = db.execute("SELECT payout_account_id FROM worker_profiles WHERE user_id=?", [user['id']]).fetchone()
                if wp and wp['payout_account_id'] and wp['payout_account_id'].startswith('acct_') and not wp['payout_account_id'].startswith('acct_sim_'):
                    account_id = wp['payout_account_id']
                else:
                    account = stripe.Account.create(
                        type="express",
                        country="US",
                        email=user['email'],
                        capabilities={"transfers": {"requested": True}},
                        metadata={"user_id": str(user['id'])}
                    )
                    account_id = account.id
                    db.execute(
                        "UPDATE worker_profiles SET payout_account_id=?, payout_method='stripe_connect' WHERE user_id=?",
                        [account_id, user['id']]
                    )
                    db.commit()

                account_link = stripe.AccountLink.create(
                    account=account_id,
                    refresh_url=f"{FRONTEND_URL}/payments?connect=refresh",
                    return_url=f"{FRONTEND_URL}/payments?connect=complete",
                    type="account_onboarding"
                )
                audit(db, user['id'], "setup_worker_payout", "worker_profile", user['id'])
                db.commit()
                return json_response({
                    "ok": True,
                    "onboarding_url": account_link.url,
                    "account_id": account_id,
                    "mode": "live"
                })
            except stripe.error.StripeError as e:
                return error_response(f"Stripe error: {str(e)}", 502)
        else:
            # Simulation
            body = get_body()
            payout_account_id = f"acct_sim_{secrets.token_hex(10)}"
            db.execute(
                "UPDATE worker_profiles SET payout_account_id=?, payout_method='stripe_connect_active', payout_method_details=? WHERE user_id=?",
                [payout_account_id, json.dumps({"bank_name": body.get("bank_name", "Demo Bank"), "last4": body.get("last4", "0000")}), user['id']]
            )
            audit(db, user['id'], "setup_worker_payout_sim", "worker_profile", user['id'])
            db.commit()
            return json_response({
                "ok": True,
                "onboarding_url": f"{FRONTEND_URL}/payments?connect=complete&simulated=true",
                "account_id": payout_account_id,
                "mode": "simulated"
            })

    elif path == "/payments/status" and method == "GET":
        user = authenticate(db)
        if not user:
            return error_response("Unauthorized", 401)

        wp = db.execute("SELECT payout_account_id, payout_method FROM worker_profiles WHERE user_id=?", [user['id']]).fetchone()
        ep = db.execute("SELECT stripe_customer_id, payment_method_id FROM employer_profiles WHERE user_id=?", [user['id']]).fetchone()

        worker_status = None
        if wp:
            if wp['payout_account_id']:
                if stripe_configured() and not wp['payout_account_id'].startswith('acct_sim_'):
                    try:
                        acct = stripe.Account.retrieve(wp['payout_account_id'])
                        worker_status = {
                            "connected": acct.payouts_enabled,
                            "payouts_enabled": acct.payouts_enabled,
                            "details_submitted": acct.details_submitted,
                            "account_id": wp['payout_account_id'],
                            "mode": "live"
                        }
                    except stripe.error.StripeError:
                        worker_status = {"connected": False, "account_id": wp['payout_account_id'], "mode": "live"}
                else:
                    worker_status = {"connected": True, "account_id": wp['payout_account_id'], "mode": "simulated"}
            else:
                worker_status = {"connected": False, "account_id": None}

        employer_status = None
        if ep:
            employer_status = {
                "has_payment_method": bool(ep['payment_method_id']),
                "stripe_customer_id": ep['stripe_customer_id'],
                "payment_method_id": ep['payment_method_id']
            }

        return json_response({
            "worker_payout_status": worker_status,
            "employer_payment_status": employer_status
        })

    elif path == "/payments/fund-escrow" and method == "POST":
        """Manually fund escrow for a milestone (employer only)."""
        user = authenticate(db)
        if not user:
            return error_response("Unauthorized", 401)

        body = get_body()
        order_id = body.get("order_id")
        milestone_id = body.get("milestone_id")
        if not order_id:
            return error_response("order_id required")

        order = db.execute("SELECT * FROM orders WHERE id=?", [int(order_id)]).fetchone()
        if not order:
            return error_response("Order not found", 404)
        if order['employer_id'] != user['id']:
            return error_response("Forbidden", 403)

        amount = float(body.get("amount", 0))
        if amount <= 0:
            return error_response("amount must be positive")

        try:
            pi_id, mode = fund_escrow_stripe(db, user['id'], amount, order_id, milestone_id, "Manual escrow funding")
        except ValueError as e:
            return error_response(str(e), 402)

        if milestone_id:
            db.execute(
                "UPDATE milestones SET status='funded', escrow_payment_id=?, funded_at=datetime('now') WHERE id=?",
                [pi_id, milestone_id]
            )

        audit(db, user['id'], "fund_escrow", "escrow_hold", None, {"order_id": order_id, "amount": amount})
        db.commit()
        return json_response({"ok": True, "payment_intent_id": pi_id, "mode": mode, "amount": amount})

    elif path == "/payments/history" and method == "GET":
        user = authenticate(db)
        if not user:
            return error_response("Unauthorized", 401)

        page = max(1, int(params.get("page", 1)))
        per_page = min(int(params.get("per_page", 50)), 100)
        offset = (page - 1) * per_page

        # Escrow holds for this user (as worker or employer)
        holds = db.execute(
            """SELECT eh.*, o.type as order_type, o.total_amount as order_total
               FROM escrow_holds eh
               JOIN orders o ON eh.order_id = o.id
               WHERE o.worker_id=? OR o.employer_id=?
               ORDER BY eh.created_at DESC
               LIMIT ? OFFSET ?""",
            [user['id'], user['id'], per_page, offset]
        ).fetchall()

        # Platform revenue for this user's orders
        revenue = db.execute(
            """SELECT pr.* FROM platform_revenue pr
               JOIN orders o ON pr.order_id = o.id
               WHERE o.employer_id=?
               ORDER BY pr.created_at DESC
               LIMIT 50""",
            [user['id']]
        ).fetchall()

        return json_response({
            "escrow_history": [row_to_dict(h) for h in holds],
            "fees_paid": [row_to_dict(r) for r in revenue]
        })

    # ═══════════════════════════════════════════════════════════════════════════
    # STRIPE WEBHOOK
    # ═══════════════════════════════════════════════════════════════════════════

    elif path == "/webhooks/stripe" and method == "POST":
        body_raw = get_body_raw()
        sig_header = os.environ.get("HTTP_STRIPE_SIGNATURE", "")

        if not stripe_configured():
            return json_response({"received": True, "mode": "simulated"})

        if not STRIPE_WEBHOOK_SECRET:
            return error_response("Webhook secret not configured", 500)

        try:
            event = stripe.Webhook.construct_event(body_raw, sig_header, STRIPE_WEBHOOK_SECRET)
        except ValueError:
            return error_response("Invalid payload", 400)
        except stripe.error.SignatureVerificationError:
            return error_response("Invalid signature", 400)

        event_type = event['type']
        data = event['data']['object']

        if event_type == 'payment_intent.succeeded':
            pi_id = data['id']
            metadata = data.get('metadata', {})
            order_id = metadata.get('order_id')
            if order_id:
                db.execute(
                    "UPDATE escrow_holds SET status='held' WHERE stripe_payment_intent_id=? AND status='held'",
                    [pi_id]
                )
                db.commit()

        elif event_type == 'payment_intent.payment_failed':
            pi_id = data['id']
            db.execute(
                "UPDATE escrow_holds SET status='refunded' WHERE stripe_payment_intent_id=? AND status='held'",
                [pi_id]
            )
            # Notify employer
            metadata = data.get('metadata', {})
            employer_id = metadata.get('employer_id')
            if employer_id:
                push_notification(db, int(employer_id), "payment_failed",
                    "Payment failed",
                    "An escrow payment failed. Please update your payment method.",
                    "/payments")
            db.commit()

        elif event_type == 'account.updated':
            # Worker Connect account updated
            account_id = data['id']
            wp = db.execute("SELECT user_id FROM worker_profiles WHERE payout_account_id=?", [account_id]).fetchone()
            if wp:
                is_active = data.get('payouts_enabled', False) and data.get('charges_enabled', False)
                new_method = 'stripe_connect_active' if is_active else 'stripe_connect_pending'
                db.execute("UPDATE worker_profiles SET payout_method=? WHERE user_id=?", [new_method, wp['user_id']])
                if is_active:
                    push_notification(db, wp['user_id'], "payout_ready",
                        "Payout account ready!",
                        "Your bank account is connected and you can now receive payments.",
                        "/payments")
                db.commit()

        elif event_type == 'transfer.paid':
            transfer_id = data['id']
            metadata = data.get('metadata', {})
            order_id = metadata.get('order_id')
            if order_id:
                push_notification(db, 0, "transfer_paid",
                    "Transfer completed",
                    f"Payment transfer {transfer_id} completed.",
                    "")
                db.commit()

        return json_response({"received": True})

    # ═══════════════════════════════════════════════════════════════════════════
    # NOTIFICATIONS
    # ═══════════════════════════════════════════════════════════════════════════

    elif path == "/notifications" and method == "GET":
        user = authenticate(db)
        if not user:
            return error_response("Unauthorized", 401)

        unread_only = params.get("unread_only", "").lower() in ('true', '1')
        limit = min(int(params.get("limit", 50)), 100)

        q = "SELECT * FROM notifications WHERE user_id=?"
        qv = [user['id']]
        if unread_only:
            q += " AND is_read=0"
        q += " ORDER BY created_at DESC LIMIT ?"
        qv.append(limit)

        notifs = db.execute(q, qv).fetchall()
        unread_count = db.execute(
            "SELECT COUNT(*) as c FROM notifications WHERE user_id=? AND is_read=0", [user['id']]
        ).fetchone()['c']
        return json_response({
            "notifications": [row_to_dict(n) for n in notifs],
            "unread_count": unread_count
        })

    elif re.match(r"^/notifications/(\d+)/read$", path) and method == "PUT":
        user = authenticate(db)
        if not user:
            return error_response("Unauthorized", 401)
        notif_id = int(re.match(r"^/notifications/(\d+)/read$", path).group(1))
        db.execute(
            "UPDATE notifications SET is_read=1 WHERE id=? AND user_id=?",
            [notif_id, user['id']]
        )
        db.commit()
        return json_response({"ok": True})

    elif path == "/notifications/read-all" and method == "POST":
        user = authenticate(db)
        if not user:
            return error_response("Unauthorized", 401)
        db.execute("UPDATE notifications SET is_read=1 WHERE user_id=?", [user['id']])
        db.commit()
        return json_response({"ok": True})

    # ═══════════════════════════════════════════════════════════════════════════
    # ADMIN ROUTES
    # ═══════════════════════════════════════════════════════════════════════════

    elif path == "/admin/dashboard" and method == "GET":
        user = authenticate(db)
        if not user or not user['is_admin']:
            return error_response("Admin access required", 403)

        stats = {
            "total_users": db.execute("SELECT COUNT(*) as c FROM users").fetchone()['c'],
            "users_with_worker_profile": db.execute("SELECT COUNT(*) as c FROM worker_profiles").fetchone()['c'],
            "users_with_employer_profile": db.execute("SELECT COUNT(*) as c FROM employer_profiles").fetchone()['c'],
            "total_services": db.execute("SELECT COUNT(*) as c FROM services WHERE status='active'").fetchone()['c'],
            "total_jobs": db.execute("SELECT COUNT(*) as c FROM jobs").fetchone()['c'],
            "open_jobs": db.execute("SELECT COUNT(*) as c FROM jobs WHERE status='open'").fetchone()['c'],
            "total_orders": db.execute("SELECT COUNT(*) as c FROM orders").fetchone()['c'],
            "active_orders": db.execute("SELECT COUNT(*) as c FROM orders WHERE status='in_progress'").fetchone()['c'],
            "completed_orders": db.execute("SELECT COUNT(*) as c FROM orders WHERE status='completed'").fetchone()['c'],
            "disputed_orders": db.execute("SELECT COUNT(*) as c FROM orders WHERE status='disputed'").fetchone()['c'],
            "total_applications": db.execute("SELECT COUNT(*) as c FROM applications").fetchone()['c'],
            "total_revenue": db.execute("SELECT COALESCE(SUM(fee_amount),0) as s FROM platform_revenue").fetchone()['s'],
            "revenue_30d": db.execute("SELECT COALESCE(SUM(fee_amount),0) as s FROM platform_revenue WHERE date(created_at) >= date('now', '-30 days')").fetchone()['s'],
            "gross_volume": db.execute("SELECT COALESCE(SUM(total_amount),0) as s FROM orders WHERE status='completed'").fetchone()['s'],
            "stripe_mode": "live" if stripe_configured() else "simulated"
        }

        orders_by_status = {}
        for s in ['pending', 'in_progress', 'submitted', 'revision_requested', 'completed', 'canceled', 'disputed']:
            orders_by_status[s] = db.execute(
                "SELECT COUNT(*) as c FROM orders WHERE status=?", [s]
            ).fetchone()['c']
        stats['orders_by_status'] = orders_by_status

        return json_response(stats)

    elif path == "/admin/users" and method == "GET":
        user = authenticate(db)
        if not user or not user['is_admin']:
            return error_response("Admin access required", 403)

        page = max(1, int(params.get("page", 1)))
        per_page = min(int(params.get("per_page", 50)), 200)
        offset = (page - 1) * per_page
        search = params.get("search", "").strip()

        conditions = []
        values = []
        if search:
            conditions.append("(u.email LIKE ? OR u.name LIKE ?)")
            pct = f"%{search}%"
            values.extend([pct, pct])

        where = " AND ".join(conditions) if conditions else "1=1"
        count = db.execute(f"SELECT COUNT(*) as c FROM users u WHERE {where}", values).fetchone()['c']
        rows = db.execute(
            f"""SELECT u.*,
                CASE WHEN wp.user_id IS NOT NULL THEN 1 ELSE 0 END as has_worker_profile,
                CASE WHEN ep.user_id IS NOT NULL THEN 1 ELSE 0 END as has_employer_profile,
                wp.avg_rating as worker_rating, wp.total_orders_completed,
                ep.avg_rating as employer_rating
                FROM users u
                LEFT JOIN worker_profiles wp ON u.id = wp.user_id
                LEFT JOIN employer_profiles ep ON u.id = ep.user_id
                WHERE {where}
                ORDER BY u.created_at DESC
                LIMIT ? OFFSET ?""",
            values + [per_page, offset]
        ).fetchall()

        result = []
        for r in rows:
            rd = row_to_dict(r)
            del rd['password_hash']
            result.append(rd)

        return json_response({
            "users": result,
            "total": count,
            "page": page,
            "per_page": per_page
        })

    elif re.match(r"^/admin/users/(\d+)$", path) and method == "PUT":
        user = authenticate(db)
        if not user or not user['is_admin']:
            return error_response("Admin access required", 403)

        target_id = int(re.match(r"^/admin/users/(\d+)$", path).group(1))
        body = get_body()

        updates = []
        vals = []
        for field in ['is_active', 'is_suspended', 'is_banned', 'is_admin']:
            if field in body:
                updates.append(f"{field} = ?")
                vals.append(1 if body[field] else 0)
        if updates:
            vals.append(target_id)
            db.execute(f"UPDATE users SET {', '.join(updates)} WHERE id=?", vals)

        audit(db, user['id'], "admin_update_user", "user", target_id, body)
        db.commit()
        return json_response({"ok": True})

    elif path == "/admin/orders" and method == "GET":
        user = authenticate(db)
        if not user or not user['is_admin']:
            return error_response("Admin access required", 403)

        page = max(1, int(params.get("page", 1)))
        per_page = min(int(params.get("per_page", 50)), 200)
        offset = (page - 1) * per_page
        status_filter = params.get("status")

        conditions = []
        values = []
        if status_filter:
            conditions.append("o.status=?")
            values.append(status_filter)

        where = " AND ".join(conditions) if conditions else "1=1"
        count = db.execute(f"SELECT COUNT(*) as c FROM orders o WHERE {where}", values).fetchone()['c']
        rows = db.execute(
            f"""SELECT o.*,
               wu.name as worker_name, wu.email as worker_email,
               eu.name as employer_name, eu.email as employer_email,
               s.title as service_title, j.title as job_title
               FROM orders o
               JOIN users wu ON o.worker_id = wu.id
               JOIN users eu ON o.employer_id = eu.id
               LEFT JOIN services s ON o.service_id = s.id
               LEFT JOIN jobs j ON o.job_id = j.id
               WHERE {where}
               ORDER BY o.updated_at DESC
               LIMIT ? OFFSET ?""",
            values + [per_page, offset]
        ).fetchall()

        return json_response({
            "orders": [row_to_dict(r) for r in rows],
            "total": count,
            "page": page,
            "per_page": per_page
        })

    elif path == "/admin/revenue" and method == "GET":
        user = authenticate(db)
        if not user or not user['is_admin']:
            return error_response("Admin access required", 403)

        total = db.execute("SELECT COALESCE(SUM(fee_amount),0) as s FROM platform_revenue").fetchone()['s']
        last_30d = db.execute("SELECT COALESCE(SUM(fee_amount),0) as s FROM platform_revenue WHERE date(created_at) >= date('now', '-30 days')").fetchone()['s']
        last_7d = db.execute("SELECT COALESCE(SUM(fee_amount),0) as s FROM platform_revenue WHERE date(created_at) >= date('now', '-7 days')").fetchone()['s']

        daily = db.execute("""
            SELECT date(created_at) as day, SUM(fee_amount) as fees, COUNT(*) as transactions
            FROM platform_revenue
            WHERE date(created_at) >= date('now', '-30 days')
            GROUP BY date(created_at)
            ORDER BY day ASC
        """).fetchall()

        by_type = db.execute("""
            SELECT fee_type, SUM(fee_amount) as total, COUNT(*) as count
            FROM platform_revenue
            GROUP BY fee_type
        """).fetchall()

        return json_response({
            "total_fees": round(total, 2),
            "fees_30d": round(last_30d, 2),
            "fees_7d": round(last_7d, 2),
            "daily_breakdown": [row_to_dict(r) for r in daily],
            "by_fee_type": [row_to_dict(r) for r in by_type],
            "stripe_mode": "live" if stripe_configured() else "simulated"
        })

    elif path == "/admin/resolve-dispute" and method == "POST":
        user = authenticate(db)
        if not user or not user['is_admin']:
            return error_response("Admin access required", 403)

        body = get_body()
        order_id = body.get("order_id")
        resolution = body.get("resolution")  # "release_to_worker", "refund_to_employer", "split"
        if not order_id or not resolution:
            return error_response("order_id and resolution required")
        if resolution not in ('release_to_worker', 'refund_to_employer', 'split'):
            return error_response("resolution must be release_to_worker, refund_to_employer, or split")

        order = db.execute("SELECT * FROM orders WHERE id=?", [int(order_id)]).fetchone()
        if not order:
            return error_response("Order not found", 404)
        if order['status'] != 'disputed':
            return error_response("Order must be disputed to resolve", 409)

        admin_notes = body.get("notes", "")

        if resolution == 'release_to_worker':
            # Release all held escrow to worker
            holds = db.execute(
                "SELECT * FROM escrow_holds WHERE order_id=? AND status='held'",
                [int(order_id)]
            ).fetchall()
            total_released = 0
            for hold in holds:
                db.execute(
                    "UPDATE escrow_holds SET status='released', released_at=datetime('now') WHERE id=?",
                    [hold['id']]
                )
                total_released += float(hold['amount'])

            fee = round(total_released * SERVICE_FEE_RATE, 2)
            worker_pay = round(total_released - fee, 2)
            db.execute(
                "INSERT INTO platform_revenue (order_id, fee_amount, fee_type) VALUES (?,?,'dispute_resolution')",
                [order_id, fee]
            )

            push_notification(db, order['worker_id'], "dispute_resolved",
                "Dispute resolved in your favor",
                f"${worker_pay:.2f} has been released to you.",
                f"/orders/{order_id}")
            push_notification(db, order['employer_id'], "dispute_resolved",
                "Dispute resolved",
                f"The dispute for order #{order_id} was resolved in the worker's favor.",
                f"/orders/{order_id}")

        elif resolution == 'refund_to_employer':
            db.execute(
                "UPDATE escrow_holds SET status='refunded', released_at=datetime('now') WHERE order_id=? AND status='held'",
                [int(order_id)]
            )
            push_notification(db, order['employer_id'], "dispute_resolved",
                "Dispute resolved — refund issued",
                f"Your payment for order #{order_id} has been refunded.",
                f"/orders/{order_id}")
            push_notification(db, order['worker_id'], "dispute_resolved",
                "Dispute resolved",
                f"The dispute for order #{order_id} was resolved in the employer's favor.",
                f"/orders/{order_id}")

        elif resolution == 'split':
            split_pct = float(body.get("worker_percent", 50)) / 100
            holds = db.execute(
                "SELECT * FROM escrow_holds WHERE order_id=? AND status='held'",
                [int(order_id)]
            ).fetchall()
            for hold in holds:
                amount = float(hold['amount'])
                worker_portion = round(amount * split_pct, 2)
                employer_portion = round(amount - worker_portion, 2)
                db.execute(
                    "UPDATE escrow_holds SET status='partial', released_at=datetime('now') WHERE id=?",
                    [hold['id']]
                )
                db.execute(
                    "INSERT INTO platform_revenue (order_id, fee_amount, fee_type) VALUES (?,?,'dispute_split')",
                    [order_id, round(worker_portion * SERVICE_FEE_RATE, 2)]
                )
            push_notification(db, order['worker_id'], "dispute_resolved",
                "Dispute resolved — split decision",
                f"The dispute for order #{order_id} was resolved with a split decision.",
                f"/orders/{order_id}")
            push_notification(db, order['employer_id'], "dispute_resolved",
                "Dispute resolved — split decision",
                f"The dispute for order #{order_id} was resolved with a split decision.",
                f"/orders/{order_id}")

        db.execute(
            "UPDATE orders SET status='completed', completed_at=datetime('now'), updated_at=datetime('now') WHERE id=?",
            [int(order_id)]
        )
        audit(db, user['id'], "resolve_dispute", "order", int(order_id), {"resolution": resolution, "notes": admin_notes})
        db.commit()
        return json_response({"ok": True, "resolution": resolution})

    elif path == "/admin/audit-log" and method == "GET":
        user = authenticate(db)
        if not user or not user['is_admin']:
            return error_response("Admin access required", 403)

        limit = min(int(params.get("limit", 100)), 500)
        logs = db.execute(
            """SELECT al.*, u.name, u.email FROM audit_log al
               LEFT JOIN users u ON al.user_id = u.id
               ORDER BY al.created_at DESC LIMIT ?""",
            [limit]
        ).fetchall()
        return json_response([row_to_dict(l) for l in logs])

    # ═══════════════════════════════════════════════════════════════════════════
    # SEED ENDPOINT
    # ═══════════════════════════════════════════════════════════════════════════

    elif path == "/seed" and method == "POST":
        if not SEED_SECRET:
            return error_response("Seed endpoint disabled", 404)

        seed_body = get_body()
        provided_secret = (seed_body or {}).get("secret", "")
        if not hmac.compare_digest(SEED_SECRET, provided_secret):
            return error_response("Forbidden", 403)

        # Check if already seeded
        existing = db.execute("SELECT COUNT(*) as c FROM users").fetchone()['c']
        if existing > 0:
            return json_response({"message": "Already seeded", "users": existing})

        # ── Create Admin ──────────────────────────────────────────────────────
        admin_cursor = db.execute(
            "INSERT INTO users (email, password_hash, name, is_admin) VALUES (?,?,?,1)",
            ["admin@gohirehumans.com", hash_password("Admin1234!"), "GoHireHumans Admin"]
        )
        admin_id = admin_cursor.lastrowid

        # ── Create Workers ────────────────────────────────────────────────────
        workers_data = [
            {
                "email": "sarah.chen@example.com", "name": "Sarah Chen",
                "skills": ["graphic_design", "ui_ux_design", "content_creation"],
                "bio": "Freelance designer with 5 years experience in brand identity and digital design. Specializes in clean, modern aesthetics.",
                "hourly_rate": 65.0, "avg_rating": 4.9, "total_reviews": 34,
                "payout_account_id": f"acct_sim_{secrets.token_hex(8)}"
            },
            {
                "email": "marcus.johnson@example.com", "name": "Marcus Johnson",
                "skills": ["web_development", "mobile_development", "software_development"],
                "bio": "Full-stack developer (React, Node.js, Python). 7 years building web apps and APIs. Fast turnaround, clean code.",
                "hourly_rate": 90.0, "avg_rating": 4.8, "total_reviews": 52,
                "payout_account_id": f"acct_sim_{secrets.token_hex(8)}"
            },
            {
                "email": "elena.rodriguez@example.com", "name": "Elena Rodriguez",
                "skills": ["writing", "copywriting", "translation", "seo"],
                "bio": "Bilingual (English/Spanish) content writer and SEO specialist. Former marketing manager turned freelancer.",
                "hourly_rate": 55.0, "avg_rating": 4.7, "total_reviews": 28,
                "payout_account_id": f"acct_sim_{secrets.token_hex(8)}"
            },
            {
                "email": "james.park@example.com", "name": "James Park",
                "skills": ["accounting", "bookkeeping", "data_analysis"],
                "bio": "CPA with 10 years in corporate finance. Available for bookkeeping, financial modeling, and tax prep.",
                "hourly_rate": 85.0, "avg_rating": 5.0, "total_reviews": 17,
                "payout_account_id": f"acct_sim_{secrets.token_hex(8)}"
            },
            {
                "email": "aisha.patel@example.com", "name": "Aisha Patel",
                "skills": ["digital_marketing", "social_media", "content_creation"],
                "bio": "Digital marketing specialist with expertise in paid social, email campaigns, and brand strategy.",
                "hourly_rate": 70.0, "avg_rating": 4.6, "total_reviews": 21,
                "payout_account_id": f"acct_sim_{secrets.token_hex(8)}"
            },
        ]

        worker_ids = []
        for w in workers_data:
            cursor = db.execute(
                "INSERT INTO users (email, password_hash, name) VALUES (?,?,?)",
                [w['email'], hash_password("Worker1234!"), w['name']]
            )
            uid = cursor.lastrowid
            worker_ids.append(uid)
            db.execute(
                """INSERT INTO worker_profiles
                   (user_id, bio, skills, hourly_rate, payout_account_id, payout_method,
                    avg_rating, total_reviews, is_verified)
                   VALUES (?,?,?,?,?,'stripe_connect_active',?,?,1)""",
                [uid, w['bio'], json.dumps(w['skills']), w['hourly_rate'],
                 w['payout_account_id'], w['avg_rating'], w['total_reviews']]
            )

        # ── Create Employers ──────────────────────────────────────────────────
        employers_data = [
            {
                "email": "hire@techstartup.io", "name": "Alex Rivera",
                "company_name": "TechStartup.io", "description": "Early-stage SaaS startup building a B2B analytics platform.",
                "payment_method_id": f"pm_sim_{secrets.token_hex(8)}",
                "stripe_customer_id": f"cus_sim_{secrets.token_hex(8)}"
            },
            {
                "email": "ops@growthagency.com", "name": "Jordan Lee",
                "company_name": "Growth Agency Co.", "description": "Full-service growth marketing agency serving e-commerce brands.",
                "payment_method_id": f"pm_sim_{secrets.token_hex(8)}",
                "stripe_customer_id": f"cus_sim_{secrets.token_hex(8)}"
            },
            {
                "email": "founder@bootstrapped.co", "name": "Taylor Kim",
                "company_name": "Bootstrapped.co", "description": "Solo founder building multiple SaaS products.",
                "payment_method_id": f"pm_sim_{secrets.token_hex(8)}",
                "stripe_customer_id": f"cus_sim_{secrets.token_hex(8)}"
            },
        ]

        employer_ids = []
        for e in employers_data:
            cursor = db.execute(
                "INSERT INTO users (email, password_hash, name) VALUES (?,?,?)",
                [e['email'], hash_password("Employer1234!"), e['name']]
            )
            uid = cursor.lastrowid
            employer_ids.append(uid)
            db.execute(
                """INSERT INTO employer_profiles
                   (user_id, company_name, description, payment_method_id, stripe_customer_id)
                   VALUES (?,?,?,?,?)""",
                [uid, e['company_name'], e['description'], e['payment_method_id'], e['stripe_customer_id']]
            )

        # ── Create Service Listings ───────────────────────────────────────────
        services_data = [
            {
                "worker_idx": 0, "category": "graphic_design", "pricing_type": "fixed",
                "title": "I will design a professional logo with brand guidelines",
                "description": "Get a unique, modern logo for your business with a full brand guidelines document. Includes 3 concepts, unlimited revisions until you're happy, all source files (AI, SVG, PNG).",
                "price": 299.0, "delivery_time_days": 5,
                "includes": "3 logo concepts, brand guidelines PDF, all source files, commercial license",
                "tags": ["logo", "branding", "graphic design", "identity"]
            },
            {
                "worker_idx": 1, "category": "web_development", "pricing_type": "hourly",
                "title": "Full-stack web development (React + Node.js)",
                "description": "Expert full-stack development using React, TypeScript, Node.js, and PostgreSQL. Available for new projects, feature development, bug fixes, and code reviews.",
                "hourly_rate": 90.0, "delivery_time_days": None,
                "includes": "Clean, documented code, unit tests, code review, deployment support",
                "tags": ["react", "nodejs", "typescript", "fullstack"]
            },
            {
                "worker_idx": 2, "category": "writing", "pricing_type": "fixed",
                "title": "SEO blog post (1500-2000 words) with keyword research",
                "description": "Well-researched, engaging blog post optimized for your target keywords. Includes keyword research, outline, writing, basic on-page SEO recommendations, and 1 revision.",
                "price": 150.0, "delivery_time_days": 3,
                "includes": "Keyword research report, 1500-2000 word post, meta description, 1 revision",
                "tags": ["seo", "blog", "content writing", "copywriting"]
            },
            {
                "worker_idx": 3, "category": "accounting", "pricing_type": "fixed",
                "title": "Monthly bookkeeping for small business (up to 200 transactions)",
                "description": "Complete monthly bookkeeping service: categorize transactions, reconcile accounts, generate P&L and balance sheet. Works with QuickBooks, Xero, or Wave.",
                "price": 350.0, "delivery_time_days": 7,
                "includes": "Transaction categorization, bank reconciliation, monthly P&L, balance sheet",
                "tags": ["bookkeeping", "accounting", "quickbooks", "small business"]
            },
            {
                "worker_idx": 4, "category": "digital_marketing", "pricing_type": "fixed",
                "title": "Complete Facebook & Instagram ad campaign setup",
                "description": "Full paid social campaign setup including audience research, creative brief, ad copy, A/B test variants, pixel setup, and campaign launch. Targeting B2B or B2C.",
                "price": 499.0, "delivery_time_days": 7,
                "includes": "Audience research, 3 ad variations, pixel setup, campaign launch, 2-week monitoring",
                "tags": ["facebook ads", "instagram", "paid social", "digital marketing"]
            },
            {
                "worker_idx": 0, "category": "ui_ux_design", "pricing_type": "fixed",
                "title": "UI/UX design for mobile app (up to 10 screens)",
                "description": "Professional mobile app design for iOS or Android. Includes user flow diagram, wireframes, and high-fidelity Figma designs for up to 10 screens.",
                "price": 650.0, "delivery_time_days": 10,
                "includes": "User flow, wireframes, 10 Figma screens, component library, handoff file",
                "tags": ["figma", "mobile design", "ui design", "ux design"]
            },
            {
                "worker_idx": 2, "category": "translation", "pricing_type": "custom",
                "title": "English to Spanish translation (marketing & technical content)",
                "description": "Native-quality English-Spanish translation for marketing copy, technical documentation, websites, and legal documents. Proofreading included. Pricing per word.",
                "delivery_time_days": 3,
                "includes": "Native Spanish translation, proofreading, glossary for technical terms",
                "tags": ["spanish", "translation", "marketing translation", "localization"]
            },
            {
                "worker_idx": 1, "category": "mobile_development", "pricing_type": "fixed",
                "title": "React Native app MVP (4-6 screens)",
                "description": "Build your mobile app MVP using React Native for cross-platform iOS and Android deployment. Includes navigation, API integration, and app store submission guidance.",
                "price": 2500.0, "delivery_time_days": 21,
                "includes": "React Native codebase, 4-6 screens, API integration, testing, source code",
                "tags": ["react native", "mobile app", "ios", "android", "mvp"]
            },
        ]

        service_ids = []
        for s in services_data:
            cursor = db.execute(
                """INSERT INTO services
                   (worker_id, title, description, category, pricing_type, price, hourly_rate,
                    delivery_time_days, includes, tags, images, status, avg_rating, total_reviews)
                   VALUES (?,?,?,?,?,?,?,?,?,?,'[]','active',?,?)""",
                [worker_ids[s['worker_idx']], s['title'], s['description'], s['category'],
                 s['pricing_type'], s.get('price'), s.get('hourly_rate'),
                 s.get('delivery_time_days'), s.get('includes', ''),
                 json.dumps(s['tags']),
                 round(4.5 + secrets.randbelow(5) * 0.1, 1),
                 secrets.randbelow(20) + 5]
            )
            service_ids.append(cursor.lastrowid)

        # ── Create Job Listings ───────────────────────────────────────────────
        jobs_data = [
            {
                "employer_idx": 0, "category": "web_development",
                "title": "React frontend developer needed for SaaS dashboard (3-month contract)",
                "description": "We're building a B2B analytics dashboard and need an experienced React developer to implement the frontend. Tech stack: React 18, TypeScript, Tailwind CSS, Recharts. Must have 3+ years React experience and portfolio of SaaS/dashboard projects.",
                "location_type": "remote", "budget_type": "hourly", "budget_amount": 85.0,
                "estimated_hours": 480, "required_skills": ["web_development", "software_development"],
                "status": "open"
            },
            {
                "employer_idx": 1, "category": "content_creation",
                "title": "Content writer for e-commerce blog — 8 articles/month",
                "description": "Seeking a content writer to produce 8 SEO-optimized blog articles per month for our e-commerce clients. Topics: fashion, home decor, fitness. Each article: 1200-1500 words, keyword research provided. Must have e-commerce/product writing experience.",
                "location_type": "remote", "budget_type": "fixed", "budget_amount": 1200.0,
                "required_skills": ["writing", "copywriting", "seo"],
                "status": "open"
            },
            {
                "employer_idx": 2, "category": "graphic_design",
                "title": "Brand designer for new SaaS product",
                "description": "Looking for a brand designer to create the visual identity for our new developer tool. Deliverables: logo, color palette, typography, basic brand guidelines. Target audience: developers and technical founders. Modern, minimal aesthetic preferred.",
                "location_type": "remote", "budget_type": "fixed", "budget_amount": 800.0,
                "required_skills": ["graphic_design", "ui_ux_design"],
                "status": "open"
            },
            {
                "employer_idx": 0, "category": "digital_marketing",
                "title": "Growth marketer to set up and run paid acquisition",
                "description": "Early-stage SaaS startup seeking a growth marketer to set up and manage our paid acquisition channels (Google Ads, LinkedIn Ads). Monthly budget: $5K. KPI: reduce CAC below $200. Must have B2B SaaS experience.",
                "location_type": "remote", "budget_type": "hourly", "budget_amount": 75.0,
                "estimated_hours": 40, "required_skills": ["digital_marketing", "seo"],
                "status": "reviewing"
            },
            {
                "employer_idx": 1, "category": "data_analysis",
                "title": "Data analyst to build performance dashboard in Looker Studio",
                "description": "We need a data analyst to connect our Google Ads, GA4, and Shopify data to Looker Studio and build a client-facing performance dashboard. Must have experience with Looker Studio, Google Ads API, and e-commerce metrics.",
                "location_type": "remote", "budget_type": "fixed", "budget_amount": 1500.0,
                "required_skills": ["data_analysis", "data_entry"],
                "status": "open"
            },
            {
                "employer_idx": 2, "category": "mobile_development",
                "title": "iOS developer for fintech app feature (Plaid integration)",
                "description": "Looking for an iOS developer to implement Plaid bank connection flow in our existing Swift/SwiftUI fintech app. Must have experience with iOS development, Plaid SDK, and financial data APIs. 2-3 week project.",
                "location_type": "remote", "budget_type": "fixed", "budget_amount": 3500.0,
                "required_skills": ["mobile_development", "software_development"],
                "status": "open"
            },
        ]

        job_ids = []
        for j in jobs_data:
            cursor = db.execute(
                """INSERT INTO jobs
                   (employer_id, title, description, category, location_type, budget_type,
                    budget_amount, estimated_hours, required_skills, status)
                   VALUES (?,?,?,?,?,?,?,?,?,?)""",
                [employer_ids[j['employer_idx']], j['title'], j['description'],
                 j['category'], j['location_type'], j['budget_type'], j['budget_amount'],
                 j.get('estimated_hours'), json.dumps(j['required_skills']), j['status']]
            )
            job_ids.append(cursor.lastrowid)

        # ── Create Sample Applications ────────────────────────────────────────
        # Apply worker 1 (Marcus, dev) to job 0 (React dev)
        db.execute(
            """INSERT INTO applications (job_id, worker_id, cover_message, portfolio_url, status)
               VALUES (?,?,?,?,'pending')""",
            [job_ids[0], worker_ids[1],
             "I'm a full-stack developer with 7 years of React experience. I've built several SaaS dashboards including a real-time analytics platform. Happy to share portfolio.",
             "https://github.com/marcusjohnson"]
        )
        # Apply worker 4 (Aisha, marketing) to job 1 (content writer)
        db.execute(
            """INSERT INTO applications (job_id, worker_id, cover_message, portfolio_url, status)
               VALUES (?,?,?,?,'pending')""",
            [job_ids[1], worker_ids[4],
             "I've been writing e-commerce content for 3 years. I specialize in fashion, lifestyle, and beauty. My articles consistently rank on page 1 for target keywords.",
             "https://portfolio.aishapatel.com"]
        )
        # Apply worker 2 (Elena) to job 1 as well
        db.execute(
            """INSERT INTO applications (job_id, worker_id, cover_message, portfolio_url, status)
               VALUES (?,?,?,?,'pending')""",
            [job_ids[1], worker_ids[2],
             "Experienced content writer with strong SEO knowledge. I've written hundreds of articles for e-commerce brands and always deliver on time.",
             "https://elenawritescopy.com"]
        )

        # ── Create a Completed Order ──────────────────────────────────────────
        order_cursor = db.execute(
            """INSERT INTO orders (type, service_id, worker_id, employer_id, status, total_amount,
               completed_at, created_at, updated_at)
               VALUES ('service_order',?,?,?,'completed',299.0,datetime('now','-5 days'),
                       datetime('now','-12 days'),datetime('now','-5 days'))""",
            [service_ids[0], worker_ids[0], employer_ids[0]]
        )
        completed_order_id = order_cursor.lastrowid

        db.execute(
            """INSERT INTO milestones (order_id, title, amount, sequence, status, funded_at, released_at)
               VALUES (?,?,299.0,1,'approved',datetime('now','-12 days'),datetime('now','-5 days'))""",
            [completed_order_id, "Logo design delivery"]
        )

        # Escrow hold (already released)
        db.execute(
            """INSERT INTO escrow_holds (order_id, amount, status, stripe_payment_intent_id, created_at, released_at)
               VALUES (?,299.0,'released',?,datetime('now','-12 days'),datetime('now','-5 days'))""",
            [completed_order_id, fake_payment_intent_id()]
        )

        # Platform revenue for this order
        db.execute(
            "INSERT INTO platform_revenue (order_id, fee_amount, fee_type) VALUES (?,2.99,'service_fee')",
            [completed_order_id]
        )

        # Mutual reviews for the completed order
        db.execute(
            """INSERT INTO reviews (order_id, from_user_id, to_user_id, rating, text, is_visible)
               VALUES (?,?,?,5,'Sarah delivered an outstanding logo that exceeded our expectations. Fast, professional, and highly recommended.',1)""",
            [completed_order_id, employer_ids[0], worker_ids[0]]
        )
        db.execute(
            """INSERT INTO reviews (order_id, from_user_id, to_user_id, rating, text, is_visible)
               VALUES (?,?,?,5,'Great client — clear brief, responsive feedback, and paid on time. Pleasure to work with.',1)""",
            [completed_order_id, worker_ids[0], employer_ids[0]]
        )

        # Update rating averages for the completed order participants
        db.execute("UPDATE worker_profiles SET avg_rating=4.9, total_reviews=35, total_orders_completed=1 WHERE user_id=?", [worker_ids[0]])
        db.execute("UPDATE employer_profiles SET avg_rating=5.0, total_reviews=1, total_orders=1 WHERE user_id=?", [employer_ids[0]])

        # ── Notifications ─────────────────────────────────────────────────────
        push_notification(db, worker_ids[0], "welcome",
            "Welcome to GoHireHumans!",
            "Your profile is live. Browse jobs or manage your services from your dashboard.",
            "/dashboard")
        push_notification(db, employer_ids[0], "welcome",
            "Welcome to GoHireHumans!",
            "Post a job or browse services to find talented professionals.",
            "/dashboard")

        db.commit()

        return json_response({
            "message": "Seed data created successfully",
            "admin": {"email": "admin@gohirehumans.com", "password": "Admin1234!", "note": "Change password in production"},
            "workers": [{"email": w['email'], "password": "Worker1234!"} for w in workers_data],
            "employers": [{"email": e['email'], "password": "Employer1234!"} for e in employers_data],
            "services_created": len(service_ids),
            "jobs_created": len(job_ids),
            "sample_completed_order_id": completed_order_id,
            "stripe_mode": "live" if stripe_configured() else "simulated"
        }, 201)

    # ═══════════════════════════════════════════════════════════════════════════
    # 404 FALLTHROUGH
    # ═══════════════════════════════════════════════════════════════════════════

    else:
        return error_response(f"Route not found: {method} {path}", 404)
