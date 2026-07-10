#!/usr/bin/env python3
"""
GoHireHumans API - CGI Backend (Redesign)
Two-sided marketplace: workers post services, employers post jobs.
Routes via PATH_INFO. Called by server.py handle_request().
"""

import base64
import gzip
import json
import math
import os
import sys
import sqlite3
import hashlib
import html
import hmac
import secrets
import tempfile
import time
import re
import threading
import urllib.parse
import urllib.request
from datetime import datetime, timezone, timedelta
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP

# Thread-local storage for per-request context (avoids os.environ race conditions)
_request_ctx = threading.local()

try:
    import stripe
    STRIPE_AVAILABLE = True
    STRIPE_SIGNATURE_ERROR = getattr(stripe, "SignatureVerificationError", ValueError)
except ImportError:
    STRIPE_AVAILABLE = False
    STRIPE_SIGNATURE_ERROR = ValueError


# ─── Config ───────────────────────────────────────────────────────────────────

SERVICE_FEE_RATE = 0.01  # 1% platform fee charged to employer on top of amount
PROCESSING_FEE_RATE = 0.03  # ~3% payment processing fee passed to buyer (covers Stripe costs)
MAX_ORDER_NOTES_LENGTH = 5000
RESEND_API_KEY = os.environ.get("RESEND_API_KEY", "").strip()
EMAIL_FROM = os.environ.get("EMAIL_FROM", "GoHireHumans <hello@gohirehumans.com>")
APP_BASE_URL = os.environ.get("APP_BASE_URL", "https://www.gohirehumans.com").rstrip("/")

# Railway volume mount: store DB in /data (the volume mount point).
# The Dockerfile creates /data, and Railway mounts a persistent volume there.
_VOLUME_DIR = "/data"
_VOLUME_ATTACHED = bool(os.environ.get("RAILWAY_VOLUME_MOUNT_PATH", ""))

# DB_PATH is resolved lazily on first request to ensure volume is mounted
_db_path_resolved = None

def _get_db_path():
    global _db_path_resolved
    if _db_path_resolved is not None:
        return _db_path_resolved

    # Build candidate list: persistent volume FIRST, then explicit env var, then CWD
    # The volume at /data is the only truly persistent storage on Railway.
    # /app/ is ephemeral — it gets wiped on every deploy.
    candidates = []
    candidates.append(os.path.join(_VOLUME_DIR, "gohirehumans.db"))  # /data/gohirehumans.db (PERSISTENT)
    explicit = os.environ.get("DATABASE_PATH", "")
    if explicit and not explicit.startswith("/app"):
        # Only use explicit path if it's NOT under /app (ephemeral container fs)
        candidates.append(explicit)
    candidates.append(os.path.join(os.getcwd(), "gohirehumans.db"))  # /app/gohirehumans.db (ephemeral fallback)

    for candidate in candidates:
        parent = os.path.dirname(candidate) or "."
        try:
            os.makedirs(parent, exist_ok=True)
            test_db = sqlite3.connect(candidate)
            test_db.execute("CREATE TABLE IF NOT EXISTS _ping (id INTEGER)")
            test_db.commit()
            test_db.close()
            _db_path_resolved = candidate
            print(f"[GoHireHumans] DB path: {candidate}", file=sys.stderr)
            return _db_path_resolved
        except Exception as e:
            print(f"[GoHireHumans] Cannot use {candidate}: {e}", file=sys.stderr)

    # Last resort: in-memory (won't persist but at least won't crash)
    _db_path_resolved = ":memory:"
    print(f"[GoHireHumans] CRITICAL: Using in-memory DB (no persistence!)", file=sys.stderr)
    return _db_path_resolved
STRIPE_SECRET_KEY = os.environ.get("STRIPE_SECRET_KEY", "").strip()
STRIPE_PUBLISHABLE_KEY = os.environ.get("STRIPE_PUBLISHABLE_KEY", "").strip()
STRIPE_WEBHOOK_SECRET = os.environ.get("STRIPE_WEBHOOK_SECRET", "").strip()
FRONTEND_URL = os.environ.get("FRONTEND_URL", "https://gohirehumans.com")
SEED_SECRET = os.environ.get("SEED_SECRET", "")
DIAGNOSTIC_ENDPOINT_ENABLED = os.environ.get("ENABLE_DIAGNOSTIC_ENDPOINT", "").strip().lower() in {"1", "true", "yes"}
DIAGNOSTIC_SECRET = os.environ.get("DIAGNOSTIC_SECRET", "").strip()
BACKUP_SECRET = os.environ.get("BACKUP_SECRET", "").strip()
ENABLE_AUTO_SEED = os.environ.get("ENABLE_AUTO_SEED", "").strip().lower() in {"1", "true", "yes"}
PRODUCTION_MODE = os.environ.get("ENVIRONMENT", os.environ.get("RAILWAY_ENVIRONMENT", "")).strip().lower() in {"production", "prod"}
# Keep new job hires paused until funding idempotency and post-charge
# reconciliation ship in the dedicated payment-hardening change.
JOB_HIRING_ENABLED = os.environ.get("JOB_HIRING_ENABLED", "").strip().lower() in {"1", "true", "yes"}
# Hourly approval and termination can move or refund money. Keep both server-side
# paths closed until the processor-safe ledger/reconciler is deployed.
HOURLY_SETTLEMENT_ENABLED = os.environ.get("HOURLY_SETTLEMENT_ENABLED", "").strip().lower() in {"1", "true", "yes"}
MAX_MONEY_INPUT_CHARS = 96
MAX_MONEY_ABS = Decimal("999999.99")
PLATFORM_FEE_BPS = 100
PROCESSING_FEE_BPS = 300



def _secret_header_matches(header_attr, expected_secret):
    provided = (
        getattr(_request_ctx, header_attr, "")
        or os.environ.get(header_attr.upper(), "")
    ).strip()
    return bool(provided) and bool(expected_secret) and hmac.compare_digest(provided, expected_secret)


def diagnostic_endpoint_allowed():
    """Return True only when production diagnostics are explicitly enabled.

    The diagnostic endpoint exposes operational internals and must remain off by
    default. When temporarily needed, it requires both an opt-in env var and a
    per-request secret header so accidental/public exposure fails closed.
    """
    if not DIAGNOSTIC_ENDPOINT_ENABLED or not DIAGNOSTIC_SECRET:
        return False
    return _secret_header_matches("http_x_diagnostic_secret", DIAGNOSTIC_SECRET)


def backup_endpoint_allowed():
    """Return True only when backup retrieval is protected by a strong secret."""
    return _secret_header_matches("http_x_backup_secret", BACKUP_SECRET)


def google_oauth_configured():
    """Google auth must fail closed unless the expected OAuth client ID is configured."""
    return bool(os.environ.get("GOOGLE_CLIENT_ID", "").strip())


def parse_int_param(params, name, default, min_value=None, max_value=None):
    raw = params.get(name, default)
    try:
        value = int(raw)
    except (TypeError, ValueError):
        raise ValueError(f"Invalid {name}: must be an integer")
    if min_value is not None and value < min_value:
        raise ValueError(f"Invalid {name}: must be at least {min_value}")
    if max_value is not None and value > max_value:
        value = max_value
    return value


def parse_float_param(params, name, default=None, min_value=None, max_value=None):
    raw = params.get(name)
    if raw in (None, ""):
        return default
    try:
        value = float(raw)
    except (TypeError, ValueError):
        raise ValueError(f"Invalid {name}: must be a number")
    if min_value is not None and value < min_value:
        raise ValueError(f"Invalid {name}: must be at least {min_value}")
    if max_value is not None and value > max_value:
        raise ValueError(f"Invalid {name}: must be at most {max_value}")
    return value


def safe_positive_amount(value, field_name, max_value=1000000):
    try:
        amount = float(value)
    except (TypeError, ValueError):
        raise ValueError(f"{field_name} must be a number")
    if amount <= 0:
        raise ValueError(f"{field_name} must be greater than zero")
    if amount > max_value:
        raise ValueError(f"{field_name} is too large")
    return amount


def stripe_configured():
    return STRIPE_AVAILABLE and bool(STRIPE_SECRET_KEY)


def stripe_attr(obj, name, default=None):
    if isinstance(obj, dict):
        return obj.get(name, default)
    return getattr(obj, name, default)


def is_live_connect_account_ready(account):
    if not account:
        return False
    capabilities = stripe_attr(account, 'capabilities', {}) or {}
    transfers_capability = stripe_attr(capabilities, 'transfers', None)
    transfers_ready = transfers_capability in (None, 'active')
    return bool(stripe_attr(account, 'payouts_enabled', False)) and bool(stripe_attr(account, 'charges_enabled', False)) and transfers_ready


def retrieve_live_connect_account(account_id):
    if not stripe_configured() or not account_id or account_id.startswith('acct_sim_'):
        return None
    return stripe.Account.retrieve(account_id)


def record_payout_transfer(db, order_id, milestone_id, worker_id, amount, transfer_type, idempotency_key, destination_account_id, stripe_transfer=None, status='recorded', error_message=''):
    transfer_id = ''
    if stripe_transfer is not None:
        transfer_id = stripe_attr(stripe_transfer, 'id', '') or ''
    db.execute(
        """INSERT INTO payout_transfers
           (order_id, milestone_id, worker_id, amount, currency, transfer_type, stripe_transfer_id,
            idempotency_key, destination_account_id, status, error_message, recorded_at)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,datetime('now'))
           ON CONFLICT(idempotency_key) DO UPDATE SET
             stripe_transfer_id=excluded.stripe_transfer_id,
             destination_account_id=excluded.destination_account_id,
             status=excluded.status,
             error_message=excluded.error_message,
             recorded_at=datetime('now')""",
        [order_id, milestone_id, worker_id, amount, 'usd', transfer_type, transfer_id,
         idempotency_key, destination_account_id or '', status, error_message or '']
    )


if stripe_configured():
    stripe.api_key = STRIPE_SECRET_KEY


# ─── Database ─────────────────────────────────────────────────────────────────

def get_db():
    path = _get_db_path()
    db = sqlite3.connect(path)
    db.row_factory = sqlite3.Row
    # Use DELETE journal mode for compatibility with network/volume filesystems
    # WAL mode requires shared memory which may not work on all volume mounts
    try:
        db.execute("PRAGMA journal_mode=WAL")
    except Exception:
        db.execute("PRAGMA journal_mode=DELETE")
    db.execute("PRAGMA foreign_keys=ON")
    return db


def ensure_one_job_hire_enforcement(db):
    """Install the one-job-hire invariant without destroying legacy history.

    A pre-existing database may legitimately contain duplicate rows created before
    the invariant existed. A partial unique index cannot be added to that database,
    so preserve and audit those rows while installing triggers that reject any new
    duplicate. Once operators reconcile the legacy rows, a later init can add the
    stronger unique index.
    """
    duplicates = db.execute(
        """SELECT job_id, COUNT(*) AS duplicate_count, GROUP_CONCAT(id) AS order_ids
           FROM orders
           WHERE type='job_hire' AND job_id IS NOT NULL
           GROUP BY job_id
           HAVING COUNT(*) > 1
           ORDER BY job_id"""
    ).fetchall()

    if not duplicates:
        db.execute(
            """CREATE UNIQUE INDEX IF NOT EXISTS idx_orders_one_job_hire
               ON orders(job_id) WHERE type='job_hire' AND job_id IS NOT NULL"""
        )
        return

    db.executescript("""
        CREATE TRIGGER IF NOT EXISTS trg_orders_one_job_hire_insert
        BEFORE INSERT ON orders
        WHEN NEW.type='job_hire' AND NEW.job_id IS NOT NULL
          AND EXISTS (
            SELECT 1 FROM orders
            WHERE type='job_hire' AND job_id=NEW.job_id
          )
        BEGIN
          SELECT RAISE(ABORT, 'job already has a hire order');
        END;

        CREATE TRIGGER IF NOT EXISTS trg_orders_one_job_hire_update
        BEFORE UPDATE OF type, job_id ON orders
        WHEN NEW.type='job_hire' AND NEW.job_id IS NOT NULL
          AND EXISTS (
            SELECT 1 FROM orders
            WHERE type='job_hire' AND job_id=NEW.job_id AND id<>NEW.id
          )
        BEGIN
          SELECT RAISE(ABORT, 'job already has a hire order');
        END;
    """)
    for row in duplicates:
        details = json.dumps({
            "job_id": row["job_id"],
            "duplicate_count": row["duplicate_count"],
            "order_ids": [int(value) for value in str(row["order_ids"] or "").split(",") if value],
            "action_required": "reconcile_supported_paths_before_unique_index",
        }, sort_keys=True)
        db.execute(
            """INSERT INTO audit_log (user_id, action, entity_type, entity_id, details)
               SELECT NULL, 'legacy_duplicate_job_hire_detected', 'job', ?, ?
               WHERE NOT EXISTS (
                 SELECT 1 FROM audit_log
                 WHERE action='legacy_duplicate_job_hire_detected'
                   AND entity_type='job' AND entity_id=?
               )""",
            [row["job_id"], details, row["job_id"]],
        )


def init_db():
    db = get_db()
    db.executescript("""
    CREATE TABLE IF NOT EXISTS users (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        email TEXT UNIQUE NOT NULL,
        password_hash TEXT NOT NULL DEFAULT '',
        name TEXT NOT NULL DEFAULT '',
        avatar_url TEXT,
        google_sub TEXT,
        referral_code TEXT UNIQUE,
        referred_by INTEGER REFERENCES users(id),
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

    CREATE TABLE IF NOT EXISTS payout_transfers (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        order_id INTEGER REFERENCES orders(id),
        milestone_id INTEGER REFERENCES milestones(id),
        worker_id INTEGER REFERENCES users(id),
        amount REAL NOT NULL,
        currency TEXT DEFAULT 'usd',
        transfer_type TEXT NOT NULL,
        stripe_transfer_id TEXT,
        idempotency_key TEXT NOT NULL UNIQUE,
        destination_account_id TEXT,
        status TEXT NOT NULL DEFAULT 'recorded' CHECK(status IN ('pending','recorded','failed','simulated')),
        error_message TEXT DEFAULT '',
        created_at TEXT DEFAULT (datetime('now')),
        recorded_at TEXT
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
    CREATE INDEX IF NOT EXISTS idx_payout_transfers_order ON payout_transfers(order_id);
    CREATE INDEX IF NOT EXISTS idx_payout_transfers_worker ON payout_transfers(worker_id);
    CREATE INDEX IF NOT EXISTS idx_payout_transfers_idempotency ON payout_transfers(idempotency_key);
    CREATE INDEX IF NOT EXISTS idx_sessions_token ON sessions(token);
    """)
    ensure_one_job_hire_enforcement(db)
    db.commit()

    # ── AI marketplace migrations ─────────────────────────────────────
    # Add AI columns to services (safe: ALTER TABLE ADD COLUMN is idempotent-ish in SQLite)
    for col_sql in [
        "ALTER TABLE services ADD COLUMN provider_type TEXT DEFAULT 'human'",
        "ALTER TABLE services ADD COLUMN fulfillment_type TEXT DEFAULT 'manual'",
        "ALTER TABLE services ADD COLUMN api_endpoint TEXT DEFAULT ''",
        "ALTER TABLE services ADD COLUMN ai_model TEXT DEFAULT ''",
        "ALTER TABLE services ADD COLUMN avg_response_time TEXT DEFAULT ''",
    ]:
        try:
            db.execute(col_sql)
        except sqlite3.OperationalError:
            pass  # Column already exists

    # Add is_ai_agent flag to users
    try:
        db.execute("ALTER TABLE users ADD COLUMN is_ai_agent INTEGER DEFAULT 0")
    except sqlite3.OperationalError:
        pass

    # Add AI indexes
    try:
        db.execute("CREATE INDEX IF NOT EXISTS idx_services_provider_type ON services(provider_type)")
    except sqlite3.OperationalError:
        pass

    # ── Google OAuth + Referral program migrations ──────────────────────
    # Note: SQLite cannot ADD COLUMN with UNIQUE constraint, so add column
    # without UNIQUE first, then create a unique index separately.
    for col_sql in [
        "ALTER TABLE users ADD COLUMN google_sub TEXT",
        "ALTER TABLE users ADD COLUMN referral_code TEXT",
        "ALTER TABLE users ADD COLUMN referred_by INTEGER REFERENCES users(id)",
    ]:
        try:
            db.execute(col_sql)
            print(f"[GoHireHumans] Migration OK: {col_sql}", file=sys.stderr)
        except sqlite3.OperationalError:
            pass  # Column already exists

    db.execute("""CREATE TABLE IF NOT EXISTS referrals (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        referrer_id INTEGER NOT NULL REFERENCES users(id),
        referred_id INTEGER NOT NULL REFERENCES users(id),
        status TEXT DEFAULT 'signed_up',
        reward_type TEXT DEFAULT 'credit',
        reward_amount REAL DEFAULT 0,
        created_at TEXT DEFAULT (datetime('now')),
        converted_at TEXT
    )""")
    try:
        db.execute("CREATE INDEX IF NOT EXISTS idx_referrals_referrer ON referrals(referrer_id)")
        db.execute("CREATE INDEX IF NOT EXISTS idx_users_referral_code ON users(referral_code)")
    except sqlite3.OperationalError:
        pass

    # ── API Keys (for AI agent integration) ──────────────────────────────────
    db.execute("""CREATE TABLE IF NOT EXISTS api_keys (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER NOT NULL REFERENCES users(id),
        key_hash TEXT NOT NULL,
        key_prefix TEXT NOT NULL,
        name TEXT NOT NULL DEFAULT 'Default Key',
        scopes TEXT DEFAULT '["read","write"]',
        rate_limit INTEGER DEFAULT 100,
        is_active INTEGER DEFAULT 1,
        last_used_at TEXT,
        total_requests INTEGER DEFAULT 0,
        created_at TEXT DEFAULT (datetime('now')),
        expires_at TEXT
    )""")
    try:
        db.execute("CREATE INDEX IF NOT EXISTS idx_api_keys_hash ON api_keys(key_hash)")
        db.execute("CREATE INDEX IF NOT EXISTS idx_api_keys_prefix ON api_keys(key_prefix)")
        db.execute("CREATE INDEX IF NOT EXISTS idx_api_keys_user ON api_keys(user_id)")
    except sqlite3.OperationalError:
        pass

    # ── API Key Usage Log ─────────────────────────────────────────────────────
    db.execute("""CREATE TABLE IF NOT EXISTS api_key_usage (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        api_key_id INTEGER NOT NULL REFERENCES api_keys(id),
        endpoint TEXT NOT NULL,
        method TEXT NOT NULL,
        status_code INTEGER,
        response_time_ms INTEGER,
        created_at TEXT DEFAULT (datetime('now'))
    )""")
    try:
        db.execute("CREATE INDEX IF NOT EXISTS idx_usage_key ON api_key_usage(api_key_id)")
        db.execute("CREATE INDEX IF NOT EXISTS idx_usage_date ON api_key_usage(created_at)")
    except sqlite3.OperationalError:
        pass

    # ── Owner admin bootstrap ───────────────────────────────────────────────
    # Requested by Billy Ray: make enzo@profilesearch.com an admin account and
    # set a locally stored strong password so Hermes can perform admin ops.
    db.execute(
        """UPDATE users
           SET is_admin=1, is_active=1, is_suspended=0, is_banned=0,
               password_hash=?, updated_at=datetime('now')
           WHERE lower(email)=lower(?)""",
        [
            "b719c181144650cf39b3c0036c4bd010:1f742ba78ac305a14137a54f0a0c5a24da241fe2185dfe7954aafb7082ec01d1",
            "enzo@profilesearch.com",
        ]
    )

    db.commit()
    db.close()


SEEDED_SAMPLE_EMAILS = {
    "sarah.chen@example.com",
    "marcus.johnson@example.com",
    "elena.rodriguez@example.com",
    "james.park@example.com",
    "aisha.patel@example.com",
    "hire@techstartup.io",
    "ops@growthagency.com",
    "founder@bootstrapped.co",
}


def is_seeded_sample_email(email):
    return (email or "").strip().lower() in SEEDED_SAMPLE_EMAILS


def seeded_sample_email_placeholders():
    return ",".join("?" for _ in SEEDED_SAMPLE_EMAILS)


def seeded_sample_email_values():
    return list(SEEDED_SAMPLE_EMAILS)


def public_non_seeded_user_condition(user_alias="u"):
    return f"LOWER({user_alias}.email) NOT IN ({seeded_sample_email_placeholders()})"


def public_non_seeded_user_values():
    return seeded_sample_email_values()


def public_non_seeded_user_subquery():
    return f"SELECT id FROM users WHERE LOWER(email) IN ({seeded_sample_email_placeholders()})"


_seeded = False
def auto_seed_if_empty():
    """Auto-seed sample data only when explicitly enabled for demos/staging."""
    global _seeded
    if _seeded or not ENABLE_AUTO_SEED:
        return
    _seeded = True
    db = get_db()
    try:
        count = db.execute("SELECT COUNT(*) as c FROM users").fetchone()['c']
        if count > 0:
            return
        print("Auto-seeding sample data...", file=sys.stderr)

        # Create Workers
        workers_data = [
            {"email": "sarah.chen@example.com", "name": "Sarah Chen",
             "skills": ["graphic_design", "ui_ux_design", "content_creation"],
             "bio": "Freelance designer with 5 years experience in brand identity and digital design. Specializes in clean, modern aesthetics.",
             "hourly_rate": 65.0, "avg_rating": 4.9, "total_reviews": 34},
            {"email": "marcus.johnson@example.com", "name": "Marcus Johnson",
             "skills": ["web_development", "mobile_development", "software_development"],
             "bio": "Full-stack developer (React, Node.js, Python). 7 years building web apps and APIs. Fast turnaround, clean code.",
             "hourly_rate": 90.0, "avg_rating": 4.8, "total_reviews": 52},
            {"email": "elena.rodriguez@example.com", "name": "Elena Rodriguez",
             "skills": ["writing", "copywriting", "translation", "seo"],
             "bio": "Bilingual (English/Spanish) content writer and SEO specialist. Former marketing manager turned freelancer.",
             "hourly_rate": 55.0, "avg_rating": 4.7, "total_reviews": 28},
            {"email": "james.park@example.com", "name": "James Park",
             "skills": ["accounting", "bookkeeping", "data_analysis"],
             "bio": "CPA with 10 years in corporate finance. Available for bookkeeping, financial modeling, and tax prep.",
             "hourly_rate": 85.0, "avg_rating": 5.0, "total_reviews": 17},
            {"email": "aisha.patel@example.com", "name": "Aisha Patel",
             "skills": ["digital_marketing", "social_media", "content_creation"],
             "bio": "Digital marketing specialist with expertise in paid social, email campaigns, and brand strategy.",
             "hourly_rate": 70.0, "avg_rating": 4.6, "total_reviews": 21},
        ]
        worker_ids = []
        for w in workers_data:
            cursor = db.execute("INSERT INTO users (email, password_hash, name) VALUES (?,?,?)",
                [w['email'], hash_password('Worker1234!'), w['name']])
            uid = cursor.lastrowid
            worker_ids.append(uid)
            payout_id = f"acct_sim_{secrets.token_hex(8)}"
            db.execute(
                """INSERT INTO worker_profiles (user_id, bio, skills, hourly_rate, payout_account_id,
                   payout_method, avg_rating, total_reviews, is_verified)
                   VALUES (?,?,?,?,?,'stripe_connect_active',?,?,1)""",
                [uid, w['bio'], json.dumps(w['skills']), w['hourly_rate'], payout_id, w['avg_rating'], w['total_reviews']])

        # Create Employers
        employers_data = [
            {"email": "hire@techstartup.io", "name": "Alex Rivera",
             "company_name": "TechStartup.io", "description": "Early-stage SaaS startup building a B2B analytics platform."},
            {"email": "ops@growthagency.com", "name": "Jordan Lee",
             "company_name": "Growth Agency Co.", "description": "Full-service growth marketing agency serving e-commerce brands."},
            {"email": "founder@bootstrapped.co", "name": "Taylor Kim",
             "company_name": "Bootstrapped.co", "description": "Solo founder building multiple SaaS products."},
        ]
        employer_ids = []
        for e in employers_data:
            cursor = db.execute("INSERT INTO users (email, password_hash, name) VALUES (?,?,?)",
                [e['email'], hash_password('Employer1234!'), e['name']])
            uid = cursor.lastrowid
            employer_ids.append(uid)
            pm_id = f"pm_sim_{secrets.token_hex(8)}"
            cus_id = f"cus_sim_{secrets.token_hex(8)}"
            db.execute(
                "INSERT INTO employer_profiles (user_id, company_name, description, payment_method_id, stripe_customer_id) VALUES (?,?,?,?,?)",
                [uid, e['company_name'], e['description'], pm_id, cus_id])

        # Create Services
        services_data = [
            {"w": 0, "cat": "graphic_design", "pt": "fixed", "title": "I will design a professional logo with brand guidelines",
             "desc": "Get a unique, modern logo for your business with a full brand guidelines document. Includes 3 concepts, unlimited revisions until you're happy, all source files (AI, SVG, PNG).",
             "price": 299.0, "hr": None, "days": 5, "inc": "3 logo concepts, brand guidelines PDF, all source files, commercial license",
             "tags": ["logo", "branding", "graphic design", "identity"]},
            {"w": 1, "cat": "web_development", "pt": "hourly", "title": "Full-stack web development (React + Node.js)",
             "desc": "Expert full-stack development using React, TypeScript, Node.js, and PostgreSQL. Available for new projects, feature development, bug fixes, and code reviews.",
             "price": None, "hr": 90.0, "days": None, "inc": "Clean, documented code, unit tests, code review, deployment support",
             "tags": ["react", "nodejs", "typescript", "fullstack"]},
            {"w": 2, "cat": "writing", "pt": "fixed", "title": "SEO blog post (1500-2000 words) with keyword research",
             "desc": "Well-researched, engaging blog post optimized for your target keywords. Includes keyword research, outline, writing, basic on-page SEO recommendations, and 1 revision.",
             "price": 150.0, "hr": None, "days": 3, "inc": "Keyword research report, 1500-2000 word post, meta description, 1 revision",
             "tags": ["seo", "blog", "content writing", "copywriting"]},
            {"w": 3, "cat": "accounting", "pt": "fixed", "title": "Monthly bookkeeping for small business (up to 200 transactions)",
             "desc": "Complete monthly bookkeeping service: categorize transactions, reconcile accounts, generate P&L and balance sheet. Works with QuickBooks, Xero, or Wave.",
             "price": 350.0, "hr": None, "days": 7, "inc": "Transaction categorization, bank reconciliation, monthly P&L, balance sheet",
             "tags": ["bookkeeping", "accounting", "quickbooks", "small business"]},
            {"w": 4, "cat": "digital_marketing", "pt": "fixed", "title": "Complete Facebook & Instagram ad campaign setup",
             "desc": "Full paid social campaign setup including audience research, creative brief, ad copy, A/B test variants, pixel setup, and campaign launch.",
             "price": 499.0, "hr": None, "days": 7, "inc": "Audience research, 3 ad variations, pixel setup, campaign launch, 2-week monitoring",
             "tags": ["facebook ads", "instagram", "paid social", "digital marketing"]},
            {"w": 0, "cat": "ui_ux_design", "pt": "fixed", "title": "UI/UX design for mobile app (up to 10 screens)",
             "desc": "Professional mobile app design for iOS or Android. Includes user flow diagram, wireframes, and high-fidelity Figma designs for up to 10 screens.",
             "price": 650.0, "hr": None, "days": 10, "inc": "User flow, wireframes, 10 Figma screens, component library, handoff file",
             "tags": ["figma", "mobile design", "ui design", "ux design"]},
            {"w": 2, "cat": "translation", "pt": "custom", "title": "English to Spanish translation (marketing & technical content)",
             "desc": "Native-quality English-Spanish translation for marketing copy, technical docs, websites, and legal docs. Proofreading included. Pricing per word.",
             "price": None, "hr": None, "days": 3, "inc": "Native Spanish translation, proofreading, glossary for technical terms",
             "tags": ["spanish", "translation", "marketing translation", "localization"]},
            {"w": 1, "cat": "mobile_development", "pt": "fixed", "title": "React Native app MVP (4-6 screens)",
             "desc": "Build your mobile app MVP using React Native for cross-platform iOS and Android deployment. Includes navigation, API integration, and app store submission guidance.",
             "price": 2500.0, "hr": None, "days": 21, "inc": "React Native codebase, 4-6 screens, API integration, testing, source code",
             "tags": ["react native", "mobile app", "ios", "android", "mvp"]},
        ]
        service_ids = []
        for s in services_data:
            cursor = db.execute(
                """INSERT INTO services (worker_id, title, description, category, pricing_type, price, hourly_rate,
                   delivery_time_days, includes, tags, images, status, avg_rating, total_reviews)
                   VALUES (?,?,?,?,?,?,?,?,?,?,'[]','active',?,?)""",
                [worker_ids[s['w']], s['title'], s['desc'], s['cat'], s['pt'], s['price'], s['hr'],
                 s['days'], s['inc'], json.dumps(s['tags']),
                 round(4.5 + secrets.randbelow(5) * 0.1, 1), secrets.randbelow(20) + 5])
            service_ids.append(cursor.lastrowid)

        # Create Jobs
        jobs_data = [
            {"e": 0, "cat": "web_development", "title": "React frontend developer needed for SaaS dashboard (3-month contract)",
             "desc": "We're building a B2B analytics dashboard and need an experienced React developer. Tech stack: React 18, TypeScript, Tailwind CSS, Recharts.",
             "loc": "remote", "bt": "hourly", "ba": 85.0, "eh": 480, "sk": ["web_development", "software_development"], "st": "open"},
            {"e": 1, "cat": "content_creation", "title": "Content writer for e-commerce blog \u2014 8 articles/month",
             "desc": "Seeking a content writer to produce 8 SEO-optimized blog articles per month for our e-commerce clients. Topics: fashion, home decor, fitness.",
             "loc": "remote", "bt": "fixed", "ba": 1200.0, "eh": None, "sk": ["writing", "copywriting", "seo"], "st": "open"},
            {"e": 2, "cat": "graphic_design", "title": "Brand designer for new SaaS product",
             "desc": "Looking for a brand designer to create the visual identity for our new developer tool. Deliverables: logo, color palette, typography, brand guidelines.",
             "loc": "remote", "bt": "fixed", "ba": 800.0, "eh": None, "sk": ["graphic_design", "ui_ux_design"], "st": "open"},
            {"e": 0, "cat": "digital_marketing", "title": "Growth marketer to set up and run paid acquisition",
             "desc": "Early-stage SaaS startup seeking a growth marketer to manage paid acquisition (Google Ads, LinkedIn Ads). Monthly budget: $5K. Must have B2B SaaS experience.",
             "loc": "remote", "bt": "hourly", "ba": 75.0, "eh": 40, "sk": ["digital_marketing", "seo"], "st": "open"},
            {"e": 1, "cat": "data_analysis", "title": "Data analyst to build performance dashboard in Looker Studio",
             "desc": "Connect Google Ads, GA4, and Shopify data to Looker Studio and build a client-facing performance dashboard.",
             "loc": "remote", "bt": "fixed", "ba": 1500.0, "eh": None, "sk": ["data_analysis", "data_entry"], "st": "open"},
            {"e": 2, "cat": "mobile_development", "title": "iOS developer for fintech app feature (Plaid integration)",
             "desc": "Implement Plaid bank connection flow in our existing Swift/SwiftUI fintech app. Must have experience with iOS development and Plaid SDK.",
             "loc": "remote", "bt": "fixed", "ba": 3500.0, "eh": None, "sk": ["mobile_development", "software_development"], "st": "open"},
        ]
        job_ids = []
        for j in jobs_data:
            cursor = db.execute(
                """INSERT INTO jobs (employer_id, title, description, category, location_type, budget_type,
                   budget_amount, estimated_hours, required_skills, status)
                   VALUES (?,?,?,?,?,?,?,?,?,?)""",
                [employer_ids[j['e']], j['title'], j['desc'], j['cat'], j['loc'], j['bt'],
                 j['ba'], j.get('eh'), json.dumps(j['sk']), j['st']])
            job_ids.append(cursor.lastrowid)

        # Sample Applications
        db.execute("INSERT INTO applications (job_id, worker_id, cover_message, portfolio_url, status) VALUES (?,?,?,?,'pending')",
            [job_ids[0], worker_ids[1], "Full-stack developer with 7 years React experience. Built several SaaS dashboards.", "https://github.com/marcusjohnson"])
        db.execute("INSERT INTO applications (job_id, worker_id, cover_message, portfolio_url, status) VALUES (?,?,?,?,'pending')",
            [job_ids[1], worker_ids[2], "Experienced content writer with strong SEO knowledge. Hundreds of e-commerce articles delivered on time.", "https://elenawritescopy.com"])

        # Completed Order + Reviews
        oc = db.execute(
            """INSERT INTO orders (type, service_id, worker_id, employer_id, status, total_amount,
               completed_at, created_at, updated_at)
               VALUES ('service_order',?,?,?,'completed',299.0,datetime('now','-5 days'),datetime('now','-12 days'),datetime('now','-5 days'))""",
            [service_ids[0], worker_ids[0], employer_ids[0]])
        oid = oc.lastrowid
        db.execute("INSERT INTO milestones (order_id, title, amount, sequence, status, funded_at, released_at) VALUES (?,?,299.0,1,'approved',datetime('now','-12 days'),datetime('now','-5 days'))",
            [oid, "Logo design delivery"])
        fpi = f"pi_sim_{secrets.token_hex(12)}"
        db.execute("INSERT INTO escrow_holds (order_id, amount, status, stripe_payment_intent_id, created_at, released_at) VALUES (?,299.0,'released',?,datetime('now','-12 days'),datetime('now','-5 days'))",
            [oid, fpi])
        db.execute("INSERT INTO platform_revenue (order_id, fee_amount, fee_type) VALUES (?,2.99,'service_fee')", [oid])
        db.execute("INSERT INTO reviews (order_id, from_user_id, to_user_id, rating, text, is_visible) VALUES (?,?,?,5,'Sarah delivered an outstanding logo. Fast, professional, highly recommended.',1)",
            [oid, employer_ids[0], worker_ids[0]])
        db.execute("INSERT INTO reviews (order_id, from_user_id, to_user_id, rating, text, is_visible) VALUES (?,?,?,5,'Great client. Clear brief, responsive feedback, paid on time.',1)",
            [oid, worker_ids[0], employer_ids[0]])

        db.commit()
        print("Auto-seed complete: 5 workers, 3 employers, 8 services, 6 jobs", file=sys.stderr)
    except Exception as ex:
        print(f"Auto-seed error: {ex}", file=sys.stderr)
    finally:
        db.close()


# ─── Rate Limiter ──────────────────────────────────────────────────────────────

_rate_limit_store = {}
_login_failure_store = {}
_rate_limit_lock = threading.Lock()


def check_rate_limit() -> bool:
    ip = getattr(_request_ctx, 'remote_addr', 'unknown')
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


def login_attempt_allowed(email):
    """Fail closed after repeated login failures for the same IP+email tuple."""
    ip = getattr(_request_ctx, 'remote_addr', 'unknown')
    key = f"{ip}:{(email or '').strip().lower()}"
    now = time.time()
    window = 15 * 60
    limit = 6
    with _rate_limit_lock:
        failures = [t for t in _login_failure_store.get(key, []) if now - t < window]
        _login_failure_store[key] = failures
        return len(failures) < limit


def record_login_failure(email):
    ip = getattr(_request_ctx, 'remote_addr', 'unknown')
    key = f"{ip}:{(email or '').strip().lower()}"
    now = time.time()
    window = 15 * 60
    with _rate_limit_lock:
        failures = [t for t in _login_failure_store.get(key, []) if now - t < window]
        failures.append(now)
        _login_failure_store[key] = failures


def clear_login_failures(email):
    ip = getattr(_request_ctx, 'remote_addr', 'unknown')
    key = f"{ip}:{(email or '').strip().lower()}"
    with _rate_limit_lock:
        _login_failure_store.pop(key, None)


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

PAYMENT_CIRCUMVENTION_PATTERNS = [
    r"\bpaypal\.me/\S+",
    r"\bpaypal\b",
    r"\bvenmo\b",
    r"\bcash\s*app\b",
    r"\bcashapp\b",
    r"\bzelle\b",
    r"\bcrypto\b",
    r"\bbitcoin\b",
    r"\bethereum\b",
    r"\bsolana\b",
    r"\bevm\b",
    r"\bwallet\b",
    r"\bdirect\s+payment\b",
    r"\boff[-\s]?platform\s+payment\b",
    r"\bpay\s+me\s+direct\b",
    r"\bsend\s+payment\s+to\b",
]


def check_payment_circumvention(text):
    for pattern in PAYMENT_CIRCUMVENTION_PATTERNS:
        if re.search(pattern, text or "", re.IGNORECASE):
            return False, "Payment instructions must stay on-platform. Do not include direct payment links, wallet addresses, or off-platform payment instructions."
    return True, None


def is_safe_external_url(value):
    if not value:
        return True
    try:
        parsed = urllib.parse.urlparse(str(value).strip())
    except Exception:
        return False
    return parsed.scheme in ("http", "https") and bool(parsed.netloc)


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
    'testing',
    'ai_writing', 'ai_coding', 'ai_image_generation', 'ai_data_analysis',
    'ai_translation', 'ai_voice', 'ai_video', 'ai_chatbot', 'ai_automation',
    'other'
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
    if not hasattr(_request_ctx, 'body_cache'):
        try:
            length = int(getattr(_request_ctx, 'content_length', 0) or 0)
            if length > 0:
                if hasattr(_request_ctx, 'raw_body'):
                    raw = _request_ctx.raw_body
                else:
                    raw = getattr(_request_ctx, 'stdin_data', '')
                    _request_ctx.raw_body = raw
                parsed = json.loads(raw)
                if not isinstance(parsed, dict):
                    _request_ctx.body_cache = None
                else:
                    _request_ctx.body_cache = parsed
            else:
                _request_ctx.body_cache = {}
        except json.JSONDecodeError:
            _request_ctx.body_cache = None
        except (ValueError, OSError):
            _request_ctx.body_cache = None
    return _request_ctx.body_cache


def get_body_raw():
    """Return raw request body as bytes (needed for Stripe webhook signature verification)."""
    if not hasattr(_request_ctx, 'raw_body'):
        # Prefer raw bytes if available (set by server.py)
        raw_bytes = getattr(_request_ctx, 'stdin_data_raw', None)
        if raw_bytes is not None and isinstance(raw_bytes, bytes):
            _request_ctx.raw_body = raw_bytes
        else:
            # Fallback: encode text data to bytes
            text_data = getattr(_request_ctx, 'stdin_data', '')
            _request_ctx.raw_body = text_data.encode('utf-8') if isinstance(text_data, str) else text_data
    return _request_ctx.raw_body


def get_query_params():
    qs = getattr(_request_ctx, 'query_string', '')
    return dict(urllib.parse.parse_qsl(qs))


def row_to_dict(row):
    if row is None:
        return None
    return dict(row)


def money_to_cents(value, field_name="amount"):
    """Convert a JSON/SQLite money value to exact cents without Decimal context rounding."""
    text = str(value)
    if (not text or text != text.strip() or len(text) > MAX_MONEY_INPUT_CHARS
            or not re.fullmatch(r"[+-]?[0-9]+(?:\.[0-9]+)?", text)):
        raise ValueError(f"{field_name} must be a valid amount in whole cents")
    try:
        decimal_value = Decimal(text)
    except (InvalidOperation, TypeError, ValueError):
        raise ValueError(f"{field_name} must be a valid amount in whole cents")
    if not decimal_value.is_finite() or abs(decimal_value) > MAX_MONEY_ABS:
        raise ValueError(f"{field_name} must be a valid amount in whole cents")
    if decimal_value == 0:
        return 0

    sign, digits, exponent = decimal_value.as_tuple()
    coefficient = int("".join(str(digit) for digit in digits))
    cent_exponent = int(exponent) + 2
    if cent_exponent >= 0:
        cents = coefficient * (10 ** cent_exponent)
    else:
        scale = -cent_exponent
        if scale > len(digits):
            raise ValueError(f"{field_name} must be specified in whole cents")
        divisor = 10 ** scale
        cents, remainder = divmod(coefficient, divisor)
        if remainder:
            raise ValueError(f"{field_name} must be specified in whole cents")
    return -cents if sign else cents


def rounded_product_cents(amount, quantity, field_name="amount"):
    """Multiply a cent-denominated amount by a bounded quantity and round half-up once."""
    base_cents = money_to_cents(amount, field_name)
    quantity_text = str(quantity).strip()
    if not quantity_text or len(quantity_text) > MAX_MONEY_INPUT_CHARS:
        raise ValueError(f"{field_name} quantity must be a valid number")
    try:
        quantity_decimal = Decimal(quantity_text)
    except (InvalidOperation, TypeError, ValueError):
        raise ValueError(f"{field_name} quantity must be a valid number")
    if not quantity_decimal.is_finite() or abs(quantity_decimal) > Decimal("10000"):
        raise ValueError(f"{field_name} quantity must be a valid number")
    return int((Decimal(base_cents) * quantity_decimal).to_integral_value(rounding=ROUND_HALF_UP))


def component_fee_cents(base_cents, basis_points):
    """Round a positive fee component half-up, with the UI's one-cent minimum."""
    if base_cents <= 0:
        return 0
    return max(1, (base_cents * basis_points + 5000) // 10000)


def buyer_charge_breakdown_cents(amount):
    """Return the exact component-rounded buyer charge sent to Stripe."""
    base_cents = money_to_cents(amount, "amount")
    if base_cents <= 0:
        raise ValueError("amount must be greater than zero")
    platform_fee_cents = component_fee_cents(base_cents, PLATFORM_FEE_BPS)
    processing_fee_cents = component_fee_cents(base_cents, PROCESSING_FEE_BPS)
    return {
        "base_cents": base_cents,
        "platform_fee_cents": platform_fee_cents,
        "processing_fee_cents": processing_fee_cents,
        "total_cents": base_cents + platform_fee_cents + processing_fee_cents,
    }


def synchronize_job_terminal_state(db, order, status="completed"):
    """Keep a job-hire order and its job in the same terminal lifecycle state."""
    if order["type"] == "job_hire" and order["job_id"]:
        db.execute(
            "UPDATE jobs SET status=?, updated_at=datetime('now') WHERE id=?",
            [status, order["job_id"]],
        )


def validated_order_notes(value, field_name="notes"):
    if not isinstance(value, str):
        raise ValueError(f"{field_name} must be a string")
    notes = value.strip()
    if not notes:
        raise ValueError(f"{field_name} must not be empty")
    if len(notes) > MAX_ORDER_NOTES_LENGTH:
        raise ValueError(f"{field_name} must be {MAX_ORDER_NOTES_LENGTH} characters or less")
    return notes


def authenticate_session(db):
    token = None
    auth_header = getattr(_request_ctx, 'http_authorization', '')
    if auth_header.startswith("Bearer "):
        token = auth_header[7:].strip()

    # NOTE: Token via query string removed for security (URL leakage via referrer/logs)
    # Tokens must be passed via Authorization: Bearer <token> header only.

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


def authenticate_api_key(db):
    api_key = (getattr(_request_ctx, 'http_x_api_key', '') or os.environ.get('HTTP_X_API_KEY', '')).strip()
    if not api_key or not api_key.startswith('ghh_'):
        return None
    key_hash_val = hashlib.sha256(api_key.encode()).hexdigest()
    row = db.execute(
        """SELECT ak.id as api_key_id, ak.scopes, ak.expires_at,
                  u.*
           FROM api_keys ak
           JOIN users u ON ak.user_id = u.id
           WHERE ak.key_hash = ? AND ak.is_active = 1""",
        [key_hash_val]
    ).fetchone()
    if not row:
        return None
    if row['expires_at']:
        try:
            if datetime.fromisoformat(row['expires_at']) < datetime.now(timezone.utc):
                return None
        except ValueError:
            return None
    if not row['is_active'] or row['is_banned'] or row['is_suspended']:
        return None
    db.execute(
        "UPDATE api_keys SET last_used_at=datetime('now'), total_requests=total_requests+1 WHERE id=?",
        [row['api_key_id']]
    )
    user = row_to_dict(row)
    user.pop('password_hash', None)
    user.pop('api_key_id', None)
    return user


def authenticate(db):
    return authenticate_session(db) or authenticate_api_key(db)


SENSITIVE_AUDIT_KEYS = {'password', 'admin_password', 'new_password', 'token', 'secret', 'api_key', 'authorization'}
SENSITIVE_AUDIT_KEY_FRAGMENTS = ('password', 'passwd', 'token', 'secret', 'api_key', 'apikey', 'authorization', 'bearer', 'credential', 'session')


def is_sensitive_audit_key(key):
    normalized = re.sub(r'[^a-z0-9]+', '_', str(key).strip().lower())
    compact = normalized.replace('_', '')
    return normalized in SENSITIVE_AUDIT_KEYS or any(fragment in normalized or fragment in compact for fragment in SENSITIVE_AUDIT_KEY_FRAGMENTS)


def redact_audit_details(value):
    if isinstance(value, dict):
        redacted = {}
        for key, item in value.items():
            if is_sensitive_audit_key(key):
                redacted[key] = '[REDACTED]'
            else:
                redacted[key] = redact_audit_details(item)
        return redacted
    if isinstance(value, list):
        return [redact_audit_details(item) for item in value]
    return value


def audit(db, user_id, action, entity_type=None, entity_id=None, details=None):
    safe_details = redact_audit_details(details) if details else None
    db.execute(
        "INSERT INTO audit_log (user_id, action, entity_type, entity_id, details) VALUES (?,?,?,?,?)",
        [user_id, action, entity_type, entity_id, json.dumps(safe_details) if safe_details else None]
    )


def require_admin_step_up(db, admin_user, body, action):
    """Require password re-auth before sensitive admin account operations."""
    password = (body or {}).get("admin_password", "")
    if not password:
        audit(db, admin_user['id'], f"{action}_step_up_missing", "user", admin_user['id'])
        db.commit()
        return "Admin password confirmation required", 403
    current = db.execute("SELECT password_hash FROM users WHERE id=?", [admin_user['id']]).fetchone()
    if not current or not verify_password(password, current['password_hash']):
        audit(db, admin_user['id'], f"{action}_step_up_failed", "user", admin_user['id'])
        db.commit()
        return "Admin password confirmation failed", 403
    return None, None


def send_email(to_email, subject, html_body):
    """Send email via Resend API. Silently fails if not configured."""
    if not RESEND_API_KEY:
        return False
    try:
        data = json.dumps({
            "from": EMAIL_FROM,
            "to": [to_email],
            "subject": subject,
            "html": html_body
        }).encode('utf-8')
        req = urllib.request.Request(
            'https://api.resend.com/emails',
            data=data,
            headers={'Authorization': f'Bearer {RESEND_API_KEY}', 'Content-Type': 'application/json'}
        )
        urllib.request.urlopen(req, timeout=10)
        return True
    except Exception:
        return False


def send_welcome_email(email, name):
    """Send welcome onboarding email to new user."""
    first_name = (name or 'there').split()[0]
    html = f"""
    <div style="font-family:'Inter',system-ui,sans-serif;max-width:560px;margin:0 auto;color:#1a1816">
      <div style="background:#0d7377;padding:24px 32px;border-radius:8px 8px 0 0">
        <h1 style="color:white;font-size:20px;margin:0;font-weight:700">Welcome to GoHireHumans</h1>
      </div>
      <div style="background:#faf9f6;padding:32px;border:1px solid #dddbd6;border-top:none;border-radius:0 0 8px 8px">
        <p style="font-size:16px;line-height:1.6;margin-bottom:16px">Hi {first_name},</p>
        <p style="font-size:15px;line-height:1.6;margin-bottom:16px">Thanks for joining GoHireHumans — the marketplace where humans and AI work together.</p>
        <p style="font-size:15px;line-height:1.6;margin-bottom:20px">Here are a few things you can do right now:</p>
        <ul style="font-size:15px;line-height:1.8;margin-bottom:24px;padding-left:20px;color:#4a4a4a">
          <li><strong>Post a service</strong> — list your skills and start earning</li>
          <li><strong>Post a job</strong> — find verified professionals for any task</li>
          <li><strong>Browse services</strong> — hire someone immediately from our catalog</li>
          <li><strong>Explore the AI Marketplace</strong> — see how AI agents can hire humans</li>
        </ul>
        <div style="text-align:center;margin-bottom:24px">
          <a href="https://www.gohirehumans.com" style="display:inline-block;background:#0d7377;color:white;padding:12px 28px;border-radius:6px;text-decoration:none;font-weight:600;font-size:15px">Get Started →</a>
        </div>
        <p style="font-size:13px;color:#6b6963;margin-bottom:8px">Where payment processing is configured, paid work uses platform payment records and review steps before funds are released or refunded.</p>
        <p style="font-size:13px;color:#6b6963">Questions? Reply to this email or check our <a href="https://www.gohirehumans.com/faq.html" style="color:#0d7377">FAQ</a>.</p>
        <hr style="border:none;border-top:1px solid #dddbd6;margin:24px 0 16px">
        <p style="font-size:11px;color:#a8a6a0;text-align:center">&copy; 2026 GoHireHumans · <a href="https://www.gohirehumans.com" style="color:#a8a6a0">gohirehumans.com</a></p>
      </div>
    </div>
    """
    return send_email(email, "Welcome to GoHireHumans — Let's get started", html)


TRANSACTIONAL_EMAIL_NOTIFICATION_TYPES = {
    "job_match",
    "new_application",
    "job_hired",
    "new_order",
    "order_submitted",
    "revision_requested",
    "order_completed",
    "review_request",
}


def notification_platform_url(link):
    link = (link or "").strip()
    if not link:
        return APP_BASE_URL
    if link.startswith("#"):
        return f"{APP_BASE_URL}/{link}"
    if link.startswith("/"):
        return f"{APP_BASE_URL}/#{link}"
    return APP_BASE_URL


def transactional_email_already_sent(db, user_id, notif_type, link, dedupe_context=None):
    dedupe_parts = [str(user_id), str(notif_type), str(link or ""), str(dedupe_context or "")]
    dedupe_key = hashlib.sha256("|".join(dedupe_parts).encode("utf-8")).hexdigest()
    row = db.execute(
        """SELECT id FROM audit_log
           WHERE action='transactional_email_sent'
             AND entity_type='notification_email'
             AND entity_id=?
             AND details LIKE ?
           LIMIT 1""",
        [user_id, f'%"dedupe_key": "{dedupe_key}"%']
    ).fetchone()
    return row is not None, dedupe_key


def send_transactional_notification_email(db, user_id, notif_type, title, message=None, link=None, dedupe_context=None):
    if notif_type not in TRANSACTIONAL_EMAIL_NOTIFICATION_TYPES:
        return False
    user = db.execute("SELECT email, name FROM users WHERE id=? AND is_active=1 AND is_banned=0", [user_id]).fetchone()
    if not user or not user['email']:
        return False
    already_sent, dedupe_key = transactional_email_already_sent(db, user_id, notif_type, link, dedupe_context)
    if already_sent:
        return False

    first_name = html.escape((user['name'] or 'there').split()[0])
    safe_title = html.escape(title or "GoHireHumans activity update")
    safe_message = html.escape(message or "There is an update related to your GoHireHumans account.")
    platform_url = notification_platform_url(link)
    safe_url = html.escape(platform_url, quote=True)
    subject = title or "GoHireHumans activity update"
    html_body = f"""
    <div style="font-family:'Inter',system-ui,sans-serif;max-width:560px;margin:0 auto;color:#1a1816">
      <div style="background:#0d7377;padding:24px 32px;border-radius:8px 8px 0 0">
        <h1 style="color:white;font-size:20px;margin:0;font-weight:700">{safe_title}</h1>
      </div>
      <div style="background:#faf9f6;padding:32px;border:1px solid #dddbd6;border-top:none;border-radius:0 0 8px 8px">
        <p style="font-size:16px;line-height:1.6;margin-bottom:16px">Hi {first_name},</p>
        <p style="font-size:15px;line-height:1.6;margin-bottom:24px">{safe_message}</p>
        <div style="text-align:center;margin-bottom:24px">
          <a href="{safe_url}" style="display:inline-block;background:#0d7377;color:white;padding:12px 28px;border-radius:6px;text-decoration:none;font-weight:600;font-size:15px">View on GoHireHumans →</a>
        </div>
        <p style="font-size:12px;color:#6b6963;line-height:1.5;margin-bottom:8px">You received this because this relates to your GoHireHumans marketplace activity.</p>
        <p style="font-size:12px;color:#6b6963;line-height:1.5">GoHireHumans keeps marketplace communication, work review, and configured payment records on-platform.</p>
        <hr style="border:none;border-top:1px solid #dddbd6;margin:24px 0 16px">
        <p style="font-size:11px;color:#a8a6a0;text-align:center">&copy; 2026 GoHireHumans · <a href="https://www.gohirehumans.com" style="color:#a8a6a0">gohirehumans.com</a></p>
      </div>
    </div>
    """
    try:
        sent = send_email(user['email'], subject, html_body)
    except Exception:
        sent = False
    if sent:
        audit(db, user_id, "transactional_email_sent", "notification_email", user_id, {
            "type": notif_type,
            "link": link or "",
            "dedupe_key": dedupe_key,
        })
    return bool(sent)


def queue_transactional_notification_email(user_id, notif_type, title, message=None, link=None, dedupe_context=None):
    pending = getattr(_request_ctx, 'pending_transactional_emails', None)
    if pending is None:
        pending = []
        _request_ctx.pending_transactional_emails = pending
    pending.append({
        "user_id": user_id,
        "notif_type": notif_type,
        "title": title,
        "message": message or "",
        "link": link or "",
        "dedupe_context": dedupe_context or "",
    })


def flush_transactional_notification_emails(db):
    pending = getattr(_request_ctx, 'pending_transactional_emails', []) or []
    _request_ctx.pending_transactional_emails = []
    audit_written = False
    for item in pending:
        try:
            audit_written = send_transactional_notification_email(
                db,
                item["user_id"],
                item["notif_type"],
                item["title"],
                item.get("message", ""),
                item.get("link", ""),
                item.get("dedupe_context", ""),
            ) or audit_written
        except Exception:
            pass
    if audit_written:
        try:
            db.commit()
        except Exception:
            pass


def push_notification(db, user_id, notif_type, title, message=None, link=None, email=False, email_message=None, email_dedupe=None):
    db.execute(
        "INSERT INTO notifications (user_id, type, title, message, link) VALUES (?,?,?,?,?)",
        [user_id, notif_type, title, message or "", link or ""]
    )
    if email:
        queue_transactional_notification_email(
            user_id,
            notif_type,
            title,
            email_message if email_message is not None else message,
            link,
            email_dedupe if email_dedupe is not None else f"{title or ''}|{message or ''}",
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
    payout_account_id = wp['payout_account_id'] or ''
    if PRODUCTION_MODE and payout_account_id.startswith('acct_sim_'):
        return False
    if stripe_configured():
        try:
            account = retrieve_live_connect_account(payout_account_id)
            return bool(account) and is_live_connect_account_ready(account)
        except Exception:
            return False
    if PRODUCTION_MODE:
        return False
    return bool(payout_account_id) and wp['payout_method'] not in ('pending_setup', None, '')


def employer_has_payment_setup(db, user_id):
    ep = db.execute("SELECT payment_method_id, stripe_customer_id FROM employer_profiles WHERE user_id = ?", [user_id]).fetchone()
    if not ep:
        return False
    if stripe_configured():
        return bool(ep['stripe_customer_id']) and bool(ep['payment_method_id'])
    if PRODUCTION_MODE:
        return False
    return bool(ep['payment_method_id']) and bool(ep['stripe_customer_id'])


def release_escrow_to_worker(db, order_id, milestone_id, amount, worker_id):
    """Release escrow hold, transfer to worker via Stripe or simulation."""
    # Employer pays the 1% platform margin on top of the listed amount.
    # Workers receive the listed amount unless Enzo explicitly changes the model.
    amount_cents = money_to_cents(amount, "escrow amount")
    fee = component_fee_cents(amount_cents, PLATFORM_FEE_BPS) / 100
    worker_payout = amount_cents / 100

    if stripe_configured() or PRODUCTION_MODE:
        wp = db.execute("SELECT payout_account_id FROM worker_profiles WHERE user_id = ?", [worker_id]).fetchone()
        payout_account_id = (wp['payout_account_id'] if wp else '') or ''
        if not payout_account_id or payout_account_id.startswith('acct_sim_'):
            raise ValueError("A live worker Stripe Connect payout account is required before release.")
        if not stripe_configured():
            raise ValueError("Stripe is not configured; live payout release is disabled in production.")
        idempotency_key = f"escrow-release:{order_id}:{milestone_id or 'full'}"
        try:
            account = retrieve_live_connect_account(payout_account_id)
            if not is_live_connect_account_ready(account):
                raise ValueError("Worker Stripe Connect account is not payout-ready.")
            transfer = stripe.Transfer.create(
                amount=amount_cents,
                currency="usd",
                destination=payout_account_id,
                metadata={"order_id": str(order_id), "milestone_id": str(milestone_id or "")},
                description=f"GoHireHumans escrow release order #{order_id}",
                idempotency_key=idempotency_key
            )
        except stripe.error.StripeError as e:
            raise ValueError(f"Stripe transfer failed: {str(e)}")
        record_payout_transfer(
            db, order_id, milestone_id, worker_id, worker_payout, 'escrow_release',
            idempotency_key, payout_account_id, transfer
        )

    # Only mark escrow/revenue after live transfer succeeds or non-production simulation is allowed.
    db.execute(
        "UPDATE escrow_holds SET status='released', released_at=datetime('now') WHERE order_id=? AND (milestone_id=? OR milestone_id IS NULL) AND status='held'",
        [order_id, milestone_id]
    )
    db.execute(
        "INSERT INTO platform_revenue (order_id, fee_amount, fee_type) VALUES (?,?,?)",
        [order_id, fee, 'service_fee']
    )
    return worker_payout, fee


def fund_escrow_stripe(db, employer_id, amount, order_id, milestone_id=None, description="Escrow hold", funding_identity=None):
    """
    Fund escrow. Returns (payment_intent_id, mode).
    - With Stripe: create PaymentIntent with capture_method=manual, then capture it.
      Platform charges employer's saved payment method.
    - Without Stripe: simulate.
    """
    ep = db.execute("SELECT stripe_customer_id, payment_method_id FROM employer_profiles WHERE user_id = ?", [employer_id]).fetchone()

    if stripe_configured():
        if not ep or not ep['stripe_customer_id'] or not ep['payment_method_id']:
            raise ValueError("A confirmed employer payment method is required before escrow can be funded.")
        try:
            charge = buyer_charge_breakdown_cents(amount)
            funding_identity = funding_identity or (f"milestone:{milestone_id}" if milestone_id is not None else None)
            if funding_identity is None:
                raise ValueError("A stable funding identity is required for live escrow funding.")
            idempotency_key = f"escrow-fund:{order_id}:{funding_identity}"
            pi = stripe.PaymentIntent.create(
                amount=charge["total_cents"],
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
                },
                idempotency_key=idempotency_key,
            )
            pi_id = pi.id
            mode = "live"
        except stripe.error.StripeError as e:
            raise ValueError(f"Payment failed: {str(e)}")
    else:
        if PRODUCTION_MODE:
            raise ValueError("Stripe is not configured; simulated escrow is disabled in production.")
        pi_id = fake_payment_intent_id()
        mode = "simulated"

    # Record escrow hold
    db.execute(
        "INSERT INTO escrow_holds (order_id, milestone_id, amount, status, stripe_payment_intent_id) VALUES (?,?,?,'held',?)",
        [order_id, milestone_id, amount, pi_id]
    )
    return pi_id, mode


# ─── Route Handler ─────────────────────────────────────────────────────────────

# Log DB location on first import (visible in Railway deploy logs)
print(f"[GoHireHumans] Volume dir /data exists: {os.path.isdir(_VOLUME_DIR)}", file=sys.stderr)
print(f"[GoHireHumans] RAILWAY_VOLUME_MOUNT_PATH: {os.environ.get('RAILWAY_VOLUME_MOUNT_PATH', '(not set)')}", file=sys.stderr)
print(f"[GoHireHumans] DB path will be resolved lazily on first request", file=sys.stderr)


def handle_request():
    # If server.py already set thread-local context, skip os.environ fallback.
    # Only populate from os.environ for direct CGI mode (not used in production).
    if not hasattr(_request_ctx, 'request_method'):
        _request_ctx.request_method = os.environ.get("REQUEST_METHOD", "GET")
        _request_ctx.path_info = os.environ.get("PATH_INFO", "")
        _request_ctx.query_string = os.environ.get("QUERY_STRING", "")
        _request_ctx.content_type = os.environ.get("CONTENT_TYPE", "")
        _request_ctx.content_length = os.environ.get("CONTENT_LENGTH", "0")
        _request_ctx.remote_addr = os.environ.get("REMOTE_ADDR", "127.0.0.1")
        _request_ctx.http_authorization = os.environ.get("HTTP_AUTHORIZATION", "")
        _request_ctx.http_x_api_key = os.environ.get("HTTP_X_API_KEY", "")
        _request_ctx.http_stripe_signature = os.environ.get("HTTP_STRIPE_SIGNATURE", "")
        _request_ctx.http_x_diagnostic_secret = os.environ.get("HTTP_X_DIAGNOSTIC_SECRET", "")
        _request_ctx.stdin_data = sys.stdin.read() if sys.stdin else ""

    try:
        init_db()
    except Exception as e:
        import traceback
        print(f"[GoHireHumans] DB init failed: {e} (path={_db_path_resolved})", file=sys.stderr)
        traceback.print_exc(file=sys.stderr)
        return error_response("Service temporarily unavailable. Please try again.", 500)

    auto_seed_if_empty()

    if not check_rate_limit():
        print("Status: 429")
        print("Content-Type: application/json")
        print()
        print(json.dumps({"error": "Rate limit exceeded", "retry_after": 60}))
        return

    db = get_db()
    try:
        _handle_routes(db)
    except Exception as e:
        print(f"[GoHireHumans] Unhandled error in route: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc(file=sys.stderr)
        error_response("Internal server error", 500)
    finally:
        db.close()


def _handle_routes(db):
    method = getattr(_request_ctx, 'request_method', 'GET')
    path = getattr(_request_ctx, 'path_info', '').rstrip("/")
    params = get_query_params()

    # Strip /api/v1 prefix so Stripe webhook URL and other prefixed paths work
    if path.startswith("/api/v1"):
        path = path[len("/api/v1"):] or "/"

    # ── Diagnostic endpoint (disabled by default; secret-gated when enabled) ──
    if path == "/diag/db" and method == "GET":
        if not diagnostic_endpoint_allowed():
            return error_response("Not found", 404)
        import stat as _stat
        volume_exists = os.path.isdir(_VOLUME_DIR)
        volume_contents = []
        if volume_exists:
            try:
                volume_contents = os.listdir(_VOLUME_DIR)
            except Exception as e:
                volume_contents = [f"error: {e}"]
        db_file_exists = os.path.isfile(_db_path_resolved) if _db_path_resolved and _db_path_resolved != ":memory:" else False
        db_size = 0
        if db_file_exists:
            try:
                db_size = os.path.getsize(_db_path_resolved)
            except Exception:
                pass
        db = get_db()
        user_count = db.execute("SELECT COUNT(*) as c FROM users").fetchone()['c']
        svc_count = db.execute("SELECT COUNT(*) as c FROM services").fetchone()['c']
        job_count = db.execute("SELECT COUNT(*) as c FROM jobs").fetchone()['c']
        oldest_user = db.execute("SELECT MIN(created_at) as d FROM users").fetchone()['d']
        newest_user = db.execute("SELECT MAX(created_at) as d FROM users").fetchone()['d']
        return json_response({
            "db_path": _db_path_resolved,
            "db_file_exists": db_file_exists,
            "db_size_bytes": db_size,
            "volume_dir_exists": volume_exists,
            "volume_contents": volume_contents,
            "user_count": user_count,
            "service_count": svc_count,
            "job_count": job_count,
            "oldest_user_created": oldest_user,
            "newest_user_created": newest_user,
            "env_DATABASE_PATH": os.environ.get("DATABASE_PATH", "(not set)"),
            "env_RAILWAY_VOLUME_MOUNT_PATH": os.environ.get("RAILWAY_VOLUME_MOUNT_PATH", "(not set)"),
            "cwd": os.getcwd(),
        })

    # ── Secret-gated SQLite backup endpoint ──────────────────────────────────
    if path == "/admin/backup" and method == "GET":
        if not backup_endpoint_allowed():
            return error_response("Not found", 404)
        db_path = _get_db_path()
        if db_path == ":memory:" or not os.path.isfile(db_path):
            return error_response("Database file not available", 503)
        created_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        fd, backup_path = tempfile.mkstemp(prefix="gohirehumans-backup-", suffix=".sqlite3")
        os.close(fd)
        try:
            source = sqlite3.connect(db_path)
            dest = sqlite3.connect(backup_path)
            try:
                source.backup(dest)
            finally:
                dest.close()
                source.close()
            with open(backup_path, "rb") as f:
                raw = f.read()
            integrity = sqlite3.connect(backup_path).execute("PRAGMA integrity_check").fetchone()[0]
            compressed = gzip.compress(raw, mtime=0)
            return json_response({
                "created_at": created_at,
                "format": "sqlite3+gzip+base64",
                "filename": f"gohirehumans-{created_at.replace(':', '').replace('-', '')}.sqlite3.gz",
                "database_size_bytes": len(raw),
                "compressed_size_bytes": len(compressed),
                "sha256_uncompressed": hashlib.sha256(raw).hexdigest(),
                "sha256_compressed": hashlib.sha256(compressed).hexdigest(),
                "integrity_check": integrity,
                "backup_b64_gzip": base64.b64encode(compressed).decode("ascii"),
            })
        finally:
            try:
                os.remove(backup_path)
            except OSError:
                pass

    # ── Public pricing info (no auth) ──────────────────────────────────────
    if path == "/pricing/info" and method == "GET":
        return json_response({
            "service_fee_rate": SERVICE_FEE_RATE,
            "processing_fee_rate": PROCESSING_FEE_RATE,
            "total_buyer_fee_rate": round(SERVICE_FEE_RATE + PROCESSING_FEE_RATE, 4),
            "fee_rounding": "round each positive component half-up to cents with a one-cent minimum",
            "platform_fee_basis_points": PLATFORM_FEE_BPS,
            "processing_fee_basis_points": PROCESSING_FEE_BPS,
            "description": "Employers pay Stripe processing plus a 1% GoHireHumans fee where configured. Workers receive the listed payout.",
            "fee_paid_by": "buyer",
            "escrow": False
        })

    # ── Public platform stats (no auth) ────────────────────────────────
    if path == "/platform/stats" and method == "GET":
        db = get_db()
        seeded_user_subquery = public_non_seeded_user_subquery()
        seeded_values = seeded_sample_email_values()
        services_count = db.execute(
            f"SELECT COUNT(*) as c FROM services WHERE status='active' AND worker_id NOT IN ({seeded_user_subquery})",
            seeded_values
        ).fetchone()['c']
        workers_count = db.execute(
            f"""SELECT COUNT(*) as c FROM worker_profiles wp
                JOIN users u ON wp.user_id = u.id
                WHERE {public_non_seeded_user_condition('u')}""",
            public_non_seeded_user_values()
        ).fetchone()['c']
        employers_count = db.execute(
            f"""SELECT COUNT(*) as c FROM employer_profiles ep
                JOIN users u ON ep.user_id = u.id
                WHERE {public_non_seeded_user_condition('u')}""",
            public_non_seeded_user_values()
        ).fetchone()['c']
        jobs_count = db.execute(
            f"SELECT COUNT(*) as c FROM jobs WHERE status='open' AND employer_id NOT IN ({seeded_user_subquery})",
            seeded_values
        ).fetchone()['c']
        completed_orders = db.execute(
            f"""SELECT COUNT(*) as c FROM orders
                WHERE status='completed'
                  AND worker_id NOT IN ({seeded_user_subquery})
                  AND employer_id NOT IN ({seeded_user_subquery})""",
            seeded_values + seeded_values
        ).fetchone()['c']
        total_users = db.execute(
            f"SELECT COUNT(*) as c FROM users WHERE is_banned=0 AND {public_non_seeded_user_condition('users')}",
            public_non_seeded_user_values()
        ).fetchone()['c']
        categories_count = db.execute(
            f"SELECT COUNT(DISTINCT category) as c FROM services WHERE status='active' AND worker_id NOT IN ({seeded_user_subquery})",
            seeded_values
        ).fetchone()['c']
        return json_response({
            "services_listed": services_count,
            "workers_registered": workers_count,
            "employers_registered": employers_count,
            "open_jobs": jobs_count,
            "completed_orders": completed_orders,
            "total_users": total_users,
            "categories": categories_count
        })

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
        ref_code = secrets.token_urlsafe(8)
        try:
            cursor = db.execute(
                "INSERT INTO users (email, password_hash, name, referral_code) VALUES (?,?,?,?)",
                [email, pw_hash, name, ref_code]
            )
        except sqlite3.OperationalError:
            # Fallback: referral_code column may not exist yet
            cursor = db.execute(
                "INSERT INTO users (email, password_hash, name) VALUES (?,?,?)",
                [email, pw_hash, name]
            )
            ref_code = None
        user_id = cursor.lastrowid

        # Track referral if ref_code was passed
        incoming_ref = body.get("ref_code", "").strip()
        if incoming_ref:
            try:
                referrer = db.execute("SELECT id FROM users WHERE referral_code = ?", [incoming_ref]).fetchone()
                if referrer and referrer['id'] != user_id:
                    db.execute("UPDATE users SET referred_by = ? WHERE id = ?", [referrer['id'], user_id])
                    db.execute("INSERT INTO referrals (referrer_id, referred_id) VALUES (?,?)",
                               [referrer['id'], user_id])
            except sqlite3.OperationalError:
                pass  # referral columns may not exist yet

        token = generate_session_token()
        expires = (datetime.now(timezone.utc) + timedelta(days=30)).isoformat()
        db.execute("INSERT INTO sessions (user_id, token, expires_at) VALUES (?,?,?)", [user_id, token, expires])
        audit(db, user_id, "register", "user", user_id)
        db.commit()

        # Send welcome email (non-blocking — silently fails if RESEND_API_KEY not set)
        try:
            send_welcome_email(email, name)
        except Exception:
            pass

        return json_response({
            "id": user_id,
            "email": email,
            "name": name,
            "is_admin": 0,
            "token": token,
            "referral_code": ref_code,
            "worker_profile": None,
            "employer_profile": None
        }, 201)

    elif path == "/auth/login" and method == "POST":
        body = get_body() or {}
        email = body.get("email", "").strip().lower()
        password = body.get("password", "")

        user = db.execute("SELECT * FROM users WHERE email = ?", [email]).fetchone()
        valid_credentials = bool(user and verify_password(password, user['password_hash']))

        if not valid_credentials:
            if not login_attempt_allowed(email):
                audit(db, user['id'] if user else None, "login_rate_limited", "user", user['id'] if user else None, {"email": email, "ip": getattr(_request_ctx, 'remote_addr', 'unknown'), "admin": bool(user['is_admin']) if user else False})
                db.commit()
                return error_response("Too many failed login attempts. Try again later.", 429)
            record_login_failure(email)
            audit(db, user['id'] if user else None, "login_failed", "user", user['id'] if user else None, {"email": email, "ip": getattr(_request_ctx, 'remote_addr', 'unknown'), "admin": bool(user['is_admin']) if user else False})
            db.commit()
            return error_response("Invalid credentials", 401)
        if is_seeded_sample_email(email):
            record_login_failure(email)
            audit(db, user['id'], "login_blocked_sample", "user", user['id'])
            db.commit()
            return error_response("Sample account login disabled", 403)
        if user['is_banned']:
            record_login_failure(email)
            audit(db, user['id'], "login_blocked_banned", "user", user['id'])
            db.commit()
            return error_response("Account banned", 403)
        if user['is_suspended']:
            record_login_failure(email)
            audit(db, user['id'], "login_blocked_suspended", "user", user['id'])
            db.commit()
            return error_response("Account suspended", 403)

        clear_login_failures(email)
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

    elif path == "/auth/google" and method == "POST":
        # Google One Tap / Sign-In with Google — verify ID token, create or login user
        body = get_body()
        id_token = body.get("credential", "").strip()
        if not id_token:
            return error_response("Google credential required")

        # Verify the token with Google's tokeninfo endpoint
        try:
            verify_url = f"https://oauth2.googleapis.com/tokeninfo?id_token={urllib.parse.quote(id_token)}"
            req = urllib.request.Request(verify_url)
            with urllib.request.urlopen(req, timeout=10) as resp:
                payload = json.loads(resp.read().decode())
        except Exception as e:
            return error_response(f"Google token verification failed: {str(e)}", 401)

        google_client_id = os.environ.get("GOOGLE_CLIENT_ID", "").strip()
        if not google_client_id:
            return error_response("Google sign-in is not configured", 503)
        if payload.get("aud") != google_client_id:
            return error_response("Google token audience mismatch", 401)
        if payload.get("iss") not in ("accounts.google.com", "https://accounts.google.com"):
            return error_response("Google token issuer mismatch", 401)

        email = payload.get("email", "").strip().lower()
        name = payload.get("name", "").strip()
        google_sub = payload.get("sub", "")
        email_verified = payload.get("email_verified", "false")

        if not email or email_verified != "true":
            return error_response("Google account email not verified", 401)

        # Check if user exists
        existing = db.execute("SELECT * FROM users WHERE email = ?", [email]).fetchone()

        if existing:
            if existing['is_banned']:
                return error_response("Account banned", 403)
            if existing['is_suspended']:
                return error_response("Account suspended", 403)
            user_id = existing['id']
            is_new_google_user = False
            # Update google_sub if not set
            db.execute("UPDATE users SET google_sub = ? WHERE id = ? AND (google_sub IS NULL OR google_sub = '')",
                       [google_sub, user_id])
        else:
            # Create new user — no password needed for Google auth
            cursor = db.execute(
                "INSERT INTO users (email, password_hash, name, google_sub) VALUES (?,?,?,?)",
                [email, "", name, google_sub]  # empty password_hash = social-only account
            )
            user_id = cursor.lastrowid
            audit(db, user_id, "register_google", "user", user_id)
            is_new_google_user = True

        token = generate_session_token()
        expires = (datetime.now(timezone.utc) + timedelta(days=30)).isoformat()
        db.execute("INSERT INTO sessions (user_id, token, expires_at) VALUES (?,?,?)",
                   [user_id, token, expires])
        audit(db, user_id, "login_google", "user", user_id)
        db.commit()

        # Send welcome email to new Google users (non-blocking)
        if is_new_google_user:
            try:
                send_welcome_email(email, name)
            except Exception:
                pass

        user = db.execute("SELECT * FROM users WHERE id = ?", [user_id]).fetchone()
        user_data = row_to_dict(user)
        del user_data['password_hash']
        user_data['token'] = token
        wp = db.execute("SELECT * FROM worker_profiles WHERE user_id = ?", [user_id]).fetchone()
        ep = db.execute("SELECT * FROM employer_profiles WHERE user_id = ?", [user_id]).fetchone()
        user_data['worker_profile'] = row_to_dict(wp)
        user_data['employer_profile'] = row_to_dict(ep)

        return json_response(user_data, 200 if existing else 201)

    elif path == "/auth/logout" and method == "POST":
        auth_header = getattr(_request_ctx, 'http_authorization', '')
        token = auth_header[7:].strip() if auth_header.startswith("Bearer ") else None
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
    # REFERRAL PROGRAM
    # ═══════════════════════════════════════════════════════════════════════════

    elif path == "/referral/code" and method == "GET":
        user = authenticate(db)
        if not user:
            return error_response("Unauthorized", 401)
        code = user.get('referral_code') or user['referral_code'] if 'referral_code' in dict(user).keys() else None
        if not code:
            code = secrets.token_urlsafe(8)
            db.execute("UPDATE users SET referral_code = ? WHERE id = ?", [code, user['id']])
            db.commit()
        referral_url = f"https://www.gohirehumans.com/?ref={code}"
        # Count referrals
        stats = db.execute("""
            SELECT
                COUNT(*) as total,
                SUM(CASE WHEN status = 'converted' THEN 1 ELSE 0 END) as converted
            FROM referrals WHERE referrer_id = ?
        """, [user['id']]).fetchone()
        return json_response({
            "code": code,
            "url": referral_url,
            "total_referrals": stats['total'] if stats else 0,
            "converted_referrals": stats['converted'] if stats else 0
        })

    elif path == "/referral/track" and method == "POST":
        # Called during registration to track a referral
        body = get_body()
        ref_code = body.get("ref_code", "").strip()
        new_user_id = body.get("user_id")
        if not ref_code or not new_user_id:
            return error_response("ref_code and user_id required")
        referrer = db.execute("SELECT id FROM users WHERE referral_code = ?", [ref_code]).fetchone()
        if not referrer or referrer['id'] == new_user_id:
            return json_response({"ok": False, "reason": "invalid_code"})
        # Prevent duplicates
        existing = db.execute("SELECT id FROM referrals WHERE referred_id = ?", [new_user_id]).fetchone()
        if existing:
            return json_response({"ok": False, "reason": "already_tracked"})
        db.execute("UPDATE users SET referred_by = ? WHERE id = ?", [referrer['id'], new_user_id])
        db.execute("INSERT INTO referrals (referrer_id, referred_id) VALUES (?,?)",
                   [referrer['id'], new_user_id])
        db.commit()
        return json_response({"ok": True})

    elif path == "/referral/leaderboard" and method == "GET":
        rows = db.execute("""
            SELECT u.name, u.referral_code, COUNT(r.id) as count
            FROM users u JOIN referrals r ON u.id = r.referrer_id
            GROUP BY u.id ORDER BY count DESC LIMIT 10
        """).fetchall()
        return json_response({"leaderboard": [dict(r) for r in rows]})

    # ═══════════════════════════════════════════════════════════════════════════
    # CATEGORIES
    # ═══════════════════════════════════════════════════════════════════════════

    elif path == "/categories" and method == "GET":
        return json_response({"categories": VALID_CATEGORIES})

    # ═══════════════════════════════════════════════════════════════════════════
    # SERVICES (Public browse, auth for mutations)
    # ═══════════════════════════════════════════════════════════════════════════

    elif path == "/services" and method == "GET":
        try:
            page = parse_int_param(params, "page", 1, min_value=1)
            per_page = parse_int_param(params, "per_page", 20, min_value=1, max_value=100)
        except ValueError as e:
            return error_response(str(e), 400)
        offset = (page - 1) * per_page
        category = params.get("category")
        search = params.get("search", "").strip()
        min_price = params.get("min_price")
        max_price = params.get("max_price")
        pricing_type = params.get("pricing_type")
        provider_type = params.get("provider_type")

        conditions = ["s.status = 'active'", f"s.worker_id NOT IN ({public_non_seeded_user_subquery()})"]
        values = seeded_sample_email_values()

        if category:
            conditions.append("s.category = ?")
            values.append(category)
        if pricing_type:
            conditions.append("s.pricing_type = ?")
            values.append(pricing_type)
        if provider_type:
            conditions.append("s.provider_type = ?")
            values.append(provider_type)
        try:
            min_price_val = parse_float_param(params, "min_price", min_value=0)
            max_price_val = parse_float_param(params, "max_price", min_value=0)
        except ValueError as e:
            return error_response(str(e), 400)
        if min_price_val is not None:
            conditions.append("(s.price >= ? OR s.hourly_rate >= ?)")
            values.extend([min_price_val, min_price_val])
        if max_price_val is not None:
            conditions.append("(s.price <= ? OR s.hourly_rate <= ?)")
            values.extend([max_price_val, max_price_val])
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
               WHERE s.id = ? AND s.status != 'removed'
                 AND s.worker_id NOT IN (""" + public_non_seeded_user_subquery() + ")""",
            [service_id] + seeded_sample_email_values()
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
        service_text = " ".join([
            str(body.get('title') or ''),
            str(body.get('description') or ''),
            str(body.get('includes') or ''),
            " ".join(body.get('tags') or []) if isinstance(body.get('tags'), list) else str(body.get('tags') or ''),
        ])
        safe, msg = check_payment_circumvention(service_text)
        if not safe:
            return error_response(f"Service rejected: {msg}", 422)

        # Ensure worker profile exists (payout can be set up later)
        ensure_worker_profile(db, user['id'])

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

        provider_type = body.get("provider_type", "human")
        if provider_type not in ('human', 'ai'):
            return error_response("provider_type must be 'human' or 'ai'")

        fulfillment_type = body.get("fulfillment_type", "manual")
        if fulfillment_type not in ('manual', 'api'):
            return error_response("fulfillment_type must be 'manual' or 'api'")

        api_endpoint = body.get("api_endpoint", "")
        ai_model = body.get("ai_model", "")
        avg_response_time = body.get("avg_response_time", "")

        if provider_type == 'ai' and fulfillment_type == 'api' and not api_endpoint:
            return error_response("api_endpoint required for API-fulfilled AI services")

        cursor = db.execute(
            """INSERT INTO services
               (worker_id, title, description, category, pricing_type, price, hourly_rate,
                delivery_time_days, includes, tags, images, status,
                provider_type, fulfillment_type, api_endpoint, ai_model, avg_response_time)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,'active',?,?,?,?,?)""",
            [user['id'], body['title'], body['description'], body['category'],
             pricing_type, price, hourly_rate,
             body.get("delivery_time_days"),
             body.get("includes", ""),
             json.dumps(tags) if isinstance(tags, list) else tags,
             json.dumps(images) if isinstance(images, list) else images,
             provider_type, fulfillment_type, api_endpoint, ai_model, avg_response_time]
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
        merged_service_text = " ".join([
            str(body.get('title') if 'title' in body else svc['title'] or ''),
            str(body.get('description') if 'description' in body else svc['description'] or ''),
            str(body.get('includes') if 'includes' in body else svc['includes'] or ''),
            " ".join(body.get('tags') or []) if isinstance(body.get('tags'), list) else str(body.get('tags') if 'tags' in body else svc['tags'] or ''),
        ])
        safe, msg = check_payment_circumvention(merged_service_text)
        if not safe:
            return error_response(f"Service update rejected: {msg}", 422)

        updates = []
        vals = []
        for field in ['title', 'description', 'category', 'pricing_type', 'price', 'hourly_rate',
                      'delivery_time_days', 'includes', 'status',
                      'provider_type', 'fulfillment_type', 'api_endpoint', 'ai_model', 'avg_response_time']:
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
        try:
            page = parse_int_param(params, "page", 1, min_value=1)
            per_page = parse_int_param(params, "per_page", 20, min_value=1, max_value=100)
        except ValueError as e:
            return error_response(str(e), 400)
        offset = (page - 1) * per_page
        category = params.get("category")
        search = params.get("search", "").strip()
        location_type = params.get("location_type")
        budget_type = params.get("budget_type")
        min_budget = params.get("min_budget")
        max_budget = params.get("max_budget")
        status_filter = params.get("status", "open")

        conditions = ["j.status = ?", f"j.employer_id NOT IN ({public_non_seeded_user_subquery()})"]
        values = [status_filter] + seeded_sample_email_values()

        if category:
            conditions.append("j.category = ?")
            values.append(category)
        if location_type:
            conditions.append("j.location_type = ?")
            values.append(location_type)
        if budget_type:
            conditions.append("j.budget_type = ?")
            values.append(budget_type)
        try:
            min_budget_val = parse_float_param(params, "min_budget", min_value=0)
            max_budget_val = parse_float_param(params, "max_budget", min_value=0)
        except ValueError as e:
            return error_response(str(e), 400)
        if min_budget_val is not None:
            conditions.append("j.budget_amount >= ?")
            values.append(min_budget_val)
        if max_budget_val is not None:
            conditions.append("j.budget_amount <= ?")
            values.append(max_budget_val)
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
               WHERE j.id = ?
                 AND j.employer_id NOT IN (""" + public_non_seeded_user_subquery() + ")""",
            [job_id] + seeded_sample_email_values()
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

        try:
            budget_cents = money_to_cents(body['budget_amount'], "budget_amount")
        except ValueError as e:
            return error_response(str(e), 400)
        if budget_cents <= 0 or budget_cents > 100000000:
            return error_response("budget_amount must be positive and <= 1,000,000")
        budget = budget_cents / 100

        job_text_parts = [
            body.get('title', ''),
            body.get('description', ''),
            body.get('location_detail', ''),
        ]
        raw_required_skills = body.get('required_skills', [])
        if isinstance(raw_required_skills, list):
            job_text_parts.extend(str(skill) for skill in raw_required_skills)
        else:
            job_text_parts.append(str(raw_required_skills or ''))
        job_safety_text = " ".join(str(part or '') for part in job_text_parts)
        safe, msg = check_content_safety(job_safety_text)
        if not safe:
            return error_response(f"Job rejected: {msg}", 422)
        safe, msg = check_payment_circumvention(job_safety_text)
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

        # ── Job-match notifications: notify workers in the same category ──
        try:
            matching_workers = db.execute(
                """SELECT DISTINCT s.worker_id, u.name
                   FROM services s JOIN users u ON u.id = s.worker_id
                   WHERE s.category = ? AND s.status = 'active' AND s.worker_id != ?
                   LIMIT 20""",
                [body['category'], user['id']]
            ).fetchall()
            for w in matching_workers:
                push_notification(
                    db, w['worker_id'], 'job_match',
                    f"New job matches your skills: {body['title'][:60]}",
                    f"A new {body['category'].replace('_',' ')} job was just posted. Budget: ${budget:,.0f}",
                    f"#/jobs/{job_id}",
                    email=True,
                    email_dedupe=f"job_match:{job_id}"
                )
        except Exception:
            pass  # Don't fail job creation if notifications error

        db.commit()
        flush_transactional_notification_emails(db)
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
        update_text_parts = [
            body.get('title', job['title']),
            body.get('description', job['description']),
            body.get('location_detail', job['location_detail'] or ''),
        ]
        update_skills = body.get('required_skills', job['required_skills'] or '')
        if isinstance(update_skills, list):
            update_text_parts.extend(str(skill) for skill in update_skills)
        else:
            update_text_parts.append(str(update_skills or ''))
        update_safety_text = " ".join(str(part or '') for part in update_text_parts)
        safe, msg = check_content_safety(update_safety_text)
        if not safe:
            return error_response(f"Job update rejected: {msg}", 422)
        safe, msg = check_payment_circumvention(update_safety_text)
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
            try:
                budget_cents = money_to_cents(body['budget_amount'], "budget_amount")
            except ValueError as e:
                return error_response(str(e), 400)
            if budget_cents <= 0 or budget_cents > 100000000:
                return error_response("budget_amount must be positive and <= 1,000,000")
            updates.append("budget_amount = ?")
            vals.append(budget_cents / 100)
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
        if job['status'] in ('hired', 'in_progress', 'completed'):
            return error_response("Cannot cancel a hired, in-progress, or completed job", 409)
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
        cover_message = body.get("cover_message", "")
        portfolio_url = body.get("portfolio_url", "")
        application_safety_text = " ".join([str(cover_message or ""), str(portfolio_url or "")])
        safe, msg = check_content_safety(application_safety_text)
        if not safe:
            return error_response(f"Application rejected: {msg}", 422)
        safe, msg = check_payment_circumvention(application_safety_text)
        if not safe:
            return error_response(f"Application rejected: {msg}", 422)
        if not is_safe_external_url(portfolio_url):
            return error_response("Application rejected: portfolio_url must be a valid http(s) URL", 422)
        existing = db.execute(
            "SELECT id FROM applications WHERE job_id = ? AND worker_id = ?",
            [job_id, user['id']]
        ).fetchone()
        if existing:
            return error_response("You have already applied to this job", 409)

        cursor = db.execute(
            "INSERT INTO applications (job_id, worker_id, cover_message, portfolio_url) VALUES (?,?,?,?)",
            [job_id, user['id'], cover_message, portfolio_url]
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
            f"/jobs/{job_id}/applications",
            email=True,
            email_dedupe=f"application:{app_id}")

        audit(db, user['id'], "apply_job", "application", app_id)
        db.commit()
        flush_transactional_notification_emails(db)
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
        if not JOB_HIRING_ENABLED:
            return error_response("New job hiring is temporarily paused while payment safeguards are finalized", 503)

        body = get_body()
        application_id = body.get("application_id")
        if application_id is None:
            return error_response("application_id required")
        try:
            application_id = int(application_id)
        except (TypeError, ValueError):
            return error_response("application_id must be an integer")

        # Verify application exists
        app = db.execute(
            "SELECT id, worker_id, status FROM applications WHERE id = ? AND job_id = ? AND status IN ('pending','shortlisted')",
            [application_id, job_id]
        ).fetchone()
        if not app:
            return error_response("Eligible application not found for this job", 404)

        # Check employer has payment setup
        ensure_employer_profile(db, user['id'])
        if not employer_has_payment_setup(db, user['id']):
            return error_response("You must set up a payment method before hiring. Use /payments/setup-employer.", 402)

        worker_id = int(app['worker_id'])
        try:
            total_cents = money_to_cents(job['budget_amount'], "job budget")
        except ValueError as e:
            return error_response(str(e), 400)
        if total_cents <= 0:
            return error_response("job budget must be greater than zero", 400)
        total_amount = total_cents / 100

        # Create order
        try:
            cursor = db.execute(
                """INSERT INTO orders (type, job_id, worker_id, employer_id, status, total_amount)
                   VALUES ('job_hire', ?, ?, ?, 'in_progress', ?)""",
                [job_id, worker_id, user['id'], total_amount]
            )
        except sqlite3.IntegrityError:
            db.rollback()
            return error_response("This job already has a hire order", 409)
        order_id = cursor.lastrowid

        if job['budget_type'] == 'fixed':
            # Fixed-price: set up milestones
            milestones_input = body.get("milestones", [])
            if not milestones_input:
                # Default: 1 milestone = full amount
                milestones_input = [{"title": "Project completion", "description": "Full project deliverable", "amount": total_amount}]

            # Validate milestone amounts sum to total
            try:
                milestone_cents = [
                    money_to_cents(m.get("amount"), f"milestone {index} amount")
                    for index, m in enumerate(milestones_input, 1)
                ]
            except (AttributeError, ValueError) as e:
                db.rollback()
                return error_response(str(e), 400)
            if any(amount <= 0 for amount in milestone_cents):
                db.rollback()
                return error_response("Milestone amounts must be greater than zero", 400)
            ms_total_cents = sum(milestone_cents)
            if ms_total_cents != total_cents:
                db.rollback()
                return error_response("Milestone amounts must exactly equal the job budget in whole cents", 400)

            milestone_ids = []
            for seq, m in enumerate(milestones_input, 1):
                mc = db.execute(
                    "INSERT INTO milestones (order_id, title, description, amount, sequence, status) VALUES (?,?,?,?,?,'pending')",
                    [order_id, m.get("title", f"Milestone {seq}"), m.get("description", ""), milestone_cents[seq - 1] / 100, seq]
                )
                milestone_ids.append(mc.lastrowid)

            # Fund first milestone escrow immediately
            first_ms_id = milestone_ids[0]
            first_ms_amount = milestone_cents[0] / 100
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
            # Hourly: use the posted rate and fund the employer-selected first-week cap.
            # The client must not be able to rewrite the worker's posted rate at hire time.
            hourly_rate = float(job['budget_amount'] or 0)
            try:
                weekly_cap = float(body.get("weekly_hour_cap", 40))
            except (TypeError, ValueError):
                db.rollback()
                return error_response("Weekly hour cap must be a number between 1 and 168", 400)
            if not math.isfinite(hourly_rate) or hourly_rate <= 0:
                db.rollback()
                return error_response("Hourly job rate must be greater than zero", 400)
            if not math.isfinite(weekly_cap) or weekly_cap < 1 or weekly_cap > 168:
                db.rollback()
                return error_response("Weekly hour cap must be between 1 and 168", 400)
            if not weekly_cap.is_integer():
                db.rollback()
                return error_response("Weekly hour cap must be a whole number between 1 and 168", 400)
            weekly_cap = int(weekly_cap)
            week_escrow_cents = rounded_product_cents(hourly_rate, weekly_cap, "hourly job rate")
            week_escrow = week_escrow_cents / 100

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
                    f"Hourly contract #{contract_id} first week escrow",
                    funding_identity=f"hourly:{contract_id}:week:{week_start}",
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
            "UPDATE applications SET status='accepted' WHERE id=?",
            [application_id]
        )
        db.execute(
            "UPDATE applications SET status='rejected' WHERE job_id=? AND worker_id!=?",
            [job_id, worker_id]
        )

        # Notify worker
        push_notification(db, worker_id, "job_hired",
            f"You've been hired!",
            f"You've been hired for: {job['title']}",
            f"/orders/{order_id}",
            email=True,
            email_dedupe=f"job_hired:{job_id}:{worker_id}:{order_id}")

        audit(db, user['id'], "hire_worker", "order", order_id, {"job_id": job_id, "worker_id": worker_id})
        db.commit()
        flush_transactional_notification_emails(db)

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
            f"/orders/{order_id}",
            email=True,
            email_dedupe=f"service_order:{order_id}")

        audit(db, user['id'], "order_service", "order", order_id)
        db.commit()
        flush_transactional_notification_emails(db)

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
                j.title as job_title,
                CASE WHEN hc.id IS NOT NULL OR j.budget_type='hourly' THEN 'hourly' ELSE 'fixed' END as contract_type,
                COALESCE(hc.hourly_rate, CASE WHEN j.budget_type='hourly' THEN j.budget_amount END) as hourly_rate,
                hc.weekly_hour_cap,
                hc.current_week_escrow_amount
                FROM orders o
                JOIN users wu ON o.worker_id = wu.id
                JOIN users eu ON o.employer_id = eu.id
                LEFT JOIN services s ON o.service_id = s.id
                LEFT JOIN jobs j ON o.job_id = j.id
                LEFT JOIN hourly_contracts hc ON hc.order_id = o.id
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
        funding_summary = {
            "base_cents": 0,
            "platform_fee_cents": 0,
            "processing_fee_cents": 0,
            "charged_total_cents": 0,
        }
        for hold in escrow:
            charge = buyer_charge_breakdown_cents(hold['amount'])
            funding_summary["base_cents"] += charge["base_cents"]
            funding_summary["platform_fee_cents"] += charge["platform_fee_cents"]
            funding_summary["processing_fee_cents"] += charge["processing_fee_cents"]
            funding_summary["charged_total_cents"] += charge["total_cents"]
        result['funding_summary'] = funding_summary
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
        try:
            notes = validated_order_notes(body.get("notes"))
        except ValueError as e:
            return error_response(str(e), 400)

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
            f"/orders/{order_id}",
            email=True,
            email_dedupe=f"order_submitted:{order_id}:{time.time_ns()}")

        audit(db, user['id'], "submit_order", "order", order_id)
        db.commit()
        flush_transactional_notification_emails(db)
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
        try:
            worker_payout, fee = release_escrow_to_worker(db, order_id, ms_id, ms_amount, order['worker_id'])
        except ValueError as e:
            db.rollback()
            return error_response(str(e), 502)

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
            if order['job_id']:
                db.execute(
                    "UPDATE jobs SET status='completed', updated_at=datetime('now') WHERE id=?",
                    [order['job_id']]
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
                f"Order #{order_id} is complete. ${worker_payout:.2f} earned. Platform margin is paid by the employer.",
                f"/orders/{order_id}",
                email=True,
                email_dedupe=f"order_completed:{order_id}:approve")
            push_notification(db, order['employer_id'], "order_completed",
                "Order completed",
                f"Order #{order_id} has been completed successfully.",
                f"/orders/{order_id}")
            # Send review request notification to employer
            push_notification(db, order['employer_id'], "review_request",
                "How was your experience?",
                f"Order #{order_id} is complete! Leave a review to help others find great professionals.",
                f"/orders/{order_id}#review")
            # Send review request email if Resend is configured
            try:
                employer = db.execute("SELECT email, name FROM users WHERE id = ?", [order['employer_id']]).fetchone()
                worker = db.execute("SELECT u.name FROM users u JOIN worker_profiles wp ON u.id = wp.user_id WHERE u.id = ?", [order['worker_id']]).fetchone()
                if employer and worker:
                    review_html = f"""
                    <div style="font-family:'Inter',system-ui,sans-serif;max-width:560px;margin:0 auto;color:#1a1816">
                      <div style="background:#0d7377;padding:24px 32px;border-radius:8px 8px 0 0">
                        <h1 style="color:white;font-size:20px;margin:0;font-weight:700">How was your experience?</h1>
                      </div>
                      <div style="background:#faf9f6;padding:32px;border:1px solid #dddbd6;border-top:none;border-radius:0 0 8px 8px">
                        <p style="font-size:16px;line-height:1.6;margin-bottom:16px">Hi {(employer['name'] or 'there').split()[0]},</p>
                        <p style="font-size:15px;line-height:1.6;margin-bottom:16px">Your order with <strong>{worker['name']}</strong> is complete! We'd love to hear how it went.</p>
                        <p style="font-size:15px;line-height:1.6;margin-bottom:24px">Your review helps other buyers find the best professionals and helps workers build their reputation.</p>
                        <div style="text-align:center;margin-bottom:24px">
                          <a href="https://www.gohirehumans.com/#/orders/{order_id}" style="display:inline-block;background:#0d7377;color:white;padding:12px 28px;border-radius:6px;text-decoration:none;font-weight:600;font-size:15px">Leave a Review →</a>
                        </div>
                        <hr style="border:none;border-top:1px solid #dddbd6;margin:24px 0 16px">
                        <p style="font-size:11px;color:#a8a6a0;text-align:center">&copy; 2026 GoHireHumans · <a href="https://www.gohirehumans.com" style="color:#a8a6a0">gohirehumans.com</a></p>
                      </div>
                    </div>
                    """
                    send_email(employer['email'], f"How was your experience with {worker['name']}?", review_html)
            except Exception:
                pass

        audit(db, user['id'], "approve_order", "order", order_id)
        db.commit()
        flush_transactional_notification_emails(db)
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
        try:
            notes = validated_order_notes(body.get("notes"))
        except ValueError as e:
            return error_response(str(e), 400)

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
            f"/orders/{order_id}",
            email=True,
            email_message=f"A revision has been requested on order #{order_id}. Open GoHireHumans to review the details.",
            email_dedupe=f"revision_requested:{order_id}:{time.time_ns()}")

        audit(db, user['id'], "request_revision", "order", order_id)
        db.commit()
        flush_transactional_notification_emails(db)
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
        if order['type'] == 'job_hire':
            return error_response("Job hires must be completed through submitted milestone approval", 409)
        if order['status'] not in ('submitted', 'in_progress'):
            return error_response("Order must be submitted or in_progress to complete", 409)

        held = db.execute("SELECT * FROM escrow_holds WHERE order_id=? AND status='held' ORDER BY id", [order_id]).fetchall()
        if not held:
            return error_response("No held payment found to release", 409)
        worker_payout_total = 0
        platform_fee_total = 0
        for hold in held:
            try:
                worker_payout, fee = release_escrow_to_worker(db, order_id, hold['milestone_id'], float(hold['amount']), order['worker_id'])
            except ValueError as e:
                db.rollback()
                return error_response(str(e), 502)
            worker_payout_total += worker_payout
            platform_fee_total += fee

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
            f"/orders/{order_id}",
            email=True,
            email_dedupe=f"order_completed:{order_id}:complete")

        audit(db, user['id'], "complete_order", "order", order_id)
        db.commit()
        flush_transactional_notification_emails(db)
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
        if not HOURLY_SETTLEMENT_ENABLED:
            return error_response(
                "Hourly settlement is temporarily paused while processor reconciliation safeguards are finalized",
                503,
            )

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

        total_hours = sum((Decimal(str(e['hours'])) for e in entries), Decimal("0"))
        total_pay_cents = rounded_product_cents(hc['hourly_rate'], total_hours, "hourly contract rate")
        worker_pay = total_pay_cents / 100
        fee = component_fee_cents(total_pay_cents, PLATFORM_FEE_BPS) / 100
        total_hours_value = float(total_hours)

        if stripe_configured() or PRODUCTION_MODE:
            wp = db.execute("SELECT payout_account_id FROM worker_profiles WHERE user_id=?", [order['worker_id']]).fetchone()
            payout_account_id = (wp['payout_account_id'] if wp else '') or ''
            if not payout_account_id or payout_account_id.startswith('acct_sim_'):
                return error_response("A live worker Stripe Connect payout account is required before approving paid hours.", 409)
            if not stripe_configured():
                return error_response("Stripe is not configured; live hourly payout release is disabled in production.", 503)
            idempotency_key = f"hourly-release:{order_id}:{week_of}"
            try:
                account = retrieve_live_connect_account(payout_account_id)
                if not is_live_connect_account_ready(account):
                    db.rollback()
                    return error_response("Worker Stripe Connect account is not payout-ready.", 409)
                transfer = stripe.Transfer.create(
                    amount=total_pay_cents,
                    currency="usd",
                    destination=payout_account_id,
                    metadata={"order_id": str(order_id), "week_of": week_of},
                    idempotency_key=idempotency_key
                )
            except stripe.error.StripeError as e:
                db.rollback()
                return error_response(f"Stripe transfer failed: {str(e)}", 502)
            record_payout_transfer(
                db, order_id, None, order['worker_id'], worker_pay, 'hourly_release',
                idempotency_key, payout_account_id, transfer
            )

        # Mark entries approved only after live transfer succeeds or non-production simulation is allowed.
        db.execute(
            "UPDATE time_entries SET status='approved' WHERE contract_id=? AND week_of=? AND status='pending'",
            [hc['id'], week_of]
        )

        # Release escrow for these hours after transfer/fail-closed checks above.
        db.execute(
            "UPDATE escrow_holds SET status='released', released_at=datetime('now') WHERE order_id=? AND status='held'",
            [order_id]
        )
        db.execute(
            "INSERT INTO platform_revenue (order_id, fee_amount, fee_type) VALUES (?,?,'hourly_service_fee')",
            [order_id, fee]
        )

        # Refund unused escrow and fund next week
        escrow_held_cents = money_to_cents(hc['current_week_escrow_amount'] or 0, "hourly escrow")
        unused = max(0, escrow_held_cents - total_pay_cents) / 100

        # Fund next week's escrow
        if hc['status'] == 'active':
            next_week_escrow = rounded_product_cents(
                hc['hourly_rate'], hc['weekly_hour_cap'], "hourly contract rate"
            ) / 100
            try:
                pi_id, mode = fund_escrow_stripe(
                    db, order['employer_id'], next_week_escrow, order_id, None,
                    "Hourly contract next week escrow",
                    funding_identity=f"hourly:{hc['id']}:renewal-after:{week_of}",
                )
                db.execute(
                    "UPDATE hourly_contracts SET current_week_escrow_amount=?, current_week_escrow_payment_id=? WHERE id=?",
                    [next_week_escrow, pi_id, hc['id']]
                )
            except ValueError:
                pass  # If can't fund next week, contract continues but without new escrow

        push_notification(db, order['worker_id'], "hours_approved",
            f"Hours approved — payment released!",
            f"{total_hours_value:g}h approved for week of {week_of}. ${worker_pay:.2f} released.",
            f"/orders/{order_id}")

        audit(db, user['id'], "approve_hours", "hourly_contract", hc['id'], {"week_of": week_of, "hours": total_hours_value})
        db.commit()
        return json_response({
            "ok": True,
            "hours_approved": total_hours_value,
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
        if not HOURLY_SETTLEMENT_ENABLED:
            return error_response(
                "Hourly settlement is temporarily paused while processor reconciliation safeguards are finalized",
                503,
            )

        body = get_body()

        db.execute("UPDATE hourly_contracts SET status='ended' WHERE id=?", [hc['id']])
        db.execute(
            "UPDATE orders SET status='completed', completed_at=datetime('now'), updated_at=datetime('now') WHERE id=?",
            [order_id]
        )
        synchronize_job_terminal_state(db, order)

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
            if PRODUCTION_MODE:
                return error_response("Stripe is not configured; simulated employer payment setup is disabled in production.", 503)
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
            if PRODUCTION_MODE:
                return error_response("Stripe is not configured; simulated worker payout setup is disabled in production.", 503)
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
                        acct = retrieve_live_connect_account(wp['payout_account_id'])
                        connected = bool(acct) and is_live_connect_account_ready(acct)
                        worker_status = {
                            "connected": connected,
                            "payouts_enabled": bool(stripe_attr(acct, 'payouts_enabled')),
                            "charges_enabled": bool(stripe_attr(acct, 'charges_enabled')),
                            "details_submitted": bool(stripe_attr(acct, 'details_submitted')),
                            "account_id": wp['payout_account_id'],
                            "mode": "live"
                        }
                    except stripe.error.StripeError:
                        worker_status = {"connected": False, "account_id": wp['payout_account_id'], "mode": "live"}
                else:
                    if PRODUCTION_MODE:
                        worker_status = {"connected": False, "account_id": None, "mode": "disabled", "message": "Simulated worker payout is disabled in production."}
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
            "employer_payment_status": employer_status,
            "worker_ready": bool(worker_status and worker_status.get("connected")),
            "employer_ready": bool(employer_status and employer_status.get("has_payment_method"))
        })

    elif path in ("/payments/prepare-order-payment", "/payments/fund-escrow") and method == "POST":
        """Create an owner-approved payment session for a milestone (employer only).

        /payments/prepare-order-payment is the public connector-language route.
        /payments/fund-escrow remains as a legacy alias for existing clients.
        """
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
            pi_id, mode = fund_escrow_stripe(db, user['id'], amount, order_id, milestone_id, "Owner-approved checkout funding")
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
        sig_header = getattr(_request_ctx, 'http_stripe_signature', '')

        if not stripe_configured():
            return json_response({"received": True, "mode": "simulated"})

        if not STRIPE_WEBHOOK_SECRET:
            return error_response("Webhook secret not configured", 500)

        try:
            event = stripe.Webhook.construct_event(body_raw, sig_header, STRIPE_WEBHOOK_SECRET)
        except ValueError:
            return error_response("Invalid payload", 400)
        except STRIPE_SIGNATURE_ERROR:
            return error_response("Invalid signature", 400)

        event_type = event.get('type')
        event_data = event.get('data') if isinstance(event, dict) else None
        data = event_data.get('object') if isinstance(event_data, dict) else None

        # Stripe can deliver thin/v2 events such as v2.core.event_destination.ping
        # that have related_object instead of the classic data.object payload.
        # Signature verification above is the security boundary; unknown/thin
        # events should be acknowledged, not crash and trigger endless retries.
        if not isinstance(data, dict):
            return json_response({"received": True})

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

    elif path == "/admin/marketplace-ops" and method == "GET":
        user = authenticate(db)
        if not user or not user['is_admin']:
            return error_response("Admin access required", 403)

        limit = min(max(1, int(params.get("limit", 20))), 100)
        job_rows = db.execute(
            """SELECT j.*, u.name as employer_name,
                      COUNT(DISTINCT a.id) as application_count
               FROM jobs j
               JOIN users u ON u.id = j.employer_id
               LEFT JOIN applications a ON a.job_id = j.id
               GROUP BY j.id
               ORDER BY j.created_at DESC
               LIMIT ?""",
            [limit]
        ).fetchall()

        recent_jobs = []
        for job in job_rows:
            job_dict = row_to_dict(job)
            job_id = job_dict["id"]
            job_link = f"#/jobs/{job_id}"
            notification_rows = db.execute(
                """SELECT n.id, n.user_id, n.type, n.title, n.link, n.is_read, n.created_at,
                          u.name as user_name
                   FROM notifications n
                   JOIN users u ON u.id = n.user_id
                   WHERE n.link = ? AND n.type = 'job_match'
                   ORDER BY n.created_at DESC""",
                [job_link]
            ).fetchall()
            application_rows = db.execute(
                """SELECT a.id, a.worker_id, a.status, a.created_at,
                          u.name as worker_name
                   FROM applications a
                   JOIN users u ON u.id = a.worker_id
                   WHERE a.job_id = ?
                   ORDER BY a.created_at DESC""",
                [job_id]
            ).fetchall()
            matching_worker_rows = db.execute(
                """SELECT DISTINCT s.worker_id, u.name as worker_name, COUNT(s.id) as matching_service_count
                   FROM services s
                   JOIN users u ON u.id = s.worker_id
                   WHERE s.category = ? AND s.status = 'active' AND s.worker_id != ?
                   GROUP BY s.worker_id, u.name
                   ORDER BY matching_service_count DESC, u.name ASC
                   LIMIT 50""",
                [job_dict["category"], job_dict["employer_id"]]
            ).fetchall()
            notifications = [row_to_dict(n) for n in notification_rows]
            applications = [row_to_dict(a) for a in application_rows]
            unread_notifications = [n for n in notifications if not n.get("is_read")]
            job_dict["job_match_notifications"] = notifications
            job_dict["job_match_notification_count"] = len(notifications)
            job_dict["job_match_unread_count"] = len(unread_notifications)
            job_dict["applications"] = applications
            job_dict["matching_workers"] = [row_to_dict(w) for w in matching_worker_rows]
            job_dict["matching_worker_count"] = len(matching_worker_rows)
            job_dict["activation_funnel"] = {
                "matching_workers": job_dict["matching_worker_count"],
                "notifications_sent": len(notifications),
                "notifications_unread": len(unread_notifications),
                "applications_submitted": len(applications),
                "status": (
                    "needs_matching_workers" if job_dict["matching_worker_count"] == 0 else
                    "needs_notifications" if len(notifications) == 0 else
                    "workers_not_reading" if len(unread_notifications) == len(notifications) and len(applications) == 0 else
                    "read_no_applications" if len(applications) == 0 else
                    "has_applications"
                )
            }
            recent_jobs.append(job_dict)

        stuck_jobs = [
            {
                "id": j["id"],
                "title": j["title"],
                "category": j["category"],
                "activation_funnel": j["activation_funnel"],
            }
            for j in recent_jobs
            if j.get("status") == "open" and j["activation_funnel"]["notifications_sent"] > 0 and j["activation_funnel"]["applications_submitted"] == 0
        ]
        summary = {
            "total_users": db.execute("SELECT COUNT(*) as c FROM users").fetchone()['c'],
            "workers_registered": db.execute("SELECT COUNT(*) as c FROM worker_profiles").fetchone()['c'],
            "employers_registered": db.execute("SELECT COUNT(*) as c FROM employer_profiles").fetchone()['c'],
            "active_services": db.execute("SELECT COUNT(*) as c FROM services WHERE status='active'").fetchone()['c'],
            "open_jobs": db.execute("SELECT COUNT(*) as c FROM jobs WHERE status='open'").fetchone()['c'],
            "total_applications": db.execute("SELECT COUNT(*) as c FROM applications").fetchone()['c'],
            "job_match_notifications_24h": db.execute("SELECT COUNT(*) as c FROM notifications WHERE type='job_match' AND datetime(created_at) >= datetime('now','-1 day')").fetchone()['c'],
            "stuck_open_jobs": len(stuck_jobs),
        }
        return json_response({
            "summary": summary,
            "recent_jobs": recent_jobs,
            "stuck_jobs": stuck_jobs,
            "limit": limit,
        })

    elif path == "/admin/application-pipeline" and method == "GET":
        user = authenticate(db)
        if not user or not user['is_admin']:
            return error_response("Admin access required", 403)

        limit = min(max(1, int(params.get("limit", 50))), 100)
        rows = db.execute(
            """SELECT a.id, a.job_id, a.worker_id, a.status, a.cover_message, a.portfolio_url,
                      a.created_at,
                      j.title as job_title, j.category as job_category, j.status as job_status,
                      j.budget_amount, j.budget_type, j.employer_id,
                      wu.name as worker_name, wu.email as worker_email,
                      eu.name as employer_name
               FROM applications a
               JOIN jobs j ON j.id = a.job_id
               JOIN users wu ON wu.id = a.worker_id
               JOIN users eu ON eu.id = j.employer_id
               ORDER BY a.created_at DESC
               LIMIT ?""",
            [limit]
        ).fetchall()
        applications = []
        for row in rows:
            item = row_to_dict(row)
            cover = (item.get("cover_message") or "").strip()
            item["cover_message_length"] = len(cover)
            item["has_portfolio_url"] = bool((item.get("portfolio_url") or "").strip())
            lower = cover.lower()
            quality_flags = []
            if len(cover) >= 120:
                quality_flags.append("specific_cover_message")
            if item["has_portfolio_url"]:
                quality_flags.append("portfolio_or_proof_url")
            if any(word in lower for word in ["deliver", "screenshot", "source", "spreadsheet", "scorecard", "tomorrow", "today", "hours", "day"]):
                quality_flags.append("deliverable_or_timing_signal")
            item["quality_flags"] = quality_flags
            item["triage_status"] = (
                "strong_candidate" if len(quality_flags) >= 2 else
                "needs_manual_review" if quality_flags else
                "weak_or_incomplete"
            )
            applications.append(item)
        summary = {
            "total_recent_applications": len(applications),
            "strong_candidates": sum(1 for a in applications if a["triage_status"] == "strong_candidate"),
            "needs_manual_review": sum(1 for a in applications if a["triage_status"] == "needs_manual_review"),
            "weak_or_incomplete": sum(1 for a in applications if a["triage_status"] == "weak_or_incomplete"),
            "pending_applications": sum(1 for a in applications if a.get("status") == "pending"),
        }
        return json_response({"summary": summary, "applications": applications, "limit": limit})

    elif path == "/admin/worker-activation-notifications" and method == "POST":
        user = authenticate(db)
        if not user or not user['is_admin']:
            return error_response("Admin access required", 403)

        body = get_body()
        user_ids = body.get("user_ids", [])
        title = (body.get("title") or "").strip()
        message = (body.get("message") or "").strip()
        link = (body.get("link") or "#/jobs").strip()
        if not isinstance(user_ids, list) or not user_ids:
            return error_response("user_ids must be a non-empty list")
        if not title or not message:
            return error_response("title and message are required")
        if len(title) > 140:
            return error_response("title must be 140 characters or less")
        if len(message) > 1200:
            return error_response("message must be 1200 characters or less")

        normalized_user_ids = []
        seen_user_ids = set()
        for raw_id in user_ids:
            try:
                worker_user_id = int(raw_id)
            except (TypeError, ValueError):
                return error_response("user_ids must contain integer user IDs")
            if worker_user_id in seen_user_ids:
                continue
            seen_user_ids.add(worker_user_id)
            normalized_user_ids.append(worker_user_id)

        existing_users = db.execute(
            f"SELECT id, name FROM users WHERE id IN ({','.join('?' for _ in normalized_user_ids)}) AND is_active=1 AND is_banned=0 AND is_suspended=0",
            normalized_user_ids
        ).fetchall()
        existing_ids = {row['id'] for row in existing_users}
        missing_ids = [uid for uid in normalized_user_ids if uid not in existing_ids]
        if missing_ids:
            return error_response(f"Unknown or inactive user_ids: {missing_ids}", 404)

        sent = []
        for worker_user_id in normalized_user_ids:
            push_notification(db, worker_user_id, "worker_activation", title, message, link)
            sent.append(worker_user_id)
        audit(db, user['id'], "send_worker_activation_notifications", "notification", None, {"user_ids": sent, "link": link})
        db.commit()
        return json_response({"ok": True, "sent_user_ids": sent, "count": len(sent)})

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

    elif re.match(r"^/admin/users/(\d+)/password$", path) and method == "PUT":
        user = authenticate(db)
        if not user or not user['is_admin']:
            return error_response("Admin access required", 403)

        target_id = int(re.match(r"^/admin/users/(\d+)/password$", path).group(1))
        body = get_body() or {}
        step_error, step_status = require_admin_step_up(db, user, body, "admin_rotate_user_password")
        if step_error:
            return error_response(step_error, step_status)
        new_password = body.get("password", "")
        if len(new_password) < 12:
            return error_response("Password must be at least 12 characters", 400)
        target = db.execute("SELECT id, email FROM users WHERE id=?", [target_id]).fetchone()
        if not target:
            return error_response("User not found", 404)
        db.execute(
            "UPDATE users SET password_hash=?, updated_at=datetime('now') WHERE id=?",
            [hash_password(new_password), target_id]
        )
        audit(db, user['id'], "admin_rotate_user_password", "user", target_id, {"target_email": target['email']})
        db.commit()
        return json_response({"ok": True, "user_id": target_id})

    elif re.match(r"^/admin/users/(\d+)$", path) and method == "PUT":
        user = authenticate(db)
        if not user or not user['is_admin']:
            return error_response("Admin access required", 403)

        target_id = int(re.match(r"^/admin/users/(\d+)$", path).group(1))
        body = get_body() or {}
        sensitive_fields = {'is_admin', 'is_active', 'is_suspended', 'is_banned'}
        if any(field in body for field in sensitive_fields):
            step_error, step_status = require_admin_step_up(db, user, body, "admin_update_user")
            if step_error:
                return error_response(step_error, step_status)

        updates = []
        vals = []
        for field in ['is_active', 'is_suspended', 'is_banned', 'is_admin']:
            if field in body:
                updates.append(f"{field} = ?")
                vals.append(1 if body[field] else 0)
        if updates:
            vals.append(target_id)
            db.execute(f"UPDATE users SET {', '.join(updates)} WHERE id=?", vals)

        audit_details = {k: v for k, v in body.items() if k != 'admin_password'}
        audit(db, user['id'], "admin_update_user", "user", target_id, audit_details)
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
               s.title as service_title, j.title as job_title,
               CASE WHEN hc.id IS NOT NULL OR j.budget_type='hourly' THEN 'hourly' ELSE 'fixed' END as contract_type,
               COALESCE(hc.hourly_rate, CASE WHEN j.budget_type='hourly' THEN j.budget_amount END) as hourly_rate,
               hc.weekly_hour_cap, hc.current_week_escrow_amount
               FROM orders o
               JOIN users wu ON o.worker_id = wu.id
               JOIN users eu ON o.employer_id = eu.id
               LEFT JOIN services s ON o.service_id = s.id
               LEFT JOIN jobs j ON o.job_id = j.id
               LEFT JOIN hourly_contracts hc ON hc.order_id = o.id
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
        step_error, step_status = require_admin_step_up(db, user, body, "admin_resolve_dispute")
        if step_error:
            return error_response(step_error, step_status)

        if body.get("manual_money_movement_confirmed") is not True:
            return error_response(
                "Manual money movement confirmation required. Complete and verify any Stripe refund/transfer outside this admin action before recording the dispute resolution.",
                409
            )
        processor_reference = str(body.get("processor_reference") or "").strip()
        if not processor_reference:
            return error_response("processor_reference required for manual dispute settlement audit trail", 400)

        holds = db.execute(
            "SELECT * FROM escrow_holds WHERE order_id=? AND status='held'",
            [int(order_id)]
        ).fetchall()
        if not holds:
            return error_response("No held payment record available to resolve", 409)

        total_held_cents = sum(money_to_cents(hold['amount'], "held payment amount") for hold in holds)
        worker_percent = 100.0 if resolution == 'release_to_worker' else 0.0
        if resolution == 'split':
            try:
                worker_percent = float(body.get("worker_percent", 50))
            except (TypeError, ValueError):
                return error_response("worker_percent must be a number between 0 and 100", 400)
            if not math.isfinite(worker_percent) or worker_percent < 0 or worker_percent > 100:
                return error_response("worker_percent must be a finite number between 0 and 100", 400)
        worker_cents = int(
            (Decimal(total_held_cents) * Decimal(str(worker_percent)) / Decimal("100"))
            .to_integral_value(rounding=ROUND_HALF_UP)
        )
        employer_cents = total_held_cents - worker_cents
        total_held = total_held_cents / 100
        worker_portion = worker_cents / 100
        employer_portion = employer_cents / 100

        escrow_status = 'released' if resolution == 'release_to_worker' else 'refunded' if resolution == 'refund_to_employer' else 'partial'
        db.execute(
            "UPDATE escrow_holds SET status=?, released_at=datetime('now') WHERE order_id=? AND status='held'",
            [escrow_status, int(order_id)]
        )
        if worker_portion > 0:
            db.execute(
                "INSERT INTO platform_revenue (order_id, fee_amount, fee_type) VALUES (?,?,?)",
                [order_id, component_fee_cents(worker_cents, PLATFORM_FEE_BPS) / 100, 'manual_dispute_resolution']
            )

        push_notification(db, order['worker_id'], "dispute_resolved",
            "Dispute resolution recorded",
            f"The dispute for order #{order_id} was resolved after manual settlement was verified by an admin.",
            f"/orders/{order_id}")
        push_notification(db, order['employer_id'], "dispute_resolved",
            "Dispute resolution recorded",
            f"The dispute for order #{order_id} was resolved after manual settlement was verified by an admin.",
            f"/orders/{order_id}")

        db.execute(
            "UPDATE orders SET status='completed', completed_at=datetime('now'), updated_at=datetime('now') WHERE id=?",
            [int(order_id)]
        )
        synchronize_job_terminal_state(db, order)
        audit(db, user['id'], "resolve_dispute_manual_settlement", "order", int(order_id), {
            "resolution": resolution,
            "notes": admin_notes,
            "manual_money_movement_confirmed": True,
            "processor_reference": processor_reference,
            "worker_portion": worker_portion,
            "employer_portion": employer_portion,
        })
        db.commit()
        return json_response({
            "ok": True,
            "resolution": resolution,
            "mode": "manual_settlement_recorded",
            "processor_reference": processor_reference,
            "worker_portion": worker_portion,
            "employer_portion": employer_portion,
        })

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
            # AI Services
            {
                "worker_idx": 1, "category": "ai_coding", "pricing_type": "fixed",
                "title": "AI Code Review & Bug Detection",
                "description": "Automated code review powered by advanced AI. Submit your codebase and get detailed analysis of bugs, security vulnerabilities, performance issues, and best practice violations. Supports Python, JavaScript, TypeScript, Go, and Rust.",
                "price": 49.0, "delivery_time_days": 1,
                "includes": "Full codebase scan, bug report, security audit, performance suggestions",
                "tags": ["ai", "code review", "debugging", "security"],
                "provider_type": "ai", "fulfillment_type": "manual", "ai_model": "GPT-4 + Custom Analysis"
            },
            {
                "worker_idx": 2, "category": "ai_writing", "pricing_type": "fixed",
                "title": "AI-Powered SEO Content Suite (5 articles)",
                "description": "Get 5 fully SEO-optimized articles written by AI, reviewed and edited by a human writer. Each article is 1500+ words with keyword research, meta descriptions, and internal linking suggestions.",
                "price": 199.0, "delivery_time_days": 2,
                "includes": "5 articles, keyword research, meta descriptions, human editing pass",
                "tags": ["ai", "seo", "content", "blog"],
                "provider_type": "ai", "fulfillment_type": "manual", "ai_model": "Claude + Human Editor"
            },
            {
                "worker_idx": 4, "category": "ai_data_analysis", "pricing_type": "fixed",
                "title": "AI Data Analysis & Visualization Dashboard",
                "description": "Upload your dataset and get a complete analysis with insights, trends, anomalies, and an interactive dashboard. Powered by AI with human QA review.",
                "price": 149.0, "delivery_time_days": 2,
                "includes": "Data cleaning, statistical analysis, visualization dashboard, insights report",
                "tags": ["ai", "data analysis", "visualization", "dashboard"],
                "provider_type": "ai", "fulfillment_type": "manual", "ai_model": "GPT-4 + Python Analytics"
            },
        ]

        service_ids = []
        for s in services_data:
            cursor = db.execute(
                """INSERT INTO services
                   (worker_id, title, description, category, pricing_type, price, hourly_rate,
                    delivery_time_days, includes, tags, images, status, avg_rating, total_reviews,
                    provider_type, fulfillment_type, api_endpoint, ai_model, avg_response_time)
                   VALUES (?,?,?,?,?,?,?,?,?,?,'[]','active',?,?,?,?,?,?,?)""",
                [worker_ids[s['worker_idx']], s['title'], s['description'], s['category'],
                 s['pricing_type'], s.get('price'), s.get('hourly_rate'),
                 s.get('delivery_time_days'), s.get('includes', ''),
                 json.dumps(s['tags']),
                 round(4.5 + secrets.randbelow(5) * 0.1, 1),
                 secrets.randbelow(20) + 5,
                 s.get('provider_type', 'human'), s.get('fulfillment_type', 'manual'),
                 s.get('api_endpoint', ''), s.get('ai_model', ''), s.get('avg_response_time', '')]
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
            "admin": {"email": "admin@gohirehumans.com", "note": "Credentials redacted. Change default password immediately."},
            "workers": [{"email": w['email']} for w in workers_data],
            "employers": [{"email": e['email']} for e in employers_data],
            "services_created": len(service_ids),
            "jobs_created": len(job_ids),
            "sample_completed_order_id": completed_order_id,
            "stripe_mode": "live" if stripe_configured() else "simulated"
        }, 201)

    # ═══════════════════════════════════════════════════════════════════════════
    # API KEY MANAGEMENT (for AI Agent Integration)
    # ═══════════════════════════════════════════════════════════════════════════

    elif path == "/api-keys" and method == "GET":
        user = authenticate(db)
        if not user:
            return error_response("Authentication required", 401)
        keys = db.execute(
            """SELECT id, key_prefix, name, scopes, rate_limit, is_active,
                      last_used_at, total_requests, created_at, expires_at
               FROM api_keys WHERE user_id = ? ORDER BY created_at DESC""",
            [user['id']]
        ).fetchall()
        return json_response({"api_keys": [row_to_dict(k) for k in keys]})

    elif path == "/api-keys" and method == "POST":
        user = authenticate(db)
        if not user:
            return error_response("Authentication required", 401)
        body = get_body()
        key_name = (body or {}).get("name", "Default Key")[:100]
        scopes = (body or {}).get("scopes", ["read", "write"])

        # Generate a unique API key: ghh_<random>
        raw_key = f"ghh_{secrets.token_hex(24)}"
        key_prefix = raw_key[:12]
        key_hash_val = hashlib.sha256(raw_key.encode()).hexdigest()

        # Limit to 5 active keys per user
        active_count = db.execute(
            "SELECT COUNT(*) as c FROM api_keys WHERE user_id = ? AND is_active = 1",
            [user['id']]
        ).fetchone()['c']
        if active_count >= 5:
            return error_response("Maximum 5 active API keys per account", 400)

        cur = db.execute(
            """INSERT INTO api_keys (user_id, key_hash, key_prefix, name, scopes)
               VALUES (?, ?, ?, ?, ?)""",
            [user['id'], key_hash_val, key_prefix, key_name, json.dumps(scopes)]
        )
        db.commit()
        audit(db, user['id'], 'api_key_created', 'api_key', cur.lastrowid)

        return json_response({
            "api_key": {
                "id": cur.lastrowid,
                "key": raw_key,
                "key_prefix": key_prefix,
                "name": key_name,
                "scopes": scopes,
                "note": "Save this key securely — it will not be shown again."
            }
        }, 201)

    elif path == "/api-keys/revoke" and method == "POST":
        user = authenticate(db)
        if not user:
            return error_response("Authentication required", 401)
        body = get_body()
        key_id = (body or {}).get("key_id")
        if not key_id:
            return error_response("key_id required", 400)
        # Verify ownership
        existing = db.execute(
            "SELECT * FROM api_keys WHERE id = ? AND user_id = ?",
            [key_id, user['id']]
        ).fetchone()
        if not existing:
            return error_response("API key not found", 404)
        db.execute("UPDATE api_keys SET is_active = 0 WHERE id = ?", [key_id])
        db.commit()
        audit(db, user['id'], 'api_key_revoked', 'api_key', key_id)
        return json_response({"message": "API key revoked"})

    elif path == "/api-keys/usage" and method == "GET":
        user = authenticate(db)
        if not user:
            return error_response("Authentication required", 401)
        params = get_query_params()
        key_id = params.get("key_id")

        try:
            days = parse_int_param(params, "days", 30, min_value=1, max_value=90)
        except ValueError as e:
            return error_response(str(e), 400)

        # Build query based on whether specific key requested
        if key_id:
            # Verify ownership
            existing = db.execute(
                "SELECT id FROM api_keys WHERE id = ? AND user_id = ?",
                [key_id, user['id']]
            ).fetchone()
            if not existing:
                return error_response("API key not found", 404)
            usage = db.execute(
                """SELECT endpoint, method, COUNT(*) as request_count,
                          AVG(response_time_ms) as avg_response_time,
                          date(created_at) as date
                   FROM api_key_usage
                   WHERE api_key_id = ? AND created_at >= datetime('now', ?)
                   GROUP BY date, endpoint, method
                   ORDER BY date DESC""",
                [key_id, f"-{days} days"]
            ).fetchall()
        else:
            # All keys for this user
            usage = db.execute(
                """SELECT ak.name as key_name, aku.endpoint, aku.method,
                          COUNT(*) as request_count,
                          AVG(aku.response_time_ms) as avg_response_time,
                          date(aku.created_at) as date
                   FROM api_key_usage aku
                   JOIN api_keys ak ON aku.api_key_id = ak.id
                   WHERE ak.user_id = ? AND aku.created_at >= datetime('now', ?)
                   GROUP BY date, ak.name, aku.endpoint, aku.method
                   ORDER BY date DESC""",
                [user['id'], f"-{days} days"]
            ).fetchall()

        # Summary stats
        summary = db.execute(
            """SELECT COUNT(*) as total_requests,
                      COUNT(DISTINCT api_key_id) as keys_used,
                      AVG(response_time_ms) as avg_response_time
               FROM api_key_usage aku
               JOIN api_keys ak ON aku.api_key_id = ak.id
               WHERE ak.user_id = ? AND aku.created_at >= datetime('now', ?)""",
            [user['id'], f"-{days} days"]
        ).fetchone()

        return json_response({
            "summary": row_to_dict(summary) if summary else {},
            "usage": [row_to_dict(u) for u in usage],
            "period_days": days
        })

    # ═══════════════════════════════════════════════════════════════════════════
    # API KEY AUTHENTICATION MIDDLEWARE HELPER
    # ═══════════════════════════════════════════════════════════════════════════

    elif path == "/api-keys/verify" and method == "POST":
        """Verify an API key and return the associated user info."""
        body = get_body()
        api_key = (body or {}).get("api_key", "")
        if not api_key or not api_key.startswith("ghh_"):
            return error_response("Invalid API key format", 401)

        key_hash_val = hashlib.sha256(api_key.encode()).hexdigest()
        key_row = db.execute(
            """SELECT ak.*, u.name, u.email, u.id as uid
               FROM api_keys ak JOIN users u ON ak.user_id = u.id
               WHERE ak.key_hash = ? AND ak.is_active = 1""",
            [key_hash_val]
        ).fetchone()

        if not key_row:
            return error_response("Invalid or revoked API key", 401)

        # Check expiry
        if key_row['expires_at']:
            from datetime import datetime as dt
            if dt.fromisoformat(key_row['expires_at']) < dt.utcnow():
                return error_response("API key expired", 401)

        # Update usage stats
        db.execute(
            "UPDATE api_keys SET last_used_at = datetime('now'), total_requests = total_requests + 1 WHERE id = ?",
            [key_row['id']]
        )
        db.commit()

        return json_response({
            "valid": True,
            "user": {
                "id": key_row['uid'],
                "name": key_row['name'],
                "email": key_row['email']
            },
            "key": {
                "id": key_row['id'],
                "name": key_row['name'],
                "scopes": json.loads(key_row['scopes']),
                "rate_limit": key_row['rate_limit']
            }
        })

    # ═══════════════════════════════════════════════════════════════════════════
    # 404 FALLTHROUGH
    # ═══════════════════════════════════════════════════════════════════════════

    else:
        return error_response(f"Route not found: {method} {path}", 404)

# Force redeploy 20260429120000
