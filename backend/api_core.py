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
import logging
import urllib.parse
import urllib.request
from datetime import datetime, timezone, timedelta
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP

# Thread-local storage for per-request context (avoids os.environ race conditions)
_request_ctx = threading.local()

try:
    import stripe
    STRIPE_AVAILABLE = True
    STRIPE_SIGNATURE_ERROR = stripe.SignatureVerificationError
    STRIPE_ERROR = stripe.StripeError
    STRIPE_PAYOUT_DEFINITIVE_PREOP_ERRORS = (
        stripe.AuthenticationError,
        stripe.InvalidRequestError,
        stripe.PermissionError,
    )
except ImportError:
    STRIPE_AVAILABLE = False
    STRIPE_SIGNATURE_ERROR = ValueError
    STRIPE_ERROR = Exception
    STRIPE_PAYOUT_DEFINITIVE_PREOP_ERRORS = ()


class _SensitiveCapabilityLogFilter(logging.Filter):
    """Last-line defense against transient Stripe response capabilities in logs."""

    _patterns = (
        re.compile(r"(?:seti|pi)_[A-Za-z0-9_]+_secret_[A-Za-z0-9_]+", re.I),
        re.compile(r"https://connect\.stripe(?:\.com|\.test)?/[^\s'\"<>]+", re.I),
        re.compile(r"(?i)(client_secret[\"']?\s*[:=]\s*[\"']?)[^\s,}\"']+"),
        re.compile(r"(?i)(account_link(?:_url)?[\"']?\s*[:=]\s*[\"']?)[^\s,}\"']+"),
    )

    @classmethod
    def redact(cls, value):
        text = str(value)
        for pattern in cls._patterns:
            text = pattern.sub(r"\1[REDACTED]" if pattern.groups else "[REDACTED]", text)
        return text

    def filter(self, record):
        record.msg = self.redact(record.getMessage())
        record.args = ()
        return True


_SENSITIVE_LOG_FILTER = _SensitiveCapabilityLogFilter()


def install_sensitive_logging_filters():
    """Disable Stripe 13 direct debug output and filter every configured handler."""
    if STRIPE_AVAILABLE:
        try:
            stripe.log = None
            stripe_logger = logging.getLogger("stripe")
            stripe_logger.setLevel(logging.WARNING)
            from stripe import _util as stripe_util
            stripe_util.STRIPE_LOG = None
        except Exception:
            pass
    for logger in (logging.getLogger(), logging.getLogger("stripe")):
        if _SENSITIVE_LOG_FILTER not in logger.filters:
            logger.addFilter(_SENSITIVE_LOG_FILTER)
        for handler in logger.handlers:
            if _SENSITIVE_LOG_FILTER not in handler.filters:
                handler.addFilter(_SENSITIVE_LOG_FILTER)


install_sensitive_logging_filters()


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

    # In production the durable database is mandatory. Never replace it with an
    # empty working-directory or in-memory database because that can make a
    # financially inconsistent deployment appear healthy.
    durable_database_required = _VOLUME_ATTACHED or PRODUCTION_MODE
    candidates = [os.path.join(_VOLUME_DIR, "gohirehumans.db")]
    explicit = os.environ.get("DATABASE_PATH", "")
    if explicit and not explicit.startswith("/app"):
        candidates.append(explicit)
    if not durable_database_required:
        candidates.append(os.path.join(os.getcwd(), "gohirehumans.db"))
    candidates = list(dict.fromkeys(candidates))

    for candidate in candidates:
        parent = os.path.dirname(candidate) or "."
        try:
            os.makedirs(parent, exist_ok=True)
            test_db = sqlite3.connect(candidate)
            # Probe readability without issuing DDL. A transient writer lock must
            # not make startup abandon the configured durable database for an
            # empty fallback database; init_db will apply busy-timeout handling
            # and fail closed if required migrations cannot acquire the lock.
            test_db.execute("PRAGMA schema_version").fetchone()
            test_db.close()
            _db_path_resolved = candidate
            print(f"[GoHireHumans] DB path: {candidate}", file=sys.stderr)
            return _db_path_resolved
        except Exception as e:
            print(f"[GoHireHumans] Cannot use {candidate}: {e}", file=sys.stderr)

    if durable_database_required:
        raise RuntimeError("No durable database path is available in production")

    # Development-only fallback. Production is required to fail closed above.
    _db_path_resolved = ":memory:"
    print("[GoHireHumans] WARNING: Using in-memory development DB", file=sys.stderr)
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
# Deliberate code-level release gate for fixed-price hiring.
JOB_HIRING_ENABLED = False
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
    return bool(
        stripe_attr(account, 'payouts_enabled', False)
        and stripe_attr(account, 'charges_enabled', False)
        and stripe_attr(capabilities, 'transfers', None) == 'active'
    )


def retrieve_live_connect_account(account_id):
    if not stripe_configured() or not account_id or account_id.startswith('acct_sim_'):
        return None
    return stripe.Account.retrieve(account_id)


def record_payout_transfer(db, order_id, milestone_id, worker_id, amount, transfer_type, idempotency_key, destination_account_id, stripe_transfer=None, status='recorded', error_message='', release_attempt_id=None):
    transfer_id = ''
    if stripe_transfer is not None:
        transfer_id = stripe_attr(stripe_transfer, 'id', '') or ''
    cursor = db.execute(
        """INSERT INTO payout_transfers
           (order_id, milestone_id, worker_id, amount, currency, transfer_type, stripe_transfer_id,
            idempotency_key, destination_account_id, status, error_message, release_attempt_id, recorded_at)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?,datetime('now'))
           ON CONFLICT(idempotency_key) DO NOTHING""",
        [order_id, milestone_id, worker_id, amount, 'usd', transfer_type, transfer_id,
         idempotency_key, destination_account_id or '', status, error_message or '', release_attempt_id]
    )
    if cursor.rowcount == 0:
        existing = db.execute(
            "SELECT * FROM payout_transfers WHERE idempotency_key=?", [idempotency_key]
        ).fetchone()
        if not existing or any((
            existing['order_id'] != order_id,
            existing['milestone_id'] != milestone_id,
            existing['worker_id'] != worker_id,
            money_to_cents(existing['amount'], 'payout transfer amount') != money_to_cents(amount, 'payout transfer amount'),
            existing['currency'] != 'usd',
            existing['transfer_type'] != transfer_type,
            existing['stripe_transfer_id'] != transfer_id,
            existing['destination_account_id'] != (destination_account_id or ''),
            existing['status'] != status,
            existing['release_attempt_id'] != release_attempt_id,
        )):
            raise FundingReconciliationRequired(
                "Durable payout transfer conflicts with the requested release."
            )
    return cursor


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


def _table_columns(db, table_name):
    supported_tables = {
        "escrow_holds", "orders", "payout_transfers", "funding_attempts",
        "funding_attempt_conflict_evidence", "payout_release_attempts",
        "payout_release_conflict_evidence", "services", "users",
        "api_key_usage",
    }
    if table_name not in supported_tables:
        raise ValueError("Unsupported migration table")
    return {
        row[1]
        for row in db.execute(f"PRAGMA table_xinfo('{table_name}')").fetchall()
    }


def ensure_column(db, table_name, column_name, alter_sql):
    """Add one expected column without hiding unrelated SQLite failures."""
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", column_name):
        raise ValueError("Invalid column name")
    if column_name in _table_columns(db, table_name):
        return False
    try:
        db.execute(alter_sql)
    except sqlite3.OperationalError as exc:
        # A concurrent initializer can win the race between schema inspection and
        # ALTER TABLE. Suppress only SQLite's exact duplicate-column outcome and
        # only after proving the expected column now exists.
        if "duplicate column name" not in str(exc).lower():
            raise
        if column_name not in _table_columns(db, table_name):
            raise
        return False
    if column_name not in _table_columns(db, table_name):
        raise RuntimeError(f"Migration did not create {table_name}.{column_name}")
    return True


def _required_transaction_index_is_valid(db, index_name):
    specifications = {
        "idx_orders_one_job_hire": {
            "index_list_sql": "PRAGMA index_list('orders')",
            "index_info_sql": "PRAGMA index_info('idx_orders_one_job_hire')",
            "columns": ["job_id"],
            "predicate": "type='job_hire' and job_id is not null",
        },
        "idx_orders_creation_idempotency": {
            "index_list_sql": "PRAGMA index_list('orders')",
            "index_info_sql": "PRAGMA index_info('idx_orders_creation_idempotency')",
            "columns": ["employer_id", "creation_idempotency_key"],
            "predicate": "creation_idempotency_key is not null",
        },
        "idx_escrow_holds_funding_identity": {
            "index_list_sql": "PRAGMA index_list('escrow_holds')",
            "index_info_sql": "PRAGMA index_info('idx_escrow_holds_funding_identity')",
            "columns": ["funding_identity"],
            "predicate": "funding_identity is not null",
        },
        "idx_escrow_holds_funding_attempt": {
            "index_list_sql": "PRAGMA index_list('escrow_holds')",
            "index_info_sql": "PRAGMA index_info('idx_escrow_holds_funding_attempt')",
            "columns": ["funding_attempt_id"],
            "predicate": "funding_attempt_id is not null",
        },
        "idx_funding_attempts_operation_attempt": {
            "index_list_sql": "PRAGMA index_list('funding_attempts')",
            "index_info_sql": "PRAGMA index_info('idx_funding_attempts_operation_attempt')",
            "columns": ["operation_key", "attempt_number"],
            "predicate": None,
        },
        "idx_funding_attempts_active_operation": {
            "index_list_sql": "PRAGMA index_list('funding_attempts')",
            "index_info_sql": "PRAGMA index_info('idx_funding_attempts_active_operation')",
            "columns": ["operation_key"],
            "predicate": "status in ('prepared','unknown','processor_succeeded')",
        },
        "idx_funding_attempts_active_milestone": {
            "index_list_sql": "PRAGMA index_list('funding_attempts')",
            "index_info_sql": "PRAGMA index_info('idx_funding_attempts_active_milestone')",
            "columns": ["milestone_id"],
            "predicate": "milestone_id is not null and status in ('prepared','unknown','processor_succeeded')",
        },
        "idx_funding_attempts_processor_intent": {
            "index_list_sql": "PRAGMA index_list('funding_attempts')",
            "index_info_sql": "PRAGMA index_info('idx_funding_attempts_processor_intent')",
            "columns": ["stripe_payment_intent_id"],
            "predicate": "stripe_payment_intent_id is not null",
        },
        "idx_funding_attempts_processor_key": {
            "index_list_sql": "PRAGMA index_list('funding_attempts')",
            "index_info_sql": "PRAGMA index_info('idx_funding_attempts_processor_key')",
            "columns": ["processor_idempotency_key"],
            "predicate": None,
        },
        "idx_funding_conflict_evidence_key": {
            "index_list_sql": "PRAGMA index_list('funding_attempt_conflict_evidence')",
            "index_info_sql": "PRAGMA index_info('idx_funding_conflict_evidence_key')",
            "columns": ["evidence_key"],
            "predicate": None,
        },
        "idx_funding_conflict_evidence_attempt": {
            "index_list_sql": "PRAGMA index_list('funding_attempt_conflict_evidence')",
            "index_info_sql": "PRAGMA index_info('idx_funding_conflict_evidence_attempt')",
            "columns": ["attempt_id", "id"],
            "predicate": None,
            "unique": False,
        },
        "idx_funding_conflict_evidence_operation": {
            "index_list_sql": "PRAGMA index_list('funding_attempt_conflict_evidence')",
            "index_info_sql": "PRAGMA index_info('idx_funding_conflict_evidence_operation')",
            "columns": ["expected_operation_key", "id"],
            "predicate": None,
            "unique": False,
        },
        "idx_funding_conflict_evidence_obligation": {
            "index_list_sql": "PRAGMA index_list('funding_attempt_conflict_evidence')",
            "index_info_sql": "PRAGMA index_info('idx_funding_conflict_evidence_obligation')",
            "columns": ["expected_order_id", "expected_milestone_id", "id"],
            "predicate": None,
            "unique": False,
        },
        "idx_funding_conflict_evidence_incoming_intent": {
            "index_list_sql": "PRAGMA index_list('funding_attempt_conflict_evidence')",
            "index_info_sql": "PRAGMA index_info('idx_funding_conflict_evidence_incoming_intent')",
            "columns": ["incoming_intent_id"],
            "predicate": "incoming_intent_id is not null",
            "unique": False,
        },
        "idx_funding_conflict_evidence_owner": {
            "index_list_sql": "PRAGMA index_list('funding_attempt_conflict_evidence')",
            "index_info_sql": "PRAGMA index_info('idx_funding_conflict_evidence_owner')",
            "columns": ["intent_owner_attempt_id"],
            "predicate": "intent_owner_attempt_id is not null",
            "unique": False,
        },
    }
    specification = specifications.get(index_name)
    if specification is None:
        raise ValueError("Unsupported required transaction index")

    index_row = next(
        (
            row
            for row in db.execute(specification["index_list_sql"]).fetchall()
            if row[1] == index_name
        ),
        None,
    )
    expected_partial = specification["predicate"] is not None
    expected_unique = int(specification.get("unique", True))
    if (index_row is None or int(index_row[2]) != expected_unique
            or bool(int(index_row[4])) != expected_partial):
        return False

    columns = [row[2] for row in db.execute(specification["index_info_sql"]).fetchall()]
    if columns != specification["columns"]:
        return False

    # index_info omits collation and direction. Exact financial identity indexes
    # require ordinary BINARY ascending keys; NOCASE/RTRIM or DESC poisoning must
    # fail readiness even when names, uniqueness, columns, and predicates match.
    xinfo_rows = db.execute(f"PRAGMA index_xinfo('{index_name}')").fetchall()
    key_shape = [
        (row[2], int(row[3]), str(row[4]).upper())
        for row in xinfo_rows
        if int(row[5]) == 1
    ]
    expected_key_shape = [
        (column, 0, "BINARY") for column in specification["columns"]
    ]
    if key_shape != expected_key_shape:
        return False

    schema_row = db.execute(
        "SELECT sql FROM sqlite_master WHERE type='index' AND name=?",
        [index_name],
    ).fetchone()
    normalized_sql = re.sub(r"\s+", " ", str(schema_row[0] if schema_row else "").strip().lower())
    _, separator, predicate = normalized_sql.partition(" where ")
    if specification["predicate"] is None:
        return not separator
    return bool(separator) and predicate == specification["predicate"]


def _normalize_transaction_schema_sql(sql):
    normalized = re.sub(r"\s+", " ", str(sql or "").strip().lower())
    normalized = re.sub(r"\bif\s+not\s+exists\s+", "", normalized)
    return normalized.rstrip(";").strip()


_REQUIRED_FUNDING_TABLE_SQL = {
    "funding_attempts": """
        CREATE TABLE funding_attempts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            operation_key TEXT NOT NULL,
            attempt_number INTEGER NOT NULL CHECK(attempt_number > 0),
            request_fingerprint TEXT NOT NULL,
            processor_idempotency_key TEXT NOT NULL,
            employer_id INTEGER NOT NULL REFERENCES users(id),
            order_id INTEGER NOT NULL REFERENCES orders(id),
            milestone_id INTEGER REFERENCES milestones(id),
            base_amount_cents INTEGER NOT NULL CHECK(base_amount_cents > 0),
            platform_fee_cents INTEGER NOT NULL CHECK(platform_fee_cents >= 0),
            processing_fee_cents INTEGER NOT NULL CHECK(processing_fee_cents >= 0),
            charged_total_cents INTEGER NOT NULL CHECK(charged_total_cents > 0),
            currency TEXT NOT NULL DEFAULT 'usd',
            status TEXT NOT NULL CHECK(status IN ('prepared','unknown','processor_succeeded','committed','failed')),
            stripe_payment_intent_id TEXT,
            processor_status TEXT,
            evidence_source TEXT,
            processor_evidence_at TEXT,
            error_code TEXT,
            error_message TEXT,
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            updated_at TEXT NOT NULL DEFAULT (datetime('now')),
            last_reconciled_at TEXT,
            committed_at TEXT
        )
    """,
    "funding_attempt_conflict_evidence": """
        CREATE TABLE funding_attempt_conflict_evidence (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            evidence_key TEXT NOT NULL,
            attempt_id INTEGER NOT NULL REFERENCES funding_attempts(id),
            conflict_type TEXT NOT NULL,
            expected_operation_key TEXT NOT NULL,
            expected_order_id INTEGER NOT NULL,
            expected_milestone_id INTEGER,
            observed_operation_key TEXT,
            observed_order_id INTEGER,
            observed_milestone_id INTEGER,
            canonical_intent_id TEXT,
            incoming_intent_id TEXT,
            incoming_processor_status TEXT,
            incoming_evidence_source TEXT NOT NULL,
            processor_event_id TEXT,
            intent_owner_attempt_id INTEGER REFERENCES funding_attempts(id),
            expected_snapshot_json TEXT NOT NULL,
            expected_snapshot_sha256 TEXT NOT NULL,
            observed_snapshot_json TEXT NOT NULL,
            observed_snapshot_sha256 TEXT NOT NULL,
            normalized_evidence_json TEXT,
            created_at TEXT NOT NULL DEFAULT (datetime('now'))
        )
    """,
}


_REQUIRED_PAYOUT_TABLE_SQL = {
    "payout_release_attempts": """
        CREATE TABLE payout_release_attempts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            operation_key TEXT NOT NULL,
            attempt_number INTEGER NOT NULL CHECK(attempt_number > 0),
            request_fingerprint TEXT NOT NULL,
            processor_idempotency_key TEXT NOT NULL,
            funding_attempt_id INTEGER NOT NULL REFERENCES funding_attempts(id),
            hold_id INTEGER NOT NULL REFERENCES escrow_holds(id),
            order_id INTEGER NOT NULL REFERENCES orders(id),
            milestone_id INTEGER REFERENCES milestones(id),
            worker_id INTEGER NOT NULL REFERENCES users(id),
            employer_id INTEGER NOT NULL REFERENCES users(id),
            amount_cents INTEGER NOT NULL CHECK(amount_cents > 0),
            currency TEXT NOT NULL DEFAULT 'usd',
            destination_account_id TEXT NOT NULL,
            expected_order_status TEXT NOT NULL,
            expected_order_total_cents INTEGER NOT NULL CHECK(expected_order_total_cents > 0),
            expected_current_milestone_id INTEGER,
            expected_milestone_status TEXT,
            expected_milestone_amount_cents INTEGER,
            expected_hold_snapshot_json TEXT NOT NULL,
            expected_hold_snapshot_sha256 TEXT NOT NULL,
            expected_lifecycle_snapshot_json TEXT NOT NULL,
            expected_lifecycle_snapshot_sha256 TEXT NOT NULL,
            status TEXT NOT NULL CHECK(status IN ('prepared','unknown','processor_succeeded','committed','failed')),
            lifecycle_status TEXT NOT NULL DEFAULT 'pending' CHECK(lifecycle_status IN ('pending','completed','manual_review')),
            processor_transfer_id TEXT,
            processor_status TEXT,
            evidence_source TEXT,
            processor_evidence_at TEXT,
            error_code TEXT,
            error_message TEXT,
            manual_review_required INTEGER NOT NULL DEFAULT 0 CHECK(manual_review_required IN (0,1)),
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            updated_at TEXT NOT NULL DEFAULT (datetime('now')),
            last_reconciled_at TEXT,
            committed_at TEXT,
            lifecycle_completed_at TEXT
        )
    """,
    "payout_release_conflict_evidence": """
        CREATE TABLE payout_release_conflict_evidence (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            evidence_key TEXT NOT NULL,
            attempt_id INTEGER NOT NULL REFERENCES payout_release_attempts(id),
            conflict_type TEXT NOT NULL,
            canonical_transfer_id TEXT,
            incoming_transfer_id TEXT,
            incoming_processor_status TEXT,
            incoming_evidence_source TEXT NOT NULL,
            expected_snapshot_json TEXT NOT NULL,
            expected_snapshot_sha256 TEXT NOT NULL,
            observed_snapshot_json TEXT NOT NULL,
            observed_snapshot_sha256 TEXT NOT NULL,
            normalized_evidence_json TEXT,
            created_at TEXT NOT NULL DEFAULT (datetime('now'))
        )
    """,
}


def _normalized_table_xinfo(rows):
    return [
        (
            int(row[0]), str(row[1]), str(row[2]).upper(), int(row[3]),
            _normalize_transaction_schema_sql(row[4]) if row[4] is not None else None,
            int(row[5]), int(row[6]),
        )
        for row in rows
    ]


def _required_table_behavior_is_valid(db, table_name):
    probes = {
        "funding_attempts": """
            INSERT INTO funding_attempts
              (operation_key,attempt_number,request_fingerprint,
               processor_idempotency_key,employer_id,order_id,
               base_amount_cents,platform_fee_cents,processing_fee_cents,
               charged_total_cents,status)
            VALUES ('__schema_probe__',0,'probe','__schema_probe_key__',
                    -1,-1,1,0,0,1,'invalid')
        """,
        "payout_release_attempts": """
            INSERT INTO payout_release_attempts
              (operation_key,attempt_number,request_fingerprint,
               processor_idempotency_key,funding_attempt_id,hold_id,order_id,
               worker_id,employer_id,amount_cents,destination_account_id,
               expected_order_status,expected_order_total_cents,
               expected_hold_snapshot_json,expected_hold_snapshot_sha256,
               expected_lifecycle_snapshot_json,expected_lifecycle_snapshot_sha256,
               status)
            VALUES ('__schema_probe__',0,'probe','__schema_probe_key__',
                    -1,-1,-1,-1,-1,0,'acct_probe','pending',0,
                    '{}','probe','{}','probe','invalid')
        """,
    }
    probe_sql = probes.get(table_name)
    if probe_sql is None:
        return True
    savepoint = f"required_schema_probe_{table_name}"
    db.execute(f"SAVEPOINT {savepoint}")
    db.execute("PRAGMA defer_foreign_keys=ON")
    rejected_by_check = False
    try:
        db.execute(probe_sql)
    except sqlite3.IntegrityError as exc:
        rejected_by_check = "check constraint failed" in str(exc).lower()
    finally:
        db.execute(f"ROLLBACK TO {savepoint}")
        db.execute(f"RELEASE {savepoint}")
        db.execute("PRAGMA defer_foreign_keys=OFF")
    return rejected_by_check


def _required_table_schema_is_valid(db, table_name, expected_sql):
    row = db.execute(
        "SELECT type,sql FROM sqlite_master WHERE name=?", [table_name]
    ).fetchone()
    if (
        not row or row["type"] != "table"
        or _normalize_transaction_schema_sql(row["sql"])
        != _normalize_transaction_schema_sql(expected_sql)
    ):
        return False

    expected = sqlite3.connect(":memory:")
    try:
        expected.execute(expected_sql)
        expected_xinfo = _normalized_table_xinfo(
            expected.execute(f"PRAGMA table_xinfo('{table_name}')").fetchall()
        )
        expected_fks = [
            tuple(item)
            for item in expected.execute(f"PRAGMA foreign_key_list('{table_name}')").fetchall()
        ]
    finally:
        expected.close()
    actual_xinfo = _normalized_table_xinfo(
        db.execute(f"PRAGMA table_xinfo('{table_name}')").fetchall()
    )
    actual_fks = [
        tuple(item)
        for item in db.execute(f"PRAGMA foreign_key_list('{table_name}')").fetchall()
    ]
    return (
        actual_xinfo == expected_xinfo
        and actual_fks == expected_fks
        and _required_table_behavior_is_valid(db, table_name)
    )


_PAYOUT_CONFLICT_EVIDENCE_TRIGGER_SQL = {
    "trg_payout_conflict_evidence_no_update": """
        CREATE TRIGGER IF NOT EXISTS trg_payout_conflict_evidence_no_update
        BEFORE UPDATE ON payout_release_conflict_evidence
        BEGIN
          SELECT RAISE(ABORT, 'payout conflict evidence is append-only');
        END
    """,
    "trg_payout_conflict_evidence_no_delete": """
        CREATE TRIGGER IF NOT EXISTS trg_payout_conflict_evidence_no_delete
        BEFORE DELETE ON payout_release_conflict_evidence
        BEGIN
          SELECT RAISE(ABORT, 'payout conflict evidence is append-only');
        END
    """,
    "trg_payout_conflict_evidence_no_replace": """
        CREATE TRIGGER IF NOT EXISTS trg_payout_conflict_evidence_no_replace
        BEFORE INSERT ON payout_release_conflict_evidence
        WHEN EXISTS (
          SELECT 1 FROM payout_release_conflict_evidence
          WHERE evidence_key=NEW.evidence_key
        )
        BEGIN
          SELECT RAISE(ABORT, 'payout conflict evidence cannot be replaced');
        END
    """,
}


_JOB_HIRE_TRIGGER_SQL = {
    "trg_orders_one_job_hire_insert": """
        CREATE TRIGGER IF NOT EXISTS trg_orders_one_job_hire_insert
        BEFORE INSERT ON orders
        WHEN NEW.type='job_hire' AND NEW.job_id IS NOT NULL
          AND EXISTS (
            SELECT 1 FROM orders
            WHERE type='job_hire' AND job_id=NEW.job_id
          )
        BEGIN
          SELECT RAISE(ABORT, 'job already has a hire order');
        END
    """,
    "trg_orders_one_job_hire_update": """
        CREATE TRIGGER IF NOT EXISTS trg_orders_one_job_hire_update
        BEFORE UPDATE OF type, job_id ON orders
        WHEN NEW.type='job_hire' AND NEW.job_id IS NOT NULL
          AND EXISTS (
            SELECT 1 FROM orders
            WHERE type='job_hire' AND job_id=NEW.job_id AND id<>NEW.id
          )
        BEGIN
          SELECT RAISE(ABORT, 'job already has a hire order');
        END
    """,
}


_FUNDING_CONFLICT_EVIDENCE_TRIGGER_SQL = {
    "trg_funding_conflict_evidence_no_update": """
        CREATE TRIGGER IF NOT EXISTS trg_funding_conflict_evidence_no_update
        BEFORE UPDATE ON funding_attempt_conflict_evidence
        BEGIN
          SELECT RAISE(ABORT, 'funding conflict evidence is append-only');
        END
    """,
    "trg_funding_conflict_evidence_no_delete": """
        CREATE TRIGGER IF NOT EXISTS trg_funding_conflict_evidence_no_delete
        BEFORE DELETE ON funding_attempt_conflict_evidence
        BEGIN
          SELECT RAISE(ABORT, 'funding conflict evidence is append-only');
        END
    """,
    "trg_funding_conflict_evidence_no_replace": """
        CREATE TRIGGER IF NOT EXISTS trg_funding_conflict_evidence_no_replace
        BEFORE INSERT ON funding_attempt_conflict_evidence
        WHEN EXISTS (
          SELECT 1 FROM funding_attempt_conflict_evidence
          WHERE evidence_key=NEW.evidence_key
        )
        BEGIN
          SELECT RAISE(ABORT, 'funding conflict evidence cannot be replaced');
        END
    """,
}


def _required_job_hire_trigger_is_valid(db, trigger_name):
    expected_sql = _JOB_HIRE_TRIGGER_SQL.get(trigger_name)
    if expected_sql is None:
        raise ValueError("Unsupported required transaction trigger")
    row = db.execute(
        "SELECT sql FROM sqlite_master WHERE type='trigger' AND name=?",
        [trigger_name],
    ).fetchone()
    return bool(row) and _normalize_transaction_schema_sql(row[0]) == (
        _normalize_transaction_schema_sql(expected_sql)
    )


def _required_funding_conflict_trigger_is_valid(db, trigger_name):
    expected_sql = _FUNDING_CONFLICT_EVIDENCE_TRIGGER_SQL.get(trigger_name)
    if expected_sql is None:
        raise ValueError("Unsupported funding-conflict evidence trigger")
    row = db.execute(
        "SELECT sql FROM sqlite_master WHERE type='trigger' AND name=?",
        [trigger_name],
    ).fetchone()
    return bool(row) and _normalize_transaction_schema_sql(row[0]) == (
        _normalize_transaction_schema_sql(expected_sql)
    )


_PAYOUT_INDEX_SQL = {
    "idx_payout_release_operation_attempt": """
        CREATE UNIQUE INDEX IF NOT EXISTS idx_payout_release_operation_attempt
        ON payout_release_attempts(operation_key, attempt_number)
    """,
    "idx_payout_release_processor_key": """
        CREATE UNIQUE INDEX IF NOT EXISTS idx_payout_release_processor_key
        ON payout_release_attempts(processor_idempotency_key)
    """,
    "idx_payout_release_processor_transfer": """
        CREATE UNIQUE INDEX IF NOT EXISTS idx_payout_release_processor_transfer
        ON payout_release_attempts(processor_transfer_id)
        WHERE processor_transfer_id IS NOT NULL
    """,
    "idx_payout_release_active_hold": """
        CREATE UNIQUE INDEX IF NOT EXISTS idx_payout_release_active_hold
        ON payout_release_attempts(hold_id)
        WHERE status IN ('prepared','unknown','processor_succeeded')
           OR lifecycle_status='pending'
    """,
    "idx_payout_release_evidence_key": """
        CREATE UNIQUE INDEX IF NOT EXISTS idx_payout_release_evidence_key
        ON payout_release_conflict_evidence(evidence_key)
    """,
    "idx_payout_release_evidence_attempt": """
        CREATE INDEX IF NOT EXISTS idx_payout_release_evidence_attempt
        ON payout_release_conflict_evidence(attempt_id, id)
    """,
    "idx_escrow_holds_release_attempt": """
        CREATE UNIQUE INDEX IF NOT EXISTS idx_escrow_holds_release_attempt
        ON escrow_holds(release_attempt_id)
        WHERE release_attempt_id IS NOT NULL
    """,
    "idx_payout_transfers_release_attempt": """
        CREATE UNIQUE INDEX IF NOT EXISTS idx_payout_transfers_release_attempt
        ON payout_transfers(release_attempt_id)
        WHERE release_attempt_id IS NOT NULL
    """,
}


def _required_payout_schema_object_is_valid(db, object_name, expected_sql, object_type):
    if object_type == "table":
        return _required_table_schema_is_valid(db, object_name, expected_sql)
    row = db.execute(
        "SELECT type,sql FROM sqlite_master WHERE name=?", [object_name]
    ).fetchone()
    return (
        bool(row)
        and row["type"] == object_type
        and _normalize_transaction_schema_sql(row["sql"])
        == _normalize_transaction_schema_sql(expected_sql)
    )


def validate_required_payout_schema(db):
    required_columns = {
        "escrow_holds": {"release_attempt_id"},
        "payout_transfers": {"release_attempt_id"},
        "payout_release_attempts": {
            "operation_key", "attempt_number", "request_fingerprint",
            "processor_idempotency_key", "funding_attempt_id", "hold_id",
            "order_id", "milestone_id", "worker_id", "employer_id",
            "amount_cents", "currency", "destination_account_id",
            "expected_order_status", "expected_order_total_cents",
            "expected_current_milestone_id", "expected_milestone_status",
            "expected_milestone_amount_cents", "expected_hold_snapshot_json",
            "expected_hold_snapshot_sha256", "expected_lifecycle_snapshot_json",
            "expected_lifecycle_snapshot_sha256", "status", "lifecycle_status",
            "processor_transfer_id", "processor_status", "evidence_source",
            "processor_evidence_at", "error_code", "error_message",
            "manual_review_required", "created_at", "updated_at",
            "last_reconciled_at", "committed_at", "lifecycle_completed_at",
        },
        "payout_release_conflict_evidence": {
            "evidence_key", "attempt_id", "conflict_type",
            "canonical_transfer_id", "incoming_transfer_id",
            "incoming_processor_status", "incoming_evidence_source",
            "expected_snapshot_json", "expected_snapshot_sha256",
            "observed_snapshot_json", "observed_snapshot_sha256",
            "normalized_evidence_json", "created_at",
        },
    }
    invalid = []
    for table_name, columns in required_columns.items():
        for column_name in sorted(columns - _table_columns(db, table_name)):
            invalid.append(f"{table_name}.{column_name}")
    for table_name, expected_sql in _REQUIRED_PAYOUT_TABLE_SQL.items():
        if not _required_payout_schema_object_is_valid(
            db, table_name, expected_sql, "table"
        ):
            invalid.append(f"{table_name} exact table schema")
    for index_name, expected_sql in _PAYOUT_INDEX_SQL.items():
        if not _required_payout_schema_object_is_valid(
            db, index_name, expected_sql, "index"
        ):
            invalid.append(f"{index_name} exact index schema")
    for trigger_name, expected_sql in _PAYOUT_CONFLICT_EVIDENCE_TRIGGER_SQL.items():
        if not _required_payout_schema_object_is_valid(
            db, trigger_name, expected_sql, "trigger"
        ):
            invalid.append(f"{trigger_name} exact trigger schema")
    if invalid:
        raise RuntimeError("Required payout schema missing: " + ", ".join(invalid))


def validate_required_transaction_schema(db):
    required_columns = {
        "escrow_holds": {
            "base_amount_cents",
            "platform_fee_cents",
            "processing_fee_cents",
            "charged_total_cents",
            "fee_policy_version",
            "funding_identity",
            "funding_attempt_id",
            "stripe_transfer_id",
        },
        "orders": {"creation_idempotency_key", "creation_request_fingerprint"},
        "funding_attempts": {
            "operation_key",
            "attempt_number",
            "request_fingerprint",
            "processor_idempotency_key",
            "employer_id",
            "order_id",
            "milestone_id",
            "base_amount_cents",
            "platform_fee_cents",
            "processing_fee_cents",
            "charged_total_cents",
            "currency",
            "status",
            "stripe_payment_intent_id",
            "processor_status",
            "evidence_source",
            "processor_evidence_at",
            "error_code",
            "error_message",
            "created_at",
            "updated_at",
            "last_reconciled_at",
            "committed_at",
        },
        "funding_attempt_conflict_evidence": {
            "evidence_key",
            "attempt_id",
            "conflict_type",
            "expected_operation_key",
            "expected_order_id",
            "expected_milestone_id",
            "observed_operation_key",
            "observed_order_id",
            "observed_milestone_id",
            "canonical_intent_id",
            "incoming_intent_id",
            "incoming_processor_status",
            "incoming_evidence_source",
            "processor_event_id",
            "intent_owner_attempt_id",
            "expected_snapshot_json",
            "expected_snapshot_sha256",
            "observed_snapshot_json",
            "observed_snapshot_sha256",
            "normalized_evidence_json",
            "created_at",
        },
    }
    missing = []
    for table_name, expected in required_columns.items():
        for column_name in sorted(expected - _table_columns(db, table_name)):
            missing.append(f"{table_name}.{column_name}")

    for table_name, expected_sql in _REQUIRED_FUNDING_TABLE_SQL.items():
        if not _required_table_schema_is_valid(db, table_name, expected_sql):
            missing.append(f"{table_name} exact table schema")

    for index_name in (
        "idx_orders_creation_idempotency",
        "idx_escrow_holds_funding_identity",
        "idx_escrow_holds_funding_attempt",
        "idx_funding_attempts_operation_attempt",
        "idx_funding_attempts_active_operation",
        "idx_funding_attempts_active_milestone",
        "idx_funding_attempts_processor_intent",
        "idx_funding_attempts_processor_key",
        "idx_funding_conflict_evidence_key",
        "idx_funding_conflict_evidence_attempt",
        "idx_funding_conflict_evidence_operation",
        "idx_funding_conflict_evidence_obligation",
        "idx_funding_conflict_evidence_incoming_intent",
        "idx_funding_conflict_evidence_owner",
    ):
        if not _required_transaction_index_is_valid(db, index_name):
            missing.append(index_name)

    for trigger_name in _FUNDING_CONFLICT_EVIDENCE_TRIGGER_SQL:
        if not _required_funding_conflict_trigger_is_valid(db, trigger_name):
            missing.append(f"{trigger_name} exact SQL")

    job_hire_index_valid = _required_transaction_index_is_valid(
        db, "idx_orders_one_job_hire"
    )
    job_hire_index_object = db.execute(
        "SELECT type FROM sqlite_master WHERE name='idx_orders_one_job_hire'"
    ).fetchone()
    if job_hire_index_object and not job_hire_index_valid:
        missing.append("idx_orders_one_job_hire exact SQL")
    job_hire_trigger_names = tuple(_JOB_HIRE_TRIGGER_SQL)
    trigger_rows = {
        row[0]
        for row in db.execute(
            """SELECT name FROM sqlite_master
               WHERE name IN (
                 'trg_orders_one_job_hire_insert',
                 'trg_orders_one_job_hire_update'
               )"""
        ).fetchall()
    }
    invalid_job_hire_triggers = [
        trigger_name
        for trigger_name in job_hire_trigger_names
        if trigger_name in trigger_rows
        and not _required_job_hire_trigger_is_valid(db, trigger_name)
    ]
    if invalid_job_hire_triggers:
        missing.extend(
            f"{trigger_name} exact SQL" for trigger_name in invalid_job_hire_triggers
        )
    exact_trigger_enforcement = all(
        _required_job_hire_trigger_is_valid(db, trigger_name)
        for trigger_name in job_hire_trigger_names
    )
    if not job_hire_index_valid and not exact_trigger_enforcement:
        missing.append("idx_orders_one_job_hire or exact trigger enforcement")

    if missing:
        raise RuntimeError("Required transaction schema missing: " + ", ".join(missing))


_PAYMENT_SETUP_INDEX_SQL = {
    "idx_payment_setup_operation_key": "CREATE UNIQUE INDEX IF NOT EXISTS idx_payment_setup_operation_key ON payment_setup_operations(operation_key)",
    "idx_payment_setup_processor_key": "CREATE UNIQUE INDEX IF NOT EXISTS idx_payment_setup_processor_key ON payment_setup_operations(processor_idempotency_key)",
}


def validate_required_payment_setup_schema(db, table_only=False):
    expected_table = """CREATE TABLE payment_setup_operations (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        operation_key TEXT NOT NULL UNIQUE,
        operation_kind TEXT NOT NULL,
        user_id INTEGER NOT NULL REFERENCES users(id),
        request_fingerprint TEXT NOT NULL,
        request_binding_json TEXT NOT NULL,
        processor_idempotency_key TEXT NOT NULL UNIQUE,
        status TEXT NOT NULL CHECK(status IN ('prepared','unknown','failed','committed')),
        processor_object_id TEXT,
        result_json TEXT,
        manual_review_required INTEGER NOT NULL DEFAULT 0 CHECK(manual_review_required IN (0,1)),
        error_code TEXT,
        created_at TEXT NOT NULL DEFAULT (datetime('now')),
        updated_at TEXT NOT NULL DEFAULT (datetime('now')),
        committed_at TEXT
    )"""
    invalid = []
    if not _required_table_schema_is_valid(db, "payment_setup_operations", expected_table):
        invalid.append("payment_setup_operations exact table schema")
    if table_only:
        if invalid:
            raise RuntimeError("Required payment setup schema missing: " + ", ".join(invalid))
        return
    for name, sql in _PAYMENT_SETUP_INDEX_SQL.items():
        if not _required_payout_schema_object_is_valid(db, name, sql, "index"):
            invalid.append(f"{name} exact index schema")
    triggers = db.execute(
        "SELECT name FROM sqlite_master WHERE type='trigger' AND tbl_name='payment_setup_operations'"
    ).fetchall()
    if triggers:
        invalid.extend(f"unexpected payment setup trigger {row[0]}" for row in triggers)
    if invalid:
        raise RuntimeError("Required payment setup schema missing: " + ", ".join(invalid))


def ensure_one_job_hire_enforcement(db):
    """Install the one-job-hire invariant without destroying legacy history.

    A pre-existing database may legitimately contain duplicate rows created before
    the invariant existed. A partial unique index cannot be added to that database,
    so preserve and audit those rows while installing triggers that reject any new
    duplicate. Once operators reconcile the legacy rows, a later init can add the
    stronger unique index. Canonical-name objects with different SQL fail closed;
    ``IF NOT EXISTS`` must never let a poisoned object bypass enforcement.
    """
    index_object = db.execute(
        "SELECT type FROM sqlite_master WHERE name='idx_orders_one_job_hire'"
    ).fetchone()
    if index_object and not _required_transaction_index_is_valid(
        db, "idx_orders_one_job_hire"
    ):
        raise RuntimeError(
            "Required transaction schema missing: idx_orders_one_job_hire exact SQL"
        )
    for trigger_name in _JOB_HIRE_TRIGGER_SQL:
        trigger_object = db.execute(
            "SELECT type FROM sqlite_master WHERE name=?",
            [trigger_name],
        ).fetchone()
        if trigger_object and not _required_job_hire_trigger_is_valid(db, trigger_name):
            raise RuntimeError(
                f"Required transaction schema missing: {trigger_name} exact SQL"
            )

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

    for trigger_sql in _JOB_HIRE_TRIGGER_SQL.values():
        db.execute(trigger_sql)
    for trigger_name in _JOB_HIRE_TRIGGER_SQL:
        if not _required_job_hire_trigger_is_valid(db, trigger_name):
            raise RuntimeError(
                f"Required transaction schema missing: {trigger_name} exact SQL"
            )
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


def _execute_schema_script(db, script):
    """Execute a SQLite script statement-by-statement without implicit commits."""
    statement = ""
    for line in script.splitlines(keepends=True):
        statement += line
        if sqlite3.complete_statement(statement):
            sql = statement.strip()
            statement = ""
            if sql:
                db.execute(sql)
    if statement.strip():
        raise RuntimeError("Incomplete schema statement")


def _prevalidate_financial_schema_before_mutation(db):
    """Reject poisoned protected objects before the first migration write."""
    protected = {
        "funding_attempts",
        "funding_attempt_conflict_evidence",
        "payout_release_attempts",
        "payout_release_conflict_evidence",
    }
    allowed_triggers = {
        "funding_attempt_conflict_evidence": set(_FUNDING_CONFLICT_EVIDENCE_TRIGGER_SQL),
        "payout_release_conflict_evidence": set(_PAYOUT_CONFLICT_EVIDENCE_TRIGGER_SQL),
    }
    rows = db.execute(
        "SELECT name,tbl_name FROM sqlite_master WHERE type='trigger' ORDER BY name"
    ).fetchall()
    unexpected = [
        row["name"] for row in rows
        if row["tbl_name"] in protected
        and row["name"] not in allowed_triggers.get(row["tbl_name"], set())
    ]
    if unexpected:
        raise RuntimeError(
            "Required financial schema has unexpected protected trigger(s): "
            + ", ".join(unexpected)
        )

    setup = db.execute(
        "SELECT type FROM sqlite_master WHERE name='payment_setup_operations'"
    ).fetchone()
    if setup:
        if setup["type"] != "table":
            raise RuntimeError("Required payment setup schema object has wrong type")
        validate_required_payment_setup_schema(db, table_only=True)
        for name, sql in _PAYMENT_SETUP_INDEX_SQL.items():
            existing = db.execute("SELECT type FROM sqlite_master WHERE name=?", [name]).fetchone()
            if existing and not _required_payout_schema_object_is_valid(db, name, sql, "index"):
                raise RuntimeError(
                    "Required payment setup schema missing: " + name + " exact index schema"
                )
        setup_triggers = db.execute(
            "SELECT name FROM sqlite_master WHERE type='trigger' AND tbl_name='payment_setup_operations'"
        ).fetchall()
        if setup_triggers:
            raise RuntimeError(
                "Required payment setup schema missing: "
                + ", ".join("unexpected payment setup trigger " + row[0] for row in setup_triggers)
            )


def _init_db_connection_steps(db):
    _execute_schema_script(db, """
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

    CREATE TABLE IF NOT EXISTS payment_setup_operations (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        operation_key TEXT NOT NULL UNIQUE,
        operation_kind TEXT NOT NULL,
        user_id INTEGER NOT NULL REFERENCES users(id),
        request_fingerprint TEXT NOT NULL,
        request_binding_json TEXT NOT NULL,
        processor_idempotency_key TEXT NOT NULL UNIQUE,
        status TEXT NOT NULL CHECK(status IN ('prepared','unknown','failed','committed')),
        processor_object_id TEXT,
        result_json TEXT,
        manual_review_required INTEGER NOT NULL DEFAULT 0 CHECK(manual_review_required IN (0,1)),
        error_code TEXT,
        created_at TEXT NOT NULL DEFAULT (datetime('now')),
        updated_at TEXT NOT NULL DEFAULT (datetime('now')),
        committed_at TEXT
    );
    CREATE INDEX IF NOT EXISTS idx_payment_setup_operations_user
        ON payment_setup_operations(user_id, operation_kind, id);

    CREATE TABLE IF NOT EXISTS order_completion_operations (
        order_id INTEGER PRIMARY KEY REFERENCES orders(id),
        employer_id INTEGER NOT NULL REFERENCES users(id),
        expected_order_status TEXT NOT NULL,
        hold_ids_json TEXT NOT NULL,
        hold_set_sha256 TEXT NOT NULL,
        status TEXT NOT NULL CHECK(status IN ('prepared','completed')),
        created_at TEXT NOT NULL DEFAULT (datetime('now')),
        completed_at TEXT
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
        creation_idempotency_key TEXT,
        creation_request_fingerprint TEXT,
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
        base_amount_cents INTEGER,
        platform_fee_cents INTEGER,
        processing_fee_cents INTEGER,
        charged_total_cents INTEGER,
        fee_policy_version TEXT,
        funding_identity TEXT,
        funding_attempt_id INTEGER REFERENCES funding_attempts(id),
        status TEXT NOT NULL DEFAULT 'held' CHECK(status IN ('held','released','refunded','partial')),
        stripe_payment_intent_id TEXT,
        stripe_transfer_id TEXT,
        created_at TEXT DEFAULT (datetime('now')),
        released_at TEXT
    );

    CREATE TABLE IF NOT EXISTS funding_attempts (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        operation_key TEXT NOT NULL,
        attempt_number INTEGER NOT NULL CHECK(attempt_number > 0),
        request_fingerprint TEXT NOT NULL,
        processor_idempotency_key TEXT NOT NULL,
        employer_id INTEGER NOT NULL REFERENCES users(id),
        order_id INTEGER NOT NULL REFERENCES orders(id),
        milestone_id INTEGER REFERENCES milestones(id),
        base_amount_cents INTEGER NOT NULL CHECK(base_amount_cents > 0),
        platform_fee_cents INTEGER NOT NULL CHECK(platform_fee_cents >= 0),
        processing_fee_cents INTEGER NOT NULL CHECK(processing_fee_cents >= 0),
        charged_total_cents INTEGER NOT NULL CHECK(charged_total_cents > 0),
        currency TEXT NOT NULL DEFAULT 'usd',
        status TEXT NOT NULL CHECK(status IN ('prepared','unknown','processor_succeeded','committed','failed')),
        stripe_payment_intent_id TEXT,
        processor_status TEXT,
        evidence_source TEXT,
        processor_evidence_at TEXT,
        error_code TEXT,
        error_message TEXT,
        created_at TEXT NOT NULL DEFAULT (datetime('now')),
        updated_at TEXT NOT NULL DEFAULT (datetime('now')),
        last_reconciled_at TEXT,
        committed_at TEXT
    );

    CREATE TABLE IF NOT EXISTS funding_attempt_conflict_evidence (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        evidence_key TEXT NOT NULL,
        attempt_id INTEGER NOT NULL REFERENCES funding_attempts(id),
        conflict_type TEXT NOT NULL,
        expected_operation_key TEXT NOT NULL,
        expected_order_id INTEGER NOT NULL,
        expected_milestone_id INTEGER,
        observed_operation_key TEXT,
        observed_order_id INTEGER,
        observed_milestone_id INTEGER,
        canonical_intent_id TEXT,
        incoming_intent_id TEXT,
        incoming_processor_status TEXT,
        incoming_evidence_source TEXT NOT NULL,
        processor_event_id TEXT,
        intent_owner_attempt_id INTEGER REFERENCES funding_attempts(id),
        expected_snapshot_json TEXT NOT NULL,
        expected_snapshot_sha256 TEXT NOT NULL,
        observed_snapshot_json TEXT NOT NULL,
        observed_snapshot_sha256 TEXT NOT NULL,
        normalized_evidence_json TEXT,
        created_at TEXT NOT NULL DEFAULT (datetime('now'))
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
    CREATE UNIQUE INDEX IF NOT EXISTS idx_funding_attempts_operation_attempt
        ON funding_attempts(operation_key, attempt_number);
    CREATE UNIQUE INDEX IF NOT EXISTS idx_funding_attempts_active_operation
        ON funding_attempts(operation_key)
        WHERE status IN ('prepared','unknown','processor_succeeded');
    CREATE UNIQUE INDEX IF NOT EXISTS idx_funding_attempts_active_milestone
        ON funding_attempts(milestone_id)
        WHERE milestone_id IS NOT NULL
          AND status IN ('prepared','unknown','processor_succeeded');
    CREATE UNIQUE INDEX IF NOT EXISTS idx_funding_attempts_processor_intent
        ON funding_attempts(stripe_payment_intent_id)
        WHERE stripe_payment_intent_id IS NOT NULL;
    CREATE UNIQUE INDEX IF NOT EXISTS idx_funding_attempts_processor_key
        ON funding_attempts(processor_idempotency_key);
    CREATE INDEX IF NOT EXISTS idx_funding_attempts_order ON funding_attempts(order_id);
    CREATE UNIQUE INDEX IF NOT EXISTS idx_funding_conflict_evidence_key
        ON funding_attempt_conflict_evidence(evidence_key);
    CREATE INDEX IF NOT EXISTS idx_funding_conflict_evidence_attempt
        ON funding_attempt_conflict_evidence(attempt_id, id);
    CREATE INDEX IF NOT EXISTS idx_funding_conflict_evidence_operation
        ON funding_attempt_conflict_evidence(expected_operation_key, id);
    CREATE INDEX IF NOT EXISTS idx_funding_conflict_evidence_obligation
        ON funding_attempt_conflict_evidence(expected_order_id, expected_milestone_id, id);
    CREATE INDEX IF NOT EXISTS idx_funding_conflict_evidence_incoming_intent
        ON funding_attempt_conflict_evidence(incoming_intent_id)
        WHERE incoming_intent_id IS NOT NULL;
    CREATE INDEX IF NOT EXISTS idx_funding_conflict_evidence_owner
        ON funding_attempt_conflict_evidence(intent_owner_attempt_id)
        WHERE intent_owner_attempt_id IS NOT NULL;
    """)
    for table_sql in _REQUIRED_PAYOUT_TABLE_SQL.values():
        db.execute(
            re.sub(
                r"^\s*CREATE TABLE ",
                "CREATE TABLE IF NOT EXISTS ",
                table_sql,
                count=1,
            )
        )
    for table_name, column_name, col_sql in [
        ("escrow_holds", "base_amount_cents", "ALTER TABLE escrow_holds ADD COLUMN base_amount_cents INTEGER"),
        ("escrow_holds", "platform_fee_cents", "ALTER TABLE escrow_holds ADD COLUMN platform_fee_cents INTEGER"),
        ("escrow_holds", "processing_fee_cents", "ALTER TABLE escrow_holds ADD COLUMN processing_fee_cents INTEGER"),
        ("escrow_holds", "charged_total_cents", "ALTER TABLE escrow_holds ADD COLUMN charged_total_cents INTEGER"),
        ("escrow_holds", "fee_policy_version", "ALTER TABLE escrow_holds ADD COLUMN fee_policy_version TEXT"),
        ("escrow_holds", "funding_identity", "ALTER TABLE escrow_holds ADD COLUMN funding_identity TEXT"),
        ("escrow_holds", "funding_attempt_id", "ALTER TABLE escrow_holds ADD COLUMN funding_attempt_id INTEGER REFERENCES funding_attempts(id)"),
        ("escrow_holds", "stripe_transfer_id", "ALTER TABLE escrow_holds ADD COLUMN stripe_transfer_id TEXT"),
        ("escrow_holds", "release_attempt_id", "ALTER TABLE escrow_holds ADD COLUMN release_attempt_id INTEGER REFERENCES payout_release_attempts(id)"),
        ("payout_transfers", "release_attempt_id", "ALTER TABLE payout_transfers ADD COLUMN release_attempt_id INTEGER REFERENCES payout_release_attempts(id)"),
        ("orders", "creation_idempotency_key", "ALTER TABLE orders ADD COLUMN creation_idempotency_key TEXT"),
        ("orders", "creation_request_fingerprint", "ALTER TABLE orders ADD COLUMN creation_request_fingerprint TEXT"),
    ]:
        ensure_column(db, table_name, column_name, col_sql)
    db.execute(
        """CREATE UNIQUE INDEX IF NOT EXISTS idx_orders_creation_idempotency
           ON orders(employer_id, creation_idempotency_key)
           WHERE creation_idempotency_key IS NOT NULL"""
    )
    db.execute(
        """CREATE UNIQUE INDEX IF NOT EXISTS idx_escrow_holds_funding_identity
           ON escrow_holds(funding_identity) WHERE funding_identity IS NOT NULL"""
    )
    db.execute(
        """CREATE UNIQUE INDEX IF NOT EXISTS idx_escrow_holds_funding_attempt
           ON escrow_holds(funding_attempt_id) WHERE funding_attempt_id IS NOT NULL"""
    )
    _run_init_db_failure_hook("mid")
    for index_sql in _PAYOUT_INDEX_SQL.values():
        db.execute(index_sql)
    for index_sql in _PAYMENT_SETUP_INDEX_SQL.values():
        db.execute(index_sql)
    for trigger_sql in _FUNDING_CONFLICT_EVIDENCE_TRIGGER_SQL.values():
        db.execute(trigger_sql)
    for trigger_sql in _PAYOUT_CONFLICT_EVIDENCE_TRIGGER_SQL.values():
        db.execute(trigger_sql)
    ensure_one_job_hire_enforcement(db)
    validate_required_transaction_schema(db)
    validate_required_payout_schema(db)
    validate_required_payment_setup_schema(db)

    # ── AI marketplace migrations ─────────────────────────────────────
    for column_name, col_sql in [
        ("provider_type", "ALTER TABLE services ADD COLUMN provider_type TEXT DEFAULT 'human'"),
        ("fulfillment_type", "ALTER TABLE services ADD COLUMN fulfillment_type TEXT DEFAULT 'manual'"),
        ("api_endpoint", "ALTER TABLE services ADD COLUMN api_endpoint TEXT DEFAULT ''"),
        ("ai_model", "ALTER TABLE services ADD COLUMN ai_model TEXT DEFAULT ''"),
        ("avg_response_time", "ALTER TABLE services ADD COLUMN avg_response_time TEXT DEFAULT ''"),
    ]:
        ensure_column(db, "services", column_name, col_sql)
    ensure_column(
        db, "users", "is_ai_agent",
        "ALTER TABLE users ADD COLUMN is_ai_agent INTEGER DEFAULT 0",
    )
    db.execute("CREATE INDEX IF NOT EXISTS idx_services_provider_type ON services(provider_type)")

    # ── Google OAuth + Referral program migrations ──────────────────────
    # SQLite cannot ADD COLUMN with UNIQUE, so uniqueness is installed below.
    for column_name, col_sql in [
        ("google_sub", "ALTER TABLE users ADD COLUMN google_sub TEXT"),
        ("referral_code", "ALTER TABLE users ADD COLUMN referral_code TEXT"),
        ("referred_by", "ALTER TABLE users ADD COLUMN referred_by INTEGER REFERENCES users(id)"),
    ]:
        if ensure_column(db, "users", column_name, col_sql):
            print(f"[GoHireHumans] Migration OK: {col_sql}", file=sys.stderr)

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
    db.execute("CREATE INDEX IF NOT EXISTS idx_referrals_referrer ON referrals(referrer_id)")
    db.execute("CREATE INDEX IF NOT EXISTS idx_users_referral_code ON users(referral_code)")

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
    db.execute("CREATE INDEX IF NOT EXISTS idx_api_keys_hash ON api_keys(key_hash)")
    db.execute("CREATE INDEX IF NOT EXISTS idx_api_keys_prefix ON api_keys(key_prefix)")
    db.execute("CREATE INDEX IF NOT EXISTS idx_api_keys_user ON api_keys(user_id)")

    # ── API Key Usage Log ─────────────────────────────────────────────────────
    db.execute("""CREATE TABLE IF NOT EXISTS api_key_usage (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        api_key_id INTEGER NOT NULL REFERENCES api_keys(id),
        request_id TEXT,
        endpoint TEXT NOT NULL,
        method TEXT NOT NULL,
        status_code INTEGER,
        response_time_ms INTEGER,
        accounting_state TEXT NOT NULL DEFAULT 'started',
        authorized_scope TEXT,
        finalized_at TEXT,
        error_message TEXT,
        created_at TEXT DEFAULT (datetime('now'))
    )""")
    for column_name, col_sql in [
        ("request_id", "ALTER TABLE api_key_usage ADD COLUMN request_id TEXT"),
        ("accounting_state", "ALTER TABLE api_key_usage ADD COLUMN accounting_state TEXT NOT NULL DEFAULT 'completed'"),
        ("authorized_scope", "ALTER TABLE api_key_usage ADD COLUMN authorized_scope TEXT"),
        ("finalized_at", "ALTER TABLE api_key_usage ADD COLUMN finalized_at TEXT"),
        ("error_message", "ALTER TABLE api_key_usage ADD COLUMN error_message TEXT"),
    ]:
        ensure_column(db, "api_key_usage", column_name, col_sql)
    db.execute("CREATE INDEX IF NOT EXISTS idx_usage_key ON api_key_usage(api_key_id)")
    db.execute("CREATE INDEX IF NOT EXISTS idx_usage_date ON api_key_usage(created_at)")
    db.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_usage_request ON api_key_usage(request_id) WHERE request_id IS NOT NULL")

    # Transactional notification email outbox. Rows are committed with the
    # domain mutation and delivered only after that writer transaction closes.
    db.execute("""CREATE TABLE IF NOT EXISTS transactional_email_outbox (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER NOT NULL REFERENCES users(id),
        notification_id INTEGER REFERENCES notifications(id),
        email_to TEXT NOT NULL,
        notification_type TEXT NOT NULL,
        title TEXT NOT NULL,
        message TEXT NOT NULL,
        link TEXT NOT NULL DEFAULT '',
        dedupe_context TEXT NOT NULL,
        dedupe_key TEXT NOT NULL UNIQUE,
        state TEXT NOT NULL DEFAULT 'pending'
            CHECK(state IN ('pending','sending','sent','failed')),
        attempts INTEGER NOT NULL DEFAULT 0,
        claimed_at TEXT,
        last_error TEXT,
        created_at TEXT NOT NULL DEFAULT (datetime('now')),
        sent_at TEXT
    )""")
    db.execute("CREATE INDEX IF NOT EXISTS idx_email_outbox_state ON transactional_email_outbox(state,id)")

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


_init_db_failure_hook = None


def _run_init_db_failure_hook(stage):
    hook = _init_db_failure_hook
    if callable(hook):
        hook(stage)


def _init_db_connection(db):
    """Install/migrate/validate the complete schema in one write transaction."""
    if db.in_transaction:
        db.rollback()
    db.execute("BEGIN IMMEDIATE")
    try:
        _prevalidate_financial_schema_before_mutation(db)
        _run_init_db_failure_hook("early")
        _init_db_connection_steps(db)
        _run_init_db_failure_hook("late")
        # Final checks execute before COMMIT so every migration is rolled back on failure.
        validate_required_transaction_schema(db)
        validate_required_payout_schema(db)
        validate_required_payment_setup_schema(db)
        _prevalidate_financial_schema_before_mutation(db)
        db.commit()
    except Exception:
        if db.in_transaction:
            db.rollback()
        raise


def init_db():
    db = get_db()
    try:
        _init_db_connection(db)
    except Exception:
        db.rollback()
        raise
    finally:
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
    _request_ctx.response_status = int(status)
    print(f"Status: {status}")
    print("Content-Type: application/json")
    print()
    print(json.dumps(data, default=str))


def error_response(message, status=400):
    json_response({"error": message}, status)


class JsonDecimalToken(float):
    """Float-compatible JSON number that retains its original request token."""

    raw: str

    def __new__(cls, raw):
        value = super().__new__(cls, raw)
        value.raw = raw
        return value

    def __str__(self):
        return self.raw


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
                # Preserve the exact lexical representation of JSON decimals. Money and
                # quantity validators must see `1e2` and hidden sub-cent tails rather
                # than a lossy binary-float normalization.
                parsed = json.loads(raw, parse_float=JsonDecimalToken, parse_constant=str)
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
            or not re.fullmatch(r"[+-]?[0-9]+(?:\.[0-9]{1,2})?", text)):
        raise ValueError(f"{field_name} must be a valid amount in whole cents")
    try:
        decimal_value = Decimal(text)
    except (InvalidOperation, TypeError, ValueError):
        raise ValueError(f"{field_name} must be a valid amount in whole cents")
    if not decimal_value.is_finite() or decimal_value.copy_abs() > MAX_MONEY_ABS:
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


def bounded_integer(value, field_name, minimum, maximum):
    """Parse a canonical JSON integer or ASCII integer string without float normalization."""
    if isinstance(value, bool):
        raise ValueError(f"{field_name} must be a whole number between {minimum} and {maximum}")
    if isinstance(value, int):
        number = value
    elif isinstance(value, str) and value == value.strip() and re.fullmatch(r"(?:0|[1-9][0-9]*)", value):
        number = int(value)
    else:
        raise ValueError(f"{field_name} must be a whole number between {minimum} and {maximum}")
    if number < minimum or number > maximum:
        raise ValueError(f"{field_name} must be a whole number between {minimum} and {maximum}")
    return number


def canonical_decimal_quantity(value, field_name, maximum=Decimal("10000")):
    """Parse a bounded canonical ASCII decimal quantity without lossy float normalization."""
    text = str(value)
    if (not text or text != text.strip() or len(text) > MAX_MONEY_INPUT_CHARS
            or not re.fullmatch(r"[0-9]+(?:\.[0-9]+)?", text)):
        raise ValueError(f"{field_name} must be a valid positive number")
    try:
        quantity = Decimal(text)
    except (InvalidOperation, TypeError, ValueError):
        raise ValueError(f"{field_name} must be a valid positive number")
    if not quantity.is_finite() or quantity <= 0 or quantity > maximum:
        raise ValueError(f"{field_name} must be a valid positive number")
    return quantity


def rounded_product_cents(amount, quantity, field_name="amount"):
    """Multiply a cent-denominated amount by a bounded quantity and round half-up once."""
    base_cents = money_to_cents(amount, field_name)
    quantity_decimal = canonical_decimal_quantity(quantity, f"{field_name} quantity")
    _, digits, exponent = quantity_decimal.as_tuple()
    exponent = int(exponent)
    coefficient = int("".join(str(digit) for digit in digits))
    sign = -1 if base_cents < 0 else 1
    exact_product = abs(base_cents) * coefficient
    if exponent >= 0:
        rounded = exact_product * (10 ** exponent)
    else:
        divisor = 10 ** (-exponent)
        rounded, remainder = divmod(exact_product, divisor)
        if remainder * 2 >= divisor:
            rounded += 1
    return sign * rounded


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


def validated_idempotency_key(value):
    if not isinstance(value, str) or value != value.strip():
        raise ValueError("idempotency_key must be a 16-128 character operation identity")
    if not 16 <= len(value) <= 128 or not re.fullmatch(r"[A-Za-z0-9._:-]+", value):
        raise ValueError("idempotency_key must be a 16-128 character operation identity")
    return value


def service_order_creation_request_fingerprint(employer_id, service_id, body):
    """Bind a service-order identity to canonical client-controlled inputs."""
    amount_cents = None
    if body.get("amount") is not None:
        amount_cents = money_to_cents(body.get("amount"), "custom service amount")

    hours_text = None
    if body.get("hours") is not None:
        hours = canonical_decimal_quantity(body.get("hours"), "hours")
        hours_text = format(hours, "f")
        if "." in hours_text:
            hours_text = hours_text.rstrip("0").rstrip(".")
        whole, separator, fraction = hours_text.partition(".")
        whole = whole.lstrip("0") or "0"
        hours_text = whole + (separator + fraction if fraction else "")

    notes = body.get("notes", "")
    if not isinstance(notes, str) or len(notes) > MAX_ORDER_NOTES_LENGTH:
        raise ValueError(f"notes must be a string of {MAX_ORDER_NOTES_LENGTH} characters or less")

    payload = {
        "amount_cents": amount_cents,
        "employer_id": int(employer_id),
        "hours": hours_text,
        "notes": notes,
        "service_id": int(service_id),
        "version": 1,
    }
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def job_hire_creation_request_fingerprint(
    employer_id, job_id, application_id, worker_id, budget_type, budget_amount, body
):
    """Bind the one-job hire identity to the selected application and terms."""
    budget_cents = money_to_cents(budget_amount, "job budget")
    terms = None
    if budget_type == "fixed":
        supplied = body.get("milestones", [])
        if not supplied:
            supplied = [{
                "title": "Project completion",
                "description": "Full project deliverable",
                "amount": budget_cents / 100,
            }]
        if not isinstance(supplied, list):
            raise ValueError("milestones must be a list")
        terms = []
        for index, milestone in enumerate(supplied, 1):
            if not isinstance(milestone, dict):
                raise ValueError(f"milestone {index} must be an object")
            title = milestone.get("title", f"Milestone {index}")
            description = milestone.get("description", "")
            if not isinstance(title, str) or not isinstance(description, str):
                raise ValueError(f"milestone {index} title and description must be strings")
            amount_cents = money_to_cents(
                milestone.get("amount"), f"milestone {index} amount"
            )
            if amount_cents <= 0:
                raise ValueError("Milestone amounts must be greater than zero")
            terms.append({
                "amount_cents": amount_cents,
                "description": description,
                "sequence": index,
                "title": title,
            })
        if sum(item["amount_cents"] for item in terms) != budget_cents:
            raise ValueError("Milestone amounts must exactly equal the job budget in whole cents")
    elif budget_type == "hourly":
        terms = {
            "weekly_hour_cap": bounded_integer(
                body.get("weekly_hour_cap", 40), "Weekly hour cap", 1, 168
            )
        }
    else:
        raise ValueError("Unsupported job budget type")

    payload = {
        "application_id": int(application_id),
        "budget_amount_cents": budget_cents,
        "budget_type": budget_type,
        "employer_id": int(employer_id),
        "job_id": int(job_id),
        "terms": terms,
        "version": 1,
        "worker_id": int(worker_id),
    }
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


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
    user = row_to_dict(row)
    try:
        scopes = json.loads(row["scopes"] or "[]")
    except (TypeError, ValueError, json.JSONDecodeError):
        return None
    if not isinstance(scopes, list) or any(not isinstance(scope, str) for scope in scopes):
        return None
    user["auth_principal_type"] = "api_key"
    user["api_key_scopes"] = sorted(set(scopes))
    _request_ctx.authenticated_api_key_id = int(row["api_key_id"])
    user.pop('password_hash', None)
    user.pop('api_key_id', None)
    return user


def _start_api_key_accounting_intent(db, api_key_id, endpoint, method, required_scope, state="started", status_code=None):
    """Durably attribute an authenticated request before route/processor work."""
    if db.in_transaction:
        db.commit()
    request_id = "api-usage:" + secrets.token_hex(16)
    db.execute("BEGIN IMMEDIATE")
    try:
        cursor = db.execute(
            """INSERT INTO api_key_usage
               (api_key_id,request_id,endpoint,method,status_code,accounting_state,
                authorized_scope,finalized_at)
               VALUES (?,?,?,?,?,?,?,CASE WHEN ?='denied' THEN datetime('now') END)""",
            [api_key_id, request_id, endpoint, method, status_code, state,
             required_scope, state],
        )
        updated = db.execute(
            """UPDATE api_keys SET last_used_at=datetime('now'),
                      total_requests=total_requests+1 WHERE id=?""",
            [api_key_id],
        )
        if updated.rowcount != 1:
            raise RuntimeError("API key disappeared before request accounting")
        db.commit()
    except Exception:
        if db.in_transaction:
            db.rollback()
        raise
    _request_ctx.api_key_accounting_intent_id = int(cursor.lastrowid)
    return int(cursor.lastrowid)


def _finalize_api_key_accounting_intent(intent_id, status_code, response_time_ms):
    """Finalize a pre-route usage intent in a separate short transaction."""
    usage_db = get_db()
    try:
        usage_db.execute("BEGIN IMMEDIATE")
        updated = usage_db.execute(
            """UPDATE api_key_usage
               SET status_code=?,response_time_ms=?,accounting_state='completed',
                   finalized_at=datetime('now')
               WHERE id=? AND accounting_state='started'""",
            [int(status_code), int(response_time_ms), int(intent_id)],
        )
        if updated.rowcount != 1:
            raise RuntimeError("API-key accounting intent is not in started state")
        usage_db.commit()
    except Exception:
        if usage_db.in_transaction:
            usage_db.rollback()
        raise
    finally:
        usage_db.close()


def recover_abandoned_api_key_accounting(cutoff="-1 hour"):
    """Explicitly recover stale started intents without touching domain state."""
    recovery_db = get_db()
    try:
        recovery_db.execute("BEGIN IMMEDIATE")
        count = recovery_db.execute(
            """UPDATE api_key_usage
               SET accounting_state='abandoned',finalized_at=datetime('now'),
                   error_message='request process ended before post-route finalization'
               WHERE accounting_state='started'
                 AND created_at <= datetime('now', ?)""",
            [cutoff],
        ).rowcount
        recovery_db.commit()
        return count
    except Exception:
        if recovery_db.in_transaction:
            recovery_db.rollback()
        raise
    finally:
        recovery_db.close()


def authenticate(db):
    return authenticate_session(db) or authenticate_api_key(db)


ALLOWED_API_KEY_SCOPES = {
    "read", "write", "payments:setup", "payments:fund", "payments:release",
}


def _api_key_route_scope(method, path):
    if path.startswith("/api-keys"):
        return None
    if re.match(r"^/orders/\d+/(approve|complete|dispute)$", path):
        return None
    if path in ("/payments/setup-employer", "/payments/confirm-setup-employer", "/payments/setup-worker"):
        return "payments:setup"
    if path in ("/payments/prepare-order-payment", "/payments/fund-escrow"):
        return "payments:fund"
    if method == "GET":
        return "read"
    return "write"


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


def send_email(to_email, subject, html_body, idempotency_key=None):
    """Send email via Resend API, using provider dedupe when supplied."""
    if not RESEND_API_KEY:
        return False
    try:
        data = json.dumps({
            "from": EMAIL_FROM,
            "to": [to_email],
            "subject": subject,
            "html": html_body
        }).encode('utf-8')
        headers = {
            'Authorization': f'Bearer {RESEND_API_KEY}',
            'Content-Type': 'application/json',
        }
        if idempotency_key:
            headers['Idempotency-Key'] = str(idempotency_key)
        req = urllib.request.Request(
            'https://api.resend.com/emails',
            data=data,
            headers=headers,
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


def send_transactional_notification_email(db, user_id, notif_type, title, message=None, link=None, dedupe_context=None, provider_idempotency_key=None):
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
        try:
            sent = send_email(
                user['email'], subject, html_body,
                idempotency_key=provider_idempotency_key or dedupe_key,
            )
        except TypeError:
            # Compatibility for local test doubles with the historical signature.
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


def flush_transactional_notification_emails(db):
    """Claim committed outbox rows, perform I/O lock-free, then finalize briefly."""
    if db.in_transaction:
        db.commit()
    # A hard-exited sender is safely retryable because the provider sees the same
    # durable idempotency key.
    db.execute("BEGIN IMMEDIATE")
    db.execute(
        """UPDATE transactional_email_outbox
           SET state='pending',claimed_at=NULL,
               last_error='recovered abandoned sender claim'
           WHERE state='sending' AND claimed_at < datetime('now','-10 minutes')"""
    )
    db.commit()

    while True:
        db.execute("BEGIN IMMEDIATE")
        row = db.execute(
            """SELECT * FROM transactional_email_outbox
               WHERE state='pending' ORDER BY id LIMIT 1"""
        ).fetchone()
        if row is None:
            db.commit()
            return
        claimed = db.execute(
            """UPDATE transactional_email_outbox
               SET state='sending',attempts=attempts+1,claimed_at=datetime('now')
               WHERE id=? AND state='pending'""",
            [row["id"]],
        )
        db.commit()
        if claimed.rowcount != 1:
            continue

        try:
            sent = send_transactional_notification_email(
                db, row["user_id"], row["notification_type"], row["title"],
                row["message"], row["link"], row["dedupe_context"],
                provider_idempotency_key=row["dedupe_key"],
            )
            error_text = None if sent else "email provider did not confirm delivery"
        except Exception as exc:
            sent = False
            error_text = str(exc)[:500]

        # send_transactional_notification_email may have written its audit row;
        # close that short local transaction together with the outbox final state.
        db.execute(
            """UPDATE transactional_email_outbox
               SET state=?,sent_at=CASE WHEN ? THEN datetime('now') ELSE sent_at END,
                   claimed_at=NULL,last_error=? WHERE id=? AND state='sending'""",
            ["sent" if sent else "pending", 1 if sent else 0, error_text, row["id"]],
        )
        db.commit()
        if not sent:
            return


def push_notification(db, user_id, notif_type, title, message=None, link=None, email=False, email_message=None, email_dedupe=None):
    cursor = db.execute(
        "INSERT INTO notifications (user_id, type, title, message, link) VALUES (?,?,?,?,?)",
        [user_id, notif_type, title, message or "", link or ""]
    )
    if email and notif_type in TRANSACTIONAL_EMAIL_NOTIFICATION_TYPES:
        user = db.execute(
            "SELECT email FROM users WHERE id=? AND is_active=1 AND is_banned=0",
            [user_id],
        ).fetchone()
        if user and user["email"]:
            dedupe_context = (
                email_dedupe if email_dedupe is not None
                else f"{title or ''}|{message or ''}"
            )
            dedupe_key = hashlib.sha256(
                f"{user_id}|{notif_type}|{link or ''}|{dedupe_context}".encode("utf-8")
            ).hexdigest()
            db.execute(
                """INSERT OR IGNORE INTO transactional_email_outbox
                   (user_id,notification_id,email_to,notification_type,title,message,
                    link,dedupe_context,dedupe_key,state)
                   VALUES (?,?,?,?,?,?,?,?,?,'pending')""",
                [user_id, cursor.lastrowid, user["email"], notif_type, title,
                 email_message if email_message is not None else (message or ""),
                 link or "", str(dedupe_context), dedupe_key],
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


class PaymentSetupReconciliationRequired(ValueError):
    """A setup processor outcome is ambiguous and must not be retried."""


class PaymentSetupDefinitiveFailure(PaymentSetupReconciliationRequired):
    """The processor definitively rejected the operation before object creation."""


def _payment_setup_profile_is_frozen(db, user_id):
    """Return the first unresolved setup operation for this payment profile."""
    return db.execute(
        """SELECT id,operation_kind,error_code FROM payment_setup_operations
           WHERE user_id=? AND (status='unknown' OR manual_review_required=1)
           ORDER BY id LIMIT 1""",
        [user_id],
    ).fetchone()


def _durable_payment_setup_result(result):
    """Return the non-secret identity subset permitted in the setup ledger."""
    if not isinstance(result, dict):
        raise RuntimeError("processor response must be an object")
    durable = {
        str(key): value
        for key, value in result.items()
        if (key in {"processor_object_id", "generation", "expires_at"} or str(key).endswith("_id"))
        and (value is None or isinstance(value, (str, int)))
    }
    if not durable.get("processor_object_id"):
        raise RuntimeError("processor response lacks a durable object identifier")
    return durable


_payment_setup_inflight_registry_lock = threading.Lock()
_payment_setup_inflight_operations = {}


def _payment_setup_operation(
    db, user_id, operation_kind, binding, processor_call, result_builder,
    apply_result=None, replay_processor_call=None, replay_result_builder=None,
):
    """Serialize one exact setup identity in-process; durable state handles restarts."""
    binding_json = json.dumps(binding, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    fingerprint = hashlib.sha256(binding_json.encode("utf-8")).hexdigest()
    operation_key = f"payment-setup:{user_id}:{operation_kind}:{fingerprint}"
    with _payment_setup_inflight_registry_lock:
        entry = _payment_setup_inflight_operations.get(operation_key)
        if entry is None:
            entry = [threading.Lock(), 0]
            _payment_setup_inflight_operations[operation_key] = entry
        entry[1] += 1
    try:
        with entry[0]:
            return _payment_setup_operation_serialized(
                db, user_id, operation_kind, binding, processor_call, result_builder,
                apply_result=apply_result,
                replay_processor_call=replay_processor_call,
                replay_result_builder=replay_result_builder,
            )
    finally:
        with _payment_setup_inflight_registry_lock:
            entry[1] -= 1
            if entry[1] == 0:
                del _payment_setup_inflight_operations[operation_key]


def _payment_setup_operation_serialized(
    db, user_id, operation_kind, binding, processor_call, result_builder,
    apply_result=None, replay_processor_call=None, replay_result_builder=None,
):
    """Run one replay-safe setup operation without a SQLite writer over I/O."""
    binding_json = json.dumps(binding, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    fingerprint = hashlib.sha256(binding_json.encode("utf-8")).hexdigest()
    operation_key = f"payment-setup:{user_id}:{operation_kind}:{fingerprint}"
    processor_key = f"{operation_key}:v1"

    if db.in_transaction:
        db.commit()
    db.execute("BEGIN IMMEDIATE")
    try:
        existing = db.execute(
            "SELECT * FROM payment_setup_operations WHERE operation_key=?",
            [operation_key],
        ).fetchone()
        if existing is not None:
            if (
                existing["request_fingerprint"] != fingerprint
                or existing["request_binding_json"] != binding_json
                or existing["processor_idempotency_key"] != processor_key
            ):
                db.rollback()
                raise PaymentSetupReconciliationRequired(
                    "Payment setup operation binding conflict requires manual review."
                )
            if existing["status"] == "committed" and not existing["manual_review_required"]:
                durable_result = json.loads(existing["result_json"] or "{}")
                db.commit()
                if replay_processor_call is None:
                    return durable_result, "replayed"
                # A replay-only retrieve/idempotent processor operation is allowed
                # only after releasing the SQLite writer. Its transient fields are
                # returned to this request and are never written back to SQLite.
                _request_ctx.processor_boundary_db = db
                try:
                    processor_result = replay_processor_call(
                        existing["processor_object_id"],
                        existing["processor_idempotency_key"],
                    )
                    replay_builder = replay_result_builder or result_builder
                    transient_result = replay_builder(processor_result)
                    transient_identity = _durable_payment_setup_result(transient_result)
                    if (
                        transient_identity["processor_object_id"]
                        != existing["processor_object_id"]
                    ):
                        raise RuntimeError("processor replay returned a different object")
                    return {**durable_result, **transient_result}, "replayed"
                except Exception:
                    raise PaymentSetupReconciliationRequired(
                        "Payment setup replay could not retrieve the exact processor object; try again later."
                    ) from None
                finally:
                    if hasattr(_request_ctx, "processor_boundary_db"):
                        delattr(_request_ctx, "processor_boundary_db")
            if existing["status"] == "prepared":
                db.execute(
                    """UPDATE payment_setup_operations
                       SET status='unknown',manual_review_required=1,
                           error_code='processor_success_before_local_finalize',
                           updated_at=datetime('now')
                       WHERE id=? AND status='prepared'""",
                    [existing["id"]],
                )
                db.commit()
                raise PaymentSetupReconciliationRequired(
                    "Payment setup process ended before local finalization; manual reconciliation is required before retry."
                )
            if existing["status"] == "failed" and not existing["manual_review_required"]:
                db.commit()
                raise PaymentSetupDefinitiveFailure(
                    "Payment setup was definitively rejected before processor object creation."
                )
            db.commit()
            raise PaymentSetupReconciliationRequired(
                "Payment setup outcome is unresolved; manual reconciliation is required before retry."
            )
        cursor = db.execute(
            """INSERT INTO payment_setup_operations
               (operation_key,operation_kind,user_id,request_fingerprint,
                request_binding_json,processor_idempotency_key,status)
               VALUES (?,?,?,?,?,?,'prepared')""",
            [operation_key, operation_kind, user_id, fingerprint, binding_json, processor_key],
        )
        operation_id = cursor.lastrowid
        db.commit()
    except Exception:
        if db.in_transaction:
            db.rollback()
        raise

    # Exposed only through request-local state so deterministic tests can prove
    # the actual route connection has no writer at every processor boundary.
    _request_ctx.processor_boundary_db = db
    processor_returned = False
    try:
        processor_result = processor_call(processor_key)
        processor_returned = True
        result = result_builder(processor_result)
        durable_result = _durable_payment_setup_result(result)
    except Exception as exc:
        if db.in_transaction:
            db.rollback()
        definitive_preoperation = bool(
            not processor_returned
            and STRIPE_PAYOUT_DEFINITIVE_PREOP_ERRORS
            and isinstance(exc, STRIPE_PAYOUT_DEFINITIVE_PREOP_ERRORS)
        )
        db.execute("BEGIN IMMEDIATE")
        try:
            db.execute(
                """UPDATE payment_setup_operations
                   SET status=?,manual_review_required=?,error_code=?,
                       updated_at=datetime('now')
                   WHERE id=? AND status='prepared'""",
                [
                    "failed" if definitive_preoperation else "unknown",
                    0 if definitive_preoperation else 1,
                    (
                        f"processor_rejected_{type(exc).__name__}"
                        if definitive_preoperation
                        else f"processor_outcome_{type(exc).__name__}"
                    ),
                    operation_id,
                ],
            )
            db.commit()
        except Exception:
            if db.in_transaction:
                db.rollback()
            raise
        if definitive_preoperation:
            raise PaymentSetupDefinitiveFailure(
                "Payment setup was definitively rejected before processor object creation."
            ) from None
        raise PaymentSetupReconciliationRequired(
            "Payment setup processor outcome is ambiguous; manual reconciliation is required."
        ) from None
    finally:
        if hasattr(_request_ctx, "processor_boundary_db"):
            delattr(_request_ctx, "processor_boundary_db")

    result_json = json.dumps(
        durable_result, sort_keys=True, separators=(",", ":"), ensure_ascii=True
    )
    try:
        db.execute("BEGIN IMMEDIATE")
        current = db.execute(
            "SELECT * FROM payment_setup_operations WHERE id=?", [operation_id]
        ).fetchone()
        if (
            current is None
            or current["status"] != "prepared"
            or current["manual_review_required"]
            or current["request_fingerprint"] != fingerprint
            or current["request_binding_json"] != binding_json
            or current["processor_idempotency_key"] != processor_key
        ):
            raise PaymentSetupReconciliationRequired(
                "Payment setup operation changed after processor I/O."
            )
        updated = db.execute(
            """UPDATE payment_setup_operations
               SET status='committed',processor_object_id=?,result_json=?,
                   committed_at=datetime('now'),updated_at=datetime('now')
               WHERE id=? AND status='prepared' AND manual_review_required=0
                 AND request_fingerprint=? AND processor_idempotency_key=?""",
            [durable_result["processor_object_id"], result_json, operation_id, fingerprint, processor_key],
        )
        if updated.rowcount != 1:
            raise PaymentSetupReconciliationRequired(
                "Payment setup result lost its exact CAS binding."
            )
        if apply_result is not None:
            applied = apply_result(db, result)
            if applied is None or getattr(applied, "rowcount", 0) != 1:
                raise PaymentSetupReconciliationRequired(
                    "Payment profile identity changed during processor I/O."
                )
        db.commit()
    except Exception:
        if db.in_transaction:
            db.rollback()
        # Processor success without an exact local commit is ambiguous. Freeze
        # the durable prepared row so retries cannot issue duplicate I/O.
        db.execute("BEGIN IMMEDIATE")
        db.execute(
            """UPDATE payment_setup_operations
               SET status='unknown',manual_review_required=1,
                   error_code=COALESCE(error_code,'post_processor_cas_failed'),
                   updated_at=datetime('now') WHERE id=? AND status='prepared'""",
            [operation_id],
        )
        db.commit()
        raise PaymentSetupReconciliationRequired(
            "Payment setup succeeded at the processor but local binding requires manual reconciliation."
        ) from None
    return result, "created"


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


def _assert_escrow_funding_conflict_free(db, order_id, milestone_id):
    holds = db.execute(
        """SELECT * FROM escrow_holds
           WHERE order_id=? AND milestone_id IS ? AND status='held'""",
        [order_id, milestone_id],
    ).fetchall()
    for hold in holds:
        if hold["funding_attempt_id"] and _funding_attempt_has_unresolved_conflict(
            db, hold["funding_attempt_id"]
        ):
            raise FundingReconciliationRequired(
                "Escrow funding has unresolved processor conflict evidence."
            )
        if _funding_obligation_has_unresolved_conflict(
            db,
            hold["funding_identity"] or f"legacy-escrow:{order_id}:{milestone_id}",
            order_id,
            milestone_id,
        ):
            raise FundingReconciliationRequired(
                "Escrow obligation has unresolved funding conflict evidence."
            )


LIVE_FUNDING_EVIDENCE_SOURCES = frozenset({
    "processor_create",
    "processor_retrieve",
    "processor_search",
    "signed_webhook",
})


def _validate_live_hold_funding_provenance(db, hold):
    """Require exact, processor-backed funding provenance before a live payout."""
    attempt_id = hold["funding_attempt_id"]
    payment_intent_id = hold["stripe_payment_intent_id"]
    if attempt_id is None or not payment_intent_id or str(payment_intent_id).startswith("pi_sim"):
        raise FundingReconciliationRequired(
            "Escrow does not have verified live funding provenance; reconciliation is required."
        )
    attempt = db.execute(
        "SELECT * FROM funding_attempts WHERE id=?", [attempt_id]
    ).fetchone()
    if attempt is None:
        raise FundingReconciliationRequired(
            "Escrow does not have verified live funding provenance; reconciliation is required."
        )
    charge = {
        "base_cents": attempt["base_amount_cents"],
        "platform_fee_cents": attempt["platform_fee_cents"],
        "processing_fee_cents": attempt["processing_fee_cents"],
        "total_cents": attempt["charged_total_cents"],
    }
    expected_fingerprint = funding_request_fingerprint(
        attempt["operation_key"],
        attempt["employer_id"],
        attempt["order_id"],
        attempt["milestone_id"],
        charge,
    )
    exact_bindings = (
        attempt["status"] == "committed"
        and attempt["error_code"] is None
        and attempt["currency"] == "usd"
        and attempt["evidence_source"] in LIVE_FUNDING_EVIDENCE_SOURCES
        and attempt["processor_evidence_at"] is not None
        and attempt["stripe_payment_intent_id"] == payment_intent_id
        and not str(attempt["stripe_payment_intent_id"]).startswith("pi_sim")
        and attempt["request_fingerprint"] == expected_fingerprint
        and attempt["order_id"] == hold["order_id"]
        and attempt["milestone_id"] == hold["milestone_id"]
        and attempt["operation_key"] == hold["funding_identity"]
        and attempt["base_amount_cents"] == hold["base_amount_cents"]
        and attempt["platform_fee_cents"] == hold["platform_fee_cents"]
        and attempt["processing_fee_cents"] == hold["processing_fee_cents"]
        and attempt["charged_total_cents"] == hold["charged_total_cents"]
        and hold["fee_policy_version"] == "component-half-up-v1"
    )
    if not exact_bindings:
        raise FundingReconciliationRequired(
            "Escrow does not have verified live funding provenance; reconciliation is required."
        )
    return attempt


PAYOUT_RELEASE_IMMUTABLE_FIELDS = (
    "operation_key", "attempt_number", "request_fingerprint",
    "processor_idempotency_key", "funding_attempt_id", "hold_id",
    "order_id", "milestone_id", "worker_id", "employer_id",
    "amount_cents", "currency", "destination_account_id",
    "expected_order_status", "expected_order_total_cents",
    "expected_current_milestone_id", "expected_milestone_status",
    "expected_milestone_amount_cents", "expected_hold_snapshot_json",
    "expected_hold_snapshot_sha256", "expected_lifecycle_snapshot_json",
    "expected_lifecycle_snapshot_sha256",
)


def _payout_json_snapshot(payload):
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    return encoded, hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _authoritative_payout_snapshots(db, hold, worker_id, destination_account_id):
    order = db.execute("SELECT * FROM orders WHERE id=?", [hold["order_id"]]).fetchone()
    if order is None or order["worker_id"] != worker_id:
        raise FundingReconciliationRequired(
            "Payout participants no longer match the funded order."
        )
    milestone = None
    if hold["milestone_id"] is not None:
        milestone = db.execute(
            "SELECT * FROM milestones WHERE id=? AND order_id=?",
            [hold["milestone_id"], hold["order_id"]],
        ).fetchone()
        if milestone is None:
            raise FundingReconciliationRequired(
                "Payout milestone no longer matches the funded order."
            )
    hold_payload = {
        field: hold[field]
        for field in (
            "id", "order_id", "milestone_id", "amount", "base_amount_cents",
            "platform_fee_cents", "processing_fee_cents", "charged_total_cents",
            "fee_policy_version", "funding_identity", "funding_attempt_id",
            "stripe_payment_intent_id", "created_at",
        )
    }
    current_milestone = db.execute(
        """SELECT id FROM milestones
           WHERE order_id=? AND status IN ('in_progress','submitted')
           ORDER BY sequence,id LIMIT 1""",
        [hold["order_id"]],
    ).fetchone()
    lifecycle_payload = {
        "order_id": order["id"],
        "order_type": order["type"],
        "order_status": order["status"],
        "order_worker_id": order["worker_id"],
        "order_employer_id": order["employer_id"],
        "order_total_cents": money_to_cents(order["total_amount"], "order total"),
        "current_milestone_id": current_milestone["id"] if current_milestone else None,
        "milestone_id": milestone["id"] if milestone else None,
        "milestone_status": milestone["status"] if milestone else None,
        "milestone_amount_cents": (
            money_to_cents(milestone["amount"], "milestone amount")
            if milestone else None
        ),
        "milestone_order_id": milestone["order_id"] if milestone else None,
        "destination_account_id": destination_account_id,
    }
    hold_json, hold_sha = _payout_json_snapshot(hold_payload)
    lifecycle_json, lifecycle_sha = _payout_json_snapshot(lifecycle_payload)
    return {
        "order": order,
        "milestone": milestone,
        "hold_json": hold_json,
        "hold_sha": hold_sha,
        "lifecycle_json": lifecycle_json,
        "lifecycle_sha": lifecycle_sha,
        "lifecycle": lifecycle_payload,
    }


def _payout_release_request_fingerprint(hold, funding_attempt, worker_id, destination, snapshots):
    payload = {
        "version": 1,
        "hold_id": hold["id"],
        "funding_attempt_id": funding_attempt["id"],
        "funding_operation_key": funding_attempt["operation_key"],
        "funding_payment_intent_id": funding_attempt["stripe_payment_intent_id"],
        "order_id": hold["order_id"],
        "milestone_id": hold["milestone_id"],
        "worker_id": worker_id,
        "employer_id": snapshots["order"]["employer_id"],
        "amount_cents": hold["base_amount_cents"],
        "currency": "usd",
        "destination_account_id": destination,
        "hold_snapshot_sha256": snapshots["hold_sha"],
        "lifecycle_snapshot_sha256": snapshots["lifecycle_sha"],
    }
    encoded, digest = _payout_json_snapshot(payload)
    return encoded, digest


def _payout_attempt_bindings_changed(current, expected):
    return any(
        current[field] != expected[field]
        for field in PAYOUT_RELEASE_IMMUTABLE_FIELDS
    )


def _insert_payout_release_conflict_evidence(
    db,
    attempt,
    conflict_type,
    observed,
    incoming_evidence_source,
    incoming_transfer_id=None,
    incoming_processor_status=None,
):
    expected_payload = {
        field: attempt[field] for field in PAYOUT_RELEASE_IMMUTABLE_FIELDS
    }
    expected_json, expected_sha = _payout_json_snapshot(expected_payload)
    observed_json, observed_sha = _payout_json_snapshot(observed)
    normalized_payload = {
        "attempt_id": attempt["id"],
        "conflict_type": conflict_type,
        "canonical_transfer_id": attempt["processor_transfer_id"],
        "incoming_transfer_id": incoming_transfer_id,
        "incoming_processor_status": incoming_processor_status,
        "incoming_evidence_source": incoming_evidence_source,
        "expected_snapshot_sha256": expected_sha,
        "observed_snapshot_sha256": observed_sha,
    }
    normalized_json, evidence_key = _payout_json_snapshot(normalized_payload)
    if db.execute(
        "SELECT 1 FROM payout_release_conflict_evidence WHERE evidence_key=?",
        [evidence_key],
    ).fetchone():
        return evidence_key
    try:
        db.execute(
            """INSERT INTO payout_release_conflict_evidence
               (evidence_key,attempt_id,conflict_type,canonical_transfer_id,
                incoming_transfer_id,incoming_processor_status,
                incoming_evidence_source,expected_snapshot_json,
                expected_snapshot_sha256,observed_snapshot_json,
                observed_snapshot_sha256,normalized_evidence_json)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
            [
                evidence_key, attempt["id"], conflict_type,
                attempt["processor_transfer_id"], incoming_transfer_id,
                incoming_processor_status, incoming_evidence_source,
                expected_json, expected_sha, observed_json, observed_sha,
                normalized_json,
            ],
        )
    except sqlite3.IntegrityError:
        if not db.execute(
            "SELECT 1 FROM payout_release_conflict_evidence WHERE evidence_key=?",
            [evidence_key],
        ).fetchone():
            raise
    return evidence_key


def _mark_payout_attempt_manual_review(db, attempt, error_code, observed, **evidence):
    _insert_payout_release_conflict_evidence(
        db, attempt, error_code, observed, **evidence
    )
    updated = db.execute(
        """UPDATE payout_release_attempts
           SET lifecycle_status='manual_review', manual_review_required=1,
               error_code=COALESCE(error_code,?),
               error_message='Payout release requires manual reconciliation.',
               updated_at=datetime('now')
           WHERE id=? AND manual_review_required=0""",
        [error_code, attempt["id"]],
    )
    if updated.rowcount not in (0, 1):
        raise FundingReconciliationRequired("Payout manual-review freeze was not durable.")


def _prepare_payout_release_attempt(db, hold_id, amount_cents, worker_id):
    if db.in_transaction:
        db.commit()
    db.execute("BEGIN IMMEDIATE")
    try:
        hold = db.execute("SELECT * FROM escrow_holds WHERE id=?", [hold_id]).fetchone()
        if hold is None:
            raise FundingReconciliationRequired("Escrow hold disappeared before payout preparation.")
        if hold["status"] not in ("held", "released"):
            raise FundingReconciliationRequired("Escrow hold is not eligible for payout release.")
        if money_to_cents(hold["amount"], "escrow amount") != amount_cents:
            raise FundingReconciliationRequired("Escrow hold amount changed before payout preparation.")
        funding_attempt = _validate_live_hold_funding_provenance(db, hold)
        profile = db.execute(
            "SELECT payout_account_id FROM worker_profiles WHERE user_id=?", [worker_id]
        ).fetchone()
        destination = (profile["payout_account_id"] if profile else "") or ""
        if not destination or destination.startswith("acct_sim_"):
            raise ValueError("A live worker Stripe Connect payout account is required before release.")
        snapshots = _authoritative_payout_snapshots(db, hold, worker_id, destination)
        _, request_fingerprint = _payout_release_request_fingerprint(
            hold, funding_attempt, worker_id, destination, snapshots
        )
        operation_key = f"escrow-release:hold:{hold['id']}"
        existing = db.execute(
            """SELECT * FROM payout_release_attempts
               WHERE operation_key=? ORDER BY attempt_number DESC LIMIT 1""",
            [operation_key],
        ).fetchone()
        if existing is not None:
            if existing["request_fingerprint"] != request_fingerprint:
                observed = {
                    "request_fingerprint": request_fingerprint,
                    "hold_snapshot_sha256": snapshots["hold_sha"],
                    "lifecycle_snapshot_sha256": snapshots["lifecycle_sha"],
                }
                _mark_payout_attempt_manual_review(
                    db,
                    existing,
                    "payout_request_binding_conflict",
                    observed,
                    incoming_evidence_source="local_replay",
                )
                db.commit()
                raise FundingReconciliationRequired(
                    "Payout replay conflicts with the durable release request."
                )
            if existing["manual_review_required"] or existing["lifecycle_status"] == "manual_review":
                db.commit()
                raise FundingReconciliationRequired(
                    "Payout release is frozen for manual reconciliation."
                )
            if existing["status"] in ("prepared", "unknown"):
                db.commit()
                raise FundingReconciliationRequired(
                    "Payout outcome is unresolved; read-only reconciliation is required before retry."
                )
            if existing["status"] in ("processor_succeeded", "committed"):
                db.commit()
                return existing, existing["status"]
            attempt_number = existing["attempt_number"] + 1
        else:
            attempt_number = 1
        processor_key = f"{operation_key}:attempt:{attempt_number}"
        cursor = db.execute(
            """INSERT INTO payout_release_attempts
               (operation_key,attempt_number,request_fingerprint,
                processor_idempotency_key,funding_attempt_id,hold_id,order_id,
                milestone_id,worker_id,employer_id,amount_cents,currency,
                destination_account_id,expected_order_status,
                expected_order_total_cents,expected_current_milestone_id,
                expected_milestone_status,expected_milestone_amount_cents,
                expected_hold_snapshot_json,expected_hold_snapshot_sha256,
                expected_lifecycle_snapshot_json,expected_lifecycle_snapshot_sha256,
                status,lifecycle_status)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,'usd',?,?,?,?,?,?,?,?,?,?,'prepared','pending')""",
            [
                operation_key, attempt_number, request_fingerprint, processor_key,
                funding_attempt["id"], hold["id"], hold["order_id"],
                hold["milestone_id"], worker_id, snapshots["order"]["employer_id"],
                amount_cents, destination, snapshots["order"]["status"],
                snapshots["lifecycle"]["order_total_cents"],
                snapshots["lifecycle"]["current_milestone_id"],
                snapshots["lifecycle"]["milestone_status"],
                snapshots["lifecycle"]["milestone_amount_cents"],
                snapshots["hold_json"], snapshots["hold_sha"],
                snapshots["lifecycle_json"], snapshots["lifecycle_sha"],
            ],
        )
        attempt_id = cursor.lastrowid
        db.commit()
    except Exception:
        if db.in_transaction:
            db.rollback()
        raise
    return db.execute(
        "SELECT * FROM payout_release_attempts WHERE id=?", [attempt_id]
    ).fetchone(), "prepared"


def _finish_payout_attempt_without_transfer(db, attempt, error_code):
    if db.in_transaction:
        db.commit()
    db.execute("BEGIN IMMEDIATE")
    try:
        updated = db.execute(
            """UPDATE payout_release_attempts
               SET status='failed', lifecycle_status='completed', error_code=?,
                   error_message='Payout processor was not called.',
                   updated_at=datetime('now'), lifecycle_completed_at=datetime('now')
               WHERE id=? AND status='prepared' AND manual_review_required=0""",
            [error_code, attempt["id"]],
        )
        if updated.rowcount != 1:
            raise FundingReconciliationRequired(
                "Payout attempt changed before the pre-processor failure was recorded."
            )
        db.commit()
    except Exception:
        if db.in_transaction:
            db.rollback()
        raise


def _record_ambiguous_payout_outcome(db, attempt, error_code, observed):
    if db.in_transaction:
        db.commit()
    db.execute("BEGIN IMMEDIATE")
    try:
        current = db.execute(
            "SELECT * FROM payout_release_attempts WHERE id=?", [attempt["id"]]
        ).fetchone()
        if current is None or _payout_attempt_bindings_changed(current, attempt):
            raise FundingReconciliationRequired(
                "Payout attempt bindings changed while recording an ambiguous outcome."
            )
        updated = db.execute(
            """UPDATE payout_release_attempts
               SET status='unknown', lifecycle_status='manual_review',
                   manual_review_required=1, error_code=?,
                   error_message='Processor outcome requires read-only reconciliation.',
                   evidence_source='processor_exception', updated_at=datetime('now')
               WHERE id=? AND status='prepared' AND manual_review_required=0""",
            [error_code, attempt["id"]],
        )
        if updated.rowcount != 1:
            raise FundingReconciliationRequired(
                "Payout attempt changed before the ambiguous outcome was frozen."
            )
        current = db.execute(
            "SELECT * FROM payout_release_attempts WHERE id=?", [attempt["id"]]
        ).fetchone()
        _insert_payout_release_conflict_evidence(
            db,
            current,
            error_code,
            observed,
            "processor_exception",
        )
        db.commit()
    except Exception:
        if db.in_transaction:
            db.rollback()
        raise


def _payout_current_snapshot_matches(db, attempt, required_hold_status):
    hold = db.execute(
        "SELECT * FROM escrow_holds WHERE id=?", [attempt["hold_id"]]
    ).fetchone()
    if hold is None:
        return False, {"missing_hold_id": attempt["hold_id"]}, None
    try:
        snapshots = _authoritative_payout_snapshots(
            db, hold, attempt["worker_id"], attempt["destination_account_id"]
        )
    except Exception as exc:
        return False, {"snapshot_error": type(exc).__name__}, hold
    profile = db.execute(
        "SELECT payout_account_id FROM worker_profiles WHERE user_id=?",
        [attempt["worker_id"]],
    ).fetchone()
    observed = {
        "hold_status": hold["status"],
        "hold_release_attempt_id": hold["release_attempt_id"],
        "hold_transfer_id": hold["stripe_transfer_id"],
        "hold_snapshot_sha256": snapshots["hold_sha"],
        "lifecycle_snapshot_sha256": snapshots["lifecycle_sha"],
        "destination_account_id": (
            (profile["payout_account_id"] if profile else "") or ""
        ),
    }
    matches = (
        hold["status"] == required_hold_status
        and snapshots["hold_sha"] == attempt["expected_hold_snapshot_sha256"]
        and snapshots["lifecycle_sha"] == attempt["expected_lifecycle_snapshot_sha256"]
        and observed["destination_account_id"] == attempt["destination_account_id"]
    )
    return matches, observed, hold


def _record_payout_processor_success(db, prepared_attempt, transfer):
    transfer_id = stripe_attr(transfer, "id", "") or ""
    if db.in_transaction:
        db.commit()
    db.execute("BEGIN IMMEDIATE")
    try:
        current = db.execute(
            "SELECT * FROM payout_release_attempts WHERE id=?", [prepared_attempt["id"]]
        ).fetchone()
        if current is None:
            raise FundingReconciliationRequired(
                "Payout attempt disappeared after processor success."
            )
        owner = db.execute(
            """SELECT * FROM payout_release_attempts
               WHERE processor_transfer_id=? AND id<>?""",
            [transfer_id, prepared_attempt["id"]],
        ).fetchone()
        if owner is not None or _payout_attempt_bindings_changed(current, prepared_attempt):
            observed = {
                "incoming_transfer_id": transfer_id,
                "owner_attempt_id": owner["id"] if owner else None,
                "binding_changed": _payout_attempt_bindings_changed(current, prepared_attempt),
            }
            _mark_payout_attempt_manual_review(
                db,
                current,
                "payout_processor_binding_conflict",
                observed,
                incoming_evidence_source="processor_create",
                incoming_transfer_id=transfer_id,
                incoming_processor_status="succeeded",
            )
            if owner is not None:
                _mark_payout_attempt_manual_review(
                    db,
                    owner,
                    "payout_processor_binding_conflict",
                    observed,
                    incoming_evidence_source="processor_create",
                    incoming_transfer_id=transfer_id,
                    incoming_processor_status="succeeded",
                )
            db.commit()
            raise FundingReconciliationRequired(
                "Processor transfer conflicts with durable payout bindings."
            )
        updated = db.execute(
            """UPDATE payout_release_attempts
               SET status='processor_succeeded', processor_transfer_id=?,
                   processor_status='succeeded', evidence_source='processor_create',
                   processor_evidence_at=datetime('now'), updated_at=datetime('now')
               WHERE id=? AND status='prepared' AND manual_review_required=0
                 AND processor_transfer_id IS NULL""",
            [transfer_id, current["id"]],
        )
        if updated.rowcount != 1:
            raise FundingReconciliationRequired(
                "Payout attempt changed before processor success was recorded."
            )
        current = db.execute(
            "SELECT * FROM payout_release_attempts WHERE id=?", [current["id"]]
        ).fetchone()
        matches, observed, _ = _payout_current_snapshot_matches(db, current, "held")
        if not matches:
            _mark_payout_attempt_manual_review(
                db,
                current,
                "payout_lifecycle_conflict",
                observed,
                incoming_evidence_source="processor_create",
                incoming_transfer_id=transfer_id,
                incoming_processor_status="succeeded",
            )
            db.commit()
            raise FundingReconciliationRequired(
                "Lifecycle changed after processor transfer; manual reconciliation is required."
            )
        db.commit()
    except Exception:
        if db.in_transaction:
            db.rollback()
        raise
    return db.execute(
        "SELECT * FROM payout_release_attempts WHERE id=?", [prepared_attempt["id"]]
    ).fetchone()


def _commit_payout_release_attempt(db, attempt):
    if db.in_transaction:
        db.commit()
    db.execute("BEGIN IMMEDIATE")
    try:
        current = db.execute(
            "SELECT * FROM payout_release_attempts WHERE id=?", [attempt["id"]]
        ).fetchone()
        if current is None or _payout_attempt_bindings_changed(current, attempt):
            raise FundingReconciliationRequired(
                "Payout bindings changed before local release settlement."
            )
        if (
            current["status"] != "processor_succeeded"
            or current["manual_review_required"]
            or not current["processor_transfer_id"]
        ):
            raise FundingReconciliationRequired(
                "Payout processor success is not eligible for local settlement."
            )
        matches, observed, hold = _payout_current_snapshot_matches(db, current, "held")
        if not matches or hold["release_attempt_id"] is not None:
            _mark_payout_attempt_manual_review(
                db,
                current,
                "payout_local_commit_conflict",
                observed,
                incoming_evidence_source="local_commit",
                incoming_transfer_id=current["processor_transfer_id"],
                incoming_processor_status=current["processor_status"],
            )
            db.commit()
            raise FundingReconciliationRequired(
                "Payout lifecycle changed before local settlement; manual reconciliation is required."
            )
        updated_hold = db.execute(
            """UPDATE escrow_holds
               SET status='released',stripe_transfer_id=?,release_attempt_id=?,
                   released_at=datetime('now')
               WHERE id=? AND status='held' AND release_attempt_id IS NULL
                 AND order_id=? AND milestone_id IS ? AND amount IS ?
                 AND base_amount_cents IS ? AND platform_fee_cents IS ?
                 AND processing_fee_cents IS ? AND charged_total_cents IS ?
                 AND fee_policy_version IS ? AND funding_identity IS ?
                 AND funding_attempt_id IS ? AND stripe_payment_intent_id IS ?
                 AND created_at IS ?""",
            [
                current["processor_transfer_id"], current["id"], hold["id"],
                hold["order_id"], hold["milestone_id"], hold["amount"],
                hold["base_amount_cents"], hold["platform_fee_cents"],
                hold["processing_fee_cents"], hold["charged_total_cents"],
                hold["fee_policy_version"], hold["funding_identity"],
                hold["funding_attempt_id"], hold["stripe_payment_intent_id"],
                hold["created_at"],
            ],
        )
        if updated_hold.rowcount != 1:
            raise FundingReconciliationRequired(
                "Escrow hold changed during exact payout settlement."
            )
        payout_amount = current["amount_cents"] / 100
        transfer_object = {"id": current["processor_transfer_id"]}
        record_payout_transfer(
            db,
            current["order_id"],
            current["milestone_id"],
            current["worker_id"],
            payout_amount,
            "escrow_release",
            current["processor_idempotency_key"],
            current["destination_account_id"],
            transfer_object,
            release_attempt_id=current["id"],
        )
        fee = component_fee_cents(current["amount_cents"], PLATFORM_FEE_BPS) / 100
        db.execute(
            "INSERT INTO platform_revenue (order_id,fee_amount,fee_type) VALUES (?,?,?)",
            [current["order_id"], fee, "service_fee"],
        )
        updated_attempt = db.execute(
            """UPDATE payout_release_attempts
               SET status='committed', committed_at=datetime('now'),
                   updated_at=datetime('now')
               WHERE id=? AND status='processor_succeeded'
                 AND lifecycle_status='pending' AND manual_review_required=0
                 AND processor_transfer_id=?""",
            [current["id"], current["processor_transfer_id"]],
        )
        if updated_attempt.rowcount != 1:
            raise FundingReconciliationRequired(
                "Payout attempt changed during exact local settlement."
            )
        db.commit()
    except Exception:
        if db.in_transaction:
            db.rollback()
        raise
    return db.execute(
        "SELECT * FROM payout_release_attempts WHERE id=?", [attempt["id"]]
    ).fetchone()


def _validate_committed_payout_replay(db, attempt):
    if attempt["status"] != "committed" or attempt["manual_review_required"]:
        raise FundingReconciliationRequired("Payout release is not safely replayable.")
    matches, observed, hold = _payout_current_snapshot_matches(db, attempt, "released")
    if (
        not matches
        or hold["release_attempt_id"] != attempt["id"]
        or hold["stripe_transfer_id"] != attempt["processor_transfer_id"]
    ):
        raise FundingReconciliationRequired(
            "Released escrow no longer matches the durable payout attempt."
        )
    transfer = db.execute(
        "SELECT * FROM payout_transfers WHERE release_attempt_id=?",
        [attempt["id"]],
    ).fetchone()
    if (
        transfer is None
        or transfer["stripe_transfer_id"] != attempt["processor_transfer_id"]
        or transfer["idempotency_key"] != attempt["processor_idempotency_key"]
        or transfer["destination_account_id"] != attempt["destination_account_id"]
        or money_to_cents(transfer["amount"], "payout transfer amount")
        != attempt["amount_cents"]
    ):
        raise FundingReconciliationRequired(
            "Durable payout transfer does not match released escrow."
        )
    _validate_live_hold_funding_provenance(db, hold)
    return hold


def _acquire_order_lifecycle_write_gate(db, order_id):
    """Serialize lifecycle writes with durable payout release intents."""
    if db.in_transaction:
        db.commit()
    db.execute("BEGIN IMMEDIATE")
    order = db.execute("SELECT * FROM orders WHERE id=?", [order_id]).fetchone()
    gate = db.execute(
        """SELECT id,status,lifecycle_status,manual_review_required
           FROM payout_release_attempts
           WHERE order_id=? AND lifecycle_status<>'completed'
           ORDER BY id LIMIT 1""",
        [order_id],
    ).fetchone()
    if gate is not None:
        db.rollback()
    return order, gate


def _release_live_escrow_with_attempt(db, hold, amount_cents, worker_id):
    attempt, mode = _prepare_payout_release_attempt(
        db, hold["id"], amount_cents, worker_id
    )
    if mode == "committed":
        _validate_committed_payout_replay(db, attempt)
        return amount_cents / 100, component_fee_cents(amount_cents, PLATFORM_FEE_BPS) / 100
    if mode == "processor_succeeded":
        committed = _commit_payout_release_attempt(db, attempt)
        _validate_committed_payout_replay(db, committed)
        return amount_cents / 100, component_fee_cents(amount_cents, PLATFORM_FEE_BPS) / 100

    try:
        account = retrieve_live_connect_account(attempt["destination_account_id"])
    except Exception:
        _finish_payout_attempt_without_transfer(db, attempt, "account_retrieval_failed")
        raise ValueError("Worker Stripe Connect account could not be verified.") from None
    if not is_live_connect_account_ready(account):
        _finish_payout_attempt_without_transfer(db, attempt, "account_not_payout_ready")
        raise ValueError("Worker Stripe Connect account is not payout-ready.")

    metadata = {
        "order_id": str(attempt["order_id"]),
        "milestone_id": str(attempt["milestone_id"] or ""),
        "hold_id": str(attempt["hold_id"]),
        "payout_release_attempt_id": str(attempt["id"]),
        "request_fingerprint": attempt["request_fingerprint"],
    }
    try:
        transfer = stripe.Transfer.create(
            amount=attempt["amount_cents"],
            currency=attempt["currency"],
            destination=attempt["destination_account_id"],
            metadata=metadata,
            description=f"GoHireHumans escrow release order #{attempt['order_id']}",
            idempotency_key=attempt["processor_idempotency_key"],
        )
    except STRIPE_ERROR as exc:
        if isinstance(exc, STRIPE_PAYOUT_DEFINITIVE_PREOP_ERRORS):
            _finish_payout_attempt_without_transfer(db, attempt, "processor_preoperation_failure")
            raise ValueError("Stripe rejected the payout before creating a transfer.") from None
        _record_ambiguous_payout_outcome(
            db,
            attempt,
            "processor_outcome_ambiguous",
            {"exception_type": type(exc).__name__},
        )
        raise FundingReconciliationRequired(
            "Payout outcome is ambiguous; read-only reconciliation is required before retry."
        ) from None
    except Exception as exc:
        _record_ambiguous_payout_outcome(
            db,
            attempt,
            "processor_outcome_unclassified",
            {"exception_type": type(exc).__name__},
        )
        raise FundingReconciliationRequired(
            "Payout outcome is unclassified; read-only reconciliation is required before retry."
        ) from None

    transfer_id = stripe_attr(transfer, "id", "") or ""

    def payout_evidence_mismatches(evidence):
        mismatches = []
        observed_amount = stripe_attr(evidence, "amount", None)
        try:
            observed_amount = int(observed_amount) if observed_amount is not None else None
        except (TypeError, ValueError):
            pass
        if observed_amount != int(attempt["amount_cents"]):
            mismatches.append("amount")
        observed_currency = stripe_attr(evidence, "currency", None)
        if not isinstance(observed_currency, str) or observed_currency.lower() != attempt["currency"]:
            mismatches.append("currency")
        observed_destination = stripe_attr(evidence, "destination", None)
        if observed_destination is not None and not isinstance(observed_destination, str):
            observed_destination = stripe_attr(observed_destination, "id", None)
        if observed_destination != attempt["destination_account_id"]:
            mismatches.append("destination")
        returned_metadata = stripe_attr(evidence, "metadata", None)
        observed_metadata = {
            key: stripe_attr(returned_metadata, key, None) for key in metadata
        } if returned_metadata is not None else None
        if observed_metadata != metadata:
            mismatches.append("metadata")
        if stripe_attr(evidence, "id", "") != transfer_id:
            mismatches.append("id")
        return mismatches

    mismatches = payout_evidence_mismatches(transfer) if transfer_id else ["id"]
    evidence = transfer
    if transfer_id and mismatches:
        retrieve = getattr(getattr(stripe, "Transfer", None), "retrieve", None)
        if callable(retrieve):
            try:
                evidence = retrieve(transfer_id)
                mismatches = payout_evidence_mismatches(evidence)
            except Exception as exc:
                _record_ambiguous_payout_outcome(
                    db, attempt, "processor_evidence_retrieve_failed",
                    {"transfer_id": transfer_id, "exception_type": type(exc).__name__},
                )
                raise FundingReconciliationRequired(
                    "Payout create succeeded but exact evidence retrieval failed; manual review is required."
                ) from None
    if not transfer_id or mismatches:
        _record_ambiguous_payout_outcome(
            db,
            attempt,
            "processor_evidence_mismatch",
            {"transfer_id": transfer_id, "mismatches": mismatches},
        )
        raise FundingReconciliationRequired(
            "Payout processor evidence is incomplete or does not exactly match the durable request."
        )
    succeeded = _record_payout_processor_success(db, attempt, evidence)
    committed = _commit_payout_release_attempt(db, succeeded)
    _validate_committed_payout_replay(db, committed)
    return amount_cents / 100, component_fee_cents(amount_cents, PLATFORM_FEE_BPS) / 100


def release_escrow_to_worker(db, order_id, milestone_id, amount, worker_id):
    """Release exactly one hold without keeping a SQLite writer over processor I/O."""
    if db.in_transaction:
        db.commit()

    amount_cents = money_to_cents(amount, "escrow amount")
    fee = component_fee_cents(amount_cents, PLATFORM_FEE_BPS) / 100
    worker_payout = amount_cents / 100
    holds = db.execute(
        """SELECT * FROM escrow_holds
           WHERE order_id=? AND milestone_id IS ? AND status IN ('held','released')
           ORDER BY id""",
        [order_id, milestone_id],
    ).fetchall()
    if len(holds) != 1:
        raise ValueError("Escrow release requires exactly one authoritative hold.")
    hold = holds[0]
    if money_to_cents(hold["amount"], "held escrow amount") != amount_cents:
        raise ValueError("Escrow hold amount no longer matches the release amount.")

    live_release = stripe_configured() or PRODUCTION_MODE
    # Exact replay has precedence over ordinary held-escrow eligibility. A
    # crash may occur after the processor transfer and exact local hold/attempt
    # commit but before the enclosing approval lifecycle commits. In that
    # state the released hold is only replayable through its durable attempt;
    # it must never fall through to a second Transfer.create.
    if hold["status"] == "released":
        if not live_release:
            raise ValueError("Escrow hold is not eligible for release.")
        return _release_live_escrow_with_attempt(db, hold, amount_cents, worker_id)
    if hold["status"] != "held":
        raise ValueError("Escrow hold is not eligible for release.")
    _assert_escrow_funding_conflict_free(db, order_id, milestone_id)

    if live_release:
        return _release_live_escrow_with_attempt(db, hold, amount_cents, worker_id)
    # Non-live test/development release. Every live release returned through the
    # durable payout-attempt ledger above; no direct processor transfer fallback
    # exists here.
    transfer_id = f"tr_sim_{secrets.token_hex(10)}"

    db.execute("BEGIN IMMEDIATE")
    try:
        current = db.execute(
            "SELECT * FROM escrow_holds WHERE id=?", [hold["id"]]
        ).fetchone()
        if current is None:
            raise FundingReconciliationRequired(
                "Escrow hold disappeared after processor transfer."
            )
        if (
            current["status"] == "released"
            and current["stripe_transfer_id"] == transfer_id
            and money_to_cents(current["amount"], "released escrow amount") == amount_cents
        ):
            db.commit()
            return worker_payout, fee

        _assert_escrow_funding_conflict_free(db, order_id, milestone_id)
        immutable_fields = (
            "order_id", "milestone_id", "amount", "base_amount_cents",
            "platform_fee_cents", "processing_fee_cents", "charged_total_cents",
            "fee_policy_version", "funding_identity", "funding_attempt_id",
            "stripe_payment_intent_id", "created_at",
        )
        if (
            current["status"] != "held"
            or any(current[field] != hold[field] for field in immutable_fields)
        ):
            raise FundingReconciliationRequired(
                "Escrow hold changed after processor transfer."
            )
        updated = db.execute(
            """UPDATE escrow_holds
               SET status='released',stripe_transfer_id=?,released_at=datetime('now')
               WHERE id=? AND order_id=? AND milestone_id IS ? AND amount IS ?
                 AND base_amount_cents IS ? AND platform_fee_cents IS ?
                 AND processing_fee_cents IS ? AND charged_total_cents IS ?
                 AND fee_policy_version IS ? AND funding_identity IS ?
                 AND funding_attempt_id IS ? AND stripe_payment_intent_id IS ?
                 AND created_at IS ? AND status='held' AND stripe_transfer_id IS ?""",
            [
                transfer_id,
                hold["id"], hold["order_id"], hold["milestone_id"], hold["amount"],
                hold["base_amount_cents"], hold["platform_fee_cents"],
                hold["processing_fee_cents"], hold["charged_total_cents"],
                hold["fee_policy_version"], hold["funding_identity"],
                hold["funding_attempt_id"], hold["stripe_payment_intent_id"],
                hold["created_at"], hold["stripe_transfer_id"],
            ],
        )
        if updated.rowcount != 1:
            latest = db.execute(
                "SELECT status,stripe_transfer_id,amount FROM escrow_holds WHERE id=?",
                [hold["id"]],
            ).fetchone()
            if not (
                latest
                and latest["status"] == "released"
                and latest["stripe_transfer_id"] == transfer_id
                and money_to_cents(latest["amount"], "released escrow amount") == amount_cents
            ):
                raise FundingReconciliationRequired(
                    "Escrow hold changed during exact release settlement."
                )
        db.execute(
            "INSERT INTO platform_revenue (order_id,fee_amount,fee_type) VALUES (?,?,?)",
            [order_id, fee, "service_fee"],
        )
        db.commit()
        return worker_payout, fee
    except Exception:
        if db.in_transaction:
            db.rollback()
        raise


class FundingPaymentFailed(ValueError):
    """A processor returned a definitive failure; a new numbered attempt is safe."""


class FundingConflict(ValueError):
    """A stable operation key was reused with different authoritative inputs."""


class FundingReconciliationRequired(ValueError):
    """The processor outcome is ambiguous; another charge must not be attempted."""


def funding_error_response(exc):
    if isinstance(exc, (FundingConflict, FundingReconciliationRequired)):
        return error_response(str(exc), 409)
    return error_response(str(exc), 402)


def funding_request_fingerprint(operation_key, employer_id, order_id, milestone_id, charge):
    """Return a canonical fingerprint for the financial commitment, not card details."""
    payload = {
        "base_amount_cents": int(charge["base_cents"]),
        "charged_total_cents": int(charge["total_cents"]),
        "currency": "usd",
        "employer_id": int(employer_id),
        "fee_policy_version": "component-half-up-v1",
        "milestone_id": None if milestone_id is None else int(milestone_id),
        "operation_key": str(operation_key),
        "order_id": int(order_id),
        "platform_fee_cents": int(charge["platform_fee_cents"]),
        "processing_fee_cents": int(charge["processing_fee_cents"]),
    }
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _funding_attempt_metadata(attempt):
    return {
        "funding_identity": str(attempt["operation_key"]),
        "funding_request_fingerprint": str(attempt["request_fingerprint"]),
        "funding_attempt_id": str(attempt["id"]),
        "funding_attempt_number": str(attempt["attempt_number"]),
        "order_id": str(attempt["order_id"]),
        "milestone_id": "" if attempt["milestone_id"] is None else str(attempt["milestone_id"]),
        "employer_id": str(attempt["employer_id"]),
    }


def _funding_processor_error_code(exc):
    name = type(exc).__name__
    return re.sub(r"[^A-Za-z0-9_.-]", "", name)[:80] or "ProcessorError"


def _funding_processor_error_is_ambiguous(exc):
    """Fail closed: only processor errors known to precede any charge are retryable."""
    name = type(exc).__name__.lower()
    definitive_pre_operation_errors = {
        "authenticationerror",
        "carderror",
        "invalidrequesterror",
        "permissionerror",
        "signatureverificationerror",
    }
    return name not in definitive_pre_operation_errors


def _processor_intent_inspection(attempt, intent, retrieval_method):
    """Validate retrieved processor evidence against one immutable ledger row."""
    intent_id = stripe_attr(intent, "id")
    status = str(stripe_attr(intent, "status", "") or "").lower()
    metadata = stripe_attr(intent, "metadata", {}) or {}
    if not isinstance(metadata, dict):
        try:
            metadata = dict(metadata)
        except Exception:
            metadata = {}
    expected_metadata = _funding_attempt_metadata(attempt)
    mismatches = []
    if not intent_id:
        mismatches.append("processor_intent_id")
    elif attempt["stripe_payment_intent_id"] and intent_id != attempt["stripe_payment_intent_id"]:
        mismatches.append("processor_intent_id")
    try:
        if int(stripe_attr(intent, "amount", -1) or -1) != int(attempt["charged_total_cents"]):
            mismatches.append("amount")
    except (TypeError, ValueError):
        mismatches.append("amount")
    if status == "succeeded":
        try:
            if int(stripe_attr(intent, "amount_received", -1) or -1) != int(attempt["charged_total_cents"]):
                mismatches.append("amount_received")
        except (TypeError, ValueError):
            mismatches.append("amount_received")
    if str(stripe_attr(intent, "currency", "") or "").lower() != str(attempt["currency"]).lower():
        mismatches.append("currency")
    for key, expected in expected_metadata.items():
        if str(metadata.get(key, "")) != expected:
            mismatches.append(f"metadata.{key}")
    if mismatches:
        return {
            "outcome": "mismatch",
            "processor_intent_id": intent_id,
            "processor_status": status or None,
            "retrieval_method": retrieval_method,
            "mismatches": sorted(set(mismatches)),
        }
    if status == "succeeded":
        outcome = "succeeded"
    elif status in {"canceled", "requires_payment_method"}:
        outcome = "failed"
    else:
        outcome = "pending"
    return {
        "outcome": outcome,
        "processor_intent_id": intent_id,
        "processor_status": status or None,
        "retrieval_method": retrieval_method,
        "mismatches": [],
    }


def inspect_funding_attempt_processor(attempt):
    """Read Stripe only and return normalized evidence; never mutate local state."""
    if not stripe_configured():
        return {"outcome": "unavailable", "reason": "stripe_not_configured"}
    try:
        intent_id = attempt["stripe_payment_intent_id"]
        if intent_id:
            intent = stripe.PaymentIntent.retrieve(intent_id)
            return _processor_intent_inspection(attempt, intent, "retrieve")

        search_method = getattr(stripe.PaymentIntent, "search", None)
        if not callable(search_method):
            return {"outcome": "unavailable", "reason": "stripe_search_unavailable"}
        fingerprint = str(attempt["request_fingerprint"])
        result = search_method(
            query=f"metadata['funding_request_fingerprint']:'{fingerprint}'",
            limit=10,
        )
        candidates = stripe_attr(result, "data", []) or []
        matching = []
        expected = _funding_attempt_metadata(attempt)
        for candidate in candidates:
            metadata = stripe_attr(candidate, "metadata", {}) or {}
            if all(str(metadata.get(key, "")) == value for key, value in expected.items()):
                matching.append(candidate)
        if not matching:
            return {"outcome": "not_found", "retrieval_method": "search"}
        if len(matching) != 1:
            return {
                "outcome": "mismatch",
                "retrieval_method": "search",
                "mismatches": ["multiple_processor_intents"],
            }
        return _processor_intent_inspection(attempt, matching[0], "search")
    except Exception as exc:
        return {
            "outcome": "unavailable",
            "reason": _funding_processor_error_code(exc),
        }


FUNDING_ATTEMPT_IMMUTABLE_FIELDS = (
    "operation_key", "attempt_number", "request_fingerprint",
    "processor_idempotency_key", "employer_id", "order_id", "milestone_id",
    "base_amount_cents", "platform_fee_cents", "processing_fee_cents",
    "charged_total_cents", "currency",
)

MANUAL_REVIEW_FUNDING_ERROR_CODES = frozenset({
    "attempt_binding_conflict",
    "prior_attempt_binding_conflict",
    "processor_intent_conflict",
    "prior_attempt_success_conflict",
    "success_conflicts_with_newer_attempt",
})

PROCESSOR_FREE_FUNDING_ERROR_CODES = frozenset({
    "attempt_binding_conflict",
    "prior_attempt_binding_conflict",
    "processor_intent_conflict",
})


def _funding_attempt_bindings_changed(current, expected):
    return any(
        current[field] != expected[field]
        for field in FUNDING_ATTEMPT_IMMUTABLE_FIELDS
    )


def _funding_attempt_snapshot(row):
    payload = {field: row[field] for field in FUNDING_ATTEMPT_IMMUTABLE_FIELDS}
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return payload, encoded, hashlib.sha256(encoded.encode()).hexdigest()


def _insert_funding_conflict_evidence(
    db,
    *,
    attempt_id,
    conflict_type,
    expected,
    observed,
    canonical_intent_id=None,
    incoming_intent_id=None,
    incoming_processor_status=None,
    incoming_evidence_source=None,
    processor_event_id=None,
    intent_owner_attempt_id=None,
):
    """Append one exact conflict observation, deduplicating only exact redelivery."""
    expected_payload, expected_json, expected_sha = _funding_attempt_snapshot(expected)
    observed_payload, observed_json, observed_sha = _funding_attempt_snapshot(observed)
    source = incoming_evidence_source or "unknown"
    normalized = {
        "canonical_intent_id": canonical_intent_id,
        "incoming_intent_id": incoming_intent_id,
        "incoming_processor_status": incoming_processor_status,
        "incoming_evidence_source": source,
        "processor_event_id": processor_event_id,
        "intent_owner_attempt_id": intent_owner_attempt_id,
    }
    normalized_json = json.dumps(normalized, sort_keys=True, separators=(",", ":"))
    key_payload = {
        "attempt_id": int(attempt_id),
        "conflict_type": conflict_type,
        "expected_snapshot_sha256": expected_sha,
        "observed_snapshot_sha256": observed_sha,
        **normalized,
    }
    evidence_key = hashlib.sha256(
        json.dumps(key_payload, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    values = [
        evidence_key,
        attempt_id,
        conflict_type,
        expected_payload["operation_key"],
        expected_payload["order_id"],
        expected_payload["milestone_id"],
        observed_payload["operation_key"],
        observed_payload["order_id"],
        observed_payload["milestone_id"],
        canonical_intent_id,
        incoming_intent_id,
        incoming_processor_status,
        source,
        processor_event_id,
        intent_owner_attempt_id,
        expected_json,
        expected_sha,
        observed_json,
        observed_sha,
        normalized_json,
    ]
    select_sql = """SELECT evidence_key,attempt_id,conflict_type,expected_operation_key,
                  expected_order_id,expected_milestone_id,observed_operation_key,
                  observed_order_id,observed_milestone_id,canonical_intent_id,
                  incoming_intent_id,incoming_processor_status,incoming_evidence_source,
                  processor_event_id,intent_owner_attempt_id,expected_snapshot_json,
                  expected_snapshot_sha256,observed_snapshot_json,observed_snapshot_sha256,
                  normalized_evidence_json,id
           FROM funding_attempt_conflict_evidence WHERE evidence_key=?"""
    existing = db.execute(select_sql, [evidence_key]).fetchone()
    if existing is not None:
        if list(existing[:-1]) != values:
            raise FundingReconciliationRequired(
                "Funding conflict evidence key collision requires manual review."
            )
        return existing["id"]

    inserted = db.execute(
        """INSERT INTO funding_attempt_conflict_evidence
           (evidence_key,attempt_id,conflict_type,expected_operation_key,
            expected_order_id,expected_milestone_id,observed_operation_key,
            observed_order_id,observed_milestone_id,canonical_intent_id,
            incoming_intent_id,incoming_processor_status,incoming_evidence_source,
            processor_event_id,intent_owner_attempt_id,expected_snapshot_json,
            expected_snapshot_sha256,observed_snapshot_json,observed_snapshot_sha256,
            normalized_evidence_json)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        values,
    )
    row = db.execute(select_sql, [evidence_key]).fetchone()
    if row is None or list(row[:-1]) != values or inserted.rowcount != 1:
        raise FundingReconciliationRequired(
            "Funding conflict evidence could not be durably verified."
        )
    return row["id"]


def _funding_attempt_has_unresolved_conflict(db, attempt_id):
    codes = tuple(sorted(MANUAL_REVIEW_FUNDING_ERROR_CODES))
    placeholders = ",".join("?" for _ in codes)
    row = db.execute(
        f"""SELECT 1 FROM funding_attempts
            WHERE id=? AND error_code IN ({placeholders})
            UNION ALL
            SELECT 1 FROM funding_attempt_conflict_evidence
            WHERE attempt_id=? LIMIT 1""",
        [attempt_id, *codes, attempt_id],
    ).fetchone()
    return bool(row)


def _funding_obligation_has_unresolved_conflict(
    db, operation_key, order_id, milestone_id
):
    codes = tuple(sorted(MANUAL_REVIEW_FUNDING_ERROR_CODES))
    placeholders = ",".join("?" for _ in codes)
    row = db.execute(
        f"""SELECT 1
            FROM funding_attempts a
            WHERE a.error_code IN ({placeholders})
              AND (a.operation_key=? OR (a.order_id=? AND a.milestone_id IS ?))
            UNION ALL
            SELECT 1
            FROM funding_attempt_conflict_evidence e
            JOIN funding_attempts subject ON subject.id=e.attempt_id
            WHERE e.expected_operation_key=?
               OR (e.expected_order_id=? AND e.expected_milestone_id IS ?)
               OR subject.operation_key=?
               OR (subject.order_id=? AND subject.milestone_id IS ?)
            LIMIT 1""",
        [
            *codes,
            operation_key,
            order_id,
            milestone_id,
            operation_key,
            order_id,
            milestone_id,
            operation_key,
            order_id,
            milestone_id,
        ],
    ).fetchone()
    return bool(row)


def _freeze_funding_attempt_binding_conflict(
    db,
    current,
    expected,
    processor_intent_id=None,
    processor_status=None,
    evidence_source=None,
    processor_event_id=None,
):
    """Atomically preserve binding drift, processor ownership, and every observation."""
    if not db.in_transaction:
        raise FundingReconciliationRequired(
            "Funding binding conflicts require an owned writer transaction."
        )
    changed_fields = [
        field
        for field in FUNDING_ATTEMPT_IMMUTABLE_FIELDS
        if current[field] != expected[field]
    ]
    if not changed_fields:
        raise FundingReconciliationRequired(
            "Funding binding conflict recording requires changed immutable inputs."
        )

    current_id = current["id"]
    durable_intent_id = current["stripe_payment_intent_id"] or None
    incoming_intent_id = processor_intent_id or None
    intent_owner = None
    if incoming_intent_id:
        intent_owner = db.execute(
            """SELECT * FROM funding_attempts
               WHERE stripe_payment_intent_id=? ORDER BY id LIMIT 1""",
            [incoming_intent_id],
        ).fetchone()
    external_owner = bool(intent_owner and intent_owner["id"] != current_id)
    intent_conflict = bool(
        (
            durable_intent_id
            and incoming_intent_id
            and durable_intent_id != incoming_intent_id
        )
        or external_owner
    )

    related = {}
    rows = db.execute(
        """SELECT DISTINCT a.*
           FROM funding_attempts a
           LEFT JOIN escrow_holds h ON h.funding_attempt_id=a.id
           WHERE a.id!=? AND (
               a.operation_key=? OR a.processor_idempotency_key=?
               OR (a.order_id=? AND a.milestone_id IS ?)
               OR a.operation_key=? OR a.processor_idempotency_key=?
               OR (a.order_id=? AND a.milestone_id IS ?)
               OR h.funding_identity=?
               OR (h.order_id=? AND h.milestone_id IS ?)
               OR h.funding_identity=?
               OR (h.order_id=? AND h.milestone_id IS ?)
           )
           ORDER BY a.id""",
        [
            current_id,
            expected["operation_key"],
            expected["processor_idempotency_key"],
            expected["order_id"],
            expected["milestone_id"],
            current["operation_key"],
            current["processor_idempotency_key"],
            current["order_id"],
            current["milestone_id"],
            expected["operation_key"],
            expected["order_id"],
            expected["milestone_id"],
            current["operation_key"],
            current["order_id"],
            current["milestone_id"],
        ],
    ).fetchall()
    for row in rows:
        related[row["id"]] = row
    if external_owner:
        related[intent_owner["id"]] = intent_owner

    original_anchor_ids = {
        row["id"]
        for row in related.values()
        if (
            row["operation_key"] == expected["operation_key"]
            or row["processor_idempotency_key"]
            == expected["processor_idempotency_key"]
            or (
                row["order_id"] == expected["order_id"]
                and row["milestone_id"] == expected["milestone_id"]
            )
        )
    }
    hold_collision = db.execute(
        """SELECT 1 FROM escrow_holds
           WHERE funding_attempt_id IS NOT ? AND (
               funding_identity=? OR (order_id=? AND milestone_id IS ?)
           ) LIMIT 1""",
        [
            current_id,
            expected["operation_key"],
            expected["order_id"],
            expected["milestone_id"],
        ],
    ).fetchone()
    restore_allowed = not original_anchor_ids and not hold_collision

    existing_code = current["error_code"]
    if existing_code in {
        "prior_attempt_success_conflict",
        "success_conflicts_with_newer_attempt",
    }:
        conflict_code = existing_code
    elif intent_conflict:
        conflict_code = "processor_intent_conflict"
    elif existing_code in MANUAL_REVIEW_FUNDING_ERROR_CODES:
        conflict_code = existing_code
    else:
        conflict_code = "attempt_binding_conflict"

    drift_details = ", ".join(
        f"{field}:{str(current[field])[:96]}->{str(expected[field])[:96]}"
        for field in changed_fields
    )
    binding_message = (
        "Funding attempt bindings changed after processor I/O; manual reconciliation "
        f"is required ({drift_details}). Structured conflict evidence was recorded."
    )
    existing_message = current["error_message"] or ""
    error_message = (
        existing_message
        if "Structured conflict evidence was recorded" in existing_message
        else f"{existing_message} {binding_message}".strip()
    )

    affected = {current_id: current, **related}
    owner_id = intent_owner["id"] if external_owner else None
    for affected_id in sorted(affected):
        if affected_id == current_id:
            conflict_type = "binding_drift"
        elif affected_id == owner_id:
            conflict_type = "processor_intent_owner_conflict"
        elif affected_id in original_anchor_ids:
            conflict_type = "original_obligation_anchor"
        else:
            conflict_type = "drift_identity_relation"
        _insert_funding_conflict_evidence(
            db,
            attempt_id=affected_id,
            conflict_type=conflict_type,
            expected=expected,
            observed=current,
            canonical_intent_id=durable_intent_id,
            incoming_intent_id=incoming_intent_id,
            incoming_processor_status=processor_status,
            incoming_evidence_source=evidence_source,
            processor_event_id=processor_event_id,
            intent_owner_attempt_id=owner_id,
        )

    for affected_id in sorted(related):
        row = related[affected_id]
        if row["error_code"] in MANUAL_REVIEW_FUNDING_ERROR_CODES:
            related_code = row["error_code"]
        elif affected_id == owner_id:
            related_code = "processor_intent_conflict"
        else:
            related_code = "prior_attempt_binding_conflict"
        note = (
            f"Funding conflict evidence from attempt {current_id} is linked to this "
            "attempt; manual reconciliation is required."
        )
        prior_message = row["error_message"] or ""
        related_message = (
            prior_message if note in prior_message else f"{prior_message} {note}".strip()
        )
        updated = db.execute(
            """UPDATE funding_attempts
               SET error_code=?,error_message=?,updated_at=datetime('now'),
                   last_reconciled_at=datetime('now')
               WHERE id=? AND status IS ? AND stripe_payment_intent_id IS ?
                 AND error_code IS ? AND error_message IS ?""",
            [
                related_code,
                related_message,
                affected_id,
                row["status"],
                row["stripe_payment_intent_id"],
                row["error_code"],
                row["error_message"],
            ],
        )
        if updated.rowcount != 1:
            raise FundingReconciliationRequired(
                "A related funding attempt changed during conflict recording."
            )

    def mark_current_without_restoration():
        updated = db.execute(
            """UPDATE funding_attempts
               SET error_code=?,error_message=?,updated_at=datetime('now'),
                   last_reconciled_at=datetime('now')
               WHERE id=? AND status IS ? AND stripe_payment_intent_id IS ?
                 AND error_code IS ? AND error_message IS ?
                 AND operation_key IS ? AND attempt_number IS ?
                 AND request_fingerprint IS ? AND processor_idempotency_key IS ?
                 AND employer_id IS ? AND order_id IS ? AND milestone_id IS ?
                 AND base_amount_cents IS ? AND platform_fee_cents IS ?
                 AND processing_fee_cents IS ? AND charged_total_cents IS ?
                 AND currency IS ?""",
            [
                conflict_code,
                error_message,
                current_id,
                current["status"],
                current["stripe_payment_intent_id"],
                current["error_code"],
                current["error_message"],
                *[current[field] for field in FUNDING_ATTEMPT_IMMUTABLE_FIELDS],
            ],
        )
        if updated.rowcount != 1:
            raise FundingReconciliationRequired(
                "Funding attempt changed while its conflict was being recorded."
            )

    if restore_allowed:
        selected_intent_id = durable_intent_id
        if not selected_intent_id and incoming_intent_id and not external_owner:
            selected_intent_id = incoming_intent_id
        db.execute("SAVEPOINT funding_conflict_restore")
        try:
            updated = db.execute(
                """UPDATE funding_attempts
                   SET operation_key=?,attempt_number=?,request_fingerprint=?,
                       processor_idempotency_key=?,employer_id=?,order_id=?,milestone_id=?,
                       base_amount_cents=?,platform_fee_cents=?,processing_fee_cents=?,
                       charged_total_cents=?,currency=?,stripe_payment_intent_id=?,
                       error_code=?,error_message=?,updated_at=datetime('now'),
                       last_reconciled_at=datetime('now')
                   WHERE id=? AND status IS ? AND stripe_payment_intent_id IS ?
                     AND error_code IS ? AND error_message IS ?
                     AND operation_key IS ? AND attempt_number IS ?
                     AND request_fingerprint IS ? AND processor_idempotency_key IS ?
                     AND employer_id IS ? AND order_id IS ? AND milestone_id IS ?
                     AND base_amount_cents IS ? AND platform_fee_cents IS ?
                     AND processing_fee_cents IS ? AND charged_total_cents IS ?
                     AND currency IS ?""",
                [
                    *[expected[field] for field in FUNDING_ATTEMPT_IMMUTABLE_FIELDS],
                    selected_intent_id,
                    conflict_code,
                    error_message,
                    current_id,
                    current["status"],
                    current["stripe_payment_intent_id"],
                    current["error_code"],
                    current["error_message"],
                    *[current[field] for field in FUNDING_ATTEMPT_IMMUTABLE_FIELDS],
                ],
            )
            if updated.rowcount != 1:
                raise FundingReconciliationRequired(
                    "Funding attempt changed during binding restoration."
                )
        except sqlite3.IntegrityError:
            db.execute("ROLLBACK TO funding_conflict_restore")
            db.execute("RELEASE funding_conflict_restore")
            mark_current_without_restoration()
        else:
            db.execute("RELEASE funding_conflict_restore")
    else:
        mark_current_without_restoration()

    db.commit()
    raise FundingConflict(
        "Funding attempt inputs changed after processor I/O; manual reconciliation is required."
    )

def _commit_funding_attempt(db, attempt, processor_intent_id):
    """Atomically materialize a hold from a fresh, conflict-free durable attempt."""
    if db.in_transaction:
        db.rollback()
        raise FundingReconciliationRequired(
            "Funding commit requires an isolated writer transaction."
        )
    db.execute("BEGIN IMMEDIATE")
    try:
        current = db.execute(
            "SELECT * FROM funding_attempts WHERE id=?", [attempt["id"]]
        ).fetchone()
        if not current:
            raise FundingReconciliationRequired("Funding attempt disappeared before commit.")
        if _funding_attempt_bindings_changed(current, attempt):
            _freeze_funding_attempt_binding_conflict(
                db, current, attempt, processor_intent_id=processor_intent_id
            )
        if current["stripe_payment_intent_id"] != processor_intent_id:
            raise FundingReconciliationRequired(
                "Processor intent changed before local funding commit."
            )
        if _funding_attempt_has_unresolved_conflict(db, current["id"]):
            raise FundingReconciliationRequired(
                "Contradictory processor evidence requires manual reconciliation."
            )
        if _funding_obligation_has_unresolved_conflict(
            db,
            current["operation_key"],
            current["order_id"],
            current["milestone_id"],
        ):
            raise FundingReconciliationRequired(
                "Funding obligation has unresolved conflict evidence."
            )
        if current["status"] not in {"processor_succeeded", "committed"}:
            raise FundingReconciliationRequired(
                "Funding attempt is not processor-succeeded at local commit."
            )

        holds = db.execute(
            "SELECT * FROM escrow_holds WHERE funding_identity=? ORDER BY id",
            [current["operation_key"]],
        ).fetchall()
        if len(holds) > 1:
            raise FundingConflict(
                "Funding identity has multiple escrow records and requires reconciliation."
            )
        existing = holds[0] if holds else None
        if existing:
            same = (
                int(existing["order_id"]) == int(current["order_id"])
                and existing["milestone_id"] == current["milestone_id"]
                and int(existing["base_amount_cents"] or money_to_cents(existing["amount"]))
                    == int(current["base_amount_cents"])
                and existing["platform_fee_cents"] is not None
                and int(existing["platform_fee_cents"]) == int(current["platform_fee_cents"])
                and existing["processing_fee_cents"] is not None
                and int(existing["processing_fee_cents"]) == int(current["processing_fee_cents"])
                and existing["charged_total_cents"] is not None
                and int(existing["charged_total_cents"]) == int(current["charged_total_cents"])
                and existing["fee_policy_version"] == "component-half-up-v1"
                and existing["funding_attempt_id"] is not None
                and int(existing["funding_attempt_id"]) == int(current["id"])
                and existing["stripe_payment_intent_id"] == processor_intent_id
            )
            if not same:
                raise FundingConflict(
                    "Funding identity conflicts with an existing escrow operation."
                )
        elif current["status"] == "committed":
            raise FundingReconciliationRequired(
                "Committed funding attempt is missing its escrow hold."
            )
        else:
            db.execute(
                """INSERT INTO escrow_holds
                   (order_id, milestone_id, amount, base_amount_cents, platform_fee_cents,
                    processing_fee_cents, charged_total_cents, fee_policy_version,
                    funding_identity, funding_attempt_id, status, stripe_payment_intent_id)
                   VALUES (?,?,?,?,?,?,?,'component-half-up-v1',?,?,'held',?)""",
                [
                    current["order_id"], current["milestone_id"],
                    current["base_amount_cents"] / 100, current["base_amount_cents"],
                    current["platform_fee_cents"], current["processing_fee_cents"],
                    current["charged_total_cents"], current["operation_key"],
                    current["id"], processor_intent_id,
                ],
            )

        if current["status"] == "processor_succeeded":
            updated = db.execute(
                """UPDATE funding_attempts
                   SET status='committed', processor_status='succeeded',
                       evidence_source=COALESCE(evidence_source,'processor_create'),
                       processor_evidence_at=COALESCE(processor_evidence_at,datetime('now')),
                       error_code=NULL, error_message=NULL, updated_at=datetime('now'),
                       last_reconciled_at=datetime('now'),
                       committed_at=COALESCE(committed_at,datetime('now'))
                   WHERE id=? AND status='processor_succeeded'
                     AND stripe_payment_intent_id=?
                     AND COALESCE(error_code,'')=''""",
                [current["id"], processor_intent_id],
            )
            if updated.rowcount != 1:
                raise FundingReconciliationRequired(
                    "Funding attempt changed before local commit."
                )
        db.commit()
    except Exception:
        if db.in_transaction:
            db.rollback()
        raise


def _settle_committed_milestone_funding(
    db,
    expected_order,
    expected_milestone,
    processor_intent_id,
    *,
    expected_order_status,
):
    """Activate one milestone only if its committed hold still matches exact lifecycle state."""
    if db.in_transaction:
        db.rollback()
        raise FundingReconciliationRequired(
            "Funding lifecycle settlement requires an isolated writer transaction."
        )
    db.execute("BEGIN IMMEDIATE")
    try:
        order = db.execute(
            "SELECT * FROM orders WHERE id=?", [expected_order["id"]]
        ).fetchone()
        milestone = db.execute(
            "SELECT * FROM milestones WHERE id=? AND order_id=?",
            [expected_milestone["id"], expected_order["id"]],
        ).fetchone()
        rows = db.execute(
            """SELECT h.*,a.status AS attempt_status,a.operation_key,
                      a.request_fingerprint,a.processor_idempotency_key,a.employer_id,
                      a.order_id AS attempt_order_id,a.milestone_id AS attempt_milestone_id,
                      a.base_amount_cents AS attempt_base_amount_cents,
                      a.platform_fee_cents AS attempt_platform_fee_cents,
                      a.processing_fee_cents AS attempt_processing_fee_cents,
                      a.charged_total_cents AS attempt_charged_total_cents,
                      a.currency AS attempt_currency,
                      a.stripe_payment_intent_id AS attempt_processor_intent_id,
                      a.error_code AS attempt_error_code
               FROM escrow_holds h
               JOIN funding_attempts a ON a.id=h.funding_attempt_id
               WHERE h.funding_identity=? ORDER BY h.id""",
            [f"milestone:{expected_milestone['id']}"],
        ).fetchall()
        if order is None or milestone is None or len(rows) != 1:
            raise FundingReconciliationRequired(
                "Committed funding lifecycle records are incomplete or ambiguous."
            )
        settlement = rows[0]

        order_fields = (
            "id", "type", "service_id", "job_id", "worker_id", "employer_id",
            "total_amount", "creation_idempotency_key",
            "creation_request_fingerprint",
        )
        milestone_fields = (
            "id", "order_id", "title", "description", "amount", "sequence",
            "released_at",
        )
        if any(order[field] != expected_order[field] for field in order_fields):
            raise FundingReconciliationRequired(
                "Order lifecycle changed after funding committed."
            )
        if any(
            milestone[field] != expected_milestone[field]
            for field in milestone_fields
        ):
            raise FundingReconciliationRequired(
                "Milestone lifecycle changed after funding committed."
            )
        pending_state = (
            order["status"] == expected_order_status
            and milestone["status"] == "pending"
            and milestone["escrow_payment_id"] == expected_milestone["escrow_payment_id"]
            and milestone["funded_at"] == expected_milestone["funded_at"]
        )
        settled_state = (
            order["status"] == "in_progress"
            and milestone["status"] == "in_progress"
            and milestone["escrow_payment_id"] == processor_intent_id
            and milestone["funded_at"] is not None
        )
        if not pending_state and not settled_state:
            raise FundingReconciliationRequired(
                "Funding lifecycle is no longer eligible for activation."
            )

        base_cents = money_to_cents(expected_milestone["amount"], "milestone amount")
        charge = buyer_charge_breakdown_cents(base_cents / 100)
        operation_key = f"milestone:{expected_milestone['id']}"
        fingerprint = funding_request_fingerprint(
            operation_key,
            expected_order["employer_id"],
            expected_order["id"],
            expected_milestone["id"],
            charge,
        )
        exact_components = (
            base_cents,
            charge["platform_fee_cents"],
            charge["processing_fee_cents"],
            charge["total_cents"],
        )
        hold_components = (
            settlement["base_amount_cents"],
            settlement["platform_fee_cents"],
            settlement["processing_fee_cents"],
            settlement["charged_total_cents"],
        )
        attempt_components = (
            settlement["attempt_base_amount_cents"],
            settlement["attempt_platform_fee_cents"],
            settlement["attempt_processing_fee_cents"],
            settlement["attempt_charged_total_cents"],
        )
        if (
            settlement["status"] != "held"
            or settlement["attempt_status"] != "committed"
            or settlement["operation_key"] != operation_key
            or settlement["request_fingerprint"] != fingerprint
            or settlement["employer_id"] != expected_order["employer_id"]
            or settlement["attempt_order_id"] != expected_order["id"]
            or settlement["attempt_milestone_id"] != expected_milestone["id"]
            or tuple(hold_components) != exact_components
            or tuple(attempt_components) != exact_components
            or settlement["fee_policy_version"] != "component-half-up-v1"
            or settlement["stripe_payment_intent_id"] != processor_intent_id
            or settlement["attempt_processor_intent_id"] != processor_intent_id
            or settlement["attempt_currency"] != "usd"
            or settlement["attempt_error_code"] is not None
            or _funding_attempt_has_unresolved_conflict(
                db, settlement["funding_attempt_id"]
            )
            or _funding_obligation_has_unresolved_conflict(
                db, operation_key, expected_order["id"], expected_milestone["id"]
            )
        ):
            raise FundingReconciliationRequired(
                "Committed funding no longer matches exact lifecycle provenance."
            )

        if settled_state:
            return settlement

        updated_milestone = db.execute(
            """UPDATE milestones
               SET status='in_progress',escrow_payment_id=?,
                   funded_at=COALESCE(funded_at,datetime('now'))
               WHERE id=? AND order_id=? AND status='pending'
                 AND title IS ? AND description IS ? AND amount IS ? AND sequence IS ?
                 AND escrow_payment_id IS ? AND funded_at IS ? AND released_at IS ?""",
            [
                processor_intent_id,
                milestone["id"], milestone["order_id"], milestone["title"],
                milestone["description"], milestone["amount"], milestone["sequence"],
                milestone["escrow_payment_id"], milestone["funded_at"],
                milestone["released_at"],
            ],
        )
        updated_order = db.execute(
            """UPDATE orders SET status='in_progress',updated_at=datetime('now')
               WHERE id=? AND status=? AND type IS ? AND service_id IS ? AND job_id IS ?
                 AND worker_id IS ? AND employer_id IS ? AND total_amount IS ?
                 AND creation_idempotency_key IS ?
                 AND creation_request_fingerprint IS ?""",
            [
                order["id"], expected_order_status, order["type"], order["service_id"],
                order["job_id"], order["worker_id"], order["employer_id"],
                order["total_amount"], order["creation_idempotency_key"],
                order["creation_request_fingerprint"],
            ],
        )
        if updated_milestone.rowcount != 1 or updated_order.rowcount != 1:
            raise FundingReconciliationRequired(
                "Funding lifecycle changed during exact settlement."
            )
        return settlement
    except Exception:
        if db.in_transaction:
            db.rollback()
        raise


def _settle_committed_order_funding(
    db,
    expected_order,
    processor_intent_id,
    *,
    expected_order_status,
):
    """Activate an aggregate order hold only against exact post-funding provenance."""
    if db.in_transaction:
        db.rollback()
        raise FundingReconciliationRequired(
            "Aggregate funding settlement requires an isolated writer transaction."
        )
    db.execute("BEGIN IMMEDIATE")
    try:
        order = db.execute(
            "SELECT * FROM orders WHERE id=?", [expected_order["id"]]
        ).fetchone()
        operation_key = f"order:{expected_order['id']}:full"
        rows = db.execute(
            """SELECT h.*,a.status AS attempt_status,a.operation_key,
                      a.request_fingerprint,a.employer_id,
                      a.order_id AS attempt_order_id,a.milestone_id AS attempt_milestone_id,
                      a.base_amount_cents AS attempt_base_amount_cents,
                      a.platform_fee_cents AS attempt_platform_fee_cents,
                      a.processing_fee_cents AS attempt_processing_fee_cents,
                      a.charged_total_cents AS attempt_charged_total_cents,
                      a.currency AS attempt_currency,
                      a.stripe_payment_intent_id AS attempt_processor_intent_id,
                      a.error_code AS attempt_error_code
               FROM escrow_holds h
               JOIN funding_attempts a ON a.id=h.funding_attempt_id
               WHERE h.funding_identity=? ORDER BY h.id""",
            [operation_key],
        ).fetchall()
        if order is None or len(rows) != 1:
            raise FundingReconciliationRequired(
                "Committed aggregate funding records are incomplete or ambiguous."
            )
        settlement = rows[0]
        order_fields = (
            "id", "type", "service_id", "job_id", "worker_id", "employer_id",
            "total_amount", "creation_idempotency_key",
            "creation_request_fingerprint",
        )
        if any(order[field] != expected_order[field] for field in order_fields):
            raise FundingReconciliationRequired(
                "Order lifecycle changed after aggregate funding committed."
            )
        pending_state = order["status"] == expected_order_status
        settled_state = order["status"] == "in_progress"
        if not pending_state and not settled_state:
            raise FundingReconciliationRequired(
                "Order is no longer eligible for aggregate funding activation."
            )

        base_cents = money_to_cents(expected_order["total_amount"], "order total")
        charge = buyer_charge_breakdown_cents(base_cents / 100)
        fingerprint = funding_request_fingerprint(
            operation_key,
            expected_order["employer_id"],
            expected_order["id"],
            None,
            charge,
        )
        exact_components = (
            base_cents,
            charge["platform_fee_cents"],
            charge["processing_fee_cents"],
            charge["total_cents"],
        )
        if (
            settlement["status"] != "held"
            or settlement["attempt_status"] != "committed"
            or settlement["operation_key"] != operation_key
            or settlement["request_fingerprint"] != fingerprint
            or settlement["employer_id"] != expected_order["employer_id"]
            or settlement["attempt_order_id"] != expected_order["id"]
            or settlement["attempt_milestone_id"] is not None
            or settlement["milestone_id"] is not None
            or tuple((
                settlement["base_amount_cents"],
                settlement["platform_fee_cents"],
                settlement["processing_fee_cents"],
                settlement["charged_total_cents"],
            )) != exact_components
            or tuple((
                settlement["attempt_base_amount_cents"],
                settlement["attempt_platform_fee_cents"],
                settlement["attempt_processing_fee_cents"],
                settlement["attempt_charged_total_cents"],
            )) != exact_components
            or settlement["fee_policy_version"] != "component-half-up-v1"
            or settlement["stripe_payment_intent_id"] != processor_intent_id
            or settlement["attempt_processor_intent_id"] != processor_intent_id
            or settlement["attempt_currency"] != "usd"
            or settlement["attempt_error_code"] is not None
            or _funding_attempt_has_unresolved_conflict(
                db, settlement["funding_attempt_id"]
            )
            or _funding_obligation_has_unresolved_conflict(
                db, operation_key, expected_order["id"], None
            )
        ):
            raise FundingReconciliationRequired(
                "Committed aggregate funding no longer matches exact provenance."
            )
        if settled_state:
            return settlement
        updated = db.execute(
            """UPDATE orders SET status='in_progress',updated_at=datetime('now')
               WHERE id=? AND status=? AND type IS ? AND service_id IS ? AND job_id IS ?
                 AND worker_id IS ? AND employer_id IS ? AND total_amount IS ?
                 AND creation_idempotency_key IS ?
                 AND creation_request_fingerprint IS ?""",
            [
                order["id"], expected_order_status, order["type"], order["service_id"],
                order["job_id"], order["worker_id"], order["employer_id"],
                order["total_amount"], order["creation_idempotency_key"],
                order["creation_request_fingerprint"],
            ],
        )
        if updated.rowcount != 1:
            raise FundingReconciliationRequired(
                "Order lifecycle changed during exact aggregate settlement."
            )
        return settlement
    except Exception:
        if db.in_transaction:
            db.rollback()
        raise


def _record_success_conflict_with_newer_attempt(
    db,
    attempt,
    processor_intent_id,
    processor_status,
    evidence_source,
    processor_event_id=None,
):
    """Atomically freeze every row implicated by a late processor success."""
    if attempt["status"] != "failed":
        return False
    newer_attempt = db.execute(
        """SELECT * FROM funding_attempts
           WHERE id>? AND (
               operation_key=? OR (? IS NOT NULL AND milestone_id=?)
           )
           ORDER BY id DESC LIMIT 1""",
        [
            attempt["id"],
            attempt["operation_key"],
            attempt["milestone_id"],
            attempt["milestone_id"],
        ],
    ).fetchone()
    if not newer_attempt:
        return False

    intent_owner = None
    if processor_intent_id:
        intent_owner = db.execute(
            """SELECT * FROM funding_attempts
               WHERE stripe_payment_intent_id=? ORDER BY id LIMIT 1""",
            [processor_intent_id],
        ).fetchone()
    owner_id = (
        intent_owner["id"]
        if intent_owner is not None and intent_owner["id"] != attempt["id"]
        else None
    )
    external_owner = owner_id is not None
    if external_owner:
        assert intent_owner is not None

    _insert_funding_conflict_evidence(
        db,
        attempt_id=attempt["id"],
        conflict_type="success_conflicts_with_newer_attempt",
        expected=attempt,
        observed=attempt,
        canonical_intent_id=attempt["stripe_payment_intent_id"],
        incoming_intent_id=processor_intent_id,
        incoming_processor_status=processor_status,
        incoming_evidence_source=evidence_source,
        processor_event_id=processor_event_id,
        intent_owner_attempt_id=owner_id,
    )
    _insert_funding_conflict_evidence(
        db,
        attempt_id=newer_attempt["id"],
        conflict_type="prior_attempt_success_conflict",
        expected=newer_attempt,
        observed=attempt,
        canonical_intent_id=newer_attempt["stripe_payment_intent_id"],
        incoming_intent_id=processor_intent_id,
        incoming_processor_status=processor_status,
        incoming_evidence_source=evidence_source,
        processor_event_id=processor_event_id,
        intent_owner_attempt_id=owner_id,
    )
    if external_owner and owner_id != newer_attempt["id"]:
        _insert_funding_conflict_evidence(
            db,
            attempt_id=owner_id,
            conflict_type="processor_intent_owner_conflict",
            expected=intent_owner,
            observed=attempt,
            canonical_intent_id=intent_owner["stripe_payment_intent_id"],
            incoming_intent_id=processor_intent_id,
            incoming_processor_status=processor_status,
            incoming_evidence_source=evidence_source,
            processor_event_id=processor_event_id,
            intent_owner_attempt_id=owner_id,
        )

    selected_intent_id = (
        attempt["stripe_payment_intent_id"]
        if external_owner
        else processor_intent_id
    )
    subject_updated = db.execute(
        """UPDATE funding_attempts
           SET stripe_payment_intent_id=?, processor_status=?, evidence_source=?,
               processor_evidence_at=datetime('now'),
               error_code='success_conflicts_with_newer_attempt',
               error_message='This formerly failed attempt later produced success evidence after a newer attempt existed.',
               updated_at=datetime('now'), last_reconciled_at=datetime('now')
           WHERE id=? AND status IS ? AND stripe_payment_intent_id IS ?
             AND error_code IS ? AND error_message IS ?
             AND operation_key IS ? AND attempt_number IS ?
             AND request_fingerprint IS ? AND processor_idempotency_key IS ?
             AND employer_id IS ? AND order_id IS ? AND milestone_id IS ?
             AND base_amount_cents IS ? AND platform_fee_cents IS ?
             AND processing_fee_cents IS ? AND charged_total_cents IS ?
             AND currency IS ?""",
        [
            selected_intent_id,
            processor_status,
            evidence_source,
            attempt["id"],
            attempt["status"],
            attempt["stripe_payment_intent_id"],
            attempt["error_code"],
            attempt["error_message"],
            *[attempt[field] for field in FUNDING_ATTEMPT_IMMUTABLE_FIELDS],
        ],
    )
    if subject_updated.rowcount != 1:
        raise FundingReconciliationRequired(
            "Late-success funding attempt changed during conflict recording."
        )

    newer_updated = db.execute(
        """UPDATE funding_attempts
           SET status=CASE
                   WHEN status IN ('prepared','unknown','failed') THEN 'unknown'
                   ELSE status
               END,
               error_code='prior_attempt_success_conflict',
               error_message='A prior failed attempt later produced success evidence; manual reconciliation is required.',
               updated_at=datetime('now'), last_reconciled_at=datetime('now')
           WHERE id=? AND status IS ? AND stripe_payment_intent_id IS ?
             AND error_code IS ? AND error_message IS ?
             AND operation_key IS ? AND attempt_number IS ?
             AND request_fingerprint IS ? AND processor_idempotency_key IS ?
             AND employer_id IS ? AND order_id IS ? AND milestone_id IS ?
             AND base_amount_cents IS ? AND platform_fee_cents IS ?
             AND processing_fee_cents IS ? AND charged_total_cents IS ?
             AND currency IS ?""",
        [
            newer_attempt["id"],
            newer_attempt["status"],
            newer_attempt["stripe_payment_intent_id"],
            newer_attempt["error_code"],
            newer_attempt["error_message"],
            *[newer_attempt[field] for field in FUNDING_ATTEMPT_IMMUTABLE_FIELDS],
        ],
    )
    if newer_updated.rowcount != 1:
        raise FundingReconciliationRequired(
            "Newer funding attempt changed during conflict recording."
        )

    if external_owner and owner_id != newer_attempt["id"]:
        owner_note = (
            f"Processor intent conflict with attempt {attempt['id']} requires manual "
            "reconciliation; structured evidence was recorded."
        )
        prior_owner_message = intent_owner["error_message"] or ""
        owner_message = (
            prior_owner_message
            if owner_note in prior_owner_message
            else f"{prior_owner_message} {owner_note}".strip()
        )
        owner_updated = db.execute(
            """UPDATE funding_attempts
               SET error_code='processor_intent_conflict',error_message=?,
                   updated_at=datetime('now'),last_reconciled_at=datetime('now')
               WHERE id=? AND status IS ? AND stripe_payment_intent_id IS ?
                 AND error_code IS ? AND error_message IS ?""",
            [
                owner_message,
                owner_id,
                intent_owner["status"],
                intent_owner["stripe_payment_intent_id"],
                intent_owner["error_code"],
                intent_owner["error_message"],
            ],
        )
        if owner_updated.rowcount != 1:
            raise FundingReconciliationRequired(
                "Processor intent owner changed during late-success conflict recording."
            )

    db.commit()
    return True


def _record_conflicting_processor_success(db, attempt, processor_intent_id, processor_status, evidence_source):
    """Persist success evidence without clearing a manual-reconciliation freeze."""
    durable_intent_id = attempt["stripe_payment_intent_id"]
    if durable_intent_id and durable_intent_id != processor_intent_id:
        raise FundingReconciliationRequired(
            "Processor success conflicts with a different durable processor intent."
        )
    db.execute(
        """UPDATE funding_attempts
           SET stripe_payment_intent_id=?, processor_status=?, evidence_source=?,
               processor_evidence_at=datetime('now'), updated_at=datetime('now'),
               last_reconciled_at=datetime('now')
           WHERE id=? AND status='unknown'
             AND error_code='prior_attempt_success_conflict'""",
        [processor_intent_id, processor_status, evidence_source, attempt["id"]],
    )
    db.commit()


def _freeze_processor_intent_conflict(
    db,
    attempt,
    incoming_intent_id,
    processor_status=None,
    evidence_source=None,
    processor_event_id=None,
):
    """Make contradictory processor identities durable and non-retryable."""
    durable_intent_id = attempt["stripe_payment_intent_id"]
    intent_owner = db.execute(
        """SELECT * FROM funding_attempts
           WHERE stripe_payment_intent_id=? ORDER BY id LIMIT 1""",
        [incoming_intent_id],
    ).fetchone()
    external_owner = bool(intent_owner and intent_owner["id"] != attempt["id"])
    owner_id = intent_owner["id"] if external_owner else None
    durable_mismatch = bool(
        durable_intent_id and durable_intent_id != incoming_intent_id
    )
    if not durable_mismatch and not external_owner:
        return False
    affected = {attempt["id"]: attempt}
    if external_owner:
        affected[owner_id] = intent_owner
    for affected_id in sorted(affected):
        affected_attempt = affected[affected_id]
        _insert_funding_conflict_evidence(
            db,
            attempt_id=affected_id,
            conflict_type=(
                "processor_intent_owner_conflict"
                if affected_id == owner_id
                else "processor_intent_conflict"
            ),
            expected=affected_attempt,
            observed=attempt,
            canonical_intent_id=affected_attempt["stripe_payment_intent_id"],
            incoming_intent_id=incoming_intent_id,
            incoming_processor_status=processor_status,
            incoming_evidence_source=evidence_source,
            processor_event_id=processor_event_id,
            intent_owner_attempt_id=owner_id,
        )

    if external_owner:
        owner_code = (
            intent_owner["error_code"]
            if intent_owner["error_code"] in MANUAL_REVIEW_FUNDING_ERROR_CODES
            else "processor_intent_conflict"
        )
        owner_note = (
            f"Processor intent conflict with attempt {attempt['id']} requires manual "
            "reconciliation; structured evidence was recorded."
        )
        prior_owner_message = intent_owner["error_message"] or ""
        owner_message = (
            prior_owner_message
            if owner_note in prior_owner_message
            else f"{prior_owner_message} {owner_note}".strip()
        )
        owner_updated = db.execute(
            """UPDATE funding_attempts
               SET error_code=?,error_message=?,updated_at=datetime('now'),
                   last_reconciled_at=datetime('now')
               WHERE id=? AND status IS ? AND stripe_payment_intent_id IS ?
                 AND error_code IS ? AND error_message IS ?""",
            [
                owner_code,
                owner_message,
                owner_id,
                intent_owner["status"],
                intent_owner["stripe_payment_intent_id"],
                intent_owner["error_code"],
                intent_owner["error_message"],
            ],
        )
        if owner_updated.rowcount != 1:
            raise FundingReconciliationRequired(
                "Processor intent owner changed during conflict recording."
            )

    conflict_code = (
        attempt["error_code"]
        if attempt["error_code"] in {
            "prior_attempt_success_conflict",
            "success_conflicts_with_newer_attempt",
        }
        else "processor_intent_conflict"
    )
    conflict_note = (
        "Processor evidence conflicts with the canonical intent; manual "
        "reconciliation is required. Structured conflict evidence was recorded."
    )
    prior_message = attempt["error_message"] or ""
    conflict_message = (
        prior_message
        if conflict_note in prior_message
        else f"{prior_message} {conflict_note}".strip()
    )
    updated = db.execute(
        """UPDATE funding_attempts
           SET error_code=?,error_message=?,updated_at=datetime('now'),
               last_reconciled_at=datetime('now')
           WHERE id=? AND status IS ? AND stripe_payment_intent_id IS ?
             AND error_code IS ? AND error_message IS ?""",
        [
            conflict_code,
            conflict_message,
            attempt["id"],
            attempt["status"],
            durable_intent_id,
            attempt["error_code"],
            attempt["error_message"],
        ],
    )
    if updated.rowcount != 1:
        raise FundingReconciliationRequired(
            "Funding attempt changed during processor conflict recording."
        )
    db.commit()
    return True


def _reconcile_funding_attempt_owned(db, attempt, apply=False, inspection=None):
    """Share one Stripe evidence normalizer between runtime, webhooks, and read-only tooling."""
    expected_attempt = attempt
    # Release any caller-owned writer transaction before processor retrieval/search.
    if getattr(db, "in_transaction", False):
        db.commit()
    inspection = inspection or inspect_funding_attempt_processor(expected_attempt)
    if not apply:
        return inspection
    # Processor inspection happens before this point. Acquire the writer lock only
    # while applying evidence so a definitive failure cannot race attempt N+1.
    db.execute("BEGIN IMMEDIATE")
    current = db.execute(
        "SELECT * FROM funding_attempts WHERE id=?", [expected_attempt["id"]]
    ).fetchone()
    if current is None:
        db.commit()
        return {**inspection, "outcome": "ignored", "reason": "funding_attempt_not_found"}
    if _funding_attempt_bindings_changed(current, expected_attempt):
        _freeze_funding_attempt_binding_conflict(
            db,
            current,
            expected_attempt,
            inspection.get("processor_intent_id"),
            inspection.get("processor_status"),
            inspection.get("retrieval_method"),
            inspection.get("processor_event_id"),
        )
    attempt = current
    incoming_intent_id = inspection.get("processor_intent_id")
    durable_intent_id = attempt["stripe_payment_intent_id"]
    if (
        incoming_intent_id
        and durable_intent_id
        and incoming_intent_id != durable_intent_id
    ):
        _freeze_processor_intent_conflict(
            db,
            attempt,
            incoming_intent_id,
            inspection.get("processor_status"),
            inspection.get("retrieval_method"),
            inspection.get("processor_event_id"),
        )
        return {
            **inspection,
            "outcome": (
                "ignored_committed"
                if attempt["status"] == "committed"
                else "ignored_monotonic"
            ),
            "reason": "durable_processor_intent_conflict",
        }
    outcome = inspection.get("outcome")
    if attempt["status"] == "committed":
        if outcome == "succeeded":
            db.commit()
            return inspection
        db.commit()
        return {
            **inspection,
            "outcome": "ignored_committed",
            "reason": "committed_attempt_is_monotonic",
        }
    if attempt["error_code"] == "processor_intent_conflict":
        db.commit()
        return {
            **inspection,
            "outcome": "ignored_monotonic",
            "reason": "processor_intent_conflict_requires_manual_reconciliation",
        }
    if outcome == "succeeded":
        if attempt["error_code"] == "prior_attempt_success_conflict":
            _record_conflicting_processor_success(
                db,
                attempt,
                incoming_intent_id,
                inspection.get("processor_status"),
                inspection.get("retrieval_method"),
            )
            return {
                **inspection,
                "outcome": "ignored_monotonic",
                "reason": "prior_attempt_success_conflict_requires_manual_reconciliation",
            }
        if _record_success_conflict_with_newer_attempt(
            db,
            attempt,
            incoming_intent_id,
            inspection.get("processor_status"),
            inspection.get("retrieval_method"),
            inspection.get("processor_event_id"),
        ):
            return {
                **inspection,
                "outcome": "ignored_monotonic",
                "reason": "newer_attempt_exists_after_definitive_failure",
            }
        if incoming_intent_id and _freeze_processor_intent_conflict(
            db,
            attempt,
            incoming_intent_id,
            inspection.get("processor_status"),
            inspection.get("retrieval_method"),
            inspection.get("processor_event_id"),
        ):
            return {
                **inspection,
                "outcome": "ignored_monotonic",
                "reason": "processor_intent_owned_by_another_attempt",
            }
        cursor = db.execute(
            """UPDATE funding_attempts
               SET status='processor_succeeded', stripe_payment_intent_id=?, processor_status=?,
                   evidence_source=?, processor_evidence_at=datetime('now'),
                   error_code=NULL, error_message=NULL, updated_at=datetime('now'),
                   last_reconciled_at=datetime('now')
               WHERE id=? AND status IN ('prepared','unknown','failed')
             AND COALESCE(error_code,'') NOT IN (
                 'prior_attempt_success_conflict','processor_intent_conflict'
             )""",
            [
                incoming_intent_id,
                inspection.get("processor_status"),
                inspection.get("retrieval_method"),
                attempt["id"],
            ],
        )
        db.commit()
        refreshed = db.execute("SELECT * FROM funding_attempts WHERE id=?", [attempt["id"]]).fetchone()
        if cursor.rowcount == 0:
            if (
                refreshed
                and refreshed["status"] == "committed"
                and refreshed["stripe_payment_intent_id"] == incoming_intent_id
            ):
                return {
                    **inspection,
                    "outcome": "ignored_committed",
                    "reason": "committed_attempt_is_monotonic",
                }
            if (
                refreshed
                and refreshed["status"] == "processor_succeeded"
                and refreshed["stripe_payment_intent_id"] == incoming_intent_id
            ):
                _commit_funding_attempt(db, expected_attempt, incoming_intent_id)
                return inspection
            return {
                **inspection,
                "outcome": "ignored_monotonic",
                "reason": "newer_durable_attempt_evidence",
            }
        _commit_funding_attempt(db, expected_attempt, incoming_intent_id)
        return inspection
    if incoming_intent_id and _freeze_processor_intent_conflict(
        db,
        attempt,
        incoming_intent_id,
        inspection.get("processor_status"),
        inspection.get("retrieval_method"),
        inspection.get("processor_event_id"),
    ):
        return {
            **inspection,
            "outcome": "ignored_monotonic",
            "reason": "processor_intent_owned_by_another_attempt",
        }
    if outcome == "failed":
        cursor = db.execute(
            """UPDATE funding_attempts
               SET status='failed', stripe_payment_intent_id=?, processor_status=?,
                   evidence_source=?, processor_evidence_at=datetime('now'),
                   error_code='processor_definitive_failure', error_message='Processor reported a definitive failure.',
                   updated_at=datetime('now'), last_reconciled_at=datetime('now')
               WHERE id=? AND status IN ('prepared','unknown')
                 AND COALESCE(error_code,'')!='prior_attempt_success_conflict'""",
            [
                inspection.get("processor_intent_id"),
                inspection.get("processor_status"),
                inspection.get("retrieval_method"),
                attempt["id"],
            ],
        )
    else:
        cursor = db.execute(
            """UPDATE funding_attempts
               SET status='unknown', processor_status=?, evidence_source=?,
                   processor_evidence_at=datetime('now'), error_code=?,
                   error_message='Processor outcome requires reconciliation.',
                   updated_at=datetime('now'), last_reconciled_at=datetime('now')
               WHERE id=? AND status IN ('prepared','unknown')
                 AND COALESCE(error_code,'')!='prior_attempt_success_conflict'""",
            [
                inspection.get("processor_status"),
                inspection.get("retrieval_method"),
                f"reconcile_{outcome or 'unknown'}",
                attempt["id"],
            ],
        )
    db.commit()
    if cursor.rowcount == 0:
        refreshed = db.execute(
            "SELECT status FROM funding_attempts WHERE id=?", [attempt["id"]]
        ).fetchone()
        return {
            **inspection,
            "outcome": "ignored_monotonic",
            "reason": (
                f"attempt_already_{refreshed['status']}" if refreshed else "funding_attempt_not_found"
            ),
        }
    return inspection


def reconcile_funding_attempt(db, attempt, apply=False, inspection=None):
    """Run shared reconciliation and release every writer transaction on failure."""
    try:
        return _reconcile_funding_attempt_owned(
            db, attempt, apply=apply, inspection=inspection
        )
    except Exception:
        if apply and getattr(db, "in_transaction", False):
            db.rollback()
        raise


def reconcile_funding_intent_event(db, intent, processor_event_id=None):
    """Apply a signed Stripe PaymentIntent event through the shared evidence path."""
    metadata = stripe_attr(intent, "metadata", {}) or {}
    if not isinstance(metadata, dict):
        try:
            metadata = dict(metadata)
        except Exception:
            metadata = {}
    attempt = None
    raw_attempt_id = str(metadata.get("funding_attempt_id", ""))
    if raw_attempt_id.isdigit():
        attempt = db.execute(
            "SELECT * FROM funding_attempts WHERE id=?", [int(raw_attempt_id)]
        ).fetchone()
    intent_id = stripe_attr(intent, "id")
    if attempt is None and intent_id:
        attempt = db.execute(
            "SELECT * FROM funding_attempts WHERE stripe_payment_intent_id=?", [intent_id]
        ).fetchone()
    if attempt is None:
        return {"outcome": "ignored", "reason": "funding_attempt_not_found"}
    inspection = _processor_intent_inspection(attempt, intent, "signed_webhook")
    inspection["processor_event_id"] = processor_event_id
    return reconcile_funding_attempt(db, attempt, apply=True, inspection=inspection)


def fund_escrow_stripe(db, employer_id, amount, order_id, milestone_id=None, description="Escrow hold", funding_identity=None):
    """Fund escrow through a durable, fingerprinted, processor-reconcilable attempt."""
    ep = db.execute(
        "SELECT stripe_customer_id, payment_method_id FROM employer_profiles WHERE user_id=?",
        [employer_id],
    ).fetchone()
    charge = buyer_charge_breakdown_cents(amount)
    if PRODUCTION_MODE and not stripe_configured():
        raise FundingPaymentFailed(
            "Payments are temporarily unavailable because simulated escrow is disabled in production."
        )
    if milestone_id is not None:
        canonical_identity = f"milestone:{int(milestone_id)}"
        if funding_identity is not None and funding_identity != canonical_identity:
            raise FundingConflict(
                "Milestone funding must use its canonical economic-obligation identity."
            )
        funding_identity = canonical_identity
    else:
        funding_identity = funding_identity or None
    if funding_identity is None:
        raise FundingConflict("A stable funding identity is required for escrow funding.")
    fingerprint = funding_request_fingerprint(
        funding_identity, employer_id, order_id, milestone_id, charge
    )
    if _funding_obligation_has_unresolved_conflict(
        db, funding_identity, order_id, milestone_id
    ):
        raise FundingReconciliationRequired(
            "Funding obligation has unresolved processor conflict evidence and "
            "requires manual reconciliation."
        )

    existing_hold = db.execute(
        "SELECT * FROM escrow_holds WHERE funding_identity=?", [funding_identity]
    ).fetchone()
    if existing_hold:
        existing_base_cents = existing_hold["base_amount_cents"]
        if existing_base_cents is None:
            existing_base_cents = money_to_cents(existing_hold["amount"], "existing escrow amount")
        same_operation = (
            int(existing_hold["order_id"]) == int(order_id)
            and existing_hold["milestone_id"] == milestone_id
            and int(existing_base_cents) == charge["base_cents"]
            and existing_hold["platform_fee_cents"] is not None
            and int(existing_hold["platform_fee_cents"]) == charge["platform_fee_cents"]
            and existing_hold["processing_fee_cents"] is not None
            and int(existing_hold["processing_fee_cents"]) == charge["processing_fee_cents"]
            and existing_hold["charged_total_cents"] is not None
            and int(existing_hold["charged_total_cents"]) == charge["total_cents"]
            and existing_hold["fee_policy_version"] == "component-half-up-v1"
            and bool(existing_hold["stripe_payment_intent_id"])
        )
        if same_operation and existing_hold["funding_attempt_id"] is not None:
            committed_attempt = db.execute(
                "SELECT * FROM funding_attempts WHERE id=?", [existing_hold["funding_attempt_id"]]
            ).fetchone()
            if committed_attempt and _funding_attempt_has_unresolved_conflict(
                db, committed_attempt["id"]
            ):
                raise FundingReconciliationRequired(
                    "Committed funding has contradictory processor evidence and requires manual reconciliation."
                )
            same_operation = bool(
                committed_attempt
                and committed_attempt["status"] == "committed"
                and committed_attempt["request_fingerprint"] == fingerprint
                and committed_attempt["stripe_payment_intent_id"]
                    == existing_hold["stripe_payment_intent_id"]
            )
        if not same_operation:
            raise FundingConflict("Funding identity conflicts with an existing escrow operation.")
        if db.in_transaction:
            db.commit()
        return existing_hold["stripe_payment_intent_id"], "replayed"

    if milestone_id is None:
        obligation_hold = db.execute(
            """SELECT * FROM escrow_holds
               WHERE order_id=? AND milestone_id IS NULL AND funding_identity IS NULL LIMIT 1""",
            [order_id],
        ).fetchone()
    else:
        obligation_hold = db.execute(
            "SELECT * FROM escrow_holds WHERE order_id=? AND milestone_id=? LIMIT 1",
            [order_id, milestone_id],
        ).fetchone()
    if obligation_hold:
        if obligation_hold["funding_identity"] is None:
            raise FundingReconciliationRequired(
                "Existing funding must complete processor reconciliation before this operation can be retried."
            )
        raise FundingConflict(
            "This economic obligation is already funded under a different operation identity."
        )

    latest = db.execute(
        "SELECT * FROM funding_attempts WHERE operation_key=? ORDER BY attempt_number DESC LIMIT 1",
        [funding_identity],
    ).fetchone()
    if latest:
        if latest["error_code"] in PROCESSOR_FREE_FUNDING_ERROR_CODES:
            raise FundingReconciliationRequired(
                "Funding attempt has contradictory durable evidence and requires manual reconciliation."
            )
        if latest["request_fingerprint"] != fingerprint:
            raise FundingConflict("Funding identity conflicts with different authoritative inputs.")
        if latest["status"] == "committed":
            raise FundingReconciliationRequired("Committed funding is missing its escrow hold.")
        if latest["status"] in {"prepared", "unknown", "processor_succeeded"}:
            # Route-level eligibility checks may hold BEGIN IMMEDIATE. Processor
            # retrieve/search must run without any SQLite writer lock; evidence is
            # applied afterward through guarded, monotonic state transitions.
            if db.in_transaction:
                db.commit()
            inspection = reconcile_funding_attempt(db, latest, apply=True)
            refreshed = db.execute("SELECT * FROM funding_attempts WHERE id=?", [latest["id"]]).fetchone()
            if refreshed["status"] == "committed":
                return refreshed["stripe_payment_intent_id"], "reconciled"
            if refreshed["status"] != "failed":
                raise FundingReconciliationRequired(
                    "Funding outcome is ambiguous; processor reconciliation is required before retry."
                )
        attempt_number = int(latest["attempt_number"]) + 1
    else:
        attempt_number = 1

    if stripe_configured() and (
            not ep or not ep["stripe_customer_id"] or not ep["payment_method_id"]):
        raise FundingPaymentFailed(
            "A confirmed employer payment method is required before escrow can be funded."
        )

    processor_idempotency_key = f"escrow-fund:{funding_identity}:attempt:{attempt_number}"
    cursor = db.execute(
        """INSERT INTO funding_attempts
           (operation_key, attempt_number, request_fingerprint, processor_idempotency_key,
            employer_id, order_id, milestone_id, base_amount_cents, platform_fee_cents,
            processing_fee_cents, charged_total_cents, currency, status)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,'usd','prepared')""",
        [
            funding_identity, attempt_number, fingerprint, processor_idempotency_key,
            employer_id, order_id, milestone_id, charge["base_cents"],
            charge["platform_fee_cents"], charge["processing_fee_cents"], charge["total_cents"],
        ],
    )
    attempt_id = cursor.lastrowid
    # This commit is the central invariant: the operation is durable before the
    # external processor can observe an idempotent create request.
    db.commit()
    prepared_attempt = db.execute(
        "SELECT * FROM funding_attempts WHERE id=?", [attempt_id]
    ).fetchone()

    if stripe_configured():
        try:
            intent = stripe.PaymentIntent.create(
                amount=charge["total_cents"],
                currency="usd",
                customer=ep["stripe_customer_id"],
                payment_method=ep["payment_method_id"],
                confirm=True,
                off_session=True,
                capture_method="automatic",
                description=description,
                metadata=_funding_attempt_metadata(prepared_attempt),
                idempotency_key=processor_idempotency_key,
            )
        except STRIPE_ERROR as exc:
            ambiguous = _funding_processor_error_is_ambiguous(exc)
            if db.in_transaction:
                db.commit()
            db.execute("BEGIN IMMEDIATE")
            try:
                current_attempt = db.execute(
                    "SELECT * FROM funding_attempts WHERE id=?", [attempt_id]
                ).fetchone()
                if current_attempt is None:
                    raise FundingReconciliationRequired(
                        "Funding attempt disappeared after processor failure."
                    )
                if _funding_attempt_bindings_changed(current_attempt, prepared_attempt):
                    _freeze_funding_attempt_binding_conflict(
                        db, current_attempt, prepared_attempt
                    )
                db.execute(
                    """UPDATE funding_attempts
                       SET status=?, error_code=?, error_message=?, updated_at=datetime('now')
                       WHERE id=? AND status='prepared'""",
                    [
                        "unknown" if ambiguous else "failed",
                        _funding_processor_error_code(exc),
                        "Processor outcome requires reconciliation." if ambiguous else "Processor declined the funding attempt.",
                        attempt_id,
                    ],
                )
                db.commit()
            except Exception:
                if db.in_transaction:
                    db.rollback()
                raise
            current_attempt = db.execute(
                "SELECT status FROM funding_attempts WHERE id=?", [attempt_id]
            ).fetchone()
            if current_attempt and current_attempt["status"] in ("processor_succeeded", "committed"):
                raise FundingReconciliationRequired(
                    "Durable processor success exists and must be reconciled before retry."
                ) from None
            if ambiguous:
                raise FundingReconciliationRequired(
                    "Funding outcome is ambiguous; processor reconciliation is required before retry."
                )
            raise FundingPaymentFailed("Payment was not completed by the processor.")
        inspection = _processor_intent_inspection(prepared_attempt, intent, "create")
        pi_id = inspection.get("processor_intent_id")
        processor_status = inspection.get("processor_status")
        mode = "live"
        evidence_source = "processor_create"
        if inspection.get("outcome") != "succeeded":
            error_code = (
                "processor_evidence_mismatch"
                if inspection.get("outcome") == "mismatch"
                else "processor_not_succeeded"
            )
            if db.in_transaction:
                db.commit()
            db.execute("BEGIN IMMEDIATE")
            try:
                current_attempt = db.execute(
                    "SELECT * FROM funding_attempts WHERE id=?", [attempt_id]
                ).fetchone()
                if current_attempt is None:
                    raise FundingReconciliationRequired(
                        "Funding attempt disappeared after processor evidence returned."
                    )
                if _funding_attempt_bindings_changed(current_attempt, prepared_attempt):
                    _freeze_funding_attempt_binding_conflict(
                        db,
                        current_attempt,
                        prepared_attempt,
                        pi_id,
                        processor_status,
                        evidence_source,
                    )
                if _freeze_processor_intent_conflict(
                    db,
                    current_attempt,
                    pi_id,
                    processor_status,
                    evidence_source,
                ):
                    raise FundingReconciliationRequired(
                        "Processor evidence conflicts with a different durable processor intent."
                    )
                db.execute(
                    """UPDATE funding_attempts
                       SET status='unknown', stripe_payment_intent_id=?, processor_status=?,
                           evidence_source='processor_create', processor_evidence_at=datetime('now'),
                           error_code=?, error_message='Processor outcome requires reconciliation.',
                           updated_at=datetime('now') WHERE id=? AND status='prepared'""",
                    [pi_id, processor_status or None, error_code, attempt_id],
                )
                db.commit()
            except Exception:
                if db.in_transaction:
                    db.rollback()
                raise
            raise FundingReconciliationRequired(
                "Funding is not confirmed; processor reconciliation is required before retry."
            )
    else:
        pi_id = fake_payment_intent_id()
        processor_status = "succeeded"
        mode = "simulated"
        evidence_source = "simulator"

    # The processor call is complete. Serialize the durable reread and apply so a
    # signed webhook cannot change provenance between this comparison and update.
    if db.in_transaction:
        db.commit()
    db.execute("BEGIN IMMEDIATE")
    try:
        current_attempt = db.execute(
            "SELECT * FROM funding_attempts WHERE id=?", [attempt_id]
        ).fetchone()
        if current_attempt is None:
            raise FundingReconciliationRequired(
                "Funding attempt disappeared after processor success."
            )
        if _funding_attempt_bindings_changed(current_attempt, prepared_attempt):
            _freeze_funding_attempt_binding_conflict(
                db,
                current_attempt,
                prepared_attempt,
                pi_id,
                processor_status,
                evidence_source,
            )
        if _freeze_processor_intent_conflict(
            db,
            current_attempt,
            pi_id,
            processor_status,
            evidence_source,
        ):
            raise FundingReconciliationRequired(
                "Processor success conflicts with a different durable processor intent."
            )
        if current_attempt["error_code"] == "prior_attempt_success_conflict":
            _record_conflicting_processor_success(
                db, current_attempt, pi_id, processor_status, evidence_source
            )
            raise FundingReconciliationRequired(
                "Multiple processor-success signals require manual reconciliation before retry."
            )
        if _record_success_conflict_with_newer_attempt(
            db, current_attempt, pi_id, processor_status, evidence_source
        ):
            raise FundingReconciliationRequired(
                "A prior failed attempt succeeded after a newer attempt existed; manual reconciliation is required."
            )

        db.execute(
            """UPDATE funding_attempts
               SET status='processor_succeeded', stripe_payment_intent_id=?, processor_status=?,
                   evidence_source=?, processor_evidence_at=datetime('now'),
                   error_code=NULL, error_message=NULL, updated_at=datetime('now')
               WHERE id=? AND status IN ('prepared','unknown','failed')
                 AND COALESCE(error_code,'') NOT IN (
                     'prior_attempt_success_conflict','processor_intent_conflict'
                 )""",
            [pi_id, processor_status, evidence_source, attempt_id],
        )
        db.commit()
    except Exception:
        if db.in_transaction:
            db.rollback()
        raise

    current_attempt = db.execute(
        "SELECT * FROM funding_attempts WHERE id=?", [attempt_id]
    ).fetchone()
    if current_attempt["status"] == "committed":
        if current_attempt["stripe_payment_intent_id"] != pi_id:
            raise FundingReconciliationRequired(
                "Processor success conflicts with a different committed processor intent."
            )
        return current_attempt["stripe_payment_intent_id"], "reconciled"
    if current_attempt["status"] != "processor_succeeded":
        if current_attempt["error_code"] == "prior_attempt_success_conflict":
            _record_conflicting_processor_success(
                db, current_attempt, pi_id, processor_status, evidence_source
            )
        raise FundingReconciliationRequired(
            "Processor success conflicts with newer durable evidence; reconciliation is required."
        )
    if current_attempt["stripe_payment_intent_id"] != pi_id:
        raise FundingReconciliationRequired(
            "Processor success conflicts with a different durable processor intent."
        )
    _commit_funding_attempt(db, prepared_attempt, pi_id)
    return pi_id, mode


def _job_hire_replay_response(db, order_id, funding_mode="replayed"):
    order_row = db.execute("SELECT * FROM orders WHERE id=?", [order_id]).fetchone()
    if order_row is None:
        raise FundingReconciliationRequired(
            "Job-hire order disappeared before the idempotent response replay."
        )
    recovered = row_to_dict(order_row)
    recovered["milestones"] = [
        row_to_dict(row)
        for row in db.execute(
            "SELECT * FROM milestones WHERE order_id=? ORDER BY sequence,id", [order_id]
        ).fetchall()
    ]
    recovered["idempotent_replay"] = True
    recovered["funding_mode"] = funding_mode
    return recovered


def _recover_fixed_job_hire_after_funding_commit_owned(
    db,
    user,
    job,
    application,
    order,
    body,
    funding_mode="replayed",
    audit_action="recover_hire_worker_after_funding",
):
    """Finish a fixed-price hire only from the exact already-committed first hold."""
    if (
        job["budget_type"] != "fixed"
        or order["type"] != "job_hire"
        or order["job_id"] != job["id"]
        or order["employer_id"] != user["id"]
        or order["worker_id"] != application["worker_id"]
        or order["status"] != "in_progress"
    ):
        raise FundingConflict("Existing job hire is not eligible for lifecycle recovery.")
    try:
        expected_creation_fingerprint = job_hire_creation_request_fingerprint(
            user["id"],
            job["id"],
            application["id"],
            application["worker_id"],
            job["budget_type"],
            job["budget_amount"],
            body,
        )
    except (TypeError, ValueError) as exc:
        raise FundingConflict(
            "Job-hire retry inputs conflict with the durable hire request."
        ) from exc
    if (
        order["creation_idempotency_key"] != f"job-hire/{job['id']}"
        or not order["creation_request_fingerprint"]
        or order["creation_request_fingerprint"] != expected_creation_fingerprint
    ):
        raise FundingConflict("Job-hire retry inputs conflict with the durable hire request.")
    if db.execute(
        "SELECT 1 FROM hourly_contracts WHERE order_id=? LIMIT 1", [order["id"]]
    ).fetchone():
        raise FundingConflict("Hourly job hires require their dedicated recovery lifecycle.")

    try:
        total_cents = money_to_cents(job["budget_amount"], "job budget")
        if money_to_cents(order["total_amount"], "order total") != total_cents:
            raise FundingConflict("Existing job-hire total conflicts with the posted budget.")
        requested = body.get("milestones", [])
        if not requested:
            requested = [{
                "title": "Project completion",
                "description": "Full project deliverable",
                "amount": total_cents / 100,
            }]
        requested_cents = [
            money_to_cents(item.get("amount"), f"milestone {index} amount")
            for index, item in enumerate(requested, 1)
        ]
    except (AttributeError, TypeError, ValueError) as exc:
        raise FundingConflict("Job-hire retry inputs do not match the committed funding operation.") from exc
    if any(value <= 0 for value in requested_cents) or sum(requested_cents) != total_cents:
        raise FundingConflict("Job-hire retry milestones do not match the committed budget.")

    milestones = db.execute(
        "SELECT * FROM milestones WHERE order_id=? ORDER BY sequence,id", [order["id"]]
    ).fetchall()
    if len(milestones) != len(requested):
        raise FundingConflict("Job-hire retry milestone count conflicts with durable state.")
    for index, (durable, supplied, amount_cents) in enumerate(
        zip(milestones, requested, requested_cents), 1
    ):
        expected_title = supplied.get("title", f"Milestone {index}")
        expected_description = supplied.get("description", "")
        if (
            durable["sequence"] != index
            or money_to_cents(durable["amount"], "durable milestone amount") != amount_cents
            or durable["title"] != expected_title
            or (durable["description"] or "") != expected_description
        ):
            raise FundingConflict("Job-hire retry milestone inputs conflict with durable state.")

    first = milestones[0]
    funding_identity = f"milestone:{first['id']}"
    charge = buyer_charge_breakdown_cents(requested_cents[0] / 100)
    expected_funding_fingerprint = funding_request_fingerprint(
        funding_identity, user["id"], order["id"], first["id"], charge
    )
    holds = db.execute(
        "SELECT * FROM escrow_holds WHERE order_id=? ORDER BY id", [order["id"]]
    ).fetchall()
    if len(holds) != 1:
        raise FundingConflict("Job-hire recovery requires exactly one committed escrow hold.")
    hold = holds[0]
    if (
        hold["milestone_id"] != first["id"]
        or hold["funding_identity"] != funding_identity
        or hold["funding_attempt_id"] is None
        or hold["status"] != "held"
        or not hold["stripe_payment_intent_id"]
        or hold["base_amount_cents"] is None
        or int(hold["base_amount_cents"]) != charge["base_cents"]
        or hold["platform_fee_cents"] is None
        or int(hold["platform_fee_cents"]) != charge["platform_fee_cents"]
        or hold["processing_fee_cents"] is None
        or int(hold["processing_fee_cents"]) != charge["processing_fee_cents"]
        or hold["charged_total_cents"] is None
        or int(hold["charged_total_cents"]) != charge["total_cents"]
        or hold["fee_policy_version"] != "component-half-up-v1"
    ):
        raise FundingConflict("Job-hire escrow provenance requires manual reconciliation.")
    attempt = db.execute(
        "SELECT * FROM funding_attempts WHERE id=?", [hold["funding_attempt_id"]]
    ).fetchone()
    if attempt is None:
        raise FundingReconciliationRequired(
            "Job-hire funding attempt disappeared during lifecycle recovery."
        )
    if (
        attempt["status"] != "committed"
        or attempt["error_code"]
        or attempt["operation_key"] != funding_identity
        or attempt["order_id"] != order["id"]
        or attempt["milestone_id"] != first["id"]
        or attempt["employer_id"] != user["id"]
        or attempt["stripe_payment_intent_id"] != hold["stripe_payment_intent_id"]
        or attempt["request_fingerprint"] != expected_funding_fingerprint
        or int(attempt["base_amount_cents"]) != charge["base_cents"]
        or int(attempt["platform_fee_cents"]) != charge["platform_fee_cents"]
        or int(attempt["processing_fee_cents"]) != charge["processing_fee_cents"]
        or int(attempt["charged_total_cents"]) != charge["total_cents"]
        or attempt["currency"] != "usd"
    ):
        raise FundingReconciliationRequired(
            "Job-hire funding evidence requires manual reconciliation."
        )
    conflicting_attempt = db.execute(
        """SELECT id,attempt_number,status,error_code
           FROM funding_attempts
           WHERE id<>?
             AND (operation_key=? OR milestone_id=?)
             AND (
               operation_key<>?
               OR COALESCE(milestone_id,-1)<>?
               OR order_id<>?
               OR employer_id<>?
               OR COALESCE(request_fingerprint,'')<>?
               OR attempt_number>?
               OR status IN ('prepared','unknown','processor_succeeded','committed')
               OR COALESCE(error_code,'') LIKE '%conflict%'
             )
           ORDER BY attempt_number DESC,id DESC
           LIMIT 1""",
        [
            attempt["id"],
            funding_identity,
            first["id"],
            funding_identity,
            first["id"],
            order["id"],
            user["id"],
            expected_funding_fingerprint,
            attempt["attempt_number"],
        ],
    ).fetchone()
    if conflicting_attempt:
        raise FundingReconciliationRequired(
            "Job-hire recovery has another unresolved or contradictory funding attempt."
        )

    accepted_application_ids = {
        row[0]
        for row in db.execute(
            "SELECT id FROM applications WHERE job_id=? AND status='accepted'",
            [job["id"]],
        ).fetchall()
    }

    # All recovery evidence is local and exact; never retrieve, search, or create
    # a processor operation from this lifecycle-only repair/replay path.
    pi_id = hold["stripe_payment_intent_id"]
    mode = funding_mode
    lifecycle_pending = (
        job["status"] in ("open", "reviewing")
        and application["status"] in ("pending", "shortlisted")
        and not accepted_application_ids
        and all(
            milestone["status"] == "pending"
            and milestone["escrow_payment_id"] is None
            for milestone in milestones
        )
    )
    lifecycle_complete = (
        job["status"] == "hired"
        and application["status"] == "accepted"
        and accepted_application_ids == {application["id"]}
        and first["status"] == "in_progress"
        and first["escrow_payment_id"] == pi_id
        and bool(first["funded_at"])
        and all(
            milestone["status"] == "pending"
            and milestone["escrow_payment_id"] is None
            for milestone in milestones[1:]
        )
    )
    if lifecycle_complete:
        # The durable lifecycle already committed and only the HTTP response was
        # lost. Commit the read transaction and replay without enqueueing or
        # flushing another notification.
        db.commit()
        return _job_hire_replay_response(db, order["id"], mode)
    if not lifecycle_pending:
        raise FundingConflict(
            "Job-hire lifecycle state conflicts with committed-funding recovery."
        )

    updated_milestone = db.execute(
        """UPDATE milestones
           SET status='in_progress', escrow_payment_id=?, funded_at=COALESCE(funded_at,datetime('now'))
           WHERE id=? AND order_id=? AND status='pending' AND escrow_payment_id IS NULL""",
        [pi_id, first["id"], order["id"]],
    )
    updated_job = db.execute(
        "UPDATE jobs SET status='hired', updated_at=datetime('now') WHERE id=? AND status IN ('open','reviewing')",
        [job["id"]],
    )
    updated_application = db.execute(
        "UPDATE applications SET status='accepted' WHERE id=? AND job_id=? AND worker_id=? AND status IN ('pending','shortlisted')",
        [application["id"], job["id"], application["worker_id"]],
    )
    if (
        updated_milestone.rowcount != 1
        or updated_job.rowcount != 1
        or updated_application.rowcount != 1
    ):
        db.rollback()
        raise FundingConflict("Job-hire lifecycle changed during committed-funding recovery.")
    db.execute(
        "UPDATE applications SET status='rejected' WHERE job_id=? AND worker_id!=?",
        [job["id"], application["worker_id"]],
    )
    push_notification(
        db,
        application["worker_id"],
        "job_hired",
        "You've been hired!",
        f"You've been hired for: {job['title']}",
        f"/orders/{order['id']}",
        email=True,
        email_dedupe=f"job_hired:{job['id']}:{application['worker_id']}:{order['id']}",
    )
    audit(
        db,
        user["id"],
        audit_action,
        "order",
        order["id"],
        {"job_id": job["id"], "worker_id": application["worker_id"]},
    )
    db.commit()
    flush_transactional_notification_emails(db)
    return _job_hire_replay_response(db, order["id"], mode)


def _recover_fixed_job_hire_after_funding_commit(
    db,
    user,
    job,
    application,
    order,
    body,
    funding_mode="replayed",
    audit_action="recover_hire_worker_after_funding",
):
    """Serialize the exact local reread and lifecycle-only recovery commit."""
    if db.in_transaction:
        raise FundingReconciliationRequired(
            "Job-hire recovery requires an isolated local writer transaction."
        )
    db.execute("BEGIN IMMEDIATE")
    try:
        fresh_job = db.execute("SELECT * FROM jobs WHERE id=?", [job["id"]]).fetchone()
        fresh_application = db.execute(
            "SELECT * FROM applications WHERE id=? AND job_id=?",
            [application["id"], job["id"]],
        ).fetchone()
        fresh_order = db.execute(
            "SELECT * FROM orders WHERE id=? AND job_id=? AND type='job_hire'",
            [order["id"], job["id"]],
        ).fetchone()
        if fresh_job is None or fresh_application is None or fresh_order is None:
            raise FundingConflict("Job-hire recovery state changed before the durable reread.")
        return _recover_fixed_job_hire_after_funding_commit_owned(
            db,
            user,
            fresh_job,
            fresh_application,
            fresh_order,
            body,
            funding_mode,
            audit_action,
        )
    except Exception:
        if db.in_transaction:
            db.rollback()
        raise


# ─── Route Handler ─────────────────────────────────────────────────────────────

# Log DB location on first import (visible in Railway deploy logs)
print(f"[GoHireHumans] Volume dir /data exists: {os.path.isdir(_VOLUME_DIR)}", file=sys.stderr)
print(f"[GoHireHumans] RAILWAY_VOLUME_MOUNT_PATH: {os.environ.get('RAILWAY_VOLUME_MOUNT_PATH', '(not set)')}", file=sys.stderr)
print(f"[GoHireHumans] DB path will be resolved lazily on first request", file=sys.stderr)


def handle_request():
    install_sensitive_logging_filters()
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

    for request_attr in (
        "authenticated_api_key_id", "api_key_accounting_intent_id",
        "response_status", "request_started_monotonic"
    ):
        if hasattr(_request_ctx, request_attr):
            delattr(_request_ctx, request_attr)
    _request_ctx.request_started_monotonic = time.monotonic()

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
        intent_id = getattr(_request_ctx, "api_key_accounting_intent_id", None)
        if intent_id is not None:
            try:
                response_time_ms = max(
                    0,
                    int(round(
                        (time.monotonic() - _request_ctx.request_started_monotonic) * 1000
                    )),
                )
                _finalize_api_key_accounting_intent(
                    intent_id,
                    int(getattr(_request_ctx, "response_status", 500)),
                    response_time_ms,
                )
            except Exception as exc:
                # The started row and aggregate attribution already committed.
                # Never overwrite a successful financial response when finalization fails.
                print(f"[GoHireHumans] API-key accounting finalization failed: {exc}", file=sys.stderr)


def _handle_routes(db):
    method = getattr(_request_ctx, 'request_method', 'GET')
    path = getattr(_request_ctx, 'path_info', '').rstrip("/")
    params = get_query_params()

    # Strip /api/v1 prefix so Stripe webhook URL and other prefixed paths work
    if path.startswith("/api/v1"):
        path = path[len("/api/v1"):] or "/"

    # Authenticate and durably account API-key requests before route code can read
    # a body, mutate domain state, or cross a processor boundary. Valid denied
    # principals are auditable; invalid keys are never attributed.
    if (getattr(_request_ctx, "http_x_api_key", "") or "").strip():
        api_principal = authenticate_api_key(db)
        if api_principal is not None:
            required_scope = _api_key_route_scope(method, path)
            granted = set(api_principal.get("api_key_scopes", []))
            api_key_id = int(_request_ctx.authenticated_api_key_id)
            if required_scope is None or required_scope not in granted:
                try:
                    _start_api_key_accounting_intent(
                        db, api_key_id, path or "/", method,
                        required_scope, state="denied", status_code=403,
                    )
                except sqlite3.Error:
                    return error_response("API-key accounting is temporarily unavailable", 503)
                # Denied rows are already terminal and must not be finalized again.
                delattr(_request_ctx, "api_key_accounting_intent_id")
                return error_response("API key scope does not permit this route", 403)
            try:
                _start_api_key_accounting_intent(
                    db, api_key_id, path or "/", method, required_scope,
                )
            except sqlite3.Error:
                return error_response("API-key accounting is temporarily unavailable", 503)

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
        if job['budget_type'] == 'hourly':
            return error_response(
                "Hourly hiring and payout settlement are deferred to Task 4.", 503
            )

        existing_job_hire = db.execute(
            "SELECT * FROM orders WHERE type='job_hire' AND job_id=? ORDER BY id LIMIT 1",
            [job_id],
        ).fetchone()
        if not existing_job_hire:
            if job['status'] not in ('open', 'reviewing'):
                return error_response("Job must be open or reviewing to hire", 409)
            if not JOB_HIRING_ENABLED:
                return error_response(
                    "New job hiring is temporarily paused while payment safeguards are finalized",
                    503,
                )

        body = get_body()
        application_id = body.get("application_id")
        if application_id is None:
            return error_response("application_id required")
        try:
            application_id = int(application_id)
        except (TypeError, ValueError):
            return error_response("application_id must be an integer")

        # Read the selected application without a lifecycle-status filter so an
        # exact retry can replay a hire that already committed accepted/hired.
        # New hires still enforce the eligible states below.
        app = db.execute(
            "SELECT id, worker_id, status FROM applications WHERE id = ? AND job_id = ?",
            [application_id, job_id]
        ).fetchone()
        if not app:
            return error_response("Application not found for this job", 404)

        if existing_job_hire and job["budget_type"] == "fixed":
            try:
                recovered = _recover_fixed_job_hire_after_funding_commit(
                    db, user, job, app, existing_job_hire, body
                )
            except (FundingPaymentFailed, FundingConflict, FundingReconciliationRequired) as exc:
                return funding_error_response(exc)
            return json_response(recovered, 200)

        # The pause gate blocks only new funding. Exact committed fixed-hire
        # recovery/replay above remains local and processor-free.
        if job['status'] not in ('open', 'reviewing'):
            return error_response("Job must be open or reviewing to hire", 409)
        if not JOB_HIRING_ENABLED:
            return error_response("New job hiring is temporarily paused while payment safeguards are finalized", 503)
        if app["status"] not in ("pending", "shortlisted"):
            return error_response("Eligible application not found for this job", 404)

        # New funding requires an active payment setup. Exact committed fixed-hire
        # lifecycle recovery above is deliberately processor-configuration-free.
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
        try:
            hire_request_fingerprint = job_hire_creation_request_fingerprint(
                user["id"],
                job_id,
                application_id,
                worker_id,
                job["budget_type"],
                job["budget_amount"],
                body,
            )
        except ValueError as e:
            return error_response(str(e), 400)
        hire_operation_key = f"job-hire/{job_id}"

        # Create order
        try:
            cursor = db.execute(
                """INSERT INTO orders
                   (type, job_id, worker_id, employer_id, status, total_amount,
                    creation_idempotency_key, creation_request_fingerprint)
                   VALUES ('job_hire', ?, ?, ?, 'in_progress', ?, ?, ?)""",
                [
                    job_id,
                    worker_id,
                    user['id'],
                    total_amount,
                    hire_operation_key,
                    hire_request_fingerprint,
                ]
            )
        except sqlite3.IntegrityError:
            db.rollback()
            existing_order = db.execute(
                "SELECT * FROM orders WHERE type='job_hire' AND job_id=? ORDER BY id LIMIT 1",
                [job_id],
            ).fetchone()
            if existing_order and job["budget_type"] == "fixed":
                try:
                    recovered = _recover_fixed_job_hire_after_funding_commit(
                        db, user, job, app, existing_order, body
                    )
                except (FundingPaymentFailed, FundingConflict, FundingReconciliationRequired) as exc:
                    return funding_error_response(exc)
                return json_response(recovered, 200)
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
                    f"Escrow for job {job_id} application {application_id} milestone 1",
                    funding_identity=f"milestone:{first_ms_id}",
                )
            except (FundingPaymentFailed, FundingConflict, FundingReconciliationRequired) as e:
                db.rollback()
                return funding_error_response(e)
            except ValueError as e:
                db.rollback()
                return error_response(str(e), 402)

            # Funding committed in its own isolated writer transaction. Complete the
            # first fixed-hire lifecycle only after a fresh serialized reread validates
            # the job, application, order, milestone, request fingerprint, hold, and
            # funding-attempt provenance together.
            new_order = db.execute(
                "SELECT * FROM orders WHERE id=? AND job_id=? AND type='job_hire'",
                [order_id, job_id],
            ).fetchone()
            try:
                completed = _recover_fixed_job_hire_after_funding_commit(
                    db,
                    user,
                    job,
                    app,
                    new_order,
                    body,
                    funding_mode=mode,
                    audit_action="hire_worker",
                )
            except (FundingPaymentFailed, FundingConflict, FundingReconciliationRequired) as e:
                db.rollback()
                return funding_error_response(e)
            completed.pop("idempotent_replay", None)
            completed.pop("funding_mode", None)
            return json_response(completed, 201)

        elif job['budget_type'] == 'hourly':
            # Hourly: use the posted rate and fund the employer-selected first-week cap.
            # The client must not be able to rewrite the worker's posted rate at hire time.
            hourly_rate = float(job['budget_amount'] or 0)
            try:
                weekly_cap = bounded_integer(body.get("weekly_hour_cap", 40), "Weekly hour cap", 1, 168)
            except ValueError as e:
                db.rollback()
                return error_response(str(e), 400)
            if not math.isfinite(hourly_rate) or hourly_rate <= 0:
                db.rollback()
                return error_response("Hourly job rate must be greater than zero", 400)
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
                    f"First week escrow for job {job_id} application {application_id} week {week_start}",
                    funding_identity=f"job:{job_id}:application:{application_id}:week:{week_start}",
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
        body = get_body()
        try:
            creation_idempotency_key = validated_idempotency_key(body.get("idempotency_key"))
            creation_request_fingerprint = service_order_creation_request_fingerprint(
                user["id"], service_id, body
            )
        except ValueError as e:
            return error_response(str(e), 400)

        # Serialize operation lookup/creation, then release the writer lock before
        # any processor call when fund_escrow_stripe durably commits its prepared row.
        if not db.in_transaction:
            db.execute("BEGIN IMMEDIATE")
        existing_order = db.execute(
            "SELECT * FROM orders WHERE employer_id=? AND creation_idempotency_key=?",
            [user['id'], creation_idempotency_key],
        ).fetchone()
        if existing_order:
            if existing_order['service_id'] != service_id:
                return error_response("idempotency_key was already used for a different service order", 409)
            if not existing_order["creation_request_fingerprint"]:
                return error_response(
                    "Existing service order requires request-fingerprint reconciliation", 409
                )
            if existing_order["creation_request_fingerprint"] != creation_request_fingerprint:
                return error_response(
                    "idempotency_key was already used with different service-order inputs", 409
                )
            milestones = db.execute(
                "SELECT * FROM milestones WHERE order_id=? ORDER BY sequence", [existing_order['id']]
            ).fetchall()
            if len(milestones) != 1:
                return error_response("Existing service order requires lifecycle reconciliation", 409)
            milestone = milestones[0]
            funding_identity = f"milestone:{milestone['id']}"
            try:
                pi_id, mode = fund_escrow_stripe(
                    db,
                    user['id'],
                    existing_order['total_amount'],
                    existing_order['id'],
                    milestone['id'],
                    f"Escrow for service {service_id} operation {creation_idempotency_key}",
                    funding_identity=funding_identity,
                )
            except (FundingPaymentFailed, FundingConflict, FundingReconciliationRequired) as exc:
                return funding_error_response(exc)

            if existing_order['status'] == 'pending':
                try:
                    _settle_committed_milestone_funding(
                        db,
                        existing_order,
                        milestone,
                        pi_id,
                        expected_order_status="pending",
                    )
                except (FundingConflict, FundingReconciliationRequired) as exc:
                    return funding_error_response(exc)
                svc_for_notice = db.execute("SELECT title, worker_id FROM services WHERE id=?", [service_id]).fetchone()
                if svc_for_notice:
                    push_notification(
                        db, svc_for_notice['worker_id'], "new_order", "New service order!",
                        f"Someone ordered your service: {svc_for_notice['title']}",
                        f"/orders/{existing_order['id']}", email=True,
                        email_dedupe=f"service_order:{existing_order['id']}",
                    )
                audit(db, user['id'], "reconcile_service_order_funding", "order", existing_order['id'])
                db.commit()
                flush_transactional_notification_emails(db)
            elif existing_order['status'] == 'in_progress' and milestone['status'] != 'in_progress':
                return error_response("Existing service order requires lifecycle reconciliation", 409)

            refreshed = db.execute("SELECT * FROM orders WHERE id=?", [existing_order['id']]).fetchone()
            result = row_to_dict(refreshed)
            milestones = db.execute(
                "SELECT * FROM milestones WHERE order_id=? ORDER BY sequence", [existing_order['id']]
            ).fetchall()
            result['milestones'] = [row_to_dict(m) for m in milestones]
            result['idempotent_replay'] = True
            result['funding_mode'] = mode
            return json_response(result, 200)

        svc = db.execute("SELECT * FROM services WHERE id = ? AND status = 'active'", [service_id]).fetchone()
        if not svc:
            return error_response("Service not found or unavailable", 404)
        if svc['worker_id'] == user['id']:
            return error_response("You cannot order your own service", 403)

        ensure_employer_profile(db, user['id'])
        if not employer_has_payment_setup(db, user['id']):
            return error_response("You must set up a payment method before ordering. Use /payments/setup-employer.", 402)

        pricing_type = svc['pricing_type']

        try:
            if pricing_type == 'fixed':
                total_amount_cents = money_to_cents(svc['price'] or 0, "service price")
            elif pricing_type == 'hourly':
                hours = canonical_decimal_quantity(body.get("hours", 1), "hours")
                total_amount_cents = rounded_product_cents(svc['hourly_rate'] or 0, hours, "service hourly rate")
            else:
                # custom pricing: employer provides an exact canonical cent amount
                total_amount_cents = money_to_cents(body.get("amount", 0), "custom service amount")
            if total_amount_cents <= 0:
                raise ValueError("Service price must be positive")
        except ValueError as e:
            return error_response(str(e), 400)
        total_amount = total_amount_cents / 100

        # Create order. The unique operation key closes the concurrent replay race.
        try:
            cursor = db.execute(
                """INSERT INTO orders
                   (type, service_id, worker_id, employer_id, status, total_amount,
                    creation_idempotency_key, creation_request_fingerprint)
                   VALUES ('service_order', ?, ?, ?, 'pending', ?, ?, ?)""",
                [
                    service_id, svc['worker_id'], user['id'], total_amount,
                    creation_idempotency_key, creation_request_fingerprint,
                ]
            )
        except sqlite3.IntegrityError:
            db.rollback()
            existing_order = db.execute(
                "SELECT * FROM orders WHERE employer_id=? AND creation_idempotency_key=?",
                [user['id'], creation_idempotency_key],
            ).fetchone()
            if not existing_order or existing_order['service_id'] != service_id:
                return error_response("idempotency_key conflicts with another operation", 409)
            return error_response("Concurrent service order creation must be retried", 409)
        order_id = cursor.lastrowid

        # Create single milestone for the full amount
        mc = db.execute(
            "INSERT INTO milestones (order_id, title, description, amount, sequence, status) VALUES (?,?,?,?,1,'pending')",
            [order_id, "Service delivery", body.get("notes", ""), total_amount]
        )
        milestone_id = mc.lastrowid
        expected_order = db.execute(
            "SELECT * FROM orders WHERE id=?", [order_id]
        ).fetchone()
        expected_milestone = db.execute(
            "SELECT * FROM milestones WHERE id=?", [milestone_id]
        ).fetchone()

        # Fund escrow
        try:
            pi_id, mode = fund_escrow_stripe(
                db, user['id'], total_amount, order_id, milestone_id,
                f"Escrow for service {service_id} operation {creation_idempotency_key}",
                funding_identity=f"milestone:{milestone_id}",
            )
        except (FundingPaymentFailed, FundingConflict, FundingReconciliationRequired) as exc:
            return funding_error_response(exc)

        try:
            _settle_committed_milestone_funding(
                db,
                expected_order,
                expected_milestone,
                pi_id,
                expected_order_status="pending",
            )
        except (FundingConflict, FundingReconciliationRequired) as exc:
            return funding_error_response(exc)

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
            "funded_amount_available": True,
            "charge_amount_available": True,
            "record_count": len(escrow),
        }
        for hold in escrow:
            persisted_base_cents = hold['base_amount_cents']
            if persisted_base_cents is not None:
                funding_summary["base_cents"] += int(persisted_base_cents)
            else:
                try:
                    funding_summary["base_cents"] += money_to_cents(hold['amount'], "legacy escrow amount")
                except ValueError:
                    funding_summary["funded_amount_available"] = False

            persisted_charge = (
                hold['platform_fee_cents'], hold['processing_fee_cents'], hold['charged_total_cents'],
            )
            if all(value is not None for value in persisted_charge):
                funding_summary["platform_fee_cents"] += int(persisted_charge[0])
                funding_summary["processing_fee_cents"] += int(persisted_charge[1])
                funding_summary["charged_total_cents"] += int(persisted_charge[2])
            else:
                # Rows created before exact charge persistence require processor reconciliation.
                funding_summary["charge_amount_available"] = False
        if not funding_summary["funded_amount_available"]:
            funding_summary["base_cents"] = None
        if not funding_summary["charge_amount_available"]:
            funding_summary["platform_fee_cents"] = None
            funding_summary["processing_fee_cents"] = None
            funding_summary["charged_total_cents"] = None
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

        # Serialize submit against completion/payout preparation and reread the
        # legal lifecycle under the same BEGIN IMMEDIATE that performs mutation.
        if db.in_transaction:
            db.commit()
        db.execute("BEGIN IMMEDIATE")
        order = db.execute("SELECT * FROM orders WHERE id=?", [order_id]).fetchone()
        if not order or order['worker_id'] != user['id']:
            db.rollback()
            return error_response("Order submission ownership changed", 409)
        if order['status'] not in ('in_progress', 'revision_requested'):
            db.rollback()
            return error_response("Order must be in_progress or revision_requested to submit", 409)
        payout_gate = db.execute(
            """SELECT id FROM payout_release_attempts
               WHERE order_id=? AND lifecycle_status<>'completed' LIMIT 1""",
            [order_id],
        ).fetchone()
        completion_gate = db.execute(
            """SELECT order_id FROM order_completion_operations
               WHERE order_id=? AND status IN ('prepared','unknown','processor_succeeded')
               LIMIT 1""",
            [order_id],
        ).fetchone()
        if payout_gate or completion_gate:
            db.rollback()
            return error_response(
                "Order has an active payout/completion operation; reconciliation is required.", 409
            )

        updated_order = db.execute(
            """UPDATE orders SET status='submitted', worker_notes=?, updated_at=datetime('now')
               WHERE id=? AND status IN ('in_progress','revision_requested')""",
            [notes, order_id]
        )
        if updated_order.rowcount != 1:
            db.rollback()
            return error_response("Order submission lifecycle changed", 409)

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
            # Exact recovery for an ambiguous next-milestone funding attempt after
            # the prior payout lifecycle was durably completed. Re-enter only the
            # existing funding identity; fund_escrow_stripe performs read-only
            # reconciliation and never creates a second attempt while unresolved.
            active_next = db.execute(
                """SELECT m.*,fa.id AS funding_attempt_id
                   FROM milestones m
                   JOIN funding_attempts fa ON fa.milestone_id=m.id
                    AND fa.operation_key=('milestone:' || m.id)
                    AND fa.status IN ('prepared','unknown','processor_succeeded')
                   WHERE m.order_id=? AND m.status='pending'
                     AND NOT EXISTS (
                         SELECT 1 FROM milestones prior
                          WHERE prior.order_id=m.order_id AND prior.sequence<m.sequence
                            AND prior.status<>'approved'
                     )
                     AND NOT EXISTS (
                         SELECT 1 FROM payout_release_attempts p
                          WHERE p.order_id=m.order_id AND p.lifecycle_status<>'completed'
                     )
                   ORDER BY m.sequence,m.id LIMIT 2""",
                [order_id],
            ).fetchall()
            if len(active_next) == 1:
                active_ms = active_next[0]
                try:
                    pi_id, mode = fund_escrow_stripe(
                        db, user['id'], float(active_ms['amount']), order_id,
                        active_ms['id'],
                        f"Escrow for order #{order_id} milestone {active_ms['sequence']}",
                    )
                except (FundingPaymentFailed, FundingConflict, FundingReconciliationRequired) as exc:
                    return funding_error_response(exc)
                db.execute("BEGIN IMMEDIATE")
                locked_order = db.execute("SELECT * FROM orders WHERE id=?", [order_id]).fetchone()
                locked_ms = db.execute(
                    "SELECT * FROM milestones WHERE id=? AND order_id=?",
                    [active_ms['id'], order_id],
                ).fetchone()
                if not locked_order or not locked_ms:
                    db.rollback()
                    return error_response("Recovered funding lifecycle changed", 409)
                try:
                    _settle_committed_milestone_funding(
                        db, locked_order, locked_ms, pi_id,
                        expected_order_status="submitted",
                    )
                except (FundingConflict, FundingReconciliationRequired) as exc:
                    db.rollback()
                    return funding_error_response(exc)
                audit(db, user['id'], "recover_ambiguous_next_funding", "order", order_id, {
                    "milestone_id": active_ms['id'],
                    "funding_attempt_id": active_ms['funding_attempt_id'],
                    "mode": mode,
                })
                db.commit()
                return json_response({
                    "ok": True, "status": "in_progress", "recovered_funding": True,
                    "payment_intent_id": pi_id,
                })

            # A next-milestone funding helper commits its hold before this route's
            # lifecycle update. If the route then crashes, an exact approve retry
            # must materialize that already-committed funding without another
            # transfer or PaymentIntent create.
            if not db.in_transaction:
                db.execute("BEGIN IMMEDIATE")
            order = db.execute("SELECT * FROM orders WHERE id=?", [order_id]).fetchone()
            recovery_rows = db.execute(
                """SELECT m.id AS milestone_id, m.sequence, m.amount AS milestone_amount,
                          h.id AS hold_id, h.stripe_payment_intent_id, h.funding_attempt_id,
                          h.base_amount_cents AS hold_base_cents,
                          h.platform_fee_cents AS hold_platform_fee_cents,
                          h.processing_fee_cents AS hold_processing_fee_cents,
                          h.charged_total_cents AS hold_total_cents,
                          h.fee_policy_version AS hold_fee_policy_version,
                          fa.operation_key, fa.request_fingerprint, fa.employer_id,
                          fa.base_amount_cents AS attempt_base_cents,
                          fa.platform_fee_cents AS attempt_platform_fee_cents,
                          fa.processing_fee_cents AS attempt_processing_fee_cents,
                          fa.charged_total_cents AS attempt_total_cents
                   FROM milestones m
                   JOIN escrow_holds h
                     ON h.order_id=m.order_id AND h.milestone_id=m.id
                    AND h.status='held'
                   JOIN funding_attempts fa
                     ON fa.id=h.funding_attempt_id AND fa.status='committed'
                    AND COALESCE(fa.error_code,'')=''
                    AND fa.currency='usd'
                    AND fa.order_id=m.order_id AND fa.milestone_id=m.id
                    AND fa.operation_key=('milestone:' || m.id)
                    AND fa.stripe_payment_intent_id=h.stripe_payment_intent_id
                   WHERE m.order_id=? AND m.status='pending'
                     AND h.funding_identity=('milestone:' || m.id)
                     AND h.stripe_payment_intent_id IS NOT NULL
                     AND NOT EXISTS (
                         SELECT 1 FROM escrow_holds other_hold
                          WHERE other_hold.order_id=m.order_id
                            AND other_hold.milestone_id=m.id
                            AND other_hold.id<>h.id
                     )
                     AND NOT EXISTS (
                         SELECT 1 FROM milestones prior
                          WHERE prior.order_id=m.order_id AND prior.sequence<m.sequence
                            AND NOT EXISTS (
                                SELECT 1 FROM escrow_holds released_hold
                                 WHERE released_hold.order_id=m.order_id
                                   AND released_hold.milestone_id=prior.id
                                   AND released_hold.status='released'
                            )
                     )
                     AND NOT EXISTS (
                         SELECT 1 FROM milestones prior
                          WHERE prior.order_id=m.order_id AND prior.sequence<m.sequence
                            AND prior.status<>'approved'
                     )
                     AND NOT EXISTS (
                         SELECT 1 FROM milestones other
                          WHERE other.order_id=m.order_id AND other.id<>m.id
                            AND other.status IN ('in_progress','submitted','funded')
                     )
                     AND NOT EXISTS (
                         SELECT 1 FROM escrow_holds prior_hold
                         JOIN milestones prior_ms ON prior_ms.id=prior_hold.milestone_id
                          WHERE prior_hold.order_id=m.order_id
                            AND prior_ms.sequence<m.sequence
                            AND prior_hold.status IN ('held','partial')
                     )
                     AND NOT EXISTS (
                         SELECT 1 FROM funding_attempts active
                          WHERE active.milestone_id=m.id
                            AND active.status IN ('prepared','unknown','processor_succeeded')
                     )
                   ORDER BY m.sequence,m.id,h.id
                   LIMIT 2""",
                [order_id],
            ).fetchall()
            if not order or order["status"] != "submitted" or len(recovery_rows) != 1:
                db.rollback()
                return error_response("No submitted milestone found", 409)
            recovery = recovery_rows[0]
            try:
                recovery_charge = buyer_charge_breakdown_cents(recovery["milestone_amount"])
            except ValueError:
                db.rollback()
                return error_response("Committed funding no longer matches milestone amount", 409)
            expected_fingerprint = funding_request_fingerprint(
                operation_key=f"milestone:{recovery['milestone_id']}",
                employer_id=order["employer_id"],
                order_id=order_id,
                milestone_id=recovery["milestone_id"],
                charge=recovery_charge,
            )
            expected_components = (
                recovery_charge["base_cents"],
                recovery_charge["platform_fee_cents"],
                recovery_charge["processing_fee_cents"],
                recovery_charge["total_cents"],
                "component-half-up-v1",
            )
            hold_components = (
                recovery["hold_base_cents"],
                recovery["hold_platform_fee_cents"],
                recovery["hold_processing_fee_cents"],
                recovery["hold_total_cents"],
                recovery["hold_fee_policy_version"],
            )
            attempt_components = (
                recovery["attempt_base_cents"],
                recovery["attempt_platform_fee_cents"],
                recovery["attempt_processing_fee_cents"],
                recovery["attempt_total_cents"],
            )
            if (
                tuple(hold_components) != expected_components
                or tuple(attempt_components) != expected_components[:4]
                or recovery["employer_id"] != order["employer_id"]
                or recovery["request_fingerprint"] != expected_fingerprint
            ):
                db.rollback()
                return error_response("Committed funding no longer matches milestone provenance", 409)
            prior_release_rows = db.execute(
                """SELECT p.id,p.lifecycle_status,h.id AS hold_id FROM payout_release_attempts p
                   JOIN escrow_holds h ON h.release_attempt_id=p.id
                   JOIN milestones m ON m.id=h.milestone_id
                   WHERE p.order_id=? AND p.status='committed'
                     AND p.lifecycle_status IN ('pending','completed')
                     AND p.manual_review_required=0
                     AND h.status='released' AND m.status='approved'
                     AND m.sequence<? ORDER BY m.sequence,p.id""",
                [order_id, recovery["sequence"]],
            ).fetchall()
            if len(prior_release_rows) != 1:
                db.rollback()
                return error_response("Prior payout recovery binding is missing or ambiguous", 409)
            prior_release_attempt_id = prior_release_rows[0]["id"]
            if prior_release_rows[0]["lifecycle_status"] == "pending":
                completed_prior = db.execute(
                    """UPDATE payout_release_attempts SET lifecycle_status='completed',
                         lifecycle_completed_at=COALESCE(lifecycle_completed_at,datetime('now')),
                         updated_at=datetime('now') WHERE id=? AND order_id=?
                         AND status='committed' AND lifecycle_status='pending'
                         AND manual_review_required=0""",
                    [prior_release_attempt_id, order_id],
                )
                if completed_prior.rowcount != 1:
                    db.rollback()
                    return error_response("Prior payout recovery CAS failed", 409)
            updated_ms = db.execute(
                """UPDATE milestones
                   SET status='in_progress', escrow_payment_id=?,
                       funded_at=COALESCE(funded_at,datetime('now'))
                   WHERE id=? AND order_id=? AND status='pending'""",
                [recovery["stripe_payment_intent_id"], recovery["milestone_id"], order_id],
            )
            updated_order = db.execute(
                """UPDATE orders SET status='in_progress', updated_at=datetime('now')
                   WHERE id=? AND status='submitted'""",
                [order_id],
            )
            if updated_ms.rowcount != 1 or updated_order.rowcount != 1:
                db.rollback()
                return error_response("Approval recovery state changed; retry reconciliation", 409)
            push_notification(
                db,
                order["worker_id"],
                "milestone_funded",
                "Next milestone funding recovered",
                f"Milestone {recovery['sequence']} funding was recovered. Continue working!",
                f"/orders/{order_id}",
            )
            audit(db, user["id"], "recover_approve_next_funding", "escrow_hold", recovery["hold_id"], {
                "order_id": order_id,
                "milestone_id": recovery["milestone_id"],
                "funding_attempt_id": recovery["funding_attempt_id"],
            })
            db.commit()
            flush_transactional_notification_emails(db)
            return json_response({
                "ok": True,
                "status": "in_progress",
                "recovered_funding": True,
                "payment_intent_id": recovery["stripe_payment_intent_id"],
            })

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
        # The worker transfer and its legal milestone approval must be durable
        # before attempting a different milestone's processor operation. This
        # prevents ambiguous next-funding loss from stranding a pending payout.
        release_binding = db.execute(
            """SELECT funding_attempt_id,release_attempt_id FROM escrow_holds
               WHERE order_id=? AND milestone_id IS ? AND status='released'""",
            [order_id, ms_id],
        ).fetchone()
        if not release_binding:
            db.rollback()
            return error_response("Released payout binding is missing", 409)
        if release_binding["release_attempt_id"] is not None:
            completed_release = db.execute(
                """UPDATE payout_release_attempts
                   SET lifecycle_status='completed',
                       lifecycle_completed_at=COALESCE(lifecycle_completed_at,datetime('now')),
                       updated_at=datetime('now')
                   WHERE id=? AND order_id=? AND milestone_id IS ? AND status='committed'
                     AND lifecycle_status='pending' AND manual_review_required=0""",
                [release_binding["release_attempt_id"], order_id, ms_id],
            )
            if completed_release.rowcount != 1:
                db.rollback()
                return error_response("Payout lifecycle completion binding changed", 409)
        elif release_binding["funding_attempt_id"] is not None:
            db.rollback()
            return error_response("Live payout release attempt binding is missing", 409)
        audit(db, user['id'], "approve_milestone_release", "order", order_id, {
            "milestone_id": ms_id,
            "payout_release_attempt_id": release_binding["release_attempt_id"],
        })
        db.commit()

        # Check if there are more milestones to fund or reconcile.
        next_ms = db.execute(
            "SELECT * FROM milestones WHERE order_id=? AND status IN ('pending','funded') ORDER BY sequence LIMIT 1",
            [order_id]
        ).fetchone()

        if next_ms:
            if next_ms['status'] == 'funded':
                # Legacy manual pre-funding used a lifecycle state that approval did
                # not consume. Never skip it and falsely complete the order; require
                # reconciliation without making another processor call.
                db.execute("UPDATE orders SET status='disputed', updated_at=datetime('now') WHERE id=?", [order_id])
                push_notification(db, order['worker_id'], "payment_issue",
                    "Payment reconciliation required",
                    f"Pre-funded milestone {next_ms['sequence']} must be reconciled before work continues.",
                    f"/orders/{order_id}")
            else:
                # Fund next milestone
                try:
                    pi_id, mode = fund_escrow_stripe(
                        db, user['id'], float(next_ms['amount']), order_id, next_ms['id'],
                        f"Escrow for order #{order_id} milestone {next_ms['sequence']}"
                    )
                    _settle_committed_milestone_funding(
                        db,
                        order,
                        next_ms,
                        pi_id,
                        expected_order_status="submitted",
                    )
                    push_notification(db, order['worker_id'], "milestone_funded",
                        f"Next milestone funded",
                        f"Milestone {next_ms['sequence']} has been funded. Continue working!",
                        f"/orders/{order_id}")
                except (FundingConflict, FundingReconciliationRequired) as e:
                    return funding_error_response(e)
                except ValueError as e:
                    # Can't fund next milestone — mark order as disputed
                    db.execute("UPDATE orders SET status='disputed', updated_at=datetime('now') WHERE id=?", [order_id])
                    push_notification(db, order['worker_id'], "payment_issue",
                        "Payment issue on next milestone",
                        f"Could not fund next milestone: {str(e)}",
                        f"/orders/{order_id}")
        elif (
            db.execute(
                "SELECT 1 FROM milestones WHERE order_id=? AND status<>'approved' LIMIT 1",
                [order_id],
            ).fetchone()
            or db.execute(
                "SELECT 1 FROM escrow_holds WHERE order_id=? AND status IN ('held','partial') LIMIT 1",
                [order_id],
            ).fetchone()
        ):
            # No ordinary pending/funded next step exists, but the order still has
            # active child work or unresolved escrow. Fail closed rather than
            # claiming completion or incrementing marketplace statistics.
            db.execute("UPDATE orders SET status='disputed', updated_at=datetime('now') WHERE id=?", [order_id])
            push_notification(db, order['worker_id'], "payment_issue",
                "Order reconciliation required",
                f"Order #{order_id} still has unresolved milestone or escrow state after approval.",
                f"/orders/{order_id}")
        else:
            # Positive completion invariant: every milestone is approved and no
            # held or partially settled escrow remains.
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
            # Review request email is a durable outbox row committed with completion.
            push_notification(db, order['employer_id'], "review_request",
                "How was your experience?",
                f"Order #{order_id} is complete! Leave a review to help others find great professionals.",
                f"/orders/{order_id}#review",
                email=True,
                email_dedupe=f"review_request:{order_id}")

        # Complete the exact payout lifecycle in the same transaction as the
        # milestone/order transition. This CAS binds completion to the
        # released hold and committed attempt recognized above.
        release_binding = db.execute(
            """SELECT funding_attempt_id,release_attempt_id FROM escrow_holds
               WHERE order_id=? AND milestone_id IS ? AND status='released'""",
            [order_id, ms_id],
        ).fetchone()
        if release_binding and release_binding["release_attempt_id"] is not None:
            completed_release = db.execute(
                """UPDATE payout_release_attempts
                   SET lifecycle_status='completed',
                       lifecycle_completed_at=COALESCE(lifecycle_completed_at,datetime('now')),
                       updated_at=datetime('now')
                   WHERE id=? AND order_id=? AND milestone_id IS ? AND status='committed'
                     AND lifecycle_status='pending' AND manual_review_required=0""",
                [release_binding["release_attempt_id"], order_id, ms_id],
            )
            if completed_release.rowcount != 1:
                release_state = db.execute(
                    """SELECT status,lifecycle_status,manual_review_required
                       FROM payout_release_attempts WHERE id=?""",
                    [release_binding["release_attempt_id"]],
                ).fetchone()
                if not release_state or tuple(release_state) != ("committed", "completed", 0):
                    db.rollback()
                    return error_response(
                        "Payout lifecycle completion binding changed; manual reconciliation is required.",
                        409,
                    )

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
        order, release_gate = _acquire_order_lifecycle_write_gate(db, order_id)
        if release_gate is not None:
            return error_response(
                "A payout release is pending or requires reconciliation; revision is blocked.",
                409,
            )
        if order['status'] != 'submitted':
            db.rollback()
            return error_response("Order must be submitted to request revision", 409)

        body = get_body()
        try:
            notes = validated_order_notes(body.get("notes"))
        except ValueError as e:
            db.rollback()
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
        order, release_gate = _acquire_order_lifecycle_write_gate(db, order_id)
        if release_gate is not None:
            return error_response(
                "A payout release is pending or requires reconciliation; dispute is blocked.",
                409,
            )
        if order['status'] in ('completed', 'canceled', 'disputed'):
            db.rollback()
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
        order = db.execute("SELECT * FROM orders WHERE id=?", [order_id]).fetchone()
        if not order:
            return error_response("Order not found", 404)
        if order['employer_id'] != user['id'] and not user['is_admin']:
            return error_response("Only the employer or admin can complete an order", 403)
        if order['type'] == 'job_hire':
            return error_response("Job hires must be completed through submitted milestone approval", 409)

        # Persist the immutable intended hold set before the first processor call.
        db.execute("BEGIN IMMEDIATE")
        order = db.execute("SELECT * FROM orders WHERE id=?", [order_id]).fetchone()
        if not order:
            db.rollback()
            return error_response("Order not found", 404)
        if order['employer_id'] != user['id'] and not user['is_admin']:
            db.rollback()
            return error_response("Order completion ownership changed", 409)
        if order['type'] == 'job_hire':
            db.rollback()
            return error_response("Job hires must be completed through submitted milestone approval", 409)
        operation = db.execute(
            "SELECT * FROM order_completion_operations WHERE order_id=?", [order_id]
        ).fetchone()
        if operation is None:
            if order['status'] not in ('submitted', 'in_progress'):
                db.rollback()
                return error_response("Order must be submitted or in_progress to complete", 409)
            holds = db.execute(
                """SELECT h.* FROM escrow_holds h WHERE h.order_id=? AND (
                     h.status='held' OR (h.status='released' AND h.release_attempt_id IS NOT NULL
                       AND EXISTS (SELECT 1 FROM payout_release_attempts p WHERE p.id=h.release_attempt_id
                         AND p.status='committed' AND p.lifecycle_status='pending'
                         AND p.manual_review_required=0))) ORDER BY h.id""", [order_id]
            ).fetchall()
            if not holds:
                db.rollback()
                return error_response("No releasable payment found", 409)
            hold_ids_json = json.dumps([int(hold['id']) for hold in holds], separators=(",", ":"))
            hold_hash = hashlib.sha256(hold_ids_json.encode()).hexdigest()
            db.execute(
                """INSERT INTO order_completion_operations
                   (order_id,employer_id,expected_order_status,hold_ids_json,hold_set_sha256,status)
                   VALUES (?,?,?,?,?,'prepared')""",
                [order_id, order['employer_id'], order['status'], hold_ids_json, hold_hash],
            )
            db.commit()
        else:
            if operation['employer_id'] != order['employer_id']:
                db.rollback()
                return error_response("Completion binding requires manual reconciliation", 409)
            hold_ids_json = operation['hold_ids_json']
            if hashlib.sha256(hold_ids_json.encode()).hexdigest() != operation['hold_set_sha256']:
                db.rollback()
                return error_response("Completion hold binding is corrupt", 409)
            if operation['status'] == 'completed':
                db.commit()
                return json_response({"ok": True, "status": "completed", "idempotent_replay": True})
            db.commit()

        hold_ids = json.loads(hold_ids_json)
        for hold_id in hold_ids:
            hold = db.execute("SELECT * FROM escrow_holds WHERE id=? AND order_id=?", [hold_id, order_id]).fetchone()
            if not hold:
                return error_response("Bound escrow hold disappeared", 409)
            try:
                release_escrow_to_worker(
                    db, order_id, hold['milestone_id'], float(hold['amount']), order['worker_id']
                )
            except ValueError as exc:
                if db.in_transaction:
                    db.rollback()
                return error_response(str(exc), 502)

        # Order, child milestones, every exact payout lifecycle, and operation
        # completion become terminal in one local transaction.
        db.execute("BEGIN IMMEDIATE")
        bound_holds = db.execute(
            f"SELECT * FROM escrow_holds WHERE order_id=? AND id IN ({','.join('?' for _ in hold_ids)}) ORDER BY id",
            [order_id, *hold_ids],
        ).fetchall()
        if len(bound_holds) != len(hold_ids) or any(h['status'] != 'released' for h in bound_holds):
            db.rollback()
            return error_response("Not every bound payout is durably released", 409)
        attempt_ids = [h['release_attempt_id'] for h in bound_holds if h['release_attempt_id'] is not None]
        live_completion = stripe_configured() or PRODUCTION_MODE
        if live_completion and len(attempt_ids) != len(bound_holds):
            db.rollback()
            return error_response("Bound payout lifecycle is incomplete", 409)
        for hold in bound_holds:
            if hold['milestone_id'] is not None:
                updated_ms = db.execute(
                    """UPDATE milestones SET status='approved',released_at=COALESCE(released_at,datetime('now'))
                       WHERE id=? AND order_id=? AND status IN ('in_progress','submitted','approved')""",
                    [hold['milestone_id'], order_id],
                )
                if updated_ms.rowcount != 1:
                    db.rollback()
                    return error_response("Milestone completion CAS failed", 409)
            if hold['release_attempt_id'] is not None:
                completed_attempt = db.execute(
                    """UPDATE payout_release_attempts SET lifecycle_status='completed',
                         lifecycle_completed_at=COALESCE(lifecycle_completed_at,datetime('now')),updated_at=datetime('now')
                       WHERE id=? AND hold_id=? AND order_id=? AND status='committed'
                         AND lifecycle_status IN ('pending','completed') AND manual_review_required=0""",
                    [hold['release_attempt_id'], hold['id'], order_id],
                )
                if completed_attempt.rowcount != 1:
                    db.rollback()
                    return error_response("Payout lifecycle completion CAS failed", 409)
        completed_order = db.execute(
            """UPDATE orders SET status='completed',completed_at=COALESCE(completed_at,datetime('now')),
                 updated_at=datetime('now') WHERE id=? AND status=?""",
            [order_id, operation['expected_order_status'] if operation else order['status']],
        )
        if completed_order.rowcount != 1:
            db.rollback()
            return error_response("Order completion CAS failed", 409)
        if db.execute("SELECT 1 FROM milestones WHERE order_id=? AND status<>'approved'", [order_id]).fetchone():
            db.rollback()
            return error_response("Order has a nonterminal milestone", 409)
        if attempt_ids and db.execute("SELECT 1 FROM payout_release_attempts WHERE id IN (%s) AND lifecycle_status<>'completed'" % ','.join('?' for _ in attempt_ids), attempt_ids).fetchone():
            db.rollback()
            return error_response("Order has a pending payout lifecycle", 409)
        db.execute("UPDATE worker_profiles SET total_orders_completed=total_orders_completed+1 WHERE user_id=?", [order['worker_id']])
        db.execute("UPDATE employer_profiles SET total_orders=total_orders+1 WHERE user_id=?", [order['employer_id']])
        finalized = db.execute(
            "UPDATE order_completion_operations SET status='completed',completed_at=datetime('now') WHERE order_id=? AND status='prepared'",
            [order_id],
        )
        if finalized.rowcount != 1:
            db.rollback()
            return error_response("Completion operation CAS failed", 409)
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
        if not authenticate(db):
            return error_response("Unauthorized", 401)
        # Task 3 intentionally contains no hourly money-movement or lifecycle
        # implementation. Task 4 must introduce a processor ledger and exact hold
        # binding before this endpoint can do anything beyond failing closed.
        return error_response(
            "Hourly settlement is unavailable until roadmap Task 4",
            503,
        )

    elif re.match(r"^/orders/(\d+)/end-contract$", path) and method == "POST":
        if not authenticate(db):
            return error_response("Unauthorized", 401)
        # Ending an hourly contract implies refund/lifecycle settlement and remains
        # wholly unavailable until Task 4 supplies that exact durable workflow.
        return error_response(
            "Hourly settlement is unavailable until roadmap Task 4",
            503,
        )

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
        if _payment_setup_profile_is_frozen(db, user["id"]):
            return error_response("Payment setup is frozen for manual reconciliation.", 409)

        ensure_employer_profile(db, user['id'])
        body = get_body()

        if stripe_configured():
            try:
                # Profile creation is durable before any processor boundary.
                db.commit()
                ep = db.execute(
                    "SELECT stripe_customer_id FROM employer_profiles WHERE user_id=?",
                    [user['id']],
                ).fetchone()
                customer_id = (ep["stripe_customer_id"] if ep else "") or ""
                if not customer_id:
                    customer_result, _ = _payment_setup_operation(
                        db,
                        user["id"],
                        "customer_create",
                        {"email": user["email"], "name": user["name"], "user_id": user["id"]},
                        lambda key: stripe.Customer.create(
                            email=user["email"], name=user["name"],
                            metadata={"user_id": str(user["id"])}, idempotency_key=key,
                        ),
                        lambda value: {
                            "processor_object_id": stripe_attr(value, "id", ""),
                            "customer_id": stripe_attr(value, "id", ""),
                        },
                        lambda conn, result: conn.execute(
                            """UPDATE employer_profiles SET stripe_customer_id=?
                               WHERE user_id=? AND (stripe_customer_id IS NULL OR stripe_customer_id=?)""",
                            [result["customer_id"], user["id"], result["customer_id"]],
                        ),
                    )
                    customer_id = customer_result["customer_id"]
                setup_result, _ = _payment_setup_operation(
                    db,
                    user["id"],
                    "setup_intent_create",
                    {"customer_id": customer_id, "payment_method_types": ["card"]},
                    lambda key: stripe.SetupIntent.create(
                        customer=customer_id, payment_method_types=["card"],
                        metadata={"user_id": str(user["id"])}, idempotency_key=key,
                    ),
                    lambda value: {
                        "processor_object_id": stripe_attr(value, "id", ""),
                        "setup_intent_id": stripe_attr(value, "id", ""),
                        "client_secret": stripe_attr(value, "client_secret", ""),
                    },
                    replay_processor_call=lambda object_id, _key: (
                        stripe.SetupIntent.retrieve(object_id)
                    ),
                )
                return json_response({
                    "client_secret": setup_result["client_secret"],
                    "customer_id": customer_id,
                    "publishable_key": STRIPE_PUBLISHABLE_KEY,
                    "mode": "live",
                })
            except PaymentSetupReconciliationRequired as e:
                return error_response(str(e), 409)
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
        if _payment_setup_profile_is_frozen(db, user["id"]):
            return error_response("Payment setup is frozen for manual reconciliation.", 409)

        body = get_body()
        payment_method_id = body.get("payment_method_id")
        if not payment_method_id:
            return error_response("payment_method_id required")

        if stripe_configured():
            try:
                ep = db.execute(
                    "SELECT stripe_customer_id FROM employer_profiles WHERE user_id=?",
                    [user["id"]],
                ).fetchone()
                customer_id = (ep["stripe_customer_id"] if ep else "") or ""
                if not customer_id:
                    return error_response("Employer Stripe customer setup is required first", 409)
                db.commit()
                _payment_setup_operation(
                    db,
                    user["id"],
                    "payment_method_attach",
                    {"customer_id": customer_id, "payment_method_id": payment_method_id},
                    lambda key: stripe.PaymentMethod.attach(
                        payment_method_id, customer=customer_id, idempotency_key=key,
                    ),
                    lambda value: {
                        "processor_object_id": stripe_attr(value, "id", "") or payment_method_id,
                        "payment_method_id": stripe_attr(value, "id", "") or payment_method_id,
                    },
                )
                _payment_setup_operation(
                    db,
                    user["id"],
                    "customer_modify",
                    {"customer_id": customer_id, "default_payment_method": payment_method_id},
                    lambda key: stripe.Customer.modify(
                        customer_id,
                        invoice_settings={"default_payment_method": payment_method_id},
                        idempotency_key=key,
                    ),
                    lambda value: {
                        "processor_object_id": stripe_attr(value, "id", "") or customer_id,
                        "customer_id": stripe_attr(value, "id", "") or customer_id,
                        "payment_method_id": payment_method_id,
                    },
                    lambda conn, result: conn.execute(
                        """UPDATE employer_profiles SET payment_method_id=?
                           WHERE user_id=? AND stripe_customer_id=?
                             AND (payment_method_id IS NULL OR payment_method_id=?)""",
                        [result["payment_method_id"], user["id"], customer_id,
                         result["payment_method_id"]],
                    ),
                )
            except PaymentSetupReconciliationRequired as e:
                return error_response(str(e), 409)
        else:
            db.execute(
                "UPDATE employer_profiles SET payment_method_id=? WHERE user_id=?",
                [payment_method_id, user['id']],
            )
        audit(db, user['id'], "confirm_employer_payment", "employer_profile", user['id'])
        db.commit()
        return json_response({"ok": True, "payment_method_id": payment_method_id})

    elif path == "/payments/setup-worker" and method == "POST":
        user = authenticate(db)
        if not user:
            return error_response("Unauthorized", 401)
        if _payment_setup_profile_is_frozen(db, user["id"]):
            return error_response("Payment setup is frozen for manual reconciliation.", 409)

        ensure_worker_profile(db, user['id'])

        if stripe_configured():
            try:
                # Profile creation is committed before Account.create.
                db.commit()
                wp = db.execute(
                    "SELECT payout_account_id FROM worker_profiles WHERE user_id=?",
                    [user["id"]],
                ).fetchone()
                account_id = (wp["payout_account_id"] if wp else "") or ""
                if not (account_id.startswith("acct_") and not account_id.startswith("acct_sim_")):
                    account_result, _ = _payment_setup_operation(
                        db,
                        user["id"],
                        "account_create",
                        {"country": "US", "email": user["email"], "type": "express", "user_id": user["id"]},
                        lambda key: stripe.Account.create(
                            type="express", country="US", email=user["email"],
                            capabilities={"transfers": {"requested": True}},
                            metadata={"user_id": str(user["id"])}, idempotency_key=key,
                        ),
                        lambda value: {
                            "processor_object_id": stripe_attr(value, "id", ""),
                            "account_id": stripe_attr(value, "id", ""),
                        },
                        lambda conn, result: conn.execute(
                            """UPDATE worker_profiles
                               SET payout_account_id=?,payout_method='stripe_connect'
                               WHERE user_id=? AND (payout_account_id IS NULL OR payout_account_id='' OR payout_account_id=?)""",
                            [result["account_id"], user["id"], result["account_id"]],
                        ),
                    )
                    account_id = account_result["account_id"]
                body = get_body()
                refresh_requested = bool(body.get("refresh") or body.get("consumed"))
                generation = 1
                previous_link = db.execute(
                    """SELECT request_binding_json,result_json FROM payment_setup_operations
                       WHERE user_id=? AND operation_kind='account_link_create'
                         AND status='committed' AND manual_review_required=0
                       ORDER BY id DESC LIMIT 1""", [user["id"]]
                ).fetchone()
                if previous_link:
                    previous_binding = json.loads(previous_link["request_binding_json"] or "{}")
                    previous_result = json.loads(previous_link["result_json"] or "{}")
                    generation = int(previous_binding.get("generation", 1))
                    if refresh_requested or int(previous_result.get("expires_at", 0) or 0) <= int(time.time()):
                        generation += 1
                capability_id = f"account-link-capability:{user['id']}:{account_id}:{generation}"

                def build_link_result(value):
                    onboarding_url = stripe_attr(value, "url", None)
                    if not isinstance(onboarding_url, str) or not onboarding_url.startswith("https://"):
                        raise ValueError("Stripe AccountLink response lacks a valid HTTPS URL")
                    expires_at = stripe_attr(value, "expires_at", None)
                    if isinstance(expires_at, bool) or not isinstance(expires_at, (int, float)):
                        raise ValueError("Stripe AccountLink response lacks a valid expiration")
                    return {
                        # AccountLink has no durable id. This synthetic identity binds the
                        # non-secret capability generation without persisting its URL.
                        "processor_object_id": capability_id,
                        "account_id": account_id,
                        "generation": generation,
                        "expires_at": int(expires_at),
                        "url": onboarding_url,
                    }
                link_result, _ = _payment_setup_operation(
                    db,
                    user["id"],
                    "account_link_create",
                    {
                        "account_id": account_id,
                        "generation": generation,
                        "purpose": "account_onboarding",
                        "refresh_url": f"{FRONTEND_URL}/payments?connect=refresh",
                        "return_url": f"{FRONTEND_URL}/payments?connect=complete",
                    },
                    lambda key: stripe.AccountLink.create(
                        account=account_id,
                        refresh_url=f"{FRONTEND_URL}/payments?connect=refresh",
                        return_url=f"{FRONTEND_URL}/payments?connect=complete",
                        type="account_onboarding", idempotency_key=key,
                    ),
                    build_link_result,
                    replay_processor_call=lambda _object_id, key: stripe.AccountLink.create(
                        account=account_id,
                        refresh_url=f"{FRONTEND_URL}/payments?connect=refresh",
                        return_url=f"{FRONTEND_URL}/payments?connect=complete",
                        type="account_onboarding", idempotency_key=key,
                    ),
                    replay_result_builder=build_link_result,
                )
                audit(db, user['id'], "setup_worker_payout", "worker_profile", user['id'])
                db.commit()
                return json_response({
                    "ok": True,
                    "onboarding_url": link_result["url"],
                    "account_id": account_id,
                    "mode": "live",
                })
            except PaymentSetupReconciliationRequired as e:
                return error_response(str(e), 409)
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
                    except STRIPE_ERROR:
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
        if body.get("order_id") is None:
            return error_response("order_id required")
        try:
            order_id = bounded_integer(body.get("order_id"), "order_id", 1, 9_223_372_036_854_775_807)
            milestone_id = (
                None
                if body.get("milestone_id") is None
                else bounded_integer(body.get("milestone_id"), "milestone_id", 1, 9_223_372_036_854_775_807)
            )
        except ValueError as e:
            return error_response(str(e), 400)

        order = db.execute("SELECT * FROM orders WHERE id=?", [order_id]).fetchone()
        if not order:
            return error_response("Order not found", 404)
        if order['employer_id'] != user['id']:
            return error_response("Forbidden", 403)

        # Serialize the final pre-processor eligibility check so two requests
        # cannot both observe the same target as unfunded.
        if not db.in_transaction:
            db.execute("BEGIN IMMEDIATE")
        order = db.execute("SELECT * FROM orders WHERE id=?", [order_id]).fetchone()
        if not order:
            return error_response("Order not found", 404)
        if order['employer_id'] != user['id']:
            return error_response("Forbidden", 403)
        if db.execute(
            "SELECT 1 FROM hourly_contracts WHERE order_id=? LIMIT 1", [order_id]
        ).fetchone():
            return error_response(
                "Hourly funding and settlement are deferred to Task 4.", 503
            )

        milestone = None
        if milestone_id is not None:
            milestone = db.execute(
                "SELECT * FROM milestones WHERE id=? AND order_id=?",
                [milestone_id, order_id],
            ).fetchone()
            if not milestone:
                return error_response("Milestone not found for this order", 404)

        try:
            authoritative_amount = milestone["amount"] if milestone is not None else order["total_amount"]
            amount_cents = money_to_cents(
                authoritative_amount,
                "milestone amount" if milestone is not None else "order total amount",
            )
            if body.get("amount") is not None:
                requested_cents = money_to_cents(body.get("amount"), "amount")
                if requested_cents != amount_cents:
                    return error_response("amount must match the authoritative funded amount", 409)
        except ValueError as e:
            return error_response(str(e), 400)
        if amount_cents <= 0:
            return error_response("amount must be positive", 400)
        amount = amount_cents / 100
        funding_identity = (
            f"milestone:{milestone_id}" if milestone_id is not None else f"order:{order_id}:full"
        )

        exact_replay = db.execute(
            "SELECT * FROM escrow_holds WHERE order_id=? AND funding_identity=? LIMIT 1",
            [order_id, funding_identity],
        ).fetchone()
        if exact_replay:
            replay_base_cents = exact_replay["base_amount_cents"]
            if replay_base_cents is None:
                try:
                    replay_base_cents = money_to_cents(exact_replay["amount"], "funded amount")
                except ValueError:
                    return error_response("Existing funding requires processor reconciliation", 409)
            if (replay_base_cents != amount_cents
                    or exact_replay["milestone_id"] != milestone_id
                    or not exact_replay["stripe_payment_intent_id"]):
                return error_response("Existing funding conflicts with this operation", 409)

            replay_pi_id = exact_replay["stripe_payment_intent_id"]
            replay_mode = "replayed"
            if exact_replay["funding_attempt_id"] is not None:
                try:
                    replay_pi_id, replay_mode = fund_escrow_stripe(
                        db,
                        user["id"],
                        amount,
                        order_id,
                        milestone_id,
                        "Owner-approved checkout funding replay",
                        funding_identity=funding_identity,
                    )
                except (FundingPaymentFailed, FundingConflict, FundingReconciliationRequired) as exc:
                    return funding_error_response(exc)
                if replay_pi_id != exact_replay["stripe_payment_intent_id"]:
                    return error_response("Existing funding conflicts with processor provenance", 409)

            hold_status = exact_replay["status"]
            replay_is_valid = False
            if milestone is not None:
                milestone_status = milestone["status"]
                order_status = order["status"]
                first_milestone = db.execute(
                    "SELECT id FROM milestones WHERE order_id=? ORDER BY sequence, id LIMIT 1",
                    [order_id],
                ).fetchone()
                hold_count = db.execute(
                    "SELECT COUNT(*) FROM escrow_holds WHERE order_id=?",
                    [order_id],
                ).fetchone()[0]
                recoverable_pending = (
                    milestone_status == "pending"
                    and exact_replay["funding_attempt_id"] is not None
                )
                if (order_status == "pending" and (milestone_status == "funded" or recoverable_pending)
                        and hold_status == "held" and first_milestone
                        and first_milestone["id"] == milestone_id and hold_count == 1):
                    updated_ms = db.execute(
                        """UPDATE milestones
                           SET status='in_progress', escrow_payment_id=COALESCE(escrow_payment_id,?),
                               funded_at=COALESCE(funded_at,datetime('now'))
                           WHERE id=? AND order_id=? AND status=?""",
                        [replay_pi_id, milestone_id, order_id, milestone_status],
                    )
                    updated_order = db.execute(
                        "UPDATE orders SET status='in_progress', updated_at=datetime('now') WHERE id=? AND status='pending'",
                        [order_id],
                    )
                    if updated_ms.rowcount != 1 or updated_order.rowcount != 1:
                        raise RuntimeError("Funding replay state changed during normalization")
                    audit(db, user['id'], "normalize_funding_replay", "escrow_hold", exact_replay["id"], {
                        "order_id": order_id,
                        "milestone_id": milestone_id,
                        "funding_identity": funding_identity,
                    })
                    replay_is_valid = True
                elif hold_status == "held" and (
                    (order_status == "in_progress" and milestone_status == "in_progress")
                    or (order_status == "submitted" and milestone_status == "submitted")
                    or (order_status == "revision_requested" and milestone_status == "in_progress")
                ):
                    replay_is_valid = True
                elif (hold_status == "released" and milestone_status == "approved"
                        and order_status in ("in_progress", "submitted", "revision_requested", "completed")):
                    replay_is_valid = True
            else:
                order_status = order["status"]
                has_milestones = db.execute(
                    "SELECT 1 FROM milestones WHERE order_id=? LIMIT 1",
                    [order_id],
                ).fetchone()
                if not has_milestones and order_status == "pending" and hold_status == "held":
                    updated_order = db.execute(
                        "UPDATE orders SET status='in_progress', updated_at=datetime('now') WHERE id=? AND status='pending'",
                        [order_id],
                    )
                    if updated_order.rowcount != 1:
                        raise RuntimeError("Legacy aggregate replay state changed during normalization")
                    audit(db, user['id'], "normalize_legacy_funding_replay", "escrow_hold", exact_replay["id"], {
                        "order_id": order_id,
                        "funding_identity": funding_identity,
                    })
                    replay_is_valid = True
                elif not has_milestones and (
                    (hold_status == "held" and order_status in ("in_progress", "submitted", "revision_requested"))
                    or (hold_status == "released" and order_status == "completed")
                ):
                    replay_is_valid = True

            if not replay_is_valid:
                return error_response("Existing funding requires lifecycle reconciliation", 409)
            db.commit()
            return json_response({
                "ok": True,
                "payment_intent_id": replay_pi_id,
                "mode": replay_mode,
                "amount": amount,
                "idempotent_replay": True,
            })

        if order["status"] != "pending":
            return error_response("Order is not eligible for new funding", 409)

        if milestone is not None:
            if milestone["status"] != "pending":
                return error_response("Milestone is not eligible for funding", 409)
            first_milestone = db.execute(
                "SELECT id FROM milestones WHERE order_id=? ORDER BY sequence, id LIMIT 1",
                [order_id],
            ).fetchone()
            if not first_milestone or first_milestone["id"] != milestone_id:
                return error_response("Only the first milestone can start a pending order", 409)
            overlapping_hold = db.execute(
                "SELECT id FROM escrow_holds WHERE order_id=? LIMIT 1",
                [order_id],
            ).fetchone()
            if overlapping_hold:
                return error_response("Order is already funded", 409)
        else:
            if db.execute("SELECT 1 FROM milestones WHERE order_id=? LIMIT 1", [order_id]).fetchone():
                return error_response("milestone_id required for orders with milestones", 409)
            if db.execute("SELECT 1 FROM escrow_holds WHERE order_id=? LIMIT 1", [order_id]).fetchone():
                return error_response("Order is already funded", 409)

        try:
            pi_id, mode = fund_escrow_stripe(
                db,
                user['id'],
                amount,
                order_id,
                milestone_id,
                "Owner-approved checkout funding",
                funding_identity=funding_identity,
            )
        except (FundingPaymentFailed, FundingConflict, FundingReconciliationRequired) as exc:
            return funding_error_response(exc)

        lifecycle_replayed = False
        if milestone_id is not None:
            try:
                _settle_committed_milestone_funding(
                    db,
                    order,
                    milestone,
                    pi_id,
                    expected_order_status="pending",
                )
            except (FundingConflict, FundingReconciliationRequired) as exc:
                return funding_error_response(exc)
        else:
            try:
                _settle_committed_order_funding(
                    db,
                    order,
                    pi_id,
                    expected_order_status="pending",
                )
            except (FundingConflict, FundingReconciliationRequired) as exc:
                return funding_error_response(exc)

        if not lifecycle_replayed:
            audit(db, user['id'], "fund_escrow", "escrow_hold", None, {"order_id": order_id, "amount": amount})
        db.commit()
        return json_response({
            "ok": True,
            "payment_intent_id": pi_id,
            "mode": mode,
            "amount": amount,
            "idempotent_replay": lifecycle_replayed or mode == "replayed",
        })

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

        if event_type in {
            'payment_intent.succeeded',
            'payment_intent.payment_failed',
            'payment_intent.canceled',
            'payment_intent.processing',
        }:
            funding_result = reconcile_funding_intent_event(
                db, data, event.get("id")
            )
            if funding_result.get('outcome') == 'failed':
                metadata = data.get('metadata', {})
                employer_id = str(metadata.get('employer_id', ''))
                if employer_id.isdigit():
                    push_notification(
                        db, int(employer_id), "payment_failed", "Payment failed",
                        "An escrow payment failed. Please update your payment method.",
                        "/payments",
                    )
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
        return error_response(
            "Refund and dispute settlement are deferred to Task 4.", 503
        )

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
        if not isinstance(scopes, list):
            return error_response("scopes must be a list", 400)
        if not scopes:
            return error_response("scopes must not be empty", 400)
        for index, scope in enumerate(scopes):
            if not isinstance(scope, str):
                return error_response(f"Invalid scope type at index {index}", 400)
            if scope not in ALLOWED_API_KEY_SCOPES:
                return error_response(f"Invalid scope: {scope}", 400)
        scopes = sorted(set(scopes))

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
