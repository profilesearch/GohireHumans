#!/usr/bin/env python3
"""Read-only funding-attempt recovery report.

This command opens a static SQLite snapshot with mode=ro&immutable=1 and performs only
Stripe retrieve/search calls. It never updates local state, creates SQLite sidecars, or
creates/retries a PaymentIntent. The JSON output is an operator decision aid for the
authenticated runtime reconciliation path.
"""

import argparse
import json
import os
import re
import sqlite3
import stat
import sys
from pathlib import Path

BACKEND_DIR = Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

import api_core  # noqa: E402


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--db", required=True, help="Path to a static checkpointed SQLite snapshot"
    )
    parser.add_argument("--attempt-id", type=int)
    parser.add_argument("--order-id", type=int)
    parser.add_argument("--operation-key")
    parser.add_argument(
        "--status",
        action="append",
        choices=["prepared", "unknown", "processor_succeeded", "failed", "committed"],
        help="Repeat to include multiple states (default: unresolved states).",
    )
    parser.add_argument("--limit", type=int, default=100)
    parser.add_argument("--json", action="store_true", help="Emit machine-readable JSON")
    destination = parser.add_mutually_exclusive_group(required=True)
    destination.add_argument(
        "--output", help="Create a new owner-only (0600) report file"
    )
    destination.add_argument(
        "--stdout", action="store_true",
        help="Explicitly accept terminal/pipe exposure instead of a protected file",
    )
    parser.add_argument(
        "--reveal-sensitive", action="store_true",
        help="Reveal fingerprints, processor IDs, and error details (never secrets)",
    )
    args = parser.parse_args()
    if args.reveal_sensitive and args.stdout:
        parser.error("--reveal-sensitive requires protected --output; stdout is not permitted")
    return args


def connect_read_only(path):
    requested = Path(path).expanduser()
    if requested.is_symlink():
        raise PermissionError("Database snapshot must not be a symlink.")
    resolved = requested.resolve(strict=True)
    metadata = resolved.stat()
    if not stat.S_ISREG(metadata.st_mode):
        raise PermissionError("Database snapshot must be a regular file.")
    if metadata.st_uid != os.geteuid() or stat.S_IMODE(metadata.st_mode) & 0o077:
        raise PermissionError(
            "Database snapshot must be owned by the current OS user with owner-only permissions (0600)."
        )
    active_sidecars = [
        candidate
        for candidate in (
            Path(str(resolved) + "-wal"),
            Path(str(resolved) + "-shm"),
            Path(str(resolved) + "-journal"),
        )
        if candidate.exists()
    ]
    if active_sidecars:
        raise RuntimeError(
            "A static checkpointed SQLite snapshot is required; active sidecar files were found."
        )
    uri = f"{resolved.as_uri()}?mode=ro&immutable=1"
    db = sqlite3.connect(uri, uri=True)
    opened = Path(db.execute("PRAGMA database_list").fetchone()[2]).resolve(strict=True)
    if opened != resolved:
        db.close()
        raise RuntimeError("SQLite opened a different database than requested.")
    db.row_factory = sqlite3.Row
    db.execute("PRAGMA query_only=ON")
    return db, resolved


_NEVER_REVEAL_KEY_FRAGMENTS = (
    "secret", "password", "passwd", "token", "authorization", "credential",
    "api_key", "apikey",
)
_DEFAULT_REDACT_KEY_FRAGMENTS = (
    "fingerprint", "stripe_", "processor_object_id", "processor_idempotency",
    "payment_intent_id", "intent_id", "transfer_id", "destination_account",
    "error_code", "error_message", "error_detail",
)
_SECRET_VALUE_PATTERN = re.compile(
    r"(?:sk_(?:live|test|restricted)|rk_(?:live|test)|whsec_|(?:seti|pi)_[^\s\"']*_secret_|ghh_[A-Za-z0-9_-]+|authorization\s*:\s*(?:bearer|basic)\s+\S+|(?:bearer|basic)\s+[A-Za-z0-9._~+/@=-]+|-----BEGIN [A-Z ]*PRIVATE KEY-----)",
    re.IGNORECASE,
)


def redact_report(value, reveal_sensitive=False, key=""):
    """Redact operator-sensitive fields, including known serialized JSON evidence."""
    normalized = re.sub(r"[^a-z0-9]+", "_", str(key).lower())
    compact = normalized.replace("_", "")
    if isinstance(value, str) and normalized.endswith("_json"):
        try:
            decoded = json.loads(value)
        except (TypeError, ValueError, json.JSONDecodeError):
            decoded = None
        if isinstance(decoded, (dict, list)):
            safe_decoded = redact_report(decoded, reveal_sensitive, "")
            return json.dumps(safe_decoded, sort_keys=True, separators=(",", ":"))
    if any(fragment in normalized or fragment.replace("_", "") in compact
           for fragment in _NEVER_REVEAL_KEY_FRAGMENTS):
        return "[REDACTED]"
    if not reveal_sensitive and (
        any(fragment in normalized for fragment in _DEFAULT_REDACT_KEY_FRAGMENTS)
        or ("processor" in normalized and normalized.endswith("_id"))
    ):
        return "[REDACTED]"
    if isinstance(value, dict):
        return {
            str(item_key): redact_report(item, reveal_sensitive, str(item_key))
            for item_key, item in value.items()
        }
    if isinstance(value, list):
        return [redact_report(item, reveal_sensitive, key) for item in value]
    if isinstance(value, tuple):
        return [redact_report(item, reveal_sensitive, key) for item in value]
    if isinstance(value, str) and _SECRET_VALUE_PATTERN.search(value):
        return "[REDACTED]"
    return value


def write_protected_output(path, text):
    """Create, never overwrite, an owner-only report in an owner-only directory."""
    output = Path(path).expanduser()
    output.parent.mkdir(parents=True, mode=0o700, exist_ok=True)
    parent = output.parent.resolve(strict=True)
    parent_metadata = parent.stat()
    if parent_metadata.st_uid != os.geteuid() or stat.S_IMODE(parent_metadata.st_mode) & 0o077:
        raise PermissionError("Output directory must be owned by the current user and mode 0700.")
    descriptor = os.open(
        output,
        os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0),
        0o600,
    )
    try:
        encoded = text.encode("utf-8")
        with os.fdopen(descriptor, "wb", closefd=False) as stream:
            stream.write(encoded)
            stream.flush()
            os.fsync(stream.fileno())
    finally:
        os.close(descriptor)
    return output.resolve(strict=True)


def main():
    args = parse_args()
    if args.limit < 1 or args.limit > 1000:
        raise SystemExit("--limit must be between 1 and 1000")
    db, resolved = connect_read_only(args.db)
    try:
        conditions = []
        values = []
        if args.attempt_id is not None:
            conditions.append("a.id=?")
            values.append(args.attempt_id)
        if args.order_id is not None:
            conditions.append("a.order_id=?")
            values.append(args.order_id)
        if args.operation_key is not None:
            conditions.append("a.operation_key=?")
            values.append(args.operation_key)
        statuses = args.status or ["prepared", "unknown", "processor_succeeded"]
        placeholders = ",".join("?" for _ in statuses)
        if args.status:
            conditions.append(f"a.status IN ({placeholders})")
            values.extend(statuses)
        else:
            manual_codes = tuple(sorted(api_core.MANUAL_REVIEW_FUNDING_ERROR_CODES))
            manual_placeholders = ",".join("?" for _ in manual_codes)
            conditions.append(
                f"""(a.status IN ({placeholders})
                      OR a.error_code IN ({manual_placeholders})
                      OR EXISTS (
                          SELECT 1 FROM funding_attempt_conflict_evidence e
                          WHERE e.attempt_id=a.id
                      ))"""
            )
            values.extend([*statuses, *manual_codes])
        where = " WHERE " + " AND ".join(conditions) if conditions else ""
        rows = db.execute(
            "SELECT a.* FROM funding_attempts a" + where + " ORDER BY a.id LIMIT ?",
            [*values, args.limit],
        ).fetchall()

        reports = []
        for row in rows:
            inspection = api_core.reconcile_funding_attempt(db, row, apply=False)
            conflicts = [
                dict(conflict)
                for conflict in db.execute(
                    """SELECT * FROM funding_attempt_conflict_evidence
                       WHERE attempt_id=? ORDER BY id""",
                    [row["id"]],
                ).fetchall()
            ]
            manual_review = bool(
                conflicts
                or row["error_code"] in api_core.MANUAL_REVIEW_FUNDING_ERROR_CODES
            )
            reports.append({
                "attempt_id": row["id"],
                "operation_key": row["operation_key"],
                "attempt_number": row["attempt_number"],
                "request_fingerprint": row["request_fingerprint"],
                "order_id": row["order_id"],
                "milestone_id": row["milestone_id"],
                "local_status": row["status"],
                "stripe_payment_intent_id": row["stripe_payment_intent_id"],
                "local_error_code": row["error_code"],
                "local_error_message": row["error_message"],
                "conflict_evidence": conflicts,
                "processor_evidence": inspection,
                "recommended_action": (
                    "runtime_reconciliation_can_commit"
                    if not manual_review and inspection.get("outcome") == "succeeded"
                    else "review_before_any_retry"
                ),
            })
        payload = {
            "read_only": True,
            "database": str(resolved),
            "stripe_configured": api_core.stripe_configured(),
            "attempt_count": len(reports),
            "attempts": reports,
        }
        safe_payload = redact_report(payload, reveal_sensitive=args.reveal_sensitive)
        if args.json:
            rendered = json.dumps(safe_payload, sort_keys=True) + "\n"
        else:
            rendered = json.dumps(safe_payload, indent=2, sort_keys=True) + "\n"
        if args.output:
            write_protected_output(args.output, rendered)
        else:
            sys.stdout.write(rendered)
        return 0
    finally:
        db.close()


if __name__ == "__main__":
    raise SystemExit(main())
