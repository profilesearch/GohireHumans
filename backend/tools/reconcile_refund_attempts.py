#!/usr/bin/env python3
"""Produce a redacted, read-only report for durable refund attempts."""
import argparse
import hashlib
import json
import os
import sqlite3
import sys
from collections.abc import Mapping
from pathlib import Path
from urllib.parse import quote

SUPPORTED_STATUSES = {"succeeded", "pending", "requires_action", "failed", "canceled"}


def val(obj, key, default=None):
    if isinstance(obj, dict):
        return obj.get(key, default)
    return getattr(obj, key, default)


def metadata_for(row):
    return {
        "operation_key": str(row["operation_key"]),
        "attempt_number": str(row["attempt_number"]),
        "request_fingerprint": str(row["request_fingerprint"]),
        "attempt_id": str(row["id"]),
        "order_id": str(row["order_id"]),
        "dispute_id": str(row["dispute_id"]),
        "hold_id": str(row["hold_id"]),
        "funding_attempt_id": str(row["funding_attempt_id"]),
    }


def exact_bindings(row, evidence):
    metadata = val(evidence, "metadata", {})
    if not isinstance(metadata, Mapping):
        return False
    evidence_id = val(evidence, "id")
    expected_id = row["processor_refund_id"]
    return bool(
        isinstance(evidence_id, str)
        and evidence_id
        and (not expected_id or evidence_id == expected_id)
        and val(evidence, "payment_intent") == row["payment_intent_id"]
        and type(val(evidence, "amount")) is int
        and val(evidence, "amount") == row["amount_cents"]
        and val(evidence, "currency") == row["currency"]
        and str(val(evidence, "status") or "") in SUPPORTED_STATUSES
        and all(str(metadata.get(key, "")) == value for key, value in metadata_for(row).items())
    )


def refund_id_digest(evidence):
    refund_id = val(evidence, "id")
    if not isinstance(refund_id, str) or not refund_id:
        return None
    return hashlib.sha256(refund_id.encode()).hexdigest()[:16]


def list_exact_candidates(row, refund_api):
    matches = []
    starting_after = None
    seen_cursors = set()
    while True:
        kwargs = {"payment_intent": row["payment_intent_id"], "limit": 100}
        if starting_after:
            kwargs["starting_after"] = starting_after
        page = refund_api.list(**kwargs)
        data = val(page, "data", []) or []
        if not isinstance(data, (list, tuple)):
            raise ValueError("malformed processor page")
        matches.extend(item for item in data if exact_bindings(row, item))
        if not val(page, "has_more", False):
            break
        if not data:
            raise ValueError("malformed processor pagination")
        cursor = val(data[-1], "id")
        if not isinstance(cursor, str) or not cursor or cursor in seen_cursors:
            raise ValueError("malformed processor pagination")
        seen_cursors.add(cursor)
        starting_after = cursor
    return matches


def build_report(db, refund_api, attempt_id=None, limit=100):
    query = "SELECT * FROM refund_attempts"
    args = []
    if attempt_id is not None:
        query += " WHERE id=?"
        args.append(attempt_id)
    query += " ORDER BY id LIMIT ?"
    args.append(max(1, min(int(limit), 500)))
    attempts = []
    for row in db.execute(query, args):
        source = "retrieve" if row["processor_refund_id"] else "list"
        if row["processor_refund_id"]:
            evidence = refund_api.retrieve(row["processor_refund_id"])
            matches = [evidence] if exact_bindings(row, evidence) else []
        else:
            matches = list_exact_candidates(row, refund_api)
        evidence = matches[0] if len(matches) == 1 else None
        attempts.append({
            "attempt_id": row["id"],
            "local_status": row["status"],
            "source": source,
            "exact_candidate": len(matches) == 1,
            "exact_candidate_count": len(matches),
            "processor_status": str(val(evidence, "status")) if evidence else None,
            "processor_refund_id_sha256_16": refund_id_digest(evidence) if evidence else None,
        })
    return {"mode": "read-only", "attempts": attempts}


def open_readonly_snapshot(path):
    database = Path(path).resolve()
    if not database.is_file():
        raise ValueError("snapshot does not exist")
    wal = Path(str(database) + "-wal")
    if wal.exists() and wal.stat().st_size:
        raise ValueError("snapshot has an uncheckpointed WAL")
    uri = "file:" + quote(database.as_posix(), safe="/") + "?mode=ro&immutable=1"
    db = sqlite3.connect(uri, uri=True)
    try:
        db.row_factory = sqlite3.Row
        db.execute("PRAGMA query_only=ON")
        opened = db.execute("PRAGMA database_list").fetchone()[2]
        if Path(opened).resolve() != database:
            raise ValueError("SQLite opened a different snapshot")
        return db
    except Exception:
        db.close()
        raise


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", default=os.environ.get("DATABASE_PATH"))
    parser.add_argument("--attempt-id", type=int)
    parser.add_argument("--limit", type=int, default=100)
    args = parser.parse_args(argv)
    if not args.database:
        parser.error("--database or DATABASE_PATH is required")

    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    import api_core

    db = None
    try:
        db = open_readonly_snapshot(args.database)
        report = build_report(db, api_core.stripe.Refund, args.attempt_id, args.limit)
        print(json.dumps(report, sort_keys=True, indent=2))
        return 0
    except Exception:
        print(json.dumps({"mode": "read-only", "error": "reconciliation_read_failed"}), file=sys.stderr)
        return 2
    finally:
        if db is not None:
            db.close()


if __name__ == "__main__":
    raise SystemExit(main())
