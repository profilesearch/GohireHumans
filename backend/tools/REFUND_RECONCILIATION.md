# Refund attempt reconciliation

`reconcile_refund_attempts.py` produces a redacted, read-only report for existing durable refund attempts. It opens SQLite with `mode=ro&immutable=1`, enables `PRAGMA query_only`, and never settles or changes local state.

Run from `backend/`:

```sh
STRIPE_SECRET_KEY=... .venv/bin/python tools/reconcile_refund_attempts.py \
  --database /path/to/snapshot.db --attempt-id 123
```

Processor access is strictly limited to `Refund.retrieve` for an attempt with a known refund ID, or fully paginated, PaymentIntent-scoped `Refund.list` for an attempt without one. A candidate is exact only when refund ID (when already bound), PaymentIntent, integer-cent amount, currency, supported status, and every durable metadata binding match. The tool never calls create/update/cancel and does not treat its report as settlement evidence. Apply reconciliation only through the signed webhook or admin runtime flow after review.

The snapshot path is URI-encoded before SQLite opens it, and the tool verifies `PRAGMA database_list` resolves to the requested file. Filenames containing URI delimiters are supported. A non-empty `-wal` sibling is rejected because immutable mode would ignore uncheckpointed transactions; checkpoint/copy a fresh snapshot first.

Output omits raw refund IDs, payment intents, metadata, user information, reasons, notes, raw errors, and secrets. It exposes only a short SHA-256 digest for a unique exact refund ID. `exact_candidate=false` or any count other than one requires manual review. Tool failures return only `reconciliation_read_failed`; investigate through protected operator logs without copying secrets into reports.
