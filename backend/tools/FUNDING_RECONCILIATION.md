# Funding-attempt reconciliation (read-only)

`reconcile_funding_attempts.py` inspects durable local funding attempts against Stripe without changing SQLite or creating/retrying any PaymentIntent.

## Safety contract

- SQLite is opened with `mode=ro&immutable=1`, exact `PRAGMA database_list` path readback, and `PRAGMA query_only=ON`; inspection cannot create `-wal`, `-shm`, journal, or misparsed sibling database files.
- `immutable=1` requires a static, transactionally consistent SQLite snapshot. Use SQLite's backup API or an offline/checkpointed copy; do not point this report at a database that another process may modify while inspection runs.
- Stripe operations are limited to `PaymentIntent.retrieve` or `PaymentIntent.search`.
- The tool never performs a processor create, confirm, capture, cancel, refund, or transfer.
- The tool never updates a funding attempt, escrow hold, order, or milestone.
- Output is evidence for an operator decision. It is not authorization to retry an ambiguous charge.
- Run as a dedicated least-privilege OS account. The snapshot must be a regular,
  non-symlink file owned by that account with mode `0600`; group/world-readable
  databases are rejected before SQLite opens them.
- Prefer `--output` into an owner-only (`0700`) directory. The tool atomically
  creates a new `0600` report and refuses to overwrite an existing file.
- Request fingerprints, processor identifiers, and local error details are
  redacted by default. `--reveal-sensitive` is an explicit privileged exception;
  it still never reveals client secrets, API keys, credentials, passwords, tokens,
  or authorization material. Treat revealed reports as financial support data.
- `--stdout` must be supplied explicitly and can expose data through terminal
  scrollback, shell pipelines, CI logs, and redirection defaults. Do not use it on
  shared terminals or logging consoles.
- Milestone funding uses the single canonical operation key `milestone:<id>` across service checkout, hiring, lifecycle, and manual recovery routes.
- The generic manual funding route rejects every order linked to an hourly contract, including historical hybrid rows that also have a milestone; hourly obligations remain isolated to their disabled dedicated settlement lifecycle.
- If next-milestone funding committed but approval crashed before lifecycle state advanced, an exact authenticated approval retry may materialize only one conflict-free `committed` attempt/`held` hold after revalidating amount, fee components, currency, employer, operation key, request fingerprint, released prior holds, and processor intent provenance. This recovery performs no processor create or transfer.
- If processor create/retrieve/search/webhook evidence presents a different processor intent ID than the durable attempt, the original intent provenance is retained and the attempt is marked `processor_intent_conflict`; retry and committed-hold replay stay blocked pending manual reconciliation.
- Every distinct contradictory intent/source/status/snapshot/owner observation is stored in append-only `funding_attempt_conflict_evidence`. Signed Stripe event IDs participate in exact-redelivery deduplication; a different event remains separate evidence even when it references the same intent. Database triggers reject evidence update, delete, and replacement.
- `funding_attempts.error_code` and `error_message` are only denormalized freeze summaries. The runtime retry, hold-commit, and escrow-release gates also read structured evidence directly, so removing or overlooking a scalar summary cannot make a conflicted obligation automatic again.
- If a formerly definitive failure later produces success evidence after attempt N+1 exists, both evidence trails are retained and the newest attempt is marked `prior_attempt_success_conflict` even if it is already committed; no automatic retry, committed-hold replay, or new commit is allowed.
- If fixed-price job-hire funding commits but the following lifecycle transaction fails, an exact retry can finish the milestone/job/application transition without processor I/O only when the durable hire-request fingerprint, selected application, milestone schedule, committed attempt, hold, fee policy, and processor intent all match. If the lifecycle committed but its HTTP response was lost, the same exact retry replays the durable response without another notification or processor call. Newer, unresolved, contradictory, cross-operation, or cross-milestone attempt evidence blocks both paths; changed inputs and legacy rows without a fingerprint fail closed. Exact recovery/replay remains available while the new-hiring feature gate is paused.
- Every reconciliation-owned SQLite writer transaction rolls back on an escaping exception so a processor/webhook race cannot leave the database locked.
- Startup validates the canonical `funding_attempts` and `funding_attempt_conflict_evidence` table SQL, not only column names. Same-name tables with missing primary keys, foreign keys, `NOT NULL`, defaults, or `CHECK` constraints fail readiness even if canonical indexes and triggers are present.
- Service checkout, manual milestone/aggregate funding, and next-milestone approval reacquire a fresh writer transaction after funding commitment and compare the complete authoritative order/milestone snapshot, committed attempt, held escrow, fee components, fingerprint, and processor intent before activating lifecycle state. Concurrent amount, identity, or status drift returns reconciliation-required without creating another PaymentIntent.
- Escrow release commits any inbound local transaction before Stripe account lookup/Transfer creation, uses the stable key `escrow-release:<order>:<milestone|full>`, and then reacquires a writer lock for an exact held-row CAS. The authoritative Transfer ID is persisted on the hold; concurrent exact replay accepts only the identical released transfer, while zero-row or divergent settlement fails closed. No SQLite writer lock spans processor I/O.
- Hourly job hiring/funding and administrator dispute settlement are Task 4 capabilities. Task 3 returns HTTP 503 before order/funding/processor or settlement mutation; it does not create a partial ledger for either deferred workflow.
- A Connect destination is payout-ready only when Stripe explicitly returns `charges_enabled=true`, `payouts_enabled=true`, and `capabilities.transfers=active`.
- Transfer create success is local-committable only with complete matching `id`, amount, currency, destination, and metadata. A sparse response may be resolved by read-only `Transfer.retrieve`; otherwise the payout freezes as unknown/manual-review.
- Startup performs protected-object prevalidation, all schema/migration work, final validation, and commit under one `BEGIN IMMEDIATE`. Unexpected triggers on any funding/payout attempt or conflict-evidence table fail readiness and roll back the entire initialization.
- API-key usage is inserted as a durable authorized request intent before route/processor work and finalized in a short post-route transaction. Valid scope denials are terminal audited rows; invalid keys are unattributed. Stale started intents are explicitly recoverable as `abandoned`.
- Transactional review email uses a durable outbox row committed with the notification. Delivery happens after the writer transaction and uses the outbox dedupe key as the provider idempotency key.
- Serialized `*_json` reconciliation evidence is parsed and recursively redacted. Secret capabilities remain hidden even with `--reveal-sensitive`.

## Usage

Create a static backup as the dedicated operator, lock down both snapshot and
destination directory, then run from `backend/` in the deployed application's
Python environment:

```bash
install -d -m 0700 /secure/ghh-reconciliation
chmod 0600 /secure/ghh-reconciliation/snapshot.db
python tools/reconcile_funding_attempts.py \
  --db /secure/ghh-reconciliation/snapshot.db \
  --output /secure/ghh-reconciliation/report.json \
  --json
```

The output path must not already exist. Move or securely remove an old report
before choosing a new path; the tool will not truncate it.

With no `--status`, the report includes ordinary unresolved attempts plus any conflicted attempt, including `failed` and `committed`. Supplying one or more `--status` options explicitly narrows that status set; exact attempt/order/operation filters still apply.

Narrow the report when possible:

```bash
python tools/reconcile_funding_attempts.py --db /secure/snapshot.db --attempt-id 123 --output /secure/attempt-123.json --json
python tools/reconcile_funding_attempts.py --db /secure/snapshot.db --order-id 456 --output /secure/order-456.json --json
python tools/reconcile_funding_attempts.py --db /secure/snapshot.db --operation-key 'milestone:456' --output /secure/milestone-456.json --json
```

Only a specifically authorized operator who needs raw correlation evidence should
add `--reveal-sensitive`. Use a distinct protected output file. SetupIntent client
secrets and all other credential material remain redacted even with that flag.

## Outcome interpretation

| Outcome | Meaning | Safe next step |
|---|---|---|
| `succeeded` | Processor evidence exactly matches amount, currency, target IDs, operation key, fingerprint, and attempt number. | Permit the authenticated runtime/webhook reconciler to materialize the hold. |
| `failed` | Stripe has a definitive canceled/failed state for this attempt. | Review before allowing the next numbered attempt. |
| `pending` | Stripe has a nonterminal state. | Do not retry; inspect again later. |
| `not_found` | Search did not find exact evidence yet. Search can be eventually consistent. | Treat as ambiguous; do not retry. |
| `mismatch` | Processor evidence differs from the immutable local request. | Escalate; do not retry or mutate records. |
| `unavailable` | Stripe is not configured, search is unavailable, or the read failed. | Restore read access and rerun; do not infer failure. |

Any row with `requires_manual_review: true`, a local manual-review error code, or non-empty `conflict_evidence` overrides the processor outcome: do not retry, replay, commit, release, or refund automatically.

Signed Stripe PaymentIntent webhooks and exact authenticated client replays use the same evidence validator. A locally committed attempt is monotonic and cannot be regressed by a stale failure event.
