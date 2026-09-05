# AgentMail application-notification pilot

## Release boundary
This integration is **disabled by default** and does not authorize live email. Preserve DNS/MX/SPF, existing inboxes, customer records and other production variables. AgentMail's managed inbox needs no GHH DNS changes. Existing Resend compatibility stays available, but there is no automatic provider failover.

First increment supports only `new_application` notifications to the owning employer's **current active account email**, and only when that exact address is allowlisted. Job-match blasts, welcome emails, applications reminders, other order/payment messages, replies, marketing, and attachments are not enabled by this adapter. They require separately reviewed scope. No private title, application text, name, revision note or dispute detail is mirrored. The message links to GoHireHumans; no off-platform payment instructions.

## Architecture and guarantees
- `push_notification` creates notification, outbox row and (only with all gates active) an approved AgentMail intent in the **same transaction**. Rollback leaves no approved send. An old outbox row cannot enroll later: no backfill or on-drain enrollment exists.
- Explicit outbox and source-notification ID high-water marks and UTC activation time exclude historical events. Freshness is limited to 15 minutes. Changing a previously enrolled row's recipient, source binding or activation fingerprint fails closed.
- The lease-bound maintenance worker selects the configured transport; AgentMail does not require a Resend key. In-app reminders continue while outbound delivery is disabled.
- The unique ledger intent commits to `prepared` before any provider POST, with no SQLite writer held across HTTP. This increment issues at most one POST per intent; it never automatically reposts after timeout, crash, ambiguous response or provider switch. Availability is deliberately sacrificed when acceptance cannot be proven. Do not call this exactly-once recipient delivery.
- Also send a stable `Idempotency-Key` as defense in depth. AgentMail documents organization-scoped 24-hour send deduplication, but this implementation does not depend on reopening that retry window.
- HTTP 200 with valid `message_id` and `thread_id` proves acceptance only. Exact handles are restricted operational data; aggregate admin health does not expose them. There is no AgentMail delivery webhook configured in this increment. `sent` in the shared outbox means API accepted; it is not recipient delivery.
- Daily and lifetime attempted-send caps default to 1. Ambiguous/prepared attempts consume budget. Do not reset/delete the ledger or raise a cap to bypass an unresolved outcome.

## Required activation packet
Before live activation, specify exact sender, recipient allowlist, event types, body, first event/canary, timing, high-water marks, total/daily budget, provider-variable changes, and verification plan. Capture recipient approval anew; another sender's historical approval does not transfer.

Required configuration (never put real keys in source, shell arguments or reports):
- `EMAIL_PROVIDER=agentmail`
- `AGENTMAIL_SEND_ENABLED=true` **last**, after all other settings and approval.
- `AGENTMAIL_API_KEY`: provision privately from approved vault into backend provider variables.
- `AGENTMAIL_INBOX_ID=gohirehumans.operations@agentmail.to`
- `AGENTMAIL_RECIPIENT_ALLOWLIST`: exact approved addresses, comma-separated.
- `AGENTMAIL_NOTIFICATION_TYPES=new_application`
- `AGENTMAIL_OUTBOX_HIGHWATER` and `AGENTMAIL_NOTIFICATION_HIGHWATER`: current authenticated admin health maxima captured at approved cutover.
- `AGENTMAIL_ACTIVATED_AT`: explicit current approved UTC instant (`YYYY-MM-DDTHH:MM:SSZ`).
- `AGENTMAIL_DAILY_SEND_CAP=1`, `AGENTMAIL_TOTAL_SEND_CAP=1` for the initial canary.

Do not create fake public jobs/applications merely to trigger a canary. A separately approved local adapter canary can exercise the released module against an isolated local fixture database; that does not verify Railway credential installation or prove production delivery. Alternatively wait for an approved genuine new application addressed to the allowlisted employer.

## Verification
1. Verify source/CI/deployment and authenticated admin readiness while disabled.
2. Read back exact production-variable names/settings without exposing the key. Confirm old pending rows are unenrolled and excluded.
3. After exact send approval, observe the one authorized fresh event and the committed intent/provider acceptance. Disable sending afterward; lifetime cap is additional containment, not a reason to leave it enabled unnecessarily.
4. Use the exact message handle for AgentMail GET readback. Recipient delivery requires authenticated delivery evidence or recipient confirmation; `sent` label is insufficient.
5. Inspect fresh aggregate health, manual-review counters and provider identity. Never auto-resend ambiguous mail, reset budget, delete ledger evidence or switch provider to bypass it.

## Rollback
Set `AGENTMAIL_SEND_ENABLED=false` and confirm no new outgoing calls before rolling back code. Retain the new ledger table and all rows; do not restore an older database backup that loses attempted-send evidence. Do not enable Resend during rollback while AgentMail attempts may be unresolved. Sending code rollback is not a refund or recall of already accepted email.

## Official contract sources
- https://docs.agentmail.to/api-reference/inboxes/messages/send
- https://docs.agentmail.to/idempotency
- https://docs.agentmail.to/errors
- https://docs.agentmail.to/events
