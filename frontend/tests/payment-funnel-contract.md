# Payment setup browser diagnostics

These events are diagnostic observations, not conversions or authoritative ledger events. No attempt identifier or ledger join is claimed. The browser does not add backend writes; the existing setup and confirmation requests are unchanged.

## Contract

- Employer prefix: `employer_payment_setup_`.
- Worker prefix: `worker_payout_setup_`.
- Employer stages: attempted, api_succeeded, form_opened (Stripe Element `ready`, once), validation_failed, submit_clicked, processor_succeeded, completed, cancelled, failed. Return/status observations are diagnostic only.
- `processor_succeeded` uses event suffix `proc_succeeded` to stay within GA4's 40-character limit; its `stage` parameter retains `processor_succeeded`.
- Processor success and backend confirmation require `confirmation.setupIntent.status === 'succeeded'` and a string payment method: `pm_` followed by one or more ASCII letters/digits/underscores, at most 255 characters total. No coercion, trimming, expanded objects, whitespace, or control characters. Missing/malformed/pending results emit only bounded failure reason `confirmation_incomplete`, keep the modal visible with a retry message, and restore Save without confirming with the backend.
- `completed` is emitted only following a successful employer backend confirmation (`ok: true`) in live mode, while its modal/session/route remains current. It is never a charge, hire, order, or executive conversion.
- Worker stages: attempted, api_succeeded, redirect_started, failed, return, status_observed. No worker completion event is emitted.
- Return query parameters trigger `/payments/status`; they never establish readiness themselves. Status remains side-specific. Known simulated/test/disabled readiness does not display real readiness.
- Exactly three explicit diagnostic fields: stage, mode, reason. Finite allowlists only. No attribution enrichment, raw errors, account/card/processor identifiers, secrets, URLs, or user input.
- Modes: live, test, simulated, disabled, unknown. `pk_test_` overrides a misleading `mode: live`. Empty static config means unknown, not simulated.
- Reasons: none, card_validation, stripe_confirm_error, confirmation_incomplete, confirm_request_error, setup_request_error, missing_setup_flow, non_live, loader_error, user_cancelled, context_changed, ready, not_ready, status_request_error.

## Evidence and safety

`payment-funnel.spec.js` exercises the actual rendered UI using deterministic API and Stripe boundaries. All external requests are fulfilled or aborted by a catch-all network kill switch. Analytics is captured in memory. No real payment details are entered. Desktop and Pixel 5 projects run the same scenarios.

Run from frontend:

```sh
PW_PORT=4196 ./node_modules/.bin/playwright test tests/payment-funnel.spec.js --workers=6 --fully-parallel --reporter=line
```

The old browser assertion and backend source-contract assertions were explicitly migrated; no legacy mixed event emitters remain.

Confirmation-gate fixture migration: the legacy `browser-regression.spec.js` backend-confirmation-failure fixture now explicitly returns `status: 'succeeded'` alongside its unchanged valid `pm_test` ID. It still exercises the original backend exception and asserts the original raw inline error, enabled Save button, and `confirm_request_error`; it is not repurposed into a malformed-processor test. The existing funnel success fixture already includes `succeeded` and retains `pm_LOCAL_ONLY`. New malformed/pending/invalid-method cases run against a permissive local `ok: true` backend and then prove a valid retry works in the same modal. Valid minimum-suffix and 255-character IDs cover acceptance boundaries.

## Boundaries

Browser diagnostics can be lost, duplicated across attempts, or blocked; never reconcile them as financial truth. Cancel does not undo an already-issued processor or backend request. It suppresses stale continuation, attachment (when confirmation has not yet been issued), and completion UI. Status API currently omits employer processor mode; unknown status is displayed from its readiness flag, with known simulated identifiers excluded. No backend mode contract, funding semantics, fees, orders, migrations, or ledger changes are included. Full candidate review and release belong to the parent agent.
