# GoHireHumans site audit and refresh (September 14, 2026)

This document records the audit of gohirehumans.com (frontend on Vercel, Flask API on Railway) and the changes made on branch `redesign/site-audit-and-ui-refresh`. It is organized by area: what was found, what changed, and what is left as a recommendation because it needs a product decision.

## Method

- Read the full backend (`backend/api_core.py`, `server.py`, `mcp_server.py`, transports, tests) and the full SPA (`frontend/index.html`).
- Parsed all 143 HTML files for shell structure, links, metadata, structured data, claims, and duplication; parsed the CSS for dead rules, duplication, and token discipline.
- Extracted every CI assertion that pins frontend markup, copy, byte sizes, and computed styles so the redesign could be delivered without silently breaking the guardrails previous work put in place.
- Verified behavior against a seeded local API and the Playwright suites (desktop and Pixel 5 projects), plus screenshots at 1280px and 390px.

## Backend

Findings, in severity order, and what changed:

- The production container ran Flask's development server (`start.sh` executed `python server.py`) even though the Procfile named gunicorn; `FLASK_DEBUG=true` would have exposed the Werkzeug debugger. `start.sh` now execs gunicorn, and `server.py` refuses debug mode in production.
- `init_db()` ran on every request (roughly 150 DDL statements plus two table-wide updates under a write lock). It now runs once per database path per process.
- The rate limiter keyed on the proxy address, so every visitor shared one bucket. Behind Railway (or with `TRUST_X_FORWARDED_FOR=1`) the client is now the rightmost `X-Forwarded-For` hop; the in-memory stores are pruned.
- The content-safety filter substring-matched short keywords, so "skills" (contains "kill"), "methodology", "hackathon", and "anti-scam" were rejected. Single-word keywords now match on word boundaries; phrases keep substring matching; a regression test covers the false positives.
- The Stripe `transfer.paid` webhook inserted a notification for user id 0, violated a foreign key, returned 500, and made Stripe retry forever. It now resolves the worker from the order and returns 200.
- API keys could reach `/admin/*` routes; they no longer can.
- Unguarded `int()` parsing in nine paginated handlers could 500 or disable `LIMIT`; they use the shared validated parser.
- Session expiry compared mismatched timestamp formats (up to a day late); expiry is written and compared consistently.
- Admin step-up password checks had no throttle; they now share the login throttle.
- `POST /referral/track` let anyone rewrite any user's referral attribution without auth; removed (registration already handles referral codes).
- `PUT /profile/worker` accepted a client-supplied `payout_method` and unvalidated hourly rates; fixed.
- Order completion incremented `services.total_reviews`; it now increments a new `total_orders` column.
- Missing webhook secrets returned 500 instead of 503; `/api-keys/verify` used naive datetimes; `/orders/{id}/log-hours` accepted NaN hours; two handlers leaked a second database connection. All fixed.
- New owner-scoped endpoints `GET /me/services` and `GET /me/jobs` return the caller's own listings in every status with pagination, so "My Services" and "My Jobs" no longer depend on the public, active-only lists (which hid paused, hired, and completed items and capped at 20 rows platform-wide).
- The MCP server sent wrong field names (`limit` instead of `per_page`, `skills_required` instead of `required_skills`, `user_name` instead of `worker_name`, and others) so agents saw "Unknown" providers and truncated results; fixed, and its pricing and platform descriptions now match the approved framing instead of describing escrow, verified professionals, and "the first marketplace for the AI economy".
- Test suite: 25 tests failed on macOS but passed in CI because a fixture compared a temp path with its resolved symlink, raised in `setUp`, and leaked `EMAIL_PROVIDER=agentmail` into later tests. Fixed; the suite is green locally and in CI (539 tests, including 38 new ones).
- Housekeeping: `.dockerignore`, README corrections (`/admin/dashboard`, real file sizes), `.env.example` paths, a JSON 413 handler, and an API Content-Security-Policy header.

Recommendations not implemented (they change financial state machines or need a decision): insert a durable `disputes` row whenever an order is auto-flipped to `disputed` so admins can resolve it; return 409/402 instead of 502 for funding conflicts on approve/complete; hash session tokens at rest and revoke sessions on password rotation; move money columns from REAL to integer cents; split `api_core.py` along the section markers listed in the audit; add a scheduled off-site backup of the SQLite volume.

## Single-page app (`frontend/index.html`)

- Confirmation dialogs and the service checkout dialog were rendered at opacity 0 (a CSS animation without a fill mode) while still blocking the page; toasts rendered below the footer; the accessibility skip link was routed as a page and bounced signed-out users to login; the admin dashboard crashed on an undefined helper. All fixed.
- The home page painted nothing until the API answered; search boxes lost focus while typing because each keystroke re-rendered the page; new tabs booted half signed in; the public nav ignored the signed-in state; app-shell routes had stale-render races; ten date sites produced "Invalid Date" outside Chrome; modals survived navigation with no dialog semantics; Stripe trust badges showed in simulated-payment mode. All fixed.
- Route titles are set per page, scroll resets on navigation, unknown routes 404 before the auth gate, the fake "Forgot password" flow is gone (there is no reset endpoint), and roughly forty form labels are associated with their inputs.
- The visual pass moves the task-draft form into the hero as the product surface, replaces emoji empty states with icon tiles, lifts inline styles into classes, renders a compact pager instead of one button per page, and switches the account listings to the owner-scoped endpoints.

## Shared CSS

- The stylesheet had six generations layered on top of each other: roughly 21KB of dead rules, 23 selectors declared twice, two modal systems, a dark-mode media query that half-darkened static pages for dark-OS users, and Tailwind-palette badges unrelated to the brand.
- It was rewritten as one system (`docs/design-system/design-system.md`): warm paper surfaces, ink text, the existing teal for actions, hairline structure, a fixed type scale with weights 400 to 700, warm shadows, no hover lifts, no gradients, and sentence-case labels. App-only rules moved to `frontend/app.css`. The pinned nav, logo, and footer rules are unchanged.
- Sizes: `style.css` 66.6KB (budget 75KB, was 88.8KB against 90KB), `app.css` 28.6KB (budget 35KB).

## Static pages (140 files)

- Only 14 pages used the canonical footer; 47 carried a visible "Created with Perplexity Computer" link and 42 empty self-links; 65 had no footer landmark; 36 had no `<main>`; none had a skip link. Every page now uses the canonical shell, a skip link, a `<main>` landmark, one font link, the analytics bootstrap with its per-page config line, and no builder attribution.
- 718KB of page-local `<style>` blocks and about 3,000 inline style attributes were replaced by the shared classes; the purple gradient "hire" template, the dark gradient blog cards, and the teal gradient hero cards are gone.
- Claims that contradicted the approved framing were corrected on every page: "0% seller fee", "1% employer fee, 100% freelancer earnings", a "Founding Freelancer program" that does not exist, "verified credentials" and "verified professionals", "the only marketplace", "lowest fees in the industry", and mismatched competitor rates (now one set of published rates across the site).
- Metadata: 51 titles over 60 characters and 58 descriptions over 160 were rewritten (pinned exact titles kept), Open Graph and Twitter cards added to the 35 and 55 pages missing them, duplicate Open Graph blocks removed, every Article gained `image` and `dateModified`, and 55 pages without structured data received BreadcrumbList and WebPage blocks.
- Plumbing: sitemap cleaned (feeds, a redirect stub, and noindex pages removed; lastmod refreshed), RSS and Atom regenerated from page metadata (new `scripts/generate_feeds.py`), duplicate and missing redirects fixed, internal working docs moved out of the deploy to `docs/ops/`, 6.4MB of orphaned assets deleted, `.well-known/ai-plugin.json` and `llms.txt` aligned to the approved framing, and `scripts/check_public_shell.py` extended from 7 pages to all 135.

Recommendations not implemented (they retire indexed URLs and need a product decision; each also needs test updates because CI pins several of these pages): consolidate the eleven "review AI output" pages to three, fold the six `categories/` pages into the matching `hire/` role pages, merge the two fee calculators, redirect `compare.html` to `vs/`, and retire or refresh the single "marketplace pulse" edition.

## Verification

- Backend: `python -m unittest discover -s backend -p 'test*.py'`.
- Static checks: `scripts/check_public_shell.py`, `scripts/sync_public_shell.py --check`, `scripts/performance_budget.py`, `backend/security_static_checks.py`, and `test_deep_audit_regressions.py`.
- Browser: the full Playwright suite on desktop and Pixel 5 projects (`PW_PORT` lets local runs avoid a busy port; CI keeps 4173).
- Screenshots of the home, marketplace, detail, auth, dashboard, and representative static pages at 1280px and 390px.
