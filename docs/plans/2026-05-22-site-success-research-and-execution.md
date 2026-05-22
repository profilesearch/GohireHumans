# GoHireHumans Site Success Research and Execution Plan

> **For Hermes:** Use this as the operating roadmap for making GoHireHumans more successful without requiring operator intervention for routine, low-risk execution.

**Goal:** Improve GoHireHumans conversion, trust, SEO, and marketplace liquidity by clarifying the product promise, fixing trust-breaking defects, adding task guidance, and creating a prioritized growth roadmap.

**Architecture:** Start with safe, reversible website and API-contract improvements. Avoid risky payment mechanics, legal claims, outbound messaging, or live user contact without Enzo approval. Treat the site as a listing/payment connector: workers receive the listed payout; employers pay Stripe processing plus a 1% GoHireHumans fee.

**Tech Stack:** Static frontend in `frontend/`, Python backend in `backend/api_core.py`, unittest regression tests in `backend/test_deep_audit_regressions.py`, Vercel frontend, Railway backend.

---

## Research synthesis

### Best positioning

GoHireHumans should be positioned as:

> Get small tasks done by real humans.

Support copy:

- Post a task, set the payout, and connect with people who can do it.
- Useful for website testing, lead research, data cleanup, phone calls, local verification, AI-output review, and other work requiring human judgment.
- Workers receive the listed payout. Employers pay Stripe processing plus 1%.

Avoid unapproved/legal-risk language:

- Guaranteed completion
- Escrow-protected / risk-free
- Platform arbitration
- Verified safe
- We guarantee quality

### Initial vertical wedges

1. Website/app testing and user feedback
2. Lead research and online research
3. AI-output review / fact-checking
4. Phone calls / admin help
5. Local verification/photo tasks
6. Data cleanup / spreadsheet work

### Core growth mechanics

- Homepage explains concrete task examples above the fold.
- Task templates reduce the blank-page problem.
- SEO pages target specific intent like `hire website testers`, `hire AI reviewers`, `hire people to make phone calls`.
- Worker email capture and task notifications improve supply liquidity.
- Employer concierge form captures demand before the marketplace is liquid.
- Analytics should track the employer and worker funnels.

---

## Executed in this branch

### Task 1: Add regression coverage for payment status contract

**Objective:** Prevent the frontend from treating ready employers/workers as not ready because `/payments/status` lacks top-level booleans.

**Files:**
- Modify: `backend/test_deep_audit_regressions.py`
- Modify: `backend/api_core.py`

**Acceptance criteria:**
- `/payments/status` still returns nested `worker_payout_status` and `employer_payment_status`.
- `/payments/status` also returns additive booleans:
  - `worker_ready`
  - `employer_ready`

### Task 2: Fix known broken links/assets and payment-copy typos

**Objective:** Remove conversion/trust-damaging 404s and typo-like copy.

**Files:**
- Modify: `frontend/index.html`
- Modify: `frontend/blog/index.html`
- Modify: `frontend/blog/best-freelance-platforms-escrow.html`
- Modify: `frontend/api-docs.html`
- Modify: `frontend/ai-integration.html`
- Modify additional affected static HTML pages
- Modify: `backend/test_deep_audit_regressions.py`

**Acceptance criteria:**
- No references to `hiw-step2-payment hold.png`.
- No references to `best-freelance-platforms-payment hold.html`.
- API docs do not advertise invalid payment endpoints with spaces.
- Static regression tests reject known broken strings.

### Task 3: Improve homepage conversion copy

**Objective:** Make the homepage immediately explain the buyer promise and the best first tasks.

**Files:**
- Modify: `frontend/index.html`

**Acceptance criteria:**
- Hero headline focuses on small tasks done by real humans.
- Primary CTA points to posting a task.
- Secondary CTAs support browsing humans and finding paid tasks.
- Above-fold trust strip explains worker payout and employer fee plainly.
- Homepage includes example first-task cards and suggested payout ranges.

---

## Next execution sequence

### P0 — Complete and deploy this branch

1. Run full backend test suite.
2. Run static checks if present.
3. Commit branch.
4. Open PR.
5. Monitor CI.
6. Fix failures up to three loops.
7. Merge if green and safe.
8. Verify production frontend copy, broken URLs, and backend `/health` after deployment.

### P1 — Build SEO/task-template pages

Create first pages:

- `/hire/website-testers.html`
- `/hire/lead-researchers.html`
- `/hire/ai-reviewers.html`
- `/hire/phone-call-help.html`
- `/hire/local-verification.html`
- `/earn/get-paid-for-human-tasks.html`

Each page should include:

- SEO title and description
- H1 with specific task intent
- Example tasks
- Suggested payout ranges
- Safety note / connector framing
- CTA to post a task or browse services

### P1 — Add analytics events

Track:

- Hero search submit
- Post task CTA
- Browse humans CTA
- Find paid tasks CTA
- Service order intent
- Job apply intent
- Explainer video play

### P1 — Add concierge demand capture

Add a low-risk form or CTA:

> Not sure what to post? Tell us the task and we’ll help turn it into a clear listing.

Do not send emails or contact users automatically without Enzo approval.

### P2 — Worker notification/digest loop

Add worker email capture:

> Get notified when new paid tasks are posted.

Needs backend/email infrastructure review before production behavior changes.

---

## Risks and constraints

- Payment mechanics and dispute behavior are reputation/legal sensitive; copy changes are okay, but charge/refund/release mechanics need explicit approval.
- Public claims must not imply escrow, arbitration, guarantees, or worker vetting unless the product truly supports them.
- Marketplace cold start is the biggest business risk; manual liquidity seeding is likely required.
- SEO pages need quality and specificity, not keyword spam.
