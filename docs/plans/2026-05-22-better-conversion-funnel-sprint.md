# GoHireHumans Better Conversion Funnel Sprint Plan

> **For Hermes:** Use subagent-driven-development skill to implement this plan task-by-task.

**Goal:** Reduce friction from visitor interest → useful task/service posting → matched to the right human or agent.

**Architecture:** Keep changes safe, reversible, and conversion-focused. Start with frontend funnel clarity and lightweight matching intake; do not contact users, send emails, change payment mechanics, or promise guarantees without Enzo approval.

**Tech Stack:** Static frontend in `frontend/index.html`, Flask backend in `backend/`, Python unittest regression tests, GitHub PR workflow.

---

## Business context from Enzo

- PSI is the closest near-term cash lane.
- GoHireHumans is the biggest upside lane if it gets traction.
- Conversion sprint priority: make the site actually usable when people get excited enough to post a service, look for work, or connect with the right humans/agents.

## Conversion problem to solve

Current product risk: people may understand the idea but still not know exactly what to do next or whether the marketplace has enough supply/demand.

Conversion target:
1. Employer lands with a task idea.
2. Site helps turn that idea into a clear task listing.
3. Worker/agent lands looking for work.
4. Site routes them to relevant task categories and/or captures their skills/interests.
5. Operator gets usable matching signals without automatic outreach or risky promises.

## Guardrails

- Do not contact users automatically.
- Do not change charge, refund, payment release, or dispute mechanics.
- Do not claim guaranteed matching, escrow protection, arbitration, verified workers, or guaranteed completion unless production behavior and legal posture support it.
- Preserve pricing framing: workers receive the listed payout; employers pay Stripe processing plus 1% GoHireHumans fee.

---

## Task 1: Audit current funnel paths

**Objective:** Identify where employer and worker journeys currently lose intent.

**Files:**
- Read: `frontend/index.html`
- Read: `backend/api_core.py`
- Read: `backend/server.py`
- Read tests under `backend/`

**Steps:**
1. Map all current CTAs and routes for post task, browse services, browse jobs, post service, and concierge draft templates.
2. Confirm which actions are static links vs backend API submissions.
3. List trust-breaking copy or JSON-LD claims that imply unsupported behavior.
4. Save findings in this plan under a follow-up implementation note or in a new PR description.

**Verification:**
- Findings cite exact file paths/line numbers.

---

## Task 2: Add a guided employer posting path

**Objective:** Give employers a low-friction way to describe a task and land in a clearer prefilled posting flow.

**Files:**
- Modify: `frontend/index.html`
- Test: `backend/test_deep_audit_regressions.py` or new focused regression test file

**Acceptance criteria:**
- Homepage includes a short guided task intake block with fields/prompts for:
  - What needs to be done?
  - What type of human/agent is needed?
  - Suggested deliverable/result
  - Suggested budget range
- CTA routes into the existing post-job flow with prefilled draft data where possible.
- If full persistence is not implemented, copy explicitly says it creates a draft, not a submitted job.
- Analytics event is emitted for guided task intake start/submit.

**Verification:**
- Regression test confirms the guided block exists.
- Regression test confirms no automatic external outreach/API submission from the static block.
- Static JS check passes.

---

## Task 3: Add worker/agent routing from “find work” intent

**Objective:** Make job seekers and agents immediately understand how to find relevant work.

**Files:**
- Modify: `frontend/index.html`
- Test: frontend/static extraction checks or backend regression text tests

**Acceptance criteria:**
- Add a worker/agent path that asks what they can do and routes to relevant job categories/searches.
- Include examples: website testing, lead research, AI-output review, calls, local verification, data cleanup.
- Avoid unsupported claims about available jobs or guaranteed work.
- Track analytics event for worker route clicks/searches.

**Verification:**
- Regression test confirms worker/agent routing copy and links exist.
- Production-safe claim test confirms no “guaranteed work” or “verified jobs” claims.

---

## Task 4: Improve marketplace liquidity messaging

**Objective:** Be honest about early-stage supply/demand while still converting interest.

**Files:**
- Modify: `frontend/index.html`
- Test: regression copy tests

**Acceptance criteria:**
- Add concise copy explaining: “If the right match is not visible yet, post the task/service anyway so the marketplace can route demand.”
- Do not promise manual matching unless the workflow exists.
- If using “concierge-style” language, frame it as help drafting/listing, not guaranteed fulfillment.

**Verification:**
- Tests assert safe language and absence of unsupported guarantee/escrow/arbitration claims.

---

## Task 5: Strengthen measurement

**Objective:** Measure whether visitors are moving through the funnel.

**Files:**
- Modify: `frontend/index.html`
- Test: static JS extraction/check

**Events to track:**
- `guided_task_intake_start`
- `guided_task_draft_created`
- `worker_route_select`
- `post_service_intent`
- `browse_relevant_jobs_intent`
- Existing CTA events should remain intact.

**Verification:**
- Extracted script passes `node --check`.
- Tests confirm event names exist.

---

## Task 6: PR, CI, merge, production verification

**Objective:** Ship only after local, CI, and production verification.

**Commands:**
```bash
git diff --check
python -m unittest discover backend
python -m py_compile backend/*.py
# Extract frontend script and run node --check if existing workflow uses that pattern.
```

**GitHub flow:**
1. Branch from current `main`.
2. Commit with conventional commit message.
3. Open PR.
4. Wait for checks.
5. Fix up to three loops.
6. Squash-merge only if clean.
7. Verify production frontend content and backend `/health`.

**Production verification:**
- `https://www.gohirehumans.com/` returns 200.
- Expected new conversion copy appears.
- Risky unsupported claims do not appear.
- `https://gohirehumans-production.up.railway.app/health` returns 200.

---

## Enzo decision needed before external growth work

Conversion work can proceed autonomously. External visibility work needs approval before contacting people or posting from owned accounts.

Approval-gated options after funnel improvement:
- Draft but do not send employer outreach.
- Draft but do not publish LinkedIn/X posts.
- Build a target list of communities/SEO pages.
- Seed example tasks/services if Enzo confirms what is real vs demo.
