# Broadsheet redesign — status and handoff plan (2026-09-15)

Enzo chose direction A, "Broadsheet", from four mockups (canvas: https://claude.ai/artifact/4fNMQ7JXbsAceM2v4CvTCY). This file records what was implemented in this pass, how to verify it, and exactly what remains. Anyone continuing this work should read `docs/design-system/design-system.md` first — it is the contract every decision below follows — and keep the option name "Broadsheet" for the direction.

## Decisions already made (do not re-open)

1. Fonts: Newsreader (serif, headlines and leads, optical sizing on) + Instrument Sans (UI and body). JetBrains Mono stays for code only.
2. Palette: paper `#f4efe6`, darker paper `#ede6d8`, ink `#1a1816`, stamp red `#b8321f`. Ink is the action colour (buttons, links); red is a mark (labels, numerals, wordmark period, active nav rule, hover, focus ring). Teal is gone everywhere except the raster icons that still need regenerating (see task 2).
3. Structure: rules and columns instead of cards, pills and tints. Every radius token is 2px. Nothing lifts on hover; titles turn red, borders turn ink.
4. Shell: a 64px sticky nav (a masthead strip shipped in the first pass and was removed the same day at Enzo's request: it read as clutter); serif wordmark `GoHireHumans.` with an `aria-hidden` red period; footer under a 4px double rule.
5. Home hero keeps the task-draft form (a conversion surface with tests and analytics) on the right, restyled as a paper form in an ink frame. The classified "board" from the mockup became the marketplace-preview section (three ruled columns fed by the live `/services` API). Adding an open-tasks board to the hero is optional (task 5).
6. Home hero copy changed to the mockup's: label "Help wanted", headline "Some work still needs a person.", lead rewritten. Tests were updated to match. Reverting to "Describe the work. Hire the right human." is a three-string change (index.html, `backend/test_deep_audit_regressions.py`, `frontend/tests/browser-regression.spec.js`).
7. Every class name is unchanged; the redesign lives in `style.css` tokens and component rules, so all 135 static pages re-skin without page edits beyond the font link, theme-color and the synced shell.

## What changed in this pass

- `frontend/style.css` — rewritten (tokens, base, components, shell, layouts, landing). 72 KB of the 75 KB budget.
- `frontend/app.css` — serif page titles and prices, red category labels, ochre stars, home hero title size, ink rule under the home hero.
- `frontend/partials/public-nav.html`, `frontend/partials/public-footer.html` — wordmark; synced into 134 pages with `python3 scripts/sync_public_shell.py`.
- `frontend/index.html` — fonts, theme-color, ink `LOGO` mark, JS nav/footer mirror the partials, hero copy, band classes (`How it works` is the darker band), `steps--display`.
- All static pages — Google Fonts link swapped (Inter → Instrument Sans + Newsreader), `theme-color` → `#f4efe6`.
- `frontend/favicon.svg`, `frontend/site.webmanifest` — ink and paper.
- Tests — `backend/test_deep_audit_regressions.py` (nav active-state pins, logo CSS pins, headline pin, nav-label helper ignores aria-hidden spans) and `frontend/tests/browser-regression.spec.js` (headline, active nav colour/background, wordmark metrics).
- Docs — `docs/design-system/design-system.md` (rewritten), `docs/design-system/public-shell.md`, `README.md`.

## How to verify (all must pass before merging)

```bash
python3 scripts/check_public_shell.py
python3 scripts/sync_public_shell.py --check
python3 scripts/performance_budget.py
python3 backend/security_static_checks.py
python3 -m unittest backend.test_deep_audit_regressions
cd frontend && npm ci && npx playwright install chromium && PW_PORT=4181 npm run test:browser
```

Always set `PW_PORT` to a port nothing else is using. The Playwright config has `reuseExistingServer: true`, so if anything already answers on 4173 (the Claude desktop app's preview server did during the redesign session) the suite silently runs against that server, every real page 404s, and hundreds of tests fail with blank-page timeouts. `lsof -nP -iTCP:4173 -sTCP:LISTEN` shows whether the port is taken.

Visual check: `cd frontend && python3 -m http.server 4180` then open `/`, `/about.html`, `/pricing.html`, `/hire/`, `/starter-offers.html`, `/blog/human-as-a-service.html`, `/#/ai-employers`, `/#/login`, `/#/services` at 1440px and 390px. At 390px `document.documentElement.scrollWidth` must equal `window.innerWidth` on every page.

## Remaining work, in priority order

Each task is self-contained. Follow the instructions literally; do not improvise new colours, fonts, radii or components — everything needed is in the design-system contract.

### 1. Keep the browser suites green (done in this pass; re-run after every change)

Verified 2026-09-15 with `PW_PORT=4181 npm run test:browser`: 666 passed, 6 skipped, 0 failed across all eleven specs on both projects, and `python3 -m unittest backend.test_deep_audit_regressions`: 119 passed. `browser-regression.spec.js` was updated for the redesign. If a later change trips a test that pins the old design, update the test, never work around it in CSS:
- Old teal `rgb(13, 115, 119)` / `rgb(230, 243, 243)` → active nav is ink `rgb(26, 24, 22)` on `rgba(0, 0, 0, 0)`.
- Old headline `Describe the work. Hire the right human.` → `Some work still needs a person.`
- Logo: no `<svg>` in `.lp-nav-logo`; font-size `26px`, weight `500`, line-height `28px`, gap `8px`, width between 120 and 240px.
- If a test measures a button height, the sizes are now 36 / 44 / 52px (`.btn-sm` / `.btn` / `.btn-lg`); phones keep the 44px minimum from `mobile-hardening.css`.
- Toast/modal tests: the modal now has a 1px ink border and 2px radius; behaviour is unchanged.

### 2. Regenerate the raster brand assets (done in this pass)

Done on 2026-09-15 with `node scripts/render_brand_assets.js`, which renders `favicon.svg` at every referenced size, packs `favicon.ico`, and paints `og-image-v2.png` from the same tokens and fonts; re-run it whenever the mark or the headline changes. The original instructions, kept for reference — previously teal: `frontend/icon-192.png`, `frontend/icon-512.png`, `frontend/apple-touch-icon.png`, `frontend/favicon-32.png`, `frontend/favicon.ico`, `frontend/og-image-v2.png`. Rebuild them from the new `frontend/favicon.svg` (ink square, 3px-ish radius, paper silhouette):
- Icons: render `favicon.svg` at 512, 192, 180 (apple-touch), and 32 px. With ImageMagick + librsvg: `magick -background none -density 512 frontend/favicon.svg -resize 512x512 frontend/icon-512.png` (repeat per size); `magick frontend/favicon-32.png frontend/favicon.ico`. Without those tools, a Playwright script that screenshots the SVG at each size also works (`page.setContent`, `page.screenshot({ omitBackground: true })`).
- OG image (1200×630): paper `#f4efe6` background; the wordmark `GoHireHumans.` top-left in Newsreader 500 at 40px with a red period; headline `Some work still needs a person.` in Newsreader 400 at 96px, ink, max width 900px, left-aligned; below it in Instrument Sans 600, 22px, uppercase, 0.14em tracking, red: `A MARKETPLACE FOR SMALL, SCOPED HUMAN HELP`; a 1px ink rule 40px from the bottom with `Free to join · Workers receive the listed payout · Employers pay Stripe processing + 1%` under it in Instrument Sans 20px, muted `#6b645b`. Build it as an HTML page that loads the Google Fonts link from the design system and screenshot it at 1200×630 with Playwright, then save as `frontend/og-image-v2.png` (keep the filename — every page references it).
- Update `<meta name="theme-color">` nowhere else; `site.webmanifest` is already ink/paper.

### 3. Give the signed-in app a full pass (recommended)

Start the backend (`cd backend && python server.py`), point `frontend/config.js` at it, sign in, and walk the dashboard, browse (`#/services`, `#/jobs`), a service detail, post-job form, orders, notifications, profile, and admin health. Everything inherits the tokens; look for:
- Teal or blue literals left in `index.html` inline styles (`grep -n "#0d7377\|#e6f3f3\|0d9488" frontend/index.html`) — replace with `var(--color-accent)` for marks or `var(--color-primary)` for actions.
- Optional consistency step: convert app cards to outlined boxes by changing `background: var(--color-surface-2)` to `background: transparent` and `border: 1px solid var(--color-divider)` to `border: 1px solid var(--color-border)` on `.svc-card`, `.job-card`, `.task-card`, `.quick-action-card`, `.filter-group`, `.seller-cta-card`, `.stat-card` in `app.css`. Keep the fill on `.skeleton-card`, `.svc-order-card`, `.auth2-card`, modals and the guided-intake form (surfaces that hold controls stay filled).
- Sidebar: `.sidebar-item.active` may deserve a 3px red left rule (`box-shadow: inset 3px 0 0 var(--color-accent)`) to match the mobile menu.

### 4. Spot-check the eight pages with page-local `<style>` blocks (done for stats, press and the fee calculator; the rest are token-only)

`stats.html`, `press.html`, `agent-onboarding.html`, `ai-integration.html`, `api-docs.html`, `tools/fee-calculator.html`, `tools/are-you-overpaying.html`, `tools/freelance-fee-calculator.html`. They reference tokens only, so they re-skin, but check radii (`var(--radius-full)` on bars is fine; anything hardcoded above 2px should become a token) and that `press.html`'s colour swatches describe the new palette (paper, darker paper, ink, stamp red) rather than teal.

### 5. Optional: an open-tasks board on the home page

The approved mockup showed a "Today's board" with open tasks (help wanted) and services offered. To add it without disturbing the task-draft form, add a section between "Start here" and "How it works" that renders up to three open jobs from `GET /jobs?per_page=3` in the same ruled-column pattern as `.lp-feed-grid` (reuse the classes; label `Help wanted`, serif title, meta line `type · budget · posted by`, price right-aligned in `.lp-feed-price`). Fall back to the existing `.lp-preview-empty` when the API is unavailable. Keep the static regression pins (`Start with QA`, the agent sentence, the fee sentence) intact.

### 6. Mobile nav polish (verified at 320px: wordmark 159px, no overflow; nothing to do unless copy changes)

On phones the nav shows the wordmark (24px), "Post a task", and the hamburger. Verified at 320px: nothing collides and there is no horizontal overflow. If copy or sizes change, re-check that viewport.

### 7. Commit

Committed on the `redesign/broadsheet` branch and opened as a pull request on 2026-09-15 at Enzo's request; merging to `main` triggers the Vercel production deploy.

## Notes for reviewers

- The style.css budget has 2.6 KB of headroom. If a task needs more CSS, remove the retired aliases block in the tokens section first (`--color-surface-alt`, `--color-surface-offset`, `--color-surface-dynamic`) after confirming with `grep -rn "color-surface-alt\|color-surface-offset\|color-surface-dynamic" frontend --include=*.html --include=*.css --include=*.js` that nothing references them.
- The sync script only propagates the partials; `index.html` renders its nav and footer from JavaScript templates and must be edited by hand to match, byte for byte in spirit (same wordmark markup, same links).
