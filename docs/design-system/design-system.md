# GoHireHumans design system (September 2026 refresh)

This document is the contract for `frontend/style.css` (shared, loaded by every page; it carries the reset and body base in its section 2), `frontend/app.css` (signed-in app surfaces, loaded by `index.html` only), `frontend/base.css` (the same reset as style.css section 2, linked by `index.html` only — static pages must not add it), and `frontend/mobile-hardening.css` (phone-width overflow and touch-target guards, a no-op above 768px). Static pages must build on these classes instead of page-local `<style>` blocks. The public shell (nav and footer partials) and its CI contract are described in `public-shell.md`.

## 1. Direction

Paper and ink. GoHireHumans is a marketplace for small, scoped, reviewable human work, so the interface should feel calm, precise, and honest: warm paper surfaces, ink text, one teal accent reserved for actions and links, hairline rules for structure, and real product surfaces (listing cards, the task-draft form, a fee table) instead of decorative imagery.

What we removed on purpose: gradient blobs and gradient bands, rotated backgrounds, dark hero boxes, pill eyebrows above every heading, uppercase tracked labels on marketing sections, emoji as icons, hover lifts, three-column icon-tile grids as the default layout, "trusted by" style claim strips, macOS window dots on code blocks, and the purple/violet badge palette.

What we kept: the deep teal `#0d7377`, the warm surface stack, Inter, near-black text, hairline borders, the tokenized architecture, the base.css typographic care, the shared nav/footer partials, and the SVG icon set.

## 2. Tokens

Defined once on `:root` in `style.css`. No dark-mode token block ships (the SPA forces light mode and no static page consumes dark tokens), so the `prefers-color-scheme` block was removed. If dark mode becomes a product decision, add a single `[data-theme="dark"]` block and make every surface consume tokens first.

### Base (style.css section 2, mirrored by base.css)

Border-box sizing and zero margins on everything; `body` in Inter at `--text-base` / `--leading-normal`, ink on paper, antialiased; `main { min-width: 0 }`; block-level media at `max-width: 100%`; form controls inherit font; unstyled buttons; collapsed full-width tables; balanced headings; `p, li, figcaption` pretty-wrapped at `max-width: 72ch` (row-type list items — `.card-list`, `.steps`, `.pub-grid`, `.lp-footer-links`, `.breadcrumb`, `.cluster` — opt out); tinted `::selection`; the `--ring` focus indicator on `:focus-visible`; reduced-motion kill switch; `.sr-only` / `.visually-hidden`; `[hidden] { display: none !important }`. Unclassed flow content gets `h1–h4 + p` 0.5em, `p + p` 1em, `p + h2` 1.75em, and padded unclassed lists.

### Color

| Token | Value | Use |
|---|---|---|
| `--color-bg` | `#f5f4f0` | page background (paper) |
| `--color-surface` | `#faf9f6` | nav, footer, sidebars, quiet bands |
| `--color-surface-2` | `#ffffff` | cards, inputs, modals |
| `--color-surface-sunken` | `#efeee9` | tinted wells, active sidebar item, code, zebra |
| `--color-divider` | `#dddbd6` | hairlines |
| `--color-border` | `#d0cec8` | input borders, stronger card borders |
| `--color-border-strong` | `#b9b6af` | hover borders, table header rule |
| `--color-text` | `#1a1816` | ink |
| `--color-text-body` | `#2b2926` | long-form body copy |
| `--color-text-muted` | `#6b6963` | secondary text (5.2:1 on paper) |
| `--color-text-faint` | `#6f6c65` | meta text, placeholders, timestamps (4.8:1 on paper, 4.5:1 on sunken — one step below muted; hierarchy comes from size, not from a lighter gray) |
| `--color-text-inverse` | `#faf9f6` | text on ink or teal |
| `--color-primary` | `#0d7377` | links, primary buttons, focus |
| `--color-primary-hover` | `#0a5f62` | |
| `--color-primary-active` | `#084d4f` | |
| `--color-primary-subtle` | `#e6f3f3` | active nav pill, selected states, tinted icon tiles |
| `--color-primary-text` | `#ffffff` | text on primary |
| `--color-accent` | `#b7791f` | ochre: star ratings, featured/starter markers only |
| `--color-accent-subtle` | `#f6ecd7` | |
| `--color-success` / `-subtle` | `#2d7a3a` / `#e8f5ea` | |
| `--color-warning` / `-subtle` | `#8f5f00` / `#fbf3dc` | 5.0:1 on its own tint, so badge text passes |
| `--color-error` / `-subtle` | `#b3261e` / `#fbe9e7` | (`--color-danger` is an alias of `--color-error`) |
| `--color-info` / `-subtle` | `#2f5f9e` / `#e7eef8` | in-progress states |

Status colors map onto the above: `open`, `active` → primary; `reviewing`, `pending`, `submitted`, `reserved` → warning; `in_progress`, `hired` → info; `completed` → success; `disputed` → error; `draft`, `canceled`, `paused`, `removed` → muted gray (`#6b6963` on `#efeee9`).

Teal tints are derived with `color-mix(in oklab, var(--color-primary) N%, transparent)` at 12%, 18%, and 35%. Never spell out rgba teal values.

### Typography

Inter 400, 500, 600, 700 from one Google Fonts link on every page: `https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&family=JetBrains+Mono:wght@400;500&display=swap`. Weights 800, 450, 550, 650, and 750 are retired.

| Token | Size | Use |
|---|---|---|
| `--text-xs` | 0.75rem (12px) | meta, badges, table captions |
| `--text-sm` | 0.8125rem (13px) | UI labels, secondary text, table cells |
| `--text-base` | 0.9375rem (15px) | UI body |
| `--text-md` | 1.0625rem (17px) | prose body, card titles |
| `--text-lg` | 1.25rem (20px) | lead paragraphs, h3 in prose |
| `--text-xl` | 1.5rem (24px) | h3 marketing, h2 prose |
| `--text-2xl` | `clamp(1.625rem, 1.35rem + 1vw, 2rem)` — 26px on phones, 32px from 1040px | section h2, app page titles |
| `--text-3xl` | `clamp(2.125rem, 1.6rem + 1.6vw, 2.625rem)` — 34px on phones, 42px from 1025px | static page h1 |
| `--text-display` | `clamp(2.5rem, 2rem + 2.5vw, 3.75rem)` | home hero h1 only |

Line heights: `--leading-tight 1.1` (display), `--leading-snug 1.25` (headings), `--leading-normal 1.55` (UI), `--leading-relaxed 1.7` (prose). Tracking: `-0.02em` on display, `-0.015em` on 2xl and 3xl, none elsewhere. Headings use weight 600; display and static page h1 also 600. Weight 700 is reserved for the logo (pinned by tests), prices, and stat values. Uppercase with tracking is allowed only on `.badge`, `th`, and `.sidebar-label`.

### Spacing and layout

`--space-1` 4px through `--space-24` 96px on a 4px scale (unchanged). `--section-y: clamp(3.5rem, 8vw, 6rem)` for public-page sections. `--nav-h: 60px`, and every sticky offset uses it. Content widths: `--content-prose 720px`, `--content-narrow 640px`, `--content-default 960px`, `--content-wide 1120px`. Public sections use `--content-wide` with `padding-inline: var(--space-6)`.

### Radius, shadow, motion

`--radius-sm 4px` (badges, checkboxes, code), `--radius-md 8px` (buttons, inputs, small cards), `--radius-lg 12px` (cards, modals, panels), `--radius-xl 16px` (hero product surface), `--radius-full`. Nested radius = parent radius minus padding.

Shadows are warm-tinted so they read as paper: `--shadow-xs 0 1px 2px rgba(26,24,22,.05)`, `--shadow-sm 0 1px 2px rgba(26,24,22,.06), 0 2px 6px rgba(26,24,22,.05)`, `--shadow-md 0 6px 18px rgba(26,24,22,.08)`, `--shadow-lg 0 18px 48px rgba(26,24,22,.14)`. `--ring: 0 0 0 3px color-mix(in oklab, var(--color-primary) 30%, transparent)`.

`--transition-fast 120ms` and `--transition-base 180ms` with `cubic-bezier(.16,1,.3,1)`. Transitions name properties (`color, background-color, border-color, box-shadow, opacity, transform`), never `all`. No hover lifts: interactive cards change border color to `--color-border-strong` and gain `--shadow-sm`.

## 3. Shared components (style.css)

Class names that tests pin are kept verbatim; new classes are additive.

- Buttons: `.btn` + `.btn-primary`, `.btn-secondary` (white, border), `.btn-ghost` (no border, muted text), `.btn-outline` (teal text and border, kept for compatibility, visually equals secondary with teal text), `.btn-danger`; sizes `.btn-sm` (32px), default (38px), `.btn-lg` (46px). Radius md, weight 500, inline-flex, gap 8px, focus ring via `--ring`. Disabled: 50% opacity, no pointer.
- Links: teal, no underline at rest, underline on hover with `text-underline-offset: 3px`; in `.prose`, links are underlined at rest. Links inside running text — `p`, `li`, `dd`, `td`, `th`, `small`, `figcaption`, `blockquote`, `.callout`, `.form-help`, auth copy — are underlined at rest with a 35% teal underline (`--color-primary-tint-35`) that turns solid on hover: teal on muted copy is about 1:1, so colour alone cannot mark them (axe `link-in-text-block`). The rule is zero-specificity (`:where()`), so buttons, nav and footer links, breadcrumbs, badges and card rows keep their own treatment.
- Cards: `.card` (white, 1px divider, radius lg, padding `--space-5`); `.card--interactive` for hover border and shadow; `.card--sunken`; `.card-title` (md, 600), `.card-text` (sm, muted). Existing `.svc-card`, `.job-card`, `.task-card`, `.stat-card`, `.lp-feed-card`, `.lp-start-card`, `.lp-home-route-card`, `.starter-offer-card`, `.quick-action-card` all render as this card.
- Badges: `.badge` (xs, 500, sentence case, padding 2px 8px, radius sm, subtle background + colored text); `.badge-open`, `.badge-submitted`, `.badge-reserved`, `.badge-in_progress`, `.badge-completed`, `.badge-disputed`, `.badge-canceled`, `.badge-draft`, `.badge-human`, `.badge-ai` map to the status colors above. `.skill-tag` and `.pill` are gray pills (radius full).
- Labels: `.eyebrow`, `.lp-section-label`, `.lp-hero-eyebrow`, `.blog-hero-label`, `.vs-eyebrow` all render as the same small teal label: 13px, 500, sentence case, no background, no border, with an 18px teal rule before it (`::before`), left-aligned. `.lp-centered-head` still centers where used.
- Section head: `.section-head` (label + h2 + optional p), left-aligned by default; `h2` is 2xl/600 with tracking; `p` is base, muted, max 60ch.
- Steps: `.step-num` (28px square, radius sm, primary-subtle background, teal 600 number). `.lp-icon-badge`, `.lp-how-step-num`, `.lp-start-card > span`, `.step-num` all render this way. Ordered step lists use `.steps` (`ol`) with hairline separators: each `li` is a two-column grid with the number (an explicit `.step-num` or a CSS counter) in column 1 beside the first content row and the content — a single wrapper or several sibling elements — in column 2, so items add no empty rows.
- Avatars: `.avatar` with `--size` (default 36px), plus `.avatar-sm` 28, `.avatar-md` 36, `.avatar-lg` 56: sunken background, ink initials, 600, radius full.
- Forms: `.label` (sm, 500, ink, margin-bottom 6px), `.input`, `.select`, `.textarea` (38px min height, white, 1px border, radius md, focus ring), `.form-group`, `.form-row` (two columns ≥640px), `.form-help` (xs, muted). Invalid: error border + `--color-error-subtle` background.
- Tables: `.data-table` (sm, ink; column `th` xs/600 uppercase tracked muted with a strong rule; `th[scope="row"]` row headers render in sentence case, 500, ink, wrapping, left-aligned with the same hairline as `td`; td hairline; hover row surface), wrapped by `.table-wrap` (overflow auto, radius lg, border, `contain: inline-size` so a wide table scrolls inside the wrapper instead of widening the page). A scroll region must be keyboard-reachable, so a `.table-wrap` that may overflow on phones needs `tabindex="0"`, `role="region"` and an `aria-label`; a `.table-wrap` without `tabindex` wraps instead below 640px — cells drop to xs, `white-space: normal`, `overflow-wrap: break-word` and `hyphens: auto` — so a table of up to four short columns fits a phone without becoming a scroll region; only an unbreakable token (a long URL) still scrolls, and that page must add the attributes. `.compare-table` and `.pricing-table` share it.
- Disclosure: `.disclosure` (`details`): summary md/600 with a chevron, hairline separators; `.trust-detail` and `.faq-item` render the same.
- Callouts: `.callout` (sunken band, radius lg, padding 16/20, 3px left rule in teal) with `.callout--warn` (warning colors), `.callout--error` and `.callout--success`. Normal flow: bare text with `<strong>`, `<p>`s, a list and a trailing `.btn` all stack; a leading `svg` switches it to an icon-plus-body flex row. `.safety-notice` (icon + `.safety-notice-body`) and `.trust-next-step` render the same way.
- Code: `.code-block` (ink background `#1a1816`, paper text, JetBrains Mono 13px, radius lg, no window dots) with optional `.code-block-label` (xs muted, above). Long lines wrap (`pre-wrap` + `overflow-wrap: anywhere`) and every code container has `contain: inline-size`, so code never widens a flex or grid parent and never becomes a scroll region (axe would require one to be focusable). `.code-block--scroll` opts into `white-space: pre` horizontal scrolling and then needs `tabindex="0"` plus an `aria-label` in markup, as api-docs does. Bare `pre` wraps the same way. Page-local `.c-*` syntax colours are tuned for the ink background.
- Empty state: `.empty-state` centered, padding `--space-12`, `.empty-state-icon` (40px tile, sunken, radius lg, holds a 20px SVG), title base/600, text sm muted, action button.
- Stat: `.stat-card` (white card; `.stat-label` xs muted; `.stat-value` 2xl/600 tabular; `.stat-sub` xs faint). Static `.stat-num` / `.stat-label` pairs render the same.
- Toasts: `.toast-container` (fixed, bottom center, gap 8px, z-index 1000), `.toast` (ink background, paper text, radius md, shadow lg, 13px), `.toast-success` (left rule success), `.toast-error` (left rule error).
- Modals: `.modal-overlay` (fixed inset 0, `rgba(26,24,22,.45)`, z-index 900, fade-in 150ms with `animation-fill-mode: forwards`, no opacity 0 at rest), `.modal` / `.modal-dialog` (white, radius lg, shadow lg, max-width 480px, padding 24), `.modal-title` (lg/600), `.modal-body`, `.modal-actions` (right-aligned buttons), `.modal-header` + `.modal-close`.
- Pagination: `.pagination` with `.page-btn` (32px, radius md, border on hover, active = ink background paper text). Compact by default.
- Skeleton: `.skeleton` shimmer on sunken background, radius sm.

### Public layout classes (static pages and SPA landing)

- `.pub-hero`: section with `padding-block: clamp(3rem, 7vw, 5.5rem)`, hairline bottom. Inner grid `.pub-hero-inner` is one column (max 720px) by default and two columns (`1.1fr 0.9fr`, gap 48px) when it contains `.pub-hero-aside` and the viewport is ≥ 960px. Children: `.pub-hero-label` (label), `h1` (3xl or display, 600), `.pub-hero-lead` (lg, muted, max 34em), `.pub-hero-actions` (flex wrap, gap 12), `.pub-hero-facts` (definition list of 2–3 verifiable facts, sm, hairlines). `.pub-hero-aside` holds a real product surface: a card, a form, or a table.
- `.pub-section`: `padding-block: var(--section-y)`; `.pub-section--alt` uses `--color-surface` with hairline top and bottom; `.pub-section--tight` halves the padding. Inner `.pub-inner` (wide) or `.pub-inner--narrow` (prose width).
- `.pub-grid`: `grid-template-columns: repeat(auto-fit, minmax(260px, 1fr))`, gap 20; `.pub-grid--2` and `.pub-grid--3` fix columns at ≥ 900px; single column below 640px.
- `.pub-split`: two-column (`minmax(0,1fr) minmax(0,1.4fr)`, gap 48) for a heading beside a list or table; stacks below 900px.
- `.pub-cta`: closing band (`--color-surface-sunken`, radius lg, padding 40, hairline) with h2 (xl), one paragraph, one primary and one ghost button in `.pub-cta-actions` (a `.cluster` inside `.pub-cta` behaves identically; both go full-width on phones).
- `.breadcrumb`: xs muted, separators `/`, margin-bottom 16.
- `.article`: max-width prose; `.article-header` (label, h1 3xl, `.article-meta` xs muted: date · reading time · author), `.prose` body: md size, body color, relaxed leading; h2 xl/600 with 40px top margin; h3 lg/600; p/ul/ol margin 1em; links underlined; `blockquote` with teal left rule; `table` renders as `.data-table`; `img`, `figure` full width radius lg; `code` inline sunken. `.article-footer` for related links.
- Hub lists: `.card-list` (single column of link rows: title over a one-line description, trailing arrow drawn by CSS) for index/hub pages; use `.pub-grid` only when items are truly parallel and short. Each row is a two-column grid, so two markup forms render identically: wrapped `<a class="card card--interactive"><span class="card-list-body"><span class="card-list-title">…</span><span class="card-list-desc">…</span></span></a>` and flat `<a class="card card--interactive"><span class="card-title">…</span><span class="card-text">…</span></a>` (or `<div class="card-title">` + `<p class="card-text">`, or `<li><a>…</a></li>` rows). An explicit `.card-list-arrow` child replaces the generated arrow.
- Utilities: `.text-muted`, `.text-faint`, `.text-sm`, `.text-xs`, `.mt-2/-3/-4/-6/-8`, `.mb-2/-3/-4/-6/-8`, `.stack > * + *` (flow spacing 16px), `.cluster` (inline flex wrap gap 8), `.visually-hidden`, `[hidden]` respected everywhere.

## 4. App components (app.css, index.html only)

- Shell: `.app-shell` grid with `.app-sidebar` 248px (surface, hairline right), `.app-main`, `.app-header` (56px, hairline bottom, sticky), `.app-content` (padding 32, max 1200). `.sidebar-logo`, `.sidebar-section`, `.sidebar-label` (xs 500 uppercase tracked muted), `.sidebar-item` (base/500 muted; icon 18px; radius md; hover surface-sunken; `.active` = ink text on sunken), `.sidebar-footer`, `.sidebar-user*`, `.sidebar-overlay` and `.open` state for mobile, `.mobile-toggle`, `.notif-btn` + `.notif-badge`.
- Page header: `.page-title` (2xl/600) + `.page-desc` (base muted) with an optional `.page-actions` on the right (`.page-header` flex row).
- Dashboard: `.stats-grid` (auto-fit 220px), `.dash-quick-actions` + `.quick-action-grid` + `.quick-action-card` (card--interactive with `.qa-icon` 36px tile, `.qa-label`, `.qa-desc`), `.dash-section`.
- Lists: `.task-list` + `.task-card` (card with header row, `.task-card-title` md/600, `.task-card-meta` sm muted, `.task-card-skills`), `.notif-item` (+`.unread` with teal dot), `.review-item`, `.reviews-list`.
- Browse: `.browse-page`, `.browse-header` (`.browse-title` 2xl/600, `.browse-sub`), `.browse-layout` (sidebar 260px + content), `.browse-sidebar` (sticky at `--nav-h + 16px`; on mobile collapsible via `[data-service-filters]` / `.is-open`), `.filter-group`, `.browse-grid` (auto-fill 280px), `.job-list`, `.browse-summary`, `.seller-cta-card`, `.browse-filter-toggle`.
- Cards: `.svc-card` (card--interactive; `.svc-card-cat` label, `.svc-card-title` md/600 2-line clamp, `.svc-card-desc` sm muted 2-line clamp, `.svc-card-worker` row with avatar, `.svc-card-rating` stars, `.svc-card-footer` with `.svc-card-price` md/700 tabular, `.svc-card-details` sm teal), `.job-card` (row card: `.job-card-main`, `.job-card-right` with `.job-budget` md/700 and status badge, `.job-card-desc`).
- Detail: `.detail-back`, `.detail-body` (content + 320px sticky sidebar), `.detail-title` (2xl/600), `.detail-meta`, `.svc-worker-row`, `.svc-seller-row`, `.svc-order-card` (card with `.svc-price-big` 2xl/700, `.svc-order-meta`, a full-width `.btn-lg` primary button, `.svc-order-note` xs muted), `.milestone-list`, `.order-summary`, `.fee-breakdown` (dl with hairlines, tabular numbers).
- Auth: `.auth2-page` (centered, paper), `.auth2-wrap` 420px, `.auth2-logo`, `.auth2-card` (white, radius lg, padding 32, shadow sm), `.auth2-title` (xl/600), `.auth2-sub`, `.auth2-google-wrap`, `.auth2-divider` (hairline with "or"), `.auth2-error` (error callout), `.auth2-toggle`, `.auth2-terms-label`.
- Stars: `.stars-row` (accent color glyphs, xs count, `aria-hidden` glyphs inside a labelled `role="img"`), `.stars` / `.star` (interactive rating: 24px, keyboard focus ring; unfilled glyphs are muted ink because they are real text inside a labelled button, filled glyphs are ochre).
- Admin: `.admin-health` uses `.card` + `.data-table`.

## 5. Page patterns

- Home (SPA landing): two-column hero with copy on the left and the task-draft form (`.lp-guided-intake`) as the product surface on the right; "Start here" as a 2×2 grid of route cards; "How it works" as a heading beside a numbered list; marketplace preview as three real listing cards; a final two-card row for workers and agents. Labels are sentence case; no gradient band; sections alternate paper and surface with hairlines.
- Editorial static page (about, pricing, faq, trust, how-it-works, docs): `.pub-hero` (one column), then `.pub-section`s with `.pub-split` or `.pub-inner--narrow`, key facts as `.data-table`, FAQ as `.disclosure`, closing `.pub-cta`.
- Hub page (hire/index, use-cases/index, ai-human-qa/index, blog/index, vs/index, tools/index, categories/index): `.pub-hero` + `.card-list` or `.pub-grid` of `.card--interactive` links with a one-line description. No gradient thumbnails.
- Article (blog posts): `.article` with `.article-header`, `.prose`, one `.callout` for the related task CTA, `.article-footer` related links.
- Task-template page (hire/*, use-cases/*, ai-human-qa/*): `.pub-hero` with the draft CTA in `.pub-hero-actions`, then "Example tasks", "Suggested payout ranges" as a small `.data-table`, "How to scope it" as `.steps`, related pages as `.card-list`, `.pub-cta`.
- Tool page (calculators, quiz, brief generators): `.pub-hero` with the tool inside `.pub-hero-aside` (card), results as `.data-table`.
- Comparison page (vs/*): `.pub-hero` + `.data-table` comparison + prose + `.pub-cta`.

## 6. Accessibility baseline

Every page: one `<h1>`, a `<main>` landmark, one `<footer>`, a skip link as the first focusable element, decorative SVGs `aria-hidden="true"`, icon-only buttons with `aria-label`, form controls with associated labels, visible focus rings (`:focus-visible` uses `--ring`), text contrast ≥ 4.5:1 for all text (muted `#6b6963` is 5.0:1 and faint `#6f6c65` is 4.8:1 on paper; both clear 4.5:1 on sunken and on every status tint), links inside running text underlined, no scrollable region without a focusable element (code wraps by default, so none is created), no horizontal scroll at 320px, touch targets ≥ 40px on phones.

## 7. Budgets

`frontend/performance-budgets.json` tracks `style_css_max_bytes`, `app_css_max_bytes`, `homepage_max_bytes`, and `static_html_max_bytes`; `scripts/performance_budget.py` enforces them. Budgets are source-size budgets with at least 10% headroom over the shipped size: `style_css_max_bytes` 75 000 (shipped ≈ 65 KB), `app_css_max_bytes` 42 000 (shipped ≈ 33 KB).
