# GoHireHumans design system ("Broadsheet", September 2026 redesign)

This document is the contract for `frontend/style.css` (shared, loaded by every page; it carries the reset and body base in its section 2), `frontend/app.css` (signed-in app surfaces, loaded by `index.html` only), `frontend/base.css` (the same reset as style.css section 2, linked by `index.html` only — static pages must not add it), and `frontend/mobile-hardening.css` (phone-width overflow and touch-target guards, a no-op above 768px). Static pages must build on these classes instead of page-local `<style>` blocks. The public shell (nav and footer partials) and its CI contract are described in `public-shell.md`.

## 1. Direction

Broadsheet. GoHireHumans is a marketplace for small, scoped, reviewable human work, and the site should read as made by people, for people: warm paper, ink, and one stamp-red mark, set like the classifieds page of a good newspaper. Headlines and leads are a serif (Newsreader); interface text and body copy are a humanist sans (Instrument Sans). Structure comes from rules and columns, not from cards, tints, or shadows. Numerals are typographic ("No. 1", "01"), labels are red small caps with a short rule, and every major section is separated by a 1px ink rule; the footer sits under a double rule.

What we removed on purpose: the teal accent and every tinted pill and band; white cards with rounded corners and shadows as the default container; hover lifts; the tinted callout band with a coloured left rule; the icon-in-a-rounded-square logo in the public shell (it survives only as an ink mark in the app sidebar, the sign-in page, and the favicon); Inter.

What we kept: the tokenized architecture and every class name (tests pin them), the shared nav/footer partials and their sync contract, the reset and typographic care, the accessibility baseline, the SVG icon set, JetBrains Mono for code, and the card-based app surfaces in `app.css` (they now sit on paper with hairline borders, which reads fine inside a working product).

## 2. Tokens

Defined once on `:root` in `style.css`. No dark-mode token block ships.

### Base (style.css section 2, mirrored by base.css)

Border-box sizing and zero margins on everything; `body` in Instrument Sans at `--text-base` / `--leading-normal`, ink on paper, antialiased; `h1`–`h3` in Newsreader (400, tight leading, `--tracking-heading`), `h4`–`h6` sans 600; `main { min-width: 0 }`; block-level media at `max-width: 100%`; form controls inherit font; unstyled buttons; collapsed full-width tables; balanced headings; `p, li, figcaption` pretty-wrapped at `max-width: 72ch` (row-type list items opt out); red-tinted `::selection`; the `--ring` focus indicator on `:focus-visible`; reduced-motion kill switch; `.sr-only` / `.visually-hidden`; `[hidden] { display: none !important }`. `hr` is a 1px ink rule. `blockquote` is serif italic with a 3px red rule. Unclassed flow content gets `h1–h4 + p` 0.5em, `p + p` 1em, `p + h2` 1.75em, and padded unclassed lists.

### Color

| Token | Value | Use |
|---|---|---|
| `--color-bg` | `#f4efe6` | page background (paper) |
| `--color-surface` | `#f4efe6` | nav, footer, mobile menu — same paper as the page |
| `--color-surface-2` | `#fbf8f2` | inputs, the task-draft form, modals, auth card, tool cards in a hero aside |
| `--color-surface-sunken` | `#ede6d8` | the darker paper: alt bands (`.pub-section--alt`, `.lp-light-band`), closing CTA band, inline code, wells, table row hover |
| `--color-divider` | `#d9d2c5` | hairlines between rows and columns |
| `--color-border` | `#c7bfb0` | input borders, outlined boxes (`.card`, `.stat-card`, `.feature-item`) |
| `--color-border-strong` | `#1a1816` | ink rules: section separators, table header rule, first/last rule of a ruled list, hover border on interactive boxes |
| `--color-rule-dotted` | `#a9a093` | reserved for dotted classified rules |
| `--color-text` | `#1a1816` | ink |
| `--color-text-body` | `#2b2723` | long-form and lead copy |
| `--color-text-muted` | `#6b645b` | secondary text (5.1:1 on paper, 4.7:1 on sunken) |
| `--color-text-faint` | `#6d665d` | meta text, placeholders, timestamps (4.6:1 on paper) |
| `--color-text-inverse` | `#f4efe6` | text on ink |
| `--color-primary` | `#1a1816` | ink is the action colour: links, primary buttons, selected states |
| `--color-primary-hover` / `-active` | `#000000` | |
| `--color-primary-subtle` | `#ebe4d6` | selected states, highlighted table cells |
| `--color-primary-text` | `#f4efe6` | text on primary |
| `--color-accent` | `#b8321f` | stamp red: labels, numerals, the wordmark period, active-nav rule, link hover, featured markers, focus ring (5.2:1 on paper) |
| `--color-accent-hover` | `#9a2818` | |
| `--color-accent-subtle` | `#f3e1dc` | featured pill background |
| `--color-star` | `#a86a12` | ochre: star-rating glyphs only |
| `--color-success` / `-subtle` | `#2e6b3a` / `#e4eedf` | |
| `--color-warning` / `-subtle` | `#8a5a00` / `#f6ebd2` | 5.0:1 on its own tint |
| `--color-error` / `-subtle` | `#a8261b` / `#f6e1dd` | (`--color-danger` is an alias) |
| `--color-info` / `-subtle` | `#2f5484` / `#e3e9f1` | in-progress states |

Status colours map onto the above exactly as before (`open`, `active` → ink; `reviewing`, `pending`, `submitted`, `reserved` → warning; `in_progress`, `hired` → info; `completed` → success; `disputed` → error; `draft`, `canceled`, `paused`, `removed` → muted). Ink tints are derived with `color-mix(in oklab, var(--color-primary) N%, transparent)` at 12%, 18%, and 35%; the running-text underline is `--color-accent-tint-55`. Never spell out rgba ink or red values.

### Typography

Two families from one Google Fonts link on every page, plus JetBrains Mono where a page shows code:
`https://fonts.googleapis.com/css2?family=Instrument+Sans:wght@400..700&family=Newsreader:ital,opsz,wght@0,6..72,400..700;1,6..72,400..700&display=swap` (append `&family=JetBrains+Mono:wght@400;500` on docs and tool pages; `index.html` loads `JetBrains+Mono:wght@400;600`). Newsreader is a variable optical-size face, so `font-optical-sizing: auto` (the default) does the right thing at every size.

| Token | Size | Face | Use |
|---|---|---|---|
| `--text-xs` | 0.75rem (12px) | sans | meta, labels, badges, table headers |
| `--text-sm` | 0.8125rem (13px) | sans | UI labels, secondary text, table cells |
| `--text-base` | 0.9375rem (15px) | sans | UI body, buttons, nav links |
| `--text-md` | 1.0625rem (17px) | sans | buttons (`.btn-lg`), mobile menu links |
| `--text-lg` | 1.25rem (20px) | serif | card and row titles, step titles, disclosure summaries, h3 |
| `--text-xl` | 1.625rem (26px) | serif | wordmark, section h3, listing titles, modal titles, prose h3 |
| `--text-2xl` | `clamp(2rem, 1.55rem + 1.6vw, 2.75rem)` — 32px on phones, 44px from 1200px | serif | section h2, app page titles, stat values, closing CTA h2 |
| `--text-3xl` | `clamp(2.5rem, 1.9rem + 2.2vw, 3.5rem)` — 40px to 56px | serif | static page h1, article h1 |
| `--text-display` | `clamp(2.75rem, 1.6rem + 4.2vw, 6rem)` — 44px to 96px | serif | home and #/ai-employers hero h1 (the home hero caps at 80px via app.css) |
| `--text-lead` | `clamp(1.125rem, 1rem + 0.5vw, 1.375rem)` — 18px to 22px | serif | hero leads, prose lead |

Line heights: `--leading-tight 0.98` (display), `--leading-snug 1.12` (serif headings), `--leading-normal 1.55` (UI), `--leading-relaxed 1.7` (prose). Tracking: `-0.025em` on display, `-0.02em` on 2xl/3xl, `-0.01em` on serif titles, `--tracking-label 0.14em` on small-caps labels. Serif headings are weight 400 (titles at lg/xl are 500); sans UI is 400/500/600. Weight 700 is retired everywhere except `<strong>` inside prose (600). Uppercase with tracking is allowed on labels (`.eyebrow` family), `.badge`, `th`, `.stat-label`, `.lp-footer-heading`, `.article-meta`, `.pub-hero-facts dt`, `.lp-masthead-inner`, and `.sidebar-label`.

Prose (`.prose`, blog posts) is set in Newsreader at 18px / 1.65; tables, callouts, badges, buttons and steps inside prose switch back to the sans.

### Spacing and layout

`--space-1` 4px through `--space-24` 96px on a 4px scale. `--section-y: clamp(3.5rem, 8vw, 6rem)` for public-page sections. `--nav-h: 64px` is the sticky bar; `--masthead-h: 34px` is the strip above it that scrolls away (the wrap is `position: sticky; top: calc(-1 * var(--masthead-h))`). Content widths: `--content-prose 720px`, `--content-narrow 640px`, `--content-default 960px`, `--content-wide 1200px`. Public sections use `--content-wide` with `padding-inline: var(--space-6)`.

### Radius, shadow, motion

Every radius token is 2px (`--radius-sm/md/lg/xl`) except `--radius-full`. Shadows exist for overlays only: `--shadow-lg` on modals and toasts; nothing on the page lifts or casts. `--ring: 0 0 0 3px color-mix(in oklab, var(--color-accent) 40%, transparent)` — red, so it is visible on paper and on ink buttons. `--transition-fast 120ms` and `--transition-base 180ms` with `cubic-bezier(.16,1,.3,1)`; transitions name properties, never `all`. Interactive rows and boxes change colour (title turns red, border turns ink); they do not move.

## 3. Shared components (style.css)

Class names that tests pin are kept verbatim; new classes are additive.

- Buttons: `.btn` + `.btn-primary` (ink on paper), `.btn-secondary` and `.btn-outline` (transparent, 1px ink border), `.btn-ghost` (no border), `.btn-danger`; sizes `.btn-sm` (36px), default (44px), `.btn-lg` (52px). Square corners, sans 600, inline-flex, gap 8px, red focus ring. Disabled: 50% opacity.
- Links: ink, no underline at rest, red with an underline on hover. Links inside running text (`p`, `li`, `dd`, `td`, `th`, `small`, `figcaption`, `blockquote`, `.callout`, `.form-help`, auth copy) carry a 55% red underline at rest that turns solid on hover, so they are distinguishable from ink copy (axe `link-in-text-block`). The rule is zero-specificity (`:where()`), so buttons, nav and footer links, breadcrumbs, badges and row links keep their own treatment.
- Cards: `.card` is an outlined box on the paper (transparent, 1px `--color-border`, 2px radius, padding `--space-5`); `.card--interactive` turns the border ink and the `.card-title` red on hover; `.card--sunken` fills with the darker paper; `.card-title` (serif lg/500), `.card-text` (sm, muted). `.pub-hero-aside .card` (tool pages) fills with `--color-surface-2` inside an ink border. Existing `.svc-card`, `.job-card`, `.task-card`, `.stat-card`, `.starter-offer-card`, `.quick-action-card`, `.feature-item` render as outlined boxes; `app.css` cards keep their `--color-surface-2` fill.
- Badges: `.badge` is a small outlined stamp — 11px, 600, uppercase, 0.08em tracking, 1px `currentColor` border, transparent background — coloured by status (`.badge-open` ink, `.badge-completed` success, `.badge-disputed` error, warning and info as before, `.badge-draft` muted, `.badge-human` muted, `.badge-ai` info). `.skill-tag` and `.pill` stay soft: sunken fill, hairline, radius full.
- Labels: `.eyebrow`, `.section-label`, `.lp-section-label`, `.lp-hero-eyebrow`, `.pub-hero-label`, `.blog-hero-label`, `.vs-eyebrow` all render as the red small-caps label: 12px, 600, uppercase, 0.14em tracking, with a 28px red rule before it (`::before`), left-aligned.
- Section head: `.section-head` (label + h2 + optional p); `h2` / `.lp-section-head` is serif 2xl/400 at line-height 1.05; `p` / `.lp-section-sub` is sans base, muted, max 60ch.
- Steps: `.step-num` (and `.lp-start-card > span`, `.trust-detail summary > span`) is a serif italic red numeral at 20px in a 28px column, no tile. `.steps` (`ol`) rows are separated by hairlines with an ink rule above the first and below the last; each `li` is a two-column grid (numeral, content); titles (`h3`, `strong`) are serif lg/500, copy is sans base in body ink. `.steps--display` (home page "How it works") swaps in leading-zero numerals (`01`) at 40–64px, weight 300, in a 96px column (56px on phones).
- Avatars: unchanged (`--size`, sunken fill, hairline).
- Forms: `.label` (sm, 600), `.input`, `.select`, `.textarea` (42px min height, `--color-surface-2`, 1px `--color-border`, 2px radius; hover and focus borders turn ink, focus adds the red ring), `.form-group`, `.form-row`, `.form-help`, `.form-error`. Invalid: error border + error tint.
- Tables: `.data-table` (sans sm; column `th` xs/600 uppercase 0.08em muted above a 1px ink rule; `th[scope="row"]` sentence case 600 ink; `td` hairlines; hover row on the darker paper), wrapped by `.table-wrap` (hairline border, 2px radius, transparent, `contain: inline-size`; the phone-wrapping and `tabindex` rules are unchanged). `.compare-table` and `.pricing-table` share it. Highlighted cells use `--color-primary-subtle` with ink text.
- Disclosure: `.disclosure` / `.trust-detail` / `.faq-item`: summary is serif lg/500 with an ink chevron and turns red on hover; hairlines between items, ink rules above the first and below the last.
- Callouts: `.callout` is a ruled box — transparent, 1px ink border, 2px radius, padding 16/20 — with `.callout--warn`, `.callout--error`, `.callout--success` switching to the semantic border and tint. Leading `svg` (red) switches it to an icon-plus-body row. `.safety-notice` and `.trust-next-step` render the same way.
- Code: `.code-block` unchanged (ink background, paper text, JetBrains Mono 13px, 2px radius, wraps by default; `.code-block--scroll` needs `tabindex="0"`).
- Empty state: `.empty-state` centered; icon tile is a sunken 40px square with a red glyph; title serif lg/500.
- Stat: `.stat-card` outlined box; `.stat-label` xs uppercase tracked muted; `.stat-value` / `.stat-num` serif 2xl/400 tabular; `.stat-sub` xs faint.
- Toasts: ink background, paper text, 2px radius, `--shadow-lg`; `.toast-success` and `.toast-error` keep a coloured left rule.
- Modals: `.modal-overlay` (ink at 50%), `.modal` / `.modal-dialog` (`--color-surface-2`, 1px ink border, 2px radius, `--shadow-lg`, max 480px), `.modal-title` serif xl/500.
- Pagination and skeleton: unchanged behaviour with the new tokens.

### Public layout classes (static pages and SPA landing)

- `.pub-hero`: `padding-block: clamp(3.5rem, 8vw, 6rem) clamp(3rem, 6vw, 4.5rem)` with a 1px ink rule below. Inner grid `.pub-hero-inner` is one column (max 820px) by default and two columns (`1.1fr 0.9fr`, gap 48px) when it contains `.pub-hero-aside` and the viewport is ≥ 901px. Children: `.pub-hero-label` (label), `h1` (serif 3xl/400 at line-height 1.02, or display with `.pub-hero--display`), `.pub-hero-lead` (serif `--text-lead`, body ink, max 34em), `.pub-hero-actions` (flex wrap, gap 16), `.pub-hero-facts` (definition list under an ink rule: `dt` xs uppercase tracked muted, `dd` sm/500 ink, hairlines between rows). `.pub-hero-aside` holds a real product surface: a form, a filled card, or a table.
- `.pub-section`: `padding-block: var(--section-y)`; `.pub-section--alt` is the darker paper between two ink rules; `.pub-section--tight` halves the padding. Inner `.pub-inner` (wide) or `.pub-inner--narrow` (prose width).
- `.pub-grid`, `.pub-grid--2`, `.pub-grid--3`, `.pub-split`: unchanged geometry (gap 20 / 48; stack below 900px / 640px).
- `.pub-cta`: closing band on the darker paper inside a 1px ink border, padding 40; h2 serif 2xl, one paragraph, one primary and one secondary button in `.pub-cta-actions` (full-width on phones).
- `.breadcrumb`: xs muted, separators `/`, red on hover, margin-bottom 20.
- `.article`: max-width prose; `.article-header` (label, h1 3xl, `.article-meta` xs uppercase tracked muted, ink rule below); `.prose` body in Newsreader 18px / 1.65 (h2 30px/400, h3 xl/500, h4 sans 600, red-tinted link underlines, serif italic blockquote, ink code blocks, sans tables and callouts, `img`/`figure` square); `.article-footer` under an ink rule.
- Hub lists: `.card-list` is a ruled list — an ink rule on top, hairlines between rows, an ink rule after the last — with no boxes: each row is a two-column grid (serif lg/500 title over a sm muted description, trailing ink arrow); hover turns title and arrow red. Both markup forms (`.card-list-item` with `.card-list-body`, or a flat `.card.card--interactive` with `.card-title` + `.card-text`) render identically.
- Utilities: unchanged (`.text-muted`, `.text-faint`, `.text-sm`, `.text-xs`, `.mt-*`, `.mb-*`, `.stack`, `.cluster`, `.visually-hidden`).

## 4. App components (app.css, index.html only)

Everything in `app.css` consumes the tokens above, so the signed-in product re-skins automatically. Deliberate changes: `.page-title`, `.browse-title`, `.detail-title`, `.svc-price-big` are serif 2xl/400; `.svc-card-price`, `.job-budget`, `.lp-feed-price` are serif lg–xl/500; `.svc-card-cat` and `.job-card-cat` are red small-caps labels; `.sidebar-logo` is the serif wordmark beside the ink mark; star glyphs use `--color-star`; the unread dot uses `--color-accent`; the home hero title caps at 80px (`clamp(2.5rem, 1.5rem + 3.6vw, 5rem)`). App cards (`.svc-card`, `.job-card`, `.task-card`, `.quick-action-card`, `.filter-group`, `.skeleton-card`) keep a `--color-surface-2` fill with hairline borders and 2px corners; converting them to outlined boxes is optional and listed in the redesign plan.

## 5. Page patterns

- Shell: a 34px masthead strip (`.lp-masthead` — "A marketplace for small, scoped human help" on the left, "Est. 2026 · United States · Free to join" on the right, hidden on phones) above a 64px nav; the wordmark `GoHireHumans.` (Newsreader 500, 26px, red aria-hidden period); sans nav links that turn red on hover, the active link underlined by a 2px red rule; ghost "Sign in" and ink "Post a task". Footer under a 4px double rule: serif wordmark, xs tagline, small-caps column headings, muted links that turn red on hover.
- Home (SPA landing): hero with the red "Help wanted" label, the serif display headline beside the task-draft form (`.lp-guided-intake`, paper form in an ink frame), serif lead, ink and outlined buttons, and the three facts as a ruled definition list. "Start here" as four ruled columns (`.lp-start-grid`, "No. 1" italic numerals, serif titles). "How it works" on the darker paper with `.steps--display` (01/02/03). Marketplace preview as three classified columns (`.lp-feed-grid`: red category label, serif title, meta, serif price). Workers and agents as two columns split by a rule (`.lp-home-route-grid`). Sections are separated by ink rules; only "How it works" uses the darker band.
- Editorial static page (about, pricing, faq, trust, how-it-works, docs): `.pub-hero` (one column), then `.pub-section`s with `.pub-split` or `.pub-inner--narrow`, key facts as `.data-table`, FAQ as `.disclosure`, closing `.pub-cta`.
- Hub page (hire, use-cases, ai-human-qa, blog, vs, tools, categories): `.pub-hero` + `.card-list` rows; `.pub-grid` of outlined boxes only when items are truly parallel and short.
- Article (blog posts): `.article` with `.article-header`, serif `.prose`, one `.callout` for the related task CTA, `.article-footer` related links.
- Task-template page (hire/*, use-cases/*, ai-human-qa/*): `.pub-hero` with the draft CTA in `.pub-hero-actions`, then "Example tasks", "Suggested payout ranges" as a `.data-table`, "How to scope it" as `.steps`, related pages as `.card-list`, `.pub-cta`.
- Tool page: `.pub-hero` with the tool inside `.pub-hero-aside` (filled card), results as `.data-table`.
- Comparison page (vs/*): `.pub-hero` + `.data-table` comparison + prose + `.pub-cta`.

## 6. Accessibility baseline

Every page: one `<h1>`, a `<main>` landmark, one `<footer>`, a skip link as the first focusable element, decorative SVGs and the wordmark period `aria-hidden="true"`, icon-only buttons with `aria-label`, form controls with associated labels, visible red focus rings (`:focus-visible` uses `--ring`), text contrast ≥ 4.5:1 for all text (muted `#6b645b` is 5.1:1 on paper and 4.7:1 on the darker paper; faint `#6d665d` is 4.6:1 on paper; red `#b8321f` is 5.2:1 on paper and 4.8:1 on the darker paper; every status colour clears 5:1 on its own tint), links inside running text underlined, no scrollable region without a focusable element, no horizontal scroll at 320px, touch targets ≥ 44px on phones.

## 7. Budgets

`frontend/performance-budgets.json` tracks `style_css_max_bytes` (75 000; shipped ≈ 72 KB), `app_css_max_bytes` (42 000; shipped ≈ 34 KB), `homepage_max_bytes`, and `static_html_max_bytes`; `scripts/performance_budget.py` enforces them. The shared stylesheet is close to its budget: trim before adding, and raise the budget deliberately (with the doc) rather than by accident.
