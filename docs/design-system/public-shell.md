# Public shell design-system contract

Canonical public shell components live in `frontend/partials/`:

- `public-nav.html`
- `public-footer.html`

Rules:

1. Public pages should use the shared `lp-nav` navigation language.
2. Primary buyer CTA remains `Request QA`; the first nav item is `Starter QA`.
3. Worker route must be labeled `For Workers`, not generic `Open Jobs`.
4. Footer text and links must use readable text tokens, not low-contrast inline rgba values.
5. Navigation pages should include `style.css?v=20260526-nav-consistency`, and public nav hover/active states must not inherit page-local `a:hover` rules.

Regression coverage is in `backend/test_deep_audit_regressions.py`.
