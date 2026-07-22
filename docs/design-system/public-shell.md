# Public shell design-system contract

Canonical public shell components live in `frontend/partials/`:

- `public-nav.html`
- `public-footer.html`

Rules:

1. Public pages use the shared `lp-nav` navigation language.
2. Desktop navigation is marketplace-first: `Marketplace`, `Find Work`, `For Agents`, `Pricing`, `Trust`.
3. Account and transaction controls remain separate: `Sign in` and the primary buyer action `Post a task`.
4. Starter offers, use cases, and FAQ remain secondary resources in the mobile menu and/or footer rather than competing desktop tabs.
5. Footer text and links use readable text tokens, not low-contrast inline rgba values.
6. Navigation pages include `style.css?v=20260526-nav-consistency`; public nav hover and active states must not inherit page-local `a:hover` rules.
7. High-visibility conversion and trust pages must not mix canonical `lp-footer` markup with old local `.footer` shells.
8. The mobile footer is intentionally compact: broad marketplace, company, and legal routes only; `Find Work` is the canonical worker label.
9. Public pages avoid visible builder/generator attribution and generic CTAs such as `Get started` when a specific action exists.
10. Use-case pages remain grouped under `Marketplace`; starter QA remains an optional wedge, not the platform-wide identity.

Run both executable contracts after changing the shell:

```bash
python3 scripts/sync_public_shell.py --check
python3 scripts/check_public_shell.py
```

Regression coverage is in `backend/test_deep_audit_regressions.py` and `frontend/tests/browser-regression.spec.js`.
