#!/usr/bin/env python3
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
required = [
    'frontend/partials/public-nav.html',
    'frontend/partials/public-footer.html',
    'docs/design-system/public-shell.md',
]
missing = [p for p in required if not (ROOT / p).exists()]
if missing:
    print('Missing public shell assets:', missing)
    raise SystemExit(1)

nav = (ROOT / 'frontend/partials/public-nav.html').read_text(encoding='utf-8', errors='ignore')
footer = (ROOT / 'frontend/partials/public-footer.html').read_text(encoding='utf-8', errors='ignore')
for token in ['Starter QA', 'Marketplace', 'For Workers', 'For Agents', 'Pricing', 'Trust', 'Request QA', 'lp-nav']:
    if token not in nav:
        print('Missing nav token', token)
        raise SystemExit(1)
for token in ['lp-footer', 'Open Jobs for Workers', 'contact@gohirehumans.com', 'Direct-payment instructions are not allowed']:
    if token not in footer:
        print('Missing footer token', token)
        raise SystemExit(1)

# Guard high-visibility public shell pages. This is intentionally scoped: the repo still
# contains older SEO/support pages with local one-off shells; new conversion/trust pages
# should not regress to mixed footers, generic CTAs, or builder attribution.
critical_pages = [
    'frontend/index.html',
    'frontend/starter-offers.html',
    'frontend/pricing.html',
    'frontend/trust-safety.html',
    'frontend/proof-packs.html',
    'frontend/stats.html',
    'frontend/use-cases/index.html',
]
problems = {}
for rel in critical_pages:
    path = ROOT / rel
    text = path.read_text(encoding='utf-8', errors='ignore')
    issues = []
    if '<div class="lp-nav-wrap">' not in text:
        issues.append('missing canonical lp nav wrapper')
    if '<footer class="lp-footer"' not in text:
        issues.append('missing canonical lp footer')
    if 'Created with Perplexity Computer' in text or 'Perplexity Computer' in text:
        issues.append('builder attribution leaked')
    if '<div class="footer">' in text or '<footer class="footer"' in text:
        issues.append('legacy local footer still present')
    if '<li><a href="/#/jobs">Open Jobs</a></li>' in text:
        issues.append('ambiguous worker jobs footer label')
    if rel == 'frontend/stats.html' and 'Get started' in text:
        issues.append('generic stats CTA')
    if issues:
        problems[rel] = issues
if problems:
    print('Public shell drift detected:')
    for rel, issues in problems.items():
        print(f'- {rel}: {", ".join(issues)}')
    raise SystemExit(1)

print('Public shell assets OK')
