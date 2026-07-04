#!/usr/bin/env python3
import json
from pathlib import Path
ROOT = Path(__file__).resolve().parents[1]
FRONTEND = ROOT / 'frontend'
budgets = json.loads((FRONTEND / 'performance-budgets.json').read_text())
failures = []
checks = [('frontend/index.html', budgets['homepage_max_bytes']), ('frontend/style.css', budgets['style_css_max_bytes'])]
for rel, limit in checks:
    size = (ROOT / rel).stat().st_size
    if size > limit:
        failures.append(f'{rel} is {size} bytes > budget {limit}')
for path in FRONTEND.rglob('*.html'):
    rel_parts = path.relative_to(FRONTEND).parts
    if any(part in rel_parts for part in ['blog', 'test-results', 'node_modules', 'playwright-report']):
        continue
    size = path.stat().st_size
    if size > budgets['static_html_max_bytes'] and path.name != 'index.html':
        failures.append(f'{path.relative_to(ROOT)} is {size} bytes > static budget {budgets["static_html_max_bytes"]}')
if failures:
    print('Performance budget failures:')
    for f in failures: print('-', f)
    raise SystemExit(1)
print('Performance budgets OK')
