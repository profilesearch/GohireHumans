#!/usr/bin/env python3
from pathlib import Path
ROOT = Path(__file__).resolve().parents[1]
required = ['frontend/partials/public-nav.html','frontend/partials/public-footer.html','docs/design-system/public-shell.md']
missing=[p for p in required if not (ROOT/p).exists()]
if missing:
    print('Missing public shell assets:', missing); raise SystemExit(1)
nav=(ROOT/'frontend/partials/public-nav.html').read_text()
for token in ['Starter QA','Marketplace','For Workers','For Agents','Pricing','Trust','Request QA','lp-nav']:
    if token not in nav:
        print('Missing nav token', token); raise SystemExit(1)
print('Public shell assets OK')
