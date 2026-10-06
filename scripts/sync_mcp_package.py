#!/usr/bin/env python3
"""Generate the standalone package copy from backend/mcp_server.py.

The backend file is canonical. Never hand-edit the package copy. Run --check
before building; the backend suite also enforces byte equality.
"""
import argparse
from pathlib import Path

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--check', action='store_true', help='Fail if the generated copy is stale')
args = parser.parse_args()
root = Path(__file__).resolve().parents[1]
source = root / 'backend/mcp_server.py'
target = root / 'backend/mcp-package/mcp_server.py'
canonical = source.read_bytes()
if args.check:
    if not target.is_file() or target.read_bytes() != canonical:
        parser.exit(1, 'MCP package source is stale; run python3 scripts/sync_mcp_package.py\n')
    print('MCP package source matches the canonical backend server')
else:
    target.write_bytes(canonical)
    print('Generated backend/mcp-package/mcp_server.py from backend/mcp_server.py')
