# MCP package maintenance

Status: local packaging preparation only; not published to PyPI, npm or the MCP Registry.

## One canonical source

Edit only `backend/mcp_server.py`. `backend/mcp-package/mcp_server.py` is a
**generated distribution mirror**, not another source to maintain. Keeping the
existing paths preserves direct single-file downloads, imports/monkeypatches in
tests, embedded docs and the historical (unpublished) npm source bundle. The
backend suite enforces byte equality; `scripts/sync_mcp_package.py --check` is the
release freshness gate. This deliberately uses the byte-equality option rather
than moving all handlers behind a shim, which would break single-file use and
source-inspection tests. No server code, tool schema, protocol or handler behavior
has changed.

## Build and test locally

Run from the repository root with uv installed:

```sh
python3 scripts/sync_mcp_package.py --check
uv build backend/mcp-package --out-dir backend/mcp-package/dist
uvx --from ./backend/mcp-package/dist/gohirehumans_mcp-2.0.0-py3-none-any.whl gohirehumans-mcp
```

The last command starts stdio and waits for JSON-RPC messages; it is not a web
server. Runtime has no third-party dependencies. The console entry point calls
the existing `mcp_server.main`.

Before any future release, regenerate after editing the canonical file:

```sh
python3 scripts/sync_mcp_package.py
python3 scripts/sync_mcp_package.py --check
```

Run the backend suite in a venv with `backend/requirements.txt`. Never call a
production write/payment tool as a smoke test. Start with no credentials and
`initialize`, `notifications/initialized`, `tools/list`, `get_categories`.

## Commands that will work only after separate publication approval

After this commit reaches the public repository:

```sh
uvx --from 'git+https://github.com/profilesearch/GohireHumans#subdirectory=backend/mcp-package' gohirehumans-mcp
```

Pin an approved commit SHA in the Git URL for reproducibility. After PyPI 2.0.0
is published:

```sh
uvx gohirehumans-mcp==2.0.0
```

The `mcp-name: io.github.profilesearch/gohirehumans` comment in README.md is
included in package metadata for official registry ownership verification.
Publishing is not part of local build/testing. npm remains unpublished and has
no executable `bin`; do not advertise `npx`.
