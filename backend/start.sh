#!/usr/bin/env bash
set -euo pipefail

if [ -d "/data" ]; then
    chmod 700 /data 2>/dev/null || true
    echo "[GoHireHumans] /data directory ready"
fi

exec python server.py
