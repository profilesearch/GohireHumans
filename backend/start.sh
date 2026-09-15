#!/usr/bin/env bash
set -euo pipefail

if [ -d "/data" ]; then
    chmod 700 /data 2>/dev/null || true
    echo "[GoHireHumans] /data directory ready"
fi

# Production WSGI server. server.py initializes the database schema and starts
# the notification maintenance worker at import time, so gunicorn's import of
# server:app performs the same startup work `python server.py` used to.
exec gunicorn --bind 0.0.0.0:${PORT:-8080} --workers 1 --threads 8 --timeout 120 --graceful-timeout 30 --keep-alive 5 --access-logfile - --error-logfile - server:app
