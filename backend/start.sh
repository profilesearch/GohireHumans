#!/bin/bash
# Ensure /data directory is writable (Railway volume permissions fix)
if [ -d "/data" ]; then
    chmod 777 /data 2>/dev/null || true
    echo "[GoHireHumans] /data directory permissions: $(ls -ld /data)"
fi

# Start gunicorn
exec gunicorn --bind 0.0.0.0:${PORT:-8080} --workers 1 --threads 4 --timeout 120 server:app
