FROM python:3.12-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Create persistent data directory for Railway volume mount.
# Railway mounts a volume to /data — if no volume is attached, this dir
# still exists but data is ephemeral (lost on redeploy).
RUN mkdir -p /data && chmod 777 /data

# Expose port (Railway sets PORT env var)
EXPOSE 8080

# Single worker: intentional for SQLite (single-writer constraint).
# Increase --workers when migrating to PostgreSQL.
CMD gunicorn --bind 0.0.0.0:${PORT:-8080} --workers 1 --threads 4 --timeout 120 server:app
