# Pinned base image — bump intentionally on a schedule, not at random.
FROM python:3.12.10-slim-bookworm

# Don't write .pyc files; flush stdout immediately so Railway logs are live.
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

# Create the runtime user up front so we can chown after copying.
RUN groupadd --system --gid 1000 app \
 && useradd --system --uid 1000 --gid app --create-home --shell /usr/sbin/nologin app

# Install dependencies first — this layer is cached unless requirements.txt changes.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Create /data fallback (Railway volume replaces this at runtime).
# Owned by app so the non-root process can write the SQLite DB.
RUN mkdir -p /app/data /data \
 && chown -R app:app /app /data \
 && chmod +x /app/start.sh

# Drop privileges
USER app

# Expose port (Railway sets PORT env var)
EXPOSE 8080

# Healthcheck mirrors railway.toml so docker-run also works.
HEALTHCHECK --interval=30s --timeout=10s --start-period=20s --retries=3 \
    CMD python -c "import urllib.request,sys; r=urllib.request.urlopen('http://127.0.0.1:'+__import__('os').environ.get('PORT','8080')+'/health',timeout=5); sys.exit(0 if r.status==200 else 1)" || exit 1

# Start script fixes volume permissions at runtime, then launches gunicorn
CMD ["/app/start.sh"]
