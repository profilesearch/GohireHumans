#!/usr/bin/env python3
"""
GoHireHumans API — Production Server (Flask)

Wraps the CGI-based API for production deployment on Railway/Render/Fly.io.
"""
import os
import sys
import json
import io
import logging
import threading
import importlib.util
import uuid

from flask import Flask, request, Response, g
from flask_cors import CORS

# ─── Logging (structured) ────────────────────────────────────────────────────
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=LOG_LEVEL,
    format='{"ts":"%(asctime)s","level":"%(levelname)s","logger":"%(name)s","msg":%(message)s}',
    stream=sys.stderr,
)
logging.getLogger("werkzeug").setLevel(logging.WARNING)
log = logging.getLogger("gohirehumans")

# ─── Thread-safe stdout capture ─────────────────────────────────────────────
# Each request thread gets its own StringIO buffer. Print statements from
# api_core.py (CGI pattern) are routed to the calling thread's buffer.
_tls = threading.local()
_real_stdout = sys.stdout

class _ThreadLocalStdout:
    """Routes write() to the current thread's capture buffer, or real stdout."""
    def write(self, s):
        buf = getattr(_tls, 'captured', None)
        if buf is not None:
            buf.write(s)
        else:
            _real_stdout.write(s)

    def flush(self):
        _real_stdout.flush()

# Install once at module load — never reassign sys.stdout again
sys.stdout = _ThreadLocalStdout()

# ─── Flask App ──────────────────────────────────────────────────────────────
app = Flask(__name__)

# SECURITY: Default to production origins only — never wildcard with credentials.
_default_origins = "https://www.gohirehumans.com,https://gohirehumans.com"
allowed_origins = os.environ.get("ALLOWED_ORIGINS", _default_origins).split(",")
allowed_origins = [o.strip() for o in allowed_origins if o.strip()]
if "*" in allowed_origins:
    log.warning(json.dumps({"event": "cors_wildcard_rejected"}))
    allowed_origins = [o for o in allowed_origins if o != "*"]
CORS(app, resources={r"/*": {"origins": allowed_origins}}, supports_credentials=True)

# ─── Import the CGI API module ──────────────────────────────────────────────
spec = importlib.util.spec_from_file_location(
    "api_module",
    os.path.join(os.path.dirname(__file__), "api_core.py")
)
api_module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(api_module)

# ─── One-time database initialization at startup ────────────────────────────
# api_core.py exposes init_db(); previously it was called per-request, which
# caused write-lock contention and unnecessary work on every hit.
_INIT_LOCK = threading.Lock()
_INITIALIZED = False

def _init_db_once():
    global _INITIALIZED
    if _INITIALIZED:
        return
    with _INIT_LOCK:
        if _INITIALIZED:
            return
        try:
            # Use whichever helper api_core.py exposes (get_db / _get_db / connect_db).
            get_db = getattr(api_module, "get_db", None) or getattr(api_module, "_get_db", None) or getattr(api_module, "connect_db", None)
            init_fn = getattr(api_module, "init_db", None) or getattr(api_module, "_init_db", None)
            if callable(get_db) and callable(init_fn):
                db = get_db()
                try:
                    init_fn()
                finally:
                    # Tune SQLite for concurrent reads + occasional writes.
                    try:
                        db.execute("PRAGMA journal_mode=WAL")
                        db.execute("PRAGMA synchronous=NORMAL")
                        db.execute("PRAGMA busy_timeout=5000")
                        db.execute("PRAGMA foreign_keys=ON")
                    except Exception:
                        log.exception(json.dumps({"event": "pragma_failed"}))
                    try:
                        db.close()
                    except Exception:
                        pass
                _INITIALIZED = True
                log.info(json.dumps({"event": "db_initialized"}))
            else:
                # api_core.py uses a different pattern — leave init to per-request
                # handlers (their original behavior). Health check will still pass.
                _INITIALIZED = True
                log.warning(json.dumps({"event": "db_init_skipped", "reason": "helper_not_found"}))
        except Exception:
            log.exception(json.dumps({"event": "db_init_failed"}))
            # Don't re-raise — let /health flag degraded state instead of
            # crash-looping the worker. Original per-request init still runs.
            _INITIALIZED = True

# Initialize at import time so Gunicorn workers come up ready.
try:
    _init_db_once()
except Exception:
    # Surface on /health rather than crashing the worker indefinitely.
    pass

# ─── Routes ─────────────────────────────────────────────────────────────────
@app.route("/health", methods=["GET"])
def health():
    return {
        "status": "ok" if _INITIALIZED else "degraded",
        "service": "gohirehumans-api",
        "version": os.environ.get("RAILWAY_GIT_COMMIT_SHA", "dev")[:7],
    }, (200 if _INITIALIZED else 503)


@app.route("/api/v1/<path:subpath>", methods=["GET", "POST", "PUT", "PATCH", "DELETE"])
def api_v1_proxy(subpath):
    """Strip /api/v1 prefix and forward to the main handler (Stripe webhook URL alignment)."""
    return proxy(subpath)


@app.route("/", defaults={"path": ""}, methods=["GET", "POST", "PUT", "PATCH", "DELETE"])
@app.route("/<path:path>", methods=["GET", "POST", "PUT", "PATCH", "DELETE"])
def proxy(path):
    """
    Proxy all requests to the CGI handler using thread-local context.
    Fully thread-safe: no shared os.environ or sys.stdout mutations.
    """
    request_id = request.headers.get("X-Request-Id") or str(uuid.uuid4())
    g.request_id = request_id

    path_info = f"/{path}" if path else ""
    query_string = request.query_string.decode("utf-8")
    body_bytes = request.get_data() if request.method in ("POST", "PUT", "PATCH") else b""
    body = body_bytes.decode("utf-8", errors="replace") if body_bytes else ""

    # Trust Railway's edge X-Forwarded-For header for the real client IP.
    forwarded = request.headers.get("X-Forwarded-For", "")
    if forwarded:
        client_ip = forwarded.split(",")[0].strip()
    else:
        client_ip = request.remote_addr or "127.0.0.1"

    # ── Set thread-local request context (read by api_core.py) ──
    ctx = api_module._request_ctx
    for attr in ('body_cache', 'raw_body'):
        if hasattr(ctx, attr):
            delattr(ctx, attr)
    ctx.request_method = request.method
    ctx.path_info = path_info
    ctx.query_string = query_string
    ctx.content_type = request.content_type or ""
    ctx.content_length = str(len(body_bytes)) if body_bytes else "0"
    ctx.remote_addr = client_ip
    ctx.http_authorization = request.headers.get("Authorization", "")
    ctx.http_x_api_key = request.headers.get("X-API-Key", "")
    ctx.http_stripe_signature = request.headers.get("Stripe-Signature", "")
    ctx.stdin_data = body
    ctx.stdin_data_raw = body_bytes  # Raw bytes for Stripe webhook signature verification

    # ── Capture CGI stdout output for this thread ──
    _tls.captured = io.StringIO()
    try:
        api_module.handle_request()
    except Exception:
        log.exception(json.dumps({
            "event": "handle_request_exception",
            "request_id": request_id,
            "method": request.method,
            "path": path_info,
        }))
        return Response(
            json.dumps({"error": "Internal server error", "request_id": request_id}),
            status=500,
            content_type="application/json",
            headers={"X-Request-Id": request_id},
        )
    finally:
        output = _tls.captured.getvalue()
        _tls.captured = None

    if not output or not output.strip():
        log.warning(json.dumps({
            "event": "empty_handler_output",
            "request_id": request_id,
            "path": path_info,
        }))
        return Response(
            json.dumps({"error": "Empty response from server", "request_id": request_id}),
            status=500,
            content_type="application/json",
            headers={"X-Request-Id": request_id},
        )

    # ── Parse CGI output (headers + body) ──
    status_code = 200
    content_type = "application/json"
    response_body = output

    if "\n\n" in output:
        header_section, response_body = output.split("\n\n", 1)
        for line in header_section.split("\n"):
            if line.startswith("Status:"):
                try:
                    status_code = int(line.split(":")[1].strip())
                except ValueError:
                    pass
            elif line.startswith("Content-Type:"):
                content_type = line.split(":", 1)[1].strip()
    elif output.strip().startswith("{") or output.strip().startswith("["):
        response_body = output.strip()
    else:
        lines = output.split("\n")
        body_start = 0
        for i, line in enumerate(lines):
            if line.startswith("Status:"):
                try:
                    status_code = int(line.split(":")[1].strip())
                except ValueError:
                    pass
                body_start = i + 1
            elif line.startswith("Content-Type:"):
                content_type = line.split(":", 1)[1].strip()
                body_start = i + 1
            elif line.strip() == "":
                body_start = i + 1
                break
            else:
                break
        response_body = "\n".join(lines[body_start:])

    # ── Guard: ensure response body is valid JSON when content type is JSON ──
    if "json" in content_type and response_body.strip():
        try:
            json.loads(response_body)
        except (json.JSONDecodeError, ValueError):
            log.error(json.dumps({
                "event": "invalid_json_response",
                "request_id": request_id,
                "path": path_info,
            }))
            return Response(
                json.dumps({"error": "Server produced invalid response", "request_id": request_id}),
                status=500,
                content_type="application/json",
                headers={"X-Request-Id": request_id},
            )
    elif "json" in content_type and not response_body.strip():
        return Response(
            json.dumps({"error": "Empty response from server", "request_id": request_id}),
            status=500,
            content_type="application/json",
            headers={"X-Request-Id": request_id},
        )

    return Response(
        response_body,
        status=status_code,
        content_type=content_type,
        headers={"X-Request-Id": request_id},
    )


@app.errorhandler(404)
def _not_found(_):
    return Response(
        json.dumps({"error": "Not found"}),
        status=404,
        content_type="application/json",
    )


@app.errorhandler(405)
def _method_not_allowed(_):
    return Response(
        json.dumps({"error": "Method not allowed"}),
        status=405,
        content_type="application/json",
    )


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    debug = os.environ.get("FLASK_DEBUG", "false").lower() == "true"
    app.run(host="0.0.0.0", port=port, debug=debug)
