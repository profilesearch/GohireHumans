#!/usr/bin/env python3
"""
GoHireHumans API — Production Server (Flask)
Wraps the CGI-based API for production deployment on Railway/Render/Fly.io.
"""

import os
import sys
import json
import io
import threading
import importlib.util
from flask import Flask, request, Response
from flask_cors import CORS

# ─── Thread-safe stdout capture ───────────────────────────────────────────────
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

# ─── Flask App ────────────────────────────────────────────────────────────────

app = Flask(__name__)
allowed_origins = os.environ.get("ALLOWED_ORIGINS", "*").split(",")
CORS(app, resources={r"/*": {"origins": allowed_origins}}, supports_credentials=True)

# ─── Import the CGI API module ────────────────────────────────────────────────
spec = importlib.util.spec_from_file_location("api_module", os.path.join(os.path.dirname(__file__), "api_core.py"))
api_module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(api_module)


@app.route("/health", methods=["GET"])
def health():
    return {"status": "ok", "service": "gohirehumans-api"}


@app.route("/api/v1/<path:subpath>", methods=["GET", "POST", "PUT", "PATCH", "DELETE"])
def api_v1_proxy(subpath):
    """Strip /api/v1 prefix and forward to the main handler (fixes Stripe webhook URL mismatch)."""
    return proxy(subpath)


@app.route("/", defaults={"path": ""}, methods=["GET", "POST", "PUT", "PATCH", "DELETE"])
@app.route("/<path:path>", methods=["GET", "POST", "PUT", "PATCH", "DELETE"])
def proxy(path):
    """
    Proxy all requests to the CGI handler using thread-local context.
    Fully thread-safe: no shared os.environ or sys.stdout mutations.
    """
    path_info = f"/{path}" if path else ""
    query_string = request.query_string.decode("utf-8")
    body_bytes = request.get_data() if request.method in ("POST", "PUT", "PATCH") else b""
    body = body_bytes.decode("utf-8") if body_bytes else ""

    # ── Set thread-local request context (read by api_core.py) ──
    ctx = api_module._request_ctx

    # Clear per-request caches first (before setting new values)
    for attr in ('body_cache', 'raw_body'):
        if hasattr(ctx, attr):
            delattr(ctx, attr)

    ctx.request_method = request.method
    ctx.path_info = path_info
    ctx.query_string = query_string
    ctx.content_type = request.content_type or ""
    ctx.content_length = str(len(body_bytes)) if body_bytes else "0"
    ctx.remote_addr = request.remote_addr or "127.0.0.1"
    ctx.http_authorization = request.headers.get("Authorization", "")
    ctx.http_x_api_key = request.headers.get("X-API-Key", "")
    ctx.http_stripe_signature = request.headers.get("Stripe-Signature", "")
    ctx.stdin_data = body
    ctx.stdin_data_raw = body_bytes  # Raw bytes needed for Stripe webhook signature verification

    # ── Capture CGI stdout output for this thread ──
    _tls.captured = io.StringIO()

    try:
        api_module.handle_request()
    except Exception:
        import traceback
        print(f"ERROR in handle_request: {traceback.format_exc()}", file=_real_stdout)
        return Response(
            json.dumps({"error": "Internal server error"}),
            status=500,
            content_type="application/json"
        )
    finally:
        output = _tls.captured.getvalue()
        _tls.captured = None  # Stop capturing for this thread

    # ── Guard: empty output means handler produced nothing ──
    if not output or not output.strip():
        return Response(
            json.dumps({"error": "Empty response from server"}),
            status=500,
            content_type="application/json"
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
            return Response(
                json.dumps({"error": "Server produced invalid response"}),
                status=500,
                content_type="application/json"
            )
    elif "json" in content_type and not response_body.strip():
        return Response(
            json.dumps({"error": "Empty response from server"}),
            status=500,
            content_type="application/json"
        )

    return Response(response_body, status=status_code, content_type=content_type)


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    debug = os.environ.get("FLASK_DEBUG", "false").lower() == "true"
    app.run(host="0.0.0.0", port=port, debug=debug)
