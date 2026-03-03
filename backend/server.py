#!/usr/bin/env python3
"""
GoHireHumans API — Production Server (Flask)
Wraps the CGI-based API for production deployment on Railway/Render/Fly.io.
"""

import os
import sys
import json
import io
import importlib.util
from flask import Flask, request, Response
from flask_cors import CORS

app = Flask(__name__)
CORS(app, resources={r"/api/*": {"origins": "*"}, r"/auth/*": {"origins": "*"}, r"/profile": {"origins": "*"}, r"/seed": {"origins": "*"}})

# ─── Import the CGI API module ────────────────────────────────────────────────
# We load api.py as a module so we can call its functions directly
spec = importlib.util.spec_from_file_location("api_module", os.path.join(os.path.dirname(__file__), "api_core.py"))
api_module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(api_module)


@app.route("/health", methods=["GET"])
def health():
    return {"status": "ok", "service": "gohirehumans-api"}


@app.route("/", defaults={"path": ""}, methods=["GET", "POST", "PUT", "PATCH", "DELETE"])
@app.route("/<path:path>", methods=["GET", "POST", "PUT", "PATCH", "DELETE"])
def proxy(path):
    """
    Proxy all requests to the CGI handler by simulating the CGI environment.
    """
    path_info = f"/{path}" if path else ""

    # Build query string
    query_string = request.query_string.decode("utf-8")

    # Get request body
    body = request.get_data(as_text=True) if request.method in ("POST", "PUT", "PATCH") else ""

    # Set CGI environment variables
    os.environ["REQUEST_METHOD"] = request.method
    os.environ["PATH_INFO"] = path_info
    os.environ["QUERY_STRING"] = query_string
    os.environ["CONTENT_TYPE"] = request.content_type or ""
    os.environ["CONTENT_LENGTH"] = str(len(body.encode("utf-8"))) if body else "0"
    os.environ["REMOTE_ADDR"] = request.remote_addr or "127.0.0.1"

    # Redirect stdin to provide the body
    old_stdin = sys.stdin
    sys.stdin = io.StringIO(body)

    # Capture stdout
    old_stdout = sys.stdout
    sys.stdout = captured = io.StringIO()

    try:
        api_module.handle_request()
    except Exception as e:
        sys.stdout = old_stdout
        sys.stdin = old_stdin
        return Response(
            json.dumps({"error": str(e)}),
            status=422,
            content_type="application/json"
        )

    sys.stdout = old_stdout
    sys.stdin = old_stdin

    output = captured.getvalue()

    # Parse CGI output (headers + body)
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
        # Try to find headers without double newline (single \n separation)
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

    return Response(response_body, status=status_code, content_type=content_type)


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    debug = os.environ.get("FLASK_DEBUG", "false").lower() == "true"
    app.run(host="0.0.0.0", port=port, debug=debug)
