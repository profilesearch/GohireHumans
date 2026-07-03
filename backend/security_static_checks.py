#!/usr/bin/env python3
"""Static security regression checks for GoHireHumans.

These checks are intentionally narrow and deterministic: they enforce the
security invariants added during hardening without trying to replace a full SAST
scanner.
"""
from pathlib import Path
import re
import sys

ROOT = Path(__file__).resolve().parents[1]
failures: list[str] = []

def require(condition: bool, message: str) -> None:
    if not condition:
        failures.append(message)

frontend = (ROOT / "frontend" / "index.html").read_text(encoding="utf-8", errors="ignore")
backend = (ROOT / "backend" / "api_core.py").read_text(encoding="utf-8", errors="ignore")
server = (ROOT / "backend" / "server.py").read_text(encoding="utf-8", errors="ignore")
trust = (ROOT / "frontend" / "trust-safety.html").read_text(encoding="utf-8", errors="ignore")

# Session/token storage: tokens should be scoped to the browser session, not
# persisted in localStorage. localStorage user profile cache is acceptable.
require("sessionStorage.setItem('ghh_token'" in frontend, "auth token should be written to sessionStorage")
require("localStorage.setItem('ghh_token'" not in frontend, "auth token must not be persisted to localStorage")
require("localStorage.removeItem('ghh_token'" in frontend, "legacy localStorage token cleanup should remain")

# XSS hardening: common modal helper must escape text by default and only render
# reviewed static HTML when allowHtml=true is explicitly passed.
require("function esc(s) { return s == null ? '' : String(s).replace(/&/g,'&amp;')" in frontend, "esc() must escape ampersand before angle brackets and quotes")
require("allowHtml = false" in frontend, "showModal should default to text rendering")
require("modalBody.textContent = message || ''" in frontend, "showModal must use textContent by default")

# Admin step-up: UI should pass admin_password for sensitive status changes and
# backend audit should redact sensitive fields broadly.
require("admin_password: adminPassword" in frontend, "admin status changes should send admin step-up password")
require("SENSITIVE_AUDIT_KEYS" in backend and "redact_audit_details" in backend, "audit details should use a sensitive-key redactor")
require("'admin_password'" in backend and "'[REDACTED]'" in backend, "admin_password must be redacted from audits")

# API security headers should remain enforced.
for header in [
    "Strict-Transport-Security",
    "X-Content-Type-Options",
    "X-Frame-Options",
    "Referrer-Policy",
    "Permissions-Policy",
    "Cache-Control",
]:
    require(header in server, f"missing API security header {header}")

# Trust/safety discoverability and content.
require('/trust-safety.html' in frontend, "Trust & Safety should be linked from public nav/footer")
for phrase in ["Stripe-Powered Processing", "Payment Review", "Issue Review", "off-platform", "dispute"]:
    require(phrase.lower() in trust.lower(), f"Trust & Safety page should cover {phrase}")

# Added-line style secret scan approximation on source files.
secret_assignment = re.compile(r"(?i)(api[_-]?key|secret|password|token|passwd)\s*=\s*['\"][^'\"]{8,}['\"]")
for rel in ["frontend/index.html", "backend/api_core.py", "backend/server.py"]:
    text = (ROOT / rel).read_text(encoding="utf-8", errors="ignore")
    for idx, line in enumerate(text.splitlines(), start=1):
        if secret_assignment.search(line) and "os.environ" not in line and "placeholder" not in line and "adminPassword" not in line:
            failures.append(f"possible hardcoded secret in {rel}:{idx}")

if failures:
    print("SECURITY STATIC CHECKS FAILED")
    for failure in failures:
        print(f"- {failure}")
    sys.exit(1)

print("SECURITY STATIC CHECKS OK")
