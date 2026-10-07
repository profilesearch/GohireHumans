"""MCP usage counting: daily, anonymous counters for calls made by the GoHireHumans MCP server."""
import contextlib
import http.client
import io
import json
import os
import socket
import sqlite3
import tempfile
import threading
import time
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from unittest import mock

from test_deep_audit_regressions import load_api_core, parse_cgi_output

ROOT = Path(__file__).resolve().parents[1]
UA = "gohirehumans-mcp/2.2.0"


def load_mcp_server() -> Any:
    import importlib.util
    spec = importlib.util.spec_from_file_location("mcp_server_usage_under_test", ROOT / "backend/mcp_server.py")
    assert spec is not None and spec.loader is not None
    module: Any = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class McpUsageCountingTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        os.environ["DATABASE_PATH"] = str(Path(self.tmp.name) / "test.db")
        os.environ["DISABLE_AUTO_SEED"] = "1"
        self.module = load_api_core()
        self.module._db_path_resolved = None
        self.module._seeded = False
        self.module.init_db()
        db = self.module.get_db()
        try:
            db.execute("INSERT INTO users (id,email,password_hash,name,is_admin) VALUES (1,'admin@example.com','x','Admin',1)")
            db.execute("INSERT INTO users (id,email,password_hash,name) VALUES (2,'worker@example.com','x','Worker')")
            db.execute("INSERT INTO sessions (user_id,token,expires_at) VALUES (1,'tok-admin',datetime('now','+1 day'))")
            db.execute("INSERT INTO sessions (user_id,token,expires_at) VALUES (2,'tok-worker',datetime('now','+1 day'))")
            db.commit()
        finally:
            db.close()

    def tearDown(self):
        self.tmp.cleanup()
        os.environ.pop("DATABASE_PATH", None)
        os.environ.pop("DISABLE_AUTO_SEED", None)

    def request(self, method, path, query="", user_agent="", client="", token="", ip="203.0.113.9",
                api_key="", body=""):
        ctx = self.module._request_ctx
        for cached in ("body_cache", "raw_body"):
            if hasattr(ctx, cached):
                delattr(ctx, cached)
        ctx.request_method = method
        ctx.path_info = path
        ctx.query_string = query
        ctx.http_authorization = f"Bearer {token}" if token else ""
        ctx.http_x_api_key = api_key
        ctx.http_user_agent = user_agent
        ctx.http_x_ghh_mcp_client = client
        ctx.stdin_data = body
        ctx.content_type = "application/json" if body else ""
        ctx.content_length = str(len(body))
        ctx.remote_addr = ip
        with contextlib.redirect_stdout(io.StringIO()) as out:
            self.module.handle_request()
        return parse_cgi_output(out.getvalue())

    def usage_rows(self):
        db = sqlite3.connect(os.environ["DATABASE_PATH"])
        db.row_factory = sqlite3.Row
        try:
            return [dict(r) for r in db.execute("SELECT * FROM mcp_usage_daily ORDER BY endpoint, client")]
        finally:
            db.close()

    def summary(self, days):
        db = self.module.get_db()
        try:
            return self.module.mcp_usage_summary(db, days)
        finally:
            db.close()

    def record(self, version="2.2.0", client="claude", method="GET", path="/categories", status=200, now=None):
        ctx = self.module._request_ctx
        ctx.http_user_agent = f"gohirehumans-mcp/{version}"
        ctx.http_x_ghh_mcp_client = client
        ctx.request_method = method
        ctx.path_info = path
        ctx.response_status = status
        return self.module.record_mcp_usage(now=now)

    # ── what gets counted ──────────────────────────────────────────────────

    def test_counts_mcp_calls_by_version_client_and_normalized_endpoint(self):
        self.request("GET", "/api/v1/categories", user_agent=UA, client="claude")
        self.request("GET", "/api/v1/categories", user_agent=UA, client="claude")
        self.request("GET", "/api/v1/services/4242", user_agent=UA, client="Cursor")
        self.request("GET", "/api/v1/services", query="search=secret+project&category=testing",
                     user_agent=UA, client="claude")
        rows = {(r["endpoint"], r["client"]): r for r in self.usage_rows()}
        self.assertEqual(rows[("/categories", "claude")]["request_count"], 2)
        self.assertEqual(rows[("/services/:id", "cursor")]["request_count"], 1)
        self.assertEqual(rows[("/services", "claude")]["request_count"], 1)
        self.assertTrue(all(r["server_version"] == "2.2.0" for r in rows.values()))
        self.assertTrue(all(r["method"] == "GET" for r in rows.values()))

    def test_every_mcp_endpoint_family_normalizes_exactly(self):
        cases = [
            ("GET", "/services", "/services"), ("GET", "/services/7", "/services/:id"),
            ("POST", "/services/7/order", "/services/:id/order"), ("GET", "/categories", "/categories"),
            ("GET", "/pricing/info", "/pricing/info"), ("GET", "/jobs", "/jobs"), ("POST", "/jobs", "/jobs"),
            ("GET", "/jobs/7", "/jobs/:id"), ("GET", "/orders/7", "/orders/:id"),
            ("POST", "/orders/7/approve", "/orders/:id/approve"),
            ("POST", "/orders/7/review", "/orders/:id/review"),
        ]
        for method, path, label in cases:
            for prefix in ("", "/api/v1"):
                self.assertEqual(self.module._mcp_usage_endpoint(method, prefix + path), label, (method, path))
        for method, path in (("PUT", "/orders/7/approve"), ("GET", "/orders/7/approve"),
                             ("DELETE", "/jobs/7"), ("POST", "/categories")):
            self.assertEqual(self.module._mcp_usage_endpoint(method, path), "other", (method, path))

    def test_ignores_requests_without_the_mcp_user_agent(self):
        for ua in ("", "Mozilla/5.0", "curl/8.0", "python-urllib/3.11", "xgohirehumans-mcp/2.2.0",
                   "gohirehumans-mcp/latest"):
            self.request("GET", "/api/v1/categories", user_agent=ua, client="claude")
        self.assertEqual(self.usage_rows(), [])

    def test_errors_are_counted(self):
        self.request("GET", "/api/v1/services/999999", user_agent=UA, client="claude")
        row = self.usage_rows()[0]
        self.assertEqual(row["request_count"], 1)
        self.assertEqual(row["error_count"], 1)

    # ── privacy ────────────────────────────────────────────────────────────

    def test_stores_no_ip_ids_query_key_body_or_identity(self):
        self.request("POST", "/api/v1/jobs", query="search=very+private+words",
                     user_agent=UA, client="claude", token="tok-worker", ip="198.51.100.77",
                     api_key="ghh_live_SECRETKEYVALUE", body='{"title": "confidential body text"}')
        self.request("GET", "/api/v1/services/987654", user_agent=UA, client="claude", ip="198.51.100.77")
        dump = json.dumps(self.usage_rows())
        for leaked in ("198.51.100.77", "987654", "private", "tok-worker", "worker@example.com",
                       "SECRETKEYVALUE", "confidential", "title"):
            self.assertNotIn(leaked, dump)
        endpoint_labels = {label for _, _, label in self.module.MCP_USAGE_ENDPOINTS} | {"other"}
        for row in self.usage_rows():
            self.assertIn(row["client"], self.module.MCP_USAGE_CLIENT_LABELS)
            self.assertIn(row["endpoint"], endpoint_labels)
            self.assertIn(row["method"], set(self.module.MCP_USAGE_METHODS) | {"OTHER"})
            self.assertRegex(row["server_version"], r"^(\d{1,3}\.\d{1,3}\.\d{1,3}|other)$")
            self.assertRegex(row["day"], r"^\d{4}-\d{2}-\d{2}$")
        self.assertEqual(
            sorted(self.usage_rows()[0].keys()),
            sorted(["day", "server_version", "client", "endpoint", "method", "request_count",
                    "error_count", "first_seen_at", "last_seen_at"]),
        )

    def test_client_header_maps_to_a_fixed_label_set(self):
        sensitive = ["198.51.100.77", "account/987654", "tok-worker", "ghh_live_abcdef0123456789",
                     "private search terms", "Bob laptop", "Évil<script>", "x" * 500, "unknown2"]
        for name in sensitive:
            self.request("GET", "/api/v1/categories", user_agent=UA, client=name)
        self.request("GET", "/api/v1/categories", user_agent=UA, client="")
        self.request("GET", "/api/v1/categories", user_agent=UA, client="ghh-internal")
        clients = {r["client"]: r["request_count"] for r in self.usage_rows()}
        self.assertEqual(clients, {"other": len(sensitive), "unknown": 1, "ghh-internal": 1})
        self.assertTrue(set(clients) <= self.module.MCP_USAGE_CLIENT_LABELS)

    def test_unknown_paths_and_ids_never_create_unbounded_endpoint_labels(self):
        for path in ("/api/v1/nope/1", "/api/v1/nope/2", "/api/v1/services/1/secret", "/api/v1/admin/users"):
            self.request("GET", path, user_agent=UA, client="claude")
        self.assertEqual({r["endpoint"] for r in self.usage_rows()}, {"other"})

    # ── bounded storage ────────────────────────────────────────────────────

    def test_daily_cap_holds_against_distinct_versions(self):
        self.module.MCP_USAGE_MAX_ROWS_PER_DAY = 5
        for i in range(40):
            self.assertTrue(self.record(version=f"{i // 10}.{i % 10}.{i}"))
        rows = self.usage_rows()
        self.assertEqual(len(rows), 6)
        self.assertEqual(sum(r["request_count"] for r in rows), 40)
        overflow = [r for r in rows if r["server_version"] == "other"]
        self.assertEqual(len(overflow), 1)
        self.assertEqual(overflow[0]["client"], "other")
        self.assertEqual(overflow[0]["request_count"], 35)

    def test_daily_cap_is_exact(self):
        self.module.MCP_USAGE_MAX_ROWS_PER_DAY = 3
        for i in range(3):
            self.record(version=f"1.0.{i}")
        self.assertEqual(len(self.usage_rows()), 3)
        self.assertNotIn("other", {r["server_version"] for r in self.usage_rows()})
        self.record(version="1.0.9")
        self.assertIn("other", {r["server_version"] for r in self.usage_rows()})
        self.record(version="1.0.0")  # an existing key keeps counting in place
        first = [r for r in self.usage_rows() if r["server_version"] == "1.0.0"][0]
        self.assertEqual(first["request_count"], 2)

    def test_overflow_bucket_is_bounded_across_endpoints_and_methods(self):
        self.module.MCP_USAGE_MAX_ROWS_PER_DAY = 2
        self.record(version="1.0.0")
        self.record(version="1.0.1")
        for method in ("GET", "POST", "PUT", "PATCH", "DELETE", "PROPFIND", "X" * 50):
            for path in ("/services", "/jobs", "/nope/1", "/orders/3/approve"):
                for v in range(3):
                    self.record(version=f"9.9.{v}", method=method, path=path)
        rows = self.usage_rows()
        endpoints = len({label for _, _, label in self.module.MCP_USAGE_ENDPOINTS}) + 1
        methods = len(self.module.MCP_USAGE_METHODS) + 1
        self.assertLessEqual(len(rows), 2 + endpoints * methods)
        self.assertTrue({r["method"] for r in rows} <= set(self.module.MCP_USAGE_METHODS) | {"OTHER"})
        self.assertEqual({r["server_version"] for r in rows}, {"1.0.0", "1.0.1", "other"})

    def test_daily_cap_admission_is_atomic_across_threads(self):
        self.module.MCP_USAGE_MAX_ROWS_PER_DAY = 3
        self.module.MCP_USAGE_BUSY_TIMEOUT_SECONDS = 5
        self.record(version="1.0.0")
        self.record(version="1.0.1")
        start = threading.Barrier(8)
        results = []

        def worker(i):
            ctx = self.module._request_ctx
            ctx.http_user_agent = f"gohirehumans-mcp/2.0.{i}"
            ctx.http_x_ghh_mcp_client = "claude"
            ctx.request_method = "GET"
            ctx.path_info = "/categories"
            ctx.response_status = 200
            start.wait()
            results.append(self.module.record_mcp_usage())

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        self.assertEqual(results, [True] * 8)
        named = [r for r in self.usage_rows() if r["server_version"] != "other"]
        self.assertEqual(len(named), 3)
        self.assertEqual(sum(r["request_count"] for r in self.usage_rows()), 10)

    def test_overflow_keeps_internal_checks_out_of_external_totals(self):
        self.module.MCP_USAGE_MAX_ROWS_PER_DAY = 2
        for i in range(6):
            self.record(version=f"1.0.{i}", client="ghh-internal")
        self.assertEqual(self.summary(1)["external_totals"]["requests"], 0)
        for i in range(3):
            self.record(version=f"3.0.{i}", client="claude")
        self.assertEqual(self.summary(1)["external_totals"]["requests"], 3)

    # ── never harms the request ────────────────────────────────────────────

    def test_counting_failure_never_changes_the_response(self):
        baseline = self.request("GET", "/api/v1/categories")
        real_connect = sqlite3.connect

        def fail_for_counter(path, *args, **kwargs):
            if kwargs.get("timeout") == self.module.MCP_USAGE_BUSY_TIMEOUT_SECONDS:
                raise sqlite3.OperationalError("database is locked")
            return real_connect(path, *args, **kwargs)

        with mock.patch.object(self.module.sqlite3, "connect", side_effect=fail_for_counter):
            with contextlib.redirect_stderr(io.StringIO()) as err:
                result = self.request("GET", "/api/v1/categories", user_agent=UA, client="claude")
        self.assertEqual(result, baseline)
        self.assertIn("MCP usage counting skipped", err.getvalue())
        self.assertEqual(self.usage_rows(), [])

    def test_counting_and_logging_failure_together_never_raise(self):
        class BrokenStderr:
            def write(self, *_):
                raise OSError("stderr closed")

            def flush(self):
                raise OSError("stderr closed")

        with mock.patch.object(self.module.sqlite3, "connect", side_effect=sqlite3.OperationalError("locked")):
            with mock.patch.object(self.module.sys, "stderr", BrokenStderr()):
                self.assertFalse(self.record())

    def test_held_write_lock_drops_the_count_without_delaying_the_response(self):
        baseline = self.request("GET", "/api/v1/categories")
        blocker = sqlite3.connect(os.environ["DATABASE_PATH"], isolation_level=None)
        try:
            blocker.execute("BEGIN IMMEDIATE")
            started = time.monotonic()
            with contextlib.redirect_stderr(io.StringIO()) as err:
                result = self.request("GET", "/api/v1/categories", user_agent=UA, client="claude")
            elapsed = time.monotonic() - started
        finally:
            blocker.execute("ROLLBACK")
            blocker.close()
        self.assertEqual(result, baseline)
        self.assertIn("MCP usage counting skipped", err.getvalue())
        self.assertLess(elapsed, 0.5)
        self.assertEqual(self.usage_rows(), [])

    def test_recorder_runs_after_the_response_status_is_known(self):
        self.request("GET", "/api/v1/jobs/424242", user_agent=UA, client="claude")
        self.assertEqual(self.usage_rows()[0]["error_count"], 1)

    # ── admin report ───────────────────────────────────────────────────────

    def test_admin_report_requires_admin(self):
        self.assertEqual(self.request("GET", "/api/v1/admin/mcp-usage")[0], 403)
        self.assertEqual(self.request("GET", "/api/v1/admin/mcp-usage", token="tok-worker")[0], 403)
        status, body = self.request("GET", "/api/v1/admin/mcp-usage", query="days=abc", token="tok-admin")
        self.assertEqual(status, 400, body)

    def test_admin_report_separates_internal_checks(self):
        self.request("GET", "/api/v1/categories", user_agent=UA, client="claude")
        self.request("GET", "/api/v1/services", user_agent=UA, client="claude")
        self.request("GET", "/api/v1/categories", user_agent=UA, client="ghh-internal")
        status, body = self.request("GET", "/api/v1/admin/mcp-usage", query="days=7", token="tok-admin")
        self.assertEqual(status, 200, body)
        self.assertEqual(body["days"], 7)
        self.assertEqual(body["external_totals"]["requests"], 2)
        self.assertEqual(body["external_totals"]["active_days"], 1)
        self.assertEqual({c["client"]: c["requests"] for c in body["by_client"]},
                         {"claude": 2, "ghh-internal": 1})
        self.assertEqual(body["by_version"], [{"server_version": "2.2.0", "requests": 2}])
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        self.assertEqual(body["by_day"], [{"day": today, "requests": 2, "errors": 0}])

    def test_admin_report_window_is_exactly_n_days(self):
        now = datetime.now(timezone.utc)
        for back in range(0, 5):
            self.record(now=now - timedelta(days=back))
        for days, expected in ((1, 1), (3, 3), (5, 5), (90, 5)):
            summary = self.summary(days)
            self.assertEqual(summary["external_totals"]["requests"], expected, days)
            self.assertEqual(summary["since"], (now - timedelta(days=days - 1)).strftime("%Y-%m-%d"))

    def test_admin_report_is_not_itself_counted_as_mcp_usage(self):
        self.request("GET", "/api/v1/admin/mcp-usage", token="tok-admin")
        self.assertEqual(self.usage_rows(), [])


class McpServerIdentifiesItselfTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.mcp = load_mcp_server()
        cls.api = load_api_core()

    def setUp(self):
        self.mcp.CLIENT_LABEL = ""
        self.mcp.API_KEY = ""
        self.mcp.AUTH_TOKEN = ""

    def initialize(self, params):
        return self.mcp.handle_message({"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": params})

    def captured_headers(self):
        seen = {}

        class FakeResponse:
            def __enter__(self):
                return self

            def __exit__(self, *exc):
                return False

            def read(self):
                return b"{}"

        def fake_urlopen(req, timeout=None):
            seen.update({k.lower(): v for k, v in req.header_items()})
            return FakeResponse()

        with mock.patch.object(self.mcp.urllib.request, "urlopen", fake_urlopen):
            self.mcp.api_request("GET", "/categories")
        return seen

    def test_sends_versioned_user_agent_matching_the_api_pattern(self):
        headers = self.captured_headers()
        self.assertEqual(headers["user-agent"], f"gohirehumans-mcp/{self.mcp.SERVER_VERSION}")
        self.assertNotIn("x-ghh-mcp-client", headers)
        self.assertTrue(self.api.MCP_USER_AGENT_RE.match(headers["user-agent"]))

    def test_known_clients_map_to_product_labels(self):
        cases = {
            "claude-ai": "claude", "Claude Desktop": "claude", "claude-code": "claude-code",
            "cursor-vscode": "cursor", "Visual Studio Code": "vscode", "GitHub Copilot": "vscode",
            "Roo Code": "roo-code", "Cline": "cline", "zed": "zed", "mcp-inspector": "mcp-inspector",
            "openai-mcp": "openai", "ghh-probe": "ghh-internal",
        }
        for raw, label in cases.items():
            self.assertEqual(self.mcp.client_label(raw), label, raw)

    def test_unrecognized_or_sensitive_names_are_never_sent(self):
        for raw in ("Bob laptop", "198.51.100.77", "tok-worker", "ghh_live_abcdef0123456789",
                    "account/987654", "private search terms", "测试客户端", "\r\nX-Evil: 1"):
            self.initialize({"clientInfo": {"name": raw, "version": "1"}})
            self.assertEqual(self.captured_headers()["x-ghh-mcp-client"], "other", raw)

    def test_non_string_or_missing_names_send_no_client_header(self):
        for params in ({}, {"clientInfo": None}, {"clientInfo": "claude"}, {"clientInfo": {"name": 42}},
                       {"clientInfo": {"name": ["claude"]}}, {"clientInfo": {"name": "   "}}, None):
            response = self.initialize(params)
            self.assertEqual(response["result"]["serverInfo"]["version"], self.mcp.SERVER_VERSION)
            self.assertNotIn("x-ghh-mcp-client", self.captured_headers(), params)

    def test_every_label_the_server_can_send_is_accepted_by_the_api(self):
        labels = {label for label, _ in self.mcp.CLIENT_LABEL_RULES}
        labels |= {label for _, label in self.mcp.CLIENT_LABEL_WORDS}
        labels |= {"ghh-internal", "other"}
        self.assertTrue(labels <= self.api.MCP_USAGE_CLIENT_LABELS, labels - self.api.MCP_USAGE_CLIENT_LABELS)
        self.assertEqual(self.mcp.client_label("ghh-anything"), self.api.MCP_USAGE_INTERNAL_CLIENT)
        for label in labels:
            self.assertRegex(label, r"^[a-z0-9-]+$")

    def test_no_new_identifying_data_is_sent(self):
        self.initialize({"clientInfo": {"name": "cursor", "version": "9.9.9"}})
        self.assertEqual(set(self.captured_headers()), {"content-type", "user-agent", "x-ghh-mcp-client"})

    def test_real_http_serialization_of_headers_succeeds(self):
        # Exercise the stdlib header encoder; a mocked urlopen cannot catch encoding errors.
        for raw in ("测试客户端", "Claude Desktop", "naïve-client"):
            self.initialize({"clientInfo": {"name": raw}})
            captured = {"bytes": b""}

            def fake_send(conn, data):
                captured["bytes"] += data if isinstance(data, bytes) else data.encode()
                raise ConnectionAbortedError("stop before any network")

            with mock.patch.object(http.client.HTTPConnection, "send", fake_send), \
                 mock.patch.object(socket, "create_connection", side_effect=AssertionError("no network")):
                with self.assertRaises(self.mcp.APIRequestError):
                    self.mcp.api_request("GET", "/categories")
            sent = captured["bytes"].lower()
            self.assertIn(b"\r\nuser-agent: gohirehumans-mcp/", sent)
            self.assertRegex(sent, rb"\r\nx-ghh-mcp-client: [a-z0-9-]+\r\n")


if __name__ == "__main__":
    unittest.main()
