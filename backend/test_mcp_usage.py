"""MCP usage counting: daily, anonymous counters for calls made by the GoHireHumans MCP server."""
import contextlib
import io
import json
import os
import sqlite3
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest import mock

from test_deep_audit_regressions import load_api_core, parse_cgi_output

ROOT = Path(__file__).resolve().parents[1]


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

    def request(self, method, path, query="", user_agent="", client="", token="", ip="203.0.113.9"):
        ctx = self.module._request_ctx
        for cached in ("body_cache", "raw_body"):
            if hasattr(ctx, cached):
                delattr(ctx, cached)
        ctx.request_method = method
        ctx.path_info = path
        ctx.query_string = query
        ctx.http_authorization = f"Bearer {token}" if token else ""
        ctx.http_x_api_key = ""
        ctx.http_user_agent = user_agent
        ctx.http_x_ghh_mcp_client = client
        ctx.stdin_data = ""
        ctx.content_type = ""
        ctx.content_length = "0"
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

    def test_counts_mcp_calls_by_version_client_and_normalized_endpoint(self):
        ua = "gohirehumans-mcp/2.2.0"
        self.request("GET", "/api/v1/categories", user_agent=ua, client="claude-ai")
        self.request("GET", "/api/v1/categories", user_agent=ua, client="claude-ai")
        self.request("GET", "/api/v1/services/4242", user_agent=ua, client="Cursor")
        self.request("GET", "/api/v1/services", query="search=secret+project&category=testing",
                     user_agent=ua, client="claude-ai")
        rows = {(r["endpoint"], r["client"]): r for r in self.usage_rows()}
        self.assertEqual(rows[("/categories", "claude-ai")]["request_count"], 2)
        self.assertEqual(rows[("/services/:id", "cursor")]["request_count"], 1)
        self.assertEqual(rows[("/services", "claude-ai")]["request_count"], 1)
        self.assertTrue(all(r["server_version"] == "2.2.0" for r in rows.values()))
        self.assertTrue(all(r["method"] == "GET" for r in rows.values()))

    def test_stores_no_ip_ids_query_or_identity(self):
        self.request("GET", "/api/v1/services/987654", query="search=very+private+words",
                     user_agent="gohirehumans-mcp/2.2.0", client="claude-ai", token="tok-worker",
                     ip="198.51.100.77")
        dump = json.dumps(self.usage_rows())
        for leaked in ("198.51.100.77", "987654", "private", "tok-worker", "worker@example.com"):
            self.assertNotIn(leaked, dump)
        self.assertEqual(
            sorted(self.usage_rows()[0].keys()),
            sorted(["day", "server_version", "client", "endpoint", "method", "request_count",
                    "error_count", "first_seen_at", "last_seen_at"]),
        )

    def test_ignores_requests_without_the_mcp_user_agent(self):
        for ua in ("", "Mozilla/5.0", "curl/8.0", "python-urllib/3.11", "xgohirehumans-mcp/2.2.0",
                   "gohirehumans-mcp/latest"):
            self.request("GET", "/api/v1/categories", user_agent=ua, client="claude-ai")
        self.assertEqual(self.usage_rows(), [])

    def test_unknown_paths_and_ids_never_create_unbounded_endpoint_labels(self):
        ua = "gohirehumans-mcp/2.2.0"
        for path in ("/api/v1/nope/1", "/api/v1/nope/2", "/api/v1/services/1/secret", "/api/v1/admin/users"):
            self.request("GET", path, user_agent=ua, client="x")
        endpoints = {r["endpoint"] for r in self.usage_rows()}
        self.assertEqual(endpoints, {"other"})

    def test_client_label_is_sanitized_and_bounded(self):
        self.request("GET", "/api/v1/categories", user_agent="gohirehumans-mcp/2.2.0",
                     client="  Évil<script>alert(1)</script> " + "x" * 200)
        self.request("GET", "/api/v1/categories", user_agent="gohirehumans-mcp/2.2.0", client="")
        clients = {r["client"] for r in self.usage_rows()}
        self.assertIn("unknown", clients)
        other = (clients - {"unknown"}).pop()
        self.assertLessEqual(len(other), 40)
        self.assertNotIn("<", other)
        self.assertRegex(other, r"^[a-z0-9 ._/-]+$")

    def test_daily_row_cap_folds_new_clients_into_other(self):
        self.module.MCP_USAGE_MAX_ROWS_PER_DAY = 3
        for i in range(6):
            self.request("GET", "/api/v1/categories", user_agent="gohirehumans-mcp/2.2.0", client=f"client{i}")
        rows = self.usage_rows()
        self.assertLessEqual(len(rows), 4)
        self.assertEqual(sum(r["request_count"] for r in rows), 6)
        self.assertIn("other", {r["client"] for r in rows})

    def test_errors_are_counted(self):
        self.request("GET", "/api/v1/services/999999", user_agent="gohirehumans-mcp/2.2.0", client="c")
        row = self.usage_rows()[0]
        self.assertEqual(row["request_count"], 1)
        self.assertEqual(row["error_count"], 1)

    def test_counting_failure_never_changes_the_response(self):
        baseline = self.request("GET", "/api/v1/categories")
        real_connect = sqlite3.connect

        def fail_for_counter(path, *args, **kwargs):
            if kwargs.get("timeout") == 2:
                raise sqlite3.OperationalError("database is locked")
            return real_connect(path, *args, **kwargs)

        with mock.patch.object(self.module.sqlite3, "connect", side_effect=fail_for_counter):
            with contextlib.redirect_stderr(io.StringIO()) as err:
                result = self.request("GET", "/api/v1/categories", user_agent="gohirehumans-mcp/2.2.0", client="c")
        self.assertEqual(result, baseline)
        self.assertIn("MCP usage counting skipped", err.getvalue())
        self.assertEqual(self.usage_rows(), [])

    def test_admin_report_requires_admin(self):
        self.assertEqual(self.request("GET", "/api/v1/admin/mcp-usage")[0], 403)
        self.assertEqual(self.request("GET", "/api/v1/admin/mcp-usage", token="tok-worker")[0], 403)
        status, body = self.request("GET", "/api/v1/admin/mcp-usage", query="days=abc", token="tok-admin")
        self.assertEqual(status, 400, body)

    def test_admin_report_separates_internal_checks(self):
        ua = "gohirehumans-mcp/2.2.0"
        self.request("GET", "/api/v1/categories", user_agent=ua, client="claude-ai")
        self.request("GET", "/api/v1/services", user_agent=ua, client="claude-ai")
        self.request("GET", "/api/v1/categories", user_agent=ua, client="ghh-probe")
        status, body = self.request("GET", "/api/v1/admin/mcp-usage", query="days=7", token="tok-admin")
        self.assertEqual(status, 200, body)
        self.assertEqual(body["days"], 7)
        self.assertEqual(body["external_totals"]["requests"], 2)
        self.assertEqual(body["external_totals"]["active_days"], 1)
        self.assertEqual({c["client"]: c["requests"] for c in body["by_client"]},
                         {"claude-ai": 2, "ghh-probe": 1})
        self.assertEqual(body["by_version"], [{"server_version": "2.2.0", "requests": 2}])
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        self.assertEqual(body["by_day"], [{"day": today, "requests": 2, "errors": 0}])

    def test_admin_report_is_not_itself_counted_as_mcp_usage(self):
        self.request("GET", "/api/v1/admin/mcp-usage", token="tok-admin")
        self.assertEqual(self.usage_rows(), [])


class McpServerIdentifiesItselfTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        import importlib.util
        from typing import Any
        spec = importlib.util.spec_from_file_location("mcp_server_usage_under_test", ROOT / "backend/mcp_server.py")
        assert spec is not None and spec.loader is not None
        module: Any = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        cls.mcp = module

    def setUp(self):
        self.mcp.CLIENT_NAME = ""
        self.mcp.API_KEY = ""
        self.mcp.AUTH_TOKEN = ""

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
        api = load_api_core()
        self.assertTrue(api.MCP_USER_AGENT_RE.match(headers["user-agent"]))

    def test_initialize_records_client_name_for_later_calls(self):
        self.mcp.handle_message({"jsonrpc": "2.0", "id": 1, "method": "initialize",
                                 "params": {"protocolVersion": "2024-11-05", "capabilities": {},
                                            "clientInfo": {"name": "claude-ai\r\nX-Evil: 1", "version": "1"}}})
        headers = self.captured_headers()
        self.assertEqual(headers["x-ghh-mcp-client"], "claude-aiX-Evil 1")
        self.assertNotIn("\n", headers["x-ghh-mcp-client"])

    def test_initialize_without_client_info_still_works(self):
        response = self.mcp.handle_message({"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {}})
        self.assertEqual(response["result"]["serverInfo"]["version"], self.mcp.SERVER_VERSION)
        self.assertNotIn("x-ghh-mcp-client", self.captured_headers())

    def test_no_new_identifying_data_is_sent(self):
        self.mcp.handle_message({"jsonrpc": "2.0", "id": 1, "method": "initialize",
                                 "params": {"clientInfo": {"name": "cursor", "version": "9.9.9"}}})
        self.assertEqual(set(self.captured_headers()), {"content-type", "user-agent", "x-ghh-mcp-client"})


if __name__ == "__main__":
    unittest.main()
