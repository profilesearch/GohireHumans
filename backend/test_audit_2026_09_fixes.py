"""Regression tests for the September 2026 backend audit fixes.

Covers: once-per-process schema init, content-safety word boundaries, the
transfer.paid webhook, API-key/admin isolation, pagination validation, session
expiry handling, admin step-up throttling, the removed referral endpoint,
worker-profile validation, log-hours parsing, webhook-secret 503s, rate-limit
store eviction, server.py proxy/IP + header behaviour, MCP field names and the
production start script.
"""
import contextlib
import hashlib
import importlib.util
import io
import json
import os
import re
import secrets
import tempfile
import time
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest import mock

from test_deep_audit_regressions import load_api_core, parse_cgi_output
from test_diagnostics import load_server

BACKEND_DIR = Path(__file__).resolve().parent
REPO_ROOT = BACKEND_DIR.parent


class AuditFixesTestCase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.env = mock.patch.dict(os.environ, {
            "DATABASE_PATH": str(Path(self.tmp.name) / "audit.db"),
            "DISABLE_AUTO_SEED": "1",
        })
        self.env.start()
        self.addCleanup(self.env.stop)
        for key in ("RESEND_API_KEY", "GOOGLE_CLIENT_ID", "RAILWAY_ENVIRONMENT",
                    "RAILWAY_PROJECT_ID", "TRUST_X_FORWARDED_FOR", "ENVIRONMENT"):
            os.environ.pop(key, None)
        self.module = load_api_core()
        self.module._db_path_resolved = None
        self.module._seeded = False
        self.module.init_db()

    def _request_api(self, method, path, payload=None, token="", query="",
                     api_key="", raw_body=None, remote_addr="127.0.0.1", extra_ctx=None):
        body = raw_body if raw_body is not None else json.dumps(payload or {})
        ctx = self.module._request_ctx
        for cached in ("body_cache", "raw_body"):
            if hasattr(ctx, cached):
                delattr(ctx, cached)
        ctx.request_method = method
        ctx.path_info = path
        ctx.query_string = query
        ctx.http_authorization = f"Bearer {token}" if token else ""
        ctx.http_x_api_key = api_key
        ctx.http_stripe_signature = ""
        ctx.http_x_diagnostic_secret = ""
        ctx.stdin_data = body
        ctx.stdin_data_raw = body.encode("utf-8")
        ctx.content_type = "application/json"
        ctx.content_length = str(len(ctx.stdin_data_raw))
        ctx.remote_addr = remote_addr
        for attr, value in (extra_ctx or {}).items():
            setattr(ctx, attr, value)
        with contextlib.redirect_stdout(io.StringIO()) as out:
            self.module.handle_request()
        return parse_cgi_output(out.getvalue())

    def _seed_users(self, admin=False):
        db = self.module.get_db()
        try:
            db.execute("INSERT INTO users (id,email,password_hash,name) VALUES (1,'worker@example.com','x','Worker')")
            db.execute("INSERT INTO worker_profiles (user_id) VALUES (1)")
            db.execute("INSERT INTO users (id,email,password_hash,name) VALUES (2,'employer@example.com','x','Employer')")
            db.execute("INSERT INTO employer_profiles (user_id) VALUES (2)")
            if admin:
                db.execute(
                    "INSERT INTO users (id,email,password_hash,name,is_admin) VALUES (3,'admin@example.com',?,'Admin',1)",
                    [self.module.hash_password("AdminPassword123!")],
                )
            for user_id, token in ((1, "tok-worker"), (2, "tok-employer"), (3, "tok-admin")):
                if user_id == 3 and not admin:
                    continue
                db.execute(
                    "INSERT INTO sessions (user_id,token,expires_at) VALUES (?,?,datetime('now','+1 day'))",
                    [user_id, token],
                )
            db.commit()
        finally:
            db.close()


class ContentSafetyTests(AuditFixesTestCase):
    def test_ordinary_words_containing_keywords_are_allowed(self):
        allowed, _ = self.module.check_content_safety(
            "My skills include React and Agile methodology; I organized a hackathon "
            "and wrote anti-scam training"
        )
        self.assertTrue(allowed)
        for text in (
            "skills", "skillset", "methodology", "method", "hackathon", "anti-scam",
            "Don't forget the deadline", "explicitly documented", "we have begun",
            "drugstore inventory", "counter-terrorism policy research", "non-violent",
        ):
            with self.subTest(text=text):
                self.assertTrue(self.module.check_content_safety(text)[0], text)

    def test_prohibited_content_and_inflections_are_still_blocked(self):
        for text in (
            "I sell illegal weapons and drugs",
            "Looking for an escort for companionship",
            "Need someone to hack into an account",
            "Scammers wanted for a phishing campaign",
            "Pay under the table, no questions asked",
            "rent my body for the evening",
            "professional massage with happy ending",
            "money laundering consultant",
            "killer", "guns", "forgery", "terrorism", "extremist", "extortion",
            "Explicit content", "methamphetamine", "erotica writing",
        ):
            with self.subTest(text=text):
                blocked, message = self.module.check_content_safety(text)
                self.assertFalse(blocked, text)
                self.assertIn("Acceptable Use Policy", message)

    def test_non_string_input_does_not_crash(self):
        self.assertTrue(self.module.check_content_safety(None)[0])
        self.assertFalse(self.module.check_content_safety(["escort"])[0])


class SchemaInitGuardTests(AuditFixesTestCase):
    def test_schema_init_runs_once_per_database_path(self):
        # setUp already initialized this path: a request must not re-run init_db.
        with mock.patch.object(self.module, "init_db", side_effect=AssertionError("re-init")):
            status, payload = self._request_api("GET", "/categories")
        self.assertEqual(status, 200, payload)

        # A different database path (tests swap DATABASE_PATH) still gets a
        # first-request initialization, exactly once.
        os.environ["DATABASE_PATH"] = str(Path(self.tmp.name) / "second.db")
        self.module._db_path_resolved = None
        real_init = self.module.init_db
        counter = mock.Mock(wraps=real_init)
        with mock.patch.object(self.module, "init_db", counter):
            self.assertEqual(self._request_api("GET", "/categories")[0], 200)
            self.assertEqual(self._request_api("GET", "/categories")[0], 200)
        self.assertEqual(counter.call_count, 1)
        self.assertIn(str(Path(self.tmp.name) / "second.db"), self.module._INITIALIZED_DB_PATHS)


class StripeWebhookTests(AuditFixesTestCase):
    def _configure_stripe(self, event):
        class FakeWebhook:
            @staticmethod
            def construct_event(body_raw, sig_header, secret):
                return event

        self.module.PRODUCTION_MODE = True
        self.module.STRIPE_AVAILABLE = True
        self.module.STRIPE_SECRET_KEY = "sk_test_configured"
        self.module.STRIPE_WEBHOOK_SECRET = "whsec_test_configured"
        self.module.STRIPE_SIGNATURE_ERROR = Exception
        self.module.stripe = type("FakeStripe", (), {"Webhook": FakeWebhook})

    def _post_webhook(self, event):
        self._configure_stripe(event)
        return self._request_api(
            "POST", "/webhooks/stripe", raw_body=json.dumps({"id": event["id"]}),
            extra_ctx={"http_stripe_signature": "t=1,v1=test"},
        )

    def test_transfer_paid_notifies_the_order_worker_and_acknowledges_unmapped_events(self):
        self._seed_users()
        db = self.module.get_db()
        try:
            db.execute(
                "INSERT INTO orders (id,type,worker_id,employer_id,status,total_amount) "
                "VALUES (7,'job_hire',1,2,'completed',100)"
            )
            db.commit()
        finally:
            db.close()

        status, body = self._post_webhook({
            "id": "evt_transfer_1", "type": "transfer.paid",
            "data": {"object": {"id": "tr_1", "metadata": {"order_id": "7"}}},
        })
        self.assertEqual(status, 200, body)
        self.assertEqual(body, {"received": True})
        db = self.module.get_db()
        try:
            rows = db.execute(
                "SELECT user_id,type,link FROM notifications WHERE type='transfer_paid'"
            ).fetchall()
            self.assertEqual([tuple(r) for r in rows], [(1, "transfer_paid", "/orders/7")])
        finally:
            db.close()

        for event in (
            {"id": "evt_transfer_2", "type": "transfer.paid",
             "data": {"object": {"id": "tr_2", "metadata": {"order_id": "999"}}}},
            {"id": "evt_transfer_3", "type": "transfer.paid",
             "data": {"object": {"id": "tr_3", "metadata": {"order_id": "not-a-number"}}}},
            {"id": "evt_transfer_4", "type": "transfer.paid",
             "data": {"object": {"id": "tr_4"}}},
        ):
            with self.subTest(event=event["id"]):
                status, body = self._post_webhook(event)
                self.assertEqual(status, 200, body)
                self.assertEqual(body, {"received": True})
        db = self.module.get_db()
        try:
            self.assertEqual(
                db.execute("SELECT COUNT(*) FROM notifications WHERE type='transfer_paid'").fetchone()[0], 1
            )
        finally:
            db.close()

    def test_missing_webhook_secrets_return_503(self):
        self.module.PRODUCTION_MODE = True
        self.module.STRIPE_AVAILABLE = True
        self.module.STRIPE_SECRET_KEY = "sk_test_configured"
        self.module.STRIPE_WEBHOOK_SECRET = ""
        status, body = self._request_api("POST", "/webhooks/stripe", raw_body="{}")
        self.assertEqual(status, 503, body)
        self.assertIn("Webhook secret", body["error"])

        self.module.RESEND_WEBHOOK_SECRET = ""
        status, body = self._request_api("POST", "/webhooks/resend", raw_body="{}")
        self.assertEqual(status, 503, body)
        self.assertIn("Webhook secret", body["error"])


class ApiKeyTests(AuditFixesTestCase):
    def _create_key(self, user_id, expires_at=None, scopes='["read","write"]'):
        api_key = "ghh_" + secrets.token_hex(16)
        db = self.module.get_db()
        try:
            db.execute(
                "INSERT INTO api_keys (user_id,key_hash,key_prefix,name,scopes,expires_at) VALUES (?,?,?,?,?,?)",
                [user_id, hashlib.sha256(api_key.encode()).hexdigest(), api_key[:12], "test", scopes, expires_at],
            )
            db.commit()
        finally:
            db.close()
        return api_key

    def test_api_keys_never_authorize_admin_routes(self):
        for path in ("/admin", "/admin/dashboard", "/admin/users", "/admin/users/1/password"):
            self.assertIsNone(self.module._api_key_route_scope("GET", path), path)
            self.assertIsNone(self.module._api_key_route_scope("PUT", path), path)
        self.assertEqual(self.module._api_key_route_scope("GET", "/services"), "read")

        self._seed_users(admin=True)
        admin_key = self._create_key(3)
        status, body = self._request_api("GET", "/admin/dashboard", api_key=admin_key)
        self.assertEqual(status, 403, body)
        self.assertIn("scope does not permit", body["error"])
        # The same key still works for ordinary read routes.
        status, body = self._request_api("GET", "/profile", api_key=admin_key)
        self.assertEqual(status, 200, body)
        self.assertEqual(body["id"], 3)
        db = self.module.get_db()
        try:
            denied = db.execute(
                "SELECT endpoint,accounting_state,status_code FROM api_key_usage WHERE accounting_state='denied'"
            ).fetchall()
            self.assertEqual([tuple(r) for r in denied], [("/admin/dashboard", "denied", 403)])
        finally:
            db.close()

    def test_api_key_verify_uses_timezone_aware_expiry(self):
        self._seed_users()
        expired = self._create_key(1, expires_at="2000-01-01 00:00:00")
        status, body = self._request_api("POST", "/api-keys/verify", {"api_key": expired})
        self.assertEqual(status, 401, body)
        self.assertIn("expired", body["error"])

        # An aware ISO timestamp used to raise TypeError against utcnow() (500).
        valid = self._create_key(1, expires_at="2999-01-01T00:00:00+00:00")
        status, body = self._request_api("POST", "/api-keys/verify", {"api_key": valid})
        self.assertEqual(status, 200, body)
        self.assertTrue(body["valid"])
        self.assertEqual(body["user"]["id"], 1)

        self.assertTrue(self.module.api_key_expired("garbage"))
        self.assertFalse(self.module.api_key_expired(None))
        self.assertFalse(self.module.api_key_expired("2999-01-01T00:00:00Z"))
        self.assertTrue(self.module.api_key_expired("2000-01-01T00:00:00Z"))


class PaginationTests(AuditFixesTestCase):
    def test_invalid_pagination_returns_400_not_500(self):
        self._seed_users(admin=True)
        cases = [
            ("/orders", "page=abc", "tok-employer"),
            ("/orders", "per_page=-1", "tok-employer"),
            ("/orders", "per_page=0", "tok-employer"),
            ("/users/1/reviews", "page=0", ""),
            ("/users/1/reviews", "per_page=x", ""),
            ("/payments/history", "per_page=-1", "tok-worker"),
            ("/notifications", "limit=0", "tok-worker"),
            ("/notifications", "limit=abc", "tok-worker"),
            ("/admin/marketplace-ops", "limit=abc", "tok-admin"),
            ("/admin/application-pipeline", "limit=0", "tok-admin"),
            ("/admin/users", "per_page=-1", "tok-admin"),
            ("/admin/orders", "page=abc", "tok-admin"),
            ("/admin/audit-log", "limit=-1", "tok-admin"),
        ]
        for path, query, token in cases:
            with self.subTest(path=path, query=query):
                status, body = self._request_api("GET", path, token=token, query=query)
                self.assertEqual(status, 400, body)
                self.assertIn("Invalid", body["error"])

    def test_valid_pagination_is_capped_and_served(self):
        self._seed_users(admin=True)
        for path, query, token, cap in (
            ("/orders", "per_page=999", "tok-employer", 100),
            ("/admin/users", "per_page=999", "tok-admin", 200),
            ("/users/1/reviews", "page=1&per_page=500", "", 100),
        ):
            with self.subTest(path=path):
                status, body = self._request_api("GET", path, token=token, query=query)
                self.assertEqual(status, 200, body)
                if "per_page" in body:
                    self.assertEqual(body["per_page"], cap)
        for path, query, token in (
            ("/notifications", "limit=500", "tok-worker"),
            ("/admin/audit-log", "limit=100000", "tok-admin"),
            ("/admin/marketplace-ops", "limit=7", "tok-admin"),
            ("/admin/application-pipeline", "limit=7", "tok-admin"),
        ):
            with self.subTest(path=path):
                self.assertEqual(self._request_api("GET", path, token=token, query=query)[0], 200)


class SessionExpiryTests(AuditFixesTestCase):
    def test_expired_sessions_are_rejected_immediately_in_both_formats(self):
        self._seed_users()
        now = datetime.now(timezone.utc)
        db = self.module.get_db()
        try:
            db.execute("INSERT INTO sessions (user_id,token,expires_at) VALUES (1,'iso-expired',?)",
                       [(now - timedelta(hours=1)).isoformat()])
            db.execute("INSERT INTO sessions (user_id,token,expires_at) VALUES (1,'iso-valid',?)",
                       [(now + timedelta(hours=1)).isoformat()])
            db.execute("INSERT INTO sessions (user_id,token,expires_at) VALUES (1,'canonical-expired',?)",
                       [(now - timedelta(seconds=5)).strftime("%Y-%m-%d %H:%M:%S")])
            db.execute("INSERT INTO sessions (user_id,token,expires_at) VALUES (1,'canonical-valid',?)",
                       [(now + timedelta(hours=1)).strftime("%Y-%m-%d %H:%M:%S")])
            db.commit()
        finally:
            db.close()
        for token, expected in (
            ("iso-expired", 401), ("iso-valid", 200),
            ("canonical-expired", 401), ("canonical-valid", 200),
        ):
            with self.subTest(token=token):
                status, body = self._request_api("GET", "/profile", token=token)
                self.assertEqual(status, expected, body)

    def test_new_sessions_are_written_in_canonical_utc_format(self):
        status, body = self._request_api("POST", "/auth/register", {
            "email": "fresh@example.com", "password": "FreshPassword123!", "name": "Fresh",
        })
        self.assertEqual(status, 201, body)
        db = self.module.get_db()
        try:
            stored = db.execute("SELECT expires_at FROM sessions WHERE token=?", [body["token"]]).fetchone()[0]
        finally:
            db.close()
        self.assertRegex(stored, r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$")
        expiry = datetime.strptime(stored, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
        self.assertAlmostEqual(
            (expiry - datetime.now(timezone.utc)).total_seconds(), 30 * 86400, delta=120
        )
        status, profile = self._request_api("GET", "/profile", token=body["token"])
        self.assertEqual(status, 200, profile)


class AdminStepUpThrottleTests(AuditFixesTestCase):
    def test_repeated_wrong_admin_passwords_are_throttled(self):
        self._seed_users(admin=True)
        payload = {"is_suspended": True, "admin_password": "WrongPassword!"}
        for attempt in range(6):
            status, body = self._request_api("PUT", "/admin/users/1", payload, token="tok-admin")
            self.assertEqual(status, 403, (attempt, body))
            self.assertIn("confirmation failed", body["error"])
        status, body = self._request_api("PUT", "/admin/users/1", payload, token="tok-admin")
        self.assertEqual(status, 429, body)
        self.assertIn("Too many", body["error"])
        # Even the right password is refused while throttled from this address.
        good = {"is_suspended": True, "admin_password": "AdminPassword123!"}
        status, body = self._request_api("PUT", "/admin/users/1", good, token="tok-admin")
        self.assertEqual(status, 429, body)
        self.module._request_ctx.remote_addr = "127.0.0.1"
        self.assertFalse(self.module.login_attempt_allowed("admin@example.com"))
        db = self.module.get_db()
        try:
            self.assertEqual(
                db.execute("SELECT COUNT(*) FROM audit_log WHERE action='admin_update_user_step_up_rate_limited'").fetchone()[0],
                2,
            )
            self.assertEqual(db.execute("SELECT is_suspended FROM users WHERE id=1").fetchone()[0], 0)
        finally:
            db.close()
        # The throttle is keyed by client address + admin email, like login.
        status, body = self._request_api("PUT", "/admin/users/1", good, token="tok-admin", remote_addr="10.0.0.9")
        self.assertEqual(status, 200, body)


class ReferralTests(AuditFixesTestCase):
    def test_referral_track_endpoint_is_gone_and_registration_still_attributes(self):
        status, body = self._request_api("POST", "/referral/track", {"ref_code": "abc", "user_id": 1})
        self.assertEqual(status, 404, body)

        status, referrer = self._request_api("POST", "/auth/register", {
            "email": "referrer@example.com", "password": "ReferrerPass123!", "name": "Referrer",
        })
        self.assertEqual(status, 201, referrer)
        status, referred = self._request_api("POST", "/auth/register", {
            "email": "referred@example.com", "password": "ReferredPass123!", "name": "Referred",
            "ref_code": referrer["referral_code"],
        })
        self.assertEqual(status, 201, referred)
        db = self.module.get_db()
        try:
            rows = db.execute("SELECT referrer_id,referred_id FROM referrals").fetchall()
            self.assertEqual([tuple(r) for r in rows], [(referrer["id"], referred["id"])])
        finally:
            db.close()


class WorkerProfileTests(AuditFixesTestCase):
    def test_payout_method_is_ignored_and_hourly_rate_validated(self):
        self._seed_users()
        status, body = self._request_api(
            "PUT", "/profile/worker",
            {"payout_method": "stripe_connect_active", "hourly_rate": "45.50", "bio": "anti-scam training author"},
            token="tok-worker",
        )
        self.assertEqual(status, 200, body)
        self.assertEqual(body["payout_method"], "pending_setup")
        self.assertEqual(body["hourly_rate"], 45.5)

        for invalid in ("abc", -5, "-0.01", "1e3", True, "12.345", [45]):
            with self.subTest(hourly_rate=invalid):
                status, body = self._request_api(
                    "PUT", "/profile/worker", {"hourly_rate": invalid}, token="tok-worker"
                )
                self.assertEqual(status, 400, body)
                self.assertIn("hourly_rate", body["error"])
        status, body = self._request_api(
            "PUT", "/profile/worker", raw_body='{"hourly_rate": NaN}', token="tok-worker"
        )
        self.assertEqual(status, 400, body)

        status, body = self._request_api("PUT", "/profile/worker", {"hourly_rate": None}, token="tok-worker")
        self.assertEqual(status, 200, body)
        self.assertIsNone(body["hourly_rate"])

        status, body = self._request_api(
            "PUT", "/profile/worker", {"bio": "Escort services available"}, token="tok-worker"
        )
        self.assertEqual(status, 422, body)


class LogHoursTests(AuditFixesTestCase):
    def setUp(self):
        super().setUp()
        self._seed_users()
        db = self.module.get_db()
        try:
            db.execute(
                "INSERT INTO orders (id,type,worker_id,employer_id,status,total_amount) "
                "VALUES (1,'job_hire',1,2,'in_progress',400)"
            )
            db.execute(
                "INSERT INTO hourly_contracts (order_id,hourly_rate,weekly_hour_cap,status) VALUES (1,50,40,'active')"
            )
            db.commit()
        finally:
            db.close()

    def test_hours_and_date_are_validated(self):
        for hours in ("abc", 0, -1, 25, "24.5", True, [2], "1e1"):
            with self.subTest(hours=hours):
                status, body = self._request_api(
                    "POST", "/orders/1/log-hours", {"hours": hours, "description": "x"}, token="tok-worker"
                )
                self.assertEqual(status, 400, body)
                self.assertIn("hours", body["error"])
        status, body = self._request_api(
            "POST", "/orders/1/log-hours", raw_body='{"hours": NaN}', token="tok-worker"
        )
        self.assertEqual(status, 400, body)
        for bad_date in (20260101, ["2026-01-01"], "01/01/2026"):
            with self.subTest(date=bad_date):
                status, body = self._request_api(
                    "POST", "/orders/1/log-hours", {"hours": 1, "date": bad_date}, token="tok-worker"
                )
                self.assertEqual(status, 400, body)
                self.assertIn("date", body["error"].lower())

        status, body = self._request_api(
            "POST", "/orders/1/log-hours",
            {"hours": "2.5", "date": "2026-09-14", "description": "Reviewed the brief"},
            token="tok-worker",
        )
        self.assertEqual(status, 201, body)
        self.assertEqual(body["hours"], 2.5)
        self.assertEqual(body["week_of"], "2026-09-14")


class OrderCompletionCounterTests(AuditFixesTestCase):
    def test_services_track_completed_orders_separately_from_reviews(self):
        db = self.module.get_db()
        try:
            columns = {row[1] for row in db.execute("PRAGMA table_info('services')").fetchall()}
        finally:
            db.close()
        self.assertIn("total_orders", columns)
        self.assertIn("total_reviews", columns)
        # Re-running the migration must be a no-op.
        self.module.init_db()
        source = (BACKEND_DIR / "api_core.py").read_text(encoding="utf-8")
        self.assertNotIn("UPDATE services SET total_reviews = total_reviews + 1", source)
        self.assertIn("UPDATE services SET total_orders = total_orders + 1", source)


class ConnectionHandlingTests(AuditFixesTestCase):
    def test_public_stats_and_diagnostics_use_the_request_connection(self):
        real_get_db = self.module.get_db
        counter = mock.Mock(wraps=real_get_db)
        with mock.patch.object(self.module, "get_db", counter):
            status, body = self._request_api("GET", "/platform/stats")
        self.assertEqual(status, 200, body)
        self.assertEqual(counter.call_count, 1)

        self.module.DIAGNOSTIC_ENDPOINT_ENABLED = True
        self.module.DIAGNOSTIC_SECRET = "diag-secret-under-test"
        counter = mock.Mock(wraps=real_get_db)
        with mock.patch.object(self.module, "get_db", counter):
            status, body = self._request_api(
                "GET", "/diag/db", extra_ctx={"http_x_diagnostic_secret": "diag-secret-under-test"}
            )
        self.assertEqual(status, 200, body)
        self.assertEqual(body["user_count"], 0)
        self.assertEqual(counter.call_count, 1)


class RateLimitStoreTests(AuditFixesTestCase):
    def test_stores_evict_stale_keys_once_they_grow_large(self):
        stale = time.time() - 3600
        store = self.module._rate_limit_store
        store.clear()
        for index in range(5001):
            store[f"10.{index // 65536}.{(index // 256) % 256}.{index % 256}"] = [stale]
        self.module._request_ctx.remote_addr = "127.0.0.1"
        self.assertTrue(self.module.check_rate_limit())
        self.assertEqual(set(store), {"127.0.0.1"})

        login_store = self.module._login_failure_store
        login_store.clear()
        for index in range(5001):
            login_store[f"10.0.0.{index}:user{index}@example.com"] = [stale]
        self.module.record_login_failure("someone@example.com")
        self.assertEqual(set(login_store), {"127.0.0.1:someone@example.com"})
        # Small stores are left untouched (pruning only kicks in past the cap).
        login_store["fresh:key"] = [stale]
        self.module.record_login_failure("someone@example.com")
        self.assertIn("fresh:key", login_store)
        store.clear()
        login_store.clear()


class ServerBehaviourTests(AuditFixesTestCase):
    def _load_server(self):
        server = load_server()
        server.api_module.PRODUCTION_MODE = False
        return server

    def _capture_client_ip(self, server, headers, env):
        seen = []

        def fake_handle_request():
            seen.append(server.api_module._request_ctx.remote_addr)
            print("Status: 200")
            print("Content-Type: application/json")
            print()
            print("{}")

        with mock.patch.dict(os.environ, env):
            for key in ("RAILWAY_ENVIRONMENT", "RAILWAY_PROJECT_ID", "TRUST_X_FORWARDED_FOR"):
                if key not in env:
                    os.environ.pop(key, None)
            with mock.patch.object(server.api_module, "handle_request", side_effect=fake_handle_request):
                response = server.app.test_client().get("/categories", headers=headers)
        self.assertEqual(response.status_code, 200, response.get_data(as_text=True))
        return seen[0]

    def test_forwarded_header_is_trusted_only_behind_a_known_proxy(self):
        server = self._load_server()
        forwarded = {"X-Forwarded-For": "203.0.113.9, 10.0.0.1"}
        self.assertEqual(self._capture_client_ip(server, forwarded, {}), "127.0.0.1")
        self.assertEqual(
            self._capture_client_ip(server, forwarded, {"RAILWAY_PROJECT_ID": "proj"}), "10.0.0.1"
        )
        self.assertEqual(
            self._capture_client_ip(server, forwarded, {"RAILWAY_ENVIRONMENT": "production"}), "10.0.0.1"
        )
        self.assertEqual(
            self._capture_client_ip(server, forwarded, {"RAILWAY_ENVIRONMENT": "production", "TRUST_X_FORWARDED_FOR": "0"}),
            "127.0.0.1",
        )
        self.assertEqual(self._capture_client_ip(server, forwarded, {"TRUST_X_FORWARDED_FOR": "1"}), "10.0.0.1")
        server.api_module.PRODUCTION_MODE = True
        self.assertEqual(self._capture_client_ip(server, forwarded, {}), "10.0.0.1")
        self.assertTrue(server._production_mode())
        server.api_module.PRODUCTION_MODE = False
        self.assertFalse(server._production_mode())

    def test_security_headers_include_csp_and_oversize_bodies_get_json_413(self):
        server = self._load_server()
        client = server.app.test_client()
        response = client.get("/health")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            response.headers["Content-Security-Policy"], "default-src 'none'; frame-ancestors 'none'"
        )
        self.assertEqual(response.headers["X-Frame-Options"], "DENY")
        response = client.post(
            "/webhooks/resend", data=b"x" * ((2 * 1024 * 1024) + 1), content_type="application/json"
        )
        self.assertEqual(response.status_code, 413)
        self.assertEqual(response.content_type, "application/json")
        self.assertEqual(response.get_json()["error"], "Request body too large")
        self.assertEqual(response.headers["Content-Security-Policy"], "default-src 'none'; frame-ancestors 'none'")


class McpServerFieldTests(unittest.TestCase):
    def _load(self):
        with mock.patch.dict(os.environ, {}, clear=True):
            spec = importlib.util.spec_from_file_location("mcp_server_audit_fixes", BACKEND_DIR / "mcp_server.py")
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
        return module

    def test_search_and_browse_send_per_page_and_print_worker_name(self):
        module = self._load()
        calls = []

        def fake_api(method, route, body=None, params=None):
            calls.append((method, route, body, params))
            if route == "/services":
                return {"services": [{"id": 1, "title": "QA pass", "category": "testing", "price": 25,
                                      "description": "Review", "worker_name": "Worker One",
                                      "worker_rating": 4.5, "delivery_time_days": 2}],
                        "total": 1, "page": 1, "per_page": 3, "total_pages": 1}
            return {"jobs": [{"id": 9, "title": "Job", "category": "testing", "budget_type": "fixed",
                              "budget_amount": 25, "description": "d"}], "total": 1}

        with mock.patch.object(module, "api_request", side_effect=fake_api):
            services_text = module.handle_search_services({"query": "qa", "limit": 3})[0]["text"]
            module.handle_browse_jobs({"limit": 4})
            module.handle_search_workers({"limit": 2})
            module.handle_get_recommended({"task_description": "review a website", "limit": 2})
        self.assertIn("Provider: Worker One", services_text)
        for method, route, body, params in calls:
            with self.subTest(route=route, params=params):
                self.assertNotIn("limit", params or {})
                self.assertIn("per_page", params or {})
        self.assertEqual(calls[0][3]["per_page"], 3)
        self.assertEqual(calls[1][3]["per_page"], 4)

    def test_service_details_use_worker_rating_and_delivery_days(self):
        module = self._load()
        with mock.patch.object(module, "api_request", return_value={
            "id": 5, "title": "Bounded review", "category": "testing", "price": 99,
            "worker_name": "Worker One", "worker_rating": 4.8, "avg_rating": 0,
            "delivery_time_days": 7, "description": "Review",
        }):
            text = module.handle_get_service_details({"service_id": "5"})[0]["text"]
        self.assertIn("**Provider:** Worker One", text)
        self.assertIn("**Rating:** 4.8", text)
        self.assertIn("**Delivery Time:** 7 day(s)", text)

    def test_create_job_sends_required_skills(self):
        module = self._load()
        calls = []
        with mock.patch.object(module, "api_request", side_effect=lambda m, r, body=None, params=None: calls.append(body) or {"id": 3}):
            module.handle_create_job({
                "title": "Build", "description": "Build a page", "category": "web_development",
                "budget_type": "fixed", "budget_amount": 100, "skills_required": ["React"],
            })
        self.assertEqual(calls[0]["required_skills"], ["React"])
        self.assertNotIn("skills_required", calls[0])

    def test_order_status_and_hire_print_total_amount(self):
        module = self._load()
        with mock.patch.object(module, "api_request", return_value={
            "id": 12, "type": "service_order", "status": "in_progress", "total_amount": 42.5,
            "created_at": "2026-09-14", "worker_name": "Worker One",
        }):
            status_text = module.handle_get_job_status({"order_id": 12})[0]["text"]
        self.assertIn("**Amount:** $42.5", status_text)
        self.assertIn("**Worker:** Worker One", status_text)

        def fake_api(method, route, body=None, params=None):
            if method == "GET":
                return {"id": 1, "worker_id": 1, "title": "Custom QA", "price": 25, "worker_name": "Worker One"}
            return {"id": 77, "status": "in_progress", "total_amount": 25.0}

        with mock.patch.object(module, "api_request", side_effect=fake_api):
            hire_text = module.handle_hire_worker({
                "service_id": 1, "requirements": "Check flow", "budget_amount": "25.00",
                "idempotency_key": "mcp-service-operation-0002",
            })[0]["text"]
        self.assertIn("Worker hired successfully", hire_text)
        self.assertIn("**Amount:** $25.0", hire_text)
        self.assertIn("**Worker:** Worker One", hire_text)

    def test_package_copy_is_identical(self):
        self.assertEqual(
            (BACKEND_DIR / "mcp_server.py").read_bytes(),
            (BACKEND_DIR / "mcp-package" / "mcp_server.py").read_bytes(),
        )


class DeploymentFilesTests(unittest.TestCase):
    def test_start_script_runs_gunicorn(self):
        start = (BACKEND_DIR / "start.sh").read_text(encoding="utf-8")
        self.assertRegex(start, r"exec gunicorn .*server:app")
        self.assertNotRegex(start, r"(?m)^\s*(exec\s+)?python3?\s+server\.py")
        self.assertIn("--workers 1", start)
        self.assertIn("--threads 8", start)
        dockerfile = (BACKEND_DIR / "Dockerfile").read_text(encoding="utf-8")
        self.assertIn('CMD ["/app/start.sh"]', dockerfile)
        self.assertIn("gunicorn", (BACKEND_DIR / "requirements.txt").read_text(encoding="utf-8"))

    def test_dockerignore_keeps_tests_and_secrets_out_of_the_image(self):
        ignore = (BACKEND_DIR / ".dockerignore").read_text(encoding="utf-8").splitlines()
        for entry in ("test_*.py", "tools/", "design/", "mcp-package/", "*.db", "*.db-*", ".env", ".env.*", "__pycache__/", "*.pyc"):
            self.assertIn(entry, ignore)

    def test_env_example_documents_durable_database_and_proxy_trust(self):
        example = (BACKEND_DIR / ".env.example").read_text(encoding="utf-8")
        self.assertIn("/data/gohirehumans.db", example)
        self.assertIn("TRUST_X_FORWARDED_FOR", example)
        self.assertNotIn("/app/data/agentwork.db", example)
        readme = (REPO_ROOT / "README.md").read_text(encoding="utf-8")
        self.assertIn("/admin/dashboard", readme)
        self.assertNotIn("/admin/stats", readme)



class OwnerScopedListingTests(AuditFixesTestCase):
    """GET /me/services and GET /me/jobs list the caller's own rows in every status."""

    def setUp(self):
        super().setUp()
        self._seed_users()
        db = self.module.get_db()
        try:
            # User 1 owns an active, a paused and a removed service plus an open
            # and a hired job; user 2 owns rows in both tables that must never
            # leak into user 1's listings. Distinct created_at values make the
            # newest-first ordering assertions deterministic.
            db.executemany(
                "INSERT INTO services (id,worker_id,title,description,category,price,status,created_at) "
                "VALUES (?,?,?,?,?,?,?,?)",
                [
                    (1, 1, "Active listing", "d", "writing", 50, "active", "2026-09-01 10:00:00"),
                    (2, 1, "Paused listing", "d", "writing", 60, "paused", "2026-09-02 10:00:00"),
                    (3, 1, "Removed listing", "d", "writing", 70, "removed", "2026-09-03 10:00:00"),
                    (4, 2, "Someone else's listing", "d", "writing", 80, "active", "2026-09-04 10:00:00"),
                ],
            )
            db.executemany(
                "INSERT INTO jobs (id,employer_id,title,description,category,budget_type,budget_amount,status,created_at) "
                "VALUES (?,?,?,?,?,?,?,?,?)",
                [
                    (1, 1, "Open job", "d", "writing", "fixed", 100, "open", "2026-09-01 10:00:00"),
                    (2, 1, "Hired job", "d", "writing", "fixed", 200, "hired", "2026-09-02 10:00:00"),
                    (3, 2, "Someone else's job", "d", "writing", "fixed", 300, "open", "2026-09-03 10:00:00"),
                ],
            )
            db.execute("INSERT INTO applications (job_id,worker_id,status) VALUES (2,2,'accepted')")
            db.commit()
        finally:
            db.close()

    def test_me_services_lists_every_non_removed_status_newest_first(self):
        status, body = self._request_api("GET", "/me/services", token="tok-worker")
        self.assertEqual(status, 200, body)
        self.assertEqual([s["id"] for s in body["services"]], [2, 1])
        self.assertEqual([s["status"] for s in body["services"]], ["paused", "active"])
        self.assertEqual((body["total"], body["page"], body["per_page"], body["total_pages"]), (2, 1, 20, 1))
        # Same projection as the public browse so the SPA card renderer is unchanged.
        public_row = self._request_api("GET", "/services")[1]["services"][0]
        self.assertEqual(set(body["services"][0]), set(public_row))
        self.assertEqual(body["services"][0]["worker_name"], "Worker")
        self.assertIn("worker_rating", body["services"][0])

    def test_me_services_removed_rows_need_include_removed_or_explicit_status(self):
        for query, expected in (
            ("include_removed=1", [3, 2, 1]),
            ("include_removed=true", [3, 2, 1]),
            ("include_removed=0", [2, 1]),
            ("status=paused", [2]),
            ("status=removed", [3]),
            ("status=active&include_removed=1", [1]),
        ):
            with self.subTest(query=query):
                status, body = self._request_api("GET", "/me/services", token="tok-worker", query=query)
                self.assertEqual(status, 200, body)
                self.assertEqual([s["id"] for s in body["services"]], expected)
                self.assertEqual(body["total"], len(expected))
        status, body = self._request_api("GET", "/me/services", token="tok-worker", query="status=deleted")
        self.assertEqual(status, 400, body)
        self.assertIn("Invalid status", body["error"])

    def test_me_jobs_lists_every_status_with_application_count(self):
        status, body = self._request_api("GET", "/me/jobs", token="tok-worker")
        self.assertEqual(status, 200, body)
        self.assertEqual([j["id"] for j in body["jobs"]], [2, 1])
        self.assertEqual([j["status"] for j in body["jobs"]], ["hired", "open"])
        self.assertEqual([j["application_count"] for j in body["jobs"]], [1, 0])
        self.assertEqual((body["total"], body["page"], body["per_page"], body["total_pages"]), (2, 1, 20, 1))
        public_row = self._request_api("GET", "/jobs")[1]["jobs"][0]
        self.assertEqual(set(body["jobs"][0]) - {"application_count"}, set(public_row))
        self.assertEqual(body["jobs"][0]["employer_name"], "Worker")
        status, body = self._request_api("GET", "/me/jobs", token="tok-worker", query="status=hired")
        self.assertEqual(status, 200, body)
        self.assertEqual([j["id"] for j in body["jobs"]], [2])
        status, body = self._request_api("GET", "/me/jobs", token="tok-worker", query="status=archived")
        self.assertEqual(status, 400, body)
        self.assertIn("Invalid status", body["error"])

    def test_other_users_rows_are_never_returned(self):
        status, body = self._request_api("GET", "/me/services", token="tok-employer", query="include_removed=1")
        self.assertEqual(status, 200, body)
        self.assertEqual([s["id"] for s in body["services"]], [4])
        status, body = self._request_api("GET", "/me/jobs", token="tok-employer")
        self.assertEqual(status, 200, body)
        self.assertEqual([j["id"] for j in body["jobs"]], [3])
        # Ownership is taken from the principal only; query params cannot widen it.
        for query in ("worker_id=2", "employer_id=2", "user_id=2", "include_removed=1"):
            with self.subTest(query=query):
                self.assertEqual(
                    [s["worker_id"] for s in self._request_api("GET", "/me/services", token="tok-worker", query=query)[1]["services"]],
                    [1] * (3 if query == "include_removed=1" else 2),
                )
                self.assertEqual(
                    [j["employer_id"] for j in self._request_api("GET", "/me/jobs", token="tok-worker", query=query)[1]["jobs"]],
                    [1, 1],
                )

    def test_seeded_sample_owners_still_see_their_own_rows(self):
        sample_email = sorted(self.module.SEEDED_SAMPLE_EMAILS)[0]
        db = self.module.get_db()
        try:
            db.execute("UPDATE users SET email=? WHERE id=1", [sample_email])
            db.commit()
        finally:
            db.close()
        # The public browse hides seeded sample accounts...
        self.assertNotIn(1, [s["id"] for s in self._request_api("GET", "/services")[1]["services"]])
        self.assertNotIn(1, [j["id"] for j in self._request_api("GET", "/jobs")[1]["jobs"]])
        # ...but owners always see their own rows.
        status, body = self._request_api("GET", "/me/services", token="tok-worker")
        self.assertEqual(status, 200, body)
        self.assertEqual([s["id"] for s in body["services"]], [2, 1])
        status, body = self._request_api("GET", "/me/jobs", token="tok-worker")
        self.assertEqual(status, 200, body)
        self.assertEqual([j["id"] for j in body["jobs"]], [2, 1])

    def test_unauthenticated_requests_are_rejected(self):
        for path in ("/me/services", "/me/jobs"):
            for token in ("", "not-a-session"):
                with self.subTest(path=path, token=token):
                    status, body = self._request_api("GET", path, token=token)
                    self.assertEqual(status, 401, body)
                    self.assertEqual(body["error"], "Unauthorized")

    def test_pagination_is_validated_and_capped(self):
        for path, key in (("/me/services", "services"), ("/me/jobs", "jobs")):
            for query in ("page=abc", "page=0", "per_page=0", "per_page=-1", "per_page=x"):
                with self.subTest(path=path, query=query):
                    status, body = self._request_api("GET", path, token="tok-worker", query=query)
                    self.assertEqual(status, 400, body)
                    self.assertIn("Invalid", body["error"])
            with self.subTest(path=path, query="per_page=999"):
                status, body = self._request_api("GET", path, token="tok-worker", query="per_page=999")
                self.assertEqual(status, 200, body)
                self.assertEqual(body["per_page"], 100)
                self.assertEqual(body["total_pages"], 1)
            with self.subTest(path=path, query="per_page=1&page=2"):
                status, body = self._request_api("GET", path, token="tok-worker", query="per_page=1&page=2")
                self.assertEqual(status, 200, body)
                self.assertEqual([row["id"] for row in body[key]], [1])
                self.assertEqual((body["total"], body["page"], body["per_page"], body["total_pages"]), (2, 2, 1, 2))
            with self.subTest(path=path, query="page=3"):
                status, body = self._request_api("GET", path, token="tok-worker", query="page=3")
                self.assertEqual(status, 200, body)
                self.assertEqual(body[key], [])

    def test_api_keys_need_read_scope(self):
        for path in ("/me/services", "/me/jobs"):
            self.assertEqual(self.module._api_key_route_scope("GET", path), "read")
        read_key = ApiKeyTests._create_key(self, 1, scopes='["read"]')
        write_only_key = ApiKeyTests._create_key(self, 1, scopes='["write"]')
        for path, key in (("/me/services", "services"), ("/me/jobs", "jobs")):
            with self.subTest(path=path):
                status, body = self._request_api("GET", path, api_key=read_key)
                self.assertEqual(status, 200, body)
                self.assertEqual([row["id"] for row in body[key]], [2, 1])
                status, body = self._request_api("GET", path, api_key=write_only_key)
                self.assertEqual(status, 403, body)
                self.assertIn("scope does not permit", body["error"])

    def test_mcp_api_docs_resource_documents_owner_listings(self):
        module = McpServerFieldTests._load(self)
        with mock.patch.object(module, "api_request", side_effect=AssertionError("Network forbidden")):
            text = module.handle_resource("gohirehumans://api-docs")["contents"][0]["text"]
        self.assertIn("`GET /me/services`", text)
        self.assertIn("`GET /me/jobs`", text)
        self.assertIn("include_removed=1", text)
        self.assertIn("application_count", text)


if __name__ == "__main__":
    unittest.main()
