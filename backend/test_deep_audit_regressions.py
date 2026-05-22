import contextlib
import importlib.util
import io
import json
import os
import sqlite3
import tempfile
from pathlib import Path
from types import ModuleType
from typing import Any, cast
import unittest

MODULE_PATH = Path(__file__).with_name("api_core.py")
REPO_ROOT = MODULE_PATH.parents[1]


def load_api_core() -> Any:
    spec = importlib.util.spec_from_file_location("api_core_under_test_regressions", MODULE_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError("Could not load api_core.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(cast(ModuleType, module))
    return module


def parse_cgi_output(output: str):
    header_text, _, body = output.partition("\n\n")
    status = 200
    for line in header_text.splitlines():
        if line.startswith("Status:"):
            status = int(line.split(":", 1)[1].strip())
    return status, json.loads(body or "{}")


class BackendRegressionTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        os.environ["DATABASE_PATH"] = str(Path(self.tmp.name) / "test.db")
        os.environ["DISABLE_AUTO_SEED"] = "1"
        os.environ.pop("GOOGLE_CLIENT_ID", None)
        self.module = load_api_core()
        self.module._db_path_resolved = None
        self.module._seeded = False
        self.module.init_db()

    def tearDown(self):
        self.tmp.cleanup()
        os.environ.pop("DATABASE_PATH", None)
        os.environ.pop("DISABLE_AUTO_SEED", None)

    def test_release_escrow_pays_worker_listed_amount_and_records_one_percent_margin(self):
        db = self.module.get_db()
        try:
            db.execute("INSERT INTO users (id,email,password_hash,name) VALUES (1,'worker@example.com','x','Worker')")
            db.execute("INSERT INTO worker_profiles (user_id,payout_account_id) VALUES (1,'acct_sim_worker')")
            db.execute("INSERT INTO users (id,email,password_hash,name) VALUES (2,'employer@example.com','x','Employer')")
            db.execute("INSERT INTO employer_profiles (user_id) VALUES (2)")
            db.execute("INSERT INTO services (id,worker_id,title,description,category,pricing_type,price) VALUES (1,1,'Svc','Desc','writing','fixed',100)")
            db.execute("INSERT INTO orders (id,type,service_id,worker_id,employer_id,status,total_amount) VALUES (1,'service_order',1,1,2,'submitted',100)")
            db.execute("INSERT INTO escrow_holds (order_id,amount,status,stripe_payment_intent_id) VALUES (1,100,'held','pi_sim')")
            payout, fee = self.module.release_escrow_to_worker(db, 1, None, 100, 1)
            self.assertEqual(payout, 100)
            self.assertEqual(fee, 1)
        finally:
            db.close()

    def test_complete_order_releases_held_escrow_before_marking_completed(self):
        db = self.module.get_db()
        try:
            db.execute("INSERT INTO users (id,email,password_hash,name) VALUES (1,'worker@example.com','x','Worker')")
            db.execute("INSERT INTO worker_profiles (user_id,payout_account_id) VALUES (1,'acct_sim_worker')")
            db.execute("INSERT INTO users (id,email,password_hash,name) VALUES (2,'employer@example.com','x','Employer')")
            db.execute("INSERT INTO employer_profiles (user_id) VALUES (2)")
            db.execute("INSERT INTO services (id,worker_id,title,description,category,pricing_type,price) VALUES (1,1,'Svc','Desc','writing','fixed',100)")
            db.execute("INSERT INTO orders (id,type,service_id,worker_id,employer_id,status,total_amount) VALUES (1,'service_order',1,1,2,'submitted',100)")
            db.execute("INSERT INTO escrow_holds (order_id,amount,status,stripe_payment_intent_id) VALUES (1,100,'held','pi_sim')")
            token = 'tok-test'
            db.execute("INSERT INTO sessions (user_id,token,expires_at) VALUES (2,?,datetime('now','+1 day'))", [token])
            db.commit()
        finally:
            db.close()
        self.module._request_ctx.request_method = "POST"
        self.module._request_ctx.path_info = "/orders/1/complete"
        self.module._request_ctx.query_string = ""
        self.module._request_ctx.http_authorization = f"Bearer {token}"
        self.module._request_ctx.stdin_data = "{}"
        self.module._request_ctx.content_type = "application/json"
        self.module._request_ctx.content_length = "2"
        self.module._request_ctx.remote_addr = "127.0.0.1"
        with contextlib.redirect_stdout(io.StringIO()) as out:
            self.module.handle_request()
        status, body = parse_cgi_output(out.getvalue())
        self.assertEqual(status, 200, body)
        db = self.module.get_db()
        try:
            order_status = db.execute("SELECT status FROM orders WHERE id=1").fetchone()[0]
            escrow_status = db.execute("SELECT status FROM escrow_holds WHERE order_id=1").fetchone()[0]
            payout = db.execute("SELECT fee_amount FROM platform_revenue WHERE order_id=1").fetchone()[0]
            self.assertEqual(order_status, "completed")
            self.assertEqual(escrow_status, "released")
            self.assertEqual(payout, 1)
        finally:
            db.close()

    def test_api_key_header_authenticates_protected_profile_route(self):
        db = self.module.get_db()
        raw_key = "ghh_test_key_value"
        key_hash = self.module.hashlib.sha256(raw_key.encode()).hexdigest()
        try:
            db.execute("INSERT INTO users (id,email,password_hash,name) VALUES (1,'api@example.com','x','API User')")
            db.execute("INSERT INTO api_keys (user_id,key_hash,key_prefix,name,scopes) VALUES (1,?,?,?,?)", [key_hash, raw_key[:12], "Test", '["read"]'])
            db.commit()
            self.module._request_ctx.http_x_api_key = raw_key
            user = self.module.authenticate(db)
            self.assertIsNotNone(user)
            self.assertEqual(user["email"], "api@example.com")
        finally:
            db.close()

    def test_public_bad_numeric_query_params_return_400_not_500(self):
        for path, query in [("/services", "per_page=abc"), ("/services", "min_price=abc"), ("/jobs", "per_page=abc"), ("/jobs", "min_budget=abc")]:
            self.module._request_ctx.request_method = "GET"
            self.module._request_ctx.path_info = path
            self.module._request_ctx.query_string = query
            self.module._request_ctx.http_authorization = ""
            self.module._request_ctx.http_x_api_key = ""
            self.module._request_ctx.stdin_data = ""
            self.module._request_ctx.content_type = ""
            self.module._request_ctx.content_length = "0"
            self.module._request_ctx.remote_addr = "127.0.0.1"
            with contextlib.redirect_stdout(io.StringIO()) as out:
                self.module.handle_request()
            status, body = parse_cgi_output(out.getvalue())
            self.assertEqual(status, 400, (path, query, body))

    def test_google_oauth_fails_closed_without_client_id(self):
        self.assertFalse(self.module.google_oauth_configured())

    def test_auto_seed_is_disabled_unless_explicitly_enabled(self):
        db = self.module.get_db()
        try:
            self.assertEqual(db.execute("SELECT COUNT(*) FROM users").fetchone()[0], 0)
        finally:
            db.close()
        self.module.auto_seed_if_empty()
        db = self.module.get_db()
        try:
            self.assertEqual(db.execute("SELECT COUNT(*) FROM users").fetchone()[0], 0)
        finally:
            db.close()

    def test_payment_status_returns_frontend_ready_booleans(self):
        db = self.module.get_db()
        try:
            db.execute("INSERT INTO users (id,email,password_hash,name) VALUES (1,'ready@example.com','x','Ready User')")
            db.execute("INSERT INTO worker_profiles (user_id,payout_account_id,payout_method) VALUES (1,'acct_sim_worker','stripe_connect_active')")
            db.execute("INSERT INTO employer_profiles (user_id,stripe_customer_id,payment_method_id) VALUES (1,'cus_sim','pm_sim')")
            db.execute("INSERT INTO sessions (user_id,token,expires_at) VALUES (1,'tok-ready',datetime('now','+1 day'))")
            db.commit()
        finally:
            db.close()

        self.module._request_ctx.request_method = "GET"
        self.module._request_ctx.path_info = "/payments/status"
        self.module._request_ctx.query_string = ""
        self.module._request_ctx.http_authorization = "Bearer tok-ready"
        self.module._request_ctx.http_x_api_key = ""
        self.module._request_ctx.stdin_data = ""
        self.module._request_ctx.content_type = ""
        self.module._request_ctx.content_length = "0"
        self.module._request_ctx.remote_addr = "127.0.0.1"
        with contextlib.redirect_stdout(io.StringIO()) as out:
            self.module.handle_request()
        status, body = parse_cgi_output(out.getvalue())
        self.assertEqual(status, 200, body)
        self.assertIn("worker_payout_status", body)
        self.assertIn("employer_payment_status", body)
        self.assertIs(body["worker_ready"], True)
        self.assertIs(body["employer_ready"], True)


class FrontendStaticRegressionTests(unittest.TestCase):
    def test_public_marketplace_pages_do_not_render_api_strings_with_raw_innerhtml(self):
        risky = []
        for rel in [
            "frontend/categories/data-entry.html",
            "frontend/categories/virtual-assistant.html",
            "frontend/categories/web-development.html",
            "frontend/categories/graphic-design.html",
            "frontend/categories/writing.html",
            "frontend/categories/translation.html",
            "frontend/stats.html",
        ]:
            text = (REPO_ROOT / rel).read_text(encoding="utf-8")
            if "innerHTML = services.map" in text or "innerHTML = recent.map" in text or "${s.title}" in text or "${j.title}" in text:
                risky.append(rel)
        self.assertEqual(risky, [])

    def test_docs_do_not_advertise_legacy_task_or_checkout_endpoints(self):
        bad = []
        legacy_terms = [
            "/api/v1/tasks",
            "/api/v1/payments/checkout",
            "/payments/fund-payment hold",
            "/payments/balance",
            "payment hold_balance",
            "Task Endpoints",
        ]
        for rel in ["frontend/api-docs.html", "frontend/how-it-works.html", "frontend/ai-integration.html", "frontend/faq.html", "README.md"]:
            text = (REPO_ROOT / rel).read_text(encoding="utf-8")
            if any(term in text for term in legacy_terms):
                bad.append(rel)
        self.assertEqual(bad, [])

    def test_no_dead_browse_hash_ctas(self):
        hits = []
        for path in (REPO_ROOT / "frontend").rglob("*.html"):
            if "#browse" in path.read_text(encoding="utf-8", errors="ignore"):
                hits.append(str(path.relative_to(REPO_ROOT)))
        self.assertEqual(hits, [])

    def test_no_known_broken_assets_links_or_payment_copy_typos(self):
        bad_terms = [
            "hiw-step2-payment hold.png",
            "best-freelance-platforms-payment hold.html",
            "Payment Payments",
            "Payment payments",
            "payment payment",
            "payment hold payment",
            "Platform fee (4%)",
        ]
        hits = []
        for path in (REPO_ROOT / "frontend").rglob("*.html"):
            text = path.read_text(encoding="utf-8", errors="ignore")
            for term in bad_terms:
                if term in text:
                    hits.append(f"{path.relative_to(REPO_ROOT)}: {term}")
        self.assertEqual(hits, [])

    def test_homepage_has_low_risk_funnel_analytics_events(self):
        text = (REPO_ROOT / "frontend/index.html").read_text(encoding="utf-8", errors="ignore")
        required_snippets = [
            "function trackEvent(eventName, params = {})",
            "gtag('event', eventName, params)",
            "function searchHeroServices()",
            "hero_search_submit",
            "post_task_cta_click",
            "browse_humans_cta_click",
            "find_paid_tasks_cta_click",
            "service_order_intent",
            "job_apply_intent",
            "explainer_video_play",
        ]
        for snippet in required_snippets:
            self.assertIn(snippet, text)

    def test_homepage_has_concierge_task_drafts_without_automatic_outreach(self):
        text = (REPO_ROOT / "frontend/index.html").read_text(encoding="utf-8", errors="ignore")
        for snippet in [
            "Not sure what to post?",
            "Concierge-style drafts",
            "startTaskDraft('website_test')",
            "startTaskDraft('lead_research')",
            "startTaskDraft('ai_review')",
            "concierge_task_draft_click",
            "getTaskDraftTemplate(getQuery().get('template'))",
            "Nothing is submitted until you review and post the listing.",
            "Workers receive the listed payout",
            "Employer pays Stripe processing + 1%",
        ]:
            self.assertIn(snippet, text)
        concierge_block = text[text.index("Task drafting help"):text.index("<!-- ═══ EXPLAINER VIDEO SECTION ═══")]
        self.assertNotIn("mailto:", concierge_block)
        self.assertNotIn("fetch(", concierge_block)
        self.assertNotIn("api(", concierge_block)

    def test_homepage_public_copy_uses_connector_pricing_framing(self):
        text = (REPO_ROOT / "frontend/index.html").read_text(encoding="utf-8", errors="ignore")
        public_landing = text[:text.index("// ═══════════════════════════════════════════════════════════════\n// SERVICES BROWSE")]
        for snippet in [
            "Workers receive the listed payout",
            "Stripe processing plus a 1% GoHireHumans fee",
            "Employer pays Stripe processing + 1%",
        ]:
            self.assertIn(snippet, public_landing)
        forbidden_terms = [
            "4% fee",
            "4% platform fee",
            "4% employer fee",
            "verified human",
            "verified professionals",
            "verified profiles",
            "protected by Stripe payment hold",
            "protects every transaction",
            "guaranteed completion",
            "escrow-protected",
            "risk-free",
            "platform arbitration",
            "verified safe",
            "guarantee quality",
        ]
        lower_public = public_landing.lower()
        for term in forbidden_terms:
            self.assertNotIn(term, lower_public)

    def test_task_template_pages_exist_with_safe_connector_framing(self):
        required_pages = [
            "frontend/hire/website-testers.html",
            "frontend/hire/lead-researchers.html",
            "frontend/hire/ai-reviewers.html",
            "frontend/hire/phone-call-help.html",
            "frontend/hire/local-verification.html",
            "frontend/earn/get-paid-for-human-tasks.html",
        ]
        forbidden_claims = [
            "guaranteed completion",
            "escrow-protected",
            "risk-free",
            "platform arbitration",
            "verified safe",
            "guarantee quality",
            "4% employer fee",
        ]
        missing = []
        unsafe = []
        for rel in required_pages:
            path = REPO_ROOT / rel
            if not path.exists():
                missing.append(rel)
                continue
            text = path.read_text(encoding="utf-8", errors="ignore")
            lower = text.lower()
            for phrase in [
                "Example tasks you can post",
                "Suggested payout ranges",
                "Connector framing",
                "Workers receive the listed payout",
                "Stripe processing plus a 1% GoHireHumans fee",
            ]:
                if phrase not in text:
                    missing.append(f"{rel}: {phrase}")
            for claim in forbidden_claims:
                if claim in lower:
                    unsafe.append(f"{rel}: {claim}")
        self.assertEqual(missing, [])
        self.assertEqual(unsafe, [])

    def test_task_template_pages_are_discoverable_in_sitemap(self):
        sitemap = (REPO_ROOT / "frontend/sitemap.xml").read_text(encoding="utf-8")
        for loc in [
            "https://www.gohirehumans.com/hire/website-testers.html",
            "https://www.gohirehumans.com/hire/lead-researchers.html",
            "https://www.gohirehumans.com/hire/ai-reviewers.html",
            "https://www.gohirehumans.com/hire/phone-call-help.html",
            "https://www.gohirehumans.com/hire/local-verification.html",
            "https://www.gohirehumans.com/earn/get-paid-for-human-tasks.html",
        ]:
            self.assertIn(loc, sitemap)


if __name__ == "__main__":
    unittest.main()
