import contextlib
import importlib.util
import io
import json
import os
import re
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

    def test_job_creation_notifies_matching_service_workers(self):
        db = self.module.get_db()
        token = "tok-employer"
        try:
            db.execute("INSERT INTO users (id,email,password_hash,name) VALUES (1,'worker@example.com','x','Worker')")
            db.execute("INSERT INTO worker_profiles (user_id) VALUES (1)")
            db.execute("INSERT INTO users (id,email,password_hash,name) VALUES (2,'employer@example.com','x','Employer')")
            db.execute("INSERT INTO employer_profiles (user_id) VALUES (2)")
            db.execute("INSERT INTO sessions (user_id,token,expires_at) VALUES (2,?,datetime('now','+1 day'))", [token])
            db.execute("INSERT INTO services (id,worker_id,title,description,category,pricing_type,price,status) VALUES (1,1,'Testing Svc','Desc','testing','fixed',25,'active')")
            db.commit()
        finally:
            db.close()

        payload = {
            "title": "Website QA pass",
            "description": "Review a public website flow and provide screenshots and prioritized notes.",
            "category": "testing",
            "budget_type": "fixed",
            "budget_amount": 25,
        }
        body = json.dumps(payload)
        self.module._request_ctx.request_method = "POST"
        self.module._request_ctx.path_info = "/api/v1/jobs"
        self.module._request_ctx.query_string = ""
        self.module._request_ctx.http_authorization = f"Bearer {token}"
        self.module._request_ctx.http_x_api_key = ""
        self.module._request_ctx.stdin_data = body
        self.module._request_ctx.content_type = "application/json"
        self.module._request_ctx.content_length = str(len(body))
        self.module._request_ctx.remote_addr = "127.0.0.1"
        with contextlib.redirect_stdout(io.StringIO()) as out:
            self.module.handle_request()
        status, response = parse_cgi_output(out.getvalue())
        self.assertEqual(status, 201, response)

        db = self.module.get_db()
        try:
            notif = db.execute(
                "SELECT user_id, type, title, link FROM notifications WHERE type='job_match'"
            ).fetchone()
            self.assertIsNotNone(notif)
            self.assertEqual(notif["user_id"], 1)
            self.assertEqual(notif["link"], f"#/jobs/{response['id']}")
        finally:
            db.close()

    def test_admin_marketplace_ops_requires_admin(self):
        self.module._request_ctx.request_method = "GET"
        self.module._request_ctx.path_info = "/api/v1/admin/marketplace-ops"
        self.module._request_ctx.query_string = ""
        self.module._request_ctx.http_authorization = ""
        self.module._request_ctx.http_x_api_key = ""
        self.module._request_ctx.stdin_data = ""
        self.module._request_ctx.content_type = ""
        self.module._request_ctx.content_length = "0"
        self.module._request_ctx.remote_addr = "127.0.0.1"
        with contextlib.redirect_stdout(io.StringIO()) as out:
            self.module.handle_request()
        status, body = parse_cgi_output(out.getvalue())
        self.assertEqual(status, 403, body)

    def test_admin_marketplace_ops_surfaces_job_notifications_and_applications(self):
        db = self.module.get_db()
        token = "tok-admin"
        try:
            db.execute("INSERT INTO users (id,email,password_hash,name,is_admin) VALUES (1,'admin@example.com','x','Admin',1)")
            db.execute("INSERT INTO users (id,email,password_hash,name) VALUES (2,'worker@example.com','x','Worker')")
            db.execute("INSERT INTO worker_profiles (user_id) VALUES (2)")
            db.execute("INSERT INTO users (id,email,password_hash,name) VALUES (3,'employer@example.com','x','Employer')")
            db.execute("INSERT INTO employer_profiles (user_id) VALUES (3)")
            db.execute("INSERT INTO sessions (user_id,token,expires_at) VALUES (1,?,datetime('now','+1 day'))", [token])
            db.execute("INSERT INTO services (id,worker_id,title,description,category,pricing_type,price,status) VALUES (1,2,'Testing Svc','Desc','testing','fixed',25,'active')")
            db.execute("INSERT INTO jobs (id,employer_id,title,description,category,budget_type,budget_amount,status) VALUES (7,3,'QA Job','Desc','testing','fixed',25,'open')")
            db.execute("INSERT INTO notifications (user_id,type,title,message,link,is_read) VALUES (2,'job_match','New job','Msg','#/jobs/7',0)")
            db.execute("INSERT INTO applications (job_id,worker_id,cover_message,status) VALUES (7,2,'I can help','pending')")
            db.commit()
        finally:
            db.close()

        self.module._request_ctx.request_method = "GET"
        self.module._request_ctx.path_info = "/api/v1/admin/marketplace-ops"
        self.module._request_ctx.query_string = "limit=5"
        self.module._request_ctx.http_authorization = f"Bearer {token}"
        self.module._request_ctx.http_x_api_key = ""
        self.module._request_ctx.stdin_data = ""
        self.module._request_ctx.content_type = ""
        self.module._request_ctx.content_length = "0"
        self.module._request_ctx.remote_addr = "127.0.0.1"
        with contextlib.redirect_stdout(io.StringIO()) as out:
            self.module.handle_request()
        status, body = parse_cgi_output(out.getvalue())
        self.assertEqual(status, 200, body)
        self.assertEqual(body["summary"]["open_jobs"], 1)
        self.assertEqual(body["summary"]["job_match_notifications_24h"], 1)
        self.assertEqual(body["summary"]["stuck_open_jobs"], 0)
        job = body["recent_jobs"][0]
        self.assertEqual(job["id"], 7)
        self.assertEqual(job["application_count"], 1)
        self.assertEqual(job["job_match_notification_count"], 1)
        self.assertEqual(job["job_match_unread_count"], 1)
        self.assertEqual(job["activation_funnel"]["notifications_sent"], 1)
        self.assertEqual(job["activation_funnel"]["notifications_unread"], 1)
        self.assertEqual(job["activation_funnel"]["applications_submitted"], 1)
        self.assertEqual(job["activation_funnel"]["status"], "has_applications")
        self.assertEqual(job["job_match_notifications"][0]["user_id"], 2)
        self.assertEqual(job["applications"][0]["worker_id"], 2)
        self.assertEqual(job["matching_workers"][0]["worker_id"], 2)
        self.assertEqual(body["stuck_jobs"], [])

    def test_admin_can_rotate_user_password_without_exposing_secret(self):
        db = self.module.get_db()
        try:
            db.execute("INSERT INTO users (id,email,password_hash,name,is_admin) VALUES (1,'admin@example.com',?,'Admin',1)", [self.module.hash_password('AdminPassword123!')])
            db.execute("INSERT INTO users (id,email,password_hash,name,is_admin) VALUES (2,'ops@example.com',?,'Ops',0)", [self.module.hash_password('OldPassword123!')])
            db.execute("INSERT INTO sessions (user_id,token,expires_at) VALUES (1,'admin-token',datetime('now','+1 day'))")
            db.commit()
        finally:
            db.close()

        new_password = 'NewTemporaryPassword123!'
        self.module._request_ctx.request_method = "PUT"
        self.module._request_ctx.path_info = "/admin/users/2/password"
        self.module._request_ctx.query_string = ""
        self.module._request_ctx.http_authorization = "Bearer admin-token"
        self.module._request_ctx.stdin_data = json.dumps({"password": new_password})
        self.module._request_ctx.content_type = "application/json"
        self.module._request_ctx.content_length = str(len(self.module._request_ctx.stdin_data))
        self.module._request_ctx.remote_addr = "127.0.0.1"
        with contextlib.redirect_stdout(io.StringIO()) as out:
            self.module.handle_request()
        status, body = parse_cgi_output(out.getvalue())
        self.assertEqual(status, 200, body)
        self.assertTrue(body["ok"])
        self.assertNotIn(new_password, json.dumps(body))

        db = self.module.get_db()
        try:
            user = db.execute("SELECT password_hash FROM users WHERE id=2").fetchone()
            self.assertTrue(self.module.verify_password(new_password, user['password_hash']))
            audit = db.execute("SELECT action, details FROM audit_log WHERE entity_type='user' AND entity_id=2").fetchone()
            self.assertEqual(audit['action'], 'admin_rotate_user_password')
            self.assertNotIn(new_password, audit['details'] or '')
        finally:
            db.close()

    def test_employer_payment_setup_handles_stripe_setup_intent(self):
        text = (REPO_ROOT / "frontend/index.html").read_text(encoding="utf-8", errors="ignore")
        required = [
            "function loadStripeJs()",
            "https://js.stripe.com/v3/",
            "showEmployerSetupIntentModal",
            "stripe.confirmCardSetup",
            "/payments/confirm-setup-employer",
            "payment_setup_completed",
            "No job is hired by this step alone",
        ]
        missing = [snippet for snippet in required if snippet not in text]
        self.assertEqual(missing, [])

    def test_admin_application_pipeline_requires_admin(self):
        self.module._request_ctx.request_method = "GET"
        self.module._request_ctx.path_info = "/api/v1/admin/application-pipeline"
        self.module._request_ctx.query_string = ""
        self.module._request_ctx.http_authorization = ""
        self.module._request_ctx.http_x_api_key = ""
        self.module._request_ctx.stdin_data = ""
        self.module._request_ctx.content_type = ""
        self.module._request_ctx.content_length = "0"
        self.module._request_ctx.remote_addr = "127.0.0.1"
        with contextlib.redirect_stdout(io.StringIO()) as out:
            self.module.handle_request()
        status, body = parse_cgi_output(out.getvalue())
        self.assertEqual(status, 403, body)

    def test_admin_application_pipeline_surfaces_quality_triage(self):
        db = self.module.get_db()
        token = "tok-admin"
        try:
            db.execute("INSERT INTO users (id,email,password_hash,name,is_admin) VALUES (1,'admin@example.com','x','Admin',1)")
            db.execute("INSERT INTO users (id,email,password_hash,name) VALUES (2,'worker@example.com','x','Worker')")
            db.execute("INSERT INTO users (id,email,password_hash,name) VALUES (3,'employer@example.com','x','Employer')")
            db.execute("INSERT INTO sessions (user_id,token,expires_at) VALUES (1,?,datetime('now','+1 day'))", [token])
            db.execute("INSERT INTO jobs (id,employer_id,title,description,category,budget_type,budget_amount,status) VALUES (7,3,'QA Job','Desc','testing','fixed',25,'open')")
            cover = "I can deliver this today with screenshots, a short issue list, and prioritized notes based on testing the signup flow on desktop and mobile."
            db.execute("INSERT INTO applications (job_id,worker_id,cover_message,portfolio_url,status) VALUES (7,2,?,'https://example.com/proof','pending')", [cover])
            db.commit()
        finally:
            db.close()

        self.module._request_ctx.request_method = "GET"
        self.module._request_ctx.path_info = "/api/v1/admin/application-pipeline"
        self.module._request_ctx.query_string = "limit=10"
        self.module._request_ctx.http_authorization = f"Bearer {token}"
        self.module._request_ctx.http_x_api_key = ""
        self.module._request_ctx.stdin_data = ""
        self.module._request_ctx.content_type = ""
        self.module._request_ctx.content_length = "0"
        self.module._request_ctx.remote_addr = "127.0.0.1"
        with contextlib.redirect_stdout(io.StringIO()) as out:
            self.module.handle_request()
        status, body = parse_cgi_output(out.getvalue())
        self.assertEqual(status, 200, body)
        self.assertEqual(body["summary"]["total_recent_applications"], 1)
        self.assertEqual(body["summary"]["strong_candidates"], 1)
        app = body["applications"][0]
        self.assertEqual(app["triage_status"], "strong_candidate")
        self.assertIn("specific_cover_message", app["quality_flags"])
        self.assertIn("portfolio_or_proof_url", app["quality_flags"])
        self.assertIn("deliverable_or_timing_signal", app["quality_flags"])

    def test_admin_worker_activation_notifications_requires_admin(self):
        self.module._request_ctx.request_method = "POST"
        self.module._request_ctx.path_info = "/api/v1/admin/worker-activation-notifications"
        self.module._request_ctx.query_string = ""
        self.module._request_ctx.http_authorization = ""
        self.module._request_ctx.http_x_api_key = ""
        self.module._request_ctx.stdin_data = json.dumps({"user_ids": [2], "title": "Paid jobs are live", "message": "Apply through the marketplace."})
        self.module._request_ctx.content_type = "application/json"
        self.module._request_ctx.content_length = str(len(self.module._request_ctx.stdin_data))
        self.module._request_ctx.remote_addr = "127.0.0.1"
        with contextlib.redirect_stdout(io.StringIO()) as out:
            self.module.handle_request()
        status, body = parse_cgi_output(out.getvalue())
        self.assertEqual(status, 403, body)

    def test_admin_worker_activation_notifications_create_in_app_notifications(self):
        db = self.module.get_db()
        token = "tok-admin"
        try:
            db.execute("INSERT INTO users (id,email,password_hash,name,is_admin) VALUES (1,'admin@example.com','x','Admin',1)")
            db.execute("INSERT INTO users (id,email,password_hash,name) VALUES (2,'worker@example.com','x','Worker')")
            db.execute("INSERT INTO users (id,email,password_hash,name) VALUES (3,'worker2@example.com','x','Worker 2')")
            db.execute("INSERT INTO sessions (user_id,token,expires_at) VALUES (1,?,datetime('now','+1 day'))", [token])
            db.commit()
        finally:
            db.close()

        body = {
            "user_ids": [2, 2, 3],
            "title": "Paid jobs are live",
            "message": "Please apply directly through the marketplace jobs page.",
            "link": "#/jobs",
        }
        self.module._request_ctx.request_method = "POST"
        self.module._request_ctx.path_info = "/api/v1/admin/worker-activation-notifications"
        self.module._request_ctx.query_string = ""
        self.module._request_ctx.http_authorization = f"Bearer {token}"
        self.module._request_ctx.http_x_api_key = ""
        self.module._request_ctx.stdin_data = json.dumps(body)
        self.module._request_ctx.content_type = "application/json"
        self.module._request_ctx.content_length = str(len(self.module._request_ctx.stdin_data))
        self.module._request_ctx.remote_addr = "127.0.0.1"
        with contextlib.redirect_stdout(io.StringIO()) as out:
            self.module.handle_request()
        status, response = parse_cgi_output(out.getvalue())
        self.assertEqual(status, 200, response)
        self.assertEqual(response["sent_user_ids"], [2, 3])
        db = self.module.get_db()
        try:
            rows = db.execute("SELECT user_id,type,title,message,link FROM notifications WHERE type='worker_activation' ORDER BY user_id").fetchall()
            self.assertEqual(len(rows), 2)
            self.assertEqual([row["user_id"] for row in rows], [2, 3])
            self.assertEqual(rows[0]["title"], "Paid jobs are live")
            self.assertEqual(rows[0]["link"], "#/jobs")
        finally:
            db.close()

    def test_jobs_page_highlights_worker_activation_path(self):
        text = (REPO_ROOT / "frontend/index.html").read_text(encoding="utf-8", errors="ignore")
        for snippet in [
            "New paid jobs",
            "Apply directly through GoHireHumans",
            "Newest open jobs are shown first",
            "worker_jobs_apply_cta_click",
            "worker_job_card_apply_click",
            "Apply now",
            "const sortedJobs = [...jobs].sort",
        ]:
            self.assertIn(snippet, text)

    def test_market_discovery_pages_capture_open_ended_demand(self):
        required = {
            "frontend/index.html": [
                "lp-market-discovery",
                "homepage_request_any_task_click",
                "homepage_task_ideas_click",
                "What do you need a human to do?",
            ],
            "frontend/ideas.html": [
                "What should people hire humans for?",
                "task_idea_interest_vote",
                "task_idea_draft_click",
                "Request this task",
            ],
            "frontend/request-any-task.html": [
                "Describe any task you need a human to do",
                "request_any_task_draft_created",
                "Create draft job",
                "Draft only",
            ],
            "frontend/sitemap.xml": [
                "https://www.gohirehumans.com/ideas.html",
                "https://www.gohirehumans.com/request-any-task.html",
            ],
            "frontend/llms.txt": [
                "Market Discovery Entry Points",
                "request-any-task.html",
                "ideas.html",
            ],
        }
        missing = {}
        for rel, snippets in required.items():
            text = (REPO_ROOT / rel).read_text(encoding="utf-8", errors="ignore")
            misses = [s for s in snippets if s not in text]
            if misses:
                missing[rel] = misses
        self.assertEqual(missing, {})

    def test_high_intent_seo_pages_feed_starter_offer_funnel(self):
        required = {
            "frontend/use-cases/ai-output-fact-checking.html": ["Hire a human to fact-check AI output", "seo_use_case_draft_click", "AI-output fact-checking review"],
            "frontend/use-cases/human-review-for-chatbot-responses.html": ["Human review for chatbot", "seo_use_case_draft_click"],
            "frontend/use-cases/hire-human-to-test-signup-flow.html": ["Hire a human to test your signup flow", "Website signup flow QA quick check"],
            "frontend/use-cases/source-checking-for-ai-research.html": ["Source checking for AI-assisted research", "Source-check AI-assisted research"],
            "frontend/use-cases/ai-agent-human-in-the-loop-tasks.html": ["Human fallback tasks for AI agents", "Human-in-the-loop verification task"],
            "frontend/use-cases/index.html": ["High-intent starter use cases", "AI Output Fact Checking"],
            "frontend/sitemap.xml": ["ai-output-fact-checking.html", "human-review-for-chatbot-responses.html", "hire-human-to-test-signup-flow.html"],
            "frontend/llms.txt": ["High-Intent Use Case Pages", "ai-agent-human-in-the-loop-tasks.html"],
            "frontend/blog/gig-economy-statistics-2026.html": ["ghh-starter-offers-internal-link", "blog_starter_offers_click"],
        }
        missing = {}
        for rel, snippets in required.items():
            text = (REPO_ROOT / rel).read_text(encoding="utf-8", errors="ignore")
            misses = [s for s in snippets if s not in text]
            if misses:
                missing[rel] = misses
        self.assertEqual(missing, {})

    def test_first_orders_conversion_infrastructure_is_discoverable(self):
        required = {
            "frontend/index.html": [
                "lp-first-orders-proof",
                "homepage_starter_offers_click",
                "homepage_sample_deliverables_click",
                "What a strong application says",
                "job_application_cover_focus",
            ],
            "frontend/starter-offers.html": [
                "Start with a small task that can actually get done.",
                "starter_offer_draft_click",
                "Website QA quick check",
                "AI-output trust review",
                "Lead research starter list",
            ],
            "frontend/examples/sample-deliverables.html": [
                "Sample website QA report",
                "Sample AI-output review scorecard",
                "Sample lead research spreadsheet preview",
                "sample_deliverable_cta_click",
            ],
            "frontend/sitemap.xml": [
                "https://www.gohirehumans.com/starter-offers.html",
                "https://www.gohirehumans.com/examples/sample-deliverables.html",
            ],
            "frontend/llms.txt": [
                "First Completed Orders Entry Points",
                "starter-offers.html",
                "sample-deliverables.html",
            ],
        }
        missing = {}
        for rel, snippets in required.items():
            text = (REPO_ROOT / rel).read_text(encoding="utf-8", errors="ignore")
            misses = [s for s in snippets if s not in text]
            if misses:
                missing[rel] = misses
        self.assertEqual(missing, {})

    def test_growth_activation_pages_and_homepage_proof_are_discoverable(self):
        required = {
            "frontend/index.html": [
                "lp-marketplace-proof",
                "homepage_live_proof_click",
                "homepage_first_task_page_click",
                "job_apply_form_opened",
                "job_application_started",
                "job_application_submitted",
            ],
            "frontend/post-a-small-task.html": [
                "Post a small human task from $25",
                "first_task_template_click",
                "Draft website QA task",
                "Draft AI review task",
                "Draft lead research task",
            ],
            "frontend/earn/open-paid-tasks.html": [
                "Find open paid tasks you can apply to today",
                "worker_open_tasks_click",
                "What a strong application says",
            ],
            "frontend/sitemap.xml": [
                "https://www.gohirehumans.com/post-a-small-task.html",
                "https://www.gohirehumans.com/earn/open-paid-tasks.html",
            ],
            "frontend/llms.txt": [
                "Conversion Entry Points",
                "post-a-small-task.html",
                "earn/open-paid-tasks.html",
            ],
        }
        missing = {}
        for rel, snippets in required.items():
            text = (REPO_ROOT / rel).read_text(encoding="utf-8", errors="ignore")
            misses = [s for s in snippets if s not in text]
            if misses:
                missing[rel] = misses
        self.assertEqual(missing, {})

    def test_owner_admin_bootstrap_promotes_enzo_account(self):
        db = self.module.get_db()
        try:
            db.execute("INSERT INTO users (email,password_hash,name,is_admin,is_active,is_suspended,is_banned) VALUES ('enzo@profilesearch.com','old','Enzo',0,0,1,1)")
            db.commit()
        finally:
            db.close()

        self.module.init_db()

        db = self.module.get_db()
        try:
            user = db.execute("SELECT email,password_hash,is_admin,is_active,is_suspended,is_banned FROM users WHERE email='enzo@profilesearch.com'").fetchone()
            self.assertIsNotNone(user)
            self.assertEqual(user["is_admin"], 1)
            self.assertEqual(user["is_active"], 1)
            self.assertEqual(user["is_suspended"], 0)
            self.assertEqual(user["is_banned"], 0)
            self.assertNotEqual(user["password_hash"], "old")
        finally:
            db.close()

    def test_public_pricing_info_uses_connector_fee_language(self):
        self.module._request_ctx.request_method = "GET"
        self.module._request_ctx.path_info = "/pricing/info"
        self.module._request_ctx.query_string = ""
        self.module._request_ctx.http_authorization = ""
        self.module._request_ctx.http_x_api_key = ""
        self.module._request_ctx.stdin_data = ""
        self.module._request_ctx.content_type = ""
        self.module._request_ctx.content_length = "0"
        self.module._request_ctx.remote_addr = "127.0.0.1"
        with contextlib.redirect_stdout(io.StringIO()) as out:
            self.module.handle_request()
        status, body = parse_cgi_output(out.getvalue())
        self.assertEqual(status, 200, body)
        self.assertEqual(body["service_fee_rate"], self.module.SERVICE_FEE_RATE)
        self.assertIn("Stripe processing plus a 1% GoHireHumans fee", body["description"])
        self.assertIn("Workers receive the listed payout", body["description"])
        self.assertFalse(body["escrow"])
        self.assertNotIn("4%", body["description"])
        self.assertNotIn("escrow", body["description"].lower())

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

    def test_static_top_tabs_use_landing_nav_chrome(self):
        static_tabs = {
            "frontend/ai-integration.html": '<a class="lp-nav-link lp-nav-link-active" href="/ai-integration.html">Agent Guide</a>',
            "frontend/use-cases/index.html": '<a class="lp-nav-link lp-nav-link-active" href="/use-cases/">Use Cases</a>',
            "frontend/about.html": '<a class="lp-nav-link lp-nav-link-active" href="/about.html">About</a>',
            "frontend/faq.html": '<a class="lp-nav-link lp-nav-link-active" href="/faq.html">FAQ</a>',
        }
        failures = self._assert_shared_landing_nav(static_tabs)
        self.assertEqual(failures, [])

    def test_core_static_pages_use_landing_nav_chrome(self):
        core_pages = {
            "frontend/404.html": None,
            "frontend/api-docs.html": None,
            "frontend/how-it-works.html": None,
            "frontend/pricing.html": None,
            "frontend/services.html": None,
            "frontend/trust-safety.html": None,
        }
        failures = self._assert_shared_landing_nav(core_pages)
        self.assertEqual(failures, [])

    def test_use_case_detail_pages_keep_use_cases_nav_active(self):
        use_case_pages = {
            str(path.relative_to(REPO_ROOT)): '<a class="lp-nav-link lp-nav-link-active" href="/use-cases/">Use Cases</a>'
            for path in (REPO_ROOT / "frontend/use-cases").glob("*.html")
            if path.name != "index.html"
        }
        self.assertGreater(len(use_case_pages), 0)
        failures = self._assert_shared_landing_nav(use_case_pages)
        self.assertEqual(failures, [])

    def test_public_nav_active_state_uses_light_pill_for_all_tabs(self):
        css = (REPO_ROOT / "frontend/style.css").read_text(encoding="utf-8", errors="ignore")
        required_snippets = [
            ".lp-nav-link.lp-nav-link-active,",
            ".lp-nav-link.lp-nav-link-active:hover,",
            ".lp-mobile-link.lp-nav-link-active,",
            "color: #0d7377 !important;",
            "background: #e6f3f3 !important;",
            "text-decoration: none !important;",
        ]
        missing = [snippet for snippet in required_snippets if snippet not in css]
        self.assertEqual(missing, [])

    def test_sitemapped_html_pages_use_single_canonical_public_nav(self):
        expected_labels = [
            "GoHireHumans",
            "Marketplace",
            "Open Jobs",
            "For Agents",
            "Agent Guide",
            "Use Cases",
            "About",
            "FAQ",
        ]
        failures = []
        for rel in self._sitemapped_html_pages():
            if rel == "frontend/index.html":
                continue
            text = (REPO_ROOT / rel).read_text(encoding="utf-8", errors="ignore")
            nav = self._first_nav(text)
            labels = self._nav_labels(nav)
            missing = []
            if '<link rel="stylesheet" href="/style.css?v=20260526-nav-consistency">' not in text:
                missing.append("cache-busted shared stylesheet")
            if text.count('<div class="lp-nav-wrap">') != 1:
                missing.append("exactly one shared nav wrapper")
            if text.count('function toggleMobileMenu()') != 1:
                missing.append("exactly one mobile menu toggle")
            if '<nav class="lp-nav" aria-label="Main navigation">' not in nav:
                missing.append("first nav uses canonical lp-nav + aria label")
            if labels[:8] != expected_labels:
                missing.append(f"top nav labels {labels[:8]!r}")
            if '<nav class="nav"' in nav or 'class="header-nav"' in nav:
                missing.append("legacy top nav class removed")
            if missing:
                failures.append({"file": rel, "missing": missing})
        self.assertEqual(failures, [])

    def test_homepage_public_nav_template_keeps_desktop_and_mobile_active_states(self):
        text = (REPO_ROOT / "frontend/index.html").read_text(encoding="utf-8", errors="ignore")
        snippets = [
            '<nav class="lp-nav" aria-label="Main navigation">',
            "lp-nav-link${activePage === l.key ? ' lp-nav-link-active' : ''}",
            "lp-mobile-link${activePage === l.key ? ' lp-nav-link-active' : ''}",
            '<link rel="stylesheet" href="/style.css?v=20260526-nav-consistency">',
            '<link rel="preload" href="/style.css?v=20260526-nav-consistency" as="style">',
        ]
        missing = [snippet for snippet in snippets if snippet not in text]
        self.assertEqual(missing, [])

    def _sitemapped_html_pages(self):
        sitemap = (REPO_ROOT / "frontend/sitemap.xml").read_text(encoding="utf-8", errors="ignore")
        pages = set()
        for loc in re.findall(r"<loc>https://www\.gohirehumans\.com([^<]*)</loc>", sitemap):
            if loc in ("", "/"):
                rel = "frontend/index.html"
            elif loc.endswith("/"):
                rel = f"frontend{loc}index.html"
            elif loc.endswith(".html"):
                rel = f"frontend{loc}"
            else:
                continue
            if (REPO_ROOT / rel).exists():
                pages.add(rel)
        pages.add("frontend/404.html")
        return sorted(pages)

    def _first_nav(self, text):
        match = re.search(r"<nav\b[^>]*>.*?</nav>", text, flags=re.S | re.I)
        return match.group(0) if match else ""

    def _nav_labels(self, nav):
        labels = []
        for anchor in re.findall(r"<a\b[^>]*>(.*?)</a>", nav, flags=re.S | re.I):
            label = re.sub(r"<[^>]+>", " ", anchor)
            label = " ".join(label.split())
            if label:
                labels.append(label)
        return labels

    def _assert_shared_landing_nav(self, pages):
        shared_snippets = [
            '<link rel="stylesheet" href="/style.css?v=20260526-nav-consistency">',
            '<div class="lp-nav-wrap">',
            '<nav class="lp-nav" aria-label="Main navigation">',
            '<a class="lp-nav-link" href="/#/services">Marketplace</a>',
            '<a class="lp-nav-link" href="/#/jobs">Open Jobs</a>',
            '<a class="lp-nav-link" href="/#/ai-employers">For Agents</a>',
            '<a class="btn btn-primary btn-sm" href="/#/register">Get started</a>',
            'function toggleMobileMenu()',
        ]
        failures = []
        for rel, active_snippet in pages.items():
            text = (REPO_ROOT / rel).read_text(encoding="utf-8", errors="ignore")
            missing = [snippet for snippet in shared_snippets if snippet not in text]
            if active_snippet and active_snippet not in text:
                missing.append(active_snippet)
            if '<nav class="nav"' in text:
                missing.append('old <nav class="nav"> removed')
            if '<header class="header">' in text and rel == "frontend/404.html":
                missing.append('old 404 header removed')
            if text.count('<div class="lp-nav-wrap">') != 1:
                missing.append('exactly one shared nav wrapper')
            if text.count('function toggleMobileMenu()') != 1:
                missing.append('exactly one mobile menu toggle')
            if missing:
                failures.append({"file": rel, "missing": missing})
        return failures

    def test_no_known_broken_assets_links_or_payment_copy_typos(self):
        bad_terms = [
            "hiw-step2-payment hold.png",
            "best-freelance-platforms-payment hold.html",
            "Payment Payments",
            "Payment payments",
            "payment payment",
            "payment hold payment",
            "approval the process",
            "Platform fee (4%)",
        ]
        hits = []
        for path in (REPO_ROOT / "frontend").rglob("*.html"):
            text = path.read_text(encoding="utf-8", errors="ignore")
            for term in bad_terms:
                if term in text:
                    hits.append(f"{path.relative_to(REPO_ROOT)}: {term}")
        self.assertEqual(hits, [])

    def test_high_intent_pricing_pages_use_connector_pricing_framing(self):
        required_phrase_pages = [
            "frontend/pricing.html",
            "frontend/tools/fee-calculator.html",
            "frontend/instagram.html",
            "frontend/stats.html",
            "frontend/faq.html",
            "frontend/trust-safety.html",
            "frontend/llms.txt",
        ]
        pricing_trust_pages = required_phrase_pages + [
            "frontend/compare.html",
            "frontend/press.html",
            "frontend/services.html",
            "frontend/tools/freelance-fee-calculator.html",
            "frontend/tools/are-you-overpaying.html",
            "frontend/blog/freelancers-switching-lower-fee-platforms.html",
            "frontend/blog/alternatives-to-fiverr.html",
            "frontend/blog/alternatives-to-freelancer.html",
            "frontend/blog/alternatives-to-toptal.html",
            "frontend/blog/alternatives-to-upwork.html",
            "frontend/blog/best-freelance-platforms-escrow.html",
            "frontend/blog/fiverr-vs-upwork-vs-gohirehumans.html",
            "frontend/blog/where-to-list-services-online.html",
            "frontend/blog/freelance-vs-full-time-2026.html",
            "frontend/blog/hire-data-entry-specialist.html",
            "frontend/blog/gohirehumans-vs-fiverr.html",
            "frontend/blog/how-to-find-human-workers-ai-tasks.html",
            "frontend/hire/hire-freelance-writer.html",
            "frontend/vs/fiverr.html",
            "frontend/vs/upwork.html",
            "frontend/vs/toptal.html",
            "frontend/vs/freelancer.html",
        ]
        forbidden_claims = [
            "4% fee",
            "4% employer fee",
            "4% service fee",
            "4% platform fee",
            "platform fee: 4%",
            "flat 4% pricing",
            "gohirehumans takes <strong>$40</strong>",
            "gohirehumans takes <strong>$400",
            "gohirehumans takes $0.80",
            "verified professionals",
            "verified profiles",
            "verified human",
            "all verified pros",
            "all workers",
            "accuracy guarantees available",
            "payment hold",
            "payment protection",
            "identity verification is included",
            "guaranteed completion",
            "escrow-protected",
            "risk-free",
            "platform arbitration",
            "protects every transaction",
            "process payments programmatically",
            "process payment processing programmatically",
            "hire humans through natural language commands",
            "resolves disputes",
            "verifies every worker",
            "requires every worker",
            "all professionals must verify",
            "every task on gohirehumans is backed by payment flow",
            "eliminates the risk of non-payment",
            "bank-grade security",
            "instant payouts",
            "every transaction is payment-supported",
            "payment systems that hold funds",
            "submit to the identity and background verification process",
            "submit to the verification process",
            "complete identity verification",
            "verified seo professionals",
            "background screening should be mandatory",
            "checks all these boxes with identity",
        ]
        forbidden_patterns = [
            re.compile(r"gohirehumans[^\n<>]{0,180}4%", re.IGNORECASE),
            re.compile(r"4%[^\n<>]{0,180}gohirehumans", re.IGNORECASE),
            re.compile(r"takes \$4", re.IGNORECASE),
            re.compile(r"gohirehumans[^\n<>]{0,220}(mandatory identity|requires identity|all freelancers|all workers)", re.IGNORECASE),
            re.compile(r"requires identity verification for (all freelancers|all workers)", re.IGNORECASE),
            re.compile(r"(all|every)\s+[^.]{0,80}\s+(verified|identity verified)", re.IGNORECASE),
            re.compile(r"(payment flow|payment-supported|payment processing support).*?(every transaction|every task|mandatory)", re.IGNORECASE),
            re.compile(r"(hire|create)\s+[^.]{0,80}\s+(humans|professionals|workers)\s+[^.]{0,120}\s+(programmatically|autonomously)", re.IGNORECASE),
            re.compile(r"(autonomous ai agents|ai agents)\s+[^.]{0,200}\s+(process payments|approve payment|without human)", re.IGNORECASE),
        ]
        failures = []
        for rel in required_phrase_pages:
            text = (REPO_ROOT / rel).read_text(encoding="utf-8", errors="ignore")
            for required in [
                "Workers receive the listed payout",
                "Stripe processing plus a 1% GoHireHumans fee",
            ]:
                if required not in text:
                    failures.append(f"{rel}: missing {required}")
        for rel in pricing_trust_pages:
            text = (REPO_ROOT / rel).read_text(encoding="utf-8", errors="ignore")
            lower = text.lower()
            for claim in forbidden_claims:
                if claim in lower:
                    failures.append(f"{rel}: forbidden {claim}")
            for pattern in forbidden_patterns:
                match = pattern.search(text)
                if match:
                    failures.append(f"{rel}: forbidden pattern {pattern.pattern}: {match.group(0)}")
        self.assertEqual(failures, [])

    def test_agent_surfaces_keep_spend_and_trust_claims_owner_authorized(self):
        high_visibility_pages = [
            "frontend/ai-integration.html",
            "frontend/api-docs.html",
            "frontend/agent-onboarding.html",
            "frontend/faq.html",
            "frontend/press.html",
            "frontend/hire/hire-ai-agent.html",
            "frontend/blog/mcp-for-marketplaces.html",
            "frontend/blog/how-to-hire-ai-agent.html",
            "frontend/blog/gohirehumans-vs-fiverr.html",
            "frontend/blog/ai-agent-marketplace-guide.html",
            "frontend/blog/hire-human-for-ai-tasks.html",
            "frontend/blog/how-to-find-human-workers-ai-tasks.html",
            "frontend/blog/how-to-hire-ai-agents-safely.html",
            "frontend/blog/on-demand-workforce-platform.html",
            "frontend/blog/freelancers-switching-lower-fee-platforms.html",
            "frontend/blog/alternatives-to-upwork.html",
            "frontend/trust-safety.html",
        ]
        forbidden_phrases = [
            "without human oversight",
            "without human intervention",
            "without human involvement",
            "no human in the loop required",
            "No human needs to manage",
            "requires zero human involvement",
            "autonomously browse services, create tasks, hire humans, manage milestones, and process payments",
            "autonomously browse services, post jobs, fund payment flow, and approve work",
            "autonomously browse services, post jobs",
            "browse services, post jobs, hire humans, and process payments programmatically",
            "browse services, post jobs, hire humans, and process payment processing programmatically",
            "hire workers through natural language commands",
            "fund payment flow, and approve work",
            "release payment processing",
            "release payment when the work is complete",
            "release payment on completion",
            "release payment upon task completion",
            "Your funds are always protected until you release them",
            "All professionals who apply",
            "Only approved professionals",
            '<div class="stat-num">4%</div><div class="stat-label">Employer Fee</div>',
        ]
        failures = []
        for rel in high_visibility_pages:
            text = (REPO_ROOT / rel).read_text(encoding="utf-8", errors="ignore")
            for phrase in forbidden_phrases:
                if phrase in text:
                    failures.append(f"{rel}: forbidden {phrase}")
        self.assertEqual(failures, [])

        required_snippets = {
            "frontend/ai-integration.html": [
                "account-owner approval before any spend",
                "listing and payment connector, not as escrow, a guarantor, or an arbitrator",
            ],
            "frontend/api-docs.html": [
                "account-owner authorization",
                "use connector/payment-processing language",
            ],
            "frontend/press.html": [
                "prepare approved workflows",
                '<div class="stat-num">1%</div><div class="stat-label">GoHireHumans Fee</div>',
            ],
            "frontend/blog/mcp-for-marketplaces.html": [
                "account-owner authorization before spend or hiring actions",
                "scoped credentials, and audit logs",
            ],
            "frontend/hire/hire-ai-agent.html": [
                "owner-approved scopes",
                "Review the specific provider, scope, and deliverables before approving paid work",
            ],
            "frontend/blog/hire-human-for-ai-tasks.html": [
                "account-owner approved scopes",
                "Worker profiles may display identity, skill, review, and history signals where available",
            ],
        }
        missing = []
        for rel, snippets in required_snippets.items():
            text = (REPO_ROOT / rel).read_text(encoding="utf-8", errors="ignore")
            for snippet in snippets:
                if snippet not in text:
                    missing.append(f"{rel}: missing {snippet}")
        self.assertEqual(missing, [])

    def test_public_pages_do_not_reintroduce_stale_payment_or_pricing_claims(self):
        html_files = sorted((REPO_ROOT / "frontend").rglob("*.html"))
        forbidden_phrases = [
            "autonomously browse services",
            "fund the payment flow",
            "release payment",
            "releasing payment",
            "Your money stays protected",
            "money stays protected",
            "protected until you approve",
            "payment protection",
            "protects every transaction",
            "protected at every step",
            "fund-escrow",
            "fund escrow",
            "quality guarantees",
            "let your AI agent hire for you",
            "programmatic job posting, hiring, and payment processing",
            "GoHireHumans identity verification adds",
            "platforms with identity verification and payment processing support like GoHireHumans",
            "order_123",
            '"owner_approved"',
            '"name": "Background Check"',
            '"name": "Skills Screening"',
            "workers are verified",
            "professionals are verified",
            "every professional on GoHireHumans is screened",
            "all professionals who apply",
            "only approved professionals",
            "4% buyer-side service fee",
            "pay just 4%",
            "fees (4%)",
            "Service fee 4%",
        ]
        stale_four_percent_patterns = [
            re.compile(r"gohirehumans.{0,240}(?<![\d.])4%(?!\d)", re.IGNORECASE),
            re.compile(r"(?<![\d.])4%(?!\d).{0,240}gohirehumans", re.IGNORECASE),
        ]
        failures = []
        for path in html_files:
            rel = str(path.relative_to(REPO_ROOT))
            text = path.read_text(encoding="utf-8", errors="ignore")
            rendered = re.sub(r"<[^>]+>", " ", text)
            rendered = re.sub(r"\s+", " ", rendered)
            lower_rendered = rendered.lower()
            for phrase in forbidden_phrases:
                if phrase.lower() in lower_rendered:
                    failures.append(f"{rel}: forbidden phrase {phrase}")
            for pattern in stale_four_percent_patterns:
                match = pattern.search(rendered)
                if match:
                    failures.append(f"{rel}: stale GoHireHumans 4% pricing context: {match.group(0)}")
        self.assertEqual(failures, [])

    def test_homepage_has_low_risk_funnel_analytics_events(self):
        text = (REPO_ROOT / "frontend/index.html").read_text(encoding="utf-8", errors="ignore")
        required_snippets = [
            "function trackEvent(eventName, params = {})",
            "gtag('event', eventName, params)",
            "function trackRecommendedEvent(eventName, params = {})",
            "function trackConfiguredKeyEvent(eventName, params = {})",
            "function trackSpaPageView(path)",
            "send_page_view: false",
            "page_path: pagePath",
            "trackSpaPageView(path)",
            "sign_up",
            "generate_lead",
            "function searchHeroServices()",
            "hero_search_submit",
            "post_task_cta_click",
            "browse_humans_cta_click",
            "agent_integration_cta_click",
            "earn_tasks_page_click",
            "seo_template_link_click",
            "service_order_intent",
            "job_apply_intent",
            "explainer_video_play",
            "concierge_task_draft_click",
            "guided_task_intake_start",
            "guided_task_draft_created",
            "worker_route_select",
            "post_service_intent",
            "browse_relevant_jobs_intent",
            "Need the first job scoped?",
            "first_task_concierge_cta_click",
            "homepage_first_task_experiment",
        ]
        for snippet in required_snippets:
            self.assertIn(snippet, text)

    def test_homepage_tracks_instagram_bio_and_referrer_attribution(self):
        text = (REPO_ROOT / "frontend/index.html").read_text(encoding="utf-8", errors="ignore")
        for snippet in [
            "function trackSocialAttribution()",
            "params.get('utm_source')",
            "utmSource === 'instagram'",
            "referrerHost.includes('instagram.com')",
            "instagram_profile_visit",
            "attribution_method",
            "trackSocialAttribution();",
        ]:
            self.assertIn(snippet, text)

    def test_gig_economy_stats_routes_drive_by_readers_to_first_task_draft(self):
        text = (REPO_ROOT / "frontend/blog/gig-economy-statistics-2026.html").read_text(encoding="utf-8", errors="ignore")
        for snippet in [
            "Turn the data into one clear task",
            "blog_demand_capture",
            "Draft your first task",
            "first_task_blog_cta_click",
            "trackBlogCTA('qualify_lead'",
            "/#/post-job?template=website_test",
        ]:
            self.assertIn(snippet, text)

    def test_llms_txt_surfaces_first_task_and_ai_qa_entry_points(self):
        text = (REPO_ROOT / "frontend/llms.txt").read_text(encoding="utf-8", errors="ignore")
        for snippet in [
            "First task draft: https://www.gohirehumans.com/#/post-job",
            "AI human QA services: https://www.gohirehumans.com/ai-human-qa/",
            "Managed AI QA request: https://www.gohirehumans.com/request-managed-ai-qa.html",
        ]:
            self.assertIn(snippet, text)

    def test_growth_opportunity_pages_route_to_tracked_ai_qa_conversion_offer(self):
        expected_pages = {
            "frontend/hire/hire-web-developer.html": [
                "Hire Web Developers for Website Fixes, QA & Landing Pages",
                "utm_content=hire_web_developer",
            ],
            "frontend/blog/verified-freelancer-marketplace.html": [
                "Verified Freelancer Marketplace: Trust Signals to Check Before Hiring",
                "utm_content=verified_freelancer_marketplace",
            ],
            "frontend/blog/on-demand-workforce-platform.html": [
                "On-Demand Workforce Platforms for AI + Human Workflows",
                "utm_content=on_demand_workforce_platform",
            ],
            "frontend/tools/fee-calculator.html": [
                "Freelancer Fee Calculator: Workers Keep the Listed Payout",
                "utm_content=fee_calculator",
            ],
        }
        required_shared_snippets = [
            "Turn AI output into a human QA task",
            "/ai-human-qa/?utm_source=gohirehumans&utm_medium=internal_cta&utm_campaign=seo_high_impression",
            "/request-managed-ai-qa.html?utm_source=gohirehumans&utm_medium=internal_cta&utm_campaign=seo_high_impression",
            "No checkout or job is created automatically from this page",
        ]
        failures = []
        for rel, page_snippets in expected_pages.items():
            text = (REPO_ROOT / rel).read_text(encoding="utf-8", errors="ignore")
            for snippet in required_shared_snippets + page_snippets:
                if snippet not in text:
                    failures.append(f"{rel}: missing {snippet}")
        self.assertEqual(failures, [])

    def test_homepage_has_guided_agent_intake_and_earning_routes(self):
        text = (REPO_ROOT / "frontend/index.html").read_text(encoding="utf-8", errors="ignore")
        for snippet in [
            "Guided agent intake",
            "Convert a prompt into a clear marketplace listing.",
            "What needs to be done?",
            "What type of human or agent is needed?",
            "Suggested deliverable/result",
            "Suggested budget range",
            "Create draft in post-job form",
            "params.set('draft_title'",
            "query.get('draft_title')",
            "query.get('draft_description')",
            "sessionStorage.setItem('ghh_guided_task_draft'",
            "consumeStoredGuidedTaskDraft()",
            "Earning surface",
            "A place agents should check for ways to make money.",
            "Website testing",
            "Lead research",
            "AI-output review",
            "Calls",
            "Local verification",
            "Data cleanup",
            "selectWorkerRoute('website_testing'",
            "postServiceIntent()",
        ]:
            self.assertIn(snippet, text)

        guided_block = text[text.index("Guided agent intake"):text.index("Earning surface")]
        self.assertNotIn("fetch(", guided_block)
        self.assertNotIn("api(", guided_block)
        self.assertNotIn("mailto:", guided_block)
        self.assertIn("does not submit a job, contact workers, send email, or promise a match", guided_block)

    def test_homepage_has_credible_agent_marketplace_liquidity_messaging(self):
        text = (REPO_ROOT / "frontend/index.html").read_text(encoding="utf-8", errors="ignore")
        self.assertNotRegex(text, r"(?i)\bpublic beta listings\b")
        for snippet in [
            "Open tasks and services",
            "review before publishing or spending",
            "where they have authorization to transact",
            "payment processing where configured",
            "Stripe payment processing is available where checkout is configured.",
        ]:
            self.assertIn(snippet, text)

    def test_homepage_has_agent_native_task_drafts_without_automatic_outreach(self):
        text = (REPO_ROOT / "frontend/index.html").read_text(encoding="utf-8", errors="ignore")
        for snippet in [
            "Agent-ready job posts",
            "Turn messy prompts into scoped work.",
            "Structured task drafts",
            "startTaskDraft('website_test')",
            "startTaskDraft('lead_research')",
            "startTaskDraft('ai_review')",
            "concierge_task_draft_click",
            "const templateDraft = getTaskDraftTemplate(query.get('template')) || {};",
            "Nothing is submitted until you approve it.",
            "Draft only. You review and publish manually when ready.",
            "Workers receive the listed payout",
            "Employer pays Stripe processing + 1%",
        ]:
            self.assertIn(snippet, text)
        task_draft_block = text[text.index("Agent-ready job posts"):text.index("Guided agent intake")]
        self.assertNotIn("mailto:", task_draft_block)
        self.assertNotIn("fetch(", task_draft_block)
        self.assertNotIn("api(", task_draft_block)

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
            "guaranteed work",
            "verified jobs",
            "guaranteed matching",
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
            "https://www.gohirehumans.com/ai-human-qa/support-reply-human-qa.html",
            "https://www.gohirehumans.com/ai-human-qa/product-content-human-qa.html",
        ]:
            self.assertIn(loc, sitemap)

    def test_ai_citation_source_page_is_linked_safe_and_structured(self):
        slug = "ai-human-qa/ai-citation-source-verification.html"
        page = (REPO_ROOT / "frontend" / slug).read_text(encoding="utf-8")
        for snippet in [
            "AI Citation and Source Verification",
            "Build a citation QA brief",
            "links, sources, quotes, statistics, and citations are real",
            "GoHireHumans is a listing and payment connector",
            "does not guarantee perfect accuracy",
            "/ai-qa-buyer-brief.html?service=citation-check",
        ]:
            self.assertIn(snippet, page)
        for unsupported in [
            "guaranteed outcomes",
            "escrow-protected",
            "platform arbitration",
            "legal review service",
        ]:
            self.assertNotIn(unsupported, page.lower())

        marker = '<script type="application/ld+json">'
        start = page.index(marker) + len(marker)
        end = page.index("</script>", start)
        structured = json.loads(page[start:end].strip())
        self.assertEqual(structured["@type"], "Service")
        self.assertEqual(structured["name"], "AI citation and source verification")

        hub = (REPO_ROOT / "frontend/ai-human-qa/index.html").read_text(encoding="utf-8")
        services = (REPO_ROOT / "frontend/ai-qa-services.html").read_text(encoding="utf-8")
        sitemap = (REPO_ROOT / "frontend/sitemap.xml").read_text(encoding="utf-8")
        href = f"/{slug}"
        self.assertIn(href, hub)
        self.assertIn(href, services)
        self.assertIn(f"https://www.gohirehumans.com/{slug}", sitemap)

    def test_homepage_routes_to_task_templates_and_worker_earn_page(self):
        home = (REPO_ROOT / "frontend/index.html").read_text(encoding="utf-8")
        for href in [
            "/hire/website-testers.html",
            "/hire/lead-researchers.html",
            "/hire/ai-reviewers.html",
            "/earn/get-paid-for-human-tasks.html",
        ]:
            self.assertIn(href, home)
        self.assertIn("Human Task Templates on GoHireHumans", home)
        self.assertIn("seo_template_link_click", home)

    def test_hire_index_uses_safe_connector_copy(self):
        hire_index = (REPO_ROOT / "frontend/hire/index.html").read_text(encoding="utf-8")
        lower = hire_index.lower()
        for phrase in [
            "workers receive the listed payout",
            "employers pay stripe processing plus 1%",
            "website-testers.html",
            "lead-researchers.html",
            "ai-reviewers.html",
            "phone-call-help.html",
            "local-verification.html",
            "get-paid-for-human-tasks.html",
        ]:
            self.assertIn(phrase, lower)
        for unsupported in [
            "verified freelancers",
            "verified professionals",
            "4% employer fee",
            "guaranteed matching",
            "guaranteed completion",
            "escrow-protected",
            "platform arbitration",
        ]:
            self.assertNotIn(unsupported, lower)

    def test_managed_ai_qa_pilot_is_manual_concierge_not_self_serve_checkout(self):
        request_page = (REPO_ROOT / "frontend/request-managed-ai-qa.html").read_text(encoding="utf-8")
        for phrase in [
            "Manual concierge pilot",
            "No self-serve checkout",
            "no payment is collected on this page",
            "no Stripe session is created",
            "no job is automatically published",
            "You approve the quote and review plan before any reviewer starts",
            "mailto:contact@gohirehumans.com",
            "managed_ai_qa_request_click",
        ]:
            self.assertIn(phrase, request_page)
        for forbidden in ["<form", "fetch(", "/api/", "stripe.redirectToCheckout", "/payments/checkout"]:
            self.assertNotIn(forbidden, request_page)

        manual_pilot_pages = sorted({
            str(path.relative_to(REPO_ROOT))
            for pattern in ["frontend/ai-qa-*.html", "frontend/ai-human-qa/*.html"]
            for path in REPO_ROOT.glob(pattern)
        } | {
            "frontend/ai-agents-need-human-auditors.html",
            "frontend/managed-ai-qa.html",
            "frontend/request-managed-ai-qa.html",
        })
        forbidden_ctas = [
            'href="/#/post-job',
            "ai_qa_post_job_click",
            "draft_title=",
            "draft_description=",
            "stripe.redirectToCheckout",
            "/payments/checkout",
            "create a Stripe session",
        ]
        offenders = []
        for rel in manual_pilot_pages:
            text = (REPO_ROOT / rel).read_text(encoding="utf-8", errors="ignore")
            for forbidden in forbidden_ctas:
                if forbidden in text:
                    offenders.append(f"{rel}: {forbidden}")
        self.assertEqual(offenders, [])

    def test_manual_ai_qa_pilot_pages_have_current_sitemap_lastmods(self):
        sitemap = (REPO_ROOT / "frontend/sitemap.xml").read_text(encoding="utf-8")
        for loc in [
            "https://www.gohirehumans.com/ai-human-qa/",
            "https://www.gohirehumans.com/ai-qa-services.html",
            "https://www.gohirehumans.com/ai-qa-buyer-brief.html",
            "https://www.gohirehumans.com/managed-ai-qa.html",
            "https://www.gohirehumans.com/request-managed-ai-qa.html",
        ]:
            start = sitemap.index(f"<loc>{loc}</loc>")
            end = sitemap.index("</url>", start)
            block = sitemap[start:end]
            self.assertIn("<lastmod>2026-05-25</lastmod>", block, loc)

    def test_ai_qa_example_deliverables_cover_every_fixed_sku(self):
        page = (REPO_ROOT / "frontend/ai-qa-example-deliverables.html").read_text(encoding="utf-8")
        sitemap = (REPO_ROOT / "frontend/sitemap.xml").read_text(encoding="utf-8")
        for snippet in [
            "AI blog post fact-check sample",
            "AI citation and source verification sample",
            "AI support reply QA sample",
            "RAG answer groundedness sample",
            "AI-built website QA sample",
            "AI-agent work audit sample",
            "AI product content QA sample",
            "No checkout or job is created automatically from this page.",
            "does not replace professional legal, medical, financial, or compliance advice",
        ]:
            self.assertIn(snippet, page)
        for card_id in [
            "blog-fact-check-sample",
            "citation-check-sample",
            "support-reply-qa-sample",
            "rag-groundedness-sample",
            "website-qa-sample",
            "agent-work-audit-sample",
            "product-content-qa-sample",
        ]:
            self.assertIn(f'id="{card_id}"', page)
        for forbidden in [
            'href="/#/post-job',
            "draft_title=",
            "draft_description=",
            "stripe.redirectToCheckout",
            "/payments/checkout",
            "create a Stripe session",
            "guaranteed outcomes",
            "platform arbitration",
        ]:
            self.assertNotIn(forbidden, page)
        loc = "https://www.gohirehumans.com/ai-qa-example-deliverables.html"
        start = sitemap.index(f"<loc>{loc}</loc>")
        end = sitemap.index("</url>", start)
        block = sitemap[start:end]
        self.assertIn("<lastmod>2026-05-26</lastmod>", block)

    def test_ai_qa_task_generator_supports_fixed_sku_shortcuts(self):
        generator = (REPO_ROOT / "frontend/ai-qa-task-generator.html").read_text(encoding="utf-8")
        services = (REPO_ROOT / "frontend/ai-qa-services.html").read_text(encoding="utf-8")
        buyer_brief = (REPO_ROOT / "frontend/ai-qa-buyer-brief.html").read_text(encoding="utf-8")
        for service in [
            "fact-check",
            "citation-check",
            "rag-groundedness",
            "support-reply-qa",
            "product-content-qa",
            "website-qa",
            "agent-work-audit",
        ]:
            self.assertIn(f'value="{service}"', generator)
            self.assertIn(f"/ai-qa-task-generator.html?service={service}", services)
        for snippet in [
            "serviceAliases",
            "'blog-fact-check':'fact-check'",
            "Generate managed brief",
            "No checkout or job is created automatically.",
            "managed_ai_qa_request_click",
            "Managed pilot note: no checkout or job should be created",
        ]:
            self.assertIn(snippet, generator)
        self.assertIn("serviceAliases", buyer_brief)
        for forbidden in [
            'href="/#/post-job',
            "draft_title=",
            "draft_description=",
            "stripe.redirectToCheckout",
            "/payments/checkout",
        ]:
            self.assertNotIn(forbidden, generator)


if __name__ == "__main__":
    unittest.main()
