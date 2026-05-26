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

    def test_ai_integration_uses_landing_nav_chrome(self):
        text = (REPO_ROOT / "frontend/ai-integration.html").read_text(encoding="utf-8", errors="ignore")
        required_snippets = [
            '<link rel="stylesheet" href="/style.css?v=20260525-agent-navfix">',
            '<div class="lp-nav-wrap">',
            '<nav class="lp-nav" aria-label="Main navigation">',
            '<a class="lp-nav-link" href="/#/services">Marketplace</a>',
            '<a class="lp-nav-link" href="/#/jobs">Open Jobs</a>',
            '<a class="lp-nav-link" href="/#/ai-employers">For Agents</a>',
            '<a class="lp-nav-link lp-nav-link-active" href="/ai-integration.html">Agent Guide</a>',
            '<a class="btn btn-primary btn-sm" href="/#/register">Get started</a>',
            'function toggleMobileMenu()',
        ]
        missing = [snippet for snippet in required_snippets if snippet not in text]
        self.assertEqual(missing, [])

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
        ]
        for snippet in required_snippets:
            self.assertIn(snippet, text)

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

    def test_homepage_has_safe_agent_marketplace_liquidity_messaging(self):
        text = (REPO_ROOT / "frontend/index.html").read_text(encoding="utf-8", errors="ignore")
        for snippet in [
            "Public beta listings",
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
