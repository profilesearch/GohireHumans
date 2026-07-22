import contextlib
import importlib.util
import io
import json
import os
import re
import sqlite3
import tempfile
import unittest
import xml.etree.ElementTree as ET
from html.parser import HTMLParser
from pathlib import Path
from types import ModuleType, SimpleNamespace
from typing import Any, cast
from unittest import mock

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

    def test_existing_session_is_rejected_immediately_after_user_suspension(self):
        token = "tok-suspended-session"
        db = self.module.get_db()
        try:
            db.execute(
                "INSERT INTO users (id,email,password_hash,name) VALUES (1,'user@example.com','x','User')"
            )
            db.execute(
                "INSERT INTO sessions (user_id,token,expires_at) VALUES (1,?,datetime('now','+1 day'))",
                [token],
            )
            db.commit()
        finally:
            db.close()

        def get_notifications():
            self.module._request_ctx.request_method = "GET"
            self.module._request_ctx.path_info = "/notifications"
            self.module._request_ctx.query_string = ""
            self.module._request_ctx.http_authorization = f"Bearer {token}"
            self.module._request_ctx.http_x_api_key = ""
            self.module._request_ctx.stdin_data = ""
            self.module._request_ctx.stdin_data_raw = b""
            self.module._request_ctx.content_type = "application/json"
            self.module._request_ctx.content_length = "0"
            self.module._request_ctx.remote_addr = "127.0.0.1"
            with contextlib.redirect_stdout(io.StringIO()) as out:
                self.module.handle_request()
            return parse_cgi_output(out.getvalue())

        status, body = get_notifications()
        self.assertEqual(status, 200, body)

        db = self.module.get_db()
        try:
            db.execute("UPDATE users SET is_suspended=1 WHERE id=1")
            db.commit()
        finally:
            db.close()

        status, body = get_notifications()
        self.assertEqual(status, 401, body)
        self.assertEqual(body.get("error"), "Unauthorized")

    def _bind_verified_funding(self, db, amount, intent_id="pi_live_like"):
        charge = self.module.buyer_charge_breakdown_cents(amount)
        fingerprint = self.module.funding_request_fingerprint(
            "order:1", 2, 1, None, charge
        )
        attempt_id = db.execute(
            """INSERT INTO funding_attempts
               (operation_key,attempt_number,request_fingerprint,
                processor_idempotency_key,employer_id,order_id,milestone_id,
                base_amount_cents,platform_fee_cents,processing_fee_cents,
                charged_total_cents,currency,status,stripe_payment_intent_id,
                processor_status,evidence_source,processor_evidence_at,committed_at)
               VALUES ('order:1',1,?,'escrow-fund:order:1:attempt:1',2,1,NULL,
                       ?,?,?,?,'usd','committed',?,'succeeded','processor_create',
                       datetime('now'),datetime('now'))""",
            [fingerprint, charge["base_cents"], charge["platform_fee_cents"],
             charge["processing_fee_cents"], charge["total_cents"], intent_id],
        ).lastrowid
        db.execute(
            """UPDATE escrow_holds SET base_amount_cents=?,platform_fee_cents=?,
                      processing_fee_cents=?,charged_total_cents=?,
                      fee_policy_version='component-half-up-v1',
                      funding_identity='order:1',funding_attempt_id=?
               WHERE order_id=1 AND milestone_id IS NULL""",
            [charge["base_cents"], charge["platform_fee_cents"],
             charge["processing_fee_cents"], charge["total_cents"], attempt_id],
        )

    def test_stripe_webhook_acknowledges_thin_events_without_data_object(self):
        class FakeWebhook:
            @staticmethod
            def construct_event(body_raw, sig_header, secret):
                return {
                    "id": "evt_test_thin",
                    "type": "v2.core.event_destination.ping",
                    "related_object": {"id": "acct_test"},
                }

        self.module.PRODUCTION_MODE = True
        self.module.STRIPE_AVAILABLE = True
        self.module.STRIPE_SECRET_KEY = "sk_test_configured"
        self.module.STRIPE_WEBHOOK_SECRET = "whsec_test_configured"
        self.module.STRIPE_SIGNATURE_ERROR = Exception
        self.module.stripe = type("FakeStripe", (), {"Webhook": FakeWebhook})
        self.module._request_ctx.request_method = "POST"
        self.module._request_ctx.path_info = "/webhooks/stripe"
        self.module._request_ctx.query_string = ""
        self.module._request_ctx.http_stripe_signature = "t=1,v1=test"
        self.module._request_ctx.stdin_data = '{"id":"evt_test_thin"}'
        self.module._request_ctx.stdin_data_raw = b'{"id":"evt_test_thin"}'
        self.module._request_ctx.content_type = "application/json"
        self.module._request_ctx.content_length = str(len(self.module._request_ctx.stdin_data_raw))
        self.module._request_ctx.remote_addr = "127.0.0.1"

        with contextlib.redirect_stdout(io.StringIO()) as out:
            self.module.handle_request()

        status, body = parse_cgi_output(out.getvalue())
        self.assertEqual(status, 200, body)
        self.assertEqual(body, {"received": True})

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

    def test_production_rejects_simulated_worker_payout_readiness_and_release(self):
        db = self.module.get_db()
        try:
            db.execute("INSERT INTO users (id,email,password_hash,name) VALUES (1,'worker@example.com','x','Worker')")
            db.execute("INSERT INTO worker_profiles (user_id,payout_account_id,payout_method) VALUES (1,'acct_sim_worker','stripe_connect_active')")
            db.execute("INSERT INTO users (id,email,password_hash,name) VALUES (2,'employer@example.com','x','Employer')")
            db.execute("INSERT INTO employer_profiles (user_id) VALUES (2)")
            db.execute("INSERT INTO services (id,worker_id,title,description,category,pricing_type,price) VALUES (1,1,'Svc','Desc','writing','fixed',100)")
            db.execute("INSERT INTO orders (id,type,service_id,worker_id,employer_id,status,total_amount) VALUES (1,'service_order',1,1,2,'submitted',100)")
            db.execute("INSERT INTO escrow_holds (order_id,amount,status,stripe_payment_intent_id) VALUES (1,100,'held','pi_live_like')")
            self._bind_verified_funding(db, 100)
            db.commit()
            self.module.PRODUCTION_MODE = True
            self.module.STRIPE_AVAILABLE = True
            self.module.STRIPE_SECRET_KEY = "sk_test_configured"
            self.assertFalse(self.module.worker_has_payout_setup(db, 1))
            with self.assertRaisesRegex(ValueError, "live worker Stripe Connect payout account"):
                self.module.release_escrow_to_worker(db, 1, None, 100, 1)
            self.assertEqual(db.execute("SELECT status FROM escrow_holds WHERE order_id=1").fetchone()[0], "held")
            self.assertEqual(db.execute("SELECT COUNT(*) FROM platform_revenue WHERE order_id=1").fetchone()[0], 0)
        finally:
            db.close()

    def test_live_worker_payout_readiness_requires_stripe_capabilities(self):
        db = self.module.get_db()
        try:
            db.execute("INSERT INTO users (id,email,password_hash,name) VALUES (1,'worker@example.com','x','Worker')")
            db.execute("INSERT INTO worker_profiles (user_id,payout_account_id,payout_method) VALUES (1,'acct_live_worker','stripe_connect_active')")
            db.commit()
            self.module.PRODUCTION_MODE = True
            self.module.STRIPE_AVAILABLE = True
            self.module.STRIPE_SECRET_KEY = "sk_test_configured"
            fake_account = SimpleNamespace(payouts_enabled=True, charges_enabled=False, details_submitted=True, capabilities={})
            self.module.stripe = type("FakeStripe", (), {
                "Account": type("Account", (), {"retrieve": mock.Mock(return_value=fake_account)}),
                "error": type("Error", (), {"StripeError": Exception})
            })
            self.assertFalse(self.module.worker_has_payout_setup(db, 1))
            fake_account.charges_enabled = True
            fake_account.capabilities = {'transfers': 'active'}
            self.assertTrue(self.module.worker_has_payout_setup(db, 1))
        finally:
            db.close()

    def test_live_release_rechecks_connect_readiness_before_transfer(self):
        db = self.module.get_db()
        try:
            db.execute("INSERT INTO users (id,email,password_hash,name) VALUES (1,'worker@example.com','x','Worker')")
            db.execute("INSERT INTO worker_profiles (user_id,payout_account_id,payout_method) VALUES (1,'acct_live_worker','stripe_connect_active')")
            db.execute("INSERT INTO users (id,email,password_hash,name) VALUES (2,'employer@example.com','x','Employer')")
            db.execute("INSERT INTO employer_profiles (user_id) VALUES (2)")
            db.execute("INSERT INTO services (id,worker_id,title,description,category,pricing_type,price) VALUES (1,1,'Svc','Desc','writing','fixed',100)")
            db.execute("INSERT INTO orders (id,type,service_id,worker_id,employer_id,status,total_amount) VALUES (1,'service_order',1,1,2,'submitted',100)")
            db.execute("INSERT INTO escrow_holds (order_id,amount,status,stripe_payment_intent_id) VALUES (1,100,'held','pi_live_like')")
            self._bind_verified_funding(db, 100)
            db.commit()
            self.module.PRODUCTION_MODE = True
            self.module.STRIPE_AVAILABLE = True
            self.module.STRIPE_SECRET_KEY = "sk_test_configured"
            fake_transfer = mock.Mock()
            fake_account = SimpleNamespace(payouts_enabled=False, charges_enabled=True, details_submitted=True, capabilities={'transfers': 'active'})
            self.module.stripe = type("FakeStripe", (), {
                "Account": type("Account", (), {"retrieve": mock.Mock(return_value=fake_account)}),
                "Transfer": type("Transfer", (), {"create": fake_transfer}),
                "error": type("Error", (), {"StripeError": Exception})
            })
            with self.assertRaisesRegex(ValueError, "not payout-ready"):
                self.module.release_escrow_to_worker(db, 1, None, 100, 1)
            fake_transfer.assert_not_called()
            self.assertEqual(db.execute("SELECT status FROM escrow_holds WHERE order_id=1").fetchone()[0], "held")
            self.assertEqual(db.execute("SELECT COUNT(*) FROM payout_transfers WHERE order_id=1").fetchone()[0], 0)
        finally:
            db.close()

    def test_live_release_updates_db_only_after_stripe_transfer_success(self):
        db = self.module.get_db()
        try:
            db.execute("INSERT INTO users (id,email,password_hash,name) VALUES (1,'worker@example.com','x','Worker')")
            db.execute("INSERT INTO worker_profiles (user_id,payout_account_id,payout_method) VALUES (1,'acct_live_worker','stripe_connect_active')")
            db.execute("INSERT INTO users (id,email,password_hash,name) VALUES (2,'employer@example.com','x','Employer')")
            db.execute("INSERT INTO employer_profiles (user_id) VALUES (2)")
            db.execute("INSERT INTO services (id,worker_id,title,description,category,pricing_type,price) VALUES (1,1,'Svc','Desc','writing','fixed',0.29)")
            db.execute("INSERT INTO orders (id,type,service_id,worker_id,employer_id,status,total_amount) VALUES (1,'service_order',1,1,2,'submitted',0.29)")
            db.execute("INSERT INTO escrow_holds (order_id,amount,status,stripe_payment_intent_id) VALUES (1,0.29,'held','pi_live_like')")
            self._bind_verified_funding(db, 0.29)
            db.commit()
            self.module.PRODUCTION_MODE = True
            self.module.STRIPE_AVAILABLE = True
            self.module.STRIPE_SECRET_KEY = "sk_test_configured"
            fake_transfer = mock.Mock(side_effect=lambda **kwargs: SimpleNamespace(
                id="tr_test_123", amount=kwargs["amount"], currency=kwargs["currency"],
                destination=kwargs["destination"], metadata=kwargs["metadata"],
            ))
            fake_account = SimpleNamespace(payouts_enabled=True, charges_enabled=True, details_submitted=True, capabilities={'transfers': 'active'})
            self.module.stripe = type("FakeStripe", (), {
                "Account": type("Account", (), {"retrieve": mock.Mock(return_value=fake_account)}),
                "Transfer": type("Transfer", (), {"create": fake_transfer}),
                "error": type("Error", (), {"StripeError": Exception})
            })
            payout, fee = self.module.release_escrow_to_worker(db, 1, None, 0.29, 1)
            self.assertEqual(payout, 0.29)
            self.assertEqual(fee, 0.01)
            fake_transfer.assert_called_once()
            self.assertEqual(fake_transfer.call_args.kwargs.get("amount"), 29)
            self.assertEqual(
                fake_transfer.call_args.kwargs.get("idempotency_key"),
                "escrow-release:hold:1:attempt:1",
            )
            self.assertEqual(db.execute("SELECT status FROM escrow_holds WHERE order_id=1").fetchone()[0], "released")
            self.assertEqual(db.execute("SELECT COUNT(*) FROM platform_revenue WHERE order_id=1").fetchone()[0], 1)
            transfer_row = db.execute("SELECT stripe_transfer_id, idempotency_key, status, transfer_type FROM payout_transfers WHERE order_id=1").fetchone()
            self.assertIsNotNone(transfer_row)
            self.assertEqual(transfer_row["stripe_transfer_id"], "tr_test_123")
            self.assertEqual(
                transfer_row["idempotency_key"], "escrow-release:hold:1:attempt:1"
            )
            self.assertEqual(transfer_row["status"], "recorded")
            self.assertEqual(transfer_row["transfer_type"], "escrow_release")
        finally:
            db.close()

    def test_live_release_transfer_failure_leaves_escrow_held(self):
        db = self.module.get_db()
        try:
            db.execute("INSERT INTO users (id,email,password_hash,name) VALUES (1,'worker@example.com','x','Worker')")
            db.execute("INSERT INTO worker_profiles (user_id,payout_account_id,payout_method) VALUES (1,'acct_live_worker','stripe_connect_active')")
            db.execute("INSERT INTO users (id,email,password_hash,name) VALUES (2,'employer@example.com','x','Employer')")
            db.execute("INSERT INTO employer_profiles (user_id) VALUES (2)")
            db.execute("INSERT INTO services (id,worker_id,title,description,category,pricing_type,price) VALUES (1,1,'Svc','Desc','writing','fixed',100)")
            db.execute("INSERT INTO orders (id,type,service_id,worker_id,employer_id,status,total_amount) VALUES (1,'service_order',1,1,2,'submitted',100)")
            db.execute("INSERT INTO escrow_holds (order_id,amount,status,stripe_payment_intent_id) VALUES (1,100,'held','pi_live_like')")
            self._bind_verified_funding(db, 100)
            db.commit()
            self.module.PRODUCTION_MODE = True
            self.module.STRIPE_AVAILABLE = True
            self.module.STRIPE_SECRET_KEY = "sk_test_configured"
            class StripeBoom(Exception):
                pass
            fake_transfer = mock.Mock(side_effect=StripeBoom("boom"))
            fake_account = SimpleNamespace(payouts_enabled=True, charges_enabled=True, details_submitted=True, capabilities={'transfers': 'active'})
            self.module.stripe = type("FakeStripe", (), {
                "Account": type("Account", (), {"retrieve": mock.Mock(return_value=fake_account)}),
                "Transfer": type("Transfer", (), {"create": fake_transfer}),
                "error": type("Error", (), {"StripeError": StripeBoom})
            })
            self.module.STRIPE_ERROR = StripeBoom
            self.module.STRIPE_PAYOUT_DEFINITIVE_PREOP_ERRORS = ()
            with self.assertRaisesRegex(
                self.module.FundingReconciliationRequired, "outcome is ambiguous"
            ):
                self.module.release_escrow_to_worker(db, 1, None, 100, 1)
            self.assertEqual(db.execute("SELECT status FROM escrow_holds WHERE order_id=1").fetchone()[0], "held")
            self.assertEqual(db.execute("SELECT COUNT(*) FROM platform_revenue WHERE order_id=1").fetchone()[0], 0)
            attempt = db.execute(
                "SELECT status,error_code FROM payout_release_attempts WHERE order_id=1"
            ).fetchone()
            self.assertEqual(tuple(attempt), ("unknown", "processor_outcome_ambiguous"))
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

    def test_service_creation_rejects_off_platform_payment_instructions(self):
        db = self.module.get_db()
        token = "tok-worker"
        try:
            db.execute("INSERT INTO users (id,email,password_hash,name) VALUES (1,'worker@example.com','x','Worker')")
            db.execute("INSERT INTO sessions (user_id,token,expires_at) VALUES (1,?,datetime('now','+1 day'))", [token])
            db.commit()
        finally:
            db.close()

        payload = {
            "title": "Website QA pass",
            "description": "I can test your website. Direct payment via PayPal is available.",
            "category": "testing",
            "pricing_type": "fixed",
            "price": 25,
            "includes": "Send payment to a crypto wallet before work starts",
        }
        body = json.dumps(payload)
        self.module._request_ctx.request_method = "POST"
        self.module._request_ctx.path_info = "/api/v1/services"
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
        self.assertEqual(status, 422, response)
        self.assertIn("Payment instructions must stay on-platform", response.get("error", ""))

    def test_service_update_rejects_off_platform_payment_instructions_in_tags(self):
        db = self.module.get_db()
        token = "tok-worker"
        try:
            db.execute("INSERT INTO users (id,email,password_hash,name) VALUES (1,'worker@example.com','x','Worker')")
            db.execute("INSERT INTO sessions (user_id,token,expires_at) VALUES (1,?,datetime('now','+1 day'))", [token])
            db.execute("INSERT INTO services (id,worker_id,title,description,category,pricing_type,price,status) VALUES (1,1,'Testing Svc','Clean QA scope','testing','fixed',25,'active')")
            db.commit()
        finally:
            db.close()

        payload = {"tags": ["qa", "solana wallet accepted"]}
        body = json.dumps(payload)
        self.module._request_ctx.request_method = "PUT"
        self.module._request_ctx.path_info = "/api/v1/services/1"
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
        self.assertEqual(status, 422, response)
        self.assertIn("Payment instructions must stay on-platform", response.get("error", ""))

    def test_job_creation_rejects_off_platform_payment_instructions(self):
        db = self.module.get_db()
        token = "tok-employer"
        try:
            db.execute("INSERT INTO users (id,email,password_hash,name) VALUES (1,'employer@example.com','x','Employer')")
            db.execute("INSERT INTO employer_profiles (user_id) VALUES (1)")
            db.execute("INSERT INTO sessions (user_id,token,expires_at) VALUES (1,?,datetime('now','+1 day'))", [token])
            db.commit()
        finally:
            db.close()

        payload = {
            "title": "Pay me via PayPal",
            "description": "Review the website and use direct payment through paypal.me/example.",
            "category": "testing",
            "budget_type": "fixed",
            "budget_amount": 25,
            "required_skills": ["qa", "zelle accepted"],
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
        self.assertEqual(status, 422, response)
        self.assertIn("Payment instructions must stay on-platform", response.get("error", ""))

    def test_application_rejects_payment_circumvention_and_unsafe_portfolio_url(self):
        db = self.module.get_db()
        token = "tok-worker"
        try:
            db.execute("INSERT INTO users (id,email,password_hash,name) VALUES (1,'employer@example.com','x','Employer')")
            db.execute("INSERT INTO employer_profiles (user_id) VALUES (1)")
            db.execute("INSERT INTO users (id,email,password_hash,name) VALUES (2,'worker@example.com','x','Worker')")
            db.execute("INSERT INTO worker_profiles (user_id) VALUES (2)")
            db.execute("INSERT INTO sessions (user_id,token,expires_at) VALUES (2,?,datetime('now','+1 day'))", [token])
            db.execute("INSERT INTO jobs (id,employer_id,title,description,category,budget_type,budget_amount,status) VALUES (7,1,'QA Job','Clean scope','testing','fixed',25,'open')")
            db.commit()
        finally:
            db.close()

        payload = {"cover_message": "I can help. Pay me via PayPal or Zelle.", "portfolio_url": "javascript:alert(document.domain)"}
        body = json.dumps(payload)
        self.module._request_ctx.request_method = "POST"
        self.module._request_ctx.path_info = "/api/v1/jobs/7/apply"
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
        self.assertEqual(status, 422, response)
        self.assertIn("Payment instructions must stay on-platform", response.get("error", ""))

        payload = {"cover_message": "I can help with this QA task.", "portfolio_url": "javascript:alert(document.domain)"}
        for attr in ("body_cache", "raw_body"):
            if hasattr(self.module._request_ctx, attr):
                delattr(self.module._request_ctx, attr)
        body = json.dumps(payload)
        self.module._request_ctx.request_method = "POST"
        self.module._request_ctx.path_info = "/api/v1/jobs/7/apply"
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
        self.assertEqual(status, 422, response)
        self.assertIn("portfolio_url must be a valid http(s) URL", response.get("error", ""))

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

        sent = []
        self.module.RESEND_API_KEY = "configured-for-test"
        self.module.send_email = lambda to, subject, html: sent.append((to, subject, html)) or True

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
            self.assertEqual(len(sent), 1)
            to, subject, html = sent[0]
            self.assertEqual(to, "worker@example.com")
            self.assertIn("New job matches your skills", subject)
            self.assertIn(f"https://www.gohirehumans.com/#/jobs/{response['id']}", html)
        finally:
            db.close()

    def test_push_notification_can_mirror_marketplace_critical_email_once(self):
        db = self.module.get_db()
        try:
            db.execute("INSERT INTO users (id,email,password_hash,name) VALUES (1,'worker@example.com','x','Worker Name')")
            sent = []
            self.module.RESEND_API_KEY = "configured-for-test"
            self.module.send_email = lambda to, subject, html: sent.append((to, subject, html)) or True

            self.module.push_notification(
                db,
                1,
                "job_match",
                "New job matches your skills: Website QA pass",
                "A new testing job was just posted. Budget: $25",
                "#/jobs/7",
                email=True,
            )
            self.module.push_notification(
                db,
                1,
                "job_match",
                "New job matches your skills: Website QA pass",
                "A new testing job was just posted. Budget: $25",
                "#/jobs/7",
                email=True,
            )

            self.assertEqual(sent, [])
            db.commit()
            self.module.flush_transactional_notification_emails(db)
            self.assertEqual(len(sent), 1)
            to, subject, html = sent[0]
            self.assertEqual(to, "worker@example.com")
            self.assertIn("New job matches your skills", subject)
            self.assertIn("https://www.gohirehumans.com/#/jobs/7", html)
            self.assertIn("You received this because this relates to your GoHireHumans marketplace activity.", html)
            self.assertNotIn("employer@example.com", html)
            email_audit_count = db.execute(
                "SELECT COUNT(*) FROM audit_log WHERE action='transactional_email_sent'"
            ).fetchone()[0]
            self.assertEqual(email_audit_count, 1)
        finally:
            db.close()

    def test_transactional_email_dedupe_audit_persists_after_reopen(self):
        db = self.module.get_db()
        try:
            db.execute("INSERT INTO users (id,email,password_hash,name) VALUES (1,'worker@example.com','x','Worker Name')")
            sent = []
            self.module.RESEND_API_KEY = "configured-for-test"
            self.module.send_email = lambda to, subject, html: sent.append((to, subject, html)) or True

            self.module.push_notification(
                db,
                1,
                "job_match",
                "New job matches your skills: Website QA pass",
                "A new testing job was just posted. Budget: $25",
                "#/jobs/7",
                email=True,
                email_dedupe="job_match:7",
            )
            db.commit()
            self.module.flush_transactional_notification_emails(db)
            self.assertEqual(len(sent), 1)
        finally:
            db.close()

        reopened = self.module.get_db()
        try:
            persisted_count = reopened.execute(
                "SELECT COUNT(*) FROM audit_log WHERE action='transactional_email_sent'"
            ).fetchone()[0]
            self.assertEqual(persisted_count, 1)
            self.module.push_notification(
                reopened,
                1,
                "job_match",
                "New job matches your skills: Website QA pass",
                "A new testing job was just posted. Budget: $25",
                "#/jobs/7",
                email=True,
                email_dedupe="job_match:7",
            )
            reopened.commit()
            self.module.flush_transactional_notification_emails(reopened)
            self.assertEqual(len(sent), 1)
        finally:
            reopened.close()

    def test_push_notification_does_not_email_generic_notifications(self):
        db = self.module.get_db()
        try:
            db.execute("INSERT INTO users (id,email,password_hash,name) VALUES (1,'worker@example.com','x','Worker Name')")
            sent = []
            self.module.RESEND_API_KEY = "configured-for-test"
            self.module.send_email = lambda to, subject, html: sent.append((to, subject, html)) or True

            self.module.push_notification(
                db,
                1,
                "worker_activation",
                "Paid GoHireHumans jobs are live",
                "Browse open jobs when you have time.",
                "#/jobs",
                email=True,
            )
            db.commit()
            self.module.flush_transactional_notification_emails(db)

            self.assertEqual(sent, [])
            self.assertEqual(
                db.execute("SELECT COUNT(*) FROM notifications WHERE type='worker_activation'").fetchone()[0],
                1,
            )
        finally:
            db.close()

    def test_transactional_email_cta_ignores_external_links(self):
        db = self.module.get_db()
        try:
            db.execute("INSERT INTO users (id,email,password_hash,name) VALUES (1,'worker@example.com','x','Worker Name')")
            sent = []
            self.module.RESEND_API_KEY = "configured-for-test"
            self.module.send_email = lambda to, subject, html: sent.append((to, subject, html)) or True

            self.module.push_notification(
                db,
                1,
                "new_application",
                "New application: Website QA",
                "Someone applied to your job.",
                "https://evil.example/phish",
                email=True,
            )
            db.commit()
            self.module.flush_transactional_notification_emails(db)

            self.assertEqual(len(sent), 1)
            self.assertIn('href="https://www.gohirehumans.com"', sent[0][2])
            self.assertNotIn("evil.example", sent[0][2])
        finally:
            db.close()

    def test_multiple_applications_to_same_job_each_send_email(self):
        db = self.module.get_db()
        try:
            db.execute("INSERT INTO users (id,email,password_hash,name) VALUES (1,'worker1@example.com','x','Worker One')")
            db.execute("INSERT INTO worker_profiles (user_id) VALUES (1)")
            db.execute("INSERT INTO sessions (user_id,token,expires_at) VALUES (1,'tok-worker-1',datetime('now','+1 day'))")
            db.execute("INSERT INTO users (id,email,password_hash,name) VALUES (2,'worker2@example.com','x','Worker Two')")
            db.execute("INSERT INTO worker_profiles (user_id) VALUES (2)")
            db.execute("INSERT INTO sessions (user_id,token,expires_at) VALUES (2,'tok-worker-2',datetime('now','+1 day'))")
            db.execute("INSERT INTO users (id,email,password_hash,name) VALUES (3,'employer@example.com','x','Employer')")
            db.execute("INSERT INTO employer_profiles (user_id) VALUES (3)")
            db.execute("INSERT INTO jobs (id,employer_id,title,description,category,budget_type,budget_amount,status) VALUES (7,3,'QA Job','Desc','testing','fixed',25,'open')")
            db.commit()
        finally:
            db.close()

        sent = []
        self.module.RESEND_API_KEY = "configured-for-test"
        self.module.send_email = lambda to, subject, html: sent.append((to, subject, html)) or True

        for token, cover in [("tok-worker-1", "First applicant"), ("tok-worker-2", "Second applicant")]:
            payload = json.dumps({"cover_message": cover})
            self.module._request_ctx.request_method = "POST"
            self.module._request_ctx.path_info = "/jobs/7/apply"
            self.module._request_ctx.query_string = ""
            self.module._request_ctx.http_authorization = f"Bearer {token}"
            self.module._request_ctx.http_x_api_key = ""
            self.module._request_ctx.stdin_data = payload
            self.module._request_ctx.content_type = "application/json"
            self.module._request_ctx.content_length = str(len(payload))
            self.module._request_ctx.remote_addr = "127.0.0.1"
            with contextlib.redirect_stdout(io.StringIO()) as out:
                self.module.handle_request()
            status, body = parse_cgi_output(out.getvalue())
            self.assertEqual(status, 201, body)

        self.assertEqual(len(sent), 2)
        self.assertEqual([item[0] for item in sent], ["employer@example.com", "employer@example.com"])

    def test_revision_request_email_omits_sensitive_notes(self):
        db = self.module.get_db()
        try:
            db.execute("INSERT INTO users (id,email,password_hash,name) VALUES (1,'worker@example.com','x','Worker')")
            db.execute("INSERT INTO worker_profiles (user_id) VALUES (1)")
            db.execute("INSERT INTO users (id,email,password_hash,name) VALUES (2,'employer@example.com','x','Employer')")
            db.execute("INSERT INTO employer_profiles (user_id) VALUES (2)")
            db.execute("INSERT INTO sessions (user_id,token,expires_at) VALUES (2,'tok-employer',datetime('now','+1 day'))")
            db.execute("INSERT INTO orders (id,type,worker_id,employer_id,status,total_amount) VALUES (9,'service_order',1,2,'submitted',25)")
            db.execute("INSERT INTO milestones (order_id,title,description,amount,sequence,status) VALUES (9,'Delivery','',25,1,'submitted')")
            db.commit()
        finally:
            db.close()

        sent = []
        self.module.RESEND_API_KEY = "configured-for-test"
        self.module.send_email = lambda to, subject, html: sent.append((to, subject, html)) or True
        secret_notes = "Please revise. Password abc123 and private client details should not leave platform."
        payload = json.dumps({"notes": secret_notes})
        self.module._request_ctx.request_method = "POST"
        self.module._request_ctx.path_info = "/orders/9/request-revision"
        self.module._request_ctx.query_string = ""
        self.module._request_ctx.http_authorization = "Bearer tok-employer"
        self.module._request_ctx.http_x_api_key = ""
        self.module._request_ctx.stdin_data = payload
        self.module._request_ctx.content_type = "application/json"
        self.module._request_ctx.content_length = str(len(payload))
        self.module._request_ctx.remote_addr = "127.0.0.1"
        with contextlib.redirect_stdout(io.StringIO()) as out:
            self.module.handle_request()
        status, body = parse_cgi_output(out.getvalue())
        self.assertEqual(status, 200, body)
        self.assertEqual(len(sent), 1)
        self.assertIn("A revision has been requested on order #9", sent[0][2])
        self.assertNotIn("abc123", sent[0][2])
        self.assertNotIn("private client details", sent[0][2])

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
        self.module._request_ctx.stdin_data = json.dumps({"password": new_password, "admin_password": "AdminPassword123!"})
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

    def test_admin_password_rotation_requires_step_up_reauth(self):
        db = self.module.get_db()
        try:
            db.execute("INSERT INTO users (id,email,password_hash,name,is_admin) VALUES (1,'admin@example.com',?,'Admin',1)", [self.module.hash_password('AdminPassword123!')])
            db.execute("INSERT INTO users (id,email,password_hash,name,is_admin) VALUES (2,'target@example.com',?,'Target',0)", [self.module.hash_password('OldPassword123!')])
            db.execute("INSERT INTO sessions (user_id,token,expires_at) VALUES (1,'admin-token',datetime('now','+1 day'))")
            db.commit()
        finally:
            db.close()

        self.module._request_ctx.request_method = "PUT"
        self.module._request_ctx.path_info = "/admin/users/2/password"
        self.module._request_ctx.query_string = ""
        self.module._request_ctx.http_authorization = "Bearer admin-token"
        self.module._request_ctx.http_x_api_key = ""
        self.module._request_ctx.stdin_data = json.dumps({"password": "NewTemporaryPassword123!"})
        self.module._request_ctx.content_type = "application/json"
        self.module._request_ctx.content_length = str(len(self.module._request_ctx.stdin_data))
        self.module._request_ctx.remote_addr = "127.0.0.1"
        with contextlib.redirect_stdout(io.StringIO()) as out:
            self.module.handle_request()
        status, body = parse_cgi_output(out.getvalue())
        self.assertEqual(status, 403, body)
        self.assertIn("Admin password confirmation required", body["error"])
        db = self.module.get_db()
        try:
            user = db.execute("SELECT password_hash FROM users WHERE id=2").fetchone()
            self.assertTrue(self.module.verify_password('OldPassword123!', user['password_hash']))
            audit = db.execute("SELECT action FROM audit_log WHERE action='admin_rotate_user_password_step_up_missing'").fetchone()
            self.assertIsNotNone(audit)
        finally:
            db.close()

    def test_admin_user_status_update_requires_step_up_and_redacts_admin_password_from_audit(self):
        db = self.module.get_db()
        try:
            db.execute("INSERT INTO users (id,email,password_hash,name,is_admin) VALUES (1,'admin@example.com',?,'Admin',1)", [self.module.hash_password('AdminPassword123!')])
            db.execute("INSERT INTO users (id,email,password_hash,name,is_admin,is_suspended) VALUES (2,'target@example.com',?,'Target',0,0)", [self.module.hash_password('TargetPassword123!')])
            db.execute("INSERT INTO sessions (user_id,token,expires_at) VALUES (1,'admin-token',datetime('now','+1 day'))")
            db.commit()
        finally:
            db.close()

        self.module._request_ctx.request_method = "PUT"
        self.module._request_ctx.path_info = "/admin/users/2"
        self.module._request_ctx.query_string = ""
        self.module._request_ctx.http_authorization = "Bearer admin-token"
        self.module._request_ctx.http_x_api_key = ""
        self.module._request_ctx.stdin_data = json.dumps({"is_suspended": True, "admin_password": "AdminPassword123!"})
        self.module._request_ctx.content_type = "application/json"
        self.module._request_ctx.content_length = str(len(self.module._request_ctx.stdin_data))
        self.module._request_ctx.remote_addr = "127.0.0.1"
        with contextlib.redirect_stdout(io.StringIO()) as out:
            self.module.handle_request()
        status, body = parse_cgi_output(out.getvalue())
        self.assertEqual(status, 200, body)
        db = self.module.get_db()
        try:
            user = db.execute("SELECT is_suspended FROM users WHERE id=2").fetchone()
            self.assertEqual(user['is_suspended'], 1)
            audit = db.execute("SELECT details FROM audit_log WHERE action='admin_update_user' AND entity_id=2").fetchone()
            self.assertNotIn("AdminPassword123!", audit['details'] or '')
            self.assertNotIn("admin_password", audit['details'] or '')
        finally:
            db.close()

    def test_login_failures_are_throttled_and_audited(self):
        db = self.module.get_db()
        try:
            db.execute("INSERT INTO users (id,email,password_hash,name,is_admin) VALUES (1,'admin@example.com',?,'Admin',1)", [self.module.hash_password('CorrectPassword123!')])
            db.commit()
        finally:
            db.close()

        statuses = []
        for _ in range(7):
            self.module._request_ctx.request_method = "POST"
            self.module._request_ctx.path_info = "/auth/login"
            self.module._request_ctx.query_string = ""
            self.module._request_ctx.http_authorization = ""
            self.module._request_ctx.http_x_api_key = ""
            self.module._request_ctx.stdin_data = json.dumps({"email": "admin@example.com", "password": "wrong"})
            self.module._request_ctx.content_type = "application/json"
            self.module._request_ctx.content_length = str(len(self.module._request_ctx.stdin_data))
            self.module._request_ctx.remote_addr = "203.0.113.5"
            if hasattr(self.module._request_ctx, 'body_cache'):
                delattr(self.module._request_ctx, 'body_cache')
            if hasattr(self.module._request_ctx, 'raw_body'):
                delattr(self.module._request_ctx, 'raw_body')
            with contextlib.redirect_stdout(io.StringIO()) as out:
                self.module.handle_request()
            status, _ = parse_cgi_output(out.getvalue())
            statuses.append(status)
        self.assertEqual(statuses[:6], [401] * 6)
        self.assertEqual(statuses[6], 429)
        db = self.module.get_db()
        try:
            failed = db.execute("SELECT COUNT(*) FROM audit_log WHERE action='login_failed'").fetchone()[0]
            limited = db.execute("SELECT COUNT(*) FROM audit_log WHERE action='login_rate_limited'").fetchone()[0]
            self.assertEqual(failed, 6)
            self.assertEqual(limited, 1)
        finally:
            db.close()

    def test_valid_login_succeeds_after_failed_attempt_threshold_and_clears_failures(self):
        db = self.module.get_db()
        try:
            db.execute("INSERT INTO users (id,email,password_hash,name,is_admin) VALUES (1,'admin@example.com',?,'Admin',1)", [self.module.hash_password('CorrectPassword123!')])
            db.commit()
        finally:
            db.close()

        for _ in range(6):
            self.module._request_ctx.request_method = "POST"
            self.module._request_ctx.path_info = "/auth/login"
            self.module._request_ctx.query_string = ""
            self.module._request_ctx.http_authorization = ""
            self.module._request_ctx.http_x_api_key = ""
            self.module._request_ctx.stdin_data = json.dumps({"email": "admin@example.com", "password": "wrong"})
            self.module._request_ctx.content_type = "application/json"
            self.module._request_ctx.content_length = str(len(self.module._request_ctx.stdin_data))
            self.module._request_ctx.remote_addr = "203.0.113.6"
            if hasattr(self.module._request_ctx, 'body_cache'):
                delattr(self.module._request_ctx, 'body_cache')
            if hasattr(self.module._request_ctx, 'raw_body'):
                delattr(self.module._request_ctx, 'raw_body')
            with contextlib.redirect_stdout(io.StringIO()) as out:
                self.module.handle_request()
            status, _ = parse_cgi_output(out.getvalue())
            self.assertEqual(status, 401)

        self.module._request_ctx.stdin_data = json.dumps({"email": "admin@example.com", "password": "CorrectPassword123!"})
        self.module._request_ctx.content_length = str(len(self.module._request_ctx.stdin_data))
        if hasattr(self.module._request_ctx, 'body_cache'):
            delattr(self.module._request_ctx, 'body_cache')
        if hasattr(self.module._request_ctx, 'raw_body'):
            delattr(self.module._request_ctx, 'raw_body')
        with contextlib.redirect_stdout(io.StringIO()) as out:
            self.module.handle_request()
        status, body = parse_cgi_output(out.getvalue())
        self.assertEqual(status, 200, body)
        self.assertIn("token", body)
        self.assertEqual(self.module._login_failure_store.get("203.0.113.6:admin@example.com"), None)

    def test_frontend_security_hardening_invariants(self):
        text = (REPO_ROOT / "frontend/index.html").read_text(encoding="utf-8", errors="ignore")
        self.assertIn("sessionStorage.setItem('ghh_token'", text)
        self.assertNotIn("localStorage.setItem('ghh_token'", text)
        self.assertIn("localStorage.removeItem('ghh_token'", text)
        self.assertIn("function esc(s) { return s == null ? '' : String(s).replace(/&/g,'&amp;')", text)
        self.assertIn("allowHtml = false", text)
        self.assertIn("modalBody.textContent = message || ''", text)
        self.assertIn("admin_password: adminPassword", text)
        self.assertNotIn("manual_money_movement_confirmed: true", text)
        self.assertNotIn("processor_reference: processorReference", text)
        self.assertIn("Issue task-amount refund", text)
        self.assertIn("Stripe processing and the 1% platform fee are not automatically refunded", text)
        self.assertIn("/trust-safety.html", text)

    def test_first_task_wizard_and_measurement_invariants(self):
        text = (REPO_ROOT / "frontend/index.html").read_text(encoding="utf-8", errors="ignore")
        required = [
            "function getTaskDraftTemplate(key)",
            "website_qa: 'website_test'",
            "ai_output_qa: 'ai_review'",
            "automation_qa: 'automation_verification'",
            "spreadsheet_cleanup: 'data_cleanup'",
            "phone_fact_check: {",
            "Make phone calls or verify a fact",
            "lead_qualification: {",
            "homepage_describe_task_primary_click",
            "Describe your task",
            'data-simplified-home="true"',
            'id="guided-task-intake"',
            "Build a task draft",
            'data-home-section="how"',
            "first_task_wizard_started",
            "first_task_template_selected",
            "first_task_wizard_review_draft_click",
            "first_task_draft_completed",
            "function markFirstTaskWizardStarted(params = {})",
            "first_task_blank_form_opened",
            "const hasMeaningfulDraft = Boolean(task || deliverable)",
            "function getStoredGuidedTaskDraft()",
            "function clearStoredGuidedTaskDraft()",
            "function getStoredAttribution()",
            "sessionStorage.setItem('ghh_attribution'",
            "utm_source",
            "utm_campaign",
        ]
        missing = [snippet for snippet in required if snippet not in text]
        self.assertEqual(missing, [])

    def test_executive_conversion_events_exclude_upper_funnel_intent(self):
        text = (REPO_ROOT / "frontend/index.html").read_text(encoding="utf-8", errors="ignore")
        excluded_key_event_leads = [
            "lead_type: 'contact_click'",
            "lead_type: 'first_task_draft_completed'",
            "lead_type:'starter_offer_router'",
            "lead_type: 'service_order_submit'",
            "lead_type: 'service_post_completed'",
        ]
        for lead_type in excluded_key_event_leads:
            matching_lines = [
                line for line in text.splitlines()
                if "trackConfiguredKeyEvent" in line and lead_type in line
            ]
            self.assertEqual(matching_lines, [], lead_type)
        recommended_lead_lines = [
            line for line in text.splitlines()
            if "trackRecommendedEvent('generate_lead'" in line
        ]
        completed_lead_types = {
            "job_application_completed",
            "job_post_completed",
            "service_order_completed",
        }
        self.assertEqual(len(recommended_lead_lines), len(completed_lead_types))
        for line in recommended_lead_lines:
            self.assertTrue(any(lead_type in line for lead_type in completed_lead_types), line)
        self.assertIn(
            "trackRecommendedEvent('generate_lead', { lead_type: 'job_application_completed'",
            text,
        )
        self.assertIn(
            "trackConfiguredKeyEvent('qualify_lead', { lead_type: 'job_application_completed'",
            text,
        )
        self.assertIn(
            "trackConfiguredKeyEvent('qualify_lead', { lead_type: 'job_post_completed'",
            text,
        )
        self.assertIn(
            "trackConfiguredKeyEvent('close_convert_lead', { lead_type: 'service_order_completed'",
            text,
        )

    def test_conversion_forms_expose_retryable_inline_failure_states(self):
        text = (REPO_ROOT / "frontend/index.html").read_text(encoding="utf-8", errors="ignore")
        required = [
            'id="jobApplicationError"',
            'id="jobApplicationSubmitBtn"',
            "job_application_failed",
            'id="postJobFormError"',
            'id="postJobSubmitBtn"',
            "Posting...",
            "payment_setup_failed",
            "confirm_request_error",
            "setup_request_error",
        ]
        missing = [snippet for snippet in required if snippet not in text]
        self.assertEqual(missing, [])

    def test_priority_search_pages_have_query_aligned_metadata(self):
        expected = {
            "frontend/blog/alternatives-to-freelancer.html": (
                "7 Best Freelancer.com Alternatives (2026) | GoHireHumans",
                "Compare seven Freelancer.com alternatives by fees, payment workflow, trust signals, and fit—including GoHireHumans, Upwork, Fiverr, Contra, and more.",
            ),
            "frontend/blog/verified-freelancer-marketplace.html": (
                "How to Verify a Freelancer: 4 Trust Signals | GoHireHumans",
                "Check four practical trust signals before hiring a freelancer: profile evidence, relevant work samples, issue-review clarity, and transparent content policies.",
            ),
            "frontend/blog/where-to-list-services-online.html": (
                "Where to List Services Online: 8 Platforms | GoHireHumans",
                "Compare eight places to list freelance services by audience, fees, payment support, and fit—including GoHireHumans, Fiverr, Upwork, Contra, and LinkedIn.",
            ),
            "frontend/hire/hire-ai-agent.html": (
                "Hire an AI Agent or Human Reviewer | GoHireHumans",
                "Browse AI-agent and human provider profiles for automation, research, content, QA, data, and scoped work. Review profiles and workflow details before hiring.",
            ),
        }
        for relative_path, (title, description) in expected.items():
            text = (REPO_ROOT / relative_path).read_text(encoding="utf-8", errors="ignore")
            self.assertIn(f'<title>{title}</title>', text, relative_path)
            self.assertIn(f'<meta name="description" content="{description}">', text, relative_path)
            self.assertLessEqual(len(title), 60, relative_path)
            self.assertLessEqual(len(description), 160, relative_path)
            self.assertEqual(text.count('<meta property="og:title"'), 1, relative_path)
            self.assertEqual(text.count('<meta property="og:description"'), 1, relative_path)
            self.assertIn(f'<meta property="og:title" content="{title}">', text, relative_path)
            self.assertIn(f'<meta property="og:description" content="{description}">', text, relative_path)
            self.assertEqual(text.count('<meta name="twitter:title"'), 1, relative_path)
            self.assertEqual(text.count('<meta name="twitter:description"'), 1, relative_path)
            self.assertIn(f'<meta name="twitter:title" content="{title}">', text, relative_path)
            self.assertIn(f'<meta name="twitter:description" content="{description}">', text, relative_path)

    def test_phase2_ui_flow_polish_invariants(self):
        text = (REPO_ROOT / "frontend/index.html").read_text(encoding="utf-8", errors="ignore")
        required = [
            "requireAuth(`jobs/${id}?apply=1`)",
            "const shouldAutoApply = getQuery().get('apply') === '1'",
            "setTimeout(() => handleJobApply(id), 0)",
            "sessionStorage.setItem('ghh_auth_intent'",
            "sessionStorage.removeItem('ghh_auth_intent')",
            "Sign in to Apply",
            "We will bring you back to this job after sign-in.",
            "id=\"apply-cover-message\"",
            "id=\"apply-portfolio-url\"",
            "New listing",
            "services-result-count",
            'data-filter-toggle',
            "function toggleServiceFilters(forceOpen)",
            "Keep payments on-platform",
            "You can apply to jobs before connecting payouts.",
            "grid-template-columns:repeat(auto-fit,minmax(240px,1fr))",
            "function safeExternalHref(value)",
            "safeExternalHref(a.portfolio_url)",
        ]
        missing = [snippet for snippet in required if snippet not in text]
        self.assertEqual(missing, [])

    def test_audit_redacts_sensitive_details_recursively(self):
        db = self.module.get_db()
        try:
            self.module.audit(db, 1, "sensitive_test", "user", 1, {
                "password": "SuperSecret123!",
                "access_token": "access-secret",
                "refreshToken": "refresh-secret",
                "bearer_token": "bearer-secret",
                "credentialPayload": "credential-secret",
                "sessionId": "session-secret",
                "nested": {"admin_password": "AdminSecret123!", "safe": "visible"},
                "items": [{"token": "tok_live_secret", "name": "ok"}],
            })
            row = db.execute("SELECT details FROM audit_log WHERE action='sensitive_test'").fetchone()
            details = json.loads(row["details"])
            self.assertEqual(details["password"], "[REDACTED]")
            self.assertEqual(details["access_token"], "[REDACTED]")
            self.assertEqual(details["refreshToken"], "[REDACTED]")
            self.assertEqual(details["bearer_token"], "[REDACTED]")
            self.assertEqual(details["credentialPayload"], "[REDACTED]")
            self.assertEqual(details["sessionId"], "[REDACTED]")
            self.assertEqual(details["nested"]["admin_password"], "[REDACTED]")
            self.assertEqual(details["nested"]["safe"], "visible")
            self.assertEqual(details["items"][0]["token"], "[REDACTED]")
            self.assertEqual(details["items"][0]["name"], "ok")
        finally:
            db.close()

    def test_trust_safety_page_covers_payment_and_dispute_safety(self):
        text = (REPO_ROOT / "frontend/trust-safety.html").read_text(encoding="utf-8", errors="ignore").lower()
        required = ["stripe-powered processing", "payment review", "issue review", "off-platform", "available evidence", "dispute"]
        missing = [snippet for snippet in required if snippet not in text]
        self.assertEqual(missing, [])

    def test_production_worker_payout_setup_refuses_simulated_records_without_stripe(self):
        db = self.module.get_db()
        token = "tok-worker-prod"
        try:
            db.execute("INSERT INTO users (id,email,password_hash,name) VALUES (1,'worker@example.com','x','Worker')")
            db.execute("INSERT INTO worker_profiles (user_id) VALUES (1)")
            db.execute("INSERT INTO sessions (user_id,token,expires_at) VALUES (1,?,datetime('now','+1 day'))", [token])
            db.commit()
        finally:
            db.close()
        self.module.PRODUCTION_MODE = True
        self.module.STRIPE_AVAILABLE = False
        self.module.STRIPE_SECRET_KEY = ""
        self.module._request_ctx.request_method = "POST"
        self.module._request_ctx.path_info = "/payments/setup-worker"
        self.module._request_ctx.query_string = ""
        self.module._request_ctx.http_authorization = f"Bearer {token}"
        self.module._request_ctx.stdin_data = json.dumps({"bank_name": "Demo Bank", "last4": "4242"})
        self.module._request_ctx.content_type = "application/json"
        self.module._request_ctx.content_length = str(len(self.module._request_ctx.stdin_data))
        self.module._request_ctx.remote_addr = "198.51.100.7"
        if hasattr(self.module._request_ctx, 'body_cache'):
            delattr(self.module._request_ctx, 'body_cache')
        if hasattr(self.module._request_ctx, 'raw_body'):
            delattr(self.module._request_ctx, 'raw_body')
        with contextlib.redirect_stdout(io.StringIO()) as out:
            self.module.handle_request()
        status, body = parse_cgi_output(out.getvalue())
        self.assertEqual(status, 503, body)
        self.assertIn("simulated worker payout setup is disabled", body["error"])
        db = self.module.get_db()
        try:
            wp = db.execute("SELECT payout_account_id,payout_method FROM worker_profiles WHERE user_id=1").fetchone()
            self.assertIsNone(wp["payout_account_id"])
            self.assertEqual(wp["payout_method"], "pending_setup")
        finally:
            db.close()

    def test_production_payment_status_does_not_mark_simulated_worker_payout_ready(self):
        db = self.module.get_db()
        token = "tok-worker-status"
        try:
            db.execute("INSERT INTO users (id,email,password_hash,name) VALUES (1,'worker@example.com','x','Worker')")
            db.execute("INSERT INTO worker_profiles (user_id,payout_account_id,payout_method) VALUES (1,'acct_sim_existing','stripe_connect_active')")
            db.execute("INSERT INTO sessions (user_id,token,expires_at) VALUES (1,?,datetime('now','+1 day'))", [token])
            db.commit()
        finally:
            db.close()
        self.module.PRODUCTION_MODE = True
        self.module.STRIPE_AVAILABLE = False
        self.module.STRIPE_SECRET_KEY = ""
        self.module._request_ctx.request_method = "GET"
        self.module._request_ctx.path_info = "/payments/status"
        self.module._request_ctx.query_string = ""
        self.module._request_ctx.http_authorization = f"Bearer {token}"
        self.module._request_ctx.stdin_data = ""
        self.module._request_ctx.content_type = "application/json"
        self.module._request_ctx.content_length = "0"
        self.module._request_ctx.remote_addr = "198.51.100.8"
        if hasattr(self.module._request_ctx, 'body_cache'):
            delattr(self.module._request_ctx, 'body_cache')
        if hasattr(self.module._request_ctx, 'raw_body'):
            delattr(self.module._request_ctx, 'raw_body')
        with contextlib.redirect_stdout(io.StringIO()) as out:
            self.module.handle_request()
        status, body = parse_cgi_output(out.getvalue())
        self.assertEqual(status, 200, body)
        self.assertFalse(body["worker_ready"])
        self.assertEqual(body["worker_payout_status"]["mode"], "disabled")

    def test_admin_dispute_resolution_requires_step_up_and_manual_settlement_reference(self):
        db = self.module.get_db()
        token = "tok-admin-dispute"
        try:
            db.execute("INSERT INTO users (id,email,password_hash,name,is_admin) VALUES (1,'admin@example.com',?,'Admin',1)", [self.module.hash_password('AdminPassword123!')])
            db.execute("INSERT INTO users (id,email,password_hash,name) VALUES (2,'worker@example.com','x','Worker')")
            db.execute("INSERT INTO worker_profiles (user_id) VALUES (2)")
            db.execute("INSERT INTO users (id,email,password_hash,name) VALUES (3,'employer@example.com','x','Employer')")
            db.execute("INSERT INTO employer_profiles (user_id) VALUES (3)")
            db.execute("INSERT INTO jobs (id,employer_id,title,description,category,budget_type,budget_amount,status) VALUES (5,3,'Disputed job','Desc','writing','fixed',100,'hired')")
            db.execute("INSERT INTO orders (id,type,job_id,worker_id,employer_id,status,total_amount) VALUES (9,'job_hire',5,2,3,'disputed',100)")
            db.execute("INSERT INTO escrow_holds (order_id,amount,status,stripe_payment_intent_id) VALUES (9,100,'held','pi_live_or_manual')")
            db.execute("INSERT INTO sessions (user_id,token,expires_at) VALUES (1,?,datetime('now','+1 day'))", [token])
            db.commit()
        finally:
            db.close()

        def call(payload):
            self.module._request_ctx.request_method = "POST"
            self.module._request_ctx.path_info = "/admin/resolve-dispute"
            self.module._request_ctx.query_string = ""
            self.module._request_ctx.http_authorization = f"Bearer {token}"
            self.module._request_ctx.stdin_data = json.dumps(payload)
            self.module._request_ctx.content_type = "application/json"
            self.module._request_ctx.content_length = str(len(self.module._request_ctx.stdin_data))
            self.module._request_ctx.remote_addr = "203.0.113.17"
            if hasattr(self.module._request_ctx, 'body_cache'):
                delattr(self.module._request_ctx, 'body_cache')
            if hasattr(self.module._request_ctx, 'raw_body'):
                delattr(self.module._request_ctx, 'raw_body')
            with contextlib.redirect_stdout(io.StringIO()) as out:
                self.module.handle_request()
            return parse_cgi_output(out.getvalue())

        payloads = [
            {"order_id": 9, "resolution": "release_to_worker"},
            {"order_id": 9, "resolution": "release_to_worker", "admin_password": "AdminPassword123!"},
            {"order_id": 9, "resolution": "split", "worker_percent": 50,
             "admin_password": "AdminPassword123!", "manual_money_movement_confirmed": True,
             "processor_reference": "stripe-tr-123"},
        ]
        for payload in payloads:
            status, body = call(payload)
            self.assertEqual(status, 503, body)
            self.assertIn("remain disabled", body["error"])
        db = self.module.get_db()
        try:
            self.assertEqual(db.execute("SELECT status FROM orders WHERE id=9").fetchone()[0], "disputed")
            self.assertEqual(db.execute("SELECT status FROM escrow_holds WHERE order_id=9").fetchone()[0], "held")
            self.assertEqual(db.execute("SELECT COUNT(*) FROM audit_log WHERE action LIKE 'resolve_dispute%'").fetchone()[0], 0)
            self.assertEqual(db.execute("SELECT COUNT(*) FROM notifications WHERE type='dispute_resolved'").fetchone()[0], 0)
        finally:
            db.close()

    def test_employer_payment_setup_handles_stripe_setup_intent(self):
        text = (REPO_ROOT / "frontend/index.html").read_text(encoding="utf-8", errors="ignore")
        required = [
            "overlay.className = 'modal-overlay active'",
            "<div class=\"modal\" style=\"max-width:520px\">",
            "Opening secure setup...",
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

    def test_stripe_customer_without_payment_method_is_not_payment_ready(self):
        db = self.module.get_db()
        try:
            db.execute("INSERT INTO users (id,email,password_hash,name) VALUES (2,'employer@example.com','x','Employer')")
            db.execute("INSERT INTO employer_profiles (user_id,stripe_customer_id,payment_method_id) VALUES (2,'cus_test_only',NULL)")
            self.module.STRIPE_AVAILABLE = True
            self.module.STRIPE_SECRET_KEY = "sk_test_configured"
            self.assertFalse(self.module.employer_has_payment_setup(db, 2))
            db.execute("UPDATE employer_profiles SET payment_method_id='pm_test_confirmed' WHERE user_id=2")
            self.assertTrue(self.module.employer_has_payment_setup(db, 2))
        finally:
            db.close()

    def test_hire_requires_confirmed_payment_method_not_just_stripe_customer(self):
        db = self.module.get_db()
        token = "tok-employer"
        try:
            db.execute("INSERT INTO users (id,email,password_hash,name) VALUES (1,'worker@example.com','x','Worker')")
            db.execute("INSERT INTO worker_profiles (user_id) VALUES (1)")
            db.execute("INSERT INTO users (id,email,password_hash,name) VALUES (2,'employer@example.com','x','Employer')")
            db.execute("INSERT INTO employer_profiles (user_id,stripe_customer_id,payment_method_id) VALUES (2,'cus_test_only',NULL)")
            db.execute("INSERT INTO sessions (user_id,token,expires_at) VALUES (2,?,datetime('now','+1 day'))", [token])
            db.execute("INSERT INTO jobs (id,employer_id,title,description,category,budget_type,budget_amount,status) VALUES (7,2,'QA Job','Desc','testing','fixed',25,'reviewing')")
            db.execute("INSERT INTO applications (job_id,worker_id,cover_message,status) VALUES (7,1,'I can help','pending')")
            db.commit()
        finally:
            db.close()

        self.module.STRIPE_AVAILABLE = True
        self.module.STRIPE_SECRET_KEY = "configured-test-key"
        self.module.JOB_HIRING_ENABLED = True
        payload = json.dumps({"application_id": 1})
        self.module._request_ctx.request_method = "POST"
        self.module._request_ctx.path_info = "/jobs/7/hire"
        self.module._request_ctx.query_string = ""
        self.module._request_ctx.http_authorization = f"Bearer {token}"
        self.module._request_ctx.http_x_api_key = ""
        self.module._request_ctx.stdin_data = payload
        self.module._request_ctx.content_type = "application/json"
        self.module._request_ctx.content_length = str(len(payload))
        self.module._request_ctx.remote_addr = "127.0.0.1"
        with contextlib.redirect_stdout(io.StringIO()) as out:
            self.module.handle_request()
        status, body = parse_cgi_output(out.getvalue())
        self.assertEqual(status, 402, body)
        self.assertIn("payment method", body["error"].lower())
        db = self.module.get_db()
        try:
            self.assertEqual(db.execute("SELECT COUNT(*) FROM orders").fetchone()[0], 0)
            self.assertEqual(db.execute("SELECT status FROM applications WHERE job_id=7 AND worker_id=1").fetchone()[0], "pending")
            self.assertEqual(db.execute("SELECT status FROM jobs WHERE id=7").fetchone()[0], "reviewing")
        finally:
            db.close()

    def test_simulated_escrow_is_disabled_in_production_when_stripe_missing(self):
        db = self.module.get_db()
        try:
            db.execute("INSERT INTO users (id,email,password_hash,name) VALUES (2,'employer@example.com','x','Employer')")
            db.execute("INSERT INTO employer_profiles (user_id,stripe_customer_id,payment_method_id) VALUES (2,'cus_sim_only','pm_sim_only')")
            self.module.STRIPE_AVAILABLE = False
            self.module.STRIPE_SECRET_KEY = ""
            self.module.PRODUCTION_MODE = True
            with self.assertRaisesRegex(ValueError, "simulated escrow is disabled in production"):
                self.module.fund_escrow_stripe(db, 2, 25, 99, None, "Test escrow")
            self.assertEqual(db.execute("SELECT COUNT(*) FROM escrow_holds").fetchone()[0], 0)
        finally:
            db.close()

    def test_production_payment_setup_refuses_simulated_employer_records_without_stripe(self):
        db = self.module.get_db()
        token = "tok-employer"
        try:
            db.execute("INSERT INTO users (id,email,password_hash,name) VALUES (2,'employer@example.com','x','Employer')")
            db.execute("INSERT INTO sessions (user_id,token,expires_at) VALUES (2,?,datetime('now','+1 day'))", [token])
            db.commit()
        finally:
            db.close()

        self.module.STRIPE_AVAILABLE = False
        self.module.STRIPE_SECRET_KEY=""
        self.module.PRODUCTION_MODE = True
        self.module._request_ctx.request_method = "POST"
        self.module._request_ctx.path_info = "/payments/setup-employer"
        self.module._request_ctx.query_string = ""
        self.module._request_ctx.http_authorization = f"Bearer {token}"
        self.module._request_ctx.http_x_api_key = ""
        self.module._request_ctx.stdin_data = "{}"
        self.module._request_ctx.content_type = "application/json"
        self.module._request_ctx.content_length = "2"
        self.module._request_ctx.remote_addr = "127.0.0.1"
        with contextlib.redirect_stdout(io.StringIO()) as out:
            self.module.handle_request()
        status, body = parse_cgi_output(out.getvalue())
        self.assertEqual(status, 503, body)
        self.assertIn("simulated employer payment setup is disabled", body["error"])
        db = self.module.get_db()
        try:
            ep = db.execute("SELECT stripe_customer_id, payment_method_id FROM employer_profiles WHERE user_id=2").fetchone()
            if ep is not None:
                self.assertIsNone(ep["stripe_customer_id"])
                self.assertIsNone(ep["payment_method_id"])
        finally:
            db.close()

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
            "No public jobs right now",
            "Create a worker profile",
            "open job${total === 1 ? '' : 's'} · newest first",
            "worker_jobs_apply_cta_click",
            "worker_job_card_apply_click",
            "Apply now",
            "const sortedJobs = [...jobs].sort",
        ]:
            self.assertIn(snippet, text)
        for contradictory in ["New paid jobs", "View open jobs"]:
            self.assertNotIn(contradictory, text)


    def test_public_homepage_visual_cleanup_invariants(self):
        text = (REPO_ROOT / "frontend/index.html").read_text(encoding="utf-8", errors="ignore")
        for snippet in [
            'data-simplified-home="true"',
            "Describe the work. Hire the right human.",
            "What do you need help with?",
            "Clear scope, visible evidence, buyer approval.",
            "A clear route for workers and agents.",
            "homepage_describe_task_primary_click",
            "homepage_high_intent_route_click",
        ]:
            self.assertIn(snippet, text)
        self.assertEqual(text.count('data-home-section="'), 5)
        self.assertNotIn("Four ways to start small.", text)
        self.assertNotIn("GA4 shows visitors", text)
        self.assertNotIn("High-intent visitors", text)

    def test_market_discovery_pages_capture_open_ended_demand(self):
        required = {
            "frontend/index.html": [
                "lp-market-discovery",
                "homepage_request_any_task_click",
                "homepage_task_ideas_click",
                "Describe any task you need a human to do or check",
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
            "frontend/use-cases/ai-output-fact-checking.html": ["Hire a human to fact-check AI output", "seo_use_case_draft_click", "AI-output fact-checking review", "location.href='/#/post-job?'+p.toString();"],
            "frontend/use-cases/human-review-for-chatbot-responses.html": ["Human review for chatbot", "seo_use_case_draft_click", "location.href='/#/post-job?'+p.toString();"],
            "frontend/use-cases/hire-human-to-test-signup-flow.html": ["Hire a human to test your signup flow", "Website signup flow QA quick check", "location.href='/#/post-job?'+p.toString();"],
            "frontend/use-cases/source-checking-for-ai-research.html": ["Source checking for AI-assisted research", "Source-check AI-assisted research", "location.href='/#/post-job?'+p.toString();"],
            "frontend/use-cases/ai-agent-human-in-the-loop-tasks.html": ["Human fallback tasks for AI agents", "Human-in-the-loop verification task", "location.href='/#/post-job?'+p.toString();"],
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

    def test_public_intent_ctas_do_not_emit_executive_conversion_events(self):
        executive_event_call = re.compile(
            r"(?:trackGHH|trackBlogCTA)\(\s*['\"](?:generate_lead|qualify_lead|close_convert_lead|purchase)['\"]"
            r"|gtag\(\s*['\"]event['\"]\s*,\s*['\"](?:generate_lead|qualify_lead|close_convert_lead|purchase)['\"]"
        )
        executive_violations = {}
        misrouted_post_task_ctas = {}
        for page in sorted((REPO_ROOT / "frontend").rglob("*.html")):
            if page.name == "index.html" and page.parent == REPO_ROOT / "frontend":
                continue
            text = page.read_text(encoding="utf-8", errors="ignore")
            matches = executive_event_call.findall(text)
            if matches:
                executive_violations[str(page.relative_to(REPO_ROOT))] = matches
            bad_post_task_links = []
            for anchor in re.findall(r"<a\b[^>]*>", text, re.IGNORECASE):
                if "post_task_cta_click" not in anchor:
                    continue
                href = re.search(r"\bhref\s*=\s*['\"]([^'\"]+)['\"]", anchor, re.IGNORECASE)
                if href is None or "post-job" not in href.group(1):
                    bad_post_task_links.append(anchor[:240])
            if bad_post_task_links:
                misrouted_post_task_ctas[str(page.relative_to(REPO_ROOT))] = bad_post_task_links
        self.assertEqual(executive_violations, {})
        self.assertEqual(misrouted_post_task_ctas, {})

    def test_faq_qualifies_checkout_and_privacy_sharing_claims(self):
        faq = (REPO_ROOT / "frontend/faq.html").read_text(encoding="utf-8", errors="ignore")
        self.assertNotIn("All payments are processed through Stripe", faq)
        self.assertNotIn("never shared with third parties", faq)
        self.assertIn("Where GoHireHumans checkout is configured, Stripe processes payments", faq)
        self.assertIn("published listings and task content may be public", faq)
        self.assertIn("service providers and marketplace participants as described in the Privacy Policy", faq)

    def test_starter_offer_taxonomy_matches_pricing_and_draft_defaults(self):
        pricing = (REPO_ROOT / "frontend/pricing.html").read_text(encoding="utf-8", errors="ignore")
        starter = (REPO_ROOT / "frontend/starter-offers.html").read_text(encoding="utf-8", errors="ignore")
        app = (REPO_ROOT / "frontend/index.html").read_text(encoding="utf-8", errors="ignore")
        website_qa = (REPO_ROOT / "frontend/use-cases/website-qa-task.html").read_text(encoding="utf-8", errors="ignore")
        lead_research = (REPO_ROOT / "frontend/use-cases/lead-research-microtask.html").read_text(encoding="utf-8", errors="ignore")
        canonical_offers = {
            "AI Output Verification": ("ai_review", "99"),
            "Automation QA Sprint": ("automation_verification", "199"),
            "Clay/GTM QA Sprint": ("clay_gtm_qa", "199"),
            "Real-World Check": ("phone_fact_check", "79"),
        }
        for offer, (template, amount) in canonical_offers.items():
            self.assertIn(f"<h3>{offer}</h3>", pricing)
            self.assertIn(f"<h3>{offer}</h3>", starter)
            match = re.search(
                rf"\b{re.escape(template)}:\s*\{{.*?budget_amount:\s*['\"](\d+)['\"]",
                app,
                re.DOTALL,
            )
            if match is None:
                self.fail(f"Missing draft template budget for {template}")
            self.assertEqual(match.group(1), amount, template)
        self.assertNotIn("<h3>Website QA Sprint</h3>", pricing)
        self.assertNotIn("<h3>Lead List Verification</h3>", pricing)
        self.assertIn("template=automation_verification", website_qa)
        self.assertNotIn("template=website_qa", website_qa)
        self.assertIn("template=clay_gtm_qa", lead_research)
        self.assertNotIn("template=lead_qualification", lead_research)

    def test_pricing_avoids_unsourced_competitor_rate_claims(self):
        pricing = (REPO_ROOT / "frontend/pricing.html").read_text(encoding="utf-8", errors="ignore")
        for name in ["Upwork", "Fiverr", "TaskRabbit"]:
            self.assertNotIn(name, pricing)
        self.assertIn("Other marketplaces", pricing)
        self.assertIn("Varies; confirm current terms", pricing)
        self.assertIn("1% + Stripe processing where checkout is configured", pricing)

    def test_first_orders_conversion_infrastructure_is_discoverable(self):
        required = {
            "frontend/index.html": [
                "homepage_starter_offers_click",
                "homepage_sample_deliverables_click",
                "homepage_proof_packs_click",
                "What a strong application says",
                "job_application_cover_focus",
            ],
            "frontend/starter-offers.html": [
                'data-starter-simplified="true"',
                "Start small when the work needs proof.",
                "starter_offer_draft_click",
                "AI Output Verification",
                "Automation QA Sprint",
                "Real-World Check",
            ],
            "frontend/pricing.html": [
                'data-pricing-order="fee-first"',
                "Prefer a fixed starting point?",
                "pricing_proof_first_cta_click",
                "clay_gtm_qa",
            ],
            "frontend/use-cases/hire-human-to-review-ai-output.html": ["AI-output review proof pack", "template=ai_review", "post_task_cta_click"],
            "frontend/use-cases/website-qa-task.html": ["Website QA proof pack", "template=automation_verification", "post_task_cta_click"],
            "frontend/use-cases/lead-research-microtask.html": ["Lead research proof pack", "template=clay_gtm_qa", "post_task_cta_click"],
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
                "homepage_starter_offers_click",
                "homepage_sample_deliverables_click",
                "homepage_high_intent_route_click",
                "Clear scope, visible evidence, buyer approval.",
                "A clear route for workers and agents.",
            ],
            "frontend/post-a-small-task.html": [
                "Humans who verify what your AI produces",
                "first_task_template_click",
                "Draft AI QA task",
                "Draft automation QA task",
                "Draft data cleanup task",
                "Draft phone/fact-check task",
                "href=\"/#/post-job?template=lead_qualification\"",
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

    def test_schema_initialization_never_resets_or_promotes_an_existing_account(self):
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
            self.assertEqual(dict(user), {
                "email": "enzo@profilesearch.com",
                "password_hash": "old",
                "is_admin": 0,
                "is_active": 0,
                "is_suspended": 1,
                "is_banned": 1,
            })
        finally:
            db.close()

    def test_seed_requires_explicit_admin_credentials_before_any_insert(self):
        self.module.SEED_SECRET = "seed-test-secret"
        payload = json.dumps({"secret": "seed-test-secret"})
        self.module._request_ctx.request_method = "POST"
        self.module._request_ctx.path_info = "/seed"
        self.module._request_ctx.query_string = ""
        self.module._request_ctx.http_authorization = ""
        self.module._request_ctx.http_x_api_key = ""
        self.module._request_ctx.stdin_data = payload
        self.module._request_ctx.content_type = "application/json"
        self.module._request_ctx.content_length = str(len(payload))
        self.module._request_ctx.remote_addr = "127.0.0.1"
        with contextlib.redirect_stdout(io.StringIO()) as out:
            self.module.handle_request()
        status, body = parse_cgi_output(out.getvalue())
        self.assertEqual(status, 400, body)
        self.assertIn("admin credentials", body["error"].lower())
        db = self.module.get_db()
        try:
            self.assertEqual(db.execute("SELECT COUNT(*) FROM users").fetchone()[0], 0)
        finally:
            db.close()

    def test_seed_accepts_only_caller_supplied_strong_admin_credentials(self):
        self.module.SEED_SECRET = "seed-test-secret"
        admin_password = "StrongSeed1!Pass"
        payload = json.dumps({
            "secret": "seed-test-secret",
            "admin_email": "owner@example.com",
            "admin_password": admin_password,
            "admin_name": "Owner",
        })
        self.module._request_ctx.request_method = "POST"
        self.module._request_ctx.path_info = "/seed"
        self.module._request_ctx.query_string = ""
        self.module._request_ctx.http_authorization = ""
        self.module._request_ctx.http_x_api_key = ""
        self.module._request_ctx.stdin_data = payload
        self.module._request_ctx.content_type = "application/json"
        self.module._request_ctx.content_length = str(len(payload))
        self.module._request_ctx.remote_addr = "127.0.0.1"
        with contextlib.redirect_stdout(io.StringIO()) as out:
            self.module.handle_request()
        status, body = parse_cgi_output(out.getvalue())
        self.assertEqual(status, 201, body)
        self.assertEqual(body["admin"]["email"], "owner@example.com")
        self.assertNotIn(admin_password, json.dumps(body))
        db = self.module.get_db()
        try:
            user = db.execute("SELECT email,password_hash,is_admin FROM users WHERE email='owner@example.com'").fetchone()
            self.assertIsNotNone(user)
            self.assertEqual(user["is_admin"], 1)
            self.assertNotEqual(user["password_hash"], admin_password)
            self.assertTrue(self.module.verify_password(admin_password, user["password_hash"]))
        finally:
            db.close()

    def test_seed_helpers_embed_no_deterministic_sample_passwords(self):
        source = MODULE_PATH.read_text(encoding="utf-8")
        self.assertNotIn("Worker1234", source)
        self.assertNotIn("Employer1234", source)
        self.assertNotIn("Admin1234", source)
        self.assertNotRegex(source, r"hash_password\(\s*['\"][^'\"]+['\"]\s*\)")

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
            "frontend/ai-integration.html": None,
            "frontend/use-cases/index.html": None,
            "frontend/about.html": None,
            "frontend/faq.html": None,
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
            str(path.relative_to(REPO_ROOT)): None
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

    def test_public_logo_shell_is_owned_and_accessible_once(self):
        css = (REPO_ROOT / "frontend/style.css").read_text(encoding="utf-8", errors="ignore")
        required_css = [
            ".lp-nav-logo {",
            "font-family: system-ui, -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif !important;",
            "font-weight: 700 !important; font-size: 15px; line-height: 28px;",
            "color: #1a1816 !important; text-decoration: none !important;",
            ".lp-nav-logo svg {",
            "width: 28px; height: 28px; flex: 0 0 28px;",
            "max-width: none; display: block;",
            ".lp-nav-logo:hover,",
            ".lp-nav-logo:focus-visible {",
        ]
        missing = [snippet for snippet in required_css if snippet not in css]
        self.assertEqual(missing, [])
        offenders = []
        for path in (REPO_ROOT / "frontend").rglob("*.html"):
            text = path.read_text(encoding="utf-8", errors="ignore")
            if "lp-nav-logo" in text and 'aria-label="GoHireHumans"' in text:
                offenders.append(str(path.relative_to(REPO_ROOT)))
        self.assertEqual(offenders, [])

    @staticmethod
    def _sitemap_urls():
        root = ET.parse(REPO_ROOT / "frontend/sitemap.xml").getroot()
        return [
            element.text.strip()
            for element in root.iter()
            if element.tag.rsplit("}", 1)[-1] == "loc" and element.text
        ]

    @staticmethod
    def _html_declares_noindex(text):
        class RobotsMetaParser(HTMLParser):
            has_noindex = False

            def handle_starttag(self, tag, attrs):
                if tag.lower() != "meta":
                    return
                attributes = {
                    key.lower(): (value or "").lower()
                    for key, value in attrs
                    if key
                }
                if attributes.get("name") not in {"robots", "googlebot"}:
                    return
                directives = attributes.get("content", "").replace(",", " ").replace(";", " ").split()
                if "noindex" in directives:
                    self.has_noindex = True

        parser = RobotsMetaParser()
        parser.feed(text)
        return parser.has_noindex

    def test_sitemap_urls_are_unique(self):
        urls = self._sitemap_urls()
        self.assertGreater(len(urls), 0)
        self.assertIn("https://www.gohirehumans.com/", urls)
        seen = set()
        duplicates = set()
        for url in urls:
            if url in seen:
                duplicates.add(url)
            seen.add(url)
        self.assertEqual(sorted(duplicates), [])

    def test_sitemapped_html_pages_do_not_opt_out_of_indexing(self):
        urls = self._sitemap_urls()
        self.assertGreater(len(urls), 0)
        offenders = []
        public_origin = "https://www.gohirehumans.com"
        for url in urls:
            self.assertTrue(url.startswith(public_origin), url)
            loc = url.removeprefix(public_origin)
            if loc in ("", "/"):
                page = REPO_ROOT / "frontend/index.html"
            elif loc.endswith("/"):
                page = REPO_ROOT / f"frontend{loc}index.html"
            elif loc.endswith(".html"):
                page = REPO_ROOT / f"frontend{loc}"
            else:
                continue
            if not page.exists():
                continue
            text = page.read_text(encoding="utf-8", errors="ignore")
            if self._html_declares_noindex(text):
                offenders.append(str(page.relative_to(REPO_ROOT)))
        self.assertEqual(offenders, [])

    def test_sitemap_noindex_guard_recognizes_attribute_order_and_directive_tokens(self):
        samples = [
            '<meta name="robots" content="noindex, nofollow">',
            '<meta content="follow, NOINDEX" name="robots">',
        ]
        for sample in samples:
            with self.subTest(sample=sample):
                self.assertTrue(self._html_declares_noindex(sample))
        self.assertFalse(self._html_declares_noindex('<meta name="robots" content="index, follow">'))

    def test_sitemapped_html_pages_use_single_canonical_public_nav(self):
        expected_labels = [
            "GoHireHumans",
            "Marketplace",
            "Find Work",
            "For Agents",
            "Pricing",
            "Trust",
            "Sign in",
            "Post a task",
        ]
        expected_mobile_labels = [
            "Marketplace", "Find Work", "For Agents", "Pricing", "Trust",
            "Starter Offers", "Use Cases", "FAQ", "Sign in", "Post a task",
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
            if text.count('function toggleMobileMenu(forceOpen)') != 1:
                missing.append("exactly one mobile menu toggle")
            if 'aria-controls="mobileMenu" aria-expanded="false"' not in text:
                missing.append("hamburger exposes menu expanded state")
            if 'id="mobileMenu" style="display:none" hidden' not in text:
                missing.append("mobile menu starts hidden semantically")
            if '<nav class="lp-nav" aria-label="Main navigation">' not in nav:
                missing.append("first nav uses canonical lp-nav + aria label")
            if labels[:8] != expected_labels:
                missing.append(f"top nav labels {labels[:8]!r}")
            mobile = re.search(r'<div class="lp-mobile-menu"[^>]*>(.*?)</div></div>', text, flags=re.S | re.I)
            mobile_labels = self._nav_labels(mobile.group(1) if mobile else "")
            if mobile_labels != expected_mobile_labels:
                missing.append(f"mobile nav labels {mobile_labels!r}")
            if '<nav class="nav"' in nav or 'class="header-nav"' in nav:
                missing.append("legacy top nav class removed")
            if missing:
                failures.append({"file": rel, "missing": missing})
        self.assertEqual(failures, [])

    def test_sitemapped_html_pages_have_extensionless_redirects_before_spa_rewrite(self):
        sitemap = (REPO_ROOT / "frontend/sitemap.xml").read_text(encoding="utf-8", errors="ignore")
        vercel = json.loads((REPO_ROOT / "frontend/vercel.json").read_text(encoding="utf-8"))
        redirects = {(r.get("source"), r.get("destination"), r.get("permanent")) for r in vercel.get("redirects", [])}
        missing = []
        for loc in re.findall(r"<loc>https://www\.gohirehumans\.com([^<]*\.html)</loc>", sitemap):
            source = loc[:-5]
            if source.endswith("/index"):
                continue
            if (source, loc, True) not in redirects:
                missing.append({"source": source, "destination": loc})
        self.assertEqual(missing, [])
        rewrite_sources = [r.get("source") for r in vercel.get("rewrites", [])]
        self.assertEqual(rewrite_sources, [])
        self.assertNotIn("/((?!api/)(?!.*\\.).*)", json.dumps(vercel))
        redirects_index = next(i for i, r in enumerate(vercel.get("redirects", [])) if r.get("source") == "/about")
        self.assertGreaterEqual(redirects_index, 0)

    def test_phase3_404_landmark_and_footer_polish_invariants(self):
        index = (REPO_ROOT / "frontend/index.html").read_text(encoding="utf-8", errors="ignore")
        css = (REPO_ROOT / "frontend/style.css").read_text(encoding="utf-8", errors="ignore")
        vercel = json.loads((REPO_ROOT / "frontend/vercel.json").read_text(encoding="utf-8"))
        required_index = [
            '<a class="skip-link" href="#main-content">Skip to content</a>',
            '<main id="main-content" tabindex="-1">',
            'function renderAppNotFound(path = \'\')',
            'return renderAppNotFound(path);',
            'We could not find <code>${safePath}</code>',
            'href="/starter-offers.html">Starter Offers</a>',
            'Find Work</button>',
            'class="lp-footer-meta">Founded 2026 &middot; United States &middot; <a href="mailto:contact@gohirehumans.com">contact@gohirehumans.com</a>',
        ]
        required_css = [
            '.skip-link {',
            '.skip-link:focus { top: var(--space-4); }',
            '#main-content:focus { outline: none; }',
            '.lp-footer-tagline {\n  font-size: var(--text-xs); color: var(--color-text-muted);',
            '.lp-footer-links a {\n  font-size: var(--text-sm); color: var(--color-text-muted);',
            '.lp-footer-copy {\n  font-size: var(--text-xs); color: var(--color-text-muted);',
        ]
        missing = [snippet for snippet in required_index if snippet not in index]
        missing += [snippet for snippet in required_css if snippet not in css]
        self.assertEqual(missing, [])
        self.assertNotIn("rewrites", vercel)
        self.assertNotIn("rgba(255,255,255,0.45)", index)
        self.assertNotIn("rgba(255,255,255,0.5)", index)

    def test_use_cases_index_has_self_canonical(self):
        text = (REPO_ROOT / "frontend/use-cases/index.html").read_text(encoding="utf-8", errors="ignore")
        self.assertIn('<link rel="canonical" href="https://www.gohirehumans.com/use-cases/">', text)

    def test_phase4_browser_regression_suite_is_configured(self):
        required_files = [
            "frontend/package.json",
            "frontend/playwright.config.js",
            "frontend/tests/browser-regression.spec.js",
        ]
        missing = [rel for rel in required_files if not (REPO_ROOT / rel).exists()]
        self.assertEqual(missing, [])
        pkg = json.loads((REPO_ROOT / "frontend/package.json").read_text(encoding="utf-8"))
        self.assertIn("test:browser", pkg.get("scripts", {}))
        self.assertIn("@playwright/test", pkg.get("devDependencies", {}))
        self.assertIn("@axe-core/playwright", pkg.get("devDependencies", {}))
        spec = (REPO_ROOT / "frontend/tests/browser-regression.spec.js").read_text(encoding="utf-8")
        for snippet in ["AxeBuilder", "no-such-route-ui-audit", "services-result-count", "Browse Jobs"]:
            self.assertIn(snippet, spec)

    def test_phase5_proof_pack_conversion_layer_exists(self):
        proof = (REPO_ROOT / "frontend/proof-packs.html").read_text(encoding="utf-8", errors="ignore")
        required = [
            "Proof packs for human verification work",
            "AI Output Verification",
            "Automation QA Sprint",
            "Clay/GTM QA Sprint",
            "Real-World Check",
            "Scope card",
            "Evidence log",
            "Issue table",
            "Final recommendation",
        ]
        missing = [snippet for snippet in required if snippet not in proof]
        self.assertEqual(missing, [])
        for rel in ["frontend/index.html", "frontend/starter-offers.html", "frontend/pricing.html", "frontend/sitemap.xml"]:
            self.assertIn("proof-packs.html", (REPO_ROOT / rel).read_text(encoding="utf-8", errors="ignore"), rel)
        starter = (REPO_ROOT / "frontend/starter-offers.html").read_text(encoding="utf-8", errors="ignore")
        for snippet in ["Clay/GTM QA Sprint", 'class="card starter-offer-card"', "clay_gtm_qa_sprint"]:
            self.assertIn(snippet, starter)
        self.assertNotIn("generate_lead", starter)
        self.assertNotIn("qualify_lead", starter)
        vercel = json.loads((REPO_ROOT / "frontend/vercel.json").read_text(encoding="utf-8"))
        redirects = {(r.get("source"), r.get("destination")) for r in vercel.get("redirects", [])}
        self.assertIn(("/proof-packs", "/proof-packs.html"), redirects)

    def test_phase6_first_10_orders_operating_system_exists(self):
        required = {
            "docs/ops/first-10-orders-playbook.md": ["concierge agency wearing a marketplace shell", "Intake checklist", "Order stages", "Kill/review criteria"],
            "docs/ops/proof-pack-template.md": ["Scope card", "Checklist run", "Issue table", "Uncertainty log", "Final recommendation"],
            "docs/ops/buyer-delivery-template.md": ["Your {{sku_name}} proof pack is ready", "Accepted", "Revision requested", "anonymized case study"],
        }
        missing = {}
        for rel, snippets in required.items():
            text = (REPO_ROOT / rel).read_text(encoding="utf-8", errors="ignore")
            absent = [snippet for snippet in snippets if snippet not in text]
            if absent:
                missing[rel] = absent
        self.assertEqual(missing, {})

    def test_phase7_design_system_public_shell_guardrails_exist(self):
        required_files = [
            "frontend/partials/public-nav.html",
            "frontend/partials/public-footer.html",
            "docs/design-system/public-shell.md",
            "scripts/check_public_shell.py",
            "scripts/sync_public_shell.py",
        ]
        missing = [rel for rel in required_files if not (REPO_ROOT / rel).exists()]
        self.assertEqual(missing, [])
        nav = (REPO_ROOT / "frontend/partials/public-nav.html").read_text(encoding="utf-8", errors="ignore")
        footer = (REPO_ROOT / "frontend/partials/public-footer.html").read_text(encoding="utf-8", errors="ignore")
        contract = (REPO_ROOT / "docs/design-system/public-shell.md").read_text(encoding="utf-8", errors="ignore")
        checker = (REPO_ROOT / "scripts/check_public_shell.py").read_text(encoding="utf-8", errors="ignore")
        syncer = (REPO_ROOT / "scripts/sync_public_shell.py").read_text(encoding="utf-8", errors="ignore")
        for token in ["Marketplace", "Find Work", "For Agents", "Pricing", "Trust", "Post a task", "lp-nav"]:
            self.assertIn(token, nav)
        for token in ["Find Work", "Starter Offers", "contact@gohirehumans.com", "Direct-payment instructions are not allowed"]:
            self.assertIn(token, footer)
            self.assertIn(token, checker)
        for token in ["marketplace-first", "Find Work", "Use-case pages remain grouped"]:
            self.assertIn(token, contract)
        for token in ["public-nav.html", "lp-mobile-menu", "GENERATED_DIRS"]:
            self.assertIn(token, syncer)

    def test_public_micro_polish_batch_b_backlog_guardrails_exist(self):
        critical_pages = [
            "frontend/index.html",
            "frontend/starter-offers.html",
            "frontend/pricing.html",
            "frontend/trust-safety.html",
            "frontend/proof-packs.html",
            "frontend/stats.html",
            "frontend/use-cases/index.html",
        ]
        problems = {}
        for rel in critical_pages:
            text = (REPO_ROOT / rel).read_text(encoding="utf-8", errors="ignore")
            issues = []
            for snippet in ['<div class="lp-nav-wrap">', '<footer class="lp-footer"', 'Find Work', 'Post a task']:
                if snippet not in text:
                    issues.append(f"missing {snippet}")
            if "Created with Perplexity Computer" in text or "Perplexity Computer" in text:
                issues.append("builder attribution leaked")
            if '<div class="footer">' in text or '<footer class="footer"' in text:
                issues.append("legacy local footer present")
            if '<li><a href="/#/jobs">Open Jobs</a></li>' in text:
                issues.append("ambiguous jobs label")
            problems[rel] = issues
        self.assertEqual({rel: issues for rel, issues in problems.items() if issues}, {})
        stats = (REPO_ROOT / "frontend/stats.html").read_text(encoding="utf-8", errors="ignore")
        self.assertIn("Create a free account", stats)
        self.assertNotIn("Get started", stats)
        trust = (REPO_ROOT / "frontend/trust-safety.html").read_text(encoding="utf-8", errors="ignore")
        self.assertIn("trust-next-step-heading", trust)
        self.assertIn("Start with a small, reviewable task", trust)
        self.assertIn("Post a task", trust)
        use_cases = (REPO_ROOT / "frontend/use-cases/index.html").read_text(encoding="utf-8", errors="ignore")
        self.assertIn("Browse high-intent task categories", use_cases)
        self.assertIn('<footer class="lp-footer"', use_cases)
        browser_spec = (REPO_ROOT / "frontend/tests/browser-regression.spec.js").read_text(encoding="utf-8", errors="ignore")
        for snippet in ["Batch B public shell backlog polish", "/use-cases/", "Find Work", "Create a free account"]:
            self.assertIn(snippet, browser_spec)

    def test_public_micro_polish_batch_a_guardrails_exist(self):
        footer = (REPO_ROOT / "frontend/partials/public-footer.html").read_text(encoding="utf-8", errors="ignore")
        index = (REPO_ROOT / "frontend/index.html").read_text(encoding="utf-8", errors="ignore")
        stats = (REPO_ROOT / "frontend/stats.html").read_text(encoding="utf-8", errors="ignore")
        browser_spec = (REPO_ROOT / "frontend/tests/browser-regression.spec.js").read_text(encoding="utf-8", errors="ignore")
        css = (REPO_ROOT / "frontend/style.css").read_text(encoding="utf-8", errors="ignore")
        required_footer = [
            'class="lp-footer"',
            'contact@gohirehumans.com',
            'Workers receive the listed payout',
            'Direct-payment instructions are not allowed',
        ]
        for snippet in required_footer:
            self.assertIn(snippet, footer)
            self.assertIn(snippet, index)
        self.assertNotIn("Created with Perplexity Computer", footer)
        self.assertNotIn("Created with Perplexity Computer", index)
        for snippet in [
            'auth2-google-wrap',
            'google-signin-divider',
            'id="auth-error" class="auth2-error" role="alert" aria-live="polite" hidden',
            'Signing in…',
            'data?.error || data?.detail || data?.message',
        ]:
            self.assertIn(snippet, index)
        for snippet in [".auth2-error", ".auth2-divider", ".lp-footer-meta"]:
            self.assertIn(snippet, css)
        for snippet in [
            "chart-fallback",
            "chart-empty",
            "No category data yet",
        ]:
            self.assertIn(snippet, stats)
        self.assertNotIn("cdn.jsdelivr.net/npm/chart.js", stats)
        for snippet in [
            "public footer and mobile menu polish stay consistent",
            "stats page renders deliberate category chart fallback",
            "auth page only shows OR divider",
        ]:
            self.assertIn(snippet, browser_spec)

    def test_phase8_performance_budget_guardrails_exist(self):
        required_files = ["frontend/performance-budgets.json", "scripts/performance_budget.py"]
        missing = [rel for rel in required_files if not (REPO_ROOT / rel).exists()]
        self.assertEqual(missing, [])
        budgets = json.loads((REPO_ROOT / "frontend/performance-budgets.json").read_text(encoding="utf-8"))
        self.assertLessEqual((REPO_ROOT / "frontend/index.html").stat().st_size, budgets["homepage_max_bytes"])
        self.assertLessEqual((REPO_ROOT / "frontend/style.css").stat().st_size, budgets["style_css_max_bytes"])

    def test_homepage_public_nav_template_keeps_desktop_and_mobile_active_states(self):
        text = (REPO_ROOT / "frontend/index.html").read_text(encoding="utf-8", errors="ignore")
        snippets = [
            '<nav class="lp-nav" aria-label="Main navigation">',
            "const activeAttrs = activePage === l.key ? ' lp-nav-link-active\" aria-current=\"page' : '';",
            "<a class=\"lp-nav-link${activeAttrs}\"",
            "<a class=\"lp-mobile-link${activeAttrs}\"",
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
            '<a class="btn btn-primary btn-sm" href="/#/post-job">Post a task</a>',
            'function toggleMobileMenu(forceOpen)',
            'aria-controls="mobileMenu" aria-expanded="false"',
            'id="mobileMenu" style="display:none" hidden',
        ]
        shared_links = [
            ("/#/services", "Marketplace"),
            ("/#/jobs", "Find Work"),
            ("/#/ai-employers", "For Agents"),
            ("/pricing.html", "Pricing"),
            ("/trust-safety.html", "Trust"),
        ]
        failures = []
        for rel, active_snippet in pages.items():
            text = (REPO_ROOT / rel).read_text(encoding="utf-8", errors="ignore")
            missing = [snippet for snippet in shared_snippets if snippet not in text]
            for href, label in shared_links:
                pattern = rf'<a class="lp-nav-link(?: lp-nav-link-active)?"(?: aria-current="page")? href="{re.escape(href)}">{re.escape(label)}</a>'
                if not re.search(pattern, text):
                    missing.append(f'public nav link {label} -> {href}')
            if active_snippet and active_snippet not in text:
                missing.append(active_snippet)
            if '<nav class="nav"' in text:
                missing.append('old <nav class="nav"> removed')
            if '<header class="header">' in text and rel == "frontend/404.html":
                missing.append('old 404 header removed')
            if text.count('<div class="lp-nav-wrap">') != 1:
                missing.append('exactly one shared nav wrapper')
            if text.count('function toggleMobileMenu(forceOpen)') != 1:
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

    def test_public_analytics_loads_only_through_the_local_fail_closed_bootstrap(self):
        html_files = sorted((REPO_ROOT / "frontend").rglob("*.html"))
        failures = []
        bootstrap_count = 0
        for path in html_files:
            rel = str(path.relative_to(REPO_ROOT))
            text = path.read_text(encoding="utf-8", errors="ignore")
            if "googletagmanager.com" in text or "google-analytics.com" in text:
                failures.append(f"{rel}: direct Google analytics reference")
            if 'src="/analytics-bootstrap.js"' in text:
                bootstrap_count += 1
            if "%24%7B" in text:
                failures.append(f"{rel}: encoded JavaScript template interpolation")
            if rel != "frontend/index.html":
                for event_name in ("generate_lead", "qualify_lead", "close_convert_lead"):
                    if re.search(
                        rf"(?:window\.)?gtag\(\s*['\"]event['\"]\s*,\s*['\"]{event_name}['\"]",
                        text,
                    ):
                        failures.append(f"{rel}: static upper-funnel key-event proxy {event_name}")
            for match in re.finditer(r'(?:href|src|action)=["\']([^"\']+)["\']', text, re.IGNORECASE):
                url = match.group(1)
                internal = url.startswith(("/", "#")) or "gohirehumans.com" in url
                if internal and re.search(r'(?:\?|&)utm_(?:source|medium|campaign|content|term)=', url, re.IGNORECASE):
                    failures.append(f"{rel}: internal UTM link {url}")
        self.assertEqual(failures, [])
        self.assertEqual(bootstrap_count, 109)

        bootstrap = (REPO_ROOT / "frontend/analytics-bootstrap.js").read_text(encoding="utf-8")
        for snippet in [
            "window.dataLayer = window.dataLayer || []",
            "window.gtag = window.gtag || function",
            "function normalizeEventParams(params)",
            "window.location.origin",
            "https://www.gohirehumans.com",
            "https://gohirehumans.com",
            "www.googletagmanager.com/gtag/js",
        ]:
            self.assertIn(snippet, bootstrap)
        self.assertNotIn("window.location.hostname", bootstrap)
        self.assertLess(
            bootstrap.index("if (!allowedOrigins.has(window.location.origin))"),
            bootstrap.index("window.dataLayer = window.dataLayer || []"),
        )
        generator = (REPO_ROOT / "scripts/generate-marketplace-pulse.py").read_text(encoding="utf-8")
        self.assertIn('<script src="/analytics-bootstrap.js"></script>', generator)
        self.assertNotIn("googletagmanager.com/gtag/js", generator)
        overpaying = (REPO_ROOT / "frontend/tools/are-you-overpaying.html").read_text(encoding="utf-8")
        self.assertIn('id="share-x" href="#"', overpaying)
        self.assertIn('id="share-li" href="#"', overpaying)
        self.assertNotIn("First completed orders", (REPO_ROOT / "frontend/index.html").read_text(encoding="utf-8"))

    def test_homepage_has_low_risk_funnel_analytics_events(self):
        text = (REPO_ROOT / "frontend/index.html").read_text(encoding="utf-8", errors="ignore")
        required_snippets = [
            "function trackEvent(eventName, params = {})",
            "function getStoredAttribution()",
            "function normalizeAnalyticsParams(params = {})",
            "const eventParams = { ...attribution, ...normalizeAnalyticsParams(params) }",
            "gtag('event', eventName, eventParams)",
            "function trackRecommendedEvent(eventName, params = {})",
            "function trackConfiguredKeyEvent(eventName, params = {})",
            "function trackSpaPageView(path)",
            "send_page_view: false",
            "page_path: pagePath",
            "trackSpaPageView(path)",
            "sign_up",
            "generate_lead",
            "browse_humans_cta_click",
            "earn_tasks_page_click",
            "homepage_high_intent_route_click",
            "homepage_starter_offers_click",
            "homepage_sample_deliverables_click",
            "homepage_proof_packs_click",
            "first_order_trust_describe_task_click",
            "first_order_trust_safety_click",
            "concierge_task_draft_click",
            "guided_task_intake_start",
            "guided_task_draft_created",
            "post_service_intent",
            "browse_relevant_jobs_intent",
            "proof_first_router",
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
            "Draft your first task",
            "first_task_blog_cta_click",
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
                "content_id:'hire_web_developer'",
            ],
            "frontend/blog/verified-freelancer-marketplace.html": [
                "Verified Freelancer Marketplace: Trust Signals to Check Before Hiring",
                "content_id:'verified_freelancer_marketplace'",
            ],
            "frontend/blog/on-demand-workforce-platform.html": [
                "On-Demand Workforce Platforms for AI + Human Workflows",
                "content_id:'on_demand_workforce_platform'",
            ],
            "frontend/tools/fee-calculator.html": [
                "Freelancer Fee Calculator: Workers Keep the Listed Payout",
                "content_id:'fee_calculator'",
            ],
        }
        required_shared_snippets = [
            "Turn AI output into a human QA task",
            "/ai-human-qa/",
            "/request-managed-ai-qa.html",
            "internal_cta_click",
            "placement:'seo_high_impression'",
            "destination:'ai_human_qa'",
            "destination:'managed_ai_qa'",
            "No checkout or job is created automatically from this page",
        ]
        failures = []
        for rel, page_snippets in expected_pages.items():
            text = (REPO_ROOT / rel).read_text(encoding="utf-8", errors="ignore")
            for snippet in required_shared_snippets + page_snippets:
                if snippet not in text:
                    failures.append(f"{rel}: missing {snippet}")
            if text.count("gtag('event','internal_cta_click'") != 2:
                failures.append(f"{rel}: expected exactly two tracked high-impression CTAs")
        self.assertEqual(failures, [])

    def test_homepage_has_guided_agent_intake_and_earning_routes(self):
        text = (REPO_ROOT / "frontend/index.html").read_text(encoding="utf-8", errors="ignore")
        for snippet in [
            "Build a task draft",
            "Answer four short prompts.",
            "What needs to be done?",
            "Who could help?",
            "Desired result",
            "Budget range",
            "Review the draft",
            "params.set('draft_title'",
            "query.get('draft_title')",
            "query.get('draft_description')",
            "sessionStorage.setItem('ghh_guided_task_draft'",
            "getStoredGuidedTaskDraft()",
            "A clear route for workers and agents.",
            "Find work or offer a service",
            "How earning works",
            "Post a service",
            "Browse jobs",
            "postServiceIntent()",
            "browse_relevant_jobs_intent",
        ]:
            self.assertIn(snippet, text)

        guided_block = text[text.index('id="guided-task-intake"'):text.index('data-home-section="how"')]
        self.assertNotIn("fetch(", guided_block)
        self.assertNotIn("api(", guided_block)
        self.assertNotIn("mailto:", guided_block)
        self.assertIn("does not publish, contact workers, charge a card, or promise a match", guided_block)

    def test_homepage_has_credible_agent_marketplace_liquidity_messaging(self):
        text = (REPO_ROOT / "frontend/index.html").read_text(encoding="utf-8", errors="ignore")
        self.assertNotRegex(text, r"(?i)\bpublic beta listings\b")
        for snippet in [
            "Describe the work. Hire the right human.",
            "Start with QA",
            "Agents can search public listings, recommend opportunities, and prepare requests for human approval.",
            "Employer pays Stripe processing + 1% where configured",
            "where checkout is configured",
        ]:
            self.assertIn(snippet, text)

    def test_homepage_has_agent_native_task_drafts_without_automatic_outreach(self):
        text = (REPO_ROOT / "frontend/index.html").read_text(encoding="utf-8", errors="ignore")
        for snippet in [
            "Build a task draft",
            "Answer four short prompts.",
            "startTaskDraft(templateKey, source = 'homepage_concierge')",
            "concierge_task_draft_click",
            "const templateDraft = getTaskDraftTemplate(query.get('template')) || {};",
            "does not publish, contact workers, charge a card, or promise a match",
            "Draft before publishing or paying",
            "Workers receive the listed payout",
            "Employer pays Stripe processing + 1% where configured",
        ]:
            self.assertIn(snippet, text)
        task_draft_block = text[text.index('id="guided-task-intake"'):text.index('data-home-section="how"')]
        self.assertNotIn("mailto:", task_draft_block)
        self.assertNotIn("fetch(", task_draft_block)
        self.assertNotIn("api(", task_draft_block)

    def test_homepage_public_copy_uses_connector_pricing_framing(self):
        text = (REPO_ROOT / "frontend/index.html").read_text(encoding="utf-8", errors="ignore")
        public_landing = text[:text.index("// ═══════════════════════════════════════════════════════════════\n// SERVICES BROWSE")]
        for snippet in [
            "Workers receive the listed payout",
            "Stripe processing plus a 1% GoHireHumans fee",
            "Employer pays Stripe processing + 1% where configured",
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
        self.assertIn("homepage_high_intent_route_click", home)

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
            nav_script_end = text.find("</script>", text.find('class="lp-nav-wrap"'))
            footer_start = text.find('<footer class="lp-footer"')
            if footer_start < 0:
                footer_start = len(text)
            if nav_script_end < 0 or nav_script_end >= footer_start:
                self.fail(rel)
            page_content = text[nav_script_end + len("</script>"):footer_start]
            self.assertNotIn('href="/#/post-job', page_content, rel)
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
            "draft_title=",
            "draft_description=",
            "stripe.redirectToCheckout",
            "/payments/checkout",
            "create a Stripe session",
            "guaranteed outcomes",
            "platform arbitration",
        ]:
            self.assertNotIn(forbidden, page)
        self.assertLessEqual(page.count('href="/#/post-job'), 2)
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
            "draft_title=",
            "draft_description=",
            "stripe.redirectToCheckout",
            "/payments/checkout",
        ]:
            self.assertNotIn(forbidden, generator)
        self.assertLessEqual(generator.count('href="/#/post-job'), 2)

    def test_frontend_transaction_lifecycle_matches_backend_contract(self):
        frontend = (REPO_ROOT / "frontend/index.html").read_text(encoding="utf-8")
        for required in [
            "Hiring temporarily paused",
            "async function handleHire(jobId, applicationId, budgetType, budgetAmount)",
            "confirmHire(${jobId},${applicationId})",
            "async function confirmHire(jobId, applicationId)",
            "confirmHourlyHire(${jobId},${applicationId})",
            "async function confirmHourlyHire(jobId, applicationId)",
            "body: { application_id: applicationId, weekly_hour_cap: weeklyCap }",
            "body: { application_id: applicationId, milestones:",
            "body: { notes: fd.get('note') }",
            "body: { notes: form.get('message'), deadline_at: deadlineAt }",
            "['in_progress','revision_requested'].includes(order.status)",
            "order.worker_notes",
            "order.employer_notes",
            "if (Array.isArray(value)) return value;",
        ]:
            self.assertIn(required, frontend)
        for stale in [
            "body: { applicant_id: workerId, milestones:",
            "body: { note: fd.get('note') }",
            "body: { message }",
        ]:
            self.assertNotIn(stale, frontend)

    def test_authenticated_submission_and_revision_notes_persist_in_order_detail(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        os.environ["DATABASE_PATH"] = str(Path(tmp.name) / "test.db")
        os.environ["DISABLE_AUTO_SEED"] = "1"
        self.addCleanup(os.environ.pop, "DATABASE_PATH", None)
        self.addCleanup(os.environ.pop, "DISABLE_AUTO_SEED", None)
        module = load_api_core()
        module._db_path_resolved = None
        module._seeded = False
        module.init_db()
        module.PRODUCTION_MODE = True
        module.JOB_HIRING_ENABLED = True
        module.STRIPE_AVAILABLE = True
        module.STRIPE_SECRET_KEY = "configured-test-key"

        class FakePaymentIntent:
            @staticmethod
            def create(**kwargs):
                return type("PaymentIntentResult", (), {
                    "id": "pi_mock_hire",
                    "status": "succeeded",
                    "amount": kwargs["amount"],
                    "amount_received": kwargs["amount"],
                    "currency": kwargs["currency"],
                    "metadata": kwargs["metadata"],
                })()

        module.stripe = type(
            "FakeStripe",
            (),
            {
                "PaymentIntent": FakePaymentIntent,
                "error": type("FakeStripeErrors", (), {"StripeError": Exception}),
            },
        )()

        db = module.get_db()
        try:
            db.execute("INSERT INTO users (id,email,password_hash,name) VALUES (1,'worker@example.com','x','Worker')")
            db.execute("INSERT INTO users (id,email,password_hash,name) VALUES (2,'employer@example.com','x','Employer')")
            db.execute("INSERT INTO worker_profiles (user_id) VALUES (1)")
            db.execute(
                "INSERT INTO employer_profiles (user_id,stripe_customer_id,payment_method_id) VALUES (2,'cus_mock','pm_mock')"
            )
            db.execute(
                "INSERT INTO jobs (id,employer_id,title,description,category,budget_type,budget_amount,status) "
                "VALUES (1,2,'QA navigation','Test mobile navigation','testing','fixed',25,'open')"
            )
            db.execute(
                "INSERT INTO applications (id,job_id,worker_id,cover_message,status) "
                "VALUES (14,1,1,'I can test this','pending')"
            )
            db.execute("INSERT INTO sessions (user_id,token,expires_at) VALUES (1,'tok-worker',datetime('now','+1 day'))")
            db.execute("INSERT INTO sessions (user_id,token,expires_at) VALUES (2,'tok-employer',datetime('now','+1 day'))")
            db.commit()
        finally:
            db.close()

        def request(method, path, token, payload=None):
            body = json.dumps(payload or {})
            for cached in ("body_cache", "raw_body"):
                if hasattr(module._request_ctx, cached):
                    delattr(module._request_ctx, cached)
            module._request_ctx.request_method = method
            module._request_ctx.path_info = path
            module._request_ctx.query_string = ""
            module._request_ctx.http_authorization = f"Bearer {token}"
            module._request_ctx.http_x_api_key = ""
            module._request_ctx.stdin_data = body
            module._request_ctx.content_type = "application/json"
            module._request_ctx.content_length = str(len(body))
            module._request_ctx.remote_addr = "127.0.0.1"
            with contextlib.redirect_stdout(io.StringIO()) as out:
                module.handle_request()
            return parse_cgi_output(out.getvalue())

        with mock.patch.object(module, "flush_transactional_notification_emails"):
            status, response = request(
                "POST",
                "/jobs/1/hire",
                "tok-employer",
                {
                    "application_id": 14,
                    "milestones": [
                        {
                            "description": "Test mobile navigation and attach evidence",
                            "amount": 25,
                        }
                    ],
                },
            )
            self.assertEqual(status, 201, response)
            order_id = response["id"]
            self.assertEqual(response["worker_id"], 1)
            self.assertEqual(response["milestones"][0]["escrow_payment_id"], "pi_mock_hire")

            status, response = request(
                "POST",
                f"/orders/{order_id}/submit",
                "tok-worker",
                {"notes": "Delivered test evidence"},
            )
            self.assertEqual(status, 200, response)
            status, response = request("GET", f"/orders/{order_id}", "tok-employer")
            self.assertEqual(status, 200, response)
            self.assertEqual(response["worker_notes"], "Delivered test evidence")

            status, response = request(
                "POST",
                f"/orders/{order_id}/request-revision",
                "tok-employer",
                {"notes": "Please retest mobile navigation"},
            )
            self.assertEqual(status, 200, response)
            status, response = request("GET", f"/orders/{order_id}", "tok-worker")
            self.assertEqual(status, 200, response)
            self.assertEqual(response["worker_notes"], "Delivered test evidence")
            self.assertEqual(response["employer_notes"], "Please retest mobile navigation")


if __name__ == "__main__":
    unittest.main()
