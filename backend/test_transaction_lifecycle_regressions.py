import contextlib
import io
import json
import os
import sqlite3
import tempfile
import unittest
from unittest import mock

from test_deep_audit_regressions import load_api_core, parse_cgi_output


class TransactionLifecycleRegressionTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        os.environ["DATABASE_PATH"] = os.path.join(self.tmp.name, "test.db")
        os.environ["DISABLE_AUTO_SEED"] = "1"
        self.api = load_api_core()
        self.api._db_path_resolved = None
        self.api._seeded = False
        self.api.init_db()
        self.api.PRODUCTION_MODE = True
        self.api.JOB_HIRING_ENABLED = True
        self.api.STRIPE_AVAILABLE = True
        self.api.STRIPE_SECRET_KEY = "configured-test-key"
        self.payment_create = mock.Mock(return_value=type("PaymentIntentResult", (), {"id": "pi_mock_hire"})())
        self.api.stripe = type("Stripe", (), {
            "PaymentIntent": type("PaymentIntent", (), {"create": self.payment_create}),
            "error": type("Error", (), {"StripeError": Exception}),
        })
        self._seed()

    def tearDown(self):
        self.tmp.cleanup()
        os.environ.pop("DATABASE_PATH", None)
        os.environ.pop("DISABLE_AUTO_SEED", None)

    def _seed(self):
        db = self.api.get_db()
        try:
            db.execute("INSERT INTO users (id,email,name,password_hash) VALUES (1,'worker1@example.com','Worker One','x')")
            db.execute("INSERT INTO users (id,email,name,password_hash) VALUES (2,'employer@example.com','Employer','x')")
            db.execute("INSERT INTO users (id,email,name,password_hash) VALUES (3,'worker2@example.com','Worker Two','x')")
            db.execute("INSERT INTO worker_profiles (user_id) VALUES (1)")
            db.execute("INSERT INTO worker_profiles (user_id) VALUES (3)")
            db.execute("INSERT INTO employer_profiles (user_id,stripe_customer_id,payment_method_id) VALUES (2,'cus_test','pm_test')")
            db.execute("INSERT INTO sessions (user_id,token,expires_at) VALUES (1,'tok-worker',datetime('now','+1 day'))")
            db.execute("INSERT INTO sessions (user_id,token,expires_at) VALUES (2,'tok-employer',datetime('now','+1 day'))")
            db.execute("INSERT INTO jobs (id,employer_id,title,description,category,budget_type,budget_amount,status) VALUES (1,2,'Fixed QA','Test flows','testing','fixed',25,'open')")
            db.execute("INSERT INTO jobs (id,employer_id,title,description,category,budget_type,budget_amount,status) VALUES (2,2,'Other job','Other scope','testing','fixed',10,'open')")
            db.execute("INSERT INTO jobs (id,employer_id,title,description,category,budget_type,budget_amount,status) VALUES (3,2,'Bad cents','Invalid precision','testing','fixed',25.555,'open')")
            db.execute("INSERT INTO jobs (id,employer_id,title,description,category,budget_type,budget_amount,status) VALUES (4,2,'Hourly QA','Desc','testing','hourly',25,'open')")
            db.execute("INSERT INTO applications (id,job_id,worker_id,status) VALUES (14,1,1,'pending')")
            db.execute("INSERT INTO applications (id,job_id,worker_id,status) VALUES (15,2,1,'pending')")
            db.execute("INSERT INTO applications (id,job_id,worker_id,status) VALUES (16,1,3,'shortlisted')")
            db.execute("INSERT INTO applications (id,job_id,worker_id,status) VALUES (17,3,1,'pending')")
            db.execute("INSERT INTO applications (id,job_id,worker_id,status) VALUES (18,4,1,'pending')")
            db.commit()
        finally:
            db.close()

    def request(self, method, path, token="tok-employer", payload=None):
        for cached in ("body_cache", "raw_body"):
            if hasattr(self.api._request_ctx, cached):
                delattr(self.api._request_ctx, cached)
        body = json.dumps(payload or {})
        ctx = self.api._request_ctx
        ctx.request_method = method
        ctx.path_info = path
        ctx.query_string = ""
        ctx.http_authorization = f"Bearer {token}"
        ctx.http_x_api_key = ""
        ctx.stdin_data = body
        ctx.content_type = "application/json"
        ctx.content_length = str(len(body.encode()))
        ctx.remote_addr = "127.0.0.1"
        with contextlib.redirect_stdout(io.StringIO()) as out:
            self.api.handle_request()
        return parse_cgi_output(out.getvalue())

    def hire_job_one(self):
        return self.request("POST", "/jobs/1/hire", payload={
            "application_id": 14,
            "milestones": [{"description": "Delivery", "amount": 25}],
        })

    def test_hire_uses_application_identity_and_enforces_one_order_per_job(self):
        status, result = self.request("POST", "/jobs/1/hire", payload={
            "applicant_id": 1,
            "milestones": [{"description": "Delivery", "amount": 25}],
        })
        self.assertEqual(status, 400, result)
        self.assertIn("application_id", result["error"])

        status, result = self.request("POST", "/jobs/1/hire", payload={
            "application_id": 15,
            "milestones": [{"description": "Delivery", "amount": 25}],
        })
        self.assertEqual(status, 404, result)
        self.assertEqual(self.payment_create.call_count, 0)

        status, result = self.hire_job_one()
        self.assertEqual(status, 201, result)
        self.assertEqual(result["worker_id"], 1)
        self.assertEqual(self.payment_create.call_count, 1)

        db = self.api.get_db()
        try:
            statuses = dict(db.execute("SELECT id,status FROM applications WHERE job_id=1").fetchall())
            self.assertEqual(statuses, {14: "accepted", 16: "rejected"})
            self.assertEqual(db.execute("SELECT status FROM jobs WHERE id=1").fetchone()[0], "hired")
            with self.assertRaises(sqlite3.IntegrityError):
                db.execute("INSERT INTO orders (type,job_id,worker_id,employer_id,status,total_amount) VALUES ('job_hire',1,3,2,'in_progress',25)")
            db.rollback()
        finally:
            db.close()

        status, result = self.hire_job_one()
        self.assertEqual(status, 409, result)
        self.assertEqual(self.payment_create.call_count, 1)

    def test_hire_rejects_non_cent_budget_before_stripe(self):
        status, result = self.request("POST", "/jobs/3/hire", payload={
            "application_id": 17,
            "milestones": [{"description": "Delivery", "amount": 25.55}],
        })
        self.assertEqual(status, 400, result)
        self.assertIn("whole cents", result["error"])
        self.assertEqual(self.payment_create.call_count, 0)

    def test_hourly_hire_funds_exact_disclosed_first_week_at_posted_rate(self):
        status, result = self.request("POST", "/jobs/4/hire", payload={
            "application_id": 18,
            "hourly_rate": 1,
            "weekly_hour_cap": 40,
        })
        self.assertEqual(status, 201, result)
        self.assertEqual(result["hourly_contract"]["hourly_rate"], 25)
        self.assertEqual(result["hourly_contract"]["weekly_hour_cap"], 40)
        self.assertEqual(result["hourly_contract"]["current_week_escrow_amount"], 1000)
        self.api.stripe.PaymentIntent.create.assert_called_once()
        self.assertEqual(self.api.stripe.PaymentIntent.create.call_args.kwargs["amount"], 104000)

    def test_hourly_hire_rejects_out_of_range_weekly_cap_before_stripe(self):
        status, result = self.request("POST", "/jobs/4/hire", payload={
            "application_id": 18,
            "weekly_hour_cap": 169,
        })
        self.assertEqual(status, 400, result)
        self.assertIn("between 1 and 168", result["error"])
        self.api.stripe.PaymentIntent.create.assert_not_called()
        db = self.api.get_db()
        try:
            self.assertEqual(db.execute("SELECT COUNT(*) FROM orders WHERE job_id=4").fetchone()[0], 0)
        finally:
            db.close()

    def test_job_create_and_update_reject_non_cent_budgets(self):
        payload = {
            "title": "Precision test",
            "description": "Reject invalid money precision",
            "category": "testing",
            "budget_type": "fixed",
            "budget_amount": 25.555,
        }
        status, result = self.request("POST", "/jobs", payload=payload)
        self.assertEqual(status, 400, result)
        self.assertIn("whole cents", result["error"])

        status, result = self.request("PUT", "/jobs/2", payload={"budget_amount": 10.001})
        self.assertEqual(status, 400, result)
        db = self.api.get_db()
        try:
            self.assertEqual(db.execute("SELECT budget_amount FROM jobs WHERE id=2").fetchone()[0], 10)
        finally:
            db.close()

    def test_notes_are_validated_and_revision_loop_completes_through_approval(self):
        status, order = self.hire_job_one()
        self.assertEqual(status, 201, order)
        order_id = order["id"]

        for bad_notes in ("   ", 123, "x" * 5001):
            status, result = self.request("POST", f"/orders/{order_id}/submit", "tok-worker", {"notes": bad_notes})
            self.assertEqual(status, 400, result)
        db = self.api.get_db()
        try:
            self.assertEqual(db.execute("SELECT status FROM orders WHERE id=?", [order_id]).fetchone()[0], "in_progress")
        finally:
            db.close()

        status, result = self.request("POST", f"/orders/{order_id}/submit", "tok-worker", {"notes": "  Initial evidence  "})
        self.assertEqual(status, 200, result)
        status, detail = self.request("GET", f"/orders/{order_id}", "tok-employer")
        self.assertEqual(detail["worker_notes"], "Initial evidence")

        status, result = self.request("POST", f"/orders/{order_id}/request-revision", payload={"notes": "   "})
        self.assertEqual(status, 400, result)
        status, detail = self.request("GET", f"/orders/{order_id}", "tok-employer")
        self.assertEqual(detail["status"], "submitted")

        status, result = self.request("POST", f"/orders/{order_id}/request-revision", payload={"notes": "  Retest navigation  "})
        self.assertEqual(status, 200, result)
        status, detail = self.request("GET", f"/orders/{order_id}", "tok-worker")
        self.assertEqual(detail["employer_notes"], "Retest navigation")

        status, result = self.request("POST", f"/orders/{order_id}/submit", "tok-worker", {"notes": "  Revised evidence  "})
        self.assertEqual(status, 200, result)

        release = mock.Mock(return_value=(25.0, 0.25))
        with mock.patch.object(self.api, "release_escrow_to_worker", release), mock.patch.object(self.api, "flush_transactional_notification_emails"):
            status, result = self.request("POST", f"/orders/{order_id}/approve")
            self.assertEqual(status, 200, result)
            status, replay = self.request("POST", f"/orders/{order_id}/approve")
            self.assertEqual(status, 409, replay)
        release.assert_called_once()

        db = self.api.get_db()
        try:
            self.assertEqual(db.execute("SELECT status FROM orders WHERE id=?", [order_id]).fetchone()[0], "completed")
            self.assertEqual(db.execute("SELECT status FROM milestones WHERE order_id=?", [order_id]).fetchone()[0], "approved")
            self.assertEqual(db.execute("SELECT status FROM jobs WHERE id=1").fetchone()[0], "completed")
        finally:
            db.close()

    def test_job_hire_cannot_bypass_submission_with_complete_endpoint(self):
        status, order = self.hire_job_one()
        self.assertEqual(status, 201, order)
        release = mock.Mock(return_value=(25.0, 0.25))
        with mock.patch.object(self.api, "release_escrow_to_worker", release):
            status, result = self.request("POST", f"/orders/{order['id']}/complete")
        self.assertEqual(status, 409, result)
        release.assert_not_called()


if __name__ == "__main__":
    unittest.main()
