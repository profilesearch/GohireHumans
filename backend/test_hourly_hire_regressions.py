import contextlib
import io
import json
import os
import tempfile
import unittest
from unittest import mock

from test_deep_audit_regressions import load_api_core, parse_cgi_output


class JobHiringPauseRegressionTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        os.environ["DATABASE_PATH"] = os.path.join(self.tmp.name, "test.db")
        os.environ["DISABLE_AUTO_SEED"] = "1"
        self.api = load_api_core()
        self.api._db_path_resolved = None
        self.api._seeded = False
        self.api.init_db()

    def tearDown(self):
        self.tmp.cleanup()
        os.environ.pop("DATABASE_PATH", None)
        os.environ.pop("DISABLE_AUTO_SEED", None)

    def request(self, path, payload):
        for cached in ("body_cache", "raw_body"):
            if hasattr(self.api._request_ctx, cached):
                delattr(self.api._request_ctx, cached)
        body = json.dumps(payload)
        ctx = self.api._request_ctx
        ctx.request_method = "POST"
        ctx.path_info = path
        ctx.query_string = ""
        ctx.http_authorization = "Bearer tok-employer"
        ctx.http_x_api_key = ""
        ctx.stdin_data = body
        ctx.content_type = "application/json"
        ctx.content_length = str(len(body.encode()))
        ctx.remote_addr = "127.0.0.1"
        with contextlib.redirect_stdout(io.StringIO()) as out:
            self.api.handle_request()
        return parse_cgi_output(out.getvalue())

    def test_job_hiring_pause_fails_before_any_stripe_call(self):
        db = self.api.get_db()
        try:
            db.execute("INSERT INTO users (id,email,name,password_hash) VALUES (1,'worker@example.com','Worker','x')")
            db.execute("INSERT INTO users (id,email,name,password_hash) VALUES (2,'employer@example.com','Employer','x')")
            db.execute("INSERT INTO employer_profiles (user_id,stripe_customer_id,payment_method_id) VALUES (2,'cus_test','pm_test')")
            db.execute("INSERT INTO sessions (user_id,token,expires_at) VALUES (2,'tok-employer',datetime('now','+1 day'))")
            db.execute("INSERT INTO jobs (id,employer_id,title,description,category,budget_type,budget_amount,status) VALUES (1,2,'Fixed QA','Test flows','testing','fixed',25,'open')")
            db.execute("INSERT INTO applications (id,job_id,worker_id,status) VALUES (1,1,1,'pending')")
            db.commit()
        finally:
            db.close()

        create = mock.Mock()
        self.api.PRODUCTION_MODE = True
        self.api.STRIPE_AVAILABLE = True
        self.api.STRIPE_SECRET_KEY = "configured-test-key"
        self.api.stripe = type("Stripe", (), {
            "PaymentIntent": type("PaymentIntent", (), {"create": create}),
            "error": type("Error", (), {"StripeError": Exception}),
        })

        status, result = self.request("/jobs/1/hire", {
            "applicant_id": 1,
            "milestones": [{"description": "Delivery", "amount": 25}],
        })
        self.assertEqual(status, 503, result)
        self.assertIn("temporarily paused", result["error"].lower())
        create.assert_not_called()
        db = self.api.get_db()
        try:
            self.assertEqual(db.execute("SELECT COUNT(*) FROM orders").fetchone()[0], 0)
        finally:
            db.close()


if __name__ == "__main__":
    unittest.main()
