"""Job applications: blank cover messages are rejected and workers can withdraw.

Covers POST/DELETE /jobs/{id}/apply and viewer_application on GET /jobs/{id}.
Runs against a throwaway SQLite database; nothing touches production.
"""
import contextlib
import hashlib
import io
import json
import os
import tempfile
import unittest

from test_deep_audit_regressions import load_api_core, parse_cgi_output


class JobApplicationWithdrawTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        os.environ["DATABASE_PATH"] = os.path.join(self.tmp.name, "apply.db")
        os.environ["DISABLE_AUTO_SEED"] = "1"
        for key in ("RESEND_API_KEY", "EMAIL_FROM", "STRIPE_SECRET_KEY"):
            os.environ.pop(key, None)
        self.api = load_api_core()
        self.api._db_path_resolved = None
        self.api._seeded = False
        self.api.init_db()
        with self.api.get_db() as db:
            for uid, email, name in ((1, "worker@example.com", "Worker One"),
                                     (2, "owner@example.com", "Owner One"),
                                     (3, "other@example.com", "Other Worker")):
                db.execute("INSERT INTO users(id,email,name,password_hash) VALUES(?,?,?,'x')", [uid, email, name])
                db.execute("INSERT INTO sessions(user_id,token,expires_at) VALUES(?,?,datetime('now','+1 day'))",
                           [uid, f"tok-{uid}"])
            db.execute("INSERT INTO worker_profiles(user_id) VALUES(1)")
            db.execute("INSERT INTO worker_profiles(user_id) VALUES(3)")
            db.execute("INSERT INTO employer_profiles(user_id) VALUES(2)")
            db.execute(
                """INSERT INTO jobs(id,employer_id,title,description,category,budget_type,budget_amount,status,created_at)
                   VALUES(7,2,'Write five product descriptions','Five descriptions','copywriting','fixed',60,'open',datetime('now'))""")
            self.raw_key = "ghh_" + "r" * 40
            db.execute("INSERT INTO api_keys(user_id,key_hash,key_prefix,name,scopes) VALUES(1,?,?,?,?)",
                       [hashlib.sha256(self.raw_key.encode()).hexdigest(), self.raw_key[:12], "ro", '["read"]'])
            db.commit()

    def tearDown(self):
        self.tmp.cleanup()
        for key in ("DATABASE_PATH", "DISABLE_AUTO_SEED"):
            os.environ.pop(key, None)

    def request(self, method, path, token="", payload=None, api_key=""):
        raw = json.dumps(payload if payload is not None else {}).encode("utf-8")
        ctx = self.api._request_ctx
        for cached in ("body_cache", "raw_body"):
            if hasattr(ctx, cached):
                delattr(ctx, cached)
        ctx.request_method = method
        ctx.path_info = path
        ctx.query_string = ""
        ctx.http_authorization = f"Bearer {token}" if token else ""
        ctx.http_x_api_key = api_key
        ctx.stdin_data = raw.decode("utf-8")
        ctx.stdin_data_raw = raw
        ctx.content_type = "application/json"
        ctx.content_length = str(len(raw))
        ctx.remote_addr = "127.0.0.1"
        with contextlib.redirect_stdout(io.StringIO()) as out:
            self.api.handle_request()
        return parse_cgi_output(out.getvalue())

    def apply(self, token="tok-1", cover="I can write these and deliver them tomorrow."):
        return self.request("POST", "/jobs/7/apply", token, {"cover_message": cover})

    def db_one(self, sql, args=()):
        with self.api.get_db() as db:
            return db.execute(sql, args).fetchone()

    # ── blank applications ────────────────────────────────────────────────
    def test_blank_missing_or_non_string_cover_message_is_rejected_and_nothing_is_stored(self):
        for payload in ({}, {"cover_message": ""}, {"cover_message": "   \n\t "}, {"cover_message": None}):
            status, body = self.request("POST", "/jobs/7/apply", "tok-1", payload)
            self.assertEqual(status, 400, (payload, body))
            self.assertIn("cover_message is required", body["error"])
        status, body = self.request("POST", "/jobs/7/apply", "tok-1", {"cover_message": ["x"]})
        self.assertEqual(status, 400, body)
        status, body = self.request("POST", "/jobs/7/apply", "tok-1", {"cover_message": "Real message", "portfolio_url": 5})
        self.assertEqual(status, 400, body)
        self.assertEqual(self.db_one("SELECT COUNT(*) FROM applications")[0], 0)
        self.assertEqual(self.db_one("SELECT COUNT(*) FROM notifications")[0], 0)
        self.assertEqual(self.db_one("SELECT status FROM jobs WHERE id=7")[0], "open")

    def test_cover_message_is_trimmed_and_null_portfolio_is_accepted(self):
        status, body = self.request("POST", "/jobs/7/apply", "tok-1",
                                    {"cover_message": "  I can do this today.  ", "portfolio_url": None})
        self.assertEqual(status, 201, body)
        self.assertEqual(body["cover_message"], "I can do this today.")
        self.assertEqual(body["portfolio_url"], "")
        self.assertEqual(self.db_one("SELECT status FROM jobs WHERE id=7")[0], "reviewing")

    # ── withdraw ──────────────────────────────────────────────────────────
    def test_worker_withdraws_own_application_and_employer_notice_is_neutralized(self):
        status, app = self.apply()
        self.assertEqual(status, 201, app)
        notice = self.db_one("SELECT id, message FROM notifications WHERE user_id=2 AND type='new_application'")
        self.assertEqual(notice["message"], "Worker One applied to your job.")

        status, detail = self.request("GET", "/jobs/7", "tok-1")
        self.assertEqual(status, 200, detail)
        self.assertEqual(detail["viewer_application"]["id"], app["id"])
        self.assertEqual(detail["viewer_application"]["status"], "pending")

        status, body = self.request("DELETE", "/jobs/7/apply", "tok-1")
        self.assertEqual(status, 200, body)
        self.assertEqual(body, {"ok": True, "withdrawn_application_id": app["id"], "can_reapply": True})
        self.assertIsNone(self.db_one("SELECT id FROM applications WHERE id=?", [app["id"]]))
        self.assertEqual(self.db_one("SELECT status FROM jobs WHERE id=7")[0], "open")
        self.assertEqual(self.db_one("SELECT message FROM notifications WHERE id=?", [notice["id"]])[0],
                         "An applicant applied, then withdrew their application.")
        status, detail = self.request("GET", "/jobs/7", "tok-1")
        self.assertIsNone(detail["viewer_application"])
        status, apps = self.request("GET", "/jobs/7/applications", "tok-2")
        self.assertEqual(status, 200, apps)
        listed = apps if isinstance(apps, list) else apps.get("applications", [])
        self.assertEqual(listed, [])

    def test_unsent_application_email_is_suppressed_on_withdraw(self):
        self.api.RESEND_API_KEY = "configured-for-test"
        status, app = self.apply()
        self.assertEqual(status, 201, app)
        row = self.db_one("""SELECT id, state FROM transactional_email_outbox
                             WHERE notification_type='new_application' AND dedupe_context=?""",
                          [f"application:{app['id']}"])
        self.assertIsNotNone(row, "apply should queue an employer email")
        self.assertEqual(row["state"], "pending")
        status, body = self.request("DELETE", "/jobs/7/apply", "tok-1")
        self.assertEqual(status, 200, body)
        row = self.db_one("SELECT state, delivery_status, message, email_to FROM transactional_email_outbox WHERE id=?",
                          [row["id"]])
        self.assertEqual((row["state"], row["delivery_status"]), ("failed", "suppressed"))
        self.assertEqual((row["message"], row["email_to"]), ("", ""))

    def test_job_stays_reviewing_when_other_applicants_remain(self):
        self.assertEqual(self.apply("tok-1")[0], 201)
        self.assertEqual(self.apply("tok-3")[0], 201)
        self.assertEqual(self.request("DELETE", "/jobs/7/apply", "tok-1")[0], 200)
        self.assertEqual(self.db_one("SELECT status FROM jobs WHERE id=7")[0], "reviewing")
        self.assertEqual(self.db_one("SELECT COUNT(*) FROM applications WHERE job_id=7")[0], 1)

    def test_withdraw_only_touches_the_callers_own_application(self):
        self.assertEqual(self.apply("tok-3")[0], 201)
        status, body = self.request("DELETE", "/jobs/7/apply", "tok-1")
        self.assertEqual(status, 404, body)
        self.assertEqual(self.request("DELETE", "/jobs/7/apply", "")[0], 401)
        self.assertEqual(self.request("DELETE", "/jobs/999/apply", "tok-1")[0], 404)
        self.assertEqual(self.db_one("SELECT COUNT(*) FROM applications WHERE job_id=7")[0], 1)

    def test_one_reapply_then_withdrawal_is_final(self):
        self.assertEqual(self.apply()[0], 201)
        self.assertEqual(self.request("DELETE", "/jobs/7/apply", "tok-1")[1]["can_reapply"], True)
        self.assertEqual(self.apply()[0], 201)
        status, body = self.request("DELETE", "/jobs/7/apply", "tok-1")
        self.assertEqual(status, 200, body)
        self.assertFalse(body["can_reapply"])
        status, body = self.apply()
        self.assertEqual(status, 409, body)
        self.assertIn("withdrawn from this job twice", body["error"])

    def test_cannot_withdraw_after_hire_or_once_application_is_decided(self):
        status, app = self.apply()
        self.assertEqual(status, 201, app)
        with self.api.get_db() as db:
            db.execute("""INSERT INTO orders(type,job_id,worker_id,employer_id,status,total_amount)
                          VALUES('job_hire',7,1,2,'in_progress',60)""")
            db.commit()
        status, body = self.request("DELETE", "/jobs/7/apply", "tok-1")
        self.assertEqual(status, 409, body)
        self.assertIsNotNone(self.db_one("SELECT id FROM applications WHERE id=?", [app["id"]]))
        with self.api.get_db() as db:
            db.execute("DELETE FROM orders")
            db.execute("UPDATE applications SET status='rejected' WHERE id=?", [app["id"]])
            db.commit()
        self.assertEqual(self.request("DELETE", "/jobs/7/apply", "tok-1")[0], 409)

    def test_hire_snapshot_fails_closed_if_application_is_withdrawn_mid_hire(self):
        status, app = self.apply()
        self.assertEqual(status, 201, app)
        with self.api.get_db() as db:
            before = self.api._job_hire_new_operation_snapshot(db, 7, app["id"])
        self.assertIsNotNone(before)
        self.assertEqual(self.request("DELETE", "/jobs/7/apply", "tok-1")[0], 200)
        with self.api.get_db() as db:
            after = self.api._job_hire_new_operation_snapshot(db, 7, app["id"])
        self.assertIsNone(after)
        self.assertFalse(self.api._job_hire_snapshot_unchanged(before, after))

    def test_read_only_api_key_cannot_withdraw_but_sees_viewer_application(self):
        status, app = self.apply()
        self.assertEqual(status, 201, app)
        status, body = self.request("DELETE", "/jobs/7/apply", api_key=self.raw_key)
        self.assertEqual(status, 403, body)
        self.assertIsNotNone(self.db_one("SELECT id FROM applications WHERE id=?", [app["id"]]))
        status, detail = self.request("GET", "/jobs/7", api_key=self.raw_key)
        self.assertEqual(status, 200, detail)
        self.assertEqual(detail["viewer_application"]["id"], app["id"])

    def test_anonymous_job_detail_has_no_viewer_application_field(self):
        status, detail = self.request("GET", "/jobs/7")
        self.assertEqual(status, 200, detail)
        self.assertNotIn("viewer_application", detail)


if __name__ == "__main__":
    unittest.main()
