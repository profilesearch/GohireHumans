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
        self.assertEqual(body, {"ok": True, "withdrawn_application_id": app["id"], "can_reapply": True,
                                "employer_notice_updated": True})
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
        self.assertNotIn("viewer_can_apply", detail)

    def drop_email_rows(self):
        """Simulate in-app-only notices (no email row links a notice to its application)."""
        with self.api.get_db() as db:
            if db.execute("SELECT 1 FROM sqlite_master WHERE name='agentmail_send_ledger'").fetchone():
                db.execute("DELETE FROM agentmail_send_ledger")
            db.execute("DELETE FROM transactional_email_outbox")
            db.commit()

    # ── review follow-ups ─────────────────────────────────────────────────
    def test_same_second_applications_without_email_each_neutralize_their_own_notice(self):
        # No email transport: notices are in-app only, so only the bound id can tell them apart.
        with self.api.get_db() as db:
            db.execute("UPDATE users SET name='Same Name' WHERE id IN (1,3)")
            db.commit()
        self.assertEqual(self.apply("tok-1")[0], 201)
        self.assertEqual(self.apply("tok-3")[0], 201)
        with self.api.get_db() as db:
            db.execute("UPDATE notifications SET created_at=(SELECT MIN(created_at) FROM notifications)")
            db.execute("UPDATE applications SET created_at=(SELECT MIN(created_at) FROM applications)")
            db.commit()
        self.drop_email_rows()
        status, body = self.request("DELETE", "/jobs/7/apply", "tok-1")
        self.assertEqual(status, 200, body)
        self.assertTrue(body["employer_notice_updated"])
        messages = [r[0] for r in self.api.get_db().execute(
            "SELECT message FROM notifications WHERE user_id=2 ORDER BY id").fetchall()]
        self.assertEqual(messages, ["An applicant applied, then withdrew their application.",
                                    "Same Name applied to your job."])

    def test_legacy_unbound_notice_is_matched_by_exact_name_or_left_untouched(self):
        # Simulate applications made before this release: no notice id in the audit row.
        self.assertEqual(self.apply("tok-1")[0], 201)
        self.assertEqual(self.apply("tok-3")[0], 201)
        with self.api.get_db() as db:
            db.execute("UPDATE audit_log SET details=NULL WHERE action='apply_job'")
            db.execute("UPDATE notifications SET created_at=(SELECT MIN(created_at) FROM notifications)")
            db.execute("UPDATE applications SET created_at=(SELECT MIN(created_at) FROM applications)")
            db.commit()
        self.drop_email_rows()
        status, body = self.request("DELETE", "/jobs/7/apply", "tok-1")
        self.assertEqual(status, 200, body)
        self.assertTrue(body["employer_notice_updated"])
        self.assertEqual(self.db_one("SELECT COUNT(*) FROM notifications WHERE message='Other Worker applied to your job.'")[0], 1)
        # Identical names and no binding: the notice is left alone and the API says so.
        with self.api.get_db() as db:
            db.execute("UPDATE users SET name='Other Worker' WHERE id=1")
            db.commit()
        self.assertEqual(self.apply("tok-1")[0], 201)
        with self.api.get_db() as db:
            db.execute("UPDATE audit_log SET details=NULL WHERE action='apply_job'")
            db.execute("UPDATE notifications SET created_at=(SELECT MIN(created_at) FROM notifications)")
            db.execute("UPDATE applications SET created_at=(SELECT MIN(created_at) FROM applications)")
            db.commit()
        self.drop_email_rows()
        before = [r[0] for r in self.api.get_db().execute("SELECT message FROM notifications ORDER BY id").fetchall()]
        status, body = self.request("DELETE", "/jobs/7/apply", "tok-1")
        self.assertEqual(status, 200, body)
        self.assertFalse(body["employer_notice_updated"])
        after = [r[0] for r in self.api.get_db().execute("SELECT message FROM notifications ORDER BY id").fetchall()]
        self.assertEqual(before, after)
        details = json.loads(self.db_one(
            "SELECT details FROM audit_log WHERE action='withdraw_application' ORDER BY id DESC LIMIT 1")[0])
        self.assertTrue(details["employer_notice_ambiguous"])

    def test_lone_unbound_notice_from_another_worker_is_never_rewritten(self):
        self.assertEqual(self.apply("tok-1")[0], 201)
        self.assertEqual(self.apply("tok-3")[0], 201)
        with self.api.get_db() as db:
            db.execute("UPDATE audit_log SET details=NULL WHERE action='apply_job'")
            db.execute("UPDATE notifications SET created_at=(SELECT MIN(created_at) FROM notifications)")
            db.execute("UPDATE applications SET created_at=(SELECT MIN(created_at) FROM applications)")
            db.commit()
        self.drop_email_rows()
        with self.api.get_db() as db:
            # The withdrawing worker's own notice is gone; only the other worker's remains.
            db.execute("DELETE FROM notifications WHERE message='Worker One applied to your job.'")
            db.commit()
        status, body = self.request("DELETE", "/jobs/7/apply", "tok-1")
        self.assertEqual(status, 200, body)
        self.assertFalse(body["employer_notice_updated"])
        self.assertEqual(self.db_one("SELECT COUNT(*) FROM notifications WHERE message='Other Worker applied to your job.'")[0], 1)
        self.assertEqual(self.db_one("SELECT COUNT(*) FROM notifications WHERE message LIKE 'An applicant applied, then withdrew%'")[0], 0)

    def test_viewer_can_apply_matches_server_rules_for_closed_jobs_and_ineligible_employers(self):
        self.assertEqual(self.request("DELETE", "/jobs/7", "tok-2")[0], 200)  # buyer cancels
        with self.api.get_db() as db:
            db.execute("UPDATE users SET is_admin=1 WHERE id=3")  # admins may still view canceled jobs
            db.commit()
        status, detail = self.request("GET", "/jobs/7", "tok-3")
        self.assertEqual(status, 200, detail)
        self.assertFalse(detail["viewer_can_apply"])
        self.assertEqual(self.request("POST", "/jobs/7/apply", "tok-3", {"cover_message": "hi"})[0], 409)
        with self.api.get_db() as db:
            db.execute("UPDATE jobs SET status='open' WHERE id=7")
            db.execute("UPDATE users SET is_suspended=1 WHERE id=2")
            db.commit()
        status, detail = self.request("GET", "/jobs/7", "tok-3")
        if status == 200:
            self.assertFalse(detail["viewer_can_apply"])
        self.assertEqual(self.request("POST", "/jobs/7/apply", "tok-3", {"cover_message": "hi"})[0], 409)

    def test_job_detail_reports_apply_and_withdraw_eligibility(self):
        status, detail = self.request("GET", "/jobs/7", "tok-1")
        self.assertEqual((detail["viewer_can_apply"], detail["viewer_can_withdraw"]), (True, False))
        status, detail = self.request("GET", "/jobs/7", "tok-2")
        self.assertFalse(detail["viewer_can_apply"], "the job owner can't apply to their own job")
        self.assertEqual(self.apply()[0], 201)
        status, detail = self.request("GET", "/jobs/7", "tok-1")
        self.assertEqual((detail["viewer_can_apply"], detail["viewer_can_withdraw"]), (False, True))
        self.assertEqual(self.request("DELETE", "/jobs/7/apply", "tok-1")[0], 200)
        self.assertEqual(self.apply()[0], 201)
        self.assertEqual(self.request("DELETE", "/jobs/7/apply", "tok-1")[0], 200)
        status, detail = self.request("GET", "/jobs/7", "tok-1")
        self.assertIsNone(detail["viewer_application"])
        self.assertEqual((detail["viewer_can_apply"], detail["viewer_can_withdraw"]), (False, False))

    def test_withdraw_still_works_and_is_reported_after_job_is_canceled(self):
        self.assertEqual(self.apply()[0], 201)
        self.assertEqual(self.request("DELETE", "/jobs/7", "tok-2")[0], 200)
        status, detail = self.request("GET", "/jobs/7", "tok-1")
        self.assertEqual(status, 200, detail)
        self.assertEqual(detail["status"], "canceled")
        self.assertTrue(detail["viewer_can_withdraw"])
        self.assertEqual(self.request("DELETE", "/jobs/7/apply", "tok-1")[0], 200)
        self.assertEqual(self.db_one("SELECT status FROM jobs WHERE id=7")[0], "canceled")
        # The job is closed and the worker no longer applied, so the scope is private again.
        self.assertEqual(self.request("GET", "/jobs/7", "tok-1")[0], 404)


if __name__ == "__main__":
    unittest.main()
