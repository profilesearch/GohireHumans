"""Daily applicant digest email: default off, once per owner per day, never retried.

Runs against a throwaway SQLite database with a fake AgentMail opener; nothing
touches the network or production.
"""
import contextlib
import io
import json
import os
import tempfile
import unittest
from datetime import datetime, timezone
from urllib.parse import parse_qs, urlsplit

from test_deep_audit_regressions import load_api_core, parse_cgi_output

NOW = datetime(2026, 10, 2, 15, 0, 0, tzinfo=timezone.utc)
SECRET = "s" * 40
ENV = {
    "APPLICANT_DIGEST_ENABLED": "true",
    "AGENTMAIL_API_KEY": "am_test_key_value",
    "AGENTMAIL_INBOX_ID": "gohirehumans.operations@agentmail.to",
    "APPLICANT_DIGEST_SECRET": SECRET,
    "APPLICANT_DIGEST_RECIPIENTS": "all",
    "APPLICANT_DIGEST_DAILY_CAP": "10",
    "APPLICANT_DIGEST_SEND_HOUR_UTC": "14",
}
ALL_KEYS = list(ENV) + ["APPLICANT_DIGEST_LOOKBACK_DAYS", "APPLICANT_DIGEST_EXPIRES_AT"]


class FakeResponse:
    def __init__(self, status, body):
        self.status, self._body = status, body

    def read(self, n):
        return self._body[:n]

    def close(self):
        pass


class FakeOpener:
    """Records every request; returns scripted outcomes (dict, Exception, or (status, bytes))."""

    def __init__(self, outcomes=None):
        self.requests, self.outcomes = [], list(outcomes or [])

    def open(self, request, timeout=None):
        self.requests.append(dict(url=request.full_url, method=request.get_method(),
                                  headers={k.lower(): v for k, v in request.header_items()},
                                  body=json.loads(request.data)))
        outcome = self.outcomes.pop(0) if self.outcomes else {"message_id": f"<m{len(self.requests)}@x>", "thread_id": f"t{len(self.requests)}"}
        if isinstance(outcome, Exception):
            raise outcome
        if isinstance(outcome, tuple):
            return FakeResponse(*outcome)
        return FakeResponse(200, json.dumps(outcome).encode())


class ApplicantDigestTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.saved_env = {k: os.environ.get(k) for k in ALL_KEYS + ["DATABASE_PATH", "DISABLE_AUTO_SEED", "GHH_JOB_HIRING_ENABLED"]}
        for k in ALL_KEYS:
            os.environ.pop(k, None)
        os.environ["DATABASE_PATH"] = os.path.join(self.tmp.name, "digest.db")
        os.environ["DISABLE_AUTO_SEED"] = "1"
        os.environ["GHH_JOB_HIRING_ENABLED"] = "1"
        for key in ("RESEND_API_KEY", "EMAIL_FROM", "STRIPE_SECRET_KEY"):
            os.environ.pop(key, None)
        self.api = load_api_core()
        self.api._db_path_resolved = None
        self.api._seeded = False
        self.api.init_db()
        with self.api.get_db() as db:
            users = [
                (1, "owner@example.com", "Owner One", 0),
                (2, "worker-ready@example.com", "Ready Worker", 0),
                (3, "worker-pending@example.com", "Pending Worker", 0),
                (4, "bot@ilands.app", "Agent Owner", 0),
                (5, "sarah.chen@example.com", "Sample Owner", 0),
                (6, "admin@example.com", "Admin", 1),
                (7, "owner2@example.com", "Owner Two", 0),
                (8, "worker-old@example.com", "Old Worker", 0),
                (9, "worker-banned@example.com", "Banned Worker", 0),
            ]
            for uid, email, name, admin in users:
                db.execute("INSERT INTO users(id,email,name,password_hash,is_admin) VALUES(?,?,?,'x',?)", [uid, email, name, admin])
                db.execute("INSERT INTO sessions(user_id,token,expires_at) VALUES(?,?,datetime('now','+1 day'))", [uid, f"tok-{uid}"])
            db.execute("INSERT INTO worker_profiles(user_id,payout_method) VALUES(2,'stripe_connect_active')")
            db.execute("INSERT INTO worker_profiles(user_id,payout_method) VALUES(3,'pending_setup')")
            for jid, owner, title, budget in ((10, 1, "Write five product descriptions", "fixed"),
                                              (11, 1, "Hourly research help", "hourly"),
                                              (12, 4, "Agent job", "fixed"),
                                              (13, 5, "Sample job", "fixed"),
                                              (14, 6, "Admin job", "fixed"),
                                              (15, 7, "Second owner job", "fixed")):
                db.execute("""INSERT INTO jobs(id,employer_id,title,description,category,budget_type,budget_amount,status,created_at)
                              VALUES(?,?,?,'d','writing',?,60,'open',datetime('now','-2 days'))""", [jid, owner, title, budget])
            db.commit()
        self.app_seq = 100

    def tearDown(self):
        self.tmp.cleanup()
        for k, v in self.saved_env.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v

    # ── helpers ────────────────────────────────────────────────────────────
    def enable(self, **overrides):
        for k, v in {**ENV, **overrides}.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v

    def add_app(self, job_id, worker_id, status="pending", age="-1 hours"):
        self.app_seq += 1
        with self.api.get_db() as db:
            db.execute("""INSERT INTO applications(id,job_id,worker_id,cover_message,status,created_at)
                          VALUES(?,?,?,'I can do this well.',?,datetime(?, ?))""",
                       [self.app_seq, job_id, worker_id, status, NOW.strftime('%Y-%m-%d %H:%M:%S'), age])
            db.commit()
        return self.app_seq

    def run_digest(self, now=NOW, opener=None):
        opener = opener or FakeOpener()
        with self.api.get_db() as db:
            result = self.api.run_applicant_digest_once(db, now=now, opener=opener)
        return result, opener

    def rows(self, sql, args=()):
        with self.api.get_db() as db:
            return [dict(r) for r in db.execute(sql, args).fetchall()]

    def request(self, method, path, token="", payload=None, query="", raw=None, content_type="application/json"):
        data = raw if raw is not None else json.dumps(payload if payload is not None else {}).encode()
        ctx = self.api._request_ctx
        for cached in ("body_cache", "raw_body"):
            if hasattr(ctx, cached):
                delattr(ctx, cached)
        ctx.request_method, ctx.path_info, ctx.query_string = method, path, query
        ctx.http_authorization = f"Bearer {token}" if token else ""
        ctx.http_x_api_key = ""
        ctx.stdin_data, ctx.stdin_data_raw = data.decode(), data
        ctx.content_type, ctx.content_length, ctx.remote_addr = content_type, str(len(data)), "127.0.0.1"
        with contextlib.redirect_stdout(io.StringIO()) as out:
            self.api.handle_request()
        return parse_cgi_output(out.getvalue())

    # ── default off / gates ────────────────────────────────────────────────
    def test_default_off_sends_nothing_and_writes_nothing(self):
        self.add_app(10, 2)
        result, opener = self.run_digest()
        self.assertEqual(result["status"], "disabled")
        self.assertEqual(opener.requests, [])
        self.assertEqual(self.rows("SELECT * FROM applicant_digest_sends"), [])

    def test_each_invalid_gate_keeps_it_off(self):
        self.add_app(10, 2)
        cases = {
            "APPLICANT_DIGEST_ENABLED": ("1", "disabled"),
            "AGENTMAIL_API_KEY": ("", "key_missing_or_invalid"),
            "AGENTMAIL_INBOX_ID": ("someone@agentmail.to", "sender_invalid"),
            "APPLICANT_DIGEST_SECRET": ("short", "secret_invalid"),
            "APPLICANT_DIGEST_RECIPIENTS": ("", "recipients_invalid"),
            "APPLICANT_DIGEST_DAILY_CAP": ("0", "daily_cap_invalid"),
            "APPLICANT_DIGEST_SEND_HOUR_UTC": ("24", "send_hour_invalid"),
            "APPLICANT_DIGEST_EXPIRES_AT": ("2026-10-02T14:59:59Z", "expired"),
        }
        for key, (value, reason) in cases.items():
            with self.subTest(key=key):
                self.enable(**{key: value})
                result, opener = self.run_digest()
                self.assertEqual(result["status"], reason)
                self.assertEqual(opener.requests, [])
        self.assertEqual(self.rows("SELECT * FROM applicant_digest_sends"), [])

    def test_nothing_before_the_send_hour(self):
        self.enable()
        self.add_app(10, 2)
        result, opener = self.run_digest(now=NOW.replace(hour=13))
        self.assertEqual(result["status"], "before_send_hour")
        self.assertEqual(opener.requests, [])

    # ── content ────────────────────────────────────────────────────────────
    def test_sends_one_email_with_ready_count_and_no_applicant_details(self):
        self.enable()
        self.add_app(10, 2)
        self.add_app(10, 3)
        result, opener = self.run_digest()
        self.assertEqual((result["attempted"], result["accepted"]), (1, 1))
        self.assertEqual(len(opener.requests), 1)
        req = opener.requests[0]
        self.assertTrue(req["url"].endswith("/inboxes/gohirehumans.operations%40agentmail.to/messages/send"))
        self.assertEqual(req["method"], "POST")
        self.assertTrue(req["headers"]["idempotency-key"].startswith("ghh-applicant-digest-"))
        body = req["body"]
        self.assertEqual(body["to"], ["owner@example.com"])
        self.assertEqual(body["reply_to"], ["gohirehumans.operations@agentmail.to"])
        self.assertEqual(body["subject"], "1 applicant ready to hire on GoHireHumans")
        text = body["text"]
        self.assertIn('"Write five product descriptions": 2 new applicants, 1 ready to hire now', text)
        self.assertIn("https://www.gohirehumans.com/#/jobs/10/applicants", text)
        for private in ("Ready Worker", "Pending Worker", "worker-ready@example.com", "I can do this well"):
            self.assertNotIn(private, json.dumps(body))
        self.assertIn("Stop these emails: https://www.gohirehumans.com/email-preferences/?t=1.", text)
        self.assertRegex(body["headers"]["List-Unsubscribe"],
                         r"^<https://gohirehumans-production\.up\.railway\.app/email-preferences/one-click\?t=1\.[0-9a-f]{32}>$")
        self.assertEqual(body["headers"]["List-Unsubscribe-Post"], "List-Unsubscribe=One-Click")
        sends = self.rows("SELECT * FROM applicant_digest_sends")
        self.assertEqual(len(sends), 1)
        self.assertEqual((sends[0]["state"], sends[0]["applications_count"], sends[0]["ready_count"]), ("accepted", 2, 1))
        self.assertEqual(sends[0]["message_id"], "<m1@x>")
        self.assertEqual(self.rows("SELECT action FROM audit_log WHERE action='applicant_digest_prepared'"),
                         [{"action": "applicant_digest_prepared"}])

    def test_hourly_job_and_paused_hiring_never_claim_ready_to_hire(self):
        self.enable()
        self.add_app(11, 2)
        _, opener = self.run_digest()
        text = opener.requests[0]["body"]["text"]
        self.assertIn('"Hourly research help": 1 new applicant', text)
        self.assertNotIn("ready to hire", text.lower())
        self.assertEqual(opener.requests[0]["body"]["subject"], "1 new applicant on your GoHireHumans job")

    def test_job_title_is_flattened_and_truncated(self):
        with self.api.get_db() as db:
            db.execute("UPDATE jobs SET title=? WHERE id=10", ["Line one\nBcc: x@evil.test\r\n" + "x" * 200])
            db.commit()
        self.enable()
        self.add_app(10, 2)
        _, opener = self.run_digest()
        text = opener.requests[0]["body"]["text"]
        title_line = [line for line in text.split("\n") if line.startswith('- "')][0]
        self.assertNotIn("\r", title_line)
        self.assertIn("Line one Bcc: x@evil.test", title_line)
        self.assertIn("…", title_line)

    # ── who gets it ────────────────────────────────────────────────────────
    def test_agents_samples_admins_inactive_and_opted_out_owners_get_nothing(self):
        self.enable()
        for job in (12, 13, 14, 15):
            self.add_app(job, 2)
        with self.api.get_db() as db:
            db.execute("UPDATE users SET is_active=0 WHERE id=7")
            db.commit()
        _, opener = self.run_digest()
        self.assertEqual(opener.requests, [])
        with self.api.get_db() as db:
            db.execute("UPDATE users SET is_active=1 WHERE id=7")
            self.api.applicant_digest.opt_out(db, 7)
            db.commit()
        _, opener = self.run_digest()
        self.assertEqual(opener.requests, [])

    def test_recipient_allowlist_limits_who_is_emailed(self):
        self.enable(APPLICANT_DIGEST_RECIPIENTS="owner2@example.com")
        self.add_app(10, 2)
        self.add_app(15, 2)
        _, opener = self.run_digest()
        self.assertEqual([r["body"]["to"] for r in opener.requests], [["owner2@example.com"]])

    def test_withdrawn_banned_old_and_already_seen_applications_are_not_counted(self):
        self.enable()
        seen = self.add_app(10, 3)
        with self.api.get_db() as db:
            db.execute("""INSERT INTO job_application_views(job_id,employer_id,first_viewed_at,last_viewed_at,last_seen_application_id)
                          VALUES(10,1,datetime('now'),datetime('now'),?)""", [seen])
            db.commit()
        self.add_app(10, 2, status="rejected")
        self.add_app(10, 8, age="-20 days")
        _, opener = self.run_digest()
        self.assertEqual(opener.requests, [])
        self.add_app(10, 9)
        with self.api.get_db() as db:
            db.execute("UPDATE users SET is_banned=1 WHERE id=9")
            db.commit()
        _, opener = self.run_digest()
        self.assertEqual(opener.requests, [])

    def test_closed_job_is_not_mentioned(self):
        self.enable()
        self.add_app(10, 2)
        with self.api.get_db() as db:
            db.execute("UPDATE jobs SET status='canceled' WHERE id=10")
            db.commit()
        _, opener = self.run_digest()
        self.assertEqual(opener.requests, [])

    # ── frequency / idempotency ────────────────────────────────────────────
    def test_at_most_one_email_per_owner_per_day_and_no_repeat_for_same_applications(self):
        self.enable()
        self.add_app(10, 2)
        self.run_digest()
        self.add_app(10, 3)
        _, opener = self.run_digest(now=NOW.replace(hour=20))
        self.assertEqual(opener.requests, [], "second email on the same day")
        _, opener = self.run_digest(now=NOW.replace(day=3))
        self.assertEqual(len(opener.requests), 1)
        self.assertIn('"Write five product descriptions": 1 new applicant', opener.requests[0]["body"]["text"])
        _, opener = self.run_digest(now=NOW.replace(day=4))
        self.assertEqual(opener.requests, [], "same applications emailed twice")

    def test_owners_with_only_seen_applications_do_not_starve_others(self):
        # Five lower-id owners whose recent applications were already viewed must
        # not use up the per-pass limit (5) ahead of an owner with news.
        with self.api.get_db() as db:
            for uid in range(20, 25):
                db.execute("INSERT INTO users(id,email,name,password_hash) VALUES(?,?,?,'x')",
                           [uid, f"seen{uid}@example.com", f"Seen {uid}"])
                db.execute("""INSERT INTO jobs(id,employer_id,title,description,category,budget_type,budget_amount,status,created_at)
                              VALUES(?,?,'Seen job','d','writing','fixed',60,'open',datetime('now','-2 days'))""", [uid + 100, uid])
            db.execute("INSERT INTO users(id,email,name,password_hash) VALUES(99,'late@example.com','Late Owner','x')")
            db.execute("""INSERT INTO jobs(id,employer_id,title,description,category,budget_type,budget_amount,status,created_at)
                          VALUES(199,99,'Late job','d','writing','fixed',60,'open',datetime('now','-2 days'))""")
            db.commit()
        for uid in range(20, 25):
            app_id = self.add_app(uid + 100, 2)
            with self.api.get_db() as db:
                db.execute("""INSERT INTO job_application_views(job_id,employer_id,first_viewed_at,last_viewed_at,last_seen_application_id)
                              VALUES(?,?,datetime('now'),datetime('now'),?)""", [uid + 100, uid, app_id])
                db.commit()
        self.add_app(199, 2)
        self.enable()
        _, opener = self.run_digest()
        self.assertEqual([r["body"]["to"] for r in opener.requests], [["late@example.com"]])

    def test_daily_cap_limits_total_sends(self):
        self.enable(APPLICANT_DIGEST_DAILY_CAP="1")
        self.add_app(10, 2)
        self.add_app(15, 2)
        result, opener = self.run_digest()
        self.assertEqual(len(opener.requests), 1)
        result, opener = self.run_digest(now=NOW.replace(hour=16))
        self.assertEqual(result["status"], "daily_cap_reached")
        self.assertEqual(opener.requests, [])

    def test_ambiguous_outcomes_are_never_retried_and_halt_sending(self):
        self.enable()
        self.add_app(10, 2)
        self.add_app(15, 2)
        for outcome in (TimeoutError("slow"), (500, b"oops"), (200, b"{}"), (200, b"not json")):
            with self.subTest(outcome=repr(outcome)):
                with self.api.get_db() as db:
                    db.execute("DELETE FROM applicant_digest_items")
                    db.execute("DELETE FROM applicant_digest_sends")
                    db.commit()
                result, opener = self.run_digest(opener=FakeOpener([outcome]))
                self.assertEqual(len(opener.requests), 1, "a second owner was attempted after an ambiguous result")
                self.assertEqual((result["unknown"], result["status"]), (1, "halted_unresolved_attempt"))
                self.assertEqual([r["state"] for r in self.rows("SELECT state FROM applicant_digest_sends")], ["unknown"])
                result, opener = self.run_digest(now=NOW.replace(hour=22))
                self.assertEqual(result["status"], "halted_unresolved_attempt")
                self.assertEqual(opener.requests, [])

    def test_gate_turned_off_between_intent_and_send_withholds(self):
        self.enable()
        self.add_app(10, 2)

        class TurnOff(FakeOpener):
            pass
        original = self.api.applicant_digest.config
        calls = {"n": 0}

        def flaky_config(now=None):
            calls["n"] += 1
            if calls["n"] >= 2:
                os.environ["APPLICANT_DIGEST_ENABLED"] = "false"
            return original(now)
        self.api.applicant_digest.config = flaky_config
        try:
            result, opener = self.run_digest(opener=TurnOff())
        finally:
            self.api.applicant_digest.config = original
        self.assertEqual(opener.requests, [])
        self.assertEqual(result["withheld"], 1)
        self.assertEqual([r["state"] for r in self.rows("SELECT state FROM applicant_digest_sends")], ["withheld"])

    def _after_intent(self, side_effect):
        """Run side_effect once, right after the intent commit (the post-intent gate recheck)."""
        original = self.api.applicant_digest.config
        calls = {"n": 0}

        def hooked(now=None):
            calls["n"] += 1
            if calls["n"] == 2:
                side_effect()
            return original(now)
        return original, hooked

    def test_opt_out_after_intent_but_before_send_withholds(self):
        self.enable()
        self.add_app(10, 2)

        def opt_out():
            other = self.api.get_db()
            try:
                self.api.applicant_digest.opt_out(other, 1)
                other.commit()
            finally:
                other.close()
        original, hooked = self._after_intent(opt_out)
        self.api.applicant_digest.config = hooked
        try:
            result, opener = self.run_digest()
        finally:
            self.api.applicant_digest.config = original
        self.assertEqual(opener.requests, [])
        self.assertEqual(result["withheld"], 1)
        self.assertEqual([r["state"] for r in self.rows("SELECT state FROM applicant_digest_sends")], ["withheld"])

    def test_admin_or_agent_promotion_after_intent_withholds(self):
        for column, value in (("is_admin", 1), ("is_ai_agent", 1)):
            with self.subTest(column=column):
                with self.api.get_db() as db:
                    db.execute("DELETE FROM applicant_digest_items")
                    db.execute("DELETE FROM applicant_digest_sends")
                    db.execute("UPDATE users SET is_admin=0, is_ai_agent=0 WHERE id=1")
                    db.commit()
                self.enable()
                if not self.rows("SELECT id FROM applications WHERE job_id=10"):
                    self.add_app(10, 2)

                def promote(column=column, value=value):
                    other = self.api.get_db()
                    try:
                        other.execute(f"UPDATE users SET {column}=? WHERE id=1", [value])
                        other.commit()
                    finally:
                        other.close()
                original, hooked = self._after_intent(promote)
                self.api.applicant_digest.config = hooked
                try:
                    result, opener = self.run_digest()
                finally:
                    self.api.applicant_digest.config = original
                self.assertEqual(opener.requests, [])
                self.assertEqual(result["withheld"], 1)

    def test_non_unique_constraint_error_is_not_mistaken_for_a_race(self):
        # Stale selection: owner 1 already has today's row (another worker), and
        # this worker's insert also violates a CHECK. That must surface, not be
        # silently treated as the benign UNIQUE race.
        self.enable()
        self.add_app(10, 2)
        original_digest = self.api.applicant_digest._digest
        original_select = self.api.applicant_digest.eligible_owners

        def stale_selection(*args, **kwargs):
            owners = original_select(*args, **kwargs)
            other = self.api.get_db()
            try:
                other.execute("""INSERT INTO applicant_digest_sends
                    (employer_id,digest_date,state,fingerprint,jobs_count,applications_count,ready_count,prepared_at)
                    VALUES(1,'2026-10-02','withheld',?,1,1,0,'2026-10-02 15:00:00')""", ["a" * 64])
                other.commit()
            finally:
                other.close()
            return owners
        self.api.applicant_digest._digest = lambda value: "short"  # violates CHECK(length=64)
        self.api.applicant_digest.eligible_owners = stale_selection
        try:
            with self.api.get_db() as db:
                with self.assertRaises(Exception):
                    self.api.applicant_digest.run_once(
                        db, now=NOW, opener=FakeOpener(), agent_sql=self.api.agent_account_sql("u"),
                        sample_emails=sorted(self.api.SEEDED_SAMPLE_EMAILS), hiring_enabled=True)
                self.assertFalse(db.in_transaction)
        finally:
            self.api.applicant_digest._digest = original_digest
            self.api.applicant_digest.eligible_owners = original_select
        self.assertEqual([r["state"] for r in self.rows("SELECT state FROM applicant_digest_sends")], ["withheld"])

    def test_lease_lost_after_intent_withholds_and_stops(self):
        self.enable()
        self.add_app(10, 2)
        self.add_app(15, 2)
        answers = iter([True, False])
        opener = FakeOpener()
        with self.api.get_db() as db:
            result = self.api.applicant_digest.run_once(
                db, now=NOW, renew_lease=lambda: next(answers, False), opener=opener,
                agent_sql=self.api.agent_account_sql("u"), sample_emails=sorted(self.api.SEEDED_SAMPLE_EMAILS),
                hiring_enabled=True)
        self.assertEqual(opener.requests, [])
        self.assertEqual((result["status"], result["withheld"]), ("lease_lost", 1))
        self.assertEqual([r["state"] for r in self.rows("SELECT state FROM applicant_digest_sends")], ["withheld"])

    def test_concurrent_worker_owning_todays_email_is_skipped_not_fatal(self):
        self.enable()
        self.add_app(10, 2)
        self.add_app(15, 2)
        original = self.api.applicant_digest.eligible_owners

        def stale_selection(*args, **kwargs):
            owners = original(*args, **kwargs)
            other = self.api.get_db()  # another worker commits owner 1's intent first
            try:
                other.execute("""INSERT INTO applicant_digest_sends
                    (employer_id,digest_date,state,fingerprint,jobs_count,applications_count,ready_count,prepared_at,
                     resolved_at,message_id,thread_id)
                    VALUES(1,'2026-10-02','accepted',?,1,1,0,'2026-10-02 15:00:00','2026-10-02 15:00:01','<x@y>','t')""",
                              ["a" * 64])
                other.commit()
            finally:
                other.close()
            return owners
        self.api.applicant_digest.eligible_owners = stale_selection
        try:
            result, opener = self.run_digest()
        finally:
            self.api.applicant_digest.eligible_owners = original
        self.assertNotEqual(result.get("status"), "error")
        self.assertEqual(result["skipped"], 1)
        self.assertEqual([r["body"]["to"] for r in opener.requests], [["owner2@example.com"]])

    def test_title_drops_bidi_zero_width_and_c1_controls(self):
        clean = self.api.applicant_digest._clean_title(
            "Normal\u0085NEXT\u202eSpoof\u200bZW\u2066iso\u2069\u2028end")
        for ch in ("\u0085", "\u202e", "\u200b", "\u2066", "\u2069", "\u2028"):
            self.assertNotIn(ch, clean)
        self.assertEqual(clean, "Normal NEXT Spoof ZW iso end")
        self.assertEqual(self.api.applicant_digest._clean_title("Caf\u00e9 \u65e5\u672c \U0001F600"), "Caf\u00e9 \u65e5\u672c \U0001F600")

    def test_no_sqlite_writer_is_held_during_provider_io(self):
        self.enable()
        self.add_app(10, 2)
        api = self.api

        class LockProbe(FakeOpener):
            def open(self, request, timeout=None):
                other = api.get_db()
                try:
                    other.execute("PRAGMA busy_timeout=50")
                    other.execute("BEGIN IMMEDIATE")
                    other.rollback()
                finally:
                    other.close()
                return super().open(request, timeout)
        result, opener = self.run_digest(opener=LockProbe())
        self.assertEqual(result["accepted"], 1)

    def test_maintenance_tick_includes_digest_and_survives_a_digest_crash(self):
        original = self.api.applicant_digest.run_once

        def boom(*a, **k):
            raise RuntimeError("bad")
        self.api.applicant_digest.run_once = boom
        try:
            with self.api.get_db() as db:
                self.assertEqual(self.api.run_applicant_digest_once(db, now=NOW), {"status": "error"})
        finally:
            self.api.applicant_digest.run_once = original

    # ── unsubscribe ────────────────────────────────────────────────────────
    def token_for(self, uid):
        self.enable()
        return self.api.applicant_digest.unsubscribe_token(uid)

    def test_one_click_and_page_unsubscribe_set_opt_out(self):
        token = self.token_for(1)
        status, body = self.request("POST", "/email-preferences/one-click", query=f"t={token}",
                                    raw=b"List-Unsubscribe=One-Click", content_type="application/x-www-form-urlencoded")
        self.assertEqual((status, body), (200, {"unsubscribed": True}))
        status, body = self.request("POST", "/api/v1/email-preferences/unsubscribe", payload={"token": token})
        self.assertEqual(status, 200)
        self.assertEqual(self.rows("SELECT applicant_digest_opt_out FROM email_preferences WHERE user_id=1"),
                         [{"applicant_digest_opt_out": 1}])
        self.add_app(10, 2)
        _, opener = self.run_digest()
        self.assertEqual(opener.requests, [])

    def test_forged_or_malformed_tokens_are_rejected_without_writes(self):
        token = self.token_for(1)
        uid, mac = token.split(".")
        for bad in ("", "1", f"2.{mac}", f"{uid}.{'0' * 32}", f"{uid}.{mac.upper()}", f"0.{mac}",
                    f"{uid}.{mac}x", "1.abc", "9" * 30 + "." + mac):
            with self.subTest(bad=bad):
                status, _ = self.request("POST", "/email-preferences/unsubscribe", payload={"token": bad})
                self.assertEqual(status, 400)
        self.assertEqual(self.rows("SELECT * FROM email_preferences"), [])

    def test_tokens_stop_working_without_a_secret(self):
        token = self.token_for(1)
        os.environ.pop("APPLICANT_DIGEST_SECRET")
        status, _ = self.request("POST", "/email-preferences/unsubscribe", payload={"token": token})
        self.assertEqual(status, 400)

    def test_get_never_unsubscribes(self):
        token = self.token_for(1)
        status, _ = self.request("GET", "/email-preferences/one-click", query=f"t={token}")
        self.assertNotEqual(status, 200)
        self.assertEqual(self.rows("SELECT * FROM email_preferences"), [])

    def test_unsubscribe_link_in_email_round_trips(self):
        self.enable()
        self.add_app(10, 2)
        _, opener = self.run_digest()
        header = opener.requests[0]["body"]["headers"]["List-Unsubscribe"].strip("<>")
        token = parse_qs(urlsplit(header).query)["t"][0]
        status, _ = self.request("POST", "/email-preferences/one-click", query=f"t={token}")
        self.assertEqual(status, 200)

    # ── schema, health, erasure ────────────────────────────────────────────
    def test_weakened_same_name_table_is_rejected(self):
        with self.api.get_db() as db:
            db.execute("DROP TABLE applicant_digest_items")
            db.execute("DROP TABLE applicant_digest_sends")
            db.execute("CREATE TABLE applicant_digest_sends (id INTEGER PRIMARY KEY, employer_id INTEGER)")
            db.commit()
            with self.assertRaises(RuntimeError):
                self.api.applicant_digest.validate_schema(db)

    def test_admin_health_reports_aggregates_only(self):
        self.enable()
        self.add_app(10, 2)
        self.run_digest()
        status, body = self.request("GET", "/admin/notification-health", token="tok-6")
        self.assertEqual(status, 200)
        digest = body["applicant_digest"]
        self.assertEqual(digest["states"]["accepted"], 1)
        self.assertEqual(digest["recipients_scope"], "all")
        self.assertNotIn("owner@example.com", json.dumps(body))
        status, _ = self.request("GET", "/admin/notification-health", token="tok-1")
        self.assertEqual(status, 403)

    def test_account_erasure_removes_digest_rows_and_preferences(self):
        self.enable()
        self.add_app(10, 2)
        self.run_digest()
        with self.api.get_db() as db:
            self.api.applicant_digest.opt_out(db, 1)
            db.commit()
        erase = getattr(self.api, "_erasure_plan")
        with self.api.get_db() as db:
            target = db.execute("SELECT * FROM users WHERE id=1").fetchone()
            plan = erase(db, target)
        deleted = plan[0] if isinstance(plan, tuple) else plan
        flat = json.dumps(deleted, default=str)
        for table in ("applicant_digest_items", "applicant_digest_sends", "email_preferences"):
            self.assertIn(table, flat)


if __name__ == "__main__":
    unittest.main()
