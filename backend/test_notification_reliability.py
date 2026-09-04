import contextlib
import base64
import hashlib
import hmac
import io
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
import tempfile
import threading
import time
from types import SimpleNamespace
import unittest
from datetime import datetime, timedelta, timezone
from unittest import mock

from test_deep_audit_regressions import load_api_core, parse_cgi_output
from test_diagnostics import load_server


class NotificationReliabilityTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        os.environ["DATABASE_PATH"] = os.path.join(self.tmp.name, "notifications.db")
        os.environ["DISABLE_AUTO_SEED"] = "1"
        for key in ("RESEND_API_KEY", "EMAIL_FROM", "RESEND_WEBHOOK_SECRET"):
            os.environ.pop(key, None)
        self.api = load_api_core()
        self.api._db_path_resolved = None
        self.api._seeded = False
        self.api.init_db()
        self._seed()

    def tearDown(self):
        self.tmp.cleanup()
        for key in (
            "DATABASE_PATH", "DISABLE_AUTO_SEED", "RESEND_API_KEY",
            "EMAIL_FROM", "RESEND_WEBHOOK_SECRET",
        ):
            os.environ.pop(key, None)

    def _seed(self):
        with self.api.get_db() as db:
            db.execute(
                "INSERT INTO users(id,email,name,password_hash) VALUES(1,'worker@example.com','Worker One','x')"
            )
            db.execute(
                "INSERT INTO users(id,email,name,password_hash) VALUES(2,'owner@example.com','Owner One','x')"
            )
            db.execute(
                "INSERT INTO users(id,email,name,password_hash,is_admin) VALUES(3,'admin@example.com','Admin','x',1)"
            )
            for user_id, token in ((1, "worker"), (2, "owner"), (3, "admin")):
                db.execute(
                    "INSERT INTO sessions(user_id,token,expires_at) VALUES(?,?,datetime('now','+1 day'))",
                    [user_id, token],
                )
            db.execute("INSERT INTO worker_profiles(user_id) VALUES(1)")
            db.execute("INSERT INTO employer_profiles(user_id) VALUES(2)")
            db.execute(
                """INSERT INTO jobs
                   (id,employer_id,title,description,category,budget_type,budget_amount,status,created_at)
                   VALUES(1,2,'Review a document','Review one document','testing','fixed',25,'reviewing',datetime('now'))"""
            )
            db.commit()

    def request(self, method, path, token="", payload=None, headers=None, raw_body=None, api_key=""):
        raw = raw_body if raw_body is not None else json.dumps(
            payload or {}, separators=(",", ":")
        ).encode("utf-8")
        ctx = self.api._request_ctx
        for cached in ("body_cache", "raw_body"):
            if hasattr(ctx, cached):
                delattr(ctx, cached)
        ctx.request_method = method
        ctx.path_info = path
        ctx.query_string = ""
        ctx.http_authorization = f"Bearer {token}" if token else ""
        ctx.http_x_api_key = api_key
        ctx.http_svix_id = (headers or {}).get("svix-id", "")
        ctx.http_svix_timestamp = (headers or {}).get("svix-timestamp", "")
        ctx.http_svix_signature = (headers or {}).get("svix-signature", "")
        ctx.stdin_data = raw.decode("utf-8")
        ctx.stdin_data_raw = raw
        ctx.content_type = "application/json"
        ctx.content_length = str(len(raw))
        ctx.remote_addr = "127.0.0.1"
        with contextlib.redirect_stdout(io.StringIO()) as out:
            self.api.handle_request()
        return parse_cgi_output(out.getvalue())

    def test_application_route_commits_outbox_without_synchronous_drain(self):
        self.api.RESEND_API_KEY = "configured-for-test"
        with mock.patch.object(
            self.api, "flush_transactional_notification_emails",
            side_effect=AssertionError("request route must not drain email outbox"),
        ):
            status, payload = self.request(
                "POST", "/jobs/1/apply", token="worker",
                payload={"cover_message": "I can complete this work.", "portfolio_url": ""},
            )
        self.assertEqual(status, 201, payload)
        with self.api.get_db() as db:
            outbox = db.execute(
                """SELECT notification_type,state,attempts
                   FROM transactional_email_outbox"""
            ).fetchone()
            self.assertEqual(tuple(outbox), ("new_application", "pending", 0))

    def test_flush_skips_legacy_mail_and_does_not_head_of_line_block(self):
        with self.api.get_db() as db:
            for index, email in enumerate(
                ("legacy@example.com", "fail@example.com", "ok@example.com"), start=1
            ):
                db.execute(
                    """INSERT INTO transactional_email_outbox
                       (user_id,email_to,notification_type,title,message,link,
                        dedupe_context,dedupe_key,state,next_attempt_at,expires_at)
                       VALUES(2,?,'new_application',?,'Message','/jobs/1/applications',
                              ?,?,'pending','2026-08-24 11:00:00',?)""",
                    [
                        email,
                        f"Message {index}",
                        f"context-{index}",
                        f"key-{index}",
                        None if index == 1 else "2099-01-01 00:00:00",
                    ],
                )
            db.commit()

            attempted = []

            def send(db_arg, user_id, notif_type, title, *args, **kwargs):
                attempted.append(title)
                if title == "Message 3":
                    return "accepted", "provider-message-3"
                return "failed", None

            with mock.patch.object(
                self.api, "send_transactional_notification_email", side_effect=send
            ):
                result = self.api.flush_transactional_notification_emails(
                    db, now=datetime(2026, 8, 24, 12, 0, tzinfo=timezone.utc)
                )

            self.assertEqual(attempted, ["Message 2", "Message 3"])
            self.assertEqual(result["sent"], 1)
            self.assertEqual(result["deferred"], 1)
            self.assertEqual(result["failed"], 0)
            self.assertEqual(result["claimed_recovered"], 0)
            self.assertEqual(result["stale_skipped"], 1)
            rows = db.execute(
                "SELECT title,state,attempts,next_attempt_at FROM transactional_email_outbox ORDER BY id"
            ).fetchall()
            self.assertEqual(tuple(rows[0])[:3], ("", "failed", 0))
            self.assertEqual(tuple(rows[1])[:3], ("Message 2", "pending", 1))
            self.assertGreater(rows[1]["next_attempt_at"], "2026-08-24 12:00:00")
            self.assertEqual(tuple(rows[2])[:3], ("", "sent", 1))

    def test_transport_timeout_stays_ambiguous_then_expires_to_manual_review(self):
        self.api.RESEND_API_KEY = "configured-for-test"
        with self.api.get_db() as db:
            db.execute(
                """INSERT INTO transactional_email_outbox
                   (user_id,email_to,notification_type,title,message,link,
                    dedupe_context,dedupe_key,state,next_attempt_at,expires_at)
                   VALUES(2,'','new_application','Ambiguous send','Message',
                          '/jobs/1/applications','ambiguous','ambiguous-key','pending',
                          datetime('now'),datetime('now','+24 hours'))"""
            )
            db.commit()
            row = db.execute(
                "SELECT created_at,expires_at FROM transactional_email_outbox"
            ).fetchone()
            first_now = datetime.fromisoformat(row["created_at"]).replace(
                tzinfo=timezone.utc
            ) + timedelta(seconds=1)
            expires = datetime.fromisoformat(row["expires_at"]).replace(
                tzinfo=timezone.utc
            )
            with mock.patch.object(
                self.api.urllib.request, "urlopen", side_effect=TimeoutError("response lost")
            ) as provider:
                first = self.api.flush_transactional_notification_emails(
                    db, now=first_now, limit=1,
                )
            self.assertEqual(first["deferred"], 1)
            provider.assert_called_once()
            pending = db.execute(
                """SELECT state,attempts,delivery_status
                   FROM transactional_email_outbox"""
            ).fetchone()
            self.assertEqual(tuple(pending), ("pending", 1, "unknown"))

            with mock.patch.object(
                self.api.urllib.request, "urlopen",
                side_effect=AssertionError("expired ambiguity must not retry"),
            ) as provider:
                expired = self.api.flush_transactional_notification_emails(
                    db, now=expires + timedelta(seconds=1), limit=1,
                )
            self.assertEqual(expired["stale_skipped"], 1)
            provider.assert_not_called()
            terminal = db.execute(
                """SELECT state,delivery_status,email_to,title,message,link
                   FROM transactional_email_outbox"""
            ).fetchone()
            self.assertEqual(
                tuple(terminal),
                ("failed", "manual_review", "", "", "", ""),
            )
            health = self.api.notification_delivery_health(db)
            self.assertEqual(health["outbox"]["manual_review"], 1)
            self.assertEqual(
                health["outbox"]["by_type"]["new_application"]["manual_review"], 1
            )

    def test_reclaimed_sender_cannot_finalize_after_claim_token_loss(self):
        with self.api.get_db() as db:
            db.execute(
                """INSERT INTO transactional_email_outbox
                   (user_id,email_to,notification_type,title,message,link,
                    dedupe_context,dedupe_key,state,next_attempt_at,expires_at)
                   VALUES(2,'owner@example.com','new_application','Claim race','Message',
                          '/jobs/1/applications','claim-race','claim-race-key','pending',
                          '2026-08-24 11:00:00','2026-08-26 12:00:00')"""
            )
            db.commit()

        first_started = threading.Event()
        second_started = threading.Event()
        release_first = threading.Event()
        release_second = threading.Event()
        call_lock = threading.Lock()
        call_number = 0

        def delayed_send(*_args, **_kwargs):
            nonlocal call_number
            with call_lock:
                call_number += 1
                mine = call_number
            if mine == 1:
                first_started.set()
                self.assertTrue(release_first.wait(10))
                return "provider-from-stale-sender"
            second_started.set()
            self.assertTrue(release_second.wait(10))
            return "provider-from-current-sender"

        results = {}

        def run_flush(name, now):
            with self.api.get_db() as worker_db:
                results[name] = self.api.flush_transactional_notification_emails(
                    worker_db, now=now, limit=1,
                )

        with mock.patch.object(
            self.api, "send_transactional_notification_email", side_effect=delayed_send,
        ):
            first = threading.Thread(
                target=run_flush,
                args=("first", datetime(2026, 8, 24, 12, 0, tzinfo=timezone.utc)),
            )
            first.start()
            self.assertTrue(first_started.wait(10))
            with self.api.get_db() as db:
                db.execute(
                    "UPDATE transactional_email_outbox SET claimed_at='2000-01-01 00:00:00'"
                )
                db.commit()
            second = threading.Thread(
                target=run_flush,
                args=("second", datetime(2026, 8, 24, 12, 20, tzinfo=timezone.utc)),
            )
            second.start()
            self.assertTrue(second_started.wait(10))
            release_first.set()
            first.join(10)
            self.assertFalse(first.is_alive())
            release_second.set()
            second.join(10)
            self.assertFalse(second.is_alive())

        self.assertEqual(results["first"]["sent"], 0)
        self.assertEqual(results["first"]["claim_lost"], 1)
        self.assertEqual(results["second"]["sent"], 1)
        with self.api.get_db() as db:
            row = db.execute(
                "SELECT state,provider_email_id FROM transactional_email_outbox"
            ).fetchone()
            self.assertEqual(tuple(row), ("sent", "provider-from-current-sender"))

    def test_application_reminder_is_exact_once_and_view_stops_escalation(self):
        with self.api.get_db() as db:
            db.execute(
                "UPDATE notification_system_state SET application_reminders_enabled_at=? WHERE id=1",
                ["2026-08-20 00:00:00"],
            )
            db.execute(
                """INSERT INTO applications
                   (id,job_id,worker_id,cover_message,status,created_at)
                   VALUES(1,1,1,'Ready to help','pending','2026-08-24 10:00:00')"""
            )
            db.commit()

            created = self.api.generate_application_reminders(
                db, now=datetime(2026, 8, 25, 12, 0, tzinfo=timezone.utc)
            )
            duplicate = self.api.generate_application_reminders(
                db, now=datetime(2026, 8, 25, 12, 1, tzinfo=timezone.utc)
            )
            self.assertEqual(created, 1)
            self.assertEqual(duplicate, 0)
            reminder = db.execute(
                "SELECT reminder_kind,application_count,notification_id FROM job_application_reminders"
            ).fetchone()
            self.assertEqual(tuple(reminder)[:2], ("24h", 1))
            notice = db.execute(
                "SELECT type,title,message,link FROM notifications WHERE id=?",
                [reminder["notification_id"]],
            ).fetchone()
            self.assertEqual(notice["type"], "application_reminder_24h")
            self.assertEqual(
                notice["title"], "Applications are waiting for Review a document"
            )
            self.assertEqual(
                notice["message"],
                "People are waiting for your response. Review the applications and choose whether to hire, decline, or keep the job open.",
            )
            self.assertEqual(notice["link"], "/jobs/1/applications")
            outbox = db.execute(
                "SELECT state,expires_at FROM transactional_email_outbox WHERE notification_id=?",
                [reminder["notification_id"]],
            ).fetchone()
            self.assertEqual(outbox["state"], "pending")
            self.assertIsNotNone(outbox["expires_at"])

        status, applications = self.request("GET", "/jobs/1/applications", token="owner")
        self.assertEqual(status, 200, applications)
        self.assertEqual(len(applications), 1)

        with self.api.get_db() as db:
            viewed = db.execute(
                "SELECT first_viewed_at,last_viewed_at FROM job_application_views WHERE job_id=1 AND employer_id=2"
            ).fetchone()
            self.assertIsNotNone(viewed)
            created_72h = self.api.generate_application_reminders(
                db, now=datetime(2026, 8, 27, 12, 0, tzinfo=timezone.utc)
            )
            self.assertEqual(created_72h, 0)
            self.assertEqual(
                db.execute("SELECT COUNT(*) FROM job_application_reminders").fetchone()[0], 1
            )

    def test_view_after_queue_suppresses_obsolete_reminder_before_email_send(self):
        self.api.RESEND_API_KEY = "configured-for-test"
        with self.api.get_db() as db:
            db.execute(
                "UPDATE notification_system_state SET application_reminders_enabled_at='2026-08-20 00:00:00' WHERE id=1"
            )
            db.execute(
                """INSERT INTO applications
                   (id,job_id,worker_id,cover_message,status,created_at)
                   VALUES(11,1,1,'Queued then viewed','pending','2026-08-24 10:00:00')"""
            )
            db.commit()
            self.assertEqual(
                self.api.generate_application_reminders(
                    db, now=datetime(2026, 8, 25, 12, 0, tzinfo=timezone.utc)
                ),
                1,
            )

        status, _ = self.request("GET", "/jobs/1/applications", token="owner")
        self.assertEqual(status, 200)
        with self.api.get_db() as db:
            with mock.patch.object(
                self.api, "send_transactional_notification_email",
                side_effect=AssertionError("obsolete reminder must not call provider"),
            ):
                result = self.api.flush_transactional_notification_emails(
                    db, now=datetime(2026, 8, 25, 12, 1, tzinfo=timezone.utc)
                )
            self.assertEqual(result["suppressed"], 0)
            row = db.execute(
                "SELECT state,delivery_status,attempts,last_error FROM transactional_email_outbox"
            ).fetchone()
            self.assertEqual(tuple(row)[:3], ("failed", "suppressed", 0))
            self.assertEqual(row["last_error"], "application cohort viewed")

    def test_view_then_new_application_does_not_revalidate_old_reminder(self):
        with self.api.get_db() as db:
            db.execute(
                "UPDATE notification_system_state SET application_reminders_enabled_at='2026-08-20 00:00:00' WHERE id=1"
            )
            db.execute(
                """INSERT INTO applications
                   (id,job_id,worker_id,cover_message,status,created_at)
                   VALUES(1,1,1,'Original cohort','pending','2026-08-24 10:00:00')"""
            )
            db.commit()
            self.assertEqual(self.api.generate_application_reminders(
                db, now=datetime(2026, 8, 25, 12, 0, tzinfo=timezone.utc),
            ), 1)
        status, _ = self.request("GET", "/jobs/1/applications", token="owner")
        self.assertEqual(status, 200)
        with self.api.get_db() as db:
            db.execute(
                """INSERT INTO applications
                   (id,job_id,worker_id,cover_message,status,created_at)
                   VALUES(2,1,3,'New after review','pending','2026-08-25 12:01:00')"""
            )
            db.commit()
            self.api.RESEND_API_KEY = "configured-for-test"
            with mock.patch.object(
                self.api, "send_transactional_notification_email",
                side_effect=AssertionError("new application must not revive old cohort reminder"),
            ):
                result = self.api.flush_transactional_notification_emails(
                    db, now=datetime(2026, 8, 25, 12, 2, tzinfo=timezone.utc), limit=10,
                )
            self.assertEqual(result["suppressed"], 0)

    def test_same_second_application_after_view_gets_distinct_cohort(self):
        with self.api.get_db() as db:
            db.execute(
                "UPDATE notification_system_state SET application_reminders_enabled_at='2026-08-20 00:00:00' WHERE id=1"
            )
            db.execute(
                """INSERT INTO applications
                   (id,job_id,worker_id,cover_message,status,created_at)
                   VALUES(1,1,1,'Original cohort','pending','2026-08-24 10:00:00')"""
            )
            db.commit()
            self.assertEqual(self.api.generate_application_reminders(
                db, now=datetime(2026, 8, 25, 12, 0, tzinfo=timezone.utc),
            ), 1)
        status, _ = self.request("GET", "/jobs/1/applications", token="owner")
        self.assertEqual(status, 200)
        with self.api.get_db() as db:
            db.execute(
                """INSERT INTO applications
                   (id,job_id,worker_id,cover_message,status,created_at)
                   VALUES(2,1,3,'Same-second later cohort','pending','2026-08-24 10:00:00')"""
            )
            db.commit()
            self.assertEqual(self.api.generate_application_reminders(
                db, now=datetime(2026, 8, 25, 12, 1, tzinfo=timezone.utc),
            ), 1)
            cohorts = db.execute(
                """SELECT cohort_first_application_id FROM job_application_reminders
                   ORDER BY id"""
            ).fetchall()
            self.assertEqual([row[0] for row in cohorts], [1, 2])

    def test_24h_reminder_expires_at_exact_48h_stage_boundary(self):
        with self.api.get_db() as db:
            db.execute(
                "UPDATE notification_system_state SET application_reminders_enabled_at='2026-08-20 00:00:00' WHERE id=1"
            )
            db.execute(
                """INSERT INTO applications
                   (id,job_id,worker_id,cover_message,status,created_at)
                   VALUES(1,1,1,'Original cohort','pending','2026-08-24 10:00:00')"""
            )
            db.commit()
            self.assertEqual(self.api.generate_application_reminders(
                db, now=datetime(2026, 8, 25, 11, 0, tzinfo=timezone.utc),
            ), 1)
            row = db.execute(
                "SELECT expires_at FROM transactional_email_outbox WHERE notification_type='application_reminder_24h'"
            ).fetchone()
            self.assertEqual(row["expires_at"], "2026-08-26 10:00:00")
            self.api.RESEND_API_KEY = "configured-for-test"
            with mock.patch.object(
                self.api, "send_transactional_notification_email",
                side_effect=AssertionError("expired reminder must not call provider"),
            ):
                result = self.api.flush_transactional_notification_emails(
                    db, now=datetime(2026, 8, 26, 10, 0, tzinfo=timezone.utc), limit=10,
                )
            self.assertEqual(result["stale_skipped"], 1)

    def test_api_key_applications_read_does_not_advance_human_view_cursor(self):
        raw_key = "ghh_notification_read_test"
        with self.api.get_db() as db:
            db.execute(
                """INSERT INTO api_keys(user_id,key_hash,key_prefix,name,scopes,is_active)
                   VALUES(2,?,?,?, '[\"read\"]',1)""",
                [hashlib.sha256(raw_key.encode()).hexdigest(), raw_key[:12], "Read test"],
            )
            db.commit()
        status, _ = self.request("GET", "/jobs/1/applications", api_key=raw_key)
        self.assertEqual(status, 200)
        with self.api.get_db() as db:
            self.assertIsNone(db.execute(
                "SELECT last_seen_application_id FROM job_application_views WHERE job_id=1 AND employer_id=2"
            ).fetchone())
        for token, expected_status in (("admin", 200), ("worker", 403), ("", 401)):
            status, _ = self.request("GET", "/jobs/1/applications", token=token)
            self.assertEqual(status, expected_status)
        with self.api.get_db() as db:
            self.assertIsNone(db.execute(
                "SELECT 1 FROM job_application_views WHERE job_id=1 AND employer_id=2"
            ).fetchone())
        status, _ = self.request("GET", "/jobs/1/applications", token="owner")
        self.assertEqual(status, 200)
        with self.api.get_db() as db:
            self.assertIsNotNone(db.execute(
                "SELECT 1 FROM job_application_views WHERE job_id=1 AND employer_id=2"
            ).fetchone())

    def test_concurrent_human_view_and_materialization_leave_no_obsolete_reminder(self):
        with self.api.get_db() as db:
            db.execute(
                "UPDATE notification_system_state SET application_reminders_enabled_at='2026-08-20 00:00:00' WHERE id=1"
            )
            db.execute(
                """INSERT INTO applications
                   (id,job_id,worker_id,cover_message,status,created_at)
                   VALUES(1,1,1,'Concurrent cohort','pending','2026-08-24 10:00:00')"""
            )
            db.commit()
        barrier = threading.Barrier(2)
        results = {}

        def materialize():
            barrier.wait()
            with self.api.get_db() as worker_db:
                results["created"] = self.api.generate_application_reminders(
                    worker_db, now=datetime(2026, 8, 25, 12, 0, tzinfo=timezone.utc)
                )

        def view():
            barrier.wait()
            results["view"] = self.request(
                "GET", "/jobs/1/applications", token="owner"
            )[0]

        threads = [threading.Thread(target=materialize), threading.Thread(target=view)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(10)
            self.assertFalse(thread.is_alive())
        self.assertEqual(results["view"], 200)
        self.assertIn(results["created"], (0, 1))
        with self.api.get_db() as db:
            self.assertEqual(db.execute(
                """SELECT COUNT(*) FROM notifications
                   WHERE user_id=2 AND is_read=0
                     AND type IN ('application_reminder_24h','application_reminder_72h')"""
            ).fetchone()[0], 0)
            self.assertEqual(db.execute(
                """SELECT COUNT(*) FROM transactional_email_outbox
                   WHERE user_id=2 AND state='pending'
                     AND notification_type IN (
                       'application_reminder_24h','application_reminder_72h'
                     )"""
            ).fetchone()[0], 0)

    def test_view_after_claim_suppresses_before_provider_io(self):
        # Keep the cohort and worker on SQLite's outbox enqueue clock.
        with self.api.get_db() as db:
            db.execute(
                "UPDATE notification_system_state SET application_reminders_enabled_at=datetime('now','-5 days') WHERE id=1"
            )
            db.execute(
                """INSERT INTO applications
                   (id,job_id,worker_id,cover_message,status,created_at)
                   VALUES(1,1,1,'Claim race cohort','pending',datetime('now','-26 hours'))"""
            )
            db.commit()
            self.assertEqual(self.api.generate_application_reminders(
                db,
            ), 1)

            class ViewBeforeSecondWriter:
                def __init__(proxy_self, connection, callback):
                    proxy_self.connection = connection
                    proxy_self.callback = callback
                    proxy_self.begin_count = 0

                def execute(proxy_self, sql, params=()):
                    if sql.strip().upper() == "BEGIN IMMEDIATE":
                        proxy_self.begin_count += 1
                        if proxy_self.begin_count == 3:
                            proxy_self.callback()
                    return proxy_self.connection.execute(sql, params)

                def __getattr__(proxy_self, name):
                    return getattr(proxy_self.connection, name)

            view_statuses = []
            proxy = ViewBeforeSecondWriter(
                db,
                lambda: view_statuses.append(self.request(
                    "GET", "/jobs/1/applications", token="owner"
                )[0]),
            )
            provider_calls = []
            self.api.RESEND_API_KEY = "configured-for-test"
            with mock.patch.object(
                self.api, "send_email",
                side_effect=lambda *args, **kwargs: provider_calls.append(args) or "provider-id",
            ):
                result = self.api.flush_transactional_notification_emails(
                    proxy,
                )
            self.assertEqual(view_statuses, [200])
            self.assertEqual(provider_calls, [])
            self.assertEqual(result["sent"], 0)
            self.assertEqual(result["suppressed"], 1)
            row = db.execute(
                """SELECT state,delivery_status,email_to,title,message,link
                   FROM transactional_email_outbox
                   WHERE notification_type='application_reminder_24h'"""
            ).fetchone()
            self.assertEqual(tuple(row), ("failed", "suppressed", "", "", "", ""))

    def test_send_uses_current_account_address_and_redacts_terminal_payload(self):
        with self.api.get_db() as db:
            self.api.push_notification(
                db, 2, "new_application", "Queued private title",
                "Queued private message", "#/jobs/1/applications", email=True,
            )
            db.execute(
                "UPDATE users SET email='current-owner@example.com' WHERE id=2"
            )
            db.commit()
            sent_to = []
            self.api.RESEND_API_KEY = "configured-for-test"
            with mock.patch.object(
                self.api, "send_email",
                side_effect=lambda to, *args, **kwargs: sent_to.append(to) or "provider-current-address",
            ):
                result = self.api.flush_transactional_notification_emails(
                    db, now=datetime.now(timezone.utc), limit=1,
                )
            self.assertEqual(result["sent"], 1)
            self.assertEqual(sent_to, ["current-owner@example.com"])
            row = db.execute(
                """SELECT state,email_to,title,message,link,provider_email_id
                   FROM transactional_email_outbox"""
            ).fetchone()
            self.assertEqual(
                tuple(row),
                ("sent", "", "", "", "", "provider-current-address"),
            )

    def test_notification_schema_validation_rejects_poisoned_same_name_table(self):
        with self.api.get_db() as db:
            db.execute("DROP TABLE notification_worker_leases")
            db.execute(
                "CREATE TABLE notification_worker_leases(worker_name TEXT PRIMARY KEY)"
            )
            db.commit()
        with self.assertRaisesRegex(RuntimeError, "Required notification schema missing"):
            self.api.init_db()

    def test_notification_schema_validation_rejects_poisoned_table_contracts(self):
        canonical = """CREATE TABLE transactional_email_outbox (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL REFERENCES users(id),
            notification_id INTEGER REFERENCES notifications(id),
            email_to TEXT NOT NULL,
            notification_type TEXT NOT NULL,
            title TEXT NOT NULL,
            message TEXT NOT NULL,
            link TEXT NOT NULL DEFAULT '',
            dedupe_context TEXT NOT NULL,
            dedupe_key TEXT NOT NULL UNIQUE,
            state TEXT NOT NULL DEFAULT 'pending'
                CHECK(state IN ('pending','sending','sent','failed')),
            attempts INTEGER NOT NULL DEFAULT 0,
            claimed_at TEXT,
            claim_token TEXT,
            last_error TEXT,
            next_attempt_at TEXT,
            expires_at TEXT,
            reminder_job_id INTEGER,
            reminder_first_application_id INTEGER,
            reminder_last_application_id INTEGER,
            reminder_cohort_started_at TEXT,
            reminder_stage TEXT,
            provider_email_id TEXT,
            delivery_status TEXT,
            delivered_at TEXT,
            bounced_at TEXT,
            complained_at TEXT,
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            sent_at TEXT
        )"""
        poisons = {
            "dedupe uniqueness moved to provider id": canonical.replace(
                "dedupe_key TEXT NOT NULL UNIQUE,",
                "dedupe_key TEXT NOT NULL,",
            ).replace(
                "provider_email_id TEXT,",
                "provider_email_id TEXT UNIQUE,",
            ),
            "foreign key bindings swapped": canonical.replace(
                "user_id INTEGER NOT NULL REFERENCES users(id),",
                "user_id INTEGER NOT NULL REFERENCES notifications(id),",
            ).replace(
                "notification_id INTEGER REFERENCES notifications(id),",
                "notification_id INTEGER REFERENCES users(id),",
            ),
            "foreign key action changed": canonical.replace(
                "user_id INTEGER NOT NULL REFERENCES users(id),",
                "user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,",
            ),
            "required nullability changed": canonical.replace(
                "email_to TEXT NOT NULL,", "email_to TEXT,",
            ),
            "required default changed": canonical.replace(
                "attempts INTEGER NOT NULL DEFAULT 0,",
                "attempts INTEGER NOT NULL DEFAULT 1,",
            ),
            "extra check semantics": canonical.replace(
                "sent_at TEXT\n        )",
                "sent_at TEXT, CHECK(attempts >= 0)\n        )",
            ),
        }
        for label, poisoned_sql in poisons.items():
            with self.subTest(label=label):
                try:
                    with self.api.get_db() as db:
                        db.execute("DROP TABLE transactional_email_outbox")
                        db.execute(poisoned_sql)
                        db.commit()
                    with self.assertRaisesRegex(
                        RuntimeError, "Required notification schema missing"
                    ):
                        self.api.init_db()
                finally:
                    with self.api.get_db() as db:
                        db.execute("DROP TABLE IF EXISTS transactional_email_outbox")
                        db.commit()
                    self.api.init_db()

        try:
            with self.api.get_db() as db:
                db.execute(
                    """CREATE TRIGGER poisoned_notification_outbox_trigger
                       BEFORE INSERT ON transactional_email_outbox
                       BEGIN SELECT RAISE(IGNORE); END"""
                )
                db.commit()
            with self.assertRaisesRegex(
                RuntimeError, "Required notification schema missing"
            ):
                self.api.init_db()
        finally:
            with self.api.get_db() as db:
                db.execute("DROP TRIGGER IF EXISTS poisoned_notification_outbox_trigger")
                db.commit()
            self.api.init_db()

    def test_notification_schema_validation_covers_every_required_table_contract(self):
        poisons = {
            "transactional_email_delivery_events": """
                CREATE TABLE transactional_email_delivery_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    provider_event_id TEXT NOT NULL,
                    provider_email_id TEXT,
                    event_type TEXT NOT NULL,
                    event_created_at TEXT,
                    payload_sha256 TEXT NOT NULL UNIQUE,
                    received_at TEXT NOT NULL DEFAULT (datetime('now'))
                )
            """,
            "notification_system_state": """
                CREATE TABLE notification_system_state (
                    id INTEGER CHECK(id=1),
                    application_reminders_enabled_at TEXT NOT NULL PRIMARY KEY
                )
            """,
            "notification_worker_leases": """
                CREATE TABLE notification_worker_leases (
                    worker_name TEXT NOT NULL,
                    owner_token TEXT PRIMARY KEY,
                    acquired_at TEXT NOT NULL,
                    heartbeat_at TEXT NOT NULL,
                    lease_expires_at TEXT NOT NULL
                )
            """,
            "job_application_views": """
                CREATE TABLE job_application_views (
                    job_id INTEGER NOT NULL REFERENCES jobs(id),
                    employer_id INTEGER NOT NULL REFERENCES users(id),
                    first_viewed_at TEXT NOT NULL,
                    last_viewed_at TEXT NOT NULL,
                    last_seen_application_id INTEGER NOT NULL DEFAULT 0,
                    PRIMARY KEY(employer_id,job_id)
                )
            """,
            "job_application_reminders": """
                CREATE TABLE job_application_reminders (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    job_id INTEGER NOT NULL REFERENCES jobs(id),
                    employer_id INTEGER NOT NULL UNIQUE REFERENCES users(id),
                    reminder_kind TEXT NOT NULL CHECK(reminder_kind IN ('24h','72h')),
                    cohort_started_at TEXT NOT NULL,
                    cohort_first_application_id INTEGER NOT NULL,
                    cohort_last_application_id INTEGER NOT NULL,
                    application_count INTEGER NOT NULL CHECK(application_count > 0),
                    notification_id INTEGER REFERENCES notifications(id),
                    created_at TEXT NOT NULL DEFAULT (datetime('now')),
                    UNIQUE(job_id,reminder_kind,cohort_first_application_id)
                )
            """,
        }
        for table_name, poisoned_sql in poisons.items():
            with self.subTest(table=table_name):
                try:
                    with self.api.get_db() as db:
                        db.execute(f"DROP TABLE {table_name}")
                        db.execute(poisoned_sql)
                        db.commit()
                    with self.assertRaisesRegex(
                        RuntimeError, "Required notification schema missing"
                    ):
                        self.api.init_db()
                finally:
                    with self.api.get_db() as db:
                        db.execute(f"DROP TABLE IF EXISTS {table_name}")
                        db.commit()
                    self.api.init_db()

    def test_notification_schema_validation_rejects_comment_mediated_bypasses(self):
        outbox = """CREATE TABLE transactional_email_outbox (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL REFERENCES users(id),
            notification_id INTEGER REFERENCES notifications(id),
            email_to TEXT NOT NULL,
            notification_type TEXT NOT NULL,
            title TEXT NOT NULL,
            message TEXT NOT NULL,
            link TEXT NOT NULL DEFAULT '',
            dedupe_context TEXT NOT NULL,
            dedupe_key TEXT NOT NULL UNIQUE,
            state TEXT NOT NULL DEFAULT 'pending'
                CHECK(state IN ('pending','sending','sent','failed')),
            attempts INTEGER NOT NULL DEFAULT 0,
            claimed_at TEXT,
            claim_token TEXT,
            last_error TEXT,
            next_attempt_at TEXT,
            expires_at TEXT,
            reminder_job_id INTEGER,
            reminder_first_application_id INTEGER,
            reminder_last_application_id INTEGER,
            reminder_cohort_started_at TEXT,
            reminder_stage TEXT,
            provider_email_id TEXT,
            delivery_status TEXT,
            delivered_at TEXT,
            bounced_at TEXT,
            complained_at TEXT,
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            sent_at TEXT
        )"""
        poisons = [
            (
                "fake-check", "transactional_email_outbox",
                outbox.replace(
                    "CHECK(state IN ('pending','sending','sent','failed'))",
                    "/* CHECK(state IN ('pending','sending','sent','failed')) */",
                ),
            ),
            (
                "fake-autoincrement", "transactional_email_delivery_events",
                """CREATE TABLE transactional_email_delivery_events (
                    id INTEGER PRIMARY KEY /* AUTOINCREMENT */,
                    provider_event_id TEXT NOT NULL UNIQUE,
                    provider_email_id TEXT,
                    event_type TEXT NOT NULL,
                    event_created_at TEXT,
                    payload_sha256 TEXT NOT NULL,
                    received_at TEXT NOT NULL DEFAULT (datetime('now'))
                )""",
            ),
            (
                "comment-separated-on-conflict", "transactional_email_outbox",
                outbox.replace(
                    "dedupe_key TEXT NOT NULL UNIQUE,",
                    "dedupe_key TEXT NOT NULL UNIQUE ON/**/CONFLICT IGNORE,",
                ),
            ),
            (
                "comment-separated-strict", "notification_system_state",
                """CREATE TABLE notification_system_state (
                    id INTEGER PRIMARY KEY CHECK(id=1),
                    application_reminders_enabled_at TEXT NOT NULL
                )/**/STRICT""",
            ),
            (
                "comment-separated-without-rowid", "job_application_views",
                """CREATE TABLE job_application_views (
                    job_id INTEGER NOT NULL REFERENCES jobs(id),
                    employer_id INTEGER NOT NULL REFERENCES users(id),
                    first_viewed_at TEXT NOT NULL,
                    last_viewed_at TEXT NOT NULL,
                    last_seen_application_id INTEGER NOT NULL DEFAULT 0,
                    PRIMARY KEY(job_id,employer_id)
                )/**/WITHOUT/**/ROWID""",
            ),
        ]
        for label, table_name, poisoned_sql in poisons:
            with self.subTest(poison=label):
                try:
                    with self.api.get_db() as db:
                        db.execute(f"DROP TABLE {table_name}")
                        db.execute(poisoned_sql)
                        db.commit()
                    with self.assertRaisesRegex(
                        RuntimeError, "Required notification schema missing"
                    ):
                        self.api.init_db()
                finally:
                    with self.api.get_db() as db:
                        db.execute(f"DROP TABLE IF EXISTS {table_name}")
                        db.commit()
                    self.api.init_db()

    def test_canonical_notification_table_semantics_are_behavioral(self):
        quoted = """CREATE TABLE [strict] (
            value TEXT DEFAULT 'AUTOINCREMENT -- /* ON CONFLICT */',
            [match] TEXT
        )"""
        executable, had_comments = self.api._strip_notification_sql_comments(quoted)
        self.assertFalse(had_comments)
        tokens = self.api._notification_sql_tokens(executable)
        for keyword in ("autoincrement", "strict", "match", "on", "conflict"):
            self.assertNotIn(keyword, tokens)

        with self.api.get_db() as db:
            with self.assertRaises(sqlite3.IntegrityError):
                db.execute("""INSERT INTO transactional_email_outbox
                    (user_id,email_to,notification_type,title,message,
                     dedupe_context,dedupe_key,state,expires_at)
                    VALUES(2,'valid@example.com','probe','T','M','state-check',
                           'state-check','corrupt','2099-01-01 00:00:00')""")
            db.rollback()

            db.execute("""INSERT INTO transactional_email_outbox
                (user_id,email_to,notification_type,title,message,
                 dedupe_context,dedupe_key,expires_at)
                VALUES(2,'valid@example.com','probe','T','M','dedupe-check',
                       'dedupe-check','2099-01-01 00:00:00')""")
            with self.assertRaises(sqlite3.IntegrityError):
                db.execute("""INSERT INTO transactional_email_outbox
                    (user_id,email_to,notification_type,title,message,
                     dedupe_context,dedupe_key,expires_at)
                    VALUES(2,'valid@example.com','probe','T','M','dedupe-check-2',
                           'dedupe-check','2099-01-01 00:00:00')""")
            db.rollback()

            first = db.execute("""INSERT INTO transactional_email_delivery_events
                (provider_event_id,event_type,payload_sha256)
                VALUES('autoincrement-1','email.sent','one')""").lastrowid
            db.commit()
            db.execute(
                "DELETE FROM transactional_email_delivery_events WHERE id=?", [first]
            )
            db.commit()
            second = db.execute("""INSERT INTO transactional_email_delivery_events
                (provider_event_id,event_type,payload_sha256)
                VALUES('autoincrement-2','email.sent','two')""").lastrowid
            db.commit()
            self.assertGreater(second, first)

    def test_production_base_outbox_schema_migrates_into_exact_contract(self):
        with self.api.get_db() as db:
            db.execute("DROP TABLE transactional_email_outbox")
            db.execute("""CREATE TABLE transactional_email_outbox (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL REFERENCES users(id),
                notification_id INTEGER REFERENCES notifications(id),
                email_to TEXT NOT NULL,
                notification_type TEXT NOT NULL,
                title TEXT NOT NULL,
                message TEXT NOT NULL,
                link TEXT NOT NULL DEFAULT '',
                dedupe_context TEXT NOT NULL,
                dedupe_key TEXT NOT NULL UNIQUE,
                state TEXT NOT NULL DEFAULT 'pending'
                    CHECK(state IN ('pending','sending','sent','failed')),
                attempts INTEGER NOT NULL DEFAULT 0,
                claimed_at TEXT,
                last_error TEXT,
                created_at TEXT NOT NULL DEFAULT (datetime('now')),
                sent_at TEXT
            )""")
            db.execute(
                """INSERT INTO transactional_email_outbox
                   (user_id,email_to,notification_type,title,message,
                    dedupe_context,dedupe_key)
                   VALUES(2,'legacy@example.com','new_application','Legacy','Legacy',
                          'legacy-base','legacy-base')"""
            )
            db.commit()

        self.api.init_db()

        with self.api.get_db() as db:
            columns = {
                row[1] for row in db.execute(
                    "PRAGMA table_xinfo('transactional_email_outbox')"
                ).fetchall()
            }
            self.assertTrue({
                "claim_token", "expires_at", "provider_email_id",
                "delivery_status", "reminder_first_application_id",
            }.issubset(columns))
            row = db.execute(
                """SELECT state,delivery_status,email_to,title,message,link
                   FROM transactional_email_outbox WHERE dedupe_key='legacy-base'"""
            ).fetchone()
            self.assertEqual(
                tuple(row), ("failed", "suppressed", "", "", "", "")
            )

    def test_notification_schema_validation_rejects_poisoned_same_name_indexes(self):
        canonical = (
            "CREATE UNIQUE INDEX idx_email_outbox_provider_id "
            "ON transactional_email_outbox(provider_email_id) "
            "WHERE provider_email_id IS NOT NULL"
        )
        poisoned = {
            "wrong column": (
                "CREATE UNIQUE INDEX idx_email_outbox_provider_id "
                "ON transactional_email_outbox(id) WHERE id IS NOT NULL"
            ),
            "wrong predicate": (
                "CREATE UNIQUE INDEX idx_email_outbox_provider_id "
                "ON transactional_email_outbox(provider_email_id) "
                "WHERE provider_email_id != ''"
            ),
            "wrong collation": (
                "CREATE UNIQUE INDEX idx_email_outbox_provider_id "
                "ON transactional_email_outbox(provider_email_id COLLATE NOCASE) "
                "WHERE provider_email_id IS NOT NULL"
            ),
            "wrong direction": (
                "CREATE UNIQUE INDEX idx_email_outbox_provider_id "
                "ON transactional_email_outbox(provider_email_id DESC) "
                "WHERE provider_email_id IS NOT NULL"
            ),
        }
        for label, poison_sql in poisoned.items():
            with self.subTest(label=label):
                with self.api.get_db() as db:
                    db.execute("DROP INDEX idx_email_outbox_provider_id")
                    db.execute(poison_sql)
                    db.commit()
                with self.assertRaisesRegex(
                    RuntimeError, "invalid index idx_email_outbox_provider_id"
                ):
                    self.api.init_db()
                with self.api.get_db() as db:
                    db.execute("DROP INDEX idx_email_outbox_provider_id")
                    db.execute(canonical)
                    db.commit()

    def test_server_rejects_oversize_body_before_proxy_buffering(self):
        server = load_server()
        response = server.app.test_client().post(
            "/webhooks/resend",
            data=b"x" * ((2 * 1024 * 1024) + 1),
            content_type="application/json",
        )
        self.assertEqual(response.status_code, 413)

    def test_suspended_recipient_is_suppressed_without_provider_retry(self):
        self.api.RESEND_API_KEY = "configured-for-test"
        with self.api.get_db() as db:
            db.execute("UPDATE users SET is_suspended=1 WHERE id=2")
            db.execute(
                """INSERT INTO transactional_email_outbox
                   (user_id,email_to,notification_type,title,message,link,
                    dedupe_context,dedupe_key,state,next_attempt_at,expires_at)
                   VALUES(2,'owner@example.com','new_application','Suspended','Message',
                          '/jobs/1/applications','suspended','suspended-recipient-key',
                          'pending','2026-08-24 11:00:00','2026-08-26 12:00:00')"""
            )
            db.commit()
            with mock.patch.object(
                self.api, "send_transactional_notification_email",
                side_effect=AssertionError("ineligible recipient must not call provider"),
            ):
                result = self.api.flush_transactional_notification_emails(
                    db, now=datetime(2026, 8, 24, 12, 0, tzinfo=timezone.utc), limit=10,
                )
            self.assertEqual(result["suppressed"], 1)
            row = db.execute(
                "SELECT state,delivery_status,last_error,attempts FROM transactional_email_outbox"
            ).fetchone()
            self.assertEqual(tuple(row), (
                "failed", "suppressed", "recipient is not eligible for email", 0,
            ))

    def test_reminders_never_backfill_applications_before_deployment_cutoff(self):
        with self.api.get_db() as db:
            enabled_at = db.execute(
                "SELECT application_reminders_enabled_at FROM notification_system_state WHERE id=1"
            ).fetchone()[0]
            db.execute(
                """INSERT INTO applications
                   (id,job_id,worker_id,cover_message,status,created_at)
                   VALUES(2,1,1,'Historical','pending',datetime(?,'-1 day'))""",
                [enabled_at],
            )
            db.commit()
            created = self.api.generate_application_reminders(
                db, now=datetime.now(timezone.utc).replace(microsecond=0) + self.api.timedelta(days=10)
            )
            self.assertEqual(created, 0)
            self.assertEqual(
                db.execute("SELECT COUNT(*) FROM job_application_reminders").fetchone()[0], 0
            )

    def test_reminders_expire_after_96_hours_without_catchup(self):
        with self.api.get_db() as db:
            db.execute(
                "UPDATE notification_system_state SET application_reminders_enabled_at='2026-08-20 00:00:00' WHERE id=1"
            )
            db.execute(
                """INSERT INTO applications
                   (id,job_id,worker_id,cover_message,status,created_at)
                   VALUES(3,1,1,'Too old for a reminder','pending','2026-08-20 10:00:00')"""
            )
            db.commit()
            created = self.api.generate_application_reminders(
                db, now=datetime(2026, 8, 24, 12, 0, tzinfo=timezone.utc)
            )
            self.assertEqual(created, 0)
            self.assertEqual(
                db.execute("SELECT COUNT(*) FROM job_application_reminders").fetchone()[0], 0
            )

    def test_svix_authoritative_known_vector_and_replay_window(self):
        args = (
            b'{"test": 2432232314}',
            "msg_p5jXN8AQM9LWM0D4loKWxJek",
            "1614265330",
            "v1,g0hM9SsE+OTPJTGt/tmIKtSyZlE3uFJELVlNIOLJ1OE=",
        )
        secret = "whsec_MfKQ9r8GKYqrTwjUPD8ILPZIo2LaLaSw"
        self.assertTrue(
            self.api.verify_resend_webhook_signature(
                *args, secret=secret, now=1614265330,
            )
        )
        self.assertFalse(
            self.api.verify_resend_webhook_signature(
                *args, secret=secret, now=1614265631,
            )
        )
        self.assertFalse(
            self.api.verify_resend_webhook_signature(
                args[0], args[1], args[2],
                "v1," + base64.b64encode(hmac.new(
                    b"", f"{args[1]}.{args[2]}.".encode() + args[0], hashlib.sha256,
                ).digest()).decode(),
                secret="whsec_", now=1614265330,
            )
        )
        self.assertFalse(
            self.api.verify_resend_webhook_signature(
                args[0], args[1], args[2],
                "v1," + base64.b64encode(hmac.new(
                    b"\x00", f"{args[1]}.{args[2]}.".encode() + args[0], hashlib.sha256,
                ).digest()).decode(),
                secret="whsec_AA==", now=1614265330,
            )
        )
        for malformed_secret in ("", "not-prefixed", "whsec_***", "whsec_AQ=="):
            self.assertFalse(
                self.api.verify_resend_webhook_signature(
                    *args, secret=malformed_secret, now=1614265330,
                )
            )
        self.assertFalse(
            self.api.verify_resend_webhook_signature(
                *args, secret="whsec_" + base64.b64encode(b"wrong-secret-material").decode(),
                now=1614265330,
            )
        )

    def test_signed_resend_delivery_webhook_is_private_and_idempotent(self):
        secret_bytes = b"notification-test-secret"
        self.api.RESEND_WEBHOOK_SECRET = "whsec_" + base64.b64encode(secret_bytes).decode()

        provider_email_id = "email-provider-123"
        with self.api.get_db() as db:
            db.execute(
                """INSERT INTO transactional_email_outbox
                   (user_id,email_to,notification_type,title,message,link,dedupe_context,
                    dedupe_key,state,attempts,provider_email_id,sent_at)
                   VALUES(2,'owner@example.com','new_application','New application','Message',
                          '/jobs/1/applications','application:1','webhook-key','sent',1,?,datetime('now'))""",
                [provider_email_id],
            )
            db.commit()

        payload = {
            "type": "email.delivered",
            "created_at": "2026-08-24T12:00:00Z",
            "data": {
                "email_id": provider_email_id,
                "to": ["owner@example.com"],
                "subject": "private subject must not be persisted",
            },
        }
        raw = json.dumps(payload, separators=(",", ":")).encode()
        message_id = "msg_webhook_1"
        timestamp = str(int(time.time()))
        signed = f"{message_id}.{timestamp}.".encode() + raw
        signature = base64.b64encode(
            hmac.new(secret_bytes, signed, hashlib.sha256).digest()
        ).decode()
        headers = {
            "svix-id": message_id,
            "svix-timestamp": timestamp,
            "svix-signature": f"v1,{signature}",
        }

        status, response = self.request(
            "POST", "/webhooks/resend", payload=payload, headers=headers
        )
        self.assertEqual(status, 200, response)
        duplicate_status, duplicate = self.request(
            "POST", "/webhooks/resend", payload=payload, headers=headers
        )
        self.assertEqual(duplicate_status, 200, duplicate)
        bad_status, _ = self.request(
            "POST", "/webhooks/resend", payload=payload,
            headers={**headers, "svix-id": "msg_webhook_tampered"},
        )
        self.assertEqual(bad_status, 400)

        late_payload = {
            "type": "email.sent",
            "created_at": "2026-08-24T11:59:00Z",
            "data": {"email_id": provider_email_id},
        }
        late_raw = json.dumps(late_payload, separators=(",", ":")).encode()
        late_id = "msg_webhook_late_sent"
        late_signed = f"{late_id}.{timestamp}.".encode() + late_raw
        late_signature = base64.b64encode(
            hmac.new(secret_bytes, late_signed, hashlib.sha256).digest()
        ).decode()
        late_status, late_response = self.request(
            "POST", "/webhooks/resend", payload=late_payload,
            headers={
                "svix-id": late_id,
                "svix-timestamp": timestamp,
                "svix-signature": f"v1,{late_signature}",
            },
        )
        self.assertEqual(late_status, 200, late_response)

        with self.api.get_db() as db:
            outbox = db.execute(
                "SELECT delivery_status,delivered_at FROM transactional_email_outbox WHERE provider_email_id=?",
                [provider_email_id],
            ).fetchone()
            self.assertEqual(outbox["delivery_status"], "delivered")
            self.assertIsNotNone(outbox["delivered_at"])
            self.assertEqual(
                db.execute("SELECT COUNT(*) FROM transactional_email_delivery_events").fetchone()[0], 2
            )
            event = db.execute(
                """SELECT provider_event_id,provider_email_id,event_type,payload_sha256
                   FROM transactional_email_delivery_events WHERE event_type='email.delivered'"""
            ).fetchone()
            self.assertEqual(event["provider_event_id"], message_id)
            self.assertEqual(event["provider_email_id"], provider_email_id)
            self.assertEqual(event["event_type"], "email.delivered")
            self.assertEqual(event["payload_sha256"], hashlib.sha256(raw).hexdigest())
            columns = {
                row["name"] for row in db.execute(
                    "PRAGMA table_xinfo('transactional_email_delivery_events')"
                ).fetchall()
            }
            self.assertFalse({"payload", "email_to", "subject", "recipient"} & columns)

    def test_authenticated_unknown_resend_event_is_acknowledged_without_mutation(self):
        secret_bytes = b"notification-test-secret"
        self.api.RESEND_WEBHOOK_SECRET = "whsec_" + base64.b64encode(secret_bytes).decode()
        payload = {"type": "domain.updated", "created_at": "2026-08-24T12:00:00Z", "data": {}}
        raw = json.dumps(payload, separators=(",", ":")).encode()
        message_id = "msg_unknown_event"
        timestamp = str(int(time.time()))
        signature = base64.b64encode(hmac.new(
            secret_bytes,
            f"{message_id}.{timestamp}.".encode() + raw,
            hashlib.sha256,
        ).digest()).decode()
        status, body = self.request(
            "POST", "/webhooks/resend", raw_body=raw,
            headers={
                "svix-id": message_id,
                "svix-timestamp": timestamp,
                "svix-signature": f"v1,{signature}",
            },
        )
        self.assertEqual(status, 200, body)
        self.assertTrue(body["ignored"])
        with self.api.get_db() as db:
            event = db.execute(
                "SELECT event_type,provider_email_id FROM transactional_email_delivery_events"
            ).fetchone()
            self.assertEqual(tuple(event), ("domain.updated", None))

    def test_provider_acceptance_id_is_persisted_for_webhook_reconciliation(self):
        self.api.RESEND_API_KEY = "configured-for-mocked-provider"
        self.api.EMAIL_FROM = "GoHireHumans <hello@gohirehumans.com>"
        with self.api.get_db() as db:
            notification_id = self.api.push_notification(
                db, 2, "new_application", "New application: Review a document",
                "Worker One applied to your job.", "/jobs/1/applications",
                email=True, email_dedupe="application:provider-id-test",
            )
            db.commit()
            response = mock.MagicMock()
            response.read.return_value = b'{"id":"email-provider-accepted"}'
            response.close.return_value = None
            with mock.patch.object(self.api.urllib.request, "urlopen", return_value=response) as send:
                result = self.api.flush_transactional_notification_emails(
                    db, now=datetime.now(timezone.utc).replace(microsecond=0)
                )
            self.assertEqual(result["sent"], 1)
            row = db.execute(
                """SELECT state,provider_email_id,delivery_status
                   FROM transactional_email_outbox WHERE notification_id=?""",
                [notification_id],
            ).fetchone()
            self.assertEqual(tuple(row), ("sent", "email-provider-accepted", "accepted"))
            sent_request = send.call_args.args[0]
            self.assertTrue(sent_request.get_header("Idempotency-key"))
            email_payload = json.loads(sent_request.data.decode("utf-8"))
            self.assertIn("Review applications", email_payload["html"])
            self.assertIn(
                "You received this because this relates to your GoHireHumans marketplace activity.",
                email_payload["html"],
            )

    def test_resend_2xx_without_provider_id_remains_unknown_and_retryable(self):
        with self.api.get_db() as db:
            db.execute(
                """INSERT INTO transactional_email_outbox
                   (user_id,notification_id,email_to,notification_type,title,message,link,
                    dedupe_context,dedupe_key,state,next_attempt_at,expires_at)
                   VALUES(2,NULL,'','new_application','Queued','Body','#/jobs/1/applications',
                          'missing-provider-id','missing-provider-id','pending',
                          '2026-08-24 12:00:00','2026-08-25 12:00:00')"""
            )
            db.commit()
            self.api.RESEND_API_KEY = "configured-for-test"
            response = SimpleNamespace(read=lambda: b"{}", close=lambda: None)
            with mock.patch.object(self.api.urllib.request, "urlopen", return_value=response):
                result = self.api.flush_transactional_notification_emails(
                    db, now=datetime(2026, 8, 24, 12, 0, tzinfo=timezone.utc), limit=1,
                )
            row = db.execute(
                """SELECT state,delivery_status,provider_email_id,attempts,next_attempt_at
                   FROM transactional_email_outbox WHERE dedupe_key='missing-provider-id'"""
            ).fetchone()
            self.assertEqual(result["sent"], 0)
            self.assertEqual(result["deferred"], 1)
            self.assertEqual(tuple(row)[:4], ("pending", "unknown", None, 1))
            self.assertGreater(row["next_attempt_at"], "2026-08-24 12:00:00")
        for malformed_body in (b"{", b'{"id":""}', b'{"id":1}'):
            response = SimpleNamespace(
                read=lambda body=malformed_body: body,
                close=lambda: None,
            )
            with mock.patch.object(self.api.urllib.request, "urlopen", return_value=response):
                self.assertEqual(
                    self.api.send_email(
                        "recipient@example.com", "Subject", "<p>Body</p>",
                        idempotency_key="stable-provider-key",
                    ),
                    ("unknown", None),
                )

    def test_admin_notification_health_is_actionable_and_contains_no_message_pii(self):
        self.api.RESEND_API_KEY = ""
        self.api.RESEND_WEBHOOK_SECRET = ""
        with self.api.get_db() as db:
            db.execute(
                """INSERT INTO transactional_email_outbox
                   (user_id,email_to,notification_type,title,message,link,dedupe_context,
                    dedupe_key,state,next_attempt_at,expires_at,created_at)
                   VALUES(2,'private-owner@example.com','new_application','Private title',
                          'Private message','/private-link','private-context','health-key',
                          'pending',datetime('now','-1 minute'),datetime('now','+1 hour'),
                          datetime('now','-10 minutes'))"""
            )
            db.execute(
                """INSERT INTO transactional_email_outbox
                   (user_id,email_to,notification_type,title,message,link,dedupe_context,
                    dedupe_key,state,created_at)
                   VALUES(2,'stale@example.com','new_order','Stale private title',
                          'Stale private message','/stale','stale-context','stale-key',
                          'pending',datetime('now','-2 days'))"""
            )
            db.execute(
                """INSERT INTO transactional_email_outbox
                   (user_id,email_to,notification_type,title,message,link,dedupe_context,
                    dedupe_key,state,delivery_status,last_error,created_at)
                   VALUES(2,'suppressed@example.com','application_reminder_24h',
                          'Suppressed private title','Suppressed private message',
                          '/jobs/1/applications','suppressed-private-context','suppressed-key',
                          'failed','suppressed','obsolete application reminder',datetime('now'))"""
            )
            db.commit()
            self.api.acquire_notification_worker_lease(
                db, "private-owner-token", now=datetime.now(timezone.utc),
                lease_seconds=120,
            )

        denied, _ = self.request("GET", "/admin/notification-health", token="owner")
        self.assertEqual(denied, 403)
        status, health = self.request("GET", "/admin/notification-health", token="admin")
        self.assertEqual(status, 200, health)
        self.assertEqual(
            health["configuration"],
            {
                "provider_configured": False,
                "sender_configured": True,
                "webhook_configured": False,
                "worker_enabled": False,
            },
        )
        self.assertEqual(health["outbox"]["states"]["pending"], 1)
        self.assertEqual(health["outbox"]["states"]["failed"], 0)
        self.assertEqual(health["outbox"]["suppressed"], 2)
        self.assertEqual(health["outbox"]["eligible_pending"], 1)
        self.assertEqual(health["outbox"]["stale_pending"], 0)
        self.assertGreaterEqual(health["outbox"]["oldest_pending_age_seconds"], 0)
        self.assertEqual(health["outbox"]["by_type"]["new_application"]["pending"], 1)
        self.assertTrue(health["worker"]["lease_active"])
        self.assertIsNotNone(health["worker"]["last_heartbeat_at"])
        self.assertIsNotNone(health["worker"]["lease_expires_at"])
        serialized = json.dumps(health)
        for private_value in (
            "private-owner@example.com", "stale@example.com", "Private title",
            "Private message", "/private-link", "private-context", "health-key",
            "suppressed@example.com", "Suppressed private title",
            "Suppressed private message", "suppressed-private-context",
            "suppressed-key", "private-owner-token",
        ):
            self.assertNotIn(private_value, serialized)

    def test_periodic_maintenance_generates_and_flushes_due_reminder(self):
        self.api.RESEND_API_KEY = "configured-for-test"
        owner_token = "periodic-owner"
        # Use the enqueue wall clock, including when maintenance opens a new DB.
        lease_now = datetime.now(timezone.utc) - timedelta(minutes=1)
        with self.api.get_db() as db:
            db.execute(
                "UPDATE notification_system_state SET application_reminders_enabled_at=datetime('now','-5 days') WHERE id=1"
            )
            db.execute(
                """INSERT INTO applications
                   (id,job_id,worker_id,cover_message,status,created_at)
                   VALUES(9,1,1,'Maintenance test','pending',datetime('now','-26 hours'))"""
            )
            self.assertTrue(self.api.acquire_notification_worker_lease(
                db, owner_token, now=lease_now, lease_seconds=120,
            ))
            db.commit()

        with mock.patch.object(
            self.api, "send_transactional_notification_email", return_value="email-maintenance-1"
        ):
            result = self.api.run_notification_maintenance_once(
                owner_token=owner_token, lease_seconds=120, lease_now=lease_now,
            )
        self.assertEqual(result["application_reminders_created"], 1)
        self.assertEqual(result["email_delivery"]["sent"], 1)
        with self.api.get_db() as db:
            self.assertEqual(
                db.execute("SELECT COUNT(*) FROM job_application_reminders").fetchone()[0], 1
            )
            sent = db.execute(
                "SELECT state,provider_email_id FROM transactional_email_outbox"
            ).fetchone()
            self.assertEqual(tuple(sent), ("sent", "email-maintenance-1"))

    def test_production_maintenance_stays_enabled_without_email_provider(self):
        self.api.PRODUCTION_MODE = True
        self.api.RESEND_API_KEY = ""
        owner_token = "providerless-owner"
        lease_now = datetime(2026, 8, 25, 11, 59, tzinfo=timezone.utc)
        with mock.patch.dict(os.environ, {
            "NOTIFICATION_MAINTENANCE_WORKER_ENABLED": "",
            "EMAIL_OUTBOX_WORKER_ENABLED": "",
        }):
            self.assertTrue(self.api.notification_worker_enabled())
        with self.api.get_db() as db:
            db.execute(
                "UPDATE notification_system_state SET application_reminders_enabled_at='2026-08-20 00:00:00' WHERE id=1"
            )
            db.execute(
                """INSERT INTO applications
                   (id,job_id,worker_id,cover_message,status,created_at)
                   VALUES(40,1,1,'Still notify in app','pending','2026-08-24 10:00:00')"""
            )
            self.assertTrue(self.api.acquire_notification_worker_lease(
                db, owner_token, now=lease_now, lease_seconds=120,
            ))
            db.commit()
        result = self.api.run_notification_maintenance_once(
            now=datetime(2026, 8, 25, 12, 0, tzinfo=timezone.utc),
            owner_token=owner_token, lease_seconds=120, lease_now=lease_now,
        )
        self.assertEqual(result["application_reminders_created"], 1)
        self.assertEqual(result["email_delivery"]["provider_unavailable"], 1)
        with self.api.get_db() as db:
            notification_count = db.execute(
                "SELECT COUNT(*) FROM notifications WHERE type='application_reminder_24h'"
            ).fetchone()[0]
            outbox = db.execute(
                "SELECT state,attempts FROM transactional_email_outbox"
            ).fetchone()
            self.assertEqual(notification_count, 1)
            self.assertEqual(tuple(outbox), ("pending", 0))

    def test_notification_worker_lease_allows_one_owner_and_expired_takeover(self):
        start = datetime(2026, 8, 24, 12, 0, tzinfo=timezone.utc)
        with self.api.get_db() as first_db, self.api.get_db() as second_db:
            self.assertTrue(
                self.api.acquire_notification_worker_lease(
                    first_db, "owner-a", now=start, lease_seconds=120,
                )
            )
            self.assertFalse(
                self.api.acquire_notification_worker_lease(
                    second_db, "owner-b", now=start + timedelta(seconds=30),
                    lease_seconds=120,
                )
            )
            self.assertTrue(
                self.api.acquire_notification_worker_lease(
                    first_db, "owner-a", now=start + timedelta(seconds=60),
                    lease_seconds=120,
                )
            )
            self.assertTrue(
                self.api.acquire_notification_worker_lease(
                    second_db, "owner-b", now=start + timedelta(seconds=181),
                    lease_seconds=120,
                )
            )
            self.assertFalse(
                self.api.acquire_notification_worker_lease(
                    first_db, "owner-a", now=start + timedelta(seconds=182),
                    lease_seconds=120,
                )
            )
            lease = second_db.execute(
                """SELECT worker_name,owner_token,heartbeat_at,lease_expires_at
                   FROM notification_worker_leases"""
            ).fetchone()
            self.assertEqual(lease["worker_name"], "notification-maintenance")
            self.assertEqual(lease["owner_token"], "owner-b")
            self.assertGreater(lease["lease_expires_at"], lease["heartbeat_at"])

    def test_old_lease_owner_stops_after_takeover_before_claim_or_provider_io(self):
        self.api.RESEND_API_KEY = "configured-for-test"
        owner_token = "paused-owner"
        lease_start = datetime(2026, 8, 25, 11, 0, tzinfo=timezone.utc)
        lease_now = lease_start + timedelta(seconds=30)
        with self.api.get_db() as db:
            db.execute(
                "UPDATE notification_system_state SET application_reminders_enabled_at='2026-08-20 00:00:00' WHERE id=1"
            )
            db.execute(
                """INSERT INTO applications
                   (id,job_id,worker_id,cover_message,status,created_at)
                   VALUES(41,1,1,'Lease takeover','pending','2026-08-24 10:00:00')"""
            )
            self.assertTrue(self.api.acquire_notification_worker_lease(
                db, owner_token, now=lease_start, lease_seconds=120,
            ))
            db.commit()

        original_generate = self.api.generate_application_reminders

        def generate_then_take_over(db, **kwargs):
            created = original_generate(db, **kwargs)
            with self.api.get_db() as takeover_db:
                self.assertTrue(self.api.acquire_notification_worker_lease(
                    takeover_db, "new-owner",
                    now=lease_start + timedelta(seconds=151), lease_seconds=120,
                ))
            return created

        with mock.patch.object(
            self.api, "generate_application_reminders",
            side_effect=generate_then_take_over,
        ), mock.patch.object(
            self.api, "send_transactional_notification_email",
            side_effect=AssertionError("old owner must not reach provider I/O"),
        ) as provider:
            result = self.api.run_notification_maintenance_once(
                now=datetime(2026, 8, 25, 12, 0, tzinfo=timezone.utc),
                owner_token=owner_token, lease_seconds=120, lease_now=lease_now,
            )
        self.assertEqual(result["lease_lost"], 1)
        provider.assert_not_called()
        with self.api.get_db() as db:
            row = db.execute(
                "SELECT state,attempts FROM transactional_email_outbox"
            ).fetchone()
            self.assertEqual(tuple(row), ("pending", 0))

    def test_lease_takeover_after_claim_reverts_before_provider_io(self):
        self.api.RESEND_API_KEY = "configured-for-test"
        owner_token = "claim-owner"
        lease_start = datetime(2026, 8, 25, 11, 0, tzinfo=timezone.utc)
        lease_now = lease_start + timedelta(seconds=30)
        with self.api.get_db() as db:
            db.execute(
                """INSERT INTO transactional_email_outbox
                   (user_id,email_to,notification_type,title,message,link,
                    dedupe_context,dedupe_key,state,next_attempt_at,expires_at)
                   VALUES(2,'','new_application','Claim lease','Message',
                          '/jobs/1/applications','claim-lease','claim-lease-key',
                          'pending','2026-08-25 10:00:00','2026-08-27 00:00:00')"""
            )
            self.assertTrue(self.api.acquire_notification_worker_lease(
                db, owner_token, now=lease_start, lease_seconds=120,
            ))
            db.commit()

            class LeaseTakeoverProxy:
                def __init__(proxy_self, inner):
                    proxy_self.inner = inner
                    proxy_self.begin_count = 0

                def execute(proxy_self, sql, params=()):
                    if sql == "BEGIN IMMEDIATE":
                        proxy_self.begin_count += 1
                        if proxy_self.begin_count == 3:
                            with self.api.get_db() as takeover_db:
                                self.assertTrue(
                                    self.api.acquire_notification_worker_lease(
                                        takeover_db, "claim-new-owner",
                                        now=lease_start + timedelta(seconds=151),
                                        lease_seconds=120,
                                    )
                                )
                    return proxy_self.inner.execute(sql, params)

                def __getattr__(proxy_self, name):
                    return getattr(proxy_self.inner, name)

            proxy = LeaseTakeoverProxy(db)
            with mock.patch.object(
                self.api, "send_transactional_notification_email",
                side_effect=AssertionError("lost lease must fence provider I/O"),
            ) as provider:
                result = self.api.flush_transactional_notification_emails(
                    proxy, now=datetime(2026, 8, 25, 12, 0, tzinfo=timezone.utc),
                    limit=1, owner_token=owner_token, lease_seconds=120,
                    lease_now=lease_now,
                )
            self.assertEqual(result["lease_lost"], 1)
            provider.assert_not_called()
            row = db.execute(
                """SELECT state,attempts,claim_token,last_error
                   FROM transactional_email_outbox"""
            ).fetchone()
            self.assertEqual(tuple(row)[:3], ("pending", 0, None))
            self.assertIn("lease lost", row["last_error"])

    def test_application_reminder_materialization_is_batch_bounded(self):
        with self.api.get_db() as db:
            db.execute(
                "UPDATE notification_system_state SET application_reminders_enabled_at='2026-08-20 00:00:00' WHERE id=1"
            )
            for offset in range(25):
                job_id = 100 + offset
                db.execute(
                    """INSERT INTO jobs
                       (id,employer_id,title,description,category,budget_type,
                        budget_amount,status,created_at)
                       VALUES(?,2,?,'Bounded','testing','fixed',25,'reviewing','2026-08-24 09:00:00')""",
                    [job_id, f"Bounded job {offset}"],
                )
                db.execute(
                    """INSERT INTO applications
                       (id,job_id,worker_id,cover_message,status,created_at)
                       VALUES(?,?,1,'Bounded','pending','2026-08-24 10:00:00')""",
                    [1000 + offset, job_id],
                )
            db.commit()
            created = self.api.generate_application_reminders(
                db, now=datetime(2026, 8, 25, 12, 0, tzinfo=timezone.utc),
                limit=7,
            )
            self.assertEqual(created, 7)
            count = db.execute(
                "SELECT COUNT(*) FROM job_application_reminders"
            ).fetchone()[0]
            self.assertEqual(count, 7)

    def test_server_worker_tick_runs_maintenance_only_for_lease_winner(self):
        server = load_server()
        maintenance_result = {
            "application_reminders_created": 0,
            "email_delivery": {
                "sent": 0, "deferred": 0, "failed": 0,
                "claimed_recovered": 0, "stale_skipped": 0,
                "suppressed": 0, "claim_lost": 0,
            },
        }
        with mock.patch.object(
            server.api_module, "acquire_notification_worker_lease",
            side_effect=[False, True],
        ) as acquire, mock.patch.object(
            server.api_module, "run_notification_maintenance_once",
            return_value=maintenance_result,
        ) as maintain:
            self.assertIsNone(server._run_notification_worker_tick("worker-a"))
            self.assertEqual(
                server._run_notification_worker_tick("worker-a"), maintenance_result
            )
        self.assertEqual(acquire.call_count, 2)
        maintain.assert_called_once_with(
            owner_token="worker-a", lease_seconds=600,
        )

    def test_production_start_script_creates_one_worker_lease_heartbeat(self):
        database_path = os.path.join(self.tmp.name, "entrypoint.db")
        env = os.environ.copy()
        env.update({
            "DATABASE_PATH": database_path,
            "DISABLE_AUTO_SEED": "1",
            "NOTIFICATION_MAINTENANCE_WORKER_ENABLED": "true",
            "EMAIL_OUTBOX_WORKER_INTERVAL_SECONDS": "15",
            "PORT": "0",
            "FLASK_DEBUG": "false",
            "PATH": f"{Path(sys.executable).parent}:{env.get('PATH', '')}",
        })
        process = subprocess.Popen(
            ["bash", "start.sh"], cwd=Path(__file__).parent, env=env,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
        )
        lease_rows = []
        try:
            deadline = time.monotonic() + 15
            while time.monotonic() < deadline:
                if os.path.exists(database_path):
                    try:
                        with sqlite3.connect(database_path) as db:
                            lease_rows = db.execute(
                                """SELECT worker_name,heartbeat_at,lease_expires_at
                                   FROM notification_worker_leases"""
                            ).fetchall()
                    except sqlite3.OperationalError:
                        lease_rows = []
                    if lease_rows:
                        break
                time.sleep(0.25)
        finally:
            process.terminate()
            try:
                stdout, stderr = process.communicate(timeout=10)
            except subprocess.TimeoutExpired:
                process.kill()
                stdout, stderr = process.communicate(timeout=10)
        self.assertEqual(len(lease_rows), 1, stderr)
        self.assertEqual(lease_rows[0][0], "notification-maintenance")
        self.assertGreater(lease_rows[0][2], lease_rows[0][1])
        self.assertEqual(stderr.count('"event": "notification_worker_started"'), 1)

    def test_applicant_view_cursor_does_not_hide_same_second_new_application(self):
        with self.api.get_db() as db:
            db.execute(
                "UPDATE notification_system_state SET application_reminders_enabled_at='2026-08-20 00:00:00' WHERE id=1"
            )
            db.execute(
                """INSERT INTO applications
                   (id,job_id,worker_id,cover_message,status,created_at)
                   VALUES(20,1,1,'Seen first','pending',datetime('now'))"""
            )
            db.commit()
        status, _ = self.request("GET", "/jobs/1/applications", token="owner")
        self.assertEqual(status, 200)
        with self.api.get_db() as db:
            view = db.execute(
                "SELECT last_viewed_at,last_seen_application_id FROM job_application_views WHERE job_id=1"
            ).fetchone()
            self.assertEqual(view["last_seen_application_id"], 20)
            db.execute(
                """INSERT INTO applications
                   (id,job_id,worker_id,cover_message,status,created_at)
                   VALUES(21,1,3,'Same-second arrival','pending',?)""",
                [view["last_viewed_at"]],
            )
            db.commit()
            reminder_time = datetime.fromisoformat(view["last_viewed_at"]).replace(
                tzinfo=timezone.utc
            ) + timedelta(hours=25)
            created = self.api.generate_application_reminders(db, now=reminder_time)
            self.assertEqual(created, 1)
            reminder = db.execute(
                "SELECT application_count FROM job_application_reminders"
            ).fetchone()
            self.assertEqual(reminder["application_count"], 1)

    def test_unseen_application_gets_exactly_one_24h_and_one_72h_reminder(self):
        with self.api.get_db() as db:
            db.execute(
                "UPDATE notification_system_state SET application_reminders_enabled_at='2026-08-20 00:00:00' WHERE id=1"
            )
            db.execute(
                """INSERT INTO applications
                   (id,job_id,worker_id,cover_message,status,created_at)
                   VALUES(30,1,1,'Awaiting review','pending','2026-08-24 10:00:00')"""
            )
            db.commit()
            self.assertEqual(
                self.api.generate_application_reminders(
                    db, now=datetime(2026, 8, 25, 12, 0, tzinfo=timezone.utc)
                ),
                1,
            )
            self.assertEqual(
                self.api.generate_application_reminders(
                    db, now=datetime(2026, 8, 27, 12, 0, tzinfo=timezone.utc)
                ),
                1,
            )
            self.assertEqual(
                self.api.generate_application_reminders(
                    db, now=datetime(2026, 8, 28, 12, 0, tzinfo=timezone.utc)
                ),
                0,
            )
            reminders = db.execute(
                "SELECT reminder_kind FROM job_application_reminders ORDER BY reminder_kind"
            ).fetchall()
            self.assertEqual([row["reminder_kind"] for row in reminders], ["24h", "72h"])
            outbox = db.execute(
                """SELECT notification_type,title,message
                   FROM transactional_email_outbox ORDER BY id"""
            ).fetchall()
            self.assertEqual(
                [row["notification_type"] for row in outbox],
                ["application_reminder_24h", "application_reminder_72h"],
            )
            for row in outbox:
                self.assertEqual(row["title"], "Applications are waiting for Review a document")
                self.assertEqual(
                    row["message"],
                    "People are waiting for your response. Review the applications and choose whether to hire, decline, or keep the job open.",
                )

    def test_legacy_outbox_migrates_without_sending_or_resetting_rollout_cutoff(self):
        with self.api.get_db() as db:
            db.execute("DROP TABLE transactional_email_outbox")
            db.execute("""CREATE TABLE transactional_email_outbox (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL REFERENCES users(id),
                notification_id INTEGER REFERENCES notifications(id),
                email_to TEXT NOT NULL,
                notification_type TEXT NOT NULL,
                title TEXT NOT NULL,
                message TEXT NOT NULL,
                link TEXT NOT NULL DEFAULT '',
                dedupe_context TEXT NOT NULL,
                dedupe_key TEXT NOT NULL UNIQUE,
                state TEXT NOT NULL DEFAULT 'pending'
                    CHECK(state IN ('pending','sending','sent','failed')),
                attempts INTEGER NOT NULL DEFAULT 0,
                claimed_at TEXT,
                last_error TEXT,
                created_at TEXT NOT NULL DEFAULT (datetime('now')),
                sent_at TEXT
            )""")
            db.execute(
                """INSERT INTO transactional_email_outbox
                   (user_id,email_to,notification_type,title,message,link,dedupe_context,dedupe_key)
                   VALUES(2,'legacy@example.com','new_application','Legacy','Old','/jobs/1/applications','legacy','legacy-key')"""
            )
            db.execute(
                "UPDATE notification_system_state SET application_reminders_enabled_at='2026-08-24 12:00:00' WHERE id=1"
            )
            db.commit()
        self.api.init_db()
        with self.api.get_db() as db:
            columns = {row[1] for row in db.execute("PRAGMA table_info(transactional_email_outbox)")}
            self.assertTrue({
                "next_attempt_at", "expires_at", "provider_email_id", "delivery_status",
                "delivered_at", "bounced_at", "complained_at",
            }.issubset(columns))
            legacy = db.execute(
                """SELECT state,attempts,expires_at,delivery_status,last_error,
                          email_to,title,message,link
                   FROM transactional_email_outbox WHERE dedupe_key='legacy-key'"""
            ).fetchone()
            self.assertEqual(tuple(legacy)[:5], (
                "failed", 0, None, "suppressed", "legacy row without validity window",
            ))
            self.assertEqual(tuple(legacy)[5:], ("", "", "", ""))
            cutoff = db.execute(
                "SELECT application_reminders_enabled_at FROM notification_system_state WHERE id=1"
            ).fetchone()[0]
            self.assertEqual(cutoff, "2026-08-24 12:00:00")
            with mock.patch.object(
                self.api, "send_transactional_notification_email",
                side_effect=AssertionError("legacy email must not send"),
            ):
                result = self.api.flush_transactional_notification_emails(
                    db, now=datetime(2026, 8, 25, 12, 0, tzinfo=timezone.utc)
                )
            self.assertEqual(result["sent"], 0)
            self.assertEqual(result["stale_skipped"], 0)


if __name__ == "__main__":
    unittest.main()
