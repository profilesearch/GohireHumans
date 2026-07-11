import contextlib
import hashlib
import io
import json
import os
import sqlite3
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone

from test_deep_audit_regressions import load_api_core, parse_cgi_output


class OrderDeadlineReminderTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        os.environ["DATABASE_PATH"] = os.path.join(self.tmp.name, "deadline.db")
        os.environ["DISABLE_AUTO_SEED"] = "1"
        self.api = load_api_core()
        self.api._db_path_resolved = None
        self.api._seeded = False
        self.api.init_db()
        self.read_api_key = "ghh_deadline_read_only_probe"
        self._seed()

    def tearDown(self):
        self.tmp.cleanup()
        os.environ.pop("DATABASE_PATH", None)
        os.environ.pop("DISABLE_AUTO_SEED", None)

    def request(self, method, path, token="", payload=None, query="", api_key=""):
        for cached in ("body_cache", "raw_body"):
            if hasattr(self.api._request_ctx, cached):
                delattr(self.api._request_ctx, cached)
        raw = json.dumps(payload or {})
        ctx = self.api._request_ctx
        ctx.request_method = method
        ctx.path_info = path
        ctx.query_string = query
        ctx.http_authorization = f"Bearer {token}" if token else ""
        ctx.http_x_api_key = api_key
        ctx.stdin_data = raw
        ctx.content_type = "application/json"
        ctx.content_length = str(len(raw.encode()))
        ctx.remote_addr = "127.0.0.1"
        with contextlib.redirect_stdout(io.StringIO()) as out:
            self.api.handle_request()
        return parse_cgi_output(out.getvalue())

    def _seed(self):
        with self.api.get_db() as db:
            db.execute("INSERT INTO users(id,email,name,password_hash) VALUES(1,'worker@x','Worker','x')")
            db.execute("INSERT INTO users(id,email,name,password_hash) VALUES(2,'employer@x','Employer','x')")
            db.execute("INSERT INTO users(id,email,name,password_hash,is_admin) VALUES(3,'admin@x','Admin','x',1)")
            for user_id, token in ((1, "worker"), (2, "employer"), (3, "admin")):
                db.execute(
                    "INSERT INTO sessions(user_id,token,expires_at) VALUES(?,?,datetime('now','+1 day'))",
                    [user_id, token],
                )
            db.execute(
                """INSERT INTO api_keys
                   (id,user_id,key_hash,key_prefix,name,scopes,is_active,total_requests)
                   VALUES(1,1,?,'ghh_dead','deadline-read','[\"read\"]',1,0)""",
                [hashlib.sha256(self.read_api_key.encode()).hexdigest()],
            )
            db.execute("INSERT INTO worker_profiles(user_id) VALUES(1)")
            db.execute(
                "INSERT INTO employer_profiles(user_id,payment_method_id,stripe_customer_id) VALUES(2,'pm_test','cus_test')"
            )
            db.execute(
                """INSERT INTO services
                   (id,worker_id,title,description,category,pricing_type,price,delivery_time_days,status)
                   VALUES(1,1,'Three-day QA','QA delivery','testing','fixed',25,3,'active')"""
            )
            db.execute(
                "INSERT INTO jobs(id,employer_id,title,description,category,budget_type,budget_amount,status) "
                "VALUES(1,2,'Fixed','Scoped','testing','fixed',25,'hired')"
            )
            db.execute(
                "INSERT INTO jobs(id,employer_id,title,description,category,budget_type,budget_amount,status) "
                "VALUES(2,2,'Hourly','Scoped','testing','hourly',25,'hired')"
            )
            db.execute(
                "INSERT INTO orders(id,type,job_id,worker_id,employer_id,status,total_amount) "
                "VALUES(10,'job_hire',1,1,2,'in_progress',25)"
            )
            db.execute(
                "INSERT INTO milestones(order_id,title,description,amount,sequence,status) "
                "VALUES(10,'Delivery','',25,1,'in_progress')"
            )
            db.execute(
                "INSERT INTO orders(id,type,job_id,worker_id,employer_id,status,total_amount) "
                "VALUES(11,'job_hire',2,1,2,'in_progress',25)"
            )
            db.execute(
                "INSERT INTO hourly_contracts(order_id,hourly_rate,weekly_hour_cap,status) "
                "VALUES(11,25,40,'active')"
            )
            db.commit()

    def test_migration_adds_deadline_fields_and_exact_reminder_table(self):
        with self.api.get_db() as db:
            order_columns = {
                row["name"]: row for row in db.execute("PRAGMA table_xinfo('orders')").fetchall()
            }
            self.assertTrue(
                {"deadline_at", "submitted_at", "revision_requested_at"}.issubset(order_columns)
            )
            reminder_sql = db.execute(
                "SELECT sql FROM sqlite_master WHERE type='table' AND name='order_reminders'"
            ).fetchone()
            self.assertIsNotNone(reminder_sql)
            reminder_columns = {
                row["name"]: row for row in db.execute("PRAGMA table_xinfo('order_reminders')").fetchall()
            }
            self.assertEqual(
                set(reminder_columns),
                {
                    "id", "order_id", "recipient_user_id", "reminder_kind",
                    "deadline_at", "notification_id", "created_at",
                },
            )
            indexes = {
                row["name"]: row for row in db.execute("PRAGMA index_list('order_reminders')").fetchall()
            }
            self.assertIn("idx_order_reminders_exact_once", indexes)
            self.assertEqual(indexes["idx_order_reminders_exact_once"]["unique"], 1)

    def test_legacy_orders_migrate_additively_without_losing_rows(self):
        current_path = os.environ["DATABASE_PATH"]
        with tempfile.TemporaryDirectory() as legacy_dir:
            legacy_path = os.path.join(legacy_dir, "legacy.db")
            conn = sqlite3.connect(legacy_path)
            conn.executescript(
                """
                CREATE TABLE users (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    email TEXT UNIQUE NOT NULL,
                    password_hash TEXT NOT NULL DEFAULT '',
                    name TEXT NOT NULL DEFAULT '',
                    avatar_url TEXT,
                    google_sub TEXT,
                    referral_code TEXT UNIQUE,
                    referred_by INTEGER REFERENCES users(id),
                    is_admin INTEGER DEFAULT 0,
                    is_active INTEGER DEFAULT 1,
                    is_banned INTEGER DEFAULT 0,
                    created_at TEXT DEFAULT (datetime('now')),
                    updated_at TEXT DEFAULT (datetime('now'))
                );
                INSERT INTO users (id,email,password_hash,name)
                VALUES (1,'legacy@example.com','x','Legacy User');

                CREATE TABLE orders (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    type TEXT NOT NULL CHECK(type IN ('service_order','job_hire')),
                    service_id INTEGER REFERENCES services(id),
                    job_id INTEGER REFERENCES jobs(id),
                    worker_id INTEGER NOT NULL REFERENCES users(id),
                    employer_id INTEGER NOT NULL REFERENCES users(id),
                    status TEXT DEFAULT 'pending' CHECK(status IN ('pending','in_progress','submitted','revision_requested','completed','disputed','canceled')),
                    total_amount REAL NOT NULL,
                    total_amount_cents INTEGER,
                    currency TEXT NOT NULL DEFAULT 'usd',
                    creation_idempotency_key TEXT,
                    creation_request_fingerprint TEXT,
                    worker_notes TEXT DEFAULT '',
                    employer_notes TEXT DEFAULT '',
                    created_at TEXT DEFAULT (datetime('now')),
                    updated_at TEXT DEFAULT (datetime('now')),
                    completed_at TEXT
                );
                INSERT INTO orders
                    (id,type,worker_id,employer_id,status,total_amount,creation_idempotency_key)
                VALUES(99,'job_hire',1,2,'in_progress',25,'legacy-order-key-0001');
                """
            )
            conn.commit()
            conn.close()
            os.environ["DATABASE_PATH"] = legacy_path
            try:
                migrated = load_api_core()
                migrated._db_path_resolved = None
                migrated.init_db()
                with migrated.get_db() as db:
                    row = db.execute(
                        "SELECT id,status,deadline_at,submitted_at,revision_requested_at FROM orders WHERE id=99"
                    ).fetchone()
                    self.assertEqual(tuple(row), (99, "in_progress", None, None, None))
                    user_columns = {
                        column["name"] for column in db.execute("PRAGMA table_xinfo('users')").fetchall()
                    }
                    self.assertIn("is_suspended", user_columns)
                    legacy_user = db.execute(
                        "SELECT email,is_suspended FROM users WHERE id=1"
                    ).fetchone()
                    self.assertEqual(tuple(legacy_user), ("legacy@example.com", 0))
                    self.assertIsNotNone(
                        db.execute(
                            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='order_reminders'"
                        ).fetchone()
                    )
            finally:
                os.environ["DATABASE_PATH"] = current_path

    def test_poisoned_reminder_ledger_schema_fails_startup_closed(self):
        current_path = os.environ["DATABASE_PATH"]
        with tempfile.TemporaryDirectory() as poisoned_dir:
            poisoned_path = os.path.join(poisoned_dir, "poisoned.db")
            conn = sqlite3.connect(poisoned_path)
            conn.executescript(
                """
                CREATE TABLE order_reminders (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    order_id INTEGER NOT NULL REFERENCES orders(id),
                    recipient_user_id INTEGER NOT NULL REFERENCES users(id),
                    reminder_kind TEXT NOT NULL,
                    deadline_at TEXT NOT NULL,
                    notification_id INTEGER UNIQUE REFERENCES notifications(id),
                    created_at TEXT NOT NULL DEFAULT (datetime('now'))
                );
                """
            )
            conn.close()
            os.environ["DATABASE_PATH"] = poisoned_path
            try:
                poisoned = load_api_core()
                poisoned._db_path_resolved = None
                with self.assertRaisesRegex(RuntimeError, "order_reminders exact table schema"):
                    poisoned.init_db()
            finally:
                os.environ["DATABASE_PATH"] = current_path

    def test_deadline_parser_requires_aware_future_time_and_normalizes_to_utc(self):
        anchor = datetime(2026, 7, 11, 12, 0, tzinfo=timezone.utc)
        self.assertEqual(
            self.api.validated_order_deadline("2026-07-13T08:30:00-04:00", now=anchor),
            "2026-07-13T12:30:00Z",
        )
        for invalid in (None, "", "2026-07-13T12:30:00", "not-a-date"):
            with self.subTest(invalid=invalid):
                with self.assertRaises(ValueError):
                    self.api.validated_order_deadline(invalid, now=anchor)
        with self.assertRaises(ValueError):
            self.api.validated_order_deadline("2026-07-11T12:30:00Z", now=anchor)
        with self.assertRaises(ValueError):
            self.api.validated_order_deadline("2028-07-11T12:30:00Z", now=anchor)

    def test_service_delivery_days_produce_canonical_utc_deadline(self):
        anchor = datetime(2026, 7, 11, 12, 0, tzinfo=timezone.utc)
        self.assertEqual(
            self.api.service_order_deadline(3, now=anchor),
            "2026-07-14T12:00:00Z",
        )
        for invalid in (None, 0, -1, 366, "three"):
            with self.subTest(invalid=invalid):
                with self.assertRaises(ValueError):
                    self.api.service_order_deadline(invalid, now=anchor)

    def test_fixed_service_checkout_persists_published_delivery_deadline(self):
        previous_production = self.api.PRODUCTION_MODE
        previous_stripe_key = self.api.STRIPE_SECRET_KEY
        self.api.PRODUCTION_MODE = False
        self.api.STRIPE_SECRET_KEY = ""
        before = datetime.now(timezone.utc).replace(microsecond=0)
        try:
            status, body = self.request(
                "POST",
                "/services/1/order",
                "employer",
                {"idempotency_key": "service-deadline-checkout-0001", "notes": "QA pass"},
            )
        finally:
            self.api.PRODUCTION_MODE = previous_production
            self.api.STRIPE_SECRET_KEY = previous_stripe_key
        after = datetime.now(timezone.utc).replace(microsecond=0)

        self.assertEqual(status, 201, body)
        deadline = datetime.fromisoformat(body["deadline_at"].replace("Z", "+00:00"))
        self.assertGreaterEqual(deadline, before + timedelta(days=3))
        self.assertLessEqual(deadline, after + timedelta(days=3))
        with self.api.get_db() as db:
            row = db.execute(
                "SELECT status,deadline_at FROM orders WHERE id=?", [body["id"]]
            ).fetchone()
            self.assertEqual(row["status"], "in_progress")
            self.assertEqual(row["deadline_at"], body["deadline_at"])

    def test_legacy_fixed_service_without_delivery_promise_gets_seven_day_deadline(self):
        with self.api.get_db() as db:
            db.execute("UPDATE services SET delivery_time_days=NULL WHERE id=1")
            db.commit()
        previous_production = self.api.PRODUCTION_MODE
        previous_stripe_key = self.api.STRIPE_SECRET_KEY
        self.api.PRODUCTION_MODE = False
        self.api.STRIPE_SECRET_KEY = ""
        before = datetime.now(timezone.utc).replace(microsecond=0)
        try:
            status, body = self.request(
                "POST",
                "/services/1/order",
                "employer",
                {"idempotency_key": "legacy-service-deadline-0001", "notes": "Legacy QA"},
            )
        finally:
            self.api.PRODUCTION_MODE = previous_production
            self.api.STRIPE_SECRET_KEY = previous_stripe_key
        after = datetime.now(timezone.utc).replace(microsecond=0)
        self.assertEqual(status, 201, body)
        deadline = datetime.fromisoformat(body["deadline_at"].replace("Z", "+00:00"))
        self.assertGreaterEqual(deadline, before + timedelta(days=7))
        self.assertLessEqual(deadline, after + timedelta(days=7))

    def test_employer_can_set_fixed_order_deadline_once_but_worker_and_hourly_cannot(self):
        future = (datetime.now(timezone.utc) + timedelta(days=7)).replace(microsecond=0)
        deadline = future.isoformat().replace("+00:00", "Z")
        status, body = self.request(
            "PUT", "/orders/10/deadline", "worker", {"deadline_at": deadline}
        )
        self.assertEqual(status, 403, body)

        status, body = self.request(
            "PUT", "/orders/10/deadline", "employer", {"deadline_at": deadline}
        )
        self.assertEqual(status, 200, body)
        self.assertEqual(body["deadline_at"], deadline)

        later = (future + timedelta(days=1)).isoformat().replace("+00:00", "Z")
        status, body = self.request(
            "PUT", "/orders/10/deadline", "employer", {"deadline_at": later}
        )
        self.assertEqual(status, 409, body)
        with self.api.get_db() as db:
            self.assertEqual(
                db.execute("SELECT deadline_at FROM orders WHERE id=10").fetchone()[0],
                deadline,
            )

        status, body = self.request(
            "PUT", "/orders/11/deadline", "employer", {"deadline_at": deadline}
        )
        self.assertEqual(status, 409, body)
        self.assertIn("fixed-price", body["error"])

    def test_submit_and_revision_persist_explicit_lifecycle_timestamps_and_new_deadline(self):
        initial_deadline = (datetime.now(timezone.utc) + timedelta(days=7)).replace(microsecond=0)
        initial_text = initial_deadline.isoformat().replace("+00:00", "Z")
        self.assertEqual(
            self.request("PUT", "/orders/10/deadline", "employer", {"deadline_at": initial_text})[0],
            200,
        )
        status, body = self.request(
            "POST", "/orders/10/submit", "worker", {"notes": "Initial delivery"}
        )
        self.assertEqual(status, 200, body)

        with self.api.get_db() as db:
            submitted = db.execute(
                "SELECT status,deadline_at,submitted_at,revision_requested_at FROM orders WHERE id=10"
            ).fetchone()
            self.assertEqual(submitted["status"], "submitted")
            self.assertEqual(submitted["deadline_at"], initial_text)
            self.assertRegex(submitted["submitted_at"], r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$")
            self.assertIsNone(submitted["revision_requested_at"])
            first_submitted_at = submitted["submitted_at"]

        revision_deadline = (datetime.now(timezone.utc) + timedelta(days=3)).replace(microsecond=0)
        revision_text = revision_deadline.isoformat().replace("+00:00", "Z")
        status, body = self.request(
            "POST",
            "/orders/10/request-revision",
            "employer",
            {"notes": "Please retest mobile", "deadline_at": revision_text},
        )
        self.assertEqual(status, 200, body)
        self.assertEqual(body["deadline_at"], revision_text)

        with self.api.get_db() as db:
            revised = db.execute(
                "SELECT status,deadline_at,submitted_at,revision_requested_at FROM orders WHERE id=10"
            ).fetchone()
            self.assertEqual(revised["status"], "revision_requested")
            self.assertEqual(revised["deadline_at"], revision_text)
            self.assertEqual(revised["submitted_at"], first_submitted_at)
            self.assertRegex(revised["revision_requested_at"], r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$")

    def test_revision_without_explicit_deadline_gets_bounded_three_day_default(self):
        self.assertEqual(
            self.request("POST", "/orders/10/submit", "worker", {"notes": "Delivery"})[0],
            200,
        )
        before = datetime.now(timezone.utc)
        status, body = self.request(
            "POST", "/orders/10/request-revision", "employer", {"notes": "One correction"}
        )
        after = datetime.now(timezone.utc)
        self.assertEqual(status, 200, body)
        deadline = datetime.fromisoformat(body["deadline_at"].replace("Z", "+00:00"))
        self.assertGreaterEqual(deadline, (before + timedelta(days=3)).replace(microsecond=0))
        self.assertLessEqual(deadline, (after + timedelta(days=3)).replace(microsecond=0))

    def test_hourly_revision_route_rejects_explicit_deadline_but_preserves_legacy_revision(self):
        with self.api.get_db() as db:
            db.execute("UPDATE orders SET status='submitted' WHERE id=11")
            baseline = tuple(
                db.execute(
                    """SELECT status,deadline_at,revision_requested_at,employer_notes
                       FROM orders WHERE id=11"""
                ).fetchone()
            )
            db.commit()
        future = (datetime.now(timezone.utc) + timedelta(days=3)).strftime("%Y-%m-%dT%H:%M:%SZ")

        status, body = self.request(
            "POST", "/orders/11/request-revision", "employer",
            {"notes": "Hourly revision", "deadline_at": future},
        )
        self.assertEqual(status, 409, body)
        self.assertIn("fixed-price", body["error"])
        with self.api.get_db() as db:
            row = db.execute(
                """SELECT status,deadline_at,revision_requested_at,employer_notes
                   FROM orders WHERE id=11"""
            ).fetchone()
            self.assertEqual(tuple(row), baseline)

        status, body = self.request(
            "POST", "/orders/11/request-revision", "employer",
            {"notes": "Hourly revision"},
        )
        self.assertEqual(status, 200, body)
        self.assertIsNone(body["deadline_at"])
        with self.api.get_db() as db:
            row = db.execute(
                """SELECT status,deadline_at,revision_requested_at,employer_notes
                   FROM orders WHERE id=11"""
            ).fetchone()
            self.assertEqual(row["status"], "revision_requested")
            self.assertIsNone(row["deadline_at"])
            self.assertIsNotNone(row["revision_requested_at"])
            self.assertEqual(row["employer_notes"], "Hourly revision")
            notification = db.execute(
                """SELECT n.message FROM notifications n
                   JOIN orders o ON o.worker_id=n.user_id
                   WHERE o.id=11 AND n.type='revision_requested'
                   ORDER BY n.id DESC LIMIT 1"""
            ).fetchone()
            self.assertIsNotNone(notification)
            self.assertNotIn("due None", notification["message"])

    def test_notification_feed_materializes_only_the_current_exact_once_reminder(self):
        now = datetime.now(timezone.utc).replace(microsecond=0)
        due_36h = (now + timedelta(hours=36)).strftime("%Y-%m-%dT%H:%M:%SZ")
        with self.api.get_db() as db:
            db.execute("UPDATE orders SET deadline_at=? WHERE id=10", [due_36h])
            db.commit()

        status, body = self.request("GET", "/notifications", "worker")
        self.assertEqual(status, 200, body)
        self.assertEqual([n["type"] for n in body["notifications"]], ["order_due_48h"])
        status, body = self.request("GET", "/notifications", "worker")
        self.assertEqual(status, 200, body)
        self.assertEqual([n["type"] for n in body["notifications"]], ["order_due_48h"])
        self.assertEqual(self.request("GET", "/notifications", "employer")[1]["notifications"], [])

        due_20h = (now + timedelta(hours=20)).strftime("%Y-%m-%dT%H:%M:%SZ")
        with self.api.get_db() as db:
            db.execute("UPDATE orders SET deadline_at=? WHERE id=10", [due_20h])
            db.commit()
        status, body = self.request("GET", "/notifications", "worker")
        self.assertEqual(status, 200, body)
        self.assertEqual(
            {n["type"] for n in body["notifications"]},
            {"order_due_48h", "order_due_24h"},
        )

        overdue = (now - timedelta(hours=2)).strftime("%Y-%m-%dT%H:%M:%SZ")
        with self.api.get_db() as db:
            db.execute("UPDATE orders SET deadline_at=? WHERE id=10", [overdue])
            db.execute("UPDATE orders SET deadline_at=? WHERE id=11", [overdue])
            db.commit()
        self.request("GET", "/notifications", "worker")
        self.request("GET", "/notifications", "employer")
        with self.api.get_db() as db:
            reminders = db.execute(
                "SELECT order_id,recipient_user_id,reminder_kind FROM order_reminders ORDER BY id"
            ).fetchall()
            self.assertEqual(
                [tuple(r) for r in reminders],
                [(10, 1, "due_48h"), (10, 1, "due_24h"), (10, 1, "overdue"), (10, 2, "overdue")],
            )
            self.assertEqual(db.execute("SELECT COUNT(*) FROM transactional_email_outbox").fetchone()[0], 0)
            self.assertEqual(db.execute("SELECT COUNT(*) FROM order_reminders WHERE order_id=11").fetchone()[0], 0)

    def test_read_scoped_api_key_does_not_materialize_reminder_domain_rows(self):
        deadline = (datetime.now(timezone.utc) + timedelta(hours=20)).strftime("%Y-%m-%dT%H:%M:%SZ")
        with self.api.get_db() as db:
            db.execute("UPDATE orders SET deadline_at=? WHERE id=10", [deadline])
            db.commit()

        status, body = self.request(
            "GET", "/notifications", api_key=self.read_api_key
        )
        self.assertEqual(status, 200, body)
        with self.api.get_db() as db:
            self.assertEqual(db.execute("SELECT COUNT(*) FROM order_reminders").fetchone()[0], 0)
            self.assertEqual(
                db.execute("SELECT COUNT(*) FROM notifications WHERE type LIKE 'order_due_%'").fetchone()[0],
                0,
            )

        self.assertEqual(self.request("GET", "/notifications", "worker")[0], 200)
        with self.api.get_db() as db:
            self.assertEqual(db.execute("SELECT COUNT(*) FROM order_reminders").fetchone()[0], 1)

    def test_submitted_and_completed_orders_never_generate_due_reminders(self):
        overdue = (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%dT%H:%M:%SZ")
        with self.api.get_db() as db:
            db.execute("UPDATE orders SET status='submitted',deadline_at=? WHERE id=10", [overdue])
            db.commit()
        self.request("GET", "/notifications", "worker")
        self.request("GET", "/notifications", "employer")
        with self.api.get_db() as db:
            self.assertEqual(db.execute("SELECT COUNT(*) FROM order_reminders").fetchone()[0], 0)

    def test_concurrent_reminder_generation_claims_one_notification(self):
        anchor = datetime.now(timezone.utc).replace(microsecond=0)
        deadline = (anchor + timedelta(hours=20)).strftime("%Y-%m-%dT%H:%M:%SZ")
        with self.api.get_db() as db:
            db.execute("UPDATE orders SET deadline_at=? WHERE id=10", [deadline])
            db.commit()

        def generate():
            db = self.api.get_db()
            try:
                return self.api.generate_order_reminders(db, 1, now=anchor)
            finally:
                db.close()

        with ThreadPoolExecutor(max_workers=2) as pool:
            created = list(pool.map(lambda _: generate(), range(2)))
        self.assertEqual(sorted(created), [0, 1])
        with self.api.get_db() as db:
            self.assertEqual(db.execute("SELECT COUNT(*) FROM order_reminders").fetchone()[0], 1)
            self.assertEqual(db.execute("SELECT COUNT(*) FROM notifications WHERE type='order_due_24h'").fetchone()[0], 1)

    def test_one_deadline_progresses_through_each_reminder_threshold_exactly_once(self):
        anchor = datetime.now(timezone.utc).replace(microsecond=0)
        deadline = (anchor + timedelta(hours=36)).strftime("%Y-%m-%dT%H:%M:%SZ")
        with self.api.get_db() as db:
            db.execute("UPDATE orders SET deadline_at=? WHERE id=10", [deadline])
            db.commit()

        for now, user_id in [
            (anchor, 1),
            (anchor + timedelta(hours=20), 1),
            (anchor + timedelta(hours=40), 1),
            (anchor + timedelta(hours=40), 2),
            (anchor + timedelta(hours=40), 1),
        ]:
            with self.api.get_db() as db:
                self.api.generate_order_reminders(db, user_id, now=now)

        with self.api.get_db() as db:
            rows = db.execute(
                "SELECT recipient_user_id,reminder_kind FROM order_reminders ORDER BY recipient_user_id,reminder_kind"
            ).fetchall()
            self.assertEqual(
                [tuple(row) for row in rows],
                [
                    (1, "due_24h"),
                    (1, "due_48h"),
                    (1, "overdue"),
                    (2, "overdue"),
                ],
            )

    def test_participant_and_admin_order_lists_expose_and_filter_overdue_fixed_orders(self):
        overdue = (datetime.now(timezone.utc) - timedelta(hours=2)).strftime("%Y-%m-%dT%H:%M:%SZ")
        with self.api.get_db() as db:
            db.execute("UPDATE orders SET deadline_at=? WHERE id IN (10,11)", [overdue])
            db.commit()

        for token in ("worker", "employer"):
            status, body = self.request("GET", "/orders", token, query="overdue=true")
            self.assertEqual(status, 200, body)
            self.assertEqual(body["total"], 1)
            self.assertEqual(body["orders"][0]["id"], 10)
            self.assertEqual(body["orders"][0]["is_overdue"], 1)
            self.assertEqual(body["orders"][0]["deadline_at"], overdue)

        status, body = self.request("GET", "/admin/orders", "admin", query="overdue=true")
        self.assertEqual(status, 200, body)
        self.assertEqual(body["total"], 1)
        self.assertEqual(body["orders"][0]["id"], 10)
        self.assertEqual(body["orders"][0]["is_overdue"], 1)

        with self.api.get_db() as db:
            db.execute("UPDATE orders SET status='submitted' WHERE id=10")
            db.commit()
        self.assertEqual(
            self.request("GET", "/admin/orders", "admin", query="overdue=true")[1]["total"],
            0,
        )


if __name__ == "__main__":
    unittest.main()
