import contextlib
import decimal
import importlib.util
import io
import json
import os
import pathlib
import sqlite3
import tempfile
import threading
import time
import unittest
from concurrent.futures import ThreadPoolExecutor
from unittest import mock

from test_deep_audit_regressions import load_api_core, parse_cgi_output


def exact_transfer(transfer_id, kwargs):
    return type("TransferResult", (), {
        "id": transfer_id,
        "amount": kwargs["amount"],
        "currency": kwargs["currency"],
        "destination": kwargs["destination"],
        "metadata": kwargs["metadata"],
    })()


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
        self._payment_sequence = 0

        def create_payment_intent(**kwargs):
            self._payment_sequence += 1
            return type(
                "PaymentIntentResult",
                (),
                {
                    "id": f"pi_mock_hire_{self._payment_sequence}",
                    "status": "succeeded",
                    "amount": kwargs["amount"],
                    "amount_received": kwargs["amount"],
                    "currency": kwargs["currency"],
                    "metadata": kwargs["metadata"],
                },
            )()

        self.payment_create = mock.Mock(side_effect=create_payment_intent)
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
            db.execute("INSERT INTO services (id,worker_id,title,description,category,pricing_type,status) VALUES (1,1,'Custom QA','Scoped QA','testing','custom','active')")
            db.execute("INSERT INTO sessions (user_id,token,expires_at) VALUES (1,'tok-worker',datetime('now','+1 day'))")
            db.execute("INSERT INTO sessions (user_id,token,expires_at) VALUES (2,'tok-employer',datetime('now','+1 day'))")
            db.execute("INSERT INTO jobs (id,employer_id,title,description,category,budget_type,budget_amount,status) VALUES (1,2,'Fixed QA','Test flows','testing','fixed',25,'open')")
            db.execute("INSERT INTO jobs (id,employer_id,title,description,category,budget_type,budget_amount,status) VALUES (2,2,'Other job','Other scope','testing','fixed',10,'open')")
            db.execute("INSERT INTO jobs (id,employer_id,title,description,category,budget_type,budget_amount,status) VALUES (3,2,'Bad cents','Invalid precision','testing','fixed',25.555,'open')")
            db.execute("INSERT INTO jobs (id,employer_id,title,description,category,budget_type,budget_amount,status) VALUES (4,2,'Hourly QA','Desc','testing','hourly',25,'open')")
            db.execute("INSERT INTO jobs (id,employer_id,title,description,category,budget_type,budget_amount,status) VALUES (5,2,'Awkward fixed','Desc','testing','fixed',25.55,'open')")
            db.execute("INSERT INTO jobs (id,employer_id,title,description,category,budget_type,budget_amount,status) VALUES (6,2,'Awkward hourly','Desc','testing','hourly',25.55,'open')")
            db.execute("INSERT INTO jobs (id,employer_id,title,description,category,budget_type,budget_amount,status) VALUES (7,2,'Tiny fixed','Desc','testing','fixed',0.01,'open')")
            db.execute("INSERT INTO applications (id,job_id,worker_id,status) VALUES (14,1,1,'pending')")
            db.execute("INSERT INTO applications (id,job_id,worker_id,status) VALUES (15,2,1,'pending')")
            db.execute("INSERT INTO applications (id,job_id,worker_id,status) VALUES (16,1,3,'shortlisted')")
            db.execute("INSERT INTO applications (id,job_id,worker_id,status) VALUES (17,3,1,'pending')")
            db.execute("INSERT INTO applications (id,job_id,worker_id,status) VALUES (18,4,1,'pending')")
            db.execute("INSERT INTO applications (id,job_id,worker_id,status) VALUES (19,5,1,'pending')")
            db.execute("INSERT INTO applications (id,job_id,worker_id,status) VALUES (20,6,1,'pending')")
            db.execute("INSERT INTO applications (id,job_id,worker_id,status) VALUES (21,7,1,'pending')")
            db.commit()
        finally:
            db.close()

    def _bind_verified_funding(self, db, order_id, milestone_id, amount, intent_id):
        charge = self.api.buyer_charge_breakdown_cents(amount)
        operation_key = f"milestone:{milestone_id}"
        fingerprint = self.api.funding_request_fingerprint(
            operation_key, 2, order_id, milestone_id, charge
        )
        attempt_id = db.execute(
            """INSERT INTO funding_attempts
               (operation_key,attempt_number,request_fingerprint,
                processor_idempotency_key,employer_id,order_id,milestone_id,
                base_amount_cents,platform_fee_cents,processing_fee_cents,
                charged_total_cents,currency,status,stripe_payment_intent_id,
                processor_status,evidence_source,processor_evidence_at,committed_at)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,'usd','committed',?,
                       'succeeded','processor_create',datetime('now'),datetime('now'))""",
            [operation_key, 1, fingerprint, f"escrow-fund:{operation_key}:attempt:1",
             2, order_id, milestone_id, charge["base_cents"],
             charge["platform_fee_cents"], charge["processing_fee_cents"],
             charge["total_cents"], intent_id],
        ).lastrowid
        db.execute(
            """UPDATE escrow_holds SET base_amount_cents=?,platform_fee_cents=?,
                      processing_fee_cents=?,charged_total_cents=?,
                      fee_policy_version='component-half-up-v1',funding_identity=?,
                      funding_attempt_id=?
               WHERE order_id=? AND milestone_id=? AND stripe_payment_intent_id=?""",
            [charge["base_cents"], charge["platform_fee_cents"],
             charge["processing_fee_cents"], charge["total_cents"], operation_key,
             attempt_id, order_id, milestone_id, intent_id],
        )

    def request(self, method, path, token="tok-employer", payload=None, raw_body=None):
        for cached in ("body_cache", "raw_body"):
            if hasattr(self.api._request_ctx, cached):
                delattr(self.api._request_ctx, cached)
        body = raw_body if raw_body is not None else json.dumps(payload or {})
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

    def seed_hourly_order(self, order_id=88, status="in_progress"):
        db = self.api.get_db()
        try:
            db.execute("UPDATE jobs SET status='hired' WHERE id=4")
            db.execute(
                "INSERT INTO orders (id,type,job_id,worker_id,employer_id,status,total_amount) VALUES (?,?,?,?,?,?,?)",
                [order_id, "job_hire", 4, 1, 2, status, 25],
            )
            db.execute(
                "INSERT INTO hourly_contracts (order_id,hourly_rate,weekly_hour_cap,current_week_escrow_amount,status,week_start_date) VALUES (?,?,?,?,?,?)",
                [order_id, 25, 40, 1000, "active", "2026-07-06"],
            )
            contract_id = db.execute("SELECT id FROM hourly_contracts WHERE order_id=?", [order_id]).fetchone()[0]
            db.execute(
                "INSERT INTO time_entries (contract_id,date,hours,description,status,week_of) VALUES (?,?,?,?,?,?)",
                [contract_id, "2026-07-09", 2, "QA", "pending", "2026-07-06"],
            )
            db.execute(
                "INSERT INTO escrow_holds (order_id,amount,status,stripe_payment_intent_id) VALUES (?,?,?,?)",
                [order_id, 1000, "held", "pi_hourly_test"],
            )
            db.commit()
        finally:
            db.close()
        return order_id

    def test_configured_database_path_is_not_abandoned_during_writer_lock(self):
        configured_path = self.api._get_db_path()
        writer = sqlite3.connect(configured_path, timeout=0.1)
        writer.execute("DROP TABLE IF EXISTS _ping")
        writer.commit()
        writer.execute("BEGIN IMMEDIATE")
        old_database_path = os.environ.get("DATABASE_PATH")
        old_volume_dir = self.api._VOLUME_DIR
        old_cwd = os.getcwd()
        fallback_dir = os.path.join(self.tmp.name, "fallback")
        os.makedirs(fallback_dir)
        try:
            os.environ["DATABASE_PATH"] = configured_path
            self.api._VOLUME_DIR = os.path.join(configured_path, "unavailable-volume")
            self.api._db_path_resolved = None
            os.chdir(fallback_dir)
            self.assertEqual(self.api._get_db_path(), configured_path)
        finally:
            os.chdir(old_cwd)
            self.api._VOLUME_DIR = old_volume_dir
            self.api._db_path_resolved = configured_path
            if old_database_path is None:
                os.environ.pop("DATABASE_PATH", None)
            else:
                os.environ["DATABASE_PATH"] = old_database_path
            writer.rollback()
            writer.close()

    def test_production_database_resolution_never_falls_back_to_ephemeral_storage(self):
        configured_path = self.api._get_db_path()
        old_database_path = os.environ.pop("DATABASE_PATH", None)
        old_volume_dir = self.api._VOLUME_DIR
        old_volume_attached = self.api._VOLUME_ATTACHED
        old_production_mode = self.api.PRODUCTION_MODE
        old_cwd = os.getcwd()
        fallback_dir = os.path.join(self.tmp.name, "ephemeral-fallback")
        os.makedirs(fallback_dir)
        try:
            self.api._VOLUME_DIR = os.path.join(configured_path, "unavailable-volume")
            self.api._VOLUME_ATTACHED = True
            self.api.PRODUCTION_MODE = True
            self.api._db_path_resolved = None
            os.chdir(fallback_dir)
            with self.assertRaisesRegex(RuntimeError, "durable database"):
                self.api._get_db_path()
        finally:
            os.chdir(old_cwd)
            self.api._VOLUME_DIR = old_volume_dir
            self.api._VOLUME_ATTACHED = old_volume_attached
            self.api.PRODUCTION_MODE = old_production_mode
            self.api._db_path_resolved = configured_path
            if old_database_path is not None:
                os.environ["DATABASE_PATH"] = old_database_path

    def test_financial_column_migration_propagates_writer_lock(self):
        columns = mock.Mock()
        columns.fetchall.return_value = [(0, "id", "INTEGER", 0, None, 1)]
        db = mock.Mock()
        db.execute.side_effect = [columns, sqlite3.OperationalError("database is locked")]

        with self.assertRaisesRegex(sqlite3.OperationalError, "database is locked"):
            self.api.ensure_column(
                db,
                "escrow_holds",
                "base_amount_cents",
                "ALTER TABLE escrow_holds ADD COLUMN base_amount_cents INTEGER",
            )

    def test_required_transaction_schema_validation_rejects_missing_column(self):
        with self.api.get_db() as db:
            db.execute("ALTER TABLE escrow_holds DROP COLUMN base_amount_cents")
            db.commit()
            with self.assertRaisesRegex(RuntimeError, "escrow_holds.base_amount_cents"):
                self.api.validate_required_transaction_schema(db)

    def test_transaction_schema_validation_rejects_malformed_same_name_indexes(self):
        with self.api.get_db() as db:
            db.execute("DROP INDEX idx_orders_creation_idempotency")
            db.execute("DROP INDEX idx_escrow_holds_funding_identity")
            db.execute("CREATE INDEX idx_orders_creation_idempotency ON orders(id)")
            db.execute("CREATE INDEX idx_escrow_holds_funding_identity ON escrow_holds(id)")
            db.commit()

        with self.assertRaisesRegex(
            RuntimeError,
            "idx_orders_creation_idempotency.*idx_escrow_holds_funding_identity",
        ):
            self.api.init_db()

    def test_transaction_schema_rejects_poisoned_collation_and_desc_index(self):
        with self.api.get_db() as db:
            db.execute("DROP INDEX idx_funding_attempts_operation_attempt")
            db.execute(
                """CREATE UNIQUE INDEX idx_funding_attempts_operation_attempt
                   ON funding_attempts(operation_key COLLATE NOCASE DESC, attempt_number)"""
            )
            db.commit()
            self.assertFalse(
                self.api._required_transaction_index_is_valid(
                    db, "idx_funding_attempts_operation_attempt"
                )
            )
        with self.assertRaisesRegex(
            RuntimeError, "idx_funding_attempts_operation_attempt"
        ):
            self.api.init_db()

    def test_required_migration_operational_error_is_not_suppressed(self):
        real = self.api.get_db()

        class FailingMigrationConnection:
            def __init__(self, wrapped):
                self.wrapped = wrapped

            @property
            def in_transaction(self):
                return self.wrapped.in_transaction

            def executescript(self, sql):
                return self.wrapped.executescript(sql)

            def execute(self, sql, parameters=()):
                if "idx_services_provider_type" in sql:
                    raise sqlite3.OperationalError("database is locked")
                return self.wrapped.execute(sql, parameters)

            def commit(self):
                return self.wrapped.commit()

            def rollback(self):
                return self.wrapped.rollback()

        try:
            with self.assertRaisesRegex(sqlite3.OperationalError, "database is locked"):
                self.api._init_db_connection(FailingMigrationConnection(real))
        finally:
            real.close()

    def test_transaction_schema_rejects_wrong_shape_same_name_financial_table(self):
        with self.api.get_db() as db:
            db.execute("DROP TABLE funding_attempt_conflict_evidence")
            db.execute(
                "CREATE TABLE funding_attempt_conflict_evidence (id INTEGER PRIMARY KEY, payload TEXT)"
            )
            db.commit()
            with self.assertRaisesRegex(RuntimeError, "funding_attempt_conflict_evidence"):
                self.api.validate_required_transaction_schema(db)
        with self.assertRaises((RuntimeError, sqlite3.OperationalError)):
            self.api.init_db()

    def test_clean_database_keeps_partial_unique_job_hire_index(self):
        with self.api.get_db() as db:
            indexes = {row[1]: row for row in db.execute("PRAGMA index_list('orders')").fetchall()}
        self.assertIn("idx_orders_one_job_hire", indexes)
        self.assertEqual(indexes["idx_orders_one_job_hire"][2], 1)
        self.assertEqual(indexes["idx_orders_one_job_hire"][4], 1)

    def test_transaction_schema_validation_rejects_malformed_same_name_job_hire_index(self):
        with self.api.get_db() as db:
            db.execute("DROP INDEX idx_orders_one_job_hire")
            db.execute("CREATE INDEX idx_orders_one_job_hire ON orders(id)")
            db.commit()
        with self.assertRaisesRegex(RuntimeError, "idx_orders_one_job_hire"):
            self.api.init_db()

    def test_duplicate_history_rejects_malformed_same_name_job_hire_guards(self):
        with self.api.get_db() as db:
            db.execute("DROP INDEX idx_orders_one_job_hire")
            db.execute(
                "INSERT INTO orders (id,type,job_id,worker_id,employer_id,status,total_amount) "
                "VALUES (100,'job_hire',2,1,2,'completed',10)"
            )
            db.execute(
                "INSERT INTO orders (id,type,job_id,worker_id,employer_id,status,total_amount) "
                "VALUES (101,'job_hire',2,3,2,'canceled',10)"
            )
            db.execute("CREATE INDEX idx_orders_one_job_hire ON orders(id)")
            db.execute(
                """CREATE TRIGGER trg_orders_one_job_hire_insert
                   BEFORE INSERT ON orders BEGIN SELECT 1; END"""
            )
            db.execute(
                """CREATE TRIGGER trg_orders_one_job_hire_update
                   BEFORE UPDATE OF job_id,type ON orders BEGIN SELECT 1; END"""
            )
            db.commit()

        with self.assertRaisesRegex(RuntimeError, "idx_orders_one_job_hire"):
            self.api.init_db()

        # Failed startup must release its writer transaction and preserve history.
        with self.api.get_db() as db:
            db.execute("BEGIN IMMEDIATE")
            self.assertEqual(
                db.execute(
                    "SELECT COUNT(*) FROM orders WHERE job_id=2 AND type='job_hire'"
                ).fetchone()[0],
                2,
            )
            db.execute("DROP INDEX idx_orders_one_job_hire")
            db.execute("DROP TRIGGER trg_orders_one_job_hire_insert")
            db.execute("DROP TRIGGER trg_orders_one_job_hire_update")
            db.execute("CREATE INDEX trg_orders_one_job_hire_insert ON orders(id)")
            db.commit()

        # A canonical trigger name occupied by the wrong schema object type also
        # fails closed rather than satisfying fallback validation.
        with self.assertRaisesRegex(
            RuntimeError, "trg_orders_one_job_hire_insert exact SQL"
        ):
            self.api.init_db()

        with self.api.get_db() as db:
            db.execute("DROP INDEX trg_orders_one_job_hire_insert")

        # Once poisoned objects are removed, startup installs exact fallback
        # enforcement and future duplicates are rejected without deleting history.
        self.api.init_db()
        with self.api.get_db() as db:
            self.assertEqual(
                db.execute(
                    "SELECT COUNT(*) FROM orders WHERE job_id=2 AND type='job_hire'"
                ).fetchone()[0],
                2,
            )
            with self.assertRaises(sqlite3.IntegrityError):
                db.execute(
                    "INSERT INTO orders (id,type,job_id,worker_id,employer_id,status,total_amount) "
                    "VALUES (102,'job_hire',2,1,2,'pending',10)"
                )
            db.rollback()

    def test_legacy_duplicate_job_hires_do_not_break_startup_or_lose_history(self):
        db = self.api.get_db()
        try:
            db.execute("DROP INDEX idx_orders_one_job_hire")
            db.execute(
                "INSERT INTO orders (id,type,job_id,worker_id,employer_id,status,total_amount) VALUES (100,'job_hire',2,1,2,'completed',10)"
            )
            db.execute(
                "INSERT INTO orders (id,type,job_id,worker_id,employer_id,status,total_amount) VALUES (101,'job_hire',2,3,2,'canceled',10)"
            )
            db.commit()
        finally:
            db.close()

        self.api.init_db()
        self.api.init_db()

        db = self.api.get_db()
        try:
            self.assertEqual(db.execute("SELECT COUNT(*) FROM orders WHERE job_id=2 AND type='job_hire'").fetchone()[0], 2)
            issue = db.execute(
                "SELECT details FROM audit_log WHERE action='legacy_duplicate_job_hire_detected' AND entity_id=2"
            ).fetchall()
            self.assertEqual(len(issue), 1)
            self.assertIn("100", issue[0][0])
            self.assertIn("101", issue[0][0])
            with self.assertRaises(sqlite3.IntegrityError):
                db.execute(
                    "INSERT INTO orders (id,type,job_id,worker_id,employer_id,status,total_amount) VALUES (102,'job_hire',2,1,2,'pending',10)"
                )
            db.rollback()
        finally:
            db.close()

    def test_money_parser_rejects_extreme_and_hidden_subcent_values_with_400(self):
        base = {
            "title": "Precision test",
            "description": "Reject pathological money input",
            "category": "testing",
            "budget_type": "fixed",
        }
        for value in (
            "1e999999",
            "1000000",
            "1e-999999",
            "25.5500000000000000000000000000000001",
            "25.550",
            "1__0.00",
            "_1.00",
            "1_.00",
            "１２.００",
            "NaN",
            "Infinity",
            "9" * 200,
        ):
            with self.subTest(value=value):
                status, result = self.request("POST", "/jobs", payload={**base, "budget_amount": value})
                self.assertEqual(status, 400, result)
                self.assertIn("budget_amount", result["error"])

        self.assertEqual(self.api.money_to_cents("999999.99"), 99_999_999)

    def test_hired_job_cannot_be_canceled_while_funded_order_is_active(self):
        status, result = self.hire_job_one()
        self.assertEqual(status, 201, result)

        status, result = self.request("DELETE", "/jobs/1")
        self.assertEqual(status, 409, result)

        with self.api.get_db() as db:
            self.assertEqual(db.execute("SELECT status FROM jobs WHERE id=1").fetchone()[0], "hired")
            self.assertEqual(db.execute("SELECT status FROM orders WHERE job_id=1").fetchone()[0], "in_progress")
            self.assertEqual(db.execute("SELECT status FROM escrow_holds WHERE order_id=(SELECT id FROM orders WHERE job_id=1)").fetchone()[0], "held")

    def test_fixed_job_hire_retry_recovers_committed_hold_after_lifecycle_crash(self):
        with mock.patch.object(
            self.api, "audit", side_effect=RuntimeError("after fixed hire funding commit")
        ):
            first_status, _ = self.hire_job_one()
        self.assertEqual(first_status, 500)

        with self.api.get_db() as db:
            durable = db.execute(
                """SELECT o.status AS order_status,m.status AS milestone_status,
                          f.status AS attempt_status,h.status AS hold_status
                   FROM orders o
                   JOIN milestones m ON m.order_id=o.id
                   JOIN funding_attempts f ON f.order_id=o.id AND f.milestone_id=m.id
                   JOIN escrow_holds h ON h.funding_attempt_id=f.id
                   WHERE o.job_id=1"""
            ).fetchone()
            self.assertEqual(tuple(durable), ("in_progress", "pending", "committed", "held"))

        # Lifecycle-only recovery must not depend on processor configuration or
        # issue any second processor call once the exact hold is committed.
        self.api.STRIPE_SECRET_KEY = ""
        with mock.patch.object(
            self.api, "audit", side_effect=RuntimeError("during fixed hire recovery")
        ):
            recovery_crash_status, recovery_crash_result = self.hire_job_one()
        self.assertEqual(recovery_crash_status, 500, recovery_crash_result)
        self.assertEqual(self.payment_create.call_count, 1)
        with self.api.get_db() as db:
            durable = db.execute(
                """SELECT j.status, m.status, a.status, h.status
                   FROM jobs j
                   JOIN orders o ON o.job_id=j.id AND o.type='job_hire'
                   JOIN milestones m ON m.order_id=o.id AND m.sequence=1
                   JOIN funding_attempts a ON a.id=(
                       SELECT funding_attempt_id FROM escrow_holds WHERE order_id=o.id LIMIT 1
                   )
                   JOIN escrow_holds h ON h.order_id=o.id
                   WHERE j.id=1"""
            ).fetchone()
            self.assertEqual(tuple(durable), ("open", "pending", "committed", "held"))
            db.execute("BEGIN IMMEDIATE")
            db.execute("UPDATE jobs SET updated_at=updated_at WHERE id=1")
            db.commit()

        conflict_status, conflict_result = self.request(
            "POST",
            "/jobs/1/hire",
            payload={
                "application_id": 14,
                "milestones": [{"description": "Changed scope", "amount": 25}],
            },
        )
        self.assertEqual(conflict_status, 409, conflict_result)
        self.assertEqual(self.payment_create.call_count, 1)

        retry_status, retry_result = self.hire_job_one()
        self.assertEqual(retry_status, 200, retry_result)
        self.assertTrue(retry_result["idempotent_replay"])
        self.assertEqual(self.payment_create.call_count, 1)

        with self.api.get_db() as db:
            state = db.execute(
                """SELECT o.status,m.status,j.status,a.status,m.escrow_payment_id
                   FROM orders o
                   JOIN milestones m ON m.order_id=o.id
                   JOIN jobs j ON j.id=o.job_id
                   JOIN applications a ON a.id=14
                   WHERE o.job_id=1"""
            ).fetchone()
            hold_count = db.execute(
                "SELECT COUNT(*) FROM escrow_holds WHERE order_id=(SELECT id FROM orders WHERE job_id=1)"
            ).fetchone()[0]
        self.assertEqual(tuple(state[:4]), ("in_progress", "in_progress", "hired", "accepted"))
        self.assertEqual(state[4], "pi_mock_hire_1")
        self.assertEqual(hold_count, 1)

    def test_fixed_job_hire_revalidates_lifecycle_under_fresh_writer_after_hold_commit(self):
        real_commit = self.api._commit_funding_attempt
        injected = False

        def commit_then_drift(db, attempt, processor_intent_id):
            nonlocal injected
            real_commit(db, attempt, processor_intent_id)
            self.assertFalse(db.in_transaction)
            writer = sqlite3.connect(self.api._get_db_path())
            try:
                writer.execute(
                    "UPDATE milestones SET amount=24 WHERE order_id=?", [attempt["order_id"]]
                )
                writer.execute(
                    "UPDATE orders SET creation_request_fingerprint=? WHERE id=?",
                    ["0" * 64, attempt["order_id"]],
                )
                writer.execute("UPDATE applications SET status='rejected' WHERE id=14")
                writer.commit()
                injected = True
            finally:
                writer.close()

        with mock.patch.object(
            self.api, "_commit_funding_attempt", side_effect=commit_then_drift
        ):
            status, result = self.hire_job_one()

        self.assertTrue(injected)
        self.assertEqual(status, 409, result)
        self.assertEqual(self.payment_create.call_count, 1)
        with self.api.get_db() as db:
            state = db.execute(
                """SELECT j.status,a.status,o.status,o.creation_request_fingerprint,
                          m.status,m.amount,h.status,COUNT(n.id)
                   FROM jobs j
                   JOIN applications a ON a.id=14
                   JOIN orders o ON o.job_id=j.id AND o.type='job_hire'
                   JOIN milestones m ON m.order_id=o.id
                   JOIN escrow_holds h ON h.order_id=o.id
                   LEFT JOIN notifications n ON n.user_id=a.worker_id AND n.type='job_hired'
                   WHERE j.id=1
                   GROUP BY j.status,a.status,o.status,o.creation_request_fingerprint,
                            m.status,m.amount,h.status"""
            ).fetchone()
        self.assertEqual(state[0], "open")
        self.assertEqual(state[1], "rejected")
        self.assertEqual(state[2], "in_progress")
        self.assertEqual(state[3], "0" * 64)
        self.assertEqual(state[4], "pending")
        self.assertEqual(state[5], 24)
        self.assertEqual(state[6], "held")
        self.assertEqual(state[7], 0)

    def test_fixed_job_hire_exact_retry_replays_after_post_commit_response_loss(self):
        with mock.patch.object(
            self.api,
            "flush_transactional_notification_emails",
            side_effect=RuntimeError("response lost after lifecycle commit"),
        ):
            first_status, _ = self.hire_job_one()
        self.assertEqual(first_status, 500)
        self.payment_create.assert_called_once()

        with self.api.get_db() as db:
            before = tuple(
                db.execute(
                    """SELECT j.status,a.status,o.status,m.status,m.escrow_payment_id,
                              (SELECT COUNT(*) FROM escrow_holds h WHERE h.order_id=o.id),
                              (SELECT COUNT(*) FROM funding_attempts f WHERE f.order_id=o.id)
                       FROM jobs j
                       JOIN applications a ON a.id=14
                       JOIN orders o ON o.job_id=j.id AND o.type='job_hire'
                       JOIN milestones m ON m.order_id=o.id AND m.sequence=1
                       WHERE j.id=1"""
                ).fetchone()
            )
        self.assertEqual(
            before,
            ("hired", "accepted", "in_progress", "in_progress", "pi_mock_hire_1", 1, 1),
        )

        self.api.JOB_HIRING_ENABLED = False
        self.api.STRIPE_SECRET_KEY = ""
        with mock.patch.object(
            self.api.stripe.PaymentIntent,
            "retrieve",
            side_effect=AssertionError("unexpected retrieve"),
            create=True,
        ), mock.patch.object(
            self.api.stripe.PaymentIntent,
            "search",
            side_effect=AssertionError("unexpected search"),
            create=True,
        ):
            retry_status, retry_result = self.hire_job_one()
        self.assertEqual(retry_status, 200, retry_result)
        self.assertTrue(retry_result["idempotent_replay"])
        self.payment_create.assert_called_once()

        changed_status, changed_result = self.request(
            "POST",
            "/jobs/1/hire",
            payload={
                "application_id": 14,
                "milestones": [{"description": "Changed scope", "amount": 25}],
            },
        )
        self.assertEqual(changed_status, 409, changed_result)
        self.payment_create.assert_called_once()

        with self.api.get_db() as db:
            after = tuple(
                db.execute(
                    """SELECT j.status,a.status,o.status,m.status,m.escrow_payment_id,
                              (SELECT COUNT(*) FROM escrow_holds h WHERE h.order_id=o.id),
                              (SELECT COUNT(*) FROM funding_attempts f WHERE f.order_id=o.id)
                       FROM jobs j
                       JOIN applications a ON a.id=14
                       JOIN orders o ON o.job_id=j.id AND o.type='job_hire'
                       JOIN milestones m ON m.order_id=o.id AND m.sequence=1
                       WHERE j.id=1"""
                ).fetchone()
            )
        self.assertEqual(after, before)

    def test_fixed_job_hire_recovery_rejects_newer_conflict_attempt(self):
        with mock.patch.object(
            self.api, "audit", side_effect=RuntimeError("after fixed hire funding commit")
        ):
            first_status, _ = self.hire_job_one()
        self.assertEqual(first_status, 500)

        with self.api.get_db() as db:
            committed = db.execute(
                "SELECT * FROM funding_attempts WHERE status='committed'"
            ).fetchone()
            db.execute(
                """INSERT INTO funding_attempts
                   (operation_key,attempt_number,request_fingerprint,processor_idempotency_key,
                    employer_id,order_id,milestone_id,base_amount_cents,platform_fee_cents,
                    processing_fee_cents,charged_total_cents,currency,status,error_code,error_message)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,'unknown',
                           'prior_attempt_success_conflict','newer contradictory attempt')""",
                [
                    committed["operation_key"],
                    int(committed["attempt_number"]) + 1,
                    committed["request_fingerprint"],
                    committed["processor_idempotency_key"] + ":conflict-test",
                    committed["employer_id"],
                    committed["order_id"],
                    committed["milestone_id"],
                    committed["base_amount_cents"],
                    committed["platform_fee_cents"],
                    committed["processing_fee_cents"],
                    committed["charged_total_cents"],
                    committed["currency"],
                ],
            )
            db.execute(
                """INSERT INTO funding_attempts
                   (operation_key,attempt_number,request_fingerprint,processor_idempotency_key,
                    employer_id,order_id,milestone_id,base_amount_cents,platform_fee_cents,
                    processing_fee_cents,charged_total_cents,currency,status,error_code,error_message)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,'failed',
                           'processor_card_declined','cross-operation failed attempt')""",
                [
                    "legacy-job-hire/1",
                    1,
                    committed["request_fingerprint"],
                    committed["processor_idempotency_key"] + ":legacy-operation",
                    committed["employer_id"],
                    committed["order_id"],
                    committed["milestone_id"],
                    committed["base_amount_cents"],
                    committed["platform_fee_cents"],
                    committed["processing_fee_cents"],
                    committed["charged_total_cents"],
                    committed["currency"],
                ],
            )
            db.commit()

        self.api.STRIPE_SECRET_KEY = ""
        with mock.patch.object(
            self.api.stripe.PaymentIntent,
            "retrieve",
            side_effect=AssertionError("unexpected retrieve"),
            create=True,
        ), mock.patch.object(
            self.api.stripe.PaymentIntent,
            "search",
            side_effect=AssertionError("unexpected search"),
            create=True,
        ):
            retry_status, retry_result = self.hire_job_one()
        self.assertEqual(retry_status, 409, retry_result)
        self.payment_create.assert_called_once()

        with self.api.get_db() as db:
            state = tuple(
                db.execute(
                    """SELECT j.status,a.status,m.status,
                              (SELECT COUNT(*) FROM escrow_holds h WHERE h.order_id=o.id)
                       FROM jobs j
                       JOIN applications a ON a.id=14
                       JOIN orders o ON o.job_id=j.id AND o.type='job_hire'
                       JOIN milestones m ON m.order_id=o.id AND m.sequence=1
                       WHERE j.id=1"""
                ).fetchone()
            )
            attempts_before = [
                tuple(row)
                for row in db.execute(
                    """SELECT operation_key,attempt_number,status,error_code
                       FROM funding_attempts ORDER BY id"""
                ).fetchall()
            ]
            db.execute(
                """DELETE FROM funding_attempts
                   WHERE operation_key=? AND attempt_number=?""",
                [committed["operation_key"], int(committed["attempt_number"]) + 1],
            )
            db.commit()
            db.execute("BEGIN IMMEDIATE")
            db.execute("UPDATE jobs SET updated_at=updated_at WHERE id=1")
            db.commit()
        self.assertEqual(state, ("open", "pending", "pending", 1))
        self.assertEqual(
            attempts_before,
            [
                (committed["operation_key"], 1, "committed", None),
                (
                    committed["operation_key"],
                    2,
                    "unknown",
                    "prior_attempt_success_conflict",
                ),
                (
                    "legacy-job-hire/1",
                    1,
                    "failed",
                    "processor_card_declined",
                ),
            ],
        )

        # Even after the newer unknown row is removed, an alternate operation
        # identity for the same milestone remains contradictory and blocks replay.
        with mock.patch.object(
            self.api.stripe.PaymentIntent,
            "retrieve",
            side_effect=AssertionError("unexpected retrieve"),
            create=True,
        ), mock.patch.object(
            self.api.stripe.PaymentIntent,
            "search",
            side_effect=AssertionError("unexpected search"),
            create=True,
        ):
            second_retry_status, second_retry_result = self.hire_job_one()
        self.assertEqual(second_retry_status, 409, second_retry_result)
        self.payment_create.assert_called_once()

        with self.api.get_db() as db:
            self.assertEqual(
                [
                    tuple(row)
                    for row in db.execute(
                        "SELECT operation_key,attempt_number,status FROM funding_attempts ORDER BY id"
                    ).fetchall()
                ],
                [
                    (committed["operation_key"], 1, "committed"),
                    ("legacy-job-hire/1", 1, "failed"),
                ],
            )
            self.assertEqual(
                tuple(
                    db.execute(
                        """SELECT j.status,a.status,m.status
                           FROM jobs j
                           JOIN applications a ON a.id=14
                           JOIN orders o ON o.job_id=j.id AND o.type='job_hire'
                           JOIN milestones m ON m.order_id=o.id AND m.sequence=1
                           WHERE j.id=1"""
                    ).fetchone()
                ),
                ("open", "pending", "pending"),
            )

    def test_funding_retry_uses_stable_stripe_idempotency_key(self):
        with self.api.get_db() as db:
            db.execute("INSERT INTO orders (id,type,worker_id,employer_id,status,total_amount) VALUES (500,'service_order',1,2,'pending',25.55)")
            db.execute("INSERT INTO milestones (id,order_id,title,amount,sequence,status) VALUES (600,500,'Delivery',25.55,1,'pending')")
            db.commit()

            self.api.fund_escrow_stripe(db, 2, 25.55, 500, 600, "Retry probe")
            first_key = self.payment_create.call_args.kwargs.get("idempotency_key")
            db.rollback()
            self.api.fund_escrow_stripe(db, 2, 25.55, 500, 600, "Retry probe")
            second_key = self.payment_create.call_args.kwargs.get("idempotency_key")

        self.assertIsNotNone(first_key)
        self.assertEqual(first_key, second_key)

    def test_committed_funding_operation_replay_returns_one_hold_without_recharging(self):
        with self.api.get_db() as db:
            db.execute("INSERT INTO orders (id,type,worker_id,employer_id,status,total_amount) VALUES (510,'service_order',1,2,'in_progress',25.55)")
            db.execute("INSERT INTO milestones (id,order_id,title,amount,sequence,status) VALUES (610,510,'Delivery',25.55,1,'pending')")
            first_pi, _ = self.api.fund_escrow_stripe(
                db, 2, 25.55, 510, 610, "Committed retry probe", funding_identity="milestone:610"
            )
            db.commit()
            second_pi, mode = self.api.fund_escrow_stripe(
                db, 2, 25.55, 510, 610, "Committed retry probe", funding_identity="milestone:610"
            )
            db.commit()
            hold_count = db.execute(
                "SELECT COUNT(*) FROM escrow_holds WHERE order_id=510 AND milestone_id=610"
            ).fetchone()[0]

        self.assertEqual(first_pi, second_pi)
        self.assertEqual(mode, "replayed")
        self.assertEqual(hold_count, 1)
        self.payment_create.assert_called_once()

    def test_legacy_funded_milestone_requires_reconciliation_before_any_retry(self):
        with self.api.get_db() as db:
            db.execute("INSERT INTO orders (id,type,worker_id,employer_id,status,total_amount) VALUES (520,'service_order',1,2,'in_progress',25.55)")
            db.execute("INSERT INTO milestones (id,order_id,title,amount,sequence,status) VALUES (620,520,'Legacy',25.55,1,'pending')")
            db.execute(
                "INSERT INTO escrow_holds (order_id,milestone_id,amount,status,stripe_payment_intent_id) VALUES (520,620,25.55,'held','pi_legacy')"
            )
            db.commit()
            with self.assertRaisesRegex(ValueError, "reconciliation"):
                self.api.fund_escrow_stripe(
                    db, 2, 25.55, 520, 620, "Legacy retry",
                    funding_identity="milestone:620",
                )
        self.payment_create.assert_not_called()

    def test_service_order_retry_recovers_durable_processor_success_after_local_response_failure(self):
        payload = {"amount": "25.55", "idempotency_key": "service-order-retry-0001"}
        with mock.patch.object(self.api, "push_notification", side_effect=RuntimeError("after Stripe")):
            status, _ = self.request("POST", "/services/1/order", payload=payload)
        self.assertEqual(status, 500)
        first_kwargs = dict(self.payment_create.call_args.kwargs)

        with self.api.get_db() as db:
            order = db.execute(
                "SELECT * FROM orders WHERE creation_idempotency_key=?", [payload["idempotency_key"]]
            ).fetchone()
            milestone = db.execute(
                "SELECT id FROM milestones WHERE order_id=?", [order["id"]]
            ).fetchone()
            attempt = db.execute(
                "SELECT * FROM funding_attempts WHERE operation_key=?",
                [f"milestone:{milestone['id']}"],
            ).fetchone()
            hold_count = db.execute(
                "SELECT COUNT(*) FROM escrow_holds WHERE order_id=?", [order["id"]]
            ).fetchone()[0]
            self.assertEqual(order["status"], "pending")
            self.assertEqual(attempt["status"], "committed")
            self.assertEqual(hold_count, 1)

        status, result = self.request("POST", "/services/1/order", payload=payload)
        self.assertEqual(status, 200, result)
        self.assertTrue(result["idempotent_replay"])
        self.assertEqual(result["status"], "in_progress")
        self.assertEqual(self.payment_create.call_count, 1)
        self.assertEqual(first_kwargs, dict(self.payment_create.call_args.kwargs))

    def test_service_order_idempotency_key_rejects_conflicting_canonical_request(self):
        key = "service-order-fingerprint-0001"
        status, created = self.request("POST", "/services/1/order", payload={
            "amount": "25.00",
            "idempotency_key": key,
            "notes": "same scope",
        })
        self.assertEqual(status, 201, created)
        status, replay = self.request("POST", "/services/1/order", payload={
            "amount": "25",
            "idempotency_key": key,
            "notes": "same scope",
        })
        self.assertEqual(status, 200, replay)
        self.assertTrue(replay["idempotent_replay"])
        status, conflict = self.request("POST", "/services/1/order", payload={
            "amount": "99.00",
            "idempotency_key": key,
            "notes": "same scope",
        })
        self.assertEqual(status, 409, conflict)
        self.assertIn("different service-order inputs", conflict["error"])
        self.payment_create.assert_called_once()
        with self.api.get_db() as db:
            order = db.execute(
                "SELECT total_amount,creation_request_fingerprint FROM orders WHERE creation_idempotency_key=?",
                [key],
            ).fetchone()
        self.assertEqual(order["total_amount"], 25)
        self.assertTrue(order["creation_request_fingerprint"])

    def test_service_order_response_loss_replay_returns_existing_order_without_recharging(self):
        payload = {"amount": "25.55", "idempotency_key": "service-checkout-replay123"}
        status, first = self.request("POST", "/services/1/order", payload=payload)
        self.assertEqual(status, 201, first)
        status, replay = self.request("POST", "/services/1/order", payload=payload)
        self.assertEqual(status, 200, replay)
        self.assertEqual(replay["id"], first["id"])
        self.assertTrue(replay["idempotent_replay"])
        self.payment_create.assert_called_once()

    def test_manual_route_cannot_create_second_intent_after_ambiguous_service_checkout(self):
        key = "ambiguous-service-checkout-0001"
        self.payment_create.side_effect = self.api.STRIPE_ERROR("response lost after send")
        status, result = self.request("POST", "/services/1/order", payload={
            "amount": "25.00",
            "idempotency_key": key,
        })
        self.assertEqual(status, 409, result)
        self.assertIn("reconciliation", result["error"].lower())
        with self.api.get_db() as db:
            order = db.execute(
                "SELECT * FROM orders WHERE creation_idempotency_key=?", [key]
            ).fetchone()
            milestone = db.execute(
                "SELECT * FROM milestones WHERE order_id=?", [order["id"]]
            ).fetchone()
            first_attempt = db.execute(
                "SELECT * FROM funding_attempts WHERE milestone_id=?", [milestone["id"]]
            ).fetchone()
        self.assertEqual(first_attempt["operation_key"], f"milestone:{milestone['id']}")
        self.assertEqual(first_attempt["status"], "unknown")

        status, retry = self.request("POST", "/payments/fund-escrow", payload={
            "order_id": order["id"],
            "milestone_id": milestone["id"],
            "amount": "25.00",
        })
        self.assertEqual(status, 409, retry)
        self.assertIn("reconciliation", retry["error"].lower())
        self.payment_create.assert_called_once()
        with self.api.get_db() as db:
            attempts = db.execute(
                "SELECT operation_key,status FROM funding_attempts WHERE milestone_id=?",
                [milestone["id"]],
            ).fetchall()
            hold_count = db.execute(
                "SELECT COUNT(*) FROM escrow_holds WHERE milestone_id=?", [milestone["id"]]
            ).fetchone()[0]
        self.assertEqual(
            [tuple(row) for row in attempts],
            [(f"milestone:{milestone['id']}", "unknown")],
        )
        self.assertEqual(hold_count, 0)

    def test_manual_aggregate_route_rejects_hourly_contract_obligations(self):
        order_id = self.seed_hourly_order(order_id=89, status="pending")
        self.payment_create.reset_mock()
        status, result = self.request("POST", "/payments/fund-escrow", payload={
            "order_id": order_id,
            "amount": "1000.00",
        })
        self.assertEqual(status, 503, result)
        self.assertIn("Task 4", result["error"])
        self.payment_create.assert_not_called()

    def test_manual_milestone_route_rejects_historical_hourly_contract_obligations(self):
        order_id = self.seed_hourly_order(order_id=891, status="pending")
        with self.api.get_db() as db:
            db.execute("DELETE FROM escrow_holds WHERE order_id=?", [order_id])
            db.execute(
                """INSERT INTO milestones
                   (id,order_id,title,amount,sequence,status)
                   VALUES (991,891,'Historical hybrid milestone',25,1,'pending')"""
            )
            db.commit()

        self.payment_create.reset_mock()
        status, result = self.request("POST", "/payments/fund-escrow", payload={
            "order_id": order_id,
            "milestone_id": 991,
            "amount": "25.00",
        })

        self.assertEqual(status, 503, result)
        self.assertIn("Task 4", result["error"])
        self.payment_create.assert_not_called()
        with self.api.get_db() as db:
            self.assertEqual(
                db.execute("SELECT COUNT(*) FROM funding_attempts WHERE order_id=?", [order_id]).fetchone()[0],
                0,
            )
            self.assertEqual(
                db.execute("SELECT COUNT(*) FROM escrow_holds WHERE order_id=?", [order_id]).fetchone()[0],
                0,
            )

    def test_manual_funding_cannot_overlap_normal_service_checkout_or_regress_status(self):
        status, order = self.request("POST", "/services/1/order", payload={
            "amount": "25.00",
            "idempotency_key": "service-order-aggregate-funding-0001",
        })
        self.assertEqual(status, 201, order)
        order_id = order["id"]
        with self.api.get_db() as db:
            milestone = db.execute(
                "SELECT id,status FROM milestones WHERE order_id=?",
                [order_id],
            ).fetchone()
            self.assertEqual(milestone["status"], "in_progress")
            self.assertEqual(
                db.execute("SELECT COUNT(*) FROM escrow_holds WHERE order_id=?", [order_id]).fetchone()[0],
                1,
            )

        self.payment_create.reset_mock()
        status, replay = self.request("POST", "/payments/fund-escrow", payload={
            "order_id": order_id,
            "milestone_id": milestone["id"],
            "amount": "25.00",
        })
        self.assertEqual(status, 200, replay)
        self.assertTrue(replay["idempotent_replay"])
        self.assertEqual(replay["mode"], "replayed")
        self.payment_create.assert_not_called()

        status, result = self.request("POST", "/payments/fund-escrow", payload={
            "order_id": order_id,
            "amount": "25.00",
        })
        self.assertEqual(status, 409, result)
        self.payment_create.assert_not_called()

        with self.api.get_db() as db:
            self.assertEqual(
                db.execute("SELECT COUNT(*) FROM escrow_holds WHERE order_id=?", [order_id]).fetchone()[0],
                1,
            )
            self.assertEqual(
                db.execute("SELECT status FROM milestones WHERE id=?", [milestone["id"]]).fetchone()[0],
                "in_progress",
            )

    def test_money_parser_is_independent_of_decimal_context(self):
        with decimal.localcontext() as context:
            context.prec = 6
            context.traps[decimal.Rounded] = True
            self.assertEqual(self.api.money_to_cents("999999.99"), 99_999_999)

    def test_money_moving_routes_reject_noncanonical_forms_before_stripe(self):
        status, result = self.request("POST", "/services/1/order", payload={
            "amount": "１２.００",
            "idempotency_key": "service-checkout-unicode123",
        })
        self.assertEqual(status, 400, result)
        self.payment_create.assert_not_called()

        with self.api.get_db() as db:
            db.execute("INSERT INTO orders (id,type,worker_id,employer_id,status,total_amount) VALUES (500,'service_order',1,2,'pending',25)")
            db.execute("INSERT INTO milestones (id,order_id,title,amount,sequence,status) VALUES (600,500,'Delivery',25,1,'pending')")
            db.commit()
        status, result = self.request("POST", "/payments/fund-escrow", payload={
            "order_id": 500,
            "milestone_id": 600,
            "amount": "1e2",
        })
        self.assertEqual(status, 400, result)
        self.payment_create.assert_not_called()

    def test_raw_json_money_numbers_preserve_lexical_form_and_fail_before_stripe(self):
        for amount_token in ("1e2", "25.550", "25.5500000000000000000000000000000001"):
            with self.subTest(route="service", amount_token=amount_token):
                self.payment_create.reset_mock()
                status, result = self.request(
                    "POST",
                    "/services/1/order",
                    raw_body=(
                        '{"amount":' + amount_token
                        + ',"idempotency_key":"service-raw-number-' + ("exponent" if "e" in amount_token else "subcent") + '"}'
                    ),
                )
                self.assertEqual(status, 400, result)
                self.payment_create.assert_not_called()

        with self.api.get_db() as db:
            db.execute("INSERT INTO orders (id,type,worker_id,employer_id,status,total_amount) VALUES (530,'service_order',1,2,'pending',25)")
            db.execute("INSERT INTO milestones (id,order_id,title,amount,sequence,status) VALUES (630,530,'Delivery',25,1,'pending')")
            db.commit()
        status, result = self.request(
            "POST",
            "/payments/fund-escrow",
            raw_body='{"order_id":530,"milestone_id":630,"amount":1e2}',
        )
        self.assertEqual(status, 400, result)
        self.payment_create.assert_not_called()

    def test_service_pricing_transitions_remain_checkout_compatible(self):
        with self.api.get_db() as db:
            db.executemany(
                """INSERT INTO services
                   (id,worker_id,title,description,category,pricing_type,price,hourly_rate,
                    delivery_time_days,status)
                   VALUES (?,?,?,?,?,?,?,?,?,'active')""",
                [
                    (10, 1, 'Fixed unchanged', 'Fixed scope', 'testing', 'fixed', 10, None, 3),
                    (11, 1, 'Fixed to hourly', 'Hourly scope', 'testing', 'fixed', 10, None, 3),
                    (12, 1, 'Hourly to fixed', 'Fixed scope', 'testing', 'hourly', None, 15, 4),
                    (13, 1, 'Fixed to custom', 'Custom scope', 'testing', 'fixed', 12, None, 5),
                ],
            )
            db.commit()

        status, rejected = self.request(
            "PUT", "/services/10", token="tok-worker", payload={"pricing_type": "hourly"}
        )
        self.assertEqual(status, 400, rejected)
        status, fixed_order = self.request(
            "POST",
            "/services/10/order",
            payload={"idempotency_key": "pricing-transition-fixed-0001"},
        )
        self.assertEqual(status, 201, fixed_order)
        self.assertEqual(fixed_order["total_amount"], 10)

        status, hourly_listing = self.request(
            "PUT",
            "/services/11",
            token="tok-worker",
            payload={"pricing_type": "hourly", "hourly_rate": 20},
        )
        self.assertEqual(status, 200, hourly_listing)
        self.assertIsNone(hourly_listing["price"])
        status, hourly_order = self.request(
            "POST",
            "/services/11/order",
            payload={"hours": 2, "idempotency_key": "pricing-transition-hourly-001"},
        )
        self.assertEqual(status, 201, hourly_order)
        self.assertEqual(hourly_order["total_amount"], 40)

        status, fixed_listing = self.request(
            "PUT",
            "/services/12",
            token="tok-worker",
            payload={"pricing_type": "fixed", "price": 30},
        )
        self.assertEqual(status, 200, fixed_listing)
        self.assertIsNone(fixed_listing["hourly_rate"])
        status, transitioned_fixed_order = self.request(
            "POST",
            "/services/12/order",
            payload={"idempotency_key": "pricing-transition-fixed-0002"},
        )
        self.assertEqual(status, 201, transitioned_fixed_order)
        self.assertEqual(transitioned_fixed_order["total_amount"], 30)
        self.assertIsNotNone(transitioned_fixed_order["deadline_at"])

        status, custom_listing = self.request(
            "PUT", "/services/13", token="tok-worker", payload={"pricing_type": "custom"}
        )
        self.assertEqual(status, 200, custom_listing)
        self.assertIsNone(custom_listing["price"])
        self.assertIsNone(custom_listing["hourly_rate"])
        status, custom_order = self.request(
            "POST",
            "/services/13/order",
            payload={"amount": "22.00", "idempotency_key": "pricing-transition-custom-001"},
        )
        self.assertEqual(status, 201, custom_order)
        self.assertEqual(custom_order["total_amount"], 22)

    def test_hourly_service_product_rounds_from_exact_decimal_coefficients(self):
        with self.api.get_db() as db:
            db.execute(
                "INSERT INTO services (id,worker_id,title,description,category,pricing_type,hourly_rate,status) VALUES (2,1,'Tiny hourly','Exact rounding','testing','hourly',0.01,'active')"
            )
            db.commit()
        status, result = self.request("POST", "/services/2/order", payload={
            "hours": "1.49999999999999999999999999999",
            "idempotency_key": "service-hourly-exact-rounding",
        })
        self.assertEqual(status, 201, result)
        self.assertEqual(self.payment_create.call_args.kwargs["amount"], 3)
        with self.api.get_db() as db:
            hold = db.execute(
                "SELECT base_amount_cents,charged_total_cents FROM escrow_holds WHERE order_id=?",
                [result["id"]],
            ).fetchone()
        self.assertEqual(tuple(hold), (1, 3))

    def test_manual_funding_requires_canonical_ids_binding_and_authoritative_amounts(self):
        with self.api.get_db() as db:
            db.execute("INSERT INTO users (id,email,name,password_hash) VALUES (99,'foreign@example.com','Foreign','x')")
            db.execute("INSERT INTO orders (id,type,worker_id,employer_id,status,total_amount) VALUES (540,'service_order',1,2,'pending',25)")
            db.execute("INSERT INTO milestones (id,order_id,title,amount,sequence,status) VALUES (640,540,'Owned',25,1,'pending')")
            db.execute("INSERT INTO orders (id,type,worker_id,employer_id,status,total_amount) VALUES (541,'service_order',1,99,'pending',25)")
            db.execute("INSERT INTO milestones (id,order_id,title,amount,sequence,status) VALUES (641,541,'Foreign',25,1,'pending')")
            db.commit()

        for milestone_id in ("0640", 640.0):
            with self.subTest(milestone_id=milestone_id):
                self.payment_create.reset_mock()
                status, result = self.request("POST", "/payments/fund-escrow", payload={
                    "order_id": 540,
                    "milestone_id": milestone_id,
                    "amount": "25.00",
                })
                self.assertEqual(status, 400, result)
                self.payment_create.assert_not_called()

        status, result = self.request("POST", "/payments/fund-escrow", payload={
            "order_id": 540,
            "milestone_id": 641,
            "amount": "25.00",
        })
        self.assertEqual(status, 404, result)
        self.payment_create.assert_not_called()
        with self.api.get_db() as db:
            self.assertEqual(db.execute("SELECT status FROM milestones WHERE id=641").fetchone()[0], "pending")
            self.assertEqual(db.execute("SELECT COUNT(*) FROM escrow_holds WHERE milestone_id=641").fetchone()[0], 0)

        status, result = self.request("POST", "/payments/fund-escrow", payload={
            "order_id": 540,
            "milestone_id": 640,
            "amount": "24.99",
        })
        self.assertEqual(status, 409, result)
        self.payment_create.assert_not_called()

        status, result = self.request("POST", "/payments/fund-escrow", payload={
            "order_id": 540,
            "milestone_id": 640,
            "amount": "25.00",
        })
        self.assertEqual(status, 200, result)
        self.assertEqual(self.payment_create.call_args.kwargs["amount"], 2600)
        self.assertEqual(self.payment_create.call_args.kwargs["metadata"]["funding_identity"], "milestone:640")
        with self.api.get_db() as db:
            hold = db.execute(
                "SELECT order_id,milestone_id,base_amount_cents,funding_identity FROM escrow_holds WHERE milestone_id=640"
            ).fetchone()
            self.assertEqual(tuple(hold), (540, 640, 2500, "milestone:640"))
            self.assertEqual(db.execute("SELECT status FROM milestones WHERE id=640").fetchone()[0], "in_progress")
            self.assertEqual(db.execute("SELECT status FROM orders WHERE id=540").fetchone()[0], "in_progress")

        self.payment_create.reset_mock()
        for payload in (
            {"order_id": 540, "milestone_id": 640, "amount": "25.00"},
            {"order_id": 540, "amount": "25.00"},
        ):
            with self.subTest(duplicate_payload=payload):
                status, result = self.request("POST", "/payments/fund-escrow", payload=payload)
                if payload.get("milestone_id") is not None:
                    self.assertEqual(status, 200, result)
                    self.assertTrue(result["idempotent_replay"])
                else:
                    self.assertEqual(status, 409, result)
                self.payment_create.assert_not_called()
        with self.api.get_db() as db:
            self.assertEqual(db.execute("SELECT COUNT(*) FROM escrow_holds WHERE order_id=540").fetchone()[0], 1)
            self.assertEqual(db.execute("SELECT status FROM milestones WHERE id=640").fetchone()[0], "in_progress")
            self.assertEqual(db.execute("SELECT status FROM orders WHERE id=540").fetchone()[0], "in_progress")

        with self.api.get_db() as db:
            db.execute("INSERT INTO orders (id,type,worker_id,employer_id,status,total_amount) VALUES (542,'service_order',1,2,'pending',25)")
            db.commit()
        self.payment_create.reset_mock()
        status, result = self.request("POST", "/payments/fund-escrow", payload={
            "order_id": 542,
            "amount": "24.99",
        })
        self.assertEqual(status, 409, result)
        self.payment_create.assert_not_called()

        status, result = self.request("POST", "/payments/fund-escrow", payload={"order_id": 542})
        self.assertEqual(status, 200, result)
        self.assertEqual(self.payment_create.call_args.kwargs["amount"], 2600)
        self.assertEqual(self.payment_create.call_args.kwargs["metadata"]["funding_identity"], "order:542:full")
        self.payment_create.reset_mock()
        status, result = self.request("POST", "/payments/fund-escrow", payload={"order_id": 542})
        self.assertEqual(status, 200, result)
        self.assertTrue(result["idempotent_replay"])
        self.payment_create.assert_not_called()
        with self.api.get_db() as db:
            self.assertEqual(db.execute("SELECT COUNT(*) FROM escrow_holds WHERE order_id=542").fetchone()[0], 1)

    def test_manual_funding_rejects_post_commit_milestone_drift(self):
        with self.api.get_db() as db:
            db.execute(
                "INSERT INTO orders (id,type,worker_id,employer_id,status,total_amount) "
                "VALUES (546,'service_order',1,2,'pending',25)"
            )
            db.execute(
                "INSERT INTO milestones (id,order_id,title,amount,sequence,status) "
                "VALUES (646,546,'Manual delivery',25,1,'pending')"
            )
            db.commit()

        real_commit = self.api._commit_funding_attempt
        drifted = False

        def commit_then_drift(db, attempt, processor_intent_id):
            nonlocal drifted
            real_commit(db, attempt, processor_intent_id)
            self.assertFalse(db.in_transaction)
            writer = sqlite3.connect(self.api._get_db_path())
            try:
                writer.execute("UPDATE milestones SET amount=99 WHERE id=646")
                writer.commit()
                drifted = True
            finally:
                writer.close()

        with mock.patch.object(
            self.api, "_commit_funding_attempt", side_effect=commit_then_drift
        ):
            status, result = self.request("POST", "/payments/fund-escrow", payload={
                "order_id": 546,
                "milestone_id": 646,
                "amount": "25.00",
            })

        self.assertTrue(drifted, result)
        self.assertEqual(status, 409, result)
        self.assertEqual(self.payment_create.call_count, 1)
        with self.api.get_db() as db:
            state = db.execute(
                """SELECT o.status,m.status,m.amount,h.amount,a.base_amount_cents
                   FROM orders o
                   JOIN milestones m ON m.order_id=o.id
                   JOIN escrow_holds h ON h.order_id=o.id AND h.milestone_id=m.id
                   JOIN funding_attempts a ON a.id=h.funding_attempt_id
                   WHERE o.id=546"""
            ).fetchone()
        self.assertEqual(tuple(state), ("pending", "pending", 99, 25, 2500))

    def test_manual_aggregate_funding_rejects_post_commit_order_total_drift(self):
        with self.api.get_db() as db:
            db.execute(
                "INSERT INTO orders (id,type,worker_id,employer_id,status,total_amount) "
                "VALUES (547,'service_order',1,2,'pending',25)"
            )
            db.commit()

        real_commit = self.api._commit_funding_attempt
        drifted = False

        def commit_then_drift(db, attempt, processor_intent_id):
            nonlocal drifted
            real_commit(db, attempt, processor_intent_id)
            self.assertFalse(db.in_transaction)
            writer = sqlite3.connect(self.api._get_db_path())
            try:
                writer.execute("UPDATE orders SET total_amount=99 WHERE id=547")
                writer.commit()
                drifted = True
            finally:
                writer.close()

        with mock.patch.object(
            self.api, "_commit_funding_attempt", side_effect=commit_then_drift
        ):
            status, result = self.request("POST", "/payments/fund-escrow", payload={
                "order_id": 547,
                "amount": "25.00",
            })

        self.assertTrue(drifted, result)
        self.assertEqual(status, 409, result)
        self.assertEqual(self.payment_create.call_count, 1)
        with self.api.get_db() as db:
            state = db.execute(
                """SELECT o.status,o.total_amount,h.amount,a.base_amount_cents
                   FROM orders o
                   JOIN escrow_holds h ON h.order_id=o.id AND h.milestone_id IS NULL
                   JOIN funding_attempts a ON a.id=h.funding_attempt_id
                   WHERE o.id=547"""
            ).fetchone()
        self.assertEqual(tuple(state), ("pending", 99, 25, 2500))

    def test_multi_hold_release_keeps_writer_lock_off_processor_io(self):
        with self.api.get_db() as db:
            db.execute(
                "INSERT INTO orders (id,type,worker_id,employer_id,status,total_amount) "
                "VALUES (548,'service_order',1,2,'submitted',25)"
            )
            db.execute(
                "INSERT INTO milestones (id,order_id,title,amount,sequence,status) "
                "VALUES (648,548,'Release one',10,1,'submitted')"
            )
            db.execute(
                "INSERT INTO milestones (id,order_id,title,amount,sequence,status) "
                "VALUES (649,548,'Release two',15,2,'submitted')"
            )
            db.execute(
                """INSERT INTO escrow_holds
                   (order_id,milestone_id,amount,stripe_payment_intent_id,
                    funding_identity,status)
                   VALUES (548,648,10,'pi_release_1','milestone:648','held')"""
            )
            db.execute(
                """INSERT INTO escrow_holds
                   (order_id,milestone_id,amount,stripe_payment_intent_id,
                    funding_identity,status)
                   VALUES (548,649,15,'pi_release_2','milestone:649','held')"""
            )
            db.execute(
                "UPDATE worker_profiles SET payout_account_id='acct_live_worker' WHERE user_id=1"
            )
            self._bind_verified_funding(db, 548, 648, 10, "pi_release_1")
            self._bind_verified_funding(db, 548, 649, 15, "pi_release_2")
            db.commit()

        transfer_calls = []

        def transfer_create(**kwargs):
            writer = sqlite3.connect(self.api._get_db_path(), timeout=0.1)
            try:
                writer.execute("UPDATE users SET name=name WHERE id=2")
                writer.commit()
            finally:
                writer.close()
            transfer_calls.append(kwargs)
            return exact_transfer(f"tr_release_{len(transfer_calls)}", kwargs)

        transfer_api = type("Transfer", (), {"create": staticmethod(transfer_create)})
        account = {"details_submitted": True, "payouts_enabled": True, "charges_enabled": True, "capabilities": {"transfers": "active"}}
        with mock.patch.object(
            self.api.stripe, "Transfer", transfer_api, create=True
        ), mock.patch.object(
            self.api, "retrieve_live_connect_account", return_value=account
        ):
            with self.api.get_db() as db:
                self.api.release_escrow_to_worker(db, 548, 648, 10, 1)
                self.api.release_escrow_to_worker(db, 548, 649, 15, 1)

        self.assertEqual(len(transfer_calls), 2)
        with self.api.get_db() as db:
            states = [tuple(row) for row in db.execute(
                "SELECT milestone_id,status,stripe_transfer_id FROM escrow_holds "
                "WHERE order_id=548 ORDER BY milestone_id"
            )]
        self.assertEqual(states, [
            (648, "released", "tr_release_1"),
            (649, "released", "tr_release_2"),
        ])

    def test_release_normalizes_modern_stripe_error_without_legacy_namespace(self):
        with self.api.get_db() as db:
            db.execute(
                "INSERT INTO orders (id,type,worker_id,employer_id,status,total_amount) "
                "VALUES (549,'service_order',1,2,'submitted',10)"
            )
            db.execute(
                "INSERT INTO milestones (id,order_id,title,amount,sequence,status) "
                "VALUES (650,549,'Modern error',10,1,'submitted')"
            )
            db.execute(
                """INSERT INTO escrow_holds
                   (order_id,milestone_id,amount,stripe_payment_intent_id,
                    funding_identity,status)
                   VALUES (549,650,10,'pi_modern_error','milestone:650','held')"""
            )
            db.execute(
                "UPDATE worker_profiles SET payout_account_id='acct_live_worker' WHERE user_id=1"
            )
            self._bind_verified_funding(db, 549, 650, 10, "pi_modern_error")
            db.commit()

        class ModernStripeError(Exception):
            pass

        transfer = type("Transfer", (), {
            "create": staticmethod(lambda **kwargs: (_ for _ in ()).throw(
                ModernStripeError("processor unavailable")
            ))
        })
        modern_stripe = type("ModernStripe", (), {"Transfer": transfer})()
        account = {"details_submitted": True, "payouts_enabled": True, "charges_enabled": True, "capabilities": {"transfers": "active"}}
        with mock.patch.object(self.api, "stripe", modern_stripe), mock.patch.object(
            self.api, "STRIPE_ERROR", ModernStripeError
        ), mock.patch.object(
            self.api, "stripe_configured", return_value=True
        ), mock.patch.object(
            self.api, "retrieve_live_connect_account", return_value=account
        ):
            with self.api.get_db() as db:
                with self.assertRaisesRegex(
                    self.api.FundingReconciliationRequired, "outcome is ambiguous"
                ):
                    self.api.release_escrow_to_worker(db, 549, 650, 10, 1)

        with self.api.get_db() as db:
            self.assertEqual(
                tuple(db.execute(
                    "SELECT status,stripe_transfer_id FROM escrow_holds WHERE milestone_id=650"
                ).fetchone()),
                ("held", None),
            )

    def test_release_fails_closed_when_exact_hold_cas_updates_zero_rows(self):
        with self.api.get_db() as db:
            db.execute(
                "INSERT INTO orders (id,type,worker_id,employer_id,status,total_amount) "
                "VALUES (550,'service_order',1,2,'submitted',10)"
            )
            db.execute(
                "INSERT INTO milestones (id,order_id,title,amount,sequence,status) "
                "VALUES (651,550,'Ignored CAS',10,1,'submitted')"
            )
            db.execute(
                """INSERT INTO escrow_holds
                   (order_id,milestone_id,amount,stripe_payment_intent_id,
                    funding_identity,status)
                   VALUES (550,651,10,'pi_ignored_cas','milestone:651','held')"""
            )
            db.execute(
                "UPDATE worker_profiles SET payout_account_id='acct_live_worker' WHERE user_id=1"
            )
            self._bind_verified_funding(db, 550, 651, 10, "pi_ignored_cas")
            db.execute(
                """CREATE TRIGGER ignore_release_cas
                   BEFORE UPDATE ON escrow_holds
                   WHEN OLD.milestone_id=651 AND NEW.status='released'
                   BEGIN SELECT RAISE(IGNORE); END"""
            )
            db.commit()

        transfer_api = type("Transfer", (), {
            "create": staticmethod(lambda **kwargs: exact_transfer("tr_ignored_cas", kwargs))
        })
        account = {"details_submitted": True, "payouts_enabled": True, "charges_enabled": True, "capabilities": {"transfers": "active"}}
        with mock.patch.object(
            self.api.stripe, "Transfer", transfer_api, create=True
        ), mock.patch.object(
            self.api, "retrieve_live_connect_account", return_value=account
        ):
            with self.api.get_db() as db:
                with self.assertRaisesRegex(
                    self.api.FundingReconciliationRequired,
                    "Escrow hold changed during exact payout settlement",
                ):
                    self.api.release_escrow_to_worker(db, 550, 651, 10, 1)

        with self.api.get_db() as db:
            hold = db.execute(
                "SELECT status,stripe_transfer_id FROM escrow_holds WHERE milestone_id=651"
            ).fetchone()
            payout_count = db.execute(
                "SELECT COUNT(*) FROM payout_transfers WHERE milestone_id=651"
            ).fetchone()[0]
            revenue_count = db.execute(
                "SELECT COUNT(*) FROM platform_revenue WHERE order_id=550"
            ).fetchone()[0]
        self.assertEqual(tuple(hold), ("held", None))
        self.assertEqual(payout_count, 0)
        self.assertEqual(revenue_count, 0)

    def test_manual_funding_only_starts_first_milestone_and_lifecycle_funds_later_steps(self):
        with self.api.get_db() as db:
            db.execute("INSERT INTO orders (id,type,worker_id,employer_id,status,total_amount) VALUES (543,'service_order',1,2,'pending',45)")
            db.execute("INSERT INTO milestones (id,order_id,title,amount,sequence,status) VALUES (643,543,'First',10,1,'pending')")
            db.execute("INSERT INTO milestones (id,order_id,title,amount,sequence,status) VALUES (644,543,'Second',15,2,'pending')")
            db.execute("INSERT INTO milestones (id,order_id,title,amount,sequence,status) VALUES (645,543,'Third',20,3,'pending')")
            db.execute("UPDATE worker_profiles SET payout_account_id='acct_live_worker' WHERE user_id=1")
            db.commit()

        for future_milestone_id in (644, 645):
            status, result = self.request("POST", "/payments/fund-escrow", payload={
                "order_id": 543,
                "milestone_id": future_milestone_id,
            })
            self.assertEqual(status, 409, result)
            self.payment_create.assert_not_called()

        status, result = self.request("POST", "/payments/fund-escrow", payload={
            "order_id": 543,
            "milestone_id": 643,
        })
        self.assertEqual(status, 200, result)
        self.assertEqual(self.payment_create.call_count, 1)

        transfer_counter = {"value": 0}

        def create_transfer(**kwargs):
            transfer_counter["value"] += 1
            return exact_transfer(f"tr_mock_release_{transfer_counter['value']}", kwargs)

        self.api.stripe.Transfer = type(
            "Transfer", (), {"create": mock.Mock(side_effect=create_transfer)}
        )
        account = {"details_submitted": True, "payouts_enabled": True, "charges_enabled": True, "capabilities": {"transfers": "active"}}
        with mock.patch.object(self.api, "retrieve_live_connect_account", return_value=account):
            for current_id, next_id in ((643, 644), (644, 645)):
                status, submitted = self.request(
                    "POST", "/orders/543/submit", token="tok-worker", payload={"notes": "Milestone delivered"}
                )
                self.assertEqual(status, 200, submitted)
                status, approved = self.request("POST", "/orders/543/approve", token="tok-employer")
                self.assertEqual(status, 200, approved)
                with self.api.get_db() as db:
                    self.assertEqual(db.execute("SELECT status FROM milestones WHERE id=?", [current_id]).fetchone()[0], "approved")
                    self.assertEqual(db.execute("SELECT status FROM milestones WHERE id=?", [next_id]).fetchone()[0], "in_progress")
                    self.assertEqual(db.execute("SELECT status FROM orders WHERE id=543").fetchone()[0], "in_progress")

        self.assertEqual(self.payment_create.call_count, 3)
        self.assertEqual(
            [call.kwargs["metadata"]["funding_identity"] for call in self.payment_create.call_args_list],
            ["milestone:643", "milestone:644", "milestone:645"],
        )
        with self.api.get_db() as db:
            self.assertEqual(db.execute("SELECT COUNT(*) FROM escrow_holds WHERE order_id=543").fetchone()[0], 3)
            self.assertEqual(
                [row[0] for row in db.execute("SELECT status FROM milestones WHERE order_id=543 ORDER BY sequence")],
                ["approved", "approved", "in_progress"],
            )

    def test_approve_rejects_next_milestone_drift_after_funding_commit(self):
        with self.api.get_db() as db:
            db.execute(
                "INSERT INTO orders (id,type,worker_id,employer_id,status,total_amount) "
                "VALUES (593,'service_order',1,2,'pending',25)"
            )
            db.execute(
                "INSERT INTO milestones (id,order_id,title,amount,sequence,status) "
                "VALUES (694,593,'First',10,1,'pending')"
            )
            db.execute(
                "INSERT INTO milestones (id,order_id,title,amount,sequence,status) "
                "VALUES (695,593,'Second',15,2,'pending')"
            )
            db.execute(
                "UPDATE worker_profiles SET payout_account_id='acct_live_worker' WHERE user_id=1"
            )
            db.commit()

        status, funded = self.request("POST", "/payments/fund-escrow", payload={
            "order_id": 593,
            "milestone_id": 694,
            "amount": "10.00",
        })
        self.assertEqual(status, 200, funded)
        status, submitted = self.request(
            "POST", "/orders/593/submit", token="tok-worker", payload={"notes": "done"}
        )
        self.assertEqual(status, 200, submitted)

        real_commit = self.api._commit_funding_attempt
        drifted = False

        def commit_then_drift(db, attempt, processor_intent_id):
            nonlocal drifted
            real_commit(db, attempt, processor_intent_id)
            if attempt["milestone_id"] == 695:
                self.assertFalse(db.in_transaction)
                writer = sqlite3.connect(self.api._get_db_path())
                try:
                    writer.execute("UPDATE milestones SET amount=99 WHERE id=695")
                    writer.commit()
                    drifted = True
                finally:
                    writer.close()

        transfer_api = type("Transfer", (), {
            "create": staticmethod(lambda **kwargs: exact_transfer("tr_approve_drift", kwargs))
        })
        account = {"details_submitted": True, "payouts_enabled": True, "charges_enabled": True, "capabilities": {"transfers": "active"}}
        with mock.patch.object(
            self.api, "_commit_funding_attempt", side_effect=commit_then_drift
        ), mock.patch.object(
            self.api.stripe, "Transfer", transfer_api, create=True
        ), mock.patch.object(
            self.api, "retrieve_live_connect_account", return_value=account
        ):
            status, result = self.request(
                "POST", "/orders/593/approve", token="tok-employer"
            )

        self.assertTrue(drifted, result)
        self.assertEqual(status, 409, result)
        self.assertEqual(self.payment_create.call_count, 2)
        with self.api.get_db() as db:
            state = db.execute(
                """SELECT o.status,first.status,second.status,second.amount,
                          h.status,h.amount,a.status,a.base_amount_cents
                   FROM orders o
                   JOIN milestones first ON first.id=694
                   JOIN milestones second ON second.id=695
                   JOIN escrow_holds h ON h.milestone_id=695
                   JOIN funding_attempts a ON a.id=h.funding_attempt_id
                   WHERE o.id=593"""
            ).fetchone()
        self.assertEqual(
            tuple(state),
            ("submitted", "approved", "pending", 99, "held", 15, "committed", 1500),
        )

    def test_approve_retry_recovers_next_milestone_after_funding_commit_crash(self):
        with self.api.get_db() as db:
            db.execute(
                "INSERT INTO orders (id,type,worker_id,employer_id,status,total_amount) VALUES (592,'service_order',1,2,'pending',25)"
            )
            db.execute(
                "INSERT INTO milestones (id,order_id,title,amount,sequence,status) VALUES (692,592,'First',10,1,'pending')"
            )
            db.execute(
                "INSERT INTO milestones (id,order_id,title,amount,sequence,status) VALUES (693,592,'Second',15,2,'pending')"
            )
            db.execute("UPDATE worker_profiles SET payout_account_id='acct_live_worker' WHERE user_id=1")
            db.commit()

        status, funded = self.request("POST", "/payments/fund-escrow", payload={
            "order_id": 592,
            "milestone_id": 692,
            "amount": "10.00",
        })
        self.assertEqual(status, 200, funded)
        status, submitted = self.request(
            "POST", "/orders/592/submit", token="tok-worker", payload={"notes": "First delivered"}
        )
        self.assertEqual(status, 200, submitted)

        self.api.stripe.Transfer = type("Transfer", (), {
            "create": mock.Mock(side_effect=lambda **kwargs: exact_transfer("tr_mock_release", kwargs))
        })
        account = {"details_submitted": True, "payouts_enabled": True, "charges_enabled": True, "capabilities": {"transfers": "active"}}
        real_audit = self.api.audit

        def crash_after_next_funding(*args, **kwargs):
            if len(args) > 2 and args[2] == "approve_order":
                raise RuntimeError("injected after next funding commit")
            return real_audit(*args, **kwargs)

        with mock.patch.object(self.api, "retrieve_live_connect_account", return_value=account), mock.patch.object(
            self.api, "audit", side_effect=crash_after_next_funding
        ):
            status, _ = self.request("POST", "/orders/592/approve", token="tok-employer")
        self.assertEqual(status, 500)
        self.assertEqual(self.payment_create.call_count, 2)

        with self.api.get_db() as db:
            first = db.execute("SELECT status FROM milestones WHERE id=692").fetchone()[0]
            second = db.execute(
                "SELECT status,escrow_payment_id FROM milestones WHERE id=693"
            ).fetchone()
            attempt = db.execute(
                "SELECT * FROM funding_attempts WHERE operation_key='milestone:693'"
            ).fetchone()
            hold = db.execute("SELECT * FROM escrow_holds WHERE milestone_id=693").fetchone()
            order_status = db.execute("SELECT status FROM orders WHERE id=592").fetchone()[0]
        self.assertEqual(first, "approved")
        self.assertEqual(tuple(second), ("pending", None))
        self.assertEqual(attempt["status"], "committed")
        self.assertEqual(hold["status"], "held")
        self.assertEqual(order_status, "submitted")

        with self.api.get_db() as db:
            db.execute("UPDATE milestones SET amount=16 WHERE id=693")
            db.commit()
        status, amount_conflict = self.request("POST", "/orders/592/approve", token="tok-employer")
        self.assertEqual(status, 409, amount_conflict)
        self.assertEqual(self.payment_create.call_count, 2)
        with self.api.get_db() as db:
            self.assertEqual(db.execute("SELECT status FROM orders WHERE id=592").fetchone()[0], "submitted")
            self.assertEqual(db.execute("SELECT status FROM milestones WHERE id=693").fetchone()[0], "pending")
            db.execute("UPDATE milestones SET amount=15 WHERE id=693")
            db.execute("UPDATE funding_attempts SET currency='eur' WHERE operation_key='milestone:693'")
            db.commit()
        status, currency_conflict = self.request("POST", "/orders/592/approve", token="tok-employer")
        self.assertEqual(status, 409, currency_conflict)
        self.assertEqual(self.payment_create.call_count, 2)
        with self.api.get_db() as db:
            self.assertEqual(db.execute("SELECT status FROM orders WHERE id=592").fetchone()[0], "submitted")
            self.assertEqual(db.execute("SELECT status FROM milestones WHERE id=693").fetchone()[0], "pending")
            db.execute("UPDATE funding_attempts SET currency='usd', error_code='processor_intent_conflict' WHERE operation_key='milestone:693'")
            db.commit()
        status, evidence_conflict = self.request("POST", "/orders/592/approve", token="tok-employer")
        self.assertEqual(status, 409, evidence_conflict)
        self.assertEqual(self.payment_create.call_count, 2)
        with self.api.get_db() as db:
            self.assertEqual(db.execute("SELECT status FROM orders WHERE id=592").fetchone()[0], "submitted")
            self.assertEqual(db.execute("SELECT status FROM milestones WHERE id=693").fetchone()[0], "pending")
            db.execute("UPDATE funding_attempts SET error_code=NULL WHERE operation_key='milestone:693'")
            db.commit()

        with self.api.get_db() as db:
            db.execute(
                """INSERT INTO escrow_holds
                   (order_id,milestone_id,amount,status,stripe_payment_intent_id)
                   VALUES (592,693,15,'held','pi_conflicting_legacy_hold')"""
            )
            db.commit()
        status, conflict = self.request("POST", "/orders/592/approve", token="tok-employer")
        self.assertEqual(status, 409, conflict)
        self.assertEqual(self.payment_create.call_count, 2)
        with self.api.get_db() as db:
            self.assertEqual(db.execute("SELECT status FROM orders WHERE id=592").fetchone()[0], "submitted")
            self.assertEqual(db.execute("SELECT status FROM milestones WHERE id=693").fetchone()[0], "pending")
            db.execute(
                "DELETE FROM escrow_holds WHERE stripe_payment_intent_id='pi_conflicting_legacy_hold'"
            )
            db.commit()

        with mock.patch.object(self.api, "retrieve_live_connect_account", return_value=account):
            status, replay = self.request("POST", "/orders/592/approve", token="tok-employer")
        self.assertEqual(status, 200, replay)
        self.assertTrue(replay["recovered_funding"])
        self.assertEqual(self.payment_create.call_count, 2)
        with self.api.get_db() as db:
            self.assertEqual(db.execute("SELECT status FROM orders WHERE id=592").fetchone()[0], "in_progress")
            second = db.execute(
                "SELECT status,escrow_payment_id FROM milestones WHERE id=693"
            ).fetchone()
            self.assertEqual(tuple(second), ("in_progress", hold["stripe_payment_intent_id"]))
            self.assertEqual(db.execute("SELECT COUNT(*) FROM escrow_holds WHERE milestone_id=693").fetchone()[0], 1)
            self.assertEqual(
                db.execute(
                    """SELECT COUNT(*) FROM payout_release_attempts
                       WHERE order_id=592 AND status='committed'
                         AND lifecycle_status='pending'"""
                ).fetchone()[0],
                0,
            )
            self.assertEqual(
                db.execute(
                    """SELECT COUNT(*) FROM payout_release_attempts
                       WHERE order_id=592 AND lifecycle_status<>'completed'"""
                ).fetchone()[0],
                0,
            )
            self.assertEqual(
                db.execute(
                    """SELECT COUNT(*) FROM funding_attempts
                       WHERE order_id=592
                         AND status IN ('prepared','unknown','processor_succeeded')"""
                ).fetchone()[0],
                0,
            )

    def test_manual_funding_replay_recovers_committed_hold_from_pending_lifecycle(self):
        with self.api.get_db() as db:
            db.execute("INSERT INTO orders (id,type,worker_id,employer_id,status,total_amount) VALUES (588,'service_order',1,2,'pending',25)")
            db.execute("INSERT INTO milestones (id,order_id,title,amount,sequence,status) VALUES (688,588,'First',25,1,'pending')")
            db.commit()

        with mock.patch.object(self.api, "audit", side_effect=RuntimeError("after funding commit")):
            status, _ = self.request("POST", "/payments/fund-escrow", payload={
                "order_id": 588,
                "milestone_id": 688,
                "amount": "25.00",
            })
        self.assertEqual(status, 500)
        with self.api.get_db() as db:
            attempt = db.execute("SELECT * FROM funding_attempts WHERE operation_key='milestone:688'").fetchone()
            hold = db.execute("SELECT * FROM escrow_holds WHERE order_id=588").fetchone()
            self.assertEqual(attempt["status"], "committed")
            self.assertEqual(hold["status"], "held")
            self.assertEqual(hold["funding_attempt_id"], attempt["id"])
            self.assertEqual(db.execute("SELECT status FROM orders WHERE id=588").fetchone()[0], "pending")
            self.assertEqual(db.execute("SELECT status FROM milestones WHERE id=688").fetchone()[0], "pending")

        status, replay = self.request("POST", "/payments/fund-escrow", payload={
            "order_id": 588,
            "milestone_id": 688,
            "amount": "25.00",
        })
        self.assertEqual(status, 200, replay)
        self.assertTrue(replay["idempotent_replay"])
        self.assertEqual(self.payment_create.call_count, 1)
        with self.api.get_db() as db:
            self.assertEqual(db.execute("SELECT status FROM orders WHERE id=588").fetchone()[0], "in_progress")
            milestone = db.execute("SELECT status,escrow_payment_id,funded_at FROM milestones WHERE id=688").fetchone()
            self.assertEqual(milestone["status"], "in_progress")
            self.assertEqual(milestone["escrow_payment_id"], hold["stripe_payment_intent_id"])
            self.assertTrue(milestone["funded_at"])

    def test_exact_legacy_first_milestone_replay_normalizes_lifecycle_without_recharging(self):
        with self.api.get_db() as db:
            db.execute("INSERT INTO orders (id,type,worker_id,employer_id,status,total_amount) VALUES (589,'service_order',1,2,'pending',25)")
            db.execute("INSERT INTO milestones (id,order_id,title,amount,sequence,status) VALUES (689,589,'First',25,1,'funded')")
            db.execute(
                """INSERT INTO escrow_holds
                   (order_id,milestone_id,amount,base_amount_cents,status,stripe_payment_intent_id,funding_identity)
                   VALUES (589,689,25,2500,'held','pi_legacy_exact','milestone:689')"""
            )
            db.commit()

        status, result = self.request("POST", "/payments/fund-escrow", payload={
            "order_id": 589,
            "milestone_id": 689,
            "amount": "25.00",
        })

        self.assertEqual(status, 200, result)
        self.assertTrue(result["idempotent_replay"])
        self.payment_create.assert_not_called()
        with self.api.get_db() as db:
            self.assertEqual(db.execute("SELECT status FROM orders WHERE id=589").fetchone()[0], "in_progress")
            self.assertEqual(db.execute("SELECT status FROM milestones WHERE id=689").fetchone()[0], "in_progress")
            self.assertEqual(db.execute("SELECT COUNT(*) FROM escrow_holds WHERE order_id=589").fetchone()[0], 1)

    def test_legacy_prefunded_next_milestone_fails_closed_instead_of_completing_order(self):
        with self.api.get_db() as db:
            db.execute("INSERT INTO orders (id,type,worker_id,employer_id,status,total_amount) VALUES (590,'service_order',1,2,'submitted',25)")
            db.execute("INSERT INTO milestones (id,order_id,title,amount,sequence,status) VALUES (690,590,'First',10,1,'submitted')")
            db.execute("INSERT INTO milestones (id,order_id,title,amount,sequence,status) VALUES (691,590,'Legacy prefunded',15,2,'funded')")
            db.execute("INSERT INTO escrow_holds (order_id,milestone_id,amount,status,stripe_payment_intent_id) VALUES (590,690,10,'held','pi_current')")
            db.execute("INSERT INTO escrow_holds (order_id,milestone_id,amount,status,stripe_payment_intent_id) VALUES (590,691,15,'held','pi_legacy_prefunded')")
            db.execute("UPDATE worker_profiles SET payout_account_id='acct_live_worker' WHERE user_id=1")
            self._bind_verified_funding(db, 590, 690, 10, "pi_current")
            db.commit()

        self.api.stripe.Transfer = type("Transfer", (), {
            "create": mock.Mock(side_effect=lambda **kwargs: exact_transfer("tr_mock_release", kwargs))
        })
        account = {"details_submitted": True, "payouts_enabled": True, "charges_enabled": True, "capabilities": {"transfers": "active"}}
        with mock.patch.object(self.api, "retrieve_live_connect_account", return_value=account):
            status, result = self.request("POST", "/orders/590/approve", token="tok-employer")
        self.assertEqual(status, 200, result)
        self.payment_create.assert_not_called()
        with self.api.get_db() as db:
            self.assertEqual(db.execute("SELECT status FROM orders WHERE id=590").fetchone()[0], "disputed")
            self.assertEqual(db.execute("SELECT status FROM milestones WHERE id=690").fetchone()[0], "approved")
            self.assertEqual(db.execute("SELECT status FROM milestones WHERE id=691").fetchone()[0], "funded")
            self.assertEqual(db.execute("SELECT status FROM escrow_holds WHERE milestone_id=691").fetchone()[0], "held")

    def test_approval_never_completes_with_nonapproved_milestone_or_held_escrow(self):
        with self.api.get_db() as db:
            db.execute("INSERT INTO orders (id,type,worker_id,employer_id,status,total_amount) VALUES (591,'service_order',1,2,'submitted',25)")
            db.execute("INSERT INTO milestones (id,order_id,title,amount,sequence,status) VALUES (693,591,'Current',10,1,'submitted')")
            db.execute("INSERT INTO milestones (id,order_id,title,amount,sequence,status) VALUES (694,591,'Unexpected active',15,2,'in_progress')")
            db.execute("INSERT INTO escrow_holds (order_id,milestone_id,amount,status,stripe_payment_intent_id) VALUES (591,693,10,'held','pi_current_591')")
            db.execute("INSERT INTO escrow_holds (order_id,milestone_id,amount,status,stripe_payment_intent_id) VALUES (591,694,15,'held','pi_other_591')")
            db.commit()

        def release_current(db, order_id, milestone_id, amount, worker_id):
            db.execute(
                "UPDATE escrow_holds SET status='released', released_at=datetime('now') WHERE order_id=? AND milestone_id=?",
                [order_id, milestone_id],
            )
            return amount, 0.10

        with mock.patch.object(self.api, "release_escrow_to_worker", side_effect=release_current), mock.patch.object(
            self.api, "flush_transactional_notification_emails"
        ):
            status, result = self.request("POST", "/orders/591/approve", token="tok-employer")

        self.assertEqual(status, 200, result)
        with self.api.get_db() as db:
            self.assertEqual(db.execute("SELECT status FROM orders WHERE id=591").fetchone()[0], "disputed")
            self.assertEqual(db.execute("SELECT status FROM milestones WHERE id=693").fetchone()[0], "approved")
            self.assertEqual(db.execute("SELECT status FROM milestones WHERE id=694").fetchone()[0], "in_progress")
            self.assertEqual(db.execute("SELECT status FROM escrow_holds WHERE milestone_id=694").fetchone()[0], "held")
            self.assertEqual(db.execute("SELECT total_orders_completed FROM worker_profiles WHERE user_id=1").fetchone()[0], 0)
            self.assertEqual(db.execute("SELECT total_orders FROM employer_profiles WHERE user_id=2").fetchone()[0], 0)

    def test_manual_funding_rejects_terminal_disputed_and_noninitial_orders(self):
        with self.api.get_db() as db:
            for offset, order_status in enumerate(("completed", "canceled", "disputed", "in_progress")):
                order_id = 560 + offset
                milestone_id = 660 + offset
                db.execute(
                    "INSERT INTO orders (id,type,worker_id,employer_id,status,total_amount) VALUES (?,'service_order',1,2,?,25)",
                    [order_id, order_status],
                )
                db.execute(
                    "INSERT INTO milestones (id,order_id,title,amount,sequence,status) VALUES (?,?,'Blocked',25,1,'pending')",
                    [milestone_id, order_id],
                )
            db.commit()

        for offset, order_status in enumerate(("completed", "canceled", "disputed", "in_progress")):
            with self.subTest(order_status=order_status):
                status, result = self.request("POST", "/payments/fund-escrow", payload={
                    "order_id": 560 + offset,
                    "milestone_id": 660 + offset,
                })
                self.assertEqual(status, 409, result)
                self.payment_create.assert_not_called()
        with self.api.get_db() as db:
            self.assertEqual(db.execute("SELECT COUNT(*) FROM escrow_holds WHERE order_id BETWEEN 560 AND 563").fetchone()[0], 0)
            self.assertEqual(
                [row[0] for row in db.execute("SELECT status FROM milestones WHERE order_id BETWEEN 560 AND 563 ORDER BY order_id")],
                ["pending", "pending", "pending", "pending"],
            )

    def test_concurrent_same_milestone_funding_creates_one_processor_operation(self):
        with self.api.get_db() as db:
            db.execute("INSERT INTO orders (id,type,worker_id,employer_id,status,total_amount) VALUES (580,'service_order',1,2,'pending',25)")
            db.execute("INSERT INTO milestones (id,order_id,title,amount,sequence,status) VALUES (680,580,'First',25,1,'pending')")
            db.commit()

        requests_ready = threading.Barrier(9)
        processor_started = threading.Event()
        release_processor = threading.Event()

        def delayed_create(**kwargs):
            processor_started.set()
            if not release_processor.wait(5):
                raise TimeoutError("concurrency probe did not release processor")
            return type("PaymentIntentResult", (), {
                "id": "pi_concurrent_once",
                "status": "succeeded",
                "amount": kwargs["amount"],
                "amount_received": kwargs["amount"],
                "currency": kwargs["currency"],
                "metadata": kwargs["metadata"],
            })()

        self.payment_create.side_effect = delayed_create
        original_json_response = self.api.json_response
        original_error_response = self.api.error_response
        self.api.json_response = lambda data, status=200: (status, data)
        self.api.error_response = lambda message, status=400: (status, {"error": message})

        def fund_once():
            requests_ready.wait(timeout=5)
            body = json.dumps({"order_id": 580, "milestone_id": 680, "amount": "25.00"})
            ctx = self.api._request_ctx
            ctx.request_method = "POST"
            ctx.path_info = "/payments/fund-escrow"
            ctx.query_string = ""
            ctx.http_authorization = "Bearer " + "tok-employer"
            ctx.http_x_api_key = ""
            ctx.stdin_data = body
            ctx.content_type = "application/json"
            ctx.content_length = str(len(body.encode()))
            ctx.remote_addr = "127.0.0.1"
            db = self.api.get_db()
            try:
                return self.api._handle_routes(db)
            finally:
                db.close()

        try:
            with ThreadPoolExecutor(max_workers=8) as pool:
                futures = [pool.submit(fund_once) for _ in range(8)]
                requests_ready.wait(timeout=5)
                self.assertTrue(processor_started.wait(2))
                release_processor.set()
                results = [future.result(timeout=5) for future in futures]
        finally:
            release_processor.set()
            self.api.json_response = original_json_response
            self.api.error_response = original_error_response

        statuses = [status for status, _ in results]
        self.assertTrue(all(status in {200, 409} for status in statuses), results)
        self.assertIn(200, statuses)
        self.assertEqual(self.payment_create.call_count, 1)
        self.assertEqual(sum(result.get("mode") == "live" for _, result in results), 1)

        status, replay = self.request("POST", "/payments/fund-escrow", payload={
            "order_id": 580,
            "milestone_id": 680,
            "amount": "25.00",
        })
        self.assertEqual(status, 200, replay)
        self.assertEqual(replay["mode"], "replayed")
        self.assertEqual(self.payment_create.call_count, 1)
        with self.api.get_db() as db:
            self.assertEqual(db.execute("SELECT COUNT(*) FROM escrow_holds WHERE order_id=580").fetchone()[0], 1)
            self.assertEqual(db.execute("SELECT status FROM milestones WHERE id=680").fetchone()[0], "in_progress")
            self.assertEqual(db.execute("SELECT status FROM orders WHERE id=580").fetchone()[0], "in_progress")

    def test_replay_accepts_normalization_completed_during_processor_lock_gap(self):
        with self.api.get_db() as db:
            db.execute("INSERT INTO orders (id,type,worker_id,employer_id,status,total_amount) VALUES (581,'service_order',1,2,'pending',25)")
            db.execute("INSERT INTO milestones (id,order_id,title,amount,sequence,status) VALUES (681,581,'First',25,1,'pending')")
            db.commit()

        payload = {"order_id": 581, "milestone_id": 681, "amount": "25.00"}
        status, funded = self.request("POST", "/payments/fund-escrow", payload=payload)
        self.assertEqual(status, 200, funded)
        self.assertEqual(self.payment_create.call_count, 1)

        with self.api.get_db() as db:
            db.execute("UPDATE orders SET status='pending' WHERE id=581")
            db.execute("UPDATE milestones SET status='pending' WHERE id=681")
            db.commit()

        original_fund = self.api.fund_escrow_stripe

        def replay_then_competing_normalization(*args, **kwargs):
            result = original_fund(*args, **kwargs)
            with self.api.get_db() as racing_db:
                racing_db.execute("BEGIN IMMEDIATE")
                racing_db.execute(
                    "UPDATE milestones SET status='in_progress' WHERE id=681 AND status='pending'"
                )
                racing_db.execute(
                    "UPDATE orders SET status='in_progress' WHERE id=581 AND status='pending'"
                )
                racing_db.commit()
            return result

        with mock.patch.object(
            self.api,
            "fund_escrow_stripe",
            side_effect=replay_then_competing_normalization,
        ):
            status, replay = self.request(
                "POST", "/payments/fund-escrow", payload=payload
            )

        self.assertEqual(status, 200, replay)
        self.assertEqual(replay["mode"], "replayed")
        self.assertEqual(self.payment_create.call_count, 1)
        with self.api.get_db() as db:
            self.assertEqual(
                db.execute("SELECT status FROM milestones WHERE id=681").fetchone()[0],
                "in_progress",
            )
            self.assertEqual(
                db.execute("SELECT status FROM orders WHERE id=581").fetchone()[0],
                "in_progress",
            )

    def test_mcp_checkout_requires_and_forwards_stable_operation_identity(self):
        backend_dir = pathlib.Path(__file__).resolve().parent
        root_source = (backend_dir / "mcp_server.py").read_text()
        package_source = (backend_dir / "mcp-package/mcp_server.py").read_text()
        self.assertEqual(root_source, package_source)
        expected_key = "mcp-service-operation-0001"
        for index, relative in enumerate(("mcp_server.py", "mcp-package/mcp_server.py")):
            path = backend_dir / relative
            spec = importlib.util.spec_from_file_location(f"mcp_server_under_test_{index}", path)
            if spec is None or spec.loader is None:
                self.fail(f"Could not load {path}")
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            hire_tool = next(tool for tool in module.TOOLS if tool["name"] == "hire_worker")
            self.assertIn("idempotency_key", hire_tool["inputSchema"]["required"])
            self.assertEqual(
                hire_tool["inputSchema"]["properties"]["idempotency_key"]["pattern"],
                "^[A-Za-z0-9._:-]{16,128}$",
            )
            self.assertEqual(hire_tool["inputSchema"]["properties"]["budget_amount"]["type"], "string")
            calls = []

            def fake_api(method, route, body=None, params=None):
                calls.append((method, route, body))
                if method == "GET":
                    return {"id": 1, "worker_id": 1, "title": "Custom QA", "price": 25}
                return {"id": 77, "status": "pending"}

            with mock.patch.object(module, "api_request", side_effect=fake_api):
                missing_key = module.handle_hire_worker({
                    "service_id": 1,
                    "budget_amount": "25.00",
                })
                invalid = module.handle_hire_worker({
                    "service_id": 1,
                    "budget_amount": 25.0,
                    "idempotency_key": expected_key,
                })
                invalid_key = module.handle_hire_worker({
                    "service_id": 1,
                    "budget_amount": "25.00",
                    "idempotency_key": "invalid key with spaces",
                })
                invalid_service = module.handle_hire_worker({
                    "service_id": 1.0,
                    "budget_amount": "25.00",
                    "idempotency_key": expected_key,
                })
                malformed = [
                    module.handle_hire_worker({
                        "service_id": 1,
                        "budget_amount": value,
                        "idempotency_key": expected_key,
                    })
                    for value in ("25.550", "1e2", " 25.00", "１２.００", "9" * 129)
                ]
            self.assertIn("idempotency_key", missing_key[0]["text"])
            self.assertIn("idempotency_key", invalid_key[0]["text"])
            self.assertIn("service_id", invalid_service[0]["text"])
            self.assertIn("canonical USD string", invalid[0]["text"])
            for response in malformed:
                self.assertIn("canonical USD string", response[0]["text"])
            self.assertEqual(calls, [])

            with mock.patch.object(module, "api_request", return_value=[]):
                malformed_service = module.handle_hire_worker({
                    "service_id": 1,
                    "idempotency_key": expected_key,
                })
            self.assertIn("invalid API response", malformed_service[0]["text"])

            args = {
                "service_id": 1,
                "requirements": "Check flow",
                "budget_amount": "25.00",
                "idempotency_key": expected_key,
            }
            with mock.patch.object(module, "api_request", side_effect=fake_api):
                module.handle_hire_worker(args)
                module.handle_hire_worker(args)
            posts = [call for call in calls if call[0] == "POST"]
            self.assertEqual(len(posts), 2)
            self.assertEqual(
                [call[2]["idempotency_key"] for call in posts],
                [expected_key, expected_key],
            )
            self.assertEqual(
                [call[2]["amount"] for call in posts],
                ["25.00", "25.00"],
            )

            malformed_calls = []

            def malformed_checkout(method, route, body=None, params=None):
                malformed_calls.append((method, route, body))
                if method == "GET":
                    return {"id": 1, "worker_id": 1, "title": "Custom QA", "price": 25}
                return []

            with mock.patch.object(module, "api_request", side_effect=malformed_checkout):
                malformed_order = module.handle_hire_worker(args)
            self.assertIn("invalid API response", malformed_order[0]["text"])
            self.assertIn("same idempotency_key", malformed_order[0]["text"])
            self.assertEqual([call[2]["idempotency_key"] for call in malformed_calls if call[0] == "POST"], [expected_key])

            def ambiguous_checkout(method, route, body=None, params=None):
                if method == "GET":
                    return {"id": 1, "worker_id": 1, "title": "Custom QA", "price": 25}
                return {"error": "upstream response lost"}

            with mock.patch.object(module, "api_request", side_effect=ambiguous_checkout):
                ambiguous_order = module.handle_hire_worker(args)
            self.assertIn("same idempotency_key", ambiguous_order[0]["text"])

            calls.clear()
            with mock.patch.object(module, "api_request", side_effect=fake_api):
                module.handle_hire_worker({
                    "service_id": 1,
                    "requirements": "Use listed amount",
                    "idempotency_key": "mcp-service-operation-omit-amount",
                })
            posts = [call for call in calls if call[0] == "POST"]
            self.assertEqual(len(posts), 1)
            self.assertNotIn("amount", posts[0][2])

    def test_mcp_omitted_budget_uses_backend_authoritative_fixed_price(self):
        with self.api.get_db() as db:
            db.execute(
                """INSERT INTO services
                   (id,worker_id,title,description,category,pricing_type,price,status)
                   VALUES (2,1,'Fixed QA','Authoritative fixed price','testing','fixed',25,'active')"""
            )
            db.commit()

        path = pathlib.Path(__file__).resolve().parent / "mcp_server.py"
        spec = importlib.util.spec_from_file_location("mcp_server_fixed_price_e2e", path)
        if spec is None or spec.loader is None:
            self.fail(f"Could not load {path}")
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)

        def local_api(method, route, body=None, params=None):
            status, payload = self.request(method, route, token="tok-employer", payload=body)
            if status >= 400:
                return {"error": payload.get("error", f"HTTP {status}")}
            return payload

        with mock.patch.object(module, "api_request", side_effect=local_api):
            result = module.handle_hire_worker({
                "service_id": 2,
                "requirements": "Use listed amount",
                "idempotency_key": "mcp-fixed-price-omitted-0001",
            })

        self.assertIn("Worker hired successfully", result[0]["text"])
        self.payment_create.assert_called_once()
        self.assertEqual(self.payment_create.call_args.kwargs["amount"], 2600)
        with self.api.get_db() as db:
            order = db.execute(
                "SELECT total_amount,creation_idempotency_key,status FROM orders WHERE service_id=2"
            ).fetchone()
            self.assertEqual(tuple(order), (25.0, "mcp-fixed-price-omitted-0001", "in_progress"))
            self.assertEqual(db.execute("SELECT COUNT(*) FROM escrow_holds WHERE order_id IN (SELECT id FROM orders WHERE service_id=2)").fetchone()[0], 1)

    def test_init_db_closes_connection_when_migration_step_fails(self):
        captured = []

        def fail_after_write(db):
            captured.append(db)
            db.execute("INSERT INTO audit_log (action) VALUES ('forced_failure')")
            raise RuntimeError("forced migration failure")

        with mock.patch.object(self.api, "ensure_one_job_hire_enforcement", side_effect=fail_after_write):
            with self.assertRaisesRegex(RuntimeError, "forced migration failure"):
                self.api.init_db()

        with self.assertRaises(sqlite3.ProgrammingError):
            captured[0].execute("SELECT 1")
        with self.api.get_db() as db:
            db.execute("INSERT INTO audit_log (action) VALUES ('immediate_retry_write')")
            db.commit()

    def test_hourly_hire_rejects_fractional_weekly_cap_before_stripe(self):
        status, result = self.request("POST", "/jobs/4/hire", payload={
            "application_id": 18,
            "weekly_hour_cap": 6.1,
        })
        self.assertEqual(status, 503, result)
        self.assertIn("Task 4", result["error"])
        self.payment_create.assert_not_called()

    def test_hourly_hire_rejects_precision_hidden_fraction_before_stripe(self):
        status, result = self.request("POST", "/jobs/4/hire", payload={
            "application_id": 18,
            "weekly_hour_cap": "6.0000000000000001",
        })
        self.assertEqual(status, 503, result)
        self.assertIn("Task 4", result["error"])
        self.payment_create.assert_not_called()

    def test_order_detail_reports_authoritative_funded_charge_not_contract_total(self):
        status, result = self.request("POST", "/jobs/5/hire", payload={
            "application_id": 19,
            "milestones": [
                {"description": "First", "amount": 10},
                {"description": "Second", "amount": 15.55},
            ],
        })
        self.assertEqual(status, 201, result)
        order_id = result["id"]

        status, detail = self.request("GET", f"/orders/{order_id}")
        self.assertEqual(status, 200, detail)
        self.assertEqual(detail["total_amount"], 25.55)
        self.assertEqual(detail["funding_summary"], {
            "base_cents": 1000,
            "platform_fee_cents": 10,
            "processing_fee_cents": 30,
            "charged_total_cents": 1040,
            "funded_amount_available": True,
            "charge_amount_available": True,
            "record_count": 1,
        })

    def test_legacy_funding_rows_never_reconstruct_historical_processor_charge(self):
        with self.api.get_db() as db:
            db.execute("INSERT INTO orders (id,type,worker_id,employer_id,status,total_amount) VALUES (700,'service_order',1,2,'in_progress',25.55)")
            db.execute("INSERT INTO escrow_holds (order_id,amount,status,stripe_payment_intent_id) VALUES (700,25.55,'held','pi_legacy')")
            db.execute("INSERT INTO orders (id,type,worker_id,employer_id,status,total_amount) VALUES (701,'service_order',1,2,'in_progress',25.555)")
            db.execute("INSERT INTO escrow_holds (order_id,amount,status,stripe_payment_intent_id) VALUES (701,25.555,'held','pi_legacy_subcent')")
            db.commit()

        status, legacy = self.request("GET", "/orders/700")
        self.assertEqual(status, 200, legacy)
        self.assertEqual(legacy["funding_summary"]["base_cents"], 2555)
        self.assertTrue(legacy["funding_summary"]["funded_amount_available"])
        self.assertFalse(legacy["funding_summary"]["charge_amount_available"])
        self.assertIsNone(legacy["funding_summary"]["charged_total_cents"])
        self.assertIsNone(legacy["funding_summary"]["platform_fee_cents"])
        self.assertIsNone(legacy["funding_summary"]["processing_fee_cents"])

        status, subcent = self.request("GET", "/orders/701")
        self.assertEqual(status, 200, subcent)
        self.assertFalse(subcent["funding_summary"]["funded_amount_available"])
        self.assertFalse(subcent["funding_summary"]["charge_amount_available"])
        self.assertIsNone(subcent["funding_summary"]["base_cents"])

    def test_stripe_charge_uses_same_component_rounded_cent_policy_as_ui(self):
        scenarios = [
            (5, 19, {"milestones": [{"description": "Delivery", "amount": 25.55}]}, 2658),
            (6, 20, {"weekly_hour_cap": 1}, 2658),
            (7, 21, {"milestones": [{"description": "Delivery", "amount": 0.01}]}, 3),
        ]
        for job_id, application_id, extra, expected_charge in scenarios:
            calls_before = self.payment_create.call_count
            status, result = self.request(
                "POST",
                f"/jobs/{job_id}/hire",
                payload={"application_id": application_id, **extra},
            )
            if job_id == 6:
                self.assertEqual(status, 503, result)
                self.assertIn("Task 4", result["error"])
                self.assertEqual(self.payment_create.call_count, calls_before)
            else:
                self.assertEqual(status, 201, result)
                self.assertEqual(self.payment_create.call_args.kwargs["amount"], expected_charge)

    def test_removed_hourly_settlement_cannot_be_reenabled_or_mutate_lifecycle(self):
        self.seed_hourly_order()
        # A stale deployment/environment monkeypatch must not resurrect the removed
        # Task-4 settlement implementation.
        self.api.HOURLY_SETTLEMENT_ENABLED = True
        self.api.PRODUCTION_MODE = True
        self.api.STRIPE_SECRET_KEY = "configured-test-key"
        transfer_create = mock.Mock(return_value=type("Transfer", (), {"id": "tr_forbidden"})())
        retrieve_account = mock.Mock(return_value=type(
            "Account", (), {"payouts_enabled": True, "details_submitted": True}
        )())
        renewal_funding = mock.Mock(return_value=("pi_forbidden", "live"))
        self.api.stripe.Transfer = type("Transfer", (), {"create": transfer_create})
        self.api.retrieve_live_connect_account = retrieve_account
        self.api.fund_escrow_stripe = renewal_funding
        with self.api.get_db() as db:
            db.execute(
                "UPDATE worker_profiles SET payout_account_id='acct_live_forbidden' WHERE user_id=1"
            )
            db.commit()
            before = {
                "order": tuple(db.execute("SELECT status,completed_at FROM orders WHERE id=88").fetchone()),
                "job": tuple(db.execute("SELECT status FROM jobs WHERE id=4").fetchone()),
                "contract": tuple(db.execute("SELECT status,current_week_escrow_payment_id FROM hourly_contracts WHERE order_id=88").fetchone()),
                "entry": tuple(db.execute("SELECT status FROM time_entries").fetchone()),
                "hold": tuple(db.execute("SELECT status,released_at,stripe_transfer_id FROM escrow_holds WHERE order_id=88").fetchone()),
                "revenue": db.execute("SELECT COUNT(*) FROM platform_revenue WHERE order_id=88").fetchone()[0],
                "payouts": db.execute("SELECT COUNT(*) FROM payout_transfers WHERE order_id=88").fetchone()[0],
                "audits": db.execute("SELECT COUNT(*) FROM audit_log").fetchone()[0],
            }

        for path, token, payload in (
            ("/orders/88/approve-hours", "tok-employer", {"week_of": "2026-07-06"}),
            ("/orders/88/end-contract", "tok-worker", {"reason": "done"}),
        ):
            status, result = self.request("POST", path, token=token, payload=payload)
            self.assertEqual(status, 503, result)
            self.assertIn("Task 4", result["error"])

        transfer_create.assert_not_called()
        retrieve_account.assert_not_called()
        renewal_funding.assert_not_called()
        with self.api.get_db() as db:
            after = {
                "order": tuple(db.execute("SELECT status,completed_at FROM orders WHERE id=88").fetchone()),
                "job": tuple(db.execute("SELECT status FROM jobs WHERE id=4").fetchone()),
                "contract": tuple(db.execute("SELECT status,current_week_escrow_payment_id FROM hourly_contracts WHERE order_id=88").fetchone()),
                "entry": tuple(db.execute("SELECT status FROM time_entries").fetchone()),
                "hold": tuple(db.execute("SELECT status,released_at,stripe_transfer_id FROM escrow_holds WHERE order_id=88").fetchone()),
                "revenue": db.execute("SELECT COUNT(*) FROM platform_revenue WHERE order_id=88").fetchone()[0],
                "payouts": db.execute("SELECT COUNT(*) FROM payout_transfers WHERE order_id=88").fetchone()[0],
                "audits": db.execute("SELECT COUNT(*) FROM audit_log").fetchone()[0],
            }
        self.assertEqual(after, before)

    def test_hourly_settlement_endpoints_fail_closed_by_default_without_mutation(self):
        self.seed_hourly_order()
        self.api.HOURLY_SETTLEMENT_ENABLED = False

        status, result = self.request(
            "POST", "/orders/88/approve-hours", payload={"week_of": "2026-07-06"}
        )
        self.assertEqual(status, 503, result)
        self.assertIn("Task 4", result["error"])
        status, result = self.request("POST", "/orders/88/end-contract", token="tok-worker", payload={"reason": "done"})
        self.assertEqual(status, 503, result)
        self.assertIn("Task 4", result["error"])

        db = self.api.get_db()
        try:
            self.assertEqual(db.execute("SELECT status FROM orders WHERE id=88").fetchone()[0], "in_progress")
            self.assertEqual(db.execute("SELECT status FROM jobs WHERE id=4").fetchone()[0], "hired")
            self.assertEqual(db.execute("SELECT status FROM hourly_contracts WHERE order_id=88").fetchone()[0], "active")
            self.assertEqual(db.execute("SELECT status FROM time_entries").fetchone()[0], "pending")
            self.assertEqual(db.execute("SELECT status FROM escrow_holds WHERE order_id=88").fetchone()[0], "held")
            self.assertEqual(db.execute("SELECT COUNT(*) FROM platform_revenue WHERE order_id=88").fetchone()[0], 0)
        finally:
            db.close()

    def test_stale_enabled_hourly_approval_flag_cannot_release_rounded_money(self):
        order_id = self.seed_hourly_order()
        self.api.HOURLY_SETTLEMENT_ENABLED = True
        self.api.PRODUCTION_MODE = False
        self.api.STRIPE_SECRET_KEY = ""
        with self.api.get_db() as db:
            db.execute("UPDATE hourly_contracts SET hourly_rate=? WHERE order_id=?", [25.55, order_id])
            db.execute(
                "UPDATE time_entries SET hours=?, description=? WHERE contract_id=(SELECT id FROM hourly_contracts WHERE order_id=?)",
                [1.5, "QA review", order_id],
            )
            db.commit()

        status, result = self.request(
            "POST", f"/orders/{order_id}/approve-hours", payload={"week_of": "2026-07-06"}, token="tok-employer"
        )
        self.assertEqual(status, 503, result)
        self.assertIn("Task 4", result["error"])
        with self.api.get_db() as db:
            self.assertEqual(db.execute("SELECT status FROM time_entries").fetchone()[0], "pending")
            self.assertEqual(
                db.execute("SELECT status FROM escrow_holds WHERE order_id=?", [order_id]).fetchone()[0],
                "held",
            )

    def test_order_list_exposes_hourly_contract_type_and_funding_summary(self):
        self.seed_hourly_order()
        status, result = self.request("GET", "/orders", token="tok-worker")
        self.assertEqual(status, 200, result)
        row = next(order for order in result["orders"] if order["id"] == 88)
        self.assertEqual(row["contract_type"], "hourly")
        self.assertEqual(row["hourly_rate"], 25)
        self.assertEqual(row["current_week_escrow_amount"], 1000)

    def test_stale_enabled_hourly_end_flag_cannot_synchronize_terminal_state(self):
        self.seed_hourly_order()
        self.api.HOURLY_SETTLEMENT_ENABLED = True
        status, result = self.request("POST", "/orders/88/end-contract", token="tok-worker", payload={"reason": "done"})
        self.assertEqual(status, 503, result)
        self.assertIn("Task 4", result["error"])

        db = self.api.get_db()
        try:
            self.assertEqual(db.execute("SELECT status FROM orders WHERE id=88").fetchone()[0], "in_progress")
            self.assertEqual(db.execute("SELECT status FROM jobs WHERE id=4").fetchone()[0], "hired")
            self.assertEqual(db.execute("SELECT status FROM hourly_contracts WHERE order_id=88").fetchone()[0], "active")
        finally:
            db.close()

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
        self.assertEqual(status, 200, result)
        self.assertTrue(result["idempotent_replay"])
        self.assertEqual(result["worker_id"], 1)
        self.assertEqual(self.payment_create.call_count, 1)

        status, result = self.request("POST", "/jobs/1/hire", payload={
            "application_id": 16,
            "milestones": [{"description": "Delivery", "amount": 25}],
        })
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
        self.assertEqual(status, 503, result)
        self.assertIn("Task 4", result["error"])
        self.api.stripe.PaymentIntent.create.assert_not_called()
        with self.api.get_db() as db:
            self.assertEqual(db.execute("SELECT COUNT(*) FROM orders WHERE job_id=4").fetchone()[0], 0)

    def test_hourly_hire_rejects_out_of_range_weekly_cap_before_stripe(self):
        status, result = self.request("POST", "/jobs/4/hire", payload={
            "application_id": 18,
            "weekly_hour_cap": 169,
        })
        self.assertEqual(status, 503, result)
        self.assertIn("Task 4", result["error"])
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

        transfer = mock.Mock(side_effect=lambda **kwargs: exact_transfer("tr_revision_release", kwargs))
        self.api.stripe.Transfer = type("Transfer", (), {"create": transfer})
        with self.api.get_db() as db:
            db.execute("UPDATE worker_profiles SET payout_account_id='acct_revision_worker' WHERE user_id=1")
            db.commit()
        account = {"details_submitted": True, "payouts_enabled": True, "charges_enabled": True,
                   "capabilities": {"transfers": "active"}}
        with mock.patch.object(self.api, "retrieve_live_connect_account", return_value=account), mock.patch.object(self.api, "flush_transactional_notification_emails"):
            status, result = self.request("POST", f"/orders/{order_id}/approve")
            self.assertEqual(status, 200, result)
            status, replay = self.request("POST", f"/orders/{order_id}/approve")
            self.assertEqual(status, 409, replay)
        transfer.assert_called_once()

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
