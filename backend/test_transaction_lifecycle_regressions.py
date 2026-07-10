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

    def test_clean_database_keeps_partial_unique_job_hire_index(self):
        with self.api.get_db() as db:
            indexes = {row[1]: row for row in db.execute("PRAGMA index_list('orders')").fetchall()}
        self.assertIn("idx_orders_one_job_hire", indexes)
        self.assertEqual(indexes["idx_orders_one_job_hire"][2], 1)
        self.assertEqual(indexes["idx_orders_one_job_hire"][4], 1)

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

    def test_service_order_retry_uses_client_operation_identity_across_full_rollback(self):
        payload = {"amount": "25.55", "idempotency_key": "service-order-retry-0001"}
        with mock.patch.object(self.api, "push_notification", side_effect=RuntimeError("after Stripe")):
            status, _ = self.request("POST", "/services/1/order", payload=payload)
        self.assertEqual(status, 500)
        first_kwargs = dict(self.payment_create.call_args.kwargs)
        first_key = first_kwargs["idempotency_key"]

        with self.api.get_db() as db:
            order_id = db.execute(
                "INSERT INTO orders (type,service_id,worker_id,employer_id,status,total_amount) VALUES ('service_order',1,1,2,'canceled',1)"
            ).lastrowid
            db.execute(
                "INSERT INTO milestones (order_id,title,amount,sequence,status) VALUES (?,'Canceled',1,1,'pending')",
                [order_id],
            )
            db.commit()

        status, result = self.request("POST", "/services/1/order", payload=payload)
        self.assertEqual(status, 201, result)
        self.assertEqual(self.payment_create.call_count, 2)
        second_kwargs = dict(self.payment_create.call_args.kwargs)
        self.assertEqual(first_key, second_kwargs["idempotency_key"])
        self.assertEqual(first_kwargs, second_kwargs)

    def test_service_order_response_loss_replay_returns_existing_order_without_recharging(self):
        payload = {"amount": "25.55", "idempotency_key": "service-checkout-replay123"}
        status, first = self.request("POST", "/services/1/order", payload=payload)
        self.assertEqual(status, 201, first)
        status, replay = self.request("POST", "/services/1/order", payload=payload)
        self.assertEqual(status, 200, replay)
        self.assertEqual(replay["id"], first["id"])
        self.assertTrue(replay["idempotent_replay"])
        self.payment_create.assert_called_once()

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
        for payload in (
            {"order_id": order_id, "milestone_id": milestone["id"], "amount": "25.00"},
            {"order_id": order_id, "amount": "25.00"},
        ):
            with self.subTest(payload=payload):
                status, result = self.request("POST", "/payments/fund-escrow", payload=payload)
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

        transfer = type("TransferResult", (), {"id": "tr_mock_release"})()
        self.api.stripe.Transfer = type("Transfer", (), {"create": mock.Mock(return_value=transfer)})
        account = {"details_submitted": True, "payouts_enabled": True, "charges_enabled": True}
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
            db.commit()

        transfer = type("TransferResult", (), {"id": "tr_mock_release"})()
        self.api.stripe.Transfer = type("Transfer", (), {"create": mock.Mock(return_value=transfer)})
        account = {"details_submitted": True, "payouts_enabled": True, "charges_enabled": True}
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
            return type("PaymentIntentResult", (), {"id": "pi_concurrent_once"})()

        self.payment_create.side_effect = delayed_create
        original_json_response = self.api.json_response
        self.api.json_response = lambda data, status=200: (status, data)

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

        self.assertEqual([status for status, _ in results], [200] * 8)
        self.assertEqual(self.payment_create.call_count, 1)
        self.assertEqual(sorted(result["mode"] for _, result in results), ["live", *(["replayed"] * 7)])
        with self.api.get_db() as db:
            self.assertEqual(db.execute("SELECT COUNT(*) FROM escrow_holds WHERE order_id=580").fetchone()[0], 1)
            self.assertEqual(db.execute("SELECT status FROM milestones WHERE id=680").fetchone()[0], "in_progress")
            self.assertEqual(db.execute("SELECT status FROM orders WHERE id=580").fetchone()[0], "in_progress")

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
        self.assertEqual(status, 400, result)
        self.assertIn("whole number", result["error"])
        self.payment_create.assert_not_called()

    def test_hourly_hire_rejects_precision_hidden_fraction_before_stripe(self):
        status, result = self.request("POST", "/jobs/4/hire", payload={
            "application_id": 18,
            "weekly_hour_cap": "6.0000000000000001",
        })
        self.assertEqual(status, 400, result)
        self.assertIn("whole number", result["error"])
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
            status, result = self.request(
                "POST",
                f"/jobs/{job_id}/hire",
                payload={"application_id": application_id, **extra},
            )
            self.assertEqual(status, 201, result)
            self.assertEqual(self.payment_create.call_args.kwargs["amount"], expected_charge)

    def test_hourly_settlement_endpoints_fail_closed_by_default_without_mutation(self):
        self.seed_hourly_order()
        self.api.HOURLY_SETTLEMENT_ENABLED = False

        status, result = self.request(
            "POST", "/orders/88/approve-hours", payload={"week_of": "2026-07-06"}
        )
        self.assertEqual(status, 503, result)
        self.assertIn("temporarily paused", result["error"])
        status, result = self.request("POST", "/orders/88/end-contract", token="tok-worker", payload={"reason": "done"})
        self.assertEqual(status, 503, result)
        self.assertIn("temporarily paused", result["error"])

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

    def test_enabled_hourly_approval_rounds_money_once_and_returns_json_safe_values(self):
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
        self.assertEqual(status, 200, result)
        self.assertEqual(result["hours_approved"], 1.5)
        self.assertEqual(result["worker_pay"], 38.33)
        self.assertEqual(result["platform_fee"], 0.38)

    def test_order_list_exposes_hourly_contract_type_and_funding_summary(self):
        self.seed_hourly_order()
        status, result = self.request("GET", "/orders", token="tok-worker")
        self.assertEqual(status, 200, result)
        row = next(order for order in result["orders"] if order["id"] == 88)
        self.assertEqual(row["contract_type"], "hourly")
        self.assertEqual(row["hourly_rate"], 25)
        self.assertEqual(row["current_week_escrow_amount"], 1000)

    def test_enabled_hourly_contract_completion_synchronizes_job_terminal_state(self):
        self.seed_hourly_order()
        self.api.HOURLY_SETTLEMENT_ENABLED = True
        status, result = self.request("POST", "/orders/88/end-contract", token="tok-worker", payload={"reason": "done"})
        self.assertEqual(status, 200, result)

        db = self.api.get_db()
        try:
            self.assertEqual(db.execute("SELECT status FROM orders WHERE id=88").fetchone()[0], "completed")
            self.assertEqual(db.execute("SELECT status FROM jobs WHERE id=4").fetchone()[0], "completed")
            self.assertEqual(db.execute("SELECT status FROM hourly_contracts WHERE order_id=88").fetchone()[0], "ended")
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

        def release_current(db, released_order_id, milestone_id, amount, worker_id):
            db.execute(
                "UPDATE escrow_holds SET status='released', released_at=datetime('now') WHERE order_id=? AND milestone_id=?",
                [released_order_id, milestone_id],
            )
            return 25.0, 0.25

        release = mock.Mock(side_effect=release_current)
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
