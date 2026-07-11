import contextlib
import hashlib
import importlib.util
import io
import json
import logging
import os
import sqlite3
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

import stripe as stripe_sdk

import test_payment_setup_attempt_ledger as payment_setup
import test_payout_attempt_ledger as payout_ledger
import test_transaction_lifecycle_regressions as lifecycle
from test_deep_audit_regressions import load_api_core, parse_cgi_output


class V7PaymentLifecycleRedTests(unittest.TestCase):
    setUp = lifecycle.TransactionLifecycleRegressionTests.setUp
    tearDown = lifecycle.TransactionLifecycleRegressionTests.tearDown
    request = lifecycle.TransactionLifecycleRegressionTests.request
    _seed = lifecycle.TransactionLifecycleRegressionTests._seed
    _bind_verified_funding = lifecycle.TransactionLifecycleRegressionTests._bind_verified_funding
    def _exact_transfer(self, transfer_id):
        def create(**kwargs):
            return SimpleNamespace(
                id=transfer_id,
                amount=kwargs["amount"],
                currency=kwargs["currency"],
                destination=kwargs["destination"],
                metadata=kwargs["metadata"],
            )
        return create

    def test_hourly_hire_is_503_before_any_order_funding_or_processor_io(self):
        before_calls = self.payment_create.call_count
        with self.api.get_db() as db:
            before = {
                table: db.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                for table in ("orders", "hourly_contracts", "escrow_holds", "funding_attempts")
            }
        status, payload = self.request(
            "POST", "/jobs/4/hire", payload={"application_id": 18, "weekly_hour_cap": 40}
        )
        self.assertEqual(status, 503, payload)
        self.assertIn("Task 4", payload["error"])
        with self.api.get_db() as db:
            after = {
                table: db.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                for table in before
            }
        self.assertEqual(after, before)
        self.assertEqual(self.payment_create.call_count, before_calls)

    def test_admin_dispute_settlement_is_503_before_any_mutation(self):
        with self.api.get_db() as db:
            db.execute("UPDATE users SET is_admin=1 WHERE id=2")
            db.execute("INSERT INTO orders(id,type,worker_id,employer_id,status,total_amount) VALUES(970,'service_order',1,2,'disputed',10)")
            db.execute("INSERT INTO escrow_holds(order_id,amount,status,stripe_payment_intent_id) VALUES(970,10,'held','pi_existing')")
            db.commit()
            before = {
                table: [tuple(row) for row in db.execute(f"SELECT * FROM {table} ORDER BY 1")]
                for table in ("orders", "escrow_holds", "platform_revenue", "audit_log")
            }
        status, payload = self.request("POST", "/admin/resolve-dispute", payload={
            "order_id": 970,
            "resolution": "release_to_worker",
            "admin_password": "irrelevant",
            "manual_money_movement_confirmed": True,
            "processor_reference": "tr_unsafe",
        })
        self.assertEqual(status, 503, payload)
        self.assertIn("Task 4", payload["error"])
        with self.api.get_db() as db:
            after = {
                table: [tuple(row) for row in db.execute(f"SELECT * FROM {table} ORDER BY 1")]
                for table in before
            }
        self.assertEqual(after, before)

    def test_ambiguous_next_funding_completes_prior_payout_and_exact_retry_never_retransfers(self):
        with self.api.get_db() as db:
            db.execute("INSERT INTO orders(id,type,worker_id,employer_id,status,total_amount) VALUES(971,'service_order',1,2,'pending',25)")
            db.execute("INSERT INTO milestones(id,order_id,title,amount,sequence,status) VALUES(972,971,'First',10,1,'pending')")
            db.execute("INSERT INTO milestones(id,order_id,title,amount,sequence,status) VALUES(973,971,'Second',15,2,'pending')")
            db.execute("UPDATE worker_profiles SET payout_account_id='acct_live_worker' WHERE user_id=1")
            db.commit()
        status, payload = self.request("POST", "/payments/fund-escrow", payload={"order_id": 971, "milestone_id": 972, "amount": "10.00"})
        self.assertEqual(status, 200, payload)
        status, payload = self.request("POST", "/orders/971/submit", token="tok-worker", payload={"notes": "done"})
        self.assertEqual(status, 200, payload)

        original_create = self.api.stripe.PaymentIntent.create
        calls = {"fund": 0, "transfer": 0}
        def payment_create(**kwargs):
            calls["fund"] += 1
            if calls["fund"] == 1:
                raise stripe_sdk.APIConnectionError("response lost")
            return original_create(**kwargs)
        self.api.stripe.PaymentIntent.create = payment_create
        self.api.stripe.PaymentIntent.retrieve = mock.Mock(side_effect=stripe_sdk.APIConnectionError("still unavailable"))
        self.api.stripe.PaymentIntent.search = mock.Mock(side_effect=stripe_sdk.APIConnectionError("still unavailable"))
        def transfer_create(**kwargs):
            calls["transfer"] += 1
            return SimpleNamespace(id="tr_prior_exact", amount=kwargs["amount"], currency=kwargs["currency"], destination=kwargs["destination"], metadata=kwargs["metadata"])
        self.api.stripe.Transfer = SimpleNamespace(create=transfer_create)
        account = {"payouts_enabled": True, "charges_enabled": True, "capabilities": {"transfers": "active"}}
        with mock.patch.object(self.api, "retrieve_live_connect_account", return_value=account):
            status, first = self.request("POST", "/orders/971/approve", token="tok-employer")
            self.assertEqual(status, 409, first)
            with self.api.get_db() as db:
                self.assertEqual(db.execute("SELECT lifecycle_status FROM payout_release_attempts WHERE order_id=971").fetchone()[0], "completed")
                self.assertEqual(db.execute("SELECT status FROM milestones WHERE id=972").fetchone()[0], "approved")
            status, retry = self.request("POST", "/orders/971/approve", token="tok-employer")
        self.assertEqual(status, 409, retry)
        self.assertIn("reconciliation", retry["error"].lower())
        self.assertEqual(calls["transfer"], 1)
        self.assertEqual(calls["fund"], 1)

    def test_submit_is_blocked_by_prepared_completion_gate_without_mutation(self):
        with self.api.get_db() as db:
            db.execute("INSERT INTO orders(id,type,worker_id,employer_id,status,total_amount) VALUES(974,'service_order',1,2,'in_progress',10)")
            db.execute("INSERT INTO order_completion_operations(order_id,employer_id,expected_order_status,hold_ids_json,hold_set_sha256,status) VALUES(974,2,'in_progress','[]',?,'prepared')", [hashlib.sha256(b"[]").hexdigest()])
            db.commit()
        status, payload = self.request("POST", "/orders/974/submit", token="tok-worker", payload={"notes": "race"})
        self.assertEqual(status, 409, payload)
        with self.api.get_db() as db:
            self.assertEqual(db.execute("SELECT status FROM orders WHERE id=974").fetchone()[0], "in_progress")

    def test_foreign_owner_funding_aliases_are_403_and_positive_owner_is_allowed(self):
        with self.api.get_db() as db:
            db.execute("INSERT INTO orders (id,type,worker_id,employer_id,status,total_amount) VALUES (980,'service_order',1,2,'pending',10)")
            db.execute("INSERT INTO milestones (id,order_id,title,amount,sequence,status) VALUES (981,980,'Only',10,1,'pending')")
            for key_id, user_id, raw in ((980, 1, 'ghh_foreign_funder'), (981, 2, 'ghh_owner_funder')):
                db.execute(
                    "INSERT INTO api_keys (id,user_id,key_hash,key_prefix,name,scopes) VALUES (?,?,?,?,?,?)",
                    [key_id, user_id, hashlib.sha256(raw.encode()).hexdigest(), raw[:12], 'v7', '[\"payments:fund\"]'],
                )
            db.commit()

        def api_request(alias, raw_key):
            body = json.dumps({"order_id": 980, "milestone_id": 981, "amount": "10.00"})
            ctx = self.api._request_ctx
            ctx.request_method, ctx.path_info, ctx.query_string = "POST", alias, ""
            ctx.http_authorization, ctx.http_x_api_key = "", raw_key
            ctx.stdin_data, ctx.content_type, ctx.content_length = body, "application/json", str(len(body))
            ctx.remote_addr = "127.0.0.1"
            with contextlib.redirect_stdout(io.StringIO()) as out:
                self.api.handle_request()
            return parse_cgi_output(out.getvalue())

        before_calls = self.payment_create.call_count
        for alias in ("/payments/fund-escrow", "/payments/prepare-order-payment"):
            status, payload = api_request(alias, "ghh_foreign_funder")
            self.assertEqual(status, 403, (alias, payload))
        self.assertEqual(self.payment_create.call_count, before_calls)
        with self.api.get_db() as db:
            self.assertEqual(db.execute("SELECT COUNT(*) FROM escrow_holds WHERE order_id=980").fetchone()[0], 0)
        status, payload = api_request("/payments/prepare-order-payment", "ghh_owner_funder")
        self.assertEqual(status, 200, payload)
        self.assertEqual(self.payment_create.call_count, before_calls + 1)

    def test_funding_hard_exit_after_processor_success_restarts_without_duplicate_create(self):
        with self.api.get_db() as db:
            db.execute("INSERT INTO orders (id,type,worker_id,employer_id,status,total_amount) VALUES (982,'service_order',1,2,'pending',10)")
            db.execute("INSERT INTO milestones (id,order_id,title,amount,sequence,status) VALUES (983,982,'Only',10,1,'pending')")
            db.commit()
        script = f'''import importlib.util, os
from types import SimpleNamespace
spec=importlib.util.spec_from_file_location("api_hard_funding", {str(Path(__file__).with_name("api_core.py"))!r})
api=importlib.util.module_from_spec(spec); spec.loader.exec_module(api)
api._db_path_resolved=None; api.STRIPE_AVAILABLE=True; api.STRIPE_SECRET_KEY="configured"
def create(**kw):
    return SimpleNamespace(id="pi_hard_success", status="succeeded", amount=kw["amount"], amount_received=kw["amount"], currency=kw["currency"], metadata=kw["metadata"])
api.stripe=SimpleNamespace(PaymentIntent=SimpleNamespace(create=create))
inspect=api._processor_intent_inspection
def hard_exit_after_exact_processor_success(*args, **kwargs):
    result=inspect(*args, **kwargs)
    if result.get("outcome") == "succeeded": os._exit(24)
    return result
api._processor_intent_inspection=hard_exit_after_exact_processor_success
db=api.get_db(); api.fund_escrow_stripe(db,2,10,982,983,funding_identity="milestone:983")
'''
        completed = subprocess.run([sys.executable, "-c", script], env=os.environ.copy(), capture_output=True, text=True)
        self.assertEqual(completed.returncode, 24, completed.stderr)
        create = mock.Mock()
        unavailable = mock.Mock(side_effect=Exception("offline reconciliation"))
        self.api.stripe.PaymentIntent = SimpleNamespace(create=create, retrieve=unavailable, search=unavailable)
        self.api.STRIPE_ERROR = Exception
        with self.api.get_db() as db, self.assertRaises(self.api.FundingReconciliationRequired):
            self.api.fund_escrow_stripe(db, 2, 10, 982, 983, funding_identity="milestone:983")
        create.assert_not_called()
        with self.api.get_db() as db:
            row = db.execute("SELECT status,error_code FROM funding_attempts WHERE order_id=982").fetchone()
            self.assertEqual(tuple(row), ("unknown", "reconcile_unavailable"))


class V7PayoutEvidenceRedTests(payout_ledger.PayoutAttemptLedgerTests):
    def test_connect_readiness_requires_explicit_active_transfer_capability(self):
        base = {"payouts_enabled": True, "charges_enabled": True}
        for capability in (None, "pending", "inactive"):
            account = dict(base)
            if capability is not None:
                account["capabilities"] = {"transfers": capability}
            with self.subTest(capability=capability):
                self.assertFalse(self.api.is_live_connect_account_ready(account))
        self.assertTrue(self.api.is_live_connect_account_ready({**base, "capabilities": {"transfers": "active"}}))

    def test_sparse_payout_create_response_is_unknown_without_read_only_retrieve(self):
        self._seed_release(980, 981, intent_id="pi_live_980", evidence_source="processor_create")
        self.transfer_create.return_value = SimpleNamespace(id="tr_sparse")
        self.transfer_create.side_effect = None
        self.api.stripe.Transfer = SimpleNamespace(create=self.transfer_create)
        with self.assertRaises(self.api.FundingReconciliationRequired):
            with self.api.get_db() as db:
                self.api.release_escrow_to_worker(db, 980, 981, 10, 1)
        with self.api.get_db() as db:
            row = db.execute("SELECT status,lifecycle_status,manual_review_required,error_code FROM payout_release_attempts WHERE order_id=980").fetchone()
        self.assertEqual(tuple(row), ("unknown", "manual_review", 1, "processor_evidence_mismatch"))


class V7SchemaAndAccountingRedTests(payment_setup.PaymentSetupAttemptLedgerTests):
    def test_every_unexpected_protected_ledger_trigger_is_rejected(self):
        protected = (
            "funding_attempts", "funding_attempt_conflict_evidence",
            "payout_release_attempts", "payout_release_conflict_evidence",
        )
        for number, table in enumerate(protected):
            with self.subTest(table=table), self.api.get_db() as db:
                name = f"trg_v7_poison_{number}"
                db.execute(f"CREATE TRIGGER {name} BEFORE DELETE ON {table} BEGIN SELECT RAISE(ABORT,'poison'); END")
                db.commit()
                with self.assertRaisesRegex(RuntimeError, "unexpected"):
                    self.api._init_db_connection(db)
                db.execute(f"DROP TRIGGER {name}")
                db.commit()

    def test_poisoned_initialization_rolls_back_every_early_schema_mutation(self):
        path = os.path.join(self.tmp.name, "poisoned-atomic.db")
        db = sqlite3.connect(path)
        db.execute("CREATE TABLE payment_setup_operations(foo TEXT)")
        db.commit()
        before = list(db.execute("SELECT type,name,sql FROM sqlite_master ORDER BY type,name"))
        db.close()
        conn = sqlite3.connect(path)
        conn.row_factory = sqlite3.Row
        with self.assertRaises(Exception):
            self.api._init_db_connection(conn)
        conn.close()
        check = sqlite3.connect(path)
        after = list(check.execute("SELECT type,name,sql FROM sqlite_master ORDER BY type,name"))
        check.close()
        self.assertEqual(after, before)

    def test_initialization_early_mid_late_failure_injection_rolls_back_atomically(self):
        for stage in ("early", "mid", "late"):
            with self.subTest(stage=stage):
                with self.api.get_db() as db:
                    db.execute("DROP TABLE IF EXISTS transactional_email_outbox")
                    db.commit()
                def failpoint(observed):
                    if observed == stage:
                        raise RuntimeError("v7 init " + stage)
                self.api._init_db_failure_hook = failpoint
                try:
                    with self.assertRaisesRegex(RuntimeError, "v7 init"):
                        self.api.init_db()
                finally:
                    self.api._init_db_failure_hook = None
                with self.api.get_db() as db:
                    self.assertIsNone(db.execute(
                        "SELECT name FROM sqlite_master WHERE name='transactional_email_outbox'"
                    ).fetchone())
                self.api.init_db()
                with self.api.get_db() as db:
                    self.assertIsNotNone(db.execute(
                        "SELECT name FROM sqlite_master WHERE type='table' AND name='transactional_email_outbox'"
                    ).fetchone())

    def test_hard_exit_account_link_processor_success_blocks_duplicate_after_restart(self):
        core = str(Path(__file__).with_name("api_core.py"))
        script = f'''import importlib.util, os
spec=importlib.util.spec_from_file_location("api_hard_setup", {core!r})
api=importlib.util.module_from_spec(spec); spec.loader.exec_module(api)
api._db_path_resolved=None
db=api.get_db()
api._payment_setup_operation(db,2,"account_link",{{"account_id":"acct_hard","refresh_generation":0}},lambda key: {{"id":"link"}},lambda result: os._exit(23))
'''
        completed = subprocess.run([sys.executable, "-c", script], env=os.environ.copy(), capture_output=True, text=True)
        self.assertEqual(completed.returncode, 23, completed.stderr)
        processor = mock.Mock()
        builder = mock.Mock()
        with self.api.get_db() as db, self.assertRaises(self.api.PaymentSetupReconciliationRequired):
            self.api._payment_setup_operation(
                db, 2, "account_link", {"account_id": "acct_hard", "refresh_generation": 0},
                processor, builder,
            )
        processor.assert_not_called()
        builder.assert_not_called()
        with self.api.get_db() as db:
            row = db.execute(
                "SELECT status,manual_review_required FROM payment_setup_operations WHERE operation_kind='account_link'"
            ).fetchone()
            self.assertEqual(tuple(row), ("unknown", 1))

    def _insert_key(self, raw_key, scopes):
        with self.api.get_db() as db:
            db.execute("INSERT INTO api_keys(user_id,key_hash,key_prefix,name,scopes,is_active,total_requests) VALUES(1,?,?,?, ?,1,0)", [hashlib.sha256(raw_key.encode()).hexdigest(), raw_key[:8], "v7", json.dumps(scopes)])
            db.commit()

    def test_valid_under_scoped_403_is_durably_counted_without_domain_or_processor_effect(self):
        raw = "ghh_v7_under_scoped"
        self._insert_key(raw, ["read"])
        before_calls = len(self.calls)
        status, payload = self.request("/payments/setup-worker", api_key=raw)
        self.assertEqual(status, 403, payload)
        with self.api.get_db() as db:
            key = db.execute("SELECT id,total_requests,last_used_at FROM api_keys WHERE name='v7'").fetchone()
            usage = db.execute("SELECT endpoint,method,status_code,accounting_state FROM api_key_usage WHERE api_key_id=?", [key["id"]]).fetchone()
            self.assertEqual(tuple(usage), ("/payments/setup-worker", "POST", 403, "denied"))
            self.assertEqual(key["total_requests"], 1)
            self.assertIsNotNone(key["last_used_at"])
            self.assertEqual(db.execute("SELECT COUNT(*) FROM payment_setup_operations").fetchone()[0], 0)
        self.assertEqual(len(self.calls), before_calls)

    def test_authorized_request_has_started_intent_before_processor_and_finalize_failure_preserves_audit(self):
        raw = "ghh_v7_accounting_durable"
        self._insert_key(raw, ["payments:setup"])
        observed = []
        original = self.api.stripe.Account.create
        def inspect_intent(**kwargs):
            with self.api.get_db() as db:
                observed.append(tuple(db.execute("SELECT accounting_state,status_code FROM api_key_usage ORDER BY id DESC LIMIT 1").fetchone()))
            return original(**kwargs)
        self.api.stripe.Account.create = inspect_intent
        self.api._finalize_api_key_accounting_intent = mock.Mock(side_effect=sqlite3.OperationalError("injected finalization failure"))
        status, payload = self.request("/payments/setup-worker", api_key=raw)
        self.assertEqual(status, 200, payload)
        self.assertEqual(observed, [("started", None)])
        with self.api.get_db() as db:
            row = db.execute("SELECT accounting_state,status_code FROM api_key_usage ORDER BY id DESC LIMIT 1").fetchone()
            self.assertEqual(tuple(row), ("started", None))
            self.assertEqual(db.execute("SELECT total_requests FROM api_keys WHERE name='v7'").fetchone()[0], 1)
            self.assertEqual(db.execute("SELECT status FROM payment_setup_operations WHERE operation_kind='account_create'").fetchone()[0], "committed")

    def test_review_email_is_durable_outbox_and_not_direct_io_inside_writer(self):
        with self.api.get_db() as db:
            db.execute("INSERT INTO notifications(user_id,type,title,message,link) VALUES(1,'marker','marker','','')")
            self.api.push_notification(db, 1, "review_request", "Review", "Please review", "/orders/1#review", email=True, email_dedupe="review:1")
            self.assertTrue(db.in_transaction)
            self.assertEqual(db.execute("SELECT state FROM transactional_email_outbox WHERE dedupe_context='review:1'").fetchone()[0], "pending")
            db.commit()


class V7SecurityRedTests(unittest.TestCase):
    def test_known_serialized_json_is_recursively_redacted_even_with_reveal_sensitive(self):
        tool_path = Path(__file__).with_name("tools") / "reconcile_funding_attempts.py"
        spec = importlib.util.spec_from_file_location("v7_reconcile_tool", tool_path)
        tool = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(tool)
        secret = "seti_v7_client_secret_never"
        row = {
            "expected_snapshot_json": json.dumps({"request_fingerprint": "fp_nested", "processor_idempotency_key": "idem_nested"}),
            "normalized_evidence_json": json.dumps({"canonical_intent_id": "pi_nested", "client_secret": secret}),
        }
        default = json.dumps(tool.redact_report(row, False), sort_keys=True)
        revealed = json.dumps(tool.redact_report(row, True), sort_keys=True)
        for value in ("fp_nested", "idem_nested", "pi_nested", secret):
            self.assertNotIn(value, default)
        self.assertIn("fp_nested", revealed)
        self.assertIn("pi_nested", revealed)
        self.assertNotIn(secret, revealed)

    def test_stripe_debug_response_capabilities_are_suppressed_in_handlers_and_stderr(self):
        api = load_api_core()
        secret = "seti_v7_client_secret_log_leak"
        link = "https://connect.stripe.test/v7-capability-url"
        stream = io.StringIO()
        handler = logging.StreamHandler(stream)
        root = logging.getLogger()
        previous = root.level
        root.addHandler(handler)
        root.setLevel(logging.DEBUG)
        stderr = io.StringIO()
        try:
            api.install_sensitive_logging_filters()
            with contextlib.redirect_stderr(stderr):
                logging.getLogger("stripe").debug("API response body %s %s", secret, link)
            emitted = stream.getvalue() + stderr.getvalue()
            self.assertNotIn(secret, emitted)
            self.assertNotIn(link, emitted)
        finally:
            root.removeHandler(handler)
            root.setLevel(previous)


if __name__ == "__main__":
    unittest.main()
