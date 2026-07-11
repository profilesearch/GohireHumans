import contextlib
import io
import json
import hashlib
import os
import sqlite3
import tempfile
import threading
import time
import unittest
from types import SimpleNamespace
from unittest import mock

import stripe

from test_payment_setup_attempt_ledger import PaymentSetupAttemptLedgerTests
from test_deep_audit_regressions import load_api_core
import test_transaction_lifecycle_regressions as lifecycle_tests

parse_cgi_output = lifecycle_tests.parse_cgi_output


class V6PaymentSetupTests(PaymentSetupAttemptLedgerTests):
    def test_account_link_real_schema_has_no_id(self):
        self.account_link_create = lambda **kwargs: stripe.AccountLink.construct_from({
            "object": "account_link", "created": 1, "expires_at": 4102444800,
            "url": "https://connect.stripe.test/capability",
        }, "test")
        self.api.stripe.AccountLink.create = self.account_link_create
        status, payload = self.request("/payments/setup-worker", "worker-token")
        self.assertEqual(status, 200, payload)
        self.assertEqual(payload["onboarding_url"], "https://connect.stripe.test/capability")
        with self.api.get_db() as db:
            dump = "\n".join(str(tuple(r)) for r in db.execute("SELECT * FROM payment_setup_operations"))
        self.assertNotIn("connect.stripe.test", dump)

    def test_payment_status_catches_pinned_connection_error(self):
        with self.api.get_db() as db:
            db.execute("INSERT INTO worker_profiles(user_id,payout_account_id,payout_method) VALUES (2,'acct_real','stripe_connect')")
            db.commit()
        with mock.patch.object(self.api, "retrieve_live_connect_account", side_effect=stripe.APIConnectionError("offline")):
            ctx = self.api._request_ctx
            for cached in ("body_cache", "raw_body"):
                if hasattr(ctx, cached):
                    delattr(ctx, cached)
            ctx.request_method = "GET"
            ctx.path_info = "/payments/status"
            ctx.query_string = ""
            ctx.http_authorization = "Bearer worker-token"
            ctx.http_x_api_key = ""
            ctx.stdin_data = ""
            ctx.content_type = "application/json"
            ctx.content_length = "0"
            ctx.remote_addr = "127.0.0.1"
            with contextlib.redirect_stdout(io.StringIO()) as out:
                self.api.handle_request()
            status, payload = parse_cgi_output(out.getvalue())
        self.assertEqual(status, 200, payload)
        self.assertFalse(payload["worker_payout_status"]["connected"])


class V6AccountLinkMatrixTests(PaymentSetupAttemptLedgerTests):
    def link(self, url, expires):
        return stripe.AccountLink.construct_from({"object": "account_link", "created": int(time.time()), "expires_at": expires, "url": url}, "test")

    def test_replay_refresh_expiry_generations_and_never_persist_url(self):
        now = int(time.time())
        values = [self.link(f"https://connect.stripe.test/{n}", now + 600) for n in range(4)]
        create = mock.Mock(side_effect=values)
        self.api.stripe.AccountLink.create = create
        responses = [self.request("/payments/setup-worker", "worker-token")]
        responses.append(self.request("/payments/setup-worker", "worker-token"))
        responses.append(self.request("/payments/setup-worker", "worker-token", {"refresh": True}))
        with self.api.get_db() as db:
            row = db.execute("SELECT id,result_json FROM payment_setup_operations WHERE operation_kind='account_link_create' ORDER BY id DESC LIMIT 1").fetchone()
            result = json.loads(row["result_json"]); result["expires_at"] = now - 1
            db.execute("UPDATE payment_setup_operations SET result_json=? WHERE id=?", [json.dumps(result), row["id"]]); db.commit()
        responses.append(self.request("/payments/setup-worker", "worker-token"))
        self.assertTrue(all(status == 200 for status, _ in responses), responses)
        keys = [call.kwargs["idempotency_key"] for call in create.call_args_list]
        self.assertEqual(keys[0], keys[1]); self.assertNotEqual(keys[1], keys[2]); self.assertNotEqual(keys[2], keys[3])
        with self.api.get_db() as db:
            dump = "\n".join(str(tuple(r)) for r in db.execute("SELECT * FROM payment_setup_operations"))
            generations = [json.loads(r[0])["generation"] for r in db.execute("SELECT request_binding_json FROM payment_setup_operations WHERE operation_kind='account_link_create' ORDER BY id")]
        self.assertEqual(generations, [1, 2, 3]); self.assertNotIn("connect.stripe.test", dump)

    def test_ambiguous_failure_freezes_without_url(self):
        self.api.stripe.AccountLink.create = mock.Mock(side_effect=stripe.APIConnectionError("lost"))
        status, _ = self.request("/payments/setup-worker", "worker-token")
        self.assertEqual(status, 409)
        with self.api.get_db() as db:
            row = db.execute("SELECT status,manual_review_required,result_json FROM payment_setup_operations WHERE operation_kind='account_link_create'").fetchone()
        self.assertEqual((row["status"], row["manual_review_required"], row["result_json"]), ("unknown", 1, None))


class V6PaymentSetupSchemaTests(unittest.TestCase):
    def test_rejects_wrong_index_and_unexpected_trigger(self):
        for scenario in ("index", "trigger"):
            with self.subTest(scenario=scenario), tempfile.TemporaryDirectory() as tmp:
                os.environ["DATABASE_PATH"] = os.path.join(tmp, "schema.db"); os.environ["DISABLE_AUTO_SEED"] = "1"
                api = load_api_core(); api._db_path_resolved = None; api.init_db()
                with api.get_db() as db:
                    if scenario == "index":
                        db.execute("DROP INDEX idx_payment_setup_processor_key")
                        db.execute("CREATE UNIQUE INDEX idx_payment_setup_processor_key ON payment_setup_operations(operation_key)")
                    else:
                        db.execute("CREATE TRIGGER poison_setup AFTER INSERT ON payment_setup_operations BEGIN SELECT 1; END")
                    db.commit()
                with self.assertRaises(RuntimeError): api.init_db()
        os.environ.pop("DATABASE_PATH", None); os.environ.pop("DISABLE_AUTO_SEED", None)


class V6RedactionTests(unittest.TestCase):
    def test_never_reveal_secret_values(self):
        from tools.reconcile_funding_attempts import redact_report
        samples = [
            "pi_123_secret_abc", "seti_123_secret_abc", "ghh_abcdef123456",
            "Authorization: Bearer abc.def", "Basic dXNlcjpwYXNz",
            "-----BEGIN PRIVATE KEY-----", "whsec_abc", "sk_live_abc", "rk_live_abc",
        ]
        for sample in samples:
            with self.subTest(sample=sample):
                self.assertEqual(redact_report({"error": [sample]}, True)["error"][0], "[REDACTED]")


class V6CompletionRecoveryMatrixTests(unittest.TestCase):
    setUp = lifecycle_tests.TransactionLifecycleRegressionTests.setUp
    tearDown = lifecycle_tests.TransactionLifecycleRegressionTests.tearDown
    request = lifecycle_tests.TransactionLifecycleRegressionTests.request
    _seed = lifecycle_tests.TransactionLifecycleRegressionTests._seed
    _bind_verified_funding = lifecycle_tests.TransactionLifecycleRegressionTests._bind_verified_funding
    def _seed_completion(self, order_id, amounts):
        with self.api.get_db() as db:
            db.execute(
                "INSERT INTO orders(id,type,worker_id,employer_id,status,total_amount) "
                "VALUES (?,'service_order',1,2,'submitted',?)",
                [order_id, sum(amounts)],
            )
            db.execute(
                "UPDATE worker_profiles SET payout_account_id='acct_live_worker' WHERE user_id=1"
            )
            for offset, amount in enumerate(amounts, 1):
                milestone_id = order_id + 100 + offset
                intent_id = f"pi_complete_{order_id}_{offset}"
                db.execute(
                    "INSERT INTO milestones(id,order_id,title,amount,sequence,status) "
                    "VALUES (?,?,?,?,?,'submitted')",
                    [milestone_id, order_id, f"Part {offset}", amount, offset],
                )
                db.execute(
                    "INSERT INTO escrow_holds(order_id,milestone_id,amount,status,stripe_payment_intent_id,funding_identity) "
                    "VALUES (?,?,?,'held',?,?)",
                    [order_id, milestone_id, amount, intent_id, f"milestone:{milestone_id}"],
                )
                self._bind_verified_funding(db, order_id, milestone_id, amount, intent_id)
            db.commit()

    def _terminal_state(self, order_id):
        with self.api.get_db() as db:
            return {
                "order": tuple(db.execute(
                    "SELECT status,completed_at FROM orders WHERE id=?", [order_id]
                ).fetchone()),
                "milestones": [tuple(row) for row in db.execute(
                    "SELECT status,released_at FROM milestones WHERE order_id=? ORDER BY sequence", [order_id]
                )],
                "holds": [tuple(row) for row in db.execute(
                    "SELECT status,stripe_transfer_id FROM escrow_holds WHERE order_id=? ORDER BY id", [order_id]
                )],
                "attempts": [tuple(row) for row in db.execute(
                    "SELECT status,lifecycle_status,manual_review_required FROM payout_release_attempts "
                    "WHERE order_id=? ORDER BY id", [order_id]
                )],
                "operation": tuple(db.execute(
                    "SELECT status,completed_at FROM order_completion_operations WHERE order_id=?", [order_id]
                ).fetchone()),
            }

    def test_multi_hold_second_definitive_failure_resumes_without_duplicate_transfer(self):
        self._seed_completion(760, [10, 15])
        calls = []

        def create_transfer(**kwargs):
            calls.append(kwargs)
            if len(calls) == 2:
                raise stripe.InvalidRequestError("definitive rejection", "amount")
            return lifecycle_tests.exact_transfer(f"tr_complete_{len(calls)}", kwargs)

        self.api.stripe.Transfer = SimpleNamespace(create=create_transfer)
        account = {"details_submitted": True, "payouts_enabled": True, "charges_enabled": True, "capabilities": {"transfers": "active"}}
        with mock.patch.object(self.api, "retrieve_live_connect_account", return_value=account):
            status, first = self.request("POST", "/orders/760/complete", token="tok-employer")
            self.assertEqual(status, 502, first)
            with self.api.get_db() as db:
                self.assertEqual(
                    [tuple(row) for row in db.execute(
                        "SELECT status,lifecycle_status FROM payout_release_attempts WHERE order_id=760 ORDER BY id"
                    )],
                    [("committed", "pending"), ("failed", "completed")],
                )
                self.assertEqual(
                    db.execute("SELECT status FROM order_completion_operations WHERE order_id=760").fetchone()[0],
                    "prepared",
                )
            status, resumed = self.request("POST", "/orders/760/complete", token="tok-employer")
        self.assertEqual(status, 200, resumed)
        self.assertEqual(len(calls), 3)
        first_hold = calls[0]["metadata"]["hold_id"]
        second_hold = calls[2]["metadata"]["hold_id"]
        self.assertNotEqual(first_hold, second_hold)
        self.assertEqual(len([call for call in calls if call["metadata"]["hold_id"] == first_hold]), 1)
        state = self._terminal_state(760)
        self.assertEqual(state["order"][0], "completed")
        self.assertTrue(state["order"][1])
        self.assertTrue(all(status == "approved" and released for status, released in state["milestones"]))
        self.assertTrue(all(status == "released" and transfer for status, transfer in state["holds"]))
        self.assertTrue(all(lifecycle == "completed" and not manual for _, lifecycle, manual in state["attempts"]))
        self.assertEqual(state["operation"][0], "completed")
        self.assertTrue(state["operation"][1])

    def test_crash_before_local_finalize_and_response_loss_exact_replay(self):
        account = {"details_submitted": True, "payouts_enabled": True, "charges_enabled": True, "capabilities": {"transfers": "active"}}
        for order_id, failure_point in ((761, "audit"), (762, "response")):
            with self.subTest(failure_point=failure_point):
                self._seed_completion(order_id, [12])
                transfer = mock.Mock(side_effect=lambda **kwargs: lifecycle_tests.exact_transfer(f"tr_{order_id}", kwargs))
                self.api.stripe.Transfer = SimpleNamespace(create=transfer)
                if failure_point == "audit":
                    real_audit = self.api.audit

                    def fail_once(*args, **kwargs):
                        if len(args) > 2 and args[2] == "complete_order":
                            raise RuntimeError("crash before local finalize")
                        return real_audit(*args, **kwargs)

                    failure_patch = mock.patch.object(self.api, "audit", side_effect=fail_once)
                else:
                    real_json_response = self.api.json_response

                    def lose_response(data, status=200):
                        if isinstance(data, dict) and data.get("status") == "completed" and not data.get("idempotent_replay"):
                            raise RuntimeError("response lost after commit")
                        return real_json_response(data, status)

                    failure_patch = mock.patch.object(self.api, "json_response", side_effect=lose_response)
                with mock.patch.object(self.api, "retrieve_live_connect_account", return_value=account), failure_patch:
                    first_status, _ = self.request("POST", f"/orders/{order_id}/complete", token="tok-employer")
                self.assertEqual(first_status, 500)
                with self.api.get_db() as db:
                    operation_status = db.execute(
                        "SELECT status FROM order_completion_operations WHERE order_id=?", [order_id]
                    ).fetchone()[0]
                    order_status = db.execute("SELECT status FROM orders WHERE id=?", [order_id]).fetchone()[0]
                expected = ("prepared", "submitted") if failure_point == "audit" else ("completed", "completed")
                self.assertEqual((operation_status, order_status), expected)
                with mock.patch.object(self.api, "retrieve_live_connect_account", return_value=account):
                    replay_status, replay = self.request("POST", f"/orders/{order_id}/complete", token="tok-employer")
                    exact_status, exact = self.request("POST", f"/orders/{order_id}/complete", token="tok-employer")
                self.assertEqual((replay_status, exact_status), (200, 200), (replay, exact))
                self.assertTrue(exact["idempotent_replay"])
                transfer.assert_called_once()
                self.assertEqual(self._terminal_state(order_id)["operation"][0], "completed")


class V6PaymentProfileCASMatrixTests(unittest.TestCase):
    setUp = PaymentSetupAttemptLedgerTests.setUp
    tearDown = PaymentSetupAttemptLedgerTests.tearDown
    request = PaymentSetupAttemptLedgerTests.request
    def assert_frozen(self, operation_kind):
        with self.api.get_db() as db:
            row = db.execute(
                "SELECT status,manual_review_required,error_code FROM payment_setup_operations "
                "WHERE operation_kind=? ORDER BY id DESC LIMIT 1", [operation_kind]
            ).fetchone()
        self.assertEqual(tuple(row), ("unknown", 1, "post_processor_cas_failed"))

    def test_customer_create_conflict_freezes_and_exact_readback_is_allowed(self):
        calls = mock.Mock()

        def create_then_conflict(**_kwargs):
            calls()
            with self.api.get_db() as writer:
                writer.execute(
                    "UPDATE employer_profiles SET stripe_customer_id='cus_racer' WHERE user_id=1"
                )
                writer.commit()
            return SimpleNamespace(id="cus_processor")

        self.api.stripe.Customer.create = create_then_conflict
        status, payload = self.request("/payments/setup-employer", "buyer-token")
        self.assertEqual(status, 409, payload)
        self.assert_frozen("customer_create")
        status, _ = self.request("/payments/setup-employer", "buyer-token")
        self.assertEqual(status, 409)
        self.assertEqual(calls.call_count, 1)
        with self.api.get_db() as db:
            self.assertEqual(db.execute(
                "SELECT stripe_customer_id FROM employer_profiles WHERE user_id=1"
            ).fetchone()[0], "cus_racer")
            db.execute("DELETE FROM payment_setup_operations")
            db.execute("DELETE FROM employer_profiles WHERE user_id=1")
            db.commit()

        def create_then_exact(**_kwargs):
            with self.api.get_db() as writer:
                writer.execute(
                    "UPDATE employer_profiles SET stripe_customer_id='cus_exact' WHERE user_id=1"
                )
                writer.commit()
            return SimpleNamespace(id="cus_exact")

        self.api.stripe.Customer.create = create_then_exact
        status, payload = self.request("/payments/setup-employer", "buyer-token")
        self.assertEqual(status, 200, payload)
        self.assertEqual(payload["customer_id"], "cus_exact")

    def test_customer_modify_payment_method_conflict_is_frozen_non_2xx(self):
        status, setup = self.request("/payments/setup-employer", "buyer-token")
        self.assertEqual(status, 200, setup)
        modify_calls = mock.Mock()

        def modify_then_conflict(*_args, **_kwargs):
            modify_calls()
            with self.api.get_db() as writer:
                writer.execute(
                    "UPDATE employer_profiles SET payment_method_id='pm_racer' WHERE user_id=1"
                )
                writer.commit()
            return SimpleNamespace(id="cus_durable")

        self.api.stripe.Customer.modify = modify_then_conflict
        status, payload = self.request(
            "/payments/confirm-setup-employer", "buyer-token", {"payment_method_id": "pm_exact"}
        )
        self.assertEqual(status, 409, payload)
        self.assert_frozen("customer_modify")
        with self.api.get_db() as db:
            self.assertEqual(db.execute(
                "SELECT payment_method_id FROM employer_profiles WHERE user_id=1"
            ).fetchone()[0], "pm_racer")
            attach = db.execute(
                "SELECT status,manual_review_required FROM payment_setup_operations "
                "WHERE operation_kind='payment_method_attach'"
            ).fetchone()
        self.assertEqual(tuple(attach), ("committed", 0))
        status, _ = self.request(
            "/payments/confirm-setup-employer", "buyer-token", {"payment_method_id": "pm_exact"}
        )
        self.assertEqual(status, 409)
        self.assertEqual(modify_calls.call_count, 1)
        with self.api.get_db() as db:
            db.execute(
                "DELETE FROM payment_setup_operations WHERE operation_kind IN ('payment_method_attach','customer_modify')"
            )
            db.execute("UPDATE employer_profiles SET payment_method_id=NULL WHERE user_id=1")
            db.commit()

        def modify_then_exact(*_args, **_kwargs):
            with self.api.get_db() as writer:
                writer.execute(
                    "UPDATE employer_profiles SET payment_method_id='pm_exact' WHERE user_id=1"
                )
                writer.commit()
            return SimpleNamespace(id="cus_durable")

        self.api.stripe.Customer.modify = modify_then_exact
        status, payload = self.request(
            "/payments/confirm-setup-employer", "buyer-token", {"payment_method_id": "pm_exact"}
        )
        self.assertEqual(status, 200, payload)
        self.assertEqual(payload["payment_method_id"], "pm_exact")

    def test_worker_account_create_conflict_freezes_and_exact_readback_is_allowed(self):
        account_calls = mock.Mock()

        def account_then_conflict(**_kwargs):
            account_calls()
            with self.api.get_db() as writer:
                writer.execute(
                    "UPDATE worker_profiles SET payout_account_id='acct_racer' WHERE user_id=2"
                )
                writer.commit()
            return SimpleNamespace(id="acct_processor")

        self.api.stripe.Account.create = account_then_conflict
        status, payload = self.request("/payments/setup-worker", "worker-token")
        self.assertEqual(status, 409, payload)
        self.assert_frozen("account_create")
        status, _ = self.request("/payments/setup-worker", "worker-token")
        self.assertEqual(status, 409)
        self.assertEqual(account_calls.call_count, 1)
        with self.api.get_db() as db:
            db.execute("DELETE FROM payment_setup_operations")
            db.execute("UPDATE worker_profiles SET payout_account_id=NULL WHERE user_id=2")
            db.commit()

        def account_then_exact(**_kwargs):
            with self.api.get_db() as writer:
                writer.execute(
                    "UPDATE worker_profiles SET payout_account_id='acct_exact' WHERE user_id=2"
                )
                writer.commit()
            return SimpleNamespace(id="acct_exact")

        self.api.stripe.Account.create = account_then_exact
        status, payload = self.request("/payments/setup-worker", "worker-token")
        self.assertEqual(status, 200, payload)
        self.assertEqual(payload["account_id"], "acct_exact")


class V6PaymentSetupSchemaMatrixTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        os.environ["DATABASE_PATH"] = os.path.join(self.tmp.name, "schema.db")
        os.environ["DISABLE_AUTO_SEED"] = "1"
        self.api = load_api_core()
        self.api._db_path_resolved = None
        self.api.init_db()

    def tearDown(self):
        self.tmp.cleanup()
        os.environ.pop("DATABASE_PATH", None)
        os.environ.pop("DISABLE_AUTO_SEED", None)

    def test_malformed_table_not_null_check_default_and_fk_fail_closed(self):
        with self.api.get_db() as db:
            canonical = db.execute(
                "SELECT sql FROM sqlite_master WHERE type='table' AND name='payment_setup_operations'"
            ).fetchone()[0]
        mutations = {
            "not_null": canonical.replace("operation_kind TEXT NOT NULL", "operation_kind TEXT"),
            "status_check": canonical.replace(" CHECK(status IN ('prepared','unknown','failed','committed'))", ""),
            "default": canonical.replace(
                " DEFAULT 0 CHECK(manual_review_required", " DEFAULT 1 CHECK(manual_review_required"
            ),
            "foreign_key": canonical.replace(" REFERENCES users(id)", ""),
        }
        for name, malformed in mutations.items():
            with self.subTest(name=name):
                with self.api.get_db() as db:
                    db.execute("DROP TABLE payment_setup_operations")
                    db.execute(malformed)
                    db.commit()
                with self.assertRaisesRegex(RuntimeError, "payment_setup_operations exact table schema"):
                    self.api.init_db()
                with self.api.get_db() as db:
                    db.execute("DROP TABLE payment_setup_operations")
                    db.execute(canonical)
                    db.commit()
                self.api.init_db()

    def test_canonical_unique_index_collation_direction_order_object_and_trigger_poisoning(self):
        scenarios = {
            "collation": "CREATE UNIQUE INDEX idx_payment_setup_operation_key ON payment_setup_operations(operation_key COLLATE NOCASE)",
            "direction": "CREATE UNIQUE INDEX idx_payment_setup_operation_key ON payment_setup_operations(operation_key DESC)",
            "order": "CREATE UNIQUE INDEX idx_payment_setup_operation_key ON payment_setup_operations(operation_key,id)",
            "wrong_column": "CREATE UNIQUE INDEX idx_payment_setup_operation_key ON payment_setup_operations(processor_idempotency_key)",
        }
        for name, poison in scenarios.items():
            with self.subTest(name=name):
                with self.api.get_db() as db:
                    db.execute("DROP INDEX idx_payment_setup_operation_key")
                    db.execute(poison)
                    db.commit()
                with self.assertRaisesRegex(RuntimeError, "idx_payment_setup_operation_key exact index schema"):
                    self.api.init_db()
                with self.api.get_db() as db:
                    db.execute("DROP INDEX idx_payment_setup_operation_key")
                    db.commit()
                self.api.init_db()
        with self.api.get_db() as db:
            db.execute("DROP INDEX idx_payment_setup_processor_key")
            db.execute("CREATE TABLE idx_payment_setup_processor_key(id INTEGER)")
            db.commit()
        with self.assertRaises((RuntimeError, sqlite3.OperationalError)):
            self.api.init_db()
        with self.api.get_db() as db:
            db.execute("DROP TABLE idx_payment_setup_processor_key")
            db.execute(
                "CREATE TRIGGER poison_payment_setup AFTER INSERT ON payment_setup_operations BEGIN SELECT 1; END"
            )
            db.commit()
        with self.assertRaisesRegex(RuntimeError, "unexpected payment setup trigger"):
            self.api.init_db()

    def test_concurrent_initialization_and_clean_prior_schema_migration(self):
        with self.api.get_db() as db:
            db.execute("DROP TABLE payment_setup_operations")
            db.commit()
        barrier = threading.Barrier(4)
        errors = []

        def initialize():
            try:
                barrier.wait()
                self.api.init_db()
            except Exception as exc:
                errors.append(exc)

        threads = [threading.Thread(target=initialize) for _ in range(4)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(10)
        self.assertTrue(all(not thread.is_alive() for thread in threads))
        self.assertEqual(errors, [])
        with self.api.get_db() as db:
            self.api.validate_required_payment_setup_schema(db)
            indexes = {
                row[1]: row for row in db.execute("PRAGMA index_list('payment_setup_operations')")
            }
        self.assertEqual(indexes["idx_payment_setup_operation_key"][2], 1)
        self.assertEqual(indexes["idx_payment_setup_processor_key"][2], 1)


class V6ExhaustiveRedactionTests(unittest.TestCase):
    def test_nested_never_secret_corpus_default_and_reveal_sensitive(self):
        from tools.reconcile_funding_attempts import redact_report
        secrets = [
            "pi_123_secret_alpha", "seti_456_secret_beta", "ghh_ABC_def-123",
            "Authorization: Bearer eyJhbGci.payload.signature", "Bearer opaque-token_123",
            "Authorization: Basic dXNlcjpwYXNz", "Basic dXNlcjpwYXNz",
            "-----BEGIN PRIVATE KEY-----\nmaterial\n-----END PRIVATE KEY-----",
            "-----BEGIN RSA PRIVATE KEY-----", "whsec_webhook_secret",
            "sk_live_secretvalue", "sk_test_secretvalue", "rk_live_secretvalue",
            "rk_test_secretvalue",
        ]
        corpus = {
            "ordinary": "safe diagnostic",
            "request_fingerprint": "fp_visible_only_when_revealed",
            "processor_object_id": "pi_public_identifier",
            "local_error_message": [
                {"nested": f"prefix {secret} suffix", "conflict": [secret]}
                for secret in secrets
            ],
            "conflict_evidence": {"observed_snapshot": tuple(secrets)},
            "credentialEnvelope": "even-safe-looking-value",
        }
        for reveal in (False, True):
            with self.subTest(reveal=reveal):
                rendered = redact_report(corpus, reveal)
                serialized = json.dumps(rendered, sort_keys=True)
                for secret in secrets:
                    self.assertNotIn(secret, serialized)
                self.assertEqual(rendered["credentialEnvelope"], "[REDACTED]")
                self.assertEqual(rendered["ordinary"], "safe diagnostic")
                if reveal:
                    self.assertEqual(rendered["request_fingerprint"], "fp_visible_only_when_revealed")
                    self.assertEqual(rendered["processor_object_id"], "pi_public_identifier")
                else:
                    self.assertEqual(rendered["request_fingerprint"], "[REDACTED]")
                    self.assertEqual(rendered["processor_object_id"], "[REDACTED]")


class V6Stripe13AccountLinkBoundaryTests(unittest.TestCase):
    setUp = PaymentSetupAttemptLedgerTests.setUp
    tearDown = PaymentSetupAttemptLedgerTests.tearDown
    request = PaymentSetupAttemptLedgerTests.request
    def _install_sdk_boundary(self, responses):
        import stripe as stripe_sdk
        from stripe._http_client import HTTPClient

        class FakeTransport(HTTPClient):
            name = "deterministic-account-link-no-network"

            def __init__(self, queued):
                super().__init__()
                self.responses = list(queued)
                self.calls = []
                self.idempotent_responses = {}
                self.lock = threading.Lock()

            def request(self, method, url, headers, post_data=None, *, _usage=None):
                with self.lock:
                    normalized_headers = dict(headers or {})
                    self.calls.append((method, url, normalized_headers, post_data))
                    idempotency_key = normalized_headers.get("Idempotency-Key")
                    if idempotency_key in self.idempotent_responses:
                        return self.idempotent_responses[idempotency_key]
                    response = self.responses.pop(0)
                if isinstance(response, Exception):
                    raise response
                body, status = response
                result = (json.dumps(body), status, {
                    "content-type": "application/json", "request-id": "req_account_link_v6"
                })
                if idempotency_key:
                    with self.lock:
                        self.idempotent_responses[idempotency_key] = result
                return result

        transport = FakeTransport(responses)
        client = stripe_sdk.StripeClient("sdk_boundary_non_network_key", http_client=transport)

        class SDKAccountLinkBoundary:
            @staticmethod
            def create(**kwargs):
                idempotency_key = kwargs.pop("idempotency_key")
                return client.v1.account_links.create(
                    kwargs, {"idempotency_key": idempotency_key}
                )

        self.api.stripe.AccountLink = SDKAccountLinkBoundary
        self.api.stripe.StripeError = stripe_sdk.StripeError
        self.api.stripe.APIConnectionError = stripe_sdk.APIConnectionError
        self.api.stripe.InvalidRequestError = stripe_sdk.InvalidRequestError
        with self.api.get_db() as db:
            db.execute(
                "INSERT OR IGNORE INTO worker_profiles(user_id,payout_account_id) VALUES (2,'acct_sdk_boundary')"
            )
            db.execute("UPDATE worker_profiles SET payout_account_id='acct_sdk_boundary' WHERE user_id=2")
            db.commit()
        self.api.retrieve_live_connect_account = lambda *_args, **_kwargs: SimpleNamespace(
            id="acct_sdk_boundary", charges_enabled=True, payouts_enabled=True,
            capabilities={"transfers": "active"},
            details_submitted=True, requirements=SimpleNamespace(currently_due=[]),
        )
        return transport

    def _all_database_bytes(self):
        chunks = []
        with sqlite3.connect(self.db_path) as db:
            for row in db.execute("SELECT sql FROM sqlite_master WHERE sql IS NOT NULL"):
                chunks.append(str(row[0]))
            for table in ["payment_setup_operations", "audit_log"]:
                for row in db.execute(f"SELECT * FROM {table}"):
                    chunks.extend(str(value) for value in row)
        backup = os.path.join(self.tmp.name, "account-link.backup")
        source = sqlite3.connect(self.db_path)
        target = sqlite3.connect(backup)
        source.backup(target)
        target.close()
        source.close()
        with open(backup, "rb") as handle:
            chunks.append(handle.read().decode("utf-8", "ignore"))
        return "\n".join(chunks)

    def test_no_id_exact_replay_expiration_refresh_and_no_url_persistence(self):
        now = int(time.time())
        urls = [
            "https://connect.stripe.test/no-id-first?secret=alpha",
            "https://connect.stripe.test/no-id-replay?secret=beta",
            "https://connect.stripe.test/explicit-refresh?secret=gamma",
            "https://connect.stripe.test/expired-refresh?secret=delta",
        ]
        responses = [
            ({"object": "account_link", "url": urls[0], "created": now, "expires_at": now + 300}, 200),
            ({"object": "account_link", "url": urls[1], "created": now, "expires_at": now + 300}, 200),
            ({"object": "account_link", "url": urls[2], "created": now, "expires_at": now + 300}, 200),
            ({"object": "account_link", "url": urls[3], "created": now, "expires_at": now + 300}, 200),
        ]
        transport = self._install_sdk_boundary(responses)
        stderr = io.StringIO()
        with contextlib.redirect_stderr(stderr):
            first_status, first = self.request("/payments/setup-worker", "worker-token")
            replay_status, replay = self.request("/payments/setup-worker", "worker-token")
            refresh_status, refresh = self.request(
                "/payments/setup-worker", "worker-token", {"refresh": True}
            )
            with self.api.get_db() as db:
                db.execute(
                    "UPDATE payment_setup_operations SET result_json=json_set(result_json,'$.expires_at',?) "
                    "WHERE operation_kind='account_link_create' "
                    "AND json_extract(result_json,'$.generation')=2",
                    [now - 1],
                )
                db.commit()
            expired_status, expired = self.request("/payments/setup-worker", "worker-token")
        self.assertEqual((first_status, replay_status, refresh_status, expired_status), (200, 200, 200, 200))
        self.assertEqual(
            [first["onboarding_url"], replay["onboarding_url"], refresh["onboarding_url"], expired["onboarding_url"]],
            [urls[0], urls[0], urls[1], urls[2]],
        )
        self.assertNotIn("id", first)
        self.assertEqual(len(transport.calls), 4)
        idempotency_keys = [call[2]["Idempotency-Key"] for call in transport.calls]
        self.assertEqual(idempotency_keys[0], idempotency_keys[1])
        self.assertNotEqual(idempotency_keys[1], idempotency_keys[2])
        self.assertNotEqual(idempotency_keys[2], idempotency_keys[3])
        persisted = self._all_database_bytes() + stderr.getvalue()
        for url in urls:
            self.assertNotIn(url, persisted)

    def test_definitive_missing_url_and_ambiguous_transport_freeze_without_secret(self):
        import stripe as stripe_sdk
        secret_url = "https://connect.stripe.test/must-never-persist?secret=epsilon"
        transport = self._install_sdk_boundary([
            ({"object": "account_link", "created": int(time.time()), "expires_at": int(time.time()) + 300}, 200),
        ])
        status, payload = self.request("/payments/setup-worker", "worker-token")
        self.assertEqual(status, 409, payload)
        self.assertEqual(len(transport.calls), 1)
        with self.api.get_db() as db:
            row = db.execute(
                "SELECT status,manual_review_required,error_code FROM payment_setup_operations "
                "WHERE operation_kind='account_link_create'"
            ).fetchone()
        self.assertEqual(tuple(row), ("unknown", 1, "processor_outcome_ValueError"))
        replay_status, _ = self.request("/payments/setup-worker", "worker-token")
        self.assertEqual(replay_status, 409)
        self.assertEqual(len(transport.calls), 1)

        with self.api.get_db() as db:
            db.execute("DELETE FROM payment_setup_operations")
            db.commit()
        transport = self._install_sdk_boundary([(
            {"error": {"type": "invalid_request_error", "message": "definitive rejection", "param": "type"}},
            400,
        )])
        definitive_status, definitive = self.request("/payments/setup-worker", "worker-token")
        self.assertEqual(definitive_status, 409, definitive)
        self.assertEqual(len(transport.calls), 1)
        with self.api.get_db() as db:
            row = db.execute(
                "SELECT status,manual_review_required,error_code FROM payment_setup_operations "
                "WHERE operation_kind='account_link_create'"
            ).fetchone()
        self.assertEqual(tuple(row), ("failed", 0, "processor_rejected_InvalidRequestError"))

        with self.api.get_db() as db:
            db.execute("DELETE FROM payment_setup_operations")
            db.commit()
        transport = self._install_sdk_boundary([
            stripe_sdk.APIConnectionError("lost response " + secret_url)
        ])
        stderr = io.StringIO()
        with contextlib.redirect_stderr(stderr):
            ambiguous_status, ambiguous = self.request("/payments/setup-worker", "worker-token")
        self.assertEqual(ambiguous_status, 409, ambiguous)
        self.assertEqual(len(transport.calls), 1)
        persisted = self._all_database_bytes() + stderr.getvalue()
        self.assertNotIn(secret_url, persisted)
        self.assertNotIn("lost response", persisted)

    def test_exact_account_link_operation_concurrency_uses_one_idempotency_identity(self):
        now = int(time.time())
        urls = [
            "https://connect.stripe.test/concurrent-one?secret=zeta",
            "https://connect.stripe.test/concurrent-two?secret=eta",
        ]
        transport = self._install_sdk_boundary([
            ({"object": "account_link", "url": urls[0], "created": now, "expires_at": now + 300}, 200),
            ({"object": "account_link", "url": urls[1], "created": now, "expires_at": now + 300}, 200),
        ])
        barrier = threading.Barrier(2)
        outcomes = []
        errors = []

        def call_operation():
            try:
                barrier.wait()
                with self.api.get_db() as db:
                    result, replay = self.api._payment_setup_operation(
                        db, user_id=2, operation_kind="account_link_create",
                        binding={"account_id": "acct_sdk_boundary", "generation": 99},
                        processor_call=lambda key: self.api.stripe.AccountLink.create(
                            account="acct_sdk_boundary", refresh_url="https://safe.test/refresh",
                            return_url="https://safe.test/return", type="account_onboarding",
                            idempotency_key=key,
                        ),
                        result_builder=lambda link: {
                            "processor_object_id": "account-link:acct_sdk_boundary:99",
                            "account_id": "acct_sdk_boundary", "generation": 99,
                            "expires_at": link.expires_at,
                        },
                        replay_result_builder=lambda _durable, transient: transient,
                    )
                    outcomes.append((result, replay))
            except Exception as exc:
                errors.append(exc)

        threads = [threading.Thread(target=call_operation) for _ in range(2)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(10)
        self.assertEqual(errors, [])
        self.assertEqual(len(outcomes), 2)
        self.assertEqual(len(transport.calls), 1)
        self.assertTrue(
            transport.calls[0][2]["Idempotency-Key"].startswith("payment-setup:2:account_link_create:")
        )
        persisted = self._all_database_bytes()
        for url in urls:
            self.assertNotIn(url, persisted)


class V6APIKeyRouteMatrixTests(unittest.TestCase):
    setUp = PaymentSetupAttemptLedgerTests.setUp
    tearDown = PaymentSetupAttemptLedgerTests.tearDown
    request = PaymentSetupAttemptLedgerTests.request
    def _insert_key(self, key_id, raw_key, scopes):
        with self.api.get_db() as db:
            db.execute(
                "INSERT OR IGNORE INTO users(id,email,name,password_hash) VALUES (3,'matrix@example.test','Matrix','x')"
            )
            db.execute(
                """INSERT INTO api_keys
                   (id,user_id,key_hash,key_prefix,name,scopes,is_active,total_requests)
                   VALUES (?,?,?,?,?,?,1,0)""",
                [key_id, 3, hashlib.sha256(raw_key.encode()).hexdigest(), raw_key[:8],
                 f"matrix-{key_id}", json.dumps(scopes)],
            )
            db.commit()

    def _matrix_request(self, method, path, token=None, payload=None, api_key=None):
        for cached in ("body_cache", "raw_body"):
            if hasattr(self.api._request_ctx, cached):
                delattr(self.api._request_ctx, cached)
        body = json.dumps(payload or {})
        ctx = self.api._request_ctx
        ctx.request_method = method
        ctx.path_info = path
        ctx.query_string = ""
        ctx.http_authorization = f"Bearer {token}" if token else ""
        ctx.http_x_api_key = api_key or ""
        ctx.stdin_data = body
        ctx.content_type = "application/json"
        ctx.content_length = str(len(body.encode()))
        ctx.remote_addr = "127.0.0.1"
        with contextlib.redirect_stdout(io.StringIO()) as out:
            self.api.handle_request()
        return parse_cgi_output(out.getvalue())

    def _domain_snapshot(self):
        tables = [
            "orders", "milestones", "escrow_holds", "funding_attempts",
            "payout_release_attempts", "payment_setup_operations",
            "employer_profiles", "worker_profiles", "services",
        ]
        with self.api.get_db() as db:
            return {
                table: [tuple(row) for row in db.execute(f"SELECT * FROM {table} ORDER BY 1")]
                for table in tables
            }

    def test_read_and_unrelated_keys_are_403_before_domain_or_processor_side_effects(self):
        keys = {
            "ghh_matrix_read_only": ["read"],
            "ghh_matrix_unrelated": ["payments:setup"],
        }
        for number, (raw_key, scopes) in enumerate(keys.items(), 40):
            self._insert_key(number, raw_key, scopes)
            routes = [
                ("/payments/setup-employer", {}),
                ("/payments/confirm-setup-employer", {"payment_method_id": "pm_blocked"}),
                ("/payments/setup-worker", {}),
                ("/payments/prepare-order-payment", {"order_id": 999}),
                ("/payments/fund-escrow", {"order_id": 999, "amount": "10.00", "operation_id": "op-matrix"}),
                ("/orders/999/approve", {}),
                ("/orders/999/complete", {}),
                ("/orders/999/dispute", {"reason": "matrix"}),
                ("/api-keys", {"name": "blocked", "scopes": ["read"]}),
                ("/api-keys/999/revoke", {}),
                ("/notifications/read-all", {}),
                ("/services", {"title": "blocked"}),
            ]
            if raw_key.endswith("unrelated"):
                # A setup-only key is permitted only on setup; all matrix denial
                # routes below use a different unrelated financial/write scope.
                routes = routes[3:]
            before = self._domain_snapshot()
            processor_calls = len(self.calls)
            for path, body in routes:
                with self.subTest(key=raw_key, path=path):
                    status, payload = self._matrix_request("POST", path, api_key=raw_key, payload=body)
                    self.assertEqual(status, 403, payload)
                    self.assertEqual(payload, {"error": "API key scope does not permit this route"})
                    self.assertEqual(self._domain_snapshot(), before)
                    self.assertEqual(len(self.calls), processor_calls)

    def test_intended_get_and_permitted_write_setup_fund_and_invalid_scope_semantics(self):
        self._insert_key(50, "ghh_matrix_read_positive", ["read"])
        status, payload = self._matrix_request(
            "GET", "/orders", api_key="ghh_matrix_read_positive"
        )
        self.assertEqual(status, 200, payload)

        self._insert_key(51, "ghh_matrix_write_positive", ["write"])
        status, payload = self._matrix_request(
            "POST", "/notifications/read-all", api_key="ghh_matrix_write_positive", payload={}
        )
        self.assertEqual(status, 200, payload)

        self._insert_key(52, "ghh_matrix_setup_positive", ["payments:setup"])
        status, payload = self._matrix_request(
            "POST", "/payments/setup-worker", api_key="ghh_matrix_setup_positive", payload={}
        )
        self.assertEqual(status, 200, payload)

        self._insert_key(53, "ghh_matrix_fund_positive", ["payments:fund"])
        status, payload = self._matrix_request(
            "POST", "/payments/fund-escrow", api_key="ghh_matrix_fund_positive",
            payload={"order_id": 999, "amount": "10.00", "operation_id": "matrix-fund"},
        )
        self.assertNotEqual(status, 403, payload)

        # Lifecycle release remains owner-session-only even if the key carries
        # the historical payments:release string.
        self._insert_key(54, "ghh_matrix_release_denied", ["payments:release"])
        status, payload = self._matrix_request(
            "POST", "/orders/999/approve", api_key="ghh_matrix_release_denied"
        )
        self.assertEqual(status, 403, payload)

        status, payload = self.request(
            "/api-keys", "buyer-token", {"name": "invalid", "scopes": ["read", "bogus:scope"]}
        )
        self.assertEqual(status, 400, payload)
        self.assertEqual(payload, {"error": "Invalid scope: bogus:scope"})
        with self.api.get_db() as db:
            self.assertEqual(
                db.execute("SELECT COUNT(*) FROM api_keys WHERE name='invalid'").fetchone()[0], 0
            )


if __name__ == "__main__":
    unittest.main()
