import contextlib
import hashlib
import io
import json
import os
import tempfile
import time
import unittest
from types import SimpleNamespace
from unittest import mock

import stripe as stripe_sdk

from test_deep_audit_regressions import load_api_core, parse_cgi_output


class PaymentSetupAttemptLedgerTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self.tmp.name, "payment-setup.db")
        os.environ["DATABASE_PATH"] = self.db_path
        os.environ["DISABLE_AUTO_SEED"] = "1"
        self.api = load_api_core()
        self.api._db_path_resolved = None
        self.api.init_db()
        self.api.STRIPE_AVAILABLE = True
        self.api.STRIPE_SECRET_KEY = "configured-test-key"
        with self.api.get_db() as db:
            db.execute("INSERT INTO users (id,email,name,password_hash) VALUES (1,'buyer@example.com','Buyer','x')")
            db.execute("INSERT INTO users (id,email,name,password_hash) VALUES (2,'worker@example.com','Worker','x')")
            db.execute("INSERT INTO sessions (user_id,token,expires_at) VALUES (1,'buyer-token',datetime('now','+1 day'))")
            db.execute("INSERT INTO sessions (user_id,token,expires_at) VALUES (2,'worker-token',datetime('now','+1 day'))")
            db.commit()
        self.calls = []

        def probe(name, result):
            def callback(*args, **kwargs):
                self.calls.append((name, args, kwargs))
                route_db = self.api._request_ctx.processor_boundary_db
                self.assertFalse(route_db.in_transaction, name)
                with self.api.get_db() as contender:
                    contender.execute("BEGIN IMMEDIATE")
                    contender.rollback()
                with self.api.get_db() as inspect:
                    row = inspect.execute(
                        "SELECT * FROM payment_setup_operations WHERE operation_kind=? ORDER BY id DESC LIMIT 1",
                        [name],
                    ).fetchone()
                    self.assertIsNotNone(row, name)
                    self.assertEqual(row["status"], "prepared", name)
                    self.assertTrue(row["processor_idempotency_key"], name)
                return result
            return callback

        self.customer_create = probe("customer_create", SimpleNamespace(id="cus_durable"))
        self.setup_intent_create = probe("setup_intent_create", SimpleNamespace(id="seti_durable", client_secret="seti_secret"))

        def setup_intent_retrieve(intent_id):
            self.calls.append(("setup_intent_retrieve", (intent_id,), {}))
            self.assertFalse(self.api._request_ctx.processor_boundary_db.in_transaction)
            return SimpleNamespace(id=intent_id, client_secret="seti_secret_replayed")

        self.setup_intent_retrieve = setup_intent_retrieve
        self.pm_attach = probe("payment_method_attach", SimpleNamespace(id="pm_exact"))
        self.customer_modify = probe("customer_modify", SimpleNamespace(id="cus_durable"))
        self.account_create = probe("account_create", SimpleNamespace(id="acct_durable"))
        self.account_link_create = probe(
            "account_link_create",
            SimpleNamespace(
                id="link_durable", url="https://example.invalid/onboard",
                expires_at=int(time.time()) + 300,
            ),
        )
        self.api.stripe = SimpleNamespace(
            Customer=SimpleNamespace(create=self.customer_create, modify=self.customer_modify),
            SetupIntent=SimpleNamespace(
                create=self.setup_intent_create, retrieve=self.setup_intent_retrieve
            ),
            PaymentMethod=SimpleNamespace(attach=self.pm_attach),
            Account=SimpleNamespace(create=self.account_create),
            AccountLink=SimpleNamespace(create=self.account_link_create),
            StripeError=stripe_sdk.StripeError,
            APIConnectionError=stripe_sdk.APIConnectionError,
        )
        self.api.STRIPE_ERROR = stripe_sdk.StripeError

    def tearDown(self):
        self.tmp.cleanup()
        os.environ.pop("DATABASE_PATH", None)
        os.environ.pop("DISABLE_AUTO_SEED", None)

    def request(self, path, token=None, payload=None, api_key=None):
        for cached in ("body_cache", "raw_body"):
            if hasattr(self.api._request_ctx, cached):
                delattr(self.api._request_ctx, cached)
        body = json.dumps(payload or {})
        ctx = self.api._request_ctx
        ctx.request_method = "POST"
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

    def test_every_payment_setup_processor_boundary_has_no_writer_and_durable_identity(self):
        status, payload = self.request("/payments/setup-employer", "buyer-token")
        self.assertEqual(status, 200, payload)
        status, payload = self.request(
            "/payments/confirm-setup-employer", "buyer-token", {"payment_method_id": "pm_exact"}
        )
        self.assertEqual(status, 200, payload)
        status, payload = self.request("/payments/setup-worker", "worker-token")
        self.assertEqual(status, 200, payload)
        self.assertEqual(
            [item[0] for item in self.calls],
            ["customer_create", "setup_intent_create", "payment_method_attach", "customer_modify", "account_create", "account_link_create"],
        )
        with self.api.get_db() as db:
            rows = db.execute("SELECT * FROM payment_setup_operations ORDER BY id").fetchall()
            self.assertEqual(len(rows), 6)
            self.assertTrue(all(row["status"] == "committed" for row in rows))
            self.assertEqual(db.execute("SELECT stripe_customer_id FROM employer_profiles WHERE user_id=1").fetchone()[0], "cus_durable")
            self.assertEqual(db.execute("SELECT payout_account_id FROM worker_profiles WHERE user_id=2").fetchone()[0], "acct_durable")

    def test_ambiguous_customer_create_is_durable_and_exact_retry_is_blocked(self):
        calls = {"count": 0}

        def ambiguous(**kwargs):
            calls["count"] += 1
            self.assertFalse(self.api._request_ctx.processor_boundary_db.in_transaction)
            with self.api.get_db() as contender:
                contender.execute("BEGIN IMMEDIATE")
                contender.rollback()
            raise stripe_sdk.APIConnectionError("response lost")

        self.api.stripe.Customer.create = ambiguous
        status, _ = self.request("/payments/setup-employer", "buyer-token")
        self.assertEqual(status, 409)
        self.api.stripe.Customer.create = lambda **kwargs: SimpleNamespace(id="cus_must_not_create")
        status, _ = self.request("/payments/setup-employer", "buyer-token")
        self.assertEqual(status, 409)
        self.assertEqual(calls["count"], 1)
        with self.api.get_db() as db:
            row = db.execute("SELECT * FROM payment_setup_operations WHERE operation_kind='customer_create'").fetchone()
            self.assertEqual(row["status"], "unknown")
            self.assertEqual(row["manual_review_required"], 1)
            self.assertIsNone(db.execute("SELECT stripe_customer_id FROM employer_profiles WHERE user_id=1").fetchone()[0])

    def test_setup_intent_secret_is_transient_across_database_backup_logs_and_restart_replay(self):
        first_secret = "seti_v5_client_secret_first"
        replay_secret = "seti_v5_client_secret_replayed"
        self.setup_intent_create = self.api.stripe.SetupIntent.create = mock.Mock(
            side_effect=lambda **kwargs: SimpleNamespace(
                id="seti_durable", client_secret=first_secret
            )
        )
        stderr = io.StringIO()
        with contextlib.redirect_stderr(stderr):
            first_status, first = self.request("/payments/setup-employer", "buyer-token")
        self.assertEqual(first_status, 200, first)
        self.assertEqual(first["client_secret"], first_secret)

        backup_path = os.path.join(self.tmp.name, "payment-setup-backup.db")
        with self.api.get_db() as source, __import__("sqlite3").connect(backup_path) as backup:
            source.backup(backup)
        with self.api.get_db() as db:
            setup_row = db.execute(
                "SELECT processor_object_id,result_json,error_code FROM payment_setup_operations "
                "WHERE operation_kind='setup_intent_create'"
            ).fetchone()
            durable_dump = json.dumps(dict(setup_row), sort_keys=True)
            audit_dump = json.dumps([
                dict(row) for row in db.execute("SELECT * FROM audit_log").fetchall()
            ], sort_keys=True)
        self.assertEqual(setup_row["processor_object_id"], "seti_durable")
        for exposed in (durable_dump, audit_dump, stderr.getvalue()):
            self.assertNotIn(first_secret, exposed)
        with open(backup_path, "rb") as backup_file:
            self.assertNotIn(first_secret.encode(), backup_file.read())

        # Simulate process restart. Durable identity replays, while the usable
        # secret must be fetched transiently from Stripe with no SQLite writer.
        restarted = load_api_core()
        restarted._db_path_resolved = None
        restarted.STRIPE_AVAILABLE = True
        restarted.STRIPE_SECRET_KEY = "configured-test-key"
        retrieve = mock.Mock()

        def retrieve_transient(intent_id):
            self.assertEqual(intent_id, "seti_durable")
            self.assertFalse(restarted._request_ctx.processor_boundary_db.in_transaction)
            with restarted.get_db() as contender:
                contender.execute("BEGIN IMMEDIATE")
                contender.rollback()
            return SimpleNamespace(id=intent_id, client_secret=replay_secret)

        retrieve.side_effect = retrieve_transient
        restarted.stripe = SimpleNamespace(
            Customer=SimpleNamespace(create=mock.Mock(side_effect=AssertionError("no create"))),
            SetupIntent=SimpleNamespace(
                create=mock.Mock(side_effect=AssertionError("no create")),
                retrieve=retrieve,
            ),
            StripeError=stripe_sdk.StripeError,
            APIConnectionError=stripe_sdk.APIConnectionError,
        )
        self.api = restarted
        replay_stderr = io.StringIO()
        with contextlib.redirect_stderr(replay_stderr):
            replay_status, replay = self.request("/payments/setup-employer", "buyer-token")
        self.assertEqual(replay_status, 200, replay)
        self.assertEqual(replay["client_secret"], replay_secret)
        retrieve.assert_called_once_with("seti_durable")
        with restarted.get_db() as db:
            all_db_text = "\n".join(
                str(value)
                for row in db.execute("SELECT * FROM payment_setup_operations").fetchall()
                for value in tuple(row)
            )
        self.assertNotIn(first_secret, all_db_text)
        self.assertNotIn(replay_secret, all_db_text)
        self.assertNotIn(first_secret, replay_stderr.getvalue())
        self.assertNotIn(replay_secret, replay_stderr.getvalue())

    def test_stripe_13_sdk_boundary_encodes_setup_create_and_retrieve_without_network(self):
        from stripe._http_client import HTTPClient

        class FakeTransport(HTTPClient):
            name = "deterministic-no-network"

            def __init__(self):
                super().__init__()
                self.calls = []

            def request(self, method, url, headers, post_data=None, *, _usage=None):
                self.calls.append((method, url, dict(headers or {}), post_data))
                body = {
                    "id": "seti_sdk_boundary",
                    "object": "setup_intent",
                    "client_secret": (
                        "seti_sdk_secret_created" if method == "post"
                        else "seti_sdk_secret_retrieved"
                    ),
                }
                return json.dumps(body), 200, {
                    "content-type": "application/json", "request-id": "req_fake_v5"
                }

        transport = FakeTransport()
        client = stripe_sdk.StripeClient(
            "sdk_boundary_non_network_key", http_client=transport
        )

        class SDKSetupIntentBoundary:
            @staticmethod
            def create(**kwargs):
                idempotency_key = kwargs.pop("idempotency_key")
                return client.v1.setup_intents.create(
                    kwargs, {"idempotency_key": idempotency_key}
                )

            @staticmethod
            def retrieve(intent_id):
                return client.v1.setup_intents.retrieve(intent_id)

        with self.api.get_db() as db:
            db.execute(
                "INSERT INTO employer_profiles (user_id,stripe_customer_id) VALUES (1,'cus_sdk_boundary')"
            )
            db.commit()
        self.api.stripe = SimpleNamespace(
            SetupIntent=SDKSetupIntentBoundary,
            StripeError=stripe_sdk.StripeError,
            APIConnectionError=stripe_sdk.APIConnectionError,
        )
        first_status, first = self.request("/payments/setup-employer", "buyer-token")
        replay_status, replay = self.request("/payments/setup-employer", "buyer-token")
        self.assertEqual((first_status, replay_status), (200, 200), (first, replay))
        self.assertEqual(first["client_secret"], "seti_sdk_secret_created")
        self.assertEqual(replay["client_secret"], "seti_sdk_secret_retrieved")
        self.assertEqual(stripe_sdk.VERSION, "13.2.0")
        self.assertEqual([call[0] for call in transport.calls], ["post", "get"])
        create_call, retrieve_call = transport.calls
        self.assertEqual(create_call[1], "https://api.stripe.com/v1/setup_intents")
        self.assertIn("customer=cus_sdk_boundary", create_call[3])
        self.assertIn("payment_method_types[0]=card", create_call[3])
        self.assertIn("metadata[user_id]=1", create_call[3])
        self.assertTrue(create_call[2]["Idempotency-Key"].startswith("payment-setup:"))
        self.assertEqual(
            retrieve_call[1],
            "https://api.stripe.com/v1/setup_intents/seti_sdk_boundary",
        )
        self.assertIsNone(retrieve_call[3])
        with self.api.get_db() as db:
            result_json = db.execute(
                "SELECT result_json FROM payment_setup_operations WHERE operation_kind='setup_intent_create'"
            ).fetchone()[0]
        self.assertEqual(
            json.loads(result_json),
            {
                "processor_object_id": "seti_sdk_boundary",
                "setup_intent_id": "seti_sdk_boundary",
            },
        )

    def test_committed_setup_calls_replay_identity_with_only_transient_retrieve_io(self):
        first_status, first = self.request("/payments/setup-employer", "buyer-token")
        self.assertEqual(first_status, 200, first)
        count = len(self.calls)
        second_status, second = self.request("/payments/setup-employer", "buyer-token")
        self.assertEqual(second_status, 200, second)
        self.assertEqual(len(self.calls), count + 1)
        self.assertEqual(self.calls[-1][0], "setup_intent_retrieve")
        self.assertEqual(second["customer_id"], first["customer_id"])
        self.assertNotEqual(second["client_secret"], first["client_secret"])

    def test_api_key_usage_accounting_is_atomic_complete_and_deferred_past_processor_io(self):
        api_key = "ghh_payment_setup_lock_probe"
        with self.api.get_db() as db:
            db.execute("INSERT INTO users (id,email,name,password_hash) VALUES (3,'api@example.com','API Worker','x')")
            db.execute(
                """INSERT INTO api_keys
                   (id,user_id,key_hash,key_prefix,name,scopes,is_active,total_requests)
                   VALUES (31,3,?,'ghh_paym','probe','["payments:setup"]',1,0)""",
                [hashlib.sha256(api_key.encode()).hexdigest()],
            )
            db.commit()
        status, payload = self.request("/payments/setup-worker", api_key=api_key)
        self.assertEqual(status, 200, payload)
        with self.api.get_db() as db:
            usage = db.execute(
                "SELECT total_requests,last_used_at FROM api_keys WHERE id=31"
            ).fetchone()
            rows = db.execute(
                """SELECT api_key_id,endpoint,method,status_code,response_time_ms
                   FROM api_key_usage WHERE api_key_id=31"""
            ).fetchall()
            self.assertEqual(usage["total_requests"], 1)
            self.assertIsNotNone(usage["last_used_at"])
            self.assertEqual(len(rows), 1)
            self.assertEqual(
                tuple(rows[0])[:4],
                (31, "/payments/setup-worker", "POST", 200),
            )
            self.assertGreaterEqual(rows[0]["response_time_ms"], 0)

            # Accounting failure must roll back both the evidence row and aggregate
            # update while preserving the already-produced route response.
            db.execute("INSERT INTO users (id,email,name,password_hash) VALUES (4,'api2@example.com','API Worker 2','x')")
            failed_key = "ghh_payment_setup_accounting_failure"
            db.execute(
                """INSERT INTO api_keys
                   (id,user_id,key_hash,key_prefix,name,scopes,is_active,total_requests)
                   VALUES (32,4,?,'ghh_fail','failure','["payments:setup"]',1,0)""",
                [hashlib.sha256(failed_key.encode()).hexdigest()],
            )
            db.execute(
                """CREATE TRIGGER fail_usage_insert BEFORE INSERT ON api_key_usage
                   BEGIN SELECT RAISE(ABORT,'forced usage failure'); END"""
            )
            db.commit()
        stderr = io.StringIO()
        with contextlib.redirect_stderr(stderr):
            failed_status, failed_payload = self.request(
                "/payments/setup-worker", api_key=failed_key
            )
        self.assertEqual(failed_status, 503, failed_payload)
        self.assertIn("accounting", failed_payload["error"].lower())
        with self.api.get_db() as db:
            self.assertEqual(
                db.execute("SELECT COUNT(*) FROM api_key_usage WHERE api_key_id=32").fetchone()[0],
                0,
            )
            self.assertEqual(
                db.execute("SELECT total_requests FROM api_keys WHERE id=32").fetchone()[0],
                0,
            )
            self.assertEqual(
                db.execute("SELECT COUNT(*) FROM api_key_usage WHERE api_key_id=31").fetchone()[0],
                1,
            )


if __name__ == "__main__":
    unittest.main()
