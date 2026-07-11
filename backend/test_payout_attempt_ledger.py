import contextlib
import io
import json
import os
import sqlite3
import tempfile
import threading
import unittest
from types import SimpleNamespace
from unittest import mock

import stripe as stripe_sdk

from test_deep_audit_regressions import load_api_core, parse_cgi_output


class PayoutAttemptLedgerTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self.tmp.name, "payout-ledger.db")
        os.environ["DATABASE_PATH"] = self.db_path
        os.environ["DISABLE_AUTO_SEED"] = "1"
        self.api = load_api_core()
        self.api._db_path_resolved = None
        self.api.init_db()
        self.api.PRODUCTION_MODE = True
        self.api.STRIPE_AVAILABLE = True
        self.api.STRIPE_SECRET_KEY = "configured-test-key"
        self.account_retrieve = mock.Mock(return_value={
            "payouts_enabled": True,
            "charges_enabled": True,
            "capabilities": {"transfers": "active"},
        })
        self.transfer_create = mock.Mock(
            side_effect=lambda **kwargs: SimpleNamespace(
                id=f"tr_{self.transfer_create.call_count}",
                amount=kwargs["amount"], currency=kwargs["currency"],
                destination=kwargs["destination"], metadata=kwargs["metadata"],
            )
        )
        self.api.stripe = SimpleNamespace(
            Account=SimpleNamespace(retrieve=self.account_retrieve),
            Transfer=SimpleNamespace(create=self.transfer_create),
            StripeError=stripe_sdk.StripeError,
            APIConnectionError=stripe_sdk.APIConnectionError,
            InvalidRequestError=stripe_sdk.InvalidRequestError,
        )
        self.api.STRIPE_ERROR = stripe_sdk.StripeError
        with self.api.get_db() as db:
            db.execute(
                "INSERT INTO users (id,email,name,password_hash) VALUES (1,'worker@example.com','Worker','x')"
            )
            db.execute(
                "INSERT INTO users (id,email,name,password_hash) VALUES (2,'employer@example.com','Employer','x')"
            )
            db.execute(
                "INSERT INTO worker_profiles (user_id,payout_account_id,payout_method) "
                "VALUES (1,'acct_live_worker','stripe_connect_active')"
            )
            db.execute("INSERT INTO employer_profiles (user_id) VALUES (2)")
            db.execute(
                "INSERT INTO sessions (user_id,token,expires_at) "
                "VALUES (1,'tok-worker',datetime('now','+1 day'))"
            )
            db.execute(
                "INSERT INTO sessions (user_id,token,expires_at) "
                "VALUES (2,'tok-employer',datetime('now','+1 day'))"
            )
            db.commit()

    def tearDown(self):
        self.tmp.cleanup()
        os.environ.pop("DATABASE_PATH", None)
        os.environ.pop("DISABLE_AUTO_SEED", None)

    def _seed_release(self, order_id, milestone_id, *, intent_id, evidence_source, linked=True):
        amount_cents = 1000
        with self.api.get_db() as db:
            db.execute(
                "INSERT INTO orders (id,type,worker_id,employer_id,status,total_amount) "
                "VALUES (?,'service_order',1,2,'submitted',10)",
                [order_id],
            )
            db.execute(
                "INSERT INTO milestones (id,order_id,title,amount,sequence,status) "
                "VALUES (?,?,'Delivery',10,1,'submitted')",
                [milestone_id, order_id],
            )
            attempt_id = None
            if linked:
                charge = self.api.buyer_charge_breakdown_cents(10)
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
                               'succeeded',?,datetime('now'),datetime('now'))""",
                    [
                        operation_key,
                        1,
                        fingerprint,
                        f"escrow-fund:{operation_key}:attempt:1",
                        2,
                        order_id,
                        milestone_id,
                        amount_cents,
                        charge["platform_fee_cents"],
                        charge["processing_fee_cents"],
                        charge["total_cents"],
                        intent_id,
                        evidence_source,
                    ],
                ).lastrowid
            db.execute(
                """INSERT INTO escrow_holds
                   (order_id,milestone_id,amount,base_amount_cents,
                    platform_fee_cents,processing_fee_cents,charged_total_cents,
                    fee_policy_version,funding_identity,funding_attempt_id,status,
                    stripe_payment_intent_id)
                   VALUES (?,?,10,?,?,?,?,?,?,?,'held',?)""",
                [
                    order_id,
                    milestone_id,
                    amount_cents,
                    10,
                    30,
                    1040,
                    "component-half-up-v1",
                    f"milestone:{milestone_id}" if linked else None,
                    attempt_id,
                    intent_id,
                ],
            )
            db.commit()

    def request(self, method, path, token, payload):
        for cached in ("body_cache", "raw_body"):
            if hasattr(self.api._request_ctx, cached):
                delattr(self.api._request_ctx, cached)
        body = json.dumps(payload)
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

    def test_live_release_rejects_simulator_pi_sim_and_unverified_legacy_provenance(self):
        vectors = [
            (101, 201, "pi_sim_normal", "simulator", True),
            (102, 202, "pi_sim_forged_live_source", "processor_create", True),
            (103, 203, "pi_live_unknown_source", "legacy_import", True),
            (104, 204, "pi_live_unlinked_legacy", None, False),
        ]
        for order_id, milestone_id, intent_id, source, linked in vectors:
            self._seed_release(
                order_id,
                milestone_id,
                intent_id=intent_id,
                evidence_source=source,
                linked=linked,
            )
            with self.subTest(intent_id=intent_id, source=source, linked=linked):
                with self.assertRaisesRegex(
                    self.api.FundingReconciliationRequired,
                    "live funding provenance",
                ):
                    with self.api.get_db() as db:
                        self.api.release_escrow_to_worker(
                            db, order_id, milestone_id, 10, 1
                        )

        self.account_retrieve.assert_not_called()
        self.transfer_create.assert_not_called()
        with self.api.get_db() as db:
            self.assertEqual(
                db.execute(
                    "SELECT COUNT(*) FROM escrow_holds WHERE status='held'"
                ).fetchone()[0],
                len(vectors),
            )

    def test_live_release_persists_exact_attempt_before_processor_without_writer_lock(self):
        self._seed_release(
            110,
            210,
            intent_id="pi_live_verified_funding",
            evidence_source="processor_create",
            linked=True,
        )
        callback_snapshots = []
        release_db = None

        def assert_prepared_attempt(boundary):
            self.assertIsNotNone(release_db)
            self.assertFalse(release_db.in_transaction, boundary)
            with self.api.get_db() as inspect_db:
                attempt = inspect_db.execute(
                    "SELECT * FROM payout_release_attempts WHERE order_id=110 AND milestone_id=210"
                ).fetchone()
                self.assertIsNotNone(attempt, boundary)
                self.assertEqual(attempt["status"], "prepared", boundary)
                self.assertEqual(attempt["attempt_number"], 1, boundary)
                self.assertEqual(attempt["amount_cents"], 1000, boundary)
                self.assertEqual(attempt["currency"], "usd", boundary)
                self.assertEqual(attempt["destination_account_id"], "acct_live_worker", boundary)
                self.assertEqual(attempt["order_id"], 110, boundary)
                self.assertEqual(attempt["milestone_id"], 210, boundary)
                self.assertEqual(attempt["worker_id"], 1, boundary)
                self.assertEqual(attempt["employer_id"], 2, boundary)
                self.assertIsNotNone(attempt["hold_id"], boundary)
                self.assertIsNotNone(attempt["funding_attempt_id"], boundary)
                self.assertRegex(
                    attempt["processor_idempotency_key"],
                    r"^escrow-release:hold:\d+:attempt:1$",
                )
                self.assertEqual(len(attempt["request_fingerprint"]), 64)
                callback_snapshots.append((boundary, attempt["id"]))
            with self.api.get_db() as contender:
                contender.execute("BEGIN IMMEDIATE")
                contender.rollback()

        def retrieve(account_id):
            self.assertEqual(account_id, "acct_live_worker")
            assert_prepared_attempt("account_retrieve")
            return {
                "payouts_enabled": True,
                "charges_enabled": True,
                "capabilities": {"transfers": "active"},
            }

        def create_transfer(**kwargs):
            assert_prepared_attempt("transfer_create")
            return SimpleNamespace(
                id="tr_live_durable_attempt",
                amount=kwargs["amount"],
                currency=kwargs["currency"],
                destination=kwargs["destination"],
                metadata=kwargs["metadata"],
            )

        self.account_retrieve.side_effect = retrieve
        self.transfer_create.side_effect = create_transfer
        with self.api.get_db() as db:
            release_db = db
            payout, _ = self.api.release_escrow_to_worker(db, 110, 210, 10, 1)
        self.assertEqual(payout, 10)
        self.assertEqual([name for name, _ in callback_snapshots], ["account_retrieve", "transfer_create"])
        self.assertEqual(callback_snapshots[0][1], callback_snapshots[1][1])
        with self.api.get_db() as db:
            attempt = db.execute(
                "SELECT * FROM payout_release_attempts WHERE order_id=110 AND milestone_id=210"
            ).fetchone()
            hold = db.execute(
                "SELECT * FROM escrow_holds WHERE order_id=110 AND milestone_id=210"
            ).fetchone()
            self.assertEqual(attempt["status"], "committed")
            self.assertEqual(attempt["processor_transfer_id"], "tr_live_durable_attempt")
            self.assertEqual(attempt["evidence_source"], "processor_create")
            self.assertEqual(hold["status"], "released")
            self.assertEqual(hold["release_attempt_id"], attempt["id"])

    def test_ambiguous_and_unclassified_transfer_outcomes_are_durable_and_block_retry(self):
        self.assertIs(self.api.STRIPE_ERROR, stripe_sdk.StripeError)
        vectors = [
            (120, 220, stripe_sdk.APIConnectionError("response lost"), "processor_outcome_ambiguous"),
            (121, 221, RuntimeError("unclassified transport failure"), "processor_outcome_unclassified"),
        ]
        for order_id, milestone_id, failure, expected_code in vectors:
            self._seed_release(
                order_id,
                milestone_id,
                intent_id=f"pi_live_verified_{order_id}",
                evidence_source="processor_create",
                linked=True,
            )
            self.transfer_create.side_effect = failure
            with self.subTest(expected_code=expected_code):
                with self.assertRaisesRegex(
                    self.api.FundingReconciliationRequired,
                    "read-only reconciliation",
                ):
                    with self.api.get_db() as db:
                        self.api.release_escrow_to_worker(
                            db, order_id, milestone_id, 10, 1
                        )
                calls_after_failure = self.transfer_create.call_count
                self.transfer_create.side_effect = None
                self.transfer_create.return_value = SimpleNamespace(
                    id=f"tr_must_not_run_{order_id}"
                )
                with self.assertRaisesRegex(
                    self.api.FundingReconciliationRequired,
                    "reconciliation",
                ):
                    with self.api.get_db() as db:
                        self.api.release_escrow_to_worker(
                            db, order_id, milestone_id, 10, 1
                        )
                self.assertEqual(self.transfer_create.call_count, calls_after_failure)
                with self.api.get_db() as db:
                    attempt = db.execute(
                        "SELECT * FROM payout_release_attempts WHERE order_id=?",
                        [order_id],
                    ).fetchone()
                    hold = db.execute(
                        "SELECT * FROM escrow_holds WHERE order_id=?",
                        [order_id],
                    ).fetchone()
                    evidence = db.execute(
                        "SELECT * FROM payout_release_conflict_evidence WHERE attempt_id=?",
                        [attempt["id"]],
                    ).fetchall()
                    self.assertEqual(attempt["status"], "unknown")
                    self.assertEqual(attempt["lifecycle_status"], "manual_review")
                    self.assertEqual(attempt["manual_review_required"], 1)
                    self.assertEqual(attempt["error_code"], expected_code)
                    self.assertEqual(hold["status"], "held")
                    self.assertIsNone(hold["stripe_transfer_id"])
                    self.assertEqual(len(evidence), 1)
                    self.assertEqual(evidence[0]["conflict_type"], expected_code)

    def test_post_transfer_cas_preserves_racing_dispute_and_amount_change(self):
        self._seed_release(
            130,
            230,
            intent_id="pi_live_verified_130",
            evidence_source="processor_create",
            linked=True,
        )

        def transfer_with_race(**kwargs):
            contender = sqlite3.connect(self.api._get_db_path())
            try:
                contender.execute("PRAGMA foreign_keys=ON")
                contender.execute("BEGIN IMMEDIATE")
                contender.execute("UPDATE orders SET status='disputed' WHERE id=130")
                contender.execute(
                    "UPDATE milestones SET status='in_progress',amount=11 WHERE id=230"
                )
                contender.commit()
            finally:
                contender.close()
            return SimpleNamespace(
                id="tr_racing_dispute_130", amount=kwargs["amount"],
                currency=kwargs["currency"], destination=kwargs["destination"],
                metadata=kwargs["metadata"],
            )

        self.transfer_create.side_effect = transfer_with_race
        with self.assertRaises(self.api.FundingReconciliationRequired):
            with self.api.get_db() as db:
                self.api.release_escrow_to_worker(db, 130, 230, 10, 1)

        with self.api.get_db() as db:
            order = db.execute("SELECT * FROM orders WHERE id=130").fetchone()
            milestone = db.execute("SELECT * FROM milestones WHERE id=230").fetchone()
            hold = db.execute("SELECT * FROM escrow_holds WHERE order_id=130").fetchone()
            attempt = db.execute(
                "SELECT * FROM payout_release_attempts WHERE order_id=130"
            ).fetchone()
            evidence = db.execute(
                "SELECT * FROM payout_release_conflict_evidence WHERE attempt_id=?",
                [attempt["id"]],
            ).fetchall()
            self.assertEqual(order["status"], "disputed")
            self.assertEqual(milestone["status"], "in_progress")
            self.assertEqual(self.api.money_to_cents(milestone["amount"]), 1100)
            self.assertEqual(hold["status"], "held")
            self.assertIsNone(hold["stripe_transfer_id"])
            self.assertEqual(attempt["status"], "processor_succeeded")
            self.assertEqual(attempt["processor_transfer_id"], "tr_racing_dispute_130")
            self.assertEqual(attempt["manual_review_required"], 1)
            self.assertEqual(attempt["lifecycle_status"], "manual_review")
            self.assertEqual(len(evidence), 1)
            self.assertEqual(evidence[0]["conflict_type"], "payout_lifecycle_conflict")

    def test_pending_release_gate_blocks_revision_and_dispute_writers(self):
        for order_id, milestone_id in ((140, 240), (141, 241)):
            self._seed_release(
                order_id,
                milestone_id,
                intent_id=f"pi_live_verified_{order_id}",
                evidence_source="processor_create",
                linked=True,
            )
            with self.api.get_db() as db:
                hold_id = db.execute(
                    "SELECT id FROM escrow_holds WHERE order_id=?", [order_id]
                ).fetchone()["id"]
                attempt, mode = self.api._prepare_payout_release_attempt(
                    db, hold_id, 1000, 1
                )
                self.assertEqual(mode, "prepared")
                self.assertEqual(attempt["lifecycle_status"], "pending")

        revision_status, revision_payload = self.request(
            "POST",
            "/orders/140/request-revision",
            "tok-employer",
            {"notes": "Revise this"},
        )
        dispute_status, dispute_payload = self.request(
            "POST",
            "/orders/141/dispute",
            "tok-worker",
            {"reason": "Race payout"},
        )
        self.assertEqual(revision_status, 409, revision_payload)
        self.assertIn("payout release", revision_payload["error"].lower())
        self.assertEqual(dispute_status, 409, dispute_payload)
        self.assertIn("payout release", dispute_payload["error"].lower())

        with self.api.get_db() as db:
            orders = db.execute(
                "SELECT id,status FROM orders WHERE id IN (140,141) ORDER BY id"
            ).fetchall()
            milestones = db.execute(
                "SELECT id,status FROM milestones WHERE id IN (240,241) ORDER BY id"
            ).fetchall()
            attempts = db.execute(
                "SELECT status,lifecycle_status FROM payout_release_attempts ORDER BY order_id"
            ).fetchall()
            self.assertEqual([row["status"] for row in orders], ["submitted", "submitted"])
            self.assertEqual([row["status"] for row in milestones], ["submitted", "submitted"])
            self.assertEqual([row["status"] for row in attempts], ["prepared", "prepared"])
            self.assertEqual([row["lifecycle_status"] for row in attempts], ["pending", "pending"])

    def test_exact_replay_finishes_lifecycle_after_local_release_commit_without_second_transfer(self):
        self._seed_release(
            150,
            250,
            intent_id="pi_live_verified_150",
            evidence_source="processor_create",
            linked=True,
        )
        with self.api.get_db() as db:
            payout, _ = self.api.release_escrow_to_worker(db, 150, 250, 10, 1)
        self.assertEqual(payout, 10)
        self.assertEqual(self.transfer_create.call_count, 1)
        with self.api.get_db() as db:
            before = db.execute(
                "SELECT * FROM payout_release_attempts WHERE order_id=150"
            ).fetchone()
            self.assertEqual(before["status"], "committed")
            self.assertEqual(before["lifecycle_status"], "pending")
            self.assertEqual(
                db.execute("SELECT status FROM orders WHERE id=150").fetchone()[0],
                "submitted",
            )

        status, payload = self.request(
            "POST", "/orders/150/approve", "tok-employer", {}
        )
        self.assertEqual(status, 200, payload)
        self.assertEqual(self.transfer_create.call_count, 1)
        with self.api.get_db() as db:
            attempt = db.execute(
                "SELECT * FROM payout_release_attempts WHERE order_id=150"
            ).fetchone()
            hold = db.execute(
                "SELECT * FROM escrow_holds WHERE order_id=150"
            ).fetchone()
            order = db.execute("SELECT * FROM orders WHERE id=150").fetchone()
            milestone = db.execute("SELECT * FROM milestones WHERE id=250").fetchone()
            self.assertEqual(attempt["status"], "committed")
            self.assertEqual(attempt["lifecycle_status"], "completed")
            self.assertIsNotNone(attempt["lifecycle_completed_at"])
            self.assertEqual(hold["status"], "released")
            self.assertEqual(hold["release_attempt_id"], attempt["id"])
            self.assertEqual(milestone["status"], "approved")
            self.assertEqual(order["status"], "completed")

    def test_legacy_database_without_payout_tables_is_migrated_exactly(self):
        with self.api.get_db() as db:
            for name in self.api._PAYOUT_CONFLICT_EVIDENCE_TRIGGER_SQL:
                db.execute(f"DROP TRIGGER {name}")
            for name in self.api._PAYOUT_INDEX_SQL:
                db.execute(f"DROP INDEX {name}")
            db.execute("DROP TABLE payout_release_conflict_evidence")
            db.execute("DROP TABLE payout_release_attempts")
            db.commit()
        self.api.init_db()
        with self.api.get_db() as db:
            self.api.validate_required_payout_schema(db)

    def test_concurrent_initializers_install_one_exact_payout_schema(self):
        errors = []
        barrier = threading.Barrier(4)

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
        self.assertFalse(errors, errors)
        self.assertTrue(all(not thread.is_alive() for thread in threads))
        with self.api.get_db() as db:
            self.api.validate_required_payout_schema(db)

    def test_poisoned_payout_index_and_trigger_fail_closed(self):
        with self.api.get_db() as db:
            db.execute("DROP INDEX idx_payout_release_processor_key")
            db.execute("CREATE INDEX idx_payout_release_processor_key ON payout_release_attempts(status)")
            db.commit()
        with self.assertRaisesRegex(RuntimeError, "exact index schema"):
            self.api.init_db()
        with self.api.get_db() as db:
            db.execute("DROP INDEX idx_payout_release_processor_key")
            db.execute(self.api._PAYOUT_INDEX_SQL["idx_payout_release_processor_key"])
            db.execute("DROP TRIGGER trg_payout_conflict_evidence_no_update")
            db.execute(
                """CREATE TRIGGER trg_payout_conflict_evidence_no_update
                   BEFORE UPDATE ON payout_release_conflict_evidence BEGIN SELECT 1; END"""
            )
            db.commit()
        with self.assertRaisesRegex(RuntimeError, "exact trigger schema"):
            self.api.init_db()

    def test_payout_conflict_evidence_is_append_only_and_replace_safe(self):
        self._seed_release(
            160, 260, intent_id="pi_live_verified_160",
            evidence_source="processor_create", linked=True,
        )
        with self.api.get_db() as db:
            hold_id = db.execute("SELECT id FROM escrow_holds WHERE order_id=160").fetchone()[0]
            attempt, _ = self.api._prepare_payout_release_attempt(db, hold_id, 1000, 1)
            db.execute("BEGIN IMMEDIATE")
            key = self.api._insert_payout_release_conflict_evidence(
                db, attempt, "test_evidence", {"state": "observed"}, "test",
            )
            db.commit()
        with self.api.get_db() as db:
            with self.assertRaisesRegex(sqlite3.IntegrityError, "append-only"):
                db.execute(
                    "UPDATE payout_release_conflict_evidence SET conflict_type='changed' WHERE evidence_key=?",
                    [key],
                )
            db.rollback()
            with self.assertRaisesRegex(sqlite3.IntegrityError, "append-only"):
                db.execute("DELETE FROM payout_release_conflict_evidence WHERE evidence_key=?", [key])
            db.rollback()
            row = db.execute(
                "SELECT * FROM payout_release_conflict_evidence WHERE evidence_key=?", [key]
            ).fetchone()
            columns = [item[1] for item in db.execute("PRAGMA table_info(payout_release_conflict_evidence)")]
            with self.assertRaisesRegex(sqlite3.IntegrityError, "cannot be replaced"):
                db.execute(
                    f"INSERT OR REPLACE INTO payout_release_conflict_evidence ({','.join(columns)}) VALUES ({','.join('?' for _ in columns)})",
                    [row[column] for column in columns],
                )

    def test_racing_payout_preparers_create_exactly_one_active_attempt(self):
        self._seed_release(
            170, 270, intent_id="pi_live_verified_170",
            evidence_source="processor_create", linked=True,
        )
        with self.api.get_db() as db:
            hold_id = db.execute("SELECT id FROM escrow_holds WHERE order_id=170").fetchone()[0]
        outcomes = []
        barrier = threading.Barrier(2)

        def prepare():
            with self.api.get_db() as db:
                barrier.wait()
                try:
                    attempt, mode = self.api._prepare_payout_release_attempt(db, hold_id, 1000, 1)
                    outcomes.append((attempt["id"], mode))
                except self.api.FundingReconciliationRequired:
                    outcomes.append((None, "blocked"))

        threads = [threading.Thread(target=prepare) for _ in range(2)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(10)
        with self.api.get_db() as db:
            self.assertEqual(
                db.execute("SELECT COUNT(*) FROM payout_release_attempts WHERE hold_id=?", [hold_id]).fetchone()[0],
                1,
            )
        self.assertEqual(len(outcomes), 2)
