import contextlib
import io
import json
import os
import pathlib
import sqlite3
import stat
import subprocess
import sys
import tempfile
import unittest
from types import SimpleNamespace
from unittest import mock

import stripe as stripe_sdk

from test_deep_audit_regressions import load_api_core, parse_cgi_output


StripeError = stripe_sdk.StripeError
APIConnectionError = stripe_sdk.APIConnectionError
CardError = stripe_sdk.CardError


class FundingAttemptLedgerTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self.tmp.name, "ledger.db")
        os.environ["DATABASE_PATH"] = self.db_path
        os.environ["DISABLE_AUTO_SEED"] = "1"
        self.api = load_api_core()
        self.api._db_path_resolved = None
        self.api.init_db()
        self.api.PRODUCTION_MODE = True
        self.api.STRIPE_AVAILABLE = True
        self.api.STRIPE_SECRET_KEY = "configured-test-key"
        self.create = mock.Mock()
        self.retrieve = mock.Mock()
        self.search = mock.Mock()
        self.api.stripe = SimpleNamespace(
            PaymentIntent=SimpleNamespace(
                create=self.create,
                retrieve=self.retrieve,
                search=self.search,
            ),
            StripeError=StripeError,
            APIConnectionError=APIConnectionError,
            CardError=CardError,
        )
        with self.api.get_db() as db:
            db.execute("INSERT INTO users (id,email,name,password_hash) VALUES (1,'worker@example.com','Worker','x')")
            db.execute("INSERT INTO users (id,email,name,password_hash) VALUES (2,'employer@example.com','Employer','x')")
            db.execute("INSERT INTO worker_profiles (user_id) VALUES (1)")
            db.execute("INSERT INTO employer_profiles (user_id,stripe_customer_id,payment_method_id) VALUES (2,'cus_test','pm_test')")
            db.execute("INSERT INTO services (id,worker_id,title,description,category,pricing_type,status) VALUES (1,1,'QA','QA work','testing','custom','active')")
            db.execute("INSERT INTO sessions (user_id,token,expires_at) VALUES (2,'tok-employer',datetime('now','+1 day'))")
            db.execute("INSERT INTO orders (id,type,worker_id,employer_id,status,total_amount) VALUES (10,'service_order',1,2,'pending',25)")
            db.execute("INSERT INTO milestones (id,order_id,title,amount,sequence,status) VALUES (20,10,'Delivery',25,1,'pending')")
            db.commit()

    def tearDown(self):
        self.tmp.cleanup()
        os.environ.pop("DATABASE_PATH", None)
        os.environ.pop("DISABLE_AUTO_SEED", None)

    def _metadata(self, fingerprint, attempt_id="1", attempt_number="1"):
        return {
            "funding_identity": "milestone:20",
            "funding_request_fingerprint": fingerprint,
            "funding_attempt_id": attempt_id,
            "funding_attempt_number": attempt_number,
            "order_id": "10",
            "milestone_id": "20",
            "employer_id": "2",
        }

    def _succeeded_intent(self, fingerprint, pi_id="pi_success", attempt_id="1", attempt_number="1"):
        return {
            "id": pi_id,
            "status": "succeeded",
            "amount": 2600,
            "amount_received": 2600,
            "currency": "usd",
            "metadata": self._metadata(fingerprint, attempt_id, attempt_number),
        }

    def _fingerprint(self):
        return self.api.funding_request_fingerprint(
            operation_key="milestone:20",
            employer_id=2,
            order_id=10,
            milestone_id=20,
            charge=self.api.buyer_charge_breakdown_cents(25),
        )

    def _insert_unknown_attempt(self, processor_intent_id=None):
        fingerprint = self._fingerprint()
        with self.api.get_db() as db:
            attempt_id = db.execute(
                """INSERT INTO funding_attempts
                   (operation_key,attempt_number,request_fingerprint,processor_idempotency_key,
                    employer_id,order_id,milestone_id,base_amount_cents,platform_fee_cents,
                    processing_fee_cents,charged_total_cents,status,stripe_payment_intent_id)
                   VALUES ('milestone:20',1,?,'escrow-fund:milestone:20:attempt:1',
                           2,10,20,2500,25,75,2600,'unknown',?)""",
                [fingerprint, processor_intent_id],
            ).lastrowid
            db.commit()
            return db.execute(
                "SELECT * FROM funding_attempts WHERE id=?", [attempt_id]
            ).fetchone()

    def _drift_attempt_bindings(self, attempt_id):
        with self.api.get_db() as writer:
            writer.execute(
                "INSERT OR IGNORE INTO orders "
                "(id,type,worker_id,employer_id,status,total_amount) "
                "VALUES (999,'service_order',1,2,'pending',25)"
            )
            writer.execute(
                "INSERT OR IGNORE INTO milestones "
                "(id,order_id,title,amount,sequence,status) "
                "VALUES (999,999,'Drifted',25,1,'pending')"
            )
            writer.execute(
                """UPDATE funding_attempts
                   SET operation_key='milestone:999',
                       processor_idempotency_key='escrow-fund:milestone:999:attempt:1',
                       employer_id=1,order_id=999,milestone_id=999,
                       request_fingerprint=?,platform_fee_cents=26,
                       charged_total_cents=2601
                   WHERE id=?""",
                ["f" * 64, attempt_id],
            )
            writer.commit()

    def _assert_manual_binding_freeze(self, attempt_id, expected_status="unknown"):
        with self.api.get_db() as db:
            attempt = db.execute(
                "SELECT * FROM funding_attempts WHERE id=?", [attempt_id]
            ).fetchone()
            hold_count = db.execute("SELECT COUNT(*) FROM escrow_holds").fetchone()[0]
        self.assertEqual(attempt["operation_key"], "milestone:20")
        self.assertEqual(attempt["order_id"], 10)
        self.assertEqual(attempt["milestone_id"], 20)
        self.assertEqual(attempt["request_fingerprint"], self._fingerprint())
        self.assertEqual(attempt["status"], expected_status)
        self.assertIn(
            attempt["error_code"],
            {"attempt_binding_conflict", "processor_intent_conflict"},
        )
        self.assertIn("binding", (attempt["error_message"] or "").lower())
        self.assertEqual(hold_count, 0)
        with self.api.get_db() as writer:
            writer.execute("UPDATE orders SET updated_at=datetime('now') WHERE id=10")
            writer.commit()

    def _assert_original_retry_is_blocked(self):
        with self.api.get_db() as db:
            with self.assertRaises(self.api.FundingReconciliationRequired):
                self.api.fund_escrow_stripe(
                    db, 2, 25, 10, 20, funding_identity="milestone:20"
                )

    def _request(self, payload):
        ctx = self.api._request_ctx
        for cached in ("body_cache", "raw_body"):
            if hasattr(ctx, cached):
                delattr(ctx, cached)
        body = json.dumps(payload)
        ctx.request_method = "POST"
        ctx.path_info = "/services/1/order"
        ctx.query_string = ""
        ctx.http_authorization = "Bearer tok-employer"
        ctx.http_x_api_key = ""
        ctx.stdin_data = body
        ctx.content_type = "application/json"
        ctx.content_length = str(len(body.encode()))
        ctx.remote_addr = "127.0.0.1"
        with contextlib.redirect_stdout(io.StringIO()) as out:
            self.api.handle_request()
        return parse_cgi_output(out.getvalue())

    def test_schema_installs_durable_attempt_ledger_and_unique_active_guard(self):
        with self.api.get_db() as db:
            columns = {row[1] for row in db.execute("PRAGMA table_info('funding_attempts')")}
            self.assertTrue({
                "operation_key", "attempt_number", "request_fingerprint",
                "processor_idempotency_key", "stripe_payment_intent_id", "status",
                "base_amount_cents", "charged_total_cents", "evidence_source",
                "processor_evidence_at",
            }.issubset(columns))
            indexes = {row[1]: row for row in db.execute("PRAGMA index_list('funding_attempts')")}
            self.assertEqual(indexes["idx_funding_attempts_operation_attempt"][2], 1)
            self.assertEqual(indexes["idx_funding_attempts_active_operation"][2], 1)
            self.assertEqual(indexes["idx_funding_attempts_active_operation"][4], 1)
            self.assertEqual(indexes["idx_funding_attempts_active_milestone"][2], 1)
            self.assertEqual(indexes["idx_funding_attempts_active_milestone"][4], 1)
            self.assertEqual(indexes["idx_funding_attempts_processor_key"][2], 1)
            hold_indexes = {row[1]: row for row in db.execute("PRAGMA index_list('escrow_holds')")}
            self.assertEqual(hold_indexes["idx_escrow_holds_funding_attempt"][2], 1)
            self.assertEqual(hold_indexes["idx_escrow_holds_funding_attempt"][4], 1)
            order_columns = {row[1] for row in db.execute("PRAGMA table_info('orders')")}
            self.assertIn("creation_request_fingerprint", order_columns)

            evidence_columns = {
                row[1]
                for row in db.execute(
                    "PRAGMA table_info('funding_attempt_conflict_evidence')"
                )
            }
            self.assertTrue({
                "evidence_key", "attempt_id", "conflict_type",
                "expected_operation_key", "expected_order_id",
                "expected_milestone_id", "observed_operation_key",
                "observed_order_id", "observed_milestone_id",
                "canonical_intent_id", "incoming_intent_id",
                "incoming_processor_status", "incoming_evidence_source",
                "intent_owner_attempt_id", "expected_snapshot_json",
                "expected_snapshot_sha256", "observed_snapshot_json",
                "observed_snapshot_sha256", "normalized_evidence_json",
                "created_at",
            }.issubset(evidence_columns))
            evidence_indexes = {
                row[1]: row
                for row in db.execute(
                    "PRAGMA index_list('funding_attempt_conflict_evidence')"
                )
            }
            self.assertEqual(
                evidence_indexes["idx_funding_conflict_evidence_key"][2], 1
            )
            triggers = {
                row[0]
                for row in db.execute(
                    "SELECT name FROM sqlite_master WHERE type='trigger'"
                )
            }
            self.assertIn("trg_funding_conflict_evidence_no_update", triggers)
            self.assertIn("trg_funding_conflict_evidence_no_delete", triggers)
            self.assertIn("trg_funding_conflict_evidence_no_replace", triggers)

    def test_conflict_evidence_rows_are_append_only_and_replace_safe(self):
        seeded_attempt = self._insert_unknown_attempt()
        attempt_id = seeded_attempt["id"]
        with self.api.get_db() as db:
            attempt = db.execute(
                "SELECT * FROM funding_attempts WHERE id=?", [attempt_id]
            ).fetchone()
            evidence_id = self.api._insert_funding_conflict_evidence(
                db,
                attempt_id=attempt_id,
                conflict_type="processor_intent_conflict",
                expected=attempt,
                observed=attempt,
                canonical_intent_id=None,
                incoming_intent_id="pi_append_only",
                incoming_processor_status="succeeded",
                incoming_evidence_source="signed_webhook",
            )
            db.commit()

            statements = [
                (
                    "UPDATE funding_attempt_conflict_evidence SET conflict_type='tampered' WHERE id=?",
                    [evidence_id],
                ),
                (
                    "DELETE FROM funding_attempt_conflict_evidence WHERE id=?",
                    [evidence_id],
                ),
                (
                    """INSERT OR REPLACE INTO funding_attempt_conflict_evidence
                       SELECT * FROM funding_attempt_conflict_evidence WHERE id=?""",
                    [evidence_id],
                ),
            ]
            for sql, parameters in statements:
                with self.assertRaises(sqlite3.IntegrityError):
                    db.execute(sql, parameters)
                db.rollback()

            remaining = db.execute(
                "SELECT incoming_intent_id FROM funding_attempt_conflict_evidence WHERE id=?",
                [evidence_id],
            ).fetchone()
        self.assertEqual(remaining["incoming_intent_id"], "pi_append_only")

    def test_conflict_evidence_key_collision_with_different_payload_fails_closed(self):
        seeded_attempt = self._insert_unknown_attempt()
        attempt_id = seeded_attempt["id"]

        class FixedDigest:
            def hexdigest(self):
                return "f" * 64

        with mock.patch.object(
            self.api.hashlib, "sha256", side_effect=lambda _value: FixedDigest()
        ):
            with self.api.get_db() as db:
                attempt = db.execute(
                    "SELECT * FROM funding_attempts WHERE id=?", [attempt_id]
                ).fetchone()
                evidence_id = self.api._insert_funding_conflict_evidence(
                    db,
                    attempt_id=attempt_id,
                    conflict_type="processor_intent_conflict",
                    expected=attempt,
                    observed=attempt,
                    incoming_intent_id="pi_collision_A",
                    incoming_processor_status="succeeded",
                    incoming_evidence_source="signed_webhook",
                    processor_event_id="evt_collision_A",
                )
                db.commit()

            with self.api.get_db() as db:
                attempt = db.execute(
                    "SELECT * FROM funding_attempts WHERE id=?", [attempt_id]
                ).fetchone()
                with self.assertRaisesRegex(
                    self.api.FundingReconciliationRequired,
                    "evidence key collision",
                ):
                    self.api._insert_funding_conflict_evidence(
                        db,
                        attempt_id=attempt_id,
                        conflict_type="processor_intent_conflict",
                        expected=attempt,
                        observed=attempt,
                        incoming_intent_id="pi_collision_B",
                        incoming_processor_status="succeeded",
                        incoming_evidence_source="signed_webhook",
                        processor_event_id="evt_collision_B",
                    )
                db.rollback()
                remaining = db.execute(
                    """SELECT id,incoming_intent_id,processor_event_id
                       FROM funding_attempt_conflict_evidence WHERE attempt_id=?""",
                    [attempt_id],
                ).fetchall()

        self.assertEqual(
            [tuple(row) for row in remaining],
            [(evidence_id, "pi_collision_A", "evt_collision_A")],
        )

    def test_one_funding_attempt_cannot_link_multiple_escrow_holds(self):
        fingerprint = self._fingerprint()
        with self.api.get_db() as db:
            cursor = db.execute(
                """INSERT INTO funding_attempts
                   (operation_key,attempt_number,request_fingerprint,processor_idempotency_key,
                    employer_id,order_id,milestone_id,base_amount_cents,platform_fee_cents,
                    processing_fee_cents,charged_total_cents,status)
                   VALUES ('milestone:20',1,?,'escrow-fund:milestone:20:attempt:1',
                           2,10,20,2500,25,75,2600,'committed')""",
                [fingerprint],
            )
            attempt_id = cursor.lastrowid
            db.execute(
                """INSERT INTO escrow_holds
                   (order_id,milestone_id,amount,funding_identity,funding_attempt_id,status,
                    stripe_payment_intent_id)
                   VALUES (10,20,25,'milestone:20',?,'held','pi_one')""",
                [attempt_id],
            )
            with self.assertRaises(sqlite3.IntegrityError):
                db.execute(
                    """INSERT INTO escrow_holds
                       (order_id,milestone_id,amount,funding_identity,funding_attempt_id,status,
                        stripe_payment_intent_id)
                       VALUES (10,20,25,'milestone:20:duplicate',?,'held','pi_one')""",
                    [attempt_id],
                )

    def test_one_milestone_cannot_have_active_attempts_under_different_operation_keys(self):
        fingerprint = self._fingerprint()
        with self.api.get_db() as db:
            db.execute(
                """INSERT INTO funding_attempts
                   (operation_key,attempt_number,request_fingerprint,processor_idempotency_key,
                    employer_id,order_id,milestone_id,base_amount_cents,platform_fee_cents,
                    processing_fee_cents,charged_total_cents,status)
                   VALUES ('milestone:20',1,?,'canonical-attempt',2,10,20,2500,25,75,2600,'unknown')""",
                [fingerprint],
            )
            with self.assertRaises(sqlite3.IntegrityError):
                db.execute(
                    """INSERT INTO funding_attempts
                       (operation_key,attempt_number,request_fingerprint,processor_idempotency_key,
                        employer_id,order_id,milestone_id,base_amount_cents,platform_fee_cents,
                        processing_fee_cents,charged_total_cents,status)
                       VALUES ('different-route-key',1,?,'different-route-attempt',
                               2,10,20,2500,25,75,2600,'prepared')""",
                    [fingerprint],
                )

    def test_malformed_same_name_funding_index_fails_closed(self):
        with self.api.get_db() as db:
            db.execute("DROP INDEX idx_funding_attempts_processor_key")
            db.execute(
                "CREATE INDEX idx_funding_attempts_processor_key "
                "ON funding_attempts(processor_idempotency_key)"
            )
            with self.assertRaises(RuntimeError):
                self.api.validate_required_transaction_schema(db)

    def test_malformed_same_name_funding_table_semantics_fail_closed(self):
        with self.api.get_db() as db:
            object_sql = [
                row[0]
                for row in db.execute(
                    """SELECT sql FROM sqlite_master
                       WHERE sql IS NOT NULL
                         AND ((type='index' AND tbl_name IN (
                                'funding_attempts','funding_attempt_conflict_evidence'
                              ))
                           OR (type='trigger' AND tbl_name='funding_attempt_conflict_evidence'))
                       ORDER BY type,name"""
                )
            ]
            attempt_columns = [
                row[1] for row in db.execute("PRAGMA table_info('funding_attempts')")
            ]
            evidence_columns = [
                row[1]
                for row in db.execute(
                    "PRAGMA table_info('funding_attempt_conflict_evidence')"
                )
            ]
            db.execute("PRAGMA foreign_keys=OFF")
            db.execute("DROP TABLE funding_attempt_conflict_evidence")
            db.execute("DROP TABLE funding_attempts")
            db.execute(
                "CREATE TABLE funding_attempts ("
                + ",".join(f'"{column}" TEXT' for column in attempt_columns)
                + ")"
            )
            db.execute(
                "CREATE TABLE funding_attempt_conflict_evidence ("
                + ",".join(f'"{column}" TEXT' for column in evidence_columns)
                + ")"
            )
            for sql in object_sql:
                db.execute(sql)

            with self.assertRaisesRegex(RuntimeError, "schema"):
                self.api.validate_required_transaction_schema(db)

    def test_malformed_same_name_conflict_index_fails_closed(self):
        with self.api.get_db() as db:
            db.execute("DROP INDEX idx_funding_conflict_evidence_key")
            db.execute(
                "CREATE INDEX idx_funding_conflict_evidence_key "
                "ON funding_attempt_conflict_evidence(evidence_key)"
            )
            with self.assertRaises(RuntimeError):
                self.api.validate_required_transaction_schema(db)

    def test_malformed_same_name_conflict_trigger_fails_closed(self):
        with self.api.get_db() as db:
            db.execute("DROP TRIGGER trg_funding_conflict_evidence_no_update")
            db.execute(
                """CREATE TRIGGER trg_funding_conflict_evidence_no_update
                   BEFORE UPDATE ON funding_attempt_conflict_evidence
                   BEGIN SELECT 1; END"""
            )
            with self.assertRaises(RuntimeError):
                self.api.validate_required_transaction_schema(db)

    def test_request_fingerprint_is_canonical_and_target_bound(self):
        charge = self.api.buyer_charge_breakdown_cents(25)
        first = self.api.funding_request_fingerprint("milestone:20", 2, 10, 20, charge)
        second = self.api.funding_request_fingerprint(
            operation_key="milestone:20", employer_id=2, order_id=10,
            milestone_id=20, charge=dict(reversed(list(charge.items()))),
        )
        changed = self.api.funding_request_fingerprint("milestone:20", 2, 10, 21, charge)
        self.assertEqual(first, second)
        self.assertRegex(first, r"^[0-9a-f]{64}$")
        self.assertNotEqual(first, changed)

    def test_milestone_funding_rejects_noncanonical_cross_route_identity_before_processor(self):
        with self.api.get_db() as db:
            with self.assertRaises(self.api.FundingConflict):
                self.api.fund_escrow_stripe(
                    db,
                    2,
                    25,
                    10,
                    20,
                    funding_identity="service-order:2:alternate-route",
                )
            attempt_count = db.execute("SELECT COUNT(*) FROM funding_attempts").fetchone()[0]
        self.assertEqual(attempt_count, 0)
        self.create.assert_not_called()

    def test_prepared_attempt_is_committed_before_processor_call(self):
        def create(**kwargs):
            with self.api.get_db() as observer:
                row = observer.execute("SELECT * FROM funding_attempts").fetchone()
                self.assertIsNotNone(row)
                self.assertEqual(row["status"], "prepared")
                self.assertEqual(row["processor_idempotency_key"], kwargs["idempotency_key"])
                return self._succeeded_intent(
                    row["request_fingerprint"],
                    attempt_id=str(row["id"]),
                    attempt_number=str(row["attempt_number"]),
                )

        self.create.side_effect = create
        with self.api.get_db() as db:
            pi_id, mode = self.api.fund_escrow_stripe(
                db, 2, 25, 10, 20, funding_identity="milestone:20"
            )
        self.assertEqual((pi_id, mode), ("pi_success", "live"))
        with self.api.get_db() as db:
            attempt = db.execute("SELECT * FROM funding_attempts").fetchone()
            hold = db.execute("SELECT * FROM escrow_holds").fetchone()
        self.assertEqual(attempt["status"], "committed")
        self.assertEqual(attempt["stripe_payment_intent_id"], "pi_success")
        self.assertEqual(hold["stripe_payment_intent_id"], "pi_success")
        self.assertEqual(hold["funding_attempt_id"], attempt["id"])

    def test_processor_success_rejects_concurrent_immutable_attempt_binding_drift(self):
        def create_then_drift(**kwargs):
            with self.api.get_db() as writer:
                writer.execute(
                    "INSERT INTO orders (id,type,worker_id,employer_id,status,total_amount) "
                    "VALUES (999,'service_order',1,2,'pending',25)"
                )
                writer.execute(
                    "INSERT INTO milestones (id,order_id,title,amount,sequence,status) "
                    "VALUES (999,999,'Drifted',25,1,'pending')"
                )
                writer.execute(
                    """UPDATE funding_attempts
                       SET operation_key='milestone:999', employer_id=1,
                           order_id=999, milestone_id=999,
                           request_fingerprint=?, platform_fee_cents=26,
                           charged_total_cents=2601
                       WHERE status='prepared'""",
                    ["f" * 64],
                )
                writer.commit()
            return {
                "id": "pi_success",
                "status": "succeeded",
                "amount": kwargs["amount"],
                "amount_received": kwargs["amount"],
                "currency": kwargs["currency"],
                "metadata": kwargs["metadata"],
            }

        self.create.side_effect = create_then_drift
        with self.api.get_db() as db:
            with self.assertRaises(self.api.FundingConflict):
                self.api.fund_escrow_stripe(
                    db, 2, 25, 10, 20, funding_identity="milestone:20"
                )

        with self.api.get_db() as db:
            attempt = db.execute("SELECT * FROM funding_attempts").fetchone()
            hold_count = db.execute("SELECT COUNT(*) FROM escrow_holds").fetchone()[0]
        self.assertEqual(attempt["status"], "prepared")
        self.assertEqual(attempt["error_code"], "attempt_binding_conflict")
        self.assertEqual(attempt["stripe_payment_intent_id"], "pi_success")
        self.assertEqual(hold_count, 0)
        self.create.assert_called_once()

    def test_processor_exception_rejects_concurrent_immutable_attempt_binding_drift(self):
        def drift_then_raise(**kwargs):
            with self.api.get_db() as writer:
                writer.execute(
                    "UPDATE funding_attempts "
                    "SET operation_key='milestone:999', request_fingerprint=? "
                    "WHERE status='prepared'",
                    ["f" * 64],
                )
                writer.commit()
            raise APIConnectionError("connection reset after send")

        self.create.side_effect = drift_then_raise
        with self.api.get_db() as db:
            with self.assertRaises(self.api.FundingConflict):
                self.api.fund_escrow_stripe(
                    db, 2, 25, 10, 20, funding_identity="milestone:20"
                )

        with self.api.get_db() as db:
            attempt = db.execute("SELECT * FROM funding_attempts").fetchone()
            hold_count = db.execute("SELECT COUNT(*) FROM escrow_holds").fetchone()[0]
        self.assertEqual(attempt["status"], "prepared")
        self.assertEqual(attempt["error_code"], "attempt_binding_conflict")
        self.assertIsNone(attempt["stripe_payment_intent_id"])
        self.assertEqual(hold_count, 0)
        self.create.assert_called_once()

    def test_existing_hold_replay_rejects_fee_policy_drift(self):
        def create(**kwargs):
            with self.api.get_db() as observer:
                row = observer.execute("SELECT * FROM funding_attempts").fetchone()
            return self._succeeded_intent(
                row["request_fingerprint"],
                attempt_id=str(row["id"]),
                attempt_number=str(row["attempt_number"]),
            )

        self.create.side_effect = create
        with self.api.get_db() as db:
            self.api.fund_escrow_stripe(
                db, 2, 25, 10, 20, funding_identity="milestone:20"
            )
            db.execute(
                "UPDATE escrow_holds SET fee_policy_version='unexpected-policy' "
                "WHERE funding_identity='milestone:20'"
            )
            db.commit()
            with self.assertRaises(self.api.FundingConflict):
                self.api.fund_escrow_stripe(
                    db, 2, 25, 10, 20, funding_identity="milestone:20"
                )
        self.create.assert_called_once()

    def test_ambiguous_processor_failure_stays_reconcilable_and_never_creates_hold(self):
        self.create.side_effect = APIConnectionError("connection reset after send")
        with self.api.get_db() as db:
            with self.assertRaises(self.api.FundingReconciliationRequired):
                self.api.fund_escrow_stripe(
                    db, 2, 25, 10, 20, funding_identity="milestone:20"
                )
        with self.api.get_db() as db:
            attempt = db.execute("SELECT * FROM funding_attempts").fetchone()
            hold_count = db.execute("SELECT COUNT(*) FROM escrow_holds").fetchone()[0]
        self.assertEqual(attempt["status"], "unknown")
        self.assertEqual(hold_count, 0)
        self.assertNotIn("connection reset", attempt["error_message"] or "")

    def test_unclassified_processor_error_fails_closed_as_ambiguous(self):
        self.create.side_effect = StripeError("unclassified processor failure")
        with self.api.get_db() as db:
            with self.assertRaises(self.api.FundingReconciliationRequired):
                self.api.fund_escrow_stripe(
                    db, 2, 25, 10, 20, funding_identity="milestone:20"
                )
        with self.api.get_db() as db:
            attempt = db.execute("SELECT * FROM funding_attempts").fetchone()
        self.assertEqual(attempt["status"], "unknown")
        self.assertEqual(attempt["error_code"], "StripeError")

    def test_pinned_stripe_sdk_exception_namespace_is_used(self):
        self.assertEqual(stripe_sdk.VERSION, "13.2.0")
        self.assertTrue(hasattr(stripe_sdk, "StripeError"))
        self.assertFalse(hasattr(stripe_sdk, "error"))
        self.assertIs(self.api.STRIPE_ERROR, stripe_sdk.StripeError)

    def test_unknown_reconciliation_releases_route_writer_lock_before_stripe(self):
        fingerprint = self._fingerprint()
        with self.api.get_db() as db:
            db.execute(
                """INSERT INTO funding_attempts
                   (operation_key,attempt_number,request_fingerprint,processor_idempotency_key,
                    employer_id,order_id,milestone_id,base_amount_cents,platform_fee_cents,
                    processing_fee_cents,charged_total_cents,status)
                   VALUES ('milestone:20',1,?,'escrow-fund:milestone:20:attempt:1',
                           2,10,20,2500,25,75,2600,'unknown')""",
                [fingerprint],
            )
            db.commit()

        writer_committed = False

        def search_without_writer_lock(**kwargs):
            nonlocal writer_committed
            writer = sqlite3.connect(self.db_path, timeout=0.2)
            try:
                writer.execute("UPDATE orders SET updated_at=datetime('now') WHERE id=10")
                writer.commit()
                writer_committed = True
            finally:
                writer.close()
            return SimpleNamespace(data=[])

        self.search.side_effect = search_without_writer_lock
        with self.api.get_db() as db:
            db.execute("BEGIN IMMEDIATE")
            with self.assertRaises(self.api.FundingReconciliationRequired):
                self.api.fund_escrow_stripe(
                    db, 2, 25, 10, 20, funding_identity="milestone:20"
                )
        self.assertTrue(writer_committed)
        self.create.assert_not_called()

    def test_webhook_success_during_create_exception_remains_committed_and_replayable(self):
        def webhook_then_connection_error(**kwargs):
            with self.api.get_db() as observer:
                attempt = observer.execute("SELECT * FROM funding_attempts").fetchone()
                intent = self._succeeded_intent(
                    attempt["request_fingerprint"],
                    pi_id="pi_webhook_won",
                    attempt_id=str(attempt["id"]),
                    attempt_number=str(attempt["attempt_number"]),
                )
                result = self.api.reconcile_funding_intent_event(observer, intent)
                self.assertEqual(result["outcome"], "succeeded")
            raise APIConnectionError("connection reset after send")

        self.create.side_effect = webhook_then_connection_error
        with self.api.get_db() as db:
            with self.assertRaises(self.api.FundingReconciliationRequired):
                self.api.fund_escrow_stripe(
                    db, 2, 25, 10, 20, funding_identity="milestone:20"
                )
        with self.api.get_db() as db:
            attempt = db.execute("SELECT * FROM funding_attempts").fetchone()
            hold = db.execute("SELECT * FROM escrow_holds").fetchone()
        self.assertEqual(attempt["status"], "committed")
        self.assertEqual(hold["funding_attempt_id"], attempt["id"])
        with self.api.get_db() as db:
            replay = self.api.fund_escrow_stripe(
                db, 2, 25, 10, 20, funding_identity="milestone:20"
            )
        self.assertEqual(replay, ("pi_webhook_won", "replayed"))
        self.assertEqual(self.create.call_count, 1)

    def test_create_success_cannot_replace_a_different_committed_webhook_intent(self):
        def webhook_then_different_success(**kwargs):
            with self.api.get_db() as observer:
                attempt = observer.execute("SELECT * FROM funding_attempts").fetchone()
                webhook_intent = self._succeeded_intent(
                    attempt["request_fingerprint"],
                    pi_id="pi_webhook_committed",
                    attempt_id=str(attempt["id"]),
                    attempt_number=str(attempt["attempt_number"]),
                )
                result = self.api.reconcile_funding_intent_event(observer, webhook_intent)
                self.assertEqual(result["outcome"], "succeeded")
                return self._succeeded_intent(
                    attempt["request_fingerprint"],
                    pi_id="pi_conflicting_create_response",
                    attempt_id=str(attempt["id"]),
                    attempt_number=str(attempt["attempt_number"]),
                )

        self.create.side_effect = webhook_then_different_success
        with self.api.get_db() as db:
            with self.assertRaises(self.api.FundingReconciliationRequired):
                self.api.fund_escrow_stripe(
                    db, 2, 25, 10, 20, funding_identity="milestone:20"
                )
        with self.api.get_db() as db:
            attempt = db.execute("SELECT * FROM funding_attempts").fetchone()
            hold = db.execute("SELECT * FROM escrow_holds").fetchone()
        self.assertEqual(attempt["status"], "committed")
        self.assertEqual(attempt["stripe_payment_intent_id"], "pi_webhook_committed")
        self.assertEqual(hold["stripe_payment_intent_id"], "pi_webhook_committed")
        self.create.assert_called_once()

    def test_create_success_cannot_replace_a_different_failed_webhook_intent(self):
        def failed_webhook_then_different_success(**kwargs):
            with self.api.get_db() as observer:
                attempt = observer.execute("SELECT * FROM funding_attempts").fetchone()
                failed_intent = self._succeeded_intent(
                    attempt["request_fingerprint"],
                    pi_id="pi_webhook_failed",
                    attempt_id=str(attempt["id"]),
                    attempt_number=str(attempt["attempt_number"]),
                )
                failed_intent["status"] = "requires_payment_method"
                failed_intent["amount_received"] = 0
                result = self.api.reconcile_funding_intent_event(observer, failed_intent)
                self.assertEqual(result["outcome"], "failed")
                return self._succeeded_intent(
                    attempt["request_fingerprint"],
                    pi_id="pi_conflicting_create_after_failure",
                    attempt_id=str(attempt["id"]),
                    attempt_number=str(attempt["attempt_number"]),
                )

        self.create.side_effect = failed_webhook_then_different_success
        with self.api.get_db() as db:
            with self.assertRaises(self.api.FundingReconciliationRequired):
                self.api.fund_escrow_stripe(
                    db, 2, 25, 10, 20, funding_identity="milestone:20"
                )
        with self.api.get_db() as db:
            attempt = db.execute("SELECT * FROM funding_attempts").fetchone()
            hold_count = db.execute("SELECT COUNT(*) FROM escrow_holds").fetchone()[0]
        self.assertEqual(attempt["status"], "failed")
        self.assertEqual(attempt["stripe_payment_intent_id"], "pi_webhook_failed")
        self.assertEqual(attempt["evidence_source"], "signed_webhook")
        self.assertEqual(attempt["error_code"], "processor_intent_conflict")
        self.assertEqual(hold_count, 0)
        self.create.assert_called_once()

        with self.api.get_db() as db:
            with self.assertRaises(self.api.FundingReconciliationRequired):
                self.api.fund_escrow_stripe(
                    db, 2, 25, 10, 20, funding_identity="milestone:20"
                )
        self.create.assert_called_once()

    def test_non_success_create_cannot_leave_a_different_failed_webhook_intent_retryable(self):
        def failed_webhook_then_different_failure(**kwargs):
            with self.api.get_db() as observer:
                attempt = observer.execute("SELECT * FROM funding_attempts").fetchone()
                durable_failure = self._succeeded_intent(
                    attempt["request_fingerprint"],
                    pi_id="pi_webhook_failure_a",
                    attempt_id=str(attempt["id"]),
                    attempt_number=str(attempt["attempt_number"]),
                )
                durable_failure["status"] = "requires_payment_method"
                durable_failure["amount_received"] = 0
                self.assertEqual(
                    self.api.reconcile_funding_intent_event(observer, durable_failure)["outcome"],
                    "failed",
                )
                conflicting_failure = dict(durable_failure)
                conflicting_failure["id"] = "pi_create_failure_b"
                return conflicting_failure

        self.create.side_effect = failed_webhook_then_different_failure
        with self.api.get_db() as db:
            with self.assertRaises(self.api.FundingReconciliationRequired):
                self.api.fund_escrow_stripe(
                    db, 2, 25, 10, 20, funding_identity="milestone:20"
                )
        with self.api.get_db() as db:
            attempt = db.execute("SELECT * FROM funding_attempts").fetchone()
        self.assertEqual(attempt["status"], "failed")
        self.assertEqual(attempt["stripe_payment_intent_id"], "pi_webhook_failure_a")
        self.assertEqual(attempt["evidence_source"], "signed_webhook")
        self.assertEqual(attempt["error_code"], "processor_intent_conflict")

        with self.api.get_db() as db:
            with self.assertRaises(self.api.FundingReconciliationRequired):
                self.api.fund_escrow_stripe(
                    db, 2, 25, 10, 20, funding_identity="milestone:20"
                )
        self.create.assert_called_once()

    def test_create_exception_cannot_regress_webhook_processor_success_before_hold_commit(self):
        def webhook_success_then_local_commit_failure(**kwargs):
            with self.api.get_db() as observer:
                attempt = observer.execute("SELECT * FROM funding_attempts").fetchone()
                intent = self._succeeded_intent(
                    attempt["request_fingerprint"],
                    pi_id="pi_webhook_processor_succeeded",
                    attempt_id=str(attempt["id"]),
                    attempt_number=str(attempt["attempt_number"]),
                )
                with mock.patch.object(
                    self.api,
                    "_commit_funding_attempt",
                    side_effect=RuntimeError("injected local hold failure"),
                ):
                    with self.assertRaises(RuntimeError):
                        self.api.reconcile_funding_intent_event(observer, intent)
            raise APIConnectionError("connection reset after send")

        self.create.side_effect = webhook_success_then_local_commit_failure
        with self.api.get_db() as db:
            with self.assertRaises(self.api.FundingReconciliationRequired):
                self.api.fund_escrow_stripe(
                    db, 2, 25, 10, 20, funding_identity="milestone:20"
                )
        with self.api.get_db() as db:
            attempt = db.execute("SELECT * FROM funding_attempts").fetchone()
            hold_count = db.execute("SELECT COUNT(*) FROM escrow_holds").fetchone()[0]
        self.assertEqual(attempt["status"], "processor_succeeded")
        self.assertEqual(attempt["stripe_payment_intent_id"], "pi_webhook_processor_succeeded")
        self.assertEqual(attempt["processor_status"], "succeeded")
        self.assertEqual(attempt["evidence_source"], "signed_webhook")
        self.assertEqual(hold_count, 0)
        self.create.assert_called_once()

    def test_non_success_create_result_cannot_regress_webhook_processor_success(self):
        def webhook_success_then_processing_result(**kwargs):
            with self.api.get_db() as observer:
                attempt = observer.execute("SELECT * FROM funding_attempts").fetchone()
                succeeded = self._succeeded_intent(
                    attempt["request_fingerprint"],
                    pi_id="pi_webhook_processor_succeeded",
                    attempt_id=str(attempt["id"]),
                    attempt_number=str(attempt["attempt_number"]),
                )
                with mock.patch.object(
                    self.api,
                    "_commit_funding_attempt",
                    side_effect=RuntimeError("injected local hold failure"),
                ):
                    with self.assertRaises(RuntimeError):
                        self.api.reconcile_funding_intent_event(observer, succeeded)
            return SimpleNamespace(
                id="pi_webhook_processor_succeeded",
                status="processing",
                amount=kwargs["amount"],
                amount_received=0,
                currency=kwargs["currency"],
                metadata=kwargs["metadata"],
            )

        self.create.side_effect = webhook_success_then_processing_result
        with self.api.get_db() as db:
            with self.assertRaises(self.api.FundingReconciliationRequired):
                self.api.fund_escrow_stripe(
                    db, 2, 25, 10, 20, funding_identity="milestone:20"
                )
        with self.api.get_db() as db:
            attempt = db.execute("SELECT * FROM funding_attempts").fetchone()
            hold_count = db.execute("SELECT COUNT(*) FROM escrow_holds").fetchone()[0]
        self.assertEqual(attempt["status"], "processor_succeeded")
        self.assertEqual(attempt["stripe_payment_intent_id"], "pi_webhook_processor_succeeded")
        self.assertEqual(attempt["processor_status"], "succeeded")
        self.assertEqual(attempt["evidence_source"], "signed_webhook")
        self.assertEqual(hold_count, 0)
        self.create.assert_called_once()

    def test_unknown_attempt_reconciles_from_stripe_search_without_second_create(self):
        fingerprint = self._fingerprint()
        with self.api.get_db() as db:
            cursor = db.execute(
                """INSERT INTO funding_attempts
                   (operation_key,attempt_number,request_fingerprint,processor_idempotency_key,
                    employer_id,order_id,milestone_id,base_amount_cents,platform_fee_cents,
                    processing_fee_cents,charged_total_cents,status)
                   VALUES ('milestone:20',1,?,'escrow-fund:milestone:20:attempt:1',2,10,20,2500,25,75,2600,'unknown')""",
                [fingerprint],
            )
            attempt_id = cursor.lastrowid
            db.commit()
        self.search.return_value = SimpleNamespace(
            data=[self._succeeded_intent(fingerprint, "pi_found", str(attempt_id), "1")]
        )

        with self.api.get_db() as db:
            result = self.api.fund_escrow_stripe(
                db, 2, 25, 10, 20, funding_identity="milestone:20"
            )
        self.assertEqual(result, ("pi_found", "reconciled"))
        self.create.assert_not_called()
        with self.api.get_db() as db:
            self.assertEqual(db.execute("SELECT status FROM funding_attempts").fetchone()[0], "committed")
            self.assertEqual(db.execute("SELECT stripe_payment_intent_id FROM escrow_holds").fetchone()[0], "pi_found")

    def test_retrieve_reconciliation_rejects_concurrent_immutable_binding_drift(self):
        attempt = self._insert_unknown_attempt("pi_retrieve_drift")

        def retrieve_then_drift(processor_intent_id):
            self.assertEqual(processor_intent_id, "pi_retrieve_drift")
            self._drift_attempt_bindings(attempt["id"])
            return self._succeeded_intent(
                attempt["request_fingerprint"],
                "pi_retrieve_drift",
                str(attempt["id"]),
                str(attempt["attempt_number"]),
            )

        self.retrieve.side_effect = retrieve_then_drift
        with self.api.get_db() as db:
            with self.assertRaises(self.api.FundingConflict):
                self.api.reconcile_funding_attempt(db, attempt, apply=True)
        self._assert_manual_binding_freeze(attempt["id"])
        self._assert_original_retry_is_blocked()
        self.retrieve.assert_called_once_with("pi_retrieve_drift")
        self.create.assert_not_called()

    def test_binding_freeze_marks_original_anchor_instead_of_rolling_back_on_uniqueness(self):
        attempt = self._insert_unknown_attempt("pi_retrieve_anchor")

        def retrieve_then_drift_and_anchor(processor_intent_id):
            self._drift_attempt_bindings(attempt["id"])
            with self.api.get_db() as writer:
                writer.execute(
                    """INSERT INTO funding_attempts
                       (operation_key,attempt_number,request_fingerprint,processor_idempotency_key,
                        employer_id,order_id,milestone_id,base_amount_cents,platform_fee_cents,
                        processing_fee_cents,charged_total_cents,status)
                       VALUES ('milestone:20',2,?,'escrow-fund:milestone:20:attempt:2',
                               2,10,20,2500,25,75,2600,'unknown')""",
                    [attempt["request_fingerprint"]],
                )
                writer.commit()
            return self._succeeded_intent(
                attempt["request_fingerprint"],
                processor_intent_id,
                str(attempt["id"]),
                str(attempt["attempt_number"]),
            )

        self.retrieve.side_effect = retrieve_then_drift_and_anchor
        with self.api.get_db() as db:
            with self.assertRaises(self.api.FundingConflict):
                self.api.reconcile_funding_attempt(db, attempt, apply=True)
        with self.api.get_db() as db:
            rows = db.execute(
                "SELECT id,operation_key,status,error_code FROM funding_attempts ORDER BY id"
            ).fetchall()
            hold_count = db.execute("SELECT COUNT(*) FROM escrow_holds").fetchone()[0]
        self.assertEqual(rows[0]["operation_key"], "milestone:999")
        self.assertEqual(rows[0]["error_code"], "attempt_binding_conflict")
        self.assertEqual(rows[1]["operation_key"], "milestone:20")
        self.assertEqual(rows[1]["error_code"], "prior_attempt_binding_conflict")
        self.assertEqual(hold_count, 0)
        self._assert_original_retry_is_blocked()
        with self.api.get_db() as writer:
            writer.execute("UPDATE orders SET updated_at=datetime('now') WHERE id=10")
            writer.commit()
        self.retrieve.assert_called_once_with("pi_retrieve_anchor")
        self.create.assert_not_called()

    def test_binding_freeze_handles_incoming_intent_owned_by_committed_anchor(self):
        attempt = self._insert_unknown_attempt()

        def search_then_drift_and_commit_anchor(**kwargs):
            self._drift_attempt_bindings(attempt["id"])
            with self.api.get_db() as writer:
                cursor = writer.execute(
                    """INSERT INTO funding_attempts
                       (operation_key,attempt_number,request_fingerprint,processor_idempotency_key,
                        employer_id,order_id,milestone_id,base_amount_cents,platform_fee_cents,
                        processing_fee_cents,charged_total_cents,status,stripe_payment_intent_id,
                        processor_status,evidence_source)
                       VALUES ('milestone:20',2,?,'escrow-fund:milestone:20:attempt:2',
                               2,10,20,2500,25,75,2600,'committed','pi_anchor_B',
                               'succeeded','signed_webhook')""",
                    [attempt["request_fingerprint"]],
                )
                writer.execute(
                    """INSERT INTO escrow_holds
                       (order_id,milestone_id,amount,base_amount_cents,platform_fee_cents,
                        processing_fee_cents,charged_total_cents,fee_policy_version,
                        funding_identity,funding_attempt_id,status,stripe_payment_intent_id)
                       VALUES (10,20,25,2500,25,75,2600,'component-half-up-v1',
                               'milestone:20',?,'held','pi_anchor_B')""",
                    [cursor.lastrowid],
                )
                writer.commit()
            return SimpleNamespace(data=[self._succeeded_intent(
                attempt["request_fingerprint"],
                "pi_anchor_B",
                str(attempt["id"]),
                str(attempt["attempt_number"]),
            )])

        self.search.side_effect = search_then_drift_and_commit_anchor
        with self.api.get_db() as db:
            with self.assertRaises(self.api.FundingConflict):
                self.api.reconcile_funding_attempt(db, attempt, apply=True)

        with self.api.get_db() as db:
            rows = db.execute(
                """SELECT id,status,stripe_payment_intent_id,evidence_source,error_code
                   FROM funding_attempts ORDER BY id"""
            ).fetchall()
            evidence = db.execute(
                """SELECT attempt_id,incoming_intent_id,intent_owner_attempt_id
                   FROM funding_attempt_conflict_evidence ORDER BY attempt_id,id"""
            ).fetchall()
        self.assertEqual(
            sum(row["stripe_payment_intent_id"] == "pi_anchor_B" for row in rows), 1
        )
        self.assertEqual(rows[0]["status"], "unknown")
        self.assertIn(
            rows[0]["error_code"], self.api.PROCESSOR_FREE_FUNDING_ERROR_CODES
        )
        self.assertEqual(rows[1]["status"], "committed")
        self.assertEqual(rows[1]["evidence_source"], "signed_webhook")
        self.assertIn(
            rows[1]["error_code"], self.api.PROCESSOR_FREE_FUNDING_ERROR_CODES
        )
        self.assertEqual(
            {row["attempt_id"] for row in evidence},
            {rows[0]["id"], rows[1]["id"]},
        )
        self.assertEqual(
            {row["incoming_intent_id"] for row in evidence}, {"pi_anchor_B"}
        )
        self.assertEqual(
            {row["intent_owner_attempt_id"] for row in evidence}, {rows[1]["id"]}
        )

        with self.api.get_db() as db:
            with self.assertRaises(self.api.FundingReconciliationRequired):
                self.api.fund_escrow_stripe(
                    db, 2, 25, 10, 20, funding_identity="milestone:20"
                )
        with self.api.get_db() as writer:
            writer.execute("UPDATE orders SET updated_at=datetime('now') WHERE id=10")
            writer.commit()
        self.create.assert_not_called()

    def test_binding_freeze_marks_unrelated_owner_without_copying_intent(self):
        attempt = self._insert_unknown_attempt()
        with self.api.get_db() as db:
            cursor = db.execute(
                """INSERT INTO funding_attempts
                   (operation_key,attempt_number,request_fingerprint,processor_idempotency_key,
                    employer_id,order_id,milestone_id,base_amount_cents,platform_fee_cents,
                    processing_fee_cents,charged_total_cents,status,stripe_payment_intent_id,
                    processor_status,evidence_source)
                   VALUES ('fixed-hire:10',1,?,'escrow-fund:fixed-hire:10:attempt:1',
                           2,10,NULL,2500,25,75,2600,'committed','pi_unrelated_owner',
                           'succeeded','signed_webhook')""",
                ["1" * 64],
            )
            owner_id = cursor.lastrowid
            db.commit()

        def search_then_drift(**kwargs):
            self._drift_attempt_bindings(attempt["id"])
            return SimpleNamespace(data=[self._succeeded_intent(
                attempt["request_fingerprint"],
                "pi_unrelated_owner",
                str(attempt["id"]),
                str(attempt["attempt_number"]),
            )])

        self.search.side_effect = search_then_drift
        with self.api.get_db() as db:
            with self.assertRaises(self.api.FundingConflict):
                self.api.reconcile_funding_attempt(db, attempt, apply=True)
        with self.api.get_db() as db:
            current = db.execute(
                "SELECT * FROM funding_attempts WHERE id=?", [attempt["id"]]
            ).fetchone()
            owner = db.execute(
                "SELECT * FROM funding_attempts WHERE id=?", [owner_id]
            ).fetchone()
            evidence_attempts = {
                row[0]
                for row in db.execute(
                    """SELECT attempt_id FROM funding_attempt_conflict_evidence
                       WHERE incoming_intent_id='pi_unrelated_owner'"""
                ).fetchall()
            }
        self.assertEqual(current["operation_key"], "milestone:20")
        self.assertIsNone(current["stripe_payment_intent_id"])
        self.assertEqual(current["status"], "unknown")
        self.assertIn(
            current["error_code"], self.api.PROCESSOR_FREE_FUNDING_ERROR_CODES
        )
        self.assertEqual(owner["status"], "committed")
        self.assertEqual(owner["stripe_payment_intent_id"], "pi_unrelated_owner")
        self.assertEqual(owner["evidence_source"], "signed_webhook")
        self.assertIn(owner["error_code"], self.api.PROCESSOR_FREE_FUNDING_ERROR_CODES)
        self.assertEqual(evidence_attempts, {attempt["id"], owner_id})
        self._assert_original_retry_is_blocked()
        self.create.assert_not_called()

    def test_search_reconciliation_rejects_concurrent_immutable_binding_drift(self):
        attempt = self._insert_unknown_attempt()

        def search_then_drift(**kwargs):
            self._drift_attempt_bindings(attempt["id"])
            return SimpleNamespace(data=[self._succeeded_intent(
                attempt["request_fingerprint"],
                "pi_search_drift",
                str(attempt["id"]),
                str(attempt["attempt_number"]),
            )])

        self.search.side_effect = search_then_drift
        with self.api.get_db() as db:
            with self.assertRaises(self.api.FundingConflict):
                self.api.reconcile_funding_attempt(db, attempt, apply=True)
        self._assert_manual_binding_freeze(attempt["id"])
        self._assert_original_retry_is_blocked()
        self.search.assert_called_once()
        self.create.assert_not_called()

    def test_signed_webhook_apply_rejects_concurrent_immutable_binding_drift(self):
        attempt = self._insert_unknown_attempt()
        intent = self._succeeded_intent(
            attempt["request_fingerprint"],
            "pi_signed_drift",
            str(attempt["id"]),
            str(attempt["attempt_number"]),
        )
        original_inspection = self.api._processor_intent_inspection

        def inspect_then_drift(snapshot, evidence, source):
            inspection = original_inspection(snapshot, evidence, source)
            if source == "signed_webhook":
                self._drift_attempt_bindings(snapshot["id"])
            return inspection

        with mock.patch.object(
            self.api, "_processor_intent_inspection", side_effect=inspect_then_drift
        ):
            with self.api.get_db() as db:
                with self.assertRaises(self.api.FundingConflict):
                    self.api.reconcile_funding_intent_event(db, intent)
        self._assert_manual_binding_freeze(attempt["id"])
        self._assert_original_retry_is_blocked()
        self.create.assert_not_called()

    def test_binding_freeze_preserves_signed_success_and_records_conflicting_create_intent(self):
        def signed_success_then_drift_then_different_create(**kwargs):
            with self.api.get_db() as observer:
                attempt = observer.execute("SELECT * FROM funding_attempts").fetchone()
                signed = self._succeeded_intent(
                    attempt["request_fingerprint"],
                    "pi_signed_A",
                    str(attempt["id"]),
                    str(attempt["attempt_number"]),
                )
                with mock.patch.object(
                    self.api,
                    "_commit_funding_attempt",
                    side_effect=RuntimeError("injected local hold failure"),
                ):
                    with self.assertRaises(RuntimeError):
                        self.api.reconcile_funding_intent_event(observer, signed)
            self._drift_attempt_bindings(attempt["id"])
            return self._succeeded_intent(
                attempt["request_fingerprint"],
                "pi_stale_create_B",
                str(attempt["id"]),
                str(attempt["attempt_number"]),
            )

        self.create.side_effect = signed_success_then_drift_then_different_create
        with self.api.get_db() as db:
            with self.assertRaises(self.api.FundingConflict):
                self.api.fund_escrow_stripe(
                    db, 2, 25, 10, 20, funding_identity="milestone:20"
                )
        with self.api.get_db() as db:
            attempt = db.execute("SELECT * FROM funding_attempts").fetchone()
            hold_count = db.execute("SELECT COUNT(*) FROM escrow_holds").fetchone()[0]
        self.assertEqual(attempt["status"], "processor_succeeded")
        self.assertEqual(attempt["stripe_payment_intent_id"], "pi_signed_A")
        self.assertEqual(attempt["processor_status"], "succeeded")
        self.assertEqual(attempt["evidence_source"], "signed_webhook")
        self.assertEqual(attempt["error_code"], "processor_intent_conflict")
        self.assertIn("binding", attempt["error_message"].lower())
        self.assertEqual(hold_count, 0)

        with self.api.get_db() as db:
            expected = db.execute(
                "SELECT * FROM funding_attempts WHERE id=?", [attempt["id"]]
            ).fetchone()
        for incoming_intent in ("pi_stale_create_C", "pi_stale_create_C"):
            self._drift_attempt_bindings(attempt["id"])
            with self.api.get_db() as db:
                db.execute("BEGIN IMMEDIATE")
                current = db.execute(
                    "SELECT * FROM funding_attempts WHERE id=?", [attempt["id"]]
                ).fetchone()
                with self.assertRaises(self.api.FundingConflict):
                    self.api._freeze_funding_attempt_binding_conflict(
                        db,
                        current,
                        expected,
                        processor_intent_id=incoming_intent,
                        processor_status="succeeded",
                        evidence_source="processor_create",
                    )
        with self.api.get_db() as db:
            evidence = db.execute(
                """SELECT incoming_intent_id,COUNT(*) AS copies
                   FROM funding_attempt_conflict_evidence
                   WHERE attempt_id=? GROUP BY incoming_intent_id ORDER BY incoming_intent_id""",
                [attempt["id"]],
            ).fetchall()
        self.assertEqual(
            [(row["incoming_intent_id"], row["copies"]) for row in evidence],
            [("pi_stale_create_B", 1), ("pi_stale_create_C", 1)],
        )
        self._assert_manual_binding_freeze(
            attempt["id"], expected_status="processor_succeeded"
        )
        self._assert_original_retry_is_blocked()
        self.create.assert_called_once()

    def test_no_drift_processor_conflicts_append_distinct_ids_and_dedupe_redelivery(self):
        fingerprint = self._fingerprint()
        with self.api.get_db() as db:
            attempt_id = db.execute(
                """INSERT INTO funding_attempts
                   (operation_key,attempt_number,request_fingerprint,
                    processor_idempotency_key,employer_id,order_id,milestone_id,
                    base_amount_cents,platform_fee_cents,processing_fee_cents,
                    charged_total_cents,status,stripe_payment_intent_id,
                    processor_status,evidence_source)
                   VALUES ('milestone:20',1,?,'no-drift-conflict',2,10,20,
                           2500,25,75,2600,'processor_succeeded','pi_no_drift_A',
                           'succeeded','signed_webhook')""",
                [fingerprint],
            ).lastrowid
            db.commit()

        observations = (
            ("pi_no_drift_B", "evt_B"),
            ("pi_no_drift_C", "evt_C_1"),
            ("pi_no_drift_C", "evt_C_1"),
            ("pi_no_drift_C", "evt_C_2"),
        )
        for incoming_intent, processor_event_id in observations:
            with self.api.get_db() as db:
                attempt = db.execute(
                    "SELECT * FROM funding_attempts WHERE id=?", [attempt_id]
                ).fetchone()
                self.assertTrue(
                    self.api._freeze_processor_intent_conflict(
                        db,
                        attempt,
                        incoming_intent,
                        "succeeded",
                        "signed_webhook",
                        processor_event_id,
                    )
                )

        with self.api.get_db() as db:
            attempt = db.execute(
                "SELECT * FROM funding_attempts WHERE id=?", [attempt_id]
            ).fetchone()
            evidence = db.execute(
                """SELECT incoming_intent_id,processor_event_id
                   FROM funding_attempt_conflict_evidence
                   WHERE attempt_id=? ORDER BY id""",
                [attempt_id],
            ).fetchall()
            with self.assertRaises(self.api.FundingReconciliationRequired):
                self.api.fund_escrow_stripe(
                    db, 2, 25, 10, 20, funding_identity="milestone:20"
                )
        self.assertEqual(attempt["status"], "processor_succeeded")
        self.assertEqual(attempt["stripe_payment_intent_id"], "pi_no_drift_A")
        self.assertEqual(attempt["error_code"], "processor_intent_conflict")
        self.assertEqual(
            [
                (row["incoming_intent_id"], row["processor_event_id"])
                for row in evidence
            ],
            [
                ("pi_no_drift_B", "evt_B"),
                ("pi_no_drift_C", "evt_C_1"),
                ("pi_no_drift_C", "evt_C_2"),
            ],
        )
        self.create.assert_not_called()
        self.retrieve.assert_not_called()
        self.search.assert_not_called()

    def test_exact_hold_replay_blocks_legacy_conflict_on_sibling_attempt(self):
        self.create.return_value = self._succeeded_intent(
            self._fingerprint(), "pi_ledger"
        )
        with self.api.get_db() as db:
            self.assertEqual(
                self.api.fund_escrow_stripe(
                    db, 2, 25, 10, 20, funding_identity="milestone:20"
                ),
                ("pi_ledger", "live"),
            )
            db.execute(
                """INSERT INTO funding_attempts
                   (operation_key,attempt_number,request_fingerprint,processor_idempotency_key,
                    employer_id,order_id,milestone_id,base_amount_cents,platform_fee_cents,
                    processing_fee_cents,charged_total_cents,status,error_code,error_message)
                   VALUES ('milestone:20',2,?,'escrow-fund:milestone:20:attempt:2',
                           2,10,20,2500,25,75,2600,'failed','processor_intent_conflict',
                           'legacy conflict requires manual review')""",
                [self._fingerprint()],
            )
            db.commit()
        self.create.reset_mock()
        with self.api.get_db() as db:
            with self.assertRaises(self.api.FundingReconciliationRequired):
                self.api.fund_escrow_stripe(
                    db, 2, 25, 10, 20, funding_identity="milestone:20"
                )
        self.create.assert_not_called()

    def test_escrow_release_blocks_structured_funding_conflict_before_processor(self):
        self.create.return_value = self._succeeded_intent(
            self._fingerprint(), "pi_release_conflict"
        )
        with self.api.get_db() as db:
            self.api.fund_escrow_stripe(
                db, 2, 25, 10, 20, funding_identity="milestone:20"
            )
            attempt = db.execute(
                "SELECT * FROM funding_attempts WHERE stripe_payment_intent_id=?",
                ["pi_release_conflict"],
            ).fetchone()
            self.api._insert_funding_conflict_evidence(
                db,
                attempt_id=attempt["id"],
                conflict_type="processor_intent_conflict",
                expected=attempt,
                observed=attempt,
                canonical_intent_id="pi_release_conflict",
                incoming_intent_id="pi_release_other",
                incoming_processor_status="succeeded",
                incoming_evidence_source="signed_webhook",
            )
            db.commit()

        transfer_create = mock.Mock()
        self.api.stripe.Transfer = SimpleNamespace(create=transfer_create)
        with self.api.get_db() as db:
            with self.assertRaises(self.api.FundingReconciliationRequired):
                self.api.release_escrow_to_worker(db, 10, 20, 25, 3)
            hold = db.execute(
                "SELECT status FROM escrow_holds WHERE order_id=10 AND milestone_id=20"
            ).fetchone()
        transfer_create.assert_not_called()
        self.assertEqual(hold["status"], "held")

    def test_mismatched_processor_evidence_fails_closed(self):
        fingerprint = self._fingerprint()
        with self.api.get_db() as db:
            cursor = db.execute(
                """INSERT INTO funding_attempts
                   (operation_key,attempt_number,request_fingerprint,processor_idempotency_key,
                    employer_id,order_id,milestone_id,base_amount_cents,platform_fee_cents,
                    processing_fee_cents,charged_total_cents,status)
                   VALUES ('milestone:20',1,?,'escrow-fund:milestone:20:attempt:1',2,10,20,2500,25,75,2600,'unknown')""",
                [fingerprint],
            )
            attempt_id = cursor.lastrowid
            db.commit()
        bad = self._succeeded_intent(fingerprint, "pi_bad", str(attempt_id), "1")
        bad["amount"] = 2601
        self.search.return_value = SimpleNamespace(data=[bad])

        with self.api.get_db() as db:
            with self.assertRaises(self.api.FundingReconciliationRequired):
                self.api.fund_escrow_stripe(db, 2, 25, 10, 20, funding_identity="milestone:20")
        with self.api.get_db() as db:
            self.assertEqual(db.execute("SELECT status FROM funding_attempts").fetchone()[0], "unknown")
            self.assertEqual(db.execute("SELECT COUNT(*) FROM escrow_holds").fetchone()[0], 0)

    def test_each_processor_evidence_binding_mismatch_fails_closed(self):
        fingerprint = self._fingerprint()
        with self.api.get_db() as db:
            cursor = db.execute(
                """INSERT INTO funding_attempts
                   (operation_key,attempt_number,request_fingerprint,processor_idempotency_key,
                    employer_id,order_id,milestone_id,base_amount_cents,platform_fee_cents,
                    processing_fee_cents,charged_total_cents,stripe_payment_intent_id,status)
                   VALUES ('milestone:20',1,?,'escrow-fund:milestone:20:attempt:1',
                           2,10,20,2500,25,75,2600,'pi_expected','unknown')""",
                [fingerprint],
            )
            attempt = db.execute("SELECT * FROM funding_attempts WHERE id=?", [cursor.lastrowid]).fetchone()
            good = self._succeeded_intent(
                fingerprint, "pi_expected", str(attempt["id"]), "1"
            )
            self.assertEqual(
                self.api._processor_intent_inspection(attempt, good, "test")["outcome"],
                "succeeded",
            )
            variants = {
                "processor_intent_id": {**good, "id": "pi_other"},
                "amount": {**good, "amount": 2601},
                "amount_received": {**good, "amount_received": 2599},
                "currency": {**good, "currency": "eur"},
            }
            for metadata_key, wrong_value in {
                "funding_identity": "milestone:other",
                "funding_request_fingerprint": "0" * 64,
                "funding_attempt_id": "999",
                "funding_attempt_number": "2",
                "order_id": "11",
                "milestone_id": "21",
                "employer_id": "1",
            }.items():
                variants[f"metadata.{metadata_key}"] = {
                    **good,
                    "metadata": {**good["metadata"], metadata_key: wrong_value},
                }
            for field, evidence in variants.items():
                with self.subTest(field=field):
                    inspection = self.api._processor_intent_inspection(
                        attempt, evidence, "test"
                    )
                    self.assertEqual(inspection["outcome"], "mismatch")
                    self.assertIn(field, inspection["mismatches"])
            self.assertEqual(db.execute("SELECT COUNT(*) FROM escrow_holds").fetchone()[0], 0)

    def test_create_response_must_match_amount_currency_and_metadata(self):
        self.create.return_value = SimpleNamespace(
            id="pi_bad_create",
            status="succeeded",
            amount=1,
            currency="usd",
            metadata={},
        )
        with self.api.get_db() as db:
            with self.assertRaises(self.api.FundingReconciliationRequired):
                self.api.fund_escrow_stripe(
                    db, 2, 25, 10, 20, funding_identity="milestone:20"
                )
        with self.api.get_db() as db:
            attempt = db.execute("SELECT * FROM funding_attempts").fetchone()
            self.assertEqual(attempt["status"], "unknown")
            self.assertEqual(attempt["evidence_source"], "processor_create")
            self.assertEqual(db.execute("SELECT COUNT(*) FROM escrow_holds").fetchone()[0], 0)

    def test_signed_webhook_reconciles_and_committed_state_never_regresses(self):
        self.create.side_effect = APIConnectionError("connection reset after send")
        with self.api.get_db() as db:
            with self.assertRaises(self.api.FundingReconciliationRequired):
                self.api.fund_escrow_stripe(
                    db, 2, 25, 10, 20, funding_identity="milestone:20"
                )
            attempt = db.execute("SELECT * FROM funding_attempts").fetchone()
            success = self._succeeded_intent(
                attempt["request_fingerprint"],
                "pi_webhook",
                str(attempt["id"]),
                str(attempt["attempt_number"]),
            )
            result = self.api.reconcile_funding_intent_event(db, success)
            self.assertEqual(result["outcome"], "succeeded")

            failed = success.copy()
            failed["status"] = "requires_payment_method"
            result = self.api.reconcile_funding_intent_event(db, failed)
            self.assertEqual(result["outcome"], "ignored_committed")

            conflicting = success.copy()
            conflicting["id"] = "pi_webhook_conflict"
            result = self.api.reconcile_funding_intent_event(
                db, conflicting, "evt_webhook_conflict"
            )
            self.assertEqual(result["outcome"], "ignored_committed")

        with self.api.get_db() as db:
            attempt = db.execute("SELECT * FROM funding_attempts").fetchone()
            hold = db.execute("SELECT * FROM escrow_holds").fetchone()
            evidence = db.execute(
                """SELECT processor_event_id FROM funding_attempt_conflict_evidence
                   WHERE attempt_id=? AND incoming_intent_id='pi_webhook_conflict'""",
                [attempt["id"]],
            ).fetchone()
            self.assertEqual(attempt["status"], "committed")
            self.assertEqual(attempt["evidence_source"], "signed_webhook")
            self.assertEqual(attempt["error_code"], "processor_intent_conflict")
            self.assertEqual(evidence["processor_event_id"], "evt_webhook_conflict")
            self.assertEqual(hold["status"], "held")

    def test_stale_reconciliation_result_cannot_regress_processor_success(self):
        fingerprint = self._fingerprint()
        with self.api.get_db() as db:
            cursor = db.execute(
                """INSERT INTO funding_attempts
                   (operation_key,attempt_number,request_fingerprint,processor_idempotency_key,
                    employer_id,order_id,milestone_id,base_amount_cents,platform_fee_cents,
                    processing_fee_cents,charged_total_cents,status)
                   VALUES ('milestone:20',1,?,'escrow-fund:milestone:20:attempt:1',
                           2,10,20,2500,25,75,2600,'unknown')""",
                [fingerprint],
            )
            attempt_id = cursor.lastrowid
            db.commit()
            stale_attempt = db.execute(
                "SELECT * FROM funding_attempts WHERE id=?", [attempt_id]
            ).fetchone()

        with self.api.get_db() as db:
            db.execute(
                "UPDATE funding_attempts SET status='processor_succeeded' WHERE id=?",
                [attempt_id],
            )
            db.commit()

        inspection = {
            "outcome": "pending",
            "processor_intent_id": "pi_pending",
            "processor_status": "processing",
            "retrieval_method": "search",
            "mismatches": [],
        }
        with self.api.get_db() as db:
            result = self.api.reconcile_funding_attempt(
                db, stale_attempt, apply=True, inspection=inspection
            )
        self.assertEqual(result["outcome"], "ignored_monotonic")
        with self.api.get_db() as db:
            self.assertEqual(
                db.execute("SELECT status FROM funding_attempts WHERE id=?", [attempt_id]).fetchone()[0],
                "processor_succeeded",
            )

    def test_stale_success_cannot_replace_different_processor_succeeded_intent(self):
        fingerprint = self._fingerprint()
        with self.api.get_db() as db:
            cursor = db.execute(
                """INSERT INTO funding_attempts
                   (operation_key,attempt_number,request_fingerprint,processor_idempotency_key,
                    employer_id,order_id,milestone_id,base_amount_cents,platform_fee_cents,
                    processing_fee_cents,charged_total_cents,status)
                   VALUES ('milestone:20',1,?,'escrow-fund:milestone:20:attempt:1',
                           2,10,20,2500,25,75,2600,'unknown')""",
                [fingerprint],
            )
            attempt_id = cursor.lastrowid
            db.commit()
            stale_attempt = db.execute(
                "SELECT * FROM funding_attempts WHERE id=?", [attempt_id]
            ).fetchone()
            db.execute(
                """UPDATE funding_attempts
                   SET status='processor_succeeded',stripe_payment_intent_id='pi_durable',
                       processor_status='succeeded' WHERE id=?""",
                [attempt_id],
            )
            db.commit()

        inspection = {
            "outcome": "succeeded",
            "processor_intent_id": "pi_stale",
            "processor_status": "succeeded",
            "retrieval_method": "retrieve",
            "mismatches": [],
        }
        with self.api.get_db() as db:
            result = self.api.reconcile_funding_attempt(
                db, stale_attempt, apply=True, inspection=inspection
            )
            attempt = db.execute(
                "SELECT * FROM funding_attempts WHERE id=?", [attempt_id]
            ).fetchone()
            hold_count = db.execute("SELECT COUNT(*) FROM escrow_holds").fetchone()[0]
        self.assertEqual(result["outcome"], "ignored_monotonic")
        self.assertEqual(result["reason"], "durable_processor_intent_conflict")
        self.assertEqual(attempt["status"], "processor_succeeded")
        self.assertEqual(attempt["stripe_payment_intent_id"], "pi_durable")
        self.assertEqual(hold_count, 0)

    def test_conflicting_reconciler_success_marks_committed_attempt_for_manual_review(self):
        def create_success(**kwargs):
            with self.api.get_db() as observer:
                prepared = observer.execute("SELECT * FROM funding_attempts").fetchone()
            return self._succeeded_intent(
                prepared["request_fingerprint"],
                pi_id="pi_original_committed",
                attempt_id=str(prepared["id"]),
                attempt_number=str(prepared["attempt_number"]),
            )

        self.create.side_effect = create_success
        with self.api.get_db() as db:
            pi_id, _ = self.api.fund_escrow_stripe(
                db, 2, 25, 10, 20, funding_identity="milestone:20"
            )
            attempt = db.execute("SELECT * FROM funding_attempts").fetchone()
        inspection = {
            "outcome": "succeeded",
            "processor_intent_id": "pi_conflicting_committed_success",
            "processor_status": "succeeded",
            "retrieval_method": "signed_webhook",
            "mismatches": [],
        }
        with self.api.get_db() as db:
            result = self.api.reconcile_funding_attempt(
                db, attempt, apply=True, inspection=inspection
            )
            durable = db.execute("SELECT * FROM funding_attempts").fetchone()
            hold = db.execute("SELECT * FROM escrow_holds").fetchone()
        self.assertEqual(result["outcome"], "ignored_committed")
        self.assertEqual(durable["status"], "committed")
        self.assertEqual(durable["stripe_payment_intent_id"], pi_id)
        self.assertEqual(durable["error_code"], "processor_intent_conflict")
        self.assertEqual(hold["stripe_payment_intent_id"], pi_id)

        with self.api.get_db() as db:
            with self.assertRaises(self.api.FundingReconciliationRequired):
                self.api.fund_escrow_stripe(
                    db, 2, 25, 10, 20, funding_identity="milestone:20"
                )
        self.create.assert_called_once()

    def test_conflicting_reconciler_success_cannot_leave_failed_attempt_retryable(self):
        fingerprint = self._fingerprint()
        with self.api.get_db() as db:
            attempt_id = db.execute(
                """INSERT INTO funding_attempts
                   (operation_key,attempt_number,request_fingerprint,processor_idempotency_key,
                    employer_id,order_id,milestone_id,base_amount_cents,platform_fee_cents,
                    processing_fee_cents,charged_total_cents,status,stripe_payment_intent_id,
                    processor_status,evidence_source)
                   VALUES ('milestone:20',1,?,'escrow-fund:milestone:20:attempt:1',
                           2,10,20,2500,25,75,2600,'failed','pi_durable_failure',
                           'requires_payment_method','signed_webhook')""",
                [fingerprint],
            ).lastrowid
            db.commit()
            stale_attempt = db.execute(
                "SELECT * FROM funding_attempts WHERE id=?", [attempt_id]
            ).fetchone()

        inspection = {
            "outcome": "succeeded",
            "processor_intent_id": "pi_conflicting_success",
            "processor_status": "succeeded",
            "retrieval_method": "search",
            "mismatches": [],
        }
        with self.api.get_db() as db:
            result = self.api.reconcile_funding_attempt(
                db, stale_attempt, apply=True, inspection=inspection
            )
            attempt = db.execute(
                "SELECT * FROM funding_attempts WHERE id=?", [attempt_id]
            ).fetchone()
        self.assertEqual(result["outcome"], "ignored_monotonic")
        self.assertEqual(attempt["status"], "failed")
        self.assertEqual(attempt["stripe_payment_intent_id"], "pi_durable_failure")
        self.assertEqual(attempt["error_code"], "processor_intent_conflict")

        with self.api.get_db() as db:
            with self.assertRaises(self.api.FundingReconciliationRequired):
                self.api.fund_escrow_stripe(
                    db, 2, 25, 10, 20, funding_identity="milestone:20"
                )
        self.create.assert_not_called()

    def test_late_success_with_intent_owned_by_newer_attempt_freezes_without_reassignment(self):
        fingerprint = self._fingerprint()
        with self.api.get_db() as db:
            first_id = db.execute(
                """INSERT INTO funding_attempts
                   (operation_key,attempt_number,request_fingerprint,processor_idempotency_key,
                    employer_id,order_id,milestone_id,base_amount_cents,platform_fee_cents,
                    processing_fee_cents,charged_total_cents,status,stripe_payment_intent_id)
                   VALUES ('milestone:20',1,?,'escrow-fund:milestone:20:attempt:1',
                           2,10,20,2500,25,75,2600,'failed',NULL)""",
                [fingerprint],
            ).lastrowid
            second_id = db.execute(
                """INSERT INTO funding_attempts
                   (operation_key,attempt_number,request_fingerprint,processor_idempotency_key,
                    employer_id,order_id,milestone_id,base_amount_cents,platform_fee_cents,
                    processing_fee_cents,charged_total_cents,status,stripe_payment_intent_id)
                   VALUES ('milestone:20',2,?,'escrow-fund:milestone:20:attempt:2',
                           2,10,20,2500,25,75,2600,'failed','pi_owned_elsewhere')""",
                [fingerprint],
            ).lastrowid
            db.commit()
            first = db.execute("SELECT * FROM funding_attempts WHERE id=?", [first_id]).fetchone()
            result = self.api.reconcile_funding_attempt(
                db,
                first,
                apply=True,
                inspection={
                    "outcome": "succeeded",
                    "processor_intent_id": "pi_owned_elsewhere",
                    "processor_status": "succeeded",
                    "retrieval_method": "signed_webhook",
                    "processor_event_id": "evt_owned_elsewhere",
                    "mismatches": [],
                },
            )
            rows = db.execute(
                """SELECT id,status,stripe_payment_intent_id,error_code
                   FROM funding_attempts ORDER BY attempt_number"""
            ).fetchall()
            evidence = db.execute(
                """SELECT attempt_id,conflict_type,incoming_intent_id,intent_owner_attempt_id
                   FROM funding_attempt_conflict_evidence ORDER BY attempt_id"""
            ).fetchall()
        self.assertEqual(result["outcome"], "ignored_monotonic")
        self.assertEqual([tuple(row) for row in rows], [
            (first_id, "failed", None, "success_conflicts_with_newer_attempt"),
            (second_id, "unknown", "pi_owned_elsewhere", "prior_attempt_success_conflict"),
        ])
        self.assertEqual([tuple(row) for row in evidence], [
            (first_id, "success_conflicts_with_newer_attempt", "pi_owned_elsewhere", second_id),
            (second_id, "prior_attempt_success_conflict", "pi_owned_elsewhere", second_id),
        ])

        with self.api.get_db() as db:
            with self.assertRaises(self.api.FundingReconciliationRequired):
                self.api.fund_escrow_stripe(
                    db, 2, 25, 10, 20, funding_identity="milestone:20"
                )
        self.create.assert_not_called()

    def test_succeeded_intent_owned_by_unrelated_attempt_freezes_before_uniqueness(self):
        target = self._insert_unknown_attempt()
        with self.api.get_db() as db:
            db.execute(
                "INSERT INTO orders (id,type,worker_id,employer_id,status,total_amount) "
                "VALUES (998,'service_order',1,2,'pending',25)"
            )
            db.execute(
                "INSERT INTO milestones (id,order_id,title,amount,sequence,status) "
                "VALUES (998,998,'Unrelated',25,1,'pending')"
            )
            owner_id = db.execute(
                """INSERT INTO funding_attempts
                   (operation_key,attempt_number,request_fingerprint,
                    processor_idempotency_key,employer_id,order_id,milestone_id,
                    base_amount_cents,platform_fee_cents,processing_fee_cents,
                    charged_total_cents,status,stripe_payment_intent_id)
                   VALUES ('milestone:998',1,?,'unrelated-owner',2,998,998,
                           2500,25,75,2600,'failed','pi_unrelated_owner')""",
                ["b" * 64],
            ).lastrowid
            db.commit()

        with self.api.get_db() as db:
            current = db.execute(
                "SELECT * FROM funding_attempts WHERE id=?", [target["id"]]
            ).fetchone()
            result = self.api.reconcile_funding_attempt(
                db,
                current,
                apply=True,
                inspection={
                    "outcome": "succeeded",
                    "processor_intent_id": "pi_unrelated_owner",
                    "processor_status": "succeeded",
                    "retrieval_method": "signed_webhook",
                    "processor_event_id": "evt_unrelated_owner",
                    "mismatches": [],
                },
            )

        self.assertEqual(result["outcome"], "ignored_monotonic")
        with self.api.get_db() as db:
            states = [tuple(row) for row in db.execute(
                """SELECT id,status,stripe_payment_intent_id,error_code
                   FROM funding_attempts WHERE id IN (?,?) ORDER BY id""",
                [target["id"], owner_id],
            )]
            evidence = [tuple(row) for row in db.execute(
                """SELECT attempt_id,conflict_type,incoming_intent_id,
                          intent_owner_attempt_id,processor_event_id
                   FROM funding_attempt_conflict_evidence ORDER BY attempt_id"""
            )]
            hold_count = db.execute(
                "SELECT COUNT(*) FROM escrow_holds WHERE funding_attempt_id=?",
                [target["id"]],
            ).fetchone()[0]
        self.assertEqual(states, [
            (target["id"], "unknown", None, "processor_intent_conflict"),
            (owner_id, "failed", "pi_unrelated_owner", "processor_intent_conflict"),
        ])
        self.assertEqual(evidence, [
            (target["id"], "processor_intent_conflict", "pi_unrelated_owner", owner_id, "evt_unrelated_owner"),
            (owner_id, "processor_intent_owner_conflict", "pi_unrelated_owner", owner_id, "evt_unrelated_owner"),
        ])
        self.assertEqual(hold_count, 0)

    def test_late_success_freeze_rolls_back_when_newer_row_cas_updates_zero(self):
        first = self._insert_unknown_attempt("pi_late_zero_cas")
        with self.api.get_db() as db:
            db.execute(
                """UPDATE funding_attempts
                   SET status='failed',error_code='card_declined',
                       error_message='declined'
                   WHERE id=?""",
                [first["id"]],
            )
            second_id = db.execute(
                """INSERT INTO funding_attempts
                   (operation_key,attempt_number,request_fingerprint,
                    processor_idempotency_key,employer_id,order_id,milestone_id,
                    base_amount_cents,platform_fee_cents,processing_fee_cents,
                    charged_total_cents,status,error_code,error_message)
                   VALUES ('milestone:20',2,?,'late-zero-cas-2',2,10,20,
                           2500,25,75,2600,'failed','card_declined','declined')""",
                [self._fingerprint()],
            ).lastrowid
            db.execute(
                f"""CREATE TRIGGER ignore_newer_late_success_freeze
                    BEFORE UPDATE ON funding_attempts
                    WHEN OLD.id={int(second_id)}
                      AND NEW.error_code='prior_attempt_success_conflict'
                    BEGIN SELECT RAISE(IGNORE); END"""
            )
            db.commit()

        with self.api.get_db() as db:
            current_first = db.execute(
                "SELECT * FROM funding_attempts WHERE id=?", [first["id"]]
            ).fetchone()
            with self.assertRaisesRegex(
                self.api.FundingReconciliationRequired,
                "Newer funding attempt changed during conflict recording",
            ):
                self.api.reconcile_funding_attempt(
                    db,
                    current_first,
                    apply=True,
                    inspection={
                        "outcome": "succeeded",
                        "processor_intent_id": "pi_late_zero_cas",
                        "processor_status": "succeeded",
                        "retrieval_method": "signed_webhook",
                        "processor_event_id": "evt_late_zero_cas",
                        "mismatches": [],
                    },
                )

        with self.api.get_db() as db:
            states = [tuple(row) for row in db.execute(
                """SELECT id,status,error_code,error_message
                   FROM funding_attempts WHERE id IN (?,?) ORDER BY id""",
                [first["id"], second_id],
            )]
            evidence_count = db.execute(
                "SELECT COUNT(*) FROM funding_attempt_conflict_evidence"
            ).fetchone()[0]
        self.assertEqual(states, [
            (first["id"], "failed", "card_declined", "declined"),
            (second_id, "failed", "card_declined", "declined"),
        ])
        self.assertEqual(evidence_count, 0)

    def test_late_success_after_newer_attempt_durably_freezes_retry(self):
        fingerprint = self._fingerprint()
        with self.api.get_db() as db:
            first_id = db.execute(
                """INSERT INTO funding_attempts
                   (operation_key,attempt_number,request_fingerprint,processor_idempotency_key,
                    employer_id,order_id,milestone_id,base_amount_cents,platform_fee_cents,
                    processing_fee_cents,charged_total_cents,status,stripe_payment_intent_id)
                   VALUES ('milestone:20',1,?,'escrow-fund:milestone:20:attempt:1',
                           2,10,20,2500,25,75,2600,'failed','pi_first_failed')""",
                [fingerprint],
            ).lastrowid
            second_id = db.execute(
                """INSERT INTO funding_attempts
                   (operation_key,attempt_number,request_fingerprint,processor_idempotency_key,
                    employer_id,order_id,milestone_id,base_amount_cents,platform_fee_cents,
                    processing_fee_cents,charged_total_cents,status,stripe_payment_intent_id)
                   VALUES ('milestone:20',2,?,'escrow-fund:milestone:20:attempt:2',
                           2,10,20,2500,25,75,2600,'failed','pi_second_failed')""",
                [fingerprint],
            ).lastrowid
            db.commit()
            first = db.execute(
                "SELECT * FROM funding_attempts WHERE id=?", [first_id]
            ).fetchone()
            result = self.api.reconcile_funding_attempt(
                db,
                first,
                apply=True,
                inspection={
                    "outcome": "succeeded",
                    "processor_intent_id": "pi_first_failed",
                    "processor_status": "succeeded",
                    "retrieval_method": "signed_webhook",
                    "mismatches": [],
                },
            )
            rows = db.execute(
                """SELECT attempt_number,status,processor_status,error_code
                   FROM funding_attempts ORDER BY attempt_number"""
            ).fetchall()
        self.assertEqual(result["outcome"], "ignored_monotonic")
        self.assertEqual([tuple(row) for row in rows], [
            (1, "failed", "succeeded", "success_conflicts_with_newer_attempt"),
            (2, "unknown", None, "prior_attempt_success_conflict"),
        ])

        with self.api.get_db() as db:
            second = db.execute(
                "SELECT * FROM funding_attempts WHERE id=?", [second_id]
            ).fetchone()
            ignored = self.api.reconcile_funding_attempt(
                db,
                second,
                apply=True,
                inspection={
                    "outcome": "failed",
                    "processor_intent_id": "pi_second_failed",
                    "processor_status": "requires_payment_method",
                    "retrieval_method": "retrieve",
                    "mismatches": [],
                },
            )
            current = db.execute(
                "SELECT status,error_code FROM funding_attempts WHERE id=?", [second_id]
            ).fetchone()
        self.assertEqual(ignored["outcome"], "ignored_monotonic")
        self.assertEqual(tuple(current), ("unknown", "prior_attempt_success_conflict"))

        failed_intent = self._succeeded_intent(
            fingerprint,
            "pi_second_failed",
            str(second_id),
            "2",
        )
        failed_intent["status"] = "requires_payment_method"
        failed_intent["amount_received"] = 0
        self.retrieve.return_value = failed_intent
        with self.api.get_db() as db:
            with self.assertRaises(self.api.FundingReconciliationRequired):
                self.api.fund_escrow_stripe(
                    db, 2, 25, 10, 20, funding_identity="milestone:20"
                )
        self.retrieve.assert_not_called()
        self.create.assert_not_called()

    def test_late_prior_success_marks_newer_committed_hold_for_manual_review(self):
        fingerprint = self._fingerprint()
        with self.api.get_db() as db:
            first_id = db.execute(
                """INSERT INTO funding_attempts
                   (operation_key,attempt_number,request_fingerprint,processor_idempotency_key,
                    employer_id,order_id,milestone_id,base_amount_cents,platform_fee_cents,
                    processing_fee_cents,charged_total_cents,status,stripe_payment_intent_id)
                   VALUES ('milestone:20',1,?,'escrow-fund:milestone:20:attempt:1',
                           2,10,20,2500,25,75,2600,'failed','pi_prior_failed')""",
                [fingerprint],
            ).lastrowid
            second_id = db.execute(
                """INSERT INTO funding_attempts
                   (operation_key,attempt_number,request_fingerprint,processor_idempotency_key,
                    employer_id,order_id,milestone_id,base_amount_cents,platform_fee_cents,
                    processing_fee_cents,charged_total_cents,status,stripe_payment_intent_id,
                    processor_status,evidence_source,committed_at)
                   VALUES ('milestone:20',2,?,'escrow-fund:milestone:20:attempt:2',
                           2,10,20,2500,25,75,2600,'committed','pi_newer_committed',
                           'succeeded','processor_create',datetime('now'))""",
                [fingerprint],
            ).lastrowid
            db.execute(
                """INSERT INTO escrow_holds
                   (order_id,milestone_id,amount,base_amount_cents,platform_fee_cents,
                    processing_fee_cents,charged_total_cents,fee_policy_version,
                    funding_identity,funding_attempt_id,status,stripe_payment_intent_id)
                   VALUES (10,20,25,2500,25,75,2600,'component-half-up-v1',
                           'milestone:20',?,'held','pi_newer_committed')""",
                [second_id],
            )
            db.commit()
            first = db.execute("SELECT * FROM funding_attempts WHERE id=?", [first_id]).fetchone()
            result = self.api.reconcile_funding_attempt(
                db,
                first,
                apply=True,
                inspection={
                    "outcome": "succeeded",
                    "processor_intent_id": "pi_prior_failed",
                    "processor_status": "succeeded",
                    "retrieval_method": "signed_webhook",
                    "mismatches": [],
                },
            )
            newer = db.execute("SELECT * FROM funding_attempts WHERE id=?", [second_id]).fetchone()
        self.assertEqual(result["outcome"], "ignored_monotonic")
        self.assertEqual(newer["status"], "committed")
        self.assertEqual(newer["error_code"], "prior_attempt_success_conflict")

        with self.api.get_db() as db:
            with self.assertRaises(self.api.FundingReconciliationRequired):
                self.api.fund_escrow_stripe(
                    db, 2, 25, 10, 20, funding_identity="milestone:20"
                )
        self.create.assert_not_called()

    def test_commit_gap_rereads_and_preserves_late_prior_success_freeze(self):
        fingerprint = self._fingerprint()
        with self.api.get_db() as db:
            first_id = db.execute(
                """INSERT INTO funding_attempts
                   (operation_key,attempt_number,request_fingerprint,processor_idempotency_key,
                    employer_id,order_id,milestone_id,base_amount_cents,platform_fee_cents,
                    processing_fee_cents,charged_total_cents,status,stripe_payment_intent_id)
                   VALUES ('milestone:20',1,?,'escrow-fund:milestone:20:attempt:1',
                           2,10,20,2500,25,75,2600,'failed','pi_prior_late')""",
                [fingerprint],
            ).lastrowid
            second_id = db.execute(
                """INSERT INTO funding_attempts
                   (operation_key,attempt_number,request_fingerprint,processor_idempotency_key,
                    employer_id,order_id,milestone_id,base_amount_cents,platform_fee_cents,
                    processing_fee_cents,charged_total_cents,status,stripe_payment_intent_id)
                   VALUES ('milestone:20',2,?,'escrow-fund:milestone:20:attempt:2',
                           2,10,20,2500,25,75,2600,'prepared',NULL)""",
                [fingerprint],
            ).lastrowid
            db.commit()
            second = db.execute("SELECT * FROM funding_attempts WHERE id=?", [second_id]).fetchone()

        original_commit = self.api._commit_funding_attempt

        def freeze_before_hold(commit_db, stale_attempt, processor_intent_id):
            with self.api.get_db() as observer:
                first = observer.execute(
                    "SELECT * FROM funding_attempts WHERE id=?", [first_id]
                ).fetchone()
                self.api.reconcile_funding_attempt(
                    observer,
                    first,
                    apply=True,
                    inspection={
                        "outcome": "succeeded",
                        "processor_intent_id": "pi_prior_late",
                        "processor_status": "succeeded",
                        "retrieval_method": "signed_webhook",
                        "mismatches": [],
                    },
                )
            return original_commit(commit_db, stale_attempt, processor_intent_id)

        with self.api.get_db() as db, mock.patch.object(
            self.api, "_commit_funding_attempt", side_effect=freeze_before_hold
        ):
            with self.assertRaises(self.api.FundingReconciliationRequired):
                self.api.reconcile_funding_attempt(
                    db,
                    second,
                    apply=True,
                    inspection={
                        "outcome": "succeeded",
                        "processor_intent_id": "pi_newer_gap",
                        "processor_status": "succeeded",
                        "retrieval_method": "search",
                        "mismatches": [],
                    },
                )

        with self.api.get_db() as db:
            rows = db.execute(
                "SELECT attempt_number,status,error_code FROM funding_attempts ORDER BY attempt_number"
            ).fetchall()
            hold_count = db.execute("SELECT COUNT(*) FROM escrow_holds").fetchone()[0]
        self.assertEqual([tuple(row) for row in rows], [
            (1, "failed", "success_conflicts_with_newer_attempt"),
            (2, "processor_succeeded", "prior_attempt_success_conflict"),
        ])
        self.assertEqual(hold_count, 0)

    def test_same_intent_failure_then_create_success_with_newer_attempt_closes_writer_lock(self):
        def create_after_failure_and_retry(**kwargs):
            with self.api.get_db() as observer:
                first = observer.execute(
                    "SELECT * FROM funding_attempts WHERE attempt_number=1"
                ).fetchone()
                failed = {
                    "id": "pi_same_intent",
                    "status": "requires_payment_method",
                    "amount": kwargs["amount"],
                    "amount_received": 0,
                    "currency": kwargs["currency"],
                    "metadata": kwargs["metadata"],
                }
                result = self.api.reconcile_funding_intent_event(observer, failed)
                self.assertEqual(result["outcome"], "failed")
                observer.execute(
                    """INSERT INTO funding_attempts
                       (operation_key,attempt_number,request_fingerprint,processor_idempotency_key,
                        employer_id,order_id,milestone_id,base_amount_cents,platform_fee_cents,
                        processing_fee_cents,charged_total_cents,currency,status)
                       VALUES (?,?,?,?,?,?,?,?,?,?,?,'usd','prepared')""",
                    [
                        first["operation_key"], 2, first["request_fingerprint"],
                        "escrow-fund:milestone:20:attempt:2", first["employer_id"],
                        first["order_id"], first["milestone_id"], first["base_amount_cents"],
                        first["platform_fee_cents"], first["processing_fee_cents"],
                        first["charged_total_cents"],
                    ],
                )
                observer.commit()
            return SimpleNamespace(
                id="pi_same_intent",
                status="succeeded",
                amount=kwargs["amount"],
                amount_received=kwargs["amount"],
                currency=kwargs["currency"],
                metadata=kwargs["metadata"],
            )

        self.create.side_effect = create_after_failure_and_retry
        route_db = self.api.get_db()
        try:
            with self.assertRaises(self.api.FundingReconciliationRequired):
                self.api.fund_escrow_stripe(
                    route_db, 2, 25, 10, 20, funding_identity="milestone:20"
                )
            self.assertFalse(route_db.in_transaction)

            contender = sqlite3.connect(self.db_path, timeout=0.1)
            try:
                contender.execute("INSERT INTO audit_log (action) VALUES ('writer_after_race')")
                contender.commit()
            finally:
                contender.close()
        finally:
            if route_db.in_transaction:
                route_db.rollback()
            route_db.close()

        with self.api.get_db() as db:
            rows = db.execute(
                """SELECT attempt_number,status,stripe_payment_intent_id,error_code
                   FROM funding_attempts ORDER BY attempt_number"""
            ).fetchall()
            hold_count = db.execute("SELECT COUNT(*) FROM escrow_holds").fetchone()[0]
        self.assertEqual([tuple(row) for row in rows], [
            (1, "failed", "pi_same_intent", "success_conflicts_with_newer_attempt"),
            (2, "unknown", None, "prior_attempt_success_conflict"),
        ])
        self.assertEqual(hold_count, 0)
        self.assertEqual(self.create.call_count, 1)

    def test_definitive_failure_allows_numbered_retry_with_new_processor_key(self):
        processor_calls = 0

        def create_with_retry(**kwargs):
            nonlocal processor_calls
            processor_calls += 1
            if processor_calls == 1:
                raise CardError("card declined", "payment_method", "card_declined")
            with self.api.get_db() as db:
                row = db.execute("SELECT * FROM funding_attempts ORDER BY attempt_number DESC").fetchone()
            return self._succeeded_intent(
                row["request_fingerprint"],
                "pi_retry",
                str(row["id"]),
                str(row["attempt_number"]),
            )

        self.create.side_effect = create_with_retry
        with self.api.get_db() as db:
            with self.assertRaises(self.api.FundingPaymentFailed):
                self.api.fund_escrow_stripe(db, 2, 25, 10, 20, funding_identity="milestone:20")
        with self.api.get_db() as db:
            result = self.api.fund_escrow_stripe(db, 2, 25, 10, 20, funding_identity="milestone:20")
        self.assertEqual(result, ("pi_retry", "live"))
        keys = [call.kwargs["idempotency_key"] for call in self.create.call_args_list]
        self.assertEqual(keys, [
            "escrow-fund:milestone:20:attempt:1",
            "escrow-fund:milestone:20:attempt:2",
        ])
        with self.api.get_db() as db:
            states = [tuple(row) for row in db.execute(
                "SELECT attempt_number,status FROM funding_attempts ORDER BY attempt_number"
            )]
        self.assertEqual(states, [(1, "failed"), (2, "committed")])

    def test_service_checkout_rejects_post_funding_milestone_drift(self):
        def create_success(**kwargs):
            return SimpleNamespace(
                id="pi_service_drift",
                status="succeeded",
                amount=kwargs["amount"],
                amount_received=kwargs["amount"],
                currency=kwargs["currency"],
                metadata=kwargs["metadata"],
            )

        real_commit = self.api._commit_funding_attempt
        drifted = False

        def commit_then_drift(db, attempt, processor_intent_id):
            nonlocal drifted
            real_commit(db, attempt, processor_intent_id)
            self.assertFalse(db.in_transaction)
            writer = sqlite3.connect(self.db_path)
            try:
                writer.execute(
                    "UPDATE milestones SET amount=99 WHERE id=?",
                    [attempt["milestone_id"]],
                )
                writer.commit()
                drifted = True
            finally:
                writer.close()

        self.create.side_effect = create_success
        with mock.patch.object(
            self.api, "_commit_funding_attempt", side_effect=commit_then_drift
        ):
            status, result = self._request({
                "idempotency_key": "service-post-funding-drift",
                "amount": "25.00",
            })

        self.assertTrue(drifted, result)
        self.assertEqual(status, 409, result)
        self.assertEqual(self.create.call_count, 1)
        with self.api.get_db() as db:
            state = db.execute(
                """SELECT o.status,m.status,m.amount,h.amount,a.base_amount_cents
                   FROM orders o
                   JOIN milestones m ON m.order_id=o.id
                   JOIN escrow_holds h ON h.order_id=o.id AND h.milestone_id=m.id
                   JOIN funding_attempts a ON a.id=h.funding_attempt_id
                   WHERE o.creation_idempotency_key=?""",
                ["service-post-funding-drift"],
            ).fetchone()
        self.assertEqual(tuple(state), ("pending", "pending", 99, 25, 2500))

    def test_legacy_service_order_without_request_fingerprint_fails_closed(self):
        key = "legacy-service-operation-1234"
        with self.api.get_db() as db:
            db.execute(
                "UPDATE orders SET service_id=1, creation_idempotency_key=? WHERE id=10",
                [key],
            )
            db.commit()
        status, result = self._request({"idempotency_key": key, "amount": "25.00"})
        self.assertEqual(status, 409, result)
        self.assertIn("request-fingerprint reconciliation", result["error"])
        self.create.assert_not_called()

    def test_existing_service_order_with_unknown_attempt_returns_reconciliation_409(self):
        key = "service-operation-12345678"
        creation_fingerprint = self.api.service_order_creation_request_fingerprint(
            2, 1, {"idempotency_key": key, "amount": "25.00"}
        )
        with self.api.get_db() as db:
            db.execute(
                """UPDATE orders
                   SET service_id=1, creation_idempotency_key=?, creation_request_fingerprint=?
                   WHERE id=10""",
                [key, creation_fingerprint],
            )
            db.commit()
        fingerprint = self.api.funding_request_fingerprint(
            "milestone:20", 2, 10, 20, self.api.buyer_charge_breakdown_cents(25)
        )
        with self.api.get_db() as db:
            db.execute(
                """INSERT INTO funding_attempts
                   (operation_key,attempt_number,request_fingerprint,processor_idempotency_key,
                    employer_id,order_id,milestone_id,base_amount_cents,platform_fee_cents,
                    processing_fee_cents,charged_total_cents,status)
                   VALUES (?,1,?,'service-attempt',2,10,20,2500,25,75,2600,'unknown')""",
                ["milestone:20", fingerprint],
            )
            db.commit()
        self.search.return_value = SimpleNamespace(data=[])
        status, result = self._request({"idempotency_key": key, "amount": "25.00"})
        self.assertEqual(status, 409, result)
        self.assertIn("reconciliation", result["error"].lower())
        self.create.assert_not_called()

    def test_recovery_cli_opens_static_snapshot_read_only(self):
        snapshot = pathlib.Path(self.tmp.name) / "report-snapshot.sqlite"
        source_db = sqlite3.connect(self.db_path)
        snapshot_db = sqlite3.connect(str(snapshot))
        try:
            source_db.backup(snapshot_db)
        finally:
            snapshot_db.close()
            source_db.close()

        os.chmod(snapshot, 0o600)
        before = snapshot.read_bytes()
        tool = os.path.join(os.path.dirname(__file__), "tools", "reconcile_funding_attempts.py")
        env = os.environ.copy()
        env.pop("STRIPE_SECRET_KEY", None)
        proc = subprocess.run(
            [sys.executable, tool, "--db", str(snapshot), "--json", "--stdout"],
            cwd=os.path.dirname(__file__), env=env, text=True, capture_output=True, timeout=30,
        )
        self.assertEqual(proc.returncode, 0, proc.stderr)
        payload = json.loads(proc.stdout)
        self.assertTrue(payload["read_only"])
        self.assertEqual(snapshot.read_bytes(), before)
        self.assertFalse(pathlib.Path(str(snapshot) + "-wal").exists())
        self.assertFalse(pathlib.Path(str(snapshot) + "-shm").exists())

    def test_recovery_cli_includes_committed_conflicts_and_structured_evidence(self):
        with self.api.get_db() as db:
            attempt_id = db.execute(
                """INSERT INTO funding_attempts
                   (operation_key,attempt_number,request_fingerprint,
                    processor_idempotency_key,employer_id,order_id,milestone_id,
                    base_amount_cents,platform_fee_cents,processing_fee_cents,
                    charged_total_cents,status,stripe_payment_intent_id,error_code,
                    error_message)
                   VALUES ('milestone:20',1,?,'cli-conflict-attempt',2,10,20,
                           2500,25,75,2600,'committed','pi_cli_A',
                           'processor_intent_conflict','manual review')""",
                [self._fingerprint()],
            ).lastrowid
            attempt = db.execute(
                "SELECT * FROM funding_attempts WHERE id=?", [attempt_id]
            ).fetchone()
            self.api._insert_funding_conflict_evidence(
                db,
                attempt_id=attempt_id,
                conflict_type="processor_intent_conflict",
                expected=attempt,
                observed=attempt,
                canonical_intent_id="pi_cli_A",
                incoming_intent_id="pi_cli_B",
                incoming_processor_status="succeeded",
                incoming_evidence_source="signed_webhook",
            )
            db.commit()

        snapshot = pathlib.Path(self.tmp.name) / "conflict-report.sqlite"
        source_db = sqlite3.connect(self.db_path)
        snapshot_db = sqlite3.connect(str(snapshot))
        try:
            source_db.backup(snapshot_db)
        finally:
            snapshot_db.close()
            source_db.close()
        os.chmod(snapshot, 0o600)
        before = snapshot.read_bytes()
        tool = os.path.join(
            os.path.dirname(__file__), "tools", "reconcile_funding_attempts.py"
        )
        env = os.environ.copy()
        env.pop("STRIPE_SECRET_KEY", None)
        report_path = pathlib.Path(self.tmp.name) / "conflict-report.json"
        proc = subprocess.run(
            [
                sys.executable, tool, "--db", str(snapshot), "--json",
                "--output", str(report_path), "--reveal-sensitive",
            ],
            cwd=os.path.dirname(__file__),
            env=env,
            text=True,
            capture_output=True,
            timeout=30,
        )
        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertEqual(proc.stdout, "")
        self.assertEqual(stat.S_IMODE(report_path.stat().st_mode), 0o600)
        payload = json.loads(report_path.read_text())
        self.assertEqual(payload["attempt_count"], 1)
        report = payload["attempts"][0]
        self.assertEqual(report["attempt_id"], attempt_id)
        self.assertEqual(report["local_status"], "committed")
        self.assertEqual(report["local_error_code"], "processor_intent_conflict")
        self.assertEqual(report["local_error_message"], "manual review")
        self.assertEqual(report["recommended_action"], "review_before_any_retry")
        self.assertEqual(
            [row["incoming_intent_id"] for row in report["conflict_evidence"]],
            ["pi_cli_B"],
        )
        self.assertEqual(snapshot.read_bytes(), before)
        self.assertFalse(pathlib.Path(str(snapshot) + "-wal").exists())
        self.assertFalse(pathlib.Path(str(snapshot) + "-shm").exists())

    def test_recovery_cli_percent_encodes_uri_metacharacters_without_filesystem_side_effects(self):
        source = pathlib.Path(self.db_path)
        requested = pathlib.Path(self.tmp.name) / "victim.db?mode=rwc&x=#frag%25.sqlite"
        requested.write_bytes(source.read_bytes())
        wal_db = sqlite3.connect(str(requested))
        try:
            self.assertEqual(wal_db.execute("PRAGMA journal_mode=WAL").fetchone()[0], "wal")
            wal_db.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        finally:
            wal_db.close()
        os.chmod(requested, 0o600)

        def directory_manifest():
            return {
                path.name: (path.stat().st_size, path.stat().st_mtime_ns, path.stat().st_mode)
                for path in pathlib.Path(self.tmp.name).iterdir()
            }

        before_manifest = directory_manifest()
        before_bytes = requested.read_bytes()
        before_stat = requested.stat()
        tool = os.path.join(os.path.dirname(__file__), "tools", "reconcile_funding_attempts.py")
        env = os.environ.copy()
        env.pop("STRIPE_SECRET_KEY", None)
        proc = subprocess.run(
            [sys.executable, tool, "--db", str(requested), "--json", "--stdout"],
            cwd=os.path.dirname(__file__), env=env, text=True, capture_output=True, timeout=30,
        )
        self.assertEqual(proc.returncode, 0, proc.stderr)
        payload = json.loads(proc.stdout)
        self.assertTrue(payload["read_only"])
        self.assertEqual(pathlib.Path(payload["database"]), requested.resolve())
        self.assertEqual(directory_manifest(), before_manifest)
        self.assertEqual(requested.read_bytes(), before_bytes)
        after_stat = requested.stat()
        self.assertEqual(after_stat.st_size, before_stat.st_size)
        self.assertEqual(after_stat.st_mtime_ns, before_stat.st_mtime_ns)
        self.assertEqual(after_stat.st_mode, before_stat.st_mode)

    def test_recovery_cli_rejects_active_sqlite_sidecars_without_mutation(self):
        source = pathlib.Path(self.db_path)
        requested = pathlib.Path(self.tmp.name) / "active.sqlite"
        requested.write_bytes(source.read_bytes())
        os.chmod(requested, 0o600)
        wal = pathlib.Path(str(requested) + "-wal")
        wal.write_bytes(b"active-sidecar-sentinel")

        def manifest():
            return {
                path.name: (path.read_bytes(), path.stat().st_mtime_ns, path.stat().st_mode)
                for path in pathlib.Path(self.tmp.name).iterdir()
                if path.is_file()
            }

        before = manifest()
        tool = pathlib.Path(__file__).parent / "tools" / "reconcile_funding_attempts.py"
        env = os.environ.copy()
        env.pop("STRIPE_SECRET_KEY", None)
        proc = subprocess.run(
            [sys.executable, str(tool), "--db", str(requested), "--json", "--stdout"],
            cwd=os.path.dirname(__file__),
            env=env,
            text=True,
            capture_output=True,
            timeout=30,
        )
        self.assertNotEqual(proc.returncode, 0)
        self.assertIn("static checkpointed SQLite snapshot is required", proc.stderr)
        self.assertEqual(manifest(), before)


if __name__ == "__main__":
    unittest.main()
