import contextlib
import io
import json
import os
import tempfile
import unittest
from types import SimpleNamespace
from unittest import mock

import stripe as stripe_sdk

from test_deep_audit_regressions import load_api_core, parse_cgi_output


class LegacyRefundReconciliationTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        os.environ["DATABASE_PATH"] = os.path.join(self.tmp.name, "legacy-refund.db")
        os.environ["DISABLE_AUTO_SEED"] = "1"
        self.api = load_api_core()
        self.api._db_path_resolved = None
        self.api.init_db()
        self.api.PRODUCTION_MODE = True
        self.api.STRIPE_AVAILABLE = True
        self.api.STRIPE_SECRET_KEY = "test"
        self.payment_retrieve = mock.Mock(side_effect=self._payment_intent)
        self.refund_list = mock.Mock(return_value={"data": [], "has_more": False})
        self.refund_retrieve = mock.Mock()
        self.refund_create = mock.Mock(side_effect=self._refund)
        self.api.stripe = SimpleNamespace(
            PaymentIntent=SimpleNamespace(retrieve=self.payment_retrieve),
            Refund=SimpleNamespace(
                list=self.refund_list,
                retrieve=self.refund_retrieve,
                create=self.refund_create,
            ),
            StripeError=stripe_sdk.StripeError,
            APIConnectionError=stripe_sdk.APIConnectionError,
            InvalidRequestError=stripe_sdk.InvalidRequestError,
            Webhook=SimpleNamespace(construct_event=mock.Mock()),
        )
        self.api.STRIPE_ERROR = stripe_sdk.StripeError
        with self.api.get_db() as db:
            db.execute(
                "INSERT INTO users(id,email,name,password_hash,is_admin) VALUES(1,'admin@x','Admin',?,1)",
                [self.api.hash_password("correct horse")],
            )
            db.execute("INSERT INTO users(id,email,name,password_hash) VALUES(2,'worker@x','Worker','x')")
            db.execute("INSERT INTO users(id,email,name,password_hash) VALUES(3,'buyer@x','Buyer','x')")
            db.execute("INSERT INTO employer_profiles(user_id,stripe_customer_id) VALUES(3,'cus_employer_3')")
            for uid, token in ((1, "admin"), (2, "worker"), (3, "buyer")):
                db.execute(
                    "INSERT INTO sessions(user_id,token,expires_at) VALUES(?,?,datetime('now','+1 day'))",
                    [uid, token],
                )
            db.execute(
                """INSERT INTO orders(
                    id,type,worker_id,employer_id,status,total_amount,
                    employer_notes,worker_notes
                ) VALUES(10,'job_hire',2,3,'in_progress',40,'initial scope',NULL)"""
            )
            db.execute(
                """INSERT INTO milestones(
                    id,order_id,title,amount,sequence,status,funded_at
                ) VALUES(30,10,'Deliverable',40,1,'in_progress',datetime('now'))"""
            )
            db.execute(
                """INSERT INTO escrow_holds(
                    id,order_id,milestone_id,amount,status,stripe_payment_intent_id
                ) VALUES(40,10,30,40,'held','pi_legacy_10')"""
            )
            db.commit()

    def tearDown(self):
        self.tmp.cleanup()
        os.environ.pop("DATABASE_PATH", None)
        os.environ.pop("DISABLE_AUTO_SEED", None)

    def _payment_intent(self, intent_id, **kwargs):
        self.assertEqual(intent_id, "pi_legacy_10")
        return {
            "id": intent_id,
            "status": "succeeded",
            "livemode": True,
            "currency": "usd",
            "customer": "cus_employer_3",
            "amount": 4160,
            "amount_received": 4160,
            "metadata": {"order_id": "10", "milestone_id": "30", "employer_id": "3"},
            "latest_charge": {
                "id": "ch_legacy_10",
                "status": "succeeded",
                "captured": True,
                "paid": True,
                "disputed": False,
                "refunded": False,
                "amount": 4160,
                "amount_refunded": 0,
                "currency": "usd",
            },
        }

    def _refund(self, **kwargs):
        evidence = {
            "id": "re_legacy_10",
            "payment_intent": kwargs["payment_intent"],
            "amount": kwargs["amount"],
            "currency": "usd",
            "metadata": kwargs["metadata"],
            "status": "succeeded",
        }
        self.refund_retrieve.return_value = evidence
        return evidence

    def request(self, method, path, token="", payload=None):
        for attr in ("body_cache", "raw_body"):
            if hasattr(self.api._request_ctx, attr):
                delattr(self.api._request_ctx, attr)
        raw = json.dumps(payload or {})
        ctx = self.api._request_ctx
        ctx.request_method = method
        ctx.path_info = path
        ctx.query_string = ""
        ctx.http_authorization = f"Bearer {token}" if token else ""
        ctx.http_x_api_key = ""
        ctx.stdin_data = raw
        ctx.content_type = "application/json"
        ctx.content_length = str(len(raw))
        ctx.remote_addr = "127.0.0.1"
        ctx.http_stripe_signature = "sig"
        with contextlib.redirect_stdout(io.StringIO()) as output:
            self.api.handle_request()
        return parse_cgi_output(output.getvalue())

    def _row_counts(self):
        with self.api.get_db() as db:
            return {
                "funding": db.execute("SELECT COUNT(*) FROM funding_attempts").fetchone()[0],
                "refund": db.execute("SELECT COUNT(*) FROM refund_attempts").fetchone()[0],
                "audit": db.execute(
                    "SELECT COUNT(*) FROM audit_log WHERE action='reconcile_legacy_refund_funding'"
                ).fetchone()[0],
            }

    def test_preflight_is_read_only_and_reports_exact_refund_eligibility(self):
        before = self._row_counts()
        status, body = self.request(
            "POST",
            "/admin/legacy-refund-preflight",
            "admin",
            {"order_ids": [10], "admin_password": "correct horse"},
        )
        self.assertEqual(status, 200, body)
        self.assertEqual(before, self._row_counts())
        self.assertEqual(
            body["orders"],
            [{
                "order_id": 10,
                "eligible": True,
                "base_amount_cents": 4000,
                "processor_charged_cents": 4160,
                "existing_refund_count": 0,
                "processor_status": "succeeded",
            }],
        )
        self.payment_retrieve.assert_called_once()
        self.refund_list.assert_called_once()
        self.refund_create.assert_not_called()

    def test_reconciliation_is_idempotent_refund_only_and_enables_exact_refund(self):
        payload = {"order_ids": [10], "admin_password": "correct horse"}
        status, body = self.request(
            "POST", "/admin/reconcile-legacy-refund-funding", "admin", payload
        )
        self.assertEqual(status, 200, body)
        status2, body2 = self.request(
            "POST", "/admin/reconcile-legacy-refund-funding", "admin", payload
        )
        self.assertEqual(status2, 200, body2)
        with self.api.get_db() as db:
            hold = db.execute("SELECT * FROM escrow_holds WHERE id=40").fetchone()
            attempt = db.execute("SELECT * FROM funding_attempts WHERE id=?", [hold["funding_attempt_id"]]).fetchone()
            self.assertEqual(db.execute("SELECT COUNT(*) FROM funding_attempts").fetchone()[0], 1)
            self.assertEqual(attempt["status"], "committed")
            self.assertEqual(attempt["error_code"], "legacy_refund_only")
            self.assertEqual(attempt["charged_total_cents"], 4160)
            self.assertEqual(hold["fee_policy_version"], "legacy-combined-four-percent-v1")
            self.api._validate_refund_hold_funding_provenance(db, hold)
            with self.assertRaisesRegex(self.api.FundingReconciliationRequired, "provenance"):
                self.api._validate_live_hold_funding_provenance(db, hold)
            self.assertEqual(
                db.execute(
                    "SELECT COUNT(*) FROM audit_log WHERE action='reconcile_legacy_refund_funding'"
                ).fetchone()[0],
                1,
            )
        freeze_status, freeze_body = self.request(
            "POST",
            "/admin/open-legacy-refund-disputes",
            "admin",
            {"order_ids": [10], "admin_password": "correct horse"},
        )
        self.assertEqual(freeze_status, 200, freeze_body)
        self.refund_create.assert_not_called()
        with self.api.get_db() as db:
            dispute = db.execute("SELECT * FROM disputes WHERE order_id=10").fetchone()
            self.assertEqual((dispute["source"], dispute["opened_by"], dispute["reason"], dispute["status"]), ("legacy", None, None, "open"))
            self.assertEqual(db.execute("SELECT status FROM orders WHERE id=10").fetchone()[0], "disputed")
        submit_status, _ = self.request(
            "POST", "/orders/10/submit", "worker", {"notes": "late delivery"}
        )
        self.assertEqual(submit_status, 409)
        with self.api.get_db() as db:
            order = db.execute("SELECT * FROM orders WHERE id=10").fetchone()
            self.assertFalse(str(order["worker_notes"] or "").strip())
            self.assertIsNone(order["submitted_at"])
        status, body = self.request(
            "POST",
            "/admin/resolve-dispute",
            "admin",
            {"order_id": 10, "resolution": "refund_to_employer", "admin_password": "correct horse"},
        )
        self.assertEqual(status, 200, body)
        self.refund_create.assert_called_once()
        self.assertEqual(self.refund_create.call_args.kwargs["amount"], 4000)
        replay_status, replay_body = self.request(
            "POST",
            "/admin/resolve-dispute",
            "admin",
            {"order_id": 10, "resolution": "refund_to_employer", "admin_password": "correct horse"},
        )
        self.assertEqual(replay_status, 200, replay_body)
        self.assertTrue(replay_body["idempotent_replay"])
        self.refund_create.assert_called_once()
        self.refund_retrieve.assert_called()
        with self.api.get_db() as db:
            self.assertEqual(db.execute("SELECT status FROM orders WHERE id=10").fetchone()[0], "canceled")
            self.assertEqual(db.execute("SELECT status FROM escrow_holds WHERE id=40").fetchone()[0], "refunded")
            self.assertEqual(db.execute("SELECT COUNT(*) FROM payout_transfers").fetchone()[0], 0)

    def test_processor_and_binding_mismatches_fail_closed_without_writes(self):
        baseline = self._payment_intent("pi_legacy_10")
        variants = {
            "intent": {**baseline, "id": "pi_other"},
            "status": {**baseline, "status": "processing"},
            "livemode": {**baseline, "livemode": False},
            "currency": {**baseline, "currency": "eur"},
            "customer": {**baseline, "customer": "cus_unrelated"},
            "amount": {**baseline, "amount_received": 4159},
            "metadata_order": {**baseline, "metadata": {**baseline["metadata"], "order_id": "11"}},
            "metadata_milestone": {**baseline, "metadata": {**baseline["metadata"], "milestone_id": "31"}},
            "metadata_employer": {**baseline, "metadata": {**baseline["metadata"], "employer_id": "4"}},
            "charge_status": {**baseline, "latest_charge": {**baseline["latest_charge"], "status": "failed"}},
            "charge_capture": {**baseline, "latest_charge": {**baseline["latest_charge"], "captured": False}},
            "charge_disputed": {**baseline, "latest_charge": {**baseline["latest_charge"], "disputed": True}},
            "charge_refunded": {**baseline, "latest_charge": {**baseline["latest_charge"], "amount_refunded": 1}},
        }
        for label, evidence in variants.items():
            with self.subTest(label=label):
                self.payment_retrieve.reset_mock(side_effect=True)
                self.payment_retrieve.side_effect = None
                self.payment_retrieve.return_value = evidence
                status, _ = self.request(
                    "POST",
                    "/admin/legacy-refund-preflight",
                    "admin",
                    {"order_ids": [10], "admin_password": "correct horse"},
                )
                self.assertEqual(status, 409)
                self.assertEqual(self._row_counts(), {"funding": 0, "refund": 0, "audit": 0})

    def test_existing_processor_refund_blocks_reconciliation(self):
        self.refund_list.return_value = {
            "data": [{"id": "re_existing", "status": "succeeded", "amount": 1}],
            "has_more": False,
        }
        status, _ = self.request(
            "POST",
            "/admin/reconcile-legacy-refund-funding",
            "admin",
            {"order_ids": [10], "admin_password": "correct horse"},
        )
        self.assertEqual(status, 409)
        self.assertEqual(self._row_counts(), {"funding": 0, "refund": 0, "audit": 0})

    def test_any_submission_note_review_status_milestone_or_escrow_change_blocks_before_processor(self):
        cases = (
            ("UPDATE orders SET worker_notes='delivery' WHERE id=10",),
            ("UPDATE orders SET submitted_at=datetime('now') WHERE id=10",),
            ("UPDATE orders SET status='submitted' WHERE id=10",),
            ("UPDATE milestones SET status='submitted' WHERE id=30",),
            ("UPDATE escrow_holds SET status='released',released_at=datetime('now') WHERE id=40",),
            ("INSERT INTO reviews(order_id,from_user_id,to_user_id,rating,text) VALUES(10,3,2,5,'done')",),
        )
        for (sql,) in cases:
            with self.subTest(sql=sql):
                with self.api.get_db() as db:
                    db.execute(sql)
                    db.commit()
                status, _ = self.request(
                    "POST",
                    "/admin/legacy-refund-preflight",
                    "admin",
                    {"order_ids": [10], "admin_password": "correct horse"},
                )
                self.assertEqual(status, 409)
                self.payment_retrieve.assert_not_called()
                self.tearDown()
                self.setUp()

    def test_atomic_dispute_freeze_aborts_all_orders_when_any_order_changed(self):
        status, body = self.request(
            "POST",
            "/admin/reconcile-legacy-refund-funding",
            "admin",
            {"order_ids": [10], "admin_password": "correct horse"},
        )
        self.assertEqual(status, 200, body)
        with self.api.get_db() as db:
            db.execute(
                """INSERT INTO orders(
                    id,type,worker_id,employer_id,status,total_amount,worker_notes,submitted_at
                ) VALUES(11,'job_hire',2,3,'submitted',25,'late delivery',datetime('now'))"""
            )
            db.commit()
        freeze_status, _ = self.request(
            "POST",
            "/admin/open-legacy-refund-disputes",
            "admin",
            {"order_ids": [10, 11], "admin_password": "correct horse"},
        )
        self.assertEqual(freeze_status, 409)
        with self.api.get_db() as db:
            self.assertEqual(db.execute("SELECT status FROM orders WHERE id=10").fetchone()[0], "in_progress")
            self.assertEqual(db.execute("SELECT COUNT(*) FROM disputes").fetchone()[0], 0)
        self.refund_create.assert_not_called()

    def test_local_state_drift_during_processor_read_prevents_reconciliation(self):
        evidence = self._payment_intent("pi_legacy_10")

        def drift(*args, **kwargs):
            with self.api.get_db() as other:
                other.execute("UPDATE orders SET submitted_at=datetime('now') WHERE id=10")
                other.commit()
            return evidence

        self.payment_retrieve.side_effect = drift
        status, _ = self.request(
            "POST",
            "/admin/reconcile-legacy-refund-funding",
            "admin",
            {"order_ids": [10], "admin_password": "correct horse"},
        )
        self.assertEqual(status, 409)
        self.assertEqual(self._row_counts(), {"funding": 0, "refund": 0, "audit": 0})


if __name__ == "__main__":
    unittest.main()
