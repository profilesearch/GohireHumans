"""Fixed-price job hiring release: worker payout eligibility is proven before any charge.

Processor responses are explicit offline fixtures; no network or live Stripe calls.
"""
import os
import sqlite3
import unittest
from unittest import mock

import test_transaction_lifecycle_regressions as fixtures
from test_deep_audit_regressions import load_api_core


class JobHiringReleaseTests(unittest.TestCase):
    setUp = fixtures.TransactionLifecycleRegressionTests.setUp
    tearDown = fixtures.TransactionLifecycleRegressionTests.tearDown
    _seed = fixtures.TransactionLifecycleRegressionTests._seed
    request = fixtures.TransactionLifecycleRegressionTests.request
    hire_job_one = fixtures.TransactionLifecycleRegressionTests.hire_job_one

    # ── helpers ───────────────────────────────────────────────────────────
    def set_account_retrieve(self, side_effect):
        self.account_retrieve = mock.Mock(side_effect=side_effect)
        self.api.stripe.Account = type("Account", (), {"retrieve": self.account_retrieve})

    def assert_no_hire_side_effects(self):
        self.payment_create.assert_not_called()
        with self.api.get_db() as db:
            self.assertEqual(db.execute("SELECT COUNT(*) FROM orders").fetchone()[0], 0)
            self.assertEqual(db.execute("SELECT COUNT(*) FROM milestones").fetchone()[0], 0)
            self.assertEqual(db.execute("SELECT COUNT(*) FROM funding_attempts").fetchone()[0], 0)
            self.assertEqual(db.execute("SELECT COUNT(*) FROM escrow_holds").fetchone()[0], 0)
            self.assertEqual(
                dict(db.execute("SELECT id,status FROM applications WHERE job_id=1").fetchall()),
                {14: "pending", 16: "shortlisted"},
            )
            self.assertEqual(
                db.execute(
                    "SELECT COUNT(*) FROM notifications WHERE type='job_hired'"
                ).fetchone()[0],
                0,
            )

    # ── release gate ──────────────────────────────────────────────────────
    def test_fixed_job_hiring_fails_closed_unless_explicitly_enabled(self):
        previous = os.environ.pop("GHH_JOB_HIRING_ENABLED", None)
        try:
            self.assertIs(load_api_core().JOB_HIRING_ENABLED, False)
            for value in ("", "0", "false", "no", "enable", "on"):
                os.environ["GHH_JOB_HIRING_ENABLED"] = value
                self.assertIs(load_api_core().JOB_HIRING_ENABLED, False, value)
            for value in ("1", "true", "YES"):
                os.environ["GHH_JOB_HIRING_ENABLED"] = value
                self.assertIs(load_api_core().JOB_HIRING_ENABLED, True, value)
        finally:
            os.environ.pop("GHH_JOB_HIRING_ENABLED", None)
            if previous is not None:
                os.environ["GHH_JOB_HIRING_ENABLED"] = previous

    def test_public_job_detail_mirrors_hiring_gate_for_ui(self):
        self.api.JOB_HIRING_ENABLED = True
        status, job = self.request("GET", "/jobs/1", token="")
        self.assertEqual(status, 200, job)
        self.assertIs(job["hiring_enabled"], True)
        status, hourly = self.request("GET", "/jobs/4", token="")
        self.assertEqual(status, 200, hourly)
        self.assertIs(hourly["hiring_enabled"], False)
        self.api.JOB_HIRING_ENABLED = False
        status, job = self.request("GET", "/jobs/1", token="")
        self.assertIs(job["hiring_enabled"], False)

    def test_kill_switch_blocks_new_fixed_hire_before_any_processor_call(self):
        self.api.JOB_HIRING_ENABLED = False
        status, result = self.hire_job_one()
        self.assertEqual(status, 503, result)
        self.assert_no_hire_side_effects()

    def test_hourly_hiring_stays_paused_even_when_fixed_hiring_is_enabled(self):
        status, result = self.request("POST", "/jobs/4/hire", payload={
            "application_id": 18, "weekly_hour_cap": 10,
        })
        self.assertEqual(status, 503, result)
        self.payment_create.assert_not_called()

    # ── worker payout eligibility before charge ───────────────────────────
    def test_hire_rejects_worker_without_payout_account_before_any_write_or_charge(self):
        with self.api.get_db() as db:
            db.execute("UPDATE worker_profiles SET payout_account_id=NULL,payout_method=NULL WHERE user_id=1")
            db.commit()
        self.set_account_retrieve(fixtures.ready_connect_account)
        status, result = self.hire_job_one()
        self.assertEqual(status, 409, result)
        self.assertIn("payout", result["error"].lower())
        self.account_retrieve.assert_not_called()
        self.assert_no_hire_side_effects()

    def test_hire_rejects_worker_without_worker_profile(self):
        with self.api.get_db() as db:
            db.execute("DELETE FROM worker_profiles WHERE user_id=1")
            db.commit()
        status, result = self.hire_job_one()
        self.assertEqual(status, 409, result)
        self.assert_no_hire_side_effects()

    def test_hire_rejects_unready_live_connect_account(self):
        def unready(account_id):
            account = fixtures.ready_connect_account(account_id)
            account["payouts_enabled"] = False
            return account

        self.set_account_retrieve(unready)
        status, result = self.hire_job_one()
        self.assertEqual(status, 409, result)
        self.account_retrieve.assert_called_once_with("acct_live_worker")
        self.assert_no_hire_side_effects()

    def test_hire_rejects_inactive_transfers_capability(self):
        def pending_transfers(account_id):
            account = fixtures.ready_connect_account(account_id)
            account["capabilities"] = {"transfers": "pending"}
            return account

        self.set_account_retrieve(pending_transfers)
        status, result = self.hire_job_one()
        self.assertEqual(status, 409, result)
        self.assert_no_hire_side_effects()

    def test_hire_rejects_connect_account_identity_mismatch(self):
        self.set_account_retrieve(lambda account_id: fixtures.ready_connect_account("acct_someone_else"))
        status, result = self.hire_job_one()
        self.assertEqual(status, 409, result)
        self.assert_no_hire_side_effects()

    def test_hire_fails_closed_when_connect_lookup_errors(self):
        self.set_account_retrieve(RuntimeError("processor unavailable"))
        status, result = self.hire_job_one()
        self.assertEqual(status, 409, result)
        self.assert_no_hire_side_effects()

    def test_hire_rejects_simulated_payout_account_in_production(self):
        with self.api.get_db() as db:
            db.execute("UPDATE worker_profiles SET payout_account_id='acct_sim_123' WHERE user_id=1")
            db.commit()
        self.set_account_retrieve(fixtures.ready_connect_account)
        status, result = self.hire_job_one()
        self.assertEqual(status, 409, result)
        self.account_retrieve.assert_not_called()
        self.assert_no_hire_side_effects()

    def test_hire_rejects_inactive_banned_or_suspended_worker(self):
        for column, value in (("is_active", 0), ("is_banned", 1), ("is_suspended", 1)):
            with self.subTest(column=column):
                with self.api.get_db() as db:
                    db.execute("UPDATE users SET is_active=1,is_banned=0,is_suspended=0 WHERE id=1")
                    db.execute(f"UPDATE users SET {column}=? WHERE id=1", [value])
                    db.commit()
                self.set_account_retrieve(fixtures.ready_connect_account)
                status, result = self.hire_job_one()
                self.assertEqual(status, 409, result)
                self.account_retrieve.assert_not_called()
                self.assert_no_hire_side_effects()

    # ── lock discipline and drift during the processor lookup ─────────────
    def test_payout_lookup_runs_without_sqlite_writer_lock(self):
        observed = []

        def ready_if_writer_free(account_id):
            contender = sqlite3.connect(self.api._get_db_path(), timeout=0.05)
            try:
                contender.execute("BEGIN IMMEDIATE")
                contender.rollback()
                observed.append("writer-free")
            except sqlite3.OperationalError as exc:
                observed.append(f"locked: {exc}")
            finally:
                contender.close()
            return fixtures.ready_connect_account(account_id)

        self.set_account_retrieve(ready_if_writer_free)
        status, result = self.hire_job_one()
        self.assertEqual(status, 201, result)
        self.assertEqual(observed, ["writer-free"])
        self.assertEqual(self.payment_create.call_count, 1)

    def _drift_during_lookup(self, sql, params=()):
        def drift_then_ready(account_id):
            writer = sqlite3.connect(self.api._get_db_path())
            try:
                writer.execute(sql, list(params))
                writer.commit()
            finally:
                writer.close()
            return fixtures.ready_connect_account(account_id)

        self.set_account_retrieve(drift_then_ready)
        return self.hire_job_one()

    def test_job_canceled_during_payout_lookup_blocks_charge(self):
        status, result = self._drift_during_lookup("UPDATE jobs SET status='canceled' WHERE id=1")
        self.assertEqual(status, 409, result)
        self.payment_create.assert_not_called()
        with self.api.get_db() as db:
            self.assertEqual(db.execute("SELECT COUNT(*) FROM orders").fetchone()[0], 0)
            self.assertEqual(db.execute("SELECT status FROM jobs WHERE id=1").fetchone()[0], "canceled")

    def test_budget_changed_during_payout_lookup_blocks_charge(self):
        status, result = self._drift_during_lookup("UPDATE jobs SET budget_amount=30 WHERE id=1")
        self.assertEqual(status, 409, result)
        self.payment_create.assert_not_called()

    def test_application_rejected_during_payout_lookup_blocks_charge(self):
        status, result = self._drift_during_lookup("UPDATE applications SET status='rejected' WHERE id=14")
        self.assertEqual(status, 409, result)
        self.payment_create.assert_not_called()

    def test_payout_binding_changed_during_lookup_blocks_charge(self):
        status, result = self._drift_during_lookup(
            "UPDATE worker_profiles SET payout_account_id='acct_replaced' WHERE user_id=1"
        )
        self.assertEqual(status, 409, result)
        self.payment_create.assert_not_called()

    def test_worker_banned_during_lookup_blocks_charge(self):
        status, result = self._drift_during_lookup("UPDATE users SET is_banned=1 WHERE id=1")
        self.assertEqual(status, 409, result)
        self.payment_create.assert_not_called()

    def test_new_application_during_lookup_does_not_block_hire(self):
        # open -> reviewing on another applicant's submission is benign drift.
        with self.api.get_db() as db:
            db.execute("INSERT INTO users (id,email,name,password_hash) VALUES (4,'w4@example.com','W4','x')")
            db.commit()
        status, result = self._drift_during_lookup(
            "INSERT INTO applications (id,job_id,worker_id,status) VALUES (30,1,4,'pending')"
        )
        self.assertEqual(status, 201, result)
        with self.api.get_db() as db:
            self.assertEqual(
                dict(db.execute("SELECT id,status FROM applications WHERE job_id=1").fetchall()),
                {14: "accepted", 16: "rejected", 30: "rejected"},
            )

    def test_worker_banned_after_hold_commit_does_not_activate_hire(self):
        real_commit = self.api._commit_funding_attempt

        def commit_then_ban(db, attempt, processor_intent_id):
            real_commit(db, attempt, processor_intent_id)
            writer = sqlite3.connect(self.api._get_db_path())
            try:
                writer.execute("UPDATE users SET is_banned=1 WHERE id=1")
                writer.commit()
            finally:
                writer.close()

        self.set_account_retrieve(fixtures.ready_connect_account)
        with mock.patch.object(self.api, "_commit_funding_attempt", side_effect=commit_then_ban):
            status, result = self.hire_job_one()
        self.assertEqual(status, 409, result)
        self.assertEqual(self.payment_create.call_count, 1)

        def durable_state():
            with self.api.get_db() as db:
                return (
                    db.execute("SELECT status FROM jobs WHERE id=1").fetchone()[0],
                    db.execute("SELECT status FROM applications WHERE id=14").fetchone()[0],
                    db.execute("SELECT status FROM milestones").fetchone()[0],
                    db.execute("SELECT status FROM escrow_holds").fetchone()[0],
                    db.execute("SELECT COUNT(*) FROM notifications WHERE type='job_hired'").fetchone()[0],
                )

        self.assertEqual(durable_state(), ("open", "pending", "pending", "held", 0))
        # Exact retry stays closed while the worker is ineligible, with no new charge.
        status, result = self.hire_job_one()
        self.assertEqual(status, 409, result)
        self.assertEqual(self.payment_create.call_count, 1)
        self.assertEqual(durable_state(), ("open", "pending", "pending", "held", 0))

    def _funded_hire_with_post_commit_change(self, sql):
        """Run a hire whose funding commits, then apply `sql` from another connection."""
        real_commit = self.api._commit_funding_attempt

        def commit_then_change(db, attempt, processor_intent_id):
            real_commit(db, attempt, processor_intent_id)
            writer = sqlite3.connect(self.api._get_db_path())
            try:
                writer.execute(sql)
                writer.commit()
            finally:
                writer.close()

        self.set_account_retrieve(fixtures.ready_connect_account)
        with mock.patch.object(self.api, "_commit_funding_attempt", side_effect=commit_then_change):
            return self.hire_job_one()

    def _funded_but_unactivated_state(self):
        with self.api.get_db() as db:
            return (
                db.execute("SELECT status FROM jobs WHERE id=1").fetchone()[0],
                db.execute("SELECT status FROM applications WHERE id=14").fetchone()[0],
                db.execute("SELECT status FROM milestones").fetchone()[0],
                db.execute("SELECT status FROM escrow_holds").fetchone()[0],
                db.execute("SELECT COUNT(*) FROM notifications WHERE type='job_hired'").fetchone()[0],
            )

    def test_payout_binding_removed_after_hold_commit_does_not_activate_hire(self):
        # Reviewer reproducer (PR #149 NO-GO): binding cleared after the hold commits.
        status, result = self._funded_hire_with_post_commit_change(
            "UPDATE worker_profiles SET payout_account_id=NULL,payout_method='pending_setup' WHERE user_id=1"
        )
        self.assertEqual(status, 409, result)
        self.assertEqual(self.payment_create.call_count, 1)
        self.assertEqual(self._funded_but_unactivated_state(), ("open", "pending", "pending", "held", 0))
        status, result = self.hire_job_one()
        self.assertEqual(status, 409, result)
        self.assertEqual(self.payment_create.call_count, 1)
        self.assertEqual(self._funded_but_unactivated_state(), ("open", "pending", "pending", "held", 0))

    def test_payout_binding_swapped_after_hold_commit_does_not_activate_hire(self):
        status, result = self._funded_hire_with_post_commit_change(
            "UPDATE worker_profiles SET payout_account_id='acct_swapped_in' WHERE user_id=1"
        )
        self.assertEqual(status, 409, result)
        self.assertEqual(self.payment_create.call_count, 1)
        self.assertEqual(self._funded_but_unactivated_state(), ("open", "pending", "pending", "held", 0))
        # Exact retry must not activate against the swapped account either.
        status, result = self.hire_job_one()
        self.assertEqual(status, 409, result)
        self.assertEqual(self._funded_but_unactivated_state(), ("open", "pending", "pending", "held", 0))

    def test_stranded_funded_hire_is_refundable_through_admin_dispute_path(self):
        from types import SimpleNamespace
        import stripe as stripe_sdk

        status, result = self._funded_hire_with_post_commit_change(
            "UPDATE users SET is_banned=1 WHERE id=1"
        )
        self.assertEqual(status, 409, result)
        with self.api.get_db() as db:
            order_id, total = db.execute("SELECT id,total_amount FROM orders").fetchone()
            charged = db.execute("SELECT charged_total_cents,stripe_payment_intent_id FROM escrow_holds").fetchone()
            db.execute(
                "INSERT INTO users (id,email,name,password_hash,is_admin) VALUES (9,'admin@example.com','Admin',?,1)",
                [self.api.hash_password("correct horse")],
            )
            db.execute("INSERT INTO sessions (user_id,token,expires_at) VALUES (9,'tok-admin',datetime('now','+1 day'))")
            db.commit()

        refunds = []

        def create_refund(**kwargs):
            refunds.append(kwargs)
            evidence = {
                "id": "re_stranded", "payment_intent": kwargs["payment_intent"],
                "amount": kwargs["amount"], "currency": "usd",
                "metadata": kwargs["metadata"], "status": "succeeded",
            }
            refund_api.retrieve.return_value = evidence
            return evidence

        refund_api = SimpleNamespace(
            create=mock.Mock(side_effect=create_refund), retrieve=mock.Mock(),
            list=mock.Mock(return_value={"data": []}),
        )
        self.api.stripe = SimpleNamespace(
            PaymentIntent=self.api.stripe.PaymentIntent, Account=self.api.stripe.Account,
            Refund=refund_api, Webhook=SimpleNamespace(construct_event=mock.Mock()),
            StripeError=stripe_sdk.StripeError, APIConnectionError=stripe_sdk.APIConnectionError,
            InvalidRequestError=stripe_sdk.InvalidRequestError,
        )
        self.api.STRIPE_ERROR = stripe_sdk.StripeError

        status, dispute = self.request("POST", f"/orders/{order_id}/dispute", payload={"reason": "Hire never started"})
        self.assertEqual(status, 200, dispute)
        status, resolved = self.request("POST", "/admin/resolve-dispute", token="tok-admin", payload={
            "order_id": order_id, "resolution": "refund_to_employer", "admin_password": "correct horse",
        })
        self.assertEqual(status, 200, resolved)
        self.assertEqual(resolved["status"], "succeeded")
        self.assertEqual(len(refunds), 1)
        self.assertEqual(refunds[0]["payment_intent"], charged["stripe_payment_intent_id"])
        self.assertEqual(refunds[0]["amount"], round(total * 100))
        with self.api.get_db() as db:
            self.assertEqual(db.execute("SELECT status FROM escrow_holds").fetchone()[0], "refunded")
            # Existing refund flow closes the order and its job; the buyer can repost.
            self.assertEqual(db.execute("SELECT status FROM orders").fetchone()[0], "canceled")
            self.assertEqual(db.execute("SELECT status FROM jobs WHERE id=1").fetchone()[0], "canceled")
            self.assertEqual(db.execute("SELECT status FROM milestones").fetchone()[0], "pending")
            self.assertEqual(
                db.execute("SELECT COUNT(*) FROM notifications WHERE type='job_hired'").fetchone()[0], 0
            )
        # Replaying the admin refund is idempotent: no second refund, no new charge.
        status, replay = self.request("POST", "/admin/resolve-dispute", token="tok-admin", payload={
            "order_id": order_id, "resolution": "refund_to_employer", "admin_password": "correct horse",
        })
        self.assertIn(status, (200, 409), replay)
        self.assertEqual(len(refunds), 1)
        self.assertEqual(self.payment_create.call_count, 1)

    def test_pending_admin_payout_reset_blocks_new_hire_before_charge(self):
        with self.api.get_db() as db:
            db.execute(
                """INSERT INTO payment_setup_operations
                   (operation_key,operation_kind,user_id,request_fingerprint,request_binding_json,
                    processor_idempotency_key,status)
                   VALUES ('reset:1','admin_payout_binding_reset',1,'fp','{}','reset:1:v1','unknown')"""
            )
            db.commit()
        self.set_account_retrieve(fixtures.ready_connect_account)
        status, result = self.hire_job_one()
        self.assertEqual(status, 409, result)
        self.account_retrieve.assert_not_called()
        self.assert_no_hire_side_effects()

    def test_new_hire_records_the_payout_account_proven_before_charge(self):
        self.set_account_retrieve(fixtures.ready_connect_account)
        status, result = self.hire_job_one()
        self.assertEqual(status, 201, result)
        with self.api.get_db() as db:
            self.assertEqual(
                db.execute("SELECT hire_payout_account_id FROM orders").fetchone()[0], "acct_live_worker"
            )

    # ── happy path and replay still hold with the new checks ──────────────
    def test_ready_worker_hire_funds_once_and_exact_retry_replays(self):
        self.set_account_retrieve(fixtures.ready_connect_account)
        status, result = self.hire_job_one()
        self.assertEqual(status, 201, result)
        self.assertEqual(self.payment_create.call_count, 1)
        self.assertEqual(self.payment_create.call_args.kwargs["amount"], 2600)
        status, replay = self.hire_job_one()
        self.assertEqual(status, 200, replay)
        self.assertTrue(replay["idempotent_replay"])
        self.assertEqual(self.payment_create.call_count, 1)
        # Exact replay of a committed hire must not depend on current payout readiness.
        self.set_account_retrieve(RuntimeError("processor unavailable"))
        status, replay = self.hire_job_one()
        self.assertEqual(status, 200, replay)
        self.assertEqual(self.payment_create.call_count, 1)

    # ── applicant list tells the buyer who can actually be hired ──────────
    def test_applicant_list_exposes_payout_ready_boolean_without_account_details(self):
        with self.api.get_db() as db:
            db.execute("UPDATE worker_profiles SET payout_method='stripe_connect_pending' WHERE user_id=3")
            db.commit()
        status, result = self.request("GET", "/jobs/1/applications")
        self.assertEqual(status, 200, result)
        apps = result if isinstance(result, list) else result["applications"]
        readiness = {a["id"]: a["worker_payout_ready"] for a in apps}
        self.assertEqual(readiness, {14: True, 16: False})
        for app in apps:
            self.assertNotIn("payout_account_id", app)
            self.assertNotIn("payout_method", app)
            self.assertIs(type(app["worker_payout_ready"]), bool)


if __name__ == "__main__":
    unittest.main()
