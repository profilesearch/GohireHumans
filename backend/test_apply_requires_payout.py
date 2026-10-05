"""Payout-ready application prerequisite, using only a throwaway database."""
import unittest
from unittest import mock

import test_job_application_withdraw as fixtures


class ApplyRequiresPayoutTests(unittest.TestCase):
    # Reuse the established request harness without inheriting its test cases.
    def setUp(self):
        fixtures.JobApplicationWithdrawTests.setUp(self)
        self.payout(None)
    tearDown = fixtures.JobApplicationWithdrawTests.tearDown
    request = fixtures.JobApplicationWithdrawTests.request
    apply = fixtures.JobApplicationWithdrawTests.apply
    db_one = fixtures.JobApplicationWithdrawTests.db_one

    def payout(self, method, account=None):
        with self.api.get_db() as db:
            db.execute("UPDATE worker_profiles SET payout_method=?, payout_account_id=? WHERE user_id=1",
                       [method, account])
            db.commit()

    def effects(self):
        with self.api.get_db() as db:
            return {table: [tuple(r) for r in db.execute(f"SELECT * FROM {table} ORDER BY 1")]
                    for table in ("applications", "notifications", "transactional_email_outbox",
                                  "audit_log", "jobs", "worker_profiles")}

    def assert_payout_blocked(self):
        self.api.RESEND_API_KEY = "configured-for-local-queue-only"
        before = self.effects()
        with mock.patch.object(self.api.stripe.Account, "retrieve", side_effect=AssertionError("No Stripe I/O")), \
             mock.patch.object(self.api, "send_email", side_effect=AssertionError("No email delivery")):
            status, body = self.apply()
        # Check effects independently so the RED proof also catches forbidden writes.
        self.assertEqual(self.effects(), before, "rejection must not write any domain rows")
        self.assertEqual(status, 403, body)
        self.assertEqual(body.get("code"), "payout_setup_required")
        self.assertIn("Finish payout setup before applying", body["error"])

    def test_missing_profile_rejected_without_writes(self):
        with self.api.get_db() as db:
            db.execute("DELETE FROM worker_profiles WHERE user_id=1")
            db.commit()
        self.assert_payout_blocked()

    def test_null_method_rejected_without_writes(self):
        self.payout(None)
        self.assert_payout_blocked()

    def test_pending_setup_rejected_without_writes(self):
        self.payout("pending_setup")
        self.assert_payout_blocked()

    def test_account_id_with_non_active_method_rejected_without_writes(self):
        self.payout("pending_setup", "acct_local_only")
        self.assert_payout_blocked()

    def test_account_id_with_null_method_rejected_without_writes(self):
        self.payout(None, "acct_local_only")
        self.assert_payout_blocked()

    def test_ready_worker_applies_with_existing_side_effects(self):
        self.payout("stripe_connect_active", "acct_local_only")
        self.api.RESEND_API_KEY = "configured-for-local-queue-only"
        with mock.patch.object(self.api.stripe.Account, "retrieve", side_effect=AssertionError("No Stripe I/O")), \
             mock.patch.object(self.api, "send_email", side_effect=AssertionError("Queue only")):
            status, app = self.apply()
        self.assertEqual(status, 201, app)
        self.assertEqual(self.db_one("SELECT COUNT(*) FROM applications")[0], 1)
        self.assertEqual(self.db_one("SELECT status FROM jobs WHERE id=7")[0], "reviewing")
        self.assertEqual(self.db_one("SELECT COUNT(*) FROM notifications WHERE type='new_application'")[0], 1)
        self.assertEqual(self.db_one("SELECT COUNT(*) FROM transactional_email_outbox WHERE notification_type='new_application'")[0], 1)
        self.assertEqual(self.db_one("SELECT COUNT(*) FROM audit_log WHERE action='apply_job'")[0], 1)

    def detail(self, token="tok-1"):
        status, body = self.request("GET", "/jobs/7", token)
        self.assertEqual(status, 200, body)
        return body

    def test_not_ready_detail_requires_payout_setup(self):
        for method, account in ((None, None), ("pending_setup", None), (None, "acct_local_only"),
                                ("pending_setup", "acct_local_only")):
            with self.subTest(method=method, account=account):
                self.payout(method, account)
                body = self.detail()
                self.assertFalse(body["viewer_can_apply"])
                self.assertEqual(body.get("viewer_apply_requirement"), "payout_setup")
                self.assertFalse(body["viewer_can_withdraw"])
        with self.api.get_db() as db:
            db.execute("DELETE FROM worker_profiles WHERE user_id=1")
            db.commit()
        body = self.detail()
        self.assertFalse(body["viewer_can_apply"])
        self.assertEqual(body.get("viewer_apply_requirement"), "payout_setup")

    def test_public_apply_docs_require_payout_setup(self):
        from pathlib import Path
        root = Path(__file__).resolve().parent.parent
        required = {
            "frontend/api-docs.html": "payout_setup_required",
            "README.md": "payout-ready",
            "frontend/earn/get-paid-for-human-tasks.html": "Set up payouts",
            "frontend/llms.txt": "payout setup",
            "frontend/faq.html": "Finish payout setup before applying",
            "frontend/how-it-works.html": "Finish payout setup before applying",
            "frontend/services.html": "Set up free payouts, then apply",
        }
        for path, snippet in required.items():
            with self.subTest(path=path):
                self.assertIn(snippet, (root / path).read_text())
        # API-key clients can't refresh the stored readiness themselves; say how it updates.
        self.assertIn("Payments page in the web app", (root / "frontend/api-docs.html").read_text())
        # No public page may still invite applying first, in any letter case, and
        # setup time isn't ours to promise (Stripe verification can take longer).
        stale = ("apply before connecting payouts", "you can apply to jobs before connecting payouts",
                 "no payment needed until you're hired", "takes a few minutes through stripe")
        public = [root / "README.md", *(root / "frontend").rglob("*.html"), *(root / "frontend").rglob("*.txt")]
        for path in public:
            if "node_modules" in path.parts or "tests" in path.parts:
                continue
            text = path.read_text(errors="ignore").lower()
            for phrase in stale:
                with self.subTest(path=str(path.relative_to(root)), phrase=phrase):
                    self.assertNotIn(phrase, text)
        api = (root / "backend/api_core.py").read_text().lower()
        self.assertNotIn("takes a few minutes through stripe", api)

    def test_ready_detail_has_no_requirement(self):
        self.payout("stripe_connect_active")
        body = self.detail()
        self.assertTrue(body["viewer_can_apply"])
        self.assertIsNone(body.get("viewer_apply_requirement"))

    def test_owner_detail_has_no_payout_requirement(self):
        body = self.detail("tok-2")
        self.assertFalse(body["viewer_can_apply"])
        self.assertIsNone(body.get("viewer_apply_requirement"))
        status, error = self.apply("tok-2")
        self.assertEqual((status, error), (403, {"error": "You cannot apply to your own job"}))

    def test_legacy_non_ready_application_is_preserved(self):
        with self.api.get_db() as db:
            db.execute("INSERT INTO applications(job_id,worker_id,cover_message) VALUES(7,1,'Existing work')")
            db.commit()
        before = self.effects()
        body = self.detail()
        self.assertFalse(body["viewer_can_apply"])
        self.assertIsNone(body.get("viewer_apply_requirement"))
        self.assertTrue(body["viewer_can_withdraw"])
        self.assertIsNotNone(body["viewer_application"])
        self.assertEqual(self.effects(), before)

    def test_withdrawn_twice_detail_has_no_payout_requirement(self):
        with self.api.get_db() as db:
            for _ in range(2):
                db.execute("INSERT INTO audit_log(user_id,action,entity_type,entity_id) VALUES(1,'withdraw_application','job',7)")
            db.commit()
        body = self.detail()
        self.assertFalse(body["viewer_can_apply"])
        self.assertIsNone(body.get("viewer_apply_requirement"))
        self.assertTrue(body.get("viewer_withdrawal_limit_reached"))

    def test_closed_job_has_no_payout_requirement(self):
        with self.api.get_db() as db:
            db.execute("UPDATE jobs SET status='canceled' WHERE id=7")
            db.execute("UPDATE users SET is_admin=1 WHERE id=1")
            db.commit()
        body = self.detail()
        self.assertFalse(body["viewer_can_apply"])
        self.assertIsNone(body.get("viewer_apply_requirement"))
        self.assertEqual(self.apply()[0], 409)

    def test_ineligible_employer_has_no_payout_requirement(self):
        with self.api.get_db() as db:
            db.execute("UPDATE users SET is_suspended=1 WHERE id=2")
            db.execute("UPDATE users SET is_admin=1 WHERE id=1")
            db.commit()
        body = self.detail()
        self.assertFalse(body["viewer_can_apply"])
        self.assertIsNone(body.get("viewer_apply_requirement"))
        self.assertEqual(self.apply()[0], 409)

    def test_ready_worker_withdraws_and_reapplies_once(self):
        self.payout("stripe_connect_active")
        self.assertEqual(self.apply()[0], 201)
        self.assertEqual(self.request("DELETE", "/jobs/7/apply", "tok-1")[0], 200)
        self.assertTrue(self.detail()["viewer_can_apply"])
        self.assertEqual(self.apply()[0], 201)
        self.assertEqual(self.request("DELETE", "/jobs/7/apply", "tok-1")[0], 200)
        self.assertFalse(self.detail()["viewer_can_apply"])
        self.assertEqual(self.apply()[0], 409)

    def test_anonymous_detail_has_no_requirement(self):
        body = self.detail("")
        self.assertNotIn("viewer_apply_requirement", body)
        self.assertEqual(self.apply("")[0], 401)


if __name__ == "__main__":
    unittest.main()
