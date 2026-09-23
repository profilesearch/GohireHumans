"""Legacy sample worker accounts must never be public or orderable.

Six @example.com worker accounts created on 2026-03-16 (users 12-17) were
sample data that later leaked into public browse. They join the seeded sample
list, and quote/order fail closed for any seeded sample worker so a buyer
holding an old link cannot pay a fictional seller.
"""
import unittest

from test_audit_2026_09_fixes import AuditFixesTestCase

LEGACY_SAMPLE_WORKERS = (
    "david.chen.design@example.com",
    "priya.sharma.dev@example.com",
    "tom.williams.write@example.com",
    "lisa.nguyen.va@example.com",
    "carlos.reyes.market@example.com",
    "anna.kowalski.trans@example.com",
)


class LegacySampleWorkerTests(AuditFixesTestCase):
    def setUp(self):
        super().setUp()
        self._seed_users()
        db = self.module.get_db()
        try:
            db.execute(
                "INSERT INTO services (id,worker_id,title,description,category,price,status,provider_type) "
                "VALUES (24,1,'SEO audit and 90-day growth plan','d','writing',50,'active','human')"
            )
            db.commit()
        finally:
            db.close()

    def _set_worker_email(self, email):
        db = self.module.get_db()
        try:
            db.execute("UPDATE users SET email=? WHERE id=1", [email])
            db.commit()
        finally:
            db.close()

    def _order_count(self):
        db = self.module.get_db()
        try:
            return db.execute("SELECT COUNT(*) AS c FROM orders").fetchone()["c"]
        finally:
            db.close()

    def _public_ids(self):
        status, body = self._request_api("GET", "/services")
        self.assertEqual(status, 200, body)
        return [s["id"] for s in body["services"]]

    def test_control_real_worker_service_is_public(self):
        self.assertIn(24, self._public_ids())
        self.assertEqual(self._request_api("GET", "/services/24")[0], 200)

    def test_legacy_sample_workers_are_in_the_seeded_list(self):
        for email in LEGACY_SAMPLE_WORKERS:
            with self.subTest(email=email):
                self.assertTrue(self.module.is_seeded_sample_email(email))
                self.assertTrue(self.module.is_seeded_sample_email(email.upper()))

    def test_legacy_sample_services_are_hidden_and_not_orderable(self):
        for email in LEGACY_SAMPLE_WORKERS:
            with self.subTest(email=email):
                self._set_worker_email(email)
                self.assertNotIn(24, self._public_ids())
                self.assertEqual(self._request_api("GET", "/services/24")[0], 404)
                status, _ = self._request_api("GET", "/platform/stats")
                self.assertEqual(status, 200)
                status, body = self._request_api(
                    "GET", "/services/24/quote", token="tok-employer")
                self.assertEqual(status, 404, body)
                status, body = self._request_api(
                    "POST", "/services/24/order", {"idempotency_key": f"k-{email[:8]}-0001"},
                    token="tok-employer")
                self.assertEqual(status, 404, body)
                self.assertEqual(self._order_count(), 0)

    def test_owner_still_sees_own_sample_service(self):
        self._set_worker_email(LEGACY_SAMPLE_WORKERS[0])
        status, body = self._request_api("GET", "/me/services", token="tok-worker")
        self.assertEqual(status, 200, body)
        self.assertEqual([s["id"] for s in body["services"]], [24])


if __name__ == "__main__":
    unittest.main()
