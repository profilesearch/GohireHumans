"""Audit trust regressions: review maturation, public aggregates and hourly posting."""
from datetime import datetime, timedelta, timezone

from test_audit_2026_09_fixes import AuditFixesTestCase


class ReviewTrustTests(AuditFixesTestCase):
    def setUp(self):
        super().setUp()
        self._seed_users()
        db = self.module.get_db()
        try:
            db.execute("""INSERT INTO services (id,worker_id,title,description,category,pricing_type,price)
                          VALUES (10,1,'Quality assurance','Review work','testing','fixed',12.50)""")
            db.execute("""INSERT INTO orders (id,type,service_id,worker_id,employer_id,status,total_amount,completed_at)
                          VALUES (20,'service_order',10,1,2,'completed',12.50,datetime('now','-1 day'))""")
            db.commit()
        finally:
            db.close()

    def _rating_state(self):
        db = self.module.get_db()
        try:
            return (dict(db.execute('SELECT avg_rating,total_reviews FROM worker_profiles WHERE user_id=1').fetchone()),
                    dict(db.execute('SELECT avg_rating,total_reviews FROM employer_profiles WHERE user_id=2').fetchone()),
                    dict(db.execute('SELECT avg_rating,total_reviews FROM services WHERE id=10').fetchone()))
        finally:
            db.close()

    def test_one_sided_review_hidden_day_13_and_public_day_15_without_another_post(self):
        status, review = self._request_api('POST', '/orders/20/review', {'rating': 5}, token='tok-employer')
        self.assertEqual(status, 201, review)
        self.assertEqual(review['is_visible'], 0)
        db = self.module.get_db()
        try:
            db.execute("UPDATE orders SET completed_at=datetime('now','-13 days') WHERE id=20")
            db.commit()
            self.module.reveal_mature_reviews(db)
            db.commit()
        finally:
            db.close()
        status, result = self._request_api('GET', '/users/1/reviews')
        self.assertEqual(status, 200, result)
        self.assertEqual(result['total'], 0)
        db = self.module.get_db()
        try:
            db.execute("UPDATE orders SET completed_at=datetime('now','-15 days') WHERE id=20")
            db.commit()
            self.module.reveal_mature_reviews(db)
            db.commit()
            self.module.reveal_mature_reviews(db)
            db.commit()
        finally:
            db.close()
        status, result = self._request_api('GET', '/users/1/reviews')
        self.assertEqual(status, 200, result)
        self.assertEqual(result['total'], 1)
        self.assertEqual(result['reviews'][0]['rating'], 5)
        worker, _, service = self._rating_state()
        self.assertEqual(worker, {'avg_rating': 5.0, 'total_reviews': 1})
        self.assertEqual(service, {'avg_rating': 5.0, 'total_reviews': 1})
        status, listing = self._request_api('GET', '/services')
        self.assertEqual(status, 200, listing)
        card = next(s for s in listing['services'] if s['id'] == 10)
        self.assertEqual((card['avg_rating'], card['total_reviews'], card['worker_rating'], card['worker_review_count']), (5.0, 1, 5.0, 1))

    def test_both_reviewers_reveal_immediately_and_refresh_both_recipients(self):
        self.assertEqual(self._request_api('POST', '/orders/20/review', {'rating': 5}, token='tok-employer')[0], 201)
        status, review = self._request_api('POST', '/orders/20/review', {'rating': 4}, token='tok-worker')
        self.assertEqual(status, 201, review)
        self.assertEqual(review['is_visible'], 1)
        self.assertEqual(self._request_api('GET', '/users/1/reviews')[1]['total'], 1)
        self.assertEqual(self._request_api('GET', '/users/2/reviews')[1]['total'], 1)
        worker, employer, service = self._rating_state()
        self.assertEqual(worker, {'avg_rating': 5.0, 'total_reviews': 1})
        self.assertEqual(employer, {'avg_rating': 4.0, 'total_reviews': 1})
        self.assertEqual(service, {'avg_rating': 5.0, 'total_reviews': 1})

    def test_init_backfills_only_derived_rating_columns_idempotently(self):
        db = self.module.get_db()
        try:
            db.execute("""INSERT INTO reviews (order_id,from_user_id,to_user_id,rating,is_visible,text)
                          VALUES (20,2,1,5,1,'Kept review')""")
            db.execute("UPDATE worker_profiles SET avg_rating=0,total_reviews=0 WHERE user_id=1")
            db.execute("UPDATE services SET avg_rating=0,total_reviews=0 WHERE id=10")
            db.commit()
        finally:
            db.close()
        self.module.init_db()
        self.module.init_db()
        worker, _, service = self._rating_state()
        self.assertEqual(worker, {'avg_rating': 5.0, 'total_reviews': 1})
        self.assertEqual(service, {'avg_rating': 5.0, 'total_reviews': 1})
        self.assertEqual(self._request_api('GET', '/users/1/reviews')[1]['reviews'][0]['text'], 'Kept review')


class HourlyPostingTrustTests(AuditFixesTestCase):
    def test_hourly_post_rejected_while_hiring_disabled_but_fixed_allowed(self):
        self._seed_users()
        payload = {'title': 'Verify a report', 'description': 'Check details',
                   'category': 'testing', 'budget_type': 'hourly', 'budget_amount': 12.50}
        status, result = self._request_api('POST', '/jobs', payload, token='tok-employer')
        self.assertEqual(status, 400, result)
        self.assertIn('fixed-price', result['error'].lower())
        db = self.module.get_db()
        try:
            self.assertEqual(db.execute('SELECT COUNT(*) FROM jobs').fetchone()[0], 0)
        finally:
            db.close()
        payload['budget_type'] = 'fixed'
        status, result = self._request_api('POST', '/jobs', payload, token='tok-employer')
        self.assertEqual(status, 201, result)
        self.assertEqual(result['budget_amount'], 12.50)
