"""Read-only, deterministic suggestions; all fixtures are local SQLite data."""
import sqlite3
import unittest
from unittest import mock

import test_job_application_withdraw as fixtures


class SuggestedApplicantsTests(unittest.TestCase):
    def setUp(self):
        # Import here so missing ranking code cannot mask the API RED assertions.
        from suggested_applicants import suggest
        self.suggest = suggest
        self.db = sqlite3.connect(':memory:')
        self.db.row_factory = sqlite3.Row
        self.db.executescript('''
            CREATE TABLE users(id INTEGER PRIMARY KEY, is_active INTEGER DEFAULT 1,
                               is_banned INTEGER DEFAULT 0, is_suspended INTEGER DEFAULT 0);
            CREATE TABLE worker_profiles(user_id INTEGER PRIMARY KEY, payout_method TEXT,
                bio TEXT, portfolio_url TEXT, total_orders_completed INTEGER DEFAULT 0,
                avg_rating REAL DEFAULT 0, total_reviews INTEGER DEFAULT 0, is_verified INTEGER DEFAULT 0);
            CREATE TABLE applications(id INTEGER PRIMARY KEY, job_id INTEGER, worker_id INTEGER,
                cover_message TEXT, portfolio_url TEXT, status TEXT, created_at TEXT);
        ''')
        self.addCleanup(self.db.close)
        self.job = dict(id=7, title='Write product descriptions',
                        description='Ecommerce catalog copywriting', budget_type='fixed', status='open')

    def add(self, aid, *, message='', portfolio='', status='pending',
            created='2026-10-01 12:00:00', payout='stripe_connect_active',
            active=1, banned=0, suspended=0, profile=True, job_id=7, **signals):
        self.db.execute('INSERT INTO users VALUES(?,?,?,?)', [aid, active, banned, suspended])
        if profile:
            fields = dict(user_id=aid, payout_method=payout, **signals)
            self.db.execute('INSERT INTO worker_profiles (' + ','.join(fields) + ') VALUES ('
                            + ','.join('?' for _ in fields) + ')', list(fields.values()))
        self.db.execute('INSERT INTO applications VALUES(?,?,?,?,?,?,?)',
                        [aid, job_id, aid, message, portfolio, status, created])
        return aid

    def suggestions(self, **kw):
        return self.suggest(self.db, self.job, kw.pop('hiring_enabled', True), **kw)

    def test_only_ready_eligible_pending_or_shortlisted_applicants(self):
        self.add(1)
        self.add(2, status='shortlisted')
        self.add(3, payout='pending_setup')
        self.add(4, payout=None)
        self.add(5, active=0)
        self.add(6, banned=1)
        self.add(7, suspended=1)
        self.add(8, status='accepted')
        self.add(9, status='rejected')
        self.add(10, profile=False)
        self.add(11, job_id=8)
        self.assertEqual(self.suggestions(), {
            1: {'rank': 1, 'reasons': ['Ready to hire']},
            2: {'rank': 2, 'reasons': ['Ready to hire']}})
        self.job['status'] = 'reviewing'
        self.assertEqual(list(self.suggestions()), [1, 2])

    def test_disabled_hourly_or_closed_job_has_no_suggestions(self):
        self.add(1)
        self.assertEqual(self.suggestions(hiring_enabled=False), {})
        self.job['budget_type'] = 'hourly'
        self.assertEqual(self.suggestions(), {})
        self.job['budget_type'] = 'fixed'
        for status in ('canceled', 'completed', 'in_progress', 'closed'):
            with self.subTest(status=status):
                self.job['status'] = status
                self.assertEqual(self.suggestions(), {})

    def test_fewer_than_three_and_default_cap(self):
        self.assertEqual(self.suggestions(), {})
        self.add(1)
        self.assertEqual(len(self.suggestions()), 1)
        for aid in (2, 3, 4):
            self.add(aid)
        self.assertEqual(list(self.suggestions()), [1, 2, 3])
        self.assertEqual(list(self.suggestions(limit=2)), [1, 2])
        self.assertEqual(self.suggestions(limit=0), {})

    def test_score_then_earliest_then_id_order_is_deterministic(self):
        self.add(40, created='2026-10-01 10:00:00')
        self.add(20, created='2026-10-01 09:00:00')
        self.add(10, created='2026-10-01 09:00:00')
        self.add(30, message='Product descriptions', created='2026-10-01 13:00:00')
        for _ in range(3):
            self.assertEqual(list(self.suggestions()), [30, 10, 20])
            self.assertEqual([v['rank'] for v in self.suggestions().values()], [1, 2, 3])

    def test_distinct_whole_job_keywords_title_and_description(self):
        self.add(1, message='product product PRODUCT')
        self.add(2, message='Ecommerce catalog')
        self.add(3, message='production descriptionsuffix')
        self.assertEqual(list(self.suggestions()), [2, 1, 3])
        self.assertEqual(self.suggestions()[2]['reasons'], ['Message addresses your task'])
        self.assertEqual(self.suggestions()[1]['reasons'], ['Ready to hire'])
        self.assertEqual(self.suggestions()[3]['reasons'], ['Ready to hire'])

    def test_stopwords_and_guided_draft_words_do_not_count(self):
        template = ('needs done type human agent needed suggested deliverable result budget preference '
                    'review before publishing please draft edit scope nothing submitted until post listing '
                    'fixed what with that this from have will your each around words')
        self.job.update(title=template + ' there about', description='')
        self.add(1, message='needs done publishing review there about')
        self.assertEqual(self.suggestions()[1]['reasons'], ['Ready to hire'])

    def test_detailed_message_uses_stripped_length_threshold(self):
        self.add(1, message=' ' * 30 + 'x' * 299 + '\n')
        self.add(2, message='\n' + 'x' * 300 + ' ')
        self.assertEqual(list(self.suggestions()), [2, 1])
        self.assertEqual(self.suggestions()[2]['reasons'], ['Detailed message'])
        self.assertEqual(self.suggestions()[1]['reasons'], ['Ready to hire'])

    def test_safe_portfolio_on_application_or_profile_counts_once(self):
        self.add(1)
        self.add(2, portfolio='HTTPS://example.test/work', portfolio_url='https://example.test/other')
        self.add(3, portfolio_url='http://example.test')
        self.assertEqual(list(self.suggestions()), [2, 3, 1])
        self.assertEqual(self.suggestions()[2]['reasons'], ['Shared a portfolio link'])
        self.assertEqual(self.suggestions()[3]['reasons'], ['Shared a portfolio link'])

    def test_unsafe_or_hostless_portfolio_not_counted(self):
        for url in ('javascript:alert(1)', 'ftp://example.test/a', 'https:///work', 'http://',
                    '/work', 'https://[broken'):
            with self.subTest(url=url):
                self.db.execute('DELETE FROM applications')
                self.db.execute('DELETE FROM worker_profiles')
                self.db.execute('DELETE FROM users')
                self.add(1, portfolio=url, portfolio_url=url)
                self.assertEqual(self.suggestions()[1]['reasons'], ['Ready to hire'])

    def test_invalid_application_portfolio_can_fall_back_to_valid_profile(self):
        self.add(1, portfolio='javascript:alert(1)', portfolio_url='https://example.test')
        self.assertEqual(self.suggestions()[1]['reasons'], ['Shared a portfolio link'])

    def test_completed_orders_reason_and_three_point_weight(self):
        self.add(1, message='x' * 300)
        self.add(2, total_orders_completed=1)
        self.add(3, total_orders_completed=12)
        result = self.suggestions()
        self.assertEqual(list(result), [2, 3, 1])
        self.assertEqual(result[2]['reasons'], ['Completed 1 order on GoHireHumans'])
        self.assertEqual(result[3]['reasons'], ['Completed 12 orders on GoHireHumans'])

    def test_rating_requires_reviews_and_four_stars(self):
        self.add(1, avg_rating=5, total_reviews=0)
        self.add(2, avg_rating=4, total_reviews=1)
        self.add(3, avg_rating=3.9, total_reviews=9)
        self.assertEqual(list(self.suggestions()), [2, 1, 3])
        self.assertEqual(self.suggestions()[2]['reasons'], ['Rated 4.0/5 (1 review)'])
        self.db.execute('UPDATE worker_profiles SET avg_rating=4.75,total_reviews=12 WHERE user_id=2')
        self.assertEqual(self.suggestions()[2]['reasons'], ['Rated 4.8/5 (12 reviews)'])

    def test_verified_profile_and_nonempty_bio_each_add_one_point(self):
        self.add(1, bio=' \n ')
        self.add(2, is_verified=1)
        self.add(3, bio='Private biography', is_verified=1)
        self.assertEqual(list(self.suggestions()), [3, 2, 1])
        self.assertEqual(self.suggestions()[3]['reasons'], ['Verified profile'])
        self.assertEqual(self.suggestions()[2]['reasons'], ['Verified profile'])
        self.db.execute('UPDATE worker_profiles SET is_verified=0 WHERE user_id=3')
        self.assertEqual(self.suggestions()[3]['reasons'], ['Ready to hire'])

    def test_weights_are_additive_and_reason_priority_is_capped(self):
        self.add(1, message='product descriptions ' + 'x' * 300,
                 portfolio='https://example.test', total_orders_completed=2,
                 avg_rating=4.5, total_reviews=3, is_verified=1, bio='Bio')
        self.add(2, message='product descriptions', total_orders_completed=2)
        self.add(3, message='x' * 300, portfolio='https://example.test', avg_rating=4.5,
                 total_reviews=3, is_verified=1)
        self.assertEqual(list(self.suggestions()), [1, 3, 2])
        self.assertEqual(self.suggestions()[1]['reasons'], [
            'Message addresses your task', 'Completed 2 orders on GoHireHumans', 'Detailed message'])

    def test_reasons_are_fixed_copy_and_score_stays_private(self):
        self.add(1, message='<script>Product descriptions https://evil.test/leak</script>',
                 portfolio='https://private.test', bio='Never echo me')
        result = self.suggestions()[1]
        self.assertEqual(set(result), {'rank', 'reasons'})
        self.assertEqual(result['reasons'], ['Message addresses your task', 'Shared a portfolio link'])
        for value in ('script', 'evil.test', 'private.test', 'Never echo me'):
            self.assertNotIn(value, str(result))

    def test_suggest_never_writes_or_opens_a_transaction(self):
        self.add(1)
        self.db.commit()
        before = self.db.total_changes
        with mock.patch('urllib.request.urlopen', side_effect=AssertionError('No network')):
            self.suggestions()
        self.assertEqual(self.db.total_changes, before)
        self.assertFalse(self.db.in_transaction)


class SuggestedApplicantsAPITests(unittest.TestCase):
    def setUp(self):
        fixtures.JobApplicationWithdrawTests.setUp(self)
        self.api.JOB_HIRING_ENABLED = True
        with self.api.get_db() as db:
            db.execute('UPDATE api_keys SET user_id=2')
            for uid in range(4, 9):
                db.execute("INSERT INTO users(id,email,name,password_hash,is_admin) VALUES(?,?,?,'x',?)",
                           [uid, f'local{uid}@example.test', f'Applicant {uid}', int(uid == 8)])
                db.execute("INSERT INTO sessions(user_id,token,expires_at) VALUES(?,?,datetime('now','+1 day'))",
                           [uid, f'tok-{uid}'])
                db.execute("INSERT INTO worker_profiles(user_id,payout_method) VALUES(?,'stripe_connect_active')", [uid])
            for aid, uid, cover, status in ((10, 1, 'Product descriptions', 'pending'),
                                           (20, 3, 'x' * 300, 'shortlisted'),
                                           (30, 4, '', 'pending'), (40, 5, '', 'pending'),
                                           (50, 6, '', 'rejected'), (60, 7, '', 'pending')):
                db.execute('''INSERT INTO applications(id,job_id,worker_id,cover_message,status,created_at)
                              VALUES(?,7,?,?,?,'2026-10-01 12:00:00')''', [aid, uid, cover, status])
            db.execute("UPDATE worker_profiles SET payout_method='pending_setup' WHERE user_id=7")
            db.commit()

    tearDown = fixtures.JobApplicationWithdrawTests.tearDown
    request = fixtures.JobApplicationWithdrawTests.request
    db_one = fixtures.JobApplicationWithdrawTests.db_one

    def assert_fields(self, apps):
        self.assertIsInstance(apps, list)
        self.assertEqual(len(apps), 6)
        expected = {10: (1, ['Message addresses your task']), 20: (2, ['Detailed message']),
                    30: (3, ['Ready to hire']), 40: (None, []), 50: (None, []), 60: (None, [])}
        for app in apps:
            self.assertIn('suggested_rank', app)
            self.assertIn('suggestion_reasons', app)
            self.assertEqual((app['suggested_rank'], app['suggestion_reasons']), expected[app['id']])
            self.assertIs(app['worker_payout_ready'], app['id'] != 60)
            self.assertNotIn('score', app)

    def domain_snapshot(self):
        # Usage accounting is pre-existing and intentionally writes for API keys.
        with self.api.get_db() as db:
            tables = [r[0] for r in db.execute("SELECT name FROM sqlite_master WHERE type='table'")
                      if r[0] not in ('api_keys', 'api_key_usage', 'sqlite_sequence')]
            return {t: [tuple(r) for r in db.execute('SELECT * FROM "' + t + '" ORDER BY rowid')]
                    for t in tables}

    def test_fields_for_every_application_and_existing_owner_audit(self):
        status, apps = self.request('GET', '/jobs/7/applications', 'tok-2')
        self.assertEqual(status, 200, apps)
        self.assert_fields(apps)
        self.assertEqual(self.db_one('SELECT last_seen_application_id FROM job_application_views')[0], 60)
        self.assertEqual(self.db_one("SELECT COUNT(*) FROM audit_log WHERE action='view_job_applications'")[0], 1)

    def test_api_key_gets_fields_without_domain_writes(self):
        before = self.domain_snapshot()
        with mock.patch('urllib.request.urlopen', side_effect=AssertionError('No network')):
            status, apps = self.request('GET', '/jobs/7/applications', api_key=self.raw_key)
        self.assertEqual(status, 200, apps)
        self.assertEqual(self.domain_snapshot(), before)
        self.assert_fields(apps)

    def test_admin_gets_same_fields_without_owner_view_writes(self):
        before = self.domain_snapshot()
        status, apps = self.request('GET', '/jobs/7/applications', 'tok-8')
        self.assertEqual(status, 200, apps)
        self.assertEqual(self.domain_snapshot(), before)
        self.assert_fields(apps)

    def test_nonowner_still_forbidden(self):
        before = self.domain_snapshot()
        status, _ = self.request('GET', '/jobs/7/applications', 'tok-1')
        self.assertEqual(status, 403)
        self.assertEqual(self.domain_snapshot(), before)

    def test_disabled_hourly_closed_api_fields_are_null_and_empty(self):
        for enabled, budget, status in ((False, 'fixed', 'open'), (True, 'hourly', 'open'),
                                        (True, 'fixed', 'canceled')):
            with self.subTest(enabled=enabled, budget=budget, status=status):
                self.api.JOB_HIRING_ENABLED = enabled
                with self.api.get_db() as db:
                    db.execute('UPDATE jobs SET budget_type=?,status=? WHERE id=7', [budget, status])
                    db.commit()
                code, apps = self.request('GET', '/jobs/7/applications', api_key=self.raw_key)
                self.assertEqual(code, 200)
                for app in apps:
                    self.assertIn('suggested_rank', app)
                    self.assertIsNone(app['suggested_rank'])
                    self.assertEqual(app['suggestion_reasons'], [])

    def test_public_applications_docs_describe_additive_fields(self):
        from pathlib import Path
        root = Path(__file__).resolve().parent.parent
        for path in ('README.md', 'frontend/api-docs.html', 'backend/mcp_server.py'):
            with self.subTest(path=path):
                text = (root / path).read_text()
                for field in ('worker_payout_ready', 'suggested_rank', 'suggestion_reasons'):
                    self.assertIn(field, text)


if __name__ == '__main__':
    unittest.main()
