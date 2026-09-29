"""Security audit regressions against the real CGI router and a disposable SQLite DB."""
import unittest
from unittest import mock
import test_password_reset as fixture


class AuditSecurityTests(unittest.TestCase):
    setUp = fixture.PasswordResetTests.setUp
    request = fixture.PasswordResetTests.request

    def add_user(self, uid, admin=False):
        self.db.execute("INSERT INTO users (id,email,password_hash,name,is_admin) VALUES (?,?,?,?,?)",
                        [uid, f'user{uid}@example.com', self.api.hash_password('local-password'), f'Account {uid}', int(admin)])
        self.db.execute("INSERT INTO sessions (user_id,token,expires_at) VALUES (?,?,datetime('now','+1 day'))",
                        [uid, f'session-{uid}'])
        self.db.commit()

    def test_omitted_key_scopes_are_read_only_and_cannot_order_or_mutate(self):
        status, payload = self.request('/api-keys', {'name': 'Reader'}, method='POST', token='old-session')
        self.assertEqual(status, 201, payload)
        self.assertEqual(payload['api_key']['scopes'], ['read'])
        key = payload['api_key']['key']
        self.assertEqual(self.request('/profile', {}, method='GET', api_key=key)[0], 200)
        self.assertEqual(self.request('/services/1/order', {}, api_key=key)[0], 403)
        self.assertEqual(self.request('/jobs', {}, api_key=key)[0], 403)
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM orders').fetchone()[0], 0)

    def test_verifier_uses_account_name_and_rejects_ineligible_accounts(self):
        status, payload = self.request('/api-keys', {'name': 'Reader'}, token='old-session')
        self.assertEqual(status, 201, payload)
        key = payload['api_key']['key']
        status, response = self.request('/api-keys/verify', {'api_key': key})
        self.assertEqual(status, 200, response)
        self.assertEqual(response['user']['name'], 'A')
        self.assertEqual(response['key']['name'], 'Reader')
        for column, value in [('is_active', 0), ('is_banned', 1), ('is_suspended', 1)]:
            with self.subTest(column=column):
                self.db.execute('UPDATE users SET is_active=1,is_banned=0,is_suspended=0 WHERE id=1')
                self.db.execute(f'UPDATE users SET {column}=? WHERE id=1', [value])
                self.db.commit()
                self.assertEqual(self.request('/api-keys/verify', {'api_key': key})[0], 401)
                self.assertEqual(self.request('/profile', {}, method='GET', api_key=key)[0], 401)

    def test_job_edit_rejects_lifecycle_and_invalid_merged_terms_without_mutation(self):
        self.db.execute("INSERT INTO jobs (id,employer_id,title,description,category,budget_type,budget_amount) VALUES (1,1,'Job','Description',?,'fixed',25)", [self.api.VALID_CATEGORIES[0]])
        self.db.commit()
        invalid = [
            {'status': 'completed'}, {'status': 'nonsense'}, {'estimated_hours': -99},
            {'category': 'bogus'}, {'budget_type': 'bogus'}, {'location_type': 'bogus'},
            {'due_by': 'yesterday'}, {'budget_amount': '-1.00'}, {'title': ''},
        ]
        before = dict(self.db.execute('SELECT * FROM jobs WHERE id=1').fetchone())
        for body in invalid:
            with self.subTest(body=body):
                status, response = self.request('/jobs/1', body, method='PUT', token='old-session')
                self.assertEqual(status, 400, (body, response))
                self.assertEqual(dict(self.db.execute('SELECT * FROM jobs WHERE id=1').fetchone()), before)
        self.assertEqual(self.request('/jobs/1', {'description': 'Updated'}, method='PUT', token='old-session')[0], 200)

    def test_public_job_visibility_and_owner_access(self):
        self.add_user(2)
        self.db.execute("INSERT INTO jobs (id,employer_id,title,description,category,budget_amount) VALUES (1,1,'Job','Private',?,25)", [self.api.VALID_CATEGORIES[0]])
        self.db.commit()
        for status in ('canceled', 'hired', 'in_progress', 'completed'):
            with self.subTest(status=status):
                self.db.execute('UPDATE jobs SET status=? WHERE id=1', [status]); self.db.commit()
                self.assertEqual(self.request('/jobs', {}, method='GET')[1]['total'], 0)
                # Patch the router's parsed query to exercise explicit status filtering.
                with mock.patch.object(self.api, 'get_query_params', return_value={'status': status}):
                    self.assertEqual(self.request('/jobs', {}, method='GET')[0], 400)
                self.assertEqual(self.request('/jobs/1', {}, method='GET')[0], 404)
                self.assertEqual(self.request('/jobs/1', {}, method='GET', token='session-2')[0], 404)
                self.assertEqual(self.request('/jobs/1', {}, method='GET', token='old-session')[0], 200)
                self.assertEqual(self.request('/me/jobs', {}, method='GET', token='old-session')[1]['total'], 1)
                self.assertEqual(self.request('/jobs/1/apply', {}, token='session-2')[0], 409)
        self.db.execute("UPDATE jobs SET status='open' WHERE id=1"); self.db.commit()
        self.assertEqual(self.request('/jobs', {}, method='GET')[1]['total'], 1)
        for column, value in [('is_active', 0), ('is_banned', 1), ('is_suspended', 1)]:
            with self.subTest(owner=column):
                self.db.execute('UPDATE users SET is_active=1,is_banned=0,is_suspended=0 WHERE id=1')
                self.db.execute(f'UPDATE users SET {column}=? WHERE id=1', [value]); self.db.commit()
                self.assertEqual(self.request('/jobs', {}, method='GET')[1]['total'], 0)
                self.assertEqual(self.request('/jobs/1', {}, method='GET')[0], 404)
                self.assertEqual(self.request('/jobs/1/apply', {}, token='session-2')[0], 409)

    def test_ineligible_service_owner_hidden_from_browse_detail_quote_and_order(self):
        self.add_user(2)
        self.db.execute("INSERT INTO services (id,worker_id,title,description,category,price,delivery_time_days) VALUES (1,1,'Service','Details',?,25,7)", [self.api.VALID_CATEGORIES[0]])
        self.db.commit()
        self.assertEqual(self.request('/services', {}, method='GET')[1]['total'], 1)
        for column, value in [('is_active', 0), ('is_banned', 1), ('is_suspended', 1)]:
            with self.subTest(owner=column):
                self.db.execute('UPDATE users SET is_active=1,is_banned=0,is_suspended=0 WHERE id=1')
                self.db.execute(f'UPDATE users SET {column}=? WHERE id=1', [value]); self.db.commit()
                self.assertEqual(self.request('/services', {}, method='GET')[1]['total'], 0)
                self.assertEqual(self.request('/services/1', {}, method='GET')[0], 404)
                self.assertEqual(self.request('/services/1/quote', {}, method='GET', token='session-2')[0], 404)
                self.assertEqual(self.request('/services/1/order', {}, token='session-2')[0], 404)

    def test_admin_notification_link_validation_at_ingress(self):
        self.add_user(2, admin=True)
        valid = {'user_ids': [1], 'title': 'Local', 'message': 'Open', 'link': "#/jobs/x'-alert(1)-'"}
        with mock.patch.object(self.api, 'require_admin_step_up', return_value=(None, None)):
            status, body = self.request('/admin/worker-activation-notifications', valid, token='session-2')
        self.assertEqual(status, 200, body)
        self.assertEqual(self.db.execute('SELECT link FROM notifications ORDER BY id DESC LIMIT 1').fetchone()[0], valid['link'])
        for link in ('https://evil.example/', '//evil.example/', 'javascript:alert(1)', '/\\evil.example', '#/jobs\nalert(1)'):
            with self.subTest(link=link):
                status, response = self.request('/admin/worker-activation-notifications', {**valid, 'link': link}, token='session-2')
                self.assertEqual(status, 400, response)
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM notifications').fetchone()[0], 1)
        with self.assertRaises(ValueError):
            self.api.push_notification(self.db, 1, 'test', 'unsafe', link='javascript:alert(1)')


if __name__ == '__main__':
    unittest.main()
