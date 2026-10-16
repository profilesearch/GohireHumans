"""Analytics account class: auth/profile responses carry only a coarse label.

The site sets `account_type` as a GA4 user property so staff and AI-agent
browsing can be separated from human visitors. The label must be one of three
fixed values and must classify agent-platform domains, flagged agents and admins
exactly as the rest of the backend does.
"""
import contextlib
import io
import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import test_deep_audit_regressions as harness

ALLOWED = {'human', 'ai_agent', 'internal'}


class AnalyticsAccountTypeTests(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        env = mock.patch.dict(os.environ, {'DATABASE_PATH': str(Path(tmp.name) / 'test.db'),
                                           'DISABLE_AUTO_SEED': '1'})
        env.start()
        self.addCleanup(env.stop)
        os.environ.pop('GOOGLE_CLIENT_ID', None)
        os.environ.pop('AGENT_PLATFORM_DOMAINS', None)
        self.module = harness.load_api_core()
        self.module._db_path_resolved = None
        self.module._seeded = False
        self.module.init_db()

    def _request_api(self, method, path, payload=None, token=''):
        ctx = self.module._request_ctx
        body = json.dumps(payload or {})
        for cached in ('body_cache', 'raw_body'):
            if hasattr(ctx, cached):
                delattr(ctx, cached)
        ctx.request_method, ctx.path_info, ctx.query_string = method, path, ''
        ctx.http_authorization = f'Bearer {token}' if token else ''
        ctx.http_x_api_key = ''
        ctx.stdin_data, ctx.stdin_data_raw = body, body.encode('utf-8')
        ctx.content_type, ctx.content_length = 'application/json', str(len(body.encode('utf-8')))
        ctx.remote_addr = '127.0.0.1'
        with contextlib.redirect_stdout(io.StringIO()) as out:
            self.module.handle_request()
        return harness.parse_cgi_output(out.getvalue())

    def register(self, email, name='Tester'):
        status, body = self._request_api('POST', '/auth/register', {
            'email': email, 'password': 'long-enough-password', 'name': name})
        self.assertEqual(status, 201, body)
        return body

    def login(self, email):
        status, body = self._request_api('POST', '/auth/login', {
            'email': email, 'password': 'long-enough-password'})
        self.assertEqual(status, 200, body)
        return body

    def profile(self, token):
        status, body = self._request_api('GET', '/profile', token=token)
        self.assertEqual(status, 200, body)
        return body

    def test_human_account_is_labelled_human_everywhere(self):
        created = self.register('person@example.com')
        self.assertEqual(created['account_type'], 'human')
        logged_in = self.login('person@example.com')
        self.assertEqual(logged_in['account_type'], 'human')
        self.assertEqual(self.profile(logged_in['token'])['account_type'], 'human')

    def test_agent_platform_domain_and_subdomain_are_ai_agent(self):
        for email in ('bot-1@ilands.app', 'bot-2@team.ilands.app'):
            created = self.register(email)
            self.assertEqual(created['account_type'], 'ai_agent', email)
            logged_in = self.login(email)
            self.assertEqual(logged_in['account_type'], 'ai_agent', email)
            self.assertEqual(self.profile(logged_in['token'])['account_type'], 'ai_agent', email)

    def test_look_alike_domain_stays_human(self):
        created = self.register('someone@evil-ilands.app')
        self.assertEqual(created['account_type'], 'human')

    def test_flagged_agent_on_any_domain_is_ai_agent(self):
        created = self.register('agent@example.org')
        db = self.module.get_db()
        try:
            db.execute('UPDATE users SET is_ai_agent=1 WHERE id=?', [created['id']])
            db.commit()
        finally:
            db.close()
        self.assertEqual(self.login('agent@example.org')['account_type'], 'ai_agent')
        self.assertEqual(self.profile(created['token'])['account_type'], 'ai_agent')

    def test_admin_is_internal_even_on_agent_domain(self):
        created = self.register('ops@ilands.app')
        db = self.module.get_db()
        try:
            db.execute('UPDATE users SET is_admin=1 WHERE id=?', [created['id']])
            db.commit()
        finally:
            db.close()
        logged_in = self.login('ops@ilands.app')
        self.assertEqual(logged_in['account_type'], 'internal')
        self.assertEqual(self.profile(logged_in['token'])['account_type'], 'internal')

    def test_google_sign_in_carries_account_type(self):
        os.environ['GOOGLE_CLIENT_ID'] = 'google-client-under-test'
        identity = {'aud': 'google-client-under-test', 'iss': 'https://accounts.google.com',
                    'email': 'g-user@example.com', 'email_verified': 'true',
                    'name': 'Google User', 'sub': 'google-sub-account-type'}
        response = mock.MagicMock()
        response.__enter__.return_value.read.return_value = json.dumps(identity).encode('utf-8')
        with mock.patch.object(self.module.urllib.request, 'urlopen', return_value=response), \
                mock.patch.object(self.module, 'send_welcome_email'):
            status, body = self._request_api('POST', '/auth/google', {'credential': 'c'})
        self.assertEqual(status, 201, body)
        self.assertEqual(body['account_type'], 'human')

    def test_label_is_always_one_of_three_fixed_values(self):
        db = self.module.get_db()
        try:
            for user_id, is_admin in ((999999, 0), (999999, 1)):
                self.assertIn(self.module.analytics_account_type(db, user_id, is_admin), ALLOWED)
        finally:
            db.close()
        self.assertEqual(set(self.module.ANALYTICS_ACCOUNT_TYPES), ALLOWED)


if __name__ == '__main__':
    unittest.main()
