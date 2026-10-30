"""Session authentication must tell an API client why a token was rejected.

Support case (2026-10-29): an AI-agent worker logged in successfully (200, token
returned) and then got a bare "Unauthorized" on every authenticated call. The full
64-character token works; the client was sending a shortened token. Nothing in the
401 said what was wrong, and the client could not self-diagnose.

Contract pinned here:
- The scheme name in "Authorization: Bearer <token>" is case-insensitive (RFC 7235).
- A 401 from session auth says why: no credential, not a Bearer header, wrong token
  length, or token unknown/expired. It never echoes the token.
- A valid session still authenticates exactly as before; API keys are unchanged.
- Login refuses a deactivated account instead of issuing a token that never works.
"""
import contextlib
import importlib.util
import io
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest import mock

MODULE_PATH = Path(__file__).with_name('api_core.py')
PASSWORD = 'correct horse battery'


def load_core():
    spec = importlib.util.spec_from_file_location('session_auth_diagnostics_core', MODULE_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class SessionAuthDiagnosticsTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.env = mock.patch.dict(os.environ, {
            'DATABASE_PATH': str(Path(self.temp.name) / 'test.db'),
            'DISABLE_AUTO_SEED': '1',
        })
        self.env.start()
        self.core = load_core()
        self.core.init_db()
        db = self.core.get_db()
        pw = self.core.hash_password(PASSWORD)
        db.executemany('INSERT INTO users (id,email,name,password_hash,is_active) VALUES (?,?,?,?,?)', [
            (1, 'agent@example.com', 'Agent', pw, 1),
            (2, 'gone@example.com', 'Deactivated', pw, 0),
        ])
        db.commit()
        db.close()

    def tearDown(self):
        self.env.stop()
        self.temp.cleanup()

    def request(self, method, path, payload=None, authorization='', api_key=''):
        c = self.core._request_ctx
        for key in ('body_cache', 'raw_body'):
            if hasattr(c, key):
                delattr(c, key)
        body = json.dumps(payload or {})
        c.request_method = method
        c.path_info = path
        c.query_string = ''
        c.http_authorization = authorization
        c.http_x_api_key = api_key
        c.stdin_data = body
        c.stdin_data_raw = body.encode()
        c.content_type = 'application/json'
        c.content_length = str(len(c.stdin_data_raw))
        c.remote_addr = '127.0.0.1'
        with contextlib.redirect_stdout(io.StringIO()) as output:
            self.core.handle_request()
        headers, _, text = output.getvalue().partition('\n\n')
        status = next((int(line.split(':', 1)[1]) for line in headers.splitlines()
                       if line.startswith('Status:')), 200)
        return status, json.loads(text or '{}')

    def login(self, email='agent@example.com'):
        status, body = self.request('POST', '/auth/login', {'email': email, 'password': PASSWORD})
        return status, body

    def test_full_token_still_authenticates(self):
        status, body = self.login()
        self.assertEqual(status, 200, body)
        token = body['token']
        self.assertEqual(len(token), 64)
        status, profile = self.request('GET', '/profile', authorization='Bearer ' + token)
        self.assertEqual(status, 200, profile)

    def test_bearer_scheme_is_case_insensitive(self):
        token = self.login()[1]['token']
        for scheme in ('bearer', 'BEARER', 'Bearer'):
            status, body = self.request('GET', '/profile', authorization=f'{scheme} {token}')
            self.assertEqual(status, 200, (scheme, body))

    def test_shortened_token_401_says_the_token_is_incomplete(self):
        token = self.login()[1]['token']
        status, body = self.request('GET', '/profile', authorization='Bearer ' + token[:32])
        self.assertEqual(status, 401)
        self.assertEqual(body.get('error'), 'Unauthorized')
        self.assertEqual(body.get('code'), 'session_token_malformed')
        self.assertIn('64', body.get('hint', ''))
        self.assertNotIn(token[:32], json.dumps(body))

    def test_unknown_token_401_says_log_in_again(self):
        status, body = self.request('GET', '/profile', authorization='Bearer ' + 'a' * 64)
        self.assertEqual(status, 401)
        self.assertEqual(body.get('code'), 'session_token_invalid')
        self.assertIn('/auth/login', body.get('hint', ''))

    def test_bare_token_401_says_to_use_bearer(self):
        token = self.login()[1]['token']
        status, body = self.request('GET', '/profile', authorization=token)
        self.assertEqual(status, 401)
        self.assertEqual(body.get('code'), 'authorization_scheme_unsupported')
        self.assertIn('Bearer', body.get('hint', ''))
        self.assertNotIn(token, json.dumps(body))

    def test_missing_credential_401_says_what_to_send(self):
        status, body = self.request('GET', '/profile')
        self.assertEqual(status, 401)
        self.assertEqual(body.get('code'), 'authentication_required')
        self.assertIn('Authorization: Bearer', body.get('hint', ''))

    def test_apply_route_gets_the_same_diagnostic(self):
        status, body = self.request('POST', '/jobs/34/apply', {}, authorization='Bearer ' + 'b' * 32)
        self.assertEqual(status, 401)
        self.assertEqual(body.get('code'), 'session_token_malformed')

    def test_deactivated_account_cannot_log_in(self):
        status, body = self.login('gone@example.com')
        self.assertEqual(status, 403, body)
        self.assertNotIn('token', body)
        db = self.core.get_db()
        try:
            self.assertEqual(db.execute('SELECT COUNT(*) FROM sessions WHERE user_id=2').fetchone()[0], 0)
        finally:
            db.close()

    def test_deactivated_account_cannot_log_in_with_google(self):
        identity = {'aud': 'google-client-under-test', 'iss': 'https://accounts.google.com',
                    'email': 'gone@example.com', 'email_verified': 'true', 'name': 'Deactivated', 'sub': 'g-sub-gone'}
        response = mock.MagicMock()
        response.__enter__.return_value.read.return_value = json.dumps(identity).encode('utf-8')
        with mock.patch.dict(os.environ, {'GOOGLE_CLIENT_ID': 'google-client-under-test'}), \
                mock.patch.object(self.core.urllib.request, 'urlopen', return_value=response), \
                mock.patch.object(self.core, 'send_welcome_email'):
            status, body = self.request('POST', '/auth/google', {'credential': 'c'})
        self.assertEqual(status, 403, body)
        self.assertNotIn('token', body)
        db = self.core.get_db()
        try:
            self.assertEqual(db.execute('SELECT COUNT(*) FROM sessions WHERE user_id=2').fetchone()[0], 0)
        finally:
            db.close()

    def test_lowercase_bearer_logout_revokes_the_session(self):
        token = self.login()[1]['token']
        status, body = self.request('POST', '/auth/logout', authorization='bearer ' + token)
        self.assertEqual(status, 200, body)
        status, body = self.request('GET', '/profile', authorization='Bearer ' + token)
        self.assertEqual(status, 401)
        self.assertEqual(body.get('code'), 'session_token_invalid')

    def test_valid_api_key_clears_a_session_failure(self):
        token = self.login()[1]['token']
        status, created = self.request('POST', '/api-keys', {'name': 'reader', 'scopes': ['read']},
                                       authorization='Bearer ' + token)
        self.assertEqual(status, 201, created)
        api_key = created['api_key']['key']
        self.assertTrue(api_key.startswith('ghh_'))
        # A broken Authorization header plus a good key authenticates via the key, with no stale diagnosis.
        status, body = self.request('GET', '/profile', authorization='Bearer ' + 'c' * 32, api_key=api_key)
        self.assertEqual(status, 200, body)
        status, body = self.request('GET', '/profile', api_key='ghh_' + 'd' * 40)
        self.assertEqual(status, 401)
        self.assertEqual(body.get('code'), 'api_key_invalid')

    def test_diagnosis_does_not_leak_into_the_next_request_on_the_same_thread(self):
        status, body = self.request('GET', '/profile', authorization='Bearer ' + 'e' * 32)
        self.assertEqual(body.get('code'), 'session_token_malformed')
        # Same thread-local context; this route returns 401 with an explicit message and must not
        # inherit the previous request's diagnosis.
        status, body = self.request('POST', '/auth/login', {'email': 'agent@example.com', 'password': 'wrong-password'})
        self.assertEqual(status, 401)
        self.assertNotIn('code', body)
        self.assertNotIn('hint', body)


if __name__ == '__main__':
    unittest.main()
