"""Password reset route and outbox contracts; disposable database, no network."""
import contextlib
import hashlib
import io
import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest import mock
from test_deep_audit_regressions import load_api_core, parse_cgi_output


class PasswordResetTests(unittest.TestCase):
    def setUp(self):
        self.stack = contextlib.ExitStack()
        self.addCleanup(self.stack.close)
        tmp = self.stack.enter_context(tempfile.TemporaryDirectory())
        self.stack.enter_context(mock.patch.dict(os.environ, {
            'DATABASE_PATH': str(Path(tmp) / 'test.db'), 'DISABLE_AUTO_SEED': '1',
            'EMAIL_PROVIDER': 'agentmail', 'AGENTMAIL_SEND_ENABLED': 'false',
            'PASSWORD_RESET_ENCRYPTION_KEY': 'a' * 64, 'PASSWORD_RESET_RESPONSE_FLOOR_SECONDS': '0',
        }, clear=True))
        self.api = load_api_core()
        self.api._db_path_resolved = None
        self.api._seeded = False
        self.api.init_db()
        self.db = self.api.get_db()
        self.addCleanup(self.db.close)
        self.api._rate_limit_store.clear()
        self.api._login_failure_store.clear()
        self.db.execute("INSERT INTO users (id,email,password_hash,name) VALUES (1,'a@example.com',?, 'A')",
                        [self.api.hash_password('old-password')])
        self.db.execute("INSERT INTO sessions (user_id,token,expires_at) VALUES (1,'old-session',datetime('now','+1 day'))")
        self.db.execute("INSERT INTO api_keys (user_id,key_hash,key_prefix,name) VALUES (1,?,?,?)",
                        [hashlib.sha256(b'ghh_old-key').hexdigest(), 'ghh_old', 'Old'])
        self.db.commit()
        self.tokens = []
        original = self.api.secrets.token_urlsafe
        def capture(size):
            token = original(size)
            if size == 32:
                self.tokens.append(token)
            return token
        self.stack.enter_context(mock.patch.object(self.api.secrets, 'token_urlsafe', side_effect=capture))

    def request(self, path, payload, ip='127.0.0.1', method='POST', token='', api_key=''):
        ctx = self.api._request_ctx
        for attr in ('body_cache', 'raw_body'):
            if hasattr(ctx, attr):
                delattr(ctx, attr)
        raw = json.dumps(payload)
        ctx.request_method, ctx.path_info, ctx.query_string = method, path, ''
        ctx.http_authorization = 'Bearer ' + token if token else ''
        ctx.http_x_api_key = api_key
        ctx.stdin_data, ctx.stdin_data_raw = raw, raw.encode()
        ctx.content_type, ctx.content_length = 'application/json', str(len(raw))
        ctx.remote_addr = ip
        with contextlib.redirect_stdout(io.StringIO()) as output:
            self.api.handle_request()
        return parse_cgi_output(output.getvalue())

    def forgot(self, email='a@example.com', ip='127.0.0.1'):
        return self.request('/auth/forgot-password', {'email': email}, ip=ip)

    def reset(self, token, password='new-password'):
        return self.request('/auth/reset-password', {'token': token, 'new_password': password})

    def test_forgot_is_generic_and_stores_only_hash_with_encrypted_outbox(self):
        existing = self.forgot()
        absent = self.forgot('absent@example.com')
        self.assertEqual(existing, absent)
        self.assertEqual(existing[0], 200)
        self.assertEqual(len(self.tokens), 1)
        token = self.tokens[0]
        row = self.db.execute('SELECT * FROM password_reset_tokens').fetchone()
        self.assertEqual(row['token_hash'], hashlib.sha256(token.encode()).hexdigest())
        self.assertIsNone(row['used_at'])
        self.assertEqual(self.db.execute("SELECT COUNT(*) FROM transactional_email_outbox WHERE notification_type='password_reset'").fetchone()[0], 1)
        self.assertNotIn(token, Path(os.environ['DATABASE_PATH']).read_bytes().decode('latin1'))
        self.assertNotIn(token, str(self.db.execute('SELECT * FROM audit_log').fetchall()))

    def test_resend_worker_delivers_only_live_link_without_persisting_token(self):
        os.environ['EMAIL_PROVIDER'] = 'resend'
        self.forgot()
        token = self.tokens[-1]
        delivered = []
        def fake_send(to, subject, body, idempotency_key=None):
            delivered.append((to, subject, body, idempotency_key))
            return 'resend-message-id'
        with mock.patch.object(self.api, 'send_email', side_effect=fake_send):
            summary = self.api.flush_transactional_notification_emails(self.db)
        self.assertEqual(summary['sent'], 1)
        self.assertEqual(len(delivered), 1)
        self.assertEqual(delivered[0][0], 'a@example.com')
        self.assertIn('/#/reset-password?token=' + token, delivered[0][2])
        row = self.db.execute("SELECT * FROM transactional_email_outbox WHERE notification_type='password_reset'").fetchone()
        self.assertEqual(row['state'], 'sent')
        self.assertEqual(row['provider_email_id'], 'resend-message-id')
        self.assertNotIn(token, json.dumps(dict(row)))
        self.assertNotIn(token, json.dumps([dict(r) for r in self.db.execute('SELECT * FROM audit_log')]))

    def test_reset_single_use_revokes_sessions_and_api_keys(self):
        self.forgot()
        token = self.tokens[-1]
        self.assertEqual(self.reset(token)[0], 200)
        self.assertEqual(self.reset(token)[0], 400)
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM sessions WHERE user_id=1').fetchone()[0], 0)
        self.assertEqual(self.db.execute('SELECT is_active FROM api_keys WHERE user_id=1').fetchone()[0], 0)
        self.assertTrue(self.api.verify_password('new-password', self.db.execute('SELECT password_hash FROM users WHERE id=1').fetchone()[0]))
        self.assertEqual(self.request('/profile', {}, method='GET', token='old-session')[0], 401)
        self.assertEqual(self.request('/profile', {}, method='GET', api_key='ghh_old-key')[0], 401)
        self.assertNotIn(token, json.dumps([dict(r) for r in self.db.execute('SELECT * FROM audit_log')]))

    def test_previous_tokens_and_expiry(self):
        self.forgot()
        first = self.tokens[-1]
        self.forgot()
        second = self.tokens[-1]
        self.assertEqual(self.reset(first)[0], 400)
        self.db.execute("UPDATE password_reset_tokens SET expires_at=datetime('now','-1 second') WHERE token_hash=?", [hashlib.sha256(second.encode()).hexdigest()])
        self.db.commit()
        self.assertEqual(self.reset(second)[0], 400)

    def test_without_encryption_key_fails_closed_without_enumeration(self):
        os.environ.pop('PASSWORD_RESET_ENCRYPTION_KEY')
        self.assertEqual(self.forgot(), self.forgot('absent@example.com'))
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM password_reset_tokens').fetchone()[0], 0)
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM transactional_email_outbox').fetchone()[0], 0)

    def test_reset_revokes_every_other_reset_capability(self):
        self.forgot()
        original = self.tokens[-1]
        self.forgot()
        current = self.tokens[-1]
        self.assertEqual(self.reset(current)[0], 200)
        self.assertEqual(self.reset(original)[0], 400)
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM password_reset_tokens WHERE used_at IS NULL').fetchone()[0], 0)

    def test_ineligible_accounts_never_queue(self):
        for update in ('is_banned=1', 'is_active=0', 'is_suspended=1', "password_hash=''",):
            self.db.execute(f'UPDATE users SET is_banned=0,is_active=1,is_suspended=0,password_hash=? WHERE id=1', [self.api.hash_password('old-password')])
            self.db.execute(f'UPDATE users SET {update} WHERE id=1')
            self.db.commit()
            self.assertEqual(self.forgot(ip=update)[0], 200)
            self.assertEqual(self.db.execute('SELECT COUNT(*) FROM password_reset_tokens').fetchone()[0], 0)
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM transactional_email_outbox').fetchone()[0], 0)

    def test_forgot_response_time_does_not_reveal_account_existence(self):
        import statistics, time as _t
        self.api.PASSWORD_RESET_RESPONSE_FLOOR_SECONDS = 0.25
        samples = {'known': [], 'unknown': []}
        for i in range(8):
            for kind, email in (('known', 'a@example.com'), ('unknown', 'absent@example.com')):
                self.api._rate_limit_store.clear()
                start = _t.perf_counter()
                status, _ = self.forgot(email, ip=f'timing-{i}')
                samples[kind].append(_t.perf_counter() - start)
                self.assertEqual(status, 200)
        known, unknown = statistics.median(samples['known']), statistics.median(samples['unknown'])
        self.assertGreaterEqual(min(samples['known'] + samples['unknown']), 0.25)
        self.assertLess(abs(known - unknown), 0.004, samples)
        # Unknown-email requests never create tokens, outbox rows or user-linked audit rows.
        self.assertEqual(self.db.execute("SELECT COUNT(*) FROM audit_log WHERE action='password_reset_requested' AND user_id IS NULL").fetchone()[0], 8)

    def test_limits_email_and_ip_and_invalid_password(self):
        self.assertEqual(self.reset('nonsense', 'short')[0], 400)
        self.forgot()
        self.assertEqual(self.reset(self.tokens[-1], 'short')[0], 400)
        for _ in range(8):
            self.forgot(ip='rate-address')
        self.assertLessEqual(self.db.execute('SELECT COUNT(*) FROM password_reset_tokens WHERE user_id=1').fetchone()[0], 3)
        for index in range(20):
            self.forgot(f'absent-{index}@example.com', ip='one-ip')
        self.assertLessEqual(len(self.api._rate_limit_store), 100)


if __name__ == '__main__':
    unittest.main()
