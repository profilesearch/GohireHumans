"""Password reset route and outbox contracts; disposable database, no network."""
import contextlib
import hashlib
import io
import json
import os
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
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
            'AGENTMAIL_API_KEY': 'offline-test-key',
            'AGENTMAIL_INBOX_ID': 'gohirehumans.operations@agentmail.to',
            'PASSWORD_RESET_EMAIL_ENABLED': 'true',
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

    def mock_agentmail(self):
        response = mock.Mock(status=200)
        message_ids = iter(range(100))
        response.read.side_effect = lambda _: json.dumps({'message_id': 'offline-' + str(next(message_ids)), 'thread_id': 'offline-thread'}).encode()
        opener = mock.Mock()
        opener.open.return_value = response
        return mock.patch.object(self.api.agentmail_transport.urllib.request, 'build_opener', return_value=opener), opener

    def test_worker_delivers_non_allowlisted_live_link_with_canary_disabled(self):
        os.environ['AGENTMAIL_RECIPIENT_ALLOWLIST'] = 'someone-else@example.com'
        os.environ['AGENTMAIL_EXPIRES_AT'] = '2026-09-18T00:00:00Z'
        self.assertEqual(self.api.agentmail_transport.config()[0], None)
        self.assertEqual(self.forgot()[0], 200)
        token = self.tokens[-1]
        patcher, opener = self.mock_agentmail()
        with patcher:
            summary = self.api.flush_transactional_notification_emails(self.db)
        self.assertEqual(summary['sent'], 1)
        self.assertEqual(opener.open.call_count, 1)
        payload = json.loads(opener.open.call_args.args[0].data)
        self.assertEqual(payload['to'], ['a@example.com'])
        self.assertEqual(payload['subject'], 'Reset your GoHireHumans password')
        self.assertIn('/#/reset-password?token=' + token, payload['text'])
        self.assertIn('30 minutes', payload['text'])
        self.assertTrue(payload['text'].endswith('GoHireHumans'))
        self.assertIs(payload['track_opens'], False)
        row = self.db.execute("SELECT * FROM transactional_email_outbox WHERE notification_type='password_reset'").fetchone()
        self.assertEqual(row['state'], 'sent')
        self.assertNotIn(token, json.dumps(dict(row)))
        self.assertNotIn(token, json.dumps([dict(r) for r in self.db.execute('SELECT * FROM audit_log')]))

    def test_new_application_stays_suppressed_with_reset_enabled(self):
        self.db.execute("INSERT INTO notifications(user_id,type,title,message,created_at) VALUES (1,'new_application','update','hello',datetime('now'))")
        notification_id = self.db.execute('SELECT MAX(id) FROM notifications').fetchone()[0]
        outbox_id = self.db.execute("""INSERT INTO transactional_email_outbox
            (user_id,notification_id,email_to,notification_type,title,message,link,dedupe_context,dedupe_key,state,created_at,expires_at)
            VALUES (1,?,'','new_application','update','hello','#/dashboard','app-test','new-app-test','pending',datetime('now'),datetime('now','+15 minutes'))""", [notification_id]).lastrowid
        self.api.agentmail_transport.enroll(self.db, outbox_id)
        self.db.commit()
        patcher, opener = self.mock_agentmail()
        with patcher:
            self.api.flush_transactional_notification_emails(self.db)
        opener.open.assert_not_called()
        self.assertEqual(self.db.execute('SELECT state FROM transactional_email_outbox WHERE id=?', [outbox_id]).fetchone()[0], 'failed')

    def test_unavailable_has_no_writes_and_throttle_still_first(self):
        os.environ['PASSWORD_RESET_EMAIL_ENABLED'] = 'false'
        self.assertEqual(self.request('/auth/password-reset/available', {}, method='GET'), (200, {'available': False}))
        before = [self.db.execute('SELECT COUNT(*) FROM ' + table).fetchone()[0]
                  for table in ('password_reset_tokens', 'transactional_email_outbox', 'audit_log')]
        self.api.PASSWORD_RESET_RESPONSE_FLOOR_SECONDS = 1
        with mock.patch.object(self.api.time, 'sleep', side_effect=AssertionError('sleep')):
            for _ in range(4):
                status, body = self.forgot()
                if _ < 3:
                    self.assertEqual(status, 503)
                    self.assertEqual(body, {'error': "Password reset by email isn't available right now. Contact gohirehumans.operations@agentmail.to for help."})
                else:
                    self.assertEqual(status, 200)
        self.assertEqual(before, [self.db.execute('SELECT COUNT(*) FROM ' + table).fetchone()[0]
                                  for table in ('password_reset_tokens', 'transactional_email_outbox', 'audit_log')])

    def test_missing_malformed_key_and_bad_reset_config_unavailable(self):
        for name, value in [('PASSWORD_RESET_ENCRYPTION_KEY', ''), ('PASSWORD_RESET_ENCRYPTION_KEY', 'bad'),
                            ('PASSWORD_RESET_DAILY_SEND_CAP', '21'), ('PASSWORD_RESET_DAILY_SEND_CAP', '0'),
                            ('APP_BASE_URL', 'https://evil.example')]:
            with self.subTest(name=name, value=value), mock.patch.dict(os.environ, {name: value}):
                self.assertEqual(self.request('/auth/password-reset/available', {}, method='GET')[1], {'available': False})
        self.assertEqual(self.request('/auth/password-reset/available', {}, method='GET')[1], {'available': True})

    def test_daily_cap_suppresses_excess_without_send(self):
        os.environ['PASSWORD_RESET_DAILY_SEND_CAP'] = '2'
        patcher, opener = self.mock_agentmail()
        with patcher:
            for index in range(3):
                self.assertEqual(self.forgot(ip='cap-' + str(index))[0], 200)
                self.api.flush_transactional_notification_emails(self.db)
        self.assertEqual(opener.open.call_count, 2)
        self.assertEqual(self.db.execute("SELECT COUNT(*) FROM agentmail_send_ledger WHERE state='accepted'").fetchone()[0], 2)

    def test_user_becomes_ineligible_after_enqueue(self):
        for column in ('is_active', 'is_banned', 'is_suspended'):
            with self.subTest(column=column):
                self.db.execute('UPDATE users SET is_active=1,is_banned=0,is_suspended=0 WHERE id=1')
                self.db.commit()
                self.forgot(ip='ineligible-' + column)
                self.db.execute('UPDATE users SET ' + column + '=? WHERE id=1', [0 if column == 'is_active' else 1])
                self.db.commit()
                patcher, opener = self.mock_agentmail()
                with patcher:
                    self.api.flush_transactional_notification_emails(self.db)
                opener.open.assert_not_called()

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

    def test_throttled_requests_write_nothing_and_skip_response_floor(self):
        import time as _t
        self.api.PASSWORD_RESET_RESPONSE_FLOOR_SECONDS = 0.3
        before = self.db.execute('SELECT COUNT(*) FROM audit_log').fetchone()[0]
        for _ in range(3):
            self.assertEqual(self.forgot('absent@example.com', ip='flood')[0], 200)
        after_allowed = self.db.execute('SELECT COUNT(*) FROM audit_log').fetchone()[0]
        self.assertEqual(after_allowed - before, 3)
        start = _t.perf_counter()
        for _ in range(20):
            status, body = self.forgot('absent@example.com', ip='flood')
            self.assertEqual(status, 200)
            self.assertIn('If an eligible account exists', body['message'])
        self.assertLess(_t.perf_counter() - start, 0.3 * 2)
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM audit_log').fetchone()[0], after_allowed)

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


    def _queue_other_type_row(self, notif_type='new_application'):
        created = self.db.execute(
            "INSERT INTO notifications(user_id,type,title,message,created_at) VALUES (1,?,'update','hello',datetime('now'))",
            [notif_type]).lastrowid
        row_id = self.db.execute("""INSERT INTO transactional_email_outbox
            (user_id,notification_id,email_to,notification_type,title,message,link,dedupe_context,dedupe_key,
             state,created_at,expires_at)
            VALUES (1,?,'',?,'update','hello','#/dashboard','ctx-'||?,'dedupe-'||?,'pending',datetime('now'),
                    datetime('now','+15 minutes'))""", [created, notif_type, notif_type, notif_type]).lastrowid
        self.db.commit()
        return row_id

    def test_reset_only_rollout_never_touches_other_notification_rows(self):
        # The general transport is off (canary disabled/expired); only the reset gate is ready.
        for provider, resend_key in (('agentmail', ''), ('resend', ''), ('resend', 'offline-resend-key')):
            with self.subTest(provider=provider, resend_key=bool(resend_key)):
                os.environ['EMAIL_PROVIDER'] = provider
                os.environ['AGENTMAIL_EXPIRES_AT'] = '2026-09-18T00:00:00Z'
                self.api.RESEND_API_KEY = resend_key
                self.db.execute("DELETE FROM transactional_email_outbox WHERE notification_type!='password_reset'")
                self.db.commit()
                row_id = self._queue_other_type_row()
                self.assertEqual(self.api.agentmail_transport.reset_config()[1], 'ready')
                self.assertIsNone(self.api.agentmail_transport.config()[0])
                if resend_key:
                    # A configured general provider keeps its existing behaviour.
                    continue
                self.db.execute('DELETE FROM notification_worker_leases')
                self.db.commit()
                self.assertTrue(self.api.acquire_notification_worker_lease(self.db, 'probe'))
                response = mock.Mock(status=200)
                response.read.return_value = b'{"id":"resend-id","message_id":"m","thread_id":"t"}'
                with mock.patch.object(self.api.urllib.request, 'urlopen', return_value=response) as resend, \
                        mock.patch.object(self.api.agentmail_transport.urllib.request, 'build_opener') as agent:
                    self.api.run_notification_maintenance_once(owner_token='probe')
                    # Even after the other row's validity window, reset-only mode must not expire it.
                    later = datetime.now(timezone.utc) + timedelta(minutes=30)
                    self.api.flush_transactional_notification_emails(self.db, now=later, only_types=('password_reset',))
                row = self.db.execute('SELECT state,attempts,delivery_status FROM transactional_email_outbox WHERE id=?',
                                      [row_id]).fetchone()
                self.assertEqual((row['state'], row['attempts']), ('pending', 0))
                self.assertIsNone(row['delivery_status'])
                self.assertEqual(resend.call_count, 0)
                self.assertEqual(agent.call_count, 0)

    def test_admin_password_rotation_revokes_issued_reset_links(self):
        self.db.execute('UPDATE users SET is_admin=1 WHERE id=1')
        self.db.commit()
        self.assertEqual(self.forgot()[0], 200)
        token = self.tokens[-1]
        with mock.patch.object(self.api, 'require_admin_step_up', return_value=(None, None)):
            status, _ = self.request('/admin/users/1/password', {'password': 'admin-rotated-password'},
                                     method='PUT', token='old-session')
        self.assertEqual(status, 200)
        used = self.db.execute('SELECT used_at FROM password_reset_tokens WHERE token_hash=?',
                               [hashlib.sha256(token.encode()).hexdigest()]).fetchone()['used_at']
        self.assertIsNotNone(used)
        pending = self.db.execute("""SELECT COUNT(*) FROM transactional_email_outbox
            WHERE notification_type='password_reset' AND state='pending'""").fetchone()[0]
        self.assertEqual(pending, 0)
        opener = mock.Mock()
        with mock.patch.object(self.api.agentmail_transport.urllib.request, 'build_opener', return_value=opener):
            self.api.flush_transactional_notification_emails(self.db)
        self.assertEqual(opener.open.call_count, 0)
        before = self.db.execute('SELECT password_hash FROM users WHERE id=1').fetchone()['password_hash']
        self.assertEqual(self.reset(token, 'attacker-new-password')[0], 400)
        after = self.db.execute('SELECT password_hash FROM users WHERE id=1').fetchone()['password_hash']
        self.assertEqual(before, after)


if __name__ == '__main__':
    unittest.main()
