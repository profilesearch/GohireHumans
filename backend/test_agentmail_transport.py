"""Offline transport contract: temporary SQLite and mocked HTTP, never a provider.

AGENTMAIL_TOTAL_SEND_CAP defaults to 1; accepted AND ambiguous prepared intents
consume the lifetime budget. Explicit overrides must be decimal integers 1..100.
Integration with api_core is covered separately by the integration test module.
"""
import json
import os
import sqlite3
import tempfile
import threading
import unittest
import urllib.error
from datetime import datetime, timedelta, timezone
from unittest import mock

try:
    from . import agentmail_transport as transport
except ImportError:
    import agentmail_transport as transport


class Clock(datetime):
    current = datetime(2026, 9, 5, 12, tzinfo=timezone.utc)

    @classmethod
    def now(cls, tz=None):
        return cls.current if tz else cls.current.replace(tzinfo=None)


class AgentMailTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.path = self.tmp.name + '/mail.db'
        self.db = self.connect()
        self.addCleanup(lambda: self.db.close())
        self.env = mock.patch.dict(os.environ, {}, clear=True)
        self.env.start()
        self.addCleanup(self.env.stop)
        Clock.current = datetime(2026, 9, 5, 12, tzinfo=timezone.utc)
        self.clock = mock.patch.object(transport, 'datetime', Clock)
        self.clock.start()
        self.addCleanup(self.clock.stop)
        # Deny accidental network calls even if a test forgets a provider mock.
        self.network = mock.patch('urllib.request.OpenerDirector.open',
                                  side_effect=AssertionError('unexpected network'))
        self.network_mock = self.network.start()
        self.addCleanup(self.network.stop)
        self.db.executescript('''
            CREATE TABLE users(id INTEGER PRIMARY KEY,email TEXT,is_active INTEGER DEFAULT 1,
                               is_banned INTEGER DEFAULT 0,is_suspended INTEGER DEFAULT 0);
            CREATE TABLE notifications(id INTEGER PRIMARY KEY,user_id INTEGER,type TEXT,created_at TEXT);
            CREATE TABLE transactional_email_outbox(
                id INTEGER PRIMARY KEY,notification_id INTEGER,user_id INTEGER,
                notification_type TEXT,created_at TEXT,expires_at TEXT,dedupe_key TEXT,
                state TEXT,claim_token TEXT);
            INSERT INTO users(id,email) VALUES(1,'canary@example.com');
        ''')
        transport.init_schema(self.db)
        self.db.commit()

    def connect(self):
        db = sqlite3.connect(self.path, timeout=2)
        db.row_factory = sqlite3.Row
        return db

    def activate(self, **overrides):
        os.environ.update({
            'EMAIL_PROVIDER': 'agentmail', 'AGENTMAIL_API_KEY': 'offline-test-key',
            'AGENTMAIL_SEND_ENABLED': 'true', 'AGENTMAIL_INBOX_ID': transport.SENDER,
            'AGENTMAIL_RECIPIENT_ALLOWLIST': 'canary@example.com',
            'AGENTMAIL_NOTIFICATION_TYPES': 'new_application',
            'AGENTMAIL_OUTBOX_HIGHWATER': '0', 'AGENTMAIL_NOTIFICATION_HIGHWATER': '0',
            'AGENTMAIL_ACTIVATED_AT': '2026-09-05T11:59:59Z',
            'AGENTMAIL_EXPIRES_AT': '2026-09-12T11:59:59Z',
        })
        os.environ.update(overrides)

    def enqueue(self, key='private-logical-key', notif_type='new_application', enroll=True):
        created = Clock.current.strftime('%Y-%m-%d %H:%M:%S')
        expires = (Clock.current + timedelta(minutes=15)).strftime('%Y-%m-%d %H:%M:%S')
        n = self.db.execute('INSERT INTO notifications(user_id,type,created_at) VALUES(1,?,?)',
                            (notif_type, created)).lastrowid
        i = self.db.execute('''INSERT INTO transactional_email_outbox
            (notification_id,user_id,notification_type,created_at,expires_at,dedupe_key,state,claim_token)
            VALUES(?,1,?,?,?,?,'sending','claim')''', (n, notif_type, created, expires, key)).lastrowid
        if enroll:
            transport.enroll(self.db, i)
        self.db.commit()
        assert i is not None
        return i

    def send(self, i=1, db=None, **overrides):
        db = db or self.db
        row = db.execute('SELECT * FROM transactional_email_outbox WHERE id=?', (i,)).fetchone()
        args = dict(outbox_id=i, claim_token=row['claim_token'], key=row['dedupe_key'],
                    user_id=row['user_id'], notification_type=row['notification_type'])
        args.update(overrides)
        return transport.send(db, **args)

    def response(self, status=200, body=None):
        response = mock.Mock(status=status)
        response.read.return_value = body if body is not None else json.dumps({
            'message_id': '<Opaque.Message+1@agentmail.to>', 'thread_id': 'Thread_1~opaque'
        }).encode()
        return response

    def ledger(self, i=1):
        return self.db.execute('SELECT * FROM agentmail_send_ledger WHERE outbox_id=?', (i,)).fetchone()

    def test_sender_window_configuration_and_enrollment(self):
        self.activate()
        start = datetime(2026, 9, 5, 11, 59, 59, tzinfo=timezone.utc)
        cases = [
            (None, start, 'expiry_time_invalid'),
            ('', start, 'expiry_time_invalid'),
            ('garbage', start, 'expiry_time_invalid'),
            ('2026-09-12T11:59:59+00:00', start, 'expiry_time_invalid'),
            ('2026-09-12T11:59:59.000Z', start, 'expiry_time_invalid'),
            ('2026-9-12T11:59:59Z', start, 'expiry_time_invalid'),
            ('2026-09-12T11:59:59z', start, 'expiry_time_invalid'),
            ('2026-09-12T11:59:59Z\n', start, 'expiry_time_invalid'),
            ('2026-02-30T11:59:59Z', start, 'expiry_time_invalid'),
            ('2026-09-05T11:59:58Z', start, 'expiry_window_invalid'),
            ('2026-09-05T11:59:59Z', start, 'expiry_window_invalid'),
            ('2026-09-12T12:00:00Z', start, 'expiry_window_invalid'),
            ('2026-09-12T11:59:59Z', start - timedelta(microseconds=1), 'activation_not_started'),
            ('2026-09-12T11:59:59Z', start, 'ready'),
            ('2026-09-12T11:59:59Z', start + timedelta(days=7, microseconds=-1), 'ready'),
            ('2026-09-12T11:59:59Z', start + timedelta(days=7), 'expired'),
            ('2026-09-12T11:59:59Z', start + timedelta(days=7, microseconds=1), 'expired'),
        ]
        for index, (expiry, now, reason) in enumerate(cases):
            with self.subTest(expiry=expiry, now=now):
                Clock.current = now
                if expiry is None:
                    os.environ.pop('AGENTMAIL_EXPIRES_AT', None)
                else:
                    os.environ['AGENTMAIL_EXPIRES_AT'] = expiry
                cfg, actual = transport.config()
                self.assertEqual(actual, reason)
                self.assertEqual(cfg is not None, reason == 'ready')
                i = self.enqueue('window-' + str(index))
                self.assertEqual(self.ledger(i) is not None, reason == 'ready')
                if reason != 'ready':
                    self.assertEqual(self.send(i), ('suppressed', None))
        self.network_mock.assert_not_called()

    def test_expiry_crossing_during_binding_prevents_enrollment(self):
        self.activate(AGENTMAIL_EXPIRES_AT='2026-09-05T12:00:01Z')
        real_binding = transport._binding

        def binding_at_expiry(db, row, cfg):
            self.assertLess(Clock.current, cfg['end'])
            Clock.current = cfg['end']
            binding = real_binding(db, row, cfg)
            self.assertIsNotNone(binding)
            return binding

        with mock.patch.object(transport, '_binding', side_effect=binding_at_expiry) as binding:
            i = self.enqueue()
        binding.assert_called_once()
        self.assertIsNone(self.ledger(i))
        self.network_mock.assert_not_called()

    def test_expiry_crossing_after_intent_before_post_never_redelivers(self):
        self.activate(AGENTMAIL_EXPIRES_AT='2026-09-05T12:00:01Z')
        i = self.enqueue()
        opener = mock.Mock()
        opener.open.return_value = self.response()
        def construct_opener(*args):
            self.assertFalse(self.db.in_transaction)
            self.assertEqual(self.ledger(i)['state'], 'prepared')
            Clock.current += timedelta(seconds=1)
            return opener
        with mock.patch('urllib.request.build_opener', side_effect=construct_opener):
            self.assertEqual(self.send(i), ('manual_review', None))
        opener.open.assert_not_called()
        before = dict(self.ledger(i))
        self.assertEqual(before['state'], 'prepared')
        self.db.close()
        self.db = self.connect()
        self.assertEqual(self.send(i), ('manual_review', None))
        # Even restoring a valid window cannot retry a committed intent.
        os.environ['AGENTMAIL_EXPIRES_AT'] = '2026-09-12T11:59:59Z'
        self.assertEqual(self.send(i), ('manual_review', None))
        self.assertEqual(dict(self.ledger(i)), before)
        self.assertEqual(transport.health(self.db)['attempts_total'], 1)
        self.network_mock.assert_not_called()

    def test_health_exposes_only_strict_absolute_expiry_and_reason(self):
        self.activate()
        cases = [
            ('2026-09-12T11:59:59Z', 'ready', '2026-09-12T11:59:59Z'),
            ('2026-09-05T12:00:00Z', 'expired', '2026-09-05T12:00:00Z'),
            ('2026-09-13T12:00:00Z', 'expiry_window_invalid', '2026-09-13T12:00:00Z'),
            ('offline-secret@example.com', 'expiry_time_invalid', None),
            ('2026-09-12T11:59:59+00:00', 'expiry_time_invalid', None),
            ('2026-02-30T11:59:59Z', 'expiry_time_invalid', None),
            ('', 'expiry_time_invalid', None),
        ]
        for expiry, reason, exposed in cases:
            with self.subTest(expiry=expiry):
                os.environ['AGENTMAIL_EXPIRES_AT'] = expiry
                result = transport.health(self.db)
                self.assertIn('expires_at', result)
                self.assertEqual(result['expires_at'], exposed)
                self.assertEqual(result['blocked_reason'], reason)
                self.assertEqual(result['ready'], reason == 'ready')
                for secret in ('offline-secret@example.com', 'offline-test-key', 'canary@example.com'):
                    self.assertNotIn(secret, json.dumps(result))
        os.environ.pop('AGENTMAIL_EXPIRES_AT')
        self.assertIsNone(transport.health(self.db).get('expires_at'))
        self.activate(AGENTMAIL_SEND_ENABLED='false')
        self.assertEqual(transport.health(self.db).get('expires_at'), '2026-09-12T11:59:59Z')
        self.assertEqual(transport.health(self.db)['blocked_reason'], 'send_disabled')
        self.activate()
        Clock.current = datetime(2026, 9, 5, 11, tzinfo=timezone.utc)
        self.assertEqual(transport.health(self.db)['blocked_reason'], 'activation_not_started')
        self.assertEqual(transport.health(self.db).get('expires_at'), '2026-09-12T11:59:59Z')

    def test_sender_send_boundary_preserves_approved_and_accepted_evidence(self):
        self.activate(AGENTMAIL_TOTAL_SEND_CAP='100', AGENTMAIL_DAILY_SEND_CAP='100',
                      AGENTMAIL_EXPIRES_AT='2026-09-05T12:00:01Z')
        for index, micros in enumerate((-1, 0, 1)):
            with self.subTest(microseconds_from_expiry=micros):
                Clock.current = datetime(2026, 9, 5, 12, tzinfo=timezone.utc)
                i = self.enqueue('send-boundary-' + str(index))
                Clock.current += timedelta(seconds=1, microseconds=micros)
                with mock.patch('urllib.request.OpenerDirector.open', return_value=self.response()) as post:
                    result = self.send(i)
                    self.assertEqual(result[0], 'accepted' if micros < 0 else 'manual_review')
                    self.assertEqual(post.call_count, 1 if micros < 0 else 0)
                before = dict(self.ledger(i))
                self.assertEqual(before['state'], 'accepted' if micros < 0 else 'approved')
                Clock.current = datetime(2026, 9, 5, 12, 0, 2, tzinfo=timezone.utc)
                self.assertEqual(self.send(i), ('manual_review', None))
                self.assertEqual(dict(self.ledger(i)), before)
        self.network_mock.assert_not_called()

    def test_missing_invalid_or_future_window_blocks_already_enrolled_send(self):
        self.activate()
        i = self.enqueue()
        before = dict(self.ledger(i))
        for expiry in (None, '', 'invalid', '2026-09-13T11:59:59Z'):
            with self.subTest(expiry=expiry):
                if expiry is None:
                    os.environ.pop('AGENTMAIL_EXPIRES_AT', None)
                else:
                    os.environ['AGENTMAIL_EXPIRES_AT'] = expiry
                self.assertEqual(self.send(i), ('manual_review', None))
                self.assertEqual(dict(self.ledger(i)), before)
        self.activate()
        Clock.current -= timedelta(seconds=2)
        self.assertEqual(self.send(i), ('manual_review', None))
        self.assertEqual(dict(self.ledger(i)), before)
        self.network_mock.assert_not_called()

    def test_committed_intent_unlocks_database_during_http(self):
        self.activate()
        i = self.enqueue()
        observations = []

        def provider(req, **kwargs):
            observations.append((self.db.in_transaction, req, kwargs))
            with self.connect() as other:
                other.execute('BEGIN IMMEDIATE')
                observations.append(other.execute('SELECT state FROM agentmail_send_ledger').fetchone()[0])
            return self.response()

        with mock.patch('urllib.request.OpenerDirector.open', side_effect=provider) as post:
            result = self.send(i)
        self.assertEqual(result, ('accepted', 'agentmail:' + transport.digest('<Opaque.Message+1@agentmail.to>')))
        post.assert_called_once()
        self.assertFalse(observations[0][0])
        self.assertEqual(observations[1], 'prepared')
        self.assertEqual(observations[0][2], {'timeout': 10})
        self.assertEqual(self.ledger()['state'], 'accepted')

    def test_default_total_budget_survives_ambiguous_attempt_and_next_day(self):
        self.activate()
        i = self.enqueue()
        with mock.patch('urllib.request.OpenerDirector.open', side_effect=TimeoutError('secret error')) as post:
            self.assertEqual(self.send(i), ('manual_review', None))
        post.assert_called_once()
        Clock.current += timedelta(days=1)
        self.db.close()
        self.db = self.connect()
        second = self.enqueue('next-day-key')
        with mock.patch('urllib.request.OpenerDirector.open', return_value=self.response()) as post:
            self.assertEqual(self.send(second), ('suppressed', None))
        post.assert_not_called()
        self.assertEqual(self.ledger()['state'], 'prepared')
        health = transport.health(self.db)
        self.assertEqual(health['attempts_total'], 1)
        self.assertEqual(health['total_cap'], 1)
        self.assertEqual(health['blocked_reason'], 'total_cap_reached')

    def test_request_is_minimal_generic_fixed_sender_with_stable_http_key(self):
        self.activate()
        i = self.enqueue()
        with mock.patch('urllib.request.OpenerDirector.open', return_value=self.response()) as post:
            self.assertEqual(self.send(i)[0], 'accepted')
            self.assertEqual(self.send(i)[0], 'accepted')
        post.assert_called_once()
        request = post.call_args.args[0]
        self.assertEqual(request.get_header('Idempotency-key'),
                         'ghh-agentmail-' + transport.digest('private-logical-key'))
        self.assertRegex(request.get_header('Idempotency-key'), r'^[A-Za-z0-9._~-]{1,256}$')
        self.assertEqual(request.get_header('Authorization'), 'Bearer offline-test-key')
        self.assertEqual(request.get_method(), 'POST')
        self.assertEqual(request.full_url,
            'https://api.agentmail.to/v0/inboxes/gohirehumans.operations%40agentmail.to/messages/send')
        payload = json.loads(request.data)
        self.assertEqual(set(payload), {'to', 'reply_to', 'subject', 'text', 'track_opens'})
        self.assertEqual(payload['to'], ['canary@example.com'])
        self.assertEqual(payload['reply_to'], [transport.SENDER])
        self.assertIs(payload['track_opens'], False)
        self.assertIn('https://www.gohirehumans.com', payload['text'])
        self.assertIn('You received this email because', payload['text'])
        self.assertNotIn('<', payload['text'])
        self.assertNotIn('private', request.data.decode().lower())

    def test_acceptance_retains_exact_opaque_ids_only_in_private_ledger(self):
        self.activate()
        i = self.enqueue()
        response = self.response()
        with mock.patch('urllib.request.OpenerDirector.open', return_value=response):
            result = self.send(i)
        self.assertEqual(result[0], 'accepted')
        ledger = dict(self.ledger())
        self.assertEqual(ledger.get('message_id'), '<Opaque.Message+1@agentmail.to>')
        self.assertEqual(ledger.get('thread_id'), 'Thread_1~opaque')
        response.read.assert_called_once_with(16385)
        response.close.assert_called_once()
        public = json.dumps(transport.health(self.db)) + str(result)
        for value in (ledger['message_id'], ledger['thread_id'], 'offline-test-key',
                      'canary@example.com', 'private-logical-key'):
            self.assertNotIn(value, public)
        for value in ('offline-test-key', 'canary@example.com', 'private-logical-key', transport.TEXT):
            self.assertNotIn(value, str(ledger))
        self.db.close()
        self.db = self.connect()
        transport.init_schema(self.db)
        self.assertEqual(self.ledger()['message_id'], ledger['message_id'])
        with mock.patch('urllib.request.OpenerDirector.open') as post:
            self.assertEqual(self.send(i), result)
        post.assert_not_called()

    def test_noncontract_responses_are_ambiguous_and_never_reposted(self):
        self.activate(AGENTMAIL_TOTAL_SEND_CAP='100', AGENTMAIL_DAILY_SEND_CAP='100')
        good = {'message_id': '<one@agentmail.to>', 'thread_id': 'thread-one'}
        cases = [(status, json.dumps(good).encode()) for status in (201, 202, 204, 301, 302, 307, 308, 400, 409, 429, 500, 503)]
        cases += [(200, body) for body in (b'', b'not json secret', b'[]', b'null', b'{}',
                                           b'x' * 16385, b'\xff', b'{"message_id":"one"}')]
        for field in ('message_id', 'thread_id'):
            for value in (None, '', 1, True, [], {}, 'white space', 'newline\n', '\u00e9', '\x00', 'x' * 999):
                cases.append((200, json.dumps(dict(good, **{field: value})).encode()))
        for index, (status, body) in enumerate(cases):
            with self.subTest(status=status, case=index):
                i = self.enqueue('bad-response-' + str(index))
                response = self.response(status, body)
                with mock.patch('urllib.request.OpenerDirector.open', return_value=response) as post:
                    self.assertEqual(self.send(i), ('manual_review', None))
                    self.assertEqual(self.send(i), ('manual_review', None))
                post.assert_called_once()
                response.close.assert_called_once()
                self.assertEqual(self.ledger(i)['state'], 'prepared')
                self.assertIsNone(self.ledger(i)['message_id'])
                self.assertIsNone(self.ledger(i)['thread_id'])
        self.assertNotIn('not json secret', str(list(self.db.execute('SELECT * FROM agentmail_send_ledger'))))


    def test_lost_acceptance_compare_and_swap_never_reports_success(self):
        self.activate()
        i = self.enqueue()
        def provider(*args, **kwargs):
            with self.connect() as other:
                other.execute("UPDATE agentmail_send_ledger SET state='unknown'")
            return self.response()
        with mock.patch('urllib.request.OpenerDirector.open', side_effect=provider) as post:
            self.assertEqual(self.send(i), ('manual_review', None))
            self.assertEqual(self.send(i), ('manual_review', None))
        post.assert_called_once()
        self.assertEqual(self.ledger()['state'], 'unknown')
        self.assertIsNone(self.ledger()['message_id'])


    def test_concurrent_claims_issue_only_one_post(self):
        self.activate()
        i = self.enqueue()
        entered, release = threading.Event(), threading.Event()
        results = []
        def provider(*args, **kwargs):
            entered.set()
            self.assertTrue(release.wait(5))
            return self.response()
        def first():
            db = self.connect()
            try:
                results.append(self.send(i, db=db))
            finally:
                db.close()
        with mock.patch('urllib.request.OpenerDirector.open', side_effect=provider) as post:
            thread = threading.Thread(target=first)
            thread.start()
            try:
                self.assertTrue(entered.wait(5))
                with self.connect() as other:
                    self.assertEqual(self.send(i, db=other), ('manual_review', None))
            finally:
                release.set()
                thread.join(5)
            self.assertFalse(thread.is_alive())
        post.assert_called_once()
        self.assertEqual(results[0][0], 'accepted')

    def test_disabled_backlog_cannot_enroll_on_drain_or_provider_switch(self):
        old = self.enqueue()
        self.activate()
        self.assertEqual(self.send(old), ('suppressed', None))
        fresh = self.enqueue('fresh')
        with mock.patch('urllib.request.OpenerDirector.open', side_effect=TimeoutError('private')) as post:
            self.assertEqual(self.send(fresh), ('manual_review', None))
        post.assert_called_once()
        os.environ['EMAIL_PROVIDER'] = 'resend'
        self.assertTrue(transport.owns_key(self.db, 'fresh'))
        self.assertEqual(self.send(fresh), ('manual_review', None))
        self.network_mock.assert_not_called()

    def test_invalid_gates_and_freshness_never_send(self):
        self.activate()
        i = self.enqueue()
        for name, value in {
            'AGENTMAIL_SEND_ENABLED':'false', 'AGENTMAIL_API_KEY':'bad\nkey',
            'AGENTMAIL_INBOX_ID':'other@example.com', 'AGENTMAIL_RECIPIENT_ALLOWLIST':'*',
            'AGENTMAIL_NOTIFICATION_TYPES':'job_match', 'AGENTMAIL_OUTBOX_HIGHWATER':'-1',
            'AGENTMAIL_NOTIFICATION_HIGHWATER':'01', 'AGENTMAIL_ACTIVATED_AT':'invalid',
            'AGENTMAIL_DAILY_SEND_CAP':'101', 'AGENTMAIL_TOTAL_SEND_CAP':'0',
        }.items():
            with self.subTest(name=name), mock.patch.dict(os.environ, {name:value}):
                self.assertIsNone(transport.config()[0])
                self.assertEqual(self.send(i), ('manual_review', None))
        Clock.current += timedelta(minutes=16)
        self.assertEqual(self.send(i), ('manual_review', None))
        self.network_mock.assert_not_called()

    def test_changed_recipient_source_or_claim_never_sends(self):
        self.activate()
        i = self.enqueue()
        self.assertEqual(self.send(i, claim_token='wrong'), ('suppressed', None))
        self.db.execute("UPDATE users SET email='changed@example.com'")
        self.db.commit()
        self.assertEqual(self.send(i), ('manual_review', None))
        self.db.execute("UPDATE users SET email='canary@example.com'")
        self.db.execute("UPDATE notifications SET type='job_match'")
        self.db.commit()
        self.assertEqual(self.send(i), ('manual_review', None))
        self.network_mock.assert_not_called()

    def test_poisoned_schema_and_enrollment_rollback_fail_closed(self):
        self.activate()
        i = self.enqueue(enroll=False)
        transport.enroll(self.db, i)
        self.db.rollback()
        self.assertIsNone(self.ledger())
        self.assertEqual(self.send(i), ('suppressed', None))
        self.db.execute("CREATE TRIGGER poison AFTER INSERT ON agentmail_send_ledger BEGIN SELECT 1; END")
        self.db.commit()
        with self.assertRaisesRegex(RuntimeError, 'agentmail_schema_invalid'):
            transport.init_schema(self.db)
        self.assertEqual(transport.health(self.db)['blocked_reason'], 'schema_invalid')
        self.network_mock.assert_not_called()


if __name__ == '__main__':
    unittest.main()
