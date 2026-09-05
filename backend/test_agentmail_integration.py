"""Real lease-bound maintenance integration, entirely offline."""
import json
import os
import unittest
import tempfile
from datetime import datetime, timedelta, timezone
from unittest import mock
from test_deep_audit_regressions import load_api_core


class AgentMailIntegrationTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.env = mock.patch.dict(os.environ, {
            'DATABASE_PATH': self.tmp.name + '/mail.db', 'DISABLE_AUTO_SEED': '1',
            'EMAIL_PROVIDER': 'agentmail', 'AGENTMAIL_API_KEY': 'offline-test-key',
            'AGENTMAIL_SEND_ENABLED': '', 'AGENTMAIL_INBOX_ID': '',
            'AGENTMAIL_RECIPIENT_ALLOWLIST': '', 'AGENTMAIL_NOTIFICATION_TYPES': '',
            'AGENTMAIL_OUTBOX_HIGHWATER': '', 'AGENTMAIL_NOTIFICATION_HIGHWATER': '',
            'AGENTMAIL_ACTIVATED_AT': '', 'AGENTMAIL_DAILY_SEND_CAP': '1',
            'AGENTMAIL_TOTAL_SEND_CAP': '1',
        })
        self.env.start()
        self.api = load_api_core()
        self.api._db_path_resolved = None
        self.api.init_db()
        self.db = self.api.get_db()
        self.db.execute("INSERT INTO users(id,email,name,password_hash) VALUES(1,'canary@example.com','Private name','x')")
        self.db.commit()
        self.api.RESEND_API_KEY = 'offline-resend-key'

    def tearDown(self):
        self.db.close()
        self.env.stop()
        self.tmp.cleanup()

    def activate(self):
        os.environ.update({
            'AGENTMAIL_SEND_ENABLED': 'true',
            'AGENTMAIL_INBOX_ID': 'gohirehumans.operations@agentmail.to',
            'AGENTMAIL_RECIPIENT_ALLOWLIST': 'canary@example.com',
            'AGENTMAIL_NOTIFICATION_TYPES': 'new_application',
            'AGENTMAIL_OUTBOX_HIGHWATER': str(self.db.execute('SELECT COALESCE(MAX(id),0) FROM transactional_email_outbox').fetchone()[0]),
            'AGENTMAIL_NOTIFICATION_HIGHWATER': str(self.db.execute('SELECT COALESCE(MAX(id),0) FROM notifications').fetchone()[0]),
            'AGENTMAIL_ACTIVATED_AT': (datetime.now(timezone.utc) - timedelta(seconds=1)).strftime('%Y-%m-%dT%H:%M:%SZ'),
        })

    def enqueue(self, context='one', notif_type='new_application'):
        self.api.push_notification(self.db, 1, notif_type, 'Private title',
                                   'Sensitive application contents', link='/jobs/1/applications',
                                   email=True, email_dedupe=context)
        self.db.commit()
        return self.db.execute('SELECT * FROM transactional_email_outbox ORDER BY id DESC').fetchone()

    def cycle(self):
        self.assertTrue(self.api.acquire_notification_worker_lease(self.db, 'offline-owner'))
        return self.api.run_notification_maintenance_once(owner_token='offline-owner')

    def test_maintenance_sends_approved_application_without_resend(self):
        self.api.RESEND_API_KEY = ''
        self.activate()
        self.enqueue()
        response = mock.MagicMock(status=200)
        response.read.return_value = b'{"message_id":"<integration@agentmail.to>","thread_id":"thread-integration"}'
        with mock.patch('urllib.request.OpenerDirector.open', return_value=response) as agent, mock.patch('urllib.request.urlopen') as resend:
            result = self.cycle()
        self.assertEqual(result['email_delivery']['sent'], 1)
        agent.assert_called_once()
        resend.assert_not_called()
        self.assertEqual(self.db.execute('SELECT state FROM agentmail_send_ledger').fetchone()[0], 'accepted')

    def _crash_before_outbox_ack(self, accepted=False):
        self.activate()
        row = self.enqueue()
        class ProcessDeath(BaseException):
            pass
        transport_send = self.api.agentmail_transport.send
        def crash_after_acceptance(*args, **kwargs):
            result = transport_send(*args, **kwargs)
            self.assertEqual(result[0], 'accepted')
            raise ProcessDeath()
        def crash_during_post(*args, **kwargs):
            self.assertFalse(self.db.in_transaction)
            raise ProcessDeath()
        response = mock.Mock(status=200)
        response.read.return_value = b'{"message_id":"<exact+handle@example.com>","thread_id":"exact-thread"}'
        with mock.patch('urllib.request.OpenerDirector.open',
                        side_effect=None if accepted else crash_during_post,
                        return_value=response) as post, mock.patch(
                'urllib.request.urlopen') as resend, mock.patch.object(
                self.api.agentmail_transport, 'send',
                side_effect=crash_after_acceptance if accepted else transport_send):
            with self.assertRaises(ProcessDeath):
                self.api.flush_transactional_notification_emails(self.db)
        post.assert_called_once()
        resend.assert_not_called()
        self.db.close()
        self.db = self.api.get_db()
        self.assertEqual(self.db.execute(
            'SELECT state FROM transactional_email_outbox WHERE id=?',
            [row['id']]).fetchone()[0], 'sending')
        return row

    def _recover_crashed_outbox(self, row, suspended=False):
        self.db.execute("UPDATE transactional_email_outbox SET claimed_at=datetime('now','-11 minutes') WHERE id=?", [row['id']])
        if suspended:
            self.db.execute('UPDATE users SET is_suspended=1 WHERE id=1')
        self.db.commit()
        self.assertTrue(self.api.acquire_notification_worker_lease(self.db, 'recovery-owner'))
        recovery_now = None if suspended else datetime.now(timezone.utc) + timedelta(days=2)
        with mock.patch('urllib.request.OpenerDirector.open') as post, mock.patch('urllib.request.urlopen') as resend:
            result = self.api.flush_transactional_notification_emails(
                self.db, now=recovery_now, owner_token='recovery-owner')
            self.api.flush_transactional_notification_emails(
                self.db, now=recovery_now, owner_token='recovery-owner')
        post.assert_not_called()
        resend.assert_not_called()
        self.db.close()
        self.db = self.api.get_db()
        after = self.db.execute('SELECT * FROM transactional_email_outbox WHERE id=?', [row['id']]).fetchone()
        self.assertEqual(after['attempts'], 1)
        self.assertIsNone(after['delivered_at'])
        for field in ('email_to', 'title', 'message', 'link'):
            self.assertEqual(after[field], '')
        for field in ('claimed_at', 'claim_token', 'next_attempt_at'):
            self.assertIsNone(after[field])
        self.assertEqual(result['claimed_recovered'], 1)
        return after

    def test_expired_crashed_attempt_requires_manual_review(self):
        row = self._crash_before_outbox_ack()
        after = self._recover_crashed_outbox(row)
        self.assertEqual((after['state'], after['delivery_status']), ('failed', 'manual_review'))
        self.assertEqual(self.api.notification_delivery_health(self.db)['outbox']['manual_review'], 1)
        self.assertEqual(self.db.execute('SELECT state FROM agentmail_send_ledger').fetchone()[0], 'prepared')

    def test_suspended_recipient_crashed_attempt_requires_manual_review(self):
        row = self._crash_before_outbox_ack()
        after = self._recover_crashed_outbox(row, suspended=True)
        self.assertEqual((after['state'], after['delivery_status']), ('failed', 'manual_review'))
        self.assertEqual(self.api.notification_delivery_health(self.db)['outbox']['manual_review'], 1)

    def test_expired_accepted_before_outbox_ack_preserves_provenance(self):
        row = self._crash_before_outbox_ack(accepted=True)
        ledger = dict(self.db.execute('SELECT * FROM agentmail_send_ledger').fetchone())
        after = self._recover_crashed_outbox(row)
        self.assertEqual((after['state'], after['delivery_status']), ('sent', 'accepted'))
        self.assertEqual(after['provider_email_id'], ledger['provider_id'])
        self.assertEqual(after['sent_at'], ledger['prepared_at'])
        self.assertEqual(dict(self.db.execute('SELECT * FROM agentmail_send_ledger').fetchone()), ledger)
        self.assertEqual(self.api.notification_delivery_health(self.db)['outbox']['manual_review'], 0)

    def test_reconciliation_does_not_touch_live_sending_claim(self):
        row = self._crash_before_outbox_ack(accepted=True)
        before = dict(self.db.execute('SELECT * FROM transactional_email_outbox WHERE id=?', [row['id']]).fetchone())
        self.db.execute('UPDATE users SET is_suspended=1 WHERE id=1')
        self.db.commit()
        with mock.patch('urllib.request.OpenerDirector.open') as post, mock.patch('urllib.request.urlopen') as resend:
            self.api.flush_transactional_notification_emails(
                self.db, now=datetime.now(timezone.utc) + timedelta(days=2))
        self.assertEqual(dict(self.db.execute('SELECT * FROM transactional_email_outbox WHERE id=?', [row['id']]).fetchone()), before)
        post.assert_not_called()
        resend.assert_not_called()

    def test_reconciliation_requires_current_worker_lease(self):
        row = self._crash_before_outbox_ack()
        self.db.execute("UPDATE transactional_email_outbox SET claimed_at=datetime('now','-11 minutes') WHERE id=?", [row['id']])
        self.db.commit()
        self.assertTrue(self.api.acquire_notification_worker_lease(self.db, 'current-owner'))
        before = dict(self.db.execute('SELECT * FROM transactional_email_outbox WHERE id=?', [row['id']]).fetchone())
        result = self.api.flush_transactional_notification_emails(self.db, owner_token='stale-owner')
        self.assertEqual(result['lease_lost'], 1)
        self.assertEqual(dict(self.db.execute('SELECT * FROM transactional_email_outbox WHERE id=?', [row['id']]).fetchone()), before)

    def test_reconciliation_is_bounded_without_suppressing_remainder(self):
        row = self._crash_before_outbox_ack()
        other = self.enqueue('second-bounded-fixture')
        # Synthetic durable unknown intent tests batching without another POST
        # or any change to the configured provider budgets.
        self.db.execute("UPDATE agentmail_send_ledger SET state='unknown',prepared_at=datetime('now') WHERE outbox_id=?", [other['id']])
        self.db.execute("UPDATE transactional_email_outbox SET claimed_at=datetime('now','-11 minutes') WHERE id=?", [row['id']])
        self.db.commit()
        now = datetime.now(timezone.utc) + timedelta(days=2)
        with mock.patch('urllib.request.OpenerDirector.open') as post, mock.patch('urllib.request.urlopen') as resend:
            first = self.api.flush_transactional_notification_emails(self.db, now=now, limit=1)
            self.assertEqual(first['failed'], 1)
            self.assertEqual(self.db.execute('SELECT state FROM transactional_email_outbox WHERE id=?', [other['id']]).fetchone()[0], 'pending')
            second = self.api.flush_transactional_notification_emails(self.db, now=now, limit=1)
            self.assertEqual(second['failed'], 1)
        self.assertEqual(self.api.notification_delivery_health(self.db)['outbox']['manual_review'], 2)
        post.assert_not_called()
        resend.assert_not_called()

    def test_reconciliation_requires_matching_outbox_id_and_key_digest(self):
        row = self._crash_before_outbox_ack(accepted=True)
        other = self.enqueue('different-outbox')
        # Cross the ledger's two identifiers: neither row owns both bindings.
        self.db.execute('UPDATE agentmail_send_ledger SET outbox_id=-outbox_id')
        self.db.execute('UPDATE agentmail_send_ledger SET outbox_id=? WHERE outbox_id=?', [other['id'], -row['id']])
        self.db.execute('UPDATE agentmail_send_ledger SET outbox_id=? WHERE outbox_id=?', [row['id'], -other['id']])
        self.db.execute("UPDATE transactional_email_outbox SET claimed_at=datetime('now','-11 minutes') WHERE id=?", [row['id']])
        self.db.commit()
        with mock.patch('urllib.request.OpenerDirector.open') as post, mock.patch('urllib.request.urlopen') as resend:
            self.api.flush_transactional_notification_emails(self.db, now=datetime.now(timezone.utc) + timedelta(days=2))
        for after in self.db.execute('SELECT state,delivery_status,provider_email_id FROM transactional_email_outbox'):
            self.assertEqual(tuple(after), ('failed', 'suppressed', None))
        post.assert_not_called()
        resend.assert_not_called()

    def test_selected_provider_health_and_worker_readiness_are_truthful(self):
        self.api.RESEND_API_KEY = ''
        self.api.RESEND_WEBHOOK_SECRET = 'legacy-secret-not-agentmail'
        self.activate()
        config = self.api.notification_delivery_health(self.db)['configuration']
        self.assertEqual(config['selected_provider'], 'agentmail')
        self.assertTrue(config['provider_configured'])
        self.assertTrue(config['sender_configured'])
        self.assertFalse(config['webhook_configured'])
        self.assertTrue(self.api.notification_worker_enabled())
        for forbidden in ('canary@example.com', 'offline-test-key', 'legacy-secret-not-agentmail'):
            self.assertNotIn(forbidden, json.dumps(self.api.notification_delivery_health(self.db)))

    def test_disabled_agentmail_does_not_drain_even_with_resend_key(self):
        self.enqueue()
        before = tuple(self.db.execute('SELECT state,attempts,delivery_status FROM transactional_email_outbox').fetchone())
        with mock.patch('urllib.request.OpenerDirector.open') as agent, mock.patch('urllib.request.urlopen') as resend:
            result = self.cycle()
        self.assertEqual(result['email_delivery'].get('provider_unavailable'), 1)
        after = tuple(self.db.execute('SELECT state,attempts,delivery_status FROM transactional_email_outbox').fetchone())
        self.assertEqual(after, before)
        agent.assert_not_called()
        resend.assert_not_called()

    def test_unknown_provider_never_falls_back_to_resend(self):
        os.environ['EMAIL_PROVIDER'] = 'typo-agentmail'
        self.enqueue()
        with mock.patch('urllib.request.urlopen') as resend, mock.patch('urllib.request.OpenerDirector.open') as agent:
            result = self.cycle()
        self.assertEqual(result['email_delivery'].get('provider_unavailable'), 1)
        resend.assert_not_called()
        agent.assert_not_called()


if __name__ == '__main__':
    unittest.main()
