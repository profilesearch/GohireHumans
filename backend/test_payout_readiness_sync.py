"""Live Connect readiness reconciles durable listing visibility, without Stripe writes."""
import hashlib
import sqlite3
import unittest
from unittest import mock

import stripe

import test_ai_listing_policy as policy


class PayoutReadinessSyncTests(unittest.TestCase):
    setUp = policy.AIListingPolicyTests.setUp
    tearDown = policy.AIListingPolicyTests.tearDown
    request = policy.AIListingPolicyTests.request
    create = policy.AIListingPolicyTests.create

    READY = {'payouts_enabled': True, 'charges_enabled': False,
             'capabilities': {'transfers': 'active'}}
    PENDING = {'payouts_enabled': False, 'capabilities': {'transfers': 'pending'}}

    def bind(self, user_id, account_id, method='stripe_connect_pending'):
        with self.core.get_db() as db:
            db.execute('UPDATE worker_profiles SET payout_account_id=?,payout_method=? WHERE user_id=?',
                       (account_id, method, user_id))
            db.commit()

    def state(self, user_id):
        with self.core.get_db() as db:
            return db.execute('SELECT payout_account_id,payout_method FROM worker_profiles WHERE user_id=?',
                              (user_id,)).fetchone()

    def tally(self, user_id):
        with self.core.get_db() as db:
            return (db.execute("SELECT COUNT(*) FROM notifications WHERE user_id=? AND type='payout_ready'", (user_id,)).fetchone()[0],
                    db.execute("SELECT COUNT(*) FROM audit_log WHERE user_id=? AND action='sync_worker_payout_readiness'", (user_id,)).fetchone()[0])

    def test_status_promotes_and_exposes_ai_listing_without_charges(self):
        listing = self.create(1)
        self.bind(1, 'acct_live_worker_000001')
        self.assertEqual(self.request('GET', f"/services/{listing['id']}")[0], 404)
        self.assertNotIn(listing['id'], [s['id'] for s in self.request('GET', '/services')[1]['services']])
        with mock.patch.object(self.core, 'stripe_configured', return_value=True), \
             mock.patch.object(self.core, 'retrieve_live_connect_account', return_value=self.READY) as retrieve:
            status, body = self.request('GET', '/payments/status', token='tok-1')
        self.assertEqual(status, 200, body)
        retrieve.assert_called_once_with('acct_live_worker_000001')
        self.assertTrue(body['worker_ready'])
        self.assertEqual(self.state(1)['payout_method'], 'stripe_connect_active')
        self.assertEqual(self.request('GET', f"/services/{listing['id']}")[0], 200)
        self.assertIn(listing['id'], [s['id'] for s in self.request('GET', '/services')[1]['services']])
        self.assertEqual(self.tally(1), (1, 1))
        with mock.patch.object(self.core, 'stripe_configured', return_value=True), \
             mock.patch.object(self.core, 'retrieve_live_connect_account', return_value=self.READY):
            self.request('GET', '/payments/status', token='tok-1')
        self.assertEqual(self.tally(1), (1, 1))

    def test_status_demotes_stale_active(self):
        listing = self.create(1)
        self.bind(1, 'acct_live_worker_000001', 'stripe_connect_active')
        with mock.patch.object(self.core, 'stripe_configured', return_value=True), \
             mock.patch.object(self.core, 'retrieve_live_connect_account', return_value=self.PENDING):
            status, body = self.request('GET', '/payments/status', token='tok-1')
        self.assertEqual(status, 200, body)
        self.assertFalse(body['worker_ready'])
        self.assertEqual(self.state(1)['payout_method'], 'stripe_connect_pending')
        self.assertEqual(self.request('GET', f"/services/{listing['id']}")[0], 404)
        self.assertEqual(self.tally(1), (0, 1))

    def test_status_stripe_error_preserves_stored_state(self):
        self.bind(1, 'acct_live_worker_000001', 'stripe_connect_active')
        with mock.patch.object(self.core, 'stripe_configured', return_value=True), \
             mock.patch.object(self.core, 'retrieve_live_connect_account',
                               side_effect=stripe.APIConnectionError('offline')):
            status, _ = self.request('GET', '/payments/status', token='tok-1')
        self.assertEqual(status, 200)
        self.assertEqual(self.state(1)['payout_method'], 'stripe_connect_active')
        self.assertEqual(self.tally(1), (0, 0))

    def test_status_account_rebound_during_retrieve_cannot_update_new_account(self):
        self.bind(1, 'acct_live_worker_000001')
        def rebound(account_id):
            self.bind(1, 'acct_live_worker_000002')
            return self.READY
        with mock.patch.object(self.core, 'stripe_configured', return_value=True), \
             mock.patch.object(self.core, 'retrieve_live_connect_account', side_effect=rebound):
            status, _ = self.request('GET', '/payments/status', token='tok-1')
        self.assertEqual(status, 200)
        self.assertEqual(tuple(self.state(1)), ('acct_live_worker_000002', 'stripe_connect_pending'))
        self.assertEqual(self.tally(1), (0, 0))

    def test_admin_dry_run_and_apply_are_private_and_step_up_gated(self):
        self.bind(1, 'acct_live_worker_000001')
        self.bind(2, 'acct_live_worker_000002', 'stripe_connect_active')
        with self.core.get_db() as db:
            db.execute('UPDATE users SET password_hash=? WHERE id=5', (self.core.hash_password('Admin-Test-Password-123!'),))
            db.commit()
        accounts = {'acct_live_worker_000001': self.READY, 'acct_live_worker_000002': self.PENDING}
        with mock.patch.object(self.core, 'stripe_configured', return_value=True), \
             mock.patch.object(self.core, 'retrieve_live_connect_account', side_effect=accounts.get) as retrieve:
            self.assertEqual(self.request('POST', '/admin/payout-readiness/sync', {'admin_password': 'Admin-Test-Password-123!'}, 'tok-1')[0], 403)
            status, body = self.request('POST', '/admin/payout-readiness/sync', {'dry_run': True}, 'tok-5')
            self.assertEqual(status, 403, body)
            self.assertIn('Admin password confirmation required', body['error'])
            self.assertEqual(retrieve.call_count, 0)
            status, body = self.request('POST', '/admin/payout-readiness/sync', {'admin_password': 'Admin-Test-Password-123!'}, 'tok-5')
            self.assertEqual(status, 200, body)
            self.assertEqual(body, {'dry_run': True, 'checked': 2, 'errors': 0,
                                    'promote': [{'user_id': 1, 'account_id_suffix': '000001'}],
                                    'demote': [{'user_id': 2, 'account_id_suffix': '000002'}], 'unchanged': 0})
            self.assertEqual(self.state(1)['payout_method'], 'stripe_connect_pending')
            self.assertEqual(self.state(2)['payout_method'], 'stripe_connect_active')
            self.assertEqual(self.tally(1), (0, 0))
            status, body = self.request('POST', '/admin/payout-readiness/sync',
                                        {'admin_password': 'Admin-Test-Password-123!', 'dry_run': False}, 'tok-5')
            self.assertEqual(status, 200, body)
            self.assertFalse(body['dry_run'])
            self.assertEqual(body['checked'], 2)
            self.assertEqual(self.state(1)['payout_method'], 'stripe_connect_active')
            self.assertEqual(self.state(2)['payout_method'], 'stripe_connect_pending')
            self.assertEqual(self.tally(1), (1, 1))
            self.assertEqual(self.tally(2), (0, 1))
            self.assertEqual(retrieve.call_count, 4)

    def test_admin_validation_and_stripe_errors_do_not_demote(self):
        self.bind(1, 'acct_live_worker_000001', 'stripe_connect_active')
        with self.core.get_db() as db:
            db.execute('UPDATE users SET password_hash=? WHERE id=5', (self.core.hash_password('Admin-Test-Password-123!'),))
            db.commit()
        with mock.patch.object(self.core, 'stripe_configured', return_value=True), \
             mock.patch.object(self.core, 'retrieve_live_connect_account',
                               side_effect=stripe.APIConnectionError('offline')):
            for limit in (0, 501, True, '1e9'):
                self.assertEqual(self.request('POST', '/admin/payout-readiness/sync',
                    {'admin_password': 'Admin-Test-Password-123!', 'limit': limit}, 'tok-5')[0], 400)
            status, body = self.request('POST', '/admin/payout-readiness/sync',
                {'admin_password': 'Admin-Test-Password-123!', 'dry_run': False, 'limit': 1}, 'tok-5')
        self.assertEqual(status, 200, body)
        self.assertEqual((body['checked'], body['errors']), (1, 1))
        self.assertEqual(self.state(1)['payout_method'], 'stripe_connect_active')
        self.assertEqual(self.tally(1), (0, 0))

    def test_read_scoped_key_status_cannot_change_listing_state(self):
        self.bind(1, 'acct_live_worker_000001')
        key = 'ghh_' + 'k' * 28
        with self.core.get_db() as db:
            db.execute('INSERT INTO api_keys(user_id,key_hash,key_prefix,name,scopes) VALUES (?,?,?,?,?)',
                       (1, hashlib.sha256(key.encode()).hexdigest(), key[:12], 'readiness test', '["read"]'))
            db.commit()
        original = self.core.handle_request
        def with_key():
            self.core._request_ctx.http_x_api_key = key
            return original()
        with mock.patch.object(self.core, 'stripe_configured', return_value=True), \
             mock.patch.object(self.core, 'retrieve_live_connect_account', return_value=self.READY), \
             mock.patch.object(self.core, 'handle_request', side_effect=with_key):
            status, body = self.request('GET', '/payments/status')
        self.assertEqual(status, 200, body)
        self.assertTrue(body['worker_ready'])
        self.assertEqual(self.state(1)['payout_method'], 'stripe_connect_pending')
        self.assertEqual(self.tally(1), (0, 0))

    def test_admin_apply_rechecks_current_method_after_retrieve(self):
        self.bind(1, 'acct_live_worker_000001', 'stripe_connect_active')
        with self.core.get_db() as db:
            db.execute('UPDATE users SET password_hash=? WHERE id=5', (self.core.hash_password('Admin-Test-Password-123!'),))
            db.commit()
        def changed_while_retrieving(account_id):
            self.bind(1, account_id, 'stripe_connect_pending')
            return self.READY
        with mock.patch.object(self.core, 'stripe_configured', return_value=True), \
             mock.patch.object(self.core, 'retrieve_live_connect_account', side_effect=changed_while_retrieving):
            status, body = self.request('POST', '/admin/payout-readiness/sync',
                {'admin_password': 'Admin-Test-Password-123!', 'dry_run': False, 'limit': 1}, 'tok-5')
        self.assertEqual(status, 200, body)
        self.assertEqual(body['promote'], [{'user_id': 1, 'account_id_suffix': '000001'}])
        self.assertEqual(self.state(1)['payout_method'], 'stripe_connect_active')

    def test_retrieve_never_holds_sqlite_writer_even_on_second_account(self):
        self.bind(1, 'acct_live_worker_000001')
        self.bind(2, 'acct_live_worker_000002')
        with self.core.get_db() as db:
            db.execute('UPDATE users SET password_hash=? WHERE id=5', (self.core.hash_password('Admin-Test-Password-123!'),))
            db.commit()
        original_get_db = self.core.get_db
        connections = []
        def track_db():
            db = original_get_db()
            connections.append(db)
            return db
        def retrieve(account_id):
            db = connections[-1]
            self.assertFalse(db.in_transaction, account_id)
            with sqlite3.connect(self.core._get_db_path(), timeout=0.2) as contender:
                contender.execute('BEGIN IMMEDIATE')
                contender.execute('ROLLBACK')
            return self.READY
        with mock.patch.object(self.core, 'get_db', side_effect=track_db), \
             mock.patch.object(self.core, 'stripe_configured', return_value=True), \
             mock.patch.object(self.core, 'retrieve_live_connect_account', side_effect=retrieve) as called:
            status, body = self.request('POST', '/admin/payout-readiness/sync',
                {'admin_password': 'Admin-Test-Password-123!', 'dry_run': False}, 'tok-5')
        self.assertEqual(status, 200, body)
        self.assertEqual(called.call_count, 2)
        self.assertEqual(self.tally(1), (1, 1))
        self.assertEqual(self.tally(2), (1, 1))


if __name__ == '__main__':
    unittest.main()
