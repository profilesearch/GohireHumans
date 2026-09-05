"""Offline checkout safety regressions: processor responses are explicit fixtures."""
import unittest
from types import SimpleNamespace
from unittest import mock

import test_transaction_lifecycle_regressions as fixtures


class ServiceCheckoutSafetyTests(unittest.TestCase):
    def setUp(self):
        fixtures.TransactionLifecycleRegressionTests.setUp(self)
        self.api.JOB_HIRING_ENABLED = False
        with self.api.get_db() as db:
            db.execute("UPDATE worker_profiles SET payout_account_id=NULL,payout_method=NULL")
            db.commit()

    tearDown = fixtures.TransactionLifecycleRegressionTests.tearDown
    _seed = fixtures.TransactionLifecycleRegressionTests._seed
    request = fixtures.TransactionLifecycleRegressionTests.request

    def ready_worker(self):
        with self.api.get_db() as db:
            db.execute("UPDATE worker_profiles SET payout_account_id='acct_checkout_fixture',payout_method='stripe_connect_active' WHERE user_id=1")
            db.commit()
        self.account_retrieve = mock.Mock(return_value={
            'id': 'acct_checkout_fixture', 'payouts_enabled': True,
            'charges_enabled': True, 'capabilities': {'transfers': 'active'}})
        self.api.stripe.Account = SimpleNamespace(retrieve=self.account_retrieve)

    def test_live_unready_processor_account_blocks_new_funding(self):
        self.ready_worker()
        for account in [None, {}, {'payouts_enabled': False},
                        {'payouts_enabled': True, 'charges_enabled': True, 'capabilities': []},
                        {'payouts_enabled': True, 'charges_enabled': True, 'capabilities': {'transfers': 'pending'}},
                        {'payouts_enabled': 'false', 'charges_enabled': True, 'capabilities': {'transfers': 'active'}}]:
            with self.subTest(account=account):
                self.account_retrieve.return_value = account
                status, data = self.request('POST', '/services/1/order', payload={
                    'idempotency_key': 'unready-worker-payout-0001', 'amount': '25.55'})
                self.assertEqual(status, 409, data)
                self.payment_create.assert_not_called()
        with self.api.get_db() as db:
            self.assertEqual(db.execute('SELECT COUNT(*) FROM funding_attempts').fetchone()[0], 0)

    def test_account_lookup_releases_writer_and_rejects_local_drift(self):
        self.ready_worker()
        for sql in [
            "UPDATE worker_profiles SET payout_account_id='acct_changed' WHERE user_id=1",
            "UPDATE users SET is_active=0 WHERE id=1",
            "UPDATE services SET price=99 WHERE id=1",
            "UPDATE services SET worker_id=3 WHERE id=1",
        ]:
            with self.subTest(sql=sql):
                with self.api.get_db() as db:
                    db.execute("UPDATE users SET is_active=1 WHERE id=1")
                    db.execute("UPDATE worker_profiles SET payout_account_id='acct_checkout_fixture' WHERE user_id=1")
                    db.execute("UPDATE services SET worker_id=1,price=25 WHERE id=1")
                    db.commit()
                def drift(account_id):
                    import sqlite3
                    with sqlite3.connect(self.api._get_db_path(), timeout=0.1) as writer:
                        writer.execute('BEGIN IMMEDIATE')
                        writer.execute(sql)
                        writer.commit()
                    return {'id': 'acct_checkout_fixture', 'payouts_enabled': True, 'charges_enabled': True,
                            'capabilities': {'transfers': 'active'}}
                self.account_retrieve.side_effect = drift
                status, data = self.request('POST', '/services/1/order', payload={
                    'idempotency_key': 'drift-worker-payout-0001', 'amount': '25.55'})
                self.assertEqual(status, 409, data)
                self.payment_create.assert_not_called()

    def test_committed_replay_survives_worker_and_service_change(self):
        self.ready_worker()
        body = {'idempotency_key': 'committed-worker-replay-0001', 'amount': '25.55'}
        status, created = self.request('POST', '/services/1/order', payload=body)
        self.assertEqual(status, 201, created)
        calls = self.account_retrieve.call_count
        with self.api.get_db() as db:
            db.execute("UPDATE worker_profiles SET payout_account_id=NULL WHERE user_id=1")
            db.execute("UPDATE services SET status='paused',price=99 WHERE id=1")
            db.commit()
        status, replay = self.request('POST', '/services/1/order', payload=body)
        self.assertEqual(status, 200, replay)
        self.assertEqual(created['id'], replay['id'])
        self.assertEqual(self.payment_create.call_count, 1)
        self.assertEqual(self.account_retrieve.call_count, calls)

    def test_missing_payout_retry_and_manual_alias_never_charge(self):
        self.ready_worker()
        self.account_retrieve.return_value = {'id': 'acct_checkout_fixture', 'payouts_enabled': False}
        body = {'idempotency_key': 'pending-worker-retry-0001', 'amount': '25.55'}
        status, _ = self.request('POST', '/services/1/order', payload=body)
        self.assertEqual(status, 409)
        with self.api.get_db() as db:
            order = db.execute('SELECT * FROM orders').fetchone()
            ms = db.execute('SELECT * FROM milestones').fetchone()
            db.execute("UPDATE worker_profiles SET payout_account_id=NULL WHERE user_id=1")
            db.commit()
        status, _ = self.request('POST', '/services/1/order', payload=body)
        self.assertEqual(status, 409)
        with self.api.get_db() as db:
            with self.assertRaises(self.api.FundingConflict):
                self.api.fund_escrow_stripe(db, 2, 25.55, order['id'], ms['id'], funding_identity=f"milestone:{ms['id']}")
        self.payment_create.assert_not_called()
        with self.api.get_db() as db:
            self.assertEqual(db.execute('SELECT COUNT(*) FROM funding_attempts').fetchone()[0], 0)
            self.assertEqual(db.execute('SELECT COUNT(*) FROM escrow_holds').fetchone()[0], 0)

    def test_processor_account_identity_and_local_flags_fail_closed(self):
        self.ready_worker()
        response = dict(self.account_retrieve.return_value)
        for account_id in [None, 'acct_someone_else', 'acct_sim_checkout']:
            self.account_retrieve.return_value = {**response, 'id': account_id}
            status, result = self.request('POST', '/services/1/order', payload={
                'idempotency_key': 'wrong-account-binding-0001', 'amount': '25.55'})
            self.assertEqual(status, 409, result)
        self.payment_create.assert_not_called()
        self.account_retrieve.return_value = response
        for field in ['is_active', 'is_banned', 'is_suspended']:
            with self.api.get_db() as db:
                db.execute('UPDATE users SET is_active=1,is_banned=0,is_suspended=0 WHERE id=1')
                db.execute('UPDATE users SET '+field+'=? WHERE id=1', [0 if field=='is_active' else 1])
                db.commit()
            calls = self.account_retrieve.call_count
            status, result = self.request('POST', '/services/1/order', payload={
                'idempotency_key': 'wrong-account-binding-0001', 'amount': '25.55'})
            self.assertEqual(status, 409, result)
            self.assertEqual(self.account_retrieve.call_count, calls)
        self.payment_create.assert_not_called()

    def test_unfunded_checkout_cannot_charge_after_order_becomes_terminal(self):
        self.ready_worker()
        response = self.account_retrieve.return_value
        self.account_retrieve.return_value = {'id': 'acct_checkout_fixture', 'payouts_enabled': False}
        body = {'idempotency_key': 'terminal-before-funding-0001', 'amount': '25.55'}
        status, _ = self.request('POST', '/services/1/order', payload=body)
        self.assertEqual(status, 409)
        with self.api.get_db() as db:
            db.execute("UPDATE orders SET status='canceled'")
            db.commit()
        self.account_retrieve.return_value = response
        status, result = self.request('POST', '/services/1/order', payload=body)
        self.assertEqual(status, 409, result)
        self.payment_create.assert_not_called()

    def test_buyer_suspension_during_account_readiness_blocks_charge(self):
        self.ready_worker()
        response = self.account_retrieve.return_value
        def suspend(account_id):
            with self.api.get_db() as db:
                db.execute("UPDATE users SET is_suspended=1 WHERE id=2")
                db.commit()
            return response
        self.account_retrieve.side_effect = suspend
        status, result = self.request('POST', '/services/1/order', payload={
            'idempotency_key': 'suspend-buyer-during-0001', 'amount': '25.55'})
        self.assertEqual(status, 409, result)
        self.payment_create.assert_not_called()

    def test_missing_worker_payout_blocks_new_checkout(self):
        status, data = self.request('POST', '/services/1/order', payload={
            'idempotency_key': 'missing-worker-payout-0001', 'amount': '25.55'})
        self.assertEqual(status, 409, data)
        self.payment_create.assert_not_called()
        with self.api.get_db() as db:
            self.assertEqual(db.execute('SELECT COUNT(*) FROM funding_attempts').fetchone()[0], 0)
            self.assertEqual(db.execute('SELECT COUNT(*) FROM escrow_holds').fetchone()[0], 0)


if __name__ == '__main__':
    unittest.main()
