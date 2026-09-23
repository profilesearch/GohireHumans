"""Admin payout binding reset: fail closed before deleting an unfinished account."""
import json
import sqlite3
import unittest
from types import SimpleNamespace
from unittest import mock

import stripe
import test_ai_listing_policy as policy


class AdminPayoutBindingResetTests(unittest.TestCase):
    setUp = policy.AIListingPolicyTests.setUp
    tearDown = policy.AIListingPolicyTests.tearDown
    request = policy.AIListingPolicyTests.request
    ACCOUNT = 'acct_old_us_123456'
    PASSWORD = 'Admin-Test-Password-123!'

    def prepare(self):
        with self.core.get_db() as db:
            db.execute('UPDATE users SET password_hash=? WHERE id=5',
                       (self.core.hash_password(self.PASSWORD),))
            db.execute('''UPDATE worker_profiles SET payout_account_id=?,payout_method='stripe_connect_pending',
                          payout_account_country='US',payout_service_agreement='full',payout_method_details='{}'
                          WHERE user_id=3''', (self.ACCOUNT,))
            db.commit()

    def account(self, **changes):
        result = dict(id=self.ACCOUNT, country='US', details_submitted=False,
                      payouts_enabled=False, charges_enabled=False,
                      capabilities={'transfers': 'pending'}, metadata={'user_id': '3'})
        result.update(changes)
        return result

    def payload(self, **changes):
        result = dict(admin_password=self.PASSWORD)
        result.update(changes)
        return result

    def reset(self, **changes):
        return self.request('POST', '/admin/users/3/payout-binding/reset',
                            self.payload(**changes), 'tok-5')

    def snapshot(self):
        with self.core.get_db() as db:
            profile = tuple(db.execute('''SELECT payout_account_id,payout_method,payout_account_country,
                                    payout_service_agreement,payout_method_details FROM worker_profiles
                                    WHERE user_id=3''').fetchone())
            operations = [tuple(row) for row in db.execute('''SELECT operation_key,status,error_code FROM
                                              payment_setup_operations WHERE user_id=3 ORDER BY id''')]
            audits = db.execute("SELECT COUNT(*) FROM audit_log WHERE action='admin_payout_binding_reset'").fetchone()[0]
            return profile, operations, audits

    def mocks(self, account=None, balance=None):
        account = self.account() if account is None else account
        balance = {'available': [{'amount': 0}], 'pending': [{'amount': 0}]} if balance is None else balance
        return (mock.patch.object(self.core, 'stripe_configured', return_value=True),
                mock.patch.object(self.core, 'retrieve_live_connect_account', return_value=account),
                mock.patch.object(self.core.stripe.Balance, 'retrieve', return_value=balance),
                mock.patch.object(self.core.stripe.Account, 'delete', return_value={'deleted': True}))

    def test_admin_step_up_and_dry_run_are_read_only(self):
        self.prepare()
        before = self.snapshot()
        p_config, p_retrieve, p_balance, p_delete = self.mocks()
        with p_config, p_retrieve as retrieve, p_balance as balance, p_delete as delete:
            self.assertEqual(self.request('POST', '/admin/users/3/payout-binding/reset',
                                          self.payload(dry_run=False, confirm_account_suffix='123456'), 'tok-3')[0], 403)
            status, body = self.request('POST', '/admin/users/3/payout-binding/reset', {}, 'tok-5')
            self.assertEqual(status, 403, body)
            self.assertIn('Admin password', body['error'])
            self.assertEqual(retrieve.call_count, 0)
            status, body = self.reset()
            self.assertEqual(status, 200, body)
            self.assertEqual(body, {'dry_run': True, 'eligible': True, 'blockers': [],
                                    'account_id_suffix': '123456', 'account_country': 'US',
                                    'details_submitted': False, 'payouts_enabled': False,
                                    'will_delete_stripe_account': True})
            retrieve.assert_called_once_with(self.ACCOUNT)
            balance.assert_called_once_with(stripe_account=self.ACCOUNT)
            delete.assert_not_called()
        self.assertEqual(self.snapshot(), before)

    def test_refuses_unsafe_stripe_account_or_balance(self):
        self.prepare()
        cases = [
            (self.account(details_submitted=True), None, 'details_submitted'),
            (self.account(payouts_enabled=True), None, 'payouts_enabled'),
            (self.account(charges_enabled=True), None, 'charges_enabled'),
            (self.account(capabilities={'transfers': 'active'}), None, 'transfers_active'),
            (self.account(metadata={'user_id': '42'}), None, 'account_owner_mismatch'),
            (self.account(id='acct_other'), None, 'account_id_mismatch'),
            (self.account(country='DE'), None, 'account_country_mismatch'),
            (self.account(), {'available': [{'amount': 100}], 'pending': []}, 'nonzero_balance'),
        ]
        before = self.snapshot()
        for account, balance_value, blocker in cases:
            with self.subTest(blocker=blocker):
                p_config, p_retrieve, p_balance, p_delete = self.mocks(account, balance_value)
                with p_config, p_retrieve, p_balance, p_delete as delete:
                    status, body = self.reset()
                    self.assertEqual(status, 200, body)
                    self.assertFalse(body['eligible'])
                    self.assertIn(blocker, body['blockers'])
                    self.assertEqual(self.reset(dry_run=False, confirm_account_suffix='123456')[0], 409)
                    delete.assert_not_called()
                self.assertEqual(self.snapshot(), before)

    def test_balance_retrieval_failure_refuses(self):
        self.prepare()
        before = self.snapshot()
        for failure in (stripe.APIConnectionError('offline'), RuntimeError('invalid Stripe response')):
            p_config, p_retrieve, p_balance, p_delete = self.mocks()
            with p_config, p_retrieve, p_balance as balance, p_delete as delete:
                balance.side_effect = failure
                status, body = self.reset()
                self.assertEqual(status, 200, body)
                self.assertIn('balance_unverified', body['blockers'])
                self.assertEqual(self.reset(dry_run=False, confirm_account_suffix='123456')[0], 409)
                delete.assert_not_called()
        self.assertEqual(self.snapshot(), before)

    def test_financial_history_frozen_and_pending_operations_refuse_before_stripe(self):
        self.prepare()
        fixtures = [
            ("INSERT INTO orders (type,worker_id,employer_id,status,total_amount) VALUES ('service_order',3,4,'pending',25)", 'financial_history'),
            ("INSERT INTO orders (type,worker_id,employer_id,status,total_amount) VALUES ('service_order',3,4,'completed',25)", 'financial_history'),
            ("INSERT INTO orders (type,worker_id,employer_id,status,total_amount) VALUES ('service_order',3,4,'disputed',25)", 'financial_history'),
            ("INSERT INTO payment_setup_operations (operation_key,operation_kind,user_id,request_fingerprint,request_binding_json,processor_idempotency_key,status) VALUES ('k','account_create',3,'hash','{}','ik','prepared')", 'setup_operation_pending'),
            ("INSERT INTO payment_setup_operations (operation_key,operation_kind,user_id,request_fingerprint,request_binding_json,processor_idempotency_key,status,manual_review_required) VALUES ('k','account_create',3,'hash','{}','ik','unknown',1)", 'setup_frozen'),
        ]
        for sql, blocker in fixtures:
            with self.subTest(blocker=blocker, sql=sql):
                with self.core.get_db() as db:
                    db.execute(sql)
                    db.commit()
                p_config, p_retrieve, p_balance, p_delete = self.mocks()
                with p_config, p_retrieve as retrieve, p_balance as balance, p_delete as delete:
                    status, body = self.reset(dry_run=False, confirm_account_suffix='123456')
                    self.assertEqual(status, 409, body)
                    self.assertIn(blocker, body['blockers'])
                    retrieve.assert_not_called()
                    balance.assert_not_called()
                    delete.assert_not_called()
                with self.core.get_db() as db:
                    db.execute('DELETE FROM orders WHERE worker_id=3')
                    db.execute('DELETE FROM payment_setup_operations WHERE user_id=3')
                    db.commit()

    def test_transfer_or_hold_history_refuses(self):
        self.prepare()
        with self.core.get_db() as db:
            db.execute("INSERT INTO orders (id,type,worker_id,employer_id,status,total_amount) VALUES (99,'service_order',3,4,'completed',25)")
            db.execute("INSERT INTO payout_transfers (order_id,worker_id,amount,transfer_type,idempotency_key) VALUES (99,3,25,'milestone','test-transfer')")
            db.commit()
        p_config, p_retrieve, p_balance, p_delete = self.mocks()
        with p_config, p_retrieve as retrieve, p_balance, p_delete:
            status, body = self.reset()
            self.assertEqual(status, 200, body)
            self.assertIn('financial_history', body['blockers'])
            retrieve.assert_not_called()

    def test_wrong_suffix_and_invalid_flags_cannot_delete(self):
        self.prepare()
        before = self.snapshot()
        p_config, p_retrieve, p_balance, p_delete = self.mocks()
        with p_config, p_retrieve as retrieve, p_balance as balance, p_delete as delete:
            status, body = self.reset(dry_run=False, confirm_account_suffix='wrong!')
            self.assertEqual(status, 409, body)
            retrieve.assert_not_called()
            for field in ('dry_run', 'delete_stripe_account'):
                self.assertEqual(self.reset(**{field: 'false'})[0], 400)
            delete.assert_not_called()
        self.assertEqual(self.snapshot(), before)

    def test_delete_failure_keeps_binding_and_ledger(self):
        self.prepare()
        before = self.snapshot()
        p_config, p_retrieve, p_balance, p_delete = self.mocks()
        with p_config, p_retrieve, p_balance, p_delete as delete:
            delete.side_effect = stripe.InvalidRequestError('cannot delete', param='id')
            status, body = self.reset(dry_run=False, confirm_account_suffix='123456')
            self.assertEqual(status, 409, body)
        self.assertEqual(self.snapshot(), before)

    def test_delete_first_clears_binding_and_new_de_setup_does_not_replay_us(self):
        self.prepare()
        self.core.CONNECT_INTERNATIONAL_ENABLED = True
        # Account.create previously committed a US identity; resetting must retire it.
        with self.core.get_db() as db:
            db.execute("""INSERT INTO payment_setup_operations
                (operation_key,operation_kind,user_id,request_fingerprint,request_binding_json,
                 processor_idempotency_key,status,processor_object_id,result_json)
                VALUES ('old-us','account_create',3,'hash',?,'old-us:v1','committed',?,?)""",
                (json.dumps({'country': 'US', 'agreement': 'full'}), self.ACCOUNT,
                 json.dumps({'account_id': self.ACCOUNT, 'processor_object_id': self.ACCOUNT})))
            db.commit()
        route_connections = []
        real_get_db = self.core.get_db
        def track_db():
            conn = real_get_db()
            route_connections.append(conn)
            return conn
        def probe(*args, **kwargs):
            self.assertFalse(route_connections[-1].in_transaction)
            with sqlite3.connect(self.core._get_db_path(), timeout=0.2) as contender:
                contender.execute('BEGIN IMMEDIATE')
                contender.rollback()
        p_config, p_retrieve, p_balance, p_delete = self.mocks()
        with mock.patch.object(self.core, 'get_db', side_effect=track_db), p_config, \
             p_retrieve as retrieve, p_balance as balance, p_delete as delete:
            retrieve.side_effect = lambda *a: (probe(), self.account())[1]
            balance.side_effect = lambda **kw: (probe(), {'available': [], 'pending': []})[1]
            delete.side_effect = lambda *a: (probe(), {'deleted': True})[1]
            status, body = self.reset(dry_run=False, confirm_account_suffix='123456')
            self.assertEqual(status, 200, body)
            self.assertEqual(body['stripe_deleted'], True)
            delete.assert_called_once_with(self.ACCOUNT)
        with self.core.get_db() as db:
            row = db.execute('SELECT payout_account_id,payout_method,payout_account_country,payout_service_agreement,payout_method_details FROM worker_profiles WHERE user_id=3').fetchone()
            self.assertEqual(tuple(row), (None, 'pending_setup', None, None, None))
            old = db.execute("SELECT status,error_code FROM payment_setup_operations WHERE operation_key='old-us'").fetchone()
            self.assertEqual(tuple(old), ('committed', 'admin_payout_binding_reset'))
            audit = db.execute("SELECT details FROM audit_log WHERE action='admin_payout_binding_reset'").fetchone()
            self.assertEqual(json.loads(audit[0]), {'old_account_suffix': '123456', 'old_country': 'US', 'stripe_deleted': True})
        def create(**kwargs):
            self.assertEqual(kwargs['country'], 'DE')
            self.assertEqual(kwargs['capabilities'], {'card_payments': {'requested': True}, 'transfers': {'requested': True}})
            return SimpleNamespace(id='acct_new_de_654321')
        with mock.patch.object(self.core, 'stripe_configured', return_value=True), \
             mock.patch.object(self.core.stripe.Account, 'create', side_effect=create) as created, \
             mock.patch.object(self.core.stripe.AccountLink, 'create', return_value=SimpleNamespace(url='https://example.invalid/onboard', expires_at=9999999999)):
            status, body = self.request('POST', '/payments/setup-worker', {'country': 'DE'}, 'tok-3')
            self.assertEqual(status, 200, body)
            self.assertEqual(body['account_id'], 'acct_new_de_654321')
            created.assert_called_once()
    def test_reset_then_same_country_uses_fresh_account_create_identity(self):
        self.prepare()
        with self.core.get_db() as db:
            db.execute('UPDATE worker_profiles SET payout_account_id=NULL WHERE user_id=3')
            db.commit()
        with mock.patch.object(self.core, 'stripe_configured', return_value=True), \
             mock.patch.object(self.core.stripe.Account, 'create', return_value=SimpleNamespace(id=self.ACCOUNT)), \
             mock.patch.object(self.core.stripe.AccountLink, 'create', return_value=SimpleNamespace(url='https://example.invalid/onboard', expires_at=9999999999)):
            status, body = self.request('POST', '/payments/setup-worker', {'country': 'US'}, 'tok-3')
            self.assertEqual(status, 200, body)
        p_config, p_retrieve, p_balance, p_delete = self.mocks()
        with p_config, p_retrieve, p_balance, p_delete:
            self.assertEqual(self.reset(dry_run=False, confirm_account_suffix='123456')[0], 200)
        with self.core.get_db() as db:
            retired = db.execute("SELECT operation_kind,error_code FROM payment_setup_operations WHERE user_id=3 ORDER BY id").fetchall()
            self.assertEqual([(row['operation_kind'], row['error_code']) for row in retired],
                             [('account_create', 'admin_payout_binding_reset'),
                              ('account_link_create', 'admin_payout_binding_reset'),
                              ('admin_payout_binding_reset', 'admin_payout_binding_reset')])
        def create(**kwargs):
            self.assertEqual(kwargs['country'], 'US')
            self.assertEqual(kwargs['capabilities'], {'transfers': {'requested': True}})
            return SimpleNamespace(id='acct_new_us_654321')
        with mock.patch.object(self.core, 'stripe_configured', return_value=True), \
             mock.patch.object(self.core.stripe.Account, 'create', side_effect=create) as created, \
             mock.patch.object(self.core.stripe.AccountLink, 'create', return_value=SimpleNamespace(url='https://example.invalid/onboard', expires_at=9999999999)):
            status, body = self.request('POST', '/payments/setup-worker', {'country': 'US'}, 'tok-3')
        self.assertEqual(status, 200, body)
        self.assertEqual(body['account_id'], 'acct_new_us_654321')
        created.assert_called_once()

    def test_binding_changed_after_stripe_delete_is_audited_and_not_cleared(self):
        self.prepare()
        new_id = 'acct_rebound_654321'
        def rebind_then_deleted(*args):
            with self.core.get_db() as db:
                db.execute('UPDATE worker_profiles SET payout_account_id=? WHERE user_id=3', (new_id,))
                db.commit()
            return {'deleted': True}
        p_config, p_retrieve, p_balance, p_delete = self.mocks()
        with p_config, p_retrieve, p_balance, p_delete as delete:
            delete.side_effect = rebind_then_deleted
            status, body = self.reset(dry_run=False, confirm_account_suffix='123456')
        self.assertEqual(status, 409, body)
        self.assertEqual(body, {'error': 'binding_changed', 'stripe_deleted': True})
        self.assertEqual(self.snapshot()[0][0], new_id)
        with self.core.get_db() as db:
            self.assertEqual(db.execute("SELECT COUNT(*) FROM audit_log WHERE action='admin_payout_binding_reset_binding_changed'").fetchone()[0], 1)

    def test_apply_without_stripe_delete_requires_confirmation_and_clears_only_binding(self):
        self.prepare()
        p_config, p_retrieve, p_balance, p_delete = self.mocks()
        with p_config, p_retrieve, p_balance, p_delete as delete:
            status, body = self.reset(dry_run=False, delete_stripe_account=False,
                                      confirm_account_suffix='123456')
            self.assertEqual(status, 200, body)
            self.assertFalse(body['stripe_deleted'])
            delete.assert_not_called()
        self.assertEqual(self.snapshot()[0], (None, 'pending_setup', None, None, None))
    def test_canceled_unfunded_order_is_not_financial_history_but_milestone_is(self):
        self.prepare()
        with self.core.get_db() as db:
            db.execute("INSERT INTO orders (id,type,worker_id,employer_id,status,total_amount) VALUES (99,'service_order',3,4,'canceled',25)")
            db.commit()
        p_config, p_retrieve, p_balance, p_delete = self.mocks()
        with p_config, p_retrieve, p_balance, p_delete as delete:
            status, body = self.reset()
            self.assertEqual(status, 200, body)
            self.assertTrue(body['eligible'])
            delete.assert_not_called()
        with self.core.get_db() as db:
            db.execute("INSERT INTO milestones (order_id,title,amount) VALUES (99,'Work',25)")
            db.commit()
        with p_config, p_retrieve, p_balance, p_delete as delete:
            status, body = self.reset(dry_run=False, confirm_account_suffix='123456')
            self.assertEqual(status, 409, body)
            self.assertIn('financial_history', body['blockers'])
            delete.assert_not_called()

    def test_financial_history_appearing_during_delete_prevents_local_clear(self):
        self.prepare()
        def add_order_then_deleted(*args):
            with self.core.get_db() as db:
                db.execute("INSERT INTO orders (type,worker_id,employer_id,status,total_amount) VALUES ('service_order',3,4,'pending',25)")
                db.commit()
            return {'deleted': True}
        p_config, p_retrieve, p_balance, p_delete = self.mocks()
        with p_config, p_retrieve, p_balance, p_delete as delete:
            delete.side_effect = add_order_then_deleted
            status, body = self.reset(dry_run=False, confirm_account_suffix='123456')
        self.assertEqual(status, 409, body)
        self.assertEqual(body, {'error': 'binding_changed', 'stripe_deleted': True})
        self.assertEqual(self.snapshot()[0][0], self.ACCOUNT)
    def test_simulated_or_missing_binding_never_reaches_stripe(self):
        self.prepare()
        p_config, p_retrieve, p_balance, p_delete = self.mocks()
        with p_config, p_retrieve as retrieve, p_balance as balance, p_delete as delete:
            for account_id in (None, 'acct_sim_123456'):
                with self.core.get_db() as db:
                    db.execute('UPDATE worker_profiles SET payout_account_id=? WHERE user_id=3', (account_id,))
                    db.commit()
                self.assertEqual(self.reset(dry_run=False, confirm_account_suffix='123456')[0], 409)
            retrieve.assert_not_called()
            balance.assert_not_called()
            delete.assert_not_called()

    def test_stripe_retrieve_failure_or_malformed_balance_fails_closed(self):
        self.prepare()
        before = self.snapshot()
        p_config, p_retrieve, p_balance, p_delete = self.mocks()
        with p_config, p_retrieve as retrieve, p_balance as balance, p_delete as delete:
            retrieve.side_effect = stripe.APIConnectionError('offline')
            status, body = self.reset()
            self.assertEqual(status, 200, body)
            self.assertIn('account_unverified', body['blockers'])
            retrieve.side_effect = None
            for invalid in ({'available': [], 'pending': None},
                            {'available': [{'amount': '0'}], 'pending': []}):
                balance.return_value = invalid
                status, body = self.reset()
                self.assertEqual(status, 200, body)
                self.assertIn('balance_unverified', body['blockers'])
            delete.assert_not_called()
        self.assertEqual(self.snapshot(), before)

    def test_delete_response_without_deleted_true_cannot_clear_binding(self):
        self.prepare()
        before = self.snapshot()
        p_config, p_retrieve, p_balance, p_delete = self.mocks()
        with p_config, p_retrieve, p_balance, p_delete as delete:
            delete.return_value = {'deleted': False}
            status, body = self.reset(dry_run=False, confirm_account_suffix='123456')
            self.assertEqual(status, 409, body)
        self.assertEqual(self.snapshot(), before)
    def test_malformed_historical_account_link_cannot_strand_deleted_account(self):
        self.prepare()
        with self.core.get_db() as db:
            db.execute("""INSERT INTO payment_setup_operations
                (operation_key,operation_kind,user_id,request_fingerprint,request_binding_json,
                 processor_idempotency_key,status,processor_object_id,result_json)
                VALUES ('bad-link','account_link_create',3,'hash','not-json','bad-link:v1',
                        'committed','link-old','{}')""")
            db.commit()
        p_config, p_retrieve, p_balance, p_delete = self.mocks()
        with p_config, p_retrieve, p_balance, p_delete as delete:
            status, body = self.reset(dry_run=False, confirm_account_suffix='123456')
            self.assertEqual(status, 200, body)
            delete.assert_called_once()
        self.assertIsNone(self.snapshot()[0][0])


if __name__ == '__main__':
    unittest.main()


class AdminPayoutBindingResetRaceRegressions(AdminPayoutBindingResetTests):
    """Blockers from independent review of 4335ef4."""

    def test_setup_worker_during_delete_is_refused_and_reset_completes(self):
        self.prepare()
        self.core.CONNECT_INTERNATIONAL_ENABLED = True
        seen = {}
        def setup_during_delete(*args):
            with mock.patch.object(self.core.stripe.AccountLink, 'create') as link, \
                 mock.patch.object(self.core.stripe.Account, 'create') as create:
                seen['setup'] = self.request('POST', '/payments/setup-worker', {'country': 'US'}, 'tok-3')
                seen['stripe_calls'] = link.call_count + create.call_count
            return {'deleted': True}
        p_config, p_retrieve, p_balance, p_delete = self.mocks()
        with p_config, p_retrieve, p_balance, p_delete as delete:
            delete.side_effect = setup_during_delete
            status, body = self.reset(dry_run=False, confirm_account_suffix='123456')
        self.assertEqual(status, 200, body)
        self.assertEqual(seen['setup'][0], 409, seen['setup'])
        self.assertEqual(seen['stripe_calls'], 0)
        with self.core.get_db() as db:
            self.assertIsNone(db.execute('SELECT payout_account_id FROM worker_profiles WHERE user_id=3').fetchone()[0])
            ops = [tuple(r) for r in db.execute("SELECT operation_kind,status,manual_review_required FROM payment_setup_operations WHERE user_id=3")]
        self.assertEqual(ops, [('admin_payout_binding_reset', 'committed', 0)])
        self.assertFalse(self.core._payment_setup_profile_is_frozen(self.core.get_db(), 3))
        with mock.patch.object(self.core, 'stripe_configured', return_value=True), \
             mock.patch.object(self.core.stripe.Account, 'create', return_value=SimpleNamespace(id='acct_new_de_654321')) as created, \
             mock.patch.object(self.core.stripe.AccountLink, 'create', return_value=SimpleNamespace(url='https://example.invalid/onboard', expires_at=9999999999)):
            status, body = self.request('POST', '/payments/setup-worker', {'country': 'DE'}, 'tok-3')
        self.assertEqual(status, 200, body)
        self.assertEqual(created.call_args.kwargs['country'], 'DE')

    def test_definitive_delete_refusal_releases_lock(self):
        self.prepare()
        before = self.snapshot()
        p_config, p_retrieve, p_balance, p_delete = self.mocks()
        with p_config, p_retrieve, p_balance, p_delete as delete:
            delete.side_effect = stripe.InvalidRequestError('cannot delete', param='id')
            status, _ = self.reset(dry_run=False, confirm_account_suffix='123456')
        self.assertEqual(status, 409)
        self.assertEqual(self.snapshot(), before)
        self.assertFalse(self.core._payment_setup_profile_is_frozen(self.core.get_db(), 3))

    def test_unknown_delete_outcome_freezes_setup_for_review(self):
        self.prepare()
        p_config, p_retrieve, p_balance, p_delete = self.mocks()
        with p_config, p_retrieve, p_balance, p_delete as delete:
            delete.side_effect = stripe.APIConnectionError('network')
            status, _ = self.reset(dry_run=False, confirm_account_suffix='123456')
        self.assertEqual(status, 503)
        self.assertTrue(self.core._payment_setup_profile_is_frozen(self.core.get_db(), 3))
        with mock.patch.object(self.core, 'stripe_configured', return_value=True), \
             mock.patch.object(self.core.stripe.AccountLink, 'create') as link:
            status, _ = self.request('POST', '/payments/setup-worker', {'country': 'US'}, 'tok-3')
        self.assertEqual(status, 409)
        link.assert_not_called()

    def test_nonzero_instant_or_reserved_balance_blocks_delete(self):
        for bucket in ('instant_available', 'connect_reserved'):
            with self.subTest(bucket=bucket):
                self.prepare()
                balance = {'available': [{'amount': 0}], 'pending': [{'amount': 0}], bucket: [{'amount': 5}]}
                p_config, p_retrieve, p_balance, p_delete = self.mocks(balance=balance)
                with p_config, p_retrieve, p_balance, p_delete as delete:
                    status, body = self.reset(dry_run=False, confirm_account_suffix='123456')
                    self.assertEqual(status, 409, body)
                    self.assertIn('nonzero_balance', body['blockers'])
                    delete.assert_not_called()


class AdminPayoutBindingResetSecondReviewRegressions(AdminPayoutBindingResetTests):
    """Blockers from independent review of 0820d06."""

    def seed_link(self):
        with mock.patch.object(self.core, 'stripe_configured', return_value=True), \
             mock.patch.object(self.core.stripe.AccountLink, 'create',
                               return_value=SimpleNamespace(url='https://example.invalid/seed', expires_at=9999999999)):
            self.assertEqual(self.request('POST', '/payments/setup-worker', {'country': 'US'}, 'tok-3')[0], 200)

    def test_second_reset_cannot_remove_in_flight_lock(self):
        self.prepare()
        self.seed_link()
        seen = {}
        def first_delete(*args):
            # A second admin reset and a worker setup arrive while delete is in flight.
            seen['second'] = self.reset(dry_run=False, confirm_account_suffix='123456')
            with mock.patch.object(self.core.stripe.AccountLink, 'create') as link:
                seen['setup'] = self.request('POST', '/payments/setup-worker', {'country': 'US'}, 'tok-3')
                seen['link_calls'] = link.call_count
            return {'deleted': True}
        p_config, p_retrieve, p_balance, p_delete = self.mocks()
        with p_config, p_retrieve, p_balance, p_delete as delete:
            delete.side_effect = first_delete
            status, body = self.reset(dry_run=False, confirm_account_suffix='123456')
            self.assertEqual(delete.call_count, 1)
        self.assertEqual(status, 200, body)
        self.assertEqual(seen['second'][0], 409, seen['second'])
        self.assertIn('reset_in_progress', seen['second'][1]['blockers'])
        self.assertEqual(seen['setup'][0], 409, seen['setup'])
        self.assertEqual(seen['link_calls'], 0)

    def test_replay_started_before_reset_cannot_return_deleted_account(self):
        self.prepare()
        self.seed_link()
        seen = {}
        p_config, p_retrieve, p_balance, p_delete = self.mocks()
        def replay_link(**kwargs):
            if 'reset' not in seen:
                # The reset completes while this replayed Stripe call is in flight.
                seen['reset'] = self.reset(dry_run=False, confirm_account_suffix='123456')
            return SimpleNamespace(url='https://example.invalid/stale', expires_at=9999999999)
        with p_config, p_retrieve, p_balance, p_delete, \
             mock.patch.object(self.core.stripe.AccountLink, 'create', side_effect=replay_link):
            status, body = self.request('POST', '/payments/setup-worker', {'country': 'US'}, 'tok-3')
        # Since the setup-session fence, a reset during any setup request is refused
        # before deletion; the replay then completes normally for the still-bound account.
        self.assertEqual(seen['reset'][0], 409, seen['reset'])
        self.assertIn('setup_operation_pending', seen['reset'][1]['blockers'])
        self.assertEqual(status, 200, body)
        with self.core.get_db() as db:
            self.assertEqual(db.execute('SELECT payout_account_id FROM worker_profiles WHERE user_id=3').fetchone()[0], self.ACCOUNT)

    def test_replay_marker_fence_discards_result_if_reset_row_appears(self):
        # Defense in depth: the marker check in _payment_setup_operation_serialized
        # still discards a replay result if a reset row appears during the call.
        self.prepare()
        self.seed_link()
        def replay_link(**kwargs):
            with self.core.get_db() as db:
                db.execute("""INSERT INTO payment_setup_operations(operation_key,operation_kind,user_id,request_fingerprint,
                              request_binding_json,processor_idempotency_key,status,manual_review_required)
                              VALUES ('r-lock','admin_payout_binding_reset',3,'x','{}','r-lock:v1','unknown',1)""")
                db.commit()
            return SimpleNamespace(url='https://example.invalid/stale', expires_at=9999999999)
        with mock.patch.object(self.core, 'stripe_configured', return_value=True), \
             mock.patch.object(self.core.stripe.AccountLink, 'create', side_effect=replay_link):
            status, body = self.request('POST', '/payments/setup-worker', {'country': 'US'}, 'tok-3')
        self.assertEqual(status, 409, body)
        self.assertNotIn('stale', json.dumps(body))

    def test_unknown_outcome_lock_blocks_later_resets(self):
        self.prepare()
        p_config, p_retrieve, p_balance, p_delete = self.mocks()
        with p_config, p_retrieve, p_balance, p_delete as delete:
            delete.side_effect = stripe.APIConnectionError('network')
            self.assertEqual(self.reset(dry_run=False, confirm_account_suffix='123456')[0], 503)
            delete.side_effect = None
            delete.return_value = {'deleted': True}
            status, body = self.reset(dry_run=False, confirm_account_suffix='123456')
            self.assertEqual(status, 409, body)
            self.assertIn('reset_in_progress', body['blockers'])
            self.assertEqual(delete.call_count, 1)


class AdminPayoutBindingResetSessionRegressions(AdminPayoutBindingResetTests):
    """Blockers from independent review of d7ff75c: gaps inside the setup route."""

    def run_setup_with_reset_at(self, hook):
        seen = {}
        real_op = self.core._payment_setup_operation
        real_audit = self.core.audit
        def op(db, user_id, kind, *a, **kw):
            if hook == 'before_link' and kind == 'account_link_create' and 'reset' not in seen:
                seen['reset'] = self.reset(dry_run=False, confirm_account_suffix='123456')
            return real_op(db, user_id, kind, *a, **kw)
        def audit(db, uid, action, *a, **kw):
            if hook == 'before_response' and action == 'setup_worker_payout' and 'reset' not in seen:
                seen['reset'] = self.reset(dry_run=False, confirm_account_suffix='123456')
            return real_audit(db, uid, action, *a, **kw)
        p_config, p_retrieve, p_balance, p_delete = self.mocks()
        with p_config, p_retrieve, p_balance, p_delete as delete, \
             mock.patch.object(self.core, '_payment_setup_operation', side_effect=op), \
             mock.patch.object(self.core, 'audit', side_effect=audit), \
             mock.patch.object(self.core.stripe.Account, 'create', return_value=SimpleNamespace(id=self.ACCOUNT)), \
             mock.patch.object(self.core.stripe.AccountLink, 'create',
                               return_value=SimpleNamespace(url='https://example.invalid/onboard', expires_at=9999999999)):
            seen['setup'] = self.request('POST', '/payments/setup-worker', {'country': 'US'}, 'tok-3')
            seen['delete_calls'] = delete.call_count
        return seen

    def assert_reset_refused_and_setup_intact(self, seen):
        self.assertEqual(seen['reset'][0], 409, seen['reset'])
        self.assertIn('setup_operation_pending', seen['reset'][1]['blockers'])
        self.assertEqual(seen['delete_calls'], 0)
        self.assertEqual(seen['setup'][0], 200, seen['setup'])
        with self.core.get_db() as db:
            self.assertEqual(db.execute('SELECT payout_account_id FROM worker_profiles WHERE user_id=3').fetchone()[0], self.ACCOUNT)
            pass
        self.assert_setup_lock_free(3)

    def test_reset_between_account_create_and_link_is_refused(self):
        self.prepare()
        with self.core.get_db() as db:
            db.execute('UPDATE worker_profiles SET payout_account_id=NULL WHERE user_id=3')
            db.commit()
        self.assert_reset_refused_and_setup_intact(self.run_setup_with_reset_at('before_link'))

    def test_reset_between_link_commit_and_response_is_refused(self):
        self.prepare()
        self.assert_reset_refused_and_setup_intact(self.run_setup_with_reset_at('before_response'))

    def test_session_removed_after_failed_setup(self):
        self.prepare()
        with mock.patch.object(self.core, 'stripe_configured', return_value=True), \
             mock.patch.object(self.core.stripe.AccountLink, 'create', side_effect=stripe.InvalidRequestError('bad', param='x')):
            status, _ = self.request('POST', '/payments/setup-worker', {'country': 'US'}, 'tok-3')
        self.assertNotEqual(status, 200)
        with self.core.get_db() as db:
            pass
        self.assert_setup_lock_free(3)

    def assert_setup_lock_free(self, user_id):
        lock = self.core._PayoutBindingLock(user_id)
        self.assertTrue(lock.acquire(exclusive=True), 'setup lock still held')
        lock.release()

    def test_long_running_setup_is_never_treated_as_stale(self):
        # Review of 1560162: a time-based stale rule let a slow live setup be
        # ignored. The OS lock has no expiry; a held shared lock always blocks.
        self.prepare()
        held = self.core._payout_setup_session_begin(self.core.get_db(), 3)
        self.assertIsNotNone(held)
        try:
            p_config, p_retrieve, p_balance, p_delete = self.mocks()
            with p_config, p_retrieve, p_balance, p_delete as delete, \
                 mock.patch.object(self.core.time, 'time', return_value=self.core.time.time() + 86400):
                status, body = self.reset(dry_run=False, confirm_account_suffix='123456')
            self.assertEqual(status, 409, body)
            self.assertIn('setup_operation_pending', body['blockers'])
            self.assertEqual(delete.call_count, 0)
        finally:
            held.release()
        p_config, p_retrieve, p_balance, p_delete = self.mocks()
        with p_config, p_retrieve, p_balance, p_delete:
            status, body = self.reset(dry_run=False, confirm_account_suffix='123456')
        self.assertEqual(status, 200, body)

    def test_setup_refused_while_reset_holds_lock(self):
        self.prepare()
        lock = self.core._PayoutBindingLock(3)
        self.assertTrue(lock.acquire(exclusive=True))
        try:
            with mock.patch.object(self.core, 'stripe_configured', return_value=True), \
                 mock.patch.object(self.core.stripe.AccountLink, 'create') as link:
                status, body = self.request('POST', '/payments/setup-worker', {'country': 'US'}, 'tok-3')
            self.assertEqual(status, 409, body)
            self.assertEqual(link.call_count, 0)
        finally:
            lock.release()
        # Other workers are unaffected by worker 3's lock.
        other = self.core._PayoutBindingLock(4)
        self.assertTrue(other.acquire(exclusive=False))
        other.release()

    def test_lock_released_when_holding_process_dies(self):
        import subprocess, sys, textwrap
        self.prepare()
        child = subprocess.Popen([sys.executable, '-c', textwrap.dedent(f"""
            import fcntl, os, sys, time
            fd = os.open({self.core._PayoutBindingLock(3)._path()!r}, os.O_RDWR | os.O_CREAT, 0o600)
            fcntl.flock(fd, fcntl.LOCK_SH)
            print('held', flush=True)
            time.sleep(60)
        """)], stdout=subprocess.PIPE, text=True)
        try:
            self.assertEqual(child.stdout.readline().strip(), 'held')
            lock = self.core._PayoutBindingLock(3)
            self.assertFalse(lock.acquire(exclusive=True))
            child.kill()
            child.wait(10)
            self.assertTrue(lock.acquire(exclusive=True))
            lock.release()
        finally:
            if child.poll() is None:
                child.kill()
            child.stdout.close()


class AdminPayoutBindingResetCountryDecisionRegression(AdminPayoutBindingResetTests):
    """Review probe on 606e073: setup chose its country from the pre-reset binding
    before taking the lock, then created a new account in that (no longer allowed)
    country after the reset."""

    def test_country_is_decided_under_the_setup_lock(self):
        import threading
        self.prepare()
        with self.core.get_db() as db:
            db.execute("UPDATE worker_profiles SET payout_account_country='DE' WHERE user_id=3")
            db.commit()
        entered, resume, seen = threading.Event(), threading.Event(), {}
        real_begin = self.core._payout_setup_session_begin
        real_is_frozen = self.core._payment_setup_profile_is_frozen

        def pause_then_lock(db, user_id):
            entered.set()
            self.assertTrue(resume.wait(20))
            return real_begin(db, user_id)

        def pause_at_decision(db, user_id):
            # On code that decides the country before locking, this is the first
            # point after that decision; pause here instead.
            if not entered.is_set():
                entered.set()
                self.assertTrue(resume.wait(20))
            return real_is_frozen(db, user_id)

        p_config, p_retrieve, p_balance, p_delete = self.mocks(account=self.account(country='DE'))
        with p_config, p_retrieve, p_balance, p_delete, \
             mock.patch.object(self.core, 'CONNECT_INTERNATIONAL_ENABLED', False), \
             mock.patch.object(self.core, '_payout_setup_session_begin', side_effect=pause_then_lock), \
             mock.patch.object(self.core, '_payment_setup_profile_is_frozen', side_effect=pause_at_decision), \
             mock.patch.object(self.core.stripe.Account, 'create', return_value=SimpleNamespace(id='acct_new_654321')) as create, \
             mock.patch.object(self.core.stripe.AccountLink, 'create',
                               return_value=SimpleNamespace(url='https://example.invalid/new', expires_at=9999999999)):
            worker = threading.Thread(target=lambda: seen.setdefault('setup', self.request('POST', '/payments/setup-worker', {}, 'tok-3')))
            worker.start()
            try:
                self.assertTrue(entered.wait(20))
                seen['reset'] = self.reset(dry_run=False, confirm_account_suffix='123456')
            finally:
                resume.set()
                worker.join(20)
            self.assertEqual(seen['reset'][0], 200, seen['reset'])
            self.assertEqual(seen['setup'][0], 200, seen['setup'])
            self.assertEqual(create.call_count, 1)
            # International payouts are off here: the only allowed country is US.
            self.assertEqual(create.call_args.kwargs['country'], 'US')


class AdminPayoutBindingResetDryRunLockRegression(AdminPayoutBindingResetTests):
    """Review of 65d3c1e: a dry run briefly took the exclusive setup lock, so a
    worker's setup starting at that moment was turned away with 409."""

    def test_dry_run_never_takes_the_setup_lock(self):
        self.prepare()
        acquired = []
        real_acquire = self.core._PayoutBindingLock.acquire

        def record(lock, exclusive):
            acquired.append(exclusive)
            return real_acquire(lock, exclusive)

        p_config, p_retrieve, p_balance, p_delete = self.mocks()
        with p_config, p_retrieve, p_balance, p_delete as delete, \
             mock.patch.object(self.core._PayoutBindingLock, 'acquire', record):
            status, body = self.reset(dry_run=True)
        self.assertEqual(status, 200, body)
        self.assertEqual(acquired, [])
        self.assertEqual(delete.call_count, 0)

    def test_setup_during_dry_run_is_not_rejected(self):
        import threading
        self.prepare()
        entered, resume, seen = threading.Event(), threading.Event(), {}
        real_retrieve = self.core.retrieve_live_connect_account

        def paused_retrieve(*args, **kwargs):
            entered.set()
            self.assertTrue(resume.wait(20))
            return real_retrieve(*args, **kwargs)

        p_config, p_retrieve, p_balance, p_delete = self.mocks()
        with p_config, p_retrieve, p_balance, p_delete as delete, \
             mock.patch.object(self.core.stripe.AccountLink, 'create',
                               return_value=SimpleNamespace(url='https://example.invalid/setup', expires_at=9999999999)):
            with mock.patch.object(self.core, 'retrieve_live_connect_account', side_effect=paused_retrieve):
                dry = threading.Thread(target=lambda: seen.setdefault('dry', self.reset(dry_run=True)))
                dry.start()
                try:
                    self.assertTrue(entered.wait(20))
                    seen['setup'] = self.request('POST', '/payments/setup-worker', {'country': 'US'}, 'tok-3')
                finally:
                    resume.set()
                    dry.join(20)
        self.assertEqual(seen['setup'][0], 200, seen['setup'])
        self.assertEqual(seen['dry'][0], 200, seen['dry'])
        self.assertEqual(delete.call_count, 0)
