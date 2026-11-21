"""Admin payment-customer delete: remove an erasable buyer's Stripe Customer, fail closed."""
import contextlib
import json
import sqlite3
import unittest
from types import SimpleNamespace
from unittest import mock

import stripe
import test_admin_payout_binding_reset as reset
import test_ai_listing_policy as policy


class AdminPaymentCustomerDeleteTests(unittest.TestCase):
    setUp = policy.AIListingPolicyTests.setUp
    tearDown = policy.AIListingPolicyTests.tearDown
    request = policy.AIListingPolicyTests.request
    payload = reset.AdminPayoutBindingResetTests.payload
    PASSWORD = reset.AdminPayoutBindingResetTests.PASSWORD
    CUSTOMER = 'cus_buyer_abc123'
    SUFFIX = 'abc123'
    PATH = '/admin/users/4/payment-customer/delete'

    def setup_row(self, db, kind, binding, status='committed', processor_object_id=None, result=None,
                  error_code=None, manual=0, user_id=4):
        binding_json, fingerprint = self.core._payment_setup_binding_digest(binding)
        key = f'payment-setup:{user_id}:{kind}:{fingerprint}'
        return db.execute("""INSERT INTO payment_setup_operations
            (operation_key,operation_kind,user_id,request_fingerprint,request_binding_json,
             processor_idempotency_key,status,processor_object_id,result_json,manual_review_required,error_code)
            VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
            [key, kind, user_id, fingerprint, binding_json, key + ':v1', status, processor_object_id,
             None if result is None else json.dumps(result, sort_keys=True, separators=(',', ':')),
             manual, error_code]).lastrowid

    def prepare(self, active=0):
        """The state a real setup-employer + confirm-setup-employer run leaves for buyer 4."""
        with self.core.get_db() as db:
            db.execute('UPDATE users SET password_hash=? WHERE id=5', (self.core.hash_password(self.PASSWORD),))
            db.execute('UPDATE users SET is_active=? WHERE id=4', (active,))
            db.execute("INSERT INTO employer_profiles(user_id,stripe_customer_id,payment_method_id) VALUES (4,?,'pm_card_1')",
                       (self.CUSTOMER,))
            self.setup_row(db, 'customer_create', {'email': 'buyer@example.com', 'name': 'Buyer', 'user_id': 4},
                           processor_object_id=self.CUSTOMER,
                           result={'customer_id': self.CUSTOMER, 'processor_object_id': self.CUSTOMER})
            self.setup_row(db, 'setup_intent_create', {'customer_id': self.CUSTOMER, 'payment_method_types': ['card']},
                           processor_object_id='seti_1', result={'processor_object_id': 'seti_1', 'setup_intent_id': 'seti_1'})
            self.setup_row(db, 'payment_method_attach', {'customer_id': self.CUSTOMER, 'payment_method_id': 'pm_card_1'},
                           processor_object_id='pm_card_1', result={'payment_method_id': 'pm_card_1', 'processor_object_id': 'pm_card_1'})
            self.setup_row(db, 'customer_modify', {'customer_id': self.CUSTOMER, 'default_payment_method': 'pm_card_1'},
                           processor_object_id=self.CUSTOMER,
                           result={'customer_id': self.CUSTOMER, 'payment_method_id': 'pm_card_1',
                                   'processor_object_id': self.CUSTOMER})
            db.commit()

    def set_active(self, active):
        with self.core.get_db() as db:
            db.execute('UPDATE users SET is_active=? WHERE id=4', (active,))
            db.commit()

    def customer(self, **changes):
        result = dict(id=self.CUSTOMER, object='customer', balance=0, metadata={'user_id': '4'})
        result.update(changes)
        return result

    @contextlib.contextmanager
    def stripe_mocks(self, customer=None, intents=None, charges=None):
        with mock.patch.object(self.core, 'stripe_configured', return_value=True), \
             mock.patch.object(self.core.stripe.Customer, 'retrieve',
                               return_value=self.customer() if customer is None else customer) as retrieve, \
             mock.patch.object(self.core.stripe.PaymentIntent, 'list',
                               return_value={'data': []} if intents is None else intents) as intent_list, \
             mock.patch.object(self.core.stripe.Charge, 'list',
                               return_value={'data': []} if charges is None else charges) as charge_list, \
             mock.patch.object(self.core.stripe.Customer, 'delete',
                               return_value={'id': self.CUSTOMER, 'deleted': True}) as delete:
            yield SimpleNamespace(retrieve=retrieve, intents=intent_list, charges=charge_list, delete=delete)

    def call(self, **changes):
        return self.request('POST', self.PATH, self.payload(**changes), 'tok-5')

    def live(self, **changes):
        return self.call(dry_run=False, confirm_customer_suffix=self.SUFFIX, **changes)

    def dump(self):
        # AUTOINCREMENT counters move when a released lock row was inserted; rows are compared exactly.
        with self.core.get_db() as db:
            return '\n'.join(line for line in db.iterdump() if 'sqlite_sequence' not in line)

    def profile(self):
        with self.core.get_db() as db:
            row = db.execute('SELECT stripe_customer_id,payment_method_id FROM employer_profiles WHERE user_id=4').fetchone()
            return tuple(row) if row else None

    def lock_rows(self):
        with self.core.get_db() as db:
            return [tuple(r) for r in db.execute(
                """SELECT status,manual_review_required,processor_object_id,error_code FROM payment_setup_operations
                   WHERE user_id=4 AND operation_kind='admin_payment_customer_delete' ORDER BY id""")]

    def audit_count(self, action):
        with self.core.get_db() as db:
            return db.execute('SELECT COUNT(*) FROM audit_log WHERE action=?', [action]).fetchone()[0]

    def assert_no_stripe(self, m):
        m.retrieve.assert_not_called()
        m.intents.assert_not_called()
        m.charges.assert_not_called()
        m.delete.assert_not_called()

    def test_dry_run_is_eligible_and_read_only(self):
        self.prepare()
        before = self.dump()
        with self.stripe_mocks() as m:
            status, body = self.call()
            self.assertEqual(status, 200, body)
            self.assertEqual(body, {'dry_run': True, 'eligible': True, 'blockers': [],
                                    'customer_id_suffix': self.SUFFIX, 'will_delete_stripe_customer': True})
            m.retrieve.assert_called_once_with(self.CUSTOMER)
            m.intents.assert_called_once_with(customer=self.CUSTOMER, limit=1)
            m.charges.assert_called_once_with(customer=self.CUSTOMER, limit=1)
            m.delete.assert_not_called()
        self.assertEqual(self.dump(), before)
        with self.core.get_db() as db:
            self.assertEqual(db.execute('SELECT COUNT(*) FROM audit_log').fetchone()[0], 0)

    def test_live_run_deletes_customer_and_retires_its_setup_rows(self):
        self.prepare()
        with self.stripe_mocks() as m:
            status, body = self.live()
            self.assertEqual(status, 200, body)
            self.assertEqual(body, {'ok': True, 'dry_run': False, 'stripe_deleted': True,
                                    'customer_id_suffix': self.SUFFIX})
            m.delete.assert_called_once_with(self.CUSTOMER)
        self.assertEqual(self.profile(), (None, None))
        self.assertEqual(self.lock_rows(), [('committed', 0, self.CUSTOMER, 'admin_payment_customer_delete')])
        with self.core.get_db() as db:
            rows = db.execute("""SELECT operation_kind,status,error_code FROM payment_setup_operations
                                 WHERE user_id=4 AND operation_kind!='admin_payment_customer_delete' ORDER BY id""").fetchall()
            self.assertEqual([tuple(r) for r in rows], [
                ('customer_create', 'committed', 'admin_payment_customer_delete'),
                ('setup_intent_create', 'committed', 'admin_payment_customer_delete'),
                ('payment_method_attach', 'committed', 'admin_payment_customer_delete'),
                ('customer_modify', 'committed', 'admin_payment_customer_delete'),
            ])
            lock = db.execute("""SELECT * FROM payment_setup_operations
                                 WHERE operation_kind='admin_payment_customer_delete'""").fetchone()
            self.assertRegex(lock['operation_key'], r'^admin-payment-customer-delete:4:[0-9a-f]{24}$')
            self.assertEqual(lock['processor_idempotency_key'], lock['operation_key'] + ':v1')
            self.assertEqual(json.loads(lock['request_binding_json']), {'customer_id_suffix': self.SUFFIX})
            self.assertIsNotNone(lock['committed_at'])
            audit = db.execute("SELECT user_id,entity_type,entity_id,details FROM audit_log WHERE action='admin_payment_customer_delete'").fetchall()
            self.assertEqual(len(audit), 1)
            self.assertEqual(tuple(audit[0])[:3], (5, 'user', 4))
            self.assertEqual(json.loads(audit[0]['details']), {'old_customer_suffix': self.SUFFIX, 'stripe_deleted': True})
        self.assertFalse(self.core._payment_setup_profile_is_frozen(self.core.get_db(), 4))

    def test_admin_step_up_flags_and_suffix_are_enforced(self):
        self.prepare()
        before = self.profile(), self.lock_rows()
        with self.stripe_mocks() as m:
            status, body = self.request('POST', self.PATH, self.payload(dry_run=False, confirm_customer_suffix=self.SUFFIX), 'tok-3')
            self.assertEqual(status, 403, body)
            status, body = self.request('POST', self.PATH, {'dry_run': False, 'confirm_customer_suffix': self.SUFFIX}, 'tok-5')
            self.assertEqual(status, 403, body)
            self.assertIn('Admin password', body['error'])
            for bad in ('false', 0, 1, None, [], {}):
                with self.subTest(dry_run=bad):
                    status, body = self.call(dry_run=bad, confirm_customer_suffix=self.SUFFIX)
                    self.assertEqual(status, 400, body)
                    self.assertEqual(body['error'], 'dry_run must be a boolean')
            for suffix in ({}, {'confirm_customer_suffix': 'wrong!'}, {'confirm_customer_suffix': 123},
                           {'confirm_customer_suffix': self.CUSTOMER}):
                with self.subTest(suffix=suffix):
                    status, body = self.call(dry_run=False, **suffix)
                    self.assertEqual(status, 409, body)
                    self.assertEqual(body['error'], 'confirm_customer_suffix does not match the bound customer')
            self.assert_no_stripe(m)
        with mock.patch.object(self.core, 'stripe_configured', return_value=False), \
             mock.patch.object(self.core.stripe.Customer, 'retrieve') as retrieve:
            self.assertEqual(self.call()[0], 503)
            retrieve.assert_not_called()
        self.assertEqual((self.profile(), self.lock_rows()), before)
        self.assertEqual(self.audit_count('admin_payment_customer_delete'), 0)

    def test_active_user_is_refused_before_any_stripe_call(self):
        self.prepare(active=1)
        before = self.dump()
        with self.stripe_mocks() as m:
            status, body = self.call()
            self.assertEqual(status, 200, body)
            self.assertEqual(body['blockers'], ['active_user'])
            self.assertFalse(body['eligible'])
            status, body = self.live()
            self.assertEqual(status, 409, body)
            self.assertEqual(body['blockers'], ['active_user'])
            self.assert_no_stripe(m)
        self.assertEqual(self.dump(), before)

    def test_local_history_or_pending_setup_refuses_before_stripe(self):
        self.prepare()
        fixtures = {
            'employer_order': ("INSERT INTO orders (type,worker_id,employer_id,status,total_amount) VALUES ('service_order',3,4,'canceled',25)", 'financial_history'),
            'funding_attempt': ("""INSERT INTO orders (id,type,worker_id,employer_id,status,total_amount) VALUES (77,'service_order',3,1,'pending',25);
                                   INSERT INTO funding_attempts (operation_key,attempt_number,request_fingerprint,processor_idempotency_key,
                                     employer_id,order_id,base_amount_cents,platform_fee_cents,processing_fee_cents,charged_total_cents,status)
                                   VALUES ('fa',1,'fp','fa:v1',4,77,2500,0,0,2500,'failed')""", 'financial_history'),
            'completion_operation': ("""INSERT INTO orders (id,type,worker_id,employer_id,status,total_amount) VALUES (78,'service_order',3,1,'pending',25);
                                        INSERT INTO order_completion_operations (order_id,employer_id,expected_order_status,hold_ids_json,hold_set_sha256,status)
                                        VALUES (78,4,'submitted','[]','x','prepared')""", 'financial_history'),
            'prepared_setup': ("""INSERT INTO payment_setup_operations (operation_key,operation_kind,user_id,request_fingerprint,
                                    request_binding_json,processor_idempotency_key,status) VALUES ('k','setup_intent_create',4,'h','{}','k:v1','prepared')""", 'setup_frozen'),
            'manual_review': ("""INSERT INTO payment_setup_operations (operation_key,operation_kind,user_id,request_fingerprint,
                                   request_binding_json,processor_idempotency_key,status,manual_review_required)
                                   VALUES ('k','payment_method_attach',4,'h','{}','k:v1','failed',1)""", 'setup_frozen'),
            'delete_in_progress': ("""INSERT INTO payment_setup_operations (operation_key,operation_kind,user_id,request_fingerprint,
                                       request_binding_json,processor_idempotency_key,status,processor_object_id,manual_review_required,error_code)
                                       VALUES ('k','admin_payment_customer_delete',4,'h','{}','k:v1','unknown','cus_buyer_abc123',1,
                                               'admin_payment_customer_delete_in_progress')""", 'delete_in_progress'),
        }
        for name, (sql, blocker) in fixtures.items():
            with self.subTest(case=name):
                with self.core.get_db() as db:
                    db.execute("DELETE FROM payment_setup_operations WHERE operation_key='k'")
                    db.execute('DELETE FROM order_completion_operations')
                    db.execute('DELETE FROM funding_attempts')
                    db.execute('DELETE FROM orders')
                    db.commit()
                    db.executescript(sql)
                    db.commit()
                before = self.dump()
                with self.stripe_mocks() as m:
                    status, body = self.call()
                    self.assertEqual(status, 200, body)
                    self.assertEqual(body['blockers'], [blocker])
                    status, body = self.live()
                    self.assertEqual(status, 409, body)
                    self.assertEqual(body['blockers'], [blocker])
                    self.assertFalse(body['eligible'])
                    self.assert_no_stripe(m)
                self.assertEqual(self.dump(), before)

    def test_stripe_checks_fail_closed(self):
        self.prepare()
        before = self.dump()
        cases = [
            ('retrieve_raises', dict(customer=stripe.APIConnectionError('offline')), 'customer_unverified'),
            ('retrieve_runtime_error', dict(customer=RuntimeError('bad response')), 'customer_unverified'),
            ('deleted', dict(customer={'id': self.CUSTOMER, 'object': 'customer', 'deleted': True}), 'customer_unverified'),
            ('id_mismatch', dict(customer=self.customer(id='cus_other_000001')), 'customer_id_mismatch'),
            ('owner_mismatch', dict(customer=self.customer(metadata={'user_id': '42'})), 'customer_owner_mismatch'),
            ('owner_missing', dict(customer=self.customer(metadata={})), 'customer_owner_mismatch'),
            ('balance', dict(customer=self.customer(balance=100)), 'nonzero_customer_balance'),
            ('negative_balance', dict(customer=self.customer(balance=-5)), 'nonzero_customer_balance'),
            ('balance_none', dict(customer=self.customer(balance=None)), 'customer_unverified'),
            ('balance_bool', dict(customer=self.customer(balance=False)), 'customer_unverified'),
            ('intent_present', dict(intents={'data': [{'id': 'pi_1'}]}), 'stripe_payments_present'),
            ('intent_object', dict(intents=SimpleNamespace(data=[SimpleNamespace(id='pi_1')])), 'stripe_payments_present'),
            ('charge_present', dict(charges={'data': [{'id': 'ch_1'}]}), 'stripe_payments_present'),
            ('intent_raises', dict(intents=stripe.APIConnectionError('offline')), 'payments_unverified'),
            ('charge_raises', dict(charges=RuntimeError('bad response')), 'payments_unverified'),
            ('intent_malformed', dict(intents={'data': None}), 'payments_unverified'),
            ('charge_malformed', dict(charges={}), 'payments_unverified'),
            ('charge_string', dict(charges={'data': 'ch_1'}), 'payments_unverified'),
        ]
        for name, values, blocker in cases:
            with self.subTest(case=name):
                errors = {k: v for k, v in values.items() if isinstance(v, Exception)}
                with self.stripe_mocks(**{k: v for k, v in values.items() if k not in errors}) as m:
                    for key, error in errors.items():
                        {'customer': m.retrieve, 'intents': m.intents, 'charges': m.charges}[key].side_effect = error
                    status, body = self.call()
                    self.assertEqual(status, 200, body)
                    self.assertFalse(body['eligible'])
                    self.assertEqual(body['blockers'], [blocker])
                    status, body = self.live()
                    self.assertEqual(status, 409, body)
                    self.assertEqual(body['blockers'], [blocker])
                    m.delete.assert_not_called()
                self.assertEqual(self.dump(), before)

    def test_definitive_delete_refusal_releases_lock_and_keeps_binding(self):
        self.prepare()
        before = self.dump()
        with self.stripe_mocks() as m:
            m.delete.side_effect = stripe.InvalidRequestError('cannot delete', param='id')
            status, body = self.live()
            self.assertEqual(status, 409, body)
            self.assertEqual(body['error'], 'Stripe customer could not be deleted; binding unchanged')
            m.delete.assert_called_once_with(self.CUSTOMER)
        self.assertEqual(self.dump(), before)

    def test_unverified_delete_result_releases_lock(self):
        self.prepare()
        before = self.dump()
        for result in ({'id': self.CUSTOMER, 'deleted': False}, {'id': self.CUSTOMER}, {'deleted': 'true'}, None):
            with self.subTest(result=result), self.stripe_mocks() as m:
                m.delete.return_value = result
                status, body = self.live()
                self.assertEqual(status, 409, body)
                self.assertEqual(body['error'], 'Stripe customer deletion could not be verified; binding unchanged')
            self.assertEqual(self.dump(), before)

    def test_unknown_delete_outcome_freezes_payment_setup(self):
        self.prepare()
        with self.stripe_mocks() as m:
            m.delete.side_effect = stripe.APIConnectionError('timeout')
            status, body = self.live()
            self.assertEqual(status, 503, body)
            self.assertEqual(body['error'], 'Stripe customer deletion could not be verified; binding unchanged')
        self.assertEqual(self.profile(), (self.CUSTOMER, 'pm_card_1'))
        self.assertEqual(self.lock_rows(), [('unknown', 1, self.CUSTOMER, 'admin_payment_customer_delete_in_progress')])
        self.set_active(1)
        with mock.patch.object(self.core, 'stripe_configured', return_value=True), \
             mock.patch.object(self.core.stripe.SetupIntent, 'create') as setup_intent, \
             mock.patch.object(self.core.stripe.Customer, 'create') as create:
            status, body = self.request('POST', '/payments/setup-employer', {}, 'tok-4')
            self.assertEqual(status, 409, body)
            self.assertIn('frozen', body['error'])
            status, body = self.request('POST', '/payments/confirm-setup-employer', {'payment_method_id': 'pm_new'}, 'tok-4')
            self.assertEqual(status, 409, body)
            setup_intent.assert_not_called()
            create.assert_not_called()
        self.set_active(0)
        # The unresolved lock also blocks a second delete before any Stripe call.
        with self.stripe_mocks() as m:
            status, body = self.call()
            self.assertEqual(body['blockers'], ['delete_in_progress'])
            self.assertEqual(self.live()[0], 409)
            self.assert_no_stripe(m)

    def test_reactivation_during_delete_prevents_local_clear(self):
        self.prepare()

        def reactivate_then_deleted(*args):
            self.set_active(1)
            return {'id': self.CUSTOMER, 'deleted': True}
        with self.stripe_mocks() as m:
            m.delete.side_effect = reactivate_then_deleted
            status, body = self.live()
            m.delete.assert_called_once_with(self.CUSTOMER)
        self.assertEqual(status, 409, body)
        self.assertEqual(body, {'error': 'binding_changed', 'stripe_deleted': True})
        self.assertEqual(self.profile(), (self.CUSTOMER, 'pm_card_1'))
        self.assertEqual(self.lock_rows(), [('unknown', 1, self.CUSTOMER, 'admin_payment_customer_delete_in_progress')])
        self.assertEqual(self.audit_count('admin_payment_customer_delete'), 0)
        with self.core.get_db() as db:
            details = db.execute("""SELECT details FROM audit_log
                                    WHERE action='admin_payment_customer_delete_binding_changed'""").fetchall()
            self.assertEqual([json.loads(d[0]) for d in details], [{'old_customer_suffix': self.SUFFIX, 'stripe_deleted': True}])
            retired = db.execute("SELECT COUNT(*) FROM payment_setup_operations WHERE error_code='admin_payment_customer_delete'").fetchone()[0]
            self.assertEqual(retired, 0)

    def test_stripe_io_runs_without_a_writer_lock(self):
        self.prepare()
        route_connections = []
        real_get_db = self.core.get_db

        def track_db():
            conn = real_get_db()
            route_connections.append(conn)
            return conn
        seen = []

        def probe(name, value):
            def run(*args, **kwargs):
                seen.append(name)
                self.assertFalse(route_connections[-1].in_transaction)
                with sqlite3.connect(self.core._get_db_path(), timeout=0.2) as contender:
                    contender.execute('BEGIN IMMEDIATE')
                    contender.rollback()
                return value
            return run
        with mock.patch.object(self.core, 'get_db', side_effect=track_db), self.stripe_mocks() as m:
            m.retrieve.side_effect = probe('retrieve', self.customer())
            m.intents.side_effect = probe('intents', {'data': []})
            m.charges.side_effect = probe('charges', {'data': []})
            m.delete.side_effect = probe('delete', {'id': self.CUSTOMER, 'deleted': True})
            status, body = self.live()
            self.assertEqual(status, 200, body)
        self.assertEqual(seen, ['retrieve', 'intents', 'charges', 'delete'])
        self.assertEqual(self.profile(), (None, None))

    def test_simulated_or_missing_customer_never_reaches_stripe(self):
        self.prepare()
        for value in ('cus_sim_abc123', '', None, 'pm_card_1', 'acct_abc123'):
            with self.subTest(value=value):
                with self.core.get_db() as db:
                    db.execute('UPDATE employer_profiles SET stripe_customer_id=? WHERE user_id=4', (value,))
                    db.commit()
                before = self.dump()
                with self.stripe_mocks() as m:
                    for body in ({}, {'dry_run': False, 'confirm_customer_suffix': (value or '')[-6:]}):
                        status, result = self.call(**body)
                        self.assertEqual(status, 409, result)
                        self.assertEqual(result['error'], 'No live payment customer to delete')
                    self.assert_no_stripe(m)
                self.assertEqual(self.dump(), before)
        with self.core.get_db() as db:
            db.execute('DELETE FROM employer_profiles WHERE user_id=4')
            db.commit()
        with self.stripe_mocks() as m:
            self.assertEqual(self.call()[0], 409)
            self.assert_no_stripe(m)

    def test_setup_after_delete_creates_a_fresh_customer(self):
        self.prepare()
        with self.core.get_db() as db:
            old_key = db.execute("SELECT processor_idempotency_key FROM payment_setup_operations WHERE operation_kind='customer_create'").fetchone()[0]
        with self.stripe_mocks():
            self.assertEqual(self.live()[0], 200)
        self.set_active(1)
        with mock.patch.object(self.core, 'stripe_configured', return_value=True), \
             mock.patch.object(self.core.stripe.Customer, 'create', return_value=SimpleNamespace(id='cus_new_654321')) as create, \
             mock.patch.object(self.core.stripe.SetupIntent, 'create',
                               return_value=SimpleNamespace(id='seti_new', client_secret='seti_new_secret')) as setup_intent:
            status, body = self.request('POST', '/payments/setup-employer', {}, 'tok-4')
            self.assertEqual(status, 200, body)
            self.assertEqual(body['customer_id'], 'cus_new_654321')
            create.assert_called_once()
            self.assertNotEqual(create.call_args.kwargs['idempotency_key'], old_key)
            self.assertEqual(setup_intent.call_args.kwargs['customer'], 'cus_new_654321')
        self.assertEqual(self.profile(), ('cus_new_654321', None))
        with self.core.get_db() as db:
            rows = db.execute("""SELECT request_binding_json,status,error_code,processor_object_id FROM payment_setup_operations
                                 WHERE user_id=4 AND operation_kind='customer_create' ORDER BY id""").fetchall()
            self.assertEqual(len(rows), 2)
            self.assertEqual(json.loads(rows[1][0]), {'delete_generation': 1, 'email': 'buyer@example.com',
                                                      'name': 'Buyer', 'user_id': 4})
            self.assertEqual(tuple(rows[1])[1:], ('committed', None, 'cus_new_654321'))


if __name__ == '__main__':
    unittest.main()
