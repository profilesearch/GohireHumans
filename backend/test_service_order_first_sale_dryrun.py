"""Local-only first-sale rehearsal: real CGI routes/SQLite, synthetic Stripe boundaries.

No test here connects to Stripe or the production API. Financial defects are
recorded as RED assertions for review, not patched in this module.
"""
import contextlib
import io
import json
import os
import tempfile
import time
import unittest
from types import SimpleNamespace
from unittest import mock

import stripe as stripe_sdk
from test_deep_audit_regressions import load_api_core, parse_cgi_output


class FirstSaleDryRun(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.old_env = {key: os.environ.get(key) for key in ('DATABASE_PATH', 'DISABLE_AUTO_SEED')}
        os.environ['DATABASE_PATH'] = os.path.join(self.tmp.name, 'dryrun.db')
        os.environ['DISABLE_AUTO_SEED'] = '1'
        self.api = load_api_core()
        self.api._db_path_resolved = None
        self.api.init_db()
        self.api.PRODUCTION_MODE = True
        self.api.JOB_HIRING_ENABLED = False
        self.api.STRIPE_AVAILABLE = True
        self.api.STRIPE_SECRET_KEY = 'synthetic-configured-key'
        self.accounts = {}
        self.intent_create = mock.Mock(side_effect=lambda **kw: SimpleNamespace(
            id='pi_dryrun_1', status='succeeded', amount=kw['amount'],
            amount_received=kw['amount'], currency=kw['currency'], metadata=kw['metadata']))
        self.transfer_create = mock.Mock(side_effect=lambda **kw: SimpleNamespace(
            id='tr_dryrun_1', amount=kw['amount'], currency=kw['currency'],
            destination=kw['destination'], metadata=kw['metadata']))
        self.account_create = mock.Mock(side_effect=lambda **kw: SimpleNamespace(id='acct_dryrun_1'))
        self.account_retrieve = mock.Mock(side_effect=lambda account_id: self.accounts.get(account_id, {
            'id': account_id, 'payouts_enabled': True, 'charges_enabled': True,
            'details_submitted': True, 'capabilities': {'transfers': 'active'}}))
        self.api.stripe = SimpleNamespace(
            Customer=SimpleNamespace(create=mock.Mock(return_value=SimpleNamespace(id='cus_dryrun_1')),
                                     modify=mock.Mock(return_value=SimpleNamespace(id='cus_dryrun_1'))),
            SetupIntent=SimpleNamespace(create=mock.Mock(return_value=SimpleNamespace(
                id='seti_dryrun_1', client_secret='synthetic_secret'))),
            PaymentMethod=SimpleNamespace(attach=mock.Mock(return_value=SimpleNamespace(id='pm_dryrun_1'))),
            Account=SimpleNamespace(create=self.account_create, retrieve=self.account_retrieve),
            AccountLink=SimpleNamespace(create=mock.Mock(side_effect=lambda **kw: SimpleNamespace(
                url='https://example.invalid/onboard', expires_at=int(time.time()) + 300))),
            PaymentIntent=SimpleNamespace(create=self.intent_create),
            Transfer=SimpleNamespace(create=self.transfer_create),
            StripeError=stripe_sdk.StripeError,
            APIConnectionError=stripe_sdk.APIConnectionError,
        )
        self.api.STRIPE_ERROR = stripe_sdk.StripeError
        self.mail_patch = mock.patch.object(self.api, 'flush_transactional_notification_emails')
        self.mail_patch.start()

    def tearDown(self):
        self.mail_patch.stop()
        self.tmp.cleanup()
        for key, value in self.old_env.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value

    def request(self, method, path, token='', payload=None):
        for key in ('body_cache', 'raw_body'):
            if hasattr(self.api._request_ctx, key):
                delattr(self.api._request_ctx, key)
        body = json.dumps(payload if payload is not None else {})
        ctx = self.api._request_ctx
        ctx.request_method, ctx.path_info, ctx.query_string = method, path, ''
        ctx.http_authorization = f'Bearer {token}' if token else ''
        ctx.http_x_api_key = ''
        ctx.stdin_data, ctx.content_type = body, 'application/json'
        ctx.content_length, ctx.remote_addr = str(len(body.encode())), '127.0.0.1'
        with contextlib.redirect_stdout(io.StringIO()) as output:
            self.api.handle_request()
        return parse_cgi_output(output.getvalue())

    def ok(self, method, path, token='', payload=None, status=200):
        code, result = self.request(method, path, token, payload)
        self.assertEqual(code, status, (path, result))
        return result

    def register(self, name):
        response = self.ok('POST', '/auth/register', payload={
            'email': f'{name}@independent.example', 'name': name.title(),
            'password': 'synthetic-long-password'}, status=201)
        return response['id'], response['token']

    def buyer_ready(self):
        buyer_id, token = self.register('buyer')
        setup = self.ok('POST', '/payments/setup-employer', token)
        self.assertEqual(setup['mode'], 'live')
        self.ok('POST', '/payments/confirm-setup-employer', token,
                {'payment_method_id': 'pm_dryrun_1'})
        self.assertTrue(self.ok('GET', '/payments/status', token)['employer_ready'])
        return buyer_id, token

    def worker_listing(self, onboard=True):
        worker_id, token = self.register('worker')
        if onboard:
            setup = self.ok('POST', '/payments/setup-worker', token)
            self.assertEqual(setup['account_id'], 'acct_dryrun_1')
            self.assertTrue(self.ok('GET', '/payments/status', token)['worker_ready'])
        listing = self.ok('POST', '/services', token, {
            'title': 'Human accessibility QA', 'description': 'Audit checkout accessibility and provide findings.',
            'category': 'web_development', 'provider_type': 'human',
            'pricing_type': 'fixed', 'price': 199, 'delivery_time_days': 7,
        }, status=201)
        return worker_id, token, listing['id']

    def place(self, buyer_token, service_id, key='service-order-12345678-1234-1234-1234-123456789012'):
        return self.request('POST', f'/services/{service_id}/order', buyer_token,
                            {'notes': 'Review checkout and attach findings', 'idempotency_key': key})

    def funded_order(self, onboard=True):
        buyer_id, buyer = self.buyer_ready()
        worker_id, worker, service = self.worker_listing(onboard=onboard)
        code, order = self.place(buyer, service)
        self.assertEqual(code, 201, order)
        return buyer_id, buyer, worker_id, worker, service, order

    def test_first_sale_revision_acceptance_releases_exactly_once(self):
        buyer_id, buyer, worker_id, worker, service, order = self.funded_order()
        oid = order['id']
        self.assertEqual(order['status'], 'in_progress')
        self.assertEqual(order['total_amount'], 199)
        self.assertEqual(len(order['milestones']), 1)
        detail = self.ok('GET', f'/orders/{oid}', buyer)
        self.assertEqual(detail['milestones'][0]['status'], 'in_progress')
        self.assertTrue(detail['deadline_at'])
        self.assertEqual(detail['funding_summary']['base_cents'], 19900)
        self.assertEqual(detail['funding_summary']['charged_total_cents'], self.intent_create.call_args.kwargs['amount'])
        self.assertEqual(len(detail['escrow_holds']), 1)
        self.assertEqual(detail['escrow_holds'][0]['status'], 'held')
        self.assertEqual(self.ok('GET', '/orders', worker)['total'], 1)
        self.assertEqual(self.ok('GET', '/orders', buyer)['total'], 1)
        self.ok('POST', f'/orders/{oid}/submit', worker, {'notes': 'First delivery attached'})
        self.ok('POST', f'/orders/{oid}/request-revision', buyer,
                {'notes': 'Please check keyboard focus'})
        self.ok('POST', f'/orders/{oid}/submit', worker, {'notes': 'Revised keyboard report'})
        approval = self.ok('POST', f'/orders/{oid}/approve', buyer)
        self.assertEqual(approval['worker_payout'], 199)
        self.assertEqual(self.request('POST', f'/orders/{oid}/approve', buyer)[0], 409)
        self.assertEqual(self.request('POST', f'/orders/{oid}/submit', worker,
                                      {'notes': 'Duplicate delivery'})[0], 409)
        self.intent_create.assert_called_once()
        self.transfer_create.assert_called_once()
        with self.api.get_db() as db:
            self.assertEqual(db.execute('SELECT status FROM orders WHERE id=?', (oid,)).fetchone()[0], 'completed')
            self.assertEqual(db.execute('SELECT status FROM milestones WHERE order_id=?', (oid,)).fetchone()[0], 'approved')
            hold = db.execute('SELECT status,amount,stripe_transfer_id FROM escrow_holds WHERE order_id=?', (oid,)).fetchone()
            self.assertEqual(tuple(hold), ('released', 199, 'tr_dryrun_1'))
            self.assertEqual(db.execute('SELECT COUNT(*) FROM payout_transfers WHERE order_id=?', (oid,)).fetchone()[0], 1)

    def test_quote_and_order_match_199_dollars_and_published_deadline(self):
        _, buyer = self.buyer_ready()
        _, worker, service = self.worker_listing()
        quote = self.ok('GET', f'/services/{service}/quote', buyer)
        self.assertEqual(quote['base_amount_cents'], 19900)
        self.assertEqual(quote['pricing_type'], 'fixed')
        self.assertEqual(quote['currency'], 'usd')
        code, order = self.request('POST', f'/services/{service}/order', buyer, {
            'notes': 'Deliver audited findings in seven days',
            'quote_token': quote['quote_token'],
            'idempotency_key': 'service-order-12345678-1234-1234-1234-123456789099',
        })
        self.assertEqual(code, 201, order)
        self.assertEqual(self.intent_create.call_args.kwargs['amount'], quote['total_charge_cents'])
        self.assertTrue(order['deadline_at'])
        self.assertEqual(order['milestones'][0]['description'], 'Deliver audited findings in seven days')
        self.assertEqual(self.ok('GET', f"/orders/{order['id']}", worker)['status'], 'in_progress')

    def test_payout_disabled_after_funding_blocks_approval_and_retains_hold(self):
        _, buyer, _, worker, _, order = self.funded_order()
        self.ok('POST', f"/orders/{order['id']}/submit", worker, {'notes': 'Work delivered'})
        self.accounts['acct_dryrun_1'] = {
            'payouts_enabled': False, 'charges_enabled': True,
            'capabilities': {'transfers': 'active'},
        }
        status, response = self.request('POST', f"/orders/{order['id']}/approve", buyer)
        self.assertEqual(status, 502, response)
        self.assertIn('not payout-ready', response['error'])
        self.transfer_create.assert_not_called()
        with self.api.get_db() as db:
            self.assertEqual(db.execute('SELECT status FROM orders WHERE id=?', (order['id'],)).fetchone()[0], 'submitted')
            self.assertEqual(db.execute('SELECT status FROM escrow_holds WHERE order_id=?', (order['id'],)).fetchone()[0], 'held')

    def test_exact_checkout_retry_does_not_charge_again(self):
        _, buyer, _, _, service, order = self.funded_order()
        code, replay = self.place(buyer, service)
        self.assertEqual(code, 200, replay)
        self.assertEqual(replay['id'], order['id'])
        self.assertTrue(replay['idempotent_replay'])
        self.intent_create.assert_called_once()
        with self.api.get_db() as db:
            self.assertEqual(db.execute('SELECT COUNT(*) FROM orders WHERE service_id=?', (service,)).fetchone()[0], 1)
            self.assertEqual(db.execute('SELECT COUNT(*) FROM escrow_holds WHERE order_id=?', (order['id'],)).fetchone()[0], 1)

    def test_worker_not_payout_ready_is_rejected_before_charge(self):
        _, buyer = self.buyer_ready()
        _, worker, service = self.worker_listing(onboard=False)
        self.assertFalse(self.ok('GET', '/payments/status', worker)['worker_ready'])
        status, response = self.place(buyer, service)
        self.assertEqual(status, 409, response)
        self.assertIn('payout setup', response['error'])
        self.intent_create.assert_not_called()
        self.transfer_create.assert_not_called()
        with self.api.get_db() as db:
            self.assertEqual(db.execute('SELECT COUNT(*) FROM escrow_holds').fetchone()[0], 0)

    def test_connect_refresh_uses_new_link_generation(self):
        _, worker = self.register('worker')
        first = self.ok('POST', '/payments/setup-worker', worker)
        first_key = self.api.stripe.AccountLink.create.call_args.kwargs['idempotency_key']
        second = self.ok('POST', '/payments/setup-worker', worker, {'refresh': True})
        second_key = self.api.stripe.AccountLink.create.call_args.kwargs['idempotency_key']
        self.assertEqual(first['account_id'], second['account_id'])
        self.assertNotEqual(first_key, second_key)
        self.assertEqual(self.account_create.call_count, 1)
        self.assertEqual(self.api.stripe.AccountLink.create.call_count, 2)

    def test_german_worker_country_request_is_not_honored(self):
        _, worker = self.register('germanworker')
        self.ok('POST', '/payments/setup-worker', worker, {'country': 'DE'})
        self.assertEqual(self.account_create.call_args.kwargs['country'], 'DE')

    def test_buyer_cancel_before_worker_starts_refunds_hold(self):
        _, buyer, _, _, _, order = self.funded_order()
        oid = order['id']
        status, _ = self.request('POST', f'/orders/{oid}/cancel', buyer,
                                 {'reason': 'No longer needed'})
        self.assertEqual(status, 200)
        with self.api.get_db() as db:
            self.assertEqual(db.execute('SELECT status FROM orders WHERE id=?', (oid,)).fetchone()[0], 'canceled')
            self.assertEqual(db.execute('SELECT status FROM escrow_holds WHERE order_id=?', (oid,)).fetchone()[0], 'refunded')

    def test_worker_declines_before_start_without_stranding_hold(self):
        _, _, _, worker, _, order = self.funded_order()
        oid = order['id']
        status, _ = self.request('POST', f'/orders/{oid}/decline', worker,
                                 {'reason': 'Cannot take this work'})
        self.assertEqual(status, 200)
        with self.api.get_db() as db:
            self.assertEqual(db.execute('SELECT status FROM orders WHERE id=?', (oid,)).fetchone()[0], 'canceled')
            self.assertEqual(db.execute('SELECT status FROM escrow_holds WHERE order_id=?', (oid,)).fetchone()[0], 'refunded')

    def test_overdue_order_can_be_disputed_but_does_not_auto_refund(self):
        _, buyer, _, worker, _, order = self.funded_order()
        oid = order['id']
        with self.api.get_db() as db:
            db.execute("UPDATE orders SET deadline_at=datetime('now','-1 day') WHERE id=?", (oid,))
            db.commit()
        overdue = self.ok('GET', '/orders', buyer)
        self.assertTrue(overdue['orders'][0]['is_overdue'])
        self.ok('POST', f'/orders/{oid}/dispute', buyer,
                {'reason': 'Deadline expired with no work delivered'})
        self.assertEqual(self.request('POST', f'/orders/{oid}/submit', worker,
                                      {'notes': 'Late attempt'})[0], 409)
        self.transfer_create.assert_not_called()
        with self.api.get_db() as db:
            self.assertEqual(db.execute('SELECT status FROM orders WHERE id=?', (oid,)).fetchone()[0], 'disputed')
            self.assertEqual(db.execute('SELECT status FROM escrow_holds WHERE order_id=?', (oid,)).fetchone()[0], 'held')

    def test_dispute_admin_refund_on_actual_service_order_replays_once(self):
        _, buyer, _, worker, _, order = self.funded_order()
        oid = order['id']
        self.ok('POST', f'/orders/{oid}/dispute', buyer,
                {'reason': 'Delivery not received by agreed date'})
        with self.api.get_db() as db:
            admin_id = db.execute(
                'INSERT INTO users(email,name,password_hash,is_admin) VALUES(?,?,?,1)',
                ('admin@independent.example', 'Admin', self.api.hash_password('synthetic-admin-password'))
            ).lastrowid
            db.execute("INSERT INTO sessions(user_id,token,expires_at) VALUES(?,'admin-token',datetime('now','+1 day'))", (admin_id,))
            db.commit()
        refund_evidence = {}
        def create_refund(**kw):
            refund_evidence.update({
                'id': 're_dryrun_1', 'payment_intent': kw['payment_intent'],
                'amount': kw['amount'], 'currency': 'usd', 'metadata': kw['metadata'],
                'status': 'succeeded',
            })
            return dict(refund_evidence)
        refund_create = mock.Mock(side_effect=create_refund)
        self.api.stripe.Refund = SimpleNamespace(
            create=refund_create, retrieve=mock.Mock(side_effect=lambda _id: dict(refund_evidence)),
            list=mock.Mock(return_value={'data': []}))
        self.api.stripe.InvalidRequestError = stripe_sdk.InvalidRequestError
        command = {'order_id': oid, 'resolution': 'refund_to_employer',
                   'admin_password': 'synthetic-admin-password'}
        self.ok('POST', '/admin/resolve-dispute', 'admin-token', command)
        replay = self.ok('POST', '/admin/resolve-dispute', 'admin-token', command)
        self.assertTrue(replay['idempotent_replay'])
        refund_create.assert_called_once()
        self.assertEqual(refund_create.call_args.kwargs['amount'], 19900)
        self.transfer_create.assert_not_called()
        with self.api.get_db() as db:
            self.assertEqual(db.execute('SELECT status FROM orders WHERE id=?', (oid,)).fetchone()[0], 'canceled')
            self.assertEqual(db.execute('SELECT status FROM escrow_holds WHERE order_id=?', (oid,)).fetchone()[0], 'refunded')
            self.assertEqual(db.execute('SELECT status FROM refund_attempts WHERE order_id=?', (oid,)).fetchone()[0], 'committed')

    def test_manual_funding_alias_does_not_recharge_existing_order(self):
        _, buyer, _, _, _, order = self.funded_order()
        milestone_id = order['milestones'][0]['id']
        for route in ('/payments/prepare-order-payment', '/payments/fund-escrow'):
            status, result = self.request('POST', route, buyer,
                                          {'order_id': order['id'], 'milestone_id': milestone_id, 'amount': 199})
            self.assertEqual(status, 200, result)
            self.assertTrue(result['idempotent_replay'])
        self.intent_create.assert_called_once()
