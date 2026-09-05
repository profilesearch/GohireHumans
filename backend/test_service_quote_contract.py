"""Authoritative quotes, exercised offline with explicit processor fixtures."""
import contextlib
import hashlib
import io
import json
import unittest
from types import SimpleNamespace
from unittest import mock
from urllib.parse import urlencode

import test_transaction_lifecycle_regressions as lifecycle
from test_deep_audit_regressions import parse_cgi_output


class ServiceQuoteContractTests(unittest.TestCase):
    tearDown = lifecycle.TransactionLifecycleRegressionTests.tearDown
    _seed = lifecycle.TransactionLifecycleRegressionTests._seed

    def setUp(self):
        lifecycle.TransactionLifecycleRegressionTests.setUp(self)
        with self.api.get_db() as db:
            db.execute("UPDATE worker_profiles SET payout_account_id='acct_quote_fixture', payout_method='stripe_connect_active' WHERE user_id=1")
            db.commit()
        self.account_retrieve = mock.Mock(return_value={
            'id': 'acct_quote_fixture', 'payouts_enabled': True,
            'charges_enabled': True, 'capabilities': {'transfers': 'active'}})
        self.api.stripe.Account = SimpleNamespace(retrieve=self.account_retrieve)

    def request(self, method, path, token='tok-employer', payload=None, query=None, api_key=''):
        ctx = self.api._request_ctx
        for cached in ('body_cache', 'raw_body'):
            if hasattr(ctx, cached):
                delattr(ctx, cached)
        body = json.dumps(payload or {})
        ctx.request_method = method
        ctx.path_info = path
        ctx.query_string = urlencode(query or {})
        ctx.http_authorization = f'Bearer {token}' if token else ''
        ctx.http_x_api_key = api_key
        ctx.stdin_data = body
        ctx.content_type = 'application/json'
        ctx.content_length = str(len(body.encode()))
        ctx.remote_addr = '127.0.0.1'
        with contextlib.redirect_stdout(io.StringIO()) as out:
            self.api.handle_request()
        return parse_cgi_output(out.getvalue())

    def domain_snapshot(self):
        with self.api.get_db() as db:
            tables = [r[0] for r in db.execute("SELECT name FROM sqlite_master WHERE type='table'")
                      if r[0] not in ('api_keys', 'api_key_usage', 'sqlite_sequence')]
            return {t: [tuple(r) for r in db.execute(f'SELECT * FROM "{t}"')] for t in tables}

    def quote(self, **query):
        status, data = self.request('GET', '/services/1/quote', query=query)
        self.assertEqual(status, 200, data)
        return data

    def test_get_quote_is_authenticated_pure_and_exact(self):
        before = self.domain_snapshot()
        status, data = self.request('GET', '/services/1/quote', token='', query={'amount': '25.55'})
        self.assertEqual(status, 401, data)
        quote = self.quote(amount='25.55')
        self.assertEqual(set(quote), {'service_id', 'pricing_type', 'currency', 'hours',
            'base_amount_cents', 'processing_fee_cents', 'platform_fee_cents',
            'total_charge_cents', 'quote_token'})
        fee = self.api.buyer_charge_breakdown_cents('25.55')
        self.assertEqual(quote['service_id'], 1)
        self.assertEqual(quote['pricing_type'], 'custom')
        self.assertEqual(quote['currency'], 'usd')
        self.assertIsNone(quote['hours'])
        for out, key in [('base_amount_cents', 'base_cents'), ('processing_fee_cents', 'processing_fee_cents'),
                         ('platform_fee_cents', 'platform_fee_cents'), ('total_charge_cents', 'total_cents')]:
            self.assertIs(type(quote[out]), int)
            self.assertEqual(quote[out], fee[key])
        self.assertRegex(quote['quote_token'], r'^[0-9a-f]{64}$')
        self.assertEqual(quote, self.quote(amount='25.55'))
        self.assertEqual(before, self.domain_snapshot())
        self.payment_create.assert_not_called()
        self.account_retrieve.assert_not_called()

    def test_stale_quote_blocks_new_order_before_domain_writes(self):
        changes = {'price': 27.01, 'hourly_rate': 41.03, 'title': 'Changed',
            'description': 'Changed scope', 'worker_id': 3, 'provider_type': 'ai',
            'fulfillment_type': 'api', 'delivery_time_days': 9,
            'includes': 'New deliverable', 'status': 'paused'}
        with self.api.get_db() as db:
            db.execute("UPDATE services SET pricing_type='fixed', price=25.55, delivery_time_days=7 WHERE id=1")
            db.commit()
        for field, value in changes.items():
            with self.subTest(field=field):
                quote = self.quote()
                with self.api.get_db() as db:
                    old = db.execute(f'SELECT {field} FROM services WHERE id=1').fetchone()[0]
                    db.execute(f'UPDATE services SET {field}=? WHERE id=1', [value])
                    db.commit()
                before = self.domain_snapshot()
                status, data = self.request('POST', '/services/1/order', payload={
                    'idempotency_key': 'stale-quote-operation-0001', 'quote_token': quote['quote_token']})
                self.assertEqual(status, 409, data)
                self.assertEqual(set(data), {'error', 'code', 'retry_safe'})
                self.assertEqual(data['code'], 'service_quote_changed')
                self.assertIs(data['retry_safe'], True)
                self.assertEqual(before, self.domain_snapshot())
                self.payment_create.assert_not_called()
                self.account_retrieve.assert_not_called()
                with self.api.get_db() as db:
                    db.execute(f'UPDATE services SET {field}=? WHERE id=1', [old])
                    db.commit()

    def test_quote_token_fingerprint_preserves_legacy_and_validates_shape(self):
        body = {'amount': '25.55', 'hours': '01.00', 'notes': 'Keep exactly',
                'idempotency_key': 'legacy-quote-operation-0001'}
        original = dict(body)
        payload = {'amount_cents': 2555, 'employer_id': 2, 'hours': '1',
                   'notes': 'Keep exactly', 'service_id': 1, 'version': 1}
        expected = hashlib.sha256(json.dumps(payload, sort_keys=True,
            separators=(',', ':'), ensure_ascii=True).encode()).hexdigest()
        fingerprint = self.api.service_order_creation_request_fingerprint
        self.assertEqual(fingerprint(2, 1, body), expected)
        self.assertEqual(body, original)
        self.assertNotEqual(fingerprint(2, 1, dict(body, quote_token='a' * 64)), expected)
        self.assertNotEqual(fingerprint(2, 1, dict(body, quote_token='a' * 64)),
                            fingerprint(2, 1, dict(body, quote_token='b' * 64)))
        for token in (None, '', 'a' * 63, 'A' * 64, 'z' * 64, 123, []):
            with self.subTest(token=token):
                status, data = self.request('POST', '/services/1/order', payload=dict(body, quote_token=token))
                self.assertEqual(status, 400, data)
        self.payment_create.assert_not_called()

    def test_read_scoped_key_quote_has_no_domain_or_processor_effect(self):
        key = 'ghh_quote_read_only_fixture'
        with self.api.get_db() as db:
            db.execute("INSERT INTO api_keys (user_id,key_hash,key_prefix,scopes) VALUES (2,?,?,?)",
                [hashlib.sha256(key.encode()).hexdigest(), 'ghh_quote', '["read"]'])
            db.execute('DELETE FROM employer_profiles WHERE user_id=2')
            db.commit()
        before = self.domain_snapshot()
        status, data = self.request('GET', '/services/1/quote', token='', api_key=key, query={'amount': '25.55'})
        self.assertEqual(status, 200, data)
        self.assertEqual(data, self.quote(amount='25.55'))
        self.assertEqual(before, self.domain_snapshot())
        self.payment_create.assert_not_called()
        self.account_retrieve.assert_not_called()
        with self.api.get_db() as db:
            usage = db.execute('SELECT authorized_scope,status_code FROM api_key_usage').fetchone()
            self.assertEqual(tuple(usage), ('read', 200))

    def test_quote_charge_parity_and_committed_replay_after_price_change(self):
        cases = [('fixed', '25.55', {}), ('hourly', '25.55', {'hours': '1.125'}),
                 ('custom', None, {'amount': '25.55'}), ('fixed', '0.01', {})]
        for index, (pricing, price, inputs) in enumerate(cases):
            with self.subTest(pricing=pricing, price=price):
                with self.api.get_db() as db:
                    db.execute('UPDATE services SET pricing_type=?,price=?,hourly_rate=?,delivery_time_days=7 WHERE id=1',
                               [pricing, price, price])
                    db.commit()
                quote = self.quote(**inputs)
                self.assertEqual(quote['hours'], inputs.get('hours'))
                body = dict(inputs, idempotency_key=f'quote-parity-operation-{index:04d}', quote_token=quote['quote_token'])
                status, order = self.request('POST', '/services/1/order', payload=body)
                self.assertEqual(status, 201, order)
                self.assertEqual(self.payment_create.call_args.kwargs['amount'], quote['total_charge_cents'])
                self.assertEqual(self.api.money_to_cents(order['total_amount']), quote['base_amount_cents'])
                with self.api.get_db() as db:
                    db.execute("UPDATE services SET price=99.99, hourly_rate=99.99, title='Changed after funding' WHERE id=1")
                    db.commit()
                count = self.payment_create.call_count
                self.account_retrieve.reset_mock()
                status, replay = self.request('POST', '/services/1/order', payload=body)
                self.assertEqual(status, 200, replay)
                self.assertEqual(replay['id'], order['id'])
                self.assertTrue(replay['idempotent_replay'])
                self.assertEqual(self.payment_create.call_count, count)
                self.account_retrieve.assert_not_called()
                changed = dict(body, quote_token='f' * 64)
                status, conflict = self.request('POST', '/services/1/order', payload=changed)
                self.assertEqual(status, 409, conflict)
                self.assertNotEqual(conflict.get('code'), 'service_quote_changed')
                self.assertNotEqual(conflict.get('retry_safe'), True)

    def test_quote_binds_buyer_quantity_custom_amount_and_fee_policy(self):
        first = self.quote(amount='25.55')['quote_token']
        self.assertNotEqual(first, self.quote(amount='25.56')['quote_token'])
        with self.api.get_db() as db:
            service = db.execute('SELECT * FROM services WHERE id=1').fetchone()
        self.assertNotEqual(first, self.api.service_quote(service, 3, {'amount': '25.55'})['quote_token'])
        with mock.patch.object(self.api, 'PLATFORM_FEE_BPS', self.api.PLATFORM_FEE_BPS + 1):
            self.assertNotEqual(first, self.quote(amount='25.55')['quote_token'])
        with self.api.get_db() as db:
            db.execute("UPDATE services SET pricing_type='hourly',hourly_rate=0.01 WHERE id=1")
            db.commit()
        self.assertEqual(self.quote(hours='01.000'), self.quote(hours='1'))
        self.assertNotEqual(self.quote(hours='1')['quote_token'], self.quote(hours='1.01')['quote_token'])

    def test_legacy_order_body_still_funds_without_quote(self):
        body = {'amount': '25.55', 'notes': 'Original notes',
                'idempotency_key': 'legacy-body-operation-0001'}
        before = dict(body)
        status, data = self.request('POST', '/services/1/order', payload=body)
        self.assertEqual(status, 201, data)
        self.assertEqual(body, before)
        self.assertEqual(data['milestones'][0]['description'], 'Original notes')
        self.assertEqual(data['creation_request_fingerprint'],
                         self.api.service_order_creation_request_fingerprint(2, 1, before))
        status, replay = self.request('POST', '/services/1/order', payload=before)
        self.assertEqual(status, 200, replay)
        self.payment_create.assert_called_once()

    def test_new_order_validates_quote_under_writer_lock(self):
        import sqlite3
        quote = self.quote(amount='25.55')
        real_quote = self.api.service_quote
        validated = []
        def under_writer(*args):
            with sqlite3.connect(self.api._get_db_path(), timeout=0.01) as writer:
                with self.assertRaisesRegex(sqlite3.OperationalError, 'locked'):
                    writer.execute('BEGIN IMMEDIATE')
            validated.append(True)
            return real_quote(*args)
        with mock.patch.object(self.api, 'service_quote', side_effect=under_writer):
            status, order = self.request('POST', '/services/1/order', payload={
                'amount': '25.55', 'quote_token': quote['quote_token'],
                'idempotency_key': 'locked-quote-operation-0001'})
        self.assertEqual(status, 201, order)
        self.assertEqual(validated, [True])

    def test_invalid_quote_quantities_are_rejected_without_effects(self):
        before = self.domain_snapshot()
        for amount in ('', '0', '-1', 'NaN', '25.555', '1e2'):
            with self.subTest(amount=amount):
                status, data = self.request('GET', '/services/1/quote', query={'amount': amount})
                self.assertEqual(status, 400, data)
        self.assertEqual(before, self.domain_snapshot())
        with self.api.get_db() as db:
            db.execute("UPDATE services SET pricing_type='hourly',hourly_rate=25.55 WHERE id=1")
            db.commit()
        before = self.domain_snapshot()
        for hours in ('', '0', '-1', 'NaN', '1e2', '10001'):
            with self.subTest(hours=hours):
                status, data = self.request('GET', '/services/1/quote', query={'hours': hours})
                self.assertEqual(status, 400, data)
        self.assertEqual(before, self.domain_snapshot())
        self.payment_create.assert_not_called()
        self.account_retrieve.assert_not_called()
