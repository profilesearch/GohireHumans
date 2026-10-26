"""A service listing takes orders only when its seller finished payout setup.

Billy decision (2026-10-26): listings whose seller is not payout-ready stay
visible, but are labeled as not taking orders, cannot be quoted or ordered,
and the owner sees why. Readiness is the stored hint
(worker_profiles.payout_method == 'stripe_connect_active'); funding still
re-checks the live Stripe account.
"""
import contextlib
import importlib.util
import io
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest import mock

MODULE_PATH = Path(__file__).with_name('api_core.py')
KEY = 'service-order-' + 'a' * 24


def load_core():
    spec = importlib.util.spec_from_file_location('service_order_readiness_core', MODULE_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class ServiceOrderReadinessTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.env = mock.patch.dict(os.environ, {
            'DATABASE_PATH': str(Path(self.temp.name) / 'test.db'),
            'DISABLE_AUTO_SEED': '1',
        })
        self.env.start()
        self.core = load_core()
        self.core.init_db()
        db = self.core.get_db()
        db.executemany('INSERT INTO users (id,email,name,password_hash) VALUES (?,?,?,?)', [
            (1, 'ready@example.com', 'Ready Seller', 'x'),
            (2, 'pending@example.com', 'Pending Seller', 'x'),
            (3, 'buyer@example.com', 'Buyer', 'x'),
        ])
        db.executemany(
            "INSERT INTO sessions (user_id,token,expires_at) VALUES (?,?,datetime('now','+1 day'))",
            [(i, f'tok-{i}') for i in (1, 2, 3)])
        db.executemany('INSERT INTO worker_profiles (user_id,payout_method,payout_account_id) VALUES (?,?,?)', [
            (1, 'stripe_connect_active', 'acct_ready_fixture'),
            (2, 'stripe_connect_pending', 'acct_pending_fixture'),
        ])
        db.commit()
        db.close()
        self.ready = self.create(1)
        self.pending = self.create(2)
        db = self.core.get_db()
        # The pending listing is newer, so the old ordering would show it first.
        db.execute("UPDATE services SET created_at='2026-01-01 00:00:00' WHERE id=?", [self.ready['id']])
        db.execute("UPDATE services SET created_at='2026-06-01 00:00:00' WHERE id=?", [self.pending['id']])
        db.commit()
        db.close()

    def tearDown(self):
        self.env.stop()
        self.temp.cleanup()

    def request(self, method, path, payload=None, token='', query=''):
        c = self.core._request_ctx
        for key in ('body_cache', 'raw_body'):
            if hasattr(c, key):
                delattr(c, key)
        body = json.dumps(payload or {})
        c.request_method = method
        c.path_info = path
        c.query_string = query
        c.http_authorization = 'Bearer ' + token if token else ''
        c.http_x_api_key = ''
        c.stdin_data = body
        c.stdin_data_raw = body.encode()
        c.content_type = 'application/json'
        c.content_length = str(len(c.stdin_data_raw))
        c.remote_addr = '127.0.0.1'
        with contextlib.redirect_stdout(io.StringIO()) as output:
            self.core.handle_request()
        headers, _, text = output.getvalue().partition('\n\n')
        status = next((int(line.split(':', 1)[1]) for line in headers.splitlines()
                       if line.startswith('Status:')), 200)
        return status, json.loads(text or '{}')

    def create(self, owner):
        status, result = self.request('POST', '/services', {
            'title': 'Proofreading', 'description': 'Careful proofreading of one short document.',
            'category': 'writing', 'pricing_type': 'fixed', 'price': 25, 'delivery_time_days': 3,
        }, f'tok-{owner}')
        self.assertEqual(status, 201, result)
        return result

    def order_count(self):
        db = self.core.get_db()
        try:
            return db.execute('SELECT COUNT(*) FROM orders').fetchone()[0]
        finally:
            db.close()

    def test_browse_and_detail_report_whether_a_listing_takes_orders(self):
        status, listing = self.request('GET', '/services')
        self.assertEqual(status, 200)
        by_id = {s['id']: s for s in listing['services']}
        self.assertIs(by_id[self.ready['id']]['accepting_orders'], True)
        self.assertIs(by_id[self.pending['id']]['accepting_orders'], False)
        self.assertIs(self.request('GET', f"/services/{self.ready['id']}")[1]['accepting_orders'], True)
        self.assertIs(self.request('GET', f"/services/{self.pending['id']}")[1]['accepting_orders'], False)

    def test_orderable_listings_are_listed_first(self):
        ids = [s['id'] for s in self.request('GET', '/services')[1]['services']]
        self.assertEqual(ids, [self.ready['id'], self.pending['id']])

    def test_only_the_owner_sees_the_payout_setup_notice(self):
        public = self.request('GET', f"/services/{self.pending['id']}")[1]
        self.assertNotIn('order_notice', public)
        buyer = self.request('GET', f"/services/{self.pending['id']}", token='tok-3')[1]
        self.assertNotIn('order_notice', buyer)
        mine = self.request('GET', '/me/services', token='tok-2')[1]['services'][0]
        self.assertIn('payout setup', mine['order_notice'].lower())
        self.assertIs(mine['accepting_orders'], False)
        ready_mine = self.request('GET', '/me/services', token='tok-1')[1]['services'][0]
        self.assertNotIn('order_notice', ready_mine)

    def test_paused_listing_does_not_take_orders_even_when_seller_is_ready(self):
        status, _ = self.request('PUT', f"/services/{self.ready['id']}", {'status': 'paused'}, 'tok-1')
        self.assertEqual(status, 200)
        mine = self.request('GET', '/me/services', token='tok-1')[1]['services'][0]
        self.assertIs(mine['accepting_orders'], False)
        self.assertNotIn('order_notice', mine)

    def test_missing_or_null_payout_method_fails_closed(self):
        db = self.core.get_db()
        db.execute('UPDATE worker_profiles SET payout_method=NULL WHERE user_id=2')
        db.commit()
        db.close()
        self.assertIs(self.request('GET', f"/services/{self.pending['id']}")[1]['accepting_orders'], False)
        db = self.core.get_db()
        db.execute('DELETE FROM worker_profiles WHERE user_id=2')
        db.commit()
        db.close()
        self.assertIs(self.request('GET', f"/services/{self.pending['id']}")[1]['accepting_orders'], False)

    def test_quote_refuses_a_seller_without_payout_setup(self):
        status, body = self.request('GET', f"/services/{self.pending['id']}/quote", token='tok-3')
        self.assertEqual(status, 409, body)
        self.assertEqual(body.get('code'), 'seller_payout_setup_required')
        status, body = self.request('GET', f"/services/{self.ready['id']}/quote", token='tok-3')
        self.assertEqual(status, 200, body)

    def test_order_refuses_before_creating_an_order_or_touching_payments(self):
        with mock.patch.object(self.core, 'fund_escrow_stripe') as fund:
            status, body = self.request('POST', f"/services/{self.pending['id']}/order",
                                        {'idempotency_key': KEY}, 'tok-3')
        self.assertEqual(status, 409, body)
        self.assertEqual(body.get('code'), 'seller_payout_setup_required')
        fund.assert_not_called()
        self.assertEqual(self.order_count(), 0)

    def test_ready_seller_passes_the_gate(self):
        # The buyer has no card, so a ready listing stops at the buyer's own payment step.
        status, body = self.request('POST', f"/services/{self.ready['id']}/order",
                                    {'idempotency_key': KEY}, 'tok-3')
        self.assertNotEqual(body.get('code'), 'seller_payout_setup_required')
        self.assertEqual(status, 402, body)

    def test_existing_order_replay_is_not_blocked_by_the_gate(self):
        db = self.core.get_db()
        db.execute("""INSERT INTO orders (type, service_id, worker_id, employer_id, status, total_amount,
                      creation_idempotency_key, creation_request_fingerprint)
                      VALUES ('service_order', ?, 2, 3, 'pending', 25, ?, 'legacy-fingerprint')""",
                   [self.pending['id'], KEY])
        db.commit()
        db.close()
        status, body = self.request('POST', f"/services/{self.pending['id']}/order",
                                    {'idempotency_key': KEY}, 'tok-3')
        self.assertNotEqual(body.get('code'), 'seller_payout_setup_required', body)


if __name__ == '__main__':
    unittest.main()
