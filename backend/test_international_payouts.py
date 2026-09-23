"""International Connect onboarding: durable country binding and payout readiness."""
import contextlib
import io
import json
import os
import unittest
from types import SimpleNamespace
from unittest import mock

from test_deep_audit_regressions import load_api_core, parse_cgi_output
from test_payment_setup_attempt_ledger import PaymentSetupAttemptLedgerTests


class InternationalPayoutTests(unittest.TestCase):
    setUp = PaymentSetupAttemptLedgerTests.setUp
    tearDown = PaymentSetupAttemptLedgerTests.tearDown
    def request(self, path, token=None, payload=None, api_key=None):
        if path not in ('/payments/connect-countries', '/payments/status'):
            return PaymentSetupAttemptLedgerTests.request(self, path, token, payload, api_key)
        for cached in ('body_cache', 'raw_body'):
            if hasattr(self.api._request_ctx, cached):
                delattr(self.api._request_ctx, cached)
        ctx = self.api._request_ctx
        ctx.request_method = 'GET'
        ctx.path_info = path
        ctx.query_string = ''
        ctx.http_authorization = f'Bearer {token}' if token else ''
        ctx.http_x_api_key = ''
        ctx.stdin_data = ''
        ctx.content_length = '0'
        ctx.remote_addr = '127.0.0.1'
        with contextlib.redirect_stdout(io.StringIO()) as out:
            self.api.handle_request()
        return parse_cgi_output(out.getvalue())

    def test_flag_off_keeps_identical_us_processor_call(self):
        status, result = self.request('/payments/setup-worker', 'worker-token', {'country': 'US'})
        self.assertEqual(status, 200, result)
        call = next(c for c in self.calls if c[0] == 'account_create')
        self.assertEqual(call[2], {
            'type': 'express', 'country': 'US', 'email': 'worker@example.com',
            'capabilities': {'transfers': {'requested': True}},
            'metadata': {'user_id': '2'}, 'idempotency_key': call[2]['idempotency_key'],
        })
        with self.api.get_db() as db:
            row = db.execute("SELECT payout_account_country,payout_service_agreement FROM worker_profiles WHERE user_id=2").fetchone()
            self.assertEqual(tuple(row), ('US', 'full'))

    def test_flag_off_rejects_non_us_without_stripe_io(self):
        status, result = self.request('/payments/setup-worker', 'worker-token', {'country': 'DE'})
        self.assertEqual(status, 400, result)
        self.assertEqual(self.calls, [])

    def test_enabled_full_agreement_and_durable_country_fingerprint(self):
        self.api.CONNECT_INTERNATIONAL_ENABLED = True
        status, result = self.request('/payments/setup-worker', 'worker-token', {'country': 'DE'})
        self.assertEqual(status, 200, result)
        call = next(c for c in self.calls if c[0] == 'account_create')
        self.assertEqual(call[2]['country'], 'DE')
        self.assertNotIn('tos_acceptance', call[2])
        with self.api.get_db() as db:
            row = db.execute("SELECT request_binding_json FROM payment_setup_operations WHERE operation_kind='account_create'").fetchone()
            self.assertEqual(json.loads(row[0])['country'], 'DE')
            self.assertEqual(json.loads(row[0])['agreement'], 'full')
            profile = db.execute("SELECT payout_account_country,payout_service_agreement FROM worker_profiles WHERE user_id=2").fetchone()
            self.assertEqual(tuple(profile), ('DE', 'full'))

    def test_recipient_agreement_countries_excluded_from_self_serve_cross_border(self):
        self.api.CONNECT_INTERNATIONAL_ENABLED = True
        for country in ('IN', 'PH', 'NG'):
            with self.subTest(country=country):
                status, result = self.request('/payments/setup-worker', 'worker-token', {'country': country})
                self.assertEqual(status, 400, result)
        status, result = self.request('/payments/connect-countries')
        self.assertEqual(status, 200, result)
        self.assertFalse([c for c in result['countries'] if c['agreement'] == 'recipient'])
        self.assertEqual(self.calls, [])

    def test_unsupported_or_malformed_country_never_calls_stripe(self):
        self.api.CONNECT_INTERNATIONAL_ENABLED = True
        for country in ('ZZ', 'BR', 'de', 15, None, 'DE<script>'):
            with self.subTest(country=country):
                status, _ = self.request('/payments/setup-worker', 'worker-token', {'country': country})
                self.assertEqual(status, 400)
        self.assertEqual(self.calls, [])

    def test_existing_account_country_mismatch_rejected_before_stripe(self):
        self.api.CONNECT_INTERNATIONAL_ENABLED = True
        status, _ = self.request('/payments/setup-worker', 'worker-token', {'country': 'DE'})
        self.assertEqual(status, 200)
        self.calls.clear()
        status, result = self.request('/payments/setup-worker', 'worker-token', {'country': 'GB'})
        self.assertEqual(status, 409, result)
        self.assertIn('country', result['error'].lower())
        self.assertEqual(self.calls, [])

    def test_legacy_us_account_cannot_be_switched_to_another_country(self):
        self.api.CONNECT_INTERNATIONAL_ENABLED = True
        with self.api.get_db() as db:
            db.execute("INSERT INTO worker_profiles(user_id,payout_account_id) VALUES (2,'acct_old_us')")
            db.commit()
        status, result = self.request('/payments/setup-worker', 'worker-token', {'country': 'DE'})
        self.assertEqual(status, 409, result)
        self.assertEqual(self.calls, [])

    def test_payout_ready_without_charges_only_if_transfers_active(self):
        self.assertTrue(self.api.is_live_connect_account_ready({
            'payouts_enabled': True, 'charges_enabled': False, 'capabilities': {'transfers': 'active'}}))
        self.assertTrue(self.api.is_live_connect_account_ready({
            'payouts_enabled': True, 'charges_enabled': True, 'capabilities': {'transfers': 'active'}}))
        self.assertFalse(self.api.is_live_connect_account_ready({
            'payouts_enabled': True, 'charges_enabled': True, 'capabilities': {'transfers': 'pending'}}))
        self.assertFalse(self.api.is_live_connect_account_ready({
            'payouts_enabled': False, 'charges_enabled': False, 'capabilities': {'transfers': 'active'}}))

    def test_public_countries_respect_flag_and_allowlist(self):
        self.api.CONNECT_INTERNATIONAL_ENABLED = False
        status, result = self.request('/payments/connect-countries')
        self.assertEqual(status, 200, result)
        self.assertEqual([c['code'] for c in result['countries']], ['US'])
        self.api.CONNECT_INTERNATIONAL_ENABLED = True
        with mock.patch.dict(os.environ, {'CONNECT_COUNTRIES_ALLOWLIST': 'DE,IN'}):
            status, result = self.request('/payments/connect-countries')
            self.assertEqual(status, 200, result)
            self.assertEqual([c['code'] for c in result['countries']], ['DE'])

    def test_signed_account_updated_marks_recipient_ready_once(self):
        self.api.STRIPE_WEBHOOK_SECRET = 'local-webhook-key'
        with self.api.get_db() as db:
            db.execute("INSERT INTO worker_profiles(user_id,payout_account_id,payout_account_country,payout_service_agreement,payout_method) VALUES (2,'acct_recipient','IN','recipient','stripe_connect_pending')")
            db.commit()
        data = {'id': 'acct_recipient', 'payouts_enabled': True,
                'charges_enabled': False, 'capabilities': {'transfers': 'active'}}
        self.api.stripe.Webhook = SimpleNamespace(construct_event=lambda *args: {
            'type': 'account.updated', 'data': {'object': data}})
        status, result = self.request('/webhooks/stripe', payload={'type': 'account.updated'})
        self.assertEqual(status, 200, result)
        with self.api.get_db() as db:
            self.assertEqual(db.execute('SELECT payout_method FROM worker_profiles WHERE user_id=2').fetchone()[0],
                             'stripe_connect_active')
            self.assertEqual(db.execute("SELECT COUNT(*) FROM notifications WHERE user_id=2 AND type='payout_ready'").fetchone()[0], 1)
        status, result = self.request('/webhooks/stripe', payload={'type': 'account.updated'})
        self.assertEqual(status, 200, result)
        with self.api.get_db() as db:
            self.assertEqual(db.execute("SELECT COUNT(*) FROM notifications WHERE user_id=2 AND type='payout_ready'").fetchone()[0], 1)
        data['capabilities']['transfers'] = 'pending'
        status, _ = self.request('/webhooks/stripe', payload={'type': 'account.updated'})
        self.assertEqual(status, 200)
        with self.api.get_db() as db:
            self.assertEqual(db.execute('SELECT payout_method FROM worker_profiles WHERE user_id=2').fetchone()[0],
                             'stripe_connect_pending')

    def test_status_retrieval_treats_recipient_as_worker_ready(self):
        with self.api.get_db() as db:
            db.execute("INSERT INTO worker_profiles(user_id,payout_account_id,payout_account_country,payout_service_agreement) VALUES (2,'acct_recipient','IN','recipient')")
            db.commit()
        self.api.stripe.Account.retrieve = mock.Mock(return_value={
            'id': 'acct_recipient', 'payouts_enabled': True, 'charges_enabled': False,
            'capabilities': {'transfers': 'active'}})
        status, result = self.request('/payments/status', 'worker-token')
        self.assertEqual(status, 200, result)
        self.assertTrue(result['worker_ready'])
        self.assertEqual(result['worker_payout_status']['country'], 'IN')
        self.assertFalse(result['worker_payout_status']['charges_enabled'])

    def test_allowlist_blocks_setup_even_when_table_supports_country(self):
        self.api.CONNECT_INTERNATIONAL_ENABLED = True
        with mock.patch.dict(os.environ, {'CONNECT_COUNTRIES_ALLOWLIST': 'US,DE'}):
            status, _ = self.request('/payments/setup-worker', 'worker-token', {'country': 'IN'})
        self.assertEqual(status, 400)
        self.assertEqual(self.calls, [])

    def test_single_allowed_country_is_used_when_client_omits_country(self):
        # Review blocker: allowlist=DE hides the selector and the UI posts {}.
        self.api.CONNECT_INTERNATIONAL_ENABLED = True
        with mock.patch.dict(os.environ, {'CONNECT_COUNTRIES_ALLOWLIST': 'DE'}):
            status, result = self.request('/payments/setup-worker', 'worker-token', {})
        self.assertEqual(status, 200, result)
        self.assertEqual(next(c for c in self.calls if c[0] == 'account_create')[2]['country'], 'DE')

    def test_flag_rollback_keeps_existing_account_bank_updates_working(self):
        # Review blocker: after DE onboarding, turning the flag off stranded the worker.
        self.api.CONNECT_INTERNATIONAL_ENABLED = True
        status, result = self.request('/payments/setup-worker', 'worker-token', {'country': 'DE'})
        self.assertEqual(status, 200, result)
        self.api.CONNECT_INTERNATIONAL_ENABLED = False
        for payload in ({}, {'country': 'DE'}):
            with self.subTest(payload=payload):
                self.calls.clear()
                status, result = self.request('/payments/setup-worker', 'worker-token', dict(payload, refresh=True))
                self.assertEqual(status, 200, result)
                names = [c[0] for c in self.calls]
                self.assertNotIn('account_create', names)
                self.assertIn('account_link_create', names)
        # A different, now-unsupported country is still refused without Stripe I/O.
        self.calls.clear()
        status, _ = self.request('/payments/setup-worker', 'worker-token', {'country': 'GB'})
        self.assertIn(status, (400, 409))
        self.assertEqual(self.calls, [])

    def test_migration_adds_columns_idempotently(self):
        self.api.init_db()
        with self.api.get_db() as db:
            cols = [row['name'] for row in db.execute('PRAGMA table_info(worker_profiles)')]
            self.assertEqual(cols.count('payout_account_country'), 1)
            self.assertEqual(cols.count('payout_service_agreement'), 1)


if __name__ == '__main__':
    unittest.main()
