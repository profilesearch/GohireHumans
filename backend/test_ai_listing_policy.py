"""AI listing ownership, visibility and payout-policy regressions."""
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


def load_core():
    spec = importlib.util.spec_from_file_location('ai_listing_policy_core', MODULE_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class AIListingPolicyTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.env = mock.patch.dict(os.environ, {
            'DATABASE_PATH': str(Path(self.temp.name) / 'test.db'),
            'DISABLE_AUTO_SEED': '1', 'AGENT_PLATFORM_DOMAINS': 'ilands.app, robots.example',
        })
        self.env.start()
        self.core = load_core()
        self.core.init_db()
        db = self.core.get_db()
        db.executemany('INSERT INTO users (id,email,name,password_hash,is_ai_agent,is_admin) VALUES (?,?,?,?,?,?)', [
            (1, 'bot@iLaNdS.App', 'Bot', 'x', 0, 0),
            (2, 'flagged@example.com', 'Flagged', 'x', 1, 0),
            (3, 'regular@example.com', 'Regular', 'x', 0, 0),
            (4, 'buyer@example.com', 'Buyer', 'x', 0, 0),
            (5, 'admin@example.com', 'Admin', 'x', 0, 1),
        ])
        db.executemany("INSERT INTO sessions (user_id,token,expires_at) VALUES (?,?,datetime('now','+1 day'))", [(i, f'tok-{i}') for i in range(1, 6)])
        db.executemany("INSERT INTO worker_profiles (user_id,payout_method) VALUES (?,?)", [(1,'pending_setup'),(2,'pending_setup'),(3,'pending_setup')])
        db.commit()
        db.close()

    def tearDown(self):
        self.env.stop()
        self.temp.cleanup()

    def request(self, method, path, payload=None, token='', query=''):
        c = self.core._request_ctx
        for key in ('body_cache','raw_body'):
            if hasattr(c,key): delattr(c,key)
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
        status = next((int(line.split(':', 1)[1]) for line in headers.splitlines() if line.startswith('Status:')), 200)
        return status, json.loads(text or '{}')

    def create(self, owner, provider_type='human'):
        status, result = self.request('POST','/services', {
            'title': 'Research support', 'description': 'A clear and bounded research service.',
            'category':'research', 'pricing_type':'fixed', 'price':25, 'provider_type':provider_type,
        },f'tok-{owner}')
        self.assertEqual(status, 201, result)
        return result

    def test_agent_detection_for_domain_flag_and_existing_ai_listing(self):
        self.assertTrue(self.core.is_agent_seller(self.core.get_db(), 1))
        self.assertTrue(self.core.is_agent_seller(self.core.get_db(), 2))
        self.assertFalse(self.core.is_agent_seller(self.core.get_db(), 3))
        self.create(3, 'ai')
        self.assertTrue(self.core.is_agent_seller(self.core.get_db(), 3))

    def test_create_update_force_ai_and_return_notice(self):
        domain = self.create(1)
        flagged = self.create(2)
        self.assertEqual(domain['provider_type'], 'ai')
        self.assertEqual(flagged['provider_type'], 'ai')
        self.assertIn('hidden', domain['notice'].lower())
        self.assertEqual(self.create(3)['provider_type'], 'human')
        status, updated = self.request('PUT',f"/services/{domain['id']}", {'provider_type':'human'},'tok-1')
        self.assertEqual(status,200,updated)
        self.assertEqual(updated['provider_type'],'ai')
        self.assertIn('hidden',updated['notice'].lower())
        ai = self.create(3, 'ai')
        status, updated = self.request('PUT',f"/services/{ai['id']}", {'provider_type':'human'},'tok-3')
        self.assertEqual(updated['provider_type'],'ai')
        self.assertIn('notice', updated)

    def test_ai_listing_does_not_hide_other_human_services_of_same_operator(self):
        human = self.create(3)
        ai = self.create(3, 'ai')
        self.assertEqual(self.request('GET', '/services')[1]['total'], 1)
        self.assertEqual(self.request('GET', f"/services/{human['id']}")[0], 200)
        self.assertEqual(self.request('GET', f"/services/{ai['id']}")[0], 404)
        self.assertEqual(self.request('GET', '/services', query='provider_type=human')[1]['total'], 1)
        # An AI listing cannot be changed back to human to bypass verification.
        status, updated = self.request('PUT', f"/services/{ai['id']}", {'provider_type':'human'}, 'tok-3')
        self.assertEqual(status, 200)
        self.assertEqual(updated['provider_type'], 'ai')
        self.assertEqual(self.create(3)['provider_type'], 'human')

    def test_backfill_is_idempotent_and_relabels_without_deletion(self):
        db = self.core.get_db()
        db.execute("INSERT INTO services (worker_id,title,description,category,status,provider_type) VALUES (1,'Legacy','Old','research','active','human')")
        db.commit()
        self.core.init_db()
        self.core.init_db()
        self.assertEqual(db.execute('SELECT is_ai_agent FROM users WHERE id=1').fetchone()[0],1)
        self.assertEqual(db.execute('SELECT provider_type FROM services WHERE worker_id=1').fetchone()[0],'ai')
        self.assertEqual(db.execute('SELECT COUNT(*) FROM services WHERE worker_id=1').fetchone()[0],1)

    def test_public_browse_detail_stats_quote_and_order_enforce_visibility(self):
        hidden = self.create(1)
        flagged = self.create(2)
        human = self.create(3)
        db = self.core.get_db()
        # Verified operator state comes only from the signed webhook's committed local state.
        db.execute("UPDATE worker_profiles SET payout_method='stripe_connect_active' WHERE user_id=2")
        db.commit()
        status, listing = self.request('GET','/services',query='search=Research&category=research')
        self.assertEqual(status,200)
        self.assertEqual(listing['total'],2)
        self.assertEqual({x['id'] for x in listing['services']},{flagged['id'],human['id']})
        self.assertEqual(next(x for x in listing['services'] if x['id']==flagged['id'])['provider_type'],'ai')
        self.assertEqual(self.request('GET','/services',query='provider_type=ai')[1]['total'],1)
        self.assertEqual(self.request('GET','/services',query='provider_type=human')[1]['total'],1)
        self.assertEqual(self.request('GET','/platform/stats')[1]['services_listed'],2)
        self.assertEqual(self.request('GET',f"/services/{hidden['id']}")[0],404)
        self.assertEqual(self.request('GET',f"/services/{hidden['id']}",token='tok-4')[0],404)
        for token in ('tok-1','tok-5'):
            status, detail = self.request('GET',f"/services/{hidden['id']}",token=token)
            self.assertEqual(status,200)
            self.assertEqual(detail['visibility'],'hidden_unverified_agent')
            self.assertEqual(detail['provider_type'],'ai')
        self.assertEqual(self.request('GET',f"/services/{flagged['id']}")[1]['provider_type'],'ai')
        self.assertEqual(self.request('GET',f"/services/{human['id']}")[0],200)
        self.assertEqual(self.request('GET',f"/services/{hidden['id']}/quote",token='tok-4')[0],409)
        status, order = self.request('POST',f"/services/{hidden['id']}/order",payload={'idempotency_key':'hidden-order'},token='tok-4')
        self.assertEqual(status,409,order)
        self.assertIn('not yet verified for payouts',order['error'])
        self.assertEqual(db.execute('SELECT COUNT(*) FROM orders').fetchone()[0],0)
        self.assertEqual(self.request('GET','/me/services',token='tok-1')[1]['services'][0]['visibility'],'hidden_unverified_agent')


class AIListingPolicyReviewRegressions(AIListingPolicyTests):
    """Blockers from independent review of PR #142 (fail-open NULL, subdomains)."""

    def test_missing_profile_or_null_payout_method_fails_closed(self):
        hidden = self.create(1)
        db = self.core.get_db()
        for mutate in ("DELETE FROM worker_profiles WHERE user_id=1",
                       "INSERT OR REPLACE INTO worker_profiles (user_id,payout_method,payout_account_id) VALUES (1,NULL,'acct_live_test')"):
            with self.subTest(mutate=mutate):
                db.execute(mutate)
                db.commit()
                self.assertTrue(self.core.service_hidden_for_unverified_agent(db, hidden['id']))
                self.assertEqual(self.request('GET', f"/services/{hidden['id']}")[0], 404)
                self.assertEqual(self.request('GET', f"/services/{hidden['id']}/quote", token='tok-4')[0], 409)
                with mock.patch.object(self.core, 'stripe', create=True) as fake_stripe:
                    status, _ = self.request('POST', f"/services/{hidden['id']}/order",
                                             payload={'idempotency_key': 'null-' + str(len(mutate))}, token='tok-4')
                    self.assertEqual(status, 409)
                    self.assertFalse(fake_stripe.mock_calls)
                self.assertEqual(db.execute('SELECT COUNT(*) FROM orders').fetchone()[0], 0)
                self.assertNotIn(hidden['id'], {s['id'] for s in self.request('GET', '/services')[1]['services']})

    def test_subdomains_are_agents_but_suffix_lookalikes_are_not(self):
        db = self.core.get_db()
        db.executemany('INSERT INTO users (id,email,name,password_hash) VALUES (?,?,?,?)', [
            (11, 'bot@team.ILANDS.app', 'Sub', 'x'),
            (12, 'bot@evil-ilands.app', 'Lookalike', 'x'),
            (13, 'bot@ilands.app.evil.com', 'Prefix', 'x'),
        ])
        db.executemany("INSERT INTO sessions (user_id,token,expires_at) VALUES (?,?,datetime('now','+1 day'))",
                       [(i, f'tok-{i}') for i in (11, 12, 13)])
        db.commit()
        self.assertTrue(self.core.is_agent_account(db, 11))
        self.assertFalse(self.core.is_agent_account(db, 12))
        self.assertFalse(self.core.is_agent_account(db, 13))
        sub = self.create(11)
        self.assertEqual(sub['provider_type'], 'ai')
        self.assertEqual(self.request('GET', f"/services/{sub['id']}")[0], 404)
        self.assertEqual(self.create(12)['provider_type'], 'human')

    def test_malformed_emails_use_last_at_sign_and_require_one(self):
        db = self.core.get_db()
        cases = {21: ('ilands.app', False), 22: ('x@evil.com@ilands.app', True),
                 23: ('x@ilands.app@evil.com', False), 24: ('  Bot@ILANDS.APP  ', True),
                 25: ('@ilands.app', True), 26: ('bot@', False)}
        db.executemany('INSERT INTO users (id,email,name,password_hash) VALUES (?,?,?,?)',
                       [(i, e, 'M', 'x') for i, (e, _) in cases.items()])
        db.executemany("INSERT INTO sessions (user_id,token,expires_at) VALUES (?,?,datetime('now','+1 day'))",
                       [(i, f'tok-{i}') for i in cases])
        db.commit()
        for uid, (email, agent) in cases.items():
            with self.subTest(email=email):
                self.assertEqual(self.core.is_agent_account(db, uid), agent)
        human = self.create(21)
        self.assertEqual(human['provider_type'], 'human')
        self.assertEqual(self.request('GET', f"/services/{human['id']}")[0], 200)
        bypass = self.create(22)
        self.assertEqual(bypass['provider_type'], 'ai')
        self.assertEqual(self.request('GET', f"/services/{bypass['id']}")[0], 404)
        db.execute("INSERT INTO services (worker_id,title,description,category,status,provider_type) VALUES (22,'Legacy','Old','research','active','human')")
        db.commit()
        self.core.init_db()
        self.assertEqual(db.execute("SELECT COUNT(*) FROM services WHERE worker_id=22 AND provider_type!='ai'").fetchone()[0], 0)

