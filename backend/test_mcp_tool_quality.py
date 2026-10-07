"""Tool-definition quality contract for the GoHireHumans MCP server.

Directories (Glama's TDQS) and MCP clients rank and route tools from these
descriptions and annotations. Every check below pins a behavior that the
backend actually has, so the definitions can't drift into overclaims.
"""
import importlib.util
import json
import os
from pathlib import Path
import unittest
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
READ_ONLY = {
    'search_services', 'get_service_details', 'get_categories', 'browse_jobs',
    'get_job_status', 'search_workers', 'get_recommended', 'get_pricing_info',
    'get_platform_info',
}
WRITES = {'create_job', 'hire_worker', 'release_payment', 'submit_review'}
MONEY = {'hire_worker', 'release_payment'}
SIBLINGS = {
    'search_services': ('search_workers', 'get_recommended', 'get_service_details'),
    'search_workers': ('search_services', 'get_recommended'),
    'get_recommended': ('search_services', 'hire_worker'),
    'get_pricing_info': ('get_platform_info',),
    'get_platform_info': ('get_pricing_info', 'get_categories'),
    'browse_jobs': ('search_services', 'search_workers', 'get_job_status'),
    'create_job': ('hire_worker', 'get_job_status'),
    'hire_worker': ('release_payment', 'get_job_status'),
    'release_payment': ('get_job_status', 'submit_review'),
    'get_service_details': ('search_services', 'hire_worker'),
    'get_categories': ('search_services', 'get_platform_info'),
    'get_job_status': ('hire_worker', 'create_job'),
}


def load(path):
    with mock.patch.dict(os.environ, {}, clear=True):
        spec = importlib.util.spec_from_file_location(f'mcp_quality_{path.parent.name}', path)
        assert spec is not None and spec.loader is not None
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
    return module


class ToolDefinitionQualityTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.module = load(ROOT / 'backend/mcp_server.py')
        cls.tools = {tool['name']: tool for tool in cls.module.TOOLS}
        cls.desc = {name: tool['description'] for name, tool in cls.tools.items()}

    def test_same_thirteen_tools_and_handlers(self):
        self.assertEqual(len(self.module.TOOLS), 13)
        self.assertEqual(set(self.tools), READ_ONLY | WRITES)
        self.assertEqual(set(self.tools), set(self.module.TOOL_HANDLERS))
        json.dumps(self.module.TOOLS)  # tools/list must stay JSON-serialisable

    def test_annotations_match_side_effects(self):
        for name, tool in self.tools.items():
            with self.subTest(tool=name):
                notes = tool.get('annotations')
                self.assertIsInstance(notes, dict)
                self.assertTrue(notes.get('title'))
                self.assertIs(notes.get('readOnlyHint'), name in READ_ONLY)
                if name in WRITES:
                    self.assertIs(notes.get('destructiveHint'), name in MONEY)
                    self.assertIn('idempotentHint', notes)
        self.assertIs(self.tools['hire_worker']['annotations']['idempotentHint'], True)
        self.assertIs(self.tools['release_payment']['annotations']['idempotentHint'], False)

    def test_every_description_states_auth(self):
        for name in READ_ONLY - {'get_job_status'}:
            with self.subTest(tool=name):
                self.assertIn('Read-only; no API key needed.', self.desc[name])
        self.assertIn('Read-only.', self.desc['get_job_status'])
        for name in WRITES:
            with self.subTest(tool=name):
                self.assertIn('GOHIREHUMANS_AUTH_TOKEN', self.desc[name])
        for name in MONEY | {'create_job'}:
            with self.subTest(tool=name):
                self.assertIn("account owner", self.desc[name])

    def test_descriptions_route_between_siblings(self):
        for name, siblings in SIBLINGS.items():
            for sibling in siblings:
                with self.subTest(tool=name, sibling=sibling):
                    self.assertIn(sibling, self.desc[name])
        self.assertIn('one result per listing', self.desc['search_services'])
        self.assertIn('one result per provider', self.desc['search_workers'])

    def test_money_and_lifecycle_facts_match_backend(self):
        hire = self.desc['hire_worker']
        for fact in ('saved payment method', 'charged immediately', 'only when the employer approves',
                     '1% platform fee and a fixed 3% processing charge',
                     'both the read scope', 'the write scope', "isn't locked to an earlier quote",
                     'no self-serve cancel option',
                     'same idempotency_key'):
            self.assertIn(fact, hire)
        release = self.desc['release_payment']
        for fact in ('cannot be undone', "'submitted' status", 'unset GOHIREHUMANS_API_KEY',
                     'rejected even alongside the session token',
                     'another milestone', "saved card is charged"):
            self.assertIn(fact, release)
        props = self.tools['release_payment']['inputSchema']['properties']
        self.assertTrue(props['milestone_id']['description'].startswith('Ignored'))
        self.assertTrue(props['rating']['description'].startswith('Not recorded'))
        review = self.desc['submit_review']
        for fact in ('completed order', 'once', "can't be edited or deleted", '14 days'):
            self.assertIn(fact, review)
        job = self.desc['create_job']
        self.assertIn("published immediately", job)
        self.assertIn('does not hire anyone or charge anything', job)
        self.assertIn('not accepted right now',
                      self.tools['create_job']['inputSchema']['properties']['budget_type']['description'])
        self.assertIn('at most 999,999.99',
                      self.tools['create_job']['inputSchema']['properties']['budget_amount']['description'])
        self.assertIn('not a spending cap',
                      self.tools['hire_worker']['inputSchema']['properties']['budget_amount']['description'])
        status = self.desc['get_job_status']
        self.assertIn("only the order's employer or worker (or a site admin) can see it", status)
        self.assertIn('the hired worker or a site admin can see it', status)
        self.assertIn('numbered separately', status)
        self.assertIn("paused listing can still be shown here but can't be ordered", self.desc['get_service_details'])

    def test_fee_copy_matches_canonical_pricing(self):
        pricing = self.desc['get_pricing_info']
        self.assertIn('employers pay a 1% platform fee plus a fixed 3% processing charge where checkout is configured',
                      pricing)
        self.assertIn('not an escrow provider', pricing)
        for name, text in self.desc.items():
            with self.subTest(tool=name):
                self.assertNotIn('Stripe processing plus', text)

    def test_no_overclaims(self):
        everything = json.dumps(self.module.TOOLS)
        for phrase in ('availability', 'AI-optimized', 'AI-powered', 'past performance',
                       'worker activity', 'experience,', 'releases payment for the entire order',
                       'freelancers can apply', 'best-rated first', '1,000,000',
                       'any request carrying an API key', 'different numbers'):
            with self.subTest(phrase=phrase):
                self.assertNotIn(phrase, everything)

    def test_descriptions_stay_compact(self):
        for name, text in self.desc.items():
            with self.subTest(tool=name):
                self.assertLessEqual(len(text), 900)
                self.assertNotIn('  ', text)

    def test_package_copy_is_identical(self):
        self.assertEqual((ROOT / 'backend/mcp_server.py').read_bytes(),
                         (ROOT / 'backend/mcp-package/mcp_server.py').read_bytes())


class BackendBehaviourBehindDescriptions(unittest.TestCase):
    """Run the real API against a throwaway database so each description claim is
    pinned to behaviour, not only to wording."""

    def setUp(self):
        import sqlite3  # noqa: F401  (api_core needs it loaded)
        import tempfile
        from test_deep_audit_regressions import load_api_core
        self.tmp = tempfile.TemporaryDirectory()
        self.env = mock.patch.dict(os.environ, {
            'DATABASE_PATH': os.path.join(self.tmp.name, 'mcp-quality.db'),
            'DISABLE_AUTO_SEED': '1'})
        self.env.start()
        self.api = load_api_core()
        self.api._db_path_resolved = None
        self.api.init_db()
        self.mcp = load(ROOT / 'backend/mcp_server.py')
        with self.api.get_db() as db:
            for uid, name in ((1, 'Admin'), (2, 'Worker Low'), (3, 'Worker High'), (4, 'Buyer'), (5, 'Other')):
                db.execute("INSERT INTO users(id,email,name,password_hash,is_admin) VALUES(?,?,?,?,?)",
                           [uid, f'u{uid}@example.test', name, 'x', 1 if uid == 1 else 0])
            for uid, tok in ((1, 'admin'), (4, 'buyer'), (5, 'other')):
                db.execute("INSERT INTO sessions(user_id,token,expires_at) VALUES(?,?,datetime('now','+1 day'))", [uid, tok])
            # Worker 2: higher LISTING rating, lower PROFILE rating.
            db.execute("INSERT INTO worker_profiles(user_id,avg_rating) VALUES(2,2.0)")
            db.execute("INSERT INTO worker_profiles(user_id,avg_rating) VALUES(3,5.0)")
            db.execute("""INSERT INTO services(id,worker_id,title,description,category,pricing_type,price,hourly_rate,status,avg_rating)
                          VALUES(11,2,'Hourly QA','QA by the hour','testing','hourly',NULL,40,'active',4.9)""")
            db.execute("""INSERT INTO services(id,worker_id,title,description,category,pricing_type,price,hourly_rate,status,avg_rating)
                          VALUES(12,3,'Fixed QA','QA fixed','testing','fixed',25,NULL,'active',3.0)""")
            db.execute("""INSERT INTO services(id,worker_id,title,description,category,pricing_type,price,hourly_rate,status,avg_rating)
                          VALUES(13,3,'Custom QA','QA custom','testing','custom',NULL,NULL,'active',2.0)""")
            db.execute("""INSERT INTO services(id,worker_id,title,description,category,pricing_type,price,hourly_rate,status,avg_rating)
                          VALUES(14,3,'Paused QA','QA paused','testing','fixed',30,NULL,'paused',1.0)""")
            db.execute("INSERT INTO jobs(id,employer_id,title,description,category,budget_type,budget_amount,status) VALUES(7,4,'Job seven','d','testing','fixed',50,'in_progress')")
            db.execute("INSERT INTO orders(id,type,worker_id,employer_id,status,total_amount) VALUES(7,'service_order',2,4,'pending',10)")
            import hashlib
            for kid, scopes in ((1, '["write"]'), (2, '["read","write"]')):
                db.execute("INSERT INTO api_keys(id,user_id,key_hash,key_prefix,scopes) VALUES(?,?,?,?,?)",
                           [kid, 4, hashlib.sha256(f'ghh_test_key_{kid}'.encode()).hexdigest(), f'ghh_{kid}', scopes])
            db.commit()

    def tearDown(self):
        self.env.stop()
        self.tmp.cleanup()

    def request(self, method, path, token='', payload=None, query='', api_key=''):
        import contextlib
        import io
        from test_deep_audit_regressions import parse_cgi_output
        for attr in ('body_cache', 'raw_body', 'authenticated_api_key_id', 'api_key_accounting_intent_id'):
            if hasattr(self.api._request_ctx, attr):
                delattr(self.api._request_ctx, attr)
        raw = json.dumps(payload or {})
        c = self.api._request_ctx
        c.request_method = method; c.path_info = path; c.query_string = query
        c.http_authorization = f'Bearer {token}' if token else ''
        c.http_x_api_key = api_key; c.stdin_data = raw; c.content_type = 'application/json'
        c.content_length = str(len(raw)); c.remote_addr = '127.0.0.1'; c.http_stripe_signature = ''
        with contextlib.redirect_stdout(io.StringIO()) as out:
            self.api.handle_request()
        return parse_cgi_output(out.getvalue())

    def as_api(self):
        """Route the MCP server's api_request through the in-process backend."""
        import urllib.parse

        def fake(method, path, body=None, params=None):
            query = urllib.parse.urlencode(params or {})
            status, data = self.request(method, path, payload=body, query=query)
            return data if status < 400 else {'error': data.get('error', status)}
        return mock.patch.object(self.mcp, 'api_request', side_effect=fake)

    def test_hire_worker_needs_read_and_write_scopes(self):
        # hire_worker's preflight is GET /services/{id}; a write-only key is refused there.
        status, _ = self.request('GET', '/services/12', api_key='ghh_test_key_1')
        self.assertEqual(status, 403)
        status, _ = self.request('GET', '/services/12', api_key='ghh_test_key_2')
        self.assertEqual(status, 200)

    def test_release_payment_rejects_valid_api_key_even_with_session(self):
        status, body = self.request('POST', '/orders/7/approve', token='buyer', api_key='ghh_test_key_2',
                                    payload={'action': 'approve'})
        self.assertEqual(status, 403, body)

    def test_hourly_custom_and_paused_listings_are_described_honestly(self):
        with self.as_api():
            hourly = self.mcp.handle_get_service_details({'service_id': '11'})[0]['text']
            custom = self.mcp.handle_get_service_details({'service_id': '13'})[0]['text']
            paused = self.mcp.handle_get_service_details({'service_id': '14'})[0]['text']
            listing = self.mcp.handle_search_services({'category': 'testing'})[0]['text']
            workers = self.mcp.handle_search_workers({'category': 'testing'})[0]['text']
            ranked = self.mcp.handle_get_recommended({'task_description': 'QA testing', 'budget_range': 'under $50'})[0]['text']
        self.assertIn('**Price:** $40/hour', hourly)
        self.assertIn('Custom (amount agreed at order time)', custom)
        self.assertIn('Paused QA', paused)  # paused listings are still returned...
        for text in (hourly, custom, listing, workers, ranked):
            self.assertNotIn('None', text)
        self.assertIn('Price: $40/hour', listing)
        self.assertIn('plus custom-priced listings', workers)
        self.assertIn('Hourly QA', ranked)          # $40/hour counts as under $50
        self.assertNotIn('Custom QA', ranked)       # custom pricing is excluded when a range is given
        # ...but can't be ordered.
        status, _ = self.request('GET', '/services/14/quote', token='buyer')
        self.assertEqual(status, 404)

    def test_search_workers_order_follows_listing_rating_not_profile_rating(self):
        with self.as_api():
            text = self.mcp.handle_search_workers({'category': 'testing'})[0]['text']
        self.assertLess(text.index('Worker Low'), text.index('Worker High'))
        self.assertIn('Rating: 2.0', text)  # profile rating shown, though listed first

    def test_job_budget_ceiling_is_999999_99(self):
        base = {'title': 'Ceiling', 'description': 'Budget ceiling check', 'category': 'testing', 'budget_type': 'fixed'}
        status, _ = self.request('POST', '/jobs', token='buyer', payload={**base, 'budget_amount': 1000000})
        self.assertEqual(status, 400)
        status, body = self.request('POST', '/jobs', token='buyer', payload={**base, 'budget_amount': 999999.99})
        self.assertNotEqual(status, 400, body)

    def test_job_and_order_ids_share_numbers_and_admins_can_read(self):
        status, order = self.request('GET', '/orders/7', token='admin')
        self.assertEqual(status, 200, order)
        status, job = self.request('GET', '/jobs/7', token='admin')
        self.assertEqual(status, 200, job)
        status, _ = self.request('GET', '/orders/7', token='other')
        self.assertEqual(status, 403)


if __name__ == '__main__':
    unittest.main()
