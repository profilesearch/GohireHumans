"""Bounded discovery spec exercised against real local routes and MCP transport."""
import importlib.util
import io
import json
from pathlib import Path
import unittest
from unittest import mock
from urllib.parse import urlsplit

import test_public_onboarding_contracts as onboarding

ROOT = Path(__file__).resolve().parents[1]


class AgentDiscoveryContracts(unittest.TestCase):
    setUp = onboarding.PublicOnboardingContracts.setUp
    tearDown = onboarding.PublicOnboardingContracts.tearDown
    request = onboarding.PublicOnboardingContracts.request

    def spec(self):
        manifest = json.loads((ROOT / 'frontend/.well-known/ai-plugin.json').read_text())
        url = urlsplit(manifest['api']['url'])
        self.assertEqual(url.netloc, 'www.gohirehumans.com')
        target = ROOT / 'frontend' / url.path.lstrip('/')
        self.assertTrue(target.is_file(), 'Manifest must resolve to a shipped OpenAPI asset')
        spec = json.loads(target.read_text())
        self.assertEqual(spec['openapi'], '3.1.0')
        self.assertEqual(spec['security'], [])
        self.assertIn('partial', spec['info']['description'].lower())
        self.assertEqual(set(spec['paths']), {'/categories', '/services', '/jobs'})
        return spec

    def test_manifest_resolves_to_bounded_spec_and_real_read_routes(self):
        spec = self.spec()
        self.assertEqual(spec['servers'][0]['url'],
                         'https://gohirehumans-production.up.railway.app/api/v1')
        for path, operations in spec['paths'].items():
            with self.subTest(path=path):
                self.assertEqual(set(operations), {'get'})
                operation = operations['get']
                self.assertEqual(operation['security'], [])
                status, body = self.request('GET', '/api/v1' + path)
                self.assertEqual(status, 200, body)
                schema = operation['responses']['200']['content']['application/json']['schema']
                for field in schema['required']:
                    self.assertIn(field, body)
                self.assertIsInstance(body[path[1:]], list)
                if path != '/categories':
                    self.assertEqual(body['total'], 0)
                    self.assertEqual(body['total_pages'], 0)
                    self.assertEqual(body['per_page'], 20)
                    self.assertEqual(self.request('GET', '/api/v1' + path, query='page=0')[0], 400)
                    status, capped = self.request('GET', '/api/v1' + path, query='per_page=101')
                    self.assertEqual(status, 200)
                    self.assertEqual(capped['per_page'], 100)

    def assert_schema(self, schema, value):
        """Validate the keywords used by this deliberately small inline contract.

        The audit also validates the entire document with an OpenAPI validator.
        Keep the repository's stdlib-only unittest runner dependency-free.
        """
        types = {'object': dict, 'array': list, 'string': str, 'integer': int,
                 'number': (int, float), 'null': type(None)}
        allowed = schema.get('type')
        allowed = allowed if isinstance(allowed, list) else [allowed]
        self.assertTrue(any(isinstance(value, types[k]) and
                            not (isinstance(value, bool) and k in ('integer', 'number'))
                            for k in allowed), (schema, value))
        if isinstance(value, dict):
            for key in schema.get('required', []):
                self.assertIn(key, value)
            for key, subschema in schema.get('properties', {}).items():
                if key in value:
                    self.assert_schema(subschema, value[key])
        elif isinstance(value, list):
            for item in value:
                self.assert_schema(schema['items'], item)
        if 'enum' in schema:
            self.assertIn(value, schema['enum'])
        if isinstance(value, (int, float)):
            if 'minimum' in schema:
                self.assertGreaterEqual(value, schema['minimum'])
            if 'maximum' in schema:
                self.assertLessEqual(value, schema['maximum'])

    def seed_discovery(self):
        db = self.api.get_db()
        db.execute("INSERT INTO users (id,email,name) VALUES (1,'discovery@example.invalid','Local fixture')")
        for index, kind in enumerate(('fixed', 'hourly', 'custom'), 1):
            db.execute("""INSERT INTO services
                (id,worker_id,title,description,category,pricing_type,price,hourly_rate,
                 provider_type,created_at) VALUES (?,1,?,'Discovery fixture','research',?,?,?,?,?)""",
                (index, f'Service {index}', kind, 25 if kind == 'fixed' else None,
                 20 if kind == 'hourly' else None, 'human' if index == 1 else 'ai',
                 f'2026-01-0{index} 00:00:00'))
        statuses = ('open', 'reviewing', 'hired', 'in_progress', 'completed', 'canceled')
        for index, status in enumerate(statuses, 1):
            db.execute("""INSERT INTO jobs
                (id,employer_id,title,description,category,budget_type,budget_amount,
                 location_type,status,created_at) VALUES (?,1,?,'Discovery fixture','research',?,25,?,?,?)""",
                (index, f'Job {index}', 'fixed' if index % 2 else 'hourly',
                 ('remote', 'on_site', 'hybrid')[(index - 1) % 3], status,
                 f'2026-01-0{index} 00:00:00'))
        db.commit()

    def test_populated_responses_pagination_and_query_contract(self):
        from urllib.parse import urlencode
        self.seed_discovery()
        spec = self.spec()
        for path, expected_ids in (('/services', {1, 2, 3}), ('/jobs', {1, 2})):
            operation = spec['paths'][path]['get']
            schemas = {code: response['content']['application/json']['schema']
                       for code, response in operation['responses'].items()}
            parameters = {p['name']: p for p in operation['parameters']}
            expected_enums = ({'pricing_type': ['fixed', 'hourly', 'custom'],
                               'provider_type': ['human', 'ai']} if path == '/services' else
                              {'budget_type': ['fixed', 'hourly'],
                               'location_type': ['remote', 'on_site', 'hybrid'],
                               'status': ['open', 'reviewing', 'hired', 'in_progress', 'completed', 'canceled']})
            self.assertTrue(set(expected_enums) <= set(parameters))
            seen = []
            for page in range(1, len(expected_ids) + 2):
                status, body = self.request('GET', '/api/v1' + path,
                                            query=urlencode({'page': page, 'per_page': 1}))
                self.assertEqual(status, 200, body)
                self.assert_schema(schemas['200'], body)
                self.assertEqual((body['total'], body['total_pages'], body['page'], body['per_page']),
                                 (len(expected_ids), len(expected_ids), page, 1))
                seen.extend(row['id'] for row in body[path[1:]])
            self.assertEqual(len(seen), len(set(seen)))
            self.assertEqual(set(seen), expected_ids)
            for name, values in expected_enums.items():
                self.assertEqual(parameters[name]['schema']['enum'], values)
                for value in values:
                    query = {name: value}
                    if path == '/jobs' and name == 'location_type' and value == 'hybrid':
                        query['status'] = 'hired'
                    status, body = self.request('GET', '/api/v1' + path, query=urlencode(query))
                    self.assertEqual(status, 200, body)
                    self.assert_schema(schemas['200'], body)
                    self.assertTrue(body[path[1:]], query)
                    self.assertTrue(all(row[name] == value for row in body[path[1:]]))
                status, body = self.request('GET', '/api/v1' + path, query=urlencode({name: 'not-a-known-value'}))
                self.assertEqual(status, 200, body)
                self.assertEqual(body['total'], 0)
                self.assert_schema(schemas['200'], body)
            for query in ('category=research', 'search=fixture', 'per_page=101'):
                status, body = self.request('GET', '/api/v1' + path, query=query)
                self.assertEqual(status, 200, body)
                self.assert_schema(schemas['200'], body)
                self.assertEqual(body['total'], len(expected_ids))
            for query in ('page=0', 'page=abc', 'per_page=0', 'per_page=-1', 'per_page=1.5'):
                status, body = self.request('GET', '/api/v1' + path, query=query)
                self.assertEqual(status, 400, body)
                self.assert_schema(schemas['400'], body)

    def mcp(self):
        path = ROOT / 'backend/mcp_server.py'
        spec = importlib.util.spec_from_file_location('discovery_mcp', path)
        assert spec is not None and spec.loader is not None
        module = importlib.util.module_from_spec(spec)
        with mock.patch.dict('os.environ', {}, clear=True):
            spec.loader.exec_module(module)
        return module

    def test_mcp_authority_and_truthful_information(self):
        mcp = self.mcp()
        docs = [mcp.handle_resource(uri)['contents'][0]['text'] for uri in
                ('gohirehumans://api-docs', 'gohirehumans://mcp-quickstart')]
        docs.append((ROOT / 'backend/mcp-package/README.md').read_text())
        for doc in docs:
            with self.subTest(surface=doc[:65]):
                self.assertIn('write', doc)
                self.assertIn('can charge', doc)
                self.assertIn('session-only', doc)
                self.assertIn('job hiring is currently paused', doc.lower())
        with mock.patch.object(mcp, 'api_request', return_value={'error': 'unavailable'}):
            info = mcp.handle_get_platform_info({})[0]['text']
            pricing = mcp.handle_get_pricing_info({})[0]['text']
        for text in (info, pricing):
            self.assertNotIn('All freelancers are screened and verified', text)
            self.assertNotIn('All payments are held', text)
            self.assertIn('where configured', text)
            self.assertIn('not', text)
        tools = {tool['name']: tool['description'] for tool in mcp.TOOLS}
        self.assertIn('can charge', tools['hire_worker'])
        self.assertIn('session-only', tools['release_payment'])
        manifest = json.loads((ROOT / 'frontend/.well-known/ai-plugin.json').read_text())
        self.assertIn('read-only', manifest['description_for_model'])
        self.assertNotIn('escrow-protected', json.dumps(manifest).lower())
        guide = (ROOT / 'frontend/ai-integration.html').read_text()
        self.assertNotIn('AI agents cannot make live phone calls', guide)
        self.assertNotIn('they cannot pick up a phone', guide)
        self.assertIn('job hiring is currently paused', guide.lower())
        self.assertNotIn('opaque...pan>', guide)

    def test_executable_mcp_headers_routes_and_payment_copy(self):
        mcp = self.mcp()
        calls = []
        def transport(req, timeout):
            calls.append(req)
            url = urlsplit(req.full_url)
            status, body = self.request(req.method, url.path, query=url.query)
            self.assertEqual(status, 200, body)
            return io.BytesIO(json.dumps(body).encode())
        with mock.patch.object(mcp.urllib.request, 'urlopen', side_effect=transport):
            for token, key in [('', ''), ('local-session-placeholder', ''), ('', 'ghh_local-placeholder')]:
                mcp.AUTH_TOKEN, mcp.API_KEY = token, key
                result = mcp.api_request('GET', '/categories')
                self.assertEqual(result['categories'], self.api.VALID_CATEGORIES)
                headers = {k.lower(): v for k, v in calls[-1].header_items()}
                self.assertEqual(headers.get('authorization'), 'Bearer ' + token if token else None)
                self.assertEqual(headers.get('x-api-key'), key or None)
            mcp.AUTH_TOKEN = mcp.API_KEY = ''
            mcp.handle_search_services({'query': 'example', 'limit': 2})
            mcp.handle_browse_jobs({'limit': 2})
        self.assertTrue(all(req.method == 'GET' for req in calls))
        with mock.patch.object(mcp, 'api_request', side_effect=[
            {'id': 1, 'title': 'Local fixture', 'price': 25, 'worker_id': 2},
            {'order': {'id': 3, 'status': 'pending', 'amount': 25}},
        ]):
            result = mcp.handle_hire_worker({'service_id': 1, 'requirements': '',
                                             'idempotency_key': 'local-fixture-operation-1'})[0]['text']
        self.assertNotIn('Payment is held in escrow', result)
        self.assertIn('returned state', result)
        with mock.patch.object(mcp, 'api_request', return_value={'status': 'completed'}):
            result = mcp.handle_release_payment({'order_id': 3})[0]['text']
        self.assertNotIn('Funds have been transferred', result)
        self.assertNotIn('Payment released successfully', result)
        self.assertIn('bank settlement', result)


if __name__ == '__main__':
    unittest.main()
