"""Execute public onboarding examples against disposable SQLite, never production.

HTML examples are the inputs (not copies maintained in the test). Responses shown
as excerpts must preserve real keys/types. All outbound transports fail closed.
"""
import contextlib
import html
import io
import json
import os
import re
import shlex
import socket
import tempfile
import unittest
from pathlib import Path
from unittest import mock
from urllib.parse import urlsplit

from test_deep_audit_regressions import load_api_core, parse_cgi_output

ROOT = Path(__file__).resolve().parents[1]
SURFACES = [
    'frontend/api-docs.html', 'frontend/ai-integration.html',
    'frontend/agent-onboarding.html', 'frontend/how-it-works.html',
    'frontend/faq.html', 'backend/mcp_server.py',
    'backend/mcp-package/mcp_server.py', 'backend/mcp-package/README.md',
]


def text(source):
    return html.unescape(re.sub(r'<[^>]+>', '', source))


def section(filename, identifier):
    source = (ROOT / 'frontend' / filename).read_text()
    start = source.index(f'id="{identifier}"')
    return text(source[start:source.index('</section>', start)])


def objects(source):
    """Read complete JSON objects embedded in rendered HTTP examples."""
    result = []
    decoder = json.JSONDecoder()
    i = 0
    while i < len(source):
        if source[i] == '{':
            try:
                value, consumed = decoder.raw_decode(source[i:])
            except ValueError:
                i += 1
                continue
            result.append(value)
            i += consumed
        else:
            i += 1
    return result


class PublicOnboardingContracts(unittest.TestCase):
    def setUp(self):
        self.stack = contextlib.ExitStack()
        self.addCleanup(self.stack.close)
        tmp = self.stack.enter_context(tempfile.TemporaryDirectory())
        self.stack.enter_context(mock.patch.dict(os.environ, {
            'DATABASE_PATH': str(Path(tmp) / 'docs.db'), 'DISABLE_AUTO_SEED': '1',
        }, clear=True))
        self.network = self.stack.enter_context(mock.patch.object(
            socket.socket, 'connect', side_effect=AssertionError('Network forbidden')))
        self.api = load_api_core()
        self.api._db_path_resolved = None
        self.api._seeded = False
        self.email = self.stack.enter_context(mock.patch.object(self.api, 'send_welcome_email'))
        self.transport = self.stack.enter_context(mock.patch.object(
            self.api.urllib.request, 'urlopen', side_effect=AssertionError('HTTP forbidden')))
        self.api.init_db()

    def tearDown(self):
        self.network.assert_not_called()
        self.transport.assert_not_called()

    def request(self, method, path, payload=None, token='', query='', api_key='', headers=None):
        ctx = self.api._request_ctx
        for attr in ('body_cache', 'raw_body'):
            if hasattr(ctx, attr):
                delattr(ctx, attr)
        raw = json.dumps(payload or {})
        ctx.request_method, ctx.path_info, ctx.query_string = method, path, query
        ctx.http_authorization = f'Bearer {token}' if token else ''
        ctx.http_x_api_key = api_key
        for name, value in (headers or {}).items():
            setattr(ctx, 'http_' + name.lower().replace('-', '_'), value)
        ctx.stdin_data, ctx.stdin_data_raw = raw, raw.encode()
        ctx.content_type, ctx.content_length = 'application/json', str(len(raw.encode()))
        ctx.remote_addr = '127.0.0.1'
        with contextlib.redirect_stdout(io.StringIO()) as out:
            self.api.handle_request()
        return parse_cgi_output(out.getvalue())

    def assert_excerpt(self, example, actual):
        for key, value in example.items():
            self.assertIn(key, actual, f'Documented response field {key!r} is absent')
            self.assertIs(type(actual[key]), type(value), f'Type mismatch for {key}')

    def test_documented_registration_login_and_profile(self):
        register = objects(section('api-docs.html', 'auth-register'))
        request, expected = register
        status, created = self.request('POST', '/auth/register', request)
        self.assertEqual(status, 201, created)
        self.assert_excerpt(expected, created)
        self.assertEqual(created['name'], request['name'])
        self.assertIsInstance(created['id'], int)
        self.assertRegex(created['token'], r'^[0-9a-f]{64}$')
        self.assertNotIn('role', request, 'Registration does not set an account role')
        login = objects(section('api-docs.html', 'auth-login'))
        status, authenticated = self.request('POST', '/auth/login', {
            'email': request['email'], 'password': request['password'],
        })
        self.assertEqual(status, 200, authenticated)
        self.assert_excerpt(login[-1], authenticated)
        profile = section('api-docs.html', 'auth-me')
        route = re.search(r'GET (/[^\s]+)', profile).group(1)
        status, current = self.request('GET', route, token=authenticated['token'])
        self.assertEqual(status, 200, current)
        self.assert_excerpt(objects(profile)[0], current)
        self.assertNotIn('password_hash', current)

    def register_owner(self):
        payload = objects(section('api-docs.html', 'auth-register'))[0]
        status, owner = self.request('POST', '/auth/register', payload)
        self.assertEqual(status, 201, owner)
        return owner

    def test_documented_job_creation(self):
        owner = self.register_owner()
        examples = objects(section('api-docs.html', 'tasks-create'))
        guide = objects(section('ai-integration.html', 'step-create-task'))[0]
        for payload in [examples[0], guide] + examples[2:]:
            with self.subTest(title=payload.get('title')):
                status, denied = self.request('POST', '/api/v1/jobs', payload)
                self.assertEqual(status, 401, denied)
                status, created = self.request('POST', '/api/v1/jobs', payload, owner['token'])
                self.assertEqual(status, 201, created)
                self.assertIs(type(created['id']), int)
                self.assertEqual(created['status'], 'open')
                for key in ('title', 'description', 'category', 'budget_type', 'budget_amount',
                            'location_type', 'location_detail', 'due_by'):
                    if key in payload:
                        self.assertEqual(created[key], payload[key], key)
                self.assertNotIn('deadline', payload)
                self.assertNotIn('location', payload)
                self.assertNotIn('budget', payload)
                if payload == examples[0]:
                    self.assert_excerpt(examples[1], created)
        self.assertTrue(any(p.get('location_type') == 'on_site' and p.get('location_detail')
                            and p.get('due_by') for p in examples[2:]),
                        'Include a native on-site example with location_detail and due_by')
        docs = section('api-docs.html', 'tasks-create')
        self.assertNotRegex(docs, r'\bdeadline\b|"local"')
        self.assertIn('hybrid', docs)

    def test_documented_public_discovery(self):
        owner = self.register_owner()
        payload = objects(section('api-docs.html', 'tasks-create'))[0]
        ids = set()
        for index in range(3):
            status, job = self.request('POST', '/api/v1/jobs',
                                       dict(payload, title=f'Discovery example {index}'), owner['token'])
            self.assertEqual(status, 201, job)
            ids.add(job['id'])
        with self.subTest(surface='categories'):
            catalog_docs = section('api-docs.html', 'categories')
            status, catalog = self.request('GET', '/api/v1/categories')
            self.assertEqual(status, 200, catalog)
            source = (ROOT / 'frontend/api-docs.html').read_text().split('id="categories"', 1)[1]
            values = re.findall(r'<td class="param-name">([^<]+)</td>', source)
            self.assertTrue(values)
            self.assertTrue(set(values) <= set(catalog['categories']), values)
            expected = objects(catalog_docs)[0]
            self.assert_excerpt(expected, catalog)
            self.assertTrue(set(expected['categories']) <= set(catalog['categories']))
            guide = section('ai-integration.html', 'use-cases')
            guide_categories = re.findall(r'\b(?:[a-z]+_)+[a-z]+\b', guide)
            self.assertTrue(set(guide_categories) <= set(catalog['categories']), guide_categories)
        with self.subTest(surface='jobs-list'):
            docs = section('api-docs.html', 'tasks-list')
            self.assertNotRegex(docs, r'\blimit\b|\boffset\b|associated with the authenticated account')
            route = re.search(r'GET (/api/v1/jobs\?[^\s]+)', docs)
            self.assertIsNotNone(route, 'Show an executable paginated public jobs request')
            path, query = route.group(1).split('?', 1)
            status, result = self.request('GET', path, query=query)
            self.assertEqual(status, 200, result)
            expected = objects(docs)[0]
            self.assert_excerpt(expected, result)
            self.assertEqual((result['total'], result['page'], result['per_page'], result['total_pages']),
                             (3, 1, 2, 2))
            self.assertEqual(len(result['jobs']), 2)
            for example, actual in zip(expected['jobs'], result['jobs']):
                self.assert_excerpt(example, actual)
            status, next_page = self.request('GET', path, query=query.replace('page=1', 'page=2'))
            self.assertEqual(status, 200, next_page)
            seen = [job['id'] for job in result['jobs'] + next_page['jobs']]
            self.assertEqual(len(seen), len(set(seen)))
            self.assertEqual(set(seen), ids)
            for invalid in ('page=0', 'per_page=0', 'page=abc'):
                self.assertEqual(self.request('GET', path, query=invalid)[0], 400)
            status, capped = self.request('GET', path, query='per_page=101')
            self.assertEqual(status, 200, capped)
            self.assertEqual(capped['per_page'], 100)
        with self.subTest(surface='job-detail'):
            docs = section('api-docs.html', 'tasks-get')
            route = re.search(r'GET (/api/v1/jobs/\{id\})', docs).group(1)
            status, actual = self.request('GET', route.replace('{id}', str(job['id'])))
            self.assertEqual(status, 200, actual)
            self.assert_excerpt(objects(docs)[0], actual)
            self.assertEqual(actual['id'], job['id'])
            self.assertIs(type(actual['id']), int)
            self.assertEqual(actual['application_count'], 0)
            self.assertEqual(self.request('GET', route.replace('{id}', 'task_xyz789'))[0], 404)
        with self.subTest(surface='services-list'):
            docs = section('api-docs.html', 'tasks-list')
            route = re.search(r'GET (/api/v1/services\?[^\s]+)', docs)
            self.assertIsNotNone(route, 'Document public service discovery alongside jobs')
            path, query = route.group(1).split('?', 1)
            status, actual = self.request('GET', path, query=query)
            self.assertEqual(status, 200, actual)
            self.assertEqual(objects(docs)[1], actual)

    def test_onboarding_workflow_stops_before_financial_actions(self):
        owner = self.register_owner()
        docs = section('api-docs.html', 'example-full')
        self.assertNotRegex(docs, r'/payments/|task_[a-z0-9]+')
        self.assertIn('does not create an order', docs)
        login, payload = objects(docs)
        status, authenticated = self.request('POST', '/auth/login', login)
        self.assertEqual(status, 200, authenticated)
        self.assertEqual(authenticated['id'], owner['id'])
        status, created = self.request('POST', '/api/v1/jobs', payload, authenticated['token'])
        self.assertEqual(status, 201, created)
        status, detail = self.request('GET', f"/api/v1/jobs/{created['id']}")
        self.assertEqual(status, 200, detail)
        self.assertEqual(detail['id'], created['id'])
        with self.api.get_db() as db:
            self.assertEqual(db.execute('SELECT COUNT(*) FROM orders').fetchone()[0], 0)
        for filename in ('api-docs.html', 'ai-integration.html'):
            source = (ROOT / 'frontend' / filename).read_text()
            self.assertEqual(source.count('<span'), source.count('</span>'), filename)
        guide = (ROOT / 'frontend/ai-integration.html').read_text()
        self.assertNotRegex(guide, r'task_abc123|research_analysis|<code>deadline</code>')
        self.assertNotIn('Read the <code>result</code> field', guide)
        schemas = [json.loads(value) for value in re.findall(
            r'<script type="application/ld\+json">([\s\S]*?)</script>', guide)]
        steps = next(s['step'] for s in schemas if s.get('@type') == 'HowTo')
        for step in steps:
            self.assertNotRegex(step['text'], r"submitted|result field|checkout session")
        self.assertTrue(any('does not create' in step['text'] for step in steps))

    def test_portal_write_warning_matches_route_scope(self):
        # Inspect authorization only: never execute hiring, orders, or payments.
        for route in ('/services/1/order', '/jobs/1/hire'):
            with self.subTest(route=route):
                self.assertEqual(self.api._api_key_route_scope('POST', route), 'write')
        self.assertEqual(self.api._api_key_route_scope(
            'POST', '/payments/fund-escrow'), 'payments:fund')

        portal = (ROOT / 'frontend/agent-onboarding.html').read_text()
        modal = portal.split('id="createKeyModal"', 1)[1].split(
            '<!-- ── Revoke Confirm Modal', 1)[0]
        scope_inputs = re.findall(r'<input\b[^>]*type="checkbox"[^>]*>', modal)
        self.assertEqual(len(scope_inputs), 2)
        self.assertTrue(any('value="read"' in node and 'checked' in node
                            for node in scope_inputs))
        self.assertTrue(any('value="write"' in node and 'checked' not in node
                            for node in scope_inputs))
        warning = text(modal)
        for phrase in ('Start with read', 'broad mutation permission',
                       'not a listing-only permission', 'hiring', 'service-order',
                       'charge a configured payment method', 'trusted',
                       'explicitly owner-approved', 'payments:*',
                       'absence does not make write nonfinancial'):
            with self.subTest(warning=phrase):
                self.assertIn(phrase, warning)
        rendered = text(portal)
        self.assertNotIn('Neither scope authorizes payment operations', rendered)
        self.assertNotIn('Hiring, payment, and approval require separate permissions', rendered)
        quickstart = rendered.split('MCP Server Setup', 1)[1].split('Generate New API Key', 1)[0]
        self.assertIn('write is broad', quickstart)
        self.assertIn('charge a configured payment method', quickstart)
        self.assertIn('not part of this quickstart', quickstart)

    def test_portal_quickstart_examples(self):
        owner = self.register_owner()
        portal = (ROOT / 'frontend/agent-onboarding.html').read_text()
        guide = portal.split('<!-- ── Quick-Start Guide', 1)[1].split('</main>', 1)[0]
        rendered = text(guide)
        self.assertNotRegex(rendered, r'/search\?|GHH_API_KEY|"npx"|professional_id|/jobs/TASK_ID/release')
        examples = objects(rendered)
        # Parse the rendered cURL itself: route, payload, header names and values.
        # Only substitute the example secret with a real disposable local key.
        commands = [shlex.split(block[block.index('curl '):]) for block in
                    (text(b) for b in re.findall(
                        r'<div class="code-block has-label">([\s\S]*?)</div>', guide))
                    if 'curl ' in block]
        self.assertEqual(len(commands), 2)
        for command in commands:
            method = command[command.index('-X') + 1]
            url = urlsplit(next(arg for arg in command if arg.startswith('https://')))
            headers = dict(command[i + 1].split(': ', 1)
                           for i, arg in enumerate(command) if arg == '-H')
            with self.subTest(method=method, path=url.path):
                if method == 'GET':
                    self.assertEqual(url.path, '/services')
                    self.assertFalse(headers, 'Public discovery must not send authentication')
                    status, services = self.request(method, url.path, query=url.query)
                    self.assertEqual(status, 200, services)
                    self.assertIn('services', services)
                    continue
                self.assertEqual((method, url.path), ('POST', '/jobs'))
                payload = json.loads(command[command.index('-d') + 1])
                auth_headers = {name: value for name, value in headers.items()
                                if name.lower() != 'content-type'}
                self.assertEqual(len(auth_headers), 1)
                header_name, template = next(iter(auth_headers.items()))
                self.assertRegex(template, r'ghh_YOUR_API_KEY|\*\*\*')
                for scope, expected_status in (('read', 403), ('write', 201)):
                    with self.subTest(scope=scope, header=header_name):
                        status, created = self.request('POST', '/api-keys', {
                            'name': f'Local portal {scope}', 'scopes': [scope],
                        }, token=owner['token'])
                        self.assertEqual(status, 201, created)
                        key = created['api_key']['key']
                        self.assertTrue(key.startswith('ghh_'))
                        self.assertEqual(created['api_key']['scopes'], [scope])
                        value = template.replace('ghh_YOUR_API_KEY', key).replace('***', key)
                        # No session token on the documented job request.
                        status, job = self.request(method, url.path, payload, headers={
                            **headers, header_name: value,
                        })
                        self.assertEqual(status, expected_status, job)
                        if scope == 'read':
                            with self.api.get_db() as db:
                                self.assertEqual(db.execute('SELECT COUNT(*) FROM jobs').fetchone()[0], 0)
                        else:
                            self.assertEqual(header_name, 'X-API-Key')
                            self.assertNotIn('budget', payload)
                            self.assertNotIn('deadline', payload)
                            self.assertEqual(job['due_by'], payload['due_by'])
                            self.assertEqual(job['employer_id'], owner['id'])
        with self.api.get_db() as db:
            self.assertEqual(db.execute('SELECT COUNT(*) FROM jobs').fetchone()[0], 1)
            self.assertEqual(db.execute('SELECT COUNT(*) FROM orders').fetchone()[0], 0)
        config = next(o for o in examples if 'mcpServers' in o)['mcpServers']['gohirehumans']
        self.assertEqual(config['command'], 'python')
        self.assertTrue(config['args'][0].endswith('/mcp_server.py'))
        self.assertIn('GOHIREHUMANS_API_KEY', config['env'])

    def test_duplicate_auth_examples_and_claims(self):
        for path in SURFACES:
            with self.subTest(path=path):
                source = (ROOT / path).read_text()
                self.assertNotRegex(source, r'(?i)JWT|full_name|access_token|ai_client|eyJ')
        auth = objects(section('ai-integration.html', 'step-auth'))
        status, result = self.request('POST', '/auth/register', auth[0])
        self.assertEqual(status, 201, result)
        status, result = self.request('POST', '/auth/login', auth[1])
        self.assertEqual(status, 200, result)
        self.assert_excerpt(auth[2], result)


if __name__ == '__main__':
    unittest.main()
