"""Regression tests for MCP transport errors, matching filters, and published curl."""
import html
from html.parser import HTMLParser
import importlib.util
import io
import json
import os
from pathlib import Path
import re
import shlex
import subprocess
import sys
import threading
import unittest
import urllib.error
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location('mcp_contract_test', ROOT / 'backend/mcp_server.py')
mcp = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(mcp)


def call(name, arguments):
    return mcp.handle_message({"jsonrpc": "2.0", "id": 1, "method": "tools/call",
                               "params": {"name": name, "arguments": arguments}})["result"]


class MCPErrorContractTests(unittest.TestCase):
    def api_error(self, status, message, name, args, expected_kind):
        error = urllib.error.HTTPError('http://127.0.0.1/api/v1/test', status,
                                       message, {}, io.BytesIO(json.dumps({"error": message}).encode()))
        with mock.patch.object(mcp.urllib.request, 'urlopen', side_effect=error):
            result = call(name, args)
        self.assertTrue(result['isError'], result)
        payload = json.loads(result['content'][0]['text'])
        self.assertEqual(payload['http_status'], status)
        self.assertEqual(payload['category'], expected_kind)
        self.assertIn(message, payload['message'])
        return payload

    def test_payment_setup_402_is_error(self):
        # The service read is mocked; the checkout request traverses api_request.
        real_request = mcp.api_request
        with mock.patch.object(mcp, 'api_request', side_effect=lambda method, path, **kw:
                        {"service": {"title": "Example", "price": 40}} if method == 'GET' else real_request(method, path, **kw)):
            payload = self.api_error(402, 'Payment setup required', 'hire_worker',
                                     {"service_id": 1, "idempotency_key": "contract-test-order-001"}, 'payment_setup_required')
        self.assertFalse(payload['retry_safe'])  # Resolve payment setup before any retry.

    def test_permission_403_is_error(self):
        self.api_error(403, 'Insufficient API key scope', 'release_payment', {"order_id": 1}, 'auth')

    def test_lifecycle_409_is_error(self):
        self.api_error(409, 'Order must be completed first', 'submit_review',
                       {"order_id": 1, "rating": 5, "comment": "Good"}, 'lifecycle_conflict')

    def test_setup_409_is_distinct_from_lifecycle(self):
        self.api_error(409, 'Payment setup required before checkout', 'release_payment',
                       {"order_id": 1}, 'payment_setup_required')

    def test_validation_rate_limit_and_server_error_categories(self):
        for code, kind in ((400, 'validation'), (422, 'validation'), (429, 'rate_limit'), (503, 'server_error')):
            with self.subTest(code=code):
                payload = self.api_error(code, 'Unavailable', 'get_service_details',
                                         {"service_id": '1'}, kind)
                self.assertEqual(payload['retry_safe'], code == 503)

    def test_success_remains_success(self):
        response = mock.MagicMock()
        response.__enter__.return_value.read.return_value = b'{"order":{"status":"completed"}}'
        with mock.patch.object(mcp.urllib.request, 'urlopen', return_value=response):
            result = call('release_payment', {"order_id": 1})
        self.assertIs(result['isError'], False)
        self.assertIn('Payment released successfully', result['content'][0]['text'])

    def test_resource_read_api_failure_is_jsonrpc_error_not_crash(self):
        error = urllib.error.HTTPError('http://127.0.0.1/api/v1/categories', 503, 'down', {},
                                       io.BytesIO(b'{"error": "Service unavailable"}'))
        with mock.patch.object(mcp.urllib.request, 'urlopen', side_effect=error):
            response = mcp.handle_message({"jsonrpc": "2.0", "id": 7, "method": "resources/read",
                                           "params": {"uri": "gohirehumans://categories"}})
        self.assertEqual(response['id'], 7)
        self.assertNotIn('result', response)
        self.assertEqual(response['error']['data']['http_status'], 503)
        self.assertEqual(response['error']['data']['category'], 'server_error')

    def test_stdio_tool_calls_retain_http_status(self):
        class LocalAPI(BaseHTTPRequestHandler):
            def do_POST(self):
                status = {"/api/v1/orders/1/approve": 403,
                          "/api/v1/orders/1/review": 409}.get(self.path, 404)
                self.send_response(status)
                self.send_header('Content-Type', 'application/json')
                self.end_headers()
                self.wfile.write(json.dumps({'error': 'Scope denied' if status == 403 else 'Order not completed'}).encode())

            def log_message(self, format, *args):
                pass

        server = ThreadingHTTPServer(('127.0.0.1', 0), LocalAPI)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            calls = [dict(jsonrpc='2.0', id=i, method='tools/call', params={'name': name, 'arguments': args})
                     for i, (name, args) in enumerate((
                         ('release_payment', {'order_id': 1}),
                         ('submit_review', {'order_id': 1, 'rating': 5, 'comment': 'Nice'})), 1)]
            process = subprocess.run([sys.executable, str(ROOT / 'backend/mcp_server.py')],
                                     input='\n'.join(json.dumps(c) for c in calls) + '\n',
                                     text=True, capture_output=True, timeout=10, check=False,
                                     env={**os.environ, 'GOHIREHUMANS_API_URL': f'http://127.0.0.1:{server.server_port}',
                                          'GOHIREHUMANS_API_KEY': '', 'GOHIREHUMANS_AUTH_TOKEN': ''})
            self.assertEqual(process.returncode, 0, process.stderr)
            responses = [json.loads(line) for line in process.stdout.splitlines()]
            self.assertEqual([r['result']['isError'] for r in responses], [True, True])
            self.assertEqual([json.loads(r['result']['content'][0]['text'])['http_status'] for r in responses], [403, 409])
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=5)


SERVICES = [
    {"id": 1, "worker_id": 1, "worker_name": "Unrated", "title": "Low cost", "price": 8, "category": "writing", "worker_rating": None},
    {"id": 2, "worker_id": 2, "worker_name": "Rated", "title": "Affordable", "price": 9, "category": "writing", "worker_rating": 4.7},
    {"id": 3, "worker_id": 3, "worker_name": "Expensive", "title": "Over budget", "price": 40, "category": "writing", "worker_rating": 5},
    {"id": 4, "worker_id": 4, "worker_name": "Below rating", "title": "Budget copy", "price": 7, "category": "writing", "worker_rating": 3},
]


class MCPMatchingTests(unittest.TestCase):
    def test_min_rating_excludes_unrated_and_below_threshold_before_limit(self):
        with mock.patch.object(mcp, 'api_request', return_value={"services": SERVICES}):
            result = call('search_workers', {"category": "writing", "min_rating": 4, "limit": 1})
        text = result['content'][0]['text']
        self.assertFalse(result['isError'])
        self.assertIn('Rated', text)
        self.assertNotIn('Unrated', text)
        self.assertNotIn('Below rating', text)
        self.assertIn('Found 2 worker(s)', text)

    def test_budget_range_excludes_over_budget_before_ranking(self):
        with mock.patch.object(mcp, 'api_request', return_value={"services": SERVICES}):
            result = call('get_recommended', {"task_description": "write copy", "budget_range": "under $10"})
        text = result['content'][0]['text']
        self.assertFalse(result['isError'])
        self.assertIn('Affordable', text)
        self.assertNotIn('Over budget', text)

    def test_budget_range_is_preserved_when_fallback_search_runs(self):
        def search(method, path, params):
            return {"services": [] if 'search' in params else [SERVICES[2]]}
        with mock.patch.object(mcp, 'api_request', side_effect=search):
            result = call('get_recommended', {"task_description": "write copy", "budget_range": "$5-10"})
        self.assertNotIn('Over budget', result['content'][0]['text'])
        self.assertIn('No workers', result['content'][0]['text'])
    def test_rating_paginates_past_unrated_first_page(self):
        def pages(method, path, params):
            return {"services": [SERVICES[0]] if params.get('page', 1) == 1 else [SERVICES[1]],
                    "total_pages": 2}
        with mock.patch.object(mcp, 'api_request', side_effect=pages):
            result = call('search_workers', {"min_rating": 4, "limit": 1})
        self.assertIn('Rated', result['content'][0]['text'])
        self.assertNotIn('Unrated', result['content'][0]['text'])

    def test_rating_paginates_when_first_page_has_one_worker_with_many_services(self):
        first = [dict(SERVICES[1], id=i) for i in range(5)]
        def pages(method, path, params):
            return {"services": first if params.get('page', 1) == 1 else [SERVICES[2]],
                    "total_pages": 2}
        with mock.patch.object(mcp, 'api_request', side_effect=pages):
            result = call('search_workers', {"min_rating": 4, "limit": 2})
        self.assertIn('Expensive', result['content'][0]['text'])

    def test_under_budget_is_exclusive(self):
        at_cap = dict(SERVICES[1], price=10, title='At cap')
        with mock.patch.object(mcp, 'api_request', return_value={"services": [at_cap, SERVICES[1]]}):
            result = call('get_recommended', {"task_description": "write copy", "budget_range": "under $10"})
        self.assertNotIn('At cap', result['content'][0]['text'])
        self.assertIn('Affordable', result['content'][0]['text'])

    def test_budget_range_enforces_lower_bound_and_rejects_unknown_format(self):
        with mock.patch.object(mcp, 'api_request', return_value={"services": SERVICES}):
            result = call('get_recommended', {"task_description": "write copy", "budget_range": "$9-10"})
        self.assertIn('Affordable', result['content'][0]['text'])
        self.assertNotIn('Low cost', result['content'][0]['text'])
        invalid = call('get_recommended', {"task_description": "write copy", "budget_range": "whatever"})
        self.assertTrue(invalid['isError'])
        self.assertIn('budget_range', invalid['content'][0]['text'])


class CurlBlocks(HTMLParser):
    def __init__(self):
        super().__init__()
        self.depth = 0
        self.fragments = []
        self.blocks = []

    def handle_starttag(self, tag, attrs):
        if tag in ('code', 'span') and (tag == 'code' or ('id', 'qsSearchCode') in attrs or ('id', 'qsPostCode') in attrs):
            self.depth += 1
            self.fragments = []
        elif self.depth:
            self.depth += 1

    def handle_endtag(self, tag):
        if self.depth:
            self.depth -= 1
            if self.depth == 0:
                value = html.unescape(''.join(self.fragments)).strip()
                if value.startswith('curl '):
                    self.blocks.append(value)

    def handle_data(self, data):
        if self.depth:
            self.fragments.append(data)


class PublishedCurlTests(unittest.TestCase):
    pages = ('frontend/agent-onboarding.html', 'frontend/blog/mcp-for-marketplaces.html')

    def curl_blocks(self):
        for page in self.pages:
            parser = CurlBlocks()
            parser.feed((ROOT / page).read_text())
            self.assertTrue(parser.blocks, page)
            for block in parser.blocks:
                yield page, block

    def test_all_curl_blocks_are_quoted_and_post_valid_headers(self):
        for page, block in self.curl_blocks():
            with self.subTest(page=page, block=block[:60]):
                tokens = shlex.split(block)
                if 'POST' in tokens:
                    headers = [tokens[i+1] for i, token in enumerate(tokens[:-1]) if token == '-H']
                    self.assertEqual(headers[0].split(': ', 1)[0], 'X-API-Key')
                    self.assertEqual(headers[0], 'X-API-Key: ghh_YOUR_API_KEY')
                    self.assertIn('Content-Type: application/json', headers)
                    self.assertEqual(len(headers), 2)

    def test_copied_post_examples_create_only_local_jobs(self):
        jobs = []
        class LocalJobs(BaseHTTPRequestHandler):
            def do_POST(self):
                length = int(self.headers.get('Content-Length', '0'))
                body = json.loads(self.rfile.read(length))
                if (self.path in ('/jobs', '/api/v1/jobs') and
                        self.headers.get('X-API-Key') == 'local-write-fixture' and
                        self.headers.get('Content-Type') == 'application/json' and
                        body.get('category') in ('writing', 'data_entry')):
                    jobs.append(body)
                    self.send_response(201)
                    self.end_headers()
                    self.wfile.write(json.dumps({'job': {'id': len(jobs)}}).encode())
                else:
                    self.send_response(401)
                    self.end_headers()

            def log_message(self, *args):
                pass

        server = ThreadingHTTPServer(('127.0.0.1', 0), LocalJobs)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            for page, block in self.curl_blocks():
                if 'POST' not in shlex.split(block):
                    continue
                with self.subTest(page=page):
                    local = (block.replace('https://gohirehumans-production.up.railway.app',
                                           f'http://127.0.0.1:{server.server_port}')
                                  .replace('ghh_YOUR_API_KEY', 'local-write-fixture')
                                  .replace('curl -X', 'curl --silent --show-error --fail-with-body -X', 1))
                    completed = subprocess.run(['/bin/sh', '-c', local], capture_output=True,
                                               text=True, timeout=10, check=False)
                    self.assertEqual(completed.returncode, 0, completed.stderr + completed.stdout)
                    self.assertIn('job', json.loads(completed.stdout))
            self.assertEqual(len(jobs), 2)
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=5)


if __name__ == '__main__':
    unittest.main()
