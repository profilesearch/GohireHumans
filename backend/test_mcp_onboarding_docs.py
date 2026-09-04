"""Check the shipped MCP onboarding resource without network or credentials."""
import importlib.util
import os
from pathlib import Path
import unittest
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]


class MCPOnboardingDocsTests(unittest.TestCase):
    def test_embedded_bootstrap_and_pagination(self):
        with mock.patch.dict(os.environ, {}, clear=True):
            spec = importlib.util.spec_from_file_location('mcp_docs_test', ROOT / 'backend/mcp_server.py')
            assert spec is not None and spec.loader is not None
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
        with mock.patch.object(module, 'api_request', side_effect=AssertionError('Network forbidden')):
            api = module.handle_resource('gohirehumans://api-docs')['contents'][0]['text']
            start = module.handle_resource('gohirehumans://mcp-quickstart')['contents'][0]['text']
        self.assertIn('api_key.key', api)
        self.assertIn('"scopes": ["read"]', api)
        self.assertIn('page, per_page', api)
        self.assertNotIn('max_price, limit)', api)
        self.assertNotIn('budget_type, limit)', api)
        self.assertNotIn('Settings → API Keys', start)
        self.assertIn('GOHIREHUMANS_AUTH_TOKEN', start)
        self.assertIn('owner approval', start.lower())
        self.assertNotIn('`POST /orders`', api)
        self.assertNotIn('`PUT /orders/{id}`', api)

    def test_readme_bootstrap_and_mirror(self):
        readme = (ROOT / 'backend/mcp-package/README.md').read_text()
        self.assertNotIn('Settings → API Keys', readme)
        self.assertIn('api_key.key', readme)
        self.assertIn('"scopes": ["read"]', readme)
        self.assertIn('owner approval', readme.lower())
        self.assertEqual((ROOT / 'backend/mcp_server.py').read_bytes(),
                         (ROOT / 'backend/mcp-package/mcp_server.py').read_bytes())


if __name__ == '__main__':
    unittest.main()
