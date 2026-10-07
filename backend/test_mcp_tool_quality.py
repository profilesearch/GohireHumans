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
        for fact in ('saved payment method', 'charged immediately',
                     '1% platform fee and a fixed 3% processing charge',
                     'only when the employer approves', 'no self-serve cancel option',
                     'same idempotency_key'):
            self.assertIn(fact, hire)
        release = self.desc['release_payment']
        for fact in ('cannot be undone', "'submitted' status", 'no API key set',
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
                       'freelancers can apply'):
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


if __name__ == '__main__':
    unittest.main()
