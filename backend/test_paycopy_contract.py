"""Contracts shared with the static payout-return forwarder and public copy."""
import json
import re
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


class PaycopyContractTests(unittest.TestCase):
    def test_connect_account_links_and_simulation_keep_static_return_url_contract(self):
        source = (ROOT / 'backend/api_core.py').read_text()
        for kind in ('refresh', 'complete'):
            url = f'f"{{FRONTEND_URL}}/payments?connect={kind}"'
            # operation payload, create call and replay call must remain in sync.
            self.assertEqual(source.count(url), 3, url)
        self.assertIn('f"{FRONTEND_URL}/payments?connect=complete&simulated=true"', source)
        self.assertTrue((ROOT / 'frontend/payments/index.html').exists())

    def test_vercel_serves_both_payment_path_spellings_without_redirecting_to_spa_404(self):
        config = json.loads((ROOT / 'frontend/vercel.json').read_text())
        for source in ('/payments', '/payments/'):
            self.assertIn({'source': source, 'destination': '/payments/index.html'}, config.get('rewrites', []))
        self.assertNotIn('/payments', [r['source'] for r in config.get('redirects', [])])

    def test_public_claims_match_backend_rates_in_metadata_schema_and_body(self):
        backend = (ROOT / 'backend/api_core.py').read_text()
        self.assertRegex(backend, r'PLATFORM_FEE_BPS = 100\b')
        self.assertRegex(backend, r'PROCESSING_FEE_BPS = 300\b')
        self.assertIn('return max(1, (base_cents * basis_points + 5000) // 10000)', backend)
        from html.parser import HTMLParser

        class Claims(HTMLParser):
            def __init__(self):
                super().__init__()
                self.meta = {}
                self.schemas = []
                self.body = []
                self.in_schema = False
                self.in_body = False
            def handle_starttag(self, tag, attrs):
                attrs = dict(attrs)
                if tag == 'meta':
                    self.meta[attrs.get('name') or attrs.get('property')] = attrs.get('content', '')
                if tag == 'script' and attrs.get('type') == 'application/ld+json':
                    self.in_schema = True
                    self.schemas.append('')
                if tag == 'body':
                    self.in_body = True
            def handle_endtag(self, tag):
                if tag == 'script': self.in_schema = False
                if tag == 'body': self.in_body = False
            def handle_data(self, data):
                if self.in_schema: self.schemas[-1] += data
                elif self.in_body: self.body.append(data)

        for name in ('pricing.html', 'faq.html', 'how-it-works.html'):
            source = (ROOT / 'frontend' / name).read_text()
            page = Claims()
            page.feed(source)
            for tag in ('description', 'og:description', 'twitter:description'):
                with self.subTest(page=name, surface=tag):
                    self.assertRegex(page.meta[tag], r'1%.*3%')
                    self.assertIn('Where checkout is configured', page.meta[tag])
            schemas = [json.loads(s) for s in page.schemas]
            schema_text = json.dumps(schemas)
            self.assertRegex(schema_text, r'1%.*3%')
            self.assertIn('Where checkout is configured', schema_text)
            self.assertIn('1% platform fee plus a fixed 3% processing charge', ' '.join(page.body))
            for surface in (source, schema_text, ' '.join(page.body)):
                self.assertNotIn('Stripe processing plus', surface)
                self.assertNotIn('per completed task', surface)
        faq = (ROOT / 'frontend/faq.html').read_text()
        for claim in ('A funded order has no self-serve cancel option', 'full charge including fees', 'base amount only'):
            self.assertGreaterEqual(faq.count(claim), 2, claim)


if __name__ == '__main__':
    unittest.main()
