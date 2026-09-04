"""Guard duplicate onboarding and payment parameter documentation."""
import html
from pathlib import Path
import re
import unittest

ROOT = Path(__file__).resolve().parents[1]


def text(name):
    return html.unescape(re.sub(r'<[^>]+>', ' ', (ROOT / 'frontend' / name).read_text()))


class SecondaryOnboardingDocs(unittest.TestCase):
    def test_duplicate_pages_distinguish_listing_from_order_delivery(self):
        how = text('how-it-works.html')
        faq = text('faq.html')
        self.assertNotIn('business_calls', how)
        self.assertIn('phone_call', how)
        self.assertNotRegex(how, r'(?s)Poll the job status.*?submitted')
        self.assertNotIn('Response includes status, deliverables, and professional notes', how)
        self.assertIn('authenticated order', how)
        self.assertNotIn('Get job status and results', faq)
        self.assertIn('public listing', faq)

    def test_payment_amount_is_optional_and_milestone_id_is_numeric(self):
        source = (ROOT / 'frontend/api-docs.html').read_text()
        section = source.split('id="payments-checkout"', 1)[1].split('</section>', 1)[0]
        rows = [html.unescape(re.sub(r'<[^>]+>', ' ', row)) for row in re.findall(r'<tr>(.*?)</tr>', section)]
        self.assertTrue(any(re.search(r'amount\s+number\s+optional', row) for row in rows))
        self.assertTrue(any(re.search(r'milestone_id\s+integer\s+optional', row) for row in rows))
        self.assertIn('authoritative', section)
        self.assertIn('owner approval', section)


if __name__ == '__main__':
    unittest.main()
