"""Payments guide retarget (2026-10-18).

Search Console (28 days to Oct 16) shows /blog/best-freelance-platforms-escrow.html ranking around position 21-25 for
"payment platform for freelancers", "freelancer payment platform", "freelancer payment solution" and similar queries,
which ask about payment tools (PayPal, Stripe, Wise, Payoneer), while the page compared only marketplaces. The page now
answers that intent with provider-sourced US fees, keeps the marketplace comparison, and corrects three competitor facts
(Fiverr's $3.50 small-order fee, Freelancer.com's client fee, Fiverr's three-day auto-completion).
"""
import html
import json
import re
import unittest
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Optional

REPO_ROOT = Path(__file__).resolve().parents[1]
FRONTEND = REPO_ROOT / "frontend"
SLUG = "best-freelance-platforms-escrow"
URL = f"https://www.gohirehumans.com/blog/{SLUG}.html"
PAGE = FRONTEND / "blog" / f"{SLUG}.html"

TITLE = "Best Payment Platforms for Freelancers (2026)"
HEAD_TITLE = f"{TITLE} | GoHireHumans"
DESCRIPTION = ("Compare PayPal, Stripe, Wise, and Payoneer fees for getting paid as a freelancer, and how Upwork, "
               "Fiverr, Freelancer.com, and GoHireHumans handle payment.")
H1 = "Best payment platforms for freelancers in 2026"
RETIRED_TITLE = "Best Freelance Platforms for Payments in 2026"
RETIRED_DESCRIPTION_PREFIX = "How GoHireHumans, Upwork, Fiverr, and Freelancer.com handle payment"
SOURCES = {
    "PayPal": "https://www.paypal.com/us/business/paypal-business-fees",
    "Stripe": "https://stripe.com/pricing",
    "Wise": "https://wise.com/us/pricing/receive",
    "Payoneer": "https://www.payoneer.com/about/pricing/",
}


def must(match: Optional[re.Match]) -> re.Match:
    assert match is not None
    return match


def text_of(fragment):
    return re.sub(r"\s+", " ", html.unescape(re.sub(r"<[^>]+>", " ", fragment))).strip()


def ld_nodes(page):
    return [json.loads(b) for b in re.findall(r'<script type="application/ld\+json">\s*(.*?)\s*</script>', page, re.S)]


def public_files():
    skip = {"node_modules", "test-results", "playwright-report", "tests"}
    return [p for p in FRONTEND.rglob("*") if p.is_file() and p.suffix in {".html", ".xml", ".txt", ".json"}
            and not (skip & set(p.relative_to(FRONTEND).parts))]


class PaymentsGuideMetadataTests(unittest.TestCase):
    def setUp(self):
        self.page = PAGE.read_text(encoding="utf-8")

    def test_head_metadata_targets_payment_platform_queries(self):
        self.assertIn(f"<title>{HEAD_TITLE}</title>", self.page)
        self.assertLessEqual(len(HEAD_TITLE), 60)
        self.assertLessEqual(len(DESCRIPTION), 160)
        for pattern in (r'<meta name="description" content="([^"]*)">', r'<meta property="og:description" content="([^"]*)">',
                        r'<meta name="twitter:description" content="([^"]*)">'):
            found = re.findall(pattern, self.page)
            self.assertEqual([html.unescape(x) for x in found], [DESCRIPTION], pattern)
        for pattern in (r'<meta property="og:title" content="([^"]*)">', r'<meta name="twitter:title" content="([^"]*)">'):
            self.assertEqual([html.unescape(x) for x in re.findall(pattern, self.page)], [HEAD_TITLE], pattern)

    def test_h1_json_ld_and_breadcrumb_agree(self):
        h1s = [text_of(x) for x in re.findall(r"<h1[^>]*>(.*?)</h1>", self.page, re.S)]
        self.assertEqual(h1s, [H1])
        nodes = ld_nodes(self.page)
        article = [n for n in nodes if n.get("@type") == "Article"]
        self.assertEqual(len(article), 1)
        self.assertEqual(article[0]["headline"], H1)
        self.assertEqual(article[0]["description"], DESCRIPTION)
        crumbs = next(n for n in nodes if n.get("@type") == "BreadcrumbList")["itemListElement"]
        self.assertEqual(crumbs[-1]["name"], H1)
        self.assertEqual(crumbs[-1]["item"], URL)

    def test_date_modified_matches_sitemap_and_atom(self):
        modified = next(n for n in ld_nodes(self.page) if n.get("@type") == "Article")["dateModified"]
        self.assertEqual(modified, "2026-10-18")
        sitemap = (FRONTEND / "sitemap.xml").read_text(encoding="utf-8")
        entry = re.search(rf"<url>\s*<loc>{re.escape(URL)}</loc>(.*?)</url>", sitemap, re.S)
        self.assertIsNotNone(entry)
        self.assertIn(f"<lastmod>{modified}</lastmod>", must(entry).group(1))
        ns = {"a": "http://www.w3.org/2005/Atom"}
        atom = {e.findtext("a:id", namespaces=ns): e for e in ET.parse(FRONTEND / "atom.xml").getroot().findall("a:entry", ns)}
        self.assertEqual(atom[URL].findtext("a:title", namespaces=ns), TITLE)
        self.assertEqual(atom[URL].findtext("a:summary", namespaces=ns), DESCRIPTION)
        self.assertEqual(atom[URL].findtext("a:updated", namespaces=ns), f"{modified}T00:00:00Z")
        rss = {i.findtext("link"): i for i in ET.parse(FRONTEND / "feed.xml").getroot().iter("item")}
        self.assertEqual(rss[URL].findtext("title"), TITLE)
        self.assertEqual(rss[URL].findtext("description"), DESCRIPTION)

    def test_blog_index_card_and_internal_links_use_the_new_title(self):
        index = (FRONTEND / "blog" / "index.html").read_text(encoding="utf-8")
        card = re.search(rf'<a class="card card--interactive" href="/blog/{SLUG}\.html">(.*?)</a>', index, re.S)
        self.assertIsNotNone(card)
        self.assertIn(f'<span class="card-list-title">{TITLE}</span>', must(card).group(1))
        self.assertIn(f'<span class="card-list-desc">{DESCRIPTION}</span>', html.unescape(must(card).group(1)))
        for sibling in ("where-to-list-services-online", "freelancers-switching-lower-fee-platforms", "fiverr-vs-upwork-vs-gohirehumans"):
            body = (FRONTEND / "blog" / f"{sibling}.html").read_text(encoding="utf-8")
            self.assertIn(f'<a href="/blog/{SLUG}.html">{TITLE}</a>', body, sibling)
        # The guide links back to the fee explainer that answers the "how do platform fees work" questions.
        self.assertIn('<a href="/blog/freelancers-switching-lower-fee-platforms.html">How Freelance Platform Fees Work in 2026</a>', self.page)

    def test_retired_title_and_description_are_gone_everywhere(self):
        stale = [str(p.relative_to(REPO_ROOT)) for p in public_files()
                 if RETIRED_TITLE in (t := p.read_text(encoding="utf-8", errors="ignore")) or RETIRED_DESCRIPTION_PREFIX in t]
        self.assertEqual(stale, [])


class PaymentsGuideContentTests(unittest.TestCase):
    def setUp(self):
        self.page = PAGE.read_text(encoding="utf-8")
        self.body = text_of(must(re.search(r"<main.*?</main>", self.page, re.S)).group(0))

    def section(self, heading):
        match = re.search(rf"<h2[^>]*>{re.escape(heading)}</h2>(.*?)(?=<h2)", self.page, re.S)
        self.assertIsNotNone(match, heading)
        return must(match).group(1)

    def test_payment_tools_section_compares_four_providers_with_official_sources(self):
        section = self.section("Payment platforms freelancers use to get paid directly")
        rows = re.findall(r"<tr>(.*?)</tr>", section, re.S)
        names = [text_of(re.findall(r"<t[hd][^>]*>(.*?)</t[hd]>", r, re.S)[0]) for r in rows[1:]]
        self.assertEqual(names, ["PayPal", "Stripe", "Wise", "Payoneer"])
        for name, url in SOURCES.items():
            self.assertIn(f'href="{url}"', section, name)
        self.assertIn("checked on each provider's US pricing page in October 2026", text_of(section))

    def test_provider_fees_match_the_published_us_pricing(self):
        section = text_of(self.section("Payment platforms freelancers use to get paid directly"))
        for fact in ("3.49% + $0.49 for PayPal Checkout and Guest Checkout payments", "2.99% + $0.49 for standard card payments",
                     "1.5% when the payer is in another country",
                     "2.9% + 30¢ per domestic card payment", "0.8%, capped at $5", "Stripe Invoicing adds its own fee",
                     "6.11 USD per incoming USD wire", "1% (minimum $1) into a receiving account in another currency",
                     "$1.50 to withdraw to a US bank", "$29.95 annual fee if you receive under $6,000 in 12 months"):
            self.assertIn(fact, section)
        # Independent review 2026-10-18: no unsupported ranking or savings claims.
        self.assertNotIn("most common", section)
        self.assertNotIn("avoid a wire fee", section)
        self.assertIn("compare their conversion, withdrawal, and account fees", section)

    def test_corrected_marketplace_facts(self):
        self.assertIn("$3.50 on orders under $200", self.body)
        self.assertNotIn("orders over $75", self.body)
        self.assertIn("three days after delivery", self.body)
        self.assertNotIn("14 days on Fiverr", self.body)
        table = self.section("Freelance marketplaces with a payment workflow")
        freelancer_row = next(text_of(r) for r in re.findall(r"<tr>(.*?)</tr>", table, re.S) if "Freelancer.com" in r)
        self.assertIn("3% (client", freelancer_row)
        self.assertIn("10% (freelancer", freelancer_row)
        self.assertNotIn("an payment", self.body)

    def test_review_corrections_toptal_escrow_and_fee_minimums(self):
        # Toptal invoices clients (twice a month, Net 10 per its FAQ), so it is not a "no payment workflow" example.
        risks = self.section("Paying with no payment workflow: the risks")
        self.assertNotIn("Toptal", risks)
        self.assertNotIn("Payment terms are direct", self.body)
        # GoHireHumans is a connector, never escrow/guarantor/arbitrator, and the page does not promise payment.
        ghh = text_of(self.section("GoHireHumans: low-fee marketplace"))
        self.assertIn("not an escrow provider, guarantor, or arbitrator", ghh)
        self.assertNotIn("unpaid-invoice", self.body)
        table = self.section("Freelance marketplaces with a payment workflow")
        rows = {text_of(re.findall(r"<td[^>]*>(.*?)</td>", r, re.S)[0]): text_of(r) for r in re.findall(r"<tr>(.*?)</tr>", table, re.S)[1:]}
        self.assertIn("$3.50 on orders under $200", rows["Fiverr"])
        self.assertIn("minimum $5 on fixed-price", rows["Freelancer.com"])
        self.assertIn("minimum $3 on fixed-price", rows["Freelancer.com"])
        self.assertIn("freelancers pay 10% or $5, whichever is greater, and employers pay 3% or $3, whichever is greater", self.body)
        self.assertIn("on fixed-price projects the minimums are $5 and $3", self.body)
        self.assertIn("A standard order completes automatically three days after delivery", self.body)
        self.assertIn("a standard Fiverr order completes three days after delivery", self.body)

    def test_faq_answers_the_payment_platform_question_in_html_and_json_ld(self):
        question = "What is the best payment platform for freelancers?"
        faq = next(n for n in ld_nodes(self.page) if n.get("@type") == "FAQPage")["mainEntity"]
        self.assertEqual(faq[0]["name"], question)
        self.assertIn(f"<h3>{question}</h3>", self.page)
        answer = faq[0]["acceptedAnswer"]["text"]
        for name in SOURCES:
            self.assertIn(name, answer)
        self.assertNotIn("usually cost less", self.page)
        fee_answer = next(q for q in faq if q["name"] == "Is payment flow free on freelance platforms?")["acceptedAnswer"]["text"]
        self.assertIn("$3.50 more on orders under $200", fee_answer)


if __name__ == "__main__":
    unittest.main()
