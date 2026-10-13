"""Entry-bound contracts for the web developer hiring page (stdlib only)."""
import json
import re
import unittest
import xml.etree.ElementTree as ET
from pathlib import Path

from test_ai_answer_readiness import (
    AnswerHTML, SCHEMA_CONTEXT, SCREENING_CLAIM, VOID_TAGS,
    nodes_of_type, normalized, schema_nodes,
)

ROOT = Path(__file__).resolve().parents[1]
FRONTEND = ROOT / "frontend"
PATH = "/hire/hire-web-developer.html"
URL = "https://www.gohirehumans.com" + PATH
TITLE = "Hire Web Developer: Fixes & Landing Pages | GoHireHumans"
DESCRIPTION = (
    "Hire a web developer for a small website fix, responsive layout, landing page or "
    "AI-built site review. Scope the task, set a payout, and review delivery."
)
MODIFIED = "2026-10-13"
CARD_TITLE = TITLE.split(" | ")[0]
SIBLINGS = (
    "hire/index.html", "hire/hire-frontend-developer.html",
    "hire/hire-ai-developer.html", "hire/hire-seo-specialist.html",
)


class Element:
    def __init__(self, tag, attrs=(), hidden_by=()):
        self.tag, self.attrs, self.children = tag, dict(attrs), []
        self.hidden_by = tuple(hidden_by)

    def text(self):
        return "".join(c.text() if isinstance(c, Element) else c for c in self.children)

    def claim_text(self):
        if self.tag in {"script", "style"}:
            return ""
        text = "".join(c.claim_text() if isinstance(c, Element) else re.sub(r"\s+", " ", c)
                       for c in self.children)
        if self.tag in {"p", "li", "div", "section", "main", "nav", "footer", "tr",
                        "caption", "summary", "title", "h1", "h2", "h3", "h4", "dt", "dd"}:
            return "\n" + text + "\n"
        return text

    def find(self, tag):
        found: list[Element] = [self] if self.tag == tag or tag == "*" else []
        for child in self.children:
            if isinstance(child, Element):
                found.extend(child.find(tag))
        return found


class Document(AnswerHTML):
    """Add a tree to the shared strict parser; never repair malformed markup."""

    def __init__(self, source):
        self.root = Element("document")
        self.element_stack = [self.root]
        super().__init__(source)
        if self.errors:
            raise AssertionError("\n".join(self.errors))

    def handle_starttag(self, tag, attrs):
        reason = self.hidden_reason(dict(attrs))
        hidden_by = self.element_stack[-1].hidden_by
        if reason:
            hidden_by += (f"<{tag}> {reason}",)
        node = Element(tag, attrs, hidden_by)
        self.element_stack[-1].children.append(node)
        super().handle_starttag(tag, attrs)
        if tag not in VOID_TAGS:
            self.element_stack.append(node)

    def handle_startendtag(self, tag, attrs):
        self.handle_starttag(tag, attrs)
        if tag not in VOID_TAGS:
            self.handle_endtag(tag)

    def handle_endtag(self, tag):
        if tag in VOID_TAGS:
            self.errors.append(f"unmatched </{tag}> on line {self.getpos()[0]} (void element)")
            return
        super().handle_endtag(tag)
        if len(self.element_stack) > 1 and self.element_stack[-1].tag == tag:
            self.element_stack.pop()

    def handle_data(self, data):
        super().handle_data(data)
        self.element_stack[-1].children.append(data)


_GHH = r"(?:gohirehumans|ghh|we|our(?:\s+platform)?|the\s+platform)"
_RATE_VERB = r"(?:charges?|costs?|earns?|starts?\s+at|ranges?\s+from)"
_RATE_SUBJECT = re.compile(
    rf"\b{_GHH}(?:['’]s)?\s+(?:[\w-]+\s+){{0,3}}(?:rates?|prices?|pricing|packages?)\b"
    rf"|\b{_GHH}(?:['’]s)?\s+(?:web\s+)?(?:developers?|workers?|freelancers?)\s+{_RATE_VERB}\b"
    rf"|\b(?:rates?|prices?|packages?)\s+(?:on|at|from)\s+{_GHH}\b"
    rf"|\b(?:developers?|workers?|freelancers?)\s+(?:on|at|through)\s+{_GHH}\s+{_RATE_VERB}\b"
    rf"|\b(?:developers?|workers?|freelancers?)\s+{_RATE_VERB}\b[^.!?;]{{0,120}}\b(?:on|at|through)\s+{_GHH}\b",
    re.IGNORECASE,
)
_MONEY = re.compile(r"[$€£]\s*\d|\b\d[\d.,– -]*\s*(?:USD|dollars?)\b", re.IGNORECASE)
_HOURLY_TERM = r"(?:hourly\b(?!\s+(?:benchmarks?|rates?|estimates?|budget)\b)|by\s+the\s+hour|per[- ]hour\s+(?:contracts?|jobs?|work))"
_HOURLY_SUBJECT = re.compile(
    rf"\b{_GHH}(?:['’]s)?\s+(?:offers?|supports?|allows?|enables?|provides?|permits?)\b[^.!?;]{{0,100}}?\b{_HOURLY_TERM}"
    rf"|\b(?:hire|hiring|book|employ)\b[^.!?;]{{0,120}}?\b{_HOURLY_TERM}[^.!?;]{{0,80}}?\b(?:on|at|through|via)\s+{_GHH}\b"
    rf"|\b(?:on|at|through|via)\s+{_GHH}\b[^.!?;]{{0,80}}?\b(?:hire|hiring|book|employ)\b[^.!?;]{{0,100}}?\b{_HOURLY_TERM}"
    rf"|\b{_HOURLY_TERM}(?:[- ](?:contracts?|jobs?|work|hiring))?\s+(?:is\s+|are\s+)?(?:available|enabled|supported|offered)\b[^.!?;]{{0,80}}?\b(?:on|at|through|via)\s+{_GHH}\b"
    rf"|\b{_GHH}(?:['’]s)?\s+{_HOURLY_TERM}(?:[- ](?:contracts?|jobs?|work|hiring))?\s+(?:is\s+|are\s+)?(?:available|enabled|supported|offered)\b",
    re.IGNORECASE,
)
_HOURLY_NEGATION = re.compile(
    r"\b(?:not|no|never)\s+(?:(?:an?|any|available|supported)\s+)?hourly\b"
    r"|\bhourly(?:[- ](?:contracts?|jobs?|work|hiring))?\s+(?:is\s+|are\s+)?(?:not|disabled|unavailable|unsupported)\b",
    re.IGNORECASE,
)


def business_claim_problems(text):
    """Reject GHH business claims, not competitors' attributed planning benchmarks."""
    text = normalized(text)
    problems = [f"unsupported screening claim {m.group(0)!r}" for m in SCREENING_CLAIM.finditer(text)]
    for sentence in re.split(r"(?<=[.!?;])\s+", text):
        if _MONEY.search(sentence):
            for match in _RATE_SUBJECT.finditer(sentence):
                # A disclaimer about GHH rates cannot exempt a separate positive claim.
                prefix = sentence[:match.start()]
                if not re.search(r"\b(?:not|no|never)\s+(?:(?:an?|the|actual|fixed|standard)\s+)?$", prefix, re.IGNORECASE):
                    problems.append(f"unsupported GoHireHumans rate claim {sentence!r}")
        for match in _HOURLY_SUBJECT.finditer(sentence):
            if not _HOURLY_NEGATION.search(match.group(0)):
                problems.append(f"unsupported GoHireHumans hourly hiring claim {match.group(0)!r}")
    return problems


class HireWebDeveloperPageTests(unittest.TestCase):
    def setUp(self):
        self.load_source((FRONTEND / PATH.lstrip("/")).read_text(encoding="utf-8"))

    def load_source(self, source):
        """Use the same page contracts for real HTML and in-memory mutations."""
        self.source = source
        document = Document(self.source)
        self.page, self.schemas = document.root, document.schemas
        for schema in self.schemas:
            roots = schema if isinstance(schema, list) else [schema]
            for root in roots:
                self.assertIsInstance(root, dict, "JSON-LD root must be an object")
                self.assertEqual(root.get("@context"), SCHEMA_CONTEXT, "JSON-LD root context")
        self.nodes = [node for node, _ in schema_nodes(self.schemas)]

    def one_schema(self, kind):
        nodes = nodes_of_type(self.schemas, kind)
        self.assertEqual(len(nodes), 1, f"Exactly one {kind} node required")
        node, context = nodes[0]
        self.assertEqual(context, SCHEMA_CONTEXT, f"{kind} effective context")
        return node

    def test_snippet_and_page_schema_are_bound_to_this_entry(self):
        self.assertEqual([n.text() for n in self.page.find("title")], [TITLE])
        for key, value in {
            "description": DESCRIPTION, "og:title": TITLE, "twitter:title": TITLE,
            "og:description": DESCRIPTION, "twitter:description": DESCRIPTION,
            "og:url": URL,
        }.items():
            with self.subTest(surface=key):
                matches = [n.attrs.get("content") for n in self.page.find("meta")
                           if (n.attrs.get("name") or n.attrs.get("property")) == key]
                self.assertEqual(matches, [value])
        self.assertLessEqual(len(TITLE), 60)
        self.assertTrue(140 <= len(DESCRIPTION) <= 160)
        self.assertEqual([n.attrs["href"] for n in self.page.find("link")
                          if n.attrs.get("rel") == "canonical"], [URL])
        webpage = self.one_schema("WebPage")
        self.assertEqual(webpage["url"], URL)
        self.assertEqual(webpage["name"], TITLE)
        self.assertEqual(webpage["description"], DESCRIPTION)
        self.assertEqual(webpage["dateModified"], MODIFIED)
        breadcrumb = self.one_schema("BreadcrumbList")
        self.assertEqual(breadcrumb["itemListElement"][-1]["item"], URL)
        faq = self.one_schema("FAQPage")
        self.assertEqual(faq["@id"], URL + "#faq")
        self.assertEqual(faq["url"], URL)
        self.assertEqual(len(self.page.find("h1")), 1)
        self.assertEqual(self.page.find("h1")[0].text(),
                         "Hire Web Developers for Website Fixes, QA & Landing Pages")

    def test_sitemap_and_each_inbound_card_match_this_entry(self):
        ns = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}
        tree = ET.parse(FRONTEND / "sitemap.xml")
        entries = [entry for entry in tree.findall("sm:url", ns)
                   if entry.findtext("sm:loc", namespaces=ns) == URL]
        self.assertEqual(len(entries), 1)
        self.assertEqual(entries[0].findtext("sm:lastmod", namespaces=ns), MODIFIED)
        for rel in SIBLINGS:
            with self.subTest(page=rel):
                doc = Document((FRONTEND / rel).read_text()).root
                cards = [a for a in doc.find("a") if a.attrs.get("href") == PATH]
                self.assertEqual(len(cards), 1)
                spans = cards[0].find("span")
                titles = [s.text() for s in spans if s.attrs.get("class") == "card-list-title"]
                descriptions = [s.text() for s in spans if s.attrs.get("class") == "card-list-desc"]
                self.assertEqual(titles, [CARD_TITLE])
                self.assertEqual(descriptions, [DESCRIPTION])

    def test_scoped_task_sections_and_real_hiring_workflow(self):
        headings = {normalized(n.text()) for n in self.page.find("h2")}
        for heading in (
            "Hire a web developer for a small project",
            "Example tasks you can post",
            "How hiring works on GoHireHumans",
            "How to scope a web fix so it gets done right",
            "Web developer costs: your payout and fees",
        ):
            self.assertIn(heading, headings)
        main = self.page.find("main")[0]
        body = normalized(main.text())
        for claim in (
            "responsive layout", "landing page", "AI-built", "small business",
            "Workers must finish Stripe payout setup before they can apply",
            "saved card", "charged when you hire", "approve the work",
            "payout is released", "fixed-price", "account-owner approval",
            "General market ranges", "not GoHireHumans quotes",
            "1% platform fee plus a fixed 3% processing charge where checkout is configured",
        ):
            self.assertIn(claim, body)
        steps = [n for n in main.find("ol") if n.attrs.get("class") == "steps"]
        self.assertEqual(len(steps), 1)
        self.assertEqual(len(steps[0].find("li")), 5)
        self.assertIn("$100 payout", body)
        self.assertIn("$104", body)

    def test_faq_schema_equals_every_visible_question_and_answer(self):
        containers = [node for node in self.page.find("section") if node.attrs.get("id") == "faq"]
        self.assertEqual(len(containers), 1, "Exactly one FAQ section required")
        container = containers[0]
        self.assertFalse(container.hidden_by, f"FAQ container is hidden: {container.hidden_by}")
        self.assertEqual(self.page.find("details"), container.find("details"), "All FAQ items must be inside #faq")
        visible = []
        for item in container.find("details"):
            summaries, answers = item.find("summary"), item.find("p")
            self.assertEqual(len(summaries), 1)
            self.assertEqual(len(answers), 1)
            for node in (item, *summaries, *answers):
                self.assertFalse(node.hidden_by, f"FAQ {node.tag} is hidden: {node.hidden_by}")
            visible.append((normalized(summaries[0].text()), normalized(answers[0].text())))
        self.assertGreaterEqual(len(visible), 7)
        self.assertEqual(len({q for q, _ in visible}), len(visible))
        entities = self.one_schema("FAQPage")["mainEntity"]
        for entity in entities:
            self.assertEqual(entity["@type"], "Question")
            self.assertEqual(entity["acceptedAnswer"]["@type"], "Answer")
        structured = [(normalized(e["name"]), normalized(e["acceptedAnswer"]["text"]))
                      for e in entities]
        self.assertEqual(structured, visible)

    def test_no_retired_or_unsupported_claims_on_any_page_surface(self):
        for phrase in (
            "escrow", "guaranteed", "vetted", "insured", "top developers",
            "Stripe processing plus 1%", "Rates on GoHireHumans typically",
            "Many developers list small packages", "supports fixed-price tasks and hourly work",
            "Some listings on GoHireHumans are AI-agent services",
        ):
            with self.subTest(phrase=phrase):
                self.assertNotIn(phrase.lower(), self.source.lower())
        surfaces = self.page.claim_text().splitlines()
        surfaces += [value for node in self.page.find("*") for value in node.attrs.values()
                     if isinstance(value, str)]
        surfaces += [value for node in self.nodes for value in node.values()
                     if isinstance(value, str)]
        for surface in surfaces:
            self.assertEqual(business_claim_problems(surface), [], surface)

    def test_all_draft_links_keep_canonical_web_development_category(self):
        from urllib.parse import parse_qs, urlsplit
        drafts = []
        examples = []
        for link in self.page.find("a"):
            href = link.attrs.get("href", "")
            fragment = urlsplit(href).fragment
            if not fragment.startswith("/post-job?"):
                continue
            query = parse_qs(fragment.split("?", 1)[1])
            drafts.append(query)
            self.assertEqual(query.get("draft_category"), ["web_development"], href)
            if "draft_title" in query:
                examples.append(query)
                self.assertTrue(query["draft_title"][0].strip())
                self.assertTrue(query.get("draft_skills", [""])[0].strip())
                descriptions = [n.text() for n in link.find("span")
                                if n.attrs.get("class") == "card-list-desc"]
                self.assertEqual(query.get("draft_description"), descriptions)
        self.assertGreaterEqual(len(drafts), 8)
        self.assertEqual(len(examples), 6)

    def test_each_internal_link_target_exists(self):
        from urllib.parse import urlsplit
        for link in self.page.find("a"):
            href = link.attrs.get("href", "")
            parsed = urlsplit(href)
            if not href.startswith("/") or parsed.netloc:
                continue
            path = parsed.path
            target = FRONTEND / path.lstrip("/")
            if path.endswith("/"):
                target /= "index.html"
            with self.subTest(href=href):
                self.assertTrue(target.is_file(), f"Missing internal target: {href}")


class HireWebDeveloperContractGuardTests(unittest.TestCase):
    """Replay the real contracts against mutations without writing any page."""

    CLAIM_CHECK = "test_no_retired_or_unsupported_claims_on_any_page_surface"
    FAQ_CHECK = "test_faq_schema_equals_every_visible_question_and_answer"

    def setUp(self):
        self.source = (FRONTEND / PATH.lstrip("/")).read_text(encoding="utf-8")

    def check_source(self, source, method):
        case = HireWebDeveloperPageTests(method)
        case.load_source(source)
        getattr(case, method)()

    def with_claim(self, claim):
        self.assertEqual(self.source.count("</main>"), 1)
        return self.source.replace("</main>", f"<p>{claim}</p>\n</main>")

    def test_ghh_rate_claim_is_caught(self):
        with self.assertRaisesRegex(AssertionError, "unsupported GoHireHumans rate claim"):
            self.check_source(self.with_claim("GoHireHumans web developers charge $30–$200 per hour."), self.CLAIM_CHECK)

    def test_ghh_hourly_hiring_claim_is_caught(self):
        with self.assertRaisesRegex(AssertionError, "unsupported GoHireHumans hourly hiring claim"):
            self.check_source(self.with_claim("Hire a developer on an hourly contract on GoHireHumans."), self.CLAIM_CHECK)

    def changed(self, old, new, count=1):
        self.assertEqual(self.source.count(old), count, f"mutation anchor {old!r}")
        return self.source.replace(old, new, count)

    def test_wrong_faq_context_is_caught(self):
        source = self.changed('"@context": "https://schema.org",\n  "@type": "FAQPage"',
                              '"@context": "https://example.org",\n  "@type": "FAQPage"')
        with self.assertRaisesRegex(AssertionError, "context"):
            self.check_source(source, self.FAQ_CHECK)

    def test_hidden_faq_container_is_caught(self):
        source = self.changed('id="faq">', 'id="faq" hidden>')
        with self.assertRaisesRegex(AssertionError, "hidden"):
            self.check_source(source, self.FAQ_CHECK)

    def test_removed_first_closing_section_is_caught(self):
        self.assertGreater(self.source.count("</section>"), 0)
        source = self.source.replace("</section>", "", 1)
        with self.assertRaisesRegex(AssertionError, "mismatched|unclosed"):
            self.check_source(source, self.FAQ_CHECK)

    def test_faq_hidden_by_attributes_styles_or_classes_is_caught(self):
        attributes = (
            'aria-hidden="true"', 'aria-hidden=" TRUE "',
            'style="display: none"', 'style="visibility: hidden !important"',
            'class="sr-only"', 'class="visually-hidden"', 'class="hidden"', 'class="d-none"',
        )
        for attrs in attributes:
            if attrs.startswith('class="'):
                hidden_class = attrs.split('"')[1]
                source = self.changed('class="pub-section pub-section--alt" id="faq"',
                                      f'class="pub-section pub-section--alt {hidden_class}" id="faq"')
            else:
                source = self.changed('id="faq">', f'id="faq" {attrs}>')
            with self.subTest(attrs=attrs), self.assertRaisesRegex(AssertionError, "hidden"):
                self.check_source(source, self.FAQ_CHECK)

    def test_hidden_faq_ancestors_are_caught(self):
        for old, new in (
            ('<body>', '<body hidden>'),
            ('<main id="main-content"', '<main aria-hidden="true" id="main-content"'),
            ('id="faq">\n    <div class="pub-inner pub-inner--narrow">',
             'id="faq">\n    <div class="pub-inner pub-inner--narrow" style="visibility:hidden">'),
        ):
            with self.subTest(ancestor=old), self.assertRaisesRegex(AssertionError, "hidden"):
                self.check_source(self.changed(old, new), self.FAQ_CHECK)

    def test_hidden_faq_items_questions_and_answers_are_caught(self):
        for old, new in (
            ('<details class="disclosure">', '<details class="disclosure" hidden>'),
            ('<summary>How much does it cost', '<summary style="display:none">How much does it cost'),
            ('<p>There is no fixed GoHireHumans developer rate.',
             '<p aria-hidden="true">There is no fixed GoHireHumans developer rate.'),
        ):
            count = self.source.count(old)
            self.assertGreater(count, 0)
            with self.subTest(element=old), self.assertRaisesRegex(AssertionError, "hidden"):
                self.check_source(self.changed(old, new, count), self.FAQ_CHECK)

    def test_non_hiding_faq_css_stays_allowed(self):
        for style in (
            "backface-visibility: hidden", "--visibility: hidden; visibility: visible",
            "display: block", "opacity: 0.5",
        ):
            with self.subTest(style=style):
                self.check_source(self.changed('id="faq">', f'id="faq" style="{style}">'), self.FAQ_CHECK)

    def test_void_tags_and_self_closing_svg_stay_valid(self):
        document = Document('<main><br><img src="example.png"/><object><param name="x"></object>'
                            '<svg><line x1="0" x2="1"/></svg></main>')
        self.assertEqual(len(document.root.find("param")), 1)
        self.assertEqual(len(document.root.find("line")), 1)

    def test_unmatched_misnested_and_unclosed_tags_are_caught(self):
        cases = (
            (self.changed('</main>', '</aside></main>'), 'mismatched </aside>'),
            (self.source.replace('</section>', '</div>', 1), 'mismatched </div>'),
            (self.changed('</html>', ''), 'unclosed <html>'),
            (self.changed('</main>', '</br></main>'), 'unmatched </br>'),
        )
        for source, fragment in cases:
            with self.subTest(problem=fragment), self.assertRaisesRegex(AssertionError, fragment):
                self.check_source(source, self.FAQ_CHECK)

    def transformed_faq_schema(self, transform):
        matches = []
        for match in re.finditer(r'<script type="application/ld\+json">(.*?)</script>', self.source, re.DOTALL):
            schema = json.loads(match.group(1))
            if schema.get("@type") == "FAQPage":
                matches.append((match, schema))
        self.assertEqual(len(matches), 1, "FAQ JSON-LD mutation anchor")
        match, schema = matches[0]
        replacement = json.dumps(transform(schema), indent=2)
        return self.source[:match.start(1)] + replacement + self.source[match.end(1):]

    def test_faq_context_inherited_from_graph_root_stays_allowed(self):
        def graph(schema):
            del schema["@context"]
            schema["@type"] = ["FAQPage"]
            return {"@context": SCHEMA_CONTEXT, "@graph": [schema]}
        source = self.transformed_faq_schema(graph)
        self.check_source(source, self.FAQ_CHECK)
        self.check_source(source, "test_snippet_and_page_schema_are_bound_to_this_entry")

    def test_wrong_local_faq_context_overriding_graph_root_is_caught(self):
        def graph(schema):
            schema["@context"] = "https://example.org"
            return {"@context": SCHEMA_CONTEXT, "@graph": [schema]}
        with self.assertRaisesRegex(AssertionError, "FAQPage effective context"):
            self.check_source(self.transformed_faq_schema(graph), self.FAQ_CHECK)

    def test_missing_json_ld_root_context_is_caught(self):
        source = self.changed('"@context": "https://schema.org",\n  "@type": "FAQPage"',
                              '"@type": "FAQPage"')
        with self.assertRaisesRegex(AssertionError, "JSON-LD root context"):
            self.check_source(source, self.FAQ_CHECK)

    def test_competitor_rate_benchmarks_stay_allowed(self):
        # Exercise the actual attributed rows, not just an invented allowed sentence.
        rows = Document(self.source).root.find("tr")
        benchmarks = [normalized(row.text()) for row in rows if row.find("td")]
        self.assertEqual(len(benchmarks), 2)
        for amount, guide in (("$15–$50/hr", "Upwork's cost guide"),
                              ("$500–$2,000 per project", "Upwork's hiring guide")):
            self.assertTrue(any(amount in row and guide in row for row in benchmarks))
        self.check_source(self.source, self.CLAIM_CHECK)
        for claim in (
            "Upwork web developers charge $15–$50 per hour according to Upwork's public cost guide.",
            "Fiverr's public guide lists developer rates of $30–$200 per hour, not GoHireHumans quotes.",
            "Upwork's cost guide lists web developers at $15–$50/hr, not GoHireHumans rates.",
            "Unlike GoHireHumans, Upwork offers hourly contracts.",
        ):
            with self.subTest(claim=claim):
                self.check_source(self.with_claim(claim), self.CLAIM_CHECK)

    def test_stripe_payout_wording_stays_allowed(self):
        for claim in (
            "Workers must finish Stripe payout setup before they can apply to jobs.",
            "Stripe verifies workers' identity for payouts.",
            "Workers are verified by Stripe for payouts before they can apply.",
            "Developers are verified for Stripe payouts.",
        ):
            with self.subTest(claim=claim):
                self.check_source(self.with_claim(claim), self.CLAIM_CHECK)

    def test_truthful_fixed_price_and_hourly_limit_wording_stays_allowed(self):
        for claim in (
            "GoHireHumans charges $4 in employer fees on a $100 payout where checkout is configured.",
            "GoHireHumans does not support hourly hiring.",
            "Hourly hiring is not available on GoHireHumans.",
            "The hourly benchmark is budgeting context, not an hourly-contract offer on GoHireHumans.",
        ):
            with self.subTest(claim=claim):
                self.check_source(self.with_claim(claim), self.CLAIM_CHECK)

    def test_claims_in_metadata_and_decoded_json_ld_are_caught(self):
        claims = {
            "GoHireHumans web developers charge $30–$200 per hour.": "rate claim",
            "Hire a developer on an hourly contract on GoHireHumans.": "hourly hiring claim",
            "All developers are screened by GoHireHumans.": "screening claim",
        }
        for claim, fragment in claims.items():
            metadata = self.changed('<meta name="description" content="',
                                    '<meta name="description" content="' + claim + ' ')
            structured = self.changed('"@type": "WebPage",',
                                      '"@type": "WebPage", "addedCopy": ' + json.dumps(claim) + ',')
            for surface, source in (("metadata", metadata), ("JSON-LD", structured)):
                with self.subTest(claim=claim, surface=surface), self.assertRaisesRegex(AssertionError, fragment):
                    self.check_source(source, self.CLAIM_CHECK)

    def test_inline_markup_does_not_hide_a_business_claim(self):
        source = self.with_claim("<strong>GoHireHumans</strong> web developers charge $30–$200 per hour.")
        with self.assertRaisesRegex(AssertionError, "rate claim"):
            self.check_source(source, self.CLAIM_CHECK)

    def test_equivalent_ghh_business_claims_are_caught(self):
        claims = {
            "GoHireHumans developer rates range from $30 to $200 per hour.": "rate claim",
            "Web developers on GoHireHumans charge $30–$200/hr.": "rate claim",
            "Rates on GHH start at $30 per hour.": "rate claim",
            "GoHireHumans supports hourly contracts.": "hourly hiring claim",
            "Hourly hiring is available on GoHireHumans.": "hourly hiring claim",
            "You can hire a developer by the hour on GoHireHumans.": "hourly hiring claim",
            "GoHireHumans supports hourly work, not hourly contracts.": "hourly hiring claim",
            "GoHireHumans screens all developers.": "screening claim",
            "Developers have been background-checked.": "screening claim",
            "GoHireHumans vets web developers.": "screening claim",
        }
        for claim, fragment in claims.items():
            with self.subTest(claim=claim), self.assertRaisesRegex(AssertionError, fragment):
                self.check_source(self.with_claim(claim), self.CLAIM_CHECK)

    def test_ghh_developer_screening_claim_is_caught(self):
        with self.assertRaisesRegex(AssertionError, "unsupported screening claim"):
            self.check_source(self.with_claim("All developers are screened by GoHireHumans."), self.CLAIM_CHECK)


if __name__ == "__main__":
    unittest.main()
