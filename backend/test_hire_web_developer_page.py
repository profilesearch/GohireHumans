"""Entry-bound contracts for the web developer hiring page (stdlib only)."""
import json
import unittest
import xml.etree.ElementTree as ET
from html.parser import HTMLParser
from pathlib import Path

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
    def __init__(self, tag, attrs=()):
        self.tag, self.attrs, self.children = tag, dict(attrs), []

    def text(self):
        return "".join(c.text() if isinstance(c, Element) else c for c in self.children)

    def find(self, tag):
        found: list[Element] = [self] if self.tag == tag else []
        for child in self.children:
            if isinstance(child, Element):
                found.extend(child.find(tag))
        return found


class Document(HTMLParser):
    VOID = {"area", "base", "br", "col", "embed", "hr", "img", "input", "link", "meta", "param", "source", "track", "wbr"}

    def __init__(self, source):
        super().__init__(convert_charrefs=True)
        self.root = Element("document")
        self.stack = [self.root]
        self.feed(source)

    def handle_starttag(self, tag, attrs):
        node = Element(tag, attrs)
        self.stack[-1].children.append(node)
        if tag not in self.VOID:
            self.stack.append(node)

    def handle_startendtag(self, tag, attrs):
        self.handle_starttag(tag, attrs)
        if tag not in self.VOID:
            self.handle_endtag(tag)

    def handle_endtag(self, tag):
        for i in range(len(self.stack) - 1, 0, -1):
            if self.stack[i].tag == tag:
                del self.stack[i:]
                break

    def handle_data(self, data):
        self.stack[-1].children.append(data)


def normalized(text):
    return " ".join(text.split())


def schema_nodes(value):
    if isinstance(value, list):
        for item in value:
            yield from schema_nodes(item)
    elif isinstance(value, dict):
        if "@type" in value:
            yield value
        for child in value.values():
            if isinstance(child, (dict, list)):
                yield from schema_nodes(child)


class HireWebDeveloperPageTests(unittest.TestCase):
    def setUp(self):
        self.source = (FRONTEND / PATH.lstrip("/")).read_text()
        self.page = Document(self.source).root
        self.nodes = []
        for script in self.page.find("script"):
            if script.attrs.get("type") == "application/ld+json":
                self.nodes.extend(schema_nodes(json.loads(script.text())))

    def one_schema(self, kind):
        nodes = [n for n in self.nodes if n.get("@type") == kind]
        self.assertEqual(len(nodes), 1, f"Exactly one {kind} node required")
        return nodes[0]

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
        visible = []
        for item in self.page.find("details"):
            summaries, answers = item.find("summary"), item.find("p")
            self.assertEqual(len(summaries), 1)
            self.assertEqual(len(answers), 1)
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


if __name__ == "__main__":
    unittest.main()
