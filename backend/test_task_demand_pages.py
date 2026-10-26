"""Contracts for the buyer task pages built from current public demand (Oct 2026):
website/app testing, human voice over, subtitles/captions and short phone videos.

Each page must be self-consistent (title, snippets, canonical, JSON-LD, FAQ), discoverable
(sitemap with matching lastmod, extensionless redirect, hire hub card, crawled inbound links),
open drafts in a real category, and carry the safety rules that protect buyers and the Stripe
account (no fake reviews, no security bypass, consent and usage rights).
"""
import ast
import html
import json
import re
import unittest
import xml.etree.ElementTree as ET
from html.parser import HTMLParser
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

from test_trust_wording import claim_problems, faq_contract_problems, fee_snippet_problems

ROOT = Path(__file__).resolve().parents[1]
FRONTEND = ROOT / "frontend"
SITE = "https://www.gohirehumans.com"
MODIFIED = "2026-10-26"

PAGES = {
    "/hire/website-testers.html": {
        "title": "Hire Website & App Testers for Real Feedback | GoHireHumans",
        "h1": "Hire website and app testers for real human feedback",
        "categories": {"testing"},
        "must": ("app", "screen recording", "test account",
                 "bypass CAPTCHAs", "app store reviews"),
    },
    "/hire/voice-over.html": {
        "title": "Hire a Human Voice Over for Short Narration | GoHireHumans",
        "h1": "Hire a human voice over for narration and short videos",
        "categories": {"voice_over", "audio_production"},
        "must": ("human voice", "WAV", "usage rights", "imitate a real person"),
    },
    "/hire/subtitles-captions.html": {
        "title": "Hire Someone to Add Subtitles and Captions | GoHireHumans",
        "h1": "Hire someone to add subtitles and captions to your videos",
        "categories": {"video_editing", "transcription", "translation"},
        "must": ("SRT", "burned-in", "timing", "machine translation"),
    },
    "/hire/short-video-creators.html": {
        "title": "Hire Short Video Creators for Product Clips | GoHireHumans",
        "h1": "Hire people to record short videos and product clips",
        "categories": {"media_capture", "video_editing", "content_creation"},
        "must": ("shot list", "vertical", "fake reviews", "disclose", "consent"),
    },
}
SHARED_PHRASES = (
    "Example tasks you can post",
    "Suggested payout ranges",
    "Connector framing",
    "Workers receive the listed payout",
    "1% platform fee plus a fixed 3% processing charge where checkout is configured",
    "Workers must finish Stripe payout setup before they can apply",
)
FORBIDDEN = ("escrow", "guarantee", "vetted", "verified", "top-rated", "instant", "available now",
             "in minutes", "risk-free")
# Pages Google has crawled recently (URL Inspection, 2026-10-25) that must link to the new pages.
BLOG = "blog/how-to-find-human-workers-ai-tasks.html"  # crawled 2026-10-25
INBOUND = {
    "/hire/voice-over.html": ("hire/index.html", "hire/hire-video-editor.html", BLOG),
    "/hire/subtitles-captions.html": ("hire/index.html", "hire/hire-video-editor.html", "hire/hire-translator.html", BLOG),
    "/hire/short-video-creators.html": ("hire/index.html", "hire/hire-video-editor.html", BLOG),
    "/hire/website-testers.html": ("hire/index.html", "hire/hire-web-developer.html", "use-cases/website-qa-task.html", BLOG),
}


def valid_categories():
    tree = ast.parse((ROOT / "backend/api_core.py").read_text(encoding="utf-8"))
    for node in tree.body:
        if isinstance(node, ast.Assign) and any(getattr(t, "id", None) == "VALID_CATEGORIES" for t in node.targets):
            return set(ast.literal_eval(node.value))
    raise AssertionError("VALID_CATEGORIES not found")


class Page(HTMLParser):
    """Collect what the contracts need, failing on unbalanced markup."""
    VOID = {"area", "base", "br", "col", "embed", "hr", "img", "input", "link", "meta", "param", "source", "track", "wbr"}

    def __init__(self, source):
        super().__init__(convert_charrefs=True)
        self.titles, self.h1, self.metas, self.links, self.anchors, self.schemas = [], [], {}, [], [], []
        self.stack, self.errors, self.capture, self.buffer = [], [], None, []
        self.anchor = None
        self.feed(source)
        self.close()
        if self.stack:
            self.errors.append(f"unclosed {[t for t, _ in self.stack]}")

    def handle_starttag(self, tag, attrs):
        a = dict(attrs)
        if tag == "meta":
            key = a.get("name") or a.get("property")
            if key:
                self.metas.setdefault(key, []).append(a.get("content"))
        elif tag == "link":
            self.links.append(a)
        if tag in self.VOID:
            return
        self.stack.append((tag, self.getpos()[0]))
        if tag in {"title", "h1"} or (tag == "script" and a.get("type") == "application/ld+json"):
            self.capture, self.buffer = tag, []
        if tag == "a":
            self.anchor = {"href": a.get("href", ""), "text": [], "desc": []}
            self.anchors.append(self.anchor)
        if tag == "span" and self.anchor is not None and a.get("class") == "card-list-desc":
            self.capture, self.buffer = "desc", []

    def handle_endtag(self, tag):
        if tag in self.VOID:
            return
        if not self.stack or self.stack[-1][0] != tag:
            self.errors.append(f"mismatched </{tag}> on line {self.getpos()[0]}")
            return
        self.stack.pop()
        text = "".join(self.buffer)
        if self.capture == "title" and tag == "title":
            self.titles.append(text)
        elif self.capture == "h1" and tag == "h1":
            self.h1.append(" ".join(text.split()))
        elif self.capture == "script" and tag == "script":
            self.schemas.append(json.loads(text))
        elif self.capture == "desc" and tag == "span" and self.anchor is not None:
            self.anchor["desc"].append(" ".join(text.split()))
        if tag in {"title", "h1", "script"} or (tag == "span" and self.capture == "desc"):
            self.capture = None
        if tag == "a":
            self.anchor = None

    def handle_data(self, data):
        if self.capture:
            self.buffer.append(data)
        if self.anchor is not None:
            self.anchor["text"].append(data)

    def schema(self, kind):
        found = [s for s in self.schemas if s.get("@type") == kind]
        if len(found) != 1:
            raise AssertionError(f"expected one {kind}, found {len(found)}")
        if found[0].get("@context") != "https://schema.org":
            raise AssertionError(f"{kind} context")
        return found[0]


def page_problems(path, source, spec, categories):
    """Every contract for one page; returns a list so mutations can be replayed in memory."""
    problems = []
    url = SITE + path
    try:
        page = Page(source)
    except (json.JSONDecodeError, AssertionError) as exc:
        return [f"parse: {exc}"]
    problems += page.errors
    title = spec["title"]
    if page.titles != [title]:
        problems.append(f"title {page.titles!r}")
    if len(title) > 60:
        problems.append(f"title is {len(title)} chars")
    if page.h1 != [spec["h1"]]:
        problems.append(f"h1 {page.h1!r}")
    descriptions = page.metas.get("description", [])
    if len(descriptions) != 1:
        problems.append("exactly one meta description")
        return problems
    description = descriptions[0]
    if not 120 <= len(description) <= 160:
        problems.append(f"description is {len(description)} chars")
    for key, value in (("og:title", title), ("twitter:title", title), ("og:description", description),
                       ("twitter:description", description), ("og:url", url)):
        if page.metas.get(key) != [value]:
            problems.append(f"{key} {page.metas.get(key)!r}")
    if page.metas.get("robots") != ["index, follow, max-image-preview:large"]:
        problems.append("robots meta")
    if [l["href"] for l in page.links if l.get("rel") == "canonical"] != [url]:
        problems.append("canonical")
    try:
        webpage = page.schema("WebPage")
        for key, value in (("url", url), ("name", title), ("description", description), ("dateModified", MODIFIED)):
            if webpage.get(key) != value:
                problems.append(f"WebPage {key} {webpage.get(key)!r}")
        if page.schema("BreadcrumbList")["itemListElement"][-1]["item"] != url:
            problems.append("breadcrumb")
        faq = page.schema("FAQPage")
        if faq.get("url") != url or faq.get("@id") != url + "#faq" or len(faq["mainEntity"]) < 5:
            problems.append("FAQPage binding")
    except AssertionError as exc:
        problems.append(str(exc))
    problems += [f"FAQ: {p}" for p in faq_contract_problems(source)]
    problems += [f"claim: {c}" for c in claim_problems(source)]
    problems += [f"fee snippet: {c}" for c in fee_snippet_problems(source)]
    for phrase in SHARED_PHRASES + spec["must"]:
        if phrase not in source:
            problems.append(f"missing {phrase!r}")
    lower = source.lower()
    for phrase in FORBIDDEN:
        if phrase in lower:
            problems.append(f"forbidden {phrase!r}")
    drafts = examples = 0
    for a in page.anchors:
        href = a["href"]
        parsed = urlsplit(href)
        if parsed.fragment.startswith("/post-job?"):  # bare /post-job is the shared nav CTA
            drafts += 1
            query = parse_qs(parsed.fragment.partition("?")[2])
            if "template" in query:
                continue
            category = query.get("draft_category", [""])[0]
            if category not in categories or category not in spec["categories"]:
                problems.append(f"draft category {category!r} in {href[:80]}")
            if "draft_title" in query:
                examples += 1
                if not query.get("draft_skills", [""])[0].strip():
                    problems.append(f"draft without skills: {href[:80]}")
                if query.get("draft_description") != a["desc"]:
                    problems.append(f"draft description differs from card: {href[:80]}")
        elif href.startswith("/") and not parsed.netloc and not href.startswith("/#"):
            target = FRONTEND / parsed.path.lstrip("/")
            if parsed.path.endswith("/"):
                target /= "index.html"
            if not target.is_file():
                problems.append(f"missing internal target {href}")
    if examples < 4:
        problems.append(f"only {examples} example drafts")
    if drafts < examples + 2:
        problems.append("hero and closing draft CTAs")
    return problems


class TaskDemandPageTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.categories = valid_categories()
        cls.sitemap = ET.parse(FRONTEND / "sitemap.xml").getroot()
        cls.vercel = json.loads((FRONTEND / "vercel.json").read_text(encoding="utf-8"))

    def test_every_page_meets_its_contract(self):
        for path, spec in PAGES.items():
            with self.subTest(page=path):
                source = (FRONTEND / path.lstrip("/")).read_text(encoding="utf-8")
                self.assertEqual(page_problems(path, source, spec, self.categories), [])

    def test_titles_and_descriptions_are_unique_across_the_site(self):
        seen = {}
        for page in FRONTEND.rglob("*.html"):
            if {"node_modules", "tests", "test-results"} & set(page.relative_to(FRONTEND).parts):
                continue
            text = page.read_text(encoding="utf-8", errors="ignore")
            for key in re.findall(r"<title>([^<]*)</title>", text) + re.findall(r'<meta name="description" content="([^"]*)"', text):
                seen.setdefault(html.unescape(key), []).append(str(page.relative_to(FRONTEND)))
        for path, spec in PAGES.items():
            with self.subTest(page=path):
                self.assertEqual(seen.get(spec["title"]), [path.lstrip("/")])

    def test_sitemap_redirect_and_lastmod(self):
        redirects = {(r["source"], r["destination"], r.get("permanent")) for r in self.vercel["redirects"]}
        for path in PAGES:
            with self.subTest(page=path):
                entries = [u for u in self.sitemap.findall("{*}url") if u.findtext("{*}loc") == SITE + path]
                self.assertEqual(len(entries), 1)
                self.assertEqual(entries[0].findtext("{*}lastmod"), MODIFIED)
                self.assertIn((path[:-5], path, True), redirects)

    def test_crawled_pages_link_to_each_page(self):
        for path, sources in INBOUND.items():
            for rel in sources:
                with self.subTest(page=path, source=rel):
                    page = Page((FRONTEND / rel).read_text(encoding="utf-8"))
                    self.assertGreaterEqual(sum(1 for a in page.anchors if a["href"] == path), 1)

    def test_hub_lists_every_task_page_once_with_the_page_description(self):
        hub = Page((FRONTEND / "hire/index.html").read_text(encoding="utf-8"))
        for path in PAGES:
            with self.subTest(page=path):
                cards = [a for a in hub.anchors if a["href"] == path]
                self.assertEqual(len(cards), 1)
                self.assertEqual(len(cards[0]["desc"]), 1)


class TaskDemandContractGuardTests(unittest.TestCase):
    """The contract must catch the drift it exists for."""
    PATH = "/hire/voice-over.html"

    def setUp(self):
        self.source = (FRONTEND / self.PATH.lstrip("/")).read_text(encoding="utf-8")
        self.spec = PAGES[self.PATH]
        self.categories = valid_categories()

    def problems(self, source):
        return page_problems(self.PATH, source, self.spec, self.categories)

    def test_real_page_is_clean(self):
        self.assertEqual(self.problems(self.source), [])

    def test_invalid_draft_category_is_caught(self):
        self.assertIn("draft_category=voice_over", self.source)
        mutated = self.source.replace("draft_category=voice_over", "draft_category=voiceover", 1)
        self.assertTrue(any("draft category" in p for p in self.problems(mutated)))

    def test_card_description_drift_is_caught(self):
        match = re.search(r'<span class="card-list-desc">([^<]+)</span>', self.source)
        assert match is not None
        mutated = self.source[:match.start(1)] + "Something else entirely." + self.source[match.end(1):]
        self.assertTrue(any("draft description differs" in p for p in self.problems(mutated)))

    def test_faq_drift_and_unsupported_claims_are_caught(self):
        match = re.search(r"<details class=\"disclosure\"><summary>[^<]+</summary><p>([^<]+)</p>", self.source)
        assert match is not None
        drift = self.source[:match.start(1)] + "A different answer." + self.source[match.end(1):]
        self.assertTrue(any(p.startswith("FAQ:") for p in self.problems(drift)))
        claim = self.source.replace("</main>", "<p>All voice actors are vetted and guaranteed.</p></main>", 1)
        found = self.problems(claim)
        self.assertTrue(any("vetted" in p for p in found), found)
        self.assertTrue(any("guarantee" in p for p in found), found)

    def test_missing_safety_rule_and_wrong_lastmod_are_caught(self):
        mutated = self.source.replace("imitate a real person", "sound like anyone")
        self.assertIn("missing 'imitate a real person'", self.problems(mutated))
        mutated = self.source.replace(f'"dateModified": "{MODIFIED}"', '"dateModified": "2026-01-01"')
        self.assertTrue(any("WebPage dateModified" in p for p in self.problems(mutated)))


if __name__ == "__main__":
    unittest.main()
