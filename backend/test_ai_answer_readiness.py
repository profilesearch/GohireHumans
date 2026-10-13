"""Contracts for concise, static answers humans and AI crawlers can quote."""
import json
import re
import unittest
import xml.etree.ElementTree as ET
from html.parser import HTMLParser
from pathlib import Path
from urllib import robotparser
from urllib.parse import urlsplit

FRONTEND = Path(__file__).resolve().parents[1] / "frontend"
SITE = "https://www.gohirehumans.com/"
VOID_TAGS = {"area", "base", "br", "col", "embed", "hr", "img", "input", "link", "meta", "source", "track", "wbr"}
HIDDEN_CLASSES = {"hidden", "sr-only", "visually-hidden", "d-none", "is-hidden", "invisible"}
HIDDEN_STYLE = re.compile(r"display:none|visibility:hidden|visibility:collapse|opacity:0(?![.\d])|content-visibility:hidden")
RETIRED_PHRASES = ("escrow", "stripe processing plus 1%", "guaranteed", "vetted", "owners approve each hire")
# Worker-screening claims GoHireHumans does not make. Stripe payout verification wording stays allowed.
SCREENING_CLAIM = re.compile(
    r"\b(?:verified|vetted|screened|pre-screened|background[- ]checked|certified|trusted|top-rated)\s+"
    r"(?:workers?|humans?|people|freelancers?|professionals?|providers?|sellers?|testers?|talent|experts?)\b"
    r"|\bwe\s+(?:verify|vet|screen|background[- ]check)\s+(?:all\s+|every\s+)?(?:workers?|humans?|freelancers?|providers?|sellers?)\b",
    re.IGNORECASE,
)


def normalized(text):
    return " ".join(text.split())


def schema_types(node):
    value = node.get("@type", [])
    return [value] if isinstance(value, str) else list(value)


def schema_nodes(value):
    """Every JSON-LD object, including nodes nested in @graph or properties."""
    if isinstance(value, dict):
        yield value
        for child in value.values():
            yield from schema_nodes(child)
    elif isinstance(value, list):
        for child in value:
            yield from schema_nodes(child)


def claim_problems(text):
    lowered = text.lower()
    problems = [f"retired phrase {phrase!r}" for phrase in RETIRED_PHRASES if phrase in lowered]
    problems += [f"unsupported screening claim {m.group(0)!r}" for m in SCREENING_CLAIM.finditer(text)]
    return problems


class AnswerHTML(HTMLParser):
    """Read visible answer cards and JSON-LD independently of JavaScript.

    Problems are collected rather than asserted so the contract also holds under `python -O`.
    """

    def __init__(self, text):
        super().__init__(convert_charrefs=True)
        self.pairs = []
        self.schemas = []
        self.errors = []
        self.stack = []
        self.card = None
        self.field = None
        self.buffer = []
        self.ld = None
        self.feed(text)
        self.close()
        for tag, _, line in self.stack:
            self.errors.append(f"unclosed <{tag}> from line {line}")

    def hidden_reason(self, attrs):
        if "hidden" in attrs:
            return "hidden attribute"
        if (attrs.get("aria-hidden") or "").strip().lower() == "true":
            return "aria-hidden"
        classes = set((attrs.get("class") or "").split())
        if classes & HIDDEN_CLASSES:
            return "hiding class " + ", ".join(sorted(classes & HIDDEN_CLASSES))
        style = re.sub(r"\s+", "", (attrs.get("style") or "").lower())
        match = HIDDEN_STYLE.search(style)
        return "inline style " + match.group(0) if match else None

    def handle_starttag(self, tag, attrs):
        attrs = dict(attrs)
        line = self.getpos()[0]
        if tag == "script" and attrs.get("type") == "application/ld+json":
            self.ld = []
        if "data-ai-answer" in attrs:
            if tag != "article" or "main" not in [t for t, _, _ in self.stack]:
                self.errors.append(f"answer card on line {line} must be an <article> inside <main>")
            for ancestor, ancestor_attrs, _ in self.stack + [(tag, attrs, line)]:
                reason = self.hidden_reason(ancestor_attrs)
                if reason:
                    self.errors.append(f"answer card on line {line} is hidden by <{ancestor}> {reason}")
            self.card = {"line": str(line)}
        if self.card is not None and tag in ("h3", "p"):
            self.field = tag
            self.buffer = []
        if tag not in VOID_TAGS:
            self.stack.append((tag, attrs, line))

    def handle_startendtag(self, tag, attrs):
        if "data-ai-answer" in dict(attrs):
            self.errors.append(f"answer card on line {self.getpos()[0]} is self-closing")

    def handle_data(self, data):
        if self.ld is not None:
            self.ld.append(data)
        if self.field:
            self.buffer.append(data)

    def handle_endtag(self, tag):
        if tag in VOID_TAGS:
            return
        if tag == "script" and self.ld is not None:
            try:
                self.schemas.append(json.loads("".join(self.ld)))
            except json.JSONDecodeError as exc:
                self.errors.append(f"invalid JSON-LD near line {self.getpos()[0]}: {exc}")
            self.ld = None
        if tag == self.field and self.card is not None:
            self.card[tag] = normalized("".join(self.buffer))
            self.field = None
        if tag == "article" and self.card is not None:
            if "h3" in self.card and "p" in self.card:
                self.pairs.append((self.card["h3"], self.card["p"]))
            else:
                self.errors.append(f"answer card on line {self.card['line']} needs an <h3> question and <p> answer")
            self.card = None
        if self.stack and self.stack[-1][0] == tag:
            self.stack.pop()
        else:
            open_tag = self.stack[-1][0] if self.stack else "nothing"
            self.errors.append(f"mismatched </{tag}> on line {self.getpos()[0]} while <{open_tag}> is open")


def faq_contract_problems(relative, questions, text):
    parsed = AnswerHTML(text)
    problems = list(parsed.errors)
    for schema in parsed.schemas:
        if isinstance(schema, dict) and schema.get("@context") != "https://schema.org":
            problems.append("JSON-LD block without https://schema.org context")
    faqs = [node for node in schema_nodes(parsed.schemas) if "FAQPage" in schema_types(node)]
    if len(faqs) != 1:
        return problems + [f"expected exactly one FAQPage node, found {len(faqs)}"]
    faq = faqs[0]
    if faq.get("url") != SITE + relative:
        problems.append(f"FAQPage url {faq.get('url')!r} is not the page URL")
    schema_pairs = []
    for question in faq.get("mainEntity", []):
        answer = question.get("acceptedAnswer", {})
        if "Question" not in schema_types(question) or "Answer" not in schema_types(answer):
            problems.append(f"malformed Question node {question.get('name')!r}")
        schema_pairs.append((normalized(question.get("name", "")), normalized(answer.get("text", ""))))
    visible_questions = [name for name, _ in parsed.pairs]
    if visible_questions != questions:
        problems.append(f"visible questions differ from the contract: {visible_questions}")
    if schema_pairs != parsed.pairs:
        problems.append("JSON-LD questions and answers differ from the visible cards")
    if len(set(visible_questions)) != len(visible_questions):
        problems.append("duplicate visible questions")
    for name, answer in parsed.pairs:
        if not answer.startswith("GoHireHumans") and "GoHireHumans" not in answer.split(".")[0]:
            problems.append(f"answer to {name!r} does not name GoHireHumans in its first sentence")
        problems += [f"{name!r}: {p}" for p in claim_problems(answer)]
    return problems


FAQ_PAGES = {
    "post-a-small-task.html": [
        "Where can I hire someone online for a small one-off task?",
        "Can I pay someone to make a phone call for me?",
        "Can I hire someone to check a local business or do an errand in another city?",
        "Can I hire someone to clean up a spreadsheet?",
        "Can I hire a human to test my website for a few hours?",
        "How are platform fees structured for workers and employers?",
    ],
    "use-cases/hire-human-to-review-ai-output.html": [
        "Where can I hire someone to fact-check AI-generated content?",
        "What does a human review of AI output include?",
    ],
    "use-cases/ai-agent-human-in-the-loop-tasks.html": [
        "How can my AI agent hire a human?",
        "Is there an MCP server to hire humans for real-world tasks?",
        "How does an AI agent get proof that a human task is complete?",
    ],
}


def page_text(relative):
    return (FRONTEND / relative).read_text(encoding="utf-8")


def mutate(text, old, new, count):
    found = text.count(old)
    if found != count:
        raise AssertionError(f"mutation anchor {old[:60]!r} found {found} times, expected {count}")
    return text.replace(old, new)


class AIAnswerReadinessTests(unittest.TestCase):
    def test_llms_summary_has_buyer_answers_and_truthful_hiring_limits(self):
        text = page_text("llms.txt")
        for section in ("## Buyer Questions and Answer Pages", "## Human and Agent Hiring", "## Pricing and Limits"):
            self.assertIn(section, text)
        for phrase in ("small, scoped human help", "account-owner approval", "Workers receive the listed payout", "1% platform fee plus a fixed 3% processing charge where checkout is configured", "payout setup", "gohirehumans.operations@agentmail.to"):
            self.assertIn(phrase, text)
        for path in ("post-a-small-task.html", "use-cases/hire-human-to-review-ai-output.html", "use-cases/website-qa-task.html", "use-cases/ai-agent-human-in-the-loop-tasks.html", "use-cases/pay-someone-to-make-phone-calls.html", "use-cases/local-verification-tasks.html", "api-docs.html", "agent-onboarding.html", "pricing.html"):
            self.assertIn(SITE + path, text)
        self.assertEqual(claim_problems(text), [])

    def test_llms_links_resolve_to_real_public_source_files(self):
        urls = re.findall(r"https://[^\s)]+", page_text("llms.txt"))
        self.assertGreaterEqual(len(urls), 15)
        for url in urls:
            with self.subTest(url=url):
                parsed = urlsplit(url)
                self.assertEqual(parsed.netloc, "www.gohirehumans.com")
                rel = parsed.path.lstrip("/")
                if not rel or rel.endswith("/"):
                    rel += "index.html"
                self.assertTrue((FRONTEND / rel).is_file(), rel)

    def test_faq_pages_match_static_visible_answers(self):
        for relative, questions in FAQ_PAGES.items():
            with self.subTest(page=relative):
                self.assertEqual(faq_contract_problems(relative, questions, page_text(relative)), [])

    def test_ai_crawlers_remain_allowed_with_sitemap(self):
        text = page_text("robots.txt")
        parser = robotparser.RobotFileParser()
        parser.parse(text.splitlines())
        self.assertIn("Sitemap: https://www.gohirehumans.com/sitemap.xml", text)
        self.assertIn("User-agent: *\nAllow: /", text)
        for agent in ("GPTBot", "ChatGPT-User", "OAI-SearchBot", "ClaudeBot", "Claude-SearchBot", "Claude-User", "PerplexityBot", "Perplexity-User", "meta-externalagent", "Google-Extended"):
            for path in ("/llms.txt", "/post-a-small-task.html", "/use-cases/hire-human-to-review-ai-output.html", "/use-cases/ai-agent-human-in-the-loop-tasks.html", "/api-docs.html"):
                with self.subTest(agent=agent, path=path):
                    self.assertTrue(parser.can_fetch(agent, "https://www.gohirehumans.com" + path))

    def test_edited_answer_pages_have_matching_modification_dates(self):
        urls = ET.parse(FRONTEND / "sitemap.xml").getroot().findall("{*}url")
        for relative in ("llms.txt", *FAQ_PAGES):
            with self.subTest(page=relative):
                matches = [node for node in urls if node.findtext("{*}loc") == SITE + relative]
                self.assertEqual(len(matches), 1)
                self.assertEqual(matches[0].findtext("{*}lastmod"), "2026-10-13")
                if relative.endswith(".html"):
                    parsed = AnswerHTML(page_text(relative))
                    pages = [node for node in parsed.schemas if "WebPage" in schema_types(node)]
                    self.assertEqual(len(pages), 1)
                    self.assertEqual(pages[0]["dateModified"], "2026-10-13")


class AIAnswerContractGuardTests(unittest.TestCase):
    """Each known way the answer cards can drift must make the contract report a problem."""

    SMALL = "post-a-small-task.html"
    AGENT = "use-cases/ai-agent-human-in-the-loop-tasks.html"

    def problems_after(self, relative, old, new, count):
        return faq_contract_problems(relative, FAQ_PAGES[relative], mutate(page_text(relative), old, new, count))

    def assert_caught(self, problems, fragment):
        self.assertTrue(any(fragment in p for p in problems), problems)

    def test_json_ld_answer_drift_is_caught(self):
        problems = self.problems_after(self.SMALL, "Review a small sample before agreeing the full scope.\"", "Review a sample first.\"", 1)
        self.assert_caught(problems, "differ from the visible cards")

    def test_second_faqpage_nested_in_graph_is_caught(self):
        extra = '<script type="application/ld+json">{"@context": "https://schema.org", "@graph": [{"@type": ["FAQPage"], "mainEntity": []}]}</script>\n</head>'
        self.assert_caught(self.problems_after(self.AGENT, "</head>", extra, 1), "exactly one FAQPage node, found 2")

    def test_cards_hidden_by_inline_visibility_are_caught(self):
        problems = self.problems_after(self.AGENT, "<article class=\"card\" data-ai-answer>", "<article class=\"card\" data-ai-answer style=\"visibility: hidden\">", 3)
        self.assert_caught(problems, "inline style visibility:hidden")

    def test_cards_hidden_by_screen_reader_only_container_are_caught(self):
        problems = self.problems_after(self.SMALL, '<div class="stack">\n<article class="card" data-ai-answer>', '<div class="stack sr-only">\n<article class="card" data-ai-answer>', 1)
        self.assert_caught(problems, "hiding class sr-only")

    def test_unbalanced_faq_markup_is_caught(self):
        problems = self.problems_after(self.AGENT, "</article>\n</div>\n<p class=\"mt-4\">Read the", "</article>\n<p class=\"mt-4\">Read the", 1)
        self.assert_caught(problems, "mismatched")

    def test_worker_screening_claim_in_both_copies_is_caught(self):
        old = "GoHireHumans is a marketplace where you can post a small, one-off task"
        problems = self.problems_after(self.SMALL, old, "GoHireHumans provides verified workers. " + old, 2)
        self.assertNotIn("JSON-LD questions and answers differ from the visible cards", problems)
        self.assert_caught(problems, "unsupported screening claim 'verified workers'")

    def test_payout_verification_wording_stays_allowed(self):
        self.assertEqual(claim_problems("Workers must finish Stripe payout setup and identity verification before applying."), [])


if __name__ == "__main__":
    unittest.main()
