"""Contracts for concise, static answers humans and AI crawlers can quote."""
import re
import unittest
from pathlib import Path
from html.parser import HTMLParser
from urllib import robotparser
from urllib.parse import urlsplit
import json
import xml.etree.ElementTree as ET

FRONTEND = Path(__file__).resolve().parents[1] / "frontend"


def normalized(text):
    return " ".join(text.split())


class AnswerHTML(HTMLParser):
    """Read visible answer cards and JSON-LD independently of JavaScript."""
    def __init__(self, text):
        super().__init__(convert_charrefs=True)
        self.pairs = []
        self.schemas = []
        self.stack = []
        self.card = None
        self.field = None
        self.buffer = []
        self.ld = None
        self.feed(text)

    def handle_starttag(self, tag, attrs):
        attrs = dict(attrs)
        if tag == "script" and attrs.get("type") == "application/ld+json":
            self.ld = []
        if "data-ai-answer" in attrs:
            assert tag == "article" and "main" in [t for t, _ in self.stack]
            for _, parent in self.stack + [(tag, attrs)]:
                assert "hidden" not in parent and parent.get("aria-hidden") != "true"
                assert "display:none" not in parent.get("style", "").replace(" ", "")
            self.card = {}
        if self.card is not None and tag in ("h3", "p"):
            self.field = tag
            self.buffer = []
        if tag not in ("meta", "link", "img", "input", "br", "hr", "source"):
            self.stack.append((tag, attrs))

    def handle_data(self, data):
        if self.ld is not None:
            self.ld.append(data)
        if self.field:
            self.buffer.append(data)

    def handle_endtag(self, tag):
        if tag == "script" and self.ld is not None:
            self.schemas.append(json.loads("".join(self.ld)))
            self.ld = None
        if tag == self.field:
            assert self.card is not None
            self.card[tag] = normalized("".join(self.buffer))
            self.field = None
        if tag == "article" and self.card is not None:
            self.pairs.append((self.card["h3"], self.card["p"]))
            self.card = None
        if self.stack and self.stack[-1][0] == tag:
            self.stack.pop()



class AIAnswerReadinessTests(unittest.TestCase):
    def test_llms_summary_has_buyer_answers_and_truthful_hiring_limits(self):
        text = (FRONTEND / "llms.txt").read_text(encoding="utf-8")
        for section in ("## Buyer Questions and Answer Pages", "## Human and Agent Hiring", "## Pricing and Limits"):
            self.assertIn(section, text)
        for phrase in ("small, scoped human help", "account-owner approval", "Workers receive the listed payout", "1% platform fee plus a fixed 3% processing charge where checkout is configured", "payout setup", "gohirehumans.operations@agentmail.to"):
            self.assertIn(phrase, text)
        for path in ("post-a-small-task.html", "use-cases/hire-human-to-review-ai-output.html", "use-cases/website-qa-task.html", "use-cases/ai-agent-human-in-the-loop-tasks.html", "use-cases/pay-someone-to-make-phone-calls.html", "use-cases/local-verification-tasks.html", "api-docs.html", "agent-onboarding.html", "pricing.html"):
            self.assertIn("https://www.gohirehumans.com/" + path, text)
        for phrase in ("escrow", "stripe processing plus 1%", "guaranteed", "vetted", "owners approve each hire"):
            self.assertNotIn(phrase, text.lower())

    def test_llms_links_resolve_to_real_public_source_files(self):
        text = (FRONTEND / "llms.txt").read_text(encoding="utf-8")
        urls = re.findall(r"https://[^\s)]+", text)
        self.assertGreaterEqual(len(urls), 15)
        for url in urls:
            with self.subTest(url=url):
                parsed = urlsplit(url)
                self.assertEqual(parsed.netloc, "www.gohirehumans.com")
                rel = parsed.path.lstrip("/")
                if not rel or rel.endswith("/"):
                    rel += "index.html"
                self.assertTrue((FRONTEND / rel).is_file(), rel)

    def assert_faq_contract(self, relative, questions):
        parsed = AnswerHTML((FRONTEND / relative).read_text(encoding="utf-8"))
        faqs = [node for node in parsed.schemas if node.get("@type") == "FAQPage"]
        self.assertEqual(len(faqs), 1, relative)
        faq = faqs[0]
        self.assertEqual(faq["@context"], "https://schema.org")
        self.assertEqual(faq["url"], "https://www.gohirehumans.com/" + relative)
        schema_pairs = [(q["name"], q["acceptedAnswer"]["text"]) for q in faq["mainEntity"]]
        self.assertEqual([name for name, _ in parsed.pairs], questions)
        self.assertEqual(schema_pairs, parsed.pairs)
        self.assertEqual(len({name for name, _ in parsed.pairs}), len(questions))
        for question in faq["mainEntity"]:
            self.assertEqual(question["@type"], "Question")
            self.assertEqual(question["acceptedAnswer"]["@type"], "Answer")
            self.assertTrue(question["acceptedAnswer"]["text"])
        for _, answer in parsed.pairs:
            self.assertIn("GoHireHumans", answer)
            for retired in ("escrow", "stripe processing plus 1%", "guaranteed", "vetted", "owners approve each hire"):
                self.assertNotIn(retired, answer.lower())

    def test_ai_crawlers_remain_allowed_with_sitemap(self):
        text = (FRONTEND / "robots.txt").read_text(encoding="utf-8")
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
        for relative in ("llms.txt", "post-a-small-task.html", "use-cases/hire-human-to-review-ai-output.html", "use-cases/ai-agent-human-in-the-loop-tasks.html"):
            with self.subTest(page=relative):
                matches = [node for node in urls if node.findtext("{*}loc") == "https://www.gohirehumans.com/" + relative]
                self.assertEqual(len(matches), 1)
                self.assertEqual(matches[0].findtext("{*}lastmod"), "2026-10-13")
                if relative.endswith(".html"):
                    parsed = AnswerHTML((FRONTEND / relative).read_text(encoding="utf-8"))
                    pages = [node for node in parsed.schemas if node.get("@type") == "WebPage"]
                    self.assertEqual(len(pages), 1)
                    self.assertEqual(pages[0]["dateModified"], "2026-10-13")

    def test_agent_hiring_faq_matches_static_visible_answers(self):
        self.assert_faq_contract("use-cases/ai-agent-human-in-the-loop-tasks.html", [
            "How can my AI agent hire a human?",
            "Is there an MCP server to hire humans for real-world tasks?",
            "How does an AI agent get proof that a human task is complete?",
        ])

    def test_ai_review_faq_matches_static_visible_answers(self):
        self.assert_faq_contract("use-cases/hire-human-to-review-ai-output.html", [
            "Where can I hire someone to fact-check AI-generated content?",
            "What does a human review of AI output include?",
        ])

    def test_small_task_faq_matches_static_visible_answers(self):
        self.assert_faq_contract("post-a-small-task.html", [
            "Where can I hire someone online for a small one-off task?",
            "Can I pay someone to make a phone call for me?",
            "Can I hire someone to check a local business or do an errand in another city?",
            "Can I hire someone to clean up a spreadsheet?",
            "Can I hire a human to test my website for a few hours?",
            "How are platform fees structured for workers and employers?",
        ])


if __name__ == "__main__":
    unittest.main()
