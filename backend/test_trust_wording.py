"""Public trust copy must describe profile evidence, not worker verification.

This is a conservative wording guard, not a semantic fact checker. Exceptions
are clause-local: a competitor name or Stripe mention cannot excuse a GHH claim.
"""
import html
from html.parser import HTMLParser
import json
from pathlib import Path
import re
import unittest

ROOT = Path(__file__).resolve().parents[1]
FRONTEND = ROOT / "frontend"
EXCLUDED = {"node_modules", "tests", "test-results", "playwright-report", "dist", "build"}
COMPETITOR = r"(?:Fiverr|Upwork|Freelancer(?:\.com)?|Toptal|Guru|TaskRabbit|99designs|Turing|Lemon\.io|Arc|BELAY)"
PATTERNS = (
    r"\b(?:GoHireHumans|GHH|we|our platform|Stripe)\s+(?:\w+\s+){0,2}(?:verif\w*|vet\w*|screen\w*|background[- ]check\w*)\s+(?:(?:every|all|each|its|our|the|human)\s+)*(?:worker|freelancer|provider|seller)s?\b",
    r"\b(?:worker\s+)?profiles?\s+(?:may\s+|can\s+)?(?:display|show|include|have)s?\b[^.!?;\n]{0,100}\b(?:identity|verification|screening|business verification|verified badges?)\b",
    r"\b(?:verified|vetted|screened|background[- ]checked|identity[- ]verified)\s+(?:(?:human|freelance|agent)\s+)?(?:workers?|freelancers?|providers?|sellers?|profiles?|account holders?|operators?|person|company)\b",
    r"\b(?:workers?|freelancers?|providers?|sellers?|profiles?)\s+(?:are|is|have been)\s+(?:verified|vetted|screened|background[- ]checked|identity[- ]verified)\b",
    r"\b(?:identity[- ]profile trust signals|identity or business verification where shown|verified freelancer marketplace|available verification details)\b",
    r"\b(?:intelligent matching|AI matching|continuous performance monitoring|meet every criterion|meets every criterion)\b",
    r"\b(?:automated screening with human review for flagged content|human moderation review for flagged content|human review for flagged content|automated systems and human moderators)\b",
    r"\b(?:full REST API supporting task creation, monitoring, webhooks|outbound (?:webhooks|callbacks) for task progress)\b",
    r"\bStripe\s+verifies?\s+(?:identity|bank details)[^.!?;\n]*",
    r"\b(?:optional identity verification|identity verification is optional|no identity verification requirement|without required skill assessments or identity verification)\b",
    # Second review: moderation and ID-check claims, coordinated claims after a disclaimer,
    # and unsupported superiority or scale claims about GHH.
    r"\b(?:moderators?|our team|we|GoHireHumans|GHH)\s+(?:\w+\s+){0,2}(?:review|check|approve|inspect)s?\s+(?:every|all|each)\s+(?:listing|task|job|post|service|worker|freelancer)s?\b",
    r"\b(?:check|verif\w*|require)s?\s+(?:government[- ]issued\s+|photo\s+)?(?:IDs?|identification)\s+(?:for|of|from)\s+(?:every|all|each)\s+(?:workers?|freelancers?|providers?|sellers?)\b",
    r"\b(?:but|and|also)\s+(?:also\s+)?(?:verif\w*|vet\w*|screen\w*|background[- ]check\w*)\s+(?:every|all|each|its|our)\s+(?:workers?|freelancers?|providers?|sellers?)\b",
    r"\b(?:safer|safest)(?:,?\s+faster)?\s+(?:alternative|option)\b",
    r"\b(?:GoHireHumans|GHH)\s+(?:is|offers|as)\s+(?:the\s+|a\s+)?(?:safer|safest|fastest|leading|#1|best)\b",
    r"\b(?:manages|handles)\s+(?:the\s+)?worker[- ]pool\s+scaling\b",
)
COMPILED = tuple(re.compile(pattern, re.I) for pattern in PATTERNS)


class PublicText(HTMLParser):
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.parts = []

    def handle_starttag(self, tag, attrs):
        if tag in {"p", "li", "td", "th", "div", "h1", "h2", "h3", "script"}:
            self.parts.append("\n")
        if tag == "meta":
            self.parts.append("\n" + (dict(attrs).get("content") or "") + "\n")

    def handle_endtag(self, tag):
        if tag in {"p", "li", "td", "th", "div", "h1", "h2", "h3", "script"}:
            self.parts.append("\n")

    def handle_data(self, data):
        self.parts.append(re.sub(r"\s+", " ", data))


def public_text(source):
    # JSON escapes and HTML entities must not conceal a copied claim.
    source = re.sub(r"\\u([0-9a-fA-F]{4})", lambda m: chr(int(m[1], 16)), source)
    parser = PublicText()
    parser.feed(source)
    return html.unescape("".join(parser.parts))


def claim_problems(source):
    text = public_text(source)
    problems = []
    for pattern in COMPILED:
        for match in pattern.finditer(text):
            # Use only the preceding clause, not unrelated disclaimers elsewhere.
            prefix = re.split(r"[.!?;\n]", text[:match.start()])[-1]
            claim = match.group(0)
            clause = prefix + claim
            if re.search(r"\b(?:does not|do not|not|never|no)\s+(?:(?:claim|show|display|offer|provide|a|any)\s+){0,3}$", prefix, re.I):
                continue
            if re.search(r"\b(?:does not|do not|never)\b", claim, re.I):
                continue
            # Only the exact processor/payout fact is exempt. A Stripe mention
            # must never excuse screening or a claim that GHH checks identity.
            if re.fullmatch(r"Stripe verifies identity and bank details for payouts(?:, not worker quality)?", claim.strip(), re.I):
                continue
            # Attribute only this match to a named competitor; never exempt a
            # clause that also asserts something about GHH or its profiles.
            if (re.match(r"\s*" + COMPETITOR + r"\b", clause, re.I)
                    and not re.search(r"GoHireHumans|\bGHH\b|\bwe\b|\bour\b", clause, re.I)
                    and not re.search(r"optional identity verification|identity verification is optional|no identity verification requirement|without required", claim, re.I)):
                continue
            problems.append(claim)
    return problems



def faq_contract_problems(source):
    blocks = re.findall(r'<script type="application/ld\+json">\s*(.*?)\s*</script>', source, re.S)
    faqs = [json.loads(block) for block in blocks if json.loads(block).get("@type") == "FAQPage"]
    problems = []
    for faq in faqs:
        for entity in faq["mainEntity"]:
            question = entity["name"]
            heading = re.search(r"<h3[^>]*>" + re.escape(question) + r"</h3>\s*<p[^>]*>(.*?)</p>", source, re.S)
            detail = re.search(r"<summary>" + re.escape(question) + r"</summary>\s*<div>(.*?)</div>", source, re.S)
            if not (heading or detail):
                problems.append(question + ": missing visible answer")
                continue
            visible = public_text((heading or detail)[1])
            visible = " ".join(visible.split())
            expected = " ".join(entity["acceptedAnswer"]["text"].split())
            if visible != expected:
                problems.append(question + ": visible and JSON-LD answer differ")
    if not faqs:
        problems.append("missing FAQPage")
    return problems


def public_files():
    return sorted(path for path in FRONTEND.rglob("*")
                  if path.is_file() and path.suffix.lower() in {".html", ".json", ".txt"}
                  and not (EXCLUDED & set(path.relative_to(FRONTEND).parts)))


class TrustWordingTests(unittest.TestCase):
    def test_public_tree_has_no_unsupported_claims(self):
        paths = public_files()
        self.assertGreater(len(paths), 100)
        failures = {str(path.relative_to(ROOT)): claim_problems(path.read_text(encoding="utf-8"))
                    for path in paths}
        self.assertEqual({path: claims for path, claims in failures.items() if claims}, {})

    def test_mcp_resources_use_the_same_truthful_profile_evidence(self):
        canonical = (ROOT / "backend/mcp_server.py").read_text()
        self.assertEqual(claim_problems(canonical), [])
        self.assertIn("Profiles show bios, skills, work samples, ratings, reviews, and completed-order counts where available.", canonical)
        self.assertEqual(canonical, (ROOT / "backend/mcp-package/mcp_server.py").read_text())

    def test_edited_faq_answers_match_json_ld(self):
        for relative in ("faq.html", "blog/hire-human-for-ai-tasks.html",
                         "blog/on-demand-workforce-platform.html", "blog/verified-freelancer-marketplace.html"):
            with self.subTest(page=relative):
                self.assertEqual(faq_contract_problems((FRONTEND / relative).read_text()), [])

    def test_faq_answer_drift_mutation_is_caught(self):
        source = (FRONTEND / "blog/verified-freelancer-marketplace.html").read_text()
        old = '<p>GoHireHumans does not claim to be safer than another marketplace.'
        self.assertEqual(source.count(old), 1)
        mutated = source.replace(old, '<p>GoHireHumans verifies every worker. ', 1)
        self.assertTrue(faq_contract_problems(mutated))
        self.assertTrue(claim_problems(mutated))

    def test_affirmative_mutations_are_caught(self):
        mutations = (
            "GoHireHumans verifies every worker",
            "GHH screens all workers",
            "We vet every freelancer",
            "Profiles show verification status",
            "Add any available verification details to your profile",
            "Workers are verified",
            "Profiles have verified badges",
            "Worker profiles may display identity signals",
            "Profiles show\n verification status",
            "Worker profiles may display ratings and identity signals",
            "Human review for flagged content",
            "Worker profiles may display identity, skill, review, and history signals where available.",
            "Our verified workers are ready to hire.",
            "GoHireHumans uses intelligent matching.",
            "Continuous performance monitoring",
            "GoHireHumans was built to meet every criterion.",
            "Full REST API supporting task creation, monitoring, webhooks, and result retrieval",
            "Task postings are screened by automated systems and human moderators.",
            "Upwork's identity verification is optional.",
            "Fiverr: No identity verification requirement",
            "Our moderators review every listing",
            "We check IDs for all workers",
            "GoHireHumans does not guarantee results but verifies every worker.",
            "Try the safer, faster alternative",
            "For the safest option with payment processing review, GoHireHumans offers free listing.",
            "GoHireHumans is the leading HaaS platform.",
            "The platform manages worker pool scaling transparently.",
        )
        for mutation in mutations:
            with self.subTest(mutation=mutation):
                self.assertTrue(claim_problems(mutation), mutation)

    def test_inline_markup_and_json_mutations_are_caught(self):
        for mutation in (
            "<p>Worker <strong>profiles</strong> may display <em>identity</em> signals.</p>",
            json.dumps({"acceptedAnswer": {"text": "GoHireHumans verifies every worker"}}),
            '<meta name="description" content="Profiles show verification status">',
            '{"text": "GoHireHumans verif\\u0069es every worker"}',
        ):
            with self.subTest(mutation=mutation):
                self.assertTrue(claim_problems(mutation))

    def test_negated_statements_are_allowed(self):
        for statement in (
            "GoHireHumans does not vet, screen, background-check, or identity-verify workers.",
            "GoHireHumans does not verify workers.",
            "Profiles do not show verification status.",
            "GoHireHumans is not a verified freelancer marketplace.",
            "We do not offer verified workers.",
        ):
            with self.subTest(statement=statement):
                self.assertEqual(claim_problems(statement), [])

    def test_payout_scoped_stripe_facts_are_allowed(self):
        for statement in (
            "Stripe verifies identity and bank details for payouts.",
            "Workers must complete Stripe payout setup before applying to jobs.",
            "AI listings remain hidden until Stripe payout verification is complete.",
        ):
            self.assertEqual(claim_problems(statement), [])

    def test_stripe_and_disclaimers_do_not_excuse_worker_screening(self):
        for statement in (
            "GoHireHumans verifies every worker via Stripe.",
            "We screen all workers via Stripe for payouts.",
            "Stripe verifies identity and bank details.",
            "Stripe verifies identity and bank details for payouts and vets workers.",
            "GoHireHumans does not guarantee results. Profiles show verification status.",
            "Stripe verifies identity for payouts. GoHireHumans vets all workers.",
            "No guarantees; GoHireHumans verifies every worker.",
        ):
            with self.subTest(statement=statement):
                self.assertTrue(claim_problems(statement))

    def test_attributed_competitor_facts_are_allowed(self):
        for statement in (
            "Toptal says it screens applicants through interviews and test projects.",
            "Fiverr may require identity verification before new freelancers' Gigs are listed.",
            "Upwork says clients can see identity verification status, not ID documents.",
            "TaskRabbit says US Taskers undergo background checks.",
        ):
            self.assertEqual(claim_problems(statement), [])

    def test_competitor_name_does_not_excuse_ghh_claim(self):
        self.assertTrue(claim_problems("Unlike Fiverr, GoHireHumans verifies every worker."))
        self.assertTrue(claim_problems("Toptal screens applicants; Profiles show verification status."))
        self.assertTrue(claim_problems("Unlike Fiverr, profiles show identity signals."))


if __name__ == "__main__":
    unittest.main()
