"""Public trust copy must describe profile evidence, not worker verification.

This is a conservative wording guard, not a semantic fact checker. Exceptions
are clause-local: a competitor name or Stripe mention cannot excuse a GHH claim.
"""
import ast
import bisect
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
    # Third review: passive and coordinated forms, payment timing, capacity and talent-equivalence claims.
    r"\b(?:every|each|all)\s+(?:listing|task|job|post|service)s?\s+(?:is|are)\s+(?:\w+\s+){0,2}reviewed\s+by\s+(?:our\s+|a\s+|human\s+)*(?:moderators?|team|staff)\b",
    r"\b(?:every|each|all)\s+(?:workers?|freelancers?|providers?|sellers?)\s+(?:has|have|is|are)\s+(?:their\s+)?(?:IDs?|identit\w+|identification)\s+(?:checked|verified)\b",
    r"\b(?:does|do)\s+(?:\w+\s+){0,3}verify\s+(?:every|all|each|its|our)\s+(?:workers?|freelancers?|providers?|sellers?)\b",
    r"\byou\s+(?:only\s+)?pay\s+(?:only\s+)?for\s+completed(?:,\s*approved)?\s+work\b",
    r"\b(?:reviews?|approves?)\s+the\s+work\s+before\s+paying\b",
    r"\benterprise[- ]scale\s+operations\b|\b(?:running|processes|processing|handles?|handling)\s+thousands\s+of\s+(?:human\s+)?tasks\s+per\s+month\b[^.!?\n]{0,160}\b(?:GoHireHumans|GHH)\b",
    r"\b(?:hire|get|deliver\w*|connect\w*\s+you\s+with)\s+(?:\w+\s+){0,2}(?:elite|top[- ]tier|Toptal[- ]quality)\s+(?:talent|developers?|freelancers?)\b|\b(?:find|access)\s+(?:\w+\s+){0,2}(?:elite|top[- ]tier|Toptal[- ]quality)\s+(?:talent|developers?|freelancers?)\b(?![^.!?\n]*\?)",
    r"\bonly\s+platform\s+with\b",
    # Payout timing: the employer is charged at hire/order; the worker is paid after approval.
    r"\b(?:the\s+)?(?:workers?|freelancers?|providers?|sellers?)\s+(?:receives?|gets?\s+paid|gets?|(?:is|are)\s+paid(?:\s+out)?)\b(?:\s+(?:the|their|its|a|an|listed|full|payout|payment|money|immediately|instantly|right\s+away|automatically))*\s*(?:when|as\s+soon\s+as|once|upon|at|on)\s+(?:(?:you|the\s+employer|employers?|the\s+client|clients?)\s+(?:hires?|orders?|books?|accepts?)\b|(?:the\s+)?(?:hire|hiring|order|booking|checkout)\b)",
    r"\b(?:payouts?|payments?)\s+(?:to\s+(?:the\s+)?workers?\s+)?(?:is|are)\s+(?:released|sent|made)\s+(?:immediately\s+)?(?:when|as\s+soon\s+as|once|upon|at)\s+(?:you\s+hire|hire|hiring|the\s+order|checkout)\b",
    # Payout authority: the employer can approve with release_payment or mark a service order
    # complete, and a site admin can also complete it. Every exclusive "only when ..." payout
    # claim has missed one of these paths, so any exclusive payout-trigger claim is rejected.
    r"\b(?:(?:is|are|gets?)\s+paid(?:\s+out)?|receives?\s+(?:the\s+|their\s+)?(?:listed\s+)?(?:payout|payment|funds|money)|(?:is|are)\s+(?:released|sent))\b[^.!?;\n]{0,40}\bonly\s+(?:(?:when|after|once|if)\s+(?:you|the\s+employer|employers?|the\s+buyer|buyers?|the\s+client|clients?)\s+(?:\w+\s+){0,2}(?:approves?|releases?|accepts?|signs?\s+off|completes?)\b|(?:after|upon|on)\s+(?:your\s+|the\s+employer'?s?\s+|employer\s+)?approval\b)",
    r"\bonly\s+(?:you|the\s+employer|employers|the\s+buyer|buyers|the\s+client|clients)\s+(?:can|may|will)\s+(?:\w+\s+){0,2}(?:release|trigger|approve)\s+(?:the\s+|a\s+|their\s+)?(?:worker'?s?\s+)?(?:payouts?|payments?|funds)\b",
    # Fees: the processing charge is a fixed 3%, never an unspecified "Stripe" fee.
    r"\b(?:1%|one\s+percent)(?:\s+(?:platform|service))?(?:\s+fee)?\s*(?:\+|plus|and)\s*(?:a\s+|the\s+)?Stripe(?:\s+(?:processing|card|payment))?(?:\s+fees?)?\b",
    r"\bStripe(?:\s+(?:processing|card|payment))?(?:\s+fees?)?\s*(?:\+|plus|and)\s*(?:a\s+|the\s+)?(?:1%|one\s+percent\b)",
    r"\$\d+(?:\.\d+)?\s*(?:\+|plus)\s*Stripe\b",
    # Fifth review: rewordings of hire-time payout, processing-cost synonyms, charge-after-completion
    # and instant-payout claims. In GHH the card is charged at hire/order; payout follows approval
    # or order completion and is never instant on hire.
    r"\b(?:workers?|freelancers?|providers?|sellers?)\s+(?:receives?|gets?|(?:is|are)\s+paid)\s+(?:(?:the|their|its|listed|full|payout|payment|money)\s+)*(?:immediately|instantly|right\s+away|automatically)\s+(?:after|once|when|upon|as\s+soon\s+as)\s+(?:you|the\s+employer|employers?|the\s+client|clients?)\s+(?:hires?|orders?|books?)\b",
    r"\b(?:payouts?|payments?|funds?)\s+(?:is|are)\s+released\s+to\s+(?:the\s+)?(?:workers?|freelancers?|providers?|sellers?)\s+(?:immediately\s+)?(?:when|once|as\s+soon\s+as|upon|at)\s+(?:you\s+hire|hire|hiring|the\s+order|checkout)\b",
    r"\b(?:when|once|as\s+soon\s+as)\s+(?:you|the\s+employer|employers?|the\s+client|clients?)\s+(?:hires?|orders?|books?)\b[^.!?;\n]{0,20},\s*(?:the\s+)?(?:workers?|freelancers?|providers?|sellers?)\s+(?:(?:receives?|gets?)\s+(?:(?:the|their|its|a|an|listed|full)\s+)*(?:payouts?|payments?|money|funds)|gets?\s+paid|(?:is|are)\s+paid)\b",
    r"\b(?:employers?|buyers?|clients?|you)\s+(?:pays?|covers?)\s+(?:the\s+)?Stripe\s+(?:(?:processing|card|payment|transaction)\s+)?(?:costs?|fees?|charges?)\b",
    r"\b(?:your|the\s+employer'?s?|the\s+buyer'?s?)\s+(?:card|payment\s+method)\s+is\s+(?:only\s+)?charged\s+only\s+(?:after|once|when)\b|\b(?:your|the\s+employer'?s?|the\s+buyer'?s?)\s+(?:card|payment\s+method)\s+is\s+charged\s+(?:only\s+)?(?:after|once|when)\s+(?:the\s+)?(?:task|work|job|order)\s+is\s+(?:completed|done|approved|finished)\b",
    r"\binstant(?:ly)?\s+payouts?\b(?!\s+(?:(?:(?:are|is)\s+(?:not|never)|aren't|isn't)\s+(?:(?:currently|yet)\s+)?(?:available|offered|supported|provided)|(?:are|is)\s+(?:currently\s+)?unavailable)(?:\s+(?:on|in|at)\s+(?:GoHireHumans|GHH|the\s+platform|this\s+site))?(?:\s+(?:yet|currently|today|at\s+this\s+time|right\s+now))?\s*(?:[.;:!?)\n]|$|,(?!\s*(?:except|unless|other\s+than|apart\s+from|save)\b)(?![^.;!?\n]{0,80}\b(?:gets?|receives?|has|have|can\s+(?:use|get|request))\s+(?:them|it|instant)\b)))|\bpay(?:s|ing)?\s+out\s+instantly\b",
    # GHH has no listing-report feature: disputes are for order participants, and listing or
    # user reports go to the operations inbox.
    r"\b(?:suspicious\s+)?listing\s+reporting\b(?!\s+(?:(?:(?:are|is)\s+(?:not|never)|aren't|isn't)\s+(?:(?:currently|yet)\s+)?(?:available|offered|supported|provided)|(?:are|is)\s+(?:currently\s+)?unavailable)(?:\s+(?:on|in|at)\s+(?:GoHireHumans|GHH|the\s+platform|this\s+site))?(?:\s+(?:yet|currently|today|at\s+this\s+time|right\s+now))?\s*(?:[.;:!?)\n]|$|,(?!\s*(?:except|unless|other\s+than|apart\s+from|save)\b)(?![^.;!?\n]{0,80}\b(?:gets?|receives?|has|have|can\s+(?:use|get|request))\s+(?:them|it|instant)\b)))|\breporting\s+(?:systems?|tools?|features?)\b(?!\s+(?:(?:(?:are|is)\s+(?:not|never)|aren't|isn't)\s+(?:(?:currently|yet)\s+)?(?:available|offered|supported|provided)|(?:are|is)\s+(?:currently\s+)?unavailable)(?:\s+(?:on|in|at)\s+(?:GoHireHumans|GHH|the\s+platform|this\s+site))?(?:\s+(?:yet|currently|today|at\s+this\s+time|right\s+now))?\s*(?:[.;:!?)\n]|$|,(?!\s*(?:except|unless|other\s+than|apart\s+from|save)\b)(?![^.;!?\n]{0,80}\b(?:gets?|receives?|has|have|can\s+(?:use|get|request))\s+(?:them|it|instant)\b)))|\breport\s+(?:this|a)\s+(?:listing|profile|user)\s+(?:button|link|feature)\b(?!\s+(?:(?:(?:are|is)\s+(?:not|never)|aren't|isn't)\s+(?:(?:currently|yet)\s+)?(?:available|offered|supported|provided)|(?:are|is)\s+(?:currently\s+)?unavailable)(?:\s+(?:on|in|at)\s+(?:GoHireHumans|GHH|the\s+platform|this\s+site))?(?:\s+(?:yet|currently|today|at\s+this\s+time|right\s+now))?\s*(?:[.;:!?)\n]|$|,(?!\s*(?:except|unless|other\s+than|apart\s+from|save)\b)(?![^.;!?\n]{0,80}\b(?:gets?|receives?|has|have|can\s+(?:use|get|request))\s+(?:them|it|instant)\b)))",
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


NEGATED_PREFIX = re.compile(r"\b(?:does not|do not|not|never|no)\s+(?:(?:claim|show|display|offer|provide|a|any)\s+){0,3}$", re.I)
GHH_MENTION = re.compile(r"GoHireHumans|\bGHH\b|\bwe\b|\bour\b", re.I)
COMPETITOR_AT = re.compile(r"\s*" + COMPETITOR + r"\b", re.I)
NEGATION_WINDOW = 160


def claim_problems(source):
    text = public_text(source)
    # Clause boundaries and GHH mentions are indexed once; each match then does bounded work, so
    # long clauses or many repeated claims never rescan text (linear in the page size).
    boundaries = [m.end() for m in re.finditer(r"[.!?;\n]", text)]
    ghh_mentions = [m.start() for m in GHH_MENTION.finditer(text)]
    problems = []
    for pattern in COMPILED:
        for match in pattern.finditer(text):
            # Use only the preceding clause, not unrelated disclaimers elsewhere.
            index = bisect.bisect_right(boundaries, match.start())
            clause_start = boundaries[index - 1] if index else 0
            # Negation cues sit right before the claim; a bounded window keeps this O(1) per match.
            prefix = text[max(clause_start, match.start() - NEGATION_WINDOW):match.start()]
            claim = match.group(0)
            clause_names_ghh = (bisect.bisect_left(ghh_mentions, clause_start)
                                < bisect.bisect_left(ghh_mentions, match.end()))
            if NEGATED_PREFIX.search(prefix):
                continue
            if re.search(r"\b(?:does not|do not|never)\b", claim, re.I):
                continue
            # Only the exact processor/payout fact is exempt. A Stripe mention
            # must never excuse screening or a claim that GHH checks identity.
            if re.fullmatch(r"Stripe verifies identity and bank details for payouts(?:, not worker quality)?", claim.strip(), re.I):
                continue
            # Attribute only this match to a named competitor; never exempt a
            # clause that also asserts something about GHH or its profiles.
            if (COMPETITOR_AT.match(text, clause_start)
                    and not clause_names_ghh
                    and not re.search(r"optional identity verification|identity verification is optional|no identity verification requirement|without required", claim, re.I)):
                continue
            problems.append(claim)
    return problems



class VisibleFaq(HTMLParser):
    """Collect every visible FAQ card as (question, answer) in document order.

    Layouts used on the site, matched by element and attribute rather than serialized markup:
    - <details><summary>Q</summary>A</details>
    - <article data-ai-answer> ... <h3>Q</h3> ... A ... </article>
    - an <h2 id="faq"> section: each <h3> is a question and the content up to the next <h3> is the answer;
      the section ends at the next <h2>, a call-to-action element whose class includes "callout",
      </main>, <footer>, or </section>/</article> enclosing the heading.
    In every layout the answer is the whole region after the question, up to the next question or the
    end of the card/section, so a second paragraph or list cannot add an unchecked claim.
    """

    VOID = {"area", "base", "br", "col", "embed", "hr", "img", "input", "link", "meta", "source", "track", "wbr"}

    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.cards = []
        self.stack = []
        self.mode = None          # "details" | "card" | "section" while inside an FAQ container
        self.container_depth = None
        self.capture = None       # "q" | "a" | None
        self.capture_tag = None
        self.capture_depth = None
        self.question = []
        self.answer = []
        self.section_depth = None
        self.section_answer_open = False
        self.skip_depth = None

    def _flush(self):
        if self.question:
            q = " ".join("".join(self.question).split())
            a = " ".join("".join(self.answer).split())
            self.cards.append((q, a))
        self.question, self.answer = [], []

    BLOCK = {"p", "div", "li", "ul", "ol", "br", "h2", "h3", "h4", "table", "tr", "td", "th", "pre"}

    def handle_starttag(self, tag, attrs):
        attrs = dict(attrs)
        classes = (attrs.get("class") or "").split()
        if tag in self.BLOCK and self.answer:
            self.answer.append(" ")
        if tag in self.VOID:
            return
        self.stack.append(tag)
        depth = len(self.stack)
        if self.skip_depth is None and ("hidden" in attrs or tag in {"script", "style", "template"}):
            self.skip_depth = depth
        if self.mode == "section" and (tag == "h2" or tag == "footer" or "callout" in classes):
            self._flush()
            self.mode = None
            self.section_depth = None
        if self.mode is None:
            if tag == "details":
                self.mode, self.container_depth = "details", depth
            elif tag == "article" and "data-ai-answer" in attrs:
                self.mode, self.container_depth = "card", depth
            elif tag == "h2" and (attrs.get("id") or "").lower() == "faq":
                self.mode, self.container_depth = "section", None
                self.section_depth = depth - 1
                self.capture, self.capture_tag, self.capture_depth = "skip-h2", tag, depth
                return
        if self.mode == "details":
            if tag == "summary" and not self.question:
                self.capture, self.capture_tag, self.capture_depth = "q", tag, depth
        elif self.mode == "card":
            if tag in {"h2", "h3"} and not self.question:
                self.capture, self.capture_tag, self.capture_depth = "q", tag, depth
            elif tag == "p" and self.question and not self.answer and self.capture is None:
                self.capture, self.capture_tag, self.capture_depth = "a", tag, depth
        elif self.mode == "section":
            if tag == "h3":
                self._flush()
                self.capture, self.capture_tag, self.capture_depth = "q", tag, depth
            elif tag == "p" and self.question and not self.answer and self.capture is None:
                self.capture, self.capture_tag, self.capture_depth = "a", tag, depth

    def handle_endtag(self, tag):
        if tag in self.VOID or tag not in self.stack:
            return
        while self.stack:
            depth = len(self.stack)
            open_tag = self.stack.pop()
            if self.skip_depth == depth:
                self.skip_depth = None
            if self.capture and self.capture_depth == depth:
                self.capture = None
            if self.mode in {"details", "card"} and self.container_depth == depth:
                self._flush()
                self.mode = self.container_depth = None
            if self.mode == "section" and self.section_depth is not None and depth <= self.section_depth:
                self._flush()
                self.mode = self.section_depth = None
            if open_tag == tag:
                break
        if self.mode == "section" and tag == "main":
            self._flush()
            self.mode = self.section_depth = None

    def handle_data(self, data):
        if self.skip_depth is not None:
            return
        if self.capture == "q":
            self.question.append(data)
        elif self.capture == "a" or (self.mode in {"details", "card", "section"} and self.question and self.capture is None):
            self.answer.append(data)

    def close(self):
        super().close()
        if self.mode:
            self._flush()


def visible_faq_cards(source):
    parser = VisibleFaq()
    parser.feed(source)
    parser.close()
    return parser.cards


def visible_faq_questions(source):
    return [question for question, _ in visible_faq_cards(source)]


def faq_contract_problems(source):
    blocks = re.findall(r'<script type="application/ld\+json">\s*(.*?)\s*</script>', source, re.S)
    faqs = [json.loads(block) for block in blocks if json.loads(block).get("@type") == "FAQPage"]
    if not faqs:
        return ["missing FAQPage"]
    def norm(text):
        return re.sub(r"\s+([.,;:!?])", r"\1", " ".join(text.split()))

    structured = [(norm(e["name"]), norm(e["acceptedAnswer"]["text"])) for faq in faqs for e in faq["mainEntity"]]
    visible = [(norm(q), norm(a)) for q, a in visible_faq_cards(source)]
    problems = []
    for label, items in (("JSON-LD", structured), ("visible", visible)):
        names = [q for q, _ in items]
        for question in sorted({q for q in names if names.count(q) > 1}):
            problems.append(f"{question}: duplicate {label} question")
    expected = dict(structured)
    shown = {}
    for question, answer in visible:
        shown.setdefault(question, []).append(answer)
        if question not in expected:
            problems.append(question + ": visible question missing from JSON-LD")
        elif answer != expected[question]:
            problems.append(question + ": visible and JSON-LD answer differ")
    for question, _ in structured:
        if question not in shown:
            problems.append(question + ": missing visible answer")
    if len(visible) != len(structured):
        problems.append(f"FAQ count differs: {len(visible)} visible vs {len(structured)} JSON-LD")
    return problems


def faq_pages():
    """Every public page that publishes FAQPage structured data."""
    return sorted(str(path.relative_to(FRONTEND)) for path in public_files()
                  if path.suffix == ".html" and '"FAQPage"' in path.read_text(encoding="utf-8"))


def public_files():
    return sorted(path for path in FRONTEND.rglob("*")
                  if path.is_file() and path.suffix.lower() in {".html", ".json", ".txt"}
                  and not (EXCLUDED & set(path.relative_to(FRONTEND).parts)))


EDITED_FAQ_PAGES = (
    "faq.html", "post-a-small-task.html",
    "use-cases/ai-agent-human-in-the-loop-tasks.html", "use-cases/hire-human-to-review-ai-output.html",
    "blog/alternatives-to-fiverr.html", "blog/alternatives-to-freelancer.html", "blog/alternatives-to-toptal.html",
    "blog/alternatives-to-upwork.html", "blog/best-freelance-platforms-escrow.html",
    "blog/complete-guide-hiring-virtual-assistant.html", "blog/freelancers-switching-lower-fee-platforms.html",
    "blog/gohirehumans-vs-fiverr.html", "blog/hire-data-entry-specialist.html", "blog/hire-human-for-ai-tasks.html",
    "blog/human-as-a-service.html", "blog/on-demand-workforce-platform.html",
    "blog/verified-freelancer-marketplace.html", "blog/where-to-list-services-online.html",
    "blog/how-to-find-human-workers-ai-tasks.html",
)


class SnippetFields(HTMLParser):
    """Meta descriptions (any attribute order or quoting) and JSON-LD blocks, parsed structurally."""
    NAMES = {"description", "og:description", "twitter:description"}

    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.fields, self.blocks, self.in_json_ld, self.buffer = [], [], False, []

    def handle_starttag(self, tag, attrs):
        attributes = {key.lower(): value or "" for key, value in attrs}
        if tag == "meta" and (attributes.get("name") or attributes.get("property") or "").lower() in self.NAMES:
            self.fields.append(attributes.get("content", ""))
        elif tag == "script" and attributes.get("type", "").lower() == "application/ld+json":
            self.in_json_ld, self.buffer = True, []

    def handle_endtag(self, tag):
        if tag == "script" and self.in_json_ld:
            self.in_json_ld = False
            self.blocks.append("".join(self.buffer))

    def handle_data(self, data):
        if self.in_json_ld:
            self.buffer.append(data)


def json_strings(node):
    if isinstance(node, dict):
        for value in node.values():
            yield from json_strings(value)
    elif isinstance(node, list):
        for value in node:
            yield from json_strings(value)
    elif isinstance(node, str):
        yield node


PERCENT = r"(?:\s*%|\s+percent\b)"
# A GHH fee claim: 1% ... 3%/processing (possibly across a sentence break) or a 4% total.
FEE_CLAIM = re.compile(
    r"(?:\b1" + PERCENT + r"|\bone\s+percent\b)[^;]{0,120}?(?:\b3" + PERCENT + r"|\bthree\s+percent\b|\bprocessing\b)"
    r"|\b4" + PERCENT + r"\s+(?:in\s+)?(?:total|combined|all-in)\b"
    r"|\b(?:total|combined|all-in)\s+(?:fee\s+)?(?:of\s+)?4" + PERCENT, re.I)
CHECKOUT_QUALIFIER = re.compile(
    r"where\s+(?:checkout|(?:card\s+)?payment\s+processing|payments?)\s+(?:is|are)\s+configured", re.I)
SENTENCE_END = re.compile(r"[.!?](?=\s|$)")


# A new clause, conjunction or dash between a fee and a qualifier means the qualifier belongs to
# something else ("... processing and drafts are saved where checkout is configured").
CLAUSE_BREAK = re.compile(r"[;\u2013\u2014]|\s-\s|[.!?](?=\s|$)|\b(?:and|but|while|so|or|whereas|although|though|yet)\b", re.I)
TAIL_BREAK = re.compile(CLAUSE_BREAK.pattern + r"|\b(?:is|are|was|were|gets?|stays?|remains?|saved|shown|listed)\b", re.I)
QUALIFIER_LEAD = re.compile(r"\s*(?:[\w ]{0,24}:\s*)?")


def qualifier_binds(field, start, match, end):
    """The checkout qualifier must modify this fee: inside it, directly after it (a short noun-phrase
    gap such as " processing charge "), or leading its sentence ("Where checkout is configured, buyers pay")."""
    if CHECKOUT_QUALIFIER.search(match.group(0)):
        return True
    after = CHECKOUT_QUALIFIER.search(field, match.end(), end)
    if after:
        gap = field[match.end():after.start()]
        if len(gap) <= 60 and not TAIL_BREAK.search(gap):
            return True
    for before in CHECKOUT_QUALIFIER.finditer(field, start, match.start()):
        gap = field[before.end():match.start()]
        if QUALIFIER_LEAD.fullmatch(field[start:before.start()]) and len(gap) <= 40 and not CLAUSE_BREAK.search(gap):
            return True
    return False


def fee_snippet_problems(source):
    """Fee claims in standalone snippets whose own clause lacks the checkout qualifier.
    A fee clause whose subject is a competitor is exempt; a clause that also names GHH is not."""
    parser = SnippetFields()
    parser.feed(source)
    fields = list(parser.fields)
    for block in parser.blocks:
        fields.extend(json_strings(json.loads(block)))
    problems = []
    for field in fields:
        for match in FEE_CLAIM.finditer(field):
            start = max([0] + [m.end() for m in SENTENCE_END.finditer(field, 0, match.start())])
            end_match = SENTENCE_END.search(field, match.end())
            end = end_match.end() if end_match else len(field)
            claim = field[start:end].strip()
            if qualifier_binds(field, start, match, end):
                continue
            # A competitor is exempt only as the fee clause's own subject ("Upwork charges ...",
            # "Fiverr's fee is ...") or locative frame ("On Fiverr, buyers pay ..."), and never when
            # the same clause names GHH ("Unlike Fiverr, employers pay ..." is a GHH claim).
            head = field[start:match.start()]
            breaks = list(CLAUSE_BREAK.finditer(head))
            tail = field[match.end():end]
            tail_break = CLAUSE_BREAK.search(tail)
            clause = ((head[breaks[-1].end():] if breaks else head) + match.group(0)
                      + (tail[:tail_break.start()] if tail_break else tail))
            if (re.match(r"\s*(?:(?:[Oo]n|[Aa]t)\s+" + COMPETITOR + r"\b|(?:\w+\s+){0,2}?" + COMPETITOR
                         + r"(?:'s)?\s+(?:\w+\s+){0,3}?(?:charges?|takes?|fees?|commission|pricing|costs?|is|are|"
                         r"keeps?|adds?)\b)", clause)
                    and not re.search(r"GoHireHumans|\bGHH\b|\bwe\b|\bour\b|\bus\b", clause, re.I)):
                continue
            problems.append(claim)
    return problems


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

    def test_every_faq_page_matches_json_ld(self):
        pages = faq_pages()
        # Discovery must find every page this change edited (a floor, not the whole set).
        self.assertLessEqual(set(EDITED_FAQ_PAGES), set(pages))
        self.assertGreaterEqual(len(pages), 50)
        for relative in pages:
            with self.subTest(page=relative):
                self.assertEqual(faq_contract_problems((FRONTEND / relative).read_text()), [])

    def test_faq_omission_mutations_are_caught(self):
        source = (FRONTEND / "blog/where-to-list-services-online.html").read_text()
        blocks = re.findall(r'<script type="application/ld\+json">\s*(.*?)\s*</script>', source, re.S)
        faq_block = next(b for b in blocks if json.loads(b).get("@type") == "FAQPage")
        faq = json.loads(faq_block)
        self.assertEqual(len(faq["mainEntity"]), 3)
        self.assertEqual(faq_contract_problems(source), [])
        # Missing schema question: visible card kept, third JSON-LD question removed.
        dropped = dict(faq, mainEntity=faq["mainEntity"][:2])
        self.assertTrue(faq_contract_problems(source.replace(faq_block, json.dumps(dropped), 1)))
        # Visible-only question: an extra visible FAQ card with no JSON-LD entry.
        anchor = "<h3>" + faq["mainEntity"][0]["name"] + "</h3>"
        self.assertEqual(source.count(anchor), 1)
        extra = "<h3>Can I list a service for free?</h3>\n          <p>Yes.</p>\n\n          " + anchor
        self.assertTrue(faq_contract_problems(source.replace(anchor, extra, 1)))
        # Duplicate visible card with a conflicting answer.
        first = faq["mainEntity"][0]["name"]
        duplicate = "<h3>" + first + "</h3>\n          <p>The employer pays a 2% platform fee.</p>\n\n          " + anchor
        self.assertTrue(faq_contract_problems(source.replace(anchor, duplicate, 1)))
        # Renamed visible question.
        self.assertTrue(faq_contract_problems(source.replace(anchor, "<h3>" + first + " (updated)</h3>", 1)))

    def test_faq_markup_variants_are_parsed(self):
        def page(body, questions):
            ld = {"@context": "https://schema.org", "@type": "FAQPage", "mainEntity": [
                {"@type": "Question", "name": q, "acceptedAnswer": {"@type": "Answer", "text": a}} for q, a in questions]}
            return ('<html><head><script type="application/ld+json">' + json.dumps(ld) + "</script></head>"
                    "<body><main>" + body + "</main></body></html>")

        qa = [("Is it free to post?", "Posting is free."), ("How are fees charged?", "A 1% platform fee plus a fixed 3% processing charge.")]
        layouts = {
            "details with attributes": "".join(f'<details class="disclosure" open><summary class="q">{q}</summary><div><p>{a}</p></div></details>' for q, a in qa),
            "data-ai-answer card with nested header": "".join(f"<article class='card' data-ai-answer><header><h3 class='card-title'>{q}</h3></header><p class='card-text'>{a}</p></article>" for q, a in qa),
            "h2 id=faq single quotes": "<h2 id='faq'>Frequently asked questions</h2>" + "".join(f"<h3>{q}</h3><p>{a}</p>" for q, a in qa),
            "h2 id=faq section then CTA callout": '<section><h2 id="faq">FAQ</h2>' + "".join(f"<h3>{q}</h3><p>{a}</p>" for q, a in qa)
                + '</section><div class="callout"><h3>Ready to post a task?</h3><p>Start now.</p></div>',
            "h2 id=faq then inline CTA callout": '<h2 id="faq">FAQ</h2>' + "".join(f"<h3>{q}</h3><p>{a}</p>" for q, a in qa)
                + '<div class="callout cta"><h3>Ready to post a task?</h3><p>Start now.</p></div>',
        }
        for name, body in layouts.items():
            with self.subTest(layout=name):
                self.assertEqual(faq_contract_problems(page(body, qa)), [])
                self.assertTrue(faq_contract_problems(page(body, qa[:1])), "missing JSON-LD question not caught")
                self.assertTrue(faq_contract_problems(page(body, qa + [("Do you vet workers?", "No.")])), "missing visible question not caught")
                wrong = [(qa[0][0], qa[0][1]), (qa[1][0], "A 2% platform fee.")]
                self.assertTrue(faq_contract_problems(page(body, wrong)), "answer drift not caught")

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
            "Every listing is reviewed by our moderators.",
            "All workers have their IDs checked before joining.",
            "GoHireHumans does not guarantee results but does verify every worker.",
            "You pay for completed, approved work, not for hours spent.",
            "The employer reviews the work before paying.",
            "From one-off tasks to enterprise-scale operations.",
            "Ready to hire elite talent?",
            "Ready to hire elite talent at 1% platform fee?",
            "Find Toptal-quality developers on GoHireHumans.",
            "GoHireHumans (only platform with REST API + MCP)",
            "No account required to explore; workers receive the listed payout when you hire.",
            "Workers get paid as soon as the employer hires them.",
            "Fees: 1% + Stripe (employer)",
            "Workers receive their listed payout when you hire.",
            "Workers are paid immediately when you hire.",
            "The worker is paid when you hire.",
            "Workers receive the listed payout upon hire.",
            "Freelancers get paid at checkout.",
            "Payouts are released as soon as you hire.",
            "Employers pay 1% plus Stripe fees.",
            "Employers pay a 1% platform fee + Stripe processing fees.",
            "Listed payout to worker; employer pays Stripe + 1%",
            "Stripe fees plus 1% for employers.",
            "The worker is paid only when the employer approves the delivered work with release_payment.",
            "Workers get paid only after you approve the work.",
            "The worker receives the listed payout only once the buyer accepts the delivery.",
            "Payouts are released only when the employer approves.",
            "Only the employer can release the worker's payout.",
            "Only you can release payment.",
            "The worker is paid only when the employer approves the work with release_payment or a site admin completes the order.",
            "Workers are paid only after approval.",
            "The worker is paid only when the employer approves the work with release_payment or completes the order, or a site admin completes it.",
            # Fifth-review rewordings and instant-payout claims.
            "Workers receive payment immediately after you hire them.",
            "Payment is released to the worker when you hire.",
            "When you hire, the worker receives the listed payout.",
            "Employers pay Stripe processing costs plus a 1% platform fee.",
            "Your card is charged only after the task is completed.",
            "The employer's card is charged once the work is approved.",
            "Approve milestone → instant payout",
            "Workers get instant payouts.",
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

    def test_truthful_payment_timing_and_fees_are_allowed(self):
        for statement in (
            "Your card is charged when you hire; the worker receives the listed payout after you approve the delivered work.",
            "The employer is charged at hire. Workers receive the listed payout after approval.",
            "Employers pay a 1% platform fee plus a fixed 3% processing charge where checkout is configured.",
            "Payments run through Stripe where checkout is configured.",
            "Workers receive the listed payout after the employer approves the work.",
            "The worker is paid when the employer approves the work with release_payment or completes the order, or a site admin completes it.",
            "In the normal flow, the worker is paid when the employer approves the submitted work with `release_payment`. A site admin can also complete a service order, which releases its payout.",
            "Nothing is published or charged until you approve it.",
            "Your card is charged for the first milestone when you hire (Stripe processing where configured).",
            "Approving a submitted milestone releases its payout and charges the next milestone, if there is one.",
            "When you hire, the card is charged for the first milestone.",
            "The worker's payout is released when you approve the delivered work.",
        ):
            with self.subTest(statement=statement):
                self.assertEqual(claim_problems(statement), [])

    def test_faq_answers_include_every_paragraph(self):
        """A second answer paragraph or list item is part of the answer and must match JSON-LD."""
        def page(body, questions):
            ld = {"@context": "https://schema.org", "@type": "FAQPage", "mainEntity": [
                {"@type": "Question", "name": q, "acceptedAnswer": {"@type": "Answer", "text": a}} for q, a in questions]}
            return ('<html><head><script type="application/ld+json">' + json.dumps(ld) + "</script></head>"
                    "<body><main>" + body + "</main></body></html>")

        q, first, second = "How are workers paid?", "The card is charged at hire.", "The payout is released after approval."
        extra = "The worker is paid immediately at hire."
        layouts = {
            "card": lambda tail: f"<article data-ai-answer><h3>{q}</h3><p>{first}</p>{tail}</article>",
            "section": lambda tail: f'<h2 id="faq">FAQ</h2><h3>{q}</h3><p>{first}</p>{tail}<h2>Related</h2>',
            "details": lambda tail: f"<details><summary>{q}</summary><p>{first}</p>{tail}</details>",
        }
        for name, layout in layouts.items():
            with self.subTest(layout=name):
                # Truthful two-paragraph answer represented in full in JSON-LD: no problem.
                self.assertEqual(faq_contract_problems(page(layout(f"<p>{second}</p>"), [(q, f"{first} {second}")])), [])
                # Contradictory extra paragraph or list item that JSON-LD omits: caught.
                self.assertTrue(faq_contract_problems(page(layout(f"<p>{extra}</p>"), [(q, first)])))
                self.assertTrue(faq_contract_problems(page(layout(f"<ul><li>{extra}</li></ul>"), [(q, first)])))
        source = (FRONTEND / "blog/alternatives-to-freelancer.html").read_text(encoding="utf-8")
        cards = visible_faq_cards(source)
        self.assertTrue(cards)
        answer = cards[0][1]
        anchor = next(m for m in re.finditer(r"<p[^>]*>([^<]+)</p>", source) if " ".join(m.group(1).split()) == answer)
        mutated = source[:anchor.end()] + "<p>The worker is paid immediately at hire.</p>" + source[anchor.end():]
        self.assertTrue(faq_contract_problems(mutated))

    def test_standalone_fee_snippets_keep_the_checkout_qualifier(self):
        """Meta descriptions and JSON-LD strings are quoted on their own, so any GHH 1% + 3% fee
        statement there must say where checkout (payment processing) is configured."""
        checked = 0
        for path in public_files():
            if path.suffix != ".html":
                continue
            source = path.read_text(encoding="utf-8")
            checked += 1
            with self.subTest(page=str(path.relative_to(FRONTEND))):
                self.assertEqual(fee_snippet_problems(source), [])
        self.assertGreater(checked, 100)
        page = (FRONTEND / "categories/graphic-design.html").read_text(encoding="utf-8")
        self.assertTrue(fee_snippet_problems(page.replace("3% processing where checkout is configured.", "3% processing.")))
        page = (FRONTEND / "index.html").read_text(encoding="utf-8")
        self.assertTrue(fee_snippet_problems(page.replace(
            "3% processing charge where checkout is configured.\"\n    },", "3% processing charge when they pay for work.\"\n    },", 1)))

    def test_fee_snippet_check_binds_the_qualifier_to_the_claim(self):
        meta = '<meta name="description" content="{}">'.format
        for snippet in (
            meta("Employers pay 1% plus 3% processing. Drafts are saved where checkout is configured."),
            meta("Unlike Fiverr, GoHireHumans charges 1% plus 3% processing."),
            meta("A 1 percent platform fee plus 3 percent processing."),
            meta("GoHireHumans charges a 1% platform fee. Employers also pay 3% processing."),
            meta("Employers pay 4% in total."),
            meta("A combined 4% fee for employers."),
            meta("Employers pay 1% plus 3% processing, and drafts are saved where checkout is configured."),
            meta("Unlike Fiverr, employers pay 1% plus 3% processing."),
            meta("Compared with Upwork, employers pay 1% plus 3% processing."),
            meta("Drafts are saved where checkout is configured, and employers pay 1% plus 3% processing."),
            meta("Employers pay 1% plus 3% processing and drafts are saved where checkout is configured."),
            meta("Employers pay 1% plus 3% processing, AND drafts are saved where checkout is configured."),
            meta("Employers pay 1% plus 3% processing \u2014 drafts are saved where checkout is configured."),
            meta("Employers pay 1% plus 3% processing; drafts are saved where checkout is configured."),
            meta("Drafts are saved where checkout is configured, employers pay 1% plus 3% processing."),
            "<meta content='Employers pay 1% plus 3% processing.' property='og:description'>",
            '<meta property="twitter:description" content="Employers pay 1% plus 3% processing.">',
            '<script type="application/ld+json">{"offers": {"description": "Employers pay 1% plus 3% processing."}}</script>',
        ):
            with self.subTest(snippet=snippet):
                self.assertTrue(fee_snippet_problems(snippet))
        for snippet in (
            meta("Employers pay a 1% platform fee plus a fixed 3% processing charge where checkout is configured."),
            meta("Upwork charges clients up to 7.99% plus a 1% processing fee on some plans."),
            meta("Workers get the listed payout. Employers pay 1% + 3% processing where checkout is configured."),
            meta("Employers pay 1% plus 3% processing where checkout is configured, and nothing else."),
            meta("Upwork charges clients 1% plus 3% processing on some plans; GoHireHumans charges 1% plus 3% processing where checkout is configured."),
            meta("On Fiverr, buyers pay a 5.5% service fee plus 1% processing."),
            meta("Where checkout is configured, buyers pay a 1% platform fee plus a fixed 3% processing charge."),
            meta("Employers pay a 1% platform fee plus a fixed 3% processing charge, paid by employers where checkout is configured."),
            meta("Pricing: where checkout is configured, the fee is 1% plus a fixed 3% processing charge."),
        ):
            with self.subTest(control=snippet):
                self.assertEqual(fee_snippet_problems(snippet), [])

    def test_snippet_metadata_is_short_and_consistent(self):
        """Search results cut descriptions at about 160 characters; Open Graph and Twitter repeat the meta."""
        for relative in ("index.html", "about.html", "categories/graphic-design.html", "categories/writing.html"):
            source = (FRONTEND / relative).read_text(encoding="utf-8")
            with self.subTest(page=relative):
                found = {name: [html.unescape(v) for v in re.findall(
                    rf'<meta (?:name|property)="{re.escape(name)}" content="([^"]*)"', source)]
                    for name in ("description", "og:description", "twitter:description")}
                self.assertEqual([len(v) for v in found.values()], [1, 1, 1])
                description = found["description"][0]
                self.assertLessEqual(len(description), 160)
                self.assertEqual(found["og:description"][0], description)
                self.assertEqual(found["twitter:description"][0], description)
                self.assertIn("where checkout is configured", description)

    def test_mcp_hire_worker_names_every_payout_trigger(self):
        """POST /orders/<id>/complete lets the employer or a site admin complete a submitted or
        in-progress service order, which releases its payout, besides release_payment approval."""
        api = (ROOT / "backend/api_core.py").read_text(encoding="utf-8")
        start = api.index('elif re.match(r"^/orders/(\\d+)/complete$", path) and method == "POST":')
        route = api[start:api.index("release_escrow_to_worker(", start)]
        self.assertIn("order['employer_id'] != user['id'] and not user['is_admin']", route)
        self.assertIn("if order['status'] not in ('submitted', 'in_progress'):", route)
        for relative in ("backend/mcp_server.py", "backend/mcp-package/mcp_server.py"):
            tree = ast.parse((ROOT / relative).read_text(encoding="utf-8"))
            tools = next(ast.literal_eval(node.value) for node in tree.body
                         if isinstance(node, ast.Assign) and any(getattr(t, "id", None) == "TOOLS" for t in node.targets))
            description = next(tool["description"] for tool in tools if tool["name"] == "hire_worker")
            with self.subTest(file=relative):
                for trigger in ("the employer approves the work with release_payment",
                                "or completes the order", "or a site admin completes it"):
                    self.assertIn(trigger, description)
                self.assertEqual(claim_problems(description), [])
                self.assertTrue(claim_problems(description.replace("The worker is paid when", "The worker is paid only when")))

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

    def test_review8_truthful_controls_are_allowed(self):
        for statement in (
            "When you hire, the worker receives instructions, not payment.",
            "Instant payouts are not available on GoHireHumans.",
            "Instant payouts aren't offered.",
            "Instant payouts are not available yet.",
            "Instant payouts are unavailable on GoHireHumans.",
            "The report this listing button is not available on GoHireHumans.",
            "Instant payouts are not available, so payouts follow approval.",
            "Instant payouts are not available on GoHireHumans, but payouts follow approval.",
            "Listing reporting is not available on GoHireHumans; email support instead.",
            "Reporting tools aren't offered; email the operations inbox.",
            "If an order goes wrong, open a dispute from the order page.",
        ):
            with self.subTest(statement=statement):
                self.assertEqual(claim_problems(statement), [])
        for mutation in (
            "Suspicious listing reporting",
            "Use the dispute and reporting systems to surface concerns.",
            "Click the report this listing button.",
            "Instant payouts are not delayed on GoHireHumans.",
            "Instant payouts are never delayed on GoHireHumans.",
            "Instant payouts are not subject to a waiting period.",
            "Listing reporting is not hard to find.",
            "Instant payouts are not offered to unverified users, but everyone else gets them.",
            "Listing reporting is not supported for guests; sign in to report any listing.",
            "Instant payouts are not available, except for verified workers.",
            "Instant payouts are not available, but verified workers get them.",
            "Click the report this listing button, which is not hard to find.",
            "When you hire, the worker receives the listed payout.",
            "When you hire, the worker gets paid.",
        ):
            with self.subTest(mutation=mutation):
                self.assertTrue(claim_problems(mutation))

    def test_claim_problems_runtime_is_linear_on_repeated_claims(self):
        """claim_problems once re-split the whole prefix per match: 186s on 1e6 chars of repeated claims."""
        import time
        for unit in ("Workers get instant payouts. ", "When you hire, the worker receives the listed payout. ",
                     "GoHireHumans verifies every worker. ", "Instant payouts are not available. ",
                     "Workers get instant payouts ", "Upwork verified workers and "):
            text = unit * (1_000_000 // len(unit))
            started = time.perf_counter()
            problems = claim_problems(text)
            elapsed = time.perf_counter() - started
            with self.subTest(unit=unit):
                self.assertLess(elapsed, 10)
                if "not available" in unit:
                    self.assertEqual(problems, [])
                elif unit.startswith("Upwork"):
                    continue
                else:
                    self.assertGreater(len(problems), 1000)

    def test_claim_guard_runtime_is_linear_on_adversarial_whitespace(self):
        """Overlapping whitespace quantifiers made one pattern quadratic (over 3s at 1e5 spaces)."""
        import time
        prefixes = ("Employers pay Stripe", "Employers pay the Stripe processing", "When you hire, the worker",
                    "Workers receive", "GoHireHumans verifies", "Profiles show", "instant", "listing")
        for prefix in prefixes:
            text = prefix + " " * 200000 + "oops"
            for index, pattern in enumerate(COMPILED):
                started = time.perf_counter()
                list(pattern.finditer(text))
                elapsed = time.perf_counter() - started
                with self.subTest(prefix=prefix, pattern=index):
                    self.assertLess(elapsed, 0.5)


if __name__ == "__main__":
    unittest.main()
