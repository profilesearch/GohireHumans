#!/usr/bin/env python3
"""Build frontend/llms-full.txt from its sources of truth.

- Buyer answers come from the visible FAQ cards (`data-ai-answer`) on the answer pages,
  so the long-form file can never say something the pages do not.
- MCP tools and resources come from backend/mcp_server.py, so the list matches what ships.
- Everything else is the reviewed template in scripts/templates/llms-full.template.txt.

Usage: python3 scripts/build_llms_full.py          write frontend/llms-full.txt
       python3 scripts/build_llms_full.py --check  exit 1 if the file is out of date
Stdlib only.
"""
import ast
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
FRONTEND = ROOT / "frontend"
TEMPLATE = ROOT / "scripts" / "templates" / "llms-full.template.txt"
OUTPUT = FRONTEND / "llms-full.txt"
MCP_SERVER = ROOT / "backend" / "mcp_server.py"
SITE = "https://www.gohirehumans.com/"
REVIEWED = "2026-10-13"
sys.path.insert(0, str(ROOT / "backend"))

from test_ai_answer_readiness import FAQ_PAGES, AnswerHTML  # noqa: E402


def _literal(name):
    tree = ast.parse(MCP_SERVER.read_text(encoding="utf-8"))
    for node in tree.body:
        if isinstance(node, ast.Assign) and any(getattr(t, "id", None) == name for t in node.targets):
            return ast.literal_eval(node.value)
    raise ValueError(f"{name} not found in {MCP_SERVER}")


# Reviewed one-line summaries. The build fails if a tool or resource is added, removed, or
# renamed without updating these, or if a summary drops a safety property the full tool
# description states (read-only, charges a card, cannot be undone).
TOOL_SUMMARIES = {
    "search_services": "Search service listings by keyword, category and price. Read-only; no key needed.",
    "get_service_details": "Full public details and current price of one listing. Read-only; no key needed. Call it just before `hire_worker`.",
    "get_categories": "List valid category slugs for human and AI-agent services. Read-only; no key needed.",
    "create_job": "Publish a fixed-budget job post that workers can apply to. Needs the owner's approval and a session token or a write-scope API key. It does not hire anyone or charge anything.",
    "browse_jobs": "Browse open job posts, newest first. Read-only; no key needed.",
    "hire_worker": "Order one service listing. Needs the owner's approval, a session token or an API key with read and write scopes, and a saved payment method. Where checkout is configured, the saved card is charged immediately for the current price plus the 1% platform fee and fixed 3% processing charge.",
    "get_job_status": "Check one order (its employer or worker only) or one job post by ID. Read-only.",
    "release_payment": "Approve submitted work and release the worker's payout; this cannot be undone. Session token only: unset `GOHIREHUMANS_API_KEY` first. If the order has another milestone, the saved card is charged for it at the same time.",
    "submit_review": "Rate and review the other party on a completed order. Needs a session token or a write-scope API key.",
    "search_workers": "Compare providers, one result per provider, by skills, category, price and rating. Read-only; no key needed.",
    "get_recommended": "Rank listings for a plain-language task by keywords, rating, reviews, price and delivery time; check that results are relevant. Read-only; no key needed.",
    "get_pricing_info": "Fees and payment terms. Read-only; no key needed.",
    "get_platform_info": "Overview of the marketplace and its typical workflow. Read-only; no key needed.",
}
RESOURCE_SUMMARIES = {
    "gohirehumans://api-docs": "REST API documentation.",
    "gohirehumans://categories": "All service categories.",
    "gohirehumans://mcp-quickstart": "Step-by-step MCP setup guide.",
}
# (property found in the full description, text the summary must then contain)
SAFETY_PROPERTIES = [
    (re.compile(r"\bRead-only\b"), "Read-only"),
    (re.compile(r"\bcharged\b"), "charged"),
    (re.compile(r"cannot be undone"), "cannot be undone"),
]


def _checked_summaries(items, key, summaries, kind):
    names = [item[key] for item in items]
    if sorted(names) != sorted(summaries):
        raise ValueError(
            f"{kind} summaries out of date: missing {sorted(set(names) - set(summaries))}, "
            f"stale {sorted(set(summaries) - set(names))}"
        )
    for item in items:
        description = " ".join((item.get("description") or "").split())
        if not description:
            raise ValueError(f"MCP {kind} {item[key]!r} has no description")
        for pattern, required in SAFETY_PROPERTIES:
            if pattern.search(description) and required not in summaries[item[key]]:
                raise ValueError(f"{kind} summary for {item[key]!r} must say {required!r}")
    return [(name, summaries[name]) for name in names]


def faq_section():
    blocks = []
    for relative, questions in FAQ_PAGES.items():
        pairs = AnswerHTML((FRONTEND / relative).read_text(encoding="utf-8")).pairs
        if [q for q, _ in pairs] != questions:
            raise ValueError(f"{relative}: visible questions differ from FAQ_PAGES")
        for question, answer in pairs:
            blocks.append(f"### {question}\n\n{answer}\n\nSource: {SITE}{relative}")
    return "\n\n".join(blocks)


def tool_lines(tools, summaries=None):
    pairs = _checked_summaries(tools, "name", TOOL_SUMMARIES if summaries is None else summaries, "tool")
    return "\n".join(f"- `{name}`: {summary}" for name, summary in pairs)


def resource_lines(resources, summaries=None):
    pairs = _checked_summaries(resources, "uri", RESOURCE_SUMMARIES if summaries is None else summaries, "resource")
    return "\n".join(f"- `{uri}`: {summary}" for uri, summary in pairs)


NEGATION = re.compile(r"(?i)\b(no|not|never|nothing|cannot|isn't|doesn't|don't|aren't)\b")
# Each rule: (name, sentence trigger, extra test that makes the sentence acceptable).
# A triggering sentence passes only if it is negated or meets the extra test.
FACT_RULES = [
    ("outbound webhook or callback claim", re.compile(r"(?i)\b(webhooks?|callbacks?)\b"), None),
    ("hourly hiring claim", re.compile(r"(?i)\bhourly\b.*\b(hire|hiring|job|contract|post)s?\b|\b(hire|hiring|job|contract|post)s?\b.*\bhourly\b"),
     re.compile(r"(?i)\brather than assuming hourly\b")),
    ("API-key payout approval claim", re.compile(r"(?i)\bAPI[- ]keys?\b.*\b(release_payment|releas\w*|approv\w* (the )?(delivered|submitted) work)\b|\b(release_payment|releas\w*)\b.*\bAPI[- ]keys?\b"),
     re.compile(r"(?i)only a session token|session token only|\bunset\b|\brejected\b")),
    ("spend or hiring without owner approval", re.compile(r"(?i)\bwithout\b[^.]*\b(approval|approving|owner|asking)\b|\bautomatic(ally)?\b[^.]*\b(hire|hires|pay|pays|charge|charges|approve|approves|release)\b"), None),
    ("unconditional payout-protection claim", re.compile(r"(?i)\bpaid only (after|when)\b|\bonly (after|once|when) (the )?(employer|buyer|you) approv|\bnever (paid|released) (before|without)\b"), None),
    ("zero or different employer fee claim", re.compile(r"(?i)\b(no|zero|0%) (platform |employer |buyer )?fees?\b|\bfree for (employers|buyers|clients)\b|\bemployers pay nothing\b"), re.compile(r"$^")),
]
FORBIDDEN = re.compile(r"(?i)\bescrow\b|\bguarantee[ds]?\b|\bvetted\b|\bverified (workers|freelancers|humans|profiles?)\b|\bintelligent matching\b")


def sentences(text):
    for line in text.splitlines():
        for sentence in re.split(r"(?<=[.!?])\s+", line):
            if sentence.strip():
                yield sentence.strip()


def factual_problems(text):
    """Sentences that contradict the platform's verified behavior."""
    problems = []
    for sentence in sentences(text):
        for name, trigger, accept in FACT_RULES:
            if not trigger.search(sentence):
                continue
            negated = NEGATION.search(sentence) and name != "zero or different employer fee claim"
            if negated or (accept is not None and accept.search(sentence)):
                continue
            problems.append(f"{name}: {sentence}")
        if FORBIDDEN.search(sentence) and not NEGATION.search(sentence):
            problems.append(f"forbidden claim: {sentence}")
    for percent in sorted(set(re.findall(r"\b\d+(?:\.\d+)?%", text))):
        if percent not in ("1%", "3%"):
            problems.append(f"unexpected fee percentage: {percent}")
    return problems


def render(template=None):
    text = TEMPLATE.read_text(encoding="utf-8") if template is None else template
    values = {
        "FAQ": faq_section,
        "TOOLS": lambda: tool_lines(_literal("TOOLS")),
        "RESOURCES": lambda: resource_lines(_literal("RESOURCES")),
    }
    for key, build in values.items():
        text = text.replace("{{" + key + "}}", build())
    leftover = re.findall(r"\{\{.*?\}\}|\{\{|\}\}", text, re.S)
    if leftover:
        raise ValueError(f"unfilled or unknown template placeholders: {leftover}")
    problems = factual_problems(text)
    if problems:
        raise ValueError("llms-full.txt would contradict the platform:\n" + "\n".join(problems))
    return text


def main():
    content = render()
    if "--check" in sys.argv:
        current = OUTPUT.read_text(encoding="utf-8") if OUTPUT.exists() else ""
        if current != content:
            print("frontend/llms-full.txt is out of date: run python3 scripts/build_llms_full.py")
            return 1
        print("frontend/llms-full.txt is up to date")
        return 0
    OUTPUT.write_text(content, encoding="utf-8")
    print(f"wrote {OUTPUT.relative_to(ROOT)} ({len(content.encode())} bytes)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
