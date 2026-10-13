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
    "hire_worker": "Order one service listing. Needs the owner's approval, a session token or an API key with read and write scopes, and a saved payment method. Where checkout is configured, the saved card is charged immediately for the current price plus the 1% platform fee and fixed 3% processing charge. The worker is paid when the employer approves the work or a site admin completes the order.",
    "get_job_status": "Check one order (its employer, its worker or a site admin only) or one job post by ID. Read-only.",
    "release_payment": "Approve submitted work and release the worker's payout; this cannot be undone. Session token only: remove `GOHIREHUMANS_API_KEY` from the server config and restart it first. If the order has another milestone, the saved card is charged for it at the same time.",
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
    (re.compile(r"\bsite admin\b"), "site admin"),
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
            summary = summaries[item[key]]
            if not pattern.search(description):
                continue
            negated = re.search(
                rf"(?i)\b(not|never|no|isn't|won't|without)\b(\W+\w+){{0,3}}\W+{re.escape(required)}", summary
            )
            if required not in summary or negated:
                raise ValueError(f"{kind} summary for {item[key]!r} must state {required!r} affirmatively")
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


REVIEWED_CLAIMS = ROOT / "scripts" / "templates" / "llms-full.reviewed-claims.txt"
# Every sentence (and every line inside a code block) in llms-full.txt must match a line in the
# reviewed-claims register exactly. Nothing is exempt: earlier keyword-based gating let
# reworded money, authority, screening and capability claims through. A new or edited
# sentence, including FAQ text pulled from the live pages, fails the build until a person
# checks it against the code and adds it to the register.
# Absolute bans, even in a reviewed sentence.
FORBIDDEN = re.compile(
    r"(?i)\bescrow\b|\bguarantee[ds]?\b|\bvetted\b|\bverified (workers|freelancers|humans|profiles?)\b|"
    r"\bintelligent matching\b"
)
FEE_ROLES = (
    (re.compile(r"(?i)(\d+(?:\.\d+)?)%\s+platform fee"), "1"),
    (re.compile(r"(?i)(\d+(?:\.\d+)?)%\s+processing"), "3"),
)


# The only code-fence lines allowed: an opening fence with one of these languages, or a bare
# closing fence. Any other line containing ``` (metadata, trailing text, indented or longer
# fences) is published text the register has not reviewed, so it fails the build.
FENCE_OPEN = {"```bash", "```json"}
FENCE_CLOSE = "```"


def fence_problems(text):
    problems, fenced = [], False
    for number, raw in enumerate(text.splitlines(), 1):
        if "```" not in raw:
            continue
        if not fenced and raw in FENCE_OPEN:
            fenced = True
        elif fenced and raw == FENCE_CLOSE:
            fenced = False
        else:
            problems.append(f"malformed code fence on line {number}: {raw!r}")
    if fenced:
        problems.append("unclosed code fence")
    return problems


def claim_units(text):
    """Sentences outside code fences, and individual lines inside them."""
    units, fenced = [], False
    for raw in text.splitlines():
        line = raw.strip()
        if (not fenced and raw in FENCE_OPEN) or (fenced and raw == FENCE_CLOSE):
            fenced = not fenced
            continue
        if not line:
            continue
        if fenced:
            units.append(" ".join(line.split()))
            continue
        line = re.sub(r"^(#+|-|\d+\.)\s+", "", line)
        for sentence in re.split(r"(?<=[.!?])\s+(?=[A-Z`(\"])", line):
            sentence = " ".join(sentence.split())
            if sentence:
                units.append(sentence)
    return units


def published_claims(text):
    return claim_units(text)


def reviewed_claims(path=None):
    path = REVIEWED_CLAIMS if path is None else path
    lines = path.read_text(encoding="utf-8").splitlines()
    return [line for line in lines if line.strip() and not line.startswith("#")]


def factual_problems(text, reviewed=None):
    """Claims that are unreviewed, stale in the register, banned, or misstate the fee split."""
    reviewed = reviewed_claims() if reviewed is None else reviewed
    found = published_claims(text)
    problems = [f"unreviewed claim: {claim}" for claim in found if claim not in set(reviewed)]
    problems += [f"reviewed claim no longer published: {claim}" for claim in reviewed if claim not in set(found)]
    problems += fence_problems(text)
    # Bans and fee roles are checked on the whole published text, not on extracted
    # sentences, so no formatting can hide them from these checks.
    flat = " ".join(text.split())
    for match in FORBIDDEN.finditer(flat):
        problems.append(f"forbidden claim: {flat[max(0, match.start() - 60):match.end() + 60]}")
    for pattern, expected in FEE_ROLES:
        for match in pattern.finditer(flat):
            if match.group(1) != expected:
                problems.append(f"wrong fee split: {flat[max(0, match.start() - 60):match.end() + 60]}")
    for percent in sorted(set(re.findall(r"\b\d+(?:\.\d+)?%", text))):
        if percent not in ("1%", "3%"):
            problems.append(f"unexpected percentage: {percent}")
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
