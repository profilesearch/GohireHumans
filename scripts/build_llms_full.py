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


def first_sentence(text):
    text = " ".join(text.split())
    match = re.match(r"(.+?[.!?])(\s|$)", text)
    return match.group(1) if match else text


def faq_section():
    blocks = []
    for relative, questions in FAQ_PAGES.items():
        pairs = AnswerHTML((FRONTEND / relative).read_text(encoding="utf-8")).pairs
        if [q for q, _ in pairs] != questions:
            raise ValueError(f"{relative}: visible questions differ from FAQ_PAGES")
        for question, answer in pairs:
            blocks.append(f"### {question}\n\n{answer}\n\nSource: {SITE}{relative}")
    return "\n\n".join(blocks)


def tool_lines(tools):
    lines = []
    for tool in tools:
        description = first_sentence(tool.get("description") or "")
        if not tool.get("name") or not description:
            raise ValueError(f"MCP tool {tool.get('name')!r} has no name or description")
        lines.append(f"- `{tool['name']}`: {description}")
    return "\n".join(lines)


def resource_lines(resources):
    return "\n".join(f"- `{r['uri']}`: {first_sentence(r.get('description') or r['name'])}" for r in resources)


def render(template=None):
    text = TEMPLATE.read_text(encoding="utf-8") if template is None else template
    values = {
        "FAQ": faq_section,
        "TOOLS": lambda: tool_lines(_literal("TOOLS")),
        "RESOURCES": lambda: resource_lines(_literal("RESOURCES")),
    }
    for key, build in values.items():
        text = text.replace("{{" + key + "}}", build())
    leftover = re.findall(r"\{\{[A-Z_]+\}\}", text)
    if leftover:
        raise ValueError(f"unfilled template placeholders: {leftover}")
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
