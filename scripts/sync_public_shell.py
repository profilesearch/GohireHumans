#!/usr/bin/env python3
"""Synchronize the canonical public nav and footer across source HTML pages.

The SPA shell in frontend/index.html is maintained separately because its links use
client-side navigation handlers. Run with --check in CI to detect any structural
nav, mobile-menu, CTA, or footer drift.
"""
from __future__ import annotations

import argparse
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
FRONTEND = ROOT / "frontend"
NAV_PATH = FRONTEND / "partials/public-nav.html"
FOOTER_PATH = FRONTEND / "partials/public-footer.html"
NAV = NAV_PATH.read_text(encoding="utf-8").strip()
FOOTER = FOOTER_PATH.read_text(encoding="utf-8").strip()
GENERATED_DIRS = {"node_modules", "test-results", "playwright-report", ".git", "dist", "build"}
NAV_MARKER = '<div class="lp-nav-wrap">'
MOBILE_MENU_MARKER = 'class="lp-mobile-menu"'
FOOTER_BLOCK = re.compile(r'<footer class="lp-footer".*?</footer>', re.DOTALL)
DIV_TAG = re.compile(r'</?div\b[^>]*>', re.IGNORECASE)
ANCHOR_TAG = re.compile(r'<a\b[^>]*>', re.IGNORECASE)


def balanced_div_bounds(text: str, marker: str = NAV_MARKER) -> tuple[int, int] | None:
    """Return bounds for the balanced div beginning at marker."""
    start = text.find(marker)
    if start < 0:
        return None
    depth = 0
    for match in DIV_TAG.finditer(text, start):
        if match.group(0).lower().startswith("</div"):
            depth -= 1
            if depth == 0:
                return start, match.end()
        else:
            depth += 1
    raise ValueError("Unbalanced public navigation wrapper")


def active_href(nav_block: str) -> str | None:
    for tag in ANCHOR_TAG.findall(nav_block):
        if 'aria-current="page"' not in tag:
            continue
        match = re.search(r'href="([^"]+)"', tag)
        if match:
            return match.group(1)
    return None


def canonical_nav(current_href: str | None) -> str:
    if not current_href:
        return NAV

    def mark_active(match: re.Match[str]) -> str:
        tag = match.group(0)
        if f'href="{current_href}"' not in tag:
            return tag
        if 'class="lp-nav-link"' in tag:
            tag = tag.replace('class="lp-nav-link"', 'class="lp-nav-link lp-nav-link-active"', 1)
        elif 'class="lp-mobile-link"' in tag:
            tag = tag.replace('class="lp-mobile-link"', 'class="lp-mobile-link lp-nav-link-active"', 1)
        if 'aria-current=' not in tag:
            tag = tag.replace(f'href="{current_href}"', f'aria-current="page" href="{current_href}"', 1)
        return tag

    return ANCHOR_TAG.sub(mark_active, NAV)


def transform(text: str) -> str:
    bounds = balanced_div_bounds(text)
    if bounds:
        start, end = bounds
        current = active_href(text[start:end])
        text = text[:start] + canonical_nav(current) + text[end:]
    if '<footer class="lp-footer"' in text:
        text = FOOTER_BLOCK.sub(FOOTER, text, count=1)
    return text


def source_html_pages() -> list[Path]:
    pages = []
    for path in FRONTEND.rglob("*.html"):
        relative_parts = path.relative_to(FRONTEND).parts
        if any(part in GENERATED_DIRS for part in relative_parts):
            continue
        if path == FRONTEND / "index.html":
            continue
        pages.append(path)
    return sorted(pages)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--check", action="store_true", help="report drift without writing")
    args = parser.parse_args()

    changed: list[str] = []
    for path in source_html_pages():
        original = path.read_text(encoding="utf-8", errors="strict")
        if NAV_MARKER not in original and '<footer class="lp-footer"' not in original:
            continue
        updated = transform(original)
        if updated != original:
            changed.append(str(path.relative_to(ROOT)))
            if not args.check:
                path.write_text(updated, encoding="utf-8")

    if args.check and changed:
        print("Public shell drift detected:")
        for rel in changed:
            print(f"- {rel}")
        return 1

    action = "would update" if args.check else "updated"
    print(f"Public shell {action} {len(changed)} static page(s)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
