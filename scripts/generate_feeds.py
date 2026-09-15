#!/usr/bin/env python3
"""Regenerate frontend/feed.xml and frontend/atom.xml from the blog pages' own metadata.

Each indexable post under frontend/blog/ contributes one item built from its <title>,
meta description, and JSON-LD datePublished. Posts marked noindex are skipped.
Run after adding or editing a blog post:

    python3 scripts/generate_feeds.py
"""
from __future__ import annotations

import html
import re
from datetime import date, datetime
from html.parser import HTMLParser
from pathlib import Path
from xml.sax.saxutils import escape

ROOT = Path(__file__).resolve().parents[1]
FRONTEND = ROOT / "frontend"
SITE = "https://www.gohirehumans.com"
DESC = (
    "Articles from GoHireHumans, a marketplace for small, scoped human work. Workers receive the "
    "listed payout; employers pay Stripe processing plus a 1% GoHireHumans fee where checkout is configured."
)


class RobotsMeta(HTMLParser):
    noindex = False

    def handle_starttag(self, tag, attrs):
        if tag.lower() != "meta":
            return
        a = {k.lower(): (v or "").lower() for k, v in attrs if k}
        if a.get("name") in {"robots", "googlebot"}:
            tokens = a.get("content", "").replace(",", " ").split()
            if "noindex" in tokens:
                self.noindex = True


def collect_posts() -> tuple[list[tuple[str, str, str, str]], list[str]]:
    posts, skipped = [], []
    for path in sorted((FRONTEND / "blog").glob("*.html")):
        if path.name == "index.html":
            continue
        text = path.read_text(encoding="utf-8", errors="ignore")
        robots = RobotsMeta()
        robots.feed(text)
        if robots.noindex:
            skipped.append(path.name)
            continue
        title_match = re.search(r"<title>(.*?)</title>", text, re.S)
        desc_match = re.search(r'<meta\s+name="description"\s+content="([^"]*)"', text) or re.search(
            r'<meta\s+content="([^"]*)"\s+name="description"', text
        )
        date_match = re.search(r'"datePublished"\s*:\s*"(\d{4}-\d{2}-\d{2})', text)
        if not (title_match and desc_match and date_match):
            raise SystemExit(f"{path.relative_to(ROOT)}: missing title, description, or datePublished")
        title = re.sub(r"\s*\|\s*GoHireHumans\s*$", "", html.unescape(title_match.group(1)).strip())
        posts.append((date_match.group(1), path.name, title, html.unescape(desc_match.group(1)).strip()))
    posts.sort(key=lambda item: (-int(item[0].replace("-", "")), item[1]))
    return posts, skipped


def rfc822(day: str) -> str:
    return datetime.strptime(day, "%Y-%m-%d").strftime("%a, %d %b %Y 00:00:00 +0000")


def iso(day: str) -> str:
    return f"{day}T00:00:00Z"


def main() -> int:
    posts, skipped = collect_posts()
    build_day = date.today().isoformat()

    rss = [
        '<?xml version="1.0" encoding="UTF-8"?>',
        '<rss version="2.0" xmlns:atom="http://www.w3.org/2005/Atom">',
        "  <channel>",
        "    <title>GoHireHumans Blog</title>",
        f"    <link>{SITE}/blog/</link>",
        f'    <atom:link href="{SITE}/feed.xml" rel="self" type="application/rss+xml" />',
        f"    <description>{escape(DESC)}</description>",
        "    <language>en-us</language>",
        f"    <lastBuildDate>{rfc822(build_day)}</lastBuildDate>",
        "    <ttl>1440</ttl>",
    ]
    for day, name, title, desc in posts:
        url = f"{SITE}/blog/{name}"
        rss += [
            "    <item>",
            f"      <title>{escape(title)}</title>",
            f"      <link>{url}</link>",
            f'      <guid isPermaLink="true">{url}</guid>',
            f"      <description>{escape(desc)}</description>",
            f"      <pubDate>{rfc822(day)}</pubDate>",
            "    </item>",
        ]
    rss += ["  </channel>", "</rss>"]

    atom = [
        '<?xml version="1.0" encoding="UTF-8"?>',
        '<feed xmlns="http://www.w3.org/2005/Atom">',
        "  <title>GoHireHumans Blog</title>",
        f'  <link href="{SITE}/blog/" />',
        f'  <link rel="self" href="{SITE}/atom.xml" />',
        f"  <updated>{iso(build_day)}</updated>",
        f"  <id>{SITE}/blog/</id>",
        f"  <subtitle>{escape(DESC)}</subtitle>",
        "  <author><name>GoHireHumans Team</name></author>",
    ]
    for day, name, title, desc in posts:
        url = f"{SITE}/blog/{name}"
        atom += [
            "  <entry>",
            f"    <title>{escape(title)}</title>",
            f'    <link href="{url}" />',
            f"    <id>{url}</id>",
            f"    <updated>{iso(day)}</updated>",
            f"    <published>{iso(day)}</published>",
            f"    <summary>{escape(desc)}</summary>",
            "    <author><name>GoHireHumans Team</name></author>",
            "  </entry>",
        ]
    atom.append("</feed>")

    (FRONTEND / "feed.xml").write_text("\n".join(rss) + "\n", encoding="utf-8")
    (FRONTEND / "atom.xml").write_text("\n".join(atom) + "\n", encoding="utf-8")
    print(f"posts in feeds: {len(posts)}; skipped (noindex): {skipped or 'none'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
