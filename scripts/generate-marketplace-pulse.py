#!/usr/bin/env python3
"""
Weekly Marketplace Pulse generator.

Run weekly (e.g. via launchd, cron, or GitHub Actions schedule):
  python3 scripts/generate-marketplace-pulse.py

What it does:
1. Fetches /platform/stats, /services, /jobs from the live API.
2. Generates a static HTML blog post at frontend/blog/marketplace-pulse-YYYY-WW.html
3. Updates frontend/blog/index.html to surface the new post at the top.
4. Updates frontend/sitemap.xml with the new URL.
5. Stages everything (caller is responsible for committing + pushing).

This produces ~52 fresh, indexable blog posts per year — Google rewards freshness.
"""
import json
import os
import sys
from datetime import datetime, timezone, timedelta
from urllib.request import urlopen, Request
from urllib.error import URLError

# ── Config ─────────────────────────────────────────────────────────────────
API = 'https://gohirehumans-production.up.railway.app'
SITE = 'https://www.gohirehumans.com'
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
BLOG_DIR = os.path.join(ROOT, 'frontend', 'blog')
SITEMAP = os.path.join(ROOT, 'frontend', 'sitemap.xml')

now = datetime.now(timezone.utc)
year, week, _ = now.isocalendar()
SLUG = f"marketplace-pulse-{year}-w{week:02d}"
DATE_DISPLAY = now.strftime('%B %d, %Y')
DATE_ISO = now.strftime('%Y-%m-%d')


def fetch(url):
    try:
        req = Request(url, headers={'User-Agent': 'GoHireHumans-Pulse/1.0'})
        with urlopen(req, timeout=15) as r:
            return json.loads(r.read())
    except (URLError, json.JSONDecodeError, TimeoutError) as e:
        print(f"WARN: {url} failed: {e}", file=sys.stderr)
        return None


# ── Pull data ──────────────────────────────────────────────────────────────
stats = fetch(f"{API}/platform/stats") or {}
services_data = fetch(f"{API}/services?limit=200") or {'services': []}
jobs_data = fetch(f"{API}/jobs?limit=200") or {'jobs': []}

services = services_data.get('services', [])
jobs = jobs_data.get('jobs', [])


def title_case(s):
    return (s or '').replace('_', ' ').title()


# Compute category histograms
def hist(items, key='category'):
    h = {}
    for it in items:
        k = it.get(key) or 'unknown'
        h[k] = h.get(k, 0) + 1
    return sorted(h.items(), key=lambda x: -x[1])


service_hist = hist(services)
job_hist = hist(jobs)

# Average price per category (services with fixed pricing only)
def avg_price():
    out = {}
    for s in services:
        if s.get('pricing_type') == 'fixed' and s.get('price'):
            cat = s.get('category', 'unknown')
            out.setdefault(cat, []).append(float(s['price']))
    return {k: sum(v) / len(v) for k, v in out.items() if v}


prices = avg_price()


# ── Build prose summary ────────────────────────────────────────────────────
def fmt(n):
    return f"{int(n):,}"


def fmt_money(n):
    return f"${int(n):,}"


top3_services = service_hist[:3]
top3_jobs = job_hist[:3]

s_total = stats.get('services_listed', len(services))
j_total = stats.get('open_jobs', len(jobs))
u_total = stats.get('total_users', 0)

intro_lines = [
    f"This is week {week} of 2026 on GoHireHumans. Here's what the marketplace looks like right now, pulled live from the public API.",
]

if top3_services:
    s_summary = ", ".join(f"{title_case(c)} ({n})" for c, n in top3_services)
    intro_lines.append(f"The most active service categories: {s_summary}.")
if top3_jobs:
    j_summary = ", ".join(f"{title_case(c)} ({n})" for c, n in top3_jobs)
    intro_lines.append(f"The most-posted job categories: {j_summary}.")


# Compose body sections
def table_rows(items, value_label='Listings'):
    rows = []
    for cat, n in items[:10]:
        rows.append(f'<tr><td>{title_case(cat)}</td><td style="text-align:right;font-family:JetBrains Mono,monospace">{fmt(n)}</td></tr>')
    return '\n              '.join(rows)


def price_rows():
    if not prices:
        return '<tr><td colspan="2" style="text-align:center;color:#a8a6a0">No fixed-price services listed this week.</td></tr>'
    rows = []
    for cat, avg in sorted(prices.items(), key=lambda x: -x[1])[:8]:
        rows.append(f'<tr><td>{title_case(cat)}</td><td style="text-align:right;font-family:JetBrains Mono,monospace">${avg:,.0f}</td></tr>')
    return '\n              '.join(rows)


# ── Render HTML ────────────────────────────────────────────────────────────
title = f"Marketplace Pulse — Week {week}, 2026"
description = f"Weekly snapshot of the GoHireHumans marketplace: {fmt(s_total)} services across {len(service_hist)} categories, {fmt(j_total)} open jobs, {fmt(u_total)} registered users."
canonical = f"{SITE}/blog/{SLUG}.html"

html = f'''<!DOCTYPE html>
<html lang="en">
<head>
  <script async src="https://www.googletagmanager.com/gtag/js?id=G-KM69M3NES8"></script>
  <script>window.dataLayer=window.dataLayer||[];function gtag(){{dataLayer.push(arguments);}}gtag('js',new Date());gtag('config','G-KM69M3NES8');</script>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>{title} | GoHireHumans</title>
  <meta name="description" content="{description}">
  <meta property="og:title" content="{title} | GoHireHumans">
  <meta property="og:description" content="{description}">
  <meta property="og:image" content="{SITE}/og-image.png">
  <meta property="og:url" content="{canonical}">
  <meta property="og:type" content="article">
  <meta property="og:site_name" content="GoHireHumans">
  <meta name="twitter:card" content="summary_large_image">
  <meta name="twitter:title" content="{title}">
  <meta name="twitter:description" content="{description}">
  <meta name="twitter:image" content="{SITE}/og-image.png">
  <link rel="canonical" href="{canonical}">
  <link rel="alternate" type="application/rss+xml" title="GoHireHumans Blog" href="/feed.xml">
  <script type="application/ld+json">
  {{"@context":"https://schema.org","@type":"Article","headline":"{title}","description":"{description}","datePublished":"{DATE_ISO}","dateModified":"{DATE_ISO}","author":{{"@type":"Organization","name":"GoHireHumans Team"}},"publisher":{{"@type":"Organization","name":"GoHireHumans","logo":{{"@type":"ImageObject","url":"{SITE}/og-image.png"}}}},"mainEntityOfPage":"{canonical}","image":"{SITE}/og-image.png"}}
  </script>
  <script type="application/ld+json">
  {{"@context":"https://schema.org","@type":"BreadcrumbList","itemListElement":[{{"@type":"ListItem","position":1,"name":"Home","item":"{SITE}/"}},{{"@type":"ListItem","position":2,"name":"Blog","item":"{SITE}/blog/"}},{{"@type":"ListItem","position":3,"name":"{title}","item":"{canonical}"}}]}}
  </script>
  <link rel="preconnect" href="https://fonts.googleapis.com">
  <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
  <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&family=JetBrains+Mono:wght@500&display=swap" rel="stylesheet">
  <style>
    *,*::before,*::after{{box-sizing:border-box;margin:0;padding:0}}
    :root{{--font:'Inter',system-ui,sans-serif;--bg:#f5f4f0;--surface:#fff;--divider:#dddbd6;--text:#1a1816;--muted:#6b6963;--faint:#a8a6a0;--primary:#0d7377;--radius:.5rem}}
    body{{font-family:var(--font);background:var(--bg);color:var(--text);line-height:1.7}}
    .header{{position:sticky;top:0;z-index:50;background:rgba(245,244,240,.92);backdrop-filter:blur(16px);border-bottom:1px solid var(--divider)}}
    .header-inner{{max-width:1200px;margin:0 auto;padding:0 1.5rem;height:56px;display:flex;align-items:center;justify-content:space-between}}
    .logo{{display:flex;align-items:center;gap:.5rem;font-weight:700;font-size:.9375rem;color:var(--text);text-decoration:none}}
    .logo-icon{{width:28px;height:28px;background:var(--primary);border-radius:.375rem;display:flex;align-items:center;justify-content:center}}
    .logo-icon svg{{width:16px;height:16px}}
    main{{max-width:760px;margin:0 auto;padding:3rem 1.5rem 4rem}}
    .breadcrumbs{{font-size:.75rem;color:var(--faint);margin-bottom:1rem}}
    .breadcrumbs a{{color:var(--muted);text-decoration:none}}
    .meta{{font-size:.75rem;color:var(--primary);font-weight:700;text-transform:uppercase;letter-spacing:.06em;margin-bottom:.75rem}}
    h1{{font-size:clamp(1.875rem,4.5vw,2.5rem);font-weight:800;letter-spacing:-.025em;line-height:1.1;margin-bottom:1rem}}
    .info{{display:flex;gap:1rem;font-size:.8125rem;color:var(--muted);margin-bottom:2rem}}
    .info span{{display:flex;align-items:center;gap:.375rem}}
    h2{{font-size:1.375rem;font-weight:700;letter-spacing:-.01em;margin-top:2.5rem;margin-bottom:1rem}}
    p{{margin-bottom:1rem}}
    table{{width:100%;border-collapse:collapse;background:var(--surface);border:1px solid var(--divider);border-radius:var(--radius);overflow:hidden;margin-bottom:1.5rem;font-size:.9375rem}}
    th,td{{padding:.625rem .875rem;border-bottom:1px solid var(--divider);text-align:left}}
    th{{font-size:.75rem;font-weight:700;text-transform:uppercase;letter-spacing:.06em;color:var(--muted);background:#faf9f6}}
    tbody tr:last-child td{{border-bottom:none}}
    .stat-strip{{display:grid;grid-template-columns:repeat(auto-fit,minmax(120px,1fr));gap:.75rem;margin-bottom:2rem}}
    .stat{{padding:1rem;background:var(--surface);border:1px solid var(--divider);border-radius:var(--radius);text-align:center}}
    .stat-num{{font-family:'JetBrains Mono',monospace;font-size:1.75rem;font-weight:700;color:var(--primary);line-height:1}}
    .stat-label{{font-size:.6875rem;font-weight:600;text-transform:uppercase;letter-spacing:.06em;color:var(--muted);margin-top:.375rem}}
    .cta{{padding:1.5rem;background:var(--primary);color:#fff;border-radius:var(--radius);text-align:center;margin-top:2rem}}
    .cta a{{color:#fff;font-weight:600;text-decoration:none}}
  </style>
</head>
<body>
  <header class="header">
    <div class="header-inner">
      <a href="/" class="logo">
        <div class="logo-icon"><svg viewBox="0 0 24 24" fill="none" stroke="white" stroke-width="2.5" stroke-linecap="round"><path d="M17 21v-2a4 4 0 0 0-4-4H5a4 4 0 0 0-4 4v2"/><circle cx="9" cy="7" r="4"/><path d="M23 21v-2a4 4 0 0 0-3-3.87"/><path d="M16 3.13a4 4 0 0 1 0 7.75"/></svg></div>
        GoHireHumans
      </a>
    </div>
  </header>

  <main>
    <div class="breadcrumbs"><a href="/">Home</a> &rarr; <a href="/blog/">Blog</a> &rarr; <span>Marketplace Pulse W{week}</span></div>
    <div class="meta">Weekly Pulse · Week {week} of 2026</div>
    <h1>{title}</h1>
    <div class="info"><span>📅 {DATE_DISPLAY}</span><span>📊 Auto-generated from API</span></div>

    <p>{intro_lines[0]}</p>

    <div class="stat-strip">
      <div class="stat"><div class="stat-num">{fmt(s_total)}</div><div class="stat-label">Services</div></div>
      <div class="stat"><div class="stat-num">{fmt(j_total)}</div><div class="stat-label">Open Jobs</div></div>
      <div class="stat"><div class="stat-num">{fmt(u_total)}</div><div class="stat-label">Users</div></div>
      <div class="stat"><div class="stat-num">{fmt(len(service_hist))}</div><div class="stat-label">Categories</div></div>
    </div>

    <h2>Top service categories</h2>
    {f"<p>{intro_lines[1]}</p>" if len(intro_lines) > 1 else ""}
    <table>
      <thead><tr><th>Category</th><th style="text-align:right">Listings</th></tr></thead>
      <tbody>{table_rows(service_hist)}</tbody>
    </table>

    <h2>Top job categories (active)</h2>
    {f"<p>{intro_lines[2]}</p>" if len(intro_lines) > 2 else ""}
    <table>
      <thead><tr><th>Category</th><th style="text-align:right">Open</th></tr></thead>
      <tbody>{table_rows(job_hist, 'Open')}</tbody>
    </table>

    <h2>Average fixed price by category</h2>
    <p>For services priced fixed (not hourly or custom), here's the current average price per category. Useful as a benchmark if you're listing a service or planning a project budget.</p>
    <table>
      <thead><tr><th>Category</th><th style="text-align:right">Avg fixed price</th></tr></thead>
      <tbody>{price_rows()}</tbody>
    </table>

    <h2>What this means</h2>
    <p>This snapshot is auto-generated weekly from the public API. The live numbers change every time someone posts a service or a job — see <a href="/stats.html" style="color:var(--primary)">/stats</a> for the most current view. Past pulses live in the <a href="/blog/" style="color:var(--primary)">blog</a>.</p>

    <div class="cta">
      <a href="/#/register">List your service or post a job — free →</a>
    </div>
  </main>
</body>
</html>
'''

# Write the file
out_path = os.path.join(BLOG_DIR, f"{SLUG}.html")
with open(out_path, 'w') as f:
    f.write(html)
print(f"Wrote {out_path}")

# ── Update sitemap.xml ─────────────────────────────────────────────────────
sitemap = open(SITEMAP).read()
new_url = f'  <url><loc>{canonical}</loc><lastmod>{DATE_ISO}</lastmod><priority>0.7</priority></url>\n'
if new_url not in sitemap:
    # Insert before </urlset>
    sitemap = sitemap.replace('</urlset>', new_url + '</urlset>')
    open(SITEMAP, 'w').write(sitemap)
    print(f"Updated {SITEMAP}")

# ── Try to add to blog index ───────────────────────────────────────────────
blog_index = os.path.join(BLOG_DIR, 'index.html')
idx = open(blog_index).read()
card = f'''
      <!-- Auto-added: Weekly Pulse W{week} -->
      <a href="/blog/{SLUG}.html" class="blog-card">
        <div class="blog-card-thumb" style="background:linear-gradient(135deg, #0d7377 0%, #1e1b4b 100%)"></div>
        <div class="blog-card-body">
          <div class="blog-card-category">Weekly Pulse</div>
          <h2 class="blog-card-title">{title}</h2>
          <p class="blog-card-desc">{description}</p>
          <div class="blog-card-meta"><span>4 min</span><span>{DATE_DISPLAY}</span></div>
        </div>
      </a>
'''
if SLUG not in idx and '<div class="blog-grid">' in idx:
    idx = idx.replace('<div class="blog-grid">', '<div class="blog-grid">\n' + card.rstrip(), 1)
    open(blog_index, 'w').write(idx)
    print(f"Updated {blog_index}")

print(f"\nMarketplace Pulse W{week} of {year} generated. {fmt(s_total)} services, {fmt(j_total)} jobs, {fmt(u_total)} users.")
print("Run: cd <repo-root> && git add frontend/blog/{}.html frontend/blog/index.html frontend/sitemap.xml && git commit -m 'pulse: week {} {}' && git push".format(SLUG, week, year))
