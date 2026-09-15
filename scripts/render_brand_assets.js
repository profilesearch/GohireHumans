#!/usr/bin/env node
/* Regenerate the raster brand assets from the Broadsheet design tokens.
   Renders frontend/favicon.svg at the icon sizes the site references, packs the
   16 and 32 px renders into favicon.ico (PNG-in-ICO), and paints the Open Graph
   image from the same tokens and fonts as style.css.
   Usage: node scripts/render_brand_assets.js   (needs `cd frontend && npm ci` first) */
const fs = require('fs');
const path = require('path');
const ROOT = path.resolve(__dirname, '..');
const FRONTEND = path.join(ROOT, 'frontend');
const { chromium } = require(path.join(FRONTEND, 'node_modules', '@playwright/test'));

const ICON_SVG = fs.readFileSync(path.join(FRONTEND, 'favicon.svg'), 'utf8');
const FONTS = 'https://fonts.googleapis.com/css2?family=Instrument+Sans:wght@400..700&family=Newsreader:ital,opsz,wght@0,6..72,400..700;1,6..72,400..700&display=swap';

function iconHtml(size) {
  const svg = ICON_SVG.replace('width="32" height="32"', `width="${size}" height="${size}"`);
  return `<!doctype html><html><head><meta charset="utf-8"><style>html,body{margin:0;background:transparent}body{width:${size}px;height:${size}px;overflow:hidden}svg{display:block}</style></head><body>${svg}</body></html>`;
}

function ogHtml() {
  return `<!doctype html><html><head><meta charset="utf-8"><link rel="stylesheet" href="${FONTS}">
<style>
html,body{margin:0}
body{width:1200px;height:630px;background:#f4efe6;color:#1a1816;font-family:'Instrument Sans',system-ui,sans-serif;position:relative;overflow:hidden;-webkit-font-smoothing:antialiased}
.wrap{position:absolute;inset:0;padding:60px 72px 56px;display:flex;flex-direction:column}
.mast{display:flex;justify-content:space-between;align-items:baseline;border-bottom:2px solid #1a1816;padding-bottom:18px}
.wordmark{font-family:'Newsreader',Georgia,serif;font-weight:500;font-size:40px;letter-spacing:-0.02em;line-height:1}
.wordmark span{color:#b8321f}
.est{font-size:15px;font-weight:600;letter-spacing:0.14em;text-transform:uppercase}
.eyebrow{display:flex;align-items:center;gap:16px;margin-top:52px;color:#b8321f;font-size:18px;font-weight:600;letter-spacing:0.14em;text-transform:uppercase}
.eyebrow::before{content:'';width:36px;height:2px;background:#b8321f}
h1{font-family:'Newsreader',Georgia,serif;font-weight:400;font-size:104px;line-height:0.98;letter-spacing:-0.025em;margin:20px 0 0;max-width:980px}
.foot{margin-top:auto;border-top:1px solid #1a1816;padding-top:18px;display:flex;justify-content:space-between;gap:24px;font-size:19px;color:#6b645b}
.foot b{color:#1a1816;font-weight:600}
</style></head><body><div class="wrap">
<div class="mast"><div class="wordmark">GoHireHumans<span>.</span></div><div class="est">Est. 2026 &middot; United States &middot; Free to join</div></div>
<div class="eyebrow">A marketplace for small, scoped human help</div>
<h1>Some work still needs a person.</h1>
<div class="foot"><span><b>Workers</b> receive the listed payout</span><span><b>Employers</b> pay Stripe processing + 1%</span><span>gohirehumans.com</span></div>
</div></body></html>`;
}

async function shot(browser, html, width, height, out, omitBackground) {
  const page = await browser.newPage({ viewport: { width, height }, deviceScaleFactor: 1 });
  await page.setContent(html, { waitUntil: 'load' });
  await page.evaluate(() => document.fonts.ready);
  await page.waitForTimeout(400);
  const buf = await page.screenshot({ path: out, omitBackground, clip: { x: 0, y: 0, width, height } });
  await page.close();
  return buf;
}

function pngToIco(entries) {
  const header = Buffer.alloc(6);
  header.writeUInt16LE(0, 0); header.writeUInt16LE(1, 2); header.writeUInt16LE(entries.length, 4);
  const dir = []; const blobs = [];
  let offset = 6 + 16 * entries.length;
  for (const { size, data } of entries) {
    const e = Buffer.alloc(16);
    e.writeUInt8(size >= 256 ? 0 : size, 0); e.writeUInt8(size >= 256 ? 0 : size, 1);
    e.writeUInt8(0, 2); e.writeUInt8(0, 3); e.writeUInt16LE(1, 4); e.writeUInt16LE(32, 6);
    e.writeUInt32LE(data.length, 8); e.writeUInt32LE(offset, 12);
    dir.push(e); blobs.push(data); offset += data.length;
  }
  return Buffer.concat([header, ...dir, ...blobs]);
}

(async () => {
  const browser = await chromium.launch();
  const icons = [
    ['icon-512.png', 512], ['icon-192.png', 192], ['apple-touch-icon.png', 180], ['favicon-32.png', 32],
  ];
  for (const [name, size] of icons) {
    await shot(browser, iconHtml(size), size, size, path.join(FRONTEND, name), true);
    console.log('wrote', name);
  }
  const png16 = await shot(browser, iconHtml(16), 16, 16, path.join(ROOT, '.favicon-16.tmp.png'), true);
  const png32 = fs.readFileSync(path.join(FRONTEND, 'favicon-32.png'));
  fs.writeFileSync(path.join(FRONTEND, 'favicon.ico'), pngToIco([{ size: 16, data: png16 }, { size: 32, data: png32 }]));
  fs.unlinkSync(path.join(ROOT, '.favicon-16.tmp.png'));
  console.log('wrote favicon.ico');
  await shot(browser, ogHtml(), 1200, 630, path.join(FRONTEND, 'og-image-v2.png'), false);
  console.log('wrote og-image-v2.png');
  await browser.close();
})().catch((err) => { console.error(err); process.exit(1); });
