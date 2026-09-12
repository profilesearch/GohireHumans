const { test, expect } = require('@playwright/test');
const fs = require('node:fs');
const path = require('node:path');
const root = path.resolve(__dirname, '..');
const evidenceURL = 'https://evidence.example.org/reports/' + 'verifiedsourcereference'.repeat(12);

async function openService(page) {
  const writes = [], orders = [], errors = [];
  page.on('pageerror', error => errors.push(error.message));
  await page.addInitScript(() => { window.GOHIREHUMANS_API_URL = location.origin + '/fixture'; });
  await page.route('**/*', async route => {
    const req = route.request(), url = new URL(req.url());
    if (!['GET', 'HEAD'].includes(req.method())) { writes.push(req.url()); return route.abort(); }
    if (/\/order(?:s|\/|$)|\/quote(?:\?|$)/.test(url.pathname)) { orders.push(req.url()); return route.abort(); }
    // No external traffic, including GA/tag-manager bootstrap or collection.
    if (url.origin !== 'http://127.0.0.1:4173' || /google-analytics|googletagmanager|\/collect/.test(req.url())) return route.abort();
    if (url.pathname === '/config.js') return route.fulfill({ contentType: 'application/javascript', body: "window.GOHIREHUMANS_API_URL=location.origin+'/fixture';" });
    if (url.pathname.startsWith('/fixture')) {
      const data = url.pathname === '/fixture/services/321' ? {
        id: 321, title: 'Research and verify supplier evidence',
        description: 'I will research your shortlisted suppliers, verify public claims, and deliver a concise evidence report.\nExample evidence reference: ' + evidenceURL + '\nIncludes a written summary and source checks.',
        includes: 'A reviewed report, source links, and one revision.',
        worker_name: 'Evidence Research Studio', worker_id: 8, worker_bio: 'Independent researcher for small businesses.',
        pricing_type: 'fixed', price: 75, provider_type: 'human', category: 'research', delivery_time_days: 3,
      } : { reviews: [], categories: [], services: [], jobs: [] };
      return route.fulfill({ contentType: 'application/json', body: JSON.stringify(data) });
    }
    const target = path.resolve(root, '.' + (url.pathname === '/' ? '/index.html' : url.pathname));
    if (!target.startsWith(root + path.sep) || !fs.existsSync(target) || !fs.statSync(target).isFile()) return route.fulfill({ status: 404, body: '' });
    return route.fulfill({ contentType: target.endsWith('.html') ? 'text/html' : target.endsWith('.css') ? 'text/css' : 'application/javascript', body: fs.readFileSync(target) });
  });
  await page.goto('/#/services/321');
  await expect(page.getByRole('button', { name: 'Order This Service', exact: true })).toBeVisible();
  await page.evaluate(() => document.fonts.ready);
  return { writes, orders, errors };
}

for (const width of [390, 320, 1280]) {
  test(`service evidence and order control fit at ${width}px without ordering`, async ({ page }, testInfo) => {
    await page.setViewportSize({ width, height: 900 });
    const safety = await openService(page);
    const selectors = ['.detail-body', '.detail-content', '.detail-sidebar', '.svc-order-card', '.lp-nav'];
    const boxes = {};
    for (const selector of selectors) {
      const box = await page.locator(selector).first().boundingBox();
      boxes[selector] = box;
      expect.soft(box.x, selector + ' left').toBeGreaterThanOrEqual(0);
      expect.soft(box.x + box.width, selector + ' right').toBeLessThanOrEqual(width + 1);
    }
    if (width < 768) {
      // A clipped document can report a perfect scrollWidth while hiding real content.
      const textRects = await page.locator('.detail-content .card').first().evaluate(card => {
        const range = document.createRange();
        range.selectNodeContents(card.lastElementChild);
        return [...range.getClientRects()].map(r => ({ left: r.left, right: r.right }));
      });
      for (const rect of textRects) {
        expect.soft(rect.left).toBeGreaterThanOrEqual(0);
        expect.soft(rect.right).toBeLessThanOrEqual(width + 1);
      }
    } else {
      // Mobile hardening must not change desktop geometry, including navigation.
      await page.locator('link[href*="mobile-hardening.css"]').evaluate(link => { link.disabled = true; });
      for (const selector of selectors) expect(await page.locator(selector).first().boundingBox()).toEqual(boxes[selector]);
    }
    const button = page.getByRole('button', { name: 'Order This Service', exact: true });
    await button.scrollIntoViewIfNeeded();
    const buttonBox = await button.boundingBox();
    expect.soft(buttonBox.x).toBeGreaterThanOrEqual(0);
    expect.soft(buttonBox.x + buttonBox.width).toBeLessThanOrEqual(width + 1);
    expect.soft(buttonBox.y).toBeGreaterThanOrEqual(0);
    expect.soft(buttonBox.y + buttonBox.height).toBeLessThanOrEqual(901);
    await expect(page.locator('[data-service-checkout]')).toHaveCount(0);
    expect(safety).toEqual({ writes: [], orders: [], errors: [] });
    await testInfo.attach('element-bounds', { body: JSON.stringify({ width, boxes, buttonBox, safety }, null, 2), contentType: 'application/json' });
  });
}
