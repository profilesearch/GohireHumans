// Listings whose seller has not finished payout setup stay visible, but cannot be ordered.
const { test, expect } = require('@playwright/test');

const base = {
  worker_name: 'Pat', description: 'Careful proofreading of one short document.', category: 'writing',
  provider_type: 'human', pricing_type: 'fixed', price: 25, status: 'active', delivery_time_days: 3,
};
const ready = { ...base, id: 501, worker_id: 7, title: 'Ready proofreading', accepting_orders: true };
const waiting = { ...base, id: 502, worker_id: 8, title: 'Waiting proofreading', accepting_orders: false };
const ownerNotice = 'Buyers cannot order this listing until you finish payout setup. Setup is free and nothing is charged: open Payments and connect your bank account.';

async function stub(page, viewer, saved = null) {
  const errors = [], writes = [], bodies = [], quotes = [];
  page.on('pageerror', e => errors.push(e.message));
  await page.addInitScript(([v, rec]) => {
    window.GOHIREHUMANS_API_URL = location.origin + '/fixture';
    if (v) {
      sessionStorage.setItem('ghh_token', 'fixture-' + v);
      localStorage.setItem('ghh_user', JSON.stringify({ id: v === 'seller' ? 8 : 9, name: v }));
    }
    if (rec && !sessionStorage.getItem('saved-seeded')) {
      sessionStorage.setItem('saved-seeded', '1');
      localStorage.setItem(`ghh_service_checkout_v1:${rec.buyer_id}:${rec.service_id}`, JSON.stringify(rec));
    }
  }, [viewer, saved]);
  await page.route('**/*', async route => {
    const req = route.request();
    const url = new URL(req.url());
    if (url.origin !== `http://127.0.0.1:${process.env.PW_PORT || 4173}`) return route.abort();
    if (saved && req.method() === 'POST' && url.pathname === '/fixture/services/502/order') {
      bodies.push(req.postDataJSON());
      return route.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify({ id: 93 }) });
    }
    if (req.method() !== 'GET') { writes.push(url.pathname); return route.abort(); }
    if (url.pathname.endsWith('/quote')) quotes.push(url.pathname);
    if (url.pathname === '/config.js') return route.fulfill({ contentType: 'application/javascript', body: "window.GOHIREHUMANS_API_URL=location.origin+'/fixture';" });
    if (url.pathname.startsWith('/fixture')) {
      const path = url.pathname.slice('/fixture'.length);
      const mine = { ...waiting, order_notice: ownerNotice };
      const data = path === '/services/501' ? ready
        : path === '/services/502' ? (viewer === 'seller' ? mine : waiting)
        : path === '/me/services' ? { services: [mine], total: 1, total_pages: 1 }
        : path === '/services' ? { services: [ready, waiting], total: 2, total_pages: 1 }
        : path === '/profile' ? { id: viewer === 'seller' ? 8 : 9 }
        : { categories: [], reviews: [], services: [], jobs: [] };
      return route.fulfill({ contentType: 'application/json', body: JSON.stringify(data) });
    }
    return route.continue();
  });
  return { errors, writes, bodies, quotes };
}

test('browse labels only the listing that is not taking orders', async ({ page }) => {
  const { errors, writes } = await stub(page, null);
  await page.goto('/#/services');
  const cards = page.locator('.svc-card');
  await expect(cards).toHaveCount(2);
  await expect(cards.filter({ hasText: 'Waiting proofreading' })).toContainText('Not taking orders yet');
  await expect(cards.filter({ hasText: 'Ready proofreading' })).not.toContainText('Not taking orders yet');
  // The badge must sit fully inside its card (it was clipped at 3 cards per row on desktop).
  const box = await cards.filter({ hasText: 'Waiting proofreading' }).boundingBox();
  const badge = await cards.filter({ hasText: 'Waiting proofreading' }).getByText('Not taking orders yet').boundingBox();
  expect(badge.x + badge.width).toBeLessThanOrEqual(box.x + box.width + 0.5);
  expect(badge.x).toBeGreaterThanOrEqual(box.x - 0.5);
  expect(errors).toEqual([]);
  expect(writes).toEqual([]);
});

for (const viewer of [null, 'buyer']) {
  test(`detail has no Order button for a seller without payout setup (${viewer || 'signed out'})`, async ({ page }) => {
    const { errors, writes } = await stub(page, viewer);
    await page.goto('/#/services/502');
    await expect(page.locator('.svc-order-card')).toContainText('Not taking orders yet. The seller has not finished payout setup.');
    await expect(page.getByRole('button', { name: 'Order This Service' })).toHaveCount(0);
    await expect(page.locator('.detail-page')).not.toContainText(ownerNotice);
    await page.goto('/#/services/501');
    await expect(page.getByRole('button', { name: 'Order This Service' })).toBeVisible();
    await expect(page.locator('.svc-order-card')).not.toContainText('Not taking orders yet');
    expect(errors).toEqual([]);
    expect(writes).toEqual([]);
  });
}

test('seller sees why buyers cannot order, with a link to Payments', async ({ page }) => {
  const { errors, writes } = await stub(page, 'seller');
  await page.goto('/#/my-services');
  const card = page.locator('.task-card');
  await expect(card).toContainText(ownerNotice);
  await expect(card.getByRole('link', { name: 'Open Payments' })).toHaveAttribute('href', '#/payments');
  await page.goto('/#/services/502');
  await expect(page.locator('.svc-order-card')).toContainText('Buyers can order it once you finish free payout setup in Payments.');
  await page.goto('/#/edit-service/502');
  await expect(page.locator('.form-page')).toContainText(ownerNotice);
  await page.goto('/#/post-service');
  await expect(page.locator('.form-page')).toContainText('Buyers can order your listing once you finish free payout setup in Payments.');
  expect(errors).toEqual([]);
  expect(writes).toEqual([]);
});

test('a buyer can still finish a saved checkout after the seller stops taking orders', async ({ page }) => {
  // Response loss: the order may already exist. Only the exact saved request may be retried.
  const quote = { service_id: 502, pricing_type: 'fixed', currency: 'usd', hours: null, base_amount_cents: 2500,
    processing_fee_cents: 75, platform_fee_cents: 25, total_charge_cents: 2600, quote_token: 'b'.repeat(64) };
  const payload = { notes: 'Saved notes', quote_token: quote.quote_token, idempotency_key: 'service-order-0123456789abcdef' };
  const saved = { version: 1, buyer_id: '9', service_id: '502', title: 'Waiting proofreading', quote, payload };
  const { errors, writes, bodies, quotes } = await stub(page, 'buyer', saved);
  await page.goto('/#/services/502');
  await expect(page.getByRole('button', { name: 'Order This Service' })).toHaveCount(0);
  await page.getByRole('button', { name: 'Resume saved order' }).click();
  await page.getByRole('button', { name: 'Place Order', exact: true }).click();
  await expect.poll(() => bodies.length).toBe(1);
  expect(bodies[0]).toEqual(payload);
  expect(quotes).toEqual([]);
  expect(errors).toEqual([]);
  expect(writes).toEqual([]);
});
