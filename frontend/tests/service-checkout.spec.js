const { test, expect } = require('@playwright/test');
const fs = require('node:fs');
const path = require('node:path');
const root = path.join(__dirname, '..');
const quote = { service_id: 321, pricing_type: 'hourly', currency: 'usd', hours: '2.5', base_amount_cents: 2555, processing_fee_cents: 104, platform_fee_cents: 26, total_charge_cents: 2685, quote_token: 'a'.repeat(64) };
async function fixture(page, options = {}) {
  const evidence = { bodies: [], quotes: [], profiles: [], external: [], errors: [] };
  page.on('pageerror', e => evidence.errors.push(e.message));
  await page.addInitScript(() => {
    window.GOHIREHUMANS_API_URL = location.origin + '/fixture';
    if (!sessionStorage.getItem('fixture-initialized')) {
      sessionStorage.setItem('fixture-initialized', '1');
      sessionStorage.setItem('ghh_token', 'fixture-buyer-2');
      localStorage.setItem('ghh_user', JSON.stringify({ id: 2, name: 'Fixture buyer' }));
    }
  });
  await page.route('**/*', async route => {
    const req = route.request(), url = new URL(req.url());
    if (url.origin !== 'http://127.0.0.1:4173') { evidence.external.push(req.url()); return route.abort(); }
    if (url.pathname === '/config.js') return route.fulfill({ contentType: 'application/javascript', body: "window.GOHIREHUMANS_API_URL=location.origin+'/fixture';" });
    if (url.pathname.startsWith('/fixture')) {
      const p = url.pathname.slice('/fixture'.length);
      const send = (data, status = 200) => route.fulfill({ status, contentType: 'application/json', body: JSON.stringify(data) });
      if (p === '/services/321/quote') {
        evidence.quotes.push(url.search);
        if (options.onQuote) return options.onQuote({ route, send, url, evidence });
        return send({ ...quote, pricing_type: options.type || 'hourly', hours: (options.type && options.type !== 'hourly') ? null : url.searchParams.get('hours') });
      }
      if (p === '/services/321/order') {
        evidence.bodies.push({ body: req.postDataJSON(), auth: req.headers().authorization });
        if (options.onOrder) return options.onOrder({ route, send, evidence });
        return send({ id: 93 }, 201);
      }
      if (req.method() !== 'GET') throw new Error('Unexpected fixture mutation ' + p);
      if (p === '/profile') {
        const auth = req.headers().authorization;
        evidence.profiles.push(auth);
        if (options.onProfile) return options.onProfile({ route, send, evidence });
        const id = { 'Bearer fixture-buyer-2': 2, 'Bearer other-token': 3 }[auth];
        return id ? send({ id, name: `Buyer ${id}` }) : send({ error: 'Unauthenticated' }, 401);
      }
      if (p === '/payments/status') return send({ employer_ready: true });
      if (p === '/services/321') return send({ id: 321, title: '<img src=x onerror=alert(1)> Review', description: 'Fixture only', worker_name: 'Seller', pricing_type: options.type || 'hourly', price: 25.55, hourly_rate: 10.22, provider_type: 'human', worker_id: 8 });
      if (p === '/orders/93') return send({ id: 93, status: 'in_progress', employer_id: 2, worker_id: 8, milestones: [] });
      return send({ categories: [], services: [], jobs: [], orders: [] });
    }
    const target = path.join(root, url.pathname === '/' ? 'index.html' : url.pathname);
    if (!target.startsWith(root + '/') || !fs.existsSync(target) || !fs.statSync(target).isFile()) return route.fulfill({ status: 404, body: '' });
    let body = fs.readFileSync(target);
    if (target.endsWith('index.html')) body = body.toString().replace(/<script\b[^>]*src=["']https?:[^>]*>[\s\S]*?<\/script>/gi, '');
    return route.fulfill({ contentType: target.endsWith('.html') ? 'text/html' : target.endsWith('.css') ? 'text/css' : 'application/javascript', body });
  });
  await page.goto('/#/services/321');
  await expect(page.getByRole('button', { name: 'Order This Service' })).toBeVisible();
  return evidence;
}
async function open(page) { await page.getByRole('button', { name: 'Order This Service' }).click(); }
async function review(page, hours = '2.5', notes = 'Buyer notes <b>literal</b> & exact') {
  await open(page);
  await expect(page.getByLabel('Order notes')).toBeVisible();
  if (await page.getByLabel('Hours').count()) await page.getByLabel('Hours').fill(hours);
  await page.getByLabel('Order notes').fill(notes);
  await page.getByRole('button', { name: 'Review quote', exact: true }).click();
  await expect(page.getByRole('button', { name: 'Place Order', exact: true })).toBeVisible();
}
test('authoritative hourly cents and explicit notes are the confirmed command', async ({ page }) => {
  const e = await fixture(page);
  await review(page);
  const modal = page.locator('[data-service-checkout]');
  await expect(modal).toContainText('USD $25.55');
  await expect(modal).toContainText('Stripe processing');
  await expect(modal).toContainText('USD $1.04');
  await expect(modal).toContainText('GoHireHumans fee');
  await expect(modal).toContainText('USD $0.26');
  await expect(modal).toContainText('USD $26.85');
  await expect(modal).toContainText('2.5');
  await expect(modal).toContainText('Buyer notes <b>literal</b> & exact');
  await expect(modal.locator('img, b')).toHaveCount(0);
  await page.getByRole('button', { name: 'Place Order', exact: true }).click();
  await expect.poll(() => e.bodies.length).toBe(1);
  expect(e.bodies[0].body).toEqual({ hours: '2.5', notes: 'Buyer notes <b>literal</b> & exact', quote_token: quote.quote_token, idempotency_key: expect.stringMatching(/^service-order-[\w-]+$/) });
  expect(e.quotes).toEqual(['?hours=2.5']);
  expect(e.errors).toEqual([]);
  expect(e.external.filter(u => /analytics|googletagmanager/.test(u))).toEqual([]);
});

test('response loss and reload preserve the exact approved payload and quote', async ({ page }, testInfo) => {
  const e = await fixture(page, { onOrder: ({ route, send, evidence }) => evidence.bodies.length === 1 ? route.abort('failed') : send({ id: 93 }, 201) });
  await review(page);
  const before = await page.locator('[data-service-checkout] .modal-body').innerText();
  await page.screenshot({ path: testInfo.outputPath('hourly-confirmation.png'), fullPage: true });
  await page.getByRole('button', { name: 'Place Order', exact: true }).click();
  await expect(page.locator('[data-checkout-error]')).toContainText('Network error');
  const pending = await page.evaluate(() => JSON.parse(localStorage.getItem('ghh_service_checkout_v1:2:321')));
  expect(pending.payload).toEqual(e.bodies[0].body);
  expect(pending.quote).toEqual(quote);
  await page.reload();
  await open(page);
  await expect(page.getByRole('button', { name: 'Place Order', exact: true })).toBeVisible();
  expect(await page.locator('[data-service-checkout] .modal-body').innerText()).toBe(before);
  await expect(page.getByLabel('Order notes')).toHaveCount(0);
  await page.screenshot({ path: testInfo.outputPath('hourly-retry-confirmation.png'), fullPage: true });
  await page.getByRole('button', { name: 'Place Order', exact: true }).click();
  await expect.poll(() => e.bodies.length).toBe(2);
  expect(e.bodies[1]).toEqual(e.bodies[0]);
  expect(e.quotes).toEqual(['?hours=2.5']);
  await expect.poll(() => page.evaluate(() => localStorage.getItem('ghh_service_checkout_v1:2:321'))).toBeNull();
  expect(e.errors).toEqual([]);
});

for (const rejection of [
  { status: 409, code: 'service_quote_changed', retry_safe: true, clears: true },
  { status: 409, code: 'service_quote_changed', retry_safe: false },
  { status: 409, code: 'service_quote_changed', retry_safe: 'true' },
  { status: 409, code: 'other_conflict', retry_safe: true },
  { status: 500, code: 'service_quote_changed', retry_safe: true },
  { status: 400, code: 'invalid_request', retry_safe: true },
]) test(`only explicit safe stale rejection can replace a quote: ${JSON.stringify(rejection)}`, async ({ page }) => {
  const e = await fixture(page, { onOrder: ({ send }) => send({ error: 'fixture rejection', ...rejection }, rejection.status) });
  await review(page);
  await page.getByRole('button', { name: 'Place Order', exact: true }).click();
  await expect(page.locator('[data-checkout-error]')).toContainText(rejection.clears ? 'Quote changed' : 'fixture rejection');
  const saved = await page.evaluate(() => localStorage.getItem('ghh_service_checkout_v1:2:321'));
  if (rejection.clears) {
    expect(saved).toBeNull();
    await expect(page.getByRole('button', { name: 'Place Order', exact: true })).toHaveCount(0);
    await page.locator('[data-close]').click();
    await review(page);
    expect(e.quotes).toHaveLength(2);
    // New quote requires another explicit confirmation; no automatic POST.
    expect(e.bodies).toHaveLength(1);
    await page.getByRole('button', { name: 'Place Order', exact: true }).click();
    await expect.poll(() => e.bodies.length).toBe(2);
    expect(e.bodies[1].body.idempotency_key).not.toBe(e.bodies[0].body.idempotency_key);
  } else {
    expect(JSON.parse(saved).payload).toEqual(e.bodies[0].body);
    await page.locator('[data-close]').click();
    await open(page);
    await page.getByRole('button', { name: 'Place Order', exact: true }).click();
    await expect.poll(() => e.bodies.length).toBe(2);
    expect(e.bodies[1]).toEqual(e.bodies[0]);
    expect(e.quotes).toHaveLength(1);
  }
});

test('legacy key without approved body fails closed even after logout', async ({ page }) => {
  const e = await fixture(page);
  await page.evaluate(() => {
    sessionStorage.setItem('ghh_pending_service_order_321', 'service-order-12345678-1234-1234-1234-123456789012');
    clearSession();
    saveSession('fixture-buyer-2', { id: 2, name: 'Fixture buyer' });
  });
  await open(page);
  await expect(page.locator('#toasts')).toContainText('legacy');
  await expect(page.locator('[data-service-checkout]')).toHaveCount(0);
  expect(e.quotes).toEqual([]);
  expect(e.bodies).toEqual([]);
  expect(await page.evaluate(() => sessionStorage.getItem('ghh_pending_service_order_321'))).toBeTruthy();
});

test('logout closes private confirmation but retains buyer-scoped recovery', async ({ page }) => {
  const e = await fixture(page, { onOrder: ({ route }) => route.abort('failed') });
  await review(page);
  await page.getByRole('button', { name: 'Place Order', exact: true }).click();
  await expect(page.locator('[data-checkout-error]')).toContainText('Network error');
  const raw = await page.evaluate(() => localStorage.getItem('ghh_service_checkout_v1:2:321'));
  await page.evaluate(() => { clearSession(); saveSession('other-token', { id: 3, name: 'Other buyer' }); });
  await expect(page.locator('[data-service-checkout]')).toHaveCount(0);
  await open(page);
  await expect(page.getByLabel('Order notes')).toHaveValue('');
  await expect(page.locator('[data-service-checkout]')).not.toContainText('Buyer notes');
  expect(await page.evaluate(() => localStorage.getItem('ghh_service_checkout_v1:2:321'))).toBe(raw);
  await page.locator('[data-close]').click();
  await page.evaluate(() => saveSession('fixture-buyer-2', { id: 2, name: 'Fixture buyer' }));
  await open(page);
  await expect(page.getByRole('button', { name: 'Place Order', exact: true })).toBeVisible();
  expect(e.quotes).toHaveLength(1);
  expect(e.bodies).toHaveLength(1);
});

test('account changes during quote loading cannot expose or submit stale confirmation', async ({ page }) => {
  let finish;
  const gate = new Promise(resolve => finish = resolve);
  const e = await fixture(page, { onQuote: async ({ send }) => { await gate; return send(quote); } });
  await open(page);
  await page.getByLabel('Order notes').fill('Private task');
  await page.getByLabel('Hours').fill('2.5');
  await page.getByRole('button', { name: 'Review quote', exact: true }).click();
  await expect.poll(() => e.quotes.length).toBe(1);
  await page.evaluate(() => saveSession('other-token', { id: 3, name: 'Other buyer' }));
  await expect(page.locator('[data-service-checkout]')).toHaveCount(0);
  finish();
  await expect(page.getByRole('button', { name: 'Place Order', exact: true })).toHaveCount(0);
  expect(e.bodies).toEqual([]);
});

test('route teardown discards only the view, not an ambiguous order', async ({ page }) => {
  const e = await fixture(page, { onOrder: ({ route }) => route.abort('failed') });
  await review(page);
  await page.getByRole('button', { name: 'Place Order', exact: true }).click();
  await expect(page.locator('[data-checkout-error]')).toContainText('Network error');
  await page.evaluate(() => navigate('#/services'));
  await expect(page.locator('[data-service-checkout]')).toHaveCount(0);
  await page.goto('/#/services/321');
  await open(page);
  await page.getByRole('button', { name: 'Place Order', exact: true }).click();
  await expect.poll(() => e.bodies.length).toBe(2);
  expect(e.bodies[1]).toEqual(e.bodies[0]);
});

test('double clicks cannot open duplicate confirmations or submit twice', async ({ page }) => {
  let finish;
  const gate = new Promise(resolve => finish = resolve);
  const e = await fixture(page, { onOrder: async ({ send }) => { await gate; return send({ id: 93 }, 201); } });
  await page.evaluate(() => { handleOrderService(321); handleOrderService(321); });
  await expect(page.locator('[data-service-checkout]')).toHaveCount(1);
  await page.getByLabel('Hours').fill('2.5');
  await page.getByLabel('Order notes').fill('One task');
  await page.getByRole('button', { name: 'Review quote', exact: true }).click();
  await page.getByRole('button', { name: 'Place Order', exact: true }).evaluate(el => { el.click(); el.click(); });
  await expect.poll(() => e.bodies.length).toBe(1);
  await expect(page.getByRole('button', { name: 'Place Order', exact: true })).toBeDisabled();
  finish();
  await expect(page).toHaveURL(/#\/orders\/93/);
  expect(e.bodies).toHaveLength(1);
});

test('storage failure blocks POST rather than using an ephemeral key', async ({ page }) => {
  const e = await fixture(page);
  await review(page);
  await page.evaluate(() => { const set = Storage.prototype.setItem; Storage.prototype.setItem = function(k, v) { if (k.startsWith('ghh_service_checkout')) throw new Error('Storage unavailable'); return set.call(this, k, v); }; });
  await page.getByRole('button', { name: 'Place Order', exact: true }).click();
  await expect(page.locator('[data-checkout-error]')).toContainText('Storage unavailable');
  expect(e.bodies).toEqual([]);
});

for (const invalid of [{ total_charge_cents: 2684 }, { currency: 'eur' }, { processing_fee_cents: 1.1 }, { quote_token: '' }, { quote_token: 'not-a-quote-token' }, { hours: '9' }, { service_id: 999 }]) {
  test(`invalid authoritative quote cannot be confirmed ${JSON.stringify(invalid)}`, async ({ page }) => {
    const e = await fixture(page, { onQuote: ({ send }) => send({ ...quote, ...invalid }) });
    await open(page);
    await page.getByLabel('Hours').fill('2.5');
    await page.getByRole('button', { name: 'Review quote', exact: true }).click();
    await expect(page.locator('[data-checkout-error]')).toContainText('quote unavailable');
    await expect(page.getByRole('button', { name: 'Place Order', exact: true })).toHaveCount(0);
    expect(e.bodies).toEqual([]);
  });
}

test('fixed checkout uses authoritative cents without an implicit hours field', async ({ page }) => {
  const e = await fixture(page, { type: 'fixed' });
  await review(page);
  await page.getByRole('button', { name: 'Place Order', exact: true }).click();
  await expect.poll(() => e.bodies.length).toBe(1);
  expect(e.bodies[0].body).not.toHaveProperty('hours');
  expect(e.bodies[0].body.quote_token).toBe(quote.quote_token);
  expect(e.quotes).toEqual(['']);
});

test('generic custom checkout is explicitly unavailable without POST', async ({ page }) => {
  const e = await fixture(page, { type: 'custom' });
  await open(page);
  await expect(page.locator('#toasts')).toContainText('Custom-price checkout is unavailable');
  expect(e.quotes).toEqual([]);
  expect(e.bodies).toEqual([]);
});

test('notes enforce the backend 5000 character limit before requesting a quote', async ({ page }) => {
  const e = await fixture(page);
  await open(page);
  const notes = page.getByLabel('Order notes');
  await expect(notes).toHaveAttribute('maxlength', '5000');
  await notes.evaluate(el => { el.value = 'x'.repeat(5001); });
  await page.getByRole('button', { name: 'Review quote', exact: true }).click();
  await expect(page.locator('[data-checkout-error]')).toContainText('5000');
  expect(e.quotes).toEqual([]);
  expect(e.bodies).toEqual([]);
});

test('canonical decimal hours are matched without rounding user input', async ({ page }) => {
  const e = await fixture(page, { onQuote: ({ send }) => send(quote) });
  await review(page, '2.500');
  await page.getByRole('button', { name: 'Place Order', exact: true }).click();
  await expect.poll(() => e.bodies.length).toBe(1);
  expect(e.bodies[0].body.hours).toBe('2.5');
});

test('pending replay bypasses changed payment setup and service eligibility', async ({ page }) => {
  const e = await fixture(page, { onOrder: ({ route, send, evidence }) => evidence.bodies.length === 1 ? route.abort('failed') : send({ id: 93 }, 201) });
  await review(page);
  await page.getByRole('button', { name: 'Place Order', exact: true }).click();
  await expect(page.locator('[data-checkout-error]')).toContainText('Network error');
  await page.locator('[data-close]').click();
  const forbidden = [];
  await page.route('**/fixture/payments/status', route => { forbidden.push('payments'); return route.fulfill({ json: { employer_ready: false } }); });
  await page.route('**/fixture/services/321', route => { forbidden.push('service'); return route.fulfill({ json: { pricing_type: 'custom', status: 'paused' } }); });
  await open(page);
  await page.getByRole('button', { name: 'Place Order', exact: true }).click();
  await expect.poll(() => e.bodies.length).toBe(2);
  expect(e.bodies[1]).toEqual(e.bodies[0]);
  expect(forbidden).toEqual([]);
});

// FE-1: use actual startup in two tabs, not a manually repaired state.user.
test('FE-1 per-tab A token with shared B user cannot read or erase B pending checkout', async ({ page, context }) => {
  const a = await fixture(page, { onOrder: ({ send }) => send({ error: 'Quote changed', code: 'service_quote_changed', retry_safe: true }, 409) });
  const buyerB = await context.newPage();
  const b = await fixture(buyerB, { onOrder: ({ route, send, evidence }) => evidence.bodies.length === 1 ? route.abort('failed') : send({ id: 93 }, 201) });
  await buyerB.evaluate(() => saveSession('other-token', { id: 3, name: 'Buyer B' }));
  await review(buyerB, '2.5', 'B PRIVATE unresolved task');
  await buyerB.getByRole('button', { name: 'Place Order', exact: true }).click();
  await expect(buyerB.locator('[data-checkout-error]')).toContainText('Network error');
  const key = 'ghh_service_checkout_v1:3:321';
  const original = await buyerB.evaluate(key => localStorage.getItem(key), key);
  await page.reload();
  expect(await page.evaluate(() => ({ token: state.token, buyer: state.user.id }))).toEqual({ token: 'fixture-buyer-2', buyer: 3 });
  await watchPendingReads(page);
  await page.evaluate(() => handleOrderService(321));
  expect.soft(await page.locator('body').innerText()).not.toContain('B PRIVATE unresolved task');
  expect.soft(await page.evaluate(() => window.pendingReads)).toEqual([]);
  // Exercise the vulnerable safe-409 path if a confirmation was wrongly exposed.
  const submit = page.getByRole('button', { name: 'Place Order', exact: true });
  if (await submit.count()) await submit.evaluate(el => el.onclick());
  expect.soft(a.profiles).toEqual(['Bearer fixture-buyer-2']);
  expect.soft(a.quotes).toEqual([]);
  expect.soft(a.bodies).toEqual([]);
  expect.soft(await page.evaluate(key => localStorage.getItem(key), key)).toBe(original);
  await buyerB.reload();
  await open(buyerB);
  await expect(buyerB.getByRole('button', { name: 'Place Order', exact: true })).toBeVisible();
  await buyerB.getByRole('button', { name: 'Place Order', exact: true }).click();
  await expect.poll(() => b.bodies.length).toBe(2);
  expect(b.bodies[1]).toEqual(b.bodies[0]);
  expect(b.quotes).toEqual(['?hours=2.5']);
  expect([...a.errors, ...b.errors]).toEqual([]);
});

async function watchPendingReads(page) {
  await page.evaluate(() => {
    window.pendingReads = [];
    const get = Storage.prototype.getItem;
    Storage.prototype.getItem = function(key) {
      if (key.startsWith('ghh_service_checkout_v1:')) window.pendingReads.push(key);
      return get.call(this, key);
    };
  });
}

async function seedPrivatePending(page) {
  return page.evaluate(q => {
    const raw = JSON.stringify({ version: 1, buyer_id: '2', service_id: '321', title: 'Private saved service', quote: q,
      payload: { hours: q.hours, notes: 'PRIVATE pending notes', quote_token: q.quote_token, idempotency_key: 'service-order-12345678-1234-1234-1234-123456789012' } });
    localStorage.setItem('ghh_service_checkout_v1:2:321', raw);
    return raw;
  }, quote);
}

for (const failure of ['network', 'missing endpoint', 'missing id', 'nested id', 'zero', 'negative', 'fractional', 'noncanonical', 'boolean', 'unsafe']) {
  test(`FE-1 unverified profile fails closed without pending disclosure: ${failure}`, async ({ page }) => {
    const values = { 'missing id': {}, 'nested id': { user: { id: 2 } }, zero: { id: 0 }, negative: { id: -2 }, fractional: { id: 2.5 }, noncanonical: { id: '02' }, boolean: { id: true }, unsafe: { id: 9007199254740992 } };
    const e = await fixture(page, { onProfile: ({ route, send }) => failure === 'network' ? route.abort('failed') : failure === 'missing endpoint' ? send({ error: 'Not found' }, 404) : send(values[failure]) });
    const raw = await seedPrivatePending(page);
    await watchPendingReads(page);
    await page.evaluate(() => handleOrderService(321));
    expect.soft(await page.evaluate(() => window.pendingReads)).toEqual([]);
    expect.soft(await page.locator('body').innerText()).not.toContain('PRIVATE pending notes');
    await expect.soft(page.locator('[data-service-checkout]')).toHaveCount(0);
    expect(e.profiles).toEqual(['Bearer fixture-buyer-2']);
    expect(e.quotes).toEqual([]);
    expect(e.bodies).toEqual([]);
    expect(await page.evaluate(() => localStorage.getItem('ghh_service_checkout_v1:2:321'))).toBe(raw);
  });
}

test('FE-1 account switch during profile fetch cannot read the old pending record', async ({ page }) => {
  let finish;
  const gate = new Promise(resolve => finish = resolve);
  const e = await fixture(page, { onProfile: async ({ send }) => { await gate; return send({ id: 2 }); } });
  const raw = await seedPrivatePending(page);
  await watchPendingReads(page);
  await page.evaluate(() => { window.checkoutOpening = handleOrderService(321); });
  try {
    await expect.poll(() => e.profiles.length).toBe(1);
    expect(await page.evaluate(() => window.pendingReads)).toEqual([]);
    await page.evaluate(() => saveSession('other-token', { id: 3, name: 'Buyer B' }));
  } finally { finish(); }
  await page.evaluate(() => window.checkoutOpening);
  expect(await page.evaluate(() => window.pendingReads)).toEqual([]);
  await expect(page.locator('[data-service-checkout]')).toHaveCount(0);
  expect(await page.locator('body').innerText()).not.toContain('PRIVATE pending notes');
  expect(e.quotes).toEqual([]);
  expect(e.bodies).toEqual([]);
  expect(await page.evaluate(() => localStorage.getItem('ghh_service_checkout_v1:2:321'))).toBe(raw);
});
