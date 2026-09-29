const { test, expect } = require('@playwright/test');
const fs = require('fs');
const path = require('path');

test('buyer fee tools use checkout component rounding for awkward cents', async ({ page }) => {
  await localOnly(page);
  await page.goto('/tools/fee-calculator.html');
  await page.locator('#toggleClient').click();
  await page.locator('#amountInput').fill('100.01');
  await expect(page.locator('.calc-row-featured')).toContainText('$104.01');
  await page.goto('/tools/freelance-fee-calculator.html');
  await page.locator('#role').selectOption('buyer');
  await page.locator('#gross').fill('33.33');
  await expect(page.locator('.calc-row-featured')).toContainText('$34.66');
  await page.goto('/tools/are-you-overpaying.html');
  await expect(page.locator('body')).not.toContainText('1% plus Stripe processing');
});

test('buyer fee tools use backend component rounding including minimum cents and invalid precision', async ({ page }) => {
  await localOnly(page);
  await page.goto('/tools/fee-calculator.html');
  await page.locator('#toggleClient').click();
  for (const [amount, total] of [['0.01', '$0.03'], ['0.49', '$0.51'], ['33', '$34.32'], ['1,234.56', '$1,283.95']]) {
    await page.locator('#amountInput').fill(amount);
    await expect(page.locator('.calc-row-featured')).toContainText(total);
  }
  await page.locator('#amountInput').fill('12.345');
  await expect(page.locator('#amountInput')).toHaveAttribute('aria-invalid', 'true');
  await expect(page.locator('.calc-row-featured')).toHaveCount(0);
  await page.goto('/tools/freelance-fee-calculator.html');
  await page.locator('#role').selectOption('buyer');
  for (const [amount, total] of [['0.01', '$0.03'], ['0.49', '$0.51'], ['33', '$34.32'], ['1234.56', '$1,283.95']]) {
    await page.locator('#gross').fill(amount);
    await expect(page.locator('.calc-row-featured .net-cell')).toContainText(total);
  }
  await page.locator('#gross').fill('12.345');
  await expect(page.locator('.calc-row-featured')).toHaveCount(0);
  await page.goto('/tools/are-you-overpaying.html?p=upwork&r=buyer&a=33.33&c=other');
  await expect(page.locator('#b-ghh')).toContainText('$15.96');
});

async function localOnly(page) {
  await page.route('**/*', route => {
    const url = new URL(route.request().url());
    if (url.origin === `http://127.0.0.1:${process.env.PW_PORT || 4173}` && route.request().method() === 'GET') {
      if (url.pathname === '/analytics-bootstrap.js') return route.fulfill({ contentType: 'application/javascript', body: '' });
      return route.continue();
    }
    return route.abort();
  });
  await page.addInitScript(() => {
    window.__events = [];
    window.gtag = (...args) => window.__events.push(args);
    sessionStorage.setItem('ghh_token', 'local-fixture');
    localStorage.setItem('ghh_user', JSON.stringify({ id: 999, name: 'Local fixture' }));
  });
}

for (const suffix of ['complete', 'refresh']) {
  test(`literal /payments?connect=${suffix} returns to worker payment status`, async ({ page }) => {
    await localOnly(page);
    const seen = [];
    await page.route('https://gohirehumans-production.up.railway.app/payments/**', route => {
      const path = new URL(route.request().url()).pathname;
      seen.push(path);
      const body = path === '/payments/status' ? { employer_ready: false, worker_ready: false } :
        path === '/payments/history' ? { escrow_history: [], fees_paid: [] } :
        { countries: [{ code: 'US', name: 'United States' }] };
      return route.fulfill({ json: body });
    });
    // Real static path: Python's directory slash redirect is permitted.
    await page.goto(`/payments?connect=${suffix}`);
    // The SPA consumes the one-time return parameter after recording the status check.
    await expect(page).toHaveURL(/\/#\/payments$/);
    await expect.poll(() => page.evaluate(() => window.__events.some(e => e[1] === 'worker_payout_setup_return'))).toBe(true);
    await expect(page.getByRole('heading', { name: 'Payments' })).toBeVisible();
    expect(seen).toContain('/payments/status');
    expect(await page.evaluate(() => window.__events.filter(e => e[1] === 'worker_payout_setup_completed'))).toEqual([]);
  });
}

test('payments history renders escrow holds from the real API response shape', async ({ page }) => {
  await localOnly(page);
  await page.goto('/');
  await page.evaluate(async () => {
    state.user = { id: 999, name: 'Local fixture' }; state.token = 'local-fixture';
    window.api = async path => {
      if (path === '/payments/status') return { employer_ready: false, worker_ready: false };
      if (path === '/payments/connect-countries') return { countries: [{ code: 'US', name: 'United States' }] };
      if (path === '/payments/history') return { escrow_history: [
        { id: 44, order_id: 17, order_type: 'job', order_total: 33.33,
          amount: 33.33, base_amount_cents: 3333, platform_fee_cents: 33,
          processing_fee_cents: 100, charged_total_cents: 3466,
          status: 'held', created_at: '2026-09-14 12:00:00' },
        { id: 45, order_id: 18, order_type: 'service', order_total: 9.99,
          amount: 9.99, base_amount_cents: 999, status: 'refunded', created_at: '2026-09-12 12:00:00' }
      ], fees_paid: [{ id: 7, order_id: 17, fee_amount: 0.33, fee_type: 'service_fee', created_at: '2026-09-14 12:00:00' }] };
      return {};
    };
    history.replaceState(null, '', '#/payments'); activeRouteHash = '#/payments';
    await renderPayments();
  });
  const rows = page.locator('table.data-table tbody tr');
  await expect(rows).toHaveCount(2);
  await expect(rows.first()).toContainText('Order #17');
  await expect(rows.first()).toContainText('$33.33');
  await expect(rows.first()).toContainText('held');
  await expect(rows.nth(1)).toContainText('refunded');
  await expect(page.getByText('No transactions yet.')).toHaveCount(0);
});

test('SPA page and form analytics never include draft text or private query strings', async ({ page }) => {
  await localOnly(page);
  const secret = 'alice@example.test';
  await page.goto('/#/post-job?draft_title=Call%20' + encodeURIComponent(secret) + '&draft_description=confidential');
  await expect.poll(() => page.evaluate(() => window.__events.some(e => e[1] === 'page_view'))).toBe(true);
  await page.evaluate(() => {
    const form = document.createElement('form');
    form.id = 'private_email@example.test';
    const input = document.createElement('input'); form.appendChild(input);
    document.body.appendChild(form); input.focus();
    form.dispatchEvent(new Event('submit', { bubbles: true, cancelable: true }));
  });
  const events = await page.evaluate(() => window.__events.filter(e => ['page_view', 'spa_page_view', 'form_start', 'form_submit_intent'].includes(e[1])));
  expect(events.some(e => e[1] === 'page_view')).toBe(true);
  expect(events.some(e => e[1] === 'form_start')).toBe(true);
  expect(JSON.stringify(events)).not.toMatch(/alice(?:%40|@)example|draft_title|draft_description|confidential|private_email/);
  const pageView = events.find(e => e[1] === 'page_view')[2];
  expect(pageView.page_path).toBe('/#/post-job');
  expect(pageView.spa_route).toBe('/post-job');
  expect(new URL(pageView.page_location).hash).toBe('#/post-job');
  expect(new URL(pageView.page_location).search).toBe('');
});

test('analytics keeps numeric ids but collapses unknown routes that could hold text', async ({ page }) => {
  await localOnly(page);
  await page.goto('/?utm_source=newsletter&email=' + encodeURIComponent('dave@example.test') + '#/services/42?note=' + encodeURIComponent('bob@example.test'));
  await expect.poll(() => page.evaluate(() => window.__events.some(e => e[1] === 'page_view'))).toBe(true);
  await page.evaluate(() => { location.hash = '#/carol@example.test'; });
  await expect.poll(() => page.evaluate(() => window.__events.filter(e => e[1] === 'page_view').length)).toBe(2);
  const views = await page.evaluate(() => window.__events.filter(e => e[1] === 'page_view').map(e => e[2]));
  expect(views.map(v => v.page_path)).toEqual(['/#/services/42', '/#/not-found']);
  expect(new URL(views[0].page_location).searchParams.get('utm_source')).toBe('newsletter');
  const all = JSON.stringify(await page.evaluate(() => window.__events));
  expect(all).not.toMatch(/bob(?:%40|@)|carol(?:%40|@)|dave(?:%40|@)|email=/);
  // Every event (and the gtag 'set' default) carries only the sanitized location.
  const setCalls = await page.evaluate(() => window.__events.filter(e => e[0] === 'set').map(e => e[1].page_location));
  expect(setCalls.length).toBeGreaterThan(0);
  for (const loc of setCalls) expect(loc).not.toMatch(/email|note|@/);
});
