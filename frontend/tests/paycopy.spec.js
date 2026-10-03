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

test('Upwork project and quiz fees label the freelancer example and exact Basic client maximum', async ({ page }) => {
  await localOnly(page);
  await page.goto('/tools/fee-calculator.html?amount=1000');
  await page.locator('#amountInput').fill('1000');
  const row = page.locator('#resultsBody tr').filter({ hasText: 'Upwork' });
  await expect(row.locator('td').nth(1)).toHaveText('$100.0010% example; actual fee is 0–15% per contract');
  await expect(row.locator('td').nth(2)).toHaveText('$79.90Up to 7.99% client fee (Basic)');
  await expect(row.locator('td').nth(3)).toHaveText('$900.00');
  await page.locator('#toggleClient').click();
  await expect(row.locator('td').nth(3)).toHaveText('$1,079.90');
  for (const [role, label, fee, difference] of [
    ['buyer', 'Upwork fee (up to 7.99% client fee, Basic)', '$958.80', '$478.80'],
    ['seller', 'Upwork fee (10% example; actual fee is 0–15% per contract)', '$1,200.00', '$1,200.00'],
  ]) {
    await page.goto(`/tools/are-you-overpaying.html?p=upwork&r=${role}&a=1000&c=other`);
    await expect(page.locator('#b-current-label')).toHaveText(label);
    await expect(page.locator('#b-current')).toHaveText(fee);
    await expect(page.locator('#r-amount')).toHaveText(difference);
    await expect(page.locator('#r-sub')).toContainText('Modeled Upwork fees');
    const shared = new URL(await page.locator('#share-x').getAttribute('href'));
    expect(shared.searchParams.get('text')).toContain('modeled fees');
  }
});

test('project fee calculator differences reconcile with the cent-rounded fees each row shows', async ({ page }) => {
  await localOnly(page);
  await page.goto('/tools/fee-calculator.html');
  const money = text => Number(text.replace(/[^0-9.]/g, ''));
  for (const mode of ['#toggleFreelancer', '#toggleClient']) {
    await page.locator(mode).click();
    for (const amount of ['33.33', '100.01', '1,234.56', '2000']) {
      await page.locator('#amountInput').fill(amount);
      const featured = page.locator('.calc-row-featured');
      const reference = money(await featured.locator('td').nth(1).locator('.amt').innerText())
        + money(await featured.locator('td').nth(2).locator('.amt').innerText());
      const rows = page.locator('#resultsBody tr:not(.calc-row-featured)');
      await expect(rows).toHaveCount(4);
      for (let i = 0; i < 4; i++) {
        const row = rows.nth(i);
        const fees = money(await row.locator('td').nth(1).locator('.amt').innerText())
          + money(await row.locator('td').nth(2).locator('.amt').innerText());
        const expected = Math.round((fees - reference) * 100) / 100;
        const shown = await row.locator('td').nth(4).innerText();
        if (expected > 0) expect(money(shown), `${amount} ${await row.locator('td').first().innerText()}`).toBe(expected);
        else expect(shown).toBe('Same or lower');
      }
    }
  }
  await page.locator('#amountInput').fill('33.33');
  await expect(page.locator('#resultsBody tr').filter({ hasText: 'Upwork' }).locator('td').nth(4)).toHaveText('$4.66 more in platform fees');
  // Half-cent oracle: fees are exact decimal products rounded half-up to the cent (no binary-float drift).
  // Expected values come from Python Decimal ROUND_HALF_UP. 40.15 catches float drift on seller fees, 73 and
  // 68.50 on buyer fees; 12.35 and 1000 are deliberate guards that also pass on float rounding.
  await page.locator('#toggleFreelancer').click();
  for (const [amount, expected] of [
    ['40.15', { Upwork: ['$4.02', '$3.21'], Fiverr: ['$8.03', '$2.21'], 'Freelancer.com': ['$4.02', '$1.20'] }],
    ['73', { Upwork: ['$7.30', '$5.83'], Fiverr: ['$14.60', '$4.02'], 'Freelancer.com': ['$7.30', '$2.19'] }],
    ['68.50', { Upwork: ['$6.85', '$5.47'], Fiverr: ['$13.70', '$3.77'], 'Freelancer.com': ['$6.85', '$2.06'] }],
    ['12.35', { Upwork: ['$1.24', '$0.99'], Fiverr: ['$2.47', '$0.68'], 'Freelancer.com': ['$1.24', '$0.37'] }],
    ['1000', { Upwork: ['$100.00', '$79.90'], Fiverr: ['$200.00', '$55.00'], 'Freelancer.com': ['$100.00', '$30.00'] }],
  ]) {
    await page.locator('#amountInput').fill(amount);
    for (const [name, [seller, buyer]] of Object.entries(expected)) {
      const row = page.locator('#resultsBody tr').filter({ has: page.getByText(name, { exact: true }) });
      await expect(row.locator('td').nth(1).locator('.amt'), `${amount} ${name} seller`).toHaveText(seller);
      await expect(row.locator('td').nth(2).locator('.amt'), `${amount} ${name} buyer`).toHaveText(buyer);
    }
  }
});

test('project fee calculator share links keep cents for amounts of $100 or more', async ({ page }) => {
  await localOnly(page);
  await page.goto('/tools/fee-calculator.html?amount=100.01&mode=client');
  await expect(page.locator('#amountInput')).toHaveValue('100.01');
  await expect(page.locator('.calc-row-featured td').nth(3)).toHaveText('$104.01');
  await expect(page).toHaveURL(/[?&]amount=100\.01(&|$)/);
  await page.goto('/tools/fee-calculator.html?amount=1234.56');
  await expect(page.locator('#amountInput')).toHaveValue('1,234.56');
  await expect(page.locator('.calc-row-featured td').nth(3)).toHaveText('$1,234.56');
  await expect(page).toHaveURL(/[?&]amount=1234\.56(&|$)/);
  // The slider still moves in whole steps and the typed path still keeps cents.
  await page.locator('#amountInput').fill('250.75');
  await page.locator('#amountInput').blur();
  await expect(page.locator('#amountInput')).toHaveValue('250.75');
  await expect(page).toHaveURL(/[?&]amount=250\.75(&|$)/);
});

test('sibling fee tools round each modeled fee to the cent so fee, net and difference reconcile', async ({ page }) => {
  await localOnly(page);
  // Expected values come from Python Decimal ROUND_HALF_UP on whole-cent amounts, not from the formula under test.
  await page.goto('/tools/freelance-fee-calculator.html');
  const row = name => page.locator('#rows tr').filter({ has: page.getByText(name, { exact: true }) });
  await page.locator('#role').selectOption('seller');
  await page.locator('#gross').fill('40.15');
  await expect(row('Upwork').locator('td').nth(1)).toHaveText('$4.02 (10% example; actual fee is 0–15% per contract)');
  await expect(row('Upwork').locator('.net-cell')).toHaveText('$36.13');
  await expect(row('Freelancer.com').locator('.net-cell')).toHaveText('$36.13');
  await expect(page.locator('#difference-text')).toContainText('on Fiverr are $96.36 higher');
  await page.locator('#role').selectOption('buyer');
  await page.locator('#gross').fill('5.50');
  await expect(row('Freelancer.com').locator('td').nth(1)).toHaveText('$0.17 (3%)');
  await expect(row('Freelancer.com').locator('.net-cell')).toHaveText('$5.67');
  await expect(row('Upwork').locator('td').nth(1)).toHaveText('$0.44 (up to 7.99% client fee, Basic)');
  await expect(page.locator('#difference-text')).toContainText('on Upwork are $2.52 higher');
  // The quiz models twelve monthly payments, each fee rounded to the cent like the GoHireHumans side.
  for (const [query, current, difference] of [
    ['p=upwork&r=seller&a=40.15', '$48.24', '$48.24'],
    ['p=upwork&r=buyer&a=33.33', '$31.92', '$15.96'],
    ['p=fiverr&r=buyer&a=73', '$48.24', '$13.20'],
    ['p=upwork&r=buyer&a=1000', '$958.80', '$478.80'],
  ]) {
    await page.goto(`/tools/are-you-overpaying.html?${query}&c=other`);
    await expect(page.locator('#b-current'), query).toHaveText(current);
    await expect(page.locator('#r-amount'), query).toHaveText(difference);
  }
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
