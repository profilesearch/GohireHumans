const { test, expect } = require('@playwright/test');

// Real application UI; only processor and API boundaries are replaced. No outbound traffic.
async function boot(page, options = {}) {
  const unexpected = [];
  await page.route('**/*', route => {
    const url = new URL(route.request().url());
    if (url.origin === `http://127.0.0.1:${process.env.PW_PORT || 4173}` && route.request().method() === 'GET') return route.continue();
    if (/google-analytics|googletagmanager|stripe\.com/.test(url.hostname)) unexpected.push(url.hostname);
    return route.fulfill({ status: 200, contentType: 'application/json', body: '{}' });
  });
  await page.goto('/');
  await page.evaluate(async options => {
    state.user = { id: 999, name: 'Local fixture' }; state.token = 'local-fixture';
    window.__events = []; window.__calls = []; window.__cardHandlers = {};
    window.__options = options;
    sessionStorage.setItem('ghh_attribution', JSON.stringify({ ref: 'PRIVATE_USER_INPUT', utm_source: 'PRIVATE_URL' }));
    window.gtag = (...args) => window.__events.push(args);
    window.Stripe = () => ({
      elements: () => ({ create: () => ({ mount() {}, on(name, fn) { window.__cardHandlers[name] = fn; }, destroy() {} }) }),
      confirmCardSetup: async () => {
        if (options.processorError) return { error: { message: 'PRIVATE_STRIPE_ERROR', type: 'card_error' } };
        if (options.processorReject) throw new Error('PRIVATE_PROCESSOR_REJECT');
        if (options.delayProcessor) await new Promise(resolve => window.__resolveProcessor = resolve);
        if (Object.hasOwn(options, 'confirmation')) return options.confirmation;
        return { setupIntent: { status: 'succeeded', payment_method: 'pm_LOCAL_ONLY' } };
      }
    });
    window.api = async (path, request = {}) => {
      window.__calls.push({ path, ...request });
      if (path === '/payments/status') {
        if (options.statusError) throw new Error('Local status unavailable');
        return options.status || { employer_ready: false, worker_ready: false };
      }
      if (path === '/payments/history') return [];
      if (path === '/payments/connect-countries') return { countries: options.countries || [{ code: 'US', name: 'United States', agreement: 'full' }] };
      if (path === '/payments/setup-employer' || path === '/payments/setup-worker') {
        if (options.setupError) throw new Error('PRIVATE_SETUP_ERROR');
        if (options.delaySetup) await new Promise(resolve => window.__resolveSetup = resolve);
        return options.setup || { client_secret: 'LOCAL_SECRET', publishable_key: 'pk_live_LOCAL_ONLY', mode: 'live' };
      }
      if (path === '/payments/confirm-setup-employer') {
        if (options.confirmError) throw new Error('PRIVATE_CONFIRM_ERROR');
        if (options.delayConfirm) await new Promise(resolve => window.__resolveConfirm = resolve);
        return { ok: true };
      }
      return {};
    };
    history.replaceState(null, '', '#/payments');
    activeRouteHash = '#/payments';
    await renderPayments();
    window.__events = [];
  }, options);
  return unexpected;
}
async function events(page) { return page.evaluate(() => __events.filter(e => /^(employer_payment_setup_|worker_payout_setup_|payment_setup_)/.test(e[1]))); }
async function names(page) { return (await events(page)).map(e => e[1]); }
async function open(page) {
  await page.getByRole('button', { name: 'Set Up Payment Method', exact: true }).click();
  await expect(page.locator('#paymentSetupModal')).toBeVisible();
}
async function ready(page) { await page.evaluate(() => __cardHandlers.ready?.()); }
async function assertPrivate(page) {
  const all = await page.evaluate(() => __events);
  expect(all.filter(e => /generate_lead|qualify_lead|close_convert_lead|purchase|sign_up/.test(e[1]))).toEqual([]);
  for (const [_, name, params] of await events(page)) {
    expect(name.length).toBeLessThanOrEqual(40);
    expect(JSON.stringify(params)).not.toMatch(/PRIVATE_|LOCAL_|pm_|pk_|acct_|https?:/);
    expect(Object.keys(params).sort()).toEqual(['mode', 'reason', 'stage']);
  }
}

for (const [scenario, reason] of [['confirmError', 'confirm_request_error'], ['processorError', 'stripe_confirm_error'], ['processorReject', 'stripe_confirm_error'], ['setupError', 'setup_request_error'], ['missingFlow', 'missing_setup_flow']]) {
  test(`employer ${scenario} is diagnostic and retryable`, async ({ page }) => {
    await boot(page, scenario === 'missingFlow' ? { setup: {} } : { [scenario]: true });
    await page.getByRole('button', { name: 'Set Up Payment Method', exact: true }).click();
    if (!['setupError', 'missingFlow'].includes(scenario)) {
      await ready(page);
      await page.locator('#confirmEmployerPaymentBtn').click();
      await expect(page.locator('#confirmEmployerPaymentBtn')).toBeEnabled();
      await expect(page.locator('#employer-card-error')).not.toBeEmpty();
    }
    await expect.poll(async () => (await events(page)).find(e => e[1] === 'employer_payment_setup_failed')?.[2]?.reason).toBe(reason);
    expect(await names(page)).not.toContain('employer_payment_setup_completed');
    if (scenario === 'processorError') await expect(page.locator('#employer-card-error')).toHaveText('PRIVATE_STRIPE_ERROR');
    if (scenario === 'confirmError') await expect(page.locator('#employer-card-error')).toHaveText('PRIVATE_CONFIRM_ERROR');
    if (['processorError', 'processorReject'].includes(scenario)) {
      expect(await names(page)).not.toContain('employer_payment_setup_proc_succeeded');
      expect(await page.evaluate(() => __calls.filter(c => c.path === '/payments/confirm-setup-employer'))).toEqual([]);
    }
    await assertPrivate(page);
  });
}

const incompleteConfirmations = [
  ['empty result', {}],
  ['null result', null],
  ['missing result', undefined],
  ['null intent', { setupIntent: null }],
  ...['processing', 'requires_action', 'requires_payment_method', 'canceled', 'SUCCEEDED', undefined].map(status =>
    [`status ${String(status)}`, { setupIntent: { status, payment_method: 'pm_LOCAL_ONLY' } }]),
  ...[
    ['missing', undefined], ['null', null], ['object', { id: 'pm_LOCAL_ONLY' }],
    ['numeric', 123], ['empty', ''], ['wrong prefix', 'card_LOCAL_ONLY'], ['empty suffix', 'pm_'],
    ['space', 'pm_LOCAL ONLY'], ['leading space', ' pm_LOCAL_ONLY'], ['trailing newline', 'pm_LOCAL_ONLY\n'],
    ['control', 'pm_LOCAL\u0000ONLY'], ['punctuation', 'pm_LOCAL-ONLY'], ['overlong', 'pm_' + 'a'.repeat(253)]
  ].map(([name, payment_method]) => [`method ${name}`, { setupIntent: { status: 'succeeded', payment_method } }])
];
for (const [scenario, confirmation] of incompleteConfirmations) {
  test(`incomplete confirmation: ${scenario} fails closed and remains retryable`, async ({ page }) => {
    const unexpected = await boot(page, { confirmation });
    await open(page); await ready(page);
    await page.locator('#confirmEmployerPaymentBtn').click();
    // Settle the handler (including a permissive ok:true backend) before checking absence.
    await expect.poll(async () => (await names(page)).some(n => /_(failed|completed)$/.test(n))).toBe(true);
    expect((await names(page)).filter(n => /_(proc_succeeded|completed)$/.test(n))).toEqual([]);
    expect(await page.evaluate(() => __calls.filter(c => c.path === '/payments/confirm-setup-employer'))).toEqual([]);
    await expect(page.locator('#paymentSetupModal')).toBeVisible();
    await expect(page.locator('#employer-card-error')).toBeVisible();
    await expect(page.locator('#employer-card-error')).toHaveText('Payment setup is not complete. Please try again.');
    await expect(page.locator('#confirmEmployerPaymentBtn')).toBeEnabled();
    await expect(page.locator('#confirmEmployerPaymentBtn')).toHaveText('Save payment method');
    expect((await events(page)).at(-1)).toEqual(['event', 'employer_payment_setup_failed', { stage: 'failed', mode: 'live', reason: 'confirmation_incomplete' }]);
    await assertPrivate(page); expect(unexpected).toEqual([]);
    // A valid retry uses the same modal and sends only the valid method, once.
    await page.evaluate(() => { __options.confirmation = { setupIntent: { status: 'succeeded', payment_method: 'pm_LOCAL_ONLY' } }; });
    await page.locator('#confirmEmployerPaymentBtn').click();
    await expect(page.locator('#paymentSetupModal')).toHaveCount(0);
    expect(await page.evaluate(() => __calls.filter(c => c.path === '/payments/confirm-setup-employer'))).toEqual([
      { path: '/payments/confirm-setup-employer', method: 'POST', body: { payment_method_id: 'pm_LOCAL_ONLY' } }
    ]);
    expect((await names(page)).filter(n => /_(proc_succeeded|completed)$/.test(n))).toEqual(['employer_payment_setup_proc_succeeded', 'employer_payment_setup_completed']);
    await assertPrivate(page);
  });
}

for (const payment_method of ['pm_a', 'pm_' + 'A0_'.repeat(84)]) {
  test(`valid succeeded confirmation accepts method length ${payment_method.length}`, async ({ page }) => {
    await boot(page, { confirmation: { setupIntent: { status: 'succeeded', payment_method } } });
    await open(page); await ready(page);
    await page.locator('#confirmEmployerPaymentBtn').click();
    await expect(page.locator('#paymentSetupModal')).toHaveCount(0);
    expect(await page.evaluate(() => __calls.filter(c => c.path === '/payments/confirm-setup-employer'))).toEqual([
      { path: '/payments/confirm-setup-employer', method: 'POST', body: { payment_method_id: payment_method } }
    ]);
    expect(await names(page)).toContain('employer_payment_setup_completed');
    await assertPrivate(page);
  });
}

test('card validation records only bounded diagnostic reason', async ({ page }) => {
  await boot(page); await open(page); await ready(page);
  await page.evaluate(() => __cardHandlers.change({ error: { message: 'PRIVATE_CARD_INPUT' } }));
  await expect(page.locator('#employer-card-error')).toHaveText('PRIVATE_CARD_INPUT');
  expect((await events(page)).at(-1)).toEqual(['event', 'employer_payment_setup_validation_failed', { stage: 'validation_failed', mode: 'live', reason: 'card_validation' }]);
  await assertPrivate(page);
});

for (const mode of ['simulated', 'test']) {
  test(`${mode} employer setup is not completed or represented as real`, async ({ page }) => {
    await boot(page, { setup: mode === 'simulated' ? { mode, payment_method_id: 'pm_LOCAL_ONLY', message: 'Payment method set up successfully' } : { mode: 'live', publishable_key: 'pk_test_LOCAL_ONLY', client_secret: 'LOCAL_SECRET' } });
    await page.getByRole('button', { name: 'Set Up Payment Method', exact: true }).click();
    if (mode === 'test') { await ready(page); await page.locator('#confirmEmployerPaymentBtn').click(); }
    await expect(page.locator('body')).toContainText(/not ready for real payments/i);
    expect((await names(page)).filter(n => /completed$/.test(n))).toEqual([]);
    await assertPrivate(page);
  });
}

test('unknown configuration has neutral trust copy and no false simulation warning', async ({ page }) => {
  await boot(page);
  const config = require('fs').readFileSync(require('path').join(__dirname, '..', 'config.js'), 'utf8');
  const result = await page.evaluate(code => {
    const warnings = []; const original = console.warn; console.warn = text => warnings.push(text);
    (0, eval)(code); console.warn = original;
    return { live: window.GOHIREHUMANS_PAYMENTS_LIVE, warnings };
  }, config);
  expect(result.live).toBeNull();
  expect(result.warnings).toEqual([]);
  for (const [key, value] of [['pk_test_LOCAL', false], ['pk_live_LOCAL', true]]) {
    expect(await page.evaluate(code => { (0, eval)(code); return window.GOHIREHUMANS_PAYMENTS_LIVE; }, config.replace("window.STRIPE_PUBLISHABLE_KEY = '';", `window.STRIPE_PUBLISHABLE_KEY = '${key}';`))).toBe(value);
  }
  await page.evaluate(async () => {
    window.api = async () => ({ employer_ready: true });
    await handleHire(5, 6, 'fixed', 20);
  });
  await expect(page.locator('body')).not.toContainText(/256-bit|PCI-DSS|processing is simulated/);
  await expect(page.locator('body')).toContainText('Payment availability and status are confirmed by the payment service.');
});

for (const query of ['connect=success', 'connect=complete', 'setup=success']) {
  test(`return ${query} observes API status instead of claiming completion`, async ({ page }) => {
    await boot(page);
    await page.evaluate(async query => { history.replaceState(null, '', '#/payments?' + query); activeRouteHash = location.hash; await renderPayments(); }, query);
    await expect(page.locator('.stat-card').filter({ hasText: 'Employer payment' })).toContainText('Not set up');
    await expect(page.locator('.stat-card').filter({ hasText: 'Worker payout' })).toContainText('Not connected');
    await expect(page.locator('body')).not.toContainText(/Bank account connected!|Payment method set up!/);
    const family = query.startsWith('connect') ? 'worker_payout_setup' : 'employer_payment_setup';
    expect(await names(page)).toContain(family + '_return');
    expect((await events(page)).find(e => e[1] === family + '_status_observed')?.[2]?.reason).toBe('not_ready');
    expect((await names(page)).filter(n => /completed$/.test(n))).toEqual([]);
    await assertPrivate(page);
  });
}
test('international payout selector is shown only with choices and posts selected country', async ({ page, isMobile }) => {
  await boot(page, { countries: [
    { code: 'US', name: 'United States', agreement: 'full' },
    { code: 'DE', name: 'Germany', agreement: 'full' },
    { code: 'CA', name: '<img src=x onerror=alert(1)>', agreement: 'full' }
  ], setup: { mode: 'simulated' } });
  const selector = page.getByRole('combobox', { name: 'Payout country' });
  await expect(selector).toBeVisible();
  await expect(selector).toHaveValue('US');
  expect(await page.locator('img[src="x"]').count()).toBe(0);
  await expect(page.locator('body')).toContainText('Payouts are sent in your local currency by Stripe; Stripe may charge a cross-border fee.');
  await selector.selectOption('DE');
  await page.getByRole('button', { name: 'Connect Bank Account', exact: true }).click();
  await expect.poll(() => page.evaluate(() => __calls.filter(c => c.path === '/payments/setup-worker'))).toEqual([
    { path: '/payments/setup-worker', method: 'POST', body: { country: 'DE' } }
  ]);
  if (isMobile) expect(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth)).toBe(true);
});
test('default-off country endpoint preserves no-selector US payout setup', async ({ page }) => {
  await boot(page, { setup: { mode: 'simulated' } });
  await expect(page.getByRole('combobox', { name: 'Payout country' })).toHaveCount(0);
  await page.getByRole('button', { name: 'Connect Bank Account', exact: true }).click();
  await expect.poll(() => page.evaluate(() => __calls.filter(c => c.path === '/payments/setup-worker'))).toEqual([
    { path: '/payments/setup-worker', method: 'POST', body: { country: 'US' } }
  ]);
});
test('single offered non-US country is posted explicitly (allowlist of one)', async ({ page }) => {
  await boot(page, { setup: { mode: 'simulated' }, countries: [{ code: 'DE', name: 'Germany', agreement: 'full' }] });
  await expect(page.getByRole('combobox', { name: 'Payout country' })).toHaveCount(0);
  await page.getByRole('button', { name: 'Connect Bank Account', exact: true }).click();
  await expect.poll(() => page.evaluate(() => __calls.filter(c => c.path === '/payments/setup-worker'))).toEqual([
    { path: '/payments/setup-worker', method: 'POST', body: { country: 'DE' } }
  ]);
});
test('existing bound account posts no country so the server keeps it after rollback', async ({ page }) => {
  await boot(page, { setup: { mode: 'simulated' }, status: { employer_ready: false, worker_ready: true,
    worker_payout_status: { connected: true, account_id: 'acct_bound', country: 'DE', mode: 'live' } } });
  await page.getByRole('button', { name: 'Update Bank Account', exact: true }).click();
  await expect.poll(() => page.evaluate(() => __calls.filter(c => c.path === '/payments/setup-worker'))).toEqual([
    { path: '/payments/setup-worker', method: 'POST', body: {} }
  ]);
});

for (const scenario of ['redirect', 'setupError', 'missing', 'simulated']) {
  test(`worker ${scenario} has a separate bounded funnel`, async ({ page }) => {
    await boot(page, scenario === 'setupError' ? { setupError: true } : { setup: scenario === 'redirect' ? { mode: 'live', onboarding_url: '#/payments?connect=success' } : scenario === 'simulated' ? { mode: 'simulated', onboarding_url: '#/payments?connect=success' } : {} });
    await page.getByRole('button', { name: 'Connect Bank Account', exact: true }).click();
    await expect.poll(() => names(page)).toContain('worker_payout_setup_' + (scenario === 'redirect' ? 'redirect_started' : scenario === 'simulated' ? 'status_observed' : 'failed'));
    expect((await names(page)).filter(n => /completed$|^payment_setup_|^employer_payment_setup_/.test(n))).toEqual([]);
    await assertPrivate(page);
  });
}
test('simulated API readiness never displays real connected status', async ({ page }) => {
  await boot(page, { status: { employer_ready: true, worker_ready: true, worker_payout_status: { mode: 'simulated' }, employer_payment_status: { payment_method_id: 'pm_sim_LOCAL' } } });
  await expect(page.locator('.stat-card').filter({ hasText: 'Worker payout' })).not.toContainText('Connected');
  await expect(page.locator('.stat-card').filter({ hasText: 'Employer payment' })).not.toContainText('Ready');
});

test('cancel during processor work cannot attach a card or claim completion', async ({ page }) => {
  await boot(page, { delayProcessor: true }); await open(page); await ready(page);
  await page.locator('#confirmEmployerPaymentBtn').click();
  await page.getByRole('button', { name: 'Cancel', exact: true }).click();
  await page.evaluate(() => __resolveProcessor());
  await expect.poll(() => names(page)).toContain('employer_payment_setup_cancelled');
  expect(await page.evaluate(() => __calls.filter(c => c.path === '/payments/confirm-setup-employer'))).toEqual([]);
  expect(await names(page)).not.toContain('employer_payment_setup_completed');
});
test('navigation during setup request cannot open a stale modal', async ({ page }) => {
  await boot(page, { delaySetup: true });
  await page.getByRole('button', { name: 'Set Up Payment Method', exact: true }).click();
  await page.evaluate(() => { location.hash = '#/services'; });
  await expect(page.locator('h1')).toContainText('Browse Services');
  await page.evaluate(() => __resolveSetup());
  await expect.poll(() => names(page)).toContain('employer_payment_setup_cancelled');
  await expect(page.locator('#paymentSetupModal')).toHaveCount(0);
});
test('rejected Stripe loader is removed so a second attempt is retryable', async ({ page }) => {
  await boot(page);
  await page.evaluate(() => { window.__stubStripe = window.Stripe; delete window.Stripe; });
  let loads = 0;
  await page.route('https://js.stripe.com/v3/', route => { loads++; return loads === 1 ? route.abort() : route.fulfill({ contentType: 'application/javascript', body: 'window.Stripe = window.__stubStripe;' }); });
  await page.getByRole('button', { name: 'Set Up Payment Method', exact: true }).click();
  await expect.poll(async () => (await events(page)).find(e => e[2]?.reason === 'loader_error')?.[1]).toBe('employer_payment_setup_failed');
  await expect(page.getByRole('button', { name: 'Set Up Payment Method', exact: true })).toBeEnabled();
  await open(page); expect(loads).toBe(2);
});

test('unsuccessful backend response never reports completed', async ({ page }) => {
  await boot(page); await open(page); await ready(page);
  await page.evaluate(() => { window.api = async () => ({ ok: false }); });
  await page.locator('#confirmEmployerPaymentBtn').click();
  await expect(page.locator('#paymentSetupModal')).toBeVisible();
  await expect(page.locator('#confirmEmployerPaymentBtn')).toBeEnabled();
  expect(await names(page)).not.toContain('employer_payment_setup_completed');
});

test('employer success is ready-gated and completed only after backend confirmation', async ({ page }) => {
  const unexpected = await boot(page, { delayConfirm: true });
  await open(page);
  expect(await names(page)).toEqual(['employer_payment_setup_attempted', 'employer_payment_setup_api_succeeded']);
  await ready(page);
  await expect.poll(() => names(page)).toContain('employer_payment_setup_form_opened');
  await page.locator('#confirmEmployerPaymentBtn').click();
  await expect.poll(() => names(page)).toContain('employer_payment_setup_proc_succeeded');
  expect(await names(page)).not.toContain('employer_payment_setup_completed');
  await page.evaluate(() => __resolveConfirm());
  await expect(page.locator('#paymentSetupModal')).toHaveCount(0);
  expect(await names(page)).toEqual(['employer_payment_setup_attempted', 'employer_payment_setup_api_succeeded', 'employer_payment_setup_form_opened', 'employer_payment_setup_submit_clicked', 'employer_payment_setup_proc_succeeded', 'employer_payment_setup_completed']);
  expect(await page.evaluate(() => __calls.filter(c => c.method === 'POST'))).toEqual([
    { path: '/payments/setup-employer', method: 'POST', body: {} },
    { path: '/payments/confirm-setup-employer', method: 'POST', body: { payment_method_id: 'pm_LOCAL_ONLY' } }
  ]);
  await assertPrivate(page); expect(unexpected).toEqual([]);
});
