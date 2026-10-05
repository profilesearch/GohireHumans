const { test, expect } = require('@playwright/test');

const INTENT = 'ghh_apply_payout_return';
const job = { id: 33, employer_id: 2, title: 'Write 5 product descriptions', description: 'Five descriptions',
  category: 'copywriting', budget_type: 'fixed', budget_amount: 60, status: 'reviewing',
  location_type: 'remote', created_at: '2026-09-29T12:00:00Z', hiring_enabled: true,
  viewer_application: null, viewer_can_apply: false, viewer_apply_requirement: 'payout_setup' };

async function boot(page, options = {}) {
  const state = { job: { ...job, ...options.job }, ready: options.ready === true, posts: 0 };
  await page.addInitScript(({ signedIn, intent, key }) => {
    if (signedIn) {
      sessionStorage.setItem('ghh_token', 'worker-token');
      localStorage.setItem('ghh_user', JSON.stringify({ id: 9, name: 'Worker', is_admin: false }));
    }
    if (intent !== undefined) sessionStorage.setItem(key, intent);
    window.__events = [];
    window.gtag = (...args) => window.__events.push(args);
  }, { signedIn: options.signedIn !== false, intent: options.intent, key: INTENT });
  // All nonlocal requests are denied unless explicitly fulfilled as fixtures below.
  await page.route('**/*', async route => {
    const req = route.request(), url = new URL(req.url());
    if (['127.0.0.1', 'localhost'].includes(url.hostname)) return route.continue();
    if (url.hostname !== 'gohirehumans-production.up.railway.app') return route.abort();
    let body = {};
    if (url.pathname === '/jobs/33' && req.method() === 'GET') body = state.job;
    if (url.pathname === '/payments/status') body = { employer_ready: false, worker_ready: state.ready };
    if (url.pathname === '/payments/history') body = { escrow_history: [] };
    if (url.pathname === '/payments/connect-countries') body = { countries: [{ code: 'US', name: 'United States' }] };
    if (url.pathname === '/auth/login') body = { token: 'worker-token', user: { id: 9, name: 'Worker' } };
    if (url.pathname === '/jobs/33/apply' && req.method() === 'POST') {
      state.posts++;
      return route.fulfill({ status: 403, contentType: 'application/json', body: JSON.stringify({
        code: 'payout_setup_required', error: 'Finish payout setup before applying. <img src=x onerror=alert(1)>' }) });
    }
    return route.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify(body) });
  });
  await page.goto(options.route || '/#/jobs/33', { waitUntil: 'domcontentloaded' });
  return state;
}

const payoutCTA = page => page.getByRole('button', { name: 'Set up payouts to apply', exact: true });
const returnCTA = page => page.getByRole('link', { name: 'Back to job to apply', exact: true });

test('not-ready worker gets payout CTA, honest copy and no apply form or withdrawal claim', async ({ page }) => {
  const state = await boot(page, { route: '/#/jobs/33?apply=1' });
  await expect(payoutCTA(page)).toBeVisible();
  const card = page.locator('.svc-order-card');
  await expect(card).toContainText('Buyers can only hire workers who can be paid');
  await expect(card).toContainText('free');
  await expect(card).toContainText('a few minutes through Stripe');
  await expect(card).toContainText('Nothing is charged');
  await expect(card).not.toContainText("You've withdrawn");
  await expect(page.getByRole('button', { name: 'Apply to This Job', exact: true })).toHaveCount(0);
  await expect(page.locator('#applyForm')).toHaveCount(0);
  expect(state.posts).toBe(0);
  expect(await page.evaluate(() => window.__events.filter(a => a[0] === 'event' && !['spa_page_view', 'page_view'].includes(a[1])).map(a => a[1])))
    .toEqual(['apply_payout_required_shown']);
});

test('payout CTA stores numeric return intent and navigates without an application', async ({ page }) => {
  const state = await boot(page);
  await payoutCTA(page).click();
  await expect(page).toHaveURL(/#\/payments$/);
  expect(await page.evaluate(key => sessionStorage.getItem(key), INTENT)).toBe('33');
  await expect(returnCTA(page)).toHaveCount(0);
  expect(state.posts).toBe(0);
  const names = await page.evaluate(() => window.__events.filter(a => a[0] === 'event' && !['spa_page_view', 'page_view'].includes(a[1])).map(a => a[1]));
  expect(names).toEqual(['apply_payout_required_shown', 'apply_payout_setup_clicked']);
});

test('ready payments callout returns once to the job and opens but never submits a form', async ({ page }) => {
  const state = await boot(page, { route: '/#/payments', ready: true, intent: '33',
    job: { viewer_can_apply: true, viewer_apply_requirement: null } });
  await expect(page.getByText('You can apply now', { exact: true })).toBeVisible();
  await expect(returnCTA(page)).toHaveAttribute('href', '#/jobs/33?apply=1');
  await returnCTA(page).click();
  await expect(page.locator('#applyForm')).toBeVisible();
  expect(await page.evaluate(key => sessionStorage.getItem(key), INTENT)).toBeNull();
  expect(state.posts).toBe(0);
  expect(await page.evaluate(() => window.__events.filter(a => a[0] === 'event' && !['spa_page_view', 'page_view'].includes(a[1])).map(a => a[1])))
    .toEqual(expect.arrayContaining(['apply_payout_return_clicked', 'job_apply_intent', 'job_apply_form_opened']));
  await page.locator('#applyForm').getByRole('button', { name: 'Cancel' }).click();
  await page.evaluate(() => navigate('#/payments'));
  await expect(page.getByRole('heading', { name: 'Payments', exact: true })).toBeVisible();
  await expect(returnCTA(page)).toHaveCount(0);
});

test('intent survives not-ready payments render until readiness is confirmed', async ({ page }) => {
  const state = await boot(page, { route: '/#/payments', intent: '33' });
  await expect(page.getByRole('heading', { name: 'Payments', exact: true })).toBeVisible();
  await expect(returnCTA(page)).toHaveCount(0);
  expect(await page.evaluate(key => sessionStorage.getItem(key), INTENT)).toBe('33');
  state.ready = true;
  await page.evaluate(() => renderPayments());
  await expect(returnCTA(page)).toBeVisible();
});

for (const intent of ['0', '-1', '33.5', '033', '33junk', '33\n', '9007199254740992', '#/jobs/33', '<img src=x>']) {
  test(`malformed payout return intent is ignored: ${JSON.stringify(intent)}`, async ({ page }) => {
    await boot(page, { route: '/#/payments', ready: true, intent });
    await expect(page.getByRole('heading', { name: 'Payments', exact: true })).toBeVisible();
    await expect(returnCTA(page)).toHaveCount(0);
    await expect(page.getByText('You can apply now', { exact: true })).toHaveCount(0);
  });
}

test('withdrawn twice shows only the withdrawal reason', async ({ page }) => {
  await boot(page, { job: { viewer_apply_requirement: null, viewer_withdrawal_limit_reached: true }, route: '/#/jobs/33?apply=1' });
  await expect(page.locator('.svc-order-card')).toContainText("You've withdrawn from this job twice");
  await expect(payoutCTA(page)).toHaveCount(0);
  await expect(page.locator('#applyForm')).toHaveCount(0);
});

test('other apply blocks never claim the worker withdrew twice', async ({ page }) => {
  await boot(page, { job: { viewer_apply_requirement: null, viewer_withdrawal_limit_reached: false } });
  await expect(page.locator('.svc-order-card')).toContainText('You cannot apply to this job right now');
  await expect(page.locator('.svc-order-card')).not.toContainText("You've withdrawn");
  await expect(payoutCTA(page)).toHaveCount(0);
});

test('ready worker still sees Apply to This Job', async ({ page }) => {
  await boot(page, { job: { viewer_can_apply: true, viewer_apply_requirement: null } });
  await expect(page.getByRole('button', { name: 'Apply to This Job', exact: true })).toBeVisible();
  await expect(payoutCTA(page)).toHaveCount(0);
});

test('anonymous sign-in resumes with payout CTA instead of auto-opening application', async ({ page }) => {
  const state = await boot(page, { signedIn: false });
  await page.getByRole('button', { name: 'Sign in to Apply', exact: true }).click();
  await page.locator('#auth-email').fill('worker@example.com');
  // Synthetic credentials only, with the login response fulfilled locally.
  await page.locator('#auth-password').fill('local-fixture-only');
  await page.locator('#authForm button[type="submit"]').click();
  await expect(payoutCTA(page)).toBeVisible();
  await expect(page.locator('#applyForm')).toHaveCount(0);
  expect(state.posts).toBe(0);
});

test('stale apply rejection shows safe payout link with a saved return intent', async ({ page }) => {
  const state = await boot(page, { job: { viewer_can_apply: true, viewer_apply_requirement: null } });
  await page.getByRole('button', { name: 'Apply to This Job', exact: true }).click();
  await page.locator('#apply-cover-message').fill('I can write these descriptions by tomorrow.');
  await page.locator('#jobApplicationSubmitBtn').click();
  const error = page.locator('#jobApplicationError');
  await expect(error).toBeVisible();
  await expect(error).toContainText('Finish payout setup before applying');
  await expect(error.locator('img')).toHaveCount(0);
  const link = error.getByRole('link', { name: 'Set up payouts to apply', exact: true });
  await expect(link).toHaveAttribute('href', '#/payments');
  await link.click();
  await expect(page).toHaveURL(/#\/payments$/);
  await expect(page.locator('#applyForm')).toHaveCount(0);
  expect(await page.evaluate(key => sessionStorage.getItem(key), INTENT)).toBe('33');
  expect(state.posts).toBe(1);
  const names = await page.evaluate(() => window.__events.filter(a => a[0] === 'event' && !['spa_page_view', 'page_view'].includes(a[1])).map(a => a[1]));
  for (const event of ['job_application_completed', 'generate_lead', 'qualify_lead']) expect(names).not.toContain(event);
});
