const { test, expect } = require('@playwright/test');

// Real local SPA/CSS; only synthetic read responses. No remote transport or writes.
test.beforeEach(async ({ page, baseURL }) => {
  page.journeyErrors = [];
  page.journeyWrites = [];
  page.on('pageerror', error => page.journeyErrors.push(error.message));
  await page.route('**/*', route => {
    const request = route.request();
    const url = new URL(request.url());
    if (request.method() !== 'GET') {
      page.journeyWrites.push(request.method() + ' ' + url.pathname);
      return route.abort();
    }
    if (url.origin === new URL(baseURL).origin) return route.continue();
    if (url.hostname === 'gohirehumans-production.up.railway.app') {
      let data = {};
      if (url.pathname === '/categories') data = { categories: ['testing', 'other'] };
      if (url.pathname === '/jobs') data = { jobs: [{ id: 123, title: 'Review a signup flow', description: 'A bounded browser review.', budget_amount: 100, budget_type: 'fixed', status: 'open', category: 'testing', location_type: 'remote' }], total: 1 };
      if (url.pathname === '/jobs/123') data = { id: 123, employer_id: 42, title: 'Review a signup flow', description: 'A bounded browser review.', budget_amount: 100, budget_type: 'fixed', status: 'open', category: 'testing' };
      if (url.pathname === '/services/321') data = { id: 321, title: 'Review service', price: 25, pricing_type: 'fixed', category: 'testing', provider_type: 'human' };
      return route.fulfill({ json: data });
    }
    return route.abort();
  });
  await page.goto('/#/terms');
  await expect(page.locator('h1').first()).toHaveText('Terms of Service');
});
test.afterEach(async ({ page }) => {
  expect(page.journeyErrors).toEqual([]);
  expect(page.journeyWrites).toEqual([]);
});

test('Find Work leads with results and accessible optional mobile filters', async ({ page }, testInfo) => {
  await page.goto('/#/jobs');
  await expect(page.locator('#jobs-summary')).toContainText('1 job accepting applications');
  await page.screenshot({ path: testInfo.outputPath('jobs-initial.png'), fullPage: true });
  const mobile = testInfo.project.name.includes('mobile');
  const filters = page.locator('[data-job-filters]');
  const toggle = page.getByRole('button', { name: 'Filters', exact: true });
  const firstJob = page.locator('#jobs-list .job-card').first();
  await expect(firstJob).toBeVisible();
  if (mobile) {
    await expect(filters).toBeHidden();
    await expect(toggle).toHaveAttribute('aria-controls', 'job-filters');
    await expect(toggle).toHaveAttribute('aria-expanded', 'false');
    expect((await firstJob.boundingBox()).y).toBeLessThan(360);
    await toggle.focus();
    await page.keyboard.press('Enter');
    await expect(filters).toBeVisible();
    await expect(toggle).toHaveAttribute('aria-expanded', 'true');
    await expect(page.locator('#job-search')).toBeFocused();
    await page.screenshot({ path: testInfo.outputPath('jobs-filters-expanded.png'), fullPage: true });
    await page.keyboard.press('Escape');
    await expect(filters).toBeHidden();
    await expect(toggle).toBeFocused();
  } else {
    await expect(filters).toBeVisible();
    await expect(toggle).toBeHidden();
    expect((await filters.boundingBox()).x).toBeLessThan((await firstJob.boundingBox()).x);
  }
  const employerLink = page.getByRole('link', { name: 'Post a job instead' });
  await expect(employerLink).toHaveAttribute('href', '#/post-job');
  expect((await employerLink.boundingBox()).y).toBeGreaterThan((await firstJob.boundingBox()).y);
  await expect(page.locator('[data-job-filters] .btn-primary')).toHaveCount(0);
});

for (const [intent, heading, amount, next] of [
  ['post-job?template=website_qa', 'Test my website signup flow', null, 'Review and edit'],
  ['jobs/123?apply=1', 'Review a signup flow', '$100', 'Review your application'],
  ['services/321', 'Review service', '$25', 'Review service details'],
]) {
  for (const source of ['redirect', 'storage']) {
    test(`auth context preserves ${intent} from ${source} without submitting`, async ({ page }, testInfo) => {
      await page.evaluate(({ intent, source }) => {
        if (source === 'storage') sessionStorage.setItem('ghh_auth_intent', intent);
        navigate('#/login' + (source === 'redirect' ? '?redirect=' + encodeURIComponent(intent) : ''));
      }, { intent, source });
      const context = page.locator('[data-auth-context]');
      await expect(context).toContainText(heading);
      // Compare the selected offer to its canonical draft, not an obsolete price.
      const expectedAmount = amount || await page.evaluate(() => '$' + getTaskDraftTemplate('website_qa').budget_amount);
      await expect(context).toContainText(expectedAmount);
      await expect(context).toContainText(next);
      await expect(context).toContainText('Nothing is submitted or charged');
      await page.screenshot({ path: testInfo.outputPath('auth-login-context.png'), fullPage: true });
      await page.locator('.auth2-toggle a').click();
      await expect(context).toContainText(heading);
      await expect(page.locator('#auth-name')).toBeVisible();
      await page.screenshot({ path: testInfo.outputPath('auth-context.png'), fullPage: true });
      await page.locator('.auth2-toggle a').click();
      await expect(page.locator('#auth-name')).toHaveCount(0);
      await expect(context).toContainText(heading);
      expect(await page.evaluate(() => consumeAuthIntent())).toBe('#/' + intent);
    });
  }
}

test('auth summary never reflects private draft text or unsafe destinations', async ({ page }) => {
  for (const intent of ['post-job?draft_title=PRIVATE_SECRET&draft_description=%3Cimg%20src%3Dx%20onerror%3Dalert(1)%3E', 'https://evil.invalid/PRIVATE_SECRET', 'post-job?template=__proto__']) {
    await page.evaluate(intent => navigate('#/login?redirect=' + encodeURIComponent(intent)), intent);
    await expect(page.locator('#authForm')).toBeVisible();
    await expect(page.locator('.auth2-card')).not.toContainText('PRIVATE_SECRET');
    await expect(page.locator('.auth2-card img')).toHaveCount(0);
  }
});

for (const outcome of ['success', 'failure']) {
  test(`late auth context ${outcome} cannot overwrite an A-B-A return`, async ({ page }) => {
    await page.evaluate(() => {
      window.authPending = [];
      api = () => new Promise((resolve, reject) => authPending.push({ resolve, reject }));
      navigate('#/login?redirect=services%2F321');
    });
    await page.waitForFunction(() => authPending.length === 1);
    await page.evaluate(() => navigate('#/terms'));
    await expect(page.locator('h1').first()).toHaveText('Terms of Service');
    await page.evaluate(() => navigate('#/login?redirect=services%2F321'));
    await page.waitForFunction(() => authPending.length === 2);
    await page.evaluate(() => authPending[1].resolve({ title: 'Newest selection', price: 12.5, pricing_type: 'fixed' }));
    await expect(page.locator('[data-auth-context]')).toContainText('Newest selection');
    await page.evaluate(outcome => outcome === 'success'
      ? authPending[0].resolve({ title: 'STALE selection', price: 99, pricing_type: 'fixed' })
      : authPending[0].reject(new Error('STALE failure')), outcome);
    await expect(page.locator('[data-auth-context]')).toContainText('Newest selection');
    await expect(page.locator('[data-auth-context]')).not.toContainText('STALE');
    await expect(page.locator('[data-auth-context]')).toContainText('$12.5');
  });
}

test('auth context read failure keeps intent and enables sign-in', async ({ page }, testInfo) => {
  await page.evaluate(() => {
    api = async () => { throw new Error('PRIVATE_FAILURE'); };
    sessionStorage.setItem('ghh_auth_intent', 'jobs/123?apply=1');
    navigate('#/login');
  });
  await expect(page.locator('[data-auth-context]')).toContainText('Details are unavailable');
  await expect(page.locator('.auth2-card')).not.toContainText('PRIVATE_FAILURE');
  await expect(page.locator('#authForm button[type=submit]')).toBeEnabled();
  expect(await page.evaluate(() => sessionStorage.getItem('ghh_auth_intent'))).toBe('jobs/123?apply=1');
  await page.screenshot({ path: testInfo.outputPath('auth-unavailable.png'), fullPage: true });
});

test('auth context treats public titles as text and unknown prices as unknown', async ({ page }) => {
  for (const value of [null, '', false, 'not-a-price', 0, -5]) {
    await page.evaluate(value => {
      api = async () => ({ title: '<img src=x onerror=alert(1)>', price: value, pricing_type: 'fixed' });
      navigate('#/login?redirect=services%2F321');
      render();
    }, value);
    await expect(page.locator('[data-auth-context]')).toContainText('<img src=x onerror=alert(1)>');
    await expect(page.locator('[data-auth-context] img')).toHaveCount(0);
    await expect(page.locator('[data-auth-context]')).not.toContainText('$');
    expect(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth)).toBe(true);
  }
});

test('worker application and service processing copy is qualified', async ({ page }) => {
  await page.goto('/#/jobs/123');
  await expect(page.locator('.svc-order-note')).toContainText('You do not pay to apply or get hired');
  await expect(page.locator('.svc-order-note')).not.toContainText('until');
  await page.goto('/#/services/321');
  await expect(page.locator('.svc-order-card')).toContainText('Stripe processing where configured');
  await expect(page.locator('.svc-order-card')).not.toContainText('Secured by Stripe');
});

for (const transition of ['anonymous-to-login', 'different-token', 'same-token-new-generation', 'different-user']) {
  test(`late 401 cannot expire a newer session: ${transition}`, async ({ page }) => {
    const result = await page.evaluate(async transition => {
      if (transition !== 'anonymous-to-login') saveSession('old-token', { id: 1 });
      const originalFetch = window.fetch;
      let resolveOld;
      window.fetch = () => new Promise(resolve => { resolveOld = resolve; });
      const pending = api('/stale-probe').catch(error => error.message);
      if (transition === 'same-token-new-generation') { clearSession(); saveSession('old-token', { id: 1 }); }
      else if (transition === 'different-user') saveSession('old-token', { id: 2 });
      else saveSession('new-token', { id: 2 });
      const expectedToken = state.token;
      resolveOld(new Response(JSON.stringify({ error: 'Old unauthorized request' }), { status: 401 }));
      await pending;
      window.fetch = originalFetch;
      return { token: state.token, stored: sessionStorage.getItem('ghh_token'), expectedToken, hash: location.hash, toast: document.getElementById('toasts').textContent };
    }, transition);
    expect(result.token).toBe(result.expectedToken);
    expect(result.stored).toBe(result.expectedToken);
    expect(result.hash).toBe('#/terms');
    expect(result.toast).not.toContain('Session expired');
  });
}

test('a genuine same-session 401 still clears credentials after navigation', async ({ page }) => {
  await page.evaluate(async () => {
    saveSession('expired-token', { id: 1 });
    const originalFetch = window.fetch;
    let resolveOld;
    window.fetch = () => new Promise(resolve => { resolveOld = resolve; });
    const pending = api('/current-probe').catch(error => error.message);
    navigate('#/privacy');
    resolveOld(new Response(JSON.stringify({ error: 'Unauthorized' }), { status: 401 }));
    await pending;
    window.fetch = originalFetch;
  });
  await expect(page).toHaveURL(/#\/login$/);
  expect(await page.evaluate(() => [state.token, sessionStorage.getItem('ghh_token')])).toEqual([null, null]);
  await expect(page.locator('#toasts')).toContainText('Session expired');
});
