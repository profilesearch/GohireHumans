const { test, expect } = require('@playwright/test');
const fs = require('node:fs');
const path = require('node:path');

// Real SPA and rendered forms; synthetic native auth/API responses only.
// Fulfill the document in-process and abort ALL other network (including GSI).
const source = fs.readFileSync(path.join(__dirname, '..', 'index.html'), 'utf8');
const start = source.indexOf('// ============================================================');
const script = source.slice(start, source.indexOf('  </script>', start)).replace(/\nrender\(\);\s*$/, '\n');
test.beforeEach(async ({ page }) => {
  page.authEvidence = { errors: [], network: [] };
  page.on('pageerror', error => page.authEvidence.errors.push(error.message));
  await page.route('**/*', route => {
    const request = route.request();
    page.authEvidence.network.push({ url: request.url(), method: request.method() });
    return request.url() === 'http://audit.invalid/' && request.method() === 'GET'
      ? route.fulfill({ contentType: 'text/html', body: '<!doctype html><html><body><div id="app"></div><div id="toasts"></div></body></html>' })
      : route.abort();
  });
  await page.goto('http://audit.invalid/');
  await page.addScriptTag({ content: script });
  await page.evaluate(() => {
    window.calls = [];
    window.newUser = false;
    window.fixtureUser = { id: 501, name: 'Local Auth Tester', email: 'auth@example.invalid', role: 'worker' };
    api = async (path, options = {}) => {
      const method = options.method || 'GET';
      calls.push({ path, method });
      if (['/auth/google', '/auth/login', '/auth/register'].includes(path) && method === 'POST') {
        return { ...fixtureUser, token: 'synthetic-auth-token', ...(path === '/auth/google' ? { is_new_user: newUser } : {}) };
      }
      if (method !== 'GET') throw new Error('Forbidden automatic mutation: ' + method + ' ' + path);
      if (path === '/categories') return { categories: ['graphic_design', 'testing', 'other'] };
      if (path === '/profile') return fixtureUser;
      if (path === '/orders') return { orders: [] };
      if (path === '/jobs/123') return { id: 123, employer_id: 42, title: 'Synthetic application job', description: 'Review this job', category: 'graphic_design', budget_amount: 100, budget_type: 'fixed', status: 'open' };
      if (path === '/services/321') return { id: 321, title: 'Synthetic review service', description: 'Review before ordering', category: 'graphic_design', price: 25, pricing_type: 'fixed', provider_type: 'human' };
      throw new Error('Unexpected fixture request: ' + path);
    };
  });
});
test.afterEach(async ({ page }) => {
  expect(page.authEvidence.errors).toEqual([]);
  expect(page.authEvidence.network).toEqual([{ url: 'http://audit.invalid/', method: 'GET' }]);
  // Includes jobs, applications, hires, orders, payments and any unknown write.
  expect(await page.evaluate(() => calls.filter(call => call.method !== 'GET' && !['/auth/google', '/auth/login', '/auth/register'].includes(call.path)))).toEqual([]);
});

async function signIn(page, method, redirect, storedIntent = '', draft = null) {
  await page.evaluate(({ method, redirect, storedIntent, draft }) => {
    newUser = method.endsWith('new');
    sessionStorage.setItem('ghh_auth_intent', storedIntent);
    if (draft) sessionStorage.setItem('ghh_guided_task_draft', JSON.stringify(draft));
    navigate('#/' + (method.endsWith('new') ? 'register' : 'login') + (redirect === null ? '' : '?redirect=' + encodeURIComponent(redirect)));
  }, { method, redirect, storedIntent, draft });
  await expect(page.locator('#authForm')).toBeVisible();
  if (method.startsWith('google')) {
    await page.evaluate(() => handleGoogleCredential({ credential: 'synthetic-google-credential' }));
  } else {
    if (method.endsWith('new')) {
      await page.locator('#auth-name').fill('Local Auth Tester');
      await page.locator('#agreeTerms').check();
    }
    await page.locator('#auth-email').fill('auth@example.invalid');
    await page.locator('#auth-password').fill('local-test-password');
    await page.locator('#authForm button[type="submit"]').click();
  }
}
async function consumed(page) {
  expect(await page.evaluate(() => sessionStorage.getItem('ghh_auth_intent'))).toBeNull();
  expect(await page.evaluate(() => state.user.id)).toBe(501);
}

// Exercise actual mode-toggle clicks: URL encoding alone does not escape JS quotes.
for (const initialMode of ['login', 'register']) {
  for (const [kind, title] of [
    ['hostile apostrophes', "'-alert(1)-'"],
    ['legitimate apostrophes', 'O\'Connor\'s "100%" café — 東京'],
  ]) {
    test(`auth mode toggles preserve ${kind} from ${initialMode}`, async ({ page }) => {
      const dialogs = [];
      page.on('dialog', async dialog => {
        dialogs.push({ type: dialog.type(), message: dialog.message() });
        await dialog.dismiss();
      });
      const description = '<img src=x onerror="window.authToggleInjected=true"> "Quote" & 100% café 東京';
      const intent = 'post-job?draft_title=' + encodeURIComponent(title)
        + '&draft_description=' + encodeURIComponent(description);
      await page.evaluate(({ initialMode, intent }) => {
        navigate('#/' + initialMode + '?redirect=' + encodeURIComponent(intent));
      }, { initialMode, intent });
      await expect(page.locator('#authForm')).toBeVisible();
      for (const mode of [initialMode === 'login' ? 'register' : 'login', initialMode]) {
        await page.locator('.auth2-toggle a').click();
        expect.soft(dialogs, 'Toggle must not execute redirect text as JavaScript').toEqual([]);
        await expect(page.locator('#auth-name')).toHaveCount(mode === 'register' ? 1 : 0);
        expect(await page.evaluate(() => currentHashWithoutQuery())).toBe('/' + mode);
        expect(await page.evaluate(() => getQuery().get('redirect'))).toBe(intent);
        expect(await page.evaluate(() => window.authToggleInjected)).toBeUndefined();
      }
      // Authenticate from the toggled form; preserve intent through consumption too.
      if (initialMode === 'register') {
        await page.locator('#auth-name').fill('Local Auth Tester');
        await page.locator('#agreeTerms').check();
      }
      await page.locator('#auth-email').fill('auth@example.invalid');
      await page.locator('#auth-password').fill('local-test-password');
      await page.locator('#authForm button[type="submit"]').click();
      await expect(page.locator('input[name="title"]')).toHaveValue(title);
      await expect(page.locator('textarea[name="description"]')).toHaveValue(description);
      await expect(page).toHaveURL('http://audit.invalid/#/' + intent);
      expect(dialogs).toEqual([]);
      expect(await page.evaluate(() => window.authToggleInjected)).toBeUndefined();
      await consumed(page);
    });
  }
}

const unsafeIntents = ['https://outside.invalid/pay', '//outside.invalid', '\\outside.invalid', 'javascript:alert(1)', '%E0%A4%A', 'services%2F321', 'post-job?draft_title=%ZZ', 'jobs/../services/321', 'login'];
for (const method of ['google-returning', 'google-new', 'email-returning', 'email-new']) {
  for (const intentSource of ['redirect', 'storage']) {
    for (const unsafe of unsafeIntents) {
      test(`${method} rejects unsafe ${intentSource} ${unsafe}`, async ({ page }) => {
        await signIn(page, method, intentSource === 'redirect' ? unsafe : null, intentSource === 'storage' ? unsafe : 'jobs/123?apply=1');
        await expect(page.locator('h1').first()).toHaveText('Dashboard', { timeout: 1500 });
        await expect(page).toHaveURL('http://audit.invalid/#/');
        await expect(page.locator('#applyForm')).toHaveCount(0);
        await consumed(page);
      });
    }
  }
  test(`${method} with no intent keeps the normal dashboard`, async ({ page }) => {
    await signIn(page, method, null);
    await expect(page.locator('h1').first()).toHaveText('Dashboard');
    await consumed(page);
  });
  test(`${method} resumes a storage-only draft without losing its content`, async ({ page }) => {
    await signIn(page, method, 'post-job', 'services/321', draft);
    await expect(page.locator('h1').first()).toHaveText('Post a Job');
    await expect(page.locator('input[name="title"]')).toHaveValue(draft.title);
    await expect(page.locator('textarea[name="description"]')).toHaveValue(draft.description);
    await expect(page.locator('#jobBudgetAmount')).toHaveValue(draft.budget_amount);
    await consumed(page);
  });
}

const draft = { title: 'Review 100% of "A & B"', description: 'First line\nCheck <details> & https://example.invalid/quote?q=1', category: 'graphic_design', budget_type: 'fixed', budget_amount: '75.50' };
const draftRoute = 'post-job?' + new URLSearchParams({ draft_title: draft.title, draft_description: draft.description, draft_budget: draft.budget_amount });
for (const method of ['google-returning', 'google-new', 'email-returning', 'email-new']) {
  for (const intentSource of ['redirect', 'storage']) {
    for (const destination of ['template', 'draft', 'application', 'service']) {
      test(`${method} resumes ${destination} from ${intentSource} without submitting`, async ({ page }) => {
        const route = { template: 'post-job?template=website_qa', draft: draftRoute, application: 'jobs/123?apply=1', service: 'services/321' }[destination];
        await signIn(page, method, intentSource === 'redirect' ? route : null, intentSource === 'storage' ? route : 'jobs/999?apply=1', destination === 'draft' ? draft : null);
        if (destination === 'template' || destination === 'draft') {
          await expect(page.locator('h1').first()).toHaveText('Post a Job', { timeout: 1500 });
          await expect(page.locator('input[name="title"]')).toHaveValue(destination === 'template' ? 'Test my website signup flow' : draft.title);
          await expect(page.locator('#postJobSubmitBtn')).toBeVisible();
          if (destination === 'draft') {
            await expect(page.locator('textarea[name="description"]')).toHaveValue(draft.description);
            await expect(page.locator('select[name="category"]')).toHaveValue('graphic_design');
            await expect(page.locator('#jobBudgetAmount')).toHaveValue(draft.budget_amount);
            expect(await page.evaluate(() => JSON.parse(sessionStorage.getItem('ghh_guided_task_draft')))).toEqual(draft);
          }
        } else if (destination === 'application') {
          await expect(page.locator('h1').first()).toHaveText('Synthetic application job', { timeout: 1500 });
          await expect(page.locator('#applyForm')).toBeVisible();
          await expect(page.locator('#apply-cover-message')).toHaveValue('');
          await expect(page.locator('#jobApplicationSubmitBtn')).toBeVisible();
        } else {
          await expect(page.locator('h1').first()).toHaveText('Synthetic review service', { timeout: 1500 });
          await expect(page.getByRole('button', { name: 'Order This Service', exact: true })).toBeVisible();
        }
        await expect(page).toHaveURL('http://audit.invalid/#/' + (destination === 'application' ? 'jobs/123' : route));
        await consumed(page);
      });
    }
  }
}
