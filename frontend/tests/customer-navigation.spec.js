const { test, expect } = require('@playwright/test');
const fs = require('fs');
const path = require('path');

// Execute the real inline SPA against local, synthetic API fixtures. No external
// requests (including authentication, payment, analytics or Google) may escape.
const source = fs.readFileSync(path.join(__dirname, '..', 'index.html'), 'utf8');
const start = source.indexOf('// ============================================================');
const script = source.slice(start, source.indexOf('  </script>', start)).replace(/\nrender\(\);\s*$/, '\n');
test.beforeEach(async ({ page }) => {
  const errors = [];
  page.on('pageerror', error => errors.push(error.message));
  page._navigationErrors = errors;
  await page.route('**/*', route => route.request().url() === 'http://audit.invalid/'
    ? route.fulfill({ contentType: 'text/html', body: '<!doctype html><html><body><div id="app"></div><div id="toasts"></div></body></html>' })
    : route.abort());
  await page.goto('http://audit.invalid/');
  await page.addScriptTag({ content: script });
  await page.evaluate(() => {
    window.calls = [];
    window.service = { id: 321, title: 'Local service A', description: 'Synthetic fixture', category: 'design', provider_type: 'ai', price: 25, pricing_type: 'fixed' };
    window.job = { id: 123, title: 'Local job A', description: 'Synthetic fixture', category: 'design', budget: 100, status: 'open', employer_id: 42 };
    window.normalApi = async (path, options = {}) => {
      calls.push({ path, method: options.method || 'GET' });
      if (path === '/categories') return { categories: ['design', 'writing'] };
      if (path.startsWith('/services?')) return { services: [service], total: 1, page: 1, per_page: 12 };
      if (path.startsWith('/jobs?')) return { jobs: [job], total: 1, page: 1, per_page: 15 };
      if (/^\/services\/\d+$/.test(path)) return { ...service, id: Number(path.split('/')[2]), title: path.endsWith('/321') ? service.title : 'Local service B' };
      if (/^\/jobs\/\d+$/.test(path)) return { ...job, id: Number(path.split('/')[2]), title: path.endsWith('/123') ? job.title : 'Local job B' };
      if (path.includes('/reviews')) return { reviews: [] };
      if (path === '/platform/stats') return {};
      throw new Error('Unexpected fixture request: ' + path);
    };
    api = normalApi;
  });
});
test.afterEach(async ({ page }) => expect(page._navigationErrors).toEqual([]));

for (const [kind, input, id] of [['services', 'srv-search', 321], ['jobs', 'job-search', 123]]) {
  for (const destination of [`#/${kind}/${id}`, '#/terms']) {
    test(`${kind} pending search cannot steal navigation to ${destination}`, async ({ page }) => {
      await page.evaluate(kind => navigate('#/' + kind), kind);
      await page.fill('#' + input, 'pending search');
      await page.evaluate(destination => navigate(destination), destination);
      await expect(page).toHaveURL('http://audit.invalid/' + destination);
      await page.waitForTimeout(550);
      expect(await page.evaluate(() => location.hash)).toBe(destination);
      await expect(page.locator('h1').first()).toHaveText(destination === '#/terms' ? 'Terms of Service' : `Local ${kind === 'jobs' ? 'job' : 'service'} A`);
    });
  }
  test(`${kind} search still applies on its current browse route`, async ({ page }) => {
    await page.evaluate(kind => navigate('#/' + kind), kind);
    await page.fill('#' + input, 'verification');
    await expect(page).toHaveURL(new RegExp(kind + '\\?search=verification'));
  });
}

for (const [kind, id] of [['services', 321], ['jobs', 123]]) {
  for (const outcome of ['success', 'failure']) {
    for (const destination of ['terms', 'other-detail', 'same-detail']) {
      test(`${kind} late detail ${outcome} cannot overwrite ${destination}`, async ({ page }) => {
        await page.evaluate(({ kind, id }) => {
          window.pending = [];
          api = (path, options) => path === `/${kind}/${id}`
            ? new Promise((resolve, reject) => pending.push({ resolve, reject }))
            : normalApi(path, options);
          navigate(`#/${kind}/${id}`);
        }, { kind, id });
        await page.waitForFunction(() => pending.length === 1);
        const target = destination === 'terms' ? '#/terms' : `#/${kind}/${destination === 'same-detail' ? id : id + 1}`;
        if (destination === 'same-detail') {
          await page.evaluate(() => navigate('#/terms'));
          await expect(page.locator('h1').first()).toHaveText('Terms of Service');
        }
        await page.evaluate(target => navigate(target), target);
        if (destination === 'same-detail') {
          await page.waitForFunction(() => pending.length === 2);
          await page.evaluate(kind => pending[1].resolve({ ...(kind === 'jobs' ? job : service), title: 'Newest detail' }), kind);
        }
        const title = destination === 'terms' ? 'Terms of Service' : destination === 'same-detail' ? 'Newest detail' : `Local ${kind === 'jobs' ? 'job' : 'service'} B`;
        await expect(page.locator('h1').first()).toHaveText(title);
        await page.evaluate(({ kind, outcome }) => {
          if (outcome === 'failure') pending[0].reject(new Error('Old request failed'));
          else pending[0].resolve(kind === 'jobs' ? job : service);
        }, { kind, outcome });
        await page.waitForTimeout(50);
        expect(await page.evaluate(() => location.hash)).toBe(target);
        await expect(page.locator('h1').first()).toHaveText(title, { timeout: 500 });
        if (destination === 'terms') await expect(page.locator('[onclick^="handleOrderService"], [onclick^="handleJobApply"]')).toHaveCount(0);
      });
    }
  }
}

test('late service reviews cannot overwrite the next route', async ({ page }) => {
  await page.evaluate(() => {
    service.worker_id = 42;
    api = (path, options) => path.includes('/reviews') ? new Promise(resolve => { window.finishReviews = resolve; }) : normalApi(path, options);
    navigate('#/services/321');
  });
  await page.waitForFunction(() => typeof finishReviews === 'function');
  await page.evaluate(() => navigate('#/terms'));
  await expect(page.locator('h1').first()).toHaveText('Terms of Service');
  await page.evaluate(() => finishReviews({ reviews: [] }));
  await page.waitForTimeout(50);
  await expect(page.locator('h1').first()).toHaveText('Terms of Service', { timeout: 500 });
});

for (const kind of ['services', 'jobs']) {
  test(`${kind} late list pagination cannot steal the current route`, async ({ page }) => {
    await page.evaluate(kind => {
      api = (path, options) => path.startsWith(`/${kind}?`) && path.includes('page=99&')
        ? new Promise(resolve => { window.finishList = resolve; }) : normalApi(path, options);
      navigate(`#/${kind}?page=99`);
    }, kind);
    await page.waitForFunction(() => typeof finishList === 'function');
    await page.evaluate(() => navigate('#/terms'));
    await page.evaluate(kind => finishList({ [kind]: [], total: 1, page: 99 }), kind);
    await page.waitForTimeout(50);
    expect(await page.evaluate(() => location.hash)).toBe('#/terms');
    await expect(page.locator('h1').first()).toHaveText('Terms of Service');
  });

  test(`${kind} late list counts cannot overwrite a newer filter result`, async ({ page }) => {
    await page.evaluate(kind => {
      api = (path, options) => path.startsWith(`/${kind}?`) && path.includes('search=old')
        ? new Promise(resolve => { window.finishList = resolve; }) : normalApi(path, options);
      navigate(`#/${kind}?search=old`);
    }, kind);
    await page.waitForFunction(() => typeof finishList === 'function');
    await page.evaluate(kind => navigate(`#/${kind}?search=new`), kind);
    const summary = page.locator(kind === 'services' ? '#services-result-count' : '#jobs-summary');
    await expect(summary).toContainText(kind === 'services' ? '1 service' : '1 job');
    await page.evaluate(kind => finishList({ [kind]: [], total: 0, page: 1 }), kind);
    await page.waitForTimeout(50);
    await expect(summary).toContainText(kind === 'services' ? '1 service' : '1 job', {timeout:500});
  });

  test(`${kind} late categories cannot duplicate options or issue stale searches`, async ({ page }) => {
    await page.evaluate(kind => {
      window.categoriesPending = [];
      api = (path, options) => path === '/categories'
        ? new Promise(resolve => categoriesPending.push(resolve)) : normalApi(path, options);
      navigate(`#/${kind}?search=old`);
    }, kind);
    await page.waitForFunction(() => categoriesPending.length === 1);
    await page.evaluate(kind => navigate(`#/${kind}?search=new`), kind);
    await page.waitForFunction(() => categoriesPending.length === 2);
    await page.evaluate(() => categoriesPending[1]({categories:['graphic_design']}));
    const option = page.locator((kind === 'services' ? '#srv-cat' : '#job-cat') + ' option[value="graphic_design"]');
    await expect(option).toHaveCount(1);
    await page.evaluate(() => categoriesPending[0]({categories:['graphic_design']}));
    await page.waitForTimeout(50);
    await expect(option).toHaveCount(1, {timeout:500});
    expect(await page.evaluate(() => calls.some(call => call.path.includes('search=old')))).toBe(false);
  });
}

for (const provider of ['ai', 'human']) {
  test(`provider ${provider} survives combined category, search and price filters`, async ({ page }) => {
    await page.evaluate(() => navigate('#/services'));
    await page.selectOption('#srv-provider', provider);
    await expect(page.locator('#srv-provider')).toHaveValue(provider, { timeout: 1000 });
    await page.selectOption('#srv-cat', 'design');
    await page.fill('#srv-search', 'verification');
    await expect(page).toHaveURL(/search=verification/);
    await page.fill('#srv-min', '10');
    await page.locator('#srv-min').dispatchEvent('change');
    await expect(page).toHaveURL(/min_price=10/);
    await page.fill('#srv-max', '100');
    await page.locator('#srv-max').dispatchEvent('change');
    await expect(page).toHaveURL(/max_price=100/);
    await expect(page.locator('#srv-provider')).toHaveValue(provider);
    const params = await page.evaluate(() => Object.fromEntries(new URLSearchParams(calls.filter(c => c.path.startsWith('/services?')).at(-1).path.split('?')[1])));
    expect(params).toMatchObject({ provider_type: provider, category: 'design', search: 'verification', min_price: '10', max_price: '100' });
    await page.evaluate(provider => navigate('#/services?provider_type=' + provider + '&category=design&min_price=10'), provider);
    await expect(page.locator('#srv-provider')).toHaveValue(provider);
    await page.getByRole('button', { name: 'Clear filters', exact: true }).click();
    await expect(page.locator('#srv-provider')).toHaveValue('');
    await expect(page).toHaveURL('http://audit.invalid/#/services');
  });
}
