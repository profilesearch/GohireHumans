const { test, expect } = require('@playwright/test');

const service = { id: 10, worker_id: 1, worker_name: 'Worker', title: 'Precise service', description: 'Deliver precise work', category: 'testing', pricing_type: 'fixed', price: 12.50, status: 'active', provider_type: 'human', worker_rating: 5, worker_review_count: 1, avg_rating: 5, total_reviews: 1 };
const job = { id: 20, employer_id: 2, employer_name: 'Employer', title: 'Precise job', description: 'Check the details', category: 'testing', budget_type: 'fixed', budget_amount: 33.33, status: 'open', location_type: 'remote', required_skills: '[]' };

async function localOnly(page) {
  await page.addInitScript(() => { window.GOHIREHUMANS_API_URL = `${location.origin}/mock-api`; });
  await page.route('**/*', route => {
    const request = route.request();
    const url = new URL(request.url());
    if (url.hostname !== '127.0.0.1') return route.abort();
    if (url.pathname === '/config.js') return route.fulfill({ status: 200, contentType: 'application/javascript', body: "window.GOHIREHUMANS_API_URL = location.origin + '/mock-api';" });
    if (!url.pathname.startsWith('/mock-api')) return route.continue();
    const path = url.pathname.slice('/mock-api'.length);
    const value = path === '/categories' ? { categories: [{ id: 'testing', name: 'Testing' }] }
      : path === '/services' ? { services: [service], total: 1, page: 1, total_pages: 1 }
      : path === '/services/10' ? service
      : path === '/users/1/reviews' ? { reviews: [], total: 0 }
      : path === '/jobs' ? { jobs: [job], total: 1, page: 1, total_pages: 1 }
      : path === '/jobs/20' ? job
      : path === '/jobs/21' ? { ...job, id: 21, budget_type: 'hourly', budget_amount: 12.50 }
      : {};
    return route.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify(value) });
  });
}

test('cent-exact service and job prices appear in list and detail', async ({ page }) => {
  await localOnly(page);
  await page.goto('/#/services');
  await expect(page.locator('.svc-card-price')).toHaveText('$12.50');
  await page.goto('/#/services/10');
  await expect(page.locator('.svc-price-big')).toHaveText('$12.50');
  await page.goto('/#/jobs');
  await expect(page.locator('.job-card .job-budget')).toHaveText('$33.33 fixed');
  await page.goto('/#/jobs/20');
  await expect(page.locator('.svc-price-big')).toHaveText('$33.33 fixed');
  expect(await page.evaluate(() => formatPrice({ pricing_type: 'fixed', price: 12 }))).toBe('$12.00');
});

test('portfolio link helper rejects blank, relative, and unsafe URLs', async ({ page }) => {
  await localOnly(page);
  await page.goto('/');
  const results = await page.evaluate(() => [
    safeExternalHref(''), safeExternalHref('   '), safeExternalHref('/portfolio'),
    safeExternalHref('javascript:alert(1)'), safeExternalHref('https://example.org/portfolio')
  ]);
  expect(results).toEqual(['', '', '', '', 'https://example.org/portfolio']);
});

test('review dialog explains both-review and fourteen-day reveal', async ({ page }) => {
  await localOnly(page);
  await page.goto('/');
  await page.evaluate(() => leaveReview(20));
  await expect(page.locator('.modal-intro')).toContainText('14 days after completion');
  await expect(page.locator('.modal-intro')).toContainText('both parties');
});

test('390px posting and browse omit hourly; legacy hourly detail says not hireable', async ({ page }) => {
  await localOnly(page);
  await page.setViewportSize({ width: 390, height: 844 });
  await page.goto('/');
  await page.evaluate(() => saveSession('local-trust-fixture', { id: 2, name: 'Buyer', role: 'employer' }));
  await page.goto('/#/post-job');
  await expect(page.locator('#jobBudgetType')).toHaveValue('fixed');
  await expect(page.locator('#jobBudgetType option[value="hourly"]')).toHaveCount(0);
  await expect(page.locator('.page-desc')).toContainText('Fixed-price');
  await page.goto('/#/jobs');
  await expect(page.locator('#job-budget-type option[value="hourly"]')).toHaveCount(0);
  await page.goto('/#/jobs/21');
  await expect(page.locator('.svc-order-card')).toContainText('Hourly hiring is not available yet');
});
