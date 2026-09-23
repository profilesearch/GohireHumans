const { test, expect } = require('@playwright/test');

const service = {
  id: 321, worker_id: 8, worker_name: '<b>Operator</b>', title: '<img src=x onerror=alert(1)> AI QA',
  description: 'Scoped automated work', category: 'research', provider_type: 'ai',
  pricing_type: 'fixed', price: 25, status: 'active', delivery_time_days: 2,
};
const notice = 'This AI listing will stay hidden until your Stripe payout account is verified.';

async function stub(page, seller = false) {
  const errors = [];
  page.on('pageerror', e => errors.push(e.message));
  await page.addInitScript(isSeller => {
    window.GOHIREHUMANS_API_URL = location.origin + '/fixture';
    if (isSeller) {
      sessionStorage.setItem('ghh_token', 'fixture-seller');
      localStorage.setItem('ghh_user', JSON.stringify({ id: 8, name: 'Seller' }));
    }
  }, seller);
  await page.route('**/*', async route => {
    const url = new URL(route.request().url());
    if (url.origin !== `http://127.0.0.1:${process.env.PW_PORT || 4173}`) return route.abort();
    if (url.pathname === '/config.js') return route.fulfill({ contentType: 'application/javascript', body: "window.GOHIREHUMANS_API_URL=location.origin+'/fixture';" });
    if (url.pathname.startsWith('/fixture')) {
      const path = url.pathname.slice('/fixture'.length);
      const data = path === '/services/321' ? { ...service, ...(seller ? { visibility: 'hidden_unverified_agent', notice } : {}) }
        : path === '/me/services' ? { services: [{ ...service, visibility: 'hidden_unverified_agent', notice }], total: 1, total_pages: 1 }
        : path === '/services' ? { services: [service], total: 1, total_pages: 1 }
        : path === '/profile' ? { id: 8, name: 'Seller' }
        : { categories: [], reviews: [], services: [], jobs: [] };
      return route.fulfill({ contentType: 'application/json', body: JSON.stringify(data) });
    }
    return route.continue();
  });
  return errors;
}

test('publishing policy is explicit on guidelines, agent page, and service form', async ({ page }) => {
  const errors = await stub(page, true);
  const policy = 'AI agents may list services only when clearly labeled as AI and operated by a verified person or company that completes Stripe payout verification.';
  await page.goto('/#/guidelines');
  await expect(page.locator('.legal-page')).toContainText(policy);
  await page.goto('/#/ai-employers');
  await expect(page.locator('.landing')).toContainText(policy);
  await page.goto('/#/post-service');
  await expect(page.locator('.form-page')).toContainText(policy);
  await page.goto('/#/edit-service/321');
  await expect(page.locator('.form-page')).toContainText(notice);
  expect(errors).toEqual([]);
});

for (const seller of [false, true]) {
  test(`AI label on browse card and detail; ${seller ? 'seller hidden notice' : 'public'} on mobile and desktop`, async ({ page }) => {
    const errors = await stub(page, seller);
    await page.goto('/#/services');
    await expect(page.locator('.svc-card .badge-ai')).toHaveText('AI service');
    await expect(page.locator('.svc-card')).toContainText('Delivered by an AI agent operated by a verified account holder.');
    await expect(page.locator('.svc-card img')).toHaveCount(0);
    await page.goto('/#/services/321');
    await expect(page.locator('.detail-content .badge-ai')).toHaveText('AI service');
    await expect(page.locator('.detail-content')).toContainText('Delivered by an AI agent operated by a verified account holder.');
    await expect(page.locator('.detail-content img')).toHaveCount(0);
    if (seller) {
      await expect(page.locator('.detail-page')).toContainText(notice);
      await page.goto('/#/my-services');
      await expect(page.locator('.task-card')).toContainText(notice);
    }
    expect(errors).toEqual([]);
  });
}
