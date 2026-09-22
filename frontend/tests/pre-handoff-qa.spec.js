const { test, expect } = require('@playwright/test');

// Static buyer journey: no external fonts/analytics or marketplace writes.
test('agency pre-handoff page shows honest sample, scope, fees, and draft route', async ({ page }) => {
  const errors = [];
  page.on('pageerror', error => errors.push(error.message));
  page.on('console', message => { if (message.type() === 'error') errors.push(message.text()); });
  await page.route('**/*', route => {
    if (new URL(route.request().url()).hostname === '127.0.0.1') return route.continue();
    return route.fulfill({ status: 204, body: '' });
  });
  const response = await page.goto('/pre-handoff-qa.html');
  expect(response.status()).toBe(200);
  await expect(page.getByRole('heading', { level: 1 })).toContainText('Test the client-facing workflow before your client does');
  await expect(page.getByRole('heading', { name: 'Scope card' })).toBeVisible();
  await expect(page.getByText('Sample — illustrative, not a real client')).toBeVisible();
  await expect(page.locator('#sample-report tbody tr')).toHaveCount(8);
  await expect(page.locator('#scope-card')).toContainText('Workers receive the listed payout. Employers pay Stripe processing plus a 1% GoHireHumans fee where checkout is configured.');
  const draft = page.getByRole('link', { name: 'Draft this task' }).first();
  await expect(draft).toHaveAttribute('href', '/#/post-job?template=automation_verification');
  await expect(page.getByRole('link', { name: 'Email the operations team' })).toHaveAttribute('href', 'mailto:gohirehumans.operations@agentmail.to');
  expect(errors).toEqual([]);
});

test('starter automation card offers the agency-specific scope before drafting', async ({ page }) => {
  await page.route('**/*', route => {
    if (new URL(route.request().url()).hostname === '127.0.0.1') return route.continue();
    return route.fulfill({ status: 204, body: '' });
  });
  await page.goto('/starter-offers.html');
  const card = page.locator('.starter-offer-card').filter({ has: page.getByRole('heading', { name: 'Automation QA Sprint' }) });
  await expect(card.getByRole('link', { name: 'See agency pre-handoff scope and sample' })).toHaveAttribute('href', '/pre-handoff-qa.html');
});
