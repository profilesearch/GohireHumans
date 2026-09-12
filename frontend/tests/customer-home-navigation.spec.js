const { test, expect } = require('@playwright/test');
const fs = require('fs');
const path = require('path');
const source = fs.readFileSync(path.join(__dirname, '..', 'index.html'), 'utf8');
const start = source.indexOf('// ============================================================');
const script = source.slice(start, source.indexOf('  </script>', start)).replace(/\nrender\(\);\s*$/, '\n');

test.beforeEach(async ({ page }) => {
  await page.route('**/*', route => route.request().url() === 'http://audit.invalid/'
    ? route.fulfill({ contentType: 'text/html', body: '<!doctype html><html><body><div id="app"></div><div id="toasts"></div></body></html>' })
    : route.abort());
  await page.goto('http://audit.invalid/');
  await page.addScriptTag({ content: script });
  await page.evaluate(() => {
    window.pendingPreviews = [];
    api = path => {
      if (path === '/services?per_page=3') return new Promise((resolve, reject) => pendingPreviews.push({ resolve, reject }));
      throw new Error('Unexpected fixture request: ' + path);
    };
    navigate('#/');
  });
  await page.waitForFunction(() => pendingPreviews.length === 1);
});

for (const outcome of ['success', 'failure']) {
  test(`late homepage preview ${outcome} preserves the destination page`, async ({ page }) => {
    await page.evaluate(() => navigate('#/terms'));
    await expect(page.locator('h1').first()).toHaveText('Terms of Service');
    await page.evaluate(outcome => outcome === 'success'
      ? pendingPreviews[0].resolve({ services: [] })
      : pendingPreviews[0].reject(new Error('Old request failed')), outcome);
    await page.waitForTimeout(50);
    await expect(page).toHaveURL('http://audit.invalid/#/terms');
    await expect(page.locator('h1').first()).toHaveText('Terms of Service', { timeout: 500 });
  });
}

test('late homepage preview cannot replace a newer homepage or erase a draft', async ({ page }) => {
  await page.evaluate(() => navigate('#/terms'));
  await expect(page.locator('h1').first()).toHaveText('Terms of Service');
  await page.evaluate(() => navigate('#/'));
  await page.waitForFunction(() => pendingPreviews.length === 2);
  await page.evaluate(() => pendingPreviews[1].resolve({ services: [] }));
  await page.locator('#guided-task-need').fill('Check my signup flow and keep this draft');
  await page.evaluate(() => pendingPreviews[0].resolve({ services: [] }));
  await page.waitForTimeout(50);
  await expect(page.locator('#guided-task-need')).toHaveValue('Check my signup flow and keep this draft', { timeout: 500 });
});
