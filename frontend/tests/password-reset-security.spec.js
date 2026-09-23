const { test, expect } = require('@playwright/test');
const fs = require('node:fs');
const path = require('node:path');
const source = fs.readFileSync(path.join(__dirname, '..', 'index.html'), 'utf8');
const start = source.indexOf('// ============================================================');
const script = source.slice(start, source.indexOf('  </script>', start)).replace(/\nrender\(\);\s*$/, '\n');

test('reset capability never enters analytics', async ({ page }) => {
  await page.route('**/*', route => route.request().url() === 'http://audit.invalid/'
    ? route.fulfill({ contentType: 'text/html', body: '<!doctype html><html><body><div id="app"></div><div id="toasts"></div></body></html>' })
    : route.abort());
  await page.goto('http://audit.invalid/');
  await page.addScriptTag({ content: script });
  await page.evaluate(() => {
    window.analytics = [];
    window.gtag = (...args) => window.analytics.push(args);
    navigate('#/reset-password?token=opaque_test_token');
  });
  await expect(page.getByRole('heading', { name: 'Set a new password' })).toBeVisible();
  const result = await page.evaluate(() => ({ url: location.href, analytics: JSON.stringify(window.analytics) }));
  expect(result.url).not.toContain('opaque_test_token');
  expect(result.analytics).not.toContain('opaque_test_token');
});
