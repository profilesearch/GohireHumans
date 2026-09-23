const { test, expect } = require('@playwright/test');
const fs = require('node:fs');
const path = require('node:path');
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
    window.calls = [];
    api = async (url, opts) => {
      calls.push({ url, body: opts?.body });
      if (url === '/auth/password-reset/available') return { available: true };
      if (url === '/auth/forgot-password') return { message: 'If an eligible account exists, a password reset link will be emailed.' };
      if (url === '/auth/reset-password') return { message: 'Password updated.' };
      throw new Error('Unexpected API call');
    };
  });
});

for (const state of [false, 'error']) {
  test(`login and forgot form fail closed on availability ${state}`, async ({ page }) => {
    await page.evaluate(state => {
      api = async url => {
        if (url === '/auth/password-reset/available') {
          if (state === 'error') throw new Error('offline');
          return { available: false };
        }
        throw new Error('Unexpected API call');
      };
      navigate('#/login');
    }, state);
    await expect(page.getByRole('link', { name: 'Forgot password?' })).toHaveCount(0);
    await page.evaluate(() => navigate('#/forgot-password'));
    await expect(page.locator('#forgot-password-form')).toHaveCount(0);
    await expect(page.getByText("Password reset by email isn't available right now. Contact gohirehumans.operations@agentmail.to for help.")).toBeVisible();
  });
}

for (const viewport of ['desktop', 'mobile']) {
  test(`${viewport}: forgot link, generic message, reset mismatch and URL secrecy`, async ({ page }) => {
    if (viewport === 'mobile') await page.setViewportSize({ width: 390, height: 844 });
    await page.evaluate(() => navigate('#/login'));
    await page.getByRole('link', { name: 'Forgot password?' }).click();
    await expect(page.getByRole('heading', { name: 'Forgot password?' })).toBeVisible();
    await page.locator('#reset-email').fill('person@example.com');
    await page.locator('#forgot-password-form button[type="submit"]').click();
    await expect(page.getByText('If an eligible account exists, a password reset link will be emailed.')).toBeVisible();
    expect(await page.evaluate(() => calls.find(call => call.url === '/auth/forgot-password'))).toEqual({ url: '/auth/forgot-password', body: { email: 'person@example.com' } });

    await page.evaluate(() => navigate('#/reset-password?token=opaque_test_token'));
    await expect(page.getByRole('heading', { name: 'Set a new password' })).toBeVisible();
    expect(page.url()).not.toContain('opaque_test_token');
    await page.locator('#new-password').fill('replacement-one');
    await page.locator('#confirm-password').fill('replacement-two');
    await page.locator('#reset-password-form button[type="submit"]').click();
    await expect(page.getByRole('alert')).toContainText('do not match');
    expect(await page.evaluate(() => calls.filter(call => call.url !== '/auth/password-reset/available').length)).toBe(1);
    await page.locator('#confirm-password').fill('replacement-one');
    await page.locator('#reset-password-form button[type="submit"]').click();
    await expect(page.getByText('Password updated.')).toBeVisible();
    expect(await page.evaluate(() => calls.find(call => call.url === '/auth/reset-password'))).toEqual({ url: '/auth/reset-password', body: { token: 'opaque_test_token', new_password: 'replacement-one' } });
    await expect(page.getByRole('link', { name: 'Sign in' })).toBeVisible();
    expect(page.url()).not.toContain('opaque_test_token');
    expect(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth)).toBe(true);
  });
}
