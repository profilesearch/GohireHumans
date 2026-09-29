const { test, expect } = require('@playwright/test');
const fs = require('node:fs');
const path = require('node:path');

const source = fs.readFileSync(path.join(__dirname, '..', 'index.html'), 'utf8');
const start = source.indexOf('// ============================================================');
const script = source.slice(start, source.indexOf('  </script>', start)).replace(/\nrender\(\);\s*$/, '\n');

test('notification apostrophe link stays inert and navigates on click and keyboard', async ({ page }) => {
  const errors = [], dialogs = [], network = [];
  page.on('pageerror', error => errors.push(error.message));
  page.on('dialog', dialog => { dialogs.push(dialog.message()); dialog.dismiss(); });
  await page.route('**/*', route => {
    network.push(route.request().url());
    return route.request().url() === 'http://audit.invalid/'
      ? route.fulfill({ contentType: 'text/html', body: '<!doctype html><html><body><div id="app"></div><div id="toasts"></div></body></html>' })
      : route.abort();
  });
  await page.goto('http://audit.invalid/');
  await page.addScriptTag({ content: script });
  await page.evaluate(() => {
    window.notificationActions = [];
    api = async (path, options = {}) => {
      notificationActions.push([path, options.method || 'GET']);
      if (path === '/notifications') return [{ id: 4, title: 'Safe', message: 'Open', link: "#/jobs/x'-alert(1)-'", is_read: false }];
      if (path === '/notifications/4/read') return { ok: true };
      throw new Error('Unexpected call ' + path);
    };
    navigate = link => { window.notificationNavigation = link; };
    renderNotifications();
  });
  const item = page.locator('.notif-item');
  await expect(item).toBeVisible();
  await item.click();
  await expect.poll(() => page.evaluate(() => window.notificationNavigation)).toBe("#/jobs/x'-alert(1)-'");
  expect(dialogs).toEqual([]);
  await item.focus();
  await page.keyboard.press('Enter');
  expect(dialogs).toEqual([]);
  expect(errors).toEqual([]);
  expect(network).toEqual(['http://audit.invalid/']);
});
