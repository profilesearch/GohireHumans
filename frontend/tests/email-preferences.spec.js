const { test, expect } = require('@playwright/test');

// The daily applicant email links here with ?t=<token>. The page must never
// unsubscribe on load (link scanners prefetch), must strip the token from the
// URL, and must talk only to the API's unsubscribe endpoint.
const ORIGIN = `http://127.0.0.1:${process.env.PW_PORT || 4173}`;
const API = 'https://gohirehumans-production.up.railway.app';
const TOKEN = '12.' + 'a'.repeat(32);

async function harness(page, apiStatus = 200) {
  const calls = [];
  await page.route('**/*', route => {
    const request = route.request();
    const url = new URL(request.url());
    if (url.origin === ORIGIN && request.method() === 'GET') return route.continue();
    if (url.origin === API && url.pathname === '/email-preferences/unsubscribe' && request.method() === 'POST') {
      calls.push({ body: request.postDataJSON(), headers: request.headers() });
      return route.fulfill({
        status: apiStatus,
        contentType: 'application/json',
        headers: { 'access-control-allow-origin': ORIGIN },
        body: JSON.stringify(apiStatus === 200 ? { unsubscribed: true } : { error: 'This unsubscribe link is not valid.' }),
      });
    }
    calls.push({ unexpected: `${request.method()} ${request.url()}` });
    return route.abort();
  });
  return calls;
}

test('opening the link does nothing until the button is pressed, and hides the token', async ({ page }) => {
  const calls = await harness(page);
  await page.goto(`/email-preferences/?t=${TOKEN}`);
  await expect(page.getByRole('heading', { name: 'Daily applicant emails' })).toBeVisible();
  await expect(page.getByRole('button', { name: 'Stop these emails' })).toBeEnabled();
  expect(new URL(page.url()).search).toBe('');
  await page.waitForTimeout(300);
  expect(calls).toEqual([]);
  expect(await page.evaluate(() => typeof window.gtag)).toBe('undefined');
});

test('pressing the button sends only the token and confirms', async ({ page }) => {
  const calls = await harness(page);
  await page.goto(`/email-preferences/?t=${TOKEN}`);
  await page.getByRole('button', { name: 'Stop these emails' }).click();
  await expect(page.getByRole('status')).toHaveText('Done. You will not get daily applicant emails anymore.');
  await expect(page.getByRole('button', { name: 'Stop these emails' })).toBeHidden();
  expect(calls.length).toBe(1);
  expect(calls[0].body).toEqual({ token: TOKEN });
});

test('a rejected link explains how to stop the emails another way', async ({ page }) => {
  await harness(page, 400);
  await page.goto(`/email-preferences/?t=${TOKEN}`);
  await page.getByRole('button', { name: 'Stop these emails' }).click();
  await expect(page.getByRole('status')).toContainText('gohirehumans.operations@agentmail.to');
});

for (const bad of ['', 't=abc', `t=${TOKEN}<script>`, 't=0.' + 'a'.repeat(32)]) {
  test(`malformed link "${bad}" disables the button and calls nothing`, async ({ page }) => {
    const calls = await harness(page);
    await page.goto(`/email-preferences/${bad ? '?' + bad : ''}`);
    await expect(page.getByRole('button', { name: 'Stop these emails' })).toBeDisabled();
    await expect(page.getByRole('status')).toContainText('This link is incomplete');
    expect(calls).toEqual([]);
  });
}

test('the page has no horizontal overflow on narrow screens', async ({ page }) => {
  await harness(page);
  await page.setViewportSize({ width: 360, height: 740 });
  await page.goto(`/email-preferences/?t=${TOKEN}`);
  const overflow = await page.evaluate(() => document.documentElement.scrollWidth - document.documentElement.clientWidth);
  expect(overflow).toBeLessThanOrEqual(0);
});
