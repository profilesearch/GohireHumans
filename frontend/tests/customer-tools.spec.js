const { test, expect } = require('@playwright/test');
const fs = require('fs');
const path = require('path');

// Synthetic customer input/clipboard/mail activation only. Serve real repository
// HTML/assets from disk; intercept ALL browser requests, never contact a server.
const origin = 'http://127.0.0.1:4192';
const root = path.resolve(__dirname, '..');
test.use({ serviceWorkers: 'block' });
test.beforeEach(async ({ context, page }) => {
  page._toolErrors = [];
  page._toolWrites = [];
  page.on('pageerror', error => page._toolErrors.push(error.message));
  await context.route('**/*', async route => {
    const request = route.request();
    if (request.method() !== 'GET') page._toolWrites.push(request.url());
    const url = new URL(request.url());
    const file = path.resolve(root, '.' + decodeURIComponent(url.pathname));
    if (url.origin !== origin || request.method() !== 'GET' || !file.startsWith(root + path.sep) || !fs.existsSync(file) || !fs.statSync(file).isFile()) return route.abort();
    const types = { '.html': 'text/html', '.js': 'application/javascript', '.css': 'text/css' };
    return route.fulfill({ body: fs.readFileSync(file), contentType: types[path.extname(file)] || 'application/octet-stream' });
  });
  await page.addInitScript(() => {
    window.syntheticClipboard = [];
    window.syntheticMailActivations = [];
    Object.defineProperty(navigator, 'clipboard', { configurable: true, value: { writeText: async text => { window.syntheticClipboard.push(text); } } });
    // Bubble phase records the href AFTER the actual page click handler, then
    // suppresses the OS mail client. No mail is opened or sent by these tests.
    document.addEventListener('click', event => {
      const link = event.target.closest('a[href^="mailto:"]');
      if (link) { window.syntheticMailActivations.push(link.href); event.preventDefault(); }
    });
  });
  page.on('dialog', dialog => dialog.dismiss());
});
test.afterEach(async ({ page }) => {
  expect(page._toolErrors).toEqual([]);
  expect(page._toolWrites).toEqual([]);
});

test('fee calculator labels buyer total and seller net without changing modeled amounts', async ({ page }) => {
  await page.goto(`${origin}/tools/freelance-fee-calculator.html`);
  const resultHeading = page.locator('.compare-table thead th').nth(2);
  await expect(resultHeading).toHaveText('You net');
  // Synthetic inputs; these rates lock existing behavior, NOT a pricing audit.
  const rates = {
    seller: { GoHireHumans: 0, Upwork: 0.10, Toptal: 0.20, Fiverr: 0.20, 'Freelancer.com': 0.10 },
    buyer: { GoHireHumans: 0.01, Upwork: 0.05, Toptal: 0, Fiverr: 0.055, 'Freelancer.com': 0.03 },
  };
  const money = n => '$' + Math.round(n).toLocaleString('en-US');
  for (const role of ['buyer', 'seller', 'buyer', 'seller']) {
    await page.locator('#role').selectOption(role);
    await expect(resultHeading).toHaveText(role === 'buyer' ? 'Your total' : 'You net');
    for (const amount of [2000, 500, 0]) {
      await page.locator('#gross').fill(String(amount));
      for (const contract of ['fixed', 'hourly']) {
        await page.locator('#contract').selectOption(contract);
        await expect(resultHeading).toHaveText(role === 'buyer' ? 'Your total' : 'You net');
        await expect(page.locator('#rows tr')).toHaveCount(5);
        for (const [platform, rate] of Object.entries(rates[role])) {
          const row = page.locator('#rows tr').filter({ has: page.getByText(platform, { exact: true }) });
          await expect(row.locator('td').nth(1)).toHaveText(`${money(amount * rate)} (${Math.round(rate * 100)}%)`);
          await expect(row.locator('.net-cell')).toHaveText(money(role === 'buyer' ? amount + amount * rate : amount - amount * rate));
        }
      }
    }
  }
});

async function expectCurrentBrief(page, fragments) {
  const text = await page.locator('#out').textContent();
  for (const fragment of fragments) expect(text).toContain(fragment);
  const link = new URL(await page.locator('#managedRequest').getAttribute('href'));
  expect(link.protocol).toBe('mailto:');
  expect(link.pathname).toBe('gohirehumans.operations@agentmail.to');
  expect(link.searchParams.get('body')).toBe(text);
  expect(link.searchParams.get('subject')).toBe('Managed AI QA pilot request: ' + text.split('\n')[0].replace('Title: ', ''));
  return text;
}

for (const filename of ['ai-qa-buyer-brief.html', 'ai-qa-task-generator.html']) {
  for (const action of ['copy', 'email']) {
    test(`${filename}: ${action} refreshes unannounced form edits immediately before action`, async ({ page }) => {
      await page.goto(`${origin}/${filename}`);
      await page.getByRole('button', { name: /^Generate/ }).click();
      // Synthetic silent value changes model autofill/restore that emits no event.
      await page.evaluate(() => {
        document.getElementById('output').value = 'Synthetic silent output & 50%';
        document.getElementById('context').value = 'Synthetic silent context';
        document.getElementById('type').value = 'agent-work-audit';
        document.getElementById('budget').selectedIndex = 3;
      });
      expect(await page.evaluate(() => syntheticMailActivations)).toEqual([]);
      if (action === 'copy') await page.getByRole('button', { name: 'Copy brief', exact: true }).click();
      else {
        await page.locator('#managedRequest').focus();
        await page.keyboard.press('Enter');
      }
      const text = await expectCurrentBrief(page, ['Synthetic silent output & 50%', 'Synthetic silent context', 'Review AI-agent work before action', '$200–$500 batch or high-context review']);
      if (action === 'copy') {
        expect(await page.evaluate(() => syntheticClipboard)).toEqual([text]);
        expect(await page.evaluate(() => syntheticMailActivations)).toEqual([]);
      } else {
        const activations = await page.evaluate(() => syntheticMailActivations);
        expect(activations).toHaveLength(1);
        expect(new URL(activations[0]).searchParams.get('body')).toBe(text);
        expect(await page.evaluate(() => syntheticClipboard)).toEqual([]);
      }
    });
  }
  test(`${filename}: edits after generate keep preview and email current`, async ({ page }) => {
    await page.goto(`${origin}/${filename}?service=source-check`);
    await expect(page.locator('#type')).toHaveValue('citation-check');
    await page.locator('#output').fill('Synthetic initial output');
    await page.getByRole('button', { name: /^Generate/ }).click();
    await expectCurrentBrief(page, ['Synthetic initial output', 'Verify AI sources and citations']);
    const revised = 'Synthetic revised output: A&B + 50%\n<em>literal, not HTML</em> 日本語';
    await page.locator('#output').fill(revised);
    await expectCurrentBrief(page, [revised]);
    await expect(page.locator('#out em')).toHaveCount(0);
    await page.locator('#context').fill('Synthetic revised context & source=local');
    await expectCurrentBrief(page, [revised, 'Synthetic revised context & source=local']);
    await page.locator('#type').selectOption('website-qa');
    await expectCurrentBrief(page, ['Test an AI-built website flow', revised]);
    await page.locator('#budget').selectOption({ index: 2 });
    await expectCurrentBrief(page, ['$75–$200 multi-step QA', revised]);
    await page.locator('#output').fill('');
    await page.locator('#context').fill('');
    await expectCurrentBrief(page, ['[paste AI output/work here]', '[paste source/context here]']);
    expect(await page.evaluate(() => syntheticMailActivations)).toEqual([]);
  });
}
