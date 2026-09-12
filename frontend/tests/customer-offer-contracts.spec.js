const { test, expect } = require('@playwright/test');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const root = path.resolve(__dirname, '..');
const app = fs.readFileSync(path.join(root, 'index.html'), 'utf8');
const source = app.slice(app.indexOf('const taskDraftTemplates ='), app.indexOf('function markFirstTaskWizardStarted'));
const templates = vm.runInNewContext(source + '; ({ templates: taskDraftTemplates, resolve: getTaskDraftTemplate })');
const offers = [
  ['AI Output Verification', 'ai_review', '99', 'expert_review', /2,000 words/, /10 claims/, /legal, medical, or financial/],
  ['Automation QA Sprint', 'automation_verification', '199', 'testing', /5 (?:supplied )?runs/, /one (?:agreed )?environment/i, /production changes/],
  ['Clay/GTM QA Sprint', 'clay_gtm_qa', '199', 'research', /25–50 rows/, /5 fields/, /new lead sourcing/],
  ['Real-World Check', 'phone_fact_check', '79', 'phone_call', /3 questions/, /2 call attempts/, /purchases/]
];
async function isolate(page) {
  const writes = [], errors = [];
  page.on('pageerror', error => errors.push(error.message));
  await page.addInitScript(() => { window.GOHIREHUMANS_API_URL = '/__offer-api'; });
  await page.route('**/*', route => {
    const request = route.request(), url = new URL(request.url());
    if (!['GET', 'HEAD'].includes(request.method())) { writes.push(request.url()); return route.abort(); }
    if (!['localhost', '127.0.0.1'].includes(url.hostname)) return route.abort();
    if (url.pathname === '/config.js') return route.fulfill({ contentType: 'application/javascript', body: '' });
    if (url.pathname.startsWith('/__offer-api/')) return route.fulfill({ contentType: 'application/json', body: JSON.stringify(url.pathname.endsWith('/categories') ? { categories: ['testing', 'expert_review', 'research', 'phone_call', 'other'] } : {}) });
    return route.continue();
  });
  return { writes, errors };
}
async function finalDraft(page, key) {
  await expect(page.locator('#authForm')).toBeVisible();
  const redirect = await page.evaluate(() => new URLSearchParams(location.hash.split('?')[1]).get('redirect'));
  expect(redirect).toBe('post-job?template=' + key);
  await page.evaluate(() => saveSession('local-offer-fixture', { id: 501, name: 'Local offer reviewer', role: 'employer' }));
  await page.goto('/#/' + redirect);
  const expected = templates.resolve(key);
  for (const name of ['title', 'description', 'category', 'budget_amount', 'budget_type']) {
    await expect(page.locator(`form [name="${name}"]`)).toHaveValue(expected[name]);
    await expect(page.locator(`form [name="${name}"]`)).toBeEditable();
  }
  await expect(page.locator('#postJobSubmitBtn')).toBeVisible();
}
for (const [name, key, price, category, ...bounds] of offers) {
  test(`${name}: bounded advertised request survives to editable draft`, async ({ page }) => {
    const evidence = await isolate(page);
    await page.goto('/starter-offers.html');
    const card = page.locator('article').filter({ has: page.getByRole('heading', { name, exact: true }) });
    await expect(card.locator('.price')).toHaveText('$' + price);
    for (const bound of bounds) await expect(card).toContainText(bound);
    const expected = templates.resolve(key);
    expect(expected.budget_amount).toBe(price);
    expect(expected.category).toBe(category);
    for (const bound of bounds) expect(expected.description).toMatch(bound);
    for (const section of ['Inputs:', 'Exclusions:', 'Deliverable:', 'Timing:', 'Acceptance criteria:']) expect(expected.description).toContain(section);
    expect(expected.description).toMatch(/after the provider accepts.*complete.*inputs/i);
    expect(expected.description).toMatch(/not guaranteed/i);
    await card.locator('a[href*="template="]').click();
    await finalDraft(page, key);
    expect(evidence).toEqual({ writes: [], errors: [] });
  });
}

test('public offer framing is a request, not managed fulfillment or invented proof', async ({ page }) => {
  await isolate(page);
  for (const file of ['starter-offers.html', 'pricing.html', 'proof-packs.html']) {
    const html = fs.readFileSync(path.join(root, file), 'utf8');
    expect(html).not.toMatch(/Founder-Managed|Founder-reviewed|Founder review|Founder-managed|GoHireHumans should review|After acceptance, ask|proof-backed/i);
    await page.goto('/' + file);
    await expect(page.locator('body')).toContainText(/suggested.*budgets/i);
    await expect(page.locator('body')).toContainText(/availability.*not guaranteed/i);
  }
});
for (const [i, price, category] of [[0, '25', 'testing'], [1, '35', 'expert_review'], [2, '50', 'research'], [3, '25', 'phone_call']]) {
  test(`smaller illustrative sample ${i} keeps its own scope and budget through auth`, async ({ page }) => {
    const evidence = await isolate(page);
    await page.goto('/examples/sample-deliverables.html');
    await expect(page.locator('body')).toContainText(/smaller.*not.*starter/i);
    const sample = page.locator('.sample').nth(i);
    if (i === 3) await expect(sample).not.toContainText('contact name');
    const link = sample.locator('a.cta');
    const href = await link.getAttribute('href');
    const query = new URLSearchParams(href.split('?')[1]);
    expect(query.get('draft_budget')).toBe(price);
    expect(query.get('draft_category')).toBe(category);
    const description = query.get('draft_description');
    for (const token of ['Inputs:', 'Exclusions:', 'Deliverable:', 'Timing:', 'not guaranteed']) expect(description).toContain(token);
    await link.click();
    await expect(page.locator('#authForm')).toBeVisible();
    const redirect = await page.evaluate(() => new URLSearchParams(location.hash.split('?')[1]).get('redirect'));
    await page.evaluate(() => saveSession('local-offer-fixture', { id: 501, name: 'Local reviewer', role: 'employer' }));
    await page.goto('/#/' + redirect);
    await expect(page.locator('input[name="title"]')).toHaveValue(query.get('draft_title'));
    await expect(page.locator('textarea[name="description"]')).toHaveValue(description);
    await expect(page.locator('select[name="category"]')).toHaveValue(category);
    await expect(page.locator('input[name="budget_amount"]')).toHaveValue(price);
    for (const name of ['title', 'description', 'category', 'budget_amount']) await expect(page.locator(`form [name="${name}"]`)).toBeEditable();
    expect(evidence).toEqual({ writes: [], errors: [] });
  });
}

for (const [name, key, price] of offers) {
  test(`pricing ${name} opens the same bounded draft`, async ({ page }) => {
    const evidence = await isolate(page);
    await page.goto('/pricing.html');
    const card = page.locator('.feature-item').filter({ has: page.getByRole('heading', { name, exact: true }) });
    await expect(card).toContainText('$' + price);
    await card.locator('a[href*="template="]').click();
    await finalDraft(page, key);
    expect(evidence).toEqual({ writes: [], errors: [] });
  });
}

for (const route of ['starter-offers.html', 'pricing.html', 'proof-packs.html', 'examples/sample-deliverables.html']) {
  test(`${route} remains readable without document overflow`, async ({ page }, testInfo) => {
    const evidence = await isolate(page);
    await page.goto('/' + route);
    await expect(page.locator('h1')).toBeVisible();
    await expect.poll(() => page.evaluate(() => document.documentElement.scrollWidth - window.innerWidth)).toBeLessThanOrEqual(1);
    const image = testInfo.outputPath(route.replaceAll('/', '-') + '.png');
    await page.screenshot({ path: image, fullPage: true });
    await testInfo.attach('page', { path: image, contentType: 'image/png' });
    expect(evidence).toEqual({ writes: [], errors: [] });
  });
}
