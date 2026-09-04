const { test, expect } = require('@playwright/test');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');

const frontend = path.resolve(__dirname, '..');
const categoryAliases = {
  unsure: 'other', testing: 'testing', expert_review: 'expert_review', research: 'research',
  phone_calls: 'phone_call', local_check: 'inspection', data_cleanup: 'data_entry', other: 'other'
};
const ideas = [
  ['Test a website signup flow', 'testing', '25'],
  ['Review AI output for trust issues', 'expert_review', '35'],
  ['Build a sourced lead list', 'research', '50'],
  ['Make a phone call and report back', 'phone_calls', '25'],
  ['Check a local place or take photos', 'local_check', '40'],
  ['Clean up a spreadsheet', 'data_cleanup', '30'],
  ['Compare vendor options', 'research', '45'],
  ['Human review of chatbot responses', 'testing', '35'],
  ['Verify source links in AI research', 'research', '30']
];

// Parse source, not browser-normalized text: literal newlines in quoted JS must fail CI.
test('all source HTML executable inline scripts parse', () => {
  const excluded = new Set(['node_modules', 'test-results', 'playwright-report', 'coverage', 'dist', 'build', '.git']);
  const failures = [];
  let pages = 0, scripts = 0;
  function scan(dir) {
    for (const entry of fs.readdirSync(dir, { withFileTypes: true })) {
      const file = path.join(dir, entry.name);
      if (entry.isDirectory()) { if (!excluded.has(entry.name)) scan(file); continue; }
      if (!/\.html?$/i.test(entry.name)) continue;
      pages++;
      const source = fs.readFileSync(file, 'utf8');
      for (const match of source.matchAll(/<script\b([^>]*)>([\s\S]*?)<\/script\s*>/gi)) {
        const attrs = match[1];
        if (/\bsrc\s*=/i.test(attrs)) continue;
        const type = attrs.match(/\btype\s*=\s*(?:"([^"]*)"|'([^']*)'|([^\s>]+))/i);
        const mime = (type ? type[1] ?? type[2] ?? type[3] : '').trim().toLowerCase();
        if (mime && !/^(?:(?:text|application)\/(?:javascript|ecmascript)|module)$/.test(mime)) continue;
        scripts++;
        const filename = `${path.relative(frontend, file)}:${source.slice(0, match.index).split('\n').length}`;
        try { new vm.Script(match[2], { filename }); }
        catch (error) { failures.push(`${filename}: ${error.message}`); }
      }
    }
  }
  scan(frontend);
  console.log(`Inline source syntax guard: ${pages} HTML files, ${scripts} executable inline scripts`);
  expect(pages).toBeGreaterThan(100);
  expect(scripts).toBeGreaterThan(100);
  expect(failures).toEqual([]);
});

async function isolate(page, signedIn = false, failDraftStorage = false) {
  const mutations = [], errors = [], blockedAnalytics = [];
  page.on('pageerror', error => errors.push(error.message));
  await page.addInitScript(({ signedIn, failDraftStorage }) => {
    window.GOHIREHUMANS_API_URL = '/__draft-test-api';
    if (failDraftStorage) {
      const setItem = Storage.prototype.setItem;
      Storage.prototype.setItem = function(key, value) {
        if (this === sessionStorage && key === 'ghh_guided_task_draft') {
          throw new DOMException('Draft storage quota exceeded', 'QuotaExceededError');
        }
        return setItem.call(this, key, value);
      };
    }
    if (signedIn) {
      sessionStorage.setItem('ghh_token', 'local-draft-test-only');
      localStorage.setItem('ghh_user', JSON.stringify({ id: 501, name: 'Draft Tester', role: 'employer' }));
    }
  }, { signedIn, failDraftStorage });
  await page.route('**/*', route => {
    const request = route.request(), url = new URL(request.url());
    if (!['GET', 'HEAD'].includes(request.method())) {
      mutations.push(`${request.method()} ${request.url()}`);
      return route.abort();
    }
    if (url.pathname === '/analytics-bootstrap.js' && /\/(?:request-any-task|ideas)\.html/.test(request.frame().url())) {
      blockedAnalytics.push(request.url());
      return route.abort();
    }
    if (!['127.0.0.1', 'localhost'].includes(url.hostname)) return route.abort();
    if (url.pathname === '/config.js') return route.fulfill({ status: 200, contentType: 'application/javascript', body: '' });
    if (url.pathname.startsWith('/__draft-test-api/')) {
      const body = url.pathname.endsWith('/categories')
        ? { categories: [...new Set(Object.values(categoryAliases))] }
        : {};
      return route.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify(body) });
    }
    return route.continue();
  });
  return { mutations, errors, blockedAnalytics };
}

const task = 'Call three shops about the "blue" item & report <availability>';
const outcome = 'Return a table with prices & source notes.\nInclude unavailable items.';
const budget = '$75.50–$100';
const urgency = 'This week';
function typedDescription(type) {
  return `What needs to be done:\n${task}\n\nDesired outcome:\n${outcome}\n\nTask type / category guess:\n${type}\n\nBudget range:\n${budget}\n\nTiming / urgency:\n${urgency}\n\nPlease review this draft before publishing.`;
}
async function fillRequest(page, type) {
  await page.goto('/request-any-task.html');
  await page.getByLabel('What do you need a human to do?', { exact: true }).fill(task);
  await page.getByLabel('Desired outcome / deliverable', { exact: true }).fill(outcome);
  await page.getByLabel('Task type guess', { exact: true }).selectOption(type);
  await page.getByLabel('Budget range', { exact: true }).fill(budget);
  await page.getByLabel('Timing / urgency', { exact: true }).fill(urgency);
  await page.getByRole('button', { name: 'Create draft job', exact: true }).click();
}
async function assertDraft(page, expected, failDraftStorage = false) {
  await expect(page.locator('input[name="title"]')).toHaveValue(expected.title);
  await expect(page.locator('textarea[name="description"]')).toHaveValue(expected.description);
  await expect(page.locator('select[name="category"]')).toHaveValue(expected.category);
  await expect(page.locator('input[name="budget_amount"]')).toHaveValue(expected.budget_amount);
  await expect(page.locator('select[name="budget_type"]')).toHaveValue('fixed');
  await expect(page.getByRole('button', { name: 'Post Job', exact: true })).toBeVisible();
  const stored = await page.evaluate(() => JSON.parse(sessionStorage.getItem('ghh_guided_task_draft')));
  if (failDraftStorage) expect(stored).toBeNull();
  else expect(stored).toMatchObject(expected);
}
function assertSafe(evidence) {
  expect(evidence.errors).toEqual([]);
  expect(evidence.mutations).toEqual([]);
  expect(evidence.blockedAnalytics.length).toBeGreaterThan(0);
}

test('typed request survives the auth destination without publishing', async ({ page }) => {
  const evidence = await isolate(page);
  await fillRequest(page, 'phone_calls');
  await expect(page.locator('.auth2-title')).toHaveText('Your job draft is saved');
  await expect(page.locator('.auth2-sub')).toContainText('Nothing has been posted or charged.');
  const redirect = await page.evaluate(() => new URLSearchParams(location.hash.split('?')[1]).get('redirect'));
  const query = new URLSearchParams(redirect.split('?')[1]);
  expect(redirect).toMatch(/^post-job\?/);
  expect(query.get('draft_title')).toBe(task);
  expect(query.get('draft_description')).toBe(typedDescription('phone_calls'));
  expect(query.get('draft_skills')).toBe('phone_calls');
  expect(query.get('draft_budget')).toBe('75.50');
  const expected = { title: task, description: typedDescription('phone_calls'), category: 'phone_call', budget_amount: '75.50' };
  expect(await page.evaluate(() => JSON.parse(sessionStorage.getItem('ghh_guided_task_draft')))).toMatchObject(expected);
  // Exercise the review destination with local fixture state, never an auth submission.
  await page.evaluate(() => saveSession('local-draft-test-only', { id: 501, name: 'Draft Tester', role: 'employer' }));
  await page.goto('/#/' + redirect);
  await assertDraft(page, expected);
  assertSafe(evidence);
});

for (const source of ['request', 'idea']) {
  for (const type of ['phone_calls', 'local_check']) {
    for (const signedIn of [true, false]) {
      for (const failDraftStorage of [false, true]) {
        test(`category handoff: ${source} ${type}, ${signedIn ? 'signed in' : 'anonymous'}, ${failDraftStorage ? 'QuotaExceededError' : 'normal storage'}`, async ({ page }) => {
          const evidence = await isolate(page, signedIn, failDraftStorage);
          let expected;
          if (source === 'request') {
            await fillRequest(page, type);
            expected = { title: task, description: typedDescription(type), category: categoryAliases[type], budget_amount: '75.50' };
          } else {
            const [title, , amount] = ideas.find(idea => idea[1] === type);
            await page.goto('/ideas.html');
            const card = page.locator('article').filter({ has: page.getByRole('heading', { name: title, exact: true }) });
            await card.getByRole('button', { name: 'Request this task', exact: true }).click();
            expected = {
              title, description: `I want help with this task idea: ${title}\n\nPlease describe the inputs, deliverable, acceptance criteria, and timing before publishing.`,
              category: categoryAliases[type], budget_amount: amount
            };
          }
          if (!signedIn) {
            await expect(page.locator('.auth2-title')).toHaveText('Your job draft is saved');
            const redirect = await page.evaluate(() => new URLSearchParams(location.hash.split('?')[1]).get('redirect'));
            expect(redirect).toMatch(/^post-job\?/);
            // Local auth fixture only: no login, publishing, or payment requests.
            await page.evaluate(() => saveSession('local-draft-test-only', { id: 501, name: 'Draft Tester', role: 'employer' }));
            await page.goto('/#/' + redirect);
          }
          await assertDraft(page, expected, failDraftStorage);
          await expect(page.locator('select[name="category"]')).toBeEditable();
          assertSafe(evidence);
        });
      }
    }
  }
}

for (const [type, category] of Object.entries(categoryAliases)) {
  test(`typed ${type} request opens the correct editable draft with analytics blocked`, async ({ page }) => {
    const evidence = await isolate(page, true);
    await fillRequest(page, type);
    await assertDraft(page, { title: task, description: typedDescription(type), category, budget_amount: '75.50' });
    assertSafe(evidence);
  });
}

for (const [title, type, amount] of ideas) {
  test(`idea: ${title} opens its own editable draft with analytics blocked`, async ({ page }) => {
    const evidence = await isolate(page, true);
    await page.goto('/ideas.html');
    const card = page.locator('article').filter({ has: page.getByRole('heading', { name: title, exact: true }) });
    await card.getByRole('button', { name: 'Request this task', exact: true }).click();
    await assertDraft(page, {
      title, description: `I want help with this task idea: ${title}\n\nPlease describe the inputs, deliverable, acceptance criteria, and timing before publishing.`,
      category: categoryAliases[type], budget_amount: amount
    });
    assertSafe(evidence);
  });
}
