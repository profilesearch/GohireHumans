const { test, expect } = require('@playwright/test');

let errors, mutations;
test.beforeEach(async ({ page }) => {
  errors = []; mutations = [];
  page.on('pageerror', error => errors.push(error.message));
  await page.addInitScript(() => {
    window.GOHIREHUMANS_API_URL = '/__intake-test-api';
    window.intakeEvents = [];
    window.gtag = (...args) => intakeEvents.push(args);
  });
  await page.route('**/*', route => {
    const request = route.request(), url = new URL(request.url());
    if (!['GET', 'HEAD'].includes(request.method())) {
      mutations.push(`${request.method()} ${url.pathname}`);
      return route.abort();
    }
    if (!['127.0.0.1', 'localhost'].includes(url.hostname) || /analytics|stripe|paypal/i.test(url.pathname)) return route.abort();
    if (url.pathname === '/config.js') return route.fulfill({ contentType: 'application/javascript', body: '' });
    if (url.pathname.startsWith('/__intake-test-api')) {
      return route.fulfill({ contentType: 'application/json', body: JSON.stringify(url.pathname.endsWith('/categories') ? { categories: ['other'] } : { services: [] }) });
    }
    return route.continue();
  });
  await page.goto('/');
  // Use a local recorder, not GA: the host allowlist disables tracking on localhost.
  await page.evaluate(() => { trackEvent = (name, params) => intakeEvents.push([name, params]); });
});
test.afterEach(() => {
  expect(errors).toEqual([]);
  expect(mutations).toEqual([]);
});

async function resumeStoredDraft(page) {
  await expect(page.locator('.auth2-title')).toHaveText('Your job draft is saved');
  const redirect = await page.evaluate(() => new URLSearchParams(location.hash.split('?')[1]).get('redirect'));
  expect(redirect).toMatch(/^post-job\?/);
  // Synthetic session state only; no account creation or auth submission.
  await page.evaluate(() => saveSession('intake-test-only', { id: 501, name: 'Intake Tester', role: 'employer' }));
  await page.goto('/#/' + redirect);
  await expect(page.locator('#job-title')).toBeEditable();
}

function draftButton(page) { return page.locator('#guided-task-intake').getByRole('button', { name: 'Build my draft', exact: true }); }

test('one sentence survives the auth handoff into an editable post-job draft', async ({ page }) => {
  const task = 'Check my signup flow and report confusing steps';
  await page.getByLabel('What do you need done?', { exact: true }).fill(task);
  await expect(page.locator('.lp-guided-optional')).not.toHaveAttribute('open', '');
  await draftButton(page).click();
  const stored = await page.evaluate(() => JSON.parse(sessionStorage.getItem('ghh_guided_task_draft')));
  expect(stored.title).toBe(task);
  expect(stored.description).toContain(task);
  await resumeStoredDraft(page);
  await expect(page.locator('#job-title')).toHaveValue(task);
  await expect(page.locator('#job-description')).toHaveValue(stored.description);
});

test('empty or whitespace-only draft is blocked accessibly and typing clears the error', async ({ page }) => {
  await page.locator('#guided-task-need').fill('   ');
  await draftButton(page).click();
  await expect(page).toHaveURL(/\/$/);
  await expect(page.locator('#guided-task-error')).toBeVisible();
  await expect(page.locator('#guided-task-error')).toHaveAttribute('role', 'alert');
  await expect(page.locator('#guided-task-need')).toHaveAttribute('aria-describedby', 'guided-task-error');
  await expect(page.locator('#guided-task-need')).toHaveAttribute('aria-invalid', 'true');
  await expect(page.locator('#guided-task-need')).toBeFocused();
  expect(await page.evaluate(() => sessionStorage.getItem('ghh_guided_task_draft'))).toBeNull();
  expect(await page.evaluate(() => intakeEvents.filter(([name]) => name === 'first_task_draft_empty_blocked'))).toEqual([
    ['first_task_draft_empty_blocked', { source: 'homepage_guided_task_intake' }]
  ]);
  expect(await page.evaluate(() => intakeEvents.filter(([name]) => ['first_task_blank_form_opened', 'first_task_draft_completed', 'guided_task_draft_created'].includes(name)))).toEqual([]);
  await page.locator('#guided-task-need').fill('Check this sentence');
  await expect(page.locator('#guided-task-error')).toBeHidden();
  await expect(page.locator('#guided-task-need')).toHaveAttribute('aria-invalid', 'false');
});

test('optional details expand and preserve helper, deliverable and exact budget', async ({ page }) => {
  await page.locator('#guided-task-need').fill('Test my landing page');
  await page.getByText('Add details (optional)', { exact: true }).click();
  await expect(page.locator('.lp-guided-optional')).toHaveAttribute('open', '');
  await page.locator('#guided-task-helper').fill('Website tester');
  await page.locator('#guided-task-deliverable').fill('Notes plus screenshots');
  await page.locator('#guided-task-budget').fill('$25.50');
  await draftButton(page).click();
  await resumeStoredDraft(page);
  await expect(page.locator('#job-description')).toContainText('Notes plus screenshots');
  await expect(page.locator('#job-required-skills')).toHaveValue('Website tester');
  await expect(page.locator('#jobBudgetAmount')).toHaveValue('25.50');
});

test('a deliverable alone still counts as a meaningful draft', async ({ page }) => {
  await page.getByText('Add details (optional)', { exact: true }).click();
  await page.locator('#guided-task-deliverable').fill('A cleaned spreadsheet with duplicate rows flagged');
  await draftButton(page).click();
  await resumeStoredDraft(page);
  await expect(page.locator('#job-description')).toContainText('A cleaned spreadsheet with duplicate rows flagged');
});

test('need-entered fires once per page session with no draft text', async ({ page }) => {
  const field = page.locator('#guided-task-need');
  await field.focus();
  await field.fill('   ');
  expect(await page.evaluate(() => intakeEvents.filter(([name]) => name === 'first_task_need_entered'))).toEqual([]);
  await field.fill('Private synthetic task text');
  await field.fill('');
  await field.fill('A second private sentence');
  expect(await page.evaluate(() => intakeEvents.filter(([name]) => name === 'first_task_need_entered'))).toEqual([
    ['first_task_need_entered', { source: 'homepage_guided_task_intake', has_task: true }]
  ]);
  await page.reload();
  await page.evaluate(() => { trackEvent = (name, params) => intakeEvents.push([name, params]); });
  await page.locator('#guided-task-need').fill('After a reload');
  expect(await page.evaluate(() => intakeEvents.filter(([name]) => name === 'first_task_need_entered'))).toEqual([]);
});

test('blocked sessionStorage still dedupes need-entered in memory', async ({ page }) => {
  await page.evaluate(() => {
    Storage.prototype.getItem = function() { throw new DOMException('Unavailable', 'SecurityError'); };
    Storage.prototype.setItem = function() { throw new DOMException('Unavailable', 'SecurityError'); };
  });
  await page.locator('#guided-task-need').fill('First sentence');
  await page.locator('#guided-task-need').fill('Second sentence');
  expect(await page.evaluate(() => intakeEvents.filter(([name]) => name === 'first_task_need_entered'))).toEqual([
    ['first_task_need_entered', { source: 'homepage_guided_task_intake', has_task: true }]
  ]);
});

test('mobile 390x844 shows the full-width draft button next to the textarea without expanding details', async ({ page }) => {
  await page.setViewportSize({ width: 390, height: 844 });
  await page.locator('#guided-task-need').scrollIntoViewIfNeeded();
  await expect(page.locator('.lp-guided-optional')).not.toHaveAttribute('open', '');
  await expect(page.locator('#guided-task-helper')).toBeHidden();
  await expect(draftButton(page)).toBeInViewport({ ratio: 1 });
  const field = await page.locator('#guided-task-need').boundingBox();
  const button = await draftButton(page).boundingBox();
  expect(button.y).toBeGreaterThanOrEqual(field.y + field.height);
  expect(button.y - field.y - field.height).toBeLessThan(40);
  expect(Math.abs(button.width - field.width)).toBeLessThan(2);
});

test('explicit start-blank route is still available', async ({ page }) => {
  await page.evaluate(() => startTaskDraft('', 'intake_regression_blank'));
  await expect(page).toHaveURL(/#\/login\?redirect=post-job$/);
  await expect(page.locator('#guided-task-error')).toHaveCount(0);
});
