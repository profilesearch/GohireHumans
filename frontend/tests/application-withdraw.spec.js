const { test, expect } = require('@playwright/test');

// Local-only fixtures: every API call is fulfilled in-page; nothing reaches production.
const API = 'https://gohirehumans-production.up.railway.app/**';

const job = {
  id: 33, employer_id: 2, title: 'Write 5 product descriptions', description: 'Five descriptions',
  category: 'copywriting', budget_type: 'fixed', budget_amount: 60, status: 'reviewing',
  location_type: 'remote', created_at: '2026-09-29T12:00:00Z', hiring_enabled: true,
};

async function workerSession(page) {
  await page.addInitScript(() => {
    sessionStorage.setItem('ghh_token', 'worker-token');
    localStorage.setItem('ghh_user', JSON.stringify({ id: 9, name: 'Worker', is_admin: false }));
  });
}

function jobRoute(page, state) {
  return page.route(API, async route => {
    const req = route.request();
    const url = new URL(req.url());
    if (url.pathname === '/jobs/33' && req.method() === 'GET') {
      return route.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify({ ...job, ...state.job }) });
    }
    if (url.pathname === '/jobs/33/apply' && req.method() === 'DELETE') {
      state.deletes += 1;
      state.job = { ...state.job, viewer_application: null };
      return route.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify({ ok: true, withdrawn_application_id: 501, can_reapply: state.canReapply }) });
    }
    return route.fulfill({ status: 200, contentType: 'application/json', body: '{}' });
  });
}

test('worker who applied sees their application and can withdraw it', async ({ page }) => {
  await workerSession(page);
  const state = { deletes: 0, canReapply: true, job: { viewer_application: { id: 501, status: 'pending', created_at: '2026-09-30T10:00:00Z' } } };
  await jobRoute(page, state);
  await page.goto('/#/jobs/33', { waitUntil: 'domcontentloaded' });
  await expect(page.locator('.svc-order-card')).toContainText('You applied');
  await expect(page.getByRole('button', { name: 'Apply to This Job' })).toHaveCount(0);
  await page.getByRole('button', { name: 'Withdraw application' }).click();
  const dialog = page.locator('.modal-dialog');
  await expect(dialog.locator('.modal-title')).toHaveText('Withdraw your application?');
  await dialog.getByRole('button', { name: 'Keep it' }).click();
  expect(state.deletes).toBe(0);
  await page.getByRole('button', { name: 'Withdraw application' }).click();
  await page.locator('.modal-dialog').getByRole('button', { name: 'Withdraw', exact: true }).click();
  await expect(page.getByRole('button', { name: 'Apply to This Job' })).toHaveCount(1);
  expect(state.deletes).toBe(1);
});

test('final withdrawal tells the worker they cannot reapply', async ({ page }) => {
  await workerSession(page);
  const state = { deletes: 0, canReapply: false, job: { viewer_application: { id: 501, status: 'pending', created_at: '2026-09-30T10:00:00Z' } } };
  await jobRoute(page, state);
  await page.goto('/#/jobs/33', { waitUntil: 'domcontentloaded' });
  await page.getByRole('button', { name: 'Withdraw application' }).click();
  await page.locator('.modal-dialog').getByRole('button', { name: 'Withdraw', exact: true }).click();
  await expect(page.getByText("You can't apply to this job again.")).toBeVisible();
});

test('a decided application shows no withdraw button', async ({ page }) => {
  await workerSession(page);
  const state = { deletes: 0, canReapply: true, job: { viewer_application: { id: 501, status: 'rejected', created_at: '2026-09-30T10:00:00Z' } } };
  await jobRoute(page, state);
  await page.goto('/#/jobs/33', { waitUntil: 'domcontentloaded' });
  await expect(page.locator('.svc-order-card')).toContainText('You applied');
  await expect(page.getByRole('button', { name: 'Withdraw application' })).toHaveCount(0);
  await expect(page.getByRole('button', { name: 'Apply to This Job' })).toHaveCount(0);
});

test('worker without an application still gets the Apply button', async ({ page }) => {
  await workerSession(page);
  const state = { deletes: 0, canReapply: true, job: { viewer_application: null } };
  await jobRoute(page, state);
  await page.goto('/#/jobs/33', { waitUntil: 'domcontentloaded' });
  await expect(page.getByRole('button', { name: 'Apply to This Job' })).toHaveCount(1);
  await expect(page.getByRole('button', { name: 'Withdraw application' })).toHaveCount(0);
});

test('after the final withdrawal the page shows why there is no Apply button', async ({ page }) => {
  await workerSession(page);
  const state = { deletes: 0, canReapply: false, job: { viewer_application: null, viewer_can_apply: false, viewer_can_withdraw: false } };
  await jobRoute(page, state);
  await page.goto('/#/jobs/33?apply=1', { waitUntil: 'domcontentloaded' });
  await expect(page.locator('.svc-order-card')).toContainText("You've withdrawn from this job twice");
  await expect(page.getByRole('button', { name: 'Apply to This Job' })).toHaveCount(0);
  await expect(page.locator('#applyForm')).toHaveCount(0);
});

test('worker keeps seeing and can withdraw their application after the job closes', async ({ page }) => {
  await workerSession(page);
  const state = { deletes: 0, canReapply: true, job: { status: 'canceled', viewer_can_withdraw: true, viewer_can_apply: false,
    viewer_application: { id: 501, status: 'pending', created_at: '2026-09-30T10:00:00Z' } } };
  await jobRoute(page, state);
  await page.goto('/#/jobs/33', { waitUntil: 'domcontentloaded' });
  await expect(page.locator('.svc-order-card')).toContainText('no longer accepting new applications');
  await page.getByRole('button', { name: 'Withdraw application' }).click();
  await page.locator('.modal-dialog').getByRole('button', { name: 'Withdraw', exact: true }).click();
  await expect.poll(() => state.deletes).toBe(1);
});

test('closed hourly job still shows the worker their application and Withdraw', async ({ page }) => {
  await workerSession(page);
  const state = { deletes: 0, canReapply: true, job: { status: 'canceled', budget_type: 'hourly', viewer_can_withdraw: true, viewer_can_apply: false,
    viewer_application: { id: 501, status: 'pending', created_at: '2026-09-30T10:00:00Z' } } };
  await jobRoute(page, state);
  await page.goto('/#/jobs/33', { waitUntil: 'domcontentloaded' });
  const card = page.locator('.svc-order-card');
  await expect(card).toContainText('You applied');
  await expect(card).toContainText('Hourly hiring is not available yet');
  await page.getByRole('button', { name: 'Withdraw application' }).click();
  await page.locator('.modal-dialog').getByRole('button', { name: 'Withdraw', exact: true }).click();
  await expect.poll(() => state.deletes).toBe(1);
});

test('server says withdrawal is not allowed: no Withdraw button', async ({ page }) => {
  await workerSession(page);
  const state = { deletes: 0, canReapply: true, job: { viewer_can_withdraw: false,
    viewer_application: { id: 501, status: 'pending', created_at: '2026-09-30T10:00:00Z' } } };
  await jobRoute(page, state);
  await page.goto('/#/jobs/33', { waitUntil: 'domcontentloaded' });
  await expect(page.locator('.svc-order-card')).toContainText('You applied');
  await expect(page.getByRole('button', { name: 'Withdraw application' })).toHaveCount(0);
});
