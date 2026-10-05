const { test, expect } = require('@playwright/test');

// Job due dates: shown as the calendar date the buyer picked (no UTC shift), and
// a due date that has passed is not advertised on a job still taking applications.
test.use({ locale: 'en-US', timezoneId: 'America/Denver' });

const baseJob = { id: 33, employer_id: 2, title: 'Write 5 product descriptions', description: 'Five descriptions',
  category: 'copywriting', budget_type: 'fixed', budget_amount: 60, status: 'reviewing', location_type: 'remote',
  created_at: '2026-09-29T12:00:00Z', hiring_enabled: true, viewer_application: null };

async function open(page, { job = {}, owner = false, now } = {}) {
  if (now) await page.clock.setFixedTime(new Date(now));
  if (owner) {
    await page.addInitScript(() => {
      sessionStorage.setItem('ghh_token', 'owner-token');
      localStorage.setItem('ghh_user', JSON.stringify({ id: 2, name: 'Buyer', is_admin: false }));
    });
  }
  await page.route('**/*', route => {
    const url = new URL(route.request().url());
    if (['127.0.0.1', 'localhost'].includes(url.hostname)) return route.continue();
    if (url.hostname !== 'gohirehumans-production.up.railway.app') return route.abort();
    const body = url.pathname === '/jobs/33' ? { ...baseJob, ...job } : {};
    return route.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify(body) });
  });
  await page.goto('/#/jobs/33', { waitUntil: 'domcontentloaded' });
  const meta = page.locator('.svc-order-meta');
  await expect(meta).toBeVisible();
  return meta;
}

test('date-only due date shows the calendar day the buyer picked', async ({ page }) => {
  const meta = await open(page, { job: { due_by: '2099-12-31' } });
  await expect(meta).toContainText('Due: 12/31/2099');
});

test('a due date is still current through the end of that local day', async ({ page }) => {
  const meta = await open(page, { job: { due_by: '2026-10-02' }, now: '2026-10-02T23:30:00-06:00' });
  await expect(meta).toContainText('Due: 10/2/2026');
  await expect(meta).not.toContainText('passed');
});

test('workers see no stale due date on a job still taking applications', async ({ page }) => {
  const meta = await open(page, { job: { due_by: '2026-10-02' }, now: '2026-10-03T00:30:00-06:00' });
  await expect(meta).toContainText('Due date passed. Say when you can deliver in your application.');
  await expect(meta).not.toContainText('Due:');
  await expect(meta).not.toContainText('10/2/2026');
});

test('the buyer sees which due date passed', async ({ page }) => {
  const meta = await open(page, { job: { due_by: '2026-10-02' }, owner: true, now: '2026-10-05T12:00:00Z' });
  await expect(meta).toContainText('Due date passed (10/2/2026)');
  await expect(meta).not.toContainText('Say when you can deliver');
});

test('a timed due date passes at that moment', async ({ page }) => {
  const meta = await open(page, { job: { due_by: '2026-10-02T15:00:00Z' }, now: '2026-10-02T15:00:01Z' });
  await expect(meta).toContainText('Due date passed.');
});

test('jobs no longer taking applications keep their original due date', async ({ page }) => {
  const meta = await open(page, { job: { status: 'hired', due_by: '2026-10-02' }, now: '2026-10-05T12:00:00Z' });
  await expect(meta).toContainText('Due: 10/2/2026');
  await expect(meta).not.toContainText('passed');
});

test('jobs without a due date show no due line', async ({ page }) => {
  const meta = await open(page, { job: { due_by: null }, now: '2026-10-05T12:00:00Z' });
  await expect(meta).not.toContainText('Due');
});

test('an unparseable due date shows no due line', async ({ page }) => {
  const meta = await open(page, { job: { due_by: 'soon<img src=x>' }, now: '2026-10-05T12:00:00Z' });
  await expect(meta).not.toContainText('Due');
  await expect(page.locator('.svc-order-meta img')).toHaveCount(0);
});
