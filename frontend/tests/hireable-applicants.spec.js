const { test, expect } = require('@playwright/test');

// Local-only fixtures: every API call is fulfilled in-page; nothing reaches production.
const API = 'https://gohirehumans-production.up.railway.app/**';

async function employerSession(page) {
  await page.addInitScript(() => {
    sessionStorage.setItem('ghh_token', 'employer-token');
    localStorage.setItem('ghh_user', JSON.stringify({ id: 2, name: 'Employer', is_admin: false }));
  });
}

function applicantsRoute(page, job, apps) {
  return page.route(API, route => {
    const url = new URL(route.request().url());
    if (url.pathname === `/jobs/${job.id}`) return route.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify(job) });
    if (url.pathname === `/jobs/${job.id}/applications`) return route.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify(apps) });
    return route.fulfill({ status: 200, contentType: 'application/json', body: '{}' });
  });
}

const liveJob = { id: 12, employer_id: 2, title: 'Fixed QA pass', budget_type: 'fixed', budget_amount: 25, status: 'reviewing', hiring_enabled: true };

test('payout-ready applicants are listed first under Ready to hire', async ({ page }) => {
  await employerSession(page);
  await applicantsRoute(page, liveJob, [
    { id: 51, worker_name: 'Newest Waiting', cover_message: 'a', status: 'pending', worker_payout_ready: false },
    { id: 52, worker_name: 'Declined One', cover_message: 'b', status: 'rejected', worker_payout_ready: true },
    { id: 53, worker_name: 'Older Ready', cover_message: 'c', status: 'shortlisted', worker_payout_ready: true },
    { id: 54, worker_name: 'Truthy String', cover_message: 'd', status: 'pending', worker_payout_ready: 'true' }
  ]);
  await page.goto('/#/jobs/12/applicants', { waitUntil: 'domcontentloaded' });
  await expect(page.locator('.page-desc')).toHaveText('4 applicants · 1 ready to hire');
  await expect(page.locator('h2.dash-section-title')).toHaveText(['Ready to hire (1)', 'Waiting on payout setup (2)', 'Other applicants (1)']);
  await expect(page.locator('.applicant-name')).toHaveText(['Older Ready', 'Newest Waiting', 'Truthy String', 'Declined One']);
  await expect(page.getByRole('button', { name: 'Hire', exact: true })).toHaveCount(1);
  await expect(page.locator('.app-notice')).toHaveCount(0);
});

test('with nobody payout-ready the buyer is told why no Hire button exists', async ({ page }) => {
  await employerSession(page);
  await applicantsRoute(page, liveJob, [
    { id: 61, worker_name: 'Waiting A', cover_message: 'a', status: 'pending', worker_payout_ready: false },
    { id: 62, worker_name: 'Waiting B', cover_message: 'b', status: 'pending' }
  ]);
  await page.goto('/#/jobs/12/applicants', { waitUntil: 'domcontentloaded' });
  await expect(page.locator('.page-desc')).toHaveText('2 applicants · 0 ready to hire');
  await expect(page.locator('.app-notice')).toContainText('No applicant can be hired yet');
  await expect(page.locator('h2.dash-section-title')).toHaveText(['Waiting on payout setup (2)']);
  await expect(page.getByRole('button', { name: 'Hire', exact: true })).toHaveCount(0);
});

test('when hiring is paused the list stays ungrouped with no readiness claims', async ({ page }) => {
  await employerSession(page);
  await applicantsRoute(page, { ...liveJob, hiring_enabled: false }, [
    { id: 71, worker_name: 'Ready', cover_message: 'a', status: 'pending', worker_payout_ready: true }
  ]);
  await page.goto('/#/jobs/12/applicants', { waitUntil: 'domcontentloaded' });
  await expect(page.locator('.page-desc')).toHaveText('1 applicant');
  await expect(page.locator('h2.dash-section-title')).toHaveCount(0);
  await expect(page.getByRole('button', { name: 'Hiring temporarily paused' })).toHaveCount(1);
});

async function applyAs(page) {
  await page.route('https://accounts.google.com/**', route => route.fulfill({ status: 204, body: '' }));
  await page.route(API, route => route.fulfill({ status: 200, contentType: 'application/json', body: '{}' }));
  await page.goto('/', { waitUntil: 'domcontentloaded' });
  await page.evaluate(async () => {
    state.user = { id: 63, name: 'Test Worker' };
    state.token = 'test-token';
    window.__testAnalyticsEvents = [];
    window.gtag = (...args) => window.__testAnalyticsEvents.push(args);
    window.__applyCalls = [];
    window.api = async (path, options = {}) => {
      window.__applyCalls.push([path, options.method || 'GET']);
      if (path === '/jobs/24/apply') return { id: 900 };
      if (path === '/jobs/24') return { id: 24, employer_id: 2, status: 'reviewing', viewer_can_apply: false, viewer_application: { id: 900, status: 'pending' } };
      return {};
    };
    await handleJobApply(24);
  });
  await page.locator('#apply-cover-message').fill('I can write these five descriptions and return them tomorrow.');
  await page.locator('#jobApplicationSubmitBtn').click();
  await expect(page.locator('#applyForm')).toHaveCount(0);
}

test('payout-ready worker sees no setup prompt after applying', async ({ page }) => {
  await applyAs(page);
  await expect(page.getByText('Application submitted!', { exact: true })).toBeVisible();
  await expect(page.locator('.modal-dialog')).toHaveCount(0);
  expect(await page.evaluate(() => window.__applyCalls.some(c => c[0] === '/payments/status'))).toBe(false);
  expect(await page.evaluate(() => window.__testAnalyticsEvents.map(a => a[1])))
    .toEqual(expect.arrayContaining(['job_application_completed', 'generate_lead', 'qualify_lead']));
});

test('guided draft makes the buyer choose a category and drops the drafting note on post', async ({ page }) => {
  const posted = [];
  await page.route('https://accounts.google.com/**', route => route.fulfill({ status: 204, body: '' }));
  await page.route(API, route => route.fulfill({ status: 200, contentType: 'application/json', body: '{}' }));
  await page.goto('/', { waitUntil: 'domcontentloaded' });
  await page.exposeFunction('__recordPost', body => posted.push(body));
  await page.evaluate(async () => {
    state.user = { id: 7, role: 'employer', name: 'Test Employer' };
    state.token = 'test-token';
    window.loadCategories = async () => [{ slug: 'web_development', name: 'Web Development' }, { slug: 'copywriting', name: 'Copywriting' }];
    window.api = async (path, options = {}) => {
      if (path === '/jobs' && options.method === 'POST') { await window.__recordPost(options.body); return { id: 77 }; }
      return {};
    };
    sessionStorage.setItem('ghh_guided_task_draft', JSON.stringify({
      title: 'Write 5 product descriptions',
      description: 'What needs to be done:\nWrite 5 descriptions\n\nPlease review this draft and edit the scope before publishing. Nothing is submitted until you post the listing.',
      budget_amount: '60', budget_type: 'fixed'
    }));
    await renderPostJob();
  });
  const category = page.locator('select[name="category"]');
  await expect(category).toHaveValue('');
  await expect(category.locator('option').first()).toHaveText('Choose a category');
  await page.locator('#postJobSubmitBtn').click();
  expect(posted).toEqual([]);
  expect(await category.evaluate(el => el.validity.valueMissing)).toBe(true);

  await category.selectOption('copywriting');
  await page.locator('#postJobSubmitBtn').click();
  await expect.poll(() => posted.length).toBe(1);
  expect(posted[0].category).toBe('copywriting');
  expect(posted[0].description).toBe('What needs to be done:\nWrite 5 descriptions');
});

test('buyer-written text that merely mentions review is never altered', async ({ page }) => {
  await page.goto('/', { waitUntil: 'domcontentloaded' });
  const out = await page.evaluate(() => [
    stripDraftReviewNote('Please review this draft before publishing. Then also check X.'),
    stripDraftReviewNote('Please review this draft before publishing.'),
    stripDraftReviewNote('Task\n\nPlease review this draft before publishing.')
  ]);
  expect(out).toEqual([
    'Please review this draft before publishing. Then also check X.',
    'Please review this draft before publishing.',
    'Task'
  ]);
});
