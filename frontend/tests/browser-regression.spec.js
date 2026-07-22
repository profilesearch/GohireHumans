const { test, expect } = require('@playwright/test');
const { AxeBuilder } = require('@axe-core/playwright');
const fs = require('fs');
const os = require('os');
const path = require('path');
const { pathToFileURL } = require('url');
const analyticsBootstrap = fs.readFileSync(path.join(__dirname, '..', 'analytics-bootstrap.js'), 'utf8');
const routes = [
  { path: '/', mustContain: 'Describe the work. Hire the right human.' },
  { path: '/starter-offers.html', mustContain: 'Start small when the work needs proof.' },
  { path: '/pricing.html', mustContain: 'Simple pricing, shown before you commit' },
  { path: '/proof-packs.html', mustContain: 'Proof packs for human verification work' },
  { path: '/ai-assistant-human-checks.html', mustContain: 'When an AI assistant should ask a human to check the work.' },
  { path: '/#/login', mustContain: 'Welcome back' },
  { path: '/#/register', mustContain: 'Join GoHireHumans' },
  { path: '/#/services', mustContain: 'Browse Services' },
  { path: '/#/jobs', mustContain: 'Browse Jobs' }
];
async function setupDeterministicLocalPage(page) {
  await page.route('https://accounts.google.com/**', route => route.fulfill({ status: 204, body: '' }));
  await page.route('https://gohirehumans-production.up.railway.app/**', route => {
    const url = route.request().url();
    const body = url.includes('/platform/stats')
      ? { services: 33, jobs: 3, users: 12 }
      : url.includes('/categories')
        ? { categories: [{ id: 'cat-qa', name: 'QA & Verification' }] }
        : url.includes('/jobs')
          ? { jobs: [{ id: 'job-1', title: 'Test one AI output', budget: 99, category_name: 'QA & Verification', status: 'open' }], total: 1 }
          : { services: [{ id: 'svc-1', title: 'AI Output Verification', price: 99, rating: 0, review_count: 0, category_name: 'QA & Verification' }], total: 1 };
    return route.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify(body) });
  });
}

async function collectConsole(page) {
  const messages = [];
  page.on('console', msg => {
    if (['error', 'warning'].includes(msg.type())) {
      const text = msg.text();
      if (/favicon|preload assets|Failed to load resource: the server responded with a status of 404/.test(text)) return;
      messages.push(`${msg.type()}: ${text}`);
    }
  });
  page.on('pageerror', err => messages.push(`pageerror: ${err.message}`));
  return messages;
}
test.describe('GoHireHumans public/browser regression suite', () => {
  test('relative dates treat SQLite-space timestamps as UTC in every browser timezone', async ({ browser }) => {
    for (const timezoneId of ['UTC', 'America/Los_Angeles', 'Asia/Tokyo']) {
      const context = await browser.newContext({
        baseURL: 'http://127.0.0.1:4173',
        timezoneId
      });
      const page = await context.newPage();
      await page.addInitScript(fixedIso => {
        const NativeDate = Date;
        const fixedMs = NativeDate.parse(fixedIso);
        window.Date = class extends NativeDate {
          constructor(...args) {
            super(...(args.length ? args : [fixedMs]));
          }
          static now() {
            return fixedMs;
          }
        };
      }, '2026-07-11T13:00:00Z');
      await setupDeterministicLocalPage(page);
      await page.goto('/', { waitUntil: 'domcontentloaded' });

      expect(await page.evaluate(() => relativeDate('2026-07-11 12:00:00'))).toBe('1h ago');
      expect(await page.evaluate(() => relativeDate('2026-07-11T12:00:00Z'))).toBe('1h ago');
      await context.close();
    }
  });

  test('localhost never requests Google Analytics or Tag Manager', async ({ page }) => {
    const analyticsRequests = [];
    page.on('request', request => {
      if (/google-analytics\.com|googletagmanager\.com/i.test(request.url())) analyticsRequests.push(request.url());
    });
    await page.goto('/');
    expect(analyticsRequests).toEqual([]);
    expect(await page.evaluate(() => Object.prototype.hasOwnProperty.call(window, 'dataLayer'))).toBe(false);
    expect(await page.evaluate(() => typeof window.gtag)).toBe('function');
    await expect(page.locator('script[src*="googletagmanager.com"], script[src*="google-analytics.com"]')).toHaveCount(0);
  });

  test('HTTPS preview hosts fail closed without requesting Google Analytics', async ({ page }) => {
    const analyticsRequests = [];
    page.on('request', request => {
      if (/google-analytics\.com|googletagmanager\.com/i.test(request.url())) analyticsRequests.push(request.url());
    });
    await page.route('https://preview.example.com/**', route => {
      if (new URL(route.request().url()).pathname === '/analytics-bootstrap.js') {
        return route.fulfill({ status: 200, contentType: 'application/javascript', body: analyticsBootstrap });
      }
      return route.fulfill({
        status: 200,
        contentType: 'text/html',
        body: '<script src="/analytics-bootstrap.js"></script><script>gtag(\'config\', \'G-KM69M3NES8\');</script>'
      });
    });
    await page.goto('https://preview.example.com/');
    expect(analyticsRequests).toEqual([]);
    expect(await page.evaluate(() => Object.prototype.hasOwnProperty.call(window, 'dataLayer'))).toBe(false);
    expect(await page.evaluate(() => typeof window.gtag)).toBe('function');
  });

  test('HTTP production and deceptive HTTPS origins fail closed', async ({ page }) => {
    const analyticsRequests = [];
    page.on('request', request => {
      if (/google-analytics\.com|googletagmanager\.com/i.test(request.url())) analyticsRequests.push(request.url());
    });
    await page.route('https://www.googletagmanager.com/**', route => route.fulfill({ status: 200, contentType: 'application/javascript', body: '' }));
    const blockedOrigins = [
      'http://www.gohirehumans.com',
      'https://www.gohirehumans.com.attacker.example',
      'https://gohirehumans.example'
    ];
    for (const origin of blockedOrigins) {
      await page.route(`${origin}/**`, route => {
        if (new URL(route.request().url()).pathname === '/analytics-bootstrap.js') {
          return route.fulfill({ status: 200, contentType: 'application/javascript', body: analyticsBootstrap });
        }
        return route.fulfill({
          status: 200,
          contentType: 'text/html',
          body: '<script src="/analytics-bootstrap.js"></script><script>gtag(\'config\', \'G-KM69M3NES8\');</script>'
        });
      });
      await page.goto(`${origin}/`);
      expect(await page.evaluate(() => Object.prototype.hasOwnProperty.call(window, 'dataLayer')), origin).toBe(false);
      expect(await page.evaluate(() => typeof window.gtag), origin).toBe('function');
    }
    expect(analyticsRequests).toEqual([]);
  });

  test('file URLs fail closed without creating an analytics queue', async ({ page }) => {
    const analyticsRequests = [];
    page.on('request', request => {
      if (/google-analytics\.com|googletagmanager\.com/i.test(request.url())) analyticsRequests.push(request.url());
    });
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'ghh-analytics-file-origin-'));
    const file = path.join(dir, 'index.html');
    fs.writeFileSync(file, `<script>${analyticsBootstrap}</script><script>gtag('config', 'G-KM69M3NES8');</script>`);
    try {
      await page.goto(pathToFileURL(file).href);
      expect(analyticsRequests).toEqual([]);
      expect(await page.evaluate(() => Object.prototype.hasOwnProperty.call(window, 'dataLayer'))).toBe(false);
      expect(await page.evaluate(() => typeof window.gtag)).toBe('function');
    } finally {
      fs.rmSync(dir, { recursive: true, force: true });
    }
  });

  test('canonical production hostname on a nonstandard port fails closed', async ({ page }) => {
    const analyticsRequests = [];
    page.on('request', request => {
      if (/google-analytics\.com|googletagmanager\.com/i.test(request.url())) analyticsRequests.push(request.url());
    });
    await page.route('https://www.googletagmanager.com/**', route => route.fulfill({ status: 200, contentType: 'application/javascript', body: '' }));
    await page.route('https://www.gohirehumans.com:4443/**', route => {
      if (new URL(route.request().url()).pathname === '/analytics-bootstrap.js') {
        return route.fulfill({ status: 200, contentType: 'application/javascript', body: analyticsBootstrap });
      }
      return route.fulfill({
        status: 200,
        contentType: 'text/html',
        body: '<script src="/analytics-bootstrap.js"></script><script>gtag(\'config\', \'G-KM69M3NES8\');</script>'
      });
    });
    await page.goto('https://www.gohirehumans.com:4443/');
    expect(analyticsRequests).toEqual([]);
    expect(await page.evaluate(() => Object.prototype.hasOwnProperty.call(window, 'dataLayer'))).toBe(false);
    expect(await page.evaluate(() => typeof window.gtag)).toBe('function');
  });

  test('canonical HTTPS production host loads once and normalizes static event context', async ({ page }) => {
    const analyticsRequests = [];
    page.on('request', request => {
      if (/google-analytics\.com|googletagmanager\.com/i.test(request.url())) analyticsRequests.push(request.url());
    });
    await page.route('https://www.googletagmanager.com/**', route => route.fulfill({ status: 200, contentType: 'application/javascript', body: '' }));
    await page.route('https://www.gohirehumans.com/**', route => {
      if (new URL(route.request().url()).pathname === '/analytics-bootstrap.js') {
        return route.fulfill({ status: 200, contentType: 'application/javascript', body: analyticsBootstrap });
      }
      return route.fulfill({
        status: 200,
        contentType: 'text/html',
        body: '<script src="/analytics-bootstrap.js"></script><script>gtag(\'config\', \'G-KM69M3NES8\');gtag(\'event\', \'static_cta_click\', {source:\'pricing\', medium:\'internal\', campaign:\'proof_first\', placement:\'hero\'});</script>'
      });
    });
    await page.goto('https://www.gohirehumans.com/');
    await expect.poll(() => analyticsRequests.length).toBe(1);
    expect(analyticsRequests).toEqual(['https://www.googletagmanager.com/gtag/js?id=G-KM69M3NES8']);
    const commands = await page.evaluate(() => window.dataLayer.map(args => {
      const values = Array.from(args);
      return [values[0], values[1] instanceof Date ? 'DATE' : values[1], values[2]];
    }));
    expect(commands.slice(0, 2).map(command => command.slice(0, 2))).toEqual([
      ['js', 'DATE'],
      ['config', 'G-KM69M3NES8']
    ]);
    const staticEvent = commands.find(command => command[0] === 'event' && command[1] === 'static_cta_click');
    expect(staticEvent?.[2]).toMatchObject({
      ui_source: 'pricing',
      ui_medium: 'internal',
      ui_campaign: 'proof_first',
      placement: 'hero'
    });
    expect(staticEvent?.[2]).not.toHaveProperty('source');
    expect(staticEvent?.[2]).not.toHaveProperty('medium');
    expect(staticEvent?.[2]).not.toHaveProperty('campaign');
  });

  test('internal analytics context cannot overwrite acquisition dimensions', async ({ page }) => {
    await setupDeterministicLocalPage(page);
    await page.goto('/', { waitUntil: 'domcontentloaded' });
    const call = await page.evaluate(() => {
      const calls = [];
      window.gtag = (...args) => calls.push(args);
      trackEvent('job_application_cover_focus', {
        source: 'job_apply_modal',
        medium: 'internal',
        campaign: 'application_flow',
        job_id: '24'
      });
      return calls.at(-1);
    });
    expect(call.slice(0, 2)).toEqual(['event', 'job_application_cover_focus']);
    expect(call[2]).toMatchObject({
      ui_source: 'job_apply_modal',
      ui_medium: 'internal',
      ui_campaign: 'application_flow',
      job_id: '24'
    });
    expect(call[2]).not.toHaveProperty('source');
    expect(call[2]).not.toHaveProperty('medium');
    expect(call[2]).not.toHaveProperty('campaign');
  });

  test('application submission failure stays visible and retryable', async ({ page }) => {
    await setupDeterministicLocalPage(page);
    await page.goto('/', { waitUntil: 'domcontentloaded' });
    await page.evaluate(async () => {
      state.user = { id: 63, name: 'Test Worker' };
      state.token = 'test-token';
      window.__testAnalyticsEvents = [];
      window.gtag = (...args) => window.__testAnalyticsEvents.push(args);
      window.api = async () => { throw new Error('Application service unavailable'); };
      await handleJobApply(24);
    });
    await page.locator('#apply-cover-message').fill('I can test the requested flow and return a proof-backed report tomorrow.');
    await page.locator('#jobApplicationSubmitBtn').click();
    await expect(page.locator('#jobApplicationError')).toContainText('Application service unavailable');
    await expect(page.locator('#jobApplicationSubmitBtn')).toBeEnabled();
    await expect(page.locator('#jobApplicationSubmitBtn')).toHaveText('Submit Application');
    const events = await page.evaluate(() => window.__testAnalyticsEvents
      .filter(args => args[0] === 'event')
      .map(args => ({ name: args[1], params: args[2] })));
    expect(events.map(event => event.name)).toContain('job_application_failed');
    expect(events.find(event => event.name === 'job_application_cover_focus')?.params?.job_id).toBe('24');
  });

  test('guided job draft persists until explicitly cleared', async ({ page }) => {
    await setupDeterministicLocalPage(page);
    await page.goto('/', { waitUntil: 'domcontentloaded' });
    const result = await page.evaluate(() => {
      const draft = { title: 'Check ten AI claims', description: 'Return a sourced issue table.' };
      sessionStorage.setItem('ghh_guided_task_draft', JSON.stringify(draft));
      const firstRead = getStoredGuidedTaskDraft();
      const stillStored = JSON.parse(sessionStorage.getItem('ghh_guided_task_draft'));
      clearStoredGuidedTaskDraft();
      return { firstRead, stillStored, afterClear: sessionStorage.getItem('ghh_guided_task_draft') };
    });
    expect(result.firstRead.title).toBe('Check ten AI claims');
    expect(result.stillStored.title).toBe('Check ten AI claims');
    expect(result.afterClear).toBeNull();
  });

  test('saved job draft context survives switching between login and registration', async ({ page }) => {
    await setupDeterministicLocalPage(page);
    const redirect = 'post-job?draft_title=Check+ten+AI+claims&draft_description=Return+a+sourced+issue+table';
    await page.goto(`/#/login?redirect=${encodeURIComponent(redirect)}`, { waitUntil: 'domcontentloaded' });

    await expect(page.locator('.auth2-title')).toHaveText('Your job draft is saved');
    await expect(page.locator('.auth2-sub')).toHaveText('Sign in or create a free account to review it. Nothing has been posted or charged.');

    await page.locator('.auth2-toggle a').click();
    await expect(page).toHaveURL(new RegExp(`#\\/register\\?redirect=${encodeURIComponent(redirect).replace(/[.*+?^${}()|[\\]\\]/g, '\\$&')}$`));
    await expect(page.locator('.auth2-title')).toHaveText('Your job draft is saved');
    await expect(page.locator('.auth2-sub')).toHaveText('Sign in or create a free account to review it. Nothing has been posted or charged.');

    await page.locator('.auth2-toggle a').click();
    await expect(page).toHaveURL(new RegExp(`#\\/login\\?redirect=${encodeURIComponent(redirect).replace(/[.*+?^${}()|[\\]\\]/g, '\\$&')}$`));

    await page.goto('/#/login?redirect=post-job', { waitUntil: 'domcontentloaded' });
    await expect(page.locator('.auth2-title')).toHaveText('Your job draft is saved');
    await expect(page.locator('.auth2-sub')).toHaveText('Sign in or create a free account to review it. Nothing has been posted or charged.');
  });

  test('job posting failure stays visible and retryable', async ({ page }) => {
    await setupDeterministicLocalPage(page);
    await page.goto('/', { waitUntil: 'domcontentloaded' });
    await page.evaluate(async () => {
      state.user = { id: 7, role: 'employer', name: 'Test Employer' };
      state.token = 'test-token';
      window.__testAnalyticsEvents = [];
      window.gtag = (...args) => window.__testAnalyticsEvents.push(args);
      window.loadCategories = async () => [{ slug: 'research', name: 'Research' }];
      window.api = async (path, options = {}) => {
        if (path === '/jobs' && options.method === 'POST') throw new Error('Job service unavailable');
        return {};
      };
      await renderPostJob();
    });
    await page.locator('input[name="title"]').fill('Check ten AI claims');
    await page.locator('textarea[name="description"]').fill('Return a sourced issue table with one row per claim.');
    await page.locator('select[name="category"]').selectOption('research');
    await page.locator('input[name="budget_amount"]').fill('25');
    await page.locator('#postJobSubmitBtn').click();
    await expect(page.locator('#postJobFormError')).toContainText('Job service unavailable');
    await expect(page.locator('#postJobSubmitBtn')).toBeEnabled();
    await expect(page.locator('#postJobSubmitBtn')).toHaveText('Post Job');
    const failed = await page.evaluate(() => window.__testAnalyticsEvents.some(args =>
      args[1] === 'job_post_failed' && args[2]?.reason === 'request_error'
    ));
    expect(failed).toBe(true);
  });

  test('payment confirmation failure restores the modal for retry', async ({ page }) => {
    await setupDeterministicLocalPage(page);
    await page.goto('/', { waitUntil: 'domcontentloaded' });
    await page.evaluate(async () => {
      window.__testAnalyticsEvents = [];
      window.gtag = (...args) => window.__testAnalyticsEvents.push(args);
      window.Stripe = () => ({
        elements: () => ({ create: () => ({ mount: () => {}, on: () => {} }) }),
        confirmCardSetup: async () => ({ setupIntent: { payment_method: 'pm_test' } })
      });
      window.api = async () => { throw new Error('Confirmation service unavailable'); };
      await showEmployerSetupIntentModal({ client_secret: 'seti_test_secret', publishable_key: 'pk_test' });
    });
    await page.locator('#confirmEmployerPaymentBtn').click();
    await expect(page.locator('#employer-card-error')).toContainText('Confirmation service unavailable');
    await expect(page.locator('#confirmEmployerPaymentBtn')).toBeEnabled();
    await expect(page.locator('#confirmEmployerPaymentBtn')).toHaveText('Save payment method');
    const failed = await page.evaluate(() => window.__testAnalyticsEvents.some(args =>
      args[1] === 'payment_setup_failed' && args[2]?.reason === 'confirm_request_error'
    ));
    expect(failed).toBe(true);
  });

  for (const route of routes) {
    test(`${route.path} renders, has no serious axe violations, and is console-clean`, async ({ page }) => {
      const messages = await collectConsole(page);
      await setupDeterministicLocalPage(page);
      const response = await page.goto(route.path, { waitUntil: 'domcontentloaded' });
      await page.waitForLoadState('networkidle', { timeout: 5000 }).catch(() => {});
      expect(response.status(), route.path).toBeLessThan(400);
      await expect(page.locator('body')).toContainText(route.mustContain);
      const scan = await new AxeBuilder({ page }).withTags(['wcag2a', 'wcag2aa']).exclude('iframe').analyze();
      const serious = scan.violations.filter(v => ['serious', 'critical'].includes(v.impact));
      expect(serious, JSON.stringify(serious.map(v => ({ id: v.id, impact: v.impact, nodes: v.nodes.length })), null, 2)).toEqual([]);
      expect(messages, messages.join('\n')).toEqual([]);
    });
  }

  test('homepage and pricing route high-intent visitors to proof-backed QA paths', async ({ page }) => {
    await setupDeterministicLocalPage(page);
    await page.goto('/', { waitUntil: 'domcontentloaded' });
    await page.evaluate(() => saveSession('pricing-draft-test-token', {
      id: 501,
      name: 'Pricing Draft Tester',
      email: 'pricing-draft@example.test'
    }));
    await expect(page.locator('body')).toContainText('What do you need help with?');
    await expect(page.locator('.lp-start-card[href="/starter-offers.html"]')).toContainText('Start with QA');
    await page.goto('/pricing.html', { waitUntil: 'domcontentloaded' });
    await expect(page.locator('body')).toContainText('Prefer a fixed starting point?');
    const offers = [
      { heading: 'AI Output Verification', template: 'ai_review', title: 'Have a human QA AI-generated output', budget: '99' },
      { heading: 'Automation QA Sprint', template: 'automation_verification', title: 'Verify AI agent or automation runs', budget: '199' },
      { heading: 'Clay/GTM QA Sprint', template: 'clay_gtm_qa', title: 'Human QA a Clay or GTM lead list', budget: '199' },
      { heading: 'Real-World Check', template: 'phone_fact_check', title: 'Make phone calls or verify a fact', budget: '79' }
    ];
    for (const offer of offers) {
      await page.goto('/pricing.html', { waitUntil: 'domcontentloaded' });
      const card = page.locator('.feature-item').filter({ has: page.getByRole('heading', { name: offer.heading, exact: true }) });
      await card.getByRole('link', { name: 'Start this draft' }).click();
      await expect(page).toHaveURL(new RegExp(`\\?template=${offer.template}(?:#.*)?$`));
      await expect(page.locator('input[name="title"]')).toHaveValue(offer.title);
      await expect(page.locator('input[name="budget_amount"]')).toHaveValue(offer.budget);
    }
    await page.goto('/pricing.html', { waitUntil: 'domcontentloaded' });
    await expect(page.locator('body')).toContainText('View starter offers');
    await page.goto('/starter-offers.html', { waitUntil: 'domcontentloaded' });
    await expect(page.locator('body')).toContainText('Four proof-backed starter offers');
  });

  test('public nav hover and active states are consistent across static and SPA pages', async ({ page, isMobile }) => {
    test.skip(isMobile, 'desktop nav hover states are hidden behind the mobile menu');
    await setupDeterministicLocalPage(page);
    const cases = [
      { path: '/', active: null },
      { path: '/starter-offers.html', active: null },
      { path: '/pricing.html', active: 'Pricing' },
      { path: '/trust-safety.html', active: 'Trust' },
      { path: '/ai-integration.html', active: 'For Agents' },
      { path: '/earn/get-paid-for-human-tasks.html', active: 'Find Work' },
      { path: '/use-cases/hire-human-to-review-ai-output.html', active: 'Marketplace' },
      { path: '/#/services', active: 'Marketplace' },
      { path: '/#/jobs', active: 'Find Work' },
      { path: '/#/ai-employers', active: 'For Agents' }
    ];
    let canonicalHover = null;
    for (const item of cases) {
      await page.goto(item.path, { waitUntil: 'domcontentloaded' });
      await page.waitForLoadState('networkidle', { timeout: 5000 }).catch(() => {});
      const navLinks = page.locator('.lp-nav-link');
      await expect(navLinks.first(), item.path).toBeVisible();

      const inactive = item.active
        ? page.locator('.lp-nav-link').filter({ hasNotText: item.active }).first()
        : page.locator('.lp-nav-link').first();
      await inactive.hover();
      await page.waitForTimeout(180);
      const hover = await inactive.evaluate(el => {
        const s = getComputedStyle(el);
        return { color: s.color, background: s.backgroundColor, textDecoration: s.textDecorationLine };
      });
      if (!canonicalHover) canonicalHover = hover;
      expect(hover.textDecoration, `${item.path} inactive hover should not underline`).toBe('none');
      expect(hover.color, `${item.path} inactive hover color`).toBe(canonicalHover.color);
      expect(hover.background, `${item.path} inactive hover background`).toBe(canonicalHover.background);

      const activeLinks = page.locator('.lp-nav-link[aria-current="page"]');
      if (item.active) {
        await expect(activeLinks, `${item.path} should expose one active nav item`).toHaveCount(1);
        await expect(activeLinks.first()).toHaveText(item.active);
        const active = await activeLinks.first().evaluate(el => {
          const s = getComputedStyle(el);
          return { color: s.color, background: s.backgroundColor, textDecoration: s.textDecorationLine };
        });
        expect(active.textDecoration, `${item.path} active nav should not underline`).toBe('none');
        expect(active.color, `${item.path} active nav color`).toBe('rgb(13, 115, 119)');
        expect(active.background, `${item.path} active nav background`).toBe('rgb(230, 243, 243)');
      } else {
        await expect(activeLinks, `${item.path} should not mark a section active`).toHaveCount(0);
      }
    }
  });

  test('public logo remains visually and accessibly consistent across static and SPA pages', async ({ page, isMobile }) => {
    test.skip(isMobile, 'desktop logo/header consistency is covered at desktop nav width');
    await setupDeterministicLocalPage(page);
    const cases = [
      '/',
      '/pricing.html',
      '/starter-offers.html',
      '/trust-safety.html',
      '/proof-packs.html',
      '/stats.html',
      '/ai-integration.html',
      '/use-cases/hire-human-to-review-ai-output.html',
      '/ai-qa-task-generator.html',
      '/ai-human-qa/index.html',
      '/#/services',
      '/#/jobs'
    ];
    for (const path of cases) {
      await page.goto(path, { waitUntil: 'domcontentloaded' });
      await page.waitForLoadState('networkidle', { timeout: 5000 }).catch(() => {});
      const logo = page.locator('.lp-nav-logo').first();
      await expect(logo, `${path} logo visible`).toBeVisible();
      await expect(logo, `${path} logo accessible name`).toHaveAccessibleName('GoHireHumans');
      await expect(logo, `${path} logo href`).toHaveAttribute('href', '/');
      const normal = await logo.evaluate(el => {
        const s = getComputedStyle(el);
        const svg = el.querySelector('svg');
        const ss = svg ? getComputedStyle(svg) : null;
        const sr = svg ? svg.getBoundingClientRect() : null;
        const r = el.getBoundingClientRect();
        return {
          color: s.color,
          textDecoration: s.textDecorationLine,
          fontSize: s.fontSize,
          fontWeight: s.fontWeight,
          lineHeight: s.lineHeight,
          gap: s.gap,
          whiteSpace: s.whiteSpace,
          width: r.width,
          svgWidth: sr ? sr.width : 0,
          svgHeight: sr ? sr.height : 0,
          svgDisplay: ss ? ss.display : '',
          svgFlexShrink: ss ? ss.flexShrink : ''
        };
      });
      expect(normal.color, `${path} logo text color`).toBe('rgb(26, 24, 22)');
      expect(normal.textDecoration, `${path} logo text decoration`).toBe('none');
      expect(normal.fontSize, `${path} logo font size`).toBe('15px');
      expect(normal.fontWeight, `${path} logo weight`).toBe('700');
      expect(normal.lineHeight, `${path} logo line height`).toBe('28px');
      expect(normal.gap, `${path} logo gap`).toBe('8px');
      expect(normal.whiteSpace, `${path} logo nowrap`).toBe('nowrap');
      expect(normal.svgWidth, `${path} logo svg width`).toBe(28);
      expect(normal.svgHeight, `${path} logo svg height`).toBe(28);
      expect(normal.svgDisplay, `${path} logo svg display`).toBe('block');
      expect(normal.svgFlexShrink, `${path} logo svg flex shrink`).toBe('0');
      expect(normal.width, `${path} logo width tolerance`).toBeGreaterThan(130);
      expect(normal.width, `${path} logo width tolerance`).toBeLessThan(170);
      await logo.hover();
      await page.waitForTimeout(180);
      const hover = await logo.evaluate(el => {
        const s = getComputedStyle(el);
        return { color: s.color, background: s.backgroundColor, textDecoration: s.textDecorationLine };
      });
      expect(hover.color, `${path} logo hover color`).toBe('rgb(26, 24, 22)');
      expect(hover.textDecoration, `${path} logo hover text decoration`).toBe('none');
    }
  });

  test('public footer and mobile menu polish stay consistent on representative pages', async ({ page, isMobile }) => {
    test.skip(!isMobile, 'mobile menu semantics are covered at mobile width');
    await setupDeterministicLocalPage(page);
    const cases = ['/', '/pricing.html', '/starter-offers.html', '/trust-safety.html', '/proof-packs.html', '/stats.html', '/#/services', '/#/jobs'];
    for (const path of cases) {
      await page.goto(path, { waitUntil: 'domcontentloaded' });
      await page.waitForLoadState('networkidle', { timeout: 5000 }).catch(() => {});
      await expect(page.locator('.lp-footer').first(), `${path} canonical footer`).toBeVisible();
      await expect(page.locator('.lp-footer').first(), `${path} footer contact`).toContainText('contact@gohirehumans.com');
      await expect(page.locator('.lp-footer').first(), `${path} no old builder credit`).not.toContainText('Created with Perplexity Computer');
      const button = page.locator('.lp-hamburger').first();
      await expect(button, `${path} hamburger controls menu`).toHaveAttribute('aria-controls', 'mobileMenu');
      await expect(button, `${path} hamburger starts closed`).toHaveAttribute('aria-expanded', 'false');
      const menu = page.locator('#mobileMenu').first();
      await expect(menu, `${path} menu initially hidden`).toBeHidden();
      await button.click();
      await expect(button, `${path} hamburger opens`).toHaveAttribute('aria-expanded', 'true');
      await expect(menu, `${path} menu visible after click`).toBeVisible();
      await page.keyboard.press('Escape');
      await expect(button, `${path} hamburger closes on escape`).toHaveAttribute('aria-expanded', 'false');
      await expect(menu, `${path} menu hidden after escape`).toBeHidden();
    }
  });

  test('Batch B public shell backlog polish remains visible on high-intent pages', async ({ page }) => {
    await setupDeterministicLocalPage(page);
    const shellPages = ['/', '/starter-offers.html', '/pricing.html', '/trust-safety.html', '/proof-packs.html', '/stats.html', '/use-cases/'];
    for (const path of shellPages) {
      await page.goto(path, { waitUntil: 'domcontentloaded' });
      await expect(page.locator('.lp-footer').first(), `${path} canonical footer`).toBeVisible();
      await expect(page.locator('.lp-footer').first(), `${path} worker jobs label`).toContainText('Find Work');
      await expect(page.locator('body'), `${path} no legacy builder attribution`).not.toContainText('Created with Perplexity Computer');
    }
    await page.goto('/stats.html', { waitUntil: 'domcontentloaded' });
    await expect(page.locator('a.btn-primary[href="/#/register"]')).toContainText('Create a free account');
    await page.goto('/trust-safety.html', { waitUntil: 'domcontentloaded' });
    await expect(page.locator('#trust-next-step-heading')).toContainText('Start with a small, reviewable task');
    await expect(page.locator('.trust-next-step a[href="/#/post-job"]')).toContainText('Post a task');
    await page.goto('/use-cases/', { waitUntil: 'domcontentloaded' });
    await expect(page.locator('main')).toContainText('Human AI Output Verification');
  });

  test('stats page renders deliberate category chart fallback without blocked CDN dependency', async ({ page }) => {
    await setupDeterministicLocalPage(page);
    await page.goto('/stats.html', { waitUntil: 'domcontentloaded' });
    await page.waitForLoadState('networkidle', { timeout: 5000 }).catch(() => {});
    await expect(page.locator('.chart-fallback').first()).toBeVisible();
    await expect(page.locator('.chart-row').first()).toBeVisible();
    await expect(page.locator('canvas#cat-chart')).toHaveCount(0);
    const cdnRequests = await page.evaluate(() => performance.getEntriesByType('resource').map(e => e.name).filter(name => name.includes('cdn.jsdelivr.net/npm/chart.js')));
    expect(cdnRequests).toEqual([]);
  });

  test('auth page only shows OR divider with rendered Google button and exposes inline login errors', async ({ page, isMobile }) => {
    test.skip(!isMobile, 'mobile auth polish regression');
    await page.route('https://accounts.google.com/**', route => route.fulfill({ status: 204, body: '' }));
    await page.route('https://gohirehumans-production.up.railway.app/auth/login', route => route.fulfill({ status: 401, contentType: 'application/json', body: JSON.stringify({ detail: 'Invalid email or password.' }) }));
    await page.goto('/#/login', { waitUntil: 'domcontentloaded' });
    await expect(page.locator('.auth2-title')).toContainText('Welcome back');
    await expect(page.locator('#google-signin-divider')).toBeHidden();
    await expect(page.locator('#google-signin-wrap')).toBeHidden();
    await page.locator('#auth-email').fill('nobody@example.com');
    await page.locator('#auth-password').fill('nottherightpassword');
    await page.locator('#authForm button[type="submit"]').click();
    await expect(page.locator('#auth-error')).toBeVisible();
    await expect(page.locator('#auth-error')).toContainText('Invalid email or password.');
  });

  test('authenticated revision loop renders both parties notes and submits the canonical payload', async ({ page }) => {
    let submissionBody = null;
    await page.addInitScript(() => {
      sessionStorage.setItem('ghh_token', 'tok-worker');
      localStorage.setItem('ghh_user', JSON.stringify({ id: 1, name: 'Worker', role: 'worker' }));
    });
    await page.route('https://accounts.google.com/**', route => route.fulfill({ status: 204, body: '' }));
    await page.route('https://gohirehumans-production.up.railway.app/**', async route => {
      const request = route.request();
      const path = new URL(request.url()).pathname;
      if (path === '/orders/7/submit' && request.method() === 'POST') {
        submissionBody = request.postDataJSON();
        return route.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify({ status: 'submitted' }) });
      }
      if (path === '/orders/7') {
        return route.fulfill({
          status: 200,
          contentType: 'application/json',
          body: JSON.stringify({
            id: 7,
            worker_id: 1,
            employer_id: 2,
            status: 'revision_requested',
            total_amount: 25,
            worker_notes: 'Initial mobile QA evidence',
            employer_notes: 'Please retest the navigation drawer',
            milestones: [{ id: 1, description: 'Retest navigation', amount: 25, status: 'in_progress' }]
          })
        });
      }
      return route.fulfill({ status: 200, contentType: 'application/json', body: '{}' });
    });

    await page.goto('/#/orders/7', { waitUntil: 'domcontentloaded' });
    await expect(page.locator('body')).toContainText('Initial mobile QA evidence');
    await expect(page.locator('body')).toContainText('Please retest the navigation drawer');
    await expect(page.locator('body')).toContainText('Retest navigation');
    await page.getByRole('button', { name: 'Submit Deliverables' }).click();
    await page.locator('textarea[name="note"]').fill('Retested mobile navigation with screenshots');
    await page.locator('form').filter({ has: page.locator('textarea[name="note"]') }).getByRole('button', { name: 'Submit for Review' }).click();
    await expect.poll(() => submissionBody).toEqual({ notes: 'Retested mobile navigation with screenshots' });
  });

  test('hourly order detail uses the backend contract and blocks unsafe settlement actions', async ({ page }) => {
    await page.addInitScript(() => {
      sessionStorage.setItem('ghh_token', 'worker-token');
      localStorage.setItem('ghh_user', JSON.stringify({ id: 1, name: 'Worker', is_admin: false }));
    });
    await page.route('https://gohirehumans-production.up.railway.app/**', async route => {
      const url = new URL(route.request().url());
      if (url.pathname === '/orders/88') {
        return route.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify({
          id: 88,
          worker_id: 1,
          employer_id: 2,
          status: 'in_progress',
          total_amount: 25,
          hourly_contract: { hourly_rate: 25, weekly_hour_cap: 40, current_week_escrow_amount: 1000 },
          time_entries: [{ id: 3, date: '2026-07-09', hours: 2, description: 'Mobile QA pass' }]
        }) });
      }
      return route.fulfill({ status: 200, contentType: 'application/json', body: '{}' });
    });

    await page.goto('/#/orders/88', { waitUntil: 'domcontentloaded' });
    await expect(page.locator('body')).toContainText('Hourly');
    await expect(page.locator('body')).toContainText('Current week funded');
    await expect(page.locator('body')).toContainText('$1000.00');
    await expect(page.locator('body')).toContainText('Mobile QA pass');
    await expect(page.locator('body')).toContainText('Hourly contract actions paused');
    await expect(page.getByRole('button', { name: 'Submit Deliverables' })).toHaveCount(0);
    await expect(page.getByRole('button', { name: 'Approve Payment' })).toHaveCount(0);
  });

  test('fee rounding and order summaries distinguish awkward-cent totals from hourly rates', async ({ page }) => {
    await page.addInitScript(() => {
      sessionStorage.setItem('ghh_token', 'worker-token');
      localStorage.setItem('ghh_user', JSON.stringify({ id: 1, name: 'Worker', is_admin: false }));
    });
    await page.route('https://gohirehumans-production.up.railway.app/**', async route => {
      const url = new URL(route.request().url());
      if (url.pathname === '/orders') {
        return route.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify({
          orders: [
            { id: 88, worker_id: 1, employer_id: 2, status: 'in_progress', total_amount: 25.55, contract_type: 'hourly', job_title: 'Hourly QA', created_at: '2026-07-09T00:00:00Z' },
            { id: 89, worker_id: 1, employer_id: 2, status: 'in_progress', total_amount: 40, contract_type: 'fixed', job_title: 'Fixed QA', created_at: '2026-07-09T00:00:00Z' }
          ]
        }) });
      }
      return route.fulfill({ status: 200, contentType: 'application/json', body: '{}' });
    });

    await page.goto('/#/orders', { waitUntil: 'domcontentloaded' });
    await expect(page.locator('body')).toContainText('$25.55/hr');
    await expect(page.locator('body')).toContainText('$40.00 total');
    await expect(page.locator('body')).not.toContainText('$25.55 total');

    const awkward = await page.evaluate(() => hireFeeBreakdown('25.55'));
    expect(awkward).toEqual({ base: 25.55, platformFee: 0.26, processingFee: 0.77, total: 26.58 });
    const tiny = await page.evaluate(() => hireFeeBreakdown('0.01'));
    expect(tiny).toEqual({ base: 0.01, platformFee: 0.01, processingFee: 0.01, total: 0.03 });
  });

  test('fixed order detail separates contract total from authoritative funded charge', async ({ page }) => {
    await page.addInitScript(() => {
      sessionStorage.setItem('ghh_token', 'employer-token');
      localStorage.setItem('ghh_user', JSON.stringify({ id: 2, name: 'Employer', is_admin: false }));
    });
    await page.route('https://gohirehumans-production.up.railway.app/**', async route => {
      const url = new URL(route.request().url());
      if (url.pathname === '/orders/90') {
        return route.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify({
          id: 90, worker_id: 1, employer_id: 2, status: 'in_progress', total_amount: 25.55,
          contract_type: 'fixed', job_title: 'Two-stage QA', created_at: '2026-07-09T00:00:00Z',
          milestones: [
            { id: 1, description: 'First', amount: 10, status: 'in_progress' },
            { id: 2, description: 'Second', amount: 15.55, status: 'pending' }
          ],
          escrow_holds: [{ id: 1, milestone_id: 1, amount: 10, status: 'held' }],
          funding_summary: { base_cents: 1000, platform_fee_cents: 10, processing_fee_cents: 30, charged_total_cents: 1040, funded_amount_available: true, charge_amount_available: true, record_count: 1 }
        }) });
      }
      return route.fulfill({ status: 200, contentType: 'application/json', body: '{}' });
    });

    await page.goto('/#/orders/90', { waitUntil: 'domcontentloaded' });
    await expect(page.locator('body')).toContainText('Total $25.55');
    await expect(page.locator('body')).toContainText('Funded to date');
    await expect(page.locator('body')).toContainText('$10.00');
    await expect(page.locator('body')).toContainText('Charged to date');
    await expect(page.locator('body')).toContainText('$10.40');
    await expect(page.locator('body')).not.toContainText('You paid');
    await expect(page.locator('body')).not.toContainText('$26.58');
  });

  test('legacy funding detail never invents a historical charged total', async ({ page }) => {
    await page.addInitScript(() => {
      sessionStorage.setItem('ghh_token', 'employer-token');
      localStorage.setItem('ghh_user', JSON.stringify({ id: 2, name: 'Employer', is_admin: false }));
    });
    await page.route('https://gohirehumans-production.up.railway.app/**', async route => {
      const url = new URL(route.request().url());
      if (url.pathname === '/orders/92') {
        return route.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify({
          id: 92, worker_id: 1, employer_id: 2, status: 'in_progress', total_amount: 25.55,
          contract_type: 'fixed', job_title: 'Legacy QA', created_at: '2026-07-01T00:00:00Z',
          milestones: [{ id: 1, description: 'Delivery', amount: 25.55, status: 'in_progress' }],
          escrow_holds: [{ id: 1, milestone_id: 1, amount: 25.55, status: 'held' }],
          funding_summary: { base_cents: 2555, platform_fee_cents: null, processing_fee_cents: null, charged_total_cents: null, funded_amount_available: true, charge_amount_available: false, record_count: 1 }
        }) });
      }
      return route.fulfill({ status: 200, contentType: 'application/json', body: '{}' });
    });

    await page.goto('/#/orders/92', { waitUntil: 'domcontentloaded' });
    await expect(page.locator('body')).toContainText('Funded to date');
    await expect(page.locator('body')).toContainText('$25.55');
    await expect(page.locator('body')).toContainText('Historical charge total unavailable');
    await expect(page.locator('body')).not.toContainText('Charged to date');
  });

  test('hourly hire modal rejects fractional caps without posting', async ({ page }) => {
    let hireBody = null;
    await page.addInitScript(() => {
      sessionStorage.setItem('ghh_token', 'employer-token');
      localStorage.setItem('ghh_user', JSON.stringify({ id: 2, name: 'Employer', is_admin: false }));
    });
    await page.route('https://gohirehumans-production.up.railway.app/**', async route => {
      const url = new URL(route.request().url());
      if (url.pathname === '/jobs/12/hire') {
        hireBody = route.request().postDataJSON();
        return route.fulfill({ status: 201, contentType: 'application/json', body: '{"id":91}' });
      }
      return route.fulfill({ status: 200, contentType: 'application/json', body: '{}' });
    });

    await page.goto('/', { waitUntil: 'domcontentloaded' });
    await page.evaluate(() => showHourlyHireModal(12, 44, 25.55));
    await page.locator('#weekly-hour-cap').fill('6.0000000000000001');
    await page.getByRole('button', { name: 'Confirm & Fund First Week' }).click();
    await expect(page.locator('body')).toContainText('whole number');
    expect(hireBody).toBeNull();
  });

  test('service checkout reuses one client operation identity after an ambiguous retry', async ({ page }) => {
    const orderBodies = [];
    await page.addInitScript(() => {
      sessionStorage.setItem('ghh_token', 'employer-token');
      localStorage.setItem('ghh_user', JSON.stringify({ id: 2, name: 'Employer', is_admin: false }));
    });
    await page.route('https://gohirehumans-production.up.railway.app/**', async route => {
      const url = new URL(route.request().url());
      if (url.pathname === '/payments/status') {
        return route.fulfill({ status: 200, contentType: 'application/json', body: '{"employer_ready":true}' });
      }
      if (url.pathname === '/services/1/order') {
        orderBodies.push(route.request().postDataJSON());
        if (orderBodies.length === 1) {
          return route.fulfill({ status: 500, contentType: 'application/json', body: '{"error":"ambiguous response loss"}' });
        }
        return route.fulfill({ status: 201, contentType: 'application/json', body: '{"id":93}' });
      }
      return route.fulfill({ status: 200, contentType: 'application/json', body: '{}' });
    });

    await page.goto('/', { waitUntil: 'domcontentloaded' });
    await page.evaluate(() => handleOrderService(1));
    await page.getByRole('button', { name: 'Place Order' }).click();
    await expect.poll(() => orderBodies.length).toBe(1);
    await page.reload({ waitUntil: 'domcontentloaded' });
    await page.evaluate(() => handleOrderService(1));
    await page.getByRole('button', { name: 'Place Order' }).click();
    await expect.poll(() => orderBodies.length).toBe(2);
    expect(orderBodies[0].idempotency_key).toMatch(/^[A-Za-z0-9._:-]{16,128}$/);
    expect(orderBodies[1].idempotency_key).toBe(orderBodies[0].idempotency_key);
    await expect.poll(() => page.evaluate(() => sessionStorage.getItem('ghh_pending_service_order_1'))).toBeNull();
  });

  test('clearing a session removes pending service checkout operation identities', async ({ page }) => {
    await page.addInitScript(() => {
      sessionStorage.setItem('ghh_token', 'employer-token');
      localStorage.setItem('ghh_user', JSON.stringify({ id: 2, name: 'Employer', is_admin: false }));
    });
    await page.route('https://gohirehumans-production.up.railway.app/**', route =>
      route.fulfill({ status: 200, contentType: 'application/json', body: '{}' })
    );
    await page.goto('/', { waitUntil: 'domcontentloaded' });

    const cleared = await page.evaluate(() => {
      const value = 'service-order-12345678-1234-1234-1234-123456789012';
      sessionStorage.setItem('ghh_pending_service_order_1', value);
      pendingServiceOrderOperations.set('1', value);
      clearSession();
      return {
        stored: sessionStorage.getItem('ghh_pending_service_order_1'),
        cached: pendingServiceOrderOperations.has('1'),
      };
    });

    expect(cleared).toEqual({ stored: null, cached: false });
  });

  test('order deadline UI renders lifecycle evidence and sends a canonical revision deadline', async ({ page }) => {
    let revisionBody = null;
    await page.addInitScript(() => {
      sessionStorage.setItem('ghh_token', 'employer-token');
      localStorage.setItem('ghh_user', JSON.stringify({ id: 2, name: 'Employer', is_admin: false }));
    });
    await page.route('https://gohirehumans-production.up.railway.app/**', async route => {
      const url = new URL(route.request().url());
      if (url.pathname === '/orders/7/request-revision') {
        revisionBody = route.request().postDataJSON();
        return route.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify({ status: 'revision_requested' }) });
      }
      if (url.pathname === '/orders/7') {
        return route.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify({
          id: 7, type: 'job_hire', budget_type: 'fixed', worker_id: 1, employer_id: 2,
          status: 'submitted', total_amount: 25, deadline_at: '2026-07-14T18:00:00Z',
          submitted_at: '2026-07-13T15:00:00Z', revision_requested_at: null, milestones: []
        }) });
      }
      return route.fulfill({ status: 200, contentType: 'application/json', body: '{}' });
    });

    await page.goto('/#/orders/7', { waitUntil: 'domcontentloaded' });
    await expect(page.locator('body')).toContainText('Deadline');
    await expect(page.locator('body')).toContainText('Submitted');
    await page.getByRole('button', { name: 'Request Revision' }).click();
    await page.locator('textarea[name="message"]').fill('Retest the checkout state');
    await page.locator('#revisionForm').getByRole('button', { name: 'Request Revision' }).click();
    await expect.poll(() => revisionBody).not.toBeNull();
    expect(revisionBody.notes).toBe('Retest the checkout state');
    expect(revisionBody.deadline_at).toMatch(/Z$/);
    expect(Number.isNaN(Date.parse(revisionBody.deadline_at))).toBe(false);
  });

  test('legacy fixed order deadline action and admin overdue filter use scoped APIs', async ({ page }) => {
    let deadlineBody = null;
    const requestedPaths = [];
    await page.addInitScript(() => {
      sessionStorage.setItem('ghh_token', 'employer-token');
      localStorage.setItem('ghh_user', JSON.stringify({ id: 2, name: 'Employer', is_admin: false }));
    });
    await page.route('https://gohirehumans-production.up.railway.app/**', async route => {
      const request = route.request();
      const url = new URL(request.url());
      requestedPaths.push(url.pathname + url.search);
      if (url.pathname === '/orders/10/deadline') {
        deadlineBody = request.postDataJSON();
        return route.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify({ id: 10, deadline_at: deadlineBody.deadline_at }) });
      }
      if (url.pathname === '/orders/10') {
        return route.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify({
          id: 10, type: 'job_hire', budget_type: 'fixed', worker_id: 1, employer_id: 2,
          status: 'in_progress', total_amount: 25, deadline_at: null, milestones: []
        }) });
      }
      if (url.pathname === '/admin/orders') {
        return route.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify({
          orders: [{ id: 10, status: 'in_progress', total_amount: 25, contract_type: 'fixed',
            worker_name: 'Worker', employer_name: 'Employer', job_title: 'Legacy QA',
            deadline_at: '2026-07-10T12:00:00Z', is_overdue: 1, created_at: '2026-07-01T00:00:00Z' }], total: 1
        }) });
      }
      return route.fulfill({ status: 200, contentType: 'application/json', body: '{}' });
    });

    await page.goto('/#/orders/10', { waitUntil: 'domcontentloaded' });
    await page.getByRole('button', { name: 'Set Deadline' }).click();
    await page.locator('#deadlineForm').getByRole('button', { name: 'Set Deadline' }).click();
    await expect.poll(() => deadlineBody).not.toBeNull();
    expect(deadlineBody.deadline_at).toMatch(/Z$/);

    await page.evaluate(() => {
      sessionStorage.setItem('ghh_token', 'admin-token');
      localStorage.setItem('ghh_user', JSON.stringify({ id: 9, name: 'Admin', is_admin: true }));
    });
    await page.reload({ waitUntil: 'domcontentloaded' });
    await page.goto('/#/admin/orders', { waitUntil: 'domcontentloaded' });
    await page.getByLabel('Overdue only').check();
    await expect.poll(() => requestedPaths.includes('/admin/orders?overdue=true')).toBe(true);
    await expect(page.locator('body')).toContainText('Overdue');
    await expect(page.locator('body')).toContainText('Deadline');
  });

  test('admin disputes use the admin-scoped filtered order endpoint', async ({ page }) => {
    const requestedPaths = [];
    await page.addInitScript(() => {
      sessionStorage.setItem('ghh_token', 'admin-token');
      localStorage.setItem('ghh_user', JSON.stringify({ id: 9, name: 'Admin', is_admin: true }));
    });
    await page.route('https://gohirehumans-production.up.railway.app/**', async route => {
      const url = new URL(route.request().url());
      requestedPaths.push(url.pathname + url.search);
      if (url.pathname === '/admin/orders') {
        return route.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify({
          orders: [{ id: 88, status: 'disputed', total_amount: 25, contract_type: 'fixed', worker_name: 'Worker', employer_name: 'Employer', job_title: 'Disputed QA', created_at: '2026-07-09T00:00:00Z' }]
        }) });
      }
      if (url.pathname === '/orders') {
        return route.fulfill({ status: 200, contentType: 'application/json', body: '{"orders":[]}' });
      }
      return route.fulfill({ status: 200, contentType: 'application/json', body: '{}' });
    });

    await page.goto('/#/admin/disputes', { waitUntil: 'domcontentloaded' });
    await expect(page.locator('body')).toContainText('Order #88');
    expect(requestedPaths).toContain('/admin/orders?status=disputed');
    expect(requestedPaths).not.toContain('/orders?status=disputed');
  });

  test('admin task-amount refund requires password and sends no manual settlement claim', async ({ page }) => {
    let body = null;
    await page.addInitScript(() => {
      sessionStorage.setItem('ghh_token', 'admin-token');
      localStorage.setItem('ghh_user', JSON.stringify({ id: 9, name: 'Admin', is_admin: true }));
      window.prompt = () => 'step-up-password';
    });
    await page.route('https://gohirehumans-production.up.railway.app/**', async route => {
      const url = new URL(route.request().url());
      if (url.pathname === '/admin/orders') return route.fulfill({status:200,contentType:'application/json',body:JSON.stringify({orders:[{id:88,status:'disputed',total_amount:25,worker_name:'Worker',employer_name:'Employer'}]})});
      if (url.pathname === '/admin/resolve-dispute') { body=route.request().postDataJSON(); return route.fulfill({status:200,contentType:'application/json',body:'{"ok":true,"resolution":"refund_to_employer","status":"succeeded","idempotent_replay":false}'}); }
      return route.fulfill({status:200,contentType:'application/json',body:'{}'});
    });
    await page.goto('/#/admin/disputes', { waitUntil: 'domcontentloaded' });
    await expect(page.getByText('Stripe processing and the 1% platform fee are not automatically refunded.')).toBeVisible();
    await page.getByRole('button',{name:'Issue task-amount refund'}).click();
    await page.getByRole('button',{name:'Issue refund'}).click();
    await expect.poll(() => body).not.toBeNull();
    expect(body).toEqual({order_id:88,resolution:'refund_to_employer',admin_password:'step-up-password'});
    await expect(page.getByText('Task-amount refund committed')).toBeVisible();
  });

  test('new job hiring stays visibly paused until payment safeguards ship', async ({ page }) => {
    let hireBody = null;
    await page.addInitScript(() => {
      sessionStorage.setItem('ghh_token', 'employer-token');
      localStorage.setItem('ghh_user', JSON.stringify({ id: 2, name: 'Employer', is_admin: false }));
    });
    await page.route('https://gohirehumans-production.up.railway.app/**', async route => {
      const url = new URL(route.request().url());
      if (url.pathname === '/jobs/12' && route.request().method() === 'GET') {
        return route.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify({
          id: 12, employer_id: 2, title: 'Hourly mobile QA', budget_type: 'hourly', budget_amount: 25, status: 'open'
        }) });
      }
      if (url.pathname === '/jobs/12/applications') {
        return route.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify({
          applications: [{ id: 44, worker_id: 1, worker_name: 'QA Worker', cover_message: 'Ready', status: 'pending' }]
        }) });
      }
      if (url.pathname === '/payments/status') {
        return route.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify({ employer_ready: true }) });
      }
      if (url.pathname === '/jobs/12/hire') {
        hireBody = route.request().postDataJSON();
        return route.fulfill({ status: 201, contentType: 'application/json', body: JSON.stringify({ id: 91 }) });
      }
      if (url.pathname === '/orders') {
        return route.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify({ orders: [] }) });
      }
      return route.fulfill({ status: 200, contentType: 'application/json', body: '{}' });
    });

    await page.goto('/#/jobs/12/applicants', { waitUntil: 'domcontentloaded' });
    const paused = page.getByRole('button', { name: 'Hiring temporarily paused' });
    await expect(paused).toBeVisible();
    await expect(paused).toBeDisabled();
    expect(hireBody).toBeNull();
  });

  test('unknown public path returns true 404 page', async ({ page }) => {
    const response = await page.goto('/no-such-route-ui-audit', { waitUntil: 'domcontentloaded' });
    expect(response.status()).toBe(404);
    // Local python static server returns its own 404 body; production serves frontend/404.html.
    // The regression target here is the HTTP status so false-200 rewrites cannot return.
    await expect(page.locator('body')).toContainText(/404|not found|File not found/i);
  });
  test('pricing and services mobile paths expose useful content before filter overload', async ({ page, isMobile }) => {
    test.skip(!isMobile, 'mobile-only smoke');
    await setupDeterministicLocalPage(page);
    await page.goto('/pricing.html');
    await expect(page.locator('body')).toContainText('Compare Fee Structures');
    await page.goto('/#/services', { waitUntil: 'domcontentloaded' });
    await page.waitForLoadState('networkidle', { timeout: 5000 }).catch(() => {});
    await expect(page.locator('#services-result-count')).toBeVisible();
    await expect(page.locator('[data-filter-toggle]')).toHaveText('Filters');
  });

  test('homepage service previews preserve unknown, zero, and canonical worker facts', async ({ page }) => {
    await setupDeterministicLocalPage(page);
    await page.route('https://gohirehumans-production.up.railway.app/services?**', route => route.fulfill({
      status: 200,
      contentType: 'application/json',
      body: JSON.stringify({ services: [
        {
          id: 31,
          title: 'Unknown review history',
          worker_name: null,
          category: 'testing',
          pricing_type: 'fixed',
          price: 99,
          delivery_time_days: null,
          worker_rating: null,
          worker_review_count: null,
        },
        {
          id: 32,
          title: 'Legacy zero-day listing',
          worker_name: 'Fast verifier',
          category: 'testing',
          pricing_type: 'fixed',
          price: 79,
          delivery_time_days: 0,
          worker_rating: 0,
          worker_review_count: 0,
        },
      ], total: 2, page: 1, per_page: 3 }),
    }));

    await page.goto('/', { waitUntil: 'domcontentloaded' });
    const cards = page.locator('.lp-feed-card');
    await expect(cards).toHaveCount(2);
    await expect(cards.nth(0)).toContainText('Review history unavailable');
    await expect(cards.nth(0)).not.toContainText('New listing');
    await expect(cards.nth(0)).not.toContainText('Flexible');
    await expect(cards.nth(0)).not.toContainText('By Professional');
    await expect(cards.nth(1)).toContainText('New listing');
    await expect(cards.nth(1)).not.toContainText('Same-day delivery');
    await expect(cards.nth(1)).not.toContainText('0 days');
  });

  test('simplified homepage presents one broad buyer path without repeated modules', async ({ page }) => {
    await setupDeterministicLocalPage(page);
    await page.goto('/', { waitUntil: 'domcontentloaded' });
    await page.waitForLoadState('networkidle', { timeout: 5000 }).catch(() => {});
    await expect(page.locator('main[data-simplified-home="true"]')).toBeVisible();
    await expect(page.locator('h1')).toContainText('Describe the work. Hire the right human.');
    await expect(page.locator('[data-home-section]')).toHaveCount(5);
    await expect(page.locator('.lp-start-card')).toHaveCount(4);
    expect(await page.locator('.lp-feed-card').count()).toBeLessThanOrEqual(3);
    await expect(page.locator('body')).not.toContainText('Four ways to start small.');
    await expect(page.locator('body')).not.toContainText('Guided first-task wizard');
    await expect(page.locator('a,button').filter({ hasText: 'Describe your task' }).first()).toBeVisible();
    const describeCard = page.getByRole('button', { name: /Describe a task/ });
    await describeCard.click();
    await expect(page.locator('#guided-task-need')).toBeFocused();
    expect(new URL(page.url()).hash).toBe('');
    const previewCard = page.locator('.lp-feed-card').first();
    await expect(previewCard).toHaveAttribute('href', /#\/services\//);
    await previewCard.focus();
    await expect(previewCard).toBeFocused();
  });

  test('simplified public shell uses broad marketplace labels and a compact mobile footer', async ({ page, isMobile }) => {
    await setupDeterministicLocalPage(page);
    await page.goto('/pricing.html', { waitUntil: 'domcontentloaded' });
    const labels = await page.locator('.lp-nav-links .lp-nav-link').allTextContents();
    expect(labels.map(label => label.trim())).toEqual(['Marketplace', 'Find Work', 'For Agents', 'Pricing', 'Trust']);
    await expect(page.locator('.lp-nav-actions .btn-primary')).toContainText('Post a task');
    if (isMobile) {
      await page.locator('.lp-hamburger').click();
      const mobileLabels = (await page.locator('#mobileMenu a').allTextContents()).map(label => label.trim());
      expect(mobileLabels).toEqual(['Marketplace', 'Find Work', 'For Agents', 'Pricing', 'Trust', 'Starter Offers', 'Use Cases', 'FAQ', 'Sign in', 'Post a task']);
      const footer = page.locator('.lp-footer').first();
      const height = await footer.evaluate(el => el.getBoundingClientRect().height);
      expect(height).toBeLessThanOrEqual(700);
      expect(await footer.locator('a').count()).toBeLessThanOrEqual(13);
    }
  });

  test('mobile marketplace shows inventory before optional filters and truthful zero-review cards', async ({ page, isMobile }) => {
    test.skip(!isMobile, 'mobile information hierarchy regression');
    await setupDeterministicLocalPage(page);
    await page.route('https://gohirehumans-production.up.railway.app/services?**', route => route.fulfill({
      status: 200,
      contentType: 'application/json',
      body: JSON.stringify({ services: [
        { id: 'svc-new', title: 'New service', description: 'No reviews yet.', pricing_type: 'fixed', price: 50, worker_rating: 0, worker_review_count: 0, provider_type: 'human', worker_name: 'New provider', delivery_time_days: 2 },
        { id: 'svc-reviewed', title: 'Reviewed service', description: 'Has verified review history.', pricing_type: 'fixed', price: 75, worker_rating: 4.8, worker_review_count: 8, provider_type: 'human', worker_name: 'Reviewed provider', delivery_time_days: 3 },
        { id: 'svc-unknown', title: 'Unknown review history', description: 'Review facts unavailable.', pricing_type: 'fixed', price: 65, worker_rating: null, worker_review_count: null, provider_type: 'human', worker_name: 'Unverified provider', delivery_time_days: 4 }
      ], total: 3 })
    }));
    await page.goto('/#/services', { waitUntil: 'domcontentloaded' });
    await page.waitForLoadState('networkidle', { timeout: 5000 }).catch(() => {});
    const filters = page.locator('[data-service-filters]').first();
    await expect(page.locator('[data-filter-toggle]')).toBeVisible();
    await expect(filters).toBeHidden();
    const firstCard = page.locator('.svc-card').first();
    await expect(firstCard).toBeVisible();
    const top = await firstCard.evaluate(el => el.getBoundingClientRect().top);
    expect(top).toBeLessThan(760);
    await expect(firstCard).toContainText('View details');
    await expect(firstCard).toHaveAttribute('href', /#\/services\/svc-new/);
    await firstCard.focus();
    await expect(firstCard).toBeFocused();
    await expect(firstCard.locator('.stars-row')).toHaveCount(0);
    await expect(firstCard).toContainText('New listing');
    const reviewedCard = page.locator('.svc-card').nth(1);
    await expect(reviewedCard.locator('.stars-row')).toBeVisible();
    await expect(reviewedCard).not.toContainText('New listing');
    const unknownCard = page.locator('.svc-card').nth(2);
    await expect(unknownCard.locator('.stars-row')).toHaveCount(0);
    await expect(unknownCard).toContainText('Review history unavailable');
    await expect(unknownCard).not.toContainText('New listing');
    const sellerCta = page.locator('[data-seller-cta]').first();
    await expect(sellerCta).toBeVisible();
    const sellerTop = await sellerCta.evaluate(el => el.getBoundingClientRect().top);
    expect(sellerTop).toBeGreaterThan(top);
  });

  test('service detail uses canonical provider, review, and delivery facts', async ({ page }) => {
    await setupDeterministicLocalPage(page);
    await page.route('https://gohirehumans-production.up.railway.app/services/91', route => route.fulfill({
      status: 200,
      contentType: 'application/json',
      body: JSON.stringify({
        id: 91,
        title: 'Evidence review',
        description: 'Review one bounded artifact.',
        category: 'expert_review',
        pricing_type: 'fixed',
        price: 99,
        worker_id: 14,
        worker_name: 'Early provider',
        provider_type: null,
        worker_rating: null,
        worker_review_count: null,
        delivery_time_days: null
      })
    }));
    await page.route('https://gohirehumans-production.up.railway.app/users/14/reviews', route => route.fulfill({
      status: 200,
      contentType: 'application/json',
      body: JSON.stringify({ reviews: [] })
    }));
    await page.goto('/#/services/91', { waitUntil: 'domcontentloaded' });
    await expect(page.getByRole('heading', { name: 'Evidence review' })).toBeVisible();
    await expect(page.locator('.badge-human, .badge-ai')).toHaveCount(0);
    await expect(page.locator('.stars-row')).toHaveCount(0);
    await expect(page.locator('.svc-worker-row')).toContainText('Review history unavailable');
    await expect(page.locator('.svc-worker-row')).not.toContainText('New listing');
    await expect(page.locator('.svc-order-meta')).not.toContainText('Delivery:');
    await expect(page.getByText('About the Provider', { exact: true })).toBeVisible();
    await expect(page.getByText('About the Seller', { exact: true })).toHaveCount(0);
    await expect(page.locator('body')).not.toContainText('? days');

    await page.route('https://gohirehumans-production.up.railway.app/services/92', route => route.fulfill({
      status: 200,
      contentType: 'application/json',
      body: JSON.stringify({
        id: 92,
        title: 'Legacy zero-day verification',
        description: 'Legacy invalid delivery metadata.',
        category: 'expert_review',
        pricing_type: 'fixed',
        price: 125,
        worker_id: 15,
        worker_name: 'Known provider',
        provider_type: 'human',
        worker_rating: 5,
        worker_review_count: 1,
        delivery_time_days: 0
      })
    }));
    await page.route('https://gohirehumans-production.up.railway.app/users/15/reviews', route => route.fulfill({
      status: 200,
      contentType: 'application/json',
      body: JSON.stringify({ reviews: [] })
    }));
    await page.goto('/#/services/92', { waitUntil: 'domcontentloaded' });
    await expect(page.getByRole('heading', { name: 'Legacy zero-day verification' })).toBeVisible();
    await expect(page.locator('.badge-human')).toContainText('Human service');
    await expect(page.locator('.stars-row')).toHaveAttribute('aria-label', '5.0 out of 5 stars, 1 review');
    await expect(page.locator('.svc-order-meta')).not.toContainText('Delivery:');
    await expect(page.getByText('About the Seller', { exact: true })).toBeVisible();
    await expect(page.locator('body')).not.toContainText('Same-day delivery');
    await expect(page.locator('body')).not.toContainText('0 days');

    await page.route('https://gohirehumans-production.up.railway.app/services/93', route => route.fulfill({
      status: 200,
      contentType: 'application/json',
      body: JSON.stringify({
        id: 93,
        title: 'Legacy unknown provider',
        description: 'Legacy provider metadata must fail closed.',
        category: 'expert_review',
        pricing_type: 'fixed',
        price: 80,
        worker_id: 16,
        worker_name: 'Legacy provider',
        provider_type: 'unknown',
        worker_rating: 0,
        worker_review_count: 0,
        delivery_time_days: 2
      })
    }));
    await page.route('https://gohirehumans-production.up.railway.app/users/16/reviews', route => route.fulfill({
      status: 200,
      contentType: 'application/json',
      body: JSON.stringify({ reviews: [] })
    }));
    await page.goto('/#/services/93', { waitUntil: 'domcontentloaded' });
    await expect(page.getByRole('heading', { name: 'Legacy unknown provider' })).toBeVisible();
    await expect(page.locator('.badge-human, .badge-ai')).toHaveCount(0);
    await expect(page.getByText('About the Provider', { exact: true })).toBeVisible();
  });

  test('My Services preserves unknown versus zero review facts and omits invalid delivery', async ({ page }) => {
    await page.addInitScript(() => {
      sessionStorage.setItem('ghh_token', 'browser-my-services-token');
      localStorage.setItem('ghh_user', JSON.stringify({
        id: 42,
        name: 'Service Owner',
        email: 'owner@example.test'
      }));
    });
    await page.route('https://accounts.google.com/**', route => route.fulfill({ status: 204, body: '' }));
    await page.route('https://gohirehumans-production.up.railway.app/**', route => {
      const url = new URL(route.request().url());
      if (url.pathname === '/services') {
        return route.fulfill({
          status: 200,
          contentType: 'application/json',
          body: JSON.stringify({ services: [
            {
              id: 941,
              worker_id: 42,
              title: 'Unknown review history',
              category: 'testing',
              pricing_type: 'fixed',
              price: 99,
              delivery_time_days: 0,
              worker_rating: null,
              worker_review_count: null,
              status: 'active'
            },
            {
              id: 942,
              worker_id: 42,
              title: 'Known new listing',
              category: 'testing',
              pricing_type: 'fixed',
              price: 79,
              delivery_time_days: 1,
              worker_rating: null,
              worker_review_count: 0,
              status: 'active'
            },
            {
              id: 943,
              worker_id: 99,
              title: 'Another owner listing',
              category: 'testing',
              pricing_type: 'fixed',
              price: 50,
              delivery_time_days: 2,
              worker_rating: 5,
              worker_review_count: 2,
              status: 'active'
            }
          ] })
        });
      }
      return route.fulfill({ status: 200, contentType: 'application/json', body: '{}' });
    });

    await page.goto('/#/my-services');
    await expect(page.getByRole('heading', { name: 'My Services' })).toBeVisible();
    const unknown = page.locator('.task-card').filter({ hasText: 'Unknown review history' });
    await expect(unknown).toContainText('Review history unavailable');
    await expect(unknown).not.toContainText('New listing');
    await expect(unknown).not.toContainText('delivery');
    const knownZero = page.locator('.task-card').filter({ hasText: 'Known new listing' });
    await expect(knownZero).toContainText('New listing');
    await expect(knownZero).toContainText('Delivery: 1 day');
    await expect(page.getByText('Another owner listing')).toHaveCount(0);

    await page.evaluate(() => renderPostService());
    await expect(page.getByRole('heading', { name: 'Post a Service' })).toBeVisible();
    await page.locator('.ai-service-toggle summary').click();
    await page.locator('#providerType').selectOption('ai');
    await page.locator('#fulfillmentType').selectOption('api');
    await expect(page.locator('#apiEndpoint')).toHaveAttribute('required', '');
    await page.locator('#fulfillmentType').selectOption('manual');
    await expect(page.locator('#apiEndpoint')).not.toHaveAttribute('required', '');
  });

  test('empty jobs route has one truthful state and no contradictory inventory claims', async ({ page }) => {
    await setupDeterministicLocalPage(page);
    await page.route('https://gohirehumans-production.up.railway.app/jobs**', route => route.fulfill({
      status: 200,
      contentType: 'application/json',
      body: JSON.stringify({ jobs: [], total: 0 })
    }));
    await page.goto('/#/jobs', { waitUntil: 'domcontentloaded' });
    await page.waitForLoadState('networkidle', { timeout: 5000 }).catch(() => {});
    await expect(page.locator('#jobs-empty-state')).toBeVisible();
    await expect(page.locator('#jobs-empty-state')).toContainText('No public jobs right now');
    await expect(page.locator('[data-job-filters]')).toBeHidden();
    await expect(page.locator('body')).not.toContainText('New paid jobs');
    await expect(page.getByText('View open jobs', { exact: true })).toHaveCount(0);
  });

  test('reviewing jobs remain discoverable and applicable while applications are accepted', async ({ page }) => {
    await setupDeterministicLocalPage(page);
    const reviewingJob = {
      id: 42,
      employer_id: 8,
      employer_name: 'GoHireHumans Operations',
      title: 'Review a live workflow',
      description: 'Return screenshots and a prioritized issue list.',
      category: 'testing',
      status: 'reviewing',
      budget_type: 'fixed',
      budget_amount: 35,
      location_type: 'remote',
      created_at: '2026-07-21T00:00:00Z',
      application_count: 1
    };
    await page.route('https://gohirehumans-production.up.railway.app/jobs**', route => {
      const url = new URL(route.request().url());
      if (url.pathname.endsWith('/jobs/42')) {
        return route.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify(reviewingJob) });
      }
      if (url.pathname.endsWith('/jobs/43')) {
        return route.fulfill({
          status: 200,
          contentType: 'application/json',
          body: JSON.stringify({ ...reviewingJob, id: 43, title: 'Already hired workflow', status: 'hired' })
        });
      }
      return route.fulfill({
        status: 200,
        contentType: 'application/json',
        body: JSON.stringify({ jobs: [reviewingJob], total: 1 })
      });
    });

    await page.goto('/#/jobs', { waitUntil: 'domcontentloaded' });
    await expect(page.getByText('Review a live workflow')).toBeVisible();
    await expect(page.locator('#jobs-summary')).toHaveText('1 job accepting applications · newest first');
    await page.getByRole('button', { name: 'Apply now' }).click();
    await expect(page).toHaveURL(/#\/jobs\/42$/);
    await expect(page.getByRole('button', { name: 'Sign in to Apply' })).toBeVisible();
    await expect(page.locator('body')).not.toContainText('This job is no longer accepting applications.');

    await page.goto('/#/jobs/43', { waitUntil: 'domcontentloaded' });
    await expect(page.getByText('Already hired workflow')).toBeVisible();
    await expect(page.getByText('This job is no longer accepting applications.')).toBeVisible();
    await expect(page.getByRole('button', { name: /Apply/ })).toHaveCount(0);
  });

  test('filtered-empty marketplace states preserve truthful recovery paths', async ({ page }) => {
    await setupDeterministicLocalPage(page);
    await page.route('https://gohirehumans-production.up.railway.app/jobs**', route => route.fulfill({
      status: 200,
      contentType: 'application/json',
      body: JSON.stringify({ jobs: [], total: 0 })
    }));
    await page.goto('/#/jobs?search=no-match', { waitUntil: 'domcontentloaded' });
    await expect(page.getByRole('heading', { name: 'No jobs match these filters' })).toBeVisible();
    await expect(page.locator('[data-job-filters]')).toBeVisible();
    await expect(page.locator('.jobs-filtered-empty-state').getByRole('button', { name: 'Clear filters' })).toBeVisible();
    await expect(page.locator('body')).not.toContainText('No public jobs right now');

    await page.route('https://gohirehumans-production.up.railway.app/services?**', route => route.fulfill({
      status: 200,
      contentType: 'application/json',
      body: JSON.stringify({ services: [], total: 0 })
    }));
    await page.goto('/#/services?category=writing', { waitUntil: 'domcontentloaded' });
    await expect(page.locator('#services-result-count')).toHaveText('0 services match the current filters');
    await expect(page.getByRole('heading', { name: 'No services match these filters' })).toBeVisible();
    await expect(page.locator('.services-filtered-empty-state').getByRole('button', { name: 'Clear filters' })).toBeVisible();
    await expect(page.locator('#service-seller-cta')).toBeHidden();
    await expect(page.locator('body')).not.toContainText('Be the first to list a service');
  });

  test('out-of-range jobs pagination recovers to the last available page', async ({ page }) => {
    await setupDeterministicLocalPage(page);
    await page.route('https://gohirehumans-production.up.railway.app/jobs**', route => {
      const pageNumber = new URL(route.request().url()).searchParams.get('page');
      const jobs = pageNumber === '3' ? [] : [{ id: 16, title: 'Recovered job', category: 'research', status: 'open', created_at: '2026-07-20T00:00:00Z' }];
      return route.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify({ jobs, total: 16 }) });
    });
    await page.goto('/#/jobs?page=3', { waitUntil: 'domcontentloaded' });
    await expect(page).toHaveURL(/#\/jobs\?page=2$/);
    await expect(page.getByText('Recovered job')).toBeVisible();

    await page.goto('/#/jobs?search=qa&page=3', { waitUntil: 'domcontentloaded' });
    await expect(page).toHaveURL(/#\/jobs\?search=qa&page=2$/);
    await expect(page.getByText('Recovered job')).toBeVisible();
    await expect(page.locator('body')).not.toContainText('No jobs match these filters');
  });

  test('out-of-range filtered services pagination recovers before showing an empty state', async ({ page }) => {
    await setupDeterministicLocalPage(page);
    await page.route('https://gohirehumans-production.up.railway.app/services?**', route => {
      const pageNumber = new URL(route.request().url()).searchParams.get('page');
      const services = pageNumber === '3' ? [] : [{
        id: 16,
        title: 'Recovered service',
        description: 'Recovered from the last available filtered page.',
        price: 40,
        category: 'writing',
        worker_name: 'Recovery worker',
        delivery_time_days: 1,
        review_count: 0
      }];
      return route.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify({ services, total: 16 }) });
    });
    await page.goto('/#/services?category=writing&page=3', { waitUntil: 'domcontentloaded' });
    await expect(page).toHaveURL(/#\/services\?category=writing&page=2$/);
    await expect(page.getByText('Recovered service')).toBeVisible();
    await expect(page.locator('body')).not.toContainText('No services match these filters');
  });

  test('jobs API failure replaces the loading summary with an honest error state', async ({ page }) => {
    await setupDeterministicLocalPage(page);
    await page.route('https://gohirehumans-production.up.railway.app/jobs**', route => route.fulfill({
      status: 503,
      contentType: 'application/json',
      body: JSON.stringify({ error: 'unavailable' })
    }));
    await page.goto('/#/jobs', { waitUntil: 'domcontentloaded' });
    await expect(page.locator('#jobs-summary')).toHaveText('Jobs are temporarily unavailable');
    await expect(page.locator('#jobs-summary')).not.toContainText('Loading');
    await expect(page.getByRole('heading', { name: 'Something went wrong' })).toBeVisible();
  });

  test('starter-offer draft clicks stay diagnostic until a persisted lead exists', async ({ page }) => {
    await setupDeterministicLocalPage(page);
    await page.goto('/starter-offers.html', { waitUntil: 'domcontentloaded' });
    const events = await page.evaluate(() => {
      const names = [];
      window.trackGHH = name => names.push(name);
      document.querySelector('.starter-offer-card .offer-link').onclick(new MouseEvent('click', { bubbles: true, cancelable: true }));
      return names;
    });
    expect(events).toContain('starter_offer_draft_click');
    expect(events).not.toContain('generate_lead');
    expect(events).not.toContain('qualify_lead');
  });

  test('pricing, starter offers, and trust use the approved simplified hierarchy', async ({ page }) => {
    await setupDeterministicLocalPage(page);

    await page.goto('/pricing.html', { waitUntil: 'domcontentloaded' });
    await expect(page.locator('main[data-pricing-order="fee-first"]')).toBeVisible();
    const feeTop = await page.locator('#fee-heading').evaluate(el => el.getBoundingClientRect().top);
    const starterTop = await page.locator('#starter-packages-heading').evaluate(el => el.getBoundingClientRect().top);
    expect(feeTop).toBeLessThan(starterTop);
    await expect(page.locator('main')).toContainText('Workers receive the listed payout');
    await expect(page.locator('main')).toContainText('Stripe processing plus a 1% GoHireHumans fee');

    await page.goto('/starter-offers.html', { waitUntil: 'domcontentloaded' });
    await expect(page.locator('main[data-starter-simplified="true"]')).toBeVisible();
    await expect(page.locator('.starter-offer-card')).toHaveCount(4);
    await expect(page.locator('body')).not.toContainText('Choose by the risk you need checked');
    for (const value of ['$99', '$199', '$79']) await expect(page.locator('main')).toContainText(value);

    await page.goto('/trust-safety.html', { waitUntil: 'domcontentloaded' });
    await expect(page.locator('main[data-trust-simplified="true"]')).toBeVisible();
    expect(await page.locator('.trust-summary .trust-badge').count()).toBeLessThanOrEqual(4);
    await expect(page.locator('body')).not.toContainText('✓ Privacy protected');
    await expect(page.locator('main')).toContainText('where available');
    await expect(page.locator('main')).toContainText('available evidence');
  });
});
