const { test, expect } = require('@playwright/test');
const { AxeBuilder } = require('@axe-core/playwright');
const routes = [
  { path: '/', mustContain: 'Human verification for AI work.' },
  { path: '/starter-offers.html', mustContain: 'Human verification for AI work before you trust it.' },
  { path: '/pricing.html', mustContain: 'Start with proof, then scale.' },
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
    await expect(page.locator('body')).toContainText('Choose the fastest proof-backed check.');
    await expect(page.locator('body')).toContainText('Start a QA sprint');
    await page.goto('/pricing.html', { waitUntil: 'domcontentloaded' });
    await expect(page.locator('body')).toContainText('Start with proof, then scale.');
    await expect(page.locator('a[href="/use-cases/hire-human-to-review-ai-output.html"]').first()).toBeVisible();
    await expect(page.locator('a[href="/use-cases/lead-research-microtask.html"]').first()).toBeVisible();
    await expect(page.locator('body')).toContainText('Choose a proof-first starter task');
    await page.goto('/starter-offers.html', { waitUntil: 'domcontentloaded' });
    await expect(page.locator('body')).toContainText('Choose by the risk you need checked');
  });

  test('public nav hover and active states are consistent across static and SPA pages', async ({ page, isMobile }) => {
    test.skip(isMobile, 'desktop nav hover states are hidden behind the mobile menu');
    await setupDeterministicLocalPage(page);
    const cases = [
      { path: '/', active: null },
      { path: '/starter-offers.html', active: 'Starter QA' },
      { path: '/pricing.html', active: 'Pricing' },
      { path: '/trust-safety.html', active: 'Trust' },
      { path: '/ai-integration.html', active: 'For Agents' },
      { path: '/earn/get-paid-for-human-tasks.html', active: 'For Workers' },
      { path: '/use-cases/hire-human-to-review-ai-output.html', active: 'Marketplace' },
      { path: '/#/services', active: 'Marketplace' },
      { path: '/#/jobs', active: 'For Workers' },
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
      await expect(page.locator('.lp-footer').first(), `${path} worker jobs label`).toContainText('Open Jobs for Workers');
      await expect(page.locator('body'), `${path} no legacy builder attribution`).not.toContainText('Created with Perplexity Computer');
    }
    await page.goto('/stats.html', { waitUntil: 'domcontentloaded' });
    await expect(page.locator('a.btn-primary[href="/#/register"]')).toContainText('Create a free account');
    await page.goto('/trust-safety.html', { waitUntil: 'domcontentloaded' });
    await expect(page.locator('#trust-next-step-heading')).toContainText('Start with a scoped review');
    await expect(page.locator('.trust-next-step a[href="/starter-offers.html"]')).toContainText('Request QA');
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
    await expect(page.locator('body')).toContainText('Compare Fees');
    await page.goto('/#/services', { waitUntil: 'domcontentloaded' });
    await page.waitForLoadState('networkidle', { timeout: 5000 }).catch(() => {});
    await expect(page.locator('#services-result-count')).toBeVisible();
    await expect(page.locator('text=Filter services')).toBeVisible();
  });
});
