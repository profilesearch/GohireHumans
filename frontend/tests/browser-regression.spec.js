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
