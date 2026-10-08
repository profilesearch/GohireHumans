const { test, expect } = require('@playwright/test');

// Product clips: muted loops on the homepage, How it works and the earn page.
// All non-local requests are blocked, so no analytics or fonts leave the machine.
let errors;
test.beforeEach(async ({ page }) => {
  errors = [];
  page.on('pageerror', e => errors.push(e.message));
  await page.addInitScript(() => {
    window.clipEvents = [];
    window.gtag = (...args) => window.clipEvents.push(args);
    window.GOHIREHUMANS_API_URL = '/__clips-test-api';
  });
  await page.route('**/*', route => {
    const url = new URL(route.request().url());
    if (!['127.0.0.1', 'localhost'].includes(url.hostname)) return route.abort();
    if (url.pathname === '/config.js') return route.fulfill({ contentType: 'application/javascript', body: '' });
    if (url.pathname.startsWith('/__clips-test-api')) return route.fulfill({ contentType: 'application/json', body: '{"services":[],"categories":[]}' });
    return route.continue();
  });
});
test.afterEach(() => expect(errors).toEqual([]));

const PAGES = [
  ['/', ['hire']],
  ['/how-it-works.html', ['hire', 'earn']],
  ['/earn/get-paid-for-human-tasks.html', ['earn']],
];

for (const [path, clips] of PAGES) {
  test(`clips render with poster, description and controls on ${path}`, async ({ page }) => {
    await page.goto(path);
    const figs = page.locator('figure.ghh-clip');
    await expect(figs).toHaveCount(clips.length);
    for (let i = 0; i < clips.length; i++) {
      const fig = figs.nth(i);
      await expect(fig).toHaveAttribute('data-clip', clips[i]);
      await expect(fig.locator('img')).toHaveAttribute('src', `/assets/clips/${clips[i]}-v1.jpg`);
      await expect(fig.locator('img')).toHaveAttribute('alt', '');
      await expect(fig.locator('figcaption')).toContainText('Animated walkthrough');
      await expect(fig.locator('button.ghh-clip-toggle')).toHaveAttribute('aria-label', /animation: how (hiring|earning) works/);
      const box = await fig.boundingBox();
      expect(Math.abs(box.width / box.height - 16 / 9)).toBeLessThan(0.02);
    }
    // Nothing overflows the viewport horizontally.
    expect(await page.evaluate(() => document.documentElement.scrollWidth <= window.innerWidth + 1)).toBe(true);
  });
}

test('clip video loads only near the viewport, plays muted, and the button pauses it', async ({ page }) => {
  await page.goto('/how-it-works.html');
  const fig = page.locator('figure.ghh-clip[data-clip="hire"]');
  // Below the fold: no video element and no video request yet.
  await expect(fig.locator('video')).toHaveCount(0);
  await fig.scrollIntoViewIfNeeded();
  const video = fig.locator('video');
  await expect(video).toHaveCount(1);
  expect(await video.evaluate(v => [v.muted, v.loop, v.playsInline, v.getAttribute('aria-hidden')])).toEqual([true, true, true, 'true']);
  expect(await video.locator('source').evaluateAll(s => s.map(x => x.getAttribute('src')))).toEqual(['/assets/clips/hire-v1.webm', '/assets/clips/hire-v1.mp4']);
  await expect.poll(() => video.evaluate(v => !v.paused)).toBe(true);
  const btn = fig.locator('button.ghh-clip-toggle');
  await expect(btn).toHaveAttribute('aria-label', 'Pause animation: how hiring works');
  await btn.click();
  await expect.poll(() => video.evaluate(v => v.paused)).toBe(true);
  await expect(btn).toHaveAttribute('aria-label', 'Play animation: how hiring works');
  // A user pause sticks when the clip scrolls away and back.
  await page.evaluate(() => window.scrollTo(0, document.body.scrollHeight));
  await fig.scrollIntoViewIfNeeded();
  await page.waitForTimeout(300);
  expect(await video.evaluate(v => v.paused)).toBe(true);
  const events = await page.evaluate(() => window.clipEvents.filter(a => a[0] === 'event').map(a => [a[1], a[2]]));
  // One view event per clip per page load, even after scrolling away and back.
  expect(events.filter(e => e[1].clip === 'hire')).toEqual([['product_clip_view', { clip: 'hire' }]]);
  expect(events.every(e => e[0] === 'product_clip_view')).toBe(true);
});

test('reduced motion: clips never autoplay but can be started by the visitor', async ({ browser }) => {
  const context = await browser.newContext({ reducedMotion: 'reduce' });
  const page = await context.newPage();
  page.on('pageerror', e => errors.push(e.message));
  await page.route('**/*', route => (new URL(route.request().url()).hostname === '127.0.0.1' ? route.continue() : route.abort()));
  await page.goto('/earn/get-paid-for-human-tasks.html');
  const fig = page.locator('figure.ghh-clip[data-clip="earn"]');
  await fig.scrollIntoViewIfNeeded();
  await page.waitForTimeout(400);
  await expect(fig.locator('video')).toHaveCount(0);
  await expect(fig.locator('img')).toBeVisible();
  const btn = fig.locator('button.ghh-clip-toggle');
  await expect(btn).toHaveAttribute('aria-label', 'Play animation: how earning works');
  await btn.click();
  await expect.poll(() => fig.locator('video').evaluate(v => !v.paused)).toBe(true);
  await context.close();
});

test('homepage clip survives SPA re-renders without duplicating controls', async ({ page }) => {
  await page.goto('/');
  await page.evaluate(() => { location.hash = '#/services'; });
  await page.evaluate(() => { location.hash = '#/'; });
  const fig = page.locator('figure.ghh-clip[data-clip="hire"]');
  await expect(fig).toHaveCount(1);
  await expect(fig.locator('button.ghh-clip-toggle')).toHaveCount(1);
  await expect(fig.locator('img')).toHaveCount(1);
});

test('unknown clip names are ignored', async ({ page }) => {
  await page.goto('/how-it-works.html');
  await page.evaluate(() => {
    const f = document.createElement('figure');
    f.className = 'ghh-clip';
    f.setAttribute('data-clip', '../../evil');
    f.id = 'bogus';
    document.body.appendChild(f);
    window.initProductClips();
  });
  await expect(page.locator('#bogus > *')).toHaveCount(0);
});
