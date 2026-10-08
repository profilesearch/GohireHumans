const { test, expect } = require('@playwright/test');

// Product clips: muted loops on the homepage, How it works and the earn page.
// All non-local requests are blocked, so no analytics or fonts leave the machine.
const V = 'v2';
let errors;
let media;
test.beforeEach(async ({ page }) => {
  errors = [];
  media = [];
  page.on('pageerror', e => errors.push(e.message));
  page.on('request', r => { if (/\/assets\/clips\/.*\.(webm|mp4)$/.test(new URL(r.url()).pathname)) media.push(new URL(r.url()).pathname); });
  await page.addInitScript(() => {
    window.clipEvents = [];
    window.gtag = (...args) => window.clipEvents.push(args);
    window.GOHIREHUMANS_API_URL = '/__clips-test-api';
  });
  await page.route('**/*', route => {
    const url = new URL(route.request().url());
    if (!['127.0.0.1', 'localhost'].includes(url.hostname)) return route.abort();
    if (url.pathname === '/config.js') return route.fulfill({ contentType: 'application/javascript', body: '' });
    if (url.pathname.startsWith('/__clips-test-api')) return route.fulfill({ contentType: 'application/json', body: '{"services":[],"categories":[],"jobs":[]}' });
    return route.continue();
  });
});
test.afterEach(() => expect(errors).toEqual([]));

const clipEvents = page => page.evaluate(() => window.clipEvents.filter(a => a[0] === 'event').map(a => [a[1], a[2]]));
const advancing = async video => {
  const t0 = await video.evaluate(v => v.currentTime);
  await expect.poll(() => video.evaluate(v => v.currentTime), { timeout: 5000 }).toBeGreaterThan(t0 + 0.2);
};

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
      await expect(fig.locator('img')).toHaveAttribute('src', `/assets/clips/${clips[i]}-${V}.jpg`);
      await expect(fig.locator('img')).toHaveAttribute('alt', '');
      await expect(fig.locator('figcaption')).toContainText('Animated walkthrough');
      await expect(fig.locator('figcaption')).toContainText('Illustrative example.');
      await expect(fig.locator('button.ghh-clip-toggle')).toHaveAttribute('aria-label', /animation: how (hiring|earning) works/);
      const box = await fig.boundingBox();
      expect(Math.abs(box.width / box.height - 16 / 9)).toBeLessThan(0.02);
    }
    // Every referenced asset exists.
    for (const name of clips) {
      for (const ext of ['jpg', 'webm', 'mp4']) {
        expect((await page.request.get(`/assets/clips/${name}-${V}.${ext}`)).status()).toBe(200);
      }
    }
    expect(await page.evaluate(() => document.documentElement.scrollWidth <= window.innerWidth + 1)).toBe(true);
  });
}

test('earn clip copy says the payout follows buyer approval', async ({ page }) => {
  await page.goto('/earn/get-paid-for-human-tasks.html');
  await expect(page.locator('figure.ghh-clip[data-clip="earn"] figcaption')).toContainText('after the buyer approves it');
});

test('clip video loads only near the viewport, plays, and the button pauses it', async ({ page }) => {
  await page.goto('/how-it-works.html');
  await page.waitForTimeout(500);
  const fig = page.locator('figure.ghh-clip[data-clip="hire"]');
  // Below the fold: no video element and no media request yet.
  await expect(fig.locator('video')).toHaveCount(0);
  expect(media).toEqual([]);
  await fig.scrollIntoViewIfNeeded();
  const video = fig.locator('video');
  await expect(video).toHaveCount(1);
  expect(await video.evaluate(v => [v.muted, v.loop, v.playsInline, v.getAttribute('aria-hidden')])).toEqual([true, true, true, 'true']);
  expect(await video.locator('source').evaluateAll(s => s.map(x => x.getAttribute('src')))).toEqual([`/assets/clips/hire-${V}.webm`, `/assets/clips/hire-${V}.mp4`]);
  await advancing(video);
  expect(media.length).toBeGreaterThan(0);
  expect(media.every(p => p.startsWith('/assets/clips/hire-'))).toBe(true);
  const btn = fig.locator('button.ghh-clip-toggle');
  await expect(btn).toHaveAttribute('aria-label', 'Pause animation: how hiring works');
  await btn.click();
  await expect.poll(() => video.evaluate(v => v.paused)).toBe(true);
  await expect(btn).toHaveAttribute('aria-label', 'Play animation: how hiring works');
  // A user pause sticks when the clip scrolls away and back.
  await page.evaluate(() => window.scrollTo(0, document.body.scrollHeight));
  await page.waitForTimeout(300);
  await fig.scrollIntoViewIfNeeded();
  await page.waitForTimeout(400);
  expect(await video.evaluate(v => v.paused)).toBe(true);
  const events = await clipEvents(page);
  expect(events.filter(e => e[1].clip === 'hire')).toEqual([['product_clip_view', { clip: 'hire' }]]);
  expect(events.every(e => e[0] === 'product_clip_view' && Object.keys(e[1]).join() === 'clip')).toBe(true);
});

test('reduced motion: clips never autoplay but can be started by the visitor', async ({ page }) => {
  await page.emulateMedia({ reducedMotion: 'reduce' });
  await page.goto('/earn/get-paid-for-human-tasks.html');
  const fig = page.locator('figure.ghh-clip[data-clip="earn"]');
  await fig.scrollIntoViewIfNeeded();
  await page.waitForTimeout(500);
  await expect(fig.locator('video')).toHaveCount(0);
  expect(media).toEqual([]);
  expect(await clipEvents(page)).toEqual([]);
  await expect(fig.locator('img')).toBeVisible();
  const btn = fig.locator('button.ghh-clip-toggle');
  await expect(btn).toHaveAttribute('aria-label', 'Play animation: how earning works');
  await btn.click();
  await advancing(fig.locator('video'));
});

test('turning on reduced motion while a clip plays pauses it; turning it off resumes', async ({ page }) => {
  await page.goto('/earn/get-paid-for-human-tasks.html');
  const fig = page.locator('figure.ghh-clip[data-clip="earn"]');
  await fig.scrollIntoViewIfNeeded();
  const video = fig.locator('video');
  await advancing(video);
  await page.emulateMedia({ reducedMotion: 'reduce' });
  await expect.poll(() => video.evaluate(v => v.paused)).toBe(true);
  await page.emulateMedia({ reducedMotion: 'no-preference' });
  await expect.poll(() => video.evaluate(v => !v.paused)).toBe(true);
});

test('homepage clip is disposed on SPA navigation and recreated once on return', async ({ page }) => {
  await page.goto('/');
  const fig = page.locator('figure.ghh-clip[data-clip="hire"]');
  await fig.scrollIntoViewIfNeeded();
  await advancing(fig.locator('video'));
  await page.evaluate(() => { window.__firstClip = document.querySelector('figure.ghh-clip'); window.__firstVideo = window.__firstClip.querySelector('video'); });
  expect(await page.evaluate(() => window.productClipsActive())).toBe(1);

  await page.evaluate(() => { location.hash = '#/services'; });
  await expect(page.locator('figure.ghh-clip')).toHaveCount(0);
  // Automatic disposal releases the detached clip's media (productClipsActive is read-only).
  await expect.poll(() => page.evaluate(() => [window.__firstClip.isConnected, window.__firstVideo.paused, window.__firstVideo.querySelectorAll('source').length])).toEqual([false, true, 0]);
  expect(await page.evaluate(() => window.productClipsActive())).toBe(0);

  await page.evaluate(() => { location.hash = '#/'; });
  await expect(fig).toHaveCount(1);
  expect(await page.evaluate(() => document.querySelector('figure.ghh-clip') !== window.__firstClip)).toBe(true);
  await expect(fig.locator('button.ghh-clip-toggle')).toHaveCount(1);
  await expect(fig.locator('img')).toHaveCount(1);
  expect(await page.evaluate(() => window.productClipsActive())).toBe(1);
  await fig.scrollIntoViewIfNeeded();
  await advancing(fig.locator('video'));
  // Still one view event for the page load.
  expect((await clipEvents(page)).filter(e => e[1].clip === 'hire')).toHaveLength(1);
});

test('no view event when playback fails', async ({ page }) => {
  await page.addInitScript(() => {
    HTMLMediaElement.prototype.play = function () { return Promise.reject(new DOMException('blocked', 'NotAllowedError')); };
  });
  await page.goto('/how-it-works.html');
  const fig = page.locator('figure.ghh-clip[data-clip="hire"]');
  await fig.scrollIntoViewIfNeeded();
  await expect(fig.locator('video')).toHaveCount(1);
  await page.waitForTimeout(600);
  expect(await clipEvents(page)).toEqual([]);
  await expect(fig.locator('button.ghh-clip-toggle')).toHaveAttribute('aria-label', 'Play animation: how hiring works');
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
