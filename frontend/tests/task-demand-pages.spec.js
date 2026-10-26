const { test, expect } = require('@playwright/test');

// Every example card on the demand-led task pages must open the matching editable draft,
// for anonymous visitors (via the saved-draft sign-in screen) and signed-in employers,
// without publishing, charging, or sending analytics.
const pages = ['/hire/website-testers.html', '/hire/voice-over.html', '/hire/subtitles-captions.html', '/hire/short-video-creators.html'];
const categories = ['testing', 'voice_over', 'audio_production', 'video_editing', 'transcription', 'translation', 'media_capture', 'content_creation'];

async function isolate(page, signedIn) {
  const evidence = { mutations: [], errors: [], analytics: [] };
  page.on('pageerror', error => evidence.errors.push(error.message));
  await page.addInitScript(signedIn => {
    window.GOHIREHUMANS_API_URL = '/__task-page-api';
    if (signedIn) {
      sessionStorage.setItem('ghh_token', 'local-task-page-test-only');
      localStorage.setItem('ghh_user', JSON.stringify({ id: 601, name: 'Task Page Tester', role: 'employer' }));
    }
  }, signedIn);
  await page.route('**/*', route => {
    const request = route.request(), url = new URL(request.url());
    if (!['GET', 'HEAD'].includes(request.method())) { evidence.mutations.push(`${request.method()} ${url.pathname}`); return route.abort(); }
    // Let the local bootstrap define gtag, but abort every request to Google so nothing is sent.
    if (/google/.test(url.hostname)) { evidence.analytics.push(url.hostname); return route.abort(); }
    if (!['127.0.0.1', 'localhost'].includes(url.hostname)) return route.abort();
    if (url.pathname === '/config.js') return route.fulfill({ status: 200, contentType: 'application/javascript', body: '' });
    if (url.pathname.startsWith('/__task-page-api/')) {
      const body = url.pathname.endsWith('/categories') ? { categories } : {};
      return route.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify(body) });
    }
    return route.continue();
  });
  return evidence;
}

async function exampleCards(page, path) {
  await page.goto(path);
  return page.locator('a.card[href^="/#/post-job?draft_title="]').evaluateAll(cards => cards.map(card => {
    const query = new URLSearchParams(card.getAttribute('href').split('?')[1]);
    return {
      href: card.getAttribute('href'),
      title: card.querySelector('.card-list-title').textContent,
      desc: card.querySelector('.card-list-desc').textContent,
      draft: { title: query.get('draft_title'), description: query.get('draft_description'), category: query.get('draft_category'), skills: query.get('draft_skills') },
    };
  }));
}

for (const path of pages) {
  for (const signedIn of [true, false]) {
    test(`${path} example cards open editable drafts (${signedIn ? 'signed in' : 'anonymous'})`, async ({ page }) => {
      const evidence = await isolate(page, signedIn);
      const cards = await exampleCards(page, path);
      expect(cards.length).toBeGreaterThanOrEqual(4);
      for (const [index, card] of cards.entries()) {
        expect(card.draft.title).toBe(card.title);
        expect(card.draft.description).toBe(card.desc);
        expect(categories).toContain(card.draft.category);
        if (index > 0) {
          // Anonymous runs must start each card signed out again (the previous card saved a fixture session).
          if (!signedIn) await page.evaluate(() => { sessionStorage.clear(); localStorage.clear(); });
          await page.goto(path);
        }
        await page.locator(`a.card[href="${card.href}"]`).click();
        if (!signedIn) {
          await expect(page.locator('.auth2-title')).toHaveText('Your job draft is saved');
          await expect(page.locator('.auth2-sub')).toContainText('Nothing has been posted or charged.');
          const redirect = await page.evaluate(() => new URLSearchParams(location.hash.split('?')[1]).get('redirect'));
          expect(redirect).toMatch(/^post-job\?/);
          // Local fixture session only: no login, publishing, or payment request is made.
          await page.evaluate(() => saveSession('local-task-page-test-only', { id: 601, name: 'Task Page Tester', role: 'employer' }));
          await page.goto('/#/' + redirect);
        }
        await expect(page.locator('h1').first()).toHaveText('Post a Job');
        await expect(page.locator('input[name="title"]')).toHaveValue(card.draft.title);
        await expect(page.locator('textarea[name="description"]')).toHaveValue(card.draft.description);
        await expect(page.locator('select[name="category"]')).toHaveValue(card.draft.category);
        await expect(page.locator('select[name="category"]')).toBeEditable();
        await expect(page.getByRole('button', { name: 'Post Job', exact: true })).toBeVisible();
      }
      expect(evidence.errors).toEqual([]);
      expect(evidence.mutations).toEqual([]);
      expect(evidence.analytics.length).toBeGreaterThan(0);
    });
  }

  // The worker block sits after the buyer close; its listing link must reach the sign-in that
  // resumes Post a Service, and its guide link must reach the worker payout page. No writes.
  test(`${path} worker block links to listing and payout guide`, async ({ page }) => {
    const evidence = await isolate(page, false);
    await page.goto(path);
    const block = page.locator('#list-your-service');
    await expect(block.getByRole('heading', { level: 2 })).toHaveText('Do this kind of work? List your service');
    await expect(block).toContainText('Buyers can order it only after you finish Stripe payout setup');
    await block.getByRole('link', { name: 'List a service' }).click();
    // Account routes send anonymous visitors to sign-in, which resumes Post a Service afterwards.
    await expect(page.locator('.auth2-title')).toHaveText('Welcome back');
    expect(await page.evaluate(() => new URLSearchParams(location.hash.split('?')[1]).get('redirect'))).toBe('post-service');
    await page.goto(path);
    await page.locator('#list-your-service').getByRole('link', { name: 'How workers get paid' }).click();
    await expect(page).toHaveURL(/\/earn\/get-paid-for-human-tasks\.html$/);
    expect(evidence.errors).toEqual([]);
    expect(evidence.mutations).toEqual([]);
  });
}
