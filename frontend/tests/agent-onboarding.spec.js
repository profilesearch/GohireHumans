// Offline portal contracts: PORTAL_NODE_TEST=1 node --test tests/agent-onboarding.spec.js
// Without that flag, Playwright runs browser fixtures for CI. No real API traffic.
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const assert = require('node:assert/strict');
const html = fs.readFileSync(path.join(__dirname, '../agent-onboarding.html'), 'utf8');
const script = [...html.matchAll(/<script>([\s\S]*?)<\/script>/g)].map(m => m[1]).find(s => s.includes('const API_BASE'));
const settle = () => new Promise(resolve => setImmediate(resolve));
function storage(values = {}, blocked = false) {
  const data = { ...values };
  return { data, getItem(k) { if (blocked) throw Error('Storage denied'); return data[k] || null; }, setItem(k, v) { if (blocked) throw Error('Storage denied'); data[k] = String(v); }, removeItem(k) { if (blocked) throw Error('Storage denied'); delete data[k]; } };
}
function element() {
  let text = '', inner = '';
  const classes = new Set();
  return { style: {}, value: '', children: [], listeners: {},
    classList: { add: c => classes.add(c), remove: c => classes.delete(c), contains: c => classes.has(c), toggle(c, on) { if (on) classes.add(c); else classes.delete(c); } },
    get textContent() { return text; }, set textContent(v) { text = String(v); inner = text.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;'); },
    get innerHTML() { return inner; }, set innerHTML(v) { inner = v; this.children = []; },
    appendChild(e) { this.children.push(e); }, addEventListener(t, fn) { this.listeners[t] = fn; }, focus() {}, querySelector() { return element(); }
  };
}
function boot({ session = {}, local = {}, blockedSession = false, blockedLocal = false, handler } = {}) {
  const nodes = new Map([...html.matchAll(/id="([^"]+)"/g)].map(m => [m[1], element()]));
  const scopeMarkup = html.match(/id="scopeGrid">([\s\S]*?)<\/div>/)[1];
  const scopes = [...scopeMarkup.matchAll(/<input[^>]*value="([^"]+)"([^>]*)>/g)].map(m => {
    const cb = element(), label = element(); cb.value = m[1]; cb.checked = m[2].includes('checked'); cb.closest = () => label; label.querySelector = () => cb; cb.label = label; return cb;
  });
  const listeners = {}, requests = [], errors = [];
  const document = {
    getElementById(id) { assert.ok(nodes.has(id), `Unknown DOM id ${id}`); return nodes.get(id); },
    createElement: element,
    addEventListener(t, fn) { listeners[t] = fn; },
    querySelectorAll(selector) {
      if (selector === '#scopeGrid input:checked') return scopes.filter(c => c.checked);
      if (selector === '#scopeGrid input') return scopes;
      if (selector === '#scopeGrid .scope-item') return scopes.map(c => c.label);
      if (selector.startsWith('.modal-overlay')) return [];
      throw Error(`Unsupported selector ${selector}`);
    }, querySelector() { return null; }
  };
  const sessionStorage = storage(session, blockedSession), localStorage = storage(local, blockedLocal);
  const context = { document, sessionStorage, localStorage, window: {}, console: { error: (...e) => errors.push(e) }, setTimeout() {}, navigator: { clipboard: { writeText: async () => {} } }, fetch: async (url, options = {}) => {
    const request = { path: new URL(url).pathname, ...options }; requests.push(request);
    const result = handler ? handler(request) : (request.path === '/api-keys' ? { api_keys: [] } : { summary: { total_requests: 0 }, usage: [], period_days: 30 });
    assert.ok(result, `Unmocked request denied: ${url}`);
    return { ok: !result.status || result.status < 400, status: result.status || 200, json: async () => result };
  } };
  // No network-capable globals are provided: fetch can only dispatch to local fixtures.
  vm.runInNewContext(script, context, { filename: 'agent-onboarding.html' });
  listeners.DOMContentLoaded();
  return { ...context.window, nodes, scopes, sessionStorage, localStorage, requests, errors, listeners };
}
if (process.env.PORTAL_NODE_TEST) {
  const { test } = require('node:test');
  test('portal recognizes the main app session token', async () => {
    const app = boot({ session: { ghh_token: 'fixture-session' } }); await settle();
    assert.equal(app.nodes.get('loggedInContent').style.display, 'block');
    assert.equal(app.requests.length, 2);
    assert.ok(app.requests.every(r => r.headers.Authorization === 'Bearer fixture-session'));
  });
  test('legacy ghh_token migrates to session storage, never generic token', async () => {
    const app = boot({ local: { ghh_token: 'fixture-legacy', token: 'unrelated-token' } }); await settle();
    assert.equal(app.sessionStorage.data.ghh_token, 'fixture-legacy');
    assert.equal(app.localStorage.data.ghh_token, undefined);
    assert.ok(app.requests.every(r => r.headers.Authorization === 'Bearer fixture-legacy'));
    const unrelated = boot({ local: { token: 'unrelated-token' } }); await settle();
    assert.equal(unrelated.requests.length, 0);
  });
  test('current session wins over a stale legacy token', async () => {
    const app = boot({ session: { ghh_token: 'fixture-current' }, local: { ghh_token: 'fixture-stale' } }); await settle();
    assert.equal(app.sessionStorage.data.ghh_token, 'fixture-current');
    assert.equal(app.localStorage.data.ghh_token, undefined);
    assert.ok(app.requests.length > 0);
    assert.ok(app.requests.every(r => r.headers.Authorization === 'Bearer fixture-current'));
  });
  test('denied storage fails closed without crashing public docs', async () => {
    const app = boot({ blockedSession: true, blockedLocal: true }); await settle();
    assert.equal(app.nodes.get('authGate').style.display, 'block');
    assert.equal(app.requests.length, 0);
  });
  test('working session survives unavailable local storage', async () => {
    const app = boot({ session: { ghh_token: 'fixture-session' }, blockedLocal: true }); await settle();
    assert.equal(app.nodes.get('loggedInContent').style.display, 'block');
  });
  test('key creation sends supported read-default scopes and reveals only nested secret', async () => {
    let created = false;
    const app = boot({ session: { ghh_token: 'fixture-session' }, handler: r => {
      if (r.path === '/api-keys/usage') return { summary: {}, usage: [], period_days: 30 };
      if (r.method === 'POST') { created = true; return { api_key: { id: 7, key: 'ghh_FAKE_ONE_TIME_SECRET', key_prefix: 'ghh_FAKE_ONE', name: 'Fixture', scopes: ['read'] } }; }
      return { api_keys: created ? [{ id: 7, name: 'Fixture', scopes: '["read"]', is_active: 1, key_prefix: 'ghh_FAKE_ONE' }] : [] };
    } }); await settle();
    assert.deepEqual(app.scopes.map(c => [c.value, c.checked]), [['read', true], ['write', false]]);
    app.nodes.get('keyName').value = 'Fixture';
    await app.createKey(); await settle();
    assert.deepEqual(JSON.parse(app.requests.find(r => r.method === 'POST').body), { name: 'Fixture', scopes: ['read'] });
    assert.equal(app.nodes.get('keyRevealText').textContent, 'ghh_FAKE_ONE_TIME_SECRET');
    assert.ok(app.nodes.get('keyReveal').classList.contains('show'));
    assert.ok(!JSON.stringify([app.localStorage.data, app.sessionStorage.data]).includes('ghh_FAKE_ONE_TIME_SECRET'));
    assert.equal(app.nodes.get('keysBody').children.length, 1);
    app.dismissKeyReveal();
    assert.equal(app.nodes.get('keyRevealText').textContent, '');
    assert.equal(app.nodes.get('keyReveal').classList.contains('show'), false);
  });
  test('write requires opt-in and successful creation resets to read-only', async () => {
    const app = boot({ session: { ghh_token: 'fixture-session' }, handler: r => r.method === 'POST' ? { api_key: { key: 'ghh_FAKE' } } : r.path === '/api-keys' ? { api_keys: [] } : { summary: {}, usage: [], period_days: 30 } }); await settle();
    assert.ok(app.scopes.find(c => c.value === 'write'));
    app.scopes.find(c => c.value === 'write').checked = true;
    app.nodes.get('keyName').value = 'Writer'; await app.createKey(); await settle();
    assert.deepEqual(JSON.parse(app.requests.find(r => r.method === 'POST').body).scopes, ['read', 'write']);
    assert.deepEqual(app.scopes.filter(c => c.checked).map(c => c.value), ['read']);
  });
  test('list decodes JSON scopes and uses is_active for revoked status', async () => {
    const app = boot({ session: { ghh_token: 'fixture-session' }, handler: r => r.path === '/api-keys' ? { api_keys: [{ id: 1, name: 'Live', scopes: '["read"]', is_active: 1, key_prefix: 'ghh_live' }, { id: 2, name: 'Old', scopes: '["read","write"]', is_active: 0, key_prefix: 'ghh_old' }] } : { summary: {}, usage: [], period_days: 30 } }); await settle();
    assert.equal(app.nodes.get('keysTable').style.display, 'table');
    const rows = app.nodes.get('keysBody').children;
    assert.equal(rows.length, 2);
    assert.match(rows[0].innerHTML, /read/); assert.match(rows[0].innerHTML, /Revoke/);
    assert.match(rows[1].innerHTML, /read, write/); assert.match(rows[1].innerHTML, /revoked/); assert.doesNotMatch(rows[1].innerHTML, /<button/);
  });
  test('missing nested secret fails visibly instead of claiming success', async () => {
    const app = boot({ session: { ghh_token: 'fixture-session' }, handler: r => r.method === 'POST' ? { api_key: { id: 7 } } : r.path === '/api-keys' ? { api_keys: [] } : { summary: {}, usage: [], period_days: 30 } }); await settle();
    app.nodes.get('keyName').value = 'Fixture'; await app.createKey();
    assert.equal(app.nodes.get('keyRevealText').textContent, '');
    assert.match(app.nodes.get('toastText').textContent, /not returned/i);
  });
  test('usage consumes summary and grouped request_count rows without invented rate limits', async () => {
    const today = new Date().toISOString().slice(0, 10);
    const rows = [{ date: today, endpoint: '/services', request_count: 3 }, { date: today, endpoint: '/jobs', request_count: 5 }];
    const app = boot({ session: { ghh_token: 'fixture-session' }, handler: r => r.path === '/api-keys' ? { api_keys: [] } : { summary: { total_requests: 29 }, usage: rows, period_days: 31 } }); await settle();
    assert.equal(app.nodes.get('statToday').textContent, String(rows.reduce((n, row) => n + row.request_count, 0)));
    assert.equal(app.nodes.get('statWeek').textContent, app.nodes.get('statToday').textContent);
    assert.equal(app.nodes.get('statMonth').textContent, app.nodes.get('statToday').textContent);
    assert.equal(app.nodes.get('statTotal').textContent, '29');
    assert.match(app.nodes.get('rateLimitText').textContent, /not reported/i);
    assert.ok(app.nodes.get('dailyBars').children.some(bar => bar.innerHTML.includes('8 req')));
    assert.match(html, /id="statPeriod">Last 31 days/);
    assert.doesNotMatch(html, /all time/);
    assert.match(script, /api-keys\/usage\?days=31/);
  });
  test('usage error is unavailable, not fabricated zero activity', async () => {
    const app = boot({ session: { ghh_token: 'fixture-session' }, handler: r => r.path === '/api-keys' ? { api_keys: [] } : { status: 503 } }); await settle();
    for (const id of ['statToday', 'statWeek', 'statMonth', 'statTotal']) assert.equal(app.nodes.get(id).textContent, '—');
    assert.match(app.nodes.get('usageLastUpdated').textContent, /unavailable/i);
    assert.match(app.nodes.get('rateLimitText').textContent, /not reported/i);
  });
  test('revocation still requires explicit confirmation and cancellation clears pending id', async () => {
    const app = boot({ session: { ghh_token: 'fixture-session' }, handler: r => r.path === '/api-keys/revoke' ? { message: 'API key revoked' } : r.path === '/api-keys' ? { api_keys: [] } : { summary: {}, usage: [], period_days: 30 } }); await settle();
    await app.confirmRevoke(); app.openRevokeModal(7, 'Fixture'); app.closeRevokeModal(); await app.confirmRevoke();
    assert.equal(app.requests.filter(r => r.method === 'POST').length, 0);
    app.openRevokeModal(7, 'Fixture'); await app.confirmRevoke(); await settle();
    const posts = app.requests.filter(r => r.method === 'POST');
    assert.equal(posts.length, 1); assert.equal(posts[0].path, '/api-keys/revoke'); assert.deepEqual(JSON.parse(posts[0].body), { key_id: 7 });
  });
} else {
  const { test, expect } = require('@playwright/test');
  test('scoped key lifecycle uses real DOM with all requests intercepted', async ({ page }) => {
    const posts = []; let created = false, revoked = false;
    const key = { id: 7, name: "Owner's fixture", key_prefix: 'ghh_FAKE_ONE', scopes: '["read"]', is_active: 1 };
    await page.route('**/*', route => {
      const req = route.request(), pathname = new URL(req.url()).pathname;
      if (req.resourceType() === 'document') return route.fulfill({ contentType: 'text/html', body: html });
      if (pathname === '/api-keys' && req.method() === 'POST') {
        posts.push(req.postDataJSON()); created = true;
        return route.fulfill({ status: 201, json: { api_key: { ...key, key: 'ghh_FAKE_ONE_TIME_SECRET', scopes: ['read'] } } });
      }
      if (pathname === '/api-keys/revoke') {
        posts.push(req.postDataJSON()); revoked = true;
        return route.fulfill({ json: { message: 'API key revoked' } });
      }
      if (pathname === '/api-keys') return route.fulfill({ json: { api_keys: created ? [{ ...key, is_active: revoked ? 0 : 1 }] : [] } });
      if (pathname === '/api-keys/usage') return route.fulfill({ json: { summary: { total_requests: 0 }, usage: [], period_days: 30 } });
      return route.abort();
    });
    await page.addInitScript(() => { window.gtag = () => {}; sessionStorage.setItem('ghh_token', 'fixture-session'); });
    await page.goto('/agent-onboarding.html');
    await page.getByRole('button', { name: 'Generate New Key', exact: true }).click();
    await expect(page.locator('#scopeGrid input')).toHaveCount(2);
    await expect(page.locator('#scopeGrid input[value="read"]')).toBeChecked();
    await expect(page.locator('#scopeGrid input[value="write"]')).not.toBeChecked();
    // Native label clicks must toggle once, not double-toggle.
    await page.locator('#scopeGrid label').filter({ hasText: 'write' }).click();
    await expect(page.locator('#scopeGrid input[value="write"]')).toBeChecked();
    await page.locator('#keyName').fill("Owner's fixture"); await page.locator('#createKeyBtn').click();
    await expect(page.locator('#keyRevealText')).toHaveText('ghh_FAKE_ONE_TIME_SECRET');
    expect(posts[0]).toEqual({ name: "Owner's fixture", scopes: ['read', 'write'] });
    await expect(page.locator('#keysBody tr')).toHaveCount(1);
    expect(await page.evaluate(() => JSON.stringify([localStorage, sessionStorage]))).not.toContain('ghh_FAKE_ONE_TIME_SECRET');
    await page.getByRole('button', { name: 'Dismiss key' }).click();
    await expect(page.locator('#keyRevealText')).toHaveText('');
    await page.locator('#keysBody button').click();
    await expect(page.locator('#revokeKeyName')).toHaveText("Owner's fixture");
    await page.locator('#revokeModal').getByRole('button', { name: 'Cancel' }).click();
    expect(posts).toHaveLength(1);
    await page.locator('#keysBody button').click(); await page.locator('#revokeConfirmBtn').click();
    await expect(page.locator('#keysBody')).toContainText('revoked');
    await expect(page.locator('#keysBody button')).toHaveCount(0);
    expect(posts[1]).toEqual({ key_id: 7 });
    await page.reload(); await expect(page.locator('#keyRevealText')).toHaveText('');
  });
  test('portal session bootstrap is offline', async ({ page }) => {
    await page.route('**/*', route => {
      if (route.request().resourceType() === 'document') return route.fulfill({ contentType: 'text/html', body: html });
      const pathname = new URL(route.request().url()).pathname;
      if (pathname === '/api-keys') return route.fulfill({ json: { api_keys: [] } });
      if (pathname === '/api-keys/usage') return route.fulfill({ json: { summary: { total_requests: 0 }, usage: [], period_days: 30 } });
      return route.abort();
    });
    await page.addInitScript(() => { window.gtag = () => {}; sessionStorage.setItem('ghh_token', 'fixture-session'); });
    await page.goto('/agent-onboarding.html');
    await expect(page.locator('#loggedInContent')).toBeVisible();
    await expect(page.locator('#keysEmpty')).toBeVisible();
  });
}
