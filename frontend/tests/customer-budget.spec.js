const { test, expect } = require('@playwright/test');

const cases = [
  ['$1,000', '1000'], ['$20–$50 fixed', ''], ['20/hr', ''],
  ['25', '25'], ['25.50', '25.50'], ['', ''], ['   ', ''],
  ['-25', ''], ['$-25', ''], ['1,00', ''], ['12,34,567', ''],
  ['25.500', ''], ['25 dollars please', ''], ['25abc', ''],
  ['USD 1,234.50', '1234.50'], ['25.50 USD', '25.50'],
  ['$25.50', '25.50'], ['1.000,50', ''], ['25 50', '']
];
for (const unavailable of [false, true]) {
  for (const [budget, amount] of cases) {
    test(`budget ${JSON.stringify(budget)} survives anonymous handoff (${unavailable ? 'storage unavailable' : 'stored'})`, async ({ page }) => {
      const mutations = [], errors = [];
      page.on('pageerror', error => errors.push(error.message));
      await page.addInitScript(unavailable => {
        window.GOHIREHUMANS_API_URL = '/__budget-test-api';
        // Analytics transport stays blocked; satisfy the unrelated inline initializer.
        window.gtag = () => {};
        if (unavailable) {
          const original = Storage.prototype.setItem;
          Storage.prototype.setItem = function(key, value) {
            if (this === sessionStorage && key === 'ghh_guided_task_draft') throw new DOMException('Unavailable', 'QuotaExceededError');
            return original.call(this, key, value);
          };
        }
      }, unavailable);
      await page.route('**/*', route => {
        const request = route.request(), url = new URL(request.url());
        if (!['GET', 'HEAD'].includes(request.method())) {
          mutations.push(`${request.method()} ${request.url()}`);
          return route.abort();
        }
        if (!['127.0.0.1', 'localhost'].includes(url.hostname) || /analytics|stripe|paypal/i.test(url.pathname)) return route.abort();
        if (url.pathname === '/config.js') return route.fulfill({ contentType: 'application/javascript', body: '' });
        if (url.pathname.startsWith('/__budget-test-api')) return route.fulfill({ contentType: 'application/json', body: JSON.stringify(url.pathname.endsWith('/categories') ? { categories: ['other'] } : {}) });
        return route.continue();
      });
      await page.goto('/');
      await page.locator('#guided-task-need').fill('Review the synthetic sample');
      // input.value assignment also covers malformed pasted/autofilled text literally.
      await page.locator('#guided-task-budget').evaluate((element, value) => { element.value = value; }, budget);
      const enteredBudget = await page.locator('#guided-task-budget').inputValue();
      await page.evaluate(() => createGuidedTaskDraft());
      await expect(page.locator('.auth2-title')).toHaveText('Your job draft is saved');
      const redirect = await page.evaluate(() => new URLSearchParams(location.hash.split('?')[1]).get('redirect'));
      expect(redirect).toMatch(/^post-job\?/);
      const params = new URLSearchParams(redirect.split('?')[1]);
      expect(params.get('draft_budget')).toBe(amount || null);
      const description = params.get('draft_description');
      if (enteredBudget.trim()) expect(description).toContain(`Budget preference (review before publishing):\n${enteredBudget.trim()}`);
      else expect(description).not.toContain('Budget preference');
      const stored = await page.evaluate(() => JSON.parse(sessionStorage.getItem('ghh_guided_task_draft')));
      if (unavailable) expect(stored).toBeNull();
      else expect(stored).toMatchObject({ description, budget_amount: amount, budget_type: 'fixed' });
      // Synthetic local state only; never submit authentication, listings or payments.
      await page.evaluate(() => saveSession('budget-test-only', { id: 501, name: 'Budget Tester', role: 'employer' }));
      await page.goto('/#/' + redirect);
      await expect(page.locator('textarea[name="description"]')).toHaveValue(description);
      await expect(page.locator('input[name="budget_amount"]')).toHaveValue(amount);
      await expect(page.locator('input[name="budget_amount"]')).toBeEditable();
      expect(errors).toEqual([]);
      expect(mutations).toEqual([]);
    });
  }
}
