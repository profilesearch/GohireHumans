const { test, expect } = require('@playwright/test');
const AxeBuilder = require('@axe-core/playwright').default;

const pages = ['/404.html','/agent-onboarding.html','/api-docs.html','/faq.html','/how-it-works.html'];
for (const route of pages) {
  test(`customer help accessibility: ${route}`, async ({ page }) => {
    await page.route('**/*', request => {
      const url = new URL(request.request().url());
      if (url.hostname === '127.0.0.1' || url.hostname === 'localhost') return request.continue();
      return request.abort();
    });
    const errors=[];page.on('pageerror', error=>errors.push(error.message));
    await page.goto(route);
    await page.evaluate(async()=>Promise.all(document.getAnimations()
      .filter(animation=>animation.effect?.getTiming().iterations!==Infinity)
      .map(animation=>animation.finished.catch(()=>{}))));
    const results=await new AxeBuilder({page}).withTags(['wcag2a','wcag2aa','wcag21aa']).analyze();
    const serious=results.violations.filter(row=>['serious','critical'].includes(row.impact));
    expect(serious.map(row=>({id:row.id,nodes:row.nodes.map(node=>({target:node.target,reason:node.failureSummary}))}))).toEqual([]);
    expect(errors).toEqual([]);
    expect(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth+1)).toBe(true);
    await expect(page.locator('footer')).toHaveCount(1);
    await expect(page.locator('footer a[href="mailto:contact@gohirehumans.com"]')).toBeVisible();
    for(const href of ['/#/terms','/#/privacy','/#/guidelines']) await expect(page.locator(`footer a[href="${href}"]`)).toBeVisible();
    if(route==='/api-docs.html'){
      const scrollable=page.locator('pre,table,.code-block');
      for(let i=0;i<await scrollable.count();i++){
        const el=scrollable.nth(i);
        if(await el.evaluate(node=>node.scrollWidth>node.clientWidth+1)){
          await el.focus();await expect(el).toBeFocused();
        }
      }
    }
  });
}
