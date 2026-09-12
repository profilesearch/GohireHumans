const { test, expect } = require('@playwright/test');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');

for (const hostname of ['www.gohirehumans.com', 'gohirehumans.com']) {
  test(`missing browser payment key does not invent backend payment mode: ${hostname}`, () => {
    const warnings = [];
    const context = { window: {}, location: { hostname }, console: { warn: (...args) => warnings.push(args.join(' ')), error: () => {} } };
    vm.runInNewContext(fs.readFileSync(path.join(__dirname, '../config.js'), 'utf8'), context);
    expect(warnings).toHaveLength(1);
    expect(warnings[0]).not.toMatch(/simulat/i);
    expect(warnings[0]).toMatch(/backend/i);
    expect(context.window.GOHIREHUMANS_CONFIG_OK).toBe(true);
    expect(context.window.GOHIREHUMANS_PAYMENTS_LIVE).toBe(false);
  });
}
