const { defineConfig, devices } = require('@playwright/test');
// PW_PORT lets local runs avoid a port already used by another project; CI keeps 4173.
const PORT = process.env.PW_PORT || '4173';
const ORIGIN = `http://127.0.0.1:${PORT}`;
module.exports = defineConfig({
  testDir: './tests', timeout: 45_000, expect: { timeout: 8_000 },
  use: { baseURL: ORIGIN, trace: 'retain-on-failure', screenshot: 'only-on-failure' },
  webServer: { command: `python3 -m http.server ${PORT}`, url: ORIGIN, reuseExistingServer: true, timeout: 20_000 },
  projects: [
    { name: 'chromium-desktop', use: { ...devices['Desktop Chrome'] } },
    { name: 'chromium-mobile', use: { ...devices['Pixel 5'] } }
  ]
});
