// GoHireHumans Frontend Configuration
// NOTE: This file is served publicly. Do not include secrets here.
// The API URL is intentionally visible — it's a public endpoint.

/**
 * GoHireHumans Frontend Configuration
 *
 * Update GOHIREHUMANS_API_URL to point to your Railway backend URL.
 * Example: "https://gohirehumans-api-production.up.railway.app"
 *
 * STRIPE_PUBLISHABLE_KEY is a public key — safe to include here.
 * Get it from https://dashboard.stripe.com/apikeys
 */

window.GOHIREHUMANS_API_URL = 'https://gohirehumans-production.up.railway.app';

// Optional static public key. Setup responses supply the authoritative key.
// An empty static key does not imply simulated backend payments.
window.STRIPE_PUBLISHABLE_KEY = '';

// ── Runtime sanity checks ──
// Surface configuration problems loudly in dev/staging without breaking prod.
window.GOHIREHUMANS_CONFIG_OK = (() => {
    const issues = [];
    if (!window.GOHIREHUMANS_API_URL) {
        issues.push('GOHIREHUMANS_API_URL is empty');
    } else if (window.GOHIREHUMANS_API_URL.endsWith('/')) {
        issues.push('GOHIREHUMANS_API_URL must not have a trailing slash');
    }

    if (issues.length) {
        console.error('[GoHireHumans] Config issues:', issues);
        return false;
    }
    return true;
})();

// Legacy display hint only, never payment authorization; null means unknown.
window.GOHIREHUMANS_PAYMENTS_LIVE = window.STRIPE_PUBLISHABLE_KEY
    ? /^pk_live_/.test(window.STRIPE_PUBLISHABLE_KEY) : null;
