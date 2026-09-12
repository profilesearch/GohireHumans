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

// Optional browser Stripe key: pk_test_... is test mode; pk_live_... is live mode.
// An empty browser key does not establish the backend's payment mode.
// Checkout availability and funding safeguards are enforced by the backend.
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
    if (!window.STRIPE_PUBLISHABLE_KEY) {
        if (location.hostname === 'www.gohirehumans.com' || location.hostname === 'gohirehumans.com') {
            console.warn('[GoHireHumans] Browser Stripe key is not configured. Check backend payment readiness before checkout.');
        }
    }
    if (issues.length) {
        console.error('[GoHireHumans] Config issues:', issues);
        return false;
    }
    return true;
})();

window.GOHIREHUMANS_PAYMENTS_LIVE = !!window.STRIPE_PUBLISHABLE_KEY &&
    /^pk_(live|test)_/.test(window.STRIPE_PUBLISHABLE_KEY);
