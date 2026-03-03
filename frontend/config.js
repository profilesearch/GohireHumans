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
window.STRIPE_PUBLISHABLE_KEY = ''; // Set to pk_test_... or pk_live_... for real Stripe Checkout
