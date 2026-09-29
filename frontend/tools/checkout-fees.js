// Mirrors backend/api_core.py buyer_charge_breakdown_cents: cents are exact,
// each 100/300 bps component is rounded half-up with a one-cent minimum.
window.ghhBuyerCharge = function (amount) {
  const raw = String(amount).trim();
  if (!/^(?:\d+|\d{1,3}(?:,\d{3})+)(?:\.\d{1,2})?$/.test(raw)) return null;
  const [whole, fraction = ''] = raw.replace(/,/g, '').split('.');
  const cents = Number(whole) * 100 + Number(fraction.padEnd(2, '0'));
  if (!Number.isSafeInteger(cents) || cents <= 0) return null;
  const fee = bps => Math.max(1, Math.floor((cents * bps + 5000) / 10000));
  const platformCents = fee(100), processingCents = fee(300);
  if (!Number.isSafeInteger(cents + platformCents + processingCents)) return null;
  return { baseCents: cents, platformCents, processingCents,
    feeCents: platformCents + processingCents,
    totalCents: cents + platformCents + processingCents };
};
