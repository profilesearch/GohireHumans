// Mirrors backend/api_core.py buyer_charge_breakdown_cents: 100/300 bps,
// half-up component rounding, minimum one cent for each positive component.
window.ghhBuyerCharge = function (amount) {
  const cents = Math.round(Number(amount) * 100);
  if (!Number.isSafeInteger(cents) || cents <= 0) return null;
  const fee = bps => Math.max(1, Math.floor((cents * bps + 5000) / 10000));
  const platformCents = fee(100), processingCents = fee(300);
  return { baseCents: cents, platformCents, processingCents,
    feeCents: platformCents + processingCents,
    totalCents: cents + platformCents + processingCents };
};
