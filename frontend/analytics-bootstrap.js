(function () {
  'use strict';

  var allowedOrigins = new Set([
    'https://www.gohirehumans.com',
    'https://gohirehumans.com'
  ]);
  if (!allowedOrigins.has(window.location.origin)) {
    window.gtag = window.gtag || function () {};
    return;
  }

  window.dataLayer = window.dataLayer || [];
  function normalizeEventParams(params) {
    if (!params || typeof params !== 'object' || Array.isArray(params)) return params;
    var normalized = Object.assign({}, params);
    ['source', 'medium', 'campaign'].forEach(function (key) {
      if (!Object.prototype.hasOwnProperty.call(normalized, key)) return;
      var internalKey = 'ui_' + key;
      if (!Object.prototype.hasOwnProperty.call(normalized, internalKey)) {
        normalized[internalKey] = normalized[key];
      }
      delete normalized[key];
    });
    return normalized;
  }
  window.gtag = window.gtag || function () {
    if (arguments[0] === 'event' && arguments.length > 2) {
      arguments[2] = normalizeEventParams(arguments[2]);
    }
    window.dataLayer.push(arguments);
  };

  var measurementId = 'G-KM69M3NES8';
  var script = document.createElement('script');
  script.async = true;
  script.src = 'https://www.googletagmanager.com/gtag/js?id=' + encodeURIComponent(measurementId);
  document.head.appendChild(script);
  window.gtag('js', new Date());

  // Coarse account class only (never an id, email or name), so staff and
  // AI-agent browsing can be separated from human visitors in reports. The app
  // calls this after sign-in; a cached signed-in user sets it on every page.
  var accountTypes = ['human', 'ai_agent', 'internal'];
  window.ghhSetAccountType = function (type) {
    if (accountTypes.indexOf(type) === -1) return;
    window.gtag('set', 'user_properties', { account_type: type });
  };
  try {
    var cachedUser = JSON.parse(window.localStorage.getItem('ghh_user') || 'null');
    if (cachedUser) window.ghhSetAccountType(cachedUser.account_type);
  } catch (e) {}
})();
