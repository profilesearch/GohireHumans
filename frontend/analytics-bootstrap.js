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
})();
