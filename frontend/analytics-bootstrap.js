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
  window.gtag = window.gtag || function () {
    window.dataLayer.push(arguments);
  };

  var measurementId = 'G-KM69M3NES8';
  var script = document.createElement('script');
  script.async = true;
  script.src = 'https://www.googletagmanager.com/gtag/js?id=' + encodeURIComponent(measurementId);
  document.head.appendChild(script);
  window.gtag('js', new Date());
})();
