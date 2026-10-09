/* GoHireHumans product clips: short muted loops of real site flows.
 * Markup: <figure class="ghh-clip" data-clip="hire"></figure>  (data-clip: hire | earn)
 * - Shows a poster image first; video sources load only when the clip nears the viewport.
 * - Plays only while on screen and never autoplays when the visitor prefers reduced motion
 *   (also when that preference changes while the page is open).
 * - A pause/play button keeps motion under the visitor's control (WCAG 2.2.2).
 * - Clips removed by SPA re-renders are disposed: unobserved, paused and their media released.
 * - Sends one product_clip_view (diagnostic) per clip name per page load, once playback starts.
 * Re-render a clip with marketing/clips/record.cjs and bump ASSET_VERSION: /assets/ is cached immutably.
 */
(function () {
  'use strict';
  var ASSET_VERSION = 'v3';
  var CLIPS = {
    hire: {
      label: 'how hiring works',
      text: 'Animated walkthrough of hiring on GoHireHumans: describe a task, post it for free, review applicants and every fee before you confirm a hire, then approve the delivered work to release the payout. Illustrative example.'
    },
    earn: {
      label: 'how earning works',
      text: 'Animated walkthrough of earning on GoHireHumans: set up payouts through Stripe for free, apply to a paid job, deliver the work, and get the listed payout after the buyer approves it. Illustrative example.'
    }
  };
  var reduce = window.matchMedia ? window.matchMedia('(prefers-reduced-motion: reduce)') : null;
  var ICON = {
    play: '<svg viewBox="0 0 24 24" aria-hidden="true"><path d="M8 5v14l11-7z" fill="currentColor"/></svg>',
    pause: '<svg viewBox="0 0 24 24" aria-hidden="true"><path d="M7 5h4v14H7zM13 5h4v14h-4z" fill="currentColor"/></svg>'
  };
  var live = [];    // active clip controllers
  var viewed = {};  // clip names that already sent product_clip_view this page load
  var observer = null;

  function reduced() { return !!(reduce && reduce.matches); }

  function el(tag, attrs) {
    var n = document.createElement(tag);
    for (var k in attrs) n.setAttribute(k, attrs[k]);
    return n;
  }

  function track(name) {
    if (viewed[name]) return;
    viewed[name] = true;
    try { if (typeof window.gtag === 'function') window.gtag('event', 'product_clip_view', { clip: name }); } catch (e) {}
  }

  function controllerFor(node) {
    for (var i = 0; i < live.length; i++) if (live[i].fig === node) return live[i];
    return null;
  }

  function dispose(c) {
    if (observer) observer.unobserve(c.fig);
    if (c.video) {
      c.video.pause();
      while (c.video.firstChild) c.video.removeChild(c.video.firstChild);
      c.video.removeAttribute('src');
      try { c.video.load(); } catch (e) {}
    }
    var i = live.indexOf(c);
    if (i >= 0) live.splice(i, 1);
  }

  function sweep() {
    for (var i = live.length - 1; i >= 0; i--) if (!live[i].fig.isConnected) dispose(live[i]);
  }

  function getObserver() {
    if (observer || !('IntersectionObserver' in window)) return observer;
    observer = new IntersectionObserver(function (entries) {
      entries.forEach(function (e) {
        var c = controllerFor(e.target);
        if (!c) return;
        if (!c.fig.isConnected) { dispose(c); return; }
        c.visible = e.isIntersecting;
        if (c.visible && c.mayAutoplay()) c.play();
        else if (!c.visible && c.video && !c.video.paused) c.video.pause();
      });
    }, { threshold: 0.4 });
    return observer;
  }

  function setup(fig) {
    var name = fig.getAttribute('data-clip');
    var meta = Object.prototype.hasOwnProperty.call(CLIPS, name) ? CLIPS[name] : null;
    if (!meta || fig.getAttribute('data-clip-ready')) return;
    fig.setAttribute('data-clip-ready', '1');
    var base = '/assets/clips/' + name + '-' + ASSET_VERSION;
    // choice: 'play' or 'pause' once the visitor uses the button; null follows the motion preference.
    var c = { fig: fig, name: name, video: null, visible: false, choice: null };

    var poster = el('img', { src: base + '.jpg', alt: '', width: '1280', height: '720', loading: 'lazy', decoding: 'async' });
    var btn = el('button', { type: 'button', 'class': 'ghh-clip-toggle' });
    var caption = el('figcaption', { 'class': 'visually-hidden' });
    caption.textContent = meta.text;
    fig.appendChild(poster);
    fig.appendChild(btn);
    fig.appendChild(caption);

    c.render = function () {
      var playing = !!(c.video && !c.video.paused);
      btn.innerHTML = playing ? ICON.pause : ICON.play;
      btn.setAttribute('aria-label', (playing ? 'Pause' : 'Play') + ' animation: ' + meta.label);
      fig.classList.toggle('is-playing', playing);
    };
    c.mayAutoplay = function () {
      return c.choice === 'play' || (c.choice !== 'pause' && !reduced());
    };
    c.ensureVideo = function () {
      if (c.video) return c.video;
      var v = el('video', { muted: '', playsinline: '', loop: '', preload: 'none', 'aria-hidden': 'true', tabindex: '-1', poster: base + '.jpg' });
      v.muted = true;
      v.defaultMuted = true;
      v.loop = true;
      v.playsInline = true;
      v.appendChild(el('source', { src: base + '.webm', type: 'video/webm; codecs="vp9"' }));
      v.appendChild(el('source', { src: base + '.mp4', type: 'video/mp4' }));
      v.addEventListener('play', c.render);
      v.addEventListener('pause', c.render);
      v.addEventListener('playing', function () { track(name); });
      fig.insertBefore(v, btn);
      c.video = v;
      return v;
    };
    c.play = function () {
      var pr = c.ensureVideo().play();
      if (pr && pr.catch) pr.catch(c.render);
    };

    btn.addEventListener('click', function () {
      if (c.video && !c.video.paused) {
        c.choice = 'pause';
        c.video.pause();
      } else {
        c.choice = 'play';
        c.play();
      }
      c.render();
    });

    live.push(c);
    var io = getObserver();
    if (io) io.observe(fig);
    c.render();
  }

  function onMotionPreference() {
    sweep();
    live.forEach(function (c) {
      if (!c.mayAutoplay()) { if (c.video && !c.video.paused) c.video.pause(); }
      else if (c.visible) c.play();
    });
  }

  function init(root) {
    sweep();
    var figs = (root || document).querySelectorAll('.ghh-clip[data-clip]');
    for (var i = 0; i < figs.length; i++) setup(figs[i]);
  }

  if (reduce) {
    if (reduce.addEventListener) reduce.addEventListener('change', onMotionPreference);
    else if (reduce.addListener) reduce.addListener(onMotionPreference);
  }
  // SPA routes replace the page body; release clips that left the document.
  window.addEventListener('hashchange', function () { setTimeout(sweep, 0); setTimeout(sweep, 1500); });
  window.initProductClips = init;
  // Read-only count of clips still holding resources (used by tests; never sweeps).
  window.productClipsActive = function () { return live.length; };
  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', function () { init(); });
  else init();
})();
