/* GoHireHumans product clips: short muted loops of real site flows.
 * Markup: <figure class="ghh-clip" data-clip="hire"></figure>  (data-clip: hire | earn)
 * - Shows a poster image first; video sources load only when the clip nears the viewport.
 * - Plays only while on screen and never autoplays when the visitor prefers reduced motion.
 * - A pause/play button keeps motion under the visitor's control (WCAG 2.2.2).
 * Re-render a clip with marketing/clips/record.cjs and bump ASSET_VERSION: /assets/ is cached immutably.
 */
(function () {
  'use strict';
  var ASSET_VERSION = 'v1';
  var CLIPS = {
    hire: {
      label: 'how hiring works',
      text: 'Animated walkthrough of hiring on GoHireHumans: describe a task, post it for free, review applicants and every fee before you confirm a hire, then approve the delivered work to release the payout.'
    },
    earn: {
      label: 'how earning works',
      text: 'Animated walkthrough of earning on GoHireHumans: set up payouts through Stripe for free, apply to a paid job, deliver the work, and get the listed payout after the buyer approves it.'
    }
  };
  var reduce = window.matchMedia ? window.matchMedia('(prefers-reduced-motion: reduce)') : { matches: false };
  var ICON = {
    play: '<svg viewBox="0 0 24 24" aria-hidden="true"><path d="M8 5v14l11-7z" fill="currentColor"/></svg>',
    pause: '<svg viewBox="0 0 24 24" aria-hidden="true"><path d="M7 5h4v14H7zM13 5h4v14h-4z" fill="currentColor"/></svg>'
  };

  function track(name, clip) {
    try { if (typeof window.gtag === 'function') window.gtag('event', name, { clip: clip }); } catch (e) {}
  }

  function el(tag, attrs) {
    var n = document.createElement(tag);
    for (var k in attrs) n.setAttribute(k, attrs[k]);
    return n;
  }

  function setup(fig) {
    var name = fig.getAttribute('data-clip');
    var meta = Object.prototype.hasOwnProperty.call(CLIPS, name) ? CLIPS[name] : null;
    if (!meta || fig.getAttribute('data-clip-ready')) return;
    fig.setAttribute('data-clip-ready', '1');
    var base = '/assets/clips/' + name + '-' + ASSET_VERSION;
    var video = null, userPaused = !!reduce.matches, viewed = false;

    var poster = el('img', { src: base + '.jpg', alt: '', width: '1280', height: '720', loading: 'lazy', decoding: 'async' });
    var btn = el('button', { type: 'button', 'class': 'ghh-clip-toggle' });
    var caption = el('figcaption', { 'class': 'visually-hidden' });
    caption.textContent = meta.text;
    fig.appendChild(poster);
    fig.appendChild(btn);
    fig.appendChild(caption);

    function render() {
      var playing = !!(video && !video.paused);
      btn.innerHTML = playing ? ICON.pause : ICON.play;
      btn.setAttribute('aria-label', (playing ? 'Pause' : 'Play') + ' animation: ' + meta.label);
      fig.classList.toggle('is-playing', playing);
    }

    function ensureVideo() {
      if (video) return video;
      video = el('video', { muted: '', playsinline: '', loop: '', preload: 'none', 'aria-hidden': 'true', tabindex: '-1', poster: base + '.jpg' });
      video.muted = true;
      video.defaultMuted = true;
      video.loop = true;
      video.playsInline = true;
      video.appendChild(el('source', { src: base + '.webm', type: 'video/webm; codecs="vp9"' }));
      video.appendChild(el('source', { src: base + '.mp4', type: 'video/mp4' }));
      video.addEventListener('play', render);
      video.addEventListener('pause', render);
      fig.insertBefore(video, btn);
      return video;
    }

    function play() {
      var pr = ensureVideo().play();
      if (pr && pr.catch) pr.catch(render);
      if (!viewed) { viewed = true; track('product_clip_view', name); }
    }

    btn.addEventListener('click', function () {
      if (video && !video.paused) {
        userPaused = true;
        video.pause();
      } else {
        userPaused = false;
        play();
      }
      render();
    });

    if ('IntersectionObserver' in window) {
      new IntersectionObserver(function (entries) {
        entries.forEach(function (e) {
          if (e.isIntersecting && !userPaused) play();
          else if (!e.isIntersecting && video && !video.paused) video.pause();
        });
      }, { threshold: 0.4 }).observe(fig);
    }
    render();
  }

  function init(root) {
    var figs = (root || document).querySelectorAll('.ghh-clip[data-clip]');
    for (var i = 0; i < figs.length; i++) setup(figs[i]);
  }
  window.initProductClips = init;
  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', function () { init(); });
  else init();
})();
