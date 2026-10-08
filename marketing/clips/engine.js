/* Deterministic clip engine: renderAt(t) sets every animated property as a pure
   function of time, so the recorder can capture frame-exact video. */
(function () {
  const clamp = (x, a = 0, b = 1) => Math.min(b, Math.max(a, x));
  const ease = {
    linear: x => x,
    outCubic: x => 1 - Math.pow(1 - x, 3),
    inCubic: x => x * x * x,
    inOutCubic: x => (x < 0.5 ? 4 * x * x * x : 1 - Math.pow(-2 * x + 2, 3) / 2),
    outExpo: x => (x === 1 ? 1 : 1 - Math.pow(2, -10 * x)),
    inOutQuint: x => (x < 0.5 ? 16 * x ** 5 : 1 - Math.pow(-2 * x + 2, 5) / 2),
    outBack: x => { const c1 = 1.5, c3 = c1 + 1; return 1 + c3 * Math.pow(x - 1, 3) + c1 * Math.pow(x - 1, 2); },
  };
  const p = (t, t0, t1, e = 'linear') => (t1 <= t0 ? (t >= t1 ? 1 : 0) : ease[e](clamp((t - t0) / (t1 - t0))));
  const lerp = (a, b, k) => a + (b - a) * k;
  const $ = s => document.querySelector(s);

  function splitWords(el) {
    if (el.dataset.split) return;
    const parts = el.textContent.split(/(\s+)/).filter(Boolean);
    el.textContent = '';
    for (const part of parts) {
      if (/^\s+$/.test(part)) { el.appendChild(document.createTextNode(' ')); continue; }
      const s = document.createElement('span');
      s.className = 'w';
      s.textContent = part;
      el.appendChild(s);
    }
    el.dataset.split = '1';
  }

  // Kinetic phrase: words rise out of a blur, staggered; exit lifts and blurs together.
  function phrase(el, t, tIn, tOut, opt = {}) {
    const stagger = opt.stagger ?? 0.065, dur = opt.dur ?? 0.5, outDur = opt.outDur ?? 0.28;
    const split = el.querySelectorAll('.w');
    const words = split.length ? split : [el];
    const visible = t >= tIn - 0.01 && (tOut == null || t <= tOut + outDur + 0.01);
    el.style.visibility = visible ? 'visible' : 'hidden';
    if (!visible) return;
    const out = tOut == null ? 0 : p(t, tOut, tOut + outDur, 'inCubic');
    words.forEach((w, i) => {
      const k = p(t, tIn + i * stagger, tIn + i * stagger + dur, 'outExpo');
      const y = lerp(0.42, 0, k) - out * 0.3;
      const blur = lerp(14, 0, k) + out * 12;
      w.style.opacity = String(k * (1 - out));
      w.style.transform = `translateY(${y}em)`;
      w.style.filter = blur > 0.05 ? `blur(${blur.toFixed(2)}px)` : 'none';
    });
  }

  // UI panel: rises in with a slight scale and blur, keeps a slow push-in while on screen.
  function panel(el, t, tIn, tOut, opt = {}) {
    const inDur = opt.inDur ?? 0.38, outDur = opt.outDur ?? 0.3;
    const visible = t >= tIn - 0.01 && (tOut == null || t <= tOut + outDur + 0.01);
    el.style.visibility = visible ? 'visible' : 'hidden';
    if (!visible) { el.style.opacity = '0'; return; }
    const k = p(t, tIn, tIn + inDur, 'outExpo');
    const out = tOut == null ? 0 : p(t, tOut, tOut + outDur, 'inCubic');
    const life = tOut == null ? (opt.life ?? 3) : (tOut + outDur - tIn);
    const drift = p(t, tIn, tIn + life, 'linear');
    const s = lerp(0.94, 1, k) * (1 + 0.025 * drift) * (1 - 0.03 * out);
    const y = lerp(46, 0, k) - out * 36;
    const blur = lerp(10, 0, k) + out * 10;
    el.style.opacity = String(k * (1 - out));
    el.style.transform = `translate(${opt.x || 0}px, ${y}px) scale(${s})`;
    el.style.filter = blur > 0.05 ? `blur(${blur.toFixed(2)}px)` : 'none';
  }

  // Appear: simple rise for rows/cards inside a panel.
  function appear(el, t, tIn, dur = 0.35, dist = 18) {
    const k = p(t, tIn, tIn + dur, 'outCubic');
    el.style.opacity = String(k);
    el.style.transform = `translateY(${lerp(dist, 0, k)}px)`;
  }

  function typeText(el, text, t, t0, t1, opt = {}) {
    const n = Math.round(p(t, t0, t1, 'linear') * text.length);
    const shown = text.slice(0, n);
    const caretOn = opt.caret && t >= t0 - 0.25 && t <= (opt.caretUntil ?? t1 + 0.4) && (t < t0 || t > t1 ? Math.floor(t * 2.2) % 2 === 0 : true);
    if (!n && opt.placeholder) {
      el.innerHTML = '';
      if (caretOn) el.appendChild(Object.assign(document.createElement('span'), { className: 'caret' }));
      el.appendChild(Object.assign(document.createElement('span'), { className: 'ph', textContent: opt.placeholder }));
      return;
    }
    el.textContent = shown;
    if (caretOn) el.appendChild(Object.assign(document.createElement('span'), { className: 'caret' }));
  }

  function targetPoint(spec) {
    if (spec.x != null) return { x: spec.x, y: spec.y };
    const r = $(spec.sel).getBoundingClientRect();
    return { x: r.left + r.width * (spec.ax ?? 0.5), y: r.top + r.height * (spec.ay ?? 0.55) };
  }

  // Resolve every cursor target once, measured at the moment its move ends,
  // so the pointer path never jumps when panels transition.
  function calibrateCursor(cfg, render) {
    window.__clipCalibrating = true;
    for (const seg of cfg.moves) {
      if (seg.to.sel == null) continue;
      render(seg.t1 + 0.02);
      seg.to = targetPoint(seg.to);
    }
    window.__clipCalibrating = false;
  }

  // Cursor: moves along segments [{t0,t1,to}] and clicks at [{t, sel}].
  function cursor(t, cfg) {
    if (window.__clipCalibrating) return;
    const el = $('#cursor'), rip = $('#ripple');
    const shown = t >= cfg.show && t <= cfg.hide + 0.25;
    el.style.visibility = shown ? 'visible' : 'hidden';
    if (!shown) { rip.style.opacity = '0'; return; }
    let pos = targetPoint(cfg.start);
    let prev = cfg.start;
    for (const seg of cfg.moves) {
      if (t < seg.t0) break;
      const a = targetPoint(prev), b = targetPoint(seg.to);
      const k = p(t, seg.t0, seg.t1, 'inOutCubic');
      // Gentle arc so moves feel hand-made rather than linear.
      const arc = Math.sin(Math.PI * k) * (seg.arc ?? -28);
      pos = { x: lerp(a.x, b.x, k), y: lerp(a.y, b.y, k) + arc };
      prev = seg.to;
    }
    let scale = 1;
    rip.style.opacity = '0';
    for (const c of cfg.clicks) {
      const d = t - c.t;
      if (d >= -0.06 && d <= 0.16) scale = 0.84;
      if (d >= 0 && d <= 0.5) {
        const k = p(t, c.t, c.t + 0.5, 'outCubic');
        rip.style.left = pos.x + 'px';
        rip.style.top = pos.y + 'px';
        rip.style.opacity = String(0.85 * (1 - k));
        rip.style.transform = `scale(${lerp(0.3, 1.6, k)})`;
      }
      if (c.sel) document.querySelector(c.sel)?.classList.toggle('is-pressed', d >= -0.04 && d <= 0.16);
    }
    const fade = p(t, cfg.show, cfg.show + 0.25) * (1 - p(t, cfg.hide, cfg.hide + 0.25));
    el.style.opacity = String(fade);
    el.style.transform = `translate(${pos.x - 9}px, ${pos.y - 5}px) scale(${scale})`;
  }

  function toast(el, t, tIn, tOut) {
    const k = p(t, tIn, tIn + 0.3, 'outBack');
    const out = tOut == null ? 0 : p(t, tOut, tOut + 0.25, 'inCubic');
    el.style.visibility = t >= tIn ? 'visible' : 'hidden';
    el.style.opacity = String(Math.min(1, k) * (1 - out));
    el.style.transform = `translateY(${lerp(22, 0, k)}px)`;
  }

  // Split frame: ink panel slides in from the left, later expands to fill the frame for the end card.
  function frame(t, cfg) {
    const intro = $('#intro'), left = $('#left'), right = $('#right'), outro = $('#outro');
    const io = p(t, cfg.introOut, cfg.introOut + 0.4, 'inCubic');
    intro.style.visibility = io < 1 ? 'visible' : 'hidden';
    intro.style.opacity = String(1 - io);
    intro.style.transform = `scale(${1 - 0.05 * io})`;
    intro.style.filter = io > 0.01 ? `blur(${(io * 12).toFixed(2)}px)` : 'none';
    const li = p(t, cfg.splitIn, cfg.splitIn + 0.5, 'inOutQuint');
    const grow = p(t, cfg.outroIn, cfg.outroIn + 0.5, 'inOutQuint');
    left.style.transform = `translateX(${lerp(-780, 0, li)}px)`;
    left.style.width = lerp(760, 1920, grow) + 'px';
    const rk = p(t, cfg.splitIn + 0.1, cfg.splitIn + 0.55, 'outCubic');
    right.style.opacity = String(rk);
    right.style.transform = `translateX(${lerp(120, 0, rk) + grow * 240}px)`;
    const lc = 1 - p(t, cfg.outroIn - 0.05, cfg.outroIn + 0.2, 'inCubic');
    const lcEl = left.querySelector('.lc'); if (lcEl) lcEl.style.opacity = String(lc);
    outro.style.visibility = t >= cfg.outroIn + 0.3 ? 'visible' : 'hidden';
  }

  function stepper(t, steps) {
    const segs = document.querySelectorAll('#left .stepper div');
    steps.forEach((s, i) => {
      const seg = segs[i];
      seg.classList.toggle('on', t >= s[0]);
      seg.querySelector('i').style.width = (p(t, s[0], s[1]) * 100).toFixed(2) + '%';
    });
  }

  window.Clip = { clamp, ease, p, lerp, $, splitWords, phrase, panel, appear, typeText, cursor, calibrateCursor, toast, frame, stepper };
})();
