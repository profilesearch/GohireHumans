#!/usr/bin/env node
/*
 * Records a deterministic clip page (marketing/clips/clip-*.html) to video.
 *
 *   node marketing/clips/record.cjs <clip-name> <out-dir> [--fps 30] [--version v1] [--preview t1,t2,...]
 *
 * The page must define window.CLIP_DURATION, window.CLIP_POSTER_T, window.CLIP_INIT() and window.renderAt(t).
 * Each frame is rendered by calling renderAt(i / fps) and screenshotting, so output is
 * frame-exact regardless of machine speed. Needs Playwright (frontend/node_modules) and
 * an ffmpeg with libx264 + libvpx-vp9 (set FFMPEG=/path/to/ffmpeg).
 *
 * Outputs in <out-dir> (<v> = --version, default v1; bump it on every re-render because
 * /assets/ is served with a one-year immutable cache):
 *   <name>-<v>-1080p.mp4  master, 1920x1080 H.264 CRF 16, for social uploads (not committed)
 *   <name>-<v>.mp4        site copy, 1280x720 H.264, faststart
 *   <name>-<v>.webm       site copy, 1280x720 VP9
 *   <name>-<v>.jpg        site poster, the frame at CLIP_POSTER_T
 * Copy the three site files to frontend/assets/clips/.
 */
const http = require('http');
const fs = require('fs');
const path = require('path');
const { spawn } = require('child_process');

const REPO = path.resolve(__dirname, '..', '..');
const { chromium } = require(path.join(REPO, 'frontend', 'node_modules', '@playwright/test'));
const FFMPEG = process.env.FFMPEG || 'ffmpeg';

const args = process.argv.slice(2);
const name = args[0];
const outDir = path.resolve(args[1] || '.');
const fps = Number((args.find((a, i) => args[i - 1] === '--fps')) || 30);
const previewArg = args.find((a, i) => args[i - 1] === '--preview');
const ver = args.find((a, i) => args[i - 1] === '--version') || 'v1';
if (!name || !fs.existsSync(path.join(__dirname, `clip-${name}.html`))) {
  console.error('usage: record.cjs <clip-name> <out-dir> [--fps 30] [--preview t1,t2]');
  process.exit(2);
}
fs.mkdirSync(outDir, { recursive: true });

const TYPES = { '.html': 'text/html', '.css': 'text/css', '.js': 'text/javascript', '.svg': 'image/svg+xml', '.png': 'image/png' };
function serve() {
  const server = http.createServer((req, res) => {
    const rel = decodeURIComponent(new URL(req.url, 'http://x').pathname);
    const file = path.normalize(path.join(REPO, rel));
    if (!file.startsWith(REPO + path.sep) || !fs.existsSync(file) || fs.statSync(file).isDirectory()) {
      res.writeHead(404); res.end(); return;
    }
    res.writeHead(200, { 'Content-Type': TYPES[path.extname(file)] || 'application/octet-stream' });
    fs.createReadStream(file).pipe(res);
  });
  return new Promise(resolve => server.listen(0, '127.0.0.1', () => resolve(server)));
}

function run(cmdArgs, input) {
  return new Promise((resolve, reject) => {
    const p = spawn(FFMPEG, ['-hide_banner', '-loglevel', 'error', '-y', ...cmdArgs], { stdio: [input ? 'pipe' : 'ignore', 'inherit', 'inherit'] });
    p.on('error', reject);
    p.on('close', code => (code === 0 ? resolve() : reject(new Error(`ffmpeg exited ${code}`))));
    if (input) input(p.stdin);
  });
}

(async () => {
  const server = await serve();
  const url = `http://127.0.0.1:${server.address().port}/marketing/clips/clip-${name}.html`;
  const browser = await chromium.launch();
  const page = await browser.newPage({ viewport: { width: 1920, height: 1080 }, deviceScaleFactor: 1 });
  // Only local files and Google Fonts; nothing else may load.
  await page.route('**/*', r => {
    const u = r.request().url();
    if (u.startsWith(`http://127.0.0.1:${server.address().port}/`) || /^https:\/\/fonts\.(googleapis|gstatic)\.com\//.test(u)) return r.continue();
    return r.abort();
  });
  const errors = [];
  page.on('pageerror', e => errors.push(String(e)));
  await page.goto(url, { waitUntil: 'networkidle' });
  await page.evaluate(async () => {
    await Promise.all(['400 100px Newsreader', '500 100px Newsreader', '400 20px "Instrument Sans"', '500 20px "Instrument Sans"', '600 20px "Instrument Sans"', '700 20px "Instrument Sans"'].map(f => document.fonts.load(f)));
    await document.fonts.ready;
    window.CLIP_INIT();
  });
  const fontsOk = await page.evaluate(() => document.fonts.check('400 100px Newsreader') && document.fonts.check('600 20px "Instrument Sans"'));
  if (!fontsOk) throw new Error('brand fonts did not load');
  const [duration, posterT] = await page.evaluate(() => [window.CLIP_DURATION, window.CLIP_POSTER_T]);

  if (previewArg) {
    for (const t of previewArg.split(',').map(Number)) {
      await page.evaluate(x => window.renderAt(x), t);
      await page.screenshot({ path: path.join(outDir, `${name}-t${t.toFixed(2)}.png`) });
    }
  } else {
    const frames = Math.round(duration * fps);
    const base = path.join(outDir, `${name}-${ver}`);
    const master = `${base}-1080p.mp4`;
    await run(['-f', 'image2pipe', '-framerate', String(fps), '-c:v', 'png', '-i', '-',
      '-c:v', 'libx264', '-preset', 'slow', '-crf', '16', '-pix_fmt', 'yuv420p', '-movflags', '+faststart', master], async stdin => {
      for (let i = 0; i < frames; i++) {
        await page.evaluate(x => window.renderAt(x), i / fps);
        const buf = await page.screenshot({ type: 'png' });
        if (!stdin.write(buf)) await new Promise(r => stdin.once('drain', r));
      }
      stdin.end();
    });
    await page.evaluate(x => window.renderAt(x), posterT ?? duration - 0.001);
    const posterPng = `${base}-poster.png`;
    await page.screenshot({ path: posterPng });
    const scale = 'scale=1280:720:flags=lanczos';
    await run(['-i', master, '-vf', scale, '-c:v', 'libx264', '-preset', 'veryslow', '-crf', '28', '-tune', 'animation', '-pix_fmt', 'yuv420p', '-movflags', '+faststart', '-an', `${base}.mp4`]);
    await run(['-i', master, '-vf', scale, '-c:v', 'libvpx-vp9', '-b:v', '0', '-crf', '44', '-row-mt', '1', '-deadline', 'good', '-cpu-used', '1', '-an', `${base}.webm`]);
    await run(['-i', posterPng, '-vf', scale, '-q:v', '4', `${base}.jpg`]);
    fs.unlinkSync(posterPng);
  }
  await browser.close();
  server.close();
  if (errors.length) { console.error('page errors:', errors); process.exit(1); }
  console.log('ok', name, previewArg ? 'preview' : 'video', outDir);
})().catch(e => { console.error(e); process.exit(1); });
