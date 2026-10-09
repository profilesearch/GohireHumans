# Product clips

Two 36-second product animations of real GoHireHumans flows, in the style of a
kinetic-type product video: an ink panel with the step copy, and the site's own
UI components animating on the right. Version v3 uses uneven step windows: hiring
spends longer on drafting and fees; earning spends longer on applications and delivery.
Panels enter in 0.5s and leave in 0.35s with no reading-hold zoom. Cursor moves take
0.7s, and typing is 20 characters/s with a visible caret. Dense panels hold at least
2.5s, and click results hold at least 1.5s. The complete hire fee table holds for
2.5s before confirmation. Step copy exits with its panel, and the end-card fine print
is fully visible for over 2s. Settled posters are at 23s (hire fees) and 31s (earned payout).

| Clip | Story | Used on |
| --- | --- | --- |
| `hire` | Describe a task → post it free → review applicants → confirm the hire with every fee shown → approve the delivered work | Homepage "How it works" band, `/how-it-works.html` (For clients) |
| `earn` | Set up payouts → find a paid job → apply → submit deliverables → "payment released" notification | `/how-it-works.html` (For workers), `/earn/get-paid-for-human-tasks.html` |

## How it is built

- `clip-*.html` lays out each scene with the production `frontend/style.css` and
  `frontend/app.css`, so cards, buttons, modals, badges, toasts and notifications are
  the real components. Copy comes from the real UI strings (`renderPostJob`,
  `handleHire`, `applicantList`, `handleJobApply`, `submitOrder`, backend
  `push_notification` titles).
- `engine.js` renders every frame as a pure function of time (`renderAt(t)`), so the
  recording is frame-exact.
- `record.cjs` serves the repo locally, steps through the frames with Playwright and
  pipes them to ffmpeg. Only local files and Google Fonts may load.

All names, amounts and order numbers in the clips are illustrative: an "Illustrative
example" tag stays on screen throughout the product UI scenes (so the posters carry it
too), and each end card repeats the disclosure. Fees match the live formula: 1% platform fee + 3% processing on the listed
payout ($60.00 → $0.60 + $1.80 = $62.40).

## Re-render

Needs Node, the frontend Playwright install, and an ffmpeg with libx264 and libvpx-vp9
(for example the `imageio-ffmpeg` wheel).

```bash
FFMPEG=/path/to/ffmpeg node marketing/clips/record.cjs hire /tmp/clips --version v4
FFMPEG=/path/to/ffmpeg node marketing/clips/record.cjs earn /tmp/clips --version v4
# preview single frames without encoding:
node marketing/clips/record.cjs hire /tmp/clips --preview 4,10,23
```

Outputs per clip: `<name>-<v>-1080p.mp4` (1920×1080 master for social uploads, not
committed), and `<name>-<v>.mp4`, `<name>-<v>.webm`, `<name>-<v>.jpg` for the site.

`/assets/` is served with a one-year immutable cache, so a re-render must use a new
version: copy the three site files to `frontend/assets/clips/` and bump
`ASSET_VERSION` in `frontend/product-clips.js`.

## On the site

`frontend/product-clips.js` turns `<figure class="ghh-clip" data-clip="hire"></figure>`
into a poster image plus a muted, looping video that loads only when the clip reaches
the viewport, pauses off screen, never autoplays under `prefers-reduced-motion` (it also
reacts when that setting changes), and has a pause/play button. Clips removed by an SPA
route change are disposed. It sends one `product_clip_view` GA4 event per clip name per
page load, once playback has actually started (diagnostic, not a conversion).

Social posts using the 1080p masters need their own approval; nothing here posts
anything.
