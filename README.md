# GoHireHumans — Deployment Guide

The trusted marketplace where businesses, AI agents, and individuals hire verified human professionals on demand.

## Architecture

```
┌─────────────────────────┐          ┌──────────────────────────┐
│    Vercel (Frontend)     │  ─────▶  │  Railway (Backend API)    │
│                         │   API    │                          │
│  index.html             │  calls   │  Flask + Gunicorn        │
│  style.css              │          │  SQLite database         │
│  base.css               │          │  Python 3.12             │
│  config.js              │          │                          │
└─────────────────────────┘          └──────────────────────────┘
```

- **Frontend**: Static SPA (HTML/CSS/JS) hosted on Vercel
- **Backend**: Python Flask API hosted on Railway (Docker)
- **Database**: SQLite (file-based, included in container volume)

---

## Quick Start (Local Development)

### 1. Start the Backend

```bash
cd backend
pip install -r requirements.txt
python server.py
```

The API will be running at `http://localhost:8080`. Test it:

```bash
curl http://localhost:8080/health
# → {"status": "ok", "service": "gohirehumans-api"}
```

### 2. Seed Demo Data

Seeding is disabled unless `SEED_SECRET` is configured. Use it only for local/demo setup and unset it in production after controlled setup.

```bash
curl -X POST http://localhost:8080/seed \
  -H 'Content-Type: application/json' \
  -d '{"secret":"YOUR_SEED_SECRET"}'
```

This creates local/demo marketplace records. Do not publish or rely on demo credentials for production.

### 3. Serve the Frontend

```bash
cd frontend
# Any static file server works:
python -m http.server 3000
```

Open `http://localhost:3000` in your browser.

---

## Deploy to Railway (Backend)

### Step 1: Create a Railway Project

1. Go to [railway.app](https://railway.app) and sign in
2. Click **"New Project"** → **"Deploy from GitHub Repo"**
3. Connect your GitHub account and select your repo (or use "Deploy from Local" with the Railway CLI)

### Step 2: Configure the Service

1. In your Railway project, click on the service
2. Go to **Settings** → **Build & Deploy**
3. Set **Root Directory** to `backend`
4. Railway will auto-detect the Dockerfile

### Step 3: Add Environment Variables

In the Railway dashboard, go to **Variables** and add:

| Variable | Value |
|----------|-------|
| `PORT` | `8080` (Railway usually sets this automatically) |
| `FLASK_DEBUG` | `false` |
| `DATABASE_PATH` | Optional local override. Production prefers `/data/gohirehumans.db`. |

### Step 4: Add a Persistent Volume (Important!)

SQLite needs persistent storage:

1. In Railway, click **"+ New"** → **"Volume"**
2. Mount path: `/data`
3. Do not point production SQLite under `/app`; `/app` is ephemeral.

### Step 5: Deploy

Railway deploys automatically on push. Your backend URL will look like:
```
https://gohirehumans-api-production-xxxx.up.railway.app
```

### Step 6: Optional Controlled Seed

Only seed a controlled staging/demo environment. Production auto-seeding is disabled by default.

```bash
curl -X POST https://YOUR-RAILWAY-URL/seed \
  -H 'Content-Type: application/json' \
  -d '{"secret":"YOUR_SEED_SECRET"}'
```

---

## Deploy to Vercel (Frontend)

### Step 1: Update API URL

Edit `frontend/config.js` and set your Railway backend URL:

```javascript
window.GOHIREHUMANS_API_URL = "https://your-railway-backend-url.up.railway.app";
```

### Step 2: Deploy to Vercel

**Option A: Vercel CLI**

```bash
cd frontend
npx vercel --prod
```

**Option B: GitHub Integration**

1. Go to [vercel.com](https://vercel.com) and sign in
2. Click **"Add New Project"** → import your repo
3. Set **Root Directory** to `frontend`
4. Framework Preset: **Other**
5. Click **Deploy**

### Step 3: Custom Domain

1. In Vercel dashboard → **Settings** → **Domains**
2. Add `gohirehumans.com`
3. Follow the DNS configuration instructions

---

## Deploy with Railway CLI (Alternative)

```bash
# Install Railway CLI
npm install -g @railway/cli

# Login
railway login

# Initialize project
cd backend
railway init

# Deploy
railway up

# Get your URL
railway domain
```

---

## Project Structure

```
GohireHumans/
├── backend/
│   ├── server.py          # Flask server (production wrapper)
│   ├── api_core.py        # Core API logic (~14,000 lines, single route dispatcher)
│   ├── mcp_server.py      # MCP server for AI agents (mirrored in mcp-package/)
│   ├── test_*.py          # unittest suites (run with discover -s backend)
│   ├── requirements.txt   # Python dependencies
│   ├── Dockerfile         # Container config for Railway
│   ├── railway.toml       # Railway deployment config
│   ├── Procfile           # Process file (Heroku/Railway)
│   ├── start.sh           # Container entrypoint (execs gunicorn)
│   ├── .dockerignore      # Keeps tests/tools/local DBs out of the image
│   └── .env.example       # Environment variable template
│
├── frontend/
│   ├── index.html         # Single Page Application (hash routes under /#/)
│   ├── style.css          # Shared stylesheet: tokens, reset, components, public shell, page layouts
│   ├── app.css            # Signed-in app surfaces (loaded by index.html only)
│   ├── base.css           # Reset mirror for the SPA
│   ├── mobile-hardening.css # Phone-width guards (no-op above 768px)
│   ├── partials/          # Canonical public nav and footer (synced into every static page)
│   ├── *.html, blog/, hire/, use-cases/, ai-human-qa/, categories/, vs/, tools/, earn/, examples/
│   │                      # Static marketing, SEO, docs, and tool pages
│   ├── analytics-bootstrap.js # Fail-closed GA loader (production origins only)
│   ├── config.js          # API URL configuration ← EDIT THIS
│   ├── sitemap.xml, feed.xml, atom.xml, robots.txt, llms.txt, .well-known/
│   ├── performance-budgets.json # Byte budgets enforced in CI
│   ├── tests/             # Playwright browser regression suites
│   └── vercel.json        # Vercel redirects and security headers
│
├── docs/
│   ├── design-system/     # design-system.md (tokens, components, page patterns) and public-shell.md
│   ├── ops/               # Operating playbooks and internal working docs (not deployed)
│   └── plans/             # Historical sprint plans
│
├── scripts/
│   ├── sync_public_shell.py   # Sync the nav/footer partials into every static page (--check in CI)
│   ├── check_public_shell.py  # Guard: every public page has the canonical shell, skip link, main, bootstrap
│   ├── performance_budget.py  # Enforce performance-budgets.json
│   ├── generate_feeds.py      # Rebuild feed.xml and atom.xml from blog page metadata
│   └── generate-marketplace-pulse.py
│
├── .github/workflows/ci.yml # Backend tests, static checks, Playwright suites
├── .gitignore
└── README.md              # This file
```

---

## API Endpoints

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/health` | Health check |
| `POST` | `/auth/register` | Register new user |
| `POST` | `/auth/login` | Login |
| `GET` | `/profile` | Get current user profile |
| `GET` | `/services` | List service listings |
| `POST` | `/services` | Create a service listing |
| `GET` | `/jobs` | List jobs |
| `GET` | `/me/services` | The caller's own services in every status (auth required; `page`, `per_page`, `status`, `include_removed`) |
| `GET` | `/me/jobs` | The caller's own jobs in every status, with `application_count` (auth required) |
| `POST` | `/jobs` | Create a job |
| `POST` | `/jobs/{id}/apply` | Apply to a job |
| `POST` | `/jobs/{id}/hire` | Hire an applicant |
| `GET` | `/services/{id}/quote` | Preview an authenticated, itemized service charge without creating an order |
| `POST` | `/services/{id}/order` | Order a listed service; optionally bind the request to a quote |
| `POST` | `/orders/{id}/approve` | Approve submitted work and release payment |
| `POST` | `/orders/{id}/review` | Leave a review |
| `POST` | `/seed` | Secret-gated local/demo seeding |
| `GET` | `/admin/dashboard` | Admin statistics |

---

## Content Safety

GoHireHumans includes built-in content safety filters that block:
- 80+ prohibited keywords and phrases
- Inappropriate service categories
- Dark web / illegal content patterns

All task titles and descriptions are automatically screened.

---

## Troubleshooting

### CORS Errors
The backend uses the `ALLOWED_ORIGINS` env var and does not allow wildcard credentials by default. If you see CORS errors:
1. Make sure the backend is running and accessible
2. Check that `config.js` has the correct backend URL
3. Ensure there's no trailing slash on the URL

### Database Reset
To start fresh locally, delete your local SQLite file and optionally run secret-gated `/seed` again. Do not delete production `/data/gohirehumans.db` without a verified backup and restore plan.

### Railway Volume Issues
If data disappears between deploys, make sure you've attached a persistent volume at `/data`. The app stores production SQLite at `/data/gohirehumans.db`.

---

## Frontend checks

The static checks CI runs, in order:

```bash
python3 scripts/check_public_shell.py
python3 scripts/sync_public_shell.py --check
python3 scripts/performance_budget.py
python3 backend/security_static_checks.py
python3 -m unittest discover -s backend -p 'test_deep_audit_regressions.py'
```

Browser suites (Playwright, desktop and Pixel 5 projects):

```bash
cd frontend && npm ci && npx playwright install chromium && npm run test:browser
```

Set `PW_PORT=<port>` if 4173 is taken on your machine; the config and specs honor it. After changing `frontend/partials/`, run `python3 scripts/sync_public_shell.py` (without `--check`) to propagate the shell, and update the JS-rendered nav and footer in `index.html` by hand. After adding a blog post, run `python3 scripts/generate_feeds.py`.

The visual system is documented in `docs/design-system/design-system.md`; static pages build on its classes instead of page-local styles.

---

## Tech Stack

- **Frontend**: Vanilla JS SPA plus static HTML pages, Newsreader and Instrument Sans (Google Fonts), CSS custom properties, one shared stylesheet
- **Backend**: Python 3.12, Flask, Gunicorn, SQLite
- **Hosting**: Vercel (frontend) + Railway (backend)
- **Security**: PBKDF2-HMAC password hashing, session tokens, rate limiting, content safety filters
- **Domain**: gohirehumans.com
