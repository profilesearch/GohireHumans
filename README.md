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

```bash
# Set SEED_SECRET in your .env first, then call /seed with the secret in the body.
curl -X POST -H 'Content-Type: application/json' \
  -d '{"secret":"YOUR_SEED_SECRET"}' http://localhost:8080/seed
```

This creates demo accounts:
- **Admin**: `admin@gohirehumans.com` / `Admin1234!`
- **Workers** (`Worker1234!`): `sarah.chen@example.com`, `marcus.johnson@example.com`, `elena.rodriguez@example.com`, `james.park@example.com`, `aisha.patel@example.com`
- **Employers** (`Employer1234!`): `hire@techstartup.io`, `ops@growthagency.com`, `founder@bootstrapped.co`

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
| `DATABASE_PATH` | `agentwork.db` |

### Step 4: Add a Persistent Volume (Important!)

SQLite needs persistent storage:

1. In Railway, click **"+ New"** → **"Volume"**
2. Mount path: `/app/data`
3. Update `DATABASE_PATH` to `/app/data/agentwork.db`

### Step 5: Deploy

Railway deploys automatically on push. Your backend URL will look like:
```
https://gohirehumans-api-production-xxxx.up.railway.app
```

### Step 6: Seed the Production Database

```bash
curl -X POST https://YOUR-RAILWAY-URL/seed
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
gohirehumans-deploy/
├── backend/
│   ├── server.py          # Flask server (production wrapper)
│   ├── api_core.py        # Core API logic (2600+ lines)
│   ├── requirements.txt   # Python dependencies
│   ├── Dockerfile         # Container config for Railway
│   ├── railway.toml       # Railway deployment config
│   ├── Procfile           # Process file (Heroku/Railway)
│   ├── .env.example       # Environment variable template
│   └── .gitignore
│
├── frontend/
│   ├── index.html         # Single Page Application
│   ├── style.css          # Main stylesheet
│   ├── base.css           # CSS reset/base
│   ├── config.js          # API URL configuration ← EDIT THIS
│   ├── vercel.json        # Vercel routing/headers config
│   └── .gitignore
│
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
| `GET` | `/tasks` | List tasks (with filters) |
| `POST` | `/tasks` | Create a new task |
| `POST` | `/tasks/{id}/apply` | Apply to a task |
| `POST` | `/tasks/{id}/accept` | Accept an application |
| `POST` | `/tasks/{id}/complete` | Mark task complete |
| `POST` | `/tasks/{id}/review` | Leave a review |
| `POST` | `/seed` | Seed demo data |
| `GET` | `/admin/stats` | Admin statistics |

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
The backend has CORS enabled for all origins (`*`). If you see CORS errors:
1. Make sure the backend is running and accessible
2. Check that `config.js` has the correct backend URL
3. Ensure there's no trailing slash on the URL

### Database Reset
To start fresh, delete the `agentwork.db` file and call `/seed` again.

### Railway Volume Issues
If data disappears between deploys, make sure you've attached a persistent volume at `/app/data` and set `DATABASE_PATH=/app/data/agentwork.db`.

---

## Tech Stack

- **Frontend**: Vanilla JS SPA, Inter font, CSS custom properties
- **Backend**: Python 3.12, Flask, Gunicorn, SQLite
- **Hosting**: Vercel (frontend) + Railway (backend)
- **Security**: HMAC password hashing, session tokens, rate limiting, content safety filters
- **Domain**: gohirehumans.com
