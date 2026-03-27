# NorCal Smash — PR website

Dark-themed web app for **Super Smash Bros. Ultimate** NorCal: live **ELO-style rankings** from cached start.gg data, a **calendar** of events (Pacific time), and a multi-step **PR Maker** flow (scrape → ingest → candidates → pairwise comparison → export).

Stack: **React 19 + Vite** (UI) and a **Python `http.server`** API (`tools/web_api.py`) that talks to start.gg and SQLite caches.

---

## Features

| Area | What it does |
|------|----------------|
| **Home** | All-time or date-range rankings; recent events with job-based loading; optional “resolve coverage gaps” scrape. |
| **Calendar** | Pick a day (Pacific) or browse “this week”; concluded vs upcoming cards. |
| **PR Maker** | Quarter shortcuts, ELO-eligible event scrape, selective ingest, merges, OOR-aware comparisons, AI argument (OpenAI), markdown + CSV export. |
| **Debug** | Header toggle + left log shelf: patched `fetch` logging and verbose page events (persisted via `localStorage`). |

---

## Prerequisites

- **Node.js** 18+ (for Vite)
- **Python** 3.11+ (3.10+ usually fine)
- **start.gg API token** — create one at [developer.start.gg](https://developer.start.gg/) and set `STARTGG_API_KEY`.
- **OpenAI API key** — optional; required only for comparison “Generate argument”.

---

## Setup

```bash
# Clone and enter the repo
cd norcal-pr-website

# Python: use a venv if you like, then install what the demo stack needs.
# (The API imports from demo/base_demo; install deps your environment is missing,
#  e.g. requests, python-dotenv — match your existing project practice.)

# Frontend
cd web
npm install
```

### Environment variables

```bash
cp .env.example .env
```

Edit **`.env`** at the **repository root** (same folder as this `README.md`). The API loads it via `python-dotenv` from that path.

| Variable | Required | Purpose |
|----------|----------|---------|
| `STARTGG_API_KEY` | **Yes** | All start.gg GraphQL usage. |
| `OPENAI_API_KEY` | No | PR Maker AI comparison arguments only. |

Never commit `.env`. Use `.env.example` as the template (no secrets).

---

## Run locally (development)

You need **two terminals**: API first, then the Vite dev server (which proxies `/api` to the API).

**Terminal 1 — API (default `http://127.0.0.1:8765`)**

```bash
cd norcal-pr-website
python3 tools/web_api.py
# Optional: python3 tools/web_api.py --host 127.0.0.1 --port 8765
```

**Terminal 2 — frontend**

```bash
cd norcal-pr-website/web
npm run dev
```

Open the URL Vite prints (typically `http://localhost:5173`). API routes are proxied per `web/vite.config.js`.

**Production-style static build**

```bash
cd web && npm run build
```

Serve `web/dist/` with any static host; you must still run the Python API (or put it behind a reverse proxy) and configure the same `/api` routing.

---

## Repository layout

```
norcal-pr-website/
├── web/                 # React app (Vite)
├── tools/               # web_api.py, recent_events helpers, etc.
├── demo/base_demo/      # ELO, scraper, processor, start.gg client (imported by API)
├── data/                # Created at runtime — gitignored (caches, DBs, logs)
└── .env                 # Local secrets — gitignored
```

Runtime artifacts (SQLite DBs, tournament cache, OOR cache, etc.) live under **`data/`** and similar paths and are **ignored by git**.

---

## Scripts (frontend)

| Command | Description |
|---------|-------------|
| `npm run dev` | Dev server + HMR |
| `npm run build` | Production bundle → `web/dist/` |
| `npm run preview` | Preview production build |
| `npm run lint` | ESLint |

---

## Security & privacy

- **Do not** commit API keys, `.env`, private keys, or ad-hoc `keys.py` files.
- The UI never embeds secrets; all privileged calls go through your local API.
- Review `.gitignore` before pushing; if you add new secret locations, ignore them and rotate any key that was ever committed.

### Secret scan (including `demo/`)

A pass was done over **`demo/`**, **`tools/`**, and **`web/src`** for hardcoded credentials (Bearer tokens, `AUTH_TOKEN=…`, OpenAI-style keys, `keys.py` imports with literals, etc.). The only hits were **plaintext start.gg tokens** and a **`from keys import …`** line inside **`demo/base_demo/all-functions.ipynb`**. That notebook was **rewritten to use `os.environ["STARTGG_API_KEY"]` / `os.environ.get("STARTGG_API_KEY")` only** (same as the rest of the project). **No new `.gitignore` entries were added for that file** because it no longer contains secrets—it is safe to track.

If that notebook (or any file) was **ever pushed with old tokens**, treat those tokens as **compromised**: revoke/rotate them in the start.gg developer dashboard and rewrite Git history if needed (`git filter-repo`, BFG, etc.) before making the repo public.

---

## License

Add a `LICENSE` file if you open-source this repo; until then, default copyright applies to your work.

---

## Contributing

1. Branch from `main` (or your default branch).
2. Keep PRs focused; match existing formatting in `web/src` and `tools/`.
3. Run `npm run lint` and smoke-test API + `npm run dev` before opening a PR.

For more detail on the Vite/React piece only, see [`web/README.md`](web/README.md).
