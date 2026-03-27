# NorCal Smash — PR website

Dark-themed web app for **Super Smash Bros. Ultimate** NorCal: live **ELO-style rankings** from cached start.gg data, a **calendar** of events (Pacific time), and a multi-step **PR Maker** flow (scrape → ingest → candidates → pairwise comparison → export).

Stack: **React 19 + Vite** (UI) and a **Python `http.server`** API (`tools/web_api.py`) that talks to start.gg and SQLite caches.

**Repository:** [github.com/shrutikmk/norcal-smash-pr-maker](https://github.com/shrutikmk/norcal-smash-pr-maker)

---

## Features

| Area | What it does |
|------|----------------|
| **Home** | All-time or date-range rankings; recent events with job-based loading; optional “resolve coverage gaps” scrape. |
| **Calendar** | Pick a day (Pacific) or browse “this week”; concluded vs upcoming cards. |
| **PR Maker** | Quarter shortcuts, ELO-eligible event scrape, selective ingest, merges, OOR-aware comparisons, AI argument (OpenAI), markdown + CSV export. |
| **Debug** | Header toggle + left log shelf: client `fetch` logging, server-side events (poll), and page-specific debug lines (`localStorage` for the toggle). |

---

## Prerequisites

- **Node.js** 18+ (for Vite)
- **Python** 3.11+ (3.10+ usually works)
- **start.gg API token** — create one at [developer.start.gg](https://developer.start.gg/) and set `STARTGG_API_KEY`.
- **OpenAI API key** — optional; required only for PR Maker “Generate argument”.

---

## Setup

```bash
git clone https://github.com/shrutikmk/norcal-smash-pr-maker.git
cd norcal-smash-pr-maker

# Python (venv recommended)
python3 -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt

# Frontend
cd web
npm install
```

### Environment variables

```bash
cp .env.example .env
```

Edit **`.env`** at the **repository root** (next to this `README.md`). The API loads it with `python-dotenv` from that path.

| Variable | Required | Purpose |
|----------|----------|---------|
| `STARTGG_API_KEY` | **Yes** | All start.gg GraphQL usage. |
| `OPENAI_API_KEY` | No | PR Maker AI comparison arguments only. |

Never commit `.env`, `keys.py`, or credential files. See `.gitignore` and `.env.example`.

---

## Run locally (development)

Use **two terminals**: API first, then the Vite dev server (proxies `/api` to the API).

**Terminal 1 — API (default `http://127.0.0.1:8765`)**

```bash
# From repo root (with venv activated)
python3 tools/web_api.py
# Optional: python3 tools/web_api.py --host 127.0.0.1 --port 8765
```

**Terminal 2 — frontend**

```bash
cd web
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
norcal-smash-pr-maker/
├── requirements.txt     # Python deps (API + demo notebook)
├── web/                 # React app (Vite)
├── tools/               # web_api.py, recent_events helpers, etc.
├── demo/base_demo/      # ELO, scraper, processor, start.gg client (imported by API)
├── data/                # Created at runtime — gitignored (caches, DBs)
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

## Security & secrets

- **Do not** commit API keys, `.env`, `.env.local`, private keys, ad-hoc `keys.py`, or `credentials.json`.
- The UI does not embed secrets; privileged calls go through your local API.
- **`demo/base_demo/all-functions.ipynb`** loads `STARTGG_API_KEY` and `OPENAI_API_KEY` only from the environment or a **repo-root `.env`** (via `python-dotenv`). Do not reintroduce hardcoded tokens or `from keys import …` in tracked files.

### If a key was ever committed or shared

Rotate it in the [start.gg](https://developer.start.gg/) / OpenAI dashboards. For leaked history on GitHub, use history rewriting ([`git filter-repo`](https://github.com/newren/git-filter-repo), BFG, etc.) in addition to rotation.

---

## License

Add a `LICENSE` file if you open-source this repo; until then, default copyright applies to your work.

---

## Contributing

1. Branch from `main`.
2. Keep PRs focused; match existing formatting in `web/src` and `tools/`.
3. Run `npm run lint` in `web/` and smoke-test the API + `npm run dev` before opening a PR.

More detail on the Vite/React app: [`web/README.md`](web/README.md).
