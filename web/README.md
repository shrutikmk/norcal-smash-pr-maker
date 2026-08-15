# NorCal Smash — web (React + Vite)

The main documentation for this repository lives in **[`../README.md`](../README.md)** (setup, env vars, how to run the stack).

Primary local run path from the repo root (API + Vite, and vLLM if needed):

```bash
../scripts/pr-maker-local-stack.sh restart
```

This folder is the React 19 + Vite SPA. API requests to `/api/*` are proxied to `http://127.0.0.1:8775` during development by default (`vite.config.js`). Override with `PR_MAKER_API_PORT` or `VITE_API_PORT` if the API runs elsewhere.

```bash
npm install
npm run dev
```
