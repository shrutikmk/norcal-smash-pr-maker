# OOR (Out-of-Region) Data Pipeline

How the PR Maker fetches, caches, and serves out-of-region player data
(majors, other scenes, notable results) for comparison cards and CSV exports.

## Architecture

```
PRMakerCandidatesPage ──► /oor-warm/start ──► _oor_warm_worker (ELO-prioritized, resumable)
PRMakerRankingPage    ──► /comparison {includeOOR, oorCacheOnly} ──┐
                      ──► /player-oor {stream} (missing players)   ├─► _load_reports_for_players
PRMakerFinalPage      ──► /final-export (CSV, usually prefetched)  ┘          │
                                                                   ┌──────────┴──────────┐
                                                              cache layers          live pipeline
                                                       memory → SQLite → rebuild   _get_live_player_report
                                                                                   (parallel pages + standings,
                                                                                    shared rate gate ≤70 rpm)
```

Every start.gg request — sync or async, OOR or scraper — goes through the
process-wide rate gate in `demo/base_demo/startgg_rate_gate.py` (default
70 req/min, under the 80/min hard limit). Concurrency in the OOR pipeline only
hides network latency; it never raises the aggregate request rate.

## Cache layers (fastest first)

| Layer | Scope | Key | Invalidation |
|---|---|---|---|
| In-memory reports (`_OOR_MEM_REPORTS`) | process | `(context_hash, name)` | LRU-ish cap (2048), `forceRefreshOOR`, restart |
| In-memory standings (`_OOR_MEM_STANDINGS`) | process | `event_id` | cap (4096), restart |
| `live_report_cache` | SQLite | `(context_hash, name)` | overwritten on refetch; empty shells force live refetch |
| `oor_event_row` + `oor_player_fetch_state` | SQLite | `(context_hash, name, tournament, event)` | granular rebuild source; `status=complete` marks finished players (incl. legit zero-OOR players) |
| `oor_player_sets_cache_v2` | SQLite | `(player_id, window_hash)` | TTL (`OOR_SETS_TTL_SEC`, default 7 days) |
| `oor_tournament_result_cache` | SQLite | `(player_id, tournament_id)` | permanent (finished brackets are immutable) |
| `oor_event_standings_cache` | SQLite | `event_id` | permanent (full `{player_id: placement}` map per event) |
| `oor_tournament_catalog` | SQLite | `window_hash` | TTL 7 days |

All SQLite tables live in `data/oor_report_cache.db` (WAL mode). The
connection is opened with `check_same_thread=False` and every helper
serializes through an `RLock` so the fetch thread pool can read/write safely
(previously, every cache access from a pool worker raised silently — warm jobs
and pair fetches never actually cached anything).

### Fingerprint formats

- **`context_hash`** (24 hex chars): SHA-256 of
  `{"v": VERSION, "s": start, "e": end, "slugs": sorted(event_slugs), "merges": sorted(merge_rules)}`.
  Identifies one PR Maker configuration; reports cached under it are reusable
  across sessions with the same window/events/merges.
- **`window_hash`** (16 hex chars): SHA-256 of `"v{VERSION}:{start}:{end}"`.
  Partitions context-independent data that only depends on the date window
  (set history, tournament catalog).
- **`_OOR_FINGERPRINT_VERSION`** is a salt baked into both hashes. Bump it when
  OOR *derivation semantics* change so stale cached reports are rebuilt instead
  of served (v2: online tournaments excluded — set nodes cached before v2 lack
  the `isOnline` fields and cannot be filtered retroactively). Old rows become
  unreachable garbage, not wrong answers.
- Context-independent caches (`player_id`, `tournament_id`, `event_id` keys)
  survive *any* context change — changing the event selection or merges never
  re-fetches standings or per-tournament results.

### Why standings maps matter

The old pipeline fetched placement per `(event, player)`: 30 candidates sharing
20 in-region events = up to 600 paginated standings scans. The new pipeline
fetches the **full standings map once per event** (`fetch_event_standings_map`)
and caches it permanently — the same 30×20 grid now costs ≤20 fetches, ever.

## Live fetch path (cache miss)

Per player, `_get_live_player_report` now:

1. Resolves in-region placements via cached standings maps (parallel fetch of
   misses, `OOR_STANDINGS_CONCURRENCY` at a time).
2. Fetches set history pages 2..N in concurrent batches
   (`OOR_SET_PAGE_CONCURRENCY`), with the pre-window early-stop check applied
   between batches; complexity errors halve `perPage` and restart.
3. Buckets OOR events, then resolves their placements in one parallel batch.
   **Online tournaments are excluded**: the set-history and catalog queries
   select `isOnline` (event-level flag wins, tournament-level as fallback) and
   online events never enter the OOR aggregation — the
   `oor_tournaments_discovered` progress event reports how many were skipped.
4. Emits the full event rows — including notable win/loss lists, `start_at`,
   `event_id` — so the granular rebuild path reproduces the complete report
   (previously notables were stored empty).

Across players, `_load_reports_for_players` runs a thread pool:
`OOR_FETCH_CONCURRENCY` workers for user-facing requests,
`OOR_WARM_CONCURRENCY` for warm jobs. Players in the active comparison pair
always jump to the front of the queue.

## Warm job

Fired automatically when candidates are selected (debounced) and again when
proceeding to ranking. The worker:

- sorts names by in-region ELO descending (top candidates warm first),
- is resumable: already-cached players are skipped on re-fire,
- reports `completed/total/currentPlayer/requestsUsed` via
  `GET /api/pr-maker/oor-warm/status`,
- logs per-batch cache hit/miss summaries to the debug feed (`server/OOR`,
  `server/OOR-warm` sources).

## Comparison fast path

`POST /api/pr-maker/comparison` with `{"includeOOR": true, "oorCacheOnly": true}`
serves whatever OOR is already cached **without touching start.gg** and returns
`missingOOR: [names]`. The ranking UI:

1. makes that single request and renders the card immediately;
2. if `missingOOR` is empty (warm cache): done — one round trip;
3. otherwise streams `/player-oor` only for the missing players (NDJSON
   progress, never blocking the card), then re-issues the cache-only request;
4. speculatively prefetches the two possible *next* pairs (win/lose branches of
   the binary insertion) and fires a background warm for any players they're
   missing.

## CSV export

- The warm job populates every cache the export needs, so
  `/final-export` and `/candidates-export` are pure local compute when warm.
- When the user finishes ranking, the UI prefetches the final CSV in the
  background and stores it in `sessionStorage.prMakerPrefetchedCsv`; the
  Download button on the final page is then instant.

## Observability

- `GET /api/pr-maker/oor-stats` — cache-layer counters (memory/SQLite/rebuild/
  live hits, set-history + standings + tournament-result hit/miss), in-memory
  cache sizes, rate-gate metrics, and active tuning values.
- Every report batch logs a one-line summary to the debug shelf:
  `mem=… sqlite=… rebuilt=… live=… · sets H/M · standings H/M · tourney H/M`
  plus the approximate start.gg request count it consumed.

## Tuning (env vars)

| Variable | Default | Meaning |
|---|---|---|
| `OOR_FETCH_CONCURRENCY` | 3 | players fetched in parallel for user requests |
| `OOR_WARM_CONCURRENCY` | 2 | players fetched in parallel by warm jobs |
| `OOR_SET_PAGE_CONCURRENCY` | 4 | set-history pages in flight per player |
| `OOR_STANDINGS_CONCURRENCY` | 4 | standings maps in flight per player |
| `OOR_SETS_TTL_SEC` | 604800 | set-history freshness threshold (7 days) |
| `STARTGG_MAX_RPM` | 70 | process-wide rolling request cap (shared gate) |

## Schema changes / migration

New table (auto-created on first connection, no migration needed):

```sql
CREATE TABLE IF NOT EXISTS oor_event_standings_cache (
    event_id TEXT NOT NULL PRIMARY KEY,
    placements_json TEXT NOT NULL,   -- {"player_id": placement|null, ...}
    fetched_at INTEGER NOT NULL
);
```

Existing tables are unchanged. Pre-existing `oor_event_row` rows written by the
old code have empty notable lists / zero `start_at`; they are healed naturally
the next time a player is refetched (or wipe `data/oor_report_cache.db` to
rebuild from scratch — everything in it is derivable).

## Benchmarking

With the local stack running:

```bash
.venv/bin/python scripts/benchmark_oor.py \
    --start 2025-01-01 --end 2025-06-01 \
    --slugs tournament/x/event/singles,tournament/y/event/singles \
    --top 25
```

Reports warm-job wall time + request count for the top-N candidates,
comparison-card latency via the cache-only path, final CSV export time, and
the cache hit rates for the run. Run twice for cold-vs-warm numbers; the
second run should show a near-100% report cache hit rate, single-digit
millisecond-to-subsecond comparisons, and ~0 start.gg requests.
