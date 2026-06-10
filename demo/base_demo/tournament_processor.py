"""
NorCal Tournament Processor — transforms scraped tournament data into set-level data for ELO.

Continues from tournament_scraper.py: reads the tournament cache, fetches sets per event,
and produces an enriched DB with player/set data ready for ELO generation.

Features:
- Reads from tournament_cache.db (output of tournament_scraper)
- Generates processed_tournament.db with sets, players, scores
- Event-level cache: skip re-fetching sets for already-processed events
- Set-level cache: skip re-fetching player/score for already-cached sets
- Verbose cache hit/miss reporting
- Configurable name merging/replacement prior to ELO generation

Performance (see SCRAPING_PERFORMANCE.md):
- Sets are fetched with a *single folded GraphQL query* that returns each set's
  players AND scores inline (``SETS_WITH_PLAYERS_QUERY``). This eliminates the
  old N+1 pattern (one extra request per individual set), cutting an event with
  N sets from ~N+4 requests down to ~ceil(N / perPage) requests.
- ``perPage`` is pushed as high as the 1000-object/request limit allows for the
  folded query shape (~60 by default; tune via ``STARTGG_SETS_PER_PAGE``), with
  an automatic halving fallback if the API ever reports a complexity error.
- Events are fetched *concurrently* via the asyncio engine in ``startgg_async``,
  all sharing the one process-wide rate budget (≤~70 req/60s).
"""

from __future__ import annotations

import asyncio
import json
import os
import sqlite3
import time
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Callable

import requests

from startgg_async import (
    DEFAULT_CONCURRENCY,
    AsyncStartGGClient,
    ComplexityLimitError,
    gather_bounded,
    run_async,
)
from startgg_rate_gate import (
    acquire_slot,
    get_metrics,
    get_request_count,
    is_complexity_error,
    is_likely_rate_limit_error,
    is_transient_server_error,
    sleep_after_429,
)
from tournament_scraper import connect_db

# Load .env from project root when available
try:
    from dotenv import load_dotenv
    # Project root .env (file lives in demo/base_demo/)
    load_dotenv(Path(__file__).resolve().parent.parent.parent / ".env")
except ImportError:
    pass

# --- Configuration ---

API_URL = "https://api.start.gg/gql/alpha"
# start.gg rate limits: 80 req/60s, max 1000 objects/request (incl. nested)
RATE_LIMIT_REQUESTS_PER_MIN = 80
RATE_LIMIT_INTERVAL_SEC = max(1.0, 60.0 / RATE_LIMIT_REQUESTS_PER_MIN)  # 1.0s for margin
MAX_OBJECTS_PER_REQUEST = 1000

# Effective per-set "cost" for the folded SETS_WITH_PLAYERS_QUERY. The raw nested
# object count is ~13, but start.gg's internal complexity scorer charges this
# deeply-nested resolver more heavily: empirically perPage 60 triggers intermittent
# "internal" errors / silent empty pages, while perPage 40 is reliably stable (the
# value recent_events.py also settled on). We model ~24 objects/node so the safe
# ceiling lands near 41, and default to 40.
SETS_OBJECTS_PER_NODE = 24
# Highest perPage empirically safe for this query shape under the 1000-object /
# complexity limits (validated against the live API).
SETS_MAX_PER_PAGE = MAX_OBJECTS_PER_REQUEST // SETS_OBJECTS_PER_NODE  # ~41


def _env_int(name: str, default: int, *, lo: int = 1, hi: int = 1000) -> int:
    raw = os.environ.get(name)
    if not raw:
        return default
    try:
        return max(lo, min(hi, int(raw)))
    except ValueError:
        return default


def _env_bool(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


# Default sets-per-page. 40 is the empirically-validated stable value for the
# folded query (perPage 60 caused start.gg internal errors / silent empty pages).
DEFAULT_SETS_PER_PAGE = min(SETS_MAX_PER_PAGE, _env_int("STARTGG_SETS_PER_PAGE", 40, hi=SETS_MAX_PER_PAGE))
# Use the async concurrent engine by default; set STARTGG_USE_ASYNC=0 to force the
# (still folded, but sequential) fallback path.
DEFAULT_USE_ASYNC = _env_bool("STARTGG_USE_ASYNC", True)
# Estimate-mode assumption when --dry-run can't see real set counts.
ESTIMATE_SETS_PER_EVENT = _env_int("STARTGG_ESTIMATE_SETS_PER_EVENT", 80, hi=10000)

# Project root data/ (file lives in demo/base_demo/)
_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DEFAULT_TOURNAMENT_CACHE = _PROJECT_ROOT / "data" / "tournament_cache.db"
DEFAULT_PROCESSED_CACHE = _PROJECT_ROOT / "data" / "processed_tournament.db"

# Default name mappings (alias -> canonical) for ELO; extend via config
DEFAULT_NAME_MAPPINGS: dict[str, str] = {
    "NLC | they call me leonidas": "Hyro",
    "NLC | Still Spoozy": "Hyro",
    "MPoor": "M4",
    "W4": "M4",
    "SALT | ebs | ERA": "ERA",
    "era": "ERA",
    "EBS | HK | the filipino flowstate.": "Skylock",
}


@dataclass
class ProcessorConfig:
    """Configuration for tournament processing."""

    tournament_cache_path: Path = field(default_factory=lambda: DEFAULT_TOURNAMENT_CACHE)
    processed_cache_path: Path = field(default_factory=lambda: DEFAULT_PROCESSED_CACHE)
    start_date: str = "2025-04-01"
    end_date: str = "2025-06-30"
    game_filter: str = "Super Smash Bros. Ultimate"
    min_entrants: int = 16
    name_mappings: dict[str, str] = field(default_factory=lambda: dict(DEFAULT_NAME_MAPPINGS))
    # When set, process exactly these event slugs (bypasses the date/game/entrants
    # filter). Used by the web PR Maker job, where the user hand-picks events.
    include_event_slugs: list[str] | None = None
    # --- Performance tuning (safe defaults; override via env or per-call) ---
    sets_per_page: int = DEFAULT_SETS_PER_PAGE  # capped to SETS_MAX_PER_PAGE in code
    concurrency: int = DEFAULT_CONCURRENCY      # max events fetched in parallel
    use_async: bool = DEFAULT_USE_ASYNC         # concurrent engine vs sequential fallback
    dry_run: bool = False                       # estimate request count/time, fetch nothing


# --- Rate Limiter ---


class RateLimiter:
    def __init__(self, interval_sec: float = RATE_LIMIT_INTERVAL_SEC):
        self.interval = interval_sec
        self._last_request_time: float = 0.0

    def wait(self) -> None:
        elapsed = time.monotonic() - self._last_request_time
        if elapsed < self.interval:
            time.sleep(self.interval - elapsed)
        self._last_request_time = time.monotonic()


# --- GraphQL Queries ---

EVENT_ID_QUERY = """
query getEventId($slug: String) {
  event(slug: $slug) {
    id
    name
  }
}
"""

SETS_PAGE_QUERY = """
query EventSets($eventId: ID!, $page: Int!, $perPage: Int!) {
  event(id: $eventId) {
    id
    name
    sets(page: $page, perPage: $perPage, sortType: STANDARD) {
      pageInfo {
        total
        totalPages
      }
      nodes {
        id
      }
    }
  }
}
"""

SET_PLAYERS_SCORE_QUERY = """
query SetsAndPlayers($setId: ID!) {
  set(id: $setId) {
    state
    slots {
      entrant {
        participants {
          player {
            gamerTag
            prefix
          }
        }
      }
      standing {
        stats {
          score {
            value
          }
        }
      }
    }
  }
}
"""

# Folded query: one paginated request returns each set's id, players AND scores
# together — no per-set follow-up requests. Resolving the event by slug (rather
# than a separate id lookup) also folds the old event-id round-trip into page 1.
# Object count per set node ≈ 13 (see SETS_OBJECTS_PER_NODE), so perPage ~60 stays
# well under the 1000-object/request limit.
SETS_WITH_PLAYERS_QUERY = """
query EventSetsWithPlayers($slug: String, $page: Int!, $perPage: Int!) {
  event(slug: $slug) {
    id
    name
    sets(page: $page, perPage: $perPage, sortType: STANDARD) {
      pageInfo {
        total
        totalPages
      }
      nodes {
        id
        slots {
          entrant {
            participants {
              player {
                gamerTag
                prefix
              }
            }
          }
          standing {
            stats {
              score {
                value
              }
            }
          }
        }
      }
    }
  }
}
"""


def _safe_sets_per_page(requested: int | None = None) -> int:
    """Clamp the requested sets perPage to the 1000-object-safe ceiling."""
    base = DEFAULT_SETS_PER_PAGE if requested is None else int(requested)
    return max(1, min(base, SETS_MAX_PER_PAGE))


# --- DB Schema ---


def _ensure_dir(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def _init_processed_db(conn: sqlite3.Connection) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS processed_events (
            event_slug TEXT PRIMARY KEY,
            event_id TEXT,
            tournament_id TEXT,
            event_name TEXT,
            processed_at INTEGER
        )
    """)
    _migrate_processed_events_columns(conn)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS sets_cache (
            set_id TEXT PRIMARY KEY,
            event_id TEXT,
            event_slug TEXT,
            p1_name TEXT,
            p2_name TEXT,
            p1_score INTEGER,
            p2_score INTEGER,
            cached_at INTEGER
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS processed_sets (
            set_id TEXT PRIMARY KEY,
            event_slug TEXT,
            p1_canonical TEXT,
            p2_canonical TEXT,
            p1_score INTEGER,
            p2_score INTEGER,
            created_at INTEGER
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_sets_event ON sets_cache(event_slug)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_processed_event ON processed_sets(event_slug)")
    conn.commit()


def _migrate_processed_events_columns(conn: sqlite3.Connection) -> None:
    """Add `tournament_updated_at` so we can skip reprocessing unchanged events."""
    cols = {row[1] for row in conn.execute("PRAGMA table_info(processed_events)").fetchall()}
    if "tournament_updated_at" not in cols:
        conn.execute("ALTER TABLE processed_events ADD COLUMN tournament_updated_at INTEGER")


def _date_to_unix(date_str: str, fmt: str = "%Y-%m-%d") -> int | None:
    try:
        return int(datetime.strptime(date_str, fmt).timestamp())
    except ValueError:
        return None


# --- API ---


def _gql_post(
    client: requests.Session,
    limiter: RateLimiter,
    query: str,
    variables: dict[str, Any],
    auth_token: str,
    max_retries: int = 30,
    max_transient_retries: int = 6,
) -> dict:
    """
    POST to start.gg GraphQL API. Retries on transient errors (5xx, 429,
    connection/timeout, start.gg "internal" hiccups). Rate-limit (429) waits use
    the full retry budget; other transient failures use a small bounded budget
    with short backoff so a flaky heavy query can't stall for minutes.
    """
    last_error: Exception | None = None
    transient_tries = 0

    def _transient_sleep() -> bool:
        nonlocal transient_tries
        transient_tries += 1
        if transient_tries > max_transient_retries:
            return False
        time.sleep(min(15.0, 2.0 * transient_tries))
        return True

    for attempt in range(max_retries):
        acquire_slot()
        try:
            resp = client.post(
                API_URL,
                headers={"Authorization": f"Bearer {auth_token}", "Content-Type": "application/json"},
                json={"query": query, "variables": variables},
                timeout=60,
            )
        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as e:
            last_error = e
            if not _transient_sleep():
                break
            continue
        if resp.status_code == 429:
            last_error = requests.exceptions.HTTPError("429 Rate limited", response=resp)
            sleep_after_429(attempt, resp)
            continue
        # 5xx: Cloudflare 520, origin 502/503, etc. - bounded retry with backoff
        if 500 <= resp.status_code < 600:
            try:
                resp.raise_for_status()
            except requests.exceptions.HTTPError as e:
                last_error = e
            if not _transient_sleep():
                break
            continue
        try:
            resp.raise_for_status()
        except requests.exceptions.HTTPError as e:
            last_error = e
            if resp.status_code == 429:
                sleep_after_429(attempt, resp)
                continue
            raise
        try:
            data = resp.json()
        except ValueError as e:
            last_error = e
            if not _transient_sleep():
                break
            continue
        if data.get("success") is False and is_likely_rate_limit_error(data, ""):
            last_error = RuntimeError(str(data.get("message") or data))
            sleep_after_429(attempt, resp)
            continue
        if "errors" in data:
            err_txt = str(data["errors"])
            if is_likely_rate_limit_error(data, err_txt):
                last_error = RuntimeError(err_txt)
                sleep_after_429(attempt, resp)
                continue
            if is_transient_server_error(data, err_txt):
                # start.gg internal hiccup on a heavy query: short bounded retry.
                last_error = RuntimeError(err_txt)
                if not _transient_sleep():
                    break
                continue
            raise RuntimeError(f"GraphQL errors: {data['errors']}")
        return data
    raise RuntimeError(f"Request failed (transient_tries={transient_tries}): {last_error}") from last_error


def _get_event_id(
    client: requests.Session,
    limiter: RateLimiter,
    event_slug: str,
    auth_token: str,
) -> str | None:
    """Fetch event ID from start.gg by slug. Slug format: tournament/xxx/event/yyy"""
    data = _gql_post(client, limiter, EVENT_ID_QUERY, {"slug": event_slug}, auth_token)
    event = data.get("data", {}).get("event")
    if not event:
        return None
    return str(event.get("id", ""))


def _get_set_ids_for_event(
    client: requests.Session,
    limiter: RateLimiter,
    event_id: str,
    auth_token: str,
    per_page: int = 40,
) -> list[str]:
    """Fetch all set IDs for an event (paginated)."""
    per_page = min(per_page, MAX_OBJECTS_PER_REQUEST // 10)  # cap for nested objects
    set_ids: list[str] = []
    page = 1
    while True:
        data = _gql_post(
            client, limiter, SETS_PAGE_QUERY,
            {"eventId": event_id, "page": page, "perPage": per_page},
            auth_token,
        )
        nodes = data.get("data", {}).get("event", {}).get("sets", {}).get("nodes", [])
        page_info = data.get("data", {}).get("event", {}).get("sets", {}).get("pageInfo", {})
        for n in nodes:
            sid = n.get("id")
            if sid:
                set_ids.append(str(sid))
        total_pages = page_info.get("totalPages", 1)
        if page >= total_pages or not nodes:
            break
        page += 1
    return set_ids


def _safe_score(slot: dict[str, Any]) -> Any:
    """Extract score from slot; returns None if standing/stats/score missing or null."""
    standing = slot.get("standing") or {}
    stats = standing.get("stats") or {}
    score = stats.get("score") or {}
    return score.get("value")


def _get_players_and_score(
    client: requests.Session,
    limiter: RateLimiter,
    set_id: str,
    auth_token: str,
) -> dict[str, Any] | None:
    """Fetch player names and scores for a set. Returns {p1_name: p1_score, p2_name: p2_score} or None."""
    data = _gql_post(client, limiter, SET_PLAYERS_SCORE_QUERY, {"setId": set_id}, auth_token)
    slots = data.get("data", {}).get("set", {}).get("slots", [])
    if not slots or len(slots) < 2 or any(s.get("entrant") is None for s in slots):
        return None
    try:
        p1_name = slots[0]["entrant"]["participants"][0]["player"]["gamerTag"]
        p1_pre = slots[0]["entrant"]["participants"][0]["player"].get("prefix") or ""
        p1_score = _safe_score(slots[0])

        p2_name = slots[1]["entrant"]["participants"][0]["player"]["gamerTag"]
        p2_pre = slots[1]["entrant"]["participants"][0]["player"].get("prefix") or ""
        p2_score = _safe_score(slots[1])

        p1_full = p1_name if not p1_pre else f"{p1_pre} | {p1_name}"
        p2_full = p2_name if not p2_pre else f"{p2_pre} | {p2_name}"

        return {p1_full: p1_score, p2_full: p2_score}
    except (KeyError, IndexError, TypeError):
        return None


# --- Folded sets fetching (the fast path) ---


def _full_name(slot: dict[str, Any]) -> str | None:
    """Build the 'PREFIX | gamerTag' display name from a set slot (or None)."""
    try:
        player = slot["entrant"]["participants"][0]["player"]
    except (KeyError, IndexError, TypeError):
        return None
    tag = player.get("gamerTag")
    if not tag:
        return None
    prefix = player.get("prefix") or ""
    return tag if not prefix else f"{prefix} | {tag}"


def _extract_set_record(node: dict[str, Any]) -> tuple[str, str, str, Any, Any] | None:
    """
    Convert one folded set node into ``(set_id, p1_name, p2_name, p1_score, p2_score)``.

    Returns ``None`` (caller skips & does not cache) for the same cases the legacy
    per-set path skipped: preview sets, fewer than 2 slots, a missing entrant, an
    unresolved player, or a null score on either side. This guarantees the
    contents of ``sets_cache`` (and therefore ELO output) are identical to before.
    """
    set_id = node.get("id")
    if set_id is None:
        return None
    set_id = str(set_id)
    if set_id.startswith("preview_"):
        return None
    slots = node.get("slots") or []
    if len(slots) < 2 or any(s.get("entrant") is None for s in slots[:2]):
        return None
    p1_name = _full_name(slots[0])
    p2_name = _full_name(slots[1])
    if p1_name is None or p2_name is None:
        return None
    p1_score = _safe_score(slots[0])
    p2_score = _safe_score(slots[1])
    if p1_score is None or p2_score is None:
        return None
    return set_id, p1_name, p2_name, p1_score, p2_score


@dataclass
class EventSetsResult:
    """Outcome of fetching one event's sets via the folded query."""

    event_slug: str
    event_id: str | None = None
    event_name: str = ""
    records: list[tuple[str, str, str, Any, Any]] = field(default_factory=list)
    total_sets: int = 0       # pageInfo.total (incl. skipped preview/incomplete)
    requests: int = 0         # approximate page requests issued for this event
    error: str | None = None


async def _fetch_event_meta_and_sets_async(
    client: AsyncStartGGClient,
    event_slug: str,
    *,
    per_page: int,
    page_concurrency: int,
) -> EventSetsResult:
    """
    Fetch event id/name + all sets in one paginated sweep.

    Page 1 carries event ``id``/``name`` alongside the sets connection, so a single
    query family yields everything; no separate event-id round-trip is needed.
    """
    res = EventSetsResult(event_slug=event_slug)
    safe_pp = _safe_sets_per_page(per_page)
    cur_pp = safe_pp
    while True:
        try:
            first = await client.gql(
                SETS_WITH_PLAYERS_QUERY, {"slug": event_slug, "page": 1, "perPage": cur_pp}
            )
        except ComplexityLimitError:
            if cur_pp <= 8:
                res.error = "complexity-limit even at perPage<=8"
                return res
            cur_pp = max(8, cur_pp // 2)
            continue
        except RuntimeError as e:
            res.error = str(e)
            return res
        break

    res.requests = 1
    event_obj = (first.get("data", {}) or {}).get("event") or {}
    if not event_obj:
        res.error = "event not found"
        return res
    res.event_id = str(event_obj.get("id") or "") or None
    res.event_name = str(event_obj.get("name") or "")
    sets_block = event_obj.get("sets") or {}
    nodes = list(sets_block.get("nodes", []) or [])
    page_info = sets_block.get("pageInfo") or {}
    total_pages = int(page_info.get("totalPages") or 1)
    res.total_sets = int(page_info.get("total") or len(nodes) or 0)

    if total_pages > 1:
        async def _fetch_page(p: int) -> list[dict[str, Any]]:
            payload = await client.gql(
                SETS_WITH_PLAYERS_QUERY, {"slug": event_slug, "page": p, "perPage": cur_pp}
            )
            block = ((payload.get("data", {}) or {}).get("event") or {}).get("sets") or {}
            return list(block.get("nodes", []) or [])

        rest = await gather_bounded(
            [(lambda p=p: _fetch_page(p)) for p in range(2, total_pages + 1)],
            concurrency=page_concurrency,
            return_exceptions=True,
        )
        res.requests += total_pages - 1
        for page_nodes in rest:
            if isinstance(page_nodes, Exception):
                res.error = res.error or f"page error: {page_nodes}"
                continue
            nodes.extend(page_nodes)

    for node in nodes:
        rec = _extract_set_record(node)
        if rec is not None:
            res.records.append(rec)
    return res


async def _fetch_events_sets_async(
    event_slugs: list[str],
    token: str,
    *,
    per_page: int,
    event_concurrency: int,
    progress_cb: Callable[[int, int], None] | None = None,
) -> dict[str, EventSetsResult]:
    """
    Concurrently fetch sets for many events. Per-event pagination uses a small
    inner concurrency; the outer semaphore bounds how many events run at once.
    The shared rate gate keeps the *aggregate* request rate under the per-key cap.
    """
    results: dict[str, EventSetsResult] = {}
    total = len(event_slugs)
    done = 0
    lock = asyncio.Lock()
    # Split the budget: more events in flight, fewer pages each, to keep latency low.
    page_concurrency = max(2, event_concurrency // 2)

    async with AsyncStartGGClient(token) as client:
        async def _one(slug: str) -> EventSetsResult:
            nonlocal done
            r = await _fetch_event_meta_and_sets_async(
                client, slug, per_page=per_page, page_concurrency=page_concurrency
            )
            async with lock:
                done += 1
                if progress_cb is not None:
                    progress_cb(done, total)
            return r

        gathered = await gather_bounded(
            [(lambda s=s: _one(s)) for s in event_slugs],
            concurrency=event_concurrency,
            return_exceptions=True,
        )

    for slug, r in zip(event_slugs, gathered):
        if isinstance(r, Exception):
            results[slug] = EventSetsResult(event_slug=slug, error=str(r))
        else:
            results[slug] = r
    return results


def fetch_events_sets(
    event_slugs: list[str],
    token: str,
    *,
    per_page: int = DEFAULT_SETS_PER_PAGE,
    concurrency: int = DEFAULT_CONCURRENCY,
    use_async: bool = DEFAULT_USE_ASYNC,
    progress_cb: Callable[[int, int], None] | None = None,
) -> dict[str, EventSetsResult]:
    """
    Fetch sets (with players + scores) for the given event slugs.

    Synchronous entry point usable from the threaded job system. Uses the
    concurrent asyncio engine when ``use_async`` is set, otherwise a sequential
    folded fallback that still issues only ~ceil(sets/perPage) requests per event.
    """
    if not event_slugs:
        return {}
    if use_async:
        return run_async(
            _fetch_events_sets_async(
                event_slugs,
                token,
                per_page=per_page,
                event_concurrency=concurrency,
                progress_cb=progress_cb,
            )
        )
    return _fetch_events_sets_sync(
        event_slugs, token, per_page=per_page, progress_cb=progress_cb
    )


def _fetch_events_sets_sync(
    event_slugs: list[str],
    token: str,
    *,
    per_page: int,
    progress_cb: Callable[[int, int], None] | None = None,
) -> dict[str, EventSetsResult]:
    """Sequential fallback using the same folded query (no asyncio/httpx)."""
    client = requests.Session()
    limiter = RateLimiter()
    out: dict[str, EventSetsResult] = {}
    total = len(event_slugs)
    safe_pp = _safe_sets_per_page(per_page)

    def _fetch_one(slug: str) -> EventSetsResult:
        cur_pp = safe_pp
        while True:
            res = EventSetsResult(event_slug=slug)
            page = 1
            try:
                while True:
                    data = _gql_post(
                        client, limiter, SETS_WITH_PLAYERS_QUERY,
                        {"slug": slug, "page": page, "perPage": cur_pp}, token,
                    )
                    event_obj = (data.get("data", {}) or {}).get("event") or {}
                    if not event_obj:
                        res.error = "event not found"
                        return res
                    if res.event_id is None:
                        res.event_id = str(event_obj.get("id") or "") or None
                        res.event_name = str(event_obj.get("name") or "")
                    sets_block = event_obj.get("sets") or {}
                    nodes = list(sets_block.get("nodes", []) or [])
                    page_info = sets_block.get("pageInfo") or {}
                    total_pages = int(page_info.get("totalPages") or 1)
                    if page == 1:
                        res.total_sets = int(page_info.get("total") or len(nodes) or 0)
                    res.requests += 1
                    for node in nodes:
                        rec = _extract_set_record(node)
                        if rec is not None:
                            res.records.append(rec)
                    if page >= total_pages or not nodes:
                        return res
                    page += 1
            except RuntimeError as e:
                if is_complexity_error(str(e)) and cur_pp > 8:
                    cur_pp = max(8, cur_pp // 2)  # halve perPage and restart this event
                    continue
                res.error = str(e)
                return res

    for i, slug in enumerate(event_slugs, 1):
        out[slug] = _fetch_one(slug)
        if progress_cb is not None:
            progress_cb(i, total)
    return out


# --- Name Merging ---


def _apply_name_mappings(
    sets: list[dict[str, Any]],
    mappings: dict[str, str],
) -> list[dict[str, Any]]:
    """Apply canonical name mappings to sets. mappings: alias -> canonical."""
    out = []
    for s in sets:
        if "Error" in s or len(s) < 2:
            continue
        scores = list(s.values())
        if any(sc is None for sc in scores):
            continue
        new_s = {}
        for name, score in s.items():
            canonical = mappings.get(name, name)
            new_s[canonical] = score
        if len(new_s) == 2:
            out.append(new_s)
    return out


# --- Main Processor ---


@dataclass
class ProcessorStats:
    event_hits: int = 0
    event_misses: int = 0
    event_api_errors: int = 0
    set_hits: int = 0
    set_misses: int = 0
    set_api_errors: int = 0

    @property
    def total_events(self) -> int:
        return self.event_hits + self.event_misses

    @property
    def total_sets(self) -> int:
        return self.set_hits + self.set_misses


def _load_events_from_tournament_cache(
    conn: sqlite3.Connection,
    config: ProcessorConfig,
) -> list[tuple[str, str, str, int | None]]:
    """Load events from cache. Returns [(event_slug, tournament_id, event_name, tournament_updated_at), ...]"""
    # `updated_at` is added by a scraper migration; tolerate older caches.
    has_updated = any(
        row[1] == "updated_at"
        for row in conn.execute("PRAGMA table_info(tournaments)").fetchall()
    )
    updated_col = "updated_at" if has_updated else "NULL AS updated_at"

    if config.include_event_slugs is not None:
        # Explicit selection (web PR Maker): the user already chose the events,
        # so skip the date/game/entrants filter and look the slugs up directly.
        slugs = [s for s in config.include_event_slugs if s]
        rows = []
        for i in range(0, len(slugs), 500):  # stay under SQLite's variable limit
            chunk = slugs[i:i + 500]
            placeholders = ",".join("?" for _ in chunk)
            rows.extend(conn.execute(
                f"""
                SELECT event_slug, tournament_id, name, {updated_col}
                FROM tournaments
                WHERE event_slug IN ({placeholders})
                ORDER BY start_at
                """,
                chunk,
            ).fetchall())
        # Preserve the caller's selection order for events found in the cache.
        by_slug = {r[0]: r for r in rows}
        rows = [by_slug[s] for s in slugs if s in by_slug]
    else:
        after = _date_to_unix(config.start_date)
        before = _date_to_unix(config.end_date)
        if after is None or before is None:
            raise ValueError("Invalid start_date or end_date")
        cur = conn.execute(
            f"""
            SELECT event_slug, tournament_id, name, {updated_col}
            FROM tournaments
            WHERE start_at >= ? AND start_at <= ?
            AND videogame_name = ? AND event_num_entrants >= ?
            ORDER BY start_at
            """,
            (after, before, config.game_filter, config.min_entrants),
        )
        rows = cur.fetchall()
    # Dedupe by event_slug
    seen: set[str] = set()
    out = []
    for row in rows:
        slug = row[0]
        if slug and slug not in seen:
            seen.add(slug)
            upd = int(row[3]) if row[3] is not None else None
            out.append((slug, row[1] or "", row[2] or "", upd))
    return out


def _load_dirty_event_slugs(conn: sqlite3.Connection) -> set[str]:
    """Event slugs the scraper flagged as new/changed since last processing."""
    try:
        cur = conn.execute("SELECT event_slug FROM dirty_events")
    except sqlite3.OperationalError:
        return set()
    return {row[0] for row in cur.fetchall() if row[0]}


@dataclass
class ProcessingResult:
    """Everything callers (CLI + web job wrapper) need from one processing run."""

    mapped_sets: list[dict[str, Any]]
    stats: ProcessorStats
    total_events: int
    estimate: dict[str, Any] | None = None  # populated only in dry-run mode


def _classify_events(
    events_to_process: list[tuple[str, str, str, int | None]],
    *,
    dirty_slugs: set[str],
    processed_updated_at: dict[str, int | None],
) -> tuple[list[tuple], list[tuple]]:
    """
    Split events into (hits, misses) preserving order.

    A *hit* was already processed and is unchanged → served from cache.
    A *miss* is new, dirty (scraper flagged a change), or whose tournament's
    ``updatedAt`` advanced past what we last processed → needs an API fetch.
    """
    processed_event_slugs = set(processed_updated_at.keys())
    hits: list[tuple] = []
    misses: list[tuple] = []
    for ev in events_to_process:
        event_slug, _tid, _name, tournament_updated_at = ev
        is_dirty = event_slug in dirty_slugs
        prev_updated = processed_updated_at.get(event_slug)
        changed = (
            tournament_updated_at is not None
            and prev_updated is not None
            and tournament_updated_at > prev_updated
        )
        if event_slug in processed_event_slugs and not is_dirty and not changed:
            hits.append(ev)
        else:
            misses.append((*ev, is_dirty or changed))
    return hits, misses


def _build_estimate(
    *,
    num_miss_events: int,
    per_page: int,
    config: ProcessorConfig,
) -> dict[str, Any]:
    """Project request count + wall time for a dry run (no network calls)."""
    metrics = get_metrics()
    safe_pp = _safe_sets_per_page(per_page)
    pages_per_event = max(1, (ESTIMATE_SETS_PER_EVENT + safe_pp - 1) // safe_pp)
    projected_requests = num_miss_events * pages_per_event
    effective_rpm = min(metrics["max_rpm"], 60.0 / max(0.001, metrics["min_interval_sec"]))
    projected_minutes = projected_requests / max(1.0, effective_rpm)
    return {
        "miss_events": num_miss_events,
        "sets_per_page": safe_pp,
        "assumed_sets_per_event": ESTIMATE_SETS_PER_EVENT,
        "pages_per_event": pages_per_event,
        "projected_requests": projected_requests,
        "effective_rpm": round(effective_rpm, 1),
        "projected_minutes": round(projected_minutes, 2),
        "concurrency": config.concurrency,
    }


def _run_processing(
    config: ProcessorConfig,
    token: str,
    *,
    verbose: bool,
    progress_cb: Callable[[int, int], None] | None = None,
) -> ProcessingResult:
    """
    Shared processing core used by both :func:`process_tournaments` (CLI/demo) and
    the web job wrapper. Fetches sets for cache-miss events using the folded query
    via the concurrent engine, then writes the cache and rebuilds processed_sets.
    """
    _ensure_dir(config.tournament_cache_path)
    _ensure_dir(config.processed_cache_path)
    if not config.tournament_cache_path.exists():
        raise FileNotFoundError(
            f"Tournament cache not found: {config.tournament_cache_path}. "
            "Run tournament_scraper.py first."
        )

    tconn = connect_db(config.tournament_cache_path)
    events_to_process = _load_events_from_tournament_cache(tconn, config)
    dirty_slugs = _load_dirty_event_slugs(tconn)
    tconn.close()

    total_events = len(events_to_process)
    pconn = connect_db(config.processed_cache_path)
    _init_processed_db(pconn)

    cur = pconn.execute("SELECT event_slug, tournament_updated_at FROM processed_events")
    processed_updated_at: dict[str, int | None] = {}
    for row in cur.fetchall():
        processed_updated_at[row[0]] = int(row[1]) if row[1] is not None else None

    hits, misses = _classify_events(
        events_to_process, dirty_slugs=dirty_slugs, processed_updated_at=processed_updated_at
    )
    stats = ProcessorStats()
    safe_pp = _safe_sets_per_page(config.sets_per_page)

    if verbose:
        print(f"[CONFIG] Tournament cache: {config.tournament_cache_path}")
        print(f"[CONFIG] Processed cache: {config.processed_cache_path}")
        print(f"[CONFIG] Date range: {config.start_date} -> {config.end_date}")
        print(f"[CONFIG] Game: {config.game_filter!r}, min_entrants: {config.min_entrants}")
        print(f"[CONFIG] sets perPage={safe_pp} (max safe {SETS_MAX_PER_PAGE}), "
              f"concurrency={config.concurrency}, async={config.use_async}")
        print(f"[INPUT] Events: {total_events} total | cache hits={len(hits)} | to fetch={len(misses)}")

    if config.dry_run:
        estimate = _build_estimate(num_miss_events=len(misses), per_page=safe_pp, config=config)
        if verbose:
            print("\n[DRY RUN] No API calls issued. Projection:")
            print(f"  Events needing fetch:    {estimate['miss_events']}")
            print(f"  sets perPage:            {estimate['sets_per_page']}")
            print(f"  assumed sets/event:      {estimate['assumed_sets_per_event']}")
            print(f"  projected requests:      ~{estimate['projected_requests']}")
            print(f"  effective rate:          ~{estimate['effective_rpm']} req/min")
            print(f"  projected wall time:     ~{estimate['projected_minutes']} min")
        pconn.close()
        return ProcessingResult(mapped_sets=[], stats=stats, total_events=total_events, estimate=estimate)

    # Drop stale cached sets for changed/dirty events up front so a re-fetch fully
    # replaces them (handles edited results / DQ reversals).
    for ev in misses:
        event_slug = ev[0]
        is_changed = ev[4]
        if is_changed:
            pconn.execute("DELETE FROM sets_cache WHERE event_slug = ?", (event_slug,))
            pconn.execute("DELETE FROM processed_sets WHERE event_slug = ?", (event_slug,))
    pconn.commit()

    # Progress: hits resolve instantly; misses resolve as the concurrent fetch
    # completes. Translate the fetch's (done, total_miss) into overall progress.
    done_base = len(hits)
    if progress_cb is not None:
        progress_cb(min(done_base, total_events), total_events)

    def _fetch_progress(done_miss: int, _total_miss: int) -> None:
        if progress_cb is not None:
            progress_cb(min(done_base + done_miss, total_events), total_events)

    fetch_start = time.time()
    req_before = get_request_count()
    miss_slugs = [ev[0] for ev in misses]
    fetched: dict[str, EventSetsResult] = {}
    if miss_slugs:
        if verbose:
            print(f"\n[FETCH] Fetching sets for {len(miss_slugs)} event(s) "
                  f"({'concurrent' if config.use_async else 'sequential'}) ...")
        fetched = fetch_events_sets(
            miss_slugs,
            token,
            per_page=safe_pp,
            concurrency=config.concurrency,
            use_async=config.use_async,
            progress_cb=_fetch_progress,
        )
    fetch_elapsed = time.time() - fetch_start
    fetch_requests = get_request_count() - req_before

    # Assemble results in original event order so all_sets / processed_sets stay
    # deterministic (ELO itself reads sets_cache ordered by tournament/set id).
    all_sets: list[dict[str, Any]] = []
    consumed_dirty: set[str] = set()
    now_ts = int(time.time())

    # Batch-load every cache-hit event's sets in a handful of IN queries instead
    # of one SELECT per event (was an N+1 over hundreds of events on big windows).
    hit_slugs = {ev[0] for ev in hits}
    hit_sets_by_slug: dict[str, list[tuple]] = {}
    hit_slug_list = sorted(hit_slugs)
    for i in range(0, len(hit_slug_list), 500):
        chunk = hit_slug_list[i:i + 500]
        placeholders = ",".join("?" for _ in chunk)
        for row in pconn.execute(
            f"SELECT event_slug, p1_name, p2_name, p1_score, p2_score "
            f"FROM sets_cache WHERE event_slug IN ({placeholders})",
            chunk,
        ).fetchall():
            hit_sets_by_slug.setdefault(row[0], []).append(row[1:])

    for ev in events_to_process:
        event_slug, tournament_id, event_name, tournament_updated_at = ev
        if event_slug in hit_slugs:
            stats.event_hits += 1
            for p1_name, p2_name, p1_score, p2_score in hit_sets_by_slug.get(event_slug, []):
                if p1_score is not None and p2_score is not None:
                    stats.set_hits += 1
                    all_sets.append({p1_name: p1_score, p2_name: p2_score})
            continue

        stats.event_misses += 1
        res = fetched.get(event_slug)
        if res is None or res.error is not None:
            stats.event_api_errors += 1
            if verbose:
                why = res.error if res else "no result"
                print(f"  [API ERROR] {event_slug!r} - {why} (skipping event)")
            continue
        if res.total_sets <= 0:
            # Event genuinely has no sets yet; leave unmarked so we retry later.
            continue

        to_insert: list[tuple] = []
        for set_id, p1, p2, s1, s2 in res.records:
            stats.set_misses += 1
            all_sets.append({p1: s1, p2: s2})
            to_insert.append((set_id, res.event_id, event_slug, p1, p2, s1, s2, now_ts))

        pconn.execute(
            "INSERT OR REPLACE INTO processed_events "
            "(event_slug, event_id, tournament_id, event_name, processed_at, tournament_updated_at) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (event_slug, res.event_id, tournament_id, event_name, now_ts, tournament_updated_at),
        )
        if to_insert:
            pconn.executemany(
                "INSERT OR REPLACE INTO sets_cache "
                "(set_id, event_id, event_slug, p1_name, p2_name, p1_score, p2_score, cached_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                to_insert,
            )
        # Commit periodically (not per event) — keeps mid-run progress durable
        # without paying one fsync per event on large windows.
        if stats.event_misses % 10 == 0:
            pconn.commit()
        if event_slug in dirty_slugs:
            consumed_dirty.add(event_slug)
    pconn.commit()

    mapped_sets = _apply_name_mappings(all_sets, config.name_mappings)

    proc_rows: list[tuple] = []
    for i, s in enumerate(mapped_sets):
        names = list(s.keys())
        scores = list(s.values())
        if len(names) == 2 and len(scores) == 2:
            proc_rows.append((f"proc_{i}", "", names[0], names[1], scores[0], scores[1], now_ts))
    pconn.execute("DELETE FROM processed_sets")
    pconn.executemany(
        "INSERT INTO processed_sets (set_id, event_slug, p1_canonical, p2_canonical, p1_score, p2_score, created_at) "
        "VALUES (?, ?, ?, ?, ?, ?, ?)",
        proc_rows,
    )
    pconn.commit()

    if verbose:
        _print_processed_db_head(pconn, limit=5)
        eff_rate = (fetch_requests / fetch_elapsed * 60.0) if fetch_elapsed > 0 else 0.0
        hit_pct = (stats.event_hits / total_events * 100.0) if total_events else 0.0
        print("\n" + "-" * 80)
        print(f"[SUMMARY] Event cache hits:  {stats.event_hits} ({hit_pct:.0f}% of events)")
        print(f"[SUMMARY] Event cache misses: {stats.event_misses}")
        print(f"[SUMMARY] Set cache hits:    {stats.set_hits}")
        print(f"[SUMMARY] Set cache misses:  {stats.set_misses}")
        if stats.event_api_errors:
            print(f"[SUMMARY] Event API errors (skipped): {stats.event_api_errors}")
        print(f"[METRICS] Fetch phase: {fetch_requests} requests in {fetch_elapsed:.1f}s "
              f"(~{eff_rate:.0f} req/min)")
        print(f"[SUMMARY] Total sets (after name mapping): {len(mapped_sets)}")
        print("-" * 80)

    pconn.close()

    if progress_cb is not None:
        progress_cb(total_events, total_events)

    if consumed_dirty:
        try:
            tconn2 = connect_db(config.tournament_cache_path)
            tconn2.executemany(
                "DELETE FROM dirty_events WHERE event_slug = ?",
                [(s,) for s in consumed_dirty],
            )
            tconn2.commit()
            tconn2.close()
        except sqlite3.OperationalError:
            pass

    return ProcessingResult(mapped_sets=mapped_sets, stats=stats, total_events=total_events)


def process_tournaments(
    config: ProcessorConfig | None = None,
    auth_token: str | None = None,
    *,
    verbose: bool = True,
) -> tuple[list[dict[str, Any]], ProcessorStats]:
    """
    Process tournaments from the scraper cache: fetch sets, apply name mappings.

    Sets are fetched with the folded query (players + scores inline) via the
    concurrent engine, sharing the global rate budget. Returns:
        (list of {p1_canonical: score, p2_canonical: score}, stats)
    """
    config = config or ProcessorConfig()
    token = auth_token or os.environ.get("STARTGG_API_KEY")
    if not token:
        raise ValueError("STARTGG_API_KEY must be set or passed as auth_token")

    result = _run_processing(config, token, verbose=verbose)
    return result.mapped_sets, result.stats


def _print_processed_db_head(conn: sqlite3.Connection, limit: int = 5) -> None:
    """Print head of processed_sets table."""
    cur = conn.execute(
        "SELECT set_id, p1_canonical, p2_canonical, p1_score, p2_score FROM processed_sets LIMIT ?",
        (limit,),
    )
    rows = cur.fetchall()
    print("\n" + "=" * 80)
    print(f"[DB] HEAD of processed_sets table (first {limit} rows):")
    print("-" * 80)
    for i, row in enumerate(rows, 1):
        print(f"  Row {i}: {row[1]} {row[3]} - {row[2]} {row[4]}")
    print("=" * 80 + "\n")


# --- CLI ---


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(
        description="Process tournaments from scraper cache: fetch sets, apply name mappings"
    )
    parser.add_argument("--start", default="2025-04-01", help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end", default="2025-06-30", help="End date (YYYY-MM-DD)")
    parser.add_argument("--game", default="Super Smash Bros. Ultimate", help="Game filter")
    parser.add_argument("--min-entrants", type=int, default=16, help="Min entrants")
    parser.add_argument("--tournament-cache", default=None, help="Path to tournament_cache.db")
    parser.add_argument("--processed-cache", default=None, help="Path to processed_tournament.db")
    parser.add_argument("--name-mappings", default=None, help="JSON file with alias->canonical mappings")
    parser.add_argument("--per-page", type=int, default=None,
                        help=f"Sets per page (clamped to safe max {SETS_MAX_PER_PAGE}); default {DEFAULT_SETS_PER_PAGE}")
    parser.add_argument("--concurrency", type=int, default=None,
                        help=f"Max events fetched in parallel; default {DEFAULT_CONCURRENCY}")
    parser.add_argument("--no-async", action="store_true",
                        help="Disable the concurrent engine (sequential folded fetch)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Estimate request count + wall time without issuing API calls")
    parser.add_argument("--quiet", action="store_true", help="Suppress verbose output")
    args = parser.parse_args()

    name_mappings = dict(DEFAULT_NAME_MAPPINGS)
    if args.name_mappings:
        p = Path(args.name_mappings)
        if p.exists():
            with open(p) as f:
                name_mappings.update(json.load(f))

    config = ProcessorConfig(
        start_date=args.start,
        end_date=args.end,
        game_filter=args.game,
        min_entrants=args.min_entrants,
        tournament_cache_path=Path(args.tournament_cache) if args.tournament_cache else DEFAULT_TOURNAMENT_CACHE,
        processed_cache_path=Path(args.processed_cache) if args.processed_cache else DEFAULT_PROCESSED_CACHE,
        name_mappings=name_mappings,
        sets_per_page=_safe_sets_per_page(args.per_page) if args.per_page else DEFAULT_SETS_PER_PAGE,
        concurrency=args.concurrency if args.concurrency else DEFAULT_CONCURRENCY,
        use_async=not args.no_async,
        dry_run=args.dry_run,
    )
    sets, stats = process_tournaments(config, verbose=not args.quiet)
    print(f"Total processed sets: {len(sets)}")


if __name__ == "__main__":
    main()
