"""
NorCal Tournament Scraper — start.gg API integration with cache and rate limiting.

Features:
- Retrieves tournaments for a configurable timeframe in NorCal (Bay Area, Sacramento)
- Filters by game and minimum entrants
- Persists to SQLite cache; skips re-saving existing records
- Reports cache hits vs misses
- Respects start.gg rate limits (80 req/60s, 1000 objects/request)
"""

from __future__ import annotations

import json
import os
import sqlite3
import time
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Iterator

# Load .env from project root when available
try:
    from dotenv import load_dotenv
    # Project root .env (file lives in demo/base_demo/)
    load_dotenv(Path(__file__).resolve().parent.parent.parent / ".env")
except ImportError:
    pass

import requests

from startgg_rate_gate import (
    acquire_slot,
    get_request_count,
    is_complexity_error,
    is_likely_rate_limit_error,
    sleep_after_429,
)


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


# --- Configuration ---

API_URL = "https://api.start.gg/gql/alpha"
# start.gg rate limits: 80 req/60s, max 1000 objects/request (incl. nested)
RATE_LIMIT_REQUESTS_PER_MIN = 80
RATE_LIMIT_INTERVAL_SEC = 60.0 / RATE_LIMIT_REQUESTS_PER_MIN  # 0.75s min; use 1.0s for margin
MAX_OBJECTS_PER_REQUEST = 1000
# per_page kept ≤100 to stay under 1000 objects (tournaments have nested events).
# The async client halves perPage automatically if a complexity error is ever hit.
DEFAULT_PER_PAGE = 50
# Concurrent fetch settings (the tournament list is less of a bottleneck than
# sets, but parallel pages still help large/multi-region windows).
DEFAULT_CONCURRENCY = _env_int("STARTGG_CONCURRENCY", 6, lo=1, hi=64)
DEFAULT_USE_ASYNC = _env_bool("STARTGG_USE_ASYNC", True)

# Incremental freshness policy. Weeks whose Sunday is within RECENT_WINDOW_DAYS of
# "now" are considered volatile (new events get added, results edited) and are
# re-fetched once their cached coverage is older than RECENT_TTL_SEC. Older weeks
# that have been fetched at least once are treated as immutable and never re-fetched.
RECENT_WINDOW_DAYS = 28
RECENT_TTL_SEC = 12 * 3600

NORCAL_REGIONS: dict[str, tuple[str, str]] = {
    "bay": ("37.77151615492457, -122.41563048985462", "70mi"),
    "sacramento": ("38.57608096237729, -121.49183616631059", "40mi"),
}


def _default_cache_path() -> Path:
    """Single shared cache for all date ranges; overlapping ranges reuse existing data."""
    project_root = Path(__file__).resolve().parent.parent.parent
    return project_root / "data" / "tournament_cache.db"


def connect_db(path: str | Path) -> sqlite3.Connection:
    """
    Open a SQLite connection in WAL mode with relaxed sync.

    WAL lets background warming and user-triggered jobs read/write concurrently
    without blocking each other; synchronous=NORMAL is durable enough for a cache.
    """
    conn = sqlite3.connect(str(path), timeout=30)
    try:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA cache_size=-32000")  # ~32MB page cache for read-heavy loads
        conn.execute("PRAGMA temp_store=MEMORY")
    except sqlite3.DatabaseError:
        pass
    return conn


@dataclass
class ScraperConfig:
    """Configuration for tournament scraping."""

    start_date: str = "2025-04-01"
    end_date: str = "2025-06-30"
    game_filter: str = "Super Smash Bros. Ultimate"
    min_entrants: int = 16
    regions: list[str] = field(default_factory=lambda: ["bay", "sacramento"])
    per_page: int = DEFAULT_PER_PAGE
    cache_path: str | Path | None = None  # None = use shared default cache
    incremental: bool = True  # fetch only weeks missing or recent+stale in cache
    force_refresh: bool = False  # bypass freshness and re-fetch the full range
    concurrency: int = DEFAULT_CONCURRENCY  # parallel (window, region) page fetches
    use_async: bool = DEFAULT_USE_ASYNC     # concurrent engine vs sequential fallback


# --- Rate Limiter ---


class RateLimiter:
    """Enforces start.gg rate limit: 80 requests per 60 seconds."""

    def __init__(self, interval_sec: float | None = None):
        # Use 1.0s for safety margin (60 req/min < 80 limit)
        self.interval = interval_sec if interval_sec is not None else max(1.0, 60.0 / RATE_LIMIT_REQUESTS_PER_MIN)
        self._last_request_time: float = 0.0

    def wait(self) -> None:
        elapsed = time.monotonic() - self._last_request_time
        if elapsed < self.interval:
            time.sleep(self.interval - elapsed)
        self._last_request_time = time.monotonic()


# --- Cache / Database ---


def _ensure_cache_dir(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def _init_cache(conn: sqlite3.Connection) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS tournaments (
            tournament_id TEXT,
            event_slug TEXT,
            name TEXT,
            city TEXT,
            slug TEXT,
            start_at INTEGER,
            event_num_entrants INTEGER,
            videogame_name TEXT,
            raw_json TEXT,
            cached_at INTEGER,
            PRIMARY KEY (tournament_id, event_slug)
        )
    """)
    _migrate_tournaments_columns(conn)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_tournaments_start_at 
        ON tournaments(start_at)
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_tournaments_videogame 
        ON tournaments(videogame_name)
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_tournaments_updated_at
        ON tournaments(updated_at)
    """)
    _ensure_coverage_table(conn)
    _ensure_week_coverage_table(conn)
    _ensure_dirty_events_table(conn)
    _ensure_ineligible_events_table(conn)
    conn.commit()


def _migrate_tournaments_columns(conn: sqlite3.Connection) -> None:
    """Add columns introduced after the original schema (idempotent)."""
    cols = {row[1] for row in conn.execute("PRAGMA table_info(tournaments)").fetchall()}
    if "updated_at" not in cols:
        conn.execute("ALTER TABLE tournaments ADD COLUMN updated_at INTEGER")


def _ensure_week_coverage_table(conn: sqlite3.Connection) -> None:
    """
    Per-week fetch ledger. A row means the week was fully inside a completed fetch
    window at `last_fetched`; `had_data` records whether any matching tournament
    existed. Used to decide which weeks still need an API call.
    """
    conn.execute("""
        CREATE TABLE IF NOT EXISTS week_coverage (
            week_start TEXT NOT NULL,
            game_filter TEXT NOT NULL,
            last_fetched INTEGER NOT NULL,
            had_data INTEGER NOT NULL,
            PRIMARY KEY (week_start, game_filter)
        )
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_week_coverage_game
        ON week_coverage(game_filter)
    """)


def _ensure_dirty_events_table(conn: sqlite3.Connection) -> None:
    """
    Event slugs whose tournament was newly seen or changed (updatedAt advanced)
    since the last scrape. The processor consumes and clears these to know which
    already-processed events must be re-fetched.
    """
    conn.execute("""
        CREATE TABLE IF NOT EXISTS dirty_events (
            event_slug TEXT PRIMARY KEY,
            marked_at INTEGER NOT NULL
        )
    """)


def _ensure_ineligible_events_table(conn: sqlite3.Connection) -> None:
    """
    Event slugs manually flagged ineligible on the PR Maker event selector.
    Stored separately from tournament rows so re-scrapes never wipe the flag.
    """
    conn.execute("""
        CREATE TABLE IF NOT EXISTS ineligible_events (
            event_slug TEXT PRIMARY KEY,
            marked_at INTEGER NOT NULL
        )
    """)


def get_ineligible_event_slugs(conn: sqlite3.Connection) -> set[str]:
    """Return event slugs flagged ineligible for manual parsing."""
    return {row[0] for row in conn.execute("SELECT event_slug FROM ineligible_events").fetchall()}


def set_event_ineligible(conn: sqlite3.Connection, event_slug: str, ineligible: bool) -> None:
    """Mark or clear an event as ineligible. Caller must commit."""
    slug = str(event_slug or "").strip()
    if not slug:
        raise ValueError("event_slug is required")
    if ineligible:
        conn.execute(
            "INSERT OR REPLACE INTO ineligible_events (event_slug, marked_at) VALUES (?, ?)",
            (slug, int(time.time())),
        )
    else:
        conn.execute("DELETE FROM ineligible_events WHERE event_slug = ?", (slug,))


def _ensure_coverage_table(conn: sqlite3.Connection) -> None:
    """
    Weeks listed here were covered by a completed scrape window and had no rows for
    the game in `tournaments` — i.e. confirmed empty, not an unscraped gap.
    """
    conn.execute("""
        CREATE TABLE IF NOT EXISTS scrape_verified_empty_weeks (
            week_start TEXT NOT NULL,
            game_filter TEXT NOT NULL,
            verified_at INTEGER NOT NULL,
            PRIMARY KEY (week_start, game_filter)
        )
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_scrape_verified_empty_weeks_game
        ON scrape_verified_empty_weeks(game_filter)
    """)


def _week_start_monday(d: date) -> date:
    return d - timedelta(days=d.weekday())


def _week_fully_inside_scrape_range(week_monday: date, range_start: date, range_end: date) -> bool:
    """Only mark a week verified if the whole Mon–Sun window was inside the scraped [start,end]."""
    week_sunday = week_monday + timedelta(days=6)
    return week_monday >= range_start and week_sunday <= range_end


def record_verified_empty_weeks_for_scrape_window(
    conn: sqlite3.Connection,
    *,
    range_start: str,
    range_end: str,
    game_filter: str,
) -> None:
    """
    After a successful scrape of [range_start, range_end], record per-week coverage
    for each fully-contained Monday week: a `week_coverage` row (with `last_fetched`
    and whether it had data), plus the legacy verified-empty marker when the week is
    empty. Weeks with coverage are not re-fetched unless recent and stale.
    """
    _ensure_coverage_table(conn)
    _ensure_week_coverage_table(conn)
    start_d = datetime.strptime(range_start, "%Y-%m-%d").date()
    end_d = datetime.strptime(range_end, "%Y-%m-%d").date()
    now = int(time.time())
    cursor = _week_start_monday(start_d)
    end_week = _week_start_monday(end_d)
    while cursor <= end_week:
        if not _week_fully_inside_scrape_range(cursor, start_d, end_d):
            cursor += timedelta(days=7)
            continue
        week_end = cursor + timedelta(days=6)
        after = int(datetime.combine(cursor, datetime.min.time()).timestamp())
        before = int(datetime.combine(week_end, datetime.max.time()).timestamp())
        row = conn.execute(
            """
            SELECT 1 FROM tournaments
            WHERE videogame_name = ?
              AND start_at >= ? AND start_at <= ?
            LIMIT 1
            """,
            (game_filter, after, before),
        ).fetchone()
        ws = cursor.strftime("%Y-%m-%d")
        had_data = 1 if row is not None else 0
        conn.execute(
            """
            INSERT OR REPLACE INTO week_coverage
            (week_start, game_filter, last_fetched, had_data)
            VALUES (?, ?, ?, ?)
            """,
            (ws, game_filter, now, had_data),
        )
        if had_data == 0:
            conn.execute(
                """
                INSERT OR REPLACE INTO scrape_verified_empty_weeks
                (week_start, game_filter, verified_at)
                VALUES (?, ?, ?)
                """,
                (ws, game_filter, now),
            )
        cursor += timedelta(days=7)


# Back-compat alias matching the plan's naming.
record_week_coverage = record_verified_empty_weeks_for_scrape_window


def compute_week_ranges_missing(
    tournament_cache_path: Path,
    *,
    game_filter: str,
    start_date: date,
    end_date: date,
) -> list[tuple[date, date]]:
    """
    Calendar weeks with no cached tournaments for `game_filter` AND no prior scrape
    that confirmed the week had zero such tournaments.

    Weeks only appear here if they are genuinely unscanned (or partially scanned);
    after a full-window scrape with no events, they are recorded in
    `scrape_verified_empty_weeks` and drop out of this list.
    """
    if not tournament_cache_path.exists():
        return [(start_date, end_date)]

    conn = sqlite3.connect(str(tournament_cache_path))
    _ensure_coverage_table(conn)
    rows = conn.execute(
        """
        SELECT DISTINCT date(start_at, 'unixepoch')
        FROM tournaments
        WHERE videogame_name = ?
          AND start_at >= ?
          AND start_at <= ?
        """,
        (
            game_filter,
            int(datetime.combine(start_date, datetime.min.time()).timestamp()),
            int(datetime.combine(end_date, datetime.max.time()).timestamp()),
        ),
    ).fetchall()
    verified_rows = conn.execute(
        """
        SELECT week_start FROM scrape_verified_empty_weeks
        WHERE game_filter = ?
        """,
        (game_filter,),
    ).fetchall()
    conn.close()

    covered_weeks: set[date] = set()
    for (day_str,) in rows:
        if not day_str:
            continue
        d = datetime.strptime(str(day_str), "%Y-%m-%d").date()
        covered_weeks.add(_week_start_monday(d))

    verified_weeks: set[date] = set()
    for (ws,) in verified_rows:
        if not ws:
            continue
        verified_weeks.add(datetime.strptime(str(ws), "%Y-%m-%d").date())

    missing: list[tuple[date, date]] = []
    cursor = _week_start_monday(start_date)
    end_week = _week_start_monday(end_date)
    in_gap = False
    gap_start: date | None = None

    while cursor <= end_week:
        has_data = cursor in covered_weeks
        confirmed_empty = cursor in verified_weeks
        week_ok = has_data or confirmed_empty

        if not week_ok and not in_gap:
            in_gap = True
            gap_start = cursor
        if week_ok and in_gap:
            prev = cursor - timedelta(days=1)
            missing.append((gap_start or start_date, prev))
            in_gap = False
            gap_start = None
        cursor += timedelta(days=7)

    if in_gap:
        missing.append((gap_start or start_date, end_date))
    return missing


def compute_week_ranges_to_fetch(
    tournament_cache_path: Path,
    *,
    game_filter: str,
    start_date: date,
    end_date: date,
    now: int | None = None,
    recent_window_days: int = RECENT_WINDOW_DAYS,
    recent_ttl_sec: int = RECENT_TTL_SEC,
) -> list[tuple[date, date]]:
    """
    Sub-windows within [start_date, end_date] that still need an API fetch.

    A week needs fetching when:
      - it has no `week_coverage` row (never fetched, or only partially scanned), OR
      - it is "recent" (its Sunday falls within `recent_window_days` of now) and its
        coverage is older than `recent_ttl_sec`.

    Older weeks that were fetched at least once are treated as immutable and skipped,
    so re-scraping a fully covered historical range issues zero API requests.
    Contiguous fetch-needing weeks are merged and clamped to the requested range.
    """
    now = int(time.time()) if now is None else now
    if not tournament_cache_path.exists():
        return [(start_date, end_date)]

    today = datetime.fromtimestamp(now).date()
    recent_cutoff = today - timedelta(days=recent_window_days)

    conn = connect_db(tournament_cache_path)
    _ensure_week_coverage_table(conn)
    rows = conn.execute(
        "SELECT week_start, last_fetched FROM week_coverage WHERE game_filter = ?",
        (game_filter,),
    ).fetchall()
    conn.close()

    coverage: dict[date, int] = {}
    for ws, last_fetched in rows:
        if not ws:
            continue
        try:
            coverage[datetime.strptime(str(ws), "%Y-%m-%d").date()] = int(last_fetched or 0)
        except ValueError:
            continue

    to_fetch: list[tuple[date, date]] = []
    cursor = _week_start_monday(start_date)
    end_week = _week_start_monday(end_date)
    in_run = False
    run_start: date | None = None

    while cursor <= end_week:
        week_sunday = cursor + timedelta(days=6)
        last_fetched = coverage.get(cursor)
        is_recent = week_sunday >= recent_cutoff
        if last_fetched is None:
            needs = True
        elif is_recent and (now - last_fetched) > recent_ttl_sec:
            needs = True
        else:
            needs = False

        if needs and not in_run:
            in_run = True
            run_start = cursor
        if not needs and in_run:
            prev_sunday = cursor - timedelta(days=1)
            to_fetch.append((run_start or start_date, prev_sunday))
            in_run = False
            run_start = None
        cursor += timedelta(days=7)

    if in_run:
        to_fetch.append((run_start or start_date, end_date))

    # Clamp the merged windows to the requested [start_date, end_date].
    clamped: list[tuple[date, date]] = []
    for a, b in to_fetch:
        clamped.append((max(a, start_date), min(b, end_date)))
    return clamped


def _tournament_to_row(t: dict[str, Any], event: dict[str, Any] | None) -> tuple:
    """Convert a tournament + event into a cache row."""
    vid = event.get("videogame") or {}
    vid_name = vid.get("name", "") if isinstance(vid, dict) else ""
    return (
        str(t.get("id", "")),
        event.get("slug", "") if event else "",
        t.get("name", ""),
        t.get("city", ""),
        t.get("slug", ""),
        t.get("startAt"),
        event.get("numEntrants") if event else None,
        vid_name,
        json.dumps({"tournament": t, "event": event}),
        int(time.time()),
        t.get("updatedAt"),
    )


def _rows_to_tournaments(rows: list[tuple]) -> list[dict]:
    """Convert cache rows back to tournament dicts for filtering."""
    out = []
    for row in rows:
        raw = json.loads(row[8]) if row[8] else {}
        out.append(raw.get("tournament", {}))
    return out


# --- API ---


def _date_to_unix(date_str: str, fmt: str = "%Y-%m-%d") -> int | None:
    try:
        return int(datetime.strptime(date_str, fmt).timestamp())
    except ValueError:
        return None


def _build_query(after_unix: int, before_unix: int) -> str:
    return f"""
query NorCalTournaments($page: Int, $perPage: Int, $coordinates: String!, $radius: String!) {{
  tournaments(
    query: {{
      page: $page
      perPage: $perPage
      filter: {{
        location: {{
          distanceFrom: $coordinates
          distance: $radius
        }}
        afterDate: {after_unix}
        beforeDate: {before_unix}
      }}
      sortBy: "startAt"
    }}
  ) {{
    pageInfo {{
      total
      totalPages
    }}
    nodes {{
      id
      name
      city
      slug
      startAt
      updatedAt
      events {{
        slug
        numEntrants
        videogame {{
          name
        }}
      }}
    }}
  }}
}}
""".strip()


class ComplexityLimitErrorSync(RuntimeError):
    """start.gg rejected the query for exceeding the object/complexity limit."""


def _fetch_page(
    client: requests.Session,
    limiter: RateLimiter,
    query: str,
    page: int,
    coords: str,
    radius: str,
    per_page: int,
    auth_token: str,
    *,
    max_retries: int = 30,
) -> list[dict]:
    last_error: Exception | None = None
    for attempt in range(max_retries):
        acquire_slot()
        payload = {
            "query": query,
            "variables": {
                "page": page,
                "perPage": per_page,
                "coordinates": coords,
                "radius": radius,
            },
        }
        try:
            resp = client.post(
                API_URL,
                headers={"Authorization": f"Bearer {auth_token}", "Content-Type": "application/json"},
                json=payload,
                timeout=60,
            )
        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as e:
            last_error = e
            time.sleep(min(90.0, 5 * (attempt + 1)))
            continue
        if resp.status_code == 429:
            last_error = requests.exceptions.HTTPError("429 Rate limited", response=resp)
            sleep_after_429(attempt, resp)
            continue
        if 500 <= resp.status_code < 600:
            last_error = RuntimeError(f"HTTP {resp.status_code}")
            time.sleep(min(120.0, 15 * (attempt + 1)))
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
            time.sleep(min(60.0, 5 * (attempt + 1)))
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
            if is_complexity_error(err_txt):
                # Signal the caller to retry with a smaller perPage (mirrors the
                # async path's ComplexityLimitError handling).
                raise ComplexityLimitErrorSync(err_txt)
            raise RuntimeError(f"GraphQL errors: {data['errors']}")
        nodes = data.get("data", {}).get("tournaments", {}).get("nodes", [])
        return nodes
    raise RuntimeError(f"Tournament page fetch failed after {max_retries} retries") from last_error


def _fetch_all_tournaments(
    client: requests.Session,
    limiter: RateLimiter,
    config: ScraperConfig,
    auth_token: str,
    *,
    after: int | None = None,
    before: int | None = None,
    verbose: bool = False,
) -> Iterator[dict]:
    if after is None:
        after = _date_to_unix(config.start_date)
    if before is None:
        before = _date_to_unix(config.end_date)
    if after is None or before is None:
        raise ValueError("Invalid start_date or end_date")
    query = _build_query(after, before)

    for region_key in config.regions:
        if region_key not in NORCAL_REGIONS:
            raise ValueError(f"Unknown region: {region_key}")
        coords, radius = NORCAL_REGIONS[region_key]
        per_page = min(config.per_page, MAX_OBJECTS_PER_REQUEST // 10)  # cap for nesting
        page = 1
        while True:
            if verbose:
                print(f"  [API] Fetching region={region_key!r} page={page} ...")
            try:
                nodes = _fetch_page(
                    client, limiter, query, page, coords, radius, per_page, auth_token
                )
            except ComplexityLimitErrorSync:
                # Event-heavy tournaments can blow the 1000-object budget at the
                # default perPage. Halve and restart this region from page 1 —
                # callers dedupe by tournament id, so re-yields are harmless.
                if per_page <= 8:
                    raise
                per_page = max(8, per_page // 2)
                page = 1
                if verbose:
                    print(f"  [API]   complexity limit hit; retrying region at perPage={per_page}")
                continue
            if verbose:
                print(f"  [API]   -> got {len(nodes)} tournaments")
            if not nodes:
                break
            for t in nodes:
                yield t
            if len(nodes) < per_page:
                break
            page += 1


async def _fetch_window_region_async(
    client: "AsyncStartGGClient",
    query: str,
    *,
    coords: str,
    radius: str,
    per_page: int,
    concurrency: int,
) -> list[dict]:
    """Fetch all tournament-list pages for one (window, region) concurrently."""
    from startgg_async import paginate_async

    nodes, _ = await paginate_async(
        client,
        query,
        {"coordinates": coords, "radius": radius},
        extract_block=lambda p: (p.get("data", {}) or {}).get("tournaments", {}) or {},
        per_page=min(per_page, MAX_OBJECTS_PER_REQUEST // 10),
        concurrency=concurrency,
        min_per_page=8,
    )
    return nodes


async def _fetch_tournaments_async(
    config: ScraperConfig,
    token: str,
    windows_unix: list[tuple[int, int]],
    *,
    verbose: bool = False,
) -> tuple[list[dict], set[int]]:
    """
    Concurrently fetch the tournament list for every (window, region) pair.

    Each pair is an independent paginated query; we run them with bounded
    concurrency, all sharing the global rate budget. Returns a flat list of
    tournament nodes (the caller dedupes by id) plus the set of window indexes
    that had at least one failed region fetch. Failed windows must NOT be
    recorded as covered, otherwise the failed region's tournaments (e.g. all
    Bay Area events) would be permanently missing from those weeks.
    """
    from startgg_async import AsyncStartGGClient, gather_bounded

    tasks: list = []  # (window_idx, query, coords, radius)
    for win_idx, (after, before) in enumerate(windows_unix):
        query = _build_query(after, before)
        for region_key in config.regions:
            if region_key not in NORCAL_REGIONS:
                raise ValueError(f"Unknown region: {region_key}")
            coords, radius = NORCAL_REGIONS[region_key]
            tasks.append((win_idx, query, coords, radius))

    if verbose:
        print(f"  [API] Fetching {len(tasks)} (window,region) pair(s) concurrently ...")

    page_concurrency = max(2, config.concurrency // 2)
    out: list[dict] = []
    failed_windows: set[int] = set()
    async with AsyncStartGGClient(token) as client:
        results = await gather_bounded(
            [
                (lambda q=q, c=c, r=r: _fetch_window_region_async(
                    client, q, coords=c, radius=r,
                    per_page=config.per_page, concurrency=page_concurrency,
                ))
                for (_, q, c, r) in tasks
            ],
            concurrency=config.concurrency,
            return_exceptions=True,
        )
    for (win_idx, _, coords, radius), res in zip(tasks, results):
        if isinstance(res, Exception):
            # Surface a clear error but let the rest of the windows succeed.
            # The window is flagged so its coverage isn't marked complete.
            failed_windows.add(win_idx)
            if verbose:
                print(f"  [API ERROR] tournament-list fetch failed ({coords} @ {radius}): {res}")
            continue
        out.extend(res)
    return out, failed_windows


# --- Flatten & Filter ---


def _flatten_and_filter(
    tournaments: list[dict],
    game_filter: str,
    min_entrants: int,
) -> list[dict]:
    """Explode events, flatten, and filter by game and min entrants."""
    out = []
    for t in tournaments:
        events = t.get("events") or []
        for ev in events:
            vid = ev.get("videogame") or {}
            game = vid.get("name", "") if isinstance(vid, dict) else ""
            entrants = ev.get("numEntrants") or 0
            if game == game_filter and entrants >= min_entrants:
                out.append({"tournament": t, "event": ev})
    return out


# --- Main Scraper ---


@dataclass
class CacheStats:
    hits: int = 0
    misses: int = 0

    @property
    def total(self) -> int:
        return self.hits + self.misses


def _get_cached_tournament_ids(conn: sqlite3.Connection, after: int, before: int) -> set[str]:
    cur = conn.execute(
        "SELECT DISTINCT tournament_id FROM tournaments WHERE start_at >= ? AND start_at <= ?",
        (after, before),
    )
    return {row[0] for row in cur.fetchall()}


def _get_cached_updated_at(conn: sqlite3.Connection, after: int, before: int) -> dict[str, int | None]:
    """Map tournament_id -> cached updated_at for the range (None if unknown)."""
    cur = conn.execute(
        "SELECT tournament_id, MAX(updated_at) FROM tournaments "
        "WHERE start_at >= ? AND start_at <= ? GROUP BY tournament_id",
        (after, before),
    )
    out: dict[str, int | None] = {}
    for tid, upd in cur.fetchall():
        out[str(tid)] = int(upd) if upd is not None else None
    return out


def _is_changed(cached_updated_at: int | None, fetched_updated_at: Any) -> bool:
    """True if a tournament is new/changed and should be (re)inserted + marked dirty."""
    if cached_updated_at is None:
        return True
    try:
        return int(fetched_updated_at) > int(cached_updated_at)
    except (TypeError, ValueError):
        return True


def _print_db_head(conn: sqlite3.Connection, limit: int = 5) -> None:
    """Print first N rows of tournaments table for debugging."""
    cur = conn.execute(
        "SELECT tournament_id, event_slug, name, city, start_at, videogame_name, event_num_entrants FROM tournaments ORDER BY start_at LIMIT ?",
        (limit,),
    )
    rows = cur.fetchall()
    cols = ["tournament_id", "event_slug", "name", "city", "start_at", "videogame_name", "event_num_entrants"]
    print("\n" + "=" * 80)
    print("[DB] HEAD of tournaments table (first %d rows):" % limit)
    print("-" * 80)
    for i, row in enumerate(rows, 1):
        print(f"  Row {i}:")
        for c, v in zip(cols, row):
            print(f"    {c}: {v}")
    print("=" * 80 + "\n")


def scrape_tournaments(
    config: ScraperConfig | None = None,
    auth_token: str | None = None,
    *,
    verbose: bool = True,
) -> tuple[list[dict], CacheStats]:
    """
    Fetch NorCal tournaments for the given config, using cache to avoid re-saving.

    Returns:
        (filtered_tournaments, cache_stats)
    """
    config = config or ScraperConfig()
    token = auth_token or os.environ.get("STARTGG_API_KEY")
    if not token:
        raise ValueError("STARTGG_API_KEY must be set or passed as auth_token")

    # Use single shared cache for all runs (overlapping date ranges reuse cache)
    if config.cache_path is None:
        cache_path = _default_cache_path()
        if verbose:
            print(f"[CONFIG] Cache path (shared): {cache_path}")
    else:
        cache_path = Path(config.cache_path)
        if verbose:
            print(f"[CONFIG] Cache path: {cache_path}")

    _ensure_cache_dir(cache_path)
    conn = connect_db(cache_path)
    _init_cache(conn)

    after = _date_to_unix(config.start_date)
    before = _date_to_unix(config.end_date)
    if after is None or before is None:
        raise ValueError("Invalid start_date or end_date")
    start_d = datetime.strptime(config.start_date, "%Y-%m-%d").date()
    end_d = datetime.strptime(config.end_date, "%Y-%m-%d").date()

    cached_updated = _get_cached_updated_at(conn, after, before)
    stats = CacheStats()
    req_before = get_request_count()

    # Decide which sub-windows actually need an API fetch.
    if config.force_refresh or not config.incremental:
        windows: list[tuple[date, date]] = [(start_d, end_d)]
        plan_reason = "force_refresh" if config.force_refresh else "non-incremental"
    else:
        windows = compute_week_ranges_to_fetch(
            cache_path,
            game_filter=config.game_filter,
            start_date=start_d,
            end_date=end_d,
        )
        plan_reason = "incremental"

    if verbose:
        print(f"[CONFIG] Date range: {config.start_date} -> {config.end_date}")
        print(f"[CONFIG] Game filter: {config.game_filter!r}, min_entrants: {config.min_entrants}")
        print(f"[CONFIG] Regions: {config.regions}")
        print(f"[CACHE] Tournaments already cached in this range: {len(cached_updated)}")
        print(f"[PLAN] Mode={plan_reason}; windows to fetch: {len(windows)}")
        for a, b in windows:
            print(f"  [PLAN]   {a.isoformat()} -> {b.isoformat()}")
        if not windows:
            print("[PLAN] Nothing to fetch (cache fresh) - serving from cache.")
        print("\n[FETCH] Querying start.gg API ...")

    client = requests.Session()
    limiter = RateLimiter()

    all_tournaments: list[dict] = []
    to_insert: list[tuple] = []
    dirty_slugs: set[str] = set()
    hit_examples: list[str] = []
    miss_examples: list[str] = []
    seen_tids: set[str] = set()

    # Build the unix windows once; the async path fetches all (window, region)
    # pairs concurrently, the sync fallback walks them sequentially.
    windows_unix = [
        (
            int(datetime.combine(w0, datetime.min.time()).timestamp()),
            int(datetime.combine(w1, datetime.max.time()).timestamp()),
        )
        for (w0, w1) in windows
    ]

    failed_window_idxs: set[int] = set()
    if config.use_async and windows_unix:
        from startgg_async import run_async

        fetched_nodes, failed_window_idxs = run_async(
            _fetch_tournaments_async(config, token, windows_unix, verbose=verbose)
        )
        tournament_stream: Iterator[dict] = iter(fetched_nodes)
    else:
        def _sequential_stream() -> Iterator[dict]:
            for win_after, win_before in windows_unix:
                yield from _fetch_all_tournaments(
                    client, limiter, config, token,
                    after=win_after, before=win_before, verbose=verbose,
                )

        tournament_stream = _sequential_stream()

    for t in tournament_stream:
        tid = str(t.get("id", ""))
        name = t.get("name", "?")
        if tid in seen_tids:
            continue
        seen_tids.add(tid)
        # force_refresh must re-write every fetched tournament even when
        # updatedAt is unchanged. PR Maker "Fresh Scrape" deletes ELO-eligible
        # rows first; skipping unchanged tournaments here would permanently drop
        # those event rows (e.g. Guildhouse singles brackets).
        changed = config.force_refresh or _is_changed(
            cached_updated.get(tid), t.get("updatedAt")
        )
        if not changed:
            stats.hits += 1
            if len(hit_examples) < 5:
                hit_examples.append(f"  [HIT]  id={tid} {name!r} (unchanged, skipped)")
            continue
        stats.misses += 1
        if len(miss_examples) < 5:
            if config.force_refresh and cached_updated.get(tid) is not None:
                label = "force-refresh"
            elif cached_updated.get(tid) is None:
                label = "new"
            else:
                label = "updated"
            miss_examples.append(f"  [MISS] id={tid} {name!r} ({label}, will insert)")
        all_tournaments.append(t)
        for ev in t.get("events") or []:
            row = _tournament_to_row(t, ev)
            to_insert.append(row)
            slug = ev.get("slug") if isinstance(ev, dict) else None
            if slug:
                dirty_slugs.add(str(slug))

    if verbose:
        print("\n[CACHE] Per-tournament decisions:")
        for ex in hit_examples:
            print(ex)
        if stats.hits > 5:
            print(f"  ... and {stats.hits - 5} more HITs (skipped)")
        for ex in miss_examples:
            print(ex)
        if stats.misses > 5:
            print(f"  ... and {stats.misses - 5} more MISSes (inserted)")

    if to_insert:
        if verbose:
            print(f"\n[DB] Inserting {len(to_insert)} rows (tournament+event pairs) ...")
        conn.executemany(
            """
            INSERT OR REPLACE INTO tournaments
            (tournament_id, event_slug, name, city, slug, start_at, event_num_entrants, videogame_name, raw_json, cached_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            to_insert,
        )
        conn.commit()
    else:
        if verbose:
            print("\n[DB] No new rows to insert (all cache hits)")

    if dirty_slugs:
        now_ts = int(time.time())
        conn.executemany(
            "INSERT OR REPLACE INTO dirty_events (event_slug, marked_at) VALUES (?, ?)",
            [(s, now_ts) for s in dirty_slugs],
        )
        conn.commit()
        if verbose:
            print(f"[DELTA] Marked {len(dirty_slugs)} event(s) dirty for reprocessing")

    # Record coverage only for the windows we actually fetched COMPLETELY.
    # A window where any region fetch failed is left uncovered so it gets
    # re-fetched next run; marking it covered with partial (e.g. Sacramento-
    # only) data would permanently drop the other region's tournaments.
    for win_idx, (win_start, win_end) in enumerate(windows):
        if win_idx in failed_window_idxs:
            if verbose:
                print(f"[PLAN] Window {win_start.isoformat()} -> {win_end.isoformat()} had a failed region fetch; "
                      "leaving it uncovered for retry next run")
            continue
        record_verified_empty_weeks_for_scrape_window(
            conn,
            range_start=win_start.isoformat(),
            range_end=win_end.isoformat(),
            game_filter=config.game_filter,
        )
    conn.commit()

    if verbose:
        api_calls = get_request_count() - req_before
        print(f"[METRICS] start.gg requests this scrape: {api_calls}")
        _print_db_head(conn, limit=5)

    conn.close()

    # Apply game/entrants filter to combined cached + fresh
    conn2 = connect_db(cache_path)
    cur = conn2.execute(
        """
        SELECT * FROM tournaments
        WHERE start_at >= ? AND start_at <= ?
        AND videogame_name = ? AND event_num_entrants >= ?
        ORDER BY start_at
        """,
        (after, before, config.game_filter, config.min_entrants),
    )
    rows = cur.fetchall()
    total_rows = conn2.execute("SELECT COUNT(*) FROM tournaments").fetchone()[0]
    if verbose:
        print(f"[DB] Total rows in cache: {total_rows}")
        print(f"[DB] Rows matching filter (game={config.game_filter!r}, min_entrants>={config.min_entrants}): {len(rows)}")
    conn2.close()

    # Build result as list of {tournament, event} for compatibility
    filtered = []
    seen = set()
    for row in rows:
        raw = json.loads(row[8]) if row[8] else {}
        t = raw.get("tournament", {})
        ev = raw.get("event", {})
        key = (t.get("id"), ev.get("slug"))
        if key not in seen:
            seen.add(key)
            filtered.append({"tournament": t, "event": ev})

    if verbose:
        print("\n" + "-" * 40)
        print(f"[SUMMARY] Cache hits:  {stats.hits}")
        print(f"[SUMMARY] Cache misses: {stats.misses}")
        print(f"[SUMMARY] Filtered tournaments returned: {len(filtered)}")
        print("-" * 40)

    return filtered, stats


# --- CLI ---


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(description="Scrape NorCal tournaments from start.gg")
    parser.add_argument("--start", default="2018-12-08", help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end", default="2026-06-30", help="End date (YYYY-MM-DD)")
    parser.add_argument("--game", default="Super Smash Bros. Ultimate", help="Game name filter")
    parser.add_argument("--min-entrants", type=int, default=16, help="Minimum entrants per event")
    parser.add_argument("--regions", nargs="+", default=["bay", "sacramento"], help="Regions to query")
    parser.add_argument("--cache", default=None, help="Path to SQLite cache file (default: data/tournament_cache.db)")
    parser.add_argument("--force-refresh", action="store_true", help="Bypass cache freshness and re-fetch the full range")
    parser.add_argument("--full", action="store_true", help="Disable incremental fetch (fetch whole range, still delta-aware)")
    parser.add_argument("--concurrency", type=int, default=None,
                        help=f"Parallel (window,region) page fetches; default {DEFAULT_CONCURRENCY}")
    parser.add_argument("--no-async", action="store_true",
                        help="Disable the concurrent engine (sequential fetch)")
    parser.add_argument("--quiet", action="store_true", help="Suppress verbose output")
    args = parser.parse_args()

    config = ScraperConfig(
        start_date=args.start,
        end_date=args.end,
        game_filter=args.game,
        min_entrants=args.min_entrants,
        regions=args.regions,
        cache_path=args.cache,
        incremental=not args.full,
        force_refresh=args.force_refresh,
        concurrency=args.concurrency if args.concurrency else DEFAULT_CONCURRENCY,
        use_async=not args.no_async,
    )
    tournaments, stats = scrape_tournaments(config, verbose=not args.quiet)
    print(f"Total: {len(tournaments)} tournaments")


if __name__ == "__main__":
    main()
