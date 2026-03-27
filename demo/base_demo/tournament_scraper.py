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

from startgg_rate_gate import acquire_slot, is_likely_rate_limit_error, sleep_after_429

# --- Configuration ---

API_URL = "https://api.start.gg/gql/alpha"
# start.gg rate limits: 80 req/60s, max 1000 objects/request (incl. nested)
RATE_LIMIT_REQUESTS_PER_MIN = 80
RATE_LIMIT_INTERVAL_SEC = 60.0 / RATE_LIMIT_REQUESTS_PER_MIN  # 0.75s min; use 1.0s for margin
MAX_OBJECTS_PER_REQUEST = 1000
# per_page kept ≤100 to stay under 1000 objects (tournaments have nested events)
DEFAULT_PER_PAGE = 50

NORCAL_REGIONS: dict[str, tuple[str, str]] = {
    "bay": ("37.77151615492457, -122.41563048985462", "70mi"),
    "sacramento": ("38.57608096237729, -121.49183616631059", "40mi"),
}


def _default_cache_path() -> Path:
    """Single shared cache for all date ranges; overlapping ranges reuse existing data."""
    project_root = Path(__file__).resolve().parent.parent.parent
    return project_root / "data" / "tournament_cache.db"


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
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_tournaments_start_at 
        ON tournaments(start_at)
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_tournaments_videogame 
        ON tournaments(videogame_name)
    """)
    _ensure_coverage_table(conn)
    conn.commit()


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
    After a successful scrape of [range_start, range_end], mark each fully-contained
    Monday week with no cached rows for `game_filter` as verified-empty (not a gap).
    """
    _ensure_coverage_table(conn)
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
        if row is None:
            ws = cursor.strftime("%Y-%m-%d")
            conn.execute(
                """
                INSERT OR REPLACE INTO scrape_verified_empty_weeks
                (week_start, game_filter, verified_at)
                VALUES (?, ?, ?)
                """,
                (ws, game_filter, now),
            )
        cursor += timedelta(days=7)


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
    nodes {{
      id
      name
      city
      slug
      startAt
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
    verbose: bool = False,
) -> Iterator[dict]:
    after = _date_to_unix(config.start_date)
    before = _date_to_unix(config.end_date)
    if after is None or before is None:
        raise ValueError("Invalid start_date or end_date")
    query = _build_query(after, before)

    for region_key in config.regions:
        if region_key not in NORCAL_REGIONS:
            raise ValueError(f"Unknown region: {region_key}")
        coords, radius = NORCAL_REGIONS[region_key]
        page = 1
        while True:
            if verbose:
                print(f"  [API] Fetching region={region_key!r} page={page} ...")
            per_page = min(config.per_page, MAX_OBJECTS_PER_REQUEST // 10)  # cap for nesting
            nodes = _fetch_page(
                client, limiter, query, page, coords, radius, per_page, auth_token
            )
            if verbose:
                print(f"  [API]   -> got {len(nodes)} tournaments")
            if not nodes:
                break
            for t in nodes:
                yield t
            if len(nodes) < per_page:
                break
            page += 1


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
    conn = sqlite3.connect(str(cache_path))
    _init_cache(conn)

    after = _date_to_unix(config.start_date)
    before = _date_to_unix(config.end_date)
    if after is None or before is None:
        raise ValueError("Invalid start_date or end_date")

    cached_ids = _get_cached_tournament_ids(conn, after, before)
    stats = CacheStats()

    if verbose:
        print(f"[CONFIG] Date range: {config.start_date} -> {config.end_date}")
        print(f"[CONFIG] Game filter: {config.game_filter!r}, min_entrants: {config.min_entrants}")
        print(f"[CONFIG] Regions: {config.regions}")
        print(f"[CACHE] Already cached in this range: {len(cached_ids)} tournaments")
        print("\n[FETCH] Querying start.gg API ...")

    client = requests.Session()
    limiter = RateLimiter()

    all_tournaments: list[dict] = []
    to_insert: list[tuple] = []
    hit_examples: list[str] = []
    miss_examples: list[str] = []

    for t in _fetch_all_tournaments(client, limiter, config, token, verbose=verbose):
        tid = str(t.get("id", ""))
        name = t.get("name", "?")
        if tid in cached_ids:
            stats.hits += 1
            if len(hit_examples) < 5:
                hit_examples.append(f"  [HIT]  id={tid} {name!r} (skipped, already in cache)")
            continue
        stats.misses += 1
        if len(miss_examples) < 5:
            miss_examples.append(f"  [MISS] id={tid} {name!r} (fetching, will insert)")
        all_tournaments.append(t)
        for ev in t.get("events") or []:
            row = _tournament_to_row(t, ev)
            to_insert.append(row)

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
            (tournament_id, event_slug, name, city, slug, start_at, event_num_entrants, videogame_name, raw_json, cached_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            to_insert,
        )
        conn.commit()
    else:
        if verbose:
            print("\n[DB] No new rows to insert (all cache hits)")

    record_verified_empty_weeks_for_scrape_window(
        conn,
        range_start=config.start_date,
        range_end=config.end_date,
        game_filter=config.game_filter,
    )
    conn.commit()

    if verbose:
        _print_db_head(conn, limit=5)

    conn.close()

    # Apply game/entrants filter to combined cached + fresh
    conn2 = sqlite3.connect(str(cache_path))
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
    parser.add_argument("--quiet", action="store_true", help="Suppress verbose output")
    args = parser.parse_args()

    config = ScraperConfig(
        start_date=args.start,
        end_date=args.end,
        game_filter=args.game,
        min_entrants=args.min_entrants,
        regions=args.regions,
        cache_path=args.cache,
    )
    tournaments, stats = scrape_tournaments(config, verbose=not args.quiet)
    print(f"Total: {len(tournaments)} tournaments")


if __name__ == "__main__":
    main()
