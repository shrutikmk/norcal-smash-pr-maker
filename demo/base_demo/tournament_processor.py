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
"""

from __future__ import annotations

import json
import os
import sqlite3
import time
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

import requests

from startgg_rate_gate import acquire_slot, is_likely_rate_limit_error, sleep_after_429

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
) -> dict:
    """POST to start.gg GraphQL API. Retries on transient errors (5xx, 429, connection/timeout)."""
    last_error: Exception | None = None
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
            wait_sec = min(90.0, 5 * (attempt + 1))
            time.sleep(wait_sec)
            continue
        if resp.status_code == 429:
            last_error = requests.exceptions.HTTPError("429 Rate limited", response=resp)
            sleep_after_429(attempt, resp)
            continue
        # 5xx: Cloudflare 520, origin 502/503, etc. - retry with longer backoff
        if 500 <= resp.status_code < 600:
            try:
                resp.raise_for_status()
            except requests.exceptions.HTTPError as e:
                last_error = e
            wait_sec = min(120.0, 15 * (attempt + 1))
            time.sleep(wait_sec)
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
        return data
    raise RuntimeError(f"Request failed after {max_retries} retries") from last_error


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
) -> list[tuple[str, str, str]]:
    """Load event slugs from tournament cache. Returns [(event_slug, tournament_id, event_name), ...]"""
    after = _date_to_unix(config.start_date)
    before = _date_to_unix(config.end_date)
    if after is None or before is None:
        raise ValueError("Invalid start_date or end_date")
    cur = conn.execute(
        """
        SELECT event_slug, tournament_id, name
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
            out.append((slug, row[1] or "", row[2] or ""))
    return out


def process_tournaments(
    config: ProcessorConfig | None = None,
    auth_token: str | None = None,
    *,
    verbose: bool = True,
) -> tuple[list[dict[str, Any]], ProcessorStats]:
    """
    Process tournaments from the scraper cache: fetch sets, apply name mappings.

    Returns:
        (list of {p1_canonical: score, p2_canonical: score}, stats)
    """
    config = config or ProcessorConfig()
    token = auth_token or os.environ.get("STARTGG_API_KEY")
    if not token:
        raise ValueError("STARTGG_API_KEY must be set or passed as auth_token")

    _ensure_dir(config.tournament_cache_path)
    _ensure_dir(config.processed_cache_path)

    if not config.tournament_cache_path.exists():
        raise FileNotFoundError(
            f"Tournament cache not found: {config.tournament_cache_path}. "
            "Run tournament_scraper.py first."
        )

    tconn = sqlite3.connect(str(config.tournament_cache_path))
    events_to_process = _load_events_from_tournament_cache(tconn, config)
    tconn.close()

    pconn = sqlite3.connect(str(config.processed_cache_path))
    _init_processed_db(pconn)

    # Event-level cache: which events have we already processed?
    cur = pconn.execute("SELECT event_slug FROM processed_events")
    processed_event_slugs = {row[0] for row in cur.fetchall()}

    # Set-level cache: which sets have we already fetched?
    cur = pconn.execute("SELECT set_id, p1_name, p2_name, p1_score, p2_score FROM sets_cache")
    sets_cache = {row[0]: {"p1_name": row[1], "p2_name": row[2], "p1_score": row[3], "p2_score": row[4]} for row in cur.fetchall()}

    stats = ProcessorStats()
    client = requests.Session()
    limiter = RateLimiter()

    all_sets: list[dict[str, Any]] = []

    if verbose:
        print(f"[CONFIG] Tournament cache: {config.tournament_cache_path}")
        print(f"[CONFIG] Processed cache: {config.processed_cache_path}")
        print(f"[CONFIG] Date range: {config.start_date} -> {config.end_date}")
        print(f"[CONFIG] Game: {config.game_filter!r}, min_entrants: {config.min_entrants}")
        print(f"[CONFIG] Name mappings: {len(config.name_mappings)} rules")
        print(f"[INPUT] Events to process: {len(events_to_process)}")
        print("\n" + "=" * 80)

    for i, (event_slug, tournament_id, event_name) in enumerate(events_to_process):
        if verbose:
            print(f"\n[EVENT {i+1}/{len(events_to_process)}] {event_slug!r}")

        if event_slug in processed_event_slugs:
            stats.event_hits += 1
            if verbose:
                print(f"  [EVENT CACHE HIT] Skipping - already processed")
            # Load set data from our cache for this event
            cur = pconn.execute(
                "SELECT set_id, p1_name, p2_name, p1_score, p2_score FROM sets_cache WHERE event_slug = ?",
                (event_slug,),
            )
            for row in cur.fetchall():
                stats.set_hits += 1
                p1_score, p2_score = row[3], row[4]
                if p1_score is not None and p2_score is not None:
                    all_sets.append({row[1]: p1_score, row[2]: p2_score})
            if verbose:
                cached_count = pconn.execute(
                    "SELECT COUNT(*) FROM sets_cache WHERE event_slug = ?", (event_slug,)
                ).fetchone()[0]
                print(f"  [SET CACHE] Loaded {cached_count} sets from DB (all hits)")
            continue

        stats.event_misses += 1
        if verbose:
            print(f"  [EVENT CACHE MISS] Fetching event ID and sets from API ...")

        try:
            event_id = _get_event_id(client, limiter, event_slug, token)
        except (requests.exceptions.HTTPError, RuntimeError) as e:
            stats.event_api_errors += 1
            if verbose:
                print(f"  [API ERROR] Event {event_slug!r} - {e} (skipping event)")
            continue
        if not event_id:
            if verbose:
                print(f"  [WARN] Could not get event ID for {event_slug!r}, skipping")
            continue

        try:
            set_ids = _get_set_ids_for_event(client, limiter, event_id, token)
        except (requests.exceptions.HTTPError, RuntimeError) as e:
            stats.event_api_errors += 1
            if verbose:
                print(f"  [API ERROR] Event {event_slug!r} - {e} (skipping event)")
            continue
        if verbose:
            print(f"  [API] Event ID={event_id}, sets count={len(set_ids)}")

        event_sets: list[dict[str, Any]] = []
        event_sets_to_insert: list[tuple] = []
        for j, set_id in enumerate(set_ids):
            if set_id in sets_cache:
                stats.set_hits += 1
                if verbose and j < 3:
                    print(f"    [SET HIT]  set_id={set_id}")
                rec = sets_cache[set_id]
                event_sets.append({rec["p1_name"]: rec["p1_score"], rec["p2_name"]: rec["p2_score"]})
                all_sets.append(event_sets[-1])
            elif set_id.startswith("preview_"):
                # Preview/placeholder sets have no real scores; skip API call
                if verbose and j < 3:
                    print(f"    [SKIP] set_id={set_id} - preview set, no scores")
                continue
            else:
                stats.set_misses += 1
                if verbose and j < 3:
                    print(f"    [SET MISS] set_id={set_id} - fetching from API")
                try:
                    result = _get_players_and_score(client, limiter, set_id, token)
                except (requests.exceptions.HTTPError, RuntimeError) as e:
                    stats.set_api_errors += 1
                    if verbose:
                        msg = str(e)[:80] + "..." if len(str(e)) > 80 else str(e)
                        print(f"    [API ERROR] set_id={set_id} - {msg} (skipping, continuing)")
                    continue
                if result and len(result) == 2:
                    s1, s2 = list(result.values())
                    if s1 is not None and s2 is not None:
                        event_sets.append(result)
                        all_sets.append(result)
                        p1, p2 = list(result.keys())
                        event_sets_to_insert.append((set_id, event_id, event_slug, p1, p2, s1, s2, int(time.time())))
                else:
                    if verbose and j < 3:
                        print(f"    [WARN] Incomplete/DQ set {set_id}, skipping")

        if verbose and len(set_ids) > 3:
            print(f"    ... and {len(set_ids) - 3} more sets")
        if set_ids:
            pconn.execute(
                "INSERT OR REPLACE INTO processed_events (event_slug, event_id, tournament_id, event_name, processed_at) VALUES (?, ?, ?, ?, ?)",
                (event_slug, event_id, tournament_id, event_name, int(time.time())),
            )
            if event_sets_to_insert:
                pconn.executemany(
                    "INSERT OR REPLACE INTO sets_cache (set_id, event_id, event_slug, p1_name, p2_name, p1_score, p2_score, cached_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                    event_sets_to_insert,
                )
            # Checkpoint after each event so a crash never loses all stage-2 progress.
            pconn.commit()
            processed_event_slugs.add(event_slug)
            for rec in event_sets_to_insert:
                sets_cache[rec[0]] = {"p1_name": rec[3], "p2_name": rec[4], "p1_score": rec[5], "p2_score": rec[6]}

    # Apply name mappings
    mapped_sets = _apply_name_mappings(all_sets, config.name_mappings)

    # Write processed_sets (canonical names) for ELO
    pconn.execute("DELETE FROM processed_sets")
    for i, s in enumerate(mapped_sets):
        names = list(s.keys())
        scores = list(s.values())
        if len(names) == 2 and len(scores) == 2:
            set_id = f"proc_{i}"
            pconn.execute(
                "INSERT INTO processed_sets (set_id, event_slug, p1_canonical, p2_canonical, p1_score, p2_score, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
                (set_id, "", names[0], names[1], scores[0], scores[1], int(time.time())),
            )
    pconn.commit()

    if verbose:
        _print_processed_db_head(pconn, limit=5)
        print("\n" + "-" * 80)
        print(f"[SUMMARY] Event cache hits:  {stats.event_hits}")
        print(f"[SUMMARY] Event cache misses: {stats.event_misses}")
        print(f"[SUMMARY] Set cache hits:    {stats.set_hits}")
        print(f"[SUMMARY] Set cache misses:  {stats.set_misses}")
        if stats.event_api_errors:
            print(f"[SUMMARY] Event API errors (skipped): {stats.event_api_errors}")
        if stats.set_api_errors:
            print(f"[SUMMARY] Set API errors (skipped): {stats.set_api_errors}")
        print(f"[SUMMARY] Total sets (after name mapping): {len(mapped_sets)}")
        print("-" * 80)

    pconn.close()
    return mapped_sets, stats


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
    )
    sets, stats = process_tournaments(config, verbose=not args.quiet)
    print(f"Total processed sets: {len(sets)}")


if __name__ == "__main__":
    main()
