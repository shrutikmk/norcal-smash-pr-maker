"""
NorCal ELO + Derived Statistics (Part 3).

Uses:
- data/tournament_cache.db (from tournament_scraper.py)
- data/processed_tournament.db (from tournament_processor.py)

Implements:
- In-region ELO
- Tournament include/exclude filters
- Demo with random tournament exclusion and random player comparison
- LIVE out-of-region API calls (no placeholders)
- Upsert-based player DB that avoids duplicates
"""

from __future__ import annotations

import json
import os
import random
import sqlite3
import time
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

import requests

from startgg_rate_gate import acquire_slot, is_likely_rate_limit_error, sleep_after_429

try:
    from dotenv import load_dotenv

    # Project root .env (file lives in demo/base_demo/)
    load_dotenv(Path(__file__).resolve().parent.parent.parent / ".env")
except Exception:
    pass

API_URL = "https://api.start.gg/gql/alpha"
SMASH_ULTIMATE_VIDEOGAME_ID = 1386
# start.gg rate limits: 80 req/60s, max 1000 objects/request (incl. nested)
RATE_LIMIT_REQUESTS_PER_MIN = 80
RATE_LIMIT_INTERVAL_SEC = max(1.0, 60.0 / RATE_LIMIT_REQUESTS_PER_MIN)  # 1.0s for margin
MAX_OBJECTS_PER_REQUEST = 1000

# Project root data/ (file lives in demo/base_demo/)
_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DEFAULT_TOURNAMENT_CACHE = _PROJECT_ROOT / "data" / "tournament_cache.db"
DEFAULT_PROCESSED_CACHE = _PROJECT_ROOT / "data" / "processed_tournament.db"
DEFAULT_PLAYER_DB = _PROJECT_ROOT / "data" / "player_db.db"

# Explicit check: Port Priority 9 (validation tournament per all-functions.ipynb)
PORT_PRIORITY_9_TOURNAMENT = "Port Priority 9"

DEFAULT_NAME_MAPPINGS: dict[str, str] = {
    "NLC | they call me leonidas": "Hyro",
    "NLC | Still Spoozy": "Hyro",
    "MPoor": "M4",
    "W4": "M4",
    "SALT | ebs | ERA": "ERA",
    "era": "ERA",
    "EBS | HK | the filipino flowstate.": "Skylock",
    "NU | Lui$": "Team Var$ | Lui$",
    "Lui$": "Team Var$ | Lui$",
    "lui$": "Team Var$ | Lui$",
}

EVENT_ENTRANTS_QUERY = """
query EventEntrants($eventId: ID!, $page: Int!, $perPage: Int!) {
  event(id: $eventId) {
    id
    entrants(query: { page: $page, perPage: $perPage }) {
      pageInfo { totalPages }
      nodes {
        participants {
          gamerTag
          prefix
          user {
            id
            player { id }
          }
        }
      }
    }
  }
}
"""

EVENT_ID_BY_SLUG_QUERY = """
query EventBySlug($slug: String) {
  event(slug: $slug) {
    id
  }
}
"""

USER_BY_SLUG_QUERY = """
query UserBySlug($slug: String!) {
  user(slug: $slug) {
    id
    player {
      id
      gamerTag
    }
  }
}
"""

PLAYER_SETS_QUERY = """
query PlayerSets($playerId: ID!, $page: Int!, $perPage: Int!) {
  player(id: $playerId) {
    id
    sets(page: $page, perPage: $perPage) {
      pageInfo { totalPages page perPage }
      nodes {
        id
        event {
          id
          slug
          name
          tournament {
            id
            slug
            name
            startAt
          }
        }
        slots {
          entrant {
            participants {
              gamerTag
              prefix
              user { id }
              player { id }
            }
          }
          standing {
            stats {
              score { value }
            }
          }
        }
      }
    }
  }
}
"""

EVENT_SETS_QUERY = """
query EventSetsDetailed($eventId: ID!, $page: Int!, $perPage: Int!) {
  event(id: $eventId) {
    sets(page: $page, perPage: $perPage, sortType: STANDARD) {
      pageInfo { totalPages }
      nodes {
        id
        slots {
          entrant {
            participants {
              gamerTag
              prefix
              user { id }
              player { id }
            }
          }
          standing {
            stats {
              score { value }
            }
          }
        }
      }
    }
  }
}
"""

EVENT_STANDINGS_QUERY = """
query EventStandings($eventId: ID!, $page: Int!, $perPage: Int!) {
  event(id: $eventId) {
    standings(query: { page: $page, perPage: $perPage }) {
      pageInfo { totalPages }
      nodes {
        placement
        entrant {
          participants {
            user { id }
            player { id }
          }
        }
      }
    }
  }
}
"""

PLAYER_SETS_FILTERED_QUERY = """
query PlayerSetsFiltered($playerId: ID!, $page: Int!, $perPage: Int!, $tournamentIds: [ID]) {
  player(id: $playerId) {
    id
    sets(page: $page, perPage: $perPage, filters: { tournamentIds: $tournamentIds }) {
      pageInfo { totalPages page perPage }
      nodes {
        id
        event {
          id
          slug
          name
          tournament {
            id
            slug
            name
            startAt
          }
        }
        slots {
          entrant {
            participants {
              gamerTag
              prefix
              user { id }
              player { id }
            }
          }
          standing {
            stats {
              score { value }
            }
          }
        }
      }
    }
  }
}
"""

TOURNAMENTS_BY_GAME_DATE_QUERY = """
query TournamentsByGameDate($page: Int!, $perPage: Int!, $afterDate: Timestamp!, $beforeDate: Timestamp!, $videogameIds: [ID]!) {
  tournaments(
    query: {
      page: $page
      perPage: $perPage
      filter: {
        afterDate: $afterDate
        beforeDate: $beforeDate
        videogameIds: $videogameIds
        past: true
      }
      sortBy: "startAt"
    }
  ) {
    pageInfo { totalPages page }
    nodes {
      id
      name
      slug
      startAt
    }
  }
}
"""


@dataclass
class EloConfig:
    tournament_cache_path: Path = field(default_factory=lambda: DEFAULT_TOURNAMENT_CACHE)
    processed_cache_path: Path = field(default_factory=lambda: DEFAULT_PROCESSED_CACHE)
    player_db_path: Path = field(default_factory=lambda: DEFAULT_PLAYER_DB)
    name_mappings: dict[str, str] = field(default_factory=lambda: dict(DEFAULT_NAME_MAPPINGS))
    start_date: str | None = None
    end_date: str | None = None
    exclude_event_slugs: set[str] = field(default_factory=set)
    exclude_tournament_ids: set[str] = field(default_factory=set)
    exclude_tournament_names: set[str] = field(default_factory=set)
    include_event_slugs: set[str] | None = None
    k_factor: float = 30.0
    initial_elo: float = 1500.0
    per_page: int = 50  # capped to MAX_OBJECTS_PER_REQUEST // 10 when making API calls
    max_retries: int = 5
    max_out_region_tournaments: int | None = 20
    oor_early_stop_player_sets: bool = False
    oor_use_tournament_catalog: bool = False


class RateLimiter:
    def __init__(self, interval_sec: float | None = None):
        self.interval = interval_sec if interval_sec is not None else RATE_LIMIT_INTERVAL_SEC
        self._last_time = 0.0

    def wait(self) -> None:
        elapsed = time.monotonic() - self._last_time
        if elapsed < self.interval:
            time.sleep(self.interval - elapsed)
        self._last_time = time.monotonic()


class StartGGClient:
    def __init__(self, auth_token: str, *, limiter: RateLimiter | None = None):
        if not auth_token:
            raise ValueError("Missing STARTGG_API_KEY")
        self.auth_token = auth_token
        self.limiter = limiter or RateLimiter()
        self.session = requests.Session()

    def gql(self, query: str, variables: dict[str, Any], max_retries: int = 30) -> dict[str, Any]:
        last_error: Exception | None = None
        for attempt in range(max_retries):
            acquire_slot()
            try:
                resp = self.session.post(
                    API_URL,
                    headers={"Authorization": f"Bearer {self.auth_token}", "Content-Type": "application/json"},
                    json={"query": query, "variables": variables},
                    timeout=60,
                )
            except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as e:
                last_error = e
                time.sleep(min(90.0, 5 * (attempt + 1)))
                continue

            if resp.status_code == 429:
                last_error = RuntimeError("Rate limited")
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
                payload = resp.json()
            except ValueError as e:
                last_error = e
                time.sleep(min(60.0, 5 * (attempt + 1)))
                continue

            if payload.get("success") is False:
                msg = str(payload.get("message") or payload)
                if is_likely_rate_limit_error(payload, msg) or "timeout" in msg.lower():
                    last_error = RuntimeError(msg)
                    sleep_after_429(attempt, resp)
                    continue
                raise RuntimeError(f"start.gg API error: {msg}")
            if "errors" in payload:
                msg = str(payload["errors"])
                if is_likely_rate_limit_error(payload, msg) or "timeout" in msg.lower():
                    last_error = RuntimeError(msg)
                    sleep_after_429(attempt, resp)
                    continue
                if "complexity" in msg.lower() or "1000 objects" in msg.lower():
                    page_dbg = (variables or {}).get("page")
                    per_page_dbg = (variables or {}).get("perPage")
                    query_name = "unknown"
                    for line in query.splitlines():
                        s = line.strip()
                        if s.startswith("query "):
                            query_name = s.split("{", 1)[0]
                            break
                    raise RuntimeError(
                        "GraphQL complexity limit exceeded "
                        f"(query={query_name}, page={page_dbg}, perPage={per_page_dbg}). "
                        "Lower perPage for this query."
                    )
                raise RuntimeError(f"GraphQL errors: {payload['errors']}")
            return payload

        raise RuntimeError(f"start.gg request failed after {max_retries} retries") from last_error


def _date_to_unix(date_str: str, fmt: str = "%Y-%m-%d") -> int | None:
    try:
        return int(datetime.strptime(date_str, fmt).timestamp())
    except ValueError:
        return None


def _canonical(name: str, mappings: dict[str, str]) -> str:
    return mappings.get(name, name)


def _name_variants(name: str) -> set[str]:
    """
    Variants used for identity resolution.
    Example:
      'LG CS3 VARS | MkLeo' -> {'LG CS3 VARS | MkLeo', 'MkLeo'}
    """
    base = (name or "").strip()
    if not base:
        return set()
    variants = {base}
    if "|" in base:
        variants.add(base.split("|")[-1].strip())
    return {v for v in variants if v}


def _slug_candidates_from_name(name: str) -> list[str]:
    """
    Build possible start.gg user slugs from player display names.
    Example: 'LG CS3 VARS | MkLeo' -> ['user/mkleo']
    """
    variants = sorted(_name_variants(name), key=len)
    out: list[str] = []
    seen: set[str] = set()
    for v in variants:
        raw = v.strip().lower()
        compact = "".join(ch for ch in raw if ch.isalnum() or ch in {"_", "-"})
        if compact:
            slug = f"user/{compact}"
            if slug not in seen:
                seen.add(slug)
                out.append(slug)
    return out


def _player_display(prefix: Any, gamer_tag: Any) -> str:
    p = (prefix or "").strip()
    g = (gamer_tag or "").strip()
    return g if not p else f"{p} | {g}"


def _get_port_priority_9_placements(report: dict[str, Any]) -> list[dict[str, Any]]:
    """Explicit Port Priority 9 check: returns out-of-region placements for Port Priority 9."""
    placements = report.get("out_region_placements", []) or []
    return [r for r in placements if PORT_PRIORITY_9_TOURNAMENT in (r.get("tournament_name") or "")]


def _safe_per_page(per_page: int) -> int:
    """Cap per_page to stay under start.gg 1000 objects/request limit (incl. nested)."""
    return min(per_page, MAX_OBJECTS_PER_REQUEST // 10)


# PLAYER_SETS_QUERY has heavy nesting (~22 objects/set: event, tournament, slots, participants, etc.)
# 1103 objects observed with 50 sets; use 40 to stay under 1000
PLAYER_SETS_PER_PAGE = 40
# EVENT_STANDINGS_QUERY can exceed complexity when perPage is too high.
EVENT_STANDINGS_PER_PAGE = 50


def _contains_opponent(opponent_names: list[str], needle: str) -> bool:
    """Case-insensitive exact/substring match against opponent names."""
    target = needle.casefold().strip()
    for name in opponent_names:
        n = (name or "").casefold().strip()
        if n == target or target in n:
            return True
    return False


def _safe_slot_score(slot: dict[str, Any]) -> int | None:
    standing = slot.get("standing") or {}
    stats = standing.get("stats") or {}
    score = stats.get("score") or {}
    value = score.get("value")
    return value if isinstance(value, int) else None


def _extract_player_set_result(node: dict[str, Any], player_id: str) -> dict[str, Any] | None:
    """Extract one player's result from a raw PlayerSets node."""
    slots = node.get("slots") or []
    if len(slots) < 2:
        return None
    a, b = slots[0], slots[1]
    parts_a = ((a.get("entrant") or {}).get("participants") or [])
    parts_b = ((b.get("entrant") or {}).get("participants") or [])
    if not parts_a or not parts_b:
        return None

    pid_a = str(((parts_a[0].get("player") or {}).get("id") or ""))
    pid_b = str(((parts_b[0].get("player") or {}).get("id") or ""))
    score_a = _safe_slot_score(a)
    score_b = _safe_slot_score(b)
    if score_a is None or score_b is None:
        return None

    name_a = _player_display(parts_a[0].get("prefix"), parts_a[0].get("gamerTag"))
    name_b = _player_display(parts_b[0].get("prefix"), parts_b[0].get("gamerTag"))

    if pid_a == str(player_id):
        return {
            "opponent": name_b,
            "player_score": score_a,
            "opponent_score": score_b,
            "result": "W" if score_a > score_b else "L",
        }
    if pid_b == str(player_id):
        return {
            "opponent": name_a,
            "player_score": score_b,
            "opponent_score": score_a,
            "result": "W" if score_b > score_a else "L",
        }
    return None


def _load_in_region_sets(config: EloConfig) -> list[dict[str, Any]]:
    pconn = sqlite3.connect(str(config.processed_cache_path))
    tconn = sqlite3.connect(str(config.tournament_cache_path))

    after = _date_to_unix(config.start_date) if config.start_date else None
    before = _date_to_unix(config.end_date) if config.end_date else None

    sql = """
        SELECT s.set_id, s.event_slug, s.p1_name, s.p2_name, s.p1_score, s.p2_score,
               e.event_id, e.tournament_id, e.event_name
        FROM sets_cache s
        LEFT JOIN processed_events e ON s.event_slug = e.event_slug
        WHERE s.p1_score IS NOT NULL AND s.p2_score IS NOT NULL
    """
    sql_params: list[Any] = []
    if config.include_event_slugs is not None:
        placeholders = ",".join("?" for _ in config.include_event_slugs)
        sql += f" AND s.event_slug IN ({placeholders})"
        sql_params.extend(config.include_event_slugs)
    sql += " ORDER BY e.tournament_id, s.set_id"
    rows = pconn.execute(sql, sql_params).fetchall()

    sets: list[dict[str, Any]] = []
    for row in rows:
        set_id, event_slug, p1_name, p2_name, s1, s2, event_id, tournament_id, event_name = row
        if config.exclude_event_slugs and event_slug in config.exclude_event_slugs:
            continue
        if config.exclude_tournament_ids and str(tournament_id) in config.exclude_tournament_ids:
            continue

        trow = tconn.execute(
            "SELECT name, start_at FROM tournaments WHERE tournament_id = ? AND event_slug = ? LIMIT 1",
            (str(tournament_id), event_slug),
        ).fetchone()
        tname = (trow[0] if trow else event_name) or ""
        start_at = int(trow[1]) if trow and trow[1] is not None else 0

        if config.exclude_tournament_names and any(ex in tname for ex in config.exclude_tournament_names):
            continue
        if after is not None and start_at < after:
            continue
        if before is not None and start_at > before:
            continue

        sets.append(
            {
                "set_id": set_id,
                "event_slug": event_slug,
                "event_id": str(event_id or ""),
                "tournament_id": str(tournament_id or ""),
                "tournament_name": tname,
                "start_at": start_at,
                "p1": _canonical(p1_name, config.name_mappings),
                "p2": _canonical(p2_name, config.name_mappings),
                "p1_score": int(s1),
                "p2_score": int(s2),
            }
        )

    pconn.close()
    tconn.close()
    return sets


def _update_elo(elo: dict[str, float], p1: str, p2: str, p1_score: int, p2_score: int, *, k: float, initial: float) -> None:
    r1 = elo.get(p1, initial)
    r2 = elo.get(p2, initial)
    e1 = 1.0 / (1 + 10 ** ((r2 - r1) / 400.0))
    e2 = 1.0 - e1
    outcome = 1 if p1_score > p2_score else 0
    elo[p1] = r1 + k * (outcome - e1)
    elo[p2] = r2 + k * ((1 - outcome) - e2)


def _compute_elo_from_sets(
    sets: list[dict[str, Any]],
    *,
    k_factor: float = 30.0,
    initial_elo: float = 1500.0,
) -> dict[str, float]:
    players = {s["p1"] for s in sets} | {s["p2"] for s in sets}
    elo = {p: initial_elo for p in players}
    for s in sets:
        _update_elo(elo, s["p1"], s["p2"], s["p1_score"], s["p2_score"], k=k_factor, initial=initial_elo)
    return dict(sorted(elo.items(), key=lambda x: x[1], reverse=True))


def compute_elo(config: EloConfig) -> tuple[dict[str, float], list[dict[str, Any]]]:
    sets = _load_in_region_sets(config)
    elo = _compute_elo_from_sets(sets, k_factor=config.k_factor, initial_elo=config.initial_elo)
    return elo, sets


def _compute_h2h(sets: list[dict[str, Any]]) -> dict[tuple[str, str], tuple[int, int]]:
    h2h: dict[tuple[str, str], tuple[int, int]] = {}
    for s in sets:
        a, b = s["p1"], s["p2"]
        key = (min(a, b), max(a, b))
        w1, w2 = h2h.get(key, (0, 0))
        a_won = s["p1_score"] > s["p2_score"]
        if a < b:
            h2h[key] = (w1 + 1, w2) if a_won else (w1, w2 + 1)
        else:
            h2h[key] = (w1, w2 + 1) if a_won else (w1 + 1, w2)
    return h2h


def _h2h_record(h2h: dict[tuple[str, str], tuple[int, int]], p1: str, p2: str) -> tuple[int, int]:
    key = (min(p1, p2), max(p1, p2))
    w1, w2 = h2h.get(key, (0, 0))
    return (w1, w2) if p1 < p2 else (w2, w1)


def _build_player_opponent_records(
    player: str,
    in_region_sets: list[dict[str, Any]],
    report: dict[str, Any],
) -> dict[str, dict[str, int]]:
    records: dict[str, dict[str, int]] = {}

    def bump(opponent: str, won: bool) -> None:
        rec = records.setdefault(opponent, {"wins": 0, "losses": 0})
        if won:
            rec["wins"] += 1
        else:
            rec["losses"] += 1

    for s in in_region_sets:
        if s["p1"] == player:
            bump(s["p2"], s["p1_score"] > s["p2_score"])
        elif s["p2"] == player:
            bump(s["p1"], s["p2_score"] > s["p1_score"])

    for opp in report.get("all_out_wins", []):
        bump(str(opp), True)
    for opp in report.get("all_out_losses", []):
        bump(str(opp), False)
    return records


def _tournament_summary_rows(report: dict[str, Any]) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    rows = (report.get("in_region_placements", []) or []) + (report.get("out_region_placements", []) or [])
    for row in rows:
        t_id = str(row.get("tournament_id") or "")
        t_name = str(row.get("tournament_name") or "")
        key = t_id or t_name.lower()
        if not key:
            continue
        cur = out.setdefault(
            key,
            {
                "tournament_name": t_name,
                "tournament_id": t_id,
                "event_rows": [],
                "wins": 0,
                "losses": 0,
                "best_placement": None,
            },
        )
        cur["event_rows"].append(row)
        cur["wins"] += int(row.get("wins") or 0)
        cur["losses"] += int(row.get("losses") or 0)
        pl = row.get("placement")
        if isinstance(pl, int):
            if cur["best_placement"] is None or pl < cur["best_placement"]:
                cur["best_placement"] = pl
    return out


def _expanded_head_to_head(
    p1: str,
    p2: str,
    in_region_sets: list[dict[str, Any]],
    reports: dict[str, dict[str, Any]],
    elo: dict[str, float] | None = None,
    initial_elo: float = 1500.0,
) -> dict[str, Any]:
    """Structured expanded H2H data for two players (extracted from run_demo)."""
    r1 = reports.get(p1, {})
    r2 = reports.get(p2, {})
    rec1 = _build_player_opponent_records(p1, in_region_sets, r1)
    rec2 = _build_player_opponent_records(p2, in_region_sets, r2)
    elo = elo or {}

    def _opp_elo(name: str) -> float:
        return elo.get(name, initial_elo)

    shared_wins_names = [
        opp for opp in rec1
        if rec1[opp]["wins"] > 0 and rec2.get(opp, {}).get("wins", 0) > 0
    ]
    shared_wins_names.sort(key=lambda o: _opp_elo(o), reverse=True)
    shared_wins = [
        {"opponent": o, "p1Wins": rec1[o]["wins"], "p1Losses": rec1[o]["losses"],
         "p2Wins": rec2[o]["wins"], "p2Losses": rec2[o]["losses"], "oppElo": round(_opp_elo(o), 2)}
        for o in shared_wins_names
    ]

    shared_losses_names = [
        opp for opp in rec1
        if rec1[opp]["losses"] > 0 and rec2.get(opp, {}).get("losses", 0) > 0
    ]
    shared_losses_names.sort(key=lambda o: _opp_elo(o), reverse=True)
    shared_losses = [
        {"opponent": o, "p1Wins": rec1[o]["wins"], "p1Losses": rec1[o]["losses"],
         "p2Wins": rec2[o]["wins"], "p2Losses": rec2[o]["losses"], "oppElo": round(_opp_elo(o), 2)}
        for o in shared_losses_names
    ]

    t1 = _tournament_summary_rows(r1)
    t2 = _tournament_summary_rows(r2)
    shared_t_keys = sorted(
        set(t1.keys()) & set(t2.keys()),
        key=lambda k: str(t1[k].get("tournament_name") or t2[k].get("tournament_name") or ""),
    )
    tournaments_both = []
    for k in shared_t_keys:
        a, b = t1[k], t2[k]
        tournaments_both.append({
            "name": a.get("tournament_name") or b.get("tournament_name") or "(unknown)",
            "p1Place": a.get("best_placement"),
            "p1WL": f"{a.get('wins', 0)}-{a.get('losses', 0)}",
            "p1Events": len(a.get("event_rows", [])),
            "p2Place": b.get("best_placement"),
            "p2WL": f"{b.get('wins', 0)}-{b.get('losses', 0)}",
            "p2Events": len(b.get("event_rows", [])),
        })

    p1_unique_wins = sorted(
        [o for o in rec1 if rec1[o]["wins"] > 0 and rec2.get(o, {}).get("wins", 0) == 0],
        key=lambda o: _opp_elo(o), reverse=True,
    )
    p2_unique_wins = sorted(
        [o for o in rec2 if rec2[o]["wins"] > 0 and rec1.get(o, {}).get("wins", 0) == 0],
        key=lambda o: _opp_elo(o), reverse=True,
    )

    return {
        "sharedWins": shared_wins,
        "sharedLosses": shared_losses,
        "tournamentsBothAttended": tournaments_both,
        "p1UniqueWins": [{"opponent": o, "wins": rec1[o]["wins"], "losses": rec1[o]["losses"],
                          "oppElo": round(_opp_elo(o), 2)} for o in p1_unique_wins[:20]],
        "p2UniqueWins": [{"opponent": o, "wins": rec2[o]["wins"], "losses": rec2[o]["losses"],
                          "oppElo": round(_opp_elo(o), 2)} for o in p2_unique_wins[:20]],
    }


def _init_player_db(db_path: Path) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path))
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS player_identity (
            canonical_name TEXT PRIMARY KEY,
            user_id TEXT,
            player_id TEXT,
            source_event_id TEXT,
            updated_at INTEGER
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS player_summary (
            canonical_name TEXT PRIMARY KEY,
            elo REAL,
            in_region_tournaments INTEGER,
            in_region_wins INTEGER,
            in_region_losses INTEGER,
            out_region_tournaments INTEGER,
            out_region_wins INTEGER,
            out_region_losses INTEGER,
            updated_at INTEGER
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS player_tournament_stats (
            canonical_name TEXT,
            tournament_id TEXT,
            event_slug TEXT,
            tournament_name TEXT,
            region TEXT,
            placement INTEGER,
            wins INTEGER,
            losses INTEGER,
            updated_at INTEGER,
            PRIMARY KEY (canonical_name, tournament_id, event_slug, region)
        )
        """
    )

    # Migrate older schemas from earlier iterations of this project.
    identity_cols = {r[1] for r in conn.execute("PRAGMA table_info(player_identity)").fetchall()}
    expected_identity = {"canonical_name", "user_id", "player_id", "source_event_id", "updated_at"}
    if not expected_identity.issubset(identity_cols):
        conn.execute("DROP TABLE IF EXISTS player_identity")
        conn.execute(
            """
            CREATE TABLE player_identity (
                canonical_name TEXT PRIMARY KEY,
                user_id TEXT,
                player_id TEXT,
                source_event_id TEXT,
                updated_at INTEGER
            )
            """
        )

    summary_cols = {r[1] for r in conn.execute("PRAGMA table_info(player_summary)").fetchall()}
    expected_summary = {
        "canonical_name",
        "elo",
        "in_region_tournaments",
        "in_region_wins",
        "in_region_losses",
        "out_region_tournaments",
        "out_region_wins",
        "out_region_losses",
        "updated_at",
    }
    if not expected_summary.issubset(summary_cols):
        conn.execute("DROP TABLE IF EXISTS player_summary")
        conn.execute(
            """
            CREATE TABLE player_summary (
                canonical_name TEXT PRIMARY KEY,
                elo REAL,
                in_region_tournaments INTEGER,
                in_region_wins INTEGER,
                in_region_losses INTEGER,
                out_region_tournaments INTEGER,
                out_region_wins INTEGER,
                out_region_losses INTEGER,
                updated_at INTEGER
            )
            """
        )

    pts_cols = {r[1] for r in conn.execute("PRAGMA table_info(player_tournament_stats)").fetchall()}
    expected_pts = {
        "canonical_name",
        "tournament_id",
        "event_slug",
        "tournament_name",
        "region",
        "placement",
        "wins",
        "losses",
        "updated_at",
    }
    if not expected_pts.issubset(pts_cols):
        conn.execute("DROP TABLE IF EXISTS player_tournament_stats")
        conn.execute(
            """
            CREATE TABLE player_tournament_stats (
                canonical_name TEXT,
                tournament_id TEXT,
                event_slug TEXT,
                tournament_name TEXT,
                region TEXT,
                placement INTEGER,
                wins INTEGER,
                losses INTEGER,
                updated_at INTEGER,
                PRIMARY KEY (canonical_name, tournament_id, event_slug, region)
            )
            """
        )
    conn.commit()
    return conn


def _player_db_counts(conn: sqlite3.Connection) -> dict[str, int]:
    return {
        "identity": int(conn.execute("SELECT COUNT(*) FROM player_identity").fetchone()[0]),
        "summary": int(conn.execute("SELECT COUNT(*) FROM player_summary").fetchone()[0]),
        "tournament_stats": int(conn.execute("SELECT COUNT(*) FROM player_tournament_stats").fetchone()[0]),
    }


def _choose_two_resolvable_players(
    *,
    players: list[str],
    conn: sqlite3.Connection,
    config: EloConfig,
    client: StartGGClient,
    verbose: bool,
) -> tuple[str, str, dict[str, dict[str, str]]]:
    # Prefer already-resolved identities from local DB to avoid incomplete demo output.
    resolved_names = {
        str(r[0])
        for r in conn.execute(
            "SELECT canonical_name FROM player_identity WHERE user_id IS NOT NULL AND user_id != ''"
        ).fetchall()
    }
    candidates = [p for p in players if p in resolved_names]
    if len(candidates) >= 2:
        p1, p2 = random.sample(candidates, 2)
        identity = _build_identity_map_live(client, config, {p1, p2}, conn, verbose=False)
        if p1 in identity and p2 in identity:
            return p1, p2, identity

    # Fallback: try a handful of random pairs and resolve live.
    for _ in range(10):
        p1, p2 = random.sample(players, 2)
        identity = _build_identity_map_live(client, config, {p1, p2}, conn, verbose=verbose)
        if p1 in identity and p2 in identity:
            return p1, p2, identity

    raise RuntimeError(
        "Could not find two players with resolvable start.gg identities for live comparison."
    )


def _upsert_player_summary(
    conn: sqlite3.Connection,
    *,
    canonical_name: str,
    elo: float,
    in_region_tournaments: int,
    in_region_wins: int,
    in_region_losses: int,
    out_region_tournaments: int,
    out_region_wins: int,
    out_region_losses: int,
) -> None:
    now = int(time.time())
    conn.execute(
        """
        INSERT INTO player_summary
        (canonical_name, elo, in_region_tournaments, in_region_wins, in_region_losses,
         out_region_tournaments, out_region_wins, out_region_losses, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(canonical_name) DO UPDATE SET
            elo=excluded.elo,
            in_region_tournaments=excluded.in_region_tournaments,
            in_region_wins=excluded.in_region_wins,
            in_region_losses=excluded.in_region_losses,
            out_region_tournaments=excluded.out_region_tournaments,
            out_region_wins=excluded.out_region_wins,
            out_region_losses=excluded.out_region_losses,
            updated_at=excluded.updated_at
        """,
        (
            canonical_name,
            elo,
            in_region_tournaments,
            in_region_wins,
            in_region_losses,
            out_region_tournaments,
            out_region_wins,
            out_region_losses,
            now,
        ),
    )


def _in_region_event_ids(processed_cache_path: Path) -> list[str]:
    conn = sqlite3.connect(str(processed_cache_path))
    rows = conn.execute("SELECT DISTINCT event_id FROM processed_events WHERE event_id IS NOT NULL AND event_id != ''").fetchall()
    conn.close()
    return [str(r[0]) for r in rows]


def _event_slugs_from_tournament_cache(
    tournament_cache_path: Path,
    *,
    start_date: str | None = None,
    end_date: str | None = None,
) -> list[str]:
    conn = sqlite3.connect(str(tournament_cache_path))
    after = _date_to_unix(start_date) if start_date else None
    before = _date_to_unix(end_date) if end_date else None
    sql = """
        SELECT DISTINCT event_slug
        FROM tournaments
        WHERE event_slug IS NOT NULL
          AND event_slug != ''
          AND videogame_name = 'Super Smash Bros. Ultimate'
    """
    params: list[Any] = []
    if after is not None:
        sql += " AND start_at >= ?"
        params.append(after)
    if before is not None:
        sql += " AND start_at <= ?"
        params.append(before)
    rows = conn.execute(sql, tuple(params)).fetchall()
    conn.close()
    return [str(r[0]) for r in rows]


def _in_region_tournament_ids(tournament_cache_path: Path) -> set[str]:
    conn = sqlite3.connect(str(tournament_cache_path))
    rows = conn.execute("SELECT DISTINCT tournament_id FROM tournaments").fetchall()
    conn.close()
    return {str(r[0]) for r in rows if r[0] is not None}


def _build_identity_map_live(
    client: StartGGClient,
    config: EloConfig,
    player_names_needed: set[str],
    conn: sqlite3.Connection,
    *,
    verbose: bool,
) -> dict[str, dict[str, str]]:
    now = int(time.time())
    mapping: dict[str, dict[str, str]] = {}
    for cname, uid, pid in conn.execute("SELECT canonical_name, user_id, player_id FROM player_identity").fetchall():
        if uid or pid:
            mapping[cname] = {"user_id": str(uid or ""), "player_id": str(pid or "")}

    # Reuse already-known identities across sponsor/prefix variants.
    variant_lookup: dict[str, dict[str, str]] = {}
    for known_name, ident in mapping.items():
        for v in _name_variants(known_name):
            variant_lookup.setdefault(v.casefold(), ident)
    for needed in list(player_names_needed):
        if needed in mapping:
            continue
        match = None
        for v in _name_variants(needed):
            match = variant_lookup.get(v.casefold())
            if match:
                break
        if match:
            mapping[needed] = {"user_id": str(match.get("user_id", "")), "player_id": str(match.get("player_id", ""))}

    unresolved = {n for n in player_names_needed if n not in mapping}
    unresolved_lower: dict[str, str] = {n.casefold(): n for n in unresolved}
    unresolved_variant: dict[str, str] = {}
    for n in unresolved:
        for v in _name_variants(n):
            unresolved_variant.setdefault(v.casefold(), n)
    if not unresolved:
        return mapping

    event_ids = _in_region_event_ids(config.processed_cache_path)
    if verbose:
        print(f"[LIVE] Resolving user IDs for {len(unresolved)} players from {len(event_ids)} events...")

    for i, event_id in enumerate(event_ids):
        if not unresolved:
            break
        page = 1
        while True:
            payload = client.gql(
                EVENT_ENTRANTS_QUERY,
                {"eventId": event_id, "page": page, "perPage": _safe_per_page(config.per_page)},
                max_retries=config.max_retries,
            )
            entrants = payload.get("data", {}).get("event", {}).get("entrants", {})
            nodes = entrants.get("nodes", [])
            total_pages = entrants.get("pageInfo", {}).get("totalPages", 1)

            for entrant in nodes:
                for part in entrant.get("participants", []) or []:
                    user = part.get("user") or {}
                    uid = user.get("id")
                    player = user.get("player") or {}
                    pid = player.get("id")
                    if uid is None:
                        continue
                    display_c = _canonical(_player_display(part.get("prefix"), part.get("gamerTag")), config.name_mappings)
                    tag_c = _canonical((part.get("gamerTag") or "").strip(), config.name_mappings)
                    for candidate in (display_c, tag_c):
                        if not candidate:
                            continue
                        resolved_key = None
                        if candidate in unresolved:
                            resolved_key = candidate
                        else:
                            cf = candidate.casefold()
                            resolved_key = unresolved_lower.get(cf) or unresolved_variant.get(cf)
                        if resolved_key:
                            mapping[resolved_key] = {"user_id": str(uid), "player_id": str(pid or "")}
                            conn.execute(
                                """
                                INSERT INTO player_identity (canonical_name, user_id, player_id, source_event_id, updated_at)
                                VALUES (?, ?, ?, ?, ?)
                                ON CONFLICT(canonical_name) DO UPDATE SET
                                    user_id=excluded.user_id,
                                    player_id=excluded.player_id,
                                    source_event_id=excluded.source_event_id,
                                    updated_at=excluded.updated_at
                                """,
                                (resolved_key, str(uid), str(pid or ""), str(event_id), now),
                            )
                            unresolved.discard(resolved_key)
                            unresolved_lower.pop(resolved_key.casefold(), None)
                            for v in _name_variants(resolved_key):
                                unresolved_variant.pop(v.casefold(), None)

            if page >= total_pages:
                break
            page += 1

        if verbose and (i + 1) % 10 == 0:
            print(f"[LIVE] Identity scan progress {i+1}/{len(event_ids)}; unresolved={len(unresolved)}")

    conn.commit()

    # Fast slug-based lookup before expensive fallback scan.
    if unresolved:
        for name in list(unresolved):
            resolved = False
            for slug in _slug_candidates_from_name(name):
                try:
                    payload = client.gql(
                        USER_BY_SLUG_QUERY,
                        {"slug": slug},
                        max_retries=config.max_retries,
                    )
                except Exception:
                    continue
                user = (payload.get("data") or {}).get("user") or {}
                player = user.get("player") or {}
                uid = str(user.get("id") or "")
                pid = str(player.get("id") or "")
                if uid and pid:
                    mapping[name] = {"user_id": uid, "player_id": pid}
                    conn.execute(
                        """
                        INSERT INTO player_identity
                        (canonical_name, user_id, player_id, source_event_id, updated_at)
                        VALUES (?, ?, ?, ?, ?)
                        ON CONFLICT(canonical_name) DO UPDATE SET
                            user_id=excluded.user_id,
                            player_id=excluded.player_id,
                            source_event_id=excluded.source_event_id,
                            updated_at=excluded.updated_at
                        """,
                        (name, uid, pid, slug, now),
                    )
                    unresolved.discard(name)
                    unresolved_lower.pop(name.casefold(), None)
                    for v in _name_variants(name):
                        unresolved_variant.pop(v.casefold(), None)
                    resolved = True
                    break
            if resolved:
                continue
        conn.commit()

    # Fallback path: processed_events table may be partial. Use tournament_cache event slugs.
    if unresolved:
        fallback_slugs = _event_slugs_from_tournament_cache(
            config.tournament_cache_path,
            start_date=config.start_date,
            end_date=config.end_date,
        )
        if verbose:
            print(
                f"[LIVE] Fallback identity scan from tournament_cache slugs: "
                f"{len(fallback_slugs)} events"
            )
        for j, event_slug in enumerate(fallback_slugs):
            if not unresolved:
                break
            try:
                payload = client.gql(
                    EVENT_ID_BY_SLUG_QUERY,
                    {"slug": event_slug},
                    max_retries=config.max_retries,
                )
                event = payload.get("data", {}).get("event") or {}
                event_id = str(event.get("id") or "")
                if not event_id:
                    continue
            except Exception:
                continue

            page = 1
            while True:
                payload = client.gql(
                    EVENT_ENTRANTS_QUERY,
                    {"eventId": event_id, "page": page, "perPage": _safe_per_page(config.per_page)},
                    max_retries=config.max_retries,
                )
                entrants = payload.get("data", {}).get("event", {}).get("entrants", {})
                nodes = entrants.get("nodes", [])
                total_pages = entrants.get("pageInfo", {}).get("totalPages", 1)

                for entrant in nodes:
                    for part in entrant.get("participants", []) or []:
                        user = part.get("user") or {}
                        uid = user.get("id")
                        player = user.get("player") or {}
                        pid = player.get("id")
                        if uid is None:
                            continue
                        display_c = _canonical(
                            _player_display(part.get("prefix"), part.get("gamerTag")),
                            config.name_mappings,
                        )
                        tag_c = _canonical(
                            (part.get("gamerTag") or "").strip(),
                            config.name_mappings,
                        )
                        for candidate in (display_c, tag_c):
                            if not candidate:
                                continue
                            resolved_key = None
                            if candidate in unresolved:
                                resolved_key = candidate
                            else:
                                cf = candidate.casefold()
                                resolved_key = unresolved_lower.get(cf) or unresolved_variant.get(cf)
                            if resolved_key:
                                mapping[resolved_key] = {
                                    "user_id": str(uid),
                                    "player_id": str(pid or ""),
                                }
                                conn.execute(
                                    """
                                    INSERT INTO player_identity
                                    (canonical_name, user_id, player_id, source_event_id, updated_at)
                                    VALUES (?, ?, ?, ?, ?)
                                    ON CONFLICT(canonical_name) DO UPDATE SET
                                        user_id=excluded.user_id,
                                        player_id=excluded.player_id,
                                        source_event_id=excluded.source_event_id,
                                        updated_at=excluded.updated_at
                                    """,
                                    (resolved_key, str(uid), str(pid or ""), str(event_id), now),
                                )
                                unresolved.discard(resolved_key)
                                unresolved_lower.pop(resolved_key.casefold(), None)
                                for v in _name_variants(resolved_key):
                                    unresolved_variant.pop(v.casefold(), None)

                if page >= total_pages:
                    break
                page += 1

            if verbose and (j + 1) % 10 == 0:
                print(
                    f"[LIVE] Fallback scan progress {j+1}/{len(fallback_slugs)}; "
                    f"unresolved={len(unresolved)}"
                )

        conn.commit()
    # If still unresolved, scan event sets to recover player IDs even when entrant->user linkage is missing.
    if unresolved:
        fallback_slugs = _event_slugs_from_tournament_cache(
            config.tournament_cache_path,
            start_date=config.start_date,
            end_date=config.end_date,
        )
        if verbose:
            print(
                f"[LIVE] Set-based identity fallback for unresolved players across "
                f"{len(fallback_slugs)} events..."
            )
        for j, event_slug in enumerate(fallback_slugs):
            if not unresolved:
                break
            try:
                payload = client.gql(
                    EVENT_ID_BY_SLUG_QUERY,
                    {"slug": event_slug},
                    max_retries=config.max_retries,
                )
                event = payload.get("data", {}).get("event") or {}
                event_id = str(event.get("id") or "")
                if not event_id:
                    continue
            except Exception:
                continue

            page = 1
            while True:
                try:
                    payload = client.gql(
                        EVENT_SETS_QUERY,
                        {"eventId": event_id, "page": page, "perPage": _safe_per_page(config.per_page)},
                        max_retries=config.max_retries,
                    )
                except Exception:
                    break
                sets_conn = payload.get("data", {}).get("event", {}).get("sets", {})
                nodes = sets_conn.get("nodes", []) or []
                total_pages = sets_conn.get("pageInfo", {}).get("totalPages", 1)
                for node in nodes:
                    for slot in node.get("slots") or []:
                        entrant = slot.get("entrant") or {}
                        for part in entrant.get("participants") or []:
                            user = part.get("user") or {}
                            uid = str(user.get("id") or "")
                            player = part.get("player") or {}
                            pid = str(player.get("id") or "")
                            if not pid:
                                continue
                            display_c = _canonical(
                                _player_display(part.get("prefix"), part.get("gamerTag")),
                                config.name_mappings,
                            )
                            tag_c = _canonical((part.get("gamerTag") or "").strip(), config.name_mappings)
                            for candidate in (display_c, tag_c):
                                if not candidate:
                                    continue
                                cf = candidate.casefold()
                                resolved_key = unresolved_lower.get(cf) or unresolved_variant.get(cf)
                                if not resolved_key:
                                    continue
                                mapping[resolved_key] = {"user_id": uid, "player_id": pid}
                                conn.execute(
                                    """
                                    INSERT INTO player_identity
                                    (canonical_name, user_id, player_id, source_event_id, updated_at)
                                    VALUES (?, ?, ?, ?, ?)
                                    ON CONFLICT(canonical_name) DO UPDATE SET
                                        user_id=excluded.user_id,
                                        player_id=excluded.player_id,
                                        source_event_id=excluded.source_event_id,
                                        updated_at=excluded.updated_at
                                    """,
                                    (resolved_key, uid, pid, str(event_id), now),
                                )
                                unresolved.discard(resolved_key)
                                unresolved_lower.pop(resolved_key.casefold(), None)
                                for v in _name_variants(resolved_key):
                                    unresolved_variant.pop(v.casefold(), None)
                                break
                if page >= total_pages:
                    break
                page += 1
            if verbose and (j + 1) % 10 == 0:
                print(
                    f"[LIVE] Set-based fallback progress {j+1}/{len(fallback_slugs)}; "
                    f"unresolved={len(unresolved)}"
                )
        conn.commit()
    # Last-resort identity resolution: try user(slug:) guesses from gamer tags.
    if unresolved:
        for name in list(unresolved):
            resolved = False
            for slug in _slug_candidates_from_name(name):
                try:
                    payload = client.gql(
                        USER_BY_SLUG_QUERY,
                        {"slug": slug},
                        max_retries=config.max_retries,
                    )
                except Exception:
                    continue
                user = (payload.get("data") or {}).get("user") or {}
                player = user.get("player") or {}
                uid = str(user.get("id") or "")
                pid = str(player.get("id") or "")
                if uid and pid:
                    mapping[name] = {"user_id": uid, "player_id": pid}
                    conn.execute(
                        """
                        INSERT INTO player_identity
                        (canonical_name, user_id, player_id, source_event_id, updated_at)
                        VALUES (?, ?, ?, ?, ?)
                        ON CONFLICT(canonical_name) DO UPDATE SET
                            user_id=excluded.user_id,
                            player_id=excluded.player_id,
                            source_event_id=excluded.source_event_id,
                            updated_at=excluded.updated_at
                        """,
                        (name, uid, pid, slug, now),
                    )
                    unresolved.discard(name)
                    unresolved_lower.pop(name.casefold(), None)
                    for v in _name_variants(name):
                        unresolved_variant.pop(v.casefold(), None)
                    resolved = True
                    break
            if resolved:
                continue
        conn.commit()
    if unresolved and verbose:
        print(f"[LIVE] unresolved players: {sorted(list(unresolved))[:10]}{'...' if len(unresolved)>10 else ''}")
    return mapping


class CancelledOOR(Exception):
    """Raised when an OOR fetch should be abandoned (best-effort between pages)."""
    pass


def _fetch_player_sets_live(
    client: StartGGClient,
    player_id: str,
    per_page: int,
    max_retries: int,
    *,
    cancel_check: Any = None,
    page_callback: Any = None,
    pr_window_start_unix: int | None = None,
    metrics_out: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    """Fetch set nodes for a player with cooperative cancellation and optional early-stop.

    When *pr_window_start_unix* is set, pagination stops early if **every** node on a
    page has ``event.tournament.startAt`` strictly before that timestamp.  This avoids
    fetching deep career history when the API returns sets in reverse-chronological order
    (observed default for ``player.sets``).  If any node on a page is missing ``startAt``,
    early-stop is suppressed for that page to avoid data loss.

    When *metrics_out* is provided (mutable dict), the function populates it with:
    ``pages_fetched``, ``total_pages``, ``early_stop``, ``wall_ms``.
    """
    t0 = time.monotonic()
    nodes_all: list[dict[str, Any]] = []
    current_per_page = min(per_page, PLAYER_SETS_PER_PAGE)
    page = 1
    early_stopped = False
    last_total_pages = 1
    while True:
        if cancel_check and cancel_check():
            raise CancelledOOR(f"Cancelled before page {page}")
        try:
            payload = client.gql(
                PLAYER_SETS_QUERY,
                {"playerId": str(player_id), "page": page, "perPage": current_per_page},
                max_retries=max_retries,
            )
        except RuntimeError as e:
            if "complexity limit exceeded" in str(e).lower() and current_per_page > 5:
                current_per_page = max(5, current_per_page // 2)
                nodes_all = []
                page = 1
                early_stopped = False
                continue
            raise
        sets_conn = payload.get("data", {}).get("player", {}).get("sets", {})
        nodes = sets_conn.get("nodes", []) or []
        nodes_all.extend(nodes)
        last_total_pages = sets_conn.get("pageInfo", {}).get("totalPages", 1)
        if page_callback:
            page_callback(page, last_total_pages, nodes)
        if page >= last_total_pages:
            break

        # Early-stop: if every node on this page predates the PR window, remaining
        # pages are even older and can be skipped.
        if pr_window_start_unix is not None and nodes:
            start_ats = []
            for n in nodes:
                sa = ((n.get("event") or {}).get("tournament") or {}).get("startAt")
                if sa is not None:
                    start_ats.append(int(sa))
            if start_ats and len(start_ats) == len(nodes) and max(start_ats) < pr_window_start_unix:
                early_stopped = True
                break

        page += 1

    if metrics_out is not None:
        metrics_out["pages_fetched"] = page
        metrics_out["total_pages"] = last_total_pages
        metrics_out["early_stop"] = early_stopped
        metrics_out["wall_ms"] = round((time.monotonic() - t0) * 1000)
    return nodes_all


# --- M4: Tournament catalog + batched tournamentIds filter ---

OOR_CATALOG_CHUNK_SIZE = 25


def fetch_oor_tournament_catalog(
    client: StartGGClient,
    after_unix: int,
    before_unix: int,
    in_region_tournament_ids: set[str],
    *,
    videogame_ids: list[int] | None = None,
    max_retries: int = 5,
    per_page: int = 50,
) -> list[str]:
    """Return tournament IDs in [after, before] that are NOT in-region.

    Paginates ``tournaments`` with ``afterDate / beforeDate / videogameIds`` and subtracts
    *in_region_tournament_ids*.
    """
    vids = videogame_ids or [SMASH_ULTIMATE_VIDEOGAME_ID]
    all_ids: list[str] = []
    page = 1
    while True:
        payload = client.gql(
            TOURNAMENTS_BY_GAME_DATE_QUERY,
            {
                "page": page,
                "perPage": per_page,
                "afterDate": after_unix,
                "beforeDate": before_unix,
                "videogameIds": [str(v) for v in vids],
            },
            max_retries=max_retries,
        )
        tourney_conn = payload.get("data", {}).get("tournaments", {})
        nodes = tourney_conn.get("nodes", []) or []
        for n in nodes:
            tid = str(n.get("id") or "")
            if tid and tid not in in_region_tournament_ids:
                all_ids.append(tid)
        total_pages = tourney_conn.get("pageInfo", {}).get("totalPages", 1)
        if page >= total_pages:
            break
        page += 1
    return all_ids


def _fetch_player_sets_by_tournaments(
    client: StartGGClient,
    player_id: str,
    tournament_ids: list[str],
    per_page: int,
    max_retries: int,
    *,
    cancel_check: Any = None,
    metrics_out: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    """Fetch a player's sets restricted to a known set of tournament IDs (chunked).

    Uses ``PLAYER_SETS_FILTERED_QUERY`` with ``filters: { tournamentIds }`` to avoid
    full-career pagination.
    """
    t0 = time.monotonic()
    nodes_all: list[dict[str, Any]] = []
    pages_total = 0
    current_per_page = min(per_page, PLAYER_SETS_PER_PAGE)
    for i in range(0, len(tournament_ids), OOR_CATALOG_CHUNK_SIZE):
        chunk = tournament_ids[i : i + OOR_CATALOG_CHUNK_SIZE]
        page = 1
        while True:
            if cancel_check and cancel_check():
                raise CancelledOOR("Cancelled during filtered set fetch")
            try:
                payload = client.gql(
                    PLAYER_SETS_FILTERED_QUERY,
                    {
                        "playerId": str(player_id),
                        "page": page,
                        "perPage": current_per_page,
                        "tournamentIds": chunk,
                    },
                    max_retries=max_retries,
                )
            except RuntimeError as e:
                if "complexity limit exceeded" in str(e).lower() and current_per_page > 5:
                    current_per_page = max(5, current_per_page // 2)
                    continue
                raise
            sets_conn = payload.get("data", {}).get("player", {}).get("sets", {})
            nodes = sets_conn.get("nodes", []) or []
            nodes_all.extend(nodes)
            total_pages = sets_conn.get("pageInfo", {}).get("totalPages", 1)
            pages_total += 1
            if page >= total_pages:
                break
            page += 1
    if metrics_out is not None:
        metrics_out["pages_fetched"] = pages_total
        metrics_out["total_pages"] = pages_total
        metrics_out["early_stop"] = False
        metrics_out["wall_ms"] = round((time.monotonic() - t0) * 1000)
        metrics_out["catalog_chunks"] = (len(tournament_ids) + OOR_CATALOG_CHUNK_SIZE - 1) // OOR_CATALOG_CHUNK_SIZE
    return nodes_all


def _fetch_event_placement_for_player_live(
    client: StartGGClient,
    event_id: str,
    player_id: str,
    per_page: int,
    max_retries: int,
) -> int | None:
    page = 1
    while True:
        payload = client.gql(EVENT_STANDINGS_QUERY, {"eventId": str(event_id), "page": page, "perPage": _safe_per_page(per_page)}, max_retries=max_retries)
        standings = payload.get("data", {}).get("event", {}).get("standings", {})
        nodes = standings.get("nodes", [])
        total_pages = standings.get("pageInfo", {}).get("totalPages", 1)
        for n in nodes:
            participants = ((n.get("entrant") or {}).get("participants") or [])
            for part in participants:
                pid = str(((part.get("player") or {}).get("id") or ""))
                if pid == str(player_id):
                    placement = n.get("placement")
                    return int(placement) if isinstance(placement, int) else None
        if page >= total_pages:
            break
        page += 1
    return None


def _dq_filtered_in_region_tournament_count(player_sets: list[dict[str, Any]], name: str) -> int:
    """Count tournaments where `name` has at least one non-DQ set (DQ = that player's score is -1)."""
    flags: dict[str, bool] = {}
    for s in player_sets:
        if s["p1"] != name and s["p2"] != name:
            continue
        tid = str(s.get("tournament_id") or "").strip()
        if not tid:
            continue
        is_dq = (s["p1_score"] == -1) if s["p1"] == name else (s["p2_score"] == -1)
        prev = flags.get(tid)
        if prev is None:
            flags[tid] = not is_dq
        elif not prev:
            flags[tid] = not is_dq
    return sum(1 for v in flags.values() if v)


def _get_live_player_report(
    *,
    client: StartGGClient,
    config: EloConfig,
    canonical_name: str,
    user_id: str,
    player_id: str,
    in_region_sets: list[dict[str, Any]],
    in_region_tournament_ids: set[str],
    verbose: bool,
    include_raw_player_sets: bool = False,
    cancel_check: Any = None,
    page_callback: Any = None,
    phase_callback: Any = None,
    preloaded_set_nodes: list[dict[str, Any]] | None = None,
    tournament_cache_lookup: Any = None,
    tournament_cache_store: Any = None,
    oor_catalog_tournament_ids: list[str] | None = None,
) -> dict[str, Any]:
    player_sets = [s for s in in_region_sets if s["p1"] == canonical_name or s["p2"] == canonical_name]
    in_wins = in_losses = 0
    in_region_tournament_count = _dq_filtered_in_region_tournament_count(player_sets, canonical_name)
    in_event_meta: dict[str, dict[str, Any]] = {}
    for s in player_sets:
        if s["event_slug"] not in in_event_meta:
            in_event_meta[s["event_slug"]] = {
                "event_id": s.get("event_id", ""),
                "event_slug": s.get("event_slug", ""),
                "tournament_id": s.get("tournament_id", ""),
                "tournament_name": s.get("tournament_name", ""),
            }
        if (s["p1"] == canonical_name and s["p1_score"] > s["p2_score"]) or (s["p2"] == canonical_name and s["p2_score"] > s["p1_score"]):
            in_wins += 1
        else:
            in_losses += 1

    in_region_placements: list[dict[str, Any]] = []
    _placement_events = [es for es, m in in_event_meta.items() if str(m.get("event_id") or "")]
    _pe_total = len(_placement_events)
    if phase_callback and _pe_total:
        phase_callback(
            "in_region_placements",
            {
                "canonical_name": canonical_name,
                "message": f"Fetching in-region bracket placements from start.gg ({_pe_total} event(s))",
                "total": _pe_total,
                "index": 0,
            },
        )
    for _pi, event_slug in enumerate(_placement_events):
        meta = in_event_meta[event_slug]
        event_id = str(meta.get("event_id") or "")
        if not event_id:
            continue
        if phase_callback:
            phase_callback(
                "in_region_placements",
                {
                    "canonical_name": canonical_name,
                    "message": f"Standings/placement for {event_slug}",
                    "total": _pe_total,
                    "index": _pi + 1,
                    "event_slug": event_slug,
                },
            )
        placement = _fetch_event_placement_for_player_live(
            client,
            event_id,
            player_id,
            EVENT_STANDINGS_PER_PAGE,
            config.max_retries,
        )
        ev_sets = [s for s in player_sets if s["event_slug"] == event_slug]
        ev_wins = 0
        ev_losses = 0
        for s in ev_sets:
            if (s["p1"] == canonical_name and s["p1_score"] > s["p2_score"]) or (
                s["p2"] == canonical_name and s["p2_score"] > s["p1_score"]
            ):
                ev_wins += 1
            else:
                ev_losses += 1
        in_region_placements.append(
            {
                "tournament_id": str(meta.get("tournament_id") or ""),
                "tournament_name": meta.get("tournament_name") or "",
                "event_slug": event_slug,
                "placement": placement,
                "wins": ev_wins,
                "losses": ev_losses,
            }
        )

    if preloaded_set_nodes is not None:
        player_set_nodes = preloaded_set_nodes
        if phase_callback:
            phase_callback(
                "set_history_cache_hit",
                {
                    "canonical_name": canonical_name,
                    "message": f"Set history CACHE HIT: using {len(player_set_nodes)} cached set node(s) — skipping Start.gg paginated fetch",
                    "set_nodes": len(player_set_nodes),
                },
            )
    else:
        _fetch_metrics: dict[str, Any] = {}
        if oor_catalog_tournament_ids is not None:
            if phase_callback:
                phase_callback(
                    "set_history_cache_miss",
                    {
                        "canonical_name": canonical_name,
                        "message": f"Fetching sets via tournament catalog ({len(oor_catalog_tournament_ids)} OOR tournament(s))",
                    },
                )
            player_set_nodes = _fetch_player_sets_by_tournaments(
                client, player_id, oor_catalog_tournament_ids,
                config.per_page, config.max_retries,
                cancel_check=cancel_check,
                metrics_out=_fetch_metrics,
            )
        else:
            if phase_callback:
                phase_callback(
                    "set_history_cache_miss",
                    {
                        "canonical_name": canonical_name,
                        "message": "Set history CACHE MISS: fetching full set history (paginated) from Start.gg",
                    },
                )
            _early_stop_ts: int | None = None
            if config.oor_early_stop_player_sets and config.start_date:
                _early_stop_ts = _date_to_unix(config.start_date)
            player_set_nodes = _fetch_player_sets_live(
                client, player_id, config.per_page, config.max_retries,
                cancel_check=cancel_check, page_callback=page_callback,
                pr_window_start_unix=_early_stop_ts,
                metrics_out=_fetch_metrics,
            )
        if phase_callback and _fetch_metrics:
            _method = "catalog" if oor_catalog_tournament_ids is not None else "paginated"
            phase_callback(
                "set_history_fetch_metrics",
                {
                    "canonical_name": canonical_name,
                    "message": (
                        f"Fetched {_fetch_metrics.get('pages_fetched', '?')}/{_fetch_metrics.get('total_pages', '?')} page(s) "
                        f"in {_fetch_metrics.get('wall_ms', '?')}ms ({_method})"
                        f"{' (early-stop)' if _fetch_metrics.get('early_stop') else ''}"
                    ),
                    **_fetch_metrics,
                    "method": _method,
                },
            )
    after = _date_to_unix(config.start_date) if config.start_date else None
    before = _date_to_unix(config.end_date) if config.end_date else None

    if phase_callback:
        phase_callback(
            "aggregating_sets",
            {
                "canonical_name": canonical_name,
                "message": f"Scanning {len(player_set_nodes)} set record(s) for out-of-region tournaments in the PR date range",
                "set_nodes": len(player_set_nodes),
            },
        )

    # Aggregate out-of-region stats from live player set history.
    out_events: dict[str, dict[str, Any]] = {}
    for node in player_set_nodes:
        event = node.get("event") or {}
        tournament = event.get("tournament") or {}
        tournament_id = str(tournament.get("id") or "")
        if not tournament_id or tournament_id in in_region_tournament_ids:
            continue
        start_at = int(tournament.get("startAt") or 0)
        if after is not None and start_at < after:
            continue
        if before is not None and start_at > before:
            continue
        event_id = str(event.get("id") or "")
        event_slug = event.get("slug") or ""
        key = event_slug or event_id
        if not key:
            continue
        if key not in out_events:
            out_events[key] = {
                "event_id": event_id,
                "event_slug": event_slug,
                "tournament_id": tournament_id,
                "tournament_name": tournament.get("name") or "",
                "start_at": start_at,
                "wins": 0,
                "losses": 0,
                "notable_wins": [],
                "notable_losses": [],
            }

        slots = node.get("slots") or []
        if len(slots) < 2:
            continue
        slot_a, slot_b = slots[0], slots[1]
        parts_a = (slot_a.get("entrant") or {}).get("participants") or []
        parts_b = (slot_b.get("entrant") or {}).get("participants") or []
        if not parts_a or not parts_b:
            continue
        pid_a = str(((parts_a[0].get("player") or {}).get("id") or ""))
        pid_b = str(((parts_b[0].get("player") or {}).get("id") or ""))
        score_a = _safe_slot_score(slot_a)
        score_b = _safe_slot_score(slot_b)
        if score_a is None or score_b is None:
            continue
        name_a = _player_display(parts_a[0].get("prefix"), parts_a[0].get("gamerTag"))
        name_b = _player_display(parts_b[0].get("prefix"), parts_b[0].get("gamerTag"))
        if pid_a == str(player_id):
            if score_a > score_b:
                out_events[key]["wins"] += 1
                out_events[key]["notable_wins"].append(name_b)
            else:
                out_events[key]["losses"] += 1
                out_events[key]["notable_losses"].append(name_b)
        elif pid_b == str(player_id):
            if score_b > score_a:
                out_events[key]["wins"] += 1
                out_events[key]["notable_wins"].append(name_a)
            else:
                out_events[key]["losses"] += 1
                out_events[key]["notable_losses"].append(name_a)

    out_events_list = sorted(out_events.values(), key=lambda x: x["start_at"], reverse=True)
    if config.max_out_region_tournaments is not None:
        allowed_tournaments: set[str] = set()
        limited_events: list[dict[str, Any]] = []
        for ev in out_events_list:
            tid = ev["tournament_id"]
            if tid in allowed_tournaments or len(allowed_tournaments) < config.max_out_region_tournaments:
                allowed_tournaments.add(tid)
                limited_events.append(ev)
        out_events_list = limited_events

    if phase_callback:
        tourney_names = [ev.get("tournament_name") or ev.get("event_slug") or "?" for ev in out_events_list]
        phase_callback(
            "oor_tournaments_discovered",
            {
                "canonical_name": canonical_name,
                "message": f"Discovered {len(out_events_list)} OOR tournament(s) in date range: {', '.join(tourney_names[:15])}{'…' if len(tourney_names) > 15 else ''}",
                "count": len(out_events_list),
                "tournaments": tourney_names,
            },
        )

    out_wins = sum(int(ev["wins"]) for ev in out_events_list)
    out_losses = sum(int(ev["losses"]) for ev in out_events_list)
    placements: list[dict[str, Any]] = []
    notable_wins: list[str] = []
    notable_losses: list[str] = []
    _oor_ev_total = len(out_events_list)
    _cache_hits = 0
    _cache_misses = 0
    for _oi, ev in enumerate(out_events_list):
        tid = ev["tournament_id"]
        t_label = ev.get("tournament_name") or ev.get("event_slug") or tid

        cached_result = tournament_cache_lookup(tid) if tournament_cache_lookup else None
        if cached_result is not None:
            _cache_hits += 1
            placement = cached_result.get("placement")
            c_wins = int(cached_result.get("wins", 0))
            c_losses = int(cached_result.get("losses", 0))
            c_nw = cached_result.get("notable_wins", [])
            c_nl = cached_result.get("notable_losses", [])
            if phase_callback:
                phase_callback(
                    "oor_tournament_cache_hit",
                    {
                        "canonical_name": canonical_name,
                        "message": f"Tournament {t_label}: CACHE HIT (W-L: {c_wins}-{c_losses}, placement: {placement})",
                        "tournament_id": tid,
                        "tournament_name": t_label,
                        "index": _oi + 1,
                        "total": _oor_ev_total,
                        "wins": c_wins,
                        "losses": c_losses,
                        "placement": placement,
                    },
                )
            notable_wins.extend(c_nw)
            notable_losses.extend(c_nl)
            placements.append({
                "tournament_id": tid,
                "tournament_name": ev["tournament_name"],
                "event_slug": ev["event_slug"],
                "placement": placement,
                "wins": c_wins,
                "losses": c_losses,
            })
            out_wins = out_wins - int(ev["wins"]) + c_wins
            out_losses = out_losses - int(ev["losses"]) + c_losses
            continue

        _cache_misses += 1
        event_id = str(ev["event_id"] or "")
        placement = None
        if phase_callback:
            phase_callback(
                "oor_tournament_cache_miss",
                {
                    "canonical_name": canonical_name,
                    "message": f"Tournament {t_label}: CACHE MISS — processing sets, fetching placement…",
                    "tournament_id": tid,
                    "tournament_name": t_label,
                    "index": _oi + 1,
                    "total": _oor_ev_total,
                },
            )
        if event_id:
            placement = _fetch_event_placement_for_player_live(
                client,
                event_id,
                player_id,
                EVENT_STANDINGS_PER_PAGE,
                config.max_retries,
            )
        notable_wins.extend(ev["notable_wins"])
        notable_losses.extend(ev["notable_losses"])
        result_row = {
            "tournament_id": tid,
            "tournament_name": ev["tournament_name"],
            "event_slug": ev["event_slug"],
            "event_id": event_id,
            "start_at": ev.get("start_at", 0),
            "placement": placement,
            "wins": ev["wins"],
            "losses": ev["losses"],
            "notable_wins": ev["notable_wins"],
            "notable_losses": ev["notable_losses"],
        }
        placements.append({
            "tournament_id": tid,
            "tournament_name": ev["tournament_name"],
            "event_slug": ev["event_slug"],
            "placement": placement,
            "wins": ev["wins"],
            "losses": ev["losses"],
        })
        if tournament_cache_store:
            tournament_cache_store(tid, result_row)
        if phase_callback:
            phase_callback(
                "oor_tournament_processed",
                {
                    "canonical_name": canonical_name,
                    "message": f"Tournament {t_label}: acquired {ev['wins'] + ev['losses']} set(s), placement: {placement}. Stored to cache.",
                    "tournament_id": tid,
                    "tournament_name": t_label,
                    "index": _oi + 1,
                    "total": _oor_ev_total,
                    "wins": ev["wins"],
                    "losses": ev["losses"],
                    "placement": placement,
                },
            )
        if verbose:
            print(
                f"    [LIVE OOR] {ev['tournament_name']} / {ev['event_slug']}: "
                f"W-L={ev['wins']}-{ev['losses']}, placement={placement}"
            )

    if phase_callback:
        phase_callback(
            "oor_tournament_summary",
            {
                "canonical_name": canonical_name,
                "message": f"Player {canonical_name}: {_cache_hits}/{_oor_ev_total} tournament(s) from cache, {_cache_misses} fetched live",
                "cache_hits": _cache_hits,
                "cache_misses": _cache_misses,
                "total": _oor_ev_total,
            },
        )

    def top_counts(names: list[str], limit: int = 10) -> list[tuple[str, int]]:
        counts: dict[str, int] = {}
        for n in names:
            counts[n] = counts.get(n, 0) + 1
        return sorted(counts.items(), key=lambda x: x[1], reverse=True)[:limit]

    report = {
        "canonical_name": canonical_name,
        "user_id": user_id,
        "in_region_tournaments": in_region_tournament_count,
        "in_region_wins": in_wins,
        "in_region_losses": in_losses,
        "in_region_placements": in_region_placements,
        "out_region_tournaments": len({ev["tournament_id"] for ev in out_events_list}),
        "out_region_wins": out_wins,
        "out_region_losses": out_losses,
        "out_region_placements": placements,
        "notable_out_wins": top_counts(notable_wins),
        "notable_out_losses": top_counts(notable_losses),
        # Keep full opponent history for explicit validation checks.
        "all_out_wins": notable_wins,
        "all_out_losses": notable_losses,
    }
    if include_raw_player_sets:
        report["raw_player_set_nodes"] = player_set_nodes
    return report


def _upsert_live_player_report(conn: sqlite3.Connection, report: dict[str, Any], elo_value: float) -> None:
    now = int(time.time())
    _upsert_player_summary(
        conn,
        canonical_name=report["canonical_name"],
        elo=elo_value,
        in_region_tournaments=report["in_region_tournaments"],
        in_region_wins=report["in_region_wins"],
        in_region_losses=report["in_region_losses"],
        out_region_tournaments=report["out_region_tournaments"],
        out_region_wins=report["out_region_wins"],
        out_region_losses=report["out_region_losses"],
    )
    for p in report.get("in_region_placements", []):
        conn.execute(
            """
            INSERT INTO player_tournament_stats
            (canonical_name, tournament_id, event_slug, tournament_name, region, placement, wins, losses, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(canonical_name, tournament_id, event_slug, region) DO UPDATE SET
                placement=excluded.placement,
                wins=excluded.wins,
                losses=excluded.losses,
                updated_at=excluded.updated_at
            """,
            (
                report["canonical_name"],
                p.get("tournament_id", ""),
                p.get("event_slug", ""),
                p.get("tournament_name", ""),
                "in_region",
                p.get("placement"),
                p.get("wins", 0),
                p.get("losses", 0),
                now,
            ),
        )
    for p in report["out_region_placements"]:
        conn.execute(
            """
            INSERT INTO player_tournament_stats
            (canonical_name, tournament_id, event_slug, tournament_name, region, placement, wins, losses, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(canonical_name, tournament_id, event_slug, region) DO UPDATE SET
                placement=excluded.placement,
                wins=excluded.wins,
                losses=excluded.losses,
                updated_at=excluded.updated_at
            """,
            (
                report["canonical_name"],
                p.get("tournament_id", ""),
                p.get("event_slug", ""),
                p.get("tournament_name", ""),
                "out_region",
                p.get("placement"),
                p.get("wins", 0),
                p.get("losses", 0),
                now,
            ),
        )
    conn.commit()


def run_demo(config: EloConfig, *, verbose: bool = True) -> None:
    token = os.environ.get("STARTGG_API_KEY", "")
    if not token:
        raise ValueError("STARTGG_API_KEY is required in env/.env")

    elo_all, sets_all = compute_elo(config)
    players = list(elo_all.keys())
    print("=" * 72)
    print("[DEMO] Part 3 - ELO + Derived Stats")
    print("=" * 72)
    print(
        "This demo shows: (1) ELO with all tournaments, (2) random tournament exclusion "
        "and its effect, (3) random player comparison with live out-of-region data, "
        "(4) head-to-head, in/out-region placements, notable wins/losses."
    )
    print("\n--- Step 1: Baseline ELO (all tournaments included) ---")
    print(f"Sets used: {len(sets_all)}")
    print(f"Players in ELO pool: {len(players)}")
    print("\n[TOP 10 ELO - all tournaments]")
    for i, (name, score) in enumerate(list(elo_all.items())[:10], 1):
        print(f"  {i:2}. {name}: {score:.2f}")
    print("\n[TOP 20 ELO - all tournaments]")
    for i, (name, score) in enumerate(list(elo_all.items())[:20], 1):
        print(f"  {i:2}. {name}: {score:.2f}")

    events = sorted({s["event_slug"] for s in sets_all if s["event_slug"]})
    excluded_event = random.choice(events)
    cfg2 = EloConfig(
        tournament_cache_path=config.tournament_cache_path,
        processed_cache_path=config.processed_cache_path,
        player_db_path=config.player_db_path,
        name_mappings=config.name_mappings,
        start_date=config.start_date,
        end_date=config.end_date,
        exclude_event_slugs={excluded_event},
        exclude_tournament_ids=config.exclude_tournament_ids,
        exclude_tournament_names=config.exclude_tournament_names,
        k_factor=config.k_factor,
        initial_elo=config.initial_elo,
        per_page=config.per_page,
        max_retries=config.max_retries,
        max_out_region_tournaments=config.max_out_region_tournaments,
    )
    elo_without, _ = compute_elo(cfg2)
    print("\n--- Step 2: Tournament exclusion demo (random event removed) ---")
    print(f"[RANDOM EXCLUSION] Event removed this run: {excluded_event}")
    print("(Different event each run to prove the include/exclude logic works.)")
    print("\n[TOP 20 ELO - one random event removed]")
    for i, (name, score) in enumerate(list(elo_without.items())[:20], 1):
        print(f"  {i:2}. {name}: {score:.2f}")

    # Compare only players present in both rankings to avoid fallback-to-initial artifacts.
    changes: list[tuple[str, float]] = []
    common_players = set(elo_all.keys()) & set(elo_without.keys())
    for name in common_players:
        s1 = elo_all[name]
        s2 = elo_without[name]
        if abs(s2 - s1) > 0.01:
            changes.append((name, s2 - s1))
    changes.sort(key=lambda x: abs(x[1]), reverse=True)
    print("\n[CHANGED ELO PLAYERS - max 5] (players whose ELO changed after exclusion)")
    for name, delta in changes[:5]:
        sign = "+" if delta >= 0 else ""
        print(f"  {name}: {sign}{delta:.2f}")

    client = StartGGClient(token)
    pdb = _init_player_db(config.player_db_path)
    counts_before = _player_db_counts(pdb)
    print(
        f"\n[PLAYER DB - BEFORE LIVE UPDATE] identity={counts_before['identity']}, "
        f"summary={counts_before['summary']}, tournament_stats={counts_before['tournament_stats']}"
    )

    p1, p2, identity = _choose_two_resolvable_players(
        players=players,
        conn=pdb,
        config=config,
        client=client,
        verbose=verbose,
    )
    print("\n--- Step 3: Random player comparison (live API, different each run) ---")
    print("[RANDOM PLAYER COMPARISON] Two players picked at random to avoid cached results:")
    print(f"  Player A: {p1}")
    print(f"  Player B: {p2}")

    h2h = _compute_h2h(sets_all)
    a_wins, b_wins = _h2h_record(h2h, p1, p2)

    in_region_ids = _in_region_tournament_ids(config.tournament_cache_path)
    player_reports: dict[str, dict[str, Any]] = {}

    for player in (p1, p2):
        if player not in identity:
            print(f"\n  [LIVE] user_id not resolved for {player}; out-of-region skipped")
            continue
        uid = identity[player]["user_id"]
        pid = identity[player]["player_id"]
        print(f"\n  [LIVE] Fetching out-of-region for {player} (user_id={uid})")
        report = _get_live_player_report(
            client=client,
            config=config,
            canonical_name=player,
            user_id=uid,
            player_id=pid,
            in_region_sets=sets_all,
            in_region_tournament_ids=in_region_ids,
            verbose=verbose,
        )
        player_reports[player] = report
        _upsert_live_player_report(pdb, report, elo_all.get(player, config.initial_elo))
        print(f"  [REPORT] {player}")
        print(f"    In-region tournaments={report['in_region_tournaments']} W-L={report['in_region_wins']}-{report['in_region_losses']}")
        if report["in_region_placements"]:
            print("    In-region placements sample:")
            for row in report["in_region_placements"][:5]:
                print(f"      - {row['tournament_name']} :: placement={row['placement']} W-L={row['wins']}-{row['losses']}")
        print(f"    Out-region tournaments={report['out_region_tournaments']} W-L={report['out_region_wins']}-{report['out_region_losses']}")
        if report["out_region_placements"]:
            print("    Out-region placements sample:")
            for row in report["out_region_placements"][:5]:
                print(f"      - {row['tournament_name']} :: placement={row['placement']} W-L={row['wins']}-{row['losses']}")
        if report["notable_out_wins"]:
            print(f"    Notable wins sample: {report['notable_out_wins'][:5]}")
        if report["notable_out_losses"]:
            print(f"    Notable losses sample: {report['notable_out_losses'][:5]}")
        # Explicit Port Priority 9 check
        pp9 = _get_port_priority_9_placements(report)
        if pp9:
            print(f"    [PORT PRIORITY 9 CHECK] Found {len(pp9)} placement(s): {[r.get('tournament_name') for r in pp9]}")

    counts_after = _player_db_counts(pdb)
    print(
        f"\n[PLAYER DB - AFTER LIVE UPDATE] identity={counts_after['identity']}, "
        f"summary={counts_after['summary']}, tournament_stats={counts_after['tournament_stats']}"
    )

    print("\n--- Step 4: Head-to-head ---")
    print(f"[HEAD-TO-HEAD] {p1} vs {p2}: {a_wins}-{b_wins}")
    print(f"[HEAD-TO-HEAD] ELO: {p1}={elo_all.get(p1, config.initial_elo):.2f}, {p2}={elo_all.get(p2, config.initial_elo):.2f}")

    if p1 in player_reports and p2 in player_reports:
        rec1 = _build_player_opponent_records(p1, sets_all, player_reports[p1])
        rec2 = _build_player_opponent_records(p2, sets_all, player_reports[p2])

        shared_wins = [
            opp for opp in rec1
            if rec1[opp]["wins"] > 0 and rec2.get(opp, {}).get("wins", 0) > 0
        ]
        shared_wins.sort(key=lambda opp: rec1[opp]["wins"] + rec2[opp]["wins"], reverse=True)
        print(f"\n[HEAD-TO-HEAD] Common opponents both players beat: {len(shared_wins)}")
        for opp in shared_wins:
            print(
                f"  {opp}: {p1} {rec1[opp]['wins']}-{rec1[opp]['losses']} | "
                f"{p2} {rec2[opp]['wins']}-{rec2[opp]['losses']}"
            )

        shared_losses = [
            opp for opp in rec1
            if rec1[opp]["losses"] > 0 and rec2.get(opp, {}).get("losses", 0) > 0
        ]
        shared_losses.sort(key=lambda opp: rec1[opp]["losses"] + rec2[opp]["losses"], reverse=True)
        print(f"\n[HEAD-TO-HEAD] Common opponents both players lost to: {len(shared_losses)}")
        for opp in shared_losses:
            print(
                f"  {opp}: {p1} {rec1[opp]['wins']}-{rec1[opp]['losses']} | "
                f"{p2} {rec2[opp]['wins']}-{rec2[opp]['losses']}"
            )

        t1 = _tournament_summary_rows(player_reports[p1])
        t2 = _tournament_summary_rows(player_reports[p2])
        shared_t_keys = sorted(
            set(t1.keys()) & set(t2.keys()),
            key=lambda k: str(t1[k].get("tournament_name") or t2[k].get("tournament_name") or ""),
        )
        print(f"\n[HEAD-TO-HEAD] Tournaments both attended: {len(shared_t_keys)}")
        for k in shared_t_keys:
            a = t1[k]
            b = t2[k]
            tname = a.get("tournament_name") or b.get("tournament_name") or "(unknown)"
            print(f"  {tname}")
            print(
                f"    {p1}: best_place={a.get('best_placement')} W-L={a.get('wins', 0)}-{a.get('losses', 0)} "
                f"events={len(a.get('event_rows', []))}"
            )
            print(
                f"    {p2}: best_place={b.get('best_placement')} W-L={b.get('wins', 0)}-{b.get('losses', 0)} "
                f"events={len(b.get('event_rows', []))}"
            )
    else:
        print("[HEAD-TO-HEAD] Could not compute expanded comparison because one live report was unavailable.")

    # Explicit Part 3 validation case from all-functions.ipynb:
    # Nov 8-9, 2025 for Lui$ should include Port Priority 9; check for Syrup win and Light loss.
    print("\n--- Step 5: Explicit Port Priority 9 validation (Lui$ on Nov 8-9, 2025) ---")
    validation_cfg = EloConfig(
        tournament_cache_path=config.tournament_cache_path,
        processed_cache_path=config.processed_cache_path,
        player_db_path=config.player_db_path,
        name_mappings=config.name_mappings,
        start_date="2025-11-08",
        end_date="2025-11-09",
        exclude_event_slugs=config.exclude_event_slugs,
        exclude_tournament_ids=config.exclude_tournament_ids,
        exclude_tournament_names=config.exclude_tournament_names,
        k_factor=config.k_factor,
        initial_elo=config.initial_elo,
        per_page=config.per_page,
        max_retries=config.max_retries,
        max_out_region_tournaments=config.max_out_region_tournaments,
    )
    try:
        lui_name = "Team Var$ | Lui$"
        lui_identity = _build_identity_map_live(client, validation_cfg, {lui_name}, pdb, verbose=False)
        if lui_name not in lui_identity:
            print("[PP9] Could not resolve start.gg identity for Team Var$ | Lui$.")
            pdb.close()
            return
        lui_report = _get_live_player_report(
            client=client,
            config=validation_cfg,
            canonical_name=lui_name,
            user_id=lui_identity[lui_name]["user_id"],
            player_id=lui_identity[lui_name]["player_id"],
            in_region_sets=sets_all,
            in_region_tournament_ids=in_region_ids,
            verbose=False,
            include_raw_player_sets=True,
        )
        pp9 = _get_port_priority_9_placements(lui_report)
        all_wins = [str(x) for x in lui_report.get("all_out_wins", [])]
        all_losses = [str(x) for x in lui_report.get("all_out_losses", [])]
        has_syrup_win = _contains_opponent(all_wins, "Syrup")
        has_light_loss = _contains_opponent(all_losses, "Light")
        print(f"[PP9] Port Priority 9 rows found: {len(pp9)}")
        print(f"[PP9] Includes win vs Syrup: {has_syrup_win}")
        print(f"[PP9] Includes loss vs Light: {has_light_loss}")
        if pp9:
            for row in pp9:
                print(
                    f"[PP9] {row.get('tournament_name', '')} :: "
                    f"placement={row.get('placement')} W-L={row.get('wins', 0)}-{row.get('losses', 0)}"
                )
        pp9_nodes = []
        for node in lui_report.get("raw_player_set_nodes", []) or []:
            event = node.get("event") or {}
            tournament = event.get("tournament") or {}
            tname = str(tournament.get("name") or "")
            if PORT_PRIORITY_9_TOURNAMENT.lower() in tname.lower():
                pp9_nodes.append(node)

        pp9_results: list[dict[str, Any]] = []
        pid_for_lui = str(lui_identity[lui_name]["player_id"])
        for node in pp9_nodes:
            one = _extract_player_set_result(node, pid_for_lui)
            if one:
                pp9_results.append(one)

        print(f"[PP9] Raw matching set nodes count: {len(pp9_nodes)}")
        if pp9_results:
            print("[PP9] Set-level results for Lui$ at Port Priority 9:")
            for r in pp9_results:
                print(f"  {r['result']} vs {r['opponent']} ({r['player_score']}-{r['opponent_score']})")

        # Filter to Ultimate Singles only, extract Lui$ data for verification
        ULTIMATE_SINGLES = "ultimate-singles"
        lui_singles: list[dict[str, Any]] = []
        for node in pp9_nodes:
            event = node.get("event") or {}
            slug = str(event.get("slug") or "")
            if ULTIMATE_SINGLES not in slug.lower():
                continue
            one = _extract_player_set_result(node, pid_for_lui)
            if one:
                ev_name = str(event.get("name") or "Ultimate Singles")
                lui_singles.append({
                    "set_id": node.get("id"),
                    "event": ev_name,
                    "opponent": one["opponent"],
                    "result": one["result"],
                    "score": f"{one['player_score']}-{one['opponent_score']}",
                })

        print(f"\n[PP9] Lui$ Ultimate Singles at Port Priority 9 ({len(lui_singles)} sets):")
        if lui_singles:
            print(json.dumps(lui_singles, indent=2, ensure_ascii=False))
        else:
            print("  (none)")
    except Exception as e:
        print(f"[PP9] Validation check could not complete: {e}")

    pdb.close()


def show_player_report(config: EloConfig, player_name: str, user_id_override: str | None = None) -> None:
    token = os.environ.get("STARTGG_API_KEY", "")
    if not token:
        raise ValueError("STARTGG_API_KEY is required in env/.env")

    elo, sets = compute_elo(config)
    canonical_player = _canonical(player_name, config.name_mappings)
    client = StartGGClient(token)
    pdb = _init_player_db(config.player_db_path)
    if user_id_override:
        uid = str(user_id_override)
        pid = ""
    else:
        identity = _build_identity_map_live(client, config, {canonical_player}, pdb, verbose=True)
        if canonical_player not in identity:
            print(f"Could not resolve user_id for {canonical_player}")
            pdb.close()
            return
        uid = identity[canonical_player]["user_id"]
        pid = identity[canonical_player]["player_id"]

    if not pid and uid:
        # Resolve player_id from user(id) when caller provides only user_id.
        payload = client.gql(
            "query UserPlayer($uID: ID!) { user(id: $uID) { id player { id gamerTag } } }",
            {"uID": str(uid)},
            max_retries=config.max_retries,
        )
        pid = str((((payload.get("data") or {}).get("user") or {}).get("player") or {}).get("id") or "")
        if not pid:
            print(f"Could not resolve player_id for user_id={uid}")
            pdb.close()
            return
    in_region_ids = _in_region_tournament_ids(config.tournament_cache_path)
    report = _get_live_player_report(
        client=client,
        config=config,
        canonical_name=canonical_player,
        user_id=uid,
        player_id=pid,
        in_region_sets=sets,
        in_region_tournament_ids=in_region_ids,
        verbose=True,
    )
    _upsert_live_player_report(pdb, report, elo.get(canonical_player, config.initial_elo))

    print("\n=== PLAYER REPORT ===")
    print(f"Player: {canonical_player}")
    print(f"User ID: {uid}")
    print(f"ELO: {elo.get(canonical_player, config.initial_elo):.2f}")
    print(f"In-region tournaments={report['in_region_tournaments']} W-L={report['in_region_wins']}-{report['in_region_losses']}")
    if report["in_region_placements"]:
        print("In-region placements sample:")
        for row in report["in_region_placements"][:10]:
            print(f"  - {row['tournament_name']} :: placement={row['placement']} W-L={row['wins']}-{row['losses']}")
    print(f"Out-region tournaments={report['out_region_tournaments']} W-L={report['out_region_wins']}-{report['out_region_losses']}")

    # Explicit Port Priority 9 check (validation per all-functions.ipynb)
    pp9 = _get_port_priority_9_placements(report)
    print("\n[PORT PRIORITY 9 CHECK]")
    if pp9:
        print(f"  Found {len(pp9)} Port Priority 9 placement(s):")
        for r in pp9:
            print(f"    {r.get('tournament_name', '')} :: placement={r.get('placement')} W-L={r.get('wins', 0)}-{r.get('losses', 0)}")
    else:
        print("  No Port Priority 9 tournaments in out-of-region placements.")

    if report["notable_out_wins"]:
        print(f"Notable wins sample: {report['notable_out_wins'][:10]}")
    if report["notable_out_losses"]:
        print(f"Notable losses sample: {report['notable_out_losses'][:10]}")
    pdb.close()


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(
        description="Part 3: ELO + derived stats. Default: full demo showing include/exclude, random comparison, etc."
    )
    parser.add_argument("--simple", action="store_true", help="Minimal output: sets, players, top N ELO only (no demo)")
    parser.add_argument("--player", default=None, help="Run one-player live report")
    parser.add_argument("--user-id", default=None, help="Optional explicit start.gg user id for --player")
    parser.add_argument("--start", default=None, help="Filter start date YYYY-MM-DD")
    parser.add_argument("--end", default=None, help="Filter end date YYYY-MM-DD")
    parser.add_argument("--exclude-event", action="append", default=[], help="Exclude event slug")
    parser.add_argument("--exclude-tournament-id", action="append", default=[], help="Exclude tournament ID")
    parser.add_argument("--exclude-tournament-name", action="append", default=[], help="Exclude tournament name substring")
    parser.add_argument("--max-oor-tournaments", type=int, default=20, help="Max out-of-region tournaments to inspect per player in live mode")
    parser.add_argument("--top", type=int, default=20, help="Top N ELO when using --simple")
    args = parser.parse_args()

    cfg = EloConfig(
        start_date=args.start,
        end_date=args.end,
        exclude_event_slugs=set(args.exclude_event),
        exclude_tournament_ids={str(x) for x in args.exclude_tournament_id},
        exclude_tournament_names=set(args.exclude_tournament_name),
        max_out_region_tournaments=args.max_oor_tournaments,
    )

    if args.player:
        show_player_report(cfg, args.player, user_id_override=args.user_id)
        return
    if args.simple:
        elo, sets = compute_elo(cfg)
        print(f"Sets used: {len(sets)}")
        print(f"Players: {len(elo)}")
        print(f"\nTop {args.top} ELO")
        for i, (name, score) in enumerate(list(elo.items())[: args.top], 1):
            print(f"  {i:2}. {name}: {score:.2f}")
        return

    # Default: full Part 3 demo (include/exclude, random comparison, live out-of-region, etc.)
    run_demo(cfg)


if __name__ == "__main__":
    main()
