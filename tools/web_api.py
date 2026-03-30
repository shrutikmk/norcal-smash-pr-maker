from __future__ import annotations

import argparse
import concurrent.futures
import csv
import errno
import hashlib
import io
import json
import math
import os
import random
import re
import sqlite3
import sys
import time as _time
import traceback
import uuid
from datetime import date, datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from threading import Lock, Thread
from typing import Any
from urllib.parse import parse_qs, urlparse
from zoneinfo import ZoneInfo

try:
    from dotenv import load_dotenv

    load_dotenv(Path(__file__).resolve().parent.parent / ".env")
except Exception:
    pass

PROJECT_ROOT = Path(__file__).resolve().parent.parent
BASE_DEMO_DIR = PROJECT_ROOT / "demo" / "base_demo"
if str(BASE_DEMO_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DEMO_DIR))

_IMPORT_ERROR: Exception | None = None
try:
    from elo_calculator import (  # type: ignore  # noqa: E402
        EloConfig, compute_elo, _compute_elo_from_sets, _load_in_region_sets,
        _expanded_head_to_head, _compute_h2h, _h2h_record,
        _in_region_tournament_ids, _init_player_db, _build_identity_map_live,
        _get_live_player_report, _upsert_live_player_report, StartGGClient,
        _build_player_opponent_records, CancelledOOR,
        fetch_oor_tournament_catalog, _fetch_player_sets_by_tournaments,
    )
    from player_ranking import _build_player_card, _build_ai_justification_prompt, _loss_to_tournament_ratio  # type: ignore  # noqa: E402
    import tournament_processor as tp  # type: ignore  # noqa: E402
    from tournament_processor import ProcessorConfig  # type: ignore  # noqa: E402
    from tournament_scraper import (  # type: ignore  # noqa: E402
        ScraperConfig,
        compute_week_ranges_missing,
        scrape_tournaments,
    )
    import recent_events as recent_events_tool  # noqa: E402
    import tournament_scraper as _ts_mod  # noqa: E402
except Exception as exc:  # pragma: no cover - environment dependency guard
    _IMPORT_ERROR = exc

ULT_RELEASE_DATE = date(2018, 12, 8)
GAME_FILTER = "Super Smash Bros. Ultimate"
# NorCal events are scheduled in local (Pacific) time; start.gg timestamps are UTC.
PACIFIC_TZ = ZoneInfo("America/Los_Angeles")
JOB_LOCK = Lock()
RECENT_EVENT_JOBS: dict[str, dict[str, Any]] = {}
DATE_RANGE_JOBS: dict[str, dict[str, Any]] = {}
COVERAGE_RESOLVE_JOBS: dict[str, dict[str, Any]] = {}
PR_MAKER_SCRAPE_JOBS: dict[str, dict[str, Any]] = {}
PR_MAKER_PROCESS_JOBS: dict[str, dict[str, Any]] = {}
OOR_WARM_JOBS: dict[str, dict[str, Any]] = {}
MIN_ENTRANTS = 16

# Ring buffer of recent server-side events for the web UI debug shelf (poll GET /api/debug/server-events).
_SERVER_DEBUG_LOCK = Lock()
_SERVER_DEBUG_EVENTS: list[dict[str, Any]] = []
_SERVER_DEBUG_SEQ = 0
_SERVER_DEBUG_CAP = 800


def server_debug_log(level: str, source: str, message: str, detail: str = "") -> None:
    """Append one line to the in-memory debug feed (thread-safe)."""
    global _SERVER_DEBUG_SEQ
    with _SERVER_DEBUG_LOCK:
        _SERVER_DEBUG_SEQ += 1
        seq = _SERVER_DEBUG_SEQ
        now = _time.time()
        lt = _time.localtime(now)
        frac = int((now % 1) * 1000)
        ts = f"{lt.tm_hour:02d}:{lt.tm_min:02d}:{lt.tm_sec:02d}.{frac:03d}"
        _SERVER_DEBUG_EVENTS.append({
            "seq": seq, "ts": ts, "level": level, "source": source,
            "message": message, "detail": detail or "",
        })
        if len(_SERVER_DEBUG_EVENTS) > _SERVER_DEBUG_CAP:
            _SERVER_DEBUG_EVENTS[:] = _SERVER_DEBUG_EVENTS[-_SERVER_DEBUG_CAP:]

# Tracks which player name is currently being fetched for pair-first priority.
# Warm threads check this and yield when the pair fetch is active.
_OOR_ACTIVE_PAIR: set[str] = set()
_OOR_ACTIVE_PAIR_LOCK = Lock()

# Cancel registry: cancel_id -> True means "this fetch group should stop."
_OOR_CANCEL_REGISTRY: dict[str, bool] = {}
_OOR_CANCEL_LOCK = Lock()

# ---------------------------------------------------------------------------
# OOR report cache (SQLite-backed, keyed by PR Maker context fingerprint)
# ---------------------------------------------------------------------------
_OOR_CACHE_DB_PATH = Path(__file__).resolve().parent.parent / "data" / "oor_report_cache.db"
_oor_cache_lock = Lock()


def _oor_cache_conn() -> sqlite3.Connection:
    _OOR_CACHE_DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(_OOR_CACHE_DB_PATH), timeout=10)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS live_report_cache (
            context_hash TEXT NOT NULL,
            canonical_name TEXT NOT NULL,
            report_json TEXT NOT NULL,
            updated_at INTEGER NOT NULL,
            PRIMARY KEY (context_hash, canonical_name)
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS oor_player_fetch_state (
            context_hash TEXT NOT NULL,
            canonical_name TEXT NOT NULL,
            pages_fetched INTEGER NOT NULL DEFAULT 0,
            total_pages INTEGER NOT NULL DEFAULT 0,
            status TEXT NOT NULL DEFAULT 'pending',
            updated_at INTEGER NOT NULL,
            PRIMARY KEY (context_hash, canonical_name)
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS oor_event_row (
            context_hash TEXT NOT NULL,
            canonical_name TEXT NOT NULL,
            tournament_id TEXT NOT NULL,
            event_slug TEXT NOT NULL,
            event_id TEXT NOT NULL DEFAULT '',
            tournament_name TEXT NOT NULL DEFAULT '',
            start_at INTEGER NOT NULL DEFAULT 0,
            wins INTEGER NOT NULL DEFAULT 0,
            losses INTEGER NOT NULL DEFAULT 0,
            notable_wins_json TEXT NOT NULL DEFAULT '[]',
            notable_losses_json TEXT NOT NULL DEFAULT '[]',
            placement INTEGER,
            updated_at INTEGER NOT NULL,
            PRIMARY KEY (context_hash, canonical_name, tournament_id, event_slug)
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS oor_player_sets_cache (
            player_id TEXT NOT NULL PRIMARY KEY,
            nodes_json TEXT NOT NULL,
            fetched_at INTEGER NOT NULL
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS oor_player_sets_cache_v2 (
            player_id TEXT NOT NULL,
            window_hash TEXT NOT NULL,
            nodes_json TEXT NOT NULL,
            fetched_at INTEGER NOT NULL,
            PRIMARY KEY (player_id, window_hash)
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS oor_tournament_catalog (
            window_hash TEXT NOT NULL PRIMARY KEY,
            tournament_ids_json TEXT NOT NULL,
            fetched_at INTEGER NOT NULL
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS oor_tournament_result_cache (
            player_id TEXT NOT NULL,
            tournament_id TEXT NOT NULL,
            event_slug TEXT NOT NULL DEFAULT '',
            event_id TEXT NOT NULL DEFAULT '',
            tournament_name TEXT NOT NULL DEFAULT '',
            start_at INTEGER NOT NULL DEFAULT 0,
            wins INTEGER NOT NULL DEFAULT 0,
            losses INTEGER NOT NULL DEFAULT 0,
            notable_wins_json TEXT NOT NULL DEFAULT '[]',
            notable_losses_json TEXT NOT NULL DEFAULT '[]',
            placement INTEGER,
            updated_at INTEGER NOT NULL,
            PRIMARY KEY (player_id, tournament_id)
        )
    """)
    conn.commit()
    return conn


def _pr_maker_context_hash(
    start: str, end: str, event_slugs: list[str], merge_rules: list[dict[str, str]],
) -> str:
    blob = json.dumps({
        "s": start, "e": end,
        "slugs": sorted(event_slugs),
        "merges": sorted(
            [(str(r.get("keep", "")), str(r.get("drop", ""))) for r in merge_rules],
        ),
    }, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(blob.encode()).hexdigest()[:24]


def _oor_window_hash(start: str, end: str) -> str:
    """Stable hash of the PR date window — used as the set-history cache partition."""
    blob = f"{start}:{end}"
    return hashlib.sha256(blob.encode()).hexdigest()[:16]


def _cache_get_report(conn: sqlite3.Connection, ctx_hash: str, name: str) -> dict[str, Any] | None:
    row = conn.execute(
        "SELECT report_json FROM live_report_cache WHERE context_hash = ? AND canonical_name = ?",
        (ctx_hash, name),
    ).fetchone()
    if row:
        try:
            return json.loads(row[0])
        except json.JSONDecodeError:
            pass
    return None


def _cache_put_report(conn: sqlite3.Connection, ctx_hash: str, name: str, report: dict[str, Any]) -> None:
    conn.execute(
        """INSERT INTO live_report_cache (context_hash, canonical_name, report_json, updated_at)
           VALUES (?, ?, ?, ?)
           ON CONFLICT(context_hash, canonical_name)
           DO UPDATE SET report_json = excluded.report_json, updated_at = excluded.updated_at""",
        (ctx_hash, name, json.dumps(report), int(_time.time())),
    )
    conn.commit()


# ---------------------------------------------------------------------------
# Granular OOR event storage helpers
# ---------------------------------------------------------------------------

def _oor_fetch_state(conn: sqlite3.Connection, ctx_hash: str, name: str) -> str | None:
    """Return status ('pending', 'fetching', 'complete') or None if no row."""
    row = conn.execute(
        "SELECT status FROM oor_player_fetch_state WHERE context_hash = ? AND canonical_name = ?",
        (ctx_hash, name),
    ).fetchone()
    return row[0] if row else None


def _oor_upsert_event_row(
    conn: sqlite3.Connection,
    ctx_hash: str,
    name: str,
    ev: dict[str, Any],
) -> None:
    conn.execute(
        """INSERT INTO oor_event_row
           (context_hash, canonical_name, tournament_id, event_slug, event_id,
            tournament_name, start_at, wins, losses,
            notable_wins_json, notable_losses_json, placement, updated_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
           ON CONFLICT(context_hash, canonical_name, tournament_id, event_slug)
           DO UPDATE SET wins=excluded.wins, losses=excluded.losses,
                         notable_wins_json=excluded.notable_wins_json,
                         notable_losses_json=excluded.notable_losses_json,
                         placement=excluded.placement,
                         updated_at=excluded.updated_at""",
        (
            ctx_hash, name,
            str(ev.get("tournament_id", "")),
            str(ev.get("event_slug", "")),
            str(ev.get("event_id", "")),
            str(ev.get("tournament_name", "")),
            int(ev.get("start_at", 0)),
            int(ev.get("wins", 0)),
            int(ev.get("losses", 0)),
            json.dumps(ev.get("notable_wins", [])),
            json.dumps(ev.get("notable_losses", [])),
            ev.get("placement"),
            int(_time.time()),
        ),
    )


def _oor_set_fetch_state(
    conn: sqlite3.Connection,
    ctx_hash: str,
    name: str,
    *,
    pages_fetched: int,
    total_pages: int,
    status: str,
) -> None:
    conn.execute(
        """INSERT INTO oor_player_fetch_state
           (context_hash, canonical_name, pages_fetched, total_pages, status, updated_at)
           VALUES (?, ?, ?, ?, ?, ?)
           ON CONFLICT(context_hash, canonical_name)
           DO UPDATE SET pages_fetched=excluded.pages_fetched, total_pages=excluded.total_pages,
                         status=excluded.status, updated_at=excluded.updated_at""",
        (ctx_hash, name, pages_fetched, total_pages, status, int(_time.time())),
    )
    conn.commit()


# ---------------------------------------------------------------------------
# Context-independent OOR caches (keyed by player_id, not context_hash)
# ---------------------------------------------------------------------------

_OOR_SETS_CACHE_TTL = 604800  # 7 days (window-keyed cache; old 24h TTL no longer needed)


def _oor_get_player_sets(
    conn: sqlite3.Connection,
    player_id: str,
    window_hash: str = "",
    ttl_seconds: int = _OOR_SETS_CACHE_TTL,
) -> tuple[list[dict[str, Any]] | None, int]:
    """Return (cached_nodes_or_None, fetched_at_unix).  Returns (None, 0) on miss.

    When *window_hash* is provided, looks up the v2 window-keyed table first.
    Falls back to the legacy table when no window_hash is given (backwards compat).
    """
    if window_hash:
        row = conn.execute(
            "SELECT nodes_json, fetched_at FROM oor_player_sets_cache_v2 WHERE player_id = ? AND window_hash = ?",
            (str(player_id), window_hash),
        ).fetchone()
    else:
        row = conn.execute(
            "SELECT nodes_json, fetched_at FROM oor_player_sets_cache WHERE player_id = ?",
            (str(player_id),),
        ).fetchone()
    if not row:
        return None, 0
    fetched_at = int(row[1])
    if int(_time.time()) - fetched_at > ttl_seconds:
        return None, fetched_at
    try:
        return json.loads(row[0]), fetched_at
    except json.JSONDecodeError:
        return None, 0


def _oor_put_player_sets(
    conn: sqlite3.Connection,
    player_id: str,
    nodes: list[dict[str, Any]],
    window_hash: str = "",
) -> None:
    now = int(_time.time())
    nodes_blob = json.dumps(nodes)
    pid = str(player_id)
    if window_hash:
        conn.execute(
            """INSERT INTO oor_player_sets_cache_v2 (player_id, window_hash, nodes_json, fetched_at)
               VALUES (?, ?, ?, ?)
               ON CONFLICT(player_id, window_hash)
               DO UPDATE SET nodes_json = excluded.nodes_json, fetched_at = excluded.fetched_at""",
            (pid, window_hash, nodes_blob, now),
        )
    # Always update legacy table too so non-window-aware code paths stay warm.
    conn.execute(
        """INSERT INTO oor_player_sets_cache (player_id, nodes_json, fetched_at)
           VALUES (?, ?, ?)
           ON CONFLICT(player_id)
           DO UPDATE SET nodes_json = excluded.nodes_json, fetched_at = excluded.fetched_at""",
        (pid, nodes_blob, now),
    )
    conn.commit()


def _oor_get_tournament_result(conn: sqlite3.Connection, player_id: str, tournament_id: str) -> dict[str, Any] | None:
    row = conn.execute(
        """SELECT event_slug, event_id, tournament_name, start_at,
                  wins, losses, notable_wins_json, notable_losses_json, placement
           FROM oor_tournament_result_cache
           WHERE player_id = ? AND tournament_id = ?""",
        (str(player_id), str(tournament_id)),
    ).fetchone()
    if not row:
        return None
    return {
        "event_slug": row[0],
        "event_id": row[1],
        "tournament_name": row[2],
        "start_at": row[3],
        "wins": row[4],
        "losses": row[5],
        "notable_wins": json.loads(row[6]) if row[6] else [],
        "notable_losses": json.loads(row[7]) if row[7] else [],
        "placement": row[8],
        "tournament_id": str(tournament_id),
    }


def _oor_put_tournament_result(conn: sqlite3.Connection, player_id: str, tournament_id: str, result: dict[str, Any]) -> None:
    conn.execute(
        """INSERT INTO oor_tournament_result_cache
           (player_id, tournament_id, event_slug, event_id, tournament_name, start_at,
            wins, losses, notable_wins_json, notable_losses_json, placement, updated_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
           ON CONFLICT(player_id, tournament_id)
           DO UPDATE SET event_slug=excluded.event_slug, event_id=excluded.event_id,
                         tournament_name=excluded.tournament_name, start_at=excluded.start_at,
                         wins=excluded.wins, losses=excluded.losses,
                         notable_wins_json=excluded.notable_wins_json,
                         notable_losses_json=excluded.notable_losses_json,
                         placement=excluded.placement, updated_at=excluded.updated_at""",
        (
            str(player_id), str(tournament_id),
            str(result.get("event_slug", "")),
            str(result.get("event_id", "")),
            str(result.get("tournament_name", "")),
            int(result.get("start_at", 0)),
            int(result.get("wins", 0)),
            int(result.get("losses", 0)),
            json.dumps(result.get("notable_wins", [])),
            json.dumps(result.get("notable_losses", [])),
            result.get("placement"),
            int(_time.time()),
        ),
    )
    conn.commit()


_OOR_CATALOG_TTL = 604800  # 7 days


def _oor_get_tournament_catalog(conn: sqlite3.Connection, window_hash: str) -> list[str] | None:
    row = conn.execute(
        "SELECT tournament_ids_json, fetched_at FROM oor_tournament_catalog WHERE window_hash = ?",
        (window_hash,),
    ).fetchone()
    if not row:
        return None
    fetched_at = int(row[1])
    if int(_time.time()) - fetched_at > _OOR_CATALOG_TTL:
        return None
    try:
        return json.loads(row[0])
    except json.JSONDecodeError:
        return None


def _oor_put_tournament_catalog(conn: sqlite3.Connection, window_hash: str, tournament_ids: list[str]) -> None:
    conn.execute(
        """INSERT INTO oor_tournament_catalog (window_hash, tournament_ids_json, fetched_at)
           VALUES (?, ?, ?)
           ON CONFLICT(window_hash)
           DO UPDATE SET tournament_ids_json = excluded.tournament_ids_json, fetched_at = excluded.fetched_at""",
        (window_hash, json.dumps(tournament_ids), int(_time.time())),
    )
    conn.commit()


def _dq_filtered_attendance_counts(sets: list[dict[str, Any]]) -> dict[str, int]:
    """Per player: distinct tournament_ids with at least one non-DQ set (player score != -1)."""
    attendance: dict[str, dict[str, bool]] = {}
    for s in sets:
        tid = str(s.get("tournament_id") or "").strip()
        if not tid:
            continue
        for p in (s["p1"], s["p2"]):
            player_tourns = attendance.setdefault(p, {})
            is_dq = (s["p1_score"] == -1) if p == s["p1"] else (s["p2_score"] == -1)
            prev = player_tourns.get(tid)
            if prev is None:
                player_tourns[tid] = not is_dq
            elif not prev:
                player_tourns[tid] = not is_dq
    return {p: sum(1 for v in d.values() if v) for p, d in attendance.items()}


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


def _oor_rebuild_report_from_rows(
    conn: sqlite3.Connection,
    ctx_hash: str,
    name: str,
    in_region_sets: list[dict[str, Any]],
) -> dict[str, Any]:
    """Reconstruct a full report dict from granular oor_event_row + in-region sets."""
    player_sets = [s for s in in_region_sets if s["p1"] == name or s["p2"] == name]
    in_region_tournament_count = _dq_filtered_in_region_tournament_count(player_sets, name)
    in_wins = in_losses = 0
    for s in player_sets:
        if (s["p1"] == name and s["p1_score"] > s["p2_score"]) or \
           (s["p2"] == name and s["p2_score"] > s["p1_score"]):
            in_wins += 1
        else:
            in_losses += 1

    in_region_placements: list[dict[str, Any]] = []
    seen_events: dict[str, dict[str, Any]] = {}
    for s in player_sets:
        slug = s["event_slug"]
        if slug not in seen_events:
            seen_events[slug] = {
                "tournament_id": s.get("tournament_id", ""),
                "tournament_name": s.get("tournament_name", ""),
                "event_slug": slug,
                "wins": 0, "losses": 0,
            }
        rec = seen_events[slug]
        if (s["p1"] == name and s["p1_score"] > s["p2_score"]) or \
           (s["p2"] == name and s["p2_score"] > s["p1_score"]):
            rec["wins"] += 1
        else:
            rec["losses"] += 1
    for slug, rec in seen_events.items():
        in_region_placements.append({**rec, "placement": None})

    rows = conn.execute(
        """SELECT tournament_id, event_slug, event_id, tournament_name, start_at,
                  wins, losses, notable_wins_json, notable_losses_json, placement
           FROM oor_event_row
           WHERE context_hash = ? AND canonical_name = ?
           ORDER BY start_at DESC""",
        (ctx_hash, name),
    ).fetchall()

    out_tournaments: set[str] = set()
    out_wins = out_losses = 0
    all_notable_wins: list[str] = []
    all_notable_losses: list[str] = []
    out_region_placements: list[dict[str, Any]] = []
    for r in rows:
        tid, eslug, eid, tname, start_at, w, l, nw_json, nl_json, pl = r
        out_tournaments.add(tid)
        out_wins += w
        out_losses += l
        nw = json.loads(nw_json) if nw_json else []
        nl = json.loads(nl_json) if nl_json else []
        all_notable_wins.extend(nw)
        all_notable_losses.extend(nl)
        out_region_placements.append({
            "tournament_id": tid, "tournament_name": tname,
            "event_slug": eslug, "placement": pl,
            "wins": w, "losses": l,
        })

    def top_counts(names_list: list[str], limit: int = 10) -> list[tuple[str, int]]:
        counts: dict[str, int] = {}
        for n in names_list:
            counts[n] = counts.get(n, 0) + 1
        return sorted(counts.items(), key=lambda x: x[1], reverse=True)[:limit]

    return {
        "canonical_name": name,
        "in_region_tournaments": in_region_tournament_count,
        "in_region_wins": in_wins,
        "in_region_losses": in_losses,
        "in_region_placements": in_region_placements,
        "out_region_tournaments": len(out_tournaments),
        "out_region_wins": out_wins,
        "out_region_losses": out_losses,
        "out_region_placements": out_region_placements,
        "notable_out_wins": top_counts(all_notable_wins),
        "notable_out_losses": top_counts(all_notable_losses),
        "all_out_wins": all_notable_wins,
        "all_out_losses": all_notable_losses,
    }


def _is_client_disconnect(exc: BaseException) -> bool:
    if isinstance(exc, (BrokenPipeError, ConnectionResetError, ConnectionAbortedError)):
        return True
    if isinstance(exc, OSError):
        return exc.errno in (errno.EPIPE, errno.ECONNRESET, errno.ECONNABORTED)
    return False


def _ensure_runtime_deps() -> None:
    if _IMPORT_ERROR is not None:
        raise RuntimeError(
            "Missing runtime dependencies for tools/web_api.py. "
            "Use the project virtualenv/interpreter where requirements are installed."
        ) from _IMPORT_ERROR


def _date_to_str(d: date) -> str:
    return d.strftime("%Y-%m-%d")


def _parse_iso_date(raw: str) -> date:
    try:
        return datetime.strptime(raw, "%Y-%m-%d").date()
    except ValueError as exc:
        raise ValueError(f"Invalid date {raw!r}; expected YYYY-MM-DD") from exc


def _merge_contiguous_ranges(ranges: list[tuple[date, date]]) -> list[tuple[date, date]]:
    if not ranges:
        return []
    sorted_r = sorted(ranges, key=lambda t: (t[0], t[1]))
    out: list[tuple[date, date]] = [sorted_r[0]]
    for a, b in sorted_r[1:]:
        la, lb = out[-1]
        if a <= lb + timedelta(days=1):
            out[-1] = (la, max(lb, b))
        else:
            out.append((a, b))
    return out


def _coverage_resolve_worker(job_id: str, ranges_payload: list[dict[str, str]]) -> None:
    try:
        _ensure_runtime_deps()
        cache_path = PROJECT_ROOT / "data" / "tournament_cache.db"
        parsed: list[tuple[date, date]] = []
        for item in ranges_payload:
            s_raw = str(item.get("start") or "").strip()
            e_raw = str(item.get("end") or "").strip()
            if not s_raw or not e_raw:
                continue
            sd = _parse_iso_date(s_raw)
            ed = _parse_iso_date(e_raw)
            if ed < sd:
                sd, ed = ed, sd
            parsed.append((sd, ed))

        if not parsed:
            missing = compute_week_ranges_missing(
                cache_path,
                game_filter=GAME_FILTER,
                start_date=ULT_RELEASE_DATE,
                end_date=date.today(),
            )
            parsed = list(missing)

        merged = _merge_contiguous_ranges(parsed)
        total = len(merged)
        with JOB_LOCK:
            COVERAGE_RESOLVE_JOBS[job_id]["totalWindows"] = total
            COVERAGE_RESOLVE_JOBS[job_id]["phase"] = "scraping"

        for i, (sd, ed) in enumerate(merged):
            with JOB_LOCK:
                COVERAGE_RESOLVE_JOBS[job_id]["currentWindow"] = i + 1
                COVERAGE_RESOLVE_JOBS[job_id]["detail"] = f"{_date_to_str(sd)} -> {_date_to_str(ed)}"
            scrape_tournaments(
                ScraperConfig(
                    start_date=_date_to_str(sd),
                    end_date=_date_to_str(ed),
                    game_filter=GAME_FILTER,
                    min_entrants=16,
                    regions=["bay", "sacramento"],
                ),
                verbose=False,
            )

        remaining = compute_week_ranges_missing(
            cache_path,
            game_filter=GAME_FILTER,
            start_date=ULT_RELEASE_DATE,
            end_date=date.today(),
        )
        remaining_payload = [
            {"start": _date_to_str(a), "end": _date_to_str(b), "days": (b - a).days + 1} for a, b in remaining
        ]
        with JOB_LOCK:
            COVERAGE_RESOLVE_JOBS[job_id]["status"] = "done"
            COVERAGE_RESOLVE_JOBS[job_id]["phase"] = "done"
            COVERAGE_RESOLVE_JOBS[job_id]["remainingMissing"] = remaining_payload
            COVERAGE_RESOLVE_JOBS[job_id]["remainingCount"] = len(remaining_payload)
    except Exception as exc:
        with JOB_LOCK:
            COVERAGE_RESOLVE_JOBS[job_id]["status"] = "error"
            COVERAGE_RESOLVE_JOBS[job_id]["error"] = str(exc)


def _filter_and_rank(
    elo: dict[str, float],
    *,
    query: str,
    max_players: int,
) -> tuple[list[dict[str, Any]], int]:
    q = query.strip().casefold()
    rows: list[dict[str, Any]] = []
    rank = 0
    for player, score in elo.items():
        rank += 1
        if q and q not in str(player).casefold():
            continue
        rows.append(
            {
                "rank": rank,
                "player": str(player),
                "elo": round(float(score), 2),
            }
        )
        if len(rows) >= max_players:
            break
    return rows, len(elo)


def _build_elo_payload(
    *,
    mode: str,
    start_date: str | None,
    end_date: str | None,
    query: str,
    max_players: int,
) -> dict[str, Any]:
    _ensure_runtime_deps()
    if mode not in {"all-time", "date-range"}:
        raise ValueError("mode must be either 'all-time' or 'date-range'")

    if mode == "date-range":
        if not start_date or not end_date:
            raise ValueError("start and end are required for date-range mode")
        start_d = _parse_iso_date(start_date)
        end_d = _parse_iso_date(end_date)
        if end_d < start_d:
            raise ValueError("end must be on or after start")

        scrape_cfg = ScraperConfig(
            start_date=_date_to_str(start_d),
            end_date=_date_to_str(end_d),
            game_filter=GAME_FILTER,
            min_entrants=16,
            regions=["bay", "sacramento"],
        )
        scrape_tournaments(scrape_cfg, verbose=False)

        _process_tournaments_with_progress(
            ProcessorConfig(
                start_date=_date_to_str(start_d),
                end_date=_date_to_str(end_d),
                game_filter=GAME_FILTER,
                min_entrants=16,
            ),
            progress_cb=None,
        )

        elo_cfg = EloConfig(
            start_date=_date_to_str(start_d),
            end_date=_date_to_str(end_d),
        )
        elo, _ = compute_elo(elo_cfg)
        rows, total_players = _filter_and_rank(elo, query=query, max_players=max_players)
        return {
            "mode": mode,
            "startDate": _date_to_str(start_d),
            "endDate": _date_to_str(end_d),
            "rankings": rows,
            "totalPlayers": total_players,
            "missingRanges": [],
        }

    elo, _ = compute_elo(EloConfig(start_date=None, end_date=None))
    rows, total_players = _filter_and_rank(elo, query=query, max_players=max_players)
    missing = compute_week_ranges_missing(
        PROJECT_ROOT / "data" / "tournament_cache.db",
        game_filter=GAME_FILTER,
        start_date=ULT_RELEASE_DATE,
        end_date=date.today(),
    )
    missing_payload = [
        {"start": _date_to_str(a), "end": _date_to_str(b), "days": (b - a).days + 1}
        for a, b in missing
    ]
    return {
        "mode": mode,
        "startDate": None,
        "endDate": None,
        "rankings": rows,
        "totalPlayers": total_players,
        "missingRanges": missing_payload,
    }


def _normalize_player_name(raw: str) -> str:
    name = str(raw or "").strip()
    return re.sub(r"^\d+\.\s*", "", name).strip() or "(unknown)"


def _normalize_top8(top8: list[dict[str, Any]]) -> list[dict[str, Any]]:
    seen: set[tuple[int, str]] = set()
    out: list[dict[str, Any]] = []
    for row in top8:
        placement = int(row.get("placement") or 0)
        if placement <= 0:
            continue
        name = _normalize_player_name(str(row.get("name") or ""))
        key = (placement, name.casefold())
        if key in seen:
            continue
        seen.add(key)
        out.append({"placement": placement, "name": name})
    out.sort(key=lambda r: int(r["placement"]))
    return out[:8]


def _pacific_day_bounds_unix(target_day: date) -> tuple[int, int]:
    """Inclusive start and exclusive end of *calendar* target_day in America/Los_Angeles, as Unix seconds."""
    start_local = datetime(
        target_day.year,
        target_day.month,
        target_day.day,
        0,
        0,
        0,
        tzinfo=PACIFIC_TZ,
    )
    end_exclusive = start_local + timedelta(days=1)
    return int(start_local.timestamp()), int(end_exclusive.timestamp())


def _today_pacific() -> date:
    return datetime.now(tz=PACIFIC_TZ).date()


def _unix_to_pacific_date_str(unix_ts: int) -> str:
    """Calendar YYYY-MM-DD in Pacific for this instant (UTC epoch seconds from start.gg)."""
    dt = datetime.fromtimestamp(int(unix_ts), tz=timezone.utc).astimezone(PACIFIC_TZ)
    return dt.strftime("%Y-%m-%d")


def _week_bounds_sun_sat(target_day: date) -> tuple[date, date]:
    # Python weekday: Monday=0 ... Sunday=6
    days_since_sunday = (target_day.weekday() + 1) % 7
    sunday = target_day - timedelta(days=days_since_sunday)
    saturday = sunday + timedelta(days=6)
    return sunday, saturday


def _fetch_tournaments_for_window(
    client: Any,
    *,
    start_unix: int,
    end_unix: int,
    regions: list[str],
) -> list[dict[str, Any]]:
    per_page = recent_events_tool._safe_per_page(base=50, approx_objects_per_node=10)
    out_by_id: dict[str, dict[str, Any]] = {}
    for region in regions:
        coords, radius = recent_events_tool.NORCAL_REGIONS[region]
        page = 1
        total_pages = 1
        while page <= total_pages:
            payload = client.gql(
                recent_events_tool.GET_TOURNAMENTS_BY_LOCATION_QUERY,
                {
                    "page": page,
                    "perPage": per_page,
                    "coordinates": coords,
                    "radius": radius,
                    "afterDate": start_unix,
                    "beforeDate": end_unix,
                    "videogameIds": [recent_events_tool.SMASH_ULTIMATE_VIDEOGAME_ID],
                },
            )
            block = payload.get("data", {}).get("tournaments", {}) or {}
            nodes = block.get("nodes", []) or []
            page_info = block.get("pageInfo", {}) or {}
            total_pages = int(page_info.get("totalPages") or 1)
            for tournament in nodes:
                t_id = str(tournament.get("id") or "")
                if not t_id:
                    continue
                t_start = int(tournament.get("startAt") or 0)
                if t_start < start_unix or t_start >= end_unix:
                    continue
                out_by_id[t_id] = tournament
            page += 1
    out = list(out_by_id.values())
    out.sort(key=lambda row: int(row.get("startAt") or 0))
    return out


def _build_calendar_cards(
    *,
    target_start: date,
    target_end: date,
    sample_registrants: int,
) -> list[dict[str, Any]]:
    _ensure_runtime_deps()
    token = os.environ.get("STARTGG_API_KEY", "").strip()
    if not token:
        raise ValueError("STARTGG_API_KEY is required in environment or .env")

    start_unix, _ = _pacific_day_bounds_unix(target_start)
    end_exclusive_unix, _ = _pacific_day_bounds_unix(target_end + timedelta(days=1))
    now_unix = int(datetime.now(tz=timezone.utc).timestamp())
    rng = random.Random(42)
    client = recent_events_tool.StartGGClient(token)
    tournaments = _fetch_tournaments_for_window(
        client,
        start_unix=start_unix,
        end_unix=end_exclusive_unix,
        regions=["bay", "sacramento"],
    )

    cards: list[dict[str, Any]] = []
    for stub in tournaments:
        slug = str(stub.get("slug") or "")
        if not slug:
            continue
        details = recent_events_tool._fetch_tournament_details(client, slug)
        if not details:
            continue
        tname = str(details.get("name") or stub.get("name") or "(unknown tournament)")
        timage = recent_events_tool._pick_tournament_image(details.get("images") or [])
        t_id = str(details.get("id") or stub.get("id") or "")
        t_end = int(details.get("endAt") or stub.get("endAt") or 0)
        concluded = t_end > 0 and t_end <= now_unix
        all_events = details.get("events") or []
        ult_events = [
            ev
            for ev in all_events
            if str((ev.get("videogame") or {}).get("name") or "").strip() == recent_events_tool.GAME_FILTER
        ]
        for ev in ult_events:
            event_id = str(ev.get("id") or "")
            event_name = str(ev.get("name") or "(unknown event)")
            event_slug = str(ev.get("slug") or "")
            event_link = recent_events_tool._event_link_from_slug(event_slug) if event_slug else f"https://start.gg/{slug}"
            event_start_unix = int(details.get("startAt") or stub.get("startAt") or 0)
            event_date = _unix_to_pacific_date_str(event_start_unix) if event_start_unix else ""
            entrant_count = int(ev.get("numEntrants") or 0)

            top8: list[dict[str, Any]] = []
            if concluded and event_id:
                try:
                    top8 = recent_events_tool._fetch_event_top8_standings(client, event_id)
                except Exception:
                    top8 = []
                if not top8:
                    try:
                        top8 = recent_events_tool._fetch_event_top8_from_sets_fallback(client, event_id)
                    except Exception:
                        top8 = []
            top8 = _normalize_top8(top8)

            sampled_names: list[str] = []
            if event_id:
                try:
                    entrants = recent_events_tool._fetch_all_event_entrants(client, event_id)
                    if concluded:
                        top8_cf = {str(row.get("name") or "").casefold() for row in top8}
                        pool = [name for name in entrants if name.casefold() not in top8_cf]
                    else:
                        pool = entrants
                    k = min(max(0, sample_registrants), len(pool))
                    sampled_names = rng.sample(pool, k) if k else []
                except Exception:
                    sampled_names = []

            card_id = f"{t_id}:{event_id or event_slug or event_name}:{event_start_unix}:{len(cards)}"
            cards.append(
                {
                    "id": card_id,
                    "tournamentName": tname,
                    "eventName": event_name,
                    "title": f"{tname} - {event_name}",
                    "date": event_date,
                    "startAt": event_start_unix,
                    "winner": str(top8[0]["name"]) if top8 else "(unavailable)",
                    "hasConcluded": concluded,
                    "top8": top8,
                    "randomRegistrants": sampled_names,
                    "entrantCount": entrant_count,
                    "eventLink": event_link,
                    "iconUrl": timage,
                }
            )
    cards.sort(key=lambda row: int(row.get("startAt") or 0))
    return cards


def _weekday_label(iso_day: str) -> str:
    d = _parse_iso_date(iso_day)
    return d.strftime("%A")


def _pr_maker_event_row_cached(
    conn: sqlite3.Connection,
    *,
    tournament_id: str,
    event_slug: str,
    game_filter: str,
    min_entrants: int,
    after: int,
    before: int,
) -> bool:
    """True if this event row exists in cache and matches homepage ELO eligibility."""
    if not tournament_id or not event_slug:
        return False
    row = conn.execute(
        """
        SELECT 1 FROM tournaments
        WHERE tournament_id = ? AND event_slug = ?
          AND videogame_name = ? AND event_num_entrants >= ?
          AND start_at >= ? AND start_at <= ?
        LIMIT 1
        """,
        (tournament_id, event_slug, game_filter, min_entrants, after, before),
    ).fetchone()
    return row is not None


def _pr_maker_merged_sets_and_elo(
    start_iso: str,
    end_iso: str,
    event_slugs: list[str],
    merge_rules: list[dict[str, str]],
) -> tuple[list[dict[str, Any]], dict[str, float]]:
    """Load in-region sets scoped to event_slugs, apply merges, compute ELO."""
    cfg = EloConfig(
        start_date=start_iso or None,
        end_date=end_iso or None,
        include_event_slugs=set(event_slugs),
    )
    sets = _load_in_region_sets(cfg)
    for rule in merge_rules:
        keep = str(rule.get("keep", ""))
        drop = str(rule.get("drop", ""))
        if not keep or not drop or keep == drop:
            continue
        for s in sets:
            if s["p1"] == drop:
                s["p1"] = keep
            if s["p2"] == drop:
                s["p2"] = keep
        sets = [s for s in sets if s["p1"] != s["p2"]]
    elo = _compute_elo_from_sets(sets, k_factor=cfg.k_factor, initial_elo=cfg.initial_elo)
    return sets, elo


def _empty_report(name: str) -> dict[str, Any]:
    return {
        "canonical_name": name, "in_region_tournaments": 0, "in_region_wins": 0,
        "in_region_losses": 0, "out_region_tournaments": 0, "out_region_wins": 0,
        "out_region_losses": 0, "notable_out_wins": [], "notable_out_losses": [],
        "all_out_wins": [], "all_out_losses": [],
        "in_region_placements": [], "out_region_placements": [],
    }


_CSV_HEADER = [
    "rank", "player", "copeland_score", "elo",
    "in_region_wins", "in_region_losses", "in_region_tournaments",
    "wins", "losses", "total_sets",
    "positive_h2h", "even_h2h", "negative_h2h",
    "tournaments_attended", "loss_to_tournament_ratio",
    "out_region_wins", "out_region_losses", "out_region_tournaments",
    "notable_oor_wins", "notable_oor_losses",
]

# Multi-value cells: use " | " so commas inside player names do not look like CSV column breaks in tools
# that guess delimiters. Python's csv.writer still quotes fields when needed.
_CSV_LIST_SEP = " | "


def _pool_copeland_scores(names: list[str], sets: list[dict[str, Any]]) -> dict[str, float]:
    """Copeland-style score from in-region sets among this name list (not PR Maker comparison clicks)."""
    pool = set(names)
    scores: dict[str, float] = {n: 0.0 for n in names}
    ordered = list(names)
    for i in range(len(ordered)):
        a = ordered[i]
        for j in range(i + 1, len(ordered)):
            b = ordered[j]
            aw, bw = 0, 0
            for s in sets:
                p1, p2 = s.get("p1"), s.get("p2")
                if p1 not in pool or p2 not in pool:
                    continue
                if {p1, p2} != {a, b}:
                    continue
                s1 = int(s.get("p1_score", 0) or 0)
                s2 = int(s.get("p2_score", 0) or 0)
                if s1 > s2:
                    if p1 == a:
                        aw += 1
                    else:
                        bw += 1
                elif s2 > s1:
                    if p2 == a:
                        aw += 1
                    else:
                        bw += 1
            if aw > bw:
                scores[a] += 1.0
            elif bw > aw:
                scores[b] += 1.0
            elif aw > 0 and aw == bw:
                scores[a] += 0.5
                scores[b] += 0.5
    return scores


def _format_notable_oor_cell(items: Any, *, limit: int = 15) -> str:
    """Format notable OOR entries without str(tuple), which injects confusing commas."""
    parts: list[str] = []
    for raw in (items or [])[:limit]:
        if isinstance(raw, (list, tuple)) and len(raw) >= 2:
            label, cnt = raw[0], raw[1]
            label_s = str(label).replace("\n", " ").replace("\r", "").strip()
            try:
                n = int(cnt)
            except (TypeError, ValueError):
                n = 1
            if n > 1:
                parts.append(f"{label_s} ×{n}")
            else:
                parts.append(label_s)
        else:
            parts.append(str(raw).replace("\n", " ").replace("\r", "").strip())
    return _CSV_LIST_SEP.join(parts)


def _csv_row_for_player(
    name: str,
    report: dict[str, Any],
    sets: list[dict[str, Any]],
    elo: dict[str, float],
    cfg: "EloConfig",
    *,
    rank: Any = "",
    copeland: Any = "",
) -> list[Any]:
    rec = _build_player_opponent_records(name, sets, report)
    wins = sum(rec[o]["wins"] for o in rec)
    losses = sum(rec[o]["losses"] for o in rec)
    pos, even, neg = [], [], []
    for opp, r in sorted(rec.items(), key=lambda x: x[1]["wins"] + x[1]["losses"], reverse=True):
        w, l = r["wins"], r["losses"]
        tag = f"{opp} ({w}-{l})"
        if w > l:
            pos.append(tag)
        elif w == l and w + l > 0:
            even.append(tag)
        elif l > w:
            neg.append(tag)
    att = int(report.get("in_region_tournaments", 0)) + int(report.get("out_region_tournaments", 0))
    ltr_raw = _loss_to_tournament_ratio(report)
    ltr: Any = "" if not math.isfinite(ltr_raw) else round(ltr_raw, 4)
    return [
        rank, name, copeland,
        round(elo.get(name, cfg.initial_elo), 2),
        int(report.get("in_region_wins", 0)),
        int(report.get("in_region_losses", 0)),
        int(report.get("in_region_tournaments", 0)),
        wins, losses, wins + losses,
        _CSV_LIST_SEP.join(pos), _CSV_LIST_SEP.join(even), _CSV_LIST_SEP.join(neg),
        att, ltr,
        int(report.get("out_region_wins", 0)),
        int(report.get("out_region_losses", 0)),
        int(report.get("out_region_tournaments", 0)),
        _format_notable_oor_cell(report.get("notable_out_wins")),
        _format_notable_oor_cell(report.get("notable_out_losses")),
    ]


def _dedupe_preserve_order(names: Any) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for n in names:
        if n not in seen:
            seen.add(n)
            out.append(n)
    return out


def _load_reports_for_players(
    names: Any,
    sets: list[dict[str, Any]],
    elo: dict[str, float],
    cfg: EloConfig,
    *,
    ctx_hash: str = "",
    oor_window_hash: str = "",
    force_refresh_oor: bool = False,
    progress_cb: Any = None,
    is_warm: bool = False,
    cancel_check: Any = None,
    stream_event: Any = None,
) -> dict[str, dict[str, Any]]:
    """Load live reports with SQLite cache + parallel fetching for cache misses.

    ``names``             – iterable of player names; order is preserved for priority.
    ``ctx_hash``          – if provided, enables the OOR report cache (read/write).
    ``oor_window_hash``   – date-window partition key for the set-history cache (v2).
    ``force_refresh_oor`` – skip set-history cache reads; overwrite on completion.
    ``progress_cb``       – optional ``(completed, total, current_name) -> None``.
    ``is_warm``           – when True, yields to active pair fetches (lower priority).
    ``cancel_check``      – optional ``() -> bool``; if returns True, stop early.
    ``stream_event``      – optional ``(dict) -> None`` for NDJSON progress (must run on request thread;
                            when set, live fetches run sequentially, not in a thread pool).
    """
    names = _dedupe_preserve_order(names)
    token = os.environ.get("STARTGG_API_KEY", "").strip()
    reports: dict[str, dict[str, Any]] = {}
    total = len(names)
    if not token:
        for i, p in enumerate(names):
            reports[p] = _empty_report(p)
            if progress_cb:
                progress_cb(i + 1, total, p)
        return reports

    cache_conn: sqlite3.Connection | None = None
    if ctx_hash:
        try:
            cache_conn = _oor_cache_conn()
        except Exception:
            cache_conn = None

    need_live: list[str] = []
    for p in names:
        if cache_conn and ctx_hash:
            cached = _cache_get_report(cache_conn, ctx_hash, p)
            if cached is not None:
                reports[p] = cached
                if stream_event:
                    stream_event({
                        "type": "progress",
                        "phase": "cache_hit",
                        "player": p,
                        "message": "OOR report loaded from SQLite cache (full report row)",
                    })
                continue
            state = _oor_fetch_state(cache_conn, ctx_hash, p)
            if state == "complete":
                try:
                    rebuilt = _oor_rebuild_report_from_rows(cache_conn, ctx_hash, p, sets)
                    reports[p] = rebuilt
                    with _oor_cache_lock:
                        _cache_put_report(cache_conn, ctx_hash, p, rebuilt)
                    if stream_event:
                        stream_event({
                            "type": "progress",
                            "phase": "cache_hit_granular",
                            "player": p,
                            "message": "OOR report rebuilt from granular event rows (no live set fetch)",
                        })
                    continue
                except Exception:
                    pass
        need_live.append(p)

    # Priority ordering: active pair names first so they get fetched before others.
    with _OOR_ACTIVE_PAIR_LOCK:
        active = set(_OOR_ACTIVE_PAIR)
    if active:
        priority = [p for p in need_live if p in active]
        rest = [p for p in need_live if p not in active]
        need_live = priority + rest

    if progress_cb:
        progress_cb(len(reports), total, "")

    if not stream_event:
        tag = "[warm] " if is_warm else ""
        if need_live:
            server_debug_log(
                "info", "server/OOR",
                f"{tag}Cache pass done: {len(reports)} ready, {len(need_live)} need Start.gg API",
                "",
            )
        else:
            server_debug_log(
                "info", "server/OOR",
                f"{tag}All {total} player(s) from SQLite cache — no live Start.gg fetch",
                "",
            )

    if not need_live:
        if cache_conn:
            cache_conn.close()
        return reports

    try:
        client = StartGGClient(token)
        pdb = _init_player_db(cfg.player_db_path)
        all_names_for_identity = set(need_live)
        identity = _build_identity_map_live(client, cfg, all_names_for_identity, pdb, verbose=False)
        in_region_ids = _in_region_tournament_ids(cfg.tournament_cache_path)

        # --- Optional M4: tournament catalog for batched player.sets filters ---
        oor_catalog: list[str] | None = None
        if cfg.oor_use_tournament_catalog and oor_window_hash and cfg.start_date and cfg.end_date:
            if cache_conn:
                try:
                    oor_catalog = _oor_get_tournament_catalog(cache_conn, oor_window_hash)
                except Exception:
                    pass
            if oor_catalog is None:
                try:
                    from elo_calculator import _date_to_unix  # type: ignore
                    _after = _date_to_unix(cfg.start_date)
                    _before = _date_to_unix(cfg.end_date)
                    if _after and _before:
                        oor_catalog = fetch_oor_tournament_catalog(
                            client, _after, _before, in_region_ids,
                            max_retries=cfg.max_retries,
                        )
                        if cache_conn and oor_catalog is not None:
                            with _oor_cache_lock:
                                _oor_put_tournament_catalog(cache_conn, oor_window_hash, oor_catalog)
                        server_debug_log(
                            "info", "server/OOR",
                            f"Built tournament catalog: {len(oor_catalog or [])} OOR tournament(s) in window",
                            f"window={oor_window_hash[:8]}",
                        )
                except Exception:
                    oor_catalog = None

        if not stream_event:
            _w = 1 if is_warm else 3
            server_debug_log(
                "info", "server/OOR",
                "Start.gg identity map built; live OOR pipeline running",
                f"{len(need_live)} player(s), workers={_w}, warm={is_warm}",
            )

        if stream_event:
            stream_event({
                "type": "progress",
                "phase": "identity_ready",
                "message": f"Resolved Start.gg identities for {len(need_live)} player(s); starting live OOR pipeline",
                "players": list(need_live),
            })

        def _fetch_one(player: str) -> tuple[str, dict[str, Any]]:
            if cancel_check and cancel_check():
                if stream_event:
                    stream_event({
                        "type": "progress",
                        "phase": "cancelled",
                        "player": player,
                        "message": "OOR fetch cancelled before work started",
                    })
                return player, _empty_report(player)
            ident = identity.get(player)
            pid = str(ident.get("player_id") or "") if ident else ""
            if not ident or not pid:
                if stream_event:
                    stream_event({
                        "type": "progress",
                        "phase": "identity_miss",
                        "player": player,
                        "message": "No Start.gg player id for this name — skipping live OOR scrape",
                    })
                return player, _empty_report(player)
            if stream_event:
                stream_event({
                    "type": "progress",
                    "phase": "player_cache_check",
                    "player": player,
                    "message": f"Checking cache for {player} (player_id={pid})…",
                    "player_id": pid,
                })

            # --- Set history cache (window-keyed v2 when available, fallback to legacy) ---
            preloaded_nodes: list[dict[str, Any]] | None = None
            if cache_conn and not force_refresh_oor:
                try:
                    cached_nodes, fetched_at = _oor_get_player_sets(cache_conn, pid, window_hash=oor_window_hash)
                    if cached_nodes is not None:
                        preloaded_nodes = cached_nodes
                        age_min = round((int(_time.time()) - fetched_at) / 60, 1)
                        _hit_msg = f"Set history CACHE HIT for {player}: {len(cached_nodes)} set node(s) cached (fetched {age_min} min ago)"
                        server_debug_log("info", "server/OOR", _hit_msg, f"window={oor_window_hash[:8]}" if oor_window_hash else "legacy")
                        if stream_event:
                            stream_event({
                                "type": "progress",
                                "phase": "set_history_cache_hit",
                                "player": player,
                                "message": _hit_msg,
                                "nodes": len(cached_nodes),
                                "age_minutes": age_min,
                            })
                    else:
                        _miss_msg = f"Set history CACHE MISS for {player}: will fetch from Start.gg"
                        server_debug_log("info", "server/OOR", _miss_msg, f"window={oor_window_hash[:8]}" if oor_window_hash else "legacy")
                        if stream_event:
                            stream_event({
                                "type": "progress",
                                "phase": "set_history_cache_miss",
                                "player": player,
                                "message": _miss_msg,
                            })
                except Exception:
                    pass
            elif force_refresh_oor:
                _fr_msg = f"Force refresh requested for {player}: bypassing set-history cache"
                server_debug_log("info", "server/OOR", _fr_msg, "")
                if stream_event:
                    stream_event({
                        "type": "progress",
                        "phase": "set_history_force_refresh",
                        "player": player,
                        "message": _fr_msg,
                    })

            # --- Tournament result cache closures (context-independent) ---
            def _tourney_lookup(tournament_id: str) -> dict[str, Any] | None:
                if not cache_conn:
                    return None
                try:
                    return _oor_get_tournament_result(cache_conn, pid, tournament_id)
                except Exception:
                    return None

            def _tourney_store(tournament_id: str, result: dict[str, Any]) -> None:
                if not cache_conn:
                    return
                try:
                    with _oor_cache_lock:
                        _oor_put_tournament_result(cache_conn, pid, tournament_id, result)
                except Exception:
                    pass

            def _phase_cb(phase: str, detail: dict[str, Any]) -> None:
                if stream_event:
                    stream_event({
                        "type": "progress",
                        "phase": phase,
                        "player": player,
                        "detail": detail,
                    })
                if phase == "set_history_fetch_metrics":
                    server_debug_log(
                        "info", "server/OOR",
                        f"Fetch metrics for {player}",
                        detail.get("message", ""),
                    )

            def _page_cb(page: int, total_pages: int, nodes: list[Any]) -> None:
                if stream_event:
                    stream_event({
                        "type": "progress",
                        "phase": "set_history_page",
                        "player": player,
                        "page": page,
                        "totalPages": total_pages,
                        "nodesThisPage": len(nodes),
                        "message": f"Set history page {page}/{total_pages} — {len(nodes)} set(s) on this page",
                    })

            try:
                report = _get_live_player_report(
                    client=client, config=cfg, canonical_name=player,
                    user_id=str(ident.get("user_id") or ""),
                    player_id=pid,
                    in_region_sets=sets, in_region_tournament_ids=in_region_ids,
                    verbose=False,
                    include_raw_player_sets=(preloaded_nodes is None),
                    cancel_check=cancel_check,
                    page_callback=_page_cb if stream_event else None,
                    phase_callback=_phase_cb,
                    preloaded_set_nodes=preloaded_nodes,
                    tournament_cache_lookup=_tourney_lookup,
                    tournament_cache_store=_tourney_store,
                    oor_catalog_tournament_ids=oor_catalog if preloaded_nodes is None else None,
                )
            except CancelledOOR:
                if stream_event:
                    stream_event({
                        "type": "progress",
                        "phase": "cancelled_mid_fetch",
                        "player": player,
                        "message": "OOR fetch stopped between start.gg pages (best-effort cancel)",
                    })
                return player, _empty_report(player)
            # Store raw set nodes in window-keyed cache after a live fetch.
            if preloaded_nodes is None and cache_conn:
                raw_nodes = report.pop("raw_player_set_nodes", None)
                if raw_nodes is not None:
                    try:
                        with _oor_cache_lock:
                            _oor_put_player_sets(cache_conn, pid, raw_nodes, window_hash=oor_window_hash)
                        if stream_event:
                            stream_event({
                                "type": "progress",
                                "phase": "set_history_stored",
                                "player": player,
                                "message": f"Stored {len(raw_nodes)} set node(s) for {player} (player_id={pid}, window={oor_window_hash[:8]}…)",
                                "nodes": len(raw_nodes),
                            })
                    except Exception:
                        pass
            else:
                report.pop("raw_player_set_nodes", None)
            if stream_event:
                stream_event({
                    "type": "progress",
                    "phase": "live_fetch_aggregate_done",
                    "player": player,
                    "message": "Live report assembled; writing cache and player DB",
                })
            if cache_conn and ctx_hash:
                try:
                    with _oor_cache_lock:
                        for pl in report.get("out_region_placements", []):
                            ev_data = {
                                "tournament_id": pl.get("tournament_id", ""),
                                "event_slug": pl.get("event_slug", ""),
                                "event_id": "",
                                "tournament_name": pl.get("tournament_name", ""),
                                "start_at": 0,
                                "wins": pl.get("wins", 0),
                                "losses": pl.get("losses", 0),
                                "notable_wins": [],
                                "notable_losses": [],
                                "placement": pl.get("placement"),
                            }
                            _oor_upsert_event_row(cache_conn, ctx_hash, player, ev_data)
                        _oor_set_fetch_state(
                            cache_conn, ctx_hash, player,
                            pages_fetched=0, total_pages=0, status="complete",
                        )
                except Exception:
                    pass
            return player, report

        def _apply_fetch_result(player: str, report: dict[str, Any]) -> None:
            reports[player] = report
            try:
                _upsert_live_player_report(pdb, report, elo.get(player, cfg.initial_elo))
            except Exception:
                pass
            if cache_conn and ctx_hash:
                try:
                    with _oor_cache_lock:
                        _cache_put_report(cache_conn, ctx_hash, player, report)
                except Exception:
                    pass
            if progress_cb:
                progress_cb(len(reports), total, player)

        # Streaming progress must run on the HTTP thread — no thread pool.
        if stream_event is not None:
            for p in list(need_live):
                if cancel_check and cancel_check():
                    break
                try:
                    player, report = _fetch_one(p)
                except Exception:
                    player = p
                    report = _empty_report(p)
                _apply_fetch_result(player, report)
        else:
            # Warm jobs use fewer workers to avoid starving pair fetches.
            workers = 1 if is_warm else 3
            with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as pool:
                futures = {pool.submit(_fetch_one, p): p for p in need_live}
                for fut in concurrent.futures.as_completed(futures):
                    if cancel_check and cancel_check():
                        break
                    try:
                        player, report = fut.result()
                    except Exception:
                        player = futures[fut]
                        report = _empty_report(player)
                    _apply_fetch_result(player, report)
        pdb.close()
    except Exception:
        for p in names:
            if p not in reports:
                reports[p] = _empty_report(p)
    if cache_conn:
        try:
            cache_conn.close()
        except Exception:
            pass
    return reports


def _load_reports_for_pair(
    p1: str, p2: str, sets: list[dict[str, Any]], elo: dict[str, float], cfg: EloConfig,
    *, ctx_hash: str = "", oor_window_hash: str = "", force_refresh_oor: bool = False,
) -> dict[str, dict[str, Any]]:
    return _load_reports_for_players(
        [p1, p2], sets, elo, cfg, ctx_hash=ctx_hash,
        oor_window_hash=oor_window_hash, force_refresh_oor=force_refresh_oor,
    )


def _oor_warm_worker(
    job_id: str,
    *,
    start: str,
    end: str,
    event_slugs: list[str],
    merge_rules: list[dict[str, str]],
    names: list[str],
) -> None:
    """Background job: preload live OOR reports for all given players into the cache."""
    total = len(names)
    try:
        server_debug_log(
            "info", "server/OOR-warm",
            "Worker started",
            f"job={job_id[:8]}… · {total} player(s)",
        )
        _ensure_runtime_deps()
        sets, elo = _pr_maker_merged_sets_and_elo(start, end, event_slugs, merge_rules)
        cfg = EloConfig(
            start_date=start or None, end_date=end or None,
            include_event_slugs=set(event_slugs),
        )
        ch = _pr_maker_context_hash(start, end, event_slugs, merge_rules)

        def _progress(completed: int, t: int, current_name: str) -> None:
            with JOB_LOCK:
                OOR_WARM_JOBS[job_id]["completed"] = completed
                OOR_WARM_JOBS[job_id]["total"] = t
                OOR_WARM_JOBS[job_id]["currentPlayer"] = current_name
            detail = f"{completed}/{t}"
            if current_name:
                detail += f" · last: {current_name}"
            server_debug_log("info", "server/OOR-warm", "Progress", detail)

        wh = _oor_window_hash(start, end)
        _load_reports_for_players(names, sets, elo, cfg, ctx_hash=ch, oor_window_hash=wh, progress_cb=_progress, is_warm=True)

        with JOB_LOCK:
            OOR_WARM_JOBS[job_id].update(status="done", completed=total)
        server_debug_log(
            "info", "server/OOR-warm",
            "Worker finished",
            f"job={job_id[:8]}… · {total} player(s) cached",
        )
    except Exception as exc:
        with JOB_LOCK:
            OOR_WARM_JOBS[job_id].update(status="error", error=str(exc))
        server_debug_log("error", "server/OOR-warm", "Worker failed", f"{type(exc).__name__}: {exc}")


def _pr_maker_scrape_worker(job_id: str, *, start: str, end: str, fresh: bool) -> None:
    """Stage-1 only: list ELO-eligible events in the date range; [CACHED] vs [NOT CACHED].

    Same filter as ``tournament_scraper.scrape_tournaments`` / ``full.py`` pipeline:
    ``game_filter`` (Ultimate) and ``min_entrants`` (16 by default). NorCal regions
    come from ``ScraperConfig`` (bay + sacramento) via ``_fetch_all_tournaments``.
    """
    try:
        _ensure_runtime_deps()

        min_entrants = 16  # matches demo/full.py default and homepage scrape config

        cache_path = _ts_mod._default_cache_path()
        _ts_mod._ensure_cache_dir(cache_path)
        conn = sqlite3.connect(str(cache_path))
        _ts_mod._init_cache(conn)

        after = _ts_mod._date_to_unix(start)
        before = _ts_mod._date_to_unix(end)
        if after is None or before is None:
            raise ValueError("Invalid start or end date")

        log_lines: list[str] = [
            f"[FILTER] Listing only events that feed homepage ELO: "
            f"{GAME_FILTER!r}, min {min_entrants}+ entrants (NorCal bay + sacramento).",
        ]

        if fresh:
            with JOB_LOCK:
                PR_MAKER_SCRAPE_JOBS[job_id]["phase"] = "clearing_cache"
            deleted = conn.execute(
                """
                DELETE FROM tournaments
                WHERE start_at >= ? AND start_at <= ?
                  AND videogame_name = ? AND event_num_entrants >= ?
                """,
                (after, before, GAME_FILTER, min_entrants),
            ).rowcount
            conn.commit()
            log_lines.append(
                f"[FRESH] Cleared {deleted} ELO-eligible row(s) in {start} .. {end} "
                f"({GAME_FILTER}, >={min_entrants} entrants).",
            )
            with JOB_LOCK:
                PR_MAKER_SCRAPE_JOBS[job_id]["log"] = list(log_lines)

        with JOB_LOCK:
            PR_MAKER_SCRAPE_JOBS[job_id]["phase"] = "fetching"

        token = os.environ.get("STARTGG_API_KEY", "").strip()
        if not token:
            raise ValueError("STARTGG_API_KEY required in environment or .env")

        client = _ts_mod.requests.Session()
        limiter = _ts_mod.RateLimiter()

        config = ScraperConfig(
            start_date=start,
            end_date=end,
            game_filter=GAME_FILTER,
            min_entrants=min_entrants,
            regions=["bay", "sacramento"],
        )

        to_insert: list[tuple] = []
        cached_count = 0
        new_count = 0
        seen_tournament_ids: set[str] = set()

        for t in _ts_mod._fetch_all_tournaments(client, limiter, config, token, verbose=False):
            tid = str(t.get("id", ""))
            if not tid or tid in seen_tournament_ids:
                continue
            seen_tournament_ids.add(tid)

            pairs = _ts_mod._flatten_and_filter([t], GAME_FILTER, min_entrants)
            if not pairs:
                continue

            t_name = str(t.get("name") or "?")
            for pair in pairs:
                ev = pair.get("event") or {}
                ev_name = str(ev.get("name") or "Event")
                slug = str(ev.get("slug") or "")
                label = f"{t_name} — {ev_name}"

                if _pr_maker_event_row_cached(
                    conn,
                    tournament_id=tid,
                    event_slug=slug,
                    game_filter=GAME_FILTER,
                    min_entrants=min_entrants,
                    after=after,
                    before=before,
                ):
                    cached_count += 1
                    log_lines.append(f"[CACHED] {label}")
                else:
                    new_count += 1
                    log_lines.append(f"[NOT CACHED] {label}")
                    row = _ts_mod._tournament_to_row(t, ev)
                    to_insert.append(row)

            with JOB_LOCK:
                PR_MAKER_SCRAPE_JOBS[job_id]["log"] = list(log_lines)
                PR_MAKER_SCRAPE_JOBS[job_id]["tournamentsCached"] = cached_count
                PR_MAKER_SCRAPE_JOBS[job_id]["tournamentsNew"] = new_count
                PR_MAKER_SCRAPE_JOBS[job_id]["tournamentsTotal"] = cached_count + new_count

        with JOB_LOCK:
            PR_MAKER_SCRAPE_JOBS[job_id]["phase"] = "saving"

        if to_insert:
            conn.executemany(
                "INSERT OR REPLACE INTO tournaments "
                "(tournament_id, event_slug, name, city, slug, start_at, event_num_entrants, videogame_name, raw_json, cached_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                to_insert,
            )
            conn.commit()
            log_lines.append(f"[DB] Inserted {len(to_insert)} new row(s) (ELO-eligible events only).")
        else:
            log_lines.append("[DB] No new rows to insert (all qualifying events already cached).")

        _ts_mod.record_verified_empty_weeks_for_scrape_window(
            conn,
            range_start=start,
            range_end=end,
            game_filter=GAME_FILTER,
        )
        conn.commit()
        conn.close()

        total = cached_count + new_count
        log_lines.append(
            f"[DONE] {cached_count} cached, {new_count} new — {total} ELO-eligible event(s) "
            f"({GAME_FILTER}, >={min_entrants} entrants).",
        )

        with JOB_LOCK:
            PR_MAKER_SCRAPE_JOBS[job_id]["status"] = "done"
            PR_MAKER_SCRAPE_JOBS[job_id]["phase"] = "done"
            PR_MAKER_SCRAPE_JOBS[job_id]["log"] = list(log_lines)
    except Exception as exc:
        with JOB_LOCK:
            PR_MAKER_SCRAPE_JOBS[job_id]["status"] = "error"
            PR_MAKER_SCRAPE_JOBS[job_id]["error"] = str(exc)
            PR_MAKER_SCRAPE_JOBS[job_id]["phase"] = "error"


def _list_cached_events_for_range(start_iso: str, end_iso: str) -> list[dict[str, Any]]:
    """Return compact event cards from tournament_cache.db for the given date range.

    Only ELO-eligible rows (GAME_FILTER + MIN_ENTRANTS).  No extra API calls.
    """
    _ensure_runtime_deps()
    cache_path = _ts_mod._default_cache_path()
    if not cache_path.exists():
        return []
    conn = sqlite3.connect(str(cache_path))
    after = _ts_mod._date_to_unix(start_iso)
    before = _ts_mod._date_to_unix(end_iso)
    if after is None or before is None:
        conn.close()
        return []
    rows = conn.execute(
        """
        SELECT tournament_id, event_slug, name, slug, start_at,
               event_num_entrants, raw_json
        FROM tournaments
        WHERE start_at >= ? AND start_at <= ?
          AND videogame_name = ? AND event_num_entrants >= ?
        ORDER BY start_at
        """,
        (after, before, GAME_FILTER, MIN_ENTRANTS),
    ).fetchall()
    conn.close()

    proc_path = PROJECT_ROOT / "data" / "processed_tournament.db"
    processed_slugs: set[str] = set()
    if proc_path.exists():
        pconn = sqlite3.connect(str(proc_path))
        try:
            processed_slugs = {r[0] for r in pconn.execute("SELECT event_slug FROM processed_events").fetchall()}
        except Exception:
            pass
        pconn.close()

    events: list[dict[str, Any]] = []
    seen_slugs: set[str] = set()
    for row in rows:
        event_slug = row[1]
        if not event_slug or event_slug in seen_slugs:
            continue
        seen_slugs.add(event_slug)
        raw = json.loads(row[6]) if row[6] else {}
        t = raw.get("tournament", {})
        ev = raw.get("event", {})
        start_unix = row[4] or 0
        events.append({
            "eventSlug": event_slug,
            "tournamentId": row[0],
            "tournamentName": str(t.get("name") or row[2] or "(unknown)"),
            "eventName": str(ev.get("name") or "(unknown event)"),
            "title": f"{t.get('name') or row[2] or '?'} — {ev.get('name') or '?'}",
            "date": _unix_to_pacific_date_str(start_unix) if start_unix else "",
            "startAt": start_unix,
            "entrantCount": row[5] or 0,
            "eventLink": f"https://start.gg/{event_slug.lstrip('/')}" if event_slug else "",
            "isProcessed": event_slug in processed_slugs,
        })
    return events


def _pr_maker_process_worker(job_id: str, *, event_slugs: list[str]) -> None:
    """Process selected events: fetch sets per event and populate processed_tournament.db."""
    try:
        _ensure_runtime_deps()
        token = os.environ.get("STARTGG_API_KEY", "").strip()
        if not token:
            raise ValueError("STARTGG_API_KEY required in environment or .env")

        proc_path = PROJECT_ROOT / "data" / "processed_tournament.db"
        proc_path.parent.mkdir(parents=True, exist_ok=True)
        pconn = sqlite3.connect(str(proc_path))
        tp._init_processed_db(pconn)

        processed_event_slugs = {
            r[0] for r in pconn.execute("SELECT event_slug FROM processed_events").fetchall()
        }
        sets_cache: dict[str, dict[str, Any]] = {
            row[0]: {"p1_name": row[1], "p2_name": row[2], "p1_score": row[3], "p2_score": row[4]}
            for row in pconn.execute(
                "SELECT set_id, p1_name, p2_name, p1_score, p2_score FROM sets_cache"
            ).fetchall()
        }

        cache_path = _ts_mod._default_cache_path()
        tconn = sqlite3.connect(str(cache_path))

        total = len(event_slugs)
        client = tp.requests.Session()
        limiter = tp.RateLimiter()
        all_sets: list[dict[str, Any]] = []

        for i, event_slug in enumerate(event_slugs):
            row = tconn.execute(
                "SELECT name, tournament_id FROM tournaments WHERE event_slug = ? LIMIT 1",
                (event_slug,),
            ).fetchone()
            event_display = row[0] if row else event_slug
            tournament_id = row[1] if row else ""

            pct = round((i / total) * 100, 2) if total else 0
            with JOB_LOCK:
                PR_MAKER_PROCESS_JOBS[job_id].update({
                    "currentEvent": i + 1,
                    "totalEvents": total,
                    "currentEventName": event_display,
                    "progressPct": pct,
                    "phase": "processing",
                })

            if event_slug in processed_event_slugs:
                cur = pconn.execute(
                    "SELECT set_id, p1_name, p2_name, p1_score, p2_score FROM sets_cache WHERE event_slug = ?",
                    (event_slug,),
                )
                for srow in cur.fetchall():
                    if srow[3] is not None and srow[4] is not None:
                        all_sets.append({srow[1]: srow[3], srow[2]: srow[4]})
                continue

            try:
                event_id = tp._get_event_id(client, limiter, event_slug, token)
            except Exception:
                continue
            if not event_id:
                continue

            try:
                set_ids = tp._get_set_ids_for_event(client, limiter, event_id, token)
            except Exception:
                continue

            with JOB_LOCK:
                PR_MAKER_PROCESS_JOBS[job_id]["currentEventSets"] = len(set_ids)
                PR_MAKER_PROCESS_JOBS[job_id]["currentEventSetsProcessed"] = 0

            event_sets_to_insert: list[tuple[Any, ...]] = []
            sets_done = 0
            for set_id in set_ids:
                if set_id in sets_cache:
                    rec = sets_cache[set_id]
                    all_sets.append({rec["p1_name"]: rec["p1_score"], rec["p2_name"]: rec["p2_score"]})
                elif set_id.startswith("preview_"):
                    pass
                else:
                    try:
                        result = tp._get_players_and_score(client, limiter, set_id, token)
                    except Exception:
                        continue
                    if result and len(result) == 2:
                        s1, s2 = list(result.values())
                        if s1 is not None and s2 is not None:
                            all_sets.append(result)
                            p1, p2 = list(result.keys())
                            event_sets_to_insert.append(
                                (set_id, event_id, event_slug, p1, p2, s1, s2, int(_time.time()))
                            )
                sets_done += 1
                if sets_done % 5 == 0 or sets_done == len(set_ids):
                    with JOB_LOCK:
                        PR_MAKER_PROCESS_JOBS[job_id]["currentEventSetsProcessed"] = sets_done

            if set_ids:
                pconn.execute(
                    "INSERT OR REPLACE INTO processed_events "
                    "(event_slug, event_id, tournament_id, event_name, processed_at) "
                    "VALUES (?, ?, ?, ?, ?)",
                    (event_slug, event_id, tournament_id, event_display, int(_time.time())),
                )
                if event_sets_to_insert:
                    pconn.executemany(
                        "INSERT OR REPLACE INTO sets_cache "
                        "(set_id, event_id, event_slug, p1_name, p2_name, p1_score, p2_score, cached_at) "
                        "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                        event_sets_to_insert,
                    )
                pconn.commit()
                processed_event_slugs.add(event_slug)
                for rec in event_sets_to_insert:
                    sets_cache[rec[0]] = {
                        "p1_name": rec[3], "p2_name": rec[4],
                        "p1_score": rec[5], "p2_score": rec[6],
                    }

        tconn.close()

        mapped_sets = tp._apply_name_mappings(all_sets, tp.DEFAULT_NAME_MAPPINGS)
        pconn.execute("DELETE FROM processed_sets")
        for idx, s in enumerate(mapped_sets):
            names = list(s.keys())
            scores = list(s.values())
            if len(names) == 2 and len(scores) == 2:
                pconn.execute(
                    "INSERT INTO processed_sets "
                    "(set_id, event_slug, p1_canonical, p2_canonical, p1_score, p2_score, created_at) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?)",
                    (f"proc_{idx}", "", names[0], names[1], scores[0], scores[1], int(_time.time())),
                )
        pconn.commit()
        pconn.close()

        with JOB_LOCK:
            PR_MAKER_PROCESS_JOBS[job_id].update({
                "status": "done",
                "phase": "done",
                "progressPct": 100.0,
                "currentEvent": total,
                "totalSetsProcessed": len(mapped_sets),
            })
    except Exception as exc:
        with JOB_LOCK:
            PR_MAKER_PROCESS_JOBS[job_id]["status"] = "error"
            PR_MAKER_PROCESS_JOBS[job_id]["error"] = str(exc)
            PR_MAKER_PROCESS_JOBS[job_id]["phase"] = "error"


def _progressive_recent_events_worker(job_id: str, *, days: int, limit: int, sample_registrants: int) -> None:
    try:
        _ensure_runtime_deps()
        token = os.environ.get("STARTGG_API_KEY", "").strip()
        if not token:
            raise ValueError("STARTGG_API_KEY is required in environment or .env")

        config = recent_events_tool.RecentEventsConfig(
            limit_tournaments=max(10, limit * 3),
            sample_registrants=max(1, sample_registrants),
            regions=["bay", "sacramento"],
            seed=42,
            verbose=False,
            fetch_window_days=max(30, days),
        )
        client = recent_events_tool.StartGGClient(token)
        rng = random.Random(42)
        now_unix = int(datetime.now(tz=timezone.utc).timestamp())
        cutoff_unix = now_unix - (days * 24 * 60 * 60)

        stubs = recent_events_tool._fetch_recent_tournament_stubs(client, config)
        with JOB_LOCK:
            RECENT_EVENT_JOBS[job_id]["totalExpected"] = limit

        cards: list[dict[str, Any]] = []
        for stub in stubs:
            if len(cards) >= limit:
                break

            t_start = int(stub.get("startAt") or 0)
            if t_start < cutoff_unix:
                continue

            slug = str(stub.get("slug") or "")
            if not slug:
                continue
            details = recent_events_tool._fetch_tournament_details(client, slug)
            if not details:
                continue

            tname = str(details.get("name") or stub.get("name") or "(unknown tournament)")
            tdate = recent_events_tool._fmt_date(
                details.get("startAt") if details.get("startAt") is not None else stub.get("startAt")
            )
            timage = recent_events_tool._pick_tournament_image(details.get("images") or [])
            all_events = details.get("events") or []
            events = [
                ev
                for ev in all_events
                if str((ev.get("videogame") or {}).get("name") or "").strip() == recent_events_tool.GAME_FILTER
            ]

            for ev in events:
                if len(cards) >= limit:
                    break

                event_id = str(ev.get("id") or "")
                event_name = str(ev.get("name") or "(unknown event)")
                event_slug = str(ev.get("slug") or "")
                event_link = recent_events_tool._event_link_from_slug(event_slug) if event_slug else f"https://start.gg/{slug}"
                num_entrants = int(ev.get("numEntrants") or 0)

                top8: list[dict[str, Any]] = []
                if event_id:
                    try:
                        top8 = recent_events_tool._fetch_event_top8_standings(client, event_id)
                    except Exception:
                        top8 = []
                    if not top8:
                        try:
                            top8 = recent_events_tool._fetch_event_top8_from_sets_fallback(client, event_id)
                        except Exception:
                            top8 = []
                top8 = _normalize_top8(top8)

                registrants: list[str] = []
                if event_id:
                    try:
                        entrants = recent_events_tool._fetch_all_event_entrants(client, event_id)
                        top8_cf = {str(row.get("name") or "").casefold() for row in top8}
                        non_top8 = [name for name in entrants if name.casefold() not in top8_cf]
                        k = min(sample_registrants, len(non_top8))
                        registrants = rng.sample(non_top8, k) if k else []
                    except Exception:
                        registrants = []

                winner = str(top8[0]["name"]) if top8 else "(unavailable)"
                card_id = f"{str(details.get('id') or stub.get('id') or '')}:{event_id or event_slug or event_name}:{len(cards)}"
                cards.append(
                    {
                        "id": card_id,
                        "tournamentName": tname,
                        "eventName": event_name,
                        "title": f"{tname} - {event_name}",
                        "date": tdate,
                        "winner": winner,
                        "top8": top8,
                        "randomRegistrants": registrants,
                        "entrantCount": num_entrants,
                        "eventLink": event_link,
                        "iconUrl": timage,
                    }
                )

                with JOB_LOCK:
                    RECENT_EVENT_JOBS[job_id]["events"] = list(cards)
                    RECENT_EVENT_JOBS[job_id]["completed"] = len(cards)
                    RECENT_EVENT_JOBS[job_id]["remaining"] = max(limit - len(cards), 0)

        with JOB_LOCK:
            RECENT_EVENT_JOBS[job_id]["status"] = "done"
            RECENT_EVENT_JOBS[job_id]["remaining"] = max(limit - len(cards), 0)
    except Exception as exc:
        with JOB_LOCK:
            RECENT_EVENT_JOBS[job_id]["status"] = "error"
            RECENT_EVENT_JOBS[job_id]["error"] = str(exc)


def _process_tournaments_with_progress(
    config: ProcessorConfig,
    *,
    progress_cb: callable | None,
) -> tuple[list[dict[str, Any]], int]:
    token = os.environ.get("STARTGG_API_KEY")
    if not token:
        raise ValueError("STARTGG_API_KEY must be set in environment or .env")
    if not config.tournament_cache_path.exists():
        raise FileNotFoundError(f"Tournament cache not found: {config.tournament_cache_path}")

    tconn = sqlite3.connect(str(config.tournament_cache_path))
    events_to_process = tp._load_events_from_tournament_cache(tconn, config)
    tconn.close()

    total_events = len(events_to_process)
    if progress_cb is not None:
        progress_cb(0, total_events)

    pconn = sqlite3.connect(str(config.processed_cache_path))
    tp._init_processed_db(pconn)
    processed_event_slugs = {row[0] for row in pconn.execute("SELECT event_slug FROM processed_events").fetchall()}
    sets_cache = {
        row[0]: {"p1_name": row[1], "p2_name": row[2], "p1_score": row[3], "p2_score": row[4]}
        for row in pconn.execute("SELECT set_id, p1_name, p2_name, p1_score, p2_score FROM sets_cache").fetchall()
    }

    client = tp.requests.Session()
    limiter = tp.RateLimiter()
    all_sets: list[dict[str, Any]] = []
    done = 0

    for event_slug, tournament_id, event_name in events_to_process:
        if event_slug in processed_event_slugs:
            cur = pconn.execute(
                "SELECT set_id, p1_name, p2_name, p1_score, p2_score FROM sets_cache WHERE event_slug = ?",
                (event_slug,),
            )
            for row in cur.fetchall():
                p1_score, p2_score = row[3], row[4]
                if p1_score is not None and p2_score is not None:
                    all_sets.append({row[1]: p1_score, row[2]: p2_score})
            done += 1
            if progress_cb is not None:
                progress_cb(done, total_events)
            continue

        event_id = tp._get_event_id(client, limiter, event_slug, token)
        if not event_id:
            done += 1
            if progress_cb is not None:
                progress_cb(done, total_events)
            continue

        set_ids = tp._get_set_ids_for_event(client, limiter, event_id, token)
        event_sets_to_insert: list[tuple[Any, ...]] = []
        for set_id in set_ids:
            if set_id in sets_cache:
                rec = sets_cache[set_id]
                all_sets.append({rec["p1_name"]: rec["p1_score"], rec["p2_name"]: rec["p2_score"]})
                continue
            if set_id.startswith("preview_"):
                continue
            result = tp._get_players_and_score(client, limiter, set_id, token)
            if result and len(result) == 2:
                s1, s2 = list(result.values())
                if s1 is not None and s2 is not None:
                    all_sets.append(result)
                    p1, p2 = list(result.keys())
                    event_sets_to_insert.append((set_id, event_id, event_slug, p1, p2, s1, s2, int(tp.time.time())))

        if set_ids:
            pconn.execute(
                "INSERT OR REPLACE INTO processed_events (event_slug, event_id, tournament_id, event_name, processed_at) VALUES (?, ?, ?, ?, ?)",
                (event_slug, event_id, tournament_id, event_name, int(tp.time.time())),
            )
            if event_sets_to_insert:
                pconn.executemany(
                    "INSERT OR REPLACE INTO sets_cache (set_id, event_id, event_slug, p1_name, p2_name, p1_score, p2_score, cached_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                    event_sets_to_insert,
                )
            pconn.commit()
            processed_event_slugs.add(event_slug)
            for rec in event_sets_to_insert:
                sets_cache[rec[0]] = {"p1_name": rec[3], "p2_name": rec[4], "p1_score": rec[5], "p2_score": rec[6]}

        done += 1
        if progress_cb is not None:
            progress_cb(done, total_events)

    mapped_sets = tp._apply_name_mappings(all_sets, config.name_mappings)
    pconn.execute("DELETE FROM processed_sets")
    for i, s in enumerate(mapped_sets):
        names = list(s.keys())
        scores = list(s.values())
        if len(names) == 2 and len(scores) == 2:
            pconn.execute(
                "INSERT INTO processed_sets (set_id, event_slug, p1_canonical, p2_canonical, p1_score, p2_score, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
                (f"proc_{i}", "", names[0], names[1], scores[0], scores[1], int(tp.time.time())),
            )
    pconn.commit()
    pconn.close()
    return mapped_sets, total_events


def _date_range_worker(job_id: str, *, start: str, end: str) -> None:
    try:
        _ensure_runtime_deps()
        start_d = _parse_iso_date(start)
        end_d = _parse_iso_date(end)
        if end_d < start_d:
            raise ValueError("end must be on or after start")

        with JOB_LOCK:
            DATE_RANGE_JOBS[job_id]["phase"] = "scraping"

        scrape_tournaments(
            ScraperConfig(
                start_date=_date_to_str(start_d),
                end_date=_date_to_str(end_d),
                game_filter=GAME_FILTER,
                min_entrants=16,
                regions=["bay", "sacramento"],
            ),
            verbose=False,
        )

        with JOB_LOCK:
            DATE_RANGE_JOBS[job_id]["phase"] = "processing"

        def _on_progress(done: int, total: int) -> None:
            pct = 100.0 if total <= 0 else min(100.0, (done / total) * 100.0)
            with JOB_LOCK:
                DATE_RANGE_JOBS[job_id]["processedEvents"] = done
                DATE_RANGE_JOBS[job_id]["totalEvents"] = total
                DATE_RANGE_JOBS[job_id]["progressPct"] = round(pct, 2)

        _process_tournaments_with_progress(
            ProcessorConfig(
                start_date=_date_to_str(start_d),
                end_date=_date_to_str(end_d),
                game_filter=GAME_FILTER,
                min_entrants=16,
            ),
            progress_cb=_on_progress,
        )

        with JOB_LOCK:
            DATE_RANGE_JOBS[job_id]["phase"] = "computing"

        elo, _ = compute_elo(EloConfig(start_date=_date_to_str(start_d), end_date=_date_to_str(end_d)))
        rows, total_players = _filter_and_rank(elo, query="", max_players=5000)
        payload = {
            "mode": "date-range",
            "startDate": _date_to_str(start_d),
            "endDate": _date_to_str(end_d),
            "rankings": rows,
            "totalPlayers": total_players,
            "missingRanges": [],
        }

        with JOB_LOCK:
            DATE_RANGE_JOBS[job_id]["status"] = "done"
            DATE_RANGE_JOBS[job_id]["phase"] = "done"
            DATE_RANGE_JOBS[job_id]["progressPct"] = 100.0
            DATE_RANGE_JOBS[job_id]["result"] = payload
    except Exception as exc:
        with JOB_LOCK:
            DATE_RANGE_JOBS[job_id]["status"] = "error"
            DATE_RANGE_JOBS[job_id]["error"] = str(exc)


def _sanitize_for_json(obj: Any) -> Any:
    """Replace NaN/Inf floats with None so output is RFC 8259 JSON (browser JSON.parse safe)."""
    if isinstance(obj, float):
        return obj if math.isfinite(obj) else None
    if isinstance(obj, dict):
        return {k: _sanitize_for_json(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_sanitize_for_json(x) for x in obj]
    if isinstance(obj, tuple):
        return [_sanitize_for_json(x) for x in obj]
    return obj


class ApiHandler(BaseHTTPRequestHandler):
    server_version = "NorCalSmashAPI/0.1"

    def _write_json(self, status: int, payload: dict[str, Any]) -> None:
        """Write JSON; ignore client disconnect (BrokenPipe) so the server thread does not crash."""
        try:
            body = json.dumps(_sanitize_for_json(payload)).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Access-Control-Allow-Origin", "*")
            self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
            self.send_header("Access-Control-Allow-Headers", "Content-Type")
            self.end_headers()
            self.wfile.write(body)
        except OSError as exc:
            if _is_client_disconnect(exc):
                return
            raise

    def _ndjson_stream(self, body_fn: Any) -> None:
        """Chunked NDJSON response. ``body_fn(emit)`` calls ``emit(dict)`` per line (request thread only)."""
        try:
            self.send_response(200)
            self.send_header("Content-Type", "application/x-ndjson; charset=utf-8")
            self.send_header("Transfer-Encoding", "chunked")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
            self.send_header("Access-Control-Allow-Headers", "Content-Type")
            self.end_headers()

            def emit(obj: dict[str, Any]) -> None:
                raw = (json.dumps(_sanitize_for_json(obj)) + "\n").encode("utf-8")
                self.wfile.write(f"{len(raw):x}\r\n".encode("ascii"))
                self.wfile.write(raw)
                self.wfile.write(b"\r\n")
                self.wfile.flush()

            try:
                body_fn(emit)
            finally:
                try:
                    self.wfile.write(b"0\r\n\r\n")
                    self.wfile.flush()
                except OSError:
                    pass
        except OSError as exc:
            if _is_client_disconnect(exc):
                return
            raise

    def do_OPTIONS(self) -> None:  # noqa: N802
        self._write_json(200, {"ok": True})

    def do_POST(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        try:
            if parsed.path == "/api/coverage/resolve/start":
                length = int(self.headers.get("Content-Length", "0") or "0")
                raw = self.rfile.read(length) if length > 0 else b"{}"
                try:
                    body = json.loads(raw.decode("utf-8") or "{}")
                except json.JSONDecodeError as exc:
                    self._write_json(400, {"error": f"Invalid JSON: {exc}"})
                    return
                ranges_in = body.get("ranges")
                if ranges_in is not None and not isinstance(ranges_in, list):
                    self._write_json(400, {"error": "ranges must be a list"})
                    return
                job_id = str(uuid.uuid4())
                with JOB_LOCK:
                    COVERAGE_RESOLVE_JOBS[job_id] = {
                        "id": job_id,
                        "status": "running",
                        "phase": "queued",
                        "totalWindows": 0,
                        "currentWindow": 0,
                        "detail": "",
                        "remainingMissing": None,
                        "remainingCount": None,
                        "error": None,
                    }
                payload_list = list(ranges_in) if isinstance(ranges_in, list) else []
                Thread(
                    target=_coverage_resolve_worker,
                    args=(job_id, payload_list),
                    daemon=True,
                ).start()
                self._write_json(200, {"jobId": job_id})
                return

            if parsed.path == "/api/pr-maker/candidates":
                _ensure_runtime_deps()
                length = int(self.headers.get("Content-Length", "0") or "0")
                raw = self.rfile.read(length) if length > 0 else b"{}"
                try:
                    body = json.loads(raw.decode("utf-8") or "{}")
                except json.JSONDecodeError as jexc:
                    self._write_json(400, {"error": f"Invalid JSON: {jexc}"})
                    return
                start_iso = str(body.get("start", ""))
                end_iso = str(body.get("end", ""))
                event_slugs = list(body.get("eventSlugs", []))
                merge_rules = list(body.get("mergeRules", []))
                if not event_slugs:
                    self._write_json(400, {"error": "eventSlugs list is required"})
                    return
                sets, elo = _pr_maker_merged_sets_and_elo(start_iso, end_iso, event_slugs, merge_rules)
                attendance = _dq_filtered_attendance_counts(sets)
                players = []
                for name in elo:
                    players.append({
                        "name": name,
                        "elo": round(elo[name], 2),
                        "attendance": attendance.get(name, 0),
                    })
                self._write_json(200, {"players": players})
                return

            if parsed.path == "/api/pr-maker/oor-warm/start":
                _ensure_runtime_deps()
                length = int(self.headers.get("Content-Length", "0") or "0")
                raw = self.rfile.read(length) if length > 0 else b"{}"
                try:
                    body = json.loads(raw.decode("utf-8") or "{}")
                except json.JSONDecodeError as jexc:
                    self._write_json(400, {"error": f"Invalid JSON: {jexc}"})
                    return
                start_iso = str(body.get("start", ""))
                end_iso = str(body.get("end", ""))
                event_slugs = list(body.get("eventSlugs", []))
                merge_rules = list(body.get("mergeRules", []))
                warm_names = list(body.get("names", []))
                if not event_slugs or not warm_names:
                    self._write_json(400, {"error": "eventSlugs and names are required"})
                    return
                job_id = str(uuid.uuid4())
                with JOB_LOCK:
                    OOR_WARM_JOBS[job_id] = {
                        "id": job_id,
                        "status": "running",
                        "completed": 0,
                        "total": len(warm_names),
                        "currentPlayer": "",
                        "error": None,
                    }
                server_debug_log(
                    "info", "server/OOR-warm",
                    f"Warm job queued jobId={job_id[:8]}…",
                    f"{len(warm_names)} player(s) (Start.gg + cache on server)",
                )
                Thread(
                    target=_oor_warm_worker,
                    args=(job_id,),
                    kwargs={
                        "start": start_iso, "end": end_iso,
                        "event_slugs": event_slugs, "merge_rules": merge_rules,
                        "names": warm_names,
                    },
                    daemon=True,
                ).start()
                self._write_json(200, {"jobId": job_id})
                return

            if parsed.path == "/api/pr-maker/player-oor":
                _ensure_runtime_deps()
                length = int(self.headers.get("Content-Length", "0") or "0")
                raw = self.rfile.read(length) if length > 0 else b"{}"
                try:
                    body = json.loads(raw.decode("utf-8") or "{}")
                except json.JSONDecodeError as jexc:
                    self._write_json(400, {"error": f"Invalid JSON: {jexc}"})
                    return
                start_iso = str(body.get("start", ""))
                end_iso = str(body.get("end", ""))
                event_slugs = list(body.get("eventSlugs", []))
                merge_rules = list(body.get("mergeRules", []))
                player = str(body.get("player", ""))
                cancel_id = str(body.get("cancelId", ""))
                if not event_slugs or not player:
                    self._write_json(400, {"error": "eventSlugs and player are required"})
                    return
                if cancel_id:
                    with _OOR_CANCEL_LOCK:
                        _OOR_CANCEL_REGISTRY[cancel_id] = False
                def _cancel_ck() -> bool:
                    if not cancel_id:
                        return False
                    with _OOR_CANCEL_LOCK:
                        return _OOR_CANCEL_REGISTRY.get(cancel_id, False)
                sets, elo = _pr_maker_merged_sets_and_elo(start_iso, end_iso, event_slugs, merge_rules)
                cfg = EloConfig(
                    start_date=start_iso or None, end_date=end_iso or None,
                    include_event_slugs=set(event_slugs),
                )
                ch = _pr_maker_context_hash(start_iso, end_iso, event_slugs, merge_rules)
                wh = _oor_window_hash(start_iso, end_iso)
                force_oor = bool(body.get("forceRefreshOOR", False))
                use_stream = bool(body.get("stream"))

                if use_stream:

                    def _run_player_oor_stream(emit: Any) -> None:
                        emit({
                            "type": "progress",
                            "phase": "stream_open",
                            "player": player,
                            "message": "NDJSON stream connected — progress events follow until type=done or error",
                        })
                        with _OOR_ACTIVE_PAIR_LOCK:
                            _OOR_ACTIVE_PAIR.add(player)
                        try:
                            try:
                                _load_reports_for_players(
                                    [player], sets, elo, cfg, ctx_hash=ch,
                                    oor_window_hash=wh, force_refresh_oor=force_oor,
                                    cancel_check=_cancel_ck, stream_event=emit,
                                )
                                emit({
                                    "type": "done",
                                    "ok": True,
                                    "player": player,
                                    "message": "OOR load finished for this player (cache updated)",
                                })
                            except Exception as exc:
                                emit({"type": "error", "player": player, "message": str(exc)})
                        finally:
                            with _OOR_ACTIVE_PAIR_LOCK:
                                _OOR_ACTIVE_PAIR.discard(player)
                            if cancel_id:
                                with _OOR_CANCEL_LOCK:
                                    _OOR_CANCEL_REGISTRY.pop(cancel_id, None)

                    self._ndjson_stream(_run_player_oor_stream)
                    return

                with _OOR_ACTIVE_PAIR_LOCK:
                    _OOR_ACTIVE_PAIR.add(player)
                try:
                    _load_reports_for_players(
                        [player], sets, elo, cfg, ctx_hash=ch,
                        oor_window_hash=wh, force_refresh_oor=force_oor,
                        cancel_check=_cancel_ck,
                    )
                finally:
                    with _OOR_ACTIVE_PAIR_LOCK:
                        _OOR_ACTIVE_PAIR.discard(player)
                    if cancel_id:
                        with _OOR_CANCEL_LOCK:
                            _OOR_CANCEL_REGISTRY.pop(cancel_id, None)
                self._write_json(200, {"ok": True, "player": player})
                return

            if parsed.path == "/api/pr-maker/oor-cancel":
                length = int(self.headers.get("Content-Length", "0") or "0")
                raw = self.rfile.read(length) if length > 0 else b"{}"
                try:
                    body = json.loads(raw.decode("utf-8") or "{}")
                except json.JSONDecodeError as jexc:
                    self._write_json(400, {"error": f"Invalid JSON: {jexc}"})
                    return
                cancel_id = str(body.get("cancelId", ""))
                if cancel_id:
                    with _OOR_CANCEL_LOCK:
                        if cancel_id in _OOR_CANCEL_REGISTRY:
                            _OOR_CANCEL_REGISTRY[cancel_id] = True
                self._write_json(200, {"ok": True})
                return

            if parsed.path == "/api/pr-maker/comparison":
                _ensure_runtime_deps()
                length = int(self.headers.get("Content-Length", "0") or "0")
                raw = self.rfile.read(length) if length > 0 else b"{}"
                try:
                    body = json.loads(raw.decode("utf-8") or "{}")
                except json.JSONDecodeError as jexc:
                    self._write_json(400, {"error": f"Invalid JSON: {jexc}"})
                    return
                start_iso = str(body.get("start", ""))
                end_iso = str(body.get("end", ""))
                event_slugs = list(body.get("eventSlugs", []))
                merge_rules = list(body.get("mergeRules", []))
                player_a = str(body.get("playerA", ""))
                player_b = str(body.get("playerB", ""))
                include_oor = body.get("includeOOR", False)
                if not event_slugs or not player_a or not player_b:
                    self._write_json(400, {"error": "eventSlugs, playerA, playerB required"})
                    return
                sets, elo = _pr_maker_merged_sets_and_elo(start_iso, end_iso, event_slugs, merge_rules)
                cfg = EloConfig(
                    start_date=start_iso or None, end_date=end_iso or None,
                    include_event_slugs=set(event_slugs),
                )
                ch = _pr_maker_context_hash(start_iso, end_iso, event_slugs, merge_rules)
                wh = _oor_window_hash(start_iso, end_iso)
                force_oor = bool(body.get("forceRefreshOOR", False))
                if include_oor:
                    reports = _load_reports_for_pair(
                        player_a, player_b, sets, elo, cfg,
                        ctx_hash=ch, oor_window_hash=wh, force_refresh_oor=force_oor,
                    )
                else:
                    reports = {
                        player_a: _empty_report(player_a),
                        player_b: _empty_report(player_b),
                    }
                card = _build_player_card(
                    p1=player_a, p2=player_b, elo=elo,
                    in_region_sets=sets, reports=reports,
                )
                expanded = _expanded_head_to_head(
                    player_a, player_b, sets, reports, elo=elo,
                )
                self._write_json(200, {"card": card, "expanded": expanded, "hasOOR": bool(include_oor)})
                return

            if parsed.path == "/api/pr-maker/comparison/argument":
                _ensure_runtime_deps()
                length = int(self.headers.get("Content-Length", "0") or "0")
                raw = self.rfile.read(length) if length > 0 else b"{}"
                try:
                    body = json.loads(raw.decode("utf-8") or "{}")
                except json.JSONDecodeError as jexc:
                    self._write_json(400, {"error": f"Invalid JSON: {jexc}"})
                    return
                start_iso = str(body.get("start", ""))
                end_iso = str(body.get("end", ""))
                event_slugs = list(body.get("eventSlugs", []))
                merge_rules = list(body.get("mergeRules", []))
                player_a = str(body.get("playerA", ""))
                player_b = str(body.get("playerB", ""))
                if not event_slugs or not player_a or not player_b:
                    self._write_json(400, {"error": "eventSlugs, playerA, playerB required"})
                    return
                try:
                    from openai import OpenAI as _OAI  # type: ignore
                except Exception:
                    self._write_json(503, {"error": "openai package not installed"})
                    return
                api_key = os.environ.get("OPENAI_API_KEY", "").strip()
                if not api_key:
                    self._write_json(503, {"error": "OPENAI_API_KEY not set"})
                    return
                sets, elo = _pr_maker_merged_sets_and_elo(start_iso, end_iso, event_slugs, merge_rules)
                cfg = EloConfig(
                    start_date=start_iso or None, end_date=end_iso or None,
                    include_event_slugs=set(event_slugs),
                )
                ch = _pr_maker_context_hash(start_iso, end_iso, event_slugs, merge_rules)
                wh = _oor_window_hash(start_iso, end_iso)
                if bool(body.get("includeOOR", False)):
                    reports = _load_reports_for_pair(
                        player_a, player_b, sets, elo, cfg,
                        ctx_hash=ch, oor_window_hash=wh,
                    )
                else:
                    reports = {
                        player_a: _empty_report(player_a),
                        player_b: _empty_report(player_b),
                    }
                card = _build_player_card(
                    p1=player_a, p2=player_b, elo=elo,
                    in_region_sets=sets, reports=reports,
                )
                prompt = _build_ai_justification_prompt(card)
                try:
                    client = _OAI(api_key=api_key)
                    resp = client.chat.completions.create(
                        model=str(body.get("model", "gpt-4o-mini")),
                        messages=[
                            {"role": "system", "content": "You are a careful esports ranking analyst."},
                            {"role": "user", "content": prompt},
                        ],
                    )
                    text = (resp.choices[0].message.content or "").strip()
                except Exception as oai_exc:
                    self._write_json(502, {"error": f"OpenAI call failed: {oai_exc}"})
                    return
                self._write_json(200, {"text": text})
                return

            if parsed.path == "/api/pr-maker/final-export":
                _ensure_runtime_deps()
                length = int(self.headers.get("Content-Length", "0") or "0")
                raw = self.rfile.read(length) if length > 0 else b"{}"
                try:
                    body = json.loads(raw.decode("utf-8") or "{}")
                except json.JSONDecodeError as jexc:
                    self._write_json(400, {"error": f"Invalid JSON: {jexc}"})
                    return
                start_iso = str(body.get("start", ""))
                end_iso = str(body.get("end", ""))
                event_slugs = list(body.get("eventSlugs", []))
                merge_rules = list(body.get("mergeRules", []))
                ranking = list(body.get("ranking", []))
                if not event_slugs or not ranking:
                    self._write_json(400, {"error": "eventSlugs and ranking are required"})
                    return
                server_debug_log(
                    "info", "server/CSV",
                    "final-export started",
                    f"{len(ranking)} ranked row(s) — loading OOR reports on server",
                )
                sets, elo = _pr_maker_merged_sets_and_elo(start_iso, end_iso, event_slugs, merge_rules)
                cfg = EloConfig(
                    start_date=start_iso or None, end_date=end_iso or None,
                    include_event_slugs=set(event_slugs),
                )
                names = [str(r.get("name", "")) for r in ranking]
                copeland_map = {str(r.get("name", "")): r.get("copelandScore", 0) for r in ranking}
                ch = _pr_maker_context_hash(start_iso, end_iso, event_slugs, merge_rules)
                wh = _oor_window_hash(start_iso, end_iso)
                reports = _load_reports_for_players(names, sets, elo, cfg, ctx_hash=ch, oor_window_hash=wh)
                server_debug_log(
                    "info", "server/CSV",
                    "final-export complete",
                    f"{len(names)} row(s)",
                )

                buf = io.StringIO()
                buf.write("\ufeff")
                writer = csv.writer(buf, lineterminator="\n")
                writer.writerow(_CSV_HEADER)
                for rank_idx, name in enumerate(names, start=1):
                    report = reports.get(name, _empty_report(name))
                    writer.writerow(_csv_row_for_player(
                        name, report, sets, elo, cfg,
                        rank=rank_idx, copeland=copeland_map.get(name, 0),
                    ))
                self._write_json(200, {"csv": buf.getvalue()})
                return

            if parsed.path == "/api/pr-maker/candidates-export":
                _ensure_runtime_deps()
                length = int(self.headers.get("Content-Length", "0") or "0")
                raw = self.rfile.read(length) if length > 0 else b"{}"
                try:
                    body = json.loads(raw.decode("utf-8") or "{}")
                except json.JSONDecodeError as jexc:
                    self._write_json(400, {"error": f"Invalid JSON: {jexc}"})
                    return
                start_iso = str(body.get("start", ""))
                end_iso = str(body.get("end", ""))
                event_slugs = list(body.get("eventSlugs", []))
                merge_rules = list(body.get("mergeRules", []))
                names = list(body.get("names", []))
                if not event_slugs or not names:
                    self._write_json(400, {"error": "eventSlugs and names are required"})
                    return
                server_debug_log(
                    "info", "server/CSV",
                    "candidates-export started",
                    f"{len(names)} name(s) — loading OOR reports (Start.gg on server)",
                )
                sets, elo = _pr_maker_merged_sets_and_elo(start_iso, end_iso, event_slugs, merge_rules)
                cfg = EloConfig(
                    start_date=start_iso or None, end_date=end_iso or None,
                    include_event_slugs=set(event_slugs),
                )
                ch = _pr_maker_context_hash(start_iso, end_iso, event_slugs, merge_rules)
                wh = _oor_window_hash(start_iso, end_iso)
                sorted_names = sorted(names, key=lambda n: elo.get(n, cfg.initial_elo), reverse=True)
                copeland_pool = _pool_copeland_scores(sorted_names, sets)
                reports = _load_reports_for_players(sorted_names, sets, elo, cfg, ctx_hash=ch, oor_window_hash=wh)
                server_debug_log(
                    "info", "server/CSV",
                    "candidates-export complete",
                    f"{len(sorted_names)} row(s)",
                )
                req_columns = list(body.get("columns", []))
                use_header = _CSV_HEADER
                col_indices = None
                if req_columns:
                    req_set = set(req_columns)
                    filtered = [c for c in _CSV_HEADER if c in req_set or c == "player"]
                    if filtered:
                        use_header = filtered
                        col_indices = [_CSV_HEADER.index(c) for c in use_header]
                buf = io.StringIO()
                buf.write("\ufeff")
                writer = csv.writer(buf, lineterminator="\n")
                writer.writerow(use_header)
                for rank_idx, name in enumerate(sorted_names, start=1):
                    report = reports.get(name, _empty_report(name))
                    cval = copeland_pool.get(name, 0.0)
                    copeland_cell: Any = int(cval) if cval == int(cval) else round(cval, 2)
                    full_row = _csv_row_for_player(
                        name, report, sets, elo, cfg,
                        rank=rank_idx, copeland=copeland_cell,
                    )
                    writer.writerow([full_row[i] for i in col_indices] if col_indices else full_row)
                self._write_json(200, {"csv": buf.getvalue()})
                return

            if parsed.path == "/api/pr-maker/process/start":
                length = int(self.headers.get("Content-Length", "0") or "0")
                raw = self.rfile.read(length) if length > 0 else b"{}"
                try:
                    body = json.loads(raw.decode("utf-8") or "{}")
                except json.JSONDecodeError as jexc:
                    self._write_json(400, {"error": f"Invalid JSON: {jexc}"})
                    return
                slugs = list(body.get("eventSlugs", []))
                if not slugs:
                    self._write_json(400, {"error": "eventSlugs list is required"})
                    return
                job_id = str(uuid.uuid4())
                with JOB_LOCK:
                    PR_MAKER_PROCESS_JOBS[job_id] = {
                        "id": job_id,
                        "status": "running",
                        "phase": "queued",
                        "currentEvent": 0,
                        "totalEvents": len(slugs),
                        "eventSlugs": list(slugs),
                        "currentEventName": "",
                        "progressPct": 0,
                        "currentEventSets": 0,
                        "currentEventSetsProcessed": 0,
                        "totalSetsProcessed": 0,
                        "error": None,
                    }
                Thread(
                    target=_pr_maker_process_worker,
                    args=(job_id,),
                    kwargs={"event_slugs": slugs},
                    daemon=True,
                ).start()
                self._write_json(200, {"jobId": job_id})
                return

            self._write_json(404, {"error": f"Unknown path: {parsed.path}"})
        except ValueError as exc:
            self._write_json(400, {"error": str(exc)})
        except OSError as exc:
            if _is_client_disconnect(exc):
                return
            raise
        except Exception as exc:
            try:
                self._write_json(
                    500,
                    {
                        "error": str(exc),
                        "traceback": traceback.format_exc(limit=3),
                    },
                )
            except OSError as wexc:
                if _is_client_disconnect(wexc):
                    return
                raise

    def do_GET(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        params = parse_qs(parsed.query)

        try:
            if parsed.path == "/api/health":
                self._write_json(200, {"ok": True})
                return

            if parsed.path == "/api/debug/server-events":
                since_raw = str(params.get("since", ["0"])[0] or "0")
                try:
                    since = max(0, int(since_raw))
                except ValueError:
                    since = 0
                with _SERVER_DEBUG_LOCK:
                    max_seq = _SERVER_DEBUG_SEQ
                    events = [dict(e) for e in _SERVER_DEBUG_EVENTS if e["seq"] > since]
                self._write_json(200, {"events": events, "maxSeq": max_seq})
                return

            if parsed.path == "/api/elo":
                mode = str(params.get("mode", ["all-time"])[0] or "all-time")
                start = params.get("start", [None])[0]
                end = params.get("end", [None])[0]
                query = str(params.get("query", [""])[0] or "")
                max_players = int(str(params.get("maxPlayers", ["500"])[0]))
                max_players = max(1, min(max_players, 5000))
                payload = _build_elo_payload(
                    mode=mode,
                    start_date=start,
                    end_date=end,
                    query=query,
                    max_players=max_players,
                )
                self._write_json(200, payload)
                return

            if parsed.path == "/api/elo/date-range/start":
                start = str(params.get("start", [""])[0] or "")
                end = str(params.get("end", [""])[0] or "")
                if not start or not end:
                    raise ValueError("start and end are required")
                job_id = str(uuid.uuid4())
                with JOB_LOCK:
                    DATE_RANGE_JOBS[job_id] = {
                        "id": job_id,
                        "status": "running",
                        "phase": "queued",
                        "processedEvents": 0,
                        "totalEvents": 0,
                        "progressPct": 0.0,
                        "result": None,
                        "error": None,
                    }
                Thread(target=_date_range_worker, args=(job_id,), kwargs={"start": start, "end": end}, daemon=True).start()
                self._write_json(200, {"jobId": job_id})
                return

            if parsed.path == "/api/elo/date-range/status":
                job_id = str(params.get("jobId", [""])[0] or "")
                if not job_id:
                    raise ValueError("jobId is required")
                with JOB_LOCK:
                    job = DATE_RANGE_JOBS.get(job_id)
                    payload = dict(job) if job else None
                if payload is None:
                    self._write_json(404, {"error": "Unknown jobId"})
                    return
                self._write_json(200, payload)
                return

            if parsed.path == "/api/recent-events/start":
                days = int(str(params.get("days", ["30"])[0]))
                limit = int(str(params.get("limit", ["10"])[0]))
                sample = int(str(params.get("sampleRegistrants", ["8"])[0]))
                days = max(1, min(days, 365))
                limit = max(1, min(limit, 25))
                sample = max(1, min(sample, 16))
                job_id = str(uuid.uuid4())
                with JOB_LOCK:
                    RECENT_EVENT_JOBS[job_id] = {
                        "id": job_id,
                        "status": "running",
                        "events": [],
                        "completed": 0,
                        "totalExpected": limit,
                        "remaining": limit,
                        "error": None,
                    }
                Thread(
                    target=_progressive_recent_events_worker,
                    args=(job_id,),
                    kwargs={"days": days, "limit": limit, "sample_registrants": sample},
                    daemon=True,
                ).start()
                self._write_json(200, {"jobId": job_id})
                return

            if parsed.path == "/api/recent-events/status":
                job_id = str(params.get("jobId", [""])[0] or "")
                if not job_id:
                    raise ValueError("jobId is required")
                with JOB_LOCK:
                    job = RECENT_EVENT_JOBS.get(job_id)
                    payload = dict(job) if job else None
                if payload is None:
                    self._write_json(404, {"error": "Unknown jobId"})
                    return
                self._write_json(200, payload)
                return

            if parsed.path == "/api/pr-maker/scrape/start":
                start = str(params.get("start", [""])[0] or "")
                end = str(params.get("end", [""])[0] or "")
                if not start or not end:
                    raise ValueError("start and end are required")
                fresh_raw = str(params.get("fresh", ["false"])[0] or "false").lower()
                fresh = fresh_raw in ("true", "1", "yes")
                job_id = str(uuid.uuid4())
                with JOB_LOCK:
                    PR_MAKER_SCRAPE_JOBS[job_id] = {
                        "id": job_id,
                        "status": "running",
                        "phase": "queued",
                        "fresh": fresh,
                        "tournamentsCached": 0,
                        "tournamentsNew": 0,
                        "tournamentsTotal": 0,
                        "log": [],
                        "error": None,
                    }
                Thread(
                    target=_pr_maker_scrape_worker,
                    args=(job_id,),
                    kwargs={"start": start, "end": end, "fresh": fresh},
                    daemon=True,
                ).start()
                self._write_json(200, {"jobId": job_id})
                return

            if parsed.path == "/api/pr-maker/scrape/status":
                job_id = str(params.get("jobId", [""])[0] or "")
                if not job_id:
                    raise ValueError("jobId is required")
                with JOB_LOCK:
                    job = PR_MAKER_SCRAPE_JOBS.get(job_id)
                    payload = dict(job) if job else None
                if payload is None:
                    self._write_json(404, {"error": "Unknown jobId"})
                    return
                self._write_json(200, payload)
                return

            if parsed.path == "/api/pr-maker/events":
                start = str(params.get("start", [""])[0] or "")
                end = str(params.get("end", [""])[0] or "")
                if not start or not end:
                    raise ValueError("start and end are required")
                events = _list_cached_events_for_range(start, end)
                self._write_json(200, {"events": events})
                return

            if parsed.path == "/api/pr-maker/process/status":
                job_id = str(params.get("jobId", [""])[0] or "")
                if not job_id:
                    raise ValueError("jobId is required")
                with JOB_LOCK:
                    job = PR_MAKER_PROCESS_JOBS.get(job_id)
                    payload = dict(job) if job else None
                if payload is None:
                    self._write_json(404, {"error": "Unknown jobId"})
                    return
                self._write_json(200, payload)
                return

            if parsed.path == "/api/pr-maker/oor-warm/status":
                job_id = str(params.get("jobId", [""])[0] or "")
                if not job_id:
                    raise ValueError("jobId is required")
                with JOB_LOCK:
                    job = OOR_WARM_JOBS.get(job_id)
                    payload = dict(job) if job else None
                if payload is None:
                    self._write_json(404, {"error": "Unknown jobId"})
                    return
                self._write_json(200, payload)
                return

            if parsed.path == "/api/calendar/day":
                raw_date = str(params.get("date", [""])[0] or "")
                sample = int(str(params.get("sampleRegistrants", ["10"])[0]))
                sample = max(0, min(sample, 24))
                target_day = _parse_iso_date(raw_date) if raw_date else _today_pacific()
                cards = _build_calendar_cards(
                    target_start=target_day,
                    target_end=target_day,
                    sample_registrants=sample,
                )
                self._write_json(
                    200,
                    {
                        "date": _date_to_str(target_day),
                        "weekday": _weekday_label(_date_to_str(target_day)),
                        "events": cards,
                        "count": len(cards),
                    },
                )
                return

            if parsed.path == "/api/calendar/week":
                raw_date = str(params.get("date", [""])[0] or "")
                sample = int(str(params.get("sampleRegistrants", ["8"])[0]))
                sample = max(0, min(sample, 24))
                anchor_day = _parse_iso_date(raw_date) if raw_date else _today_pacific()
                week_start, week_end = _week_bounds_sun_sat(anchor_day)
                cards = _build_calendar_cards(
                    target_start=week_start,
                    target_end=week_end,
                    sample_registrants=sample,
                )
                grouped: dict[str, list[dict[str, Any]]] = {}
                for card in cards:
                    iso_day = str(card.get("date") or "")
                    if not iso_day:
                        continue
                    grouped.setdefault(iso_day, []).append(card)
                ordered_days = sorted(grouped.keys())
                grouped_payload = [
                    {
                        "date": day,
                        "weekday": _weekday_label(day),
                        "events": grouped[day],
                    }
                    for day in ordered_days
                    if grouped[day]
                ]
                self._write_json(
                    200,
                    {
                        "anchorDate": _date_to_str(anchor_day),
                        "weekStart": _date_to_str(week_start),
                        "weekEnd": _date_to_str(week_end),
                        "days": grouped_payload,
                        "count": len(cards),
                    },
                )
                return

            if parsed.path == "/api/coverage/resolve/status":
                job_id = str(params.get("jobId", [""])[0] or "")
                if not job_id:
                    raise ValueError("jobId is required")
                with JOB_LOCK:
                    job = COVERAGE_RESOLVE_JOBS.get(job_id)
                    payload = dict(job) if job else None
                if payload is None:
                    self._write_json(404, {"error": "Unknown jobId"})
                    return
                self._write_json(200, payload)
                return

            self._write_json(404, {"error": f"Unknown path: {parsed.path}"})
        except ValueError as exc:
            self._write_json(400, {"error": str(exc)})
        except OSError as exc:
            if _is_client_disconnect(exc):
                return
            raise
        except Exception as exc:
            try:
                self._write_json(
                    500,
                    {
                        "error": str(exc),
                        "traceback": traceback.format_exc(limit=3),
                    },
                )
            except OSError as wexc:
                if _is_client_disconnect(wexc):
                    return
                raise


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Local API for NorCal Smash React UI")
    parser.add_argument("--host", default="127.0.0.1", help="Bind host")
    parser.add_argument("--port", type=int, default=8765, help="Bind port")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    server = ThreadingHTTPServer((args.host, args.port), ApiHandler)
    print(f"[NorCal Smash API] serving on http://{args.host}:{args.port}")
    server.serve_forever()


if __name__ == "__main__":
    main()
