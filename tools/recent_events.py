"""
Recent NorCal events inspector (standalone tool).

Outputs the most recent concluded tournaments in NorCal and, for each event:
- bracket link
- top 8 placements
- random sample of registrants not in top 8

Rate limits are respected for start.gg API usage:
- 80 requests / 60 seconds
- 1000 objects / request
https://developer.start.gg/docs/rate-limits/
"""

from __future__ import annotations

import argparse
import os
import random
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

_BASE_DEMO = Path(__file__).resolve().parent.parent / "demo" / "base_demo"
if str(_BASE_DEMO) not in sys.path:
    sys.path.insert(0, str(_BASE_DEMO))
from startgg_rate_gate import acquire_slot, is_likely_rate_limit_error, sleep_after_429

_IMPORT_ERROR: Exception | None = None
try:
    import requests
except Exception as e:  # pragma: no cover - environment dependency guard
    requests = Any  # type: ignore
    _IMPORT_ERROR = e

try:
    from dotenv import load_dotenv

    load_dotenv(Path(__file__).resolve().parent.parent / ".env")
except Exception:
    pass

API_URL = "https://api.start.gg/gql/alpha"
RATE_LIMIT_REQUESTS_PER_MIN = 80
RATE_LIMIT_INTERVAL_SEC = max(1.0, 60.0 / RATE_LIMIT_REQUESTS_PER_MIN)
MAX_OBJECTS_PER_REQUEST = 1000

# Super Smash Bros. Ultimate videogame ID (matches demo/base_demo)
SMASH_ULTIMATE_VIDEOGAME_ID = 1386
GAME_FILTER = "Super Smash Bros. Ultimate"

# Matches existing NorCal coordinates used in demo pipeline.
NORCAL_REGIONS: dict[str, tuple[str, str]] = {
    "bay": ("37.77151615492457, -122.41563048985462", "70mi"),
    "sacramento": ("38.57608096237729, -121.49183616631059", "40mi"),
}

GET_TOURNAMENTS_BY_LOCATION_QUERY = """
query TournamentsByLocation(
  $page: Int!,
  $perPage: Int!,
  $coordinates: String!,
  $radius: String!,
  $afterDate: Timestamp,
  $beforeDate: Timestamp,
  $videogameIds: [ID]
) {
  tournaments(
    query: {
      page: $page
      perPage: $perPage
      filter: {
        location: { distanceFrom: $coordinates, distance: $radius }
        afterDate: $afterDate
        beforeDate: $beforeDate
        videogameIds: $videogameIds
      }
      sortBy: "startAt"
    }
  ) {
    pageInfo {
      total
      totalPages
      page
    }
    nodes {
      id
      name
      slug
      city
      startAt
      endAt
    }
  }
}
"""

GET_TOURNAMENT_DETAILS_QUERY = """
query TournamentDetails($slug: String!) {
  tournament(slug: $slug) {
    id
    name
    slug
    startAt
    endAt
    images {
      url
      type
    }
    events {
      id
      name
      slug
      numEntrants
      videogame {
        id
        name
      }
    }
  }
}
"""

GET_EVENT_STANDINGS_QUERY = """
query EventStandings($eventId: ID!, $page: Int!, $perPage: Int!) {
  event(id: $eventId) {
    id
    name
    standings(query: { page: $page, perPage: $perPage }) {
      pageInfo {
        total
        totalPages
      }
      nodes {
        placement
        entrant {
          id
          name
        }
      }
    }
  }
}
"""

GET_EVENT_ENTRANTS_QUERY = """
query EventEntrants($eventId: ID!, $page: Int!, $perPage: Int!) {
  event(id: $eventId) {
    id
    name
    numEntrants
    entrants(query: { page: $page, perPage: $perPage }) {
      pageInfo {
        total
        totalPages
      }
      nodes {
        id
        name
        participants {
          player {
            gamerTag
          }
        }
      }
    }
  }
}
"""

GET_EVENT_SETS_QUERY = """
query EventSets($eventId: ID!, $page: Int!, $perPage: Int!) {
  event(id: $eventId) {
    id
    name
    sets(page: $page, perPage: $perPage, sortType: STANDARD) {
      pageInfo {
        totalPages
      }
      nodes {
        id
        winnerId
        slots {
          entrant {
            id
            name
            participants {
              player {
                gamerTag
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


class RateLimiter:
    def __init__(self, interval_sec: float = RATE_LIMIT_INTERVAL_SEC):
        self.interval = interval_sec
        self._last_request_time = 0.0

    def wait(self) -> None:
        elapsed = time.monotonic() - self._last_request_time
        if elapsed < self.interval:
            time.sleep(self.interval - elapsed)
        self._last_request_time = time.monotonic()


class StartGGClient:
    def __init__(self, auth_token: str):
        if not auth_token:
            raise ValueError("Missing STARTGG_API_KEY")
        self.auth_token = auth_token
        self.session = requests.Session()
        self.limiter = RateLimiter()

    def gql(self, query: str, variables: dict[str, Any], *, max_retries: int = 30) -> dict[str, Any]:
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
                last_error = requests.exceptions.HTTPError("429 Rate Limited", response=resp)
                sleep_after_429(attempt, resp)
                continue
            if 500 <= resp.status_code < 600:
                try:
                    resp.raise_for_status()
                except requests.exceptions.HTTPError as e:
                    last_error = e
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
                last_error = RuntimeError(msg)
                if is_likely_rate_limit_error(payload, msg) or "timeout" in msg.lower():
                    sleep_after_429(attempt, resp)
                    continue
                raise RuntimeError(msg)
            if "errors" in payload:
                msg = str(payload["errors"])
                last_error = RuntimeError(msg)
                if is_likely_rate_limit_error(payload, msg) or "timeout" in msg.lower():
                    sleep_after_429(attempt, resp)
                    continue
                raise RuntimeError(f"GraphQL errors: {msg}")
            return payload
        raise RuntimeError(f"Request failed after {max_retries} retries") from last_error


@dataclass
class RecentEventsConfig:
    limit_tournaments: int = 10
    sample_registrants: int = 10
    regions: list[str] | None = None
    min_entrants: int = 0
    seed: int | None = None
    verbose: bool = True
    fetch_window_days: int = 1200

    def __post_init__(self) -> None:
        if self.regions is None:
            self.regions = ["bay", "sacramento"]


def _stage_banner(title: str) -> None:
    print("\n" + "=" * 88)
    print(title)
    print("=" * 88)


def _now_unix() -> int:
    return int(datetime.now(tz=timezone.utc).timestamp())


def _fmt_date(unix_ts: int | None) -> str:
    if unix_ts is None:
        return "unknown-date"
    return datetime.fromtimestamp(int(unix_ts), tz=timezone.utc).strftime("%Y-%m-%d")


def _safe_per_page(base: int, approx_objects_per_node: int) -> int:
    limit = max(1, MAX_OBJECTS_PER_REQUEST // max(1, approx_objects_per_node))
    return min(base, limit)


def _pick_tournament_image(images: list[dict[str, Any]]) -> str | None:
    if not images:
        return None
    for img in images:
        if str(img.get("type") or "").lower() == "profile" and img.get("url"):
            return str(img["url"])
    for img in images:
        if str(img.get("type") or "").lower() == "banner" and img.get("url"):
            return str(img["url"])
    if images[0].get("url"):
        return str(images[0]["url"])
    return None


def _fetch_recent_tournament_stubs(client: StartGGClient, config: RecentEventsConfig) -> list[dict[str, Any]]:
    """
    Fetch the most recent concluded tournaments with minimal API calls.
    Uses date range + per-region last-page fetch: sortBy startAt is ascending,
    so the last page has the newest tournaments. We fetch page 1 for pageInfo,
    then only the last page per region.
    """
    now_unix = _now_unix()
    after_unix = now_unix - config.fetch_window_days * 24 * 60 * 60
    per_page = _safe_per_page(base=50, approx_objects_per_node=10)

    out_by_id: dict[str, dict[str, Any]] = {}
    for region in config.regions or []:
        if region not in NORCAL_REGIONS:
            raise ValueError(f"Unknown region {region!r}; expected one of {sorted(NORCAL_REGIONS)}")
        coords, radius = NORCAL_REGIONS[region]

        # Page 1: get pageInfo and initial nodes (filter: Super Smash Bros. Ultimate only)
        data = client.gql(
            GET_TOURNAMENTS_BY_LOCATION_QUERY,
            {
                "page": 1,
                "perPage": per_page,
                "coordinates": coords,
                "radius": radius,
                "afterDate": after_unix,
                "beforeDate": now_unix,
                "videogameIds": [SMASH_ULTIMATE_VIDEOGAME_ID],
            },
        )
        block = data.get("data", {}).get("tournaments", {}) or {}
        nodes = block.get("nodes", []) or []
        page_info = block.get("pageInfo", {}) or {}
        total_pages = int(page_info.get("totalPages") or 1)

        if config.verbose:
            total = page_info.get("total")
            print(f"  [API] region={region!r} page=1: {len(nodes)} tournaments (totalPages={total_pages}, total={total})")

        for t in nodes:
            tid = str(t.get("id") or "")
            if not tid:
                continue
            end_at = t.get("endAt")
            start_at = t.get("startAt")
            if end_at is not None and int(end_at) > now_unix:
                continue
            if start_at is not None and int(start_at) > now_unix:
                continue
            out_by_id[tid] = t

        # Only fetch last page if there is more than one page (last page = newest tournaments)
        if total_pages > 1:
            data_last = client.gql(
                GET_TOURNAMENTS_BY_LOCATION_QUERY,
                {
                    "page": total_pages,
                    "perPage": per_page,
                    "coordinates": coords,
                    "radius": radius,
                    "afterDate": after_unix,
                    "beforeDate": now_unix,
                    "videogameIds": [SMASH_ULTIMATE_VIDEOGAME_ID],
                },
            )
            nodes_last = (
                data_last.get("data", {}).get("tournaments", {}).get("nodes", []) or []
            )
            if config.verbose:
                print(f"  [API] region={region!r} page={total_pages} (last): {len(nodes_last)} tournaments")
            for t in nodes_last:
                tid = str(t.get("id") or "")
                if not tid:
                    continue
                end_at = t.get("endAt")
                start_at = t.get("startAt")
                if end_at is not None and int(end_at) > now_unix:
                    continue
                if start_at is not None and int(start_at) > now_unix:
                    continue
                out_by_id[tid] = t

    out = list(out_by_id.values())
    out.sort(key=lambda t: int(t.get("startAt") or 0), reverse=True)
    return out[: config.limit_tournaments]


def _fetch_tournament_details(client: StartGGClient, slug: str) -> dict[str, Any] | None:
    payload = client.gql(GET_TOURNAMENT_DETAILS_QUERY, {"slug": slug})
    return payload.get("data", {}).get("tournament")


def _fetch_event_top8_standings(client: StartGGClient, event_id: str) -> list[dict[str, Any]]:
    payload = client.gql(
        GET_EVENT_STANDINGS_QUERY,
        {
            "eventId": event_id,
            "page": 1,
            "perPage": 8,
        },
    )
    nodes = payload.get("data", {}).get("event", {}).get("standings", {}).get("nodes", []) or []
    top8: list[dict[str, Any]] = []
    for row in nodes:
        placement = row.get("placement")
        entrant_name = (row.get("entrant") or {}).get("name")
        if isinstance(placement, int) and entrant_name:
            top8.append({"placement": placement, "name": str(entrant_name)})
    top8.sort(key=lambda r: int(r["placement"]))
    return top8[:8]


def _safe_slot_score(slot: dict[str, Any]) -> int | None:
    standing = slot.get("standing") or {}
    stats = standing.get("stats") or {}
    score = stats.get("score") or {}
    val = score.get("value")
    return int(val) if isinstance(val, int) else None


def _entrant_display_name(entrant: dict[str, Any]) -> str:
    name = str(entrant.get("name") or "").strip()
    if name:
        return name
    participants = entrant.get("participants") or []
    if participants:
        player = participants[0].get("player") or {}
        tag = str(player.get("gamerTag") or "").strip()
        if tag:
            return tag
    return "(unknown)"


def _fetch_event_top8_from_sets_fallback(client: StartGGClient, event_id: str) -> list[dict[str, Any]]:
    per_page = _safe_per_page(base=40, approx_objects_per_node=22)
    page = 1
    stats: dict[str, dict[str, Any]] = {}

    while True:
        payload = client.gql(
            GET_EVENT_SETS_QUERY,
            {"eventId": event_id, "page": page, "perPage": per_page},
        )
        sets_block = payload.get("data", {}).get("event", {}).get("sets", {}) or {}
        nodes = sets_block.get("nodes", []) or []
        total_pages = int((sets_block.get("pageInfo") or {}).get("totalPages") or 1)
        for node in nodes:
            slots = node.get("slots") or []
            if len(slots) < 2:
                continue
            s1, s2 = slots[0], slots[1]
            e1 = s1.get("entrant") or {}
            e2 = s2.get("entrant") or {}
            n1 = _entrant_display_name(e1)
            n2 = _entrant_display_name(e2)
            score1 = _safe_slot_score(s1)
            score2 = _safe_slot_score(s2)
            if score1 is None or score2 is None:
                continue
            rec1 = stats.setdefault(n1, {"name": n1, "wins": 0, "losses": 0, "sets": 0})
            rec2 = stats.setdefault(n2, {"name": n2, "wins": 0, "losses": 0, "sets": 0})
            rec1["sets"] += 1
            rec2["sets"] += 1
            if score1 > score2:
                rec1["wins"] += 1
                rec2["losses"] += 1
            elif score2 > score1:
                rec2["wins"] += 1
                rec1["losses"] += 1
        if page >= total_pages:
            break
        page += 1

    ranked = sorted(
        stats.values(),
        key=lambda r: (-int(r["wins"]), int(r["losses"]), str(r["name"]).casefold()),
    )
    out: list[dict[str, Any]] = []
    for i, rec in enumerate(ranked[:8], 1):
        out.append({"placement": i, "name": str(rec["name"])})
    return out


def _fetch_all_event_entrants(client: StartGGClient, event_id: str) -> list[str]:
    per_page = _safe_per_page(base=50, approx_objects_per_node=10)
    page = 1
    names: list[str] = []
    seen: set[str] = set()
    while True:
        payload = client.gql(
            GET_EVENT_ENTRANTS_QUERY,
            {"eventId": event_id, "page": page, "perPage": per_page},
        )
        entrants_block = payload.get("data", {}).get("event", {}).get("entrants", {}) or {}
        nodes = entrants_block.get("nodes", []) or []
        total_pages = int((entrants_block.get("pageInfo") or {}).get("totalPages") or 1)
        for node in nodes:
            name = str(node.get("name") or "").strip()
            if not name:
                participants = node.get("participants") or []
                if participants:
                    tag = str(((participants[0].get("player") or {}).get("gamerTag") or "")).strip()
                    name = tag
            if not name:
                continue
            key = name.casefold()
            if key in seen:
                continue
            seen.add(key)
            names.append(name)
        if page >= total_pages:
            break
        page += 1
    return names


def _event_link_from_slug(slug: str) -> str:
    # Event slug is usually like: tournament/.../event/...
    return f"https://start.gg/{slug.lstrip('/')}"


def run_recent_events(config: RecentEventsConfig) -> None:
    token = os.environ.get("STARTGG_API_KEY", "")
    if not token:
        raise ValueError("STARTGG_API_KEY is required in env/.env")

    rng = random.Random(config.seed)
    client = StartGGClient(token)

    _stage_banner("[RECENT EVENTS] Fetch most recent concluded NorCal tournaments")
    stubs = _fetch_recent_tournament_stubs(client, config)
    print(f"[INFO] Concluded tournaments found (deduped, capped): {len(stubs)}")

    if not stubs:
        print("[INFO] No tournaments found for current query window.")
        return

    for i, stub in enumerate(stubs, 1):
        slug = str(stub.get("slug") or "")
        if not slug:
            continue
        if config.verbose:
            print(f"\n[TOURNAMENT {i}/{len(stubs)}] slug={slug!r} -> fetching details")
        details = _fetch_tournament_details(client, slug)
        if not details:
            print(f"\n[TOURNAMENT {i}] Could not fetch details for slug={slug!r}, skipping")
            continue

        tname = str(details.get("name") or stub.get("name") or "(unknown tournament)")
        tdate = _fmt_date(details.get("startAt") if details.get("startAt") is not None else stub.get("startAt"))
        timage = _pick_tournament_image(details.get("images") or [])
        all_events = details.get("events") or []
        events = [
            ev
            for ev in all_events
            if str((ev.get("videogame") or {}).get("name") or "").strip() == GAME_FILTER
        ]
        if not events:
            if config.verbose:
                print(f"\n[TOURNAMENT {i}] No {GAME_FILTER!r} events, skipping")
            continue

        print("\n" + "-" * 88)
        print(f"Tournament #{i}: {tname}")
        print(f"Date: {tdate}")
        print(f"Tournament Link: https://start.gg/{slug}")
        print(f"Tournament Icon URL: {timage or '(none found)'}")
        print(f"Events ({GAME_FILTER}, {len(events)}):")

        for ev_idx, ev in enumerate(events, 1):
            event_id = str(ev.get("id") or "")
            event_name = str(ev.get("name") or "(unknown event)")
            event_slug = str(ev.get("slug") or "")
            num_entrants = ev.get("numEntrants")
            event_link = _event_link_from_slug(event_slug) if event_slug else "(missing event slug)"

            print(f"\n  [Event {ev_idx}/{len(events)}] {event_name}")
            print(f"    Bracket: {event_link}")
            print(f"    Registered Count: {num_entrants if num_entrants is not None else '(unknown)'}")

            top8 = []
            standings_mode = "standings"
            if event_id:
                try:
                    top8 = _fetch_event_top8_standings(client, event_id)
                except Exception as e:
                    if config.verbose:
                        print(f"    [WARN] standings query failed: {e}")
                if not top8:
                    standings_mode = "sets-fallback"
                    try:
                        top8 = _fetch_event_top8_from_sets_fallback(client, event_id)
                    except Exception as e:
                        if config.verbose:
                            print(f"    [WARN] sets fallback failed: {e}")
                        top8 = []

            print(f"    Top 8 ({standings_mode}):")
            if not top8:
                print("      (unavailable)")
            else:
                for row in top8:
                    print(f"      {int(row['placement']):>2}. {row['name']}")

            random_registrants: list[str] = []
            if event_id:
                try:
                    entrants = _fetch_all_event_entrants(client, event_id)
                    top8_names_cf = {str(r["name"]).casefold() for r in top8}
                    non_top8 = [n for n in entrants if n.casefold() not in top8_names_cf]
                    k = min(config.sample_registrants, len(non_top8))
                    random_registrants = rng.sample(non_top8, k) if k > 0 else []
                except Exception as e:
                    if config.verbose:
                        print(f"    [WARN] entrants fetch failed: {e}")

            print(f"    Random registrants not in Top 8 (up to {config.sample_registrants}):")
            if not random_registrants:
                print("      (none)")
            else:
                for name in random_registrants:
                    print(f"      - {name}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Return the most recent concluded NorCal tournaments and per-event details: "
            "image, date, event links, top 8, and random non-top8 registrants."
        )
    )
    parser.add_argument("--limit", type=int, default=10, help="Number of tournaments to return (default: 10)")
    parser.add_argument(
        "--sample-registrants",
        type=int,
        default=10,
        help="Random non-top8 registrants per event (default: 10)",
    )
    parser.add_argument("--regions", nargs="+", default=["bay", "sacramento"], help="Regions to query")
    parser.add_argument("--seed", type=int, default=None, help="Optional RNG seed for reproducible sampling")
    parser.add_argument(
        "--window-days",
        type=int,
        default=1200,
        help="How far back to search when finding recent tournaments (default: 1200)",
    )
    parser.add_argument("--quiet", action="store_true", help="Reduce verbose logs")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if _IMPORT_ERROR is not None:
        raise RuntimeError(
            "Missing runtime dependencies for recent_events.py. "
            "Run with the project virtualenv/interpreter that has dependencies installed."
        ) from _IMPORT_ERROR
    cfg = RecentEventsConfig(
        limit_tournaments=max(1, args.limit),
        sample_registrants=max(0, args.sample_registrants),
        regions=list(args.regions),
        seed=args.seed,
        verbose=not args.quiet,
        fetch_window_days=max(30, args.window_days),
    )
    run_recent_events(cfg)


if __name__ == "__main__":
    main()
