"""
Diagnostic for the NorCal geo-filter bug (notebook all-functions.ipynb).

Replicates the notebook's tournament query against start.gg and compares:
  1. The buggy behavior: radius hardcoded to "50mi" + pagination capped at 9 pages.
  2. The intended behavior: SF @ 70mi, Sacramento @ 40mi, full pagination.

Checks specifically whether Guildhouse (San Jose) events are captured.
"""

from __future__ import annotations

import json
import os
import sys
import time
from pathlib import Path

import requests

try:
    from dotenv import load_dotenv
except ImportError:
    load_dotenv = None

REPO_ROOT = Path(__file__).resolve().parent.parent
if load_dotenv:
    load_dotenv(REPO_ROOT / ".env")

TOKEN = (os.environ.get("STARTGG_API_KEY") or "").strip()
if not TOKEN:
    sys.exit("STARTGG_API_KEY not set")

URL = "https://api.start.gg/gql/alpha"

QUERY = """
query NorCalTournaments($page: Int, $perPage: Int, $coordinates: String!, $radius: String!, $after: Timestamp, $before: Timestamp) {
  tournaments(
    query: {
      page: $page
      perPage: $perPage
      filter: {
        location: { distanceFrom: $coordinates, distance: $radius }
        afterDate: $after
        beforeDate: $before
      }
      sortBy: "startAt"
    }
  ) {
    pageInfo { total totalPages }
    nodes {
      id
      name
      city
      slug
      startAt
      events { slug numEntrants videogame { name } }
    }
  }
}
""".strip()

SF = "37.77151615492457, -122.41563048985462"
SAC = "38.57608096237729, -121.49183616631059"

AFTER = int(time.mktime(time.strptime("2025-04-01", "%Y-%m-%d")))
BEFORE = int(time.mktime(time.strptime("2025-06-30", "%Y-%m-%d")))


def fetch_region(coords: str, radius: str, max_pages: int | None, per_page: int = 50) -> tuple[list[dict], int]:
    """Returns (nodes, reported_total). max_pages=None means paginate to the end."""
    nodes: list[dict] = []
    total = -1
    page = 1
    while True:
        if max_pages is not None and page > max_pages:
            break
        variables = {
            "page": page, "perPage": per_page,
            "coordinates": coords, "radius": radius,
            "after": AFTER, "before": BEFORE,
        }
        r = requests.post(
            URL,
            headers={"Authorization": f"Bearer {TOKEN}"},
            json={"query": QUERY, "variables": variables},
            timeout=60,
        )
        data = r.json()
        if "errors" in data:
            print(f"    [error on page {page}]: {data['errors']}")
            break
        block = data["data"]["tournaments"]
        total = block["pageInfo"]["total"]
        batch = block["nodes"] or []
        nodes.extend(batch)
        if not batch or len(batch) < per_page:
            break
        page += 1
        time.sleep(0.8)  # stay under 80 req/min
    return nodes, total


def summarize(label: str, nodes: list[dict]) -> dict:
    by_id = {str(t["id"]): t for t in nodes}
    cities = {}
    guildhouse = []
    for t in by_id.values():
        cities[t.get("city") or "?"] = cities.get(t.get("city") or "?", 0) + 1
        if "guildhouse" in (t.get("name") or "").lower() or "guildhouse" in (t.get("slug") or "").lower():
            guildhouse.append(t)
    print(f"\n=== {label} ===")
    print(f"  unique tournaments: {len(by_id)}")
    top = sorted(cities.items(), key=lambda kv: -kv[1])[:12]
    print(f"  top cities: {top}")
    print(f"  Guildhouse tournaments found: {len(guildhouse)}")
    for g in guildhouse[:5]:
        print(f"    - {g['name']} ({g['city']}) start.gg/{g['slug']}")
    return by_id


print(f"Date window: 2025-04-01 .. 2025-06-30 (unix {AFTER}..{BEFORE})")

# --- Buggy notebook behavior: radius forced to 50mi, pages capped at 9 ---
print("\n[1] Notebook-as-written: SF @ '50mi' (hardcoded), pages 1-9 only")
buggy_sf, total_sf_50 = fetch_region(SF, "50mi", max_pages=9)
print(f"  API reports total={total_sf_50} tournaments in range; fetched {len(buggy_sf)}")
buggy_ids = summarize("SF 50mi, capped at 9 pages", buggy_sf)

# --- Intended behavior: SF @ 70mi, full pagination ---
print("\n[2] Intended: SF @ '70mi', full pagination")
good_sf, total_sf_70 = fetch_region(SF, "70mi", max_pages=None)
print(f"  API reports total={total_sf_70}; fetched {len(good_sf)}")
good_sf_ids = summarize("SF 70mi, full", good_sf)

print("\n[3] Sacramento @ '40mi', full pagination")
sac, total_sac = fetch_region(SAC, "40mi", max_pages=None)
print(f"  API reports total={total_sac}; fetched {len(sac)}")
sac_ids = summarize("SAC 40mi, full", sac)

missing = set(good_sf_ids) - set(buggy_ids)
print(f"\nBay tournaments missed by the buggy notebook logic: {len(missing)}")
missed_guildhouse = [good_sf_ids[i] for i in missing
                     if "guildhouse" in (good_sf_ids[i].get("name") or "").lower()]
print(f"  ...of which Guildhouse: {len(missed_guildhouse)}")
for g in missed_guildhouse[:10]:
    print(f"    - {g['name']} ({g['city']})")
