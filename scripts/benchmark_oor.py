#!/usr/bin/env python3
"""
Benchmark the OOR (out-of-region) data pipeline end-to-end against a running
local API server (tools/web_api.py).

Measures, for a real PR Maker context (date window + event slugs):
  1. Warm job wall time + start.gg requests used for the top-N candidates.
  2. Comparison-card latency for sample pairs via the cache-only fast path
     (the single round trip the ranking UI makes per pair on a warm cache).
  3. Final CSV export wall time for the full candidate list.
  4. Cache hit/miss counters diffed across the run (/api/pr-maker/oor-stats).

Typical usage (server already running on :8000, events already processed):

    .venv/bin/python scripts/benchmark_oor.py \
        --start 2025-01-01 --end 2025-06-01 \
        --slugs tournament/x/event/singles,tournament/y/event/singles \
        --top 25

Run it twice to see cold-vs-warm behavior: the first run pays the live
start.gg fetches, the second should be nearly all cache hits.
"""

from __future__ import annotations

import argparse
import json
import random
import sys
import time
import urllib.request


def _post(base: str, path: str, body: dict) -> dict:
    req = urllib.request.Request(
        f"{base}{path}",
        data=json.dumps(body).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=600) as resp:
        return json.loads(resp.read().decode("utf-8"))


def _get(base: str, path: str) -> dict:
    with urllib.request.urlopen(f"{base}{path}", timeout=60) as resp:
        return json.loads(resp.read().decode("utf-8"))


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--base-url", default="http://127.0.0.1:8000")
    ap.add_argument("--start", required=True, help="PR window start (YYYY-MM-DD)")
    ap.add_argument("--end", required=True, help="PR window end (YYYY-MM-DD)")
    ap.add_argument("--slugs", required=True,
                    help="Comma-separated event slugs (or @file with one slug per line)")
    ap.add_argument("--top", type=int, default=25, help="Top-N candidates by ELO to benchmark")
    ap.add_argument("--pairs", type=int, default=10, help="Sample comparison pairs to time")
    ap.add_argument("--seed", type=int, default=7)
    args = ap.parse_args()

    if args.slugs.startswith("@"):
        with open(args.slugs[1:], encoding="utf-8") as fh:
            slugs = [ln.strip() for ln in fh if ln.strip()]
    else:
        slugs = [s.strip() for s in args.slugs.split(",") if s.strip()]
    shared = {"start": args.start, "end": args.end, "eventSlugs": slugs, "mergeRules": []}
    base = args.base_url.rstrip("/")
    rng = random.Random(args.seed)

    stats0 = _get(base, "/api/pr-maker/oor-stats")
    print(f"tuning: {stats0.get('tuning')}")

    # --- candidate pool -----------------------------------------------------
    cand = _post(base, "/api/pr-maker/candidates", shared)
    players = sorted(cand.get("players", []), key=lambda p: p.get("elo", 0), reverse=True)
    names = [p["name"] for p in players[: args.top]]
    if len(names) < 2:
        print("Not enough candidates in this context — check slugs/date window.", file=sys.stderr)
        return 1
    print(f"candidates: {len(players)} total, benchmarking top {len(names)}")

    # --- 1. warm job ----------------------------------------------------------
    t0 = time.monotonic()
    job = _post(base, "/api/pr-maker/oor-warm/start", {**shared, "names": names})
    job_id = job["jobId"]
    status: dict = {}
    while True:
        time.sleep(1.0)
        status = _get(base, f"/api/pr-maker/oor-warm/status?jobId={job_id}")
        if status.get("status") in ("done", "error"):
            break
        print(f"\r  warm {status.get('completed', 0)}/{status.get('total', '?')} "
              f"({status.get('currentPlayer', '')})", end="", flush=True)
    warm_s = time.monotonic() - t0
    print(f"\nwarm job: {status.get('status')} in {warm_s:.1f}s · "
          f"~{status.get('requestsUsed', '?')} start.gg request(s) for {len(names)} player(s)")

    # --- 2. comparison fast path ----------------------------------------------
    latencies = []
    for _ in range(min(args.pairs, len(names) * (len(names) - 1) // 2)):
        a, b = rng.sample(names, 2)
        t = time.monotonic()
        data = _post(base, "/api/pr-maker/comparison",
                     {**shared, "playerA": a, "playerB": b, "includeOOR": True, "oorCacheOnly": True})
        dt = (time.monotonic() - t) * 1000
        missing = data.get("missingOOR") or []
        latencies.append(dt)
        print(f"  comparison {a} vs {b}: {dt:.0f}ms"
              f"{' (missing OOR: ' + ', '.join(missing) + ')' if missing else ' (full OOR from cache)'}")
    if latencies:
        latencies.sort()
        print(f"comparison latency: median {latencies[len(latencies) // 2]:.0f}ms · "
              f"max {latencies[-1]:.0f}ms over {len(latencies)} pair(s)")

    # --- 3. final CSV export ----------------------------------------------------
    ranking = [{"name": n, "copelandScore": len(names) - i} for i, n in enumerate(names)]
    t0 = time.monotonic()
    csv_resp = _post(base, "/api/pr-maker/final-export", {**shared, "ranking": ranking})
    export_s = time.monotonic() - t0
    n_rows = max(0, len(csv_resp.get("csv", "").splitlines()) - 1)
    print(f"final-export: {n_rows} row(s) in {export_s:.2f}s")

    # --- 4. cache counters -------------------------------------------------------
    stats1 = _get(base, "/api/pr-maker/oor-stats")
    c0, c1 = stats0.get("counters", {}), stats1.get("counters", {})
    diff = {k: c1.get(k, 0) - c0.get(k, 0) for k in c1}
    req_used = stats1.get("rateGate", {}).get("requests", 0) - stats0.get("rateGate", {}).get("requests", 0)
    print("\ncache counters for this run:")
    for k in sorted(diff):
        if diff[k]:
            print(f"  {k}: +{diff[k]}")
    hits = diff.get("report_mem_hits", 0) + diff.get("report_sqlite_hits", 0) + diff.get("report_rebuilds", 0)
    total_reports = hits + diff.get("report_live_fetches", 0)
    if total_reports:
        print(f"report cache hit rate: {hits}/{total_reports} ({100.0 * hits / total_reports:.0f}%)")
    print(f"start.gg requests attributable to this run: ~{req_used:.0f}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
