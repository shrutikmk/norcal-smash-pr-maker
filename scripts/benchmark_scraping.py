#!/usr/bin/env python3
"""
Benchmark the scraping pipeline: request count, wall time, cache-hit %, and the
projected request count under the *old* per-set (N+1) approach for comparison.

Usage:
  # Estimate only (no API calls): show projected requests/time for a fresh fetch.
  python scripts/benchmark_scraping.py --start 2026-01-01 --end 2026-03-31 --dry-run

  # Real run: scrape + process the range and report measured metrics.
  python scripts/benchmark_scraping.py --start 2026-01-01 --end 2026-03-31

  # Tune on the fly:
  STARTGG_CONCURRENCY=12 STARTGG_SETS_PER_PAGE=60 \
      python scripts/benchmark_scraping.py --start 2026-01-01 --end 2026-03-31

The "old approach" figure models the previous implementation, which issued:
  per event: 1 (event id) + ceil(sets / 20) (set-id pages) + 1 per individual set
so its requests grow with the *total number of sets*, not pages — the bottleneck
this rewrite removes.
"""

from __future__ import annotations

import argparse
import sqlite3
import sys
import time
from math import ceil
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "demo" / "base_demo"))

try:
    from dotenv import load_dotenv

    load_dotenv(ROOT / ".env")
except ImportError:
    pass

import os  # noqa: E402

import startgg_rate_gate as gate  # noqa: E402
import tournament_processor as tp  # noqa: E402
import tournament_scraper as ts  # noqa: E402

OLD_SETS_ID_PER_PAGE = 20  # the previous conservative set-id pagination size


def _count_sets_in_range(processed_db: Path, tournament_db: Path, start: str, end: str) -> tuple[int, int]:
    """Return (num_events, num_sets) currently cached for the range, for old-cost modeling."""
    if not processed_db.exists():
        return 0, 0
    after = tp._date_to_unix(start)
    before = tp._date_to_unix(end)
    pconn = sqlite3.connect(str(processed_db))
    # event_slugs in range come from the tournament cache
    slugs: list[str] = []
    if tournament_db.exists():
        tconn = sqlite3.connect(str(tournament_db))
        rows = tconn.execute(
            "SELECT DISTINCT event_slug FROM tournaments "
            "WHERE start_at >= ? AND start_at <= ? AND event_num_entrants >= 16",
            (after, before),
        ).fetchall()
        tconn.close()
        slugs = [r[0] for r in rows if r[0]]
    if not slugs:
        return 0, 0
    placeholders = ",".join("?" for _ in slugs)
    num_sets = pconn.execute(
        f"SELECT COUNT(*) FROM sets_cache WHERE event_slug IN ({placeholders})", slugs
    ).fetchone()[0]
    pconn.close()
    return len(slugs), int(num_sets)


def _old_request_model(num_events: int, num_sets: int) -> int:
    """Requests the previous N+1 implementation would have issued."""
    avg_sets = (num_sets / num_events) if num_events else 0
    set_id_pages = ceil(avg_sets / OLD_SETS_ID_PER_PAGE) if avg_sets else 1
    return num_events * (1 + set_id_pages) + num_sets


def main() -> None:
    parser = argparse.ArgumentParser(description="Benchmark scraping performance")
    parser.add_argument("--start", default="2026-01-01")
    parser.add_argument("--end", default="2026-03-31")
    parser.add_argument("--game", default="Super Smash Bros. Ultimate")
    parser.add_argument("--min-entrants", type=int, default=16)
    parser.add_argument("--dry-run", action="store_true", help="Estimate only; no API calls")
    parser.add_argument("--no-async", action="store_true")
    parser.add_argument("--concurrency", type=int, default=None)
    parser.add_argument("--per-page", type=int, default=None)
    args = parser.parse_args()

    if not os.environ.get("STARTGG_API_KEY") and not args.dry_run:
        print("ERROR: STARTGG_API_KEY not set (needed for a real run). Use --dry-run to estimate.")
        sys.exit(1)

    concurrency = args.concurrency or ts.DEFAULT_CONCURRENCY
    per_page = tp._safe_sets_per_page(args.per_page) if args.per_page else tp.DEFAULT_SETS_PER_PAGE
    use_async = not args.no_async

    print("=" * 72)
    print("SCRAPING BENCHMARK")
    print(f"  range={args.start}..{args.end}  game={args.game!r}")
    print(f"  async={use_async}  concurrency={concurrency}  sets_per_page={per_page}")
    print(f"  rate gate: {gate.get_metrics()['max_rpm']:.0f} req/min, "
          f"min interval {gate.get_metrics()['min_interval_sec']:.2f}s")
    print("=" * 72)

    proc_cfg = tp.ProcessorConfig(
        start_date=args.start,
        end_date=args.end,
        game_filter=args.game,
        min_entrants=args.min_entrants,
        sets_per_page=per_page,
        concurrency=concurrency,
        use_async=use_async,
        dry_run=args.dry_run,
    )

    if args.dry_run:
        # Scraper dry estimate (plan only) + processor dry estimate.
        tp.process_tournaments(proc_cfg, verbose=True)
        return

    # --- Scrape phase ---
    req0 = gate.get_request_count()
    t0 = time.time()
    ts.scrape_tournaments(
        ts.ScraperConfig(
            start_date=args.start,
            end_date=args.end,
            game_filter=args.game,
            min_entrants=args.min_entrants,
            concurrency=concurrency,
            use_async=use_async,
        ),
        verbose=False,
    )
    scrape_reqs = gate.get_request_count() - req0
    scrape_time = time.time() - t0

    # --- Process phase ---
    req1 = gate.get_request_count()
    t1 = time.time()
    sets, stats = tp.process_tournaments(proc_cfg, verbose=False)
    proc_reqs = gate.get_request_count() - req1
    proc_time = time.time() - t1

    num_events, num_sets = _count_sets_in_range(
        tp.DEFAULT_PROCESSED_CACHE, tp.DEFAULT_TOURNAMENT_CACHE, args.start, args.end
    )
    old_proc_reqs = _old_request_model(num_events, num_sets)
    total_reqs = scrape_reqs + proc_reqs
    total_time = scrape_time + proc_time
    metrics = gate.get_metrics()

    print("\nRESULTS")
    print("-" * 72)
    print(f"  Scrape phase:   {scrape_reqs:>5} requests  {scrape_time:>7.1f}s")
    print(f"  Process phase:  {proc_reqs:>5} requests  {proc_time:>7.1f}s")
    print(f"  TOTAL (new):    {total_reqs:>5} requests  {total_time:>7.1f}s")
    eff = (total_reqs / total_time * 60.0) if total_time else 0.0
    print(f"  Effective rate: ~{eff:.0f} req/min   (cap {metrics['max_rpm']:.0f})")
    print(f"  Rate-limit (429) backoffs: {metrics['rate_limit_hits']:.0f}")
    print("-" * 72)
    print(f"  Events in range: {num_events}   sets cached: {num_sets}")
    hit_pct = (stats.event_hits / stats.total_events * 100.0) if stats.total_events else 0.0
    print(f"  Event cache hits: {stats.event_hits}/{stats.total_events} ({hit_pct:.0f}%)")
    print("-" * 72)
    print("  OLD per-set (N+1) model for the SAME data:")
    print(f"    process requests would be ~{old_proc_reqs} "
          f"(vs {proc_reqs} now)")
    if proc_reqs > 0:
        print(f"    ⇒ ~{old_proc_reqs / max(1, proc_reqs):.0f}x fewer process requests")
    print("=" * 72)


if __name__ == "__main__":
    main()
