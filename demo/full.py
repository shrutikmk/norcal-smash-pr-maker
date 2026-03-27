"""
Full NorCal pipeline demo runner (Parts 1-4) in one file.

Runs, in order:
1) tournament_scraper.py
2) tournament_processor.py
3) elo_calculator.py
4) player_ranking.py

Default date window: 2026-01-01 to 2026-03-31.
"""

from __future__ import annotations

import argparse
import contextlib
import os
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any, TextIO

# Allow importing the four pipeline modules directly from demo/base_demo/.
THIS_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = THIS_DIR.parent
BASE_DEMO_DIR = THIS_DIR / "base_demo"

try:
    from dotenv import load_dotenv
    load_dotenv(PROJECT_ROOT / ".env")
except ImportError:
    pass
if str(BASE_DEMO_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DEMO_DIR))


DEFAULT_START_DATE = "2026-01-01"
DEFAULT_END_DATE = "2026-03-31"


class TeeStream:
    """Write output to terminal and an on-disk log file."""

    def __init__(self, console_stream: TextIO, file_stream: TextIO):
        self.console_stream = console_stream
        self.file_stream = file_stream

    def write(self, data: str) -> int:
        a = self.console_stream.write(data)
        self.file_stream.write(data)
        return a

    def flush(self) -> None:
        self.console_stream.flush()
        self.file_stream.flush()


@contextlib.contextmanager
def tee_output(log_path: Path):
    """Context manager that mirrors stdout/stderr to a log file."""
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with log_path.open("w", encoding="utf-8") as f:
        old_out, old_err = sys.stdout, sys.stderr
        out_tee = TeeStream(old_out, f)
        err_tee = TeeStream(old_err, f)
        try:
            sys.stdout = out_tee
            sys.stderr = err_tee
            yield
        finally:
            sys.stdout = old_out
            sys.stderr = old_err


def _stage_banner(title: str) -> None:
    print("\n" + "=" * 88)
    print(title)
    print("=" * 88)


def run_full_pipeline(args: argparse.Namespace) -> None:
    if not os.environ.get("STARTGG_API_KEY"):
        raise ValueError("Missing STARTGG_API_KEY in environment or .env")

    # Lazy imports keep `python demo/full.py --help` working without runtime deps installed.
    from tournament_scraper import ScraperConfig, scrape_tournaments
    from tournament_processor import ProcessorConfig, process_tournaments
    from elo_calculator import EloConfig, run_demo
    from player_ranking import RankingConfig, run_ranking_demo

    _stage_banner("[FULL DEMO] NorCal Pipeline (Parts 1-4)")
    print(f"Date range: {args.start} to {args.end}")
    print("Order: scraper -> processor -> elo demo -> player ranking demo")
    print(f"Verbose mode: {not args.quiet}")

    started_at = time.time()

    # Stage 1: tournament scraper
    stage_start = time.time()
    _stage_banner("[STAGE 1/4] tournament_scraper.py")
    scraper_cfg = ScraperConfig(
        start_date=args.start,
        end_date=args.end,
        game_filter=args.game,
        min_entrants=args.min_entrants,
    )
    scraped, scrape_stats = scrape_tournaments(scraper_cfg, verbose=not args.quiet)
    print(
        f"[STAGE 1 DONE] Returned {len(scraped)} filtered event rows | "
        f"cache hits={scrape_stats.hits}, misses={scrape_stats.misses} | "
        f"elapsed={time.time() - stage_start:.1f}s"
    )

    # Stage 2: tournament processor
    stage_start = time.time()
    _stage_banner("[STAGE 2/4] tournament_processor.py")
    processor_cfg = ProcessorConfig(
        start_date=args.start,
        end_date=args.end,
        game_filter=args.game,
        min_entrants=args.min_entrants,
    )
    processed_sets, proc_stats = process_tournaments(processor_cfg, verbose=not args.quiet)
    err_msg = ""
    if proc_stats.event_api_errors or proc_stats.set_api_errors:
        err_msg = f" | API errors: events={proc_stats.event_api_errors}, sets={proc_stats.set_api_errors}"
    print(
        f"[STAGE 2 DONE] Processed sets={len(processed_sets)} | "
        f"event hits={proc_stats.event_hits}, misses={proc_stats.event_misses} | "
        f"set hits={proc_stats.set_hits}, misses={proc_stats.set_misses}{err_msg} | "
        f"elapsed={time.time() - stage_start:.1f}s"
    )

    # Stage 3: ELO + derived stats demo
    stage_start = time.time()
    _stage_banner("[STAGE 3/4] elo_calculator.py")
    elo_cfg = EloConfig(
        start_date=args.start,
        end_date=args.end,
        max_out_region_tournaments=args.max_oor_tournaments,
    )
    run_demo(elo_cfg, verbose=not args.quiet)
    print(f"[STAGE 3 DONE] elapsed={time.time() - stage_start:.1f}s")

    # Stage 4: pairwise ranking demo
    stage_start = time.time()
    _stage_banner("[STAGE 4/4] player_ranking.py")
    ranking_cfg = RankingConfig(
        start_date=args.start,
        end_date=args.end,
        contenders_from_top_n=args.top_n,
        contenders_count=args.contenders,
        random_seed=args.seed,
        openai_model=args.openai_model,
        interactive=args.interactive,
        verbose=not args.quiet,
    )
    final_ranking: list[tuple[str, int]] = run_ranking_demo(ranking_cfg)
    print(f"[STAGE 4 DONE] elapsed={time.time() - stage_start:.1f}s")

    _stage_banner("[FULL DEMO COMPLETE]")
    print(f"Total elapsed: {time.time() - started_at:.1f}s")
    print("Final pairwise ranking:")
    for i, (name, wins) in enumerate(final_ranking, 1):
        print(f"  {i}. {name} (pairwise wins={wins})")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Full end-to-end NorCal demo in one file. Runs scraper -> processor -> "
            "ELO demo -> player ranking demo."
        )
    )
    parser.add_argument("--start", default=DEFAULT_START_DATE, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end", default=DEFAULT_END_DATE, help="End date (YYYY-MM-DD)")
    parser.add_argument("--game", default="Super Smash Bros. Ultimate", help="Game filter")
    parser.add_argument("--min-entrants", type=int, default=16, help="Minimum entrants per event")
    parser.add_argument(
        "--max-oor-tournaments",
        type=int,
        default=20,
        help="Max out-of-region tournaments inspected per player in live mode",
    )
    parser.add_argument("--top-n", type=int, default=30, help="Part 4: sample contenders from top N ELO")
    parser.add_argument("--contenders", type=int, default=5, help="Part 4: contender count")
    parser.add_argument("--seed", type=int, default=None, help="Optional random seed for reproducible runs")
    parser.add_argument("--openai-model", default="gpt-4o-mini", help="Part 4 OpenAI model")
    parser.add_argument("--interactive", action="store_true", help="Part 4 interactive AI choice prompts")
    parser.add_argument("--quiet", action="store_true", help="Reduce verbosity from each stage")
    parser.add_argument(
        "--log-file",
        default=None,
        help="Optional log file path. Default: data/demo_logs/full_pipeline_<timestamp>.txt",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    log_path = (
        Path(args.log_file)
        if args.log_file
        else (THIS_DIR.parent / "data" / "demo_logs" / f"full_pipeline_{datetime.now():%Y%m%d_%H%M%S}.txt")
    )

    # Always write a full log because this demo is intentionally verbose.
    with tee_output(log_path):
        print(f"[LOG] Mirroring output to: {log_path}")
        run_full_pipeline(args)

    print(f"[DONE] Full demo log written to: {log_path}")


if __name__ == "__main__":
    main()
