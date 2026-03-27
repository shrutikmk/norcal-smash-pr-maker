"""
Standalone ELO display tool for fast cache-aware experimentation.

This tool:
- picks a random 4-week window by default (between 2018-12-08 and today)
- runs scraper + processor for that window (cache-aware, verbose)
- computes top-K ELO in-window
- computes top-K ELO with random 30% tournament exclusion (in-window)
- computes top-K all-time ELO over the currently scraped corpus
- reports likely missing date ranges in the local corpus cache
"""

from __future__ import annotations

import argparse
import os
import random
import sys
from dataclasses import replace
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

# Allow importing demo/base_demo modules directly.
THIS_FILE = Path(__file__).resolve()
PROJECT_ROOT = THIS_FILE.parent.parent
BASE_DEMO_DIR = PROJECT_ROOT / "demo" / "base_demo"
if str(BASE_DEMO_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DEMO_DIR))

_IMPORT_ERROR: Exception | None = None
try:
    from elo_calculator import EloConfig, compute_elo  # type: ignore  # noqa: E402
    from tournament_processor import ProcessorConfig, process_tournaments  # type: ignore  # noqa: E402
    from tournament_scraper import (  # type: ignore  # noqa: E402
        ScraperConfig,
        compute_week_ranges_missing,
        scrape_tournaments,
    )
except Exception as e:  # pragma: no cover - environment-specific dependency guard
    _IMPORT_ERROR = e
    EloConfig = Any  # type: ignore
    ProcessorConfig = Any  # type: ignore
    ScraperConfig = Any  # type: ignore

    def compute_week_ranges_missing(*args: Any, **kwargs: Any) -> Any:
        raise RuntimeError("demo/base_demo imports unavailable in current interpreter") from _IMPORT_ERROR

    def compute_elo(*args: Any, **kwargs: Any) -> Any:
        raise RuntimeError("demo/base_demo imports unavailable in current interpreter") from _IMPORT_ERROR

    def process_tournaments(*args: Any, **kwargs: Any) -> Any:
        raise RuntimeError("demo/base_demo imports unavailable in current interpreter") from _IMPORT_ERROR

    def scrape_tournaments(*args: Any, **kwargs: Any) -> Any:
        raise RuntimeError("demo/base_demo imports unavailable in current interpreter") from _IMPORT_ERROR

ULT_RELEASE_DATE = date(2018, 12, 8)


def _parse_date_or_die(raw: str) -> date:
    try:
        return datetime.strptime(raw, "%Y-%m-%d").date()
    except ValueError as e:
        raise ValueError(f"Invalid date {raw!r}; expected YYYY-MM-DD") from e


def _date_to_str(d: date) -> str:
    return d.strftime("%Y-%m-%d")


def _pick_random_4_week_window(rng: random.Random) -> tuple[date, date]:
    today = date.today()
    span_days = 28
    latest_start = today - timedelta(days=span_days - 1)
    if latest_start < ULT_RELEASE_DATE:
        return ULT_RELEASE_DATE, today
    window_count = (latest_start - ULT_RELEASE_DATE).days + 1
    start = ULT_RELEASE_DATE + timedelta(days=rng.randrange(window_count))
    end = min(today, start + timedelta(days=span_days - 1))
    return start, end


def _stage_banner(title: str) -> None:
    print("\n" + "=" * 88)
    print(title)
    print("=" * 88)


def _copy_elo_cfg_with(
    cfg: EloConfig,
    *,
    start_date: str | None = None,
    end_date: str | None = None,
    exclude_event_slugs: set[str] | None = None,
    exclude_tournament_ids: set[str] | None = None,
    exclude_tournament_names: set[str] | None = None,
) -> EloConfig:
    return replace(
        cfg,
        start_date=start_date if start_date is not None else cfg.start_date,
        end_date=end_date if end_date is not None else cfg.end_date,
        exclude_event_slugs=exclude_event_slugs if exclude_event_slugs is not None else cfg.exclude_event_slugs,
        exclude_tournament_ids=exclude_tournament_ids if exclude_tournament_ids is not None else cfg.exclude_tournament_ids,
        exclude_tournament_names=exclude_tournament_names
        if exclude_tournament_names is not None
        else cfg.exclude_tournament_names,
    )


def _apply_include_filters(
    sets_in: list[dict[str, Any]],
    *,
    include_event_slugs: set[str],
    include_tournament_ids: set[str],
    include_tournament_names: set[str],
) -> list[dict[str, Any]]:
    if not include_event_slugs and not include_tournament_ids and not include_tournament_names:
        return sets_in

    out: list[dict[str, Any]] = []
    for s in sets_in:
        event_slug = str(s.get("event_slug") or "")
        tournament_id = str(s.get("tournament_id") or "")
        tournament_name = str(s.get("tournament_name") or "")
        name_cf = tournament_name.casefold()

        keep = False
        if include_event_slugs and event_slug in include_event_slugs:
            keep = True
        if include_tournament_ids and tournament_id in include_tournament_ids:
            keep = True
        if include_tournament_names and any(tok.casefold() in name_cf for tok in include_tournament_names):
            keep = True
        if keep:
            out.append(s)
    return out


def _recompute_elo_from_sets(
    sets_in: list[dict[str, Any]],
    *,
    k_factor: float,
    initial_elo: float,
) -> dict[str, float]:
    def update(elo: dict[str, float], p1: str, p2: str, s1: int, s2: int) -> None:
        r1 = elo.get(p1, initial_elo)
        r2 = elo.get(p2, initial_elo)
        e1 = 1.0 / (1 + 10 ** ((r2 - r1) / 400.0))
        outcome = 1 if s1 > s2 else 0
        elo[p1] = r1 + k_factor * (outcome - e1)
        elo[p2] = r2 + k_factor * ((1 - outcome) - (1 - e1))

    ordered = sorted(
        sets_in,
        key=lambda s: (int(s.get("start_at") or 0), str(s.get("set_id") or "")),
    )
    players = {str(s["p1"]) for s in ordered} | {str(s["p2"]) for s in ordered}
    elo: dict[str, float] = {p: initial_elo for p in players}
    for s in ordered:
        update(elo, str(s["p1"]), str(s["p2"]), int(s["p1_score"]), int(s["p2_score"]))
    return dict(sorted(elo.items(), key=lambda x: x[1], reverse=True))


def _compute_elo_with_includes(
    cfg: EloConfig,
    *,
    include_event_slugs: set[str],
    include_tournament_ids: set[str],
    include_tournament_names: set[str],
) -> tuple[dict[str, float], list[dict[str, Any]]]:
    elo_base, sets_base = compute_elo(cfg)
    sets_filtered = _apply_include_filters(
        sets_base,
        include_event_slugs=include_event_slugs,
        include_tournament_ids=include_tournament_ids,
        include_tournament_names=include_tournament_names,
    )
    if len(sets_filtered) == len(sets_base):
        return elo_base, sets_base
    return (
        _recompute_elo_from_sets(
            sets_filtered,
            k_factor=cfg.k_factor,
            initial_elo=cfg.initial_elo,
        ),
        sets_filtered,
    )


def _print_top_k(elo: dict[str, float], top_k: int, *, title: str) -> None:
    print(f"\n[{title}]")
    if not elo:
        print("  (no players)")
        return
    for i, (name, score) in enumerate(list(elo.items())[:top_k], 1):
        print(f"  {i:2}. {name}: {score:.2f}")


def _print_top_k_with_delta(
    baseline: dict[str, float],
    variant: dict[str, float],
    top_k: int,
    *,
    title: str,
) -> None:
    print(f"\n[{title}]")
    top_players = [name for name, _ in list(variant.items())[:top_k]]
    if not top_players:
        print("  (no players)")
        return
    for i, name in enumerate(top_players, 1):
        score = variant.get(name, 0.0)
        base = baseline.get(name, score)
        delta = score - base
        sign = "+" if delta >= 0 else ""
        print(f"  {i:2}. {name}: {score:.2f} ({sign}{delta:.2f} vs baseline)")


def _week_ranges_missing(
    tournament_cache_path: Path,
    *,
    game_filter: str,
    start_date: date,
    end_date: date,
) -> list[tuple[date, date]]:
    return compute_week_ranges_missing(
        tournament_cache_path,
        game_filter=game_filter,
        start_date=start_date,
        end_date=end_date,
    )


def _print_missing_ranges(game_filter: str, tournament_cache_path: Path, limit: int) -> None:
    today = date.today()
    missing = _week_ranges_missing(
        tournament_cache_path,
        game_filter=game_filter,
        start_date=ULT_RELEASE_DATE,
        end_date=today,
    )

    print("\n[MISSING DATE RANGES TO POPULATE] (unscraped weeks; weeks confirmed empty via scrape are excluded)")
    if not missing:
        print(f"  No missing weekly ranges detected for {game_filter!r} between {ULT_RELEASE_DATE} and {today}.")
        return

    print(
        f"  Found {len(missing)} missing range(s) between {ULT_RELEASE_DATE} and {today}; "
        f"showing first {min(limit, len(missing))}:"
    )
    for i, (a, b) in enumerate(missing[:limit], 1):
        days = (b - a).days + 1
        print(f"  {i:2}. {_date_to_str(a)} -> {_date_to_str(b)} ({days} days)")
    if len(missing) > limit:
        print(f"  ... and {len(missing) - limit} more")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Standalone cache-aware ELO display tool. By default, picks a random 4-week "
            "window, runs scrape+process for it, then prints in-range and all-time ELO views."
        )
    )
    parser.add_argument("--start", default=None, help="Start date YYYY-MM-DD (default: random 4-week window)")
    parser.add_argument("--end", default=None, help="End date YYYY-MM-DD (default: random 4-week window)")
    parser.add_argument("--top-k", type=int, default=10, help="Top-K players to display (default: 10)")
    parser.add_argument("--seed", type=int, default=None, help="Optional random seed")
    parser.add_argument("--game", default="Super Smash Bros. Ultimate", help="Game filter")
    parser.add_argument("--min-entrants", type=int, default=16, help="Minimum entrants per event")
    parser.add_argument("--regions", nargs="+", default=["bay", "sacramento"], help="Regions for stage-1 scrape")
    parser.add_argument("--skip-random-drop", action="store_true", help="Skip random 30%% tournament exclusion demo")
    parser.add_argument("--exclude-event", action="append", default=[], help="Exclude event slug (repeatable)")
    parser.add_argument("--exclude-tournament-id", action="append", default=[], help="Exclude tournament ID (repeatable)")
    parser.add_argument(
        "--exclude-tournament-name",
        action="append",
        default=[],
        help="Exclude tournament name substring (repeatable)",
    )
    parser.add_argument("--include-event", action="append", default=[], help="Include only event slugs (repeatable)")
    parser.add_argument("--include-tournament-id", action="append", default=[], help="Include only tournament IDs")
    parser.add_argument(
        "--include-tournament-name",
        action="append",
        default=[],
        help="Include only tournament name substrings",
    )
    parser.add_argument("--missing-limit", type=int, default=20, help="Max missing ranges to print")
    parser.add_argument("--quiet", action="store_true", help="Reduce verbose logs")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    verbose = not args.quiet

    if _IMPORT_ERROR is not None:
        raise RuntimeError(
            "Could not import demo/base_demo pipeline modules. "
            "Run with the project virtualenv/interpreter that has dependencies installed."
        ) from _IMPORT_ERROR

    rng = random.Random(args.seed)
    if args.start and args.end:
        start_d = _parse_date_or_die(args.start)
        end_d = _parse_date_or_die(args.end)
    elif args.start or args.end:
        raise ValueError("Provide both --start and --end together, or neither.")
    else:
        start_d, end_d = _pick_random_4_week_window(rng)

    if end_d < start_d:
        raise ValueError("--end must be >= --start")

    start_s = _date_to_str(start_d)
    end_s = _date_to_str(end_d)

    _stage_banner("[ELO DISPLAY] Standalone cache-aware runner")
    print(f"Window used: {start_s} -> {end_s}")
    print(f"Game filter: {args.game!r}, min entrants: {args.min_entrants}")
    print(f"Top-K requested: {args.top_k}")
    if args.seed is not None:
        print(f"Random seed: {args.seed}")

    if not os.environ.get("STARTGG_API_KEY"):
        raise ValueError("Missing STARTGG_API_KEY in environment or .env")

    _stage_banner("[STAGE 1] Scrape tournaments for selected window")
    scraper_cfg = ScraperConfig(
        start_date=start_s,
        end_date=end_s,
        game_filter=args.game,
        min_entrants=args.min_entrants,
        regions=args.regions,
    )
    filtered_tournaments, scrape_stats = scrape_tournaments(scraper_cfg, verbose=verbose)
    print(
        f"[STAGE 1 DONE] filtered rows={len(filtered_tournaments)} | "
        f"cache hits={scrape_stats.hits}, misses={scrape_stats.misses}"
    )

    _stage_banner("[STAGE 2] Process sets for selected window")
    processor_cfg = ProcessorConfig(
        start_date=start_s,
        end_date=end_s,
        game_filter=args.game,
        min_entrants=args.min_entrants,
    )
    processed_sets, proc_stats = process_tournaments(processor_cfg, verbose=verbose)
    print(
        f"[STAGE 2 DONE] processed sets={len(processed_sets)} | "
        f"event hits={proc_stats.event_hits}, misses={proc_stats.event_misses}, api_errors={proc_stats.event_api_errors} | "
        f"set hits={proc_stats.set_hits}, misses={proc_stats.set_misses}, api_errors={proc_stats.set_api_errors}"
    )
    if scrape_stats.misses == 0 and proc_stats.event_misses == 0 and proc_stats.set_misses == 0:
        print("[LATENCY NOTE] Range appears fully cached; API work was largely avoided.")
    else:
        print("[LATENCY NOTE] New cache entries were added for this run.")

    include_event_slugs = set(args.include_event)
    include_tournament_ids = {str(x) for x in args.include_tournament_id}
    include_tournament_names = set(args.include_tournament_name)
    exclude_event_slugs = set(args.exclude_event)
    exclude_tournament_ids = {str(x) for x in args.exclude_tournament_id}
    exclude_tournament_names = set(args.exclude_tournament_name)

    range_cfg = EloConfig(
        start_date=start_s,
        end_date=end_s,
        exclude_event_slugs=exclude_event_slugs,
        exclude_tournament_ids=exclude_tournament_ids,
        exclude_tournament_names=exclude_tournament_names,
    )

    _stage_banner("[STAGE 3] In-range ELO (baseline)")
    elo_range, sets_range = _compute_elo_with_includes(
        range_cfg,
        include_event_slugs=include_event_slugs,
        include_tournament_ids=include_tournament_ids,
        include_tournament_names=include_tournament_names,
    )
    print(f"Sets in baseline in-range pool: {len(sets_range)}")
    print(f"Players in baseline in-range ELO: {len(elo_range)}")
    _print_top_k(elo_range, args.top_k, title=f"TOP {args.top_k} ELO - SELECTED WINDOW")

    if not args.skip_random_drop:
        _stage_banner("[STAGE 4] In-range ELO with random 30% tournament exclusion")
        unique_tournaments: dict[tuple[str, str], dict[str, str]] = {}
        for s in sets_range:
            tid = str(s.get("tournament_id") or "")
            tname = str(s.get("tournament_name") or "")
            slug = str(s.get("event_slug") or "")
            unique_tournaments[(tid, tname)] = {"tournament_id": tid, "tournament_name": tname, "event_slug": slug}

        tourneys = list(unique_tournaments.values())
        random_exclude_n = 0
        sampled: list[dict[str, str]] = []
        if tourneys:
            random_exclude_n = max(1, int(round(len(tourneys) * 0.30)))
            random_exclude_n = min(random_exclude_n, len(tourneys))
            sampled = rng.sample(tourneys, random_exclude_n)

        sampled_ids = {t["tournament_id"] for t in sampled if t["tournament_id"]}
        sampled_names = {t["tournament_name"] for t in sampled if not t["tournament_id"] and t["tournament_name"]}
        sampled_events = {t["event_slug"] for t in sampled if not t["tournament_id"] and not t["tournament_name"] and t["event_slug"]}

        dropped_cfg = _copy_elo_cfg_with(
            range_cfg,
            exclude_event_slugs=range_cfg.exclude_event_slugs | sampled_events,
            exclude_tournament_ids=range_cfg.exclude_tournament_ids | sampled_ids,
            exclude_tournament_names=range_cfg.exclude_tournament_names | sampled_names,
        )
        elo_dropped, sets_dropped = _compute_elo_with_includes(
            dropped_cfg,
            include_event_slugs=include_event_slugs,
            include_tournament_ids=include_tournament_ids,
            include_tournament_names=include_tournament_names,
        )

        print(f"Tournaments in baseline pool: {len(tourneys)}")
        print(f"Randomly excluded this run: {random_exclude_n} (~30%)")
        if sampled:
            print("Sample of excluded tournaments this run:")
            for t in sampled[:10]:
                name = t["tournament_name"] or "(no-name)"
                tid = t["tournament_id"] or "n/a"
                print(f"  - id={tid} name={name}")
            if len(sampled) > 10:
                print(f"  ... and {len(sampled) - 10} more")
        print(f"Sets remaining after random exclusion: {len(sets_dropped)}")
        _print_top_k_with_delta(
            elo_range,
            elo_dropped,
            args.top_k,
            title=f"TOP {args.top_k} ELO - WINDOW WITH RANDOM 30% EXCLUSION",
        )

    _stage_banner("[STAGE 5] All-time ELO over local corpus")
    all_time_cfg = EloConfig(
        start_date=None,
        end_date=None,
        exclude_event_slugs=exclude_event_slugs,
        exclude_tournament_ids=exclude_tournament_ids,
        exclude_tournament_names=exclude_tournament_names,
    )
    elo_all_time, sets_all_time = _compute_elo_with_includes(
        all_time_cfg,
        include_event_slugs=include_event_slugs,
        include_tournament_ids=include_tournament_ids,
        include_tournament_names=include_tournament_names,
    )
    print(f"All-time sets in local corpus: {len(sets_all_time)}")
    print(f"All-time players in local corpus: {len(elo_all_time)}")
    _print_top_k(elo_all_time, args.top_k, title=f"TOP {args.top_k} ELO - ALL-TIME LOCAL CORPUS")

    _stage_banner("[STAGE 6] Coverage gaps")
    _print_missing_ranges(
        args.game,
        PROJECT_ROOT / "data" / "tournament_cache.db",
        args.missing_limit,
    )


if __name__ == "__main__":
    main()
