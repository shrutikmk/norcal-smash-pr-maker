"""
Part 4: Pairwise ranking demo on top of Part 1-3 pipeline outputs.

Builds on:
- demo/tournament_scraper.py (tournament cache)
- demo/tournament_processor.py (processed set cache)
- demo/elo_calculator.py (ELO + live out-of-region reporting + player DB)

Demo behavior:
- Uses date range April 1, 2025 to June 30, 2025 by default
- Randomly selects 5 contenders from top 30 ELO
- Runs pairwise comparisons and prints player cards
- Uses a 50/50 split between:
  - simulated human choice (higher ELO)
  - OpenAI-assisted choice (with explicit caveat for out-of-region uncertainty)
- Stores generated comparison cards in the Part 3 player DB
- Prints final ranking
"""

from __future__ import annotations

import argparse
import itertools
import json
import os
import random
import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

try:
    from dotenv import load_dotenv

    # Project root .env (file lives in demo/base_demo/)
    load_dotenv(Path(__file__).resolve().parent.parent.parent / ".env")
except Exception:
    pass

try:
    from openai import OpenAI
except Exception:  # pragma: no cover - optional dependency at runtime
    OpenAI = None  # type: ignore[assignment]

from elo_calculator import (
    DEFAULT_PLAYER_DB,
    DEFAULT_PROCESSED_CACHE,
    DEFAULT_TOURNAMENT_CACHE,
    EloConfig,
    StartGGClient,
    _build_identity_map_live,
    _build_player_opponent_records,
    _compute_h2h,
    _get_live_player_report,
    _h2h_record,
    _in_region_tournament_ids,
    _init_player_db,
    _upsert_live_player_report,
    compute_elo,
)


@dataclass
class RankingConfig:
    tournament_cache_path: Path = DEFAULT_TOURNAMENT_CACHE
    processed_cache_path: Path = DEFAULT_PROCESSED_CACHE
    player_db_path: Path = DEFAULT_PLAYER_DB
    start_date: str = "2025-04-01"
    end_date: str = "2025-06-30"
    contenders_from_top_n: int = 30
    contenders_count: int = 5
    random_seed: int | None = None
    openai_model: str = "gpt-4o-mini"
    interactive: bool = False
    verbose: bool = True


def _init_cards_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS pairwise_cards (
            card_id INTEGER PRIMARY KEY AUTOINCREMENT,
            created_at INTEGER NOT NULL,
            date_start TEXT NOT NULL,
            date_end TEXT NOT NULL,
            player_a TEXT NOT NULL,
            player_b TEXT NOT NULL,
            decision_source TEXT NOT NULL,
            chosen_player TEXT NOT NULL,
            card_json TEXT NOT NULL
        )
        """
    )
    conn.commit()


def _loss_to_tournament_ratio(report: dict[str, Any]) -> float:
    losses = int(report.get("in_region_losses", 0)) + int(report.get("out_region_losses", 0))
    tournaments = int(report.get("in_region_tournaments", 0)) + int(report.get("out_region_tournaments", 0))
    return losses / tournaments if tournaments > 0 else float("inf")


def _top_opponents(records: dict[str, dict[str, int]], *, kind: str, limit: int = 12) -> list[tuple[str, int]]:
    values: list[tuple[str, int]] = []
    for opp, rec in records.items():
        count = int(rec.get(kind, 0))
        if count > 0:
            values.append((opp, count))
    values.sort(key=lambda x: x[1], reverse=True)
    return values[:limit]


def _shared_and_unique(
    left: list[tuple[str, int]],
    right: list[tuple[str, int]],
) -> tuple[list[tuple[str, int]], list[tuple[str, int]], list[tuple[str, int]]]:
    left_map = dict(left)
    right_map = dict(right)
    shared = sorted(
        [(name, left_map[name] + right_map[name]) for name in left_map.keys() & right_map.keys()],
        key=lambda x: x[1],
        reverse=True,
    )
    unique_left = sorted([(n, c) for n, c in left if n not in right_map], key=lambda x: x[1], reverse=True)
    unique_right = sorted([(n, c) for n, c in right if n not in left_map], key=lambda x: x[1], reverse=True)
    return shared, unique_left, unique_right


def _build_player_card(
    *,
    p1: str,
    p2: str,
    elo: dict[str, float],
    in_region_sets: list[dict[str, Any]],
    reports: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    r1 = reports[p1]
    r2 = reports[p2]
    rec1 = _build_player_opponent_records(p1, in_region_sets, r1)
    rec2 = _build_player_opponent_records(p2, in_region_sets, r2)

    wins1 = _top_opponents(rec1, kind="wins")
    wins2 = _top_opponents(rec2, kind="wins")
    losses1 = _top_opponents(rec1, kind="losses")
    losses2 = _top_opponents(rec2, kind="losses")
    shared_wins, unique_wins_1, unique_wins_2 = _shared_and_unique(wins1, wins2)
    shared_losses, unique_losses_1, unique_losses_2 = _shared_and_unique(losses1, losses2)

    h2h = _compute_h2h(in_region_sets)
    p1_h2h_wins, p2_h2h_wins = _h2h_record(h2h, p1, p2)

    card = {
        "players": [p1, p2],
        "elo": {p1: round(float(elo.get(p1, 1500.0)), 2), p2: round(float(elo.get(p2, 1500.0)), 2)},
        "head_to_head_in_region": {p1: p1_h2h_wins, p2: p2_h2h_wins},
        "shared_wins": shared_wins[:10],
        "unique_wins": {p1: unique_wins_1[:10], p2: unique_wins_2[:10]},
        "shared_losses": shared_losses[:10],
        "unique_losses": {p1: unique_losses_1[:10], p2: unique_losses_2[:10]},
        "loss_to_tournament_ratio": {
            p1: round(_loss_to_tournament_ratio(r1), 4),
            p2: round(_loss_to_tournament_ratio(r2), 4),
        },
        "in_region_summary": {
            p1: {
                "tournaments": int(r1.get("in_region_tournaments", 0)),
                "wins": int(r1.get("in_region_wins", 0)),
                "losses": int(r1.get("in_region_losses", 0)),
            },
            p2: {
                "tournaments": int(r2.get("in_region_tournaments", 0)),
                "wins": int(r2.get("in_region_wins", 0)),
                "losses": int(r2.get("in_region_losses", 0)),
            },
        },
        "out_region_summary": {
            p1: {
                "tournaments": int(r1.get("out_region_tournaments", 0)),
                "wins": int(r1.get("out_region_wins", 0)),
                "losses": int(r1.get("out_region_losses", 0)),
                "notable_wins": r1.get("notable_out_wins", [])[:10],
                "notable_losses": r1.get("notable_out_losses", [])[:10],
            },
            p2: {
                "tournaments": int(r2.get("out_region_tournaments", 0)),
                "wins": int(r2.get("out_region_wins", 0)),
                "losses": int(r2.get("out_region_losses", 0)),
                "notable_wins": r2.get("notable_out_wins", [])[:10],
                "notable_losses": r2.get("notable_out_losses", [])[:10],
            },
        },
    }
    return card


def _print_card(card: dict[str, Any]) -> None:
    p1, p2 = card["players"]
    print("-" * 72)
    print(f"[PLAYER CARD] {p1} vs {p2}")
    print("-" * 72)
    print(f"ELO: {p1}={card['elo'][p1]} | {p2}={card['elo'][p2]}")
    print(f"In-region H2H: {p1} {card['head_to_head_in_region'][p1]} - {card['head_to_head_in_region'][p2]} {p2}")
    print(
        f"Loss/Tournament ratio: {p1}={card['loss_to_tournament_ratio'][p1]} | "
        f"{p2}={card['loss_to_tournament_ratio'][p2]}"
    )
    print(
        f"In-region W-L/T: {p1} {card['in_region_summary'][p1]['wins']}-{card['in_region_summary'][p1]['losses']}"
        f"/{card['in_region_summary'][p1]['tournaments']} | "
        f"{p2} {card['in_region_summary'][p2]['wins']}-{card['in_region_summary'][p2]['losses']}"
        f"/{card['in_region_summary'][p2]['tournaments']}"
    )
    print(
        f"Out-region W-L/T: {p1} {card['out_region_summary'][p1]['wins']}-{card['out_region_summary'][p1]['losses']}"
        f"/{card['out_region_summary'][p1]['tournaments']} | "
        f"{p2} {card['out_region_summary'][p2]['wins']}-{card['out_region_summary'][p2]['losses']}"
        f"/{card['out_region_summary'][p2]['tournaments']}"
    )
    print(f"Shared wins (sample): {card['shared_wins'][:5]}")
    print(f"Shared losses (sample): {card['shared_losses'][:5]}")
    print(f"{p1} unique wins (sample): {card['unique_wins'][p1][:5]}")
    print(f"{p2} unique wins (sample): {card['unique_wins'][p2][:5]}")
    print(f"{p1} unique losses (sample): {card['unique_losses'][p1][:5]}")
    print(f"{p2} unique losses (sample): {card['unique_losses'][p2][:5]}")
    print(f"{p1} notable OOR wins: {card['out_region_summary'][p1]['notable_wins'][:5]}")
    print(f"{p2} notable OOR wins: {card['out_region_summary'][p2]['notable_wins'][:5]}")


def _build_ai_justification_prompt(card: dict[str, Any]) -> str:
    p1, p2 = card["players"]
    return f"""
You are helping with a Smash Ultimate regional ranking.

Important caveat:
- You may be missing out-of-region context.
- You should explicitly acknowledge uncertainty when relevant.

Task:
- Make arguments for BOTH players.
- Then choose one player as better ranked for this specific pairwise comparison.

Decision guidance:
- Favor higher quality wins over quantity of low-quality wins.
- Penalize bad losses.
- Consider in-region head-to-head record.
- Use loss-to-tournament ratio as a consistency signal.
- Include out-of-region summary and notable wins/losses, while acknowledging incompleteness risk.

Comparison card:
{json.dumps(card, indent=2)}

Return format:
1) "Arguments for {p1}: ..."
2) "Arguments for {p2}: ..."
3) "Caveat: ..."
4) "Decision: <exactly one of: {p1} or {p2}>"
""".strip()


def _build_ai_parse_prompt(card: dict[str, Any], justification: str) -> str:
    p1, p2 = card["players"]
    return f"""
Given the ranking card and analyst text below, return a strict JSON object:
{{
  "decision": "{p1}" | "{p2}",
  "confidence": "low" | "medium" | "high",
  "rationale_short": "<max 2 sentences>"
}}

Card:
{json.dumps(card, indent=2)}

Analyst text:
{justification}

Rules:
- The "decision" must be exactly one of {p1} or {p2}.
- Output ONLY JSON.
""".strip()


def _openai_assisted_decision(
    *,
    card: dict[str, Any],
    model: str,
    client: Any,
) -> tuple[str, str, dict[str, Any]]:
    p1, p2 = card["players"]

    justification_prompt = _build_ai_justification_prompt(card)
    justification_resp = client.chat.completions.create(
        model=model,
        messages=[
            {"role": "system", "content": "You are a careful esports ranking analyst."},
            {"role": "user", "content": justification_prompt},
        ],
    )
    justification = (justification_resp.choices[0].message.content or "").strip()

    parse_prompt = _build_ai_parse_prompt(card, justification)
    parse_resp = client.chat.completions.create(
        model=model,
        messages=[
            {"role": "system", "content": "Return strict JSON only."},
            {"role": "user", "content": parse_prompt},
        ],
    )
    raw_json = (parse_resp.choices[0].message.content or "").strip()
    parsed: dict[str, Any] = {}
    try:
        parsed = json.loads(raw_json)
    except Exception:
        parsed = {"decision": p1, "confidence": "low", "rationale_short": "Failed to parse JSON; fallback used."}

    decision = str(parsed.get("decision", ""))
    if decision not in (p1, p2):
        decision = p1 if card["elo"][p1] >= card["elo"][p2] else p2
        parsed["decision"] = decision
        parsed["confidence"] = "low"
        parsed["rationale_short"] = "Invalid decision in parse step; fell back to higher ELO."

    return decision, justification, parsed


def _openai_unavailable_reason() -> str:
    """Human-readable explanation for why OpenAI path is unavailable."""
    if OpenAI is None:
        return (
            "OpenAI Python client is not importable (package missing or broken install). "
            "Install/repair with: pip install openai"
        )
    key = (os.environ.get("OPENAI_API_KEY") or "").strip()
    if not key:
        return "OPENAI_API_KEY is missing/empty in environment or .env."
    return "OpenAI client initialized, but API request failed (auth/model/network/runtime)."


def _store_card(
    conn: sqlite3.Connection,
    *,
    cfg: RankingConfig,
    card: dict[str, Any],
    decision_source: str,
    chosen_player: str,
    extra: dict[str, Any] | None = None,
) -> None:
    payload = {"card": card, "extra": extra or {}}
    p1, p2 = card["players"]
    conn.execute(
        """
        INSERT INTO pairwise_cards
        (created_at, date_start, date_end, player_a, player_b, decision_source, chosen_player, card_json)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            int(time.time()),
            cfg.start_date,
            cfg.end_date,
            p1,
            p2,
            decision_source,
            chosen_player,
            json.dumps(payload),
        ),
    )
    conn.commit()


def _load_live_reports_for_contenders(
    *,
    contenders: list[str],
    elo_cfg: EloConfig,
    in_region_sets: list[dict[str, Any]],
    elo: dict[str, float],
    verbose: bool,
) -> tuple[dict[str, dict[str, Any]], sqlite3.Connection]:
    token = os.environ.get("STARTGG_API_KEY", "")
    if not token:
        raise ValueError("STARTGG_API_KEY is required in env/.env")

    client = StartGGClient(token)
    pdb = _init_player_db(elo_cfg.player_db_path)
    identity = _build_identity_map_live(client, elo_cfg, set(contenders), pdb, verbose=verbose)
    in_region_ids = _in_region_tournament_ids(elo_cfg.tournament_cache_path)

    reports: dict[str, dict[str, Any]] = {}
    for player in contenders:
        ident = identity.get(player)
        if not ident or not str(ident.get("player_id") or ""):
            if verbose:
                print(
                    f"[LIVE] Could not resolve user/player IDs for {player}; skipping out-of-region fetch. "
                    "Likely causes: (1) player name in local DB includes sponsor/prefix variation that does not "
                    "exactly match start.gg participant tags, (2) this player was not found in scanned in-region "
                    "events during identity bootstrap, or (3) start.gg response omitted user linkage for those entrants. "
                    "This run will still compute in-region ELO, but out-of-region stats for this player are unavailable."
                )
            reports[player] = {
                "canonical_name": player,
                "in_region_tournaments": 0,
                "in_region_wins": 0,
                "in_region_losses": 0,
                "out_region_tournaments": 0,
                "out_region_wins": 0,
                "out_region_losses": 0,
                "notable_out_wins": [],
                "notable_out_losses": [],
                "all_out_wins": [],
                "all_out_losses": [],
            }
            continue

        uid = str(ident.get("user_id") or "")
        pid = str(ident.get("player_id") or "")
        if verbose:
            if uid:
                print(f"[LIVE] Fetching out-of-region stats for {player} ...")
            else:
                print(
                    f"[LIVE] Fetching out-of-region stats for {player} with player_id only "
                    "(user_id unavailable from entrants payload)."
                )
        report = _get_live_player_report(
            client=client,
            config=elo_cfg,
            canonical_name=player,
            user_id=uid,
            player_id=pid,
            in_region_sets=in_region_sets,
            in_region_tournament_ids=in_region_ids,
            verbose=verbose,
        )
        reports[player] = report
        _upsert_live_player_report(pdb, report, elo.get(player, elo_cfg.initial_elo))

    return reports, pdb


def run_ranking_demo(cfg: RankingConfig) -> list[tuple[str, int]]:
    if cfg.random_seed is not None:
        random.seed(cfg.random_seed)

    elo_cfg = EloConfig(
        tournament_cache_path=cfg.tournament_cache_path,
        processed_cache_path=cfg.processed_cache_path,
        player_db_path=cfg.player_db_path,
        start_date=cfg.start_date,
        end_date=cfg.end_date,
    )

    print("=" * 72)
    print("[DEMO] Part 4 - Pairwise Ranking")
    print("=" * 72)
    print(f"Date range: {cfg.start_date} to {cfg.end_date}")
    print("Using DB artifacts from Part 1-3 pipeline.")

    elo, in_region_sets = compute_elo(elo_cfg)
    top_pool = list(elo.items())[: cfg.contenders_from_top_n]
    if len(top_pool) < cfg.contenders_count:
        raise RuntimeError(
            f"Not enough players to sample {cfg.contenders_count} contenders from top {cfg.contenders_from_top_n}. "
            f"Available: {len(top_pool)}"
        )

    contenders = [name for name, _ in random.sample(top_pool, cfg.contenders_count)]
    print(f"\n[SELECTION] Random contenders ({cfg.contenders_count}) from top {cfg.contenders_from_top_n} ELO:")
    for i, player in enumerate(contenders, 1):
        print(f"  {i}. {player} ({elo[player]:.2f})")
    print("(Selection is random each run by design.)")

    reports, pdb = _load_live_reports_for_contenders(
        contenders=contenders,
        elo_cfg=elo_cfg,
        in_region_sets=in_region_sets,
        elo=elo,
        verbose=cfg.verbose,
    )
    _init_cards_table(pdb)

    openai_client = None
    openai_status_reason = _openai_unavailable_reason()
    if OpenAI is not None and os.environ.get("OPENAI_API_KEY"):
        try:
            openai_client = OpenAI(api_key=os.environ.get("OPENAI_API_KEY"))
            openai_status_reason = "OpenAI client is available."
        except Exception as e:
            openai_client = None
            openai_status_reason = f"OpenAI client init failed: {type(e).__name__}: {e}"

    all_pairs = list(itertools.combinations(contenders, 2))
    random.shuffle(all_pairs)
    ai_pair_count = len(all_pairs) // 2
    ai_indices = set(random.sample(range(len(all_pairs)), ai_pair_count))
    scores = {p: 0 for p in contenders}

    print(f"\n[PAIRWISE] Total comparisons: {len(all_pairs)}")
    print(
        f"Decision split target: {len(all_pairs) - ai_pair_count} simulated-human "
        f"+ {ai_pair_count} OpenAI-assisted"
    )

    for idx, (p1, p2) in enumerate(all_pairs):
        print(f"\n[QUESTION {idx + 1}/{len(all_pairs)}] Who should rank higher: {p1} or {p2}?")
        card = _build_player_card(p1=p1, p2=p2, elo=elo, in_region_sets=in_region_sets, reports=reports)
        _print_card(card)

        use_ai = idx in ai_indices
        if cfg.interactive:
            raw = input("Use AI assistant for this comparison? [y/N]: ").strip().lower()
            use_ai = raw in {"y", "yes"}

        if use_ai:
            if openai_client is None:
                print(
                    "[AI] OpenAI unavailable; falling back to simulated-human decision. "
                    f"Reason: {openai_status_reason}"
                )
                use_ai = False
            else:
                print("[AI] User requested OpenAI help. Generating arguments for both players...")
                try:
                    decision, justification, parsed = _openai_assisted_decision(
                        card=card,
                        model=cfg.openai_model,
                        client=openai_client,
                    )
                except Exception as e:
                    print(
                        "[AI] OpenAI call failed; falling back to simulated-human decision. "
                        f"Error: {type(e).__name__}: {e}"
                    )
                    chosen = p1 if elo[p1] >= elo[p2] else p2
                    print(
                        "[DECISION] Simulated human decision after AI failure: choosing higher ELO "
                        f"({chosen})."
                    )
                    scores[chosen] += 1
                    _store_card(
                        pdb,
                        cfg=cfg,
                        card=card,
                        decision_source="simulated_human_after_ai_error",
                        chosen_player=chosen,
                        extra={"rule": "higher_elo", "ai_error": f"{type(e).__name__}: {e}"},
                    )
                    continue
                print("[AI] Justification:")
                print(justification)
                print(f"[AI] Parsed decision payload: {parsed}")

                go_with_ai = True
                if cfg.interactive:
                    accept_raw = input(f"Go with AI decision ({decision})? [Y/n]: ").strip().lower()
                    go_with_ai = accept_raw not in {"n", "no"}
                if go_with_ai:
                    chosen = decision
                    source = "openai"
                    print(f"[DECISION] Going with AI decision: {chosen}")
                else:
                    chosen = p1 if elo[p1] >= elo[p2] else p2
                    source = "human_override"
                    print(f"[DECISION] User override applied: {chosen}")

                scores[chosen] += 1
                _store_card(
                    pdb,
                    cfg=cfg,
                    card=card,
                    decision_source=source,
                    chosen_player=chosen,
                    extra={"justification": justification, "parsed": parsed},
                )
                continue

        # Simulated human path (explicitly requested for ~50% of comparisons).
        chosen = p1 if elo[p1] >= elo[p2] else p2
        print(
            "[DECISION] Simulated human decision: choosing higher ELO "
            f"({chosen}) to mimic a fast manual judgment."
        )
        scores[chosen] += 1
        _store_card(
            pdb,
            cfg=cfg,
            card=card,
            decision_source="simulated_human",
            chosen_player=chosen,
            extra={"rule": "higher_elo"},
        )

    pdb.close()

    final = sorted(contenders, key=lambda p: (scores[p], elo[p]), reverse=True)
    print("\n" + "=" * 72)
    print("[FINAL RANKING]")
    print("=" * 72)
    for i, player in enumerate(final, 1):
        print(f"{i}. {player} | pairwise_wins={scores[player]} | elo={elo[player]:.2f}")

    return [(p, scores[p]) for p in final]


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Part 4 ranking demo: random contenders, player cards, pairwise ranking, optional OpenAI."
    )
    parser.add_argument("--start-date", default="2025-04-01")
    parser.add_argument("--end-date", default="2025-06-30")
    parser.add_argument("--top-n", type=int, default=30, help="Sample contenders from top N ELO players.")
    parser.add_argument("--contenders", type=int, default=5, help="How many contenders to rank.")
    parser.add_argument("--seed", type=int, default=None, help="Optional random seed for reproducibility.")
    parser.add_argument("--openai-model", default="gpt-4o-mini")
    parser.add_argument("--interactive", action="store_true", help="Ask at each comparison whether to use AI.")
    parser.add_argument("--quiet", action="store_true")
    args = parser.parse_args()

    cfg = RankingConfig(
        start_date=args.start_date,
        end_date=args.end_date,
        contenders_from_top_n=args.top_n,
        contenders_count=args.contenders,
        random_seed=args.seed,
        openai_model=args.openai_model,
        interactive=args.interactive,
        verbose=not args.quiet,
    )
    run_ranking_demo(cfg)


if __name__ == "__main__":
    main()
