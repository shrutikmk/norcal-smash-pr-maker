"""Shared-bracket matching for player comparison cards."""

from __future__ import annotations

import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
_DEMO = _ROOT / "demo" / "base_demo"
if str(_DEMO) not in sys.path:
    sys.path.insert(0, str(_DEMO))

from elo_calculator import _expanded_head_to_head  # noqa: E402


def _report(*placements):
    return {
        "in_region_placements": list(placements),
        "out_region_placements": [],
        "all_out_wins": [],
        "all_out_losses": [],
    }


def test_expanded_h2h_matches_same_bracket_only():
    """Players at the same tournament but different brackets should not be compared together."""
    reports = {
        "Twan": _report({
            "tournament_id": "832345",
            "tournament_name": "LVL UP EXPO 2026",
            "event_slug": "t/lvl/singles",
            "placement": 193,
            "wins": 2,
            "losses": 2,
        }),
        "Flow": _report(
            {
                "tournament_id": "832345",
                "tournament_name": "LVL UP EXPO 2026",
                "event_slug": "t/lvl/singles",
                "placement": 17,
                "wins": 8,
                "losses": 4,
            },
            {
                "tournament_id": "832345",
                "tournament_name": "LVL UP EXPO 2026",
                "event_slug": "t/lvl/2v2",
                "placement": 5,
                "wins": 3,
                "losses": 1,
            },
        ),
    }
    expanded = _expanded_head_to_head("Twan", "Flow", [], reports)
    rows = expanded["tournamentsBothAttended"]
    assert len(rows) == 1
    row = rows[0]
    assert row["eventSlug"] == "t/lvl/singles"
    assert row["p1WL"] == "2-2"
    assert row["p2WL"] == "8-4"
    assert row["p1Place"] == 193
    assert row["p2Place"] == 17


def test_expanded_h2h_no_overlap_when_brackets_differ():
    reports = {
        "P1": _report({
            "tournament_id": "832345",
            "tournament_name": "LVL UP EXPO 2026",
            "event_slug": "t/lvl/singles",
            "placement": 32,
            "wins": 4,
            "losses": 2,
        }),
        "P2": _report({
            "tournament_id": "832345",
            "tournament_name": "LVL UP EXPO 2026",
            "event_slug": "t/lvl/2v2",
            "placement": 8,
            "wins": 5,
            "losses": 2,
        }),
    }
    expanded = _expanded_head_to_head("P1", "P2", [], reports)
    assert expanded["tournamentsBothAttended"] == []


def test_expanded_h2h_lists_each_shared_bracket_separately():
    reports = {
        "P1": _report(
            {
                "tournament_id": "t1",
                "tournament_name": "Local Monthly",
                "event_slug": "t/local/singles",
                "placement": 1,
                "wins": 5,
                "losses": 0,
            },
            {
                "tournament_id": "t1",
                "tournament_name": "Local Monthly",
                "event_slug": "t/local/2v2",
                "placement": 4,
                "wins": 2,
                "losses": 2,
            },
        ),
        "P2": _report(
            {
                "tournament_id": "t1",
                "tournament_name": "Local Monthly",
                "event_slug": "t/local/singles",
                "placement": 3,
                "wins": 4,
                "losses": 1,
            },
            {
                "tournament_id": "t1",
                "tournament_name": "Local Monthly",
                "event_slug": "t/local/2v2",
                "placement": 2,
                "wins": 3,
                "losses": 1,
            },
        ),
    }
    expanded = _expanded_head_to_head("P1", "P2", [], reports)
    slugs = {r["eventSlug"] for r in expanded["tournamentsBothAttended"]}
    assert slugs == {"t/local/singles", "t/local/2v2"}
    by_slug = {r["eventSlug"]: r for r in expanded["tournamentsBothAttended"]}
    assert by_slug["t/local/singles"]["p1WL"] == "5-0"
    assert by_slug["t/local/singles"]["p2WL"] == "4-1"
    assert by_slug["t/local/2v2"]["p1WL"] == "2-2"
    assert by_slug["t/local/2v2"]["p2WL"] == "3-1"


def test_expanded_h2h_has_beaten_unified_and_sorted_by_elo():
    """Has Beaten merges shared + unique wins; missing opponent history is null."""
    sets = [
        {"p1": "Lui$", "p2": "HighElo", "p1_score": 3, "p2_score": 1},
        {"p1": "Lui$", "p2": "Shared", "p1_score": 3, "p2_score": 0},
        {"p1": "Kurama", "p2": "Shared", "p1_score": 3, "p2_score": 2},
        {"p1": "Kurama", "p2": "OnlyK", "p1_score": 3, "p2_score": 1},
    ]
    reports = {"Lui$": _report(), "Kurama": _report()}
    elo = {"HighElo": 1800.0, "Shared": 1600.0, "OnlyK": 1500.0}
    expanded = _expanded_head_to_head("Lui$", "Kurama", sets, reports, elo=elo)
    rows = expanded["hasBeaten"]
    assert [r["opponent"] for r in rows] == ["HighElo", "Shared", "OnlyK"]

    by_opp = {r["opponent"]: r for r in rows}
    assert by_opp["HighElo"]["p1Wins"] == 1
    assert by_opp["HighElo"]["p1Losses"] == 0
    assert by_opp["HighElo"]["p2Wins"] is None
    assert by_opp["HighElo"]["p2Losses"] is None

    assert by_opp["Shared"]["p1Wins"] == 1
    assert by_opp["Shared"]["p2Wins"] == 1
    assert by_opp["OnlyK"]["p1Wins"] is None
    assert by_opp["OnlyK"]["p2Wins"] == 1


def test_expanded_h2h_has_lost_to_unified_and_sorted_by_elo():
    """Has Lost To merges shared + unique losses; missing opponent history is null."""
    sets = [
        {"p1": "Lui$", "p2": "Boss", "p1_score": 0, "p2_score": 3},
        {"p1": "Kurama", "p2": "Boss", "p1_score": 1, "p2_score": 3},
        {"p1": "Lui$", "p2": "OnlyLLoss", "p1_score": 1, "p2_score": 3},
        {"p1": "Kurama", "p2": "OnlyKLoss", "p1_score": 0, "p2_score": 3},
    ]
    reports = {"Lui$": _report(), "Kurama": _report()}
    elo = {"Boss": 1900.0, "OnlyLLoss": 1700.0, "OnlyKLoss": 1600.0}
    expanded = _expanded_head_to_head("Lui$", "Kurama", sets, reports, elo=elo)
    rows = expanded["hasLostTo"]
    assert [r["opponent"] for r in rows] == ["Boss", "OnlyLLoss", "OnlyKLoss"]

    by_opp = {r["opponent"]: r for r in rows}
    assert by_opp["Boss"]["p1Losses"] == 1
    assert by_opp["Boss"]["p2Losses"] == 1
    assert by_opp["OnlyLLoss"]["p1Losses"] == 1
    assert by_opp["OnlyLLoss"]["p2Wins"] is None
    assert by_opp["OnlyKLoss"]["p2Losses"] == 1
    assert by_opp["OnlyKLoss"]["p1Wins"] is None


def test_expanded_h2h_oor_has_beaten_and_lost_to():
    """OOR tables use all_out_wins/losses only (not in-region sets)."""
    sets = [
        {"p1": "Lui$", "p2": "LocalOpp", "p1_score": 3, "p2_score": 0},
    ]
    reports = {
        "Lui$": {
            "in_region_placements": [],
            "out_region_placements": [],
            "all_out_wins": ["Samsora", "Neo", "Neo"],
            "all_out_losses": ["Wrath"],
        },
        "Kurama": {
            "in_region_placements": [],
            "out_region_placements": [],
            "all_out_wins": ["Samsora"],
            "all_out_losses": ["Neo", "Wrath"],
        },
    }
    elo = {"Samsora": 2000.0, "Neo": 1800.0, "Wrath": 1900.0, "LocalOpp": 1600.0}
    expanded = _expanded_head_to_head("Lui$", "Kurama", sets, reports, elo=elo)

    beaten = {r["opponent"]: r for r in expanded["hasBeatenOOR"]}
    assert "LocalOpp" not in beaten
    assert [r["opponent"] for r in expanded["hasBeatenOOR"]] == ["Samsora", "Neo"]
    assert beaten["Samsora"]["p1Wins"] == 1
    assert beaten["Samsora"]["p2Wins"] == 1
    assert beaten["Neo"]["p1Wins"] == 2
    assert beaten["Neo"]["p1Losses"] == 0
    # Kurama only lost to Neo OOR → still a record (0-1), not "--"
    assert beaten["Neo"]["p2Wins"] == 0
    assert beaten["Neo"]["p2Losses"] == 1

    lost = {r["opponent"]: r for r in expanded["hasLostToOOR"]}
    assert [r["opponent"] for r in expanded["hasLostToOOR"]] == ["Wrath", "Neo"]
    assert lost["Wrath"]["p1Losses"] == 1
    assert lost["Wrath"]["p2Losses"] == 1
    assert lost["Neo"]["p1Wins"] == 2
    assert lost["Neo"]["p1Losses"] == 0
    assert lost["Neo"]["p2Wins"] == 0
    assert lost["Neo"]["p2Losses"] == 1


def test_expanded_h2h_tournaments_attended_includes_oor():
    """Out-of-region placements appear alongside in-region events."""
    reports = {
        "Lui$": {
            "in_region_placements": [{
                "tournament_id": "1",
                "tournament_name": "Local",
                "event_slug": "t/local/singles",
                "placement": 2,
                "wins": 3,
                "losses": 1,
                "start_at": 100,
            }],
            "out_region_placements": [{
                "tournament_id": "9",
                "tournament_name": "Genesis",
                "event_slug": "t/genesis/singles",
                "placement": 65,
                "wins": 2,
                "losses": 2,
                "start_at": 300,
                "notable_wins": ["BigName"],
                "notable_losses": ["Other"],
            }],
            "all_out_wins": ["BigName"],
            "all_out_losses": ["Other"],
        },
        "Kurama": {
            "in_region_placements": [{
                "tournament_id": "1",
                "tournament_name": "Local",
                "event_slug": "t/local/singles",
                "placement": 1,
                "wins": 4,
                "losses": 0,
                "start_at": 100,
            }],
            "out_region_placements": [],
            "all_out_wins": [],
            "all_out_losses": [],
        },
    }
    expanded = _expanded_head_to_head("Lui$", "Kurama", [], reports)
    slugs = [r["eventSlug"] for r in expanded["tournamentsAttended"]]
    assert slugs == ["t/genesis/singles", "t/local/singles"]
    by_slug = {r["eventSlug"]: r for r in expanded["tournamentsAttended"]}
    assert by_slug["t/genesis/singles"]["p1"]["region"] == "out"
    assert by_slug["t/genesis/singles"]["p2"] is None
    assert by_slug["t/local/singles"]["p1"]["region"] == "in"
    assert by_slug["t/local/singles"]["p2"]["region"] == "in"
    assert by_slug["t/genesis/singles"]["p1"]["sets"][0]["opponent"] == "BigName"


def test_expanded_h2h_tournaments_attended_union_with_runs():
    """Union of brackets either player attended, with scored runs and unique sides."""
    reports = {
        "Lui$": _report(
            {
                "tournament_id": "1",
                "tournament_name": "Shared Local",
                "event_slug": "t/shared/singles",
                "placement": 3,
                "wins": 2,
                "losses": 1,
                "start_at": 200,
            },
            {
                "tournament_id": "2",
                "tournament_name": "Lui$ Only",
                "event_slug": "t/lui/singles",
                "placement": 1,
                "wins": 1,
                "losses": 0,
                "start_at": 100,
            },
        ),
        "Kurama": _report(
            {
                "tournament_id": "1",
                "tournament_name": "Shared Local",
                "event_slug": "t/shared/singles",
                "placement": 1,
                "wins": 3,
                "losses": 0,
                "start_at": 200,
            },
        ),
    }
    sets = [
        {
            "set_id": "s1",
            "event_slug": "t/shared/singles",
            "tournament_id": "1",
            "tournament_name": "Shared Local",
            "start_at": 200,
            "p1": "Lui$",
            "p2": "OppA",
            "p1_score": 3,
            "p2_score": 1,
        },
        {
            "set_id": "s2",
            "event_slug": "t/shared/singles",
            "tournament_id": "1",
            "tournament_name": "Shared Local",
            "start_at": 200,
            "p1": "Kurama",
            "p2": "OppB",
            "p1_score": 3,
            "p2_score": 0,
        },
        {
            "set_id": "s3",
            "event_slug": "t/lui/singles",
            "tournament_id": "2",
            "tournament_name": "Lui$ Only",
            "start_at": 100,
            "p1": "Lui$",
            "p2": "OppC",
            "p1_score": 3,
            "p2_score": 2,
        },
    ]
    expanded = _expanded_head_to_head("Lui$", "Kurama", sets, reports)
    rows = expanded["tournamentsAttended"]
    assert [r["eventSlug"] for r in rows] == ["t/shared/singles", "t/lui/singles"]

    shared = rows[0]
    assert shared["shared"] is True
    assert shared["p1"]["place"] == 3
    assert shared["p2"]["place"] == 1
    assert shared["p1"]["sets"][0]["opponent"] == "OppA"
    assert shared["p1"]["sets"][0]["won"] is True
    assert shared["p1"]["sets"][0]["playerScore"] == 3
    assert shared["p2"]["sets"][0]["opponent"] == "OppB"

    only = rows[1]
    assert only["shared"] is False
    assert only["p1"] is not None
    assert only["p2"] is None
    assert only["p1"]["sets"][0]["opponent"] == "OppC"
    assert len(expanded["tournamentsBothAttended"]) == 1
    assert expanded["tournamentsBothAttended"][0]["eventSlug"] == "t/shared/singles"
