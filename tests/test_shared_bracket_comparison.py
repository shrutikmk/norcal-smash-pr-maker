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
