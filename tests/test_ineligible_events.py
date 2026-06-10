"""Tests for manual ineligible-event flags on the PR Maker event selector."""

from __future__ import annotations

import json
import sqlite3
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "demo" / "base_demo"))
sys.path.insert(0, str(ROOT / "tools"))

import tournament_scraper as ts  # noqa: E402
import web_api  # noqa: E402

GAME = "Super Smash Bros. Ultimate"
SLUG_A = "tournament/a/event/singles"
SLUG_B = "tournament/b/event/singles"


def _raw_json(name: str, event_name: str = "Singles") -> str:
    return json.dumps({
        "tournament": {"id": "t1", "name": name},
        "event": {"name": event_name},
    })


def _make_cache(tmp_path: Path) -> Path:
    cache = tmp_path / "tournament_cache.db"
    conn = sqlite3.connect(cache)
    ts._init_cache(conn)
    rows = [
        ("t1", SLUG_A, "Weekly A", "San Jose", "a", 1700000000,
         32, GAME, _raw_json("Weekly A"), 1700000001, 1700000001),
        ("t2", SLUG_B, "Weekly B", "Sacramento", "b", 1700600000,
         48, GAME, _raw_json("Weekly B"), 1700600001, 1700600001),
    ]
    conn.executemany(
        "INSERT OR REPLACE INTO tournaments "
        "(tournament_id, event_slug, name, city, slug, start_at, event_num_entrants, "
        " videogame_name, raw_json, cached_at, updated_at) VALUES (?,?,?,?,?,?,?,?,?,?,?)",
        rows,
    )
    conn.commit()
    conn.close()
    return cache


@pytest.fixture
def cache_env(tmp_path, monkeypatch):
    cache = _make_cache(tmp_path)
    monkeypatch.setattr(web_api._ts_mod, "_default_cache_path", lambda: cache)
    return cache


def test_set_and_get_ineligible_event_slugs(tmp_path):
    cache = _make_cache(tmp_path)
    conn = sqlite3.connect(cache)
    ts._ensure_ineligible_events_table(conn)
    assert ts.get_ineligible_event_slugs(conn) == set()

    ts.set_event_ineligible(conn, SLUG_A, True)
    conn.commit()
    assert ts.get_ineligible_event_slugs(conn) == {SLUG_A}

    ts.set_event_ineligible(conn, SLUG_A, False)
    conn.commit()
    assert ts.get_ineligible_event_slugs(conn) == set()
    conn.close()


def test_list_cached_events_includes_is_ineligible(cache_env):
    events = web_api._list_cached_events_for_range("2023-11-01", "2023-12-31")
    by_slug = {ev["eventSlug"]: ev for ev in events}
    assert by_slug[SLUG_A]["isIneligible"] is False
    assert by_slug[SLUG_B]["isIneligible"] is False

    conn = sqlite3.connect(cache_env)
    ts.set_event_ineligible(conn, SLUG_B, True)
    conn.commit()
    conn.close()

    events = web_api._list_cached_events_for_range("2023-11-01", "2023-12-31")
    by_slug = {ev["eventSlug"]: ev for ev in events}
    assert by_slug[SLUG_A]["isIneligible"] is False
    assert by_slug[SLUG_B]["isIneligible"] is True


def test_set_cached_event_ineligible_persists(cache_env):
    result = web_api._set_cached_event_ineligible(SLUG_A, True)
    assert result == {"eventSlug": SLUG_A, "ineligible": True}

    events = web_api._list_cached_events_for_range("2023-11-01", "2023-12-31")
    flagged = [ev for ev in events if ev["isIneligible"]]
    assert len(flagged) == 1
    assert flagged[0]["eventSlug"] == SLUG_A

    web_api._set_cached_event_ineligible(SLUG_A, False)
    events = web_api._list_cached_events_for_range("2023-11-01", "2023-12-31")
    assert all(not ev["isIneligible"] for ev in events)


def test_set_cached_event_ineligible_rejects_unknown_slug(cache_env):
    with pytest.raises(ValueError, match="not found in cache"):
        web_api._set_cached_event_ineligible("tournament/missing/event/singles", True)
