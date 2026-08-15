"""Tournament icon slug normalization and local cache."""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "demo" / "base_demo"))
sys.path.insert(0, str(ROOT / "tools"))

import recent_events as recent_events_tool  # noqa: E402
import web_api  # noqa: E402


def test_tournament_slug_from_event_slug():
    assert recent_events_tool.tournament_slug_from_event_slug(
        "tournament/go-elsewhere-1/event/ultimate-singles"
    ) == "tournament/go-elsewhere-1"
    assert recent_events_tool.tournament_slug_from_event_slug(
        "/tournament/go-elsewhere-1/"
    ) == "tournament/go-elsewhere-1"
    assert recent_events_tool.tournament_slug_from_event_slug("") == ""
    assert recent_events_tool.tournament_slug_from_event_slug(
        "tournament/foo"
    ) == "tournament/foo"


def test_normalize_icon_slugs_dedupes_event_and_tournament():
    slugs = web_api._normalize_tournament_icon_slugs([
        "tournament/go-elsewhere-1/event/ultimate-singles",
        "tournament/go-elsewhere-1",
        "",
        "tournament/other/event/singles",
    ])
    assert slugs == ["tournament/go-elsewhere-1", "tournament/other"]


def test_icon_cache_roundtrip_and_attach(tmp_path, monkeypatch):
    db_path = tmp_path / "tournament_icons.db"
    monkeypatch.setattr(web_api, "_TOURNAMENT_ICON_DB_PATH", db_path)
    monkeypatch.setattr(web_api, "_tournament_icon_schema_ready", False)

    web_api._store_tournament_icon("tournament/go-elsewhere-1", "https://img.example/g.png")
    cached = web_api._cached_tournament_icons(["tournament/go-elsewhere-1", "tournament/missing"])
    assert cached == {"tournament/go-elsewhere-1": "https://img.example/g.png"}

    expanded = {
        "tournamentsAttended": [
            {"eventSlug": "tournament/go-elsewhere-1/event/ultimate-singles", "name": "go elsewhere #1"},
            {"eventSlug": "tournament/missing/event/singles", "name": "Missing"},
        ]
    }
    web_api._attach_cached_tournament_icons(expanded)
    assert expanded["tournamentsAttended"][0]["iconUrl"] == "https://img.example/g.png"
    assert "iconUrl" not in expanded["tournamentsAttended"][1]


def test_resolve_icons_cache_only_leaves_pending(tmp_path, monkeypatch):
    db_path = tmp_path / "tournament_icons.db"
    monkeypatch.setattr(web_api, "_TOURNAMENT_ICON_DB_PATH", db_path)
    monkeypatch.setattr(web_api, "_tournament_icon_schema_ready", False)
    web_api._store_tournament_icon("tournament/cached", "https://img.example/c.png")

    result = web_api._resolve_tournament_icons(
        ["tournament/cached/event/singles", "tournament/fresh/event/singles"],
        fetch_missing=False,
    )
    assert result["icons"] == {"tournament/cached": "https://img.example/c.png"}
    assert result["pending"] == ["tournament/fresh"]
