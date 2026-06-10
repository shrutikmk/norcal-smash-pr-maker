"""
Tests for the local-computation performance work (no live start.gg needed).

Covers:
- batched tournament-metadata prefetch in _load_in_region_sets (N+1 removal)
- explicit event-slug selection in the processor (PR Maker fast path)
- single-pass Copeland scoring matching the brute-force reference
- web API derived-data memoization + stamp invalidation
- sync scraper complexity-limit fallback (perPage halving)
"""

from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "demo" / "base_demo"))
sys.path.insert(0, str(ROOT / "tools"))

import elo_calculator as ec  # noqa: E402
import tournament_processor as tp  # noqa: E402
import tournament_scraper as ts  # noqa: E402
import web_api  # noqa: E402


# --- fixtures ---------------------------------------------------------------


def _make_caches(tmp_path):
    """Tiny tournament + processed caches with two events and four sets."""
    tcache = tmp_path / "tournament_cache.db"
    pcache = tmp_path / "processed_tournament.db"

    tconn = sqlite3.connect(tcache)
    ts._init_cache(tconn)
    rows = [
        ("t1", "tournament/a/event/singles", "Weekly A", "San Jose", "a", 1700000000,
         32, "Super Smash Bros. Ultimate", "{}", 1700000001, 1700000001),
        ("t2", "tournament/b/event/singles", "Weekly B", "Sacramento", "b", 1700600000,
         48, "Super Smash Bros. Ultimate", "{}", 1700600001, 1700600001),
    ]
    tconn.executemany(
        "INSERT OR REPLACE INTO tournaments "
        "(tournament_id, event_slug, name, city, slug, start_at, event_num_entrants, "
        " videogame_name, raw_json, cached_at, updated_at) VALUES (?,?,?,?,?,?,?,?,?,?,?)",
        rows,
    )
    tconn.commit()
    tconn.close()

    pconn = sqlite3.connect(pcache)
    tp._init_processed_db(pconn)
    pconn.executemany(
        "INSERT INTO processed_events (event_slug, event_id, tournament_id, event_name, processed_at, tournament_updated_at) "
        "VALUES (?,?,?,?,?,?)",
        [
            ("tournament/a/event/singles", "e1", "t1", "Singles", 1700000100, 1700000001),
            ("tournament/b/event/singles", "e2", "t2", "Singles", 1700600100, 1700600001),
        ],
    )
    pconn.executemany(
        "INSERT INTO sets_cache (set_id, event_id, event_slug, p1_name, p2_name, p1_score, p2_score, cached_at) "
        "VALUES (?,?,?,?,?,?,?,?)",
        [
            ("s1", "e1", "tournament/a/event/singles", "Alice", "Bob", 3, 1, 1700000100),
            ("s2", "e1", "tournament/a/event/singles", "Alice", "Cara", 3, 2, 1700000100),
            ("s3", "e2", "tournament/b/event/singles", "Bob", "Cara", 0, 3, 1700600100),
            ("s4", "e2", "tournament/b/event/singles", "Alice", "Bob", 1, 3, 1700600100),
        ],
    )
    pconn.commit()
    pconn.close()
    return tcache, pcache


# --- _load_in_region_sets ----------------------------------------------------


def test_load_in_region_sets_prefetches_tournament_metadata(tmp_path):
    tcache, pcache = _make_caches(tmp_path)
    cfg = ec.EloConfig(
        tournament_cache_path=tcache,
        processed_cache_path=pcache,
        start_date=None,
        end_date=None,
    )
    sets = ec._load_in_region_sets(cfg)
    assert len(sets) == 4
    by_id = {s["set_id"]: s for s in sets}
    assert by_id["s1"]["tournament_name"] == "Weekly A"
    assert by_id["s1"]["start_at"] == 1700000000
    assert by_id["s3"]["tournament_name"] == "Weekly B"
    # Date filtering still applies via the prefetched start_at values.
    cfg2 = ec.EloConfig(
        tournament_cache_path=tcache,
        processed_cache_path=pcache,
        start_date="2023-11-20",  # after Weekly A's start_at
        end_date="2024-01-01",
    )
    sets2 = ec._load_in_region_sets(cfg2)
    assert {s["event_slug"] for s in sets2} == {"tournament/b/event/singles"}


# --- include_event_slugs (PR Maker fast path) --------------------------------


def test_load_events_with_explicit_slugs(tmp_path):
    tcache, _ = _make_caches(tmp_path)
    conn = sqlite3.connect(tcache)
    cfg = tp.ProcessorConfig(
        tournament_cache_path=tcache,
        include_event_slugs=["tournament/b/event/singles", "missing/slug"],
    )
    events = tp._load_events_from_tournament_cache(conn, cfg)
    conn.close()
    assert [e[0] for e in events] == ["tournament/b/event/singles"]
    assert events[0][1] == "t2"


def test_load_events_date_filter_unaffected(tmp_path):
    tcache, _ = _make_caches(tmp_path)
    conn = sqlite3.connect(tcache)
    cfg = tp.ProcessorConfig(
        tournament_cache_path=tcache,
        start_date="2023-11-01",
        end_date="2023-12-31",
    )
    events = tp._load_events_from_tournament_cache(conn, cfg)
    conn.close()
    assert {e[0] for e in events} == {
        "tournament/a/event/singles", "tournament/b/event/singles",
    }


# --- Copeland single pass -----------------------------------------------------


def _brute_force_copeland(names, sets):
    """Reference implementation (the old O(n^2 * sets) version)."""
    pool = set(names)
    scores = {n: 0.0 for n in names}
    ordered = list(names)
    for i in range(len(ordered)):
        a = ordered[i]
        for j in range(i + 1, len(ordered)):
            b = ordered[j]
            aw, bw = 0, 0
            for s in sets:
                p1, p2 = s.get("p1"), s.get("p2")
                if p1 not in pool or p2 not in pool or {p1, p2} != {a, b}:
                    continue
                s1, s2 = int(s.get("p1_score", 0) or 0), int(s.get("p2_score", 0) or 0)
                if s1 > s2:
                    aw, bw = (aw + 1, bw) if p1 == a else (aw, bw + 1)
                elif s2 > s1:
                    aw, bw = (aw + 1, bw) if p2 == a else (aw, bw + 1)
            if aw > bw:
                scores[a] += 1.0
            elif bw > aw:
                scores[b] += 1.0
            elif aw > 0 and aw == bw:
                scores[a] += 0.5
                scores[b] += 0.5
    return scores


def test_pool_copeland_matches_brute_force():
    names = ["Alice", "Bob", "Cara", "Dan"]
    sets = [
        {"p1": "Alice", "p2": "Bob", "p1_score": 3, "p2_score": 1},
        {"p1": "Bob", "p2": "Alice", "p1_score": 3, "p2_score": 2},
        {"p1": "Alice", "p2": "Bob", "p1_score": 3, "p2_score": 0},   # Alice 2-1 Bob
        {"p1": "Cara", "p2": "Bob", "p1_score": 3, "p2_score": 2},
        {"p1": "Bob", "p2": "Cara", "p1_score": 3, "p2_score": 1},    # 1-1 tie
        {"p1": "Dan", "p2": "Alice", "p1_score": 0, "p2_score": 3},
        {"p1": "Eve", "p2": "Alice", "p1_score": 3, "p2_score": 0},   # outside pool
        {"p1": "Alice", "p2": "Alice", "p1_score": 3, "p2_score": 0},  # degenerate
        {"p1": "Cara", "p2": "Dan", "p1_score": 2, "p2_score": 2},    # draw set ignored
    ]
    assert web_api._pool_copeland_scores(names, sets) == _brute_force_copeland(names, sets)


# --- memoization ---------------------------------------------------------------


def test_memoized_returns_cached_until_stamp_changes():
    web_api._MEMO.clear()
    calls = {"n": 0}

    def compute():
        calls["n"] += 1
        return calls["n"]

    assert web_api._memoized(("k",), compute, stamp=(1,)) == 1
    assert web_api._memoized(("k",), compute, stamp=(1,)) == 1   # cache hit
    assert calls["n"] == 1
    assert web_api._memoized(("k",), compute, stamp=(2,)) == 2   # stamp changed
    assert calls["n"] == 2


def test_memoized_evicts_oldest():
    web_api._MEMO.clear()
    for i in range(web_api._MEMO_MAX_ENTRIES + 4):
        web_api._memoized(("k", i), lambda i=i: i, stamp=(0,))
    assert len(web_api._MEMO) <= web_api._MEMO_MAX_ENTRIES


# --- sync scraper complexity fallback ------------------------------------------


def test_fetch_all_tournaments_halves_per_page_on_complexity(monkeypatch):
    calls = []

    def fake_fetch_page(client, limiter, query, page, coords, radius, per_page, token, **kw):
        calls.append((page, per_page))
        if per_page > 25:
            raise ts.ComplexityLimitErrorSync("Query complexity exceeds 1000 objects")
        # one short page ends pagination
        return [{"id": f"t-{page}", "name": "X", "events": []}]

    monkeypatch.setattr(ts, "_fetch_page", fake_fetch_page)
    cfg = ts.ScraperConfig(
        start_date="2024-01-01", end_date="2024-01-31",
        regions=["bay"], per_page=100,
    )
    out = list(ts._fetch_all_tournaments(object(), object(), cfg, "tok"))
    assert out  # got nodes after fallback
    # First attempt at the capped perPage (100), then halved until <= 25.
    tried = [pp for (_pg, pp) in calls]
    assert tried[0] == 100
    assert any(pp <= 25 for pp in tried)
    assert min(tried) >= 8


# --- force_refresh must re-insert unchanged tournaments -------------------------


def test_force_refresh_reinserts_after_elo_only_delete(tmp_path, monkeypatch):
    """PR Maker Fresh Scrape deletes ELO-eligible rows then force_refresh'es.

    Unchanged tournaments must still be re-written or their qualifying event
    rows vanish (Guildhouse singles-with-42-entrants bug).
    """
    cache = tmp_path / "tournament_cache.db"
    after, before = 1_700_000_000, 1_800_000_000
    tournament = {
        "id": "gh217",
        "name": "Guildhouse Weekly 217",
        "city": "San Jose",
        "slug": "guildhouse-weekly-217",
        "startAt": after + 1000,
        "updatedAt": 1_700_000_100,
        "events": [
            {
                "slug": "tournament/gh217/event/smash-ultimate-singles-switch-7-30-pm",
                "numEntrants": 42,
                "videogame": {"name": "Super Smash Bros. Ultimate"},
            },
            {
                "slug": "tournament/gh217/event/smash-ultimate-redemption-ladder",
                "numEntrants": 6,
                "videogame": {"name": "Super Smash Bros. Ultimate"},
            },
        ],
    }

    def fake_stream(*_a, **_kw):
        yield tournament

    monkeypatch.setattr(ts, "_fetch_all_tournaments", lambda *a, **kw: fake_stream())

    cfg = ts.ScraperConfig(
        start_date="2023-11-14",
        end_date="2024-01-01",
        cache_path=cache,
        regions=["bay"],
        use_async=False,
        incremental=False,
        force_refresh=True,
    )
    ts.scrape_tournaments(cfg, auth_token="tok", verbose=False)

    conn = sqlite3.connect(cache)
    elo = conn.execute(
        "SELECT event_slug, event_num_entrants FROM tournaments "
        "WHERE videogame_name = ? AND event_num_entrants >= 16",
        ("Super Smash Bros. Ultimate",),
    ).fetchall()
    conn.close()
    assert ("tournament/gh217/event/smash-ultimate-singles-switch-7-30-pm", 42) in elo

    # Simulate PR Maker fresh: delete ELO-eligible rows only, then force_refresh.
    conn = sqlite3.connect(cache)
    conn.execute(
        "DELETE FROM tournaments WHERE videogame_name = ? AND event_num_entrants >= 16",
        ("Super Smash Bros. Ultimate",),
    )
    conn.commit()
    conn.close()

    _, stats = ts.scrape_tournaments(cfg, auth_token="tok", verbose=False)
    assert stats.hits == 0
    assert stats.misses == 1

    conn = sqlite3.connect(cache)
    row = conn.execute(
        "SELECT event_num_entrants FROM tournaments WHERE event_slug = ?",
        ("tournament/gh217/event/smash-ultimate-singles-switch-7-30-pm",),
    ).fetchone()
    conn.close()
    assert row is not None and row[0] == 42


# --- CSV / OOR report cache: in-region stats must come from sets --------------


def test_refresh_in_region_stats_overrides_stale_cache_zeros():
    sets = [
        {
            "p1": "Alice", "p2": "Bob", "p1_score": 3, "p2_score": 1,
            "event_slug": "tournament/a/event/singles", "tournament_id": "t1",
            "tournament_name": "Weekly A",
        },
        {
            "p1": "Alice", "p2": "Cara", "p1_score": 2, "p2_score": 3,
            "event_slug": "tournament/a/event/singles", "tournament_id": "t1",
            "tournament_name": "Weekly A",
        },
    ]
    stale = web_api._empty_report("Alice")
    refreshed = web_api._refresh_in_region_stats(stale, "Alice", sets)
    assert refreshed["in_region_wins"] == 1
    assert refreshed["in_region_losses"] == 1
    assert refreshed["in_region_tournaments"] == 1
    assert not web_api._is_empty_report(refreshed)


def test_empty_cached_report_triggers_live_oor_refetch(monkeypatch, tmp_path):
    """Empty shells in live_report_cache must not short-circuit OOR warm/export."""
    cache = tmp_path / "oor.db"
    monkeypatch.setattr(web_api, "_OOR_CACHE_DB_PATH", cache)

    sets = [
        {
            "p1": "Alice", "p2": "Bob", "p1_score": 3, "p2_score": 1,
            "event_slug": "t/a/s", "tournament_id": "t1", "tournament_name": "A",
        },
    ]
    ctx = "ctx-test"
    conn = web_api._oor_cache_conn()
    web_api._cache_put_report(conn, ctx, "Alice", web_api._empty_report("Alice"))
    conn.close()

    live_calls: list[str] = []

    def fake_live(**kwargs):
        live_calls.append(kwargs["canonical_name"])
        return {
            "canonical_name": "Alice",
            "in_region_tournaments": 0,
            "in_region_wins": 0,
            "in_region_losses": 0,
            "out_region_tournaments": 1,
            "out_region_wins": 2,
            "out_region_losses": 1,
            "notable_out_wins": [("Bob", 1)],
            "notable_out_losses": [],
            "all_out_wins": ["Bob"],
            "all_out_losses": [],
            "in_region_placements": [],
            "out_region_placements": [{"tournament_id": "x", "wins": 2, "losses": 1}],
        }

    monkeypatch.setattr(web_api, "_get_live_player_report", fake_live)
    monkeypatch.setattr(web_api, "_build_identity_map_live", lambda *a, **kw: {
        "Alice": {"player_id": "pid1", "user_id": "uid1"},
    })
    monkeypatch.setattr(web_api, "_init_player_db", lambda *a, **kw: sqlite3.connect(":memory:"))
    monkeypatch.setattr(web_api, "_upsert_live_player_report", lambda *a, **kw: None)
    monkeypatch.setattr(web_api, "_in_region_tournament_ids", lambda *a, **kw: set())
    monkeypatch.setattr(web_api, "StartGGClient", lambda token: object())

    cfg = ec.EloConfig()
    reports = web_api._load_reports_for_players(
        ["Alice"], sets, {"Alice": 1500.0}, cfg, ctx_hash=ctx, oor_window_hash="win1",
    )
    assert live_calls == ["Alice"]
    assert reports["Alice"]["in_region_wins"] == 1
    assert reports["Alice"]["out_region_wins"] == 2
