"""
Tests for the optimized OOR (out-of-region) data pipeline.

Covers:
- cache reads/writes from thread-pool workers (check_same_thread regression)
- granular oor_event_row storage keeping notable win/loss lists (rebuild fidelity)
- in-memory hot report cache layered over SQLite
- cache-only report loading (comparison fast path) and missing-player detection
- parallel set-history pagination (ordering, early-stop, complexity fallback)
- shared event-standings maps replacing per-(event, player) standings rescans
"""

from __future__ import annotations

import sqlite3
import sys
import threading
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "demo" / "base_demo"))
sys.path.insert(0, str(ROOT / "tools"))

import elo_calculator as ec  # noqa: E402
import web_api  # noqa: E402


# --- fixtures ---------------------------------------------------------------


SETS = [
    {
        "p1": "Alice", "p2": "Bob", "p1_score": 3, "p2_score": 1,
        "event_slug": "t/a/s", "event_id": "e1", "tournament_id": "t1",
        "tournament_name": "Weekly A",
    },
    {
        "p1": "Alice", "p2": "Cara", "p1_score": 2, "p2_score": 3,
        "event_slug": "t/a/s", "event_id": "e1", "tournament_id": "t1",
        "tournament_name": "Weekly A",
    },
]


def _live_report(name: str) -> dict:
    return {
        "canonical_name": name,
        "in_region_tournaments": 0,
        "in_region_wins": 0,
        "in_region_losses": 0,
        "in_region_placements": [],
        "out_region_tournaments": 1,
        "out_region_wins": 2,
        "out_region_losses": 1,
        "out_region_placements": [{
            "tournament_id": "oor-t1",
            "tournament_name": "Major X",
            "event_slug": "t/major-x/s",
            "event_id": "oor-e1",
            "start_at": 1700000000,
            "placement": 7,
            "wins": 2,
            "losses": 1,
            "notable_wins": ["FamousPlayer", "OtherPlayer"],
            "notable_losses": ["TopSeed"],
        }],
        "notable_out_wins": [("FamousPlayer", 1), ("OtherPlayer", 1)],
        "notable_out_losses": [("TopSeed", 1)],
        "all_out_wins": ["FamousPlayer", "OtherPlayer"],
        "all_out_losses": ["TopSeed"],
    }


@pytest.fixture()
def oor_env(monkeypatch, tmp_path):
    """Fresh OOR cache DB + clean in-memory layers + stubbed start.gg deps."""
    monkeypatch.setattr(web_api, "_OOR_CACHE_DB_PATH", tmp_path / "oor.db")
    monkeypatch.setattr(web_api, "_oor_schema_ready", False)
    web_api._OOR_MEM_REPORTS.clear()
    web_api._OOR_MEM_STANDINGS.clear()
    monkeypatch.setenv("STARTGG_API_KEY", "test-token")

    live_calls: list[str] = []

    def fake_live(**kwargs):
        live_calls.append(kwargs["canonical_name"])
        return _live_report(kwargs["canonical_name"])

    monkeypatch.setattr(web_api, "_get_live_player_report", fake_live)
    monkeypatch.setattr(web_api, "_build_identity_map_live", lambda *a, **kw: {
        "Alice": {"player_id": "pid1", "user_id": "uid1"},
        "Bob": {"player_id": "pid2", "user_id": "uid2"},
    })
    monkeypatch.setattr(web_api, "_init_player_db", lambda *a, **kw: sqlite3.connect(":memory:"))
    monkeypatch.setattr(web_api, "_upsert_live_player_report", lambda *a, **kw: None)
    monkeypatch.setattr(web_api, "_in_region_tournament_ids", lambda *a, **kw: set())
    monkeypatch.setattr(web_api, "StartGGClient", lambda token: object())
    return live_calls


# --- cache works from pool threads (check_same_thread regression) ------------


def test_pool_path_persists_caches_and_skips_refetch(oor_env):
    """The non-stream path runs _fetch_one in a ThreadPoolExecutor; cache writes
    from those worker threads must persist (the old conn was same-thread-only
    and every write silently failed)."""
    live_calls = oor_env
    cfg = ec.EloConfig()
    ctx = "ctx-pool"

    reports = web_api._load_reports_for_players(
        ["Alice"], SETS, {"Alice": 1500.0}, cfg, ctx_hash=ctx, oor_window_hash="w1",
    )
    assert live_calls == ["Alice"]
    assert reports["Alice"]["out_region_wins"] == 2

    # Cached rows must exist (written from a pool worker thread).
    conn = web_api._oor_cache_conn()
    assert web_api._cache_get_report(conn, ctx, "Alice") is not None
    assert web_api._oor_event_row_count(conn, ctx, "Alice") == 1
    assert web_api._oor_fetch_state(conn, ctx, "Alice") == "complete"
    conn.close()

    # Second load (memory cleared → SQLite layer) must not hit the live path.
    web_api._OOR_MEM_REPORTS.clear()
    reports2 = web_api._load_reports_for_players(
        ["Alice"], SETS, {"Alice": 1500.0}, cfg, ctx_hash=ctx, oor_window_hash="w1",
    )
    assert live_calls == ["Alice"]  # unchanged
    assert reports2["Alice"]["out_region_wins"] == 2


def test_event_rows_keep_notables_and_rebuild_preserves_them(oor_env):
    cfg = ec.EloConfig()
    ctx = "ctx-rebuild"
    web_api._load_reports_for_players(
        ["Alice"], SETS, {"Alice": 1500.0}, cfg, ctx_hash=ctx, oor_window_hash="w1",
    )

    conn = web_api._oor_cache_conn()
    row = conn.execute(
        "SELECT notable_wins_json, notable_losses_json, start_at, event_id "
        "FROM oor_event_row WHERE context_hash = ? AND canonical_name = ?",
        (ctx, "Alice"),
    ).fetchone()
    assert row is not None
    assert "FamousPlayer" in row[0]
    assert "TopSeed" in row[1]
    assert row[2] == 1700000000
    assert row[3] == "oor-e1"

    # Drop the full-report row; rebuild from granular rows must restore notables.
    conn.execute("DELETE FROM live_report_cache WHERE context_hash = ?", (ctx,))
    conn.commit()
    rebuilt = web_api._oor_rebuild_report_from_rows(conn, ctx, "Alice", SETS)
    conn.close()
    assert rebuilt["out_region_wins"] == 2
    assert ("FamousPlayer", 1) in [tuple(t) for t in rebuilt["notable_out_wins"]]
    assert rebuilt["out_region_placements"][0]["notable_losses"] == ["TopSeed"]


# --- in-memory hot cache ------------------------------------------------------


def test_memory_cache_serves_repeat_loads_without_sqlite(oor_env, monkeypatch):
    cfg = ec.EloConfig()
    ctx = "ctx-mem"
    web_api._load_reports_for_players(
        ["Alice"], SETS, {"Alice": 1500.0}, cfg, ctx_hash=ctx, oor_window_hash="w1",
    )
    assert web_api._mem_report_get(ctx, "Alice") is not None

    # Break SQLite entirely: memory layer must still serve the report.
    monkeypatch.setattr(web_api, "_oor_cache_conn", lambda: (_ for _ in ()).throw(RuntimeError("db down")))
    reports = web_api._load_reports_for_players(
        ["Alice"], SETS, {"Alice": 1500.0}, cfg, ctx_hash=ctx, oor_window_hash="w1",
    )
    assert reports["Alice"]["out_region_wins"] == 2


def test_force_refresh_drops_memory_entry(oor_env):
    cfg = ec.EloConfig()
    ctx = "ctx-force"
    live_calls = oor_env
    web_api._load_reports_for_players(
        ["Alice"], SETS, {"Alice": 1500.0}, cfg, ctx_hash=ctx, oor_window_hash="w1",
    )
    assert live_calls == ["Alice"]
    web_api._load_reports_for_players(
        ["Alice"], SETS, {"Alice": 1500.0}, cfg, ctx_hash=ctx, oor_window_hash="w1",
        force_refresh_oor=True,
    )
    assert live_calls == ["Alice", "Alice"]  # refetched despite warm caches


# --- cache-only loading (comparison fast path) ---------------------------------


def test_load_cached_reports_only_hits_and_missing(oor_env, monkeypatch):
    cfg = ec.EloConfig()
    ctx = "ctx-cacheonly"
    monkeypatch.setattr(
        web_api,
        "_player_id_for_name",
        lambda name: {"Alice": "pid1", "Bob": "pid2"}.get(name, ""),
    )
    web_api._load_reports_for_players(
        ["Alice"], SETS, {"Alice": 1500.0}, cfg, ctx_hash=ctx, oor_window_hash="w1",
    )
    conn = web_api._oor_cache_conn()
    web_api._oor_put_event_standings(conn, "e1", {"pid1": 5})
    conn.close()
    reports, missing = web_api._load_cached_reports_only(["Alice", "Bob"], SETS, ctx)
    assert "Alice" in reports
    assert reports["Alice"]["out_region_wins"] == 2
    # In-region stats refreshed from the live set pool even on cache hits.
    assert reports["Alice"]["in_region_wins"] == 1
    assert missing == ["Bob"]
    assert len(oor_env) == 1  # cache-only path never triggers live fetches


def test_load_cached_reports_only_accepts_complete_no_oor_player(oor_env):
    """A player marked complete with zero OOR rows is a legit no-OOR player, not a miss."""
    ctx = "ctx-no-oor"
    conn = web_api._oor_cache_conn()
    web_api._oor_set_fetch_state(conn, ctx, "Bob", pages_fetched=1, total_pages=1, status="complete")
    conn.close()
    reports, missing = web_api._load_cached_reports_only(["Bob"], [], ctx)
    assert missing == []
    assert reports["Bob"]["out_region_tournaments"] == 0


def test_tournament_result_cache_is_per_event_slug(oor_env):
    """Two brackets at the same tournament must not share one cached W-L row."""
    conn = web_api._oor_cache_conn()
    pid = "pid-multi"
    tid = "tourney-832345"
    web_api._oor_put_tournament_result(conn, pid, "t/lvl/2v2", {
        "tournament_id": tid,
        "event_slug": "t/lvl/2v2",
        "event_id": "e-2v2",
        "tournament_name": "LVL UP",
        "start_at": 1,
        "wins": 0,
        "losses": 0,
        "notable_wins": [],
        "notable_losses": [],
        "placement": None,
    })
    web_api._oor_put_tournament_result(conn, pid, "t/lvl/singles", {
        "tournament_id": tid,
        "event_slug": "t/lvl/singles",
        "event_id": "e-singles",
        "tournament_name": "LVL UP",
        "start_at": 1,
        "wins": 3,
        "losses": 2,
        "notable_wins": ["Pollo"],
        "notable_losses": ["Abe"],
        "placement": 17,
    })
    shell = web_api._oor_get_tournament_result(conn, pid, "t/lvl/2v2")
    singles = web_api._oor_get_tournament_result(conn, pid, "t/lvl/singles")
    conn.close()
    assert shell is not None and shell["wins"] == 0
    assert singles is not None and singles["wins"] == 3
    assert singles["notable_wins"] == ["Pollo"]


def test_load_cached_reports_only_refetches_empty_event_shells(oor_env):
    """All-zero event rows from a bad tournament_id cache must trigger a refetch."""
    ctx = "ctx-shells"
    conn = web_api._oor_cache_conn()
    for slug in ("t/lvl/2v2", "t/lvl/singles"):
        web_api._oor_upsert_event_row(conn, ctx, "Bob", {
            "tournament_id": "832345",
            "event_slug": slug,
            "event_id": "e1",
            "tournament_name": "LVL UP",
            "start_at": 1,
            "wins": 0,
            "losses": 0,
            "notable_wins": [],
            "notable_losses": [],
            "placement": None,
        })
    web_api._oor_set_fetch_state(conn, ctx, "Bob", pages_fetched=1, total_pages=1, status="complete")
    web_api._cache_put_report(conn, ctx, "Bob", web_api._empty_report("Bob"))
    conn.close()

    reports, missing = web_api._load_cached_reports_only(["Bob"], SETS, ctx)
    assert missing == ["Bob"]
    assert "Bob" not in reports


def test_load_cached_reports_only_refetches_bracket_bleed_rows(oor_env):
    """Identical singles + doubles rows at one tournament must trigger a refetch."""
    ctx = "ctx-bleed"
    conn = web_api._oor_cache_conn()
    bleed = {
        "tournament_id": "832345",
        "tournament_name": "LVL UP",
        "start_at": 1,
        "wins": 1,
        "losses": 2,
        "notable_wins": ["Beastly"],
        "notable_losses": ["CX | Madnoah14", "CTG | Zomba"],
        "placement": 9,
    }
    web_api._oor_upsert_event_row(conn, ctx, "Puresalt", {
        **bleed,
        "event_slug": "t/lvl/singles",
        "event_id": "e-singles",
    })
    web_api._oor_upsert_event_row(conn, ctx, "Puresalt", {
        **bleed,
        "event_slug": "t/lvl/2v2",
        "event_id": "e-2v2",
    })
    web_api._oor_set_fetch_state(conn, ctx, "Puresalt", pages_fetched=1, total_pages=1, status="complete")
    conn.close()

    reports, missing = web_api._load_cached_reports_only(["Puresalt"], SETS, ctx)
    assert missing == ["Puresalt"]
    assert "Puresalt" not in reports


def test_resolve_set_for_player_matches_any_team_member():
    node = {
        "slots": [
            {
                "entrant": {"participants": [
                    {"prefix": "A", "gamerTag": "Ally", "player": {"id": "ally"}},
                    {"prefix": "", "gamerTag": "Puresalt", "player": {"id": "41129"}},
                ]},
                "standing": {"stats": {"score": {"value": 2}}},
            },
            {
                "entrant": {"participants": [
                    {"prefix": "CS3", "gamerTag": "Andrik", "player": {"id": "andrik"}},
                ]},
                "standing": {"stats": {"score": {"value": 3}}},
            },
        ],
    }
    resolved = ec._resolve_set_for_player(node, "41129")
    assert resolved is not None
    opp, won, pscore, oscore = resolved
    assert opp == "CS3 | Andrik"
    assert won is False
    assert pscore == 2 and oscore == 3


def test_is_oor_eligible_event_skips_doubles():
    assert ec._is_oor_eligible_event({
        "slug": "tournament/lvl-up-expo/event/smash-ultimate-2v2",
        "name": "Smash Ultimate 2v2",
    }) is False
    assert ec._is_oor_eligible_event({
        "slug": "tournament/lvl-up-expo/event/smash-ultimate-singles",
        "name": "Smash Ultimate Singles",
    }) is True


def test_oor_aggregation_skips_doubles_brackets():
    nodes = [
        {
            "event": {
                "id": "e-singles",
                "slug": "tournament/lvl/event/singles",
                "name": "Singles",
                "tournament": {"id": "t1", "name": "Major", "startAt": 1700000000},
            },
            "slots": [
                {"entrant": {"participants": [{"gamerTag": "Puresalt", "player": {"id": "41129"}}]},
                 "standing": {"stats": {"score": {"value": 3}}}},
                {"entrant": {"participants": [{"prefix": "CS3", "gamerTag": "Andrik", "player": {"id": "9"}}]},
                 "standing": {"stats": {"score": {"value": 1}}}},
            ],
        },
        {
            "event": {
                "id": "e-2v2",
                "slug": "tournament/lvl/event/2v2",
                "name": "2v2",
                "tournament": {"id": "t1", "name": "Major", "startAt": 1700000000},
            },
            "slots": [
                {"entrant": {"participants": [
                    {"gamerTag": "Puresalt", "player": {"id": "41129"}},
                    {"gamerTag": "Ally", "player": {"id": "ally"}},
                ]},
                 "standing": {"stats": {"score": {"value": 0}}}},
                {"entrant": {"participants": [
                    {"prefix": "CX", "gamerTag": "Madnoah14", "player": {"id": "x"}},
                    {"gamerTag": "Zomba", "player": {"id": "z"}},
                ]},
                 "standing": {"stats": {"score": {"value": 2}}}},
            ],
        },
    ]
    out_events: dict[str, dict] = {}
    for node in nodes:
        event = node["event"]
        if not ec._is_oor_eligible_event(event):
            continue
        key = event["slug"]
        if key not in out_events:
            out_events[key] = {"notable_wins": [], "notable_losses": [], "wins": 0, "losses": 0}
        resolved = ec._resolve_set_for_player(node, "41129")
        if resolved is None:
            continue
        opp, won, _, _ = resolved
        if won:
            out_events[key]["wins"] += 1
            out_events[key]["notable_wins"].append(opp)
        else:
            out_events[key]["losses"] += 1
            out_events[key]["notable_losses"].append(opp)
    assert list(out_events) == ["tournament/lvl/event/singles"]
    assert out_events["tournament/lvl/event/singles"]["notable_wins"] == ["CS3 | Andrik"]
    assert out_events["tournament/lvl/event/singles"]["notable_losses"] == []


# --- parallel set-history pagination -------------------------------------------


class _FakeSetsClient:
    """Serves PLAYER_SETS_QUERY pages; optionally rejects high perPage."""

    def __init__(self, pages: list[list[dict]], complexity_above: int | None = None):
        self.pages = pages
        self.complexity_above = complexity_above
        self.calls: list[tuple[int, int]] = []
        self._lock = threading.Lock()

    def gql(self, query, variables, max_retries=5):
        page = int(variables["page"])
        per_page = int(variables["perPage"])
        with self._lock:
            self.calls.append((page, per_page))
        if self.complexity_above is not None and per_page > self.complexity_above:
            raise RuntimeError("GraphQL complexity limit exceeded (query=PlayerSets)")
        nodes = self.pages[page - 1] if page <= len(self.pages) else []
        return {
            "data": {"player": {"sets": {
                "nodes": nodes,
                "pageInfo": {"totalPages": len(self.pages)},
            }}},
        }


def _set_node(ts: int) -> dict:
    return {"event": {"tournament": {"startAt": ts}}, "id": f"s{ts}"}


def test_fetch_player_sets_live_parallel_collects_all_pages():
    pages = [[_set_node(100 + i)] for i in range(5)]
    client = _FakeSetsClient(pages)
    seen_pages: list[int] = []
    nodes = ec._fetch_player_sets_live(
        client, "pid", 40, 3,
        page_callback=lambda p, t, n: seen_pages.append(p),
        page_concurrency=3,
    )
    assert len(nodes) == 5
    assert sorted(seen_pages) == [1, 2, 3, 4, 5]


def test_fetch_player_sets_live_early_stop_between_batches():
    # Reverse-chronological pages; pages 3+ are entirely before the PR window.
    window_start = 1000
    pages = [
        [_set_node(2000)],
        [_set_node(1500)],
        [_set_node(900)],   # all pre-window → stop here
        [_set_node(800)],
        [_set_node(700)],
        [_set_node(600)],
        [_set_node(500)],
        [_set_node(400)],
    ]
    metrics: dict = {}
    client = _FakeSetsClient(pages)
    ec._fetch_player_sets_live(
        client, "pid", 40, 3,
        pr_window_start_unix=window_start,
        metrics_out=metrics,
        page_concurrency=2,
    )
    assert metrics["early_stop"] is True
    # Page 1 + at most two batches of 2 → never all 8 pages.
    assert metrics["pages_fetched"] < len(pages)


def test_fetch_player_sets_live_complexity_halves_per_page():
    pages = [[_set_node(100)]]
    client = _FakeSetsClient(pages, complexity_above=20)
    nodes = ec._fetch_player_sets_live(client, "pid", 40, 3, page_concurrency=2)
    assert len(nodes) == 1
    tried = sorted({pp for (_p, pp) in client.calls}, reverse=True)
    assert tried[0] == 40
    assert tried[-1] <= 20


# --- shared standings maps ------------------------------------------------------


class _FakeStandingsClient:
    def __init__(self, standings_by_event: dict[str, list[tuple[str, int]]]):
        self.standings = standings_by_event
        self.fetched_events: list[str] = []
        self._lock = threading.Lock()

    def gql(self, query, variables, max_retries=5):
        eid = str(variables["eventId"])
        with self._lock:
            self.fetched_events.append(eid)
        nodes = [
            {
                "placement": placement,
                "entrant": {"participants": [{"player": {"id": pid}}]},
            }
            for pid, placement in self.standings.get(eid, [])
        ]
        return {"data": {"event": {"standings": {
            "nodes": nodes, "pageInfo": {"totalPages": 1},
        }}}}


def test_fetch_event_standings_map_builds_full_map():
    client = _FakeStandingsClient({"e1": [("p1", 1), ("p2", 2), ("p3", 5)]})
    smap = ec.fetch_event_standings_map(client, "e1")
    assert smap == {"p1": 1, "p2": 2, "p3": 5}


def test_resolve_placements_reuses_cached_maps():
    client = _FakeStandingsClient({"e2": [("p9", 3)]})
    store: dict[str, dict] = {"e1": {"p9": 7}}

    placements, hits, misses = ec._resolve_placements(
        client, "p9", ["e1", "e2"], 3,
        standings_lookup=lambda eid: store.get(eid),
        standings_store=lambda eid, smap: store.__setitem__(eid, smap),
    )
    assert placements == {"e1": 7, "e2": 3}
    assert (hits, misses) == (1, 1)
    assert client.fetched_events == ["e2"]  # cached event never refetched
    assert store["e2"] == {"p9": 3}         # miss persisted for other players


def test_oor_fingerprints_are_version_salted():
    """Bumping _OOR_FINGERPRINT_VERSION must invalidate context/window caches."""
    ch = web_api._pr_maker_context_hash("2026-01-01", "2026-06-01", ["t/a/s"], [])
    wh = web_api._oor_window_hash("2026-01-01", "2026-06-01")
    import hashlib
    # v1 (unsalted) formats must no longer match.
    legacy_wh = hashlib.sha256(b"2026-01-01:2026-06-01").hexdigest()[:16]
    assert wh != legacy_wh
    assert len(ch) == 24 and len(wh) == 16


# --- online tournaments excluded from OOR ---------------------------------------


def _oor_set_node(tournament_id: str, *, event_online=None, tournament_online=None,
                  win=True, opponent="Opp", ts=1769000000):
    event = {
        "id": f"e-{tournament_id}",
        "slug": f"t/{tournament_id}/s",
        "name": "Singles",
        "tournament": {
            "id": tournament_id,
            "name": f"Tourney {tournament_id}",
            "startAt": ts,
        },
    }
    if event_online is not None:
        event["isOnline"] = event_online
    if tournament_online is not None:
        event["tournament"]["isOnline"] = tournament_online
    me = {"entrant": {"participants": [{"prefix": "", "gamerTag": "Me", "player": {"id": "pid-me"}}]},
          "standing": {"stats": {"score": {"value": 3 if win else 1}}}}
    them = {"entrant": {"participants": [{"prefix": "", "gamerTag": opponent, "player": {"id": "pid-them"}}]},
            "standing": {"stats": {"score": {"value": 1 if win else 3}}}}
    return {"id": f"set-{tournament_id}", "event": event, "slots": [me, them]}


def test_set_node_is_online_semantics():
    assert ec._set_node_is_online(_oor_set_node("t1", event_online=True)) is True
    assert ec._set_node_is_online(_oor_set_node("t2", tournament_online=True)) is True
    # Event-level flag is more precise and wins over the tournament flag.
    assert ec._set_node_is_online(_oor_set_node("t3", event_online=False, tournament_online=True)) is False
    # Legacy cached nodes without either field are treated as offline.
    assert ec._set_node_is_online(_oor_set_node("t4")) is False


def test_live_report_excludes_online_tournaments():
    nodes = [
        _oor_set_node("off-1", event_online=False, opponent="OfflineFoe"),
        _oor_set_node("on-1", event_online=True, opponent="WifiWarrior"),
        _oor_set_node("on-2", tournament_online=True, opponent="LagLord"),
    ]
    phases = []
    report = ec._get_live_player_report(
        client=object(),
        config=ec.EloConfig(start_date="2026-01-01", end_date="2026-06-01"),
        canonical_name="Me",
        user_id="u1",
        player_id="pid-me",
        in_region_sets=[],
        in_region_tournament_ids=set(),
        verbose=False,
        phase_callback=lambda ph, d: phases.append((ph, d)),
        preloaded_set_nodes=nodes,
        standings_lookup=lambda eid: {"pid-me": 5},  # no live standings calls
    )
    assert report["out_region_tournaments"] == 1
    assert report["out_region_placements"][0]["tournament_id"] == "off-1"
    all_wins = report["all_out_wins"]
    assert "OfflineFoe" in all_wins
    assert "WifiWarrior" not in all_wins and "LagLord" not in all_wins
    discovered = [d for ph, d in phases if ph == "oor_tournaments_discovered"]
    assert discovered and discovered[0]["online_excluded"] == 2


def test_catalog_excludes_online_tournaments():
    class _CatalogClient:
        def gql(self, query, variables, max_retries=5):
            return {"data": {"tournaments": {
                "pageInfo": {"totalPages": 1},
                "nodes": [
                    {"id": "100", "isOnline": False},
                    {"id": "200", "isOnline": True},
                    {"id": "300"},  # missing flag → offline
                    {"id": "400", "isOnline": False},  # in-region, excluded
                ],
            }}}

    ids = ec.fetch_oor_tournament_catalog(
        _CatalogClient(), 1, 2, in_region_tournament_ids={"400"},
    )
    assert ids == ["100", "300"]


def test_standings_sqlite_cache_roundtrip(oor_env):
    conn = web_api._oor_cache_conn()
    web_api._oor_put_event_standings(conn, "e77", {"p1": 4, "p2": None})
    # Memory layer
    assert web_api._mem_standings_get("e77") == {"p1": 4, "p2": None}
    # SQLite layer (memory cleared)
    web_api._OOR_MEM_STANDINGS.clear()
    assert web_api._oor_get_event_standings(conn, "e77") == {"p1": 4, "p2": None}
    conn.close()


def test_refresh_in_region_stats_enriches_placements_from_standings_cache(oor_env):
    """Rebuilt/cached reports must recover placements already stored for the event."""
    conn = web_api._oor_cache_conn()
    web_api._oor_put_event_standings(conn, "gh-218", {"pid-nabster": 13, "pid-natron": 7})

    sets = [{
        "p1": "8Bit | Nabster", "p2": "NLC | Natron", "p1_score": 2, "p2_score": 3,
        "event_slug": "t/guildhouse/218", "event_id": "gh-218", "tournament_id": "t1",
        "tournament_name": "Guildhouse Weekly 218",
    }]
    report = {
        "canonical_name": "8Bit | Nabster",
        "in_region_placements": [{
            "tournament_id": "t1",
            "tournament_name": "Guildhouse Weekly 218",
            "event_slug": "t/guildhouse/218",
            "event_id": "gh-218",
            "placement": None,
            "wins": 2,
            "losses": 2,
        }],
    }
    refreshed = web_api._refresh_in_region_stats(
        report, "8Bit | Nabster", sets, cache_conn=conn, player_id="pid-nabster",
    )
    conn.close()
    assert refreshed["in_region_placements"][0]["placement"] == 13
    assert refreshed["in_region_placements"][0]["wins"] == 0
    assert refreshed["in_region_placements"][0]["losses"] == 1


def test_rebuild_report_then_refresh_recovers_in_region_placements(oor_env):
    conn = web_api._oor_cache_conn()
    web_api._oor_put_event_standings(conn, "gh-218", {"pid-nabster": 13})

    sets = [{
        "p1": "8Bit | Nabster", "p2": "NLC | Natron", "p1_score": 2, "p2_score": 3,
        "event_slug": "t/guildhouse/218", "event_id": "gh-218", "tournament_id": "t1",
        "tournament_name": "Guildhouse Weekly 218",
    }]
    rebuilt = web_api._oor_rebuild_report_from_rows(conn, "ctx1", "8Bit | Nabster", sets)
    assert rebuilt["in_region_placements"][0]["placement"] is None
    refreshed = web_api._refresh_in_region_stats(
        rebuilt, "8Bit | Nabster", sets, cache_conn=conn, player_id="pid-nabster",
    )
    conn.close()
    assert refreshed["in_region_placements"][0]["placement"] == 13


def test_load_cached_reports_only_marks_incomplete_in_region_placements_missing(oor_env):
    """Rebuild path must not satisfy cache when in-region placements are still null."""
    ctx = "ctx-incomplete-placements"
    conn = web_api._oor_cache_conn()
    web_api._oor_set_fetch_state(conn, ctx, "ebs | ayden", pages_fetched=1, total_pages=1, status="complete")
    conn.close()

    sets = [{
        "p1": "Alsoda", "p2": "ebs | ayden", "p1_score": 3, "p2_score": 1,
        "event_slug": "t/guildhouse/223", "event_id": "gh-223", "tournament_id": "t1",
        "tournament_name": "Guildhouse Weekly 223",
    }]
    reports, missing = web_api._load_cached_reports_only(["ebs | ayden"], sets, ctx)
    assert "ebs | ayden" not in reports
    assert missing == ["ebs | ayden"]


def test_load_cached_reports_only_rebuild_with_standings_cache_has_placements(oor_env, monkeypatch):
    """Standings backfill should keep rebuild-path players off the missing list."""
    monkeypatch.setattr(
        web_api,
        "_player_id_for_name",
        lambda name: "pid-ayden" if "ayden" in name.casefold() else "",
    )
    ctx = "ctx-standings-backfill"
    conn = web_api._oor_cache_conn()
    web_api._oor_set_fetch_state(conn, ctx, "ebs | ayden", pages_fetched=1, total_pages=1, status="complete")
    web_api._oor_put_event_standings(conn, "gh-223", {"pid-ayden": 9})
    conn.close()

    sets = [{
        "p1": "Alsoda", "p2": "ebs | ayden", "p1_score": 3, "p2_score": 1,
        "event_slug": "t/guildhouse/223", "event_id": "gh-223", "tournament_id": "t1",
        "tournament_name": "Guildhouse Weekly 223",
    }]
    reports, missing = web_api._load_cached_reports_only(
        ["ebs | ayden"], sets, ctx,
    )
    assert missing == []
    assert reports["ebs | ayden"]["in_region_placements"][0]["placement"] == 9


def test_expanded_head_to_head_shows_backfilled_placement(oor_env):
    conn = web_api._oor_cache_conn()
    web_api._oor_put_event_standings(conn, "gh-223", {"pid-ayden": 9, "pid-alsoda": 4})

    sets = [{
        "p1": "Alsoda", "p2": "ebs | ayden", "p1_score": 3, "p2_score": 1,
        "event_slug": "t/guildhouse/223", "event_id": "gh-223", "tournament_id": "t1",
        "tournament_name": "Guildhouse Weekly 223",
    }]
    report_a = web_api._refresh_in_region_stats(
        web_api._empty_report("Alsoda"), "Alsoda", sets,
        cache_conn=conn, player_id="pid-alsoda",
    )
    report_b = web_api._refresh_in_region_stats(
        web_api._empty_report("ebs | ayden"), "ebs | ayden", sets,
        cache_conn=conn, player_id="pid-ayden",
    )
    conn.close()
    expanded = ec._expanded_head_to_head(
        "Alsoda", "ebs | ayden", sets,
        {"Alsoda": report_a, "ebs | ayden": report_b},
    )
    both = expanded["tournamentsBothAttended"]
    assert len(both) == 1
    assert both[0]["p1Place"] == 4
    assert both[0]["p2Place"] == 9
