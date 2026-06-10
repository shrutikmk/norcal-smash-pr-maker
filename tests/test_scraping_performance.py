"""
Tests for the parallel / high-perPage scraping pipeline (no live start.gg needed).

Covers:
- folded set-record extraction matches the legacy per-set semantics
- the 1000-object-safe perPage clamp
- the shared rate gate (sync + async) honoring the min-interval spacing
- the async bounded paginator fetching every page with a stubbed client
- dry-run estimate math
"""

from __future__ import annotations

import asyncio
import sys
import time
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "demo" / "base_demo"))

import startgg_async as sa  # noqa: E402
import startgg_rate_gate as gate  # noqa: E402
import tournament_processor as tp  # noqa: E402


# --- folded set extraction ------------------------------------------------


def _set_node(set_id, p1, p1_pre, s1, p2, p2_pre, s2):
    def slot(tag, prefix, score):
        player = {"gamerTag": tag}
        if prefix is not None:
            player["prefix"] = prefix
        return {
            "entrant": {"participants": [{"player": player}]},
            "standing": {"stats": {"score": {"value": score}}},
        }

    return {"id": set_id, "slots": [slot(p1, p1_pre, s1), slot(p2, p2_pre, s2)]}


def test_extract_set_record_with_prefix():
    node = _set_node("s1", "MkLeo", "LG", 3, "Tweek", None, 1)
    assert tp._extract_set_record(node) == ("s1", "LG | MkLeo", "Tweek", 3, 1)


def test_extract_set_record_skips_preview():
    assert tp._extract_set_record({"id": "preview_5", "slots": []}) is None


def test_extract_set_record_skips_missing_entrant():
    node = _set_node("s1", "A", None, 1, "B", None, 0)
    node["slots"][1]["entrant"] = None
    assert tp._extract_set_record(node) is None


def test_extract_set_record_skips_null_score():
    node = _set_node("s1", "A", None, None, "B", None, 0)
    assert tp._extract_set_record(node) is None


def test_extract_set_record_skips_fewer_than_two_slots():
    node = _set_node("s1", "A", None, 1, "B", None, 0)
    node["slots"] = node["slots"][:1]
    assert tp._extract_set_record(node) is None


# --- perPage safety clamp --------------------------------------------------


def test_sets_per_page_clamped_to_object_limit():
    assert tp._safe_sets_per_page(10) == 10
    assert tp._safe_sets_per_page(99999) == tp.SETS_MAX_PER_PAGE
    assert tp._safe_sets_per_page(None) == tp.DEFAULT_SETS_PER_PAGE
    # The default must never exceed the 1000-object ceiling.
    assert tp.DEFAULT_SETS_PER_PAGE * tp.SETS_OBJECTS_PER_NODE <= tp.MAX_OBJECTS_PER_REQUEST


# --- rate gate -------------------------------------------------------------


def test_acquire_slot_respects_min_interval(monkeypatch):
    # Tighten the interval so the test is fast but still observable.
    monkeypatch.setattr(gate, "_MIN_INTERVAL_SEC", 0.05)
    monkeypatch.setattr(gate, "_MAX_REQUESTS_PER_60S", 1000)
    gate._times.clear()
    gate._last_emit_monotonic = 0.0

    start = time.monotonic()
    for _ in range(5):
        gate.acquire_slot()
    elapsed = time.monotonic() - start
    # 5 emits at >=0.05s spacing => at least ~0.2s (4 gaps), allow scheduler slack.
    assert elapsed >= 0.15


def test_acquire_slot_async_shares_budget(monkeypatch):
    monkeypatch.setattr(gate, "_MIN_INTERVAL_SEC", 0.02)
    monkeypatch.setattr(gate, "_MAX_REQUESTS_PER_60S", 1000)
    gate._times.clear()
    gate._last_emit_monotonic = 0.0

    before = gate.get_request_count()

    async def _hammer():
        await asyncio.gather(*[gate.acquire_slot_async() for _ in range(10)])

    asyncio.run(_hammer())
    assert gate.get_request_count() - before == 10


def test_metrics_snapshot_has_expected_keys():
    m = gate.get_metrics()
    for key in ("requests", "rate_limit_hits", "backoff_sec", "max_rpm", "min_interval_sec"):
        assert key in m


def test_is_complexity_error():
    assert gate.is_complexity_error("Query complexity exceeds the 1000 objects limit")
    assert not gate.is_complexity_error("some unrelated error")


def test_is_transient_server_error():
    payload = {"errors": [{"message": "An unknown error has occurred",
                           "extensions": {"category": "internal"}}]}
    assert gate.is_transient_server_error(payload, str(payload["errors"]))
    # category=internal alone is enough even with a different message
    assert gate.is_transient_server_error(
        {"errors": [{"message": "boom", "extensions": {"category": "internal"}}]}, "boom"
    )
    assert not gate.is_transient_server_error(
        {"errors": [{"message": "Variable $x not defined"}]}, "Variable $x not defined"
    )


# --- async paginator with a stubbed client --------------------------------


class _FakeClient:
    """Minimal stand-in for AsyncStartGGClient.gql returning canned pages."""

    def __init__(self, total_pages, per_page):
        self.total_pages = total_pages
        self.per_page = per_page
        self.calls = 0

    async def gql(self, query, variables):
        self.calls += 1
        page = variables["page"]
        # Each page returns `per_page` nodes (last page may be short, irrelevant here).
        nodes = [{"id": f"{page}-{i}"} for i in range(self.per_page)]
        return {
            "data": {
                "conn": {
                    "pageInfo": {"totalPages": self.total_pages},
                    "nodes": nodes,
                }
            }
        }


def test_paginate_async_fetches_all_pages():
    client = _FakeClient(total_pages=4, per_page=5)

    async def _run():
        return await sa.paginate_async(
            client,
            "query",
            {},
            extract_block=lambda p: p["data"]["conn"],
            per_page=5,
            concurrency=3,
        )

    nodes, first_block = asyncio.run(_run())
    assert len(nodes) == 4 * 5
    assert client.calls == 4  # one per page, no extra
    assert first_block["pageInfo"]["totalPages"] == 4


def test_gather_bounded_preserves_order():
    async def _run():
        async def make(i):
            async def coro():
                await asyncio.sleep((5 - i) * 0.001)  # later items finish first
                return i

            return coro

        factories = [await make(i) for i in range(5)]
        return await sa.gather_bounded(factories, concurrency=2)

    assert asyncio.run(_run()) == [0, 1, 2, 3, 4]


# --- dry-run estimate ------------------------------------------------------


def test_build_estimate_projects_requests():
    cfg = tp.ProcessorConfig(sets_per_page=40, concurrency=8)
    est = tp._build_estimate(num_miss_events=10, per_page=40, config=cfg)
    assert est["miss_events"] == 10
    assert est["sets_per_page"] == 40
    # 10 events * ceil(assumed/per_page) pages.
    assert est["projected_requests"] == 10 * est["pages_per_event"]
    assert est["projected_minutes"] >= 0
