"""Tests for tools/llm_client.py (no live vLLM required)."""

from __future__ import annotations

import sys
from pathlib import Path

import httpx
import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "tools"))

from llm_client import (  # noqa: E402
    VllmRoute,
    _default_served_name,
    _vllm_probe_url_candidates,
    diagnose_vllm_metal_server,
    probe_vllm_route,
    strip_reasoning_blocks,
    vllm_route_from_env,
)


def test_vllm_probe_url_candidates_includes_v1_models() -> None:
    route = VllmRoute("http://127.0.0.1:8000/v1", "gemma-4-26B-A4B")
    urls = _vllm_probe_url_candidates(route)
    assert "http://127.0.0.1:8000/v1/models" in urls
    assert len(urls) == len(set(urls))


def test_default_served_name_from_url_path() -> None:
    assert _default_served_name("http://127.0.0.1:8000/v1", fallback="gemma-4-26B-A4B") == "gemma-4-26B-A4B"
    assert (
        _default_served_name("http://127.0.0.1:8000/gemma-4-26B-A4B/v1", fallback="fallback")
        == "gemma-4-26B-A4B"
    )


def test_vllm_route_from_env_missing_returns_none(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("VLLM_14B_BASE_URL", raising=False)
    monkeypatch.delenv("VLLM_14B_MODEL", raising=False)
    assert vllm_route_from_env() is None


def test_vllm_route_from_env_parses_vars(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("VLLM_14B_BASE_URL", "http://127.0.0.1:8000/v1")
    monkeypatch.setenv("VLLM_14B_MODEL", "MyModel")
    route = vllm_route_from_env()
    assert route is not None
    assert route.api_base == "http://127.0.0.1:8000/v1"
    assert route.served_model_name == "MyModel"


def test_probe_vllm_route_returns_false_on_connection_refused() -> None:
    route = VllmRoute("http://127.0.0.1:59998/v1", "m")
    with httpx.Client() as client:
        assert probe_vllm_route(client, route, timeout_sec=0.5) is False


def test_diagnose_vllm_metal_server_exits_nonzero_when_down() -> None:
    route = VllmRoute("http://127.0.0.1:59997/v1", "m")
    with httpx.Client() as client:
        rc = diagnose_vllm_metal_server(client, route, timeout_sec=0.5)
    assert rc == 1


def test_strip_reasoning_blocks_removes_thinking_xml() -> None:
    think_open = "<" + "think" + ">"
    think_close = "</" + "think" + ">"
    raw = f"Answer here.\n\n{think_open}\nsecret chain\n{think_close}\n\nDone."
    cleaned = strip_reasoning_blocks(raw)
    assert "secret chain" not in cleaned
    assert "Answer here." in cleaned
    assert "Done." in cleaned
