"""Local vLLM Metal client (OpenAI-compatible API) for Gemma 4 models."""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import urljoin, urlparse

import httpx

_CHAT_COMPLETIONS_PATH = "/chat/completions"

# api_base that already passed a probe in this process (skip re-probing).
_last_probed_base: str | None = None

_REASONING_BLOCK_RE = re.compile(
    r"<\s*(?:(?:redacted_)?thinking|think)\b[^>]*>"
    r".*?"
    r"</\s*(?:(?:redacted_)?thinking|think)\s*>",
    re.IGNORECASE | re.DOTALL,
)


@dataclass(frozen=True)
class VllmRoute:
    """OpenAI-compatible root, e.g. ``http://127.0.0.1:8000/v1``."""

    api_base: str
    served_model_name: str


def _env(name: str, default: str = "") -> str:
    return (os.environ.get(name) or default).strip()


def _default_served_name(base_url: str, *, fallback: str) -> str:
    raw = (base_url or "").strip()
    if not raw:
        return fallback
    p = urlparse(raw)
    path = (p.path or "").rstrip("/")
    if path.endswith("/v1"):
        path = path[: -len("/v1")].rstrip("/")
    if path:
        seg = path.split("/")[-1]
        if seg:
            return seg
    return fallback


def vllm_route_from_env() -> VllmRoute | None:
    """Return the configured route or ``None`` if ``VLLM_14B_BASE_URL`` is unset."""
    base = _env("VLLM_14B_BASE_URL")
    if not base:
        return None
    model = _env("VLLM_14B_MODEL", "") or _default_served_name(
        base,
        fallback="gemma-4-26B-A4B",
    )
    return VllmRoute(api_base=base.rstrip("/"), served_model_name=model)


def strip_reasoning_blocks(text: str) -> str:
    """Remove chain-of-thought XML blocks from model output."""
    cleaned = _REASONING_BLOCK_RE.sub("", text)
    return re.sub(r"\n{3,}", "\n\n", cleaned).strip()


def _chat_url(api_base: str) -> str:
    return api_base.rstrip("/") + _CHAT_COMPLETIONS_PATH


def _merge_chat_template_kwargs(
    body: dict[str, Any], enable_thinking: bool | None
) -> None:
    if enable_thinking is None:
        return
    body.setdefault("chat_template_kwargs", {})["enable_thinking"] = bool(enable_thinking)


def chat_completion_text(
    client: httpx.Client,
    *,
    api_base: str,
    model: str,
    messages: list[dict[str, str]],
    temperature: float = 0.7,
    top_p: float = 0.8,
    max_tokens: int = 2048,
    enable_thinking: bool | None = False,
    timeout_sec: float = 600.0,
) -> str:
    """Non-streaming chat completion; returns assistant message content only."""
    url = _chat_url(api_base)
    body: dict[str, Any] = {
        "model": model,
        "messages": messages,
        "temperature": float(temperature),
        "top_p": float(top_p),
        "max_tokens": int(max_tokens),
        "stream": False,
    }
    _merge_chat_template_kwargs(body, enable_thinking)
    r = client.post(url, json=body, timeout=timeout_sec)
    r.raise_for_status()
    data = r.json()
    choices = data.get("choices")
    if not choices:
        return ""
    msg = choices[0].get("message") or {}
    content = msg.get("content")
    if content is None:
        return ""
    return strip_reasoning_blocks(str(content))


def _vllm_probe_url_candidates(route: VllmRoute) -> list[str]:
    base = route.api_base.rstrip("/")
    root = base.removesuffix("/v1") if base.endswith("/v1") else base
    candidates: list[str] = []
    if base.endswith("/v1"):
        candidates.append(f"{base}/models")
    candidates.append(urljoin(root + "/", "v1/models"))
    candidates.append(urljoin(root + "/", "health"))
    if not root.endswith("/"):
        candidates.append(f"{root}/health")

    seen: set[str] = set()
    ordered: list[str] = []
    for url in candidates:
        if url and url not in seen:
            seen.add(url)
            ordered.append(url)
    return ordered


def probe_vllm_route(client: httpx.Client, route: VllmRoute, timeout_sec: float = 3.0) -> bool:
    """True if the server looks like a reachable OpenAI-compatible vLLM instance."""
    for url in _vllm_probe_url_candidates(route):
        try:
            r = client.get(url, timeout=timeout_sec)
            if r.status_code < 500:
                return True
        except (OSError, httpx.RequestError):
            continue
    return False


def diagnose_vllm_metal_server(
    client: httpx.Client,
    route: VllmRoute,
    *,
    timeout_sec: float = 8.0,
) -> int:
    """Print a per-URL probe report; return 0 iff the server is healthy."""
    print("vLLM Metal / OpenAI-compatible server check (PR Maker)", file=sys.stderr)
    print(
        f"\napi_base={route.api_base!r} served_model={route.served_model_name!r}",
        file=sys.stderr,
    )
    lane_ok = False
    for url in _vllm_probe_url_candidates(route):
        try:
            r = client.get(url, timeout=timeout_sec)
            ct = (r.headers.get("content-type") or "").split(";")[0].strip().lower()
            detail = f"status={r.status_code}"
            if ct == "application/json" and r.content:
                try:
                    data = r.json()
                    if isinstance(data, dict) and "data" in data:
                        ids = [
                            str(m.get("id", ""))
                            for m in data["data"][:4]
                            if isinstance(m, dict)
                        ]
                        detail += f" models={ids!r}"
                    else:
                        snippet = json.dumps(data, ensure_ascii=False)[:180]
                        detail += f" body={snippet!r}"
                except (json.JSONDecodeError, ValueError):
                    detail += f" body={(r.text or '')[:120]!r}"
            elif r.text:
                detail += f" body={(r.text or '')[:120]!r}"
            if r.status_code < 500:
                print(f"  GET {url} -> {detail}", file=sys.stderr)
                lane_ok = True
                break
            print(f"  GET {url} -> {detail} (trying next URL)", file=sys.stderr)
        except (OSError, httpx.RequestError) as exc:
            print(f"  GET {url} -> {type(exc).__name__}: {exc}", file=sys.stderr)
    if not lane_ok:
        print(
            "  Server not healthy — start vLLM, e.g.:\n"
            '    vllm serve "$HOME/models/gemma-4-26B-A4B" --port 8000 \\\n'
            "      --served-model-name gemma-4-26B-A4B --reasoning-parser gemma4",
            file=sys.stderr,
        )
        print("\nvLLM probe failed. Fix the server, then re-run.", file=sys.stderr)
        return 1
    print("\nvLLM responded (HTTP < 500 on at least one probe URL).", file=sys.stderr)
    return 0


def llm_unavailable_reason() -> str:
    """Human-readable explanation for why the local LLM path is unavailable."""
    route = vllm_route_from_env()
    if route is None:
        return (
            "VLLM_14B_BASE_URL is missing/empty in environment or .env. "
            "Start vLLM Metal and set e.g. VLLM_14B_BASE_URL=http://127.0.0.1:8000/v1"
        )
    try:
        with httpx.Client() as client:
            if probe_vllm_route(client, route):
                return "Local vLLM server is available."
    except Exception as exc:
        return f"vLLM probe failed: {type(exc).__name__}: {exc}"
    return (
        f"vLLM server at {route.api_base!r} is not reachable. "
        'Start it with: vllm serve "$HOME/models/gemma-4-26B-A4B" --port 8000 '
        "--served-model-name gemma-4-26B-A4B --reasoning-parser gemma4"
    )


def complete_chat(
    messages: list[dict[str, str]],
    *,
    model: str | None = None,
    temperature: float = 0.7,
    top_p: float = 0.8,
    max_tokens: int = 2048,
    enable_thinking: bool | None = False,
    timeout_sec: float = 600.0,
    client: httpx.Client | None = None,
) -> str:
    """Run one chat completion against the configured vLLM route."""
    route = vllm_route_from_env()
    if route is None:
        raise RuntimeError(llm_unavailable_reason())
    use_model = (model or "").strip() or route.served_model_name

    if client is not None:
        return chat_completion_text(
            client,
            api_base=route.api_base,
            model=use_model,
            messages=messages,
            temperature=temperature,
            top_p=top_p,
            max_tokens=max_tokens,
            enable_thinking=enable_thinking,
            timeout_sec=timeout_sec,
        )

    global _last_probed_base
    with httpx.Client() as owned:
        # Probe only on the first call (or after the route changes) — re-probing
        # before every completion adds a round trip per AI argument request.
        if _last_probed_base != route.api_base:
            if not probe_vllm_route(owned, route, timeout_sec=min(timeout_sec, 8.0)):
                raise RuntimeError(llm_unavailable_reason())
            _last_probed_base = route.api_base
        return chat_completion_text(
            owned,
            api_base=route.api_base,
            model=use_model,
            messages=messages,
            temperature=temperature,
            top_p=top_p,
            max_tokens=max_tokens,
            enable_thinking=enable_thinking,
            timeout_sec=timeout_sec,
        )


def _load_repo_dotenv() -> None:
    try:
        from dotenv import load_dotenv
    except ImportError:
        return
    root = Path(__file__).resolve().parent.parent
    load_dotenv(root / ".env")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Local vLLM Metal client utilities.")
    parser.add_argument(
        "--diagnose-only",
        action="store_true",
        help="Probe VLLM_14B_BASE_URL and exit 0 if the server responds.",
    )
    args = parser.parse_args(argv)
    if not args.diagnose_only:
        parser.print_help()
        return 1

    _load_repo_dotenv()
    route = vllm_route_from_env()
    if route is None:
        print(llm_unavailable_reason(), file=sys.stderr)
        return 1
    with httpx.Client() as client:
        return diagnose_vllm_metal_server(client, route)


if __name__ == "__main__":
    raise SystemExit(main())
