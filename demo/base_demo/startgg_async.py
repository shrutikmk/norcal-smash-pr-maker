"""
Concurrent start.gg GraphQL engine (asyncio + httpx).

This module is the performance core of the scraping pipeline. It provides:

  * :class:`AsyncStartGGClient` — an ``httpx.AsyncClient`` wrapper that POSTs to
    the single GraphQL endpoint, sharing the *process-wide* rate budget with the
    synchronous code via :func:`startgg_rate_gate.acquire_slot_async`. It handles
    429s (honoring ``Retry-After``), transient 5xx / network errors, and
    GraphQL-level rate-limit / complexity errors.
  * :func:`paginate_async` — fetch page 1 of a connection to learn ``totalPages``,
    then fetch the remaining pages *concurrently* (bounded), instead of walking
    them one-at-a-time.
  * :func:`gather_bounded` — run many coroutines with a concurrency ceiling.
  * :func:`run_async` — safely drive an async coroutine from synchronous callers
    (the existing threaded job system), even if a loop is already running.

Why this is fast and still safe:
  * Concurrency only hides network round-trip latency. The shared rate gate is
    still the global throttle, so no matter how many tasks are in flight we never
    exceed ~``STARTGG_MAX_RPM`` requests / 60s. Concurrency lets us actually
    *reach* that ceiling instead of paying ``interval + latency`` per request
    sequentially.
  * The 1000-objects-per-request limit is respected by keeping ``perPage`` under
    a safe ceiling for each query shape (see the processor/scraper modules) and
    by retrying with a halved ``perPage`` if the API ever reports a complexity
    error.
"""

from __future__ import annotations

import asyncio
import os
import threading
from typing import Any, Awaitable, Callable, Iterable, Sequence

import httpx

from startgg_rate_gate import (
    acquire_slot_async,
    is_complexity_error,
    is_likely_rate_limit_error,
    is_transient_server_error,
    sleep_after_429_async,
)

API_URL = "https://api.start.gg/gql/alpha"

# start.gg hard limit; queries must keep perPage * objects_per_node under this.
MAX_OBJECTS_PER_REQUEST = 1000


def _env_int(name: str, default: int, *, lo: int = 1, hi: int = 1000) -> int:
    raw = os.environ.get(name)
    if not raw:
        return default
    try:
        return max(lo, min(hi, int(raw)))
    except ValueError:
        return default


# Max simultaneous in-flight HTTP requests. The rate gate (≤~70/min) is the real
# throughput throttle; this bounds how many heavy queries start.gg processes at
# once. Their backend sheds load (intermittent "internal" errors) when hit with
# many expensive sets queries in parallel, so 6 is a conservative default that
# still saturates the rate limit on light queries. Tune via STARTGG_CONCURRENCY.
DEFAULT_CONCURRENCY = _env_int("STARTGG_CONCURRENCY", 6, lo=1, hi=64)


class AsyncStartGGClient:
    """Async GraphQL client that shares the global rate budget and retries."""

    def __init__(
        self,
        auth_token: str,
        *,
        client: httpx.AsyncClient | None = None,
        timeout: float = 60.0,
        max_retries: int = 30,
        max_transient_retries: int = 6,
    ):
        if not auth_token:
            raise ValueError("Missing STARTGG_API_KEY")
        self.auth_token = auth_token
        # Rate-limit (429) waits use the full budget — those are legitimate, must
        # honor them. Transient server hiccups (5xx, start.gg "internal" errors,
        # JSON/connection blips) get a *small, bounded* budget with short backoff
        # so one flaky heavy query can't stall the whole batch for minutes; we'd
        # rather give up and let the event be retried cheaply on the next run.
        self.max_retries = max_retries
        self.max_transient_retries = max_transient_retries
        self._timeout = timeout
        self._client = client
        self._owns_client = client is None

    async def __aenter__(self) -> "AsyncStartGGClient":
        if self._client is None:
            limits = httpx.Limits(
                max_connections=DEFAULT_CONCURRENCY * 2,
                max_keepalive_connections=DEFAULT_CONCURRENCY * 2,
            )
            self._client = httpx.AsyncClient(timeout=self._timeout, limits=limits)
        return self

    async def __aexit__(self, *exc: Any) -> None:
        if self._owns_client and self._client is not None:
            await self._client.aclose()
            self._client = None

    async def gql(self, query: str, variables: dict[str, Any]) -> dict[str, Any]:
        """
        POST one GraphQL operation, retrying transient failures. Each attempt
        first acquires a shared rate slot, so concurrent callers never exceed the
        per-key budget collectively.
        """
        assert self._client is not None, "Use AsyncStartGGClient as an async context manager"
        headers = {
            "Authorization": f"Bearer {self.auth_token}",
            "Content-Type": "application/json",
        }
        last_error: Exception | None = None
        transient_tries = 0  # 5xx / internal / network blips (bounded, short backoff)

        async def _transient_backoff() -> bool:
            """Sleep briefly; return False once the bounded budget is exhausted."""
            nonlocal transient_tries
            transient_tries += 1
            if transient_tries > self.max_transient_retries:
                return False
            await asyncio.sleep(min(15.0, 2.0 * transient_tries))
            return True

        for attempt in range(self.max_retries):
            await acquire_slot_async()
            try:
                resp = await self._client.post(
                    API_URL, headers=headers, json={"query": query, "variables": variables}
                )
            except (httpx.ConnectError, httpx.ReadTimeout, httpx.ConnectTimeout,
                    httpx.RemoteProtocolError, httpx.PoolTimeout, httpx.HTTPError) as e:
                last_error = e
                if not await _transient_backoff():
                    break
                continue

            if resp.status_code == 429:
                last_error = RuntimeError("429 Rate limited")
                await sleep_after_429_async(attempt, resp)
                continue
            if 500 <= resp.status_code < 600:
                last_error = RuntimeError(f"HTTP {resp.status_code}")
                if not await _transient_backoff():
                    break
                continue
            if resp.status_code >= 400:
                resp.raise_for_status()

            try:
                payload = resp.json()
            except ValueError as e:
                last_error = e
                if not await _transient_backoff():
                    break
                continue

            if payload.get("success") is False:
                msg = str(payload.get("message") or payload)
                if is_likely_rate_limit_error(payload, msg) or "timeout" in msg.lower():
                    last_error = RuntimeError(msg)
                    await sleep_after_429_async(attempt, resp)
                    continue
                raise RuntimeError(f"start.gg API error: {msg}")
            if "errors" in payload:
                msg = str(payload["errors"])
                if is_likely_rate_limit_error(payload, msg) or "timeout" in msg.lower():
                    last_error = RuntimeError(msg)
                    await sleep_after_429_async(attempt, resp)
                    continue
                if is_complexity_error(msg):
                    # Surface a typed error so paginators can retry at lower perPage.
                    raise ComplexityLimitError(
                        f"Query exceeded 1000-object limit (perPage="
                        f"{(variables or {}).get('perPage')}): {msg}"
                    )
                if is_transient_server_error(payload, msg):
                    # start.gg internal hiccup on a heavy query: short, bounded retry.
                    last_error = RuntimeError(msg)
                    if not await _transient_backoff():
                        break
                    continue
                raise RuntimeError(f"GraphQL errors: {payload['errors']}")
            return payload

        raise RuntimeError(
            f"start.gg request failed (transient_tries={transient_tries}): {last_error}"
        ) from last_error


class ComplexityLimitError(RuntimeError):
    """Raised when start.gg rejects a query for exceeding the 1000-object limit."""


async def gather_bounded(
    factories: Sequence[Callable[[], Awaitable[Any]]],
    *,
    concurrency: int,
    return_exceptions: bool = False,
) -> list[Any]:
    """
    Run coroutine *factories* with at most ``concurrency`` in flight, preserving
    input order in the returned results. We accept factories (zero-arg callables
    returning a coroutine) rather than coroutines so nothing is scheduled until a
    semaphore slot is free.
    """
    sem = asyncio.Semaphore(max(1, concurrency))
    results: list[Any] = [None] * len(factories)

    async def _run(idx: int, factory: Callable[[], Awaitable[Any]]) -> None:
        async with sem:
            results[idx] = await factory()

    tasks = [asyncio.create_task(_run(i, f)) for i, f in enumerate(factories)]
    gathered = await asyncio.gather(*tasks, return_exceptions=return_exceptions)
    if return_exceptions:
        # Propagate per-task exceptions into the positional results slot.
        for i, g in enumerate(gathered):
            if isinstance(g, Exception):
                results[i] = g
    return results


async def paginate_async(
    client: AsyncStartGGClient,
    query: str,
    base_variables: dict[str, Any],
    *,
    extract_block: Callable[[dict[str, Any]], dict[str, Any]],
    per_page: int,
    concurrency: int = DEFAULT_CONCURRENCY,
    page_var: str = "page",
    per_page_var: str = "perPage",
    max_pages: int | None = None,
    min_per_page: int = 8,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """
    Fetch every page of a paginated connection concurrently.

    ``extract_block(payload)`` must return the connection object that has
    ``{"pageInfo": {"totalPages": N}, "nodes": [...]}``. Page 1 is fetched first
    to learn ``totalPages``; remaining pages are then fetched with bounded
    concurrency. On a complexity error, ``per_page`` is halved and the fetch
    restarts (down to ``min_per_page``).

    Returns ``(all_nodes, first_page_block)`` — the second value lets callers read
    ids/metadata that live alongside the connection (e.g. ``event.id``).
    """
    cur_per_page = per_page
    while True:
        try:
            first = await client.gql(
                query, {**base_variables, page_var: 1, per_page_var: cur_per_page}
            )
            first_block = extract_block(first) or {}
            nodes = list(first_block.get("nodes", []) or [])
            page_info = first_block.get("pageInfo") or {}
            total_pages = int(page_info.get("totalPages") or 1)
            if max_pages is not None:
                total_pages = min(total_pages, max_pages)
            if total_pages <= 1:
                return nodes, first_block

            async def _fetch_page(p: int) -> list[dict[str, Any]]:
                payload = await client.gql(
                    query, {**base_variables, page_var: p, per_page_var: cur_per_page}
                )
                block = extract_block(payload) or {}
                return list(block.get("nodes", []) or [])

            rest = await gather_bounded(
                [(lambda p=p: _fetch_page(p)) for p in range(2, total_pages + 1)],
                concurrency=concurrency,
            )
            for page_nodes in rest:
                nodes.extend(page_nodes)
            return nodes, first_block
        except ComplexityLimitError:
            if cur_per_page <= min_per_page:
                raise
            cur_per_page = max(min_per_page, cur_per_page // 2)
            continue


# --- Sync bridge ----------------------------------------------------------


def run_async(coro: Awaitable[Any]) -> Any:
    """
    Run an async coroutine to completion from synchronous code.

    Works from the threaded job system. If the current thread already has a
    running event loop (rare here, but possible inside async frameworks), the
    coroutine is run on a dedicated background thread with its own loop so we
    never raise "asyncio.run() cannot be called from a running event loop".
    """
    try:
        running = asyncio.get_running_loop()
    except RuntimeError:
        running = None

    if running is None:
        return asyncio.run(coro)

    result: dict[str, Any] = {}

    def _runner() -> None:
        loop = asyncio.new_event_loop()
        try:
            asyncio.set_event_loop(loop)
            result["value"] = loop.run_until_complete(coro)
        except Exception as e:  # noqa: BLE001 - re-raised on the caller thread
            result["error"] = e
        finally:
            loop.close()

    t = threading.Thread(target=_runner, daemon=True)
    t.start()
    t.join()
    if "error" in result:
        raise result["error"]
    return result.get("value")
