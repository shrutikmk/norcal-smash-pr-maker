"""
Process-wide coordination for start.gg GraphQL HTTP requests.

Official limits (per API key): max 80 requests per 60 seconds, max 1000 objects
per request. See: https://developer.start.gg/docs/rate-limits

All scraper / processor / recent_events / elo_calculator traffic in one Python
process shares this gate so parallel threads (e.g. UI jobs) AND asyncio tasks
(the concurrent scraper) do not exceed the per-key budget and trigger HTTP 429.

The gate enforces two limits at once:
  * a rolling 60s window cap (``_MAX_REQUESTS_PER_60S``), and
  * a minimum spacing between consecutive emits (``_MIN_INTERVAL_SEC``).

Both the synchronous ``acquire_slot()`` (used by ``requests``-based code and the
background-warming threads) and the asyncio ``acquire_slot_async()`` (used by the
concurrent ``startgg_async`` engine) share the *same* global state, so a single
budget is split fairly across every worker regardless of execution model.

Tuning (env vars, read once at import):
  * ``STARTGG_MAX_RPM``       — rolling-window cap. Default 70 (margin under 80).
  * ``STARTGG_MIN_INTERVAL``  — min seconds between emits. Default 60/75 ≈ 0.8s.
"""

from __future__ import annotations

import asyncio
import os
import random
import threading
import time
from collections import deque
from email.utils import parsedate_to_datetime
from typing import Any


def _env_float(name: str, default: float) -> float:
    raw = os.environ.get(name)
    if not raw:
        return default
    try:
        val = float(raw)
        return val if val > 0 else default
    except ValueError:
        return default


# Sliding window: allow at most this many requests in any 60s window.
# Keep margin under the hard 80/60s limit so retries/clock-skew never push over.
_MAX_REQUESTS_PER_60S = max(1, int(_env_float("STARTGG_MAX_RPM", 70)))

# Minimum spacing between consecutive emits (steady-state ~75/min max by default).
_MIN_INTERVAL_SEC = _env_float("STARTGG_MIN_INTERVAL", 60.0 / 75.0)

_WINDOW_SEC = 60.0

_lock = threading.Lock()
_times: deque[float] = deque()
_last_emit_monotonic: float = 0.0

# Monotonic counter of total slots acquired (i.e. GraphQL requests emitted) in
# this process. Used to measure API usage per scrape for cache-efficiency
# verification; never reset so callers should snapshot before/after.
_total_requests: int = 0

# Observability counters (never reset; snapshot before/after a phase).
_total_429s: int = 0
_total_backoff_sec: float = 0.0
_total_throttle_sleeps: int = 0


def _reserve_slot_nonblocking() -> float:
    """
    Try to claim one emit slot.

    Returns 0.0 and records the emit if a slot is available right now; otherwise
    returns how many seconds the caller should wait before retrying. Holds the
    lock only for bookkeeping (never across a sleep), so it is safe to call from
    both threads and the asyncio event loop.
    """
    global _last_emit_monotonic, _total_requests, _total_throttle_sleeps
    with _lock:
        now = time.monotonic()
        while _times and _times[0] <= now - _WINDOW_SEC:
            _times.popleft()
        sleep_for = 0.0
        if len(_times) >= _MAX_REQUESTS_PER_60S:
            sleep_for = max(sleep_for, _times[0] + _WINDOW_SEC - now)
        gap = _MIN_INTERVAL_SEC - (now - _last_emit_monotonic)
        if gap > 0:
            sleep_for = max(sleep_for, gap)
        if sleep_for <= 0.001:
            _last_emit_monotonic = time.monotonic()
            _times.append(_last_emit_monotonic)
            _total_requests += 1
            return 0.0
        _total_throttle_sleeps += 1
        return sleep_for


def acquire_slot() -> None:
    """
    Block (synchronously) until this process may send one GraphQL request without
    exceeding our conservative interpretation of start.gg's rolling limit.
    """
    while True:
        sleep_for = _reserve_slot_nonblocking()
        if sleep_for <= 0.0:
            return
        time.sleep(min(sleep_for + random.uniform(0, 0.15), 5.0))


async def acquire_slot_async() -> None:
    """
    Async counterpart to :func:`acquire_slot`. Awaits (without blocking the event
    loop) until a request slot is free, sharing the same global budget as the
    synchronous path so concurrent async workers + warming threads stay under the
    per-key limit collectively.
    """
    while True:
        sleep_for = _reserve_slot_nonblocking()
        if sleep_for <= 0.0:
            return
        await asyncio.sleep(min(sleep_for + random.uniform(0, 0.15), 5.0))


def get_request_count() -> int:
    """Total GraphQL request slots acquired since process start (monotonic)."""
    with _lock:
        return _total_requests


def get_metrics() -> dict[str, float]:
    """
    Snapshot of cumulative gate metrics for observability. Callers diff two
    snapshots to attribute requests / 429s / backoff time to one phase.
    """
    with _lock:
        return {
            "requests": float(_total_requests),
            "rate_limit_hits": float(_total_429s),
            "backoff_sec": _total_backoff_sec,
            "throttle_sleeps": float(_total_throttle_sleeps),
            "max_rpm": float(_MAX_REQUESTS_PER_60S),
            "min_interval_sec": _MIN_INTERVAL_SEC,
        }


def _record_backoff(delay: float) -> None:
    global _total_429s, _total_backoff_sec
    with _lock:
        _total_429s += 1
        _total_backoff_sec += float(delay)


def _compute_429_delay(attempt: int, response: Any) -> float:
    """Honor Retry-After when present; otherwise exponential backoff with cap."""
    delay: float | None = None
    if response is not None:
        try:
            raw = response.headers.get("Retry-After")
        except AttributeError:
            raw = None
        if raw:
            try:
                delay = float(raw)
            except ValueError:
                try:
                    dt = parsedate_to_datetime(raw)
                    if dt is not None:
                        delay = max(0.0, (dt.timestamp() - time.time()))
                except (TypeError, ValueError, OSError):
                    delay = None
    if delay is None:
        delay = min(120.0, 15.0 * (2 ** min(attempt, 6)))
    delay = min(180.0, max(5.0, delay))
    delay += random.uniform(0, 2.0)
    return delay


def sleep_after_429(attempt: int, response: Any) -> None:
    """Synchronously back off after a 429 / rate-limit error (records metrics)."""
    delay = _compute_429_delay(attempt, response)
    _record_backoff(delay)
    time.sleep(delay)


async def sleep_after_429_async(attempt: int, response: Any) -> None:
    """Async back off after a 429 / rate-limit error (records metrics)."""
    delay = _compute_429_delay(attempt, response)
    _record_backoff(delay)
    await asyncio.sleep(delay)


def is_likely_rate_limit_error(payload: dict | None, errors_text: str) -> bool:
    blob = (errors_text or "").lower()
    if "rate limit" in blob or "too many requests" in blob or "429" in blob:
        return True
    if payload and payload.get("success") is False:
        msg = str(payload.get("message") or "").lower()
        if "rate limit" in msg or "too many" in msg:
            return True
    return False


def is_complexity_error(errors_text: str) -> bool:
    """True when start.gg rejected a query for exceeding the 1000-object limit."""
    blob = (errors_text or "").lower()
    return "complexity" in blob or "1000 objects" in blob or "query complexity" in blob


def is_transient_server_error(payload: dict | None, errors_text: str) -> bool:
    """
    True for start.gg's intermittent backend failures that succeed on retry.

    Heavy/deeply-nested queries (e.g. an event's sets with players+scores at a
    high perPage, or many such queries in flight) sometimes return a GraphQL-level
    error like ``{"message": "An unknown error has occurred",
    "extensions": {"category": "internal"}}`` instead of data. These are transient
    server hiccups, not client errors, so callers should back off and retry rather
    than abort the whole event.
    """
    blob = (errors_text or "").lower()
    signals = (
        "an unknown error has occurred",
        "something went wrong",
        "service unavailable",
        "internal server error",
        '"category": "internal"',
        "'category': 'internal'",
    )
    if any(sig in blob for sig in signals):
        return True
    if payload and isinstance(payload.get("errors"), list):
        for err in payload["errors"]:
            ext = (err or {}).get("extensions") or {}
            if str(ext.get("category", "")).lower() == "internal":
                return True
    return False
