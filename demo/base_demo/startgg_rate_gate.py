"""
Process-wide coordination for start.gg GraphQL HTTP requests.

Official limits (per API key): max 80 requests per 60 seconds, max 1000 objects
per request. See: https://developer.start.gg/docs/rate-limits

All scraper / processor / recent_events / elo_calculator traffic in one Python
process shares this gate so parallel threads (e.g. UI jobs) do not exceed the
per-key budget and trigger HTTP 429.
"""

from __future__ import annotations

import random
import threading
import time
from collections import deque
from email.utils import parsedate_to_datetime
from typing import Any

# Sliding window: allow at most this many requests in any 60s window (margin under 80).
_MAX_REQUESTS_PER_60S = 70

# Minimum spacing between consecutive emits (steady-state ~75/min max).
_MIN_INTERVAL_SEC = 60.0 / 75.0

_WINDOW_SEC = 60.0

_lock = threading.Lock()
_times: deque[float] = deque()
_last_emit_monotonic: float = 0.0


def acquire_slot() -> None:
    """
    Block until this process may send one GraphQL request without exceeding
    our conservative interpretation of start.gg's rolling limit.
    """
    global _last_emit_monotonic
    while True:
        sleep_for = 0.0
        with _lock:
            now = time.monotonic()
            while _times and _times[0] <= now - _WINDOW_SEC:
                _times.popleft()
            if len(_times) >= _MAX_REQUESTS_PER_60S:
                sleep_for = max(sleep_for, _times[0] + _WINDOW_SEC - now)
            gap = _MIN_INTERVAL_SEC - (now - _last_emit_monotonic)
            if gap > 0:
                sleep_for = max(sleep_for, gap)
            if sleep_for <= 0.001:
                _last_emit_monotonic = time.monotonic()
                _times.append(_last_emit_monotonic)
                return
        time.sleep(min(sleep_for + random.uniform(0, 0.15), 5.0))


def sleep_after_429(attempt: int, response: Any) -> None:
    """Honor Retry-After when present; otherwise exponential backoff with cap."""
    delay: float | None = None
    if response is not None:
        raw = response.headers.get("Retry-After")
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
    time.sleep(delay)


def is_likely_rate_limit_error(payload: dict | None, errors_text: str) -> bool:
    blob = (errors_text or "").lower()
    if "rate limit" in blob or "too many requests" in blob or "429" in blob:
        return True
    if payload and payload.get("success") is False:
        msg = str(payload.get("message") or "").lower()
        if "rate limit" in msg or "too many" in msg:
            return True
    return False
