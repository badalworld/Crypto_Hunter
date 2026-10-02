"""
Per-endpoint sliding-window rate limiter.

MEXC Futures documents most endpoints at *20 requests / 2 seconds* per endpoint
(some are 1/s).  We budget every endpoint group at ``fraction`` (default 95 %) of its
documented limit and share the budget between the scanner, the signal engine, the
executor and the trailing-stop manager – whichever coroutine asks first gets the slot,
the others await.
"""
from __future__ import annotations

import asyncio
import math
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Deque, Dict, Tuple


@dataclass
class _Window:
    limit: int
    period: float
    hits: Deque[float] = field(default_factory=deque)
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)


# Documented limits: (requests, seconds).  Unknown endpoints fall back to DEFAULT.
MEXC_FUTURES_LIMITS: Dict[str, Tuple[int, float]] = {
    "contract/detail": (1, 5.0),
    "contract/ticker": (20, 2.0),
    "contract/kline": (20, 2.0),
    "contract/depth": (20, 2.0),
    "contract/ping": (20, 2.0),
    "private/account/assets": (20, 2.0),
    "private/position/open_positions": (20, 2.0),
    "private/position/list/history_positions": (20, 2.0),
    "private/position/change_leverage": (20, 2.0),
    "private/position/position_mode": (20, 2.0),
    "private/order/create": (20, 2.0),
    "private/order/cancel": (20, 2.0),
    "private/order/cancel_all": (20, 2.0),
    "private/order/get": (20, 2.0),
    "private/order/list/open_orders": (20, 2.0),
    "private/stoporder/open_orders": (20, 2.0),
    "private/stoporder/list/orders": (20, 2.0),
    "private/stoporder/change_plan_price": (20, 2.0),
    "private/stoporder/cancel": (20, 2.0),
    "private/stoporder/cancel_all": (20, 2.0),
    "private/planorder/place/v2": (20, 2.0),
    "private/planorder/cancel": (20, 2.0),
    "private/planorder/list/orders": (20, 2.0),
}
DEFAULT_LIMIT: Tuple[int, float] = (20, 2.0)


class RateLimiter:
    def __init__(self, fraction: float = 0.95, limits: Dict[str, Tuple[int, float]] | None = None):
        self.fraction = fraction
        self._limits = dict(limits or MEXC_FUTURES_LIMITS)
        self._windows: Dict[str, _Window] = {}
        self._global = _Window(limit=max(1, int(500 * fraction)), period=10.0)  # account-wide guard
        self.stats: Dict[str, int] = {}

    def _window(self, key: str) -> _Window:
        w = self._windows.get(key)
        if w is None:
            n, period = self._limits.get(key, DEFAULT_LIMIT)
            w = _Window(limit=max(1, math.floor(n * self.fraction)), period=period)
            self._windows[key] = w
        return w

    @staticmethod
    def key_for(path: str) -> str:
        p = path.split("?")[0].strip("/")
        if p.startswith("api/v1/"):
            p = p[len("api/v1/"):]
        # Collapse path parameters (kline/{symbol}, order/get/{id})
        for prefix in ("contract/kline", "private/order/get", "contract/depth"):
            if p.startswith(prefix):
                return prefix
        return p

    async def acquire(self, path: str) -> None:
        key = self.key_for(path)
        await self._wait(self._global)
        await self._wait(self._window(key))
        self.stats[key] = self.stats.get(key, 0) + 1

    async def _wait(self, w: _Window) -> None:
        async with w.lock:
            while True:
                now = time.monotonic()
                while w.hits and now - w.hits[0] >= w.period:
                    w.hits.popleft()
                if len(w.hits) < w.limit:
                    w.hits.append(now)
                    return
                await asyncio.sleep(max(0.005, w.period - (now - w.hits[0]) + 0.002))

    def usage(self) -> Dict[str, Dict[str, float]]:
        now = time.monotonic()
        out = {}
        for k, w in self._windows.items():
            live = sum(1 for t in w.hits if now - t < w.period)
            out[k] = {"used": live, "budget": w.limit, "period": w.period, "pct": round(100 * live / w.limit, 1)}
        return out
