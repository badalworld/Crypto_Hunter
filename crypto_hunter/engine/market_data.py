"""
Market data cache.

* ``KlineStore``  – per-symbol OHLCV arrays for the signal timeframe (5m) and the 1h bias
                    timeframe. Backfilled via REST, kept live via ``push.kline`` pushes,
                    and re-validated via REST right after each candle close.
* ``TickerCache`` – last / fair / bid / ask per symbol from ``push.tickers`` / ``push.ticker``.
"""
from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

import numpy as np

from ..exchange.models import Ticker
from ..exchange.rest import MexcFuturesREST
from ..strategy.divergence import Bars

log = logging.getLogger("ch.marketdata")

INTERVAL_SEC = {"Min1": 60, "Min5": 300, "Min15": 900, "Min30": 1800, "Min60": 3600, "Hour4": 14400, "Day1": 86400}


@dataclass
class _Series:
    interval: str
    time: List[int] = field(default_factory=list)
    open: List[float] = field(default_factory=list)
    high: List[float] = field(default_factory=list)
    low: List[float] = field(default_factory=list)
    close: List[float] = field(default_factory=list)
    vol: List[float] = field(default_factory=list)
    fetched_at: float = 0.0

    def to_bars(self, closed_only: bool, now: Optional[float] = None) -> Bars:
        n = len(self.time)
        if closed_only and n:
            now = now or time.time()
            sec = INTERVAL_SEC.get(self.interval, 300)
            while n > 0 and self.time[n - 1] + sec > now + 1:
                n -= 1
        return Bars(
            time=np.asarray(self.time[:n], dtype=float), open=np.asarray(self.open[:n], dtype=float),
            high=np.asarray(self.high[:n], dtype=float), low=np.asarray(self.low[:n], dtype=float),
            close=np.asarray(self.close[:n], dtype=float), volume=np.asarray(self.vol[:n], dtype=float),
        )

    def upsert(self, t: int, o: float, h: float, l: float, c: float, v: float) -> bool:
        """Insert/replace a bar.  Returns True when a *new* bar was appended (previous one closed)."""
        if self.time and t == self.time[-1]:
            self.open[-1], self.high[-1], self.low[-1], self.close[-1], self.vol[-1] = o, h, l, c, v
            return False
        if self.time and t < self.time[-1]:
            # late update for an older bar
            try:
                i = self.time.index(t)
                self.open[i], self.high[i], self.low[i], self.close[i], self.vol[i] = o, h, l, c, v
            except ValueError:
                pass
            return False
        self.time.append(t); self.open.append(o); self.high.append(h); self.low.append(l); self.close.append(c); self.vol.append(v)
        return True

    def trim(self, max_bars: int) -> None:
        if len(self.time) > max_bars:
            k = len(self.time) - max_bars
            for arr in (self.time, self.open, self.high, self.low, self.close, self.vol):
                del arr[:k]


class KlineStore:
    def __init__(self, rest: MexcFuturesREST, interval: str = "Min5", history_bars: int = 400):
        self.rest = rest
        self.interval = interval
        self.history_bars = history_bars
        self._series: Dict[Tuple[str, str], _Series] = {}
        self._locks: Dict[Tuple[str, str], asyncio.Lock] = {}

    def _lock(self, key: Tuple[str, str]) -> asyncio.Lock:
        if key not in self._locks:
            self._locks[key] = asyncio.Lock()
        return self._locks[key]

    async def _fetch(self, symbol: str, interval: str, bars: int) -> _Series:
        sec = INTERVAL_SEC.get(interval, 300)
        end = int(time.time())
        start = end - sec * (bars + 2)
        data = await self.rest.get_kline(symbol, interval, start, end)
        s = _Series(interval=interval)
        for t, o, h, l, c, v in zip(data.get("time", []), data.get("open", []), data.get("high", []),
                                    data.get("low", []), data.get("close", []), data.get("vol", [])):
            s.upsert(int(t), float(o), float(h), float(l), float(c), float(v))
        s.fetched_at = time.time()
        return s

    async def ensure(self, symbol: str, interval: Optional[str] = None, min_bars: int = 50,
                     max_age: Optional[float] = None) -> Optional[Bars]:
        """Return closed bars, back-filling via REST if missing/stale."""
        interval = interval or self.interval
        key = (symbol, interval)
        sec = INTERVAL_SEC.get(interval, 300)
        max_age = max_age if max_age is not None else sec
        async with self._lock(key):
            s = self._series.get(key)
            if s is None or len(s.time) < min_bars or time.time() - s.fetched_at > max_age:
                bars = self.history_bars if interval == self.interval else max(min_bars, 260)
                s = await self._fetch(symbol, interval, bars)
                if len(s.time) == 0:
                    return None
                self._series[key] = s
            return s.to_bars(closed_only=True)

    async def refresh(self, symbol: str, interval: Optional[str] = None, tail: int = 3) -> Optional[Bars]:
        """Re-validate the last ``tail`` bars from REST (called right after candle close)."""
        interval = interval or self.interval
        key = (symbol, interval)
        async with self._lock(key):
            s = self._series.get(key)
            if s is None or len(s.time) < 50:
                s = await self._fetch(symbol, interval, self.history_bars)
                self._series[key] = s
            else:
                fresh = await self._fetch(symbol, interval, tail)
                for i in range(len(fresh.time)):
                    s.upsert(fresh.time[i], fresh.open[i], fresh.high[i], fresh.low[i], fresh.close[i], fresh.vol[i])
                s.fetched_at = time.time()
                s.trim(self.history_bars + 10)
            return s.to_bars(closed_only=True)

    def bars(self, symbol: str, interval: Optional[str] = None, closed_only: bool = True) -> Optional[Bars]:
        s = self._series.get((symbol, interval or self.interval))
        return s.to_bars(closed_only) if s else None

    def on_ws_kline(self, symbol: str, interval: str, data: Dict[str, Any]) -> bool:
        """Apply a ``push.kline`` payload: {symbol, interval, t, o, c, h, l, a, q}. Returns True if a bar closed."""
        key = (symbol, interval)
        s = self._series.get(key)
        if s is None:
            return False
        try:
            t = int(data["t"]); o = float(data["o"]); h = float(data["h"]); l = float(data["l"]); c = float(data["c"])
            v = float(data.get("q", data.get("v", 0)) or 0)
        except (KeyError, TypeError, ValueError):
            return False
        new_bar = s.upsert(t, o, h, l, c, v)
        if new_bar:
            s.trim(self.history_bars + 10)
        return new_bar

    def drop(self, symbol: str) -> None:
        for key in [k for k in self._series if k[0] == symbol]:
            self._series.pop(key, None)

    def symbols(self) -> List[str]:
        return sorted({k[0] for k in self._series if k[1] == self.interval})


class TickerCache:
    def __init__(self) -> None:
        self._t: Dict[str, Ticker] = {}
        self.updated_at: float = 0.0

    def update_many(self, tickers: List[Ticker]) -> None:
        for t in tickers:
            self._t[t.symbol] = t
        self.updated_at = time.time()

    def update_ws(self, d: Dict[str, Any]) -> Optional[Ticker]:
        sym = d.get("symbol")
        if not sym:
            return None
        prev = self._t.get(sym)
        last = float(d.get("lastPrice") or (prev.last if prev else 0) or 0)
        t = Ticker(
            symbol=sym, last=last,
            fair=float(d.get("fairPrice") or (prev.fair if prev else last) or last),
            index=float(d.get("indexPrice") or (prev.index if prev else last) or last),
            bid=float(d.get("bid1") or (prev.bid if prev else last) or last),
            ask=float(d.get("ask1") or (prev.ask if prev else last) or last),
            volume24=float(d.get("volume24") or (prev.volume24 if prev else 0) or 0),
            amount24=float(d.get("amount24") or (prev.amount24 if prev else 0) or 0),
            rise_fall_rate=float(d.get("riseFallRate") or (prev.rise_fall_rate if prev else 0) or 0),
            ts=float(d.get("timestamp") or time.time() * 1000) / 1000.0,
        )
        self._t[sym] = t
        self.updated_at = time.time()
        return t

    def get(self, symbol: str) -> Optional[Ticker]:
        return self._t.get(symbol)

    def all(self) -> List[Ticker]:
        return list(self._t.values())
