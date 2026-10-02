"""
MEXC Futures public WebSocket (``wss://contract.mexc.com/edge``).

Channels used
* ``sub.tickers``  – all contract tickers (last/fair/index price, 24h volume) ~1 s
* ``sub.ticker``   – per-symbol ticker for symbols with open positions (faster peak-ROI tracking)
* ``sub.kline``    – live 5m candle per watched symbol

Reconnects with exponential backoff and re-subscribes everything after reconnect.
Server requires a ``{"method":"ping"}`` at least every ~20 s – we send every 10 s.
"""
from __future__ import annotations

import asyncio
import json
import logging
import random
import time
import zlib
from typing import Any, Awaitable, Callable, Dict, Optional, Set

import websockets
from websockets.exceptions import ConnectionClosed

log = logging.getLogger("ch.ws.public")

TickerCB = Callable[[Dict[str, Any]], Awaitable[None] | None]
KlineCB = Callable[[str, str, Dict[str, Any]], Awaitable[None] | None]


class PublicWS:
    def __init__(self, url: str, on_ticker: TickerCB, on_kline: KlineCB, interval: str = "Min5"):
        self.url = url
        self.on_ticker = on_ticker
        self.on_kline = on_kline
        self.interval = interval
        self._ws: Optional[websockets.WebSocketClientProtocol] = None
        self._task: Optional[asyncio.Task] = None
        self._ping_task: Optional[asyncio.Task] = None
        self._kline_symbols: Set[str] = set()
        self._ticker_symbols: Set[str] = set()
        self._stopping = False
        self.connected = False
        self.last_message_ts: float = 0.0
        self.reconnects = 0

    # ---------------------------------------------------------------- lifecycle
    def start(self) -> None:
        if not self._task or self._task.done():
            self._stopping = False
            self._task = asyncio.create_task(self._run(), name="public-ws")

    async def stop(self) -> None:
        self._stopping = True
        for t in (self._ping_task, self._task):
            if t and not t.done():
                t.cancel()
        if self._ws:
            try:
                await self._ws.close()
            except Exception:
                pass
        self.connected = False

    # ---------------------------------------------------------------- subscriptions
    async def set_kline_symbols(self, symbols: Set[str]) -> None:
        add = symbols - self._kline_symbols
        rem = self._kline_symbols - symbols
        self._kline_symbols = set(symbols)
        for s in rem:
            await self._send({"method": "unsub.kline", "param": {"symbol": s, "interval": self.interval}})
        for s in add:
            await self._send({"method": "sub.kline", "param": {"symbol": s, "interval": self.interval}})

    async def set_ticker_symbols(self, symbols: Set[str]) -> None:
        add = symbols - self._ticker_symbols
        rem = self._ticker_symbols - symbols
        self._ticker_symbols = set(symbols)
        for s in rem:
            await self._send({"method": "unsub.ticker", "param": {"symbol": s}})
        for s in add:
            await self._send({"method": "sub.ticker", "param": {"symbol": s}})

    async def _resubscribe(self) -> None:
        await self._send({"method": "sub.tickers", "param": {}})
        for s in self._kline_symbols:
            await self._send({"method": "sub.kline", "param": {"symbol": s, "interval": self.interval}})
        for s in self._ticker_symbols:
            await self._send({"method": "sub.ticker", "param": {"symbol": s}})

    async def _send(self, msg: Dict[str, Any]) -> None:
        if self._ws and self.connected:
            try:
                await self._ws.send(json.dumps(msg))
            except Exception as exc:  # pragma: no cover - network
                log.debug("ws send failed: %s", exc)

    # ---------------------------------------------------------------- main loop
    async def _run(self) -> None:
        backoff = 1.0
        while not self._stopping:
            try:
                async with websockets.connect(self.url, ping_interval=None, max_size=16 * 1024 * 1024,
                                              open_timeout=10, close_timeout=3) as ws:
                    self._ws = ws
                    self.connected = True
                    backoff = 1.0
                    log.info("Public WS connected")
                    self._ping_task = asyncio.create_task(self._pinger())
                    await self._resubscribe()
                    async for raw in ws:
                        self.last_message_ts = time.time()
                        await self._handle(raw)
            except asyncio.CancelledError:
                break
            except (ConnectionClosed, OSError, asyncio.TimeoutError, Exception) as exc:
                if self._stopping:
                    break
                self.reconnects += 1
                log.warning("Public WS disconnected (%s) – reconnecting in %.1fs", exc, backoff)
            finally:
                self.connected = False
                if self._ping_task:
                    self._ping_task.cancel()
            await asyncio.sleep(backoff + random.uniform(0, 0.5))
            backoff = min(backoff * 2, 30.0)

    async def _pinger(self) -> None:
        try:
            while True:
                await asyncio.sleep(10)
                await self._send({"method": "ping"})
                if self.last_message_ts and time.time() - self.last_message_ts > 45 and self._ws:
                    log.warning("Public WS stale (>45s) – forcing reconnect")
                    await self._ws.close()
        except asyncio.CancelledError:
            pass

    async def _handle(self, raw: str | bytes) -> None:
        if isinstance(raw, (bytes, bytearray)):
            try:
                raw = zlib.decompress(raw, 16 + zlib.MAX_WBITS)
            except zlib.error:
                try:
                    raw = zlib.decompress(raw)
                except zlib.error:
                    return
        try:
            msg = json.loads(raw)
        except json.JSONDecodeError:
            return
        channel = msg.get("channel", "")
        data = msg.get("data")
        if channel == "push.tickers" and isinstance(data, list):
            for t in data:
                await _maybe_await(self.on_ticker(t))
        elif channel == "push.ticker" and isinstance(data, dict):
            await _maybe_await(self.on_ticker(data))
        elif channel == "push.kline" and isinstance(data, dict):
            await _maybe_await(self.on_kline(data.get("symbol") or msg.get("symbol", ""), data.get("interval", self.interval), data))
        elif channel == "pong":
            return
        elif channel.startswith("rs.error"):
            log.warning("Public WS error: %s", msg)


async def _maybe_await(x: Any) -> None:
    if asyncio.iscoroutine(x) or isinstance(x, asyncio.Future):
        await x
