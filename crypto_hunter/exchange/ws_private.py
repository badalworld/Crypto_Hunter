"""
MEXC Futures private WebSocket – account / position / order / TP-SL pushes.

Login: ``{"method":"login","param":{"apiKey","reqTime","signature"}}`` where
``signature = HMAC_SHA256(secret, apiKey + reqTime)``.
After login the server pushes ``push.personal.position``, ``push.personal.asset``,
``push.personal.order``, ``push.personal.stop.order``, ``push.personal.stop.planorder`` …
"""
from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import logging
import random
import time
import zlib
from typing import Any, Awaitable, Callable, Dict, Optional

import websockets
from websockets.exceptions import ConnectionClosed

from ..security import Credentials

log = logging.getLogger("ch.ws.private")

EventCB = Callable[[str, Dict[str, Any]], Awaitable[None] | None]


class PrivateWS:
    def __init__(self, url: str, credentials: Credentials, on_event: EventCB):
        self.url = url
        self.creds = credentials
        self.on_event = on_event
        self._ws: Optional[websockets.WebSocketClientProtocol] = None
        self._task: Optional[asyncio.Task] = None
        self._ping_task: Optional[asyncio.Task] = None
        self._stopping = False
        self.connected = False
        self.logged_in = False
        self.last_message_ts = 0.0
        self.reconnects = 0

    def start(self) -> None:
        if not self._task or self._task.done():
            self._stopping = False
            self._task = asyncio.create_task(self._run(), name="private-ws")

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
        self.connected = self.logged_in = False

    def _login_msg(self) -> Dict[str, Any]:
        req_time = str(int(time.time() * 1000))
        sig = hmac.new(self.creds.api_secret.encode(), f"{self.creds.api_key}{req_time}".encode(), hashlib.sha256).hexdigest()
        return {"method": "login", "param": {"apiKey": self.creds.api_key, "reqTime": req_time, "signature": sig}}

    async def _run(self) -> None:
        backoff = 1.0
        while not self._stopping:
            try:
                async with websockets.connect(self.url, ping_interval=None, max_size=8 * 1024 * 1024,
                                              open_timeout=10, close_timeout=3) as ws:
                    self._ws = ws
                    self.connected = True
                    await ws.send(json.dumps(self._login_msg()))
                    self._ping_task = asyncio.create_task(self._pinger())
                    async for raw in ws:
                        self.last_message_ts = time.time()
                        if await self._handle(raw):
                            backoff = 1.0
            except asyncio.CancelledError:
                break
            except (ConnectionClosed, OSError, asyncio.TimeoutError, Exception) as exc:
                if self._stopping:
                    break
                self.reconnects += 1
                log.warning("Private WS disconnected (%s) – reconnecting in %.1fs", exc, backoff)
            finally:
                self.connected = self.logged_in = False
                if self._ping_task:
                    self._ping_task.cancel()
            await asyncio.sleep(backoff + random.uniform(0, 0.5))
            backoff = min(backoff * 2, 30.0)

    async def _pinger(self) -> None:
        try:
            while True:
                await asyncio.sleep(10)
                if self._ws:
                    await self._ws.send(json.dumps({"method": "ping"}))
                    if self.last_message_ts and time.time() - self.last_message_ts > 45:
                        log.warning("Private WS stale – forcing reconnect")
                        await self._ws.close()
        except asyncio.CancelledError:
            pass
        except Exception as exc:  # pragma: no cover
            log.debug("private pinger: %s", exc)

    async def _handle(self, raw: str | bytes) -> bool:
        if isinstance(raw, (bytes, bytearray)):
            try:
                raw = zlib.decompress(raw, 16 + zlib.MAX_WBITS)
            except zlib.error:
                try:
                    raw = zlib.decompress(raw)
                except zlib.error:
                    return False
        try:
            msg = json.loads(raw)
        except json.JSONDecodeError:
            return False
        channel = msg.get("channel", "")
        if channel == "rs.login":
            self.logged_in = True
            log.info("Private WS authenticated")
            return True
        if channel == "rs.error":
            log.error("Private WS error: %s", msg.get("data"))
            return False
        if channel == "pong":
            return False
        if channel.startswith("push.personal."):
            kind = channel[len("push.personal."):]
            data = msg.get("data") or {}
            try:
                res = self.on_event(kind, data)
                if asyncio.iscoroutine(res):
                    await res
            except Exception:  # pragma: no cover
                log.exception("private event handler failed for %s", kind)
            return True
        return False
