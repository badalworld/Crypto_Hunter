"""
FastAPI application: REST control plane + WebSocket stream for the dashboard.

Security
* Optional bearer token (``CH_DASHBOARD_TOKEN``) protects every /api route and the WS.
* Credentials are accepted only over POST /api/credentials and immediately encrypted;
  responses only ever contain a masked key.
"""
from __future__ import annotations

import asyncio
import json
import logging
import math
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

from fastapi import Depends, FastAPI, HTTPException, Query, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from ..config import AppSettings, BotConfig
from ..engine.bot import Bot
from ..persistence.db import Database

log = logging.getLogger("ch.api")
STATIC_DIR = Path(__file__).parent / "static"


def _clean(obj: Any) -> Any:
    """Make payloads JSON safe (inf/nan -> null)."""
    if isinstance(obj, float):
        return obj if math.isfinite(obj) else None
    if isinstance(obj, dict):
        return {k: _clean(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_clean(v) for v in obj]
    return obj


class Hub:
    def __init__(self) -> None:
        self.clients: Set[WebSocket] = set()
        self._lock = asyncio.Lock()

    async def broadcast(self, msg: Dict[str, Any]) -> None:
        if not self.clients:
            return
        payload = json.dumps(_clean(msg))
        dead = []
        for ws in list(self.clients):
            try:
                await ws.send_text(payload)
            except Exception:
                dead.append(ws)
        for ws in dead:
            self.clients.discard(ws)


class CredentialsIn(BaseModel):
    api_key: str = Field(min_length=8, max_length=256)
    api_secret: str = Field(min_length=8, max_length=256)


class ConfigPatch(BaseModel):
    patch: Dict[str, Any]


def create_app(settings: AppSettings, cfg: BotConfig, db: Database) -> FastAPI:
    hub = Hub()
    bot = Bot(settings, cfg, db, hub.broadcast)
    app = FastAPI(title="Crypto Hunter", version="1.0.0", docs_url=None, redoc_url=None)
    app.state.bot = bot

    # ------------------------------------------------------------------ auth
    async def auth(request: Request) -> None:
        if not settings.dashboard_token:
            return
        tok = request.headers.get("authorization", "").removeprefix("Bearer ").strip() or request.query_params.get("token")
        if tok != settings.dashboard_token:
            raise HTTPException(401, "invalid dashboard token")

    # ------------------------------------------------------------- lifecycle
    @app.on_event("startup")
    async def _startup() -> None:
        stored = await db.load_settings()
        if stored:
            try:
                bot.apply_config(cfg.merged(stored))
            except Exception as exc:
                log.warning("Stored settings invalid, ignoring: %s", exc)
        await bot.boot()

    @app.on_event("shutdown")
    async def _shutdown() -> None:
        await bot.shutdown()
        await db.close()

    # ---------------------------------------------------------------- static
    app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

    @app.get("/")
    async def index() -> FileResponse:
        return FileResponse(STATIC_DIR / "index.html")

    # ------------------------------------------------------------------- api
    @app.get("/api/status", dependencies=[Depends(auth)])
    async def status() -> JSONResponse:
        return JSONResponse(_clean(await bot.snapshot()))

    @app.get("/api/config", dependencies=[Depends(auth)])
    async def get_config() -> Dict[str, Any]:
        return {"config": bot.cfg.model_dump(mode="json"), "schema": BotConfig.model_json_schema(),
                "auth_required": bool(settings.dashboard_token)}

    @app.put("/api/config", dependencies=[Depends(auth)])
    async def put_config(body: ConfigPatch) -> Dict[str, Any]:
        try:
            new = await bot.update_config(body.patch)
        except Exception as exc:
            raise HTTPException(400, str(exc))
        return {"config": new.model_dump(mode="json")}

    @app.post("/api/credentials", dependencies=[Depends(auth)])
    async def set_credentials(body: CredentialsIn) -> Dict[str, Any]:
        try:
            return await bot.set_credentials(body.api_key, body.api_secret)
        except ValueError as exc:
            raise HTTPException(400, str(exc))

    @app.delete("/api/credentials", dependencies=[Depends(auth)])
    async def delete_credentials() -> Dict[str, Any]:
        await bot.clear_credentials()
        return {"ok": True}

    @app.post("/api/bot/start", dependencies=[Depends(auth)])
    async def start() -> Dict[str, Any]:
        try:
            await bot.start()
        except ValueError as exc:
            raise HTTPException(400, str(exc))
        return {"state": bot.state}

    @app.post("/api/bot/stop", dependencies=[Depends(auth)])
    async def stop() -> Dict[str, Any]:
        await bot.stop("user")
        return {"state": bot.state}

    @app.post("/api/positions/close_all", dependencies=[Depends(auth)])
    async def close_all() -> Dict[str, Any]:
        return {"closed": await bot.close_all()}

    @app.post("/api/positions/{symbol}/{side}/close", dependencies=[Depends(auth)])
    async def close_one(symbol: str, side: str) -> Dict[str, Any]:
        ok = await bot.close_one(symbol, side)
        if not ok:
            raise HTTPException(404, "position not managed")
        return {"ok": True}

    @app.get("/api/positions", dependencies=[Depends(auth)])
    async def positions() -> List[Dict[str, Any]]:
        return _clean(bot.positions_dict())

    @app.get("/api/trades", dependencies=[Depends(auth)])
    async def trades(limit: int = Query(200, le=2000)) -> List[Dict[str, Any]]:
        return _clean(await db.list_trades(limit))

    @app.get("/api/metrics", dependencies=[Depends(auth)])
    async def metrics() -> Dict[str, Any]:
        return _clean(await bot.metrics())

    @app.get("/api/equity", dependencies=[Depends(auth)])
    async def equity(hours: float = Query(168, le=24 * 60)) -> Dict[str, Any]:
        return _clean({"curve": await db.equity_curve(since=time.time() - hours * 3600),
                       "projection": await bot.projection()})

    @app.get("/api/events", dependencies=[Depends(auth)])
    async def events(limit: int = Query(100, le=1000)) -> List[Dict[str, Any]]:
        return _clean(await db.recent_events(limit))

    @app.get("/api/watchlist", dependencies=[Depends(auth)])
    async def watchlist() -> Dict[str, Any]:
        return _clean({"watchlist": [w.to_dict() for w in bot.watchlist],
                       "scan": [w.to_dict() for w in bot.scanner.last_scan[:60]], "ts": bot.scanner.last_scan_ts})

    @app.get("/api/signals", dependencies=[Depends(auth)])
    async def signals() -> List[Dict[str, Any]]:
        return _clean(bot.last_signals)

    @app.get("/healthz")
    async def healthz() -> Dict[str, Any]:
        return {"ok": True, "state": bot.state}

    # -------------------------------------------------------------- websocket
    @app.websocket("/ws")
    async def ws_endpoint(ws: WebSocket, token: Optional[str] = None) -> None:
        if settings.dashboard_token and token != settings.dashboard_token:
            await ws.close(code=4401)
            return
        await ws.accept()
        hub.clients.add(ws)
        try:
            await ws.send_text(json.dumps(_clean(await bot.snapshot())))
            await ws.send_text(json.dumps(_clean({"type": "bootstrap", "trades": await db.list_trades(200),
                                                  "events": await db.recent_events(100),
                                                  "equity": await db.equity_curve(since=time.time() - 7 * 86400),
                                                  "projection": await bot.projection(),
                                                  "config": bot.cfg.model_dump(mode="json")})))
            while True:
                try:
                    raw = await asyncio.wait_for(ws.receive_text(), timeout=30)
                except asyncio.TimeoutError:
                    await ws.send_text('{"type":"ping"}')
                    continue
                if raw == "ping":
                    await ws.send_text('{"type":"pong"}')
        except WebSocketDisconnect:
            pass
        except Exception as exc:  # pragma: no cover
            log.debug("ws client error: %s", exc)
        finally:
            hub.clients.discard(ws)

    return app
