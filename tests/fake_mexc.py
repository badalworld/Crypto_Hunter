"""
Minimal in-process emulation of the MEXC Futures REST API used by the integration tests.

It validates request signatures exactly like MEXC does, so a passing test proves the
client's auth/signing + payload shapes are correct.  This is a *test fixture only* –
the bot has no simulated/paper mode.
"""
from __future__ import annotations

import hashlib
import hmac
import json
import time
from typing import Any, Dict, List

from aiohttp import web

API_KEY, API_SECRET = "mx0FAKEKEY1234567890", "fakesecret1234567890abcdef"


class FakeMexc:
    def __init__(self) -> None:
        self.price = 100.0
        self.fair = 100.0
        self.orders: Dict[str, Dict[str, Any]] = {}
        self.positions: Dict[int, Dict[str, Any]] = {}
        self.stop_orders: Dict[int, Dict[str, Any]] = {}
        self.history: List[Dict[str, Any]] = []
        self.equity = 1000.0
        self.calls: List[str] = []
        self.plan_changes: List[Dict[str, Any]] = []
        self._oid = 700000
        self._pid = 1000
        self._sid = 50000
        self.leverage_calls: List[Dict[str, Any]] = []
        self.plan_orders: Dict[int, Dict[str, Any]] = {}

    # ------------------------------------------------------------ helpers
    def _auth(self, request: web.Request, body: str) -> None:
        if not request.path.startswith("/api/v1/private/"):
            return
        key, ts, sig = request.headers.get("ApiKey"), request.headers.get("Request-Time"), request.headers.get("Signature")
        assert key == API_KEY, "bad api key"
        assert abs(int(ts) - time.time() * 1000) < 10_000, "bad req time"
        param = body if request.method == "POST" else request.query_string
        expect = hmac.new(API_SECRET.encode(), f"{key}{ts}{param}".encode(), hashlib.sha256).hexdigest()
        assert sig == expect, f"bad signature for {request.path}: {param!r}"

    @staticmethod
    def ok(data: Any = None) -> web.Response:
        return web.json_response({"success": True, "code": 0, "data": data})

    def set_price(self, p: float) -> None:
        self.price = self.fair = p
        self._check_triggers()

    def _check_triggers(self) -> None:
        for sid, so in list(self.stop_orders.items()):
            if so["state"] != 1:
                continue
            pos = self.positions.get(int(so["positionId"]))
            if not pos or pos["holdVol"] <= 0:
                continue
            long = pos["positionType"] == 1
            sl, tp = so.get("stopLossPrice") or 0, so.get("takeProfitPrice") or 0
            hit = None
            if long and sl and self.fair <= sl: hit = ("SL", sl)
            if long and tp and self.fair >= tp: hit = ("TP", tp)
            if not long and sl and self.fair >= sl: hit = ("SL", sl)
            if not long and tp and self.fair <= tp: hit = ("TP", tp)
            if hit:
                so["state"], so["triggerSide"] = 3, (2 if hit[0] == "SL" else 1)
                self._close_position(pos, hit[1])

    def _close_position(self, pos: Dict[str, Any], px: float) -> None:
        qty = pos["holdVol"] * 0.01  # contractSize 0.01
        pnl = (px - pos["holdAvgPrice"]) * qty if pos["positionType"] == 1 else (pos["holdAvgPrice"] - px) * qty
        pos["holdVol"], pos["state"] = 0, 3
        self.equity += pnl
        self.history.insert(0, {**pos, "closeAvgPrice": px, "realised": pnl, "closeVol": pos["openVol"]})

    # ------------------------------------------------------------ handlers
    async def handle(self, request: web.Request) -> web.Response:
        body = await request.text()
        self.calls.append(f"{request.method} {request.path}")
        try:
            self._auth(request, body)
        except AssertionError as exc:
            return web.json_response({"success": False, "code": 602, "message": str(exc)})
        p = request.path
        j = json.loads(body) if body else {}
        if p == "/api/v1/contract/ping":
            return self.ok(int(time.time() * 1000))
        if p == "/api/v1/contract/detail":
            return self.ok([{"symbol": "TEST_USDT", "baseCoin": "TEST", "quoteCoin": "USDT", "settleCoin": "USDT",
                             "contractSize": 0.01, "priceUnit": 0.01, "volUnit": 1, "minVol": 1, "maxVol": 1e7,
                             "priceScale": 2, "volScale": 0, "maxLeverage": 100, "minLeverage": 1,
                             "takerFeeRate": 0.0006, "makerFeeRate": 0.0002, "state": 0, "apiAllowed": True}])
        if p == "/api/v1/contract/ticker":
            t = {"symbol": "TEST_USDT", "lastPrice": self.price, "fairPrice": self.fair, "indexPrice": self.fair,
                 "bid1": self.price * 0.9999, "ask1": self.price * 1.0001, "volume24": 1e7, "amount24": 5e8,
                 "riseFallRate": 0.05, "timestamp": int(time.time() * 1000)}
            return self.ok(t if request.query.get("symbol") else [t])
        if p.startswith("/api/v1/contract/kline/"):
            n = 300
            now = int(time.time()) // 300 * 300
            times = [now - 300 * (n - 1 - i) for i in range(n)]
            closes = [self.price] * n
            return self.ok({"time": times, "open": closes, "close": closes, "high": [c * 1.005 for c in closes],
                            "low": [c * 0.995 for c in closes], "vol": [1000] * n, "amount": [1000 * self.price] * n})
        if p == "/api/v1/private/account/assets":
            margin = sum(x["im"] for x in self.positions.values() if x["holdVol"] > 0)
            return self.ok([{"currency": "USDT", "equity": self.equity, "availableBalance": self.equity - margin,
                             "cashBalance": self.equity, "frozenBalance": 0, "positionMargin": margin, "unrealized": 0}])
        if p == "/api/v1/private/position/position_mode":
            return self.ok(1)
        if p == "/api/v1/private/position/change_leverage":
            self.leverage_calls.append(j)
            return self.ok()
        if p == "/api/v1/private/position/open_positions":
            return self.ok([x for x in self.positions.values() if x["holdVol"] > 0])
        if p == "/api/v1/private/position/list/history_positions":
            return self.ok(self.history)
        if p == "/api/v1/private/order/create":
            return self._create_order(j)
        if p.startswith("/api/v1/private/order/get/"):
            return self.ok(self.orders.get(p.rsplit("/", 1)[1]))
        if p == "/api/v1/private/stoporder/open_orders":
            return self.ok([s for s in self.stop_orders.values() if s["state"] == 1])
        if p == "/api/v1/private/stoporder/change_plan_price":
            so = self.stop_orders.get(int(j["stopPlanOrderId"]))
            if not so or so["state"] != 1:
                return web.json_response({"success": False, "code": 2005, "message": "stop order not found"})
            so["stopLossPrice"], so["takeProfitPrice"] = j.get("stopLossPrice"), j.get("takeProfitPrice")
            self.plan_changes.append(dict(j))
            self._check_triggers()
            return self.ok()
        if p == "/api/v1/private/stoporder/cancel_all":
            for so in self.stop_orders.values():
                if str(so["positionId"]) == str(j.get("positionId")) or j.get("symbol") == so["symbol"]:
                    so["state"] = 2
            return self.ok()
        if p == "/api/v1/private/planorder/place/v2":
            self._sid += 1
            self.plan_orders[self._sid] = {"id": self._sid, **j, "state": 1}
            return self.ok(self._sid)
        if p == "/api/v1/private/planorder/list/orders":
            return self.ok([o for o in self.plan_orders.values() if o["state"] == 1])
        if p == "/api/v1/private/planorder/cancel":
            for item in j:
                o = self.plan_orders.get(int(item["orderId"]))
                if o:
                    o["state"] = 2
            return self.ok()
        return web.json_response({"success": False, "code": 404, "message": f"unknown {p}"}, status=404)

    def _create_order(self, j: Dict[str, Any]) -> web.Response:
        self._oid += 1
        oid = str(self._oid)
        side, vol = int(j["side"]), float(j["vol"])
        assert j.get("type") in (3, 5)
        fill_px = self.price * (1.0002 if side in (1, 2) else 0.9998)
        if side in (1, 3):  # open
            self._pid += 1
            pid = self._pid
            lev = int(j["leverage"])
            im = vol * 0.01 * fill_px / lev
            self.positions[pid] = {"positionId": pid, "symbol": j["symbol"], "positionType": 1 if side == 1 else 2,
                                   "openType": j["openType"], "state": 1, "holdVol": vol, "openVol": vol,
                                   "holdAvgPrice": fill_px, "openAvgPrice": fill_px, "im": im, "oim": im,
                                   "leverage": lev, "realised": 0, "liquidatePrice": 0}
            self.equity -= vol * 0.01 * fill_px * 0.0006
            if j.get("stopLossPrice") or j.get("takeProfitPrice"):
                self._sid += 1
                self.stop_orders[self._sid] = {"id": self._sid, "orderId": 0, "symbol": j["symbol"], "positionId": pid,
                                               "stopLossPrice": j.get("stopLossPrice"), "takeProfitPrice": j.get("takeProfitPrice"),
                                               "state": 1, "triggerSide": 0, "positionType": 1 if side == 1 else 2, "vol": vol}
        else:  # close
            pid = int(j.get("positionId") or 0)
            pos = self.positions.get(pid) or next((x for x in self.positions.values() if x["symbol"] == j["symbol"] and x["holdVol"] > 0), None)
            assert pos is not None, "no position to close"
            pid = pos["positionId"]
            self._close_position(pos, fill_px)
        self.orders[oid] = {"orderId": oid, "symbol": j["symbol"], "positionId": pid, "price": fill_px, "vol": vol,
                            "side": side, "state": 3, "dealAvgPrice": fill_px, "dealVol": vol, "leverage": j.get("leverage"),
                            "externalOid": j.get("externalOid")}
        return self.ok({"orderId": oid, "ts": int(time.time() * 1000)})


async def start_fake(port: int = 0):
    fake = FakeMexc()
    app = web.Application()
    app.router.add_route("*", "/{tail:.*}", fake.handle)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", port)
    await site.start()
    actual_port = site._server.sockets[0].getsockname()[1]  # type: ignore[attr-defined]
    return fake, runner, f"http://127.0.0.1:{actual_port}"
