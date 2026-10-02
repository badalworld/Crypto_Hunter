"""
Order routing.

Entry   : market (type 5) or IOC (type 3) order with ``stopLossPrice`` / ``takeProfitPrice``
          attached, so the exchange holds the protective orders from the first millisecond.
Fill    : polled via ``GET order/get/{id}`` (plus private-WS pushes) – returns avg fill price,
          filled vol and positionId.
TP/SL   : the attached TP/SL becomes a *position TP/SL plan order* (``stoporder/open_orders``).
          Trailing updates call ``stoporder/change_plan_price`` on that id – one request per
          ratchet step.  If MEXC did not create a plan order (edge cases), we fall back to
          reduce-only trigger orders (``planorder/place/v2``) and cancel/replace them.
Close   : market order on the close side (4 = close long, 2 = close short) with positionId;
          ``reduceOnly=true`` in one-way mode.  Close orders can never increase exposure.
"""
from __future__ import annotations

import asyncio
import logging
import time
import uuid
from dataclasses import dataclass
from typing import Any, Dict, Optional

from ..config import BotConfig
from ..exchange.models import Contract, MexcAPIError
from ..exchange.rest import MexcFuturesREST

log = logging.getLogger("ch.executor")

SIDE_OPEN = {"long": 1, "short": 3}
SIDE_CLOSE = {"long": 4, "short": 2}
POSITION_TYPE = {"long": 1, "short": 2}
ORDER_TYPE = {"market": 5, "ioc": 3}
STATE_FILLED, STATE_CANCELED, STATE_INVALID = 3, 4, 5


class PositionGone(Exception):
    """Raised when a stop update is attempted for a position the exchange no longer holds."""


@dataclass
class FillResult:
    order_id: str
    avg_price: float
    filled_vol: float
    position_id: Optional[int]
    state: int
    raw: Dict[str, Any]


class OrderExecutor:
    def __init__(self, rest: MexcFuturesREST, cfg: BotConfig):
        self.rest = rest
        self.cfg = cfg
        self.position_mode: int = 1  # 1 hedge, 2 one-way
        self._leverage_set: Dict[str, float] = {}

    def update_config(self, cfg: BotConfig) -> None:
        self.cfg = cfg

    async def detect_position_mode(self) -> int:
        try:
            self.position_mode = await self.rest.get_position_mode()
        except MexcAPIError as exc:
            log.warning("position_mode query failed (%s) – assuming hedge mode", exc)
            self.position_mode = 1
        return self.position_mode

    # ------------------------------------------------------------------ leverage
    async def ensure_leverage(self, symbol: str, side: str, leverage: int) -> None:
        key = f"{symbol}:{side}:{leverage}:{self.cfg.open_type_code}"
        if time.time() - self._leverage_set.get(key, 0) < 3600:
            return
        try:
            await self.rest.change_leverage(symbol, leverage, self.cfg.open_type_code, POSITION_TYPE[side])
            self._leverage_set[key] = time.time()
        except MexcAPIError as exc:
            # Common benign cases: leverage already set / open position exists with same leverage
            log.info("change_leverage %s %s x%d: %s", symbol, side, leverage, exc.message)
            self._leverage_set[key] = time.time()

    # --------------------------------------------------------------------- entry
    async def open_position(self, symbol: str, side: str, vol: float, ref_price: float, leverage: int,
                            stop_loss: float, take_profit: float, contract: Contract) -> FillResult:
        await self.ensure_leverage(symbol, side, leverage)
        ext = f"CH{uuid.uuid4().hex[:18]}"
        order_type = ORDER_TYPE[self.cfg.execution.entry_order_type]
        price = ref_price
        if order_type == 3:  # IOC needs an aggressive limit price
            slip = 0.003
            price = contract.round_price(ref_price * (1 + slip) if side == "long" else ref_price * (1 - slip))
        t0 = time.perf_counter()
        res = await self.rest.create_order(
            symbol=symbol, vol=vol, side=SIDE_OPEN[side], order_type=order_type, open_type=self.cfg.open_type_code,
            price=price, leverage=leverage, external_oid=ext,
            stop_loss_price=contract.round_price(stop_loss, "down" if side == "long" else "up"),
            take_profit_price=contract.round_price(take_profit, "up" if side == "long" else "down"),
            loss_trend=self.cfg.trend_code, profit_trend=self.cfg.trend_code,
            position_mode=self.position_mode,
        )
        order_id = str(res.get("orderId"))
        log.info("ENTRY %s %s vol=%s sent in %.0fms -> order %s", side.upper(), symbol, vol,
                 (time.perf_counter() - t0) * 1000, order_id)
        return await self.await_fill(order_id)

    async def await_fill(self, order_id: str, timeout: float = 8.0) -> FillResult:
        deadline = time.time() + timeout
        last: Dict[str, Any] = {}
        delay = 0.15
        while time.time() < deadline:
            try:
                last = await self.rest.get_order(order_id)
            except MexcAPIError as exc:
                log.debug("get_order %s: %s", order_id, exc)
            state = int(last.get("state", 0) or 0)
            deal_vol = float(last.get("dealVol", 0) or 0)
            if state in (STATE_FILLED, STATE_CANCELED, STATE_INVALID) or (state == 2 and deal_vol > 0 and time.time() > deadline - timeout / 2):
                break
            await asyncio.sleep(delay)
            delay = min(delay * 1.5, 1.0)
        return FillResult(
            order_id=order_id,
            avg_price=float(last.get("dealAvgPrice", 0) or 0),
            filled_vol=float(last.get("dealVol", 0) or 0),
            position_id=int(last["positionId"]) if last.get("positionId") else None,
            state=int(last.get("state", 0) or 0),
            raw=last,
        )

    # ------------------------------------------------------------------- TP / SL
    async def find_stop_plan_order(self, symbol: str, position_id: Optional[int]) -> Optional[Dict[str, Any]]:
        try:
            orders = await self.rest.get_stop_open_orders(symbol)
        except MexcAPIError as exc:
            log.warning("stoporder/open_orders %s failed: %s", symbol, exc)
            return None
        live = [o for o in orders if int(o.get("state", 1) or 1) == 1]
        if position_id:
            for o in live:
                if str(o.get("positionId")) == str(position_id):
                    return o
        return live[-1] if live and not position_id else None

    async def update_stop(self, symbol: str, side: str, stop_plan_order_id: Optional[str], new_stop: float,
                          take_profit: float, contract: Contract, position_id: Optional[int] = None,
                          vol: float = 0.0, leverage: int = 1) -> Dict[str, Any]:
        """Move the exchange-side stop.  Returns updated ids: {stop_plan_order_id, sl_plan_order_id?}."""
        sl = contract.round_price(new_stop, "down" if side == "long" else "up")
        tp = contract.round_price(take_profit, "up" if side == "long" else "down")
        if stop_plan_order_id:
            try:
                await self.rest.change_plan_price(stop_plan_order_id, sl, tp, self.cfg.trend_code, self.cfg.trend_code)
                return {"stop_plan_order_id": stop_plan_order_id, "stop_price": sl}
            except MexcAPIError as exc:
                log.warning("change_plan_price %s failed (%s) – re-discovering plan order", symbol, exc.message)
        found = await self.find_stop_plan_order(symbol, position_id)
        if found:
            await self.rest.change_plan_price(found["id"], sl, tp, self.cfg.trend_code, self.cfg.trend_code)
            return {"stop_plan_order_id": str(found["id"]), "stop_price": sl}
        # No plan order – make sure the position still exists before arming anything new
        live = [ep for ep in await self.rest.get_open_positions(symbol) if ep.side == side and ep.hold_vol > 0]
        if not live:
            raise PositionGone(f"{symbol} {side} is no longer open")
        vol = live[0].hold_vol or vol
        # Fallback: reduce-only trigger orders
        ids = await self.place_fallback_triggers(symbol, side, vol, sl, tp, leverage, live[0].position_id or position_id)
        ids["stop_price"] = sl
        return ids

    async def place_fallback_triggers(self, symbol: str, side: str, vol: float, sl: float, tp: float,
                                      leverage: int, position_id: Optional[int]) -> Dict[str, Any]:
        """Reduce-only trigger (plan) orders when no position TP/SL exists."""
        try:
            existing = await self.rest.get_plan_orders(symbol)
            close_side = SIDE_CLOSE[side]
            stale = [o["id"] for o in existing if int(o.get("side", 0)) == close_side]
            if stale:
                await self.rest.cancel_plan_orders(symbol, stale)
        except MexcAPIError as exc:
            log.debug("plan order cleanup %s: %s", symbol, exc)
        reduce_only = True if self.position_mode == 2 else None
        trig_sl = 2 if side == "long" else 1   # long SL fires when price <= sl
        trig_tp = 1 if side == "long" else 2
        sl_id = await self.rest.place_plan_order(symbol, vol, SIDE_CLOSE[side], self.cfg.open_type_code, sl, trig_sl,
                                                 self.cfg.trend_code, leverage, order_type=5,
                                                 position_mode=self.position_mode, reduce_only=reduce_only)
        tp_id = await self.rest.place_plan_order(symbol, vol, SIDE_CLOSE[side], self.cfg.open_type_code, tp, trig_tp,
                                                 self.cfg.trend_code, leverage, order_type=5,
                                                 position_mode=self.position_mode, reduce_only=reduce_only)
        log.info("Fallback trigger orders placed for %s: SL=%s TP=%s", symbol, sl_id, tp_id)
        return {"sl_plan_order_id": str(sl_id), "tp_plan_order_id": str(tp_id), "stop_plan_order_id": None}

    # --------------------------------------------------------------------- close
    async def close_position(self, symbol: str, side: str, vol: float, ref_price: float,
                             position_id: Optional[int], contract: Contract) -> FillResult:
        reduce_only = True if self.position_mode == 2 else None
        res = await self.rest.create_order(
            symbol=symbol, vol=vol, side=SIDE_CLOSE[side], order_type=5, open_type=self.cfg.open_type_code,
            price=ref_price, position_id=position_id, position_mode=self.position_mode, reduce_only=reduce_only,
            external_oid=f"CHX{uuid.uuid4().hex[:17]}",
        )
        order_id = str(res.get("orderId"))
        log.info("CLOSE %s %s vol=%s -> order %s", side.upper(), symbol, vol, order_id)
        return await self.await_fill(order_id)

    async def cancel_protection(self, symbol: str, position_id: Optional[int], plan_ids: list[str]) -> None:
        try:
            await self.rest.cancel_all_stop_orders(symbol=symbol, position_id=position_id)
        except MexcAPIError as exc:
            log.debug("cancel stop orders %s: %s", symbol, exc)
        ids = [i for i in plan_ids if i]
        if ids:
            try:
                await self.rest.cancel_plan_orders(symbol, ids)
            except MexcAPIError as exc:
                log.debug("cancel plan orders %s: %s", symbol, exc)
