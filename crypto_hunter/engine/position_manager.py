"""
Managed position lifecycle: peak-ROI tracking, stepped trailing stop, failsafe, exit
classification and persistence.

Every tick (``push.ticker`` / ``push.tickers``) for a symbol with an open managed position:
  1. ROI from fair / last (per ``peak_price_source``) -> update peak ROI
  2. ladder -> candidate stop ROI; ratchet (never lower)
  3. if the stop ROI moved to a new step -> ONE ``change_plan_price`` request, persist
  4. failsafe: if price is beyond stop/TP for > grace seconds, market close

All state lives in SQLite (``positions`` table) so a restart resumes exactly where it stopped.
"""
from __future__ import annotations

import asyncio
import json
import logging
import time
from dataclasses import dataclass, field, asdict
from typing import Any, Awaitable, Callable, Dict, List, Optional, Tuple

from ..config import BotConfig
from ..exchange.models import Contract, ExchangePosition, MexcAPIError
from ..persistence.db import Database
from ..risk.roi import price_for_roi, ratchet, roi_pct, trailing_stop_roi, unrealized_pnl
from .executor import OrderExecutor

log = logging.getLogger("ch.positions")

EventCB = Callable[[str, str, str, Optional[str], Optional[Dict[str, Any]]], Awaitable[None]]


@dataclass
class ManagedPosition:
    symbol: str
    side: str
    entry_price: float
    vol: float
    contract_size: float
    leverage: int
    margin: float
    atr: float
    initial_stop_price: float
    stop_price: float
    tp_price: float
    peak_price: float
    opened_at: float
    position_id: Optional[int] = None
    stop_roi: Optional[float] = None
    peak_roi: float = 0.0
    stop_plan_order_id: Optional[str] = None
    entry_order_id: Optional[str] = None
    sl_plan_order_id: Optional[str] = None
    tp_plan_order_id: Optional[str] = None
    signal_json: Optional[str] = None
    status: str = "open"
    updated_at: float = field(default_factory=time.time)
    # runtime only
    last_price: float = 0.0
    fair_price: float = 0.0
    breach_since: Optional[float] = None
    tp_breach_since: Optional[float] = None
    _lock: asyncio.Lock = field(default_factory=asyncio.Lock, repr=False, compare=False)
    _updating: bool = False
    _last_persist: float = 0.0

    @property
    def key(self) -> Tuple[str, str]:
        return (self.symbol, self.side)

    def roi(self, price: float) -> float:
        return roi_pct(self.side, self.entry_price, price, self.leverage)

    def row(self) -> Dict[str, Any]:
        d = {k: v for k, v in asdict(self).items()
             if k not in ("last_price", "fair_price", "breach_since", "tp_breach_since", "_lock", "_updating", "_last_persist")}
        return d

    @classmethod
    def from_row(cls, r: Dict[str, Any]) -> "ManagedPosition":
        fields = {k: r[k] for k in r if k in cls.__dataclass_fields__ and not k.startswith("_")}
        return cls(**fields)

    def to_dict(self, price: Optional[float] = None) -> Dict[str, Any]:
        px = price or self.fair_price or self.last_price or self.entry_price
        cur_roi = self.roi(px)
        d = self.row()
        d.pop("signal_json", None)
        d.update({
            "mark_price": px, "last_price": self.last_price or px, "roi": cur_roi,
            "unrealized_pnl": unrealized_pnl(self.side, self.entry_price, px, self.vol, self.contract_size),
            "stop_roi_effective": self.stop_roi if self.stop_roi is not None else self.roi(self.initial_stop_price),
            "trailing_active": self.stop_roi is not None,
            "notional": self.vol * self.contract_size * self.entry_price,
            "age_sec": time.time() - self.opened_at,
        })
        return d


class PositionManager:
    def __init__(self, db: Database, executor: OrderExecutor, cfg: BotConfig, emit: EventCB):
        self.db = db
        self.exe = executor
        self.cfg = cfg
        self.emit = emit
        self.positions: Dict[Tuple[str, str], ManagedPosition] = {}
        self.contracts: Dict[str, Contract] = {}
        self.on_closed: Optional[Callable[[Dict[str, Any]], Awaitable[None]]] = None
        self.on_changed: Optional[Callable[[ManagedPosition], Awaitable[None]]] = None
        self._closing: set = set()

    def update_config(self, cfg: BotConfig) -> None:
        self.cfg = cfg

    # ------------------------------------------------------------- persistence
    async def load(self) -> None:
        rows = await self.db.list_positions()
        self.positions = {}
        for r in rows:
            p = ManagedPosition.from_row(r)
            self.positions[p.key] = p
        if rows:
            log.info("Restored %d managed positions from DB: %s", len(rows), [p.symbol for p in self.positions.values()])

    async def persist(self, p: ManagedPosition) -> None:
        p._last_persist = time.time()
        await self.db.upsert_position(p.row())

    def symbols(self) -> set:
        return {p.symbol for p in self.positions.values()}

    def count(self) -> int:
        return len(self.positions)

    def has(self, symbol: str, side: Optional[str] = None) -> bool:
        if side:
            return (symbol, side) in self.positions
        return any(k[0] == symbol for k in self.positions)

    # ------------------------------------------------------------------ open
    async def register(self, p: ManagedPosition) -> None:
        self.positions[p.key] = p
        await self.persist(p)
        if self.on_changed:
            await self.on_changed(p)

    # ------------------------------------------------------------------ ticks
    async def on_price(self, symbol: str, last: float, fair: float) -> None:
        for key in [k for k in self.positions if k[0] == symbol]:
            p = self.positions.get(key)
            if p is None or p.status != "open":
                continue
            p.last_price, p.fair_price = last, fair or last
            src = self.cfg.exits.peak_price_source
            if src == "fair":
                track = fair or last
            elif src == "last":
                track = last
            else:  # both -> the more favourable of the two for peak tracking
                track = max(last, fair) if p.side == "long" else min(last, fair) if fair else last
            roi_now = p.roi(track)
            changed = False
            if roi_now > p.peak_roi:
                p.peak_roi = roi_now
                p.peak_price = track
                changed = True
            # --- trailing ladder
            new_stop_roi = ratchet(p.stop_roi, trailing_stop_roi(p.peak_roi, self.cfg.exits))
            if new_stop_roi is not None and new_stop_roi != p.stop_roi and not p._updating:
                asyncio.create_task(self._apply_trail(p, new_stop_roi))
            elif changed and time.time() - p._last_persist > 2.0:
                asyncio.create_task(self.persist(p))  # throttle peak-ROI writes to one per 2 s
            # --- failsafe on the *mark* price (what the exchange triggers on) and last
            if self.cfg.exits.software_failsafe:
                await self._failsafe(p, fair or last, last)

    async def _apply_trail(self, p: ManagedPosition, new_stop_roi: float) -> None:
        async with p._lock:
            if p.status != "open" or (p.stop_roi is not None and new_stop_roi <= p.stop_roi):
                return
            contract = self.contracts.get(p.symbol)
            if contract is None:
                return
            p._updating = True
            try:
                new_stop = price_for_roi(p.side, p.entry_price, new_stop_roi, p.leverage)
                res = await self.exe.update_stop(p.symbol, p.side, p.stop_plan_order_id, new_stop, p.tp_price,
                                                 contract, p.position_id, p.vol, p.leverage)
                prev = p.stop_roi
                p.stop_roi = new_stop_roi
                p.stop_price = res.get("stop_price", new_stop)
                if res.get("stop_plan_order_id") is not None or "stop_plan_order_id" in res:
                    p.stop_plan_order_id = res.get("stop_plan_order_id") or p.stop_plan_order_id
                if res.get("sl_plan_order_id"):
                    p.sl_plan_order_id, p.tp_plan_order_id = res.get("sl_plan_order_id"), res.get("tp_plan_order_id")
                await self.persist(p)
                await self.emit("info", "trail", f"{p.symbol} {p.side} stop -> {new_stop_roi:.0f}% ROI @ {p.stop_price} "
                                f"(peak {p.peak_roi:.1f}%)", p.symbol,
                                {"prev_stop_roi": prev, "stop_roi": new_stop_roi, "stop_price": p.stop_price, "peak_roi": p.peak_roi})
                if self.on_changed:
                    await self.on_changed(p)
            except MexcAPIError as exc:
                await self.emit("error", "trail_error", f"{p.symbol}: failed to move stop ({exc.message})", p.symbol)
            except Exception as exc:  # pragma: no cover
                log.exception("trail update failed for %s", p.symbol)
                await self.emit("error", "trail_error", f"{p.symbol}: {exc}", p.symbol)
            finally:
                p._updating = False

    async def _failsafe(self, p: ManagedPosition, mark: float, last: float) -> None:
        ex = self.cfg.exits
        tol = ex.failsafe_breach_pct / 100.0
        if p.side == "long":
            stop_breached = mark <= p.stop_price * (1 - tol) and last <= p.stop_price * (1 - tol)
            tp_breached = mark >= p.tp_price * (1 + tol)
        else:
            stop_breached = mark >= p.stop_price * (1 + tol) and last >= p.stop_price * (1 + tol)
            tp_breached = mark <= p.tp_price * (1 - tol)
        now = time.time()
        p.breach_since = (p.breach_since or now) if stop_breached else None
        p.tp_breach_since = (p.tp_breach_since or now) if tp_breached else None
        if p.breach_since and now - p.breach_since >= ex.failsafe_grace_sec:
            await self.force_close(p, "FAILSAFE", mark)
        elif p.tp_breach_since and now - p.tp_breach_since >= ex.failsafe_grace_sec:
            await self.force_close(p, "TP", mark)

    # ----------------------------------------------------------------- closing
    async def force_close(self, p: ManagedPosition, reason: str, ref_price: float) -> None:
        if p.key in self._closing:
            return
        self._closing.add(p.key)
        try:
            contract = self.contracts.get(p.symbol)
            if contract is None:
                return
            p.status = "closing"
            await self.emit("warn", "close", f"{p.symbol} {p.side}: software close ({reason}) @ {ref_price}", p.symbol)
            fill = await self.exe.close_position(p.symbol, p.side, p.vol, ref_price, p.position_id, contract)
            exit_px = fill.avg_price or ref_price
            await self.exe.cancel_protection(p.symbol, p.position_id, [p.sl_plan_order_id or "", p.tp_plan_order_id or ""])
            await self.finalize(p, exit_px, reason)
        except MexcAPIError as exc:
            p.status = "open"
            await self.emit("error", "close_error", f"{p.symbol}: close failed ({exc.message})", p.symbol)
        except Exception as exc:  # pragma: no cover
            p.status = "open"
            log.exception("force_close failed for %s", p.symbol)
            await self.emit("error", "close_error", f"{p.symbol}: {exc}", p.symbol)
        finally:
            self._closing.discard(p.key)

    def classify_exit(self, p: ManagedPosition, exit_price: float) -> str:
        roi = p.roi(exit_price)
        ex = self.cfg.exits
        if roi >= ex.tp_roi * 0.9:
            return "TP"
        if p.stop_roi is not None:
            return "TRAIL"
        return "SL"

    async def finalize(self, p: ManagedPosition, exit_price: Optional[float], reason: Optional[str],
                       realised: Optional[float] = None) -> Dict[str, Any]:
        """Record the closed trade, drop the managed position, fire callbacks."""
        self.positions.pop(p.key, None)
        await self.db.delete_position(p.symbol, p.side)
        exit_px = exit_price or p.fair_price or p.last_price or p.entry_price
        reason = reason or self.classify_exit(p, exit_px)
        pnl = realised if realised is not None else unrealized_pnl(p.side, p.entry_price, exit_px, p.vol, p.contract_size)
        roi = (pnl / p.margin * 100.0) if p.margin > 0 else p.roi(exit_px)
        trade = {
            "symbol": p.symbol, "side": p.side, "entry_price": p.entry_price, "exit_price": exit_px, "vol": p.vol,
            "contract_size": p.contract_size, "leverage": p.leverage, "margin": p.margin, "pnl": pnl, "roi": roi,
            "peak_roi": p.peak_roi, "reason": reason, "opened_at": p.opened_at, "closed_at": time.time(),
            "signal_json": p.signal_json,
        }
        trade["id"] = await self.db.insert_trade(trade)
        level = "info" if pnl >= 0 else "warn"
        await self.emit(level, "exit", f"{p.symbol} {p.side} closed [{reason}] pnl {pnl:+.4f} USDT ({roi:+.1f}% ROI, peak {p.peak_roi:.1f}%)",
                        p.symbol, {"trade": trade})
        if self.on_closed:
            await self.on_closed(trade)
        return trade

    # ------------------------------------------------------------ reconciliation
    async def reconcile(self, exchange_positions: List[ExchangePosition], adopt: Callable[[ExchangePosition], Awaitable[None]]) -> None:
        """Compare exchange state with managed state.

        * managed but gone on exchange  -> closed by exchange TP/SL/liquidation -> finalize
        * on exchange but not managed   -> adopt (attach protective orders & trailing)
        * both                          -> refresh vol / avg price / positionId
        """
        live: Dict[Tuple[str, str], ExchangePosition] = {}
        for ep in exchange_positions:
            if ep.hold_vol > 0:
                live[(ep.symbol, ep.side)] = ep
        for key, p in list(self.positions.items()):
            ep = live.get(key)
            if ep is None:
                if p.status == "closing" or key in self._closing:
                    continue
                if time.time() - p.opened_at < 5:
                    continue  # just opened; exchange listing may lag
                exit_px, realised = await self._lookup_close(p)
                await self.finalize(p, exit_px, None, realised)
            else:
                changed = False
                if p.position_id != ep.position_id:
                    p.position_id = ep.position_id; changed = True
                if abs(p.vol - ep.hold_vol) > 1e-9:
                    p.vol = ep.hold_vol; changed = True
                if ep.hold_avg_price > 0 and abs(p.entry_price - ep.hold_avg_price) / ep.hold_avg_price > 1e-6 and p.stop_roi is None:
                    # exchange's own avg price is authoritative; re-derive TP / initial SL only before trailing starts
                    p.entry_price = ep.hold_avg_price
                    p.tp_price = price_for_roi(p.side, p.entry_price, self.cfg.exits.tp_roi, p.leverage)
                    changed = True
                if changed:
                    await self.persist(p)
        for key, ep in live.items():
            if key not in self.positions:
                await adopt(ep)

    async def _lookup_close(self, p: ManagedPosition) -> Tuple[Optional[float], Optional[float]]:
        try:
            hist = await self.exe.rest.get_history_positions(p.symbol, page_size=10)
        except MexcAPIError:
            return None, None
        for h in hist:
            if p.position_id and str(h.get("positionId")) == str(p.position_id):
                close_px = float(h.get("closeAvgPrice") or 0) or None
                realised = float(h.get("realised")) if h.get("realised") is not None else None
                return close_px, realised
        return None, None

    async def on_private_position(self, data: Dict[str, Any]) -> None:
        """``push.personal.position`` handler – fast close detection."""
        try:
            ep = ExchangePosition.from_api(data)
        except Exception:
            return
        p = self.positions.get((ep.symbol, ep.side))
        if p is None:
            return
        if ep.hold_vol <= 0 and p.status == "open" and p.key not in self._closing:
            await asyncio.sleep(0.5)  # let history endpoint catch up
            exit_px, realised = await self._lookup_close(p)
            await self.finalize(p, exit_px, None, realised)
        elif ep.hold_vol > 0:
            if p.position_id != ep.position_id or abs(p.vol - ep.hold_vol) > 1e-9:
                p.position_id, p.vol = ep.position_id, ep.hold_vol
                await self.persist(p)
