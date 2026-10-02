"""
Crypto Hunter orchestrator.

Tasks while RUNNING
  scanner_loop   every ``rescan_interval_sec``: rank markets, update watch-list & WS subs
  candle_loop    at every 5m close (+2 s): refresh bars, detect AO divergence, filter, enter
  sync_loop      every ``position_sync_interval_sec``: account equity + exchange positions, reconcile
  snapshot_loop  persist equity curve, push dashboard snapshot
  public WS      tickers / klines -> price ticks -> PositionManager (peak ROI, trailing, failsafe)
  private WS     position / asset / order pushes -> fast close detection, live balance
"""
from __future__ import annotations

import asyncio
import datetime as dt
import json
import logging
import math
import time
from typing import Any, Awaitable, Callable, Dict, List, Optional, Set

from ..config import AppSettings, BotConfig
from ..exchange.models import AccountAsset, Contract, ExchangePosition, MexcAPIError
from ..exchange.rate_limiter import RateLimiter
from ..exchange.rest import MexcFuturesREST
from ..exchange.ws_private import PrivateWS
from ..exchange.ws_public import PublicWS
from ..persistence.db import Database
from ..risk.roi import price_for_roi
from ..risk.sizing import compute_size, projection_curve, required_daily_growth
from ..security import Cipher, Credentials, MasterKey, REDACTOR
from ..strategy.divergence import Bars, detect_divergence
from ..strategy.filters import FilterContext, apply_filters, compute_atr
from ..strategy.scanner import MarketScanner, ScanEntry
from .executor import OrderExecutor
from .market_data import INTERVAL_SEC, KlineStore, TickerCache
from .position_manager import ManagedPosition, PositionManager

log = logging.getLogger("ch.bot")


def _local_ips() -> List[str]:
    import socket
    ips: List[str] = []
    try:
        for info in socket.getaddrinfo(socket.gethostname(), None):
            ip = info[4][0]
            if ip not in ips and not ip.startswith("127.") and ":" not in ip:
                ips.append(ip)
    except Exception:
        pass
    return ips

Broadcast = Callable[[Dict[str, Any]], Awaitable[None]]


class Bot:
    def __init__(self, settings: AppSettings, cfg: BotConfig, db: Database, broadcast: Broadcast):
        self.settings = settings
        self.cfg = cfg
        self.db = db
        self.broadcast = broadcast
        self.cipher = Cipher(MasterKey.load_or_create(settings.data_dir, settings.master_key))
        self.rl = RateLimiter(cfg.execution.rate_limit_fraction)
        self.rest = MexcFuturesREST(
            cfg.execution.rest_base_url, self.rl, None, cfg.execution.legacy_rest_base_url,
            cfg.execution.http_timeout_sec, cfg.execution.max_retries, cfg.execution.retry_backoff_base_sec,
            cfg.execution.connection_pool_size, cfg.execution.recv_window_sec,
        )
        self.klines = KlineStore(self.rest, cfg.signal.timeframe, cfg.signal.history_bars)
        self.tickers = TickerCache()
        self.scanner = MarketScanner(self.rest, cfg.scanner, cfg.signal, self.klines)
        self.executor = OrderExecutor(self.rest, cfg)
        self.pm = PositionManager(db, self.executor, cfg, self.emit)
        self.pm.on_closed = self._on_trade_closed
        self.pm.on_changed = self._on_position_changed
        self.public_ws: Optional[PublicWS] = None
        self.private_ws: Optional[PrivateWS] = None

        self.credentials: Optional[Credentials] = None
        self.running = False
        self.state = "STOPPED"           # STOPPED | STARTING | RUNNING | PAUSED | ERROR
        self.state_reason = ""
        self.started_at: Optional[float] = None
        self.watchlist: List[ScanEntry] = []
        self.asset: Optional[AccountAsset] = None
        self.asset_ts = 0.0
        self.exchange_positions: List[ExchangePosition] = []
        self._tasks: List[asyncio.Task] = []
        self._last_signal_bar: Dict[str, float] = {}
        self._entry_lock = asyncio.Lock()
        self._last_errors: List[str] = []
        self.session_start_equity: Optional[float] = None
        self.day_start_equity: Optional[float] = None
        self.day_key: Optional[str] = None
        self.last_signals: List[Dict[str, Any]] = []
        self.account_ok = False
        self.public_ip: Dict[str, Any] = {"ip": None, "ts": 0}

    # ================================================================== events
    async def emit(self, level: str, kind: str, message: str, symbol: Optional[str] = None,
                   data: Optional[Dict[str, Any]] = None) -> None:
        getattr(log, "warning" if level == "warn" else level if level in ("info", "error", "debug") else "info")(
            "%s%s", f"[{symbol}] " if symbol else "", message)
        ev = await self.db.log_event(level, kind, message, symbol, data)
        if level == "error":
            self._last_errors = (self._last_errors + [message])[-10:]
        await self.broadcast({"type": "event", "event": ev})

    async def _on_position_changed(self, p: ManagedPosition) -> None:
        await self.broadcast({"type": "position", "position": p.to_dict()})

    async def _on_trade_closed(self, trade: Dict[str, Any]) -> None:
        f = self.cfg.filters
        minutes = f.cooldown_after_loss_minutes if (trade.get("pnl") or 0) < 0 else f.cooldown_after_any_exit_minutes
        if minutes > 0:
            await self.db.set_cooldown(trade["symbol"], time.time() + minutes * 60, "loss" if (trade.get("pnl") or 0) < 0 else "exit")
        await self.broadcast({"type": "trade", "trade": trade})
        await self._refresh_ws_subs()
        await self.push_snapshot()

    # ============================================================ credentials
    async def load_credentials(self) -> Optional[Credentials]:
        row = await self.db.load_credentials()
        if not row:
            self.credentials = None
            return None
        creds = Credentials(self.cipher.decrypt(row["api_key_enc"]), self.cipher.decrypt(row["api_secret_enc"]))
        REDACTOR.register(creds.api_key, creds.api_secret)
        self.credentials = creds
        self.rest.set_credentials(creds)
        return creds

    async def set_credentials(self, api_key: str, api_secret: str) -> Dict[str, Any]:
        api_key, api_secret = api_key.strip(), api_secret.strip()
        if len(api_key) < 8 or len(api_secret) < 8:
            raise ValueError("API key / secret look invalid")
        REDACTOR.register(api_key, api_secret)
        creds = Credentials(api_key, api_secret)
        await self.rest.start()
        prev = self.rest.creds
        self.rest.set_credentials(creds)
        try:
            asset = await self.rest.get_asset(self.cfg.scanner.quote_coin)
        except MexcAPIError as exc:
            self.rest.set_credentials(prev)
            raise ValueError(f"MEXC rejected the credentials: {exc.message} (code {exc.code})") from exc
        except Exception as exc:
            self.rest.set_credentials(prev)
            raise ValueError(f"Could not reach MEXC to verify the credentials: {exc}") from exc
        await self.db.save_credentials(self.cipher.encrypt(api_key), self.cipher.encrypt(api_secret))
        self.credentials = creds
        self.asset, self.asset_ts, self.account_ok = asset, time.time(), True
        await self.emit("info", "credentials", f"API credentials saved ({creds.masked()}); equity {asset.equity:.2f} {asset.currency}")
        if self.running:
            await self._restart_private_ws()
        await self.push_snapshot()
        return {"equity": asset.equity, "available": asset.available, "masked_key": creds.masked()}

    async def clear_credentials(self) -> None:
        if self.running:
            await self.stop("credentials removed")
        await self.db.delete_credentials()
        self.credentials = None
        self.rest.set_credentials(None)
        self.account_ok = False
        await self.emit("warn", "credentials", "API credentials removed")
        await self.push_snapshot()

    # ================================================================== config
    async def update_config(self, patch: Dict[str, Any]) -> BotConfig:
        new_cfg = self.cfg.merged(patch)
        stored = await self.db.load_settings()
        from ..config import deep_merge
        await self.db.save_settings(deep_merge(stored, patch))
        self.apply_config(new_cfg)
        await self.emit("info", "config", "Configuration updated", None, {"patch": patch})
        await self.push_snapshot()
        return new_cfg

    def apply_config(self, cfg: BotConfig) -> None:
        self.cfg = cfg
        self.rl.fraction = cfg.execution.rate_limit_fraction
        self.scanner.cfg, self.scanner.sig_cfg = cfg.scanner, cfg.signal
        self.klines.history_bars = cfg.signal.history_bars
        self.executor.update_config(cfg)
        self.pm.update_config(cfg)

    # =============================================================== lifecycle
    async def refresh_public_ip(self, force: bool = False) -> Dict[str, Any]:
        if force or time.time() - (self.public_ip.get("ts") or 0) > 600 or not self.public_ip.get("ip"):
            await self.rest.start()
            self.public_ip = await self.rest.public_ip()
            self.public_ip["local_ips"] = _local_ips()
        return self.public_ip

    async def boot(self) -> None:
        await self.rest.start()
        await self.load_credentials()
        try:
            await self.scanner.refresh_contracts(force=True)
            self.pm.contracts = self.scanner.contracts
        except Exception as exc:
            log.warning("contract list unavailable at boot: %s", exc)
        await self.pm.load()
        asyncio.create_task(self.refresh_public_ip(force=True))
        self.session_start_equity = await self.db.kv_get("session_start_equity")
        day = await self.db.kv_get("day_start")
        if day:
            self.day_key, self.day_start_equity = day.get("key"), day.get("equity")
        if self.credentials:
            try:
                self.asset = await self.rest.get_asset(self.cfg.scanner.quote_coin)
                self.asset_ts, self.account_ok = time.time(), True
            except Exception as exc:
                log.warning("Initial account query failed: %s", exc)
        if self.cfg.auto_start and self.credentials:
            asyncio.create_task(self.start())

    async def start(self) -> None:
        if self.running:
            return
        if not self.credentials:
            raise ValueError("Set MEXC API credentials first")
        self.state, self.state_reason = "STARTING", ""
        await self.push_snapshot()
        try:
            await self.rest.start()
            try:
                await self.rest.ping()
            except Exception as exc:
                log.warning("ping failed: %s", exc)
            self.asset = await self.rest.get_asset(self.cfg.scanner.quote_coin)
            self.asset_ts, self.account_ok = time.time(), True
            if self.session_start_equity is None:
                self.session_start_equity = self.asset.equity
                await self.db.kv_set("session_start_equity", self.asset.equity)
                await self.db.kv_set("session_start_ts", time.time())
            await self._roll_day(self.asset.equity)
            await self.executor.detect_position_mode()
            await self.scanner.refresh_contracts(force=True)
            self.pm.contracts = self.scanner.contracts
            self.exchange_positions = await self.rest.get_open_positions()
            await self.pm.reconcile(self.exchange_positions, self._adopt_external)
        except Exception as exc:
            msg = exc.message if isinstance(exc, MexcAPIError) else f"{type(exc).__name__}: {exc}"
            self.state, self.state_reason = "ERROR", msg
            await self.emit("error", "start", f"Start failed: {msg}")
            await self.push_snapshot()
            raise ValueError(msg) from exc

        self.running = True
        self.started_at = time.time()
        self.state = "RUNNING"
        self.public_ws = PublicWS(self.cfg.execution.ws_url, self._on_ws_ticker, self._on_ws_kline, self.cfg.signal.timeframe)
        self.public_ws.start()
        await self._restart_private_ws()
        self._tasks = [
            asyncio.create_task(self._guard(self.scanner_loop), name="scanner"),
            asyncio.create_task(self._guard(self.candle_loop), name="candles"),
            asyncio.create_task(self._guard(self.sync_loop), name="sync"),
            asyncio.create_task(self._guard(self.snapshot_loop), name="snapshot"),
        ]
        await self.emit("info", "start", f"Crypto Hunter started – equity {self.asset.equity:.2f} USDT, "
                        f"{self.pm.count()} managed positions, mode={'hedge' if self.executor.position_mode == 1 else 'one-way'}")
        await self.push_snapshot()

    async def stop(self, reason: str = "user") -> None:
        if not self.running and self.state == "STOPPED":
            return
        self.running = False
        for t in self._tasks:
            t.cancel()
        self._tasks = []
        if self.public_ws:
            await self.public_ws.stop()
        if self.private_ws:
            await self.private_ws.stop()
        self.state, self.state_reason = "STOPPED", reason
        await self.emit("warn", "stop", f"Crypto Hunter stopped ({reason}). Open positions keep their exchange-side TP/SL.")
        await self.push_snapshot()

    async def close_all(self) -> int:
        n = 0
        for p in list(self.pm.positions.values()):
            t = self.tickers.get(p.symbol)
            await self.pm.force_close(p, "MANUAL", (t.fair if t else p.entry_price))
            n += 1
        return n

    async def close_one(self, symbol: str, side: str) -> bool:
        p = self.pm.positions.get((symbol, side))
        if not p:
            return False
        t = self.tickers.get(symbol)
        await self.pm.force_close(p, "MANUAL", (t.fair if t else p.entry_price))
        return True

    async def _restart_private_ws(self) -> None:
        if self.private_ws:
            await self.private_ws.stop()
        if self.credentials:
            self.private_ws = PrivateWS(self.cfg.execution.ws_url, self.credentials, self._on_private_event)
            self.private_ws.start()

    async def _guard(self, coro_fn: Callable[[], Awaitable[None]]) -> None:
        while self.running:
            try:
                await coro_fn()
                return
            except asyncio.CancelledError:
                return
            except Exception as exc:
                log.exception("%s crashed – restarting in 5s", coro_fn.__name__)
                await self.emit("error", "loop", f"{coro_fn.__name__} crashed: {exc}")
                await asyncio.sleep(5)

    # ============================================================ WS callbacks
    async def _on_ws_ticker(self, d: Dict[str, Any]) -> None:
        t = self.tickers.update_ws(d)
        if t and self.pm.has(t.symbol):
            await self.pm.on_price(t.symbol, t.last, t.fair)

    async def _on_ws_kline(self, symbol: str, interval: str, d: Dict[str, Any]) -> None:
        self.klines.on_ws_kline(symbol, interval, d)

    async def _on_private_event(self, kind: str, data: Dict[str, Any]) -> None:
        if kind == "position":
            await self.pm.on_private_position(data)
        elif kind == "asset":
            if data.get("currency") == self.cfg.scanner.quote_coin:
                self.asset = AccountAsset.from_api(data)
                self.asset_ts = time.time()
        elif kind in ("order", "stop.order", "stop.planorder", "order.deal"):
            log.debug("private %s: %s", kind, json.dumps(data)[:300])

    # ==================================================================== loops
    async def scanner_loop(self) -> None:
        while self.running:
            try:
                tickers = await self.rest.get_tickers()
                self.tickers.update_many(tickers)
                self.watchlist = await self.scanner.scan(tickers)
                self.pm.contracts = self.scanner.contracts
                await self._refresh_ws_subs()
                await self.broadcast({"type": "watchlist", "watchlist": [w.to_dict() for w in self.watchlist]})
            except Exception as exc:
                await self.emit("error", "scanner", f"Scan failed: {exc}")
            await asyncio.sleep(self.cfg.scanner.rescan_interval_sec)

    async def _refresh_ws_subs(self) -> None:
        if not self.public_ws:
            return
        watch: Set[str] = {w.symbol for w in self.watchlist} | self.pm.symbols()
        await self.public_ws.set_kline_symbols(watch)
        await self.public_ws.set_ticker_symbols(self.pm.symbols())

    async def candle_loop(self) -> None:
        sec = INTERVAL_SEC.get(self.cfg.signal.timeframe, 300)
        while self.running:
            now = time.time()
            next_close = (math.floor(now / sec) + 1) * sec
            await asyncio.sleep(max(0.5, next_close - now + 2.0))
            if not self.running:
                return
            symbols = [w.symbol for w in self.watchlist]
            if not symbols:
                continue
            sem = asyncio.Semaphore(5)

            async def _eval(sym: str) -> None:
                async with sem:
                    try:
                        bars = await self.klines.refresh(sym)
                    except Exception as exc:
                        log.warning("kline refresh %s failed: %s", sym, exc)
                        return
                if bars is None or len(bars) < 60:
                    return
                await self.evaluate_symbol(sym, bars)

            await asyncio.gather(*(_eval(s) for s in symbols))

    async def sync_loop(self) -> None:
        while self.running:
            try:
                asset, positions = await asyncio.gather(
                    self.rest.get_asset(self.cfg.scanner.quote_coin), self.rest.get_open_positions())
                self.asset, self.asset_ts, self.account_ok = asset, time.time(), True
                self.exchange_positions = positions
                await self._roll_day(asset.equity)
                await self.pm.reconcile(positions, self._adopt_external)
                if self.cfg.risk.max_daily_loss_pct > 0 and self.day_start_equity:
                    dd = (self.day_start_equity - asset.equity) / self.day_start_equity * 100
                    if dd >= self.cfg.risk.max_daily_loss_pct and self.state == "RUNNING":
                        self.state, self.state_reason = "PAUSED", f"daily loss {dd:.1f}% ≥ {self.cfg.risk.max_daily_loss_pct}%"
                        await self.emit("warn", "risk", f"New entries paused: {self.state_reason}")
                    elif dd < self.cfg.risk.max_daily_loss_pct and self.state == "PAUSED":
                        self.state, self.state_reason = "RUNNING", ""
            except MexcAPIError as exc:
                self.account_ok = False
                await self.emit("error", "sync", f"Account sync failed: {exc.message}")
            except Exception as exc:
                self.account_ok = False
                log.warning("sync failed: %s", exc)
            await asyncio.sleep(self.cfg.execution.position_sync_interval_sec)

    async def snapshot_loop(self) -> None:
        last_persist = 0.0
        while self.running:
            if self.asset and time.time() - last_persist >= self.cfg.execution.equity_snapshot_interval_sec:
                await self.db.insert_equity(self.asset.equity, self.asset.cash, self.asset.unrealized)
                last_persist = time.time()
            await self.push_snapshot()
            await asyncio.sleep(1.0)

    async def _roll_day(self, equity: float) -> None:
        key = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%d")
        if key != self.day_key:
            self.day_key, self.day_start_equity = key, equity
            await self.db.kv_set("day_start", {"key": key, "equity": equity})

    # ================================================================== signals
    async def evaluate_symbol(self, symbol: str, bars: Bars) -> None:
        sc = self.cfg.signal
        last_t = float(bars.time[-1])
        if self._last_signal_bar.get(symbol) == last_t:
            return
        self._last_signal_bar[symbol] = last_t
        div = detect_divergence(bars, sc.ao_fast, sc.ao_slow, sc.pivot_left, sc.pivot_right,
                                sc.min_pivot_distance, sc.max_pivot_distance,
                                allow_zero_cross=sc.allow_zero_cross_between_pivots)
        if div is None:
            return
        atr_v, atr_p = compute_atr(bars, sc.atr_period)
        bars_1h = None
        if self.cfg.filters.trend_filter in ("ema_1h", "both"):
            try:
                bars_1h = await self.klines.ensure(symbol, "Min60", min_bars=self.cfg.filters.ema_slow + 5, max_age=900)
            except Exception as exc:
                log.warning("1h bars for %s failed: %s", symbol, exc)
        t = self.tickers.get(symbol)
        cooldowns = await self.db.cooldowns()
        ctx = FilterContext(bars_5m=bars, bars_1h=bars_1h, atr_value=atr_v, atr_pct=atr_p,
                            spread_pct=t.spread_pct if t else 0.0, cooldown_until=cooldowns.get(symbol))
        fr = apply_filters(div, ctx, self.cfg.filters)
        rec = {"ts": time.time(), "symbol": symbol, "side": div.side, "magnitude": round(div.magnitude, 3),
               "atr_pct": round(atr_p, 3) if atr_p == atr_p else None, "passed": fr.passed,
               "failed": fr.failed, "checks": fr.to_dict(), "divergence": div.to_dict()}
        self.last_signals = ([rec] + self.last_signals)[:50]
        await self.broadcast({"type": "signal", "signal": rec})
        if not fr.passed:
            await self.emit("info", "signal_rejected", f"{symbol} {div.side} AO divergence rejected: {', '.join(fr.failed)}",
                            symbol, {"checks": fr.to_dict(), "magnitude": div.magnitude})
            return
        await self.emit("info", "signal", f"{symbol} {div.side.upper()} AO divergence confirmed "
                        f"(magnitude {div.magnitude:.2f}, ATR% {atr_p:.2f})", symbol, rec)
        await self.try_enter(symbol, div.side, atr_v, rec)

    # ==================================================================== entry
    def _entry_gate(self, symbol: str, side: str) -> Optional[str]:
        r = self.cfg.risk
        if not self.running or self.state != "RUNNING":
            return f"bot state {self.state} {self.state_reason}".strip()
        if not self.asset or time.time() - self.asset_ts > 60:
            return "account data stale"
        if self.asset.equity < r.min_equity_usdt:
            return f"equity {self.asset.equity:.2f} < min {r.min_equity_usdt}"
        if self.pm.count() >= r.max_open_positions:
            return f"max open positions ({r.max_open_positions}) reached"
        if self.pm.has(symbol, side) or (r.max_positions_per_symbol == 1 and self.pm.has(symbol)):
            return "position already open on symbol"
        for ep in self.exchange_positions:
            if ep.symbol == symbol and ep.hold_vol > 0 and ep.side != side:
                return "opposite position exists on exchange"
        return None

    async def try_enter(self, symbol: str, side: str, atr_v: float, signal: Dict[str, Any]) -> None:
        async with self._entry_lock:
            why = self._entry_gate(symbol, side)
            if why:
                await self.emit("info", "entry_skipped", f"{symbol} {side}: {why}", symbol)
                return
            contract = self.scanner.contracts.get(symbol)
            t = self.tickers.get(symbol)
            if contract is None or t is None or t.last <= 0 or not (atr_v == atr_v) or atr_v <= 0:
                await self.emit("warn", "entry_skipped", f"{symbol}: missing contract/ticker/ATR", symbol)
                return
            r, ex = self.cfg.risk, self.cfg.exits
            lev = min(r.leverage, contract.max_leverage)
            ref = t.ask if side == "long" else t.bid
            ref = ref if ref > 0 else t.last
            stop_dist = atr_v * r.atr_stop_multiplier
            # keep the SL inside the liquidation band (~ -85% ROI) – otherwise the SL would never fire
            max_dist = ref * 0.85 / lev
            if stop_dist > max_dist:
                stop_dist = max_dist
            size = compute_size(self.asset.equity, self.asset.available, ref, stop_dist, contract, r, contract.taker_fee)
            if size.vol <= 0:
                await self.emit("warn", "entry_skipped", f"{symbol}: sizing failed – {size.reason}", symbol, size.to_dict())
                return
            sl = ref - stop_dist if side == "long" else ref + stop_dist
            tp = price_for_roi(side, ref, ex.tp_roi, lev)
            try:
                fill = await self.executor.open_position(symbol, side, size.vol, ref, lev, sl, tp, contract)
            except MexcAPIError as exc:
                await self.emit("error", "entry_error", f"{symbol} {side}: order rejected – {exc.message} (code {exc.code})", symbol)
                return
            if fill.filled_vol <= 0 or fill.avg_price <= 0:
                if fill.state not in (3, 4, 5):  # still live/unknown -> never leave an unmanaged resting order behind
                    try:
                        await self.rest.cancel_orders([fill.order_id])
                    except MexcAPIError as exc:
                        log.warning("cancel of unfilled entry %s failed: %s", fill.order_id, exc.message)
                await self.emit("error", "entry_error", f"{symbol} {side}: order {fill.order_id} not filled (state {fill.state})", symbol, fill.raw)
                return
            entry = fill.avg_price
            sl = entry - stop_dist if side == "long" else entry + stop_dist
            tp = price_for_roi(side, entry, ex.tp_roi, lev)
            p = ManagedPosition(
                symbol=symbol, side=side, entry_price=entry, vol=fill.filled_vol, contract_size=contract.contract_size,
                leverage=lev, margin=contract.notional(fill.filled_vol, entry) / lev, atr=atr_v,
                initial_stop_price=contract.round_price(sl, "down" if side == "long" else "up"),
                stop_price=contract.round_price(sl, "down" if side == "long" else "up"),
                tp_price=contract.round_price(tp, "up" if side == "long" else "down"), peak_price=entry,
                opened_at=time.time(), position_id=fill.position_id, entry_order_id=fill.order_id,
                signal_json=json.dumps(signal), last_price=t.last, fair_price=t.fair,
            )
            await self.pm.register(p)
            await self.emit("info", "entry", f"OPENED {side.upper()} {symbol} vol={p.vol} @ {entry} "
                            f"lev x{lev} margin {p.margin:.2f} SL {p.stop_price} TP {p.tp_price}", symbol, p.to_dict())
            # Attach / verify exchange-side protection, re-anchored to the real fill price
            asyncio.create_task(self._arm_protection(p, contract))
            await self._refresh_ws_subs()

    async def _arm_protection(self, p: ManagedPosition, contract: Contract) -> None:
        await asyncio.sleep(0.8)
        try:
            if p.position_id is None:
                for ep in await self.rest.get_open_positions(p.symbol):
                    if ep.side == p.side and ep.hold_vol > 0:
                        p.position_id = ep.position_id
                        break
            res = await self.executor.update_stop(p.symbol, p.side, p.stop_plan_order_id, p.stop_price, p.tp_price,
                                                  contract, p.position_id, p.vol, p.leverage)
            p.stop_plan_order_id = res.get("stop_plan_order_id") or p.stop_plan_order_id
            if res.get("sl_plan_order_id"):
                p.sl_plan_order_id, p.tp_plan_order_id = res["sl_plan_order_id"], res.get("tp_plan_order_id")
            p.stop_price = res.get("stop_price", p.stop_price)
            await self.pm.persist(p)
            await self.emit("info", "protection", f"{p.symbol}: exchange SL {p.stop_price} / TP {p.tp_price} armed "
                            f"({'plan#' + str(p.stop_plan_order_id) if p.stop_plan_order_id else 'trigger orders'})", p.symbol)
        except Exception as exc:
            await self.emit("error", "protection", f"{p.symbol}: failed to arm exchange TP/SL ({exc}); software failsafe active", p.symbol)

    async def _adopt_external(self, ep: ExchangePosition) -> None:
        """Manage a position that exists on the exchange but not in our DB (manual or pre-crash)."""
        contract = self.scanner.contracts.get(ep.symbol)
        if contract is None:
            return
        bars = await self.klines.ensure(ep.symbol, min_bars=self.cfg.signal.atr_period * 3)
        atr_v, _ = compute_atr(bars, self.cfg.signal.atr_period) if bars is not None else (float("nan"), float("nan"))
        if not (atr_v == atr_v) or atr_v <= 0:
            atr_v = ep.hold_avg_price * 0.01
        lev = ep.leverage or self.cfg.risk.leverage
        dist = min(atr_v * self.cfg.risk.atr_stop_multiplier, ep.hold_avg_price * 0.85 / lev)
        sl = ep.hold_avg_price - dist if ep.side == "long" else ep.hold_avg_price + dist
        tp = price_for_roi(ep.side, ep.hold_avg_price, self.cfg.exits.tp_roi, lev)
        t = self.tickers.get(ep.symbol)
        p = ManagedPosition(
            symbol=ep.symbol, side=ep.side, entry_price=ep.hold_avg_price, vol=ep.hold_vol, contract_size=contract.contract_size,
            leverage=lev, margin=ep.im or contract.notional(ep.hold_vol, ep.hold_avg_price) / lev, atr=atr_v,
            initial_stop_price=contract.round_price(sl), stop_price=contract.round_price(sl), tp_price=contract.round_price(tp),
            peak_price=ep.hold_avg_price, opened_at=time.time(), position_id=ep.position_id,
            signal_json=json.dumps({"adopted": True}), last_price=t.last if t else ep.hold_avg_price,
            fair_price=t.fair if t else ep.hold_avg_price,
        )
        existing = await self.executor.find_stop_plan_order(ep.symbol, ep.position_id)
        if existing:
            p.stop_plan_order_id = str(existing["id"])
            if existing.get("stopLossPrice"):
                p.stop_price = p.initial_stop_price = float(existing["stopLossPrice"])
            if existing.get("takeProfitPrice"):
                p.tp_price = float(existing["takeProfitPrice"])
        await self.pm.register(p)
        await self.emit("warn", "adopt", f"Adopted exchange position {ep.side} {ep.symbol} vol={ep.hold_vol} @ {ep.hold_avg_price}", ep.symbol)
        if not existing:
            asyncio.create_task(self._arm_protection(p, contract))
        await self._refresh_ws_subs()

    # ================================================================= metrics
    async def metrics(self) -> Dict[str, Any]:
        trades = await self.db.all_trades()
        now = time.time()
        day_start = dt.datetime.now(dt.timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0).timestamp()
        wins = [t for t in trades if (t.get("pnl") or 0) > 0]
        losses = [t for t in trades if (t.get("pnl") or 0) <= 0]
        today = [t for t in trades if t["closed_at"] >= day_start]
        curve = await self.db.equity_curve(since=now - 30 * 86400)
        peak, max_dd = 0.0, 0.0
        for pt in curve:
            peak = max(peak, pt["equity"])
            if peak > 0:
                max_dd = max(max_dd, (peak - pt["equity"]) / peak * 100)
        equity = self.asset.equity if self.asset else 0.0
        start_eq = self.session_start_equity or equity or 0.0
        gross_win = sum(t["pnl"] for t in wins)
        gross_loss = -sum(t["pnl"] for t in losses)
        reasons: Dict[str, int] = {}
        for t in trades:
            reasons[t.get("reason") or "?"] = reasons.get(t.get("reason") or "?", 0) + 1
        return {
            "trades": len(trades), "wins": len(wins), "losses": len(losses),
            "win_rate": (len(wins) / len(trades) * 100) if trades else 0.0,
            "avg_roi": (sum(t.get("roi") or 0 for t in trades) / len(trades)) if trades else 0.0,
            "avg_win_roi": (sum(t["roi"] for t in wins) / len(wins)) if wins else 0.0,
            "avg_loss_roi": (sum(t["roi"] for t in losses) / len(losses)) if losses else 0.0,
            "profit_factor": (gross_win / gross_loss) if gross_loss > 0 else (float("inf") if gross_win > 0 else 0.0),
            "total_pnl": sum(t.get("pnl") or 0 for t in trades),
            "total_gross_pnl": sum(t.get("gross_pnl") if t.get("gross_pnl") is not None else (t.get("pnl") or 0) for t in trades),
            "total_fees": sum(t.get("fee") or 0 for t in trades),
            "total_funding": sum(t.get("funding") or 0 for t in trades),
            "open_fees": sum(p.fee_paid for p in self.pm.positions.values()),
            "open_funding": sum(p.funding for p in self.pm.positions.values()),
            "estimated_trades": sum(1 for t in trades if t.get("pnl_source") == "estimate"),
            "daily_pnl": sum(t.get("pnl") or 0 for t in today),
            "daily_pnl_equity": (equity - self.day_start_equity) if self.day_start_equity else 0.0,
            "max_drawdown_pct": max_dd,
            "session_start_equity": start_eq,
            "session_return_pct": ((equity / start_eq - 1) * 100) if start_eq else 0.0,
            "target_equity": self.cfg.risk.target_equity_usdt,
            "target_progress_pct": min(100.0, equity / self.cfg.risk.target_equity_usdt * 100) if equity else 0.0,
            "required_daily_growth_pct": required_daily_growth(equity or start_eq, self.cfg.risk.target_equity_usdt, self.cfg.risk.target_days),
            "exit_reasons": reasons,
        }

    async def projection(self) -> List[Dict[str, float]]:
        start_ts = await self.db.kv_get("session_start_ts") or time.time()
        start_eq = self.session_start_equity or (self.asset.equity if self.asset else 0)
        return projection_curve(start_eq, self.cfg.risk.target_equity_usdt, self.cfg.risk.target_days, start_ts)

    # ================================================================ snapshot
    def status(self) -> Dict[str, Any]:
        return {
            "state": self.state, "state_reason": self.state_reason, "running": self.running,
            "started_at": self.started_at, "has_credentials": self.credentials is not None,
            "masked_key": self.credentials.masked() if self.credentials else None,
            "account_ok": self.account_ok,
            "position_mode": "hedge" if self.executor.position_mode == 1 else "one-way",
            "ws_public": bool(self.public_ws and self.public_ws.connected),
            "ws_private": bool(self.private_ws and self.private_ws.logged_in),
            "rest_latency_ms": round(self.rest.last_latency_ms, 1),
            "rate_limits": self.rl.usage(),
            "last_errors": self._last_errors[-3:],
            "server_time": time.time(),
            "public_ip": self.public_ip,
        }

    def account_dict(self) -> Dict[str, Any]:
        a = self.asset
        if not a:
            return {"equity": 0, "available": 0, "cash": 0, "unrealized": 0, "position_margin": 0, "currency": self.cfg.scanner.quote_coin, "ts": 0}
        return {"equity": a.equity, "available": a.available, "cash": a.cash, "unrealized": a.unrealized,
                "position_margin": a.position_margin, "currency": a.currency, "ts": self.asset_ts}

    def positions_dict(self) -> List[Dict[str, Any]]:
        out = []
        for p in self.pm.positions.values():
            t = self.tickers.get(p.symbol)
            if t:
                p.last_price, p.fair_price = t.last, t.fair
            out.append(p.to_dict())
        return out

    async def snapshot(self) -> Dict[str, Any]:
        return {
            "type": "snapshot", "status": self.status(), "account": self.account_dict(),
            "positions": self.positions_dict(), "watchlist": [w.to_dict() for w in self.watchlist],
            "metrics": await self.metrics(), "signals": self.last_signals[:20],
        }

    async def push_snapshot(self) -> None:
        try:
            await self.broadcast(await self.snapshot())
        except Exception as exc:  # pragma: no cover
            log.debug("snapshot broadcast failed: %s", exc)

    async def shutdown(self) -> None:
        if self.running:
            await self.stop("shutdown")
        await self.rest.close()
