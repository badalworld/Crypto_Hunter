"""
Market scanner – ranks MEXC USDT perpetuals by volatility (ATR%) and liquidity (24h
quote volume) and returns the top-N watch-list.

score = w * rank_pct(ATR%) + (1 - w) * rank_pct(amount24)
"""
from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, asdict
from typing import Dict, List, Optional

import numpy as np

from ..config import ScannerConfig, SignalConfig
from ..exchange.models import Contract, Ticker
from ..exchange.rest import MexcFuturesREST
from .indicators import atr_pct

log = logging.getLogger("ch.scanner")


@dataclass
class ScanEntry:
    symbol: str
    last: float
    amount24: float
    atr: float
    atr_pct: float
    change24_pct: float
    score: float
    rank: int = 0

    def to_dict(self) -> Dict:
        return asdict(self)


class MarketScanner:
    def __init__(self, rest: MexcFuturesREST, cfg: ScannerConfig, sig_cfg: SignalConfig, kline_store):
        self.rest = rest
        self.cfg = cfg
        self.sig_cfg = sig_cfg
        self.klines = kline_store
        self.contracts: Dict[str, Contract] = {}
        self.last_scan: List[ScanEntry] = []
        self.last_scan_ts: float = 0.0
        self._contracts_ts = 0.0

    async def refresh_contracts(self, force: bool = False) -> Dict[str, Contract]:
        if force or not self.contracts or time.time() - self._contracts_ts > 3600:
            cs = await self.rest.get_contracts()
            self.contracts = {c.symbol: c for c in cs}
            self._contracts_ts = time.time()
            log.info("Loaded %d contracts", len(self.contracts))
        return self.contracts

    def _eligible(self, c: Contract, t: Ticker) -> bool:
        cfg = self.cfg
        if cfg.symbol_whitelist and c.symbol not in cfg.symbol_whitelist:
            return False
        if c.symbol in cfg.symbol_blacklist:
            return False
        if c.quote_coin != cfg.quote_coin or c.settle_coin != cfg.quote_coin:
            return False
        if c.state != 0 or not c.api_allowed:
            return False
        if t.amount24 < cfg.min_quote_volume_24h or t.last <= cfg.min_price or t.last <= 0:
            return False
        return True

    async def scan(self, tickers: Optional[List[Ticker]] = None) -> List[ScanEntry]:
        await self.refresh_contracts()
        tickers = tickers or await self.rest.get_tickers()
        cands = [t for t in tickers if t.symbol in self.contracts and self._eligible(self.contracts[t.symbol], t)]
        cands.sort(key=lambda t: t.amount24, reverse=True)
        cands = cands[: self.cfg.candidates_by_volume]

        sem = asyncio.Semaphore(6)

        async def _one(t: Ticker) -> Optional[ScanEntry]:
            async with sem:
                try:
                    bars = await self.klines.ensure(t.symbol, min_bars=self.sig_cfg.atr_period * 4 + 5)
                except Exception as exc:
                    log.debug("kline fetch failed for %s: %s", t.symbol, exc)
                    return None
            if bars is None or len(bars) < self.sig_cfg.atr_period + 2:
                return None
            a, ap = atr_pct(bars.high, bars.low, bars.close, self.sig_cfg.atr_period)
            if not np.isfinite(ap):
                return None
            return ScanEntry(symbol=t.symbol, last=t.last, amount24=t.amount24, atr=a, atr_pct=ap,
                             change24_pct=t.rise_fall_rate * 100.0, score=0.0)

        results = [r for r in await asyncio.gather(*(_one(t) for t in cands)) if r]
        if not results:
            self.last_scan, self.last_scan_ts = [], time.time()
            return []

        atrs = np.array([r.atr_pct for r in results])
        vols = np.array([r.amount24 for r in results])
        atr_rank = atrs.argsort().argsort() / max(1, len(results) - 1)
        vol_rank = vols.argsort().argsort() / max(1, len(results) - 1)
        w = self.cfg.volatility_weight
        for r, ar, vr in zip(results, atr_rank, vol_rank):
            r.score = float(w * ar + (1 - w) * vr)
        results.sort(key=lambda r: r.score, reverse=True)
        for i, r in enumerate(results, 1):
            r.rank = i
        self.last_scan = results
        self.last_scan_ts = time.time()
        top = results[: self.cfg.top_n]
        log.info("Scan complete: %d candidates, watching %s", len(results), [t.symbol for t in top])
        return top
