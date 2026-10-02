"""
Position sizing & compounding projection.

``margin`` mode     margin = equity * risk% ; notional = margin * leverage
``stop_risk`` mode  size so that a stop-loss hit (ATR * multiplier away) loses equity * risk%
Both are recalculated from *current equity* on every entry, so winners compound.
"""
from __future__ import annotations

import math
import time
from dataclasses import dataclass, asdict
from typing import Dict, List, Optional

from ..config import RiskConfig
from ..exchange.models import Contract


@dataclass
class SizeResult:
    vol: float
    margin: float
    notional: float
    qty: float            # base-coin quantity
    reason: Optional[str] = None  # populated when vol == 0

    def to_dict(self) -> Dict:
        return asdict(self)


def compute_size(equity: float, available: float, price: float, stop_distance: float,
                 contract: Contract, cfg: RiskConfig, fee_rate: float = 0.0006) -> SizeResult:
    if equity <= 0 or price <= 0:
        return SizeResult(0, 0, 0, 0, "no equity / price")
    lev = min(cfg.leverage, contract.max_leverage)
    risk_cash = equity * cfg.risk_per_trade_pct / 100.0

    if cfg.sizing_mode == "stop_risk" and stop_distance > 0:
        qty = risk_cash / stop_distance
        notional = qty * price
        margin = notional / lev
        # Never exceed margin-mode allocation by more than 2x – protects against tiny ATR
        if margin > 2 * risk_cash:
            margin = 2 * risk_cash
            notional = margin * lev
    else:
        margin = risk_cash
        notional = margin * lev

    # Keep a buffer for taker fees (open + close) and funding
    fee_buffer = notional * fee_rate * 2
    max_margin = max(0.0, available - fee_buffer) * 0.98
    if margin > max_margin:
        margin = max_margin
        notional = margin * lev
    if margin <= 0:
        return SizeResult(0, 0, 0, 0, "insufficient available balance")

    vol = contract.round_vol(notional / (contract.contract_size * price))
    if vol < contract.min_vol:
        return SizeResult(0, margin, notional, 0, f"size {vol} < minVol {contract.min_vol} (increase equity or risk%)")
    vol = min(vol, contract.max_vol)
    notional = contract.notional(vol, price)
    margin = notional / lev
    return SizeResult(vol=vol, margin=margin, notional=notional, qty=vol * contract.contract_size)


def projection_curve(start_equity: float, target: float, days: int, start_ts: Optional[float] = None,
                     points_per_day: int = 24) -> List[Dict[str, float]]:
    """Geometric growth curve from start_equity to target over ``days`` (what compounding must deliver)."""
    start_ts = start_ts or time.time()
    if start_equity <= 0:
        return []
    total = days * points_per_day
    rate = (target / start_equity) ** (1.0 / total) - 1.0 if target > start_equity else 0.0
    out = []
    for i in range(total + 1):
        out.append({"ts": start_ts + i * 86400.0 / points_per_day, "equity": start_equity * (1 + rate) ** i})
    return out


def required_daily_growth(start_equity: float, target: float, days: int) -> float:
    if start_equity <= 0 or target <= start_equity:
        return 0.0
    return ((target / start_equity) ** (1.0 / days) - 1.0) * 100.0
