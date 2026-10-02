"""
ROI-on-margin maths and the stepped trailing-stop ladder.

ROI% (long)  = (price - entry) / entry * 100 * leverage
ROI% (short) = (entry - price) / entry * 100 * leverage
price(ROI)   = entry * (1 ± ROI / (100 * leverage))

Trailing ladder (defaults start=30, initial=20, step=10, stop_step=10):
    peak < 30            -> None  (initial ATR stop stays)
    stop = floor((peak - start) / step) * stop_step + initial
    peak 30 -> 20, 40 -> 30, 50 -> 40, 100 -> 90, 200 -> TP
The stop ROI only ever ratchets up (see ``ratchet``).
"""
from __future__ import annotations

import math
from typing import Optional

from ..config import ExitConfig


def roi_pct(side: str, entry: float, price: float, leverage: float) -> float:
    if entry <= 0:
        return 0.0
    if side == "long":
        return (price - entry) / entry * 100.0 * leverage
    return (entry - price) / entry * 100.0 * leverage


def price_for_roi(side: str, entry: float, target_roi: float, leverage: float) -> float:
    frac = target_roi / (100.0 * leverage)
    return entry * (1.0 + frac) if side == "long" else entry * (1.0 - frac)


def trailing_stop_roi(peak_roi: float, cfg: ExitConfig) -> Optional[float]:
    if peak_roi < cfg.trail_start_roi:
        return None
    steps = math.floor((peak_roi - cfg.trail_start_roi) / cfg.trail_step_roi + 1e-9)
    stop = steps * cfg.trail_stop_step_roi + cfg.trail_initial_stop_roi
    # Never place the stop at/above TP
    return min(stop, cfg.tp_roi - cfg.trail_stop_step_roi)


def ratchet(current_stop_roi: Optional[float], new_stop_roi: Optional[float]) -> Optional[float]:
    """Return the stop ROI to use – only moves in the profit direction."""
    if new_stop_roi is None:
        return current_stop_roi
    if current_stop_roi is None:
        return new_stop_roi
    return max(current_stop_roi, new_stop_roi)


def unrealized_pnl(side: str, entry: float, price: float, vol: float, contract_size: float) -> float:
    qty = vol * contract_size
    return (price - entry) * qty if side == "long" else (entry - price) * qty
