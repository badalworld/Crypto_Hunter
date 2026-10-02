"""
Awesome Oscillator regular-divergence detection.

Bullish divergence : price prints a *lower low* while AO prints a *higher low*
                     (both AO pivots below zero).
Bearish divergence : price prints a *higher high* while AO prints a *lower high*
                     (both AO pivots above zero).

Pivots are detected on the AO series (``left`` bars strictly lower/higher before,
``right`` bars after).  The *right* lookback is the natural confirmation lag – a
divergence is only reported once the second AO pivot is confirmed by ``right`` closed
bars.  The price extreme compared is the lowest low / highest high inside a small
window around each AO pivot so the structure is robust to one-bar misalignment.

Magnitude is dimensionless: ``|AO2 - AO1| / mean(|AO|)`` over the lookback window –
so the same threshold works for BTC and for a $0.0001 meme coin.
"""
from __future__ import annotations

from dataclasses import dataclass, asdict
from typing import Dict, Optional

import numpy as np

from .indicators import awesome_oscillator, pivot_highs, pivot_lows


@dataclass
class Divergence:
    side: str                 # long | short
    bar_index: int            # index of the confirming (last closed) bar
    pivot1_index: int
    pivot2_index: int
    price1: float
    price2: float
    ao1: float
    ao2: float
    magnitude: float
    pivot_extreme: float      # 2nd pivot candle extreme used for confirmation (high for long, low for short)

    def to_dict(self) -> Dict:
        return asdict(self)


@dataclass
class Bars:
    time: np.ndarray
    open: np.ndarray
    high: np.ndarray
    low: np.ndarray
    close: np.ndarray
    volume: np.ndarray

    def __len__(self) -> int:
        return len(self.close)


def _window_min(x: np.ndarray, i: int, w: int) -> tuple[int, float]:
    lo, hi = max(0, i - w), min(len(x), i + w + 1)
    j = lo + int(np.argmin(x[lo:hi]))
    return j, float(x[j])


def _window_max(x: np.ndarray, i: int, w: int) -> tuple[int, float]:
    lo, hi = max(0, i - w), min(len(x), i + w + 1)
    j = lo + int(np.argmax(x[lo:hi]))
    return j, float(x[j])


def detect_divergence(
    bars: Bars,
    ao_fast: int = 5,
    ao_slow: int = 34,
    left: int = 3,
    right: int = 2,
    min_distance: int = 5,
    max_distance: int = 60,
    price_window: int = 2,
    allow_zero_cross: bool = True,
) -> Optional[Divergence]:
    """Return the freshest divergence whose second pivot was just confirmed, else None.

    "Just confirmed" means the second AO pivot sits exactly ``right`` bars before the
    last closed bar, so each divergence fires exactly once.
    """
    n = len(bars)
    if n < ao_slow + left + right + max_distance // 2:
        return None
    ao = awesome_oscillator(bars.high, bars.low, ao_fast, ao_slow)
    last = n - 1
    expected_pivot = last - right
    norm = float(np.nanmean(np.abs(ao[-max(max_distance, 50):])))
    if not np.isfinite(norm) or norm <= 0:
        return None

    # ---- bullish
    lows = pivot_lows(ao, left, right)
    if lows and lows[-1] == expected_pivot:
        p2 = lows[-1]
        for p1 in reversed(lows[:-1]):
            dist = p2 - p1
            if dist < min_distance:
                continue
            if dist > max_distance:
                break
            if ao[p1] >= 0 or ao[p2] >= 0:
                continue
            _, price1 = _window_min(bars.low, p1, price_window)
            j2, price2 = _window_min(bars.low, p2, price_window)
            if price2 < price1 and ao[p2] > ao[p1]:
                # Optional strict mode: AO must stay below zero between the two troughs
                if not allow_zero_cross and np.nanmax(ao[p1:p2 + 1]) >= 0:
                    break
                return Divergence(
                    side="long", bar_index=last, pivot1_index=p1, pivot2_index=p2,
                    price1=price1, price2=price2, ao1=float(ao[p1]), ao2=float(ao[p2]),
                    magnitude=float(abs(ao[p2] - ao[p1]) / norm),
                    pivot_extreme=float(np.max(bars.high[j2:min(n, p2 + 1)])) if j2 <= p2 else float(bars.high[j2]),
                )
            break  # only compare against the most recent qualifying pivot

    # ---- bearish
    highs = pivot_highs(ao, left, right)
    if highs and highs[-1] == expected_pivot:
        p2 = highs[-1]
        for p1 in reversed(highs[:-1]):
            dist = p2 - p1
            if dist < min_distance:
                continue
            if dist > max_distance:
                break
            if ao[p1] <= 0 or ao[p2] <= 0:
                continue
            _, price1 = _window_max(bars.high, p1, price_window)
            j2, price2 = _window_max(bars.high, p2, price_window)
            if price2 > price1 and ao[p2] < ao[p1]:
                if not allow_zero_cross and np.nanmin(ao[p1:p2 + 1]) <= 0:
                    break
                return Divergence(
                    side="short", bar_index=last, pivot1_index=p1, pivot2_index=p2,
                    price1=price1, price2=price2, ao1=float(ao[p1]), ao2=float(ao[p2]),
                    magnitude=float(abs(ao[p2] - ao[p1]) / norm),
                    pivot_extreme=float(np.min(bars.low[j2:min(n, p2 + 1)])) if j2 <= p2 else float(bars.low[j2]),
                )
            break
    return None
