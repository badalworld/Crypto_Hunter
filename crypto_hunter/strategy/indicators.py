"""Vectorised indicators (numpy)."""
from __future__ import annotations

from typing import List, Tuple

import numpy as np


def sma(x: np.ndarray, n: int) -> np.ndarray:
    out = np.full_like(x, np.nan, dtype=float)
    if len(x) >= n:
        c = np.cumsum(np.insert(x.astype(float), 0, 0.0))
        out[n - 1:] = (c[n:] - c[:-n]) / n
    return out


def ema(x: np.ndarray, n: int) -> np.ndarray:
    out = np.full_like(x, np.nan, dtype=float)
    if len(x) < n:
        return out
    k = 2.0 / (n + 1)
    out[n - 1] = np.mean(x[:n])
    for i in range(n, len(x)):
        out[i] = x[i] * k + out[i - 1] * (1 - k)
    return out


def awesome_oscillator(high: np.ndarray, low: np.ndarray, fast: int = 5, slow: int = 34) -> np.ndarray:
    hl2 = (high + low) / 2.0
    return sma(hl2, fast) - sma(hl2, slow)


def true_range(high: np.ndarray, low: np.ndarray, close: np.ndarray) -> np.ndarray:
    prev_close = np.roll(close, 1)
    prev_close[0] = close[0]
    return np.maximum.reduce([high - low, np.abs(high - prev_close), np.abs(low - prev_close)])


def atr(high: np.ndarray, low: np.ndarray, close: np.ndarray, n: int = 14) -> np.ndarray:
    """Wilder's ATR."""
    tr = true_range(high, low, close)
    out = np.full_like(tr, np.nan, dtype=float)
    if len(tr) < n:
        return out
    out[n - 1] = np.mean(tr[:n])
    for i in range(n, len(tr)):
        out[i] = (out[i - 1] * (n - 1) + tr[i]) / n
    return out


def pivot_lows(x: np.ndarray, left: int, right: int) -> List[int]:
    """Indices i where x[i] is strictly lower than ``left`` bars before and <= ``right`` bars after."""
    idx: List[int] = []
    n = len(x)
    for i in range(left, n - right):
        v = x[i]
        if np.isnan(v):
            continue
        if np.all(x[i - left:i] > v) and np.all(x[i + 1:i + 1 + right] >= v):
            idx.append(i)
    return idx


def pivot_highs(x: np.ndarray, left: int, right: int) -> List[int]:
    idx: List[int] = []
    n = len(x)
    for i in range(left, n - right):
        v = x[i]
        if np.isnan(v):
            continue
        if np.all(x[i - left:i] < v) and np.all(x[i + 1:i + 1 + right] <= v):
            idx.append(i)
    return idx


def atr_pct(high: np.ndarray, low: np.ndarray, close: np.ndarray, n: int = 14) -> Tuple[float, float]:
    a = atr(high, low, close, n)
    if len(a) == 0 or np.isnan(a[-1]) or close[-1] <= 0:
        return float("nan"), float("nan")
    return float(a[-1]), float(a[-1] / close[-1] * 100.0)
