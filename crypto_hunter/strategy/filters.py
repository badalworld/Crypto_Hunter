"""Fake-signal filters applied before entry.  Each returns (passed, reason)."""
from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

import numpy as np

from ..config import FilterConfig
from .divergence import Bars, Divergence
from .indicators import atr as atr_fn, ema


@dataclass
class FilterContext:
    bars_5m: Bars
    bars_1h: Optional[Bars]
    atr_value: float
    atr_pct: float
    spread_pct: float
    cooldown_until: Optional[float] = None
    now: float = field(default_factory=time.time)


@dataclass
class FilterResult:
    passed: bool
    checks: Dict[str, Tuple[bool, str]]

    @property
    def failed(self) -> List[str]:
        return [k for k, (ok, _) in self.checks.items() if not ok]

    def to_dict(self) -> Dict:
        return {k: {"ok": ok, "detail": d} for k, (ok, d) in self.checks.items()}


def _trend_ok(bars: Bars, side: str, fast: int, slow: int) -> Tuple[bool, str]:
    if len(bars) < slow + 1:
        return False, f"insufficient bars for EMA{slow} ({len(bars)})"
    ef, es = ema(bars.close, fast), ema(bars.close, slow)
    f, s = float(ef[-1]), float(es[-1])
    if not np.isfinite(f) or not np.isfinite(s):
        return False, "EMA not available"
    if side == "long":
        return f > s, f"EMA{fast}={f:.6g} {'>' if f > s else '<='} EMA{slow}={s:.6g}"
    return f < s, f"EMA{fast}={f:.6g} {'<' if f < s else '>='} EMA{slow}={s:.6g}"


def apply_filters(div: Divergence, ctx: FilterContext, cfg: FilterConfig) -> FilterResult:
    checks: Dict[str, Tuple[bool, str]] = {}
    b = ctx.bars_5m

    # 1. Divergence magnitude
    if cfg.min_divergence_magnitude > 0:
        ok = div.magnitude >= cfg.min_divergence_magnitude
        checks["magnitude"] = (ok, f"{div.magnitude:.3f} vs min {cfg.min_divergence_magnitude}")

    # 2. Confirmation close beyond the 2nd pivot candle's extreme
    if cfg.require_confirmation_close:
        c = float(b.close[-1])
        if div.side == "long":
            ok = c > div.pivot_extreme
            checks["confirmation"] = (ok, f"close {c:.6g} {'>' if ok else '<='} pivot high {div.pivot_extreme:.6g}")
        else:
            ok = c < div.pivot_extreme
            checks["confirmation"] = (ok, f"close {c:.6g} {'<' if ok else '>='} pivot low {div.pivot_extreme:.6g}")

    # 3. Volume spike
    if cfg.volume_spike_enabled:
        n = cfg.volume_lookback
        if len(b.volume) > n + 1:
            avg = float(np.mean(b.volume[-n - 1:-1]))
            cur = float(b.volume[-1])
            ok = avg > 0 and cur > avg * cfg.volume_multiplier
            checks["volume_spike"] = (ok, f"vol {cur:.4g} vs {cfg.volume_multiplier}x avg {avg:.4g}")
        else:
            checks["volume_spike"] = (False, "insufficient volume history")

    # 4. Trend alignment
    if cfg.trend_filter in ("ema_5m", "both"):
        checks["trend_5m"] = _trend_ok(b, div.side, cfg.ema_fast, cfg.ema_slow)
    if cfg.trend_filter in ("ema_1h", "both"):
        if ctx.bars_1h is None:
            checks["trend_1h"] = (False, "no 1h bars")
        else:
            checks["trend_1h"] = _trend_ok(ctx.bars_1h, div.side, cfg.ema_fast, cfg.ema_slow)

    # 5. Minimum ATR%
    if cfg.min_atr_pct > 0:
        ok = np.isfinite(ctx.atr_pct) and ctx.atr_pct >= cfg.min_atr_pct
        checks["atr_pct"] = (ok, f"ATR% {ctx.atr_pct:.3f} vs min {cfg.min_atr_pct}")

    # 6. Cooldown
    if ctx.cooldown_until and ctx.cooldown_until > ctx.now:
        checks["cooldown"] = (False, f"cooldown {int(ctx.cooldown_until - ctx.now)}s remaining")
    else:
        checks["cooldown"] = (True, "none")

    # 7. Spread
    if cfg.max_spread_pct > 0:
        ok = ctx.spread_pct <= cfg.max_spread_pct
        checks["spread"] = (ok, f"spread {ctx.spread_pct:.3f}% vs max {cfg.max_spread_pct}%")

    return FilterResult(passed=all(ok for ok, _ in checks.values()), checks=checks)


def compute_atr(bars: Bars, period: int) -> Tuple[float, float]:
    a = atr_fn(bars.high, bars.low, bars.close, period)
    if len(a) == 0 or not np.isfinite(a[-1]) or bars.close[-1] <= 0:
        return float("nan"), float("nan")
    return float(a[-1]), float(a[-1] / bars.close[-1] * 100.0)
