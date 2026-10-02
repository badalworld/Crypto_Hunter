import asyncio
import hashlib
import hmac
import time

import numpy as np
import pytest

from crypto_hunter.config import BotConfig, ExitConfig, FilterConfig, RiskConfig
from crypto_hunter.exchange.models import Contract
from crypto_hunter.exchange.rate_limiter import RateLimiter
from crypto_hunter.exchange.rest import MexcFuturesREST
from crypto_hunter.risk.roi import price_for_roi, ratchet, roi_pct, trailing_stop_roi
from crypto_hunter.risk.sizing import compute_size, projection_curve, required_daily_growth
from crypto_hunter.security import Cipher, Credentials, MasterKey, SecretRedactingFilter
from crypto_hunter.strategy.divergence import Bars, detect_divergence
from crypto_hunter.strategy.filters import FilterContext, apply_filters
from crypto_hunter.strategy.indicators import atr, awesome_oscillator, ema, pivot_lows, sma


# ------------------------------------------------------------------ ROI maths
def test_roi_formulas():
    assert roi_pct("long", 100, 110, 10) == pytest.approx(100.0)
    assert roi_pct("short", 100, 90, 10) == pytest.approx(100.0)
    assert roi_pct("long", 100, 95, 10) == pytest.approx(-50.0)
    assert price_for_roi("long", 100, 200, 10) == pytest.approx(120.0)
    assert price_for_roi("short", 100, 200, 10) == pytest.approx(80.0)
    # round trip
    for side in ("long", "short"):
        p = price_for_roi(side, 1234.5, 37.0, 10)
        assert roi_pct(side, 1234.5, p, 10) == pytest.approx(37.0)


@pytest.mark.parametrize("peak,expected", [
    (0, None), (29.99, None), (30, 20), (35, 20), (39.99, 20), (40, 30), (50, 40), (100, 90), (199.9, 180),
])
def test_trailing_ladder(peak, expected):
    assert trailing_stop_roi(peak, ExitConfig()) == expected


def test_trailing_tp_clamp():
    cfg = ExitConfig()
    assert trailing_stop_roi(200, cfg) == 190  # never at/above TP
    assert trailing_stop_roi(500, cfg) == 190


def test_ratchet_never_moves_back():
    assert ratchet(None, None) is None
    assert ratchet(None, 20) == 20
    assert ratchet(40, 30) == 40
    assert ratchet(40, 50) == 50
    assert ratchet(40, None) == 40


def test_exit_config_validation():
    with pytest.raises(ValueError):
        ExitConfig(trail_initial_stop_roi=40, trail_start_roi=30)
    with pytest.raises(ValueError):
        ExitConfig(tp_roi=20, trail_start_roi=30)


# ------------------------------------------------------------------ indicators
def test_sma_ema_atr_shapes():
    x = np.arange(1, 51, dtype=float)
    s = sma(x, 5)
    assert np.isnan(s[3]) and s[4] == pytest.approx(3.0) and s[-1] == pytest.approx(48.0)
    e = ema(x, 10)
    assert np.isnan(e[8]) and e[-1] > e[-2]
    a = atr(x + 1, x - 1, x, 14)
    assert a[-1] == pytest.approx(2.0, rel=0.05)
    ao = awesome_oscillator(x + 1, x - 1, 5, 34)
    assert np.isnan(ao[32]) and ao[-1] > 0


def test_pivot_lows():
    x = np.array([5, 4, 3, 1, 2, 3, 4, 2, 0.5, 1, 2, 3, 4], dtype=float)
    assert pivot_lows(x, 2, 2) == [3, 8]


# ------------------------------------------------------------------ divergence
def _synthetic_bullish(n=160):
    """Price: two declining troughs (lower low); momentum of the 2nd decline is weaker (higher AO low)."""
    rng = np.random.default_rng(1)
    t = np.arange(n, dtype=float)
    base = 100 + 0.02 * t
    trough1 = -12 * np.exp(-((t - 80) ** 2) / (2 * 6 ** 2))  # sharp, deep dip
    trough2 = -14 * np.exp(-((t - 125) ** 2) / (2 * 14 ** 2))  # deeper price but much slower -> weaker AO
    close = base + trough1 + trough2 + rng.normal(0, 0.05, n)
    # make the last bars a confirmation push up
    close[-3:] += [0.5, 1.5, 3.0]
    high = close + 0.3
    low = close - 0.3
    vol = np.full(n, 100.0)
    vol[-1] = 500.0
    return Bars(time=t * 300, open=close, high=high, low=low, close=close, volume=vol)


def test_detects_bullish_divergence_once():
    bars = _synthetic_bullish()
    cfg = BotConfig().signal
    found = None
    for end in range(100, len(bars) + 1):
        sub = Bars(*(getattr(bars, f)[:end] for f in ("time", "open", "high", "low", "close", "volume")))
        d = detect_divergence(sub, cfg.ao_fast, cfg.ao_slow, cfg.pivot_left, cfg.pivot_right,
                              cfg.min_pivot_distance, cfg.max_pivot_distance)
        if d:
            found = (end, d)
            # subsequent bars must not re-fire the same pivot
            nxt = Bars(*(getattr(bars, f)[:end + 1] for f in ("time", "open", "high", "low", "close", "volume")))
            d2 = detect_divergence(nxt, cfg.ao_fast, cfg.ao_slow, cfg.pivot_left, cfg.pivot_right,
                                   cfg.min_pivot_distance, cfg.max_pivot_distance)
            assert d2 is None or d2.pivot2_index != d.pivot2_index
            break
    assert found is not None, "bullish divergence not detected"
    _, d = found
    assert d.side == "long"
    assert d.price2 < d.price1 and d.ao2 > d.ao1 and d.ao1 < 0 and d.ao2 < 0
    assert d.magnitude > 0


def test_filters_reject_and_pass():
    bars = _synthetic_bullish()
    cfg = BotConfig().signal
    d = None
    for end in range(100, len(bars) + 1):
        sub = Bars(*(getattr(bars, f)[:end] for f in ("time", "open", "high", "low", "close", "volume")))
        d = detect_divergence(sub, cfg.ao_fast, cfg.ao_slow, cfg.pivot_left, cfg.pivot_right, cfg.min_pivot_distance, cfg.max_pivot_distance)
        if d:
            bars = sub
            break
    assert d is not None
    f = FilterConfig(trend_filter="off", require_confirmation_close=False, volume_spike_enabled=False, min_atr_pct=0, min_divergence_magnitude=0)
    ctx = FilterContext(bars_5m=bars, bars_1h=None, atr_value=1.0, atr_pct=1.0, spread_pct=0.01)
    assert apply_filters(d, ctx, f).passed
    # cooldown blocks
    ctx2 = FilterContext(bars_5m=bars, bars_1h=None, atr_value=1.0, atr_pct=1.0, spread_pct=0.01, cooldown_until=time.time() + 100)
    r = apply_filters(d, ctx2, f)
    assert not r.passed and r.failed == ["cooldown"]
    # low ATR% blocks
    f3 = FilterConfig(trend_filter="off", require_confirmation_close=False, volume_spike_enabled=False, min_atr_pct=2.0, min_divergence_magnitude=0)
    assert "atr_pct" in apply_filters(d, ctx, f3).failed
    # spread blocks
    ctx4 = FilterContext(bars_5m=bars, bars_1h=None, atr_value=1.0, atr_pct=1.0, spread_pct=1.0)
    assert "spread" in apply_filters(d, ctx4, f).failed


# ------------------------------------------------------------------ sizing
def _contract(**kw):
    base = dict(symbol="X_USDT", baseCoin="X", quoteCoin="USDT", settleCoin="USDT", contractSize=0.001, priceUnit=0.1,
                volUnit=1, minVol=1, maxVol=1e9, priceScale=1, volScale=0, maxLeverage=200, minLeverage=1,
                takerFeeRate=0.0006, makerFeeRate=0.0002, state=0, apiAllowed=True)
    base.update(kw)
    return Contract.from_api(base)


def test_margin_sizing_compounds_from_equity():
    c = _contract(contractSize=0.0001)              # 5 USDT notional per contract at 50k
    r = RiskConfig(leverage=10, risk_per_trade_pct=8)
    s1 = compute_size(100, 100, 50_000, 300, c, r)  # margin 8 -> notional 80 -> 16 contracts
    s2 = compute_size(200, 200, 50_000, 300, c, r)  # equity doubled -> size doubled (compounding)
    assert s1.vol == 16 and s2.vol == 32
    assert s1.margin == pytest.approx(16 * 0.0001 * 50_000 / 10)


def test_sizing_respects_min_vol_and_balance():
    c = _contract(contractSize=1, minVol=1)
    r = RiskConfig(leverage=10, risk_per_trade_pct=8)
    s = compute_size(10, 10, 100, 1, c, r)      # margin 0.8 -> notional 8 -> 0.08 contracts < minVol
    assert s.vol == 0 and "minVol" in (s.reason or "")
    s = compute_size(1000, 5, 100, 1, c, r)     # only 5 available
    assert s.margin <= 5


def test_stop_risk_sizing():
    c = _contract(contractSize=1, priceUnit=0.01, priceScale=2)
    r = RiskConfig(leverage=10, risk_per_trade_pct=8, sizing_mode="stop_risk")
    s = compute_size(1000, 1000, 100, 2.0, c, r)   # risk 80 USDT over a 2.0 stop -> 40 units, capped at 2x margin mode (160 margin)
    assert s.qty <= 16 * 1  # 160 margin * 10 / 100 = 16 units cap
    assert s.vol > 0


def test_contract_rounding():
    c = _contract(priceUnit=0.5, priceScale=1, volUnit=10)
    assert c.round_price(100.26, "down") == 100.0
    assert c.round_price(100.26, "up") == 100.5
    assert c.round_vol(129) == 120


def test_projection():
    curve = projection_curve(100, 10_000, 7, start_ts=0, points_per_day=1)
    assert len(curve) == 8 and curve[0]["equity"] == 100 and curve[-1]["equity"] == pytest.approx(10_000)
    assert required_daily_growth(100, 10_000, 7) == pytest.approx(93.07, rel=1e-3)


# ------------------------------------------------------------------ signing & security
def test_signature_matches_spec():
    rl = RateLimiter()
    c = MexcFuturesREST("https://api.mexc.com", rl, Credentials("ak", "sk"))
    qs = c._query_string({"symbol": "BTC_USDT", "page_num": 1, "empty": None})
    assert qs == "page_num=1&symbol=BTC_USDT"
    sig = c._sign("1700000000000", qs)
    assert sig == hmac.new(b"sk", b"ak1700000000000page_num=1&symbol=BTC_USDT", hashlib.sha256).hexdigest()


def test_cipher_roundtrip_and_redaction(tmp_path):
    key = MasterKey.load_or_create(tmp_path, None)
    assert (tmp_path / ".master_key").exists()
    assert MasterKey.load_or_create(tmp_path, None) == key
    ci = Cipher(key)
    assert ci.decrypt(ci.encrypt("supersecretvalue")) == "supersecretvalue"
    f = SecretRedactingFilter()
    f.register("supersecretvalue")
    assert f.redact("token supersecretvalue leaked") == "token *** leaked"
    assert "***" in f.redact('{"apiKey": "mx0abcdefghijklmnop"}')


# ------------------------------------------------------------------ rate limiter
def test_rate_limiter_caps_at_fraction():
    async def run():
        rl = RateLimiter(fraction=0.95, limits={"x": (20, 2.0)})
        t0 = time.monotonic()
        for _ in range(19):
            await rl.acquire("/api/v1/x")
        fast = time.monotonic() - t0
        await rl.acquire("/api/v1/x")  # 20th must wait for the window
        slow = time.monotonic() - t0
        return fast, slow
    fast, slow = asyncio.run(run())
    assert fast < 0.5 and slow >= 1.9


def test_rate_limiter_key_normalisation():
    assert RateLimiter.key_for("/api/v1/contract/kline/BTC_USDT?interval=Min5") == "contract/kline"
    assert RateLimiter.key_for("/api/v1/private/order/get/123") == "private/order/get"
    assert RateLimiter.key_for("/api/v1/private/order/create") == "private/order/create"
