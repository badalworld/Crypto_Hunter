"""
Crypto Hunter configuration.

All strategy / risk / execution parameters live in ``BotConfig``.  Values are loaded in
this order (later wins):

1. Built-in defaults (below)
2. ``config.yaml`` in the working directory (or ``CH_CONFIG`` env var)
3. Runtime overrides saved from the dashboard (persisted in SQLite ``settings`` table)

API credentials are *never* part of this model – they are stored encrypted by
``crypto_hunter.security.CredentialStore``.
"""
from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Dict, Literal, Optional

import yaml
from pydantic import BaseModel, Field, field_validator, model_validator


class SignalConfig(BaseModel):
    timeframe: str = Field("Min5", description="MEXC kline interval used for signals")
    ao_fast: int = Field(5, ge=2)
    ao_slow: int = Field(34, ge=5)
    atr_period: int = Field(14, ge=2)
    pivot_left: int = Field(3, ge=1, description="Bars to the left required for an AO pivot")
    pivot_right: int = Field(2, ge=1, description="Bars to the right required (confirmation lag)")
    min_pivot_distance: int = Field(5, ge=2, description="Minimum bars between the two divergence pivots")
    max_pivot_distance: int = Field(60, ge=5, description="Maximum bars between the two divergence pivots")
    allow_zero_cross_between_pivots: bool = Field(
        True, description="If false, AO must stay on one side of zero between the two pivots (strict Bill Williams style)",
    )
    history_bars: int = Field(400, ge=120, description="Bars kept per symbol for indicator warm-up")

    @model_validator(mode="after")
    def _check(self) -> "SignalConfig":
        if self.ao_fast >= self.ao_slow:
            raise ValueError("ao_fast must be < ao_slow")
        if self.min_pivot_distance >= self.max_pivot_distance:
            raise ValueError("min_pivot_distance must be < max_pivot_distance")
        return self


class FilterConfig(BaseModel):
    """Fake-signal filters.  Every filter can be disabled individually."""

    min_divergence_magnitude: float = Field(
        0.15, ge=0.0,
        description="|AO2-AO1| normalised by the mean |AO| of the lookback window. 0 disables.",
    )
    require_confirmation_close: bool = Field(
        True, description="Last closed candle must close beyond the second pivot candle's extreme",
    )
    volume_spike_enabled: bool = True
    volume_lookback: int = Field(20, ge=3)
    volume_multiplier: float = Field(1.5, ge=0.0)
    trend_filter: Literal["off", "ema_5m", "ema_1h", "both"] = Field(
        "ema_5m", description="EMA 50/200 alignment on 5m, higher timeframe bias on 1h, or both",
    )
    ema_fast: int = Field(50, ge=2)
    ema_slow: int = Field(200, ge=5)
    min_atr_pct: float = Field(0.35, ge=0.0, description="Minimum ATR% (ATR/close*100) to trade")
    cooldown_after_loss_minutes: int = Field(60, ge=0)
    cooldown_after_any_exit_minutes: int = Field(10, ge=0)
    max_spread_pct: float = Field(0.15, ge=0.0, description="Skip entries when bid/ask spread exceeds this %")


class ScannerConfig(BaseModel):
    top_n: int = Field(12, ge=1, le=60, description="Number of symbols to actively watch/trade")
    candidates_by_volume: int = Field(60, ge=5, le=200, description="Pre-filter by 24h quote volume before ATR ranking")
    min_quote_volume_24h: float = Field(20_000_000, ge=0, description="USDT 24h turnover floor")
    min_price: float = Field(0.0, ge=0.0)
    rescan_interval_sec: int = Field(120, ge=30)
    volatility_weight: float = Field(0.6, ge=0.0, le=1.0, description="Score weight for ATR%; remainder goes to volume")
    quote_coin: str = "USDT"
    symbol_blacklist: list[str] = Field(default_factory=list)
    symbol_whitelist: list[str] = Field(default_factory=list, description="If non-empty only these symbols are considered")


class RiskConfig(BaseModel):
    leverage: int = Field(10, ge=1, le=200)
    risk_per_trade_pct: float = Field(8.0, gt=0.0, le=100.0, description="% of equity used as margin per trade")
    sizing_mode: Literal["margin", "stop_risk"] = Field(
        "margin",
        description="margin: margin = equity*risk%. stop_risk: size so a stop-loss hit loses equity*risk%",
    )
    max_open_positions: int = Field(10, ge=1, le=50)
    max_positions_per_symbol: int = Field(1, ge=1, le=2)
    atr_stop_multiplier: float = Field(3.0, gt=0.0)
    open_type: Literal["isolated", "cross"] = "isolated"
    max_daily_loss_pct: float = Field(25.0, ge=0.0, description="Pause new entries after this % daily drawdown (0 disables)")
    min_equity_usdt: float = Field(5.0, ge=0.0, description="Do not trade below this equity")
    target_equity_usdt: float = Field(10_000.0, gt=0.0)
    target_days: int = Field(7, ge=1)


class ExitConfig(BaseModel):
    """ROI-on-margin based exits.  ROI% = price_change% * leverage."""

    tp_roi: float = Field(200.0, gt=0.0)
    trail_start_roi: float = Field(30.0, ge=0.0)
    trail_initial_stop_roi: float = Field(20.0, ge=0.0)
    trail_step_roi: float = Field(10.0, gt=0.0)
    trail_stop_step_roi: float = Field(10.0, gt=0.0)
    peak_price_source: Literal["fair", "last", "both"] = Field(
        "both", description="Price used to track peak ROI: mark (fair) price, last trade, or max of both",
    )
    software_failsafe: bool = Field(True, description="Market-close if price breaches the stop and the exchange stop did not fire")
    failsafe_grace_sec: float = Field(3.0, ge=0.0)
    failsafe_breach_pct: float = Field(0.05, ge=0.0, description="Extra % beyond the stop price before failsafe fires")

    @model_validator(mode="after")
    def _check(self) -> "ExitConfig":
        if self.trail_initial_stop_roi > self.trail_start_roi:
            raise ValueError("trail_initial_stop_roi must be <= trail_start_roi")
        if self.trail_start_roi >= self.tp_roi:
            raise ValueError("trail_start_roi must be < tp_roi")
        return self


class ExecutionConfig(BaseModel):
    rest_base_url: str = "https://api.mexc.com"
    legacy_rest_base_url: str = Field(
        "https://contract.mexc.com", description="Fallback host for public market data if the primary host fails",
    )
    ws_url: str = "wss://contract.mexc.com/edge"
    recv_window_sec: int = Field(10, ge=1, le=60)
    rate_limit_fraction: float = Field(0.95, gt=0.0, le=1.0, description="Fraction of MEXC limits we allow ourselves to use")
    http_timeout_sec: float = Field(10.0, gt=0.0)
    max_retries: int = Field(4, ge=0)
    retry_backoff_base_sec: float = Field(0.25, gt=0.0)
    connection_pool_size: int = Field(32, ge=1)
    entry_order_type: Literal["market", "ioc"] = "market"
    position_sync_interval_sec: float = Field(5.0, ge=1.0)
    equity_snapshot_interval_sec: float = Field(30.0, ge=5.0)
    price_trend_source: Literal["last", "fair"] = Field(
        "fair", description="Trigger reference for exchange TP/SL (lossTrend/profitTrend)",
    )


class BotConfig(BaseModel):
    signal: SignalConfig = Field(default_factory=SignalConfig)
    filters: FilterConfig = Field(default_factory=FilterConfig)
    scanner: ScannerConfig = Field(default_factory=ScannerConfig)
    risk: RiskConfig = Field(default_factory=RiskConfig)
    exits: ExitConfig = Field(default_factory=ExitConfig)
    execution: ExecutionConfig = Field(default_factory=ExecutionConfig)
    auto_start: bool = Field(False, description="Start trading automatically when credentials exist")

    # ------------------------------------------------------------------ helpers
    @classmethod
    def load(cls, path: Optional[str | Path] = None, overrides: Optional[Dict[str, Any]] = None) -> "BotConfig":
        path = Path(path or os.environ.get("CH_CONFIG", "config.yaml"))
        data: Dict[str, Any] = {}
        if path.exists():
            with path.open("r", encoding="utf-8") as fh:
                data = yaml.safe_load(fh) or {}
        if overrides:
            data = deep_merge(data, overrides)
        return cls.model_validate(data)

    def merged(self, patch: Dict[str, Any]) -> "BotConfig":
        """Return a validated copy with ``patch`` applied (used by the dashboard)."""
        return BotConfig.model_validate(deep_merge(self.model_dump(mode="json"), patch))

    @property
    def open_type_code(self) -> int:
        return 1 if self.risk.open_type == "isolated" else 2

    @property
    def trend_code(self) -> int:
        return 2 if self.execution.price_trend_source == "fair" else 1


def deep_merge(base: Dict[str, Any], patch: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(base)
    for k, v in (patch or {}).items():
        if isinstance(v, dict) and isinstance(out.get(k), dict):
            out[k] = deep_merge(out[k], v)
        else:
            out[k] = v
    return out


class AppSettings(BaseModel):
    """Process-level settings sourced from environment variables."""

    host: str = os.environ.get("CH_HOST", "0.0.0.0")
    port: int = int(os.environ.get("CH_PORT", "8080"))
    data_dir: Path = Path(os.environ.get("CH_DATA_DIR", "data"))
    db_path: Path = Path(os.environ.get("CH_DB_PATH", os.path.join(os.environ.get("CH_DATA_DIR", "data"), "crypto_hunter.sqlite")))
    log_level: str = os.environ.get("CH_LOG_LEVEL", "INFO")
    log_file: Optional[str] = os.environ.get("CH_LOG_FILE", os.path.join("logs", "crypto_hunter.log"))
    dashboard_token: Optional[str] = os.environ.get("CH_DASHBOARD_TOKEN") or None
    master_key: Optional[str] = os.environ.get("CH_MASTER_KEY") or None
    config_path: str = os.environ.get("CH_CONFIG", "config.yaml")

    @field_validator("log_level")
    @classmethod
    def _upper(cls, v: str) -> str:
        return v.upper()
