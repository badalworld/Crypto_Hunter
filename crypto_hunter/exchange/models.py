from __future__ import annotations

import math
from dataclasses import dataclass, field
from decimal import Decimal, ROUND_DOWN, ROUND_UP
from typing import Any, Dict, Optional


@dataclass
class Contract:
    symbol: str
    base_coin: str
    quote_coin: str
    settle_coin: str
    contract_size: float
    price_unit: float
    vol_unit: float
    min_vol: float
    max_vol: float
    price_scale: int
    vol_scale: int
    max_leverage: int
    min_leverage: int
    taker_fee: float
    maker_fee: float
    state: int
    api_allowed: bool
    raw: Dict[str, Any] = field(default_factory=dict, repr=False)

    @classmethod
    def from_api(cls, d: Dict[str, Any]) -> "Contract":
        return cls(
            symbol=d["symbol"],
            base_coin=d.get("baseCoin", ""),
            quote_coin=d.get("quoteCoin", ""),
            settle_coin=d.get("settleCoin", ""),
            contract_size=float(d.get("contractSize", 1)),
            price_unit=float(d.get("priceUnit", 10 ** -int(d.get("priceScale", 2)))),
            vol_unit=float(d.get("volUnit", 1)),
            min_vol=float(d.get("minVol", 1)),
            max_vol=float(d.get("maxVol", 1e12)),
            price_scale=int(d.get("priceScale", 2)),
            vol_scale=int(d.get("volScale", 0)),
            max_leverage=int(d.get("maxLeverage", 20)),
            min_leverage=int(d.get("minLeverage", 1)),
            taker_fee=float(d.get("takerFeeRate", 0.0006)),
            maker_fee=float(d.get("makerFeeRate", 0.0002)),
            state=int(d.get("state", 0)),
            api_allowed=bool(d.get("apiAllowed", True)),
            raw=d,
        )

    # ---------------------------------------------------------------- rounding
    def round_price(self, price: float, direction: str = "nearest") -> float:
        unit = Decimal(str(self.price_unit))
        p = Decimal(str(price))
        if direction == "down":
            q = (p / unit).to_integral_value(rounding=ROUND_DOWN) * unit
        elif direction == "up":
            q = (p / unit).to_integral_value(rounding=ROUND_UP) * unit
        else:
            q = (p / unit).to_integral_value() * unit
        return float(round(q, max(self.price_scale, 0)))

    def round_vol(self, vol: float) -> float:
        unit = Decimal(str(self.vol_unit))
        q = (Decimal(str(vol)) / unit).to_integral_value(rounding=ROUND_DOWN) * unit
        v = float(q)
        if self.vol_scale == 0:
            v = float(int(v))
        return v

    def notional(self, vol: float, price: float) -> float:
        return vol * self.contract_size * price


@dataclass
class Ticker:
    symbol: str
    last: float
    fair: float
    index: float
    bid: float
    ask: float
    volume24: float
    amount24: float
    rise_fall_rate: float
    ts: float

    @classmethod
    def from_api(cls, d: Dict[str, Any]) -> "Ticker":
        last = float(d.get("lastPrice") or 0)
        return cls(
            symbol=d["symbol"],
            last=last,
            fair=float(d.get("fairPrice") or last),
            index=float(d.get("indexPrice") or last),
            bid=float(d.get("bid1") or last),
            ask=float(d.get("ask1") or last),
            volume24=float(d.get("volume24") or 0),
            amount24=float(d.get("amount24") or 0),
            rise_fall_rate=float(d.get("riseFallRate") or 0),
            ts=float(d.get("timestamp") or 0) / 1000.0,
        )

    @property
    def spread_pct(self) -> float:
        if self.bid > 0 and self.ask > 0 and self.ask >= self.bid:
            return (self.ask - self.bid) / self.ask * 100.0
        return 0.0


@dataclass
class ExchangePosition:
    position_id: int
    symbol: str
    position_type: int        # 1 long, 2 short
    open_type: int
    hold_vol: float
    hold_avg_price: float
    open_avg_price: float
    liquidate_price: float
    im: float                 # initial margin
    leverage: int
    realised: float
    state: int
    unrealized: Optional[float]
    hold_fee: float = 0.0          # funding so far (+ received / - paid)
    raw: Dict[str, Any] = field(default_factory=dict, repr=False)

    @classmethod
    def from_api(cls, d: Dict[str, Any]) -> "ExchangePosition":
        return cls(
            position_id=int(d.get("positionId", 0)),
            symbol=d["symbol"],
            position_type=int(d.get("positionType", 1)),
            open_type=int(d.get("openType", 1)),
            hold_vol=float(d.get("holdVol", 0) or 0),
            hold_avg_price=float(d.get("holdAvgPrice", 0) or 0),
            open_avg_price=float(d.get("openAvgPrice", 0) or 0),
            liquidate_price=float(d.get("liquidatePrice", 0) or 0),
            im=float(d.get("im", 0) or 0),
            leverage=int(d.get("leverage", 1) or 1),
            realised=float(d.get("realised", 0) or 0),
            state=int(d.get("state", 1)),
            unrealized=(float(d["unRealizedPnl"]) if d.get("unRealizedPnl") is not None else None),
            hold_fee=float(d.get("holdFee", 0) or 0),
            raw=d,
        )

    @property
    def side(self) -> str:
        return "long" if self.position_type == 1 else "short"


@dataclass
class PositionLedger:
    """MEXC's own settlement numbers for a closed position (history_positions).

    realised == close_pnl + fee + funding  (fee is negative when paid).
    """
    position_id: int
    close_avg_price: float
    close_pnl: float          # closeProfitLoss – price PnL, fees excluded
    fee: float                # trading fees, signed as MEXC reports (negative = paid)
    total_fee: float          # absolute accumulated fees (open + close)
    funding: float            # holdFee (+ received / - paid)
    realised: float           # net PnL credited to the wallet
    profit_ratio: float       # realised / initial margin
    close_vol: float
    state: int

    @classmethod
    def from_api(cls, d: Dict[str, Any]) -> "PositionLedger":
        realised = float(d.get("realised", 0) or 0)
        close_pnl = float(d["closeProfitLoss"]) if d.get("closeProfitLoss") is not None else realised
        fee = float(d.get("fee", 0) or 0)
        total_fee = float(d.get("totalFee", abs(fee)) or abs(fee))
        return cls(
            position_id=int(d.get("positionId", 0)), close_avg_price=float(d.get("closeAvgPrice", 0) or 0),
            close_pnl=close_pnl, fee=fee, total_fee=total_fee, funding=float(d.get("holdFee", 0) or 0),
            realised=realised, profit_ratio=float(d.get("profitRatio", 0) or 0) * 100.0,
            close_vol=float(d.get("closeVol", 0) or 0), state=int(d.get("state", 3) or 3),
        )


@dataclass
class AccountAsset:
    currency: str
    equity: float
    available: float
    cash: float
    frozen: float
    position_margin: float
    unrealized: float

    @classmethod
    def from_api(cls, d: Dict[str, Any]) -> "AccountAsset":
        return cls(
            currency=d.get("currency", ""),
            equity=float(d.get("equity", 0) or 0),
            available=float(d.get("availableBalance", 0) or 0),
            cash=float(d.get("cashBalance", 0) or 0),
            frozen=float(d.get("frozenBalance", 0) or 0),
            position_margin=float(d.get("positionMargin", 0) or 0),
            unrealized=float(d.get("unrealized", 0) or 0),
        )


class MexcAPIError(Exception):
    def __init__(self, code: int, message: str, path: str = "", payload: Any = None):
        super().__init__(f"MEXC {path} -> code={code} msg={message}")
        self.code = code
        self.message = message
        self.path = path
        self.payload = payload

    # HTTP transport failures (status used as code) and MEXC's transient/system codes.
    _TRANSIENT = {429, 500, 502, 503, 504, 510, 600, 10073}

    @property
    def retryable(self) -> bool:
        return self.code in self._TRANSIENT


def is_nan(x: float) -> bool:
    return x is None or (isinstance(x, float) and math.isnan(x))
