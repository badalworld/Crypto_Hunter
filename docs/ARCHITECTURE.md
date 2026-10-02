# Crypto Hunter – Architecture

```
                         ┌─────────────────────────────────────────────────────────────┐
                         │                        MEXC Futures                         │
                         │  REST https://api.mexc.com      WS wss://contract.mexc.com  │
                         └───────┬──────────────────────────────┬──────────────┬───────┘
          signed REST (orders,   │            public WS         │   private WS │
          TP/SL, account, kline) │   (tickers, 5m klines)       │ (positions,  │
                                 │                              │  assets)     │
┌────────────────────────────────▼──────────────────────────────▼──────────────▼────────┐
│  exchange/                                                                            │
│   rest.py  ── RateLimiter (95 % of per-endpoint limits) ── retry/backoff ── pooling   │
│   ws_public.py (reconnect + resubscribe)      ws_private.py (login, reconnect)        │
└───────┬──────────────────────────┬───────────────────────────────────┬────────────────┘
        │                          │                                   │
┌───────▼───────┐        ┌─────────▼──────────┐              ┌─────────▼──────────┐
│ strategy/     │        │ engine/market_data │              │ engine/position_   │
│  scanner.py   │◄──────►│  KlineStore        │─── ticks ───►│  manager.py        │
│  (ATR% ×      │        │  TickerCache       │              │  peak ROI, ladder, │
│   volume rank)│        └─────────┬──────────┘              │  ratchet, failsafe │
└───────┬───────┘                  │ closed 5m bars          └─────────┬──────────┘
        │ watch-list     ┌─────────▼──────────┐                        │ change_plan_price
        │                │ strategy/          │                        │ market close
        │                │  divergence.py (AO)│                        ▼
        │                │  filters.py        │              ┌────────────────────┐
        │                └─────────┬──────────┘              │ engine/executor.py │
        │                          │ confirmed signal        │  entry+TP/SL, fill │
        │                ┌─────────▼──────────┐              │  poll, close,      │
        └───────────────►│ engine/bot.py      │─────────────►│  fallback triggers │
                         │  gates · sizing ·  │              └────────────────────┘
                         │  loops · metrics   │
                         └───┬────────────┬───┘
                             │            │ broadcast (snapshot / event / trade / position)
                   ┌─────────▼──┐   ┌─────▼───────────────┐
                   │ persistence│   │ api/server.py       │   /ws  ─────►  dashboard
                   │  SQLite WAL│   │ FastAPI + WebSocket │   /api/*       (static/)
                   └────────────┘   └─────────────────────┘
```

## 1. Signal logic (`strategy/`)

**Timeframe** – 5-minute MEXC klines (`Min5`). Bars are back-filled by REST (400 bars),
kept live through `push.kline`, and the last three bars are re-validated by REST two seconds
after every candle close. Signals are evaluated only on *closed* bars.

**Awesome Oscillator** – `AO = SMA5(HL2) − SMA34(HL2)`.

**Divergence detection** (`divergence.py`)

1. Find AO pivots: a bar is a pivot low when AO is strictly below the `pivot_left` bars
   before it and ≤ the `pivot_right` bars after it (symmetrically for pivot highs).
   `pivot_right` is the confirmation lag – a divergence fires exactly once, when its
   second pivot becomes confirmed.
2. Compare the latest pivot with the most recent earlier pivot `min_pivot_distance …
   max_pivot_distance` bars away.
3. *Bullish*: price low₂ < price low₁ **and** AO₂ > AO₁ with both AO values < 0.
   *Bearish*: price high₂ > price high₁ **and** AO₂ < AO₁ with both AO values > 0.
   Price extremes are taken from a ±2-bar window around each AO pivot.
4. **Magnitude** = |AO₂ − AO₁| / mean|AO| of the lookback window (dimensionless, so one
   threshold works across assets of any price).

**Fake-signal filters** (`filters.py`, all configurable, all must pass):

| Filter | Default | Rule |
|---|---|---|
| Magnitude | 0.15 | divergence magnitude ≥ threshold |
| Confirmation close | on | last closed candle closes above the 2nd-pivot candle's high (long) / below its low (short) |
| Volume spike | 1.5× / 20 | signal-candle volume > multiplier × average of previous N candles |
| Trend alignment | `ema_5m` | EMA50 > EMA200 for longs, < for shorts (`ema_1h` / `both` use 1h bars) |
| Minimum ATR% | 0.35 % | ATR14 / close × 100 ≥ threshold |
| Cooldown | 60 min / 10 min | per-symbol block after a loss / after any exit |
| Spread | 0.15 % | bid/ask spread below threshold |

Every rejected signal is logged with the failing checks and shown in the dashboard
**Signals** tab.

**Asset selection** (`scanner.py`) – every `rescan_interval_sec`: all USDT-settled perpetuals
with `apiAllowed`, state 0 and 24h turnover ≥ `min_quote_volume_24h` are pre-filtered, the
top `candidates_by_volume` are ranked by `score = w·rank(ATR%) + (1−w)·rank(amount24)` and
the top-N become the watch-list (kline + ticker subscriptions follow the watch-list).

## 2. Risk engine (`risk/`, `engine/bot.py`)

**Entry gates** (checked under a lock, in order): bot RUNNING (not PAUSED by the daily-loss
guard), fresh account data, equity ≥ `min_equity_usdt`, open positions < `max_open_positions`,
no position on the symbol, no opposite exchange position.

**Sizing** (`sizing.py`) – recomputed from *current equity* on every entry (compounding):

* `margin` mode (default): `margin = equity × risk% ; notional = margin × leverage`
* `stop_risk` mode: size so that a stop-loss hit loses `equity × risk%` (capped at 2× margin mode)

Contracts are rounded to `volUnit`, checked against `minVol/maxVol`, and the margin is capped
by available balance minus a fee buffer.

**Initial stop** – `3 × ATR14` from the fill price (clamped inside the liquidation band,
≈ −85 % ROI). **Take-profit** – `+200 % ROI` (price = entry × (1 ± 2.0/leverage)).
Both are sent *with the entry order* (`stopLossPrice` / `takeProfitPrice`), so the exchange
holds the protection before the fill is even confirmed.

**Stepped trailing stop** (`roi.py`) – ROI on margin:

```
ROI%  = ±(price − entry) / entry × 100 × leverage
stop  = None                                        if peak < TRAIL_START_ROI (30)
stop  = floor((peak − 30) / 10) × 10 + 20           otherwise  (30→20, 40→30, 100→90)
stop  = min(stop, TP_ROI − step)                    never at/above TP
```

The stop ROI is **ratcheted** (`max(old, new)`), converted to a price, rounded to the
contract tick in the conservative direction, and pushed to MEXC with **one**
`stoporder/change_plan_price` per step. Peak ROI is tracked from every ticker push using
fair (mark) price, last price or the better of both (`peak_price_source`).

**Failsafe** – if mark *and* last price sit beyond the stop (or beyond TP) for
`failsafe_grace_sec` and the exchange order has not fired, a reduce-side market order closes
the position (`reason = FAILSAFE`).

**Daily loss guard** – when equity falls `max_daily_loss_pct` below the UTC-day start, new
entries pause (`PAUSED`) while open positions keep being managed.

**Projection** – the dashboard draws the geometric curve from session-start equity to
`target_equity_usdt` over `target_days` and the required %/day next to actual equity.

## 3. Execution (`engine/executor.py`, `exchange/rest.py`)

* Entry: market (`type 5`) or IOC (`type 3`) via `POST /api/v1/private/order/create`;
  `side` 1/3 open long/short, `openType` isolated by default, `leverage` set via
  `position/change_leverage` first.
* Fill: `GET order/get/{id}` polled with increasing delay until state 3 (filled); the real
  `dealAvgPrice` re-anchors SL/TP.
* Protection handle: `GET stoporder/open_orders` → plan order id for the position →
  `change_plan_price` for every trailing step. If no plan order exists (e.g. adopted
  positions) reduce-only trigger orders (`planorder/place/v2`) are used instead.
* Close: `order/create` with the close side (4 = close long, 2 = close short) and
  `positionId`; `reduceOnly=true` in one-way mode. Close orders cannot add exposure.
* Latency: keep-alive pooled `aiohttp` session, `Recv-Window` 10 s, exponential back-off with
  jitter on network/5xx/429 errors, business errors surfaced immediately.
* Rate limits: a sliding-window bucket per endpoint sized to 95 % of MEXC's documented
  limit (20 req / 2 s for all used endpoints, 1 / 5 s for `contract/detail`) shared by the
  scanner, signal engine, executor and trailing manager; live usage is shown in the
  dashboard **API budget** tab.

## 4. State persistence (`persistence/db.py`)

SQLite (WAL) tables:

| Table | Purpose |
|---|---|
| `credentials` | Fernet-encrypted API key + secret |
| `settings` | dashboard overrides of `config.yaml` |
| `positions` | every managed position: entry, vol, leverage, margin, ATR, initial/current stop, **stop ROI**, **peak ROI / peak price**, TP, `stop_plan_order_id`, fallback trigger ids, status |
| `trades` | closed trades with exit price, PnL, ROI, peak ROI, reason (`TP`/`SL`/`TRAIL`/`FAILSAFE`/`MANUAL`) |
| `equity_snapshots` | equity curve |
| `cooldowns` | per-symbol no-trade-until |
| `events` | audit log streamed to the dashboard |
| `kv` | session start equity/time, UTC-day start equity |

On start the manager reloads `positions`, then **reconciles** against
`position/open_positions`: positions closed while offline are finalised (exit price and
realised PnL from `history_positions`), positions found on the exchange but unknown to the
bot are adopted (existing TP/SL discovered or new protection armed).
Trailing state therefore survives restarts and crashes; the ladder never resets.

## 5. Dashboard (`api/`)

FastAPI serves the static SPA and a `/ws` stream. The server pushes a full snapshot every
second (status, account, positions with live ROI/peak/stop, watch-list, metrics) plus
immediate `event`, `trade`, `position`, `signal`, `watchlist` messages. Nothing is polled
or simulated client-side; the only client → server calls are the control actions
(start/stop, credentials, config, manual close).
