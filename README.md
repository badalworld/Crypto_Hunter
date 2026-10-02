# Crypto Hunter

Automated **MEXC USDT-perpetual futures** trading bot: Awesome-Oscillator divergence on the
5-minute chart, traded on the most volatile & liquid contracts, with ROI-based take-profit, a
stepped trailing stop that ratchets on the exchange side, strict risk gates, persistent state
and a real-time glassmorphism dashboard.

> **This bot places real orders with real money.** There is no paper/simulation mode.
> Futures with 10× leverage and 8 % margin per trade can lose the full margin of every
> position. The $10 000-in-7-days projection on the dashboard is a *goal curve* (≈ +93 %/day
> from $100) – it is not a forecast, and nothing in this repository guarantees profit.
> Start with capital you can afford to lose entirely.

---

## Features

| Area | What it does |
|---|---|
| Signal | Bullish / bearish AO divergence (5m), pivot-confirmed, dimensionless magnitude |
| Asset selection | Scans all MEXC USDT perps, ranks by ATR% + 24h quote volume, trades top-N |
| Fake-signal filters | Magnitude threshold, confirmation close, volume spike, EMA 50/200 trend (5m / 1h), min ATR%, per-symbol cooldown, max spread |
| Sizing | 8 % of **current** equity as margin × 10× leverage (compounding), or stop-risk sizing |
| Risk | Max 10 concurrent positions, 3×ATR initial SL, daily-loss pause, liquidation-band clamp |
| Exits | TP at +200 % ROI; trailing ladder 30→20, 40→30, …, 100→90 % ROI, exchange-side `change_plan_price`, software failsafe |
| Execution | Signed REST on `api.mexc.com`, pooled keep-alive, retry/back-off, per-endpoint limiter at 95 % of MEXC limits |
| Market data | `wss://contract.mexc.com/edge` tickers + klines, private stream for positions/assets, auto-reconnect |
| Persistence | SQLite WAL: positions (peak ROI, stop ROI, order ids), trades, equity, cooldowns, events |
| Dashboard | Live equity vs projection, positions with ROI/peak/stop, trade history, metrics, signals, API budget, settings |
| Security | API keys Fernet-encrypted at rest, log redaction, optional dashboard bearer token |

## Quick start

```bash
git clone <this repo> && cd Crypto_Hunter
python3.11 -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt

cp .env.example .env
# generate a master key for credential encryption and (recommended) a dashboard token
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
python -c "import secrets; print(secrets.token_urlsafe(32))"
#   -> put them into .env as CH_MASTER_KEY / CH_DASHBOARD_TOKEN
set -a; . ./.env; set +a

python -m crypto_hunter
# dashboard: http://localhost:8080/?token=<CH_DASHBOARD_TOKEN>
```

1. Open **Settings → MEXC API credentials**, paste the API key + secret, **Save & verify**
   (the bot calls `account/assets` to verify before storing them encrypted).
2. Review the parameters (all pre-filled from `config.yaml`), **Apply configuration**.
3. Press **▶ Start**. The scanner builds the watch-list within a few seconds, signals are
   evaluated at every 5-minute close.

### MEXC account prerequisites

* KYC-verified account; API key with **Futures → Order placing** (and read) permission,
  IP-bound. Unbound keys expire after 90 days.
* USDT in the **Futures** wallet.
* Leave the account in the default **hedge** position mode (one-way is also supported – the
  bot detects the mode and sets `reduceOnly` accordingly).
* Futures API order placement was re-opened by MEXC on 2026-03-31 (see their changelog). If
  `order/create` returns a permission / maintenance error, check your key's futures
  permission in the MEXC user centre.

## Configuration

Everything except the API keys lives in [`config.yaml`](config.yaml) and can be changed live
from the dashboard (overrides are persisted in the DB and survive restarts). Key parameters:

```yaml
risk:    leverage 10 · risk_per_trade_pct 8 · max_open_positions 10 · atr_stop_multiplier 3
exits:   tp_roi 200 · trail_start_roi 30 · trail_initial_stop_roi 20 · trail_step_roi 10 · trail_stop_step_roi 10
filters: min_divergence_magnitude 0.15 · require_confirmation_close · volume_multiplier 1.5
         trend_filter ema_5m · min_atr_pct 0.35 · cooldown_after_loss_minutes 60
scanner: top_n 12 · min_quote_volume_24h 20 000 000 · volatility_weight 0.6
```

Process-level settings (host/port, data dir, log file, master key, dashboard token) come from
environment variables – see [`.env.example`](.env.example).

## Running in production

**Docker**

```bash
docker compose up -d --build        # binds 127.0.0.1:8080, data/ and logs/ are volumes
```

**systemd** – see [`deploy/crypto-hunter.service`](deploy/crypto-hunter.service)
(`/opt/crypto_hunter`, dedicated `hunter` user, hardened unit).

Operational notes

* Put a TLS reverse proxy (Caddy/nginx) in front if you access the dashboard remotely and
  always set `CH_DASHBOARD_TOKEN`.
* Back up `data/` (SQLite DB + `.master_key` if you did not set `CH_MASTER_KEY`). Losing the
  master key means re-entering the API credentials – nothing else.
* Keep server time NTP-synced; MEXC rejects requests whose `Request-Time` drifts > 10 s.
* Logs rotate at 20 MB × 5 in `logs/`; secrets are scrubbed by a logging filter.
* `/healthz` returns `{"ok":true,"state":...}` for liveness probes.
* Stopping the bot leaves open positions protected by their exchange-side SL/TP, but the
  trailing ladder only advances while the bot runs. On restart it resumes from the persisted
  peak/stop state and reconciles with the exchange.

## Tests

```bash
pytest -q
```

* `tests/test_core.py` – ROI/ladder maths, indicators, divergence detection, filters, sizing,
  signing, encryption/redaction, rate limiter.
* `tests/test_integration.py` – full engine lifecycle against an in-process MEXC emulator
  that **verifies request signatures**: entry with attached TP/SL, plan-order discovery, three
  trailing ratchets (one `change_plan_price` each, no move-back), restart persistence,
  exchange-side stop fill → `TRAIL` trade record, manual close, adoption of a pre-existing
  exchange position. The emulator is a test fixture only; the product has no simulated mode.
* `tests/preview_with_emulator.py` – runs the real stack against the emulator so the dashboard
  can be inspected on machines without exchange access.

## Project layout

```
crypto_hunter/
  config.py              all parameters (pydantic) + YAML/DB layering
  security.py            Fernet credential store, log redaction
  exchange/   rest.py    signed REST client, pooling, retry, rate limiter
              ws_public.py / ws_private.py   market & account streams
  strategy/   indicators.py divergence.py filters.py scanner.py
  risk/       roi.py (ROI ↔ price, trailing ladder, ratchet)   sizing.py (compounding, projection)
  engine/     bot.py (orchestrator) executor.py (orders) position_manager.py (trailing/persist)
              market_data.py (kline + ticker cache)
  persistence/db.py      SQLite schema & queries
  api/        server.py  FastAPI + /ws stream      static/  dashboard
config.yaml · .env.example · Dockerfile · docker-compose.yml · deploy/ · docs/ARCHITECTURE.md
```

Architecture, signal logic, risk engine and persistence are described in detail in
[`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md).

## License

MPL-2.0 – see [LICENSE](LICENSE). Trading involves substantial risk; the authors accept no
liability for financial losses.
