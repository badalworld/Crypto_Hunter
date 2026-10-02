"""End-to-end engine test against the in-process MEXC emulator (signature-checked)."""
import asyncio
from pathlib import Path

import pytest

from crypto_hunter.config import AppSettings, BotConfig
from crypto_hunter.engine.bot import Bot
from crypto_hunter.persistence.db import Database
from crypto_hunter.security import Credentials

from fake_mexc import API_KEY, API_SECRET, start_fake


@pytest.fixture
async def env(tmp_path: Path):
    fake, runner, url = await start_fake()
    settings = AppSettings()
    settings.data_dir = tmp_path
    settings.db_path = tmp_path / "t.sqlite"
    settings.master_key = None
    cfg = BotConfig().merged({
        "execution": {"rest_base_url": url, "legacy_rest_base_url": url, "ws_url": "ws://127.0.0.1:1/edge",
                      "position_sync_interval_sec": 1, "max_retries": 0},
        "scanner": {"min_quote_volume_24h": 0, "rescan_interval_sec": 30},
        "exits": {"software_failsafe": False},
    })
    db = await Database(settings.db_path).open()
    broadcasts = []

    async def bc(msg):
        broadcasts.append(msg)

    bot = Bot(settings, cfg, db, bc)
    bot.broadcasts = broadcasts
    await bot.boot()
    await bot.set_credentials(API_KEY, API_SECRET)
    yield bot, fake, db
    await bot.shutdown()
    await db.close()
    await runner.cleanup()


@pytest.mark.asyncio
async def test_full_trade_lifecycle(env):
    bot, fake, db = env
    await bot.start()
    assert bot.state == "RUNNING" and bot.asset.equity == pytest.approx(1000.0)
    # wait for the scanner to populate the watch-list
    for _ in range(50):
        if bot.watchlist:
            break
        await asyncio.sleep(0.1)
    assert [w.symbol for w in bot.watchlist] == ["TEST_USDT"]

    # --- entry
    await bot.try_enter("TEST_USDT", "long", atr_v=0.5, signal={"test": True})
    assert bot.pm.count() == 1
    p = bot.pm.positions[("TEST_USDT", "long")]
    # 8% of 1000 = 80 margin * 10x = 800 notional / (0.01 * ~100) = 800 contracts
    assert p.vol == pytest.approx(800, rel=0.01)
    assert p.margin == pytest.approx(80, rel=0.01)
    assert p.stop_price == pytest.approx(p.entry_price - 1.5, abs=0.02)          # 3 x ATR
    assert p.tp_price == pytest.approx(p.entry_price * 1.2, rel=1e-4)             # +200% ROI at 10x
    assert fake.leverage_calls and fake.leverage_calls[0]["leverage"] == 10
    create = [c for c in fake.calls if c.endswith("order/create")]
    assert len(create) == 1
    await asyncio.sleep(1.2)  # _arm_protection
    assert p.stop_plan_order_id is not None, "exchange TP/SL plan order should be discovered"
    rows = await db.list_positions()
    assert rows and rows[0]["stop_plan_order_id"] == p.stop_plan_order_id
    # live fee / unrealised figures are copied from the exchange on sync
    await asyncio.sleep(1.2)
    assert p.fee_paid == pytest.approx(p.vol * 0.01 * p.entry_price * 0.0006, rel=1e-6)
    d = p.to_dict()
    assert d["pnl_source"] == "exchange" and d["net_pnl"] < d["unrealized_pnl"]

    # --- price rises: peak 35% ROI -> stop locks +20%
    entry = p.entry_price
    px = entry * (1 + 0.35 / 10)
    fake.set_price(px)
    await bot.pm.on_price("TEST_USDT", px, px)
    await asyncio.sleep(0.3)
    assert p.peak_roi == pytest.approx(35, abs=0.01)
    assert p.stop_roi == 20
    assert p.stop_price == pytest.approx(entry * 1.02, rel=1e-4)
    assert len(fake.plan_changes) >= 1 and fake.plan_changes[-1]["stopLossPrice"] == pytest.approx(entry * 1.02, abs=0.011)

    # --- same step again: no new exchange request
    n = len(fake.plan_changes)
    px2 = entry * (1 + 0.38 / 10)
    fake.set_price(px2)
    await bot.pm.on_price("TEST_USDT", px2, px2)
    await asyncio.sleep(0.2)
    assert len(fake.plan_changes) == n and p.stop_roi == 20

    # --- peak 100% -> stop 90%
    px3 = entry * (1 + 1.0 / 10)
    fake.set_price(px3)
    await bot.pm.on_price("TEST_USDT", px3, px3)
    await asyncio.sleep(0.3)
    assert p.stop_roi == 90 and p.stop_price == pytest.approx(entry * 1.09, rel=1e-4)

    # --- pull-back: stop must NOT move down
    px4 = entry * (1 + 0.95 / 10)
    fake.set_price(px4)
    await bot.pm.on_price("TEST_USDT", px4, px4)
    await asyncio.sleep(0.2)
    assert p.stop_roi == 90

    # --- restart persistence: a fresh manager restores peak/stop state
    from crypto_hunter.engine.position_manager import PositionManager
    pm2 = PositionManager(db, bot.executor, bot.cfg, bot.emit)
    await pm2.load()
    r = pm2.positions[("TEST_USDT", "long")]
    assert r.peak_roi == pytest.approx(100, abs=0.01) and r.stop_roi == 90 and r.stop_plan_order_id == p.stop_plan_order_id

    # --- exchange stop fires (price drops through the 90% lock) -> sync loop detects the close
    px5 = entry * (1 + 0.85 / 10)
    fake.set_price(px5)  # fake exchange triggers SL plan at 1.09*entry
    for _ in range(40):
        if bot.pm.count() == 0:
            break
        await asyncio.sleep(0.1)
    assert bot.pm.count() == 0
    trades = await db.list_trades()
    assert len(trades) == 1
    t = trades[0]
    assert t["reason"] == "TRAIL" and t["pnl"] > 0
    assert t["exit_price"] == pytest.approx(entry * 1.09, rel=1e-4)
    # PnL comes from MEXC's ledger: net = gross - fees + funding, exactly as the platform credits it
    assert t["pnl_source"] == "exchange"
    qty = t["vol"] * 0.01
    assert t["gross_pnl"] == pytest.approx((t["exit_price"] - entry) * qty, rel=1e-6)
    assert t["fee"] == pytest.approx(qty * entry * 0.0006 + qty * t["exit_price"] * 0.0006, rel=1e-6)
    assert t["funding"] == pytest.approx(fake.FUNDING)
    assert t["pnl"] == pytest.approx(t["gross_pnl"] - t["fee"] + t["funding"], abs=1e-9)
    assert t["pnl"] == pytest.approx(fake.history[0]["realised"], abs=1e-9)
    assert t["exchange_roi"] == pytest.approx(fake.history[0]["profitRatio"] * 100, rel=1e-6)
    assert t["roi"] < 90  # net of fees/funding, below the price-only 90 % lock
    cds = await db.cooldowns()
    assert "TEST_USDT" in cds  # post-exit cooldown recorded
    m = await bot.metrics()
    assert m["trades"] == 1 and m["win_rate"] == 100 and m["exit_reasons"] == {"TRAIL": 1}
    assert any(b.get("type") == "trade" for b in bot.broadcasts)


@pytest.mark.asyncio
async def test_entry_gates_and_manual_close(env):
    bot, fake, db = env
    await bot.start()
    await bot.update_config({"risk": {"max_open_positions": 1}})
    await bot.try_enter("TEST_USDT", "short", atr_v=0.5, signal={})
    assert bot.pm.count() == 1
    # second entry blocked by max positions / same symbol
    await bot.try_enter("TEST_USDT", "short", atr_v=0.5, signal={})
    assert bot.pm.count() == 1
    assert len([c for c in fake.calls if c.endswith("order/create")]) == 1
    # manual close -> reduce-side market order, trade recorded as MANUAL
    assert await bot.close_one("TEST_USDT", "short")
    assert bot.pm.count() == 0
    t = (await db.list_trades())[0]
    assert t["reason"] == "MANUAL" and t["side"] == "short"
    assert len([c for c in fake.calls if c.endswith("order/create")]) == 2


@pytest.mark.asyncio
async def test_adopts_external_position(env):
    bot, fake, db = env
    # open a position "manually" on the exchange before the bot starts
    fake._create_order({"symbol": "TEST_USDT", "vol": 50, "side": 1, "type": 5, "openType": 1, "leverage": 5})
    await bot.start()
    assert bot.pm.count() == 1
    p = bot.pm.positions[("TEST_USDT", "long")]
    assert p.vol == 50 and p.leverage == 5
    await asyncio.sleep(1.2)
    # the manual position had no TP/SL -> fallback reduce-only trigger orders were placed
    assert p.sl_plan_order_id is not None and p.tp_plan_order_id is not None
    sl = fake.plan_orders[int(p.sl_plan_order_id)]
    assert sl["side"] == 4 and sl["triggerType"] == 2 and sl["triggerPrice"] == pytest.approx(p.stop_price)


@pytest.mark.asyncio
async def test_bad_signature_is_rejected(env):
    bot, fake, db = env
    bot.rest.set_credentials(Credentials(API_KEY, "wrong-secret-0000000"))
    from crypto_hunter.exchange.models import MexcAPIError
    with pytest.raises(MexcAPIError) as ei:
        await bot.rest.get_assets()
    assert "signature" in ei.value.message
