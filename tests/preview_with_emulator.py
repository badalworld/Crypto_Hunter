"""
DEV HARNESS ONLY – runs the full Crypto Hunter stack against the in-process, signature-
checking MEXC emulator from ``tests/fake_mexc.py`` and walks the emulated mark price so
the dashboard can be inspected where the real exchange is unreachable (CI, sandboxes).

This is *not* a paper-trading mode of the product: the bot code path is byte-for-byte the
live path; only the HTTP endpoint it talks to is swapped via ``execution.rest_base_url``.

    python tests/preview_with_emulator.py            # dashboard on :8080
"""
from __future__ import annotations

import asyncio
import logging
import math
import os
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
sys.path.insert(0, str(Path(__file__).parent.parent))

import uvicorn

from crypto_hunter.api.server import create_app
from crypto_hunter.config import AppSettings, BotConfig
from crypto_hunter.logging_setup import setup_logging
from crypto_hunter.persistence.db import Database
from fake_mexc import API_KEY, API_SECRET, start_fake


async def main() -> None:
    settings = AppSettings()
    settings.data_dir = Path(os.environ.get("CH_DATA_DIR", "data/emulator"))
    settings.db_path = settings.data_dir / "emulator.sqlite"
    setup_logging("INFO", None)
    fake, runner, url = await start_fake()
    cfg = BotConfig.load("config.yaml").merged({
        "execution": {"rest_base_url": url, "legacy_rest_base_url": url, "ws_url": "ws://127.0.0.1:1/edge",
                      "position_sync_interval_sec": 1},
        "scanner": {"min_quote_volume_24h": 0, "rescan_interval_sec": 30},
        "exits": {"software_failsafe": False},
    })
    db = await Database(settings.db_path).open()
    app = create_app(settings, cfg, db)
    bot = app.state.bot

    async def driver() -> None:
        await asyncio.sleep(2)
        await bot.set_credentials(API_KEY, API_SECRET)
        await bot.start()
        await asyncio.sleep(3)
        logging.info("EMULATOR: forcing a long entry so the UI has a live position")
        await bot.try_enter("TEST_USDT", "long", atr_v=0.5, signal={"emulator": True})
        t0 = time.time()
        while True:
            await asyncio.sleep(1)
            # slow upward drift with noise so peak ROI / trailing ladder visibly progress
            e = time.time() - t0
            px = 100 * (1 + 0.0035 * e / 10 + 0.0008 * math.sin(e / 3))
            fake.set_price(px)
            await bot.pm.on_price("TEST_USDT", px, px)
            if bot.pm.count() == 0 and e > 30:
                await asyncio.sleep(5)
                t0 = time.time()
                fake.set_price(100)
                await bot.try_enter("TEST_USDT", "short" if int(t0) % 2 else "long", atr_v=0.5, signal={"emulator": True})

    asyncio.create_task(driver())
    server = uvicorn.Server(uvicorn.Config(app, host="0.0.0.0", port=int(os.environ.get("CH_PORT", "8080")),
                                           log_level="warning", access_log=False))
    try:
        await server.serve()
    finally:
        await runner.cleanup()


if __name__ == "__main__":
    asyncio.run(main())
