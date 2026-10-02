"""Entrypoint: ``python -m crypto_hunter``."""
from __future__ import annotations

import asyncio
import logging

import uvicorn

from .api.server import create_app
from .config import AppSettings, BotConfig
from .logging_setup import setup_logging
from .persistence.db import Database

log = logging.getLogger("ch.main")


def main() -> None:
    settings = AppSettings()
    setup_logging(settings.log_level, settings.log_file)
    cfg = BotConfig.load(settings.config_path)
    db = Database(settings.db_path)

    async def _serve() -> None:
        await db.open()  # aiosqlite binds to the running loop – open inside it
        app = create_app(settings, cfg, db)
        config = uvicorn.Config(app, host=settings.host, port=settings.port, log_level=settings.log_level.lower(),
                                access_log=False, ws_ping_interval=20, ws_ping_timeout=20)
        server = uvicorn.Server(config)
        log.info("Crypto Hunter dashboard on http://%s:%d  (config: %s, db: %s)", settings.host, settings.port,
                 settings.config_path, settings.db_path)
        await server.serve()

    try:
        asyncio.run(_serve())
    except KeyboardInterrupt:  # pragma: no cover
        pass


if __name__ == "__main__":
    main()
