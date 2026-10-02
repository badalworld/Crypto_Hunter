from __future__ import annotations

import logging
import logging.handlers
import os
from typing import Optional

from .security import REDACTOR


def setup_logging(level: str = "INFO", log_file: Optional[str] = None) -> None:
    fmt = logging.Formatter("%(asctime)s | %(levelname)-7s | %(name)s | %(message)s")
    root = logging.getLogger()
    root.setLevel(level)
    for h in list(root.handlers):
        root.removeHandler(h)

    console = logging.StreamHandler()
    console.setFormatter(fmt)
    console.addFilter(REDACTOR)
    root.addHandler(console)

    if log_file:
        os.makedirs(os.path.dirname(log_file) or ".", exist_ok=True)
        fh = logging.handlers.RotatingFileHandler(log_file, maxBytes=20_000_000, backupCount=5, encoding="utf-8")
        fh.setFormatter(fmt)
        fh.addFilter(REDACTOR)
        root.addHandler(fh)

    # Quiet noisy libraries
    for noisy in ("aiohttp.access", "websockets.client", "uvicorn.access"):
        logging.getLogger(noisy).setLevel(logging.WARNING)
