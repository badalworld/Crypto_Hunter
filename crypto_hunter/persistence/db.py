"""
SQLite persistence (aiosqlite, WAL mode).

Tables
------
credentials       encrypted MEXC API key/secret
settings          dashboard overrides of config.yaml (JSON)
positions         *managed* open positions incl. trailing state (peak ROI, stop ROI, order ids)
trades            closed trade history
equity_snapshots  equity curve
cooldowns         per-symbol no-trade-until timestamps
events            structured audit log shown on the dashboard
kv                misc (e.g. equity at session start / day start)
"""
from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

import aiosqlite

SCHEMA = """
PRAGMA journal_mode=WAL;
PRAGMA synchronous=NORMAL;
CREATE TABLE IF NOT EXISTS credentials (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    api_key_enc TEXT NOT NULL,
    api_secret_enc TEXT NOT NULL,
    updated_at REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS settings (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    data TEXT NOT NULL,
    updated_at REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS positions (
    symbol TEXT NOT NULL,
    side TEXT NOT NULL,                 -- long | short
    position_id INTEGER,
    entry_price REAL NOT NULL,
    vol REAL NOT NULL,
    contract_size REAL NOT NULL,
    leverage INTEGER NOT NULL,
    margin REAL NOT NULL,
    atr REAL NOT NULL,
    initial_stop_price REAL NOT NULL,
    stop_price REAL NOT NULL,
    stop_roi REAL,                      -- NULL until trailing activates
    tp_price REAL NOT NULL,
    peak_roi REAL NOT NULL DEFAULT 0,
    peak_price REAL NOT NULL,
    stop_plan_order_id TEXT,            -- MEXC TP/SL plan order id (change_plan_price)
    entry_order_id TEXT,
    sl_plan_order_id TEXT,              -- fallback trigger-order ids when position TP/SL is unavailable
    tp_plan_order_id TEXT,
    opened_at REAL NOT NULL,
    updated_at REAL NOT NULL,
    signal_json TEXT,
    status TEXT NOT NULL DEFAULT 'open', -- pending | open | closing
    PRIMARY KEY (symbol, side)
);
CREATE TABLE IF NOT EXISTS trades (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol TEXT NOT NULL,
    side TEXT NOT NULL,
    entry_price REAL NOT NULL,
    exit_price REAL,
    vol REAL NOT NULL,
    contract_size REAL NOT NULL,
    leverage INTEGER NOT NULL,
    margin REAL NOT NULL,
    pnl REAL,
    roi REAL,
    peak_roi REAL,
    reason TEXT,                        -- TP | SL | TRAIL | FAILSAFE | MANUAL | EXTERNAL
    opened_at REAL NOT NULL,
    closed_at REAL NOT NULL,
    signal_json TEXT
);
CREATE INDEX IF NOT EXISTS idx_trades_closed ON trades(closed_at);
CREATE TABLE IF NOT EXISTS equity_snapshots (
    ts REAL PRIMARY KEY,
    equity REAL NOT NULL,
    balance REAL NOT NULL,
    unrealized REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS cooldowns (
    symbol TEXT PRIMARY KEY,
    until_ts REAL NOT NULL,
    reason TEXT
);
CREATE TABLE IF NOT EXISTS events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts REAL NOT NULL,
    level TEXT NOT NULL,
    kind TEXT NOT NULL,
    symbol TEXT,
    message TEXT NOT NULL,
    data TEXT
);
CREATE INDEX IF NOT EXISTS idx_events_ts ON events(ts);
CREATE TABLE IF NOT EXISTS kv (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
"""


def _row(r: Optional[aiosqlite.Row]) -> Optional[Dict[str, Any]]:
    return dict(r) if r is not None else None


class Database:
    def __init__(self, path: Path | str):
        self.path = Path(path)
        self._db: Optional[aiosqlite.Connection] = None

    async def open(self) -> "Database":
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._db = await aiosqlite.connect(self.path, isolation_level=None)
        self._db.row_factory = aiosqlite.Row
        await self._db.executescript(SCHEMA)
        return self

    async def close(self) -> None:
        if self._db:
            await self._db.close()
            self._db = None

    @property
    def db(self) -> aiosqlite.Connection:
        assert self._db is not None, "Database not opened"
        return self._db

    # ------------------------------------------------------------ credentials
    async def save_credentials(self, api_key_enc: str, api_secret_enc: str) -> None:
        await self.db.execute(
            "INSERT INTO credentials(id, api_key_enc, api_secret_enc, updated_at) VALUES (1,?,?,?) "
            "ON CONFLICT(id) DO UPDATE SET api_key_enc=excluded.api_key_enc, "
            "api_secret_enc=excluded.api_secret_enc, updated_at=excluded.updated_at",
            (api_key_enc, api_secret_enc, time.time()),
        )

    async def load_credentials(self) -> Optional[Dict[str, Any]]:
        async with self.db.execute("SELECT * FROM credentials WHERE id=1") as cur:
            return _row(await cur.fetchone())

    async def delete_credentials(self) -> None:
        await self.db.execute("DELETE FROM credentials WHERE id=1")

    # --------------------------------------------------------------- settings
    async def save_settings(self, data: Dict[str, Any]) -> None:
        await self.db.execute(
            "INSERT INTO settings(id, data, updated_at) VALUES (1,?,?) "
            "ON CONFLICT(id) DO UPDATE SET data=excluded.data, updated_at=excluded.updated_at",
            (json.dumps(data), time.time()),
        )

    async def load_settings(self) -> Dict[str, Any]:
        async with self.db.execute("SELECT data FROM settings WHERE id=1") as cur:
            r = await cur.fetchone()
            return json.loads(r["data"]) if r else {}

    # -------------------------------------------------------------- positions
    async def upsert_position(self, p: Dict[str, Any]) -> None:
        p = dict(p)
        p["updated_at"] = time.time()
        cols = ",".join(p.keys())
        qs = ",".join("?" for _ in p)
        upd = ",".join(f"{k}=excluded.{k}" for k in p if k not in ("symbol", "side"))
        await self.db.execute(
            f"INSERT INTO positions({cols}) VALUES ({qs}) ON CONFLICT(symbol, side) DO UPDATE SET {upd}",
            tuple(p.values()),
        )

    async def update_position(self, symbol: str, side: str, **fields: Any) -> None:
        if not fields:
            return
        fields["updated_at"] = time.time()
        sets = ",".join(f"{k}=?" for k in fields)
        await self.db.execute(
            f"UPDATE positions SET {sets} WHERE symbol=? AND side=?", (*fields.values(), symbol, side)
        )

    async def delete_position(self, symbol: str, side: str) -> None:
        await self.db.execute("DELETE FROM positions WHERE symbol=? AND side=?", (symbol, side))

    async def list_positions(self) -> List[Dict[str, Any]]:
        async with self.db.execute("SELECT * FROM positions ORDER BY opened_at") as cur:
            return [dict(r) for r in await cur.fetchall()]

    # ----------------------------------------------------------------- trades
    async def insert_trade(self, t: Dict[str, Any]) -> int:
        cols = ",".join(t.keys())
        qs = ",".join("?" for _ in t)
        cur = await self.db.execute(f"INSERT INTO trades({cols}) VALUES ({qs})", tuple(t.values()))
        return cur.lastrowid or 0

    async def list_trades(self, limit: int = 200, since: Optional[float] = None) -> List[Dict[str, Any]]:
        if since is not None:
            q = "SELECT * FROM trades WHERE closed_at>=? ORDER BY closed_at DESC LIMIT ?"
            args: tuple = (since, limit)
        else:
            q = "SELECT * FROM trades ORDER BY closed_at DESC LIMIT ?"
            args = (limit,)
        async with self.db.execute(q, args) as cur:
            return [dict(r) for r in await cur.fetchall()]

    async def all_trades(self) -> List[Dict[str, Any]]:
        async with self.db.execute("SELECT * FROM trades ORDER BY closed_at ASC") as cur:
            return [dict(r) for r in await cur.fetchall()]

    # ----------------------------------------------------------------- equity
    async def insert_equity(self, equity: float, balance: float, unrealized: float, ts: Optional[float] = None) -> None:
        await self.db.execute(
            "INSERT OR REPLACE INTO equity_snapshots(ts, equity, balance, unrealized) VALUES (?,?,?,?)",
            (ts or time.time(), equity, balance, unrealized),
        )

    async def equity_curve(self, since: Optional[float] = None, limit: int = 5000) -> List[Dict[str, Any]]:
        if since is None:
            since = 0
        async with self.db.execute(
            "SELECT * FROM (SELECT * FROM equity_snapshots WHERE ts>=? ORDER BY ts DESC LIMIT ?) ORDER BY ts ASC",
            (since, limit),
        ) as cur:
            return [dict(r) for r in await cur.fetchall()]

    # -------------------------------------------------------------- cooldowns
    async def set_cooldown(self, symbol: str, until_ts: float, reason: str) -> None:
        await self.db.execute(
            "INSERT INTO cooldowns(symbol, until_ts, reason) VALUES (?,?,?) "
            "ON CONFLICT(symbol) DO UPDATE SET until_ts=MAX(cooldowns.until_ts, excluded.until_ts), reason=excluded.reason",
            (symbol, until_ts, reason),
        )

    async def cooldowns(self) -> Dict[str, float]:
        now = time.time()
        await self.db.execute("DELETE FROM cooldowns WHERE until_ts<?", (now,))
        async with self.db.execute("SELECT symbol, until_ts FROM cooldowns") as cur:
            return {r["symbol"]: r["until_ts"] for r in await cur.fetchall()}

    # ----------------------------------------------------------------- events
    async def log_event(self, level: str, kind: str, message: str, symbol: Optional[str] = None,
                        data: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        ts = time.time()
        cur = await self.db.execute(
            "INSERT INTO events(ts, level, kind, symbol, message, data) VALUES (?,?,?,?,?,?)",
            (ts, level, kind, symbol, message, json.dumps(data) if data else None),
        )
        await self.db.execute("DELETE FROM events WHERE id < (SELECT MAX(id) FROM events) - 5000")
        return {"id": cur.lastrowid, "ts": ts, "level": level, "kind": kind, "symbol": symbol,
                "message": message, "data": data}

    async def recent_events(self, limit: int = 100) -> List[Dict[str, Any]]:
        async with self.db.execute("SELECT * FROM events ORDER BY id DESC LIMIT ?", (limit,)) as cur:
            rows = [dict(r) for r in await cur.fetchall()]
        for r in rows:
            r["data"] = json.loads(r["data"]) if r.get("data") else None
        return rows

    # --------------------------------------------------------------------- kv
    async def kv_get(self, key: str, default: Any = None) -> Any:
        async with self.db.execute("SELECT value FROM kv WHERE key=?", (key,)) as cur:
            r = await cur.fetchone()
            return json.loads(r["value"]) if r else default

    async def kv_set(self, key: str, value: Any) -> None:
        await self.db.execute(
            "INSERT INTO kv(key, value) VALUES (?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (key, json.dumps(value)),
        )
