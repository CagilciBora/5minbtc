"""SQLite handler: schema, insert prediction, update outcome, queries."""

import json
from datetime import datetime, timezone
from typing import Optional

import aiosqlite

from polymarket_validator import config
from polymarket_validator.utils.logger import get_logger

log = get_logger("db")

_CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS predictions (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    market_id        TEXT NOT NULL,
    question         TEXT,
    predicted_at     TEXT,
    time_remaining_s INTEGER,
    p_hat            REAL,
    p_market         REAL,
    label            TEXT,
    btc_price        REAL,
    features_json    TEXT,
    bet_size         REAL DEFAULT 0,
    kelly_fraction   REAL DEFAULT 0,
    outcome          REAL,
    pnl              REAL,
    resolved_at      TEXT
);
"""

_INSERT_PREDICTION = """
INSERT INTO predictions
    (market_id, question, predicted_at, time_remaining_s, p_hat, p_market,
     label, btc_price, features_json, bet_size, kelly_fraction)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
"""

_UPDATE_OUTCOME = """
UPDATE predictions
SET outcome = ?, pnl = ?, resolved_at = ?
WHERE market_id = ? AND outcome IS NULL;
"""

_SELECT_UNRESOLVED = """
SELECT id, market_id, label, p_hat, p_market, predicted_at, features_json, bet_size
FROM predictions
WHERE outcome IS NULL AND predicted_at < ?;
"""

_SELECT_RESOLVED = """
SELECT market_id, label, p_hat, p_market, outcome, bet_size, pnl
FROM predictions
WHERE outcome IS NOT NULL;
"""

_SELECT_BANKROLL = """
SELECT COALESCE(SUM(pnl), 0.0) FROM predictions WHERE pnl IS NOT NULL;
"""

_MARKET_EXISTS = """
SELECT COUNT(*) FROM predictions WHERE market_id = ?;
"""

# Columns added in v2 — we migrate gracefully
_NEW_COLUMNS = [
    ("bet_size",       "REAL DEFAULT 0"),
    ("kelly_fraction", "REAL DEFAULT 0"),
    ("pnl",            "REAL"),
]


class Database:
    """Manages a single shared aiosqlite connection."""

    def __init__(self, db_path: str = config.DB_PATH):
        self._db_path = db_path
        self._conn: Optional[aiosqlite.Connection] = None

    async def connect(self):
        self._conn = await aiosqlite.connect(self._db_path)
        await self._conn.execute("PRAGMA journal_mode=WAL;")
        await self._conn.execute(_CREATE_TABLE)
        await self._conn.commit()

        # Migrate: add new columns to existing DBs that may not have them
        for col, typedef in _NEW_COLUMNS:
            try:
                await self._conn.execute(
                    f"ALTER TABLE predictions ADD COLUMN {col} {typedef};"
                )
                await self._conn.commit()
                log.info("Migrated DB: added column %s", col)
            except aiosqlite.OperationalError:
                pass  # column already exists

        log.info("Database initialized at %s", self._db_path)

    async def close(self):
        if self._conn:
            await self._conn.commit()
            await self._conn.close()
            log.info("Database connection closed")

    async def market_exists(self, market_id: str) -> bool:
        async with self._conn.execute(_MARKET_EXISTS, (market_id,)) as cursor:
            row = await cursor.fetchone()
            return row[0] > 0

    async def insert_prediction(
        self,
        market_id: str,
        question: str,
        time_remaining_s: int,
        p_hat: float,
        p_market: float,
        label: str,
        btc_price: float,
        features: dict,
        bet_size: float = 0.0,
        kelly_fraction: float = 0.0,
    ):
        predicted_at = datetime.now(timezone.utc).isoformat()
        features_json = json.dumps(features)
        await self._conn.execute(
            _INSERT_PREDICTION,
            (
                market_id,
                question,
                predicted_at,
                int(time_remaining_s),
                p_hat,
                p_market,
                label,
                btc_price,
                features_json,
                bet_size,
                kelly_fraction,
            ),
        )
        await self._conn.commit()

    async def update_outcome(self, market_id: str, outcome: float, pnl: float = 0.0):
        resolved_at = datetime.now(timezone.utc).isoformat()
        await self._conn.execute(
            _UPDATE_OUTCOME, (outcome, pnl, resolved_at, market_id)
        )
        await self._conn.commit()

    async def get_unresolved(self, older_than_iso: str) -> list[dict]:
        async with self._conn.execute(_SELECT_UNRESOLVED, (older_than_iso,)) as cursor:
            rows = await cursor.fetchall()
        return [
            {
                "id": r[0],
                "market_id": r[1],
                "label": r[2],
                "p_hat": r[3],
                "p_market": r[4],
                "predicted_at": r[5],
                "features": json.loads(r[6]) if r[6] else {},
                "bet_size": r[7] or 0.0,
            }
            for r in rows
        ]

    async def get_resolved(self) -> list[dict]:
        async with self._conn.execute(_SELECT_RESOLVED) as cursor:
            rows = await cursor.fetchall()
        return [
            {
                "market_id": r[0],
                "label": r[1],
                "p_hat": r[2],
                "p_market": r[3],
                "outcome": r[4],
                "bet_size": r[5] or 0.0,
                "pnl": r[6],
            }
            for r in rows
        ]

    async def get_bankroll(self) -> float:
        """Current bankroll = initial + sum of all resolved P&Ls."""
        async with self._conn.execute(_SELECT_BANKROLL) as cursor:
            row = await cursor.fetchone()
        total_pnl = row[0] if row else 0.0
        return config.INITIAL_BANKROLL + total_pnl
