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
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    market_id TEXT NOT NULL,
    question TEXT,
    predicted_at TEXT,
    time_remaining_s INTEGER,
    p_hat REAL,
    p_market REAL,
    label TEXT,
    btc_price REAL,
    features_json TEXT,
    outcome REAL,
    resolved_at TEXT
);
"""

_INSERT_PREDICTION = """
INSERT INTO predictions
    (market_id, question, predicted_at, time_remaining_s, p_hat, p_market,
     label, btc_price, features_json)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);
"""

_UPDATE_OUTCOME = """
UPDATE predictions
SET outcome = ?, resolved_at = ?
WHERE market_id = ? AND outcome IS NULL;
"""

_SELECT_UNRESOLVED = """
SELECT id, market_id, label, p_hat, p_market, predicted_at
FROM predictions
WHERE outcome IS NULL AND predicted_at < ?;
"""

_SELECT_RESOLVED = """
SELECT market_id, label, p_hat, p_market, outcome
FROM predictions
WHERE outcome IS NOT NULL;
"""

_MARKET_EXISTS = """
SELECT COUNT(*) FROM predictions WHERE market_id = ?;
"""


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
            ),
        )
        await self._conn.commit()

    async def update_outcome(self, market_id: str, outcome: float):
        resolved_at = datetime.now(timezone.utc).isoformat()
        await self._conn.execute(_UPDATE_OUTCOME, (outcome, resolved_at, market_id))
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
            }
            for r in rows
        ]
