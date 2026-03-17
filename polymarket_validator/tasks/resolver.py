"""Async loop: polls expired unresolved markets every 2 minutes."""

import asyncio
from datetime import datetime, timedelta, timezone

import aiohttp

from polymarket_validator import config
from polymarket_validator.feeds.polymarket_api import get_resolved_price
from polymarket_validator.storage.db import Database
from polymarket_validator.utils.logger import get_logger

log = get_logger("resolver")


async def run_resolver(db: Database, session: aiohttp.ClientSession):
    """Poll for resolved markets and update outcomes in the database."""
    log.info("Resolver started (poll interval=%ds)", config.RESOLVER_POLL_INTERVAL)

    while True:
        try:
            cutoff = datetime.now(timezone.utc) - timedelta(
                seconds=config.RESOLVER_MIN_AGE_SECONDS
            )
            unresolved = await db.get_unresolved(cutoff.isoformat())

            for pred in unresolved:
                resolved_price = await get_resolved_price(session, pred["market_id"])
                if resolved_price is None:
                    continue

                await db.update_outcome(pred["market_id"], resolved_price)

                label = pred["label"]
                p_hat = pred["p_hat"]
                # Determine if prediction was correct
                if label == "UP":
                    correct = resolved_price == 1.0
                elif label == "DOWN":
                    correct = resolved_price == 0.0
                else:
                    correct = None  # SKIP

                mark = "\u2713" if correct else ("\u2717" if correct is not None else "-")
                log.info(
                    "[RESOLVED] %s | label=%s | outcome=%s | p_hat=%.2f",
                    pred["market_id"][:8],
                    label,
                    mark,
                    p_hat,
                )

        except asyncio.CancelledError:
            log.info("Resolver cancelled")
            return
        except Exception:
            log.exception("Resolver error")

        await asyncio.sleep(config.RESOLVER_POLL_INTERVAL)
