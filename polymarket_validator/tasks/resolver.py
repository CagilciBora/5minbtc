"""Async loop: polls expired unresolved markets, computes P&L, updates model."""

import asyncio
from datetime import datetime, timedelta, timezone

import aiohttp

from polymarket_validator import config
from polymarket_validator.feeds.polymarket_api import get_resolved_price
from polymarket_validator.model.predictor import Predictor
from polymarket_validator.storage.db import Database
from polymarket_validator.utils.kelly import compute_pnl
from polymarket_validator.utils.logger import get_logger
from polymarket_validator.web import server as web_state

log = get_logger("resolver")


async def run_resolver(db: Database, session: aiohttp.ClientSession, predictor: Predictor):
    """Poll for resolved markets, compute P&L, and do online model updates."""
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

                label = pred["label"]
                p_hat = pred["p_hat"]
                p_market = pred["p_market"]
                bet_size = pred["bet_size"]
                features = pred.get("features", {})

                # Correctness
                if label == "UP":
                    correct = resolved_price == 1.0
                elif label == "DOWN":
                    correct = resolved_price == 0.0
                else:
                    correct = None  # SKIP

                # P&L simulation
                pnl = compute_pnl(label, resolved_price, bet_size, p_market)

                # Persist outcome + P&L
                await db.update_outcome(pred["market_id"], resolved_price, pnl)

                # Online model update (skip SKIP predictions)
                if label in ("UP", "DOWN") and features:
                    try:
                        predictor.update(features, resolved_price)
                    except Exception:
                        log.exception("Model update failed")

                # Updated bankroll
                bankroll = await db.get_bankroll()

                mark = "\u2713" if correct else ("\u2717" if correct is not None else "-")
                pnl_str = f"${pnl:+.2f}" if pnl else "$0.00"
                log.info(
                    "[RESOLVED] %s | %s %s | p_hat=%.3f | bet=$%.2f | pnl=%s | bank=$%.2f",
                    pred["market_id"][:8],
                    label,
                    mark,
                    p_hat,
                    bet_size,
                    pnl_str,
                    bankroll,
                )

                # Push to web dashboard
                await web_state.update_resolution(
                    pred["market_id"], resolved_price, correct, pnl, bankroll
                )

        except asyncio.CancelledError:
            log.info("Resolver cancelled")
            return
        except Exception:
            log.exception("Resolver error")

        await asyncio.sleep(config.RESOLVER_POLL_INTERVAL)
