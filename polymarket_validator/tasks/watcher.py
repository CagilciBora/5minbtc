"""Async loop: finds markets expiring soon, triggers prediction at T-60s."""

import asyncio
from datetime import datetime, timezone

import aiohttp

from polymarket_validator import config
from polymarket_validator.feeds.binance_ws import BinanceFeed
from polymarket_validator.feeds.polymarket_api import fetch_active_btc_markets
from polymarket_validator.features.builder import build_features
from polymarket_validator.model.predictor import Predictor
from polymarket_validator.storage.db import Database
from polymarket_validator.utils.logger import get_logger
from polymarket_validator.web import server as web_state

log = get_logger("watcher")


async def run_watcher(
    feed: BinanceFeed,
    db: Database,
    predictor: Predictor,
    session: aiohttp.ClientSession,
):
    """Poll Polymarket every WATCHER_POLL_INTERVAL seconds for expiring markets."""
    log.info("Watcher started (poll interval=%ds)", config.WATCHER_POLL_INTERVAL)

    while True:
        try:
            if not feed.is_ready():
                elapsed = feed.warmup_elapsed()
                log.info(
                    "Feed warming up... (%d/%ds)",
                    elapsed,
                    config.BINANCE_BUFFER_SECONDS,
                )
                await asyncio.sleep(config.WATCHER_POLL_INTERVAL)
                continue

            markets = await fetch_active_btc_markets(session)
            log.info("Scan: %d BTC 5-min markets in window", len(markets))
            await web_state.update_active_markets(
                [{"id": m["condition_id"][:10], "q": m["question"], "sec": m["seconds_until_expiry"]} for m in markets]
            )

            for market in markets:
                sec = market["seconds_until_expiry"]
                mid = market["condition_id"]

                if not (config.PREDICTION_WINDOW_LOW < sec < config.PREDICTION_WINDOW_HIGH):
                    continue

                if await db.market_exists(mid):
                    continue

                # Build features
                binance_snap = await feed.snapshot()
                features_dict, features_array = build_features(binance_snap, market)

                # Run model
                p_hat, label = predictor.predict(features_dict, features_array)
                p_market = market["mid"]

                # Store prediction
                await db.insert_prediction(
                    market_id=mid,
                    question=market["question"],
                    time_remaining_s=int(sec),
                    p_hat=p_hat,
                    p_market=p_market,
                    label=label,
                    btc_price=binance_snap["last_price"],
                    features=features_dict,
                )

                edge = p_hat - p_market
                log.info(
                    "[PREDICTION] %s | label=%s | p_hat=%.2f | p_market=%.2f | edge=%+.2f",
                    mid[:8],
                    label,
                    p_hat,
                    p_market,
                    edge,
                )
                log.debug("Features: %s", features_dict)

                # Push to web dashboard
                await web_state.update_prediction({
                    "market_id": mid,
                    "question": market["question"],
                    "predicted_at": datetime.now(timezone.utc).isoformat(),
                    "time_remaining_s": int(sec),
                    "p_hat": p_hat,
                    "p_market": p_market,
                    "label": label,
                    "btc_price": binance_snap["last_price"],
                    "features": features_dict,
                    "outcome": None,
                    "correct": None,
                    "resolved": False,
                })

        except asyncio.CancelledError:
            log.info("Watcher cancelled")
            return
        except Exception:
            log.exception("Watcher error")

        await asyncio.sleep(config.WATCHER_POLL_INTERVAL)
