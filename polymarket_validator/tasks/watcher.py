"""Async loop: detects markets that just opened and makes predictions at T+0."""

import asyncio
from datetime import datetime, timezone

import aiohttp

from polymarket_validator import config
from polymarket_validator.feeds.binance_ws import BinanceFeed
from polymarket_validator.feeds.polymarket_api import fetch_active_btc_markets
from polymarket_validator.features.builder import build_features
from polymarket_validator.model.predictor import Predictor
from polymarket_validator.storage.db import Database
from polymarket_validator.utils.kelly import half_kelly_fraction
from polymarket_validator.utils.logger import get_logger
from polymarket_validator.web import server as web_state

log = get_logger("watcher")


async def run_watcher(
    feed: BinanceFeed,
    db: Database,
    predictor: Predictor,
    session: aiohttp.ClientSession,
):
    """Poll Polymarket every WATCHER_POLL_INTERVAL seconds.

    Triggers a prediction at market OPEN (seconds_until_expiry ≈ 265–295),
    i.e. 5–35 seconds after the 5-minute window starts.  This gives the model
    the full 5 minutes of Binance context while betting at the start of the
    window (best odds, least market information priced in).
    """
    log.info("Watcher started (poll interval=%ds)", config.WATCHER_POLL_INTERVAL)

    while True:
        try:
            if not feed.is_ready():
                elapsed = feed.warmup_elapsed()
                log.info("Feed warming up... (%d/%ds)", elapsed, config.BINANCE_BUFFER_SECONDS)
                await asyncio.sleep(config.WATCHER_POLL_INTERVAL)
                continue

            markets = await fetch_active_btc_markets(session)
            log.info("Scan: %d BTC 5-min markets in window", len(markets))
            await web_state.update_active_markets(
                [
                    {"id": m["condition_id"][:10], "q": m["question"], "sec": m["seconds_until_expiry"]}
                    for m in markets
                ]
            )

            for market in markets:
                sec = market["seconds_until_expiry"]
                mid = market["condition_id"]

                # Only act at market OPEN (first 5–35 seconds of the 5-min window)
                if not (config.PREDICTION_WINDOW_LOW < sec < config.PREDICTION_WINDOW_HIGH):
                    continue

                if await db.market_exists(mid):
                    continue

                # --- Build features ---
                binance_snap = await feed.snapshot()
                features_dict, features_array = build_features(binance_snap, market)

                # --- Run model ---
                p_hat, label = predictor.predict(features_dict, features_array)
                p_market = market["mid"]

                # --- Half-Kelly bet sizing ---
                bankroll = await db.get_bankroll()
                kelly_frac = half_kelly_fraction(label, p_hat, p_market)
                bet_size = round(bankroll * kelly_frac, 2)

                # Confidence = probability of the predicted direction
                confidence = p_hat if label == "UP" else (1.0 - p_hat) if label == "DOWN" else 0.5
                edge = p_hat - p_market

                # --- Store prediction ---
                await db.insert_prediction(
                    market_id=mid,
                    question=market["question"],
                    time_remaining_s=int(sec),
                    p_hat=p_hat,
                    p_market=p_market,
                    label=label,
                    btc_price=binance_snap["last_price"],
                    features=features_dict,
                    bet_size=bet_size,
                    kelly_fraction=kelly_frac,
                )

                log.info(
                    "[PREDICTION] %s | %s | conf=%.1f%% | p_hat=%.3f | p_mkt=%.3f"
                    " | edge=%+.3f | bet=$%.2f (%.1f%%)",
                    mid[:8],
                    label,
                    confidence * 100,
                    p_hat,
                    p_market,
                    edge,
                    bet_size,
                    kelly_frac * 100,
                )

                # --- Push to web dashboard ---
                await web_state.update_prediction({
                    "market_id": mid,
                    "question": market["question"],
                    "predicted_at": datetime.now(timezone.utc).isoformat(),
                    "time_remaining_s": int(sec),
                    "p_hat": p_hat,
                    "p_market": p_market,
                    "label": label,
                    "confidence": confidence,
                    "btc_price": binance_snap["last_price"],
                    "features": features_dict,
                    "bet_size": bet_size,
                    "kelly_fraction": kelly_frac,
                    "bankroll": bankroll,
                    "outcome": None,
                    "pnl": None,
                    "correct": None,
                    "resolved": False,
                })

        except asyncio.CancelledError:
            log.info("Watcher cancelled")
            return
        except Exception:
            log.exception("Watcher error")

        await asyncio.sleep(config.WATCHER_POLL_INTERVAL)
