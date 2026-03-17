"""Entry point: runs all async tasks for the Polymarket BTC validator."""

import asyncio
import signal

import aiohttp

from polymarket_validator.feeds.binance_ws import BinanceFeed
from polymarket_validator.model.predictor import Predictor
from polymarket_validator.storage.db import Database
from polymarket_validator.tasks.resolver import run_resolver
from polymarket_validator.tasks.stats import run_stats
from polymarket_validator.tasks.watcher import run_watcher
from polymarket_validator.utils.logger import get_logger, setup_logging

log = get_logger("main")


async def main():
    setup_logging("INFO")
    log.info("Starting Polymarket BTC 5-min validator (paper trading)")

    # Initialize components
    feed = BinanceFeed()
    db = Database()
    predictor = Predictor()

    await db.connect()

    session = aiohttp.ClientSession()

    # Create tasks
    tasks = []
    feed_task = asyncio.create_task(feed.run(), name="binance_feed")
    tasks.append(feed_task)

    watcher_task = asyncio.create_task(
        run_watcher(feed, db, predictor, session), name="watcher"
    )
    tasks.append(watcher_task)

    resolver_task = asyncio.create_task(
        run_resolver(db, session), name="resolver"
    )
    tasks.append(resolver_task)

    stats_task = asyncio.create_task(run_stats(db), name="stats")
    tasks.append(stats_task)

    # Graceful shutdown
    shutdown_event = asyncio.Event()

    def _signal_handler():
        log.info("Shutdown signal received")
        shutdown_event.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _signal_handler)
        except NotImplementedError:
            # Windows doesn't support add_signal_handler
            pass

    try:
        await shutdown_event.wait()
    except KeyboardInterrupt:
        log.info("KeyboardInterrupt received")

    # Cleanup
    log.info("Shutting down...")
    for t in tasks:
        t.cancel()

    await asyncio.gather(*tasks, return_exceptions=True)
    await feed.stop()
    await session.close()

    # Print final stats
    resolved = await db.get_resolved()
    if resolved:
        from polymarket_validator.tasks.stats import _compute_stats

        log.info("Final stats:%s", _compute_stats(resolved))

    await db.close()
    log.info("Shutdown complete")


def run():
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    run()
