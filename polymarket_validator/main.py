"""Entry point: runs all async tasks for the Polymarket BTC validator."""

import asyncio
import os
import signal
import sys

import aiohttp

from polymarket_validator.feeds.binance_ws import BinanceFeed
from polymarket_validator.model.predictor import Predictor
from polymarket_validator.storage.db import Database
from polymarket_validator.tasks.resolver import run_resolver
from polymarket_validator.tasks.stats import run_stats
from polymarket_validator.tasks.watcher import run_watcher
from polymarket_validator.web.server import run_server
from polymarket_validator.utils.logger import get_logger, setup_logging

log = get_logger("main")

WEB_PORT = int(os.environ.get("VALIDATOR_WEB_PORT", "8080"))


async def main():
    setup_logging("INFO")
    log.info("Starting Polymarket BTC 5-min validator (paper trading)")

    # Initialize components
    feed = BinanceFeed()
    db = Database()
    predictor = Predictor()

    await db.connect()

    # Disable Brotli by explicitly setting Accept-Encoding to gzip/deflate only
    session = aiohttp.ClientSession(
        headers={"Accept-Encoding": "gzip, deflate"}
    )

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

    # Web dashboard
    web_task = asyncio.create_task(
        run_server(feed, db, port=WEB_PORT), name="web_server"
    )
    tasks.append(web_task)

    # --- Graceful shutdown ---
    shutdown_event = asyncio.Event()
    _shutdown_called = False

    def _signal_handler():
        nonlocal _shutdown_called
        if _shutdown_called:
            # Second Ctrl-C: force exit immediately
            log.info("Force exit (second signal)")
            os._exit(1)
        _shutdown_called = True
        log.info("Shutdown signal received (press Ctrl-C again to force)")
        shutdown_event.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _signal_handler)
        except NotImplementedError:
            pass

    try:
        await shutdown_event.wait()
    except KeyboardInterrupt:
        pass

    # Cleanup: stop feed first so WS doesn't reconnect
    log.info("Shutting down...")
    await feed.stop()

    # Cancel all tasks
    for t in tasks:
        t.cancel()

    # Wait with a timeout so we don't hang forever
    done, pending = await asyncio.wait(tasks, timeout=5)
    for t in pending:
        log.warning("Task %s did not finish in time, forcing cancel", t.get_name())
        t.cancel()

    await session.close()

    # Print final stats
    try:
        resolved = await db.get_resolved()
        if resolved:
            from polymarket_validator.tasks.stats import _compute_stats
            log.info("Final stats:%s", _compute_stats(resolved))
    except Exception:
        pass

    await db.close()
    log.info("Shutdown complete")


def run():
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
    except SystemExit:
        pass


if __name__ == "__main__":
    run()
