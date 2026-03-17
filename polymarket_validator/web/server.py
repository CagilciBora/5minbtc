"""aiohttp web server + WebSocket push for the live dashboard."""

import asyncio
import json
import os
import time
from pathlib import Path

from aiohttp import web

from polymarket_validator import config
from polymarket_validator.feeds.binance_ws import BinanceFeed
from polymarket_validator.storage.db import Database
from polymarket_validator.utils.logger import get_logger

log = get_logger("web")

# Shared state that tasks can publish into
_state = {
    "price_history": [],       # list of {t, price} dicts (last 300s)
    "current_price": 0.0,
    "predictions": [],         # recent predictions from watcher
    "stats": None,             # latest stats dict
    "feed_ready": False,
    "feed_warmup": 0,
    "active_markets": [],      # currently tracked markets
}
_state_lock = asyncio.Lock()
_ws_clients: set[web.WebSocketResponse] = set()


async def publish_state_update():
    """Push current state to all connected WebSocket clients."""
    async with _state_lock:
        payload = json.dumps(_state, default=str)
    dead = set()
    for ws in _ws_clients:
        try:
            await ws.send_str(payload)
        except (ConnectionError, ConnectionResetError, RuntimeError):
            dead.add(ws)
    _ws_clients.difference_update(dead)


async def update_price(price: float, timestamp: float):
    """Called by binance feed bridge to push a price point."""
    async with _state_lock:
        _state["current_price"] = price
        _state["price_history"].append({"t": timestamp, "p": price})
        # Keep last 300 seconds
        cutoff = time.time() - config.BINANCE_BUFFER_SECONDS
        while _state["price_history"] and _state["price_history"][0]["t"] < cutoff:
            _state["price_history"].pop(0)


async def update_feed_status(ready: bool, warmup: int):
    """Called by watcher bridge to update feed status."""
    async with _state_lock:
        _state["feed_ready"] = ready
        _state["feed_warmup"] = warmup


async def update_prediction(prediction: dict):
    """Called when a new prediction is made."""
    async with _state_lock:
        _state["predictions"].insert(0, prediction)
        # Keep last 100
        _state["predictions"] = _state["predictions"][:100]


async def update_resolution(market_id: str, outcome: float, correct: bool | None):
    """Called when a market resolves."""
    async with _state_lock:
        for pred in _state["predictions"]:
            if pred.get("market_id") == market_id:
                pred["outcome"] = outcome
                pred["correct"] = correct
                pred["resolved"] = True
                break


async def update_stats(stats: dict):
    """Called by stats task to push latest stats."""
    async with _state_lock:
        _state["stats"] = stats


async def update_active_markets(markets: list):
    """Called by watcher to show currently tracked markets."""
    async with _state_lock:
        _state["active_markets"] = markets


# --- WebSocket push loop ---

async def run_ws_push(feed: BinanceFeed, db: Database):
    """Push state to all WebSocket clients every second."""
    while True:
        try:
            # Update price from feed
            snap = await feed.snapshot()
            if snap["last_price"] > 0:
                await update_price(snap["last_price"], time.time())
            await update_feed_status(feed.is_ready(), feed.warmup_elapsed())

            # Refresh predictions from DB
            resolved = await db.get_resolved()
            if resolved:
                await update_stats({
                    "total": len(resolved),
                    "directional": len([r for r in resolved if r["label"] in ("UP", "DOWN")]),
                    "skipped": len([r for r in resolved if r["label"] == "SKIP"]),
                    "resolved_list": resolved,
                })

            if _ws_clients:
                await publish_state_update()
        except asyncio.CancelledError:
            return
        except Exception:
            log.exception("WS push error")
        await asyncio.sleep(1)


# --- HTTP handlers ---

async def handle_index(request: web.Request) -> web.Response:
    html_path = Path(__file__).parent / "dashboard.html"
    return web.FileResponse(html_path)


async def handle_ws(request: web.Request) -> web.WebSocketResponse:
    ws = web.WebSocketResponse()
    await ws.prepare(request)
    _ws_clients.add(ws)
    log.info("WebSocket client connected (%d total)", len(_ws_clients))
    try:
        async for msg in ws:
            pass  # We only push, don't read
    finally:
        _ws_clients.discard(ws)
        log.info("WebSocket client disconnected (%d remaining)", len(_ws_clients))
    return ws


async def handle_api_predictions(request: web.Request) -> web.Response:
    """REST endpoint to get all predictions from DB."""
    db: Database = request.app["db"]
    resolved = await db.get_resolved()
    return web.json_response(resolved)


def create_app(feed: BinanceFeed, db: Database) -> web.Application:
    app = web.Application()
    app["feed"] = feed
    app["db"] = db
    app.router.add_get("/", handle_index)
    app.router.add_get("/ws", handle_ws)
    app.router.add_get("/api/predictions", handle_api_predictions)
    return app


async def run_server(feed: BinanceFeed, db: Database, host: str = "0.0.0.0", port: int = 8080):
    """Start the web server and WS push loop."""
    app = create_app(feed, db)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, host, port)
    await site.start()
    log.info("Dashboard running at http://%s:%d", host, port)

    # Run WS push loop
    try:
        await run_ws_push(feed, db)
    finally:
        await runner.cleanup()
