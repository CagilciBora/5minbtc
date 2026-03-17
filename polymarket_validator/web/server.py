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
    "price_history": [],       # list of {t, price}
    "current_price": 0.0,
    "predictions": [],         # recent predictions (newest first)
    "stats": None,
    "feed_ready": False,
    "feed_warmup": 0,
    "active_markets": [],
    "bankroll": config.INITIAL_BANKROLL,
    "total_pnl": 0.0,
}
_state_lock = asyncio.Lock()
_ws_clients: set[web.WebSocketResponse] = set()


async def publish_state_update():
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
    async with _state_lock:
        _state["current_price"] = price
        _state["price_history"].append({"t": timestamp, "p": price})
        cutoff = time.time() - config.BINANCE_BUFFER_SECONDS
        while _state["price_history"] and _state["price_history"][0]["t"] < cutoff:
            _state["price_history"].pop(0)


async def update_feed_status(ready: bool, warmup: int):
    async with _state_lock:
        _state["feed_ready"] = ready
        _state["feed_warmup"] = warmup


async def update_prediction(prediction: dict):
    async with _state_lock:
        _state["predictions"].insert(0, prediction)
        _state["predictions"] = _state["predictions"][:100]
        # Update bankroll from latest prediction context
        if prediction.get("bankroll") is not None:
            _state["bankroll"] = prediction["bankroll"]


async def update_resolution(
    market_id: str,
    outcome: float,
    correct: bool | None,
    pnl: float,
    bankroll: float,
):
    async with _state_lock:
        for pred in _state["predictions"]:
            if pred.get("market_id") == market_id:
                pred["outcome"] = outcome
                pred["correct"] = correct
                pred["pnl"] = pnl
                pred["resolved"] = True
                break
        _state["bankroll"] = bankroll
        _state["total_pnl"] = bankroll - config.INITIAL_BANKROLL


async def update_stats(stats: dict):
    async with _state_lock:
        _state["stats"] = stats


async def update_active_markets(markets: list):
    async with _state_lock:
        _state["active_markets"] = markets


# --- WebSocket push loop ---

async def run_ws_push(feed: BinanceFeed, db: Database):
    while True:
        try:
            snap = await feed.snapshot()
            if snap["last_price"] > 0:
                await update_price(snap["last_price"], time.time())
            await update_feed_status(feed.is_ready(), feed.warmup_elapsed())

            # Sync bankroll from DB
            bankroll = await db.get_bankroll()
            async with _state_lock:
                _state["bankroll"] = bankroll
                _state["total_pnl"] = bankroll - config.INITIAL_BANKROLL

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
            pass
    finally:
        _ws_clients.discard(ws)
        log.info("WebSocket client disconnected (%d remaining)", len(_ws_clients))
    return ws


async def handle_api_predictions(request: web.Request) -> web.Response:
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
    app = create_app(feed, db)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, host, port)
    await site.start()
    log.info("Dashboard running at http://%s:%d", host, port)
    try:
        await run_ws_push(feed, db)
    finally:
        await runner.cleanup()
