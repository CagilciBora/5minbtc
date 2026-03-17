"""Binance WebSocket BTC/USDT tick feed with rolling OHLCV state."""

import asyncio
import json
import time
from collections import deque
from typing import Optional

import numpy as np
import websockets

from polymarket_validator import config
from polymarket_validator.utils.logger import get_logger

log = get_logger("binance")


class Trade:
    __slots__ = ("price", "qty", "timestamp", "is_buyer_maker")

    def __init__(self, price: float, qty: float, timestamp: float, is_buyer_maker: bool):
        self.price = price
        self.qty = qty
        self.timestamp = timestamp
        self.is_buyer_maker = is_buyer_maker


class BinanceFeed:
    """Connects to Binance aggTrade stream and maintains rolling trade buffer."""

    def __init__(self):
        self._trades: deque[Trade] = deque()
        self._lock = asyncio.Lock()
        self._start_time: Optional[float] = None
        self._running = False
        self._ws = None

    def is_ready(self) -> bool:
        """True once we have at least BINANCE_BUFFER_SECONDS of data."""
        if self._start_time is None:
            return False
        return (time.time() - self._start_time) >= config.BINANCE_BUFFER_SECONDS

    def warmup_elapsed(self) -> int:
        """Seconds of data collected so far."""
        if self._start_time is None:
            return 0
        return int(time.time() - self._start_time)

    async def run(self):
        """Main loop: connect to WebSocket and consume trades."""
        self._running = True
        retry = 0
        while self._running:
            try:
                async with websockets.connect(config.BINANCE_WS_URL) as ws:
                    self._ws = ws
                    log.info("Connected to Binance aggTrade stream")
                    retry = 0
                    if self._start_time is None:
                        self._start_time = time.time()
                    async for raw in ws:
                        if not self._running:
                            break
                        await self._handle_message(raw)
            except (websockets.ConnectionClosed, OSError, asyncio.CancelledError) as e:
                if not self._running:
                    break
                retry += 1
                if retry > config.MAX_RETRIES:
                    retry = config.MAX_RETRIES
                wait = config.RETRY_BACKOFF_BASE ** retry
                log.warning("Binance WS disconnected (%s), reconnecting in %ss", e, wait)
                await asyncio.sleep(wait)

    async def stop(self):
        """Gracefully close the WebSocket."""
        self._running = False
        if self._ws:
            await self._ws.close()

    async def _handle_message(self, raw: str):
        data = json.loads(raw)
        trade = Trade(
            price=float(data["p"]),
            qty=float(data["q"]),
            timestamp=float(data["T"]) / 1000.0,
            is_buyer_maker=data["m"],
        )
        async with self._lock:
            self._trades.append(trade)
            cutoff = time.time() - config.BINANCE_BUFFER_SECONDS
            while self._trades and self._trades[0].timestamp < cutoff:
                self._trades.popleft()

    async def snapshot(self) -> dict:
        """Return a snapshot of computed features from the current trade buffer.

        Returns a dict with keys:
            last_price, return_30s, return_1m, return_3m, return_5m,
            rsi_14, atr_5m, trade_flow_imbalance, candle_body_ratio
        """
        async with self._lock:
            trades = list(self._trades)

        if not trades:
            return self._empty_snapshot()

        now = trades[-1].timestamp
        last_price = trades[-1].price

        # --- Returns ---
        return_30s = self._compute_return(trades, now, 30, last_price)
        return_1m = self._compute_return(trades, now, 60, last_price)
        return_3m = self._compute_return(trades, now, 180, last_price)
        return_5m = self._compute_return(trades, now, 300, last_price)

        # --- 1-minute candles for RSI ---
        candles = self._build_1m_candles(trades, now, num_candles=15)
        rsi_14 = self._compute_rsi(candles, 14)

        # --- ATR proxy: (high - low) / mean over last 5m ---
        five_min_trades = [t for t in trades if t.timestamp >= now - 300]
        if five_min_trades:
            prices_5m = [t.price for t in five_min_trades]
            hi, lo, mn = max(prices_5m), min(prices_5m), np.mean(prices_5m)
            atr_5m = (hi - lo) / mn if mn else 0.0
        else:
            atr_5m = 0.0

        # --- Trade flow imbalance over last 60s ---
        one_min_trades = [t for t in trades if t.timestamp >= now - 60]
        buy_vol = sum(t.qty for t in one_min_trades if not t.is_buyer_maker)
        sell_vol = sum(t.qty for t in one_min_trades if t.is_buyer_maker)
        total_vol = buy_vol + sell_vol
        trade_flow_imbalance = (buy_vol - sell_vol) / total_vol if total_vol else 0.0

        # --- Candle body ratio for last 1m ---
        last_1m_trades = [t for t in trades if t.timestamp >= now - 60]
        if last_1m_trades:
            o = last_1m_trades[0].price
            c = last_1m_trades[-1].price
            h = max(t.price for t in last_1m_trades)
            lo_1m = min(t.price for t in last_1m_trades)
            rng = h - lo_1m
            candle_body_ratio = abs(c - o) / rng if rng else 0.0
        else:
            candle_body_ratio = 0.0

        return {
            "last_price": last_price,
            "return_30s": return_30s,
            "return_1m": return_1m,
            "return_3m": return_3m,
            "return_5m": return_5m,
            "rsi_14": rsi_14,
            "atr_5m": atr_5m,
            "trade_flow_imbalance": trade_flow_imbalance,
            "candle_body_ratio": candle_body_ratio,
        }

    @staticmethod
    def _compute_return(trades: list, now: float, seconds: int, last_price: float) -> float:
        target_time = now - seconds
        past_price = None
        for t in trades:
            if t.timestamp >= target_time:
                past_price = t.price
                break
        if past_price is None or past_price == 0:
            return 0.0
        return (last_price - past_price) / past_price

    @staticmethod
    def _build_1m_candles(trades: list, now: float, num_candles: int = 15) -> list:
        """Build 1-minute candles ending at `now`. Returns list of close prices."""
        candles = []
        for i in range(num_candles, 0, -1):
            start = now - i * 60
            end = now - (i - 1) * 60
            bucket = [t.price for t in trades if start <= t.timestamp < end]
            if bucket:
                candles.append(bucket[-1])  # close price
        return candles

    @staticmethod
    def _compute_rsi(closes: list, period: int = 14) -> float:
        if len(closes) < period + 1:
            return 50.0  # neutral default
        deltas = [closes[i] - closes[i - 1] for i in range(1, len(closes))]
        recent = deltas[-(period):]
        gains = [d for d in recent if d > 0]
        losses = [-d for d in recent if d < 0]
        avg_gain = sum(gains) / period if gains else 0.0
        avg_loss = sum(losses) / period if losses else 0.0
        if avg_loss == 0:
            return 100.0
        rs = avg_gain / avg_loss
        return 100.0 - (100.0 / (1.0 + rs))

    @staticmethod
    def _empty_snapshot() -> dict:
        return {
            "last_price": 0.0,
            "return_30s": 0.0,
            "return_1m": 0.0,
            "return_3m": 0.0,
            "return_5m": 0.0,
            "rsi_14": 50.0,
            "atr_5m": 0.0,
            "trade_flow_imbalance": 0.0,
            "candle_body_ratio": 0.0,
        }
