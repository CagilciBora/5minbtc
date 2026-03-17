"""REST calls to Polymarket CLOB API."""

import asyncio
import re
from datetime import datetime, timezone
from typing import Optional

import aiohttp

from polymarket_validator import config
from polymarket_validator.utils.logger import get_logger

log = get_logger("polymarket")


async def _fetch_with_retry(session: aiohttp.ClientSession, url: str) -> Optional[dict | list]:
    """GET a URL with exponential backoff retries."""
    for attempt in range(config.MAX_RETRIES + 1):
        try:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status == 200:
                    return await resp.json()
                log.warning("Polymarket API %s returned %d", url, resp.status)
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            log.warning("Polymarket API error (attempt %d): %s", attempt + 1, e)
        if attempt < config.MAX_RETRIES:
            await asyncio.sleep(config.RETRY_BACKOFF_BASE ** (attempt + 1))
    return None


def _parse_end_date(iso_str: str) -> Optional[datetime]:
    """Parse ISO date string to timezone-aware datetime."""
    if not iso_str:
        return None
    try:
        # Handle various ISO formats
        iso_str = iso_str.replace("Z", "+00:00")
        return datetime.fromisoformat(iso_str)
    except (ValueError, TypeError):
        return None


def _matches_btc_5min(question: str) -> bool:
    """Check if a market question matches BTC 5-minute pattern."""
    q = question.lower()
    has_btc = any(kw in q for kw in config.MARKET_QUESTION_KEYWORDS)
    has_time = bool(re.search(config.MARKET_QUESTION_TIME_PATTERN, q, re.IGNORECASE))
    return has_btc and has_time


def _extract_market_fields(market: dict) -> Optional[dict]:
    """Extract relevant fields from a market object."""
    question = market.get("question", "")
    if not _matches_btc_5min(question):
        return None

    end_date_str = market.get("end_date_iso", "")
    end_date = _parse_end_date(end_date_str)
    if end_date is None:
        return None

    now = datetime.now(timezone.utc)
    seconds_until_expiry = (end_date - now).total_seconds()

    if seconds_until_expiry < 0 or seconds_until_expiry > config.MARKET_LOOKAHEAD_SECONDS:
        return None

    # Extract best bid/ask from tokens if available
    tokens = market.get("tokens", [])
    best_bid = 0.0
    best_ask = 1.0
    if tokens:
        # Take first token (typically the "Yes" outcome)
        token = tokens[0]
        best_bid = float(token.get("price", 0.5))
        best_ask = float(token.get("price", 0.5))

    # Fall back to top-level fields if present
    if "best_bid" in market:
        best_bid = float(market["best_bid"])
    if "best_ask" in market:
        best_ask = float(market["best_ask"])

    mid = (best_bid + best_ask) / 2.0

    return {
        "condition_id": market.get("condition_id", ""),
        "question": question,
        "end_date_iso": end_date_str,
        "end_date": end_date,
        "seconds_until_expiry": seconds_until_expiry,
        "best_bid": best_bid,
        "best_ask": best_ask,
        "mid": mid,
        "resolved_price": market.get("resolved_price"),
    }


async def fetch_active_btc_markets(session: aiohttp.ClientSession) -> list[dict]:
    """Fetch and filter active Polymarket BTC 5-minute markets."""
    data = await _fetch_with_retry(session, config.POLYMARKET_MARKETS_URL)
    if data is None:
        return []

    markets_raw = data if isinstance(data, list) else data.get("data", data.get("markets", []))
    if not isinstance(markets_raw, list):
        log.warning("Unexpected markets response type: %s", type(markets_raw))
        return []

    results = []
    for m in markets_raw:
        parsed = _extract_market_fields(m)
        if parsed:
            results.append(parsed)

    log.debug("Found %d BTC 5-min markets within lookahead window", len(results))
    return results


async def fetch_market(session: aiohttp.ClientSession, condition_id: str) -> Optional[dict]:
    """Fetch a single market by condition_id."""
    url = f"{config.POLYMARKET_MARKETS_URL}/{condition_id}"
    data = await _fetch_with_retry(session, url)
    if data is None:
        return None
    return _extract_market_fields(data) or data


async def get_resolved_price(session: aiohttp.ClientSession, condition_id: str) -> Optional[float]:
    """Check if a market has resolved and return the resolved price."""
    url = f"{config.POLYMARKET_MARKETS_URL}/{condition_id}"
    data = await _fetch_with_retry(session, url)
    if data is None:
        return None
    rp = data.get("resolved_price")
    if rp is not None:
        try:
            return float(rp)
        except (ValueError, TypeError):
            pass
    return None
