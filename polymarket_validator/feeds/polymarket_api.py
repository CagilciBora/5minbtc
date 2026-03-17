"""REST calls to Polymarket CLOB API and Gamma API."""

import asyncio
import re
from datetime import datetime, timezone
from typing import Optional

import aiohttp

from polymarket_validator import config
from polymarket_validator.utils.logger import get_logger

log = get_logger("polymarket")


async def _fetch_with_retry(
    session: aiohttp.ClientSession, url: str, params: dict | None = None
) -> Optional[dict | list]:
    """GET a URL with exponential backoff retries."""
    for attempt in range(config.MAX_RETRIES + 1):
        try:
            async with session.get(
                url,
                params=params,
                timeout=aiohttp.ClientTimeout(total=15),
            ) as resp:
                if resp.status == 200:
                    return await resp.json()
                # Log the response body for debugging non-200
                try:
                    body = await resp.text()
                except Exception:
                    body = "(could not read body)"
                log.warning(
                    "Polymarket API error (attempt %d): %d, url=%s, body=%s",
                    attempt + 1, resp.status, url, body[:200],
                )
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
    """Extract relevant fields from a market object.

    Handles both CLOB API and Gamma API response shapes.
    """
    question = market.get("question", "")
    if not _matches_btc_5min(question):
        return None

    # Gamma API uses "end_date_iso", CLOB uses "end_date_iso" or "expiration"
    end_date_str = (
        market.get("end_date_iso")
        or market.get("expiration")
        or market.get("end_date")
        or ""
    )
    end_date = _parse_end_date(end_date_str)
    if end_date is None:
        return None

    now = datetime.now(timezone.utc)
    seconds_until_expiry = (end_date - now).total_seconds()

    if seconds_until_expiry < 0 or seconds_until_expiry > config.MARKET_LOOKAHEAD_SECONDS:
        return None

    # --- Extract best bid/ask ---
    # Gamma API shape: market has "tokens" array, each with "price"
    # CLOB API shape: market has "tokens" array with "token_id" and orderbook data
    best_bid = 0.5
    best_ask = 0.5

    tokens = market.get("tokens", [])
    if tokens:
        # First token is typically "Yes" / UP outcome
        token = tokens[0]
        price = token.get("price")
        if price is not None:
            try:
                p = float(price)
                best_bid = p
                best_ask = p
            except (ValueError, TypeError):
                pass

    # Top-level overrides (CLOB API format)
    if "best_bid" in market:
        try:
            best_bid = float(market["best_bid"])
        except (ValueError, TypeError):
            pass
    if "best_ask" in market:
        try:
            best_ask = float(market["best_ask"])
        except (ValueError, TypeError):
            pass

    mid = (best_bid + best_ask) / 2.0

    # resolved_price may be string or float
    resolved_price = market.get("resolved_price")
    if resolved_price is not None:
        try:
            resolved_price = float(resolved_price)
        except (ValueError, TypeError):
            resolved_price = None

    return {
        "condition_id": market.get("condition_id", market.get("id", "")),
        "question": question,
        "end_date_iso": end_date_str,
        "end_date": end_date,
        "seconds_until_expiry": seconds_until_expiry,
        "best_bid": best_bid,
        "best_ask": best_ask,
        "mid": mid,
        "resolved_price": resolved_price,
    }


async def fetch_active_btc_markets(session: aiohttp.ClientSession) -> list[dict]:
    """Fetch and filter active Polymarket BTC 5-minute markets.

    Uses the Gamma API for market discovery (public, no auth, supports text search).
    Falls back to the CLOB API if Gamma fails.
    """
    results = []

    # --- Try Gamma API first (more reliable for market discovery) ---
    gamma_data = await _fetch_with_retry(
        session,
        config.GAMMA_MARKETS_URL,
        params={
            "closed": "false",
            "limit": "100",
        },
    )
    if gamma_data is not None:
        markets_raw = gamma_data if isinstance(gamma_data, list) else gamma_data.get("data", [])
        if isinstance(markets_raw, list):
            for m in markets_raw:
                parsed = _extract_market_fields(m)
                if parsed:
                    results.append(parsed)
            log.debug("Gamma API: found %d BTC 5-min markets", len(results))
            if results:
                return results

    # --- Fallback: CLOB API ---
    clob_data = await _fetch_with_retry(session, config.CLOB_MARKETS_URL)
    if clob_data is not None:
        markets_raw = clob_data if isinstance(clob_data, list) else clob_data.get("data", clob_data.get("markets", []))
        if isinstance(markets_raw, list):
            for m in markets_raw:
                parsed = _extract_market_fields(m)
                if parsed:
                    results.append(parsed)
            log.debug("CLOB API: found %d BTC 5-min markets", len(results))

    return results


async def fetch_market(session: aiohttp.ClientSession, condition_id: str) -> Optional[dict]:
    """Fetch a single market by condition_id."""
    # Try Gamma first
    data = await _fetch_with_retry(
        session,
        f"{config.GAMMA_MARKETS_URL}/{condition_id}",
    )
    if data is None:
        data = await _fetch_with_retry(
            session,
            f"{config.CLOB_MARKETS_URL}/{condition_id}",
        )
    if data is None:
        return None
    return _extract_market_fields(data) or data


async def get_resolved_price(session: aiohttp.ClientSession, condition_id: str) -> Optional[float]:
    """Check if a market has resolved and return the resolved price."""
    # Try Gamma first
    data = await _fetch_with_retry(
        session,
        f"{config.GAMMA_MARKETS_URL}/{condition_id}",
    )
    if data is None:
        data = await _fetch_with_retry(
            session,
            f"{config.CLOB_MARKETS_URL}/{condition_id}",
        )
    if data is None:
        return None

    rp = data.get("resolved_price")
    if rp is not None:
        try:
            return float(rp)
        except (ValueError, TypeError):
            pass
    return None
