"""Builds the 15-feature vector from live feed snapshots."""

import numpy as np

from polymarket_validator import config


def build_features(
    binance_snapshot: dict,
    market: dict,
    contract_open_price: float | None = None,
) -> tuple[dict, np.ndarray]:
    """Build feature vector from Binance snapshot and Polymarket market state.

    Args:
        binance_snapshot: dict from BinanceFeed.snapshot()
        market: parsed market dict from polymarket_api
        contract_open_price: BTC price when the contract opened (if known)

    Returns:
        (feature_dict, feature_array) where feature_array is in the canonical order
        defined by config.FEATURE_NAMES.
    """
    btc_price = binance_snapshot["last_price"]
    poly_mid = market.get("mid", 0.5)
    poly_spread = market.get("best_ask", 1.0) - market.get("best_bid", 0.0)
    time_remaining_s = market.get("seconds_until_expiry", 60)

    # If contract open price is unknown, approximate with 5m-ago price
    if contract_open_price is None or contract_open_price == 0:
        ret_5m = binance_snapshot["return_5m"]
        if ret_5m != 0 and btc_price != 0:
            contract_open_price = btc_price / (1 + ret_5m)
        else:
            contract_open_price = btc_price

    distance_to_open = (
        (btc_price - contract_open_price) / contract_open_price
        if contract_open_price != 0
        else 0.0
    )
    p_deviation = poly_mid - 0.5
    is_above_open = 1.0 if btc_price > contract_open_price else 0.0

    features = {
        "return_30s": binance_snapshot["return_30s"],
        "return_1m": binance_snapshot["return_1m"],
        "return_3m": binance_snapshot["return_3m"],
        "return_5m": binance_snapshot["return_5m"],
        "rsi_14": binance_snapshot["rsi_14"],
        "atr_5m": binance_snapshot["atr_5m"],
        "trade_flow_imbalance": binance_snapshot["trade_flow_imbalance"],
        "candle_body_ratio": binance_snapshot["candle_body_ratio"],
        "poly_mid": poly_mid,
        "poly_spread": poly_spread,
        "binance_price": btc_price,
        "time_remaining_s": time_remaining_s,
        "distance_to_open": distance_to_open,
        "p_deviation": p_deviation,
        "is_above_open": is_above_open,
    }

    feature_array = np.array(
        [features[name] for name in config.FEATURE_NAMES], dtype=np.float64
    )

    return features, feature_array
