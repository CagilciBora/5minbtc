"""Configuration constants for the Polymarket BTC validator."""

import os

# --- Binance ---
BINANCE_WS_URL = "wss://stream.binance.com:9443/ws/btcusdt@aggTrade"
BINANCE_BUFFER_SECONDS = 300  # need 5 min of data for all features

# --- Polymarket APIs ---
CLOB_BASE_URL = "https://clob.polymarket.com"
CLOB_MARKETS_URL = f"{CLOB_BASE_URL}/markets"

GAMMA_BASE_URL = "https://gamma-api.polymarket.com"
GAMMA_MARKETS_URL = f"{GAMMA_BASE_URL}/markets"

# --- Market filtering ---
MARKET_LOOKAHEAD_SECONDS = 600
MARKET_QUESTION_KEYWORDS = ["bitcoin", "btc"]
MARKET_QUESTION_TIME_PATTERN = r"5.?min"

# --- Prediction trigger: at market OPEN (5-35s after window starts) ---
# A 5-min market has 300s until expiry when it opens.
# We predict when seconds_until_expiry is 265-295 (= opened 5-35s ago).
PREDICTION_WINDOW_LOW = 265
PREDICTION_WINDOW_HIGH = 295

# --- Resolver ---
RESOLVER_MIN_AGE_SECONDS = 360   # wait 6 min: 5 min for market to close + 1 min buffer
RESOLVER_POLL_INTERVAL = 60      # check every minute

# --- Stats ---
STATS_PRINT_INTERVAL = 60
STATS_MIN_RESOLVED = 5

# --- Edge thresholds for stratified accuracy ---
EDGE_THRESHOLDS = [0.05, 0.10, 0.20]

# --- Retry / backoff ---
MAX_RETRIES = 3
RETRY_BACKOFF_BASE = 2

# --- Watcher ---
WATCHER_POLL_INTERVAL = 5  # seconds

# --- Database ---
DB_PATH = os.environ.get("VALIDATOR_DB_PATH", "predictions.db")

# --- Feature names (ordered) ---
FEATURE_NAMES = [
    "return_30s",
    "return_1m",
    "return_3m",
    "return_5m",
    "rsi_14",
    "atr_5m",
    "trade_flow_imbalance",
    "candle_body_ratio",
    "candle_body_signed",   # NEW: signed (direction) candle body
    "vol_ratio",            # NEW: recent vol activity vs 5-min avg
    "poly_mid",
    "poly_spread",
    "binance_price",
    "time_remaining_s",
    "distance_to_open",
    "p_deviation",
    "is_above_open",
]

# --- Model thresholds ---
P_THRESHOLD_UP = 0.58    # predict UP if P(UP) > this
P_THRESHOLD_DOWN = 0.42  # predict DOWN if P(UP) < this

# --- Money simulation (Half-Kelly) ---
INITIAL_BANKROLL = 1000.0
KELLY_MAX_FRACTION = 0.10    # hard cap: never bet more than 10% of bankroll
MIN_KELLY_FRACTION = 0.005   # skip bet if Half-Kelly < 0.5% (noise)

# Legacy aliases (kept for any references)
FLOW_IMBALANCE_THRESHOLD = 0.1
P_HAT_UP = 0.70
P_HAT_DOWN = 0.30
P_HAT_SKIP = 0.50
