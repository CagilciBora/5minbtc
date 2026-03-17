"""Configuration constants for the Polymarket BTC validator."""

import os

# --- Binance ---
BINANCE_WS_URL = "wss://stream.binance.com:9443/ws/btcusdt@aggTrade"
BINANCE_BUFFER_SECONDS = 300  # warmup period

# --- Polymarket APIs ---
# CLOB API (requires no auth for reads, but may need Brotli or have quirks)
CLOB_BASE_URL = "https://clob.polymarket.com"
CLOB_MARKETS_URL = f"{CLOB_BASE_URL}/markets"

# Gamma API (public, better for market discovery/search)
GAMMA_BASE_URL = "https://gamma-api.polymarket.com"
GAMMA_MARKETS_URL = f"{GAMMA_BASE_URL}/markets"

# --- Market filtering ---
MARKET_LOOKAHEAD_SECONDS = 600  # only consider markets expiring within 10 min
MARKET_QUESTION_KEYWORDS = ["bitcoin", "btc"]
MARKET_QUESTION_TIME_PATTERN = r"5.?min"

# --- Prediction trigger ---
PREDICTION_WINDOW_LOW = 55   # seconds before expiry (inclusive)
PREDICTION_WINDOW_HIGH = 65  # seconds before expiry (inclusive)

# --- Resolver ---
RESOLVER_MIN_AGE_SECONDS = 600  # wait 10 min after prediction before polling
RESOLVER_POLL_INTERVAL = 120    # poll every 2 minutes

# --- Stats ---
STATS_PRINT_INTERVAL = 60       # print stats every 60 seconds
STATS_MIN_RESOLVED = 10         # minimum resolved predictions before printing

# --- Edge thresholds for stratified accuracy ---
EDGE_THRESHOLDS = [0.10, 0.20]

# --- Retry / backoff ---
MAX_RETRIES = 3
RETRY_BACKOFF_BASE = 2  # seconds: 2, 4, 8

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
    "poly_mid",
    "poly_spread",
    "binance_price",
    "time_remaining_s",
    "distance_to_open",
    "p_deviation",
    "is_above_open",
]

# --- Model thresholds (placeholder) ---
FLOW_IMBALANCE_THRESHOLD = 0.1
P_HAT_UP = 0.70
P_HAT_DOWN = 0.30
P_HAT_SKIP = 0.50
