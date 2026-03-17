"""Logistic regression model for BTC 5-min direction prediction.

Features used (9 inputs to the linear layer):
  0: return_30s          short-term price momentum
  1: return_1m           1-minute momentum
  2: return_3m           3-minute momentum
  3: rsi_norm            (RSI-50)/50  → -1..+1  (mean reversion signal)
  4: trade_flow_imbalance buy-pressure relative to sell-pressure
  5: candle_body_signed  direction × conviction of last 1m candle
  6: macd_proxy          return_1m - return_3m  (momentum acceleration)
  7: distance_to_open    current price vs window-open price
  8: vol_deviation       vol_ratio - 1.0  (0 = average activity)

Initial weights are hand-tuned from market-microstructure research:
  - Short-term momentum (returns, flow) carries for ~5 minutes in liquid markets
  - RSI mean-reversion is weak but real at 5-min horizon
  - Volume surge confirms directional moves

Weights are updated online via gradient descent after each resolved market.
"""

import numpy as np

from polymarket_validator import config
from polymarket_validator.utils.logger import get_logger

log = get_logger("model")


def _sigmoid(x: float) -> float:
    return 1.0 / (1.0 + np.exp(-float(np.clip(x, -20.0, 20.0))))


class Predictor:
    """Online logistic regression for BTC 5-min direction.

    Interface contract (unchanged from placeholder):
        predict(features_dict, features_array) -> (p_hat, label)
            p_hat: float in [0, 1], P(UP)
            label: 'UP', 'DOWN', or 'SKIP'

    Extra public method:
        update(features_dict, outcome) -> None
            outcome: 1.0 = UP resolved, 0.0 = DOWN resolved
    """

    # Keys extracted from features_dict (in order)
    _KEYS = [
        "return_30s",
        "return_1m",
        "return_3m",
        "rsi_norm",           # computed internally
        "trade_flow_imbalance",
        "candle_body_signed",
        "macd_proxy",         # computed internally
        "distance_to_open",
        "vol_deviation",      # computed internally
    ]

    def __init__(self):
        # Hand-tuned initial weights.
        # Returns are tiny (e.g. 0.001) so need large weights.
        # Flow imbalance is [-1, 1] so needs moderate weight.
        self._w = np.array([
            600.0,   # return_30s  — strong short-term momentum
            350.0,   # return_1m
            100.0,   # return_3m   — weaker at longer horizon
            -1.8,    # rsi_norm    — mean-reversion bias
            3.5,     # trade_flow_imbalance — buy-pressure signal
            0.9,     # candle_body_signed   — directional conviction
            250.0,   # macd_proxy  — momentum acceleration
            500.0,   # distance_to_open    — position vs window open
            0.15,    # vol_deviation       — volume confirmation
        ], dtype=np.float64)
        self._bias = 0.0
        self._lr = 0.05          # base learning rate
        self._l2 = 0.001         # L2 regularisation
        self._n = 0              # update counter
        log.info("Loaded logistic-regression predictor (hand-tuned initial weights)")

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def predict(self, features_dict: dict, features_array: np.ndarray) -> tuple[float, str]:
        x = self._extract(features_dict)
        p_hat = _sigmoid(float(self._w @ x) + self._bias)

        if p_hat > config.P_THRESHOLD_UP:
            label = "UP"
        elif p_hat < config.P_THRESHOLD_DOWN:
            label = "DOWN"
        else:
            label = "SKIP"

        return p_hat, label

    def update(self, features_dict: dict, outcome: float):
        """One gradient-descent step on a resolved prediction.

        Args:
            features_dict: same dict that was passed to predict()
            outcome: 1.0 if UP resolved, 0.0 if DOWN resolved
        """
        x = self._extract(features_dict)
        p = _sigmoid(float(self._w @ x) + self._bias)
        err = p - outcome  # d(log-loss)/d(z)

        # Decaying learning rate
        lr = self._lr / (1.0 + 0.001 * self._n)

        self._w -= lr * (err * x + self._l2 * self._w)
        self._bias -= lr * err
        self._n += 1

        if self._n % 10 == 0:
            log.info(
                "Model updated n=%d | bias=%.4f | top-w: flow=%.3f ret30=%.1f",
                self._n, self._bias, self._w[4], self._w[0],
            )

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    @staticmethod
    def _extract(fd: dict) -> np.ndarray:
        rsi_norm = (fd.get("rsi_14", 50.0) - 50.0) / 50.0
        macd_proxy = fd.get("return_1m", 0.0) - fd.get("return_3m", 0.0)
        vol_dev = fd.get("vol_ratio", 1.0) - 1.0

        return np.array([
            fd.get("return_30s", 0.0),
            fd.get("return_1m", 0.0),
            fd.get("return_3m", 0.0),
            rsi_norm,
            fd.get("trade_flow_imbalance", 0.0),
            fd.get("candle_body_signed", 0.0),
            macd_proxy,
            fd.get("distance_to_open", 0.0),
            vol_dev,
        ], dtype=np.float64)
