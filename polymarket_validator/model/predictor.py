"""Placeholder prediction model. Swap this module to upgrade the model."""

import numpy as np

from polymarket_validator import config
from polymarket_validator.utils.logger import get_logger

log = get_logger("model")


class Predictor:
    """Placeholder model using simple heuristics.

    Interface contract (must be preserved when swapping models):
        predict(features_dict, features_array) -> (p_hat, label)
            p_hat: float in [0, 1], estimated probability of UP outcome
            label: str, one of "UP", "DOWN", "SKIP"
    """

    def __init__(self):
        log.info("Loaded placeholder predictor (heuristic model)")

    def predict(self, features_dict: dict, features_array: np.ndarray) -> tuple[float, str]:
        """Run inference on the feature vector.

        Placeholder logic:
            - If trade_flow_imbalance > threshold AND return_30s > 0 -> UP
            - If trade_flow_imbalance < -threshold AND return_30s < 0 -> DOWN
            - Else -> SKIP
        """
        flow = features_dict["trade_flow_imbalance"]
        ret_30s = features_dict["return_30s"]

        if flow > config.FLOW_IMBALANCE_THRESHOLD and ret_30s > 0:
            return config.P_HAT_UP, "UP"
        elif flow < -config.FLOW_IMBALANCE_THRESHOLD and ret_30s < 0:
            return config.P_HAT_DOWN, "DOWN"
        else:
            return config.P_HAT_SKIP, "SKIP"
