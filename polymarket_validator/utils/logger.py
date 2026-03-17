"""Structured logging setup."""

import logging
import sys


def setup_logging(level: str = "INFO") -> logging.Logger:
    """Configure and return the root application logger."""
    logger = logging.getLogger("validator")
    logger.setLevel(getattr(logging, level.upper(), logging.INFO))

    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setLevel(logging.DEBUG)
        fmt = logging.Formatter(
            "[%(asctime)s] %(levelname)-5s %(name)s | %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        handler.setFormatter(fmt)
        logger.addHandler(handler)

    return logger


def get_logger(name: str) -> logging.Logger:
    """Get a child logger under the validator namespace."""
    return logging.getLogger(f"validator.{name}")
