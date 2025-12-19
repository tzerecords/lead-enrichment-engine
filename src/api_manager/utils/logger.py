from __future__ import annotations

import logging
from typing import Optional, Dict, Any


def get_logger(name: str = "tier1") -> logging.Logger:
    """Return a configured logger for the Tier 1 enricher.

    The logger is configured once with a simple structured format and INFO level
    by default. Callers can override the level on the returned logger.
    """

    logger = logging.getLogger(name)
    if logger.handlers:
        return logger

    logger.setLevel(logging.INFO)
    handler = logging.StreamHandler()
    formatter = logging.Formatter(
        fmt="%(asctime)s %(levelname)s %(name)s %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.propagate = False
    return logger


def log_event(
    logger: logging.Logger,
    level: int,
    message: str,
    extra: Optional[Dict[str, Any]] = None,
) -> None:
    """Log an event with optional structured extra context.

    Args:
        logger: Logger instance from ``get_logger``.
        level: Logging level from ``logging`` (e.g., logging.INFO).
        message: Human-readable message.
        extra: Optional dictionary with additional context.
    """

    if extra is None:
        logger.log(level, message)
    else:
        logger.log(level, f"{message} | extra={extra}")

