from __future__ import annotations

import functools
import time
from typing import Callable, Tuple, Type, Any

from .logger import get_logger, log_event


def with_retry(
    exceptions: Tuple[Type[BaseException], ...],
    max_attempts: int = 3,
    backoff_factor: float = 2.0,
    base_delay: float = 0.5,
) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    """Simple retry decorator with exponential backoff.

    Args:
        exceptions: Tuple of exception types that should trigger a retry.
        max_attempts: Maximum number of attempts (including the first).
        backoff_factor: Multiplier for the delay after each failure.
        base_delay: Initial delay in seconds.
    """

    def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
        logger = get_logger(f"tier1.retry.{func.__name__}")

        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            delay = base_delay
            attempt = 1
            while True:
                try:
                    return func(*args, **kwargs)
                except exceptions as exc:  # type: ignore[misc]
                    if attempt >= max_attempts:
                        raise
                    log_event(
                        logger,
                        level=30,
                        message="Retryable error, will retry",
                        extra={"attempt": attempt, "error": str(exc)},
                    )
                    time.sleep(delay)
                    delay *= backoff_factor
                    attempt += 1

        return wrapper

    return decorator

