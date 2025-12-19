from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Dict

from .logger import get_logger, log_event


DEFAULT_STORAGE_PATH = Path(os.getenv("TIER1_RATE_LIMIT_FILE", "tier1_rate_limits.json"))


@dataclass
class ProviderLimit:
    """Rate limit configuration for a provider."""

    name: str
    monthly_limit: int


class RateLimiter:
    """Simple JSON-based rate limiter for API providers.

    This is not distributed-safe but is sufficient for the current batch CLI use case.
    """

    def __init__(self, storage_path: Path = DEFAULT_STORAGE_PATH) -> None:
        self.storage_path = storage_path
        self.logger = get_logger("tier1.rate_limiter")
        self._usage: Dict[str, int] = {}
        self._load()

    def _load(self) -> None:
        if self.storage_path.exists():
            try:
                with self.storage_path.open("r", encoding="utf-8") as f:
                    self._usage = json.load(f)
            except Exception as exc:  # pragma: no cover - defensive
                log_event(
                    self.logger,
                    level=20,
                    message="Failed to load rate limit file, starting fresh",
                    extra={"error": str(exc)},
                )
                self._usage = {}

    def _save(self) -> None:
        try:
            with self.storage_path.open("w", encoding="utf-8") as f:
                json.dump(self._usage, f)
        except Exception as exc:  # pragma: no cover - defensive
            log_event(
                self.logger,
                level=40,
                message="Failed to persist rate limit file",
                extra={"error": str(exc)},
            )

    def increment(self, provider: str, count: int = 1) -> None:
        """Increment usage counter for a provider."""

        self._usage[provider] = self._usage.get(provider, 0) + count
        self._save()

    def get_usage(self, provider: str) -> int:
        """Return current usage for a provider."""

        return self._usage.get(provider, 0)

    def get_remaining(self, provider: str, limit: ProviderLimit) -> int:
        """Return remaining quota for a provider."""

        used = self.get_usage(provider)
        return max(limit.monthly_limit - used, 0)

    def check_limit(self, provider: str, limit: ProviderLimit, alert_threshold: float = 0.8) -> bool:
        """Check whether a call is allowed and emit alerts when close to the limit.

        Returns:
            bool: True if the call is allowed, False if limit would be exceeded.
        """

        used = self.get_usage(provider)
        ratio = used / max(limit.monthly_limit, 1)

        if ratio >= 0.95:
            log_event(
                self.logger,
                level=50,
                message="Rate limit CRITICAL threshold reached",
                extra={"provider": provider, "used": used, "limit": limit.monthly_limit},
            )
        elif ratio >= alert_threshold:
            log_event(
                self.logger,
                level=30,
                message="Rate limit warning threshold reached",
                extra={"provider": provider, "used": used, "limit": limit.monthly_limit},
            )

        if used + 1 > limit.monthly_limit:
            return False

        return True

