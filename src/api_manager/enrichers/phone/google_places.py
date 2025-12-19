from __future__ import annotations

from typing import Optional

import googlemaps

from ...base import PhoneResult
from ...utils.logger import get_logger, log_event
from ...utils.rate_limiter import RateLimiter, ProviderLimit
from ...utils.retry import with_retry
from ....utils.config_loader import load_yaml_config


class GooglePlacesPhoneFinder:
    """Phone finder using Google Maps Places API."""

    source_name = "google_places"

    def __init__(
        self,
        api_key: str,
        rate_limiter: Optional[RateLimiter] = None,
        monthly_limit: int = 10000,
    ) -> None:
        self.client = googlemaps.Client(key=api_key)
        self.logger = get_logger("tier1.google_places")
        self.rate_limiter = rate_limiter or RateLimiter()
        self.limit = ProviderLimit(name=self.source_name, monthly_limit=monthly_limit)

    @with_retry((Exception,))  # broad for simplicity around HTTP/client errors
    def _search(self, query: str) -> Optional[dict]:
        results = self.client.places(query=query, region="es")
        candidates = results.get("results", [])
        if not candidates:
            return None
        return candidates[0]

    def find(self, company_name: str, address: Optional[str] = None) -> PhoneResult:
        if not self.rate_limiter.check_limit(self.source_name, self.limit):
            log_event(
                self.logger,
                level=30,
                message="Google Places rate limit exceeded, skipping call",
                extra={"company_name": company_name},
            )
            return PhoneResult(phone=None, confidence=0.0, source=self.source_name)

        query_parts = [company_name, "España"]
        if address:
            query_parts.insert(1, address)
        query = " ".join(part for part in query_parts if part)

        place = None
        try:
            place = self._search(query)
            self.rate_limiter.increment(self.source_name)
        except Exception as exc:  # pragma: no cover - defensive
            log_event(
                self.logger,
                level=40,
                message="Google Places search failed",
                extra={"query": query, "error": str(exc)},
            )
            return PhoneResult(phone=None, confidence=0.0, source=self.source_name)

        if not place:
            return PhoneResult(phone=None, confidence=0.0, source=self.source_name)

        phone = place.get("formatted_phone_number") or place.get("international_phone_number")
        user_ratings_total = place.get("user_ratings_total", 0)
        rating = place.get("rating", 0.0)

        # Simple heuristic for confidence
        confidence = 0.3
        if phone:
            confidence = 0.6
            if user_ratings_total >= 10:
                confidence = 0.8
            if rating and rating >= 4.0 and user_ratings_total >= 20:
                confidence = 0.9

        return PhoneResult(
            phone=phone,
            confidence=confidence,
            source=self.source_name,
            extra={"place_id": place.get("place_id")},
        )


def load_google_places_from_config(config_path: str) -> GooglePlacesPhoneFinder:
    cfg = load_yaml_config(config_path)
    api_keys = load_yaml_config("config/api_keys.yaml")
    tier1 = cfg.get("tier1", {})
    rate_limits = tier1.get("rate_limits", {})

    monthly_limit = int(rate_limits.get("google_places", 10000))
    api_key = api_keys.get("google_maps", {}).get("api_key", "")

    return GooglePlacesPhoneFinder(api_key=api_key, monthly_limit=monthly_limit)
