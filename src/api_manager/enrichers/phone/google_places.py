from __future__ import annotations

from typing import Optional, Dict, Any

import requests

from ...base import PhoneResult
from ...utils.logger import get_logger, log_event
from ...utils.rate_limiter import RateLimiter, ProviderLimit
from ...utils.retry import with_retry


class GooglePlacesEnricher:
    """Company enricher using Google Places API (New).

    Uses the new Places API (places.googleapis.com) directly via HTTP requests.
    Provides phone number, company name (razón social), and address.
    Uses daily rate limit of 200 requests (configurable).
    """

    source_name = "google_places"
    BASE_URL = "https://places.googleapis.com/v1"

    def __init__(
        self,
        api_key: str,
        rate_limiter: Optional[RateLimiter] = None,
        daily_limit: int = 200,
    ) -> None:
        self.api_key = api_key
        self.logger = get_logger("tier1.google_places")
        self.rate_limiter = rate_limiter or RateLimiter()
        self.limit = ProviderLimit(name=self.source_name, monthly_limit=daily_limit)

    def _get_headers(self) -> Dict[str, str]:
        """Get HTTP headers for Places API requests."""
        return {
            "Content-Type": "application/json",
            "X-Goog-Api-Key": self.api_key,
            "X-Goog-FieldMask": "places.id,places.displayName,places.formattedAddress,places.nationalPhoneNumber,places.internationalPhoneNumber,places.rating,places.userRatingCount,places.businessStatus",
        }

    @with_retry((requests.RequestException,))
    def _search_place(self, text_query: str) -> Optional[dict]:
        """Search for a place using Google Places API (New).

        Args:
            text_query: Search query string.

        Returns:
            First place result or None if not found.
        """
        url = f"{self.BASE_URL}/places:searchText"
        payload = {
            "textQuery": text_query,
            "languageCode": "es",
            "regionCode": "ES",
        }

        try:
            response = requests.post(
                url,
                json=payload,
                headers=self._get_headers(),
                timeout=5,  # Maximum 5 seconds
            )
            response.raise_for_status()
            data = response.json()

            places = data.get("places", [])
            if not places:
                return None
            return places[0]

        except requests.RequestException as exc:
            log_event(
                self.logger,
                level=40,
                message="Places API search request failed",
                extra={"text_query": text_query, "error": str(exc)},
            )
            raise

    @with_retry((requests.RequestException,))
    def _get_place_details(self, place_id: str) -> Optional[dict]:
        """Get detailed information for a place using place_id.

        Args:
            place_id: Google Places place ID.

        Returns:
            Place details or None if not found.
        """
        url = f"{self.BASE_URL}/places/{place_id}"
        headers = {
            "Content-Type": "application/json",
            "X-Goog-Api-Key": self.api_key,
            "X-Goog-FieldMask": "id,displayName,formattedAddress,nationalPhoneNumber,internationalPhoneNumber,rating,userRatingCount,businessStatus",
        }

        try:
            response = requests.get(url, headers=headers, timeout=5)  # Maximum 5 seconds
            response.raise_for_status()
            return response.json()
        except requests.RequestException as exc:
            log_event(
                self.logger,
                level=40,
                message="Failed to get place details",
                extra={"place_id": place_id, "error": str(exc)},
            )
            return None

    def _calculate_confidence(self, place: Dict[str, Any]) -> float:
        """Calculate confidence score based on place metadata."""
        score = 0.5
        if place.get("nationalPhoneNumber") or place.get("internationalPhoneNumber"):
            score += 0.1
        user_rating_count = place.get("userRatingCount", 0)
        if user_rating_count >= 10:
            score += 0.2
        if place.get("businessStatus") == "OPERATIONAL":
            score += 0.2
        rating = place.get("rating", 0.0)
        if rating and rating >= 4.0 and user_rating_count >= 20:
            score += 0.1
        return min(score, 1.0)

    def find_company(
        self, company_name: str, city: Optional[str] = None
    ) -> Dict[str, Any]:
        """Find company information (phone, name, address) using Google Places.

        Args:
            company_name: Company name to search for.
            city: Optional city name to narrow search.

        Returns:
            Dictionary with:
            - phone: Formatted phone number or None
            - international_phone: International format phone or None
            - name: Company name from Google Places
            - address: Formatted address
            - confidence: Confidence score (0.0-1.0)
            - source: "google_places"
            - error: Error message if failed, None otherwise
        """
        # Check rate limit
        if not self.rate_limiter.check_limit(self.source_name, self.limit):
            log_event(
                self.logger,
                level=30,
                message="Google Places rate limit exceeded, skipping call",
                extra={"company_name": company_name},
            )
            return {
                "phone": None,
                "international_phone": None,
                "name": None,
                "address": None,
                "confidence": 0.0,
                "source": self.source_name,
                "error": "RATE_LIMIT_EXCEEDED",
            }

        # Build search query
        query_parts = [company_name, "España"]
        if city:
            query_parts.insert(1, city)
        text_query = " ".join(part for part in query_parts if part)

        # Search for place
        place = None
        try:
            place = self._search_place(text_query)
            self.rate_limiter.increment(self.source_name)
        except Exception as exc:
            log_event(
                self.logger,
                level=40,
                message="Google Places search failed",
                extra={"text_query": text_query, "error": str(exc)},
            )
            return {
                "phone": None,
                "international_phone": None,
                "name": None,
                "address": None,
                "confidence": 0.0,
                "source": self.source_name,
                "error": f"SEARCH_FAILED: {str(exc)}",
            }

        if not place:
            return {
                "phone": None,
                "international_phone": None,
                "name": None,
                "address": None,
                "confidence": 0.0,
                "source": self.source_name,
                "error": "NOT_FOUND",
            }

        # Get detailed information (if we have place_id)
        place_id = place.get("id")
        detail = None
        if place_id:
            detail = self._get_place_details(place_id)
            if detail:
                self.rate_limiter.increment(self.source_name)  # Count detail call too

        # Use detail if available, otherwise use basic place info
        data = detail if detail else place

        # Extract data (new API uses different field names)
        phone = data.get("nationalPhoneNumber") or data.get("internationalPhoneNumber")
        international_phone = data.get("internationalPhoneNumber")
        # displayName is a dict with text field in new API
        name_obj = data.get("displayName")
        name = name_obj.get("text") if isinstance(name_obj, dict) else name_obj or data.get("name")
        address = data.get("formattedAddress")
        confidence = self._calculate_confidence(data) if detail else 0.5

        return {
            "phone": phone,
            "international_phone": international_phone,
            "name": name,
            "address": address,
            "confidence": confidence,
            "source": self.source_name,
            "error": None,
        }

    def find(self, company_name: str, address: Optional[str] = None) -> PhoneResult:
        """Legacy method for PhoneFinder interface compatibility.

        This method is kept for backward compatibility but find_company()
        should be used for full enrichment.
        """
        result = self.find_company(company_name=company_name, city=address)
        return PhoneResult(
            phone=result.get("phone") or result.get("international_phone"),
            confidence=result.get("confidence", 0.0),
            source=self.source_name,
            extra={"error": result.get("error")},
        )


def load_google_places_from_config(config_path: str) -> GooglePlacesEnricher:
    """Helper to build GooglePlacesEnricher from YAML config."""
    from ....utils.config_loader import load_yaml_config

    cfg = load_yaml_config(config_path)
    try:
        api_keys = load_yaml_config("config/api_keys.yaml")
    except FileNotFoundError:
        api_keys = {}

    tier1 = cfg.get("tier1", {})
    rate_limits = tier1.get("rate_limits", {})

    daily_limit = int(rate_limits.get("google_places", 200))
    # Support both old format (google_maps.api_key) and new format (google_places_key)
    api_key = (
        api_keys.get("google_places_key")
        or api_keys.get("google_maps", {}).get("api_key", "")
        or ""
    )

    return GooglePlacesEnricher(api_key=api_key, daily_limit=daily_limit)
