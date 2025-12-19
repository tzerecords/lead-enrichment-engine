from __future__ import annotations

import re
from typing import Optional

import requests
from bs4 import BeautifulSoup

from ...base import PhoneResult
from ...utils.logger import get_logger, log_event
from ...utils.retry import with_retry


_PHONE_REGEX = re.compile(
    r"((?:\+34|0034|34)?[\s-]?(?:6|7|8|9)(?:[\s-]?[0-9]){8})"
)


def _normalize_url(url: str) -> str:
    """Normalize URL by adding https:// scheme if missing.

    Args:
        url: URL string (may be missing scheme).

    Returns:
        Normalized URL with https:// scheme.

    Examples:
        "www.endesa.es" -> "https://www.endesa.es"
        "endesa.es" -> "https://endesa.es"
        "https://endesa.es" -> "https://endesa.es" (no change)
    """
    url = url.strip()
    if not url:
        return url

    # If already has scheme, return as-is
    if url.startswith(("http://", "https://")):
        return url

    # Add https:// prefix
    return f"https://{url}"


class WebScraperPhoneFinder:
    """Fallback phone finder using the company website.

    Expects the lead to provide a ``WEBSITE``/URL; if not present, this finder
    will usually return no result. Automatically adds https:// if scheme is missing.
    """

    source_name = "web_scraper"

    def __init__(self, timeout: int = 10) -> None:
        self.timeout = timeout
        self.logger = get_logger("tier1.web_scraper")

    @with_retry((requests.RequestException,))
    def _fetch(self, url: str) -> str:
        normalized_url = _normalize_url(url)
        resp = requests.get(
            normalized_url,
            timeout=self.timeout,
            headers={"User-Agent": "Tier1Enricher/1.0"},
        )
        resp.raise_for_status()
        return resp.text

    def find(self, company_name: str, address: Optional[str] = None, website: Optional[str] = None) -> PhoneResult:  # type: ignore[override]
        if not website:
            return PhoneResult(phone=None, confidence=0.0, source=self.source_name)

        try:
            html = self._fetch(website)
        except requests.RequestException as exc:
            log_event(
                self.logger,
                level=30,
                message="Web scraper failed to fetch website",
                extra={"website": website, "error": str(exc)},
            )
            return PhoneResult(phone=None, confidence=0.0, source=self.source_name)

        soup = BeautifulSoup(html, "html.parser")

        # First try explicit tel: links
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if href.lower().startswith("tel:"):
                phone = href.split(":", 1)[1]
                return PhoneResult(phone=phone, confidence=0.6, source=self.source_name)

        # Then regex over text
        text = soup.get_text(" ", strip=True)
        match = _PHONE_REGEX.search(text)
        if match:
            return PhoneResult(phone=match.group(1), confidence=0.5, source=self.source_name)

        return PhoneResult(phone=None, confidence=0.0, source=self.source_name)
