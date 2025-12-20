from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, List
from urllib.parse import urljoin, urlparse

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import urllib3

from ..api_manager.utils.logger import get_logger, log_event

# Suppress SSL warnings for self-signed certificates
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


@dataclass
class ScrapedPage:
    """Result of scraping a web page."""

    html: Optional[str]
    url: str  # Final URL after redirects
    success: bool
    error: Optional[str] = None


# Common contact page paths to try
CONTACT_PATHS = [
    "/contacto",
    "/contactanos",
    "/contact",
    "/contact-us",
    "/about",
    "/about-us",
    "/quienes-somos",
    "/equipo",
    "/team",
]


def _normalize_url(url: str) -> str:
    """Normalize URL by adding https:// scheme if missing.

    Args:
        url: URL string (may be missing scheme).

    Returns:
        Normalized URL with https:// scheme.
    """
    url = url.strip()
    if not url:
        return url

    if url.startswith(("http://", "https://")):
        return url

    return f"https://{url}"


class ContactPageScraper:
    """Web scraper for fetching contact page HTML.

    Auto-detects contact pages by trying common paths.
    Handles redirects, timeouts, SSL issues, and 404s.
    """

    def __init__(self, timeout: int = 5, max_redirects: int = 5) -> None:
        """Initialize scraper.

        Args:
            timeout: Request timeout in seconds (default: 5).
            max_redirects: Maximum number of redirects to follow (default: 5).
        """
        self.timeout = timeout
        self.max_redirects = max_redirects
        self.logger = get_logger("tier2.web_scraper")

        # Configure session with retry strategy
        self.session = requests.Session()
        retry_strategy = Retry(
            total=2,
            backoff_factor=0.3,
            status_forcelist=[429, 500, 502, 503, 504],
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)

        # Accept invalid certificates (for self-signed or expired)
        self.session.verify = False

    def _fetch_url(self, url: str) -> ScrapedPage:
        """Fetch HTML content from a URL.

        Args:
            url: URL to fetch.

        Returns:
            ScrapedPage with HTML content and final URL.
        """
        try:
            response = self.session.get(
                url,
                timeout=self.timeout,
                headers={
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                    "Accept-Language": "es-ES,es;q=0.9,en;q=0.8",
                },
                allow_redirects=True,
            )

            response.raise_for_status()

            # Get final URL after redirects
            final_url = response.url

            # Try to decode content
            content = response.text
            if not content:
                return ScrapedPage(
                    html=None,
                    url=final_url,
                    success=False,
                    error="Empty response",
                )

            return ScrapedPage(
                html=content,
                url=final_url,
                success=True,
            )

        except requests.exceptions.Timeout:
            return ScrapedPage(
                html=None,
                url=url,
                success=False,
                error="TIMEOUT",
            )
        except requests.exceptions.TooManyRedirects:
            return ScrapedPage(
                html=None,
                url=url,
                success=False,
                error="TOO_MANY_REDIRECTS",
            )
        except requests.exceptions.HTTPError as exc:
            status_code = exc.response.status_code if exc.response else None
            return ScrapedPage(
                html=None,
                url=url,
                success=False,
                error=f"HTTP_{status_code}",
            )
        except requests.exceptions.RequestException as exc:
            log_event(
                self.logger,
                level=30,
                message="Request failed",
                extra={"url": url, "error": str(exc)},
            )
            return ScrapedPage(
                html=None,
                url=url,
                success=False,
                error=f"REQUEST_ERROR: {str(exc)}",
            )

    def scrape_contact_page(self, base_url: str) -> ScrapedPage:
        """Scrape contact page by trying common paths.

        Args:
            base_url: Base website URL (e.g., "www.example.com" or "https://example.com").

        Returns:
            ScrapedPage with HTML content from first successful path, or error if all fail.
        """
        base_url = _normalize_url(base_url)
        parsed = urlparse(base_url)
        base_domain = f"{parsed.scheme}://{parsed.netloc}"

        # Try base URL first (might be homepage with contact info)
        result = self._fetch_url(base_url)
        if result.success:
            log_event(
                self.logger,
                level=20,
                message="Successfully scraped base URL",
                extra={"url": result.url},
            )
            return result

        # Try common contact page paths
        for path in CONTACT_PATHS:
            contact_url = urljoin(base_domain, path)
            result = self._fetch_url(contact_url)

            if result.success:
                log_event(
                    self.logger,
                    level=20,
                    message="Successfully scraped contact page",
                    extra={"url": result.url, "path": path},
                )
                return result

            # Log failure but continue trying
            log_event(
                self.logger,
                level=10,
                message="Contact path failed, trying next",
                extra={"url": contact_url, "error": result.error},
            )

        # All paths failed, return last error
        return ScrapedPage(
            html=None,
            url=base_url,
            success=False,
            error="ALL_PATHS_FAILED",
        )

    def scrape_url(self, url: str) -> ScrapedPage:
        """Scrape a specific URL (no path detection).

        Args:
            url: Full URL to scrape.

        Returns:
            ScrapedPage with HTML content.
        """
        normalized_url = _normalize_url(url)
        return self._fetch_url(normalized_url)


def load_scraper_from_config() -> ContactPageScraper:
    """Helper to create ContactPageScraper with default settings."""
    return ContactPageScraper(timeout=5, max_redirects=5)
