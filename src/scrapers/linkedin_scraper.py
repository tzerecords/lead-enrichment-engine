from __future__ import annotations

from dataclasses import dataclass
from typing import Optional
from urllib.parse import unquote, urlparse, parse_qs
import re
import time

from playwright.sync_api import sync_playwright, Browser, Page, TimeoutError as PlaywrightTimeout

from ..api_manager.utils.logger import get_logger, log_event


@dataclass
class LinkedInResult:
    """Result of LinkedIn company search."""

    company_url: Optional[str]
    success: bool
    error: Optional[str] = None


class LinkedInScraper:
    """LinkedIn company URL finder using Google Dorking with Playwright.

    Uses Google search with site:linkedin.com/company to find company LinkedIn page.
    No individual profile scraping (too complex for free version).
    """

    def __init__(self, timeout: int = 15) -> None:
        """Initialize LinkedIn scraper.

        Args:
            timeout: Page load timeout in seconds (default: 15).
        """
        self.timeout = timeout * 1000  # Playwright uses milliseconds
        self.logger = get_logger("tier2.linkedin_scraper")

    def _extract_linkedin_url(self, page: Page) -> Optional[str]:
        """Extract LinkedIn company URL from Google search results.

        Args:
            page: Playwright page object.

        Returns:
            LinkedIn company URL if found, None otherwise.
        """
        try:
            # Wait for search results to load (increased timeout to 15s)
            page.wait_for_selector("div#search", timeout=15000)

            # Get page content
            content = page.content()

            # Look for LinkedIn company URLs in the HTML
            # Pattern: https://www.linkedin.com/company/...
            linkedin_pattern = r'https?://(?:www\.)?linkedin\.com/company/[a-zA-Z0-9-]+'
            matches = re.findall(linkedin_pattern, content)

            if matches:
                # Return first match (most relevant)
                url = matches[0]
                # Ensure it's a full URL
                if not url.startswith("http"):
                    url = f"https://{url}"
                return url

            # Alternative: try to find in href attributes
            links = page.query_selector_all('a[href*="linkedin.com/company"]')
            if links:
                href = links[0].get_attribute("href")
                if href:
                    # Google sometimes wraps URLs
                    if href.startswith("/url?q="):
                        parsed = parse_qs(urlparse(href).query)
                        if "q" in parsed:
                            href = unquote(parsed["q"][0])
                    if "linkedin.com/company" in href:
                        return href

            return None

        except Exception as exc:
            log_event(
                self.logger,
                level=30,
                message="Failed to extract LinkedIn URL",
                extra={"error": str(exc)},
            )
            return None

    def find_company(self, company_name: str) -> LinkedInResult:
        """Find LinkedIn company page using Google Dorking.

        Args:
            company_name: Company name to search for.

        Returns:
            LinkedInResult with company URL or error.
        """
        if not company_name or not company_name.strip():
            return LinkedInResult(
                company_url=None,
                success=False,
                error="EMPTY_COMPANY_NAME",
            )

        # Build Google search query
        query = f'site:linkedin.com/company "{company_name}"'
        google_url = f"https://www.google.com/search?q={query.replace(' ', '+')}"

        try:
            with sync_playwright() as p:
                # Launch browser (headless mode) with explicit user agent
                browser = p.chromium.launch(
                    headless=True,
                    args=["--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"]
                )
                context = browser.new_context(
                    user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
                )
                page = context.new_page()

                try:
                    # Navigate to Google search
                    page.goto(google_url, wait_until="domcontentloaded", timeout=self.timeout)
                    
                    # Add 2 second delay to let page fully load
                    time.sleep(2)

                    # Extract LinkedIn URL
                    linkedin_url = self._extract_linkedin_url(page)

                    if linkedin_url:
                        log_event(
                            self.logger,
                            level=20,
                            message="LinkedIn company URL found",
                            extra={"company": company_name, "url": linkedin_url},
                        )
                        return LinkedInResult(
                            company_url=linkedin_url,
                            success=True,
                        )
                    else:
                        return LinkedInResult(
                            company_url=None,
                            success=False,
                            error="NOT_FOUND",
                        )

                finally:
                    browser.close()

        except PlaywrightTimeout:
            log_event(
                self.logger,
                level=30,
                message="Playwright timeout",
                extra={"company": company_name},
            )
            return LinkedInResult(
                company_url=None,
                success=False,
                error="TIMEOUT",
            )

        except Exception as exc:
            log_event(
                self.logger,
                level=40,
                message="LinkedIn scraper failed",
                extra={"company": company_name, "error": str(exc)},
            )
            return LinkedInResult(
                company_url=None,
                success=False,
                error=f"SCRAPER_ERROR: {str(exc)}",
            )


def load_linkedin_scraper_from_config() -> LinkedInScraper:
    """Helper to create LinkedInScraper with default settings."""
    return LinkedInScraper(timeout=15)
