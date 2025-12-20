"""Tier 3 enricher for website and CNAE fields.

Only fills empty fields based on enrichment_rules.yaml.
"""

from __future__ import annotations

from typing import Protocol, Any
from urllib.parse import urlparse
import re

import pandas as pd
import requests

from ..utils.logger import setup_logger
from ..utils.config_loader import load_yaml_config

logger = setup_logger()


class SearchClient(Protocol):
    """Protocol for search clients that can find company websites and CNAE codes."""

    def search_company_website(self, query: str) -> str | None:
        """Search for company website.

        Args:
            query: Search query string.

        Returns:
            URL string if found, None otherwise.
        """
        ...

    def search_company_cnae(self, query: str) -> str | None:
        """Search for company CNAE code.

        Args:
            query: Search query string.

        Returns:
            CNAE code string if found, None otherwise.
        """
        ...


class HttpClient(Protocol):
    """Protocol for HTTP clients that can check URL validity."""

    def is_url_alive(self, url: str, timeout: float) -> bool:
        """Check if URL is alive (returns acceptable status code).

        Args:
            url: URL to check.
            timeout: Request timeout in seconds.

        Returns:
            True if URL is alive, False otherwise.
        """
        ...


class SimpleSearchClient:
    """Simple search client using Google search (via requests).

    This is a basic implementation. In production, you might want to use
    a proper search API (Google Custom Search, Bing, etc.).
    """

    def __init__(self, max_results: int = 3) -> None:
        """Initialize search client.

        Args:
            max_results: Maximum number of search results to consider.
        """
        self.max_results = max_results

    def search_company_website(self, query: str) -> str | None:
        """Search for company website using simple web scraping.

        Note: This is a placeholder implementation. In production,
        you should use a proper search API.

        Args:
            query: Search query string.

        Returns:
            URL string if found, None otherwise.
        """
        # Placeholder: In production, use Google Custom Search API or similar
        # For now, return None to indicate no result
        logger.debug(f"Searching for website with query: {query}")
        return None

    def search_company_cnae(self, query: str) -> str | None:
        """Search for company CNAE code.

        Args:
            query: Search query string.

        Returns:
            CNAE code string if found, None otherwise.
        """
        # Placeholder: In production, query official registers or APIs
        logger.debug(f"Searching for CNAE with query: {query}")
        return None


class SimpleHttpClient:
    """Simple HTTP client using requests library."""

    def __init__(self, accepted_status_codes: list[int] | None = None) -> None:
        """Initialize HTTP client.

        Args:
            accepted_status_codes: List of accepted HTTP status codes.
                Defaults to [200, 301, 302, 307, 308].
        """
        self.accepted_status_codes = accepted_status_codes or [200, 301, 302, 307, 308]

    def is_url_alive(self, url: str, timeout: float) -> bool:
        """Check if URL is alive (returns acceptable status code).

        Args:
            url: URL to check.
            timeout: Request timeout in seconds.

        Returns:
            True if URL is alive, False otherwise.
        """
        try:
            # Normalize URL
            if not url.startswith(("http://", "https://")):
                url = f"https://{url}"

            # Ensure timeout is maximum 5 seconds
            timeout = min(float(timeout), 5.0)

            response = requests.head(
                url,
                timeout=timeout,
                allow_redirects=True,
                headers={
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
                },
            )
            return response.status_code in self.accepted_status_codes
        except Exception as e:
            logger.debug(f"URL check failed for {url}: {e}")
            return False


class Tier3Enricher:
    """Tier 3 enricher for website and CNAE.

    Solo rellena campos vacíos basándose en reglas de enrichment_rules.yaml.
    """

    def __init__(
        self,
        search_client: SearchClient | None = None,
        http_client: HttpClient | None = None,
        rules: dict[str, Any] | None = None,
    ) -> None:
        """Initialize Tier3 enricher.

        Args:
            search_client: Search client for finding websites/CNAE.
                If None, creates SimpleSearchClient.
            http_client: HTTP client for validating URLs.
                If None, creates SimpleHttpClient.
            rules: Tier3 rules from enrichment_rules.yaml.
                If None, loads from config.
        """
        if rules is None:
            config = load_yaml_config("config/rules/enrichment_rules.yaml")
            rules = config.get("tier3", {})

        self._rules = rules
        self._search_client = search_client or SimpleSearchClient(
            max_results=rules.get("website", {}).get("max_results", 3)
        )
        website_config = rules.get("website", {})
        self._http_client = http_client or SimpleHttpClient(
            accepted_status_codes=website_config.get("accepted_status_codes", [200, 301, 302, 307, 308])
        )

    def _is_empty(self, value: Any) -> bool:
        """Check if value is empty (None, NaN, empty string).

        Args:
            value: Value to check.

        Returns:
            True if value is empty, False otherwise.
        """
        if value is None:
            return True
        if isinstance(value, float) and pd.isna(value):
            return True
        if isinstance(value, str) and not value.strip():
            return True
        return False

    def _normalize_url(self, url: str) -> str:
        """Normalize URL by adding https:// if missing.

        Args:
            url: URL string.

        Returns:
            Normalized URL.
        """
        url = url.strip()
        if not url:
            return url
        if url.startswith(("http://", "https://")):
            return url
        return f"https://{url}"

    def _is_blacklisted_domain(self, url: str) -> bool:
        """Check if URL domain is in blacklist.

        Args:
            url: URL to check.

        Returns:
            True if domain is blacklisted, False otherwise.
        """
        blacklist = self._rules.get("website", {}).get("domains_blacklist", [])
        if not blacklist:
            return False

        try:
            parsed = urlparse(self._normalize_url(url))
            domain = parsed.netloc.lower()
            for blacklisted in blacklist:
                if blacklisted.lower() in domain:
                    return True
        except Exception:
            pass
        return False

    def enrich_website(self, df: pd.DataFrame) -> pd.DataFrame:
        """Intenta completar WEBSITE para filas con WEBSITE vacío.

        Entrada:
            df: DataFrame con al menos columnas RAZON_SOCIAL, CIF, WEBSITE.

        Salida:
            Nuevo DataFrame con WEBSITE rellenado cuando se encuentra
            un dominio válido (status HTTP 200-399).
        """
        df_result = df.copy()

        # Initialize WEBSITE_SOURCE column if not exists
        if "WEBSITE_SOURCE" not in df_result.columns:
            df_result["WEBSITE_SOURCE"] = None

        website_config = self._rules.get("website", {})
        if not website_config.get("enabled", True):
            logger.info("Website enrichment is disabled in config")
            return df_result

        # Filter rows where WEBSITE is empty
        mask_empty = df_result["WEBSITE"].apply(self._is_empty)
        df_empty = df_result[mask_empty].copy()

        if len(df_empty) == 0:
            logger.info("No rows with empty WEBSITE to enrich")
            return df_result

        logger.info(f"Enriching WEBSITE for {len(df_empty)} rows")

        query_template = website_config.get("query_template", "{razon_social} {cif}")
        timeout = min(website_config.get("http_timeout", 3.0), 5.0)  # Maximum 5 seconds

        for idx, row in df_empty.iterrows():
            try:
                # Build search query
                query = query_template.format(
                    razon_social=row.get("RAZON_SOCIAL", ""),
                    cif=row.get("CIF", ""),
                ).strip()

                if not query:
                    continue

                # Search for website
                website_url = self._search_client.search_company_website(query)

                if not website_url:
                    continue

                # Check if blacklisted
                if self._is_blacklisted_domain(website_url):
                    logger.debug(f"Skipping blacklisted domain: {website_url}")
                    continue

                # Validate URL is alive
                if self._http_client.is_url_alive(website_url, timeout):
                    df_result.loc[idx, "WEBSITE"] = self._normalize_url(website_url)
                    df_result.loc[idx, "WEBSITE_SOURCE"] = "search"
                    logger.debug(f"Enriched WEBSITE for row {idx}: {website_url}")
            except Exception as e:
                logger.warning(f"Error enriching WEBSITE for row {idx}: {e}")

        return df_result

    def enrich_cnae(self, df: pd.DataFrame) -> pd.DataFrame:
        """Intenta completar CNAE para filas con CNAE vacío.

        Entrada:
            df: DataFrame con al menos columnas RAZON_SOCIAL, CIF, CNAE.

        Salida:
            Nuevo DataFrame con CNAE rellenado cuando se encuentra
            un código válido desde la fuente configurada.
        """
        df_result = df.copy()

        # Initialize CNAE_SOURCE column if not exists
        if "CNAE_SOURCE" not in df_result.columns:
            df_result["CNAE_SOURCE"] = None

        cnae_config = self._rules.get("cnae", {})
        if not cnae_config.get("enabled", True):
            logger.info("CNAE enrichment is disabled in config")
            return df_result

        # Filter rows where CNAE is empty
        mask_empty = df_result["CNAE"].apply(self._is_empty)
        df_empty = df_result[mask_empty].copy()

        if len(df_empty) == 0:
            logger.info("No rows with empty CNAE to enrich")
            return df_result

        logger.info(f"Enriching CNAE for {len(df_empty)} rows")

        query_template = cnae_config.get("query_template", "{razon_social} {cif} CNAE")

        for idx, row in df_empty.iterrows():
            try:
                # Build search query
                query = query_template.format(
                    razon_social=row.get("RAZON_SOCIAL", ""),
                    cif=row.get("CIF", ""),
                ).strip()

                if not query:
                    continue

                # Search for CNAE
                cnae_code = self._search_client.search_company_cnae(query)

                if not cnae_code:
                    continue

                # Validate CNAE format (basic: should be numeric, 4-5 digits)
                if re.match(r"^\d{4,5}$", str(cnae_code).strip()):
                    df_result.loc[idx, "CNAE"] = str(cnae_code).strip()
                    df_result.loc[idx, "CNAE_SOURCE"] = "search"
                    logger.debug(f"Enriched CNAE for row {idx}: {cnae_code}")
            except Exception as e:
                logger.warning(f"Error enriching CNAE for row {idx}: {e}")

        return df_result

    def process_missing_only(self, df: pd.DataFrame) -> pd.DataFrame:
        """Aplica enriquecimiento Tier3 SOLO a campos vacíos.

        Lógica:
            - Filtrar filas donde WEBSITE está vacío/NaN → aplicar enrich_website.
            - Filtrar filas donde CNAE está vacío/NaN → aplicar enrich_cnae.
            - NO tocar filas que ya tienen WEBSITE/CNAE.
            - NO sobrescribir valores existentes.
            - Añadir columnas de metadata al final si procede, por ejemplo:
              - WEBSITE_SOURCE
              - CNAE_SOURCE

        Entrada:
            df: DataFrame tras Tier2, con columnas WEBSITE y CNAE (si existen).

        Salida:
            DataFrame actualizado con WEBSITE/CNAE rellenados sólo cuando
            estaban vacíos, preservando el resto de datos.
        """
        # Ensure WEBSITE and CNAE columns exist
        if "WEBSITE" not in df.columns:
            df["WEBSITE"] = None
        if "CNAE" not in df.columns:
            df["CNAE"] = None

        # Enrich website
        df = self.enrich_website(df)

        # Enrich CNAE
        df = self.enrich_cnae(df)

        return df
