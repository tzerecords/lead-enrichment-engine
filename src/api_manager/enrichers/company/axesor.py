from __future__ import annotations

from ...base import CompanyData


class AxesorCompanyEnricher:
    """Stub for future Axesor company enricher.

    To enable:
      1. Add Axesor API client dependency.
      2. Add API key/config to config/api_keys.yaml.
      3. Wire provider in config/tier1_config.yaml.
    """

    source_name = "axesor"

    def __init__(self, api_key: str) -> None:
        self.api_key = api_key

    def enrich(self, cif: str) -> CompanyData:  # pragma: no cover - stub
        raise NotImplementedError("Axesor integration pending")
