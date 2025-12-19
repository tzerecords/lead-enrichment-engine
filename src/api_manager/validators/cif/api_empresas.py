from __future__ import annotations

from typing import Optional, Dict, Any

import requests

from ...base import CIFResult, CIFValidator, CompanyData
from ...utils.logger import get_logger, log_event
from ...utils.rate_limiter import RateLimiter, ProviderLimit
from ...utils.retry import with_retry
from ....utils.config_loader import load_yaml_config


class APIEmpresasCIFValidator(CIFValidator):
    """CIF validator using APIEmpresas.es.

    This validator can also provide company data (razón social, address, CNAE).
    """

    source_name = "api_empresas"

    def __init__(
        self,
        api_key: str,
        base_url: str = "https://api.apiempresas.es/v1",
        rate_limiter: Optional[RateLimiter] = None,
        monthly_limit: int = 2000,
    ) -> None:
        self.api_key = api_key
        self.base_url = base_url.rstrip("/")
        self.logger = get_logger("tier1.api_empresas")
        self.rate_limiter = rate_limiter or RateLimiter()
        self.limit = ProviderLimit(name=self.source_name, monthly_limit=monthly_limit)

    @with_retry((requests.RequestException,))
    def _request_cif(self, cif: str) -> Dict[str, Any]:
        url = f"{self.base_url}/cif/{cif}"
        headers = {"Authorization": f"Bearer {self.api_key}"}
        response = requests.get(url, headers=headers, timeout=5)
        if response.status_code == 404:
            return {}
        response.raise_for_status()
        return response.json()

    def validate(self, cif: str) -> CIFResult:
        if not self.rate_limiter.check_limit(self.source_name, self.limit):
            log_event(
                self.logger,
                level=30,
                message="APIEmpresas rate limit exceeded, skipping call",
                extra={"cif": cif},
            )
            return CIFResult(
                valid=False,
                exists=False,
                razon_social=None,
                source=self.source_name,
                estado=None,
                extra={"error": "rate_limit_exceeded"},
            )

        normalized = cif.strip().upper()
        data: Dict[str, Any] = {}
        try:
            data = self._request_cif(normalized)
            self.rate_limiter.increment(self.source_name)
        except requests.RequestException as exc:
            log_event(
                self.logger,
                level=40,
                message="APIEmpresas request failed",
                extra={"cif": normalized, "error": str(exc)},
            )
            return CIFResult(
                valid=False,
                exists=False,
                razon_social=None,
                source=self.source_name,
                estado=None,
                extra={"error": str(exc)},
            )

        if not data:
            # CIF does not exist in APIEmpresas
            return CIFResult(
                valid=False,
                exists=False,
                razon_social=None,
                source=self.source_name,
                estado="UNKNOWN",
                extra={"normalized": normalized},
            )

        razon_social = data.get("razon_social") or data.get("nombre")
        estado = data.get("estado", "UNKNOWN")

        return CIFResult(
            valid=True,
            exists=True,
            razon_social=razon_social,
            source=self.source_name,
            estado=estado,
            extra={"raw": data},
        )


class APIEmpresasCompanyEnricher:
    """Company enricher based on APIEmpresas CIF endpoint."""

    source_name = "api_empresas"

    def __init__(
        self,
        api_key: str,
        base_url: str = "https://api.apiempresas.es/v1",
        rate_limiter: Optional[RateLimiter] = None,
        monthly_limit: int = 2000,
    ) -> None:
        self._validator = APIEmpresasCIFValidator(
            api_key=api_key,
            base_url=base_url,
            rate_limiter=rate_limiter,
            monthly_limit=monthly_limit,
        )

    def enrich(self, cif: str) -> CompanyData:
        result = self._validator.validate(cif)
        data = (result.extra or {}).get("raw", {})
        address = data.get("domicilio") or data.get("direccion")
        cnae = data.get("cnae")
        employees = data.get("empleados")
        return CompanyData(
            razon_social=result.razon_social,
            address=address,
            cnae=str(cnae) if cnae is not None else None,
            employees=int(employees) if employees is not None else None,
            source=self.source_name,
            extra={"raw": data},
        )


def load_apiempresas_from_config(config_path: str) -> APIEmpresasCIFValidator:
    """Helper to build validator from YAML config and api_keys.yaml.

    This keeps construction logic out of the orchestrator.
    """

    cfg = load_yaml_config(config_path)
    api_keys = load_yaml_config("config/api_keys.yaml")
    tier1 = cfg.get("tier1", {})
    rate_limits = tier1.get("rate_limits", {})

    monthly_limit = int(rate_limits.get("api_empresas", 2000))
    api_key = api_keys.get("api_empresas", {}).get("api_key", "")

    return APIEmpresasCIFValidator(api_key=api_key, monthly_limit=monthly_limit)
