from __future__ import annotations

from datetime import datetime, timezone
from typing import Dict, Any, List

from .base import CIFResult, PhoneResult, PhoneValidation, BatchReport
from .utils.logger import get_logger, log_event
from .validators.cif.api_empresas import APIEmpresasCIFValidator
from .validators.cif.borme_validator import BORMECIFValidator
from .validators.cif.regex_validator import RegexCIFValidator
from .validators.phone.libphone_validator import LibPhoneValidator
from .enrichers.phone.google_places import GooglePlacesPhoneFinder
from .enrichers.phone.web_scraper import WebScraperPhoneFinder
from src.utils.config_loader import load_yaml_config


class Tier1Enricher:
    """Main orchestrator for Tier 1 enrichment (CIF, phone, razón social)."""

    def __init__(self, config_path: str = "config/tier1_config.yaml") -> None:
        self.config_path = config_path
        self.logger = get_logger("tier1.orchestrator")
        self.config = load_yaml_config(config_path)
        self._build_providers()

    def _build_providers(self) -> None:
        tier1 = self.config.get("tier1", {})
        rate_limits = tier1.get("rate_limits", {})

        # NOTE: api_keys.yaml is optional; empty keys mean calls will fail fast if used
        try:
            api_keys = load_yaml_config("config/api_keys.yaml")
        except FileNotFoundError:
            api_keys = {}

        api_empresas_key = api_keys.get("api_empresas", {}).get("api_key", "")
        google_maps_key = api_keys.get("google_maps", {}).get("api_key", "")

        api_empresas_limit = int(rate_limits.get("api_empresas", 2000))
        google_places_limit = int(rate_limits.get("google_places", 10000))

        # CIF validators
        self.cif_primary = APIEmpresasCIFValidator(
            api_key=api_empresas_key,
            monthly_limit=api_empresas_limit,
        )
        self.cif_fallback = BORMECIFValidator()
        self.cif_last_resort = RegexCIFValidator()

        # Phone finders / validators
        self.phone_finder = GooglePlacesPhoneFinder(
            api_key=google_maps_key,
            monthly_limit=google_places_limit,
        )
        self.phone_finder_fallback = WebScraperPhoneFinder()
        self.phone_validator = LibPhoneValidator(region="ES")

    @staticmethod
    def _now_iso() -> str:
        return datetime.now(timezone.utc).isoformat()

    def enrich_lead(self, lead: Dict[str, Any]) -> Dict[str, Any]:
        """Enrich a single lead dictionary.

        Expected input keys: CIF, NOMBRE_EMPRESA, CIUDAD, WEBSITE (best effort).
        """

        cif = str(lead.get("CIF", "")).strip()
        company_name = str(lead.get("NOMBRE_EMPRESA", "")).strip()
        city = str(lead.get("CIUDAD", "")).strip() or None
        website = str(lead.get("WEBSITE", "")).strip() or None

        enrichment_errors: List[str] = []

        # 1) CIF validation waterfall
        try:
            cif_result = self.cif_primary.validate(cif)
            if not cif_result.valid and not cif_result.exists:
                cif_result = self.cif_fallback.validate(cif)
            if not cif_result.valid:
                cif_result = self.cif_last_resort.validate(cif)
        except Exception as exc:  # pragma: no cover - defensive
            log_event(self.logger, 40, "CIF validation failed", {"cif": cif, "error": str(exc)})
            cif_result = CIFResult(
                valid=False,
                exists=False,
                razon_social=None,
                source="error",
                estado=None,
                extra={"error": str(exc)},
            )
            enrichment_errors.append(f"cif_error:{exc}")

        # 2) Company name
        razon_social = lead.get("RAZON_SOCIAL") or cif_result.razon_social
        razon_social_source = cif_result.source if cif_result.razon_social else "input"

        # 3) Phone discovery waterfall
        try:
            phone_result = self.phone_finder.find(
                company_name=company_name or (razon_social or ""),
                address=city,
            )
            if not phone_result.phone:
                phone_result = self.phone_finder_fallback.find(
                    company_name=company_name or (razon_social or ""),
                    address=city,
                    website=website,
                )
        except Exception as exc:  # pragma: no cover - defensive
            log_event(self.logger, 40, "Phone discovery failed", {"cif": cif, "error": str(exc)})
            phone_result = PhoneResult(phone=None, confidence=0.0, source="error", extra={"error": str(exc)})
            enrichment_errors.append(f"phone_error:{exc}")

        # 4) Phone validation
        if phone_result.phone:
            try:
                phone_validation = self.phone_validator.validate(phone_result.phone)
            except Exception as exc:  # pragma: no cover - defensive
                enrichment_errors.append(f"phone_validation_error:{exc}")
                phone_validation = PhoneValidation(
                    valid=False,
                    formatted=None,
                    type="UNKNOWN",
                    carrier=None,
                    active=None,
                    extra={"error": str(exc)},
                )
        else:
            phone_validation = PhoneValidation(
                valid=False,
                formatted=None,
                type="UNKNOWN",
                carrier=None,
                active=None,
                extra=None,
            )

        enriched = dict(lead)
        enriched.update(
            {
                "CIF_VALID": cif_result.valid,
                "CIF_EXISTS": cif_result.exists,
                "CIF_STATUS": cif_result.estado or "UNKNOWN",
                "RAZON_SOCIAL": razon_social,
                "RAZON_SOCIAL_SOURCE": razon_social_source,
                "PHONE": phone_validation.formatted or phone_result.phone,
                "PHONE_VALID": phone_validation.valid,
                "PHONE_TYPE": phone_validation.type,
                "PHONE_SOURCE": phone_result.source,
                "ENRICHMENT_TIMESTAMP": self._now_iso(),
                "ENRICHMENT_ERRORS": ",".join(enrichment_errors) if enrichment_errors else "",
            }
        )

        return enriched

    def enrich_batch(self, leads: List[Dict[str, Any]]) -> BatchReport:
        """Enrich a batch of leads in place and return aggregate stats."""

        try:
            from tqdm import tqdm  # type: ignore[import]
            iterator = tqdm(leads, desc="Tier1 enrichment")
        except Exception:  # pragma: no cover - optional dependency
            iterator = leads

        total = len(leads)
        cif_validated = 0
        phone_found = 0
        errors: List[str] = []

        for idx, lead in enumerate(iterator):
            enriched = self.enrich_lead(lead)
            leads[idx] = enriched
            if enriched.get("CIF_VALID"):
                cif_validated += 1
            if enriched.get("PHONE"):
                phone_found += 1
            if enriched.get("ENRICHMENT_ERRORS"):
                errors.append(enriched["ENRICHMENT_ERRORS"])

        return BatchReport(
            total=total,
            cif_validated=cif_validated,
            phone_found=phone_found,
            errors=errors,
            provider_calls={},
        )
