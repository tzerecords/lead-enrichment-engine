from __future__ import annotations

from datetime import datetime, timezone
from typing import Dict, Any, List

from .base import CIFResult, PhoneResult, PhoneValidation, BatchReport
from .utils.logger import get_logger, log_event
from .validators.cif.borme_validator import BORMECIFValidator
from .validators.cif.regex_validator import RegexCIFValidator
from .validators.phone.libphone_validator import LibPhoneValidator
from .enrichers.phone.google_places import GooglePlacesEnricher
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

        # Load API keys (support both old and new format)
        try:
            api_keys = load_yaml_config("config/api_keys.yaml")
        except FileNotFoundError:
            api_keys = {}

        # Support both formats: new (google_places_key) and old (google_maps.api_key)
        google_places_key = (
            api_keys.get("google_places_key")
            or api_keys.get("google_maps", {}).get("api_key", "")
            or ""
        )

        google_places_limit = int(rate_limits.get("google_places", 200))

        # CIF validators (primary: regex_local, fallback: borme)
        self.cif_primary = RegexCIFValidator()
        self.cif_fallback = BORMECIFValidator()

        # Phone/company enrichers
        self.google_places = GooglePlacesEnricher(
            api_key=google_places_key,
            daily_limit=google_places_limit,
        )
        self.phone_finder_fallback = WebScraperPhoneFinder()
        self.phone_validator = LibPhoneValidator(region="ES")

    @staticmethod
    def _now_iso() -> str:
        return datetime.now(timezone.utc).isoformat()

    def enrich_lead(self, lead: Dict[str, Any]) -> Dict[str, Any]:
        """Enrich a single lead dictionary.

        Expected input keys: CIF, NOMBRE_EMPRESA, CIUDAD, WEBSITE (best effort).
        Returns enriched dict with columns from tier1_config.yaml.
        """

        cif = str(lead.get("CIF", "")).strip()
        company_name = str(lead.get("NOMBRE_EMPRESA", "")).strip()
        city = str(lead.get("CIUDAD", "")).strip() or None
        website = str(lead.get("WEBSITE", "")).strip() or None

        errors: List[str] = []

        # 1) CIF validation waterfall (primary: regex_local, fallback: borme)
        cif_result: CIFResult
        try:
            cif_result = self.cif_primary.validate(cif)
            if not cif_result.valid:
                cif_result = self.cif_fallback.validate(cif)
        except Exception as exc:  # pragma: no cover - defensive
            log_event(self.logger, 40, "CIF validation failed", {"cif": cif, "error": str(exc)})
            cif_result = CIFResult(
                valid=False,
                exists=False,
                razon_social=None,
                source="error",
                estado=None,
                extra={"error": str(exc), "format_ok": False},
            )
            errors.append(f"CIF_ERROR:{exc}")

        # Extract CIF metadata
        cif_format_ok = cif_result.extra.get("format_ok", False) if cif_result.extra else False
        cif_error = cif_result.extra.get("error") if cif_result.extra else None
        if cif_error:
            errors.append(f"CIF:{cif_error}")

        # 2) Company enrichment (Google Places for phone + razón social)
        razon_social = lead.get("RAZON_SOCIAL") or lead.get("NOMBRE_EMPRESA", "")
        razon_social_source = "input"
        phone_result: PhoneResult
        company_data: Dict[str, Any] = {}

        try:
            company_data = self.google_places.find_company(
                company_name=company_name or razon_social or "",
                city=city,
            )

            if company_data.get("error"):
                error_msg = company_data.get("error", "UNKNOWN")
                errors.append(f"GOOGLE_PLACES:{error_msg}")
                # Try fallback web scraper for phone only
                phone_result = self.phone_finder_fallback.find(
                    company_name=company_name or razon_social or "",
                    address=city,
                    website=website,
                )
            else:
                # Use Google Places data
                phone = company_data.get("phone") or company_data.get("international_phone")
                phone_result = PhoneResult(
                    phone=phone,
                    confidence=company_data.get("confidence", 0.0),
                    source=company_data.get("source", "google_places"),
                    extra={"error": None},
                )

                # Update razón social if Google has better data
                google_name = company_data.get("name")
                if google_name and google_name.strip():
                    razon_social = google_name
                    razon_social_source = "google_places"

        except Exception as exc:  # pragma: no cover - defensive
            log_event(self.logger, 40, "Company enrichment failed", {"cif": cif, "error": str(exc)})
            phone_result = PhoneResult(phone=None, confidence=0.0, source="error", extra={"error": str(exc)})
            errors.append(f"ENRICHMENT_ERROR:{exc}")

        # 3) Phone validation
        phone_validation: PhoneValidation
        if phone_result.phone:
            try:
                phone_validation = self.phone_validator.validate(phone_result.phone)
            except Exception as exc:  # pragma: no cover - defensive
                errors.append(f"PHONE_VALIDATION_ERROR:{exc}")
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

        # Build enriched result
        enriched = dict(lead)
        enriched.update(
            {
                "CIF": cif,
                "CIF_VALID": cif_result.valid,
                "CIF_FORMAT_OK": cif_format_ok,
                "RAZON_SOCIAL": razon_social,
                "RAZON_SOCIAL_SOURCE": razon_social_source,
                "PHONE": phone_validation.formatted or phone_result.phone,
                "PHONE_VALID": phone_validation.valid,
                "PHONE_TYPE": phone_validation.type,
                "PHONE_SOURCE": phone_result.source,
                "ENRICHMENT_TIMESTAMP": self._now_iso(),
                "ERRORS": ",".join(errors) if errors else "",
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
            if enriched.get("ERRORS"):
                errors.append(enriched["ERRORS"])

        return BatchReport(
            total=total,
            cif_validated=cif_validated,
            phone_found=phone_found,
            errors=errors,
            provider_calls={},
        )
