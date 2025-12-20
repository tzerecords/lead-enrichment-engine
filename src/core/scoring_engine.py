"""Scoring engine for data quality assessment.

Calculates completeness, confidence, and quality scores for leads.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import pandas as pd

from ..utils.logger import setup_logger
from ..utils.config_loader import load_yaml_config

logger = setup_logger()


@dataclass
class ScoringConfig:
    """Configuration for scoring engine."""

    completeness_fields: dict[str, float]
    completeness_min_high: float
    completeness_min_medium: float
    confidence_sources: dict[str, float]
    confidence_min_high: float
    confidence_min_medium: float
    quality_high: dict[str, float]
    quality_medium: dict[str, float]


class ScoringEngine:
    """Calcula scores de completitud, confianza y calidad de datos."""

    def __init__(self, validation_rules: dict[str, Any] | None = None) -> None:
        """Initialize scoring engine.

        Args:
            validation_rules: Validation rules from validation_rules.yaml.
                If None, loads from config.
        """
        if validation_rules is None:
            validation_rules = load_yaml_config("config/rules/validation_rules.yaml")

        self._config = self._parse_config(validation_rules)

    def _parse_config(self, rules: dict[str, Any]) -> ScoringConfig:
        """Parsea validation_rules.yaml a ScoringConfig.

        Args:
            rules: Validation rules dictionary.

        Returns:
            ScoringConfig object.
        """
        scoring = rules.get("scoring", {})

        completeness = scoring.get("completeness", {})
        confidence = scoring.get("confidence", {})
        quality = scoring.get("quality", {})

        return ScoringConfig(
            completeness_fields=completeness.get("fields", {}),
            completeness_min_high=completeness.get("min_percent_for_high", 80),
            completeness_min_medium=completeness.get("min_percent_for_medium", 50),
            confidence_sources=confidence.get("sources", {}),
            confidence_min_high=confidence.get("min_for_high", 70),
            confidence_min_medium=confidence.get("min_for_medium", 40),
            quality_high=quality.get("high", {}),
            quality_medium=quality.get("medium", {}),
        )

    def _is_empty(self, value: Any) -> bool:
        """Check if value is empty.

        Args:
            value: Value to check.

        Returns:
            True if empty, False otherwise.
        """
        if value is None:
            return True
        if isinstance(value, float) and pd.isna(value):
            return True
        if isinstance(value, str) and not value.strip():
            return True
        return False

    def _is_valid(self, row: pd.Series, field: str) -> bool:
        """Check if field is valid (not empty and validation flag is True if exists).

        Args:
            row: Row Series.
            field: Field name.

        Returns:
            True if field is valid, False otherwise.
        """
        # Check if field is empty
        value = row.get(field)
        if self._is_empty(value):
            return False

        # Check validation flag if exists (e.g., EMAIL_VALID, PHONE_VALID, CIF_VALID)
        validation_flag = f"{field}_VALID"
        if validation_flag in row.index:
            valid_flag = row[validation_flag]
            # Handle boolean, string "True"/"False", or None
            if isinstance(valid_flag, bool):
                return valid_flag
            if isinstance(valid_flag, str):
                return valid_flag.lower() in ("true", "1", "yes")
            if valid_flag is None or pd.isna(valid_flag):
                return True  # If flag doesn't exist, assume valid if not empty

        return True

    def calculate_completeness(self, row: pd.Series) -> float:
        """Devuelve un porcentaje 0-100 de completitud.

        Usa:
            - Campos definidos en YAML con pesos.
            - Solo suma si el campo no está vacío y (si existe) su flag *_VALID es True.

        Args:
            row: Row Series.

        Returns:
            Completeness score (0-100).
        """
        total_weight = 0.0
        filled_weight = 0.0

        for field, weight in self._config.completeness_fields.items():
            total_weight += weight
            if self._is_valid(row, field):
                filled_weight += weight

        if total_weight == 0:
            return 0.0

        score = (filled_weight / total_weight) * 100.0
        return round(score, 2)

    def calculate_confidence(self, row: pd.Series) -> float:
        """Devuelve un score 0-100 de confianza.

        Usa:
            - Columnas de fuentes (ej. EMAIL_SOURCE, WEBSITE_SOURCE, CNAE_SOURCE).
            - Pesos de cada fuente desde YAML.
            - Penaliza datos inválidos.

        Args:
            row: Row Series.

        Returns:
            Confidence score (0-100).
        """
        total_weight = 0.0
        confidence_weight = 0.0

        # Check email confidence
        email = row.get("EMAIL")
        if not self._is_empty(email):
            email_valid = row.get("EMAIL_VALID", False)
            if email_valid:
                email_level = row.get("EMAIL_VALIDATION_LEVEL", "syntax")
                if email_level == "mx":
                    source_key = "email_mx"
                else:
                    source_key = "email_syntax_only"
                weight = self._config.confidence_sources.get(source_key, 0)
                total_weight += weight
                confidence_weight += weight
            # Penalize invalid emails
            elif email_valid is False:
                # Small penalty for invalid email
                total_weight += self._config.confidence_sources.get("email_syntax_only", 0)

        # Check phone confidence
        phone = row.get("TELEFONO")
        if not self._is_empty(phone):
            phone_valid = row.get("PHONE_VALID", False)
            if phone_valid:
                weight = self._config.confidence_sources.get("phone_normalized", 0)
                total_weight += weight
                confidence_weight += weight

        # Check website confidence
        website = row.get("WEBSITE")
        if not self._is_empty(website):
            website_source = row.get("WEBSITE_SOURCE", "")
            if website_source:
                weight = self._config.confidence_sources.get("website_validated", 0)
                total_weight += weight
                confidence_weight += weight

        # Check CNAE confidence
        cnae = row.get("CNAE")
        if not self._is_empty(cnae):
            cnae_source = row.get("CNAE_SOURCE", "")
            if cnae_source == "official_register" or "chamber" in str(cnae_source).lower():
                weight = self._config.confidence_sources.get("cnae_official_register", 0)
            else:
                weight = self._config.confidence_sources.get("cnae_inferred", 0)
            total_weight += weight
            confidence_weight += weight

        if total_weight == 0:
            return 0.0

        score = (confidence_weight / total_weight) * 100.0 if total_weight > 0 else 0.0
        return round(score, 2)

    def assign_data_quality(self, completeness: float, confidence: float, row: pd.Series) -> str:
        """Devuelve High / Medium / Low según utilidad de datos (menos agresivo).

        Args:
            completeness: Completeness score (0-100).
            confidence: Confidence score (0-100).
            row: Row Series to check for useful data.

        Returns:
            Quality level: "High", "Medium", or "Low".
        """
        # High: tiene datos realmente útiles
        email_specific = str(row.get("EMAIL_SPECIFIC", "") or "").strip()
        has_real_email = email_specific and email_specific not in ["", "NO_EMAIL_FOUND", "NOT_FOUND"]
        
        website = str(row.get("WEBSITE", "") or "").strip()
        has_real_website = website and website not in ["", "NOT_FOUND", "NO_WEBSITE_FOUND"]
        
        cnae = str(row.get("CNAE", "") or "").strip()
        has_real_cnae = cnae and cnae not in ["", "NOT_FOUND"]
        
        phone_valid = row.get("PHONE_VALID", False)
        razon_social = str(row.get("RAZON_SOCIAL", "") or "").strip()
        has_razon_social = razon_social and razon_social not in ["", "NOT_FOUND"]
        
        # High criteria: tiene EMAIL_SPECIFIC real OR (WEBSITE real y CNAE real) OR (PHONE_VALID True y RAZON_SOCIAL no vacío)
        if has_real_email:
            return "High"
        if has_real_website and has_real_cnae:
            return "High"
        if phone_valid and has_razon_social:
            return "High"
        
        # Medium: cumple al menos 2 de:
        # - CIF_FORMAT_OK True
        # - PHONE_VALID True
        # - EMAIL original existe y parece válido (contiene "@")
        # - RAZON_SOCIAL no vacío
        criteria_count = 0
        
        cif_format_ok = row.get("CIF_FORMAT_OK", False)
        if cif_format_ok:
            criteria_count += 1
        
        if phone_valid:
            criteria_count += 1
        
        email_original = str(row.get("EMAIL", "") or "").strip()
        if email_original and "@" in email_original:
            criteria_count += 1
        
        if has_razon_social:
            criteria_count += 1
        
        if criteria_count >= 2:
            return "Medium"
        
        # Low: resto
        return "Low"

    def _build_sources_summary(self, row: pd.Series) -> str:
        """Devuelve una string tipo 'email:mx; website:search; cnae:chamber_of_commerce'.

        Args:
            row: Row Series.

        Returns:
            Sources summary string.
        """
        sources = []

        # Email source
        email = row.get("EMAIL")
        if not self._is_empty(email):
            email_level = row.get("EMAIL_VALIDATION_LEVEL", "")
            if email_level:
                sources.append(f"email:{email_level}")

        # Phone source
        phone = row.get("TELEFONO")
        if not self._is_empty(phone):
            phone_valid = row.get("PHONE_VALID", False)
            if phone_valid:
                sources.append("phone:normalized")

        # Website source
        website_source = row.get("WEBSITE_SOURCE")
        if website_source:
            sources.append(f"website:{website_source}")

        # CNAE source
        cnae_source = row.get("CNAE_SOURCE")
        if cnae_source:
            sources.append(f"cnae:{cnae_source}")

        return "; ".join(sources) if sources else ""

    def annotate_row(self, row: pd.Series) -> pd.Series:
        """Calcula y añade columnas de scoring a una fila.

        Args:
            row: Row Series.

        Returns:
            Row Series with scoring columns added.
        """
        completeness = self.calculate_completeness(row)
        confidence = self.calculate_confidence(row)
        quality = self.assign_data_quality(completeness, confidence, row)
        sources = self._build_sources_summary(row)

        # Crear una copia para evitar SettingWithCopyWarning
        result = row.copy()
        result["COMPLETITUD_SCORE"] = completeness
        result["CONFIDENCE_SCORE"] = confidence
        result["DATA_QUALITY"] = quality
        result["LAST_UPDATED"] = datetime.now(timezone.utc).isoformat()
        result["DATA_SOURCES"] = sources
        return result

    def annotate_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """Aplica annotate_row a todo el DataFrame.

        Args:
            df: Input DataFrame.

        Returns:
            DataFrame with scoring columns added.
        """
        # Apply scoring to each row
        df_result = df.copy()

        # Initialize scoring columns
        df_result["COMPLETITUD_SCORE"] = 0.0
        df_result["CONFIDENCE_SCORE"] = 0.0
        df_result["DATA_QUALITY"] = "Low"
        df_result["LAST_UPDATED"] = ""
        df_result["DATA_SOURCES"] = ""

        # Apply scoring row by row
        for idx in df_result.index:
            row = df_result.loc[idx]
            annotated_row = self.annotate_row(row)
            df_result.loc[idx] = annotated_row

        return df_result
