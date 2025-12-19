"""Comprehensive Spanish fiscal identifier validator (CIF/NIF/NIE).

Handles validation of:
- CIF (companies): 1 letter + 8 characters (e.g., B67217349, G08663478)
- NIF (individuals): 8 digits + 1 letter (e.g., 37277293C, 46664095Q)
- NIE (foreigners): X/Y/Z + 7 digits + 1 letter (e.g., X3263669S, Y4402928Y)
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Optional, Dict, Any

from ..utils.config_loader import load_yaml_config


@dataclass
class CifValidationResult:
    """Result of CIF/NIF/NIE validation."""

    is_valid: bool
    formatted_id: str
    id_type: str  # "CIF", "NIF", "NIE"
    entity_type: Optional[str]  # Only for CIF: "SL", "SA", "Fundación", etc.
    error: Optional[str] = None


# Control letters for MOD-23 algorithm
_CONTROL_LETTERS = "TRWAGMYFPDXBNJZSQVHLCKE"

# CIF entity type mapping
CIF_ENTITY_TYPES: Dict[str, str] = {
    "A": "Sociedad Anónima",
    "B": "Sociedad Limitada",
    "C": "Sociedad Colectiva",
    "D": "Sociedad Comanditaria",
    "E": "Comunidad de Bienes",
    "F": "Sociedad Cooperativa",
    "G": "Asociación/Fundación",
    "H": "Comunidad de Propietarios",
    "J": "Sociedad Civil",
    "N": "Entidad Extranjera",
    "P": "Corporación Local",
    "Q": "Organismo Autónomo",
    "R": "Congregación Religiosa",
    "S": "Órgano de la Administración",
    "U": "Unión Temporal de Empresas",
    "V": "Otros tipos no definidos",
    "W": "Establecimiento permanente",
}


class CifValidator:
    """Validator for Spanish fiscal identifiers (CIF/NIF/NIE)."""

    def __init__(self, config_path: str = "config/rules/validation_rules.yaml") -> None:
        """Initialize validator with config.

        Args:
            config_path: Path to validation rules YAML.
        """
        try:
            self.config = load_yaml_config(config_path)
            cif_config = self.config.get("cif_validation", {})
            self.patterns = cif_config.get("patterns", {})
            self.entity_types = cif_config.get("entity_types", CIF_ENTITY_TYPES)
        except FileNotFoundError:
            # Use defaults if config not found
            self.patterns = {
                "cif": r"^[A-W]\d{8}$",
                "nif": r"^\d{8}[A-Z]$",
                "nie": r"^[XYZ]\d{7}[A-Z]$",
            }
            self.entity_types = CIF_ENTITY_TYPES

    def normalize(self, fiscal_id: str) -> str:
        """Normalize fiscal identifier: remove spaces, uppercase, trim.

        Args:
            fiscal_id: Raw fiscal identifier string.

        Returns:
            Normalized string.
        """
        return fiscal_id.strip().upper().replace(" ", "").replace("-", "")

    def validate_nif(self, nif: str) -> CifValidationResult:
        """Validate NIF (8 digits + 1 letter).

        Args:
            nif: Normalized NIF string.

        Returns:
            CifValidationResult.
        """
        pattern = re.compile(self.patterns.get("nif", r"^\d{8}[A-Z]$"))
        if not pattern.match(nif):
            return CifValidationResult(
                is_valid=False,
                formatted_id=nif,
                id_type="NIF",
                entity_type=None,
                error="INVALID_FORMAT",
            )

        # Extract components
        digits = nif[:8]
        control_letter = nif[8]

        # Calculate MOD-23 check digit
        remainder = int(digits) % 23
        expected_letter = _CONTROL_LETTERS[remainder]

        if control_letter != expected_letter:
            return CifValidationResult(
                is_valid=False,
                formatted_id=nif,
                id_type="NIF",
                entity_type=None,
                error="INVALID_CHECKSUM",
            )

        return CifValidationResult(
            is_valid=True,
            formatted_id=nif,
            id_type="NIF",
            entity_type=None,
        )

    def validate_nie(self, nie: str) -> CifValidationResult:
        """Validate NIE (X/Y/Z + 7 digits + 1 letter).

        Args:
            nie: Normalized NIE string.

        Returns:
            CifValidationResult.
        """
        pattern = re.compile(self.patterns.get("nie", r"^[XYZ]\d{7}[A-Z]$"))
        if not pattern.match(nie):
            return CifValidationResult(
                is_valid=False,
                formatted_id=nie,
                id_type="NIE",
                entity_type=None,
                error="INVALID_FORMAT",
            )

        # Extract components
        prefix = nie[0]  # X, Y, or Z
        digits = nie[1:8]
        control_letter = nie[8]

        # Map prefix to number: X=0, Y=1, Z=2
        prefix_map = {"X": "0", "Y": "1", "Z": "2"}
        full_number = prefix_map[prefix] + digits

        # Calculate MOD-23 check digit (same as NIF)
        remainder = int(full_number) % 23
        expected_letter = _CONTROL_LETTERS[remainder]

        if control_letter != expected_letter:
            return CifValidationResult(
                is_valid=False,
                formatted_id=nie,
                id_type="NIE",
                entity_type=None,
                error="INVALID_CHECKSUM",
            )

        return CifValidationResult(
            is_valid=True,
            formatted_id=nie,
            id_type="NIE",
            entity_type=None,
        )

    def validate_cif(self, cif: str) -> CifValidationResult:
        """Validate CIF (1 letter + 8 characters: 7 digits + 1 control).

        Args:
            cif: Normalized CIF string.

        Returns:
            CifValidationResult.
        """
        pattern = re.compile(self.patterns.get("cif", r"^[A-W]\d{8}$"))
        if not pattern.match(cif):
            return CifValidationResult(
                is_valid=False,
                formatted_id=cif,
                id_type="CIF",
                entity_type=None,
                error="INVALID_FORMAT",
            )

        # Extract components
        organization_type = cif[0]
        number_part = cif[1:8]  # 7 digits
        control_char = cif[8]  # Last character (digit or letter)

        # Get entity type
        entity_type = self.entity_types.get(organization_type, "Unknown")

        # Calculate MOD-23 checksum
        # Sum of digits at odd positions (1-indexed: positions 1, 3, 5, 7)
        sum_a = sum(int(number_part[i]) for i in range(1, 7, 2))

        # Sum of digits at even positions (1-indexed: positions 0, 2, 4, 6)
        # Each digit is doubled, then we sum tens + units
        sum_b = 0
        for i in range(0, 7, 2):
            doubled = int(number_part[i]) * 2
            sum_b += (doubled // 10) + (doubled % 10)

        total = sum_a + sum_b
        unit_digit = total % 10
        control_digit = (10 - unit_digit) % 10

        # Determine expected control character
        if organization_type in "NPQRSW":
            # These types use a letter from CONTROL_LETTERS
            expected_control = _CONTROL_LETTERS[control_digit]
        else:
            # Other types use a digit
            expected_control = str(control_digit)

        # Validate checksum
        if control_char != expected_control:
            return CifValidationResult(
                is_valid=False,
                formatted_id=cif,
                id_type="CIF",
                entity_type=entity_type,
                error="INVALID_CHECKSUM",
            )

        return CifValidationResult(
            is_valid=True,
            formatted_id=cif,
            id_type="CIF",
            entity_type=entity_type,
        )

    def validate(self, fiscal_id: str) -> CifValidationResult:
        """Validate fiscal identifier (auto-detect type: CIF, NIF, or NIE).

        Args:
            fiscal_id: Raw fiscal identifier string.

        Returns:
            CifValidationResult with validation result.
        """
        if not fiscal_id or not fiscal_id.strip():
            return CifValidationResult(
                is_valid=False,
                formatted_id="",
                id_type="UNKNOWN",
                entity_type=None,
                error="EMPTY_INPUT",
            )

        normalized = self.normalize(fiscal_id)

        # Auto-detect type based on format
        if re.match(self.patterns.get("nie", r"^[XYZ]\d{7}[A-Z]$"), normalized):
            return self.validate_nie(normalized)
        elif re.match(self.patterns.get("nif", r"^\d{8}[A-Z]$"), normalized):
            return self.validate_nif(normalized)
        elif re.match(self.patterns.get("cif", r"^[A-W]\d{8}$"), normalized):
            return self.validate_cif(normalized)
        else:
            return CifValidationResult(
                is_valid=False,
                formatted_id=normalized,
                id_type="UNKNOWN",
                entity_type=None,
                error="UNKNOWN_FORMAT",
            )
