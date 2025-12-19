from __future__ import annotations

import re
from typing import Optional, Dict, Any

from ...base import CIFResult, CIFValidator


# Spanish CIF format: [A-HJNP-SUVW][0-9]{7}[0-9A-J]
_CIF_REGEX = re.compile(r"^[A-HJNP-SUVW]\d{7}[0-9A-J]$")

# Control letters for organization types NPQRSW
_CONTROL_LETTERS = "JABCDEFGHI"


class RegexCIFValidator(CIFValidator):
    """CIF validator using MOD-23 checksum algorithm (official Spanish standard).

    This validator checks both format and checksum according to the official
    Spanish CIF validation algorithm. It does not check existence in official
    registries, only that the structure and checksum are mathematically valid.
    """

    source_name = "regex_local"

    def validate(self, cif: str) -> CIFResult:
        """Validate CIF using MOD-23 checksum algorithm.

        Returns CIFResult with:
        - valid: True if format AND checksum are valid
        - extra["format_ok"]: True if format matches regex
        - extra["organization_type"]: First letter of CIF (A-W)
        - extra["error"]: Error code if invalid (INVALID_FORMAT, INVALID_CHECKSUM)
        """
        normalized = cif.strip().upper()
        
        # Step 1: Check format
        if not _CIF_REGEX.match(normalized):
            return CIFResult(
                valid=False,
                exists=False,
                razon_social=None,
                source=self.source_name,
                estado=None,
                extra={
                    "normalized": normalized,
                    "format_ok": False,
                    "organization_type": None,
                    "error": "INVALID_FORMAT",
                },
            )

        # Step 2: Extract components
        organization_type = normalized[0]
        number_part = normalized[1:8]  # 7 digits
        control_char = normalized[8]  # Last character (digit or letter)

        # Step 3: Calculate MOD-23 checksum
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

        # Step 4: Determine expected control character
        if organization_type in "NPQRSW":
            # These types use a letter
            expected_control = _CONTROL_LETTERS[control_digit]
        else:
            # Other types use a digit
            expected_control = str(control_digit)

        # Step 5: Validate checksum
        if control_char != expected_control:
            return CIFResult(
                valid=False,
                exists=False,
                razon_social=None,
                source=self.source_name,
                estado=None,
                extra={
                    "normalized": normalized,
                    "format_ok": True,
                    "organization_type": organization_type,
                    "error": "INVALID_CHECKSUM",
                    "expected_control": expected_control,
                    "actual_control": control_char,
                },
            )

        # Valid CIF
        return CIFResult(
            valid=True,
            exists=False,  # We don't check existence, only format/checksum
            razon_social=None,
            source=self.source_name,
            estado=None,
            extra={
                "normalized": normalized,
                "format_ok": True,
                "organization_type": organization_type,
                "error": None,
            },
        )
