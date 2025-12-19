"""Spanish phone number validator and formatter."""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Optional

from ..utils.config_loader import load_yaml_config


@dataclass
class PhoneValidationResult:
    """Result of phone validation."""

    is_valid: bool
    formatted_phone: str
    phone_type: str  # "mobile", "landline", "special", "invalid"
    international_format: str  # "+34 XXX XXX XXX"
    error: Optional[str] = None


class PhoneValidator:
    """Validator for Spanish phone numbers."""

    def __init__(self, config_path: str = "config/rules/validation_rules.yaml") -> None:
        """Initialize validator with config.

        Args:
            config_path: Path to validation rules YAML.
        """
        try:
            self.config = load_yaml_config(config_path)
            phone_config = self.config.get("phone_validation", {}).get("spain", {})
            self.mobile_prefixes = phone_config.get("mobile_prefixes", ["6", "7"])
            self.landline_prefixes = phone_config.get("landline_prefixes", ["8", "9"])
            self.special_prefixes = phone_config.get("special_prefixes", ["800", "900", "901", "902", "905"])
            self.length = phone_config.get("length", 9)
            self.international_prefix = phone_config.get("international_prefix", "+34")
        except FileNotFoundError:
            # Use defaults
            self.mobile_prefixes = ["6", "7"]
            self.landline_prefixes = ["8", "9"]
            self.special_prefixes = ["800", "900", "901", "902", "905"]
            self.length = 9
            self.international_prefix = "+34"

    def normalize(self, phone: str) -> str:
        """Normalize phone: remove spaces, parentheses, dashes, dots.

        Args:
            phone: Raw phone string.

        Returns:
            Normalized string (digits only, may include +34 prefix).
        """
        # Remove common separators
        normalized = re.sub(r"[\s\-\(\)\.]", "", phone.strip())
        
        # Remove +34 prefix if present at start
        if normalized.startswith("+34"):
            normalized = normalized[3:]
        elif normalized.startswith("0034"):
            normalized = normalized[4:]
        elif normalized.startswith("34") and len(normalized) == 11:  # 34 + 9 dígitos
            normalized = normalized[2:]
        
        return normalized

    def detect_type(self, digits: str) -> str:
        """Detect phone type from normalized digits.

        Args:
            digits: 9-digit phone number (without prefix).

        Returns:
            Phone type: "mobile", "landline", "special", or "invalid".
        """
        if len(digits) != self.length:
            return "invalid"

        first_digit = digits[0]
        first_three = digits[:3]

        # Check special numbers (800, 900, etc.)
        if first_three in self.special_prefixes:
            return "special"

        # Check mobile (starts with 6 or 7)
        if first_digit in self.mobile_prefixes:
            return "mobile"

        # Check landline (starts with 8 or 9)
        if first_digit in self.landline_prefixes:
            return "landline"

        return "invalid"

    def format_international(self, digits: str) -> str:
        """Format phone number in international format (+34 XXX XXX XXX).

        Args:
            digits: 9-digit phone number.

        Returns:
            Formatted string: "+34 XXX XXX XXX".
        """
        if len(digits) != 9:
            return f"{self.international_prefix} {digits}"

        # Format: +34 XXX XXX XXX (3-3-3 grouping)
        return f"{self.international_prefix} {digits[:3]} {digits[3:6]} {digits[6:]}"

    def validate(self, phone: str) -> PhoneValidationResult:
        """Validate and format Spanish phone number.

        Args:
            phone: Raw phone string (various formats supported).

        Returns:
            PhoneValidationResult with validation result.
        """
        if not phone or not phone.strip():
            return PhoneValidationResult(
                is_valid=False,
                formatted_phone="",
                phone_type="invalid",
                international_format="",
                error="EMPTY_INPUT",
            )

        # Normalize
        normalized = self.normalize(phone)

        # Check if it's all digits
        if not normalized.isdigit():
            return PhoneValidationResult(
                is_valid=False,
                formatted_phone=normalized,
                phone_type="invalid",
                international_format="",
                error="INVALID_CHARACTERS",
            )

        # Check length (should be 9 digits)
        if len(normalized) != self.length:
            return PhoneValidationResult(
                is_valid=False,
                formatted_phone=normalized,
                phone_type="invalid",
                international_format="",
                error=f"INVALID_LENGTH (expected {self.length}, got {len(normalized)})",
            )

        # Detect type
        phone_type = self.detect_type(normalized)

        if phone_type == "invalid":
            return PhoneValidationResult(
                is_valid=False,
                formatted_phone=normalized,
                phone_type="invalid",
                international_format="",
                error="INVALID_PREFIX",
            )

        # Format
        international = self.format_international(normalized)

        return PhoneValidationResult(
            is_valid=True,
            formatted_phone=normalized,
            phone_type=phone_type,
            international_format=international,
        )
