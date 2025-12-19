from __future__ import annotations

from typing import Optional

import phonenumbers

from ...base import PhoneValidation, PhoneValidator


class LibPhoneValidator(PhoneValidator):
    """Phone validator based on Google's libphonenumber.

    This only checks format and metadata, not real-time line activity.
    """

    source_name = "libphonenumber"

    def __init__(self, region: str = "ES") -> None:
        self.region = region

    def validate(self, phone: str) -> PhoneValidation:
        raw = phone.strip()
        try:
            parsed = phonenumbers.parse(raw, self.region)
        except phonenumbers.NumberParseException:
            return PhoneValidation(
                valid=False,
                formatted=None,
                type="UNKNOWN",
                carrier=None,
                active=None,
                extra={"input": raw},
            )

        valid = phonenumbers.is_valid_number(parsed)
        number_type = phonenumbers.number_type(parsed)
        type_name = phonenumbers.PhoneNumberType._VALUES_TO_NAMES.get(number_type, "UNKNOWN")

        formatted = phonenumbers.format_number(parsed, phonenumbers.PhoneNumberFormat.E164)

        return PhoneValidation(
            valid=valid,
            formatted=formatted,
            type=type_name,
            carrier=None,
            active=None,
            extra={"region": self.region},
        )
