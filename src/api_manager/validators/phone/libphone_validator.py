from __future__ import annotations

from typing import Optional

import phonenumbers
from phonenumbers import number_type, PhoneNumberType

from ...base import PhoneValidation, PhoneValidator


# Map PhoneNumberType enum values to string names
PHONE_TYPE_MAP = {
    PhoneNumberType.FIXED_LINE: "FIXED_LINE",
    PhoneNumberType.MOBILE: "MOBILE",
    PhoneNumberType.FIXED_LINE_OR_MOBILE: "FIXED_LINE_OR_MOBILE",
    PhoneNumberType.TOLL_FREE: "TOLL_FREE",
    PhoneNumberType.PREMIUM_RATE: "PREMIUM_RATE",
    PhoneNumberType.SHARED_COST: "SHARED_COST",
    PhoneNumberType.VOIP: "VOIP",
    PhoneNumberType.PERSONAL_NUMBER: "PERSONAL_NUMBER",
    PhoneNumberType.PAGER: "PAGER",
    PhoneNumberType.UAN: "UAN",
    PhoneNumberType.VOICEMAIL: "VOICEMAIL",
}


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
        phone_type_enum = number_type(parsed)
        type_str = PHONE_TYPE_MAP.get(phone_type_enum, "UNKNOWN")

        formatted = phonenumbers.format_number(parsed, phonenumbers.PhoneNumberFormat.E164)

        return PhoneValidation(
            valid=valid,
            formatted=formatted,
            type=type_str,
            carrier=None,
            active=None,
            extra={"region": self.region},
        )
