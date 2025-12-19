from __future__ import annotations

from ...base import PhoneValidation, PhoneValidator


class TwilioLookupPhoneValidator(PhoneValidator):
    """Stub for future Twilio Lookup (HLR) validator.

    To enable:
      1. Install Twilio client library.
      2. Add API keys to config/api_keys.yaml.
      3. Wire provider in config/tier1_config.yaml.
    """

    source_name = "twilio"

    def __init__(self, account_sid: str, auth_token: str) -> None:
        self.account_sid = account_sid
        self.auth_token = auth_token

    def validate(self, phone: str) -> PhoneValidation:  # pragma: no cover - stub
        raise NotImplementedError("Twilio Lookup integration pending")
