from __future__ import annotations

from ...base import CIFResult, CIFValidator


class FiscoCheckValidator(CIFValidator):
    """Stub for future FiscoCheck CIF validator.

    To enable:
      1. Install FiscoCheck client.
      2. Add API key to config/api_keys.yaml.
      3. Wire provider in config/tier1_config.yaml.
    """

    source_name = "fiscocheck"

    def __init__(self, api_key: str) -> None:
        self.api_key = api_key

    def validate(self, cif: str) -> CIFResult:  # pragma: no cover - stub
        raise NotImplementedError("FiscoCheck integration pending")
