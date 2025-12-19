from __future__ import annotations

import re
from typing import Optional

from ...base import CIFResult, CIFValidator


_CIF_REGEX = re.compile(r"^[A-HJNP-SUVW]\d{7}[0-9A-J]$")


class RegexCIFValidator(CIFValidator):
    """Last-resort CIF validator based only on format/checksum.

    This does not check existence in official registries, only that the
    structure looks like a valid Spanish CIF.
    """

    source_name = "regex"

    def validate(self, cif: str) -> CIFResult:
        normalized = cif.strip().upper()
        valid = bool(_CIF_REGEX.match(normalized))
        return CIFResult(
            valid=valid,
            exists=False,
            razon_social=None,
            source=self.source_name,
            estado=None,
            extra={"normalized": normalized},
        )
