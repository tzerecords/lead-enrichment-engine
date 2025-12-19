from __future__ import annotations

from typing import Optional, Dict, Any

from ...base import CIFResult, CIFValidator
from ...utils.logger import get_logger, log_event


class BORMECIFValidator(CIFValidator):
    """Fallback CIF validator using local/open BORME data.

    For now this is a simple stub that can be wired to a local lookup
    (CSV/XML) in the future. It assumes a separate process has generated
    an index of CIF -> company info.
    """

    source_name = "borme"

    def __init__(self, index: Optional[Dict[str, Dict[str, Any]]] = None) -> None:
        self.logger = get_logger("tier1.borme")
        # index: {CIF: {"razon_social": str, "estado": str}}
        self.index = index or {}

    def validate(self, cif: str) -> CIFResult:
        normalized = cif.strip().upper()
        info = self.index.get(normalized)
        if not info:
            return CIFResult(
                valid=False,
                exists=False,
                razon_social=None,
                source=self.source_name,
                estado="UNKNOWN",
                extra={"normalized": normalized},
            )

        razon_social = info.get("razon_social")
        estado = info.get("estado", "UNKNOWN")
        return CIFResult(
            valid=True,
            exists=True,
            razon_social=razon_social,
            source=self.source_name,
            estado=estado,
            extra={"normalized": normalized},
        )
