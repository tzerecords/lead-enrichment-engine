"""Priority calculation engine based on YAML rules."""

import logging
from typing import Dict, Any, Optional
import pandas as pd

from src.utils.config_loader import load_priority_rules

logger = logging.getLogger("lead_enrichment")


class PriorityEngine:
    """Calculate lead priority based on consumption and services."""

    def __init__(self) -> None:
        """Initialize priority engine and load rules."""
        self.rules = load_priority_rules()
        logger.info("Priority engine initialized")

    def _get_consumo(self, row: pd.Series) -> Optional[float]:
        """Get consumption value from row, trying multiple column names.

        Tries CONSUMO_MWH (ideal) and falls back to CONSUMO (real Excel column).

        Args:
            row: DataFrame row.

        Returns:
            Consumption value as float, or None if missing/invalid.
        """
        raw = row.get("CONSUMO_MWH")
        if raw is None or (isinstance(raw, float) and pd.isna(raw)) or raw == "":
            raw = row.get("CONSUMO")

        if raw is None or raw == "" or (isinstance(raw, float) and pd.isna(raw)):
            return None

        try:
            return float(raw)
        except (ValueError, TypeError):
            return None

    def _get_service_value(self, row: pd.Series, service: str) -> bool:
        """Check if a given service (LUZ/GAS) is present in the row.

        Tries dedicated columns (LUZ/GAS) and falls back to 'L/V' combined column.

        Args:
            row: DataFrame row.
            service: Service name (e.g., "LUZ", "GAS").

        Returns:
            True if service appears to be present.
        """
        service = service.upper()

        # 1) Direct column (LUZ / GAS)
        if service in row.index:
            val = row[service]
            return not (pd.isna(val) or val == "" or val is False)

        # 2) Fallback: combined 'L/V' column from Alejandro's Excel
        if "L/V" in row.index:
            lv_raw = row["L/V"]
            if pd.isna(lv_raw):
                return False
            lv = str(lv_raw).upper()

            if service == "LUZ":
                # Consider L or LUZ as indicating electricity service
                return "L" in lv or "LUZ" in lv
            if service == "GAS":
                # Consider G/GAS/V as indicating gas/other combustible service
                return "G" in lv or "GAS" in lv or "V" in lv

        return False

    def _check_services(self, row: pd.Series, required_services: list) -> bool:
        """Check if row has all required services.

        Args:
            row: DataFrame row.
            required_services: List of required service names (e.g., ["LUZ", "GAS"]).

        Returns:
            True if all required services are present and truthy.
        """
        for service in required_services:
            if not self._get_service_value(row, service):
                return False
        return True

    def _check_priority_4(self, row: pd.Series) -> bool:
        """Check if row matches Priority 4 criteria.

        Args:
            row: DataFrame row.

        Returns:
            True if matches Priority 4.
        """
        rule = self.rules.get("priority_4", {}).get("conditions", {})
        consumo_min = rule.get("consumo_min", float("inf"))
        required_services = rule.get("requires_services", [])

        consumo_float = self._get_consumo(row)
        if consumo_float is None:
            return False

        if consumo_float < consumo_min:
            return False

        if required_services and not self._check_services(row, required_services):
            return False

        return True

    def _check_priority_3(self, row: pd.Series) -> bool:
        """Check if row matches Priority 3 criteria.

        Args:
            row: DataFrame row.

        Returns:
            True if matches Priority 3.
        """
        rule = self.rules.get("priority_3", {}).get("conditions", [])
        if not isinstance(rule, list):
            rule = [rule]

        consumo_float = self._get_consumo(row)
        if consumo_float is None:
            return False

        for condition in rule:
            consumo_min = condition.get("consumo_min", float("inf"))
            required_services = condition.get("requires_services", [])

            if consumo_float < consumo_min:
                continue

            if required_services:
                if not self._check_services(row, required_services):
                    continue

            return True

        return False

    def _check_priority_2(self, row: pd.Series) -> bool:
        """Check if row matches Priority 2 criteria.

        Args:
            row: DataFrame row.

        Returns:
            True if matches Priority 2.
        """
        rule = self.rules.get("priority_2", {}).get("conditions", {})
        consumo_min = rule.get("consumo_min", 70)
        consumo_max = rule.get("consumo_max", 99)

        consumo_float = self._get_consumo(row)
        if consumo_float is None:
            return False

        return consumo_min <= consumo_float <= consumo_max

    def calculate_priority(self, row: pd.Series) -> int:
        """Calculate priority for a single row.

        Args:
            row: DataFrame row with lead data.

        Returns:
            Priority value (1-4, where 4 is highest).
        """
        # Check in descending order (4 → 3 → 2 → 1)
        if self._check_priority_4(row):
            return 4
        if self._check_priority_3(row):
            return 3
        if self._check_priority_2(row):
            return 2
        # Default to Priority 1
        return 1

    def calculate_priorities(self, df: pd.DataFrame) -> pd.Series:
        """Calculate priorities for entire DataFrame.

        Args:
            df: DataFrame with lead data.

        Returns:
            Series with priority values.
        """
        logger.info(f"Calculating priorities for {len(df)} rows")
        priorities = df.apply(self.calculate_priority, axis=1)
        logger.info(f"Priority distribution: {priorities.value_counts().to_dict()}")
        return priorities

