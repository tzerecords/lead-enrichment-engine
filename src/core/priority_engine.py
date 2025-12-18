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

    def _check_services(self, row: pd.Series, required_services: list) -> bool:
        """Check if row has all required services.

        Args:
            row: DataFrame row.
            required_services: List of required service names (e.g., ["LUZ", "GAS"]).

        Returns:
            True if all required services are present and truthy.
        """
        for service in required_services:
            service_col = service.upper()
            if service_col not in row.index:
                return False
            # Check if service value is truthy (not NaN, not empty, not False)
            service_value = row[service_col]
            if pd.isna(service_value) or service_value == "" or service_value == False:
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

        consumo = row.get("CONSUMO_MWH", None)
        if pd.isna(consumo) or consumo == "":
            return False

        try:
            consumo_float = float(consumo)
        except (ValueError, TypeError):
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

        consumo = row.get("CONSUMO_MWH", None)
        if pd.isna(consumo) or consumo == "":
            return False

        try:
            consumo_float = float(consumo)
        except (ValueError, TypeError):
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

        consumo = row.get("CONSUMO_MWH", None)
        if pd.isna(consumo) or consumo == "":
            return False

        try:
            consumo_float = float(consumo)
        except (ValueError, TypeError):
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

