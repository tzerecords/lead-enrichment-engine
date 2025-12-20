"""Batch CIF revalidator for M3.

Revalidates CIFs that failed in Tier1.
"""

from __future__ import annotations

from typing import Any

import pandas as pd

from ..tier1.cif_validator import CifValidator
from ..utils.logger import setup_logger
from ..utils.config_loader import load_yaml_config

logger = setup_logger()


def revalidate_cifs(df: pd.DataFrame, rules: dict[str, Any] | None = None) -> pd.DataFrame:
    """Revalida CIFs que fallaron en Tier1.

    Lógica:
        - Seleccionar filas donde CIF_VALID es False o NaN.
        - Aplicar lógica de validación (posiblemente más estricta/relajada según YAML).
        - Actualizar CIF_VALID, CIF_REASON.
        - Añadir flag CIF_RECHECKED = True donde se ha revalidado.

    Args:
        df: Input DataFrame with CIF column and optionally CIF_VALID column.
        rules: CIF validation rules from validation_rules.yaml.
            If None, loads from config.

    Returns:
        DataFrame with CIF_VALID, CIF_REASON, CIF_RECHECKED columns updated/added.
    """
    if rules is None:
        config = load_yaml_config("config/rules/validation_rules.yaml")
        rules = config.get("cif", {})

    df_result = df.copy()

    # Initialize validation columns if not exist
    if "CIF_VALID" not in df_result.columns:
        df_result["CIF_VALID"] = None
    if "CIF_REASON" not in df_result.columns:
        df_result["CIF_REASON"] = ""
    if "CIF_RECHECKED" not in df_result.columns:
        df_result["CIF_RECHECKED"] = False

    # Get CIF column name
    cif_column = rules.get("column", "CIF")

    if cif_column not in df_result.columns:
        logger.warning(f"CIF column '{cif_column}' not found in DataFrame")
        return df_result

    # Get revalidation config
    revalidation_config = rules.get("revalidation", {})
    if not revalidation_config.get("enabled", True):
        logger.info("CIF revalidation is disabled in config")
        return df_result

    # Initialize validator
    validator = CifValidator()

    # Find rows where CIF_VALID is False or NaN
    mask_to_revalidate = (
        (df_result["CIF_VALID"] == False) | (df_result["CIF_VALID"].isna())
    ) & (df_result[cif_column].notna()) & (df_result[cif_column] != "")

    df_to_revalidate = df_result[mask_to_revalidate].copy()

    if len(df_to_revalidate) == 0:
        logger.info("No CIFs to revalidate")
        return df_result

    logger.info(f"Revalidating CIFs for {len(df_to_revalidate)} rows")

    # Revalidate each CIF
    for idx, row in df_to_revalidate.iterrows():
        cif_value = str(row.get(cif_column, "")).strip()

        if not cif_value:
            continue

        # Revalidate
        result = validator.validate(cif_value)

        # Update validation columns
        df_result.loc[idx, "CIF_VALID"] = result.is_valid
        if result.is_valid:
            df_result.loc[idx, "CIF_REASON"] = "ok"
        else:
            df_result.loc[idx, "CIF_REASON"] = result.error or "invalid"
        df_result.loc[idx, "CIF_RECHECKED"] = True

    logger.info(f"CIF revalidation complete")
    return df_result
