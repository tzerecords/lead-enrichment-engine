"""Batch phone validator for M3.

Validates all phones in a DataFrame and adds validation columns.
"""

from __future__ import annotations

from typing import Any

import pandas as pd

from ..tier1.phone_validator import PhoneValidator
from ..utils.logger import setup_logger
from ..utils.config_loader import load_yaml_config

logger = setup_logger()


def validate_all_phones(df: pd.DataFrame, rules: dict[str, Any] | None = None) -> pd.DataFrame:
    """Valida todos los teléfonos del DataFrame.

    Usa:
        - Reglas de formato/longitud/prefijos desde validation_rules.yaml.

    Modifica:
        - PHONE_VALID: bool
        - PHONE_REASON: texto corto (ej. 'empty', 'invalid_length', 'invalid_prefix', 'ok')
        - PHONE_NORMALIZED: string con formato normalizado

    Args:
        df: Input DataFrame with TELEFONO column.
        rules: Phone validation rules from validation_rules.yaml.
            If None, loads from config.

    Returns:
        DataFrame with PHONE_VALID, PHONE_REASON, PHONE_NORMALIZED columns added.
    """
    if rules is None:
        config = load_yaml_config("config/rules/validation_rules.yaml")
        rules = config.get("phone", {})

    df_result = df.copy()

    # Initialize validation columns
    if "PHONE_VALID" not in df_result.columns:
        df_result["PHONE_VALID"] = False
    if "PHONE_REASON" not in df_result.columns:
        df_result["PHONE_REASON"] = ""
    if "PHONE_NORMALIZED" not in df_result.columns:
        df_result["PHONE_NORMALIZED"] = ""

    # Get phone column name
    phone_column = rules.get("column", "TELEFONO")

    if phone_column not in df_result.columns:
        logger.warning(f"Phone column '{phone_column}' not found in DataFrame")
        return df_result

    # Initialize validator
    validator = PhoneValidator()

    logger.info(f"Validating phones for {len(df_result)} rows")

    # Validate each phone
    for idx, row in df_result.iterrows():
        phone = row.get(phone_column)

        # Check if empty
        if pd.isna(phone) or (isinstance(phone, str) and not str(phone).strip()):
            df_result.loc[idx, "PHONE_VALID"] = False
            df_result.loc[idx, "PHONE_REASON"] = "empty"
            df_result.loc[idx, "PHONE_NORMALIZED"] = ""
            continue

        # Validate phone
        result = validator.validate(str(phone))

        if result.is_valid:
            df_result.loc[idx, "PHONE_VALID"] = True
            df_result.loc[idx, "PHONE_REASON"] = "ok"
            df_result.loc[idx, "PHONE_NORMALIZED"] = result.international_format
        else:
            df_result.loc[idx, "PHONE_VALID"] = False
            df_result.loc[idx, "PHONE_REASON"] = result.error or "invalid"
            df_result.loc[idx, "PHONE_NORMALIZED"] = result.formatted_phone

    logger.info(f"Phone validation complete")
    return df_result
