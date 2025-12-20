"""Batch email validator for M3.

Validates all emails in a DataFrame and adds validation columns.
"""

from __future__ import annotations

from typing import Any

import pandas as pd

from .email_validator import EmailValidator
from ..utils.logger import setup_logger
from ..utils.config_loader import load_yaml_config

logger = setup_logger()


def validate_all_emails(df: pd.DataFrame, rules: dict[str, Any] | None = None) -> pd.DataFrame:
    """Valida todas las direcciones de email del DataFrame.

    Usa:
        - Regex de sintaxis desde validation_rules.yaml.
        - Config MX (activar/desactivar, timeout, retries) desde YAML.

    Modifica:
        - EMAIL_VALID: bool
        - EMAIL_REASON: texto corto (ej. 'empty', 'invalid_syntax', 'no_mx', 'ok')
        - EMAIL_VALIDATION_LEVEL: 'none' | 'syntax' | 'mx'

    Args:
        df: Input DataFrame with EMAIL column.
        rules: Email validation rules from validation_rules.yaml.
            If None, loads from config.

    Returns:
        DataFrame with EMAIL_VALID, EMAIL_REASON, EMAIL_VALIDATION_LEVEL columns added.
    """
    if rules is None:
        config = load_yaml_config("config/rules/validation_rules.yaml")
        rules = config.get("email", {})

    df_result = df.copy()

    # Initialize validation columns
    if "EMAIL_VALID" not in df_result.columns:
        df_result["EMAIL_VALID"] = False
    if "EMAIL_REASON" not in df_result.columns:
        df_result["EMAIL_REASON"] = ""
    if "EMAIL_VALIDATION_LEVEL" not in df_result.columns:
        df_result["EMAIL_VALIDATION_LEVEL"] = "none"

    # Get email column name
    email_column = rules.get("column", "EMAIL")

    if email_column not in df_result.columns:
        logger.warning(f"Email column '{email_column}' not found in DataFrame")
        return df_result

    # Get MX check config
    mx_config = rules.get("mx_check", {})
    mx_enabled = mx_config.get("enabled", True)
    mx_timeout = min(mx_config.get("timeout", 2.0), 5.0)  # Maximum 5 seconds

    # Initialize validator
    validator = EmailValidator(dns_timeout=mx_timeout)

    logger.info(f"Validating emails for {len(df_result)} rows")

    # Validate each email
    for idx, row in df_result.iterrows():
        email = row.get(email_column)

        # Check if empty
        if pd.isna(email) or (isinstance(email, str) and not email.strip()):
            df_result.loc[idx, "EMAIL_VALID"] = False
            df_result.loc[idx, "EMAIL_REASON"] = "empty"
            df_result.loc[idx, "EMAIL_VALIDATION_LEVEL"] = "none"
            continue

        # Validate email
        result = validator.validate(str(email))

        if not result.valid:
            df_result.loc[idx, "EMAIL_VALID"] = False
            df_result.loc[idx, "EMAIL_REASON"] = result.error or "invalid_syntax"
            df_result.loc[idx, "EMAIL_VALIDATION_LEVEL"] = "none"
        elif result.generic:
            df_result.loc[idx, "EMAIL_VALID"] = True  # Syntax valid
            df_result.loc[idx, "EMAIL_REASON"] = "generic_email"
            df_result.loc[idx, "EMAIL_VALIDATION_LEVEL"] = "syntax"
        elif result.deliverable:
            df_result.loc[idx, "EMAIL_VALID"] = True
            df_result.loc[idx, "EMAIL_REASON"] = "ok"
            df_result.loc[idx, "EMAIL_VALIDATION_LEVEL"] = "mx"
        else:
            # Syntax valid but MX check failed
            df_result.loc[idx, "EMAIL_VALID"] = True  # Syntax is valid
            df_result.loc[idx, "EMAIL_REASON"] = result.error or "no_mx"
            df_result.loc[idx, "EMAIL_VALIDATION_LEVEL"] = "syntax"  # Only syntax validated

    logger.info(f"Email validation complete")
    return df_result
