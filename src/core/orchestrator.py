from __future__ import annotations

from pathlib import Path
from typing import Tuple, Dict, Any

import pandas as pd

from src.core.priority_engine import PriorityEngine
from src.api_manager.tier1_enricher import Tier1Enricher
from src.api_manager.base import BatchReport
from src.enrichers.tier2_enricher import Tier2Enricher, Tier2BatchReport
from src.enrichers.tier3_enricher import Tier3Enricher
from src.tier1.cif_validator import CifValidator
from src.tier1.phone_validator import PhoneValidator
from src.validators.email_batch_validator import validate_all_emails
from src.validators.phone_batch_validator import validate_all_phones
from src.validators.cif_batch_validator import revalidate_cifs
from src.core.scoring_engine import ScoringEngine
from src.utils.logger import setup_logger
from src.utils.config_loader import load_yaml_config

logger = setup_logger()


def run_pipeline(
    df: pd.DataFrame,
    tier1_only: bool = False,
    config_path: str = "config/tier1_config.yaml",
) -> Tuple[pd.DataFrame, BatchReport]:
    """Run full pipeline: priority + Tier1 enrichment.

    This function expects a DataFrame with the temporary column ``_IS_RED_ROW``
    already present (as produced by ``read_excel``). Red rows are skipped for
    processing but preserved in the output.
    """

    df_result = df.copy()

    # Filter out red rows for processing (but keep them marked in df_result)
    mask_process = ~df_result.get("_IS_RED_ROW", False)
    df_process = df_result[mask_process].copy()

    logger.info(
        "Core orchestrator: processing %d rows (skipping %d red rows)",
        len(df_process),
        len(df_result) - len(df_process),
    )

    # 1) Priority calculation (unless tier1-only)
    if not tier1_only:
        priority_engine = PriorityEngine()
        priorities = priority_engine.calculate_priorities(df_process)
        df_result["PRIORITY"] = None
        df_result.loc[mask_process, "PRIORITY"] = priorities.values

    # 2) Tier1 validation (CIF/NIF/NIE + Phone) - append to OBSERVACIONES
    logger.info("Running Tier1 validation (CIF/NIF/NIE + Phone)...")
    cif_validator = CifValidator()
    phone_validator = PhoneValidator()

    # Ensure OBSERVACIONES column exists
    if "OBSERVACIONES" not in df_result.columns:
        df_result["OBSERVACIONES"] = ""

    # Process each row and append validation results to OBSERVACIONES
    try:
        from tqdm import tqdm
        iterator = tqdm(df_process.iterrows(), total=len(df_process), desc="Tier1 validation")
    except Exception:
        iterator = df_process.iterrows()

    # Process each row and update OBSERVACIONES directly
    for idx, row in iterator:
        observaciones_parts = []
        # Get original OBSERVACIONES from df_result using the same index
        try:
            original_obs = str(df_result.loc[idx, "OBSERVACIONES"] or "").strip()
        except KeyError:
            # If index doesn't exist in df_result, skip (shouldn't happen, but safety check)
            logger.warning(f"Index {idx} not found in df_result, skipping OBSERVACIONES update")
            continue

        # CIF/NIF/NIE validation
        cif_value = str(row.get("CIF/NIF", "")).strip()
        if cif_value:
            cif_result = cif_validator.validate(cif_value)
            if cif_result.is_valid:
                entity_info = f", {cif_result.entity_type}" if cif_result.entity_type else ""
                observaciones_parts.append(
                    f"CIF/NIF/NIE: {cif_result.formatted_id} ({cif_result.id_type}{entity_info}) ✓"
                )
            else:
                observaciones_parts.append(
                    f"CIF/NIF/NIE: {cif_value} - {cif_result.error or 'INVALID'}"
                )

        # Phone validation
        phone_value = str(row.get("TELÉFONO", "")).strip()
        if phone_value:
            phone_result = phone_validator.validate(phone_value)
            if phone_result.is_valid:
                observaciones_parts.append(
                    f"Teléfono: {phone_result.international_format} ({phone_result.phone_type}) ✓"
                )
            else:
                observaciones_parts.append(
                    f"Teléfono: {phone_value} - {phone_result.error or 'INVALID'}"
                )

        # Append to OBSERVACIONES (preserve existing content)
        if observaciones_parts:
            new_obs = " | ".join(observaciones_parts)
            if original_obs:
                final_obs = f"{original_obs} | {new_obs}"
            else:
                final_obs = new_obs
            
            # Write directly to df_result using .loc
            df_result.loc[idx, "OBSERVACIONES"] = final_obs
            logger.debug(f"Updated OBSERVACIONES for index {idx}: {final_obs[:50]}...")

    # 3) Tier1 enrichment (existing phone/company enrichment)
    logger.info("Running Tier1 enrichment (phone finder + company name)...")
    enricher = Tier1Enricher(config_path=config_path)

    records = df_process.to_dict(orient="records")
    batch_report = enricher.enrich_batch(records)

    # Bring enriched columns back into the main DataFrame
    df_enriched = pd.DataFrame(records)
    indices = df_process.index.to_list()

    # Update enriched columns, but preserve OBSERVACIONES (already updated by validators)
    for col in df_enriched.columns:
        if col != "OBSERVACIONES":  # Don't overwrite OBSERVACIONES - it was already updated by validators
            df_result.loc[indices, col] = df_enriched[col].values

    # Clean temporary column before returning
    if "_IS_RED_ROW" in df_result.columns:
        df_result = df_result.drop(columns=["_IS_RED_ROW"])

    return df_result, batch_report


def run_tier2_enrichment(
    df: pd.DataFrame,
    tier2_config_path: str = "config/tier2_config.yaml",
    enable_email_research: bool = False,
) -> Tuple[pd.DataFrame, Tier2BatchReport]:
    """Run Tier2 enrichment for priority>=2 leads only.

    This function expects a DataFrame with PRIORITY column already calculated.
    Only processes leads with priority >= 2.

    Args:
        df: DataFrame with PRIORITY column.
        tier2_config_path: Path to Tier2 config YAML.

    Returns:
        Tuple of (enriched DataFrame, Tier2BatchReport).
    """
    df_result = df.copy()

    # Filter to priority >= 2 only
    mask_tier2 = df_result.get("PRIORITY", 0) >= 2
    df_tier2 = df_result[mask_tier2].copy()

    if len(df_tier2) == 0:
        logger.info("No leads with priority>=2, skipping Tier2 enrichment")
        # Initialize Tier2 columns with None
        for col in [
            "EMAIL_SPECIFIC",
            "EMAIL_VALID",
            "CONTACT_NAME",
            "CONTACT_TITLE",
            "LINKEDIN_COMPANY",
            "EMAIL_RESEARCHED",
            "EMAIL_SOURCE",
            "EMAIL_CONFIDENCE",
            "RESEARCH_NOTES",
            "TIER2_ERRORS",
        ]:
            df_result[col] = None
        return df_result, Tier2BatchReport(
            total=0,
            emails_found=0,
            emails_researched=0,
            linkedin_found=0,
            contacts_found=0,
            total_openai_tokens=0,
            errors=[],
        )

    logger.info(f"Tier2 enrichment: processing {len(df_tier2)} leads with priority>=2")

    # Ensure OBSERVACIONES column exists
    if "OBSERVACIONES" not in df_result.columns:
        df_result["OBSERVACIONES"] = ""

    # Run Tier2 enrichment
    tier2_enricher = Tier2Enricher(config_path=tier2_config_path)
    records = df_tier2.to_dict(orient="records")
    tier2_report = tier2_enricher.enrich_batch(records, enable_email_research=enable_email_research)

    # Bring enriched columns back into main DataFrame
    df_tier2_enriched = pd.DataFrame(records)
    indices = df_tier2.index.to_list()

    # Initialize columns for all rows
    tier2_columns = [
        "EMAIL_SPECIFIC",
        "EMAIL_VALID",
        "CONTACT_NAME",
        "CONTACT_TITLE",
        "LINKEDIN_COMPANY",
        "EMAIL_RESEARCHED",
        "EMAIL_SOURCE",
        "EMAIL_CONFIDENCE",
        "RESEARCH_NOTES",
        "TIER2_ERRORS",
    ]
    for col in tier2_columns:
        if col not in df_result.columns:
            df_result[col] = None

    # Update only Tier2 rows
    for col in df_tier2_enriched.columns:
        if col in tier2_columns:
            df_result.loc[indices, col] = df_tier2_enriched[col].values

    # Append Tier2 enrichment results to OBSERVACIONES
    for i, record in enumerate(records):
        idx = indices[i]
        observaciones_parts = []
        original_obs = str(df_result.loc[idx, "OBSERVACIONES"] or "").strip()

        # Add email research info
        if record.get("EMAIL_RESEARCHED"):
            obs_parts = [f"Email investigado: {record.get('EMAIL_RESEARCHED')}"]
            if record.get("EMAIL_SOURCE"):
                obs_parts.append(f"Fuente: {record.get('EMAIL_SOURCE')}")
            if record.get("RESEARCH_NOTES"):
                obs_parts.append(record.get("RESEARCH_NOTES"))
            observaciones_parts.append(" | ".join(obs_parts))

        # Add contact info
        if record.get("CONTACT_NAME"):
            contact_info = f"Contacto: {record.get('CONTACT_NAME')}"
            if record.get("CONTACT_TITLE"):
                contact_info += f" ({record.get('CONTACT_TITLE')})"
            observaciones_parts.append(contact_info)

        # Add LinkedIn
        if record.get("LINKEDIN_COMPANY"):
            observaciones_parts.append(f"LinkedIn: {record.get('LINKEDIN_COMPANY')}")

        # Append to OBSERVACIONES
        if observaciones_parts:
            new_obs = " | ".join(observaciones_parts)
            if original_obs:
                df_result.loc[idx, "OBSERVACIONES"] = f"{original_obs} | {new_obs}"
            else:
                df_result.loc[idx, "OBSERVACIONES"] = new_obs

    return df_result, tier2_report


def run_tier3_and_validation(
    df: pd.DataFrame,
    enable_tier3: bool = True,
) -> pd.DataFrame:
    """Run Tier3 enrichment, batch validation, and scoring.

    Pipeline:
        1. Tier3 enrichment (WEBSITE, CNAE for empty fields only)
        2. Batch email validation (all emails)
        3. Batch phone validation (all phones)
        4. CIF revalidation (failed CIFs only)
        5. Scoring (completeness, confidence, quality)

    Args:
        df: DataFrame after Tier2 enrichment.
        enable_tier3: Whether to run Tier3 enrichment (default: True).

    Returns:
        DataFrame with Tier3 enrichment, validation flags, and scoring columns.
    """
    df_result = df.copy()

    # Load configs
    enrichment_rules = load_yaml_config("config/rules/enrichment_rules.yaml")
    validation_rules = load_yaml_config("config/rules/validation_rules.yaml")

    # 1) Tier3 enrichment
    if enable_tier3:
        logger.info("Running Tier3 enrichment (WEBSITE, CNAE)...")
        tier3_rules = enrichment_rules.get("tier3", {})
        tier3_enricher = Tier3Enricher(rules=tier3_rules)
        df_result = tier3_enricher.process_missing_only(df_result)
    else:
        logger.info("Tier3 enrichment skipped")
        # Initialize Tier3 columns if not exist
        if "WEBSITE" not in df_result.columns:
            df_result["WEBSITE"] = None
        if "CNAE" not in df_result.columns:
            df_result["CNAE"] = None
        if "WEBSITE_SOURCE" not in df_result.columns:
            df_result["WEBSITE_SOURCE"] = None
        if "CNAE_SOURCE" not in df_result.columns:
            df_result["CNAE_SOURCE"] = None

    # 2) Batch email validation
    logger.info("Running batch email validation...")
    email_rules = validation_rules.get("email", {})
    df_result = validate_all_emails(df_result, email_rules)

    # 3) Batch phone validation
    logger.info("Running batch phone validation...")
    phone_rules = validation_rules.get("phone", {})
    df_result = validate_all_phones(df_result, phone_rules)

    # 4) CIF revalidation
    logger.info("Running CIF revalidation...")
    cif_rules = validation_rules.get("cif", {})
    df_result = revalidate_cifs(df_result, cif_rules)

    # 5) Scoring
    logger.info("Calculating data quality scores...")
    scoring_rules = validation_rules.get("scoring", {})
    scoring_engine = ScoringEngine(validation_rules={"scoring": scoring_rules})
    df_result = scoring_engine.annotate_dataframe(df_result)

    logger.info("Tier3, validation, and scoring complete")
    return df_result
