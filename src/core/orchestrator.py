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
import os
import re
import asyncio
from typing import List, Dict, Any

logger = setup_logger()


def run_pipeline(
    df: pd.DataFrame,
    tier1_only: bool = False,
    config_path: str = "config/tier1_config.yaml",
    progress_callback: callable = None,
    check_stop_callback: callable = None,
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

    # 2) Tier1 validation (CIF/NIF/NIE + Phone)
    # NOTE: OBSERVACIONES column is NEVER modified - it must remain exactly as in input
    logger.info("Running Tier1 validation (CIF/NIF/NIE + Phone)...")
    cif_validator = CifValidator()
    phone_validator = PhoneValidator()

    # Ensure OBSERVACIONES column exists (but DO NOT modify it)
    if "OBSERVACIONES" not in df_result.columns:
        df_result["OBSERVACIONES"] = ""

    # Validation is done by Tier1Enricher, no need to duplicate here
    # OBSERVACIONES remains untouched

    # 3) Tier1 enrichment (existing phone/company enrichment)
    logger.info("Running Tier1 enrichment (phone finder + company name)...")
    enricher = Tier1Enricher(config_path=config_path)

    records = df_process.to_dict(orient="records")
    total_leads = len(records)
    
    # Debug: Log BEFORE enrichment
    logger.info(f"BEFORE Tier1: Sample record keys: {list(records[0].keys()) if records else 'NO RECORDS'}")
    if records:
        sample = records[0]
        logger.info(f"BEFORE Tier1: CIF/NIF={sample.get('CIF/NIF')}, CIF={sample.get('CIF')}, "
                   f"TELEFONO 1={sample.get('TELEFONO 1')}, NOMBRE CLIENTE={sample.get('NOMBRE CLIENTE')}")
    
    # Enrich with progress callback (now passed to enrich_batch)
    batch_report = enricher.enrich_batch(records, progress_callback=progress_callback, check_stop_callback=check_stop_callback)

    # Debug: Log AFTER enrichment
    logger.info(f"AFTER Tier1: Sample record keys: {list(records[0].keys()) if records else 'NO RECORDS'}")
    if records:
        sample = records[0]
        logger.info(f"AFTER Tier1: CIF={sample.get('CIF')}, PHONE={sample.get('PHONE')}, "
                   f"RAZON_SOCIAL={sample.get('RAZON_SOCIAL')}, CIF_VALID={sample.get('CIF_VALID')}")

    # Bring enriched columns back into the main DataFrame
    df_enriched = pd.DataFrame(records)
    indices = df_process.index.to_list()
    
    logger.info(f"Tier1 enriched DataFrame columns: {list(df_enriched.columns)}")
    logger.info(f"Tier1 enriched DataFrame shape: {df_enriched.shape}")

    # Update enriched columns, but NEVER touch OBSERVACIONES (must remain exactly as input)
    for col in df_enriched.columns:
        if col != "OBSERVACIONES":  # OBSERVACIONES is NEVER modified
            if col not in df_result.columns:
                df_result[col] = None  # Initialize column if it doesn't exist
                logger.info(f"Initialized new column: {col}")
            try:
                df_result.loc[indices, col] = df_enriched[col].values
                non_null_count = df_enriched[col].notna().sum()
                if non_null_count > 0:
                    logger.info(f"Updated column {col}: {non_null_count}/{len(indices)} non-null values")
                    # Log sample values
                    sample_values = df_enriched[col].dropna().head(2).tolist()
                    logger.info(f"  Sample values in {col}: {sample_values}")
            except Exception as e:
                logger.error(f"Error updating column {col}: {e}", exc_info=True)
    
    logger.info(f"df_result columns after Tier1: {list(df_result.columns)}")
    logger.info(f"df_result shape after Tier1: {df_result.shape}")

    # Clean temporary column before returning
    if "_IS_RED_ROW" in df_result.columns:
        df_result = df_result.drop(columns=["_IS_RED_ROW"])

    return df_result, batch_report


def run_tier2_enrichment(
    df: pd.DataFrame,
    tier2_config_path: str = "config/tier2_config.yaml",
    enable_email_research: bool = False,
    force_tier2: bool = False,
) -> Tuple[pd.DataFrame, Tier2BatchReport]:
    """Run Tier2 enrichment for priority>=2 leads only (or all non-red if force_tier2=True).

    This function expects a DataFrame with PRIORITY column already calculated.
    Only processes leads with priority >= 2, unless force_tier2=True.

    Args:
        df: DataFrame with PRIORITY column.
        tier2_config_path: Path to Tier2 config YAML.
        enable_email_research: Whether to enable email research.
        force_tier2: If True, process ALL non-red rows regardless of priority (for testing).

    Returns:
        Tuple of (enriched DataFrame, Tier2BatchReport).
    """
    df_result = df.copy()

    # Initialize mask as boolean Series indexed by df_result.index
    mask_tier2 = pd.Series(False, index=df_result.index)
    
    # Filter to priority >= 2 only (or all non-red if force_tier2)
    if force_tier2:
        # Process all non-red rows
        if "_IS_RED_ROW" in df_result.columns:
            mask_tier2 = (df_result["_IS_RED_ROW"] == False)
        else:
            # If _IS_RED_ROW doesn't exist, process all rows
            mask_tier2 = pd.Series(True, index=df_result.index)
        logger.info("Force Tier2 enabled: processing ALL non-red rows")
    else:
        # Normal mode: priority >= 2 and not red
        priority_mask = (df_result.get("PRIORITY", pd.Series(0, index=df_result.index)).fillna(0) >= 2)
        red_mask = (df_result.get("_IS_RED_ROW", pd.Series(False, index=df_result.index)) == False)
        mask_tier2 = priority_mask & red_mask
    
    # Validate mask before using
    assert isinstance(mask_tier2, pd.Series), f"mask_tier2 must be Series, got {type(mask_tier2)}"
    assert mask_tier2.dtype == bool, f"mask_tier2 must be bool dtype, got {mask_tier2.dtype}"
    assert len(mask_tier2) == len(df_result), f"mask_tier2 length {len(mask_tier2)} != df_result length {len(df_result)}"
    
    logger.info(f"Tier2 mask true count: {mask_tier2.sum()} / {len(mask_tier2)} (force={force_tier2})")
    
    # Use .loc for boolean indexing
    df_tier2 = df_result.loc[mask_tier2].copy()

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

    # Ensure OBSERVACIONES column exists (but DO NOT modify it)
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
    # NOTE: OBSERVACIONES is NEVER modified - all Tier2 info is in separate columns
    for col in df_tier2_enriched.columns:
        if col in tier2_columns:
            df_result.loc[indices, col] = df_tier2_enriched[col].values

    return df_result, tier2_report


def run_tavily_complementary_search(
    df: pd.DataFrame,
    progress_callback: callable = None,
    check_stop_callback: callable = None,
) -> pd.DataFrame:
    """Run complementary Tavily search for PRIORITY >= 2 leads.
    
    For leads with PRIORITY >= 2:
    1. If Google didn't find phone → Try Tavily for phone
    2. Always search Tavily for email (Google doesn't provide emails)
    
    Uses a single optimized Tavily query per lead.
    
    Args:
        df: DataFrame with PRIORITY, PHONE, PHONE_SOURCE columns.
        
    Returns:
        DataFrame with updated PHONE, PHONE_SOURCE, EMAIL_FOUND, EMAIL_SOURCE columns.
    """
    df_result = df.copy()
    
    # Initialize Tavily client
    tavily_key = os.getenv("TAVILY_API_KEY")
    if not tavily_key:
        logger.warning("TAVILY_API_KEY not found, skipping complementary Tavily search")
        return df_result
    
    try:
        from tavily import TavilyClient
        tavily_client = TavilyClient(api_key=tavily_key)
    except Exception as e:
        logger.warning(f"Could not initialize Tavily client: {e}")
        return df_result
    
    # Filter to PRIORITY >= 2 leads
    priority_mask = (df_result.get("PRIORITY", pd.Series(0, index=df_result.index)).fillna(0) >= 2)
    red_mask = (df_result.get("_IS_RED_ROW", pd.Series(False, index=df_result.index)) == False)
    mask_priority = priority_mask & red_mask
    
    df_priority = df_result.loc[mask_priority].copy()
    
    if len(df_priority) == 0:
        logger.info("No leads with PRIORITY >= 2, skipping complementary Tavily search")
        return df_result
    
    logger.info(f"Running complementary Tavily search for {len(df_priority)} PRIORITY >= 2 leads")
    
    # Initialize columns if they don't exist
    if "EMAIL_FOUND" not in df_result.columns:
        df_result["EMAIL_FOUND"] = None
    if "EMAIL_SOURCE" not in df_result.columns:
        df_result["EMAIL_SOURCE"] = None
    
    # Prepare leads for batch processing
    leads_to_process = []
    for idx, row in df_priority.iterrows():
        company_name = (
            str(row.get("NOMBRE CLIENTE", "") or "").strip() or
            str(row.get("NOMBRE_CLIENTE", "") or "").strip() or
            str(row.get("RAZON_SOCIAL", "") or "").strip() or
            str(row.get("NOMBRE_EMPRESA", "") or "").strip() or
            ""
        )
        
        if not company_name:
            continue
        
        # Check if we need phone (Google didn't find it)
        phone = row.get("PHONE")
        phone_source = str(row.get("PHONE_SOURCE", "") or "").strip()
        needs_phone = (pd.isna(phone) or not phone or phone_source in ["NOT_FOUND", "error", ""])
        
        # Always search for email for PRIORITY >= 2
        needs_email = True
        
        if not needs_phone and not needs_email:
            continue
        
        leads_to_process.append({
            "idx": idx,
            "company_name": company_name,
            "needs_phone": needs_phone,
            "needs_email": needs_email
        })
    
    # Process in parallel batches of 5-10
    async def search_tavily_async(lead_info: Dict[str, Any]) -> Dict[str, Any]:
        """Async Tavily search for a single lead."""
        idx = lead_info["idx"]
        company_name = lead_info["company_name"]
        needs_phone = lead_info["needs_phone"]
        needs_email = lead_info["needs_email"]
        
        result = {"idx": idx, "phone": None, "email": None}
        
        try:
            # Optimized query: search for both phone and email in one call
            # Improved query to be more specific
            query = f'"{company_name}" contacto email teléfono España'
            
            # Tavily client is synchronous, so we run it in executor
            loop = asyncio.get_event_loop()
            tavily_response = await loop.run_in_executor(
                None, 
                lambda: tavily_client.search(query, max_results=5, search_depth="advanced")
            )
            
            logger.debug(f"Tavily search for {company_name}: {len(tavily_response.get('results', []))} results")
            
            if tavily_response.get("results"):
                content_combined = " ".join([r.get("content", "") or "" for r in tavily_response.get("results", [])])
                urls_combined = " ".join([r.get("url", "") or "" for r in tavily_response.get("results", [])])
                all_text = content_combined + " " + urls_combined
                
                logger.debug(f"Tavily content length for {company_name}: {len(all_text)} chars")
                
                # Extract phone if needed
                if needs_phone:
                    phone_pattern = r'(?:\+34|34)?[\s.-]?([6-9]\d{8})'
                    matches = re.findall(phone_pattern, all_text)
                    if matches:
                        phone_digits = matches[0].replace(" ", "").replace(".", "").replace("-", "").strip()
                        if len(phone_digits) == 9:
                            result["phone"] = f"+34{phone_digits}"
                            logger.info(f"Tavily found phone for {company_name}: {result['phone']}")
                
                # Extract email
                if needs_email:
                    email_pattern = r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'
                    email_matches = re.findall(email_pattern, all_text)
                    if email_matches:
                        # Filter emails that look like company emails (not generic)
                        valid_emails = [e for e in email_matches if not any(x in e.lower() for x in ['example', 'test', 'noreply', 'no-reply', 'info@', 'contact@', 'hello@'])]
                        if valid_emails:
                            # Prefer emails with company domain
                            company_domain = company_name.lower().replace(" ", "").replace(".", "").replace(",", "")[:15]
                            preferred_email = None
                            for email in valid_emails:
                                email_domain = email.split('@')[1].split('.')[0] if '@' in email else ""
                                if company_domain in email_domain or any(word in email_domain for word in company_name.lower().split()[:2] if len(word) > 3):
                                    preferred_email = email
                                    break
                            
                            result["email"] = preferred_email or valid_emails[0]
                            logger.info(f"Tavily found email for {company_name}: {result['email']}")
                    else:
                        logger.debug(f"No email matches found in Tavily results for {company_name}")
            else:
                logger.debug(f"Tavily returned no results for {company_name}")
        
        except Exception as e:
            logger.warning(f"Tavily complementary search failed for {company_name}: {e}", exc_info=True)
        
        return result
    
    async def process_batch(leads_batch: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Process a batch of leads in parallel."""
        tasks = [search_tavily_async(lead) for lead in leads_batch]
        return await asyncio.gather(*tasks)
    
    # Process in batches of 8 (good balance between speed and rate limits)
    batch_size = 8
    all_results = []
    
    total_batches = (len(leads_to_process) + batch_size - 1) // batch_size
    for i in range(0, len(leads_to_process), batch_size):
        batch = leads_to_process[i:i + batch_size]
        batch_num = i // batch_size + 1
        logger.info(f"Processing Tavily batch {batch_num}/{total_batches} ({len(batch)} leads)")
        
        # Update progress during Tavily
        if progress_callback:
            # Estimate progress: Tier1 was ~50%, Tavily is ~30%, Tier3 is ~20%
            # So Tavily starts at 50% and goes to 80%
            tavily_progress_start = 0.5
            tavily_progress_end = 0.8
            progress = tavily_progress_start + (batch_num / total_batches) * (tavily_progress_end - tavily_progress_start)
            try:
                progress_callback(int(progress * len(leads_to_process)), len(leads_to_process), f"Buscando con Tavily (batch {batch_num}/{total_batches})...")
            except Exception as e:
                logger.warning(f"Progress callback error during Tavily: {e}")
        
        # Check stop
        if check_stop_callback and check_stop_callback():
            logger.info("Stop requested by user during Tavily search")
            raise KeyboardInterrupt("Processing stopped by user")
        
        try:
            batch_results = asyncio.run(process_batch(batch))
            all_results.extend(batch_results)
            
            # Log results for debugging
            phones_found = sum(1 for r in batch_results if r.get("phone"))
            emails_found = sum(1 for r in batch_results if r.get("email"))
            logger.info(f"Tavily batch {batch_num}: {phones_found} phones, {emails_found} emails found")
        except KeyboardInterrupt:
            raise
        except Exception as e:
            logger.error(f"Error processing Tavily batch: {e}")
            continue
    
    # Update DataFrame with results
    for result in all_results:
        idx = result["idx"]
        if result.get("phone"):
            df_result.loc[idx, "PHONE"] = result["phone"]
            df_result.loc[idx, "PHONE_SOURCE"] = "tavily"
            logger.debug(f"Found phone via Tavily: {result['phone']}")
        if result.get("email"):
            df_result.loc[idx, "EMAIL_FOUND"] = result["email"]
            df_result.loc[idx, "EMAIL_SOURCE"] = "tavily"
            logger.debug(f"Found email via Tavily: {result['email']}")
    
    logger.info("✅ Complementary Tavily search completed")
    return df_result


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


def process_file(
    input_path: Path,
    output_path: Path,
    tiers: list[int] = [1, 3],
    enable_email_research: bool = False,
    force_tier2: bool = False,
    progress_callback: callable = None,
    check_stop_callback: callable = None,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    """Process Excel file through the full pipeline.

    Args:
        input_path: Path to input Excel file.
        output_path: Path for output Excel file.
        tiers: List of tiers to run (1, 2, 3). Default: [1, 3].
        enable_email_research: Whether to enable email research in Tier2.

    Returns:
        Tuple of (processed DataFrame, metrics dict).
    """
    from src.core.excel_processor import read_excel, write_excel

    errors_list = []  # List to collect errors: {row: int, field: str, error: str}

    try:
        # Read Excel file
        logger.info(f"Reading Excel file: {input_path}")
        df, metadata = read_excel(input_path)

        # Run Tier1 (always runs if tier 1 is in tiers)
        if 1 in tiers:
            logger.info("Running Tier1 pipeline...")
            df_result, batch_report = run_pipeline(
                df=df,
                tier1_only=False,
                config_path="config/tier1_config.yaml",
                progress_callback=progress_callback,
                check_stop_callback=check_stop_callback,
            )
        else:
            # Just calculate priorities if Tier1 is skipped
            from src.core.priority_engine import PriorityEngine
            priority_engine = PriorityEngine()
            mask_process = ~df.get("_IS_RED_ROW", False)
            df_process = df[mask_process].copy()
            priorities = priority_engine.calculate_priorities(df_process)
            df_result = df.copy()
            df_result["PRIORITY"] = None
            df_result.loc[mask_process, "PRIORITY"] = priorities.values
            batch_report = None

        # Run complementary Tavily search for PRIORITY >= 2 (after Tier1, before Tier2)
        # This searches for phone (if Google didn't find it) and email (always for PRIORITY >= 2)
        logger.info("Running complementary Tavily search for PRIORITY >= 2 leads...")
        df_result = run_tavily_complementary_search(
            df_result,
            progress_callback=progress_callback,
            check_stop_callback=check_stop_callback
        )
        
        # Run Tier2 if requested
        tier2_report = None
        if 2 in tiers:
            logger.info("Running Tier2 enrichment...")
            df_result, tier2_report = run_tier2_enrichment(
                df=df_result,
                tier2_config_path="config/tier2_config.yaml",
                enable_email_research=enable_email_research,
                force_tier2=force_tier2,
            )

        # Run Tier3 if requested
        if 3 in tiers:
            logger.info("Running Tier3 enrichment + validation + scoring...")
            df_result = run_tier3_and_validation(df_result, enable_tier3=True)

        # ============================================
        # Add status columns for DATOS_TÉCNICOS
        # ============================================
        red_df_indices = metadata.get("red_df_indices", [])
        
        # Initialize status columns
        df_result["ENRICHMENT_STATUS"] = None
        df_result["ENRICHMENT_NOTES"] = None
        df_result["TIER1_STATUS"] = None
        df_result["TIER2_STATUS"] = None
        df_result["TIER3_STATUS"] = None
        
        # Set status for red rows (skipped)
        for idx in red_df_indices:
            if idx < len(df_result):
                row_idx = df_result.index[idx]
                df_result.loc[row_idx, "ENRICHMENT_STATUS"] = "SKIPPED_RED"
                df_result.loc[row_idx, "ENRICHMENT_NOTES"] = "original red row"
                df_result.loc[row_idx, "TIER1_STATUS"] = "SKIPPED"
                df_result.loc[row_idx, "TIER2_STATUS"] = "SKIPPED"
                df_result.loc[row_idx, "TIER3_STATUS"] = "SKIPPED"
        
        # Set status for processed rows
        mask_processed = ~df_result.index.isin([df_result.index[i] for i in red_df_indices if i < len(df_result)])
        df_processed = df_result[mask_processed].copy()
        
        # Tier1 status
        if 1 in tiers:
            for idx in df_processed.index:
                errors = str(df_result.loc[idx, "ERRORS"] or "").strip()
                if "GOOGLE_PLACES" in errors and "rate limit" in errors.lower():
                    df_result.loc[idx, "TIER1_STATUS"] = "RATE_LIMITED"
                    df_result.loc[idx, "ENRICHMENT_STATUS"] = "RATE_LIMITED"
                    df_result.loc[idx, "ENRICHMENT_NOTES"] = "google_places rate limit"
                elif "GOOGLE_PLACES" in errors:
                    df_result.loc[idx, "TIER1_STATUS"] = "NOT_FOUND"
                    if df_result.loc[idx, "ENRICHMENT_STATUS"] is None:
                        df_result.loc[idx, "ENRICHMENT_STATUS"] = "NOT_FOUND"
                    if df_result.loc[idx, "ENRICHMENT_NOTES"] is None:
                        df_result.loc[idx, "ENRICHMENT_NOTES"] = "google_places no match"
                elif errors:
                    df_result.loc[idx, "TIER1_STATUS"] = "ERROR"
                    if df_result.loc[idx, "ENRICHMENT_STATUS"] is None:
                        df_result.loc[idx, "ENRICHMENT_STATUS"] = "ERROR"
                    if df_result.loc[idx, "ENRICHMENT_NOTES"] is None:
                        df_result.loc[idx, "ENRICHMENT_NOTES"] = f"tier1 error: {errors[:50]}"
                else:
                    df_result.loc[idx, "TIER1_STATUS"] = "OK"
                    if df_result.loc[idx, "ENRICHMENT_STATUS"] is None:
                        df_result.loc[idx, "ENRICHMENT_STATUS"] = "OK"
        else:
            for idx in df_processed.index:
                df_result.loc[idx, "TIER1_STATUS"] = "SKIPPED"
        
        # Tier2 status
        if 2 in tiers:
            for idx in df_processed.index:
                tier2_errors = str(df_result.loc[idx, "TIER2_ERRORS"] or "").strip()
                if tier2_errors:
                    if "tavily" in tier2_errors.lower() or "openai" in tier2_errors.lower():
                        df_result.loc[idx, "TIER2_STATUS"] = "ERROR"
                        if df_result.loc[idx, "ENRICHMENT_STATUS"] not in ["RATE_LIMITED", "ERROR"]:
                            df_result.loc[idx, "ENRICHMENT_STATUS"] = "ERROR"
                        notes = f"tavily error: {tier2_errors[:50]}"
                        if df_result.loc[idx, "ENRICHMENT_NOTES"]:
                            df_result.loc[idx, "ENRICHMENT_NOTES"] += f" | {notes}"
                        else:
                            df_result.loc[idx, "ENRICHMENT_NOTES"] = notes
                    else:
                        df_result.loc[idx, "TIER2_STATUS"] = "ERROR"
                else:
                    email = str(df_result.loc[idx, "EMAIL_SPECIFIC"] or "").strip()
                    if email and email not in ["", "NO_EMAIL_FOUND", "NOT_FOUND"]:
                        df_result.loc[idx, "TIER2_STATUS"] = "OK"
                    else:
                        df_result.loc[idx, "TIER2_STATUS"] = "NOT_FOUND"
                
                # Ensure EMAIL_SPECIFIC is not blank if Tier2 ran
                if pd.isna(df_result.loc[idx, "EMAIL_SPECIFIC"]) or str(df_result.loc[idx, "EMAIL_SPECIFIC"]).strip() == "":
                    df_result.loc[idx, "EMAIL_SPECIFIC"] = "NO_EMAIL_FOUND"
                
                # Check for contact without email
                contact_name = str(df_result.loc[idx, "CONTACT_NAME"] or "").strip()
                if contact_name and contact_name not in ["", "NOT_FOUND", "NO_CONTACT_FOUND"]:
                    email_val = str(df_result.loc[idx, "EMAIL_SPECIFIC"] or "").strip()
                    if email_val in ["", "NO_EMAIL_FOUND", "NOT_FOUND"]:
                        if df_result.loc[idx, "ENRICHMENT_NOTES"]:
                            df_result.loc[idx, "ENRICHMENT_NOTES"] += " | contact found, no email"
                        else:
                            df_result.loc[idx, "ENRICHMENT_NOTES"] = "contact found, no email"
        else:
            for idx in df_processed.index:
                df_result.loc[idx, "TIER2_STATUS"] = "SKIPPED"
        
        # Tier3 status
        if 3 in tiers:
            for idx in df_processed.index:
                # Ensure WEBSITE and CNAE are not blank if Tier3 ran
                if pd.isna(df_result.loc[idx, "WEBSITE"]) or str(df_result.loc[idx, "WEBSITE"]).strip() == "":
                    df_result.loc[idx, "WEBSITE"] = "NOT_FOUND"
                if pd.isna(df_result.loc[idx, "CNAE"]) or str(df_result.loc[idx, "CNAE"]).strip() == "":
                    df_result.loc[idx, "CNAE"] = "NOT_FOUND"
                
                website = str(df_result.loc[idx, "WEBSITE"] or "").strip()
                cnae = str(df_result.loc[idx, "CNAE"] or "").strip()
                
                if website == "NOT_FOUND" and cnae == "NOT_FOUND":
                    df_result.loc[idx, "TIER3_STATUS"] = "NOT_FOUND"
                elif website == "NOT_FOUND" or cnae == "NOT_FOUND":
                    df_result.loc[idx, "TIER3_STATUS"] = "OK"  # Partial success
                else:
                    df_result.loc[idx, "TIER3_STATUS"] = "OK"
        else:
            for idx in df_processed.index:
                df_result.loc[idx, "TIER3_STATUS"] = "SKIPPED"

        # Ensure PHONE_SOURCE is properly set (should never be "web_scraper" now)
        if "PHONE_SOURCE" in df_result.columns:
            mask_missing_source = df_result["PHONE_SOURCE"].isna() | (df_result["PHONE_SOURCE"] == "")
            mask_no_phone = df_result["PHONE"].isna() | (df_result["PHONE"] == "")
            mask_needs_fix = mask_missing_source & mask_no_phone
            if mask_needs_fix.any():
                df_result.loc[mask_needs_fix, "PHONE_SOURCE"] = "NOT_FOUND"
                logger.info(f"Set {mask_needs_fix.sum()} rows with missing PHONE_SOURCE to 'NOT_FOUND'")

        # Write output Excel
        logger.info(f"Writing output to: {output_path}")
        write_excel(df_result, metadata, output_path, preserve_format=True, force_tier2=force_tier2)

        # Add errors sheet if there are any errors
        if errors_list:
            logger.info(f"Writing {len(errors_list)} errors to Excel sheet...")
            try:
                from openpyxl import load_workbook
                wb = load_workbook(output_path)
                df_errors = pd.DataFrame(errors_list)
                with pd.ExcelWriter(output_path, mode='a', engine='openpyxl', if_sheet_exists='replace') as writer:
                    writer.book = wb
                    df_errors.to_excel(writer, sheet_name='Errores', index=False)
                wb.save(output_path)
                wb.close()
            except Exception as e:
                logger.error(f"Error writing errors sheet: {e}")

        # Calculate metrics
        total_rows = len(df_result)
        high_quality = (df_result["DATA_QUALITY"] == "High").sum() if "DATA_QUALITY" in df_result.columns else 0
        emails_valid = df_result["EMAIL_VALID"].sum() if "EMAIL_VALID" in df_result.columns else 0

        metrics = {
            "total_processed": total_rows,
            "high_quality": high_quality,
            "emails_valid": emails_valid,
            "errors_count": len(errors_list),
        }

        logger.info("✅ Processing complete!")
        return df_result, metrics

    except Exception as e:
        logger.error(f"Error processing file: {e}", exc_info=True)
        # Add to errors list
        errors_list.append({
            "row": "N/A",
            "field": "SYSTEM",
            "error": str(e)
        })
        raise
