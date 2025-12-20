"""CLI entry point for Lead Enrichment Engine."""

import argparse
import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.utils.logger import setup_logger
from src.core.excel_processor import read_excel, write_excel
from src.core.orchestrator import run_pipeline, run_tier2_enrichment, run_tier3_and_validation

logger = setup_logger()


def generate_output_filename(input_path: Path) -> Path:
    """Generate output filename starting with 'LIMPIO_'.

    Args:
        input_path: Path to input Excel file.

    Returns:
        Path for output file in /Users/matiaswas/Downloads/ale/ directory.
    """
    input_name = input_path.stem
    output_dir = Path("/Users/matiaswas/Downloads/ale")
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / f"LIMPIO_{input_name}.xlsx"
    return output_path


def main() -> None:
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Lead Enrichment Engine - Process Excel leads and calculate priorities"
    )
    parser.add_argument("input_file", type=str, help="Path to input Excel file")
    parser.add_argument(
        "--output",
        type=str,
        default=None,
        help="Path to output Excel file (default: LIMPIO_<input_name>.xlsx)",
    )
    parser.add_argument(
        "--tier1-only",
        action="store_true",
        help="Run only Tier1 enrichment (skip priority_engine)",
    )
    parser.add_argument(
        "--tier2",
        action="store_true",
        help="Enable Tier2 enrichment for priority>=2 leads (emails, contacts, LinkedIn)",
    )
    parser.add_argument(
        "--research-emails",
        action="store_true",
        help="Enable Tavily+OpenAI email research for priority>=3 leads (requires --tier2)",
    )
    parser.add_argument(
        "--tier3",
        action="store_true",
        help="Enable Tier3 enrichment + validation + scoring (WEBSITE, CNAE, batch validation, scoring)",
    )

    args = parser.parse_args()

    input_path = Path(args.input_file)

    # Validate input file exists
    if not input_path.exists():
        logger.error(f"Input file not found: {input_path}")
        sys.exit(1)

    logger.info(f"Starting Lead Enrichment Engine")
    logger.info(f"Input file: {input_path}")

    try:
        # Read Excel file
        logger.info("Reading Excel file...")
        df, metadata = read_excel(input_path)

        # Run core pipeline (priority + Tier1 enrichment)
        df_result, batch_report = run_pipeline(
            df=df,
            tier1_only=args.tier1_only,
            config_path="config/tier1_config.yaml",
        )

        # Run Tier2 enrichment if flag is set
        tier2_report = None
        if args.tier2:
            enable_research = args.research_emails
            if enable_research:
                logger.info("Starting Tier2 enrichment with email research for priority>=2 leads...")
            else:
                logger.info("Starting Tier2 enrichment for priority>=2 leads...")
            df_result, tier2_report = run_tier2_enrichment(
                df=df_result,
                tier2_config_path="config/tier2_config.yaml",
                enable_email_research=enable_research,
            )

        # Run Tier3 enrichment, validation, and scoring if flag is set
        if args.tier3:
            logger.info("Starting Tier3 enrichment + validation + scoring...")
            df_result = run_tier3_and_validation(df_result, enable_tier3=True)

        # Generate output path
        if args.output:
            output_path = Path(args.output)
        else:
            output_path = generate_output_filename(input_path)

        # Write output Excel
        logger.info(f"Writing output to: {output_path}")
        write_excel(df_result, metadata, output_path, preserve_format=True)

        # Log stats
        logger.info("✅ Processing complete!")
        logger.info(f"Output file: {output_path}")
        logger.info(f"Total rows processed (non-red): {batch_report.total}")
        logger.info(f"CIF validated: {batch_report.cif_validated}/{batch_report.total}")
        logger.info(f"Phone found: {batch_report.phone_found}/{batch_report.total}")

        if tier2_report:
            logger.info("--- Tier2 Statistics ---")
            logger.info(f"Tier2 leads processed: {tier2_report.total}")
            logger.info(f"Emails found: {tier2_report.emails_found}/{tier2_report.total}")
            if args.research_emails:
                logger.info(f"Emails researched: {tier2_report.emails_researched}/{tier2_report.total}")
            logger.info(f"LinkedIn URLs found: {tier2_report.linkedin_found}/{tier2_report.total}")
            logger.info(f"Contacts found: {tier2_report.contacts_found}/{tier2_report.total}")
            # Estimate cost (GPT-4o-mini: $0.15 per 1M input tokens, $0.60 per 1M output tokens)
            input_cost = (tier2_report.total_openai_tokens * 0.9) / 1_000_000 * 0.15
            output_cost = (tier2_report.total_openai_tokens * 0.1) / 1_000_000 * 0.60
            total_cost = input_cost + output_cost
            logger.info(f"OpenAI tokens used: {tier2_report.total_openai_tokens:,}")
            logger.info(f"Estimated OpenAI cost: ${total_cost:.4f}")

        if args.tier3:
            logger.info("--- Tier3 Statistics ---")
            total_rows = len(df_result)
            websites_enriched = df_result["WEBSITE"].notna().sum() if "WEBSITE" in df_result.columns else 0
            cnae_enriched = df_result["CNAE"].notna().sum() if "CNAE" in df_result.columns else 0
            emails_valid = df_result["EMAIL_VALID"].sum() if "EMAIL_VALID" in df_result.columns else 0
            phones_valid = df_result["PHONE_VALID"].sum() if "PHONE_VALID" in df_result.columns else 0
            high_quality = (df_result["DATA_QUALITY"] == "High").sum() if "DATA_QUALITY" in df_result.columns else 0
            logger.info(f"Websites enriched: {websites_enriched}/{total_rows}")
            logger.info(f"CNAE enriched: {cnae_enriched}/{total_rows}")
            logger.info(f"Valid emails: {emails_valid}/{total_rows}")
            logger.info(f"Valid phones: {phones_valid}/{total_rows}")
            logger.info(f"High quality leads: {high_quality}/{total_rows}")

    except Exception as e:
        logger.error(f"Error processing file: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()

