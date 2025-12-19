"""CLI entry point for Lead Enrichment Engine."""

import argparse
import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.utils.logger import setup_logger
from src.core.excel_processor import read_excel, write_excel
from src.core.orchestrator import run_pipeline

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

    except Exception as e:
        logger.error(f"Error processing file: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()

