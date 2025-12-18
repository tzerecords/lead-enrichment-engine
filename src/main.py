"""CLI entry point for Lead Enrichment Engine."""

import argparse
import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.utils.logger import setup_logger
from src.core.excel_processor import read_excel, write_excel
from src.core.priority_engine import PriorityEngine

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
    parser.add_argument(
        "input_file",
        type=str,
        help="Path to input Excel file",
    )
    parser.add_argument(
        "--output",
        type=str,
        default=None,
        help="Path to output Excel file (default: data/output/LIMPIO_<input_name>.xlsx)",
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

        # Filter out red rows for processing (but keep them marked)
        df_process = df[~df.get("_IS_RED_ROW", False)].copy()
        logger.info(f"Processing {len(df_process)} rows (skipping {len(df) - len(df_process)} red rows)")

        # Calculate priorities
        logger.info("Calculating priorities...")
        priority_engine = PriorityEngine()
        priorities = priority_engine.calculate_priorities(df_process)

        # Add PRIORITY column to DataFrame
        df["PRIORITY"] = None
        df.loc[~df.get("_IS_RED_ROW", False), "PRIORITY"] = priorities.values

        # Remove temporary _IS_RED_ROW column before writing
        if "_IS_RED_ROW" in df.columns:
            df = df.drop(columns=["_IS_RED_ROW"])

        # Generate output path
        if args.output:
            output_path = Path(args.output)
        else:
            output_path = generate_output_filename(input_path)

        # Write output Excel
        logger.info(f"Writing output to: {output_path}")
        write_excel(df, metadata, output_path, preserve_format=True)

        logger.info("✅ Processing complete!")
        logger.info(f"Output file: {output_path}")
        logger.info(f"Total rows processed: {len(df_process)}")
        logger.info(f"Red rows skipped: {len(df) - len(df_process)}")

    except Exception as e:
        logger.error(f"Error processing file: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()

