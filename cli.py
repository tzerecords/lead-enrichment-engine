from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import List, Dict, Any

import pandas as pd

from src.api_manager.tier1_enricher import Tier1Enricher


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Tier 1 lead enrichment CLI")
    parser.add_argument("--input", required=True, help="Input CSV with leads")
    parser.add_argument("--output", required=True, help="Output enriched CSV path")
    parser.add_argument("--config", default="config/tier1_config.yaml", help="Tier1 config YAML path")
    parser.add_argument("--batch-size", type=int, default=100, help="Batch size for processing")
    parser.add_argument("--dry-run", action="store_true", help="Run without calling real external APIs (future use)")
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    input_path = Path(args.input)
    output_path = Path(args.output)

    df = pd.read_csv(input_path)
    leads: List[Dict[str, Any]] = df.to_dict(orient="records")

    enricher = Tier1Enricher(config_path=args.config)
    report = enricher.enrich_batch(leads)

    enriched_df = pd.DataFrame(leads)
    enriched_df.to_csv(output_path, index=False)

    report_path = output_path.with_suffix(".report.json")
    with report_path.open("w", encoding="utf-8") as f:
        json.dump({
            "total": report.total,
            "cif_validated": report.cif_validated,
            "phone_found": report.phone_found,
            "errors": report.errors,
        }, f, ensure_ascii=False, indent=2)


if __name__ == "__main__":
    main()
