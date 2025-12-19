from __future__ import annotations

from pathlib import Path
from typing import Tuple, Dict, Any

import pandas as pd

from src.core.priority_engine import PriorityEngine
from src.api_manager.tier1_enricher import Tier1Enricher
from src.api_manager.base import BatchReport
from src.utils.logger import setup_logger

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

    # 2) Tier1 enrichment
    enricher = Tier1Enricher(config_path=config_path)

    records = df_process.to_dict(orient="records")
    batch_report = enricher.enrich_batch(records)

    # Bring enriched columns back into the main DataFrame
    df_enriched = pd.DataFrame(records)
    indices = df_process.index.to_list()

    for col in df_enriched.columns:
        df_result.loc[indices, col] = df_enriched[col].values

    # Clean temporary column before returning
    if "_IS_RED_ROW" in df_result.columns:
        df_result = df_result.drop(columns=["_IS_RED_ROW"])

    return df_result, batch_report
