from __future__ import annotations

from src.api_manager.tier1_enricher import Tier1Enricher


def test_tier1_enricher_instantiation() -> None:
    """Basic smoke test to ensure Tier1Enricher can be constructed."""

    enricher = Tier1Enricher(config_path="config/tier1_config.yaml")
    assert enricher is not None
