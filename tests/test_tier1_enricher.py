"""Smoke tests for Tier1Enricher."""

from src.api_manager.tier1_enricher import Tier1Enricher


def test_tier1_enricher_instantiation() -> None:
    """Test that Tier1Enricher can be instantiated without errors."""
    enricher = Tier1Enricher(config_path="config/tier1_config.yaml")
    assert enricher is not None
    assert enricher.config is not None


def test_tier1_enricher_enrich_lead_basic() -> None:
    """Test that enrich_lead returns expected structure."""
    enricher = Tier1Enricher(config_path="config/tier1_config.yaml")

    # Minimal lead
    lead = {
        "CIF": "B12345678",
        "NOMBRE_EMPRESA": "Test Company",
        "CIUDAD": "Madrid",
    }

    result = enricher.enrich_lead(lead)

    # Check required columns are present
    assert "CIF" in result
    assert "CIF_VALID" in result
    assert "CIF_FORMAT_OK" in result
    assert "RAZON_SOCIAL" in result
    assert "RAZON_SOCIAL_SOURCE" in result
    assert "PHONE" in result
    assert "PHONE_VALID" in result
    assert "PHONE_TYPE" in result
    assert "PHONE_SOURCE" in result
    assert "ENRICHMENT_TIMESTAMP" in result
    assert "ERRORS" in result

    # Check types
    assert isinstance(result["CIF_VALID"], bool)
    assert isinstance(result["CIF_FORMAT_OK"], bool)
    assert isinstance(result["PHONE_VALID"], bool)
    assert isinstance(result["ERRORS"], str)


def test_tier1_enricher_enrich_batch() -> None:
    """Test that enrich_batch processes multiple leads."""
    enricher = Tier1Enricher(config_path="config/tier1_config.yaml")

    leads = [
        {"CIF": "B12345678", "NOMBRE_EMPRESA": "Company 1", "CIUDAD": "Madrid"},
        {"CIF": "A28015865", "NOMBRE_EMPRESA": "Company 2", "CIUDAD": "Barcelona"},
    ]

    report = enricher.enrich_batch(leads)

    assert report.total == 2
    assert report.cif_validated >= 0  # May be 0 if CIFs are invalid
    assert report.phone_found >= 0
    assert isinstance(report.errors, list)

    # Check that leads were modified in place
    assert len(leads) == 2
    assert "CIF_VALID" in leads[0]
    assert "CIF_VALID" in leads[1]
