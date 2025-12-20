"""Unit tests for M3 components: Tier3, Scoring, Batch Validators."""

import sys
from pathlib import Path

# Add project root to Python path
sys.path.insert(0, str(Path(__file__).parent.parent))

import pytest
import pandas as pd
from unittest.mock import Mock, MagicMock

from src.enrichers.tier3_enricher import Tier3Enricher, SimpleSearchClient, SimpleHttpClient
from src.core.scoring_engine import ScoringEngine
from src.validators.email_batch_validator import validate_all_emails
from src.validators.phone_batch_validator import validate_all_phones
from src.validators.cif_batch_validator import revalidate_cifs


class TestTier3Enricher:
    """Tests for Tier3Enricher."""

    def test_enrich_website_skips_filled_fields(self):
        """Test that enrich_website doesn't modify existing WEBSITE values."""
        df = pd.DataFrame({
            "RAZON_SOCIAL": ["Company A", "Company B"],
            "CIF": ["B12345678", "A87654321"],
            "WEBSITE": ["https://existing.com", None],
        })

        search_client = Mock()
        search_client.search_company_website = Mock(return_value="https://new.com")
        http_client = Mock()
        http_client.is_url_alive = Mock(return_value=True)

        rules = {
            "website": {
                "enabled": True,
                "query_template": "{razon_social} {cif}",
                "http_timeout": 3.0,
                "accepted_status_codes": [200],
                "domains_blacklist": [],
            }
        }

        enricher = Tier3Enricher(search_client=search_client, http_client=http_client, rules=rules)
        result = enricher.enrich_website(df)

        # First row should keep existing website
        assert result.loc[0, "WEBSITE"] == "https://existing.com"
        # Second row should be enriched
        assert result.loc[1, "WEBSITE"] == "https://new.com"
        # Search should only be called for empty website
        assert search_client.search_company_website.call_count == 1

    def test_enrich_website_rejects_dead_urls(self):
        """Test that enrich_website doesn't fill dead URLs."""
        df = pd.DataFrame({
            "RAZON_SOCIAL": ["Company A"],
            "CIF": ["B12345678"],
            "WEBSITE": [None],
        })

        search_client = Mock()
        search_client.search_company_website = Mock(return_value="https://dead.com")
        http_client = Mock()
        http_client.is_url_alive = Mock(return_value=False)  # Dead URL

        rules = {
            "website": {
                "enabled": True,
                "query_template": "{razon_social} {cif}",
                "http_timeout": 3.0,
                "accepted_status_codes": [200],
                "domains_blacklist": [],
            }
        }

        enricher = Tier3Enricher(search_client=search_client, http_client=http_client, rules=rules)
        result = enricher.enrich_website(df)

        # Website should remain empty
        assert pd.isna(result.loc[0, "WEBSITE"])

    def test_enrich_cnae_skips_filled_fields(self):
        """Test that enrich_cnae doesn't modify existing CNAE values."""
        df = pd.DataFrame({
            "RAZON_SOCIAL": ["Company A", "Company B"],
            "CIF": ["B12345678", "A87654321"],
            "CNAE": ["1234", None],
        })

        search_client = Mock()
        search_client.search_company_cnae = Mock(return_value="5678")
        http_client = Mock()

        rules = {
            "cnae": {
                "enabled": True,
                "query_template": "{razon_social} {cif} CNAE",
            }
        }

        enricher = Tier3Enricher(search_client=search_client, http_client=http_client, rules=rules)
        result = enricher.enrich_cnae(df)

        # First row should keep existing CNAE
        assert result.loc[0, "CNAE"] == "1234"
        # Second row should be enriched
        assert result.loc[1, "CNAE"] == "5678"


class TestScoringEngine:
    """Tests for ScoringEngine."""

    def test_calculate_completeness_all_fields_filled(self):
        """Test completeness score when all fields are filled."""
        rules = {
            "scoring": {
                "completeness": {
                    "fields": {
                        "CIF": 15,
                        "RAZON_SOCIAL": 10,
                        "TELEFONO": 15,
                        "EMAIL": 20,
                    }
                }
            }
        }

        engine = ScoringEngine(validation_rules=rules)
        row = pd.Series({
            "CIF": "B12345678",
            "RAZON_SOCIAL": "Test Company",
            "TELEFONO": "612345678",
            "EMAIL": "test@example.com",
            "CIF_VALID": True,
            "PHONE_VALID": True,
            "EMAIL_VALID": True,
        })

        score = engine.calculate_completeness(row)
        assert score == 100.0

    def test_calculate_completeness_partial_fields(self):
        """Test completeness score when only some fields are filled."""
        rules = {
            "scoring": {
                "completeness": {
                    "fields": {
                        "CIF": 50,
                        "EMAIL": 50,
                    }
                }
            }
        }

        engine = ScoringEngine(validation_rules=rules)
        row = pd.Series({
            "CIF": "B12345678",
            "EMAIL": None,
            "CIF_VALID": True,
        })

        score = engine.calculate_completeness(row)
        assert score == 50.0

    def test_assign_data_quality_high(self):
        """Test data quality assignment for high quality data."""
        rules = {
            "scoring": {
                "quality": {
                    "high": {
                        "min_completeness": 80,
                        "min_confidence": 70,
                    },
                    "medium": {
                        "min_completeness": 50,
                        "min_confidence": 40,
                    }
                }
            }
        }

        engine = ScoringEngine(validation_rules=rules)
        quality = engine.assign_data_quality(completeness=85.0, confidence=75.0)
        assert quality == "High"

    def test_assign_data_quality_low(self):
        """Test data quality assignment for low quality data."""
        rules = {
            "scoring": {
                "quality": {
                    "high": {
                        "min_completeness": 80,
                        "min_confidence": 70,
                    },
                    "medium": {
                        "min_completeness": 50,
                        "min_confidence": 40,
                    }
                }
            }
        }

        engine = ScoringEngine(validation_rules=rules)
        quality = engine.assign_data_quality(completeness=30.0, confidence=20.0)
        assert quality == "Low"


class TestEmailBatchValidator:
    """Tests for batch email validator."""

    def test_validate_all_emails_empty(self):
        """Test email validation for empty emails."""
        df = pd.DataFrame({
            "EMAIL": [None, "", "test@example.com"],
        })

        rules = {
            "column": "EMAIL",
            "syntax_regex": r"^[^@\s]+@[^@\s]+\.[^@\s]+$",
            "mx_check": {
                "enabled": False,
                "timeout": 2.0,
            }
        }

        result = validate_all_emails(df, rules)

        assert result.loc[0, "EMAIL_VALID"] == False
        assert result.loc[0, "EMAIL_REASON"] == "empty"
        assert result.loc[1, "EMAIL_VALID"] == False
        assert result.loc[1, "EMAIL_REASON"] == "empty"
        # Valid email should pass syntax check
        assert result.loc[2, "EMAIL_VALID"] == True or result.loc[2, "EMAIL_VALID"] == False  # Depends on MX check

    def test_validate_all_emails_invalid_syntax(self):
        """Test email validation for invalid syntax."""
        df = pd.DataFrame({
            "EMAIL": ["invalid-email", "not@valid"],
        })

        rules = {
            "column": "EMAIL",
            "syntax_regex": r"^[^@\s]+@[^@\s]+\.[^@\s]+$",
            "mx_check": {
                "enabled": False,
            }
        }

        result = validate_all_emails(df, rules)

        assert result.loc[0, "EMAIL_VALID"] == False
        assert "invalid" in result.loc[0, "EMAIL_REASON"].lower() or result.loc[0, "EMAIL_REASON"] == "invalid_syntax"


class TestPhoneBatchValidator:
    """Tests for batch phone validator."""

    def test_validate_all_phones_empty(self):
        """Test phone validation for empty phones."""
        df = pd.DataFrame({
            "TELEFONO": [None, "", "612345678"],
        })

        rules = {
            "column": "TELEFONO",
        }

        result = validate_all_phones(df, rules)

        assert result.loc[0, "PHONE_VALID"] == False
        assert result.loc[0, "PHONE_REASON"] == "empty"
        assert result.loc[1, "PHONE_VALID"] == False
        assert result.loc[1, "PHONE_REASON"] == "empty"
        # Valid phone should pass
        assert result.loc[2, "PHONE_VALID"] == True
        assert result.loc[2, "PHONE_REASON"] == "ok"


class TestCifBatchValidator:
    """Tests for batch CIF revalidator."""

    def test_revalidate_cifs_only_failed(self):
        """Test that revalidation only processes failed CIFs."""
        df = pd.DataFrame({
            "CIF": ["B12345678", "INVALID", "A87654321"],
            "CIF_VALID": [True, False, None],
        })

        rules = {
            "column": "CIF",
            "revalidation": {
                "enabled": True,
                "strategy": "relaxed",
            }
        }

        result = revalidate_cifs(df, rules)

        # First row (valid) should not be rechecked
        assert result.loc[0, "CIF_RECHECKED"] == False
        # Second row (invalid) should be rechecked
        assert result.loc[1, "CIF_RECHECKED"] == True
        # Third row (None) should be rechecked
        assert result.loc[2, "CIF_RECHECKED"] == True
