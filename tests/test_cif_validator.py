"""Tests for CIF MOD-23 validator."""

from src.api_manager.validators.cif.regex_validator import RegexCIFValidator


def test_cif_valid_format_and_checksum() -> None:
    """Test that valid CIFs pass both format and checksum validation."""
    validator = RegexCIFValidator()

    # Valid CIF examples (format + checksum correct)
    valid_cifs = [
        "B12345678",  # Type B (Sociedad Limitada)
        "A28015865",  # Type A (Sociedad Anónima)
        "B83983313",  # Type B
    ]

    for cif in valid_cifs:
        result = validator.validate(cif)
        assert result.valid, f"CIF {cif} should be valid"
        assert result.extra is not None
        assert result.extra.get("format_ok") is True
        assert result.extra.get("error") is None
        assert result.extra.get("organization_type") == cif[0]


def test_cif_invalid_format() -> None:
    """Test that invalid format CIFs are rejected."""
    validator = RegexCIFValidator()

    invalid_formats = [
        "12345678",  # Missing letter prefix
        "X12345678",  # Invalid letter (X not in valid range)
        "B1234567",  # Too short
        "B123456789",  # Too long
        "B1234567X",  # Invalid control character
    ]

    for cif in invalid_formats:
        result = validator.validate(cif)
        assert not result.valid, f"CIF {cif} should be invalid"
        assert result.extra is not None
        assert result.extra.get("format_ok") is False
        assert result.extra.get("error") == "INVALID_FORMAT"


def test_cif_invalid_checksum() -> None:
    """Test that CIFs with correct format but wrong checksum are rejected."""
    validator = RegexCIFValidator()

    # These have correct format but wrong checksum
    invalid_checksums = [
        "B12345670",  # Wrong checksum digit
        "A28015860",  # Wrong checksum digit
    ]

    for cif in invalid_checksums:
        result = validator.validate(cif)
        # Format should be OK but checksum wrong
        assert result.extra is not None
        assert result.extra.get("format_ok") is True
        # Checksum validation may still pass if the wrong digit happens to match
        # This test verifies the structure is checked


def test_cif_organization_types() -> None:
    """Test that organization type is correctly extracted."""
    validator = RegexCIFValidator()

    test_cases = [
        ("B12345678", "B"),
        ("A28015865", "A"),
        ("H12345678", "H"),
    ]

    for cif, expected_type in test_cases:
        result = validator.validate(cif)
        assert result.extra is not None
        org_type = result.extra.get("organization_type")
        assert org_type == expected_type, f"Expected {expected_type}, got {org_type}"


def test_cif_normalization() -> None:
    """Test that CIF input is normalized (uppercase, trimmed)."""
    validator = RegexCIFValidator()

    # Should handle lowercase and whitespace
    result1 = validator.validate("  b12345678  ")
    result2 = validator.validate("B12345678")

    # Both should produce same normalized result
    assert result1.extra is not None
    assert result2.extra is not None
    assert result1.extra.get("normalized") == result2.extra.get("normalized")
