from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Optional, Dict, Any, List


@dataclass
class CIFResult:
    """Result of CIF validation.

    Attributes:
        valid: Whether the CIF format/checksum is valid.
        exists: Whether the CIF exists in official sources.
        razon_social: Official company name if available.
        source: Provider that produced this result.
        estado: Company state (e.g., ACTIVA, BAJA, CONCURSO, UNKNOWN).
        extra: Optional provider-specific data.
    """

    valid: bool
    exists: bool
    razon_social: Optional[str]
    source: str
    estado: Optional[str] = None
    extra: Optional[Dict[str, Any]] = None


@dataclass
class PhoneResult:
    """Result of phone discovery for a company.

    Attributes:
        phone: Discovered phone number in raw format.
        confidence: Confidence score between 0 and 1.
        source: Provider that produced this result.
        extra: Optional provider-specific data.
    """

    phone: Optional[str]
    confidence: float
    source: str
    extra: Optional[Dict[str, Any]] = None


@dataclass
class PhoneValidation:
    """Validation result for a phone number.

    Attributes:
        valid: Whether the phone number is considered valid.
        formatted: Normalized phone number (e.g., E.164).
        type: Phone type (MOBILE, FIXED_LINE, UNKNOWN, ...).
        carrier: Carrier name if available.
        active: Whether the number appears active (if provider supports it).
        extra: Optional provider-specific data.
    """

    valid: bool
    formatted: Optional[str]
    type: str
    carrier: Optional[str] = None
    active: Optional[bool] = None
    extra: Optional[Dict[str, Any]] = None


@dataclass
class CompanyData:
    """Enriched company data.

    Attributes:
        razon_social: Official legal name.
        address: Postal address if available.
        cnae: CNAE code.
        employees: Approximate number of employees.
        source: Provider that produced this data.
        extra: Optional provider-specific data.
    """

    razon_social: Optional[str]
    address: Optional[str]
    cnae: Optional[str]
    employees: Optional[int]
    source: str
    extra: Optional[Dict[str, Any]] = None


@dataclass
class BatchReport:
    """Aggregated statistics for a batch enrichment run."""

    total: int
    cif_validated: int
    phone_found: int
    errors: List[str]
    provider_calls: Dict[str, int]


class CIFValidator(ABC):
    """Abstract base class for CIF validators."""

    @abstractmethod
    def validate(self, cif: str) -> CIFResult:
        """Validate a CIF and return structured result."""
        raise NotImplementedError


class PhoneFinder(ABC):
    """Abstract base class for discovering company phone numbers."""

    @abstractmethod
    def find(self, company_name: str, address: Optional[str] = None) -> PhoneResult:
        """Find a phone number for the given company."""
        raise NotImplementedError


class PhoneValidator(ABC):
    """Abstract base class for validating phone numbers."""

    @abstractmethod
    def validate(self, phone: str) -> PhoneValidation:
        """Validate and normalize a phone number."""
        raise NotImplementedError


class CompanyEnricher(ABC):
    """Abstract base class for enriching company data from CIF."""

    @abstractmethod
    def enrich(self, cif: str) -> CompanyData:
        """Enrich company information based on CIF."""
        raise NotImplementedError
