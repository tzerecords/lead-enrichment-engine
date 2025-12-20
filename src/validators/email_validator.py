from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Optional

import dns.resolver
import dns.exception

from ..api_manager.utils.logger import get_logger, log_event


@dataclass
class EmailValidationResult:
    """Result of email validation."""

    valid: bool  # Syntax is valid
    deliverable: bool  # MX record exists
    generic: bool  # Is generic email (info@, contact@, etc.)
    error: Optional[str] = None  # Error message if validation failed


# Email regex pattern
# Allows: letters, numbers, +, -, _, . in local part
# Requires: @ symbol
# Domain: letters, numbers, -, . (at least one dot for TLD)
# TLD: at least 2 characters
EMAIL_REGEX = re.compile(
    r"^[a-zA-Z0-9][a-zA-Z0-9._+-]*@[a-zA-Z0-9]([a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?(\.[a-zA-Z0-9]([a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?)*\.[a-zA-Z]{2,}$"
)

# Generic email local parts to reject
GENERIC_LOCAL_PARTS = {
    "info",
    "contact",
    "contacto",
    "admin",
    "administrator",
    "noreply",
    "no-reply",
    "support",
    "soporte",
    "help",
    "ayuda",
    "ventas",
    "comercial",
    "sales",
    "marketing",
    "webmaster",
    "postmaster",
    "abuse",
    "privacy",
    "legal",
}


class EmailValidator:
    """Email validator with syntax, MX record, and generic email detection.

    Validates:
    - Email syntax (regex)
    - MX record existence (DNS lookup)
    - Generic email detection (info@, contact@, etc.)
    """

    def __init__(self, dns_timeout: float = 5.0) -> None:
        """Initialize email validator.

        Args:
            dns_timeout: DNS query timeout in seconds (default: 5.0).
        """
        self.dns_timeout = dns_timeout
        self.logger = get_logger("tier2.email_validator")

    def _validate_syntax(self, email: str) -> bool:
        """Validate email syntax using regex.

        Args:
            email: Email address to validate.

        Returns:
            True if syntax is valid, False otherwise.
        """
        if not email or not isinstance(email, str):
            return False

        email = email.strip().lower()
        return bool(EMAIL_REGEX.match(email))

    def _check_mx_record(self, domain: str) -> tuple[bool, Optional[str]]:
        """Check if domain has MX records (email deliverable).

        Args:
            domain: Domain name to check (without @).

        Returns:
            Tuple of (has_mx, error_message).
            has_mx: True if MX record exists, False otherwise.
            error_message: Error message if DNS query failed, None if successful.
        """
        try:
            # Query MX records
            answers = dns.resolver.resolve(domain, "MX", lifetime=self.dns_timeout)
            if answers:
                mx_records = [str(rdata.exchange) for rdata in answers]
                log_event(
                    self.logger,
                    level=10,
                    message="MX records found",
                    extra={"domain": domain, "mx_count": len(mx_records)},
                )
                return True, None

            return False, None

        except dns.resolver.NoAnswer:
            # No MX record found
            log_event(
                self.logger,
                level=20,
                message="No MX record found",
                extra={"domain": domain},
            )
            return False, None

        except dns.resolver.NXDOMAIN:
            # Domain doesn't exist
            return False, "DOMAIN_NOT_FOUND"

        except dns.resolver.Timeout:
            log_event(
                self.logger,
                level=30,
                message="DNS query timeout",
                extra={"domain": domain},
            )
            return False, "DNS_TIMEOUT"

        except dns.exception.DNSException as exc:
            log_event(
                self.logger,
                level=30,
                message="DNS query failed",
                extra={"domain": domain, "error": str(exc)},
            )
            return False, f"DNS_ERROR: {str(exc)}"

        except Exception as exc:
            log_event(
                self.logger,
                level=40,
                message="Unexpected error checking MX",
                extra={"domain": domain, "error": str(exc)},
            )
            return False, f"UNEXPECTED_ERROR: {str(exc)}"

    def _is_generic_email(self, email: str) -> bool:
        """Check if email is generic (info@, contact@, etc.).

        Args:
            email: Email address to check.

        Returns:
            True if email is generic, False otherwise.
        """
        if not email or "@" not in email:
            return True

        local_part = email.split("@")[0].lower().strip()

        # Check exact match
        if local_part in GENERIC_LOCAL_PARTS:
            return True

        # Check if starts with generic prefix (e.g., "info2", "contacto1")
        for generic in GENERIC_LOCAL_PARTS:
            if local_part.startswith(generic) and (
                local_part == generic or local_part[len(generic) :].isdigit()
            ):
                return True

        return False

    def validate(self, email: str) -> EmailValidationResult:
        """Validate email address comprehensively.

        Args:
            email: Email address to validate.

        Returns:
            EmailValidationResult with validation details.
        """
        if not email:
            return EmailValidationResult(
                valid=False,
                deliverable=False,
                generic=True,
                error="EMPTY_EMAIL",
            )

        email = email.strip().lower()

        # Step 1: Syntax validation
        if not self._validate_syntax(email):
            return EmailValidationResult(
                valid=False,
                deliverable=False,
                generic=False,
                error="INVALID_SYNTAX",
            )

        # Step 2: Generic email detection
        is_generic = self._is_generic_email(email)
        if is_generic:
            return EmailValidationResult(
                valid=True,
                deliverable=False,  # Don't check MX for generic emails
                generic=True,
                error="GENERIC_EMAIL",
            )

        # Step 3: MX record check (only for non-generic emails)
        domain = email.split("@")[1]
        has_mx, mx_error = self._check_mx_record(domain)

        return EmailValidationResult(
            valid=True,
            deliverable=has_mx,
            generic=False,
            error=mx_error if not has_mx else None,
        )


def load_email_validator_from_config() -> EmailValidator:
    """Helper to create EmailValidator with default settings."""
    return EmailValidator(dns_timeout=5.0)
