from __future__ import annotations

import json
from dataclasses import dataclass
from typing import List, Dict, Any, Optional

import openai
from openai import OpenAI

from ..api_manager.utils.logger import get_logger, log_event


@dataclass
class ContactInfo:
    """Structured contact information."""

    name: str
    title: Optional[str]
    email: Optional[str]


@dataclass
class ParsedContactData:
    """Result of parsing HTML for contact information."""

    emails: List[str]
    contacts: List[ContactInfo]
    raw_response: Optional[str] = None
    error: Optional[str] = None


class OpenAIParser:
    """HTML parser using OpenAI GPT-4o-mini to extract emails and contacts.

    Uses GPT-4o-mini (cheapest model) with strict token limits.
    Rejects generic emails like info@, contact@, admin@.
    """

    MODEL = "gpt-4o-mini"
    MAX_TOKENS = 4000  # Strict limit per lead
    MAX_OUTPUT_TOKENS = 500  # Limit response size
    GENERIC_EMAIL_DOMAINS = {"info", "contact", "admin", "noreply", "no-reply", "support", "help"}

    def __init__(self, api_key: str) -> None:
        """Initialize OpenAI parser.

        Args:
            api_key: OpenAI API key.
        """
        self.client = OpenAI(api_key=api_key)
        self.logger = get_logger("tier2.openai_parser")

    def _is_generic_email(self, email: str) -> bool:
        """Check if email is generic (info@, contact@, etc.)."""
        if not email or "@" not in email:
            return True
        local_part = email.split("@")[0].lower().strip()
        return local_part in self.GENERIC_EMAIL_DOMAINS

    def _build_prompt(self, html_content: str) -> str:
        """Build the prompt for OpenAI."""
        return """Extract all specific email addresses and contact information from this company's contact page HTML.

Return a JSON object with this exact structure:
{
  "emails": ["email1@example.com", "email2@example.com"],
  "contacts": [
    {"name": "John Doe", "title": "Sales Manager", "email": "john@example.com"},
    {"name": "Jane Smith", "title": "CEO", "email": "jane@example.com"}
  ]
}

Rules:
- Ignore generic emails like info@, contact@, admin@, noreply@, support@, help@
- Only include specific person emails (e.g., firstname.lastname@, name@)
- Extract full names and job titles when available
- If a contact has no email, include them in contacts array but set email to null
- Return valid JSON only, no markdown or extra text

HTML content:
""" + html_content[:50000]  # Limit HTML to ~50k chars to stay within token limits

    def parse_html(self, html_content: str) -> ParsedContactData:
        """Parse HTML content to extract emails and contacts.

        Args:
            html_content: Raw HTML content from company contact page.

        Returns:
            ParsedContactData with emails and contacts, or error if parsing failed.
        """
        if not html_content or not html_content.strip():
            return ParsedContactData(
                emails=[],
                contacts=[],
                error="Empty HTML content",
            )

        try:
            prompt = self._build_prompt(html_content)

            # Estimate tokens (rough: 1 token ≈ 4 characters)
            estimated_tokens = len(prompt) // 4
            if estimated_tokens > self.MAX_TOKENS:
                log_event(
                    self.logger,
                    level=30,
                    message="HTML content too large, truncating",
                    extra={"estimated_tokens": estimated_tokens, "max_tokens": self.MAX_TOKENS},
                )
                # Truncate prompt to fit
                max_chars = self.MAX_TOKENS * 4
                prompt = prompt[:max_chars]

            response = self.client.chat.completions.create(
                model=self.MODEL,
                messages=[
                    {
                        "role": "system",
                        "content": "You are a data extraction assistant. Extract contact information from HTML and return only valid JSON.",
                    },
                    {"role": "user", "content": prompt},
                ],
                max_tokens=self.MAX_OUTPUT_TOKENS,
                temperature=0.1,  # Low temperature for consistent JSON output
            )

            content = response.choices[0].message.content.strip()

            # Try to extract JSON from response (may have markdown code blocks)
            if "```json" in content:
                content = content.split("```json")[1].split("```")[0].strip()
            elif "```" in content:
                content = content.split("```")[1].split("```")[0].strip()

            data = json.loads(content)

            # Parse emails (filter out generic ones)
            emails = []
            if isinstance(data.get("emails"), list):
                for email in data.get("emails", []):
                    if email and isinstance(email, str) and not self._is_generic_email(email):
                        emails.append(email.lower().strip())

            # Parse contacts
            contacts = []
            if isinstance(data.get("contacts"), list):
                for contact in data.get("contacts", []):
                    if not isinstance(contact, dict):
                        continue
                    name = contact.get("name", "").strip()
                    if not name:
                        continue

                    email = contact.get("email")
                    if email:
                        email = email.strip().lower()
                        if self._is_generic_email(email):
                            email = None

                    contacts.append(
                        ContactInfo(
                            name=name,
                            title=contact.get("title"),
                            email=email,
                        )
                    )

            return ParsedContactData(
                emails=emails,
                contacts=contacts,
                raw_response=content,
            )

        except json.JSONDecodeError as exc:
            log_event(
                self.logger,
                level=40,
                message="Failed to parse OpenAI JSON response",
                extra={"error": str(exc)},
            )
            return ParsedContactData(
                emails=[],
                contacts=[],
                error=f"JSON_PARSE_ERROR: {str(exc)}",
            )

        except Exception as exc:
            log_event(
                self.logger,
                level=40,
                message="OpenAI parsing failed",
                extra={"error": str(exc)},
            )
            return ParsedContactData(
                emails=[],
                contacts=[],
                error=f"OPENAI_ERROR: {str(exc)}",
            )


def load_openai_parser_from_config() -> Optional[OpenAIParser]:
    """Helper to build OpenAIParser from config/api_keys.yaml."""
    from ..utils.config_loader import load_yaml_config

    try:
        api_keys = load_yaml_config("config/api_keys.yaml")
        openai_key = api_keys.get("openai_api_key")
        if not openai_key:
            return None
        return OpenAIParser(api_key=openai_key)
    except FileNotFoundError:
        return None
    except Exception as exc:
        logger = get_logger("tier2.openai_parser")
        log_event(logger, level=40, message="Failed to load OpenAI parser", extra={"error": str(exc)})
        return None
