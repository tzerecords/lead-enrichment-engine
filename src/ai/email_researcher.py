"""Two-phase email researcher using Tavily API + OpenAI GPT-4o-mini.

Phase 1: Company validation and enrichment
Phase 2: Contact hunting (only if Phase 1 succeeds)
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from difflib import SequenceMatcher
from typing import Optional
from urllib.parse import urlparse

from tavily import TavilyClient
from openai import OpenAI

from ..api_manager.utils.logger import get_logger, log_event
from ..utils.config_loader import load_yaml_config


@dataclass
class CompanyEnrichment:
    """Company enrichment data from Phase 1."""

    razon_social_oficial: Optional[str]
    nombre_comercial: Optional[str]
    website_validado: Optional[str]
    company_exists: bool
    confidence_score: float  # 0.0-1.0
    source_url: str


@dataclass
class EmailResearchResult:
    """Result of two-phase email research using Tavily + OpenAI."""

    # Phase 1 - Company
    company_enrichment: CompanyEnrichment

    # Phase 2 - Contact
    email: Optional[str]
    contact_name: Optional[str]
    contact_position: Optional[str]
    linkedin_url: Optional[str]

    # Metadata
    source_url: str  # NOT optional - must capture
    confidence: float
    notes: str
    search_phase_reached: int  # 1 or 2
    error: Optional[str] = None


class EmailResearcher:
    """Two-phase email researcher using Tavily API + OpenAI GPT-4o-mini.

    Phase 1: Company validation and enrichment
    Phase 2: Contact hunting (only if Phase 1 succeeds)
    """

    MODEL = "gpt-4o-mini"
    MAX_RESULTS = 5
    TEMPERATURE = 0.0  # Deterministic output

    def __init__(self, tavily_api_key: str, openai_api_key: str, config_path: str = "config/rules/enrichment_rules.yaml") -> None:
        """Initialize email researcher.

        Args:
            tavily_api_key: Tavily API key.
            openai_api_key: OpenAI API key.
            config_path: Path to enrichment rules YAML.
        """
        self.tavily_client = TavilyClient(api_key=tavily_api_key)
        self.openai_client = OpenAI(api_key=openai_api_key)
        self.logger = get_logger("tier2.email_researcher")
        self.tavily_call_count = 0

        # Load config
        try:
            self.config = load_yaml_config(config_path)
            tier2_config = self.config.get("tier2_enrichment", {}).get("email_research", {})
            self.phase1_template = tier2_config.get(
                "phase1_query_template",
                "{company} {city} {website} razón social oficial business information",
            )
            self.phase2_template = tier2_config.get(
                "phase2_query_template",
                "{razon_social_oficial} {city} email contact director owner CEO gerente site:linkedin.com OR site:{website}",
            )
            self.skip_phase2_if_not_found = tier2_config.get("skip_phase2_if_company_not_found", True)
            self.confidence_threshold = self.config.get("company_enrichment", {}).get("confidence_threshold", 0.5)
        except FileNotFoundError:
            # Use defaults
            self.phase1_template = "{company} {city} {website} razón social oficial business information"
            self.phase2_template = "{razon_social_oficial} {city} email contact director owner CEO gerente site:linkedin.com OR site:{website}"
            self.skip_phase2_if_not_found = True
            self.confidence_threshold = 0.5

    def _extract_domain(self, website: Optional[str]) -> Optional[str]:
        """Extract clean domain from website URL.

        Args:
            website: Website URL (may include http://, www., etc.).

        Returns:
            Clean domain or None.
        """
        if not website:
            return None

        parsed = urlparse(website if website.startswith("http") else f"https://{website}")
        domain = parsed.netloc or parsed.path.strip("/")
        # Remove www. prefix
        if domain.startswith("www."):
            domain = domain[4:]
        return domain

    def _calculate_name_similarity(self, name1: str, name2: str) -> float:
        """Calculate similarity score between two company names.

        Args:
            name1: First company name.
            name2: Second company name.

        Returns:
            Similarity score 0.0-1.0.
        """
        return SequenceMatcher(None, name1.lower(), name2.lower()).ratio()

    def _phase1_company_enrichment(
        self, company: str, city: Optional[str], website: Optional[str]
    ) -> CompanyEnrichment:
        """Phase 1: Company validation and enrichment.

        Args:
            company: Company name from Excel.
            city: Optional city name.
            website: Optional company website URL.

        Returns:
            CompanyEnrichment with validated company data.
        """
        # Build Phase 1 query
        domain = self._extract_domain(website)
        query = self.phase1_template.format(company=company, city=city or "", website=domain or "").strip()

        try:
            self.tavily_call_count += 1
            log_event(
                self.logger,
                level=20,
                message=f"Tavily call #{self.tavily_call_count} - Phase 1: Company enrichment",
                extra={"query": query},
            )

            search_response = self.tavily_client.search(
                query=query,
                max_results=self.MAX_RESULTS,
                search_depth="advanced",
            )

            results = search_response.get("results", [])
            if not results:
                return CompanyEnrichment(
                    razon_social_oficial=None,
                    nombre_comercial=None,
                    website_validado=None,
                    company_exists=False,
                    confidence_score=0.0,
                    source_url="",
                )

            # Extract snippets and URLs
            snippets = []
            urls = []
            for result in results:
                content = result.get("content", "")
                url = result.get("url", "")
                if content:
                    snippets.append(content)
                if url:
                    urls.append(url)

            if not snippets:
                return CompanyEnrichment(
                    razon_social_oficial=None,
                    nombre_comercial=None,
                    website_validado=None,
                    company_exists=False,
                    confidence_score=0.0,
                    source_url=urls[0] if urls else "",
                )

            # Send to OpenAI for company data extraction
            combined_content = "\n\n---\n\n".join(snippets[:3])

            prompt = f"""Extract official company information from the following search results about "{company}".

Search results:
{combined_content}

Extract and return JSON with:
{{
  "razon_social_oficial": "Official registered business name (or null if not found)",
  "nombre_comercial": "Commercial/trade name (or null if not found)",
  "website_validado": "Confirmed website URL (or null if not found)",
  "company_exists": true/false,
  "confidence": 0.0-1.0 (how confident you are this matches "{company}")
}}

Return valid JSON only:"""

            try:
                response = self.openai_client.chat.completions.create(
                    model=self.MODEL,
                    messages=[
                        {
                            "role": "system",
                            "content": "You are a company data extraction assistant. Extract official company information and return only valid JSON.",
                        },
                        {"role": "user", "content": prompt},
                    ],
                    response_format={"type": "json_object"},
                    temperature=self.TEMPERATURE,
                    max_tokens=300,
                )

                content = response.choices[0].message.content.strip()
                data = json.loads(content)

                razon_social = data.get("razon_social_oficial")
                nombre_comercial = data.get("nombre_comercial")
                website_validado = data.get("website_validado")
                company_exists = data.get("company_exists", False)
                confidence = float(data.get("confidence", 0.0))

                # Calculate name similarity for additional confidence check
                if razon_social:
                    similarity = self._calculate_name_similarity(company, razon_social)
                    confidence = max(confidence, similarity)

                source_url = urls[0] if urls else ""

                return CompanyEnrichment(
                    razon_social_oficial=razon_social,
                    nombre_comercial=nombre_comercial,
                    website_validado=website_validado,
                    company_exists=company_exists,
                    confidence_score=confidence,
                    source_url=source_url,
                )

            except (json.JSONDecodeError, Exception) as exc:
                log_event(
                    self.logger,
                    level=30,
                    message="Phase 1 OpenAI extraction failed",
                    extra={"error": str(exc)},
                )
                return CompanyEnrichment(
                    razon_social_oficial=None,
                    nombre_comercial=None,
                    website_validado=website,
                    company_exists=False,
                    confidence_score=0.0,
                    source_url=urls[0] if urls else "",
                )

        except Exception as exc:
            log_event(
                self.logger,
                level=40,
                message="Phase 1 Tavily search failed",
                extra={"query": query, "error": str(exc)},
            )
            return CompanyEnrichment(
                razon_social_oficial=None,
                nombre_comercial=None,
                website_validado=website,
                company_exists=False,
                confidence_score=0.0,
                source_url="",
            )

    def _phase2_contact_hunting(
        self, company_enrichment: CompanyEnrichment, city: Optional[str]
    ) -> tuple[Optional[str], Optional[str], Optional[str], Optional[str], str]:
        """Phase 2: Contact hunting for email, name, position, LinkedIn.

        Args:
            company_enrichment: Company data from Phase 1.
            city: Optional city name.

        Returns:
            Tuple of (email, contact_name, contact_position, linkedin_url, source_url).
        """
        # Use official name if available, otherwise fallback
        company_name = company_enrichment.razon_social_oficial or company_enrichment.nombre_comercial
        if not company_name:
            return None, None, None, None, company_enrichment.source_url

        website = company_enrichment.website_validado or ""

        # Build Phase 2 query
        query = self.phase2_template.format(
            razon_social_oficial=company_name,
            city=city or "",
            website=website,
        ).strip()

        try:
            self.tavily_call_count += 1
            log_event(
                self.logger,
                level=20,
                message=f"Tavily call #{self.tavily_call_count} - Phase 2: Contact hunting",
                extra={"query": query},
            )

            search_response = self.tavily_client.search(
                query=query,
                max_results=self.MAX_RESULTS,
                search_depth="advanced",
            )

            results = search_response.get("results", [])
            if not results:
                return None, None, None, None, company_enrichment.source_url

            # Extract snippets and URLs
            snippets = []
            urls = []
            for result in results:
                content = result.get("content", "")
                url = result.get("url", "")
                if content:
                    snippets.append(content)
                if url:
                    urls.append(url)

            if not snippets:
                return None, None, None, None, company_enrichment.source_url

            # Send to OpenAI for contact extraction
            combined_content = "\n\n---\n\n".join(snippets[:3])

            prompt = f"""Extract SPECIFIC contact information from the following search results about {company_name}.

Search results:
{combined_content}

Rules:
- Extract ONLY specific person emails (e.g., firstname.lastname@, name@company.com)
- REJECT generic emails: info@, contact@, contacto@, admin@, noreply@, support@, help@, ventas@, comercial@
- Extract contact name, job title/position, and LinkedIn URL if available
- Return JSON with: {{
  "email": "found@email.com" or null,
  "contact_name": "Full name" or null,
  "contact_position": "Job title" or null,
  "linkedin_url": "LinkedIn profile URL" or null,
  "source_url": "URL where found" or null
}}

Return valid JSON only:"""

            try:
                response = self.openai_client.chat.completions.create(
                    model=self.MODEL,
                    messages=[
                        {
                            "role": "system",
                            "content": "You are a contact extraction assistant. Extract specific contact information and return only valid JSON.",
                        },
                        {"role": "user", "content": prompt},
                    ],
                    response_format={"type": "json_object"},
                    temperature=self.TEMPERATURE,
                    max_tokens=300,
                )

                content = response.choices[0].message.content.strip()
                data = json.loads(content)

                email = data.get("email")
                contact_name = data.get("contact_name")
                contact_position = data.get("contact_position")
                linkedin_url = data.get("linkedin_url")
                source_url = data.get("source_url") or (urls[0] if urls else company_enrichment.source_url)

                # Validate email is not generic
                if email:
                    local_part = email.split("@")[0].lower()
                    generic_prefixes = ["info", "contact", "contacto", "admin", "noreply", "support", "help", "ventas", "comercial"]
                    if any(local_part.startswith(prefix) for prefix in generic_prefixes):
                        email = None  # Reject generic email

                return email, contact_name, contact_position, linkedin_url, source_url

            except (json.JSONDecodeError, Exception) as exc:
                log_event(
                    self.logger,
                    level=30,
                    message="Phase 2 OpenAI extraction failed",
                    extra={"error": str(exc)},
                )
                return None, None, None, None, urls[0] if urls else company_enrichment.source_url

        except Exception as exc:
            log_event(
                self.logger,
                level=40,
                message="Phase 2 Tavily search failed",
                extra={"query": query, "error": str(exc)},
            )
            return None, None, None, None, company_enrichment.source_url

    def research_email(
        self,
        company: str,
        city: Optional[str] = None,
        website: Optional[str] = None,
    ) -> EmailResearchResult:
        """Research email using two-phase strategy.

        Args:
            company: Company name.
            city: Optional city name.
            website: Optional company website URL.

        Returns:
            EmailResearchResult with Phase 1 and Phase 2 data.
        """
        if not company or not company.strip():
            return EmailResearchResult(
                company_enrichment=CompanyEnrichment(
                    razon_social_oficial=None,
                    nombre_comercial=None,
                    website_validado=None,
                    company_exists=False,
                    confidence_score=0.0,
                    source_url="",
                ),
                email=None,
                contact_name=None,
                contact_position=None,
                linkedin_url=None,
                source_url="",
                confidence=0.0,
                notes="Empty company name",
                search_phase_reached=0,
                error="EMPTY_COMPANY_NAME",
            )

        # Phase 1: Company enrichment
        company_enrichment = self._phase1_company_enrichment(company, city, website)

        # Check if we should skip Phase 2
        if self.skip_phase2_if_not_found and not company_enrichment.company_exists:
            return EmailResearchResult(
                company_enrichment=company_enrichment,
                email=None,
                contact_name=None,
                contact_position=None,
                linkedin_url=None,
                source_url=company_enrichment.source_url,
                confidence=company_enrichment.confidence_score,
                notes="Company not found in Phase 1, skipping Phase 2",
                search_phase_reached=1,
                error="COMPANY_NOT_FOUND",
            )

        # Check confidence threshold
        if company_enrichment.confidence_score < self.confidence_threshold:
            notes = f"Low confidence ({company_enrichment.confidence_score:.2f} < {self.confidence_threshold}), flagging for manual review"
        else:
            notes = ""

        # Phase 2: Contact hunting
        email, contact_name, contact_position, linkedin_url, source_url = self._phase2_contact_hunting(
            company_enrichment, city
        )

        # Calculate overall confidence
        confidence = company_enrichment.confidence_score
        if email:
            confidence = min(1.0, confidence + 0.2)  # Boost if email found

        if not notes:
            if email:
                notes = f"Email found via Phase 2 research"
            else:
                notes = f"Phase 2 completed but no specific email found"

        return EmailResearchResult(
            company_enrichment=company_enrichment,
            email=email,
            contact_name=contact_name,
            contact_position=contact_position,
            linkedin_url=linkedin_url,
            source_url=source_url,
            confidence=confidence,
            notes=notes,
            search_phase_reached=2,
        )


def load_email_researcher_from_config() -> Optional[EmailResearcher]:
    """Helper to build EmailResearcher from config/api_keys.yaml."""
    try:
        api_keys = load_yaml_config("config/api_keys.yaml")
        tavily_key = api_keys.get("tavily_api_key")
        openai_key = api_keys.get("openai_api_key")

        if not tavily_key or not openai_key:
            logger = get_logger("tier2.email_researcher")
            log_event(
                logger,
                level=30,
                message="Missing API keys for email researcher",
                extra={"has_tavily": bool(tavily_key), "has_openai": bool(openai_key)},
            )
            return None

        return EmailResearcher(tavily_api_key=tavily_key, openai_api_key=openai_key)
    except FileNotFoundError:
        return None
    except Exception as exc:
        logger = get_logger("tier2.email_researcher")
        log_event(logger, level=40, message="Failed to load email researcher", extra={"error": str(exc)})
        return None
