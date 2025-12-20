from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Any, List, Optional

import os
from ..scrapers.web_scraper import ContactPageScraper
from ..ai.openai_parser import OpenAIParser
from ..ai.email_researcher import EmailResearcher, load_email_researcher_from_config
from ..validators.email_validator import EmailValidator
from ..scrapers.linkedin_scraper import LinkedInScraper
from ..api_manager.utils.logger import get_logger, log_event
from ..utils.config_loader import load_yaml_config


@dataclass
class Tier2EnrichmentResult:
    """Result of Tier2 enrichment for a single lead."""

    email_specific: Optional[str]
    email_valid: bool
    contact_name: Optional[str]
    contact_title: Optional[str]
    linkedin_company: Optional[str]
    email_researched: Optional[str]  # From Tavily+OpenAI research
    email_source: Optional[str]  # Source URL of researched email
    email_confidence: float  # Confidence score 0.0-1.0
    research_notes: Optional[str]  # Notes from research
    errors: List[str]
    openai_tokens_used: int = 0


@dataclass
class Tier2BatchReport:
    """Aggregated statistics for Tier2 batch enrichment."""

    total: int
    emails_found: int
    emails_researched: int  # Emails found via Tavily+OpenAI
    linkedin_found: int
    contacts_found: int
    total_openai_tokens: int
    errors: List[str]


class Tier2Enricher:
    """Tier2 enricher for priority>=2 leads.

    Enriches with:
    - Specific emails (from contact pages)
    - Contact names and titles
    - LinkedIn company URLs
    - Email validation (MX records)
    """

    def __init__(self, config_path: str = "config/tier2_config.yaml") -> None:
        """Initialize Tier2 enricher.

        Args:
            config_path: Path to Tier2 config YAML.
        """
        self.config_path = config_path
        self.logger = get_logger("tier2.enricher")

        # Load config (fallback to tier1_config if tier2 doesn't exist)
        try:
            self.config = load_yaml_config(config_path)
        except FileNotFoundError:
            self.logger.warning(f"Tier2 config not found at {config_path}, using defaults")
            self.config = {}

        tier2_config = self.config.get("tier2", {})

        # Initialize components
        self.scraper = ContactPageScraper(
            timeout=tier2_config.get("timeout", 10),
            max_redirects=5,
        )

        # Load OpenAI parser
        from ..ai.openai_parser import load_openai_parser_from_config

        self.openai_parser = load_openai_parser_from_config()
        if not self.openai_parser:
            self.logger.warning("OpenAI parser not available (missing API key)")

        self.email_validator = EmailValidator(dns_timeout=5.0)
        self.linkedin_scraper = LinkedInScraper(timeout=15)

        # Load email researcher (for priority>=3)
        self.email_researcher = load_email_researcher_from_config()
        if not self.email_researcher:
            self.logger.warning("Email researcher not available (missing API keys)")
        
        # Log API key status
        openai_key = os.getenv('OPENAI_API_KEY')
        tavily_key = os.getenv('TAVILY_API_KEY')
        self.logger.info(f"API Keys Status - OpenAI: {bool(openai_key)}, Tavily: {bool(tavily_key)}")
        self.logger.info(f"Email researcher loaded: {self.email_researcher is not None}")

        # Track OpenAI usage
        self.total_tokens = 0

    def enrich_lead(self, lead: Dict[str, Any], enable_email_research: bool = False) -> Tier2EnrichmentResult:
        """Enrich a single lead with Tier2 data.

        Args:
            lead: Lead dictionary with at least: WEBSITE, NOMBRE_EMPRESA, RAZON_SOCIAL, PRIORITY.
            enable_email_research: If True, use Tavily+OpenAI email research for priority>=3.

        Returns:
            Tier2EnrichmentResult with enriched data.
        """
        website = str(lead.get("WEBSITE", "")).strip() or None
        
        # Extract company name from multiple possible column names
        company_name = (
            str(lead.get('RAZON_SOCIAL', '') or '') or 
            str(lead.get('NOMBRE CLIENTE', '') or '') or  # Excel de Alejandro
            str(lead.get('NOMBRE_CLIENTE', '') or '') or  # Sin espacio
            str(lead.get('NOMBRE_EMPRESA', '') or '') or
            str(lead.get('company_name', '') or '') or
            str(lead.get('NOMBRE', '') or '') or
            ''
        ).strip()
        
        city = str(lead.get("CIUDAD", "")).strip() or None
        priority = lead.get("PRIORITY")
        if priority is not None:
            try:
                priority = int(priority)
            except (ValueError, TypeError):
                priority = None

        errors: List[str] = []
        email_specific: Optional[str] = None
        email_valid = False
        contact_name: Optional[str] = None
        contact_title: Optional[str] = None
        linkedin_company: Optional[str] = None
        email_researched: Optional[str] = None
        email_source: Optional[str] = None
        email_confidence = 0.0
        research_notes: Optional[str] = None
        tokens_used = 0

        # Step 1: Scrape contact page
        html_content: Optional[str] = None
        if website:
            try:
                page = self.scraper.scrape_contact_page(website)
                if page.success and page.html:
                    html_content = page.html
                else:
                    errors.append(f"SCRAPE_FAILED:{page.error or 'UNKNOWN'}")
            except Exception as exc:
                errors.append(f"SCRAPE_ERROR:{str(exc)}")
        else:
            errors.append("NO_WEBSITE")

        # Step 2: Parse HTML with OpenAI (if we have content and parser)
        contacts_data = None
        if html_content and self.openai_parser:
            try:
                contacts_data = self.openai_parser.parse_html(html_content)
                if contacts_data.error:
                    errors.append(f"OPENAI_PARSE:{contacts_data.error}")
                else:
                    # Estimate tokens (rough: 1 token ≈ 4 chars)
                    tokens_used = len(html_content) // 4
                    self.total_tokens += tokens_used

                    # Get first specific email
                    if contacts_data.emails:
                        email_specific = contacts_data.emails[0]

                    # Get first contact with name
                    if contacts_data.contacts:
                        first_contact = contacts_data.contacts[0]
                        contact_name = first_contact.name
                        contact_title = first_contact.title
                        # Prefer contact's email if available
                        if first_contact.email:
                            email_specific = first_contact.email

            except Exception as exc:
                errors.append(f"OPENAI_ERROR:{str(exc)}")

        # Step 3: Validate email (if we found one)
        if email_specific:
            try:
                validation = self.email_validator.validate(email_specific)
                email_valid = validation.valid and validation.deliverable and not validation.generic
                if not email_valid:
                    if validation.generic:
                        errors.append("EMAIL_GENERIC")
                    elif not validation.deliverable:
                        errors.append(f"EMAIL_NO_MX:{validation.error or 'UNKNOWN'}")
            except Exception as exc:
                errors.append(f"EMAIL_VALIDATION_ERROR:{str(exc)}")

        # Step 4: Email research with Tavily+OpenAI (for priority>=3, if enabled)
        self.logger.debug(
            f"Email research check - enable_email_research={enable_email_research}, "
            f"priority={priority}, email_researcher={self.email_researcher is not None}"
        )
        
        if enable_email_research:
            self.logger.debug(f"ENTRANDO en bloque de email research para: {company_name}")
            if priority is None:
                self.logger.debug(f"  → Saltando: priority es None")
            elif priority < 3:
                self.logger.debug(f"  → Saltando: priority={priority} < 3")
            elif not self.email_researcher:
                self.logger.warning(f"  → Saltando: email_researcher no disponible")
            else:
                self.logger.info(f"  → EJECUTANDO email research para: {company_name} (priority={priority})")
        
        if enable_email_research and priority is not None and priority >= 3 and self.email_researcher:
            # Skip if company name is empty
            if not company_name:
                self.logger.debug(f"Skipping email research: empty company name (CIF: {lead.get('CIF/NIF', 'N/A')})")
            else:
                try:
                    self.logger.info(f"🔍 Researching email for: '{company_name}' (priority={priority})")
                    self.logger.info(f"Llamando email_researcher.research_email() para: {company_name}")
                    research_result = self.email_researcher.research_email(
                        company=company_name,
                        city=city,
                        website=website,
                    )
                    self.logger.info(f"Email research completado para: {company_name}, resultado: email={research_result.email is not None}")

                    # Extract company enrichment data (Phase 1)
                    company_enrichment = research_result.company_enrichment
                    if company_enrichment:
                        # Add company validation info to research_notes
                        # Initialize research_notes as empty string to avoid None concatenation
                        if research_notes is None:
                            research_notes = ""
                        
                        if company_enrichment.razon_social_oficial:
                            if research_notes:
                                research_notes += " | "
                            research_notes += f"Razón social: {company_enrichment.razon_social_oficial}"
                            if company_enrichment.nombre_comercial:
                                research_notes += f" | Nombre comercial: {company_enrichment.nombre_comercial}"
                        if company_enrichment.confidence_score < 0.5:
                            if research_notes:
                                research_notes += " | "
                            research_notes += f"⚠️ Baja confianza ({company_enrichment.confidence_score:.2f}) - revisar manualmente"

                    if research_result.email:
                        email_researched = research_result.email
                        email_source = research_result.source_url
                        email_confidence = research_result.confidence
                        if research_result.notes:
                            # If we already have research_notes, append to it; otherwise use notes directly
                            if research_notes:
                                research_notes += f" | {research_result.notes}"
                            else:
                                research_notes = research_result.notes

                        # If we didn't find email from scraping, use researched email
                        if not email_specific:
                            email_specific = email_researched
                            # Validate researched email
                            try:
                                validation = self.email_validator.validate(email_researched)
                                email_valid = validation.valid and validation.deliverable and not validation.generic
                            except Exception:
                                pass  # Keep email_valid as False

                        # Use contact info from research if available
                        if research_result.contact_name and not contact_name:
                            contact_name = research_result.contact_name
                        if research_result.contact_position and not contact_title:
                            contact_title = research_result.contact_position

                        # Estimate tokens (rough: ~500 tokens per research call)
                        tokens_used += 500
                        self.total_tokens += 500

                    elif research_result.error:
                        errors.append(f"EMAIL_RESEARCH:{research_result.error}")

                except Exception as exc:
                    import traceback
                    self.logger.error(
                        f"Email research failed para {company_name}: {str(exc)}",
                        exc_info=True
                    )
                    self.logger.error(f"Traceback completo:\n{traceback.format_exc()}")
                    log_event(
                        self.logger,
                        level=30,
                        message="Email research failed (non-blocking)",
                        extra={"company": company_name, "error": str(exc)},
                    )
                    errors.append(f"EMAIL_RESEARCH_ERROR:{str(exc)}")
        else:
            if enable_email_research:
                self.logger.debug(f"SALTANDO email research para {company_name} - condiciones no cumplidas")

        # Step 5: Find LinkedIn (if we have company name) - optional, don't block on errors
        if company_name:
            try:
                linkedin_result = self.linkedin_scraper.find_company(company_name)
                if linkedin_result.success and linkedin_result.company_url:
                    linkedin_company = linkedin_result.company_url
                elif linkedin_result.error:
                    # Log but don't add to errors (LinkedIn is optional)
                    log_event(
                        self.logger,
                        level=20,
                        message="LinkedIn not found (optional)",
                        extra={"company": company_name, "error": linkedin_result.error},
                    )
            except Exception as exc:
                # Log but don't block processing
                log_event(
                    self.logger,
                    level=30,
                    message="LinkedIn scraper error (non-blocking)",
                    extra={"company": company_name, "error": str(exc)},
                )

        return Tier2EnrichmentResult(
            email_specific=email_specific,
            email_valid=email_valid,
            contact_name=contact_name,
            contact_title=contact_title,
            linkedin_company=linkedin_company,
            email_researched=email_researched,
            email_source=email_source,
            email_confidence=email_confidence,
            research_notes=research_notes,
            errors=errors,
            openai_tokens_used=tokens_used,
        )

    def enrich_batch(self, leads: List[Dict[str, Any]], enable_email_research: bool = False) -> Tier2BatchReport:
        """Enrich a batch of leads (should be filtered to priority>=2).

        Args:
            leads: List of lead dictionaries.
            enable_email_research: If True, use Tavily+OpenAI email research for priority>=3.

        Returns:
            Tier2BatchReport with aggregate statistics.
        """
        self.logger.info(f"enrich_batch called with enable_email_research={enable_email_research}, total leads={len(leads)}")
        
        # Verify API keys
        openai_key = os.getenv('OPENAI_API_KEY')
        tavily_key = os.getenv('TAVILY_API_KEY')
        self.logger.info(f"API Keys Status - OpenAI configured: {bool(openai_key)}, Tavily configured: {bool(tavily_key)}")
        self.logger.info(f"Email researcher available: {self.email_researcher is not None}")
        
        if enable_email_research:
            self.logger.info("ENTRANDO en bloque de email research")
            priority_3_count = sum(1 for lead in leads if lead.get("PRIORITY") is not None and int(lead.get("PRIORITY", 0)) >= 3)
            self.logger.info(f"Starting email research for {priority_3_count} priority>=3 leads (out of {len(leads)} total)")
            
            # Log sample priorities for debugging
            sample_priorities = [lead.get("PRIORITY") for lead in leads[:5]]
            self.logger.debug(f"Sample priorities (first 5): {sample_priorities}")
        else:
            self.logger.info("SALTANDO email research (enable_email_research=False)")
        
        try:
            from tqdm import tqdm

            iterator = tqdm(leads, desc="Tier2 enrichment")
        except Exception:
            iterator = leads

        total = len(leads)
        emails_found = 0
        emails_researched = 0
        linkedin_found = 0
        contacts_found = 0
        errors: List[str] = []

        for idx, lead in enumerate(iterator):
            if enable_email_research and idx < 3:  # Log first 3 leads for debugging
                self.logger.debug(
                    f"Processing lead {idx+1}: company={lead.get('NOMBRE_EMPRESA') or lead.get('RAZON_SOCIAL')}, "
                    f"priority={lead.get('PRIORITY')}"
                )
            result = self.enrich_lead(lead, enable_email_research=enable_email_research)
            
            if enable_email_research and result.email_researched:
                self.logger.info(f"Email encontrado via research: {result.email_researched} para lead {idx+1}")

            # Update lead dict in place
            lead["EMAIL_SPECIFIC"] = result.email_specific
            lead["EMAIL_VALID"] = result.email_valid
            lead["CONTACT_NAME"] = result.contact_name
            lead["CONTACT_TITLE"] = result.contact_title
            lead["LINKEDIN_COMPANY"] = result.linkedin_company
            lead["EMAIL_RESEARCHED"] = result.email_researched
            lead["EMAIL_SOURCE"] = result.email_source
            lead["EMAIL_CONFIDENCE"] = result.email_confidence
            lead["RESEARCH_NOTES"] = result.research_notes
            lead["TIER2_ERRORS"] = ",".join(result.errors) if result.errors else ""

            # Count successes
            if result.email_specific:
                emails_found += 1
            if result.email_researched:
                emails_researched += 1
            if result.linkedin_company:
                linkedin_found += 1
            if result.contact_name:
                contacts_found += 1
            if result.errors:
                errors.extend(result.errors)

        # Log email research completion and costs
        if enable_email_research:
            self.logger.info(f"Email research complete: {emails_researched}/{total} leads")
            # Estimate cost (GPT-4o-mini: $0.15 per 1M input tokens, $0.60 per 1M output tokens)
            # Rough estimate: 90% input, 10% output
            input_cost = (self.total_tokens * 0.9) / 1_000_000 * 0.15
            output_cost = (self.total_tokens * 0.1) / 1_000_000 * 0.60
            total_cost = input_cost + output_cost
            self.logger.info(f"OpenAI tokens used: {self.total_tokens:,}, estimated cost: ${total_cost:.4f}")

        return Tier2BatchReport(
            total=total,
            emails_found=emails_found,
            emails_researched=emails_researched,
            linkedin_found=linkedin_found,
            contacts_found=contacts_found,
            total_openai_tokens=self.total_tokens,
            errors=errors,
        )
