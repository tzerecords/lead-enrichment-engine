"""Test script for two-phase email research functionality."""

import sys

sys.path.insert(0, ".")
from src.ai.email_researcher import load_email_researcher_from_config

print("Loading email researcher from config...")
researcher = load_email_researcher_from_config()

if not researcher:
    print("❌ Email researcher not available (missing API keys)")
    print("Please check config/api_keys.yaml has both tavily_api_key and openai_api_key")
    sys.exit(1)

print("✅ Email researcher loaded successfully\n")

# Test with a company
print("Researching email for: La Fabrique, Barcelona, lafabrique.cat")
print("=" * 60)

result = researcher.research_email(
    company="La Fabrique",
    city="Barcelona",
    website="lafabrique.cat",
)

# Phase 1 - Company Enrichment
print("\n📋 PHASE 1 - Company Enrichment:")
print("-" * 60)
company = result.company_enrichment
print(f"Razón Social Oficial: {company.razon_social_oficial or 'Not found'}")
print(f"Nombre Comercial: {company.nombre_comercial or 'Not found'}")
print(f"Website Validado: {company.website_validado or 'Not found'}")
print(f"Company Exists: {company.company_exists}")
print(f"Confidence Score: {company.confidence_score:.2f}")
print(f"Source URL: {company.source_url or 'N/A'}")

# Phase 2 - Contact Hunting
print(f"\n📧 PHASE 2 - Contact Hunting (Phase reached: {result.search_phase_reached}):")
print("-" * 60)
print(f"Email: {result.email or 'Not found'}")
print(f"Contact Name: {result.contact_name or 'Not found'}")
print(f"Contact Position: {result.contact_position or 'Not found'}")
print(f"LinkedIn URL: {result.linkedin_url or 'Not found'}")
print(f"Source URL: {result.source_url or 'N/A'}")
print(f"Overall Confidence: {result.confidence:.2f}")
print(f"Notes: {result.notes or 'N/A'}")

if result.error:
    print(f"\n⚠️  Error: {result.error}")

# Summary
print("\n" + "=" * 60)
if result.email:
    print("✅ Email research successful!")
    print(f"   Found email: {result.email}")
    if result.contact_name:
        print(f"   Contact: {result.contact_name} ({result.contact_position or 'N/A'})")
else:
    print("❌ No email found")
    if result.search_phase_reached < 2:
        print(f"   Stopped at Phase {result.search_phase_reached}")

print(f"\nTavily calls used: {researcher.tavily_call_count}")
