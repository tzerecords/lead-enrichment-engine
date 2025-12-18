# Lead Enrichment Engine - Context for Cursor Agent

## Project Overview

Python CLI script that enriches dirty B2B lead data from Excel files. Takes 500 leads every 2 weeks, validates CIF/phone/company name for ALL, finds specific emails + contacts for high-priority clients (>70 MWh consumption). Returns clean Excel preserving original format.

## Current Milestone: M1 - Setup & Foundation

**Goal:** Functional repo + Excel I/O + priority calculation working

**Status:** 

- ✅ Folder structure created
- ✅ Config files created (.env.example, requirements.txt, YAMLs)
- ⏳ Need to implement core functionality

**M1 Tasks:**

1. `src/utils/config_loader.py` - Load YAML configs
2. `src/core/excel_processor.py` - Read/write Excel preserving format
3. `src/core/priority_engine.py` - Calculate priority (reads priority_rules.yaml)
4. `src/main.py` - CLI entry point
5. `src/utils/logger.py` - Logging setup
6. `tests/sample_data.xlsx` - Test data with 10 rows

**Success criteria:** 

`python src/main.py data/input/sample_data.xlsx` reads Excel, skips red rows, calculates priorities, writes clean Excel to data/output/ with PRIORITY column added and original format preserved.

## Stack

- Python 3.11+
- pandas + openpyxl (Excel I/O)
- PyYAML (editable config)
- tqdm (progress bars)

## Critical Rules

### 1. Config is EDITABLE (NEVER hardcode)

- ALL business logic lives in `config/rules/*.yaml`
- Python code READS from YAML, never hardcodes scoring/priority logic
- Use `utils/config_loader.py` to load YAMLs

### 2. Excel Handling

- Preserve original colors with openpyxl
- OBSERVACIONES column is UNTOUCHABLE (never modify)
- Skip rows with red background color
- Keep Luz/Gas duplicates separate (same company, different service)

### 3. Code Style

- Type hints on ALL functions
- Google-style docstrings
- Robust error handling: try/except + logging
- Progress bars with tqdm for long operations

### 4. Git Commits

- Messages in English
- Format: `feat: add CIF validator` / `fix: handle missing consumo`
- Commit frequently (after each working feature)

## Architecture

```
src/
├── main.py                    # CLI entry point
├── core/
│   ├── excel_processor.py     # Read/write Excel (preserve format)
│   ├── priority_engine.py     # Calculate priority (reads YAML)
│   └── orchestrator.py        # Coordinate full flow
├── utils/
│   ├── config_loader.py       # Load YAMLs
│   └── logger.py              # Logging setup
└── config/
    ├── rules/                 # ← EDITABLE (YAML) - NO HARDCODE
    │   ├── priority_rules.yaml
    │   ├── enrichment_rules.yaml
    │   └── validation_rules.yaml
    ├── settings.py
    └── api_keys.py
```

## Excel Format Details

**Input columns:** CIF, RAZON_SOCIAL, TELEFONO, EMAIL, CONSUMO_MWH, LUZ, GAS, OBSERVACIONES

**Detect red rows:** Use openpyxl to check fill color of cells. If row has red background (any shade), skip it entirely.

**Preserve format:** When writing output Excel:

- Copy all original columns unchanged
- Add new columns at the end (PRIORITY, etc)
- Preserve cell colors/formatting with openpyxl
- OBSERVACIONES column must be identical to input

**Priority calculation:** Read from `config/rules/priority_rules.yaml`:

- Priority 4: consumo≥300 MWh + Luz + Gas
- Priority 3: consumo≥200 MWh + Luz + Gas, OR consumo≥100 MWh
- Priority 2: 70≤consumo<100 MWh
- Priority 1: consumo<70 MWh OR missing consumo

## Questions/Clarifications

- If YAML format is unclear, ask before assuming
- If Excel structure is ambiguous, ask before coding
- Always explain your plan BEFORE writing code

