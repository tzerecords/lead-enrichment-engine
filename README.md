# Lead Enrichment Engine

Python tool (CLI + Web API) that enriches dirty B2B lead data from Excel files. Validates CIF/phone/company name, calculates priorities, and returns clean Excel preserving original format.

**CLI** = Command Line Interface (interfaz de línea de comandos) - usar desde terminal  
**Web API** = Interfaz web - subir Excel desde navegador/app y recibir Excel procesado

## Setup

1. **Create virtual environment:**
```bash
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

2. **Install dependencies:**
```bash
pip install -r requirements.txt
```

## Usage

### CLI (Command Line Interface)

Process an Excel file from terminal:
```bash
python src/main.py data/input/sample_data.xlsx
```

Output will be saved to `data/output/LIMPIO_<input_name>.xlsx`

### Web API

Start the API server:
```bash
python run_api.py
```

The API will be available at `http://localhost:8000`

**Endpoints:**
- `GET /` - Health check
- `GET /health` - Health check
- `POST /process` - Upload Excel file, get processed Excel back

**Example with curl:**
```bash
curl -X POST "http://localhost:8000/process" \
  -H "accept: application/json" \
  -H "Content-Type: multipart/form-data" \
  -F "file=@tests/sample_data.xlsx" \
  --output LIMPIO_output.xlsx
```

**API Documentation:**
- Swagger UI: `http://localhost:8000/docs`
- ReDoc: `http://localhost:8000/redoc`

## Features

- ✅ Reads Excel files preserving format
- ✅ Detects and skips rows with red background
- ✅ Calculates priority based on consumption and services (from YAML config)
- ✅ Preserves original Excel formatting
- ✅ Adds PRIORITY column to output

## Project Structure

```
src/
├── main.py                    # CLI entry point
├── core/
│   ├── excel_processor.py     # Read/write Excel (preserve format)
│   └── priority_engine.py     # Calculate priority (reads YAML)
├── utils/
│   ├── config_loader.py       # Load YAMLs
│   └── logger.py              # Logging setup
└── config/
    └── rules/
        └── priority_rules.yaml # Priority calculation rules
```

## Configuration

Priority rules are defined in `config/rules/priority_rules.yaml`. Edit this file to change priority calculation logic.

## Testing

Test with sample data:
```bash
python src/main.py tests/sample_data.xlsx
```

