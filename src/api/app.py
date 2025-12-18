"""FastAPI web application for Lead Enrichment Engine."""

import io
from pathlib import Path
from typing import Optional
import tempfile

from fastapi import FastAPI, File, UploadFile, HTTPException
from fastapi.responses import Response, StreamingResponse
from fastapi.middleware.cors import CORSMiddleware

from src.utils.logger import setup_logger
from src.core.excel_processor import read_excel, write_excel
from src.core.priority_engine import PriorityEngine

# Setup logger
logger = setup_logger()

# Create FastAPI app
app = FastAPI(
    title="Lead Enrichment Engine API",
    description="API para enriquecer datos de leads B2B desde archivos Excel",
    version="1.0.0",
)

# Enable CORS for web frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # In production, specify actual origins
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/")
async def root():
    """Health check endpoint."""
    return {
        "status": "ok",
        "message": "Lead Enrichment Engine API",
        "version": "1.0.0",
    }


@app.post("/process")
async def process_excel(file: UploadFile = File(...)) -> Response:
    """Process Excel file and return enriched Excel file.

    Args:
        file: Uploaded Excel file.

    Returns:
        Excel file with PRIORITY column added and red rows excluded.
    """
    logger.info(f"Received file: {file.filename}")

    # Validate file type
    if not file.filename.endswith((".xlsx", ".xls")):
        raise HTTPException(
            status_code=400, detail="File must be Excel format (.xlsx or .xls)"
        )

    try:
        # Read uploaded file into memory
        contents = await file.read()

        # Create temporary file for processing
        with tempfile.NamedTemporaryFile(
            delete=False, suffix=".xlsx", mode="wb"
        ) as tmp_input:
            tmp_input.write(contents)
            tmp_input_path = Path(tmp_input.name)

        try:
            # Read Excel file
            logger.info("Reading Excel file...")
            df, metadata = read_excel(tmp_input_path)

            # Filter out red rows for processing
            df_process = df[~df.get("_IS_RED_ROW", False)].copy()
            logger.info(
                f"Processing {len(df_process)} rows "
                f"(skipping {len(df) - len(df_process)} red rows)"
            )

            # Calculate priorities
            logger.info("Calculating priorities...")
            priority_engine = PriorityEngine()
            priorities = priority_engine.calculate_priorities(df_process)

            # Add PRIORITY column to DataFrame
            df["PRIORITY"] = None
            df.loc[~df.get("_IS_RED_ROW", False), "PRIORITY"] = priorities.values

            # Remove temporary _IS_RED_ROW column before writing
            if "_IS_RED_ROW" in df.columns:
                df = df.drop(columns=["_IS_RED_ROW"])

            # Create temporary output file
            with tempfile.NamedTemporaryFile(
                delete=False, suffix=".xlsx", mode="wb"
            ) as tmp_output:
                tmp_output_path = Path(tmp_output.name)

            # Write output Excel
            logger.info("Writing output Excel...")
            write_excel(df, metadata, tmp_output_path, preserve_format=True)

            # Read output file into memory
            with open(tmp_output_path, "rb") as f:
                output_data = f.read()

            # Generate output filename
            input_name = Path(file.filename).stem
            output_filename = f"LIMPIO_{input_name}.xlsx"

            # Clean up temporary files
            tmp_input_path.unlink()
            tmp_output_path.unlink()

            logger.info(f"✅ Processing complete! Returning {output_filename}")

            # Return Excel file as response
            return Response(
                content=output_data,
                media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                headers={"Content-Disposition": f'attachment; filename="{output_filename}"'},
            )

        except Exception as e:
            # Clean up on error
            if tmp_input_path.exists():
                tmp_input_path.unlink()
            logger.error(f"Error processing file: {e}", exc_info=True)
            raise HTTPException(status_code=500, detail=f"Error processing file: {str(e)}")

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Unexpected error: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Unexpected error: {str(e)}")


@app.get("/health")
async def health_check():
    """Health check endpoint."""
    return {"status": "healthy"}

