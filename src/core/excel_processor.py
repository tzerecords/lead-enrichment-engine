"""Excel file processor with format preservation."""

import logging
from pathlib import Path
from typing import Tuple, Dict, Any, List
from copy import copy
import pandas as pd
from openpyxl import load_workbook
from openpyxl.styles import PatternFill
from openpyxl.utils import get_column_letter
import re

logger = logging.getLogger("lead_enrichment")


def _is_red_color(color_value: Any) -> bool:
    """Check if color is red (any shade).

    Args:
        color_value: Color value from openpyxl (can be RGB, hex, or theme).

    Returns:
        True if color appears to be red.
    """
    if color_value is None:
        return False

    # Handle RGB tuple
    if isinstance(color_value, tuple):
        r, g, b = color_value[:3]
        # Red if R is significantly higher than G and B
        return r > 150 and r > g + 50 and r > b + 50

    # Handle hex string
    if isinstance(color_value, str):
        # Remove # if present
        hex_color = color_value.replace("#", "").upper()
        # Handle ARGB format (8 chars) - remove alpha channel
        if len(hex_color) == 8:
            hex_color = hex_color[2:]  # Remove alpha (FF)
        if len(hex_color) == 6:
            try:
                r = int(hex_color[0:2], 16)
                g = int(hex_color[2:4], 16)
                b = int(hex_color[4:6], 16)
                return r > 150 and r > g + 50 and r > b + 50
            except ValueError:
                return False

    # Handle openpyxl color objects
    if hasattr(color_value, "rgb"):
        rgb_str = str(color_value.rgb)
        if rgb_str:
            # Format: "FFRRGGBB" (ARGB) or "RRGGBB" or "#RRGGBB"
            rgb_clean = rgb_str.replace("#", "").upper()
            # Remove alpha channel if present (first 2 chars if length is 8)
            if len(rgb_clean) == 8:
                rgb_clean = rgb_clean[2:]  # Remove alpha (FF)
            if len(rgb_clean) == 6:
                try:
                    r = int(rgb_clean[0:2], 16)
                    g = int(rgb_clean[2:4], 16)
                    b = int(rgb_clean[4:6], 16)
                    return r > 150 and r > g + 50 and r > b + 50
                except ValueError:
                    return False

    return False


def _detect_red_rows(workbook_path: Path) -> List[int]:
    """Detect rows with red background color.

    Args:
        workbook_path: Path to Excel file.

    Returns:
        List of row indices (1-based) that have red background.
    """
    logger.info(f"Detecting red rows in {workbook_path}")
    red_rows = []

    try:
        wb = load_workbook(workbook_path, data_only=False)
        ws = wb.active

        # Iterate through rows (skip header row 1)
        for row_idx in range(2, ws.max_row + 1):
            row_has_red = False

            # Check each cell in the row
            for col_idx in range(1, ws.max_column + 1):
                cell = ws.cell(row=row_idx, column=col_idx)

                # Check if cell has a fill with a pattern (not just default fill)
                if cell.fill and cell.fill.patternType and cell.fill.patternType != "none":
                    # Check fill color
                    fill_color = None
                    if hasattr(cell.fill, "start_color") and cell.fill.start_color:
                        fill_color = cell.fill.start_color
                    elif hasattr(cell.fill, "fgColor") and cell.fill.fgColor:
                        fill_color = cell.fill.fgColor

                    if fill_color:
                        # Try different ways to get color value
                        color_value = None
                        if hasattr(fill_color, "rgb"):
                            color_value = fill_color.rgb
                        elif hasattr(fill_color, "value"):
                            color_value = fill_color.value
                        elif hasattr(fill_color, "index"):
                            # Indexed color
                            pass

                        if color_value and _is_red_color(color_value):
                            row_has_red = True
                            logger.debug(f"Row {row_idx} cell {col_idx} is red: {color_value}")
                            break

            if row_has_red:
                red_rows.append(row_idx)
                logger.debug(f"Row {row_idx} detected as red")

        wb.close()
        logger.info(f"Found {len(red_rows)} red rows: {red_rows}")

    except Exception as e:
        logger.error(f"Error detecting red rows: {e}")
        raise

    return red_rows


def read_excel(filepath: Path) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    """Read Excel file and detect red rows.

    Args:
        filepath: Path to input Excel file.

    Returns:
        Tuple of (DataFrame, metadata dict).
        Metadata includes: red_row_indices (1-based), original_columns, etc.
    """
    logger.info(f"Reading Excel file: {filepath}")

    if not filepath.exists():
        raise FileNotFoundError(f"Excel file not found: {filepath}")

    # Detect red rows first
    red_row_indices = _detect_red_rows(filepath)

    # Read with pandas
    df = pd.read_excel(filepath, engine="openpyxl")

    # Mark red rows (add column to indicate red status)
    # Convert 1-based row indices to 0-based DataFrame indices
    # Row 2 in Excel = index 1 in DataFrame (row 1 is header)
    red_df_indices = [idx - 2 for idx in red_row_indices if idx >= 2]
    df["_IS_RED_ROW"] = False
    df.loc[red_df_indices, "_IS_RED_ROW"] = True

    logger.info(f"Read {len(df)} rows, {len(red_df_indices)} marked as red")

    # Ensure filepath is a Path object for consistency
    filepath_obj = Path(filepath) if not isinstance(filepath, Path) else filepath
    
    metadata = {
        "red_row_indices": red_row_indices,
        "red_df_indices": red_df_indices,
        "original_columns": list(df.columns),
        "filepath": str(filepath_obj),  # Store as string for JSON serialization compatibility
    }
    
    logger.debug(f"Created metadata with filepath: {metadata['filepath']}")
    logger.debug(f"Filepath exists: {filepath_obj.exists()}")
    
    # Debug logging for metadata (workbook_context equivalent)
    logger.info(f"DEBUG read_excel: returning metadata with keys: {metadata.keys() if metadata else 'None'}")
    logger.info(f"DEBUG read_excel: metadata type: {type(metadata)}")
    logger.info(f"DEBUG read_excel: metadata is None? {metadata is None}")
    if metadata is not None:
        logger.info(f"DEBUG read_excel: metadata keys: {metadata.keys() if isinstance(metadata, dict) else 'NOT A DICT'}")

    return df, metadata


def write_excel(
    df: pd.DataFrame,
    metadata: Dict[str, Any],
    output_path: Path,
    preserve_format: bool = True,
) -> None:
    """Write DataFrame to Excel preserving original format.

    Args:
        df: DataFrame to write.
        metadata: Metadata from read_excel (includes original filepath).
        output_path: Path for output Excel file.
        preserve_format: Whether to preserve original formatting.
    """
    logger.info(f"Writing Excel file: {output_path}")

    # Ensure output directory exists
    output_path.parent.mkdir(parents=True, exist_ok=True)

    # Debug logging: verify metadata structure (workbook_context equivalent)
    logger.info(f"DEBUG write_excel: metadata type: {type(metadata)}")
    logger.info(f"DEBUG write_excel: metadata value: {metadata}")
    logger.info(f"DEBUG write_excel: metadata is None? {metadata is None}")
    if metadata is not None:
        logger.info(f"DEBUG write_excel: metadata keys: {metadata.keys() if isinstance(metadata, dict) else 'NOT A DICT'}")
    
    # Additional debug logging
    logger.debug(f"Metadata keys: {list(metadata.keys()) if metadata else 'None'}")
    logger.debug(f"Metadata type: {type(metadata)}")
    if metadata:
        logger.debug(f"Metadata has 'filepath': {'filepath' in metadata}")
        if "filepath" in metadata:
            logger.debug(f"Original filepath: {metadata['filepath']}")
            logger.debug(f"Original filepath exists: {Path(metadata['filepath']).exists() if metadata['filepath'] else 'N/A'}")

    if preserve_format and "filepath" in metadata:
        # Load original workbook to copy formatting
        original_path = metadata["filepath"]
        # Convert to Path if it's a string
        original_path_obj = Path(original_path) if isinstance(original_path, str) else original_path
        
        logger.debug(f"Attempting to load original workbook from: {original_path}")
        logger.debug(f"Original path type: {type(original_path)}")
        logger.debug(f"Original path exists: {original_path_obj.exists() if original_path_obj else 'N/A'}")
        
        try:
            if not original_path or not original_path_obj.exists():
                raise FileNotFoundError(f"Original filepath in metadata is invalid or doesn't exist: {original_path}")
            
            # Use Path object for load_workbook
            original_path = original_path_obj
            
            wb_original = load_workbook(original_path)
            ws_original = wb_original.active
            logger.debug(f"Loaded original workbook: max_row={ws_original.max_row}, max_column={ws_original.max_column}")

            # Create new workbook
            wb_new = load_workbook(original_path)
            ws_new = wb_new.active

            # Clear data but keep formatting structure
            # We'll write new data while preserving colors

            # Write header row
            for col_idx, col_name in enumerate(df.columns, start=1):
                cell = ws_new.cell(row=1, column=col_idx)
                cell.value = col_name
                # Copy header formatting from original if exists
                if col_idx <= ws_original.max_column:
                        orig_cell = ws_original.cell(row=1, column=col_idx)
                        try:
                            if orig_cell.fill and orig_cell.fill.patternType:
                                cell.fill = PatternFill(
                                    start_color=orig_cell.fill.start_color,
                                    end_color=orig_cell.fill.end_color,
                                    fill_type=orig_cell.fill.fill_type
                                )
                        except Exception:
                            pass  # Skip if fill copy fails
                        try:
                            if orig_cell.font:
                                cell.font = orig_cell.font.copy()
                        except Exception:
                            pass  # Skip if font copy fails

            # Write data rows (skip red rows)
            # Filter out red rows if the column exists
            if "_IS_RED_ROW" in df.columns:
                df_filtered = df[df["_IS_RED_ROW"] == False].copy()
                df_filtered = df_filtered.drop(columns=["_IS_RED_ROW"])
            else:
                df_filtered = df.copy()
            
            # Reset index para iterar secuencialmente
            df_filtered = df_filtered.reset_index(drop=True)

            excel_row = 2  # Start at row 2 (after header)
            original_columns_count = len(metadata.get("original_columns", []))
            
            for row_idx in range(len(df_filtered)):
                row_data = df_filtered.iloc[row_idx]
                
                for col_idx, value in enumerate(row_data, start=1):
                    cell = ws_new.cell(row=excel_row, column=col_idx)
                    cell.value = value
                    
                    # Copy format from FIRST data row (row 2) of original as template
                    # Solo para columnas originales (no las nuevas que agregamos)
                    if col_idx <= original_columns_count:
                        try:
                            orig_cell = ws_original.cell(row=2, column=col_idx)  # Siempre usar row 2 como template
                            
                            # Copy formatting
                            if orig_cell.has_style:
                                cell.font = copy(orig_cell.font)
                                cell.fill = copy(orig_cell.fill)
                                cell.border = copy(orig_cell.border)
                                cell.alignment = copy(orig_cell.alignment)
                                cell.number_format = orig_cell.number_format
                        except Exception as e:
                            logger.debug(f"Could not copy format for col {col_idx}: {e}")
                
                excel_row += 1

            wb_original.close()
            wb_new.save(output_path)
            wb_new.close()
            logger.info(f"Excel file written with format preservation: {output_path}")

        except Exception as e:
            import traceback
            logger.error(f"DEBUG write_excel: Exception type: {type(e)}")
            logger.error(f"DEBUG write_excel: Exception message: {e}")
            logger.error(f"DEBUG write_excel: Exception args: {e.args}")
            logger.error(f"DEBUG write_excel: Full traceback:\n{traceback.format_exc()}")
            logger.error(f"DEBUG write_excel: Exception repr: {repr(e)}")
            logger.warning(
                f"Error preserving format, falling back to simple write: {e}"
            )
            # Fallback to simple pandas write
            if "_IS_RED_ROW" in df.columns:
                df_filtered = df[df["_IS_RED_ROW"] == False].copy()
                df_filtered = df_filtered.drop(columns=["_IS_RED_ROW"])
            else:
                df_filtered = df.copy()
            df_filtered.to_excel(output_path, index=False, engine="openpyxl")
    else:
        # Simple write without format preservation
        if "_IS_RED_ROW" in df.columns:
            df_filtered = df[df["_IS_RED_ROW"] == False].copy()
            df_filtered = df_filtered.drop(columns=["_IS_RED_ROW"])
        else:
            df_filtered = df.copy()
        df_filtered.to_excel(output_path, index=False, engine="openpyxl")
        logger.info(f"Excel file written (simple mode): {output_path}")

