"""Excel file processor with format preservation."""

import logging
from pathlib import Path
from typing import Tuple, Dict, Any, List
from copy import copy
import pandas as pd
from openpyxl import load_workbook
from openpyxl.styles import PatternFill, Font
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


def _auto_adjust_column_widths(ws) -> None:
    """Auto-adjust column widths for a worksheet.
    
    Args:
        ws: openpyxl worksheet object.
    """
    for column in ws.columns:
        max_length = 0
        column_letter = get_column_letter(column[0].column)
        for cell in column:
            try:
                if cell.value:
                    cell_len = len(str(cell.value))
                    if cell_len > max_length:
                        max_length = cell_len
            except:
                pass
        adjusted_width = min(max_length + 2, 50)
        ws.column_dimensions[column_letter].width = adjusted_width


def write_excel(
    df: pd.DataFrame,
    metadata: Dict[str, Any],
    output_path: Path,
    preserve_format: bool = True,
    force_tier2: bool = False,
) -> None:
    """Write DataFrame to Excel with 3 sheets: BBDD ORIGINAL, HIGHLIGHT, DATOS_TÉCNICOS.

    Args:
        df: DataFrame to write.
        metadata: Metadata from read_excel (includes original filepath).
        output_path: Path for output Excel file.
        preserve_format: Whether to preserve original formatting (only for HOJA 1).
    """
    logger.info(f"Writing Excel file with 3 sheets: {output_path}")

    # Ensure output directory exists
    output_path.parent.mkdir(parents=True, exist_ok=True)

    # Get red row indices from metadata
    red_df_indices = metadata.get("red_df_indices", [])
    
    # Prepare data: remove red rows for HOJA 1 and HOJA 2, keep all for HOJA 3
    if "_IS_RED_ROW" in df.columns:
        df_processed = df[df["_IS_RED_ROW"] == False].copy()
        df_processed = df_processed.drop(columns=["_IS_RED_ROW"])
        # Keep all rows (including red) for HOJA 3
        df_all = df.drop(columns=["_IS_RED_ROW"]).copy()
    else:
        df_processed = df.copy()
        df_all = df.copy()
    
    # ============================================
    # Calculate COLOR column based on processing status
    # ============================================
    def calculate_color_status(row_idx: int, row: pd.Series) -> str:
        """Calculate COLOR status for a row.
        
        Args:
            row_idx: DataFrame index (0-based).
            row: Series representing the row.
            
        Returns:
            Color status string.
        """
        # Check if row was originally red (ignored)
        if row_idx in red_df_indices:
            return "ROJO (IGNORADA)"
        
        # Get priority
        priority = row.get("PRIORITY")
        if pd.isna(priority) or priority is None:
            priority = 0
        else:
            priority = float(priority)
        
        # If priority < 2, it's TIER1 only
        if priority < 2:
            return "MORADO (TIER1)"
        
        # Priority >= 2: check if enriched with new data
        has_website = pd.notna(row.get("WEBSITE")) and str(row.get("WEBSITE", "")).strip() not in ["", "NOT_FOUND", "NO_WEBSITE_FOUND"]
        has_email = pd.notna(row.get("EMAIL_SPECIFIC")) and str(row.get("EMAIL_SPECIFIC", "")).strip() not in ["", "NO_EMAIL_FOUND", "NOT_FOUND"]
        has_contact = pd.notna(row.get("CONTACT_NAME")) and str(row.get("CONTACT_NAME", "")).strip() not in ["", "NOT_FOUND", "NO_CONTACT_FOUND"]
        
        if has_website or has_email or has_contact:
            return "VERDE (ENRIQUECIDA)"
        else:
            return "AMARILLO (SIN DATOS NUEVOS)"
    
    # Calculate COLOR for all rows in df_all (includes red rows)
    df_all["COLOR"] = df_all.apply(
        lambda row: calculate_color_status(row.name, row),
        axis=1
    )
    
    # Also add to df_processed for HOJA 1
    df_processed["COLOR"] = df_processed.apply(
        lambda row: calculate_color_status(row.name, row),
        axis=1
    )

    # ============================================
    # HOJA 1: "BBDD ORIGINAL" - Columnas originales + 6 nuevas
    # ============================================
    original_columns = metadata.get("original_columns", [])
    # Remove _IS_RED_ROW from original columns if present
    original_columns = [col for col in original_columns if col != "_IS_RED_ROW"]
    
    # Add 7 new columns (including COLOR)
    new_columns = ['PRIORITY', 'WEBSITE', 'CNAE', 'EMAIL_SPECIFIC', 'CONTACT_NAME', 'DATA_QUALITY', 'COLOR']
    
    # Build list of columns for HOJA 1
    hoja1_columns = []
    for col in original_columns:
        if col in df_processed.columns:
            hoja1_columns.append(col)
    for col in new_columns:
        if col in df_processed.columns:
            hoja1_columns.append(col)
    
    # Ensure COLOR is included if it exists
    if "COLOR" in df_processed.columns and "COLOR" not in hoja1_columns:
        hoja1_columns.append("COLOR")
    
    df_original = df_processed[[col for col in hoja1_columns if col in df_processed.columns]].copy()
    
    # ============================================
    # HOJA 2: "HIGHLIGHT" - Filtrada y ordenada
    # ============================================
    # Filter logic:
    # - If force_tier2: include ALL non-red rows (all processed rows)
    # - Otherwise: PRIORITY >= 3 AND DATA_QUALITY in ['High', 'Medium']
    if force_tier2:
        # Include all processed rows when force_tier2 is enabled
        mask_highlight = pd.Series([True] * len(df_processed), index=df_processed.index)
        logger.info("HIGHLIGHT: Including all processed rows (force_tier2=True)")
    else:
        # Normal filtering: high priority and quality
        mask_highlight = pd.Series([True] * len(df_processed), index=df_processed.index)
        
        if 'PRIORITY' in df_processed.columns:
            mask_highlight = mask_highlight & (df_processed['PRIORITY'] >= 3)
        
        if 'DATA_QUALITY' in df_processed.columns:
            mask_highlight = mask_highlight & (df_processed['DATA_QUALITY'].isin(['High', 'Medium']))
        
        logger.info(f"HIGHLIGHT: Filtered to {mask_highlight.sum()}/{len(mask_highlight)} rows (PRIORITY>=3, DATA_QUALITY High/Medium)")
    
    df_highlight = df_processed[mask_highlight].copy()
    
    # If HIGHLIGHT is empty and force_tier2, fallback to all processed rows
    if len(df_highlight) == 0 and not force_tier2:
        logger.warning("HIGHLIGHT is empty, falling back to all processed rows")
        df_highlight = df_processed.copy()
    
    # Select highlight columns (new list with status columns)
    highlight_cols = [
        'NOMBRE CLIENTE', 'CONSUMO', 'PRIORITY', 'DATA_QUALITY',
        'EMAIL_SPECIFIC', 'CONTACT_NAME', 'WEBSITE', 'CNAE',
        'ENRICHMENT_STATUS', 'ENRICHMENT_NOTES', 'OBSERVACIONES'
    ]
    
    # Use available columns (only include columns that exist)
    available_highlight_cols = [col for col in highlight_cols if col in df_highlight.columns]
    
    # Add any missing important columns if they exist
    optional_cols = ['TELEFONO 1', 'PHONE', 'LINKEDIN_COMPANY']
    for col in optional_cols:
        if col in df_highlight.columns and col not in available_highlight_cols:
            available_highlight_cols.append(col)
    
    df_highlight = df_highlight[available_highlight_cols].copy()
    
    # Ensure NOT_FOUND/NO_EMAIL_FOUND values are preserved (don't modify them)
    # OBSERVACIONES: Keep original format, only truncate if extremely long (>500 chars)
    if 'OBSERVACIONES' in df_highlight.columns:
        df_highlight['OBSERVACIONES'] = df_highlight['OBSERVACIONES'].apply(
            lambda x: str(x)[:500] + '...' if pd.notna(x) and len(str(x)) > 500 else (str(x) if pd.notna(x) else '')
        )
    
    # Sort: PRIORITY DESC, DATA_QUALITY DESC, CONSUMO DESC
    sort_cols = []
    if 'PRIORITY' in df_highlight.columns:
        sort_cols.append('PRIORITY')
    if 'DATA_QUALITY' in df_highlight.columns:
        sort_cols.append('DATA_QUALITY')
    if 'CONSUMO' in df_highlight.columns:
        sort_cols.append('CONSUMO')
    
    if sort_cols:
        df_highlight = df_highlight.sort_values(by=sort_cols, ascending=[False] * len(sort_cols))
    
    # ============================================
    # HOJA 3: "DATOS_TÉCNICOS" - Todas las columnas (incluye filas rojas)
    # ============================================
    df_technical = df_all.copy()
    
    # ============================================
    # Write Excel with 3 sheets
    # ============================================
    # First, write all sheets using pandas
    with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
        df_original.to_excel(writer, sheet_name='BBDD ORIGINAL', index=False)
        df_highlight.to_excel(writer, sheet_name='HIGHLIGHT', index=False)
        df_technical.to_excel(writer, sheet_name='DATOS_TÉCNICOS', index=False)
    
    # Now apply formatting
    wb = load_workbook(output_path)
    
    # ============================================
    # Format HOJA 1: Preserve original format (then apply COLOR fills)
    # ============================================
    ws_original = wb['BBDD ORIGINAL']
    
    if preserve_format and "filepath" in metadata:
        original_path = metadata["filepath"]
        original_path_obj = Path(original_path) if isinstance(original_path, str) else original_path
        
        try:
            if original_path_obj.exists():
                wb_source = load_workbook(original_path_obj)
                ws_source = wb_source.active
                
                # Copy header formatting
                for col_idx in range(1, min(len(hoja1_columns) + 1, ws_source.max_column + 1)):
                    cell = ws_original.cell(row=1, column=col_idx)
                    if col_idx <= ws_source.max_column:
                        orig_cell = ws_source.cell(row=1, column=col_idx)
                        try:
                            if orig_cell.fill and orig_cell.fill.patternType:
                                cell.fill = PatternFill(
                                    start_color=orig_cell.fill.start_color,
                                    end_color=orig_cell.fill.end_color,
                                    fill_type=orig_cell.fill.fill_type
                                )
                        except Exception:
                            pass
                        try:
                            if orig_cell.font:
                                cell.font = orig_cell.font.copy()
                        except Exception:
                            pass
                
                # Copy data row formatting (use row 2 as template) - BUT skip fill (we'll apply COLOR-based fills)
                original_columns_count = len(original_columns)
                for row_idx in range(2, ws_original.max_row + 1):
                    for col_idx in range(1, len(hoja1_columns) + 1):
                        cell = ws_original.cell(row=row_idx, column=col_idx)
                        if col_idx <= original_columns_count and ws_source.max_row >= 2:
                            try:
                                orig_cell = ws_source.cell(row=2, column=col_idx)
                                if orig_cell.has_style:
                                    cell.font = copy(orig_cell.font)
                                    # Skip fill - we'll apply COLOR-based fills below
                                    cell.border = copy(orig_cell.border)
                                    cell.alignment = copy(orig_cell.alignment)
                                    cell.number_format = orig_cell.number_format
                            except Exception:
                                pass
                
                wb_source.close()
        except Exception as e:
            logger.warning(f"Could not preserve format for HOJA 1: {e}")
    
    # Apply COLOR-based background colors AFTER preserving other formatting
    # Find COLOR column index
    color_col_idx = None
    for idx, col_name in enumerate(hoja1_columns, start=1):
        if col_name == "COLOR":
            color_col_idx = idx
            break
    
    # Define color mappings (ARGB format for openpyxl)
    color_fills = {
        "ROJO (IGNORADA)": PatternFill(start_color="FFFF0000", end_color="FFFF0000", fill_type="solid"),  # Red
        "MORADO (TIER1)": PatternFill(start_color="FF800080", end_color="FF800080", fill_type="solid"),  # Purple
        "VERDE (ENRIQUECIDA)": PatternFill(start_color="FF00FF00", end_color="FF00FF00", fill_type="solid"),  # Green
        "AMARILLO (SIN DATOS NUEVOS)": PatternFill(start_color="FFFFFF00", end_color="FFFFFF00", fill_type="solid"),  # Yellow
    }
    
    # Apply background colors to entire rows based on COLOR column
    if color_col_idx:
        logger.info(f"Applying COLOR-based background fills to HOJA 1 (COLOR column at index {color_col_idx})")
        for row_idx in range(2, ws_original.max_row + 1):  # Start from row 2 (skip header)
            color_cell = ws_original.cell(row=row_idx, column=color_col_idx)
            color_value = str(color_cell.value or "").strip()
            
            if color_value in color_fills:
                fill = color_fills[color_value]
                # Apply fill to entire row
                for col_idx in range(1, ws_original.max_column + 1):
                    cell = ws_original.cell(row=row_idx, column=col_idx)
                    cell.fill = fill
    
    # Auto-adjust widths for HOJA 1
    _auto_adjust_column_widths(ws_original)
    
    # ============================================
    # Format HOJA 2: Headers bold, clean white background
    # ============================================
    ws_highlight = wb['HIGHLIGHT']
    
    # Make headers bold
    for cell in ws_highlight[1]:
        cell.font = Font(bold=True)
    
    # Auto-adjust widths
    _auto_adjust_column_widths(ws_highlight)
    
    # ============================================
    # Format HOJA 3: Auto-adjust widths only
    # ============================================
    ws_technical = wb['DATOS_TÉCNICOS']
    _auto_adjust_column_widths(ws_technical)
    
    # Save workbook
    wb.save(output_path)
    wb.close()
    
    logger.info(f"Excel file written with 3 sheets: {output_path}")

