"""Streamlit web interface for Lead Enrichment Engine."""

import streamlit as st
import pandas as pd
from pathlib import Path
import tempfile
import sys
import os
import hashlib

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.core.orchestrator import process_file
from src.utils.logger import setup_logger

logger = setup_logger()

# Configuration
MAX_ROWS_PER_BATCH = 100
COST_PER_LEAD_ESTIMATE = 0.05  # $0.05 per lead estimate

# Page config
st.set_page_config(
    page_title="Lead Enrichment Engine",
    page_icon="📊",
    layout="wide"
)

# Initialize session state
if 'uploaded_file_id' not in st.session_state:
    st.session_state.uploaded_file_id = None
if 'processing' not in st.session_state:
    st.session_state.processing = False
if 'file_preview' not in st.session_state:
    st.session_state.file_preview = None
if 'num_rows' not in st.session_state:
    st.session_state.num_rows = 0
if 'processing_result' not in st.session_state:
    st.session_state.processing_result = None
if 'processing_output_path' not in st.session_state:
    st.session_state.processing_output_path = None
if 'processing_error' not in st.session_state:
    st.session_state.processing_error = None
if 'tier2_enabled' not in st.session_state:
    st.session_state.tier2_enabled = False
if 'tier3_enabled' not in st.session_state:
    st.session_state.tier3_enabled = True


def check_api_keys():
    """Check if API keys are configured."""
    return {
        "OpenAI": bool(os.getenv("OPENAI_API_KEY")),
        "Tavily": bool(os.getenv("TAVILY_API_KEY")),
        "Google Places": bool(os.getenv("GOOGLE_PLACES_API_KEY")),
    }


def get_rate_limit_status():
    """Get Google Places rate limit status from tier1_rate_limits.json."""
    import json
    rate_limit_file = project_root / "tier1_rate_limits.json"
    if rate_limit_file.exists():
        try:
            with open(rate_limit_file, 'r') as f:
                data = json.load(f)
                used = data.get("google_places", {}).get("used", 0)
                limit = data.get("google_places", {}).get("limit", 200)
                return used, limit
        except Exception:
            return 0, 200
    return 0, 200


def estimate_cost(num_rows: int, tier2_enabled: bool, tier3_enabled: bool) -> float:
    """Estimate processing cost based on number of rows and tiers."""
    base_cost = num_rows * COST_PER_LEAD_ESTIMATE
    if tier2_enabled:
        base_cost *= 1.5  # Tier2 adds email research costs
    if tier3_enabled:
        base_cost *= 1.2  # Tier3 adds validation costs
    return base_cost


# Sidebar
with st.sidebar:
    st.header("🔧 API Health")
    
    # API Keys status
    keys = check_api_keys()
    for api_name, is_configured in keys.items():
        if is_configured:
            st.success(f"✅ {api_name}: OK")
        else:
            st.error(f"❌ {api_name}: MISSING_KEY")
    
    st.markdown("---")
    
    # Rate Limits
    st.subheader("📊 Rate Limits")
    used, limit = get_rate_limit_status()
    st.metric("Google Places", f"{used} / {limit}")
    
    if used >= limit:
        st.error("🚨 RATE LIMIT REACHED")
        st.warning("Tier1 no llamará a Google Places hasta resetear límites.")
    else:
        remaining = limit - used
        st.info(f"✅ {remaining} llamadas restantes")
    
    # Reset button
    if st.button("🔄 Resetear límites API"):
        rate_limit_file = project_root / "tier1_rate_limits.json"
        if rate_limit_file.exists():
            try:
                rate_limit_file.unlink()
                st.success("✅ Límites reseteados")
                st.rerun()
            except Exception as e:
                st.error(f"Error: {e}")
        else:
            st.info("No hay límites para resetear")

# Title
st.title("📊 Lead Enrichment Engine v1.0")

# Clear cache button
if st.button("🗑️ Limpiar caché y empezar de nuevo"):
    st.session_state.uploaded_file_id = None
    st.session_state.processing = False
    st.session_state.file_preview = None
    st.session_state.num_rows = 0
    st.rerun()

st.markdown("---")

# File uploader
uploaded_file = st.file_uploader(
    "Sube tu archivo Excel (.xlsx)",
    type=["xlsx"],
    help="Selecciona un archivo Excel con los leads a procesar"
)

# File cache detection and validation
if uploaded_file is not None:
    # Generate file ID (hash of name + size)
    file_id = hashlib.md5(f"{uploaded_file.name}{uploaded_file.size}".encode()).hexdigest()
    
    # Check if it's a NEW file
    if file_id != st.session_state.uploaded_file_id:
        st.session_state.uploaded_file_id = file_id
        st.session_state.processing = False  # Reset processing state
        
        # Read and validate
        try:
            with tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx") as tmp:
                tmp.write(uploaded_file.getvalue())
                tmp_path = tmp.name
            
            df_preview = pd.read_excel(tmp_path, engine="openpyxl")
            num_rows = len(df_preview)
            st.session_state.num_rows = num_rows
            st.session_state.file_preview = df_preview
            
            # Cleanup temp file
            os.unlink(tmp_path)
            
            st.success(f"✅ Nuevo archivo detectado: **{uploaded_file.name}**")
            
            # Show preview
            st.markdown(f"### 📊 **Archivo detectado: {num_rows} leads**")
            
            # Show preview table
            with st.expander("👀 Vista previa (primeras 5 filas)", expanded=True):
                st.dataframe(df_preview.head(5), use_container_width=True)
            
            # Warning if too many rows
            if num_rows > MAX_ROWS_PER_BATCH:
                st.warning(f"⚠️ **ADVERTENCIA**: Estás a punto de procesar {num_rows} leads, que excede el límite recomendado de {MAX_ROWS_PER_BATCH}.")
                st.warning("Esto consumirá créditos significativos de API y puede tomar mucho tiempo.")
            
            # Cost estimation
            tier2_enabled = st.session_state.get('tier2_enabled', False)
            tier3_enabled = st.session_state.get('tier3_enabled', True)
            estimated_cost = estimate_cost(num_rows, tier2_enabled, tier3_enabled)
            
            if num_rows > 10:
                st.info(f"💰 **Costo estimado**: ${estimated_cost:.2f} USD")
            
        except Exception as e:
            st.error(f"❌ Error al leer el archivo: {str(e)}")
            logger.error(f"Error reading uploaded file: {e}", exc_info=True)
            st.session_state.uploaded_file_id = None
    else:
        # Same file already uploaded
        st.info(f"📄 Archivo ya cargado: **{uploaded_file.name}** ({st.session_state.num_rows} leads)")
        
        # Show preview again
        if st.session_state.file_preview is not None:
            with st.expander("👀 Vista previa (primeras 5 filas)", expanded=False):
                st.dataframe(st.session_state.file_preview.head(5), use_container_width=True)

# Tier selection checkboxes
st.markdown("### Opciones de procesamiento")
col1, col2, col3 = st.columns(3)

with col1:
    tier1_enabled = st.checkbox("Tier1 (Validación + Prioridad)", value=True, disabled=True)
    
with col2:
    tier2_enabled = st.checkbox("Tier2 (Emails + Contactos)", value=False)
    st.session_state.tier2_enabled = tier2_enabled
    
with col3:
    tier3_enabled = st.checkbox("Tier3 (Enriquecimiento + Validación)", value=True)
    st.session_state.tier3_enabled = tier3_enabled

# Debug option: Force Tier2
st.markdown("---")
force_tier2 = st.checkbox("🧪 Forzar Tier2 (debug)", value=False, help="Ejecuta Tier2 para TODAS las filas NO rojas, aunque PRIORITY < 2. Solo para testing.")

# Process button and confirmation
if uploaded_file is not None and not st.session_state.processing:
    # Build tiers list
    tiers = []
    if tier1_enabled:
        tiers.append(1)
    if tier2_enabled:
        tiers.append(2)
    if tier3_enabled:
        tiers.append(3)
    
    if not tiers:
        st.error("⚠️ Debes seleccionar al menos un tier para procesar.")
    else:
        # Show confirmation with cost
        num_rows = st.session_state.num_rows
        estimated_cost = estimate_cost(num_rows, tier2_enabled, tier3_enabled)
        
        col1, col2 = st.columns(2)
        with col1:
            if st.button(f"✅ Procesar {num_rows} leads", type="primary", use_container_width=True):
                # Final confirmation for large batches
                if num_rows > MAX_ROWS_PER_BATCH:
                    st.warning(f"⚠️ Estás a punto de procesar {num_rows} leads con un costo estimado de ${estimated_cost:.2f} USD.")
                    st.warning("Por favor, confirma que deseas continuar.")
                else:
                    st.session_state.processing = True
                    st.rerun()
        
        with col2:
            if st.button("❌ Cancelar", use_container_width=True):
                st.session_state.uploaded_file_id = None
                st.session_state.processing = False
                st.rerun()

# Processing with STOP button
if st.session_state.processing and uploaded_file is not None:
    st.markdown("---")
    st.markdown("### ⚙️ Procesando...")
    
    # STOP button
    stop_col1, stop_col2, stop_col3 = st.columns([1, 2, 1])
    with stop_col2:
        if st.button("🛑 DETENER PROCESAMIENTO", type="secondary", use_container_width=True):
            st.session_state.processing = False
            st.error("❌ Procesamiento cancelado por el usuario")
            st.rerun()
    
    # Wrap processing in container to avoid placeholder issues
    with st.container():
        progress_placeholder = st.empty()
        status_placeholder = st.empty()
        
        # Create temporary files
        with tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx") as tmp_input:
            tmp_input_path = Path(tmp_input.name)
            tmp_input.write(uploaded_file.getvalue())
        
        with tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx") as tmp_output:
            tmp_output_path = Path(tmp_output.name)
        
        # Results are already initialized at the top, just clear them
        st.session_state.processing_result = None
        st.session_state.processing_error = None
        st.session_state.processing_output_path = None
        
        try:
            # Build tiers list
            tiers = []
            if tier1_enabled:
                tiers.append(1)
            if tier2_enabled:
                tiers.append(2)
            if tier3_enabled:
                tiers.append(3)
            
            if not tiers:
                st.error("⚠️ Debes seleccionar al menos un tier para procesar.")
                st.session_state.processing = False
                st.rerun()
            
            # Check if processing was cancelled
            if not st.session_state.processing:
                st.stop()
            
            # Process file
            with progress_placeholder.container():
                with st.spinner(f"Procesando {uploaded_file.name} ({st.session_state.num_rows} leads)..."):
                    status_placeholder.info("🔄 Iniciando procesamiento...")
                    
                    try:
                        df_result, metrics = process_file(
                            force_tier2=force_tier2,
                            input_path=tmp_input_path,
                            output_path=tmp_output_path,
                            tiers=tiers,
                            enable_email_research=tier2_enabled,
                        )
                        
                        # Check if processing was cancelled
                        if not st.session_state.processing:
                            st.stop()
                        
                        # Store results in session state BEFORE clearing processing
                        st.session_state.processing_result = {
                            'metrics': metrics,
                            'output_path': tmp_output_path,
                            'filename': uploaded_file.name
                        }
                        st.session_state.processing_output_path = tmp_output_path
                        st.session_state.processing_error = None
                        
                        # Set processing to False BEFORE any UI updates
                        st.session_state.processing = False
                        
                        # Log successful processing
                        logger.info(f"File processed successfully: {uploaded_file.name}, rows: {st.session_state.num_rows}")
                        
                        # Rerun to show results
                        st.rerun()
                        
                    except Exception as e:
                        logger.error(f"Error processing file: {e}", exc_info=True)
                        st.session_state.processing_error = str(e)
                        st.session_state.processing_result = None
                        st.session_state.processing = False
                        st.rerun()
        
        except Exception as e:
            logger.error(f"Error in processing setup: {e}", exc_info=True)
            st.session_state.processing_error = str(e)
            st.session_state.processing = False
            st.rerun()
        
        finally:
            # Cleanup temporary files only if processing failed
            if st.session_state.processing:
                try:
                    tmp_input_path.unlink(missing_ok=True)
                    tmp_output_path.unlink(missing_ok=True)
                except Exception as e:
                    logger.warning(f"Error cleaning up temp files: {e}")

# Display results after processing completes
processing_result = st.session_state.get('processing_result')
if not st.session_state.processing and processing_result is not None:
    st.markdown("---")
    st.success("✅ Procesamiento completado!")
    
    # Display metrics
    metrics = processing_result.get('metrics', {})
    st.markdown("### 📈 Métricas")
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Total procesados", metrics.get("total_processed", 0))
    
    with col2:
        st.metric("Alta calidad", metrics.get("high_quality", 0))
    
    with col3:
        st.metric("Emails válidos", metrics.get("emails_valid", 0))
    
    with col4:
        st.metric("Errores", metrics.get("errors_count", 0))
    
    # Download button
    output_path = st.session_state.get('processing_output_path')
    if output_path and Path(output_path).exists():
        with open(output_path, "rb") as f:
            output_data = f.read()
        
        output_filename = f"LIMPIO_{processing_result.get('filename', 'output')}"
        st.download_button(
            label="📥 Descargar Excel procesado",
            data=output_data,
            file_name=output_filename,
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True,
            type="primary"
        )
        
        # Cleanup after download is available
        try:
            Path(output_path).unlink(missing_ok=True)
        except Exception as e:
            logger.warning(f"Error cleaning up output file: {e}")
    
    # Clear result state
    st.session_state.processing_result = None
    st.session_state.processing_output_path = None

# Display errors after processing fails
processing_error = st.session_state.get('processing_error')
if not st.session_state.processing and processing_error is not None:
    st.markdown("---")
    st.error(f"❌ Error al procesar el archivo: {processing_error}")
    st.session_state.processing_error = None

elif uploaded_file is None:
    st.info("👆 Por favor, sube un archivo Excel para comenzar.")

# Footer
st.markdown("---")
st.markdown(
    "<small>Lead Enrichment Engine v1.0 | Procesa y enriquece leads B2B desde Excel</small>",
    unsafe_allow_html=True
)
