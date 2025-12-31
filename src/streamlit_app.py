"""Streamlit web interface for Lead Enrichment Engine v2.0 - Ultra-simple UI."""

import streamlit as st
import pandas as pd
from pathlib import Path
import tempfile
import sys
import os
import hashlib
from datetime import datetime, timedelta
import json

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.core.orchestrator import process_file
from src.utils.logger import setup_logger

logger = setup_logger()

# Page config
st.set_page_config(
    page_title="Lead Enrichment Engine",
    page_icon="📊",
    layout="centered"
)

# Initialize session state
if 'uploaded_file_id' not in st.session_state:
    st.session_state.uploaded_file_id = None
if 'processing' not in st.session_state:
    st.session_state.processing = False
if 'num_rows' not in st.session_state:
    st.session_state.num_rows = 0
if 'processing_result' not in st.session_state:
    st.session_state.processing_result = None
if 'processing_output_path' not in st.session_state:
    st.session_state.processing_output_path = None
if 'processing_error' not in st.session_state:
    st.session_state.processing_error = None
if 'processed_files' not in st.session_state:
    st.session_state.processed_files = []


def check_api_keys():
    """Check if API keys are configured."""
    return {
        "OpenAI": bool(os.getenv("OPENAI_API_KEY")),
        "Tavily": bool(os.getenv("TAVILY_API_KEY")),
        "Google Places": bool(os.getenv("GOOGLE_PLACES_API_KEY")),
    }


def get_rate_limit_status():
    """Get rate limit status from tier1_rate_limits.json."""
    rate_limit_file = project_root / "tier1_rate_limits.json"
    if rate_limit_file.exists():
        try:
            with open(rate_limit_file, 'r') as f:
                data = json.load(f)
                google_used = data.get("google_places", {}).get("used", 0)
                google_limit = data.get("google_places", {}).get("limit", 200)
                return google_used, google_limit
        except Exception:
            return 0, 200
    return 0, 200


def get_api_status_text() -> str:
    """Return compact API status string for display."""
    # Import settings
    try:
        from config.settings import DAILY_LIMITS
    except ImportError:
        # Fallback to tier1_config.yaml if settings.py doesn't exist
        try:
            from src.utils.config_loader import load_yaml_config
            tier1_config = load_yaml_config("config/tier1_config.yaml")
            google_limit = tier1_config.get("tier1", {}).get("rate_limits", {}).get("google_places", 200)
            DAILY_LIMITS = {"google_places": google_limit, "tavily": 1000}
        except Exception:
            DAILY_LIMITS = {"google_places": 1000, "tavily": 1000}
    
    # Get used counts
    google_used, google_limit_file = get_rate_limit_status()
    google_limit = DAILY_LIMITS.get("google_places", 1000)
    
    # Check API keys
    keys = check_api_keys()
    
    # Build status string
    status_parts = []
    
    # Google Places
    status_parts.append(f"Google: {google_used}/{google_limit} hoy")
    
    # Tavily (if we can get usage, otherwise just OK/MISSING)
    if keys["Tavily"]:
        status_parts.append("Tavily: OK")
    else:
        status_parts.append("Tavily: MISSING")
    
    # OpenAI
    if keys["OpenAI"]:
        status_parts.append("OpenAI: OK")
    else:
        status_parts.append("OpenAI: MISSING")
    
    return " | ".join(status_parts)


def format_time_ago(timestamp: datetime) -> str:
    """Format timestamp as 'hace X días/horas'."""
    now = datetime.now()
    diff = now - timestamp
    
    if diff.days > 0:
        if diff.days == 1:
            return "hace 1 día"
        return f"hace {diff.days} días"
    elif diff.seconds >= 3600:
        hours = diff.seconds // 3600
        if hours == 1:
            return "hace 1 hora"
        return f"hace {hours} horas"
    else:
        minutes = diff.seconds // 60
        if minutes < 1:
            return "hace unos momentos"
        if minutes == 1:
            return "hace 1 minuto"
        return f"hace {minutes} minutos"


# Title - Centered
st.markdown("<h1 style='text-align: center;'>📊 Lead Enrichment Engine</h1>", unsafe_allow_html=True)
st.markdown("<p style='text-align: center; color: #666;'>Enriquece y valida tus leads B2B</p>", unsafe_allow_html=True)

st.markdown("---")

# Historial de archivos procesados
if st.session_state.processed_files:
    st.markdown("### Historial de archivos procesados")
    for idx, file_info in enumerate(st.session_state.processed_files):
        col1, col2 = st.columns([5, 1])
        with col1:
            time_str = format_time_ago(file_info['timestamp'])
            st.text(f"{file_info['filename']} ({time_str})")
        with col2:
            if st.button("🗑️", key=f"delete_{idx}", help="Eliminar del historial"):
                # Remove from list
                st.session_state.processed_files.pop(idx)
                # Try to delete file if it exists
                if 'output_path' in file_info and Path(file_info['output_path']).exists():
                    try:
                        Path(file_info['output_path']).unlink()
                    except Exception:
                        pass
                st.rerun()
    st.markdown("---")

# File uploader
uploaded_file = st.file_uploader(
    "Sube tu archivo Excel (.xlsx)",
    type=["xlsx"],
    help="Selecciona un archivo Excel con los leads a procesar"
)

# File validation
if uploaded_file is not None:
    # Generate file ID (hash of name + size)
    file_id = hashlib.md5(f"{uploaded_file.name}{uploaded_file.size}".encode()).hexdigest()
    
    # Check if it's a NEW file
    if file_id != st.session_state.uploaded_file_id:
        st.session_state.uploaded_file_id = file_id
        st.session_state.processing = False
        
        # Read and validate
        try:
            with tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx") as tmp:
                tmp.write(uploaded_file.getvalue())
                tmp_path = tmp.name
            
            df_preview = pd.read_excel(tmp_path, engine="openpyxl")
            num_rows = len(df_preview)
            st.session_state.num_rows = num_rows
            
            # Cleanup temp file
            os.unlink(tmp_path)
            
            st.success(f"✅ Archivo cargado: **{uploaded_file.name}** ({num_rows} leads)")
            
        except Exception as e:
            st.error(f"❌ Error al leer el archivo: {str(e)}")
            logger.error(f"Error reading uploaded file: {e}", exc_info=True)
            st.session_state.uploaded_file_id = None
    else:
        st.info(f"📄 Archivo cargado: **{uploaded_file.name}** ({st.session_state.num_rows} leads)")

# Sección "¿Cómo funciona?"
with st.expander("ℹ️ ¿Cómo funciona?", expanded=False):
    st.markdown("""
    **¿Qué hace esta herramienta?**
    - Valida CIFs y corrige datos de empresas
    - Busca teléfonos actualizados
    - Encuentra emails de contacto solo para leads prioritarios (alto consumo energético)
    
    **¿Por qué no busca emails para todos?**
    Solo buscamos emails para empresas con consumo relevante. 
    Buscar para todos sería lento y costoso sin beneficio real.
    
    **¿Qué significan los colores en el Excel?**
    - 🟢 Verde: Lead enriquecido con datos nuevos
    - 🟣 Morado: Lead validado (baja prioridad)
    - 🟡 Amarillo: No se encontraron datos nuevos
    - 🔴 Rojo: Lead ignorado (fila original roja)
    """)

# Estado de APIs - discreto
api_status = get_api_status_text()
st.caption(f"Estado de APIs: {api_status}")

st.markdown("---")

# Botón PROCESAR - único y grande
if uploaded_file is not None and not st.session_state.processing:
    if st.button("🚀 PROCESAR", type="primary", use_container_width=True):
        st.session_state.processing = True
        st.rerun()

# Processing
if st.session_state.processing and uploaded_file is not None:
    st.markdown("### ⚙️ Procesando...")
    
    # Progress bar
    progress_bar = st.progress(0)
    status_text = st.empty()
    
    # Create temporary files
    with tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx") as tmp_input:
        tmp_input_path = Path(tmp_input.name)
        tmp_input.write(uploaded_file.getvalue())
    
    with tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx") as tmp_output:
        tmp_output_path = Path(tmp_output.name)
    
    # Clear previous results
    st.session_state.processing_result = None
    st.session_state.processing_error = None
    st.session_state.processing_output_path = None
    
    try:
        # Show progress messages
        status_text.info("🔄 Iniciando procesamiento...")
        progress_bar.progress(10)
        
        status_text.info("📊 Analizando archivo y calculando prioridades...")
        progress_bar.progress(30)
        
        status_text.info("🔍 Validando CIFs y buscando datos de empresas...")
        progress_bar.progress(50)
        
        status_text.info("📧 Buscando emails para leads prioritarios...")
        progress_bar.progress(70)
        
        status_text.info("✨ Enriqueciendo datos y validando información...")
        progress_bar.progress(90)
        
        # Process file with default configuration
        df_result, metrics = process_file(
            input_path=tmp_input_path,
            output_path=tmp_output_path,
            tiers=[1, 3],  # Tier2 se ejecuta automáticamente si hay PRIORITY>=2
            enable_email_research=True,  # Siempre activo para Tier2
            force_tier2=False  # Nunca forzar
        )
        
        progress_bar.progress(100)
        status_text.success("✅ Procesamiento completado!")
        
        # Store results
        st.session_state.processing_result = {
            'metrics': metrics,
            'output_path': tmp_output_path,
            'filename': uploaded_file.name
        }
        st.session_state.processing_output_path = tmp_output_path
        st.session_state.processing_error = None
        
        # Add to history
        st.session_state.processed_files.append({
            'filename': uploaded_file.name,
            'timestamp': datetime.now(),
            'rows': st.session_state.num_rows,
            'output_path': str(tmp_output_path)
        })
        
        # Set processing to False
        st.session_state.processing = False
        
        logger.info(f"File processed successfully: {uploaded_file.name}, rows: {st.session_state.num_rows}")
        
        # Rerun to show results
        st.rerun()
        
    except Exception as e:
        logger.error(f"Error processing file: {e}", exc_info=True)
        st.session_state.processing_error = str(e)
        st.session_state.processing_result = None
        st.session_state.processing = False
        progress_bar.progress(0)
        status_text.error(f"❌ Error: {str(e)}")
        st.rerun()
    
    finally:
        # Cleanup input temp file
        try:
            tmp_input_path.unlink(missing_ok=True)
        except Exception:
            pass

# Display results after processing completes
processing_result = st.session_state.get('processing_result')
if not st.session_state.processing and processing_result is not None:
    st.markdown("---")
    st.success("✅ ¡Procesamiento completado!")
    
    # Display metrics
    metrics = processing_result.get('metrics', {})
    if metrics:
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Total procesados", metrics.get("total_processed", 0))
        with col2:
            st.metric("Alta calidad", metrics.get("high_quality", 0))
        with col3:
            st.metric("Emails válidos", metrics.get("emails_valid", 0))
    
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
    
    # Clear result state after showing
    st.session_state.processing_result = None
    st.session_state.processing_output_path = None

# Display errors
processing_error = st.session_state.get('processing_error')
if not st.session_state.processing and processing_error is not None:
    st.markdown("---")
    st.error(f"❌ Error al procesar el archivo: {processing_error}")
    st.session_state.processing_error = None

elif uploaded_file is None:
    st.info("👆 Por favor, sube un archivo Excel para comenzar.")
