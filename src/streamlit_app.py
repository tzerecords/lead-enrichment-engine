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
if 'stop_requested' not in st.session_state:
    st.session_state.stop_requested = False
if 'current_lead' not in st.session_state:
    st.session_state.current_lead = ""
if 'current_progress' not in st.session_state:
    st.session_state.current_progress = 0.0
if 'total_leads' not in st.session_state:
    st.session_state.total_leads = 0
if 'processed_count' not in st.session_state:
    st.session_state.processed_count = 0
if 'phones_found' not in st.session_state:
    st.session_state.phones_found = 0
if 'emails_found' not in st.session_state:
    st.session_state.emails_found = 0


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
                google_limit = data.get("google_places", {}).get("limit", 10000)
                return google_used, google_limit
        except Exception:
            return 0, 10000
    return 0, 10000


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
            google_limit = tier1_config.get("tier1", {}).get("rate_limits", {}).get("google_places", 10000)
            DAILY_LIMITS = {"google_places": google_limit, "tavily": 10000}
        except Exception:
            DAILY_LIMITS = {"google_places": 10000, "tavily": 10000}
    
    # Get used counts
    google_used, google_limit_file = get_rate_limit_status()
    google_limit = DAILY_LIMITS.get("google_places", 10000)
    
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


# Title - Centered with professional font
st.markdown("""
    <h1 style='text-align: center; font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif; font-weight: 600; letter-spacing: -0.5px;'>
    Enriquecimiento de Leads
    </h1>
    """, unsafe_allow_html=True)

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
    "Carga tu archivo Excel (.xlsx)",
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
        # Limpiar resultados anteriores cuando se carga un nuevo archivo
        st.session_state.processing_result = None
        st.session_state.processing_output_path = None
        st.session_state.processing_error = None
        st.session_state.last_processed_file_name = uploaded_file.name
        
        # Read and validate
        try:
            with tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx") as tmp:
                tmp.write(uploaded_file.getvalue())
                tmp_path = tmp.name
            
            df_preview = pd.read_excel(tmp_path, engine="openpyxl")
            num_rows = len(df_preview)
            st.session_state.num_rows = num_rows
            st.session_state.total_leads = num_rows
            
            # Cleanup temp file
            os.unlink(tmp_path)
            
            st.success(f"✅ Archivo cargado: **{uploaded_file.name}**")
            st.info(f"📊 {num_rows} empresas encontradas")
            
        except Exception as e:
            st.error(f"❌ Error al leer el archivo: {str(e)}")
            logger.error(f"Error reading uploaded file: {e}", exc_info=True)
            st.session_state.uploaded_file_id = None
    else:
        st.info(f"📄 Archivo cargado: **{uploaded_file.name}** ({st.session_state.num_rows} leads)")

# Sección "Cómo funciona"
with st.expander("ℹ️ Cómo funciona", expanded=False):
    st.markdown("""
    **📋 ANTES DE SUBIR TU EXCEL:**
    
    ⚠️ **IMPORTANTE**: Limpia tu Excel antes de subirlo. Borra todas las filas que no necesites (duplicados, empresas que ya no te interesan, datos obsoletos, etc.). 
    
    Si subes más de 1000 leads, el proceso será muy lento y costoso. Trabaja un poco antes de subir el archivo para obtener mejores resultados.
    
    ---
    
    **¿Qué hace esta herramienta?**
    
    La herramienta limpia y enriquece tus leads de forma inteligente:
    - **Limpia las filas descartadas** (rojas o baja prioridad) para procesar solo lo relevante
    - Esto hace el proceso **más rápido, más barato y más eficiente**
    - Valida CIFs y corrige datos de empresas
    - Busca teléfonos actualizados con Google Places y Tavily
    - Encuentra emails de contacto solo para leads prioritarios (alto consumo energético)
    
    **¿Por qué limpia filas descartadas?**
    
    Al descartar filas rojas y leads de baja prioridad antes de procesar, optimizamos:
    - ⚡ **Velocidad**: Procesamos menos datos, es más rápido
    - 💰 **Costo**: Usamos menos llamadas a APIs, es más barato
    - 🎯 **Calidad**: Nos enfocamos en los leads que realmente importan
    
    **¿Por qué no busca emails para todos?**
    
    Solo buscamos emails para empresas con consumo relevante (≥70 MWh). 
    Buscar para todos sería lento y costoso sin beneficio real.
    
    **¿Qué significan los colores en el Excel?**
    - 🟢 Verde: Lead enriquecido con datos nuevos
    - 🟣 Morado: Lead validado (baja prioridad)
    - 🟡 Amarillo: No se encontraron datos nuevos
    - 🔴 Rojo: Lead ignorado (fila original roja)
    
    **🔗 Gestionar APIs:**
    - [Google Cloud Console](https://console.cloud.google.com/google/maps-apis/metrics?authuser=5&project=project-5bcbc3d3-652e-4f80-876&supportedpurview=project)
    - [Tavily Dashboard](https://app.tavily.com/home)
    - [OpenAI API Keys](https://platform.openai.com/settings/proj_MR51aZ02sjbHHaLo7MRk1xN8/api-keys)
    """)

# Estado de APIs - discreto
api_status = get_api_status_text()
st.caption(f"Estado de APIs: {api_status}")

st.markdown("---")

# Callback para botón DETENER
def request_stop():
    """Callback para solicitar detener el procesamiento."""
    st.session_state.stop_requested = True
    st.session_state.processing = False

# Botón PROCESAR - único y grande (solo si no hay resultado y no está procesando)
if uploaded_file is not None and not st.session_state.processing and not st.session_state.processing_result:
    if st.button("🚀 PROCESAR", type="primary", use_container_width=True):
        st.session_state.processing = True
        st.session_state.stop_requested = False
        st.rerun()

# Processing
if st.session_state.processing and uploaded_file is not None:
    st.subheader("⚙️ Procesando...")
    
    # Progress bar with text and spinner for visual feedback
    progress_container = st.container()
    with progress_container:
        progress_bar = st.progress(0.0, text="Iniciando...")
        status_text = st.empty()
        # Spinner for visual animation
        spinner_placeholder = st.empty()
    
    # Metrics columns
    col1, col2, col3 = st.columns(3)
    with col1:
        metric_processed = st.metric("Procesados", f"{st.session_state.processed_count}/{st.session_state.total_leads}")
    with col2:
        metric_phones = st.metric("Teléfonos encontrados", st.session_state.phones_found)
    with col3:
        metric_emails = st.metric("Emails encontrados", st.session_state.emails_found)
    
    # STOP button centered
    _, col_btn, _ = st.columns([2, 1, 2])
    with col_btn:
        if st.button("⏹️ DETENER", on_click=request_stop, type="secondary", use_container_width=True):
            st.warning("⏸️ Deteniendo procesamiento...")
            st.rerun()
    
    # Create temporary files
    with tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx") as tmp_input:
        tmp_input_path = Path(tmp_input.name)
        tmp_input.write(uploaded_file.getvalue())
    
    with tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx") as tmp_output:
        tmp_output_path = Path(tmp_output.name)
    
    # Clear previous results and reset counters
    st.session_state.processing_result = None
    st.session_state.processing_error = None
    st.session_state.processing_output_path = None
    st.session_state.stop_requested = False
    st.session_state.processed_count = 0
    st.session_state.phones_found = 0
    st.session_state.emails_found = 0
    
    # Progress callback function
    def update_progress(current: int, total: int, company_name: str = ""):
        """Update progress bar and metrics."""
        if total > 0:
            progress = (current + 1) / total
            progress_text = f"Lead {current + 1}/{total}"
            if company_name:
                progress_text += f" - {company_name[:40]}"
            
            # Update progress bar
            progress_bar.progress(progress, text=progress_text)
            
            # Update metrics (spinner removed - progress bar provides visual feedback)
            st.session_state.processed_count = current + 1
            metric_processed.metric("Procesados", f"{current + 1}/{total}")
    
    # Check stop callback
    def check_stop() -> bool:
        """Check if stop was requested."""
        return st.session_state.get('stop_requested', False)
    
    try:
        # Show initial progress
        status_text.info("📊 Analizando archivo y calculando prioridades...")
        progress_bar.progress(0.0, text="Iniciando...")
        
        # Process file with default configuration (this will take time)
        # Note: No usar st.spinner() aquí porque bloquea los callbacks de progress
        df_result, metrics = process_file(
            input_path=tmp_input_path,
            output_path=tmp_output_path,
            tiers=[1, 3],  # Tier2 se ejecuta automáticamente si hay PRIORITY>=2
            enable_email_research=True,  # Siempre activo para Tier2
            force_tier2=False,  # Nunca forzar
            progress_callback=update_progress,
            check_stop_callback=check_stop
        )
        
        # Check if stopped
        if st.session_state.stop_requested:
            st.warning("⏸️ Procesamiento detenido por el usuario")
            st.session_state.processing = False
            spinner_placeholder.empty()
            st.rerun()
        
        # Update final metrics
        phones_count = metrics.get("phone_found", 0) if metrics else 0
        emails_count = metrics.get("emails_found", 0) if metrics else 0
        st.session_state.phones_found = phones_count
        st.session_state.emails_found = emails_count
        metric_phones.metric("Teléfonos encontrados", phones_count)
        metric_emails.metric("Emails encontrados", emails_count)
        
        # Final progress
        spinner_placeholder.empty()
        progress_bar.progress(1.0, text="✅ Completado!")
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
        
        # Guardar nombre del archivo procesado para detectar nuevos archivos
        st.session_state.last_processed_file_name = uploaded_file.name
        
        # Set processing to False
        st.session_state.processing = False
        
        logger.info(f"File processed successfully: {uploaded_file.name}, rows: {st.session_state.num_rows}")
        
        # Rerun to show results
        st.rerun()
        
    except Exception as e:
        logger.error(f"Error processing file: {e}", exc_info=True)
        # Limpiar UI y mostrar error limpio
        spinner_placeholder.empty()
        progress_bar.empty()
        status_text.empty()
        st.session_state.processing = False
        st.session_state.processing_error = str(e)
        st.session_state.processing_result = None
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
    
    # NO limpiar result state - mantener UI hasta que se cargue nuevo archivo o se cierre la página
    # El estado se mantendrá en st.session_state hasta que la sesión termine o se cargue un nuevo archivo

# Display errors
processing_error = st.session_state.get('processing_error')
if not st.session_state.processing and processing_error is not None:
    st.markdown("---")
    error_msg = processing_error
    # Simplificar mensajes técnicos para el usuario
    if "Reindexing only valid with uniquely valued Index objects" in error_msg:
        error_msg = "Error al generar el Excel: columnas duplicadas detectadas. Por favor, contacta al soporte técnico."
    elif "InvalidIndexError" in error_msg:
        error_msg = "Error al generar el Excel: problema con la estructura de datos. Por favor, contacta al soporte técnico."
    
    st.error(f"❌ **Error al procesar el archivo**\n\n{error_msg}")
    st.info("💡 **Sugerencia:** Verifica que el archivo Excel tenga un formato válido y no contenga columnas duplicadas.")
    st.session_state.processing_error = None

elif uploaded_file is None:
    st.info("👆 Por favor, carga un archivo Excel para comenzar.")
