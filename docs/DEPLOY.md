# Deploy en Streamlit Cloud

Guía paso a paso para desplegar la aplicación Lead Enrichment Engine en Streamlit Cloud y compartirla con Alejandro.

## Requisitos previos

1. ✅ Repositorio en GitHub (público o privado)
2. ✅ Cuenta en [Streamlit Cloud](https://share.streamlit.io) (gratis)
3. ✅ Archivo `requirements.txt` con todas las dependencias

## Pasos para deploy

### 1. Preparar el repositorio

Asegúrate de que tu repositorio tenga estos archivos:
- ✅ `src/streamlit_app.py` (archivo principal de Streamlit)
- ✅ `requirements.txt` (con todas las dependencias)
- ✅ `requirements_ui.txt` (con streamlit) - O añade `streamlit>=1.30.0` a `requirements.txt`
- ✅ `config/` (con todos los archivos de configuración YAML)
- ✅ `.gitignore` (para no subir archivos sensibles como `.env`)

**IMPORTANTE**: 
- No subas archivos con API keys (`.env`, `config/api_keys.yaml`). Usa Secrets de Streamlit Cloud.
- Si usas `requirements_ui.txt`, Streamlit Cloud usará `requirements.txt` por defecto. Asegúrate de que incluya `streamlit` o combina ambos archivos.

### 2. Crear cuenta en Streamlit Cloud

1. Ve a [share.streamlit.io](https://share.streamlit.io)
2. Haz clic en **"Sign in"** e inicia sesión con tu cuenta de GitHub
3. Autoriza a Streamlit Cloud a acceder a tus repositorios

### 3. Crear nueva aplicación

1. En el dashboard de Streamlit Cloud, haz clic en **"New app"**
2. Completa el formulario:
   - **Repository**: Selecciona tu repositorio (ej: `tu-usuario/lead-enrichment-engine`)
   - **Branch**: `main` (o la rama que uses)
   - **Main file path**: `src/streamlit_app.py`
3. Haz clic en **"Deploy!"**

### 4. Configurar API Keys (CRÍTICO)

La aplicación necesita API keys para funcionar. Configúralas así:

1. En la página de tu app en Streamlit Cloud, haz clic en **"⚙️ Settings"** (arriba a la derecha)
2. Ve a la pestaña **"Secrets"**
3. Añade tus API keys en formato TOML (copia y pega esto, reemplazando con tus keys reales):

```toml
GOOGLE_PLACES_API_KEY = "tu-api-key-de-google-aqui"
TAVILY_API_KEY = "tu-api-key-de-tavily-aqui"
OPENAI_API_KEY = "tu-api-key-de-openai-aqui"
```

**Nota**: 
- Las API keys se guardan de forma segura y no son visibles en el código
- Puedes obtener las keys desde los links en la sección "Cómo funciona" de la app
- Después de guardar, la app se reiniciará automáticamente

### 5. Verificar el deploy

1. Streamlit Cloud construirá tu aplicación automáticamente (puede tardar 1-2 minutos)
2. Revisa los logs en la pestaña **"Logs"** si hay errores
3. Una vez completado, tu app estará disponible en:
   `https://<tu-app-name>.streamlit.app`

### 6. Compartir la aplicación

1. Copia la URL de tu app (ej: `https://lead-enrichment-engine.streamlit.app`)
2. Compártela con quien necesite usarla - pueden acceder directamente sin necesidad de cuenta
3. La app se actualiza automáticamente cada vez que haces push a `main`

## Troubleshooting

### Error: "Module not found"

- Verifica que todas las dependencias estén en `requirements.txt`
- Asegúrate de que `streamlit>=1.30.0` esté incluido en `requirements.txt`
- Si usas `requirements_ui.txt`, combínalo con `requirements.txt` o añade streamlit a `requirements.txt`

### Error: "File not found" o problemas con rutas

- Usa rutas relativas al directorio raíz del proyecto
- No uses rutas absolutas como `/Users/...`
- Asegúrate de que los archivos de configuración estén en `config/`

### Error: "API key not found"

- Verifica que las variables de entorno estén configuradas en **Secrets** (Settings → Secrets)
- Asegúrate de usar los nombres exactos: `GOOGLE_PLACES_API_KEY`, `TAVILY_API_KEY`, `OPENAI_API_KEY`
- Revisa los logs en Streamlit Cloud para ver errores específicos

### La app se carga pero no procesa archivos

- Verifica los logs en Streamlit Cloud (pestaña "Logs")
- Asegúrate de que los archivos temporales se manejen correctamente
- Revisa que las rutas de archivos sean correctas
- Verifica que las API keys estén correctamente configuradas

## Actualizar la aplicación

Cada vez que hagas push a la rama principal (`main`), Streamlit Cloud reconstruirá automáticamente tu aplicación. No necesitas hacer nada más.

**Para compartir actualizaciones con Alejandro:**
- Simplemente haz push a `main`
- La app se actualizará automáticamente en 1-2 minutos
- Alejandro verá la nueva versión la próxima vez que recargue la página

## Notas importantes

- Streamlit Cloud tiene límites de memoria y CPU
- Los archivos temporales se limpian automáticamente después de cada sesión
- El tiempo máximo de ejecución por request es limitado
- Para archivos grandes, considera optimizar el procesamiento

## Recursos

- [Documentación de Streamlit Cloud](https://docs.streamlit.io/streamlit-community-cloud)
- [Troubleshooting Streamlit Cloud](https://docs.streamlit.io/streamlit-community-cloud/troubleshooting)
