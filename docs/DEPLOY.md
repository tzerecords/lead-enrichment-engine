# Deploy en Streamlit Cloud

Guía para desplegar la aplicación Lead Enrichment Engine en Streamlit Cloud.

## Requisitos previos

1. Repositorio en GitHub (público o privado)
2. Cuenta en [Streamlit Cloud](https://share.streamlit.io)
3. Archivo `requirements.txt` con todas las dependencias

## Pasos para deploy

### 1. Preparar el repositorio

Asegúrate de que tu repositorio tenga:
- ✅ `src/streamlit_app.py` (archivo principal de Streamlit)
- ✅ `requirements.txt` (con todas las dependencias, incluyendo streamlit)
- ✅ `config/` (con todos los archivos de configuración)
- ✅ `.streamlit/config.toml` (opcional, para configuración de Streamlit)

### 2. Crear cuenta en Streamlit Cloud

1. Ve a [share.streamlit.io](https://share.streamlit.io)
2. Inicia sesión con tu cuenta de GitHub
3. Autoriza a Streamlit Cloud a acceder a tus repositorios

### 3. Crear nueva aplicación

1. Haz clic en **"New app"**
2. Selecciona:
   - **Repository**: Tu repositorio (`tzerecords/lead-enrichment-engine`)
   - **Branch**: `main` (o la rama que uses)
   - **Main file path**: `src/streamlit_app.py`
3. Haz clic en **"Deploy!"**

### 4. Configurar variables de entorno (si es necesario)

Si tu aplicación necesita API keys o variables de entorno:

1. En la página de tu app en Streamlit Cloud, haz clic en **"Settings"** (⚙️)
2. Ve a **"Secrets"**
3. Añade tus variables de entorno en formato TOML:

```toml
[api_keys]
GOOGLE_PLACES_API_KEY = "tu-api-key"
OPENAI_API_KEY = "tu-api-key"
# ... etc
```

### 5. Verificar el deploy

1. Streamlit Cloud construirá tu aplicación automáticamente
2. Revisa los logs si hay errores
3. Una vez completado, tu app estará disponible en:
   `https://<tu-app-name>.streamlit.app`

## Troubleshooting

### Error: "Module not found"

- Verifica que todas las dependencias estén en `requirements.txt`
- Asegúrate de que `streamlit` esté incluido

### Error: "File not found" o problemas con rutas

- Usa rutas relativas al directorio raíz del proyecto
- No uses rutas absolutas como `/Users/...`

### Error: "API key not found"

- Verifica que las variables de entorno estén configuradas en Secrets
- Asegúrate de que el código lea las variables correctamente

### La app se carga pero no procesa archivos

- Verifica los logs en Streamlit Cloud
- Asegúrate de que los archivos temporales se manejen correctamente
- Revisa que las rutas de archivos sean correctas

## Actualizar la aplicación

Cada vez que hagas push a la rama principal (`main`), Streamlit Cloud reconstruirá automáticamente tu aplicación.

## Notas importantes

- Streamlit Cloud tiene límites de memoria y CPU
- Los archivos temporales se limpian automáticamente después de cada sesión
- El tiempo máximo de ejecución por request es limitado
- Para archivos grandes, considera optimizar el procesamiento

## Recursos

- [Documentación de Streamlit Cloud](https://docs.streamlit.io/streamlit-community-cloud)
- [Troubleshooting Streamlit Cloud](https://docs.streamlit.io/streamlit-community-cloud/troubleshooting)
