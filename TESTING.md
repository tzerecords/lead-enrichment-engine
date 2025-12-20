# Guía de Testing - Lead Enrichment Engine

## 🚀 Inicio Rápido

### 1. Instalar dependencias

```bash
# Activar entorno virtual (si existe)
source .venv/bin/activate  # macOS/Linux
# o
.venv\Scripts\activate  # Windows

# Instalar dependencias base
pip install -r requirements.txt

# Instalar dependencias de UI
pip install -r requirements_ui.txt
```

### 2. Configurar API Keys

```bash
# Copiar template
cp config/api_keys.yaml.example config/api_keys.yaml

# Editar con tus keys
# Necesitas: OPENAI_API_KEY, TAVILY_API_KEY, GOOGLE_PLACES_API_KEY
```

### 3. Ejecutar Streamlit

```bash
streamlit run src/streamlit_app.py
```

La app se abrirá en: `http://localhost:8501`

---

## ✅ Checklist de Testing

### Test 1: File Cache Bug Fix
- [ ] Sube un archivo Excel (ej: `tests/sample_data.xlsx`)
- [ ] Verifica que muestra "✅ Nuevo archivo detectado"
- [ ] Sube el MISMO archivo otra vez
- [ ] Debe mostrar "📄 Archivo ya cargado" (NO procesar de nuevo)
- [ ] Sube un archivo DIFERENTE
- [ ] Debe detectar como nuevo archivo

### Test 2: Row Limit & Cost Warning
- [ ] Sube un archivo con <10 filas → No debe mostrar advertencia de costo
- [ ] Sube un archivo con 50-100 filas → Debe mostrar costo estimado
- [ ] Sube un archivo con >100 filas → Debe mostrar WARNING rojo
- [ ] Verifica que muestra número correcto de filas detectadas

### Test 3: Cancel Button
- [ ] Sube archivo y haz clic en "✅ Procesar"
- [ ] Durante procesamiento, verifica que aparece botón "🛑 DETENER PROCESAMIENTO"
- [ ] Haz clic en STOP → Debe cancelar y mostrar mensaje de cancelación
- [ ] Verifica que no sigue procesando después de cancelar

### Test 4: Processing Flow Completo
- [ ] Sube archivo pequeño (2-5 filas) para test rápido
- [ ] Selecciona Tier1 + Tier3 (Tier2 opcional)
- [ ] Haz clic en "✅ Procesar"
- [ ] Espera a que complete
- [ ] Verifica que muestra métricas (Total, Alta calidad, Emails válidos, Errores)
- [ ] Verifica que aparece botón de descarga
- [ ] Descarga el Excel y verifica que tiene 3 hojas:
  - [ ] "BBDD ORIGINAL"
  - [ ] "HIGHLIGHT"
  - [ ] "DATOS_TÉCNICOS"

### Test 5: Error Handling
- [ ] Sube un archivo corrupto o inválido
- [ ] Verifica que muestra error claro
- [ ] Verifica que no crashea la app

### Test 6: API Status
- [ ] Verifica que el expander "🔧 Estado de APIs" muestra estado correcto
- [ ] Si falta alguna key, debe mostrar ⚠️
- [ ] Si todas están configuradas, debe mostrar ✅

---

## 🐛 Debugging

### Ver logs en tiempo real

```bash
# En otra terminal, mientras Streamlit corre:
tail -f debug.log
# o
tail -f output.log
```

### Errores comunes

**Error: "Module not found"**
```bash
# Asegúrate de estar en el directorio raíz del proyecto
cd /Users/matiaswas/Code/MVPs/lead-enrichment-engine
streamlit run src/streamlit_app.py
```

**Error: "API key not found"**
- Verifica que `config/api_keys.yaml` existe
- Verifica que tiene las keys correctas
- Verifica que las variables de entorno están configuradas (si las usas)

**Error: "File not found"**
- Verifica que el archivo Excel existe
- Verifica permisos de lectura

---

## 📊 Testing con Archivos de Prueba

### Archivo pequeño (test rápido)
```bash
# Usa el archivo de test existente
tests/sample_data.xlsx  # Si existe
# o
tests/m3_test_data.xlsx  # Si existe
```

### Crear archivo de test manualmente
1. Abre Excel
2. Crea columnas: `CIF/NIF`, `NOMBRE CLIENTE`, `TELEFONO 1`, `MAIL`, `CONSUMO`, `L/V`
3. Añade 3-5 filas de datos de prueba
4. Guarda como `.xlsx`

---

## 🚀 Próximos Pasos Después de Testing

Una vez que todo funciona:

1. **Deploy en Streamlit Cloud**
   - Ver `docs/DEPLOY.md`
   - Push a GitHub
   - Deploy en share.streamlit.io

2. **Optimizaciones**
   - Ajustar `MAX_ROWS_PER_BATCH` si es necesario
   - Ajustar `COST_PER_LEAD_ESTIMATE` según uso real
   - Añadir más validaciones si es necesario

3. **Mejoras de UX**
   - Añadir más métricas
   - Mejorar visualización de resultados
   - Añadir historial de procesamientos

---

## 📝 Notas

- **Primera ejecución puede ser lenta**: Streamlit carga todas las dependencias
- **Procesar archivos grandes**: Puede tomar varios minutos, ten paciencia
- **API costs**: Monitorea el uso de APIs, especialmente OpenAI/Tavily
- **Logs**: Revisa `debug.log` o `output.log` para detalles de procesamiento
