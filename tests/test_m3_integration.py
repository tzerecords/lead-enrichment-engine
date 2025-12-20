"""Integration tests for M3 pipeline."""

import sys
from pathlib import Path

# Add project root to Python path
sys.path.insert(0, str(Path(__file__).parent.parent))

import pandas as pd
import pytest
from src.core.orchestrator import run_tier3_and_validation
from src.utils.config_loader import load_yaml_config


def test_m3_full_pipeline():
    """Test completo del pipeline M3."""
    # Cargar reglas
    enrichment_rules = load_yaml_config("config/rules/enrichment_rules.yaml")
    validation_rules = load_yaml_config("config/rules/validation_rules.yaml")

    # DataFrame de prueba (10 filas)
    df = pd.DataFrame({
        'CIF': ['A12345678', 'B87654321', 'C11111111', 'D22222222', 'E33333333', 
                'F44444444', 'G55555555', 'H66666666', 'I77777777', 'J88888888'],
        'RAZON_SOCIAL': ['Empresa A', 'Empresa B', 'Empresa C', 'Empresa D', 'Empresa E',
                         'Empresa F', 'Empresa G', 'Empresa H', 'Empresa I', 'Empresa J'],
        'EMAIL': ['valid@example.com', 'invalid.email', '', 'test@', 'good@test.com',
                  'another@valid.com', 'bad@', '', 'ok@mail.es', 'final@company.com'],
        'TELEFONO': ['612345678', '12345', '987654321', '5555', '611222333',
                     '922334455', '99', '633445566', '', '644556677'],
        'WEBSITE': ['', 'https://existing.com', '', '', 'https://another.com',
                    '', '', 'https://third.com', '', ''],
        'CNAE': ['', '1234', '', '', '5678',
                 '', '', '9012', '', ''],
        'CONSUMO_MWH': [100, 200, 50, 300, 150, 80, 20, 250, 180, 90],
        'OBSERVACIONES': ['Nota1', 'Nota2', 'Nota3', 'Nota4', 'Nota5',
                          'Nota6', 'Nota7', 'Nota8', 'Nota9', 'Nota10']
    })

    # Ejecutar pipeline M3
    df_result = run_tier3_and_validation(df.copy(), enable_tier3=True)

    # Verificaciones
    assert 'COMPLETITUD_SCORE' in df_result.columns
    assert 'CONFIDENCE_SCORE' in df_result.columns
    assert 'DATA_QUALITY' in df_result.columns
    assert 'DATA_SOURCES' in df_result.columns
    assert 'LAST_UPDATED' in df_result.columns
    assert 'EMAIL_VALID' in df_result.columns
    assert 'PHONE_VALID' in df_result.columns

    # Verificar que valores existentes NO se sobrescribieron
    assert df_result.loc[1, 'WEBSITE'] == 'https://existing.com'
    assert df_result.loc[1, 'CNAE'] == '1234'

    # Verificar emails inválidos marcados
    assert df_result.loc[1, 'EMAIL_VALID'] == False  # 'invalid.email'
    assert df_result.loc[3, 'EMAIL_VALID'] == False  # 'test@'

    # Verificar teléfonos inválidos
    assert df_result.loc[1, 'PHONE_VALID'] == False  # '12345' muy corto

    # Estadísticas
    print("\n📊 ESTADÍSTICAS M3:")
    print(f"Emails válidos: {df_result['EMAIL_VALID'].sum()}/{len(df_result)}")
    print(f"Teléfonos válidos: {df_result['PHONE_VALID'].sum()}/{len(df_result)}")
    print(f"DATA_QUALITY distribution:")
    print(df_result['DATA_QUALITY'].value_counts())
    print(f"COMPLETITUD_SCORE promedio: {df_result['COMPLETITUD_SCORE'].mean():.1f}")

    return df_result
