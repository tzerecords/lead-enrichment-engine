"""Test that orchestrator correctly updates OBSERVACIONES column."""

import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import pandas as pd
from src.core.orchestrator import run_pipeline


def test_observaciones_update():
    """Test that OBSERVACIONES is correctly updated with validation results."""
    # Sample data matching Excel structure
    df = pd.DataFrame({
        'CIF/NIF': ['A12345674', '12345678Z'],
        'TELÉFONO': ['612345678', '914567890'],
        'NOMBRE EMPRESA': ['Test SA', 'Autónomo'],
        'MWh TOTAL': [100.0, 50.0],
        'OBSERVACIONES': ['', 'Nota previa'],
        '_IS_RED_ROW': [False, False]
    })

    print("=== TEST OBSERVACIONES UPDATE ===\n")
    print("Input DataFrame:")
    print(df[['NOMBRE EMPRESA', 'CIF/NIF', 'TELÉFONO', 'OBSERVACIONES']])
    print()

    # Run pipeline
    df_result, report = run_pipeline(df, tier1_only=True)

    print("Output DataFrame:")
    print(df_result[['NOMBRE EMPRESA', 'OBSERVACIONES']])
    print()

    # Verify results
    print("=== VERIFICATION ===")
    for idx, row in df_result.iterrows():
        nombre = row['NOMBRE EMPRESA']
        obs = str(row['OBSERVACIONES'] or '')
        print(f"\n{nombre}:")
        print(f"  OBSERVACIONES: {obs}")
        
        # Check that OBSERVACIONES contains validation results
        if nombre == 'Test SA':
            assert 'CIF/NIF/NIE' in obs, f"Expected CIF validation in OBSERVACIONES, got: {obs}"
            assert 'Teléfono' in obs, f"Expected phone validation in OBSERVACIONES, got: {obs}"
            assert 'A12345674' in obs or 'CIF' in obs, f"Expected CIF value in OBSERVACIONES, got: {obs}"
            print("  ✅ Test SA: OBSERVACIONES contains CIF and phone validation")
        elif nombre == 'Autónomo':
            assert 'Nota previa' in obs, f"Expected 'Nota previa' to be preserved, got: {obs}"
            assert 'CIF/NIF/NIE' in obs, f"Expected CIF validation in OBSERVACIONES, got: {obs}"
            assert 'Teléfono' in obs, f"Expected phone validation in OBSERVACIONES, got: {obs}"
            print("  ✅ Autónomo: OBSERVACIONES preserves previous content and adds validations")

    print("\n✅ All tests passed! OBSERVACIONES is correctly updated.")


if __name__ == '__main__':
    test_observaciones_update()
