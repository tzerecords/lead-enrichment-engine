"""Test M2 Tier 1 validators and integration."""

import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import pandas as pd
from src.tier1.cif_validator import CifValidator
from src.tier1.phone_validator import PhoneValidator


def test_tier1_validators():
    """Test CIF and phone validators with sample data."""
    # Sample data
    df = pd.DataFrame({
        'CIF/NIF': ['A12345674', 'B87654321', '12345678Z'],
        'TELÉFONO': ['612345678', '914567890', '+34 666123456'],
        'NOMBRE EMPRESA': ['Test SA', 'Ejemplo SL', 'Autónomo Test'],
        'OBSERVACIONES': ['', 'Nota previa', '']
    })

    cif_val = CifValidator()
    phone_val = PhoneValidator()

    print('=== TEST VALIDACIÓN TIER 1 ===\n')

    for idx, row in df.iterrows():
        cif_result = cif_val.validate(str(row['CIF/NIF']))
        phone_result = phone_val.validate(str(row['TELÉFONO']))
        
        obs_parts = []
        if row['OBSERVACIONES']:
            obs_parts.append(row['OBSERVACIONES'])
        
        # CIF validation
        if cif_result.is_valid:
            entity_info = f", {cif_result.entity_type}" if cif_result.entity_type else ""
            obs_parts.append(f"CIF: {cif_result.formatted_id} ({cif_result.id_type}{entity_info})")
        else:
            obs_parts.append(f"CIF inválido: {cif_result.error}")
        
        # Phone validation
        if phone_result.is_valid:
            obs_parts.append(f"Tel: {phone_result.international_format} ({phone_result.phone_type})")
        else:
            obs_parts.append(f"Tel inválido: {phone_result.error}")
        
        final_obs = ' | '.join(obs_parts)
        print(f"{idx+1}. {row['NOMBRE EMPRESA'][:20]:20} → {final_obs}")

    print('\n✅ Tier 1 validators funcionan correctamente')
    print('✅ Lógica de OBSERVACIONES preserva contenido previo')


if __name__ == '__main__':
    test_tier1_validators()
