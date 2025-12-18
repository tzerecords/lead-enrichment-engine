"""Configuration loader for YAML files."""

import yaml
from pathlib import Path
from typing import Dict, Any
import logging

logger = logging.getLogger("lead_enrichment")


def load_yaml(filepath: Path) -> Dict[str, Any]:
    """Load YAML file and return as dictionary.

    Args:
        filepath: Path to YAML file.

    Returns:
        Dictionary with YAML contents.

    Raises:
        FileNotFoundError: If file doesn't exist.
        yaml.YAMLError: If YAML is malformed.
    """
    try:
        if not filepath.exists():
            raise FileNotFoundError(f"Config file not found: {filepath}")

        with open(filepath, "r", encoding="utf-8") as f:
            config = yaml.safe_load(f)

        if config is None:
            logger.warning(f"YAML file is empty: {filepath}")
            return {}

        return config

    except yaml.YAMLError as e:
        logger.error(f"Error parsing YAML file {filepath}: {e}")
        raise
    except Exception as e:
        logger.error(f"Error loading YAML file {filepath}: {e}")
        raise


def load_priority_rules() -> Dict[str, Any]:
    """Load priority rules from config/rules/priority_rules.yaml.

    Returns:
        Dictionary with priority rules.

    Raises:
        FileNotFoundError: If priority_rules.yaml doesn't exist.
    """
    # Get project root (assuming config_loader.py is in src/utils/)
    project_root = Path(__file__).parent.parent.parent
    rules_path = project_root / "config" / "rules" / "priority_rules.yaml"

    logger.info(f"Loading priority rules from {rules_path}")
    return load_yaml(rules_path)

