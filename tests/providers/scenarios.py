# tests/providers/scenarios.py
"""postgres backend test scenario configuration mapping table"""

import os
from typing import Dict, Any, Tuple, Type
from rhosocial.activerecord.backend.impl.postgres.backend import PostgresBackend
from rhosocial.activerecord.backend.impl.postgres.config import PostgresConnectionConfig

# Scenario name -> configuration dictionary mapping table (postgres only)
SCENARIO_MAP: Dict[str, Dict[str, Any]] = {}


def register_scenario(name: str, config: Dict[str, Any]):
    """Register postgres test scenario"""
    SCENARIO_MAP[name] = config


def get_scenario(name: str) -> Tuple[Type[PostgresBackend], PostgresConnectionConfig]:
    """
    Retrieves the backend class and a connection configuration object for a given
    scenario name. This is called by the provider to set up the database for a test.
    """
    if name not in SCENARIO_MAP:
        # If the specified scenario is not found, use the first available one as a fallback
        if SCENARIO_MAP:
            name = next(iter(SCENARIO_MAP))
        else:
            raise ValueError("No scenarios registered")

    # Unpack the configuration dictionary into the dataclass constructor.
    config = PostgresConnectionConfig(**SCENARIO_MAP[name])
    return PostgresBackend, config


def get_enabled_scenarios() -> Dict[str, Any]:
    """
    Returns the map of all currently enabled scenarios. The testsuite's conftest
    uses this to parameterize the tests, causing them to run for each scenario.
    """
    return SCENARIO_MAP


def _load_scenarios_from_config():
    """
    Load scenarios from a configuration file with the following priority:
    1. Environment variable specified path (highest priority)
    2. Default path tests/config/postgres_scenarios.yaml (lowest priority)
    If no valid configuration file is found, terminate with an error.
    """
    try:
        import yaml
    except ImportError:
        raise ImportError("PyYAML is required to load postgres scenario configuration files. Please install it.")

    # First, try to load from path specified in environment variable
    env_config_path = os.getenv("POSTGRES_SCENARIOS_CONFIG_PATH")
    if env_config_path and os.path.exists(env_config_path):
        print(f"Loading postgres scenarios from environment-specified path: {env_config_path}")
        config_path = env_config_path
    else:
        # Default path with lowest priority
        # Note: The path is relative to this file's location.
        config_path = os.path.join(os.path.dirname(__file__), "..", "config", "postgres_scenarios.yaml")
        if not os.path.exists(config_path):
            raise FileNotFoundError(
                "No postgres scenarios configuration file found. "
                "Either set POSTGRES_SCENARIOS_CONFIG_PATH environment variable to point to a valid YAML file "
                f"or place postgres_scenarios.yaml in the {os.path.dirname(config_path)} directory."
            )
        print(f"Loading postgres scenarios from default path: {config_path}")

    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            config_data = yaml.safe_load(f)

        if 'scenarios' not in config_data:
            raise ValueError(f"Configuration file {config_path} does not contain 'scenarios' key")

        for scenario_name, config in config_data['scenarios'].items():
            register_scenario(scenario_name, config)

    except Exception as e:
        raise RuntimeError(f"Failed to load or parse scenario configuration file {config_path}: {e}")


def _register_default_scenarios():
    """
    Registers the scenarios loaded from an external configuration file.
    No hardcoded scenarios are registered in the code itself.
    """
    _load_scenarios_from_config()


# Register default scenarios during initialization
_register_default_scenarios()
