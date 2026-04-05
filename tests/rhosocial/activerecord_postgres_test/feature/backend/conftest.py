# tests/rhosocial/activerecord_postgres_test/feature/backend/conftest.py
import asyncio
import sys
import pytest
import pytest_asyncio
import yaml
import os
from typing import Dict, Any, Tuple, Type

from rhosocial.activerecord.backend.impl.postgres import PostgresBackend, AsyncPostgresBackend
from rhosocial.activerecord.backend.impl.postgres.config import PostgresConnectionConfig


def ensure_compatible_event_loop():
    if sys.platform == "win32":
        policy = asyncio.get_event_loop_policy()
        if not isinstance(policy, asyncio.WindowsSelectorEventLoopPolicy):
            asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

ensure_compatible_event_loop()

# --- Scenario Loading Logic ---

SCENARIO_MAP: Dict[str, Dict[str, Any]] = {}

def register_scenario(name: str, config: Dict[str, Any]):
    SCENARIO_MAP[name] = config

def _load_scenarios_from_config():
    """
    Load scenarios from a configuration file.
    Uses POSTGRES_SCENARIOS_CONFIG_PATH or a default path.
    """
    config_path = None
    env_config_path = os.getenv("POSTGRES_SCENARIOS_CONFIG_PATH")

    if env_config_path and os.path.exists(env_config_path):
        config_path = env_config_path
    else:
        # Path is relative to this file's location
        default_path = os.path.join(os.path.dirname(__file__), "../../../../config", "postgres_scenarios.yaml")
        if os.path.exists(default_path):
            config_path = default_path
        elif env_config_path:
            print(f"Warning: Scenario file specified in POSTGRES_SCENARIOS_CONFIG_PATH not found: {env_config_path}")
            return

    if not config_path:
        raise FileNotFoundError(
            "No PostgreSQL scenarios configuration file found. "
            "Set POSTGRES_SCENARIOS_CONFIG_PATH or place postgres_scenarios.yaml in tests/config."
        )

    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            config_data = yaml.safe_load(f)

        if 'scenarios' not in config_data:
            raise ValueError(f"Configuration file {config_path} does not contain 'scenarios' key")

        for scenario_name, config in config_data['scenarios'].items():
            if config: # Ensure config is not null or empty
                register_scenario(scenario_name, config)

    except ImportError:
        raise ImportError("PyYAML is required to load scenario configuration files")

_load_scenarios_from_config()

def get_scenario_config(name: str) -> Dict[str, Any]:
    if name not in SCENARIO_MAP:
        if SCENARIO_MAP:
            name = next(iter(SCENARIO_MAP))
        else:
            raise ValueError("No scenarios registered or enabled")
    return SCENARIO_MAP[name]

# --- Provider Logic for Fixtures ---

class BackendFeatureProvider:
    def __init__(self):
        self._backend = None
        self._async_backend = None

    def setup_backend(self, scenario_name: str):
        print(f"DEBUG: setup_backend called with scenario_name: {scenario_name}")
        if self._backend:
            return self._backend
        config_dict = get_scenario_config(scenario_name)
        # Remove 'enabled' keyword as it's not a connection parameter
        config_dict.pop('enabled', None)
        config = PostgresConnectionConfig(**config_dict)
        self._backend = PostgresBackend(connection_config=config)
        self._backend.connect()
        # Introspect and adapt to detect server version and extensions
        self._backend.introspect_and_adapt()
        return self._backend

    async def setup_async_backend(self, scenario_name: str):
        print(f"DEBUG: setup_async_backend called with scenario_name: {scenario_name}")
        if self._async_backend:
            return self._async_backend
        config_dict = get_scenario_config(scenario_name)
        # Remove 'enabled' keyword as it's not a connection parameter
        config_dict.pop('enabled', None)
        config = PostgresConnectionConfig(**config_dict)
        self._async_backend = AsyncPostgresBackend(connection_config=config)
        await self._async_backend.connect()
        # Introspect and adapt to detect server version and extensions
        await self._async_backend.introspect_and_adapt()
        return self._async_backend

    def cleanup(self):
        if self._backend:
            self._backend.disconnect()
            self._backend = None

    async def async_cleanup(self):
        if self._async_backend:
            await self._async_backend.disconnect()
            self._async_backend = None

# --- Fixtures ---

def get_scenario_names():
    return list(SCENARIO_MAP.keys())

@pytest.fixture(scope="function", params=get_scenario_names())
def postgres_backend(request):
    scenario_name = request.param
    provider = BackendFeatureProvider()
    backend = provider.setup_backend(scenario_name)
    backend.scenario_name = scenario_name # Attach for test reference
    yield backend
    provider.cleanup()

@pytest_asyncio.fixture(scope="function", params=get_scenario_names())
async def async_postgres_backend(request):
    scenario_name = request.param
    provider = BackendFeatureProvider()
    backend = await provider.setup_async_backend(scenario_name)
    backend.scenario_name = scenario_name # Attach for test reference
    yield backend
    await provider.async_cleanup()


# --- Non-parameterized Single Backend Fixtures ---


@pytest.fixture(scope="function")
def postgres_backend_single():
    """Non-parameterized fixture using the first available scenario."""
    scenario_names = get_scenario_names()
    if not scenario_names:
        pytest.skip("No PostgreSQL scenarios configured")
    scenario_name = scenario_names[0]
    provider = BackendFeatureProvider()
    backend = provider.setup_backend(scenario_name)
    backend.scenario_name = scenario_name
    yield backend
    provider.cleanup()


@pytest_asyncio.fixture(scope="function")
async def async_postgres_backend_single():
    """Non-parameterized async fixture using the first available scenario."""
    scenario_names = get_scenario_names()
    if not scenario_names:
        pytest.skip("No PostgreSQL scenarios configured")
    scenario_name = scenario_names[0]
    provider = BackendFeatureProvider()
    backend = await provider.setup_async_backend(scenario_name)
    backend.scenario_name = scenario_name
    yield backend
    await provider.async_cleanup()


# --- Control Backend for Session Modification Tests ---


@pytest.fixture(scope="function")
def postgres_control_backend():
    """
    Dedicated control backend for tests that modify PostgreSQL session settings.

    This fixture provides an independent backend instance for operations that
    need to control or interfere with the main test backend, such as:
    - Terminating other connections (pg_terminate_backend)
    - Setting global variables
    - Monitoring other connections

    The fixture is NOT parameterized to ensure consistent behavior across
    all PostgreSQL scenarios.
    """
    scenario_names = get_scenario_names()
    if not scenario_names:
        pytest.skip("No PostgreSQL scenarios configured")
    scenario_name = scenario_names[0]
    provider = BackendFeatureProvider()
    backend = provider.setup_backend(scenario_name)
    yield backend
    provider.cleanup()


@pytest_asyncio.fixture(scope="function")
async def async_postgres_control_backend():
    """
    Dedicated async control backend for tests that modify PostgreSQL session settings.

    This fixture provides an independent async backend instance for operations that
    need to control or interfere with the main test backend.
    """
    scenario_names = get_scenario_names()
    if not scenario_names:
        pytest.skip("No PostgreSQL scenarios configured")
    scenario_name = scenario_names[0]
    provider = BackendFeatureProvider()
    backend = await provider.setup_async_backend(scenario_name)
    yield backend
    await provider.async_cleanup()
