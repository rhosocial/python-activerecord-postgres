# tests/rhosocial/activerecord_postgres_test/feature/backend/conftest.py
"""Fixtures for the backend-owned tests.

The backend runs these against a real server. The scenario supplies the
connection parameters; the database name is derived from the directory that
owns the test, so two directories never share tables.
"""
import os
from typing import Any, Dict, Tuple, Type

import pytest
import pytest_asyncio

import support.postgres

from rhosocial.activerecord.backend.impl.postgres.backend import PostgresBackend
from rhosocial.activerecord.backend.impl.postgres.backend.async_backend import (
    AsyncPostgresBackend,
)
from rhosocial.activerecord.backend.impl.postgres.config import PostgresConnectionConfig

# --- Scenario Loading Logic ---

SCENARIO_MAP: Dict[str, Dict[str, Any]] = dict(support.postgres.SCENARIO_MAP)


def _apply_scenario_filter():
    """Filter SCENARIO_MAP based on the active scenarios env var.

    Keeps the backend-feature scenarios consistent with the scenarios active
    for the rest of the test run.
    """
    filter_str = os.getenv("POSTGRES_ACTIVE_SCENARIOS") or os.getenv(
        "TESTSUITE_ACTIVE_SCENARIOS"
    )
    if not filter_str:
        return
    allowed = set(s.strip() for s in filter_str.split(",") if s.strip())
    if not allowed:
        return
    for name in [n for n in SCENARIO_MAP if n not in allowed]:
        del SCENARIO_MAP[name]


_apply_scenario_filter()


def get_enabled_scenarios() -> Dict[str, Any]:
    return SCENARIO_MAP


def get_scenario_names():
    return list(get_enabled_scenarios().keys())


_PREPARED_DATABASES = set()


def _database_for(request) -> str:
    """Derive the logical database the directory owning the test runs in."""
    parts = list(request.node.path.parts)
    try:
        index = max(i for i, part in enumerate(parts) if part == "backend")
    except ValueError:
        return "test_backend"
    names = [part for part in parts[index + 1 : -1] if part]
    return "test_backend" + "".join("_" + name for name in names)


def _prepare_database(scenario_name: str, database: str) -> None:
    """Create the directory's database once per scenario."""
    key = (scenario_name, database)
    if key in _PREPARED_DATABASES:
        return
    support.postgres.scenario(scenario_name).reset_database(database)
    _PREPARED_DATABASES.add(key)


def get_scenario(
    name: str, database: str = "test_backend"
) -> Tuple[Type[PostgresBackend], PostgresConnectionConfig]:
    if name not in SCENARIO_MAP:
        if SCENARIO_MAP:
            name = next(iter(SCENARIO_MAP))
        else:
            raise ValueError("No scenarios registered")
    _prepare_database(name, database)
    return PostgresBackend, support.postgres.scenario(name).make_config(database)


def pytest_collection_modifyitems(config, items):
    """Pin a backend directory's tests to one worker.

    These directories create and drop tables in the directory's own database,
    so two workers must never run tests from the same directory at once.
    """
    for item in items:
        parts = item.path.parts
        if "backend" not in parts:
            continue
        item.add_marker(pytest.mark.xdist_group("postgres-backend::" + str(item.path.parent)))


# --- Provider Logic for Fixtures ---


class BackendFeatureProvider:
    def __init__(self):
        self._backend = None
        self._async_backend = None

    def setup_backend(self, scenario_name: str, database: str):
        if self._backend:
            return self._backend
        backend_class, config = get_scenario(scenario_name, database)
        self._backend = backend_class(connection_config=config)
        self._backend.connect()
        # Introspect and adapt to detect server version and extensions.
        self._backend.introspect_and_adapt()
        return self._backend

    async def setup_async_backend(self, scenario_name: str, database: str):
        if self._async_backend:
            return self._async_backend
        _, config = get_scenario(scenario_name, database)
        self._async_backend = AsyncPostgresBackend(connection_config=config)
        await self._async_backend.connect()
        # Introspect and adapt to detect server version and extensions.
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


@pytest.fixture(scope="function", params=get_scenario_names())
def postgres_backend(request):
    scenario_name = request.param
    provider = BackendFeatureProvider()
    backend = provider.setup_backend(scenario_name, _database_for(request))
    backend.scenario_name = scenario_name  # Attach for test reference
    yield backend
    provider.cleanup()


@pytest_asyncio.fixture(scope="function", params=get_scenario_names())
async def async_postgres_backend(request):
    scenario_name = request.param
    provider = BackendFeatureProvider()
    backend = await provider.setup_async_backend(scenario_name, _database_for(request))
    backend.scenario_name = scenario_name  # Attach for test reference
    yield backend
    await provider.async_cleanup()


# --- Non-parameterized Single Backend Fixtures ---


@pytest.fixture(scope="function")
def postgres_backend_single(request):
    """Non-parameterized fixture using the first available scenario."""
    scenario_names = get_scenario_names()
    if not scenario_names:
        pytest.skip("No PostgreSQL scenarios configured")
    scenario_name = scenario_names[0]
    provider = BackendFeatureProvider()
    backend = provider.setup_backend(scenario_name, _database_for(request))
    backend.scenario_name = scenario_name
    yield backend
    provider.cleanup()


@pytest_asyncio.fixture(scope="function")
async def async_postgres_backend_single(request):
    """Non-parameterized async fixture using the first available scenario."""
    scenario_names = get_scenario_names()
    if not scenario_names:
        pytest.skip("No PostgreSQL scenarios configured")
    scenario_name = scenario_names[0]
    provider = BackendFeatureProvider()
    backend = await provider.setup_async_backend(scenario_name, _database_for(request))
    backend.scenario_name = scenario_name
    yield backend
    await provider.async_cleanup()


# --- Class-scoped Backend Fixtures ---


@pytest.fixture(scope="class")
def postgres_backend_class(request):
    """Class-scoped backend fixture for extension integration tests.

    Uses the first available scenario. Suitable for test classes where
    the backend is shared across all test methods in the class.
    """
    scenario_names = get_scenario_names()
    if not scenario_names:
        pytest.skip("No PostgreSQL scenarios configured")
    scenario_name = scenario_names[0]
    provider = BackendFeatureProvider()
    backend = provider.setup_backend(scenario_name, _database_for(request))
    backend.scenario_name = scenario_name
    yield backend
    provider.cleanup()


@pytest_asyncio.fixture(scope="class")
async def async_postgres_backend_class(request):
    """Class-scoped async backend fixture for extension integration tests."""
    scenario_names = get_scenario_names()
    if not scenario_names:
        pytest.skip("No PostgreSQL scenarios configured")
    scenario_name = scenario_names[0]
    provider = BackendFeatureProvider()
    backend = await provider.setup_async_backend(scenario_name, _database_for(request))
    backend.scenario_name = scenario_name
    yield backend
    await provider.async_cleanup()


# --- Control Backend for Session Modification Tests ---


@pytest.fixture(scope="function")
def postgres_control_backend(request):
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
    backend = provider.setup_backend(scenario_name, _database_for(request))
    yield backend
    provider.cleanup()


@pytest_asyncio.fixture(scope="function")
async def async_postgres_control_backend(request):
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
    backend = await provider.setup_async_backend(scenario_name, _database_for(request))
    yield backend
    await provider.async_cleanup()


@pytest.fixture(scope="function")
def postgres_dialect():
    """Fixture providing PostgresDialect instance for testing transaction expressions."""
    from rhosocial.activerecord.backend.impl.postgres.dialect import PostgresDialect

    return PostgresDialect(version=(15, 0, 0))


# --- Extension Availability Utilities ---
# These are defined in utils.py and automatically available via conftest
# for tests that don't need explicit import.
# For explicit import in test files, use: from rhosocial.activerecord_postgres_test.feature.backend.utils import requires_extension  # noqa: E501
from rhosocial.activerecord_postgres_test.feature.backend.utils import (  # noqa: E402, F401
    requires_extension,
    requires_extension_available,
    skip_if_extension_missing,
)
