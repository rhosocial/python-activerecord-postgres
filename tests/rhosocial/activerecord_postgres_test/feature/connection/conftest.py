# tests/rhosocial/activerecord_postgres_test/feature/connection/conftest.py
"""L2 backend fixtures for the connection-pool tests.

The pool fixtures own their connections: each creates the directory's database
for the scenario it runs under and hands out PostgreSQL backends from it. The
concurrency tables are created and dropped by the table fixtures through that
pool, from the definitions in ``pool_definitions.py``.

``database_config`` declares the directory's identity (scenario, configuration
and database name). It exists for the xdist grouping contract: a scenario's
tests for one directory stay on one worker, while different scenarios are
different databases and may run in parallel.
"""

from functools import partial
from pathlib import Path
from typing import Generator

import pytest
import pytest_asyncio

from rhosocial.activerecord.backend.options import ExecutionOptions
from rhosocial.activerecord.backend.schema import StatementType
from rhosocial.activerecord.connection.pool import (
    AsyncBackendPool,
    BackendPool,
    PoolConfig,
)

from pool_definitions import (
    AsyncConcurrentTestPostsDefinition,
    AsyncConcurrentTestUsersDefinition,
    ConcurrentTestPostsDefinition,
    ConcurrentTestUsersDefinition,
)

DDL = ExecutionOptions(stmt_type=StatementType.DDL)


def _database_name() -> str:
    """Database name from this directory's namespace (below ``feature``)."""
    parts = Path(__file__).parent.parts
    start = parts.index("feature") + 1
    return "test_" + "_".join(parts[start:])


@pytest.fixture(scope="session")
def database_config(backend_scenario):
    """The scenario, its connection configuration, and the directory database name."""
    name = _database_name()
    return backend_scenario, backend_scenario.make_config(name), name


@pytest.fixture(scope="function")
def postgres_pool(database_config) -> Generator[BackendPool, None, None]:
    """Create a BackendPool with PostgreSQL backends for testing."""
    scenario, config, name = database_config
    scenario.reset_database(name)
    pool_config = PoolConfig(
        min_size=1,
        max_size=5,
        timeout=30.0,
        validate_on_borrow=True,
        validation_query="SELECT 1",
        backend_factory=partial(scenario.backend_class, connection_config=config),
    )

    pool = BackendPool.create(pool_config)
    yield pool
    pool.close(timeout=5.0, force=True)


@pytest_asyncio.fixture(scope="function")
async def async_postgres_pool(database_config) -> AsyncBackendPool:
    """Create an AsyncBackendPool with PostgreSQL backends for testing."""
    scenario, config, name = database_config
    scenario.reset_database(name)
    pool_config = PoolConfig(
        min_size=1,
        max_size=5,
        timeout=30.0,
        validate_on_borrow=True,
        validation_query="SELECT 1",
        backend_factory=partial(scenario.async_backend_class, connection_config=config),
    )

    pool = await AsyncBackendPool.create(pool_config)
    yield pool
    await pool.close(timeout=5.0, force=True)


@pytest.fixture(scope="function")
def postgres_pool_large(database_config) -> Generator[BackendPool, None, None]:
    """Create a larger BackendPool for stress testing."""
    scenario, config, name = database_config
    scenario.reset_database(name)
    pool_config = PoolConfig(
        min_size=2,
        max_size=10,
        timeout=60.0,
        validate_on_borrow=False,  # Faster for stress tests
        backend_factory=partial(scenario.backend_class, connection_config=config),
    )

    pool = BackendPool.create(pool_config)
    yield pool
    pool.close(timeout=5.0, force=True)


@pytest_asyncio.fixture(scope="function")
async def async_postgres_pool_large(database_config) -> AsyncBackendPool:
    """Create a larger AsyncBackendPool for stress testing."""
    scenario, config, name = database_config
    scenario.reset_database(name)
    pool_config = PoolConfig(
        min_size=2,
        max_size=10,
        timeout=60.0,
        validate_on_borrow=False,
        backend_factory=partial(scenario.async_backend_class, connection_config=config),
    )

    pool = await AsyncBackendPool.create(pool_config)
    yield pool
    await pool.close(timeout=5.0, force=True)


# --- Table Setup Fixtures ---


@pytest.fixture(scope="function")
def postgres_pool_with_tables(postgres_pool: BackendPool) -> Generator[BackendPool, None, None]:
    """Create a pool with test tables initialized."""
    definitions = (
        ConcurrentTestPostsDefinition,
        ConcurrentTestUsersDefinition,
    )
    with postgres_pool.connection() as backend:
        fixtures = [definition(backend.dialect) for definition in definitions]
        for fixture in fixtures:
            backend.execute(*fixture.drop_table.to_sql(), options=DDL)
            backend.execute(*fixture.create_table.to_sql(), options=DDL)

    yield postgres_pool

    with postgres_pool.connection() as backend:
        for fixture in reversed(fixtures):
            backend.execute(*fixture.drop_table.to_sql(), options=DDL)


@pytest_asyncio.fixture(scope="function")
async def async_postgres_pool_with_tables(
    async_postgres_pool: AsyncBackendPool,
) -> AsyncBackendPool:
    """Create an async pool with test tables initialized."""
    definitions = (
        AsyncConcurrentTestPostsDefinition,
        AsyncConcurrentTestUsersDefinition,
    )
    async with async_postgres_pool.connection() as backend:
        fixtures = [definition(backend.dialect) for definition in definitions]
        for fixture in fixtures:
            await backend.execute(*fixture.drop_table.to_sql(), options=DDL)
            await backend.execute(*fixture.create_table.to_sql(), options=DDL)

    yield async_postgres_pool

    async with async_postgres_pool.connection() as backend:
        for fixture in reversed(fixtures):
            await backend.execute(*fixture.drop_table.to_sql(), options=DDL)
