# tests/rhosocial/activerecord_postgres_test/feature/connection/conftest.py
"""Pytest configuration for the connection pool tests.

The directory owns its logical database the same way every migrated directory
does: it declares the lifecycle here and takes the scenario and configuration
from the testsuite ``environment``, so running against several scenarios gets
one pool per scenario instead of a first-scenario guess. The concurrency tables
are created and dropped by the table fixtures themselves, so ``prepare`` and
``teardown`` have no group to manage.
"""

from typing import Generator

import pytest
import pytest_asyncio

from rhosocial.activerecord.connection.pool import (
    AsyncBackendPool,
    BackendPool,
    PoolConfig,
)


def ensure_database(environment):
    """Create the directory's database, or clear it when it already exists."""
    environment.reset_database()


def prepare(environment):
    """No group tables: each table fixture creates what it needs."""


def teardown(environment):
    """No group tables: each table fixture drops what it created."""


def _backend_factory(environment):
    """Return a factory producing sync backends for this environment."""
    config = environment.make_config()

    def factory():
        return environment.scenario.backend_class(connection_config=config)

    return factory


def _async_backend_factory(environment):
    """Return a factory producing async backends for this environment."""
    config = environment.make_config()

    def factory():
        return environment.scenario.async_backend_class(connection_config=config)

    return factory


@pytest.fixture(scope="function")
def postgres_pool(environment) -> Generator[BackendPool, None, None]:
    """Create a BackendPool with PostgreSQL backends for testing."""
    pool_config = PoolConfig(
        min_size=1,
        max_size=5,
        timeout=30.0,
        validate_on_borrow=True,
        validation_query="SELECT 1",
        backend_factory=_backend_factory(environment),
    )

    pool = BackendPool.create(pool_config)
    yield pool
    pool.close(timeout=5.0, force=True)


@pytest_asyncio.fixture(scope="function")
async def async_postgres_pool(environment) -> AsyncBackendPool:
    """Create an AsyncBackendPool with PostgreSQL backends for testing."""
    pool_config = PoolConfig(
        min_size=1,
        max_size=5,
        timeout=30.0,
        validate_on_borrow=True,
        validation_query="SELECT 1",
        backend_factory=_async_backend_factory(environment),
    )

    pool = await AsyncBackendPool.create(pool_config)
    yield pool
    await pool.close(timeout=5.0, force=True)


@pytest.fixture(scope="function")
def postgres_pool_large(environment) -> Generator[BackendPool, None, None]:
    """Create a larger BackendPool for stress testing."""
    pool_config = PoolConfig(
        min_size=2,
        max_size=10,
        timeout=60.0,
        validate_on_borrow=False,  # Faster for stress tests
        backend_factory=_backend_factory(environment),
    )

    pool = BackendPool.create(pool_config)
    yield pool
    pool.close(timeout=5.0, force=True)


@pytest_asyncio.fixture(scope="function")
async def async_postgres_pool_large(environment) -> AsyncBackendPool:
    """Create a larger AsyncBackendPool for stress testing."""
    pool_config = PoolConfig(
        min_size=2,
        max_size=10,
        timeout=60.0,
        validate_on_borrow=False,
        backend_factory=_async_backend_factory(environment),
    )

    pool = await AsyncBackendPool.create(pool_config)
    yield pool
    await pool.close(timeout=5.0, force=True)


# --- Table Setup Fixtures ---


@pytest.fixture(scope="function")
def postgres_pool_with_tables(postgres_pool: BackendPool) -> Generator[BackendPool, None, None]:
    """Create a pool with test tables initialized."""
    with postgres_pool.connection() as backend:
        backend.execute("DROP TABLE IF EXISTS concurrent_test_users")
        backend.execute("DROP TABLE IF EXISTS concurrent_test_posts")
        backend.execute("""
            CREATE TABLE concurrent_test_users (
                id SERIAL PRIMARY KEY,
                thread_id INTEGER,
                name VARCHAR(255),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        backend.execute("""
            CREATE TABLE concurrent_test_posts (
                id SERIAL PRIMARY KEY,
                thread_id INTEGER,
                user_id INTEGER,
                title VARCHAR(255),
                content TEXT
            )
        """)

    yield postgres_pool

    with postgres_pool.connection() as backend:
        backend.execute("DROP TABLE IF EXISTS concurrent_test_posts")
        backend.execute("DROP TABLE IF EXISTS concurrent_test_users")


@pytest_asyncio.fixture(scope="function")
async def async_postgres_pool_with_tables(
    async_postgres_pool: AsyncBackendPool,
) -> AsyncBackendPool:
    """Create an async pool with test tables initialized."""
    async with async_postgres_pool.connection() as backend:
        await backend.execute("DROP TABLE IF EXISTS concurrent_test_users")
        await backend.execute("DROP TABLE IF EXISTS concurrent_test_posts")
        await backend.execute("""
            CREATE TABLE concurrent_test_users (
                id SERIAL PRIMARY KEY,
                task_id INTEGER,
                name VARCHAR(255),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        await backend.execute("""
            CREATE TABLE concurrent_test_posts (
                id SERIAL PRIMARY KEY,
                task_id INTEGER,
                user_id INTEGER,
                title VARCHAR(255),
                content TEXT
            )
        """)

    yield async_postgres_pool

    async with async_postgres_pool.connection() as backend:
        await backend.execute("DROP TABLE IF EXISTS concurrent_test_posts")
        await backend.execute("DROP TABLE IF EXISTS concurrent_test_users")
