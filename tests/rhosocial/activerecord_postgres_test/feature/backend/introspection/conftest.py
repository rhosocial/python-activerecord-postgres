# tests/rhosocial/activerecord_postgres_test/feature/backend/introspection/conftest.py
"""
Pytest fixtures for PostgreSQL introspection tests.

This module provides fixtures for testing database introspection
functionality with PostgreSQL backends.
"""

import pytest
import pytest_asyncio

# SQL statements for test tables
_TABLES_SQL = [
    "DROP TABLE IF EXISTS post_tags CASCADE",
    "DROP TABLE IF EXISTS posts CASCADE",
    "DROP TABLE IF EXISTS tags CASCADE",
    "DROP TABLE IF EXISTS users CASCADE",
    """
    CREATE TABLE users (
        id SERIAL PRIMARY KEY,
        name VARCHAR(100) NOT NULL,
        email VARCHAR(255) NOT NULL,
        age INTEGER,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """,
    "CREATE INDEX idx_users_email ON users (email)",
    "CREATE INDEX idx_users_name_age ON users (name, age)",
    """
    CREATE TABLE posts (
        id SERIAL PRIMARY KEY,
        title VARCHAR(255) NOT NULL,
        content TEXT,
        user_id INTEGER NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """,
    "CREATE INDEX idx_posts_user_id ON posts (user_id)",
    """
    CREATE TABLE tags (
        id SERIAL PRIMARY KEY,
        name VARCHAR(50) NOT NULL UNIQUE
    )
    """,
    """
    CREATE TABLE post_tags (
        post_id INTEGER NOT NULL,
        tag_id INTEGER NOT NULL,
        PRIMARY KEY (post_id, tag_id)
    )
    """,
]

_FK_SQL = [
    """
    ALTER TABLE posts
    ADD CONSTRAINT fk_posts_user
    FOREIGN KEY (user_id) REFERENCES users(id)
    ON DELETE CASCADE
    """,
    """
    ALTER TABLE post_tags
    ADD CONSTRAINT fk_post_tags_post
    FOREIGN KEY (post_id) REFERENCES posts(id)
    ON DELETE CASCADE
    """,
    """
    ALTER TABLE post_tags
    ADD CONSTRAINT fk_post_tags_tag
    FOREIGN KEY (tag_id) REFERENCES tags(id)
    ON DELETE CASCADE
    """,
]

_CLEANUP_TABLES_SQL = [
    "DROP TABLE IF EXISTS post_tags CASCADE",
    "DROP TABLE IF EXISTS posts CASCADE",
    "DROP TABLE IF EXISTS tags CASCADE",
    "DROP TABLE IF EXISTS users CASCADE",
]

# SQL statements for view tests
_VIEW_SQL = [
    "DROP VIEW IF EXISTS user_summary CASCADE",
    "DROP TABLE IF EXISTS user_stats CASCADE",
    "DROP TABLE IF EXISTS users CASCADE",
    """
    CREATE TABLE users (
        id SERIAL PRIMARY KEY,
        name VARCHAR(100) NOT NULL,
        email VARCHAR(255) NOT NULL
    )
    """,
    """
    CREATE TABLE user_stats (
        user_id INTEGER PRIMARY KEY,
        post_count INTEGER DEFAULT 0
    )
    """,
    """
    CREATE VIEW user_summary AS
    SELECT u.id, u.name, u.email, COALESCE(s.post_count, 0) as post_count
    FROM users u
    LEFT JOIN user_stats s ON u.id = s.user_id
    """,
]

_CLEANUP_VIEW_SQL = [
    "DROP VIEW IF EXISTS user_summary CASCADE",
    "DROP TABLE IF EXISTS user_stats CASCADE",
    "DROP TABLE IF EXISTS users CASCADE",
]

# SQL statements for trigger tests
_TRIGGER_SQL = [
    "DROP TABLE IF EXISTS users CASCADE",
    """
    CREATE TABLE users (
        id SERIAL PRIMARY KEY,
        name VARCHAR(100) NOT NULL,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """,
    """
    CREATE OR REPLACE FUNCTION update_timestamp_func()
    RETURNS TRIGGER AS $$
    BEGIN
        NEW.updated_at := CURRENT_TIMESTAMP;
        RETURN NEW;
    END;
    $$ LANGUAGE plpgsql
    """,
    """
    CREATE TRIGGER update_user_timestamp
    BEFORE UPDATE ON users
    FOR EACH ROW
    EXECUTE PROCEDURE update_timestamp_func()
    """,
]

_CLEANUP_TRIGGER_SQL = [
    "DROP TRIGGER IF EXISTS update_user_timestamp ON users",
    "DROP FUNCTION IF EXISTS update_timestamp_func()",
    "DROP TABLE IF EXISTS users CASCADE",
]


def _execute_sql_list(backend, sql_list):
    """Execute a list of SQL statements."""
    for sql in sql_list:
        backend.execute(sql)


@pytest.fixture(scope="function")
def postgres_backend_single(request):
    """Fixture providing a single PostgreSQL backend for introspection tests."""
    from tests.providers.scenarios import get_scenario, get_enabled_scenarios

    # Get first available scenario
    enabled_scenarios = get_enabled_scenarios()
    if not enabled_scenarios:
        pytest.skip("No PostgreSQL scenarios configured")

    first_scenario = list(enabled_scenarios.keys())[0]
    backend_class, config = get_scenario(first_scenario)
    backend = backend_class(connection_config=config)
    backend.connect()
    backend.introspect_and_adapt()
    yield backend
    backend.disconnect()


@pytest_asyncio.fixture(scope="function")
async def async_postgres_backend_single(request):
    """Async fixture providing a single PostgreSQL backend for introspection tests."""
    from tests.providers.scenarios import get_scenario, get_enabled_scenarios
    from rhosocial.activerecord.backend.impl.postgres import AsyncPostgresBackend

    # Get first available scenario
    enabled_scenarios = get_enabled_scenarios()
    if not enabled_scenarios:
        pytest.skip("No PostgreSQL scenarios configured")

    first_scenario = list(enabled_scenarios.keys())[0]
    _backend_class, config = get_scenario(first_scenario)
    backend = AsyncPostgresBackend(connection_config=config)
    await backend.connect()
    await backend.introspect_and_adapt()
    yield backend
    await backend.disconnect()


@pytest.fixture(scope="function")
def backend_with_tables(request):
    """Fixture providing backend with test tables created."""
    from tests.providers.scenarios import get_scenario, get_enabled_scenarios

    # Get first available scenario
    enabled_scenarios = get_enabled_scenarios()
    if not enabled_scenarios:
        pytest.skip("No PostgreSQL scenarios configured")

    first_scenario = list(enabled_scenarios.keys())[0]
    backend_class, config = get_scenario(first_scenario)
    backend = backend_class(connection_config=config)
    backend.connect()
    backend.introspect_and_adapt()

    # Create tables
    _execute_sql_list(backend, _TABLES_SQL)
    # Add foreign keys
    _execute_sql_list(backend, _FK_SQL)
    # Clear introspection cache
    backend.introspector.clear_cache()
    yield backend

    try:
        backend.introspector.clear_cache()
        _execute_sql_list(backend, _CLEANUP_TABLES_SQL)
    except Exception:
        pass
    finally:
        backend.disconnect()


@pytest_asyncio.fixture(scope="function")
async def async_backend_with_tables(request):
    """Async fixture providing backend with test tables created."""
    from tests.providers.scenarios import get_scenario, get_enabled_scenarios
    from rhosocial.activerecord.backend.impl.postgres import AsyncPostgresBackend

    # Get first available scenario
    enabled_scenarios = get_enabled_scenarios()
    if not enabled_scenarios:
        pytest.skip("No PostgreSQL scenarios configured")

    first_scenario = list(enabled_scenarios.keys())[0]
    _backend_class, config = get_scenario(first_scenario)
    backend = AsyncPostgresBackend(connection_config=config)
    await backend.connect()
    await backend.introspect_and_adapt()

    # Create tables
    for sql in _TABLES_SQL:
        await backend.execute(sql)
    # Add foreign keys
    for sql in _FK_SQL:
        await backend.execute(sql)
    # Clear introspection cache
    backend.introspector.clear_cache()
    yield backend

    try:
        backend.introspector.clear_cache()
        for sql in _CLEANUP_TABLES_SQL:
            await backend.execute(sql)
    except Exception:
        pass
    finally:
        await backend.disconnect()


@pytest.fixture(scope="function")
def backend_with_view(request):
    """Fixture providing backend with test view created."""
    from tests.providers.scenarios import get_scenario, get_enabled_scenarios

    enabled_scenarios = get_enabled_scenarios()
    if not enabled_scenarios:
        pytest.skip("No PostgreSQL scenarios configured")

    first_scenario = list(enabled_scenarios.keys())[0]
    backend_class, config = get_scenario(first_scenario)
    backend = backend_class(connection_config=config)
    backend.connect()
    backend.introspect_and_adapt()

    _execute_sql_list(backend, _VIEW_SQL)
    backend.introspector.clear_cache()
    yield backend

    try:
        backend.introspector.clear_cache()
        _execute_sql_list(backend, _CLEANUP_VIEW_SQL)
    except Exception:
        pass
    finally:
        backend.disconnect()


@pytest.fixture(scope="function")
def backend_with_trigger(request):
    """Fixture providing backend with test trigger created."""
    from tests.providers.scenarios import get_scenario, get_enabled_scenarios

    enabled_scenarios = get_enabled_scenarios()
    if not enabled_scenarios:
        pytest.skip("No PostgreSQL scenarios configured")

    first_scenario = list(enabled_scenarios.keys())[0]
    backend_class, config = get_scenario(first_scenario)
    backend = backend_class(connection_config=config)
    backend.connect()
    backend.introspect_and_adapt()

    _execute_sql_list(backend, _TRIGGER_SQL)
    backend.introspector.clear_cache()
    yield backend

    try:
        backend.introspector.clear_cache()
        _execute_sql_list(backend, _CLEANUP_TRIGGER_SQL)
    except Exception:
        pass
    finally:
        backend.disconnect()
