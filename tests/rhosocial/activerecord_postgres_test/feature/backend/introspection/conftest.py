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

# SQL statements for sequence / materialized view introspection tests.
# The sequence is created with non-default attributes so a test can tell a
# parsed row from a default-valued one, and it is drawn from once so that
# ``last_value`` has something to report.
_SEQUENCE_SQL = [
    "DROP MATERIALIZED VIEW IF EXISTS seq_demo_mv",
    "DROP SEQUENCE IF EXISTS seq_demo_untouched",
    "DROP TABLE IF EXISTS seq_demo_tbl",
    "DROP SEQUENCE IF EXISTS seq_demo_id",
    """
    CREATE SEQUENCE seq_demo_id
    START 7 INCREMENT 3 MINVALUE 2 MAXVALUE 1000 CACHE 5 NO CYCLE
    """,
    "SELECT nextval('seq_demo_id')",
    # Never drawn from: last_value stays NULL and must still parse.
    "CREATE SEQUENCE seq_demo_untouched START 1",
    "CREATE TABLE seq_demo_tbl (id INTEGER PRIMARY KEY, amount INTEGER)",
    "INSERT INTO seq_demo_tbl (id, amount) VALUES (1, 100), (2, 250)",
    """
    CREATE MATERIALIZED VIEW seq_demo_mv AS
    SELECT id, amount FROM seq_demo_tbl
    """,
]

_CLEANUP_SEQUENCE_SQL = [
    "DROP MATERIALIZED VIEW IF EXISTS seq_demo_mv",
    "DROP TABLE IF EXISTS seq_demo_tbl",
    "DROP SEQUENCE IF EXISTS seq_demo_untouched",
    "DROP SEQUENCE IF EXISTS seq_demo_id",
]


def _execute_sql_list(backend, sql_list):
    """Execute a list of SQL statements."""
    for sql in sql_list:
        backend.execute(sql)


async def _async_execute_sql_list(backend, sql_list):
    """Execute a list of SQL statements asynchronously."""
    for sql in sql_list:
        await backend.execute(sql)


@pytest.fixture(scope="function")
def backend_with_tables(postgres_backend):
    """Fixture providing backend with test tables created."""
    postgres_backend.introspector.clear_cache()
    _execute_sql_list(postgres_backend, _TABLES_SQL)
    _execute_sql_list(postgres_backend, _FK_SQL)
    postgres_backend.introspector.clear_cache()
    yield postgres_backend
    try:
        postgres_backend.introspector.clear_cache()
        _execute_sql_list(postgres_backend, _CLEANUP_TABLES_SQL)
    except Exception:
        pass


@pytest_asyncio.fixture(scope="function")
async def async_backend_with_tables(async_postgres_backend):
    """Async fixture providing backend with test tables created."""
    async_postgres_backend.introspector.clear_cache()
    await _async_execute_sql_list(async_postgres_backend, _TABLES_SQL)
    await _async_execute_sql_list(async_postgres_backend, _FK_SQL)
    async_postgres_backend.introspector.clear_cache()
    yield async_postgres_backend
    try:
        async_postgres_backend.introspector.clear_cache()
        await _async_execute_sql_list(async_postgres_backend, _CLEANUP_TABLES_SQL)
    except Exception:
        pass


@pytest.fixture(scope="function")
def backend_with_view(postgres_backend):
    """Fixture providing backend with a test view."""
    postgres_backend.introspector.clear_cache()
    _execute_sql_list(postgres_backend, _VIEW_SQL)
    postgres_backend.introspector.clear_cache()
    yield postgres_backend
    try:
        postgres_backend.introspector.clear_cache()
        _execute_sql_list(postgres_backend, _CLEANUP_VIEW_SQL)
    except Exception:
        pass


@pytest_asyncio.fixture(scope="function")
async def async_backend_with_view(async_postgres_backend):
    """Async fixture providing backend with a test view."""
    async_postgres_backend.introspector.clear_cache()
    await _async_execute_sql_list(async_postgres_backend, _VIEW_SQL)
    async_postgres_backend.introspector.clear_cache()
    yield async_postgres_backend
    try:
        async_postgres_backend.introspector.clear_cache()
        await _async_execute_sql_list(async_postgres_backend, _CLEANUP_VIEW_SQL)
    except Exception:
        pass


@pytest.fixture(scope="function")
def backend_with_trigger(postgres_backend):
    """Fixture providing backend with a test trigger."""
    postgres_backend.introspector.clear_cache()
    _execute_sql_list(postgres_backend, _TRIGGER_SQL)
    postgres_backend.introspector.clear_cache()
    yield postgres_backend
    try:
        postgres_backend.introspector.clear_cache()
        _execute_sql_list(postgres_backend, _CLEANUP_TRIGGER_SQL)
    except Exception:
        pass


@pytest_asyncio.fixture(scope="function")
async def async_backend_with_trigger(async_postgres_backend):
    """Async fixture providing backend with a test trigger."""
    async_postgres_backend.introspector.clear_cache()
    await _async_execute_sql_list(async_postgres_backend, _TRIGGER_SQL)
    async_postgres_backend.introspector.clear_cache()
    yield async_postgres_backend
    try:
        async_postgres_backend.introspector.clear_cache()
        await _async_execute_sql_list(async_postgres_backend, _CLEANUP_TRIGGER_SQL)
    except Exception:
        pass


@pytest.fixture(scope="function")
def backend_with_sequence(postgres_backend):
    """Fixture providing backend with a demo sequence and materialized view."""
    postgres_backend.introspector.clear_cache()
    _execute_sql_list(postgres_backend, _SEQUENCE_SQL)
    postgres_backend.introspector.clear_cache()
    yield postgres_backend
    try:
        postgres_backend.introspector.clear_cache()
        _execute_sql_list(postgres_backend, _CLEANUP_SEQUENCE_SQL)
    except Exception:
        pass


@pytest_asyncio.fixture(scope="function")
async def async_backend_with_sequence(async_postgres_backend):
    """Async fixture providing backend with a demo sequence and MV."""
    async_postgres_backend.introspector.clear_cache()
    await _async_execute_sql_list(async_postgres_backend, _SEQUENCE_SQL)
    async_postgres_backend.introspector.clear_cache()
    yield async_postgres_backend
    try:
        async_postgres_backend.introspector.clear_cache()
        await _async_execute_sql_list(async_postgres_backend, _CLEANUP_SEQUENCE_SQL)
    except Exception:
        pass
