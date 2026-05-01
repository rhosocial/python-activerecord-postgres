# tests/rhosocial/activerecord_postgres_test/feature/backend/named_connection/example_connections.py
"""
Example named connections for PostgreSQL testing.

This module contains sample connection definitions for testing
the named connection functionality with PostgreSQL backend.
"""
from rhosocial.activerecord.backend.impl.postgres.config import PostgresConnectionConfig


def postgres_18(database: str = "test_db"):
    """PostgreSQL 18 development server connection."""
    return PostgresConnectionConfig(
        host="db-dev-1-n.rho.im",
        port=15441,
        database=database,
        username="root",
        password="password",
        sslmode="prefer",
    )


def postgres_18_with_pool(pool_size: int = 5):
    """PostgreSQL 18 connection with custom pool size."""
    if isinstance(pool_size, str):
        pool_size = int(pool_size)
    return PostgresConnectionConfig(
        host="db-dev-1-n.rho.im",
        port=15441,
        database="test_db",
        username="root",
        password="password",
        sslmode="prefer",
        pool_size=pool_size,
    )


def postgres_18_readonly():
    """PostgreSQL 18 read-only connection (shorter timeout)."""
    return PostgresConnectionConfig(
        host="db-dev-1-n.rho.im",
        port=15441,
        database="test_db",
        username="root",
        password="password",
        sslmode="prefer",
        pool_timeout=10,
    )
