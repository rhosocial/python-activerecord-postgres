# src/rhosocial/activerecord/backend/impl/postgres/examples/named_connections/development.py
"""Development environment connection examples."""

from rhosocial.activerecord.backend.impl.postgres.config import PostgresConnectionConfig


def local_dev():
    """Local development PostgreSQL database connection.

    Connects to localhost with default credentials.
    Useful for local development and testing.

    Returns:
        PostgresConnectionConfig: Development database configuration.
    """
    return PostgresConnectionConfig(
        host="localhost",
        port=5432,
        user="postgres",
        database="dev",
    )


def local_dev_no_password():
    """Local PostgreSQL connection without password.

    For PostgreSQL installations that use trust authentication.

    Returns:
        PostgresConnectionConfig: No-password database configuration.
    """
    return PostgresConnectionConfig(
        host="localhost",
        port=5432,
        user="postgres",
        password="",
        database="dev",
    )


def test_db():
    """Test database connection.

    Returns:
        PostgresConnectionConfig: Test database configuration.
    """
    return PostgresConnectionConfig(
        host="localhost",
        port=5432,
        user="postgres",
        database="test",
    )