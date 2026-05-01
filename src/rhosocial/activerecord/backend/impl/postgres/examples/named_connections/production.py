# src/rhosocial/activerecord/backend/impl/postgres/examples/named_connections/production.py
"""Production environment connection examples."""

from rhosocial.activerecord.backend.impl.postgres.config import PostgresConnectionConfig


def prod_db():
    """Production PostgreSQL database connection.

    Connects to production database server with SSL enabled.

    Returns:
        PostgresConnectionConfig: Production database configuration.
    """
    return PostgresConnectionConfig(
        host="prod-postgres.example.com",
        port=5432,
        user="app_user",
        database="production",
        sslmode="require",
    )


def prod_db_ssl():
    """Production PostgreSQL database with full SSL verification.

    Uses SSL with certificate verification for secure
    production connections.

    Returns:
        PostgresConnectionConfig: SSL-verified database configuration.
    """
    return PostgresConnectionConfig(
        host="prod-postgres.example.com",
        port=5432,
        user="app_user",
        database="production",
        sslmode="verify-full",
        sslcert="/path/to/client.crt",
        sslkey="/path/to/client.key",
        sslrootcert="/path/to/ca.crt",
    )


def prod_replica():
    """Production PostgreSQL read replica connection.

    For read-heavy workloads, connect to a read replica
    to distribute load.

    Returns:
        PostgresConnectionConfig: Read replica database configuration.
    """
    return PostgresConnectionConfig(
        host="prod-postgres-replica.example.com",
        port=5432,
        user="app_user",
        database="production",
    )