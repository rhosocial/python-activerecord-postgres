"""
pg_logicalinspect extension - logical replication inspection.

This example demonstrates:
1. Check if pg_logicalinspect extension is available
2. CREATE EXTENSION pg_logicalinspect
3. Inspect a logical replication slot

Note: pg_logicalinspect is available from PostgreSQL 18+.
"""

import os
from rhosocial.activerecord.backend.impl.postgres import PostgresBackend
from rhosocial.activerecord.backend.impl.postgres.config import (
    PostgresConnectionConfig,
)

config = PostgresConnectionConfig(
    host=os.getenv("PG_HOST", "localhost"),
    port=int(os.getenv("PG_PORT", "5432")),
    database=os.getenv("PG_DATABASE", "test"),
    username=os.getenv("PG_USERNAME", "postgres"),
    password=os.getenv("PG_PASSWORD", ""),
)
backend = PostgresBackend(connection_config=config)
backend.connect()
backend.introspect_and_adapt()
dialect = backend.dialect

available = dialect.is_extension_available("pg_logicalinspect")
installed = dialect.is_extension_installed("pg_logicalinspect")
print(f"Extension check: pg_logicalinspect available = {available}, installed = {installed}")

if available and not installed:
    from rhosocial.activerecord.backend.impl.postgres.expression import (
        PostgresCreateExtensionExpression,
    )
    create_ext = PostgresCreateExtensionExpression(dialect=dialect, name="pg_logicalinspect")
    sql, params = create_ext.to_sql()
    backend.execute(sql, params)
    backend.introspect_and_adapt()

installed = dialect.is_extension_installed("pg_logicalinspect")

if installed:
    print("pg_logicalinspect extension is ready for use")
    print(f"Features: {dialect.supports_pg_logicalinspect()}")

    sql, params = dialect.format_inspect_slot_statement("my_slot", limit=50)
    print(f"\nInspect logical slot: {sql}")
else:
    print("pg_logicalinspect not available on this server (requires PostgreSQL 18+)")

backend.disconnect()