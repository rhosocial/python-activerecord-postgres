# src/rhosocial/activerecord/backend/impl/postgres/examples/extensions/pg_repack.py
"""
pg_repack extension - reorganize tables and indexes with minimal locking.

This example demonstrates:
1. Check if pg_repack extension is available
2. CREATE EXTENSION
3. Query the extension version using repack.repack_version()

Note: pg_repack is primarily a command-line tool. The internal SQL functions
in the "repack" schema are not intended for direct use. There is no public
SQL API like repack_table() or repack_index().

To actually repack a table or index, use the pg_repack command-line utility:
    pg_repack --table=tablename dbname
    pg_repack --index=indexname dbname
"""

# ============================================================
# SECTION: Setup (necessary for execution, reference only)
# ============================================================
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

# ============================================================
# SECTION: Business Logic (the pattern to learn)
# ============================================================
from rhosocial.activerecord.backend.impl.postgres.expression import (
    PostgresCreateExtensionExpression,
)
from rhosocial.activerecord.backend.expression import (
    QueryExpression,
)
from rhosocial.activerecord.backend.options import ExecutionOptions
from rhosocial.activerecord.backend.schema import StatementType
from rhosocial.activerecord.backend.impl.postgres.functions.pg_repack import (
    repack_version,
)

# Check if pg_repack extension is available
available = dialect.is_extension_available("pg_repack")
installed = dialect.is_extension_installed("pg_repack")
print(f"Extension check: pg_repack available = {available}, installed = {installed}")

# Create extension using expression
if available and not installed:
    create_ext = PostgresCreateExtensionExpression(
        dialect=dialect,
        name="pg_repack",
    )
    sql, params = create_ext.to_sql()
    print("\n--- CREATE EXTENSION ---")
    print(f"SQL: {sql}")
    backend.execute(sql, params)
    backend.introspect_and_adapt()

# Re-check after creation
installed = dialect.is_extension_installed("pg_repack")

if installed:
    opts = ExecutionOptions(stmt_type=StatementType.DQL)

    # Example 1: Query pg_repack version
    version_func = repack_version(dialect)
    query = QueryExpression(
        dialect=dialect,
        select=[version_func.as_("version")],
    )
    sql, params = query.to_sql()
    print("\n--- REPACK VERSION ---")
    print(f"SQL: {sql}")
    try:
        result = backend.execute(sql, params, options=opts)
        print(f"pg_repack version: {result.data[0]['version']}")
    except Exception as e:
        print(f"Note: pg_repack requires superuser privileges: {e}")

    print("\nNote: To actually repack a table, use the pg_repack CLI tool:")
    print("  pg_repack --table=tablename dbname")

else:
    print("\nSkipping execution - pg_repack extension not available on this server")
    print("To enable pg_repack, install it and run: CREATE EXTENSION pg_repack;")

# ============================================================
# SECTION: Teardown (necessary for execution, reference only)
# ============================================================
backend.disconnect()
