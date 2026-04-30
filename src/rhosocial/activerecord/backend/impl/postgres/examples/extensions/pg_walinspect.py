# src/rhosocial/activerecord/backend/impl/postgres/examples/extensions/pg_walinspect.py
"""
pg_walinspect extension - inspect Write-Ahead Log (WAL) records.

This example demonstrates:
1. Check if pg_walinspect extension is available
2. CREATE EXTENSION
3. Get WAL records info using pg_get_wal_records_info with LSN parameters

Note: pg_logical_emit_message is a built-in PostgreSQL function, not part
of pg_walinspect. It has been removed from the pg_walinspect module.
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
from rhosocial.activerecord.backend.impl.postgres.functions.pg_walinspect import (
    pg_get_wal_records_info,
)

# Check if pg_walinspect extension is available
available = dialect.is_extension_available("pg_walinspect")
installed = dialect.is_extension_installed("pg_walinspect")
print(f"Extension check: pg_walinspect available = {available}, installed = {installed}")

# Create extension using expression
if available and not installed:
    create_ext = PostgresCreateExtensionExpression(
        dialect=dialect,
        name="pg_walinspect",
    )
    sql, params = create_ext.to_sql()
    print("\n--- CREATE EXTENSION ---")
    print(f"SQL: {sql}")
    backend.execute(sql, params)
    backend.introspect_and_adapt()

# Re-check after creation
installed = dialect.is_extension_installed("pg_walinspect")

if installed:
    opts = ExecutionOptions(stmt_type=StatementType.DQL)

    # Example 1: Get current WAL LSN position
    current_lsn_query = QueryExpression(
        dialect=dialect,
        select=[dialect.parse_expression("pg_current_wal_lsn()").as_("current_lsn")],
    )
    sql, params = current_lsn_query.to_sql()
    print("\n--- GET CURRENT WAL LSN ---")
    print(f"SQL: {sql}")
    try:
        lsn_result = backend.execute(sql, params, options=opts)
        current_lsn = lsn_result.data[0]["current_lsn"]
        print(f"Current LSN: {current_lsn}")
    except Exception as e:
        print(f"Note: pg_walinspect requires superuser privileges: {e}")
        current_lsn = None

    # Example 2: Get WAL records info using LSN range
    if current_lsn:
        wal_func = pg_get_wal_records_info(
            dialect,
            current_lsn,
            current_lsn,
        )
        query = QueryExpression(
            dialect=dialect,
            select=[wal_func.as_("wal_records")],
        )
        sql, params = query.to_sql()
        print("\n--- GET WAL RECORDS INFO ---")
        print(f"SQL: {sql}")
        try:
            result = backend.execute(sql, params, options=opts)
            print(f"Result: {result.data}")
        except Exception as e:
            print(f"Note: pg_walinspect requires superuser privileges: {e}")

else:
    print("\nSkipping execution - pg_walinspect extension not available on this server")
    print("To enable pg_walinspect, install it and run: CREATE EXTENSION pg_walinspect;")

# ============================================================
# SECTION: Teardown (necessary for execution, reference only)
# ============================================================
backend.disconnect()
