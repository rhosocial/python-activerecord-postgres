# src/rhosocial/activerecord/backend/impl/postgres/examples/extensions/pgaudit.py
"""
pgaudit extension - session and object audit logging for PostgreSQL.

This example demonstrates:
1. Check if pgaudit extension is available
2. CREATE EXTENSION
3. Set pgaudit role using pgaudit_set_role
4. Set pgaudit log level using pgaudit_log_level
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
from rhosocial.activerecord.backend.impl.postgres.functions.pgaudit import (
    pgaudit_set_role,
    pgaudit_log_level,
)

# Check if pgaudit extension is available
available = dialect.is_extension_available("pgaudit")
installed = dialect.is_extension_installed("pgaudit")
print(f"Extension check: pgaudit available = {available}, installed = {installed}")

# Create extension using expression
if available and not installed:
    create_ext = PostgresCreateExtensionExpression(
        dialect=dialect,
        name="pgaudit",
    )
    sql, params = create_ext.to_sql()
    print("\n--- CREATE EXTENSION ---")
    print(f"SQL: {sql}")
    backend.execute(sql, params)
    backend.introspect_and_adapt()

# Re-check after creation
installed = dialect.is_extension_installed("pgaudit")

if installed:
    opts = ExecutionOptions(stmt_type=StatementType.DQL)

    # Example 1: Set the pgaudit role for auditing
    role_func = pgaudit_set_role(dialect, "audit_role")
    query = QueryExpression(
        dialect=dialect,
        select=[role_func.as_("set_config_result")],
    )
    sql, params = query.to_sql()
    print("\n--- SET PGAUDIT ROLE ---")
    print(f"SQL: {sql}")
    try:
        result = backend.execute(sql, params, options=opts)
        print(f"Result: {result.data}")
    except Exception as e:
        print(f"Note: pgaudit configuration requires superuser privileges: {e}")

    # Example 2: Set the pgaudit log level
    level_func = pgaudit_log_level(dialect, "log")
    query = QueryExpression(
        dialect=dialect,
        select=[level_func.as_("set_config_result")],
    )
    sql, params = query.to_sql()
    print("\n--- SET PGAUDIT LOG LEVEL ---")
    print(f"SQL: {sql}")
    try:
        result = backend.execute(sql, params, options=opts)
        print(f"Result: {result.data}")
    except Exception as e:
        print(f"Note: pgaudit configuration requires superuser privileges: {e}")

else:
    print("\nSkipping execution - pgaudit extension not available on this server")
    print("To enable pgaudit, install it and add to shared_preload_libraries,")
    print("then run: CREATE EXTENSION pgaudit;")

# ============================================================
# SECTION: Teardown (necessary for execution, reference only)
# ============================================================
backend.disconnect()
