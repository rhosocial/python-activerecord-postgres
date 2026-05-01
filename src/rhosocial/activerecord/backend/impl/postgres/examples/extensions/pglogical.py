# src/rhosocial/activerecord/backend/impl/postgres/examples/extensions/pglogical.py
"""
pglogical extension - logical streaming replication for PostgreSQL.

This example demonstrates:
1. Check if pglogical extension is available
2. CREATE EXTENSION
3. Create a pglogical node using pglogical_create_node
4. Show subscription status using pglogical_show_subscription_status
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
from rhosocial.activerecord.backend.impl.postgres.functions.pglogical import (
    pglogical_create_node,
    pglogical_show_subscription_status,
)

# Check if pglogical extension is available
available = dialect.is_extension_available("pglogical")
installed = dialect.is_extension_installed("pglogical")
print(f"Extension check: pglogical available = {available}, installed = {installed}")

# Create extension using expression
if available and not installed:
    create_ext = PostgresCreateExtensionExpression(
        dialect=dialect,
        name="pglogical",
    )
    sql, params = create_ext.to_sql()
    print("\n--- CREATE EXTENSION ---")
    print(f"SQL: {sql}")
    backend.execute(sql, params)
    backend.introspect_and_adapt()

# Re-check after creation
installed = dialect.is_extension_installed("pglogical")

if installed:
    opts = ExecutionOptions(stmt_type=StatementType.DQL)

    # Example 1: Create a pglogical node
    node_func = pglogical_create_node(
        dialect,
        "provider_node",
        "host=localhost dbname=test",
    )
    query = QueryExpression(
        dialect=dialect,
        select=[node_func.as_("node_result")],
    )
    sql, params = query.to_sql()
    print("\n--- CREATE PGLOGICAL NODE ---")
    print(f"SQL: {sql}")
    try:
        result = backend.execute(sql, params, options=opts)
        print(f"Result: {result.data}")
    except Exception as e:
        print(f"Note: pglogical requires superuser privileges: {e}")

    # Example 2: Show subscription status
    status_func = pglogical_show_subscription_status(dialect)
    query = QueryExpression(
        dialect=dialect,
        select=[status_func.as_("subscription_status")],
    )
    sql, params = query.to_sql()
    print("\n--- SHOW SUBSCRIPTION STATUS ---")
    print(f"SQL: {sql}")
    try:
        result = backend.execute(sql, params, options=opts)
        print(f"Result: {result.data}")
    except Exception as e:
        print(f"Note: pglogical requires superuser privileges: {e}")

else:
    print("\nSkipping execution - pglogical extension not available on this server")
    print("To enable pglogical, install it and run: CREATE EXTENSION pglogical;")

# ============================================================
# SECTION: Teardown (necessary for execution, reference only)
# ============================================================
backend.disconnect()
