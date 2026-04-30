# src/rhosocial/activerecord/backend/impl/postgres/examples/extensions/pg_surgery.py
"""
pg_surgery extension - perform surgery on relation pages.

This example demonstrates:
1. Check if pg_surgery extension is available
2. CREATE EXTENSION
3. Verify heap_force_freeze and heap_force_kill function factories

Note: The actual pg_surgery functions are:
- heap_force_freeze(reloid regclass, tids tid[])
- heap_force_kill(reloid regclass, tids tid[])
These require regclass and tid[] types which are complex to construct.
pg_surgery is intended for data recovery and requires superuser privileges.
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
from rhosocial.activerecord.backend.impl.postgres.functions.pg_surgery import (
    heap_force_freeze,
    heap_force_kill,
)

# Check if pg_surgery extension is available
available = dialect.is_extension_available("pg_surgery")
installed = dialect.is_extension_installed("pg_surgery")
print(f"Extension check: pg_surgery available = {available}, installed = {installed}")

# Create extension using expression
if available and not installed:
    create_ext = PostgresCreateExtensionExpression(
        dialect=dialect,
        name="pg_surgery",
    )
    sql, params = create_ext.to_sql()
    print("\n--- CREATE EXTENSION ---")
    print(f"SQL: {sql}")
    backend.execute(sql, params)
    backend.introspect_and_adapt()

# Re-check after creation
installed = dialect.is_extension_installed("pg_surgery")

if installed:
    # Example 1: Generate heap_force_freeze expression
    # heap_force_freeze(reloid regclass, tids tid[])
    freeze_func = heap_force_freeze(
        dialect,
        "repair_data",
        "'{(0,1)}'::tid[]",
    )
    sql, params = freeze_func.to_sql()
    print("\n--- HEAP FORCE FREEZE (SQL generation only) ---")
    print(f"SQL: {sql}")
    print("Note: heap_force_freeze requires regclass and tid[] parameters and superuser privileges")

    # Example 2: Generate heap_force_kill expression
    # heap_force_kill(reloid regclass, tids tid[])
    kill_func = heap_force_kill(
        dialect,
        "repair_data",
        "'{(0,1)}'::tid[]",
    )
    sql, params = kill_func.to_sql()
    print("\n--- HEAP FORCE KILL (SQL generation only) ---")
    print(f"SQL: {sql}")
    print("Note: heap_force_kill requires regclass and tid[] parameters and superuser privileges")

    print("\nWarning: pg_surgery functions are potentially dangerous and should")
    print("only be used by experienced administrators for data recovery.")

else:
    print("\nSkipping execution - pg_surgery extension not available on this server")
    print("To enable pg_surgery, install it and run: CREATE EXTENSION pg_surgery;")

# ============================================================
# SECTION: Teardown (necessary for execution, reference only)
# ============================================================
backend.disconnect()
