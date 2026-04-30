# src/rhosocial/activerecord/backend/impl/postgres/examples/extensions/pg_partman.py
"""
pg_partman extension - partition management for PostgreSQL.

This example demonstrates:
1. Check if pg_partman extension is available
2. CREATE EXTENSION and create table with timestamp column
3. Create partition set using create_parent
4. Run partition maintenance using run_maintenance

Note: pg_partman functions are in the public schema, not a "partman" schema.
The actual function signatures are:
- create_parent(p_parent_table text, p_control text, p_interval text,
                p_type text DEFAULT 'native', p_premake int DEFAULT 4, ...)
- run_maintenance(p_parent_table text DEFAULT NULL::text, ...)
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

# Clean up for demo
from rhosocial.activerecord.backend.expression import DropTableExpression

drop_expr = DropTableExpression(dialect=dialect, table_name="log_entries", if_exists=True)
sql, params = drop_expr.to_sql()
backend.execute(sql, params)

# ============================================================
# SECTION: Business Logic (the pattern to learn)
# ============================================================
from rhosocial.activerecord.backend.impl.postgres.expression import (
    PostgresCreateExtensionExpression,
)
from rhosocial.activerecord.backend.expression import (
    CreateTableExpression,
    ColumnDefinition,
    ColumnConstraint,
    ColumnConstraintType,
    QueryExpression,
)
from rhosocial.activerecord.backend.options import ExecutionOptions
from rhosocial.activerecord.backend.schema import StatementType
from rhosocial.activerecord.backend.impl.postgres.functions.pg_partman import (
    create_parent,
    run_maintenance,
)

# Check if pg_partman extension is available
available = dialect.is_extension_available("pg_partman")
installed = dialect.is_extension_installed("pg_partman")
print(f"Extension check: pg_partman available = {available}, installed = {installed}")

# Create extension using expression
if available and not installed:
    create_ext = PostgresCreateExtensionExpression(
        dialect=dialect,
        name="pg_partman",
    )
    sql, params = create_ext.to_sql()
    print("\n--- CREATE EXTENSION ---")
    print(f"SQL: {sql}")
    backend.execute(sql, params)
    backend.introspect_and_adapt()

# Re-check after creation
installed = dialect.is_extension_installed("pg_partman")

if installed:
    # Example 1: Create table with timestamp column for time partitioning
    columns = [
        ColumnDefinition(
            name="id",
            data_type="SERIAL",
            constraints=[
                ColumnConstraint(ColumnConstraintType.PRIMARY_KEY),
            ],
        ),
        ColumnDefinition(name="created_at", data_type="TIMESTAMP"),
        ColumnDefinition(name="level", data_type="TEXT"),
        ColumnDefinition(name="message", data_type="TEXT"),
    ]

    create_expr = CreateTableExpression(
        dialect=dialect,
        table_name="log_entries",
        columns=columns,
        if_not_exists=True,
    )
    sql, params = create_expr.to_sql()
    print("\n--- CREATE TABLE ---")
    print(f"SQL: {sql}")
    backend.execute(sql, params)

    # Example 2: Create partition set using create_parent
    # create_parent(parent_table, control, interval, partition_type, premake)
    part_func = create_parent(
        dialect,
        "public.log_entries",
        "created_at",
        "1 day",
    )
    query = QueryExpression(
        dialect=dialect,
        select=[part_func.as_("partition_result")],
    )
    sql, params = query.to_sql()
    print("\n--- CREATE PARENT (partition set) ---")
    print(f"SQL: {sql}")
    opts = ExecutionOptions(stmt_type=StatementType.DQL)
    try:
        result = backend.execute(sql, params, options=opts)
        print(f"Result: {result.data}")
    except Exception as e:
        print(f"Note: create_parent requires superuser privileges and partitioned table: {e}")

    # Example 3: Run partition maintenance
    maint_func = run_maintenance(dialect)
    query = QueryExpression(
        dialect=dialect,
        select=[maint_func],
    )
    sql, params = query.to_sql()
    print("\n--- RUN MAINTENANCE ---")
    print(f"SQL: {sql}")
    try:
        backend.execute(sql, params, options=opts)
        print("Maintenance completed")
    except Exception as e:
        print(f"Note: run_maintenance requires superuser privileges: {e}")

else:
    print("\nSkipping execution - pg_partman extension not available on this server")
    print("To enable pg_partman, install it and run: CREATE EXTENSION pg_partman;")

# ============================================================
# SECTION: Teardown (necessary for execution, reference only)
# ============================================================
drop_expr = DropTableExpression(dialect=dialect, table_name="log_entries", if_exists=True)
sql, params = drop_expr.to_sql()
backend.execute(sql, params)
backend.disconnect()
