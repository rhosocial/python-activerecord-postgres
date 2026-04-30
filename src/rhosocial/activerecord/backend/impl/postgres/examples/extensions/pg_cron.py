# src/rhosocial/activerecord/backend/impl/postgres/examples/extensions/pg_cron.py
"""
pg_cron extension - cron-based job scheduler for PostgreSQL.

This example demonstrates:
1. Check if pg_cron extension is available
2. CREATE EXTENSION
3. Schedule a cron job using cron_schedule
4. Unschedule a cron job using cron_unschedule
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
from rhosocial.activerecord.backend.expression.core import Literal
from rhosocial.activerecord.backend.options import ExecutionOptions
from rhosocial.activerecord.backend.schema import StatementType
from rhosocial.activerecord.backend.impl.postgres.functions.pg_cron import (
    cron_schedule,
    cron_unschedule,
)

# Check if pg_cron extension is available
available = dialect.is_extension_available("pg_cron")
installed = dialect.is_extension_installed("pg_cron")
print(f"Extension check: pg_cron available = {available}, installed = {installed}")

# Create extension using expression
if available and not installed:
    create_ext = PostgresCreateExtensionExpression(
        dialect=dialect,
        name="pg_cron",
    )
    sql, params = create_ext.to_sql()
    print("\n--- CREATE EXTENSION ---")
    print(f"SQL: {sql}")
    backend.execute(sql, params)
    backend.introspect_and_adapt()

# Re-check after creation
installed = dialect.is_extension_installed("pg_cron")

if installed:
    opts = ExecutionOptions(stmt_type=StatementType.DQL)

    # Example 1: Schedule a cron job
    schedule_func = cron_schedule(
        dialect,
        "0 * * * *",
        Literal(dialect, "SELECT 1"),
    )
    query = QueryExpression(
        dialect=dialect,
        select=[schedule_func.as_("job_id")],
    )
    sql, params = query.to_sql()
    print("\n--- SCHEDULE CRON JOB ---")
    print(f"SQL: {sql}")
    try:
        result = backend.execute(sql, params, options=opts)
        print(f"Result: {result.data}")
        if result.data and result.data[0]["job_id"] is not None:
            job_id = result.data[0]["job_id"]
            print(f"Scheduled job with ID: {job_id}")

            # Example 2: Unschedule the cron job
            unschedule_func = cron_unschedule(dialect, job_id)
            query = QueryExpression(
                dialect=dialect,
                select=[unschedule_func.as_("unschedule_result")],
            )
            sql, params = query.to_sql()
            print("\n--- UNSCHEDULE CRON JOB ---")
            print(f"SQL: {sql}")
            result = backend.execute(sql, params, options=opts)
            print(f"Result: {result.data}")
            print(f"Job {job_id} has been unscheduled")
        else:
            print("No job ID returned, skipping unschedule")
    except Exception as e:
        print(f"Note: pg_cron requires superuser privileges: {e}")

else:
    print("\nSkipping execution - pg_cron extension not available on this server")
    print("To enable pg_cron, install it and add to shared_preload_libraries,")
    print("then run: CREATE EXTENSION pg_cron;")

# ============================================================
# SECTION: Teardown (necessary for execution, reference only)
# ============================================================
backend.disconnect()
