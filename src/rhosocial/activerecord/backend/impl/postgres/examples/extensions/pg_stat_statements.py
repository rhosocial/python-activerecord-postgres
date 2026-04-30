# src/rhosocial/activerecord/backend/impl/postgres/examples/extensions/pg_stat_statements.py
"""
pg_stat_statements extension - query execution statistics.

This example demonstrates:
1. Check if pg_stat_statements extension is available
2. CREATE EXTENSION
3. Query execution statistics from pg_stat_statements view
4. Reset statistics using pg_stat_statements_reset

Note: pg_stat_statements requires shared_preload_libraries configuration
in postgresql.conf and superuser access for some operations.
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
    CreateTableExpression,
    ColumnDefinition,
    ColumnConstraint,
    ColumnConstraintType,
    DropTableExpression,
    InsertExpression,
    QueryExpression,
    TableExpression,
    Column,
)
from rhosocial.activerecord.backend.expression.statements import ValuesSource
from rhosocial.activerecord.backend.expression.core import Literal
from rhosocial.activerecord.backend.expression.query_parts import OrderByClause
from rhosocial.activerecord.backend.expression.predicates import ComparisonPredicate
from rhosocial.activerecord.backend.options import ExecutionOptions
from rhosocial.activerecord.backend.schema import StatementType
from rhosocial.activerecord.backend.impl.postgres.functions.pg_stat_statements import (
    pg_stat_statements_reset,
)

# Check if pg_stat_statements extension is available
available = dialect.is_extension_available("pg_stat_statements")
installed = dialect.is_extension_installed("pg_stat_statements")
print(f"Extension check: pg_stat_statements available = {available}, installed = {installed}")

# Create extension using expression
if available and not installed:
    create_ext = PostgresCreateExtensionExpression(
        dialect=dialect,
        name="pg_stat_statements",
    )
    sql, params = create_ext.to_sql()
    print("\n--- CREATE EXTENSION ---")
    print(f"SQL: {sql}")
    try:
        backend.execute(sql, params)
        backend.introspect_and_adapt()
    except Exception as e:
        print(f"Could not create extension (may require superuser): {e}")

# Re-check after creation
installed = dialect.is_extension_installed("pg_stat_statements")

if installed:
    # Example 1: Query pg_stat_statements view for top queries by total execution time
    # This requires superuser or pg_read_all_stats role
    try:
        query = QueryExpression(
            dialect=dialect,
            select=[
                Column(dialect, "query"),
                Column(dialect, "calls"),
                Column(dialect, "total_exec_time"),
                Column(dialect, "mean_exec_time"),
            ],
            from_=TableExpression(dialect, "pg_stat_statements"),
            order_by=OrderByClause(dialect, [Column(dialect, "total_exec_time")], descending=True),
        )
        sql, params = query.to_sql()
        print("\n--- Query top statements by total execution time ---")
        print(f"SQL: {sql}")
        opts = ExecutionOptions(stmt_type=StatementType.DQL)
        result = backend.execute(sql, params, options=opts)
        print(f"Number of tracked statements: {len(result.data) if result.data else 0}")
        if result.data:
            for row in result.data[:3]:
                print(f"  calls={row.get('calls')}, total_time={row.get('total_exec_time')}, query={row.get('query', '')[:80]}...")
    except Exception as e:
        print(f"Could not query pg_stat_statements (may require pg_read_all_stats role): {e}")

    # Example 2: Reset statistics using pg_stat_statements_reset function
    try:
        reset_func = pg_stat_statements_reset(dialect)
        query = QueryExpression(
            dialect=dialect,
            select=[reset_func],
        )
        sql, params = query.to_sql()
        print("\n--- Reset statistics ---")
        print(f"SQL: {sql}")
        backend.execute(sql, params, options=opts)
        print("Statistics reset successfully")
    except Exception as e:
        print(f"Could not reset statistics (may require superuser): {e}")

    # Example 3: Create a test table and run some queries to generate statistics
    columns = [
        ColumnDefinition(
            name="id",
            data_type="SERIAL",
            constraints=[
                ColumnConstraint(ColumnConstraintType.PRIMARY_KEY),
            ],
        ),
        ColumnDefinition(name="name", data_type="TEXT"),
        ColumnDefinition(name="value", data_type="INTEGER"),
    ]

    create_expr = CreateTableExpression(
        dialect=dialect,
        table_name="stats_demo",
        columns=columns,
        if_not_exists=True,
    )
    sql, params = create_expr.to_sql()
    print("\n--- CREATE test table ---")
    print(f"SQL: {sql}")
    backend.execute(sql, params)

    # Insert data
    rows = [[Literal(dialect, f"item{i}"), Literal(dialect, i * 10)] for i in range(5)]
    insert_expr = InsertExpression(
        dialect=dialect,
        into="stats_demo",
        columns=["name", "value"],
        source=ValuesSource(dialect, rows),
    )
    sql, params = insert_expr.to_sql()
    backend.execute(sql, params)

    # Run a query to generate a statistic entry
    query = QueryExpression(
        dialect=dialect,
        select=[Column(dialect, "name"), Column(dialect, "value")],
        from_=TableExpression(dialect, "stats_demo"),
        where=ComparisonPredicate(
            Column(dialect, "value"),
            ">",
            Literal(dialect, 20),
        ),
    )
    sql, params = query.to_sql()
    print("\n--- Execute a query to generate statistics ---")
    print(f"SQL: {sql}")
    result = backend.execute(sql, params, options=opts)
    print(f"Query result: {result.data}")

    # Check statistics again
    try:
        query = QueryExpression(
            dialect=dialect,
            select=[
                Column(dialect, "query"),
                Column(dialect, "calls"),
            ],
            from_=TableExpression(dialect, "pg_stat_statements"),
        )
        sql, params = query.to_sql()
        result = backend.execute(sql, params, options=opts)
        print("\n--- Statistics after queries ---")
        print(f"Tracked statements: {len(result.data) if result.data else 0}")
    except Exception as e:
        print(f"Could not check statistics: {e}")

    # Cleanup demo table
    drop_expr = DropTableExpression(dialect=dialect, table_name="stats_demo", if_exists=True)
    sql, params = drop_expr.to_sql()
    backend.execute(sql, params)

else:
    print("\nSkipping execution - pg_stat_statements extension not available on this server")
    print("To enable pg_stat_statements:")
    print("1. Add 'shared_preload_libraries = pg_stat_statements' to postgresql.conf")
    print("2. Restart PostgreSQL")
    print("3. Run: CREATE EXTENSION pg_stat_statements;")

# ============================================================
# SECTION: Teardown (necessary for execution, reference only)
# ============================================================
backend.disconnect()
