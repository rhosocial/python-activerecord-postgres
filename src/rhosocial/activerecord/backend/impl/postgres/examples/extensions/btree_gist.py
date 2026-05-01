# src/rhosocial/activerecord/backend/impl/postgres/examples/extensions/btree_gist.py
"""
btree_gist extension - B-tree equivalence for GiST indexes.

This example demonstrates:
1. Check if btree_gist extension is available
2. CREATE EXTENSION and create table with timestamp column
3. Create GiST index with btree_gist operator class using CreateIndexExpression
4. Query with range condition using ComparisonPredicate
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

drop_expr = DropTableExpression(dialect=dialect, table_name="events", if_exists=True)
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
    InsertExpression,
    QueryExpression,
    TableExpression,
    Column,
    CreateIndexExpression,
)
from rhosocial.activerecord.backend.expression.statements import ValuesSource
from rhosocial.activerecord.backend.expression.core import Literal
from rhosocial.activerecord.backend.expression.predicates import ComparisonPredicate
from rhosocial.activerecord.backend.options import ExecutionOptions
from rhosocial.activerecord.backend.schema import StatementType

# Check if btree_gist extension is available
available = dialect.is_extension_available("btree_gist")
installed = dialect.is_extension_installed("btree_gist")
print(f"Extension check: btree_gist available = {available}, installed = {installed}")

# Create extension using expression
if available and not installed:
    create_ext = PostgresCreateExtensionExpression(
        dialect=dialect,
        name="btree_gist",
    )
    sql, params = create_ext.to_sql()
    print("\n--- CREATE EXTENSION ---")
    print(f"SQL: {sql}")
    backend.execute(sql, params)
    backend.introspect_and_adapt()

# Re-check after creation
installed = dialect.is_extension_installed("btree_gist")

if installed:
    # Example 1: Create table with event data
    columns = [
        ColumnDefinition(
            name="id",
            data_type="SERIAL",
            constraints=[
                ColumnConstraint(ColumnConstraintType.PRIMARY_KEY),
            ],
        ),
        ColumnDefinition(name="name", data_type="TEXT"),
        ColumnDefinition(name="created_at", data_type="TIMESTAMP"),
    ]

    create_expr = CreateTableExpression(
        dialect=dialect,
        table_name="events",
        columns=columns,
        if_not_exists=True,
    )
    sql, params = create_expr.to_sql()
    print("\n--- CREATE TABLE ---")
    print(f"SQL: {sql}")
    backend.execute(sql, params)

    # Example 2: Insert event data
    rows = [
        [Literal(dialect, "Conference"), Literal(dialect, "2024-01-15 10:00:00")],
        [Literal(dialect, "Workshop"), Literal(dialect, "2024-03-20 14:30:00")],
        [Literal(dialect, "Meetup"), Literal(dialect, "2024-06-10 09:00:00")],
        [Literal(dialect, "Hackathon"), Literal(dialect, "2024-09-05 16:45:00")],
    ]
    insert_expr = InsertExpression(
        dialect=dialect,
        into="events",
        columns=["name", "created_at"],
        source=ValuesSource(dialect, rows),
    )
    sql, params = insert_expr.to_sql()
    print("\n--- INSERT data ---")
    print(f"SQL: {sql}")
    backend.execute(sql, params)

    # Example 3: Create GiST index with btree_gist operator class
    # btree_gist allows GiST indexes to support B-tree operations on scalar types,
    # which is useful for exclusion constraints and range queries
    create_idx = CreateIndexExpression(
        dialect=dialect,
        index_name="idx_events_created_at_gist",
        table_name="events",
        columns=["created_at"],
        index_type="GIST",
        if_not_exists=True,
        dialect_options={"opclasses": {"created_at": "int4_ops"}},
    )
    sql, params = create_idx.to_sql()
    print("\n--- CREATE GIST INDEX with btree_gist operator class ---")
    print(f"SQL: {sql}")
    backend.execute(sql, params)
    print("GiST index created: idx_events_created_at_gist")

    # Example 4: Query with range condition using ComparisonPredicate
    # Find events after March 1, 2024
    opts = ExecutionOptions(stmt_type=StatementType.DQL)
    query = QueryExpression(
        dialect=dialect,
        select=[Column(dialect, "name"), Column(dialect, "created_at")],
        from_=TableExpression(dialect, "events"),
        where=ComparisonPredicate(
            dialect,
            ">",
            Column(dialect, "created_at"),
            Literal(dialect, "2024-03-01 00:00:00"),
        ),
    )
    sql, params = query.to_sql()
    result = backend.execute(sql, params, options=opts)
    print("\n--- QUERY events after 2024-03-01 ---")
    print(f"SQL: {sql}")
    print(f"Results: {result.data}")

else:
    print("\nSkipping execution - btree_gist extension not available on this server")
    print("To enable btree_gist, run: CREATE EXTENSION btree_gist;")

# ============================================================
# SECTION: Teardown (necessary for execution, reference only)
# ============================================================
drop_expr = DropTableExpression(dialect=dialect, table_name="events", if_exists=True)
sql, params = drop_expr.to_sql()
backend.execute(sql, params)
backend.disconnect()
