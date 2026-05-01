# src/rhosocial/activerecord/backend/impl/postgres/examples/extensions/btree_gin.py
"""
btree_gin extension - B-tree equivalence for GIN indexes.

This example demonstrates:
1. Check if btree_gin extension is available
2. CREATE EXTENSION and create table with text and array columns
3. Create GIN index with btree_gin operator class using CreateIndexExpression
4. Query data using the GIN index
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

drop_expr = DropTableExpression(dialect=dialect, table_name="employees", if_exists=True)
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
from rhosocial.activerecord.backend.options import ExecutionOptions
from rhosocial.activerecord.backend.schema import StatementType

# Check if btree_gin extension is available
available = dialect.is_extension_available("btree_gin")
installed = dialect.is_extension_installed("btree_gin")
print(f"Extension check: btree_gin available = {available}, installed = {installed}")

# Create extension using expression
if available and not installed:
    create_ext = PostgresCreateExtensionExpression(
        dialect=dialect,
        name="btree_gin",
    )
    sql, params = create_ext.to_sql()
    print("\n--- CREATE EXTENSION ---")
    print(f"SQL: {sql}")
    backend.execute(sql, params)
    backend.introspect_and_adapt()

# Re-check after creation
installed = dialect.is_extension_installed("btree_gin")

if installed:
    # Example 1: Create table with employee data
    columns = [
        ColumnDefinition(
            name="id",
            data_type="SERIAL",
            constraints=[
                ColumnConstraint(ColumnConstraintType.PRIMARY_KEY),
            ],
        ),
        ColumnDefinition(name="name", data_type="TEXT"),
        ColumnDefinition(name="department", data_type="TEXT"),
    ]

    create_expr = CreateTableExpression(
        dialect=dialect,
        table_name="employees",
        columns=columns,
        if_not_exists=True,
    )
    sql, params = create_expr.to_sql()
    print("\n--- CREATE TABLE ---")
    print(f"SQL: {sql}")
    backend.execute(sql, params)

    # Example 2: Insert employee data
    rows = [
        [Literal(dialect, "Alice"), Literal(dialect, "Engineering")],
        [Literal(dialect, "Bob"), Literal(dialect, "Marketing")],
        [Literal(dialect, "Charlie"), Literal(dialect, "Engineering")],
        [Literal(dialect, "Diana"), Literal(dialect, "Sales")],
        [Literal(dialect, "Eve"), Literal(dialect, "Engineering")],
    ]
    insert_expr = InsertExpression(
        dialect=dialect,
        into="employees",
        columns=["name", "department"],
        source=ValuesSource(dialect, rows),
    )
    sql, params = insert_expr.to_sql()
    print("\n--- INSERT data ---")
    print(f"SQL: {sql}")
    backend.execute(sql, params)

    # Example 3: Create GIN index with btree_gin operator class
    # btree_gin allows GIN indexes to support equality checks on scalar types
    create_idx = CreateIndexExpression(
        dialect=dialect,
        index_name="idx_employees_name_gin",
        table_name="employees",
        columns=["name"],
        index_type="GIN",
        if_not_exists=True,
        dialect_options={"opclasses": {"name": "int4_ops"}},
    )
    sql, params = create_idx.to_sql()
    print("\n--- CREATE GIN INDEX with btree_gin operator class ---")
    print(f"SQL: {sql}")
    backend.execute(sql, params)
    print("GIN index created: idx_employees_name_gin")

    # Example 4: Query data using the GIN index
    opts = ExecutionOptions(stmt_type=StatementType.DQL)
    query = QueryExpression(
        dialect=dialect,
        select=[Column(dialect, "id"), Column(dialect, "name"), Column(dialect, "department")],
        from_=TableExpression(dialect, "employees"),
    )
    sql, params = query.to_sql()
    result = backend.execute(sql, params, options=opts)
    print("\n--- QUERY all employees ---")
    print(f"SQL: {sql}")
    print(f"Results: {result.data}")

else:
    print("\nSkipping execution - btree_gin extension not available on this server")
    print("To enable btree_gin, run: CREATE EXTENSION btree_gin;")

# ============================================================
# SECTION: Teardown (necessary for execution, reference only)
# ============================================================
drop_expr = DropTableExpression(dialect=dialect, table_name="employees", if_exists=True)
sql, params = drop_expr.to_sql()
backend.execute(sql, params)
backend.disconnect()
