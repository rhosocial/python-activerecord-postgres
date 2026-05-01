# src/rhosocial/activerecord/backend/impl/postgres/examples/extensions/hypopg.py
"""
hypopg extension - hypothetical index testing.

This example demonstrates:
1. Check if hypopg extension is available
2. CREATE EXTENSION and create table
3. Create hypothetical indexes using hypopg_create_index
4. List hypothetical indexes using hypopg_show_indexes
5. Estimate index size using hypopg_estimate_size
6. Reset hypothetical indexes using hypopg_reset
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

drop_expr = DropTableExpression(dialect=dialect, table_name="products", if_exists=True)
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
)
from rhosocial.activerecord.backend.expression.statements import ValuesSource
from rhosocial.activerecord.backend.expression.core import Literal
from rhosocial.activerecord.backend.options import ExecutionOptions
from rhosocial.activerecord.backend.schema import StatementType
from rhosocial.activerecord.backend.impl.postgres.functions.hypopg import (
    hypopg_create_index,
    hypopg_reset,
    hypopg_show_indexes,
    hypopg_estimate_size,
)

# Check if hypopg extension is available
available = dialect.is_extension_available("hypopg")
installed = dialect.is_extension_installed("hypopg")
print(f"Extension check: hypopg available = {available}, installed = {installed}")

# Create extension using expression
if available and not installed:
    create_ext = PostgresCreateExtensionExpression(
        dialect=dialect,
        name="hypopg",
    )
    sql, params = create_ext.to_sql()
    print("\n--- CREATE EXTENSION ---")
    print(f"SQL: {sql}")
    backend.execute(sql, params)
    backend.introspect_and_adapt()

# Re-check after creation
installed = dialect.is_extension_installed("hypopg")

if installed:
    # Example 1: Create table with product data
    columns = [
        ColumnDefinition(
            name="id",
            data_type="SERIAL",
            constraints=[
                ColumnConstraint(ColumnConstraintType.PRIMARY_KEY),
            ],
        ),
        ColumnDefinition(name="name", data_type="TEXT"),
        ColumnDefinition(name="category", data_type="TEXT"),
        ColumnDefinition(name="price", data_type="NUMERIC(10,2)"),
    ]

    create_expr = CreateTableExpression(
        dialect=dialect,
        table_name="products",
        columns=columns,
        if_not_exists=True,
    )
    sql, params = create_expr.to_sql()
    print("\n--- CREATE TABLE ---")
    print(f"SQL: {sql}")
    backend.execute(sql, params)

    # Insert sample data
    rows = [
        [Literal(dialect, "Laptop"), Literal(dialect, "Electronics"), Literal(dialect, 999.99)],
        [Literal(dialect, "Mouse"), Literal(dialect, "Electronics"), Literal(dialect, 29.99)],
        [Literal(dialect, "Desk"), Literal(dialect, "Furniture"), Literal(dialect, 249.99)],
        [Literal(dialect, "Chair"), Literal(dialect, "Furniture"), Literal(dialect, 199.99)],
        [Literal(dialect, "Monitor"), Literal(dialect, "Electronics"), Literal(dialect, 499.99)],
    ]
    insert_expr = InsertExpression(
        dialect=dialect,
        into="products",
        columns=["name", "category", "price"],
        source=ValuesSource(dialect, rows),
    )
    sql, params = insert_expr.to_sql()
    print("\n--- INSERT data ---")
    print(f"SQL: {sql}")
    backend.execute(sql, params)

    # Example 2: Create a hypothetical B-tree index
    create_func = hypopg_create_index(
        dialect,
        "CREATE INDEX ON products USING btree (category)",
    )
    query = QueryExpression(
        dialect=dialect,
        select=[create_func.as_("index_id")],
    )
    sql, params = query.to_sql()
    print("\n--- CREATE HYPOTHETICAL INDEX ---")
    print(f"SQL: {sql}")
    opts = ExecutionOptions(stmt_type=StatementType.DQL)
    result = backend.execute(sql, params, options=opts)
    print(f"Result: {result.data}")
    if result.data and result.data[0]["index_id"] is not None:
        index_id = result.data[0]["index_id"]
        print(f"Hypothetical index created with ID: {index_id}")

        # Example 3: Estimate the size of the hypothetical index
        size_func = hypopg_estimate_size(dialect, index_id)
        query = QueryExpression(
            dialect=dialect,
            select=[size_func.as_("estimated_size")],
        )
        sql, params = query.to_sql()
        print("\n--- ESTIMATE INDEX SIZE ---")
        print(f"SQL: {sql}")
        result = backend.execute(sql, params, options=opts)
        print(f"Estimated size: {result.data}")

    # Example 4: List all hypothetical indexes
    show_func = hypopg_show_indexes(dialect)
    query = QueryExpression(
        dialect=dialect,
        select=[show_func.as_("index_info")],
    )
    sql, params = query.to_sql()
    print("\n--- LIST HYPOTHETICAL INDEXES ---")
    print(f"SQL: {sql}")
    result = backend.execute(sql, params, options=opts)
    print(f"Hypothetical indexes: {result.data}")

    # Example 5: Reset all hypothetical indexes
    reset_func = hypopg_reset(dialect)
    query = QueryExpression(
        dialect=dialect,
        select=[reset_func],
    )
    sql, params = query.to_sql()
    print("\n--- RESET HYPOTHETICAL INDEXES ---")
    print(f"SQL: {sql}")
    backend.execute(sql, params, options=opts)
    print("All hypothetical indexes have been reset")

else:
    print("\nSkipping execution - hypopg extension not available on this server")
    print("To enable hypopg, install it and run: CREATE EXTENSION hypopg;")

# ============================================================
# SECTION: Teardown (necessary for execution, reference only)
# ============================================================
drop_expr = DropTableExpression(dialect=dialect, table_name="products", if_exists=True)
sql, params = drop_expr.to_sql()
backend.execute(sql, params)
backend.disconnect()
