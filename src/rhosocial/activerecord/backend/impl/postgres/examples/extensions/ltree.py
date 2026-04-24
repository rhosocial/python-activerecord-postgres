# src/rhosocial/activerecord/backend/impl/postgres/examples/extensions/ltree.py
"""
ltree extension - hierarchical path data type.

This example demonstrates:
1. Check if ltree extension is available
2. CREATE EXTENSION and create table with ltree column
3. Insert hierarchical path data
4. Execute ancestor/descendant queries (@>, <@)
5. Execute pattern matching queries (~)
6. Create GiST index for fast path queries
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
backend.execute("DROP TABLE IF EXISTS categories", ())

# ============================================================
# SECTION: Business Logic (the pattern to learn)
# ============================================================
from rhosocial.activerecord.backend.impl.postgres.expression import (
    CreateExtensionExpression,
)
from rhosocial.activerecord.backend.expression import (
    CreateTableExpression,
    ColumnDefinition,
    ColumnConstraint,
    ColumnConstraintType,
    CreateIndexExpression,
)
from rhosocial.activerecord.backend.expression.core import Literal
from rhosocial.activerecord.backend.expression.statements.dml import (
    InsertExpression,
)
from rhosocial.activerecord.backend.expression.statements import (
    ValuesSource,
)
from rhosocial.activerecord.backend.expression import (
    Column,
    QueryExpression,
    TableExpression,
)
from rhosocial.activerecord.backend.options import ExecutionOptions
from rhosocial.activerecord.backend.schema import StatementType

# Check if ltree extension is available
available = dialect.is_extension_available("ltree")
installed = dialect.is_extension_installed("ltree")
print(f"Extension check: ltree available = {available}, installed = {installed}")

# Create extension using expression
if available and not installed:
    create_ext = CreateExtensionExpression(
        dialect=dialect,
        name="ltree",
    )
    sql, params = create_ext.to_sql()
    print(f"\n--- CREATE EXTENSION ---")
    print(f"SQL: {sql}")
    backend.execute(sql, params)
    # Re-detect extensions after creation
    backend.introspect_and_adapt()

# Re-check after creation
installed = dialect.is_extension_installed("ltree")

if installed:
    # Example 1: Create table with ltree column
    columns = [
        ColumnDefinition(
            name="id",
            data_type="SERIAL",
            constraints=[
                ColumnConstraint(ColumnConstraintType.PRIMARY_KEY),
            ],
        ),
        ColumnDefinition(
            name="name",
            data_type="VARCHAR(100)",
            constraints=[
                ColumnConstraint(ColumnConstraintType.NOT_NULL),
            ],
        ),
        ColumnDefinition(
            name="path",
            data_type="LTREE",
            constraints=[
                ColumnConstraint(ColumnConstraintType.NOT_NULL),
            ],
        ),
    ]

    create_expr = CreateTableExpression(
        dialect=dialect,
        table_name="categories",
        columns=columns,
        if_not_exists=True,
    )
    sql, params = create_expr.to_sql()
    print(f"\n--- CREATE TABLE ---")
    print(f"SQL: {sql}")
    backend.execute(sql, params)

    # Example 2: Insert hierarchical path data
    # ltree paths use dot-separated labels (only alphanumeric and underscore)
    insert_expr = InsertExpression(
        dialect=dialect,
        into="categories",
        columns=["name", "path"],
        source=ValuesSource(
            dialect,
            [
                [Literal(dialect, "Electronics"), Literal(dialect, "electronics")],
                [Literal(dialect, "Computers"), Literal(dialect, "electronics.computers")],
                [Literal(dialect, "Laptops"), Literal(dialect, "electronics.computers.laptops")],
                [Literal(dialect, "Desktops"), Literal(dialect, "electronics.computers.desktops")],
                [Literal(dialect, "Phones"), Literal(dialect, "electronics.phones")],
                [Literal(dialect, "Smartphones"), Literal(dialect, "electronics.phones.smartphones")],
                [Literal(dialect, "Clothing"), Literal(dialect, "clothing")],
                [Literal(dialect, "Men"), Literal(dialect, "clothing.men")],
                [Literal(dialect, "Shirts"), Literal(dialect, "clothing.men.shirts")],
            ],
        ),
    )
    sql, params = insert_expr.to_sql()
    print(f"\n--- INSERT hierarchical data ---")
    print(f"SQL: {sql}")
    print(f"Params: {params}")
    backend.execute(sql, params)

    # Example 3: Ancestor query - find all descendants of 'electronics.computers'
    # @> means "is ancestor of" (left is ancestor of right)
    opts = ExecutionOptions(stmt_type=StatementType.DQL)
    result = backend.execute(
        "SELECT name, path::TEXT FROM categories "
        "WHERE 'electronics.computers' @> path "
        "ORDER BY path",
        (),
        options=opts,
    )
    print(f"\n--- Ancestor query (@>) ---")
    print(f"Find descendants of 'electronics.computers':")
    print(f"Results: {result.data}")

    # Example 4: Descendant query - find all ancestors of 'electronics.computers.laptops'
    # <@ means "is descendant of" (left is descendant of right)
    result = backend.execute(
        "SELECT name, path::TEXT FROM categories "
        "WHERE path <@ 'electronics.computers.laptops' "
        "ORDER BY path",
        (),
        options=opts,
    )
    print(f"\n--- Descendant query (<@) ---")
    print(f"Find ancestors of 'electronics.computers.laptops':")
    print(f"Results: {result.data}")

    # Example 5: Pattern matching query (~)
    # Find categories matching a lquery pattern
    # '*' matches any single label, '*{,2}' matches up to 2 labels
    result = backend.execute(
        "SELECT name, path::TEXT FROM categories "
        "WHERE path ~ 'electronics.*{1}' "
        "ORDER BY path",
        (),
        options=opts,
    )
    print(f"\n--- Pattern matching query (~) ---")
    print(f"Pattern: 'electronics.*{{1}}' (direct children of electronics):")
    print(f"Results: {result.data}")

    # Example 6: Search for any path under electronics at any depth
    result = backend.execute(
        "SELECT name, path::TEXT FROM categories "
        "WHERE path ~ 'electronics.*' "
        "ORDER BY path",
        (),
        options=opts,
    )
    print(f"\n--- Pattern matching (any depth) ---")
    print(f"Pattern: 'electronics.*' (all under electronics):")
    print(f"Results: {result.data}")

    # Example 7: Create GiST index for fast path queries
    create_idx = CreateIndexExpression(
        dialect=dialect,
        index_name="idx_categories_path",
        table_name="categories",
        columns=["path"],
        index_type="GIST",
        if_not_exists=True,
    )
    sql, params = create_idx.to_sql()
    print(f"\n--- CREATE GIST INDEX ---")
    print(f"SQL: {sql}")
    backend.execute(sql, params)
    print("GiST index created: idx_categories_path")

else:
    print("\nSkipping execution - ltree not available on this server")
    print("To enable ltree, run: CREATE EXTENSION ltree;")

# ============================================================
# SECTION: Teardown (necessary for execution, reference only)
# ============================================================
backend.execute("DROP TABLE IF EXISTS categories", ())
backend.disconnect()
