# src/rhosocial/activerecord/backend/impl/postgres/examples/extensions/cube.py
"""
cube extension - multi-dimensional cube data type.

This example demonstrates:
1. Check if cube extension is available
2. CREATE EXTENSION and create table with cube column
3. Use contains operator (@>)
4. Use distance operator (<->)
5. Create GiST index for fast cube queries
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
backend.execute("DROP TABLE IF EXISTS products", ())

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

# Check if cube extension is available
available = dialect.is_extension_available("cube")
installed = dialect.is_extension_installed("cube")
print(f"Extension check: cube available = {available}, installed = {installed}")

# Create extension using expression
if available and not installed:
    create_ext = CreateExtensionExpression(
        dialect=dialect,
        name="cube",
    )
    sql, params = create_ext.to_sql()
    print(f"\n--- CREATE EXTENSION ---")
    print(f"SQL: {sql}")
    backend.execute(sql, params)
    # Re-detect extensions after creation
    backend.introspect_and_adapt()

# Re-check after creation
installed = dialect.is_extension_installed("cube")

if installed:
    # Example 1: Create table with cube column
    # cube can represent a point or a hypercube (range in each dimension)
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
            name="feature_vector",
            data_type="CUBE",
        ),
    ]

    create_expr = CreateTableExpression(
        dialect=dialect,
        table_name="products",
        columns=columns,
        if_not_exists=True,
    )
    sql, params = create_expr.to_sql()
    print(f"\n--- CREATE TABLE ---")
    print(f"SQL: {sql}")
    backend.execute(sql, params)

    # Example 2: Insert cube data
    # Points: '(x, y, z)' represents a single point
    # Ranges: '(x1,x2),(y1,y2)' represents a hypercube
    insert_expr = InsertExpression(
        dialect=dialect,
        into="products",
        columns=["name", "feature_vector"],
        source=ValuesSource(
            dialect,
            [
                [Literal(dialect, "Widget A"), Literal(dialect, "(0.5, 0.8, 0.2)")],
                [Literal(dialect, "Widget B"), Literal(dialect, "(0.3, 0.6, 0.9)")],
                [Literal(dialect, "Widget C"), Literal(dialect, "(0.7, 0.4, 0.1)")],
                [Literal(dialect, "Widget D"), Literal(dialect, "(0.2, 0.3, 0.7)")],
                # A range entry (min/max in each dimension)
                [Literal(dialect, "Category X"), Literal(dialect, "(0.1,0.5),(0.2,0.6),(0.0,0.3)")],
            ],
        ),
    )
    sql, params = insert_expr.to_sql()
    print(f"\n--- INSERT cube data ---")
    print(f"SQL: {sql}")
    print(f"Params: {params}")
    backend.execute(sql, params)

    opts = ExecutionOptions(stmt_type=StatementType.DQL)

    # Example 3: Contains operator (@>)
    # Check if a range cube contains a point cube
    result = backend.execute(
        "SELECT name, feature_vector::TEXT FROM products "
        "WHERE '(0.1,0.5),(0.2,0.6),(0.0,0.3)' @> feature_vector "
        "ORDER BY name",
        (),
        options=opts,
    )
    print(f"\n--- Contains operator (@>) ---")
    print(f"Points contained within range '(0.1,0.5),(0.2,0.6),(0.0,0.3)':")
    print(f"Results: {result.data}")

    # Example 4: Contained by operator (<@)
    result = backend.execute(
        "SELECT name, feature_vector::TEXT FROM products "
        "WHERE feature_vector <@ '(0.1,0.5),(0.2,0.6),(0.0,0.3)' "
        "ORDER BY name",
        (),
        options=opts,
    )
    print(f"\n--- Contained by operator (<@) ---")
    print(f"Points contained by the range cube:")
    print(f"Results: {result.data}")

    # Example 5: Distance operator (<->)
    # Euclidean distance between cubes
    result = backend.execute(
        "SELECT name, feature_vector::TEXT, "
        "  feature_vector <-> '(0.5, 0.8, 0.2)' AS distance "
        "FROM products "
        "WHERE feature_vector <-> '(0.5, 0.8, 0.2)' < 0.5 "
        "ORDER BY distance",
        (),
        options=opts,
    )
    print(f"\n--- Distance operator (<->) ---")
    print(f"Products within distance 0.5 from point (0.5, 0.8, 0.2):")
    print(f"Results: {result.data}")

    # Example 6: Nearest neighbor search
    result = backend.execute(
        "SELECT name, feature_vector::TEXT, "
        "  feature_vector <-> '(0.4, 0.5, 0.6)' AS distance "
        "FROM products "
        "ORDER BY feature_vector <-> '(0.4, 0.5, 0.6)' "
        "LIMIT 3",
        (),
        options=opts,
    )
    print(f"\n--- Nearest neighbor search ---")
    print(f"3 nearest to point (0.4, 0.5, 0.6):")
    print(f"Results: {result.data}")

    # Example 7: Create GiST index for fast cube queries
    create_idx = CreateIndexExpression(
        dialect=dialect,
        index_name="idx_products_feature_vector",
        table_name="products",
        columns=["feature_vector"],
        index_type="GIST",
        if_not_exists=True,
    )
    sql, params = create_idx.to_sql()
    print(f"\n--- CREATE GIST INDEX ---")
    print(f"SQL: {sql}")
    backend.execute(sql, params)
    print("GiST index created: idx_products_feature_vector")

else:
    print("\nSkipping execution - cube not available on this server")
    print("To enable cube, run: CREATE EXTENSION cube;")

# ============================================================
# SECTION: Teardown (necessary for execution, reference only)
# ============================================================
backend.execute("DROP TABLE IF EXISTS products", ())
backend.disconnect()
