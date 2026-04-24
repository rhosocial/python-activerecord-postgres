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
from rhosocial.activerecord.backend.expression import DropTableExpression

drop_expr = DropTableExpression(dialect=dialect, table_name="products", if_exists=True)
sql, params = drop_expr.to_sql()
backend.execute(sql, params)

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
    QueryExpression,
    TableExpression,
    Column,
    OrderByClause,
    LimitOffsetClause,
)
from rhosocial.activerecord.backend.expression.core import Literal, Subquery
from rhosocial.activerecord.backend.expression.operators import (
    BinaryExpression,
    RawSQLExpression,
)
from rhosocial.activerecord.backend.expression.predicates import ComparisonPredicate
from rhosocial.activerecord.backend.expression.statements.dml import (
    InsertExpression,
)
from rhosocial.activerecord.backend.expression.statements import (
    ValuesSource,
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
    query = QueryExpression(
        dialect=dialect,
        select=[Column(dialect, "name"), Column(dialect, "feature_vector")],
        from_=TableExpression(dialect, "products"),
        where=BinaryExpression(
            dialect, "@>",
            RawSQLExpression(dialect, "'(0.1,0.5),(0.2,0.6),(0.0,0.3)'"),
            Column(dialect, "feature_vector"),
        ),
        order_by=OrderByClause(dialect, expressions=[Column(dialect, "name")]),
    )
    sql, params = query.to_sql()
    result = backend.execute(sql, params, options=opts)
    print(f"\n--- Contains operator (@>) ---")
    print(f"Points contained within range '(0.1,0.5),(0.2,0.6),(0.0,0.3)':")
    print(f"SQL: {sql}")
    print(f"Results: {result.data}")

    # Example 4: Contained by operator (<@)
    query = QueryExpression(
        dialect=dialect,
        select=[Column(dialect, "name"), Column(dialect, "feature_vector")],
        from_=TableExpression(dialect, "products"),
        where=BinaryExpression(
            dialect, "<@",
            Column(dialect, "feature_vector"),
            RawSQLExpression(dialect, "'(0.1,0.5),(0.2,0.6),(0.0,0.3)'"),
        ),
        order_by=OrderByClause(dialect, expressions=[Column(dialect, "name")]),
    )
    sql, params = query.to_sql()
    result = backend.execute(sql, params, options=opts)
    print(f"\n--- Contained by operator (<@) ---")
    print(f"Points contained by the range cube:")
    print(f"SQL: {sql}")
    print(f"Results: {result.data}")

    # Example 5: Distance operator (<->)
    # Euclidean distance between cubes
    # BinaryExpression lacks AliasableMixin, so wrap with Subquery to add alias.
    # We need separate instances for SELECT, WHERE, and ORDER BY.
    dist_for_select = Subquery(
        dialect,
        BinaryExpression(
            dialect, "<->",
            Column(dialect, "feature_vector"),
            RawSQLExpression(dialect, "'(0.5, 0.8, 0.2)'"),
        ),
    ).as_("distance")

    dist_base = BinaryExpression(
        dialect, "<->",
        Column(dialect, "feature_vector"),
        RawSQLExpression(dialect, "'(0.5, 0.8, 0.2)'"),
    )
    dist_sql, dist_params = dist_base.to_sql()

    # Wrap the distance expression SQL as a RawSQLExpression so it can be used
    # in ComparisonPredicate (< comparison with the threshold)
    dist_value_expr = RawSQLExpression(dialect, dist_sql, dist_params)

    dist_for_order = BinaryExpression(
        dialect, "<->",
        Column(dialect, "feature_vector"),
        RawSQLExpression(dialect, "'(0.5, 0.8, 0.2)'"),
    )

    query = QueryExpression(
        dialect=dialect,
        select=[
            Column(dialect, "name"),
            Column(dialect, "feature_vector"),
            dist_for_select,
        ],
        from_=TableExpression(dialect, "products"),
        where=ComparisonPredicate(
            dialect, "<",
            dist_value_expr,
            Literal(dialect, 0.5),
        ),
        order_by=OrderByClause(dialect, expressions=[dist_for_order]),
    )
    sql, params = query.to_sql()
    result = backend.execute(sql, params, options=opts)
    print(f"\n--- Distance operator (<->) ---")
    print(f"Products within distance 0.5 from point (0.5, 0.8, 0.2):")
    print(f"SQL: {sql}")
    print(f"Params: {params}")
    print(f"Results: {result.data}")

    # Example 6: Nearest neighbor search
    nn_dist_for_select = Subquery(
        dialect,
        BinaryExpression(
            dialect, "<->",
            Column(dialect, "feature_vector"),
            RawSQLExpression(dialect, "'(0.4, 0.5, 0.6)'"),
        ),
    ).as_("distance")

    nn_dist_for_order = BinaryExpression(
        dialect, "<->",
        Column(dialect, "feature_vector"),
        RawSQLExpression(dialect, "'(0.4, 0.5, 0.6)'"),
    )

    query = QueryExpression(
        dialect=dialect,
        select=[
            Column(dialect, "name"),
            Column(dialect, "feature_vector"),
            nn_dist_for_select,
        ],
        from_=TableExpression(dialect, "products"),
        order_by=OrderByClause(dialect, expressions=[nn_dist_for_order]),
        limit_offset=LimitOffsetClause(dialect, limit=3),
    )
    sql, params = query.to_sql()
    result = backend.execute(sql, params, options=opts)
    print(f"\n--- Nearest neighbor search ---")
    print(f"3 nearest to point (0.4, 0.5, 0.6):")
    print(f"SQL: {sql}")
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
drop_expr = DropTableExpression(dialect=dialect, table_name="products", if_exists=True)
sql, params = drop_expr.to_sql()
backend.execute(sql, params)
backend.disconnect()
