# src/rhosocial/activerecord/backend/impl/postgres/examples/extensions/pgrouting.py
"""
pgrouting extension - geospatial routing functionality.

This example demonstrates:
1. Check if postgis and pgrouting extensions are available
2. CREATE EXTENSION and create edges table with routing columns
3. Insert edge data for a simple directed graph
4. Execute pgr_dijkstra for shortest path (Dijkstra's algorithm)
5. Execute pgr_astar for shortest path (A* algorithm)
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

drop_expr = DropTableExpression(dialect=dialect, table_name="route_edges", if_exists=True)
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
from rhosocial.activerecord.backend.impl.postgres.functions.pgrouting import (
    pgr_dijkstra,
    pgr_astar,
)

opts = ExecutionOptions(stmt_type=StatementType.DQL)

# Check if extensions are available
postgis_available = dialect.is_extension_available("postgis")
postgis_installed = dialect.is_extension_installed("postgis")
pgr_available = dialect.is_extension_available("pgrouting")
pgr_installed = dialect.is_extension_installed("pgrouting")
print(f"Extension: postgis available = {postgis_available}, installed = {postgis_installed}")
print(f"Extension: pgrouting available = {pgr_available}, installed = {pgr_installed}")

# Create extensions if needed
if postgis_available and not postgis_installed:
    create_ext = PostgresCreateExtensionExpression(
        dialect=dialect,
        name="postgis",
    )
    sql, params = create_ext.to_sql()
    print("\n--- CREATE EXTENSION postgis ---")
    print(f"SQL: {sql}")
    backend.execute(sql, params)
    backend.introspect_and_adapt()

if pgr_available and not pgr_installed:
    create_ext = PostgresCreateExtensionExpression(
        dialect=dialect,
        name="pgrouting",
    )
    sql, params = create_ext.to_sql()
    print("\n--- CREATE EXTENSION pgrouting ---")
    print(f"SQL: {sql}")
    backend.execute(sql, params)
    backend.introspect_and_adapt()

# Re-check after creation
pgr_installed = dialect.is_extension_installed("pgrouting")

if pgr_installed:
    # Example 1: Create edges table for routing
    columns = [
        ColumnDefinition(
            name="id",
            data_type="SERIAL",
            constraints=[
                ColumnConstraint(ColumnConstraintType.PRIMARY_KEY),
            ],
        ),
        ColumnDefinition(name="source", data_type="INT"),
        ColumnDefinition(name="target", data_type="INT"),
        ColumnDefinition(name="cost", data_type="FLOAT"),
        ColumnDefinition(name="reverse_cost", data_type="FLOAT"),
        ColumnDefinition(name="x1", data_type="FLOAT"),
        ColumnDefinition(name="y1", data_type="FLOAT"),
        ColumnDefinition(name="x2", data_type="FLOAT"),
        ColumnDefinition(name="y2", data_type="FLOAT"),
    ]

    create_expr = CreateTableExpression(
        dialect=dialect,
        table_name="route_edges",
        columns=columns,
        if_not_exists=True,
    )
    sql, params = create_expr.to_sql()
    print("\n--- CREATE TABLE ---")
    print(f"SQL: {sql}")
    backend.execute(sql, params)

    # Example 2: Insert edge data for a simple directed graph
    # Graph: 1 --(cost=1)--> 2 --(cost=2)--> 3
    #             1 --(cost=4)--> 3
    insert_expr = InsertExpression(
        dialect=dialect,
        into="route_edges",
        columns=["source", "target", "cost", "reverse_cost", "x1", "y1", "x2", "y2"],
        source=ValuesSource(
            dialect,
            [
                [Literal(dialect, 1), Literal(dialect, 2),
                 Literal(dialect, 1.0), Literal(dialect, 1.0),
                 Literal(dialect, 0.0), Literal(dialect, 0.0),
                 Literal(dialect, 1.0), Literal(dialect, 0.0)],
                [Literal(dialect, 2), Literal(dialect, 3),
                 Literal(dialect, 2.0), Literal(dialect, 2.0),
                 Literal(dialect, 1.0), Literal(dialect, 0.0),
                 Literal(dialect, 2.0), Literal(dialect, 0.0)],
                [Literal(dialect, 1), Literal(dialect, 3),
                 Literal(dialect, 4.0), Literal(dialect, 4.0),
                 Literal(dialect, 0.0), Literal(dialect, 0.0),
                 Literal(dialect, 2.0), Literal(dialect, 0.0)],
            ],
        ),
    )
    sql, params = insert_expr.to_sql()
    print("\n--- INSERT edge data ---")
    print(f"SQL: {sql}")
    print(f"Params: {params}")
    backend.execute(sql, params)

    # Example 3: Shortest path using Dijkstra's algorithm
    dijkstra_func = pgr_dijkstra(
        dialect,
        "SELECT id, source, target, cost, reverse_cost FROM route_edges",
        Literal(dialect, 1),
        Literal(dialect, 3),
    )
    query = QueryExpression(
        dialect=dialect,
        select=[dijkstra_func.as_("route")],
    )
    sql, params = query.to_sql()
    print("\n--- pgr_dijkstra (1 -> 3) ---")
    print(f"SQL: {sql}")
    print(f"Params: {params}")
    result = backend.execute(sql, params, options=opts)
    print(f"Shortest path (Dijkstra): {result.data}")

    # Example 4: Shortest path using A* algorithm
    astar_func = pgr_astar(
        dialect,
        "SELECT id, source, target, cost, reverse_cost, x1, y1, x2, y2 FROM route_edges",
        Literal(dialect, 1),
        Literal(dialect, 3),
    )
    query = QueryExpression(
        dialect=dialect,
        select=[astar_func.as_("route")],
    )
    sql, params = query.to_sql()
    print("\n--- pgr_astar (1 -> 3) ---")
    print(f"SQL: {sql}")
    print(f"Params: {params}")
    result = backend.execute(sql, params, options=opts)
    print(f"Shortest path (A*): {result.data}")

else:
    print("\nSkipping - pgrouting not available on this server")
    print("To enable: install PostGIS and pgRouting, then run:")
    print("  CREATE EXTENSION postgis;")
    print("  CREATE EXTENSION pgrouting;")

# ============================================================
# SECTION: Teardown (necessary for execution, reference only)
# ============================================================
drop_expr = DropTableExpression(dialect=dialect, table_name="route_edges", if_exists=True)
sql, params = drop_expr.to_sql()
backend.execute(sql, params)
backend.disconnect()
