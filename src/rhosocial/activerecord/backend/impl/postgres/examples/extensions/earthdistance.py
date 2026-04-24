# src/rhosocial/activerecord/backend/impl/postgres/examples/extensions/earthdistance.py
"""
earthdistance extension - geographic distance calculations.

This example demonstrates:
1. Check if earthdistance extension is available (depends on cube)
2. CREATE EXTENSION and create table with coordinate columns
3. Calculate distance between two points
4. Find locations within a specified range
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

# Clean up for demo using DropTableExpression
from rhosocial.activerecord.backend.expression import DropTableExpression

drop_expr = DropTableExpression(dialect=dialect, table_name="cities", if_exists=True)
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
    QueryExpression,
    Column,
    TableExpression,
)
from rhosocial.activerecord.backend.expression.core import FunctionCall, Literal, Subquery
from rhosocial.activerecord.backend.expression.operators import BinaryExpression
from rhosocial.activerecord.backend.expression.query_parts import OrderByClause
from rhosocial.activerecord.backend.expression.statements.dml import (
    InsertExpression,
)
from rhosocial.activerecord.backend.expression.statements import (
    ValuesSource,
)
from rhosocial.activerecord.backend.options import ExecutionOptions
from rhosocial.activerecord.backend.schema import StatementType

opts = ExecutionOptions(stmt_type=StatementType.DQL)

# Check if earthdistance extension is available
# earthdistance depends on cube extension
cube_available = dialect.is_extension_available("cube")
cube_installed = dialect.is_extension_installed("cube")
available = dialect.is_extension_available("earthdistance")
installed = dialect.is_extension_installed("earthdistance")
print(f"Extension check: cube available = {cube_available}, installed = {cube_installed}")
print(f"Extension check: earthdistance available = {available}, installed = {installed}")

# Create cube extension first (dependency of earthdistance)
if cube_available and not cube_installed:
    create_ext = CreateExtensionExpression(
        dialect=dialect,
        name="cube",
    )
    sql, params = create_ext.to_sql()
    print(f"\n--- CREATE EXTENSION (cube) ---")
    print(f"SQL: {sql}")
    backend.execute(sql, params)
    backend.introspect_and_adapt()

# Create earthdistance extension
if available and not installed:
    create_ext = CreateExtensionExpression(
        dialect=dialect,
        name="earthdistance",
    )
    sql, params = create_ext.to_sql()
    print(f"\n--- CREATE EXTENSION (earthdistance) ---")
    print(f"SQL: {sql}")
    backend.execute(sql, params)
    # Re-detect extensions after creation
    backend.introspect_and_adapt()

# Re-check after creation
installed = dialect.is_extension_installed("earthdistance")

if installed:
    # Example 1: Create table with coordinate columns
    # Store latitude and longitude as FLOAT8
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
            name="latitude",
            data_type="FLOAT8",
            constraints=[
                ColumnConstraint(ColumnConstraintType.NOT_NULL),
            ],
        ),
        ColumnDefinition(
            name="longitude",
            data_type="FLOAT8",
            constraints=[
                ColumnConstraint(ColumnConstraintType.NOT_NULL),
            ],
        ),
    ]

    create_expr = CreateTableExpression(
        dialect=dialect,
        table_name="cities",
        columns=columns,
        if_not_exists=True,
    )
    sql, params = create_expr.to_sql()
    print(f"\n--- CREATE TABLE ---")
    print(f"SQL: {sql}")
    backend.execute(sql, params)

    # Example 2: Insert city coordinate data
    insert_expr = InsertExpression(
        dialect=dialect,
        into="cities",
        columns=["name", "latitude", "longitude"],
        source=ValuesSource(
            dialect,
            [
                [Literal(dialect, "New York"), Literal(dialect, 40.7128), Literal(dialect, -74.0060)],
                [Literal(dialect, "Los Angeles"), Literal(dialect, 34.0522), Literal(dialect, -118.2437)],
                [Literal(dialect, "Chicago"), Literal(dialect, 41.8781), Literal(dialect, -87.6298)],
                [Literal(dialect, "Houston"), Literal(dialect, 29.7604), Literal(dialect, -95.3698)],
                [Literal(dialect, "Philadelphia"), Literal(dialect, 39.9526), Literal(dialect, -75.1652)],
            ],
        ),
    )
    sql, params = insert_expr.to_sql()
    print(f"\n--- INSERT coordinate data ---")
    print(f"SQL: {sql}")
    print(f"Params: {params}")
    backend.execute(sql, params)

    # Example 3: Calculate distance between two cities
    # earth_distance(ll_to_earth(lat1, lon1), ll_to_earth(lat2, lon2))
    # Returns distance in meters (on earth surface)
    ny_point = FunctionCall(
        dialect, "ll_to_earth",
        Literal(dialect, 40.7128), Literal(dialect, -74.0060),
    )
    la_point = FunctionCall(
        dialect, "ll_to_earth",
        Literal(dialect, 34.0522), Literal(dialect, -118.2437),
    )
    distance_func = FunctionCall(dialect, "earth_distance", ny_point, la_point)

    query = QueryExpression(
        dialect=dialect,
        select=[distance_func.as_("distance_meters")],
    )
    sql, params = query.to_sql()
    print(f"\n--- Distance between New York and Los Angeles ---")
    print(f"SQL: {sql}")
    result = backend.execute(sql, params, options=opts)
    print(f"Result: {result.data}")
    if result.data and result.data[0]:
        km = result.data[0]["distance_meters"] / 1000
        print(f"Distance (km): {km:.1f}")

    # Example 4: Find cities within a radius using earth_box + earth_distance
    # earth_box returns the bounding box for a point and radius
    # Then use earth_distance for exact distance check
    # The @> operator checks if the bounding box contains the city point
    ny_point = FunctionCall(
        dialect, "ll_to_earth",
        Literal(dialect, 40.7128), Literal(dialect, -74.0060),
    )
    city_point = FunctionCall(
        dialect, "ll_to_earth",
        Column(dialect, "latitude"), Column(dialect, "longitude"),
    )
    distance_func = FunctionCall(dialect, "earth_distance", ny_point, city_point)
    box_func = FunctionCall(
        dialect, "earth_box",
        ny_point, Literal(dialect, 500000),
    )

    # Build distance_km expression: earth_distance(...) / 1000.0
    # BinaryExpression lacks AliasableMixin, so wrap with Subquery to add alias
    distance_km_expr = BinaryExpression(
        dialect, "/", distance_func, Literal(dialect, 1000.0),
    )
    distance_km_aliased = Subquery(dialect, distance_km_expr).as_("distance_km")

    # WHERE: box @> city_point (the @> is the contains operator for cube type)
    contains_pred = BinaryExpression(dialect, "@>", box_func, city_point)

    query = QueryExpression(
        dialect=dialect,
        select=[
            Column(dialect, "name"),
            distance_km_aliased,
        ],
        from_=TableExpression(dialect, "cities"),
        where=contains_pred,
        order_by=OrderByClause(
            dialect,
            expressions=[(distance_km_expr, "ASC")],
        ),
    )
    sql, params = query.to_sql()
    print(f"\n--- Cities within 500km of New York ---")
    print(f"SQL: {sql}")
    print(f"Params: {params}")
    result = backend.execute(sql, params, options=opts)
    print(f"Results: {result.data}")

    # Example 5: Simple radius search with exact distance
    # Find cities within 1500km of Chicago using earth_distance directly
    chicago_point = FunctionCall(
        dialect, "ll_to_earth",
        Literal(dialect, 41.8781), Literal(dialect, -87.6298),
    )
    city_point = FunctionCall(
        dialect, "ll_to_earth",
        Column(dialect, "latitude"), Column(dialect, "longitude"),
    )
    distance_func = FunctionCall(dialect, "earth_distance", chicago_point, city_point)

    # ROUND(earth_distance(...) / 1000.0) AS distance_km
    distance_km_expr = BinaryExpression(
        dialect, "/", distance_func, Literal(dialect, 1000.0),
    )
    round_distance = FunctionCall(dialect, "ROUND", distance_km_expr)

    # WHERE: earth_distance(...) < 1500000
    within_radius_pred = BinaryExpression(
        dialect, "<", distance_func, Literal(dialect, 1500000),
    )

    query = QueryExpression(
        dialect=dialect,
        select=[
            Column(dialect, "name"),
            round_distance.as_("distance_km"),
        ],
        from_=TableExpression(dialect, "cities"),
        where=within_radius_pred,
        order_by=OrderByClause(
            dialect,
            expressions=[(distance_km_expr, "ASC")],
        ),
    )
    sql, params = query.to_sql()
    print(f"\n--- Cities within 1500km of Chicago ---")
    print(f"SQL: {sql}")
    print(f"Params: {params}")
    result = backend.execute(sql, params, options=opts)
    print(f"Results: {result.data}")

else:
    print("\nSkipping execution - earthdistance not available on this server")
    print("To enable earthdistance, install cube and run:")
    print("  CREATE EXTENSION cube;")
    print("  CREATE EXTENSION earthdistance;")

# ============================================================
# SECTION: Teardown (necessary for execution, reference only)
# ============================================================
drop_expr = DropTableExpression(dialect=dialect, table_name="cities", if_exists=True)
sql, params = drop_expr.to_sql()
backend.execute(sql, params)
backend.disconnect()
