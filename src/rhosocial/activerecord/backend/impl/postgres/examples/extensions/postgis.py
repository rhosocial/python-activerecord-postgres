# src/rhosocial/activerecord/backend/impl/postgres/examples/extensions/postgis.py
"""
postgis extension - spatial data and geographic objects.

This example demonstrates:
1. Check if postgis extension is available
2. CREATE EXTENSION and create table with geometry column
3. Insert spatial data (point, linestring, polygon)
4. Execute spatial queries (ST_Distance, ST_Contains)
5. Create GIST spatial index
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

drop_expr = DropTableExpression(dialect=dialect, table_name="locations", if_exists=True)
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
    CreateIndexExpression,
)
from rhosocial.activerecord.backend.expression.core import (
    Literal,
    FunctionCall,
    Subquery,
)
from rhosocial.activerecord.backend.expression.operators import BinaryExpression
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
from rhosocial.activerecord.backend.expression.query_parts import WhereClause
from rhosocial.activerecord.backend.expression.predicates import ComparisonPredicate
from rhosocial.activerecord.backend.options import ExecutionOptions
from rhosocial.activerecord.backend.schema import StatementType

# Check if postgis extension is available
available = dialect.is_extension_available("postgis")
installed = dialect.is_extension_installed("postgis")
print(f"Extension check: postgis available = {available}, installed = {installed}")

# Create extension using expression
if available and not installed:
    create_ext = PostgresCreateExtensionExpression(
        dialect=dialect,
        name="postgis",
    )
    sql, params = create_ext.to_sql()
    print(f"\n--- CREATE EXTENSION ---")
    print(f"SQL: {sql}")
    backend.execute(sql, params)
    # Re-detect extensions after creation
    backend.introspect_and_adapt()

# Re-check after creation
installed = dialect.is_extension_installed("postgis")

if installed:
    # Example 1: Create table with geometry column
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
            name="geom",
            data_type="GEOMETRY(Point, 4326)",
        ),
    ]

    create_expr = CreateTableExpression(
        dialect=dialect,
        table_name="locations",
        columns=columns,
        if_not_exists=True,
    )
    sql, params = create_expr.to_sql()
    print(f"\n--- CREATE TABLE ---")
    print(f"SQL: {sql}")
    backend.execute(sql, params)

    # Example 2: Insert spatial data (point)
    # Use FunctionCall to compose ST_SetSRID(ST_MakePoint(lng, lat), 4326) for WGS84 points
    insert_expr = InsertExpression(
        dialect=dialect,
        into="locations",
        columns=["name", "geom"],
        source=ValuesSource(
            dialect,
            [
                [
                    Literal(dialect, "New York"),
                    FunctionCall(
                        dialect, "ST_SetSRID",
                        FunctionCall(
                            dialect, "ST_MakePoint",
                            Literal(dialect, -74.006),
                            Literal(dialect, 40.7128),
                        ),
                        Literal(dialect, 4326),
                    ),
                ],
                [
                    Literal(dialect, "Los Angeles"),
                    FunctionCall(
                        dialect, "ST_SetSRID",
                        FunctionCall(
                            dialect, "ST_MakePoint",
                            Literal(dialect, -118.2437),
                            Literal(dialect, 34.0522),
                        ),
                        Literal(dialect, 4326),
                    ),
                ],
                [
                    Literal(dialect, "Chicago"),
                    FunctionCall(
                        dialect, "ST_SetSRID",
                        FunctionCall(
                            dialect, "ST_MakePoint",
                            Literal(dialect, -87.6298),
                            Literal(dialect, 41.8781),
                        ),
                        Literal(dialect, 4326),
                    ),
                ],
            ],
        ),
    )
    sql, params = insert_expr.to_sql()
    print(f"\n--- INSERT spatial data ---")
    print(f"SQL: {sql}")
    print(f"Params: {params}")
    backend.execute(sql, params)

    # Example 3: Spatial query - ST_Distance
    # Calculate distance between New York and Los Angeles (in degrees)
    # Build subqueries for each city's geom
    ny_geom = Subquery(
        dialect,
        QueryExpression(
            dialect=dialect,
            select=[Column(dialect, "geom")],
            from_=TableExpression(dialect, "locations"),
            where=ComparisonPredicate(
                dialect, "=",
                Column(dialect, "name"),
                Literal(dialect, "New York"),
            ),
        ),
    )

    la_geom = Subquery(
        dialect,
        QueryExpression(
            dialect=dialect,
            select=[Column(dialect, "geom")],
            from_=TableExpression(dialect, "locations"),
            where=ComparisonPredicate(
                dialect, "=",
                Column(dialect, "name"),
                Literal(dialect, "Los Angeles"),
            ),
        ),
    )

    distance_query = QueryExpression(
        dialect=dialect,
        select=[
            FunctionCall(
                dialect, "ST_Distance", ny_geom, la_geom,
            ).as_("distance_degrees"),
        ],
    )
    sql, params = distance_query.to_sql()
    print(f"\n--- ST_Distance query ---")
    print(f"SQL: {sql}")
    print(f"Params: {params}")
    opts = ExecutionOptions(stmt_type=StatementType.DQL)
    result = backend.execute(sql, params, options=opts)
    print(f"Distance (degrees): {result.data}")

    # Example 4: Spatial query - ST_DWithin
    # Find locations within a bounding area
    dwithin_query = QueryExpression(
        dialect=dialect,
        select=[
            Column(dialect, "name"),
            FunctionCall(
                dialect, "ST_AsText", Column(dialect, "geom"),
            ).as_("point"),
        ],
        from_=TableExpression(dialect, "locations"),
        where=FunctionCall(
            dialect, "ST_DWithin",
            Column(dialect, "geom"),
            FunctionCall(
                dialect, "ST_SetSRID",
                FunctionCall(
                    dialect, "ST_MakePoint",
                    Literal(dialect, -74.006),
                    Literal(dialect, 40.7128),
                ),
                Literal(dialect, 4326),
            ).cast("geography"),
            Literal(dialect, 500000),
        ),
    )
    sql, params = dwithin_query.to_sql()
    print(f"\n--- ST_DWithin query (within 500km of New York) ---")
    print(f"SQL: {sql}")
    print(f"Params: {params}")
    result = backend.execute(sql, params, options=opts)
    print(f"Results: {result.data}")

    # Example 5: Create GIST spatial index
    create_idx = CreateIndexExpression(
        dialect=dialect,
        index_name="idx_locations_geom",
        table_name="locations",
        columns=["geom"],
        index_type="GIST",
        if_not_exists=True,
    )
    sql, params = create_idx.to_sql()
    print(f"\n--- CREATE GIST INDEX ---")
    print(f"SQL: {sql}")
    backend.execute(sql, params)
    print("GIST spatial index created: idx_locations_geom")

else:
    print("\nSkipping execution - postgis not available on this server")
    print("To enable postgis, install PostGIS and run: CREATE EXTENSION postgis;")

# ============================================================
# SECTION: Teardown (necessary for execution, reference only)
# ============================================================
drop_expr = DropTableExpression(dialect=dialect, table_name="locations", if_exists=True)
sql, params = drop_expr.to_sql()
backend.execute(sql, params)
backend.disconnect()
