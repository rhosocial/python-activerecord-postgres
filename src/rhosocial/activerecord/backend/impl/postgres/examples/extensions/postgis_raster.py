# src/rhosocial/activerecord/backend/impl/postgres/examples/extensions/postgis_raster.py
"""
postgis_raster extension - raster data storage and analysis.

This example demonstrates:
1. Check if postgis extension is available
2. CREATE EXTENSION and create table with raster column
3. Insert raster data using st_rast_from_hexwkb
4. Query raster metadata using st_summary
5. Query pixel values using st_value
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

drop_expr = DropTableExpression(dialect=dialect, table_name="terrain_data", if_exists=True)
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
)
from rhosocial.activerecord.backend.expression.statements import ValuesSource
from rhosocial.activerecord.backend.expression.core import Literal
from rhosocial.activerecord.backend.options import ExecutionOptions
from rhosocial.activerecord.backend.schema import StatementType
from rhosocial.activerecord.backend.impl.postgres.functions.postgis_raster import (
    st_rast_from_hexwkb,
    st_value,
    st_summary,
)

opts = ExecutionOptions(stmt_type=StatementType.DQL)

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
    print("\n--- CREATE EXTENSION ---")
    print(f"SQL: {sql}")
    backend.execute(sql, params)
    # Re-detect extensions after creation
    backend.introspect_and_adapt()

# Re-check after creation
installed = dialect.is_extension_installed("postgis")

if installed:
    # Example 1: Create table with raster column
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
            data_type="TEXT",
            constraints=[
                ColumnConstraint(ColumnConstraintType.NOT_NULL),
            ],
        ),
        ColumnDefinition(
            name="rast",
            data_type="RASTER",
        ),
    ]

    create_expr = CreateTableExpression(
        dialect=dialect,
        table_name="terrain_data",
        columns=columns,
        if_not_exists=True,
    )
    sql, params = create_expr.to_sql()
    print("\n--- CREATE TABLE ---")
    print(f"SQL: {sql}")
    backend.execute(sql, params)

    # Example 2: Insert raster data using st_rast_from_hexwkb
    # A minimal 1x1 pixel raster in HexWKB format
    hex_wkb = "01000001000000000000000000000000000000000000F03F000000000000F03F000000000000000000000000000000000000000000000000E6100000010001004400010101"
    insert_expr = InsertExpression(
        dialect=dialect,
        into="terrain_data",
        columns=["name", "rast"],
        source=ValuesSource(
            dialect,
            [
                [
                    Literal(dialect, "elevation_1x1"),
                    st_rast_from_hexwkb(dialect, hex_wkb, srid=4326),
                ],
            ],
        ),
    )
    sql, params = insert_expr.to_sql()
    print("\n--- INSERT raster data ---")
    print(f"SQL: {sql}")
    print(f"Params: {params}")
    backend.execute(sql, params)

    # Example 3: Query raster summary using st_summary
    summary_query = QueryExpression(
        dialect=dialect,
        select=[st_summary(dialect, Column(dialect, "rast")).as_("summary")],
        from_=TableExpression(dialect, "terrain_data"),
    )
    sql, params = summary_query.to_sql()
    print("\n--- ST_Summary query ---")
    print(f"SQL: {sql}")
    print(f"Params: {params}")
    result = backend.execute(sql, params, options=opts)
    print(f"Raster summary: {result.data}")

    # Example 4: Query pixel value using st_value
    value_query = QueryExpression(
        dialect=dialect,
        select=[
            st_value(
                dialect,
                Column(dialect, "rast"),
                Literal(dialect, 1),
                Literal(dialect, 1),
            ).as_("pixel_value"),
        ],
        from_=TableExpression(dialect, "terrain_data"),
    )
    sql, params = value_query.to_sql()
    print("\n--- ST_Value query (pixel at 1,1) ---")
    print(f"SQL: {sql}")
    print(f"Params: {params}")
    result = backend.execute(sql, params, options=opts)
    print(f"Pixel value: {result.data}")

else:
    print("\nSkipping execution - postgis not available on this server")
    print("To enable postgis, install PostGIS and run: CREATE EXTENSION postgis;")

# ============================================================
# SECTION: Teardown (necessary for execution, reference only)
# ============================================================
drop_expr = DropTableExpression(dialect=dialect, table_name="terrain_data", if_exists=True)
sql, params = drop_expr.to_sql()
backend.execute(sql, params)
backend.disconnect()
