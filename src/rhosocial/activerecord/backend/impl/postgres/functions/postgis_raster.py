# src/rhosocial/activerecord/backend/impl/postgres/functions/postgis_raster.py
"""
PostgreSQL PostGIS Raster Extension Functions.

This module provides SQL expression generators for PostGIS raster
extension functions. All functions return Expression objects (FunctionCall)
that integrate with the expression-dialect architecture.

The PostGIS raster extension provides support for storing, indexing, and
querying raster (gridded) geospatial data. Raster data includes satellite
imagery, digital elevation models, and other gridded spatial datasets.

PostGIS Documentation: https://postgis.net/docs/RT_reference.html

The PostGIS extension must be installed:
    CREATE EXTENSION IF NOT EXISTS postgis;

For raster support, PostGIS raster is included by default in PostGIS 2.0+.

Supported functions:
- st_rast_from_hexwkb: Construct a raster from HexWKB data with SRID
- st_value: Return the value of a raster pixel at a specified location
- st_summary: Return a text summary of the raster

Note:
    PostGIS raster functions require PostGIS with raster support enabled.
    Some operations may also require the postgis_raster extension to be
    explicitly installed on certain PostgreSQL versions.

All functions follow the expression-dialect separation architecture:
- First parameter is always the dialect instance
- They return Expression objects (FunctionCall, etc.)
- They do not concatenate SQL strings directly
"""

from typing import TYPE_CHECKING

from rhosocial.activerecord.backend.expression import bases, core

if TYPE_CHECKING:
    from rhosocial.activerecord.backend.dialect import SQLDialectBase


# ============== PostGIS Raster Functions ==============

def st_rast_from_hexwkb(
    dialect: "SQLDialectBase",
    raster_data: str,
    srid: int = 4326,
) -> core.FunctionCall:
    """Generate SQL expression for constructing a raster from HexWKB data with SRID.

    Constructs a raster value from hexadecimal Well-Known Binary (HexWKB) data
    and sets its spatial reference system identifier (SRID). This generates
    ST_SetSRID(ST_RastFromHexWKB(raster_data), srid).

    This is the standard way to create a raster literal with a known coordinate
    system in PostGIS.

    Args:
        dialect: The SQL dialect instance
        raster_data: The HexWKB representation of raster data
        srid: Spatial reference system ID (default: 4326, WGS 84)

    Returns:
        FunctionCall for ST_SetSRID(ST_RastFromHexWKB(raster_data), srid)

    Example:
        >>> from rhosocial.activerecord.backend.impl.postgres.dialect import PostgresDialect
        >>> dialect = PostgresDialect()
        >>> st_rast_from_hexwkb(dialect, '0100000...', srid=4326)
        # Generates: ST_SetSRID(ST_RastFromHexWKB('0100000...'), 4326)
        >>> st_rast_from_hexwkb(dialect, raster_data)
        # Generates: ST_SetSRID(ST_RastFromHexWKB(raster_data), 4326)
    """
    inner = core.FunctionCall(
        dialect, "ST_RastFromHexWKB",
        raster_data if isinstance(raster_data, bases.BaseExpression) else core.Literal(dialect, raster_data),
    )
    return core.FunctionCall(
        dialect, "ST_SetSRID",
        inner,
        srid if isinstance(srid, bases.BaseExpression) else core.Literal(dialect, srid),
    )


def st_value(
    dialect: "SQLDialectBase",
    raster_expr: bases.BaseExpression,
    x: int,
    y: int,
) -> core.FunctionCall:
    """Generate SQL expression for the ST_Value function.

    Returns the value of a raster pixel at the specified column (x) and
    row (y) position. The pixel coordinates are 1-indexed. Returns NULL
    if the pixel has no data (is a nodata value).

    Args:
        dialect: The SQL dialect instance
        raster_expr: The raster column or expression
        x: The column number of the pixel (1-indexed)
        y: The row number of the pixel (1-indexed)

    Returns:
        FunctionCall for ST_Value(raster_expr, x, y)

    Example:
        >>> st_value(dialect, 'elevation_raster', 10, 20)
        # Generates: ST_Value(elevation_raster, 10, 20)
        >>> st_value(dialect, raster_column, 1, 1)
        # Generates: ST_Value(raster_column, 1, 1)
    """
    return core.FunctionCall(
        dialect, "ST_Value",
        raster_expr if isinstance(raster_expr, bases.BaseExpression) else core.Literal(dialect, raster_expr),
        x if isinstance(x, bases.BaseExpression) else core.Literal(dialect, x),
        y if isinstance(y, bases.BaseExpression) else core.Literal(dialect, y),
    )


def st_summary(
    dialect: "SQLDialectBase",
    raster_expr: bases.BaseExpression,
) -> core.FunctionCall:
    """Generate SQL expression for the ST_Summary function.

    Returns a text summary of the raster, including dimensions, pixel type,
    number of bands, and other metadata. This is useful for debugging and
    understanding the structure of raster data.

    Args:
        dialect: The SQL dialect instance
        raster_expr: The raster column or expression

    Returns:
        FunctionCall for ST_Summary(raster_expr)

    Example:
        >>> st_summary(dialect, 'elevation_raster')
        # Generates: ST_Summary(elevation_raster)
        >>> st_summary(dialect, raster_column)
        # Generates: ST_Summary(raster_column)
    """
    return core.FunctionCall(
        dialect, "ST_Summary",
        raster_expr if isinstance(raster_expr, bases.BaseExpression) else core.Literal(dialect, raster_expr),
    )


__all__ = [
    # PostGIS raster functions
    "st_rast_from_hexwkb",
    "st_value",
    "st_summary",
]
