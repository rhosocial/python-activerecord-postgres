# src/rhosocial/activerecord/backend/impl/postgres/functions/postgis.py
"""
PostgreSQL PostGIS function factories.

This module provides SQL expression generators for PostGIS spatial
functions and operators. All functions return Expression objects
that integrate with the Expression/Dialect architecture.

PostGIS Documentation: https://postgis.net/docs/

The PostGIS extension must be installed:
    CREATE EXTENSION IF NOT EXISTS postgis;
"""

from typing import Optional, Union, TYPE_CHECKING

from rhosocial.activerecord.backend.expression import bases, core
from rhosocial.activerecord.backend.impl.postgres.type_values.postgis import PostgresGeometry

if TYPE_CHECKING:
    from rhosocial.activerecord.backend.dialect import SQLDialectBase

def geometry_expr(
    dialect: "SQLDialectBase",
    value: Union[PostgresGeometry, str],
) -> "bases.BaseExpression":
    """Build the expression for a geometry given as domain data.

    A :class:`PostgresGeometry` carries a geometry type, a WKT body and an SRID,
    and which function constructs it from those depends on the first: geography
    goes to ST_GeogFromText and everything else to ST_GeomFromText. That is
    worth doing and worth naming. It was not worth doing inside a converter that
    every function in the module called on the way past, where it also had to
    guess whether a string was WKT or a column name holding some.

    Args:
        dialect: The SQL dialect instance.
        value: A geometry object, or WKT text.

    Returns:
        A construction call for an object, or a literal for WKT.
    """
    if isinstance(value, PostgresGeometry):
        if value.geometry_type == "geography":
            return st_geog_from_text(dialect, core.Literal(dialect, value.wkt))
        return st_geom_from_text(
            dialect,
            core.Literal(dialect, value.wkt),
            core.Literal(dialect, value.srid) if value.srid is not None else None,
        )
    return core.Literal(dialect, value)


# === Geometry Construction ===

def st_make_point(
    dialect: "SQLDialectBase",
    x: "bases.BaseExpression",
    y: "bases.BaseExpression",
    z: Optional["bases.BaseExpression"] = None,
    m: Optional["bases.BaseExpression"] = None,
) -> core.FunctionCall:
    """ST_MakePoint(x, y [, z [, m]]) - Construct a point geometry.

    Args:
        dialect: The SQL dialect instance
        x: X coordinate (longitude)
        y: Y coordinate (latitude)
        z: Optional Z coordinate
        m: Optional M coordinate

    Returns:
        FunctionCall for ST_MakePoint
    """
    args = [x, y]
    if z is not None:
        args.append(z)
    if m is not None:
        args.append(m)
    return core.FunctionCall(dialect, "ST_MakePoint", *args)


def st_geom_from_text(
    dialect: "SQLDialectBase",
    wkt: "bases.BaseExpression",
    srid: Optional["bases.BaseExpression"] = None,
) -> core.FunctionCall:
    """ST_GeomFromText(wkt [, srid]) - Construct geometry from WKT.

    Args:
        dialect: The SQL dialect instance
        wkt: Well-Known Text string or PostgresGeometry
        srid: Optional SRID

    Returns:
        FunctionCall for ST_GeomFromText
    """
    args = [wkt]
    if srid is not None:
        args.append(srid)
    return core.FunctionCall(dialect, "ST_GeomFromText", *args)


def st_geog_from_text(
    dialect: "SQLDialectBase",
    wkt: "bases.BaseExpression",
) -> core.FunctionCall:
    """ST_GeogFromText(wkt) - Construct geography from WKT.

    Args:
        dialect: The SQL dialect instance
        wkt: Well-Known Text string or PostgresGeometry

    Returns:
        FunctionCall for ST_GeogFromText
    """
    return core.FunctionCall(dialect, "ST_GeogFromText", wkt)


def st_set_srid(
    dialect: "SQLDialectBase",
    geometry: "bases.BaseExpression",
    srid: "bases.BaseExpression",
) -> core.FunctionCall:
    """ST_SetSRID(geometry, srid) - Set the SRID of a geometry.

    Args:
        dialect: The SQL dialect instance
        geometry: Geometry expression
        srid: Spatial Reference System ID

    Returns:
        FunctionCall for ST_SetSRID
    """
    return core.FunctionCall(
        dialect, "ST_SetSRID",
        geometry,
        srid,
    )


def st_transform(
    dialect: "SQLDialectBase",
    geometry: "bases.BaseExpression",
    target_srid: "bases.BaseExpression",
) -> core.FunctionCall:
    """ST_Transform(geometry, target_srid) - Transform geometry to a different SRID.

    Args:
        dialect: The SQL dialect instance
        geometry: Geometry expression
        target_srid: Target SRID

    Returns:
        FunctionCall for ST_Transform
    """
    return core.FunctionCall(
        dialect, "ST_Transform",
        geometry,
        target_srid,
    )


# === Spatial Predicates ===

def st_contains(
    dialect: "SQLDialectBase",
    outer: "bases.BaseExpression",
    inner: "bases.BaseExpression",
) -> core.FunctionCall:
    """ST_Contains(outer, inner) - Test if outer geometry contains inner.

    Returns:
        FunctionCall for ST_Contains
    """
    return core.FunctionCall(
        dialect, "ST_Contains",
        outer,
        inner,
    )


def st_intersects(
    dialect: "SQLDialectBase",
    geom1: "bases.BaseExpression",
    geom2: "bases.BaseExpression",
) -> core.FunctionCall:
    """ST_Intersects(geom1, geom2) - Test if geometries intersect.

    Returns:
        FunctionCall for ST_Intersects
    """
    return core.FunctionCall(
        dialect, "ST_Intersects",
        geom1,
        geom2,
    )


def st_within(
    dialect: "SQLDialectBase",
    inner: "bases.BaseExpression",
    outer: "bases.BaseExpression",
) -> core.FunctionCall:
    """ST_Within(inner, outer) - Test if inner geometry is within outer.

    Returns:
        FunctionCall for ST_Within
    """
    return core.FunctionCall(
        dialect, "ST_Within",
        inner,
        outer,
    )


def st_dwithin(
    dialect: "SQLDialectBase",
    geom1: "bases.BaseExpression",
    geom2: "bases.BaseExpression",
    distance: "bases.BaseExpression",
    use_spheroid: bool = False,
) -> core.FunctionCall:
    """ST_DWithin(geom1, geom2, distance [, use_spheroid]) - Test proximity.

    Args:
        dialect: The SQL dialect instance
        geom1: First geometry
        geom2: Second geometry
        distance: Distance threshold
        use_spheroid: Use spheroid for geography type

    Returns:
        FunctionCall for ST_DWithin
    """
    args = [
        geom1,
        geom2,
        distance,
    ]
    if use_spheroid:
        args.append(core.Literal(dialect, True))
    return core.FunctionCall(dialect, "ST_DWithin", *args)


def st_crosses(
    dialect: "SQLDialectBase",
    geom1: "bases.BaseExpression",
    geom2: "bases.BaseExpression",
) -> core.FunctionCall:
    """ST_Crosses(geom1, geom2) - Test if geometries cross."""
    return core.FunctionCall(
        dialect, "ST_Crosses",
        geom1,
        geom2,
    )


def st_touches(
    dialect: "SQLDialectBase",
    geom1: "bases.BaseExpression",
    geom2: "bases.BaseExpression",
) -> core.FunctionCall:
    """ST_Touches(geom1, geom2) - Test if geometries touch."""
    return core.FunctionCall(
        dialect, "ST_Touches",
        geom1,
        geom2,
    )


def st_overlaps(
    dialect: "SQLDialectBase",
    geom1: "bases.BaseExpression",
    geom2: "bases.BaseExpression",
) -> core.FunctionCall:
    """ST_Overlaps(geom1, geom2) - Test if geometries overlap."""
    return core.FunctionCall(
        dialect, "ST_Overlaps",
        geom1,
        geom2,
    )


# === Spatial Measurements ===

def st_distance(
    dialect: "SQLDialectBase",
    geom1: "bases.BaseExpression",
    geom2: "bases.BaseExpression",
    use_spheroid: bool = False,
) -> core.FunctionCall:
    """ST_Distance(geom1, geom2 [, use_spheroid]) - Calculate distance.

    Args:
        dialect: The SQL dialect instance
        geom1: First geometry
        geom2: Second geometry
        use_spheroid: Use spheroid for geography type

    Returns:
        FunctionCall for ST_Distance
    """
    args = [
        geom1,
        geom2,
    ]
    if use_spheroid:
        args.append(core.Literal(dialect, True))
    return core.FunctionCall(dialect, "ST_Distance", *args)


def st_area(
    dialect: "SQLDialectBase",
    geom: "bases.BaseExpression",
    use_spheroid: bool = False,
) -> core.FunctionCall:
    """ST_Area(geom [, use_spheroid]) - Calculate area.

    Args:
        dialect: The SQL dialect instance
        geom: Geometry or geography
        use_spheroid: Use spheroid for geography type

    Returns:
        FunctionCall for ST_Area
    """
    args = [geom]
    if use_spheroid:
        args.append(core.Literal(dialect, True))
    return core.FunctionCall(dialect, "ST_Area", *args)


def st_length(
    dialect: "SQLDialectBase",
    geom: "bases.BaseExpression",
    use_spheroid: bool = False,
) -> core.FunctionCall:
    """ST_Length(geom [, use_spheroid]) - Calculate length.

    Returns:
        FunctionCall for ST_Length
    """
    args = [geom]
    if use_spheroid:
        args.append(core.Literal(dialect, True))
    return core.FunctionCall(dialect, "ST_Length", *args)


# === Spatial Output ===

def st_as_geojson(
    dialect: "SQLDialectBase",
    geom: "bases.BaseExpression",
) -> core.FunctionCall:
    """ST_AsGeoJSON(geom) - Convert geometry to GeoJSON.

    Returns:
        FunctionCall for ST_AsGeoJSON
    """
    return core.FunctionCall(dialect, "ST_AsGeoJSON", geom)


def st_as_text(
    dialect: "SQLDialectBase",
    geom: "bases.BaseExpression",
) -> core.FunctionCall:
    """ST_AsText(geom) - Convert geometry to WKT.

    Returns:
        FunctionCall for ST_AsText
    """
    return core.FunctionCall(dialect, "ST_AsText", geom)


# === Spatial Operations ===

def st_buffer(
    dialect: "SQLDialectBase",
    geom: "bases.BaseExpression",
    radius: "bases.BaseExpression",
) -> core.FunctionCall:
    """ST_Buffer(geom, radius) - Create buffer around geometry.

    Returns:
        FunctionCall for ST_Buffer
    """
    return core.FunctionCall(
        dialect, "ST_Buffer",
        geom,
        radius,
    )


def st_envelope(
    dialect: "SQLDialectBase",
    geom: "bases.BaseExpression",
) -> core.FunctionCall:
    """ST_Envelope(geom) - Compute bounding box.

    Returns:
        FunctionCall for ST_Envelope
    """
    return core.FunctionCall(dialect, "ST_Envelope", geom)


def st_centroid(
    dialect: "SQLDialectBase",
    geom: "bases.BaseExpression",
) -> core.FunctionCall:
    """ST_Centroid(geom) - Compute geometric centroid.

    Returns:
        FunctionCall for ST_Centroid
    """
    return core.FunctionCall(dialect, "ST_Centroid", geom)


__all__ = [
    # Construction
    "st_make_point",
    "st_geom_from_text",
    "st_geog_from_text",
    "st_set_srid",
    "st_transform",
    # Predicates
    "st_contains",
    "st_intersects",
    "st_within",
    "st_dwithin",
    "st_crosses",
    "st_touches",
    "st_overlaps",
    # Measurements
    "st_distance",
    "st_area",
    "st_length",
    # Output
    "st_as_geojson",
    "st_as_text",
    # Operations
    "st_buffer",
    "st_envelope",
    "st_centroid",
]
