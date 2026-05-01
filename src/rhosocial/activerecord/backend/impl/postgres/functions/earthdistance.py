# src/rhosocial/activerecord/backend/impl/postgres/functions/earthdistance.py
"""
PostgreSQL earthdistance Extension Functions and Operators.

This module provides SQL expression generators for PostgreSQL earthdistance
extension functions and operators. All functions return Expression objects
(FunctionCall, BinaryExpression) that integrate with the expression-dialect
architecture.

The earthdistance extension provides earth distance calculation functions
for computing great-circle distances between geographic points.

PostgreSQL Documentation: https://www.postgresql.org/docs/current/earthdistance.html

The earthdistance extension must be installed:
    CREATE EXTENSION IF NOT EXISTS earthdistance;

The cube extension is also required:
    CREATE EXTENSION IF NOT EXISTS cube;

Supported functions:
- ll_to_earth: Convert latitude/longitude to earth surface point
- earth_distance: Calculate great-circle distance between two points
- earth_box: Calculate bounding box for a point and radius

Supported operators:
- <-> : Earth distance operator (earthdistance_operator)
- <@ : Point inside circle containment check (point_inside_circle)
"""

from typing import Union, TYPE_CHECKING

from rhosocial.activerecord.backend.expression import bases, core
from rhosocial.activerecord.backend.expression.operators import BinaryExpression

if TYPE_CHECKING:
    from rhosocial.activerecord.backend.dialect import SQLDialectBase


def _convert_to_expression(
    dialect: "SQLDialectBase",
    expr: Union[str, "bases.BaseExpression", float],
) -> "bases.BaseExpression":
    """Convert an input value to an appropriate BaseExpression.

    Supports strings, floats, and existing BaseExpression objects.

    Args:
        dialect: The SQL dialect instance
        expr: Value to convert

    Returns:
        BaseExpression representing the value
    """
    if isinstance(expr, bases.BaseExpression):
        return expr
    elif isinstance(expr, str):
        return core.Literal(dialect, expr)
    else:
        return core.Literal(dialect, expr)


# ============== Functions ==============

def ll_to_earth(
    dialect: "SQLDialectBase",
    latitude: Union[float, str, "bases.BaseExpression"],
    longitude: Union[float, str, "bases.BaseExpression"],
) -> core.FunctionCall:
    """Convert latitude and longitude to a point on the earth's surface.

    Returns a point (cube) representing the location on the earth's surface
    identified by the given latitude and longitude.

    Args:
        dialect: The SQL dialect instance
        latitude: Latitude in degrees (float, string, or expression)
        longitude: Longitude in degrees (float, string, or expression)

    Returns:
        FunctionCall for ll_to_earth(latitude, longitude)

    Example:
        >>> from rhosocial.activerecord.backend.impl.postgres import PostgresDialect
        >>> dialect = PostgresDialect()
        >>> ll_to_earth(dialect, 40.7128, -74.0060)
        # Generates: ll_to_earth(40.7128, -74.006)
    """
    return core.FunctionCall(
        dialect, "ll_to_earth",
        _convert_to_expression(dialect, latitude),
        _convert_to_expression(dialect, longitude),
    )


def earth_distance(
    dialect: "SQLDialectBase",
    point1: Union[str, "bases.BaseExpression"],
    point2: Union[str, "bases.BaseExpression"],
) -> core.FunctionCall:
    """Calculate the great-circle distance between two points on the earth's surface.

    Returns the distance in meters between two points on the earth's surface,
    represented as cube points (typically from ll_to_earth).

    Args:
        dialect: The SQL dialect instance
        point1: First earth point (cube), typically from ll_to_earth
        point2: Second earth point (cube), typically from ll_to_earth

    Returns:
        FunctionCall for earth_distance(point1, point2)

    Example:
        >>> earth_distance(
        ...     dialect,
        ...     ll_to_earth(dialect, 40.7128, -74.0060),
        ...     ll_to_earth(dialect, 34.0522, -118.2437),
        ... )
        # Generates: earth_distance(ll_to_earth(40.7128, -74.006), ll_to_earth(34.0522, -118.2437))
    """
    return core.FunctionCall(
        dialect, "earth_distance",
        _convert_to_expression(dialect, point1),
        _convert_to_expression(dialect, point2),
    )


def earth_box(
    dialect: "SQLDialectBase",
    center: Union[str, "bases.BaseExpression"],
    radius: Union[float, str, "bases.BaseExpression"],
) -> core.FunctionCall:
    """Calculate a bounding box for a point and radius on the earth's surface.

    Returns a box (cube) suitable for an indexed search using the cube @>
    operator. The box is a cube large enough to contain all points within
    the given great-circle distance (in meters) from the center point.

    Args:
        dialect: The SQL dialect instance
        center: Center earth point (cube), typically from ll_to_earth
        radius: Search radius in meters

    Returns:
        FunctionCall for earth_box(center, radius)

    Example:
        >>> earth_box(dialect, ll_to_earth(dialect, 40.7128, -74.0060), 5000)
        # Generates: earth_box(ll_to_earth(40.7128, -74.006), 5000)
    """
    return core.FunctionCall(
        dialect, "earth_box",
        _convert_to_expression(dialect, center),
        _convert_to_expression(dialect, radius),
    )


# ============== Operators ==============

def earthdistance_operator(
    dialect: "SQLDialectBase",
    column: Union[str, "bases.BaseExpression"],
    latitude: Union[float, str, "bases.BaseExpression"],
    longitude: Union[float, str, "bases.BaseExpression"],
    operator: str = "<->",
) -> BinaryExpression:
    """Generate SQL expression for earth distance operator.

    Uses the <-> operator to calculate the distance between an earth point
    column and a given latitude/longitude. The result is in meters.

    Args:
        dialect: The SQL dialect instance
        column: Column or expression containing the earth point (cube)
        latitude: Latitude in degrees
        longitude: Longitude in degrees
        operator: The distance operator to use (default: '<->')

    Returns:
        BinaryExpression for column <-> ll_to_earth(latitude, longitude)

    Example:
        >>> earthdistance_operator(dialect, 'location', 40.7128, -74.0060)
        # Generates: location <-> ll_to_earth(40.7128, -74.006)

        >>> earthdistance_operator(dialect, 'location', 40.7128, -74.0060, operator='<->')
        # Generates: location <-> ll_to_earth(40.7128, -74.006)
    """
    return BinaryExpression(
        dialect, operator,
        _convert_to_expression(dialect, column),
        ll_to_earth(dialect, latitude, longitude),
    )


def point_inside_circle(
    dialect: "SQLDialectBase",
    point: Union[str, "bases.BaseExpression"],
    center_lat: Union[float, str, "bases.BaseExpression"],
    center_lon: Union[float, str, "bases.BaseExpression"],
    radius: Union[float, str, "bases.BaseExpression"],
) -> BinaryExpression:
    """Generate SQL expression for checking if a point is inside a circle.

    Uses the <@ containment operator with earth_box to check if a point
    falls within a specified radius of a center location. This is typically
    used with an earthdistance index for efficient proximity queries.

    Args:
        dialect: The SQL dialect instance
        point: Column or expression containing the earth point (cube)
        center_lat: Center latitude in degrees
        center_lon: Center longitude in degrees
        radius: Search radius in meters

    Returns:
        BinaryExpression for point <@ earth_box(ll_to_earth(center_lat, center_lon), radius)

    Example:
        >>> point_inside_circle(dialect, 'location', 40.7128, -74.0060, 5000)
        # Generates: location <@ earth_box(ll_to_earth(40.7128, -74.006), 5000)
    """
    return BinaryExpression(
        dialect, "<@",
        _convert_to_expression(dialect, point),
        earth_box(dialect, ll_to_earth(dialect, center_lat, center_lon), radius),
    )


__all__ = [
    # Functions
    "ll_to_earth",
    "earth_distance",
    "earth_box",
    # Operators
    "earthdistance_operator",
    "point_inside_circle",
]
