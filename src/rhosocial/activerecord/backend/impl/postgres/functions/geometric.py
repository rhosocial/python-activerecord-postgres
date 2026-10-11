# src/rhosocial/activerecord/backend/impl/postgres/functions/geometric.py
"""
PostgreSQL Geometric Functions and Operators.

This module provides SQL expression generators for PostgreSQL geometric
functions and operators. All functions return Expression objects
that integrate with the Expression/Dialect architecture.

PostgreSQL Documentation: https://www.postgresql.org/docs/current/functions-geometry.html

Supported operators:
- <->  : Distance between geometric objects
- @>   : Contains
- <@   : Contained by
- &&   : Overlaps
- <<   : Strictly left of
- >>   : Strictly right of
- &<   : Does not extend to the right of
- &>   : Does not extend to the left of

Supported functions:
- area(geom)     : Area of a geometric object
- center(geom)   : Center point of a geometric object
- length(geom)   : Length of a path or line segment
- width(geom)    : Horizontal size of a box
- height(geom)   : Vertical size of a box
- npoints(geom)  : Number of points in a path or polygon
"""

from typing import TYPE_CHECKING

from rhosocial.activerecord.backend.expression import bases, core
from rhosocial.activerecord.backend.expression.operators import BinaryExpression

if TYPE_CHECKING:
    from rhosocial.activerecord.backend.dialect import SQLDialectBase


# ============== Operators ==============

def geometry_distance(
    dialect: "SQLDialectBase",
    geom1: bases.BaseExpression,
    geom2: bases.BaseExpression,
) -> BinaryExpression:
    """Generate PostgreSQL distance operator expression.

    Computes the distance between two geometric objects.

    Args:
        dialect: The SQL dialect instance
        geom1: First geometric object or expression
        geom2: Second geometric object or expression

    Returns:
        BinaryExpression for geom1 <-> geom2
    """
    return BinaryExpression(
        dialect, "<->",
        geom1 if isinstance(geom1, bases.BaseExpression) else core.Literal(dialect, geom1),
        geom2 if isinstance(geom2, bases.BaseExpression) else core.Literal(dialect, geom2),
    )


def geometry_contains(
    dialect: "SQLDialectBase",
    geom1: bases.BaseExpression,
    geom2: bases.BaseExpression,
) -> BinaryExpression:
    """Generate PostgreSQL contains operator expression.

    Tests whether geom1 contains geom2.

    Args:
        dialect: The SQL dialect instance
        geom1: Container geometric object
        geom2: Contained geometric object

    Returns:
        BinaryExpression for geom1 @> geom2
    """
    return BinaryExpression(
        dialect, "@>",
        geom1 if isinstance(geom1, bases.BaseExpression) else core.Literal(dialect, geom1),
        geom2 if isinstance(geom2, bases.BaseExpression) else core.Literal(dialect, geom2),
    )


def geometry_contained_by(
    dialect: "SQLDialectBase",
    geom1: bases.BaseExpression,
    geom2: bases.BaseExpression,
) -> BinaryExpression:
    """Generate PostgreSQL contained by operator expression.

    Tests whether geom1 is contained by geom2.

    Args:
        dialect: The SQL dialect instance
        geom1: Contained geometric object
        geom2: Container geometric object

    Returns:
        BinaryExpression for geom1 <@ geom2
    """
    return BinaryExpression(
        dialect, "<@",
        geom1 if isinstance(geom1, bases.BaseExpression) else core.Literal(dialect, geom1),
        geom2 if isinstance(geom2, bases.BaseExpression) else core.Literal(dialect, geom2),
    )


def geometry_overlaps(
    dialect: "SQLDialectBase",
    geom1: bases.BaseExpression,
    geom2: bases.BaseExpression,
) -> BinaryExpression:
    """Generate PostgreSQL overlaps operator expression.

    Tests whether two geometric objects overlap.

    Args:
        dialect: The SQL dialect instance
        geom1: First geometric object
        geom2: Second geometric object

    Returns:
        BinaryExpression for geom1 && geom2
    """
    return BinaryExpression(
        dialect, "&&",
        geom1 if isinstance(geom1, bases.BaseExpression) else core.Literal(dialect, geom1),
        geom2 if isinstance(geom2, bases.BaseExpression) else core.Literal(dialect, geom2),
    )


def geometry_strictly_left(
    dialect: "SQLDialectBase",
    geom1: bases.BaseExpression,
    geom2: bases.BaseExpression,
) -> BinaryExpression:
    """Generate PostgreSQL strictly left operator expression.

    Tests whether geom1 is strictly to the left of geom2.

    Args:
        dialect: The SQL dialect instance
        geom1: First geometric object
        geom2: Second geometric object

    Returns:
        BinaryExpression for geom1 << geom2
    """
    return BinaryExpression(
        dialect, "<<",
        geom1 if isinstance(geom1, bases.BaseExpression) else core.Literal(dialect, geom1),
        geom2 if isinstance(geom2, bases.BaseExpression) else core.Literal(dialect, geom2),
    )


def geometry_strictly_right(
    dialect: "SQLDialectBase",
    geom1: bases.BaseExpression,
    geom2: bases.BaseExpression,
) -> BinaryExpression:
    """Generate PostgreSQL strictly right operator expression.

    Tests whether geom1 is strictly to the right of geom2.

    Args:
        dialect: The SQL dialect instance
        geom1: First geometric object
        geom2: Second geometric object

    Returns:
        BinaryExpression for geom1 >> geom2
    """
    return BinaryExpression(
        dialect, ">>",
        geom1 if isinstance(geom1, bases.BaseExpression) else core.Literal(dialect, geom1),
        geom2 if isinstance(geom2, bases.BaseExpression) else core.Literal(dialect, geom2),
    )


def geometry_not_extend_right(
    dialect: "SQLDialectBase",
    geom1: bases.BaseExpression,
    geom2: bases.BaseExpression,
) -> BinaryExpression:
    """Generate PostgreSQL does not extend to the right operator.

    Tests whether geom1 does not extend to the right of geom2.

    Args:
        dialect: The SQL dialect instance
        geom1: First geometric object
        geom2: Second geometric object

    Returns:
        BinaryExpression for geom1 &< geom2
    """
    return BinaryExpression(
        dialect, "&<",
        geom1 if isinstance(geom1, bases.BaseExpression) else core.Literal(dialect, geom1),
        geom2 if isinstance(geom2, bases.BaseExpression) else core.Literal(dialect, geom2),
    )


def geometry_not_extend_left(
    dialect: "SQLDialectBase",
    geom1: bases.BaseExpression,
    geom2: bases.BaseExpression,
) -> BinaryExpression:
    """Generate PostgreSQL does not extend to the left operator.

    Tests whether geom1 does not extend to the left of geom2.

    Args:
        dialect: The SQL dialect instance
        geom1: First geometric object
        geom2: Second geometric object

    Returns:
        BinaryExpression for geom1 &> geom2
    """
    return BinaryExpression(
        dialect, "&>",
        geom1 if isinstance(geom1, bases.BaseExpression) else core.Literal(dialect, geom1),
        geom2 if isinstance(geom2, bases.BaseExpression) else core.Literal(dialect, geom2),
    )


# ============== Functions ==============

def geometry_area(
    dialect: "SQLDialectBase",
    geom: bases.BaseExpression,
) -> core.FunctionCall:
    """Generate PostgreSQL area function expression.

    Computes the area of a geometric object.

    Args:
        dialect: The SQL dialect instance
        geom: Geometric object

    Returns:
        FunctionCall for area(geom)
    """
    geom_expr = geom if isinstance(geom, bases.BaseExpression) else core.Literal(dialect, geom)
    return core.FunctionCall(dialect, "area", geom_expr)


def geometry_center(
    dialect: "SQLDialectBase",
    geom: bases.BaseExpression,
) -> core.FunctionCall:
    """Generate PostgreSQL center function expression.

    Computes the center point of a geometric object.

    Args:
        dialect: The SQL dialect instance
        geom: Geometric object

    Returns:
        FunctionCall for center(geom)
    """
    geom_expr = geom if isinstance(geom, bases.BaseExpression) else core.Literal(dialect, geom)
    return core.FunctionCall(dialect, "center", geom_expr)


def geometry_length(
    dialect: "SQLDialectBase",
    geom: bases.BaseExpression,
) -> core.FunctionCall:
    """Generate PostgreSQL length function expression.

    Computes the length of a path or line segment.

    Args:
        dialect: The SQL dialect instance
        geom: Geometric object (path, line segment)

    Returns:
        FunctionCall for length(geom)
    """
    geom_expr = geom if isinstance(geom, bases.BaseExpression) else core.Literal(dialect, geom)
    return core.FunctionCall(dialect, "length", geom_expr)


def geometry_width(
    dialect: "SQLDialectBase",
    geom: bases.BaseExpression,
) -> core.FunctionCall:
    """Generate PostgreSQL width function expression.

    Computes the horizontal size of a box.

    Args:
        dialect: The SQL dialect instance
        geom: Geometric object (box)

    Returns:
        FunctionCall for width(geom)
    """
    geom_expr = geom if isinstance(geom, bases.BaseExpression) else core.Literal(dialect, geom)
    return core.FunctionCall(dialect, "width", geom_expr)


def geometry_height(
    dialect: "SQLDialectBase",
    geom: bases.BaseExpression,
) -> core.FunctionCall:
    """Generate PostgreSQL height function expression.

    Computes the vertical size of a box.

    Args:
        dialect: The SQL dialect instance
        geom: Geometric object (box)

    Returns:
        FunctionCall for height(geom)
    """
    geom_expr = geom if isinstance(geom, bases.BaseExpression) else core.Literal(dialect, geom)
    return core.FunctionCall(dialect, "height", geom_expr)


def geometry_npoints(
    dialect: "SQLDialectBase",
    geom: bases.BaseExpression,
) -> core.FunctionCall:
    """Generate PostgreSQL npoints function expression.

    Returns the number of points in a path or polygon.

    Args:
        dialect: The SQL dialect instance
        geom: Geometric object (path, polygon)

    Returns:
        FunctionCall for npoints(geom)
    """
    geom_expr = geom if isinstance(geom, bases.BaseExpression) else core.Literal(dialect, geom)
    return core.FunctionCall(dialect, "npoints", geom_expr)


__all__ = [
    "geometry_distance",
    "geometry_contains",
    "geometry_contained_by",
    "geometry_overlaps",
    "geometry_strictly_left",
    "geometry_strictly_right",
    "geometry_not_extend_right",
    "geometry_not_extend_left",
    "geometry_area",
    "geometry_center",
    "geometry_length",
    "geometry_width",
    "geometry_height",
    "geometry_npoints",
]
