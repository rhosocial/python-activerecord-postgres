# src/rhosocial/activerecord/backend/impl/postgres/functions/geometric.py
"""
PostgreSQL Geometric Functions and Operators.

This module provides SQL expression generators for PostgreSQL geometric
functions and operators.

PostgreSQL Documentation: https://www.postgresql.org/docs/current/functions-geometry.html

All functions follow the expression-dialect separation architecture:
- First parameter is always the dialect instance
- They return SQL expression strings
"""
from typing import Any, TYPE_CHECKING

if TYPE_CHECKING:
    from rhosocial.activerecord.backend.dialect import SQLDialectBase


def geometry_distance(dialect: "SQLDialectBase", geom1: Any, geom2: Any) -> str:
    """Generate PostgreSQL distance operator expression.

    Args:
        dialect: The SQL dialect instance
        geom1: First geometric object or expression
        geom2: Second geometric object or expression

    Returns:
        SQL expression: geom1 <-> geom2
    """
    return f"{geom1} <-> {geom2}"


def geometry_contains(dialect: "SQLDialectBase", geom1: Any, geom2: Any) -> str:
    """Generate PostgreSQL contains operator expression.

    Args:
        dialect: The SQL dialect instance
        geom1: Container geometric object
        geom2: Contained geometric object

    Returns:
        SQL expression: geom1 @> geom2
    """
    return f"{geom1} @> {geom2}"


def geometry_contained_by(dialect: "SQLDialectBase", geom1: Any, geom2: Any) -> str:
    """Generate PostgreSQL contained by operator expression.

    Args:
        dialect: The SQL dialect instance
        geom1: Contained geometric object
        geom2: Container geometric object

    Returns:
        SQL expression: geom1 <@ geom2
    """
    return f"{geom1} <@ {geom2}"


def geometry_overlaps(dialect: "SQLDialectBase", geom1: Any, geom2: Any) -> str:
    """Generate PostgreSQL overlaps operator expression.

    Args:
        dialect: The SQL dialect instance
        geom1: First geometric object
        geom2: Second geometric object

    Returns:
        SQL expression: geom1 && geom2
    """
    return f"{geom1} && {geom2}"


def geometry_strictly_left(dialect: "SQLDialectBase", geom1: Any, geom2: Any) -> str:
    """Generate PostgreSQL strictly left operator expression.

    Args:
        dialect: The SQL dialect instance
        geom1: First geometric object
        geom2: Second geometric object

    Returns:
        SQL expression: geom1 << geom2
    """
    return f"{geom1} << {geom2}"


def geometry_strictly_right(dialect: "SQLDialectBase", geom1: Any, geom2: Any) -> str:
    """Generate PostgreSQL strictly right operator expression.

    Args:
        dialect: The SQL dialect instance
        geom1: First geometric object
        geom2: Second geometric object

    Returns:
        SQL expression: geom1 >> geom2
    """
    return f"{geom1} >> {geom2}"


def geometry_not_extend_right(dialect: "SQLDialectBase", geom1: Any, geom2: Any) -> str:
    """Generate PostgreSQL does not extend to the right operator.

    Args:
        dialect: The SQL dialect instance
        geom1: First geometric object
        geom2: Second geometric object

    Returns:
        SQL expression: geom1 &< geom2
    """
    return f"{geom1} &< {geom2}"


def geometry_not_extend_left(dialect: "SQLDialectBase", geom1: Any, geom2: Any) -> str:
    """Generate PostgreSQL does not extend to the left operator.

    Args:
        dialect: The SQL dialect instance
        geom1: First geometric object
        geom2: Second geometric object

    Returns:
        SQL expression: geom1 &> geom2
    """
    return f"{geom1} &> {geom2}"


def geometry_area(dialect: "SQLDialectBase", geom: Any) -> str:
    """Generate PostgreSQL area function expression.

    Args:
        dialect: The SQL dialect instance
        geom: Geometric object

    Returns:
        SQL expression: area(geom)
    """
    return f"area({geom})"


def geometry_center(dialect: "SQLDialectBase", geom: Any) -> str:
    """Generate PostgreSQL center function expression.

    Args:
        dialect: The SQL dialect instance
        geom: Geometric object

    Returns:
        SQL expression: center(geom)
    """
    return f"center({geom})"


def geometry_length(dialect: "SQLDialectBase", geom: Any) -> str:
    """Generate PostgreSQL length function expression.

    Args:
        dialect: The SQL dialect instance
        geom: Geometric object (path, line segment)

    Returns:
        SQL expression: length(geom)
    """
    return f"length({geom})"


def geometry_width(dialect: "SQLDialectBase", geom: Any) -> str:
    """Generate PostgreSQL width function expression.

    Args:
        dialect: The SQL dialect instance
        geom: Geometric object (box)

    Returns:
        SQL expression: width(geom)
    """
    return f"width({geom})"


def geometry_height(dialect: "SQLDialectBase", geom: Any) -> str:
    """Generate PostgreSQL height function expression.

    Args:
        dialect: The SQL dialect instance
        geom: Geometric object (box)

    Returns:
        SQL expression: height(geom)
    """
    return f"height({geom})"


def geometry_npoints(dialect: "SQLDialectBase", geom: Any) -> str:
    """Generate PostgreSQL npoints function expression.

    Args:
        dialect: The SQL dialect instance
        geom: Geometric object (path, polygon)

    Returns:
        SQL expression: npoints(geom)
    """
    return f"npoints({geom})"


__all__ = [
    'geometry_distance',
    'geometry_contains',
    'geometry_contained_by',
    'geometry_overlaps',
    'geometry_strictly_left',
    'geometry_strictly_right',
    'geometry_not_extend_right',
    'geometry_not_extend_left',
    'geometry_area',
    'geometry_center',
    'geometry_length',
    'geometry_width',
    'geometry_height',
    'geometry_npoints',
]
