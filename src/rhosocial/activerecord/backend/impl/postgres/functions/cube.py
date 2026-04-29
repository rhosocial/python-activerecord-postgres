# src/rhosocial/activerecord/backend/impl/postgres/functions/cube.py
"""
PostgreSQL cube Extension Functions and Operators.

This module provides SQL expression generators for PostgreSQL cube
extension functions and operators. All functions return Expression objects
(FunctionCall, BinaryExpression, BinaryArithmeticExpression) that integrate
with the expression-dialect architecture.

The cube extension provides a data type representing multidimensional cubes,
which can be used for representing geometric points, boxes, and performing
spatial queries such as containment, overlap, and distance calculations.

PostgreSQL Documentation: https://www.postgresql.org/docs/current/cube.html

The cube extension must be installed:
    CREATE EXTENSION IF NOT EXISTS cube;

Supported operators:
- union : Cube union (smallest cube including both)
- inter : Cube intersection
- @>   : Cube contains
- <->  : Distance between cubes

Supported functions:
- cube: Construct a cube from coordinates
- cube: Construct a cube from dimension and coordinate
- cube_size: Return the size of a cube

All functions follow the expression-dialect separation architecture:
- First parameter is always the dialect instance
- They return Expression objects (FunctionCall, BinaryExpression, etc.)
- They do not concatenate SQL strings directly
"""

from typing import List, Union, TYPE_CHECKING

from rhosocial.activerecord.backend.expression import bases, core
from rhosocial.activerecord.backend.expression.operators import (
    BinaryExpression,
    BinaryArithmeticExpression,
)

if TYPE_CHECKING:
    from rhosocial.activerecord.backend.dialect import SQLDialectBase


def _convert_to_expression(
    dialect: "SQLDialectBase",
    expr: Union[str, "bases.BaseExpression"],
) -> "bases.BaseExpression":
    """Convert an input value to an appropriate BaseExpression.

    Supports strings and existing BaseExpression objects.

    For string inputs, generates a literal expression. For
    BaseExpression inputs, returns them unchanged.

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


# ============== Cube Construction ==============

def cube_literal(
    dialect: "SQLDialectBase",
    coordinates: Union[List[float], str, "bases.BaseExpression"],
) -> core.FunctionCall:
    """Generate SQL expression for the cube constructor from coordinates.

    Constructs a cube value from a list of coordinates. This generates
    the SQL expression cube(ARRAY[...]) which creates a point cube
    at the specified coordinates.

    Args:
        dialect: The SQL dialect instance
        coordinates: A list of float coordinates (e.g., [1.0, 2.0, 3.0]),
            a string representation, or an existing expression

    Returns:
        FunctionCall for cube(ARRAY[coordinates])

    Example:
        >>> from rhosocial.activerecord.backend.impl.postgres import PostgresDialect
        >>> dialect = PostgresDialect()
        >>> cube_literal(dialect, [1.0, 2.0, 3.0])
        # Generates SQL: cube(ARRAY[1.0, 2.0, 3.0])
        >>> cube_literal(dialect, '(1,2,3),(4,5,6)')
        # Generates SQL: cube('(1,2,3),(4,5,6)')
    """
    if isinstance(coordinates, list):
        coords_str = ", ".join(str(c) for c in coordinates)
        array_literal = "ARRAY[{}]".format(coords_str)
        return core.FunctionCall(
            dialect, "cube",
            core.Literal(dialect, array_literal),
        )
    return core.FunctionCall(
        dialect, "cube",
        _convert_to_expression(dialect, coordinates),
    )


def cube_dimension(
    dialect: "SQLDialectBase",
    dim: Union[int, str, "bases.BaseExpression"],
    coord: Union[float, str, "bases.BaseExpression"],
) -> core.FunctionCall:
    """Generate SQL expression for the cube constructor from dimension and coordinate.

    Constructs a cube representing a point in the specified number of
    dimensions, with all coordinates set to the given value.
    This generates the SQL expression cube(dim, coord).

    Args:
        dialect: The SQL dialect instance
        dim: Number of dimensions (positive integer)
        coord: Coordinate value to use for all dimensions

    Returns:
        FunctionCall for cube(dim, coord)

    Example:
        >>> cube_dimension(dialect, 3, 1.0)
        # Generates SQL: cube(3, 1.0)
        # Creates a point (1, 1, 1) in 3 dimensions
    """
    return core.FunctionCall(
        dialect, "cube",
        _convert_to_expression(dialect, dim),
        _convert_to_expression(dialect, coord),
    )


# ============== Cube Functions ==============

def cube_size(
    dialect: "SQLDialectBase",
    cube_expr: Union[str, "bases.BaseExpression"],
) -> core.FunctionCall:
    """Generate SQL expression for the cube_size function.

    Returns the total size (volume) of the cube. For a point, this
    returns 0. For a box, it returns the product of the lengths of
    all dimensions.

    Args:
        dialect: The SQL dialect instance
        cube_expr: The cube column or expression

    Returns:
        FunctionCall for cube_size(cube_expr)

    Example:
        >>> cube_size(dialect, 'location')
        # Generates SQL: cube_size(location)
    """
    return core.FunctionCall(
        dialect, "cube_size",
        _convert_to_expression(dialect, cube_expr),
    )


# ============== Cube Operators ==============

def cube_union(
    dialect: "SQLDialectBase",
    cube1: Union[str, "bases.BaseExpression"],
    cube2: Union[str, "bases.BaseExpression"],
) -> BinaryExpression:
    """Generate SQL expression for the cube union operator.

    Produces the smallest cube that includes both input cubes.
    This uses the PostgreSQL "union" operator for cubes.

    Args:
        dialect: The SQL dialect instance
        cube1: The first cube expression
        cube2: The second cube expression

    Returns:
        BinaryExpression for cube1 union cube2

    Example:
        >>> cube_union(dialect, 'location1', 'location2')
        # Generates SQL: location1 union location2
    """
    return BinaryExpression(
        dialect, "union",
        _convert_to_expression(dialect, cube1),
        _convert_to_expression(dialect, cube2),
    )


def cube_inter(
    dialect: "SQLDialectBase",
    cube1: Union[str, "bases.BaseExpression"],
    cube2: Union[str, "bases.BaseExpression"],
) -> BinaryExpression:
    """Generate SQL expression for the cube intersection operator.

    Produces the intersection of two cubes. This uses the PostgreSQL
    "inter" operator for cubes.

    Args:
        dialect: The SQL dialect instance
        cube1: The first cube expression
        cube2: The second cube expression

    Returns:
        BinaryExpression for cube1 inter cube2

    Example:
        >>> cube_inter(dialect, 'location1', cube_literal(dialect, [1, 2, 3]))
        # Generates SQL: location1 inter cube(ARRAY[1, 2, 3])
    """
    return BinaryExpression(
        dialect, "inter",
        _convert_to_expression(dialect, cube1),
        _convert_to_expression(dialect, cube2),
    )


def cube_contains(
    dialect: "SQLDialectBase",
    cube_expr: Union[str, "bases.BaseExpression"],
    target: Union[str, "bases.BaseExpression"],
) -> BinaryExpression:
    """Generate SQL expression for the cube contains operator (@>).

    Checks whether the left cube contains the right cube. A cube contains
    another if every point of the right cube is also a point of the left cube.
    This operator can use a GiST index for efficient lookups.

    Args:
        dialect: The SQL dialect instance
        cube_expr: The containing cube expression (left side)
        target: The contained cube or point expression (right side)

    Returns:
        BinaryExpression for cube_expr @> target

    Example:
        >>> cube_contains(dialect, 'bounding_box', cube_literal(dialect, [1, 2, 3]))
        # Generates SQL: bounding_box @> cube(ARRAY[1, 2, 3])
    """
    return BinaryExpression(
        dialect, "@>",
        _convert_to_expression(dialect, cube_expr),
        _convert_to_expression(dialect, target),
    )


def cube_distance(
    dialect: "SQLDialectBase",
    cube_expr: Union[str, "bases.BaseExpression"],
    target: Union[str, "bases.BaseExpression"],
) -> BinaryArithmeticExpression:
    """Generate SQL expression for the cube distance operator (<->).

    Computes the distance between two cubes. For points, this is the
    Euclidean distance. For boxes, it is the minimum distance between
    any two points in the two cubes (0 if they overlap).
    This operator can use a GiST index for efficient nearest-neighbor queries.

    Args:
        dialect: The SQL dialect instance
        cube_expr: The first cube expression
        target: The second cube expression to measure distance to

    Returns:
        BinaryArithmeticExpression for cube_expr <-> target

    Example:
        >>> cube_distance(dialect, 'location', cube_literal(dialect, [1, 2, 3]))
        # Generates SQL: location <-> cube(ARRAY[1, 2, 3])
    """
    return BinaryArithmeticExpression(
        dialect, "<->",
        _convert_to_expression(dialect, cube_expr),
        _convert_to_expression(dialect, target),
    )


__all__ = [
    # Cube construction
    "cube_literal",
    "cube_dimension",
    # Cube functions
    "cube_size",
    # Cube operators
    "cube_union",
    "cube_inter",
    "cube_contains",
    "cube_distance",
]
