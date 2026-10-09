# src/rhosocial/activerecord/backend/impl/postgres/functions/ltree.py
"""
PostgreSQL ltree Functions and Operators.

This module provides SQL expression generators for PostgreSQL ltree
extension functions and operators. All functions return Expression objects
(FunctionCall, BinaryExpression) that integrate with the expression-dialect
architecture.

The ltree extension provides a hierarchical label tree data type for
representing tree-like structures such as organizational charts,
category trees, and file system paths.

PostgreSQL Documentation: https://www.postgresql.org/docs/current/ltree.html

The ltree extension must be installed:
    CREATE EXTENSION IF NOT EXISTS ltree;

Supported types:
- ltree: Label path (e.g., 'Top.Science.Astronomy')
- lquery: Pattern for matching ltree values (e.g., '*.Astronomy.*')
- ltxtquery: Full-text query for ltree values (e.g., 'Science & Astronomy')

Supported operators:
- @>  : Left argument is ancestor of right (or equal)
- <@  : Left argument is descendant of right (or equal)
- ~   : ltree matches lquery
- @   : ltree matches ltxtquery
- ||  : ltree concatenation

Supported functions:
- nlevel(ltree): Number of labels in path
- subpath(ltree, start[, length]): Extract subpath
- lca(ltree, ltree, ...): Lowest common ancestor

All functions follow the expression-dialect separation architecture:
- First parameter is always the dialect instance
- They return Expression objects (FunctionCall, BinaryExpression, etc.)
- They do not concatenate SQL strings directly
"""

from typing import Optional, TYPE_CHECKING

from rhosocial.activerecord.backend.expression import bases, core
from rhosocial.activerecord.backend.expression.operators import BinaryExpression
from rhosocial.activerecord.backend.impl.postgres.expression.types import (
    PostgresLqueryType,
    PostgresLtxtqueryType,
)

if TYPE_CHECKING:
    from rhosocial.activerecord.backend.dialect import SQLDialectBase


# ============== Literal Constructors ==============

def ltree_literal(
    dialect: "SQLDialectBase",
    path: str,
) -> core.Literal:
    """Create an ltree literal expression.

    Wraps a path string as a Literal expression suitable for use in
    ltree operations.

    Args:
        dialect: The SQL dialect instance
        path: Label path string (e.g., 'Top.Science.Astronomy')

    Returns:
        Literal expression for the ltree path

    Example:
        >>> from rhosocial.activerecord.backend.impl.postgres.dialect import PostgresDialect
        >>> dialect = PostgresDialect()
        >>> ltree_literal(dialect, 'Top.Science.Astronomy')
    """
    return core.Literal(dialect, path)


def lquery_literal(
    dialect: "SQLDialectBase",
    pattern: str,
) -> "bases.BaseExpression":
    """Create an lquery literal expression with type cast.

    Wraps a pattern string as a Literal expression and casts it to
    the lquery type, producing SQL like: pattern::lquery

    Args:
        dialect: The SQL dialect instance
        pattern: lquery pattern string (e.g., '*.Astronomy.*' or 'Top.*{1,2}')

    Returns:
        Cast Literal expression for the lquery pattern

    Example:
        >>> lquery_literal(dialect, '*.Astronomy.*')
        # Generates SQL: '*.Astronomy.*'::lquery
    """
    return core.Literal(dialect, pattern).cast(PostgresLqueryType(dialect))


def ltxtquery_literal(
    dialect: "SQLDialectBase",
    query: str,
) -> "bases.BaseExpression":
    """Create an ltxtquery literal expression with type cast.

    Wraps a query string as a Literal expression and casts it to
    the ltxtquery type, producing SQL like: query::ltxtquery

    Args:
        dialect: The SQL dialect instance
        query: ltxtquery string (e.g., 'Science & Astronomy')

    Returns:
        Cast Literal expression for the ltxtquery

    Example:
        >>> ltxtquery_literal(dialect, 'Science & Astronomy')
        # Generates SQL: 'Science & Astronomy'::ltxtquery
    """
    return core.Literal(dialect, query).cast(PostgresLtxtqueryType(dialect))


# ============== ltree Operators ==============

def ltree_ancestor(
    dialect: "SQLDialectBase",
    column: bases.BaseExpression,
    path: bases.BaseExpression,
) -> BinaryExpression:
    """Generate SQL expression for ltree ancestor operator (@>).

    Checks if the left argument is an ancestor of the right argument
    (or equal to it).

    Args:
        dialect: The SQL dialect instance
        column: The ltree column or expression (left side)
        path: The descendant path to compare against (right side)

    Returns:
        BinaryExpression for column @> path

    Example:
        >>> ltree_ancestor(dialect, 'path_col', 'Top.Science')
        # Generates SQL: path_col @> 'Top.Science'
    """
    return BinaryExpression(
        dialect, "@>",
        column if isinstance(column, bases.BaseExpression) else core.Literal(dialect, column),
        path if isinstance(path, bases.BaseExpression) else core.Literal(dialect, path),
    )


def ltree_descendant(
    dialect: "SQLDialectBase",
    column: bases.BaseExpression,
    path: bases.BaseExpression,
) -> BinaryExpression:
    """Generate SQL expression for ltree descendant operator (<@).

    Checks if the left argument is a descendant of the right argument
    (or equal to it).

    Args:
        dialect: The SQL dialect instance
        column: The ltree column or expression (left side)
        path: The ancestor path to compare against (right side)

    Returns:
        BinaryExpression for column <@ path

    Example:
        >>> ltree_descendant(dialect, 'path_col', 'Top.Science')
        # Generates SQL: path_col <@ 'Top.Science'
    """
    return BinaryExpression(
        dialect, "<@",
        column if isinstance(column, bases.BaseExpression) else core.Literal(dialect, column),
        path if isinstance(path, bases.BaseExpression) else core.Literal(dialect, path),
    )


def ltree_matches(
    dialect: "SQLDialectBase",
    column: bases.BaseExpression,
    pattern: bases.BaseExpression,
) -> BinaryExpression:
    """Generate SQL expression for ltree lquery match operator (~).

    Checks if the ltree value matches the lquery pattern.

    Args:
        dialect: The SQL dialect instance
        column: The ltree column or expression (left side)
        pattern: The lquery pattern (right side)

    Returns:
        BinaryExpression for column ~ pattern

    Example:
        >>> ltree_matches(dialect, 'path_col', lquery_literal(dialect, '*.Astronomy.*'))
        # Generates SQL: path_col ~ '*.Astronomy.*'::lquery
    """
    return BinaryExpression(
        dialect, "~",
        column if isinstance(column, bases.BaseExpression) else core.Literal(dialect, column),
        pattern if isinstance(pattern, bases.BaseExpression) else core.Literal(dialect, pattern),
    )


def ltree_text_search(
    dialect: "SQLDialectBase",
    column: bases.BaseExpression,
    query: bases.BaseExpression,
) -> BinaryExpression:
    """Generate SQL expression for ltree ltxtquery match operator (@).

    Checks if the ltree value matches the ltxtquery full-text query.

    Args:
        dialect: The SQL dialect instance
        column: The ltree column or expression (left side)
        query: The ltxtquery expression (right side)

    Returns:
        BinaryExpression for column @ query

    Example:
        >>> ltree_text_search(dialect, 'path_col', ltxtquery_literal(dialect, 'Science & Astronomy'))
        # Generates SQL: path_col @ 'Science & Astronomy'::ltxtquery
    """
    return BinaryExpression(
        dialect, "@",
        column if isinstance(column, bases.BaseExpression) else core.Literal(dialect, column),
        query if isinstance(query, bases.BaseExpression) else core.Literal(dialect, query),
    )


def ltree_concat(
    dialect: "SQLDialectBase",
    left: bases.BaseExpression,
    right: bases.BaseExpression,
) -> BinaryExpression:
    """Generate SQL expression for ltree concatenation operator (||).

    Concatenates two ltree paths, producing a new ltree value.

    Args:
        dialect: The SQL dialect instance
        left: Left ltree expression
        right: Right ltree expression

    Returns:
        BinaryExpression for left || right

    Example:
        >>> ltree_concat(dialect, ltree_literal(dialect, 'Top.Science'), ltree_literal(dialect, 'Astronomy'))
        # Generates SQL: 'Top.Science' || 'Astronomy'
    """
    return BinaryExpression(
        dialect, "||",
        left if isinstance(left, bases.BaseExpression) else core.Literal(dialect, left),
        right if isinstance(right, bases.BaseExpression) else core.Literal(dialect, right),
    )


# ============== ltree Functions ==============

def ltree_nlevel(
    dialect: "SQLDialectBase",
    expr: bases.BaseExpression,
) -> core.FunctionCall:
    """Generate SQL expression for nlevel function.

    Returns the number of labels in the ltree path.

    Args:
        dialect: The SQL dialect instance
        expr: The ltree expression

    Returns:
        FunctionCall for nlevel(expr)

    Example:
        >>> ltree_nlevel(dialect, 'path_col')
        # Generates SQL: nlevel(path_col)
        >>> ltree_nlevel(dialect, ltree_literal(dialect, 'Top.Science.Astronomy'))
        # Returns 3 (number of labels)
    """
    return core.FunctionCall(
        dialect, "nlevel",
        expr if isinstance(expr, bases.BaseExpression) else core.Literal(dialect, expr))


def ltree_subpath(
    dialect: "SQLDialectBase",
    expr: bases.BaseExpression,
    start: int,
    length: Optional[int] = None,
) -> core.FunctionCall:
    """Generate SQL expression for subpath function.

    Extracts a subpath from the ltree value, starting at the given
    position. If length is provided, extracts that many labels;
    otherwise extracts from start to the end.

    Args:
        dialect: The SQL dialect instance
        expr: The ltree expression
        start: Starting position (0-indexed)
        length: Optional number of labels to extract

    Returns:
        FunctionCall for subpath(expr, start[, length])

    Example:
        >>> ltree_subpath(dialect, 'path_col', 0, 2)
        # Generates SQL: subpath(path_col, 0, 2)
        >>> ltree_subpath(dialect, 'path_col', 1)
        # Generates SQL: subpath(path_col, 1)
    """
    args = [
        expr if isinstance(expr, bases.BaseExpression) else core.Literal(dialect, expr),
        start if isinstance(start, bases.BaseExpression) else core.Literal(dialect, start),
    ]
    if length is not None:
        args.append(length if isinstance(length, bases.BaseExpression) else core.Literal(dialect, length))
    return core.FunctionCall(dialect, "subpath", *args)


def ltree_lca(
    dialect: "SQLDialectBase",
    *paths: bases.BaseExpression,
) -> core.FunctionCall:
    """Generate SQL expression for lca (lowest common ancestor) function.

    Computes the lowest common ancestor of the given ltree paths.

    Args:
        dialect: The SQL dialect instance
        *paths: Two or more ltree path expressions

    Returns:
        FunctionCall for lca(path1, path2, ...)

    Example:
        >>> ltree_lca(dialect, ltree_literal(dialect, 'Top.Science.Astronomy'),
        ...           ltree_literal(dialect, 'Top.Science.Physics'))
        # Generates SQL: lca('Top.Science.Astronomy', 'Top.Science.Physics')
        # Returns: 'Top.Science'
    """
    args = [path if isinstance(path, bases.BaseExpression) else core.Literal(dialect, path) for path in paths]
    return core.FunctionCall(dialect, "lca", *args)


__all__ = [
    # Literal constructors
    "ltree_literal",
    "lquery_literal",
    "ltxtquery_literal",
    # ltree operators
    "ltree_ancestor",
    "ltree_descendant",
    "ltree_matches",
    "ltree_text_search",
    "ltree_concat",
    # ltree functions
    "ltree_nlevel",
    "ltree_subpath",
    "ltree_lca",
]
