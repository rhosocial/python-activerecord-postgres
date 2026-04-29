# src/rhosocial/activerecord/backend/impl/postgres/functions/pgrouting.py
"""
PostgreSQL pgRouting Extension Functions.

This module provides SQL expression generators for PostgreSQL pgRouting
extension functions. All functions return Expression objects (FunctionCall)
that integrate with the expression-dialect architecture.

The pgRouting extension extends the PostGIS/PostgreSQL geospatial database
to provide geospatial routing functionality.

PostgreSQL Documentation: https://docs.pgrouting.org/latest/en/

The pgRouting extension must be installed (requires PostGIS):
    CREATE EXTENSION IF NOT EXISTS postgis;
    CREATE EXTENSION IF NOT EXISTS pgrouting;

Supported functions:
- pgr_dijkstra: Shortest path using Dijkstra's algorithm
- pgr_astar: Shortest path using A* algorithm

All functions follow the expression-dialect separation architecture:
- First parameter is always the dialect instance
- They return Expression objects (FunctionCall, BinaryExpression, etc.)
- They do not concatenate SQL strings directly
"""

from typing import Optional, Union, TYPE_CHECKING

from rhosocial.activerecord.backend.expression import bases, core

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


# ============== Routing Functions ==============

def pgr_dijkstra(
    dialect: "SQLDialectBase",
    edges_sql: Union[str, "bases.BaseExpression"],
    start_vid: Union[int, "bases.BaseExpression"],
    end_vid: Union[int, "bases.BaseExpression"],
    directed: Union[bool, "bases.BaseExpression"] = True,
) -> core.FunctionCall:
    """Calculate the shortest path using Dijkstra's algorithm.

    Returns the shortest path from a start vertex to an end vertex
    using Dijkstra's algorithm. The edges_sql parameter is a SQL query
    that returns a set of edges with columns: id, source, target, cost
    (and optionally reverse_cost).

    Args:
        dialect: The SQL dialect instance
        edges_sql: SQL query that selects the edge data. Must return
                   columns: id, source, target, cost [, reverse_cost]
        start_vid: Starting vertex identifier
        end_vid: Ending vertex identifier
        directed: Whether the graph is directed (default: True). When
                  True, the graph is treated as directed; when False,
                  it is treated as undirected

    Returns:
        FunctionCall for pgr_dijkstra(edges_sql, start_vid, end_vid[, directed])

    Example:
        >>> from rhosocial.activerecord.backend.impl.postgres import PostgresDialect
        >>> dialect = PostgresDialect()
        >>> pgr_dijkstra(
        ...     dialect,
        ...     'SELECT id, source, target, cost FROM edges',
        ...     1, 5)
        >>> pgr_dijkstra(
        ...     dialect,
        ...     'SELECT id, source, target, cost, reverse_cost FROM edges',
        ...     1, 5, directed=False)
    """
    return core.FunctionCall(
        dialect, "pgr_dijkstra",
        _convert_to_expression(dialect, edges_sql),
        _convert_to_expression(dialect, start_vid),
        _convert_to_expression(dialect, end_vid),
        _convert_to_expression(dialect, directed),
    )


def pgr_astar(
    dialect: "SQLDialectBase",
    edges_sql: Union[str, "bases.BaseExpression"],
    start_vid: Union[int, "bases.BaseExpression"],
    end_vid: Union[int, "bases.BaseExpression"],
    directed: Union[bool, "bases.BaseExpression"] = True,
) -> core.FunctionCall:
    """Calculate the shortest path using A* algorithm.

    Returns the shortest path from a start vertex to an end vertex
    using the A* algorithm. The A* algorithm uses heuristics to
    improve performance over Dijkstra's algorithm for large graphs.

    The edges_sql parameter is a SQL query that returns a set of edges
    with columns: id, source, target, cost, x1, y1, x2, y2
    (and optionally reverse_cost).

    Args:
        dialect: The SQL dialect instance
        edges_sql: SQL query that selects the edge data. Must return
                   columns: id, source, target, cost, x1, y1, x2, y2
                   [, reverse_cost]. The x1/y1 and x2/y2 are the
                   coordinates of the start and end points of the edge
        start_vid: Starting vertex identifier
        end_vid: Ending vertex identifier
        directed: Whether the graph is directed (default: True). When
                  True, the graph is treated as directed; when False,
                  it is treated as undirected

    Returns:
        FunctionCall for pgr_astar(edges_sql, start_vid, end_vid[, directed])

    Example:
        >>> pgr_astar(
        ...     dialect,
        ...     'SELECT id, source, target, cost, x1, y1, x2, y2 FROM edges',
        ...     1, 5)
        >>> pgr_astar(
        ...     dialect,
        ...     'SELECT id, source, target, cost, x1, y1, x2, y2, reverse_cost FROM edges',
        ...     1, 5, directed=False)
    """
    return core.FunctionCall(
        dialect, "pgr_astar",
        _convert_to_expression(dialect, edges_sql),
        _convert_to_expression(dialect, start_vid),
        _convert_to_expression(dialect, end_vid),
        _convert_to_expression(dialect, directed),
    )


__all__ = [
    # Routing functions
    "pgr_dijkstra",
    "pgr_astar",
]
