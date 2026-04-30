# src/rhosocial/activerecord/backend/impl/postgres/functions/tablefunc.py
"""
PostgreSQL tablefunc Extension Functions.

This module provides SQL expression generators for PostgreSQL tablefunc
extension functions. All functions return Expression objects (FunctionCall)
that integrate with the expression-dialect architecture.

The tablefunc extension provides functions to produce crosstab
(pivot table) displays, connectby tree traversal, and random number
generation.

PostgreSQL Documentation: https://www.postgresql.org/docs/current/tablefunc.html

The tablefunc extension must be installed:
    CREATE EXTENSION IF NOT EXISTS tablefunc;

Supported functions:
- crosstab: Produce pivot table (crosstab) displays
- connectby: Produce tree-structured display of hierarchical data
- normal_rand: Generate a set of normally distributed random values

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


# ============== Crosstab Functions ==============

def crosstab(
    dialect: "SQLDialectBase",
    source_sql: Union[str, "bases.BaseExpression"],
    categories_sql: Optional[Union[str, "bases.BaseExpression"]] = None,
) -> core.FunctionCall:
    """Produce a pivot table (crosstab) display from query results.

    The crosstab function produces a pivot table display of data.
    The source_sql query must return three columns: row_name (the
    label for each row), category (the label for each column), and
    value (the value to place in each cell).

    When categories_sql is provided (two-argument form), it must
    return a single column of category names that defines the set
    of output columns and their order. When omitted (one-argument
    form), the output columns are determined from the source_sql
    query, which may produce inconsistent results if the category
    set varies.

    Args:
        dialect: The SQL dialect instance
        source_sql: SQL query that produces the source data set. Must
                    return exactly three columns: row_name, category,
                    value
        categories_sql: Optional SQL query that produces the set of
                        category names. Must return exactly one column.
                        When provided, ensures consistent column ordering

    Returns:
        FunctionCall for crosstab(source_sql[, categories_sql])

    Example:
        >>> from rhosocial.activerecord.backend.impl.postgres import PostgresDialect
        >>> dialect = PostgresDialect()
        >>> crosstab(dialect, 'SELECT row_name, category, value FROM data')
        >>> crosstab(
        ...     dialect,
        ...     'SELECT row_name, category, value FROM data',
        ...     'SELECT DISTINCT category FROM data ORDER BY category')
    """
    if categories_sql is not None:
        return core.FunctionCall(
            dialect, "crosstab",
            _convert_to_expression(dialect, source_sql),
            _convert_to_expression(dialect, categories_sql),
        )
    return core.FunctionCall(
        dialect, "crosstab",
        _convert_to_expression(dialect, source_sql),
    )


# ============== Hierarchical Tree Functions ==============

def connectby(
    dialect: "SQLDialectBase",
    table_name: Union[str, "bases.BaseExpression"],
    key_column: Union[str, "bases.BaseExpression"],
    parent_column: Union[str, "bases.BaseExpression"],
    start_value: Union[str, "bases.BaseExpression"],
    max_depth: Optional[Union[int, "bases.BaseExpression"]] = None,
    branch_delim: Optional[Union[str, "bases.BaseExpression"]] = None,
) -> core.FunctionCall:
    """Produce a tree-structured display of hierarchical data.

    The connectby function displays a hierarchical tree structure from
    data in a table. It traverses the tree starting from the specified
    root value, following parent-child relationships.

    Args:
        dialect: The SQL dialect instance
        table_name: Name of the table containing the hierarchical data
        key_column: Name of the column that uniquely identifies each row
        parent_column: Name of the column that references the parent row's
                       key column
        start_value: Key value of the row to start traversal from (the
                     root of the tree)
        max_depth: Optional maximum depth of the tree to traverse. When
                   provided, limits the depth of the recursion
        branch_delim: Optional delimiter string used to separate keys
                      in the branch path output. When provided, an
                      additional branch column is included in the output

    Returns:
        FunctionCall for connectby(table_name, key_column, parent_column, start_value[, max_depth[, branch_delim]])

    Example:
        >>> connectby(dialect, 'employees', 'emp_id', 'manager_id', '1')
        >>> connectby(dialect, 'employees', 'emp_id', 'manager_id', '1', max_depth=3)
        >>> connectby(dialect, 'employees', 'emp_id', 'manager_id', '1', max_depth=3, branch_delim='~')
    """
    args = [
        _convert_to_expression(dialect, table_name),
        _convert_to_expression(dialect, key_column),
        _convert_to_expression(dialect, parent_column),
        _convert_to_expression(dialect, start_value),
    ]
    if max_depth is not None:
        args.append(_convert_to_expression(dialect, max_depth))
    if branch_delim is not None:
        args.append(_convert_to_expression(dialect, branch_delim))
    return core.FunctionCall(dialect, "connectby", *args)


# ============== Random Data Functions ==============

def normal_rand(
    dialect: "SQLDialectBase",
    num_values: Union[int, "bases.BaseExpression"],
    mean: Union[float, "bases.BaseExpression"],
    stddev: Union[float, "bases.BaseExpression"],
    seed: Optional[Union[int, "bases.BaseExpression"]] = None,
) -> core.FunctionCall:
    """Generate a set of normally distributed random values.

    Returns a set of random values following a normal (Gaussian)
    distribution with the specified mean and standard deviation.

    Args:
        dialect: The SQL dialect instance
        num_values: Number of random values to generate (positive integer)
        mean: Mean (average) of the normal distribution
        stddev: Standard deviation of the normal distribution
                (must be non-negative)
        seed: Optional seed value for reproducible random results

    Returns:
        FunctionCall for normal_rand(num_values, mean, stddev[, seed])

    Example:
        >>> normal_rand(dialect, 100, 5.0, 1.5)
        >>> normal_rand(dialect, 50, 0, 1, seed=42)
    """
    args = [
        _convert_to_expression(dialect, num_values),
        _convert_to_expression(dialect, mean),
        _convert_to_expression(dialect, stddev),
    ]
    if seed is not None:
        args.append(_convert_to_expression(dialect, seed))
    return core.FunctionCall(dialect, "normal_rand", *args)


__all__ = [
    # Crosstab functions
    "crosstab",
    # Hierarchical tree functions
    "connectby",
    # Random data functions
    "normal_rand",
]
