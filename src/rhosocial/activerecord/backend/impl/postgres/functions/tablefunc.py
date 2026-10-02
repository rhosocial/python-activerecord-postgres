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

from typing import Optional, TYPE_CHECKING

from rhosocial.activerecord.backend.expression import bases, core

if TYPE_CHECKING:
    from rhosocial.activerecord.backend.dialect import SQLDialectBase


# ============== Crosstab Functions ==============

def crosstab(
    dialect: "SQLDialectBase",
    source_sql: str,
    categories_sql: Optional[str] = None,
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
        >>> from rhosocial.activerecord.backend.impl.postgres.dialect import PostgresDialect
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
            source_sql if isinstance(source_sql, bases.BaseExpression) else core.Literal(dialect, source_sql),
            categories_sql if isinstance(categories_sql, bases.BaseExpression)
            else core.Literal(dialect, categories_sql),
        )
    return core.FunctionCall(
        dialect, "crosstab",
        source_sql if isinstance(source_sql, bases.BaseExpression) else core.Literal(dialect, source_sql),
    )


# ============== Hierarchical Tree Functions ==============

def connectby(
    dialect: "SQLDialectBase",
    table_name: str,
    key_column: str,
    parent_column: str,
    start_value: str,
    max_depth: Optional[int] = None,
    branch_delim: Optional[str] = None,
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
        table_name if isinstance(table_name, bases.BaseExpression) else core.Literal(dialect, table_name),
        key_column if isinstance(key_column, bases.BaseExpression) else core.Literal(dialect, key_column),
        parent_column if isinstance(parent_column, bases.BaseExpression) else core.Literal(dialect, parent_column),
        start_value if isinstance(start_value, bases.BaseExpression) else core.Literal(dialect, start_value),
    ]
    if max_depth is not None:
        args.append(max_depth if isinstance(max_depth, bases.BaseExpression) else core.Literal(dialect, max_depth))
    if branch_delim is not None:
        args.append(
            branch_delim if isinstance(branch_delim, bases.BaseExpression)
            else core.Literal(dialect, branch_delim))
    return core.FunctionCall(dialect, "connectby", *args)


# ============== Random Data Functions ==============

def normal_rand(
    dialect: "SQLDialectBase",
    num_values: int,
    mean: float,
    stddev: float,
    seed: Optional[int] = None,
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
        num_values if isinstance(num_values, bases.BaseExpression) else core.Literal(dialect, num_values),
        mean if isinstance(mean, bases.BaseExpression) else core.Literal(dialect, mean),
        stddev if isinstance(stddev, bases.BaseExpression) else core.Literal(dialect, stddev),
    ]
    if seed is not None:
        args.append(seed if isinstance(seed, bases.BaseExpression) else core.Literal(dialect, seed))
    return core.FunctionCall(dialect, "normal_rand", *args)


__all__ = [
    # Crosstab functions
    "crosstab",
    # Hierarchical tree functions
    "connectby",
    # Random data functions
    "normal_rand",
]
