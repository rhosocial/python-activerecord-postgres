# src/rhosocial/activerecord/backend/impl/postgres/functions/json.py
"""
PostgreSQL JSON Functions.

This module provides two categories of functions:

1. **JSON path builder functions** - Return PostgresJsonPath objects for
   composing jsonpath expressions. These do not take a dialect parameter
   since they build path objects, not SQL strings.

2. **JSONB query functions** - Return core.FunctionCall Expression objects
   that integrate with the Expression/Dialect architecture. These accept
   a dialect parameter as their first argument.

PostgreSQL Documentation: https://www.postgresql.org/docs/current/functions-json.html
"""

import json
from typing import Any, Dict, Optional, Union, TYPE_CHECKING

from rhosocial.activerecord.backend.expression import bases, core

if TYPE_CHECKING:
    from rhosocial.activerecord.backend.dialect import SQLDialectBase
    from ..types.json import PostgresJsonPath


def _convert_to_expression(
    dialect: "SQLDialectBase",
    expr: Union[str, "bases.BaseExpression"],
) -> "bases.BaseExpression":
    """Convert an input value to an appropriate BaseExpression.

    Supports strings and existing BaseExpression objects.

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


def _convert_jsonpath(
    dialect: "SQLDialectBase",
    path: Union["PostgresJsonPath", str, "bases.BaseExpression"],
) -> "bases.BaseExpression":
    """Convert a jsonpath value to an appropriate BaseExpression.

    Handles PostgresJsonPath objects, strings, and existing
    BaseExpression objects.

    Args:
        dialect: The SQL dialect instance
        path: jsonpath value to convert

    Returns:
        BaseExpression representing the jsonpath literal
    """
    from ..types.json import PostgresJsonPath

    if isinstance(path, PostgresJsonPath):
        return core.Literal(dialect, str(path))
    elif isinstance(path, bases.BaseExpression):
        return path
    else:
        return core.Literal(dialect, str(path))


# ============== Path Builder Functions ==============


def json_path_root() -> "PostgresJsonPath":
    """Create root jsonpath expression.

    Returns:
        PostgresJsonPath representing the root ('$')

    Examples:
        >>> json_path_root()
        PostgresJsonPath('$')
    """
    from ..types.json import PostgresJsonPath

    return PostgresJsonPath("$")


def json_path_key(path: Union["PostgresJsonPath", str], key: str) -> "PostgresJsonPath":
    """Add object member access to path.

    Args:
        path: Existing path (PostgresJsonPath or string)
        key: Member name to access

    Returns:
        New PostgresJsonPath with key appended

    Examples:
        >>> json_path_key('$', 'name')
        PostgresJsonPath('$.name')

        >>> json_path_key('$.store', 'book')
        PostgresJsonPath('$.store.book')
    """
    from ..types.json import PostgresJsonPath

    if isinstance(path, PostgresJsonPath):
        return path.key(key)
    return PostgresJsonPath(path).key(key)


def json_path_index(path: Union["PostgresJsonPath", str], index: Union[int, str]) -> "PostgresJsonPath":
    """Add array index access to path.

    Args:
        path: Existing path (PostgresJsonPath or string)
        index: Array index (integer, 'last', or 'last-N')

    Returns:
        New PostgresJsonPath with index appended

    Examples:
        >>> json_path_index('$', 0)
        PostgresJsonPath('$[0]')

        >>> json_path_index('$.items', 'last')
        PostgresJsonPath('$.items[last]')
    """
    from ..types.json import PostgresJsonPath

    if isinstance(path, PostgresJsonPath):
        return path.index(index)
    return PostgresJsonPath(path).index(index)


def json_path_wildcard(path: Union["PostgresJsonPath", str], array: bool = True) -> "PostgresJsonPath":
    """Add wildcard access to path.

    Args:
        path: Existing path (PostgresJsonPath or string)
        array: If True, add array wildcard [*]; otherwise add object wildcard .*

    Returns:
        New PostgresJsonPath with wildcard appended

    Examples:
        >>> json_path_wildcard('$')
        PostgresJsonPath('$[*]')

        >>> json_path_wildcard('$', array=False)
        PostgresJsonPath('$.*')

        >>> json_path_wildcard('$.items')
        PostgresJsonPath('$.items[*]')
    """
    from ..types.json import PostgresJsonPath

    if isinstance(path, PostgresJsonPath):
        return path.wildcard_array() if array else path.wildcard_object()
    path_obj = PostgresJsonPath(path)
    return path_obj.wildcard_array() if array else path_obj.wildcard_object()


def json_path_filter(path: Union["PostgresJsonPath", str], condition: str) -> "PostgresJsonPath":
    """Add filter expression to path.

    Args:
        path: Existing path (PostgresJsonPath or string)
        condition: Filter condition (use @ for current element)

    Returns:
        New PostgresJsonPath with filter appended

    Examples:
        >>> json_path_filter('$.items[*]', '@.price < 10')
        PostgresJsonPath('$.items[*]?(@.price < 10)')

        >>> json_path_filter('$.books[*]', '@.author == "King"')
        PostgresJsonPath('$.books[*]?(@.author == "King")')
    """
    from ..types.json import PostgresJsonPath

    if isinstance(path, PostgresJsonPath):
        return path.filter(condition)
    return PostgresJsonPath(path).filter(condition)


# ============== JSONB Query Functions ==============


def jsonb_path_query(
    dialect: "SQLDialectBase",
    column: Union[str, "bases.BaseExpression"],
    path: Union["PostgresJsonPath", str, "bases.BaseExpression"],
    vars: Optional[Dict[str, Any]] = None,
    silent: bool = False,
) -> core.FunctionCall:
    """Generate jsonb_path_query function expression.

    This function extracts all JSON values that match the jsonpath
    expression, returning a set of jsonb values.

    Args:
        dialect: The SQL dialect instance
        column: Column name or expression referencing jsonb data
        path: jsonpath expression (PostgresJsonPath, string, or BaseExpression)
        vars: Optional variables for the path as a dict
        silent: If True, suppress errors (PostgreSQL 13+)

    Returns:
        FunctionCall expression for jsonb_path_query

    Examples:
        >>> func = jsonb_path_query(dialect, 'data', '$.items[*]')
        >>> func.to_sql()
        ('jsonb_path_query(%s, %s)', ('data', '$.items[*]'))

        >>> func = jsonb_path_query(dialect, 'data', '$.items[*]', vars={'min': 10})
        >>> func.to_sql()
        ('jsonb_path_query(%s, %s, %s)', ('data', '$.items[*]', '{"min": 10}'))

        >>> func = jsonb_path_query(dialect, 'data', '$.items[*]', silent=True)
        >>> func.to_sql()
        ('jsonb_path_query(%s, %s, %s)', ('data', '$.items[*]', True))
    """
    col_expr = _convert_to_expression(dialect, column)
    path_expr = _convert_jsonpath(dialect, path)
    args = [col_expr, path_expr]
    if vars is not None:
        vars_expr = core.Literal(dialect, json.dumps(vars))
        args.append(vars_expr)
    if silent:
        args.append(core.Literal(dialect, True))
    return core.FunctionCall(dialect, "jsonb_path_query", *args)


def jsonb_path_query_first(
    dialect: "SQLDialectBase",
    column: Union[str, "bases.BaseExpression"],
    path: Union["PostgresJsonPath", str, "bases.BaseExpression"],
    vars: Optional[Dict[str, Any]] = None,
    silent: bool = False,
) -> core.FunctionCall:
    """Generate jsonb_path_query_first function expression.

    This function extracts the first JSON value that matches the
    jsonpath expression, returning a single jsonb value.

    Args:
        dialect: The SQL dialect instance
        column: Column name or expression referencing jsonb data
        path: jsonpath expression (PostgresJsonPath, string, or BaseExpression)
        vars: Optional variables for the path as a dict
        silent: If True, suppress errors (PostgreSQL 13+)

    Returns:
        FunctionCall expression for jsonb_path_query_first

    Examples:
        >>> func = jsonb_path_query_first(dialect, 'data', '$.items[0]')
        >>> func.to_sql()
        ('jsonb_path_query_first(%s, %s)', ('data', '$.items[0]'))

        >>> func = jsonb_path_query_first(dialect, 'data', '$.name', vars={'lang': 'en'})
        >>> func.to_sql()
        ('jsonb_path_query_first(%s, %s, %s)', ('data', '$.name', '{"lang": "en"}'))
    """
    col_expr = _convert_to_expression(dialect, column)
    path_expr = _convert_jsonpath(dialect, path)
    args = [col_expr, path_expr]
    if vars is not None:
        vars_expr = core.Literal(dialect, json.dumps(vars))
        args.append(vars_expr)
    if silent:
        args.append(core.Literal(dialect, True))
    return core.FunctionCall(dialect, "jsonb_path_query_first", *args)


def jsonb_path_exists(
    dialect: "SQLDialectBase",
    column: Union[str, "bases.BaseExpression"],
    path: Union["PostgresJsonPath", str, "bases.BaseExpression"],
    vars: Optional[Dict[str, Any]] = None,
    silent: bool = False,
) -> core.FunctionCall:
    """Generate jsonb_path_exists function expression.

    This function checks whether the jsonpath expression returns
    any items for the specified jsonb value.

    Args:
        dialect: The SQL dialect instance
        column: Column name or expression referencing jsonb data
        path: jsonpath expression (PostgresJsonPath, string, or BaseExpression)
        vars: Optional variables for the path as a dict
        silent: If True, suppress errors (PostgreSQL 13+)

    Returns:
        FunctionCall expression for jsonb_path_exists

    Examples:
        >>> func = jsonb_path_exists(dialect, 'data', '$.items[*]?(@.active)')
        >>> func.to_sql()
        ('jsonb_path_exists(%s, %s)', ('data', '$.items[*]?(@.active)'))

        >>> func = jsonb_path_exists(dialect, 'data', '$.items[*]', vars={'min': 5})
        >>> func.to_sql()
        ('jsonb_path_exists(%s, %s, %s)', ('data', '$.items[*]', '{"min": 5}'))
    """
    col_expr = _convert_to_expression(dialect, column)
    path_expr = _convert_jsonpath(dialect, path)
    args = [col_expr, path_expr]
    if vars is not None:
        vars_expr = core.Literal(dialect, json.dumps(vars))
        args.append(vars_expr)
    if silent:
        args.append(core.Literal(dialect, True))
    return core.FunctionCall(dialect, "jsonb_path_exists", *args)


def jsonb_path_match(
    dialect: "SQLDialectBase",
    column: Union[str, "bases.BaseExpression"],
    path: Union["PostgresJsonPath", str, "bases.BaseExpression"],
    vars: Optional[Dict[str, Any]] = None,
    silent: bool = False,
) -> core.FunctionCall:
    """Generate jsonb_path_match function expression.

    This function returns true if the jsonpath predicate is satisfied
    for the specified jsonb value (i.e., returns a non-empty sequence).

    Unlike jsonb_path_exists which checks for any match, jsonb_path_match
    is used with predicate expressions that return boolean results.

    Args:
        dialect: The SQL dialect instance
        column: Column name or expression referencing jsonb data
        path: jsonpath predicate expression (PostgresJsonPath, string, or BaseExpression)
        vars: Optional variables for the path as a dict
        silent: If True, suppress errors (PostgreSQL 13+)

    Returns:
        FunctionCall expression for jsonb_path_match

    Examples:
        >>> func = jsonb_path_match(dialect, 'data', '$.items[*] ? (@.price > 10)')
        >>> func.to_sql()
        ('jsonb_path_match(%s, %s)', ('data', '$.items[*] ? (@.price > 10)'))

        >>> func = jsonb_path_match(dialect, 'data', '$.count > $threshold', vars={'threshold': 5})
        >>> func.to_sql()
        ('jsonb_path_match(%s, %s, %s)', ('data', '$.count > $threshold', '{"threshold": 5}'))
    """
    col_expr = _convert_to_expression(dialect, column)
    path_expr = _convert_jsonpath(dialect, path)
    args = [col_expr, path_expr]
    if vars is not None:
        vars_expr = core.Literal(dialect, json.dumps(vars))
        args.append(vars_expr)
    if silent:
        args.append(core.Literal(dialect, True))
    return core.FunctionCall(dialect, "jsonb_path_match", *args)


__all__ = [
    "json_path_root",
    "json_path_key",
    "json_path_index",
    "json_path_wildcard",
    "json_path_filter",
    "jsonb_path_query",
    "jsonb_path_query_first",
    "jsonb_path_exists",
    "jsonb_path_match",
]
