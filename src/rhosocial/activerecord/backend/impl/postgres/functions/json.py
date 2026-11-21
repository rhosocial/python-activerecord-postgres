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
    from ..type_values.json import PostgresJsonPath


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
    from ..type_values.json import PostgresJsonPath

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
    from ..type_values.json import PostgresJsonPath

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
    from ..type_values.json import PostgresJsonPath

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
    from ..type_values.json import PostgresJsonPath

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
    from ..type_values.json import PostgresJsonPath

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
    from ..type_values.json import PostgresJsonPath

    if isinstance(path, PostgresJsonPath):
        return path.filter(condition)
    return PostgresJsonPath(path).filter(condition)


# ============== JSONB Query Functions ==============


def jsonb_path_query(
    dialect: "SQLDialectBase",
    column: bases.BaseExpression,
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
    col_expr = column if isinstance(column, bases.BaseExpression) else core.Literal(dialect, column)
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
    column: bases.BaseExpression,
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
    col_expr = column if isinstance(column, bases.BaseExpression) else core.Literal(dialect, column)
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
    column: bases.BaseExpression,
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
    col_expr = column if isinstance(column, bases.BaseExpression) else core.Literal(dialect, column)
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
    column: bases.BaseExpression,
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
    col_expr = column if isinstance(column, bases.BaseExpression) else core.Literal(dialect, column)
    path_expr = _convert_jsonpath(dialect, path)
    args = [col_expr, path_expr]
    if vars is not None:
        vars_expr = core.Literal(dialect, json.dumps(vars))
        args.append(vars_expr)
    if silent:
        args.append(core.Literal(dialect, True))
    return core.FunctionCall(dialect, "jsonb_path_match", *args)


def create_jsonb_index(
    dialect: "SQLDialectBase",
    table_name: str,
    key: str,
    *,
    column_name: str = "data",
    index_name: Optional[str] = None,
    opclass: Optional[str] = "jsonb_path_ops",
    as_text: bool = False,
    if_not_exists: bool = False,
):
    """Build a ``CREATE INDEX`` on a key inside a ``jsonb`` column.

    Two shapes are common and both need the path inlined: a server rejects a
    bind parameter inside an index expression before it can infer its type.

    * ``as_text=False`` (default): a GIN index on the jsonb value at *key*,
      with ``jsonb_path_ops`` unless another opclass is given. Supports
      ``@?`` / ``@@`` containment on that sub-document.
    * ``as_text=True``: a btree index on the text at *key* (``->>``), for
      equality, ordering and range scans. No opclass is applied.

    Args:
        dialect: The SQL dialect instance.
        table_name: Table holding the jsonb column.
        key: A JSONPath key such as ``"$.name"`` or ``"$.profile.address"``.
        column_name: Name of the jsonb column (default ``"data"``).
        index_name: Index name; generated from table and column when omitted.
        opclass: Operator class for the GIN form. Pass ``None`` for default.
        as_text: Build a text (btree) expression index instead of GIN.
        if_not_exists: Add ``IF NOT EXISTS``.

    Returns:
        A ready-to-execute ``PostgresCreateIndexExpression``.
    """
    from rhosocial.activerecord.backend.expression.objects import Index, Table
    from rhosocial.activerecord.backend.expression.functions.json import (
        json_extract,
        json_extract_text,
    )

    from ..expression.ddl import PostgresCreateIndexExpression

    path = key if key.startswith("$") else f"$.{key}"
    builder = json_extract_text if as_text else json_extract
    expression = builder(dialect, column_name, path, inline_path=True)

    name = index_name or f"idx_{table_name}_{column_name}_{'text' if as_text else 'gin'}"
    return PostgresCreateIndexExpression(
        dialect,
        index=Index(dialect, name),
        table=Table(dialect, table_name),
        columns=[expression],
        index_type=None if as_text else "GIN",
        if_not_exists=if_not_exists,
        expression_opclasses=[] if as_text or opclass is None else [(expression, opclass)],
    )


__all__ = [
    "create_jsonb_index",
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
