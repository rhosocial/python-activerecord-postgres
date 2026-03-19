# src/rhosocial/activerecord/backend/impl/postgres/functions/json.py
"""
PostgreSQL JSON Functions.

This module provides SQL expression generators for PostgreSQL JSON
functions and operators.

PostgreSQL Documentation: https://www.postgresql.org/docs/current/functions-json.html

All functions follow the expression-dialect separation architecture:
- For SQL expression generators, first parameter is always the dialect instance
- They return SQL expression strings or PostgresJsonPath objects
"""
from typing import Any, Dict, Optional, Union, TYPE_CHECKING

if TYPE_CHECKING:
    from rhosocial.activerecord.backend.dialect import SQLDialectBase
    from ..types.json import PostgresJsonPath


# Path builder utility functions

def json_path_root() -> "PostgresJsonPath":
    """Create root jsonpath expression.

    Returns:
        PostgresJsonPath representing the root ('$')

    Examples:
        >>> json_path_root()
        PostgresJsonPath('$')
    """
    from ..types.json import PostgresJsonPath
    return PostgresJsonPath('$')


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


def json_path_index(
    path: Union["PostgresJsonPath", str],
    index: Union[int, str]
) -> "PostgresJsonPath":
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


def json_path_filter(
    path: Union["PostgresJsonPath", str],
    condition: str
) -> "PostgresJsonPath":
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


# SQL expression generators for common jsonpath operations

def jsonb_path_query(
    dialect: "SQLDialectBase",
    column: str,
    path: Union["PostgresJsonPath", str],
    vars: Optional[Dict[str, Any]] = None
) -> str:
    """Generate jsonb_path_query SQL expression.

    This function extracts JSON values that match the path.

    Args:
        dialect: The SQL dialect instance
        column: Column name or JSON expression
        path: jsonpath expression
        vars: Optional variables for the path

    Returns:
        SQL expression for jsonb_path_query function

    Examples:
        >>> jsonb_path_query(dialect, 'data', '$.items[*]')
        "jsonb_path_query(data, '$.items[*]')"
    """
    from ..types.json import PostgresJsonPath
    import json

    path_str = path.path if isinstance(path, PostgresJsonPath) else path
    path_literal = f"'{path_str}'"

    if vars:
        vars_json = json.dumps(vars).replace("'", "''")
        return f"jsonb_path_query({column}, {path_literal}, '{vars_json}')"

    return f"jsonb_path_query({column}, {path_literal})"


def jsonb_path_query_first(
    dialect: "SQLDialectBase",
    column: str,
    path: Union["PostgresJsonPath", str],
    vars: Optional[Dict[str, Any]] = None
) -> str:
    """Generate jsonb_path_query_first SQL expression.

    This function extracts the first JSON value that matches the path.

    Args:
        dialect: The SQL dialect instance
        column: Column name or JSON expression
        path: jsonpath expression
        vars: Optional variables for the path

    Returns:
        SQL expression for jsonb_path_query_first function
    """
    from ..types.json import PostgresJsonPath
    import json

    path_str = path.path if isinstance(path, PostgresJsonPath) else path
    path_literal = f"'{path_str}'"

    if vars:
        vars_json = json.dumps(vars).replace("'", "''")
        return f"jsonb_path_query_first({column}, {path_literal}, '{vars_json}')"

    return f"jsonb_path_query_first({column}, {path_literal})"


def jsonb_path_exists(
    dialect: "SQLDialectBase",
    column: str,
    path: Union["PostgresJsonPath", str],
    vars: Optional[Dict[str, Any]] = None
) -> str:
    """Generate jsonb_path_exists SQL expression.

    This function checks if the path returns any values.

    Args:
        dialect: The SQL dialect instance
        column: Column name or JSON expression
        path: jsonpath expression
        vars: Optional variables for the path

    Returns:
        SQL expression for jsonb_path_exists function

    Examples:
        >>> jsonb_path_exists(dialect, 'data', '$.items[*]?(@.active)')
        "jsonb_path_exists(data, '$.items[*]?(@.active)')"
    """
    from ..types.json import PostgresJsonPath
    import json

    path_str = path.path if isinstance(path, PostgresJsonPath) else path
    path_literal = f"'{path_str}'"

    if vars:
        vars_json = json.dumps(vars).replace("'", "''")
        return f"jsonb_path_exists({column}, {path_literal}, '{vars_json}')"

    return f"jsonb_path_exists({column}, {path_literal})"


def jsonb_path_match(
    dialect: "SQLDialectBase",
    column: str,
    path: Union["PostgresJsonPath", str],
    vars: Optional[Dict[str, Any]] = None
) -> str:
    """Generate jsonb_path_match SQL expression.

    This function returns true if the path predicate is satisfied.

    Args:
        dialect: The SQL dialect instance
        column: Column name or JSON expression
        path: jsonpath predicate expression
        vars: Optional variables for the path

    Returns:
        SQL expression for jsonb_path_match function
    """
    from ..types.json import PostgresJsonPath
    import json

    path_str = path.path if isinstance(path, PostgresJsonPath) else path
    path_literal = f"'{path_str}'"

    if vars:
        vars_json = json.dumps(vars).replace("'", "''")
        return f"jsonb_path_match({column}, {path_literal}, '{vars_json}')"

    return f"jsonb_path_match({column}, {path_literal})"


__all__ = [
    'json_path_root',
    'json_path_key',
    'json_path_index',
    'json_path_wildcard',
    'json_path_filter',
    'jsonb_path_query',
    'jsonb_path_query_first',
    'jsonb_path_exists',
    'jsonb_path_match',
]
