# src/rhosocial/activerecord/backend/impl/postgres/functions/hstore.py
"""
PostgreSQL hstore function factories.

This module provides SQL expression generators for PostgreSQL hstore
functions and operators. All functions return Expression objects
that integrate with the Expression/Dialect architecture.

hstore is a PostgreSQL extension that stores key/value pairs in a single value.

PostgreSQL Documentation: https://www.postgresql.org/docs/current/hstore.html

The hstore extension must be installed:
    CREATE EXTENSION IF NOT EXISTS hstore;

Supported functions:
- Constructors: hstore(record), hstore(key, value)
- Key/Value Extraction: akeys, skeys, avals, svals, each
- Conversion: hstore_to_array, hstore_to_matrix, hstore_to_json, hstore_to_jsonb
- Subset: slice
- Existence: exist, defined
- Delete: delete

Supported operators:
- ->  : Access single key (value)
- ->> : Access single key (text)
- ||  : Concatenate
- ?   : Key exists
- ?&  : All keys exist
- ?|  : Any key exists
- @>  : Contains
- <@  : Contained by
- -   : Delete (by key, keys, or pairs)
- #=  : Record update
"""

from typing import Dict, List, Optional, Union, TYPE_CHECKING

from rhosocial.activerecord.backend.expression import bases, core
from rhosocial.activerecord.backend.expression.operators import BinaryExpression
from rhosocial.activerecord.backend.impl.postgres.types.hstore import PostgresHstore

if TYPE_CHECKING:
    from rhosocial.activerecord.backend.dialect import SQLDialectBase


def _convert_to_expression(
    dialect: "SQLDialectBase",
    expr: Union[PostgresHstore, Dict[str, Optional[str]], str, "bases.BaseExpression"],
) -> "bases.BaseExpression":
    """Convert an input value to an appropriate BaseExpression.

    Supports PostgresHstore objects, Dict, strings, and existing
    BaseExpression objects.

    For PostgresHstore and Dict inputs, generates a typed hstore literal
    expression with ::hstore cast.

    Args:
        dialect: The SQL dialect instance
        expr: Value to convert

    Returns:
        BaseExpression representing the value
    """
    if isinstance(expr, bases.BaseExpression):
        return expr
    elif isinstance(expr, PostgresHstore):
        literal = core.Literal(dialect, expr.to_postgres_string())
        return literal.cast("hstore")
    elif isinstance(expr, dict):
        hstore = PostgresHstore(data=expr)
        literal = core.Literal(dialect, hstore.to_postgres_string())
        return literal.cast("hstore")
    elif isinstance(expr, str):
        return core.Literal(dialect, expr)
    else:
        return core.Literal(dialect, expr)


# ============== Constructors ==============

def hstore_from_record(
    dialect: "SQLDialectBase",
    record: Union[str, "bases.BaseExpression"],
) -> core.FunctionCall:
    """Construct hstore from a record/row.

    Args:
        dialect: The SQL dialect instance
        record: The record expression (e.g., table column)

    Returns:
        FunctionCall for hstore(record)
    """
    return core.FunctionCall(dialect, "hstore", _convert_to_expression(dialect, record))


def hstore_from_key_value(
    dialect: "SQLDialectBase",
    key: Union[str, "bases.BaseExpression"],
    value: Union[str, "bases.BaseExpression"],
) -> core.FunctionCall:
    """Construct hstore from a single key/value pair.

    Args:
        dialect: The SQL dialect instance
        key: The key string
        value: The value string

    Returns:
        FunctionCall for hstore(key, value)
    """
    return core.FunctionCall(
        dialect, "hstore",
        _convert_to_expression(dialect, key),
        _convert_to_expression(dialect, value),
    )


# ============== Key/Value Extraction ==============

def hstore_akeys(
    dialect: "SQLDialectBase",
    hstore_expr: Union[PostgresHstore, Dict, str, "bases.BaseExpression"],
) -> core.FunctionCall:
    """Get all keys from hstore as a text array.

    Args:
        dialect: The SQL dialect instance
        hstore_expr: The hstore expression

    Returns:
        FunctionCall for akeys(hstore)
    """
    return core.FunctionCall(dialect, "akeys", _convert_to_expression(dialect, hstore_expr))


def hstore_skeys(
    dialect: "SQLDialectBase",
    hstore_expr: Union[PostgresHstore, Dict, str, "bases.BaseExpression"],
) -> core.FunctionCall:
    """Get all keys from hstore as a set.

    Args:
        dialect: The SQL dialect instance
        hstore_expr: The hstore expression

    Returns:
        FunctionCall for skeys(hstore)
    """
    return core.FunctionCall(dialect, "skeys", _convert_to_expression(dialect, hstore_expr))


def hstore_avals(
    dialect: "SQLDialectBase",
    hstore_expr: Union[PostgresHstore, Dict, str, "bases.BaseExpression"],
) -> core.FunctionCall:
    """Get all values from hstore as a text array.

    Args:
        dialect: The SQL dialect instance
        hstore_expr: The hstore expression

    Returns:
        FunctionCall for avals(hstore)
    """
    return core.FunctionCall(dialect, "avals", _convert_to_expression(dialect, hstore_expr))


def hstore_svals(
    dialect: "SQLDialectBase",
    hstore_expr: Union[PostgresHstore, Dict, str, "bases.BaseExpression"],
) -> core.FunctionCall:
    """Get all values from hstore as a set.

    Args:
        dialect: The SQL dialect instance
        hstore_expr: The hstore expression

    Returns:
        FunctionCall for svals(hstore)
    """
    return core.FunctionCall(dialect, "svals", _convert_to_expression(dialect, hstore_expr))


def hstore_each(
    dialect: "SQLDialectBase",
    hstore_expr: Union[PostgresHstore, Dict, str, "bases.BaseExpression"],
) -> core.FunctionCall:
    """Get all key/value pairs from hstore as a set of (key, value) records.

    Args:
        dialect: The SQL dialect instance
        hstore_expr: The hstore expression

    Returns:
        FunctionCall for each(hstore)
    """
    return core.FunctionCall(dialect, "each", _convert_to_expression(dialect, hstore_expr))


# ============== Conversion Functions ==============

def hstore_to_array(
    dialect: "SQLDialectBase",
    hstore_expr: Union[PostgresHstore, Dict, str, "bases.BaseExpression"],
) -> core.FunctionCall:
    """Convert hstore to text array (alternating keys and values).

    Args:
        dialect: The SQL dialect instance
        hstore_expr: The hstore expression

    Returns:
        FunctionCall for hstore_to_array(hstore)
    """
    return core.FunctionCall(dialect, "hstore_to_array", _convert_to_expression(dialect, hstore_expr))


def hstore_to_matrix(
    dialect: "SQLDialectBase",
    hstore_expr: Union[PostgresHstore, Dict, str, "bases.BaseExpression"],
) -> core.FunctionCall:
    """Convert hstore to 2D text array (key/value pairs as rows).

    Args:
        dialect: The SQL dialect instance
        hstore_expr: The hstore expression

    Returns:
        FunctionCall for hstore_to_matrix(hstore)
    """
    return core.FunctionCall(dialect, "hstore_to_matrix", _convert_to_expression(dialect, hstore_expr))


def hstore_to_json(
    dialect: "SQLDialectBase",
    hstore_expr: Union[PostgresHstore, Dict, str, "bases.BaseExpression"],
) -> core.FunctionCall:
    """Convert hstore to JSON (values as strings).

    Args:
        dialect: The SQL dialect instance
        hstore_expr: The hstore expression

    Returns:
        FunctionCall for hstore_to_json(hstore)
    """
    return core.FunctionCall(dialect, "hstore_to_json", _convert_to_expression(dialect, hstore_expr))


def hstore_to_jsonb(
    dialect: "SQLDialectBase",
    hstore_expr: Union[PostgresHstore, Dict, str, "bases.BaseExpression"],
) -> core.FunctionCall:
    """Convert hstore to JSONB (values as strings).

    Args:
        dialect: The SQL dialect instance
        hstore_expr: The hstore expression

    Returns:
        FunctionCall for hstore_to_jsonb(hstore)
    """
    return core.FunctionCall(dialect, "hstore_to_jsonb", _convert_to_expression(dialect, hstore_expr))


def hstore_to_json_loose(
    dialect: "SQLDialectBase",
    hstore_expr: Union[PostgresHstore, Dict, str, "bases.BaseExpression"],
) -> core.FunctionCall:
    """Convert hstore to JSON with loose type inference.

    Attempts to infer proper JSON types for values (numeric, boolean, etc).

    Args:
        dialect: The SQL dialect instance
        hstore_expr: The hstore expression

    Returns:
        FunctionCall for hstore_to_json_loose(hstore)
    """
    return core.FunctionCall(dialect, "hstore_to_json_loose", _convert_to_expression(dialect, hstore_expr))


def hstore_to_jsonb_loose(
    dialect: "SQLDialectBase",
    hstore_expr: Union[PostgresHstore, Dict, str, "bases.BaseExpression"],
) -> core.FunctionCall:
    """Convert hstore to JSONB with loose type inference.

    Attempts to infer proper JSON types for values (numeric, boolean, etc).

    Args:
        dialect: The SQL dialect instance
        hstore_expr: The hstore expression

    Returns:
        FunctionCall for hstore_to_jsonb_loose(hstore)
    """
    return core.FunctionCall(dialect, "hstore_to_jsonb_loose", _convert_to_expression(dialect, hstore_expr))


# ============== Subset Functions ==============

def hstore_slice(
    dialect: "SQLDialectBase",
    hstore_expr: Union[PostgresHstore, Dict, str, "bases.BaseExpression"],
    keys: Union[str, "bases.BaseExpression"],
) -> core.FunctionCall:
    """Extract subset of hstore by keys.

    Args:
        dialect: The SQL dialect instance
        hstore_expr: The hstore expression
        keys: Array of keys to extract

    Returns:
        FunctionCall for slice(hstore, keys)
    """
    return core.FunctionCall(
        dialect, "slice",
        _convert_to_expression(dialect, hstore_expr),
        _convert_to_expression(dialect, keys),
    )


# ============== Existence Functions ==============

def hstore_exist(
    dialect: "SQLDialectBase",
    hstore_expr: Union[PostgresHstore, Dict, str, "bases.BaseExpression"],
    key: Union[str, "bases.BaseExpression"],
) -> core.FunctionCall:
    """Check if key exists in hstore (including NULL values).

    Args:
        dialect: The SQL dialect instance
        hstore_expr: The hstore expression
        key: The key to check

    Returns:
        FunctionCall for exist(hstore, key)
    """
    return core.FunctionCall(
        dialect, "exist",
        _convert_to_expression(dialect, hstore_expr),
        _convert_to_expression(dialect, key),
    )


def hstore_defined(
    dialect: "SQLDialectBase",
    hstore_expr: Union[PostgresHstore, Dict, str, "bases.BaseExpression"],
    key: Union[str, "bases.BaseExpression"],
) -> core.FunctionCall:
    """Check if key exists and has a non-NULL value.

    Args:
        dialect: The SQL dialect instance
        hstore_expr: The hstore expression
        key: The key to check

    Returns:
        FunctionCall for defined(hstore, key)
    """
    return core.FunctionCall(
        dialect, "defined",
        _convert_to_expression(dialect, hstore_expr),
        _convert_to_expression(dialect, key),
    )


# ============== Delete Functions ==============

def hstore_delete(
    dialect: "SQLDialectBase",
    hstore_expr: Union[PostgresHstore, Dict, str, "bases.BaseExpression"],
    key: Union[str, "bases.BaseExpression"],
) -> core.FunctionCall:
    """Delete a key from hstore.

    Args:
        dialect: The SQL dialect instance
        hstore_expr: The hstore expression
        key: The key to delete

    Returns:
        FunctionCall for delete(hstore, key)
    """
    return core.FunctionCall(
        dialect, "delete",
        _convert_to_expression(dialect, hstore_expr),
        _convert_to_expression(dialect, key),
    )


def hstore_delete_keys(
    dialect: "SQLDialectBase",
    hstore_expr: Union[PostgresHstore, Dict, str, "bases.BaseExpression"],
    keys: Union[str, "bases.BaseExpression"],
) -> core.FunctionCall:
    """Delete multiple keys from hstore.

    Args:
        dialect: The SQL dialect instance
        hstore_expr: The hstore expression
        keys: Array of keys to delete

    Returns:
        FunctionCall for delete(hstore, keys)
    """
    return core.FunctionCall(
        dialect, "delete",
        _convert_to_expression(dialect, hstore_expr),
        _convert_to_expression(dialect, keys),
    )


def hstore_delete_pairs(
    dialect: "SQLDialectBase",
    hstore_expr: Union[PostgresHstore, Dict, str, "bases.BaseExpression"],
    pairs: Union[PostgresHstore, Dict, str, "bases.BaseExpression"],
) -> core.FunctionCall:
    """Delete matching key/value pairs from hstore.

    Args:
        dialect: The SQL dialect instance
        hstore_expr: The hstore expression
        pairs: The hstore containing keys/values to delete

    Returns:
        FunctionCall for delete(hstore, hstore)
    """
    return core.FunctionCall(
        dialect, "delete",
        _convert_to_expression(dialect, hstore_expr),
        _convert_to_expression(dialect, pairs),
    )


# ============== Record Functions ==============

def hstore_populate_record(
    dialect: "SQLDialectBase",
    record: Union[str, "bases.BaseExpression"],
    hstore_expr: Union[PostgresHstore, Dict, str, "bases.BaseExpression"],
) -> core.FunctionCall:
    """Update a record/row with values from hstore.

    Args:
        dialect: The SQL dialect instance
        record: The record expression to update
        hstore_expr: The hstore containing field values

    Returns:
        FunctionCall for populate_record(record, hstore)
    """
    return core.FunctionCall(
        dialect, "populate_record",
        _convert_to_expression(dialect, record),
        _convert_to_expression(dialect, hstore_expr),
    )


# ============== Operators ==============

def hstore_get_value(
    dialect: "SQLDialectBase",
    hstore_expr: Union[PostgresHstore, Dict, str, "bases.BaseExpression"],
    key: Union[str, "bases.BaseExpression"],
) -> BinaryExpression:
    """Get value by key (operator ->).

    Returns the value associated with the key, or NULL if not present.

    Args:
        dialect: The SQL dialect instance
        hstore_expr: The hstore expression
        key: The key to access

    Returns:
        BinaryExpression for hstore -> key
    """
    return BinaryExpression(
        dialect, "->",
        _convert_to_expression(dialect, hstore_expr),
        _convert_to_expression(dialect, key),
    )


def hstore_get_value_as_text(
    dialect: "SQLDialectBase",
    hstore_expr: Union[PostgresHstore, Dict, str, "bases.BaseExpression"],
    key: Union[str, "bases.BaseExpression"],
) -> BinaryExpression:
    """Get value by key as text (operator ->>).

    Returns the value associated with the key as text, or NULL if not present.

    Args:
        dialect: The SQL dialect instance
        hstore_expr: The hstore expression
        key: The key to access

    Returns:
        BinaryExpression for hstore ->> key
    """
    return BinaryExpression(
        dialect, "->>",
        _convert_to_expression(dialect, hstore_expr),
        _convert_to_expression(dialect, key),
    )


def hstore_get_values(
    dialect: "SQLDialectBase",
    hstore_expr: Union[PostgresHstore, Dict, str, "bases.BaseExpression"],
    keys: Union[str, "bases.BaseExpression"],
) -> BinaryExpression:
    """Get values by multiple keys (operator -> text[]).

    Args:
        dialect: The SQL dialect instance
        hstore_expr: The hstore expression
        keys: Array of keys to access

    Returns:
        BinaryExpression for hstore -> text[]
    """
    return BinaryExpression(
        dialect, "->",
        _convert_to_expression(dialect, hstore_expr),
        _convert_to_expression(dialect, keys),
    )


def hstore_concat(
    dialect: "SQLDialectBase",
    left: Union[PostgresHstore, Dict, str, "bases.BaseExpression"],
    right: Union[PostgresHstore, Dict, str, "bases.BaseExpression"],
) -> BinaryExpression:
    """Concatenate two hstores (operator ||).

    Args:
        dialect: The SQL dialect instance
        left: Left hstore expression
        right: Right hstore expression

    Returns:
        BinaryExpression for hstore || hstore
    """
    return BinaryExpression(
        dialect, "||",
        _convert_to_expression(dialect, left),
        _convert_to_expression(dialect, right),
    )


def hstore_key_exists(
    dialect: "SQLDialectBase",
    hstore_expr: Union[PostgresHstore, Dict, str, "bases.BaseExpression"],
    key: Union[str, "bases.BaseExpression"],
) -> BinaryExpression:
    """Check if key exists (operator ?).

    Args:
        dialect: The SQL dialect instance
        hstore_expr: The hstore expression
        key: The key to check

    Returns:
        BinaryExpression for hstore ? key
    """
    return BinaryExpression(
        dialect, "?",
        _convert_to_expression(dialect, hstore_expr),
        _convert_to_expression(dialect, key),
    )


def hstore_all_keys_exist(
    dialect: "SQLDialectBase",
    hstore_expr: Union[PostgresHstore, Dict, str, "bases.BaseExpression"],
    keys: Union[str, "bases.BaseExpression"],
) -> BinaryExpression:
    """Check if all keys exist (operator ?&).

    Args:
        dialect: The SQL dialect instance
        hstore_expr: The hstore expression
        keys: Array of keys that must all exist

    Returns:
        BinaryExpression for hstore ?& keys
    """
    return BinaryExpression(
        dialect, "?&",
        _convert_to_expression(dialect, hstore_expr),
        _convert_to_expression(dialect, keys),
    )


def hstore_any_key_exists(
    dialect: "SQLDialectBase",
    hstore_expr: Union[PostgresHstore, Dict, str, "bases.BaseExpression"],
    keys: Union[str, "bases.BaseExpression"],
) -> BinaryExpression:
    """Check if any key exists (operator ?|).

    Args:
        dialect: The SQL dialect instance
        hstore_expr: The hstore expression
        keys: Array of keys where any must exist

    Returns:
        BinaryExpression for hstore ?| keys
    """
    return BinaryExpression(
        dialect, "?|",
        _convert_to_expression(dialect, hstore_expr),
        _convert_to_expression(dialect, keys),
    )


def hstore_contains(
    dialect: "SQLDialectBase",
    left: Union[PostgresHstore, Dict, str, "bases.BaseExpression"],
    right: Union[PostgresHstore, Dict, str, "bases.BaseExpression"],
) -> BinaryExpression:
    """Check if left hstore contains right (operator @>).

    Args:
        dialect: The SQL dialect instance
        left: Left hstore expression
        right: Right hstore expression

    Returns:
        BinaryExpression for hstore @> hstore
    """
    return BinaryExpression(
        dialect, "@>",
        _convert_to_expression(dialect, left),
        _convert_to_expression(dialect, right),
    )


def hstore_contained_by(
    dialect: "SQLDialectBase",
    left: Union[PostgresHstore, Dict, str, "bases.BaseExpression"],
    right: Union[PostgresHstore, Dict, str, "bases.BaseExpression"],
) -> BinaryExpression:
    """Check if left hstore is contained by right (operator <@).

    Args:
        dialect: The SQL dialect instance
        left: Left hstore expression
        right: Right hstore expression

    Returns:
        BinaryExpression for hstore <@ hstore
    """
    return BinaryExpression(
        dialect, "<@",
        _convert_to_expression(dialect, left),
        _convert_to_expression(dialect, right),
    )


def hstore_subtract_key(
    dialect: "SQLDialectBase",
    hstore_expr: Union[PostgresHstore, Dict, str, "bases.BaseExpression"],
    key: Union[str, "bases.BaseExpression"],
) -> BinaryExpression:
    """Delete key from hstore (operator - text).

    Args:
        dialect: The SQL dialect instance
        hstore_expr: The hstore expression
        key: The key to delete

    Returns:
        BinaryExpression for hstore - key
    """
    return BinaryExpression(
        dialect, "-",
        _convert_to_expression(dialect, hstore_expr),
        _convert_to_expression(dialect, key),
    )


def hstore_subtract_keys(
    dialect: "SQLDialectBase",
    hstore_expr: Union[PostgresHstore, Dict, str, "bases.BaseExpression"],
    keys: Union[str, "bases.BaseExpression"],
) -> BinaryExpression:
    """Delete multiple keys from hstore (operator - text[]).

    Args:
        dialect: The SQL dialect instance
        hstore_expr: The hstore expression
        keys: Array of keys to delete

    Returns:
        BinaryExpression for hstore - text[]
    """
    return BinaryExpression(
        dialect, "-",
        _convert_to_expression(dialect, hstore_expr),
        _convert_to_expression(dialect, keys),
    )


def hstore_subtract_pairs(
    dialect: "SQLDialectBase",
    left: Union[PostgresHstore, Dict, str, "bases.BaseExpression"],
    right: Union[PostgresHstore, Dict, str, "bases.BaseExpression"],
) -> BinaryExpression:
    """Delete matching pairs from hstore (operator - hstore).

    Args:
        dialect: The SQL dialect instance
        left: Left hstore expression
        right: Right hstore containing pairs to delete

    Returns:
        BinaryExpression for hstore - hstore
    """
    return BinaryExpression(
        dialect, "-",
        _convert_to_expression(dialect, left),
        _convert_to_expression(dialect, right),
    )


def hstore_to_array_operator(
    dialect: "SQLDialectBase",
    hstore_expr: Union[PostgresHstore, Dict, str, "bases.BaseExpression"],
) -> BinaryExpression:
    """Convert hstore to alternating key/value array (operator %%).

    Note: This is a unary-style operator in PostgreSQL. We represent it
    as a BinaryExpression with %% as the operator since PostgreSQL
    treats it as a prefix operator.

    Args:
        dialect: The SQL dialect instance
        hstore_expr: The hstore expression

    Returns:
        BinaryExpression for %%hstore
    """
    return BinaryExpression(
        dialect, "%%",
        core.Literal(dialect, ""),
        _convert_to_expression(dialect, hstore_expr),
    )


def hstore_to_matrix_operator(
    dialect: "SQLDialectBase",
    hstore_expr: Union[PostgresHstore, Dict, str, "bases.BaseExpression"],
) -> BinaryExpression:
    """Convert hstore to 2D key/value array (operator %#).

    Note: This is a unary-style operator in PostgreSQL. We represent it
    as a BinaryExpression with %# as the operator since PostgreSQL
    treats it as a prefix operator.

    Args:
        dialect: The SQL dialect instance
        hstore_expr: The hstore expression

    Returns:
        BinaryExpression for %#hstore
    """
    return BinaryExpression(
        dialect, "%#",
        core.Literal(dialect, ""),
        _convert_to_expression(dialect, hstore_expr),
    )


def hstore_record_update(
    dialect: "SQLDialectBase",
    record: Union[str, "bases.BaseExpression"],
    hstore_expr: Union[PostgresHstore, Dict, str, "bases.BaseExpression"],
) -> BinaryExpression:
    """Update record fields from hstore (operator #=).

    Args:
        dialect: The SQL dialect instance
        record: The record expression to update
        hstore_expr: The hstore containing field values

    Returns:
        BinaryExpression for record #= hstore
    """
    return BinaryExpression(
        dialect, "#=",
        _convert_to_expression(dialect, record),
        _convert_to_expression(dialect, hstore_expr),
    )


# ============== Subscript Access ==============

def hstore_subscript_get(
    dialect: "SQLDialectBase",
    hstore_expr: Union[PostgresHstore, Dict, str, "bases.BaseExpression"],
    key: Union[str, "bases.BaseExpression"],
) -> core.FunctionCall:
    """Get value by key using subscript syntax.

    Note: Subscript access (hstore['key']) is equivalent to the -> operator
    in PostgreSQL. This function generates the -> operator expression
    since subscript syntax is not directly representable in the Expression
    system.

    Args:
        dialect: The SQL dialect instance
        hstore_expr: The hstore expression
        key: The key to access

    Returns:
        FunctionCall representing subscript access (using -> operator)
    """
    return hstore_get_value(dialect, hstore_expr, key)


def hstore_subscript_set(
    dialect: "SQLDialectBase",
    hstore_expr: Union[PostgresHstore, Dict, str, "bases.BaseExpression"],
    key: Union[str, "bases.BaseExpression"],
    value: Union[str, "bases.BaseExpression"],
) -> core.FunctionCall:
    """Set value by key using subscript syntax.

    Generates an expression equivalent to hstore[key] = value using
    the hstore concatenation operator (||) to set a single key.

    Args:
        dialect: The SQL dialect instance
        hstore_expr: The hstore expression
        key: The key to set
        value: The value to set

    Returns:
        FunctionCall representing the update expression
    """
    single_pair = hstore_from_key_value(dialect, key, value)
    return hstore_concat(dialect, hstore_expr, single_pair)


__all__ = [
    # Constructors
    "hstore_from_record",
    "hstore_from_key_value",
    # Key/Value Extraction
    "hstore_akeys",
    "hstore_skeys",
    "hstore_avals",
    "hstore_svals",
    "hstore_each",
    # Conversion
    "hstore_to_array",
    "hstore_to_matrix",
    "hstore_to_json",
    "hstore_to_jsonb",
    "hstore_to_json_loose",
    "hstore_to_jsonb_loose",
    # Subset
    "hstore_slice",
    # Existence
    "hstore_exist",
    "hstore_defined",
    # Delete
    "hstore_delete",
    "hstore_delete_keys",
    "hstore_delete_pairs",
    # Record
    "hstore_populate_record",
    # Operators
    "hstore_get_value",
    "hstore_get_value_as_text",
    "hstore_get_values",
    "hstore_concat",
    "hstore_key_exists",
    "hstore_all_keys_exist",
    "hstore_any_key_exists",
    "hstore_contains",
    "hstore_contained_by",
    "hstore_subtract_key",
    "hstore_subtract_keys",
    "hstore_subtract_pairs",
    "hstore_to_array_operator",
    "hstore_to_matrix_operator",
    "hstore_record_update",
    # Subscript
    "hstore_subscript_get",
    "hstore_subscript_set",
]
