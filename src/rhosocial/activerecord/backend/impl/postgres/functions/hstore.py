# src/rhosocial/activerecord/backend/impl/postgres/functions/hstore.py
"""
PostgreSQL hstore functions for SQL expression generation.

This module provides utility functions for generating PostgreSQL hstore SQL expressions.

hstore is a PostgreSQL extension that stores key/value pairs in a single value.

PostgreSQL Documentation: https://www.postgresql.org/docs/current/hstore.html

Supported functions:
- Constructors: hstore(record), hstore(text, text)
- Key/Value Extraction: akeys, skeys, avals, svals, each
- Conversion: hstore_to_array, hstore_to_matrix, hstore_to_json, hstore_to_jsonb
- Subsets: slice
- Existence: exist, defined
- Delete: delete
- Record: populate_record

Supported operators:
- -> text        - Access single key
- -> text[]      - Access multiple keys
- ||             - Concatenate
- ?              - Key exists
- ?&             - All keys exist
- ?|             - Any key exists
- @>             - Contains
- <@             - Contained by
- - text         - Delete by key
- - text[]       - Delete by keys
- - hstore       - Delete matching pairs
- %%             - To array
- %#             - To matrix
- #=             - Record update
"""

from typing import Any


def _to_sql(expr: Any) -> str:
    """Convert an expression to its SQL string representation."""
    if hasattr(expr, 'to_sql'):
        return expr.to_sql()[0]
    return str(expr)


# ============== Constructors ==============

def hstore_from_record(record: str) -> str:
    """
    Construct hstore from a record/row.

    Args:
        record: The record expression (e.g., 'ROW(1,2)' or table column)

    Returns:
        SQL expression: hstore(record)

    Example:
        >>> hstore_from_record("ROW(1,2)")
        "hstore(ROW(1,2))"
    """
    return f"hstore({record})"


def hstore_from_key_value(key: str, value: str) -> str:
    """
    Construct hstore from a single key/value pair.

    Args:
        key: The key string
        value: The value string

    Returns:
        SQL expression: hstore(key, value)

    Example:
        >>> hstore_from_key_value("'name'", "'value'")
        "hstore('name', 'value')"
    """
    return f"hstore({key}, {value})"


# ============== Key/Value Extraction ==============

def akeys(hstore_expr: str) -> str:
    """
    Get all keys from hstore as a text array.

    Args:
        hstore_expr: The hstore expression

    Returns:
        SQL expression: akeys(hstore)

    Example:
        >>> akeys("data")
        "akeys(data)"
    """
    return f"akeys({hstore_expr})"


def skeys(hstore_expr: str) -> str:
    """
    Get all keys from hstore as a set.

    Args:
        hstore_expr: The hstore expression

    Returns:
        SQL expression: skeys(hstore)

    Example:
        >>> skeys("data")
        "skeys(data)"
    """
    return f"skeys({hstore_expr})"


def avals(hstore_expr: str) -> str:
    """
    Get all values from hstore as a text array.

    Args:
        hstore_expr: The hstore expression

    Returns:
        SQL expression: avals(hstore)

    Example:
        >>> avals("data")
        "avals(data)"
    """
    return f"avals({hstore_expr})"


def svals(hstore_expr: str) -> str:
    """
    Get all values from hstore as a set.

    Args:
        hstore_expr: The hstore expression

    Returns:
        SQL expression: svals(hstore)

    Example:
        >>> svals("data")
        "svals(data)"
    """
    return f"svals({hstore_expr})"


def each(hstore_expr: str) -> str:
    """
    Get all key/value pairs from hstore as a set of (key, value) records.

    Args:
        hstore_expr: The hstore expression

    Returns:
        SQL expression: each(hstore)

    Example:
        >>> each("data")
        "each(data)"
    """
    return f"each({hstore_expr})"


# ============== Conversion Functions ==============

def hstore_to_array(hstore_expr: str) -> str:
    """
    Convert hstore to text array (alternating keys and values).

    Args:
        hstore_expr: The hstore expression

    Returns:
        SQL expression: hstore_to_array(hstore)

    Example:
        >>> hstore_to_array("data")
        "hstore_to_array(data)"
    """
    return f"hstore_to_array({hstore_expr})"


def hstore_to_matrix(hstore_expr: str) -> str:
    """
    Convert hstore to 2D text array (key/value pairs as rows).

    Args:
        hstore_expr: The hstore expression

    Returns:
        SQL expression: hstore_to_matrix(hstore)

    Example:
        >>> hstore_to_matrix("data")
        "hstore_to_matrix(data)"
    """
    return f"hstore_to_matrix({hstore_expr})"


def hstore_to_json(hstore_expr: str) -> str:
    """
    Convert hstore to JSON (values as strings).

    Args:
        hstore_expr: The hstore expression

    Returns:
        SQL expression: hstore_to_json(hstore)

    Example:
        >>> hstore_to_json("data")
        "hstore_to_json(data)"
    """
    return f"hstore_to_json({hstore_expr})"


def hstore_to_jsonb(hstore_expr: str) -> str:
    """
    Convert hstore to JSONB (values as strings).

    Args:
        hstore_expr: The hstore expression

    Returns:
        SQL expression: hstore_to_jsonb(hstore)

    Example:
        >>> hstore_to_jsonb("data")
        "hstore_to_jsonb(data)"
    """
    return f"hstore_to_jsonb({hstore_expr})"


def hstore_to_json_loose(hstore_expr: str) -> str:
    """
    Convert hstore to JSON with loose type inference.

    Attempts to infer proper JSON types for values (numeric, boolean, etc).

    Args:
        hstore_expr: The hstore expression

    Returns:
        SQL expression: hstore_to_json_loose(hstore)

    Example:
        >>> hstore_to_json_loose("data")
        "hstore_to_json_loose(data)"
    """
    return f"hstore_to_json_loose({hstore_expr})"


def hstore_to_jsonb_loose(hstore_expr: str) -> str:
    """
    Convert hstore to JSONB with loose type inference.

    Attempts to infer proper JSON types for values (numeric, boolean, etc).

    Args:
        hstore_expr: The hstore expression

    Returns:
        SQL expression: hstore_to_jsonb_loose(hstore)

    Example:
        >>> hstore_to_jsonb_loose("data")
        "hstore_to_jsonb_loose(data)"
    """
    return f"hstore_to_jsonb_loose({hstore_expr})"


# ============== Subset Functions ==============

def hstore_slice(hstore_expr: str, keys: str) -> str:
    """
    Extract subset of hstore by keys.

    Args:
        hstore_expr: The hstore expression
        keys: Array of keys to extract (e.g., "ARRAY['a', 'b']")

    Returns:
        SQL expression: slice(hstore, keys)

    Example:
        >>> hstore_slice("data", "ARRAY['a', 'b']")
        "slice(data, ARRAY['a', 'b'])"
    """
    return f"slice({hstore_expr}, {keys})"


# ============== Existence Functions ==============

def hstore_exist(hstore_expr: str, key: str) -> str:
    """
    Check if key exists in hstore (including NULL values).

    Args:
        hstore_expr: The hstore expression
        key: The key to check

    Returns:
        SQL expression: exist(hstore, key)

    Example:
        >>> hstore_exist("data", "'name'")
        "exist(data, 'name')"
    """
    return f"exist({hstore_expr}, {key})"


def hstore_defined(hstore_expr: str, key: str) -> str:
    """
    Check if key exists and has a non-NULL value.

    Args:
        hstore_expr: The hstore expression
        key: The key to check

    Returns:
        SQL expression: defined(hstore, key)

    Example:
        >>> hstore_defined("data", "'name'")
        "defined(data, 'name')"
    """
    return f"defined({hstore_expr}, {key})"


# ============== Delete Functions ==============

def hstore_delete(hstore_expr: str, key: str) -> str:
    """
    Delete a key from hstore.

    Args:
        hstore_expr: The hstore expression
        key: The key to delete

    Returns:
        SQL expression: delete(hstore, key)

    Example:
        >>> hstore_delete("data", "'name'")
        "delete(data, 'name')"
    """
    return f"delete({hstore_expr}, {key})"


def hstore_delete_keys(hstore_expr: str, keys: str) -> str:
    """
    Delete multiple keys from hstore.

    Args:
        hstore_expr: The hstore expression
        keys: Array of keys to delete (e.g., "ARRAY['a', 'b']")

    Returns:
        SQL expression: delete(hstore, keys)

    Example:
        >>> hstore_delete_keys("data", "ARRAY['name', 'age']")
        "delete(data, ARRAY['name', 'age'])"
    """
    return f"delete({hstore_expr}, {keys})"


def hstore_delete_pairs(hstore_expr: str, pairs: str) -> str:
    """
    Delete matching key/value pairs from hstore.

    Args:
        hstore_expr: The hstore expression
        pairs: The hstore containing keys/values to delete

    Returns:
        SQL expression: delete(hstore, hstore)

    Example:
        >>> hstore_delete_pairs("data", "'a=>1'::hstore")
        "delete(data, 'a=>1'::hstore)"
    """
    return f"delete({hstore_expr}, {pairs})"


# ============== Record Functions ==============

def hstore_populate_record(record: str, hstore_expr: str) -> str:
    """
    Update a record/row with values from hstore.

    Args:
        record: The record expression to update
        hstore_expr: The hstore containing field values

    Returns:
        SQL expression: populate_record(record, hstore)

    Example:
        >>> hstore_populate_record("ROW(1,2)", "'f1=>42'::hstore")
        "populate_record(ROW(1,2), 'f1=>42'::hstore)"
    """
    return f"populate_record({record}, {hstore_expr})"


# ============== Operators ==============

def hstore_get_value(hstore_expr: str, key: str) -> str:
    """
    Get value by key (operator ->).

    Args:
        hstore_expr: The hstore expression
        key: The key to access

    Returns:
        SQL expression: hstore -> key

    Example:
        >>> hstore_get_value("data", "'name'")
        "data -> 'name'"
    """
    return f"{hstore_expr} -> {key}"


def hstore_get_values(hstore_expr: str, keys: str) -> str:
    """
    Get values by multiple keys (operator -> text[]).

    Args:
        hstore_expr: The hstore expression
        keys: Array of keys to access

    Returns:
        SQL expression: hstore -> text[]

    Example:
        >>> hstore_get_values("data", "ARRAY['name', 'age']")
        "data -> ARRAY['name', 'age']"
    """
    return f"{hstore_expr} -> {keys}"


def hstore_concat(left: str, right: str) -> str:
    """
    Concatenate two hstores (operator ||).

    Args:
        left: Left hstore expression
        right: Right hstore expression

    Returns:
        SQL expression: hstore || hstore

    Example:
        >>> hstore_concat("'a=>1'::hstore", "'b=>2'::hstore")
        "'a=>1'::hstore || 'b=>2'::hstore"
    """
    return f"{left} || {right}"


def hstore_key_exists(hstore_expr: str, key: str) -> str:
    """
    Check if key exists (operator ?).

    Args:
        hstore_expr: The hstore expression
        key: The key to check

    Returns:
        SQL expression: hstore ? key

    Example:
        >>> hstore_key_exists("data", "'name'")
        "data ? 'name'"
    """
    return f"{hstore_expr} ? {key}"


def hstore_all_keys_exist(hstore_expr: str, keys: str) -> str:
    """
    Check if all keys exist (operator ?&).

    Args:
        hstore_expr: The hstore expression
        keys: Array of keys that must all exist

    Returns:
        SQL expression: hstore ?& keys

    Example:
        >>> hstore_all_keys_exist("data", "ARRAY['a', 'b']")
        "data ?& ARRAY['a', 'b']"
    """
    return f"{hstore_expr} ?& {keys}"


def hstore_any_key_exists(hstore_expr: str, keys: str) -> str:
    """
    Check if any key exists (operator ?|).

    Args:
        hstore_expr: The hstore expression
        keys: Array of keys where any must exist

    Returns:
        SQL expression: hstore ?| keys

    Example:
        >>> hstore_any_key_exists("data", "ARRAY['a', 'b']")
        "data ?| ARRAY['a', 'b']"
    """
    return f"{hstore_expr} ?| {keys}"


def hstore_contains(left: str, right: str) -> str:
    """
    Check if left hstore contains right (operator @>).

    Args:
        left: Left hstore expression
        right: Right hstore expression

    Returns:
        SQL expression: hstore @> hstore

    Example:
        >>> hstore_contains("data", "'a=>1'")
        "data @> 'a=>1'"
    """
    return f"{left} @> {right}"


def hstore_contained_by(left: str, right: str) -> str:
    """
    Check if left hstore is contained by right (operator <@).

    Args:
        left: Left hstore expression
        right: Right hstore expression

    Returns:
        SQL expression: hstore <@ hstore

    Example:
        >>> hstore_contained_by("'a=>1'", "data")
        "'a=>1' <@ data"
    """
    return f"{left} <@ {right}"


def hstore_subtract_key(hstore_expr: str, key: str) -> str:
    """
    Delete key from hstore (operator - text).

    Args:
        hstore_expr: The hstore expression
        key: The key to delete

    Returns:
        SQL expression: hstore - key

    Example:
        >>> hstore_subtract_key("data", "'name'")
        "data - 'name'"
    """
    return f"{hstore_expr} - {key}"


def hstore_subtract_keys(hstore_expr: str, keys: str) -> str:
    """
    Delete multiple keys from hstore (operator - text[]).

    Args:
        hstore_expr: The hstore expression
        keys: Array of keys to delete

    Returns:
        SQL expression: hstore - text[]

    Example:
        >>> hstore_subtract_keys("data", "ARRAY['a', 'b']")
        "data - ARRAY['a', 'b']"
    """
    return f"{hstore_expr} - {keys}"


def hstore_subtract_pairs(left: str, right: str) -> str:
    """
    Delete matching pairs from hstore (operator - hstore).

    Args:
        left: Left hstore expression
        right: Right hstore containing pairs to delete

    Returns:
        SQL expression: hstore - hstore

    Example:
        >>> hstore_subtract_pairs("data", "'a=>1'::hstore")
        "data - 'a=>1'::hstore"
    """
    return f"{left} - {right}"


def hstore_to_array_operator(hstore_expr: str) -> str:
    """
    Convert hstore to alternating key/value array (operator %%).

    Args:
        hstore_expr: The hstore expression

    Returns:
        SQL expression: %%hstore

    Example:
        >>> hstore_to_array_operator("data")
        "%%data"
    """
    return f"%%{hstore_expr}"


def hstore_to_matrix_operator(hstore_expr: str) -> str:
    """
    Convert hstore to 2D key/value array (operator %#).

    Args:
        hstore_expr: The hstore expression

    Returns:
        SQL expression: %#hstore

    Example:
        >>> hstore_to_matrix_operator("data")
        "%#data"
    """
    return f"%#{hstore_expr}"


def hstore_record_update(record: str, hstore_expr: str) -> str:
    """
    Update record fields from hstore (operator #=).

    Args:
        record: The record expression to update
        hstore_expr: The hstore containing field values

    Returns:
        SQL expression: record #= hstore

    Example:
        >>> record_update("ROW(1,2)", "'f1=>42'::hstore")
        "ROW(1,2) #= 'f1=>42'::hstore"
    """
    return f"{record} #= {hstore_expr}"


# ============== Subscript Access ==============

def hstore_subscript_get(hstore_expr: str, key: str) -> str:
    """
    Get value by key using subscript syntax.

    Args:
        hstore_expr: The hstore expression
        key: The key to access

    Returns:
        SQL expression: hstore[key]

    Example:
        >>> hstore_subscript_get("data", "'name'")
        "data['name']"
    """
    return f"{hstore_expr}[{key}]"


def hstore_subscript_set(hstore_expr: str, key: str, value: str) -> str:
    """
    Set value by key using subscript syntax.

    Args:
        hstore_expr: The hstore expression
        key: The key to set
        value: The value to set

    Returns:
        SQL expression: hstore[key] = value

    Example:
        >>> hstore_subscript_set("data", "'name'", "'value'")
        "data['name'] = 'value'"
    """
    return f"{hstore_expr}[{key}] = {value}"


__all__ = [
    # Constructors
    "hstore_from_record",
    "hstore_from_key_value",
    # Key/Value Extraction
    "akeys",
    "skeys",
    "avals",
    "svals",
    "each",
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
