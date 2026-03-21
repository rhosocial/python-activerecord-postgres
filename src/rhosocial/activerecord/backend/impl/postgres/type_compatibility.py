# src/rhosocial/activerecord/backend/impl/postgres/type_compatibility.py
"""PostgreSQL type casting compatibility matrix.

This module provides type casting compatibility information and warnings
for PostgreSQL type conversions. It helps users understand when type
conversions may require intermediate types or produce unexpected results.

PostgreSQL Documentation:
- https://www.postgresql.org/docs/current/typeconv.html
- https://www.postgresql.org/docs/current/datatype.html

Type Casting Rules:
1. PostgreSQL allows most type conversion attempts
2. Some conversions require intermediate types (e.g., money -> numeric -> float8)
3. Incompatible conversions will raise errors at runtime in PostgreSQL
4. This module provides warnings to help users avoid potential issues

Example:
    >>> from rhosocial.activerecord.backend.impl.postgres.type_compatibility import (
    ...     check_cast_compatibility
    ... )
    >>> import warnings
    >>> warnings.simplefilter('always')
    >>> check_cast_compatibility('money', 'float8')
    UserWarning: Type cast from 'money' to 'float8' may require intermediate
    conversion. Consider: money::numeric::float8
"""

import warnings
from typing import Set, Tuple, Optional, Dict


# Directly compatible type casts (no warning needed)
# Format: (source_type_lowercase, target_type_lowercase)
DIRECT_COMPATIBLE_CASTS: Set[Tuple[str, str]] = {
    # Same type conversions (identity)
    ('money', 'money'),
    ('numeric', 'numeric'),
    ('integer', 'integer'),
    ('bigint', 'bigint'),
    ('smallint', 'smallint'),
    ('real', 'real'),
    ('double precision', 'double precision'),
    ('float4', 'float4'),
    ('float8', 'float8'),
    ('text', 'text'),
    ('varchar', 'varchar'),
    ('boolean', 'boolean'),
    ('bool', 'bool'),
    ('json', 'json'),
    ('jsonb', 'jsonb'),
    ('date', 'date'),
    ('timestamp', 'timestamp'),
    ('timestamptz', 'timestamptz'),

    # Numeric type widening
    ('smallint', 'integer'),
    ('smallint', 'bigint'),
    ('smallint', 'numeric'),
    ('smallint', 'real'),
    ('smallint', 'double precision'),
    ('integer', 'bigint'),
    ('integer', 'numeric'),
    ('integer', 'real'),
    ('integer', 'double precision'),
    ('bigint', 'numeric'),
    ('real', 'double precision'),
    ('float4', 'float8'),
    ('real', 'float8'),
    ('float4', 'double precision'),

    # Numeric to/from text
    ('integer', 'text'),
    ('bigint', 'text'),
    ('smallint', 'text'),
    ('numeric', 'text'),
    ('real', 'text'),
    ('double precision', 'text'),

    # Money <-> Numeric (directly compatible)
    ('money', 'numeric'),
    ('numeric', 'money'),

    # Numeric to numeric types
    ('numeric', 'integer'),
    ('numeric', 'bigint'),
    ('numeric', 'smallint'),
    ('numeric', 'real'),
    ('numeric', 'double precision'),
    ('numeric', 'float4'),
    ('numeric', 'float8'),

    # Text type conversions
    ('text', 'varchar'),
    ('varchar', 'text'),
    ('char', 'text'),
    ('char', 'varchar'),
    ('text', 'char'),
    ('varchar', 'char'),

    # Temporal type conversions
    ('date', 'timestamp'),
    ('date', 'timestamptz'),
    ('timestamp', 'timestamptz'),
    ('timestamptz', 'timestamp'),
    ('timestamp', 'date'),
    ('timestamptz', 'date'),

    # JSON conversions
    ('json', 'jsonb'),
    ('jsonb', 'json'),
    ('json', 'text'),
    ('jsonb', 'text'),
    ('text', 'json'),
    ('text', 'jsonb'),

    # Boolean conversions
    ('boolean', 'text'),
    ('bool', 'text'),
    ('text', 'boolean'),
    ('text', 'bool'),

    # UUID conversions
    ('uuid', 'text'),
    ('text', 'uuid'),

    # Network address conversions
    ('inet', 'text'),
    ('cidr', 'text'),
    ('text', 'inet'),
    ('text', 'cidr'),
    ('inet', 'cidr'),
    ('macaddr', 'text'),
    ('macaddr8', 'text'),
    ('text', 'macaddr'),
    ('text', 'macaddr8'),
    ('macaddr', 'macaddr8'),
    ('macaddr8', 'macaddr'),
}

# Type casts that require a warning
# PostgreSQL will attempt these but they may:
# 1. Require intermediate types
# 2. Produce unexpected results
# 3. Lose precision or information
WARNED_CASTS: Set[Tuple[str, str]] = {
    # Money to float requires numeric intermediate
    ('money', 'real'),
    ('money', 'double precision'),
    ('money', 'float4'),
    ('money', 'float8'),

    # Money to integer requires numeric intermediate
    ('money', 'integer'),
    ('money', 'bigint'),
    ('money', 'smallint'),

    # Boolean to numeric (unusual conversion)
    ('boolean', 'integer'),
    ('boolean', 'bigint'),
    ('boolean', 'smallint'),
    ('bool', 'integer'),
    ('bool', 'bigint'),
    ('bool', 'smallint'),

    # Numeric to boolean (may be unexpected)
    ('integer', 'boolean'),
    ('bigint', 'boolean'),
    ('smallint', 'boolean'),
    ('integer', 'bool'),
    ('bigint', 'bool'),
    ('smallint', 'bool'),
    ('numeric', 'boolean'),
    ('numeric', 'bool'),

    # Float to money (precision concerns)
    ('real', 'money'),
    ('double precision', 'money'),
    ('float4', 'money'),
    ('float8', 'money'),

    # Integer to money (precision concerns)
    ('integer', 'money'),
    ('bigint', 'money'),
    ('smallint', 'money'),

    # JSON to numeric types (requires proper JSON content)
    ('json', 'integer'),
    ('json', 'bigint'),
    ('json', 'numeric'),
    ('jsonb', 'integer'),
    ('jsonb', 'bigint'),
    ('jsonb', 'numeric'),

    # Timestamp precision changes
    ('timestamptz', 'time'),
    ('timestamp', 'time'),

    # ===== New: Time precision loss =====
    # Note: timestamptz->timestamp and timestamptz->date are in DIRECT_COMPATIBLE_CASTS
    # so they won't produce warnings. Only time component loss is warned.
    ('timestamp', 'time'),           # Date component lost
    ('timestamptz', 'time'),         # Date + timezone lost

    # ===== New: Numeric precision loss =====
    ('double precision', 'real'),    # Precision loss (15 -> 6 significant digits)
    ('float8', 'float4'),            # Same as above
    ('bigint', 'integer'),           # May overflow
    ('bigint', 'smallint'),          # More likely to overflow
    ('integer', 'smallint'),         # May overflow

    # ===== New: JSON type risks =====
    ('jsonb', 'varchar'),            # Structure lost + may truncate
    ('json', 'varchar'),             # Structure lost + may truncate

    # ===== New: UUID type risks =====
    ('uuid', 'varchar'),             # May truncate (UUID needs at least 36 chars)
    ('uuid', 'char'),                # Will truncate (char is typically 1 char)
}

# Intermediate type suggestions for problematic casts
# Format: (source, target) -> suggested_intermediate_type
INTERMEDIATE_SUGGESTIONS: Dict[Tuple[str, str], str] = {
    # Money to float types
    ('money', 'real'): 'numeric',
    ('money', 'double precision'): 'numeric',
    ('money', 'float4'): 'numeric',
    ('money', 'float8'): 'numeric',

    # Money to integer types
    ('money', 'integer'): 'numeric',
    ('money', 'bigint'): 'numeric',
    ('money', 'smallint'): 'numeric',

    # ===== New: Numeric type conversion suggestions =====
    ('bigint', 'integer'): 'numeric',  # Use numeric to avoid overflow
    ('bigint', 'smallint'): 'numeric',
    ('integer', 'smallint'): 'numeric',

    # ===== New: JSON type conversion suggestions =====
    ('jsonb', 'varchar'): 'text',
    ('json', 'varchar'): 'text',
}

# Type categories for broader compatibility checks
# Based on PostgreSQL's pg_type.typcategory
TYPE_CATEGORIES: Dict[str, str] = {
    # Numeric types
    'smallint': 'N',
    'integer': 'N',
    'bigint': 'N',
    'decimal': 'N',
    'numeric': 'N',
    'real': 'N',
    'double precision': 'N',
    'float4': 'N',
    'float8': 'N',
    'serial': 'N',
    'bigserial': 'N',
    'smallserial': 'N',

    # Monetary types
    'money': 'N',

    # String types
    'text': 'S',
    'varchar': 'S',
    'char': 'S',
    'character': 'S',
    'character varying': 'S',
    'name': 'S',

    # Boolean
    'boolean': 'B',
    'bool': 'B',

    # Date/Time
    'date': 'D',
    'time': 'D',
    'timetz': 'D',
    'timestamp': 'D',
    'timestamptz': 'D',
    'interval': 'T',

    # Geometric
    'point': 'G',
    'line': 'G',
    'lseg': 'G',
    'box': 'G',
    'path': 'G',
    'polygon': 'G',
    'circle': 'G',

    # Network
    'inet': 'I',
    'cidr': 'I',
    'macaddr': 'I',
    'macaddr8': 'I',

    # Bit string
    'bit': 'V',
    'varbit': 'V',

    # JSON
    'json': 'U',
    'jsonb': 'U',

    # UUID
    'uuid': 'U',

    # Array types
    'array': 'A',

    # Enum (user-defined)
    'enum': 'E',

    # Range types (built-in)
    'int4range': 'R',
    'int8range': 'R',
    'numrange': 'R',
    'tsrange': 'R',
    'tstzrange': 'R',
    'daterange': 'R',

    # ===== New: Multirange types (PostgreSQL 14+) =====
    'int4multirange': 'R',
    'int8multirange': 'R',
    'nummultirange': 'R',
    'tsmultirange': 'R',
    'tstzmultirange': 'R',
    'datemultirange': 'R',

    # ===== New: Pseudo-types =====
    'anyelement': 'P',
    'anyarray': 'P',
    'anynonarray': 'P',
    'anyenum': 'P',
    'anyrange': 'P',
    'anymultirange': 'P',
    'void': 'P',
    'trigger': 'P',
    'event_trigger': 'P',
    'pg_lsn': 'P',
    'pg_lsn_diff': 'P',
    'txid_snapshot': 'P',
    'pg_snapshot': 'P',

    # ===== New: Additional JSON types =====
    'jsonpath': 'U',

    # ===== New: Full-text search types =====
    'tsvector': 'U',
    'tsquery': 'U',

    # ===== New: Object identifier types =====
    'oid': 'N',
    'regclass': 'N',
    'regtype': 'N',
    'regproc': 'N',
    'regnamespace': 'N',
    'regrole': 'N',
    'xid': 'N',
    'cid': 'N',
    'tid': 'N',
}


def check_cast_compatibility(source_type: Optional[str], target_type: str) -> bool:
    """Check type cast compatibility and emit warnings for problematic casts.

    This function checks if a type conversion is likely to succeed and emits
    warnings for conversions that may require intermediate types or produce
    unexpected results.

    Args:
        source_type: Source type name (may be None for expressions without type info)
        target_type: Target type name

    Returns:
        Always returns True (PostgreSQL will attempt the conversion)

    Warns:
        UserWarning: When the cast may require intermediate conversion or
            produce unexpected results

    Example:
        >>> import warnings
        >>> warnings.simplefilter('always')
        >>> check_cast_compatibility('money', 'float8')
        UserWarning: Type cast from 'money' to 'float8' may require
        intermediate conversion. Consider: money::numeric::float8

    Note:
        PostgreSQL allows most type conversion attempts. This function
        provides guidance but does not prevent any conversion.
    """
    if source_type is None:
        return True

    source_lower = source_type.lower()
    target_lower = target_type.lower()

    # Same type - no warning
    if source_lower == target_lower:
        return True

    # Directly compatible - no warning
    if (source_lower, target_lower) in DIRECT_COMPATIBLE_CASTS:
        return True

    # Check if this is a warned cast
    if (source_lower, target_lower) in WARNED_CASTS:
        intermediate = INTERMEDIATE_SUGGESTIONS.get((source_lower, target_lower))

        if intermediate:
            warnings.warn(
                f"Type cast from '{source_type}' to '{target_type}' may require "
                f"intermediate conversion. Consider: "
                f"{source_type}::{intermediate}::{target_type}",
                UserWarning,
                stacklevel=3
            )
        else:
            warnings.warn(
                f"Type cast from '{source_type}' to '{target_type}' may produce "
                f"unexpected results or lose precision.",
                UserWarning,
                stacklevel=3
            )

    return True


def get_compatible_types(source_type: str) -> Set[str]:
    """Get set of directly compatible target types for a source type.

    Args:
        source_type: Source type name

    Returns:
        Set of type names that can be directly converted to without warning

    Example:
        >>> get_compatible_types('money')
        {'money', 'numeric'}
    """
    source_lower = source_type.lower()
    return {
        target
        for (source, target) in DIRECT_COMPATIBLE_CASTS
        if source == source_lower
    }


def get_intermediate_type(source_type: str, target_type: str) -> Optional[str]:
    """Get suggested intermediate type for a problematic cast.

    Args:
        source_type: Source type name
        target_type: Target type name

    Returns:
        Intermediate type name if a suggestion exists, None otherwise

    Example:
        >>> get_intermediate_type('money', 'float8')
        'numeric'
    """
    key = (source_type.lower(), target_type.lower())
    return INTERMEDIATE_SUGGESTIONS.get(key)


def get_type_category(type_name: str) -> Optional[str]:
    """Get the category of a PostgreSQL type.

    Args:
        type_name: Type name

    Returns:
        Single-character category code or None if unknown

    Category codes:
        A: Array types
        B: Boolean types
        C: Composite types
        D: Date/time types
        E: Enum types
        G: Geometric types
        I: Network address types
        N: Numeric types
        P: Pseudo-types
        R: Range types
        S: String types
        T: Timespan types
        U: User-defined types
        V: Bit string types
        X: Unknown type
    """
    return TYPE_CATEGORIES.get(type_name.lower())


def is_same_category(type1: str, type2: str) -> bool:
    """Check if two types belong to the same category.

    Args:
        type1: First type name
        type2: Second type name

    Returns:
        True if both types are in the same category
    """
    cat1 = get_type_category(type1)
    cat2 = get_type_category(type2)
    return cat1 is not None and cat1 == cat2


__all__ = [
    'DIRECT_COMPATIBLE_CASTS',
    'WARNED_CASTS',
    'INTERMEDIATE_SUGGESTIONS',
    'TYPE_CATEGORIES',
    'check_cast_compatibility',
    'get_compatible_types',
    'get_intermediate_type',
    'get_type_category',
    'is_same_category',
]
