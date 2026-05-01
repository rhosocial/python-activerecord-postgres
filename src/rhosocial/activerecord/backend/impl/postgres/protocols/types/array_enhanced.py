# src/rhosocial/activerecord/backend/impl/postgres/protocols/types/array_enhanced.py
"""PostgreSQL array enhanced support protocol definition.

This module defines the protocol for PostgreSQL-specific array
enhancements beyond basic array support.
"""

from typing import Protocol, runtime_checkable


@runtime_checkable
class PostgresArrayEnhancedSupport(Protocol):
    """PostgreSQL array enhanced support protocol.

    PostgreSQL arrays are a native data type that can hold multiple
    values of the same type. This protocol covers PostgreSQL-specific
    array capabilities beyond basic array operations.

    Array enhanced features:
    - Native array type and ARRAY constructor (all versions)
    - Array subscript access (all versions)
    - array_fill function (PG 8.4+)
    - array_position / array_positions functions (PG 9.5+)
    - Domain arrays (PG 11+)

    Feature Source: Native support (no extension required)

    Official Documentation:
    https://www.postgresql.org/docs/current/arrays.html
    https://www.postgresql.org/docs/current/functions-array.html

    Version Requirements:
    - Basic arrays: All modern PostgreSQL versions
    - array_fill: PostgreSQL 8.4+
    - array_position: PostgreSQL 9.5+
    - Domain arrays: PostgreSQL 11+
    """

    def supports_array_type(self) -> bool:
        """Whether array data types are supported.

        Native feature, all modern PostgreSQL versions.
        PostgreSQL supports arrays of any built-in or user-defined
        base type, enum type, or composite type.
        """
        ...

    def supports_array_constructor(self) -> bool:
        """Whether ARRAY constructor expression is supported.

        Native feature, all modern PostgreSQL versions.
        The ARRAY[...] constructor creates array values.
        """
        ...

    def supports_array_access(self) -> bool:
        """Whether array subscript access is supported.

        Native feature, all modern PostgreSQL versions.
        Enables arr[1] and arr[1:3] subscript syntax.
        """
        ...

    def supports_array_fill(self) -> bool:
        """Whether array_fill function is supported.

        Native feature, PostgreSQL 8.4+.
        Creates an array filled with copies of a given value.
        """
        ...

    def supports_array_position(self) -> bool:
        """Whether array_position / array_positions are supported.

        Native feature, PostgreSQL 9.5+.
        Returns the subscript of the first/last occurrence of
        a value in an array.
        """
        ...

    def supports_domain_arrays(self) -> bool:
        """Whether arrays of domain types are supported.

        Native feature, PostgreSQL 11+.
        Allows creating arrays over domain types.
        """
        ...
