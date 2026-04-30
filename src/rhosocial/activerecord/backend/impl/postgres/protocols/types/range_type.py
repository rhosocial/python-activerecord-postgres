# src/rhosocial/activerecord/backend/impl/postgres/protocols/types/range_type.py
"""PostgreSQL range type support protocol definition.

This module defines the protocol for PostgreSQL range type features,
including range operators, functions, and constructor functions.
"""

from typing import Protocol, runtime_checkable


@runtime_checkable
class PostgresRangeTypeSupport(Protocol):
    """PostgreSQL range type support protocol.

    PostgreSQL range types represent a range of values of a given
    element type (subtype). Built-in range types include int4range,
    int8range, numrange, tsrange, tstzrange, and daterange.

    Range features:
    - Range operators: @>, <@, &&, -|-, <<, >>, &<, &>
    - Range functions: lower(), upper(), isempty(), etc.
    - Range constructors: int4range(), int8range(), numrange(), etc.
    - Custom range type creation

    Feature Source: Native support (no extension required)

    Official Documentation:
    https://www.postgresql.org/docs/current/rangetypes.html
    https://www.postgresql.org/docs/current/functions-range.html

    Version Requirements:
    - Range types and operators: PostgreSQL 9.2+
    - Range constructors: PostgreSQL 9.2+
    """

    def supports_range_type(self) -> bool:
        """Whether range data types are supported.

        Native feature, PostgreSQL 9.2+.
        Includes built-in range types (int4range, numrange, tsrange, etc.)
        and custom range type creation.
        """
        ...

    def supports_range_operators(self) -> bool:
        """Whether range operators are supported.

        Native feature, PostgreSQL 9.2+.
        Includes containment (@>, <@), overlap (&&), adjacency (-|-),
        strict left/right (<<, >>), and extend bounds (&<, &>) operators.
        """
        ...

    def supports_range_functions(self) -> bool:
        """Whether range functions are supported.

        Native feature, PostgreSQL 9.2+.
        Includes lower(), upper(), isempty(), lower_inc(), upper_inc(),
        lower_inf(), upper_inf(), and set operations (union, intersection,
        difference).
        """
        ...

    def supports_range_constructors(self) -> bool:
        """Whether range constructor functions are supported.

        Native feature, PostgreSQL 9.2+.
        Includes int4range(), int8range(), numrange(), tsrange(),
        tstzrange(), and daterange() constructor functions.
        """
        ...
