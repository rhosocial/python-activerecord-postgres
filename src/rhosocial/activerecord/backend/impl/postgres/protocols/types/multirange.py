# src/rhosocial/activerecord/backend/impl/postgres/protocols/types/multirange.py
"""PostgreSQL multirange type support protocol."""

from typing import Protocol


class PostgresMultirangeSupport(Protocol):
    """PostgreSQL multirange type support protocol.

    PostgreSQL 14 introduced multirange types, which are arrays of
    non-overlapping ranges. Each range type has a corresponding multirange type.

    Multirange types:
    - int4multirange: Multiple int4range values
    - int8multirange: Multiple int8range values
    - nummultirange: Multiple numrange values
    - tsmultirange: Multiple tsrange values
    - tstzmultirange: Multiple tstzrange values
    - datemultirange: Multiple daterange values

    Version requirement: PostgreSQL 14+
    """

    def supports_multirange(self) -> bool:
        """Check if multirange types are supported.

        Returns:
            True if PostgreSQL version >= 14.0
        """
        ...

    def supports_multirange_constructor(self) -> bool:
        """Check if multirange constructor function is supported.

        The multirange constructor function (e.g., int4multirange(range1, range2))
        was added in PostgreSQL 14.

        Returns:
            True if PostgreSQL version >= 14.0
        """
        ...
