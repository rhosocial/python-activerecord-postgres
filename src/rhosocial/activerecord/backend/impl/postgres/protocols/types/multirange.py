# src/rhosocial/activerecord/backend/impl/postgres/protocols/types/multirange.py
"""PostgreSQL multirange type support protocol."""

from typing import Protocol, runtime_checkable, Optional, Tuple


@runtime_checkable
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

    def format_create_multirange_type_statement(
        self, name: str, range_type: str, schema: Optional[str] = None
    ) -> Tuple[str, tuple]:
        """Format CREATE TYPE statement for a custom multirange type.

        Note: PostgreSQL automatically creates multirange types when you create
        a range type. This method is for documentation and explicit creation
        if needed.

        Args:
            name: The multirange type name
            range_type: The associated range type
            schema: Optional schema name

        Returns:
            Tuple of (SQL statement, parameters)
        """
        ...

    def format_multirange_agg_function(
        self, range_column: str, table_name: str, where_clause: Optional[str] = None, schema: Optional[str] = None
    ) -> Tuple[str, tuple]:
        """Format multirange_agg aggregate function call.

        The multirange_agg function aggregates multiple ranges into a multirange.

        Args:
            range_column: The range column to aggregate
            table_name: Table name
            where_clause: Optional WHERE clause
            schema: Optional schema name

        Returns:
            Tuple of (SQL statement, parameters)
        """
        ...
