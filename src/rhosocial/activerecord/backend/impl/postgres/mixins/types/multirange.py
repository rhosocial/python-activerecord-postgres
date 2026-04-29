# src/rhosocial/activerecord/backend/impl/postgres/mixins/types/multirange.py
"""PostgreSQL multirange type support mixin implementation.

This module provides the MultirangeMixin class for handling PostgreSQL
multirange type operations.

For SQL expression generation of multirange operators and functions,
use the function factories in ``functions/range.py`` instead of the
removed format_* methods.

Methods retained in this mixin:
- supports_*: Capability detection
- format_create_multirange_type_statement: DDL statement
- format_multirange_agg_function: Complete SELECT query template
"""

from typing import Optional, Tuple


class MultirangeMixin:
    """Mixin providing PostgreSQL multirange type support methods.

    This mixin implements the PostgresMultirangeSupport protocol.
    Designed for multiple inheritance with SQLDialectBase.
    """

    def supports_multirange(self) -> bool:
        """Check if multirange types are supported.

        Returns:
            True if PostgreSQL version >= 14.0
        """
        return self.version >= (14, 0, 0)

    def supports_multirange_constructor(self) -> bool:
        """Check if multirange constructor function is supported.

        Returns:
            True if PostgreSQL version >= 14.0
        """
        return self.version >= (14, 0, 0)

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

        Example:
            >>> format_create_multirange_type_statement('my_multirange', 'my_range')
            ('CREATE TYPE my_multirange AS MULTIRANGE (my_range)', ())
        """
        full_name = f"{schema}.{name}" if schema else name
        sql = f"CREATE TYPE {full_name} AS MULTIRANGE ({range_type})"
        return (sql, ())

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

        Example:
            >>> format_multirange_agg_function('period', 'events')
            ('SELECT multirange_agg(period) FROM events', ())
        """
        full_table = f"{schema}.{table_name}" if schema else table_name
        sql = f"SELECT multirange_agg({range_column}) FROM {full_table}"
        if where_clause:
            sql += f" WHERE {where_clause}"
        return (sql, ())
