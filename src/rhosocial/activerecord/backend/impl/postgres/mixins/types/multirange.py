# src/rhosocial/activerecord/backend/impl/postgres/mixins/types/multirange.py
"""PostgreSQL multirange type support mixin implementation.

This module provides the MultirangeMixin class for handling PostgreSQL
multirange type operations.
"""

from typing import Optional, Tuple


class MultirangeMixin:
    """Mixin providing PostgreSQL multirange type formatting methods.

    This mixin implements the MultirangeSupport protocol.
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

    def format_range_merge_function(self, multirange_column: str) -> str:
        """Format range_merge function call on a multirange.

        The range_merge function returns the smallest range that includes
        all ranges in the multirange.

        Args:
            multirange_column: The multirange column/expression

        Returns:
            SQL function call string

        Example:
            >>> format_range_merge_function('my_multirange')
            "range_merge(my_multirange)"
        """
        return f"range_merge({multirange_column})"

    def format_multirange_contains(self, multirange_column: str, value: str) -> str:
        """Format a containment check for multirange.

        Args:
            multirange_column: The multirange column
            value: The value or range to check containment

        Returns:
            SQL containment expression

        Example:
            >>> format_multirange_contains('periods', '5')
            "periods @> 5"
        """
        return f"{multirange_column} @> {value}"

    def format_multirange_is_contained_by(self, multirange_column: str, value: str) -> str:
        """Format an "is contained by" check for multirange.

        Args:
            multirange_column: The multirange column
            value: The value or range to check

        Returns:
            SQL containment expression

        Example:
            >>> format_multirange_is_contained_by('periods', '[1,100)')
            "periods <@ '[1,100)'"
        """
        return f"{multirange_column} <@ '{value}'"

    def format_multirange_overlaps(self, multirange_column: str, other: str) -> str:
        """Format an overlaps check for multirange.

        Args:
            multirange_column: The multirange column
            other: The other multirange/range to check overlap with

        Returns:
            SQL overlaps expression

        Example:
            >>> format_multirange_overlaps('periods', '[10,20)')
            "periods && '[10,20)'"
        """
        return f"{multirange_column} && '{other}'"

    def format_multirange_union(self, multirange_column: str, other: str) -> str:
        """Format multirange union operation.

        Args:
            multirange_column: The multirange column
            other: The other multirange/range to union with

        Returns:
            SQL union expression

        Example:
            >>> format_multirange_union('periods', '[10,20)')
            "periods + '[10,20)'"
        """
        return f"{multirange_column} + '{other}'"

    def format_multirange_intersection(self, multirange_column: str, other: str) -> str:
        """Format multirange intersection operation.

        Args:
            multirange_column: The multirange column
            other: The other multirange/range to intersect with

        Returns:
            SQL intersection expression

        Example:
            >>> format_multirange_intersection('periods', '[10,20)')
            "periods * '[10,20)'"
        """
        return f"{multirange_column} * '{other}'"

    def format_multirange_difference(self, multirange_column: str, other: str) -> str:
        """Format multirange difference operation.

        Args:
            multirange_column: The multirange column
            other: The other multirange/range to subtract

        Returns:
            SQL difference expression

        Example:
            >>> format_multirange_difference('periods', '[10,20)')
            "periods - '[10,20)'"
        """
        return f"{multirange_column} - '{other}'"
