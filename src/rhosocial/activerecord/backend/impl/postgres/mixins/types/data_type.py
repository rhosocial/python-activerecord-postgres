# src/rhosocial/activerecord/backend/impl/postgres/mixins/types/data_type.py
"""PostgreSQL data type support mixin implementation.

This module provides the DataTypeMixin class for handling PostgreSQL-specific
data type operations including multirange types, xid8, and other features.
"""

from typing import Optional, List, Any


class PostgresDataTypeMixin:
    """Mixin providing PostgreSQL data type formatting methods.

    This mixin implements the PostgresDataTypeSupport protocol.
    Designed for multiple inheritance with SQLDialectBase.
    """

    def supports_multirange_type(self) -> bool:
        """Whether multirange data types are supported.

        Native feature, PostgreSQL 14+.
        """
        return self.version >= (14, 0, 0)

    def supports_domain_arrays(self) -> bool:
        """Whether arrays of domain types are supported.

        Native feature, PostgreSQL 11+.
        """
        return self.version >= (11, 0, 0)

    def supports_composite_domains(self) -> bool:
        """Whether domains over composite types are supported.

        Native feature, PostgreSQL 11+.
        """
        return self.version >= (11, 0, 0)

    def supports_jsonb_subscript(self) -> bool:
        """Whether JSONB subscript notation is supported.

        Native feature, PostgreSQL 14+.
        """
        return self.version >= (14, 0, 0)

    def supports_numeric_infinity(self) -> bool:
        """Whether NUMERIC type supports Infinity values.

        Native feature, PostgreSQL 14+.
        """
        return self.version >= (14, 0, 0)

    def supports_nondeterministic_collation(self) -> bool:
        """Whether nondeterministic ICU collations are supported.

        Native feature, PostgreSQL 12+.
        """
        return self.version >= (12, 0, 0)

    def supports_xid8_type(self) -> bool:
        """Whether xid8 (64-bit transaction ID) type is supported.

        Native feature, PostgreSQL 13+.
        """
        return self.version >= (13, 0, 0)

    def format_multirange_literal(self, ranges: List[str], multirange_type: str) -> str:
        """Format a multirange literal value.

        Args:
            ranges: List of range literal strings (e.g., ['[1,5)', '[10,20)'])
            multirange_type: The multirange type name (e.g., 'int4multirange')

        Returns:
            SQL multirange literal string

        Example:
            >>> format_multirange_literal(['[1,5)', '[10,20)'], 'int4multirange')
            "int4multirange('[1,5)', '[10,20)')"
        """
        ranges_str = ", ".join(ranges)
        return f"{multirange_type}({ranges_str})"

    def format_multirange_constructor(self, multirange_type: str, *range_values: str) -> str:
        """Format a multirange constructor function call.

        Args:
            multirange_type: The multirange type name
            *range_values: Range values as strings

        Returns:
            SQL constructor function call

        Example:
            >>> format_multirange_constructor('int4multirange', '[1,5)', '[10,20)')
            "int4multirange('[1,5)', '[10,20)')"
        """
        if range_values:
            values_str = ", ".join(f"'{r}'" for r in range_values)
            return f"{multirange_type}({values_str})"
        return f"{multirange_type}()"

    def format_xid8_literal(self, value: int) -> str:
        """Format an xid8 (64-bit transaction ID) literal.

        Args:
            value: The transaction ID value

        Returns:
            SQL xid8 literal string

        Example:
            >>> format_xid8_literal(123456789)
            "123456789::xid8"
        """
        return f"{value}::xid8"

    def format_array_literal(self, elements: List[Any], element_type: Optional[str] = None) -> str:
        """Format an array literal.

        Args:
            elements: List of array elements
            element_type: Optional element type cast

        Returns:
            SQL array literal string

        Example:
            >>> format_array_literal([1, 2, 3], 'int')
            "ARRAY[1, 2, 3]::int[]"
        """
        if not elements:
            if element_type:
                return f"'{{}}'::{element_type}[]"
            return "'{}'"

        formatted_elements = []
        for e in elements:
            if isinstance(e, str):
                formatted_elements.append(f"'{e}'")
            elif e is None:
                formatted_elements.append("NULL")
            else:
                formatted_elements.append(str(e))

        array_str = f"ARRAY[{', '.join(formatted_elements)}]"
        if element_type:
            array_str += f"::{element_type}[]"
        return array_str
