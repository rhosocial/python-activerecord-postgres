# src/rhosocial/activerecord/backend/impl/postgres/mixins/types/data_type.py
"""PostgreSQL data type support mixin implementation.

This module provides the DataTypeMixin class for handling PostgreSQL-specific
data type operations.

For SQL expression generation of data type literals (multirange, xid8, array),
use the function factories in ``functions/range.py`` and ``functions/data_type.py``
instead of the removed format_* methods.

Methods retained in this mixin:
- supports_*: Capability detection
- format_create_range_type: DDL statement
"""

from typing import Tuple


class PostgresDataTypeMixin:
    """Mixin providing PostgreSQL data type support methods.

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

    def format_create_range_type(self, expr) -> Tuple[str, tuple]:
        """Format CREATE TYPE AS RANGE statement from expression object.

        Args:
            expr: PostgresCreateRangeTypeExpression instance

        Returns:
            Tuple of (SQL string, empty params tuple)
        """
        exists_clause = "IF NOT EXISTS " if expr.if_not_exists else ""
        type_name = f"{expr.schema}.{expr.name}" if expr.schema else expr.name

        options = [f"subtype={expr.subtype}"]

        if expr.subtype_opclass:
            options.append(f"subtype_opclass={expr.subtype_opclass}")
        if expr.collation:
            options.append(f"collation={expr.collation}")
        if expr.canonical:
            options.append(f"canonical={expr.canonical}")
        if expr.subtype_diff:
            options.append(f"subtype_diff={expr.subtype_diff}")

        options_str = ", ".join(options)
        sql = f"CREATE TYPE {exists_clause}{type_name} AS RANGE ({options_str})"
        return (sql, ())
