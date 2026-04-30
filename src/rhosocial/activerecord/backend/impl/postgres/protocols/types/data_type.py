# src/rhosocial/activerecord/backend/impl/postgres/protocols/types/data_type.py
"""PostgreSQL data type support protocol definition.

This module defines the protocol for PostgreSQL-specific data type features.
"""

from typing import Protocol, runtime_checkable, Tuple


@runtime_checkable
class PostgresDataTypeSupport(Protocol):
    """PostgreSQL data type enhancements protocol.

    Feature Source: Native support (no extension required)

    PostgreSQL data type features:
    - Multirange types (PG 14+)
    - Domain arrays (PG 11+)
    - Composite domains (PG 11+)
    - JSONB subscript (PG 14+)
    - Numeric Infinity (PG 14+)
    - Nondeterministic ICU collation (PG 12+)
    - xid8 type (PG 13+)

    Official Documentation:
    - Multirange: https://www.postgresql.org/docs/current/rangetypes.html
    - Domains: https://www.postgresql.org/docs/current/sql-createdomain.html
    - JSONB: https://www.postgresql.org/docs/current/datatype-json.html
    - ICU Collation: https://www.postgresql.org/docs/current/collation.html#COLLATION-NONDETERMINISTIC
    - xid8: https://www.postgresql.org/docs/current/datatype-oid.html

    Version Requirements:
    - Domain arrays: PostgreSQL 11+
    - Composite domains: PostgreSQL 11+
    - ICU collation: PostgreSQL 12+
    - xid8: PostgreSQL 13+
    - Multirange: PostgreSQL 14+
    - JSONB subscript: PostgreSQL 14+
    - Numeric Infinity: PostgreSQL 14+
    """

    def supports_multirange_type(self) -> bool:
        """Whether multirange data types are supported.

        Native feature, PostgreSQL 14+.
        Enables representation of non-contiguous ranges.
        """
        ...

    def supports_domain_arrays(self) -> bool:
        """Whether arrays of domain types are supported.

        Native feature, PostgreSQL 11+.
        Allows creating arrays over domain types.
        """
        ...

    def supports_composite_domains(self) -> bool:
        """Whether domains over composite types are supported.

        Native feature, PostgreSQL 11+.
        Allows creating domains over composite types.
        """
        ...

    def supports_jsonb_subscript(self) -> bool:
        """Whether JSONB subscript notation is supported.

        Native feature, PostgreSQL 14+.
        Enables jsonb['key'] subscript syntax.
        """
        ...

    def supports_numeric_infinity(self) -> bool:
        """Whether NUMERIC type supports Infinity values.

        Native feature, PostgreSQL 14+.
        Enables Infinity/-Infinity in NUMERIC type.
        """
        ...

    def supports_nondeterministic_collation(self) -> bool:
        """Whether nondeterministic ICU collations are supported.

        Native feature, PostgreSQL 12+.
        Enables case/accent insensitive collations.
        """
        ...

    def supports_xid8_type(self) -> bool:
        """Whether xid8 (64-bit transaction ID) type is supported.

        Native feature, PostgreSQL 13+.
        Provides 64-bit transaction identifiers.
        """
        ...

    def format_create_range_type(self, expr) -> Tuple[str, tuple]:
        """Format CREATE TYPE AS RANGE statement from expression object.

        Args:
            expr: PostgresCreateRangeTypeExpression instance

        Returns:
            Tuple of (SQL string, empty params tuple)
        """
        ...
