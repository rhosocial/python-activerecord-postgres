# src/rhosocial/activerecord/backend/impl/postgres/mixins/types/range_type.py
"""
PostgreSQL range type mixin.

Implements the PostgresRangeTypeSupport protocol.
"""


class PostgresRangeTypeMixin:
    """Mixin for PostgreSQL range type support.

    Provides version-aware capability detection for PostgreSQL
    range data types.
    """

    def supports_range_type(self) -> bool:
        """Whether range data types are supported (PostgreSQL 9.2+)."""
        return self.version >= (9, 2, 0)

    def supports_range_operators(self) -> bool:
        """Whether range operators are supported (PostgreSQL 9.2+)."""
        return self.version >= (9, 2, 0)

    def supports_range_functions(self) -> bool:
        """Whether range functions are supported (PostgreSQL 9.2+)."""
        return self.version >= (9, 2, 0)

    def supports_range_constructors(self) -> bool:
        """Whether range constructor functions are supported (PostgreSQL 9.2+)."""
        return self.version >= (9, 2, 0)
