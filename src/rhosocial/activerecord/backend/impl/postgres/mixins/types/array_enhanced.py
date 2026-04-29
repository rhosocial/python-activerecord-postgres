# src/rhosocial/activerecord/backend/impl/postgres/mixins/types/array_enhanced.py
"""
PostgreSQL enhanced array mixin.

Implements the PostgresArrayEnhancedSupport protocol.
"""


class PostgresArrayEnhancedMixin:
    """Mixin for PostgreSQL enhanced array support.

    Provides version-aware capability detection for PostgreSQL
    array features beyond the standard ArraySupport protocol.
    """

    def supports_array_type(self) -> bool:
        """PostgreSQL has native array types support."""
        return True  # Supported in all modern versions

    def supports_array_constructor(self) -> bool:
        """ARRAY constructor is supported."""
        return True  # Supported in all modern versions

    def supports_array_access(self) -> bool:
        """Array subscript access is supported."""
        return True  # Supported in all modern versions

    def supports_array_fill(self) -> bool:
        """Whether array_fill function is supported (PostgreSQL 8.4+)."""
        return self.version >= (8, 4, 0)

    def supports_array_position(self) -> bool:
        """Whether array_position / array_positions are supported (PostgreSQL 9.5+)."""
        return self.version >= (9, 5, 0)

    def supports_domain_arrays(self) -> bool:
        """Whether arrays over domain types are supported (PostgreSQL 11+)."""
        return self.version >= (11, 0, 0)
