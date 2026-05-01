# src/rhosocial/activerecord/backend/impl/postgres/mixins/types/jsonb_enhanced.py
"""
PostgreSQL JSONB enhanced mixin.

Implements the PostgresJSONBEnhancedSupport protocol.
"""


class PostgresJSONBEnhancedMixin:
    """Mixin for PostgreSQL enhanced JSONB support.

    Provides version-aware capability detection for PostgreSQL
    JSON and JSONB features beyond the standard JSONSupport protocol.
    """

    def supports_json_type(self) -> bool:
        """JSON is supported since PostgreSQL 9.2."""
        return self.version >= (9, 2, 0)

    def supports_jsonb(self) -> bool:
        """Check if PostgreSQL version supports JSONB type (introduced in 9.4)."""
        return self.version >= (9, 4, 0)

    def supports_json_path(self) -> bool:
        """Whether JSON path expressions are supported (PostgreSQL 12+)."""
        return self.version >= (12, 0, 0)

    def supports_json_table(self) -> bool:
        """JSON_TABLE function is supported since PostgreSQL 12."""
        return self.version >= (12, 0, 0)

    def supports_jsonb_subscript(self) -> bool:
        """Whether JSONB subscript is supported (PostgreSQL 11+)."""
        return self.version >= (11, 0, 0)

    def supports_infinity_numeric_infinity_jsonb(self) -> bool:
        """Whether numeric infinity values are allowed in JSONB (PostgreSQL 17+)."""
        return self.version >= (17, 0, 0)
