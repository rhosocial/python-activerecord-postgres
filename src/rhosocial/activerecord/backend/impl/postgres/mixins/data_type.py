# src/rhosocial/activerecord/backend/impl/postgres/mixins/data_type.py
"""PostgreSQL data type enhancements mixin.

This module contains the PostgresDataTypeMixin class which provides
methods to check support for various PostgreSQL data type features
based on the server version.
"""


class PostgresDataTypeMixin:
    """PostgreSQL data type enhancements implementation.

    All features are native, using version number for detection.
    """

    def supports_multirange_type(self) -> bool:
        """Multirange is native feature, PG 14+."""
        return self.version >= (14, 0, 0)

    def supports_domain_arrays(self) -> bool:
        """Domain arrays are native feature, PG 11+."""
        return self.version >= (11, 0, 0)

    def supports_composite_domains(self) -> bool:
        """Composite domains are native feature, PG 11+."""
        return self.version >= (11, 0, 0)

    def supports_jsonb_subscript(self) -> bool:
        """JSONB subscript is native feature, PG 14+."""
        return self.version >= (14, 0, 0)

    def supports_numeric_infinity(self) -> bool:
        """Numeric Infinity is native feature, PG 14+."""
        return self.version >= (14, 0, 0)

    def supports_nondeterministic_collation(self) -> bool:
        """Nondeterministic ICU collation is native feature, PG 12+."""
        return self.version >= (12, 0, 0)

    def supports_xid8_type(self) -> bool:
        """xid8 type is native feature, PG 13+."""
        return self.version >= (13, 0, 0)

    # =========================================================================
    # PostgreSQL 17+ Data Type Features
    # =========================================================================

    def supports_infinity_numeric_infinity_jsonb(self) -> bool:
        """Numeric infinity in JSONB is supported since PostgreSQL 17.

        Returns:
            True if supported
        """
        return self.version >= (17, 0, 0)
