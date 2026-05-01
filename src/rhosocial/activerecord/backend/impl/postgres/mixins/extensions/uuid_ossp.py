# src/rhosocial/activerecord/backend/impl/postgres/mixins/extensions/uuid_ossp.py
"""
PostgreSQL uuid-ossp UUID generation functionality mixin.

This module provides functionality to check uuid-ossp extension features.

For SQL expression generation, use the function factories in
``functions/uuid.py`` instead of the removed format_* methods.
"""


class PostgresUuidOssMixin:
    """uuid-ossp UUID generation functionality implementation."""

    def supports_uuid_generation(self) -> bool:
        """Check if UUID generation functions are supported."""
        return self.check_extension_feature("uuid_ossp", "generation")
