# src/rhosocial/activerecord/backend/impl/postgres/mixins/extensions/uuid_ossp.py
"""
uuid-ossp UUID generation functionality implementation.

This module provides the PostgresUuidOssMixin class that adds support for
uuid-ossp extension features.
"""

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    pass


class PostgresUuidOssMixin:
    """uuid-ossp UUID generation functionality implementation."""

    def supports_uuid_generation(self) -> bool:
        """Check if UUID generation functions are supported."""
        return self.check_extension_feature("uuid_ossp", "generation")

    def format_uuid_generate_v1(self) -> str:
        """Format UUID v1 generation.

        Returns:
            SQL UUID generation using v1 (MAC + time)
        """
        return "uuid_generate_v1()"

    def format_uuid_generate_v1mc(self) -> str:
        """Format UUID v1mc generation.

        Returns:
            SQL UUID generation using v1 (random node)
        """
        return "uuid_generate_v1mc()"

    def format_uuid_generate_v4(self) -> str:
        """Format UUID v4 generation.

        Returns:
            SQL UUID generation using v4 (random)
        """
        return "uuid_generate_v4()"

    def format_uuid_generate_v5(self, namespace: str, name: str) -> str:
        """Format UUID v5 generation.

        Args:
            namespace: UUID namespace
            name: Name to hash

        Returns:
            SQL UUID generation using v5 (SHA-1)
        """
        return f"uuid_generate_v5('{namespace}', '{name}')"