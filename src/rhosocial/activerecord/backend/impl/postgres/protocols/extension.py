# src/rhosocial/activerecord/backend/impl/postgres/protocols/extension.py
"""PostgreSQL extension support protocol.

This module defines the protocol for PostgreSQL extension detection
and management functionality.
"""

from typing import Protocol, runtime_checkable, Dict, Optional

from .base import PostgresExtensionInfo


@runtime_checkable
class PostgresExtensionSupport(Protocol):
    """PostgreSQL extension detection protocol.

    PostgreSQL supports installing additional functionality modules via CREATE EXTENSION.
    Common extensions include:
    - PostGIS: Spatial database functionality
    - pgvector: Vector similarity search
    - pg_trgm: Trigram similarity
    - hstore: Key-value pair storage
    - uuid-ossp: UUID generation functions

    Detection methods:
    - Automatic detection: introspect_and_adapt() queries pg_extension
    - Manual detection: SELECT * FROM pg_extension WHERE extname = 'xxx';
    - Programmatic detection: dialect.is_extension_installed('xxx')

    Version requirement: PostgreSQL 9.1+ supports CREATE EXTENSION
    """

    def detect_extensions(self, connection) -> Dict[str, "PostgresExtensionInfo"]:
        """Detect all installed extensions.

        Queries pg_extension system table to get extension information.
        This method should be called within introspect_and_adapt().

        Args:
            connection: Database connection object

        Returns:
            Dictionary mapping extension names to extension info
        """
        ...

    def get_extension_info(self, name: str) -> Optional["PostgresExtensionInfo"]:
        """Get information for a specific extension.

        Args:
            name: Extension name

        Returns:
            Extension info, or None if not detected or doesn't exist
        """
        ...

    def is_extension_installed(self, name: str) -> bool:
        """Check if an extension is installed.

        Args:
            name: Extension name

        Returns:
            True if extension is installed and enabled
        """
        ...

    def get_extension_version(self, name: str) -> Optional[str]:
        """Get extension version.

        Args:
            name: Extension name

        Returns:
            Version string, or None if not installed
        """
        ...
