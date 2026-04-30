# src/rhosocial/activerecord/backend/impl/postgres/protocols/extension.py
"""PostgreSQL extension support protocol.

This module defines the protocol for PostgreSQL extension detection
and management functionality.
"""

from typing import Protocol, runtime_checkable, Dict, Optional, TYPE_CHECKING

from .base import PostgresExtensionInfo

if TYPE_CHECKING:
    from rhosocial.activerecord.backend.expression.statements import SQLQueryAndParams
    from ..expression.ddl.extension import PostgresCreateExtensionExpression, PostgresDropExtensionExpression


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

    Extension States:
    - Unknown: Extension is not in KNOWN_EXTENSIONS and hasn't been detected
    - Available: Extension exists in pg_available_extensions (can be installed)
    - Installed: Extension is enabled in the database

    Detection methods:
    - Automatic detection: introspect_and_adapt() queries pg_extension and pg_available_extensions
    - Manual detection: SELECT * FROM pg_extension WHERE extname = 'xxx';
    - Programmatic detection: dialect.is_extension_installed('xxx')
    - Query availability: dialect.is_extension_available('xxx')

    Version requirement: PostgreSQL 9.1+ supports CREATE EXTENSION
    """

    def detect_extensions(self, connection) -> Dict[str, "PostgresExtensionInfo"]:
        """Detect all installed and available extensions.

        Queries pg_extension system table to get installed extension information,
        and pg_available_extensions to get available extensions.

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
        """Check if an extension is installed (enabled in database).

        Args:
            name: Extension name

        Returns:
            True if extension is installed and enabled
        """
        ...

    def is_extension_available(self, name: str) -> bool:
        """Check if an extension is available (can be installed).

        Args:
            name: Extension name

        Returns:
            True if extension exists in pg_available_extensions
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

    def check_extension_feature(self, ext_name: str, feature_name: str) -> bool:
        """Check if an extension feature is supported based on installed version.

        This method checks if:
        1. The extension is installed
        2. The extension version meets the minimum requirement for the feature

        Args:
            ext_name: Extension name (e.g., 'vector', 'postgis')
            feature_name: Feature name defined in KNOWN_EXTENSIONS features dict

        Returns:
            True if extension is installed and version meets feature requirement
        """
        ...

    def get_extension_min_version_for_feature(self, ext_name: str, feature_name: str) -> Optional[str]:
        """Get the minimum extension version required for a feature.

        Args:
            ext_name: Extension name
            feature_name: Feature name

        Returns:
            Minimum version string, or None if not defined
        """
        ...

    def format_create_extension(self, expr: "PostgresCreateExtensionExpression") -> "SQLQueryAndParams":
        """Format CREATE EXTENSION expression.

        Args:
            expr: PostgresCreateExtensionExpression instance

        Returns:
            Tuple of (SQL string, params)
        """
        ...

    def format_drop_extension(self, expr: "PostgresDropExtensionExpression") -> "SQLQueryAndParams":
        """Format DROP EXTENSION expression.

        Args:
            expr: PostgresDropExtensionExpression instance

        Returns:
            Tuple of (SQL string, params)
        """
        ...