# src/rhosocial/activerecord/backend/impl/postgres/mixins/extension.py
"""PostgreSQL extension detection mixin.

This module provides functionality to detect and manage PostgreSQL extensions,
including version checking and feature support verification.
"""

from typing import Dict, Optional

from ..protocols.base import PostgresExtensionInfo


class PostgresExtensionMixin:
    """PostgreSQL extension detection implementation.

    Known extensions definition with minimum version requirements and descriptions.
    """

    KNOWN_EXTENSIONS = {
        "postgis": {
            "min_version": "2.0",
            "description": "PostGIS spatial database extension",
            "category": "spatial",
            "documentation": "https://postgis.net/docs/",
            "repository": "https://postgis.net/",
            "features": {
                "geometry_type": {"min_version": "2.0"},
                "geography_type": {"min_version": "2.0"},
                "spatial_index": {"min_version": "2.0"},
                "spatial_functions": {"min_version": "2.0"},
            },
        },
        "vector": {
            "min_version": "0.1",
            "description": "pgvector vector similarity search",
            "category": "vector",
            "documentation": "https://github.com/pgvector/pgvector",
            "repository": "https://github.com/pgvector/pgvector",
            "features": {
                "type": {"min_version": "0.1"},
                "similarity_search": {"min_version": "0.1"},
                "ivfflat_index": {"min_version": "0.1"},
                "hnsw_index": {"min_version": "0.5.0"},
            },
        },
        "pg_trgm": {
            "min_version": "1.0",
            "description": "Trigram similarity search",
            "category": "text",
            "documentation": "https://www.postgresql.org/docs/current/pgtrgm.html",
            "features": {
                "similarity": {"min_version": "1.0"},
                "index": {"min_version": "1.0"},
            },
        },
        "hstore": {
            "min_version": "1.0",
            "description": "Key-value pair storage",
            "category": "data",
            "documentation": "https://www.postgresql.org/docs/current/hstore.html",
            "features": {
                "type": {"min_version": "1.0"},
                "operators": {"min_version": "1.0"},
            },
        },
        "uuid-ossp": {
            "min_version": "1.0",
            "description": "UUID generation functions",
            "category": "utility",
            "documentation": "https://www.postgresql.org/docs/current/uuid-ossp.html",
            "features": {},
        },
        "pgcrypto": {
            "min_version": "1.0",
            "description": "Cryptographic functions",
            "category": "security",
            "documentation": "https://www.postgresql.org/docs/current/pgcrypto.html",
            "features": {},
        },
        "ltree": {
            "min_version": "1.0",
            "description": "Label tree for hierarchical data",
            "category": "data",
            "documentation": "https://www.postgresql.org/docs/current/ltree.html",
            "features": {
                "type": {"min_version": "1.0"},
                "operators": {"min_version": "1.0"},
                "index": {"min_version": "1.0"},
            },
        },
        "intarray": {
            "min_version": "1.0",
            "description": "Integer array operators and indexes",
            "category": "data",
            "documentation": "https://www.postgresql.org/docs/current/intarray.html",
            "features": {
                "operators": {"min_version": "1.0"},
                "functions": {"min_version": "1.0"},
                "index": {"min_version": "1.0"},
            },
        },
        "earthdistance": {
            "min_version": "1.0",
            "description": "Great-circle distance calculations",
            "category": "spatial",
            "documentation": "https://www.postgresql.org/docs/current/earthdistance.html",
            "dependencies": ["cube"],
            "features": {
                "type": {"min_version": "1.0"},
                "operators": {"min_version": "1.0"},
            },
        },
        "tablefunc": {
            "min_version": "1.0",
            "description": "Table functions (crosstab, connectby)",
            "category": "utility",
            "documentation": "https://www.postgresql.org/docs/current/tablefunc.html",
            "features": {
                "crosstab": {"min_version": "1.0"},
                "connectby": {"min_version": "1.0"},
                "normal_rand": {"min_version": "1.0"},
            },
        },
        "pg_stat_statements": {
            "min_version": "1.0",
            "description": "Query execution statistics",
            "category": "monitoring",
            "documentation": "https://www.postgresql.org/docs/current/pgstatstatements.html",
            "requires_preload": True,
            "features": {
                "view": {"min_version": "1.0"},
                "reset": {"min_version": "1.0"},
            },
        },
    }

    def detect_extensions(self, connection) -> Dict[str, PostgresExtensionInfo]:
        """Query installed extensions from database.

        Queries pg_extension system table to get extension information.

        Args:
            connection: Database connection object

        Returns:
            Dictionary mapping extension names to extension info
        """
        cursor = connection.cursor()
        try:
            cursor.execute("""
                SELECT extname, extversion, nspname as schema_name
                FROM pg_extension
                JOIN pg_namespace ON pg_extension.extnamespace = pg_namespace.oid
            """)

            extensions = {}
            for row in cursor.fetchall():
                ext_name = row[0]
                extensions[ext_name] = PostgresExtensionInfo(
                    name=ext_name, installed=True, version=row[1], schema=row[2]
                )

            # Add known but not installed extensions
            for known_ext in self.KNOWN_EXTENSIONS:
                if known_ext not in extensions:
                    extensions[known_ext] = PostgresExtensionInfo(name=known_ext, installed=False)

            return extensions
        finally:
            cursor.close()

    def is_extension_installed(self, name: str) -> bool:
        """Check if extension is installed."""
        if not hasattr(self, "_extensions"):
            return False
        info = self._extensions.get(name)
        return info.installed if info else False

    def get_extension_version(self, name: str) -> Optional[str]:
        """Get extension version."""
        if not hasattr(self, "_extensions"):
            return None
        info = self._extensions.get(name)
        return info.version if info else None

    def get_extension_info(self, name: str) -> Optional[PostgresExtensionInfo]:
        """Get extension info."""
        if not hasattr(self, "_extensions"):
            return None
        return self._extensions.get(name)

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

        Example:
            >>> dialect.check_extension_feature('vector', 'hnsw_index')
            True  # Only if pgvector >= 0.5.0 is installed
        """
        # Check if extension is installed
        if not self.is_extension_installed(ext_name):
            return False

        # Get extension info from KNOWN_EXTENSIONS
        ext_config = self.KNOWN_EXTENSIONS.get(ext_name)
        if not ext_config or "features" not in ext_config:
            # No feature requirements defined, assume supported if installed
            return True

        # Get feature requirements
        feature_config = ext_config.get("features", {}).get(feature_name)
        if not feature_config:
            # Feature not defined, assume supported if extension installed
            return True

        # Check version requirement
        min_version = feature_config.get("min_version")
        if not min_version:
            # No version requirement, assume supported
            return True

        # Compare with installed version
        installed_version = self.get_extension_version(ext_name)
        if not installed_version:
            # Cannot determine version, assume not supported
            return False

        return self._compare_versions(installed_version, min_version) >= 0

    def get_extension_min_version_for_feature(self, ext_name: str, feature_name: str) -> Optional[str]:
        """Get the minimum extension version required for a feature.

        Args:
            ext_name: Extension name
            feature_name: Feature name

        Returns:
            Minimum version string, or None if not defined
        """
        ext_config = self.KNOWN_EXTENSIONS.get(ext_name)
        if not ext_config:
            return None

        feature_config = ext_config.get("features", {}).get(feature_name)
        if not feature_config:
            return None

        return feature_config.get("min_version")

    def _compare_versions(self, v1: str, v2: str) -> int:
        """Compare version numbers.

        Returns:
            Positive if v1 > v2, negative if v1 < v2, 0 if equal
        """

        def parse_version(v):
            parts = []
            for part in v.split("."):
                try:
                    parts.append(int(part))
                except ValueError:
                    # Handle versions like "0.5.0.dev"
                    num = "".join(c for c in part if c.isdigit())
                    parts.append(int(num) if num else 0)
            return parts

        p1, p2 = parse_version(v1), parse_version(v2)
        # Pad version lengths
        max_len = max(len(p1), len(p2))
        p1.extend([0] * (max_len - len(p1)))
        p2.extend([0] * (max_len - len(p2)))

        for a, b in zip(p1, p2):
            if a != b:
                return a - b
        return 0
