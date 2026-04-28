# src/rhosocial/activerecord/backend/impl/postgres/mixins/extension.py
"""PostgreSQL extension detection mixin.

This module provides functionality to detect and manage PostgreSQL extensions,
including version checking and feature support verification.
"""

from typing import Dict, Optional, TYPE_CHECKING

from ..protocols.base import PostgresExtensionInfo

if TYPE_CHECKING:
    from rhosocial.activerecord.backend.expression.bases import SQLQueryAndParams
    from ..expression.ddl.extension import PostgresCreateExtensionExpression, PostgresDropExtensionExpression


class PostgresExtensionMixin:
    """PostgreSQL extension detection implementation.

    Known extensions definition with minimum version requirements and descriptions.
    """

    KNOWN_EXTENSIONS = {
        # Spatial/Geospatial
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
        "postgis_raster": {
            "min_version": "3.0",
            "description": "PostGIS raster extension",
            "category": "spatial",
            "dependencies": ["postgis"],
            "features": {
                "raster_type": {"min_version": "3.0"},
            },
        },
        "postgis_topology": {
            "min_version": "2.0",
            "description": "PostGIS topology extension",
            "category": "spatial",
            "dependencies": ["postgis"],
            "features": {},
            "requires_dialect": False,
        },
        "postgis_sfcgal": {
            "min_version": "2.0",
            "description": "PostGIS SFCGAL 3D geometry extension",
            "category": "spatial",
            "dependencies": ["postgis"],
            "features": {},
            "requires_dialect": False,
        },
        "postgis_tiger_geocoder": {
            "min_version": "2.0",
            "description": "PostGIS Tiger geocoder extension",
            "category": "spatial",
            "dependencies": ["postgis"],
            "features": {},
            "requires_dialect": False,
        },
        "pgrouting": {
            "min_version": "2.0",
            "description": "pgRouting path finding extension",
            "category": "spatial",
            "dependencies": ["postgis"],
            "features": {
                "dijkstra": {"min_version": "2.0"},
            },
        },
        "earthdistance": {
            "min_version": "1.0",
            "description": "Great-circle distance calculations",
            "category": "spatial",
            "dependencies": ["cube"],
            "features": {
                "type": {"min_version": "1.0"},
                "operators": {"min_version": "1.0"},
            },
        },
        # Vector Search
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
        # Full-Text Search
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
        "fuzzystrmatch": {
            "min_version": "1.0",
            "description": "Fuzzy string matching (Levenshtein, Soundex)",
            "category": "text",
            "documentation": "https://www.postgresql.org/docs/current/fuzzystrmatch.html",
            "features": {
                "levenshtein": {"min_version": "1.0"},
                "soundex": {"min_version": "1.0"},
            },
        },
        # Data Types
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
        "cube": {
            "min_version": "1.0",
            "description": "Multidimensional cube data type",
            "category": "data",
            "documentation": "https://www.postgresql.org/docs/current/cube.html",
            "features": {
                "type": {"min_version": "1.0"},
                "operators": {"min_version": "1.0"},
            },
        },
        "citext": {
            "min_version": "1.0",
            "description": "Case-insensitive text type",
            "category": "data",
            "documentation": "https://www.postgresql.org/docs/current/citext.html",
            "features": {
                "type": {"min_version": "1.0"},
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
        # Indexing
        "bloom": {
            "min_version": "1.0",
            "description": "Bloom filter index access method",
            "category": "indexing",
            "documentation": "https://www.postgresql.org/docs/current/bloom.html",
            "features": {},
        },
        "btree_gin": {
            "min_version": "1.0",
            "description": "B-tree index support for GIN",
            "category": "indexing",
            "documentation": "https://www.postgresql.org/docs/current/btree_gin.html",
            "features": {},
        },
        "btree_gist": {
            "min_version": "1.0",
            "description": "B-tree index support for GiST",
            "category": "indexing",
            "documentation": "https://www.postgresql.org/docs/current/btree_gist.html",
            "features": {},
        },
        # Partitioning
        "pg_partman": {
            "min_version": "4.0",
            "description": "Partition management extension",
            "category": "partitioning",
            "documentation": "https://github.com/pgpartman/pg_partman",
            "features": {
                "auto_partition": {"min_version": "4.0"},
            },
        },
        # Monitoring & Statistics
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
        "pg_surgery": {
            "min_version": "1.0",
            "description": "Repair corrupted data",
            "category": "monitoring",
            "documentation": "https://www.postgresql.org/docs/current/pgsurgery.html",
            "features": {},
        },
        "pg_walinspect": {
            "min_version": "1.0",
            "description": "WAL inspection functions",
            "category": "monitoring",
            "documentation": "https://www.postgresql.org/docs/current/pgwalinspect.html",
            "features": {},
        },
        "amcheck": {
            "min_version": "1.0",
            "description": "Index integrity checking",
            "category": "monitoring",
            "documentation": "https://www.postgresql.org/docs/current/amcheck.html",
            "features": {},
            "requires_dialect": False,
        },
        "pageinspect": {
            "min_version": "1.0",
            "description": "Page-level inspection",
            "category": "monitoring",
            "documentation": "https://www.postgresql.org/docs/current/pageinspect.html",
            "features": {},
            "requires_dialect": False,
        },
        # Replication
        "pglogical": {
            "min_version": "2.0",
            "description": "Logical replication",
            "category": "replication",
            "documentation": "https://www.2ndquadrant.com/en/resources/pglogical/",
            "features": {
                "replication": {"min_version": "2.0"},
            },
        },
        "pg_logicalinspect": {
            "min_version": "1.0",
            "description": "Logical replication inspection",
            "category": "replication",
            "features": {},
            "requires_dialect": False,
        },
        # Security & Auditing
        "pgaudit": {
            "min_version": "1.0",
            "description": "Audit logging extension",
            "category": "security",
            "documentation": "https://www.pgaudit.org/",
            "features": {
                "session": {"min_version": "1.0"},
                "row": {"min_version": "1.7"},
            },
        },
        # Scheduling
        "pg_cron": {
            "min_version": "1.0",
            "description": "Job scheduling extension",
            "category": "scheduling",
            "documentation": "https://github.com/citusdata/pg_cron",
            "features": {
                "schedule": {"min_version": "1.0"},
                "cancel": {"min_version": "1.0"},
                "run": {"min_version": "1.5"},
            },
        },
        # Database Maintenance
        "pg_repack": {
            "min_version": "1.0",
            "description": "Online table and index rebuild",
            "category": "maintenance",
            "documentation": "https://github.com/reorg/pg_repack",
            "features": {
                "rebuild": {"min_version": "1.0"},
            },
        },
        # Testing
        "pgtap": {
            "min_version": "1.0",
            "description": "Database testing framework",
            "category": "testing",
            "documentation": "https://pgtap.org/",
            "features": {
                "tests": {"min_version": "1.0"},
            },
            "requires_dialect": False,
        },
        # Performance & Optimization
        "hypopg": {
            "min_version": "1.0",
            "description": "Hypothetical indexes",
            "category": "performance",
            "documentation": "https://github.com/HypoPG/hypopg",
            "features": {
                "hypothetical_indexes": {"min_version": "1.0"},
            },
        },
        # Oracle Compatibility
        "orafce": {
            "min_version": "3.0",
            "description": "Oracle compatibility functions",
            "category": "compatibility",
            "documentation": "https://github.com/orafce/orafce",
            "features": {
                "functions": {"min_version": "3.0"},
            },
        },
        # Address Standardization
        "address_standardizer": {
            "min_version": "2.0",
            "description": "Address standardization",
            "category": "utility",
            "dependencies": ["postgis"],
            "documentation": "https://postgis.net/docs/Geocode.html",
            "features": {},
        },
        "address_standardizer_data_us": {
            "min_version": "2.0",
            "description": "US address standardization data",
            "category": "utility",
            "dependencies": ["address_standardizer"],
            "features": {},
            "requires_dialect": False,
        },
    }

    def detect_extensions(self, connection) -> Dict[str, PostgresExtensionInfo]:
        """Query installed and available extensions from database.

        Queries pg_extension for installed extensions and pg_available_extensions
        for available (installable) extensions.

        Args:
            connection: Database connection object

        Returns:
            Dictionary mapping extension names to extension info
        """
        cursor = connection.cursor()
        try:
            extensions = {}

            cursor.execute("""
                SELECT extname, extversion, nspname as schema_name
                FROM pg_extension
                JOIN pg_namespace ON pg_extension.extnamespace = pg_namespace.oid
            """)

            for row in cursor.fetchall():
                ext_name = row[0]
                extensions[ext_name] = PostgresExtensionInfo(
                    name=ext_name,
                    installed=True,
                    available=True,
                    version=row[1],
                    schema=row[2],
                )

            cursor.execute("""
                SELECT name, default_version
                FROM pg_available_extensions
            """)

            for row in cursor.fetchall():
                ext_name = row[0]
                if ext_name in extensions:
                    extensions[ext_name].available = True
                else:
                    extensions[ext_name] = PostgresExtensionInfo(
                        name=ext_name,
                        installed=False,
                        available=True,
                        version=row[1],
                    )

            return extensions
        finally:
            cursor.close()

    def is_extension_installed(self, name: str) -> bool:
        """Check if extension is installed."""
        if not hasattr(self, "_extensions"):
            return False
        info = self._extensions.get(name)
        return info.installed if info else False

    def is_extension_available(self, name: str) -> bool:
        """Check if extension is available (can be installed)."""
        if not hasattr(self, "_extensions"):
            return False
        info = self._extensions.get(name)
        return info.available if info else False

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

    def format_create_extension(
        self, expr: "PostgresCreateExtensionExpression"
    ) -> "SQLQueryAndParams":
        """Format CREATE EXTENSION expression.

        Args:
            expr: PostgresCreateExtensionExpression instance

        Returns:
            Tuple of (SQL string, empty params tuple).
        """
        parts = ["CREATE EXTENSION"]

        if expr.if_not_exists:
            parts.append("IF NOT EXISTS")

        # Quote extension name if it contains hyphen or other special chars
        if "-" in expr.name or "_" in expr.name or any(c.isupper() for c in expr.name):
            name = f'"{expr.name}"'
        else:
            name = expr.name
        parts.append(name)

        if expr.schema:
            parts.append(f"SCHEMA {expr.schema}")

        if expr.version:
            parts.append(f"VERSION '{expr.version}'")

        if expr.cascade:
            parts.append("CASCADE")

        return " ".join(parts), ()

    def format_drop_extension(
        self, expr: "PostgresDropExtensionExpression"
    ) -> "SQLQueryAndParams":
        """Format DROP EXTENSION expression.

        Args:
            expr: PostgresDropExtensionExpression instance

        Returns:
            Tuple of (SQL string, empty params tuple).
        """
        parts = ["DROP EXTENSION"]

        if expr.if_exists:
            parts.append("IF EXISTS")

        # Quote extension name if it contains hyphen or other special chars
        if "-" in expr.name or "_" in expr.name or any(c.isupper() for c in expr.name):
            name = f'"{expr.name}"'
        else:
            name = expr.name
        parts.append(name)

        if expr.cascade:
            parts.append("CASCADE")
        elif expr.restrict:
            parts.append("RESTRICT")

        return " ".join(parts), ()
