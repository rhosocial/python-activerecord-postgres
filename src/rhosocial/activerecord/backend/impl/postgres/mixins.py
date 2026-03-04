# src/rhosocial/activerecord/backend/impl/postgres/mixins.py
"""PostgreSQL dialect-specific Mixin implementations."""
from typing import Dict, Optional, Tuple, Type
import logging
from .protocols import PostgresExtensionInfo


class PostgresBackendMixin:
    """PostgreSQL backend shared methods mixin.

    This mixin provides methods that are shared between sync and async
    PostgreSQL backend implementations. These methods do not involve
    I/O operations and have identical implementations in both backends.

    Classes using this mixin must provide:
    - self._dialect: PostgresDialect instance
    - self.adapter_registry: Type adapter registry
    - self.log(level, message): Logging method
    - self.config: Connection configuration
    """

    def _prepare_sql_and_params(
        self,
        sql: str,
        params: Optional[Tuple]
    ) -> Tuple[str, Optional[Tuple]]:
        """
        Prepare SQL and parameters for PostgreSQL execution.

        Converts the generic '?' placeholder to PostgreSQL-compatible '%s' placeholder.
        """
        if params is None:
            return sql, None

        # Replace '?' placeholders with '%s' for PostgreSQL
        prepared_sql = sql.replace('?', '%s')
        return prepared_sql, params

    def create_expression(self, expression_str: str):
        """Create an expression object for raw SQL expressions."""
        from rhosocial.activerecord.backend.expression.operators import RawSQLExpression
        return RawSQLExpression(self.dialect, expression_str)

    def requires_manual_commit(self) -> bool:
        """Check if manual commit is required for this database."""
        return not getattr(self.config, 'autocommit', False)

    def get_default_adapter_suggestions(self) -> Dict[Type, Tuple['SQLTypeAdapter', Type]]:
        """
        [Backend Implementation] Provides default type adapter suggestions for PostgreSQL.

        This method defines a curated set of type adapter suggestions for common Python
        types, mapping them to their typical PostgreSQL-compatible representations as
        demonstrated in test fixtures. It explicitly retrieves necessary `SQLTypeAdapter`
        instances from the backend's `adapter_registry`. If an adapter for a specific
        (Python type, DB driver type) pair is not registered, no suggestion will be
        made for that Python type.

        Returns:
            Dict[Type, Tuple[SQLTypeAdapter, Type]]: A dictionary where keys are
            original Python types (`TypeRegistry`'s `py_type`), and values are
            tuples containing a `SQLTypeAdapter` instance and the target
            Python type (`TypeRegistry`'s `db_type`) expected by the driver.
        """
        suggestions: Dict[Type, Tuple['SQLTypeAdapter', Type]] = {}

        # Define a list of desired Python type to DB driver type mappings.
        # This list reflects types seen in test fixtures and common usage,
        # along with their preferred database-compatible Python types for the driver.
        # Types that are natively compatible with the DB driver (e.g., Python str, int, float)
        # and for which no specific conversion logic is needed are omitted from this list.
        # The consuming layer should assume pass-through behavior for any Python type
        # that does not have an explicit adapter suggestion.
        #
        # Exception: If a user requires specific processing for a natively compatible type
        # (e.g., custom serialization/deserialization for JSON strings beyond basic conversion),
        # they would need to implement and register their own specialized adapter.
        # This backend's default suggestions do not cater to such advanced processing needs.
        from datetime import date, datetime, time
        from decimal import Decimal
        from uuid import UUID
        from enum import Enum

        type_mappings = [
            (bool, bool),  # Python bool -> DB driver bool (PostgreSQL BOOLEAN)
            # Why str for date/time?
            # PostgreSQL has native DATE, TIME, TIMESTAMP types but accepts string representations
            (datetime, str),  # Python datetime -> DB driver str (PostgreSQL TIMESTAMP)
            (date, str),      # Python date -> DB driver str (PostgreSQL DATE)
            (time, str),      # Python time -> DB driver str (PostgreSQL TIME)
            (Decimal, Decimal),  # Python Decimal -> DB driver Decimal (PostgreSQL NUMERIC/DECIMAL)
            (UUID, str),      # Python UUID -> DB driver str (PostgreSQL UUID type)
            (dict, str),      # Python dict -> DB driver str (PostgreSQL JSON/JSONB)
            (list, list),     # Python list -> DB driver list (PostgreSQL arrays - psycopg handles natively)
            (Enum, str),      # Python Enum -> DB driver str (PostgreSQL TEXT)
        ]

        # Iterate through the defined mappings and retrieve adapters from the registry.
        for py_type, db_type in type_mappings:
            adapter = self.adapter_registry.get_adapter(py_type, db_type)
            if adapter:
                suggestions[py_type] = (adapter, db_type)
            else:
                # Log a debug message if a specific adapter is expected but not found.
                self.log(logging.DEBUG, f"No adapter found for ({py_type.__name__}, {db_type.__name__}). "
                              "Suggestion will not be provided for this type.")

        return suggestions


class PostgresExtensionMixin:
    """PostgreSQL extension detection implementation.

    Known extensions definition with minimum version requirements and descriptions.
    """

    KNOWN_EXTENSIONS = {
        'postgis': {
            'min_version': '2.0',
            'description': 'PostGIS spatial database extension',
            'category': 'spatial',
            'documentation': 'https://postgis.net/docs/',
            'repository': 'https://postgis.net/',
        },
        'vector': {
            'min_version': '0.1',
            'description': 'pgvector vector similarity search',
            'category': 'vector',
            'documentation': 'https://github.com/pgvector/pgvector',
            'repository': 'https://github.com/pgvector/pgvector',
        },
        'pg_trgm': {
            'min_version': '1.0',
            'description': 'Trigram similarity search',
            'category': 'text',
            'documentation': 'https://www.postgresql.org/docs/current/pgtrgm.html',
        },
        'hstore': {
            'min_version': '1.0',
            'description': 'Key-value pair storage',
            'category': 'data',
            'documentation': 'https://www.postgresql.org/docs/current/hstore.html',
        },
        'uuid-ossp': {
            'min_version': '1.0',
            'description': 'UUID generation functions',
            'category': 'utility',
            'documentation': 'https://www.postgresql.org/docs/current/uuid-ossp.html',
        },
        'pgcrypto': {
            'min_version': '1.0',
            'description': 'Cryptographic functions',
            'category': 'security',
            'documentation': 'https://www.postgresql.org/docs/current/pgcrypto.html',
        },
        'ltree': {
            'min_version': '1.0',
            'description': 'Label tree for hierarchical data',
            'category': 'data',
            'documentation': 'https://www.postgresql.org/docs/current/ltree.html',
        },
        'intarray': {
            'min_version': '1.0',
            'description': 'Integer array operators and indexes',
            'category': 'data',
            'documentation': 'https://www.postgresql.org/docs/current/intarray.html',
        },
        'earthdistance': {
            'min_version': '1.0',
            'description': 'Great-circle distance calculations',
            'category': 'spatial',
            'documentation': 'https://www.postgresql.org/docs/current/earthdistance.html',
            'dependencies': ['cube'],
        },
        'tablefunc': {
            'min_version': '1.0',
            'description': 'Table functions (crosstab, connectby)',
            'category': 'utility',
            'documentation': 'https://www.postgresql.org/docs/current/tablefunc.html',
        },
        'pg_stat_statements': {
            'min_version': '1.0',
            'description': 'Query execution statistics',
            'category': 'monitoring',
            'documentation': 'https://www.postgresql.org/docs/current/pgstatstatements.html',
            'requires_preload': True,
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
                    name=ext_name,
                    installed=True,
                    version=row[1],
                    schema=row[2]
                )

            # Add known but not installed extensions
            for known_ext in self.KNOWN_EXTENSIONS:
                if known_ext not in extensions:
                    extensions[known_ext] = PostgresExtensionInfo(
                        name=known_ext,
                        installed=False
                    )

            return extensions
        finally:
            cursor.close()

    def is_extension_installed(self, name: str) -> bool:
        """Check if extension is installed."""
        if not hasattr(self, '_extensions'):
            return False
        info = self._extensions.get(name)
        return info.installed if info else False

    def get_extension_version(self, name: str) -> Optional[str]:
        """Get extension version."""
        if not hasattr(self, '_extensions'):
            return None
        info = self._extensions.get(name)
        return info.version if info else None

    def get_extension_info(self, name: str) -> Optional[PostgresExtensionInfo]:
        """Get extension info."""
        if not hasattr(self, '_extensions'):
            return None
        return self._extensions.get(name)

    def _compare_versions(self, v1: str, v2: str) -> int:
        """Compare version numbers.

        Returns:
            Positive if v1 > v2, negative if v1 < v2, 0 if equal
        """
        def parse_version(v):
            parts = []
            for part in v.split('.'):
                try:
                    parts.append(int(part))
                except ValueError:
                    # Handle versions like "0.5.0.dev"
                    num = ''.join(c for c in part if c.isdigit())
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


class PostgresMaterializedViewMixin:
    """PostgreSQL materialized view extended features implementation."""

    def supports_materialized_view_concurrent_refresh(self) -> bool:
        """CONCURRENTLY is supported since PostgreSQL 9.4."""
        return self.version >= (9, 4, 0)


class PostgresTableMixin:
    """PostgreSQL table extended features implementation."""

    def supports_table_inheritance(self) -> bool:
        """PostgreSQL supports table inheritance."""
        return True


class PostgresPgvectorMixin:
    """pgvector vector similarity search implementation."""

    def supports_pgvector_type(self) -> bool:
        """Check if pgvector extension is installed."""
        return self.is_extension_installed('vector')

    def supports_pgvector_similarity_search(self) -> bool:
        """Check if pgvector extension is installed."""
        return self.is_extension_installed('vector')

    def supports_pgvector_ivfflat_index(self) -> bool:
        """IVFFlat index requires pgvector."""
        return self.is_extension_installed('vector')

    def supports_pgvector_hnsw_index(self) -> bool:
        """HNSW index requires pgvector 0.5.0+."""
        if not self.is_extension_installed('vector'):
            return False
        version = self.get_extension_version('vector')
        if version:
            return self._compare_versions(version, '0.5.0') >= 0
        return False


class PostgresPostGISMixin:
    """PostGIS spatial functionality implementation."""

    def supports_postgis_geometry_type(self) -> bool:
        """Check if PostGIS extension is installed."""
        return self.is_extension_installed('postgis')

    def supports_postgis_geography_type(self) -> bool:
        """Check if PostGIS extension is installed."""
        return self.is_extension_installed('postgis')

    def supports_postgis_spatial_index(self) -> bool:
        """Check if PostGIS extension is installed."""
        return self.is_extension_installed('postgis')

    def supports_postgis_spatial_functions(self) -> bool:
        """Check if PostGIS extension is installed."""
        return self.is_extension_installed('postgis')


class PostgresPgTrgmMixin:
    """pg_trgm trigram functionality implementation."""

    def supports_pg_trgm_similarity(self) -> bool:
        """Check if pg_trgm extension is installed."""
        return self.is_extension_installed('pg_trgm')

    def supports_pg_trgm_index(self) -> bool:
        """Check if pg_trgm extension is installed."""
        return self.is_extension_installed('pg_trgm')


class PostgresHstoreMixin:
    """hstore key-value storage functionality implementation."""

    def supports_hstore_type(self) -> bool:
        """Check if hstore extension is installed."""
        return self.is_extension_installed('hstore')

    def supports_hstore_operators(self) -> bool:
        """Check if hstore extension is installed."""
        return self.is_extension_installed('hstore')
