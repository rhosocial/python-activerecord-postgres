# src/rhosocial/activerecord/backend/impl/postgres/mixins.py
"""PostgreSQL dialect-specific Mixin implementations."""
from typing import Dict, Optional, Tuple, Type, List
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
            'features': {
                'geometry_type': {'min_version': '2.0'},
                'geography_type': {'min_version': '2.0'},
                'spatial_index': {'min_version': '2.0'},
                'spatial_functions': {'min_version': '2.0'},
            },
        },
        'vector': {
            'min_version': '0.1',
            'description': 'pgvector vector similarity search',
            'category': 'vector',
            'documentation': 'https://github.com/pgvector/pgvector',
            'repository': 'https://github.com/pgvector/pgvector',
            'features': {
                'type': {'min_version': '0.1'},
                'similarity_search': {'min_version': '0.1'},
                'ivfflat_index': {'min_version': '0.1'},
                'hnsw_index': {'min_version': '0.5.0'},
            },
        },
        'pg_trgm': {
            'min_version': '1.0',
            'description': 'Trigram similarity search',
            'category': 'text',
            'documentation': 'https://www.postgresql.org/docs/current/pgtrgm.html',
            'features': {
                'similarity': {'min_version': '1.0'},
                'index': {'min_version': '1.0'},
            },
        },
        'hstore': {
            'min_version': '1.0',
            'description': 'Key-value pair storage',
            'category': 'data',
            'documentation': 'https://www.postgresql.org/docs/current/hstore.html',
            'features': {
                'type': {'min_version': '1.0'},
                'operators': {'min_version': '1.0'},
            },
        },
        'uuid-ossp': {
            'min_version': '1.0',
            'description': 'UUID generation functions',
            'category': 'utility',
            'documentation': 'https://www.postgresql.org/docs/current/uuid-ossp.html',
            'features': {},
        },
        'pgcrypto': {
            'min_version': '1.0',
            'description': 'Cryptographic functions',
            'category': 'security',
            'documentation': 'https://www.postgresql.org/docs/current/pgcrypto.html',
            'features': {},
        },
        'ltree': {
            'min_version': '1.0',
            'description': 'Label tree for hierarchical data',
            'category': 'data',
            'documentation': 'https://www.postgresql.org/docs/current/ltree.html',
            'features': {
                'type': {'min_version': '1.0'},
                'operators': {'min_version': '1.0'},
                'index': {'min_version': '1.0'},
            },
        },
        'intarray': {
            'min_version': '1.0',
            'description': 'Integer array operators and indexes',
            'category': 'data',
            'documentation': 'https://www.postgresql.org/docs/current/intarray.html',
            'features': {
                'operators': {'min_version': '1.0'},
                'functions': {'min_version': '1.0'},
                'index': {'min_version': '1.0'},
            },
        },
        'earthdistance': {
            'min_version': '1.0',
            'description': 'Great-circle distance calculations',
            'category': 'spatial',
            'documentation': 'https://www.postgresql.org/docs/current/earthdistance.html',
            'dependencies': ['cube'],
            'features': {
                'type': {'min_version': '1.0'},
                'operators': {'min_version': '1.0'},
            },
        },
        'tablefunc': {
            'min_version': '1.0',
            'description': 'Table functions (crosstab, connectby)',
            'category': 'utility',
            'documentation': 'https://www.postgresql.org/docs/current/tablefunc.html',
            'features': {
                'crosstab': {'min_version': '1.0'},
                'connectby': {'min_version': '1.0'},
                'normal_rand': {'min_version': '1.0'},
            },
        },
        'pg_stat_statements': {
            'min_version': '1.0',
            'description': 'Query execution statistics',
            'category': 'monitoring',
            'documentation': 'https://www.postgresql.org/docs/current/pgstatstatements.html',
            'requires_preload': True,
            'features': {
                'view': {'min_version': '1.0'},
                'reset': {'min_version': '1.0'},
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
        if not ext_config or 'features' not in ext_config:
            # No feature requirements defined, assume supported if installed
            return True

        # Get feature requirements
        feature_config = ext_config.get('features', {}).get(feature_name)
        if not feature_config:
            # Feature not defined, assume supported if extension installed
            return True

        # Check version requirement
        min_version = feature_config.get('min_version')
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

        feature_config = ext_config.get('features', {}).get(feature_name)
        if not feature_config:
            return None

        return feature_config.get('min_version')

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
        """Check if pgvector supports vector type."""
        return self.check_extension_feature('vector', 'type')

    def supports_pgvector_similarity_search(self) -> bool:
        """Check if pgvector supports similarity search."""
        return self.check_extension_feature('vector', 'similarity_search')

    def supports_pgvector_ivfflat_index(self) -> bool:
        """Check if pgvector supports IVFFlat index."""
        return self.check_extension_feature('vector', 'ivfflat_index')

    def supports_pgvector_hnsw_index(self) -> bool:
        """Check if pgvector supports HNSW index (requires 0.5.0+)."""
        return self.check_extension_feature('vector', 'hnsw_index')


class PostgresPostGISMixin:
    """PostGIS spatial functionality implementation."""

    def supports_postgis_geometry_type(self) -> bool:
        """Check if PostGIS supports geometry type."""
        return self.check_extension_feature('postgis', 'geometry_type')

    def supports_postgis_geography_type(self) -> bool:
        """Check if PostGIS supports geography type."""
        return self.check_extension_feature('postgis', 'geography_type')

    def supports_postgis_spatial_index(self) -> bool:
        """Check if PostGIS supports spatial index."""
        return self.check_extension_feature('postgis', 'spatial_index')

    def supports_postgis_spatial_functions(self) -> bool:
        """Check if PostGIS supports spatial functions."""
        return self.check_extension_feature('postgis', 'spatial_functions')


class PostgresPgTrgmMixin:
    """pg_trgm trigram functionality implementation."""

    def supports_pg_trgm_similarity(self) -> bool:
        """Check if pg_trgm supports similarity functions."""
        return self.check_extension_feature('pg_trgm', 'similarity')

    def supports_pg_trgm_index(self) -> bool:
        """Check if pg_trgm supports trigram index."""
        return self.check_extension_feature('pg_trgm', 'index')


class PostgresHstoreMixin:
    """hstore key-value storage functionality implementation."""

    def supports_hstore_type(self) -> bool:
        """Check if hstore supports hstore type."""
        return self.check_extension_feature('hstore', 'type')

    def supports_hstore_operators(self) -> bool:
        """Check if hstore supports operators."""
        return self.check_extension_feature('hstore', 'operators')


# =============================================================================
# Native Feature Mixins (PostgreSQL Built-in Features)
# =============================================================================


class PostgresPartitionMixin:
    """PostgreSQL partitioning enhancements implementation.

    All features are native, using version number for detection.
    """

    def supports_hash_partitioning(self) -> bool:
        """HASH partitioning is native feature, PG 11+."""
        return self.version >= (11, 0, 0)

    def supports_default_partition(self) -> bool:
        """DEFAULT partition is native feature, PG 11+."""
        return self.version >= (11, 0, 0)

    def supports_partition_key_update(self) -> bool:
        """Partition key row movement is native feature, PG 11+."""
        return self.version >= (11, 0, 0)

    def supports_concurrent_detach(self) -> bool:
        """Concurrent DETACH is native feature, PG 14+."""
        return self.version >= (14, 0, 0)

    def supports_partition_bounds_expression(self) -> bool:
        """Partition bounds expression is native feature, PG 12+."""
        return self.version >= (12, 0, 0)

    def supports_partitionwise_join(self) -> bool:
        """Partitionwise join is native feature, PG 11+."""
        return self.version >= (11, 0, 0)

    def supports_partitionwise_aggregate(self) -> bool:
        """Partitionwise aggregate is native feature, PG 11+."""
        return self.version >= (11, 0, 0)


class PostgresIndexMixin:
    """PostgreSQL index enhancements implementation.

    All features are native, using version number for detection.
    """

    def supports_safe_hash_index(self) -> bool:
        """Hash index WAL logging is native feature, PG 10+."""
        return self.version >= (10, 0, 0)

    def supports_parallel_create_index(self) -> bool:
        """Parallel index build is native feature, PG 11+."""
        return self.version >= (11, 0, 0)

    def supports_gist_include(self) -> bool:
        """GiST INCLUDE is native feature, PG 12+."""
        return self.version >= (12, 0, 0)

    def supports_reindex_concurrently(self) -> bool:
        """REINDEX CONCURRENTLY is native feature, PG 12+."""
        return self.version >= (12, 0, 0)

    def supports_btree_deduplication(self) -> bool:
        """B-tree deduplication is native feature, PG 13+."""
        return self.version >= (13, 0, 0)

    def supports_brin_multivalue(self) -> bool:
        """BRIN multivalue is native feature, PG 14+."""
        return self.version >= (14, 0, 0)

    def supports_brin_bloom(self) -> bool:
        """BRIN bloom filter is native feature, PG 14+."""
        return self.version >= (14, 0, 0)

    def supports_spgist_include(self) -> bool:
        """SP-GiST INCLUDE is native feature, PG 14+."""
        return self.version >= (14, 0, 0)


class PostgresVacuumMixin:
    """PostgreSQL VACUUM enhancements implementation.

    All features are native, using version number for detection.
    """

    def supports_parallel_vacuum(self) -> bool:
        """Parallel VACUUM is native feature, PG 13+."""
        return self.version >= (13, 0, 0)

    def supports_index_cleanup_auto(self) -> bool:
        """INDEX_CLEANUP AUTO is native feature, PG 14+."""
        return self.version >= (14, 0, 0)

    def supports_vacuum_process_toast(self) -> bool:
        """PROCESS_TOAST control is native feature, PG 14+."""
        return self.version >= (14, 0, 0)


class PostgresQueryOptimizationMixin:
    """PostgreSQL query optimization implementation.

    All features are native, using version number for detection.
    """

    def supports_jit(self) -> bool:
        """JIT compilation is native feature, PG 11+."""
        return self.version >= (11, 0, 0)

    def supports_incremental_sort(self) -> bool:
        """Incremental sort is native feature, PG 13+."""
        return self.version >= (13, 0, 0)

    def supports_memoize(self) -> bool:
        """Memoize is native feature, PG 14+."""
        return self.version >= (14, 0, 0)

    def supports_async_foreign_scan(self) -> bool:
        """Async foreign scan is native feature, PG 14+."""
        return self.version >= (14, 0, 0)


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


class PostgresSQLSyntaxMixin:
    """PostgreSQL SQL syntax enhancements implementation.

    All features are native, using version number for detection.
    """

    def supports_generated_columns(self) -> bool:
        """Generated columns are native feature, PG 12+."""
        return self.version >= (12, 0, 0)

    def supports_cte_search_cycle(self) -> bool:
        """CTE SEARCH/CYCLE is native feature, PG 14+."""
        return self.version >= (14, 0, 0)

    def supports_fetch_with_ties(self) -> bool:
        """FETCH WITH TIES is native feature, PG 13+."""
        return self.version >= (13, 0, 0)


class PostgresLogicalReplicationMixin:
    """PostgreSQL logical replication enhancements implementation.

    All features are native, using version number for detection.
    """

    def supports_commit_timestamp(self) -> bool:
        """Commit timestamp is native feature, PG 10+."""
        return self.version >= (10, 0, 0)

    def supports_streaming_transactions(self) -> bool:
        """Streaming transactions is native feature, PG 14+."""
        return self.version >= (14, 0, 0)

    def supports_two_phase_decoding(self) -> bool:
        """Two-phase decoding is native feature, PG 14+."""
        return self.version >= (14, 0, 0)

    def supports_binary_replication(self) -> bool:
        """Binary replication is native feature, PG 14+."""
        return self.version >= (14, 0, 0)


# =============================================================================
# DDL Feature Mixins (PostgreSQL-specific DDL extensions)
# =============================================================================


class PostgresTriggerMixin:
    """PostgreSQL trigger DDL implementation.

    PostgreSQL uses 'EXECUTE FUNCTION func_name()' syntax instead of
    the SQL:1999 standard 'EXECUTE func_name'.
    """

    def supports_trigger_referencing(self) -> bool:
        """REFERENCING clause is supported since PostgreSQL 10."""
        return self.version >= (10, 0, 0)

    def supports_trigger_when(self) -> bool:
        """WHEN condition is supported in all modern versions."""
        return True

    def supports_statement_trigger(self) -> bool:
        """FOR EACH STATEMENT is supported."""
        return True

    def supports_instead_of_trigger(self) -> bool:
        """INSTEAD OF triggers are supported."""
        return True

    def supports_trigger_if_not_exists(self) -> bool:
        """IF NOT EXISTS is supported since PostgreSQL 9.5."""
        return self.version >= (9, 5, 0)

    def format_create_trigger_statement(self, expr) -> Tuple[str, tuple]:
        """Format CREATE TRIGGER statement (PostgreSQL syntax).

        PostgreSQL uses 'EXECUTE FUNCTION func_name()' instead of standard 'EXECUTE func_name'.
        """
        parts = ["CREATE TRIGGER"]

        if expr.if_not_exists and self.supports_trigger_if_not_exists():
            parts.append("IF NOT EXISTS")

        parts.append(self.format_identifier(expr.trigger_name))

        parts.append(expr.timing.value)

        if expr.update_columns:
            cols = ", ".join(self.format_identifier(c) for c in expr.update_columns)
            events_str = f"UPDATE OF {cols}"
        else:
            events_str = " OR ".join(e.value for e in expr.events)
        parts.append(events_str)

        parts.append("ON")
        parts.append(self.format_identifier(expr.table_name))

        if expr.referencing and self.supports_trigger_referencing():
            parts.append(expr.referencing)

        if expr.level:
            parts.append(expr.level.value)

        all_params = []
        if expr.condition and self.supports_trigger_when():
            cond_sql, cond_params = expr.condition.to_sql()
            parts.append(f"WHEN ({cond_sql})")
            all_params.extend(cond_params)

        parts.append("EXECUTE FUNCTION")
        parts.append(expr.function_name + "()")

        return " ".join(parts), tuple(all_params)

    def format_drop_trigger_statement(self, expr) -> Tuple[str, tuple]:
        """Format DROP TRIGGER statement (PostgreSQL syntax)."""
        parts = ["DROP TRIGGER"]

        if expr.if_exists:
            parts.append("IF EXISTS")

        parts.append(self.format_identifier(expr.trigger_name))

        if expr.table_name:
            parts.append("ON")
            parts.append(self.format_identifier(expr.table_name))

        return " ".join(parts), ()


class PostgresCommentMixin:
    """PostgreSQL COMMENT ON implementation."""

    def format_comment_statement(
        self,
        object_type: str,
        object_name: str,
        comment
    ) -> Tuple[str, tuple]:
        """Format COMMENT ON statement (PostgreSQL-specific).

        Args:
            object_type: Object type (TABLE, COLUMN, VIEW, etc.)
            object_name: Object name (e.g., 'table_name' or 'table_name.column_name')
            comment: Comment text, or None to drop comment

        Returns:
            Tuple of (SQL string, parameters)
        """
        if comment is None:
            comment_value = "NULL"
        else:
            comment_value = self.get_parameter_placeholder()

        parts = ["COMMENT ON", object_type, self.format_identifier(object_name)]
        parts.append("IS")
        parts.append(comment_value)

        sql = " ".join(parts)

        if comment is None:
            return sql, ()
        else:
            return sql, (comment,)


class PostgresTypeMixin:
    """PostgreSQL type DDL implementation.

    PostgreSQL supports user-defined types, particularly ENUM types.
    """

    def supports_create_type(self) -> bool:
        """CREATE TYPE is supported."""
        return True

    def supports_drop_type(self) -> bool:
        """DROP TYPE is supported."""
        return True

    def supports_type_if_not_exists(self) -> bool:
        """PostgreSQL does NOT support IF NOT EXISTS for CREATE TYPE."""
        return False

    def supports_type_if_exists(self) -> bool:
        """DROP TYPE IF EXISTS is supported."""
        return True

    def supports_type_cascade(self) -> bool:
        """DROP TYPE CASCADE is supported."""
        return True

    def format_create_type_enum_statement(
        self,
        name: str,
        values: List[str],
        schema: Optional[str] = None
    ) -> Tuple[str, tuple]:
        """Format CREATE TYPE ... AS ENUM statement.

        PostgreSQL syntax:
        CREATE TYPE [schema.]name AS ENUM ('value1', 'value2', ...);

        Args:
            name: Type name
            values: List of enum values
            schema: Optional schema name

        Returns:
            Tuple of (SQL string, parameters tuple)
        """
        if not values:
            raise ValueError("ENUM type must have at least one value")

        type_name = self.format_identifier(name)
        if schema:
            type_name = f"{self.format_identifier(schema)}.{type_name}"

        escaped_values = [f"'{v}'" for v in values]
        values_str = ", ".join(escaped_values)

        sql = f"CREATE TYPE {type_name} AS ENUM ({values_str})"
        return sql, ()

    def format_drop_type_statement(
        self,
        name: str,
        schema: Optional[str] = None,
        if_exists: bool = False,
        cascade: bool = False
    ) -> Tuple[str, tuple]:
        """Format DROP TYPE statement.

        PostgreSQL syntax:
        DROP TYPE [IF EXISTS] [schema.]name [CASCADE];

        Args:
            name: Type name
            schema: Optional schema name
            if_exists: Whether to add IF EXISTS
            cascade: Whether to add CASCADE

        Returns:
            Tuple of (SQL string, parameters tuple)
        """
        parts = ["DROP TYPE"]

        if if_exists:
            parts.append("IF EXISTS")

        type_name = self.format_identifier(name)
        if schema:
            type_name = f"{self.format_identifier(schema)}.{type_name}"
        parts.append(type_name)

        if cascade:
            parts.append("CASCADE")

        return " ".join(parts), ()


# =============================================================================
# Extension Feature Mixins (Additional PostgreSQL Extensions)
# =============================================================================


class PostgresLtreeMixin:
    """ltree label tree implementation."""

    def supports_ltree_type(self) -> bool:
        """Check if ltree supports ltree type."""
        return self.check_extension_feature('ltree', 'type')

    def supports_ltree_operators(self) -> bool:
        """Check if ltree supports operators."""
        return self.check_extension_feature('ltree', 'operators')

    def supports_ltree_index(self) -> bool:
        """Check if ltree supports index."""
        return self.check_extension_feature('ltree', 'index')


class PostgresIntarrayMixin:
    """intarray integer array implementation."""

    def supports_intarray_operators(self) -> bool:
        """Check if intarray supports operators."""
        return self.check_extension_feature('intarray', 'operators')

    def supports_intarray_functions(self) -> bool:
        """Check if intarray supports functions."""
        return self.check_extension_feature('intarray', 'functions')

    def supports_intarray_index(self) -> bool:
        """Check if intarray supports index."""
        return self.check_extension_feature('intarray', 'index')


class PostgresEarthdistanceMixin:
    """earthdistance earth distance implementation."""

    def supports_earthdistance_type(self) -> bool:
        """Check if earthdistance supports earth type."""
        return self.check_extension_feature('earthdistance', 'type')

    def supports_earthdistance_operators(self) -> bool:
        """Check if earthdistance supports operators."""
        return self.check_extension_feature('earthdistance', 'operators')


class PostgresTablefuncMixin:
    """tablefunc table functions implementation."""

    def supports_tablefunc_crosstab(self) -> bool:
        """Check if tablefunc supports crosstab."""
        return self.check_extension_feature('tablefunc', 'crosstab')

    def supports_tablefunc_connectby(self) -> bool:
        """Check if tablefunc supports connectby."""
        return self.check_extension_feature('tablefunc', 'connectby')

    def supports_tablefunc_normal_rand(self) -> bool:
        """Check if tablefunc supports normal_rand."""
        return self.check_extension_feature('tablefunc', 'normal_rand')


class PostgresPgStatStatementsMixin:
    """pg_stat_statements query statistics implementation."""

    def supports_pg_stat_statements_view(self) -> bool:
        """Check if pg_stat_statements supports view."""
        return self.check_extension_feature('pg_stat_statements', 'view')

    def supports_pg_stat_statements_reset(self) -> bool:
        """Check if pg_stat_statements supports reset."""
        return self.check_extension_feature('pg_stat_statements', 'reset')


class EnumTypeMixin:
    """Mixin providing PostgreSQL ENUM type formatting methods.

    This mixin implements the EnumTypeSupport protocol.
    """

    def format_enum_type_name(self, name: str, schema: Optional[str] = None) -> str:
        """Format enum type name with optional schema.

        Args:
            name: Type name
            schema: Optional schema name

        Returns:
            Formatted type name (e.g., 'schema.name' or 'name')
        """
        if schema:
            return f"{schema}.{name}"
        return name

    def format_enum_values(self, values: List[str]) -> str:
        """Format enum values list for SQL.

        Args:
            values: List of enum values

        Returns:
            SQL-formatted values string
        """
        return ', '.join(f"'{v}'" for v in values)

    def format_create_enum_type(self, name: str, values: List[str], schema: Optional[str] = None, if_not_exists: bool = False) -> str:
        """Format CREATE TYPE statement for enum.

        Args:
            name: Type name
            values: Allowed values
            schema: Optional schema
            if_not_exists: Add IF NOT EXISTS

        Returns:
            SQL statement
        """
        full_name = self.format_enum_type_name(name, schema)
        values_str = self.format_enum_values(values)
        exists_clause = "IF NOT EXISTS " if if_not_exists else ""
        return f"CREATE TYPE {exists_clause}{full_name} AS ENUM ({values_str})"

    def format_drop_enum_type(self, name: str, schema: Optional[str] = None, if_exists: bool = False, cascade: bool = False) -> str:
        """Format DROP TYPE statement.

        Args:
            name: Type name
            schema: Optional schema
            if_exists: Add IF EXISTS
            cascade: Add CASCADE

        Returns:
            SQL statement
        """
        full_name = self.format_enum_type_name(name, schema)
        exists_clause = "IF EXISTS " if if_exists else ""
        cascade_clause = " CASCADE" if cascade else ""
        return f"DROP TYPE {exists_clause}{full_name}{cascade_clause}"

    def format_alter_enum_add_value(self, type_name: str, new_value: str, schema: Optional[str] = None, before: Optional[str] = None, after: Optional[str] = None) -> str:
        """Format ALTER TYPE ADD VALUE statement.

        Args:
            type_name: Type name
            new_value: New value to add
            schema: Optional schema
            before: Add before this value
            after: Add after this value

        Returns:
            SQL statement
        """
        full_name = self.format_enum_type_name(type_name, schema)
        sql = f"ALTER TYPE {full_name} ADD VALUE '{new_value}'"
        if before:
            sql += f" BEFORE '{before}'"
        elif after:
            sql += f" AFTER '{after}'"
        return sql

    def create_enum_type(self, name: str, values: List[str], schema: Optional[str] = None, if_not_exists: bool = False) -> str:
        """Generate CREATE TYPE statement for enum.

        Implements EnumTypeSupport protocol.

        Args:
            name: Enum type name
            values: List of allowed values
            schema: Optional schema name
            if_not_exists: Add IF NOT EXISTS clause (PG 9.1+)

        Returns:
            SQL statement string
        """
        return self.format_create_enum_type(name, values, schema, if_not_exists)

    def drop_enum_type(self, name: str, schema: Optional[str] = None, if_exists: bool = False, cascade: bool = False) -> str:
        """Generate DROP TYPE statement for enum.

        Implements EnumTypeSupport protocol.

        Args:
            name: Enum type name
            schema: Optional schema name
            if_exists: Add IF EXISTS clause
            cascade: Add CASCADE clause

        Returns:
            SQL statement string
        """
        return self.format_drop_enum_type(name, schema, if_exists, cascade)

    def alter_enum_add_value(self, type_name: str, new_value: str, schema: Optional[str] = None, before: Optional[str] = None, after: Optional[str] = None) -> str:
        """Generate ALTER TYPE ADD VALUE statement.

        Implements EnumTypeSupport protocol.

        Note: Requires PostgreSQL 9.1+

        Args:
            type_name: Enum type name
            new_value: New value to add
            schema: Optional schema name
            before: Add before this value
            after: Add after this value

        Returns:
            SQL statement string
        """
        return self.format_alter_enum_add_value(type_name, new_value, schema, before, after)
