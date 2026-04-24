# src/rhosocial/activerecord/backend/impl/postgres/mixins/__init__.py
"""PostgreSQL mixin implementations.

This package organizes PostgreSQL-specific mixins by category:
- extension: Extension detection and management
- materialized_view: Materialized view features
- table: Table features
- ddl: DDL-related mixins (partition, index, trigger, etc.)
- dml: DML-related mixins (vacuum, stored procedure, etc.)
- types: Data type mixins (enum, multirange, etc.)
- extensions: Extension mixins (pgvector, postgis, hstore, etc.)
"""

from .extension import PostgresExtensionMixin
from .materialized_view import PostgresMaterializedViewMixin
from .table import PostgresTableMixin

# DDL mixins
from .ddl.partition import PostgresPartitionMixin
from .ddl.index import PostgresIndexMixin
from .ddl.trigger import PostgresTriggerMixin
from .ddl.comment import PostgresCommentMixin
from .ddl.type import PostgresTypeMixin
from .ddl.constraint import PostgresConstraintMixin

# DML mixins
from .dml.vacuum import PostgresVacuumMixin
from .dml.stored_procedure import PostgresStoredProcedureMixin
from .dml.extended_statistics import PostgresExtendedStatisticsMixin
from .dml.advisory_lock import PostgresAdvisoryLockMixin
from .dml.locking import PostgresLockingMixin

# Type mixins
from .types.enum import EnumTypeMixin
from .types.data_type import PostgresDataTypeMixin as TypesDataTypeMixin
from .types.multirange import MultirangeMixin

# Extension mixins
from .extensions.pgvector import PostgresPgvectorMixin
from .extensions.postgis import PostgresPostGISMixin
from .extensions.postgis_raster import PostgresPostgisRasterMixin
from .extensions.pgrouting import PostgresPgroutingMixin
from .extensions.hstore import PostgresHstoreMixin
from .extensions.ltree import PostgresLtreeMixin
from .extensions.intarray import PostgresIntarrayMixin
from .extensions.pg_trgm import PostgresPgTrgmMixin
from .extensions.earthdistance import PostgresEarthdistanceMixin
from .extensions.tablefunc import PostgresTablefuncMixin
from .extensions.pg_stat_statements import PostgresPgStatStatementsMixin
from .extensions.citext import PostgresCitextMixin
from .extensions.pgcrypto import PostgresPgcryptoMixin
from .extensions.fuzzystrmatch import PostgresFuzzystrmatchMixin
from .extensions.cube import PostgresCubeMixin
from .extensions.uuid_ossp import PostgresUuidOssMixin
from .extensions.bloom import PostgresBloomMixin
from .extensions.btree_gin import PostgresBtreeGinMixin
from .extensions.btree_gist import PostgresBtreeGistMixin

# Additional mixins
from .query_optimization import PostgresQueryOptimizationMixin
from .data_type import PostgresDataTypeMixin
from .sql_syntax import PostgresSQLSyntaxMixin
from .logical_replication import PostgresLogicalReplicationMixin

# Introspection capability mixin
from .introspection import PostgresIntrospectionCapabilityMixin

__all__ = [
    # Core mixins
    "PostgresExtensionMixin",
    "PostgresMaterializedViewMixin",
    "PostgresTableMixin",
    # DDL mixins
    "PostgresPartitionMixin",
    "PostgresIndexMixin",
    "PostgresTriggerMixin",
    "PostgresCommentMixin",
    "PostgresTypeMixin",
    # DML mixins
    "PostgresVacuumMixin",
    "PostgresStoredProcedureMixin",
    "PostgresExtendedStatisticsMixin",
    "PostgresAdvisoryLockMixin",
    "PostgresLockingMixin",
    # Type mixins
    "EnumTypeMixin",
    "TypesDataTypeMixin",
    "MultirangeMixin",
    # Extension mixins
    "PostgresPgvectorMixin",
    "PostgresPostGISMixin",
    "PostgresPostgisRasterMixin",
    "PostgresPgroutingMixin",
    "PostgresHstoreMixin",
    "PostgresLtreeMixin",
    "PostgresIntarrayMixin",
    "PostgresPgTrgmMixin",
    "PostgresEarthdistanceMixin",
    "PostgresTablefuncMixin",
    "PostgresPgStatStatementsMixin",
    "PostgresCitextMixin",
    "PostgresPgcryptoMixin",
    "PostgresFuzzystrmatchMixin",
    "PostgresCubeMixin",
    "PostgresUuidOssMixin",
    "PostgresBloomMixin",
    "PostgresBtreeGinMixin",
    "PostgresBtreeGistMixin",
    # Additional mixins
    "PostgresQueryOptimizationMixin",
    "PostgresDataTypeMixin",
    "PostgresSQLSyntaxMixin",
    "PostgresLogicalReplicationMixin",
    # Introspection capability mixin
    "PostgresIntrospectionCapabilityMixin",
]
