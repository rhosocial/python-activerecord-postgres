# src/rhosocial/activerecord/backend/impl/postgres/protocols/__init__.py
"""PostgreSQL protocol definitions.

This package organizes PostgreSQL-specific protocols by category:
- base: Common dataclasses and base protocols
- ddl: DDL-related protocols (partition, index, trigger, etc.)
- dml: DML-related protocols (vacuum, stored procedure, etc.)
- types: Data type protocols (multirange, enum, etc.)
- extensions: Extension protocols (pgvector, postgis, hstore, etc.)
"""

from .base import PostgresExtensionInfo
from .extension import PostgresExtensionSupport
from .materialized_view import PostgresMaterializedViewSupport
from .table import PostgresTableSupport

# DDL protocols
from .ddl.partition import PostgresPartitionSupport
from .ddl.index import PostgresIndexSupport
from .ddl.trigger import PostgresTriggerSupport
from .ddl.comment import PostgresCommentSupport
from .ddl.type import PostgresTypeSupport
from .ddl.constraint import PostgresConstraintSupport
from .ddl.policy import PostgresPolicySupport
from .ddl.rls_config import PostgresRlsConfigSupport
from .ddl.table_settings import PostgresAlterTableSettingsSupport
from .ddl.cluster import PostgresClusterSupport
from .ddl.domain import PostgresDomainSupport
from .ddl.collation import PostgresCollationDDLSupport
from .ddl.foreign_table import PostgresForeignTableDDLSupport
from .ddl.routine import PostgresRoutineDDLSupport
from .ddl.publication import PostgresPublicationSupport

# DML protocols
from .dml.vacuum import PostgresVacuumSupport
from .dml.stored_procedure import PostgresStoredProcedureSupport
from .dml.extended_statistics import PostgresExtendedStatisticsSupport
from .dml.advisory_lock import PostgresAdvisoryLockSupport
from .dml.locking import PostgresLockingSupport

# Type protocols
from .types.data_type import PostgresDataTypeSupport
from .types.multirange import PostgresMultirangeSupport
from .types.enum import PostgresEnumTypeSupport
from .types.full_text_search import PostgresFullTextSearchSupport
from .types.range_type import PostgresRangeTypeSupport
from .types.jsonb_enhanced import PostgresJSONBEnhancedSupport
from .types.array_enhanced import PostgresArrayEnhancedSupport

# Extension protocols
from .extensions.pgvector import PostgresPgvectorSupport
from .extensions.postgis import PostgresPostGISSupport
from .extensions.postgis_raster import PostgresPostgisRasterSupport
from .extensions.pgrouting import PostgresPgroutingSupport
from .extensions.hstore import PostgresHstoreSupport
from .extensions.ltree import PostgresLtreeSupport
from .extensions.intarray import PostgresIntarraySupport
from .extensions.pg_trgm import PostgresPgTrgmSupport
from .extensions.earthdistance import PostgresEarthdistanceSupport
from .extensions.tablefunc import PostgresTablefuncSupport
from .extensions.pg_stat_statements import PostgresPgStatStatementsSupport
from .extensions.citext import PostgresCitextSupport
from .extensions.pgcrypto import PostgresPgcryptoSupport
from .extensions.fuzzystrmatch import PostgresFuzzystrmatchSupport
from .extensions.cube import PostgresCubeSupport
from .extensions.uuid_ossp import PostgresUuidOssSupport
from .extensions.bloom import PostgresBloomSupport
from .extensions.btree_gin import PostgresBtreeGinSupport
from .extensions.btree_gist import PostgresBtreeGistSupport
from .extensions.pg_cron import PostgresPgCronSupport
from .extensions.pg_partman import PostgresPgPartmanSupport
from .extensions.pg_surgery import PostgresPgSurgerySupport
from .extensions.pg_walinspect import PostgresPgWalinspectSupport
from .extensions.pglogical import PostgresPgLogicalSupport
from .extensions.pgaudit import PostgresPgauditSupport
from .extensions.pg_repack import PostgresPgRepackSupport
from .extensions.hypopg import PostgresHypoPgSupport
from .extensions.orafce import PostgresOrafceSupport
from .extensions.address_standardizer import PostgresAddressStandardizerSupport

# Additional protocols from original file
from .query_optimization import PostgresQueryOptimizationSupport
from .logical_replication import PostgresLogicalReplicationSupport
from .parallel_query import PostgresParallelQuerySupport

# Per-feature protocol imports
from .cte import PostgresCTESupport
from .window import PostgresWindowSupport
from .filter_clause import PostgresFilterSupport
from .returning import PostgresReturningSupport
from .grouping import PostgresGroupingSupport
from .explain import PostgresExplainSupport
from .merge import PostgresMergeSupport
from .upsert import PostgresUpsertSupport
from .lateral_join import PostgresLateralJoinSupport
from .set_operation import PostgresSetOperationSupport
from .ilike import PostgresILIKESupport
from .join import PostgresJoinSupport
from .truncate import PostgresTruncateSupport
from .schema import PostgresSchemaSupport
from .sequence import PostgresSequenceSupport
from .transaction import PostgresTransactionSupport
from .view import PostgresViewSupport
from .xml import PostgresXMLSupport
from .collation import PostgresCollationSupport
from .ordered_set_aggregation import PostgresOrderedSetAggSupport
from .features import PostgresFeaturesSupport

__all__ = [
    # Base
    "PostgresExtensionInfo",
    # Core protocols
    "PostgresExtensionSupport",
    "PostgresMaterializedViewSupport",
    "PostgresTableSupport",
    # DDL protocols
    "PostgresPartitionSupport",
    "PostgresIndexSupport",
    "PostgresTriggerSupport",
    "PostgresCommentSupport",
    "PostgresTypeSupport",
    "PostgresConstraintSupport",
    "PostgresPolicySupport",
    "PostgresRlsConfigSupport",
    "PostgresAlterTableSettingsSupport",
    "PostgresClusterSupport",
    "PostgresDomainSupport",
    "PostgresCollationDDLSupport",
    "PostgresForeignTableDDLSupport",
    "PostgresRoutineDDLSupport",
    "PostgresPublicationSupport",
    # DML protocols
    "PostgresVacuumSupport",
    "PostgresStoredProcedureSupport",
    "PostgresExtendedStatisticsSupport",
    "PostgresAdvisoryLockSupport",
    "PostgresLockingSupport",
    # Type protocols
    "PostgresDataTypeSupport",
    "PostgresMultirangeSupport",
    "PostgresEnumTypeSupport",
    "PostgresFullTextSearchSupport",
    "PostgresRangeTypeSupport",
    "PostgresJSONBEnhancedSupport",
    "PostgresArrayEnhancedSupport",
    # Extension protocols
    "PostgresPgvectorSupport",
    "PostgresPostGISSupport",
    "PostgresPostgisRasterSupport",
    "PostgresPgroutingSupport",
    "PostgresHstoreSupport",
    "PostgresLtreeSupport",
    "PostgresIntarraySupport",
    "PostgresPgTrgmSupport",
    "PostgresEarthdistanceSupport",
    "PostgresTablefuncSupport",
    "PostgresPgStatStatementsSupport",
    "PostgresCitextSupport",
    "PostgresPgcryptoSupport",
    "PostgresFuzzystrmatchSupport",
    "PostgresCubeSupport",
    "PostgresUuidOssSupport",
    "PostgresBloomSupport",
    "PostgresBtreeGinSupport",
    "PostgresBtreeGistSupport",
    "PostgresPgCronSupport",
    "PostgresPgPartmanSupport",
    "PostgresPgSurgerySupport",
    "PostgresPgWalinspectSupport",
    "PostgresPgLogicalSupport",
    "PostgresPgauditSupport",
    "PostgresPgRepackSupport",
    "PostgresHypoPgSupport",
    "PostgresOrafceSupport",
    "PostgresAddressStandardizerSupport",
    # Additional protocols
    "PostgresQueryOptimizationSupport",
    "PostgresLogicalReplicationSupport",
    "PostgresParallelQuerySupport",
    # Per-feature protocols
    "PostgresCTESupport",
    "PostgresWindowSupport",
    "PostgresFilterSupport",
    "PostgresReturningSupport",
    "PostgresGroupingSupport",
    "PostgresExplainSupport",
    "PostgresMergeSupport",
    "PostgresUpsertSupport",
    "PostgresLateralJoinSupport",
    "PostgresSetOperationSupport",
    "PostgresILIKESupport",
    "PostgresJoinSupport",
    "PostgresTruncateSupport",
    "PostgresSchemaSupport",
    "PostgresSequenceSupport",
    "PostgresTransactionSupport",
    "PostgresViewSupport",
    "PostgresXMLSupport",
    "PostgresCollationSupport",
    "PostgresOrderedSetAggSupport",
    "PostgresFeaturesSupport",
]
