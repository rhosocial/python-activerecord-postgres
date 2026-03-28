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

# DML protocols
from .dml.vacuum import PostgresVacuumSupport
from .dml.stored_procedure import PostgresStoredProcedureSupport
from .dml.extended_statistics import PostgresExtendedStatisticsSupport

# Type protocols
from .types.data_type import PostgresDataTypeSupport
from .types.multirange import MultirangeSupport
from .types.enum import EnumTypeSupport

# Extension protocols
from .extensions.pgvector import PostgresPgvectorSupport
from .extensions.postgis import PostgresPostGISSupport
from .extensions.hstore import PostgresHstoreSupport
from .extensions.ltree import PostgresLtreeSupport
from .extensions.intarray import PostgresIntarraySupport
from .extensions.pg_trgm import PostgresPgTrgmSupport
from .extensions.earthdistance import PostgresEarthdistanceSupport
from .extensions.tablefunc import PostgresTablefuncSupport
from .extensions.pg_stat_statements import PostgresPgStatStatementsSupport

# Additional protocols from original file
from .query_optimization import PostgresQueryOptimizationSupport
from .sql_syntax import PostgresSQLSyntaxSupport
from .logical_replication import PostgresLogicalReplicationSupport
from .parallel_query import PostgresParallelQuerySupport

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
    # DML protocols
    "PostgresVacuumSupport",
    "PostgresStoredProcedureSupport",
    "PostgresExtendedStatisticsSupport",
    # Type protocols
    "PostgresDataTypeSupport",
    "MultirangeSupport",
    "EnumTypeSupport",
    # Extension protocols
    "PostgresPgvectorSupport",
    "PostgresPostGISSupport",
    "PostgresHstoreSupport",
    "PostgresLtreeSupport",
    "PostgresIntarraySupport",
    "PostgresPgTrgmSupport",
    "PostgresEarthdistanceSupport",
    "PostgresTablefuncSupport",
    "PostgresPgStatStatementsSupport",
    # Additional protocols
    "PostgresQueryOptimizationSupport",
    "PostgresSQLSyntaxSupport",
    "PostgresLogicalReplicationSupport",
    "PostgresParallelQuerySupport",
]
