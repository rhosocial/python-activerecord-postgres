# src/rhosocial/activerecord/backend/impl/postgres/protocols/ddl/__init__.py
"""DDL-related PostgreSQL protocols."""

from .partition import PostgresPartitionSupport
from .index import PostgresIndexSupport
from .trigger import PostgresTriggerSupport
from .comment import PostgresCommentSupport
from .type import PostgresTypeSupport
from .constraint import PostgresConstraintSupport
from .policy import PostgresPolicySupport
from .rls_config import PostgresRlsConfigSupport
from .table_settings import PostgresAlterTableSettingsSupport
from .cluster import PostgresClusterSupport
from .domain import PostgresDomainSupport
from .collation import PostgresCollationDDLSupport
from .foreign_table import PostgresForeignTableDDLSupport
from .routine import PostgresRoutineDDLSupport
from .publication import PostgresPublicationSupport

__all__ = [
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
]
