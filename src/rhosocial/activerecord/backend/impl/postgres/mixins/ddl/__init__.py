# src/rhosocial/activerecord/backend/impl/postgres/mixins/ddl/__init__.py
"""DDL-related PostgreSQL mixins."""

from .partition import PostgresPartitionMixin
from .index import PostgresIndexMixin
from .trigger import PostgresTriggerMixin
from .comment import PostgresCommentMixin
from .type import PostgresTypeMixin
from .constraint import PostgresConstraintMixin
from .column import PostgresAlterColumnModifierMixin
from .policy import PostgresPolicyMixin
from .rls_config import PostgresRlsConfigMixin
from .table_settings import PostgresAlterTableSettingsMixin
from .cluster import PostgresClusterMixin
from .domain import PostgresDomainMixin
from .collation import PostgresCollationDDLMixin
from .foreign_table import PostgresForeignTableMixin
from .routine import PostgresRoutineMixin
from .publication import PostgresPublicationMixin

__all__ = [
    "PostgresPartitionMixin",
    "PostgresIndexMixin",
    "PostgresTriggerMixin",
    "PostgresCommentMixin",
    "PostgresTypeMixin",
    "PostgresConstraintMixin",
    "PostgresAlterColumnModifierMixin",
    "PostgresPolicyMixin",
    "PostgresRlsConfigMixin",
    "PostgresAlterTableSettingsMixin",
    "PostgresClusterMixin",
    "PostgresDomainMixin",
    "PostgresCollationDDLMixin",
    "PostgresForeignTableMixin",
    "PostgresRoutineMixin",
    "PostgresPublicationMixin",
]
