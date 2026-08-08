# src/rhosocial/activerecord/backend/impl/postgres/expression/ddl/__init__.py
"""
PostgreSQL DDL expressions.

Directory structure:
- vacuum.py     - VACUUM/ANALYZE expressions
- partition.py  - Partition DDL expressions
- index.py      - Index DDL expressions
- statistics.py - Statistics DDL expressions
- comment.py   - COMMENT expressions
- mv.py         - Materialized view expressions
- type.py       - Enum/Range type expressions
- extension.py  - Extension DDL expressions
- policy.py     - Row-Level Security POLICY expressions (CREATE/ALTER/DROP)
- rls_config.py - Row-Level Security table configuration (ENABLE/DISABLE/FORCE)
- table_settings.py - ALTER TABLE SET LOGGED/UNLOGGED/ACCESS METHOD
- cluster.py    - CLUSTER expressions
- domain.py     - CREATE/ALTER/DROP DOMAIN expressions
- collation.py  - CREATE/DROP COLLATION object expressions
- foreign_table.py - CREATE/DROP FOREIGN TABLE expressions
- routine.py    - CREATE/DROP FUNCTION / AGGREGATE expressions
- publication.py - CREATE/DROP PUBLICATION / SUBSCRIPTION expressions

Missing expressions — why?
============================
This package does **not** provide ``PostgresCreateIndexExpression`` or
``PostgresDropIndexExpression``.  Those were removed because the generic
``CreateIndexExpression`` / ``DropIndexExpression`` (from
``rhosocial.activerecord.backend.expression.statements.ddl_index``) already
accept all common parameters **and** a ``dialect_options`` dict.

PG‑specific features such as ``NULLS NOT DISTINCT`` or ``CONCURRENTLY`` on
DROP INDEX are passed through ``dialect_options`` and consumed by the dialect
(``PostgresIndexMixin``).  See ``index.py`` for the supported keys.

Example::

    from rhosocial.activerecord.backend.expression.statements.ddl_index import (
        CreateIndexExpression,
        DropIndexExpression,
    )
    from rhosocial.activerecord.backend.impl.postgres import PostgresDialect

    d = PostgresDialect((15, 0, 0))

    # NULLS NOT DISTINCT via dialect_options
    expr = CreateIndexExpression(
        d, "idx_uniq_abc", "t", ["a", "b"],
        unique=True,
        dialect_options={"nulls_not_distinct": True},
    )
    sql, _ = expr.to_sql()   # → CREATE UNIQUE INDEX … NULLS NOT DISTINCT

    # DROP INDEX CONCURRENTLY via dialect_options (PG 18+)
    expr = DropIndexExpression(
        d, "idx_old",
        dialect_options={"concurrent": True},
    )
    sql, _ = expr.to_sql()   # → DROP INDEX CONCURRENTLY …
"""

from .vacuum import PostgresVacuumExpression, PostgresAnalyzeExpression
from .partition import (
    PartitionValue,
    PostgresCreatePartitionExpression,
    PostgresDetachPartitionExpression,
    PostgresAttachPartitionExpression,
    PostgresPartitionMetadataExpression,
)
from .index import (
    PostgresAlterIndexExpression,
    PostgresAlterIndexActionType,
    PostgresReindexExpression,
)
from .statistics import (
    PostgresCreateStatisticsExpression,
    PostgresDropStatisticsExpression,
)
from .comment import PostgresCommentExpression
from .mv import PostgresRefreshMaterializedViewExpression
from .type import (
    PostgresCreateEnumTypeExpression,
    PostgresDropEnumTypeExpression,
    PostgresAlterEnumAddValueExpression,
    PostgresAlterEnumTypeAddValueExpression,
    PostgresAlterEnumTypeRenameValueExpression,
    PostgresCreateRangeTypeExpression,
)
from .extension import PostgresCreateExtensionExpression, PostgresDropExtensionExpression
from .pg_partman import (
    PostgresPgPartmanCreateParentExpression,
    PostgresPgPartmanRunMaintenanceExpression,
    PostgresPgPartmanUpdateConfigExpression,
    PostgresPgPartmanDeleteConfigExpression,
)
from .policy import (
    AlterPolicyMode,
    PolicyCommand,
    PolicyType,
    PostgresCreatePolicyExpression,
    PostgresAlterPolicyExpression,
    PostgresDropPolicyExpression,
)
from .rls_config import (
    RlsConfigurationMode,
    PostgresAlterTableRlsExpression,
    PostgresForceRlsExpression,
)
from .table_settings import (
    LoggingMode,
    PostgresAlterTableSettingsExpression,
)
from .cluster import PostgresClusterExpression
from .domain import (
    AlterDomainActionType,
    PostgresCreateDomainExpression,
    PostgresAlterDomainExpression,
    PostgresDropDomainExpression,
)
from .collation import (
    PostgresCreateCollationExpression,
    PostgresDropCollationExpression,
)
from .foreign_table import (
    PostgresCreateForeignTableExpression,
    PostgresDropForeignTableExpression,
)
from .routine import (
    PostgresCreateFunctionExpression,
    PostgresDropFunctionExpression,
    PostgresCreateAggregateExpression,
    PostgresDropAggregateExpression,
)
from .publication import (
    PostgresCreatePublicationExpression,
    PostgresDropPublicationExpression,
    PostgresCreateSubscriptionExpression,
    PostgresDropSubscriptionExpression,
)

__all__ = [
    # vacuum
    "PostgresVacuumExpression",
    "PostgresAnalyzeExpression",
    # partition
    "PartitionValue",
    "PostgresCreatePartitionExpression",
    "PostgresDetachPartitionExpression",
    "PostgresAttachPartitionExpression",
    "PostgresPartitionMetadataExpression",
    # index
    "PostgresAlterIndexExpression",
    "PostgresAlterIndexActionType",
    "PostgresReindexExpression",
    # statistics
    "PostgresCreateStatisticsExpression",
    "PostgresDropStatisticsExpression",
    # comment
    "PostgresCommentExpression",
    # mv
    "PostgresRefreshMaterializedViewExpression",
    # type (enum/range)
    "PostgresCreateEnumTypeExpression",
    "PostgresDropEnumTypeExpression",
    "PostgresAlterEnumAddValueExpression",
    "PostgresAlterEnumTypeAddValueExpression",
    "PostgresAlterEnumTypeRenameValueExpression",
    "PostgresCreateRangeTypeExpression",
    # extension
    "PostgresCreateExtensionExpression",
    "PostgresDropExtensionExpression",
    "PostgresPgPartmanCreateParentExpression",
    "PostgresPgPartmanRunMaintenanceExpression",
    "PostgresPgPartmanUpdateConfigExpression",
    "PostgresPgPartmanDeleteConfigExpression",
    # policy
    "PolicyType",
    "PolicyCommand",
    "AlterPolicyMode",
    "PostgresAlterPolicyExpression",
    "PostgresDropPolicyExpression",
    # rls_config
    "RlsConfigurationMode",
    "PostgresAlterTableRlsExpression",
    "PostgresForceRlsExpression",
    # table_settings
    "LoggingMode",
    "PostgresAlterTableSettingsExpression",
    # cluster
    "PostgresClusterExpression",
    # domain
    "AlterDomainActionType",
    "PostgresCreateDomainExpression",
    "PostgresAlterDomainExpression",
    "PostgresDropDomainExpression",
    # collation
    "PostgresCreateCollationExpression",
    "PostgresDropCollationExpression",
    # foreign_table
    "PostgresCreateForeignTableExpression",
    "PostgresDropForeignTableExpression",
    # routine
    "PostgresCreateFunctionExpression",
    "PostgresDropFunctionExpression",
    "PostgresCreateAggregateExpression",
    "PostgresDropAggregateExpression",
    # publication
    "PostgresCreatePublicationExpression",
    "PostgresDropPublicationExpression",
    "PostgresCreateSubscriptionExpression",
    "PostgresDropSubscriptionExpression",
]
