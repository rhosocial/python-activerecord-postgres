# src/rhosocial/activerecord/backend/impl/postgres/expression/ddl/__init__.py
"""
PostgreSQL DDL expressions.

Directory structure:
- vacuum.py     - VACUUM/ANALYZE expressions
- partition.py  - Partition DDL expressions
- index.py      - Index DDL expressions
- index_definition.py - PostgreSQL index definition / CREATE INDEX expressions
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

PostgreSQL index expressions
============================
The generic ``CreateIndexExpression`` / ``DropIndexExpression`` (from
``rhosocial.activerecord.backend.expression.statements.ddl_index``) carry all
common parameters.

PostgreSQL-only features are supplied through typed fields, not a
``dialect_options`` bag:

* ``PostgresCreateIndexExpression`` adds ``opclasses``,
  ``nulls_not_distinct`` and ``with_options`` for ``CREATE INDEX``.
* ``PostgresIndexDefinition`` adds the same options to an inline table index
  definition.
* ``PostgresDropIndexExpression`` marks PostgreSQL ownership of a
  ``DROP INDEX`` (the generic expression already carries ``concurrent``).

The typed fields are consumed by ``PostgresIndexMixin`` (see ``index.py``).

Example::

    from rhosocial.activerecord.backend.impl.postgres import PostgresDialect
    from rhosocial.activerecord.backend.impl.postgres.expression.ddl import (
        PostgresCreateIndexExpression,
    )

    d = PostgresDialect((15, 0, 0))

    # NULLS NOT DISTINCT via a typed field
    expr = PostgresCreateIndexExpression(
        d, "idx_uniq_abc", "t", ["a", "b"],
        unique=True,
        nulls_not_distinct=True,
    )
    sql, _ = expr.to_sql()   # → CREATE UNIQUE INDEX … NULLS NOT DISTINCT
"""

from .vacuum import PostgresVacuumExpression, PostgresAnalyzeExpression
from .table_options import PostgresCreateTableOptions
from .column import (
    PostgresColumnStorage,
    PostgresColumnDefinition,
    PostgresColumnOptions,
)
from .alter_column import PostgresAlterColumn
from .partition import (
    PartitionValue,
    PostgresPartitionClause,
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
from .index_definition import (
    PostgresCreateIndexExpression,
    PostgresIndexDefinition,
    PostgresDropIndexExpression,
)
from .exclude_constraint import PostgresExcludeConstraint
from .statistics import (
    PostgresCreateStatisticsExpression,
    PostgresDropStatisticsExpression,
)
from .comment import PostgresCommentExpression
from .mv import PostgresRefreshMaterializedViewExpression
from .type import (
    PostgresAddEnumValueAction,
    PostgresAddTypeAttributeAction,
    PostgresAlterEnumAddValueExpression,
    PostgresAlterEnumTypeAddValueExpression,
    PostgresAlterEnumTypeRenameValueExpression,
    PostgresAlterTypeAttributeAction,
    PostgresBaseTypeDefinition,
    PostgresChangeTypeOwnerAction,
    PostgresCompositeTypeAttribute,
    PostgresCompositeTypeDefinition,
    PostgresCreateEnumTypeExpression,
    PostgresCreateRangeTypeExpression,
    PostgresDropEnumTypeExpression,
    PostgresDropTypeAttributeAction,
    PostgresDropTypeExpression,
    PostgresEnumTypeDefinition,
    PostgresRangeTypeDefinition,
    PostgresRenameEnumValueAction,
    PostgresRenameTypeAction,
    PostgresRenameTypeAttributeAction,
    PostgresSetTypePropertiesAction,
    PostgresSetTypeSchemaAction,
    PostgresShellTypeDefinition,
    AlterEnumAddValueExpression,
    CreateEnumTypeExpression,
    DropEnumTypeExpression,
    EnumTypeNameExpression,
    EnumValuesExpression,
)
from .extension import PostgresCreateExtensionExpression, PostgresDropExtensionExpression
from .multirange import (
    CreateMultirangeTypeExpression,
    MultirangeAggFunctionExpression,
)
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
    DomainCheckConstraint,
    DomainNullability,
    DomainValueExpression,
    PostgresAddDomainCheckAction,
    PostgresAlterDomainExpression,
    PostgresChangeDomainOwnerAction,
    PostgresCreateDomainExpression,
    PostgresDropDomainCheckAction,
    PostgresDropDomainExpression,
    PostgresRenameDomainConstraintAction,
    PostgresSetDomainSchemaAction,
    PostgresValidateDomainConstraintAction,
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
    # table options
    "PostgresCreateTableOptions",
    # column
    "PostgresColumnStorage",
    "PostgresColumnDefinition",
    "PostgresColumnOptions",
    "PostgresAlterColumn",
    # partition
    "PartitionValue",
    "PostgresPartitionClause",
    "PostgresCreatePartitionExpression",
    "PostgresDetachPartitionExpression",
    "PostgresAttachPartitionExpression",
    "PostgresPartitionMetadataExpression",
    # index
    "PostgresAlterIndexExpression",
    "PostgresAlterIndexActionType",
    "PostgresReindexExpression",
    "PostgresCreateIndexExpression",
    "PostgresIndexDefinition",
    "PostgresDropIndexExpression",
    "PostgresExcludeConstraint",
    # statistics
    "PostgresCreateStatisticsExpression",
    "PostgresDropStatisticsExpression",
    # comment
    "PostgresCommentExpression",
    # mv
    "PostgresRefreshMaterializedViewExpression",
    # type
    "PostgresCompositeTypeAttribute",
    "PostgresCompositeTypeDefinition",
    "PostgresEnumTypeDefinition",
    "PostgresRangeTypeDefinition",
    "PostgresBaseTypeDefinition",
    "PostgresShellTypeDefinition",
    "PostgresRenameTypeAction",
    "PostgresSetTypeSchemaAction",
    "PostgresChangeTypeOwnerAction",
    "PostgresRenameTypeAttributeAction",
    "PostgresAddTypeAttributeAction",
    "PostgresDropTypeAttributeAction",
    "PostgresAlterTypeAttributeAction",
    "PostgresAddEnumValueAction",
    "PostgresRenameEnumValueAction",
    "PostgresSetTypePropertiesAction",
    "PostgresDropTypeExpression",
    "PostgresCreateEnumTypeExpression",
    "PostgresDropEnumTypeExpression",
    "PostgresAlterEnumAddValueExpression",
    "PostgresAlterEnumTypeAddValueExpression",
    "PostgresAlterEnumTypeRenameValueExpression",
    "PostgresCreateRangeTypeExpression",
    "EnumTypeNameExpression",
    "EnumValuesExpression",
    "CreateEnumTypeExpression",
    "DropEnumTypeExpression",
    "AlterEnumAddValueExpression",
    # multirange
    "CreateMultirangeTypeExpression",
    "MultirangeAggFunctionExpression",
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
    "PostgresCreatePolicyExpression",
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
    "DomainNullability",
    "DomainValueExpression",
    "DomainCheckConstraint",
    "PostgresAddDomainCheckAction",
    "PostgresDropDomainCheckAction",
    "PostgresRenameDomainConstraintAction",
    "PostgresValidateDomainConstraintAction",
    "PostgresChangeDomainOwnerAction",
    "PostgresSetDomainSchemaAction",
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
