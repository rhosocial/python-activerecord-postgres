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
The generic ``CreateIndexStatement`` / ``DropIndexStatement`` (from
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

    from rhosocial.activerecord.backend.impl.postgres.dialect import PostgresDialect
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

from ..copy import (
    PostgresCopyFormat,
    PostgresCopyFromStatement,
    PostgresCopyLogVerbosity,
    PostgresCopyOnError,
    PostgresCopyToStatement,
)
from .vacuum import PostgresVacuumStatement, PostgresAnalyzeStatement
from .table_options import PostgresCreateTableOptions
from .column import (
    PostgresColumnStorage,
    PostgresColumnDefinition,
    PostgresColumnOptions,
)
from .alter_column import PostgresAlterColumnAction
from .partition import (
    PostgresPartitionValue,
    PostgresCreatePartitionStatement,
    PostgresDetachPartitionStatement,
    PostgresAttachPartitionStatement,
    PostgresPartitionMetadataQueryStatement,
)
from .index import (
    PostgresAlterIndexStatement,
    PostgresAlterIndexActionType,
    PostgresReindexStatement,
)
from .index_definition import (
    PostgresCreateIndexExpression,
    PostgresIndexDefinition,
    PostgresDropIndexExpression,
)
from .exclude_constraint import PostgresExcludeConstraint
from .constraint import PostgresAlterConstraintAction
from .statistics import (
    PostgresCreateStatisticsStatement,
    PostgresDropStatisticsStatement,
)
from .comment import PostgresCommentOnStatement
from .mv import (
    PostgresMaterializedViewAlterAction,
    PostgresAlterMaterializedViewStatement,
    PostgresChangeMaterializedViewOwnerAction,
    PostgresCreateMaterializedViewExpression,
    PostgresDropMaterializedViewExpression,
    PostgresRefreshMaterializedViewPgStatement,
    PostgresRenameMaterializedViewAction,
    PostgresResetMaterializedViewPropertiesAction,
    PostgresSetMaterializedViewPropertiesAction,
    PostgresSetMaterializedViewSchemaAction,
)
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
    PostgresEnumTypeNameExpression,
    PostgresEnumValuesExpression,
)
from .extension import PostgresCreateExtensionStatement, PostgresDropExtensionStatement
from .multirange import (
    PostgresCreateMultirangeTypeExpression,
    PostgresMultirangeAggFunctionExpression,
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
    PostgresCreatePolicyStatement,
    PostgresAlterPolicyStatement,
    PostgresDropPolicyStatement,
)
from .rls_config import (
    RlsConfigurationMode,
    PostgresAlterTableRlsStatement,
    PostgresForceRlsStatement,
)
from .table_settings import (
    LoggingMode,
    PostgresAlterTableSettingsStatement,
)
from .cluster import PostgresClusterStatement
from .repack import PostgresRepackStatement
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
    PostgresCreateCollationStatement,
    PostgresDropCollationStatement,
)
from .foreign_table import (
    PostgresCreateForeignTableStatement,
    PostgresDropForeignTableStatement,
)
from .routine import (
    PostgresCreateFunctionDdlStatement,
    PostgresDropFunctionDdlStatement,
    PostgresCreateAggregateDdlStatement,
    PostgresDropAggregateDdlStatement,
)
from .publication import (
    PostgresCreatePublicationStatement,
    PostgresDropPublicationStatement,
    PostgresCreateSubscriptionStatement,
    PostgresDropSubscriptionStatement,
)

__all__ = [
    # copy
    "PostgresCopyFormat",
    "PostgresCopyFromStatement",
    "PostgresCopyLogVerbosity",
    "PostgresCopyOnError",
    "PostgresCopyToStatement",
    # vacuum
    "PostgresVacuumStatement",
    "PostgresAnalyzeStatement",
    # table options
    "PostgresCreateTableOptions",
    # column
    "PostgresColumnStorage",
    "PostgresColumnDefinition",
    "PostgresColumnOptions",
    "PostgresAlterColumnAction",
    # partition
    "PostgresPartitionValue",
    "PostgresCreatePartitionStatement",
    "PostgresDetachPartitionStatement",
    "PostgresAttachPartitionStatement",
    "PostgresPartitionMetadataQueryStatement",
    # index
    "PostgresAlterIndexStatement",
    "PostgresAlterIndexActionType",
    "PostgresReindexStatement",
    "PostgresCreateIndexExpression",
    "PostgresIndexDefinition",
    "PostgresDropIndexExpression",
    "PostgresExcludeConstraint",
    "PostgresAlterConstraintAction",
    # statistics
    "PostgresCreateStatisticsStatement",
    "PostgresDropStatisticsStatement",
    # comment
    "PostgresCommentOnStatement",
    # mv
    "PostgresMaterializedViewAlterAction",
    "PostgresAlterMaterializedViewStatement",
    "PostgresChangeMaterializedViewOwnerAction",
    "PostgresCreateMaterializedViewExpression",
    "PostgresDropMaterializedViewExpression",
    "PostgresRefreshMaterializedViewPgStatement",
    "PostgresRenameMaterializedViewAction",
    "PostgresResetMaterializedViewPropertiesAction",
    "PostgresSetMaterializedViewPropertiesAction",
    "PostgresSetMaterializedViewSchemaAction",
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
    "PostgresEnumTypeNameExpression",
    "PostgresEnumValuesExpression",
    # multirange
    "PostgresCreateMultirangeTypeExpression",
    "PostgresMultirangeAggFunctionExpression",
    # extension
    "PostgresCreateExtensionStatement",
    "PostgresDropExtensionStatement",
    "PostgresPgPartmanCreateParentExpression",
    "PostgresPgPartmanRunMaintenanceExpression",
    "PostgresPgPartmanUpdateConfigExpression",
    "PostgresPgPartmanDeleteConfigExpression",
    # policy
    "PolicyType",
    "PolicyCommand",
    "AlterPolicyMode",
    "PostgresCreatePolicyStatement",
    "PostgresAlterPolicyStatement",
    "PostgresDropPolicyStatement",
    # rls_config
    "RlsConfigurationMode",
    "PostgresAlterTableRlsStatement",
    "PostgresForceRlsStatement",
    # table_settings
    "LoggingMode",
    "PostgresAlterTableSettingsStatement",
    # cluster
    "PostgresClusterStatement",
    "PostgresRepackStatement",
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
    "PostgresCreateCollationStatement",
    "PostgresDropCollationStatement",
    # foreign_table
    "PostgresCreateForeignTableStatement",
    "PostgresDropForeignTableStatement",
    # routine
    "PostgresCreateFunctionDdlStatement",
    "PostgresDropFunctionDdlStatement",
    "PostgresCreateAggregateDdlStatement",
    "PostgresDropAggregateDdlStatement",
    # publication
    "PostgresCreatePublicationStatement",
    "PostgresDropPublicationStatement",
    "PostgresCreateSubscriptionStatement",
    "PostgresDropSubscriptionStatement",
]
