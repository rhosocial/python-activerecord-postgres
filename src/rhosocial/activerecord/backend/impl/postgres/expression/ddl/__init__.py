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
]
