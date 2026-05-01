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
"""

from .vacuum import PostgresVacuumExpression, PostgresAnalyzeExpression
from .partition import (
    PostgresCreatePartitionExpression,
    PostgresDetachPartitionExpression,
    PostgresAttachPartitionExpression,
)
from .index import PostgresReindexExpression
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

__all__ = [
    # vacuum
    "PostgresVacuumExpression",
    "PostgresAnalyzeExpression",
    # partition
    "PostgresCreatePartitionExpression",
    "PostgresDetachPartitionExpression",
    "PostgresAttachPartitionExpression",
    # index
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
]