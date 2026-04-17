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
"""

from .vacuum import VacuumExpression, AnalyzeExpression
from .partition import (
    CreatePartitionExpression,
    DetachPartitionExpression,
    AttachPartitionExpression,
)
from .index import ReindexExpression
from .statistics import (
    CreateStatisticsExpression,
    DropStatisticsExpression,
)
from .comment import CommentExpression
from .mv import RefreshMaterializedViewPgExpression
from .type import (
    CreateEnumTypeExpression,
    DropEnumTypeExpression,
    AlterEnumAddValueExpression,
    AlterEnumTypeAddValueExpression,
    AlterEnumTypeRenameValueExpression,
    CreateRangeTypeExpression,
)

__all__ = [
    # vacuum
    "VacuumExpression",
    "AnalyzeExpression",
    # partition
    "CreatePartitionExpression",
    "DetachPartitionExpression",
    "AttachPartitionExpression",
    # index
    "ReindexExpression",
    # statistics
    "CreateStatisticsExpression",
    "DropStatisticsExpression",
    # comment
    "CommentExpression",
    # mv
    "RefreshMaterializedViewPgExpression",
    # type (enum/range)
    "CreateEnumTypeExpression",
    "DropEnumTypeExpression",
    "AlterEnumAddValueExpression",
    "AlterEnumTypeAddValueExpression",
    "AlterEnumTypeRenameValueExpression",
    "CreateRangeTypeExpression",
]