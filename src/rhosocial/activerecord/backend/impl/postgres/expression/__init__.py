# src/rhosocial/activerecord/backend/impl/postgres/expression/__init__.py
"""
PostgreSQL-specific expression classes.

This module provides expression classes that are specific to PostgreSQL,
such as advisory lock expressions, DDL statements, and row-level locking support.

Directory structure:
- advisory/    - Advisory lock expressions
- locking.py   - Lock strength expressions
- ddl/         - DDL statement expressions
"""

from .advisory import (
    AdvisoryLockExpression,
    AdvisoryUnlockExpression,
    AdvisoryUnlockAllExpression,
    TryAdvisoryLockExpression,
    AdvisoryLockType,
)
from .locking import LockStrength, PostgresForUpdateClause
from .ddl import (
    VacuumExpression,
    AnalyzeExpression,
    CreatePartitionExpression,
    DetachPartitionExpression,
    AttachPartitionExpression,
    ReindexExpression,
    CreateStatisticsExpression,
    DropStatisticsExpression,
    CommentExpression,
    RefreshMaterializedViewPgExpression,
    CreateEnumTypeExpression,
    DropEnumTypeExpression,
    AlterEnumAddValueExpression,
    AlterEnumTypeAddValueExpression,
    AlterEnumTypeRenameValueExpression,
    CreateRangeTypeExpression,
    CreateExtensionExpression,
    DropExtensionExpression,
)

__all__ = [
    # advisory
    "AdvisoryLockExpression",
    "AdvisoryUnlockExpression",
    "AdvisoryUnlockAllExpression",
    "TryAdvisoryLockExpression",
    "AdvisoryLockType",
    # locking
    "LockStrength",
    "PostgresForUpdateClause",
    # ddl
    "VacuumExpression",
    "AnalyzeExpression",
    "CreatePartitionExpression",
    "DetachPartitionExpression",
    "AttachPartitionExpression",
    "ReindexExpression",
    "CreateStatisticsExpression",
    "DropStatisticsExpression",
    "CommentExpression",
    "RefreshMaterializedViewPgExpression",
    "CreateEnumTypeExpression",
    "DropEnumTypeExpression",
    "AlterEnumAddValueExpression",
    "AlterEnumTypeAddValueExpression",
    "AlterEnumTypeRenameValueExpression",
    "CreateRangeTypeExpression",
    "CreateExtensionExpression",
    "DropExtensionExpression",
]