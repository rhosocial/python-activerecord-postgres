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
    PostgresAdvisoryLockExpression,
    PostgresAdvisoryUnlockExpression,
    PostgresAdvisoryUnlockAllExpression,
    PostgresTryAdvisoryLockExpression,
    AdvisoryLockType,
)
from .locking import LockStrength
from .ddl import (
    PostgresVacuumExpression,
    PostgresAnalyzeExpression,
    PostgresCreatePartitionExpression,
    PostgresDetachPartitionExpression,
    PostgresAttachPartitionExpression,
    PostgresAlterIndexExpression,
    PostgresAlterIndexActionType,
    PostgresReindexExpression,
    PostgresCreateStatisticsExpression,
    PostgresDropStatisticsExpression,
    PostgresCommentExpression,
    PostgresRefreshMaterializedViewExpression,
    PostgresCreateEnumTypeExpression,
    PostgresDropEnumTypeExpression,
    PostgresAlterEnumAddValueExpression,
    PostgresAlterEnumTypeAddValueExpression,
    PostgresAlterEnumTypeRenameValueExpression,
    PostgresCreateRangeTypeExpression,
    PostgresCreateExtensionExpression,
    PostgresDropExtensionExpression,
    PostgresPgPartmanCreateParentExpression,
    PostgresPgPartmanRunMaintenanceExpression,
    PostgresPgPartmanUpdateConfigExpression,
    PostgresPgPartmanDeleteConfigExpression,
)

__all__ = [
    # advisory
    "PostgresAdvisoryLockExpression",
    "PostgresAdvisoryUnlockExpression",
    "PostgresAdvisoryUnlockAllExpression",
    "PostgresTryAdvisoryLockExpression",
    "AdvisoryLockType",
    # locking
    "LockStrength",
    # ddl
    "PostgresVacuumExpression",
    "PostgresAnalyzeExpression",
    "PostgresCreatePartitionExpression",
    "PostgresDetachPartitionExpression",
    "PostgresAttachPartitionExpression",
    "PostgresAlterIndexExpression",
    "PostgresAlterIndexActionType",
    "PostgresReindexExpression",
    "PostgresCreateStatisticsExpression",
    "PostgresDropStatisticsExpression",
    "PostgresCommentExpression",
    "PostgresRefreshMaterializedViewExpression",
    "PostgresCreateEnumTypeExpression",
    "PostgresDropEnumTypeExpression",
    "PostgresAlterEnumAddValueExpression",
    "PostgresAlterEnumTypeAddValueExpression",
    "PostgresAlterEnumTypeRenameValueExpression",
    "PostgresCreateRangeTypeExpression",
    "PostgresCreateExtensionExpression",
    "PostgresDropExtensionExpression",
    "PostgresPgPartmanCreateParentExpression",
    "PostgresPgPartmanRunMaintenanceExpression",
    "PostgresPgPartmanUpdateConfigExpression",
    "PostgresPgPartmanDeleteConfigExpression",
]
