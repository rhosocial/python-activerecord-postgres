# src/rhosocial/activerecord/backend/impl/postgres/expression/__init__.py
"""
PostgreSQL-specific expression classes.

This module provides expression classes that are specific to PostgreSQL,
such as advisory lock expressions and row-level locking support.
"""

from .advisory import (
    AdvisoryLockExpression,
    AdvisoryUnlockExpression,
    AdvisoryUnlockAllExpression,
    TryAdvisoryLockExpression,
    AdvisoryLockType,
)
from .locking import LockStrength, PostgresForUpdateClause

__all__ = [
    "AdvisoryLockExpression",
    "AdvisoryUnlockExpression",
    "AdvisoryUnlockAllExpression",
    "TryAdvisoryLockExpression",
    "AdvisoryLockType",
    "LockStrength",
    "PostgresForUpdateClause",
]