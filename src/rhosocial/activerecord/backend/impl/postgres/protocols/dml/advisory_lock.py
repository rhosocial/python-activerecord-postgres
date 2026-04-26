# src/rhosocial/activerecord/backend/impl/postgres/protocols/dml/advisory_lock.py
"""
PostgreSQL advisory lock protocol definitions.

Advisory locks are application-level locks managed by the database server.
They provide a way for applications to coordinate activities across sessions
without relying on table-level or row-level locks.
"""

from typing import Protocol, Tuple, Union, TYPE_CHECKING

if TYPE_CHECKING:
    from rhosocial.activerecord.backend.impl.postgres.expression.advisory import (
        PostgresAdvisoryLockExpression,
        PostgresAdvisoryUnlockExpression,
        PostgresAdvisoryUnlockAllExpression,
        PostgresTryAdvisoryLockExpression,
    )


class PostgresAdvisoryLockSupport(Protocol):
    """
    Protocol for PostgreSQL advisory lock support.

    Advisory locks are application-level locks that provide a way for
    applications to coordinate activities across multiple sessions.

    Features:
    - Session-level locks: Released explicitly or when session ends
    - Transaction-level locks: Released when transaction ends
    - Exclusive locks: Only one session can hold
    - Shared locks: Multiple sessions can hold simultaneously
    - Non-blocking try variants: Return boolean instead of waiting

    PostgreSQL version support:
    - All advisory lock functions: PostgreSQL 9.0+
    """

    def supports_advisory_lock(self) -> bool:
        """
        Whether advisory locks are supported.

        Advisory locks are supported in PostgreSQL 9.0 and later.

        Returns:
            True if advisory locks are supported
        """
        ...  # pragma: no cover

    def format_advisory_lock(self, expr: "PostgresAdvisoryLockExpression") -> Tuple[str, tuple]:
        """
        Format SQL for acquiring an advisory lock.

        Args:
            expr: PostgresAdvisoryLockExpression with lock parameters

        Returns:
            Tuple of (SQL string, parameters)
        """
        ...  # pragma: no cover

    def format_advisory_unlock(self, expr: "PostgresAdvisoryUnlockExpression") -> Tuple[str, tuple]:
        """
        Format SQL for releasing an advisory lock.

        Args:
            expr: PostgresAdvisoryUnlockExpression with lock parameters

        Returns:
            Tuple of (SQL string, parameters)
        """
        ...  # pragma: no cover

    def format_advisory_unlock_all(self, expr: "PostgresAdvisoryUnlockAllExpression") -> Tuple[str, tuple]:
        """
        Format SQL for releasing all advisory locks.

        Args:
            expr: PostgresAdvisoryUnlockAllExpression

        Returns:
            Tuple of (SQL string, parameters)
        """
        ...  # pragma: no cover

    def format_try_advisory_lock(self, expr: "PostgresTryAdvisoryLockExpression") -> Tuple[str, tuple]:
        """
        Format SQL for non-blocking advisory lock acquisition.

        Args:
            expr: PostgresTryAdvisoryLockExpression with lock parameters

        Returns:
            Tuple of (SQL string, parameters)
        """
        ...  # pragma: no cover