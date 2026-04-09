# src/rhosocial/activerecord/backend/impl/postgres/expression/advisory/lock.py
"""
Advisory lock expression classes for PostgreSQL.

Advisory locks are application-level locks that provide a way for
applications to coordinate activities across multiple sessions.
Unlike regular locks, advisory locks are not tied to specific table rows
and can be used for arbitrary application-defined purposes.

PostgreSQL advisory locks:
- pg_advisory_lock(key) - acquire exclusive session-level lock
- pg_advisory_lock_shared(key) - acquire shared session-level lock
- pg_advisory_unlock(key) - release session-level lock
- pg_advisory_unlock_all(key) - release all session-level locks
- pg_try_advisory_lock(key) - non-blocking acquire exclusive lock
- pg_try_advisory_lock_shared(key) - non-blocking acquire shared lock
- pg_advisory_xact_lock(key) - acquire exclusive transaction-level lock
- pg_advisory_xact_lock_shared(key) - acquire shared transaction-level lock
- pg_try_advisory_xact_lock(key) - non-blocking acquire transaction-level lock

Keys can be either a single 64-bit integer or two 32-bit integers.
"""

from enum import Enum
from typing import Tuple, Union, TYPE_CHECKING

from rhosocial.activerecord.backend.expression.bases import BaseExpression, SQLQueryAndParams

if TYPE_CHECKING:
    from rhosocial.activerecord.backend.dialect import SQLDialectBase


class AdvisoryLockType(Enum):
    """Type of advisory lock."""

    EXCLUSIVE = "exclusive"  # Only one session can hold the lock
    SHARED = "shared"  # Multiple sessions can hold the lock simultaneously


class AdvisoryLockExpression(BaseExpression):
    """
    Expression for acquiring an advisory lock.

    Advisory locks are application-level locks managed by the database server.
    They are not tied to specific table rows and can be used for arbitrary
    coordination purposes.

    Usage:
        # Acquire exclusive session-level lock
        expr = AdvisoryLockExpression(dialect, key=12345)
        sql, params = expr.to_sql()  # SELECT pg_advisory_lock(12345)

        # Acquire shared session-level lock
        expr = AdvisoryLockExpression(dialect, key=12345, shared=True)
        sql, params = expr.to_sql()  # SELECT pg_advisory_lock_shared(12345)

        # Transaction-level lock (auto-released on commit/rollback)
        expr = AdvisoryLockExpression(dialect, key=12345, session=False)
        sql, params = expr.to_sql()  # SELECT pg_advisory_xact_lock(12345)

        # Two-key variant
        expr = AdvisoryLockExpression(dialect, key=(123, 456))
        sql, params = expr.to_sql()  # SELECT pg_advisory_lock(123, 456)
    """

    def __init__(
        self,
        dialect: "SQLDialectBase",
        key: Union[int, Tuple[int, int]],
        shared: bool = False,
        session: bool = True,
    ):
        """
        Initialize advisory lock expression.

        Args:
            dialect: The SQL dialect to use for formatting
            key: Lock key - either a single 64-bit integer or tuple of two 32-bit integers
            shared: If True, acquire shared lock; otherwise exclusive lock
            session: If True (default), session-level lock; otherwise transaction-level
        """
        super().__init__(dialect)
        self.key = key
        self.shared = shared
        self.session = session

    def to_sql(self) -> SQLQueryAndParams:
        """
        Generate SQL for acquiring advisory lock.

        Returns:
            Tuple of (SQL string, parameters tuple)
        """
        return self.dialect.format_advisory_lock(self)


class AdvisoryUnlockExpression(BaseExpression):
    """
    Expression for releasing an advisory lock.

    Usage:
        # Release session-level lock
        expr = AdvisoryUnlockExpression(dialect, key=12345)
        sql, params = expr.to_sql()  # SELECT pg_advisory_unlock(12345)

        # Release shared session-level lock
        expr = AdvisoryUnlockExpression(dialect, key=12345, shared=True)
        # Note: pg_advisory_unlock works for both exclusive and shared locks

        # Two-key variant
        expr = AdvisoryUnlockExpression(dialect, key=(123, 456))
        sql, params = expr.to_sql()  # SELECT pg_advisory_unlock(123, 456)
    """

    def __init__(
        self,
        dialect: "SQLDialectBase",
        key: Union[int, Tuple[int, int]],
        shared: bool = False,
    ):
        """
        Initialize advisory unlock expression.

        Args:
            dialect: The SQL dialect to use for formatting
            key: Lock key - either a single 64-bit integer or tuple of two 32-bit integers
            shared: If True, indicates this was a shared lock (informational only)
        """
        super().__init__(dialect)
        self.key = key
        self.shared = shared

    def to_sql(self) -> SQLQueryAndParams:
        """
        Generate SQL for releasing advisory lock.

        Returns:
            Tuple of (SQL string, parameters tuple)
        """
        return self.dialect.format_advisory_unlock(self)


class AdvisoryUnlockAllExpression(BaseExpression):
    """
    Expression for releasing all advisory locks held by the current session.

    Usage:
        expr = AdvisoryUnlockAllExpression(dialect)
        sql, params = expr.to_sql()  # SELECT pg_advisory_unlock_all()
    """

    def __init__(self, dialect: "SQLDialectBase"):
        """Initialize advisory unlock all expression."""
        super().__init__(dialect)

    def to_sql(self) -> SQLQueryAndParams:
        """
        Generate SQL for releasing all advisory locks.

        Returns:
            Tuple of (SQL string, parameters tuple)
        """
        return self.dialect.format_advisory_unlock_all(self)


class TryAdvisoryLockExpression(BaseExpression):
    """
    Expression for non-blocking advisory lock acquisition.

    Returns a boolean indicating whether the lock was acquired.
    Unlike AdvisoryLockExpression, this does not wait if the lock is unavailable.

    Usage:
        # Non-blocking acquire exclusive session-level lock
        expr = TryAdvisoryLockExpression(dialect, key=12345)
        sql, params = expr.to_sql()  # SELECT pg_try_advisory_lock(12345)

        # Non-blocking acquire shared session-level lock
        expr = TryAdvisoryLockExpression(dialect, key=12345, shared=True)
        sql, params = expr.to_sql()  # SELECT pg_try_advisory_lock_shared(12345)

        # Transaction-level variant
        expr = TryAdvisoryLockExpression(dialect, key=12345, session=False)
        sql, params = expr.to_sql()  # SELECT pg_try_advisory_xact_lock(12345)
    """

    def __init__(
        self,
        dialect: "SQLDialectBase",
        key: Union[int, Tuple[int, int]],
        shared: bool = False,
        session: bool = True,
    ):
        """
        Initialize try advisory lock expression.

        Args:
            dialect: The SQL dialect to use for formatting
            key: Lock key - either a single 64-bit integer or tuple of two 32-bit integers
            shared: If True, acquire shared lock; otherwise exclusive lock
            session: If True (default), session-level lock; otherwise transaction-level
        """
        super().__init__(dialect)
        self.key = key
        self.shared = shared
        self.session = session

    def to_sql(self) -> SQLQueryAndParams:
        """
        Generate SQL for non-blocking advisory lock acquisition.

        Returns:
            Tuple of (SQL string, parameters tuple)
        """
        return self.dialect.format_try_advisory_lock(self)