# src/rhosocial/activerecord/backend/impl/postgres/mixins/dml/advisory_lock.py
"""
PostgreSQL advisory lock mixin implementation.

Provides methods for working with PostgreSQL advisory locks, which are
application-level locks managed by the database server.
"""

from typing import Tuple, TYPE_CHECKING

if TYPE_CHECKING:
    from rhosocial.activerecord.backend.expression.advisory import (
        AdvisoryLockExpression,
        AdvisoryUnlockExpression,
        AdvisoryUnlockAllExpression,
        TryAdvisoryLockExpression,
    )


class PostgresAdvisoryLockMixin:
    """
    Mixin providing PostgreSQL advisory lock support.

    Advisory locks are application-level locks that provide a way for
    applications to coordinate activities across multiple sessions.

    The lock functions available in PostgreSQL:
    - pg_advisory_lock(key) - acquire exclusive session-level lock
    - pg_advisory_lock_shared(key) - acquire shared session-level lock
    - pg_advisory_unlock(key) - release session-level lock
    - pg_advisory_unlock_all() - release all session-level locks
    - pg_try_advisory_lock(key) - non-blocking acquire exclusive lock
    - pg_try_advisory_lock_shared(key) - non-blocking acquire shared lock
    - pg_advisory_xact_lock(key) - acquire exclusive transaction-level lock
    - pg_advisory_xact_lock_shared(key) - acquire shared transaction-level lock
    - pg_try_advisory_xact_lock(key) - non-blocking acquire transaction-level lock

    Keys can be either:
    - A single 64-bit integer (bigint)
    - Two 32-bit integers (int, int)
    """

    def supports_advisory_lock(self) -> bool:
        """
        Whether advisory locks are supported.

        Advisory locks are supported in PostgreSQL 9.0 and later.

        Returns:
            True if advisory locks are supported
        """
        return True  # PostgreSQL 9.0+ supports advisory locks

    def format_advisory_lock(self, expr: "AdvisoryLockExpression") -> Tuple[str, tuple]:
        """
        Format SQL for acquiring an advisory lock.

        Generates:
        - SELECT pg_advisory_lock(key) for exclusive session lock
        - SELECT pg_advisory_lock_shared(key) for shared session lock
        - SELECT pg_advisory_xact_lock(key) for exclusive transaction lock
        - SELECT pg_advisory_xact_lock_shared(key) for shared transaction lock

        Args:
            expr: AdvisoryLockExpression with lock parameters

        Returns:
            Tuple of (SQL string, parameters)
        """
        # Build function name based on options
        if expr.session:
            # Session-level lock
            if expr.shared:
                func_name = "pg_advisory_lock_shared"
            else:
                func_name = "pg_advisory_lock"
        else:
            # Transaction-level lock
            if expr.shared:
                func_name = "pg_advisory_xact_lock_shared"
            else:
                func_name = "pg_advisory_xact_lock"

        # Handle key format
        if isinstance(expr.key, tuple):
            key1, key2 = expr.key
            return f"SELECT {func_name}(%s, %s)", (key1, key2)
        else:
            return f"SELECT {func_name}(%s)", (expr.key,)

    def format_advisory_unlock(self, expr: "AdvisoryUnlockExpression") -> Tuple[str, tuple]:
        """
        Format SQL for releasing an advisory lock.

        Generates:
        - SELECT pg_advisory_unlock(key) for exclusive lock
        - SELECT pg_advisory_unlock_shared(key) for shared lock

        Args:
            expr: AdvisoryUnlockExpression with lock parameters

        Returns:
            Tuple of (SQL string, parameters)
        """
        # Build function name based on shared flag
        # Note: Shared locks must use pg_advisory_unlock_shared
        func_name = "pg_advisory_unlock_shared" if expr.shared else "pg_advisory_unlock"

        # Handle key format
        if isinstance(expr.key, tuple):
            key1, key2 = expr.key
            return f"SELECT {func_name}(%s, %s)", (key1, key2)
        else:
            return f"SELECT {func_name}(%s)", (expr.key,)

    def format_advisory_unlock_all(self, expr: "AdvisoryUnlockAllExpression") -> Tuple[str, tuple]:
        """
        Format SQL for releasing all advisory locks.

        Generates:
        - SELECT pg_advisory_unlock_all()

        Args:
            expr: AdvisoryUnlockAllExpression

        Returns:
            Tuple of (SQL string, parameters)
        """
        return "SELECT pg_advisory_unlock_all()", ()

    def format_try_advisory_lock(self, expr: "TryAdvisoryLockExpression") -> Tuple[str, tuple]:
        """
        Format SQL for non-blocking advisory lock acquisition.

        Generates:
        - SELECT pg_try_advisory_lock(key) for exclusive session lock
        - SELECT pg_try_advisory_lock_shared(key) for shared session lock
        - SELECT pg_try_advisory_xact_lock(key) for exclusive transaction lock
        - SELECT pg_try_advisory_xact_lock_shared(key) for shared transaction lock

        Args:
            expr: TryAdvisoryLockExpression with lock parameters

        Returns:
            Tuple of (SQL string, parameters)
        """
        # Build function name based on options
        if expr.session:
            # Session-level lock
            if expr.shared:
                func_name = "pg_try_advisory_lock_shared"
            else:
                func_name = "pg_try_advisory_lock"
        else:
            # Transaction-level lock
            if expr.shared:
                func_name = "pg_try_advisory_xact_lock_shared"
            else:
                func_name = "pg_try_advisory_xact_lock"

        # Handle key format
        if isinstance(expr.key, tuple):
            key1, key2 = expr.key
            return f"SELECT {func_name}(%s, %s)", (key1, key2)
        else:
            return f"SELECT {func_name}(%s)", (expr.key,)