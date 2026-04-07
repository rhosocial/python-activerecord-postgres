# src/rhosocial/activerecord/backend/impl/postgres/transaction.py
"""PostgreSQL transaction management with sync and async support.

This module provides simplified transaction management for PostgreSQL.
All SQL generation is delegated to the dialect's format_* methods via
the Expression system, ensuring single SQL statement per format method.
"""
from typing import Dict, Optional, List, TYPE_CHECKING
import logging

from rhosocial.activerecord.backend.errors import TransactionError
from rhosocial.activerecord.backend.transaction import (
    TransactionManager,
    AsyncTransactionManager,
    IsolationLevel,
    TransactionState,
    TransactionMode,
)

if TYPE_CHECKING:
    from .backend import PostgresBackend
    from .async_backend import AsyncPostgresBackend


class PostgresTransactionManager(TransactionManager):
    """Synchronous PostgreSQL transaction manager implementation.

    All transaction operations are delegated to the base class
    which uses backend.execute() for SQL execution.
    PostgreSQL-specific features like DEFERRABLE are supported.
    """

    # PostgreSQL supported isolation level mappings
    _ISOLATION_LEVELS: Dict[IsolationLevel, str] = {
        IsolationLevel.READ_UNCOMMITTED: "READ UNCOMMITTED",
        IsolationLevel.READ_COMMITTED: "READ COMMITTED",  # PostgreSQL default
        IsolationLevel.REPEATABLE_READ: "REPEATABLE READ",
        IsolationLevel.SERIALIZABLE: "SERIALIZABLE",
    }

    def __init__(self, backend: "PostgresBackend", logger=None):
        """Initialize PostgreSQL transaction manager.

        Args:
            backend: PostgreSQL backend instance.
            logger: Optional logger instance.
        """
        super().__init__(backend, logger)
        # PostgreSQL default isolation level is READ COMMITTED
        self._isolation_level = IsolationLevel.READ_COMMITTED
        # DEFERRABLE mode for SERIALIZABLE transactions
        self._is_deferrable: Optional[bool] = None
        # Deferred constraints list
        self._deferred_constraints: List[str] = []

    def _build_begin_sql(self) -> tuple:
        """Build BEGIN TRANSACTION SQL with PostgreSQL-specific options.

        This method creates a BeginTransactionExpression and delegates
        SQL generation to the backend's dialect. All options including
        DEFERRABLE are handled by the Expression/Dialect system.

        Returns:
            Tuple of (SQL string, parameters tuple).
        """
        from rhosocial.activerecord.backend.expression.transaction import BeginTransactionExpression

        expr = BeginTransactionExpression(self._backend.dialect)

        if self._isolation_level is not None:
            expr.isolation_level(self._isolation_level)

        if self._transaction_mode == TransactionMode.READ_ONLY:
            expr.read_only()

        # DEFERRABLE is handled by the Expression/Dialect system
        if self._isolation_level == IsolationLevel.SERIALIZABLE and self._is_deferrable is not None:
            expr.deferrable(self._is_deferrable)

        return expr.to_sql()

    def set_deferrable(self, deferrable: bool = True) -> None:
        """Set transaction deferrable mode.

        In PostgreSQL, DEFERRABLE only affects SERIALIZABLE transactions.

        Args:
            deferrable: Whether constraints should be deferrable

        Raises:
            TransactionError: If called on active transaction
        """
        if self.is_active:
            error_msg = "Cannot change deferrable mode of active transaction"
            self.log(logging.ERROR, error_msg)
            raise TransactionError(error_msg)
        self._is_deferrable = deferrable
        self.log(logging.DEBUG, f"Set deferrable mode to {deferrable}")

    def get_deferred_constraints(self) -> List[str]:
        """Get list of currently deferred constraints.

        Returns:
            Names of deferred constraints
        """
        return self._deferred_constraints.copy()

    def defer_constraint(self, constraint_name: str) -> None:
        """Defer constraint checking until transaction commit.

        Args:
            constraint_name: Name of the constraint to defer

        Raises:
            TransactionError: If constraint deferral fails
        """
        try:
            self.log(logging.DEBUG, f"Deferring constraint: {constraint_name}")
            self._backend.execute(f"SET CONSTRAINTS {constraint_name} DEFERRED")
            self._deferred_constraints.append(constraint_name)
            self.log(logging.INFO, f"Deferred constraint: {constraint_name}")
        except Exception as e:
            error_msg = f"Failed to defer constraint {constraint_name}: {str(e)}"
            self.log(logging.ERROR, error_msg)
            raise TransactionError(error_msg) from e

    def get_current_isolation_level(self) -> IsolationLevel:
        """Get current transaction isolation level.

        Returns:
            Current isolation level

        Raises:
            TransactionError: If getting isolation level fails
        """
        try:
            result = self._backend.execute("SHOW transaction_isolation")
            if result.data:
                # result.data is a list of dicts with column names as keys
                level_name = result.data[0]['transaction_isolation'].upper().replace(" ", "_")
                for isolation_level, pg_level in self._ISOLATION_LEVELS.items():
                    if pg_level.upper().replace(" ", "_") == level_name:
                        return isolation_level
            return IsolationLevel.READ_COMMITTED
        except Exception as e:
            error_msg = f"Failed to get transaction isolation level: {str(e)}"
            self.log(logging.ERROR, error_msg)
            raise TransactionError(error_msg) from e

    def get_active_savepoint(self) -> Optional[str]:
        """Get the name of the most recently created active savepoint.

        Returns:
            Name of the most recent savepoint, or None if no savepoints are active.
        """
        if self._active_savepoints:
            return self._active_savepoints[-1]
        return None

    def commit(self) -> None:
        """Commit the current transaction and clear deferred constraints."""
        super().commit()
        # Clear deferred constraints when outermost transaction commits
        if self._transaction_level == 0:
            self._deferred_constraints.clear()

    def rollback(self) -> None:
        """Rollback the current transaction and clear deferred constraints."""
        super().rollback()
        # Clear deferred constraints when outermost transaction rolls back
        if self._transaction_level == 0:
            self._deferred_constraints.clear()


class AsyncPostgresTransactionManager(AsyncTransactionManager):
    """Asynchronous PostgreSQL transaction manager implementation.

    All transaction operations are delegated to the base class
    which uses backend.execute() for SQL execution.
    PostgreSQL-specific features like DEFERRABLE are supported.
    """

    # PostgreSQL supported isolation level mappings
    _ISOLATION_LEVELS: Dict[IsolationLevel, str] = {
        IsolationLevel.READ_UNCOMMITTED: "READ UNCOMMITTED",
        IsolationLevel.READ_COMMITTED: "READ COMMITTED",  # PostgreSQL default
        IsolationLevel.REPEATABLE_READ: "REPEATABLE READ",
        IsolationLevel.SERIALIZABLE: "SERIALIZABLE",
    }

    def __init__(self, backend: "AsyncPostgresBackend", logger=None):
        """Initialize async PostgreSQL transaction manager.

        Args:
            backend: Async PostgreSQL backend instance.
            logger: Optional logger instance.
        """
        super().__init__(backend, logger)
        # PostgreSQL default isolation level is READ COMMITTED
        self._isolation_level = IsolationLevel.READ_COMMITTED
        # DEFERRABLE mode for SERIALIZABLE transactions
        self._is_deferrable: Optional[bool] = None
        # Deferred constraints list
        self._deferred_constraints: List[str] = []

    def _build_begin_sql(self) -> tuple:
        """Build BEGIN TRANSACTION SQL with PostgreSQL-specific options.

        This method creates a BeginTransactionExpression and delegates
        SQL generation to the backend's dialect. All options including
        DEFERRABLE are handled by the Expression/Dialect system.

        Returns:
            Tuple of (SQL string, parameters tuple).
        """
        from rhosocial.activerecord.backend.expression.transaction import BeginTransactionExpression

        expr = BeginTransactionExpression(self._backend.dialect)

        if self._isolation_level is not None:
            expr.isolation_level(self._isolation_level)

        if self._transaction_mode == TransactionMode.READ_ONLY:
            expr.read_only()

        # DEFERRABLE is handled by the Expression/Dialect system
        if self._isolation_level == IsolationLevel.SERIALIZABLE and self._is_deferrable is not None:
            expr.deferrable(self._is_deferrable)

        return expr.to_sql()

    async def set_deferrable(self, deferrable: bool = True) -> None:
        """Set transaction deferrable mode.

        In PostgreSQL, DEFERRABLE only affects SERIALIZABLE transactions.

        Args:
            deferrable: Whether constraints should be deferrable

        Raises:
            TransactionError: If called on active transaction
        """
        if self.is_active:
            error_msg = "Cannot change deferrable mode of active transaction"
            self.log(logging.ERROR, error_msg)
            raise TransactionError(error_msg)
        self._is_deferrable = deferrable
        self.log(logging.DEBUG, f"Set deferrable mode to {deferrable}")

    def get_deferred_constraints(self) -> List[str]:
        """Get list of currently deferred constraints.

        Returns:
            Names of deferred constraints
        """
        return self._deferred_constraints.copy()

    async def defer_constraint(self, constraint_name: str) -> None:
        """Defer constraint checking until transaction commit.

        Args:
            constraint_name: Name of the constraint to defer

        Raises:
            TransactionError: If constraint deferral fails
        """
        try:
            self.log(logging.DEBUG, f"Deferring constraint: {constraint_name}")
            await self._backend.execute(f"SET CONSTRAINTS {constraint_name} DEFERRED")
            self._deferred_constraints.append(constraint_name)
            self.log(logging.INFO, f"Deferred constraint: {constraint_name}")
        except Exception as e:
            error_msg = f"Failed to defer constraint {constraint_name}: {str(e)}"
            self.log(logging.ERROR, error_msg)
            raise TransactionError(error_msg) from e

    async def get_current_isolation_level(self) -> IsolationLevel:
        """Get current transaction isolation level asynchronously.

        Returns:
            Current isolation level

        Raises:
            TransactionError: If getting isolation level fails
        """
        try:
            result = await self._backend.execute("SHOW transaction_isolation")
            if result.data:
                # result.data is a list of dicts with column names as keys
                level_name = result.data[0]['transaction_isolation'].upper().replace(" ", "_")
                for isolation_level, pg_level in self._ISOLATION_LEVELS.items():
                    if pg_level.upper().replace(" ", "_") == level_name:
                        return isolation_level
            return IsolationLevel.READ_COMMITTED
        except Exception as e:
            error_msg = f"Failed to get transaction isolation level: {str(e)}"
            self.log(logging.ERROR, error_msg)
            raise TransactionError(error_msg) from e

    def get_active_savepoint(self) -> Optional[str]:
        """Get the name of the most recently created active savepoint.

        Returns:
            Name of the most recent savepoint, or None if no savepoints are active.
        """
        if self._active_savepoints:
            return self._active_savepoints[-1]
        return None

    async def commit(self) -> None:
        """Commit the current transaction and clear deferred constraints."""
        await super().commit()
        # Clear deferred constraints when outermost transaction commits
        if self._transaction_level == 0:
            self._deferred_constraints.clear()

    async def rollback(self) -> None:
        """Rollback the current transaction and clear deferred constraints."""
        await super().rollback()
        # Clear deferred constraints when outermost transaction rolls back
        if self._transaction_level == 0:
            self._deferred_constraints.clear()
