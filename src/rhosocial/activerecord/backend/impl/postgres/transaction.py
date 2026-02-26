# src/rhosocial/activerecord/backend/impl/postgres/transaction.py
"""postgres transaction management with sync and async support."""

from typing import Dict, Optional, List
import logging
from psycopg.errors import Error as PsycopgError

from ...errors import TransactionError
from ...transaction import (
    TransactionManager,
    AsyncTransactionManager,
    IsolationLevel,
    TransactionState
)


class PostgresTransactionMixin:
    """Shared logic for postgres transaction management.

    Contains non-I/O logic shared between sync and async implementations:
    - Isolation level mapping
    - Savepoint name generation
    - SQL statement building
    """

    # postgres supported isolation level mappings
    _ISOLATION_LEVELS: Dict[IsolationLevel, str] = {
        IsolationLevel.READ_UNCOMMITTED: "READ UNCOMMITTED",
        IsolationLevel.READ_COMMITTED: "READ COMMITTED",  # postgres default
        IsolationLevel.REPEATABLE_READ: "REPEATABLE READ",
        IsolationLevel.SERIALIZABLE: "SERIALIZABLE"
    }

    def __init__(self, connection, logger=None):
        """Initialize transaction manager.

        Args:
            connection: postgres database connection
            logger: Optional logger instance
        """
        self._active_savepoint = None
        self._savepoint_counter = 0
        self._deferred_constraints: List[str] = []
        self._is_deferrable: Optional[bool] = None
        self._active_transaction = False
        self._state = TransactionState.INACTIVE

    def _get_savepoint_name(self, level: int) -> str:
        """Generate savepoint name for nested transactions.

        Args:
            level: Transaction nesting level

        Returns:
            Savepoint name
        """
        return f"SP_{level}"

    def _build_begin_statement(self) -> str:
        """Build BEGIN statement with isolation level and deferrable mode.

        Returns:
            SQL BEGIN statement
        """
        sql_parts = ["BEGIN"]

        # Add isolation level
        if self._isolation_level:
            level = self._ISOLATION_LEVELS.get(self._isolation_level)
            if level:
                sql_parts.append(f"ISOLATION LEVEL {level}")

        # Add deferrable mode for SERIALIZABLE transactions
        if (self._isolation_level == IsolationLevel.SERIALIZABLE and
                self._is_deferrable is not None):
            sql_parts.append(
                "DEFERRABLE" if self._is_deferrable else "NOT DEFERRABLE"
            )

        return " ".join(sql_parts)

    def set_deferrable(self, deferrable: bool = True) -> None:
        """Set transaction deferrable mode.

        In postgres, DEFERRABLE only affects SERIALIZABLE transactions.

        Args:
            deferrable: Whether constraints should be deferrable

        Raises:
            TransactionError: If called on active transaction
        """
        self._is_deferrable = deferrable
        if self.is_active:
            error_msg = "Cannot change deferrable mode of active transaction"
            self.log(logging.ERROR, error_msg)
            raise TransactionError(error_msg)
        self.log(logging.DEBUG, f"Set deferrable mode to {deferrable}")

    def get_active_savepoint(self) -> Optional[str]:
        """Get name of active savepoint.

        Returns:
            Active savepoint name or None
        """
        return self._active_savepoint

    def get_deferred_constraints(self) -> List[str]:
        """Get list of currently deferred constraints.

        Returns:
            Names of deferred constraints
        """
        return self._deferred_constraints.copy()

    def supports_savepoint(self) -> bool:
        """Check if savepoints are supported.

        Returns:
            Always True for postgres
        """
        return True

    @property
    def is_active(self) -> bool:
        """Check if transaction is active.

        Returns:
            True if in transaction
        """
        if not self._connection or self._connection.closed:
            return False

        # Check based on autocommit mode
        if self._connection.autocommit:
            return False

        # If we've explicitly tracked transaction state, use that
        return self._active_transaction


class PostgresTransactionManager(PostgresTransactionMixin, TransactionManager):
    """Synchronous postgres transaction manager implementation."""

    def __init__(self, connection, logger=None):
        """Initialize sync transaction manager.

        Args:
            connection: postgres database connection (sync)
            logger: Optional logger instance
        """
        TransactionManager.__init__(self, connection, logger)
        PostgresTransactionMixin.__init__(self, connection, logger)

    def _set_isolation_level(self) -> None:
        """Set transaction isolation level.

        This is called at the start of each transaction.

        Raises:
            TransactionError: If setting isolation level fails
        """
        if self._isolation_level:
            level = self._ISOLATION_LEVELS.get(self._isolation_level)
            if level:
                try:
                    self.log(logging.DEBUG, f"Setting isolation level to {level}")
                    cursor = self._connection.cursor()
                    cursor.execute(f"SET TRANSACTION ISOLATION LEVEL {level}")
                    cursor.close()
                except PsycopgError as e:
                    error_msg = f"Failed to set isolation level to {level}: {str(e)}"
                    self.log(logging.ERROR, error_msg)
                    raise TransactionError(error_msg)
            else:
                error_msg = f"Unsupported isolation level: {self._isolation_level}"
                self.log(logging.ERROR, error_msg)
                raise TransactionError(error_msg)

    def defer_constraint(self, constraint_name: str) -> None:
        """Defer constraint checking until transaction commit.

        Args:
            constraint_name: Name of the constraint to defer

        Raises:
            TransactionError: If constraint deferral fails
        """
        try:
            self.log(logging.DEBUG, f"Deferring constraint: {constraint_name}")
            cursor = self._connection.cursor()
            cursor.execute(f"SET CONSTRAINTS {constraint_name} DEFERRED")
            cursor.close()
            self._deferred_constraints.append(constraint_name)
            self.log(logging.INFO, f"Deferred constraint: {constraint_name}")
        except PsycopgError as e:
            error_msg = f"Failed to defer constraint {constraint_name}: {str(e)}"
            self.log(logging.ERROR, error_msg)
            raise TransactionError(error_msg)

    def _do_begin(self) -> None:
        """Begin postgres transaction.

        Sets isolation level and starts transaction.

        Raises:
            TransactionError: If begin fails
        """
        try:
            self.log(logging.DEBUG, "Beginning transaction")

            # Set connection to non-autocommit mode
            if self._connection.autocommit:
                self._connection.autocommit = False
                self.log(logging.DEBUG, "Set autocommit mode to False")

            # Build and execute BEGIN statement
            begin_sql = self._build_begin_statement()
            cursor = self._connection.cursor()
            self.log(logging.DEBUG, f"Executing: {begin_sql}")
            cursor.execute(begin_sql)
            cursor.close()

            self._active_transaction = True
            self._state = TransactionState.ACTIVE
            self.log(logging.INFO, f"Started transaction with isolation level {self._isolation_level}")

        except PsycopgError as e:
            error_msg = f"Failed to begin transaction: {str(e)}"
            self.log(logging.ERROR, error_msg)
            raise TransactionError(error_msg)

    def _do_commit(self) -> None:
        """Commit postgres transaction.

        Raises:
            TransactionError: If commit fails
        """
        try:
            self.log(logging.DEBUG, "Committing transaction")
            self._connection.commit()
            self._active_transaction = False
            self._state = TransactionState.COMMITTED
            self.log(logging.INFO, "Transaction committed")
        except PsycopgError as e:
            error_msg = f"Failed to commit transaction: {str(e)}"
            self.log(logging.ERROR, error_msg)
            raise TransactionError(error_msg)
        finally:
            self._active_savepoint = None
            self._savepoint_counter = 0
            self._deferred_constraints.clear()

    def _do_rollback(self) -> None:
        """Rollback postgres transaction.

        Raises:
            TransactionError: If rollback fails
        """
        try:
            self.log(logging.DEBUG, "Rolling back transaction")
            self._connection.rollback()
            self._active_transaction = False
            self._state = TransactionState.ROLLED_BACK
            self.log(logging.INFO, "Transaction rolled back")
        except PsycopgError as e:
            error_msg = f"Failed to rollback transaction: {str(e)}"
            self.log(logging.ERROR, error_msg)
            raise TransactionError(error_msg)
        finally:
            self._active_savepoint = None
            self._savepoint_counter = 0
            self._deferred_constraints.clear()

    def _do_create_savepoint(self, name: str) -> None:
        """Create postgres savepoint.

        Args:
            name: Savepoint name

        Raises:
            TransactionError: If create savepoint fails
        """
        try:
            # postgres requires active transaction for savepoints
            if not self.is_active:
                self.log(logging.DEBUG, "No active transaction, beginning one before creating savepoint")
                self._do_begin()

            cursor = self._connection.cursor()
            sql = f"SAVEPOINT {name}"
            self.log(logging.DEBUG, f"Executing: {sql}")
            cursor.execute(sql)
            cursor.close()
            self._active_savepoint = name
            self.log(logging.INFO, f"Created savepoint: {name}")
        except PsycopgError as e:
            error_msg = f"Failed to create savepoint {name}: {str(e)}"
            self.log(logging.ERROR, error_msg)
            raise TransactionError(error_msg)

    def _do_release_savepoint(self, name: str) -> None:
        """Release postgres savepoint.

        Args:
            name: Savepoint name

        Raises:
            TransactionError: If release savepoint fails
        """
        try:
            cursor = self._connection.cursor()
            sql = f"RELEASE SAVEPOINT {name}"
            self.log(logging.DEBUG, f"Executing: {sql}")
            cursor.execute(sql)
            cursor.close()
            if self._active_savepoint == name:
                self._active_savepoint = None
            self.log(logging.INFO, f"Released savepoint: {name}")
        except PsycopgError as e:
            error_msg = f"Failed to release savepoint {name}: {str(e)}"
            self.log(logging.ERROR, error_msg)
            raise TransactionError(error_msg)

    def _do_rollback_savepoint(self, name: str) -> None:
        """Rollback to postgres savepoint.

        Args:
            name: Savepoint name

        Raises:
            TransactionError: If rollback to savepoint fails
        """
        try:
            cursor = self._connection.cursor()
            sql = f"ROLLBACK TO SAVEPOINT {name}"
            self.log(logging.DEBUG, f"Executing: {sql}")
            cursor.execute(sql)
            cursor.close()
            if self._active_savepoint == name:
                self._active_savepoint = None
            self.log(logging.INFO, f"Rolled back to savepoint: {name}")
        except PsycopgError as e:
            error_msg = f"Failed to rollback to savepoint {name}: {str(e)}"
            self.log(logging.ERROR, error_msg)
            raise TransactionError(error_msg)

    def get_current_isolation_level(self) -> IsolationLevel:
        """Get current transaction isolation level.

        Returns:
            Current isolation level

        Raises:
            TransactionError: If getting isolation level fails
        """
        try:
            cursor = self._connection.cursor()
            cursor.execute("SHOW transaction_isolation")
            result = cursor.fetchone()
            cursor.close()

            if result:
                # Convert postgres level name to IsolationLevel enum
                level_name = result[0].upper().replace(' ', '_')
                for isolation_level, pg_level in self._ISOLATION_LEVELS.items():
                    if pg_level.upper().replace(' ', '_') == level_name:
                        return isolation_level

            # Default to READ COMMITTED if not found
            return IsolationLevel.READ_COMMITTED

        except PsycopgError as e:
            error_msg = f"Failed to get transaction isolation level: {str(e)}"
            self.log(logging.ERROR, error_msg)
            raise TransactionError(error_msg)


class AsyncPostgresTransactionMixin:
    """Async version of shared logic for postgres transaction management.

    Contains non-I/O logic shared between sync and async implementations:
    - Isolation level mapping
    - Savepoint name generation
    - SQL statement building
    """

    # postgres supported isolation level mappings
    _ISOLATION_LEVELS: Dict[IsolationLevel, str] = {
        IsolationLevel.READ_UNCOMMITTED: "READ UNCOMMITTED",
        IsolationLevel.READ_COMMITTED: "READ COMMITTED",  # postgres default
        IsolationLevel.REPEATABLE_READ: "REPEATABLE READ",
        IsolationLevel.SERIALIZABLE: "SERIALIZABLE"
    }

    def __init__(self, connection, logger=None):
        """Initialize transaction manager.

        Args:
            connection: postgres database connection
            logger: Optional logger instance
        """
        self._active_savepoint = None
        self._savepoint_counter = 0
        self._deferred_constraints: List[str] = []
        self._is_deferrable: Optional[bool] = None
        self._active_transaction = False
        self._state = TransactionState.INACTIVE

    def _get_savepoint_name(self, level: int) -> str:
        """Generate savepoint name for nested transactions.

        Args:
            level: Transaction nesting level

        Returns:
            Savepoint name
        """
        return f"SP_{level}"

    def _build_begin_statement(self) -> str:
        """Build BEGIN statement with isolation level and deferrable mode.

        Returns:
            SQL BEGIN statement
        """
        sql_parts = ["BEGIN"]

        # Add isolation level
        if self._isolation_level:
            level = self._ISOLATION_LEVELS.get(self._isolation_level)
            if level:
                sql_parts.append(f"ISOLATION LEVEL {level}")

        # Add deferrable mode for SERIALIZABLE transactions
        if (self._isolation_level == IsolationLevel.SERIALIZABLE and
                self._is_deferrable is not None):
            sql_parts.append(
                "DEFERRABLE" if self._is_deferrable else "NOT DEFERRABLE"
            )

        return " ".join(sql_parts)

    async def set_deferrable(self, deferrable: bool = True) -> None:
        """Set transaction deferrable mode.

        In postgres, DEFERRABLE only affects SERIALIZABLE transactions.

        Args:
            deferrable: Whether constraints should be deferrable

        Raises:
            TransactionError: If called on active transaction
        """
        self._is_deferrable = deferrable
        if self.is_active:
            error_msg = "Cannot change deferrable mode of active transaction"
            self.log(logging.ERROR, error_msg)
            raise TransactionError(error_msg)
        self.log(logging.DEBUG, f"Set deferrable mode to {deferrable}")

    def get_active_savepoint(self) -> Optional[str]:
        """Get name of active savepoint.

        Returns:
            Active savepoint name or None
        """
        return self._active_savepoint

    def get_deferred_constraints(self) -> List[str]:
        """Get list of currently deferred constraints.

        Returns:
            Names of deferred constraints
        """
        return self._deferred_constraints.copy()

    async def supports_savepoint(self) -> bool:
        """Check if savepoints are supported.

        Returns:
            Always True for postgres
        """
        return True

    @property
    def is_active(self) -> bool:
        """Check if transaction is active.

        Returns:
            True if in transaction
        """
        if not self._connection or self._connection.closed:
            return False

        # Check based on autocommit mode
        if self._connection.autocommit:
            return False

        # If we've explicitly tracked transaction state, use that
        return self._active_transaction


class AsyncPostgresTransactionManager(AsyncTransactionManager):
    """Asynchronous postgres transaction manager implementation."""

    # postgres supported isolation level mappings
    _ISOLATION_LEVELS: Dict[IsolationLevel, str] = {
        IsolationLevel.READ_UNCOMMITTED: "READ UNCOMMITTED",
        IsolationLevel.READ_COMMITTED: "READ COMMITTED",  # postgres default
        IsolationLevel.REPEATABLE_READ: "REPEATABLE READ",
        IsolationLevel.SERIALIZABLE: "SERIALIZABLE"
    }

    def __init__(self, connection, logger=None):
        """Initialize async transaction manager.

        Args:
            connection: postgres database connection (async)
            logger: Optional logger instance
        """
        super().__init__(connection, logger)
        self._active_savepoint = None
        self._savepoint_counter = 0
        self._deferred_constraints: List[str] = []
        self._is_deferrable: Optional[bool] = None
        self._active_transaction = False
        self._state = TransactionState.INACTIVE
        self._isolation_level: Optional[IsolationLevel] = None

    async def _set_isolation_level(self) -> None:
        """Set transaction isolation level asynchronously.

        This is called at the start of each transaction.

        Raises:
            TransactionError: If setting isolation level fails
        """
        if self._isolation_level:
            level = self._ISOLATION_LEVELS.get(self._isolation_level)
            if level:
                try:
                    self.log(logging.DEBUG, f"Setting isolation level to {level}")
                    cursor = self._connection.cursor()
                    await cursor.execute(f"SET TRANSACTION ISOLATION LEVEL {level}")
                    await cursor.close()
                except PsycopgError as e:
                    error_msg = f"Failed to set isolation level to {level}: {str(e)}"
                    self.log(logging.ERROR, error_msg)
                    raise TransactionError(error_msg)
            else:
                error_msg = f"Unsupported isolation level: {self._isolation_level}"
                self.log(logging.ERROR, error_msg)
                raise TransactionError(error_msg)

    async def defer_constraint(self, constraint_name: str) -> None:
        """Defer constraint checking until transaction commit (async).

        Args:
            constraint_name: Name of the constraint to defer

        Raises:
            TransactionError: If constraint deferral fails
        """
        try:
            self.log(logging.DEBUG, f"Deferring constraint: {constraint_name}")
            cursor = self._connection.cursor()
            await cursor.execute(f"SET CONSTRAINTS {constraint_name} DEFERRED")
            await cursor.close()
            self._deferred_constraints.append(constraint_name)
            self.log(logging.INFO, f"Deferred constraint: {constraint_name}")
        except PsycopgError as e:
            error_msg = f"Failed to defer constraint {constraint_name}: {str(e)}"
            self.log(logging.ERROR, error_msg)
            raise TransactionError(error_msg)

    def _build_begin_statement(self) -> str:
        """Build BEGIN statement with isolation level and deferrable mode.

        Returns:
            SQL BEGIN statement
        """
        sql_parts = ["BEGIN"]

        # Add isolation level
        if self._isolation_level:
            level = self._ISOLATION_LEVELS.get(self._isolation_level)
            if level:
                sql_parts.append(f"ISOLATION LEVEL {level}")

        # Add deferrable mode for SERIALIZABLE transactions
        if (self._isolation_level == IsolationLevel.SERIALIZABLE and
                self._is_deferrable is not None):
            sql_parts.append(
                "DEFERRABLE" if self._is_deferrable else "NOT DEFERRABLE"
            )

        return " ".join(sql_parts)

    async def _do_begin(self) -> None:
        """Begin postgres transaction asynchronously.

        Sets isolation level and starts transaction.

        Raises:
            TransactionError: If begin fails
        """
        try:
            self.log(logging.DEBUG, "Beginning transaction")

            # Set connection to non-autocommit mode
            if self._connection.autocommit:
                self._connection.autocommit = False
                self.log(logging.DEBUG, "Set autocommit mode to False")

            # Build and execute BEGIN statement
            begin_sql = self._build_begin_statement()
            cursor = self._connection.cursor()
            self.log(logging.DEBUG, f"Executing: {begin_sql}")
            await cursor.execute(begin_sql)
            await cursor.close()

            self._active_transaction = True
            self._state = TransactionState.ACTIVE
            self.log(logging.INFO, f"Started transaction with isolation level {self._isolation_level}")

        except PsycopgError as e:
            error_msg = f"Failed to begin transaction: {str(e)}"
            self.log(logging.ERROR, error_msg)
            raise TransactionError(error_msg)

    async def _do_commit(self) -> None:
        """Commit postgres transaction asynchronously.

        Raises:
            TransactionError: If commit fails
        """
        try:
            self.log(logging.DEBUG, "Committing transaction")
            await self._connection.commit()
            self._active_transaction = False
            self._state = TransactionState.COMMITTED
            self.log(logging.INFO, "Transaction committed")
        except PsycopgError as e:
            error_msg = f"Failed to commit transaction: {str(e)}"
            self.log(logging.ERROR, error_msg)
            raise TransactionError(error_msg)
        finally:
            self._active_savepoint = None
            self._savepoint_counter = 0
            self._deferred_constraints.clear()

    async def _do_rollback(self) -> None:
        """Rollback postgres transaction asynchronously.

        Raises:
            TransactionError: If rollback fails
        """
        try:
            self.log(logging.DEBUG, "Rolling back transaction")
            await self._connection.rollback()
            self._active_transaction = False
            self._state = TransactionState.ROLLED_BACK
            self.log(logging.INFO, "Transaction rolled back")
        except PsycopgError as e:
            error_msg = f"Failed to rollback transaction: {str(e)}"
            self.log(logging.ERROR, error_msg)
            raise TransactionError(error_msg)
        finally:
            self._active_savepoint = None
            self._savepoint_counter = 0
            self._deferred_constraints.clear()

    async def _do_create_savepoint(self, name: str) -> None:
        """Create postgres savepoint asynchronously.

        Args:
            name: Savepoint name

        Raises:
            TransactionError: If create savepoint fails
        """
        try:
            # postgres requires active transaction for savepoints
            if not self.is_active:
                self.log(logging.DEBUG, "No active transaction, beginning one before creating savepoint")
                await self._do_begin()

            cursor = self._connection.cursor()
            sql = f"SAVEPOINT {name}"
            self.log(logging.DEBUG, f"Executing: {sql}")
            await cursor.execute(sql)
            await cursor.close()
            self._active_savepoint = name
            self.log(logging.INFO, f"Created savepoint: {name}")
        except PsycopgError as e:
            error_msg = f"Failed to create savepoint {name}: {str(e)}"
            self.log(logging.ERROR, error_msg)
            raise TransactionError(error_msg)

    async def _do_release_savepoint(self, name: str) -> None:
        """Release postgres savepoint asynchronously.

        Args:
            name: Savepoint name

        Raises:
            TransactionError: If release savepoint fails
        """
        try:
            cursor = self._connection.cursor()
            sql = f"RELEASE SAVEPOINT {name}"
            self.log(logging.DEBUG, f"Executing: {sql}")
            await cursor.execute(sql)
            await cursor.close()
            if self._active_savepoint == name:
                self._active_savepoint = None
            self.log(logging.INFO, f"Released savepoint: {name}")
        except PsycopgError as e:
            error_msg = f"Failed to release savepoint {name}: {str(e)}"
            self.log(logging.ERROR, error_msg)
            raise TransactionError(error_msg)

    async def _do_rollback_savepoint(self, name: str) -> None:
        """Rollback to postgres savepoint asynchronously.

        Args:
            name: Savepoint name

        Raises:
            TransactionError: If rollback to savepoint fails
        """
        try:
            cursor = self._connection.cursor()
            sql = f"ROLLBACK TO SAVEPOINT {name}"
            self.log(logging.DEBUG, f"Executing: {sql}")
            await cursor.execute(sql)
            await cursor.close()
            if self._active_savepoint == name:
                self._active_savepoint = None
            self.log(logging.INFO, f"Rolled back to savepoint: {name}")
        except PsycopgError as e:
            error_msg = f"Failed to rollback to savepoint {name}: {str(e)}"
            self.log(logging.ERROR, error_msg)
            raise TransactionError(error_msg)

    async def supports_savepoint(self) -> bool:
        """Check if savepoints are supported.

        Returns:
            Always True for postgres
        """
        return True

    async def get_current_isolation_level(self) -> IsolationLevel:
        """Get current transaction isolation level asynchronously.

        Returns:
            Current isolation level

        Raises:
            TransactionError: If getting isolation level fails
        """
        try:
            cursor = self._connection.cursor()
            await cursor.execute("SHOW transaction_isolation")
            result = await cursor.fetchone()
            await cursor.close()

            if result:
                # Convert postgres level name to IsolationLevel enum
                level_name = result[0].upper().replace(' ', '_')
                for isolation_level, pg_level in self._ISOLATION_LEVELS.items():
                    if pg_level.upper().replace(' ', '_') == level_name:
                        return isolation_level

            # Default to READ COMMITTED if not found
            return IsolationLevel.READ_COMMITTED

        except PsycopgError as e:
            error_msg = f"Failed to get transaction isolation level: {str(e)}"
            self.log(logging.ERROR, error_msg)
            raise TransactionError(error_msg)