# src/rhosocial/activerecord/backend/impl/postgres/backend/async_backend.py
"""PostgreSQL asynchronous backend implementation.

This module provides an asynchronous PostgreSQL implementation:
- PostgreSQL asynchronous backend with connection management and query execution
- PostgreSQL-specific connection configuration
- Type mapping and value conversion
- Transaction management with savepoint support
- PostgreSQL dialect and expression handling
- PostgreSQL-specific type definitions and mappings

Architecture:
- AsyncPostgresBackend: Asynchronous implementation using psycopg
- Independent from ORM frameworks - uses only native drivers
"""

import datetime
import logging
from typing import Dict, List, Optional, Tuple

import psycopg
from psycopg import AsyncConnection
from psycopg.errors import Error as PsycopgError
from psycopg.errors import (
    IntegrityError as PsycopgIntegrityError,
    OperationalError as PsycopgOperationalError,
    ProgrammingError as PsycopgProgrammingError,
    DeadlockDetected as PsycopgDeadlockError,
)

from rhosocial.activerecord.backend.base import AsyncStorageBackend
from rhosocial.activerecord.backend.introspection.backend_mixin import IntrospectorBackendMixin
from rhosocial.activerecord.backend.explain import AsyncExplainBackendMixin
from rhosocial.activerecord.backend.errors import (
    ConnectionError,
    DatabaseError,
    DeadlockError,
    IntegrityError,
    OperationalError,
    QueryError,
)
from rhosocial.activerecord.backend.result import QueryResult
from ..config import PostgresConnectionConfig
from ..dialect import PostgresDialect
from .base import PostgresBackendMixin, PostgresConcurrencyMixin
from ..protocols import PostgresExtensionInfo
from ..transaction import AsyncPostgresTransactionManager


class AsyncPostgresBackend(
    IntrospectorBackendMixin,
    PostgresBackendMixin,
    AsyncExplainBackendMixin,
    PostgresConcurrencyMixin,
    AsyncStorageBackend,
):
    """Asynchronous PostgreSQL-specific backend implementation."""

    def __init__(self, **kwargs):
        """Initialize async PostgreSQL backend with connection configuration.

        Note:
        The dialect is initialized with a default version (9.6.0) and no plugins
        are enabled. Call introspect_and_adapt() after connecting to detect
        the actual server version and installed extensions.
        """
        # Ensure we have proper PostgreSQL configuration
        connection_config = kwargs.get("connection_config")

        if connection_config is None:
            # Extract PostgreSQL-specific parameters from kwargs
            config_params = {}
            pg_specific_params = [
                "host",
                "port",
                "database",
                "username",
                "password",
                "charset",
                "collation",
                "timezone",
                "version",
                "pool_size",
                "pool_timeout",
                "pool_name",
                "pool_reset_session",
                "pool_pre_ping",
                "ssl_ca",
                "ssl_cert",
                "ssl_key",
                "ssl_verify_cert",
                "ssl_verify_identity",
                "log_queries",
                "log_level",
                "application_name",
                "fallback_application_name",
                "connect_timeout",
                "options",
                "service",
                "target_session_attrs",
                "gssencmode",
                "channel_binding",
                "replication",
                "assume_role",
                "role",
                "search_path",
                "row_security",
                "datestyle",
                "intervalstyle",
                "timezone_setting",
                "extra_float_digits",
                "client_encoding",
                "tcp_user_timeout",
                "tcp_keepalives_idle",
                "tcp_keepalives_interval",
                "tcp_keepalives_count",
                "load_balance_hosts",
                "replication",
                "keepalives",
                "keepalives_idle",
                "keepalives_interval",
                "keepalives_count",
            ]

            for param in pg_specific_params:
                if param in kwargs:
                    config_params[param] = kwargs[param]

            # Set defaults if not provided
            if "host" not in config_params:
                config_params["host"] = "localhost"
            if "port" not in config_params:
                config_params["port"] = 5432

            kwargs["connection_config"] = PostgresConnectionConfig(**config_params)

        # Initialize the base class first to ensure all base properties are set
        super().__init__(**kwargs)

        # Initialize PostgreSQL-specific components
        # Use default version (9.6.0) - actual version detected via introspect_and_adapt()
        self._dialect = PostgresDialect((9, 6, 0))

        # Register PostgreSQL-specific type adapters (same as sync backend)
        # Note: XML adapter is NOT registered by default due to potential conflicts.
        # See PostgresXMLAdapter documentation for details.
        self._register_postgres_adapters()

        # Initialize transaction manager (will use backend.execute())
        self._transaction_manager = AsyncPostgresTransactionManager(self, self.logger)

        self.log(logging.INFO, "AsyncPostgreSQLBackend initialized")

    def _create_introspector(self):
        """Create and return an AsyncPostgreSQLIntrospector with an async executor."""
        from rhosocial.activerecord.backend.introspection.executor import AsyncIntrospectorExecutor
        from ..introspection import AsyncPostgreSQLIntrospector

        return AsyncPostgreSQLIntrospector(self, AsyncIntrospectorExecutor(self))

    @property
    def dialect(self):
        """Get the PostgreSQL dialect instance."""
        return self._dialect

    @property
    def transaction_manager(self):
        """Get the async PostgreSQL transaction manager."""
        return self._transaction_manager


    async def connect(self):
        """Establish async connection to PostgreSQL database."""
        try:
            # Prepare connection parameters from config
            conn_params = {
                "host": self.config.host,
                "port": self.config.port,
                "dbname": self.config.database,
                "user": self.config.username,
                "password": self.config.password,
            }

            # Add additional parameters if they exist in config
            additional_params = [
                "application_name",
                "fallback_application_name",
                "connect_timeout",
                "options",
                "service",
                "target_session_attrs",
                "gssencmode",
                "channel_binding",
                "replication",
                "assume_role",
                "role",
                "search_path",
                "row_security",
                "datestyle",
                "intervalstyle",
                "timezone",
                "extra_float_digits",
                "client_encoding",
                "tcp_user_timeout",
                "tcp_keepalives_idle",
                "tcp_keepalives_interval",
                "tcp_keepalives_count",
                "load_balance_hosts",
                "keepalives",
                "keepalives_idle",
                "keepalives_interval",
                "keepalives_count",
            ]

            for param in additional_params:
                if hasattr(self.config, param):
                    value = getattr(self.config, param)
                    if value is not None:  # Only add the parameter if it's not None
                        conn_params[param] = value

            # Handle 'options' parameter specially as it should be a string, not a dict
            if hasattr(self.config, "options") and self.config.options is not None:
                options_value = self.config.options
                if isinstance(options_value, dict):
                    options_str = " ".join([f"-c {k}={v}" for k, v in options_value.items()])
                    conn_params["options"] = options_str
                else:
                    conn_params["options"] = options_value

            # Add SSL parameters if provided
            ssl_params = {}
            if hasattr(self.config, "ssl_ca"):
                ssl_params["sslcert"] = self.config.ssl_ca
            if hasattr(self.config, "ssl_cert"):
                ssl_params["sslcert"] = self.config.ssl_cert
            if hasattr(self.config, "ssl_key"):
                ssl_params["sslkey"] = self.config.ssl_key
            if hasattr(self.config, "ssl_mode"):
                ssl_params["sslmode"] = self.config.ssl_mode

            if ssl_params:
                conn_params.update(ssl_params)

            self._connection = await AsyncConnection.connect(**conn_params)
            await self._connection.set_autocommit(True)  # Disable psycopg auto-transaction management

            self.log(
                logging.INFO,
                f"Connected to PostgreSQL database: {self.config.host}:{self.config.port}/{self.config.database}",
            )
        except PsycopgError as e:
            self.log(logging.ERROR, f"Failed to connect to PostgreSQL database: {str(e)}")
            raise ConnectionError(f"Failed to connect to PostgreSQL: {str(e)}") from None

    async def disconnect(self):
        """Close async connection to PostgreSQL database."""
        if self._connection:
            try:
                # Rollback any active transaction
                if self.in_transaction:
                    await self.transaction_manager.rollback()

                await self._connection.close()
                self._connection = None
                self.log(logging.INFO, "Disconnected from PostgreSQL database")
            except PsycopgError as e:
                self.log(logging.ERROR, f"Error during disconnection: {str(e)}")
                raise OperationalError(f"Error during PostgreSQL disconnection: {str(e)}") from None

    async def _get_cursor(self):
        """Get a database cursor, ensuring connection is active.

        This method implements automatic connection health checking (Plan A):
        - Checks if connection object exists
        - Checks if connection is still valid using closed/broken attributes
        - Automatically reconnects if connection was lost
        """
        if not self._connection:
            self.log(logging.DEBUG, "No connection, connecting...")
            await self.connect()
        elif self._connection.closed or self._connection.broken:
            self.log(logging.DEBUG, "Connection lost, reconnecting...")
            self._connection = None  # Clear the closed/broken connection
            await self.connect()

        return self._connection.cursor()

    async def execute_many(self, sql: str, params_list: List[Tuple]) -> QueryResult:
        """Execute the same SQL statement multiple times with different parameters asynchronously."""
        if not self._connection:
            await self.connect()

        cursor = None
        start_time = datetime.datetime.now()

        try:
            cursor = await self._get_cursor()

            # Log the batch operation if logging is enabled
            if getattr(self.config, "log_queries", False):
                self.log(logging.DEBUG, f"Executing batch operation: {sql}")
                self.log(logging.DEBUG, f"With {len(params_list)} parameter sets")

            # Execute multiple statements
            affected_rows = 0
            for params in params_list:
                await cursor.execute(sql, params)
                affected_rows += cursor.rowcount

            duration = (datetime.datetime.now() - start_time).total_seconds()

            result = QueryResult(affected_rows=affected_rows, data=None, duration=duration)

            self.log(
                logging.INFO,
                f"Batch operation completed, affected {affected_rows} rows, duration={duration:.3f}s",
            )
            return result

        except PsycopgIntegrityError as e:
            self.log(logging.ERROR, f"Integrity error in batch: {str(e)}")
            raise IntegrityError(str(e)) from None
        except PsycopgError as e:
            self.log(logging.ERROR, f"PostgreSQL error in batch: {str(e)}")
            raise DatabaseError(str(e)) from None
        except Exception as e:
            self.log(logging.ERROR, f"Unexpected error during batch execution: {str(e)}")
            raise QueryError(str(e)) from None
        finally:
            if cursor:
                await cursor.close()

    async def get_server_version(self) -> Optional[Tuple[int, int, int]]:
        """Get PostgreSQL server version asynchronously.

        Returns:
        Tuple of (major, minor, patch) version numbers, or None if version
        cannot be determined.
        """
        if not self._connection:
            await self.connect()

        cursor = None
        try:
            cursor = await self._get_cursor()
            await cursor.execute("SELECT version()")
            version_row = await cursor.fetchone()
            if not version_row or not version_row[0]:
                self.log(logging.WARNING, "PostgreSQL version query returned no result")
                return None

            version_str = version_row[0]

            # Extract version from string like "PostgreSQL 13.2..."
            import re

            match = re.search(r"PostgreSQL (\d+)\.(\d+)(?:\.(\d+))?", version_str)
            if match:
                major = int(match.group(1))
                minor = int(match.group(2))
                patch = int(match.group(3)) if match.group(3) else 0
                return (major, minor, patch)
            else:
                self.log(logging.WARNING, f"Could not parse PostgreSQL version: {version_str}")
                return None
        except Exception as e:
            self.log(logging.WARNING, f"Could not determine PostgreSQL version: {str(e)}")
            return None
        finally:
            if cursor:
                await cursor.close()

    async def introspect_and_adapt(self) -> None:
        """Introspect backend and adapt backend instance to actual server capabilities.

        This method ensures a connection exists, queries the actual PostgreSQL server version
        and installed extensions, and updates the backend's internal state.

        Introspection includes:
        1. Server version: affects version-dependent feature support checks
        2. Installed extensions: queries pg_extension system table to determine plugin availability

        After detection, the dialect instance caches extension information,
        which can be queried via is_extension_installed().

        Note: If version cannot be determined, the default version (9.6.0) is retained.
        """
        # Ensure connection exists
        if not self._connection:
            await self.connect()

        # Get server version
        actual_version = await self.get_server_version()

        # If version cannot be determined, keep the default version
        if actual_version is None:
            self.log(logging.WARNING, "Could not determine server version, retaining default (9.6.0)")
            actual_version = (9, 6, 0)

        cached_version = getattr(self, "_server_version_cache", None)
        version_changed = cached_version != actual_version

        if version_changed:
            self._server_version_cache = actual_version
            self._dialect = PostgresDialect(actual_version)

        # Detect installed extensions
        extensions = await self._detect_extensions()
        self._dialect._extensions = extensions

        # Log detected extensions
        installed_exts = [f"{k}={v.version}" for k, v in extensions.items() if v.installed]
        ext_info = ", ".join(installed_exts) if installed_exts else "none"

        if version_changed:
            self.log(logging.INFO, f"Adapted to PostgreSQL {actual_version}, extensions: {ext_info}")
        else:
            self.log(logging.DEBUG, f"Extensions detected: {ext_info}")

    async def _detect_extensions(self) -> Dict[str, PostgresExtensionInfo]:
        """Detect installed and available extensions."""
        async with self._connection.cursor() as cursor:
            await cursor.execute(PostgresDialect.SQL_INSTALLED_EXTENSIONS)
            installed_rows = await cursor.fetchall()

            await cursor.execute(PostgresDialect.SQL_AVAILABLE_EXTENSIONS)
            available_rows = await cursor.fetchall()

            return PostgresDialect.build_extension_map(installed_rows, available_rows)

    async def ping(self, reconnect: bool = True) -> bool:
        """
        Ping the PostgreSQL server to check if the async connection is alive.

        Args:
            reconnect: If True, attempt to reconnect if the connection is dead.
                      If False, just return the current connection status.

        Returns:
            True if the connection is alive (or was successfully reconnected),
            False if the connection is dead and reconnect is False or reconnection failed.
        """
        try:
            if not self._connection:
                if reconnect:
                    await self.connect()
                    return True
                else:
                    return False

            # Check connection status without triggering auto-reconnect
            if self._connection.closed or self._connection.broken:
                if reconnect:
                    self._connection = None
                    await self.connect()
                    return True
                else:
                    return False

            # Execute a simple query to verify connection is actually working
            cursor = await self._get_cursor()
            await cursor.execute("SELECT 1")
            await cursor.fetchone()
            await cursor.close()

            return True
        except psycopg.Error as e:
            self.log(logging.WARNING, f"PostgreSQL async connection ping failed: {str(e)}")
            if reconnect:
                try:
                    self._connection = None
                    await self.connect()
                    return True
                except Exception as connect_error:
                    self.log(logging.ERROR, f"Failed to reconnect after ping failure: {str(connect_error)}")
                    return False
            return False

    async def _reconnect(self) -> bool:
        """
        Attempt to reconnect to the PostgreSQL server.

        Returns:
            True if reconnection was successful, False otherwise
        """
        try:
            self._connection = None
            await self.connect()
            return True
        except Exception as e:
            self.log(logging.ERROR, f"Failed to reconnect: {str(e)}")
            return False

    async def execute(
        self,
        sql: str,
        params: Optional[Tuple] = None,
        *,
        options=None,
        max_retries: int = 2,
        **kwargs,
    ) -> QueryResult:
        """
        Execute a SQL statement with optional parameters asynchronously.

        This method wraps the base execute method with automatic reconnection
        logic for connection errors (Plan B: Error Retry Mechanism).

        Args:
            sql: SQL statement to execute
            params: Optional parameters for the SQL statement
            options: Execution options
            max_retries: Maximum number of retry attempts for connection errors
            **kwargs: Additional keyword arguments

        Returns:
            QueryResult containing the query results
        """
        from rhosocial.activerecord.backend.options import ExecutionOptions, StatementType

        # If no options provided, create default options from kwargs
        if options is None:
            # Determine statement type based on SQL
            sql_upper = sql.strip().upper()
            if sql_upper.startswith(("SELECT", "WITH", "SHOW", "DESCRIBE", "EXPLAIN")):
                stmt_type = StatementType.DQL
            elif sql_upper.startswith(("INSERT", "UPDATE", "DELETE", "UPSERT")):
                stmt_type = StatementType.DML
            else:
                stmt_type = StatementType.DDL

            # Extract column_mapping and column_adapters from kwargs if present
            column_mapping = kwargs.get("column_mapping")
            column_adapters = kwargs.get("column_adapters")

            options = ExecutionOptions(
                stmt_type=stmt_type,
                process_result_set=None,
                column_adapters=column_adapters,
                column_mapping=column_mapping,
            )
        else:
            # If options is provided but column_mapping or column_adapters are explicitly passed in kwargs,
            # update the options with these values
            if "column_mapping" in kwargs:
                options.column_mapping = kwargs["column_mapping"]
            if "column_adapters" in kwargs:
                options.column_adapters = kwargs["column_adapters"]

        # Retry loop for connection errors (Plan B)
        # We catch the framework's OperationalError because the original PsycopgOperationalError
        # has already been converted by _handle_error() in the base class.
        for attempt in range(max_retries + 1):
            try:
                return await super().execute(sql, params, options=options)
            except OperationalError as e:
                error_msg = str(e)
                if self._is_connection_error_message(error_msg) and attempt < max_retries:
                    self.log(logging.WARNING,
                             f"Connection error on attempt {attempt + 1}, retrying... Error: {error_msg}")
                    if await self._reconnect():
                        continue
                    # If reconnection fails, break and raise the error
                    self.log(logging.ERROR, "Failed to reconnect after connection error")
                    break
                raise

    async def _handle_error(self, error: Exception) -> None:
        """Handle PostgreSQL-specific errors asynchronously."""
        error_msg = str(error)

        # Try to rollback transaction to recover from aborted state
        # PostgreSQL requires explicit ROLLBACK after errors in a transaction
        await self._try_rollback_transaction()

        if isinstance(error, PsycopgIntegrityError):
            if "duplicate key value violates unique constraint" in error_msg.lower():
                self.log(logging.ERROR, f"Unique constraint violation: {error_msg}")
                raise IntegrityError(f"Unique constraint violation: {error_msg}")
            elif "violates foreign key constraint" in error_msg.lower():
                self.log(logging.ERROR, f"Foreign key constraint violation: {error_msg}")
                raise IntegrityError(f"Foreign key constraint violation: {error_msg}")
            self.log(logging.ERROR, f"Integrity error: {error_msg}")
            raise IntegrityError(error_msg)
        elif isinstance(error, PsycopgOperationalError):
            if "deadlock detected" in error_msg.lower():
                self.log(logging.ERROR, f"Deadlock error: {error_msg}")
                raise DeadlockError(error_msg)
            self.log(logging.ERROR, f"Operational error: {error_msg}")
            raise OperationalError(error_msg)
        elif isinstance(error, PsycopgProgrammingError):
            self.log(logging.ERROR, f"Programming error: {error_msg}")
            raise QueryError(error_msg)
        elif isinstance(error, PsycopgDeadlockError):
            self.log(logging.ERROR, f"Deadlock error: {error_msg}")
            raise DeadlockError(error_msg)
        elif isinstance(error, PsycopgError):
            self.log(logging.ERROR, f"PostgreSQL error: {error_msg}")
            raise DatabaseError(error_msg)
        else:
            self.log(logging.ERROR, f"Unexpected error: {error_msg}")
            raise error

    async def _try_rollback_transaction(self) -> None:
        """Attempt to rollback transaction to recover from aborted state asynchronously.

        PostgreSQL requires explicit ROLLBACK after query errors in a transaction.
        This method attempts to recover by rolling back the transaction, regardless
        of the transaction manager's state.
        """
        try:
            # First, try the transaction manager if active
            if self.in_transaction:
                self.log(logging.INFO, "Attempting to rollback transaction after error")
                await self._transaction_manager.rollback()

            # Also try direct connection rollback to handle edge cases
            # This ensures we recover from aborted transaction state
            if self._connection and not self._connection.closed:
                try:
                    await self._connection.rollback()
                    self.log(logging.INFO, "Direct connection rollback completed")
                except Exception as direct_error:
                    self.log(logging.DEBUG, f"Direct rollback not needed or failed: {direct_error}")
        except Exception as rollback_error:
            self.log(logging.WARNING, f"Failed to rollback transaction: {rollback_error}")

    async def _handle_auto_commit(self) -> None:
        """Handle auto commit based on PostgreSQL connection and transaction state asynchronously.

        This method will commit the current connection if:
        1. The connection exists and is open
        2. There is no active transaction managed by transaction_manager

        It's used by insert/update/delete operations to ensure changes are
        persisted immediately when auto_commit=True is specified.
        """
        try:
            # Check if connection exists
            if not self._connection:
                return

            # Check if we're not in an active transaction
            if not self.in_transaction:
                # For PostgreSQL, if autocommit is disabled, we need to commit explicitly
                if not getattr(self.config, "autocommit", False):
                    await self._connection.commit()
                    self.log(logging.DEBUG, "Auto-committed operation (not in active transaction)")
        except Exception as e:
            # Just log the error but don't raise - this is a convenience feature
            self.log(logging.WARNING, f"Failed to auto-commit: {str(e)}")

    async def _handle_auto_commit_if_needed(self) -> None:
        """
        Handle auto-commit for PostgreSQL asynchronously.

        PostgreSQL respects the autocommit setting, but we also need to handle explicit commits.
        """
        if not self.in_transaction and self._connection:
            if not getattr(self.config, "autocommit", False):
                await self._connection.commit()
                self.log(logging.DEBUG, "Auto-committed operation (not in active transaction)")

    # region Advisory Lock Methods

    async def execute_advisory_lock(
        self,
        key,
        shared: bool = False,
        session: bool = True
    ) -> None:
        """
        Acquire an advisory lock asynchronously.

        This method blocks until the lock is acquired.

        Args:
            key: Lock key - either a single 64-bit integer or tuple of two 32-bit integers
            shared: If True, acquire shared lock; otherwise exclusive lock
            session: If True (default), session-level lock; otherwise transaction-level

        Raises:
            DatabaseError: If the lock operation fails
        """
        from rhosocial.activerecord.backend.impl.postgres.expression.advisory import PostgresAdvisoryLockExpression

        expr = PostgresAdvisoryLockExpression(self.dialect, key=key, shared=shared, session=session)
        sql, params = expr.to_sql()
        await self.execute(sql, params)

    async def execute_advisory_unlock(self, key, shared: bool = False) -> bool:
        """
        Release an advisory lock asynchronously.

        Args:
            key: Lock key - either a single 64-bit integer or tuple of two 32-bit integers
            shared: If True, release shared lock; otherwise exclusive lock

        Returns:
            True if the lock was released, False if it was not held
        """
        from rhosocial.activerecord.backend.impl.postgres.expression.advisory import PostgresAdvisoryUnlockExpression

        expr = PostgresAdvisoryUnlockExpression(self.dialect, key=key, shared=shared)
        sql, params = expr.to_sql()
        result = await self.execute(sql, params)
        # pg_advisory_unlock returns boolean in first column
        if result.data and len(result.data) > 0:
            row = result.data[0]
            # Result is a dict with column name as key
            return bool(list(row.values())[0])
        return False

    async def execute_advisory_unlock_all(self) -> None:
        """
        Release all advisory locks held by the current session asynchronously.
        """
        from rhosocial.activerecord.backend.impl.postgres.expression.advisory import PostgresAdvisoryUnlockAllExpression

        expr = PostgresAdvisoryUnlockAllExpression(self.dialect)
        sql, params = expr.to_sql()
        await self.execute(sql, params)

    async def try_advisory_lock(
        self,
        key,
        shared: bool = False,
        session: bool = True
    ) -> bool:
        """
        Non-blocking advisory lock acquisition asynchronously.

        Args:
            key: Lock key - either a single 64-bit integer or tuple of two 32-bit integers
            shared: If True, acquire shared lock; otherwise exclusive lock
            session: If True (default), session-level lock; otherwise transaction-level

        Returns:
            True if the lock was acquired, False if it was not available
        """
        from rhosocial.activerecord.backend.impl.postgres.expression.advisory import PostgresTryAdvisoryLockExpression

        expr = PostgresTryAdvisoryLockExpression(self.dialect, key=key, shared=shared, session=session)
        sql, params = expr.to_sql()
        result = await self.execute(sql, params)
        # pg_try_advisory_lock returns boolean in first column
        if result.data and len(result.data) > 0:
            row = result.data[0]
            # Result is a dict with column name as key
            return bool(list(row.values())[0])
        return False

    def advisory_lock(
        self,
        key,
        shared: bool = False,
        session: bool = True
    ):
        """
        Async context manager for advisory locks.

        Automatically acquires the lock on entry and releases it on exit.

        Args:
            key: Lock key - either a single 64-bit integer or tuple of two 32-bit integers
            shared: If True, acquire shared lock; otherwise exclusive lock
            session: If True (default), session-level lock; otherwise transaction-level

        Returns:
            Async context manager that holds the lock

        Example:
            async with backend.advisory_lock(key=12345):
                # Critical section - lock is held
                await process_data()
            # Lock automatically released

            # Shared lock for read-only access
            async with backend.advisory_lock(key=12345, shared=True):
                await read_data()
        """
        from contextlib import asynccontextmanager

        @asynccontextmanager
        async def _advisory_lock_context():
            await self.execute_advisory_lock(key=key, shared=shared, session=session)
            try:
                yield
            finally:
                # Only release session-level locks; transaction-level are auto-released
                if session:
                    await self.execute_advisory_unlock(key=key)

        return _advisory_lock_context()

    # endregion


__all__ = ["AsyncPostgresBackend"]
