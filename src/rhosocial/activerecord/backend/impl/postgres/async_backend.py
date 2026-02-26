# src/rhosocial/activerecord/backend/impl/postgres/async_backend.py
"""
Asynchronous PostgreSQL backend implementation using psycopg's async functionality.

This module provides an async implementation for interacting with PostgreSQL databases,
handling connections, queries, transactions, and type adaptations tailored for PostgreSQL's
specific behaviors and SQL dialect. The async backend mirrors the functionality of
the synchronous backend but uses async/await for I/O operations.
"""
import datetime
import logging
from typing import Dict, List, Optional, Tuple, Type, Union

import psycopg
import psycopg.pq
from psycopg import AsyncConnection
from psycopg.errors import Error as PsycopgError
from psycopg.errors import (
    IntegrityError as PsycopgIntegrityError,
    OperationalError as PsycopgOperationalError,
    ProgrammingError as PsycopgProgrammingError,
    DeadlockDetected as PsycopgDeadlockError
)
from psycopg.types.json import Jsonb

from rhosocial.activerecord.backend.base import AsyncStorageBackend
from rhosocial.activerecord.backend.errors import (
    ConnectionError,
    DatabaseError,
    DeadlockError,
    IntegrityError,
    OperationalError,
    QueryError,
)
from rhosocial.activerecord.backend.result import QueryResult
from .config import PostgresConnectionConfig
from .dialect import PostgresDialect
from .transaction import AsyncPostgresTransactionManager


class AsyncPostgresBackend(AsyncStorageBackend):
    """Asynchronous PostgreSQL-specific backend implementation."""

    def __init__(self, **kwargs):
        """Initialize async PostgreSQL backend with connection configuration."""
        # Ensure we have proper PostgreSQL configuration
        connection_config = kwargs.get('connection_config')

        if connection_config is None:
            # Extract PostgreSQL-specific parameters from kwargs
            config_params = {}
            pg_specific_params = [
                'host', 'port', 'database', 'username', 'password',
                'charset', 'collation', 'timezone', 'version',
                'pool_size', 'pool_timeout', 'pool_name', 'pool_reset_session', 'pool_pre_ping',
                'ssl_ca', 'ssl_cert', 'ssl_key', 'ssl_verify_cert', 'ssl_verify_identity',
                'log_queries', 'log_level',
                'application_name', 'fallback_application_name', 'connect_timeout',
                'options', 'service', 'target_session_attrs',
                'gssencmode', 'channel_binding', 'replication', 'assume_role',
                'role', 'search_path', 'row_security', 'datestyle', 'intervalstyle',
                'timezone_setting', 'extra_float_digits', 'client_encoding',
                'tcp_user_timeout', 'tcp_keepalives_idle', 'tcp_keepalives_interval',
                'tcp_keepalives_count', 'load_balance_hosts', 'replication',
                'keepalives', 'keepalives_idle', 'keepalives_interval', 'keepalives_count'
            ]

            for param in pg_specific_params:
                if param in kwargs:
                    config_params[param] = kwargs[param]

            # Set defaults if not provided
            if 'host' not in config_params:
                config_params['host'] = 'localhost'
            if 'port' not in config_params:
                config_params['port'] = 5432

            kwargs['connection_config'] = PostgresConnectionConfig(**config_params)

        # Initialize the base class first to ensure all base properties are set
        super().__init__(**kwargs)

        # Initialize PostgreSQL-specific components
        # Note: We delay initializing dialect until connect() is called to avoid async issues during construction
        self._dialect = None  # Will be initialized in connect method
        
        # Register PostgreSQL-specific type adapters (same as sync backend)
        self._register_postgres_adapters()

        # Initialize transaction manager with connection (will be set when connected)
        # Pass None for connection initially, it will be updated later
        # Use the logger from the base class
        self._transaction_manager = AsyncPostgresTransactionManager(None, self.logger)

        self.log(logging.INFO, "AsyncPostgreSQLBackend initialized")

    def _register_postgres_adapters(self):
        """Register PostgreSQL-specific type adapters."""
        from .adapters import (
            PostgresJSONBAdapter,
            PostgresNetworkAddressAdapter,
        )
        
        pg_adapters = [
            PostgresJSONBAdapter(),
            PostgresNetworkAddressAdapter(),
        ]
        
        for adapter in pg_adapters:
            for py_type, db_types in adapter.supported_types.items():
                for db_type in db_types:
                    self.adapter_registry.register(adapter, py_type, db_type)
        
        self.log(logging.DEBUG, "Registered PostgreSQL-specific type adapters")

    @property
    def dialect(self):
        """Get the PostgreSQL dialect instance."""
        return self._dialect

    @property
    def transaction_manager(self):
        """Get the async PostgreSQL transaction manager."""
        # Update the transaction manager's connection if needed
        if self._transaction_manager:
            self._transaction_manager._connection = self._connection
        return self._transaction_manager

    async def connect(self):
        """Establish async connection to PostgreSQL database."""
        try:
            # Prepare connection parameters from config
            conn_params = {
                'host': self.config.host,
                'port': self.config.port,
                'dbname': self.config.database,
                'user': self.config.username,
                'password': self.config.password,
            }

            # Add additional parameters if they exist in config
            additional_params = [
                'application_name', 'fallback_application_name', 'connect_timeout',
                'options', 'service', 'target_session_attrs',
                'gssencmode', 'channel_binding', 'replication', 'assume_role',
                'role', 'search_path', 'row_security', 'datestyle', 'intervalstyle',
                'timezone', 'extra_float_digits', 'client_encoding',
                'tcp_user_timeout', 'tcp_keepalives_idle', 'tcp_keepalives_interval',
                'tcp_keepalives_count', 'load_balance_hosts',
                'keepalives', 'keepalives_idle', 'keepalives_interval', 'keepalives_count'
            ]

            for param in additional_params:
                if hasattr(self.config, param):
                    value = getattr(self.config, param)
                    if value is not None:  # Only add the parameter if it's not None
                        conn_params[param] = value
            
            # Handle 'options' parameter specially as it should be a string, not a dict
            if hasattr(self.config, 'options') and self.config.options is not None:
                options_value = self.config.options
                if isinstance(options_value, dict):
                    options_str = ' '.join([f"-c {k}={v}" for k, v in options_value.items()])
                    conn_params['options'] = options_str
                else:
                    conn_params['options'] = options_value

            # Add SSL parameters if provided
            ssl_params = {}
            if hasattr(self.config, 'ssl_ca'):
                ssl_params['sslcert'] = self.config.ssl_ca
            if hasattr(self.config, 'ssl_cert'):
                ssl_params['sslcert'] = self.config.ssl_cert
            if hasattr(self.config, 'ssl_key'):
                ssl_params['sslkey'] = self.config.ssl_key
            if hasattr(self.config, 'ssl_mode'):
                ssl_params['sslmode'] = self.config.ssl_mode

            if ssl_params:
                conn_params.update(ssl_params)

            self._connection = await AsyncConnection.connect(**conn_params)

            # Initialize dialect after connection is established
            if self._dialect is None:
                self._dialect = PostgresDialect(await self.get_server_version())

            self.log(logging.INFO, f"Connected to PostgreSQL database: {self.config.host}:{self.config.port}/{self.config.database}")
        except PsycopgError as e:
            self.log(logging.ERROR, f"Failed to connect to PostgreSQL database: {str(e)}")
            raise ConnectionError(f"Failed to connect to PostgreSQL: {str(e)}")

    async def disconnect(self):
        """Close async connection to PostgreSQL database."""
        if self._connection:
            try:
                # Rollback any active transaction
                if self.transaction_manager.is_active:
                    await self.transaction_manager.rollback()
                
                await self._connection.close()
                self._connection = None
                self.log(logging.INFO, "Disconnected from PostgreSQL database")
            except PsycopgError as e:
                self.log(logging.ERROR, f"Error during disconnection: {str(e)}")
                raise OperationalError(f"Error during PostgreSQL disconnection: {str(e)}")

    async def _get_cursor(self):
        """Get a database cursor, ensuring connection is active."""
        if not self._connection:
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
            if getattr(self.config, 'log_queries', False):
                self.log(logging.DEBUG, f"Executing batch operation: {sql}")
                self.log(logging.DEBUG, f"With {len(params_list)} parameter sets")
            
            # Execute multiple statements
            affected_rows = 0
            for params in params_list:
                await cursor.execute(sql, params)
                affected_rows += cursor.rowcount
            
            duration = (datetime.datetime.now() - start_time).total_seconds()
            
            result = QueryResult(
                affected_rows=affected_rows,
                data=None,
                duration=duration
            )
            
            self.log(logging.INFO, f"Batch operation completed, affected {affected_rows} rows, duration={duration:.3f}s")
            return result
            
        except PsycopgIntegrityError as e:
            self.log(logging.ERROR, f"Integrity error in batch: {str(e)}")
            raise IntegrityError(str(e))
        except PsycopgError as e:
            self.log(logging.ERROR, f"PostgreSQL error in batch: {str(e)}")
            raise DatabaseError(str(e))
        except Exception as e:
            self.log(logging.ERROR, f"Unexpected error during batch execution: {str(e)}")
            raise QueryError(str(e))
        finally:
            if cursor:
                await cursor.close()

    async def get_server_version(self) -> tuple:
        """Get PostgreSQL server version asynchronously."""
        if not self._connection:
            await self.connect()
        
        cursor = None
        try:
            cursor = await self._get_cursor()
            await cursor.execute("SELECT version()")
            version_row = await cursor.fetchone()
            version_str = version_row[0] if version_row else "13.0.0"
            
            # Extract version from string like "PostgreSQL 13.2..."
            import re
            match = re.search(r'PostgreSQL (\d+)\.(\d+)(?:\.(\d+))?', version_str)
            if match:
                major = int(match.group(1))
                minor = int(match.group(2))
                patch = int(match.group(3)) if match.group(3) else 0
                return (major, minor, patch)
            else:
                # Default to a recent version if parsing fails
                self.log(logging.WARNING, f"Could not parse PostgreSQL version: {version_str}, defaulting to 13.0.0")
                return (13, 0, 0)
        except Exception as e:
            self.log(logging.WARNING, f"Could not determine PostgreSQL version: {str(e)}, defaulting to 13.0.0")
            return (13, 0, 0)  # Default to a recent version
        finally:
            if cursor:
                await cursor.close()

    def requires_manual_commit(self) -> bool:
        """Check if manual commit is required for this database."""
        return not getattr(self.config, 'autocommit', False)

    async def execute(self, sql: str, params: Optional[Tuple] = None, *, options=None, **kwargs) -> QueryResult:
        """Execute a SQL statement with optional parameters asynchronously."""
        from rhosocial.activerecord.backend.options import ExecutionOptions, StatementType
        
        # If no options provided, create default options from kwargs
        if options is None:
            # Determine statement type based on SQL
            sql_upper = sql.strip().upper()
            if sql_upper.startswith(('SELECT', 'WITH', 'SHOW', 'DESCRIBE', 'EXPLAIN')):
                stmt_type = StatementType.DQL
            elif sql_upper.startswith(('INSERT', 'UPDATE', 'DELETE', 'UPSERT')):
                stmt_type = StatementType.DML
            else:
                stmt_type = StatementType.DDL
                
            # Extract column_mapping and column_adapters from kwargs if present
            column_mapping = kwargs.get('column_mapping')
            column_adapters = kwargs.get('column_adapters')
            
            options = ExecutionOptions(
                stmt_type=stmt_type,
                process_result_set=None,  # Let the base logic determine this based on stmt_type
                column_adapters=column_adapters,
                column_mapping=column_mapping
            )
        else:
            # If options is provided but column_mapping or column_adapters are explicitly passed in kwargs,
            # update the options with these values
            if 'column_mapping' in kwargs:
                options.column_mapping = kwargs['column_mapping']
            if 'column_adapters' in kwargs:
                options.column_adapters = kwargs['column_adapters']
        
        return await super().execute(sql, params, options=options)

    def _prepare_sql_and_params(
        self,
        sql: str,
        params: Optional[Tuple]
    ) -> Tuple[str, Optional[Tuple]]:
        """
        Prepare SQL and parameters for PostgreSQL execution.

        Converts the generic '?' placeholder to PostgreSQL-compatible '%s' placeholder.
        """
        if params is None:
            return sql, None

        # Replace '?' placeholders with '%s' for PostgreSQL
        prepared_sql = sql.replace('?', '%s')
        return prepared_sql, params

    def create_expression(self, expression_str: str):
        """Create an expression object for raw SQL expressions."""
        from rhosocial.activerecord.backend.expression.operators import RawSQLExpression
        return RawSQLExpression(self.dialect, expression_str)

    async def ping(self, reconnect: bool = True) -> bool:
        """Ping the PostgreSQL server to check if the async connection is alive."""
        try:
            if not self._connection:
                if reconnect:
                    await self.connect()
                    return True
                else:
                    return False

            # Execute a simple query to check connection
            cursor = await self._get_cursor()
            await cursor.execute("SELECT 1")
            await cursor.fetchone()
            await cursor.close()

            return True
        except psycopg.Error as e:
            self.log(logging.WARNING, f"PostgreSQL async connection ping failed: {str(e)}")
            if reconnect:
                try:
                    await self.disconnect()
                    await self.connect()
                    return True
                except Exception as connect_error:
                    self.log(logging.ERROR, f"Failed to reconnect after ping failure: {str(connect_error)}")
                    return False
            return False

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
            if self._transaction_manager and self._transaction_manager.is_active:
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
            if not self._transaction_manager or not self._transaction_manager.is_active:
                # For PostgreSQL, if autocommit is disabled, we need to commit explicitly
                if not getattr(self.config, 'autocommit', False):
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
            if not getattr(self.config, 'autocommit', False):
                await self._connection.commit()
                self.log(logging.DEBUG, "Auto-committed operation (not in active transaction)")

    def get_default_adapter_suggestions(self) -> Dict[Type, Tuple['SQLTypeAdapter', Type]]:
        """
        [Backend Implementation] Provides default type adapter suggestions for PostgreSQL.

        This method defines a curated set of type adapter suggestions for common Python
        types, mapping them to their typical PostgreSQL-compatible representations as
        demonstrated in test fixtures. It explicitly retrieves necessary `SQLTypeAdapter`
        instances from the backend's `adapter_registry`. If an adapter for a specific
        (Python type, DB driver type) pair is not registered, no suggestion will be
        made for that Python type.

        Returns:
            Dict[Type, Tuple[SQLTypeAdapter, Type]]: A dictionary where keys are
            original Python types (`TypeRegistry`'s `py_type`), and values are
            tuples containing a `SQLTypeAdapter` instance and the target
            Python type (`TypeRegistry`'s `db_type`) expected by the driver.
        """
        suggestions: Dict[Type, Tuple['SQLTypeAdapter', Type]] = {}

        # Define a list of desired Python type to DB driver type mappings.
        # This list reflects types seen in test fixtures and common usage,
        # along with their preferred database-compatible Python types for the driver.
        # Types that are natively compatible with the DB driver (e.g., Python str, int, float)
        # and for which no specific conversion logic is needed are omitted from this list.
        # The consuming layer should assume pass-through behavior for any Python type
        # that does not have an explicit adapter suggestion.
        #
        # Exception: If a user requires specific processing for a natively compatible type
        # (e.g., custom serialization/deserialization for JSON strings beyond basic conversion),
        # they would need to implement and register their own specialized adapter.
        # This backend's default suggestions do not cater to such advanced processing needs.
        from datetime import date, datetime, time
        from decimal import Decimal
        from uuid import UUID
        from enum import Enum

        type_mappings = [
            (bool, bool),        # Python bool -> DB driver bool (PostgreSQL BOOLEAN)
            # Why str for date/time?
            # PostgreSQL has native DATE, TIME, TIMESTAMP types but accepts string representations
            (datetime, str),    # Python datetime -> DB driver str (PostgreSQL TIMESTAMP)
            (date, str),        # Python date -> DB driver str (PostgreSQL DATE)
            (time, str),        # Python time -> DB driver str (PostgreSQL TIME)
            (Decimal, Decimal),   # Python Decimal -> DB driver Decimal (PostgreSQL NUMERIC/DECIMAL)
            (UUID, str),        # Python UUID -> DB driver str (PostgreSQL UUID type)
            (dict, str),        # Python dict -> DB driver str (PostgreSQL JSON/JSONB)
            (list, list),      # Python list -> DB driver list (PostgreSQL arrays - psycopg handles natively)
            (Enum, str),        # Python Enum -> DB driver str (PostgreSQL TEXT)
        ]

        # Iterate through the defined mappings and retrieve adapters from the registry.
        for py_type, db_type in type_mappings:
            adapter = self.adapter_registry.get_adapter(py_type, db_type)
            if adapter:
                suggestions[py_type] = (adapter, db_type)
            else:
                # Log a debug message if a specific adapter is expected but not found.
                self.log(logging.DEBUG, f"No adapter found for ({py_type.__name__}, {db_type.__name__}). "
                                      "Suggestion will not be provided for this type.")

        return suggestions

    def log(self, level: int, message: str):
        """Log a message with the specified level."""
        if hasattr(self, '_logger') and self._logger:
            self._logger.log(level, message)
        else:
            # Fallback logging
            print(f"[{logging.getLevelName(level)}] {message}")