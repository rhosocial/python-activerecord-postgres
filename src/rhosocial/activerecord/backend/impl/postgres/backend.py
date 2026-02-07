# src/rhosocial/activerecord/backend/impl/postgres/backend.py
"""
PostgreSQL backend implementation with sync and async support.

This module provides both synchronous and asynchronous PostgreSQL implementations:
- PostgreSQL synchronous backend with connection management and query execution
- PostgreSQL asynchronous backend for async/await workflows
- PostgreSQL-specific connection configuration
- Type mapping and value conversion
- Transaction management with savepoint support (sync and async)
- PostgreSQL dialect and expression handling
- PostgreSQL-specific type definitions and mappings

Architecture:
- PostgreSQLBackend: Synchronous implementation using psycopg
- AsyncPostgreSQLBackend: Asynchronous implementation using psycopg
- Both share common logic through PostgreSQLBackendMixin
- Independent from ORM frameworks - uses only native drivers
"""
import datetime
import logging
import re
from typing import Optional, Tuple, List, Any, Dict, Union, Type

# Import psycopg for both sync and async
import psycopg
from psycopg import Connection
from psycopg.errors import Error as PsycopgError
from psycopg.errors import (
    IntegrityError as PsycopgIntegrityError,
    OperationalError as PsycopgOperationalError,
    ProgrammingError as PsycopgProgrammingError,
    DeadlockDetected as PsycopgDeadlockError
)
from psycopg.types.json import Jsonb

from rhosocial.activerecord.backend.base import StorageBackend
from rhosocial.activerecord.backend.errors import (
    ConnectionError,
    DatabaseError,
    DeadlockError,
    IntegrityError,
    OperationalError,
    QueryError,
)
from rhosocial.activerecord.backend.result import QueryResult
from rhosocial.activerecord.backend.config import ConnectionConfig
from .adapters import (
    PostgresJSONBAdapter,
    PostgresNetworkAddressAdapter,
)
from .config import PostgresConnectionConfig
from .dialect import PostgresDialect
from .transaction import PostgresTransactionManager


class PostgresBackend(StorageBackend):
    """PostgreSQL-specific backend implementation."""

    def __init__(self, **kwargs):
        """Initialize PostgreSQL backend with connection configuration."""
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

        super().__init__(**kwargs)
        
        # Initialize PostgreSQL-specific components
        self._dialect = PostgresDialect(self.get_server_version())
        self._transaction_manager = PostgresTransactionManager(self)

        # Register PostgreSQL-specific type adapters
        self._register_postgres_adapters()

        self.log(logging.INFO, "PostgreSQLBackend initialized")

    def _register_postgres_adapters(self):
        """Register PostgreSQL-specific type adapters."""
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
        """Get the PostgreSQL transaction manager."""
        return self._transaction_manager

    def connect(self):
        """Establish connection to PostgreSQL database."""
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
                'service', 'target_session_attrs',
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
                # Convert options dict to string format if needed, or use as-is if it's already a string
                options_value = self.config.options
                if isinstance(options_value, dict):
                    # Convert dict to connection string format
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

            self._connection = psycopg.connect(**conn_params)
            
            self.log(logging.INFO, f"Connected to PostgreSQL database: {self.config.host}:{self.config.port}/{self.config.database}")
        except PsycopgError as e:
            self.log(logging.ERROR, f"Failed to connect to PostgreSQL database: {str(e)}")
            raise ConnectionError(f"Failed to connect to PostgreSQL: {str(e)}")

    def disconnect(self):
        """Close connection to PostgreSQL database."""
        if self._connection:
            try:
                # Rollback any active transaction
                if self.transaction_manager.is_active:
                    self.transaction_manager.rollback()
                
                self._connection.close()
                self._connection = None
                self.log(logging.INFO, "Disconnected from PostgreSQL database")
            except PsycopgError as e:
                self.log(logging.ERROR, f"Error during disconnection: {str(e)}")
                raise OperationalError(f"Error during PostgreSQL disconnection: {str(e)}")

    def _get_cursor(self):
        """Get a database cursor, ensuring connection is active."""
        if not self._connection:
            self.connect()
        
        return self._connection.cursor()


    def execute_many(self, sql: str, params_list: List[Tuple]) -> QueryResult:
        """Execute the same SQL statement multiple times with different parameters."""
        if not self._connection:
            self.connect()
        
        cursor = None
        start_time = datetime.datetime.now()
        
        try:
            cursor = self._get_cursor()
            
            # Log the batch operation if logging is enabled
            if getattr(self.config, 'log_queries', False):
                self.log(logging.DEBUG, f"Executing batch operation: {sql}")
                self.log(logging.DEBUG, f"With {len(params_list)} parameter sets")
            
            # Execute multiple statements
            affected_rows = 0
            for params in params_list:
                cursor.execute(sql, params)
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
                cursor.close()

    def get_server_version(self) -> tuple:
        """Get PostgreSQL server version."""
        if not self._connection:
            self.connect()
        
        cursor = None
        try:
            cursor = self._get_cursor()
            cursor.execute("SELECT version()")
            version_str = cursor.fetchone()[0]
            
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
                cursor.close()

    def requires_manual_commit(self) -> bool:
        """Check if manual commit is required for this database."""
        return not getattr(self.config, 'autocommit', False)

    def execute(self, sql: str, params: Optional[Tuple] = None, *, options=None, **kwargs) -> QueryResult:
        """Execute a SQL statement with optional parameters."""
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
        
        return super().execute(sql, params, options=options)

    def create_expression(self, expression_str: str):
        """Create an expression object for raw SQL expressions."""
        from rhosocial.activerecord.backend.expression.operators import RawSQLExpression
        return RawSQLExpression(self.dialect, expression_str)

    def ping(self, reconnect: bool = True) -> bool:
        """Ping the PostgreSQL server to check if the connection is alive."""
        try:
            if not self._connection:
                if reconnect:
                    self.connect()
                    return True
                else:
                    return False

            # Execute a simple query to check connection
            cursor = self._get_cursor()
            cursor.execute("SELECT 1")
            cursor.fetchone()
            cursor.close()

            return True
        except psycopg.Error as e:
            self.log(logging.WARNING, f"PostgreSQL connection ping failed: {str(e)}")
            if reconnect:
                try:
                    self.disconnect()
                    self.connect()
                    return True
                except Exception as connect_error:
                    self.log(logging.ERROR, f"Failed to reconnect after ping failure: {str(connect_error)}")
                    return False
            return False

    def _handle_error(self, error: Exception) -> None:
        """Handle PostgreSQL-specific errors."""
        if isinstance(error, PsycopgIntegrityError):
            self.log(logging.ERROR, f"Integrity error: {str(error)}")
            raise IntegrityError(str(error)) from error
        elif isinstance(error, PsycopgOperationalError):
            self.log(logging.ERROR, f"Operational error: {str(error)}")
            raise OperationalError(str(error)) from error
        elif isinstance(error, PsycopgProgrammingError):
            self.log(logging.ERROR, f"Programming error: {str(error)}")
            raise QueryError(str(error)) from error
        elif isinstance(error, PsycopgDeadlockError):
            self.log(logging.ERROR, f"Deadlock error: {str(error)}")
            raise DeadlockError(str(error)) from error
        elif isinstance(error, PsycopgError):
            self.log(logging.ERROR, f"PostgreSQL error: {str(error)}")
            raise DatabaseError(str(error)) from error
        else:
            self.log(logging.ERROR, f"Unexpected error: {str(error)}")
            raise error

    def log(self, level: int, message: str):
        """Log a message with the specified level."""
        if hasattr(self, '_logger') and self._logger:
            self._logger.log(level, message)
        else:
            # Fallback logging
            print(f"[{logging.getLevelName(level)}] {message}")