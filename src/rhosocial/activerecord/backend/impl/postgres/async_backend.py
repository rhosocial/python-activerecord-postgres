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
from .async_transaction import AsyncPostgresTransactionManager


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

        super().__init__(**kwargs)
        
        # Initialize PostgreSQL-specific components
        self._dialect = PostgresDialect(self.get_server_version())
        self._transaction_manager = AsyncPostgresTransactionManager(self)

        # Register PostgreSQL-specific type adapters (same as sync backend)
        self._register_postgres_adapters()

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
                    conn_params[param] = getattr(self.config, param)

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

    async def execute(self, sql: str, params: Optional[Tuple] = None) -> QueryResult:
        """Execute a SQL statement with optional parameters asynchronously."""
        if not self._connection:
            await self.connect()
        
        cursor = None
        start_time = datetime.datetime.now()
        
        try:
            cursor = await self._get_cursor()
            
            # Log the query if logging is enabled
            if getattr(self.config, 'log_queries', False):
                self.log(logging.DEBUG, f"Executing SQL: {sql}")
                if params:
                    self.log(logging.DEBUG, f"With params: {params}")
            
            # Execute the query
            if params:
                await cursor.execute(sql, params)
            else:
                await cursor.execute(sql)
            
            # Get results
            duration = (datetime.datetime.now() - start_time).total_seconds()
            
            # For SELECT queries, fetch results
            if sql.strip().upper().startswith(('SELECT', 'WITH', 'VALUES', 'TABLE', 'SHOW', 'EXPLAIN')):
                rows = await cursor.fetchall()
                columns = [desc[0] for desc in cursor.description] if cursor.description else []
                data = [dict(zip(columns, row)) for row in rows] if rows else []
            else:
                data = None
            
            result = QueryResult(
                affected_rows=cursor.rowcount,
                data=data,
                duration=duration
            )
            
            self.log(logging.INFO, f"Query executed successfully, affected {cursor.rowcount} rows, duration={duration:.3f}s")
            return result
            
        except PsycopgIntegrityError as e:
            self.log(logging.ERROR, f"Integrity error: {str(e)}")
            raise IntegrityError(str(e))
        except PsycopgDeadlockError as e:
            self.log(logging.ERROR, f"Deadlock error: {str(e)}")
            raise DeadlockError(str(e))
        except PsycopgError as e:
            self.log(logging.ERROR, f"PostgreSQL error: {str(e)}")
            raise DatabaseError(str(e))
        except Exception as e:
            self.log(logging.ERROR, f"Unexpected error during execution: {str(e)}")
            raise QueryError(str(e))
        finally:
            if cursor:
                await cursor.close()

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

    def log(self, level: int, message: str):
        """Log a message with the specified level."""
        if hasattr(self, '_logger') and self._logger:
            self._logger.log(level, message)
        else:
            # Fallback logging
            print(f"[{logging.getLevelName(level)}] {message}")