# src/rhosocial/activerecord/backend/impl/postgres/backend.py
"""postgres backend implementation with sync and async support."""

import logging
import re
from typing import Optional, Tuple, List, Any, Dict, Union, Type

# Import psycopg3 for both sync and async
import psycopg
from psycopg import Connection, AsyncConnection
from psycopg import Cursor, AsyncCursor
from psycopg.errors import Error as PsycopgError
from psycopg.errors import (
    IntegrityError as PsycopgIntegrityError,
    OperationalError as PsycopgOperationalError,
    ProgrammingError as PsycopgProgrammingError,
    DeadlockDetected as PsycopgDeadlockError
)
from psycopg.types.json import Jsonb

from .adapters import (
    PostgresJSONBAdapter,
    PostgresNetworkAddressAdapter,
)
from .config import PostgresConnectionConfig
from .dialect import PostgresDialect, PostgresSQLBuilder
from .transaction import PostgresTransactionManager, AsyncPostgresTransactionManager
from rhosocial.activerecord.backend.base import StorageBackend, AsyncStorageBackend
from rhosocial.activerecord.backend.capabilities import (
    DatabaseCapabilities,
    CTECapability,
    JSONCapability,
    TransactionCapability,
    BulkOperationCapability,
    JoinCapability,
    ConstraintCapability,
    AggregateFunctionCapability,
    DateTimeFunctionCapability,
    StringFunctionCapability,
    MathematicalFunctionCapability,
    WindowFunctionCapability,
    AdvancedGroupingCapability,
    ALL_SET_OPERATIONS,
    ALL_WINDOW_FUNCTIONS,
    ALL_RETURNING_FEATURES,
    ALL_CTE_FEATURES,
)
from rhosocial.activerecord.backend.errors import (
    ConnectionError,
    IntegrityError,
    OperationalError,
    QueryError,
    DeadlockError,
    DatabaseError,
)
from rhosocial.activerecord.backend import ReturningOptions, QueryResult
from rhosocial.activerecord.backend.type_adapter import SQLTypeAdapter


class PostgresBackendMixin:
    """Shared logic between sync and async postgres backends.

    This mixin contains non-I/O logic that can be shared between
    sync and async implementations:
    - Configuration validation
    - Version parsing
    - Capability initialization
    - Type converter registration
    - Error mapping
    """

    def get_default_adapter_suggestions(self) -> Dict[Type, Tuple['SQLTypeAdapter', Type]]:
        """
        [Backend Implementation] Provides default type adapter suggestions for PostgreSQL.

        This method defines a curated set of type adapter suggestions for common Python
        types, mapping them to their typical PostgreSQL-compatible representations.
        It retrieves `SQLTypeAdapter` instances from the backend's `adapter_registry`.
        If an adapter for a specific (Python type, DB driver type) pair is not
        registered, no suggestion is made for that Python type.

        Returns:
            Dict[Type, Tuple[SQLTypeAdapter, Type]]: A dictionary where keys are
            original Python types, and values are tuples containing a `SQLTypeAdapter`
            instance and the target type expected by the driver.
        """
        import uuid
        from psycopg.types.json import Jsonb

        suggestions: Dict[Type, Tuple['SQLTypeAdapter', Type]] = {}

        # psycopg handles many types natively. These suggestions are for types
        # requiring specific adapters, like JSONB or custom objects.
        type_mappings = [
            (dict, Jsonb),
        ]

        try:
            import ipaddress
            type_mappings.extend([
                (ipaddress.IPv4Address, str),
                (ipaddress.IPv6Address, str),
                (ipaddress.IPv4Network, str),
                (ipaddress.IPv6Network, str),
            ])
        except ImportError:
            self.logger.debug("ipaddress module not found, skipping network address adapter suggestions.")

        for py_type, db_driver_type in type_mappings:
            adapter = self.adapter_registry.get_adapter(py_type, db_driver_type)
            if adapter:
                suggestions[py_type] = (adapter, db_driver_type)
            else:
                self.logger.debug(f"No adapter found for ({py_type.__name__}, {db_driver_type.__name__}). "
                                  "Suggestion will not be provided for this type.")

        return suggestions


    def _ensure_postgres_config(self, kwargs: Dict) -> PostgresConnectionConfig:
        """Ensure configuration is PostgresConnectionConfig type.

        Args:
            kwargs: Configuration parameters

        Returns:
            PostgresConnectionConfig instance
        """
        if "connection_config" in kwargs and kwargs["connection_config"] is not None:
            config = kwargs["connection_config"]
            if not isinstance(config, PostgresConnectionConfig):
                # Convert legacy config to PostgresConnectionConfig
                config = PostgresConnectionConfig(
                    host=config.host,
                    port=config.port,
                    database=config.database,
                    username=config.username,
                    password=config.password,
                    driver_type=config.driver_type,
                    options=config.options,
                )
            return config
        else:
            return PostgresConnectionConfig(**kwargs)

    def _prepare_connection_params(self, config: PostgresConnectionConfig) -> Dict[str, Any]:
        """Prepare connection parameters for psycopg.

        Args:
            config: postgres connection configuration

        Returns:
            Dictionary of connection parameters
        """
        params = {
            "host": config.host,
            "port": config.port,
            "dbname": config.database,
            "user": config.username,
            "password": config.password,
        }

        # Add optional parameters from postgres-specific configuration
        if config.sslmode:
            params["sslmode"] = config.sslmode
        if config.sslcert:
            params["sslcert"] = config.sslcert
        if config.sslkey:
            params["sslkey"] = config.sslkey
        if config.sslrootcert:
            params["sslrootcert"] = config.sslrootcert
        if config.sslcrl:
            params["sslcrl"] = config.sslcrl
        if config.sslcompression is not None:
            params["sslcompression"] = config.sslcompression
        if config.application_name:
            params["application_name"] = config.application_name
        if config.target_session_attrs:
            params["target_session_attrs"] = config.target_session_attrs
        if config.connect_timeout:
            params["connect_timeout"] = config.connect_timeout
        if config.client_encoding:
            params["client_encoding"] = config.client_encoding
        if config.service:
            params["service"] = config.service
        if config.gssencmode:
            params["gssencmode"] = config.gssencmode
        if config.channel_binding:
            params["channel_binding"] = config.channel_binding

        # Pass through other options that are not in the predefined set
        if config.options:
            for key, value in config.options.items():
                if key not in params:  # Avoid duplicates
                    params[key] = value

        return params

    def _parse_server_version(self, version_string: str) -> Tuple[int, ...]:
        """Parse postgres version string to tuple.

        Args:
            version_string: Version string (e.g., "postgres 14.5")

        Returns:
            Version tuple (e.g., (14, 5, 0))
        """
        # Extract version numbers from string like "postgres 14.5 on x86_64..."
        match = re.search(r'(\d+)\.(\d+)(?:\.(\d+))?', version_string)
        if match:
            major = int(match.group(1))
            minor = int(match.group(2))
            patch = int(match.group(3)) if match.group(3) else 0
            return (major, minor, patch)
        return (0, 0, 0)

    def _initialize_capabilities(self) -> DatabaseCapabilities:
        """Initialize postgres capabilities based on version.

        postgres has excellent support for modern SQL features.
        Most features are available from version 9.6+.

        Returns:
            DatabaseCapabilities instance
        """
        capabilities = DatabaseCapabilities()
        version = self.get_server_version()

        # Basic set operations (all versions)
        capabilities.add_set_operation(ALL_SET_OPERATIONS)

        # Join operations (all versions)
        capabilities.add_join_operation([
            JoinCapability.INNER_JOIN,
            JoinCapability.LEFT_OUTER_JOIN,
            JoinCapability.RIGHT_OUTER_JOIN,
            JoinCapability.FULL_OUTER_JOIN,
            JoinCapability.CROSS_JOIN,
        ])

        # CTEs - supported from 8.4+ (basically all modern versions)
        if version >= (8, 4, 0):
            capabilities.add_cte(ALL_CTE_FEATURES)

        # Window functions - supported from 8.4+
        if version >= (8, 4, 0):
            capabilities.add_window_function(ALL_WINDOW_FUNCTIONS)

        # RETURNING clause - supported from 8.2+
        if version >= (8, 2, 0):
            capabilities.add_returning(ALL_RETURNING_FEATURES)

        # JSON operations - JSON from 9.2+, JSONB from 9.4+
        if version >= (9, 4, 0):
            # JSONB support (better performance)
            capabilities.add_json([
                JSONCapability.JSON_EXTRACT,
                JSONCapability.JSON_SET,
                JSONCapability.JSON_ARRAY,
                JSONCapability.JSON_OBJECT,
                JSONCapability.JSON_CONTAINS,
                JSONCapability.JSON_EXISTS,
            ])
        elif version >= (9, 2, 0):
            # Basic JSON support
            capabilities.add_json([
                JSONCapability.JSON_EXTRACT,
                JSONCapability.JSON_ARRAY,
                JSONCapability.JSON_OBJECT,
            ])

        # Advanced grouping - CUBE, ROLLUP, GROUPING SETS from 9.5+
        if version >= (9, 5, 0):
            capabilities.add_advanced_grouping([
                AdvancedGroupingCapability.CUBE,
                AdvancedGroupingCapability.ROLLUP,
                AdvancedGroupingCapability.GROUPING_SETS,
            ])

        # Transaction capabilities
        capabilities.add_transaction([
            TransactionCapability.SAVEPOINT,
            #TransactionCapability.NESTED_TRANSACTION,
            TransactionCapability.ISOLATION_LEVELS,
            #TransactionCapability.DEFERRABLE,
        ])

        # Bulk operations
        capabilities.add_bulk_operation([
            BulkOperationCapability.MULTI_ROW_INSERT,
            BulkOperationCapability.BATCH_OPERATIONS,
            # BulkOperationCapability.BATCH_DELETE,
            BulkOperationCapability.UPSERT,  # ON CONFLICT
        ])

        # Constraints
        capabilities.add_constraint([
            ConstraintCapability.PRIMARY_KEY,
            ConstraintCapability.FOREIGN_KEY,
            ConstraintCapability.UNIQUE,
            ConstraintCapability.CHECK,
            ConstraintCapability.NOT_NULL,
            ConstraintCapability.DEFAULT,
            ConstraintCapability.STRICT_TABLES,
            # ConstraintCapability.EXCLUSION,  # postgres specific
        ])

        # Aggregate functions
        capabilities.add_aggregate_function([
            # AggregateFunctionCapability.COUNT,
            # AggregateFunctionCapability.SUM,
            # AggregateFunctionCapability.AVG,
            # AggregateFunctionCapability.MIN,
            # AggregateFunctionCapability.MAX,
            AggregateFunctionCapability.GROUP_CONCAT,
            # AggregateFunctionCapability.DISTINCT_AGGREGATE,
        ])

        # String functions
        capabilities.add_string_function([
            StringFunctionCapability.CONCAT,
            StringFunctionCapability.SUBSTRING,
            StringFunctionCapability.UPPER,
            StringFunctionCapability.LOWER,
            # StringFunctionCapability.LENGTH,
            StringFunctionCapability.TRIM,
            # StringFunctionCapability.REPLACE,
            # StringFunctionCapability.REGEX,
        ])

        # Date/time functions
        capabilities.add_datetime_function([
            # DateTimeFunctionCapability.NOW,
            DateTimeFunctionCapability.DATE_ADD,
            DateTimeFunctionCapability.DATE_SUB,
            # DateTimeFunctionCapability.DATE_DIFF,
            # DateTimeFunctionCapability.DATE_FORMAT,
            DateTimeFunctionCapability.EXTRACT,
        ])

        # Mathematical functions
        capabilities.add_mathematical_function([
            MathematicalFunctionCapability.ABS,
            MathematicalFunctionCapability.CEIL,
            MathematicalFunctionCapability.FLOOR,
            MathematicalFunctionCapability.ROUND,
            MathematicalFunctionCapability.POWER,
            MathematicalFunctionCapability.SQRT,
            # MathematicalFunctionCapability.MOD,
        ])

        return capabilities

    def _register_postgres_adapters(self):
        """Register postgres-specific type adapters to the adapter_registry."""
        postgres_adapters = [
            PostgresJSONBAdapter(),
            PostgresNetworkAddressAdapter(),
        ]
        for adapter in postgres_adapters:
            for py_type, db_types in adapter.supported_types.items():
                for db_type in db_types:
                    self.adapter_registry.register(adapter, py_type, db_type, allow_override=True)
        self.logger.debug("Registered postgres-specific type adapters.")

    def _map_exception(self, error: Exception) -> DatabaseError:
        """Map postgres exceptions to standard backend exceptions.

        Args:
            error: Original exception

        Returns:
            Mapped DatabaseError subclass
        """
        if isinstance(error, PsycopgIntegrityError):
            return IntegrityError(str(error))
        elif isinstance(error, PsycopgOperationalError):
            return OperationalError(str(error))
        elif isinstance(error, PsycopgDeadlockError):
            return DeadlockError(str(error))
        elif isinstance(error, PsycopgProgrammingError):
            return QueryError(str(error))
        elif isinstance(error, PsycopgError):
            return DatabaseError(str(error))
        else:
            raise error


class PostgresBackend(PostgresBackendMixin, StorageBackend):
    """Synchronous Postgres backend implementation using psycopg3."""

    def __init__(self, **kwargs):
        """Initialize synchronous postgres backend.

        Args:
            **kwargs: Configuration parameters
        """
        # Initialize configuration first
        self.config = self._ensure_postgres_config(kwargs)

        # Initialize parent with config
        super().__init__(connection_config=self.config)
        self._register_postgres_adapters()

        # postgres specific setup
        self._connection: Optional[Connection] = None
        self._dialect = PostgresDialect(self.config)
        self._transaction_manager: Optional[PostgresTransactionManager] = None
        self._server_version_cache: Optional[Tuple[int, ...]] = None

    def build_sql(self, sql: str, params: Optional[Union[Tuple, List, Dict]] = None) -> Tuple[str, Tuple]:
        """
        Build SQL statement with parameters.

        Overrides the base implementation to use the postgres-specific SQL builder.
        """
        builder = PostgresSQLBuilder(self.dialect)
        return builder.build(sql, params)

    def insert(self,
               table: str,
               data: Dict,
               returning: Optional[Union[bool, List[str], ReturningOptions]] = None,
               column_adapters: Optional[Dict] = None,
               column_mapping: Optional[Dict[str, str]] = None,
               auto_commit: Optional[bool] = True,
               primary_key: Optional[str] = None) -> QueryResult:
        """Insert record and force returning the primary key.

        Overrides the base implementation to ignore the incorrect `returning=False`
        passed from the base model class.
        """
        # psycopg3 doesn't handle None for SERIAL columns, so we pop the pk if it's None.
        if primary_key and data.get(primary_key) is None:
            data.pop(primary_key, None)

        # Force returning the primary key, overriding the faulty value from the base class
        final_returning = [primary_key] if primary_key else None
        return super().insert(
            table, data, returning=final_returning, column_adapters=column_adapters,
            column_mapping=column_mapping,
            auto_commit=auto_commit, primary_key=primary_key
        )
    @property
    def dialect(self) -> PostgresDialect:
        """Get SQL dialect handler.

        Returns:
            postgres dialect instance
        """
        return self._dialect

    @property
    def transaction_manager(self) -> PostgresTransactionManager:
        """Get transaction manager.

        Returns:
            Synchronous postgres transaction manager
        """
        if self._transaction_manager is None:
            if self._connection is None:
                self.connect()
            self._transaction_manager = PostgresTransactionManager(
                self._connection,
                logger=self.logger
            )
        return self._transaction_manager

    def connect(self) -> None:
        """Establish connection to postgres database.

        Raises:
            ConnectionError: If connection fails
        """
        try:
            self.log(logging.INFO, f"Connecting to postgres at {self.config.host}:{self.config.port}")

            params = self._prepare_connection_params(self.config)
            self._connection = psycopg.connect(**params)
            self._connection.autocommit = True

            # Get version and register converters
            version = self.get_server_version()

            self.log(logging.INFO, f"Connected to postgres version {'.'.join(map(str, version))}")

            # --- ADDED SECTION for SSL/TLS Status Logging ---
            cursor = self._connection.cursor()
            cursor.execute("SELECT ssl, version, cipher FROM pg_stat_ssl s JOIN pg_stat_activity a ON s.pid = a.pid WHERE a.pid = pg_backend_pid();")
            result = cursor.fetchone()
            cursor.close()

            if result:
                ssl_enabled = result[0]
                ssl_version = result[1]
                ssl_cipher = result[2]
                if ssl_enabled:
                    self.log(logging.INFO, f"SSL/TLS is ENABLED for this connection. Version: {ssl_version}, Cipher: {ssl_cipher}")
                else:
                    self.log(logging.INFO, "SSL/TLS is DISABLED for this connection.")
            else:
                self.log(logging.INFO, "Could not retrieve SSL/TLS status from pg_stat_ssl.")
            # --- END ADDITION ---

        except PsycopgError as e:
            error_msg = f"Failed to connect to postgres: {str(e)}"
            self.log(logging.ERROR, error_msg)
            raise ConnectionError(error_msg)

    def disconnect(self) -> None:
        """Close postgres connection.

        Raises:
            ConnectionError: If disconnection fails
        """
        try:
            if self._connection:
                self.log(logging.INFO, "Disconnecting from postgres")
                self._connection.close()
                self._connection = None
                self._transaction_manager = None
                self.log(logging.INFO, "Disconnected from postgres")
        except PsycopgError as e:
            error_msg = f"Failed to disconnect from postgres: {str(e)}"
            self.log(logging.ERROR, error_msg)
            raise ConnectionError(error_msg, original_error=e)

    def ping(self) -> bool:
        """Check if connection is alive.

        Returns:
            True if connection is alive, False otherwise
        """
        try:
            if not self._connection or self._connection.closed:
                return False

            # Execute simple query to check connection
            cursor = self._connection.cursor()
            cursor.execute("SELECT 1")
            cursor.close()
            return True

        except PsycopgError:
            return False

    def get_server_version(self) -> Tuple[int, ...]:
        """Get postgres server version.

        Returns:
            Version tuple (major, minor, patch)

        Raises:
            OperationalError: If version query fails
        """
        if self._server_version_cache:
            return self._server_version_cache

        try:
            if not self._connection:
                self.connect()

            cursor = self._connection.cursor()
            cursor.execute("SELECT version()")
            version_string = cursor.fetchone()[0]
            cursor.close()

            self._server_version_cache = self._parse_server_version(version_string)
            return self._server_version_cache

        except PsycopgError as e:
            error_msg = f"Failed to get server version: {str(e)}"
            self.log(logging.ERROR, error_msg)
            raise OperationalError(error_msg, original_error=e)

    def _get_cursor(self) -> Cursor:
        """Get database cursor for query execution.

        Returns:
            postgres cursor

        Raises:
            ConnectionError: If not connected
        """
        if not self._connection:
            raise ConnectionError("Not connected to database")
        return self._connection.cursor()

    def _handle_error(self, error: Exception) -> None:
        """Handle and map database errors.

        Args:
            error: Original exception
        """
        mapped_error = self._map_exception(error)
        self.log(logging.ERROR, f"Database error: {str(mapped_error)}")
        raise mapped_error

class AsyncPostgresBackend(PostgresBackendMixin, AsyncStorageBackend):
    """Asynchronous postgres backend implementation using psycopg3."""

    def __init__(self, **kwargs):
        """Initialize asynchronous postgres backend.

        Args:
            **kwargs: Configuration parameters
        """
        # Initialize configuration first
        self.config = self._ensure_postgres_config(kwargs)

        # Initialize parent with config
        super().__init__(connection_config=self.config)
        self._register_postgres_adapters()

        # postgres specific setup
        self._connection: Optional[AsyncConnection] = None
        self._dialect = PostgresDialect(self.config)
        self._transaction_manager: Optional[AsyncPostgresTransactionManager] = None
        self._server_version_cache: Optional[Tuple[int, ...]] = None

    async def insert(self,
                     table: str,
                     data: Dict,
                     returning: Optional[Union[bool, List[str], ReturningOptions]] = None,
                     column_adapters: Optional[Dict] = None,
                     column_mapping: Optional[Dict[str, str]] = None,
                     auto_commit: Optional[bool] = True,
                     primary_key: Optional[str] = None) -> QueryResult:
        """Insert record and force returning the primary key asynchronously.

        Overrides the base implementation to ignore the incorrect `returning=False`
        passed from the base model class.
        """
        # psycopg3 doesn't handle None for SERIAL columns, so we pop the pk if it's None.
        if primary_key and data.get(primary_key) is None:
            data.pop(primary_key, None)
            
        # Force returning the primary key, overriding the faulty value from the base class
        final_returning = [primary_key] if primary_key else None
        return await super().insert(
            table, data, returning=final_returning, column_adapters=column_adapters,
            column_mapping=column_mapping,
            auto_commit=auto_commit, primary_key=primary_key
        )

    def build_sql(self, sql: str, params: Optional[Union[Tuple, List, Dict]] = None) -> Tuple[str, Tuple]:
        """
        Build SQL statement with parameters.

        Overrides the base implementation to use the postgres-specific SQL builder.
        """
        builder = PostgresSQLBuilder(self.dialect)
        return builder.build(sql, params)

    @property
    def dialect(self) -> PostgresDialect:
        """Get SQL dialect handler.

        Returns:
            postgres dialect instance
        """
        return self._dialect

    @property
    def transaction_manager(self) -> AsyncPostgresTransactionManager:
        """Get async transaction manager.

        Returns:
            Asynchronous postgres transaction manager
        """
        if self._transaction_manager is None:
            if self._connection is None:
                raise ConnectionError("Not connected to database. Call await backend.connect() first.")
            self._transaction_manager = AsyncPostgresTransactionManager(
                self._connection,
                logger=self.logger
            )
        return self._transaction_manager

    async def connect(self) -> None:
        """Establish async connection to postgres database.

        Raises:
            ConnectionError: If connection fails
        """
        try:
            self.log(logging.INFO, f"Connecting to postgres at {self.config.host}:{self.config.port}")

            params = self._prepare_connection_params(self.config)
            self._connection = await psycopg.AsyncConnection.connect(**params)
            await self._connection.set_autocommit(True)

            # Get version and register converters
            version = await self.get_server_version()

            self.log(logging.INFO, f"Connected to postgres version {'.'.join(map(str, version))}")

            # --- ADDED SECTION for SSL/TLS Status Logging ---
            cursor = self._connection.cursor()
            await cursor.execute("SELECT ssl, version, cipher FROM pg_stat_ssl s JOIN pg_stat_activity a ON s.pid = a.pid WHERE a.pid = pg_backend_pid();")
            result = await cursor.fetchone()
            await cursor.close()

            if result:
                ssl_enabled = result[0]
                ssl_version = result[1]
                ssl_cipher = result[2]
                if ssl_enabled:
                    self.log(logging.INFO, f"SSL/TLS is ENABLED for this connection. Version: {ssl_version}, Cipher: {ssl_cipher}")
                else:
                    self.log(logging.INFO, "SSL/TLS is DISABLED for this connection.")
            else:
                self.log(logging.INFO, "Could not retrieve SSL/TLS status from pg_stat_ssl.")
            # --- END ADDITION ---

        except PsycopgError as e:
            error_msg = f"Failed to connect to postgres: {str(e)}"
            self.log(logging.ERROR, error_msg)
            raise ConnectionError(error_msg)

    async def disconnect(self) -> None:
        """Close async postgres connection.

        Raises:
            ConnectionError: If disconnection fails
        """
        try:
            if self._connection:
                self.log(logging.INFO, "Disconnecting from postgres")
                await self._connection.close()
                self._connection = None
                self._transaction_manager = None
                self.log(logging.INFO, "Disconnected from postgres")
        except PsycopgError as e:
            error_msg = f"Failed to disconnect from postgres: {str(e)}"
            self.log(logging.ERROR, error_msg)
            raise ConnectionError(error_msg, original_error=e)

    async def ping(self) -> bool:
        """Check if async connection is alive.

        Returns:
            True if connection is alive, False otherwise
        """
        try:
            if not self._connection or self._connection.closed:
                return False

            # Execute simple query to check connection
            cursor = self._connection.cursor()
            await cursor.execute("SELECT 1")
            await cursor.close()
            return True

        except PsycopgError:
            return False

    async def get_server_version(self) -> Tuple[int, ...]:
        """Get postgres server version asynchronously.

        Returns:
            Version tuple (major, minor, patch)

        Raises:
            OperationalError: If version query fails
        """
        if self._server_version_cache:
            return self._server_version_cache

        try:
            if not self._connection:
                await self.connect()

            cursor = self._connection.cursor()
            await cursor.execute("SELECT version()")
            result = await cursor.fetchone()
            version_string = result[0]
            await cursor.close()

            self._server_version_cache = self._parse_server_version(version_string)
            return self._server_version_cache

        except PsycopgError as e:
            error_msg = f"Failed to get server version: {str(e)}"
            self.log(logging.ERROR, error_msg)
            raise OperationalError(error_msg, original_error=e)

    async def _handle_auto_commit(self) -> None:
        """Handle auto commit based on psycopg connection and transaction state."""
        try:
            if not self._connection:
                return

            if hasattr(self._connection, 'autocommit') and not self._connection.autocommit:
                if not self._transaction_manager or not self._transaction_manager.is_active:
                    await self._connection.commit()
                    self.log(logging.DEBUG, "Auto-committed operation (not in active transaction)")
        except Exception as e:
            self.log(logging.WARNING, f"Failed to auto-commit: {str(e)}")

    async def _get_cursor(self) -> AsyncCursor:
        """Get async database cursor for query execution.

        Returns:
            postgres async cursor

        Raises:
            ConnectionError: If not connected
        """
        if not self._connection:
            raise ConnectionError("Not connected to database")
        return self._connection.cursor()

    async def _handle_error(self, error: Exception) -> None:
        """Handle and map database errors asynchronously.

        Args:
            error: Original exception
        """
        mapped_error = self._map_exception(error)
        self.log(logging.ERROR, f"Database error: {str(mapped_error)}")
        raise mapped_error
