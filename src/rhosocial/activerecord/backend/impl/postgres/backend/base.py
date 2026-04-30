# src/rhosocial/activerecord/backend/impl/postgres/backend/base.py
"""PostgreSQL backend shared functionality.

This module provides the base mixin class for PostgreSQL backend implementations,
containing methods that are shared between sync and async backends.
"""

from typing import Dict, Tuple, Type, TYPE_CHECKING
import logging

if TYPE_CHECKING:
    from rhosocial.activerecord.backend.type_adapter import SQLTypeAdapter
    from rhosocial.activerecord.backend.protocols import ConcurrencyHint


class PostgresBackendMixin:
    """PostgreSQL backend shared methods mixin.

    This mixin provides methods that are shared between sync and async
    PostgreSQL backend implementations. These methods do not involve
    I/O operations and have identical implementations in both backends.

    Classes using this mixin must provide:
    - self._dialect: PostgresDialect instance
    - self.adapter_registry: Type adapter registry
    - self.config: Connection configuration
    - self._logger: Logger instance
    """

    # PostgreSQL connection error SQLSTATE codes
    # Reference: https://www.postgresql.org/docs/current/errcodes-appendix.html
    CONNECTION_ERROR_SQLSTATES = {
        "08000",  # CONNECTION_EXCEPTION
        "08001",  # SQLCLIENT_UNABLE_TO_ESTABLISH_SQLCONNECTION
        "08003",  # CONNECTION_DOES_NOT_EXIST
        "08004",  # SQLSERVER_REJECTED_ESTABLISHMENT_OF_SQLCONNECTION
        "08006",  # CONNECTION_FAILURE
        "08007",  # TRANSACTION_RESOLUTION_UNKNOWN
        "08P01",  # PROTOCOL_VIOLATION
        "57P01",  # ADMIN_SHUTDOWN
        "57P02",  # CRASH_SHUTDOWN
        "57P03",  # CANNOT_CONNECT_NOW
        "57P04",  # DATABASE_DROPPED
    }

    def log(self, level: int, message: str):
        """Log a message with the specified level."""
        if hasattr(self, "_logger") and self._logger:
            self._logger.log(level, message)
        else:
            # Fallback logging
            print(f"[{logging.getLevelName(level)}] {message}")

    def get_default_schema(self) -> str:
        """Get the default schema name for PostgreSQL.

        Returns the configured default_schema, or 'public' as the
        PostgreSQL standard default if not explicitly set.

        Returns:
            str: Default schema name
        """
        return getattr(self.config, "default_schema", None) or "public"

    def create_expression(self, expression_str: str):
        """Create an expression object for raw SQL expressions."""
        from rhosocial.activerecord.backend.expression.operators import RawSQLExpression

        return RawSQLExpression(self.dialect, expression_str)

    @property
    def threadsafety(self) -> int:
        """Return driver threadsafety level.

        PostgreSQL connections can be safely shared across threads when using
        psycopg version 3, which supports thread-safe connection handling.

        Returns:
            2 (connections can be safely shared across threads)
        """
        return 2

    def requires_manual_commit(self) -> bool:
        """Check if manual commit is required for this database."""
        return not getattr(self.config, "autocommit", False)

    def _register_postgres_adapters(self):
        """Register PostgreSQL-specific type adapters.

        Range and Multirange adapter registration is controlled by configuration:
        - range_adapter_mode: Controls how Range types are handled
          - NATIVE (default): Use psycopg's native Range type (pass-through)
          - CUSTOM: Use PostgresRange custom type
          - BOTH: Register both adapters, with custom taking precedence
        - multirange_adapter_mode: Same for Multirange types (PG 14+ only)

        Note: The following adapters are NOT registered by default due to str->str
        type mapping conflicts with existing string handling:
        - PostgresXMLAdapter: XML type
        - PostgresBitStringAdapter: bit/varbit types
        - PostgresJsonPathAdapter: jsonpath type

        Users should specify these explicitly when working with such columns.
        """
        from ..adapters import (
            PostgresListAdapter,
            PostgresJSONBAdapter,
            PostgresNetworkAddressAdapter,
            PostgresEnumAdapter,
            PostgresRangeAdapter,
            PostgresMultirangeAdapter,
        )
        from ..adapters.geometric import PostgresGeometryAdapter
        from ..adapters.monetary import PostgresMoneyAdapter
        from ..adapters.network_address import PostgresMacaddrAdapter, PostgresMacaddr8Adapter
        from ..adapters.object_identifier import PostgresOidAdapter, PostgresXidAdapter, PostgresTidAdapter
        from ..adapters.pg_lsn import PostgresLsnAdapter
        from ..adapters.text_search import PostgresTsVectorAdapter, PostgresTsQueryAdapter
        from ..config import RangeAdapterMode

        pg_adapters = [
            PostgresListAdapter(),
            PostgresJSONBAdapter(),
            PostgresNetworkAddressAdapter(),
            PostgresGeometryAdapter(),
            PostgresEnumAdapter(),
            PostgresMoneyAdapter(),
            PostgresMacaddrAdapter(),
            PostgresMacaddr8Adapter(),
            PostgresTsVectorAdapter(),
            PostgresTsQueryAdapter(),
            PostgresLsnAdapter(),
            PostgresOidAdapter(),
            PostgresXidAdapter(),
            PostgresTidAdapter(),
            # PostgresJsonPathAdapter(), # Not registered: str->str conflict
            # PostgresXMLAdapter is NOT registered by default
            # due to str->str type pair conflict
        ]

        for adapter in pg_adapters:
            for py_type, db_types in adapter.supported_types.items():
                for db_type in db_types:
                    self.adapter_registry.register(adapter, py_type, db_type)

        # Register Range adapters based on configuration
        range_mode = getattr(self.config, "range_adapter_mode", RangeAdapterMode.NATIVE)
        if range_mode in (RangeAdapterMode.CUSTOM, RangeAdapterMode.BOTH):
            range_adapter = PostgresRangeAdapter()
            for py_type, db_types in range_adapter.supported_types.items():
                for db_type in db_types:
                    self.adapter_registry.register(range_adapter, py_type, db_type)
            self.log(logging.DEBUG, "Registered PostgresRangeAdapter (custom range type)")

        # Register Multirange adapters based on configuration (PG 14+ only)
        # Note: Version check happens in dialect, but at this point we haven't
        # connected yet, so we register if configured. The adapter itself will
        # handle version-specific logic if needed.
        multirange_mode = getattr(self.config, "multirange_adapter_mode", RangeAdapterMode.NATIVE)
        if multirange_mode in (RangeAdapterMode.CUSTOM, RangeAdapterMode.BOTH):
            multirange_adapter = PostgresMultirangeAdapter()
            for py_type, db_types in multirange_adapter.supported_types.items():
                for db_type in db_types:
                    self.adapter_registry.register(multirange_adapter, py_type, db_type)
            self.log(logging.DEBUG, "Registered PostgresMultirangeAdapter (custom multirange type)")

        self.log(logging.DEBUG, "Registered PostgreSQL-specific type adapters")

    def _is_connection_error(self, error: Exception) -> bool:
        """Check if an error indicates a connection loss.

        This method identifies errors that result from lost or invalid connections,
        which should trigger automatic reconnection attempts.

        Args:
            error: The exception to check

        Returns:
            True if the error indicates a connection problem, False otherwise
        """
        from psycopg.errors import OperationalError as PsycopgOperationalError

        if isinstance(error, PsycopgOperationalError):
            # Check SQLSTATE code if available
            sqlstate = getattr(error, 'sqlstate', None)
            if sqlstate and sqlstate in self.CONNECTION_ERROR_SQLSTATES:
                return True

            # Check for common connection error patterns in message
            error_msg = str(error).lower()
            connection_error_patterns = [
                'connection',
                'connect',
                'closed',
                'terminated',
                'unexpectedly',
                'broken pipe',
                'reset by peer',
                'server closed',
                'server has gone away',
            ]
            for pattern in connection_error_patterns:
                if pattern in error_msg:
                    return True

        return False

    def _is_connection_error_message(self, error_msg: str) -> bool:
        """Check if an error message indicates a connection loss.

        This method identifies error messages that result from lost or invalid connections,
        which should trigger automatic reconnection attempts.

        Args:
            error_msg: The error message to check

        Returns:
            True if the message indicates a connection problem, False otherwise
        """
        error_msg_lower = error_msg.lower()
        connection_error_patterns = [
            'connection',
            'connect',
            'closed',
            'terminated',
            'terminating connection due to administrator command',
            'unexpectedly',
            'broken pipe',
            'reset by peer',
            'server closed',
            'server has gone away',
            'ssl connection has been closed',
            'consuming input failed',
        ]
        for pattern in connection_error_patterns:
            if pattern in error_msg_lower:
                return True
        return False

    def _parse_explain_result(self, raw_rows, sql, duration):
        """Return a typed :class:`PostgresExplainResult` for PostgreSQL EXPLAIN output."""
        from ..explain import PostgresExplainResult, PostgresExplainPlanLine
        rows = [PostgresExplainPlanLine(line=r.get("QUERY PLAN", "")) for r in raw_rows]
        return PostgresExplainResult(raw_rows=raw_rows, sql=sql, duration=duration, rows=rows)

    def get_default_adapter_suggestions(self) -> Dict[Type, Tuple["SQLTypeAdapter", Type]]:
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

        Note:
            PostgreSQL XML and BitString types are NOT included here due to str->str
            mapping conflicts with the base string handling. Users must explicitly
            specify adapters for these types when needed.
        """
        suggestions: Dict[Type, Tuple["SQLTypeAdapter", Type]] = {}

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

        # Import PostgreSQL-specific types
        from ..types.range import PostgresRange, PostgresMultirange
        from ..types.geometric import Point, Line, LineSegment, Box, Path, Polygon, Circle
        from ..types.text_search import PostgresTsVector, PostgresTsQuery
        from ..types.monetary import PostgresMoney
        from ..types.network_address import PostgresMacaddr, PostgresMacaddr8
        from ..types.pg_lsn import PostgresLsn
        from ..types.json import PostgresJsonPath
        from ..types.object_identifier import (
            OID,
            RegClass,
            RegType,
            RegProc,
            RegProcedure,
            RegOper,
            RegOperator,
            RegConfig,
            RegDictionary,
            RegNamespace,
            RegRole,
            RegCollation,
            XID,
            XID8,
            CID,
            TID,
        )

        type_mappings = [
            # === Standard Python Types ===
            (bool, bool),  # Python bool -> DB driver bool (PostgreSQL BOOLEAN)
            # Why str for date/time?
            # PostgreSQL has native DATE, TIME, TIMESTAMP types but accepts string representations
            (datetime, str),  # Python datetime -> DB driver str (PostgreSQL TIMESTAMP)
            (date, str),  # Python date -> DB driver str (PostgreSQL DATE)
            (time, str),  # Python time -> DB driver str (PostgreSQL TIME)
            (Decimal, Decimal),  # Python Decimal -> DB driver Decimal (PostgreSQL NUMERIC/DECIMAL)
            (UUID, str),  # Python UUID -> DB driver str (PostgreSQL UUID type)
            (dict, str),  # Python dict -> DB driver str (PostgreSQL JSON/JSONB)
            (list, list),  # Python list -> DB driver list (PostgreSQL arrays - psycopg handles natively)
            (Enum, str),  # Python Enum -> DB driver str (PostgreSQL TEXT)
            # === PostgreSQL Range Types ===
            (PostgresRange, str),  # PostgreSQL range types (int4range, daterange, etc.)
            (PostgresMultirange, str),  # PostgreSQL multirange types (PG 14+)
            # === PostgreSQL Geometric Types ===
            # All geometric types map to str representation
            (Point, str),  # 2D point (x, y)
            (Line, str),  # Infinite line {A, B, C}
            (LineSegment, str),  # Line segment [(x1,y1),(x2,y2)]
            (Box, str),  # Rectangular box (x1,y1),(x2,y2)
            (Path, str),  # Sequence of points (open or closed)
            (Polygon, str),  # Closed path
            (Circle, str),  # Circle <(x,y),r>
            # === PostgreSQL Text Search Types ===
            (PostgresTsVector, str),  # Text search vector
            (PostgresTsQuery, str),  # Text search query
            # === PostgreSQL Monetary Type ===
            (PostgresMoney, str),  # MONEY type with locale-aware formatting
            # === PostgreSQL Network Address Types ===
            (PostgresMacaddr, str),  # 6-byte MAC address
            (PostgresMacaddr8, str),  # 8-byte MAC address
            # === PostgreSQL pg_lsn Type ===
            (PostgresLsn, str),  # Log Sequence Number
            # === PostgreSQL JSON Path Type ===
            (PostgresJsonPath, str),  # JSON path expression
            # === PostgreSQL Object Identifier Types ===
            (OID, int),  # Unsigned 4-byte integer for internal object identification
            (RegClass, str),  # Relation (table/view/sequence) name
            (RegType, str),  # Data type name
            (RegProc, str),  # Function name
            (RegProcedure, str),  # Function name with argument types
            (RegOper, str),  # Operator name
            (RegOperator, str),  # Operator name with argument types
            (RegConfig, str),  # Text search configuration name
            (RegDictionary, str),  # Text search dictionary name
            (RegNamespace, str),  # Namespace (schema) name
            (RegRole, str),  # Role (user/group) name
            (RegCollation, str),  # Collation name
            # === PostgreSQL Transaction/Command Identifiers ===
            (XID, int),  # 32-bit transaction identifier
            (XID8, int),  # 64-bit transaction identifier (PG 13+)
            (CID, int),  # Command identifier
            # === PostgreSQL Tuple Identifier ===
            (TID, str),  # Tuple identifier (block number, offset)
            # Note: PostgresXML and PostgresBitString are NOT included here
            # because they both map to str, which conflicts with base string handling.
            # Users must explicitly specify adapters for these types.
        ]

        # Iterate through the defined mappings and retrieve adapters from the registry.
        for py_type, db_type in type_mappings:
            adapter = self.adapter_registry.get_adapter(py_type, db_type)
            if adapter:
                suggestions[py_type] = (adapter, db_type)
            else:
                # Log a debug message if a specific adapter is expected but not found.
                self.log(
                    logging.DEBUG,
                    f"No adapter found for ({py_type.__name__}, {db_type.__name__}). "
                    "Suggestion will not be provided for this type.",
                )

        return suggestions


class PostgresConcurrencyMixin:
    """Mixin providing PostgreSQL-specific concurrency hint.

    Uses PostgreSQL connection pool for concurrency limit.
    Returns pool.size as the concurrency limit.
    """

    def get_concurrency_hint(self) -> "ConcurrencyHint":
        """Get concurrency hint based on connection pool size."""
        from rhosocial.activerecord.backend.protocols import ConcurrencyHint

        pool = getattr(self, "_pool", None)
        if pool and hasattr(pool, "size"):
            max_concurrency = pool.size
            return ConcurrencyHint(
                max_concurrency=max_concurrency,
                reason=f"connection pool size={max_concurrency}",
            )
        # No pool - return None to indicate no constraint
        return ConcurrencyHint(
            max_concurrency=None,
            reason="no connection pool configured",
        )


__all__ = ["PostgresBackendMixin", "PostgresConcurrencyMixin"]
