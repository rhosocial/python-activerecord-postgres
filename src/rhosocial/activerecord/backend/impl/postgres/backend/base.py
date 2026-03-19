# src/rhosocial/activerecord/backend/impl/postgres/backend/base.py
"""PostgreSQL backend shared functionality.

This module provides the base mixin class for PostgreSQL backend implementations,
containing methods that are shared between sync and async backends.
"""
from typing import Dict, Optional, Tuple, Type, TYPE_CHECKING
import logging

if TYPE_CHECKING:
    from rhosocial.activerecord.backend.type_adapter import SQLTypeAdapter


class PostgresBackendMixin:
    """PostgreSQL backend shared methods mixin.

    This mixin provides methods that are shared between sync and async
    PostgreSQL backend implementations. These methods do not involve
    I/O operations and have identical implementations in both backends.

    Classes using this mixin must provide:
    - self._dialect: PostgresDialect instance
    - self.adapter_registry: Type adapter registry
    - self.log(level, message): Logging method
    - self.config: Connection configuration
    """

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

    def requires_manual_commit(self) -> bool:
        """Check if manual commit is required for this database."""
        return not getattr(self.config, 'autocommit', False)

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

        Note:
            PostgreSQL XML and BitString types are NOT included here due to str->str
            mapping conflicts with the base string handling. Users must explicitly
            specify adapters for these types when needed.
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

        # Import PostgreSQL-specific types
        from ..types.range import PostgresRange, PostgresMultirange
        from ..types.geometric import (
            Point, Line, LineSegment, Box, Path, Polygon, Circle
        )
        from ..types.text_search import PostgresTsVector, PostgresTsQuery
        from ..types.monetary import PostgresMoney
        from ..types.network_address import PostgresMacaddr, PostgresMacaddr8
        from ..types.pg_lsn import PostgresLsn
        from ..types.json import PostgresJsonPath
        from ..types.object_identifier import (
            OID, RegClass, RegType, RegProc, RegProcedure, RegOper, RegOperator,
            RegConfig, RegDictionary, RegNamespace, RegRole, RegCollation,
            XID, XID8, CID, TID
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
                self.log(logging.DEBUG, f"No adapter found for ({py_type.__name__}, {db_type.__name__}). "
                      "Suggestion will not be provided for this type.")

        return suggestions


__all__ = ['PostgresBackendMixin']
