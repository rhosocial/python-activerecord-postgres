# src/rhosocial/activerecord/backend/impl/postgres/adapters/pg_lsn.py
"""
PostgreSQL pg_lsn Type Adapter.

This module provides type adapter for PostgreSQL pg_lsn type.

PostgreSQL Documentation: https://www.postgresql.org/docs/current/datatype-pg-lsn.html

The pg_lsn (Log Sequence Number) type represents a position in the
PostgreSQL Write-Ahead Log (WAL). It is a 64-bit integer internally,
displayed as two hexadecimal numbers separated by a slash.
"""
from typing import Any, Dict, List, Optional, Set, Type

from ..types.pg_lsn import PostgresLsn


class PostgresLsnAdapter:
    """PostgreSQL pg_lsn type adapter.

    This adapter converts between Python PostgresLsn/int/str and
    PostgreSQL pg_lsn values.

    Note: pg_lsn type is available since PostgreSQL 9.4.
    """

    @property
    def supported_types(self) -> Dict[Type, Set[Type]]:
        """Return supported type mappings."""
        return {
            PostgresLsn: {str},
        }

    def to_database(
        self,
        value: Any,
        target_type: Type,
        options: Optional[Dict[str, Any]] = None
    ) -> Optional[str]:
        """Convert Python value to PostgreSQL pg_lsn.

        Args:
            value: Python value (PostgresLsn, int, or str)
            target_type: Target type (str)
            options: Optional conversion options

        Returns:
            LSN string in PostgreSQL format, or None

        Raises:
            TypeError: If value type is not supported
            ValueError: If LSN value is invalid
        """
        if value is None:
            return None

        if isinstance(value, PostgresLsn):
            return str(value)

        if isinstance(value, int):
            return str(PostgresLsn(value))

        if isinstance(value, str):
            lsn = PostgresLsn.from_string(value)
            return str(lsn)

        raise TypeError(
            f"Cannot convert {type(value).__name__} to pg_lsn"
        )

    def from_database(
        self,
        value: Any,
        target_type: Type,
        options: Optional[Dict[str, Any]] = None
    ) -> Optional[PostgresLsn]:
        """Convert PostgreSQL pg_lsn to Python object.

        Args:
            value: Database value (string)
            target_type: Target type (PostgresLsn)
            options: Optional conversion options

        Returns:
            PostgresLsn instance, or None

        Raises:
            TypeError: If value type is not supported
            ValueError: If LSN format is invalid
        """
        if value is None:
            return None

        if isinstance(value, PostgresLsn):
            return value

        if isinstance(value, str):
            return PostgresLsn.from_string(value)

        if isinstance(value, int):
            return PostgresLsn(value)

        raise TypeError(
            f"Cannot convert {type(value).__name__} from pg_lsn"
        )

    def to_database_batch(
        self,
        values: List[Any],
        target_type: Type,
        options: Optional[Dict[str, Any]] = None
    ) -> List[Any]:
        """Batch convert Python values to PostgreSQL format."""
        return [
            self.to_database(v, target_type, options)
            for v in values
        ]

    def from_database_batch(
        self,
        values: List[Any],
        target_type: Type,
        options: Optional[Dict[str, Any]] = None
    ) -> List[Optional[PostgresLsn]]:
        """Batch convert database values to Python objects."""
        return [
            self.from_database(v, target_type, options)
            for v in values
        ]


__all__ = ['PostgresLsnAdapter']
