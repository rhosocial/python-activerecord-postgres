# src/rhosocial/activerecord/backend/impl/postgres/adapters/uuid.py
"""
PostgreSQL UUID Type Adapter.

This module provides a type adapter for the PostgreSQL uuid data type,
enabling bidirectional conversion between Python uuid.UUID objects
and PostgreSQL uuid values.

PostgreSQL Documentation: https://www.postgresql.org/docs/current/datatype-uuid.html

The uuid data type is a built-in PostgreSQL type (no extension required).
For UUID generation functions, see:
- gen_random_uuid() (PostgreSQL 13+, built-in)
- uuid_generate_v4() (uuid-ossp extension, PostgreSQL < 13)
"""

import uuid
from typing import Any, Dict, Optional, Set, Type


class PostgresUUIDAdapter:
    """PostgreSQL UUID type adapter.

    This adapter converts between Python uuid.UUID objects and
    PostgreSQL uuid values.

    The PostgreSQL uuid data type is built-in and requires no extension.
    It stores RFC 4122 compliant UUIDs in 16 bytes.

    Supported conversions:
    - uuid.UUID -> str (to database)
    - str -> uuid.UUID (from database)
    """

    @property
    def supported_types(self) -> Dict[Type, Set[Type]]:
        """Return supported type mappings."""
        return {
            uuid.UUID: {str},
        }

    def to_database(self, value: Any, target_type: Type, options: Optional[Dict[str, Any]] = None) -> Optional[str]:
        """Convert Python value to PostgreSQL uuid.

        Args:
            value: Python value (uuid.UUID, str, or None)
            target_type: Target type (str)
            options: Optional conversion options

        Returns:
            UUID string in standard format, or None

        Raises:
            TypeError: If value type is not supported
            ValueError: If string value is not a valid UUID
        """
        if value is None:
            return None

        if isinstance(value, uuid.UUID):
            return str(value)

        if isinstance(value, str):
            # Validate that the string is a valid UUID
            uuid.UUID(value)
            return value

        raise TypeError(f"Cannot convert {type(value).__name__} to UUID")

    def from_database(
        self, value: Any, target_type: Type,
        options: Optional[Dict[str, Any]] = None
    ) -> Optional[uuid.UUID]:
        """Convert PostgreSQL uuid to Python object.

        Args:
            value: Database value (string or uuid.UUID)
            target_type: Target type (uuid.UUID)
            options: Optional conversion options

        Returns:
            uuid.UUID instance, or None

        Raises:
            TypeError: If value type is not supported
            ValueError: If string value is not a valid UUID
        """
        if value is None:
            return None

        if isinstance(value, uuid.UUID):
            return value

        if isinstance(value, str):
            return uuid.UUID(value)

        raise TypeError(f"Cannot convert {type(value).__name__} from UUID")

    def to_database_batch(self, values: list, target_type: Type, options: Optional[Dict[str, Any]] = None) -> list:
        """Batch convert Python values to PostgreSQL format."""
        return [self.to_database(v, target_type, options) for v in values]

    def from_database_batch(self, values: list, target_type: Type, options: Optional[Dict[str, Any]] = None) -> list:
        """Batch convert database values to Python objects."""
        return [self.from_database(v, target_type, options) for v in values]


__all__ = [
    "PostgresUUIDAdapter",
]
