# src/rhosocial/activerecord/backend/impl/postgres/adapters/bit_string.py
"""
PostgreSQL Bit String Types Adapter.

This module provides type adapters for PostgreSQL bit string types.

PostgreSQL Documentation: https://www.postgresql.org/docs/current/datatype-bit.html

Bit String Types:
- bit(n): Fixed-length bit string
- bit varying(n) / varbit(n): Variable-length bit string

Bit strings are strings of 1s and 0s used for bit masks and bit manipulation.
"""
from typing import Any, Dict, Optional, Set, Type

from ..types.bit_string import PostgresBitString


class PostgresBitStringAdapter:
    """PostgreSQL bit string type adapter.

    This adapter converts between Python PostgresBitString objects and
    PostgreSQL bit string values.

    Supported types:
    - bit(n): Fixed-length bit string
    - bit varying(n) / varbit(n): Variable-length bit string
    """

    @property
    def supported_types(self) -> Dict[Type, Set[Type]]:
        """Return supported type mappings."""
        return {
            PostgresBitString: {str},
        }

    def to_database(
        self,
        value: Any,
        target_type: Type,
        options: Optional[Dict[str, Any]] = None
    ) -> Optional[str]:
        """Convert Python value to PostgreSQL bit string.

        Args:
            value: PostgresBitString, str, or int
            target_type: Target type (not used, kept for interface)
            options: Optional conversion options
            - 'length': Fixed length for bit type

        Returns:
            PostgreSQL bit string literal, or None if value is None
        """
        if value is None:
            return None

        if isinstance(value, PostgresBitString):
            return value.to_postgres_string()

        if isinstance(value, str):
            # Validate it's a valid bit string
            if not all(c in '01' for c in value):
                raise ValueError(f"Invalid bit string: {value}")
            return f"B'{value}'"

        if isinstance(value, int):
            length = options.get('length') if options else None
            bs = PostgresBitString.from_int(value, length)
            return bs.to_postgres_string()

        raise TypeError(f"Cannot convert {type(value).__name__} to bit string")

    def from_database(
        self,
        value: Any,
        target_type: Type,
        options: Optional[Dict[str, Any]] = None
    ) -> Optional[PostgresBitString]:
        """Convert PostgreSQL bit string to Python object.

        Args:
            value: Bit string value from database
            target_type: Target Python type
            options: Optional conversion options

        Returns:
            PostgresBitString object, or None if value is None
        """
        if value is None:
            return None

        # If already a PostgresBitString, return as-is
        if isinstance(value, PostgresBitString):
            return value

        if isinstance(value, str):
            return PostgresBitString.from_postgres_string(value)

        if isinstance(value, int):
            return PostgresBitString.from_int(value)

        raise TypeError(f"Cannot convert {type(value).__name__} to PostgresBitString")

    def to_database_batch(
        self,
        values: list,
        target_type: Type,
        options: Optional[Dict[str, Any]] = None
    ) -> list:
        """Batch convert values to database format."""
        return [self.to_database(v, target_type, options) for v in values]

    def from_database_batch(
        self,
        values: list,
        target_type: Type,
        options: Optional[Dict[str, Any]] = None
    ) -> list:
        """Batch convert values from database format."""
        return [self.from_database(v, target_type, options) for v in values]


__all__ = ['PostgresBitStringAdapter']
