# src/rhosocial/activerecord/backend/impl/postgres/adapters/pgvector.py
"""
PostgreSQL pgvector Type Adapter.

This module provides a type adapter for the PostgreSQL vector data type
(pgvector extension), enabling bidirectional conversion between Python
PostgresVector objects / List[float] and PostgreSQL vector values.

pgvector Documentation: https://github.com/pgvector/pgvector

The vector type requires the pgvector extension:
    CREATE EXTENSION IF NOT EXISTS vector;
"""

from typing import Any, Dict, Optional, Set, Type

from rhosocial.activerecord.backend.impl.postgres.types.pgvector import PostgresVector


class PostgresVectorAdapter:
    """PostgreSQL pgvector type adapter.

    This adapter converts between Python PostgresVector objects (or
    List[float] as a convenience) and PostgreSQL vector values.

    Supported conversions:
    - PostgresVector -> str (to database)
    - List[float] -> str (to database, convenience)
    - str -> PostgresVector (from database)
    - list -> PostgresVector (from database)
    """

    @property
    def supported_types(self) -> Dict[Type, Set[Type]]:
        """Return supported type mappings."""
        return {
            PostgresVector: {str},
        }

    def to_database(self, value: Any, target_type: Type, options: Optional[Dict[str, Any]] = None) -> Optional[str]:
        """Convert Python value to PostgreSQL vector.

        Args:
            value: Python value (PostgresVector, List[float], str, or None)
            target_type: Target type (str)
            options: Optional conversion options

        Returns:
            Vector string in format ``[v1, v2, v3]``, or None

        Raises:
            TypeError: If value type is not supported
        """
        if value is None:
            return None

        if isinstance(value, PostgresVector):
            return value.to_postgres_string()

        if isinstance(value, list):
            vec = PostgresVector(values=value)
            return vec.to_postgres_string()

        if isinstance(value, str):
            return value

        raise TypeError(f"Cannot convert {type(value).__name__} to vector")

    def from_database(
        self, value: Any, target_type: Type,
        options: Optional[Dict[str, Any]] = None
    ) -> Optional[PostgresVector]:
        """Convert PostgreSQL vector to Python object.

        Args:
            value: Database value (string, list, or PostgresVector)
            target_type: Target type (PostgresVector)
            options: Optional conversion options

        Returns:
            PostgresVector instance, or None

        Raises:
            TypeError: If value type is not supported
        """
        if value is None:
            return None

        if isinstance(value, PostgresVector):
            return value

        if isinstance(value, str):
            return PostgresVector.from_postgres_string(value)

        if isinstance(value, list):
            return PostgresVector(values=value)

        raise TypeError(f"Cannot convert {type(value).__name__} from vector")

    def to_database_batch(self, values: list, target_type: Type, options: Optional[Dict[str, Any]] = None) -> list:
        """Batch convert Python values to PostgreSQL format."""
        return [self.to_database(v, target_type, options) for v in values]

    def from_database_batch(self, values: list, target_type: Type, options: Optional[Dict[str, Any]] = None) -> list:
        """Batch convert database values to Python objects."""
        return [self.from_database(v, target_type, options) for v in values]


__all__ = [
    "PostgresVectorAdapter",
]
