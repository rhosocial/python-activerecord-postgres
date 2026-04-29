# src/rhosocial/activerecord/backend/impl/postgres/adapters/postgis.py
"""
PostgreSQL PostGIS Type Adapter.

This module provides a type adapter for the PostgreSQL geometry/geography
types (PostGIS extension), enabling bidirectional conversion between Python
PostgresGeometry objects and PostgreSQL spatial values.

PostGIS Documentation: https://postgis.net/docs/

The geometry/geography types require the PostGIS extension:
    CREATE EXTENSION IF NOT EXISTS postgis;
"""

from typing import Any, Dict, Optional, Set, Type

from rhosocial.activerecord.backend.impl.postgres.types.postgis import PostgresGeometry


class PostgresGeometryAdapter:
    """PostgreSQL PostGIS geometry/geography type adapter.

    This adapter converts between Python PostgresGeometry objects
    and PostgreSQL geometry/geography values.

    Supported conversions:
    - PostgresGeometry -> str (to database)
    - str -> PostgresGeometry (from database)
    """

    @property
    def supported_types(self) -> Dict[Type, Set[Type]]:
        """Return supported type mappings."""
        return {
            PostgresGeometry: {str},
        }

    def to_database(self, value: Any, target_type: Type, options: Optional[Dict[str, Any]] = None) -> Optional[str]:
        """Convert Python value to PostgreSQL geometry/geography.

        Args:
            value: Python value (PostgresGeometry, str, or None)
            target_type: Target type (str)
            options: Optional conversion options

        Returns:
            SQL function call string (e.g., ``ST_GeomFromText(...)``), or None

        Raises:
            TypeError: If value type is not supported
        """
        if value is None:
            return None

        if isinstance(value, PostgresGeometry):
            return value.to_postgres_string()

        if isinstance(value, str):
            return value

        raise TypeError(f"Cannot convert {type(value).__name__} to geometry")

    def from_database(self, value: Any, target_type: Type, options: Optional[Dict[str, Any]] = None) -> Optional[PostgresGeometry]:
        """Convert PostgreSQL geometry/geography to Python object.

        Args:
            value: Database value (WKT/EWKT string or PostgresGeometry)
            target_type: Target type (PostgresGeometry)
            options: Optional conversion options

        Returns:
            PostgresGeometry instance, or None

        Raises:
            TypeError: If value type is not supported
        """
        if value is None:
            return None

        if isinstance(value, PostgresGeometry):
            return value

        if isinstance(value, str):
            return PostgresGeometry.from_postgres_string(value)

        raise TypeError(f"Cannot convert {type(value).__name__} from geometry")

    def to_database_batch(self, values: list, target_type: Type, options: Optional[Dict[str, Any]] = None) -> list:
        """Batch convert Python values to PostgreSQL format."""
        return [self.to_database(v, target_type, options) for v in values]

    def from_database_batch(self, values: list, target_type: Type, options: Optional[Dict[str, Any]] = None) -> list:
        """Batch convert database values to Python objects."""
        return [self.from_database(v, target_type, options) for v in values]


__all__ = [
    "PostgresGeometryAdapter",
]
