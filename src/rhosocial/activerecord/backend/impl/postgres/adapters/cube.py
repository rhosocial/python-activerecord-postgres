# src/rhosocial/activerecord/backend/impl/postgres/adapters/cube.py
"""
PostgreSQL cube type adapter.

This module provides the PostgresCubeAdapter for bidirectional conversion
between Python PostgresCube objects and PostgreSQL cube string representations.

PostgreSQL Documentation: https://www.postgresql.org/docs/current/cube.html
"""

from typing import Any, Dict, List, Optional, Set, Type

from rhosocial.activerecord.backend.impl.postgres.types.cube import PostgresCube


class PostgresCubeAdapter:
    """PostgreSQL cube type adapter.

    Converts between Python PostgresCube objects (or str for convenience)
    and PostgreSQL cube values.
    """

    @property
    def supported_types(self) -> Dict[Type, Set[Type]]:
        return {
            PostgresCube: {str},
        }

    def to_database(self, value: Any, target_type: Type, options: Optional[Dict[str, Any]] = None) -> Optional[str]:
        if value is None:
            return None
        if isinstance(value, PostgresCube):
            return value.to_postgres_string()
        if isinstance(value, str):
            return value
        raise TypeError(f"Cannot convert {type(value).__name__} to cube")

    def from_database(self, value: Any, target_type: Type, options: Optional[Dict[str, Any]] = None) -> Optional[PostgresCube]:
        if value is None:
            return None
        if isinstance(value, PostgresCube):
            return value
        if isinstance(value, str):
            return PostgresCube.from_postgres_string(value)
        raise TypeError(f"Cannot convert {type(value).__name__} from cube")

    def to_database_batch(self, values: list, target_type: Type, options: Optional[Dict[str, Any]] = None) -> list:
        return [self.to_database(v, target_type, options) for v in values]

    def from_database_batch(self, values: list, target_type: Type, options: Optional[Dict[str, Any]] = None) -> list:
        return [self.from_database(v, target_type, options) for v in values]


__all__ = [
    "PostgresCubeAdapter",
]