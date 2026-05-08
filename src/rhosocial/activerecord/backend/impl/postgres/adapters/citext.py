# src/rhosocial/activerecord/backend/impl/postgres/adapters/citext.py
"""
PostgreSQL citext type adapter.

This module provides the PostgresCitextAdapter for bidirectional conversion
between Python PostgresCitext objects and PostgreSQL citext string values.

PostgreSQL Documentation: https://www.postgresql.org/docs/current/citext.html
"""

from typing import Any, Dict, List, Optional, Set, Type

from rhosocial.activerecord.backend.impl.postgres.types.citext import PostgresCitext


class PostgresCitextAdapter:
    """PostgreSQL citext type adapter.

    Converts between Python PostgresCitext objects (or str for convenience)
    and PostgreSQL citext values.
    """

    @property
    def supported_types(self) -> Dict[Type, Set[Type]]:
        return {
            PostgresCitext: {str},
        }

    def to_database(self, value: Any, target_type: Type, options: Optional[Dict[str, Any]] = None) -> Optional[str]:
        if value is None:
            return None
        if isinstance(value, PostgresCitext):
            return value.to_postgres_string()
        if isinstance(value, str):
            return value
        raise TypeError(f"Cannot convert {type(value).__name__} to citext")

    def from_database(self, value: Any, target_type: Type, options: Optional[Dict[str, Any]] = None) -> Optional[PostgresCitext]:
        if value is None:
            return None
        if isinstance(value, PostgresCitext):
            return value
        if isinstance(value, str):
            return PostgresCitext.from_postgres_string(value)
        raise TypeError(f"Cannot convert {type(value).__name__} from citext")

    def to_database_batch(self, values: list, target_type: Type, options: Optional[Dict[str, Any]] = None) -> list:
        return [self.to_database(v, target_type, options) for v in values]

    def from_database_batch(self, values: list, target_type: Type, options: Optional[Dict[str, Any]] = None) -> list:
        return [self.from_database(v, target_type, options) for v in values]


__all__ = [
    "PostgresCitextAdapter",
]