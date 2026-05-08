# src/rhosocial/activerecord/backend/impl/postgres/adapters/ltree.py
"""
PostgreSQL ltree type adapter.

This module provides type adapters for the PostgreSQL ltree, lquery,
and ltxtquery data types, enabling bidirectional conversion between
Python objects and PostgreSQL string representations.

PostgreSQL Documentation: https://www.postgresql.org/docs/current/ltree.html
"""

from typing import Any, Dict, List, Optional, Set, Type

from rhosocial.activerecord.backend.impl.postgres.types.ltree import (
    PostgresLtree,
    PostgresLquery,
    PostgresLtxtquery,
)


class PostgresLtreeAdapter:
    """PostgreSQL ltree type adapter.

    Converts between Python PostgresLtree objects (or str for convenience)
    and PostgreSQL ltree values.
    """

    @property
    def supported_types(self) -> Dict[Type, Set[Type]]:
        return {
            PostgresLtree: {str},
        }

    def to_database(self, value: Any, target_type: Type, options: Optional[Dict[str, Any]] = None) -> Optional[str]:
        if value is None:
            return None
        if isinstance(value, PostgresLtree):
            return value.to_postgres_string()
        if isinstance(value, str):
            return value
        raise TypeError(f"Cannot convert {type(value).__name__} to ltree")

    def from_database(self, value: Any, target_type: Type, options: Optional[Dict[str, Any]] = None) -> Optional[PostgresLtree]:
        if value is None:
            return None
        if isinstance(value, PostgresLtree):
            return value
        if isinstance(value, str):
            return PostgresLtree.from_postgres_string(value)
        raise TypeError(f"Cannot convert {type(value).__name__} from ltree")

    def to_database_batch(self, values: list, target_type: Type, options: Optional[Dict[str, Any]] = None) -> list:
        return [self.to_database(v, target_type, options) for v in values]

    def from_database_batch(self, values: list, target_type: Type, options: Optional[Dict[str, Any]] = None) -> list:
        return [self.from_database(v, target_type, options) for v in values]


class PostgresLqueryAdapter:
    """PostgreSQL lquery type adapter."""

    @property
    def supported_types(self) -> Dict[Type, Set[Type]]:
        return {
            PostgresLquery: {str},
        }

    def to_database(self, value: Any, target_type: Type, options: Optional[Dict[str, Any]] = None) -> Optional[str]:
        if value is None:
            return None
        if isinstance(value, PostgresLquery):
            return value.to_postgres_string()
        if isinstance(value, str):
            return value
        raise TypeError(f"Cannot convert {type(value).__name__} to lquery")

    def from_database(self, value: Any, target_type: Type, options: Optional[Dict[str, Any]] = None) -> Optional[PostgresLquery]:
        if value is None:
            return None
        if isinstance(value, PostgresLquery):
            return value
        if isinstance(value, str):
            return PostgresLquery(pattern=value)
        raise TypeError(f"Cannot convert {type(value).__name__} from lquery")

    def to_database_batch(self, values: list, target_type: Type, options: Optional[Dict[str, Any]] = None) -> list:
        return [self.to_database(v, target_type, options) for v in values]

    def from_database_batch(self, values: list, target_type: Type, options: Optional[Dict[str, Any]] = None) -> list:
        return [self.from_database(v, target_type, options) for v in values]


class PostgresLtxtqueryAdapter:
    """PostgreSQL ltxtquery type adapter."""

    @property
    def supported_types(self) -> Dict[Type, Set[Type]]:
        return {
            PostgresLtxtquery: {str},
        }

    def to_database(self, value: Any, target_type: Type, options: Optional[Dict[str, Any]] = None) -> Optional[str]:
        if value is None:
            return None
        if isinstance(value, PostgresLtxtquery):
            return value.to_postgres_string()
        if isinstance(value, str):
            return value
        raise TypeError(f"Cannot convert {type(value).__name__} to ltxtquery")

    def from_database(self, value: Any, target_type: Type, options: Optional[Dict[str, Any]] = None) -> Optional[PostgresLtxtquery]:
        if value is None:
            return None
        if isinstance(value, PostgresLtxtquery):
            return value
        if isinstance(value, str):
            return PostgresLtxtquery(query=value)
        raise TypeError(f"Cannot convert {type(value).__name__} from ltxtquery")

    def to_database_batch(self, values: list, target_type: Type, options: Optional[Dict[str, Any]] = None) -> list:
        return [self.to_database(v, target_type, options) for v in values]

    def from_database_batch(self, values: list, target_type: Type, options: Optional[Dict[str, Any]] = None) -> list:
        return [self.from_database(v, target_type, options) for v in values]


__all__ = [
    "PostgresLtreeAdapter",
    "PostgresLqueryAdapter",
    "PostgresLtxtqueryAdapter",
]