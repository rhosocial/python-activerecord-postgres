# src/rhosocial/activerecord/backend/impl/postgres/types/enum.py
"""PostgreSQL ENUM type support.

This module provides:
- PostgresEnumType: Type reference expression for ENUM types
- EnumTypeManager for lifecycle management
- Enum utility functions (enum_range, enum_first, enum_last, etc.)

PostgreSQL Documentation: https://www.postgresql.org/docs/current/datatype-enum.html

PostgreSQL ENUM types are custom types created with CREATE TYPE.
They are reusable across tables and require explicit management.

Version requirements:
- Basic ENUM: PostgreSQL 8.3+
- Adding values: PostgreSQL 9.1+
"""

from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple

from rhosocial.activerecord.backend.expression.bases import BaseExpression

if TYPE_CHECKING:
    from rhosocial.activerecord.backend.dialect import SQLDialectBase

from ..statements import (
    CreateEnumTypeExpression,
    DropEnumTypeExpression,
    AlterEnumTypeAddValueExpression,
    AlterEnumTypeRenameValueExpression,
)


class PostgresEnumType(BaseExpression):
    """PostgreSQL ENUM type reference expression.

    This class represents a PostgreSQL ENUM type reference as a SQL expression.
    It is used in column definitions and type casts, NOT for DDL operations.

    For DDL operations (CREATE TYPE, DROP TYPE, etc.), use:
    - CreateEnumTypeExpression
    - DropEnumTypeExpression
    - AlterEnumTypeAddValueExpression
    - AlterEnumTypeRenameValueExpression

    PostgreSQL ENUM types are custom types created with CREATE TYPE.
    They are reusable across tables, unlike MySQL's inline ENUM.

    Attributes:
        name: Enum type name
        values: List of allowed values
        schema: Optional schema name

    Examples:
        >>> from rhosocial.activerecord.backend.impl.postgres import PostgresDialect
        >>> dialect = PostgresDialect()
        >>> status_enum = PostgresEnumType(
        ...     dialect=dialect,
        ...     name='video_status',
        ...     values=['pending', 'processing', 'ready', 'failed']
        ... )
        >>> status_enum.to_sql()
        ('video_status', ())

        >>> # With schema
        >>> status_enum = PostgresEnumType(
        ...     dialect=dialect,
        ...     name='video_status',
        ...     values=['draft', 'published'],
        ...     schema='app'
        ... )
        >>> status_enum.to_sql()
        ('app.video_status', ())
    """

    def __init__(self, dialect: "SQLDialectBase", name: str, values: List[str], schema: Optional[str] = None):
        """Initialize PostgreSQL ENUM type reference expression.

        Args:
            dialect: SQL dialect instance
            name: Enum type name
            values: List of allowed values
            schema: Optional schema name

        Raises:
            ValueError: If name is empty or values is empty
        """
        super().__init__(dialect)

        if not name:
            raise ValueError("Enum type name cannot be empty")
        if not values:
            raise ValueError("ENUM must have at least one value")
        if len(values) != len(set(values)):
            raise ValueError("ENUM values must be unique")

        self._name = name
        self._values = list(values)
        self._schema = schema

    @property
    def name(self) -> str:
        """Enum type name."""
        return self._name

    @property
    def values(self) -> List[str]:
        """List of allowed values."""
        return list(self._values)

    @property
    def schema(self) -> Optional[str]:
        """Schema name, if any."""
        return self._schema

    def to_sql(self) -> Tuple[str, tuple]:
        """Generate SQL type reference for use in column definitions.

        Returns:
            Tuple of (SQL string, empty params tuple)

        Examples:
            >>> status_enum.to_sql()
            ('video_status', ())

            >>> # With schema
            >>> status_enum.to_sql()
            ('app.video_status', ())
        """
        if self._schema:
            return (f"{self._schema}.{self._name}", ())
        return (self._name, ())

    def validate_value(self, value: str) -> bool:
        """Check if a value is valid for this enum.

        Args:
            value: Value to validate

        Returns:
            True if value is valid
        """
        return value in self._values

    @classmethod
    def from_python_enum(
        cls, dialect: "SQLDialectBase", enum_class: type, name: Optional[str] = None, schema: Optional[str] = None
    ) -> "PostgresEnumType":
        """Create PostgresEnumType from Python Enum class.

        Args:
            dialect: SQL dialect instance
            enum_class: Python Enum class
            name: Optional type name (defaults to enum class name in lowercase)
            schema: Optional schema name

        Returns:
            PostgresEnumType instance
        """
        from enum import Enum

        if not issubclass(enum_class, Enum):
            raise TypeError("enum_class must be a Python Enum class")

        type_name = name or enum_class.__name__.lower()
        values = [e.name for e in enum_class]
        return cls(dialect=dialect, name=type_name, values=values, schema=schema)

    def __str__(self) -> str:
        return self.to_sql()[0]

    def __repr__(self) -> str:
        return (
            f"PostgresEnumType(dialect={self._dialect!r}, name={self._name!r}, "
            f"values={self._values!r}, schema={self._schema!r})"
        )

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, PostgresEnumType):
            return NotImplemented
        return self._name == other._name and self._values == other._values and self._schema == other._schema

    def __hash__(self) -> int:
        return hash((self._name, tuple(self._values), self._schema))


class EnumTypeManager:
    """Manager for PostgreSQL enum type lifecycle.

    This class provides utilities for managing the creation, modification,
    and deletion of PostgreSQL enum types.
    """

    def __init__(self, backend):
        """Initialize with a database backend.

        Args:
            backend: PostgreSQL backend instance
        """
        self._backend = backend
        self._enum_types: Dict[str, Any] = {}

    def register(self, enum_type: Any) -> None:
        """Register an enum type for management.

        Args:
            enum_type: PostgresEnumType instance
        """
        key = enum_type.to_sql()[0]  # Get the SQL string from tuple
        self._enum_types[key] = enum_type

    def create_type(self, enum_type: PostgresEnumType, if_not_exists: bool = False) -> None:
        """Create an enum type in the database.

        Args:
            enum_type: PostgresEnumType instance
            if_not_exists: Use IF NOT EXISTS clause
        """
        expr = CreateEnumTypeExpression(
            dialect=self._backend.dialect,
            name=enum_type.name,
            values=enum_type.values,
            schema=enum_type.schema,
            if_not_exists=if_not_exists,
        )
        sql, params = expr.to_sql()
        self._backend.execute(sql, params)
        self.register(enum_type)

    def drop_type(self, enum_type: PostgresEnumType, if_exists: bool = False, cascade: bool = False) -> None:
        """Drop an enum type from the database.

        Args:
            enum_type: PostgresEnumType instance
            if_exists: Use IF EXISTS clause
            cascade: Use CASCADE clause
        """
        expr = DropEnumTypeExpression(
            dialect=self._backend.dialect,
            name=enum_type.name,
            schema=enum_type.schema,
            if_exists=if_exists,
            cascade=cascade,
        )
        sql, params = expr.to_sql()
        self._backend.execute(sql, params)
        key = enum_type.to_sql()[0]
        if key in self._enum_types:
            del self._enum_types[key]

    def add_value(
        self, enum_type: PostgresEnumType, new_value: str, before: Optional[str] = None, after: Optional[str] = None
    ) -> None:
        """Add a new value to an enum type.

        Note: Requires PostgreSQL 9.1+

        Args:
            enum_type: PostgresEnumType instance
            new_value: New value to add
            before: Add before this value
            after: Add after this value
        """
        if new_value in enum_type.values:
            raise ValueError(f"Value '{new_value}' already exists in enum")

        expr = AlterEnumTypeAddValueExpression(
            dialect=self._backend.dialect,
            type_name=enum_type.name,
            new_value=new_value,
            schema=enum_type.schema,
            before=before,
            after=after,
        )
        sql, params = expr.to_sql()
        self._backend.execute(sql, params)
        enum_type._values.append(new_value)

    def rename_value(self, enum_type: PostgresEnumType, old_value: str, new_value: str) -> None:
        """Rename a value in an enum type.

        Note: Requires PostgreSQL 9.1+

        Args:
            enum_type: PostgresEnumType instance
            old_value: Current value name
            new_value: New value name
        """
        if old_value not in enum_type.values:
            raise ValueError(f"Value '{old_value}' not found in enum")

        expr = AlterEnumTypeRenameValueExpression(
            dialect=self._backend.dialect,
            type_name=enum_type.name,
            old_value=old_value,
            new_value=new_value,
            schema=enum_type.schema,
        )
        sql, params = expr.to_sql()
        self._backend.execute(sql, params)
        # Update the values list
        idx = enum_type._values.index(old_value)
        enum_type._values[idx] = new_value

    def type_exists(self, name: str, schema: Optional[str] = None) -> bool:
        """Check if an enum type exists in the database.

        Args:
            name: Type name
            schema: Optional schema name

        Returns:
            True if type exists
        """
        where_clause = "typname = %s"
        params = [name]

        if schema:
            where_clause += " AND n.nspname = %s"
            params.append(schema)

        sql = f"""
        SELECT EXISTS (
            SELECT 1
            FROM pg_type t
            JOIN pg_namespace n ON t.typnamespace = n.oid
            WHERE {where_clause}
        )
        """
        result = self._backend.fetch_one(sql, tuple(params))
        return result[list(result.keys())[0]] if result else False

    def get_type_values(self, name: str, schema: Optional[str] = None) -> Optional[List[str]]:
        """Get the values of an enum type from the database.

        Args:
            name: Type name
            schema: Optional schema name

        Returns:
            List of enum values, or None if type doesn't exist
        """
        where_clause = "t.typname = %s"
        params = [name]

        if schema:
            where_clause += " AND n.nspname = %s"
            params.append(schema)

        sql = f"""
        SELECT e.enumlabel
        FROM pg_type t
        JOIN pg_namespace n ON t.typnamespace = n.oid
        JOIN pg_enum e ON t.oid = e.enumtypid
        WHERE {where_clause}
        ORDER BY e.enumsortorder
        """
        results = self._backend.fetch_all(sql, tuple(params))
        return [r["enumlabel"] for r in results] if results else None


__all__ = ["PostgresEnumType", "EnumTypeManager"]
