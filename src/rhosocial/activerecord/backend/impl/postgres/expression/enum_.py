# src/rhosocial/activerecord/backend/impl/postgres/expression/enum_.py
"""PostgreSQL ENUM type reference expression.

This module provides:
- PostgresEnumType: Type reference expression for ENUM types
- Enum utility functions (enum_range, enum_first, enum_last, etc.)

PostgreSQL Documentation: https://www.postgresql.org/docs/current/datatype-enum.html

PostgreSQL ENUM types are custom types created with CREATE TYPE.
They are reusable across tables.

Version requirements:
- Basic ENUM: PostgreSQL 8.3+
- Adding values: PostgreSQL 9.1+
"""

from typing import TYPE_CHECKING, List, Optional

from rhosocial.activerecord.backend.expression.bases import BaseExpression

if TYPE_CHECKING:
    from rhosocial.activerecord.backend.dialect import SQLDialectBase


class PostgresEnumType(BaseExpression):
    """PostgreSQL ENUM type reference expression.

    This class represents a PostgreSQL ENUM type reference as a SQL expression.
    It is used in column definitions and type casts, NOT for DDL operations.

    For DDL operations (CREATE TYPE, DROP TYPE, etc.), use:
    - PostgresCreateEnumTypeExpression
    - PostgresDropEnumTypeExpression
    - PostgresAlterEnumTypeAddValueExpression
    - PostgresAlterEnumTypeRenameValueExpression

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

    @property
    def format_method(self) -> str:
        """The dialect formatting method that renders this expression."""
        return "format_enum_type_expression"

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
        if self._schema:
            return f"{self._schema}.{self._name}"
        return self._name

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


__all__ = ["PostgresEnumType"]
