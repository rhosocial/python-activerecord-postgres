# src/rhosocial/activerecord/backend/impl/postgres/protocols/types/enum.py
"""PostgreSQL ENUM type support protocol definition.

This module defines the protocol for PostgreSQL-specific ENUM type management.
"""

from typing import Protocol, runtime_checkable, List, Optional, Tuple, Union, TYPE_CHECKING

if TYPE_CHECKING:
    from ...expression.ddl.type import (
        PostgresCreateEnumTypeExpression,
        PostgresDropEnumTypeExpression,
        PostgresAlterEnumAddValueExpression,
    )


@runtime_checkable
class PostgresEnumTypeSupport(Protocol):
    """PostgreSQL ENUM type management protocol.

    PostgreSQL ENUM types are custom types created with CREATE TYPE.
    They are reusable across tables and require explicit lifecycle management.

    Version requirements:
    - Basic ENUM: PostgreSQL 8.3+
    - Adding values: PostgreSQL 9.1+
    - IF NOT EXISTS: PostgreSQL 9.1+
    """

    def create_enum_type(
        self, name: str, values: List[str], schema: Optional[str] = None, if_not_exists: bool = False
    ) -> str:
        """Generate CREATE TYPE statement for enum.

        Args:
            name: Enum type name
            values: List of allowed values
            schema: Optional schema name
            if_not_exists: Add IF NOT EXISTS clause (PG 9.1+)

        Returns:
            SQL statement string
        """
        ...

    def drop_enum_type(
        self, name: str, schema: Optional[str] = None, if_exists: bool = False, cascade: bool = False
    ) -> str:
        """Generate DROP TYPE statement for enum.

        Args:
            name: Enum type name
            schema: Optional schema name
            if_exists: Add IF EXISTS clause
            cascade: Add CASCADE clause

        Returns:
            SQL statement string
        """
        ...

    def alter_enum_add_value(
        self,
        type_name: str,
        new_value: str,
        schema: Optional[str] = None,
        before: Optional[str] = None,
        after: Optional[str] = None,
    ) -> str:
        """Generate ALTER TYPE ADD VALUE statement.

        Note: Requires PostgreSQL 9.1+

        Args:
            type_name: Enum type name
            new_value: New value to add
            schema: Optional schema name
            before: Add before this value
            after: Add after this value

        Returns:
            SQL statement string
        """
        ...

    def format_enum_type_name(self, name: str, schema: Optional[str] = None) -> str:
        """Format enum type name with optional schema.

        Args:
            name: Type name
            schema: Optional schema name

        Returns:
            Formatted type name (e.g., 'schema.name' or 'name')
        """
        ...

    def format_enum_values(self, values: List[str]) -> str:
        """Format enum values list for SQL.

        Args:
            values: List of enum values

        Returns:
            SQL-formatted values string
        """
        ...

    def format_create_enum_type(
        self,
        expr_or_name: Union["PostgresCreateEnumTypeExpression", str],
        values: Optional[List[str]] = None,
        schema: Optional[str] = None,
        if_not_exists: bool = False,
    ) -> Union[str, Tuple[str, tuple]]:
        """Format CREATE TYPE for either the legacy or expression-based API.

        Args:
            expr_or_name: PostgresCreateEnumTypeExpression instance or type name string
            values: List of allowed values (required when expr_or_name is a string)
            schema: Optional schema name
            if_not_exists: Add IF NOT EXISTS clause

        Returns:
            SQL statement string or tuple of (SQL string, params tuple)
        """
        ...

    def format_create_enum_type_raw(
        self, name: str, values: List[str], schema: Optional[str] = None, if_not_exists: bool = False
    ) -> str:
        """Format CREATE TYPE statement for enum.

        Args:
            name: Type name
            values: Allowed values
            schema: Optional schema
            if_not_exists: Add IF NOT EXISTS

        Returns:
            SQL statement string
        """
        ...

    def format_drop_enum_type(
        self,
        expr_or_name: Union["PostgresDropEnumTypeExpression", str],
        schema: Optional[str] = None,
        if_exists: bool = False,
        cascade: bool = False,
    ) -> Union[str, Tuple[str, tuple]]:
        """Format DROP TYPE for either the legacy or expression-based API.

        Args:
            expr_or_name: PostgresDropEnumTypeExpression instance or type name string
            schema: Optional schema name
            if_exists: Add IF EXISTS clause
            cascade: Add CASCADE clause

        Returns:
            SQL statement string or tuple of (SQL string, params tuple)
        """
        ...

    def format_drop_enum_type_raw(
        self, name: str, schema: Optional[str] = None, if_exists: bool = False, cascade: bool = False
    ) -> str:
        """Format DROP TYPE statement.

        Args:
            name: Type name
            schema: Optional schema
            if_exists: Add IF EXISTS
            cascade: Add CASCADE

        Returns:
            SQL statement string
        """
        ...

    def format_alter_enum_add_value(
        self,
        expr_or_type_name: Union["PostgresAlterEnumAddValueExpression", str],
        new_value: Optional[str] = None,
        schema: Optional[str] = None,
        before: Optional[str] = None,
        after: Optional[str] = None,
    ) -> Union[str, Tuple[str, tuple]]:
        """Format ALTER TYPE ADD VALUE for either the legacy or expression-based API.

        Args:
            expr_or_type_name: PostgresAlterEnumAddValueExpression instance or type name string
            new_value: New value to add (required when expr_or_type_name is a string)
            schema: Optional schema name
            before: Add before this value
            after: Add after this value

        Returns:
            SQL statement string or tuple of (SQL string, params tuple)
        """
        ...

    def format_alter_enum_add_value_raw(
        self, type_name: str, new_value: str, schema: Optional[str] = None,
        before: Optional[str] = None, after: Optional[str] = None
    ) -> str:
        """Format ALTER TYPE ADD VALUE statement.

        Args:
            type_name: Type name
            new_value: New value to add
            schema: Optional schema
            before: Add before this value
            after: Add after this value

        Returns:
            SQL statement string
        """
        ...

    def format_alter_enum_type_add_value(self, expr) -> Tuple[str, tuple]:
        """Format ALTER TYPE ADD VALUE statement from expression object.

        Args:
            expr: PostgresAlterEnumTypeAddValueExpression instance

        Returns:
            Tuple of (SQL string, empty params tuple)
        """
        ...

    def format_alter_enum_type_rename_value(self, expr) -> Tuple[str, tuple]:
        """Format ALTER TYPE RENAME VALUE statement from expression object.

        Args:
            expr: PostgresAlterEnumTypeRenameValueExpression instance

        Returns:
            Tuple of (SQL string, empty params tuple)
        """
        ...
