# src/rhosocial/activerecord/backend/impl/postgres/mixins/types/enum.py
"""PostgreSQL ENUM type support mixin.

This module provides the EnumTypeMixin class for handling PostgreSQL
enumerated type operations.
"""

from typing import Optional, List, TYPE_CHECKING

if TYPE_CHECKING:
    pass


class EnumTypeMixin:
    """Mixin providing PostgreSQL ENUM type formatting methods.

    This mixin implements the EnumTypeSupport protocol.
    """

    def format_enum_type_name(self, name: str, schema: Optional[str] = None) -> str:
        """Format enum type name with optional schema.

        Args:
            name: Type name
            schema: Optional schema name

        Returns:
            Formatted type name (e.g., 'schema.name' or 'name')
        """
        if schema:
            return f"{schema}.{name}"
        return name

    def format_enum_values(self, values: List[str]) -> str:
        """Format enum values list for SQL.

        Args:
            values: List of enum values

        Returns:
            SQL-formatted values string
        """
        return ", ".join(f"'{v}'" for v in values)

    def format_create_enum_type(
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
        full_name = self.format_enum_type_name(name, schema)
        values_str = self.format_enum_values(values)
        exists_clause = "IF NOT EXISTS " if if_not_exists else ""
        return f"CREATE TYPE {exists_clause}{full_name} AS ENUM ({values_str})"

    def format_drop_enum_type(
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
        full_name = self.format_enum_type_name(name, schema)
        exists_clause = "IF EXISTS " if if_exists else ""
        cascade_clause = " CASCADE" if cascade else ""
        return f"DROP TYPE {exists_clause}{full_name}{cascade_clause}"

    def format_alter_enum_add_value(
        self,
        type_name: str,
        new_value: str,
        schema: Optional[str] = None,
        before: Optional[str] = None,
        after: Optional[str] = None,
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
        full_name = self.format_enum_type_name(type_name, schema)
        sql = f"ALTER TYPE {full_name} ADD VALUE '{new_value}'"
        if before:
            sql += f" BEFORE '{before}'"
        elif after:
            sql += f" AFTER '{after}'"
        return sql

    # =========================================================================
    # Convenience methods for EnumTypeSupport protocol
    # =========================================================================

    def create_enum_type(
        self, name: str, values: List[str], schema: Optional[str] = None, if_not_exists: bool = False
    ) -> str:
        """Generate CREATE TYPE statement for enum.

        Implements EnumTypeSupport protocol.

        Args:
            name: Enum type name
            values: List of allowed values
            schema: Optional schema name
            if_not_exists: Add IF NOT EXISTS clause (PG 9.1+)

        Returns:
            SQL statement string
        """
        return self.format_create_enum_type(name, values, schema, if_not_exists)

    def drop_enum_type(
        self, name: str, schema: Optional[str] = None, if_exists: bool = False, cascade: bool = False
    ) -> str:
        """Generate DROP TYPE statement for enum.

        Implements EnumTypeSupport protocol.

        Args:
            name: Enum type name
            schema: Optional schema name
            if_exists: Add IF EXISTS clause
            cascade: Add CASCADE clause

        Returns:
            SQL statement string
        """
        return self.format_drop_enum_type(name, schema, if_exists, cascade)

    def alter_enum_add_value(
        self,
        type_name: str,
        new_value: str,
        schema: Optional[str] = None,
        before: Optional[str] = None,
        after: Optional[str] = None,
    ) -> str:
        """Generate ALTER TYPE ADD VALUE statement.

        Implements EnumTypeSupport protocol.

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
        return self.format_alter_enum_add_value(type_name, new_value, schema, before, after)
