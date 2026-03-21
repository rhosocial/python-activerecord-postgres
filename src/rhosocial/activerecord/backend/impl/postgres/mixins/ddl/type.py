# Copyright (c) 2024 rhosocial.
# Licensed under the MIT License.
#
# PostgreSQL Type DDL Mixin Implementation
#
# This module provides PostgreSQL-specific type DDL functionality,
# including CREATE TYPE and DROP TYPE statement formatting.
#
# PostgreSQL supports user-defined types, particularly ENUM types.

from typing import Optional, Tuple, List


class PostgresTypeMixin:
    """PostgreSQL type DDL implementation.

    PostgreSQL supports user-defined types, particularly ENUM types.
    """

    def supports_create_type(self) -> bool:
        """CREATE TYPE is supported."""
        return True

    def supports_drop_type(self) -> bool:
        """DROP TYPE is supported."""
        return True

    def supports_type_if_not_exists(self) -> bool:
        """PostgreSQL does NOT support IF NOT EXISTS for CREATE TYPE."""
        return False

    def supports_type_if_exists(self) -> bool:
        """DROP TYPE IF EXISTS is supported."""
        return True

    def supports_type_cascade(self) -> bool:
        """DROP TYPE CASCADE is supported."""
        return True

    def format_create_type_enum_statement(
        self,
        name: str,
        values: List[str],
        schema: Optional[str] = None
    ) -> Tuple[str, tuple]:
        """Format CREATE TYPE ... AS ENUM statement.

        PostgreSQL syntax:
        CREATE TYPE [schema.]name AS ENUM ('value1', 'value2', ...);

        Args:
            name: Type name
            values: List of enum values
            schema: Optional schema name

        Returns:
            Tuple of (SQL string, parameters tuple)
        """
        if not values:
            raise ValueError("ENUM type must have at least one value")

        type_name = self.format_identifier(name)
        if schema:
            type_name = f"{self.format_identifier(schema)}.{type_name}"

        escaped_values = [f"'{v}'" for v in values]
        values_str = ", ".join(escaped_values)

        sql = f"CREATE TYPE {type_name} AS ENUM ({values_str})"
        return sql, ()

    def format_drop_type_statement(
        self,
        name: str,
        schema: Optional[str] = None,
        if_exists: bool = False,
        cascade: bool = False
    ) -> Tuple[str, tuple]:
        """Format DROP TYPE statement.

        PostgreSQL syntax:
        DROP TYPE [IF EXISTS] [schema.]name [CASCADE];

        Args:
            name: Type name
            schema: Optional schema name
            if_exists: Whether to add IF EXISTS
            cascade: Whether to add CASCADE

        Returns:
            Tuple of (SQL string, parameters tuple)
        """
        parts = ["DROP TYPE"]

        if if_exists:
            parts.append("IF EXISTS")

        type_name = self.format_identifier(name)
        if schema:
            type_name = f"{self.format_identifier(schema)}.{type_name}"
        parts.append(type_name)

        if cascade:
            parts.append("CASCADE")

        return " ".join(parts), ()
