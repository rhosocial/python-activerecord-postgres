# src/rhosocial/activerecord/backend/impl/postgres/enum_type_manager.py
"""PostgreSQL ENUM type lifecycle management.

This module provides:
- EnumTypeManager for lifecycle management of PostgreSQL ENUM types

PostgreSQL Documentation: https://www.postgresql.org/docs/current/datatype-enum.html

PostgreSQL ENUM types are custom types created with CREATE TYPE.
They are reusable across tables and require explicit management.

Version requirements:
- Basic ENUM: PostgreSQL 8.3+
- Adding values: PostgreSQL 9.1+
"""

from typing import Any, Dict, List, Optional

from rhosocial.activerecord.backend.expression.statements.ddl_type import (
    AlterTypeExpression,
    CreateTypeExpression,
)

from .expression.ddl import (
    PostgresAddEnumValueAction,
    PostgresDropTypeExpression,
    PostgresEnumTypeDefinition,
    PostgresRenameEnumValueAction,
)
from .expression.enum_ import PostgresEnumType


class EnumTypeManager:
    """Manager for PostgreSQL enum type lifecycle.

    This class provides utilities for managing the creation, modification,
    and deletion of PostgreSQL enum types.
    """

    def __init__(self, backend: Any) -> None:
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
        expr = CreateTypeExpression(
            dialect=self._backend.dialect,
            type_name=enum_type.name,
            definition=PostgresEnumTypeDefinition(
                self._backend.dialect,
                enum_type.values,
            ),
            schema_name=enum_type.schema,
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
        expr = PostgresDropTypeExpression(
            dialect=self._backend.dialect,
            type_name=enum_type.name,
            schema_name=enum_type.schema,
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

        action = PostgresAddEnumValueAction(
            self._backend.dialect,
            new_value,
            before=before,
            after=after,
        )
        expr = AlterTypeExpression(
            dialect=self._backend.dialect,
            type_name=enum_type.name,
            actions=[action],
            schema_name=enum_type.schema,
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

        action = PostgresRenameEnumValueAction(
            self._backend.dialect,
            old_value,
            new_value,
        )
        expr = AlterTypeExpression(
            dialect=self._backend.dialect,
            type_name=enum_type.name,
            actions=[action],
            schema_name=enum_type.schema,
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
        where_clause = f"typname = {self._backend.dialect.p()}"
        params = [name]

        if schema:
            where_clause += f" AND n.nspname = {self._backend.dialect.p()}"
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
        where_clause = f"t.typname = {self._backend.dialect.p()}"
        params = [name]

        if schema:
            where_clause += f" AND n.nspname = {self._backend.dialect.p()}"
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



__all__ = ["EnumTypeManager"]
