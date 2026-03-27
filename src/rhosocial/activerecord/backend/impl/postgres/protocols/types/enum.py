# src/rhosocial/activerecord/backend/impl/postgres/protocols/types/enum.py
"""PostgreSQL ENUM type support protocol definition.

This module defines the protocol for PostgreSQL-specific ENUM type management.
"""

from typing import Protocol, runtime_checkable, List, Optional


@runtime_checkable
class EnumTypeSupport(Protocol):
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
