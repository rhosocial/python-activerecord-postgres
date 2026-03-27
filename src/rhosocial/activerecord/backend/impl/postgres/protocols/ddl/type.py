# src/rhosocial/activerecord/backend/impl/postgres/protocols/ddl/type.py
"""PostgreSQL type DDL protocol.

This module defines the protocol for PostgreSQL-specific type DDL features
that extend beyond standard SQL.
"""

from typing import Protocol, runtime_checkable, Optional, Tuple, List


@runtime_checkable
class PostgresTypeSupport(Protocol):
    """PostgreSQL type DDL protocol.

    Feature Source: Native support (no extension required)

    PostgreSQL supports user-defined types:
    - ENUM types (CREATE TYPE ... AS ENUM)
    - Composite types (CREATE TYPE ... AS)
    - Range types
    - Base types (with C functions)

    This protocol focuses on ENUM type DDL support.

    Official Documentation:
    - CREATE TYPE: https://www.postgresql.org/docs/current/sql-createtype.html
    - DROP TYPE: https://www.postgresql.org/docs/current/sql-droptype.html

    Version Requirements:
    - All versions support ENUM types
    """

    def supports_create_type(self) -> bool:
        """Whether CREATE TYPE is supported.

        Native feature, all versions.
        """
        ...

    def supports_drop_type(self) -> bool:
        """Whether DROP TYPE is supported.

        Native feature, all versions.
        """
        ...

    def supports_type_if_not_exists(self) -> bool:
        """Whether CREATE TYPE IF NOT EXISTS is supported.

        PostgreSQL does NOT support IF NOT EXISTS for CREATE TYPE.
        Always returns False.
        """
        ...

    def supports_type_if_exists(self) -> bool:
        """Whether DROP TYPE IF EXISTS is supported.

        Native feature, all versions.
        """
        ...

    def supports_type_cascade(self) -> bool:
        """Whether DROP TYPE CASCADE is supported.

        Native feature, all versions.
        """
        ...

    def format_create_type_enum_statement(
        self, name: str, values: List[str], schema: Optional[str] = None
    ) -> Tuple[str, tuple]:
        """Format CREATE TYPE ... AS ENUM statement.

        Args:
            name: Type name
            values: List of enum values
            schema: Optional schema name

        Returns:
            Tuple of (SQL string, parameters tuple)
        """
        ...

    def format_drop_type_statement(
        self, name: str, schema: Optional[str] = None, if_exists: bool = False, cascade: bool = False
    ) -> Tuple[str, tuple]:
        """Format DROP TYPE statement.

        Args:
            name: Type name
            schema: Optional schema name
            if_exists: Whether to add IF EXISTS
            cascade: Whether to add CASCADE

        Returns:
            Tuple of (SQL string, parameters tuple)
        """
        ...
