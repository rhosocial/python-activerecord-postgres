# src/rhosocial/activerecord/backend/impl/postgres/types.py
"""
PostgreSQL-specific type definitions and helpers.

This module provides type-safe helpers for defining PostgreSQL-specific data types
such as ENUM.
"""
from typing import List, Optional


class PostgresEnumType:
    """Helper class for defining PostgreSQL ENUM column types.

    PostgreSQL ENUM is a reusable type object created with CREATE TYPE.
    Unlike MySQL's inline ENUM, PostgreSQL ENUM can be shared across tables.

    Example:
        >>> status_enum = PostgresEnumType(
        ...     name='subtitle_video_status',
        ...     values=['pending', 'processing', 'ready', 'failed']
        ... )
        >>> status_enum.to_sql()
        'subtitle_video_status'

        >>> # With schema
        >>> status_enum = PostgresEnumType(
        ...     name='video_status',
        ...     values=['draft', 'published'],
        ...     schema='app'
        ... )
        >>> status_enum.to_sql()
        'app.video_status'
    """

    def __init__(
        self,
        name: str,
        values: List[str],
        schema: Optional[str] = None
    ):
        """
        Initialize PostgreSQL ENUM type definition.

        Args:
            name: Enum type name (e.g., 'video_status')
            values: List of allowed enum values (must have at least one value)
            schema: Optional schema name (e.g., 'public', 'app')

        Raises:
            ValueError: If values list is empty or name is empty
        """
        if not name:
            raise ValueError("Enum type name cannot be empty")
        if not values:
            raise ValueError("ENUM must have at least one value")

        self.name = name
        self.values = values
        self.schema = schema

    def to_sql(self) -> str:
        """
        Generate the SQL type reference for use in column definitions.

        Returns:
            SQL type name (e.g., 'video_status' or 'app.video_status')
        """
        if self.schema:
            return f"{self.schema}.{self.name}"
        return self.name

    def __str__(self) -> str:
        return self.to_sql()

    def __repr__(self) -> str:
        return f"PostgresEnumType(name={self.name!r}, values={self.values!r}, schema={self.schema!r})"
