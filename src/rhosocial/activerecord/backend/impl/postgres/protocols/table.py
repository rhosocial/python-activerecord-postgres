# src/rhosocial/activerecord/backend/impl/postgres/protocols/table.py
"""PostgreSQL table extended features protocol.

This module defines the PostgresTableSupport protocol for features
exclusive to PostgreSQL table management.
"""

from typing import Protocol, runtime_checkable

from rhosocial.activerecord.backend.dialect.protocols import TableSupport


@runtime_checkable
class PostgresTableSupport(TableSupport, Protocol):
    """PostgreSQL table extended features protocol.

    PostgreSQL's table support includes exclusive features:
    - INHERITS table inheritance
    - TABLESPACE table-level storage
    - ON COMMIT control for temporary tables

    Version requirements:
    - INHERITS: All versions
    - TABLESPACE: All versions
    - ON COMMIT: PostgreSQL 8.0+

    Note: These features don't require additional plugins, they're part of
    PostgreSQL official distribution.
    """

    def supports_table_inheritance(self) -> bool:
        """Whether table inheritance (INHERITS) is supported.

        PostgreSQL supports table inheritance, where child tables inherit
        all columns from parent tables.
        Syntax: CREATE TABLE child (...) INHERITS (parent);
        """
        ...
