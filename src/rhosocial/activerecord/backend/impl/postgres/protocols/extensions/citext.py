# src/rhosocial/activerecord/backend/impl/postgres/protocols/extensions/citext.py
"""citext extension protocol definition.

This module defines the protocol for citext case-insensitive text
functionality in PostgreSQL.
"""

from typing import Optional, Protocol, runtime_checkable


@runtime_checkable
class PostgresCitextSupport(Protocol):
    """citext case-insensitive text extension protocol.

    Feature Source: Extension support (requires citext extension)

    citext provides case-insensitive text data type:
    - Automatic case-insensitive comparisons
    - Lowercase normalization

    Extension Information:
    - Extension name: citext
    - Install command: CREATE EXTENSION citext;
    - Minimum version: 1.0
    - Documentation: https://www.postgresql.org/docs/current/citext.html
    """

    def supports_citext_type(self) -> bool:
        """Whether citext type is supported."""
        ...

    def format_citext_column(self, column_name: str, length: Optional[int] = None) -> str:
        """Format a citext column definition."""
        ...

    def format_citext_literal(self, value: str) -> str:
        """Format a citext type literal value."""
        ...