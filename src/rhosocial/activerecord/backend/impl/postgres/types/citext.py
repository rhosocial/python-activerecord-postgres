# src/rhosocial/activerecord/backend/impl/postgres/types/citext.py
"""
PostgreSQL citext type definition.

This module provides the PostgresCitext data class for representing
PostgreSQL citext case-insensitive character string values in Python.

citext provides a case-insensitive character string type that behaves
like TEXT but with case-insensitive comparison semantics.

PostgreSQL Documentation: https://www.postgresql.org/docs/current/citext.html

The citext extension must be installed:
    CREATE EXTENSION IF NOT EXISTS citext;
"""

from dataclasses import dataclass


@dataclass(frozen=True)
class PostgresCitext:
    """PostgreSQL citext case-insensitive text type representation.

    Wraps a string value for use as a citext column value.

    Attributes:
        value: The underlying text value.

    Examples:
        >>> c = PostgresCitext("Hello World")
        >>> str(c)
        'Hello World'
        >>> PostgresCitext.from_postgres_string("Hello")
        PostgresCitext(value='Hello')
    """

    value: str

    def to_postgres_string(self) -> str:
        """Return the underlying text value."""
        return self.value

    @classmethod
    def from_postgres_string(cls, value: str) -> "PostgresCitext":
        """Create a PostgresCitext from a string value.

        Args:
            value: The text value.

        Returns:
            PostgresCitext instance.
        """
        return cls(value=value)

    def to_sql_literal(self) -> str:
        """Convert to SQL literal with type cast."""
        escaped = self.value.replace("'", "''")
        return f"'{escaped}'::citext"

    def __str__(self) -> str:
        return self.value

    def __repr__(self) -> str:
        return f"PostgresCitext(value={self.value!r})"

    def __eq__(self, other) -> bool:
        if isinstance(other, PostgresCitext):
            return self.value.lower() == other.value.lower()
        if isinstance(other, str):
            return self.value.lower() == other.lower()
        return NotImplemented

    def __hash__(self) -> int:
        return hash(self.value.lower())


__all__ = [
    "PostgresCitext",
]