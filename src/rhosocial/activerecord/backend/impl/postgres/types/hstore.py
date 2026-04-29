# src/rhosocial/activerecord/backend/impl/postgres/types/hstore.py
"""
PostgreSQL hstore type definition.

This module provides the PostgresHstore data class for representing
PostgreSQL hstore key-value pairs in Python.

hstore is a PostgreSQL extension that stores key/value pairs within
a single PostgreSQL value.

PostgreSQL Documentation: https://www.postgresql.org/docs/current/hstore.html

The hstore extension must be installed:
    CREATE EXTENSION IF NOT EXISTS hstore;
"""

from dataclasses import dataclass
from typing import Dict, Optional, List


def _escape_hstore_value(value: str) -> str:
    """Escape a string value for hstore literal format.

    hstore uses the format: key=>"value" with special escaping rules.
    - Double quotes in values are escaped as \"
    - Backslashes in values are escaped as \\\\
    - NULL values are represented without quotes
    """
    escaped = value.replace('\\', '\\\\').replace('"', '\\"')
    return escaped


def _escape_hstore_key(key: str) -> str:
    """Escape a key for hstore literal format.

    Keys follow the same escaping rules as values.
    """
    escaped = key.replace('\\', '\\\\').replace('"', '\\"')
    return escaped


@dataclass(frozen=True)
class PostgresHstore:
    """Immutable representation of a PostgreSQL hstore value.

    Stores key-value pairs where both keys and values are strings.
    None values are supported to represent SQL NULL in the hstore.

    Attributes:
        data: Dictionary of key-value pairs. Values can be str or None.

    Examples:
        >>> h = PostgresHstore(data={"name": "Alice", "age": "30"})
        >>> h["name"]
        'Alice'

        >>> h = PostgresHstore.from_postgres_string('"name"=>"Alice", "age"=>"30"')
        >>> h.data
        {'name': 'Alice', 'age': '30'}
    """

    data: Dict[str, Optional[str]]

    def __post_init__(self):
        """Validate hstore data."""
        if self.data is None:
            object.__setattr__(self, 'data', {})

    def __getitem__(self, key: str) -> Optional[str]:
        """Access a value by key."""
        return self.data[key]

    def __contains__(self, key: str) -> bool:
        """Check if a key exists in the hstore."""
        return key in self.data

    def __len__(self) -> int:
        """Return the number of key-value pairs."""
        return len(self.data)

    def __bool__(self) -> bool:
        """Return True if the hstore is non-empty."""
        return bool(self.data)

    def keys(self) -> List[str]:
        """Return all keys."""
        return list(self.data.keys())

    def values(self) -> List[Optional[str]]:
        """Return all values."""
        return list(self.data.values())

    def items(self):
        """Return all key-value pairs."""
        return self.data.items()

    def to_postgres_string(self) -> str:
        """Convert to PostgreSQL hstore literal string.

        Returns:
            hstore literal string suitable for SQL queries.
            Format: 'key1=>"value1", key2=>"value2"'

        Examples:
            >>> h = PostgresHstore(data={"a": "1", "b": "2"})
            >>> h.to_postgres_string()
            '"a"=>"1", "b"=>"2"'
        """
        if not self.data:
            return ""

        pairs = []
        for key, value in self.data.items():
            escaped_key = _escape_hstore_key(key)
            if value is None:
                pairs.append(f'"{escaped_key}"=>NULL')
            else:
                escaped_value = _escape_hstore_value(value)
                pairs.append(f'"{escaped_key}"=>"{escaped_value}"')

        return ", ".join(pairs)

    @classmethod
    def from_postgres_string(cls, value: str) -> "PostgresHstore":
        """Parse a PostgreSQL hstore string representation.

        Handles both the text output format from PostgreSQL and
        the literal input format.

        Args:
            value: hstore string from PostgreSQL (e.g., '"a"=>"1", "b"=>"2"')

        Returns:
            PostgresHstore instance

        Raises:
            ValueError: If the string cannot be parsed

        Examples:
            >>> h = PostgresHstore.from_postgres_string('"name"=>"Alice"')
            >>> h.data
            {'name': 'Alice'}
        """
        if not value or not value.strip():
            return cls(data={})

        data = {}
        i = 0
        s = value.strip()

        while i < len(s):
            # Skip whitespace and commas
            while i < len(s) and s[i] in (' ', ','):
                i += 1

            if i >= len(s):
                break

            # Parse key (must start with ")
            if s[i] != '"':
                raise ValueError(f"Expected '\"' at position {i}, got '{s[i]}'")
            i += 1

            key, i = _parse_quoted_string(s, i)

            # Skip whitespace
            while i < len(s) and s[i] == ' ':
                i += 1

            # Expect =>
            if i + 1 >= len(s) or s[i:i+2] != '=>':
                raise ValueError(f"Expected '=>' at position {i}")
            i += 2

            # Skip whitespace
            while i < len(s) and s[i] == ' ':
                i += 1

            # Parse value
            if i < len(s) and s[i] == '"':
                i += 1
                val, i = _parse_quoted_string(s, i)
            elif i + 3 < len(s) and s[i:i+4].upper() == 'NULL':
                val = None
                i += 4
            else:
                raise ValueError(f"Expected '\"' or 'NULL' at position {i}")

            data[key] = val

        return cls(data=data)

    def __eq__(self, other) -> bool:
        """Test equality."""
        if isinstance(other, PostgresHstore):
            return self.data == other.data
        if isinstance(other, dict):
            return self.data == other
        return NotImplemented

    def __hash__(self) -> int:
        """Hash based on frozenset of items for immutability."""
        return hash(frozenset(
            (k, v) for k, v in sorted(self.data.items())
        ))

    def __str__(self) -> str:
        """String representation (hstore literal format)."""
        return self.to_postgres_string()

    def __repr__(self) -> str:
        """Developer representation."""
        return f"PostgresHstore(data={self.data!r})"


def _parse_quoted_string(s: str, start: int):
    """Parse a double-quoted string with escape handling.

    Handles \\" and \\\\\\\\ escape sequences per hstore format.

    Args:
        s: The full string to parse
        start: Index right after the opening quote

    Returns:
        Tuple of (parsed string, next index after closing quote)
    """
    result = []
    i = start
    while i < len(s):
        if s[i] == '\\':
            if i + 1 < len(s):
                result.append(s[i + 1])
                i += 2
            else:
                result.append('\\')
                i += 1
        elif s[i] == '"':
            return ''.join(result), i + 1
        else:
            result.append(s[i])
            i += 1

    raise ValueError("Unterminated quoted string")
