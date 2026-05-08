# src/rhosocial/activerecord/backend/impl/postgres/types/ltree.py
"""
PostgreSQL ltree, lquery, and ltxtquery type definitions.

This module provides data classes for representing PostgreSQL ltree
hierarchical label path values, lquery pattern values, and ltxtquery
full-text query values in Python.

The ltree extension provides a hierarchical label tree data type for
representing tree-like structures such as organizational charts,
category trees, and file system paths.

PostgreSQL Documentation: https://www.postgresql.org/docs/current/ltree.html

The ltree extension must be installed:
    CREATE EXTENSION IF NOT EXISTS ltree;
"""

from dataclasses import dataclass
from typing import List, Optional


def _validate_ltree_label(label: str) -> None:
    """Validate a single ltree label.

    ltree labels must contain only alphanumeric characters
    and underscores (a-z, A-Z, 0-9, _).
    """
    if not label:
        raise ValueError("Empty label in ltree path")
    for ch in label:
        if not (ch.isalnum() or ch == '_'):
            raise ValueError(
                f"Invalid character {ch!r} in ltree label {label!r}"
            )


@dataclass(frozen=True)
class PostgresLtree:
    """PostgreSQL ltree hierarchical label path type representation.

    Stores a dot-separated path of labels representing a position
    in a hierarchical tree structure.

    Attributes:
        labels: List of label strings representing the path components.

    Examples:
        >>> path = PostgresLtree(["Top", "Science", "Astronomy"])
        >>> str(path)
        'Top.Science.Astronomy'
        >>> PostgresLtree.from_postgres_string("Top.Science.Astronomy")
        PostgresLtree(labels=['Top', 'Science', 'Astronomy'])
    """

    labels: List[str]

    def __post_init__(self):
        if not self.labels:
            raise ValueError("ltree path must have at least one label")
        for label in self.labels:
            _validate_ltree_label(label)

    def to_postgres_string(self) -> str:
        """Convert to PostgreSQL ltree literal string.

        Returns:
            Dot-separated label path string.

        Examples:
            >>> PostgresLtree(["Top", "Science"]).to_postgres_string()
            'Top.Science'
        """
        return ".".join(self.labels)

    @classmethod
    def from_postgres_string(cls, value: str) -> "PostgresLtree":
        """Parse a PostgreSQL ltree string representation.

        Args:
            value: Dot-separated label path string.

        Returns:
            PostgresLtree instance.

        Raises:
            ValueError: If the string is empty or contains invalid labels.

        Examples:
            >>> PostgresLtree.from_postgres_string("Top.Science.Astronomy")
            PostgresLtree(labels=['Top', 'Science', 'Astronomy'])
        """
        value = value.strip()
        if not value:
            raise ValueError("Cannot parse empty ltree string")
        labels = value.split(".")
        return cls(labels=labels)

    def to_sql_literal(self) -> str:
        """Convert to SQL literal with type cast.

        Returns:
            String in format ``'Top.Science'::ltree``
        """
        return f"'{self.to_postgres_string()}'::ltree"

    @property
    def nlevel(self) -> int:
        """Number of labels in the path."""
        return len(self.labels)

    def subpath(self, start: int, length: Optional[int] = None) -> "PostgresLtree":
        """Extract a subpath from this ltree value.

        Args:
            start: Starting position (0-indexed).
            length: Optional number of labels to extract.

        Returns:
            New PostgresLtree with the subpath.

        Raises:
            IndexError: If start or length is out of range.
        """
        if length is not None:
            return PostgresLtree(labels=self.labels[start:start + length])
        return PostgresLtree(labels=self.labels[start:])

    def is_ancestor_of(self, other: "PostgresLtree") -> bool:
        """Check if this path is an ancestor of (or equal to) another.

        Args:
            other: Another PostgresLtree to compare against.

        Returns:
            True if this path is an ancestor of other.
        """
        if len(self.labels) > len(other.labels):
            return False
        return self.labels == other.labels[:len(self.labels)]

    def is_descendant_of(self, other: "PostgresLtree") -> bool:
        """Check if this path is a descendant of (or equal to) another.

        Args:
            other: Another PostgresLtree to compare against.

        Returns:
            True if this path is a descendant of other.
        """
        return other.is_ancestor_of(self)

    def __str__(self) -> str:
        return self.to_postgres_string()

    def __repr__(self) -> str:
        return f"PostgresLtree(labels={self.labels})"

    def __eq__(self, other) -> bool:
        if isinstance(other, PostgresLtree):
            return self.labels == other.labels
        if isinstance(other, str):
            return self.to_postgres_string() == other
        return NotImplemented

    def __hash__(self) -> int:
        return hash(tuple(self.labels))

    def __len__(self) -> int:
        return len(self.labels)


@dataclass(frozen=True)
class PostgresLquery:
    """PostgreSQL lquery pattern type representation.

    lquery represents a pattern for matching ltree values.

    Attributes:
        pattern: The lquery pattern string.

    Examples:
        >>> q = PostgresLquery("*.Astronomy.*")
        >>> q.to_postgres_string()
        '*.Astronomy.*'
    """

    pattern: str

    def __post_init__(self):
        if not self.pattern.strip():
            raise ValueError("lquery pattern cannot be empty")

    def to_postgres_string(self) -> str:
        """Return the lquery pattern string."""
        return self.pattern

    def to_sql_literal(self) -> str:
        """Convert to SQL literal with type cast."""
        return f"'{self.pattern}'::lquery"

    def __str__(self) -> str:
        return self.pattern

    def __repr__(self) -> str:
        return f"PostgresLquery(pattern={self.pattern!r})"

    def __eq__(self, other) -> bool:
        if isinstance(other, PostgresLquery):
            return self.pattern == other.pattern
        if isinstance(other, str):
            return self.pattern == other
        return NotImplemented

    def __hash__(self) -> int:
        return hash(self.pattern)


@dataclass(frozen=True)
class PostgresLtxtquery:
    """PostgreSQL ltxtquery full-text query type representation.

    ltxtquery represents a full-text search query for ltree values.

    Attributes:
        query: The ltxtquery string.

    Examples:
        >>> q = PostgresLtxtquery("Science & Astronomy")
        >>> q.to_postgres_string()
        'Science & Astronomy'
    """

    query: str

    def __post_init__(self):
        if not self.query.strip():
            raise ValueError("ltxtquery cannot be empty")

    def to_postgres_string(self) -> str:
        """Return the ltxtquery string."""
        return self.query

    def to_sql_literal(self) -> str:
        """Convert to SQL literal with type cast."""
        return f"'{self.query}'::ltxtquery"

    def __str__(self) -> str:
        return self.query

    def __repr__(self) -> str:
        return f"PostgresLtxtquery(query={self.query!r})"

    def __eq__(self, other) -> bool:
        if isinstance(other, PostgresLtxtquery):
            return self.query == other.query
        if isinstance(other, str):
            return self.query == other
        return NotImplemented

    def __hash__(self) -> int:
        return hash(self.query)


__all__ = [
    "PostgresLtree",
    "PostgresLquery",
    "PostgresLtxtquery",
]