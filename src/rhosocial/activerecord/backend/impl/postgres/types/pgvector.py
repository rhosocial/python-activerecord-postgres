# src/rhosocial/activerecord/backend/impl/postgres/types/pgvector.py
"""
PostgreSQL pgvector vector type.

This module provides the PostgresVector data class for representing
pgvector vector values in Python, enabling type-safe vector operations.

pgvector Documentation: https://github.com/pgvector/pgvector

The vector type is provided by the pgvector extension.
Install with: CREATE EXTENSION IF NOT EXISTS vector;
"""

from dataclasses import dataclass
from typing import List, Optional


@dataclass(frozen=True)
class PostgresVector:
    """PostgreSQL pgvector vector type representation.

    Stores a vector as a list of float values with optional dimension
    validation. This type maps to the PostgreSQL ``vector`` type provided
    by the pgvector extension.

    Attributes:
        values: List of float values representing the vector components.
        dimensions: Number of dimensions. When not provided, automatically
            inferred from the length of values.

    Examples:
        >>> v = PostgresVector([1.0, 2.0, 3.0])
        >>> v.values
        [1.0, 2.0, 3.0]
        >>> v.dimensions
        3
        >>> v.to_postgres_string()
        '[1.0, 2.0, 3.0]'
        >>> PostgresVector.from_postgres_string('[1.0,2.0,3.0]')
        PostgresVector(values=[1.0, 2.0, 3.0], dimensions=3)
    """

    values: List[float]
    dimensions: Optional[int] = None

    def __post_init__(self):
        if not self.values:
            raise ValueError("Vector must have at least one dimension")
        if self.dimensions is not None and len(self.values) != self.dimensions:
            raise ValueError(
                f"Expected {self.dimensions} dimensions, got {len(self.values)}"
            )
        if self.dimensions is None:
            object.__setattr__(self, 'dimensions', len(self.values))

    def to_postgres_string(self) -> str:
        """Convert to PostgreSQL vector literal format.

        Returns:
            String in format ``[v1, v2, v3]``

        Examples:
            >>> PostgresVector([1.0, 2.0, 3.0]).to_postgres_string()
            '[1.0, 2.0, 3.0]'
        """
        values_str = ", ".join(str(v) for v in self.values)
        return f"[{values_str}]"

    @classmethod
    def from_postgres_string(cls, value: str) -> "PostgresVector":
        """Parse from PostgreSQL vector string format.

        Args:
            value: String in format ``[v1,v2,v3]`` (as returned by PostgreSQL)

        Returns:
            PostgresVector instance

        Raises:
            ValueError: If the string format is invalid

        Examples:
            >>> PostgresVector.from_postgres_string('[1.0,2.0,3.0]')
            PostgresVector(values=[1.0, 2.0, 3.0], dimensions=3)
        """
        value = value.strip()
        if not value.startswith("[") or not value.endswith("]"):
            raise ValueError(f"Invalid vector format: {value!r}")

        inner = value[1:-1].strip()
        if not inner:
            raise ValueError("Empty vector")

        parts = inner.split(",")
        try:
            values = [float(p.strip()) for p in parts]
        except ValueError as e:
            raise ValueError(f"Invalid vector component: {e}") from e

        return cls(values=values)

    def to_sql_literal(self) -> str:
        """Convert to SQL literal with type cast.

        Returns:
            String in format ``[v1, v2, v3]::vector(N)``

        Examples:
            >>> PostgresVector([1.0, 2.0, 3.0]).to_sql_literal()
            '[1.0, 2.0, 3.0]::vector(3)'
        """
        return f"{self.to_postgres_string()}::vector({self.dimensions})"

    def __str__(self) -> str:
        return self.to_postgres_string()

    def __repr__(self) -> str:
        return f"PostgresVector(values={self.values}, dimensions={self.dimensions})"

    def __eq__(self, other) -> bool:
        if isinstance(other, PostgresVector):
            return self.values == other.values
        if isinstance(other, list):
            return self.values == other
        return NotImplemented

    def __hash__(self) -> int:
        return hash(tuple(self.values))

    def __len__(self) -> int:
        return len(self.values)


__all__ = [
    "PostgresVector",
]
