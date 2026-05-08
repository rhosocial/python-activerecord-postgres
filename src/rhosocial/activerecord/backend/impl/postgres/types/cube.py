# src/rhosocial/activerecord/backend/impl/postgres/types/cube.py
"""
PostgreSQL cube type definition.

This module provides the PostgresCube data class for representing
PostgreSQL cube multidimensional cube values in Python.

The cube extension provides a data type for representing multidimensional
points and cubes (hypercubes), enabling spatial containment and distance queries.

PostgreSQL Documentation: https://www.postgresql.org/docs/current/cube.html

The cube extension must be installed:
    CREATE EXTENSION IF NOT EXISTS cube;
"""

from dataclasses import dataclass
from typing import List, Optional, Tuple


@dataclass(frozen=True)
class PostgresCube:
    """PostgreSQL cube multidimensional cube type representation.

    Represents either a point (single coordinate set) or a bounding box
    (two coordinate sets defining opposite corners of a hyperrectangle).

    Attributes:
        lower: Lower-left corner coordinates (for a point, same as upper).
        upper: Upper-right corner coordinates (for a point, same as lower).
            None indicates a point (only lower is meaningful).

    Examples:
        >>> c = PostgresCube([1.0, 2.0, 3.0])
        >>> str(c)
        '(1.0, 2.0, 3.0)'
        >>> c = PostgresCube([1.0, 1.0], [3.0, 4.0])
        >>> str(c)
        '(1.0, 1.0),(3.0, 4.0)'
        >>> PostgresCube.from_postgres_string('(1,2),(3,4)')
        PostgresCube(lower=[1.0, 2.0], upper=[3.0, 4.0])
    """

    lower: List[float]
    upper: Optional[List[float]] = None

    def __post_init__(self):
        if not self.lower:
            raise ValueError("Cube must have at least one dimension")
        if self.upper is not None and len(self.lower) != len(self.upper):
            raise ValueError(
                f"Lower corner has {len(self.lower)} dimensions, "
                f"upper corner has {len(self.upper)} dimensions"
            )

    @property
    def dimensions(self) -> int:
        """Number of dimensions."""
        return len(self.lower)

    @property
    def is_point(self) -> bool:
        """Whether this cube represents a single point."""
        return self.upper is None

    def to_postgres_string(self) -> str:
        """Convert to PostgreSQL cube literal string.

        Returns:
            String in format ``(x1, x2, ...)`` for points or
            ``(x1, x2, ...),(y1, y2, ...)`` for boxes.
        """
        lower_str = f"({', '.join(str(v) for v in self.lower)})"
        if self.upper is not None:
            upper_str = f"({', '.join(str(v) for v in self.upper)})"
            return f"{lower_str},{upper_str}"
        return lower_str

    @classmethod
    def from_postgres_string(cls, value: str) -> "PostgresCube":
        """Parse a PostgreSQL cube string representation.

        Args:
            value: Cube string from PostgreSQL (e.g., '(1,2),(3,4)' or '(1,2,3)').

        Returns:
            PostgresCube instance.

        Raises:
            ValueError: If the string cannot be parsed.

        Examples:
            >>> PostgresCube.from_postgres_string('(1,2,3)')
            PostgresCube(lower=[1.0, 2.0, 3.0], upper=None)
            >>> PostgresCube.from_postgres_string('(1,2),(3,4)')
            PostgresCube(lower=[1.0, 2.0], upper=[3.0, 4.0])
        """
        value = value.strip()
        if not value.startswith("("):
            raise ValueError(f"Invalid cube format: {value!r}")

        def parse_tuple(s: str, start: int) -> Tuple[List[float], int]:
            if start >= len(s) or s[start] != '(':
                raise ValueError(f"Expected '(' at position {start}")
            i = start + 1
            parts = []
            current = []
            while i < len(s) and s[i] != ')':
                if s[i] == ',':
                    if current:
                        parts.append(float(''.join(current)))
                        current = []
                    i += 1
                elif s[i] in (' ', '\t'):
                    i += 1
                else:
                    current.append(s[i])
                    i += 1
            if s[i] != ')':
                raise ValueError("Unterminated tuple")
            if current:
                parts.append(float(''.join(current)))
            return parts, i + 1

        lower, pos = parse_tuple(value, 0)
        upper = None
        if pos < len(value) and value[pos] == ',':
            upper, _ = parse_tuple(value, pos + 1)

        return cls(lower=lower, upper=upper)

    def to_sql_literal(self) -> str:
        """Convert to SQL literal with type cast.

        Returns:
            String in format ``'(1,2,3)'::cube``
        """
        return f"'{self.to_postgres_string()}'::cube"

    def __str__(self) -> str:
        return self.to_postgres_string()

    def __repr__(self) -> str:
        if self.upper is not None:
            return f"PostgresCube(lower={self.lower}, upper={self.upper})"
        return f"PostgresCube(lower={self.lower})"

    def __eq__(self, other) -> bool:
        if isinstance(other, PostgresCube):
            return self.lower == other.lower and self.upper == other.upper
        if isinstance(other, str):
            return self.to_postgres_string() == other
        return NotImplemented

    def __hash__(self) -> int:
        upper_tuple = tuple(self.upper) if self.upper else None
        return hash((tuple(self.lower), upper_tuple))


__all__ = [
    "PostgresCube",
]