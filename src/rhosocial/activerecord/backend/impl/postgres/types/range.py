# src/rhosocial/activerecord/backend/impl/postgres/types/range.py
"""
PostgreSQL range types representation.

This module provides Python classes for PostgreSQL range types:
- PostgresRange: Represents a contiguous range of values
- PostgresMultirange: Represents an ordered list of non-overlapping ranges (PostgreSQL 14+)

Supported range types:
- int4range, int8range: Integer ranges
- numrange: Numeric ranges
- tsrange, tstzrange: Timestamp ranges
- daterange: Date ranges

For type adapters (conversion between Python and database),
see adapters.range module.
"""
from dataclasses import dataclass
from typing import Any, List, Optional


@dataclass
class PostgresRange:
    """PostgreSQL range type representation.

    PostgreSQL range types represent a contiguous range of values of a given
    element type (called the range's subtype). The range includes a lower
    bound and an upper bound.

    Attributes:
        lower: Lower bound value (None for unbounded below)
        upper: Upper bound value (None for unbounded above)
        lower_inc: True if lower bound is inclusive ('['), False if exclusive ('(')
        upper_inc: True if upper bound is inclusive (']'), False if exclusive (')')

    Examples:
        # Closed range [1, 10]
        PostgresRange(1, 10, lower_inc=True, upper_inc=False)

        # Unbounded range [1, infinity)
        PostgresRange(1, None)

        # Empty range
        PostgresRange.empty()
    """
    lower: Optional[Any] = None
    upper: Optional[Any] = None
    lower_inc: bool = True
    upper_inc: bool = False

    @classmethod
    def empty(cls) -> 'PostgresRange':
        """Create an empty range.

        Empty ranges have no bounds and contain no values.
        """
        return cls(lower=None, upper=None, lower_inc=False, upper_inc=False)

    @property
    def is_empty(self) -> bool:
        """Check if this is an empty range."""
        # An empty range is represented by having no bounds but being defined
        # In PostgreSQL, empty ranges are explicitly marked as 'empty'
        return (self.lower is None and self.upper is None and
                not self.lower_inc and not self.upper_inc)

    @property
    def is_unbounded_below(self) -> bool:
        """Check if the range has no lower bound."""
        return self.lower is None and not self.is_empty

    @property
    def is_unbounded_above(self) -> bool:
        """Check if the range has no upper bound."""
        return self.upper is None and not self.is_empty

    def __post_init__(self):
        """Validate range bounds after initialization."""
        # Empty range validation
        if self.lower is None and self.upper is None:
            if self.lower_inc or self.upper_inc:
                # This would be invalid - unbounded on both sides with inclusive bounds
                # Reset to empty range
                self.lower_inc = False
                self.upper_inc = False

    def to_postgres_string(self) -> str:
        """Convert to PostgreSQL range literal string.

        Returns:
            str: PostgreSQL range literal like '[1,10)' or 'empty'

        Examples:
            PostgresRange(1, 10).to_postgres_string() -> '[1,10)'
            PostgresRange(None, 10).to_postgres_string() -> '(,10)'
            PostgresRange.empty().to_postgres_string() -> 'empty'
        """
        if self.is_empty:
            return 'empty'

        # For unbounded ranges, use exclusive bracket
        lower_bracket = '[' if (self.lower_inc and self.lower is not None) else '('
        upper_bracket = ']' if (self.upper_inc and self.upper is not None) else ')'

        lower_str = '' if self.lower is None else str(self.lower)
        upper_str = '' if self.upper is None else str(self.upper)

        return f'{lower_bracket}{lower_str},{upper_str}{upper_bracket}'

    @classmethod
    def from_postgres_string(cls, value: str) -> 'PostgresRange':
        """Parse PostgreSQL range literal string.

        Args:
            value: PostgreSQL range literal like '[1,10)' or 'empty'

        Returns:
            PostgresRange: Parsed range object

        Raises:
            ValueError: If the string format is invalid
        """
        if not value:
            raise ValueError("Empty string is not a valid range")

        value = value.strip()

        if value.lower() == 'empty':
            return cls.empty()

        if len(value) < 3:
            raise ValueError(f"Range string too short: {value}")

        # Parse brackets
        lower_inc = value[0] == '['
        upper_inc = value[-1] == ']'

        if value[0] not in ('[', '('):
            raise ValueError(f"Invalid lower bound bracket: {value[0]}")
        if value[-1] not in (']', ')'):
            raise ValueError(f"Invalid upper bound bracket: {value[-1]}")

        # Extract content between brackets
        content = value[1:-1]

        # Find the comma that separates bounds
        # Need to handle quoted strings and nested structures
        comma_pos = _find_bound_separator(content)
        if comma_pos == -1:
            raise ValueError(f"No bound separator found in: {content}")

        lower_str = content[:comma_pos].strip()
        upper_str = content[comma_pos + 1:].strip()

        lower = None if lower_str == '' else lower_str
        upper = None if upper_str == '' else upper_str

        return cls(lower=lower, upper=upper,
                   lower_inc=lower_inc, upper_inc=upper_inc)

    def __contains__(self, value: Any) -> bool:
        """Check if a value is contained in the range.

        Note: This only works if the bound values are comparable with the
        given value. For proper containment checks, use SQL @> operator.
        """
        if self.is_empty:
            return False

        # Check lower bound
        if self.lower is not None:
            if self.lower_inc:
                if value < self.lower:
                    return False
            else:
                if value <= self.lower:
                    return False

        # Check upper bound
        if self.upper is not None:
            if self.upper_inc:
                if value > self.upper:
                    return False
            else:
                if value >= self.upper:
                    return False

        return True

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, PostgresRange):
            return NotImplemented
        return (self.lower == other.lower and
                self.upper == other.upper and
                self.lower_inc == other.lower_inc and
                self.upper_inc == other.upper_inc)

    def __hash__(self) -> int:
        return hash((self.lower, self.upper, self.lower_inc, self.upper_inc))

    def __repr__(self) -> str:
        if self.is_empty:
            return "PostgresRange.empty()"
        return f"PostgresRange(lower={self.lower!r}, upper={self.upper!r}, lower_inc={self.lower_inc}, upper_inc={self.upper_inc})"


def _find_bound_separator(content: str) -> int:
    """Find the comma that separates lower and upper bounds.

    This handles quoted strings and special characters properly.
    """
    depth = 0
    in_quotes = False
    quote_char = None

    for i, char in enumerate(content):
        if char in ('"', "'") and (i == 0 or content[i-1] != '\\'):
            if not in_quotes:
                in_quotes = True
                quote_char = char
            elif char == quote_char:
                in_quotes = False
                quote_char = None
        elif char in '([{':
            depth += 1
        elif char in ')]}':
            depth -= 1
        elif char == ',' and depth == 0 and not in_quotes:
            return i

    return -1


# Multirange support for PostgreSQL 14+
@dataclass
class PostgresMultirange:
    """PostgreSQL multirange type representation (PostgreSQL 14+).

    A multirange is an ordered list of non-overlapping ranges.

    Attributes:
        ranges: List of PostgresRange objects
    """
    ranges: List[PostgresRange]

    @classmethod
    def empty(cls) -> 'PostgresMultirange':
        """Create an empty multirange."""
        return cls(ranges=[])

    @property
    def is_empty(self) -> bool:
        """Check if this multirange contains no ranges."""
        return len(self.ranges) == 0

    def to_postgres_string(self) -> str:
        """Convert to PostgreSQL multirange literal string.

        Returns:
            str: PostgreSQL multirange literal like '{[1,5), [10,15)}'
        """
        if not self.ranges:
            return '{}'

        range_strs = [r.to_postgres_string() for r in self.ranges]
        return '{' + ', '.join(range_strs) + '}'

    @classmethod
    def from_postgres_string(cls, value: str) -> 'PostgresMultirange':
        """Parse PostgreSQL multirange literal string.

        Args:
            value: PostgreSQL multirange literal like '{[1,5), [10,15)}'

        Returns:
            PostgresMultirange: Parsed multirange object
        """
        value = value.strip()

        if value == '{}':
            return cls.empty()

        if not value.startswith('{') or not value.endswith('}'):
            raise ValueError(f"Invalid multirange format: {value}")

        content = value[1:-1]

        # Parse individual ranges
        ranges = []
        current_range = ''
        depth = 0

        for char in content:
            if char in ('[', '('):
                depth += 1
                current_range += char
            elif char in (']', ')'):
                depth -= 1
                current_range += char
            elif char == ',' and depth == 0:
                # Separator between ranges
                ranges.append(PostgresRange.from_postgres_string(current_range.strip()))
                current_range = ''
            else:
                current_range += char

        if current_range.strip():
            ranges.append(PostgresRange.from_postgres_string(current_range.strip()))

        return cls(ranges=ranges)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, PostgresMultirange):
            return NotImplemented
        return self.ranges == other.ranges

    def __repr__(self) -> str:
        if not self.ranges:
            return "PostgresMultirange.empty()"
        return f"PostgresMultirange(ranges={self.ranges!r})"


__all__ = ['PostgresRange', 'PostgresMultirange']
