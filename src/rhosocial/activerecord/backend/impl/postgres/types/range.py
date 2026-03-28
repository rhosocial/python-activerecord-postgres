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

Design Decisions:
-----------------

Why create our own PostgresRange instead of using psycopg.types.range.Range?

1. Vendor Independence: By using our own class, we avoid tight coupling to psycopg.
   Users can switch database drivers without rewriting their domain models.

2. Dataclass Benefits: PostgresRange is a @dataclass, providing:
   - Immutability by default (frozen=True can be added)
   - Automatic __eq__, __hash__, __repr__ generation
   - Clear, declarative syntax
   - Easy serialization/deserialization

3. No External Dependencies: PostgresRange works without psycopg installed,
   making it suitable for code that doesn't directly interact with the database.

4. Adapter Pattern: The adapter layer (adapters.range) handles conversion
   between PostgresRange and psycopg.types.range.Range, keeping concerns separated.

Comparison with Other Approaches:
- Python Standard Library (ipaddress, uuid): No vendor lock-in, directly usable
- psycopg.types.range.Range: Tightly coupled to psycopg driver
- Our PostgresRange: Driver-agnostic, adapter-based conversion

Usage Recommendations:
----------------------

1. **Recommended**: Use PostgresRange for Python code:
   ```python
   from rhosocial.activerecord.backend.impl.postgres.types import PostgresRange
   range_obj = PostgresRange(1, 10)
   ```

2. **Advanced**: Use psycopg.adapt.Range directly if you need psycopg-specific features:
   ```python
   from psycopg.types.range import Range
   # Will be passed through without conversion
   ```

3. **Conversion**: Use PostgresRange.from_psycopg_range() when needed:
   ```python
   postgres_range = PostgresRange.from_psycopg_range(psycopg_range)
   ```

See Also:
- adapters.range: Conversion between PostgresRange and database values
- psycopg.types.range: psycopg's native Range implementation
"""

from dataclasses import dataclass
from typing import Any, List, Optional


@dataclass
class PostgresRange:
    """PostgreSQL range type representation.

    PostgreSQL range types represent a contiguous range of values of a given
    element type (called the range's subtype). The range includes a lower
    bound and an upper bound.

    This class is driver-agnostic and can be used without psycopg installed.
    For conversion to/from psycopg's Range type, use the from_psycopg_range()
    and to_psycopg_range() methods.

    Attributes:
        lower: Lower bound value (None for unbounded below)
        upper: Upper bound value (None for unbounded above)
        lower_inc: True if lower bound is inclusive ('['), False if exclusive ('(')
        upper_inc: True if upper bound is inclusive (']'), False if exclusive (')')

    Examples:
        # Closed range [1, 10)
        PostgresRange(1, 10, lower_inc=True, upper_inc=False)

        # Unbounded range [1, infinity)
        PostgresRange(1, None)

        # Empty range
        PostgresRange.empty()

        # Convert from psycopg Range
        PostgresRange.from_psycopg_range(psycopg_range)

        # Convert to psycopg Range
        postgres_range.to_psycopg_range()
    """

    lower: Optional[Any] = None
    upper: Optional[Any] = None
    lower_inc: bool = True
    upper_inc: bool = False

    @classmethod
    def empty(cls) -> "PostgresRange":
        """Create an empty range.

        Empty ranges have no bounds and contain no values.
        """
        return cls(lower=None, upper=None, lower_inc=False, upper_inc=False)

    @property
    def is_empty(self) -> bool:
        """Check if this is an empty range."""
        # An empty range is represented by having no bounds but being defined
        # In PostgreSQL, empty ranges are explicitly marked as 'empty'
        return self.lower is None and self.upper is None and not self.lower_inc and not self.upper_inc

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
            return "empty"

        # For unbounded ranges, use exclusive bracket
        lower_bracket = "[" if (self.lower_inc and self.lower is not None) else "("
        upper_bracket = "]" if (self.upper_inc and self.upper is not None) else ")"

        lower_str = "" if self.lower is None else str(self.lower)
        upper_str = "" if self.upper is None else str(self.upper)

        return f"{lower_bracket}{lower_str},{upper_str}{upper_bracket}"

    @classmethod
    def from_postgres_string(cls, value: str) -> "PostgresRange":
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

        if value.lower() == "empty":
            return cls.empty()

        if len(value) < 3:
            raise ValueError(f"Range string too short: {value}")

        # Parse brackets
        lower_inc = value[0] == "["
        upper_inc = value[-1] == "]"

        if value[0] not in ("[", "("):
            raise ValueError(f"Invalid lower bound bracket: {value[0]}")
        if value[-1] not in ("]", ")"):
            raise ValueError(f"Invalid upper bound bracket: {value[-1]}")

        # Extract content between brackets
        content = value[1:-1]

        # Find the comma that separates bounds
        # Need to handle quoted strings and nested structures
        comma_pos = _find_bound_separator(content)
        if comma_pos == -1:
            raise ValueError(f"No bound separator found in: {content}")

        lower_str = content[:comma_pos].strip()
        upper_str = content[comma_pos + 1 :].strip()

        lower = None if lower_str == "" else lower_str
        upper = None if upper_str == "" else upper_str

        return cls(lower=lower, upper=upper, lower_inc=lower_inc, upper_inc=upper_inc)

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
        return (
            self.lower == other.lower
            and self.upper == other.upper
            and self.lower_inc == other.lower_inc
            and self.upper_inc == other.upper_inc
        )

    def __hash__(self) -> int:
        return hash((self.lower, self.upper, self.lower_inc, self.upper_inc))

    def __repr__(self) -> str:
        if self.is_empty:
            return "PostgresRange.empty()"
        return (
            f"PostgresRange(lower={self.lower!r}, upper={self.upper!r}, "
            f"lower_inc={self.lower_inc}, upper_inc={self.upper_inc})"
        )

    @classmethod
    def from_psycopg_range(cls, psycopg_range: Any) -> "PostgresRange":
        """Convert from psycopg.types.range.Range to PostgresRange.

        This method creates a PostgresRange instance from a psycopg Range object.
        It's useful when you receive Range objects directly from psycopg and want
        to work with a driver-agnostic representation.

        Args:
            psycopg_range: A psycopg.types.range.Range instance

        Returns:
            PostgresRange: A new PostgresRange instance

        Raises:
            ImportError: If psycopg is not installed
            TypeError: If the argument is not a psycopg Range

        Examples:
            >>> from psycopg.types.range import Range
            >>> psycopg_range = Range(1, 10, '[')
            >>> postgres_range = PostgresRange.from_psycopg_range(psycopg_range)
            >>> postgres_range.lower
            1
            >>> postgres_range.upper
            10
            >>> postgres_range.lower_inc
            True
            >>> postgres_range.upper_inc
            False

        Note:
            psycopg uses a bounds string like '[)' where:
            - '[' means lower bound is inclusive
            - ')' means upper bound is exclusive
        """
        try:
            from psycopg.types.range import Range
        except ImportError as e:
            raise ImportError(
                "psycopg is required to use from_psycopg_range(). Install it with: pip install psycopg[binary]"
            ) from e

        if not isinstance(psycopg_range, Range):
            raise TypeError(f"Expected psycopg.types.range.Range, got {type(psycopg_range).__name__}")

        lower_inc = bool(psycopg_range.bounds and psycopg_range.bounds[0] == "[")
        upper_inc = bool(psycopg_range.bounds and psycopg_range.bounds[1] == "]")

        return cls(lower=psycopg_range.lower, upper=psycopg_range.upper, lower_inc=lower_inc, upper_inc=upper_inc)

    def to_psycopg_range(self) -> Any:
        """Convert to psycopg.types.range.Range.

        This method creates a psycopg Range instance from this PostgresRange.
        It's useful when you need to pass range values to psycopg directly,
        for example in raw SQL queries or when using psycopg-specific features.

        Returns:
            psycopg.types.range.Range: A new Range instance

        Raises:
            ImportError: If psycopg is not installed

        Examples:
            >>> postgres_range = PostgresRange(1, 10, lower_inc=True, upper_inc=False)
            >>> psycopg_range = postgres_range.to_psycopg_range()
            >>> psycopg_range.lower
            1
            >>> psycopg_range.upper
            10
            >>> psycopg_range.bounds
            '[)'

        Note:
            This method requires psycopg to be installed. If you're writing
            database-agnostic code, avoid calling this method directly and
            use the adapter layer instead.
        """
        try:
            from psycopg.types.range import Range
        except ImportError as e:
            raise ImportError(
                "psycopg is required to use to_psycopg_range(). Install it with: pip install psycopg[binary]"
            ) from e

        if self.is_empty:
            return Range(empty=True)

        bounds = "[" if self.lower_inc else "("
        bounds += "]" if self.upper_inc else ")"

        return Range(self.lower, self.upper, bounds)


def _find_bound_separator(content: str) -> int:
    """Find the comma that separates lower and upper bounds.

    This handles quoted strings and special characters properly.
    """
    depth = 0
    in_quotes = False
    quote_char = None

    for i, char in enumerate(content):
        if char in ('"', "'") and (i == 0 or content[i - 1] != "\\"):
            if not in_quotes:
                in_quotes = True
                quote_char = char
            elif char == quote_char:
                in_quotes = False
                quote_char = None
        elif char in "([{":
            depth += 1
        elif char in ")]}":
            depth -= 1
        elif char == "," and depth == 0 and not in_quotes:
            return i

    return -1


# Multirange support for PostgreSQL 14+
@dataclass
class PostgresMultirange:
    """PostgreSQL multirange type representation (PostgreSQL 14+).

    A multirange is an ordered list of non-overlapping ranges.

    This class is driver-agnostic and can be used without psycopg installed.
    For conversion to/from psycopg's Multirange type, use the from_psycopg_multirange()
    and to_psycopg_multirange() methods.

    Attributes:
        ranges: List of PostgresRange objects

    Examples:
        # Create from ranges
        PostgresMultirange([PostgresRange(1, 5), PostgresRange(10, 15)])

        # Empty multirange
        PostgresMultirange.empty()

        # Convert from psycopg Multirange
        PostgresMultirange.from_psycopg_multirange(psycopg_multirange)

        # Convert to psycopg Multirange
        postgres_multirange.to_psycopg_multirange()
    """

    ranges: List[PostgresRange]

    @classmethod
    def empty(cls) -> "PostgresMultirange":
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
            return "{}"

        range_strs = [r.to_postgres_string() for r in self.ranges]
        return "{" + ", ".join(range_strs) + "}"

    @classmethod
    def from_postgres_string(cls, value: str) -> "PostgresMultirange":
        """Parse PostgreSQL multirange literal string.

        Args:
            value: PostgreSQL multirange literal like '{[1,5), [10,15)}'

        Returns:
            PostgresMultirange: Parsed multirange object
        """
        value = value.strip()

        if value == "{}":
            return cls.empty()

        if not value.startswith("{") or not value.endswith("}"):
            raise ValueError(f"Invalid multirange format: {value}")

        content = value[1:-1]

        # Parse individual ranges
        ranges = []
        current_range = ""
        depth = 0

        for char in content:
            if char in ("[", "("):
                depth += 1
                current_range += char
            elif char in ("]", ")"):
                depth -= 1
                current_range += char
            elif char == "," and depth == 0:
                # Separator between ranges
                ranges.append(PostgresRange.from_postgres_string(current_range.strip()))
                current_range = ""
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

    @classmethod
    def from_psycopg_multirange(cls, psycopg_multirange: Any) -> "PostgresMultirange":
        """Convert from psycopg.types.range.Multirange to PostgresMultirange.

        This method creates a PostgresMultirange instance from a psycopg Multirange object.

        Note: Multirange support was added in psycopg 3.2+. If using an older version,
        this method will raise ImportError.

        Args:
            psycopg_multirange: A psycopg.types.range.Multirange instance

        Returns:
            PostgresMultirange: A new PostgresMultirange instance

        Raises:
            ImportError: If psycopg is not installed or version < 3.2
            TypeError: If the argument is not a psycopg Multirange

        Examples:
            >>> from psycopg.types.range import Multirange, Range
            >>> psycopg_mr = Multirange([Range(1, 5, '[)'), Range(10, 15, '[)')])
            >>> postgres_mr = PostgresMultirange.from_psycopg_multirange(psycopg_mr)
        """
        try:
            from psycopg.types.range import Multirange
        except ImportError as e:
            raise ImportError(
                "psycopg 3.2+ is required to use from_psycopg_multirange(). "
                "Multirange support was added in psycopg 3.2. "
                "Install it with: pip install psycopg[binary]>=3.2"
            ) from e

        if not isinstance(psycopg_multirange, Multirange):
            raise TypeError(f"Expected psycopg.types.range.Multirange, got {type(psycopg_multirange).__name__}")

        ranges = [PostgresRange.from_psycopg_range(r) for r in psycopg_multirange]

        return cls(ranges=ranges)

    def to_psycopg_multirange(self) -> Any:
        """Convert to psycopg.types.range.Multirange.

        This method creates a psycopg Multirange instance from this PostgresMultirange.

        Note: Multirange support was added in psycopg 3.2+. If using an older version,
        this method will raise ImportError.

        Returns:
            psycopg.types.range.Multirange: A new Multirange instance

        Raises:
            ImportError: If psycopg is not installed or version < 3.2

        Examples:
            >>> mr = PostgresMultirange([PostgresRange(1, 5), PostgresRange(10, 15)])
            >>> psycopg_mr = mr.to_psycopg_multirange()
        """
        try:
            from psycopg.types.range import Multirange
        except ImportError as e:
            raise ImportError(
                "psycopg 3.2+ is required to use to_psycopg_multirange(). "
                "Multirange support was added in psycopg 3.2. "
                "Install it with: pip install psycopg[binary]>=3.2"
            ) from e

        psycopg_ranges = [r.to_psycopg_range() for r in self.ranges]
        return Multirange(psycopg_ranges)


__all__ = ["PostgresRange", "PostgresMultirange"]
