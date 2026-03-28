# src/rhosocial/activerecord/backend/impl/postgres/types/pg_lsn.py
"""PostgreSQL pg_lsn type representation.

This module provides PostgresLsn for representing PostgreSQL
pg_lsn (Log Sequence Number) values in Python.

The pg_lsn type represents a position in the PostgreSQL Write-Ahead
Log (WAL). It is a 64-bit integer internally, displayed as two
hexadecimal numbers separated by a slash.

Format: XXXXXXXX/YYYYYYYY
- XXXXXXXX: Higher 4 bytes (WAL segment number)
- YYYYYYYY: Lower 4 bytes (byte offset within segment)

Examples:
    '16/B374D848' - Typical LSN format
    '0/0' - Minimum LSN value
    'FFFFFFFF/FFFFFFFF' - Maximum LSN value

Available since PostgreSQL 9.4.

Operations supported:
- Comparison: <, <=, >, >=, =, <>
- Subtraction: lsn1 - lsn2 (returns byte distance as int)
- Addition: lsn + bytes (returns new LSN)

For type adapter (conversion between Python and database),
see adapters.pg_lsn.PostgresLsnAdapter.
"""

from dataclasses import dataclass
import re


_LSN_PATTERN = re.compile(r"^([0-9A-Fa-f]+)/([0-9A-Fa-f]+)$")


@dataclass
class PostgresLsn:
    """PostgreSQL pg_lsn type representation.

    The pg_lsn (Log Sequence Number) represents a position in the
    Write-Ahead Log (WAL). Internally stored as a 64-bit integer.

    Attributes:
        value: The LSN value as a 64-bit integer

    Examples:
        PostgresLsn('16/B374D848')  # Parse from string
        PostgresLsn(123456789)  # Create from integer
        PostgresLsn.from_string('16/B374D848')
    """

    value: int

    def __post_init__(self):
        """Validate LSN value is within 64-bit range."""
        if not isinstance(self.value, int):
            raise TypeError(f"LSN value must be int, got {type(self.value).__name__}")
        if self.value < 0:
            raise ValueError(f"LSN value must be non-negative, got {self.value}")
        if self.value > 0xFFFFFFFFFFFFFFFF:
            raise ValueError(f"LSN value exceeds 64-bit range: {self.value}")

    @classmethod
    def from_string(cls, lsn_str: str) -> "PostgresLsn":
        """Parse LSN from string format (e.g., '16/B374D848').

        Args:
            lsn_str: LSN string in PostgreSQL format

        Returns:
            PostgresLsn instance

        Raises:
            ValueError: If string format is invalid
        """
        lsn_str = lsn_str.strip()
        match = _LSN_PATTERN.match(lsn_str)
        if not match:
            raise ValueError(f"Invalid LSN format: '{lsn_str}'. Expected format: XXXXXXXX/YYYYYYYY")

        high = int(match.group(1), 16)
        low = int(match.group(2), 16)
        value = (high << 32) | low
        return cls(value)

    def __str__(self) -> str:
        """Return LSN in PostgreSQL display format."""
        high = (self.value >> 32) & 0xFFFFFFFF
        low = self.value & 0xFFFFFFFF
        return f"{high:X}/{low:08X}"

    def __repr__(self) -> str:
        return f"PostgresLsn('{self!s}')"

    def __eq__(self, other: object) -> bool:
        if isinstance(other, PostgresLsn):
            return self.value == other.value
        if isinstance(other, int):
            return self.value == other
        if isinstance(other, str):
            try:
                other_lsn = PostgresLsn.from_string(other)
                return self.value == other_lsn.value
            except ValueError:
                return False
        return NotImplemented

    def __ne__(self, other: object) -> bool:
        result = self.__eq__(other)
        if result is NotImplemented:
            return NotImplemented
        return not result

    def __lt__(self, other: object) -> bool:
        if isinstance(other, PostgresLsn):
            return self.value < other.value
        if isinstance(other, int):
            return self.value < other
        return NotImplemented

    def __le__(self, other: object) -> bool:
        if isinstance(other, PostgresLsn):
            return self.value <= other.value
        if isinstance(other, int):
            return self.value <= other
        return NotImplemented

    def __gt__(self, other: object) -> bool:
        if isinstance(other, PostgresLsn):
            return self.value > other.value
        if isinstance(other, int):
            return self.value > other
        return NotImplemented

    def __ge__(self, other: object) -> bool:
        if isinstance(other, PostgresLsn):
            return self.value >= other.value
        if isinstance(other, int):
            return self.value >= other
        return NotImplemented

    def __sub__(self, other: object) -> int:
        """Subtract two LSNs to get byte distance.

        In PostgreSQL, LSN - LSN returns the number of bytes between
        the two positions in the WAL.

        Args:
            other: Another PostgresLsn or integer

        Returns:
            Integer byte distance between LSNs

        Raises:
            TypeError: If other is not PostgresLsn or int
        """
        if isinstance(other, PostgresLsn):
            return self.value - other.value
        if isinstance(other, int):
            return self.value - other
        return NotImplemented

    def __rsub__(self, other: object) -> int:
        """Reverse subtraction (int - LSN)."""
        if isinstance(other, int):
            return other - self.value
        return NotImplemented

    def __add__(self, other: object) -> "PostgresLsn":
        """Add bytes to LSN to get new position.

        In PostgreSQL, LSN + integer returns a new LSN position.

        Args:
            other: Integer number of bytes to add

        Returns:
            New PostgresLsn at offset position

        Raises:
            TypeError: If other is not an integer
            ValueError: If result exceeds 64-bit range
        """
        if isinstance(other, int):
            new_value = self.value + other
            if new_value < 0:
                raise ValueError("LSN addition would result in negative value")
            if new_value > 0xFFFFFFFFFFFFFFFF:
                raise ValueError("LSN addition exceeds 64-bit range")
            return PostgresLsn(new_value)
        return NotImplemented

    def __radd__(self, other: object) -> "PostgresLsn":
        """Reverse addition (int + LSN)."""
        return self.__add__(other)

    def __hash__(self) -> int:
        return hash(self.value)

    def __int__(self) -> int:
        """Return LSN as 64-bit integer."""
        return self.value

    @property
    def high(self) -> int:
        """Return the higher 32 bits (segment number)."""
        return (self.value >> 32) & 0xFFFFFFFF

    @property
    def low(self) -> int:
        """Return the lower 32 bits (byte offset)."""
        return self.value & 0xFFFFFFFF


__all__ = ["PostgresLsn"]
