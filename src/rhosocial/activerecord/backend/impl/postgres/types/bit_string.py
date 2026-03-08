# src/rhosocial/activerecord/backend/impl/postgres/types/bit_string.py
"""
PostgreSQL bit string type representation.

This module provides PostgresBitString for representing PostgreSQL
bit strings in Python.

Supported bit string types:
- bit(n): Fixed-length bit string
- bit varying(n) / varbit(n): Variable-length bit string

All bit string types are available since early PostgreSQL versions.

For type adapter (conversion between Python and database),
see adapters.bit_string.PostgresBitStringAdapter.
"""
from dataclasses import dataclass
from typing import Optional


@dataclass
class PostgresBitString:
    """PostgreSQL bit string type representation.

    PostgreSQL bit strings are strings of 1s and 0s used for bit masks
    and bit manipulation operations.

    Attributes:
        bits: String of '0' and '1' characters
        length: Fixed length (for bit type), None for variable length (varbit)

    Examples:
        PostgresBitString('10101010')  # Variable length
        PostgresBitString('1010', length=8)  # Fixed length, padded
    """
    bits: str
    length: Optional[int] = None

    def __post_init__(self):
        """Validate bit string."""
        if not all(c in '01' for c in self.bits):
            raise ValueError(f"Invalid bit string: {self.bits}. Must contain only 0s and 1s.")

        # Pad if fixed length is specified
        if self.length is not None and len(self.bits) < self.length:
            self.bits = self.bits.ljust(self.length, '0')

    def __str__(self) -> str:
        """Return bit string."""
        return self.bits

    def __int__(self) -> int:
        """Convert to integer."""
        return int(self.bits, 2) if self.bits else 0

    def __len__(self) -> int:
        """Return length of bit string."""
        return len(self.bits)

    def __eq__(self, other: object) -> bool:
        if isinstance(other, PostgresBitString):
            return self.bits == other.bits and self.length == other.length
        if isinstance(other, str):
            return self.bits == other
        if isinstance(other, int):
            return int(self) == other
        return NotImplemented

    def __hash__(self) -> int:
        return hash((self.bits, self.length))

    def to_postgres_string(self) -> str:
        """Convert to PostgreSQL bit string literal.

        Returns:
            PostgreSQL bit string literal like B'10101'
        """
        return f"B'{self.bits}'"

    @classmethod
    def from_postgres_string(cls, value: str) -> 'PostgresBitString':
        """Parse PostgreSQL bit string literal.

        Args:
            value: PostgreSQL bit string literal like '10101' or B'10101'

        Returns:
            PostgresBitString object
        """
        value = value.strip()

        # Handle B'...' format
        if value.upper().startswith("B'") and value.endswith("'"):
            bits = value[2:-1]
        else:
            # Plain bit string
            bits = value

        return cls(bits=bits)

    @classmethod
    def from_int(cls, value: int, length: Optional[int] = None) -> 'PostgresBitString':
        """Create bit string from integer.

        Args:
            value: Integer value
            length: Optional fixed length (will pad with zeros if needed)

        Returns:
            PostgresBitString object
        """
        if value < 0:
            raise ValueError("Bit string cannot represent negative integers")

        bits = bin(value)[2:]  # Remove '0b' prefix

        if length is not None:
            bits = bits.zfill(length)

        return cls(bits=bits, length=length)


__all__ = ['PostgresBitString']
