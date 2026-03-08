# src/rhosocial/activerecord/backend/impl/postgres/types/monetary.py
"""
PostgreSQL MONEY type representation.

This module provides the PostgresMoney class for representing
PostgreSQL MONEY values in Python.

The MONEY type stores currency amounts with a fixed fractional precision.
The format is determined by the database's LC_MONETARY locale setting.

PostgreSQL Documentation: https://www.postgresql.org/docs/current/datatype-money.html

Available since early PostgreSQL versions.

For type adapter (conversion between Python and database),
see adapters.monetary.PostgresMoneyAdapter.
"""
from dataclasses import dataclass
from decimal import Decimal


@dataclass
class PostgresMoney:
    """PostgreSQL MONEY type representation.

    The MONEY type stores currency amounts with fixed precision.
    The actual display format depends on the database locale.

    Attributes:
        amount: The monetary amount as Decimal

    Examples:
        >>> from decimal import Decimal
        >>> PostgresMoney(Decimal('1234.56'))
        PostgresMoney(Decimal('1234.56'))
        >>> PostgresMoney(100.50)
        PostgresMoney(Decimal('100.50'))
        
        # Arithmetic operations
        >>> m1 = PostgresMoney(100)
        >>> m2 = PostgresMoney(50)
        >>> m1 + m2
        PostgresMoney(Decimal('150'))
        >>> m1 - m2
        PostgresMoney(Decimal('50'))
        >>> m1 * 2
        PostgresMoney(Decimal('200'))
    """
    amount: Decimal

    def __post_init__(self):
        """Ensure amount is Decimal."""
        if not isinstance(self.amount, Decimal):
            self.amount = Decimal(str(self.amount))

    def __str__(self) -> str:
        """Return string representation."""
        return str(self.amount)

    def __repr__(self) -> str:
        return f"PostgresMoney({self.amount!r})"

    def __eq__(self, other: object) -> bool:
        if isinstance(other, PostgresMoney):
            return self.amount == other.amount
        if isinstance(other, (Decimal, int, float, str)):
            return self.amount == Decimal(str(other))
        return NotImplemented

    def __lt__(self, other: object) -> bool:
        if isinstance(other, PostgresMoney):
            return self.amount < other.amount
        if isinstance(other, (Decimal, int, float)):
            return self.amount < Decimal(str(other))
        return NotImplemented

    def __le__(self, other: object) -> bool:
        if isinstance(other, PostgresMoney):
            return self.amount <= other.amount
        if isinstance(other, (Decimal, int, float)):
            return self.amount <= Decimal(str(other))
        return NotImplemented

    def __gt__(self, other: object) -> bool:
        if isinstance(other, PostgresMoney):
            return self.amount > other.amount
        if isinstance(other, (Decimal, int, float)):
            return self.amount > Decimal(str(other))
        return NotImplemented

    def __ge__(self, other: object) -> bool:
        if isinstance(other, PostgresMoney):
            return self.amount >= other.amount
        if isinstance(other, (Decimal, int, float)):
            return self.amount >= Decimal(str(other))
        return NotImplemented

    def __add__(self, other: object) -> 'PostgresMoney':
        if isinstance(other, PostgresMoney):
            return PostgresMoney(self.amount + other.amount)
        if isinstance(other, (Decimal, int, float)):
            return PostgresMoney(self.amount + Decimal(str(other)))
        return NotImplemented

    def __sub__(self, other: object) -> 'PostgresMoney':
        if isinstance(other, PostgresMoney):
            return PostgresMoney(self.amount - other.amount)
        if isinstance(other, (Decimal, int, float)):
            return PostgresMoney(self.amount - Decimal(str(other)))
        return NotImplemented

    def __mul__(self, other: object) -> 'PostgresMoney':
        # Money * number = money
        if isinstance(other, (Decimal, int, float)):
            return PostgresMoney(self.amount * Decimal(str(other)))
        return NotImplemented

    def __truediv__(self, other: object) -> Decimal:
        # Money / number = money (in PostgreSQL)
        # Money / money = number (ratio)
        if isinstance(other, PostgresMoney):
            return self.amount / other.amount
        if isinstance(other, (Decimal, int, float)):
            return self.amount / Decimal(str(other))
        return NotImplemented

    def __neg__(self) -> 'PostgresMoney':
        return PostgresMoney(-self.amount)

    def __abs__(self) -> 'PostgresMoney':
        return PostgresMoney(abs(self.amount))

    def __hash__(self) -> int:
        return hash(self.amount)


__all__ = ['PostgresMoney']
