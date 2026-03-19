# src/rhosocial/activerecord/backend/impl/postgres/adapters/monetary.py
"""
PostgreSQL Monetary Types Adapter.

This module provides type adapters for PostgreSQL monetary types.

PostgreSQL Documentation: https://www.postgresql.org/docs/current/datatype-money.html

The money type stores a currency amount with a fixed fractional precision.
The range of money type is -92233720368547758.08 to +92233720368547758.07.
"""
from typing import Any, Dict, Optional, Set, Type

from ..types.monetary import PostgresMoney


class PostgresMoneyAdapter:
    """PostgreSQL MONEY type adapter.

    This adapter converts between Python PostgresMoney/Decimal and
    PostgreSQL MONEY values.

    Note: MONEY values from PostgreSQL come as strings with currency
    symbols and formatting based on locale. This adapter strips
    non-numeric characters and parses the value.

    **IMPORTANT: Only PostgresMoney type is registered by default.**

    Reason for not registering Decimal -> str:
    The base TypeRegistry already registers Decimal -> str mapping.
    Registering it again would cause a conflict. If you need to
    use Decimal directly with MONEY columns, use PostgresMoney wrapper
    or specify the adapter explicitly.

    To use with Decimal values:
    ```python
    from rhosocial.activerecord.backend.impl.postgres import PostgresMoney
    # Wrap Decimal in PostgresMoney
    money = PostgresMoney(Decimal('100.50'))
    ```
    """

    @property
    def supported_types(self) -> Dict[Type, Set[Type]]:
        """Return supported type mappings.

        Note: Only PostgresMoney is registered to avoid conflicts with
        the base Decimal adapter.
        """
        return {
            PostgresMoney: {str},
        }

    def to_database(
        self,
        value: Any,
        target_type: Type,
        options: Optional[Dict[str, Any]] = None
    ) -> Optional[str]:
        """Convert Python value to PostgreSQL MONEY.

        Args:
            value: PostgresMoney, Decimal, int, float, str, or None
            target_type: Target type
            options: Optional conversion options

        Returns:
            Numeric string for MONEY, or None
        """
        if value is None:
            return None

        if isinstance(value, PostgresMoney):
            return str(value.amount)

        from decimal import Decimal, InvalidOperation
        if isinstance(value, Decimal):
            return str(value)

        if isinstance(value, (int, float)):
            return str(Decimal(str(value)))

        if isinstance(value, str):
            try:
                Decimal(value)
                return value
            except (ValueError, InvalidOperation) as e:
                raise ValueError(f"Invalid money value: {value}") from None

        raise TypeError(f"Cannot convert {type(value).__name__} to MONEY")

    def from_database(
        self,
        value: Any,
        target_type: Type,
        options: Optional[Dict[str, Any]] = None
    ) -> Optional[PostgresMoney]:
        """Convert PostgreSQL MONEY value to Python.

        Args:
            value: MONEY value from database (usually string with formatting)
            target_type: Target Python type
            options: Optional conversion options

        Returns:
            PostgresMoney object, or None
        """
        if value is None:
            return None

        if isinstance(value, PostgresMoney):
            return value

        from decimal import Decimal
        if isinstance(value, Decimal):
            return PostgresMoney(value)

        if isinstance(value, (int, float)):
            return PostgresMoney(Decimal(str(value)))

        if isinstance(value, str):
            amount = self._parse_money_string(value)
            return PostgresMoney(amount)

        raise TypeError(f"Cannot convert {type(value).__name__} from MONEY")

    def _parse_money_string(self, value: str):
        """Parse a money string to Decimal.

        Handles various formats:
        - $1,234.56
        - €1.234,56 (European format)
        - -$1,234.56 (negative)
        - 1234.56 (plain number)

        Args:
            value: Money string from PostgreSQL

        Returns:
            Decimal amount
        """
        import re
        from decimal import Decimal, InvalidOperation

        value = value.strip()

        if not value:
            return Decimal('0')

        is_negative = '-' in value or '(' in value

        has_dot = '.' in value
        has_comma = ',' in value

        if has_dot and has_comma:
            dot_pos = value.rfind('.')
            comma_pos = value.rfind(',')

            if comma_pos > dot_pos:
                value = value.replace('.', '')
                value = value.replace(',', '.')
            else:
                value = value.replace(',', '')
        elif has_comma:
            parts = value.split(',')
            if len(parts) == 2 and len(parts[1]) <= 2:
                value = value.replace(',', '.')
            else:
                value = value.replace(',', '')

        MONEY_PATTERN = re.compile(r'[^\d.-]')
        cleaned = MONEY_PATTERN.sub('', value)

        if is_negative:
            cleaned = '-' + cleaned.replace('-', '')

        if not cleaned or cleaned == '-':
            return Decimal('0')

        try:
            return Decimal(cleaned)
        except (ValueError, InvalidOperation):
            raise ValueError(f"Cannot parse money value: {value}") from None

    def to_database_batch(
        self,
        values: list,
        target_type: Type,
        options: Optional[Dict[str, Any]] = None
    ) -> list:
        """Batch convert values to database format."""
        return [self.to_database(v, target_type, options) for v in values]

    def from_database_batch(
        self,
        values: list,
        target_type: Type,
        options: Optional[Dict[str, Any]] = None
    ) -> list:
        """Batch convert values from database format."""
        return [self.from_database(v, target_type, options) for v in values]


__all__ = ['PostgresMoneyAdapter']
