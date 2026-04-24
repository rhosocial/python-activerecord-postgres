# src/rhosocial/activerecord/backend/impl/postgres/protocols/extensions/orafce.py
"""orafce extension protocol definition.

This module defines the protocol for orafce Oracle compatibility
functionality in PostgreSQL.
"""

from typing import Optional, Protocol, runtime_checkable


@runtime_checkable
class PostgresOrafceSupport(Protocol):
    """orafce Oracle compatibility extension protocol.

    Feature Source: Extension support (requires orafce extension)

    orafce provides Oracle compatibility functions:
    - String functions
    - Date functions
    - Type conversions

    Extension Information:
    - Extension name: orafce
    - Install command: CREATE EXTENSION orafce;
    - Minimum version: 3.0
    - Documentation: https://github.com/orafce/orafce
    """

    def supports_orafce(self) -> bool:
        """Whether orafce extension is available."""
        ...

    def format_add_months(self, date_expr: str, months: int) -> str:
        """Format ADD_MONTHS function."""
        ...

    def format_last_day(self, date_expr: str) -> str:
        """Format LAST_DAY function."""
        ...

    def format_months_between(self, date1: str, date2: str) -> str:
        """Format MONTHS_BETWEEN function."""
        ...

    def format_next_day(self, date_expr: str, day: str) -> str:
        """Format NEXT_DAY function."""
        ...

    def format_nvl(self, expr1: str, expr2: str) -> str:
        """Format NVL function."""
        ...

    def format_nvl2(self, expr1: str, expr2: str, expr3: str) -> str:
        """Format NVL2 function."""
        ...

    def format_decode(
        self, expr: str, *matches: str, default: Optional[str] = None
    ) -> str:
        """Format DECODE function."""
        ...

    def format_trunc(self, value: str, format: Optional[str] = None) -> str:
        """Format TRUNC function."""
        ...

    def format_round(self, value: str, format: Optional[str] = None) -> str:
        """Format ROUND function."""
        ...

    def format_instr(
        self, string_expr: str, substring_expr: str, position: int = 1, occurrence: int = 1
    ) -> str:
        """Format INSTR function."""
        ...

    def format_substr(
        self, string_expr: str, position: int, length: Optional[int] = None
    ) -> str:
        """Format SUBSTR function."""
        ...