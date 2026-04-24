# src/rhosocial/activerecord/backend/impl/postgres/mixins/extensions/orafce.py
"""
orafce Oracle compatibility functionality implementation.

This module provides the PostgresOrafceMixin class that adds support for
orafce extension features.
"""

from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    pass


class PostgresOrafceMixin:
    """orafce Oracle compatibility functionality implementation."""

    def supports_orafce(self) -> bool:
        """Check if orafce extension is available."""
        return self.is_extension_installed("orafce")

    def format_add_months(self, date_expr: str, months: int) -> str:
        """Format ADD_MONTHS function.

        Args:
            date_expr: Date expression
            months: Number of months to add

        Returns:
            SQL ADD_MONTHS function
        """
        return f"ADD_MONTHS({date_expr}, {months})"

    def format_last_day(self, date_expr: str) -> str:
        """Format LAST_DAY function.

        Args:
            date_expr: Date expression

        Returns:
            SQL LAST_DAY function
        """
        return f"LAST_DAY({date_expr})"

    def format_months_between(self, date1: str, date2: str) -> str:
        """Format MONTHS_BETWEEN function.

        Args:
            date1: First date
            date2: Second date

        Returns:
            SQL MONTHS_BETWEEN function
        """
        return f"MONTHS_BETWEEN({date1}, {date2})"

    def format_next_day(self, date_expr: str, day: str) -> str:
        """Format NEXT_DAY function.

        Args:
            date_expr: Date expression
            day: Target day of week

        Returns:
            SQL NEXT_DAY function
        """
        return f"NEXT_DAY({date_expr}, '{day}')"

    def format_nvl(self, expr1: str, expr2: str) -> str:
        """Format NVL function.

        Args:
            expr1: Expression to check
            expr2: Default value

        Returns:
            SQL NVL function
        """
        return f"NVL({expr1}, {expr2})"

    def format_nvl2(self, expr1: str, expr2: str, expr3: str) -> str:
        """Format NVL2 function.

        Args:
            expr1: Expression to check
            expr2: Value if not null
            expr3: Value if null

        Returns:
            SQL NVL2 function
        """
        return f"NVL2({expr1}, {expr2}, {expr3})"

    def format_decode(
        self, expr: str, *matches: str, default: Optional[str] = None
    ) -> str:
        """Format DECODE function.

        Args:
            expr: Expression to evaluate
            matches: Pairs of (search, result)
            default: Optional default value

        Returns:
            SQL DECODE function
        """
        args = [expr]
        for m in matches:
            args.append(m)
        if default:
            args.append(default)
        return f"DECODE({', '.join(args)})"

    def format_trunc(self, value: str, format: Optional[str] = None) -> str:
        """Format TRUNC function.

        Args:
            value: Value to truncate
            format: Optional format

        Returns:
            SQL TRUNC function
        """
        if format:
            return f"TRUNC({value}, '{format}')"
        return f"TRUNC({value})"

    def format_round(self, value: str, format: Optional[str] = None) -> str:
        """Format ROUND function.

        Args:
            value: Value to round
            format: Optional format

        Returns:
            SQL ROUND function
        """
        if format:
            return f"ROUND({value}, '{format}')"
        return f"ROUND({value})"

    def format_instr(
        self, string_expr: str, substring_expr: str, position: int = 1, occurrence: int = 1
    ) -> str:
        """Format INSTR function.

        Args:
            string_expr: String to search in
            substring_expr: Substring to find
            position: Starting position (1-based)
            occurrence: Which occurrence to find

        Returns:
            SQL INSTR function
        """
        return f"INSTR({string_expr}, {substring_expr}, {position}, {occurrence})"

    def format_substr(
        self, string_expr: str, position: int, length: Optional[int] = None
    ) -> str:
        """Format SUBSTR function.

        Args:
            string_expr: String expression
            position: Starting position (1-based)
            length: Optional length of substring

        Returns:
            SQL SUBSTR function
        """
        if length is not None:
            return f"SUBSTR({string_expr}, {position}, {length})"
        return f"SUBSTR({string_expr}, {position})"