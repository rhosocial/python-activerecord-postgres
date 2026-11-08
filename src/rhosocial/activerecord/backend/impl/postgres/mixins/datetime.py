# src/rhosocial/activerecord/backend/impl/postgres/mixins/datetime.py
"""PostgreSQL datetime formatting mixin."""

from typing import Tuple, TYPE_CHECKING

if TYPE_CHECKING:
    from rhosocial.activerecord.backend.expression.datetime import (
        DatetimeDiffExpression,
        SecondsBetweenExpression,
    )


class PostgresDateTimeMixin:
    """PostgreSQL datetime diff and elapsed-seconds formatting."""

    def format_datetime_diff_expression(self, expr: "DatetimeDiffExpression") -> Tuple[str, tuple]:
        start_sql, start_params = expr.start.to_sql()
        end_sql, end_params = expr.end.to_sql()
        seconds_sql = f"EXTRACT(EPOCH FROM ({end_sql} - {start_sql}))"
        factors = {
            "second": "1",
            "minute": "60",
            "hour": "3600",
            "day": "86400",
            "week": "604800",
        }
        if expr.unit.value in factors:
            sql = f"({seconds_sql} / {factors[expr.unit.value]})"
            params = end_params + start_params
        elif expr.unit.value == "month":
            sql = (
                f"((EXTRACT(YEAR FROM {end_sql}) - "
                f"EXTRACT(YEAR FROM {start_sql})) * 12 + "
                f"(EXTRACT(MONTH FROM {end_sql}) - "
                f"EXTRACT(MONTH FROM {start_sql})))"
            )
            params = end_params + start_params + end_params + start_params
        else:
            sql = (
                f"(EXTRACT(YEAR FROM {end_sql}) - "
                f"EXTRACT(YEAR FROM {start_sql}))"
            )
            params = end_params + start_params
        return self.apply_alias(sql, params, expr)

    def format_seconds_between_expression(
        self, expr: "SecondsBetweenExpression"
    ) -> Tuple[str, tuple]:
        """Render signed, fractional seconds from ``start`` to ``end``.

        ``EXTRACT(EPOCH FROM interval)`` is a ``numeric``, so the subtraction
        keeps microseconds and the sign is whatever ``end - start`` yields:
        an end before the start is negative, equal instants are exactly zero.

        The SQL writes ``end`` first, so its bind parameters are collected
        first as well -- bind order follows the SQL, not the constructor's
        argument order.
        """
        start_sql, start_params = expr.start.to_sql()
        end_sql, end_params = expr.end.to_sql()
        sql = f"EXTRACT(EPOCH FROM ({end_sql} - {start_sql}))"
        return self.apply_alias(sql, end_params + start_params, expr)
