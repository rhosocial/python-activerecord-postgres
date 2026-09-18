# src/rhosocial/activerecord/backend/impl/postgres/mixins/datetime.py
"""PostgreSQL datetime formatting mixin."""

from typing import Tuple, TYPE_CHECKING

if TYPE_CHECKING:
    from rhosocial.activerecord.backend.expression.datetime import DateTimeDiffExpression


class PostgresDateTimeMixin:
    """PostgreSQL datetime diff expression formatting."""

    def format_datetime_diff_expression(self, expr: "DateTimeDiffExpression") -> Tuple[str, tuple]:
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
