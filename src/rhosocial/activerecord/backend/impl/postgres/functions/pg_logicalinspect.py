# src/rhosocial/activerecord/backend/impl/postgres/functions/pg_logicalinspect.py
"""pg_logicalinspect logical snapshot inspection function factories.

Available on PostgreSQL 18+. All factories take the dialect first and return
Expression nodes.
"""

from typing import TYPE_CHECKING

from rhosocial.activerecord.backend.expression.functions import FunctionCall
from rhosocial.activerecord.backend.expression.value_types import Parameter
from rhosocial.activerecord.backend.expression.bases import BaseExpression

if TYPE_CHECKING:  # pragma: no cover
    from rhosocial.activerecord.backend.dialect import SQLDialectBase


def _expr(dialect: "SQLDialectBase", value):
    """Expressions pass through; other values become bound parameters."""
    if isinstance(value, BaseExpression):
        return value
    return Parameter(dialect, value)


def pg_get_logical_snapshot_meta(dialect: "SQLDialectBase", filename) -> FunctionCall:
    """``pg_get_logical_snapshot_meta(filename)``."""
    return FunctionCall(dialect, "pg_get_logical_snapshot_meta", _expr(dialect, filename))


def pg_get_logical_snapshot_info(dialect: "SQLDialectBase", filename) -> FunctionCall:
    """``pg_get_logical_snapshot_info(filename)``."""
    return FunctionCall(dialect, "pg_get_logical_snapshot_info", _expr(dialect, filename))


__all__ = ["pg_get_logical_snapshot_meta", "pg_get_logical_snapshot_info"]
