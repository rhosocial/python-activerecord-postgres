# src/rhosocial/activerecord/backend/impl/postgres/functions/pgstattuple.py
"""pgstattuple tuple-level statistics function factories.

All factories take the dialect first and return Expression nodes.
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


def pgstattuple(dialect: "SQLDialectBase", relation) -> FunctionCall:
    """``pgstattuple(relation)``."""
    return FunctionCall(dialect, "pgstattuple", _expr(dialect, relation))


def pgstatindex(dialect: "SQLDialectBase", index) -> FunctionCall:
    """``pgstatindex(index)``."""
    return FunctionCall(dialect, "pgstatindex", _expr(dialect, index))


def pg_relpages(dialect: "SQLDialectBase", relation) -> FunctionCall:
    """``pg_relpages(relation)``."""
    return FunctionCall(dialect, "pg_relpages", _expr(dialect, relation))


__all__ = ["pgstattuple", "pgstatindex", "pg_relpages"]
