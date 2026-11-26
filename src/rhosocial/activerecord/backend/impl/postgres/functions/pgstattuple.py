# src/rhosocial/activerecord/backend/impl/postgres/functions/pgstattuple.py
"""pgstattuple tuple-level statistics function factories.

All factories take the dialect first and return Expression nodes.
"""

from typing import TYPE_CHECKING

from rhosocial.activerecord.backend.expression import bases, core

if TYPE_CHECKING:  # pragma: no cover
    from rhosocial.activerecord.backend.dialect import SQLDialectBase


def _expr(dialect: "SQLDialectBase", value):
    """Expressions pass through; other values become bound parameters."""
    if isinstance(value, bases.BaseExpression):
        return value
    return core.Literal(dialect, value)


def pgstattuple(dialect: "SQLDialectBase", relation) -> core.FunctionCall:
    """``pgstattuple(relation)``."""
    return core.FunctionCall(dialect, "pgstattuple", _expr(dialect, relation))


def pgstatindex(dialect: "SQLDialectBase", index) -> core.FunctionCall:
    """``pgstatindex(index)``."""
    return core.FunctionCall(dialect, "pgstatindex", _expr(dialect, index))


def pg_relpages(dialect: "SQLDialectBase", relation) -> core.FunctionCall:
    """``pg_relpages(relation)``."""
    return core.FunctionCall(dialect, "pg_relpages", _expr(dialect, relation))


__all__ = ["pgstattuple", "pgstatindex", "pg_relpages"]
