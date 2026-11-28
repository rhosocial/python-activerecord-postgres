# src/rhosocial/activerecord/backend/impl/postgres/functions/amcheck.py
"""amcheck integrity-check function factories.

All factories take the dialect first and return Expression nodes.
"""

from enum import Enum
from typing import TYPE_CHECKING, Optional

from rhosocial.activerecord.backend.expression.functions import CastExpression
from rhosocial.activerecord.backend.expression.functions import FunctionCall
from rhosocial.activerecord.backend.expression.value_types import Parameter
from rhosocial.activerecord.backend.expression.bases import BaseExpression
from rhosocial.activerecord.backend.expression.types import BigIntType
from rhosocial.activerecord.backend.impl.postgres.expression.types import PostgresRegClassType

if TYPE_CHECKING:  # pragma: no cover
    from rhosocial.activerecord.backend.dialect import SQLDialectBase


def _expr(dialect: "SQLDialectBase", value):
    """Expressions pass through; other values become bound parameters."""
    if isinstance(value, BaseExpression):
        return value
    return Parameter(dialect, value)


class SkipMode(str, Enum):
    """Visibility states that ``verify_heapam`` may skip."""

    __str__ = str.__str__

    NONE = "none"
    ALL_VISIBLE = "all-visible"
    ALL_FROZEN = "all-frozen"


def bt_index_check(
    dialect: "SQLDialectBase", index, *, heapallindexed: Optional[bool] = None
) -> FunctionCall:
    """``bt_index_check(index)`` or ``bt_index_check(index, heapallindexed)``.

    Omitting ``heapallindexed`` emits the one-argument form, which exists in
    every amcheck version; older servers have no two-argument overload.
    """
    arguments = [_expr(dialect, index)]
    if heapallindexed is not None:
        if not isinstance(heapallindexed, bool):
            raise TypeError("heapallindexed must be a bool")
        arguments.append(Parameter(dialect, heapallindexed))
    return FunctionCall(dialect, "bt_index_check", *arguments)


def bt_index_parent_check(
    dialect: "SQLDialectBase", index, *, heapallindexed: Optional[bool] = None
) -> FunctionCall:
    """``bt_index_parent_check(index[, heapallindexed])``."""
    arguments = [_expr(dialect, index)]
    if heapallindexed is not None:
        if not isinstance(heapallindexed, bool):
            raise TypeError("heapallindexed must be a bool")
        arguments.append(Parameter(dialect, heapallindexed))
    return FunctionCall(dialect, "bt_index_parent_check", *arguments)


def verify_heapam(
    dialect: "SQLDialectBase",
    relation,
    *,
    on_error_stop: bool = False,
    check_toast: bool = False,
    skip: SkipMode = SkipMode.NONE,
    start_block=None,
    end_block=None,
) -> FunctionCall:
    """``verify_heapam(relation, on_error_stop, check_toast, skip, startblock, endblock)``."""
    for name, value in (("on_error_stop", on_error_stop), ("check_toast", check_toast)):
        if not isinstance(value, bool):
            raise TypeError(f"{name} must be a bool")
    if not isinstance(skip, SkipMode):
        raise TypeError("skip must be a SkipMode")
    # Explicit casts keep the six-argument signature unambiguous on the wire.
    regclass = CastExpression(dialect, _expr(dialect, relation), PostgresRegClassType(dialect))
    bigint = BigIntType(dialect)
    return FunctionCall(
        dialect, "verify_heapam",
        regclass,
        Parameter(dialect, on_error_stop),
        Parameter(dialect, check_toast),
        Parameter(dialect, skip.value),
        CastExpression(dialect, Parameter(dialect, start_block), bigint),
        CastExpression(dialect, Parameter(dialect, end_block), bigint),
    )


__all__ = ["SkipMode", "bt_index_check", "bt_index_parent_check", "verify_heapam"]
