# src/rhosocial/activerecord/backend/impl/postgres/functions/pageinspect.py
"""pageinspect low-level page inspection function factories.

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


def get_raw_page(
    dialect: "SQLDialectBase", relation, fork: str, block_number: int
) -> FunctionCall:
    """``get_raw_page(relation, fork, block_number)``."""
    if not isinstance(block_number, int) or isinstance(block_number, bool) or block_number < 0:
        raise ValueError("block_number must be a non-negative integer")
    return FunctionCall(
        dialect, "get_raw_page",
        _expr(dialect, relation), Parameter(dialect, fork), Parameter(dialect, block_number),
    )


def page_header(dialect: "SQLDialectBase", page) -> FunctionCall:
    """``page_header(page)``."""
    return FunctionCall(dialect, "page_header", _expr(dialect, page))


def bt_page_stats(
    dialect: "SQLDialectBase", relation, block_number: int
) -> FunctionCall:
    """``bt_page_stats(relation, block_number)``."""
    if not isinstance(block_number, int) or isinstance(block_number, bool) or block_number < 0:
        raise ValueError("block_number must be a non-negative integer")
    return FunctionCall(
        dialect, "bt_page_stats", _expr(dialect, relation), Parameter(dialect, block_number)
    )


def heap_page_items(dialect: "SQLDialectBase", page) -> FunctionCall:
    """``heap_page_items(page)``."""
    return FunctionCall(dialect, "heap_page_items", _expr(dialect, page))


__all__ = ["get_raw_page", "page_header", "bt_page_stats", "heap_page_items"]
