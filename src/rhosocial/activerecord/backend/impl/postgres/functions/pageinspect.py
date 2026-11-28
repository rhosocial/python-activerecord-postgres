# src/rhosocial/activerecord/backend/impl/postgres/functions/pageinspect.py
"""pageinspect low-level page inspection function factories.

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
    return core.Parameter(dialect, value)


def get_raw_page(
    dialect: "SQLDialectBase", relation, fork: str, block_number: int
) -> core.FunctionCall:
    """``get_raw_page(relation, fork, block_number)``."""
    if not isinstance(block_number, int) or isinstance(block_number, bool) or block_number < 0:
        raise ValueError("block_number must be a non-negative integer")
    return core.FunctionCall(
        dialect, "get_raw_page",
        _expr(dialect, relation), core.Parameter(dialect, fork), core.Parameter(dialect, block_number),
    )


def page_header(dialect: "SQLDialectBase", page) -> core.FunctionCall:
    """``page_header(page)``."""
    return core.FunctionCall(dialect, "page_header", _expr(dialect, page))


def bt_page_stats(
    dialect: "SQLDialectBase", relation, block_number: int
) -> core.FunctionCall:
    """``bt_page_stats(relation, block_number)``."""
    if not isinstance(block_number, int) or isinstance(block_number, bool) or block_number < 0:
        raise ValueError("block_number must be a non-negative integer")
    return core.FunctionCall(
        dialect, "bt_page_stats", _expr(dialect, relation), core.Parameter(dialect, block_number)
    )


def heap_page_items(dialect: "SQLDialectBase", page) -> core.FunctionCall:
    """``heap_page_items(page)``."""
    return core.FunctionCall(dialect, "heap_page_items", _expr(dialect, page))


__all__ = ["get_raw_page", "page_header", "bt_page_stats", "heap_page_items"]
