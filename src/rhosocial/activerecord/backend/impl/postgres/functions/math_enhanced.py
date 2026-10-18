# src/rhosocial/activerecord/backend/impl/postgres/functions/math_enhanced.py
"""PostgreSQL enhanced math function factories.

The factories whose semantics the core has classed (commit a3978e10 for the
math set, 9b559030 for the aggregate set) now delegate to those classed
entry points instead of building a generic ``FunctionCall`` -- the generic
node is reserved for user-defined functions, and a classed concept wearing
the generic node is exactly what that reservation forbids:

* ``round_`` -> :class:`~...functions.math.RoundFunction` (integer-column
  aware, via ``for_operand``; the precision is normalised here so both an
  ``int`` and an already-built expression render as the second operand)
* ``power``  -> core ``power`` (``PowerFunction``)
* ``sqrt``   -> core ``sqrt`` (``SquareRootFunction``)
* ``mod``    -> core ``mod`` (``ModuloFunction``)
* ``ceil``   -> core ``ceil`` (``CeilFunction``, integer-column aware)
* ``floor``  -> core ``floor`` (``FloorFunction``, integer-column aware)
* ``trunc``  -> :class:`~...functions.math.TruncateFunction` (integer-column
  aware, via ``for_operand``; PostgreSQL spells it ``TRUNC`` -- the dialect
  overrides ``format_truncate_function`` in ``mixins/expression.py``,
  measured live: ``TRUNC(3.716, 2)`` answers ``3.71`` on 17.11 while
  ``TRUNCATE(3.716)`` is refused with ``UndefinedFunction: function
  truncate(numeric) does not exist``)
* ``avg``    -> core ``avg`` (``AvgFunction``)

``pow`` delegates to ``PowerFunction`` too, and that is one visible change:
the factory used to spell PostgreSQL's ``POW`` alias and now writes the
canonical ``POWER``, which PostgreSQL also accepts (measured live 2026-10-18
against 17.11: ``SELECT POW(2.0, 10)`` and ``SELECT POWER(2.0, 10)`` both
answer ``1024``).

``max_`` and ``min_`` stay on ``FunctionCall`` on purpose: they spell the
scalar ``GREATEST`` / ``LEAST``, which the core has not classed -- its own
``greatest`` / ``least`` factories also build the generic node -- and the
core aggregate ``MinFunction`` / ``MaxFunction`` are different concepts (a
group reduction, not a row-wise comparison of operands).
"""

from typing import TYPE_CHECKING

from rhosocial.activerecord.backend.expression import bases, core, functions as core_functions
# The math node classes ride in the ``math`` submodule: the functions package
# exports the aggregate and string classes at its top level but only the
# factories for the math family. Importing the two classes this factory
# constructs directly keeps the classed entry points explicit.
from rhosocial.activerecord.backend.expression.functions.math import (
    RoundFunction,
    TruncateFunction,
)

if TYPE_CHECKING:  # pragma: no cover
    from rhosocial.activerecord.backend.dialect import SQLDialectBase


def round_(
    dialect: "SQLDialectBase",
    expr: "bases.BaseExpression",
    precision: int = 0,
) -> "bases.BaseExpression":
    """Create a ROUND node.

    Rounds a numeric value to the specified number of decimal places.

    Usage:
        - round_(dialect, Column("price")) -> ROUND("price", ?)
        - round_(dialect, Column("price"), 2) -> ROUND("price", ?)

    Delegates to the core ``round_`` factory, which builds
    :class:`~...functions.math.RoundFunction` (or the integer-preserving
    variant for an integer operand). The precision always renders, as
    before: the default 0 is a real request for a whole number.

    Args:
        dialect: The SQL dialect instance
        expr: The numeric expression to round
        precision: Number of decimal places (default 0); an int is wrapped
            as a bind literal, an existing expression is passed through

    Returns:
        A ``RoundFunction`` node representing the ROUND function

    Version: All PostgreSQL versions
    """
    precision_expr = (
        precision
        if isinstance(precision, bases.BaseExpression)
        else core.Literal(dialect, precision)
    )
    return RoundFunction.for_operand(
        dialect, type(expr), expr, precision_expr
    )


def pow(
    dialect: "SQLDialectBase",
    base: "bases.BaseExpression",
    exponent: "bases.BaseExpression",
) -> "bases.BaseExpression":
    """Create a POWER node.

    Returns the value of base raised to the power of exponent. The factory
    keeps its historical name but now writes ``POWER`` -- the canonical
    spelling PostgreSQL documents -- instead of the ``POW`` alias; the server
    accepts both (measured 2026-10-18, 17.11: both answer 1024 for
    ``(2.0, 10)``).

    Usage:
        - pow(dialect, 2, 3) -> POWER(?, ?)
        - pow(dialect, Column("x"), 2) -> POWER("x", ?)

    Args:
        dialect: The SQL dialect instance
        base: The base value
        exponent: The exponent value

    Returns:
        A :class:`~...functions.math.PowerFunction` node

    Version: All PostgreSQL versions
    """
    return core_functions.power(dialect, base, exponent)


def power(
    dialect: "SQLDialectBase",
    base: "bases.BaseExpression",
    exponent: "bases.BaseExpression",
) -> "bases.BaseExpression":
    """Create a POWER node.

    Returns the value of base raised to the power of exponent.

    Usage:
        - power(dialect, 2, 3) -> POWER(?, ?)
        - power(dialect, Column("x"), 2) -> POWER("x", ?)

    Delegates to the core ``power`` factory, which builds
    :class:`~...functions.math.PowerFunction`.

    Args:
        dialect: The SQL dialect instance
        base: The base value
        exponent: The exponent value

    Returns:
        A :class:`~...functions.math.PowerFunction` node

    Version: All PostgreSQL versions
    """
    return core_functions.power(dialect, base, exponent)


def sqrt(
    dialect: "SQLDialectBase",
    expr: "bases.BaseExpression",
) -> "bases.BaseExpression":
    """Create a SQRT node.

    Returns the square root of the argument.

    Usage:
        - sqrt(dialect, 16) -> SQRT(?)
        - sqrt(dialect, Column("value")) -> SQRT("value")

    Delegates to the core ``sqrt`` factory, which builds
    :class:`~...functions.math.SquareRootFunction`.

    Args:
        dialect: The SQL dialect instance
        expr: The numeric expression

    Returns:
        A :class:`~...functions.math.SquareRootFunction` node

    Version: All PostgreSQL versions
    """
    return core_functions.sqrt(dialect, expr)


def mod(
    dialect: "SQLDialectBase",
    dividend: "bases.BaseExpression",
    divisor: "bases.BaseExpression",
) -> "bases.BaseExpression":
    """Create a MOD node.

    Returns the remainder of dividend divided by divisor.

    Usage:
        - mod(dialect, 10, 3) -> MOD(?, ?)
        - mod(dialect, Column("total"), Column("count")) -> MOD("total", "count")

    Delegates to the core ``mod`` factory, which builds
    :class:`~...functions.math.ModuloFunction`.

    Args:
        dialect: The SQL dialect instance
        dividend: The dividend value
        divisor: The divisor value

    Returns:
        A :class:`~...functions.math.ModuloFunction` node

    Version: All PostgreSQL versions
    """
    return core_functions.mod(dialect, dividend, divisor)


def ceil(
    dialect: "SQLDialectBase",
    expr: "bases.BaseExpression",
) -> "bases.BaseExpression":
    """Create a CEIL node.

    Returns the smallest integer greater than or equal to the argument.

    Usage:
        - ceil(dialect, 3.14) -> CEIL(?)
        - ceil(dialect, Column("value")) -> CEIL("value")

    Delegates to the core ``ceil`` factory, which builds
    :class:`~...functions.math.CeilFunction` (or the integer-preserving
    variant for an integer operand).

    Args:
        dialect: The SQL dialect instance
        expr: The numeric expression

    Returns:
        A ``CeilFunction`` node representing the CEIL function

    Version: All PostgreSQL versions
    """
    return core_functions.ceil(dialect, expr)


def floor(
    dialect: "SQLDialectBase",
    expr: "bases.BaseExpression",
) -> "bases.BaseExpression":
    """Create a FLOOR node.

    Returns the largest integer less than or equal to the argument.

    Usage:
        - floor(dialect, 3.14) -> FLOOR(?)
        - floor(dialect, Column("value")) -> FLOOR("value")

    Delegates to the core ``floor`` factory, which builds
    :class:`~...functions.math.FloorFunction` (or the integer-preserving
    variant for an integer operand).

    Args:
        dialect: The SQL dialect instance
        expr: The numeric expression

    Returns:
        A ``FloorFunction`` node representing the FLOOR function

    Version: All PostgreSQL versions
    """
    return core_functions.floor(dialect, expr)


def trunc(
    dialect: "SQLDialectBase",
    expr: "bases.BaseExpression",
    precision: int = 0,
) -> "bases.BaseExpression":
    """Create a TRUNC node.

    Returns the value truncated to the specified number of decimal places.

    Usage:
        - trunc(dialect, 3.14) -> TRUNC(?, ?)
        - trunc(dialect, 3.14, 2) -> TRUNC(?, ?)
        - trunc(dialect, Column("price"), 2) -> TRUNC("price", ?)

    Delegates to the core ``truncate`` factory, which builds
    :class:`~...functions.math.TruncateFunction`; the dialect renders that
    node as PostgreSQL's ``TRUNC`` (see ``mixins/expression.py`` -- the
    SQL-standard ``TRUNCATE`` spelling is a table statement here, measured
    live as rejected for numerics). The precision always renders, as
    before: the default 0 is a real request for a whole number.

    Args:
        dialect: The SQL dialect instance
        expr: The numeric expression to truncate
        precision: Number of decimal places (default 0); an int is wrapped
            as a bind literal, an existing expression is passed through

    Returns:
        A ``TruncateFunction`` node rendering as the TRUNC function

    Version: All PostgreSQL versions
    """
    precision_expr = (
        precision
        if isinstance(precision, bases.BaseExpression)
        else core.Literal(dialect, precision)
    )
    return TruncateFunction.for_operand(
        dialect, type(expr), expr, precision_expr
    )


def max_(
    dialect: "SQLDialectBase",
    expr1: "bases.BaseExpression",
    expr2: "bases.BaseExpression",
    *more: "bases.BaseExpression",
) -> "core.FunctionCall":
    """Create a GREATEST function call.

    Returns the maximum value among the arguments.

    Usage:
        - max_(dialect, 1, 2) -> GREATEST(?, ?)
        - max_(dialect, Column("a"), Column("b")) -> GREATEST("a", "b")

    Note:
        PostgreSQL's GREATEST and LEAST are scalar functions, and the core
        has not classed them -- its own ``greatest`` / ``least`` factories
        also build the generic node, and the aggregate ``MaxFunction`` is a
        different concept (a group reduction). The generic ``FunctionCall``
        is therefore still the right node here.
        For aggregate MAX, use the core ``MaxFunction``.

    Args:
        dialect: The SQL dialect instance
        expr1: First expression
        expr2: Second expression
        *more: Additional expressions

    Returns:
        A FunctionCall instance representing GREATEST

    Version: All PostgreSQL versions
    """
    exprs = [expr1, expr2]
    for m in more:
        exprs.append(m)
    return core.FunctionCall(dialect, "GREATEST", *exprs)


def min_(
    dialect: "SQLDialectBase",
    expr1: "bases.BaseExpression",
    expr2: "bases.BaseExpression",
    *more: "bases.BaseExpression",
) -> "core.FunctionCall":
    """Create a LEAST function call.

    Returns the minimum value among the arguments.

    Usage:
        - min_(dialect, 1, 2) -> LEAST(?, ?)
        - min_(dialect, Column("a"), Column("b")) -> LEAST("a", "b")

    Note:
        Like ``max_`` above: GREATEST / LEAST are scalar and unclassed even
        in the core, so the generic ``FunctionCall`` remains the right node.
        For aggregate MIN, use the core ``MinFunction``.

    Args:
        dialect: The SQL dialect instance
        expr1: First expression
        expr2: Second expression
        *more: Additional expressions

    Returns:
        A FunctionCall instance representing LEAST

    Version: All PostgreSQL versions
    """
    exprs = [expr1, expr2]
    for m in more:
        exprs.append(m)
    return core.FunctionCall(dialect, "LEAST", *exprs)


def avg(
    dialect: "SQLDialectBase",
    expr: "bases.BaseExpression",
) -> "bases.BaseExpression":
    """Create an AVG aggregate node.

    Returns the average value of all non-NULL values in a group.

    Usage:
        - avg(dialect, Column("price")) -> AVG("price")

    Delegates to the core ``avg`` factory, which builds
    :class:`~...functions.aggregate.AvgFunction`.

    Args:
        dialect: The SQL dialect instance
        expr: The expression to average

    Returns:
        An :class:`~...functions.aggregate.AvgFunction` node

    Version: All PostgreSQL versions
    """
    return core_functions.avg(dialect, expr)


__all__ = [
    "round_",
    "pow",
    "power",
    "sqrt",
    "mod",
    "ceil",
    "floor",
    "trunc",
    "max_",
    "min_",
    "avg",
]
