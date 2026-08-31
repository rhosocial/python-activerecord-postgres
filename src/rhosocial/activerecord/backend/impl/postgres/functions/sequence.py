# src/rhosocial/activerecord/backend/impl/postgres/functions/sequence.py
"""
PostgreSQL Sequence Functions.

This module provides SQL expression generators for PostgreSQL sequence
functions. All functions return FunctionCall expression objects that
integrate with the expression-dialect architecture.

PostgreSQL Documentation: https://www.postgresql.org/docs/current/functions-sequence.html

Sequence functions:
- nextval(regclass): Advance the sequence and return the new value
- currval(regclass): Return the value most recently obtained with nextval
- lastval(): Return the value most recently obtained with nextval in the
  current session (no arguments)
- setval(regclass, bigint): Set the sequence's current value
- setval(regclass, bigint, boolean): Set the sequence's current value and
  control whether the next call to nextval advances past it

The sequence name is accepted as a plain string (e.g. ``'user_id_seq'`` or
``'schema.user_id_seq'``). PostgreSQL resolves it to a ``regclass``
automatically, including schema-qualified and double-quoted names.

All functions follow the expression-dialect separation architecture:
- First parameter is always the dialect instance
- They return core.FunctionCall expression objects (not raw SQL strings)
"""

from typing import Any, Optional, Union, TYPE_CHECKING

from rhosocial.activerecord.backend.expression import bases, core

if TYPE_CHECKING:
    from rhosocial.activerecord.backend.dialect import SQLDialectBase


def _convert_to_expression(
    dialect: "SQLDialectBase",
    expr: Any,
) -> "bases.BaseExpression":
    """Convert an input value to an appropriate BaseExpression.

    If the input is already a BaseExpression, it is returned as-is.
    Otherwise, it is wrapped in a core.Literal for parameterized output.
    """
    if isinstance(expr, bases.BaseExpression):
        return expr
    elif isinstance(expr, str):
        return core.Literal(dialect, expr)
    else:
        return core.Literal(dialect, expr)


def nextval(
    dialect: "SQLDialectBase",
    sequence_name: Union[str, "bases.BaseExpression"],
) -> core.FunctionCall:
    """Advance the sequence and return the new value.

    Args:
        dialect: The SQL dialect instance
        sequence_name: Sequence name (e.g. 'user_id_seq'), schema-qualified
            name (e.g. 'public.user_id_seq'), or an expression

    Returns:
        FunctionCall: SQL expression for nextval(sequence_name)

    Example:
        >>> func = nextval(dialect, 'user_id_seq')
        >>> func.to_sql()
        ('nextval(%s)', ('user_id_seq',))
    """
    return core.FunctionCall(
        dialect,
        "nextval",
        _convert_to_expression(dialect, sequence_name),
    )


def currval(
    dialect: "SQLDialectBase",
    sequence_name: Union[str, "bases.BaseExpression"],
) -> core.FunctionCall:
    """Return the value most recently obtained by nextval for this sequence.

    Unlike nextval, currval does not advance the sequence. It errors if
    nextval has not yet been called for the sequence in the current session.

    Args:
        dialect: The SQL dialect instance
        sequence_name: Sequence name or an expression

    Returns:
        FunctionCall: SQL expression for currval(sequence_name)

    Example:
        >>> func = currval(dialect, 'user_id_seq')
        >>> func.to_sql()
        ('currval(%s)', ('user_id_seq',))
    """
    return core.FunctionCall(
        dialect,
        "currval",
        _convert_to_expression(dialect, sequence_name),
    )


def lastval(dialect: "SQLDialectBase") -> core.FunctionCall:
    """Return the value most recently returned by nextval in the session.

    Takes no arguments. Errors if nextval has not yet been called in the
    current session.

    Args:
        dialect: The SQL dialect instance

    Returns:
        FunctionCall: SQL expression for lastval()

    Example:
        >>> func = lastval(dialect)
        >>> func.to_sql()
        ('lastval()', ())
    """
    return core.FunctionCall(dialect, "lastval")


def setval(
    dialect: "SQLDialectBase",
    sequence_name: Union[str, "bases.BaseExpression"],
    value: Union[int, "bases.BaseExpression"],
    is_called: Optional[Union[bool, "bases.BaseExpression"]] = None,
) -> core.FunctionCall:
    """Set the sequence's current value.

    With two arguments, the next call to nextval will return ``value + 1``
    (i.e. the value is treated as the last value that was returned).
    With three arguments and ``is_called=False``, the next call to nextval
    returns ``value`` itself.

    Args:
        dialect: The SQL dialect instance
        sequence_name: Sequence name or an expression
        value: The new current value for the sequence
        is_called: Optional boolean controlling nextval behavior

    Returns:
        FunctionCall: SQL expression for setval(sequence_name, value[, is_called])

    Example:
        >>> func = setval(dialect, 'user_id_seq', 1000)
        >>> func.to_sql()
        ('setval(%s, %s)', ('user_id_seq', 1000))
        >>> func = setval(dialect, 'user_id_seq', 1000, False)
        >>> func.to_sql()
        ('setval(%s, %s, %s)', ('user_id_seq', 1000, False))
    """
    args = [
        _convert_to_expression(dialect, sequence_name),
        _convert_to_expression(dialect, value),
    ]
    if is_called is not None:
        args.append(_convert_to_expression(dialect, is_called))
    return core.FunctionCall(dialect, "setval", *args)


__all__ = [
    "nextval",
    "currval",
    "lastval",
    "setval",
]
