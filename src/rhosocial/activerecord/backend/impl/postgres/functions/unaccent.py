# src/rhosocial/activerecord/backend/impl/postgres/functions/unaccent.py
"""
PostgreSQL unaccent extension function factories.

``unaccent`` removes accents from text.  It is a contrib module, so a plain
string argument is a literal value; pass ``Column(dialect, name)`` to normalize
a column (for example inside ``to_tsvector``).

Documentation: https://www.postgresql.org/docs/current/unaccent.html
"""

from typing import Optional, TYPE_CHECKING

from rhosocial.activerecord.backend.expression import bases, core

if TYPE_CHECKING:
    from rhosocial.activerecord.backend.dialect import SQLDialectBase


def _expr(dialect: "SQLDialectBase", value) -> bases.BaseExpression:
    return value if isinstance(value, bases.BaseExpression) else core.Parameter(dialect, value)


def unaccent(
    dialect: "SQLDialectBase",
    text,
    dictionary: Optional[str] = None,
) -> core.FunctionCall:
    """Build ``unaccent(text)`` or ``unaccent(dictionary, text)``.

    Args:
        dialect: The SQL dialect instance.
        text: Text expression or literal to normalize.
        dictionary: Optional text search dictionary name (identifier).
    """
    if dictionary is not None:
        if not isinstance(dictionary, str) or not dictionary.isidentifier():
            raise ValueError(f"Invalid unaccent dictionary name: {dictionary!r}")
        return core.FunctionCall(dialect, "unaccent", core.Parameter(dialect, dictionary), _expr(dialect, text))
    return core.FunctionCall(dialect, "unaccent", _expr(dialect, text))


__all__ = ["unaccent"]
