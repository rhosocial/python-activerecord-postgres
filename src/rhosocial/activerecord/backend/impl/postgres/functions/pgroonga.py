# src/rhosocial/activerecord/backend/impl/postgres/functions/pgroonga.py
"""
PostgreSQL PGroonga function factories and v2 operator factories.

Every factory takes the dialect as its first argument and returns an Expression
node.  A plain string ``column`` is a column reference; query text is a bound
Literal value.  Pass an Expression explicitly when you need a different shape.

PGroonga documentation: https://pgroonga.github.io/reference/

The extension must be enabled first::

    CREATE EXTENSION pgroonga;
"""

from typing import Any, Sequence, Union, TYPE_CHECKING

from rhosocial.activerecord.backend.expression import bases, core
from rhosocial.activerecord.backend.expression.operators import BinaryExpression, RawSQL

if TYPE_CHECKING:
    from rhosocial.activerecord.backend.dialect import SQLDialectBase

Operand = Union[str, bases.BaseExpression]


def _column(dialect: "SQLDialectBase", column: Operand) -> bases.BaseExpression:
    """A string names a column; an Expression is used as given."""
    return core.Column(dialect, column) if isinstance(column, str) else column


def _literal(dialect: "SQLDialectBase", value: Any) -> bases.BaseExpression:
    """A string is a bound value; an Expression is used as given."""
    return core.Literal(dialect, value) if not isinstance(value, bases.BaseExpression) else value


def _array(dialect: "SQLDialectBase", values: Sequence[Any]) -> bases.BaseExpression:
    """Bind a sequence as one array parameter (or pass an Expression through)."""
    if isinstance(values, bases.BaseExpression):
        return values
    return core.Literal(dialect, list(values))


def _binary(
    dialect: "SQLDialectBase",
    operator: str,
    column: Operand,
    value: Any,
) -> BinaryExpression:
    return BinaryExpression(dialect, operator, _column(dialect, column), _literal(dialect, value))


# === v2 operators ===


def pgroonga_match(dialect: "SQLDialectBase", column: Operand, keyword: Any) -> BinaryExpression:
    """Full text search by one keyword: ``column &@ keyword``."""
    return _binary(dialect, "&@", column, keyword)


def pgroonga_query(dialect: "SQLDialectBase", column: Operand, query: Any) -> BinaryExpression:
    """Full text search by query language: ``column &@~ query``.

    The query language supports ``AND`` / ``OR`` / ``!`` and quoted phrases;
    e.g. ``'groonga OR postgresql -mysql'``.
    """
    return _binary(dialect, "&@~", column, query)


def pgroonga_match_in(
    dialect: "SQLDialectBase", column: Operand, keywords: Sequence[Any]
) -> BinaryExpression:
    """Full text search by an array of keywords: ``column &@| keywords``."""
    return BinaryExpression(
        dialect, "&@|", _column(dialect, column), _array(dialect, keywords)
    )


def pgroonga_query_in(
    dialect: "SQLDialectBase", column: Operand, queries: Sequence[Any]
) -> BinaryExpression:
    """Search by an array of query-language strings: ``column &@~| queries``."""
    return BinaryExpression(
        dialect, "&@~|", _column(dialect, column), _array(dialect, queries)
    )


def pgroonga_prefix(dialect: "SQLDialectBase", column: Operand, prefix: Any) -> BinaryExpression:
    """Prefix search: ``column &^ prefix`` (needs term search opclass)."""
    return _binary(dialect, "&^", column, prefix)


def pgroonga_prefix_in(
    dialect: "SQLDialectBase", column: Operand, prefixes: Sequence[Any]
) -> BinaryExpression:
    """Prefix search by an array: ``column &^| prefixes``."""
    return BinaryExpression(
        dialect, "&^|", _column(dialect, column), _array(dialect, prefixes)
    )


def pgroonga_regexp(dialect: "SQLDialectBase", column: Operand, pattern: Any) -> BinaryExpression:
    """Regular expression search: ``column &~ pattern``."""
    return _binary(dialect, "&~", column, pattern)


def pgroonga_regexp_in(
    dialect: "SQLDialectBase", column: Operand, patterns: Sequence[Any]
) -> BinaryExpression:
    """Regular expression search by an array: ``column &~| patterns``."""
    return BinaryExpression(
        dialect, "&~|", _column(dialect, column), _array(dialect, patterns)
    )


def pgroonga_equal_query(
    dialect: "SQLDialectBase", column: Operand, query: Any
) -> BinaryExpression:
    """Equal search by query language: ``column &=~ query`` (3.0.0+)."""
    return _binary(dialect, "&=~", column, query)


# === functions ===


def pgroonga_score(dialect: "SQLDialectBase") -> core.FunctionCall:
    """Score of the current row for PGroonga ranking.

    Produces ``pgroonga_score(tableoid, ctid)`` -- use it in SELECT / ORDER BY
    together with a match predicate in WHERE.
    """
    return core.FunctionCall(
        dialect,
        "pgroonga_score",
        RawSQL(dialect, "tableoid"),
        RawSQL(dialect, "ctid"),
    )


def _keywords(
    dialect: "SQLDialectBase", keywords: Sequence[Any]
) -> bases.BaseExpression:
    return _array(dialect, keywords)


def pgroonga_snippet_html(
    dialect: "SQLDialectBase",
    target: Operand,
    keywords: Sequence[Any],
) -> core.FunctionCall:
    """HTML snippet with the matched keywords wrapped in tags."""
    return core.FunctionCall(dialect, "pgroonga_snippet_html", _column(dialect, target), _keywords(dialect, keywords))


def pgroonga_highlight_html(
    dialect: "SQLDialectBase",
    target: Operand,
    keywords: Sequence[Any],
) -> core.FunctionCall:
    """HTML text with the matched keywords highlighted."""
    return core.FunctionCall(dialect, "pgroonga_highlight_html", _column(dialect, target), _keywords(dialect, keywords))


def pgroonga_match_positions_byte(
    dialect: "SQLDialectBase",
    target: Operand,
    keywords: Sequence[Any],
) -> core.FunctionCall:
    """Match positions in bytes (for byte-oriented slicing)."""
    return core.FunctionCall(
        dialect, "pgroonga_match_positions_byte", _column(dialect, target), _keywords(dialect, keywords)
    )


def pgroonga_match_positions_character(
    dialect: "SQLDialectBase",
    target: Operand,
    keywords: Sequence[Any],
) -> core.FunctionCall:
    """Match positions in characters (safe for CJK text)."""
    return core.FunctionCall(
        dialect, "pgroonga_match_positions_character", _column(dialect, target), _keywords(dialect, keywords)
    )


def pgroonga_query_escape(dialect: "SQLDialectBase", value: Any) -> core.FunctionCall:
    """Escape a string so it is treated as a literal keyword in query syntax."""
    return core.FunctionCall(dialect, "pgroonga_query_escape", _literal(dialect, value))


def pgroonga_query_extract_keywords(
    dialect: "SQLDialectBase", query: Any
) -> core.FunctionCall:
    """Extract keywords from a query-language string."""
    return core.FunctionCall(dialect, "pgroonga_query_extract_keywords", _literal(dialect, query))


def pgroonga_command(
    dialect: "SQLDialectBase", name: Any, *arguments: Any
) -> core.FunctionCall:
    """Run a Groonga command directly (advanced escape hatch)."""
    return core.FunctionCall(
        dialect, "pgroonga_command", _literal(dialect, name), *(_literal(dialect, arg) for arg in arguments)
    )


__all__ = [
    "pgroonga_match",
    "pgroonga_query",
    "pgroonga_match_in",
    "pgroonga_query_in",
    "pgroonga_prefix",
    "pgroonga_prefix_in",
    "pgroonga_regexp",
    "pgroonga_regexp_in",
    "pgroonga_equal_query",
    "pgroonga_score",
    "pgroonga_snippet_html",
    "pgroonga_highlight_html",
    "pgroonga_match_positions_byte",
    "pgroonga_match_positions_character",
    "pgroonga_query_escape",
    "pgroonga_query_extract_keywords",
    "pgroonga_command",
]
