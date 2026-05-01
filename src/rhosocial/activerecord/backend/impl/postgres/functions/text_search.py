# src/rhosocial/activerecord/backend/impl/postgres/functions/text_search.py
"""
PostgreSQL text search functions for SQL expression generation.

This module provides utility functions for generating PostgreSQL full-text search
SQL expressions using the Expression/Dialect architecture.

All functions follow the expression-dialect separation architecture:
- First parameter is always the dialect instance
- They return Expression objects (FunctionCall, BinaryExpression, etc.)
- They do not concatenate SQL strings directly

Supported functions:
- to_tsvector() - Convert text to tsvector
- to_tsquery() - Convert to tsquery
- plainto_tsquery() - Convert plain text to tsquery
- phraseto_tsquery() - Convert phrase text to tsquery
- websearch_to_tsquery() - Convert web search style query to tsquery
- ts_matches() - Test tsvector @@ tsquery
- ts_matches_expr() - Test tsvector @@@ tsquery (deprecated)
- ts_rank() - Calculate relevance rank
- ts_rank_cd() - Calculate rank by cover density
- ts_headline() - Display query matches with highlighting
- tsvector_concat() - Concatenate two tsvectors
- tsvector_strip() - Remove positions and weights from tsvector
- tsvector_setweight() - Set weight for all lexemes
- tsvector_length() - Get number of lexemes

PostgreSQL Documentation: https://www.postgresql.org/docs/current/functions-textsearch.html

Examples:
    Generate tsvector from document:
        >>> to_tsvector(dialect, "hello world", config="english")

    Generate tsquery from query:
        >>> to_tsquery(dialect, "hello & world", config="english")

    Test if tsvector matches tsquery:
        >>> ts_matches(dialect, "title_tsvector", "to_tsquery('hello')")

    Calculate relevance rank:
        >>> ts_rank(dialect, "title_tsvector", "to_tsquery('hello')")

    Get highlighted text with matches:
        >>> ts_headline(dialect, "content", "to_tsquery('hello')")
"""

from typing import Any, List, Optional, Union, TYPE_CHECKING

from rhosocial.activerecord.backend.expression import bases, core
from rhosocial.activerecord.backend.expression.operators import BinaryExpression

if TYPE_CHECKING:
    from rhosocial.activerecord.backend.dialect import SQLDialectBase


def _convert_to_expression(
    dialect: "SQLDialectBase",
    expr: Union[str, "bases.BaseExpression"],
) -> "bases.BaseExpression":
    """Convert an input value to an appropriate BaseExpression.

    Supports strings and existing BaseExpression objects.

    Args:
        dialect: The SQL dialect instance
        expr: Value to convert

    Returns:
        BaseExpression representing the value
    """
    if isinstance(expr, bases.BaseExpression):
        return expr
    elif isinstance(expr, str):
        return core.Literal(dialect, expr)
    else:
        return core.Literal(dialect, expr)


# ============== Full-Text Search Functions ==============


def to_tsvector(
    dialect: "SQLDialectBase",
    document: Any,
    config: str = "english",
) -> core.FunctionCall:
    """Generate SQL expression for PostgreSQL to_tsvector function.

    Converts a text document to a tsvector for full-text search.

    Args:
        dialect: The SQL dialect instance
        document: The document text to convert to tsvector
        config: Text search configuration (default: 'english')

    Returns:
        FunctionCall for to_tsvector(config, document)

    Example:
        >>> to_tsvector(dialect, 'hello world')
        >>> to_tsvector(dialect, 'hello world', config='simple')
    """
    return core.FunctionCall(
        dialect, "to_tsvector",
        core.Literal(dialect, config),
        _convert_to_expression(dialect, document),
    )


def to_tsquery(
    dialect: "SQLDialectBase",
    query: Any,
    config: str = "english",
) -> core.FunctionCall:
    """Generate SQL expression for PostgreSQL to_tsquery function.

    Converts a query string to a tsquery for full-text search.

    Args:
        dialect: The SQL dialect instance
        query: The query string to convert to tsquery
        config: Text search configuration (default: 'english')

    Returns:
        FunctionCall for to_tsquery(config, query)

    Example:
        >>> to_tsquery(dialect, 'hello & world')
        >>> to_tsquery(dialect, 'hello & world', config='simple')
    """
    return core.FunctionCall(
        dialect, "to_tsquery",
        core.Literal(dialect, config),
        _convert_to_expression(dialect, query),
    )


def plainto_tsquery(
    dialect: "SQLDialectBase",
    query: Any,
    config: str = "english",
) -> core.FunctionCall:
    """Generate SQL expression for PostgreSQL plainto_tsquery function.

    Converts plain text to tsquery, handling special characters.

    Args:
        dialect: The SQL dialect instance
        query: The plain text query
        config: Text search configuration (default: 'english')

    Returns:
        FunctionCall for plainto_tsquery(config, query)

    Example:
        >>> plainto_tsquery(dialect, 'hello world')
        >>> plainto_tsquery(dialect, 'hello world', config='simple')
    """
    return core.FunctionCall(
        dialect, "plainto_tsquery",
        core.Literal(dialect, config),
        _convert_to_expression(dialect, query),
    )


def phraseto_tsquery(
    dialect: "SQLDialectBase",
    query: Any,
    config: str = "english",
) -> core.FunctionCall:
    """Generate SQL expression for PostgreSQL phraseto_tsquery function.

    Converts text to tsquery for phrase search.

    Args:
        dialect: The SQL dialect instance
        query: The phrase query string
        config: Text search configuration (default: 'english')

    Returns:
        FunctionCall for phraseto_tsquery(config, query)

    Example:
        >>> phraseto_tsquery(dialect, 'hello world')
        >>> phraseto_tsquery(dialect, 'hello world', config='simple')
    """
    return core.FunctionCall(
        dialect, "phraseto_tsquery",
        core.Literal(dialect, config),
        _convert_to_expression(dialect, query),
    )


def websearch_to_tsquery(
    dialect: "SQLDialectBase",
    query: Any,
    config: str = "english",
) -> core.FunctionCall:
    """Generate SQL expression for PostgreSQL websearch_to_tsquery function.

    Converts web search style query to tsquery (PostgreSQL 11+).

    Args:
        dialect: The SQL dialect instance
        query: The web search query string
        config: Text search configuration (default: 'english')

    Returns:
        FunctionCall for websearch_to_tsquery(config, query)

    Example:
        >>> websearch_to_tsquery(dialect, 'hello -world')
        >>> websearch_to_tsquery(dialect, 'hello -world', config='simple')
    """
    return core.FunctionCall(
        dialect, "websearch_to_tsquery",
        core.Literal(dialect, config),
        _convert_to_expression(dialect, query),
    )


def ts_rank(
    dialect: "SQLDialectBase",
    vector: Any,
    query: Any,
    weights: Optional[List[float]] = None,
) -> core.FunctionCall:
    """Generate SQL expression for PostgreSQL ts_rank function.

    Calculates relevance rank for tsvector matching tsquery.

    Args:
        dialect: The SQL dialect instance
        vector: The tsvector expression or column
        query: The tsquery expression
        weights: Optional list of 4 weights [D, C, B, A] (default [0.1, 0.2, 0.4, 1.0])

    Returns:
        FunctionCall for ts_rank([weights,] vector, query)

    Example:
        >>> ts_rank(dialect, 'title_tsvector', "to_tsquery('hello')")
        >>> ts_rank(dialect, 'title_tsvector', "to_tsquery('hello')", [0.1, 0.2, 0.4, 1.0])
    """
    args: list = []
    if weights is not None:
        weights_str = "ARRAY[" + ", ".join(str(w) for w in weights) + "]"
        args.append(core.Literal(dialect, weights_str))
    args.append(_convert_to_expression(dialect, vector))
    args.append(_convert_to_expression(dialect, query))
    return core.FunctionCall(dialect, "ts_rank", *args)


def ts_rank_cd(
    dialect: "SQLDialectBase",
    vector: Any,
    query: Any,
    weights: Optional[List[float]] = None,
) -> core.FunctionCall:
    """Generate SQL expression for PostgreSQL ts_rank_cd function.

    Calculates relevance rank using cover density.

    Args:
        dialect: The SQL dialect instance
        vector: The tsvector expression or column
        query: The tsquery expression
        weights: Optional list of 4 weights [D, C, B, A]

    Returns:
        FunctionCall for ts_rank_cd([weights,] vector, query)

    Example:
        >>> ts_rank_cd(dialect, 'title_tsvector', "to_tsquery('hello')")
        >>> ts_rank_cd(dialect, 'title_tsvector', "to_tsquery('hello')", [0.1, 0.2, 0.4, 1.0])
    """
    args: list = []
    if weights is not None:
        weights_str = "ARRAY[" + ", ".join(str(w) for w in weights) + "]"
        args.append(core.Literal(dialect, weights_str))
    args.append(_convert_to_expression(dialect, vector))
    args.append(_convert_to_expression(dialect, query))
    return core.FunctionCall(dialect, "ts_rank_cd", *args)


def ts_headline(
    dialect: "SQLDialectBase",
    document: Any,
    query: Any,
    config: str = "english",
    options: Optional[Any] = None,
) -> core.FunctionCall:
    """Generate SQL expression for PostgreSQL ts_headline function.

    Displays query matches in document with highlighting.

    PostgreSQL signature: ts_headline([config,] document, query [, options])

    Args:
        dialect: The SQL dialect instance
        document: The document text or expression
        query: The tsquery expression
        config: Text search configuration (default: 'english')
        options: Optional headline options string

    Returns:
        FunctionCall for ts_headline(config, document, query[, options])

    Example:
        >>> ts_headline(dialect, 'content', "to_tsquery('hello')")
        >>> ts_headline(dialect, 'content', "to_tsquery('hello')", config='english')
        >>> ts_headline(dialect, 'content', "to_tsquery('hello')", options='StartSel=<b>, StopSel=</b>')
    """
    args: list = [
        core.Literal(dialect, config),
        _convert_to_expression(dialect, document),
        _convert_to_expression(dialect, query),
    ]
    if options is not None:
        args.append(_convert_to_expression(dialect, options))
    return core.FunctionCall(dialect, "ts_headline", *args)


def tsvector_strip(
    dialect: "SQLDialectBase",
    vector: Any,
) -> core.FunctionCall:
    """Generate SQL expression for strip function.

    Removes positions and weights from tsvector.

    Note: The PostgreSQL function name is "strip", not "tsvector_strip".

    Args:
        dialect: The SQL dialect instance
        vector: The tsvector expression

    Returns:
        FunctionCall for strip(vector)

    Example:
        >>> tsvector_strip(dialect, 'title_tsvector')
    """
    return core.FunctionCall(
        dialect, "strip",
        _convert_to_expression(dialect, vector),
    )


def tsvector_setweight(
    dialect: "SQLDialectBase",
    vector: Any,
    weight: Any,
) -> core.FunctionCall:
    """Generate SQL expression for setweight function.

    Sets weight for all lexemes in tsvector.

    Note: The PostgreSQL function name is "setweight", not "tsvector_setweight".

    Args:
        dialect: The SQL dialect instance
        vector: The tsvector expression
        weight: Weight character ('A', 'B', 'C', or 'D')

    Returns:
        FunctionCall for setweight(vector, weight)

    Example:
        >>> tsvector_setweight(dialect, 'title_tsvector', 'A')
    """
    return core.FunctionCall(
        dialect, "setweight",
        _convert_to_expression(dialect, vector),
        _convert_to_expression(dialect, weight),
    )


def tsvector_length(
    dialect: "SQLDialectBase",
    vector: Any,
) -> core.FunctionCall:
    """Generate SQL expression for length function.

    Returns the number of lexemes in tsvector.

    Note: The PostgreSQL function name is "length", not "tsvector_length".

    Args:
        dialect: The SQL dialect instance
        vector: The tsvector expression

    Returns:
        FunctionCall for length(vector)

    Example:
        >>> tsvector_length(dialect, 'title_tsvector')
    """
    return core.FunctionCall(
        dialect, "length",
        _convert_to_expression(dialect, vector),
    )


# ============== Full-Text Search Operators ==============


def ts_matches(
    dialect: "SQLDialectBase",
    vector: Any,
    query: Any,
) -> BinaryExpression:
    """Generate SQL expression for tsvector @@ tsquery match operator.

    Args:
        dialect: The SQL dialect instance
        vector: The tsvector expression or column
        query: The tsquery expression

    Returns:
        BinaryExpression for vector @@ query

    Example:
        >>> ts_matches(dialect, 'title_tsvector', "to_tsquery('hello')")
    """
    return BinaryExpression(
        dialect, "@@",
        _convert_to_expression(dialect, vector),
        _convert_to_expression(dialect, query),
    )


def ts_matches_expr(
    dialect: "SQLDialectBase",
    vector: Any,
    query: Any,
) -> BinaryExpression:
    """Generate SQL expression for tsvector @@@ tsquery match operator.

    The @@@ operator is a deprecated alias for @@.

    Args:
        dialect: The SQL dialect instance
        vector: The tsvector expression or column
        query: The tsquery expression

    Returns:
        BinaryExpression for vector @@@ query

    Example:
        >>> ts_matches_expr(dialect, 'title_tsvector', "to_tsquery('hello')")
    """
    return BinaryExpression(
        dialect, "@@@",
        _convert_to_expression(dialect, vector),
        _convert_to_expression(dialect, query),
    )


def tsvector_concat(
    dialect: "SQLDialectBase",
    vec1: Any,
    vec2: Any,
) -> BinaryExpression:
    """Generate SQL expression for tsvector concatenation.

    Args:
        dialect: The SQL dialect instance
        vec1: First tsvector expression
        vec2: Second tsvector expression

    Returns:
        BinaryExpression for vec1 || vec2

    Example:
        >>> tsvector_concat(dialect, 'title_tsvector', 'body_tsvector')
    """
    return BinaryExpression(
        dialect, "||",
        _convert_to_expression(dialect, vec1),
        _convert_to_expression(dialect, vec2),
    )


__all__ = [
    "to_tsvector",
    "to_tsquery",
    "plainto_tsquery",
    "phraseto_tsquery",
    "websearch_to_tsquery",
    "ts_matches",
    "ts_matches_expr",
    "ts_rank",
    "ts_rank_cd",
    "ts_headline",
    "tsvector_concat",
    "tsvector_strip",
    "tsvector_setweight",
    "tsvector_length",
]
