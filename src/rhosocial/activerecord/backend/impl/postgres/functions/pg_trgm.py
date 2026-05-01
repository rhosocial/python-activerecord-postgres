# src/rhosocial/activerecord/backend/impl/postgres/functions/pg_trgm.py
"""
PostgreSQL pg_trgm Extension Functions and Operators.

This module provides SQL expression generators for PostgreSQL pg_trgm
extension functions and operators. All functions return Expression objects
(FunctionCall, BinaryExpression) that integrate with the expression-dialect
architecture.

The pg_trgm extension provides trigram similarity functions and operators
for text matching and fuzzy search.

PostgreSQL Documentation: https://www.postgresql.org/docs/current/pgtrgm.html

The pg_trgm extension must be installed:
    CREATE EXTENSION IF NOT EXISTS pg_trgm;

Supported functions:
- similarity: Calculate similarity between two texts
- word_similarity: Calculate word similarity between a document and a query
- show_trgm: Show the trigrams of a text string

Supported operators:
- %  : Text is similar (similarity threshold)
- !% : Text is not similar (negated similarity threshold)
"""

from typing import Union, TYPE_CHECKING

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

    For string inputs, generates a literal expression.

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


# ============== Similarity Functions ==============

def similarity(
    dialect: "SQLDialectBase",
    text1: Union[str, "bases.BaseExpression"],
    text2: Union[str, "bases.BaseExpression"],
) -> core.FunctionCall:
    """Calculate the similarity between two texts.

    Returns a number between 0 and 1 indicating how similar the two
    arguments are. The similarity is based on the number of matching
    trigrams divided by the total number of trigrams in both strings.

    The current similarity threshold is controlled by the
    pg_trgm.similarity_threshold GUC variable.

    Args:
        dialect: The SQL dialect instance
        text1: First text to compare (string or column reference)
        text2: Second text to compare (string or column reference)

    Returns:
        FunctionCall for similarity(text1, text2)

    Example:
        >>> from rhosocial.activerecord.backend.impl.postgres import PostgresDialect
        >>> dialect = PostgresDialect()
        >>> similarity(dialect, 'name_col', 'search_term')
        >>> # Generates: similarity(name_col, 'search_term')
    """
    return core.FunctionCall(
        dialect, "similarity",
        _convert_to_expression(dialect, text1),
        _convert_to_expression(dialect, text2),
    )


def word_similarity(
    dialect: "SQLDialectBase",
    column: Union[str, "bases.BaseExpression"],
    query: Union[str, "bases.BaseExpression"],
) -> core.FunctionCall:
    """Calculate the word similarity between a document column and a query.

    Returns a number between 0 and 1 indicating the greatest similarity
    between the query and any word in the document. This is useful for
    finding documents that contain words similar to the query terms.

    The current word similarity threshold is controlled by the
    pg_trgm.word_similarity_threshold GUC variable.

    Args:
        dialect: The SQL dialect instance
        column: Document column or text to search in
        query: Query text to search for

    Returns:
        FunctionCall for word_similarity(column, query)

    Example:
        >>> from rhosocial.activerecord.backend.impl.postgres import PostgresDialect
        >>> dialect = PostgresDialect()
        >>> word_similarity(dialect, 'title_col', 'search')
        >>> # Generates: word_similarity(title_col, 'search')
    """
    return core.FunctionCall(
        dialect, "word_similarity",
        _convert_to_expression(dialect, column),
        _convert_to_expression(dialect, query),
    )


# ============== Utility Functions ==============

def show_trgm(
    dialect: "SQLDialectBase",
    text: Union[str, "bases.BaseExpression"],
) -> core.FunctionCall:
    """Show the trigrams of a text string.

    Returns an array of all trigrams found in the given string.
    This is primarily useful for debugging and understanding how
    the trigram matching works.

    Args:
        dialect: The SQL dialect instance
        text: Text to extract trigrams from

    Returns:
        FunctionCall for show_trgm(text)

    Example:
        >>> from rhosocial.activerecord.backend.impl.postgres import PostgresDialect
        >>> dialect = PostgresDialect()
        >>> show_trgm(dialect, 'hello')
        >>> # Generates: show_trgm('hello')
    """
    return core.FunctionCall(
        dialect, "show_trgm",
        _convert_to_expression(dialect, text),
    )


# ============== Similarity Operators ==============

def similarity_operator(
    dialect: "SQLDialectBase",
    column: Union[str, "bases.BaseExpression"],
    text: Union[str, "bases.BaseExpression"],
    negate: bool = False,
) -> BinaryExpression:
    """Generate SQL expression for the similarity operator (%).

    The % operator returns true if the similarity between its arguments
    is greater than the current similarity threshold set by
    pg_trgm.similarity_threshold.

    When negate is True, the !% operator is used instead, which returns
    true when the similarity is below the threshold.

    Args:
        dialect: The SQL dialect instance
        column: Column or text expression to compare
        text: Text to compare against
        negate: If True, use the negated operator (!%) instead of %

    Returns:
        BinaryExpression for column % text (or column !% text if negate)

    Example:
        >>> from rhosocial.activerecord.backend.impl.postgres import PostgresDialect
        >>> dialect = PostgresDialect()
        >>> similarity_operator(dialect, 'name_col', 'search')
        >>> # Generates: name_col % 'search'
        >>> similarity_operator(dialect, 'name_col', 'search', negate=True)
        >>> # Generates: name_col !% 'search'
    """
    operator = "!%" if negate else "%"
    return BinaryExpression(
        dialect, operator,
        _convert_to_expression(dialect, column),
        _convert_to_expression(dialect, text),
    )


__all__ = [
    # Similarity functions
    "similarity",
    "word_similarity",
    # Utility functions
    "show_trgm",
    # Similarity operators
    "similarity_operator",
]
