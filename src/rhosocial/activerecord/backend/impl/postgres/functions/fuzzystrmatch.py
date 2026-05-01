# src/rhosocial/activerecord/backend/impl/postgres/functions/fuzzystrmatch.py
"""
PostgreSQL fuzzystrmatch extension function factories.

This module provides SQL expression generators for PostgreSQL fuzzystrmatch
extension functions. All functions return FunctionCall expression objects
that integrate with the Expression/Dialect architecture.

The fuzzystrmatch extension provides functions for fuzzy string matching,
including Levenshtein distance, Soundex, and Double Metaphone algorithms.

PostgreSQL Documentation: https://www.postgresql.org/docs/current/fuzzystrmatch.html

The fuzzystrmatch extension must be installed:
    CREATE EXTENSION IF NOT EXISTS fuzzystrmatch;

Supported functions:
- levenshtein: Calculate Levenshtein distance between two strings
- levenshtein_less_equal: Calculate Levenshtein distance with a maximum threshold
- soundex: Calculate the Soundex code for a string
- difference: Calculate the difference between two Soundex codes
- dmetaphone: Calculate the Double Metaphone code for a string
- dmetaphone_alt: Calculate the alternate Double Metaphone code for a string
- metaphone: Calculate the Metaphone code for a string
"""

from typing import Union, TYPE_CHECKING

from rhosocial.activerecord.backend.expression import bases, core

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


# ============== Levenshtein Distance Functions ==============

def levenshtein(
    dialect: "SQLDialectBase",
    source: Union[str, "bases.BaseExpression"],
    target: Union[str, "bases.BaseExpression"],
) -> core.FunctionCall:
    """Calculate the Levenshtein distance between two strings.

    The Levenshtein distance is the minimum number of single-character
    edits (insertions, deletions, or substitutions) required to change
    one string into the other.

    Args:
        dialect: The SQL dialect instance
        source: The source string expression
        target: The target string expression

    Returns:
        FunctionCall for levenshtein(source, target)

    Example:
        >>> from rhosocial.activerecord.backend.impl.postgres import PostgresDialect
        >>> dialect = PostgresDialect()
        >>> levenshtein(dialect, 'name_col', 'search_term')
    """
    return core.FunctionCall(
        dialect, "levenshtein",
        _convert_to_expression(dialect, source),
        _convert_to_expression(dialect, target),
    )


def levenshtein_less_equal(
    dialect: "SQLDialectBase",
    source: Union[str, "bases.BaseExpression"],
    target: Union[str, "bases.BaseExpression"],
    threshold: Union[int, "bases.BaseExpression"],
) -> core.FunctionCall:
    """Calculate the Levenshtein distance with a maximum threshold.

    This function computes the Levenshtein distance but stops when the
    distance exceeds the specified threshold, returning the threshold
    value. This is significantly faster than the full levenshtein()
    function for large strings when only small distances are of interest.

    Args:
        dialect: The SQL dialect instance
        source: The source string expression
        target: The target string expression
        threshold: The maximum distance to compute (non-negative integer)

    Returns:
        FunctionCall for levenshtein_less_equal(source, target, threshold)

    Example:
        >>> levenshtein_less_equal(dialect, 'name_col', 'search_term', 3)
    """
    return core.FunctionCall(
        dialect, "levenshtein_less_equal",
        _convert_to_expression(dialect, source),
        _convert_to_expression(dialect, target),
        _convert_to_expression(dialect, threshold),
    )


# ============== Soundex Functions ==============

def soundex(
    dialect: "SQLDialectBase",
    text: Union[str, "bases.BaseExpression"],
) -> core.FunctionCall:
    """Calculate the Soundex code for a string.

    The Soundex system converts similar-sounding strings into the same
    code, which is useful for phonetic matching of names.

    Args:
        dialect: The SQL dialect instance
        text: The string expression to encode

    Returns:
        FunctionCall for soundex(text)

    Example:
        >>> soundex(dialect, 'name_col')
    """
    return core.FunctionCall(
        dialect, "soundex",
        _convert_to_expression(dialect, text),
    )


def difference(
    dialect: "SQLDialectBase",
    source: Union[str, "bases.BaseExpression"],
    target: Union[str, "bases.BaseExpression"],
) -> core.FunctionCall:
    """Calculate the difference between two Soundex codes.

    Converts both strings to Soundex codes and returns the number of
    matching code positions. The result ranges from 0 (no similarity)
    to 4 (identical Soundex codes).

    Args:
        dialect: The SQL dialect instance
        source: The first string expression
        target: The second string expression

    Returns:
        FunctionCall for difference(source, target)

    Example:
        >>> difference(dialect, 'name_col', 'search_term')
    """
    return core.FunctionCall(
        dialect, "difference",
        _convert_to_expression(dialect, source),
        _convert_to_expression(dialect, target),
    )


# ============== Metaphone Functions ==============

def metaphone(
    dialect: "SQLDialectBase",
    text: Union[str, "bases.BaseExpression"],
    max_length: Union[int, "bases.BaseExpression"],
) -> core.FunctionCall:
    """Calculate the Metaphone code for a string.

    The Metaphone system is an improvement over Soundex for phonetic
    matching of English words. The max_length parameter specifies the
    maximum length of the returned code.

    Args:
        dialect: The SQL dialect instance
        text: The string expression to encode
        max_length: Maximum length of the output code (positive integer)

    Returns:
        FunctionCall for metaphone(text, max_length)

    Example:
        >>> metaphone(dialect, 'name_col', 6)
    """
    return core.FunctionCall(
        dialect, "metaphone",
        _convert_to_expression(dialect, text),
        _convert_to_expression(dialect, max_length),
    )


def dmetaphone(
    dialect: "SQLDialectBase",
    text: Union[str, "bases.BaseExpression"],
) -> core.FunctionCall:
    """Calculate the primary Double Metaphone code for a string.

    The Double Metaphone system produces two codes for each input string
    (a primary and an alternate), providing better phonetic matching than
    the original Metaphone algorithm, especially for non-English words.

    Args:
        dialect: The SQL dialect instance
        text: The string expression to encode

    Returns:
        FunctionCall for dmetaphone(text)

    Example:
        >>> dmetaphone(dialect, 'name_col')
    """
    return core.FunctionCall(
        dialect, "dmetaphone",
        _convert_to_expression(dialect, text),
    )


def dmetaphone_alt(
    dialect: "SQLDialectBase",
    text: Union[str, "bases.BaseExpression"],
) -> core.FunctionCall:
    """Calculate the alternate Double Metaphone code for a string.

    Returns the alternate code from the Double Metaphone algorithm,
    which represents a secondary phonetic interpretation of the
    input string. This is useful for matching words with ambiguous
    pronunciations.

    Args:
        dialect: The SQL dialect instance
        text: The string expression to encode

    Returns:
        FunctionCall for dmetaphone_alt(text)

    Example:
        >>> dmetaphone_alt(dialect, 'name_col')
    """
    return core.FunctionCall(
        dialect, "dmetaphone_alt",
        _convert_to_expression(dialect, text),
    )


__all__ = [
    "levenshtein",
    "levenshtein_less_equal",
    "soundex",
    "difference",
    "metaphone",
    "dmetaphone",
    "dmetaphone_alt",
]
