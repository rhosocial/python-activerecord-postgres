# src/rhosocial/activerecord/backend/impl/postgres/functions/text_search.py
"""
PostgreSQL text search functions for SQL expression generation.

This module provides utility functions for generating PostgreSQL full-text search SQL expressions.

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

Examples:
    Generate tsvector from document:
        >>> to_tsvector("hello world", config="english")
        "to_tsvector('english', 'hello world')"

    Generate tsquery from query:
        >>> to_tsquery("hello & world", config="english")
        "to_tsquery('english', 'hello & world')"

    Test if tsvector matches tsquery:
        >>> ts_matches("title_tsvector", "to_tsquery('hello')")
        "title_tsvector @@ to_tsquery('hello')"

    Calculate relevance rank:
        >>> ts_rank("title_tsvector", "to_tsquery('hello')")
        "ts_rank(title_tsvector, to_tsquery('hello'))"

    Get highlighted text with matches:
        >>> ts_headline("content", "to_tsquery('hello')")
        "ts_headline(content, to_tsquery('hello'))"
"""

from typing import List, Optional


def to_tsvector(document: str, config: Optional[str] = None) -> str:
    """Generate SQL expression for PostgreSQL to_tsvector function.

    Args:
        document: The document text to convert to tsvector
        config: Optional text search configuration (e.g., 'english', 'simple')

    Returns:
        str: SQL expression for to_tsvector function

    Examples:
        >>> to_tsvector("hello world")
        "to_tsvector('hello world')"
        >>> to_tsvector("hello world", "english")
        "to_tsvector('english', 'hello world')"
    """
    if config:
        return f"to_tsvector('{config}', {document})"
    return f"to_tsvector({document})"


def to_tsquery(query: str, config: Optional[str] = None) -> str:
    """Generate SQL expression for PostgreSQL to_tsquery function.

    Args:
        query: The query string to convert to tsquery
        config: Optional text search configuration

    Returns:
        str: SQL expression for to_tsquery function

    Examples:
        >>> to_tsquery("hello & world")
        "to_tsquery('hello & world')"
        >>> to_tsquery("hello & world", "english")
        "to_tsquery('english', 'hello & world')"
    """
    if config:
        return f"to_tsquery('{config}', '{query}')"
    return f"to_tsquery('{query}')"


def plainto_tsquery(query: str, config: Optional[str] = None) -> str:
    """Generate SQL expression for PostgreSQL plainto_tsquery function.

    Converts plain text to tsquery, handling special characters.

    Args:
        query: The plain text query
        config: Optional text search configuration

    Returns:
        str: SQL expression for plainto_tsquery function

    Examples:
        >>> plainto_tsquery("hello world")
        "plainto_tsquery('hello world')"
        >>> plainto_tsquery("hello world", "english")
        "plainto_tsquery('english', 'hello world')"
    """
    if config:
        return f"plainto_tsquery('{config}', '{query}')"
    return f"plainto_tsquery('{query}')"


def phraseto_tsquery(query: str, config: Optional[str] = None) -> str:
    """Generate SQL expression for PostgreSQL phraseto_tsquery function.

    Converts text to tsquery for phrase search.

    Args:
        query: The phrase query string
        config: Optional text search configuration

    Returns:
        str: SQL expression for phraseto_tsquery function

    Examples:
        >>> phraseto_tsquery("hello world")
        "phraseto_tsquery('hello world')"
        >>> phraseto_tsquery("hello world", "english")
        "phraseto_tsquery('english', 'hello world')"
    """
    if config:
        return f"phraseto_tsquery('{config}', '{query}')"
    return f"phraseto_tsquery('{query}')"


def websearch_to_tsquery(query: str, config: Optional[str] = None) -> str:
    """Generate SQL expression for PostgreSQL websearch_to_tsquery function.

    Converts web search style query to tsquery (PostgreSQL 11+).

    Args:
        query: The web search query string
        config: Optional text search configuration

    Returns:
        str: SQL expression for websearch_to_tsquery function

    Examples:
        >>> websearch_to_tsquery("hello -world")
        "websearch_to_tsquery('hello -world')"
        >>> websearch_to_tsquery("hello -world", "english")
        "websearch_to_tsquery('english', 'hello -world')"
    """
    if config:
        return f"websearch_to_tsquery('{config}', '{query}')"
    return f"websearch_to_tsquery('{query}')"


def ts_matches(vector: str, query: str) -> str:
    """Generate SQL expression for tsvector @@ tsquery match operator.

    Args:
        vector: The tsvector expression or column
        query: The tsquery expression

    Returns:
        str: SQL expression using @@ operator

    Examples:
        >>> ts_matches("title_tsvector", "to_tsquery('hello')")
        "title_tsvector @@ to_tsquery('hello')"
    """
    return f"{vector} @@ {query}"


def ts_matches_expr(vector: str, query: str) -> str:
    """Generate SQL expression for tsvector @@@ tsquery match operator.

    The @@@ operator is a deprecated alias for @@.

    Args:
        vector: The tsvector expression or column
        query: The tsquery expression

    Returns:
        str: SQL expression using @@@ operator

    Examples:
        >>> ts_matches_expr("title_tsvector", "to_tsquery('hello')")
        "title_tsvector @@@ to_tsquery('hello')"
    """
    return f"{vector} @@@ {query}"


def ts_rank(vector: str, query: str, weights: Optional[List[float]] = None, normalization: int = 0) -> str:
    """Generate SQL expression for PostgreSQL ts_rank function.

    Calculates relevance rank for tsvector matching tsquery.

    Args:
        vector: The tsvector expression or column
        query: The tsquery expression
        weights: Optional list of 4 weights [D, C, B, A] (default [0.1, 0.2, 0.4, 1.0])
        normalization: Normalization method (0-32, default 0)

    Returns:
        str: SQL expression for ts_rank function

    Examples:
        >>> ts_rank("title_tsvector", "to_tsquery('hello')")
        "ts_rank(title_tsvector, to_tsquery('hello'))"
        >>> ts_rank("title_tsvector", "to_tsquery('hello')", [0.1, 0.2, 0.4, 1.0])
        "ts_rank(array[0.1, 0.2, 0.4, 1.0], title_tsvector, to_tsquery('hello'))"
        >>> ts_rank("title_tsvector", "to_tsquery('hello')", normalization=1)
        "ts_rank(title_tsvector, to_tsquery('hello'), 1)"
    """
    if weights:
        weights_str = f"array[{', '.join(str(w) for w in weights)}]"
        if normalization != 0:
            return f"ts_rank({weights_str}, {vector}, {query}, {normalization})"
        return f"ts_rank({weights_str}, {vector}, {query})"
    if normalization != 0:
        return f"ts_rank({vector}, {query}, {normalization})"
    return f"ts_rank({vector}, {query})"


def ts_rank_cd(vector: str, query: str, weights: Optional[List[float]] = None, normalization: int = 0) -> str:
    """Generate SQL expression for PostgreSQL ts_rank_cd function.

    Calculates relevance rank using cover density.

    Args:
        vector: The tsvector expression or column
        query: The tsquery expression
        weights: Optional list of 4 weights [D, C, B, A]
        normalization: Normalization method (0-32, default 0)

    Returns:
        str: SQL expression for ts_rank_cd function

    Examples:
        >>> ts_rank_cd("title_tsvector", "to_tsquery('hello')")
        "ts_rank_cd(title_tsvector, to_tsquery('hello'))"
        >>> ts_rank_cd("title_tsvector", "to_tsquery('hello')", [0.1, 0.2, 0.4, 1.0])
        "ts_rank_cd(array[0.1, 0.2, 0.4, 1.0], title_tsvector, to_tsquery('hello'))"
    """
    if weights:
        weights_str = f"array[{', '.join(str(w) for w in weights)}]"
        if normalization != 0:
            return f"ts_rank_cd({weights_str}, {vector}, {query}, {normalization})"
        return f"ts_rank_cd({weights_str}, {vector}, {query})"
    if normalization != 0:
        return f"ts_rank_cd({vector}, {query}, {normalization})"
    return f"ts_rank_cd({vector}, {query})"


def ts_headline(document: str, query: str, config: Optional[str] = None, options: Optional[str] = None) -> str:
    """Generate SQL expression for PostgreSQL ts_headline function.

    Displays query matches in document with highlighting.

    Args:
        document: The document text or expression
        query: The tsquery expression
        config: Optional text search configuration
        options: Optional headline options string

    Returns:
        str: SQL expression for ts_headline function

    Examples:
        >>> ts_headline("content", "to_tsquery('hello')")
        "ts_headline(content, to_tsquery('hello'))"
        >>> ts_headline("content", "to_tsquery('hello')", config="english")
        "ts_headline('english', content, to_tsquery('hello'))"
        >>> ts_headline("content", "to_tsquery('hello')", options="StartSel=<b>, StopSel=</b>")
        "ts_headline(content, to_tsquery('hello'), 'StartSel=<b>, StopSel=</b>')"
    """
    if config:
        if options:
            return f"ts_headline('{config}', {document}, {query}, '{options}')"
        return f"ts_headline('{config}', {document}, {query})"
    if options:
        return f"ts_headline({document}, {query}, '{options}')"
    return f"ts_headline({document}, {query})"


def tsvector_concat(vec1: str, vec2: str) -> str:
    """Generate SQL expression for tsvector concatenation.

    Args:
        vec1: First tsvector expression
        vec2: Second tsvector expression

    Returns:
        str: SQL expression using || operator

    Examples:
        >>> tsvector_concat("title_tsvector", "body_tsvector")
        "title_tsvector || body_tsvector"
    """
    return f"{vec1} || {vec2}"


def tsvector_strip(vec: str) -> str:
    """Generate SQL expression for strip function.

    Removes positions and weights from tsvector.

    Args:
        vec: The tsvector expression

    Returns:
        str: SQL expression for strip function

    Examples:
        >>> tsvector_strip("title_tsvector")
        "strip(title_tsvector)"
    """
    return f"strip({vec})"


def tsvector_setweight(vec: str, weight: str) -> str:
    """Generate SQL expression for setweight function.

    Sets weight for all lexemes in tsvector.

    Args:
        vec: The tsvector expression
        weight: Weight character ('A', 'B', 'C', or 'D')

    Returns:
        str: SQL expression for setweight function

    Examples:
        >>> tsvector_setweight("title_tsvector", "A")
        "setweight(title_tsvector, 'A')"
    """
    return f"setweight({vec}, '{weight}')"


def tsvector_length(vec: str) -> str:
    """Generate SQL expression for tsvector length.

    Returns the number of lexemes in tsvector.

    Args:
        vec: The tsvector expression

    Returns:
        str: SQL expression for length function

    Examples:
        >>> tsvector_length("title_tsvector")
        "length(title_tsvector)"
    """
    return f"length({vec})"


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
