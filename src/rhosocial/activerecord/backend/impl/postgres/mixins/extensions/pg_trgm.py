# Copyright (C) 2025 Rhosocial
# SPDX-License-Identifier: MIT
# Author: vistart <vistart@rhosocial.com>
#
# This file is part of the rhosocial-activerecord-postgres package.
# It provides pg_trgm (trigram) extension mixin for PostgreSQL backend.
#
# pg_trgm is a PostgreSQL extension that provides functions and operators
# for determining the similarity of alphanumeric text based on trigram
# matching, as well as index operator classes that support fast searching
# for similar strings.
#
# For more information about pg_trgm, see:
# https://www.postgresql.org/docs/current/pgtrgm.html

"""
pg_trgm trigram functionality implementation.

This module provides the PostgresPgTrgmMixin class that adds support for
pg_trgm extension features including similarity functions and trigram indexes.
"""

from typing import TYPE_CHECKING, Optional, Tuple

if TYPE_CHECKING:
    pass  # No type imports needed for this mixin


class PostgresPgTrgmMixin:
    """pg_trgm trigram functionality implementation."""

    def supports_pg_trgm_similarity(self) -> bool:
        """Check if pg_trgm supports similarity functions."""
        return self.check_extension_feature('pg_trgm', 'similarity')

    def supports_pg_trgm_index(self) -> bool:
        """Check if pg_trgm supports trigram index."""
        return self.check_extension_feature('pg_trgm', 'index')

    def format_similarity_expression(
        self,
        column: str,
        text: str,
        threshold: Optional[float] = None,
        use_operator: bool = True
    ) -> str:
        """Format a similarity expression.

        Args:
            column: The text column name
            text: The text to compare with
            threshold: Optional similarity threshold (0-1)
            use_operator: Use operator (%) instead of function

        Returns:
            SQL similarity expression

        Example:
            >>> format_similarity_expression('name', 'search_term')
            "name % 'search_term'"
            >>> format_similarity_expression('name', 'search_term', threshold=0.3)
            "name % 'search_term' AND similarity(name, 'search_term') > 0.3"
        """
        if use_operator:
            expr = f"{column} % '{text}'"
            if threshold is not None:
                expr += f" AND similarity({column}, '{text}') > {threshold}"
            return expr
        else:
            if threshold is not None:
                return f"similarity({column}, '{text}') > {threshold}"
            return f"similarity({column}, '{text}')"

    def format_trgm_index_statement(
        self,
        index_name: str,
        table_name: str,
        column_name: str,
        index_type: str = 'gin',
        schema: Optional[str] = None
    ) -> Tuple[str, tuple]:
        """Format CREATE INDEX statement for trigram index.

        Args:
            index_name: Name of the index
            table_name: Table name
            column_name: Text column name
            index_type: Index type - 'gin' (default) or 'gist'
            schema: Optional schema name

        Returns:
            Tuple of (SQL statement, parameters)

        Example:
            >>> format_trgm_index_statement('idx_name_trgm', 'users', 'name')
            ("CREATE INDEX idx_name_trgm ON users USING gin (name gin_trgm_ops)", ())
        """
        full_table = f"{schema}.{table_name}" if schema else table_name
        sql = f"CREATE INDEX {index_name} ON {full_table} USING {index_type} ({column_name} gin_trgm_ops)"
        return (sql, ())

    def format_similarity_function(self, text1: str, text2: str) -> str:
        """Format similarity function call.

        Args:
            text1: First text
            text2: Second text

        Returns:
            SQL similarity function call

        Example:
            >>> format_similarity_function('name', "'search'")
            "similarity(name, 'search')"
        """
        return f"similarity({text1}, {text2})"

    def format_show_trgm(self, text: str) -> str:
        """Format show_trgm function (show trigrams of text).

        Args:
            text: Text to analyze

        Returns:
            SQL function call

        Example:
            >>> format_show_trgm("'hello'")
            "show_trgm('hello')"
        """
        return f"show_trgm({text})"

    def format_word_similarity(self, column: str, query: str) -> str:
        """Format word_similarity function.

        Word similarity compares based on matching trigrams in query order.

        Args:
            column: The text column
            query: The query string

        Returns:
            SQL function call

        Example:
            >>> format_word_similarity('name', "'search'")
            "word_similarity(name, 'search')"
        """
        return f"word_similarity({column}, '{query}')"

    def format_similarity_operator(self, column: str, text: str, negate: bool = False) -> str:
        """Format similarity operator (%).

        Args:
            column: The text column
            text: The text to compare
            negate: Use != instead of ==

        Returns:
            SQL operator expression

        Example:
            >>> format_similarity_operator('name', 'search')
            "name % 'search'"
            >>> format_similarity_operator('name', 'search', negate=True)
            "name !% 'search'"
        """
        op = "!%" if negate else "%"
        return f"{column} {op} '{text}'"

    def format_similarity_threshold(self, threshold: float) -> str:
        """Format SET similarity_threshold statement.

        Args:
            threshold: Similarity threshold value (0-1)

        Returns:
            SQL SET statement

        Example:
            >>> format_similarity_threshold(0.3)
            "SET similarity_threshold = 0.3"
        """
        return f"SET similarity_threshold = {threshold}"
