# src/rhosocial/activerecord/backend/impl/postgres/mixins/extensions/fuzzystrmatch.py
"""
fuzzystrmatch fuzzy string matching functionality implementation.

This module provides the PostgresFuzzystrmatchMixin class that adds support for
fuzzystrmatch extension features.
"""

from typing import TYPE_CHECKING, Tuple

if TYPE_CHECKING:
    pass


class PostgresFuzzystrmatchMixin:
    """fuzzystrmatch fuzzy string matching functionality implementation."""

    def supports_fuzzystrmatch(self) -> bool:
        """Check if fuzzystrmatch extension is available."""
        return self.is_extension_installed("fuzzystrmatch")

    def format_levenshtein(self, source: str, target: str) -> str:
        """Format Levenshtein distance calculation.

        Args:
            source: Source string
            target: Target string

        Returns:
            SQL function call
        """
        return f"levenshtein('{source}', '{target}')"

    def format_levenshtein_less_equal(
        self, source: str, target: str, threshold: int
    ) -> str:
        """Format bounded Levenshtein distance.

        Args:
            source: Source string
            target: Target string
            threshold: Maximum distance

        Returns:
            SQL function call
        """
        return f"levenshtein_less_equal('{source}', '{target}', {threshold})"

    def format_soundex(self, text: str) -> str:
        """Format Soundex encoding.

        Args:
            text: Input text

        Returns:
            SQL function call
        """
        return f"soundex('{text}')"

    def format_dmetaphone(self, text: str) -> str:
        """Format Double Metaphone encoding.

        Args:
            text: Input text

        Returns:
            SQL function call
        """
        return f"dmetaphone('{text}')"

    def format_dmetaphone_alt(self, text: str) -> str:
        """Format alternative Double Metaphone encoding.

        Args:
            text: Input text

        Returns:
            SQL function call
        """
        return f"dmetaphone_alt('{text}')"

    def format_difference(self, s1_expr: str, s2_expr: str) -> Tuple[str, tuple]:
        """Format difference function.

        The difference() function returns an integer indicating how similar
        the Soundex codes of two strings are.

        Args:
            s1_expr: First string expression
            s2_expr: Second string expression

        Returns:
            Tuple of (SQL function call, parameters)

        Example:
            >>> format_difference("'hello'", "'helo'")
            ("difference('hello', 'helo')", ())
        """
        return (f"difference({s1_expr}, {s2_expr})", ())

    def format_metaphone(self, str_expr: str, max_length: int) -> Tuple[str, tuple]:
        """Format metaphone function.

        The metaphone() function returns a metaphone code of specified
        maximum length for the input string.

        Args:
            str_expr: Input string expression
            max_length: Maximum length of the metaphone code

        Returns:
            Tuple of (SQL function call, parameters)

        Example:
            >>> format_metaphone("'hello'", 6)
            ("metaphone('hello', 6)", ())
        """
        return (f"metaphone({str_expr}, {max_length})", ())