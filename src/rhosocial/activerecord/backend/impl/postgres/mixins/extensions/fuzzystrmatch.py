# src/rhosocial/activerecord/backend/impl/postgres/mixins/extensions/fuzzystrmatch.py
"""
fuzzystrmatch fuzzy string matching functionality implementation.

This module provides the PostgresFuzzystrmatchMixin class that adds support for
fuzzystrmatch extension features.
"""

from typing import TYPE_CHECKING

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