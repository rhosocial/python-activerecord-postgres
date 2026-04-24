# src/rhosocial/activerecord/backend/impl/postgres/protocols/extensions/fuzzystrmatch.py
"""fuzzystrmatch extension protocol definition.

This module defines the protocol for fuzzystrmatch fuzzy string matching
functionality in PostgreSQL.
"""

from typing import Protocol, Tuple, runtime_checkable


@runtime_checkable
class PostgresFuzzystrmatchSupport(Protocol):
    """fuzzystrmatch fuzzy string matching extension protocol.

    Feature Source: Extension support (requires fuzzystrmatch extension)

    fuzzystrmatch provides fuzzy string matching:
    - Levenshtein distance
    - Soundex

    Extension Information:
    - Extension name: fuzzystrmatch
    - Install command: CREATE EXTENSION fuzzystrmatch;
    - Minimum version: 1.0
    - Documentation: https://www.postgresql.org/docs/current/fuzzystrmatch.html
    """

    def supports_fuzzystrmatch(self) -> bool:
        """Whether fuzzystrmatch is available."""
        ...

    def format_levenshtein(self, source: str, target: str) -> str:
        """Format Levenshtein distance calculation."""
        ...

    def format_levenshtein_less_equal(self, source: str, target: str, threshold: int) -> str:
        """Format bounded Levenshtein distance."""
        ...

    def format_soundex(self, text: str) -> str:
        """Format Soundex encoding."""
        ...

    def format_dmetaphone(self, text: str) -> str:
        """Format Double Metaphone encoding."""
        ...

    def format_dmetaphone_alt(self, text: str) -> str:
        """Format alternative Double Metaphone encoding."""
        ...

    def format_difference(self, s1_expr: str, s2_expr: str) -> Tuple[str, tuple]:
        """Format difference function."""
        ...

    def format_metaphone(self, str_expr: str, max_length: int) -> Tuple[str, tuple]:
        """Format metaphone function."""
        ...