# src/rhosocial/activerecord/backend/impl/postgres/protocols/extensions/fuzzystrmatch.py
"""fuzzystrmatch extension protocol definition.

This module defines the protocol for fuzzystrmatch fuzzy string matching
functionality in PostgreSQL.
"""

from typing import Protocol, runtime_checkable


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