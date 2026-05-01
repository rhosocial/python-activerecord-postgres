# src/rhosocial/activerecord/backend/impl/postgres/protocols/extensions/pg_trgm.py
"""pg_trgm extension protocol definition.

This module defines the protocol for pg_trgm trigram functionality
in PostgreSQL.

For SQL expression generation, use the function factories in
``functions/pg_trgm.py`` instead of the removed format_* methods.
"""

from typing import Optional, Protocol, Tuple, runtime_checkable


@runtime_checkable
class PostgresPgTrgmSupport(Protocol):
    """pg_trgm trigram functionality protocol.

    Feature Source: Extension support (requires pg_trgm extension)

    pg_trgm provides trigram-based text similarity search:
    - Similarity calculation
    - Fuzzy search
    - Similarity indexing

    Extension Information:
    - Extension name: pg_trgm
    - Install command: CREATE EXTENSION pg_trgm;
    - Minimum version: 1.0
    - Documentation: https://www.postgresql.org/docs/current/pgtrgm.html
    """

    def supports_pg_trgm_similarity(self) -> bool:
        """Whether pg_trgm trigram similarity calculation is supported."""
        ...

    def supports_pg_trgm_index(self) -> bool:
        """Whether pg_trgm trigram indexing is supported."""
        ...

    def format_trgm_index_statement(
        self, index_name: str, table_name: str, column_name: str, index_type: str = "gin", schema: Optional[str] = None
    ) -> Tuple[str, tuple]:
        """Format CREATE INDEX statement for trigram index."""
        ...
