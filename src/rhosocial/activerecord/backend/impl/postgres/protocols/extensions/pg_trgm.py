# src/rhosocial/activerecord/backend/impl/postgres/protocols/extensions/pg_trgm.py
"""pg_trgm extension protocol definition.

This module defines the protocol for pg_trgm trigram functionality
in PostgreSQL.
"""

from typing import Protocol, runtime_checkable


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

    Detection methods:
    - Automatic detection: introspect_and_adapt() queries pg_extension
    - Manual detection: SELECT * FROM pg_extension WHERE extname = 'pg_trgm';
    - Programmatic detection: dialect.is_extension_installed('pg_trgm')
    """

    def supports_pg_trgm_similarity(self) -> bool:
        """Whether pg_trgm trigram similarity calculation is supported.

        Requires pg_trgm extension.
        Supports similarity functions: similarity(), show_trgm(), etc.
        """
        ...

    def supports_pg_trgm_index(self) -> bool:
        """Whether pg_trgm trigram indexing is supported.

        Requires pg_trgm extension.
        Supports creating GiST or GIN trigram indexes on text columns.
        """
        ...
