# src/rhosocial/activerecord/backend/impl/postgres/mixins/types/full_text_search.py
"""
PostgreSQL full-text search mixin.

Implements the PostgresFullTextSearchSupport protocol.
"""


class PostgresFullTextSearchMixin:
    """Mixin for PostgreSQL full-text search support.

    Provides version-aware capability detection for PostgreSQL
    full-text search features.
    """

    def supports_full_text_search(self) -> bool:
        """Whether basic full-text search is supported (PostgreSQL 8.3+)."""
        return self.version >= (8, 3, 0)

    def supports_ts_rank_cd(self) -> bool:
        """Whether coverage density ranking is supported (PostgreSQL 8.5+)."""
        return self.version >= (8, 5, 0)

    def supports_phrase_search(self) -> bool:
        """Whether phrase search is supported (PostgreSQL 9.6+)."""
        return self.version >= (9, 6, 0)

    def supports_websearch_tsquery(self) -> bool:
        """Whether web-style search is supported (PostgreSQL 11+)."""
        return self.version >= (11, 0, 0)
