# src/rhosocial/activerecord/backend/impl/postgres/protocols/types/full_text_search.py
"""PostgreSQL full-text search support protocol definition.

This module defines the protocol for PostgreSQL full-text search features,
including tsvector/tsquery types, text search functions, and ranking.
"""

from typing import Protocol, runtime_checkable


@runtime_checkable
class PostgresFullTextSearchSupport(Protocol):
    """PostgreSQL full-text search support protocol.

    PostgreSQL provides comprehensive full-text search support with
    tsvector and tsquery data types, various search functions, and
    ranking capabilities.

    Full-text search features:
    - tsvector/tsquery data types
    - to_tsvector / to_tsquery / plainto_tsquery (PG 8.3+)
    - ts_rank / ts_headline / ts_matches (PG 8.3+)
    - ts_rank_cd coverage density ranking (PG 8.5+)
    - phraseto_tsquery phrase search (PG 9.6+)
    - websearch_to_tsquery web-style search (PG 11+)

    Feature Source: Native support (no extension required)

    Official Documentation:
    https://www.postgresql.org/docs/current/functions-textsearch.html

    Version Requirements:
    - Basic text search: PostgreSQL 8.3+
    - Coverage density ranking: PostgreSQL 8.5+
    - Phrase search: PostgreSQL 9.6+
    - Web-style search: PostgreSQL 11+
    """

    def supports_full_text_search(self) -> bool:
        """Whether basic full-text search is supported.

        Native feature, PostgreSQL 8.3+.
        Includes to_tsvector, to_tsquery, plainto_tsquery,
        ts_matches, ts_rank, ts_headline, and tsvector operations.
        """
        ...

    def supports_ts_rank_cd(self) -> bool:
        """Whether coverage density ranking (ts_rank_cd) is supported.

        Native feature, PostgreSQL 8.5+.
        Provides ranking based on how close query terms are to each other.
        """
        ...

    def supports_phrase_search(self) -> bool:
        """Whether phrase search (phraseto_tsquery) is supported.

        Native feature, PostgreSQL 9.6+.
        Enables searching for exact phrases, not just individual words.
        """
        ...

    def supports_websearch_tsquery(self) -> bool:
        """Whether web-style search (websearch_to_tsquery) is supported.

        Native feature, PostgreSQL 11+.
        Provides a web-search-style query parser that supports
        quoted phrases, OR, and negation operators.
        """
        ...
