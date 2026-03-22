# src/rhosocial/activerecord/backend/impl/postgres/protocols/sql_syntax.py
"""PostgreSQL SQL syntax enhancements protocol.

This module defines the PostgresSQLSyntaxSupport protocol for PostgreSQL-specific
SQL syntax features that extend beyond the SQL standard.
"""
from typing import Protocol, runtime_checkable


@runtime_checkable
class PostgresSQLSyntaxSupport(Protocol):
    """PostgreSQL SQL syntax enhancements protocol.

    Feature Source: Native support (no extension required)

    PostgreSQL SQL syntax features:
    - Generated columns (PG 12+)
    - CTE SEARCH/CYCLE (PG 14+)
    - FETCH WITH TIES (PG 13+)

    Official Documentation:
    - Generated Columns: https://www.postgresql.org/docs/current/ddl-generated-columns.html
    - CTE SEARCH/CYCLE: https://www.postgresql.org/docs/current/queries-with.html
    - FETCH WITH TIES: https://www.postgresql.org/docs/current/sql-select.html#SQL-LIMIT

    Version Requirements:
    - Generated columns: PostgreSQL 12+
    - FETCH WITH TIES: PostgreSQL 13+
    - CTE SEARCH/CYCLE: PostgreSQL 14+
    """

    def supports_generated_columns(self) -> bool:
        """Whether generated columns are supported.

        Native feature, PostgreSQL 12+.
        Enables GENERATED ALWAYS AS columns.
        """
        ...

    def supports_cte_search_cycle(self) -> bool:
        """Whether CTE SEARCH and CYCLE clauses are supported.

        Native feature, PostgreSQL 14+.
        Enables SQL-standard SEARCH and CYCLE in CTEs.
        """
        ...

    def supports_fetch_with_ties(self) -> bool:
        """Whether FETCH FIRST WITH TIES is supported.

        Native feature, PostgreSQL 13+.
        Includes tied rows in result set.
        """
        ...
