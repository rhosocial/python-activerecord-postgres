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

    def supports_call_statement(self) -> bool:
        """Whether CALL statement for stored procedures is supported.

        Native feature, PostgreSQL 11+.
        PostgreSQL 11 introduced stored procedures that can manage their own transactions.
        """
        ...

    def supports_stored_procedure_transaction_control(self) -> bool:
        """Whether stored procedures with transaction control are supported.

        Native feature, PostgreSQL 11+.
        Procedures can use COMMIT and ROLLBACK within the procedure body.
        """
        ...

    def supports_sql_body_functions(self) -> bool:
        """Whether SQL-body functions are supported.

        Native feature, PostgreSQL 14+.
        Functions can use SQL-standard function bodies with
        BEGIN ATOMIC ... END syntax.
        """
        ...

    def supports_nulls_not_distinct_unique(self) -> bool:
        """Whether UNIQUE NULLS NOT DISTINCT is supported.

        Native feature, PostgreSQL 15+.
        Creates unique constraints that treat NULL values as equal.
        """
        ...

    def supports_regexp_like(self) -> bool:
        """Whether REGEXP_LIKE function is supported.

        Native feature, PostgreSQL 16+.
        SQL-standard regular expression matching function.
        """
        ...

    def supports_random_normal(self) -> bool:
        """Whether random_normal() function is supported.

        Native feature, PostgreSQL 16+.
        Returns normally distributed random values.
        """
        ...

    def supports_json_table_nested_path(self) -> bool:
        """Whether enhanced JSON_TABLE NESTED PATH is supported.

        Native feature, PostgreSQL 17+.
        """
        ...

    def supports_merge_with_cte(self) -> bool:
        """Whether MERGE statement with CTE is supported.

        Native feature, PostgreSQL 17+.
        """
        ...

    def supports_update_returning_old(self) -> bool:
        """Whether UPDATE RETURNING OLD is supported.

        Native feature, PostgreSQL 17+.
        Allows returning old values in UPDATE RETURNING.
        """
        ...
