# src/rhosocial/activerecord/backend/impl/postgres/mixins/sql_syntax.py
"""PostgreSQL SQL syntax enhancements mixin."""


class PostgresSQLSyntaxMixin:
    """PostgreSQL SQL syntax enhancements implementation.

    All features are native, using version number for detection.
    """

    def supports_generated_columns(self) -> bool:
        """Generated columns are native feature, PG 12+."""
        return self.version >= (12, 0, 0)

    def supports_cte_search_cycle(self) -> bool:
        """CTE SEARCH/CYCLE is native feature, PG 14+."""
        return self.version >= (14, 0, 0)

    def supports_fetch_with_ties(self) -> bool:
        """FETCH WITH TIES is native feature, PG 13+."""
        return self.version >= (13, 0, 0)

    # =========================================================================
    # Stored Procedures Support (PostgreSQL 11+)
    # =========================================================================

    def supports_call_statement(self) -> bool:
        """CALL statement for stored procedures is supported since PostgreSQL 11.

        PostgreSQL 11 introduced stored procedures (not to be confused
        with functions) that can manage their own transactions.

        Returns:
            True if CALL statement is supported
        """
        return self.version >= (11, 0, 0)

    def supports_stored_procedure_transaction_control(self) -> bool:
        """Stored procedures with transaction control are supported since PostgreSQL 11.

        Procedures can use COMMIT and ROLLBACK within the procedure body.

        Returns:
            True if procedure transaction control is supported
        """
        return self.version >= (11, 0, 0)

    def supports_sql_body_functions(self) -> bool:
        """SQL-body functions are supported since PostgreSQL 14.

        Functions can use SQL-standard function bodies with
        BEGIN ATOMIC ... END syntax.

        Returns:
            True if SQL body functions are supported
        """
        return self.version >= (14, 0, 0)

    # =========================================================================
    # PostgreSQL 15+ Features
    # =========================================================================

    def supports_nulls_not_distinct_unique(self) -> bool:
        """UNIQUE NULLS NOT DISTINCT is supported since PostgreSQL 15.

        Creates unique constraints that treat NULL values as equal,
        allowing only one NULL in the column.

        Returns:
            True if supported
        """
        return self.version >= (15, 0, 0)

    # =========================================================================
    # PostgreSQL 16+ Features
    # =========================================================================

    def supports_regexp_like(self) -> bool:
        """REGEXP_LIKE function is supported since PostgreSQL 16.

        SQL-standard regular expression matching function.

        Returns:
            True if supported
        """
        return self.version >= (16, 0, 0)

    def supports_random_normal(self) -> bool:
        """random_normal() function is supported since PostgreSQL 16.

        Returns normally distributed random values.

        Returns:
            True if supported
        """
        return self.version >= (16, 0, 0)

    # =========================================================================
    # PostgreSQL 17+ Features
    # =========================================================================

    def supports_json_table_nested_path(self) -> bool:
        """Enhanced JSON_TABLE NESTED PATH is supported since PostgreSQL 17.

        Returns:
            True if supported
        """
        return self.version >= (17, 0, 0)

    def supports_merge_with_cte(self) -> bool:
        """MERGE statement with CTE is supported since PostgreSQL 17.

        Returns:
            True if supported
        """
        return self.version >= (17, 0, 0)

    def supports_update_returning_old(self) -> bool:
        """UPDATE RETURNING OLD is supported since PostgreSQL 17.

        Allows returning old values in UPDATE RETURNING.

        Returns:
            True if supported
        """
        return self.version >= (17, 0, 0)
