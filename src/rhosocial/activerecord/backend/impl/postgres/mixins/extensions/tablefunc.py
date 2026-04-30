# src/rhosocial/activerecord/backend/impl/postgres/mixins/extensions/tablefunc.py
"""
PostgreSQL tablefunc table functions functionality mixin.

This module provides functionality to check tablefunc extension features,
including crosstab, connectby, and normal_rand functions, as well as
complete query templates for crosstab and connectby with output definitions.

For simple SQL expression generation (crosstab, connectby, normal_rand
function calls), use the function factories in ``functions/tablefunc.py``
instead of the removed format_* methods.
"""

from typing import Optional


class PostgresTablefuncMixin:
    """tablefunc table functions implementation."""

    def supports_tablefunc_crosstab(self) -> bool:
        """Check if tablefunc supports crosstab."""
        return self.check_extension_feature("tablefunc", "crosstab")

    def supports_tablefunc_connectby(self) -> bool:
        """Check if tablefunc supports connectby."""
        return self.check_extension_feature("tablefunc", "connectby")

    def supports_tablefunc_normal_rand(self) -> bool:
        """Check if tablefunc supports normal_rand."""
        return self.check_extension_feature("tablefunc", "normal_rand")

    def format_crosstab_with_definition(
        self, source_sql: str, output_columns: str, categories_sql: Optional[str] = None
    ) -> str:
        """Format crosstab function with explicit column definition.

        This generates a complete SELECT query wrapping the crosstab function
        with an AS output column definition, which is required for crosstab
        to return properly typed results.

        Args:
            source_sql: Source query
            output_columns: Column definition string
            categories_sql: Optional categories query

        Returns:
            SQL crosstab query with column definition

        Example:
            >>> format_crosstab_with_definition("SELECT row, cat, val FROM t", "row text, cat1 int, cat2 int")
            "SELECT * FROM crosstab('SELECT row, cat, val FROM t') AS ct(row text, cat1 int, cat2 int)"
        """
        if categories_sql:
            return f"SELECT * FROM crosstab('{source_sql}', '{categories_sql}') AS ct({output_columns})"
        return f"SELECT * FROM crosstab('{source_sql}') AS ct({output_columns})"

    def format_connectby_full(
        self,
        table_name: str,
        key_column: str,
        parent_column: str,
        start_value: str,
        max_depth: int,
        branch_delim: str = "~",
    ) -> str:
        """Format connectby function with full options.

        This generates a complete SELECT query wrapping the connectby function
        with an AS output column definition, which is required for connectby
        to return properly typed results.

        Args:
            table_name: Table name
            key_column: Primary key column
            parent_column: Parent reference column
            start_value: Starting value
            max_depth: Maximum depth
            branch_delim: Branch delimiter

        Returns:
            SQL connectby query

        Example:
            >>> format_connectby_full('categories', 'id', 'parent_id', '1', 5)
            ("SELECT * FROM connectby('categories', 'id', 'parent_id', '1', 5, '~') "
             "AS t(keyid text, parent_keyid text, level int, branch text, sort_column text)")
        """
        return (
            f"SELECT * FROM connectby('{table_name}', '{key_column}', "
            f"'{parent_column}', '{start_value}', {max_depth}, '{branch_delim}') "
            f"AS t(keyid text, parent_keyid text, level int, branch text)"
        )
