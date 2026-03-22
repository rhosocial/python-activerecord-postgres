# src/rhosocial/activerecord/backend/impl/postgres/mixins/extensions/tablefunc.py
"""PostgreSQL tablefunc table functions functionality mixin.

This module provides functionality to check tablefunc extension features,
including crosstab, connectby, and normal_rand functions.
"""

from typing import Optional, Tuple


class PostgresTablefuncMixin:
    """tablefunc table functions implementation."""

    def supports_tablefunc_crosstab(self) -> bool:
        """Check if tablefunc supports crosstab."""
        return self.check_extension_feature('tablefunc', 'crosstab')

    def supports_tablefunc_connectby(self) -> bool:
        """Check if tablefunc supports connectby."""
        return self.check_extension_feature('tablefunc', 'connectby')

    def supports_tablefunc_normal_rand(self) -> bool:
        """Check if tablefunc supports normal_rand."""
        return self.check_extension_feature('tablefunc', 'normal_rand')

    def format_crosstab_function(
        self,
        sql: str,
        categories: Optional[str] = None
    ) -> str:
        """Format crosstab function for pivot queries.

        Args:
            sql: Source SQL query that returns row_name, category, value
            categories: Optional categories SQL (for crosstab with 2 parameters)

        Returns:
            SQL crosstab function call

        Example:
            >>> format_crosstab_function("SELECT row_name, cat, value FROM table")
            "crosstab('SELECT row_name, cat, value FROM table')"
            >>> format_crosstab_function("SELECT row_name, cat, value FROM table", "SELECT DISTINCT cat FROM table ORDER BY 1")
            "crosstab('SELECT row_name, cat, value FROM table', 'SELECT DISTINCT cat FROM table ORDER BY 1')"
        """
        if categories:
            return f"crosstab('{sql}', '{categories}')"
        return f"crosstab('{sql}')"

    def format_connectby_function(
        self,
        table_name: str,
        key_column: str,
        parent_column: str,
        start_value: str,
        max_depth: Optional[int] = None,
        branch_delim: str = '~',
        order_by_column: Optional[str] = None
    ) -> str:
        """Format connectby function for hierarchical queries.

        Args:
            table_name: Table name
            key_column: Primary key column name
            parent_column: Parent reference column name
            start_value: Starting key value
            max_depth: Maximum depth (optional)
            branch_delim: Branch delimiter character
            order_by_column: Optional column to order siblings

        Returns:
            SQL connectby function call

        Example:
            >>> format_connectby_function('tree', 'id', 'parent_id', '1')
            "connectby('tree', 'id', 'parent_id', '1')"
            >>> format_connectby_function('tree', 'id', 'parent_id', '1', max_depth=3)
            "connectby('tree', 'id', 'parent_id', '1', 3)"
        """
        params = [f"'{table_name}'", f"'{key_column}'", f"'{parent_column}'", f"'{start_value}'"]
        if max_depth is not None:
            params.append(str(max_depth))
        return f"connectby({', '.join(params)})"

    def format_normal_rand_function(
        self,
        num_values: int,
        mean: float,
        stddev: float,
        seed: Optional[int] = None
    ) -> str:
        """Format normal_rand function for random values.

        Args:
            num_values: Number of random values to generate
            mean: Mean of the normal distribution
            stddev: Standard deviation
            seed: Optional seed for reproducible results

        Returns:
            SQL normal_rand function call

        Example:
            >>> format_normal_rand_function(100, 0, 1)
            "normal_rand(100, 0, 1)"
        """
        return f"normal_rand({num_values}, {mean}, {stddev})"

    def format_crosstab_with_definition(
        self,
        source_sql: str,
        output_columns: str,
        categories_sql: Optional[str] = None
    ) -> str:
        """Format crosstab function with explicit column definition.

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
        branch_delim: str = '~'
    ) -> str:
        """Format connectby function with full options.

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
            "SELECT * FROM connectby('categories', 'id', 'parent_id', '1', 5, '~') AS t(keyid text, parent_keyid text, level int, branch text, sort_column text)"
        """
        return f"SELECT * FROM connectby('{table_name}', '{key_column}', '{parent_column}', '{start_value}', {max_depth}, '{branch_delim}') AS t(keyid text, parent_keyid text, level int, branch text)"
