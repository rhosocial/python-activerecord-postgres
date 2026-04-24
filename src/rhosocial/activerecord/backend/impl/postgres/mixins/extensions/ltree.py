# src/rhosocial/activerecord/backend/impl/postgres/mixins/extensions/ltree.py
"""
ltree label tree functionality implementation.

This module provides the PostgresLtreeMixin class that adds support for
ltree extension features including ltree type, operators, and indexes.
"""

from typing import TYPE_CHECKING, Optional, Tuple

if TYPE_CHECKING:
    pass  # No type imports needed for this mixin


class PostgresLtreeMixin:
    """ltree label tree implementation."""

    def supports_ltree_type(self) -> bool:
        """Check if ltree supports ltree type."""
        return self.check_extension_feature("ltree", "type")

    def supports_ltree_operators(self) -> bool:
        """Check if ltree supports operators."""
        return self.check_extension_feature("ltree", "operators")

    def supports_ltree_index(self) -> bool:
        """Check if ltree supports index."""
        return self.check_extension_feature("ltree", "index")

    def format_ltree_literal(self, path: str) -> str:
        """Format an ltree literal value.

        Args:
            path: Label path (e.g., 'Top.Science.Astronomy')

        Returns:
            SQL ltree literal string

        Example:
            >>> format_ltree_literal('Top.Science.Astronomy')
            "'Top.Science.Astronomy'"
        """
        return f"'{path}'"

    def format_lquery_literal(self, pattern: str) -> str:
        """Format an lquery pattern literal.

        Args:
            pattern: lquery pattern (e.g., '*.Astronomy.*' or 'Top.*{1,2}')

        Returns:
            SQL lquery literal string

        Example:
            >>> format_lquery_literal('*.Astronomy.*')
            "'*.Astronomy.*'::lquery"
        """
        return f"'{pattern}'::lquery"

    def format_ltree_operator(self, column: str, operator: str, value: str, value_type: str = "ltree") -> str:
        """Format an ltree operator expression.

        Args:
            column: The ltree column name
            operator: The operator symbol
            value: The value to compare with
            value_type: Type of value - 'ltree', 'lquery', or 'ltxtquery'

        Returns:
            SQL operator expression

        Example:
            >>> format_ltree_operator('path', '@>', 'Top.Science')
            "path @> 'Top.Science'"
            >>> format_ltree_operator('path', '~', '*.Astronomy.*', 'lquery')
            "path ~ '*.Astronomy.*'::lquery"
        """
        if value_type == "lquery":
            return f"{column} {operator} '{value}'::lquery"
        elif value_type == "ltxtquery":
            return f"{column} {operator} '{value}'::ltxtquery"
        else:
            return f"{column} {operator} '{value}'"

    def format_ltree_is_ancestor(self, column: str, path: str) -> str:
        """Format ltree ancestor check (column is ancestor of path).

        Args:
            column: The ltree column name
            path: The descendant path

        Returns:
            SQL ancestor expression

        Example:
            >>> format_ltree_is_ancestor('path', 'Top.Science.Astronomy')
            "path @> 'Top.Science.Astronomy'"
        """
        return f"{column} @> '{path}'"

    def format_ltree_is_descendant(self, column: str, path: str) -> str:
        """Format ltree descendant check (column is descendant of path).

        Args:
            column: The ltree column name
            path: The ancestor path

        Returns:
            SQL descendant expression

        Example:
            >>> format_ltree_is_descendant('path', 'Top.Science')
            "path <@ 'Top.Science'"
        """
        return f"{column} <@ '{path}'"

    def format_ltree_matches(self, column: str, pattern: str) -> str:
        """Format ltree lquery match.

        Args:
            column: The ltree column name
            pattern: lquery pattern

        Returns:
            SQL match expression

        Example:
            >>> format_ltree_matches('path', '*.Astronomy.*')
            "path ~ '*.Astronomy.*'::lquery"
        """
        return f"{column} ~ '{pattern}'::lquery"

    def format_ltree_index_statement(
        self, index_name: str, table_name: str, column_name: str, index_type: str = "gist", schema: Optional[str] = None
    ) -> Tuple[str, tuple]:
        """Format CREATE INDEX statement for ltree column.

        Args:
            index_name: Name of the index
            table_name: Table name
            column_name: ltree column name
            index_type: Index type - 'gist' (default) or 'btree'
            schema: Optional schema name

        Returns:
            Tuple of (SQL statement, parameters)

        Example:
            >>> format_ltree_index_statement('idx_path', 'categories', 'path')
            ("CREATE INDEX idx_path ON categories USING gist (path)", ())
        """
        full_table = f"{schema}.{table_name}" if schema else table_name
        sql = f"CREATE INDEX {index_name} ON {full_table} USING {index_type} ({column_name})"
        return (sql, ())

    def format_ltree_subpath(self, column: str, start: int, length: Optional[int] = None) -> str:
        """Format subpath extraction.

        Args:
            column: The ltree column name
            start: Starting position (0-indexed)
            length: Optional number of labels to extract

        Returns:
            SQL subpath expression

        Example:
            >>> format_ltree_subpath('path', 0, 2)
            "subpath(path, 0, 2)"
        """
        if length is not None:
            return f"subpath({column}, {start}, {length})"
        return f"subpath({column}, {start})"

    def format_ltree_nlevel(self, column: str) -> str:
        """Format nlevel function (count of labels).

        Args:
            column: The ltree column name

        Returns:
            SQL nlevel expression

        Example:
            >>> format_ltree_nlevel('path')
            "nlevel(path)"
        """
        return f"nlevel({column})"

    def format_ltree_concat(self, left: str, right: str) -> str:
        """Format ltree path concatenation.

        Args:
            left: Left ltree expression
            right: Right ltree expression

        Returns:
            SQL concatenation expression

        Example:
            >>> format_ltree_concat("'Top.Science'", "'Astronomy'")
            "'Top.Science' || 'Astronomy'"
        """
        return f"{left} || {right}"

    def format_ltree_lca(self, *paths: str) -> str:
        """Format lca (lowest common ancestor) function.

        Args:
            *paths: ltree path expressions

        Returns:
            SQL lca function call

        Example:
            >>> format_ltree_lca("'Top.Science.Astronomy'", "'Top.Science.Physics'")
            "lca('Top.Science.Astronomy', 'Top.Science.Physics')"
        """
        paths_str = ", ".join(paths)
        return f"lca({paths_str})"
