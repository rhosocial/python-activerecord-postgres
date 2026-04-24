# src/rhosocial/activerecord/backend/impl/postgres/protocols/extensions/ltree.py
"""PostgreSQL ltree extension protocol.

This module defines the protocol for ltree (label tree) data type support,
which provides hierarchical label path operations.
"""

from typing import Optional, Protocol, Tuple, runtime_checkable


@runtime_checkable
class PostgresLtreeSupport(Protocol):
    """ltree label tree protocol.

    Feature Source: Extension support (requires ltree extension)

    ltree provides label tree data type:
    - ltree data type for label paths
    - lquery for label path patterns
    - ltxtquery for full-text queries
    - Index support (GiST, B-tree)

    Extension Information:
    - Extension name: ltree
    - Install command: CREATE EXTENSION ltree;
    - Minimum version: 1.0
    - Documentation: https://www.postgresql.org/docs/current/ltree.html

    Detection methods:
    - Automatic detection: introspect_and_adapt() queries pg_extension
    - Manual detection: SELECT * FROM pg_extension WHERE extname = 'ltree';
    - Programmatic detection: dialect.is_extension_installed('ltree')
    """

    def supports_ltree_type(self) -> bool:
        """Whether ltree data type is supported.

        Requires ltree extension.
        Stores label paths like 'Top.Science.Astronomy'.
        """
        ...

    def supports_ltree_operators(self) -> bool:
        """Whether ltree operators are supported.

        Requires ltree extension.
        Supports operators: <@, @>, ~, ? and more.
        """
        ...

    def supports_ltree_index(self) -> bool:
        """Whether ltree indexes are supported.

        Requires ltree extension.
        Supports GiST and B-tree indexes on ltree.
        """
        ...

    def format_ltree_literal(self, path: str) -> str:
        """Format an ltree literal value."""
        ...

    def format_lquery_literal(self, pattern: str) -> str:
        """Format an lquery pattern literal."""
        ...

    def format_ltree_operator(self, column: str, operator: str, value: str, value_type: str = "ltree") -> str:
        """Format an ltree operator expression."""
        ...

    def format_ltree_is_ancestor(self, column: str, path: str) -> str:
        """Format ltree ancestor check (column is ancestor of path)."""
        ...

    def format_ltree_is_descendant(self, column: str, path: str) -> str:
        """Format ltree descendant check (column is descendant of path)."""
        ...

    def format_ltree_matches(self, column: str, pattern: str) -> str:
        """Format ltree lquery match."""
        ...

    def format_ltree_index_statement(
        self, index_name: str, table_name: str, column_name: str, index_type: str = "gist", schema: Optional[str] = None
    ) -> Tuple[str, tuple]:
        """Format CREATE INDEX statement for ltree column."""
        ...

    def format_ltree_subpath(self, column: str, start: int, length: Optional[int] = None) -> str:
        """Format subpath extraction."""
        ...

    def format_ltree_nlevel(self, column: str) -> str:
        """Format nlevel function (count of labels)."""
        ...

    def format_ltree_concat(self, left: str, right: str) -> str:
        """Format ltree path concatenation."""
        ...

    def format_ltree_lca(self, *paths: str) -> str:
        """Format lca (lowest common ancestor) function."""
        ...
