# src/rhosocial/activerecord/backend/impl/postgres/protocols/extensions/ltree.py
"""PostgreSQL ltree extension protocol.

This module defines the protocol for ltree (label tree) data type support,
which provides hierarchical label path operations.
"""

from typing import Protocol, runtime_checkable


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
