# src/rhosocial/activerecord/backend/impl/postgres/protocols/extensions/btree_gist.py
"""btree_gist extension protocol — capability probe only.

Index DDL is an expression: ``functions.btree_gist_index`` returns a
``PostgresCreateIndexExpression``.
"""

from typing import Protocol, runtime_checkable


@runtime_checkable
class PostgresBtreeGistSupport(Protocol):
    """btree_gist composite index extension protocol."""

    def supports_btree_gist(self) -> bool: ...
