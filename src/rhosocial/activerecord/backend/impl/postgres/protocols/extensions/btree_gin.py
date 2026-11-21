# src/rhosocial/activerecord/backend/impl/postgres/protocols/extensions/btree_gin.py
"""btree_gin extension protocol — capability probe only.

Index DDL is an expression: ``functions.btree_gin_index`` returns a
``PostgresCreateIndexExpression``.
"""

from typing import Protocol, runtime_checkable


@runtime_checkable
class PostgresBtreeGinSupport(Protocol):
    """btree_gin composite index extension protocol."""

    def supports_btree_gin(self) -> bool: ...
