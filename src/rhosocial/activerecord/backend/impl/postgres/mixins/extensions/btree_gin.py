# src/rhosocial/activerecord/backend/impl/postgres/mixins/extensions/btree_gin.py
"""btree_gin capability probe.

Index DDL is produced by ``functions.btree_gin_index`` as an expression node.
"""


class PostgresBtreeGinMixin:
    """btree_gin composite index functionality implementation."""

    def supports_btree_gin(self) -> bool:
        """Check if btree_gin extension is available."""
        return self.is_extension_installed("btree_gin")
