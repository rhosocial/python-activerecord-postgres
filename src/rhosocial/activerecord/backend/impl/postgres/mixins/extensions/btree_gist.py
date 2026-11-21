# src/rhosocial/activerecord/backend/impl/postgres/mixins/extensions/btree_gist.py
"""btree_gist capability probe.

Index DDL is produced by ``functions.btree_gist_index`` as an expression node.
"""


class PostgresBtreeGistMixin:
    """btree_gist composite index functionality implementation."""

    def supports_btree_gist(self) -> bool:
        """Check if btree_gist extension is available."""
        return self.is_extension_installed("btree_gist")
