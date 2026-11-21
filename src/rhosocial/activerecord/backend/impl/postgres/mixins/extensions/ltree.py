# src/rhosocial/activerecord/backend/impl/postgres/mixins/extensions/ltree.py
"""ltree capability probes.

Index DDL comes from ``functions.ltree.ltree_index``.
"""


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
