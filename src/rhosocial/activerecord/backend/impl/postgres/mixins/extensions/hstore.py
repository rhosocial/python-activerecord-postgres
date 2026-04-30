# src/rhosocial/activerecord/backend/impl/postgres/mixins/extensions/hstore.py
"""PostgreSQL hstore key-value storage functionality mixin.

This module provides functionality to check hstore extension features,
including type support and operators.

For SQL expression generation, use the function factories in
``functions/hstore.py`` instead of the removed format_* methods.
"""



class PostgresHstoreMixin:
    """hstore key-value storage functionality implementation."""

    def supports_hstore_type(self) -> bool:
        """Check if hstore supports hstore type."""
        return self.check_extension_feature("hstore", "type")

    def supports_hstore_operators(self) -> bool:
        """Check if hstore supports operators."""
        return self.check_extension_feature("hstore", "operators")
