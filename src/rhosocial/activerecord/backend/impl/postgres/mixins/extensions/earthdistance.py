# src/rhosocial/activerecord/backend/impl/postgres/mixins/extensions/earthdistance.py
"""
PostgreSQL earthdistance earth distance functionality mixin.

This module provides functionality to check earthdistance extension features,
including earth type support and operators.

For SQL expression generation, use the function factories in
``functions/earthdistance.py`` instead of the removed format_* methods.
"""


class PostgresEarthdistanceMixin:
    """earthdistance earth distance implementation."""

    def supports_earthdistance_type(self) -> bool:
        """Check if earthdistance supports earth type."""
        return self.check_extension_feature("earthdistance", "type")

    def supports_earthdistance_operators(self) -> bool:
        """Check if earthdistance supports operators."""
        return self.check_extension_feature("earthdistance", "operators")
