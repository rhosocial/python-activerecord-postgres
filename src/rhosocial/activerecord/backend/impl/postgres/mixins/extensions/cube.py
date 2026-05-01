# src/rhosocial/activerecord/backend/impl/postgres/mixins/extensions/cube.py
"""
PostgreSQL cube multidimensional cube functionality mixin.

This module provides functionality to check cube extension features.

For SQL expression generation, use the function factories in
``functions/cube.py`` instead of the removed format_* methods.
"""


class PostgresCubeMixin:
    """cube multidimensional cube functionality implementation."""

    def supports_cube_type(self) -> bool:
        """Check if cube type is supported."""
        return self.check_extension_feature("cube", "type")
