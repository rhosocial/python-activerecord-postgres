# src/rhosocial/activerecord/backend/impl/postgres/mixins/extensions/pgrouting.py
"""
PostgreSQL pgRouting path finding functionality mixin.

This module provides functionality to check pgRouting extension features,
including Dijkstra and A* shortest path algorithm support.

For SQL expression generation, use the function factories in
``functions/pgrouting.py`` instead of the removed format_* methods.
"""


class PostgresPgroutingMixin:
    """pgRouting path finding functionality implementation."""

    def supports_pgrouting(self) -> bool:
        """Check if pgRouting extension is installed."""
        return self.is_extension_installed("pgrouting")

    def supports_pgrouting_dijkstra(self) -> bool:
        """Check if pgRouting Dijkstra algorithm is supported."""
        return self.check_extension_feature("pgrouting", "dijkstra")
