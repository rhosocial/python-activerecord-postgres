# src/rhosocial/activerecord/backend/impl/postgres/mixins/extensions/pgrouting.py
"""
pgRouting path finding functionality implementation.

This module provides the PostgresPgroutingMixin class that adds support for
pgrouting extension features including Dijkstra and A* shortest path algorithms.
"""

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    pass  # No type imports needed for this mixin


class PostgresPgroutingMixin:
    """pgRouting path finding functionality implementation."""

    def supports_pgrouting(self) -> bool:
        """Check if pgRouting extension is installed."""
        return self.is_extension_installed("pgrouting")

    def supports_pgrouting_dijkstra(self) -> bool:
        """Check if pgRouting Dijkstra algorithm is supported."""
        return self.check_extension_feature("pgrouting", "dijkstra")

    def format_pgr_dijkstra(self, edges_sql: str, start_vid: int, end_vid: int, directed: bool = True) -> str:
        """Format pgr_dijkstra function call.

        Args:
            edges_sql: SQL query that returns edge data (id, source, target, cost, reverse_cost)
            start_vid: Start vertex ID
            end_vid: End vertex ID
            directed: Whether the graph is directed (default: True)

        Returns:
            SQL function call string

        Example:
            >>> format_pgr_dijkstra(
            ...     "SELECT id, source, target, cost, reverse_cost FROM edges",
            ...     1, 5
            ... )
            "SELECT * FROM pgr_dijkstra('SELECT id, source, target, cost, reverse_cost FROM edges', 1, 5, directed := true)"
        """
        dir_flag = "true" if directed else "false"
        return f"SELECT * FROM pgr_dijkstra('{edges_sql}', {start_vid}, {end_vid}, directed := {dir_flag})"

    def format_pgr_astar(self, edges_sql: str, start_vid: int, end_vid: int, directed: bool = True) -> str:
        """Format pgr_aStar function call.

        Args:
            edges_sql: SQL query that returns edge data with x/y coordinates
            start_vid: Start vertex ID
            end_vid: End vertex ID
            directed: Whether the graph is directed (default: True)

        Returns:
            SQL function call string

        Example:
            >>> format_pgr_astar(
            ...     "SELECT id, source, target, cost, reverse_cost, x1, y1, x2, y2 FROM edges",
            ...     1, 5
            ... )
            "SELECT * FROM pgr_aStar('SELECT id, source, target, cost, reverse_cost, x1, y1, x2, y2 FROM edges', 1, 5, directed := true)"
        """
        dir_flag = "true" if directed else "false"
        return f"SELECT * FROM pgr_aStar('{edges_sql}', {start_vid}, {end_vid}, directed := {dir_flag})"
