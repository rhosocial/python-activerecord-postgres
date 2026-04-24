# src/rhosocial/activerecord/backend/impl/postgres/protocols/extensions/pgrouting.py
"""pgRouting extension protocol definition.

This module defines the protocol for pgRouting path finding
functionality in PostgreSQL.
"""

from typing import Protocol, runtime_checkable


@runtime_checkable
class PostgresPgroutingSupport(Protocol):
    """pgRouting path finding functionality protocol.

    Feature Source: Extension support (requires pgrouting extension)

    pgRouting provides geospatial routing functionality:
    - Dijkstra shortest path algorithm
    - A* shortest path algorithm
    - Various other routing algorithms (pgr_ksp, pgr_bdDijkstra, etc.)

    Extension Information:
    - Extension name: pgrouting
    - Install command: CREATE EXTENSION pgrouting;
    - Minimum version: 2.0
    - Dependencies: postgis
    - Website: https://pgrouting.org/
    - Documentation: https://docs.pgrouting.org/

    Detection methods:
    - Automatic detection: introspect_and_adapt() queries pg_extension
    - Manual detection: SELECT * FROM pg_extension WHERE extname = 'pgrouting';
    - Programmatic detection: dialect.is_extension_installed('pgrouting')

    Notes:
    - Requires PostGIS extension as a dependency
    - Dijkstra algorithm available since version 2.0
    - A* algorithm available since version 2.0
    - Installation requires superuser privileges
    """

    def supports_pgrouting(self) -> bool:
        """Whether pgRouting extension is installed and available.

        Requires pgrouting extension.
        pgRouting provides geospatial routing and path finding functionality.
        """
        ...

    def supports_pgrouting_dijkstra(self) -> bool:
        """Whether pgRouting Dijkstra algorithm is supported.

        Requires pgrouting extension with Dijkstra feature support.
        Dijkstra algorithm finds the shortest path between two nodes.
        """
        ...

    def format_pgr_dijkstra(self, edges_sql: str, start_vid: int, end_vid: int, directed: bool = True) -> str:
        """Format pgr_dijkstra function call.

        Args:
            edges_sql: SQL query that returns edge data (id, source, target, cost, reverse_cost)
            start_vid: Start vertex ID
            end_vid: End vertex ID
            directed: Whether the graph is directed (default: True)

        Returns:
            SQL function call string
        """
        ...

    def format_pgr_astar(self, edges_sql: str, start_vid: int, end_vid: int, directed: bool = True) -> str:
        """Format pgr_aStar function call.

        Args:
            edges_sql: SQL query that returns edge data with x/y coordinates
            start_vid: Start vertex ID
            end_vid: End vertex ID
            directed: Whether the graph is directed (default: True)

        Returns:
            SQL function call string
        """
        ...
