# src/rhosocial/activerecord/backend/impl/postgres/protocols/extensions/cube.py
"""cube extension protocol definition.

This module defines the protocol for cube multidimensional cube
functionality in PostgreSQL.
"""

from typing import List, Protocol, runtime_checkable


@runtime_checkable
class PostgresCubeSupport(Protocol):
    """cube multidimensional cube extension protocol.

    Feature Source: Extension support (requires cube extension)

    cube provides multidimensional cube data type:
    - Point representation
    - Cube operations

    Extension Information:
    - Extension name: cube
    - Install command: CREATE EXTENSION cube;
    - Minimum version: 1.0
    - Documentation: https://www.postgresql.org/docs/current/cube.html
    """

    def supports_cube_type(self) -> bool:
        """Whether cube type is supported."""
        ...

    def format_cube(self, coordinates: List[float]) -> str:
        """Format a cube from coordinates."""
        ...

    def format_cube_dimension(self, dim: int, coord: float) -> str:
        """Format a point in specified dimension."""
        ...

    def format_cube_union(self, cube1: str, cube2: str) -> str:
        """Format union of two cubes."""
        ...

    def format_cube_inter(self, cube1: str, cube2: str) -> str:
        """Format intersection of two cubes."""
        ...

    def format_cube_contains(self, cube_expr: str, target: str) -> str:
        """Format cube containment check."""
        ...

    def format_cube_distance(self, cube_expr: str, target: str) -> str:
        """Format cube distance operator."""
        ...

    def format_cube_size(self, cube_expr: str) -> str:
        """Format cube_size function."""
        ...