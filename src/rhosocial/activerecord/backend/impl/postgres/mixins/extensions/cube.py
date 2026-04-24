# src/rhosocial/activerecord/backend/impl/postgres/mixins/extensions/cube.py
"""
cube multidimensional cube functionality implementation.

This module provides the PostgresCubeMixin class that adds support for
cube extension features.
"""

from typing import TYPE_CHECKING, List

if TYPE_CHECKING:
    pass


class PostgresCubeMixin:
    """cube multidimensional cube functionality implementation."""

    def supports_cube_type(self) -> bool:
        """Check if cube type is supported."""
        return self.check_extension_feature("cube", "type")

    def format_cube(self, coordinates: List[float]) -> str:
        """Format a cube from coordinates.

        Args:
            coordinates: List of coordinate values

        Returns:
            SQL cube literal
        """
        coords = ", ".join(str(c) for c in coordinates)
        return f"cube(ARRAY[{coords}])"

    def format_cube_dimension(self, dim: int, coord: float) -> str:
        """Format a point in specified dimension.

        Args:
            dim: Number of dimensions
            coord: Coordinate value

        Returns:
            SQL cube from dimension
        """
        return f"cube({dim}, {coord})"

    def format_cube_union(self, cube1: str, cube2: str) -> str:
        """Format union of two cubes.

        Args:
            cube1: First cube
            cube2: Second cube

        Returns:
            SQL cube union
        """
        return f"{cube1} union {cube2}"

    def format_cube_inter(self, cube1: str, cube2: str) -> str:
        """Format intersection of two cubes.

        Args:
            cube1: First cube
            cube2: Second cube

        Returns:
            SQL cube intersection
        """
        return f"{cube1} inter {cube2}"

    def format_cube_contains(self, cube_expr: str, target: str) -> str:
        """Format cube containment check.

        Args:
            cube_expr: The cube expression or column name
            target: The target cube to check containment

        Returns:
            SQL containment expression

        Example:
            >>> format_cube_contains('cube_col', "'(1,1)'")
            "cube_col @> '(1,1)'"
        """
        return f"{cube_expr} @> {target}"

    def format_cube_distance(self, cube_expr: str, target: str) -> str:
        """Format cube distance operator.

        Args:
            cube_expr: The cube expression or column name
            target: The target cube to measure distance to

        Returns:
            SQL distance expression

        Example:
            >>> format_cube_distance('cube_col', "'(1,1)'")
            "cube_col <-> '(1,1)'"
        """
        return f"{cube_expr} <-> {target}"

    def format_cube_size(self, cube_expr: str) -> str:
        """Format cube_size function.

        Args:
            cube_expr: The cube expression or column name

        Returns:
            SQL cube_size function call

        Example:
            >>> format_cube_size('cube_col')
            "cube_size(cube_col)"
        """
        return f"cube_size({cube_expr})"