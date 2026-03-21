# Copyright (C) 2025 rhosocial<mailto:rhosocial@rhosocial.com>
# SPDX-License-Identifier: MIT
# Author: rhosocial<mailto:rhosocial@rhosocial.com>
# Date: 2025-03-21
# Version: 1.0.0
# Description: PostgreSQL earthdistance extension mixin implementation.
# License: MIT
"""PostgreSQL earthdistance earth distance functionality mixin.

This module provides functionality to check earthdistance extension features,
including earth type support and operators.
"""


class PostgresEarthdistanceMixin:
    """earthdistance earth distance implementation."""

    def supports_earthdistance_type(self) -> bool:
        """Check if earthdistance supports earth type."""
        return self.check_extension_feature('earthdistance', 'type')

    def supports_earthdistance_operators(self) -> bool:
        """Check if earthdistance supports operators."""
        return self.check_extension_feature('earthdistance', 'operators')

    def format_earth_literal(self, point: tuple) -> str:
        """Format an earth point literal.

        Args:
            point: Tuple of (latitude, longitude) in degrees

        Returns:
            SQL earth literal string

        Example:
            >>> format_earth_literal((40.7128, -74.0060))
            "ll_to_earth(40.7128, -74.006)"
        """
        lat, lon = point
        return f"ll_to_earth({lat}, {lon})"

    def format_earthdistance_operator(
        self,
        column: str,
        point: tuple,
        operator: str = '<->'
    ) -> str:
        """Format an earthdistance operator expression.

        Args:
            column: The earth/point column name
            point: Tuple of (latitude, longitude)
            operator: Distance operator

        Returns:
            SQL operator expression

        Example:
            >>> format_earthdistance_operator('location', (40.7128, -74.0060))
            "location <-> ll_to_earth(40.7128, -74.006)"
        """
        earth_point = self.format_earth_literal(point)
        return f"{column} {operator} {earth_point}"

    def format_ll_to_earth_function(self, latitude: float, longitude: float) -> str:
        """Format ll_to_earth function call.

        Converts latitude/longitude to earth point.

        Args:
            latitude: Latitude in degrees
            longitude: Longitude in degrees

        Returns:
            SQL function call

        Example:
            >>> format_ll_to_earth_function(40.7128, -74.0060)
            "ll_to_earth(40.7128, -74.006)"
        """
        return f"ll_to_earth({latitude}, {longitude})"

    def format_earth_distance(self, point1: str, point2: str) -> str:
        """Format earth_distance function call.

        Calculates distance between two earth points in meters.

        Args:
            point1: First earth point expression
            point2: Second earth point expression

        Returns:
            SQL function call

        Example:
            >>> format_earth_distance('point1', 'point2')
            "earth_distance(point1, point2)"
        """
        return f"earth_distance({point1}, {point2})"

    def format_earth_box(self, center: str, radius: float) -> str:
        """Format earth_box function call.

        Creates a box around a point with given radius.

        Args:
            center: Center point expression
            radius: Radius in meters

        Returns:
            SQL function call

        Example:
            >>> format_earth_box('location', 10000)
            "earth_box(location, 10000)"
        """
        return f"earth_box({center}, {radius})"

    def format_point_inside_circle(
        self,
        point: str,
        center: tuple,
        radius: float
    ) -> str:
        """Format point inside circle check.

        Args:
            point: Point column/expression
            center: Center point as (lat, lon) tuple
            radius: Radius in meters

        Returns:
            SQL expression

        Example:
            >>> format_point_inside_circle('location', (40.7128, -74.0060), 5000)
            "point @ earth_box(ll_to_earth(40.7128, -74.006), 5000)"
        """
        center_earth = self.format_ll_to_earth_function(center[0], center[1])
        return f"{point} <@ earth_box({center_earth}, {radius})"

    def format_distance_within(
        self,
        column: str,
        center: tuple,
        radius: float
    ) -> str:
        """Format distance within expression for WHERE clause.

        Args:
            column: The earth/point column
            center: Center point as (lat, lon) tuple
            radius: Maximum distance in meters

        Returns:
            SQL expression for WHERE clause

        Example:
            >>> format_distance_within('location', (40.7128, -74.0060), 10000)
            "(location <@ earth_box(ll_to_earth(40.7128, -74.006), 10000))"
        """
        center_earth = self.format_ll_to_earth_function(center[0], center[1])
        return f"({column} <@ earth_box({center_earth}, {radius}))"

    def format_order_by_distance(
        self,
        column: str,
        center: tuple,
        ascending: bool = True
    ) -> str:
        """Format ORDER BY distance expression.

        Args:
            column: The earth/point column
            center: Center point as (lat, lon) tuple
            ascending: Sort ascending (nearest first) or descending

        Returns:
            SQL ORDER BY expression

        Example:
            >>> format_order_by_distance('location', (40.7128, -74.0060))
            "location <-> ll_to_earth(40.7128, -74.006) ASC"
        """
        center_earth = self.format_ll_to_earth_function(center[0], center[1])
        direction = "ASC" if ascending else "DESC"
        return f"{column} <-> {center_earth} {direction}"
