# src/rhosocial/activerecord/backend/impl/postgres/protocols/extensions/earthdistance.py
"""PostgreSQL earthdistance extension protocol.

This module defines the protocol for earthdistance extension support,
which provides earth distance calculations for geographic points.
"""

from typing import Protocol, runtime_checkable


@runtime_checkable
class PostgresEarthdistanceSupport(Protocol):
    """earthdistance earth distance calculation protocol.

    Feature Source: Extension support (requires earthdistance extension)

    earthdistance provides earth distance calculations:
    - earth data type for surface points
    - Great-circle distance calculation
    - Distance operators (<@> for miles)

    Extension Information:
    - Extension name: earthdistance
    - Install command: CREATE EXTENSION earthdistance;
    - Minimum version: 1.0
    - Dependencies: cube extension
    - Documentation: https://www.postgresql.org/docs/current/earthdistance.html

    Detection methods:
    - Automatic detection: introspect_and_adapt() queries pg_extension
    - Manual detection: SELECT * FROM pg_extension WHERE extname = 'earthdistance';
    - Programmatic detection: dialect.is_extension_installed('earthdistance')
    """

    def supports_earthdistance_type(self) -> bool:
        """Whether earth data type is supported.

        Requires earthdistance extension.
        Represents points on Earth's surface.
        """
        ...

    def supports_earthdistance_operators(self) -> bool:
        """Whether earthdistance operators are supported.

        Requires earthdistance extension.
        Supports <@> operator for distance in miles.
        """
        ...

    def format_earth_literal(self, point: tuple) -> str:
        """Format an earth point literal."""
        ...

    def format_earthdistance_operator(self, column: str, point: tuple, operator: str = "<->") -> str:
        """Format an earthdistance operator expression."""
        ...

    def format_ll_to_earth_function(self, latitude: float, longitude: float) -> str:
        """Format ll_to_earth function call."""
        ...

    def format_earth_distance(self, point1: str, point2: str) -> str:
        """Format earth_distance function call."""
        ...

    def format_earth_box(self, center: str, radius: float) -> str:
        """Format earth_box function call."""
        ...

    def format_point_inside_circle(self, point: str, center: tuple, radius: float) -> str:
        """Format point inside circle check."""
        ...

    def format_distance_within(self, column: str, center: tuple, radius: float) -> str:
        """Format distance within expression for WHERE clause."""
        ...

    def format_order_by_distance(self, column: str, center: tuple, ascending: bool = True) -> str:
        """Format ORDER BY distance expression."""
        ...
