# src/rhosocial/activerecord/backend/impl/postgres/adapters/geometric.py
"""
PostgreSQL Geometric Types Adapter.

This module provides type adapters for PostgreSQL geometric types.

PostgreSQL Documentation: https://www.postgresql.org/docs/current/datatype-geometric.html

Geometric Types:
- point: (x, y) coordinate
- line: infinite line {A, B, C} where Ax + By + C = 0
- lseg: line segment [(x1,y1),(x2,y2)]
- box: rectangular box (x1,y1),(x2,y2)
- path: sequence of points (open or closed)
- polygon: closed path (automatically closed)
- circle: circle <(x,y),r>
"""
from typing import Any, Dict, List, Optional, Set, Type

from ..types.geometric import (
    Point,
    Line,
    LineSegment,
    Box,
    Path,
    Polygon,
    Circle,
)


class PostgresGeometryAdapter:
    """PostgreSQL geometric type adapter.

    This adapter converts between Python geometric objects and
    PostgreSQL geometric type values.

    Supported types:
    - point: 2D point (Point)
    - line: infinite line (Line)
    - lseg: line segment (LineSegment)
    - box: rectangular box (Box)
    - path: sequence of points (Path)
    - polygon: closed path (Polygon)
    - circle: circle (Circle)
    """

    GEOMETRY_TYPES: Dict[str, Type] = {
        'point': Point,
        'line': Line,
        'lseg': LineSegment,
        'box': Box,
        'path': Path,
        'polygon': Polygon,
        'circle': Circle,
    }

    @property
    def supported_types(self) -> Dict[Type, Set[Type]]:
        """Return supported type mappings."""
        return {
            Point: {str},
            Line: {str},
            LineSegment: {str},
            Box: {str},
            Path: {str},
            Polygon: {str},
            Circle: {str},
        }

    def to_database(
        self,
        value: Any,
        target_type: Type,
        options: Optional[Dict[str, Any]] = None
    ) -> Optional[str]:
        """Convert Python geometric object to PostgreSQL string.

        Args:
            value: Geometric object (Point, Line, etc.)
            target_type: Target type (not used, kept for interface)
            options: Optional conversion options

        Returns:
            PostgreSQL geometric literal string, or None if value is None
        """
        if value is None:
            return None

        if isinstance(value, (Point, Line, LineSegment, Box, Path, Polygon, Circle)):
            return value.to_postgres_string()

        if isinstance(value, str):
            return value

        if isinstance(value, (tuple, list)):
            # Handle tuple format for Point: (x, y)
            if len(value) == 2 and all(isinstance(v, (int, float)) for v in value):
                return Point(float(value[0]), float(value[1])).to_postgres_string()

        raise TypeError(f"Cannot convert {type(value).__name__} to geometric type")

    def from_database(
        self,
        value: Any,
        target_type: Type,
        options: Optional[Dict[str, Any]] = None
    ) -> Any:
        """Convert PostgreSQL geometric value to Python object.

        Args:
            value: Geometric value from database
            target_type: Target Python type
            options: Optional conversion options

        Returns:
            Geometric object, or None if value is None
        """
        if value is None:
            return None

        # If already a geometric object, return as-is
        if isinstance(value, target_type):
            return value

        if isinstance(value, str):
            if target_type == Point:
                return Point.from_postgres_string(value)
            elif target_type == Line:
                return Line.from_postgres_string(value)
            elif target_type == LineSegment:
                return LineSegment.from_postgres_string(value)
            elif target_type == Box:
                return Box.from_postgres_string(value)
            elif target_type == Path:
                return Path.from_postgres_string(value)
            elif target_type == Polygon:
                return Polygon.from_postgres_string(value)
            elif target_type == Circle:
                return Circle.from_postgres_string(value)

        raise TypeError(f"Cannot convert {type(value).__name__} to {target_type.__name__}")

    def to_database_batch(
        self,
        values: List[Any],
        target_type: Type,
        options: Optional[Dict[str, Any]] = None
    ) -> List[Any]:
        """Batch convert values to database format."""
        return [self.to_database(v, target_type, options) for v in values]

    def from_database_batch(
        self,
        values: List[Any],
        target_type: Type,
        options: Optional[Dict[str, Any]] = None
    ) -> List[Any]:
        """Batch convert values from database format."""
        return [self.from_database(v, target_type, options) for v in values]


__all__ = ['PostgresGeometryAdapter']
