# src/rhosocial/activerecord/backend/impl/postgres/types/geometric.py
"""
PostgreSQL geometric types representation.

This module provides Python classes for PostgreSQL geometric types:
- Point: 2D coordinate (x, y)
- Line: Infinite line {A, B, C} where Ax + By + C = 0
- LineSegment: Line segment [(x1,y1),(x2,y2)]
- Box: Rectangular box (x1,y1),(x2,y2)
- Path: Sequence of points (open or closed)
- Polygon: Closed path (automatically closed)
- Circle: Circle <(x,y),r>

All geometric types are available since early PostgreSQL versions.

For type adapter (conversion between Python and database),
see adapters.geometric.PostgresGeometryAdapter.
"""

from dataclasses import dataclass
from typing import List


@dataclass
class Point:
    """PostgreSQL POINT type representation.

    A point represents a 2D coordinate (x, y).

    Attributes:
        x: X coordinate
        y: Y coordinate

    Examples:
        Point(1.5, 2.5) -> (1.5,2.5) in PostgreSQL
    """

    x: float
    y: float

    def to_postgres_string(self) -> str:
        """Convert to PostgreSQL point literal."""
        return f"({self.x},{self.y})"

    @classmethod
    def from_postgres_string(cls, value: str) -> "Point":
        """Parse PostgreSQL point string."""
        value = value.strip()
        # Format: (x,y) or x,y
        if value.startswith("(") and value.endswith(")"):
            value = value[1:-1]
        parts = value.split(",")
        if len(parts) != 2:
            raise ValueError(f"Invalid point format: {value}")
        return cls(float(parts[0].strip()), float(parts[1].strip()))

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Point):
            return NotImplemented
        return self.x == other.x and self.y == other.y

    def __hash__(self) -> int:
        return hash((self.x, self.y))


@dataclass
class Line:
    """PostgreSQL LINE type representation.

    An infinite line defined by the equation Ax + By + C = 0.

    Attributes:
        A: Coefficient A
        B: Coefficient B
        C: Coefficient C

    Examples:
        Line(1, -1, 0) represents line y = x (x - y = 0)
    """

    A: float
    B: float
    C: float

    def to_postgres_string(self) -> str:
        """Convert to PostgreSQL line literal."""
        return f"{{{self.A},{self.B},{self.C}}}"

    @classmethod
    def from_postgres_string(cls, value: str) -> "Line":
        """Parse PostgreSQL line string."""
        value = value.strip()
        # Format: {A,B,C}
        if value.startswith("{") and value.endswith("}"):
            value = value[1:-1]
        parts = value.split(",")
        if len(parts) != 3:
            raise ValueError(f"Invalid line format: {value}")
        return cls(float(parts[0].strip()), float(parts[1].strip()), float(parts[2].strip()))

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Line):
            return NotImplemented
        return self.A == other.A and self.B == other.B and self.C == other.C


@dataclass
class LineSegment:
    """PostgreSQL LSEG type representation.

    A line segment defined by two endpoints.

    Attributes:
        start: Starting point
        end: Ending point

    Examples:
        LineSegment(Point(0, 0), Point(1, 1))
    """

    start: Point
    end: Point

    def to_postgres_string(self) -> str:
        """Convert to PostgreSQL lseg literal."""
        return f"[{self.start.to_postgres_string()},{self.end.to_postgres_string()}]"

    @classmethod
    def from_postgres_string(cls, value: str) -> "LineSegment":
        """Parse PostgreSQL lseg string."""
        value = value.strip()
        # Format: [(x1,y1),(x2,y2)] or ((x1,y1),(x2,y2))
        if value.startswith("[") and value.endswith("]"):
            value = value[1:-1]
        elif value.startswith("(") and value.endswith(")"):
            # Handle ((x1,y1),(x2,y2))
            value = value[1:-1]

        # Find the separator between two points
        depth = 0
        sep_idx = -1
        for i, c in enumerate(value):
            if c == "(":
                depth += 1
            elif c == ")":
                depth -= 1
            elif c == "," and depth == 0:
                sep_idx = i
                break

        if sep_idx == -1:
            raise ValueError(f"Invalid lseg format: {value}")

        start_str = value[:sep_idx].strip()
        end_str = value[sep_idx + 1 :].strip()

        return cls(Point.from_postgres_string(start_str), Point.from_postgres_string(end_str))


@dataclass
class Box:
    """PostgreSQL BOX type representation.

    A rectangular box defined by two opposite corners.
    PostgreSQL stores the upper-right corner first, then lower-left.

    Attributes:
        upper_right: Upper right corner point
        lower_left: Lower left corner point

    Examples:
        Box(Point(10, 10), Point(0, 0)) represents a box from (0,0) to (10,10)
    """

    upper_right: Point
    lower_left: Point

    def to_postgres_string(self) -> str:
        """Convert to PostgreSQL box literal."""
        return f"{self.upper_right.to_postgres_string()},{self.lower_left.to_postgres_string()}"

    @classmethod
    def from_postgres_string(cls, value: str) -> "Box":
        """Parse PostgreSQL box string."""
        value = value.strip()
        # Format: (x1,y1),(x2,y2)

        # Find the separator between two points
        depth = 0
        sep_idx = -1
        for i, c in enumerate(value):
            if c == "(":
                depth += 1
            elif c == ")":
                depth -= 1
            elif c == "," and depth == 0:
                sep_idx = i
                break

        if sep_idx == -1:
            raise ValueError(f"Invalid box format: {value}")

        first_str = value[:sep_idx].strip()
        second_str = value[sep_idx + 1 :].strip()

        return cls(Point.from_postgres_string(first_str), Point.from_postgres_string(second_str))


@dataclass
class Path:
    """PostgreSQL PATH type representation.

    A path is a sequence of points, which can be open or closed.

    Attributes:
        points: List of points forming the path
        is_closed: True if the path is closed (polygon), False if open

    Examples:
        Path([Point(0, 0), Point(1, 0), Point(1, 1)], is_closed=False)
    """

    points: List[Point]
    is_closed: bool = False

    def to_postgres_string(self) -> str:
        """Convert to PostgreSQL path literal."""
        if not self.points:
            return "()" if self.is_closed else "[]"

        points_str = ",".join(p.to_postgres_string() for p in self.points)
        if self.is_closed:
            return f"({points_str})"
        else:
            return f"[{points_str}]"

    @classmethod
    def from_postgres_string(cls, value: str) -> "Path":
        """Parse PostgreSQL path string."""
        value = value.strip()
        if not value:
            return cls(points=[], is_closed=False)

        # Determine if open or closed
        is_closed = value.startswith("(")
        if value.startswith("(") and value.endswith(")"):
            value = value[1:-1]
        elif value.startswith("[") and value.endswith("]"):
            value = value[1:-1]
        else:
            raise ValueError(f"Invalid path format: {value}")

        if not value:
            return cls(points=[], is_closed=is_closed)

        # Parse points
        points = []
        current = ""
        depth = 0

        for c in value:
            if c == "(":
                depth += 1
                current += c
            elif c == ")":
                depth -= 1
                current += c
            elif c == "," and depth == 0:
                if current.strip():
                    points.append(Point.from_postgres_string(current.strip()))
                current = ""
            else:
                current += c

        if current.strip():
            points.append(Point.from_postgres_string(current.strip()))

        return cls(points=points, is_closed=is_closed)


@dataclass
class Polygon:
    """PostgreSQL POLYGON type representation.

    A polygon is a closed path. PostgreSQL automatically closes polygons.

    Attributes:
        points: List of points forming the polygon

    Examples:
        Polygon([Point(0, 0), Point(1, 0), Point(1, 1), Point(0, 1)])
    """

    points: List[Point]

    def to_postgres_string(self) -> str:
        """Convert to PostgreSQL polygon literal."""
        if not self.points:
            return "()"
        points_str = ",".join(p.to_postgres_string() for p in self.points)
        return f"({points_str})"

    @classmethod
    def from_postgres_string(cls, value: str) -> "Polygon":
        """Parse PostgreSQL polygon string."""
        value = value.strip()
        # Format: ((x1,y1),(x2,y2),...)
        if value.startswith("(") and value.endswith(")"):
            value = value[1:-1]

        if not value:
            return cls(points=[])

        # Parse points
        points = []
        current = ""
        depth = 0

        for c in value:
            if c == "(":
                depth += 1
                current += c
            elif c == ")":
                depth -= 1
                current += c
            elif c == "," and depth == 0:
                if current.strip():
                    points.append(Point.from_postgres_string(current.strip()))
                current = ""
            else:
                current += c

        if current.strip():
            points.append(Point.from_postgres_string(current.strip()))

        return cls(points=points)


@dataclass
class Circle:
    """PostgreSQL CIRCLE type representation.

    A circle defined by a center point and radius.

    Attributes:
        center: Center point of the circle
        radius: Radius of the circle

    Examples:
        Circle(Point(0, 0), 5.0) represents a circle centered at origin with radius 5
    """

    center: Point
    radius: float

    def to_postgres_string(self) -> str:
        """Convert to PostgreSQL circle literal."""
        return f"<({self.center.x},{self.center.y}),{self.radius}>"

    @classmethod
    def from_postgres_string(cls, value: str) -> "Circle":
        """Parse PostgreSQL circle string."""
        value = value.strip()
        # Format: <(x,y),r> or ((x,y),r)

        # Remove outer brackets
        if value.startswith("<") and value.endswith(">"):
            value = value[1:-1]
        elif value.startswith("(") and value.endswith(")"):
            value = value[1:-1]

        # Find the separator between center and radius
        depth = 0
        sep_idx = -1
        for i, c in enumerate(value):
            if c == "(":
                depth += 1
            elif c == ")":
                depth -= 1
            elif c == "," and depth == 0:
                sep_idx = i
                break

        if sep_idx == -1:
            raise ValueError(f"Invalid circle format: {value}")

        center_str = value[:sep_idx].strip()
        radius_str = value[sep_idx + 1 :].strip()

        return cls(Point.from_postgres_string(center_str), float(radius_str))


__all__ = ["Point", "Line", "LineSegment", "Box", "Path", "Polygon", "Circle"]
