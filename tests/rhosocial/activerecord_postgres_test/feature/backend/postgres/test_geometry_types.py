# tests/rhosocial/activerecord_postgres_test/feature/backend/postgres/test_geometry_types.py
"""
Unit tests for PostgreSQL geometry types.

Tests for:
- Point, Line, LineSegment, Box, Path, Polygon, Circle data classes
- PostgresGeometryAdapter conversion
"""
import pytest

from rhosocial.activerecord.backend.impl.postgres.types.geometric import (
    Point,
    Line,
    LineSegment,
    Box,
    Path,
    Polygon,
    Circle,
)
from rhosocial.activerecord.backend.impl.postgres.adapters.geometric import PostgresGeometryAdapter


class TestPoint:
    """Tests for Point data class."""

    def test_create_point(self):
        """Test creating a point."""
        p = Point(1.5, 2.5)
        assert p.x == 1.5
        assert p.y == 2.5

    def test_point_to_postgres_string(self):
        """Test point to PostgreSQL string conversion."""
        p = Point(1.5, 2.5)
        assert p.to_postgres_string() == '(1.5,2.5)'

    def test_point_from_postgres_string(self):
        """Test parsing PostgreSQL point string."""
        p = Point.from_postgres_string('(1.5,2.5)')
        assert p.x == 1.5
        assert p.y == 2.5

    def test_point_from_postgres_string_no_parens(self):
        """Test parsing point string without outer parens."""
        p = Point.from_postgres_string('1.5,2.5')
        assert p.x == 1.5
        assert p.y == 2.5

    def test_point_equality(self):
        """Test point equality."""
        p1 = Point(1.0, 2.0)
        p2 = Point(1.0, 2.0)
        p3 = Point(1.0, 3.0)
        assert p1 == p2
        assert p1 != p3

    def test_point_hash(self):
        """Test point hashability."""
        p1 = Point(1.0, 2.0)
        p2 = Point(1.0, 2.0)
        assert hash(p1) == hash(p2)
        assert len({p1, p2}) == 1


class TestLine:
    """Tests for Line data class."""

    def test_create_line(self):
        """Test creating a line."""
        l = Line(1.0, -1.0, 0.0)
        assert l.A == 1.0
        assert l.B == -1.0
        assert l.C == 0.0

    def test_line_to_postgres_string(self):
        """Test line to PostgreSQL string conversion."""
        l = Line(1.0, -1.0, 0.0)
        assert l.to_postgres_string() == '{1.0,-1.0,0.0}'

    def test_line_from_postgres_string(self):
        """Test parsing PostgreSQL line string."""
        l = Line.from_postgres_string('{1.0,-1.0,0.0}')
        assert l.A == 1.0
        assert l.B == -1.0
        assert l.C == 0.0

    def test_line_equality(self):
        """Test line equality."""
        l1 = Line(1.0, 2.0, 3.0)
        l2 = Line(1.0, 2.0, 3.0)
        l3 = Line(1.0, 2.0, 4.0)
        assert l1 == l2
        assert l1 != l3


class TestLineSegment:
    """Tests for LineSegment data class."""

    def test_create_lseg(self):
        """Test creating a line segment."""
        ls = LineSegment(Point(0, 0), Point(1, 1))
        assert ls.start.x == 0
        assert ls.start.y == 0
        assert ls.end.x == 1
        assert ls.end.y == 1

    def test_lseg_to_postgres_string(self):
        """Test lseg to PostgreSQL string conversion."""
        ls = LineSegment(Point(0, 0), Point(1, 1))
        assert ls.to_postgres_string() == '[(0,0),(1,1)]'

    def test_lseg_from_postgres_string(self):
        """Test parsing PostgreSQL lseg string."""
        ls = LineSegment.from_postgres_string('[(0,0),(1,1)]')
        assert ls.start.x == 0
        assert ls.start.y == 0
        assert ls.end.x == 1
        assert ls.end.y == 1

    def test_lseg_from_postgres_string_parens(self):
        """Test parsing lseg string with parentheses."""
        ls = LineSegment.from_postgres_string('((0,0),(1,1))')
        assert ls.start.x == 0
        assert ls.end.x == 1


class TestBox:
    """Tests for Box data class."""

    def test_create_box(self):
        """Test creating a box."""
        b = Box(Point(10, 10), Point(0, 0))
        assert b.upper_right.x == 10
        assert b.upper_right.y == 10
        assert b.lower_left.x == 0
        assert b.lower_left.y == 0

    def test_box_to_postgres_string(self):
        """Test box to PostgreSQL string conversion."""
        b = Box(Point(10, 10), Point(0, 0))
        assert b.to_postgres_string() == '(10,10),(0,0)'

    def test_box_from_postgres_string(self):
        """Test parsing PostgreSQL box string."""
        b = Box.from_postgres_string('(10,10),(0,0)')
        assert b.upper_right.x == 10
        assert b.upper_right.y == 10
        assert b.lower_left.x == 0
        assert b.lower_left.y == 0


class TestPath:
    """Tests for Path data class."""

    def test_create_open_path(self):
        """Test creating an open path."""
        p = Path([Point(0, 0), Point(1, 0), Point(1, 1)], is_closed=False)
        assert len(p.points) == 3
        assert p.is_closed is False

    def test_create_closed_path(self):
        """Test creating a closed path."""
        p = Path([Point(0, 0), Point(1, 0), Point(1, 1), Point(0, 1)], is_closed=True)
        assert len(p.points) == 4
        assert p.is_closed is True

    def test_open_path_to_postgres_string(self):
        """Test open path to PostgreSQL string conversion."""
        p = Path([Point(0, 0), Point(1, 1)], is_closed=False)
        assert p.to_postgres_string() == '[(0,0),(1,1)]'

    def test_closed_path_to_postgres_string(self):
        """Test closed path to PostgreSQL string conversion."""
        p = Path([Point(0, 0), Point(1, 1)], is_closed=True)
        assert p.to_postgres_string() == '((0,0),(1,1))'

    def test_path_from_postgres_string_open(self):
        """Test parsing PostgreSQL open path string."""
        p = Path.from_postgres_string('[(0,0),(1,1)]')
        assert p.is_closed is False
        assert len(p.points) == 2
        assert p.points[0].x == 0

    def test_path_from_postgres_string_closed(self):
        """Test parsing PostgreSQL closed path string."""
        p = Path.from_postgres_string('((0,0),(1,1))')
        assert p.is_closed is True
        assert len(p.points) == 2

    def test_empty_path(self):
        """Test empty path."""
        p = Path(points=[], is_closed=False)
        assert p.to_postgres_string() == '[]'


class TestPolygon:
    """Tests for Polygon data class."""

    def test_create_polygon(self):
        """Test creating a polygon."""
        pg = Polygon([Point(0, 0), Point(1, 0), Point(1, 1), Point(0, 1)])
        assert len(pg.points) == 4

    def test_polygon_to_postgres_string(self):
        """Test polygon to PostgreSQL string conversion."""
        pg = Polygon([Point(0, 0), Point(1, 1)])
        assert pg.to_postgres_string() == '((0,0),(1,1))'

    def test_polygon_from_postgres_string(self):
        """Test parsing PostgreSQL polygon string."""
        pg = Polygon.from_postgres_string('((0,0),(1,1))')
        assert len(pg.points) == 2
        assert pg.points[0].x == 0

    def test_empty_polygon(self):
        """Test empty polygon."""
        pg = Polygon(points=[])
        assert pg.to_postgres_string() == '()'


class TestCircle:
    """Tests for Circle data class."""

    def test_create_circle(self):
        """Test creating a circle."""
        c = Circle(Point(0, 0), 5.0)
        assert c.center.x == 0
        assert c.center.y == 0
        assert c.radius == 5.0

    def test_circle_to_postgres_string(self):
        """Test circle to PostgreSQL string conversion."""
        c = Circle(Point(0, 0), 5.0)
        assert c.to_postgres_string() == '<(0,0),5.0>'

    def test_circle_from_postgres_string(self):
        """Test parsing PostgreSQL circle string."""
        c = Circle.from_postgres_string('<(0,0),5.0>')
        assert c.center.x == 0
        assert c.center.y == 0
        assert c.radius == 5.0

    def test_circle_from_postgres_string_parens(self):
        """Test parsing circle string with parens."""
        c = Circle.from_postgres_string('((0,0),5.0)')
        assert c.center.x == 0
        assert c.radius == 5.0


class TestPostgresGeometryAdapter:
    """Tests for PostgresGeometryAdapter."""

    def test_adapter_supported_types(self):
        """Test supported types property."""
        adapter = PostgresGeometryAdapter()
        supported = adapter.supported_types
        assert Point in supported
        assert Line in supported
        assert Circle in supported

    def test_to_database_point(self):
        """Test converting Point to database."""
        adapter = PostgresGeometryAdapter()
        p = Point(1.5, 2.5)
        result = adapter.to_database(p, str)
        assert result == '(1.5,2.5)'

    def test_to_database_none(self):
        """Test converting None to database."""
        adapter = PostgresGeometryAdapter()
        result = adapter.to_database(None, Point)
        assert result is None

    def test_to_database_string(self):
        """Test converting string to database."""
        adapter = PostgresGeometryAdapter()
        result = adapter.to_database('(1,2)', Point)
        assert result == '(1,2)'

    def test_to_database_tuple(self):
        """Test converting tuple to database (Point)."""
        adapter = PostgresGeometryAdapter()
        result = adapter.to_database((1.5, 2.5), Point)
        assert result == '(1.5,2.5)'

    def test_from_database_point(self):
        """Test converting string from database to Point."""
        adapter = PostgresGeometryAdapter()
        result = adapter.from_database('(1.5,2.5)', Point)
        assert isinstance(result, Point)
        assert result.x == 1.5
        assert result.y == 2.5

    def test_from_database_none(self):
        """Test converting None from database."""
        adapter = PostgresGeometryAdapter()
        result = adapter.from_database(None, Point)
        assert result is None

    def test_from_database_already_object(self):
        """Test converting already typed object."""
        adapter = PostgresGeometryAdapter()
        p = Point(1.0, 2.0)
        result = adapter.from_database(p, Point)
        assert result is p

    def test_from_database_line(self):
        """Test converting string from database to Line."""
        adapter = PostgresGeometryAdapter()
        result = adapter.from_database('{1,-1,0}', Line)
        assert isinstance(result, Line)
        assert result.A == 1.0
        assert result.B == -1.0

    def test_from_database_circle(self):
        """Test converting string from database to Circle."""
        adapter = PostgresGeometryAdapter()
        result = adapter.from_database('<(0,0),5>', Circle)
        assert isinstance(result, Circle)
        assert result.center.x == 0
        assert result.radius == 5.0

    def test_from_database_lseg(self):
        """Test converting string from database to LineSegment."""
        adapter = PostgresGeometryAdapter()
        result = adapter.from_database('[(0,0),(1,1)]', LineSegment)
        assert isinstance(result, LineSegment)
        assert result.start.x == 0
        assert result.end.x == 1

    def test_from_database_box(self):
        """Test converting string from database to Box."""
        adapter = PostgresGeometryAdapter()
        result = adapter.from_database('(10,10),(0,0)', Box)
        assert isinstance(result, Box)
        assert result.upper_right.x == 10
        assert result.lower_left.x == 0

    def test_from_database_path(self):
        """Test converting string from database to Path."""
        adapter = PostgresGeometryAdapter()
        result = adapter.from_database('[(0,0),(1,1)]', Path)
        assert isinstance(result, Path)
        assert result.is_closed is False
        assert len(result.points) == 2

    def test_from_database_polygon(self):
        """Test converting string from database to Polygon."""
        adapter = PostgresGeometryAdapter()
        result = adapter.from_database('((0,0),(1,1))', Polygon)
        assert isinstance(result, Polygon)
        assert len(result.points) == 2

    def test_batch_to_database(self):
        """Test batch conversion to database."""
        adapter = PostgresGeometryAdapter()
        values = [Point(1, 2), Point(3, 4), None]
        results = adapter.to_database_batch(values, Point)
        assert results[0] == '(1,2)'
        assert results[1] == '(3,4)'
        assert results[2] is None

    def test_batch_from_database(self):
        """Test batch conversion from database."""
        adapter = PostgresGeometryAdapter()
        values = ['(1,2)', '(3,4)', None]
        results = adapter.from_database_batch(values, Point)
        assert results[0].x == 1
        assert results[1].x == 3
        assert results[2] is None


class TestGeometryTypeMappings:
    """Tests for geometry type mappings."""

    def test_geometry_types_defined(self):
        """Test that all geometry types are defined."""
        adapter = PostgresGeometryAdapter()
        assert 'point' in adapter.GEOMETRY_TYPES
        assert 'line' in adapter.GEOMETRY_TYPES
        assert 'lseg' in adapter.GEOMETRY_TYPES
        assert 'box' in adapter.GEOMETRY_TYPES
        assert 'path' in adapter.GEOMETRY_TYPES
        assert 'polygon' in adapter.GEOMETRY_TYPES
        assert 'circle' in adapter.GEOMETRY_TYPES
