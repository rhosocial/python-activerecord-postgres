# tests/rhosocial/activerecord_postgres_test/feature/backend/postgres/test_postgis_types.py
"""Unit tests for PostgreSQL PostGIS geometry type.

Tests for:
- PostgresGeometry data class
- PostgresGeometryAdapter conversion
"""
import pytest

from rhosocial.activerecord.backend.impl.postgres.types.postgis import PostgresGeometry
from rhosocial.activerecord.backend.impl.postgres.adapters.postgis import PostgresGeometryAdapter


class TestPostgresGeometry:
    """Tests for PostgresGeometry data class."""

    def test_create_from_wkt(self):
        """Test creating geometry from WKT."""
        g = PostgresGeometry(wkt="POINT(1 2)")
        assert g.wkt == "POINT(1 2)"
        assert g.srid is None
        assert g.geometry_type == "geometry"

    def test_create_with_srid(self):
        """Test creating geometry with SRID."""
        g = PostgresGeometry(wkt="POINT(1 2)", srid=4326)
        assert g.srid == 4326

    def test_create_geography(self):
        """Test creating geography type."""
        g = PostgresGeometry(wkt="POINT(1 2)", geometry_type="geography")
        assert g.geometry_type == "geography"

    def test_invalid_geometry_type(self):
        """Test that invalid geometry_type raises ValueError."""
        with pytest.raises(ValueError, match="geometry_type must be"):
            PostgresGeometry(wkt="POINT(1 2)", geometry_type="invalid")

    def test_empty_wkt(self):
        """Test that empty WKT raises ValueError."""
        with pytest.raises(ValueError, match="WKT must not be empty"):
            PostgresGeometry(wkt="  ")

    def test_point_convenience(self):
        """Test point convenience constructor."""
        g = PostgresGeometry.point(1.0, 2.0)
        assert g.wkt == "POINT(1.0 2.0)"
        assert g.srid is None

    def test_point_with_srid(self):
        """Test point with SRID."""
        g = PostgresGeometry.point(1.0, 2.0, srid=4326)
        assert g.srid == 4326

    def test_point_geography(self):
        """Test point as geography."""
        g = PostgresGeometry.point(1.0, 2.0, srid=4326, geometry_type="geography")
        assert g.geometry_type == "geography"

    def test_linestring_convenience(self):
        """Test linestring convenience constructor."""
        g = PostgresGeometry.linestring([(1.0, 2.0), (3.0, 4.0)])
        assert "LINESTRING" in g.wkt
        assert "1.0 2.0" in g.wkt

    def test_linestring_too_few_points(self):
        """Test that linestring with <2 points raises ValueError."""
        with pytest.raises(ValueError, match="at least 2 points"):
            PostgresGeometry.linestring([(1.0, 2.0)])

    def test_polygon_convenience(self):
        """Test polygon convenience constructor."""
        g = PostgresGeometry.polygon([[(0, 0), (1, 0), (1, 1), (0, 1), (0, 0)]])
        assert "POLYGON" in g.wkt

    def test_polygon_no_rings(self):
        """Test that polygon with no rings raises ValueError."""
        with pytest.raises(ValueError, match="at least one ring"):
            PostgresGeometry.polygon([])

    def test_to_postgres_string_geometry_no_srid(self):
        """Test geometry to SQL without SRID."""
        g = PostgresGeometry(wkt="POINT(1 2)")
        assert g.to_postgres_string() == "ST_GeomFromText('POINT(1 2)')"

    def test_to_postgres_string_geometry_with_srid(self):
        """Test geometry to SQL with SRID."""
        g = PostgresGeometry(wkt="POINT(1 2)", srid=4326)
        assert g.to_postgres_string() == "ST_GeomFromText('POINT(1 2)', 4326)"

    def test_to_postgres_string_geography_no_srid(self):
        """Test geography to SQL without SRID."""
        g = PostgresGeometry(wkt="POINT(1 2)", geometry_type="geography")
        assert g.to_postgres_string() == "ST_GeogFromText('POINT(1 2)')"

    def test_to_postgres_string_geography_with_srid(self):
        """Test geography to SQL with SRID."""
        g = PostgresGeometry(wkt="POINT(1 2)", srid=4326, geometry_type="geography")
        assert g.to_postgres_string() == "ST_GeogFromText('SRID=4326;POINT(1 2)')"

    def test_from_postgres_string_wkt(self):
        """Test parsing WKT string."""
        g = PostgresGeometry.from_postgres_string("POINT(1 2)")
        assert g.wkt == "POINT(1 2)"
        assert g.srid is None

    def test_from_postgres_string_ewkt(self):
        """Test parsing EWKT string."""
        g = PostgresGeometry.from_postgres_string("SRID=4326;POINT(1 2)")
        assert g.wkt == "POINT(1 2)"
        assert g.srid == 4326

    def test_from_postgres_string_invalid_ewkt(self):
        """Test that invalid EWKT raises ValueError."""
        with pytest.raises(ValueError, match="Invalid SRID in EWKT"):
            PostgresGeometry.from_postgres_string("SRID=abc;POINT(1 2)")

    def test_equality(self):
        """Test equality."""
        g1 = PostgresGeometry(wkt="POINT(1 2)", srid=4326)
        g2 = PostgresGeometry(wkt="POINT(1 2)", srid=4326)
        assert g1 == g2

    def test_inequality(self):
        """Test inequality."""
        g1 = PostgresGeometry(wkt="POINT(1 2)")
        g2 = PostgresGeometry(wkt="POINT(3 4)")
        assert g1 != g2

    def test_hash(self):
        """Test hashability."""
        g1 = PostgresGeometry(wkt="POINT(1 2)", srid=4326)
        g2 = PostgresGeometry(wkt="POINT(1 2)", srid=4326)
        assert hash(g1) == hash(g2)

    def test_str_with_srid(self):
        """Test string representation with SRID."""
        g = PostgresGeometry(wkt="POINT(1 2)", srid=4326)
        assert str(g) == "SRID=4326;POINT(1 2)"

    def test_str_without_srid(self):
        """Test string representation without SRID."""
        g = PostgresGeometry(wkt="POINT(1 2)")
        assert str(g) == "POINT(1 2)"

    def test_frozen(self):
        """Test that geometry is immutable (frozen dataclass)."""
        g = PostgresGeometry(wkt="POINT(1 2)")
        with pytest.raises(AttributeError):
            g.wkt = "POINT(3 4)"


class TestPostgresGeometryAdapter:
    """Tests for PostgresGeometryAdapter."""

    def test_adapter_supported_types(self):
        """Test supported_types property."""
        adapter = PostgresGeometryAdapter()
        supported = adapter.supported_types
        assert PostgresGeometry in supported

    def test_to_database_geometry(self):
        """Test converting PostgresGeometry to database."""
        adapter = PostgresGeometryAdapter()
        g = PostgresGeometry.point(1.0, 2.0, srid=4326)
        result = adapter.to_database(g, str)
        assert result == "ST_GeomFromText('POINT(1.0 2.0)', 4326)"

    def test_to_database_geography(self):
        """Test converting PostgresGeometry geography to database."""
        adapter = PostgresGeometryAdapter()
        g = PostgresGeometry.point(1.0, 2.0, srid=4326, geometry_type="geography")
        result = adapter.to_database(g, str)
        assert result == "ST_GeogFromText('SRID=4326;POINT(1.0 2.0)')"

    def test_to_database_string(self):
        """Test converting string to database (passthrough)."""
        adapter = PostgresGeometryAdapter()
        result = adapter.to_database("ST_GeomFromText('POINT(1 2)')", str)
        assert result == "ST_GeomFromText('POINT(1 2)')"

    def test_to_database_none(self):
        """Test converting None to database."""
        adapter = PostgresGeometryAdapter()
        result = adapter.to_database(None, str)
        assert result is None

    def test_to_database_invalid_type(self):
        """Test converting invalid type raises TypeError."""
        adapter = PostgresGeometryAdapter()
        with pytest.raises(TypeError, match="Cannot convert"):
            adapter.to_database(12345, str)

    def test_from_database_wkt(self):
        """Test converting WKT string from database."""
        adapter = PostgresGeometryAdapter()
        result = adapter.from_database("POINT(1 2)", PostgresGeometry)
        assert isinstance(result, PostgresGeometry)
        assert result.wkt == "POINT(1 2)"

    def test_from_database_ewkt(self):
        """Test converting EWKT string from database."""
        adapter = PostgresGeometryAdapter()
        result = adapter.from_database("SRID=4326;POINT(1 2)", PostgresGeometry)
        assert isinstance(result, PostgresGeometry)
        assert result.srid == 4326

    def test_from_database_geometry(self):
        """Test converting PostgresGeometry from database."""
        adapter = PostgresGeometryAdapter()
        g = PostgresGeometry.point(1.0, 2.0)
        result = adapter.from_database(g, PostgresGeometry)
        assert result is g

    def test_from_database_none(self):
        """Test converting None from database."""
        adapter = PostgresGeometryAdapter()
        result = adapter.from_database(None, PostgresGeometry)
        assert result is None

    def test_from_database_invalid_type(self):
        """Test converting invalid type from database raises TypeError."""
        adapter = PostgresGeometryAdapter()
        with pytest.raises(TypeError, match="Cannot convert"):
            adapter.from_database(12345, PostgresGeometry)

    def test_batch_to_database(self):
        """Test batch conversion to database."""
        adapter = PostgresGeometryAdapter()
        values = [
            PostgresGeometry.point(1.0, 2.0, srid=4326),
            "ST_GeomFromText('POINT(3 4)')",
            None,
        ]
        results = adapter.to_database_batch(values, str)
        assert "ST_GeomFromText" in results[0]
        assert "ST_GeomFromText" in results[1]
        assert results[2] is None

    def test_batch_from_database(self):
        """Test batch conversion from database."""
        adapter = PostgresGeometryAdapter()
        values = ["POINT(1 2)", "SRID=4326;POINT(3 4)", None]
        results = adapter.from_database_batch(values, PostgresGeometry)
        assert isinstance(results[0], PostgresGeometry)
        assert isinstance(results[1], PostgresGeometry)
        assert results[1].srid == 4326
        assert results[2] is None
