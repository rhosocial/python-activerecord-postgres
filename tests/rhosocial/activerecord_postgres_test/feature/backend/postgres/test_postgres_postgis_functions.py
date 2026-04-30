# tests/rhosocial/activerecord_postgres_test/feature/backend/postgres/test_postgres_postgis_functions.py
"""
Tests for PostgreSQL PostGIS function factories.

Functions: st_make_point, st_geom_from_text, st_geog_from_text, st_set_srid,
           st_transform, st_contains, st_intersects, st_within, st_dwithin,
           st_crosses, st_touches, st_overlaps, st_distance, st_area,
           st_length, st_as_geojson, st_as_text, st_buffer, st_envelope,
           st_centroid
"""

import pytest

from rhosocial.activerecord.backend.impl.postgres.dialect import PostgresDialect
from rhosocial.activerecord.backend.impl.postgres.functions.postgis import (
    st_make_point,
    st_geom_from_text,
    st_geog_from_text,
    st_set_srid,
    st_transform,
    st_contains,
    st_intersects,
    st_within,
    st_dwithin,
    st_crosses,
    st_touches,
    st_overlaps,
    st_distance,
    st_area,
    st_length,
    st_as_geojson,
    st_as_text,
    st_buffer,
    st_envelope,
    st_centroid,
)
from rhosocial.activerecord.backend.impl.postgres.types.postgis import PostgresGeometry
from rhosocial.activerecord.backend.expression import bases
from rhosocial.activerecord.backend.expression.core import FunctionCall


class TestPostgisConstructionFunctions:
    """Tests for PostGIS construction functions."""

    def test_st_make_point_2d(self, postgres_dialect: PostgresDialect):
        """Test ST_MakePoint with 2D coordinates."""
        result = st_make_point(postgres_dialect, 1.0, 2.0)
        assert isinstance(result, FunctionCall)
        sql, params = result.to_sql()
        assert "ST_MAKEPOINT" in sql
        assert params == (1.0, 2.0)

    def test_st_make_point_3d(self, postgres_dialect: PostgresDialect):
        """Test ST_MakePoint with 3D coordinates."""
        result = st_make_point(postgres_dialect, 1.0, 2.0, 3.0)
        assert isinstance(result, FunctionCall)
        sql, params = result.to_sql()
        assert "ST_MAKEPOINT" in sql
        assert params == (1.0, 2.0, 3.0)

    def test_st_geom_from_text_no_srid(self, postgres_dialect: PostgresDialect):
        """Test ST_GeomFromText without SRID."""
        result = st_geom_from_text(postgres_dialect, "POINT(1 2)")
        assert isinstance(result, FunctionCall)
        sql, params = result.to_sql()
        assert "ST_GEOMFROMTEXT" in sql

    def test_st_geom_from_text_with_srid(self, postgres_dialect: PostgresDialect):
        """Test ST_GeomFromText with SRID."""
        result = st_geom_from_text(postgres_dialect, "POINT(1 2)", 4326)
        assert isinstance(result, FunctionCall)
        sql, params = result.to_sql()
        assert "ST_GEOMFROMTEXT" in sql
        assert 4326 in params

    def test_st_geog_from_text(self, postgres_dialect: PostgresDialect):
        """Test ST_GeogFromText."""
        result = st_geog_from_text(postgres_dialect, "POINT(1 2)")
        assert isinstance(result, FunctionCall)
        sql, params = result.to_sql()
        assert "ST_GEOGFROMTEXT" in sql

    def test_st_set_srid(self, postgres_dialect: PostgresDialect):
        """Test ST_SetSRID with nested ST_MakePoint."""
        point = st_make_point(postgres_dialect, 1.0, 2.0)
        result = st_set_srid(postgres_dialect, point, 4326)
        assert isinstance(result, FunctionCall)
        sql, params = result.to_sql()
        assert "ST_SETSRID" in sql
        assert "ST_MAKEPOINT" in sql
        assert 4326 in params

    def test_st_transform(self, postgres_dialect: PostgresDialect):
        """Test ST_Transform."""
        result = st_transform(postgres_dialect, "geom", 3857)
        assert isinstance(result, FunctionCall)
        sql, params = result.to_sql()
        assert "ST_TRANSFORM" in sql
        assert 3857 in params

    def test_st_make_point_with_postgresgeometry(self, postgres_dialect: PostgresDialect):
        """Test using PostgresGeometry object as input to predicate."""
        g = PostgresGeometry.point(1.0, 2.0, srid=4326)
        result = st_contains(postgres_dialect, "boundary", g)
        assert isinstance(result, FunctionCall)
        sql, params = result.to_sql()
        assert "ST_CONTAINS" in sql
        assert "ST_GEOMFROMTEXT" in sql


class TestPostgisPredicateFunctions:
    """Tests for PostGIS predicate functions."""

    def test_st_contains(self, postgres_dialect: PostgresDialect):
        """Test ST_Contains."""
        result = st_contains(postgres_dialect, "boundary", "point")
        assert isinstance(result, FunctionCall)
        sql, params = result.to_sql()
        assert "ST_CONTAINS" in sql

    def test_st_intersects(self, postgres_dialect: PostgresDialect):
        """Test ST_Intersects."""
        result = st_intersects(postgres_dialect, "geom1", "geom2")
        assert isinstance(result, FunctionCall)
        sql, params = result.to_sql()
        assert "ST_INTERSECTS" in sql

    def test_st_within(self, postgres_dialect: PostgresDialect):
        """Test ST_Within."""
        result = st_within(postgres_dialect, "inner", "outer")
        assert isinstance(result, FunctionCall)
        sql, params = result.to_sql()
        assert "ST_WITHIN" in sql

    def test_st_dwithin(self, postgres_dialect: PostgresDialect):
        """Test ST_DWithin without spheroid."""
        result = st_dwithin(postgres_dialect, "geom1", "geom2", 1000)
        assert isinstance(result, FunctionCall)
        sql, params = result.to_sql()
        assert "ST_DWITHIN" in sql

    def test_st_dwithin_spheroid(self, postgres_dialect: PostgresDialect):
        """Test ST_DWithin with spheroid."""
        result = st_dwithin(postgres_dialect, "geom1", "geom2", 1000, use_spheroid=True)
        assert isinstance(result, FunctionCall)
        sql, params = result.to_sql()
        assert "ST_DWITHIN" in sql

    def test_st_crosses(self, postgres_dialect: PostgresDialect):
        """Test ST_Crosses."""
        result = st_crosses(postgres_dialect, "geom1", "geom2")
        assert isinstance(result, FunctionCall)
        assert "ST_CROSSES" in result.to_sql()[0]

    def test_st_touches(self, postgres_dialect: PostgresDialect):
        """Test ST_Touches."""
        result = st_touches(postgres_dialect, "geom1", "geom2")
        assert isinstance(result, FunctionCall)
        assert "ST_TOUCHES" in result.to_sql()[0]

    def test_st_overlaps(self, postgres_dialect: PostgresDialect):
        """Test ST_Overlaps."""
        result = st_overlaps(postgres_dialect, "geom1", "geom2")
        assert isinstance(result, FunctionCall)
        assert "ST_OVERLAPS" in result.to_sql()[0]


class TestPostgisMeasurementFunctions:
    """Tests for PostGIS measurement functions."""

    def test_st_distance(self, postgres_dialect: PostgresDialect):
        """Test ST_Distance."""
        result = st_distance(postgres_dialect, "geom1", "geom2")
        assert isinstance(result, FunctionCall)
        sql, params = result.to_sql()
        assert "ST_DISTANCE" in sql

    def test_st_distance_spheroid(self, postgres_dialect: PostgresDialect):
        """Test ST_Distance with spheroid."""
        result = st_distance(postgres_dialect, "geom1", "geom2", use_spheroid=True)
        assert isinstance(result, FunctionCall)

    def test_st_area(self, postgres_dialect: PostgresDialect):
        """Test ST_Area."""
        result = st_area(postgres_dialect, "geom")
        assert isinstance(result, FunctionCall)
        assert "ST_AREA" in result.to_sql()[0]

    def test_st_length(self, postgres_dialect: PostgresDialect):
        """Test ST_Length."""
        result = st_length(postgres_dialect, "geom")
        assert isinstance(result, FunctionCall)
        assert "ST_LENGTH" in result.to_sql()[0]


class TestPostgisOutputFunctions:
    """Tests for PostGIS output functions."""

    def test_st_as_geojson(self, postgres_dialect: PostgresDialect):
        """Test ST_AsGeoJSON."""
        result = st_as_geojson(postgres_dialect, "geom")
        assert isinstance(result, FunctionCall)
        assert "ST_ASGEOJSON" in result.to_sql()[0]

    def test_st_as_text(self, postgres_dialect: PostgresDialect):
        """Test ST_AsText."""
        result = st_as_text(postgres_dialect, "geom")
        assert isinstance(result, FunctionCall)
        assert "ST_ASTEXT" in result.to_sql()[0]


class TestPostgisOperationFunctions:
    """Tests for PostGIS operation functions."""

    def test_st_buffer(self, postgres_dialect: PostgresDialect):
        """Test ST_Buffer."""
        result = st_buffer(postgres_dialect, "geom", 100)
        assert isinstance(result, FunctionCall)
        assert "ST_BUFFER" in result.to_sql()[0]

    def test_st_envelope(self, postgres_dialect: PostgresDialect):
        """Test ST_Envelope."""
        result = st_envelope(postgres_dialect, "geom")
        assert isinstance(result, FunctionCall)
        assert "ST_ENVELOPE" in result.to_sql()[0]

    def test_st_centroid(self, postgres_dialect: PostgresDialect):
        """Test ST_Centroid."""
        result = st_centroid(postgres_dialect, "geom")
        assert isinstance(result, FunctionCall)
        assert "ST_CENTROID" in result.to_sql()[0]


class TestPostgisNestedExpressions:
    """Tests for nested PostGIS expressions."""

    def test_nested_st_set_srid_st_make_point(self, postgres_dialect: PostgresDialect):
        """Test ST_SetSRID(ST_MakePoint(x, y), srid) pattern."""
        point = st_make_point(postgres_dialect, 116.4, 39.9)
        result = st_set_srid(postgres_dialect, point, 4326)
        sql, params = result.to_sql()
        assert "ST_SETSRID" in sql
        assert "ST_MAKEPOINT" in sql

    def test_all_functions_return_expression_objects(self, postgres_dialect: PostgresDialect):
        """Test that all PostGIS functions return Expression objects (not strings)."""
        assert isinstance(st_make_point(postgres_dialect, 1, 2), bases.BaseExpression)
        assert isinstance(st_geom_from_text(postgres_dialect, "POINT(1 2)"), bases.BaseExpression)
        assert isinstance(st_geog_from_text(postgres_dialect, "POINT(1 2)"), bases.BaseExpression)
        assert isinstance(st_set_srid(postgres_dialect, "geom", 4326), bases.BaseExpression)
        assert isinstance(st_transform(postgres_dialect, "geom", 3857), bases.BaseExpression)
        assert isinstance(st_contains(postgres_dialect, "a", "b"), bases.BaseExpression)
        assert isinstance(st_intersects(postgres_dialect, "a", "b"), bases.BaseExpression)
        assert isinstance(st_within(postgres_dialect, "a", "b"), bases.BaseExpression)
        assert isinstance(st_dwithin(postgres_dialect, "a", "b", 100), bases.BaseExpression)
        assert isinstance(st_distance(postgres_dialect, "a", "b"), bases.BaseExpression)
        assert isinstance(st_area(postgres_dialect, "geom"), bases.BaseExpression)
        assert isinstance(st_length(postgres_dialect, "geom"), bases.BaseExpression)
        assert isinstance(st_as_geojson(postgres_dialect, "geom"), bases.BaseExpression)
        assert isinstance(st_as_text(postgres_dialect, "geom"), bases.BaseExpression)
        assert isinstance(st_buffer(postgres_dialect, "geom", 100), bases.BaseExpression)
        assert isinstance(st_envelope(postgres_dialect, "geom"), bases.BaseExpression)
        assert isinstance(st_centroid(postgres_dialect, "geom"), bases.BaseExpression)
