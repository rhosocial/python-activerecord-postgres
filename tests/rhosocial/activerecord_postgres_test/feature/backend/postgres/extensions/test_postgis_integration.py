# tests/rhosocial/activerecord_postgres_test/feature/backend/postgres/extensions/test_postgis_integration.py

"""Integration tests for the PostGIS extension.

These tests require a PostgreSQL database with the PostGIS extension installed.
Tests will be automatically skipped if the extension is not available.

All database operations use expression objects, not raw SQL strings.
"""

import pytest
import pytest_asyncio

from rhosocial.activerecord_postgres_test.feature.backend.utils import (
    ensure_extension_installed,
    async_ensure_extension_installed,
)
from rhosocial.activerecord.backend.expression import (
    CreateTableExpression,
    ColumnDefinition,
    ColumnConstraint,
    ColumnConstraintType,
    DropTableExpression,
    InsertExpression,
    QueryExpression,
    TableExpression,
    Column,
    CreateIndexExpression,
    OrderByClause,
)
from rhosocial.activerecord.backend.expression.statements import ValuesSource
from rhosocial.activerecord.backend.expression.core import Literal, FunctionCall
from rhosocial.activerecord.backend.expression.predicates import ComparisonPredicate
from rhosocial.activerecord.backend.options import ExecutionOptions
from rhosocial.activerecord.backend.schema import StatementType


# --- Helper functions ---


def _setup_geo_points_table(backend, dialect, table_name):
    """Create and populate the test_geo_points table using expressions."""
    columns = [
        ColumnDefinition(
            name="id",
            data_type="SERIAL",
            constraints=[
                ColumnConstraint(ColumnConstraintType.PRIMARY_KEY),
            ],
        ),
        ColumnDefinition(name="geom", data_type="GEOMETRY(POINT, 4326)"),
    ]
    create_expr = CreateTableExpression(
        dialect=dialect,
        table_name=table_name,
        columns=columns,
        if_not_exists=True,
    )
    sql, params = create_expr.to_sql()
    backend.execute(sql, params)

    insert_expr = InsertExpression(
        dialect=dialect,
        into=table_name,
        columns=["geom"],
        source=ValuesSource(
            dialect,
            [
                [FunctionCall(dialect, "ST_GeomFromText", Literal(dialect, "POINT(1.0 2.0)"), Literal(dialect, 4326))],
                [FunctionCall(dialect, "ST_GeomFromText", Literal(dialect, "POINT(3.0 4.0)"), Literal(dialect, 4326))],
            ],
        ),
    )
    sql, params = insert_expr.to_sql()
    backend.execute(sql, params)


def _setup_geo_distance_table(backend, dialect, table_name):
    """Create and populate the test_geo_distance table using expressions."""
    columns = [
        ColumnDefinition(
            name="id",
            data_type="SERIAL",
            constraints=[
                ColumnConstraint(ColumnConstraintType.PRIMARY_KEY),
            ],
        ),
        ColumnDefinition(name="geom", data_type="GEOMETRY(POINT, 4326)"),
    ]
    create_expr = CreateTableExpression(
        dialect=dialect,
        table_name=table_name,
        columns=columns,
        if_not_exists=True,
    )
    sql, params = create_expr.to_sql()
    backend.execute(sql, params)

    insert_expr = InsertExpression(
        dialect=dialect,
        into=table_name,
        columns=["geom"],
        source=ValuesSource(
            dialect,
            [
                [FunctionCall(dialect, "ST_GeomFromText", Literal(dialect, "POINT(0 0)"), Literal(dialect, 4326))],
                [FunctionCall(dialect, "ST_GeomFromText", Literal(dialect, "POINT(3 4)"), Literal(dialect, 4326))],
            ],
        ),
    )
    sql, params = insert_expr.to_sql()
    backend.execute(sql, params)


def _setup_geo_contains_table(backend, dialect, table_name):
    """Create and populate the test_geo_contains table using expressions."""
    columns = [
        ColumnDefinition(
            name="id",
            data_type="SERIAL",
            constraints=[
                ColumnConstraint(ColumnConstraintType.PRIMARY_KEY),
            ],
        ),
        ColumnDefinition(name="geom", data_type="GEOMETRY(POLYGON, 4326)"),
    ]
    create_expr = CreateTableExpression(
        dialect=dialect,
        table_name=table_name,
        columns=columns,
        if_not_exists=True,
    )
    sql, params = create_expr.to_sql()
    backend.execute(sql, params)

    insert_expr = InsertExpression(
        dialect=dialect,
        into=table_name,
        columns=["geom"],
        source=ValuesSource(
            dialect,
            [
                [
                    FunctionCall(
                        dialect,
                        "ST_GeomFromText",
                        Literal(dialect, "POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))"),
                        Literal(dialect, 4326),
                    )
                ],
            ],
        ),
    )
    sql, params = insert_expr.to_sql()
    backend.execute(sql, params)


def _setup_geo_index_table(backend, dialect, table_name, index_name):
    """Create and populate the test_geo_index table with GiST index using expressions."""
    columns = [
        ColumnDefinition(
            name="id",
            data_type="SERIAL",
            constraints=[
                ColumnConstraint(ColumnConstraintType.PRIMARY_KEY),
            ],
        ),
        ColumnDefinition(name="geom", data_type="GEOMETRY(POINT, 4326)"),
    ]
    create_expr = CreateTableExpression(
        dialect=dialect,
        table_name=table_name,
        columns=columns,
        if_not_exists=True,
    )
    sql, params = create_expr.to_sql()
    backend.execute(sql, params)

    insert_expr = InsertExpression(
        dialect=dialect,
        into=table_name,
        columns=["geom"],
        source=ValuesSource(
            dialect,
            [
                [FunctionCall(dialect, "ST_GeomFromText", Literal(dialect, "POINT(1 1)"), Literal(dialect, 4326))],
            ],
        ),
    )
    sql, params = insert_expr.to_sql()
    backend.execute(sql, params)

    create_index = CreateIndexExpression(
        dialect=dialect,
        index_name=index_name,
        table_name=table_name,
        columns=["geom"],
        index_type="GIST",
        if_not_exists=True,
    )
    sql, params = create_index.to_sql()
    backend.execute(sql, params)


def _setup_geog_table(backend, dialect, table_name):
    """Create and populate the test_geog table using expressions."""
    columns = [
        ColumnDefinition(
            name="id",
            data_type="SERIAL",
            constraints=[
                ColumnConstraint(ColumnConstraintType.PRIMARY_KEY),
            ],
        ),
        ColumnDefinition(name="geog", data_type="GEOGRAPHY(POINT, 4326)"),
    ]
    create_expr = CreateTableExpression(
        dialect=dialect,
        table_name=table_name,
        columns=columns,
        if_not_exists=True,
    )
    sql, params = create_expr.to_sql()
    backend.execute(sql, params)

    insert_expr = InsertExpression(
        dialect=dialect,
        into=table_name,
        columns=["geog"],
        source=ValuesSource(
            dialect,
            [
                [FunctionCall(dialect, "ST_GeogFromText", Literal(dialect, "SRID=4326;POINT(0 0)"))],
            ],
        ),
    )
    sql, params = insert_expr.to_sql()
    backend.execute(sql, params)


def _teardown_table(backend, dialect, table_name):
    """Drop a test table using expression."""
    drop_expr = DropTableExpression(
        dialect=dialect,
        table_name=table_name,
        if_exists=True,
    )
    sql, params = drop_expr.to_sql()
    backend.execute(sql, params)


async def _async_setup_geo_points_table(backend, dialect, table_name):
    """Async: create and populate the test_geo_points table using expressions."""
    columns = [
        ColumnDefinition(
            name="id",
            data_type="SERIAL",
            constraints=[
                ColumnConstraint(ColumnConstraintType.PRIMARY_KEY),
            ],
        ),
        ColumnDefinition(name="geom", data_type="GEOMETRY(POINT, 4326)"),
    ]
    create_expr = CreateTableExpression(
        dialect=dialect,
        table_name=table_name,
        columns=columns,
        if_not_exists=True,
    )
    sql, params = create_expr.to_sql()
    await backend.execute(sql, params)

    insert_expr = InsertExpression(
        dialect=dialect,
        into=table_name,
        columns=["geom"],
        source=ValuesSource(
            dialect,
            [
                [FunctionCall(dialect, "ST_GeomFromText", Literal(dialect, "POINT(1.0 2.0)"), Literal(dialect, 4326))],
                [FunctionCall(dialect, "ST_GeomFromText", Literal(dialect, "POINT(3.0 4.0)"), Literal(dialect, 4326))],
            ],
        ),
    )
    sql, params = insert_expr.to_sql()
    await backend.execute(sql, params)


async def _async_setup_geo_distance_table(backend, dialect, table_name):
    """Async: create and populate the test_geo_distance table using expressions."""
    columns = [
        ColumnDefinition(
            name="id",
            data_type="SERIAL",
            constraints=[
                ColumnConstraint(ColumnConstraintType.PRIMARY_KEY),
            ],
        ),
        ColumnDefinition(name="geom", data_type="GEOMETRY(POINT, 4326)"),
    ]
    create_expr = CreateTableExpression(
        dialect=dialect,
        table_name=table_name,
        columns=columns,
        if_not_exists=True,
    )
    sql, params = create_expr.to_sql()
    await backend.execute(sql, params)

    insert_expr = InsertExpression(
        dialect=dialect,
        into=table_name,
        columns=["geom"],
        source=ValuesSource(
            dialect,
            [
                [FunctionCall(dialect, "ST_GeomFromText", Literal(dialect, "POINT(0 0)"), Literal(dialect, 4326))],
                [FunctionCall(dialect, "ST_GeomFromText", Literal(dialect, "POINT(3 4)"), Literal(dialect, 4326))],
            ],
        ),
    )
    sql, params = insert_expr.to_sql()
    await backend.execute(sql, params)


async def _async_setup_geo_contains_table(backend, dialect, table_name):
    """Async: create and populate the test_geo_contains table using expressions."""
    columns = [
        ColumnDefinition(
            name="id",
            data_type="SERIAL",
            constraints=[
                ColumnConstraint(ColumnConstraintType.PRIMARY_KEY),
            ],
        ),
        ColumnDefinition(name="geom", data_type="GEOMETRY(POLYGON, 4326)"),
    ]
    create_expr = CreateTableExpression(
        dialect=dialect,
        table_name=table_name,
        columns=columns,
        if_not_exists=True,
    )
    sql, params = create_expr.to_sql()
    await backend.execute(sql, params)

    insert_expr = InsertExpression(
        dialect=dialect,
        into=table_name,
        columns=["geom"],
        source=ValuesSource(
            dialect,
            [
                [
                    FunctionCall(
                        dialect,
                        "ST_GeomFromText",
                        Literal(dialect, "POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))"),
                        Literal(dialect, 4326),
                    )
                ],
            ],
        ),
    )
    sql, params = insert_expr.to_sql()
    await backend.execute(sql, params)


async def _async_setup_geo_index_table(backend, dialect, table_name, index_name):
    """Async: create and populate the test_geo_index table with GiST index using expressions."""
    columns = [
        ColumnDefinition(
            name="id",
            data_type="SERIAL",
            constraints=[
                ColumnConstraint(ColumnConstraintType.PRIMARY_KEY),
            ],
        ),
        ColumnDefinition(name="geom", data_type="GEOMETRY(POINT, 4326)"),
    ]
    create_expr = CreateTableExpression(
        dialect=dialect,
        table_name=table_name,
        columns=columns,
        if_not_exists=True,
    )
    sql, params = create_expr.to_sql()
    await backend.execute(sql, params)

    insert_expr = InsertExpression(
        dialect=dialect,
        into=table_name,
        columns=["geom"],
        source=ValuesSource(
            dialect,
            [
                [FunctionCall(dialect, "ST_GeomFromText", Literal(dialect, "POINT(1 1)"), Literal(dialect, 4326))],
            ],
        ),
    )
    sql, params = insert_expr.to_sql()
    await backend.execute(sql, params)

    create_index = CreateIndexExpression(
        dialect=dialect,
        index_name=index_name,
        table_name=table_name,
        columns=["geom"],
        index_type="GIST",
        if_not_exists=True,
    )
    sql, params = create_index.to_sql()
    await backend.execute(sql, params)


async def _async_setup_geog_table(backend, dialect, table_name):
    """Async: create and populate the test_geog table using expressions."""
    columns = [
        ColumnDefinition(
            name="id",
            data_type="SERIAL",
            constraints=[
                ColumnConstraint(ColumnConstraintType.PRIMARY_KEY),
            ],
        ),
        ColumnDefinition(name="geog", data_type="GEOGRAPHY(POINT, 4326)"),
    ]
    create_expr = CreateTableExpression(
        dialect=dialect,
        table_name=table_name,
        columns=columns,
        if_not_exists=True,
    )
    sql, params = create_expr.to_sql()
    await backend.execute(sql, params)

    insert_expr = InsertExpression(
        dialect=dialect,
        into=table_name,
        columns=["geog"],
        source=ValuesSource(
            dialect,
            [
                [FunctionCall(dialect, "ST_GeogFromText", Literal(dialect, "SRID=4326;POINT(0 0)"))],
            ],
        ),
    )
    sql, params = insert_expr.to_sql()
    await backend.execute(sql, params)


async def _async_teardown_table(backend, dialect, table_name):
    """Async: drop a test table using expression."""
    drop_expr = DropTableExpression(
        dialect=dialect,
        table_name=table_name,
        if_exists=True,
    )
    sql, params = drop_expr.to_sql()
    await backend.execute(sql, params)


# --- Table name constants ---

T_GEO_POINTS = "test_geo_points"
T_GEO_DISTANCE = "test_geo_distance"
T_GEO_CONTAINS = "test_geo_contains"
T_GEO_INDEX = "test_geo_index"
T_GEOG = "test_geog"
IDX_GEO = "idx_test_geo_geom"

T_GEO_POINTS_ASYNC = "test_geo_points_async"
T_GEO_DISTANCE_ASYNC = "test_geo_distance_async"
T_GEO_CONTAINS_ASYNC = "test_geo_contains_async"
T_GEO_INDEX_ASYNC = "test_geo_index_async"
T_GEOG_ASYNC = "test_geog_async"
IDX_GEO_ASYNC = "idx_test_geo_geom_async"


# --- Sync fixture and tests ---


@pytest.fixture
def postgis_env(postgres_backend_single):
    """Independent test environment for PostGIS extension."""
    backend = postgres_backend_single
    ensure_extension_installed(backend, "postgis")
    dialect = backend.dialect

    # Clean up residual tables from previous runs
    for table_name in [T_GEO_POINTS, T_GEO_DISTANCE, T_GEO_CONTAINS, T_GEO_INDEX, T_GEOG]:
        _teardown_table(backend, dialect, table_name)

    _setup_geo_points_table(backend, dialect, T_GEO_POINTS)
    _setup_geo_distance_table(backend, dialect, T_GEO_DISTANCE)
    _setup_geo_contains_table(backend, dialect, T_GEO_CONTAINS)
    _setup_geo_index_table(backend, dialect, T_GEO_INDEX, IDX_GEO)
    _setup_geog_table(backend, dialect, T_GEOG)

    yield backend, dialect

    for table_name in [T_GEO_POINTS, T_GEO_DISTANCE, T_GEO_CONTAINS, T_GEO_INDEX, T_GEOG]:
        _teardown_table(backend, dialect, table_name)


class TestPostGISIntegration:
    """Integration tests for PostGIS spatial database functionality."""

    def test_geometry_type_crud(self, postgis_env):
        """Test querying geometry column data."""
        backend, dialect = postgis_env
        opts = ExecutionOptions(stmt_type=StatementType.DQL)

        query = QueryExpression(
            dialect=dialect,
            select=[
                FunctionCall(dialect, "ST_AsText", Column(dialect, "geom")).as_("wkt"),
            ],
            from_=TableExpression(dialect, T_GEO_POINTS),
            order_by=OrderByClause(dialect, [Column(dialect, "id")]),
        )
        sql, params = query.to_sql()
        result = backend.execute(sql, params, options=opts)
        assert result.data is not None
        assert len(result.data) == 2
        assert result.data[0]["wkt"] == "POINT(1 2)"
        assert result.data[1]["wkt"] == "POINT(3 4)"

    def test_st_distance(self, postgis_env):
        """Test ST_Distance function for measuring distances between geometries."""
        backend, dialect = postgis_env
        opts = ExecutionOptions(stmt_type=StatementType.DQL)

        # Distance from (0,0) to (3,4) = 5 (Pythagorean theorem in 2D plane)
        query = QueryExpression(
            dialect=dialect,
            select=[
                FunctionCall(
                    dialect,
                    "ST_Distance",
                    FunctionCall(dialect, "ST_GeomFromText", Literal(dialect, "POINT(0 0)"), Literal(dialect, 4326)),
                    FunctionCall(dialect, "ST_GeomFromText", Literal(dialect, "POINT(3 4)"), Literal(dialect, 4326)),
                ).as_("dist"),
            ],
        )
        sql, params = query.to_sql()
        result = backend.execute(sql, params, options=opts)
        assert result.data is not None
        assert float(result.data[0]["dist"]) == pytest.approx(5.0, abs=1e-4)

    def test_st_contains(self, postgis_env):
        """Test ST_Contains function for spatial containment."""
        backend, dialect = postgis_env
        opts = ExecutionOptions(stmt_type=StatementType.DQL)

        # Point inside the polygon
        query_inside = QueryExpression(
            dialect=dialect,
            select=[
                FunctionCall(
                    dialect,
                    "ST_Contains",
                    FunctionCall(
                        dialect,
                        "ST_GeomFromText",
                        Literal(dialect, "POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))"),
                        Literal(dialect, 4326),
                    ),
                    FunctionCall(dialect, "ST_GeomFromText", Literal(dialect, "POINT(5 5)"), Literal(dialect, 4326)),
                ).as_("contains"),
            ],
        )
        sql, params = query_inside.to_sql()
        result_inside = backend.execute(sql, params, options=opts)
        assert result_inside.data is not None
        assert result_inside.data[0]["contains"] is True

        # Point outside the polygon
        query_outside = QueryExpression(
            dialect=dialect,
            select=[
                FunctionCall(
                    dialect,
                    "ST_Contains",
                    FunctionCall(
                        dialect,
                        "ST_GeomFromText",
                        Literal(dialect, "POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))"),
                        Literal(dialect, 4326),
                    ),
                    FunctionCall(dialect, "ST_GeomFromText", Literal(dialect, "POINT(15 15)"), Literal(dialect, 4326)),
                ).as_("contains"),
            ],
        )
        sql, params = query_outside.to_sql()
        result_outside = backend.execute(sql, params, options=opts)
        assert result_outside.data is not None
        assert result_outside.data[0]["contains"] is False

    def test_st_intersects(self, postgis_env):
        """Test ST_Intersects function for checking spatial intersection."""
        backend, dialect = postgis_env
        opts = ExecutionOptions(stmt_type=StatementType.DQL)

        # Two polygons that share an area
        query = QueryExpression(
            dialect=dialect,
            select=[
                FunctionCall(
                    dialect,
                    "ST_Intersects",
                    FunctionCall(
                        dialect,
                        "ST_GeomFromText",
                        Literal(dialect, "POLYGON((0 0, 5 0, 5 5, 0 5, 0 0))"),
                        Literal(dialect, 4326),
                    ),
                    FunctionCall(
                        dialect,
                        "ST_GeomFromText",
                        Literal(dialect, "POLYGON((3 3, 8 3, 8 8, 3 8, 3 3))"),
                        Literal(dialect, 4326),
                    ),
                ).as_("intersects"),
            ],
        )
        sql, params = query.to_sql()
        result = backend.execute(sql, params, options=opts)
        assert result.data is not None
        assert result.data[0]["intersects"] is True

        # Two polygons that don't intersect
        query_no = QueryExpression(
            dialect=dialect,
            select=[
                FunctionCall(
                    dialect,
                    "ST_Intersects",
                    FunctionCall(
                        dialect,
                        "ST_GeomFromText",
                        Literal(dialect, "POLYGON((0 0, 2 0, 2 2, 0 2, 0 0))"),
                        Literal(dialect, 4326),
                    ),
                    FunctionCall(
                        dialect,
                        "ST_GeomFromText",
                        Literal(dialect, "POLYGON((5 5, 8 5, 8 8, 5 8, 5 5))"),
                        Literal(dialect, 4326),
                    ),
                ).as_("intersects"),
            ],
        )
        sql, params = query_no.to_sql()
        result_no = backend.execute(sql, params, options=opts)
        assert result_no.data is not None
        assert result_no.data[0]["intersects"] is False

    def test_st_area(self, postgis_env):
        """Test ST_Area function for calculating polygon area."""
        backend, dialect = postgis_env
        opts = ExecutionOptions(stmt_type=StatementType.DQL)

        query = QueryExpression(
            dialect=dialect,
            select=[
                FunctionCall(
                    dialect,
                    "ST_Area",
                    FunctionCall(
                        dialect,
                        "ST_GeomFromText",
                        Literal(dialect, "POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))"),
                        Literal(dialect, 4326),
                    ),
                ).as_("area"),
            ],
        )
        sql, params = query.to_sql()
        result = backend.execute(sql, params, options=opts)
        assert result.data is not None
        # Area of a 10x10 square in SRID 4326 (geographic coords) is not 100
        # but we can verify the result is positive
        assert float(result.data[0]["area"]) > 0

    def test_spatial_index(self, postgis_env):
        """Test that GiST spatial index was created successfully."""
        backend, dialect = postgis_env
        opts = ExecutionOptions(stmt_type=StatementType.DQL)

        query = QueryExpression(
            dialect=dialect,
            select=[Column(dialect, "indexname")],
            from_=TableExpression(dialect, "pg_indexes"),
            where=ComparisonPredicate(
                dialect,
                "=",
                Column(dialect, "tablename"),
                Literal(dialect, T_GEO_INDEX),
            ),
        )
        sql, params = query.to_sql()
        result = backend.execute(sql, params, options=opts)
        assert result.data is not None
        index_names = [row["indexname"] for row in result.data]
        assert IDX_GEO in index_names

    def test_geography_type(self, postgis_env):
        """Test geography type for geodetic calculations."""
        backend, dialect = postgis_env
        opts = ExecutionOptions(stmt_type=StatementType.DQL)

        query = QueryExpression(
            dialect=dialect,
            select=[
                FunctionCall(dialect, "ST_AsText", Column(dialect, "geog")).as_("wkt"),
            ],
            from_=TableExpression(dialect, T_GEOG),
        )
        sql, params = query.to_sql()
        result = backend.execute(sql, params, options=opts)
        assert result.data is not None
        assert len(result.data) == 1
        assert result.data[0]["wkt"] is not None

    def test_st_transform(self, postgis_env):
        """Test ST_Transform for coordinate system conversion."""
        backend, dialect = postgis_env
        opts = ExecutionOptions(stmt_type=StatementType.DQL)

        # Transform a point from WGS84 (4326) to Web Mercator (3857)
        query = QueryExpression(
            dialect=dialect,
            select=[
                FunctionCall(
                    dialect,
                    "ST_AsText",
                    FunctionCall(
                        dialect,
                        "ST_Transform",
                        FunctionCall(
                            dialect, "ST_GeomFromText", Literal(dialect, "POINT(0 0)"), Literal(dialect, 4326)
                        ),
                        Literal(dialect, 3857),
                    ),
                ).as_("wkt"),
            ],
        )
        sql, params = query.to_sql()
        result = backend.execute(sql, params, options=opts)
        assert result.data is not None
        # The origin (0,0) in WGS84 should map to (0,0) in Web Mercator
        assert result.data[0]["wkt"] is not None

    def test_st_as_geojson(self, postgis_env):
        """Test ST_AsGeoJSON for GeoJSON output."""
        backend, dialect = postgis_env
        opts = ExecutionOptions(stmt_type=StatementType.DQL)

        query = QueryExpression(
            dialect=dialect,
            select=[
                FunctionCall(
                    dialect,
                    "ST_AsGeoJSON",
                    FunctionCall(dialect, "ST_GeomFromText", Literal(dialect, "POINT(1 2)"), Literal(dialect, 4326)),
                ).as_("geojson"),
            ],
        )
        sql, params = query.to_sql()
        result = backend.execute(sql, params, options=opts)
        assert result.data is not None
        geojson = result.data[0]["geojson"]
        assert '"type"' in geojson
        assert '"Point"' in geojson
        assert '"coordinates"' in geojson

    def test_st_buffer(self, postgis_env):
        """Test ST_Buffer for creating buffer zones around geometries."""
        backend, dialect = postgis_env
        opts = ExecutionOptions(stmt_type=StatementType.DQL)

        # ST_Buffer with ::geography cast - FunctionCall.cast() generates the
        # PostgreSQL-specific ::type syntax (e.g. ST_GEOMFROMTEXT(...)::geography)
        geog_point = FunctionCall(
            dialect, "ST_GeomFromText",
            Literal(dialect, "POINT(0 0)"), Literal(dialect, 4326),
        ).cast("geography")
        query = QueryExpression(
            dialect=dialect,
            select=[
                FunctionCall(
                    dialect,
                    "ST_AsText",
                    FunctionCall(
                        dialect,
                        "ST_Buffer",
                        geog_point,
                        Literal(dialect, 1000),
                    ),
                ).as_("wkt"),
            ],
        )
        sql, params = query.to_sql()
        result = backend.execute(sql, params, options=opts)
        assert result.data is not None
        # Buffer of a point should produce a polygon
        assert result.data[0]["wkt"].startswith("POLYGON")


# --- Async fixture and tests ---


@pytest_asyncio.fixture
async def async_postgis_env(async_postgres_backend_single):
    """Independent async test environment for PostGIS extension."""
    backend = async_postgres_backend_single
    await async_ensure_extension_installed(backend, "postgis")
    dialect = backend.dialect

    # Clean up residual tables from previous runs
    for table_name in [T_GEO_POINTS_ASYNC, T_GEO_DISTANCE_ASYNC, T_GEO_CONTAINS_ASYNC, T_GEO_INDEX_ASYNC, T_GEOG_ASYNC]:
        await _async_teardown_table(backend, dialect, table_name)

    await _async_setup_geo_points_table(backend, dialect, T_GEO_POINTS_ASYNC)
    await _async_setup_geo_distance_table(backend, dialect, T_GEO_DISTANCE_ASYNC)
    await _async_setup_geo_contains_table(backend, dialect, T_GEO_CONTAINS_ASYNC)
    await _async_setup_geo_index_table(backend, dialect, T_GEO_INDEX_ASYNC, IDX_GEO_ASYNC)
    await _async_setup_geog_table(backend, dialect, T_GEOG_ASYNC)

    yield backend, dialect

    for table_name in [T_GEO_POINTS_ASYNC, T_GEO_DISTANCE_ASYNC, T_GEO_CONTAINS_ASYNC, T_GEO_INDEX_ASYNC, T_GEOG_ASYNC]:
        await _async_teardown_table(backend, dialect, table_name)


class TestAsyncPostGISIntegration:
    """Async integration tests for PostGIS spatial database functionality."""

    @pytest.mark.asyncio
    async def test_async_geometry_type_crud(self, async_postgis_env):
        """Test querying geometry column data."""
        backend, dialect = async_postgis_env
        opts = ExecutionOptions(stmt_type=StatementType.DQL)

        query = QueryExpression(
            dialect=dialect,
            select=[
                FunctionCall(dialect, "ST_AsText", Column(dialect, "geom")).as_("wkt"),
            ],
            from_=TableExpression(dialect, T_GEO_POINTS_ASYNC),
            order_by=OrderByClause(dialect, [Column(dialect, "id")]),
        )
        sql, params = query.to_sql()
        result = await backend.execute(sql, params, options=opts)
        assert result.data is not None
        assert len(result.data) == 2
        assert result.data[0]["wkt"] == "POINT(1 2)"
        assert result.data[1]["wkt"] == "POINT(3 4)"

    @pytest.mark.asyncio
    async def test_async_st_distance(self, async_postgis_env):
        """Test ST_Distance function for measuring distances between geometries."""
        backend, dialect = async_postgis_env
        opts = ExecutionOptions(stmt_type=StatementType.DQL)

        # Distance from (0,0) to (3,4) = 5 (Pythagorean theorem in 2D plane)
        query = QueryExpression(
            dialect=dialect,
            select=[
                FunctionCall(
                    dialect,
                    "ST_Distance",
                    FunctionCall(dialect, "ST_GeomFromText", Literal(dialect, "POINT(0 0)"), Literal(dialect, 4326)),
                    FunctionCall(dialect, "ST_GeomFromText", Literal(dialect, "POINT(3 4)"), Literal(dialect, 4326)),
                ).as_("dist"),
            ],
        )
        sql, params = query.to_sql()
        result = await backend.execute(sql, params, options=opts)
        assert result.data is not None
        assert float(result.data[0]["dist"]) == pytest.approx(5.0, abs=1e-4)

    @pytest.mark.asyncio
    async def test_async_st_contains(self, async_postgis_env):
        """Test ST_Contains function for spatial containment."""
        backend, dialect = async_postgis_env
        opts = ExecutionOptions(stmt_type=StatementType.DQL)

        # Point inside the polygon
        query_inside = QueryExpression(
            dialect=dialect,
            select=[
                FunctionCall(
                    dialect,
                    "ST_Contains",
                    FunctionCall(
                        dialect,
                        "ST_GeomFromText",
                        Literal(dialect, "POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))"),
                        Literal(dialect, 4326),
                    ),
                    FunctionCall(dialect, "ST_GeomFromText", Literal(dialect, "POINT(5 5)"), Literal(dialect, 4326)),
                ).as_("contains"),
            ],
        )
        sql, params = query_inside.to_sql()
        result_inside = await backend.execute(sql, params, options=opts)
        assert result_inside.data is not None
        assert result_inside.data[0]["contains"] is True

        # Point outside the polygon
        query_outside = QueryExpression(
            dialect=dialect,
            select=[
                FunctionCall(
                    dialect,
                    "ST_Contains",
                    FunctionCall(
                        dialect,
                        "ST_GeomFromText",
                        Literal(dialect, "POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))"),
                        Literal(dialect, 4326),
                    ),
                    FunctionCall(dialect, "ST_GeomFromText", Literal(dialect, "POINT(15 15)"), Literal(dialect, 4326)),
                ).as_("contains"),
            ],
        )
        sql, params = query_outside.to_sql()
        result_outside = await backend.execute(sql, params, options=opts)
        assert result_outside.data is not None
        assert result_outside.data[0]["contains"] is False

    @pytest.mark.asyncio
    async def test_async_st_intersects(self, async_postgis_env):
        """Test ST_Intersects function for checking spatial intersection."""
        backend, dialect = async_postgis_env
        opts = ExecutionOptions(stmt_type=StatementType.DQL)

        # Two polygons that share an area
        query = QueryExpression(
            dialect=dialect,
            select=[
                FunctionCall(
                    dialect,
                    "ST_Intersects",
                    FunctionCall(
                        dialect,
                        "ST_GeomFromText",
                        Literal(dialect, "POLYGON((0 0, 5 0, 5 5, 0 5, 0 0))"),
                        Literal(dialect, 4326),
                    ),
                    FunctionCall(
                        dialect,
                        "ST_GeomFromText",
                        Literal(dialect, "POLYGON((3 3, 8 3, 8 8, 3 8, 3 3))"),
                        Literal(dialect, 4326),
                    ),
                ).as_("intersects"),
            ],
        )
        sql, params = query.to_sql()
        result = await backend.execute(sql, params, options=opts)
        assert result.data is not None
        assert result.data[0]["intersects"] is True

        # Two polygons that don't intersect
        query_no = QueryExpression(
            dialect=dialect,
            select=[
                FunctionCall(
                    dialect,
                    "ST_Intersects",
                    FunctionCall(
                        dialect,
                        "ST_GeomFromText",
                        Literal(dialect, "POLYGON((0 0, 2 0, 2 2, 0 2, 0 0))"),
                        Literal(dialect, 4326),
                    ),
                    FunctionCall(
                        dialect,
                        "ST_GeomFromText",
                        Literal(dialect, "POLYGON((5 5, 8 5, 8 8, 5 8, 5 5))"),
                        Literal(dialect, 4326),
                    ),
                ).as_("intersects"),
            ],
        )
        sql, params = query_no.to_sql()
        result_no = await backend.execute(sql, params, options=opts)
        assert result_no.data is not None
        assert result_no.data[0]["intersects"] is False

    @pytest.mark.asyncio
    async def test_async_st_area(self, async_postgis_env):
        """Test ST_Area function for calculating polygon area."""
        backend, dialect = async_postgis_env
        opts = ExecutionOptions(stmt_type=StatementType.DQL)

        query = QueryExpression(
            dialect=dialect,
            select=[
                FunctionCall(
                    dialect,
                    "ST_Area",
                    FunctionCall(
                        dialect,
                        "ST_GeomFromText",
                        Literal(dialect, "POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))"),
                        Literal(dialect, 4326),
                    ),
                ).as_("area"),
            ],
        )
        sql, params = query.to_sql()
        result = await backend.execute(sql, params, options=opts)
        assert result.data is not None
        assert float(result.data[0]["area"]) > 0

    @pytest.mark.asyncio
    async def test_async_spatial_index(self, async_postgis_env):
        """Test that GiST spatial index was created successfully."""
        backend, dialect = async_postgis_env
        opts = ExecutionOptions(stmt_type=StatementType.DQL)

        query = QueryExpression(
            dialect=dialect,
            select=[Column(dialect, "indexname")],
            from_=TableExpression(dialect, "pg_indexes"),
            where=ComparisonPredicate(
                dialect,
                "=",
                Column(dialect, "tablename"),
                Literal(dialect, T_GEO_INDEX_ASYNC),
            ),
        )
        sql, params = query.to_sql()
        result = await backend.execute(sql, params, options=opts)
        assert result.data is not None
        index_names = [row["indexname"] for row in result.data]
        assert IDX_GEO_ASYNC in index_names

    @pytest.mark.asyncio
    async def test_async_geography_type(self, async_postgis_env):
        """Test geography type for geodetic calculations."""
        backend, dialect = async_postgis_env
        opts = ExecutionOptions(stmt_type=StatementType.DQL)

        query = QueryExpression(
            dialect=dialect,
            select=[
                FunctionCall(dialect, "ST_AsText", Column(dialect, "geog")).as_("wkt"),
            ],
            from_=TableExpression(dialect, T_GEOG_ASYNC),
        )
        sql, params = query.to_sql()
        result = await backend.execute(sql, params, options=opts)
        assert result.data is not None
        assert len(result.data) == 1
        assert result.data[0]["wkt"] is not None

    @pytest.mark.asyncio
    async def test_async_st_transform(self, async_postgis_env):
        """Test ST_Transform for coordinate system conversion."""
        backend, dialect = async_postgis_env
        opts = ExecutionOptions(stmt_type=StatementType.DQL)

        # Transform a point from WGS84 (4326) to Web Mercator (3857)
        query = QueryExpression(
            dialect=dialect,
            select=[
                FunctionCall(
                    dialect,
                    "ST_AsText",
                    FunctionCall(
                        dialect,
                        "ST_Transform",
                        FunctionCall(
                            dialect, "ST_GeomFromText", Literal(dialect, "POINT(0 0)"), Literal(dialect, 4326)
                        ),
                        Literal(dialect, 3857),
                    ),
                ).as_("wkt"),
            ],
        )
        sql, params = query.to_sql()
        result = await backend.execute(sql, params, options=opts)
        assert result.data is not None
        assert result.data[0]["wkt"] is not None

    @pytest.mark.asyncio
    async def test_async_st_as_geojson(self, async_postgis_env):
        """Test ST_AsGeoJSON for GeoJSON output."""
        backend, dialect = async_postgis_env
        opts = ExecutionOptions(stmt_type=StatementType.DQL)

        query = QueryExpression(
            dialect=dialect,
            select=[
                FunctionCall(
                    dialect,
                    "ST_AsGeoJSON",
                    FunctionCall(dialect, "ST_GeomFromText", Literal(dialect, "POINT(1 2)"), Literal(dialect, 4326)),
                ).as_("geojson"),
            ],
        )
        sql, params = query.to_sql()
        result = await backend.execute(sql, params, options=opts)
        assert result.data is not None
        geojson = result.data[0]["geojson"]
        assert '"type"' in geojson
        assert '"Point"' in geojson
        assert '"coordinates"' in geojson

    @pytest.mark.asyncio
    async def test_async_st_buffer(self, async_postgis_env):
        """Test ST_Buffer for creating buffer zones around geometries."""
        backend, dialect = async_postgis_env
        opts = ExecutionOptions(stmt_type=StatementType.DQL)

        # ST_Buffer with ::geography cast - FunctionCall.cast() generates the
        # PostgreSQL-specific ::type syntax (e.g. ST_GEOMFROMTEXT(...)::geography)
        geog_point = FunctionCall(
            dialect, "ST_GeomFromText",
            Literal(dialect, "POINT(0 0)"), Literal(dialect, 4326),
        ).cast("geography")
        query = QueryExpression(
            dialect=dialect,
            select=[
                FunctionCall(
                    dialect,
                    "ST_AsText",
                    FunctionCall(
                        dialect,
                        "ST_Buffer",
                        geog_point,
                        Literal(dialect, 1000),
                    ),
                ).as_("wkt"),
            ],
        )
        sql, params = query.to_sql()
        result = await backend.execute(sql, params, options=opts)
        assert result.data is not None
        # Buffer of a point should produce a polygon
        assert result.data[0]["wkt"].startswith("POLYGON")
