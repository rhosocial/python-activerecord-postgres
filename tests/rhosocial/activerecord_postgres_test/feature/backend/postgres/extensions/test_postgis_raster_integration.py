"""Integration tests for the PostGIS raster extension.

These tests require a PostgreSQL database with the postgis extension installed.
Tests will be automatically skipped if the extension is not available.
All database operations use expression objects, not raw SQL strings.

Note: Creating valid raster hex WKB data manually is impractical, so this
test uses ST_MakeEmptyRaster to create test rasters and verifies the
function factories work correctly.
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
)
from rhosocial.activerecord.backend.expression.statements import ValuesSource
from rhosocial.activerecord.backend.expression.core import Literal, FunctionCall
from rhosocial.activerecord.backend.impl.postgres.functions.postgis_raster import (
    st_rast_from_hexwkb,
    st_value,
    st_summary,
)
from rhosocial.activerecord.backend.options import ExecutionOptions
from rhosocial.activerecord.backend.schema import StatementType


TABLE_NAME = "test_raster_data"
ASYNC_TABLE_NAME = "test_raster_data_async"


def _make_empty_raster(dialect, width=10, height=10, srid=4326):
    """Create an empty raster using ST_MakeEmptyRaster + ST_AddBand.

    This is more reliable than using hex WKB data.
    """
    # ST_MakeEmptyRaster(width, height, upperleftx, upperlefty, scalex, scaley, skewx, skewy, srid)
    empty_rast = FunctionCall(
        dialect, "ST_MakeEmptyRaster",
        Literal(dialect, width),
        Literal(dialect, height),
        Literal(dialect, 0.0),
        Literal(dialect, 0.0),
        Literal(dialect, 1.0),
        Literal(dialect, 1.0),
        Literal(dialect, 0.0),
        Literal(dialect, 0.0),
        Literal(dialect, srid),
    )
    # ST_AddBand(rast, index, pixeltype, initialvalue, nodatavalue)
    rast_with_band = FunctionCall(
        dialect, "ST_AddBand",
        empty_rast,
        Literal(dialect, 1),
        Literal(dialect, "8BUI"),
        Literal(dialect, 0),
        Literal(dialect, 0),
    )
    return rast_with_band


@pytest.fixture
def raster_env(postgres_backend_single):
    """Test environment for PostGIS raster functions."""
    backend = postgres_backend_single
    ensure_extension_installed(backend, "postgis", "postgis_raster")
    dialect = backend.dialect

    # Setup: create test_raster_data table using expression
    columns = [
        ColumnDefinition(
            name="id",
            data_type="SERIAL",
            constraints=[
                ColumnConstraint(ColumnConstraintType.PRIMARY_KEY),
            ],
        ),
        ColumnDefinition(name="name", data_type="TEXT"),
        ColumnDefinition(name="rast", data_type="RASTER"),
    ]
    create_expr = CreateTableExpression(
        dialect=dialect,
        table_name=TABLE_NAME,
        columns=columns,
        if_not_exists=True,
    )
    sql, params = create_expr.to_sql()
    backend.execute(sql, params)

    # Setup: insert raster data using ST_MakeEmptyRaster expression
    rast_expr = _make_empty_raster(dialect)
    insert_expr = InsertExpression(
        dialect=dialect,
        into=TABLE_NAME,
        columns=["name", "rast"],
        source=ValuesSource(
            dialect,
            [
                [
                    Literal(dialect, "test_empty_raster"),
                    rast_expr,
                ],
            ],
        ),
    )
    sql, params = insert_expr.to_sql()
    backend.execute(sql, params)

    yield backend, dialect

    # Teardown: drop table using expression
    drop_expr = DropTableExpression(
        dialect=dialect,
        table_name=TABLE_NAME,
        if_exists=True,
    )
    sql, params = drop_expr.to_sql()
    backend.execute(sql, params)


class TestPostgisRasterIntegration:
    """Integration tests for PostGIS raster extension functions."""

    def test_st_summary(self, raster_env):
        """Test ST_Summary function for raster metadata."""
        backend, dialect = raster_env

        summary_func = st_summary(dialect, Column(dialect, "rast"))
        query = QueryExpression(
            dialect=dialect,
            select=[summary_func.as_("summary")],
            from_=TableExpression(dialect, TABLE_NAME),
        )
        sql, params = query.to_sql()
        opts = ExecutionOptions(stmt_type=StatementType.DQL)
        result = backend.execute(sql, params, options=opts)
        assert result.data is not None
        assert len(result.data) > 0
        # ST_Summary returns a text summary of the raster
        assert result.data[0]["summary"] is not None

    def test_st_value(self, raster_env):
        """Test ST_Value function for pixel value retrieval.

        Note: ST_Value on an empty raster with nodata=0 returns NULL
        since there is no actual pixel data.
        """
        backend, dialect = raster_env

        value_func = st_value(
            dialect,
            Column(dialect, "rast"),
            Literal(dialect, 1),
            Literal(dialect, 1),
        )
        query = QueryExpression(
            dialect=dialect,
            select=[value_func.as_("pixel_value")],
            from_=TableExpression(dialect, TABLE_NAME),
        )
        sql, params = query.to_sql()
        opts = ExecutionOptions(stmt_type=StatementType.DQL)
        result = backend.execute(sql, params, options=opts)
        assert result.data is not None
        assert len(result.data) > 0

    def test_st_rast_from_hexwkb_sql_generation(self, raster_env):
        """Test st_rast_from_hexwkb generates valid SQL expression.

        This tests SQL generation only, not execution, since creating
        valid hex WKB data manually is impractical.
        """
        backend, dialect = raster_env

        func = st_rast_from_hexwkb(dialect, "01000001...", srid=4326)
        sql, params = func.to_sql()
        assert "st_rastfromhexwkb" in sql.lower()
        assert "st_setsrid" in sql.lower()


@pytest_asyncio.fixture
async def async_raster_env(async_postgres_backend_single):
    """Async test environment for PostGIS raster functions."""
    backend = async_postgres_backend_single
    await async_ensure_extension_installed(backend, "postgis", "postgis_raster")
    dialect = backend.dialect

    # Setup: create test_raster_data_async table using expression
    columns = [
        ColumnDefinition(
            name="id",
            data_type="SERIAL",
            constraints=[
                ColumnConstraint(ColumnConstraintType.PRIMARY_KEY),
            ],
        ),
        ColumnDefinition(name="name", data_type="TEXT"),
        ColumnDefinition(name="rast", data_type="RASTER"),
    ]
    create_expr = CreateTableExpression(
        dialect=dialect,
        table_name=ASYNC_TABLE_NAME,
        columns=columns,
        if_not_exists=True,
    )
    sql, params = create_expr.to_sql()
    await backend.execute(sql, params)

    # Setup: insert raster data using ST_MakeEmptyRaster expression
    rast_expr = _make_empty_raster(dialect)
    insert_expr = InsertExpression(
        dialect=dialect,
        into=ASYNC_TABLE_NAME,
        columns=["name", "rast"],
        source=ValuesSource(
            dialect,
            [
                [
                    Literal(dialect, "test_empty_raster"),
                    rast_expr,
                ],
            ],
        ),
    )
    sql, params = insert_expr.to_sql()
    await backend.execute(sql, params)

    yield backend, dialect

    # Teardown: drop table using expression
    drop_expr = DropTableExpression(
        dialect=dialect,
        table_name=ASYNC_TABLE_NAME,
        if_exists=True,
    )
    sql, params = drop_expr.to_sql()
    await backend.execute(sql, params)


class TestAsyncPostgisRasterIntegration:
    """Async integration tests for PostGIS raster extension functions."""

    @pytest.mark.asyncio
    async def test_async_st_summary(self, async_raster_env):
        """Test ST_Summary function for raster metadata asynchronously."""
        backend, dialect = async_raster_env

        summary_func = st_summary(dialect, Column(dialect, "rast"))
        query = QueryExpression(
            dialect=dialect,
            select=[summary_func.as_("summary")],
            from_=TableExpression(dialect, ASYNC_TABLE_NAME),
        )
        sql, params = query.to_sql()
        opts = ExecutionOptions(stmt_type=StatementType.DQL)
        result = await backend.execute(sql, params, options=opts)
        assert result.data is not None
        assert len(result.data) > 0
        # ST_Summary returns a text summary of the raster
        assert result.data[0]["summary"] is not None

    @pytest.mark.asyncio
    async def test_async_st_value(self, async_raster_env):
        """Test ST_Value function for pixel value retrieval asynchronously.

        Note: ST_Value on an empty raster with nodata=0 returns NULL
        since there is no actual pixel data.
        """
        backend, dialect = async_raster_env

        value_func = st_value(
            dialect,
            Column(dialect, "rast"),
            Literal(dialect, 1),
            Literal(dialect, 1),
        )
        query = QueryExpression(
            dialect=dialect,
            select=[value_func.as_("pixel_value")],
            from_=TableExpression(dialect, ASYNC_TABLE_NAME),
        )
        sql, params = query.to_sql()
        opts = ExecutionOptions(stmt_type=StatementType.DQL)
        result = await backend.execute(sql, params, options=opts)
        assert result.data is not None
        assert len(result.data) > 0

    @pytest.mark.asyncio
    async def test_async_st_rast_from_hexwkb_sql_generation(self, async_raster_env):
        """Test st_rast_from_hexwkb generates valid SQL expression asynchronously.

        This tests SQL generation only, not execution, since creating
        valid hex WKB data manually is impractical.
        """
        backend, dialect = async_raster_env

        func = st_rast_from_hexwkb(dialect, "01000001...", srid=4326)
        sql, params = func.to_sql()
        assert "st_rastfromhexwkb" in sql.lower()
        assert "st_setsrid" in sql.lower()
