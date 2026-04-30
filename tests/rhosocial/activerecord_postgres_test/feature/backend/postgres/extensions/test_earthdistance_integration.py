# tests/rhosocial/activerecord_postgres_test/feature/backend/postgres/extensions/test_earthdistance_integration.py

"""Integration tests for the earthdistance extension.

These tests require a PostgreSQL database with the earthdistance and cube
extensions installed. Tests will be automatically skipped if the extensions
are not available.
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
)
from rhosocial.activerecord.backend.expression.statements import ValuesSource
from rhosocial.activerecord.backend.expression.core import Literal, FunctionCall
from rhosocial.activerecord.backend.options import ExecutionOptions
from rhosocial.activerecord.backend.schema import StatementType


TABLE_NAME = "test_earth"
ASYNC_TABLE_NAME = "test_earth_async"


@pytest.fixture
def earthdistance_env(postgres_backend_single):
    """Independent test environment for earthdistance extension."""
    backend = postgres_backend_single
    ensure_extension_installed(backend, "cube", "earthdistance")
    dialect = backend.dialect

    # Clean up residual tables from previous runs
    for table_name in [TABLE_NAME]:
        drop_expr = DropTableExpression(dialect=dialect, table_name=table_name, if_exists=True)
        sql, params = drop_expr.to_sql()
        backend.execute(sql, params)

    # Setup: create test table using expression
    columns = [
        ColumnDefinition(
            name="id",
            data_type="SERIAL",
            constraints=[
                ColumnConstraint(ColumnConstraintType.PRIMARY_KEY),
            ],
        ),
        ColumnDefinition(name="loc", data_type="POINT"),
    ]
    create_expr = CreateTableExpression(
        dialect=dialect,
        table_name=TABLE_NAME,
        columns=columns,
        if_not_exists=True,
    )
    sql, params = create_expr.to_sql()
    backend.execute(sql, params)

    # Setup: insert test data using expression
    insert_expr = InsertExpression(
        dialect=dialect,
        into=TABLE_NAME,
        columns=["loc"],
        source=ValuesSource(
            dialect,
            [
                [FunctionCall(dialect, "POINT", Literal(dialect, 0), Literal(dialect, 0))],
                [FunctionCall(dialect, "POINT", Literal(dialect, 0), Literal(dialect, 1))],
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


class TestEarthdistanceIntegration:
    """Integration tests for earthdistance great-circle distance calculations."""

    def test_earth_distance_operator(self, earthdistance_env):
        """Test the earthdistance <@> operator for great-circle distance."""
        backend, dialect = earthdistance_env
        opts = ExecutionOptions(stmt_type=StatementType.DQL)

        # earth_distance returns meters between two points on Earth's surface
        dist_func = FunctionCall(
            dialect, "earth_distance",
            FunctionCall(dialect, "ll_to_earth", Literal(dialect, 0), Literal(dialect, 0)),
            FunctionCall(dialect, "ll_to_earth", Literal(dialect, 0), Literal(dialect, 1)),
        ).as_("dist")
        query = QueryExpression(dialect=dialect, select=[dist_func])
        sql, params = query.to_sql()
        result = backend.execute(sql, params, options=opts)
        # 1 degree of latitude ≈ 111320 meters
        assert float(result.data[0]['dist']) == pytest.approx(111320.0, rel=0.01)

    def test_earth_box_function(self, earthdistance_env):
        """Test the earth_box function for bounding box calculation."""
        backend, dialect = earthdistance_env
        opts = ExecutionOptions(stmt_type=StatementType.DQL)

        box_func = FunctionCall(
            dialect, "earth_box",
            FunctionCall(dialect, "ll_to_earth", Literal(dialect, 0), Literal(dialect, 0)),
            Literal(dialect, 100.0),
        ).as_("bbox")
        query = QueryExpression(dialect=dialect, select=[box_func])
        sql, params = query.to_sql()
        result = backend.execute(sql, params, options=opts)
        assert result.data[0]['bbox'] is not None

    def test_earth_distance_zero(self, earthdistance_env):
        """Test that distance from a point to itself is zero."""
        backend, dialect = earthdistance_env
        opts = ExecutionOptions(stmt_type=StatementType.DQL)

        dist_func = FunctionCall(
            dialect, "earth_distance",
            FunctionCall(dialect, "ll_to_earth", Literal(dialect, 40.7), Literal(dialect, -74.0)),
            FunctionCall(dialect, "ll_to_earth", Literal(dialect, 40.7), Literal(dialect, -74.0)),
        ).as_("dist")
        query = QueryExpression(dialect=dialect, select=[dist_func])
        sql, params = query.to_sql()
        result = backend.execute(sql, params, options=opts)
        assert float(result.data[0]['dist']) == pytest.approx(0.0, abs=0.01)


@pytest_asyncio.fixture
async def async_earthdistance_env(async_postgres_backend_single):
    """Independent async test environment for earthdistance extension."""
    backend = async_postgres_backend_single
    await async_ensure_extension_installed(backend, "cube", "earthdistance")
    dialect = backend.dialect

    # Clean up residual tables from previous runs
    for table_name in [ASYNC_TABLE_NAME]:
        drop_expr = DropTableExpression(dialect=dialect, table_name=table_name, if_exists=True)
        sql, params = drop_expr.to_sql()
        await backend.execute(sql, params)

    # Setup: create test table using expression
    columns = [
        ColumnDefinition(
            name="id",
            data_type="SERIAL",
            constraints=[
                ColumnConstraint(ColumnConstraintType.PRIMARY_KEY),
            ],
        ),
        ColumnDefinition(name="loc", data_type="POINT"),
    ]
    create_expr = CreateTableExpression(
        dialect=dialect,
        table_name=ASYNC_TABLE_NAME,
        columns=columns,
        if_not_exists=True,
    )
    sql, params = create_expr.to_sql()
    await backend.execute(sql, params)

    # Setup: insert test data using expression
    insert_expr = InsertExpression(
        dialect=dialect,
        into=ASYNC_TABLE_NAME,
        columns=["loc"],
        source=ValuesSource(
            dialect,
            [
                [FunctionCall(dialect, "POINT", Literal(dialect, 0), Literal(dialect, 0))],
                [FunctionCall(dialect, "POINT", Literal(dialect, 0), Literal(dialect, 1))],
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


class TestAsyncEarthdistanceIntegration:
    """Async integration tests for earthdistance great-circle distance calculations."""

    @pytest.mark.asyncio
    async def test_async_earth_distance_operator(self, async_earthdistance_env):
        """Test the earthdistance <@> operator for great-circle distance (async)."""
        backend, dialect = async_earthdistance_env
        opts = ExecutionOptions(stmt_type=StatementType.DQL)

        dist_func = FunctionCall(
            dialect, "earth_distance",
            FunctionCall(dialect, "ll_to_earth", Literal(dialect, 0), Literal(dialect, 0)),
            FunctionCall(dialect, "ll_to_earth", Literal(dialect, 0), Literal(dialect, 1)),
        ).as_("dist")
        query = QueryExpression(dialect=dialect, select=[dist_func])
        sql, params = query.to_sql()
        result = await backend.execute(sql, params, options=opts)
        # 1 degree of latitude ≈ 111320 meters
        assert float(result.data[0]['dist']) == pytest.approx(111320.0, rel=0.01)

    @pytest.mark.asyncio
    async def test_async_earth_box_function(self, async_earthdistance_env):
        """Test the earth_box function for bounding box calculation (async)."""
        backend, dialect = async_earthdistance_env
        opts = ExecutionOptions(stmt_type=StatementType.DQL)

        box_func = FunctionCall(
            dialect, "earth_box",
            FunctionCall(dialect, "ll_to_earth", Literal(dialect, 0), Literal(dialect, 0)),
            Literal(dialect, 100.0),
        ).as_("bbox")
        query = QueryExpression(dialect=dialect, select=[box_func])
        sql, params = query.to_sql()
        result = await backend.execute(sql, params, options=opts)
        assert result.data[0]['bbox'] is not None

    @pytest.mark.asyncio
    async def test_async_earth_distance_zero(self, async_earthdistance_env):
        """Test that distance from a point to itself is zero (async)."""
        backend, dialect = async_earthdistance_env
        opts = ExecutionOptions(stmt_type=StatementType.DQL)

        dist_func = FunctionCall(
            dialect, "earth_distance",
            FunctionCall(dialect, "ll_to_earth", Literal(dialect, 40.7), Literal(dialect, -74.0)),
            FunctionCall(dialect, "ll_to_earth", Literal(dialect, 40.7), Literal(dialect, -74.0)),
        ).as_("dist")
        query = QueryExpression(dialect=dialect, select=[dist_func])
        sql, params = query.to_sql()
        result = await backend.execute(sql, params, options=opts)
        assert float(result.data[0]['dist']) == pytest.approx(0.0, abs=0.01)
