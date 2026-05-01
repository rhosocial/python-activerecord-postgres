"""Integration tests for the pgRouting extension.

These tests require a PostgreSQL database with both the postgis and
pgrouting extensions installed.
Tests will be automatically skipped if the extensions are not available.
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
from rhosocial.activerecord.backend.expression.core import Literal
from rhosocial.activerecord.backend.impl.postgres.functions.pgrouting import (
    pgr_dijkstra,
    pgr_astar,
)
from rhosocial.activerecord.backend.options import ExecutionOptions
from rhosocial.activerecord.backend.schema import StatementType


@pytest.fixture
def pgrouting_env(postgres_backend_single):
    """Test environment for pgRouting extension with edge table prepared."""
    backend = postgres_backend_single
    ensure_extension_installed(backend, "postgis", "pgrouting")
    dialect = backend.dialect

    # Setup: create test_route_edges table using expression
    columns = [
        ColumnDefinition(
            name="id",
            data_type="SERIAL",
            constraints=[
                ColumnConstraint(ColumnConstraintType.PRIMARY_KEY),
            ],
        ),
        ColumnDefinition(name="source", data_type="INT"),
        ColumnDefinition(name="target", data_type="INT"),
        ColumnDefinition(name="cost", data_type="FLOAT"),
        ColumnDefinition(name="reverse_cost", data_type="FLOAT"),
        ColumnDefinition(name="x1", data_type="FLOAT"),
        ColumnDefinition(name="y1", data_type="FLOAT"),
        ColumnDefinition(name="x2", data_type="FLOAT"),
        ColumnDefinition(name="y2", data_type="FLOAT"),
    ]
    create_expr = CreateTableExpression(
        dialect=dialect,
        table_name="test_route_edges",
        columns=columns,
        if_not_exists=True,
    )
    sql, params = create_expr.to_sql()
    backend.execute(sql, params)

    # Setup: insert edge data for a simple graph (3 edges)
    # Graph: 1 --(cost=1)--> 2 --(cost=2)--> 3
    #             1 --(cost=4)--> 3
    insert_expr = InsertExpression(
        dialect=dialect,
        into="test_route_edges",
        columns=["source", "target", "cost", "reverse_cost", "x1", "y1", "x2", "y2"],
        source=ValuesSource(
            dialect,
            [
                # Edge 1->2
                [
                    Literal(dialect, 1), Literal(dialect, 2),
                    Literal(dialect, 1.0), Literal(dialect, 1.0),
                    Literal(dialect, 0.0), Literal(dialect, 0.0),
                    Literal(dialect, 1.0), Literal(dialect, 0.0),
                ],
                # Edge 2->3
                [
                    Literal(dialect, 2), Literal(dialect, 3),
                    Literal(dialect, 2.0), Literal(dialect, 2.0),
                    Literal(dialect, 1.0), Literal(dialect, 0.0),
                    Literal(dialect, 2.0), Literal(dialect, 0.0),
                ],
                # Edge 1->3
                [
                    Literal(dialect, 1), Literal(dialect, 3),
                    Literal(dialect, 4.0), Literal(dialect, 4.0),
                    Literal(dialect, 0.0), Literal(dialect, 0.0),
                    Literal(dialect, 2.0), Literal(dialect, 0.0),
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
        table_name="test_route_edges",
        if_exists=True,
    )
    sql, params = drop_expr.to_sql()
    backend.execute(sql, params)


class TestPgroutingIntegration:
    """Integration tests for pgRouting extension functions."""

    def test_pgr_dijkstra(self, pgrouting_env):
        """Test pgr_dijkstra for shortest path using Dijkstra's algorithm."""
        backend, dialect = pgrouting_env

        dijkstra_func = pgr_dijkstra(
            dialect,
            "SELECT id, source, target, cost, reverse_cost FROM test_route_edges",
            Literal(dialect, 1),
            Literal(dialect, 3),
        )
        query = QueryExpression(
            dialect=dialect,
            select=[dijkstra_func.as_("route")],
        )
        sql, params = query.to_sql()
        opts = ExecutionOptions(stmt_type=StatementType.DQL)
        result = backend.execute(sql, params, options=opts)
        assert result.data is not None
        # pgr_dijkstra returns routing result rows
        assert len(result.data) > 0

    def test_pgr_astar(self, pgrouting_env):
        """Test pgr_astar for shortest path using A* algorithm."""
        backend, dialect = pgrouting_env

        astar_func = pgr_astar(
            dialect,
            "SELECT id, source, target, cost, reverse_cost, x1, y1, x2, y2 FROM test_route_edges",
            Literal(dialect, 1),
            Literal(dialect, 3),
        )
        query = QueryExpression(
            dialect=dialect,
            select=[astar_func.as_("route")],
        )
        sql, params = query.to_sql()
        opts = ExecutionOptions(stmt_type=StatementType.DQL)
        result = backend.execute(sql, params, options=opts)
        assert result.data is not None
        # pgr_astar returns routing result rows
        assert len(result.data) > 0


@pytest_asyncio.fixture
async def async_pgrouting_env(async_postgres_backend_single):
    """Async test environment for pgRouting extension with edge table prepared."""
    backend = async_postgres_backend_single
    await async_ensure_extension_installed(backend, "postgis", "pgrouting")
    dialect = backend.dialect

    # Setup: create test_route_edges_async table using expression
    columns = [
        ColumnDefinition(
            name="id",
            data_type="SERIAL",
            constraints=[
                ColumnConstraint(ColumnConstraintType.PRIMARY_KEY),
            ],
        ),
        ColumnDefinition(name="source", data_type="INT"),
        ColumnDefinition(name="target", data_type="INT"),
        ColumnDefinition(name="cost", data_type="FLOAT"),
        ColumnDefinition(name="reverse_cost", data_type="FLOAT"),
        ColumnDefinition(name="x1", data_type="FLOAT"),
        ColumnDefinition(name="y1", data_type="FLOAT"),
        ColumnDefinition(name="x2", data_type="FLOAT"),
        ColumnDefinition(name="y2", data_type="FLOAT"),
    ]
    create_expr = CreateTableExpression(
        dialect=dialect,
        table_name="test_route_edges_async",
        columns=columns,
        if_not_exists=True,
    )
    sql, params = create_expr.to_sql()
    await backend.execute(sql, params)

    # Setup: insert edge data for a simple graph (3 edges)
    # Graph: 1 --(cost=1)--> 2 --(cost=2)--> 3
    #             1 --(cost=4)--> 3
    insert_expr = InsertExpression(
        dialect=dialect,
        into="test_route_edges_async",
        columns=["source", "target", "cost", "reverse_cost", "x1", "y1", "x2", "y2"],
        source=ValuesSource(
            dialect,
            [
                # Edge 1->2
                [
                    Literal(dialect, 1), Literal(dialect, 2),
                    Literal(dialect, 1.0), Literal(dialect, 1.0),
                    Literal(dialect, 0.0), Literal(dialect, 0.0),
                    Literal(dialect, 1.0), Literal(dialect, 0.0),
                ],
                # Edge 2->3
                [
                    Literal(dialect, 2), Literal(dialect, 3),
                    Literal(dialect, 2.0), Literal(dialect, 2.0),
                    Literal(dialect, 1.0), Literal(dialect, 0.0),
                    Literal(dialect, 2.0), Literal(dialect, 0.0),
                ],
                # Edge 1->3
                [
                    Literal(dialect, 1), Literal(dialect, 3),
                    Literal(dialect, 4.0), Literal(dialect, 4.0),
                    Literal(dialect, 0.0), Literal(dialect, 0.0),
                    Literal(dialect, 2.0), Literal(dialect, 0.0),
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
        table_name="test_route_edges_async",
        if_exists=True,
    )
    sql, params = drop_expr.to_sql()
    await backend.execute(sql, params)


class TestAsyncPgroutingIntegration:
    """Async integration tests for pgRouting extension functions."""

    @pytest.mark.asyncio
    async def test_async_pgr_dijkstra(self, async_pgrouting_env):
        """Test pgr_dijkstra for shortest path using Dijkstra's algorithm asynchronously."""
        backend, dialect = async_pgrouting_env

        dijkstra_func = pgr_dijkstra(
            dialect,
            "SELECT id, source, target, cost, reverse_cost FROM test_route_edges_async",
            Literal(dialect, 1),
            Literal(dialect, 3),
        )
        query = QueryExpression(
            dialect=dialect,
            select=[dijkstra_func.as_("route")],
        )
        sql, params = query.to_sql()
        opts = ExecutionOptions(stmt_type=StatementType.DQL)
        result = await backend.execute(sql, params, options=opts)
        assert result.data is not None
        # pgr_dijkstra returns routing result rows
        assert len(result.data) > 0

    @pytest.mark.asyncio
    async def test_async_pgr_astar(self, async_pgrouting_env):
        """Test pgr_astar for shortest path using A* algorithm asynchronously."""
        backend, dialect = async_pgrouting_env

        astar_func = pgr_astar(
            dialect,
            "SELECT id, source, target, cost, reverse_cost, x1, y1, x2, y2 FROM test_route_edges_async",
            Literal(dialect, 1),
            Literal(dialect, 3),
        )
        query = QueryExpression(
            dialect=dialect,
            select=[astar_func.as_("route")],
        )
        sql, params = query.to_sql()
        opts = ExecutionOptions(stmt_type=StatementType.DQL)
        result = await backend.execute(sql, params, options=opts)
        assert result.data is not None
        # pgr_astar returns routing result rows
        assert len(result.data) > 0
