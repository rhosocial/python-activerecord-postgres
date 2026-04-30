# tests/rhosocial/activerecord_postgres_test/feature/backend/postgres/extensions/test_cube_integration.py

"""Integration tests for PostgreSQL cube extension with real database.

These tests require a live PostgreSQL connection with cube extension installed
and test:
- CUBE column creation and data retrieval
- cube_distance() function (<-> operator equivalent)
- Containment operator @> via Literal
- Overlap operator && via Literal
- GiST index on CUBE column
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
from rhosocial.activerecord.backend.expression.core import Literal, FunctionCall, Subquery
from rhosocial.activerecord.backend.expression import RawSQLExpression
from rhosocial.activerecord.backend.expression.predicates import ComparisonPredicate
from rhosocial.activerecord.backend.options import ExecutionOptions
from rhosocial.activerecord.backend.schema import StatementType


# --- Sync fixture and tests ---


@pytest.fixture
def cube_env(postgres_backend_single):
    """Independent test environment for cube extension."""
    backend = postgres_backend_single
    ensure_extension_installed(backend, "cube")
    dialect = backend.dialect

    # Clean up residual tables from previous runs
    for table_name in ["test_cubes", "test_cube_dist", "test_cube_idx"]:
        drop_expr = DropTableExpression(dialect=dialect, table_name=table_name, if_exists=True)
        sql, params = drop_expr.to_sql()
        backend.execute(sql, params)

    # Setup: create test_cubes table
    create_cubes = CreateTableExpression(
        dialect=dialect,
        table_name="test_cubes",
        columns=[
            ColumnDefinition(
                name="id",
                data_type="SERIAL",
                constraints=[
                    ColumnConstraint(ColumnConstraintType.PRIMARY_KEY),
                ],
            ),
            ColumnDefinition(name="c", data_type="CUBE"),
        ],
        if_not_exists=True,
    )
    sql, params = create_cubes.to_sql()
    backend.execute(sql, params)

    # Insert data into test_cubes
    insert_cubes = InsertExpression(
        dialect=dialect,
        into="test_cubes",
        columns=["c"],
        source=ValuesSource(
            dialect,
            [
                [Literal(dialect, "(1,2,3)").cast("cube")],
                [Literal(dialect, "(4,5,6)").cast("cube")],
            ],
        ),
    )
    sql, params = insert_cubes.to_sql()
    backend.execute(sql, params)

    # Setup: create test_cube_dist table
    create_dist = CreateTableExpression(
        dialect=dialect,
        table_name="test_cube_dist",
        columns=[
            ColumnDefinition(
                name="id",
                data_type="SERIAL",
                constraints=[
                    ColumnConstraint(ColumnConstraintType.PRIMARY_KEY),
                ],
            ),
            ColumnDefinition(name="c", data_type="CUBE"),
        ],
        if_not_exists=True,
    )
    sql, params = create_dist.to_sql()
    backend.execute(sql, params)

    # Insert data into test_cube_dist
    insert_dist = InsertExpression(
        dialect=dialect,
        into="test_cube_dist",
        columns=["c"],
        source=ValuesSource(
            dialect,
            [
                [Literal(dialect, "(1,1)").cast("cube")],
                [Literal(dialect, "(4,5)").cast("cube")],
            ],
        ),
    )
    sql, params = insert_dist.to_sql()
    backend.execute(sql, params)

    # Setup: create test_cube_idx table
    create_idx_table = CreateTableExpression(
        dialect=dialect,
        table_name="test_cube_idx",
        columns=[
            ColumnDefinition(
                name="id",
                data_type="SERIAL",
                constraints=[
                    ColumnConstraint(ColumnConstraintType.PRIMARY_KEY),
                ],
            ),
            ColumnDefinition(name="c", data_type="CUBE"),
        ],
        if_not_exists=True,
    )
    sql, params = create_idx_table.to_sql()
    backend.execute(sql, params)

    # Insert data into test_cube_idx
    insert_idx = InsertExpression(
        dialect=dialect,
        into="test_cube_idx",
        columns=["c"],
        source=ValuesSource(
            dialect,
            [[Literal(dialect, "(1,2,3)").cast("cube")]],
        ),
    )
    sql, params = insert_idx.to_sql()
    backend.execute(sql, params)

    # Create GiST index on test_cube_idx
    create_index = CreateIndexExpression(
        dialect=dialect,
        index_name="idx_test_cube",
        table_name="test_cube_idx",
        columns=["c"],
        index_type="GIST",
        if_not_exists=True,
    )
    sql, params = create_index.to_sql()
    backend.execute(sql, params)

    yield backend, dialect

    # Teardown: drop tables
    for table_name in ["test_cubes", "test_cube_dist", "test_cube_idx"]:
        drop_expr = DropTableExpression(
            dialect=dialect,
            table_name=table_name,
            if_exists=True,
        )
        sql, params = drop_expr.to_sql()
        backend.execute(sql, params)


class TestCubeIntegration:
    """Integration tests for cube multidimensional data type."""

    def test_cube_type_crud(self, cube_env):
        """Test querying cube column data."""
        backend, dialect = cube_env
        opts = ExecutionOptions(stmt_type=StatementType.DQL)

        query = QueryExpression(
            dialect=dialect,
            select=[Column(dialect, "c")],
            from_=TableExpression(dialect, "test_cubes"),
            order_by=OrderByClause(dialect, [Column(dialect, "id")]),
        )
        sql, params = query.to_sql()
        result = backend.execute(sql, params, options=opts)
        assert result.data is not None
        assert len(result.data) == 2
        assert result.data[0]['c'] is not None
        assert result.data[1]['c'] is not None

    def test_cube_distance(self, cube_env):
        """Test cube distance via cube_distance() function."""
        backend, dialect = cube_env
        opts = ExecutionOptions(stmt_type=StatementType.DQL)

        # cube_distance() is the function equivalent of the <-> operator
        dist_func = FunctionCall(
            dialect,
            "cube_distance",
            Literal(dialect, "(1,1)").cast("cube"),
            Literal(dialect, "(4,5)").cast("cube"),
        ).as_("distance")
        query = QueryExpression(
            dialect=dialect,
            select=[dist_func],
        )
        sql, params = query.to_sql()
        result = backend.execute(sql, params, options=opts)
        assert result.data is not None
        # Distance between (1,1) and (4,5) = sqrt(9+16) = 5
        assert float(result.data[0]['distance']) == pytest.approx(5.0, abs=1e-4)

    def test_cube_contains(self, cube_env):
        """Test cube containment operator @> via Literal."""
        backend, dialect = cube_env
        opts = ExecutionOptions(stmt_type=StatementType.DQL)

        # A point cube is contained by a range cube
        contains_expr = Subquery(
            dialect,
            ComparisonPredicate(
                dialect, "@>",
                Literal(dialect, "(0,0),(10,10)").cast("cube"),
                Literal(dialect, "(5,5)").cast("cube"),
            ),
        ).as_("contains")
        query = QueryExpression(
            dialect=dialect,
            select=[contains_expr],
        )
        sql, params = query.to_sql()
        result = backend.execute(sql, params, options=opts)
        assert result.data is not None
        assert result.data[0]['contains'] is True

        # Point outside the range cube
        not_contains_expr = Subquery(
            dialect,
            ComparisonPredicate(
                dialect, "@>",
                Literal(dialect, "(0,0),(10,10)").cast("cube"),
                Literal(dialect, "(15,15)").cast("cube"),
            ),
        ).as_("contains")
        query2 = QueryExpression(
            dialect=dialect,
            select=[not_contains_expr],
        )
        sql2, params2 = query2.to_sql()
        result2 = backend.execute(sql2, params2, options=opts)
        assert result2.data is not None
        assert result2.data[0]['contains'] is False

    def test_cube_overlap(self, cube_env):
        """Test cube overlap operator && via Literal."""
        backend, dialect = cube_env
        opts = ExecutionOptions(stmt_type=StatementType.DQL)

        # Overlapping cubes
        overlap_expr = Subquery(
            dialect,
            ComparisonPredicate(
                dialect, "&&",
                Literal(dialect, "(0,0),(5,5)").cast("cube"),
                Literal(dialect, "(3,3),(8,8)").cast("cube"),
            ),
        ).as_("overlaps")
        query = QueryExpression(
            dialect=dialect,
            select=[overlap_expr],
        )
        sql, params = query.to_sql()
        result = backend.execute(sql, params, options=opts)
        assert result.data is not None
        assert result.data[0]['overlaps'] is True

        # Non-overlapping cubes
        no_overlap_expr = Subquery(
            dialect,
            ComparisonPredicate(
                dialect, "&&",
                Literal(dialect, "(0,0),(2,2)").cast("cube"),
                Literal(dialect, "(5,5),(8,8)").cast("cube"),
            ),
        ).as_("overlaps")
        query2 = QueryExpression(
            dialect=dialect,
            select=[no_overlap_expr],
        )
        sql2, params2 = query2.to_sql()
        result2 = backend.execute(sql2, params2, options=opts)
        assert result2.data is not None
        assert result2.data[0]['overlaps'] is False

    def test_cube_index(self, cube_env):
        """Test that GiST index on cube column was created successfully."""
        backend, dialect = cube_env
        opts = ExecutionOptions(stmt_type=StatementType.DQL)

        query = QueryExpression(
            dialect=dialect,
            select=[Column(dialect, "indexname")],
            from_=TableExpression(dialect, "pg_indexes"),
            where=Column(dialect, "tablename") == Literal(dialect, "test_cube_idx"),
        )
        sql, params = query.to_sql()
        result = backend.execute(sql, params, options=opts)
        assert result.data is not None
        index_names = [r['indexname'] for r in result.data]
        assert "idx_test_cube" in index_names


# --- Async fixture and tests ---


@pytest_asyncio.fixture
async def async_cube_env(async_postgres_backend_single):
    """Independent async test environment for cube extension."""
    backend = async_postgres_backend_single
    await async_ensure_extension_installed(backend, "cube")
    dialect = backend.dialect

    # Clean up residual tables from previous runs
    for table_name in ["test_cubes_async", "test_cube_dist_async", "test_cube_idx_async"]:
        drop_expr = DropTableExpression(dialect=dialect, table_name=table_name, if_exists=True)
        sql, params = drop_expr.to_sql()
        await backend.execute(sql, params)

    # Setup: create test_cubes_async table
    create_cubes = CreateTableExpression(
        dialect=dialect,
        table_name="test_cubes_async",
        columns=[
            ColumnDefinition(
                name="id",
                data_type="SERIAL",
                constraints=[
                    ColumnConstraint(ColumnConstraintType.PRIMARY_KEY),
                ],
            ),
            ColumnDefinition(name="c", data_type="CUBE"),
        ],
        if_not_exists=True,
    )
    sql, params = create_cubes.to_sql()
    await backend.execute(sql, params)

    # Insert data into test_cubes_async
    insert_cubes = InsertExpression(
        dialect=dialect,
        into="test_cubes_async",
        columns=["c"],
        source=ValuesSource(
            dialect,
            [
                [Literal(dialect, "(1,2,3)").cast("cube")],
                [Literal(dialect, "(4,5,6)").cast("cube")],
            ],
        ),
    )
    sql, params = insert_cubes.to_sql()
    await backend.execute(sql, params)

    # Setup: create test_cube_dist_async table
    create_dist = CreateTableExpression(
        dialect=dialect,
        table_name="test_cube_dist_async",
        columns=[
            ColumnDefinition(
                name="id",
                data_type="SERIAL",
                constraints=[
                    ColumnConstraint(ColumnConstraintType.PRIMARY_KEY),
                ],
            ),
            ColumnDefinition(name="c", data_type="CUBE"),
        ],
        if_not_exists=True,
    )
    sql, params = create_dist.to_sql()
    await backend.execute(sql, params)

    # Insert data into test_cube_dist_async
    insert_dist = InsertExpression(
        dialect=dialect,
        into="test_cube_dist_async",
        columns=["c"],
        source=ValuesSource(
            dialect,
            [
                [Literal(dialect, "(1,1)").cast("cube")],
                [Literal(dialect, "(4,5)").cast("cube")],
            ],
        ),
    )
    sql, params = insert_dist.to_sql()
    await backend.execute(sql, params)

    # Setup: create test_cube_idx_async table
    create_idx_table = CreateTableExpression(
        dialect=dialect,
        table_name="test_cube_idx_async",
        columns=[
            ColumnDefinition(
                name="id",
                data_type="SERIAL",
                constraints=[
                    ColumnConstraint(ColumnConstraintType.PRIMARY_KEY),
                ],
            ),
            ColumnDefinition(name="c", data_type="CUBE"),
        ],
        if_not_exists=True,
    )
    sql, params = create_idx_table.to_sql()
    await backend.execute(sql, params)

    # Insert data into test_cube_idx_async
    insert_idx = InsertExpression(
        dialect=dialect,
        into="test_cube_idx_async",
        columns=["c"],
        source=ValuesSource(
            dialect,
            [[Literal(dialect, "(1,2,3)").cast("cube")]],
        ),
    )
    sql, params = insert_idx.to_sql()
    await backend.execute(sql, params)

    # Create GiST index on test_cube_idx_async
    create_index = CreateIndexExpression(
        dialect=dialect,
        index_name="idx_test_cube_async",
        table_name="test_cube_idx_async",
        columns=["c"],
        index_type="GIST",
        if_not_exists=True,
    )
    sql, params = create_index.to_sql()
    await backend.execute(sql, params)

    yield backend, dialect

    # Teardown: drop tables
    for table_name in ["test_cubes_async", "test_cube_dist_async", "test_cube_idx_async"]:
        drop_expr = DropTableExpression(
            dialect=dialect,
            table_name=table_name,
            if_exists=True,
        )
        sql, params = drop_expr.to_sql()
        await backend.execute(sql, params)


class TestAsyncCubeIntegration:
    """Async integration tests for cube multidimensional data type."""

    @pytest.mark.asyncio
    async def test_async_cube_type_crud(self, async_cube_env):
        """Test querying cube column data."""
        backend, dialect = async_cube_env
        opts = ExecutionOptions(stmt_type=StatementType.DQL)

        query = QueryExpression(
            dialect=dialect,
            select=[Column(dialect, "c")],
            from_=TableExpression(dialect, "test_cubes_async"),
            order_by=OrderByClause(dialect, [Column(dialect, "id")]),
        )
        sql, params = query.to_sql()
        result = await backend.execute(sql, params, options=opts)
        assert result.data is not None
        assert len(result.data) == 2
        assert result.data[0]['c'] is not None
        assert result.data[1]['c'] is not None

    @pytest.mark.asyncio
    async def test_async_cube_distance(self, async_cube_env):
        """Test cube distance via cube_distance() function."""
        backend, dialect = async_cube_env
        opts = ExecutionOptions(stmt_type=StatementType.DQL)

        dist_func = FunctionCall(
            dialect,
            "cube_distance",
            Literal(dialect, "(1,1)").cast("cube"),
            Literal(dialect, "(4,5)").cast("cube"),
        ).as_("distance")
        query = QueryExpression(
            dialect=dialect,
            select=[dist_func],
        )
        sql, params = query.to_sql()
        result = await backend.execute(sql, params, options=opts)
        assert result.data is not None
        assert float(result.data[0]['distance']) == pytest.approx(5.0, abs=1e-4)

    @pytest.mark.asyncio
    async def test_async_cube_contains(self, async_cube_env):
        """Test cube containment operator @> via Literal."""
        backend, dialect = async_cube_env
        opts = ExecutionOptions(stmt_type=StatementType.DQL)

        # A point cube is contained by a range cube
        # TODO: Replace with ComparisonPredicate when it supports .as_() alias
        contains_expr = RawSQLExpression(
            dialect, "'(0,0),(10,10)'::cube @> '(5,5)'::cube AS contains"
        )
        query = QueryExpression(
            dialect=dialect,
            select=[contains_expr],
        )
        sql, params = query.to_sql()
        result = await backend.execute(sql, params, options=opts)
        assert result.data is not None
        assert result.data[0]['contains'] is True

        # Point outside the range cube
        # TODO: Replace with ComparisonPredicate when it supports .as_() alias
        not_contains_expr = RawSQLExpression(
            dialect, "'(0,0),(10,10)'::cube @> '(15,15)'::cube AS contains"
        )
        query2 = QueryExpression(
            dialect=dialect,
            select=[not_contains_expr],
        )
        sql2, params2 = query2.to_sql()
        result2 = await backend.execute(sql2, params2, options=opts)
        assert result2.data is not None
        assert result2.data[0]['contains'] is False

    @pytest.mark.asyncio
    async def test_async_cube_overlap(self, async_cube_env):
        """Test cube overlap operator && via Literal."""
        backend, dialect = async_cube_env
        opts = ExecutionOptions(stmt_type=StatementType.DQL)

        # Overlapping cubes
        # TODO: Replace with ComparisonPredicate when it supports .as_() alias
        overlap_expr = RawSQLExpression(
            dialect, "'(0,0),(5,5)'::cube && '(3,3),(8,8)'::cube AS overlaps"
        )
        query = QueryExpression(
            dialect=dialect,
            select=[overlap_expr],
        )
        sql, params = query.to_sql()
        result = await backend.execute(sql, params, options=opts)
        assert result.data is not None
        assert result.data[0]['overlaps'] is True

        # Non-overlapping cubes
        # TODO: Replace with ComparisonPredicate when it supports .as_() alias
        no_overlap_expr = RawSQLExpression(
            dialect, "'(0,0),(2,2)'::cube && '(5,5),(8,8)'::cube AS overlaps"
        )
        query2 = QueryExpression(
            dialect=dialect,
            select=[no_overlap_expr],
        )
        sql2, params2 = query2.to_sql()
        result2 = await backend.execute(sql2, params2, options=opts)
        assert result2.data is not None
        assert result2.data[0]['overlaps'] is False

    @pytest.mark.asyncio
    async def test_async_cube_index(self, async_cube_env):
        """Test that GiST index on cube column was created successfully."""
        backend, dialect = async_cube_env
        opts = ExecutionOptions(stmt_type=StatementType.DQL)

        query = QueryExpression(
            dialect=dialect,
            select=[Column(dialect, "indexname")],
            from_=TableExpression(dialect, "pg_indexes"),
            where=Column(dialect, "tablename") == Literal(dialect, "test_cube_idx_async"),
        )
        sql, params = query.to_sql()
        result = await backend.execute(sql, params, options=opts)
        assert result.data is not None
        index_names = [r['indexname'] for r in result.data]
        assert "idx_test_cube_async" in index_names
